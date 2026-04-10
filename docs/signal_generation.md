# 信号产生逻辑文档

> 函数位置：`ae_server.py` → `AutoExchangeStrategy.server_scan_sell_surge_signals()`
>
> 调用方：`scan_loop()` 后台线程，每小时 UTC 第 3～8 分钟触发一次
>
> 最后更新：2026-04-10

---

## 核心思路

策略基于**卖量暴涨做空**：当某个币的上一小时主动卖出量相对昨日平均水平出现异常放大时，视为空头信号，做空该币。

---

## 触发时机

`scan_loop()` 是一个常驻后台线程：

1. 每秒检查 `is_running` 是否为 True（需通过 Web API `POST /api/start_trading` 开启）。
2. 检查当前 UTC 分钟是否在 **3 ≤ minute < 8** 窗口内。
3. 检查本小时是否已扫描过（`last_scan_hour` 去重，每小时只扫一次）。
4. 满足条件 → 刷新账户余额 → 调用 `server_scan_sell_surge_signals()` → 对返回的信号依次尝试开仓。

选择在每小时第 3～8 分钟扫描的原因：等上一小时 K 线完全收盘后再取数据，避免取到未收盘的不完整 K 线。

---

## 信号产生流程

### 第一步：获取交易对列表

调用 `_server_get_active_symbols()`：

- 通过 `futures_exchange_info()` 获取所有币安 U 本位合约。
- 筛选条件：以 `USDT` 结尾 + 状态 `TRADING` + 合约类型 `PERPETUAL`。
- 通常返回 300+ 个交易对。
- API 失败时降级使用 `BACKUP_SYMBOL_LIST`（30 个主流币）。
- 顺带刷新 `exchange_info` 缓存，供后续建仓查精度规则时使用。

### 第二步：并发预热昨日数据缓存

调用 `yesterday_cache.prefetch_all(symbols)`：

- 对所有交易对并发（`max_workers=20`）获取**昨日日 K 线**。
- 从日 K 中提取：
  - 总成交量（`volume`，K 线字段 index 5）
  - 主动买入量（`taker_buy_volume`，K 线字段 index 9）
  - **昨日总卖量** = 总成交量 - 主动买入量
  - **昨日平均小时卖量** = 昨日总卖量 / 24
- 结果按 UTC 日期缓存，当天内不重复请求。

### 第三步：并发扫描每个交易对

使用 `ThreadPoolExecutor(max_workers=10)` 并发执行 `_scan_one(symbol)`，每个交易对的判断逻辑：

#### 3.1 获取昨日基准

从缓存读取该币的**昨日平均小时卖量**（`yesterday_avg_hour_sell`），无数据则跳过。

#### 3.2 获取上一小时 K 线

```
check_hour = 当前整点 - 1小时
```

调用 `futures_klines(symbol, interval="1h", startTime=check_hour, limit=2)` 获取：

- `klines[0]`：上一个完整小时（信号判断用）
- `klines[1]`：当前小时（取开盘价作为信号价格）

**时间戳校验**：检查 `klines[0]` 的开盘时间是否精确等于 `check_hour`，防止 API 因数据缺失返回错误时段。

#### 3.3 计算卖量暴涨倍数

```
上一小时卖量 = 上一小时总成交量 - 上一小时主动买入量
暴涨倍数 = 上一小时卖量 / 昨日平均小时卖量
```

#### 3.4 阈值过滤

```python
sell_surge_threshold <= surge_ratio <= sell_surge_max
```

- `sell_surge_threshold`：默认 **10 倍**（下限，低于此不算暴涨）
- `sell_surge_max`：默认 **14008 倍**（上限，极端异常值排除）

不在区间内 → 跳过。

#### 3.5 确定信号价格

- 优先使用 `klines[1]` 的开盘价（即信号发生后下一小时的开盘价，更接近实际入场价）。
- 如果只有一根 K 线，使用上一小时收盘价。

#### 3.6 当日买量倍数风控

调用 `server_calculate_intraday_buy_surge_ratio(symbol, signal_time)`：

- 取信号前 **12 小时**的 1h K 线。
- 计算**每相邻两小时的主动买入量比值**（`curr_buy_vol / prev_buy_vol`）。
- 取其中的**最大值**作为当日买量倍数。
- 结果按 `(symbol, 小时)` 缓存，避免同一小时重复计算。

**危险区间过滤**（过滤多空博弈信号）：

```python
intraday_buy_ratio_danger_ranges = [
    (4.81, 6.61),   # 危险区间1
    (9.45, 11.1),   # 危险区间2
]
```

如果当日买量倍数落在任一危险区间内 → 信号被过滤。

**逻辑依据**：卖量暴涨的同时买量也暴涨，说明多空双方都在大量交易，方向不明确，做空胜率低。回测数据：

- 5～7 倍买量倍数：止盈率 16.7%，止损率 20.8% ❌
- 10～15 倍买量倍数：止盈率 6.2%，止损率 56.2% ❌
- \>15 倍买量倍数：止盈率 10.0%，止损率 50.0% ❌

#### 3.7 产出信号

通过所有检查后，返回信号字典：

```python
{
    "symbol": "ETHUSDT",
    "surge_ratio": 15.3,          # 卖量暴涨倍数
    "price": 3200.50,             # 信号价格
    "signal_time": "2026-04-10 12:00:00 UTC",
    "hour_sell_volume": 50000.0,  # 上一小时卖量
    "yesterday_avg": 3267.0,      # 昨日平均小时卖量
    "intraday_buy_ratio": 2.5,    # 当日买量倍数
}
```

### 第四步：汇总排序

所有交易对扫描完成后，按**卖量暴涨倍数降序**排列：

```python
sorted(signals, key=lambda x: x["surge_ratio"], reverse=True)
```

倍数越高优先开仓。

---

## 从信号到开仓

`scan_loop()` 拿到排序后的信号列表后：

1. 按顺序逐个调用 `server_open_position(signal)`。
2. 受 `max_opens_per_scan`（默认 1）限制，本轮最多成功开 1 仓。
3. 开仓失败（风控拒绝、已持仓等）不影响后续信号尝试。
4. 所有信号（无论是否开仓）都写入 `signal_history.json` 用于回溯。

---

## 关键参数

| 参数 | 默认值 | 来源 | 说明 |
|------|--------|------|------|
| `sell_surge_threshold` | 10.0 | config.ini [SIGNAL] | 卖量暴涨倍数下限 |
| `sell_surge_max` | 14008.0 | config.ini [SIGNAL] | 卖量暴涨倍数上限（排除极端值） |
| `enable_intraday_buy_ratio_filter` | True | 代码内 | 是否启用当日买量倍数风控 |
| `intraday_buy_ratio_danger_ranges` | [(4.81,6.61), (9.45,11.1)] | 代码内 | 买量倍数危险区间 |
| `max_opens_per_scan` | 1 | config.ini [STRATEGY] | 单次扫描最多开仓数 |

---

## 数据来源

所有数据均来自币安 U 本位合约 API：

| 数据 | 接口 | K 线字段 |
|------|------|----------|
| 昨日总成交量 | `futures_klines(interval="1d")` | index 5: volume |
| 昨日主动买入量 | 同上 | index 9: taker_buy_volume |
| 上一小时总成交量 | `futures_klines(interval="1h")` | index 5: volume |
| 上一小时主动买入量 | 同上 | index 9: taker_buy_volume |
| 上一小时收盘价 | 同上 | index 4: close |
| 下一小时开盘价 | 同上（limit=2 取第二根） | index 1: open |

**卖量计算方式**：`卖量 = 总成交量 - 主动买入量`（币安 K 线不直接提供主动卖出量，用减法推算）。

---

## 时间线示例

```
UTC 12:00   上一小时（11:00-12:00）K 线收盘
UTC 12:03   scan_loop 进入扫描窗口
            → 获取 300+ 交易对
            → 并发预热昨日缓存
            → 并发扫描每个交易对的 11:00 K 线
            → ETHUSDT 卖量 = 昨日均值的 15.3 倍 → 产生信号
            → 按倍数排序 → 尝试开仓
UTC 12:03~  server_open_position 执行风控 → 下单做空
UTC 13:03   下一轮扫描（检查 12:00-13:00 K 线）
```
