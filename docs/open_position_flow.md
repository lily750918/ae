# server_open_position 开仓逻辑文档

> 函数位置：`ae_server.py` → `AutoExchangeStrategy.server_open_position(signal)`
>
> 最后更新：2026-04-10

---

## 入参

`signal` 字典，由 `server_scan_sell_surge_signals()` 扫描产生，包含：

| 字段 | 说明 |
|------|------|
| `symbol` | 交易对，如 `ETHUSDT` |
| `price` | 信号发生时的价格 |
| `surge_ratio` | 卖量暴涨倍数 |
| `signal_time` | 信号发生的 UTC 时间 |

---

## 流程

### 第一阶段：并发锁

1. **Symbol 锁**（非阻塞）：获取该 symbol 的专属锁，拿不到说明同一币种正在开仓，直接跳过。
2. **全局建仓锁** `_entry_global_lock`（阻塞）：保证同一时刻只有一个线程在走建仓流程。

### 第二阶段：风控检查（5 道关卡）

3. **滑点检查**：信号价 vs 当前市价偏差 > 3% → 拒绝。
4. **持仓限制检查** `server_check_position_limits()` → 返回 `(passed, exchange_symbols)`：
   - 调用交易所 `futures_position_information()` 获取实际持仓（API 失败降级用内存）
   - 当前持仓数 ≥ `max_positions` → 拒绝
   - 今日建仓数 ≥ `max_daily_entries` → 拒绝
   - 小时限制开启 (`enable_hourly_entry_limit`) 且本小时已建仓 → 拒绝
   - 连续亏损冷却期内 → 拒绝
   - 同时返回交易所活跃持仓的 symbol 集合，供下一步去重使用
5. **BTC 昨日阳线风控**：昨日 BTC 日K 收盘 > 开盘 → 当日不开新仓。
6. **重复持仓检查**（双重保险，交易所优先）：
   - **第一层**：`symbol in exchange_symbols` — 以交易所实际持仓为准，防止内存与交易所不一致时重复开仓（如进程重启后内存为空）
   - **第二层**：检查内存 `self.positions` — 覆盖交易所 API 降级场景
7. **资金充足性检查**：余额 × `position_size_ratio` 算出投入金额，要求可用余额至少留 15% 余量。

### 第三阶段：计算仓位

8. 计算建仓金额 = 账户余额 × `position_size_ratio`（默认 5%）。
9. 计算数量 = (建仓金额 × 杠杆) / 当前价格。
10. 获取交易对精度规则（`exchange_info` 本地缓存）。
11. 按 `stepSize` 取整，检查 ≥ `minQty`。

### 第四阶段：交易所操作

12. 设置杠杆（默认 3x）。
13. 设置逐仓模式 `ISOLATED`（已是则忽略）。
14. 设置单向持仓模式（已是则忽略）。
15. **★ 市价做空下单**：`SELL MARKET`。
16. 取实际成交价 `avgPrice`（无则 fallback 到 ticker 价）。

### 第五阶段：本地记录

17. 构建 `position` 字典，包含：
    - `position_id`（UUID）
    - `symbol`、`direction=short`
    - `entry_price`（实际成交价）、`entry_time`
    - `quantity`、`position_value`、`surge_ratio`、`leverage`
    - `tp_pct=33%`（初始止盈）、`status=normal`
    - `order_id`（交易所订单号）
    - `tp_order_id=None`、`sl_order_id=None`（待第六阶段填充）
18. 加锁（`_positions_sync_lock`）写入内存 `self.positions` + `daily_entries++`。
19. 持久化到 `data/positions_record.json`（失败重试 3 次，全失败仅告警不影响仓位——交易所仓位优先，监控循环和重启时均可从交易所重新加载）。

### 第六阶段：创建止盈止损

20. 调用 `server_create_tp_sl_orders(position, symbol_info)`：
    - **止盈**：`entry_price × (1 - tp_pct / 100)`，初始 33%，算法单 `TAKE_PROFIT_MARKET`。
    - **止损**：`entry_price × (1 + stop_loss_pct / 100)`，默认 18%，算法单 `STOP_MARKET`。
    - 失败不影响开仓结果，监控循环（每 30 秒）会发现缺失并补挂。

> **第五、第六阶段顺序说明**：从交易所下单成功到止盈止损挂上，不管中间插什么操作都存在一个无 TP/SL 的时间窗口。第五阶段只是写内存 + 写文件，耗时几毫秒到几十毫秒，窗口可忽略。先记录再挂单的好处是：如果 TP/SL 创建失败，监控循环已经能看到这个持仓并自动补挂。

### 第七阶段：收尾

21. 打印建仓完成摘要日志（含价格、数量、金额、杠杆、TP/SL 价格）。
22. `notify_positions_changed()`（SSE 推送前端刷新）。
23. `return True`。

### 异常处理

- `BinanceAPIException` / `OSError` / `TimeoutError` / `Exception` → 日志 + `return False`。
- `finally`：保证全局建仓锁 + symbol 锁都释放。

---

## 关键参数

来自 `config.ini`：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `leverage` | 3 | 杠杆倍数 |
| `position_size_ratio` | 0.05 | 每仓占余额比例（5%） |
| `max_positions` | 5 | 最大同时持仓数 |
| `max_daily_entries` | 5 | 每日最大建仓次数 |
| `max_opens_per_scan` | 1 | 单次扫描最多开仓数 |
| `stop_loss_pct` | 18.0 | 止损百分比 |
| `strong_coin_tp_pct` | 33.0 | 初始止盈百分比（后续动态调整） |
| `medium_coin_tp_pct` | 21.0 | 中等强度止盈 |
| `weak_coin_tp_pct` | 11.0 | 弱势止盈 |

---

## 保障链

```
交易所仓位 → 内存列表 → 止盈止损订单 → 文件持久化
```

- **交易所仓位**：真金白银，最高优先级。
- **内存列表**：监控循环的工作依据，append/remove 均加锁。
- **止盈止损**：开仓后立即创建，失败由监控循环每 30 秒补挂；启动时 `server_check_and_recreate_missing_tp_sl` 也会对账补挂。
- **文件持久化**：进程重启时的恢复依据；丢了不致命，`server_load_existing_positions` 会从交易所重新加载。
