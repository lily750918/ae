# AE Server 运行逻辑说明

基于 `ae_server.py` 的 **Auto Exchange** 无图形界面服务：币安 U 本位合约、卖量暴涨做空策略，带 Flask Web 监控与 REST API。

---

## 项目结构

```
ae/
├── ae_server.py            # 入口：Flask 端点 + main() + signal_handler
├── config.py               # 配置加载/校验/热更新、Binance 客户端工厂
├── state.py                # 全局共享状态（strategy, is_running, start_time）
├── loops.py                # 后台线程：scan_loop, monitor_loop, daily_report_loop
├── strategy/               # 策略核心（Mixin 组合模式）
│   ├── __init__.py         #   AutoExchangeStrategy = 所有 Mixin 组合
│   ├── core.py             #   StrategyCore: __init__、参数、日志工具
│   ├── scanner.py          #   ScannerMixin: 信号扫描、账户余额、exchange_info
│   ├── entry.py            #   EntryMixin: 开仓、持仓限制、资金检查、BTC 风控
│   ├── exit.py             #   ExitMixin: 平仓、退出条件检查
│   ├── tp_sl.py            #   TpSlMixin: 止盈止损创建/同步/动态调整/对账
│   ├── positions.py        #   PositionsMixin: 持仓加载/保存/清理
│   └── monitor.py          #   MonitorMixin: 监控循环主体
├── utils/                  # 工具模块
│   ├── logging_config.py   #   日志配置、flush
│   ├── email.py            #   邮件报警
│   ├── orders.py           #   算法单工具函数
│   ├── cache.py            #   昨日数据缓存、备用交易对列表
│   └── daily_report.py     #   日报生成与发送
├── templates/              # Flask 页面模板
│   ├── monitor.html        #   监控页
│   ├── params.html         #   参数编辑页
│   └── _nav.html           #   导航栏
├── config.ini              # 运行时配置（API 密钥 + 策略参数）
├── config.ini.example      # 配置模板
├── requirements.txt        # Python 依赖
└── docs/                   # 文档
    ├── open_position_flow.md   # 开仓流程详解
    ├── signal_generation.md    # 信号产生逻辑详解
    └── refactor_plan.md        # 拆分方案（已完成）
```

---

## 1. 启动阶段（`main()`）

1. 注册 `SIGINT` / `SIGTERM`，退出时将全局标志 `is_running` 置为 `False`。
2. 读取 **`config.ini`**（及环境变量中的 API 密钥；密钥未配置时进入 **仅界面模式**：可开 Web，不可下单/拉行情类接口）。
3. 构造 **`AutoExchangeStrategy`**：初始化币安客户端、策略参数、并从 **`positions_record.json`** 恢复本地持仓列表。
4. **若已配置 API**：
   - 同步账户余额；
   - **`server_check_and_recreate_missing_tp_sl()`**：对所有本地持仓按 **交易所开放算法单** 对账止盈/止损（见下文第 6 节），缺失则补挂；
   - 执行 BTC 日级风控入口（昨日阳线相关逻辑）。
5. 在后台线程启动 **Flask**（默认 `0.0.0.0:5002`），再启动三条后台循环线程：
   - `scan_loop`：信号扫描；
   - `monitor_loop`：持仓监控；
   - `daily_report_loop`：每日邮件报告。
6. 主线程周期休眠并打心跳日志。

---

## 2. 策略开关：`is_running`

- 全局变量 **`is_running`** 决定扫描、监控、日报线程是否执行实质逻辑；为 `False` 时这些线程仅短暂休眠后重试。
- **`is_running` 默认在命令行启动后仍为 `False`**。需要通过 Web 认证后调用 **`POST /api/start_trading`** 才会置为 `True`，策略才开始扫单与盯盘。
- **`POST /api/stop_trading`** 将 `is_running` 置为 `False`。

---

## 3. 信号扫描（`scan_loop` → `server_scan_sell_surge_signals`）

- 使用 **UTC** 时间；在 **每小时第 3～8 分**（`3 <= minute < 8`）且该小时尚未扫描过时执行，避免同一小时重复扫。
- 刷新余额 → **并发扫描所有 USDT 永续合约**（300+个交易对），计算每个币上一小时的卖量暴涨倍数。
- 信号判定：`卖量暴涨倍数 = 上一小时卖量 / 昨日平均小时卖量`，在 `[sell_surge_threshold, sell_surge_max]` 区间内视为有效信号。
- 额外风控：当日买量倍数落在危险区间（多空博弈）→ 过滤。
- 信号按倍数降序排列，依次尝试开仓，受 `max_opens_per_scan` 限制。
- 所有信号（无论是否开仓）写入 `signal_history.json` 用于回溯，保留最近 90 天。

> 详细文档：[docs/signal_generation.md](docs/signal_generation.md)

---

## 4. 开仓（`server_open_position`）

- 风控检查（5 道关卡）：滑点 → 持仓限制（以交易所为准） → BTC 昨日阳线 → 重复持仓检查（交易所优先 + 内存双重保险） → 资金充足性。
- 计算仓位：`余额 × position_size_ratio × leverage / 价格`，按 `stepSize` 取整。
- 交易所操作：设杠杆 → 设逐仓 → 市价做空。
- 本地记录：加锁写入内存 + 持久化文件（失败重试 3 次，全失败仅告警不影响仓位）。
- 创建止盈止损：失败不影响开仓结果，监控循环会补挂。

> 详细文档：[docs/open_position_flow.md](docs/open_position_flow.md)

---

## 5. 持仓监控（`monitor_loop` → `server_monitor_positions`）

- 约每 **30 秒** 一轮。
- 先执行 BTC 日级风控（与扫描链路一致的相关入口），再：
  - 与交易所对齐仓位（清除「幽灵持仓」）；
  - 检查是否触发平仓条件，必要时 **`server_close_position`**；
  - **止盈/止损**：先与交易所对账，再补挂缺失侧；
  - 止盈/止损互斥管理（一侧成交后取消另一侧等）；
  - 动态止盈窗口（如 2h / 12h）内更新交易所止盈单等。

---

## 6. 止盈 / 止损：以交易所为准

策略**不**再仅凭本地 `tp_order_id` / `sl_order_id` 判断「是否已有单」，而是：

1. **`server_sync_tp_sl_ids_from_exchange(position)`**
   - 调用 `futures_get_open_algo_orders(symbol)`，按止盈/止损算法单类型 + 平仓方向筛选。
   - 将本地 ID 更新为与交易所一致；交易所没有则置为 `None`。
   - 拉单失败不修改本地，避免网络错误时误清空。

2. **调用时机**
   - **服务启动**：对每个持仓先对账，再补挂缺失的 TP/SL。
   - **监控循环**：每轮先对账，再按需补挂。
   - **平仓前**：先对账，再取消算法单后平仓。

3. **`server_create_tp_sl_orders`**
   - 仅对当前 `position` 上仍为空的 TP/SL 下新单。

---

## 7. 每日报告（`daily_report_loop`）

- 依赖 **`is_running == True`** 才执行发送逻辑；按 UTC 日期与日界条件触发 **`send_daily_report()`**。

---

## 8. Web 与配置

- **`/`**：监控页（HTTP Basic，默认用户/密码以启动日志为准）。
- **`/api/status`** 等接口返回运行状态、持仓数量、当日建仓次数等。
- 策略参数主要来自 **`config.ini`** 的 `[STRATEGY]`、`[SIGNAL]`、`[RISK]`。

### BTC 昨日收涨 → 当日不开仓（UTC）

- 拉取 **BTCUSDT** 昨日日 K，按 **UTC 日期** 判定「昨天」。
- 若 **收盘价 > 开盘价**（昨日收涨），**不开新仓**。
- 新 UTC 日若昨日收涨，可对程序管理的空仓做 **市价一刀切**。

---

## 9. 本地文件与运行方式

| 文件 | 作用 |
|------|------|
| `config.ini` | API 与策略参数（勿将密钥提交到公开仓库） |
| `positions_record.json` | 本地持仓与订单 ID 等持久化 |
| `trade_history.json` | 交易历史记录 |
| `signal_history.json` | 信号扫描历史（保留 90 天） |

运行示例（需在项目目录下，且已安装依赖）：

```bash
pip install -r requirements.txt
python ae_server.py
```

浏览器访问控制台打印的地址（如 `http://localhost:5002`），使用 **`POST /api/start_trading`** 开启自动交易（需已配置 API）。

---

## 10. 保障链

```
交易所仓位 → 内存列表 → 止盈止损订单 → 文件持久化
```

- **交易所仓位**：真金白银，最高优先级。
- **内存列表**：监控循环的工作依据，append/remove 均加锁（`_positions_sync_lock`）。
- **止盈止损**：开仓后立即创建，失败由监控循环每 30 秒补挂；启动时也会对账补挂。
- **文件持久化**：进程重启时的恢复依据；丢了不致命，`server_load_existing_positions` 会从交易所重新加载。
