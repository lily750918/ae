# AE Server 运行逻辑说明

基于 `ae_server.py` 的 **Auto Exchange** 无图形界面服务：币安 U 本位合约、卖量暴涨做空策略，带 Flask Web 监控与 REST API。

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

> **注意**：`main()` 在启动时已各拉起一个 `scan_loop` / `monitor_loop`；`start_trading` 内会再各启动一对同名循环。一旦 `is_running` 为 `True`，可能存在 **两套扫描/监控线程同时工作**。若需单一逻辑实例，应在代码层合并为只启动一次或仅在 `main` 中置位 `is_running`。

---

## 3. 信号扫描（`scan_loop`）

- 使用 **UTC** 时间；在 **每小时第 3～4 分**（`3 <= minute < 5`）且该小时尚未扫描过时执行，避免同一小时重复扫。
- 刷新余额 → **`server_scan_sell_surge_signals()`** 获取候选 → 按规则排序后依次 **`server_open_position(signal)`**，受 `max_positions`、`max_daily_entries`、`max_opens_per_scan`、是否已持仓及风控（如当日买量倍数等）约束。
- 开仓侧要点：合约 **做空**、设杠杆、尝试 **逐仓 ISOLATED**、单向持仓；本地记录持仓并写 **`positions_record.json`**，并创建/维护止盈止损（与监控、对章逻辑配合）。

---

## 4. 持仓监控（`monitor_loop`）

- 约每 **30 秒** 一轮。
- 先执行 BTC 日级风控（与扫描链路一致的相关入口），再 **`server_monitor_positions()`**：
  - 与交易所对齐仓位（如清除交易所已平仓但本地残留的「幽灵持仓」）；
  - 检查是否触发平仓条件，必要时 **`server_close_position`**；
  - **止盈/止损**：先与交易所对账，再补挂缺失侧（见第 6 节）；
  - 止盈/止损互斥管理（一侧成交后取消另一侧等）；
  - 动态止盈窗口（如 2h / 12h）内更新交易所止盈单等。

---

## 5. Web 与配置

- **`/`**：监控页（HTTP Basic，默认用户/密码以启动日志为准）。
- **`/api/status`** 等接口返回 `running: is_running`、持仓数量、当日建仓次数等；另含 **`exchange_status`**（币安 U 本位 `futures_ping` 延迟、`futures_time` 服务器时间（若 SDK 支持）、未配置 API 或异常时的说明）。
- 策略参数主要来自 **`config.ini`** 的 `[STRATEGY]`、`[SIGNAL]`、`[RISK]`；BTC 日级部分开关写在代码内。

### BTC 昨日收涨 → 当日不开仓（UTC）

- 拉取 **BTCUSDT** 昨日日 K（`futures_klines` 1d），按 **UTC 日期** 判定「昨天」。  
- 若 **收盘价 > 开盘价**（昨日收涨），**不开新仓**（`server_open_position` 内拦截）；日志 / 拒绝原因中会写 **涨跌幅度 %**。  
- 代码：`check_btc_yesterday_yang_blocks_entry_live`；开关 **`enable_btc_yesterday_yang_no_new_entry`**（默认 `True`）。  
- 另：新 UTC 日若昨日收涨，可对程序管理的空仓做 **市价一刀切**（ **`enable_btc_yesterday_yang_flatten_at_open`**，默认 `True`）。

---

## 6. 止盈 / 止损：以交易所为准（近期改动）

策略**不**再仅凭本地 `tp_order_id` / `sl_order_id` 判断「是否已有单」，而是：

1. **`server_sync_tp_sl_ids_from_exchange(position)`**  
   - 调用 `futures_get_open_algo_orders(symbol)`，按 **止盈算法单类型**、**止损算法单类型**，且 **`side` 等于当前仓位的平仓方向**（与 `server_manage_tp_sl_orders` 一致）筛选。  
   - 多条时 **优先**与本地记录的 `algoId` 一致的那条，否则取一条代表。  
   - 将本地 ID **更新为与交易所一致**；交易所没有则置为 `None`（清除脏 ID）并保存 **`positions_record.json`**。  
   - **若拉单失败**：返回 `False`，**不修改本地**，避免网络错误时误清空。

2. **调用时机**  
   - **服务启动**：`server_check_and_recreate_missing_tp_sl` 对每个持仓先对账，再在交易所仍缺 TP 或 SL 时 **`server_create_tp_sl_orders`** 补挂。  
   - **监控循环**：`server_check_and_create_tp_sl` 先对账，再按需补挂。  
   - **平仓前**：`server_close_position` 内先对账，仅在交易所仍缺时尝试补挂，再进入取消算法单与平仓流程。

3. **`server_create_tp_sl_orders`**  
   - 仅对 **当前 `position` 上仍为空的 TP/SL** 下新单；空字符串 ID 视为未设置。

---

## 7. 每日报告（`daily_report_loop`）

- 同样依赖 **`is_running == True`** 才执行发送逻辑；按 UTC 日期与日界条件触发 **`send_daily_report()`**。

---

## 8. 本地文件与运行方式

| 文件 | 作用 |
|------|------|
| `config.ini` | API 与策略参数（勿将密钥提交到公开仓库） |
| `positions_record.json` | 本地持仓与订单 ID 等持久化 |

运行示例（需在项目目录或正确工作目录下，且已安装依赖）：

```bash
python ae_server.py
```

浏览器访问控制台打印的地址（如 `http://localhost:5002`），使用 **`POST /api/start_trading`** 开启自动交易（需已配置 API）。

---

## 9. 相关模块

- **`binance_api.py` / `binance_sdk.py`**：交易所封装（如使用）。
- **`hm1l/hm1l.py`**：回测与策略参考实现，与 AE 卖量暴涨逻辑对齐；服务器实盘以 `ae_server.py` 为准。
