# ae_server.py 拆分方案

> 当前状态：单文件 8300 行，所有逻辑混在一起
>
> 目标：按职责拆分为多个模块，保持运行行为不变

---

## 当前文件结构（按行号）

```
L1-55        imports
L56-115      日志配置、工具函数
L116-265     日报相关工具函数（日期标记、邮件发送）
L266-580     generate_daily_report()、send_daily_report()
L580-685     send_email_alert()
L686-810     YesterdayDataCache 类、BACKUP_SYMBOL_LIST
L811-1218    配置加载/校验/热更新、Binance 客户端创建
L1220-5365   ★ AutoExchangeStrategy 类（4145 行）
  L1220-1490   __init__（参数、锁、状态）
  L1494-1985   持仓加载/保存/清理
  L1986-2320   买量倍数计算、账户余额/状态
  L2329-2520   信号扫描（scan_sell_surge_signals）
  L2523-2660   持仓限制检查、资金检查
  L2692-2780   BTC 日线风控
  L2783-3098   ★ 开仓（server_open_position）
  L3099-3665   K线获取、订单查询、TP更新
  L3666-3992   动态止盈计算、退出条件检查
  L3993-4408   TP/SL 创建/同步/管理/对账
  L4409-4935   ★ 平仓（server_close_position）
  L4937-5365   ★ 监控循环主体（server_monitor_positions）
L5367-5440   Flask app 创建、认证、SSE、工具函数
L5441-7840   Flask API 端点（~2400 行）
L7846-7910   save_signal_records、api_signal_history
L7911-8060   scan_loop（后台线程）
L8059-8145   monitor_loop（后台线程）
L8145-8160   信号处理（SIGINT/SIGTERM）
L8159-8300   main()、daily_report_loop
```

---

## 拆分方案

### 目录结构

```
ae/
├── ae_server.py          # 入口：main()、信号处理、后台线程启动（~150 行）
├── config.py             # 配置加载/校验/热更新、Binance 客户端工厂（~450 行）
├── strategy/
│   ├── __init__.py       # 导出 AutoExchangeStrategy
│   ├── core.py           # __init__、参数、锁、状态、账户余额（~500 行）
│   ├── scanner.py        # 信号扫描：scan_sell_surge_signals + 买量倍数（~400 行）
│   ├── positions.py      # 持仓加载/保存/清理/prune（~500 行）
│   ├── entry.py          # 开仓：open_position + 限制检查 + 资金检查（~450 行）
│   ├── exit.py           # 平仓：close_position（~550 行）
│   ├── tp_sl.py          # 止盈止损：创建/同步/管理/对账/动态计算（~800 行）
│   ├── monitor.py        # monitor_positions 主循环体（~450 行）
│   └── risk.py           # BTC 日线风控、退出条件检查（~300 行）
├── web/
│   ├── __init__.py       # Flask app 创建、CORS、认证
│   ├── api.py            # REST API 端点（status/positions/close/tp_sl/...）（~1200 行）
│   ├── views.py          # 页面路由（index/params）+ SSE（~200 行）
│   └── logs.py           # 日志/交易历史/信号历史 查询端点（~600 行）
├── utils/
│   ├── __init__.py
│   ├── logging.py        # 日志配置、flush（~40 行）
│   ├── email.py          # 邮件发送（alert + daily report）（~150 行）
│   ├── orders.py         # 算法单工具函数（trigger_price/cancel/pick）（~100 行）
│   └── cache.py          # YesterdayDataCache（~100 行）
├── loops.py              # scan_loop、monitor_loop、daily_report_loop（~250 行）
├── templates/            # 不变
├── config.ini
├── config.ini.example
└── requirements.txt
```

### 拆分原则

1. **AutoExchangeStrategy 拆成多个 mixin 或组合模块**：`core.py` 定义基类（含 `__init__`），其他模块定义方法，通过 mixin 或直接在 `__init__.py` 中组合成完整类。
2. **全局变量集中管理**：`strategy`、`is_running`、`start_time` 等全局变量放在 `ae_server.py` 入口或一个共享的 `state.py` 中，其他模块 import 引用。
3. **Flask 端点拆成 Blueprint**：`web/api.py`、`web/views.py`、`web/logs.py` 各自注册 Blueprint，在 `web/__init__.py` 中创建 app 并注册。
4. **后台线程与入口分离**：`loops.py` 定义三个循环函数，`ae_server.py` 的 `main()` 负责启动线程和 Flask。

---

## 拆分顺序（建议分步执行，每步可独立验证）

| 步骤 | 内容 | 风险 |
|------|------|------|
| 1 | 抽出 `utils/`（logging、email、orders、cache） | 低：纯工具函数，无状态 |
| 2 | 抽出 `config.py` | 低：配置加载独立 |
| 3 | 抽出 `web/` (Flask Blueprint) | 中：需处理全局 strategy 引用 |
| 4 | 拆 `strategy/` 各模块 | 高：方法间互调密集，需仔细处理 self 引用 |
| 5 | 抽出 `loops.py` + 精简 `ae_server.py` 入口 | 低：逻辑简单 |

---

## 需要注意的点

### 循环依赖风险

`strategy/entry.py`（开仓）调用 `strategy/tp_sl.py`（创建止盈止损），`strategy/monitor.py` 又调用 `strategy/exit.py`（平仓）和 `strategy/tp_sl.py`。这些都是 `self.xxx()` 方法调用，只要最终组合成同一个类实例就没问题。

**推荐方式**：mixin 模式

```python
# strategy/core.py
class StrategyCore:
    def __init__(self, config): ...

# strategy/scanner.py
class ScannerMixin:
    def server_scan_sell_surge_signals(self): ...

# strategy/entry.py
class EntryMixin:
    def server_open_position(self, signal): ...

# strategy/__init__.py
class AutoExchangeStrategy(
    StrategyCore,
    ScannerMixin,
    EntryMixin,
    ExitMixin,
    TpSlMixin,
    MonitorMixin,
    RiskMixin,
    PositionsMixin,
):
    pass
```

### 全局变量

当前有这些跨模块共享的全局变量：

| 变量 | 使用位置 |
|------|----------|
| `strategy` | Flask 端点、loops、main |
| `is_running` | Flask 端点、loops、signal_handler |
| `start_time` | Flask 端点、report |
| `app` | Flask 相关 |

建议放在 `state.py` 中集中管理：

```python
# state.py
strategy = None
is_running = False
start_time = None
```

### 配置热更新

`_apply_normalized_editable_to_strategy` 和 `_reinit_strategy_binance_client_after_ini_change` 直接修改 strategy 实例属性，拆分后需确保它们能 import 到 strategy 引用。

### 测试

每步拆完后验证：
1. `python3 -m py_compile ae_server.py` — 语法检查
2. 启动后 `POST /api/start_trading` → 观察 scan_loop 正常执行
3. 检查 Web 页面和 API 端点正常响应

---

## 拆分前后对比

| | 拆分前 | 拆分后 |
|---|---|---|
| 文件数 | 1 | ~15 |
| 最大文件 | 8300 行 | ~1200 行（web/api.py） |
| 可读性 | 差，需要跳来跳去 | 按职责定位 |
| 测试难度 | 只能集成测试 | 可对单模块单元测试 |
| 改动风险 | 改一处可能影响全局 | 模块边界清晰 |
