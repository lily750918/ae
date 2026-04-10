"""
AE Server - Auto Exchange 自动交易软件（服务器版本）
基于 ae.py 改造，去除 Tkinter GUI，添加 Flask Web API

核心功能：
- 无GUI后台运行
- Flask Web监控界面
- 完整的API接口（查看+操作）
- 支持远程控制（手动平仓、修改止盈止损等）

作者：量化交易助手
版本：v2.0 (Server Edition)
创建时间：2026-02-12
"""

from flask import (
    Flask,
    jsonify,
    request,
    render_template,
    Response,
    send_file,
    stream_with_context,
)
from flask_cors import CORS
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import threading
import time
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import csv
import io
import re
from collections import defaultdict
from logging.handlers import RotatingFileHandler
import requests
from requests.adapters import HTTPAdapter
from binance.client import Client
from binance.exceptions import BinanceAPIException
import os
import configparser
import tempfile
import signal
import sys
import glob
import smtplib
import uuid  # ✨ 用于生成持仓唯一ID
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ==================== 日志配置（从 utils 导入） ====================
from utils.logging_config import (
    log_dir,
    flush_logging_handlers,
    _log_asctime_local,
    _position_change_fmt_value,
)



# python-binance 共用同一 Session；扫描/预热使用 ThreadPoolExecutor(max_workers=20)，
# 而 urllib3 默认每主机 pool_maxsize=10，并发超过时会刷屏「Connection pool is full」。
# ==================== 配置模块（从 config.py 导入） ====================
from config import (
    _BINANCE_HTTP_POOL_MAX,
    _configure_binance_http_adapter,
    SCRIPT_DIR,
    CONFIG_INI_PATH,
    POSITIONS_RECORD_FILE,
    TRADE_HISTORY_FILE,
    SIGNAL_HISTORY_FILE,
    _signal_history_lock,
    load_config,
    _CONFIG_EDITABLE_KEYS,
    _config_editable_defaults_strings,
    _get_config_editable_dict,
    _parse_bool_incoming,
    _merge_editable_post,
    _validate_editable_merged,
    _atomic_write_config_ini,
    _STRATEGY_CONFIG_LOCK,
    _apply_normalized_editable_to_strategy,
    _strip_cred_plain,
    _resolve_binance_keys_for_effective_trading,
    _file_binance_keys,
    _key_tail_4,
    _create_binance_client,
    _sync_strategy_config_binance_from_parser,
    _reinit_strategy_binance_client_after_ini_change,
)


# ==================== 日报相关（从 utils/daily_report.py 导入） ====================
from utils.daily_report import (
    DAILY_REPORT_LAST_SENT_FILE,
    _daily_report_load_last_sent_date,
    _daily_report_save_last_sent_date,
    _seconds_sleep_until_after_next_utc_midnight,
    _fetch_futures_realized_pnl_window,
    generate_daily_report,
    send_daily_report,
)


# ==================== 策略核心类（从 strategy/ 导入） ====================
from strategy import AutoExchangeStrategy


# ==================== Flask Web服务 ====================
app = Flask(__name__)
CORS(app)  # 允许跨域
# 前端约每 10s 轮询 /api，默认会在终端刷屏打印 GET … 200
logging.getLogger("werkzeug").setLevel(logging.WARNING)
auth = HTTPBasicAuth()

# 🔐 用户认证配置
# 用户名和密码（可以从环境变量或配置文件读取）
users = {
    "admin": generate_password_hash("admin123")  # 固定密码admin123
}


@auth.verify_password
def verify_password(username, password):
    """验证用户名和密码"""
    if username in users and check_password_hash(users.get(username), password):
        return username
    return None


# 全局变量
strategy = None
is_running = False
start_time = None  # 系统启动时间
scan_thread = None
monitor_thread = None

# 持仓变更 SSE：开仓/平仓/幽灵仓清理/改 TP·SL 时推送，供监控页刷新（减少轮询 /api/positions）
_POSITION_SSE_LOCK = threading.Lock()
_POSITION_SSE_QUEUES: List[queue.Queue] = []
_LAST_POSITION_SSE_NOTIFY = 0.0


def notify_positions_changed() -> None:
    """通知所有已连接监控页：持仓列表或订单绑定已变，请拉取 /api/positions（1s 内合并多次）。"""
    global _LAST_POSITION_SSE_NOTIFY
    now = time.time()
    if now - _LAST_POSITION_SSE_NOTIFY < 1.0:
        return
    _LAST_POSITION_SSE_NOTIFY = now
    with _POSITION_SSE_LOCK:
        subs = list(_POSITION_SSE_QUEUES)
    payload = {"type": "positions_changed"}
    for q in subs:
        try:
            q.put_nowait(payload)
        except queue.Full:
            pass


# —— 敏感 POST 限流（内存滑动窗口，按 IP + 路径）——
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_EVENTS: Dict[Tuple[str, str], List[float]] = defaultdict(list)

_SENSITIVE_API_RATE_LIMITS: Dict[str, Tuple[int, float]] = {
    "/api/start_trading": (20, 60.0),
    "/api/stop_trading": (20, 60.0),
    "/api/manual_scan": (12, 60.0),
    "/api/close_position": (40, 60.0),
    "/api/update_tp_sl": (50, 60.0),
    "/api/cancel_order": (40, 60.0),
    "/api/send_daily_report": (10, 3600.0),
    "/api/config_editable": (20, 60.0),
    "/api/binance_credentials": (12, 60.0),
    "/api/binance_test": (20, 60.0),
}


def _request_client_ip() -> str:
    xff = request.headers.get("X-Forwarded-For", "") or request.headers.get(
        "X-Real-IP", ""
    )
    if xff:
        return xff.split(",")[0].strip()[:64]
    return (request.remote_addr or "unknown")[:64]


def _rate_limit_allow(bucket_key: str, max_calls: int, period_sec: float) -> bool:
    ip = _request_client_ip()
    key = (ip, bucket_key)
    now = time.time()
    cutoff = now - period_sec
    with _RATE_LIMIT_LOCK:
        lst = _RATE_LIMIT_EVENTS[key]
        while lst and lst[0] < cutoff:
            lst.pop(0)
        if len(lst) >= max_calls:
            return False
        lst.append(now)
        return True


@app.before_request
def _api_rate_limit_sensitive_posts():
    if request.method != "POST" or not request.path.startswith("/api"):
        return None
    spec = _SENSITIVE_API_RATE_LIMITS.get(request.path)
    if not spec:
        return None
    max_n, period = spec
    if not _rate_limit_allow(request.path, max_n, period):
        logging.warning("⚠️ API 限流: %s %s", _request_client_ip(), request.path)
        return jsonify({"success": False, "error": "请求过于频繁，请稍后再试"}), 429
    return None


# 未配置 API 时仍允许这些路径（由视图内自行返回说明或 400；其余 /api 返回 503 避免访问 None client）
_UI_ONLY_ALLOWED_API_PATHS = frozenset(
    {
        "/api/status",
        "/api/start_trading",
        "/api/stop_trading",
        "/api/manual_scan",
        "/api/trade_history",
        "/api/signal_history",
        "/api/trade_history_export",
        "/api/signal_history_export",
        "/api/daily_report_download",
        "/api/positions/stream",
        "/api/config_editable",
        "/api/binance_credentials",
        "/api/binance_test",
    }
)


@app.before_request
def _ui_only_block_data_apis():
    if not request.path.startswith("/api"):
        return None
    if request.path == "/api/health":
        return None
    if strategy is None:
        return None
    if getattr(strategy, "api_configured", True):
        return None
    if request.path in _UI_ONLY_ALLOWED_API_PATHS:
        return None
    return jsonify(
        {
            "success": False,
            "error": "仅界面模式：未配置 API 密钥。请在环境变量或 config.ini [BINANCE] 填写 api_key / api_secret 后重启。",
            "api_configured": False,
        }
    ), 503


# ==================== Web界面路由 ====================
@app.route("/")
@auth.login_required
def index():
    """主页 - Web监控界面"""
    return render_template("monitor.html")


@app.route("/params")
@auth.login_required
def params_page():
    """策略参数编辑页（写入 config.ini，并热应用到运行中的 strategy）。"""
    return render_template("params.html")


@app.route("/api/config_editable", methods=["GET", "POST"])
@auth.login_required
def api_config_editable():
    """读取/保存可编辑的 config.ini 段（不含 BINANCE）。"""
    if request.method == "GET":
        return jsonify({"success": True, "config": _get_config_editable_dict()})
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        body = {}
    merged = _merge_editable_post(body)
    normalized, err = _validate_editable_merged(merged)
    if err:
        return jsonify({"success": False, "error": err}), 400
    parser = configparser.ConfigParser()
    if os.path.exists(CONFIG_INI_PATH):
        parser.read(CONFIG_INI_PATH, encoding="utf-8")
    for sec, kv in normalized.items():
        if not parser.has_section(sec):
            parser.add_section(sec)
        for k, v in kv.items():
            parser.set(sec, k, v)
    try:
        _atomic_write_config_ini(parser)
    except Exception as e:
        logging.error(f"❌ 写入 config.ini 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

    applied = False
    global strategy
    if strategy is not None:
        try:
            with _STRATEGY_CONFIG_LOCK:
                _apply_normalized_editable_to_strategy(strategy, normalized)
            applied = True
            st = normalized["STRATEGY"]
            rk = normalized["RISK"]
            logging.info(
                "✅ 策略运行参数已热更新: 杠杆=%s 最大持仓=%s 止损%%=%s 24h涨幅平仓=%s",
                st.get("leverage"),
                st.get("max_positions"),
                rk.get("stop_loss_pct"),
                rk.get("enable_max_gain_24h_exit"),
            )
        except Exception as e:
            logging.error(f"❌ 热更新策略参数失败: {e}")
            return jsonify(
                {
                    "success": False,
                    "error": f"已写入 config.ini，但应用到内存失败: {e}",
                }
            ), 500
    else:
        logging.info("✅ config.ini 已更新（进程内 strategy 未初始化，下次启动后加载）")

    return jsonify(
        {
            "success": True,
            "applied_runtime": applied,
            "message": (
                "已保存并已应用到当前运行策略。已开仓位与交易所已挂止盈/止损单不会自动重算；若遇异常可重启进程以完全对齐。"
                if applied
                else "已保存到 config.ini。当前无运行中的策略实例，下次启动进程后将加载。"
            ),
        }
    )


@app.route("/api/binance_credentials", methods=["GET", "POST"])
@auth.login_required
def api_binance_credentials():
    """查询/保存 Binance API 密钥（仅写 config.ini；环境变量仍优先于文件）。"""
    cfg = load_config()
    fk, fs = _file_binance_keys(cfg)
    ek, es, src = _resolve_binance_keys_for_effective_trading(cfg)
    env_key = _strip_cred_plain(os.getenv("BINANCE_API_KEY"))
    env_sec = _strip_cred_plain(os.getenv("BINANCE_API_SECRET"))
    env_complete = bool(env_key and env_sec)

    if request.method == "GET":
        return jsonify(
            {
                "success": True,
                "effective_configured": bool(ek and es),
                "effective_source": src,
                "env_overrides_file": env_complete,
                "file_has_key": bool(fk),
                "file_has_secret": bool(fs),
                "key_masked": "********" if ek else "",
                "secret_masked": "********" if es else "",
                "key_tail": _key_tail_4(ek) if ek else "",
                "copyable_key_hint": (
                    f"当前 API Key 尾号（仅标识）: …{_key_tail_4(ek)}"
                    if ek and _key_tail_4(ek)
                    else ""
                ),
                "note": (
                    "当前密钥来自环境变量 BINANCE_API_*，config.ini 中的修改在取消环境变量前不会作为运行时生效来源。"
                    if env_complete
                    else ""
                ),
            }
        )

    body = request.get_json(silent=True) or {}
    k = _strip_cred_plain(body.get("api_key"))
    sec = _strip_cred_plain(body.get("api_secret"))
    if not k or not sec:
        return jsonify(
            {"success": False, "error": "api_key 与 api_secret 不能为空"}
        ), 400
    if len(k) < 8 or len(sec) < 8:
        return jsonify({"success": False, "error": "密钥长度过短"}), 400
    try:
        test_cl = _create_binance_client(k, sec)
        _configure_binance_http_adapter(getattr(test_cl, "session", None))
        t0 = time.time()
        test_cl.futures_ping()
        _ = (time.time() - t0) * 1000
    except Exception as e:
        return jsonify({"success": False, "error": f"保存前连通性校验失败: {e}"}), 400

    parser = configparser.ConfigParser()
    if os.path.exists(CONFIG_INI_PATH):
        parser.read(CONFIG_INI_PATH, encoding="utf-8")
    if not parser.has_section("BINANCE"):
        parser.add_section("BINANCE")
    parser.set("BINANCE", "api_key", k)
    parser.set("BINANCE", "api_secret", sec)
    try:
        _atomic_write_config_ini(parser)
    except Exception as e:
        logging.error(f"❌ 写入 Binance 密钥到 config.ini 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

    ok_re, err_re = _reinit_strategy_binance_client_after_ini_change()
    if not ok_re:
        logging.error(f"❌ 保存密钥后热重载客户端失败: {err_re}")
        return jsonify(
            {
                "success": False,
                "error": f"已写入 config.ini，但热重载交易客户端失败: {err_re}",
            }
        ), 500

    warn = ""
    if env_complete:
        warn = "已写入 config.ini。当前进程仍优先使用环境变量 BINANCE_API_*，运行时实际密钥未改变；若需使用新文件密钥请清空环境变量并重启或热重载。"

    logging.info("✅ Binance API 密钥已通过网页更新并写入 config.ini")
    return jsonify(
        {
            "success": True,
            "applied_runtime": not env_complete,
            "warning": warn,
            "message": (
                "已保存到 config.ini，并已尝试热重载 Binance 客户端。"
                if not warn
                else warn
            ),
        }
    )


@app.route("/api/binance_test", methods=["POST"])
@auth.login_required
def api_binance_test():
    """期货 API 连通测试；请求体可带 api_key/api_secret，省略则使用当前生效密钥（环境变量优先）。"""
    body = request.get_json(silent=True) or {}
    bk = _strip_cred_plain(body.get("api_key"))
    bs = _strip_cred_plain(body.get("api_secret"))
    if not bk or not bs:
        bk, bs, _src = _resolve_binance_keys_for_effective_trading()
    if not bk or not bs:
        return jsonify(
            {
                "success": False,
                "ok": False,
                "error": "未配置密钥：请在请求中传入 api_key / api_secret，或在 config.ini / 环境变量中配置",
            }
        ), 400
    try:
        cl = _create_binance_client(bk, bs)
        _configure_binance_http_adapter(getattr(cl, "session", None))
        t0 = time.time()
        cl.futures_ping()
        ms = (time.time() - t0) * 1000
        return jsonify(
            {
                "success": True,
                "ok": True,
                "latency_ms": round(ms, 1),
                "message": "U 本位期货 API 连通正常",
            }
        )
    except Exception as e:
        return jsonify(
            {
                "success": True,
                "ok": False,
                "message": str(e)[:300],
            }
        )


@app.route("/api/health")
def api_health():
    """探活：无需 Basic 认证、不调用交易所（供负载均衡 / 脚本检测）。"""
    api_ok = bool(strategy is not None and getattr(strategy, "api_configured", False))
    uptime_sec = None
    if start_time is not None:
        uptime_sec = int((datetime.now(timezone.utc) - start_time).total_seconds())
    return jsonify(
        {
            "ok": True,
            "service": "ae_server",
            "api_configured": api_ok,
            "trading": bool(is_running),
            "uptime_sec": uptime_sec,
        }
    )


# ==================== API接口 - 查看类 ====================
@app.route("/api/status")
@auth.login_required
def get_status():
    """获取系统状态"""
    try:
        if strategy is None:
            return jsonify({"error": "Strategy not initialized"}), 500

        # 获取详细账户信息
        account_info = strategy.server_get_account_info()

        # 今日统计
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_entries = (
            strategy.daily_entries if strategy.last_entry_date == today else 0
        )

        result = {
            "success": True,
            "api_configured": getattr(strategy, "api_configured", True),
            "running": is_running,
            "positions_count": len(strategy.positions),  # atomic len() on CPython — acceptable
            "today_entries": today_entries,
            "max_positions": strategy.max_positions,
            "max_daily_entries": strategy.max_daily_entries,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "exchange_status": strategy.server_get_exchange_status(),
        }

        # 添加详细账户信息
        if account_info:
            result.update(
                {
                    "total_balance": account_info["total_balance"],
                    "available_balance": account_info["available_balance"],
                    "unrealized_pnl": account_info["unrealized_pnl"],
                    "daily_pnl": account_info["daily_pnl"],
                }
            )
        else:
            # 降级：如果获取详细信息失败，使用简单余额
            balance = strategy.server_get_account_balance()
            strategy.account_balance = balance
            strategy.account_available_balance = balance
            result["balance"] = balance

        return jsonify(result)
    except Exception as e:
        logging.error(f"❌ 获取系统状态失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/funding_fee")
@auth.login_required
def get_funding_fee():
    """获取资金费历史"""
    try:
        days = int(request.args.get("days", 3))

        # 查询最近N天的资金费
        now = datetime.now(timezone.utc)
        start_time = int((now - timedelta(days=days)).timestamp() * 1000)

        income_history = strategy.client.futures_income_history(
            incomeType="FUNDING_FEE", startTime=start_time, limit=1000
        )

        # 按日期分组统计
        daily_fees = {}
        total_fee = 0

        for record in income_history:
            income = float(record["income"])
            timestamp = int(record["time"]) / 1000
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            date_str = dt.strftime("%Y-%m-%d")
            symbol = record["symbol"]

            if date_str not in daily_fees:
                daily_fees[date_str] = {"total": 0, "count": 0, "details": []}

            daily_fees[date_str]["total"] += income
            daily_fees[date_str]["count"] += 1
            daily_fees[date_str]["details"].append(
                {"time": dt.strftime("%H:%M UTC"), "symbol": symbol, "amount": income}
            )

            total_fee += income

        return jsonify(
            {
                "success": True,
                "days": days,
                "daily_fees": daily_fees,
                "total_fee": total_fee,
                "average_daily": total_fee / days if days > 0 else 0,
            }
        )
    except Exception as e:
        logging.error(f"❌ 获取资金费失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/positions")
@auth.login_required
def get_positions():
    """获取持仓详情"""
    try:
        if strategy is None:
            return jsonify({"error": "Strategy not initialized"}), 500

        # 获取币安持仓信息
        positions_info = strategy.client.futures_position_information()

        # 交易所已无仓位则从本地移除，页面与策略状态一致
        strategy.server_prune_flat_positions_from_exchange(positions_info)

        # 获取账户余额信息（用于计算仓位占比）
        account_balance = 0
        try:
            account_info = strategy.client.futures_account()
            account_balance = float(account_info.get("totalWalletBalance", 0))
        except Exception as e:
            logging.error(f"❌ 获取账户余额失败: {e}")
            account_balance = 0

        result = []
        with strategy._positions_sync_lock:
            positions_snapshot = strategy.positions[:]
        logging.debug(
            "get_positions: strategy.positions 长度=%d", len(positions_snapshot)
        )
        for pos in positions_snapshot:
            symbol = pos["symbol"]
            # 从交易所获取实时价格和盈亏
            binance_pos = next(
                (p for p in positions_info if p["symbol"] == symbol), None
            )
            try:
                _amt = (
                    float(binance_pos.get("positionAmt", 0) or 0)
                    if binance_pos
                    else 0.0
                )
            except (TypeError, ValueError):
                _amt = 0.0
            if abs(_amt) < 1e-8:
                logging.debug("⏭️ %s 交易所无持仓，跳过返回", symbol)
                continue

            logging.debug("get_positions 处理持仓: %s", pos.get("symbol", "UNKNOWN"))

            if binance_pos:
                mark_price = float(binance_pos["markPrice"])
                unrealized_pnl = float(binance_pos["unRealizedProfit"])
                # 根据仓位方向计算盈亏百分比
                direction = pos.get("direction", "short")  # 默认做空，保持向后兼容
                if direction == "long":
                    pnl_pct = (
                        (mark_price - pos["entry_price"]) / pos["entry_price"]
                    ) * 100  # 做多：价格上涨盈利
                else:
                    pnl_pct = (
                        (pos["entry_price"] - mark_price) / pos["entry_price"]
                    ) * 100  # 做空：价格下跌盈利
            else:
                # 如果交易所没有数据，用市价
                ticker = strategy.client.futures_symbol_ticker(symbol=symbol)
                mark_price = float(ticker["price"])
                # 根据仓位方向计算盈亏百分比
                direction = pos.get("direction", "short")  # 默认做空，保持向后兼容
                if direction == "long":
                    pnl_pct = (
                        (mark_price - pos["entry_price"]) / pos["entry_price"]
                    ) * 100  # 做多：价格上涨盈利
                else:
                    pnl_pct = (
                        (pos["entry_price"] - mark_price) / pos["entry_price"]
                    ) * 100  # 做空：价格下跌盈利
                unrealized_pnl = (
                    pnl_pct / 100 * pos["position_value"] * strategy.leverage
                )

            # 💰 计算新增字段
            leverage = int(pos.get("leverage", strategy.leverage))
            quantity = pos["quantity"]
            entry_price = pos["entry_price"]

            # 1. 持仓投入金额（保证金）= 持仓价值 / 杠杆
            position_margin = (quantity * entry_price) / leverage

            # 2. 当下金额（当前仓位价值）
            current_value = quantity * mark_price

            # 3. 仓位占比 = 投入金额 / 账户总余额 * 100%
            position_ratio = (
                (position_margin / account_balance * 100) if account_balance > 0 else 0
            )

            # 获取挂单（与下方 get_tp_sl 共用，避免每轮轮询同一 symbol 打两次算法单 API）
            try:
                algo_orders = strategy.client.futures_get_open_algo_orders(
                    symbol=symbol
                )
                orders = []
                for order in algo_orders:
                    orders.append(
                        {
                            "id": order.get("algoId", ""),
                            "type": order.get("orderType", ""),
                            "side": order.get("side", ""),
                            "price": float(order.get("triggerPrice", 0)),
                            "status": order.get(
                                "status", "ACTIVE"
                            ),  # 🔧 修复：status字段可能不存在
                        }
                    )
            except Exception as e:
                logging.error(f"❌ 查询 {symbol} 挂单失败: {e}")
                algo_orders = []
                orders = []

            # 计算持仓时间
            entry_time = datetime.fromisoformat(pos["entry_time"])
            elapsed_hours = (
                datetime.now(timezone.utc) - entry_time
            ).total_seconds() / 3600

            # 🔧 修复：算法单在 openAlgoOrders；普通条件单在 openOrders；最后合并本地 pos 缓存
            def get_tp_sl_for_position(symbol, pos_row, algo_os_cached):
                tp_price_val = "N/A"
                sl_price_val = "N/A"
                is_long = pos_row.get("direction") == "long"
                close_side = position_close_side(is_long)
                try:
                    algo_os = list(algo_os_cached or [])
                    logging.debug("🔍 %s 算法单: %d 条", symbol, len(algo_os))
                    for order in algo_os:
                        ot = order.get("orderType", "")
                        sd = order.get("side", "")
                        if sd != close_side:
                            continue
                        trig = futures_algo_trigger_price(order)
                        if trig is None:
                            continue
                        if ot in FUTURES_ALGO_TP_TYPES and tp_price_val == "N/A":
                            tp_price_val = f"{trig:.6f}"
                            logging.debug(
                                "✅ %s 算法单止盈 %s 触发价=%s",
                                symbol,
                                ot,
                                tp_price_val,
                            )
                        elif ot in FUTURES_ALGO_SL_TYPES and sl_price_val == "N/A":
                            sl_price_val = f"{trig:.6f}"
                            logging.debug(
                                "✅ %s 算法单止损 %s 触发价=%s",
                                symbol,
                                ot,
                                sl_price_val,
                            )
                except Exception as e:
                    logging.warning(f"⚠️ {symbol} 解析算法单失败: {e}")

                if tp_price_val == "N/A" or sl_price_val == "N/A":
                    try:
                        all_orders = strategy.client.futures_get_open_orders(
                            symbol=symbol
                        )
                        logging.debug("🔍 %s 普通挂单: %d 条", symbol, len(all_orders))
                        for order in all_orders:
                            order_type = order.get("type", "")
                            order_side = order.get("side", "")
                            if order_side != close_side:
                                continue
                            if (
                                order_type
                                in ("TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET")
                                and tp_price_val == "N/A"
                            ):
                                sp = order.get("stopPrice") or order.get("price")
                                if sp and float(sp) > 0:
                                    tp_price_val = f"{float(sp):.6f}"
                            elif order_type == "STOP_MARKET" and sl_price_val == "N/A":
                                sp = order.get("stopPrice")
                                if sp and float(sp) > 0:
                                    sl_price_val = f"{float(sp):.6f}"
                    except Exception as e:
                        logging.warning(f"⚠️ {symbol} 查询普通挂单失败: {e}")

                if tp_price_val == "N/A" and pos_row.get("tp_price") is not None:
                    try:
                        tp_price_val = f"{float(pos_row['tp_price']):.6f}"
                        logging.debug(
                            "✅ %s 使用本地缓存止盈价 %s", symbol, tp_price_val
                        )
                    except (TypeError, ValueError):
                        pass
                if sl_price_val == "N/A" and pos_row.get("sl_price") is not None:
                    try:
                        sl_price_val = f"{float(pos_row['sl_price']):.6f}"
                        logging.debug(
                            "✅ %s 使用本地缓存止损价 %s", symbol, sl_price_val
                        )
                    except (TypeError, ValueError):
                        pass

                logging.debug(
                    "📊 %s 止盈止损展示: TP=%s, SL=%s",
                    symbol,
                    tp_price_val,
                    sl_price_val,
                )
                return tp_price_val, sl_price_val

            tp_price, sl_price = get_tp_sl_for_position(symbol, pos, algo_orders)

            result.append(
                {
                    "position_id": pos.get(
                        "position_id", "N/A"
                    ),  # 添加position_id用于精确修改
                    "symbol": symbol,
                    "direction": pos.get("direction", "short"),  # 仓位方向
                    "entry_price": pos["entry_price"],
                    "entry_time": pos["entry_time"],
                    "quantity": pos["quantity"],
                    "mark_price": mark_price,
                    "pnl": unrealized_pnl,
                    "pnl_pct": pnl_pct,
                    "leverage": leverage,
                    "tp_pct": pos.get("tp_pct", strategy.strong_coin_tp_pct),
                    "orders": orders,
                    "elapsed_hours": elapsed_hours,
                    "tp_2h_checked": pos.get("tp_2h_checked", False),
                    "tp_12h_checked": pos.get("tp_12h_checked", False),
                    "is_consecutive": pos.get("is_consecutive_confirmed", False),
                    "dynamic_tp_strong": pos.get("dynamic_tp_strong", False),
                    "dynamic_tp_medium": pos.get("dynamic_tp_medium", False),
                    "dynamic_tp_weak": pos.get("dynamic_tp_weak", False),
                    "position_margin": position_margin,  # 持仓投入金额（保证金）
                    "current_value": current_value,  # 当下金额（当前仓位价值）
                    "position_ratio": position_ratio,  # 仓位占比（%）
                    "account_balance": account_balance,  # 账户总余额（用于前端显示）
                    "tp_price": tp_price,  # 止盈价格
                    "sl_price": sl_price,  # 止损价格
                }
            )

        logging.debug(
            "get_positions 返回: positions=%d account_balance=%s",
            len(result),
            account_balance,
        )

        return jsonify(
            {
                "success": True,
                "positions": result,
                "account_balance": account_balance,
            }
        )

    except Exception as e:
        logging.error(f"❌ 获取持仓失败: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/positions/stream")
@auth.login_required
def positions_stream():
    """SSE：后端持仓/订单有结构性变化时推送，前端再拉 /api/positions（监控循环仍在服务端运行）。"""

    def generate():
        q: queue.Queue = queue.Queue(maxsize=32)
        with _POSITION_SSE_LOCK:
            _POSITION_SSE_QUEUES.append(q)
        try:
            yield f"data: {json.dumps({'type': 'connected'})}\n\n"
            while True:
                try:
                    msg = q.get(timeout=25.0)
                    yield f"data: {json.dumps(msg)}\n\n"
                except queue.Empty:
                    yield ": ping\n\n"
        finally:
            with _POSITION_SSE_LOCK:
                try:
                    _POSITION_SSE_QUEUES.remove(q)
                except ValueError:
                    pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# 监控页 /api/logs 默认过滤：这些 INFO 行对看盘价值低、刷屏多（完整日志仍在 ae_server_*.log 文件中）
_LOG_MONITOR_NOISE_INFO_MARKERS = (
    "💓 系统运行中",
    "💰 账户余额:",  # 定时扫描前余额行，看盘价值低
    "👁️ [监控] 已检查",
    "🔄 exchange_info 缓存已刷新",
    "📦 开始并发预热昨日缓存",
    "📦 昨日缓存预热完成",
    "📌 ",  # 止盈止损 ID 与交易所对齐（高频）
    "💾 已保存持仓记录",
    "💾 已保存建仓记录到文件",
    "get_positions 返回",
    "get_positions:",
    "get_positions 处理持仓",
    "get_positions: strategy",
    "收到修改止盈止损请求",
    "解析参数: symbol=",
    "记录的订单ID",
    "请求参数 - TP价格",
    "当前订单[",
    "检查订单:",
    "重新查询后还有",
    "跳过已取消的订单",
    "预取消算法单",
    "预清理算法单",
    "从交易所获取实际持仓数量",
    "订单验证结果: TP验证",
    "算法单API响应",
    "创建止盈算法单:",
    "创建止损算法单:",
    "止盈 algoId/orderId",
    "止损算法单API响应",
    "普通订单查询结果",
    "当前普通订单:",
)


def _log_line_ok_for_monitor(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if " - DEBUG - " in s:
        return False
    if " - ERROR - " in s or " - CRITICAL - " in s:
        return True
    if " - WARNING - " in s:
        return True
    if " - INFO - " in s:
        for m in _LOG_MONITOR_NOISE_INFO_MARKERS:
            if m in s:
                return False
        return True
    # 非标准 logging 行（如多行异常的续行）不混入监控要点
    return False


# 监控页仓位段：仅展示 server_log_position_change 改版后的单行格式，忽略旧版多行块
_POSITION_CHANGE_LOG_STD_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - INFO - \[仓位变动\] "
)


def _position_changes_line_for_monitor(s: str) -> bool:
    return bool(_POSITION_CHANGE_LOG_STD_RE.match(s.strip()))


@app.route("/api/logs")
@auth.login_required
def get_logs():
    """获取最新日志。默认 monitor 模式：去掉高频噪音 INFO，保留 WARNING+ 与业务要点；?full=1 返回未过滤尾部。"""
    try:
        # 获取请求参数
        lines_count = request.args.get("lines", 100, type=int)
        lines_count = min(max(lines_count, 1), 500)  # 最多500行
        full_tail = request.args.get("full", 0, type=int) == 1

        log_files = glob.glob(os.path.join(log_dir, "ae_server_*.log"))
        latest_log = max(log_files, key=os.path.getmtime) if log_files else None

        last_lines: List[str] = []
        log_file_name = "no logs found"

        if latest_log:
            with open(latest_log, "r", encoding="utf-8") as f:
                main_logs = f.readlines()
            if full_tail:
                chunk = (
                    main_logs[-lines_count:]
                    if len(main_logs) > lines_count
                    else main_logs
                )
                last_lines = [ln.strip() for ln in reversed(chunk) if ln.strip()]
            else:
                # 多读一些再过滤；分桶保留足够多重要信息避免被 INFO 淹没
                raw_take = min(len(main_logs), max(lines_count * 40, 600))
                rev = list(reversed(main_logs[-raw_take:]))
                errs = []
                warns = []
                infos = []
                for i, ln in enumerate(rev):
                    st = ln.strip()
                    if not st:
                        continue
                    if " - ERROR - " in st or " - CRITICAL - " in st:
                        errs.append((i, st))
                    elif " - WARNING - " in st:
                        warns.append((i, st))
                    elif _log_line_ok_for_monitor(st):
                        infos.append((i, st))
                merged = (errs + warns + infos)[:lines_count]
                # 重新按原来的出现顺序（最新到最老）稳定排序，恢复时间线的连续性
                merged.sort(key=lambda x: x[0])
                last_lines = [item[1] for item in merged]
            log_file_name = os.path.basename(latest_log)

        position_snippet: List[str] = []
        position_log_file = os.path.join(log_dir, "position_changes.log")
        if os.path.exists(position_log_file):
            pos_want = min(24, lines_count)
            with open(position_log_file, "r", encoding="utf-8") as f:
                plines = f.readlines()
            if plines:
                scan_lines = max(pos_want * 80, 400)
                chunk = plines[-scan_lines:] if len(plines) > scan_lines else plines
                std_only = [
                    ln.strip() for ln in chunk if _position_changes_line_for_monitor(ln)
                ]
                tail_std = (
                    std_only[-pos_want:] if len(std_only) > pos_want else std_only
                )
                position_snippet = list(reversed(tail_std))
                if position_snippet:
                    if log_file_name == "no logs found":
                        log_file_name = (
                            f"position_changes.log(标准行{len(position_snippet)}条)"
                        )
                    else:
                        log_file_name = f"{log_file_name} | position_changes.log(标准行{len(position_snippet)}条)"

        out_lines: List[str] = []
        if position_snippet:
            out_lines.append("--- 仓位变动（仅单行标准格式，旧版多行块已忽略） ---")
            out_lines.extend(position_snippet)
            out_lines.append(
                "--- ae_server 主日志（要点"
                + ("，未过滤" if full_tail else "，已过滤噪音")
                + "）---"
            )
        out_lines.extend(last_lines)

        return jsonify(
            {
                "success": True,
                "logs": out_lines,
                "log_file": log_file_name,
                "log_view": "full" if full_tail else "monitor",
            }
        )

    except Exception as e:
        logging.error(f"❌ 获取日志失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs/search")
@auth.login_required
def search_logs():
    """搜索所有日志文件中的关键字"""
    try:
        keyword = request.args.get("keyword", "")
        date = request.args.get("date", "")  # 可选：只搜索特定日期，格式：YYYYMMDD
        max_results = request.args.get("max", 100, type=int)
        max_results = min(max_results, 500)  # 最多500条

        if not keyword:
            return jsonify({"error": "keyword参数必须提供"}), 400

        # 获取日志文件
        if date:
            # 只搜索指定日期的日志
            log_pattern = os.path.join(log_dir, f"ae_server_{date}_*.log")
        else:
            # 搜索所有日志
            log_pattern = os.path.join(log_dir, "ae_server_*.log")

        log_files = sorted(glob.glob(log_pattern), key=os.path.getmtime, reverse=True)

        if not log_files:
            return jsonify({"success": True, "results": [], "files_searched": 0})

        results = []
        files_searched = 0

        # 搜索日志文件
        for log_file in log_files:
            files_searched += 1
            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        if keyword in line:
                            results.append(
                                {
                                    "file": os.path.basename(log_file),
                                    "line": line_num,
                                    "content": line.strip(),
                                }
                            )

                            if len(results) >= max_results:
                                break
            except Exception as file_error:
                logging.warning(f"⚠️ 读取日志文件失败 {log_file}: {file_error}")

            if len(results) >= max_results:
                break

        return jsonify(
            {
                "success": True,
                "keyword": keyword,
                "results": results,
                "files_searched": files_searched,
                "total_found": len(results),
            }
        )

    except Exception as e:
        logging.error(f"❌ 搜索日志失败: {e}")
        return jsonify({"error": str(e)}), 500


# ==================== API接口 - 操作类 ====================
@app.route("/api/close_position", methods=["POST"])
@auth.login_required
def api_close_position():
    """手动平仓 - API端点"""
    try:
        if strategy is None:
            return jsonify({"error": "Strategy not initialized"}), 500

        data = request.json
        symbol = data["symbol"]

        # 查找持仓（快照查找，避免迭代时被其他线程修改列表）
        with strategy._positions_sync_lock:
            position = next((p for p in strategy.positions if p["symbol"] == symbol), None)

        if not position:
            return jsonify({"error": f"{symbol} not found"}), 404

        # 记录变动前状态
        before_state = {
            "持仓数量": position["quantity"],
            "建仓价格": position["entry_price"],
            "当前价格": strategy.client.futures_symbol_ticker(symbol=symbol)["price"],
            "未实现盈亏": position.get("pnl", 0),
        }

        # 执行平仓
        strategy.server_close_position(position, "manual_close")

        # 记录变动后状态
        after_state = {"持仓数量": 0, "状态": "已平仓"}

        # 统一日志记录
        strategy.server_log_position_change(
            "manual_close",
            symbol,
            {
                "操作人": "Web界面用户",
                "请求IP": request.remote_addr,
                "平仓原因": "手动平仓",
                "持仓ID": position.get("position_id", "未知")[:8],
            },
            before_state,
            after_state,
            success=True,
        )

        return jsonify({"success": True, "message": f"{symbol} 平仓成功"})

    except Exception as e:
        logging.error(f"❌ 手动平仓失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/update_tp_sl", methods=["POST"])
@auth.login_required
def update_tp_sl():
    """修改止盈止损（支持精确定位position_id，解决重复持仓问题）"""
    try:
        if strategy is None:
            return jsonify(
                {
                    "success": False,
                    "message": "止盈止损更新失败",
                    "error": "Strategy not initialized",
                }
            ), 500

        data = request.json
        logging.info(f"📋 收到修改止盈止损请求: {data}")  # 🔧 调试日志
        symbol = data.get("symbol")
        position_id = data.get("position_id")  # ✨ 新增：支持通过position_id精确定位
        tp_price = data.get("tp_price")  # 止盈价格
        sl_price = data.get("sl_price")  # 止损价格
        logging.info(
            f"🔍 解析参数: symbol={symbol}, position_id={position_id}, tp_price={tp_price}, sl_price={sl_price}"
        )  # 🔧 调试日志

        # ✨ 优先通过position_id查找（精确匹配）
        if position_id:
            with strategy._positions_sync_lock:
                position = next(
                    (p for p in strategy.positions if p.get("position_id") == position_id),
                    None,
                )
            if not position:
                return jsonify(
                    {
                        "success": False,
                        "message": "止盈止损更新失败",
                        "error": f"Position ID {position_id[:8]} not found",
                    }
                ), 404
            logging.info(
                f"🎯 通过position_id定位持仓: {position_id[:8]} ({position['symbol']})"
            )
        elif symbol:
            # 兼容旧版本：通过symbol查找（如有多个持仓会有歧义）
            with strategy._positions_sync_lock:
                matching_positions = [
                    p for p in strategy.positions if p["symbol"] == symbol
                ]
            if not matching_positions:
                return jsonify(
                    {
                        "success": False,
                        "message": "止盈止损更新失败",
                        "error": f"{symbol} not found",
                    }
                ), 404
            if len(matching_positions) > 1:
                logging.warning(
                    f"⚠️ {symbol} 发现{len(matching_positions)}个持仓，建议使用position_id参数精确定位"
                )
                # 返回所有持仓的ID供用户选择
                positions_info = [
                    {
                        "position_id": p.get("position_id", "未知")[:8],
                        "entry_price": p["entry_price"],
                        "entry_time": p["entry_time"],
                        "quantity": p["quantity"],
                    }
                    for p in matching_positions
                ]
                return jsonify(
                    {
                        "success": False,
                        "message": "止盈止损更新失败",
                        "error": f"{symbol} 存在多个持仓，请使用position_id参数指定",
                        "positions": positions_info,
                    }
                ), 400
            position = matching_positions[0]
        else:
            return jsonify(
                {
                    "success": False,
                    "message": "止盈止损更新失败",
                    "error": "必须提供symbol或position_id参数",
                }
            ), 400

        entry_price = position["entry_price"]
        symbol = position["symbol"]

        # 🔧 从交易所获取实际持仓数量（避免数量不一致问题）
        try:
            positions_info = strategy.client.futures_position_information(symbol=symbol)
            actual_position = next(
                (p for p in positions_info if p["symbol"] == symbol), None
            )

            if actual_position:
                actual_amt = float(actual_position["positionAmt"])
                quantity = abs(actual_amt)  # 取绝对值作为订单数量
                is_long_position = actual_amt > 0
                logging.info(
                    f"📊 {symbol} 从交易所获取实际持仓数量: {quantity} (方向: {'做多' if is_long_position else '做空'}, 记录数量: {position['quantity']})"
                )
            else:
                quantity = position["quantity"]
                is_long_position = False
                logging.warning(
                    f"⚠️ {symbol} 无法获取实际持仓，使用程序记录数量: {quantity}"
                )
        except Exception as get_position_error:
            quantity = position["quantity"]
            is_long_position = False
            logging.warning(
                f"⚠️ {symbol} 获取实际持仓失败: {get_position_error}，使用程序记录数量: {quantity}"
            )

        # 🆕 记录修改请求的详细信息
        logging.info(f"""
╔════════════════════════════════════════════════════════════════════════════╗
║ 🔧 Web界面修改止盈止损请求
╠════════════════════════════════════════════════════════════════════════════╣
║ 交易对: {symbol}
║ Position ID: {position.get("position_id", "N/A")[:8]}
║ 建仓价格: ${entry_price:.6f}
║ 请求来源IP: {request.remote_addr}
║ 请求参数:
║   - 止盈价格: {f"${float(tp_price):.6f}" if tp_price else "❌ 不修改"}
║   - 止损价格: {f"${float(sl_price):.6f}" if sl_price else "❌ 不修改"}
║ 当前订单ID:
║   - 止盈订单: {position.get("tp_order_id", "N/A")}
║   - 止损订单: {position.get("sl_order_id", "N/A")}
╚════════════════════════════════════════════════════════════════════════════╝
""")

        # ✨ 取消现有订单（使用记录的订单ID精确取消）
        old_tp_id = position.get("tp_order_id")
        old_sl_id = position.get("sl_order_id")

        # 🔧 定义订单方向（用于取消逻辑）
        tp_side = "SELL" if is_long_position else "BUY"
        sl_side = "SELL" if is_long_position else "BUY"

        try:
            # 预清理算法单（止盈/止损在 openAlgoOrders，记录的 ID 常为 algoId）
            cs_pre = position_close_side(is_long_position)
            try:
                _aos = strategy.client.futures_get_open_algo_orders(symbol=symbol)
                for ao in _aos:
                    if ao.get("side") != cs_pre:
                        continue
                    aid = ao.get("algoId")
                    if not aid:
                        continue
                    ot = ao.get("orderType", "")
                    if (tp_price and ot in FUTURES_ALGO_TP_TYPES) or (
                        sl_price and ot in FUTURES_ALGO_SL_TYPES
                    ):
                        try:
                            strategy.client.futures_cancel_algo_order(
                                symbol=symbol, algoId=aid
                            )
                            logging.info(f"🧹 {symbol} 预取消算法单 {ot} algoId={aid}")
                        except Exception as _cx:
                            logging.debug(f"预取消算法单 {aid}: {_cx}")
            except Exception as _pre_err:
                logging.warning(f"⚠️ {symbol} 预清理算法单失败: {_pre_err}")

            # 🔧 修复：智能取消订单逻辑 - 只取消属于当前持仓的订单
            all_orders = strategy.client.futures_get_open_orders(symbol=symbol)
            tp_order_count = len(
                [
                    o
                    for o in all_orders
                    if o.get("type") in ("TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET")
                ]
            )
            sl_order_count = len([o for o in all_orders if o["type"] == "STOP_MARKET"])
            logging.info(
                f"📋 {symbol} 当前普通订单: 止盈×{tp_order_count}, 止损×{sl_order_count}"
            )

            cancelled_tp_orders = []
            cancelled_sl_orders = []

            # 步骤1：优先通过记录的订单ID取消
            recorded_tp_id = position.get("tp_order_id")
            recorded_sl_id = position.get("sl_order_id")

            logging.info(
                f"🔍 {symbol} 记录的订单ID - TP: {recorded_tp_id}, SL: {recorded_sl_id}"
            )
            logging.info(
                f"🔍 {symbol} 请求参数 - TP价格: {tp_price}, SL价格: {sl_price}"
            )

            # 记录所有当前订单的详细信息
            for i, order in enumerate(all_orders):
                logging.info(
                    f"🔍 {symbol} 当前订单[{i}]: ID={order.get('orderId')}, 类型={order.get('type')}, 方向={order.get('side')}, 价格={order.get('stopPrice')}, 数量={order.get('origQty')}"
                )

            if recorded_tp_id and tp_price:
                logging.info(f"🔍 {symbol} 尝试取消记录的止盈订单ID: {recorded_tp_id}")
                if cancel_order_algo_or_regular(
                    strategy.client, symbol, str(recorded_tp_id)
                ):
                    cancelled_tp_orders.append(str(recorded_tp_id))
                    logging.info(
                        f"✅ {symbol} 已取消记录的止盈订单 (ID: {recorded_tp_id})"
                    )
                else:
                    logging.warning(
                        f"⚠️ {symbol} 按记录ID取消止盈失败，将尝试智能匹配: {recorded_tp_id}"
                    )

            if recorded_sl_id and sl_price:
                logging.info(f"🔍 {symbol} 尝试取消记录的止损订单ID: {recorded_sl_id}")
                if cancel_order_algo_or_regular(
                    strategy.client, symbol, str(recorded_sl_id)
                ):
                    cancelled_sl_orders.append(str(recorded_sl_id))
                    logging.info(
                        f"✅ {symbol} 已取消记录的止损订单 (ID: {recorded_sl_id})"
                    )
                else:
                    logging.warning(
                        f"⚠️ {symbol} 按记录ID取消止损失败，将尝试智能匹配: {recorded_sl_id}"
                    )

            # 步骤2：通过智能匹配取消剩余的订单（防止重复订单）
            # 重新查询订单（因为上面可能取消了一些）
            all_orders = strategy.client.futures_get_open_orders(symbol=symbol)
            logging.info(f"🔍 {symbol} 重新查询后还有 {len(all_orders)} 个订单")

            for order in all_orders:
                order_id = str(order.get("orderId"))
                order_type = order.get("type", "")
                order_side = order.get("side", "")
                order_qty = float(order.get("origQty", 0))
                order_stop_price = order.get("stopPrice")

                logging.info(
                    f"🔍 {symbol} 检查订单: ID={order_id}, 类型={order_type}, 方向={order_side}, 价格={order_stop_price}, 数量={order_qty}"
                )

                # 跳过已取消的订单
                if (order_id in cancelled_tp_orders) or (
                    order_id in cancelled_sl_orders
                ):
                    logging.info(f"⏭️ {symbol} 跳过已取消的订单: {order_id}")
                    continue

                should_cancel = False
                cancel_reason = ""

                # 智能匹配：止盈订单
                if tp_price:
                    logging.debug(
                        f"🔍 {symbol} 检查止盈订单匹配: tp_price={tp_price}, order_type={order_type}, order_side={order_side}, tp_side={tp_side}"
                    )
                    if order_type in ("TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET"):
                        if order_side == tp_side:
                            # 检查数量是否匹配（应该等于持仓数量）
                            qty_diff = abs(order_qty - quantity)
                            logging.debug(
                                f"🔍 {symbol} 数量检查: 订单数量{order_qty}, 持仓数量{quantity}, 差异{qty_diff}"
                            )
                            if qty_diff < 0.001:  # 允许小误差
                                should_cancel = True
                                cancel_reason = (
                                    f"更新止盈（数量匹配: {order_qty} ≈ {quantity}）"
                                )
                                logging.info(
                                    f"🎯 {symbol} 找到需要取消的止盈订单: {order_id} ({order_stop_price})"
                                )
                            else:
                                logging.info(
                                    f"⏭️ {symbol} 止盈订单数量不匹配: {order_qty} ≠ {quantity} (差异: {qty_diff})"
                                )
                        else:
                            logging.debug(
                                f"⏭️ {symbol} 止盈订单方向不匹配: {order_side} ≠ {tp_side}"
                            )
                    else:
                        logging.debug(f"⏭️ {symbol} 不是止盈订单: {order_type}")

                # 智能匹配：止损订单
                if sl_price:
                    logging.debug(
                        f"🔍 {symbol} 检查止损订单匹配: sl_price={sl_price}, order_type={order_type}, order_side={order_side}, sl_side={sl_side}"
                    )
                    if order_type == "STOP_MARKET":
                        if order_side == sl_side:
                            # 检查数量是否匹配（应该等于持仓数量）
                            qty_diff = abs(order_qty - quantity)
                            logging.debug(
                                f"🔍 {symbol} 数量检查: 订单数量{order_qty}, 持仓数量{quantity}, 差异{qty_diff}"
                            )
                            if qty_diff < 0.001:  # 允许小误差
                                should_cancel = True
                                cancel_reason = (
                                    f"更新止损（数量匹配: {order_qty} ≈ {quantity}）"
                                )
                                logging.info(
                                    f"🎯 {symbol} 找到需要取消的止损订单: {order_id} ({order_stop_price})"
                                )
                            else:
                                logging.info(
                                    f"⏭️ {symbol} 止损订单数量不匹配: {order_qty} ≠ {quantity} (差异: {qty_diff})"
                                )
                        else:
                            logging.debug(
                                f"⏭️ {symbol} 止损订单方向不匹配: {order_side} ≠ {sl_side}"
                            )
                    else:
                        logging.debug(f"⏭️ {symbol} 不是止损订单: {order_type}")

                if should_cancel:
                    try:
                        logging.info(
                            f"🚀 {symbol} 正在取消订单: {order_type} {order_id} ({cancel_reason})"
                        )
                        cancel_result = strategy.client.futures_cancel_order(
                            symbol=symbol, orderId=order["orderId"]
                        )
                        logging.info(f"📋 {symbol} 取消API响应: {cancel_result}")
                        logging.info(
                            f"✅ {symbol} 已取消订单: {order_type} (ID: {order_id}, 原因: {cancel_reason})"
                        )
                        if order_type in ("TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET"):
                            cancelled_tp_orders.append(order_id)
                        elif order_type == "STOP_MARKET":
                            cancelled_sl_orders.append(order_id)
                    except Exception as cancel_error:
                        logging.error(
                            f"❌ {symbol} 取消订单失败 (ID: {order_id}): {cancel_error}"
                        )
                else:
                    logging.debug(f"⏭️ {symbol} 订单 {order_id} 不需要取消")

            logging.info(
                f"📊 {symbol} 取消完成: 止盈订单×{len(cancelled_tp_orders)}, 止损订单×{len(cancelled_sl_orders)}"
            )

            # 🔧 备用方案：如果智能匹配仍然失败，尝试更宽松的取消策略
            if tp_price and len(cancelled_tp_orders) == 0:
                logging.warning(f"⚠️ {symbol} 止盈订单取消失败，尝试更宽松的策略")
                # 取消所有 TAKE_PROFIT 订单（不管方向和数量）
                for order in all_orders:
                    if (
                        order.get("type") in ("TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET")
                        and str(order.get("orderId")) not in cancelled_tp_orders
                    ):
                        try:
                            logging.info(
                                f"🚨 {symbol} 宽松取消止盈订单: {order.get('orderId')} (类型: {order.get('type')}, 方向: {order.get('side')}, 数量: {order.get('origQty')})"
                            )
                            cancel_result = strategy.client.futures_cancel_order(
                                symbol=symbol, orderId=order["orderId"]
                            )
                            logging.info(
                                f"✅ {symbol} 宽松取消成功: {order.get('orderId')}"
                            )
                            cancelled_tp_orders.append(str(order.get("orderId")))
                        except Exception as e:
                            logging.error(
                                f"❌ {symbol} 宽松取消失败: {order.get('orderId')} - {e}"
                            )

            if sl_price and len(cancelled_sl_orders) == 0:
                logging.warning(f"⚠️ {symbol} 止损订单取消失败，尝试更宽松的策略")
                # 取消所有 STOP_LOSS 订单（不管方向和数量）
                for order in all_orders:
                    if (
                        order.get("type") == "STOP_MARKET"
                        and str(order.get("orderId")) not in cancelled_sl_orders
                    ):
                        try:
                            logging.info(
                                f"🚨 {symbol} 宽松取消止损订单: {order.get('orderId')} (类型: {order.get('type')}, 方向: {order.get('side')}, 数量: {order.get('origQty')})"
                            )
                            cancel_result = strategy.client.futures_cancel_order(
                                symbol=symbol, orderId=order["orderId"]
                            )
                            logging.info(
                                f"✅ {symbol} 宽松取消成功: {order.get('orderId')}"
                            )
                            cancelled_sl_orders.append(str(order.get("orderId")))
                        except Exception as e:
                            logging.error(
                                f"❌ {symbol} 宽松取消失败: {order.get('orderId')} - {e}"
                            )

            logging.info(
                f"📊 {symbol} 最终取消统计: 止盈订单×{len(cancelled_tp_orders)}, 止损订单×{len(cancelled_sl_orders)}"
            )

        except Exception as query_error:
            logging.warning(f"⚠️ {symbol} 查询订单失败: {query_error}")

        from decimal import Decimal, ROUND_HALF_UP

        # 🔧 动态获取价格精度（修复COMPUSDT、LPTUSDT等币种的精度错误）
        try:
            exchange_info = strategy.client.futures_exchange_info()
            symbol_info = next(
                (s for s in exchange_info["symbols"] if s["symbol"] == symbol), None
            )

            if symbol_info:
                price_filter = next(
                    (
                        f
                        for f in symbol_info["filters"]
                        if f["filterType"] == "PRICE_FILTER"
                    ),
                    None,
                )
                if price_filter:
                    # 直接从原始字符串获取tick_size，避免浮点精度损失
                    tick_size_str = price_filter["tickSize"].rstrip("0")
                    tick_size = float(tick_size_str)
                    if "." in tick_size_str:
                        price_precision = len(tick_size_str.split(".")[-1])
                    else:
                        price_precision = 0
                    logging.info(
                        f"📏 {symbol} 价格精度: tickSize={tick_size_str}, precision={price_precision}"
                    )
                else:
                    tick_size = Decimal("0.000001")
                    price_precision = 6
            else:
                tick_size = Decimal("0.000001")
                price_precision = 6
        except (ValueError, TypeError, KeyError) as e:
            logging.debug(f"获取价格精度失败: {e}")
            tick_size = Decimal("0.000001")
            price_precision = 6

        tick_size_decimal = (
            Decimal(str(tick_size)) if isinstance(tick_size, float) else tick_size
        )
        tsf = float(tick_size_decimal)

        # 设置新的止盈订单（算法单 TAKE_PROFIT_MARKET）
        new_tp_order_id = None
        tp_order_validated = False
        if tp_price:
            try:
                tp_price_decimal = Decimal(str(tp_price))
                tp_price_adjusted = float(
                    tp_price_decimal.quantize(tick_size_decimal, rounding=ROUND_HALF_UP)
                )
                tp_side = position_close_side(is_long_position)
                tp_trig = str(
                    Decimal(str(tp_price_adjusted)).quantize(
                        tick_size_decimal, rounding=ROUND_HALF_UP
                    )
                )

                logging.info(
                    f"🚀 {symbol} 创建止盈算法单: TAKE_PROFIT_MARKET side={tp_side} trigger={tp_trig}"
                )
                tp_order = strategy.client.futures_create_algo_order(
                    symbol=symbol,
                    side=tp_side,
                    type="TAKE_PROFIT_MARKET",
                    triggerPrice=tp_trig,
                    algoType="CONDITIONAL",
                    closePosition=True,
                    workingType="CONTRACT_PRICE",
                    priceProtect="true",
                )
                logging.info(f"📋 {symbol} 止盈算法单API响应: {tp_order}")

                api_order_id = str(
                    tp_order.get("algoId") or tp_order.get("orderId") or ""
                )
                logging.info(f"📋 {symbol} 止盈 algoId/orderId: {api_order_id}")

                if api_order_id and api_order_id.strip():
                    tp_order_validated = True
                    new_tp_order_id = api_order_id
                    logging.info(f"✅ {symbol} 止盈算法单创建成功: {new_tp_order_id}")
                else:
                    tp_order_validated = False
                    new_tp_order_id = ""
                    logging.warning(
                        f"⚠️ {symbol} 止盈API未返回ID，尝试在算法单列表中验证"
                    )
                    try:
                        time.sleep(0.5)
                        aos = strategy.client.futures_get_open_algo_orders(
                            symbol=symbol
                        )
                        for order in aos:
                            if (
                                order.get("orderType") not in FUTURES_ALGO_TP_TYPES
                                or order.get("side") != tp_side
                            ):
                                continue
                            trig = futures_algo_trigger_price(order)
                            if (
                                trig is not None
                                and abs(trig - tp_price_adjusted) < tsf * 2
                            ):
                                tp_order_validated = True
                                new_tp_order_id = str(order.get("algoId") or "")
                                logging.info(
                                    f"✅ {symbol} 止盈验证成功 algoId={new_tp_order_id} 价={trig}"
                                )
                                break
                    except Exception as e:
                        logging.warning(f"⚠️ {symbol} 止盈验证异常: {e}")

                if tp_order_validated:
                    logging.info(f"✅ {symbol} 止盈确认: 订单ID: {new_tp_order_id}")
                else:
                    logging.warning(f"⚠️ {symbol} 止盈可能未在交易所可见，请检查日志")

                position["tp_order_id"] = new_tp_order_id
                position["tp_price"] = tp_price_adjusted
                tp_pct = abs((entry_price - tp_price_adjusted) / entry_price * 100)
                position["tp_pct"] = tp_pct
                logging.info(
                    f"✅ {symbol} 止盈已更新: {tp_price_adjusted} ({tp_pct:.1f}%), ID: {new_tp_order_id}"
                )
            except Exception as e:
                logging.error(f"❌ {symbol} 设置止盈失败: {e}")
                raise Exception(f"设置止盈失败: {e}")

        # 设置新的止损订单（算法单 STOP_MARKET）
        new_sl_order_id = None
        sl_order_validated = False
        if sl_price:
            try:
                sl_price_decimal = Decimal(str(sl_price))
                sl_price_adjusted = float(
                    sl_price_decimal.quantize(tick_size_decimal, rounding=ROUND_HALF_UP)
                )
                sl_side = position_close_side(is_long_position)
                sl_trig = str(
                    Decimal(str(sl_price_adjusted)).quantize(
                        tick_size_decimal, rounding=ROUND_HALF_UP
                    )
                )

                logging.info(
                    f"🚀 {symbol} 创建止损算法单: STOP_MARKET side={sl_side} trigger={sl_trig}"
                )
                sl_order = strategy.client.futures_create_algo_order(
                    symbol=symbol,
                    side=sl_side,
                    type="STOP_MARKET",
                    triggerPrice=sl_trig,
                    algoType="CONDITIONAL",
                    closePosition=True,
                    workingType="CONTRACT_PRICE",
                    priceProtect="true",
                )
                logging.info(f"📋 {symbol} 止损算法单API响应: {sl_order}")

                api_order_id = str(
                    sl_order.get("algoId") or sl_order.get("orderId") or ""
                )
                if api_order_id and api_order_id.strip():
                    sl_order_validated = True
                    new_sl_order_id = api_order_id
                    logging.info(f"✅ {symbol} 止损算法单创建成功: {new_sl_order_id}")
                else:
                    sl_order_validated = False
                    new_sl_order_id = ""
                    logging.warning(
                        f"⚠️ {symbol} 止损API未返回ID，尝试在算法单列表中验证"
                    )
                    try:
                        time.sleep(0.5)
                        aos = strategy.client.futures_get_open_algo_orders(
                            symbol=symbol
                        )
                        for order in aos:
                            if (
                                order.get("orderType") not in FUTURES_ALGO_SL_TYPES
                                or order.get("side") != sl_side
                            ):
                                continue
                            trig = futures_algo_trigger_price(order)
                            if (
                                trig is not None
                                and abs(trig - sl_price_adjusted) < tsf * 2
                            ):
                                sl_order_validated = True
                                new_sl_order_id = str(order.get("algoId") or "")
                                logging.info(
                                    f"✅ {symbol} 止损验证成功 algoId={new_sl_order_id} 价={trig}"
                                )
                                break
                    except Exception as e:
                        logging.warning(f"⚠️ {symbol} 止损验证异常: {e}")

                if sl_order_validated:
                    logging.info(f"✅ {symbol} 止损确认: 订单ID: {new_sl_order_id}")
                else:
                    logging.warning(f"⚠️ {symbol} 止损可能未在交易所可见，请检查日志")

                position["sl_order_id"] = new_sl_order_id
                position["sl_price"] = sl_price_adjusted
                logging.info(
                    f"✅ {symbol} 止损已更新: {sl_price_adjusted}, ID: {new_sl_order_id}"
                )
            except Exception as e:
                logging.error(f"❌ {symbol} 设置止损失败: {e}")
                raise Exception(f"设置止损失败: {e}")

        # 记录变动前状态
        before_state = {
            "止盈价格": position.get("tp_price", "无"),
            "止损价格": position.get("sl_price", "无"),
        }

        # 记录变动后状态
        after_state = {}
        if tp_price:
            after_state["止盈价格"] = (
                tp_price_adjusted if "tp_price_adjusted" in locals() else tp_price
            )
        if sl_price:
            after_state["止损价格"] = (
                sl_price_adjusted if "sl_price_adjusted" in locals() else sl_price
            )

        # 统一日志记录
        details = {
            "操作人": "Web界面用户",
            "请求IP": request.remote_addr,
            "持仓数量": quantity,
            "建仓价格": entry_price,
        }

        if tp_price:
            details["新止盈价格"] = (
                tp_price_adjusted if "tp_price_adjusted" in locals() else tp_price
            )
        if sl_price:
            details["新止损价格"] = (
                sl_price_adjusted if "sl_price_adjusted" in locals() else sl_price
            )

        strategy.server_log_position_change(
            "manual_tp_sl",
            symbol,
            details,
            before_state,
            after_state,
            success=bool(
                (new_tp_order_id and str(new_tp_order_id).strip())
                or (new_sl_order_id and str(new_sl_order_id).strip())
            ),
            error_msg=None
            if (
                (new_tp_order_id and str(new_tp_order_id).strip())
                or (new_sl_order_id and str(new_sl_order_id).strip())
            )
            else "未修改任何订单",
        )

        # 保存记录
        strategy.server_save_positions_record()

        # 🔧 修复：根据验证结果和请求参数判断实际成功状态
        # 只有请求修改的订单需要验证成功，未请求修改的订单不影响结果
        tp_success = (
            not tp_price or tp_order_validated
        )  # 如果没请求修改TP，或TP验证成功
        sl_success = (
            not sl_price or sl_order_validated
        )  # 如果没请求修改SL，或SL验证成功
        actual_success = tp_success and sl_success

        logging.info(
            f"📊 {symbol} 订单验证结果: TP验证={tp_order_validated}, SL验证={sl_order_validated}, 总体成功={actual_success}"
        )

        if actual_success:
            notify_positions_changed()

        return jsonify(
            {
                "success": actual_success,
                "message": "止盈止损已更新" if actual_success else "止盈止损更新失败",
                "position_id": position.get("position_id", "未知")[:8],
                "tp_order_id": new_tp_order_id,
                "sl_order_id": new_sl_order_id,
            }
        )

    except Exception as e:
        logging.error(f"❌ 修改止盈止损失败: {e}")
        return jsonify(
            {"success": False, "message": "止盈止损更新失败", "error": str(e)}
        ), 500


@app.route("/api/cancel_order", methods=["POST"])
@auth.login_required
def cancel_order():
    """取消订单"""
    try:
        if strategy is None:
            return jsonify({"error": "Strategy not initialized"}), 500

        data = request.json
        symbol = data["symbol"]
        order_id = data["order_id"]

        if not cancel_order_algo_or_regular(strategy.client, symbol, str(order_id)):
            return jsonify({"error": "取消订单失败（算法单与普通单均尝试失败）"}), 400

        logging.info(f"✅ Web界面取消订单: {symbol} - {order_id}")

        return jsonify({"success": True, "message": "订单已取消"})

    except Exception as e:
        logging.error(f"❌ 取消订单失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/position_logs")
@auth.login_required
def get_position_logs():
    """获取仓位变动日志（分页显示）"""
    try:
        # 获取查询参数
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))

        # 读取仓位变动日志文件
        position_log_file = os.path.join(log_dir, "position_changes.log")

        if not os.path.exists(position_log_file):
            return jsonify(
                {
                    "success": True,
                    "logs": [],
                    "total_pages": 0,
                    "current_page": 1,
                    "message": "暂无仓位变动日志",
                }
            )

        # 读取文件内容
        with open(position_log_file, "r", encoding="utf-8") as f:
            content = f.read()

        # 按记录分割（每条记录以'='*80开头）
        records = content.split("=" * 80)
        records = [r.strip() for r in records if r.strip()]

        # 计算分页
        total_records = len(records)
        total_pages = (total_records + per_page - 1) // per_page

        # 如果没有明确指定page参数（即前端没有传page参数），默认显示最后一页（最新记录）
        page_param = request.args.get("page")
        if page_param is None:  # 没有page参数，默认显示最后一页
            page = total_pages if total_pages > 0 else 1

        start_idx = (page - 1) * per_page
        end_idx = min(start_idx + per_page, total_records)

        # 获取当前页的记录
        page_records = records[start_idx:end_idx]

        return jsonify(
            {
                "success": True,
                "logs": page_records,
                "total_pages": total_pages,
                "current_page": page,
                "total_records": total_records,
                "per_page": per_page,
            }
        )

    except Exception as e:
        logging.error(f"❌ 获取仓位日志失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/strategy_params")
@auth.login_required
def get_strategy_params():
    """获取策略运行参数"""
    try:
        if strategy is None:
            return jsonify({"error": "Strategy not initialized"}), 500
        s = strategy
        return jsonify(
            {
                "success": True,
                "params": {
                    "core": {
                        "label": "核心参数",
                        "items": [
                            {"key": "杠杆", "value": f"{s.leverage}x"},
                            {
                                "key": "单仓比例",
                                "value": f"{s.position_size_ratio * 100:.1f}%",
                            },
                            {"key": "最大持仓数", "value": s.max_positions},
                            {"key": "每日最大建仓", "value": s.max_daily_entries},
                            {
                                "key": "单次扫描最多开仓",
                                "value": s.max_opens_per_scan
                                if s.max_opens_per_scan
                                else "不限制",
                            },
                            {
                                "key": "每小时建仓限制",
                                "value": "启用"
                                if s.enable_hourly_entry_limit
                                else "关闭",
                            },
                        ],
                    },
                    "signal": {
                        "label": "信号过滤",
                        "items": [
                            {
                                "key": "卖量暴涨阈值（下限）",
                                "value": f"{s.sell_surge_threshold}x",
                            },
                            {
                                "key": "卖量暴涨阈值（上限）",
                                "value": f"{s.sell_surge_max}x",
                            },
                            {
                                "key": "买量倍数风控",
                                "value": "启用"
                                if s.enable_intraday_buy_ratio_filter
                                else "关闭",
                            },
                            {
                                "key": "买量危险区间",
                                "value": "  |  ".join(
                                    f"{a}-{b}x"
                                    for a, b in s.intraday_buy_ratio_danger_ranges
                                ),
                            },
                        ],
                    },
                    "tp": {
                        "label": "止盈参数",
                        "items": [
                            {"key": "强势币止盈", "value": f"{s.strong_coin_tp_pct}%"},
                            {"key": "中势币止盈", "value": f"{s.medium_coin_tp_pct}%"},
                            {"key": "弱势币止盈", "value": f"{s.weak_coin_tp_pct}%"},
                            {
                                "key": "2h动态止盈强势K占比",
                                "value": f"{s.dynamic_tp_2h_ratio * 100:.0f}%",
                            },
                            {
                                "key": "2h动态止盈跌幅阈值",
                                "value": f"{s.dynamic_tp_2h_growth_threshold * 100:.1f}%",
                            },
                            {
                                "key": "12h动态止盈强势K占比",
                                "value": f"{s.dynamic_tp_12h_ratio * 100:.0f}%",
                            },
                            {
                                "key": "12h动态止盈跌幅阈值",
                                "value": f"{s.dynamic_tp_12h_growth_threshold * 100:.1f}%",
                            },
                        ],
                    },
                    "risk": {
                        "label": "止损 / 风控",
                        # 两列展示，压缩顶栏高度（前端 hparam-col-split）
                        "columns": [
                            [
                                {"key": "止损比例", "value": f"{s.stop_loss_pct}%"},
                                {
                                    "key": "最大持仓时长",
                                    "value": f"{s.max_hold_hours:.0f}h",
                                },
                                {
                                    "key": "12h及早止损",
                                    "value": "启用"
                                    if s.enable_12h_early_stop
                                    else "关闭",
                                },
                                {
                                    "key": "12h及早止损涨幅阈值",
                                    "value": f"{s.early_stop_12h_threshold * 100:.1f}%",
                                },
                            ],
                            [
                                {
                                    "key": "24h涨幅平仓",
                                    "value": "启用"
                                    if s.enable_max_gain_24h_exit
                                    else "关闭",
                                },
                                {
                                    "key": "24h涨幅平仓阈值",
                                    "value": f"{s.max_gain_24h_threshold * 100:.2f}%",
                                },
                                {
                                    "key": "BTC昨日阳线不建新仓",
                                    "value": "启用"
                                    if s.enable_btc_yesterday_yang_no_new_entry
                                    else "关闭",
                                },
                                {
                                    "key": "BTC昨日阳线UTC日初平仓",
                                    "value": "启用"
                                    if s.enable_btc_yesterday_yang_flatten_at_open
                                    else "关闭",
                                },
                            ],
                        ],
                    },
                },
            }
        )
    except Exception as e:
        logging.error(f"❌ 获取策略参数失败: {e}")
        return jsonify({"error": str(e)}), 500


def _parse_iso_datetime_param(s: Optional[str]) -> Optional[datetime]:
    """解析查询参数中的 ISO 时间，统一为 UTC；空串返回 None。"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _trade_record_sort_ts(t: dict) -> Optional[datetime]:
    """用于时段筛选：优先平仓时间，否则开仓时间。"""
    for key in ("close_time", "entry_time"):
        raw = t.get(key)
        if not raw:
            continue
        try:
            return _parse_iso_datetime_param(str(raw))
        except ValueError:
            continue
    return None


def _signal_record_sort_ts(s: dict) -> Optional[datetime]:
    raw = s.get("scan_time") or ""
    if not raw:
        return None
    try:
        return _parse_iso_datetime_param(str(raw))
    except ValueError:
        return None


def _signal_event_ts_for_sort(s: dict) -> Optional[datetime]:
    """列表/导出排序：优先信号发生时间 signal_time（与界面「信号时间」一致），否则退回 scan_time。"""
    raw = s.get("signal_time")
    if raw:
        t = str(raw).strip()
        if t.endswith(" UTC"):
            t = t[:-4].strip()
        try:
            norm = t.replace(" ", "T") if "T" not in t else t
            dt = datetime.fromisoformat(norm)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            try:
                return _parse_iso_datetime_param(str(raw).strip())
            except ValueError:
                pass
    return _signal_record_sort_ts(s)


def _signal_history_sort_key(x: dict) -> Tuple[datetime, float, str]:
    dt = _signal_event_ts_for_sort(x) or datetime.min.replace(tzinfo=timezone.utc)
    try:
        ratio = float(x.get("surge_ratio") or 0)
    except (TypeError, ValueError):
        ratio = 0.0
    return (dt, ratio, str(x.get("symbol") or ""))


@app.route("/api/trade_history")
@auth.login_required
def get_trade_history():
    """获取交易历史记录"""
    try:
        limit = int(request.args.get("limit", 50))
        if not os.path.exists(TRADE_HISTORY_FILE):
            return jsonify({"success": True, "trades": [], "total": 0})
        with open(TRADE_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        # 最新在前
        history = list(reversed(history))
        total = len(history)
        return jsonify({"success": True, "trades": history[:limit], "total": total})
    except Exception as e:
        logging.error(f"❌ 获取交易历史失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trade_history_export")
@auth.login_required
def export_trade_history_csv():
    """导出交易记录为 CSV；可选查询参数 start、end（ISO8601，含时区或本地解析由前端保证为 UTC）。"""
    try:
        try:
            start = _parse_iso_datetime_param(request.args.get("start"))
            end = _parse_iso_datetime_param(request.args.get("end"))
        except ValueError as e:
            return jsonify({"success": False, "error": f"时间格式无效: {e}"}), 400
        if start and end and start > end:
            return jsonify({"success": False, "error": "开始时间不能晚于结束时间"}), 400

        if not os.path.exists(TRADE_HISTORY_FILE):
            rows_out: List[dict] = []
        else:
            with open(TRADE_HISTORY_FILE, "r", encoding="utf-8") as f:
                rows_out = json.load(f)
            if not isinstance(rows_out, list):
                rows_out = []

        filtered: List[dict] = []
        no_range = start is None and end is None
        for t in rows_out:
            if not isinstance(t, dict):
                continue
            ts = _trade_record_sort_ts(t)
            if not no_range:
                if ts is None:
                    continue
                if start and ts < start:
                    continue
                if end and ts > end:
                    continue
            filtered.append(t)

        def _trade_sort_key(x: dict):
            tsv = _trade_record_sort_ts(x)
            return tsv or datetime.min.replace(tzinfo=timezone.utc)

        filtered.sort(key=_trade_sort_key)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(
            [
                "symbol",
                "direction",
                "entry_price",
                "exit_price",
                "quantity",
                "entry_time",
                "close_time",
                "elapsed_hours",
                "pnl_pct",
                "pnl_value",
                "reason",
                "reason_cn",
                "position_value",
                "leverage",
            ]
        )
        for t in filtered:
            w.writerow(
                [
                    t.get("symbol", ""),
                    t.get("direction", ""),
                    t.get("entry_price", ""),
                    t.get("exit_price", ""),
                    t.get("quantity", ""),
                    t.get("entry_time", ""),
                    t.get("close_time", ""),
                    t.get("elapsed_hours", ""),
                    t.get("pnl_pct", ""),
                    t.get("pnl_value", ""),
                    t.get("reason", ""),
                    t.get("reason_cn", ""),
                    t.get("position_value", ""),
                    t.get("leverage", ""),
                ]
            )

        raw = "\ufeff" + buf.getvalue()
        fname = (
            f"trade_history_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        )
        return Response(
            raw.encode("utf-8"),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{fname}"',
                "Cache-Control": "no-store",
            },
        )
    except Exception as e:
        logging.error(f"❌ 导出交易 CSV 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/signal_history_export")
@auth.login_required
def export_signal_history_csv():
    """导出信号记录为 CSV；时段按 scan_time 筛选，参数 start / end 可选。"""
    try:
        try:
            start = _parse_iso_datetime_param(request.args.get("start"))
            end = _parse_iso_datetime_param(request.args.get("end"))
        except ValueError as e:
            return jsonify({"success": False, "error": f"时间格式无效: {e}"}), 400
        if start and end and start > end:
            return jsonify({"success": False, "error": "开始时间不能晚于结束时间"}), 400

        if not os.path.exists(SIGNAL_HISTORY_FILE):
            rows_out = []
        else:
            with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
                rows_out = json.load(f)
            if not isinstance(rows_out, list):
                rows_out = []

        filtered: List[dict] = []
        no_range = start is None and end is None
        for s in rows_out:
            if not isinstance(s, dict):
                continue
            ts = _signal_record_sort_ts(s)
            if not no_range:
                if ts is None:
                    continue
                if start and ts < start:
                    continue
                if end and ts > end:
                    continue
            filtered.append(s)

        filtered.sort(key=_signal_history_sort_key, reverse=True)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(
            [
                "scan_time",
                "signal_time",
                "symbol",
                "surge_ratio",
                "price",
                "intraday_buy_ratio",
                "opened",
            ]
        )
        for s in filtered:
            w.writerow(
                [
                    s.get("scan_time", ""),
                    s.get("signal_time", ""),
                    s.get("symbol", ""),
                    s.get("surge_ratio", ""),
                    s.get("price", ""),
                    s.get("intraday_buy_ratio", ""),
                    "yes" if s.get("opened") else "no",
                ]
            )

        raw = "\ufeff" + buf.getvalue()
        fname = (
            f"signal_history_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        )
        return Response(
            raw.encode("utf-8"),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{fname}"',
                "Cache-Control": "no-store",
            },
        )
    except Exception as e:
        logging.error(f"❌ 导出信号 CSV 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/start_trading", methods=["POST"])
@auth.login_required
def start_trading():
    """启动自动交易"""
    global is_running

    try:
        logging.info("📥 收到 Web 请求: 启动交易 (POST /api/start_trading)")
        flush_logging_handlers()

        if strategy is None or not getattr(strategy, "api_configured", False):
            logging.warning("⚠️ 启动交易被拒绝: 未配置 API")
            flush_logging_handlers()
            return jsonify(
                {
                    "success": False,
                    "message": "未配置 API 密钥，无法启动自动交易（仅可浏览界面）",
                }
            ), 400
        # 🔒 使用原子操作防止并发启动
        if is_running:
            logging.warning("⚠️ 启动交易已忽略: 当前已是运行中 (is_running=True)")
            flush_logging_handlers()
            return jsonify({"success": False, "message": "已经在运行中"})

        # 扫描/监控线程由 main() 在进程启动时已创建；此处只打开开关，避免重复线程导致同一动作执行两遍
        is_running = True
        logging.info(
            "🚀 Web 界面: 自动交易已启动（复用已有 scan_loop / monitor_loop 线程，is_running=True）"
        )
        flush_logging_handlers()
        return jsonify({"success": True, "message": "自动交易已启动"})

    except Exception as e:
        logging.error(f"❌ 启动交易失败: {e}")
        flush_logging_handlers()
        return jsonify({"error": str(e)}), 500


@app.route("/api/send_daily_report", methods=["POST"])
@auth.login_required
def send_daily_report_api():
    """手动发送每日报告"""
    try:
        logging.info("📧 手动触发发送每日报告")

        # 发送报告
        send_daily_report()

        return jsonify({"success": True, "message": "每日报告已发送"})

    except Exception as e:
        logging.error(f"❌ 手动发送每日报告失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/daily_report_download")
@auth.login_required
def daily_report_download():
    """下载已落盘的日报 txt（默认当天 UTC 日期，可选 ?date=YYYYMMDD）。"""
    raw = (request.args.get("date") or "").strip()
    if not raw:
        d = datetime.now(timezone.utc).strftime("%Y%m%d")
    else:
        if len(raw) != 8 or not raw.isdigit():
            return jsonify(
                {"success": False, "error": "date 须为 8 位 YYYYMMDD（UTC 日历日）"}
            ), 400
        d = raw
    fname = f"daily_report_{d}.txt"
    path = os.path.join(log_dir, fname)
    if not os.path.isfile(path):
        return jsonify(
            {
                "success": False,
                "error": f"未找到 {fname}（可能尚未生成过该日日报）",
            }
        ), 404
    try:
        return send_file(
            path,
            as_attachment=True,
            download_name=fname,
            mimetype="text/plain; charset=utf-8",
        )
    except Exception as e:
        logging.error(f"❌ 下载日报失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/stop_trading", methods=["POST"])
@auth.login_required
def stop_trading():
    """停止自动交易"""
    global is_running

    try:
        was_running = is_running
        logging.info("📥 收到 Web 请求: 停止交易 (POST /api/stop_trading)")
        flush_logging_handlers()

        is_running = False

        if was_running:
            logging.info(
                "⏹️ Web 界面: 自动交易已停止 (is_running=False，扫描/监控循环将休眠)"
            )
        else:
            logging.info(
                "⏹️ Web 界面: 停止交易（当前已为停止状态，已幂等设置为 is_running=False）"
            )
        flush_logging_handlers()

        return jsonify({"success": True, "message": "自动交易已停止"})

    except Exception as e:
        logging.error(f"❌ 停止交易失败: {e}")
        flush_logging_handlers()
        return jsonify({"error": str(e)}), 500


@app.route("/api/manual_scan", methods=["POST"])
@auth.login_required
def manual_scan():
    """手动扫描"""
    try:
        if strategy is None:
            return jsonify({"error": "Strategy not initialized"}), 500
        if not getattr(strategy, "api_configured", False):
            return jsonify(
                {"success": False, "error": "未配置 API 密钥，无法扫描或建仓"}
            ), 400

        logging.info("🔍 Web界面触发手动扫描...")

        if not strategy.server_sync_wallet_snapshot():
            strategy.account_balance = strategy.server_get_account_balance()
            strategy.account_available_balance = strategy.account_balance

        # 扫描信号
        signals = strategy.server_scan_sell_surge_signals()

        # 尝试建仓（与 hm1l 一致：按倍数排序后连续尝试，直至额度/扫描上限）
        opened_count = 0
        opened_syms = set()
        cap = strategy.max_opens_per_scan
        for signal in signals:
            if cap > 0 and opened_count >= cap:
                break
            if strategy.server_open_position(signal):
                opened_count += 1
                opened_syms.add(signal["symbol"])

        if signals:
            save_signal_records(
                signals, datetime.now(timezone.utc).isoformat(), opened_syms
            )

        return jsonify(
            {
                "success": True,
                "message": f"扫描完成，发现 {len(signals)} 个信号，建仓 {opened_count} 个",
                "signals": signals,
            }
        )

    except Exception as e:
        logging.error(f"❌ 手动扫描失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/klines")
@auth.login_required
def api_klines():
    """返回合约 K 线数据，供前端 Lightweight Charts 使用"""
    try:
        if strategy is None or not getattr(strategy, "api_configured", False):
            return jsonify({"success": False, "error": "未配置 API"}), 503
        symbol = request.args.get("symbol", "BTCUSDT").upper()
        interval = request.args.get("interval", "1h")
        limit = min(int(request.args.get("limit", 200)), 500)
        klines = strategy.client.futures_klines(
            symbol=symbol, interval=interval, limit=limit
        )
        data = [
            {
                "time": int(k[0]) // 1000,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
            for k in klines
        ]
        return jsonify({"success": True, "data": data})
    except Exception as e:
        logging.error(f"❌ api_klines 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


def save_signal_records(signals: list, scan_time: str, opened_symbols: set):
    """写入 data/signal_history.json，保留最近 90 天。

    同一 (symbol, signal_time) 只保留一行：定时扫描与手动扫描在同一 UTC 小时内会重复检测
    上一完整小时，原先会追加两条；现改为合并，`opened` 取多次扫描的或（任一次建仓即为 True）。
    """
    try:
        with _signal_history_lock:
            if os.path.exists(SIGNAL_HISTORY_FILE):
                with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
                    history = json.load(f)
            else:
                history = []
            if not isinstance(history, list):
                history = []

            for s in signals:
                sym = s.get("symbol", "")
                sig_t = s.get("signal_time", "")
                opened_now = sym in opened_symbols

                history.append(
                    {
                        "scan_time": scan_time,
                        "signal_time": sig_t,
                        "symbol": sym,
                        "surge_ratio": round(float(s.get("surge_ratio", 0)), 4),
                        "price": s.get("price", 0),
                        "intraday_buy_ratio": round(
                            float(s.get("intraday_buy_ratio", 0)), 4
                        ),
                        "opened": opened_now,
                    }
                )

            # 保留最近 90 天（按 scan_time）
            cutoff = (
                datetime.now(timezone.utc) - __import__("datetime").timedelta(days=90)
            ).isoformat()
            history = [r for r in history if r.get("scan_time", "") >= cutoff]
            with open(SIGNAL_HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"❌ 写入信号历史失败: {e}")


@app.route("/api/signal_history")
@auth.login_required
def api_signal_history():
    """返回信号历史记录，按日期+倍数降序。"""
    try:
        limit = int(request.args.get("limit", 500))
        if not os.path.exists(SIGNAL_HISTORY_FILE):
            return jsonify({"success": True, "signals": [], "total": 0})
        with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        history.sort(key=_signal_history_sort_key, reverse=True)
        total = len(history)
        return jsonify({"success": True, "signals": history[:limit], "total": total})
    except Exception as e:
        logging.error(f"❌ 获取信号历史失败: {e}")
        return jsonify({"error": str(e)}), 500


# ==================== 后台线程（从 loops.py 导入） ====================
from loops import scan_loop, monitor_loop, daily_report_loop


# ==================== 信号处理 ====================
def signal_handler(sig, frame):
    """处理Ctrl+C信号"""
    global is_running

    logging.info("\n⏹️ 收到停止信号，正在退出...")
    is_running = False

    # 给线程1秒时间退出
    time.sleep(1)

    logging.info("👋 AE Server 已停止")
    sys.exit(0)


# ==================== 主程序 ====================
def main():
    """主函数"""
    global strategy, is_running, scan_thread, monitor_thread

    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logging.info("=" * 60)
        logging.info("🚀 AE Server v2.0 启动中...")
        logging.info("=" * 60)

        # 加载配置
        config = load_config()
        logging.info("✅ 配置文件加载成功")

        # 初始化策略引擎
        strategy = AutoExchangeStrategy(config)
        global start_time
        start_time = datetime.now(timezone.utc)
        logging.info("✅ 策略引擎初始化完成")

        if strategy.api_configured:
            if not strategy.server_sync_wallet_snapshot():
                strategy.account_balance = strategy.server_get_account_balance()
                strategy.account_available_balance = strategy.account_balance
            strategy.server_check_and_recreate_missing_tp_sl()
            try:
                strategy.server_maybe_btc_yesterday_yang_flatten_at_new_utc_day()
            except Exception as e:
                logging.error(f"❌ 启动时 BTC 昨日阳线一刀切检查失败: {e}")
        else:
            logging.info("🔕 仅界面模式：跳过账户同步、止盈止损检查与 BTC 日级风控")

        # 启动Flask服务（后台线程）
        flask_thread = threading.Thread(
            target=lambda: app.run(
                host="0.0.0.0", port=5002, debug=False, use_reloader=False
            ),
            daemon=True,
        )
        flask_thread.start()

        logging.info("✅ Flask Web服务已启动: http://localhost:5002")

        # 🔧 关键修复：启动扫描和监控线程
        logging.info("🚀 启动后台任务线程...")

        # 启动扫描线程
        scan_thread = threading.Thread(target=scan_loop, daemon=True)
        scan_thread.start()
        logging.info("✅ 信号扫描线程已启动")

        # 启动监控线程
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        logging.info("✅ 持仓监控线程已启动")

        # 启动每日报告线程
        report_thread = threading.Thread(target=daily_report_loop, daemon=True)
        report_thread.start()
        logging.info("✅ 每日报告线程已启动")

        logging.info("=" * 60)
        logging.info("📋 使用说明:")
        logging.info("  - 浏览器打开: http://localhost:5002")

        # 🔐 显示Web认证信息
        logging.info("  - Web用户名: admin")
        logging.info("  - Web密码: admin123")
        logging.info("  - 外部访问: http://45.77.37.106:5002")
        logging.info("  - API服务器(旧): http://localhost:5001")
        logging.info("  - 停止程序: Ctrl+C")
        logging.info("=" * 60)

        # 主线程保持运行
        while True:
            time.sleep(60)
            # 每分钟输出一次状态
            if is_running:
                if strategy is not None and getattr(strategy, "api_configured", False):
                    strategy.server_sync_wallet_snapshot()
                logging.info(
                    f"💓 系统运行中... 持仓: {len(strategy.positions)}, "
                    f"余额: ${strategy.account_balance:.2f}  可用余额: ${strategy.account_available_balance:.2f}"
                )
                # 🔧 强制刷新日志
                flush_logging_handlers()

    except FileNotFoundError:
        logging.error("❌ 配置文件不存在")
        sys.exit(1)

    except Exception as e:
        logging.error(f"❌ 程序启动失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
