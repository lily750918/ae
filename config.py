"""
config.py – Configuration loading, validation, and hot-reload helpers.

Extracted from ae_server.py during modular refactor.
"""

import os
import logging
import configparser
import tempfile
import threading
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from binance.client import Client
from binance.exceptions import BinanceAPIException

from utils.cache import YesterdayDataCache
from utils.logging_config import log_dir

# NOTE: 'strategy' global will be imported from state.py in a later refactor step

# ---------------------------------------------------------------------------
# Binance HTTP pool tuning
# ---------------------------------------------------------------------------
# python-binance 共用同一 Session；扫描/预热使用 ThreadPoolExecutor(max_workers=20)，
# 而 urllib3 默认每主机 pool_maxsize=10，并发超过时会刷屏「Connection pool is full」。
_BINANCE_HTTP_POOL_MAX = 32


def _configure_binance_http_adapter(session: Optional[requests.Session]) -> None:
    if session is None:
        return
    adapter = HTTPAdapter(
        pool_connections=_BINANCE_HTTP_POOL_MAX,
        pool_maxsize=_BINANCE_HTTP_POOL_MAX,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)


# ---------------------------------------------------------------------------
# Paths & locks
# ---------------------------------------------------------------------------
# 数据库路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_INI_PATH = os.path.join(SCRIPT_DIR, "config.ini")

# 持仓记录文件
POSITIONS_RECORD_FILE = os.path.join(SCRIPT_DIR, "positions_record.json")

# 交易历史记录文件
TRADE_HISTORY_FILE = os.path.join(SCRIPT_DIR, "trade_history.json")

# 信号历史记录文件
SIGNAL_HISTORY_FILE = os.path.join(SCRIPT_DIR, "signal_history.json")
_signal_history_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Config loading / editing / validation
# ---------------------------------------------------------------------------
def load_config():
    """从配置文件加载配置；若无文件则使用空段（仅界面模式 + 代码内 fallback 参数）。"""
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_INI_PATH):
        logging.warning(
            "⚠️ 未找到 config.ini，使用内存默认段（可无密钥仅浏览界面；交易前请创建 config.ini）"
        )
        for sec in ("BINANCE", "STRATEGY", "SIGNAL", "RISK"):
            config[sec] = {}
        return config
    config.read(CONFIG_INI_PATH, encoding="utf-8")
    return config


# —— Web「参数编辑」可读写的 config.ini 键（不含 BINANCE）——
_CONFIG_EDITABLE_KEYS: Dict[str, Tuple[str, ...]] = {
    "STRATEGY": (
        "leverage",
        "position_size_ratio",
        "max_positions",
        "max_daily_entries",
        "enable_hourly_entry_limit",
        "max_opens_per_scan",
    ),
    "SIGNAL": ("sell_surge_threshold", "sell_surge_max"),
    "RISK": (
        "strong_coin_tp_pct",
        "medium_coin_tp_pct",
        "weak_coin_tp_pct",
        "stop_loss_pct",
        "enable_max_gain_24h_exit",
        "max_gain_24h_threshold",
        "max_hold_hours",
    ),
}


def _config_editable_defaults_strings() -> Dict[str, Dict[str, str]]:
    """与 AutoExchangeStrategy.__init__ 的 fallback 对齐（字符串形式写入 ini）。"""
    return {
        "STRATEGY": {
            "leverage": "3",
            "position_size_ratio": "0.09",
            "max_positions": "10",
            "max_daily_entries": "6",
            "enable_hourly_entry_limit": "false",
            "max_opens_per_scan": "0",
        },
        "SIGNAL": {
            "sell_surge_threshold": "10",
            "sell_surge_max": "14008",
        },
        "RISK": {
            "strong_coin_tp_pct": "33",
            "medium_coin_tp_pct": "21",
            "weak_coin_tp_pct": "10",
            "stop_loss_pct": "18",
            "enable_max_gain_24h_exit": "false",
            "max_gain_24h_threshold": "6.3",
            "max_hold_hours": "72",
        },
    }


def _get_config_editable_dict() -> Dict[str, Dict[str, str]]:
    cfg = load_config()
    defaults = _config_editable_defaults_strings()
    out: Dict[str, Dict[str, str]] = {}
    for sec, keys in _CONFIG_EDITABLE_KEYS.items():
        out[sec] = {}
        for k in keys:
            if cfg.has_section(sec) and cfg.has_option(sec, k):
                out[sec][k] = (cfg.get(sec, k, fallback="") or "").strip()
            else:
                out[sec][k] = defaults[sec][k]
    return out


def _parse_bool_incoming(v) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "on"):
        return True
    if s in ("false", "0", "no", "off"):
        return False
    return None


def _merge_editable_post(body: dict) -> Dict[str, Dict[str, str]]:
    base = _get_config_editable_dict()
    merged: Dict[str, Dict[str, str]] = {}
    for sec, keys in _CONFIG_EDITABLE_KEYS.items():
        inc = body.get(sec)
        if not isinstance(inc, dict):
            inc = {}
        row = dict(base[sec])
        for k in keys:
            if k in inc:
                row[k] = inc[k]
        merged[sec] = row
    return merged


def _validate_editable_merged(
    merged: Dict[str, Dict[str, str]],
) -> Tuple[Optional[Dict[str, Dict[str, str]]], Optional[str]]:
    """返回写入 ini 的规范化字符串表，或 (None, 错误信息)。"""
    out: Dict[str, Dict[str, str]] = {}
    try:
        st = merged["STRATEGY"]
        lev = float(st["leverage"])
        if not (1 <= lev <= 125):
            return None, "杠杆 leverage 应在 1–125"
        psr = float(st["position_size_ratio"])
        if not (0 < psr <= 1):
            return None, "单仓比例 position_size_ratio 应在 (0, 1]"
        mp = int(float(st["max_positions"]))
        mde = int(float(st["max_daily_entries"]))
        mos = int(float(st["max_opens_per_scan"]))
        if mp < 1 or mp > 500:
            return None, "max_positions 应在 1–500"
        if mde < 0 or mde > 500:
            return None, "max_daily_entries 应在 0–500"
        if mos < 0 or mos > 500:
            return None, "max_opens_per_scan 应在 0–500"
        eh = _parse_bool_incoming(st["enable_hourly_entry_limit"])
        if eh is None:
            return None, "enable_hourly_entry_limit 应为 true/false"
        out["STRATEGY"] = {
            "leverage": str(int(lev)) if lev == int(lev) else str(lev),
            "position_size_ratio": str(psr),
            "max_positions": str(mp),
            "max_daily_entries": str(mde),
            "enable_hourly_entry_limit": "true" if eh else "false",
            "max_opens_per_scan": str(mos),
        }

        sg = merged["SIGNAL"]
        sst = float(sg["sell_surge_threshold"])
        ssm = float(sg["sell_surge_max"])
        if sst <= 0 or ssm <= 0:
            return None, "卖量阈值须为正数"
        if ssm <= sst:
            return None, "sell_surge_max 须大于 sell_surge_threshold"
        out["SIGNAL"] = {
            "sell_surge_threshold": str(sst),
            "sell_surge_max": str(ssm),
        }

        rk = merged["RISK"]
        for key in (
            "strong_coin_tp_pct",
            "medium_coin_tp_pct",
            "weak_coin_tp_pct",
            "stop_loss_pct",
        ):
            x = float(rk[key])
            if not (0.1 <= x <= 99):
                return None, f"{key} 应在 0.1–99"
        mg = _parse_bool_incoming(rk["enable_max_gain_24h_exit"])
        if mg is None:
            return None, "enable_max_gain_24h_exit 应为 true/false"
        mgt = float(rk["max_gain_24h_threshold"])
        if not (0.01 <= mgt <= 100):
            return None, "max_gain_24h_threshold（百分比）应在 0.01–100"
        mhh = float(rk["max_hold_hours"])
        if not (1 <= mhh <= 720):
            return None, "max_hold_hours 应在 1–720"
        out["RISK"] = {
            "strong_coin_tp_pct": str(float(rk["strong_coin_tp_pct"])),
            "medium_coin_tp_pct": str(float(rk["medium_coin_tp_pct"])),
            "weak_coin_tp_pct": str(float(rk["weak_coin_tp_pct"])),
            "stop_loss_pct": str(float(rk["stop_loss_pct"])),
            "enable_max_gain_24h_exit": "true" if mg else "false",
            "max_gain_24h_threshold": str(mgt),
            "max_hold_hours": str(int(mhh)) if mhh == int(mhh) else str(mhh),
        }
    except (TypeError, ValueError, KeyError) as e:
        return None, f"参数格式无效: {e}"
    return out, None


def _atomic_write_config_ini(parser: configparser.ConfigParser) -> None:
    fd, tmp_path = tempfile.mkstemp(
        suffix=".ini", prefix="ae_config_", dir=SCRIPT_DIR, text=True
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            parser.write(f)
        os.replace(tmp_path, CONFIG_INI_PATH)
    except OSError as e:
        logging.error(f"写入配置文件失败: {e}")
        try:
            os.unlink(tmp_path)
        except OSError:
            logging.warning(f"清理临时文件失败: {tmp_path}")
        raise
    except Exception as e:
        logging.error(f"写入配置文件异常: {e}")
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# 热更新运行中 strategy 的可编辑段时，批量写属性避免与扫描/监控线程交错读到半成品
_STRATEGY_CONFIG_LOCK = threading.Lock()


def _apply_normalized_editable_to_strategy(
    s: "AutoExchangeStrategy", norm: Dict[str, Dict[str, str]]
) -> None:
    """将已通过校验的 STRATEGY/SIGNAL/RISK 字符串表写回策略实例（与 __init__ 解析一致）。"""
    cp = configparser.ConfigParser()
    for sec, kv in norm.items():
        if not cp.has_section(sec):
            cp.add_section(sec)
        for k, v in kv.items():
            cp.set(sec, k, v)

    s.leverage = cp.getfloat("STRATEGY", "leverage")
    s.position_size_ratio = cp.getfloat("STRATEGY", "position_size_ratio")
    s.max_positions = cp.getint("STRATEGY", "max_positions")
    s.max_daily_entries = cp.getint("STRATEGY", "max_daily_entries")
    s.enable_hourly_entry_limit = cp.getboolean("STRATEGY", "enable_hourly_entry_limit")
    s.max_opens_per_scan = cp.getint("STRATEGY", "max_opens_per_scan")

    s.sell_surge_threshold = cp.getfloat("SIGNAL", "sell_surge_threshold")
    s.sell_surge_max = cp.getfloat("SIGNAL", "sell_surge_max")

    s.strong_coin_tp_pct = cp.getfloat("RISK", "strong_coin_tp_pct")
    s.medium_coin_tp_pct = cp.getfloat("RISK", "medium_coin_tp_pct")
    s.weak_coin_tp_pct = cp.getfloat("RISK", "weak_coin_tp_pct")
    s.stop_loss_pct = cp.getfloat("RISK", "stop_loss_pct")
    s.enable_max_gain_24h_exit = cp.getboolean("RISK", "enable_max_gain_24h_exit")
    s.max_gain_24h_threshold = cp.getfloat("RISK", "max_gain_24h_threshold") / 100.0
    s.max_hold_hours = cp.getfloat("RISK", "max_hold_hours")

    cfg = getattr(s, "config", None)
    if cfg is not None:
        try:
            for sec, kv in norm.items():
                if not cfg.has_section(sec):
                    cfg.add_section(sec)
                for k, v in kv.items():
                    cfg.set(sec, k, v)
        except configparser.Error as e:
            logging.warning(f"同步配置到strategy.config失败: {e}")
        except AttributeError as e:
            logging.warning(f"strategy.config不存在或不可写: {e}")


def _strip_cred_plain(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _resolve_binance_keys_for_effective_trading(
    cfg: Optional[configparser.ConfigParser] = None,
) -> Tuple[Optional[str], Optional[str], str]:
    """与策略一致：环境变量优先，否则 config.ini [BINANCE]。返回 (key, secret, 'env'|'file'|'none')。"""
    if cfg is None:
        cfg = load_config()
    k = _strip_cred_plain(os.getenv("BINANCE_API_KEY"))
    s = _strip_cred_plain(os.getenv("BINANCE_API_SECRET"))
    if k and s:
        return k, s, "env"
    try:
        if cfg.has_section("BINANCE"):
            ik = _strip_cred_plain(cfg.get("BINANCE", "api_key", fallback=""))
            isec = _strip_cred_plain(cfg.get("BINANCE", "api_secret", fallback=""))
            if ik and isec:
                return ik, isec, "file"
    except configparser.Error as e:
        logging.debug(f"读取BINANCE配置失败: {e}")
    except Exception as e:
        logging.debug(f"解析密钥异常: {e}")
    return None, None, "none"


def _file_binance_keys(
    cfg: configparser.ConfigParser,
) -> Tuple[Optional[str], Optional[str]]:
    if not cfg.has_section("BINANCE"):
        return None, None
    return (
        _strip_cred_plain(cfg.get("BINANCE", "api_key", fallback="")),
        _strip_cred_plain(cfg.get("BINANCE", "api_secret", fallback="")),
    )


def _key_tail_4(k: Optional[str]) -> str:
    if not k or len(k) < 4:
        return ""
    return k[-4:]


def _create_binance_client(api_key: str, api_secret: str) -> Client:
    """创建 U 本位期货 Client（与 AutoExchangeStrategy.__init__ 逻辑一致，含现货 ping 绕过）。"""
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            try:
                c = Client(api_key, api_secret, tld="com", testnet=False, ping=False)
            except TypeError:
                c = Client(api_key, api_secret, tld="com", testnet=False)
            c.FUTURES_RECV_WINDOW = 10000
            return c
        except Exception as e:
            last_error = e
            error_msg = str(e)
            if (
                "SSL" in error_msg
                or "ping" in error_msg
                or "api.binance.com" in error_msg
            ):
                try:
                    raw = object.__new__(Client)
                    try:
                        Client.__init__(
                            raw,
                            api_key=api_key,
                            api_secret=api_secret,
                            tld="com",
                            testnet=False,
                            ping=False,
                        )
                    except TypeError:
                        Client.__init__(
                            raw,
                            api_key,
                            api_secret,
                            tld="com",
                            testnet=False,
                        )
                    raw.FUTURES_RECV_WINDOW = 10000
                    return raw
                except Exception as bypass_error:
                    last_error = bypass_error
                    if attempt < 2:
                        logging.warning(
                            f"⚠️ Binance Client 尝试 {attempt + 1}/3 失败，2秒后重试..."
                        )
                        time.sleep(2)
                        continue
                    raise bypass_error
            else:
                if attempt < 2:
                    logging.warning(
                        f"⚠️ Binance Client 尝试 {attempt + 1}/3 失败: {error_msg[:80]}"
                    )
                    time.sleep(2)
                    continue
                raise
    if last_error:
        raise last_error
    raise RuntimeError("无法创建币安客户端")


def _sync_strategy_config_binance_from_parser(
    strategy_obj: "AutoExchangeStrategy", parser: configparser.ConfigParser
) -> None:
    cfg = getattr(strategy_obj, "config", None)
    if cfg is None or not parser.has_section("BINANCE"):
        return
    try:
        if not cfg.has_section("BINANCE"):
            cfg.add_section("BINANCE")
        for opt in ("api_key", "api_secret"):
            if parser.has_option("BINANCE", opt):
                cfg.set("BINANCE", opt, parser.get("BINANCE", opt))
    except configparser.Error as e:
        logging.warning(f"同步BINANCE配置失败: {e}")


def _reinit_strategy_binance_client_after_ini_change() -> Tuple[bool, str]:
    """写入 config.ini 后按「环境变量优先」规则重建 strategy 的 client。返回 (ok, message)。"""
    # NOTE: 'strategy' global will be imported from state.py in a later refactor step
    global strategy
    if strategy is None:
        return True, "strategy 未初始化，下次启动将读取新密钥"
    cfg = load_config()
    k, s, src = _resolve_binance_keys_for_effective_trading(cfg)
    with _STRATEGY_CONFIG_LOCK:
        if not k or not s:
            strategy.api_configured = False
            strategy.client = None
            strategy.yesterday_cache = YesterdayDataCache(None)
            _sync_strategy_config_binance_from_parser(strategy, cfg)
            return True, "已清除运行中的 API 客户端（当前无有效密钥）"
        try:
            cl = _create_binance_client(k, s)
            _configure_binance_http_adapter(getattr(cl, "session", None))
            cl.futures_ping()
        except Exception as e:
            return False, str(e)
        strategy.client = cl
        strategy.api_configured = True
        strategy.yesterday_cache = YesterdayDataCache(cl)
        _sync_strategy_config_binance_from_parser(strategy, cfg)
    logging.info("✅ Binance 客户端已按当前配置热重载（来源=%s）", src)
    return True, ""
