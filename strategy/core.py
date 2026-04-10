"""Strategy core: initialisation and simple utility helpers."""

import os
import subprocess
import logging
import configparser
import threading
import time
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional

from binance.client import Client
from binance.exceptions import BinanceAPIException

from config import _configure_binance_http_adapter
from utils.cache import YesterdayDataCache
from utils.logging_config import _log_asctime_local, _position_change_fmt_value


class StrategyCore:
    """自动交易策略核心类 – 初始化与基础工具方法"""

    def __init__(self, config: configparser.ConfigParser):
        """初始化策略参数"""
        # 加载配置
        self.config = config

        # 🔐 安全改进：优先从环境变量读取 API 密钥，再读 config.ini；皆无则仅界面模式（可打开网页，无实盘）
        def _strip_cred(v: Optional[str]) -> Optional[str]:
            if v is None:
                return None
            s = str(v).strip()
            return s if s else None

        api_key = _strip_cred(os.getenv("BINANCE_API_KEY"))
        api_secret = _strip_cred(os.getenv("BINANCE_API_SECRET"))

        if api_key and api_secret:
            logging.info("✅ 从环境变量加载 API 密钥")
        else:
            logging.warning("⚠️ 环境变量未完整设置，尝试从 config.ini [BINANCE] 读取")
            try:
                if config.has_section("BINANCE"):
                    ik = _strip_cred(config.get("BINANCE", "api_key", fallback=""))
                    isec = _strip_cred(config.get("BINANCE", "api_secret", fallback=""))
                    api_key = api_key or ik
                    api_secret = api_secret or isec
            except configparser.Error as e:
                logging.debug(f"读取config.ini的BINANCE配置失败: {e}")

        self.api_configured = bool(api_key and api_secret)

        if not self.api_configured:
            logging.warning(
                "⚠️ 未配置有效 API 密钥：以「仅界面模式」启动（可访问监控页；交易与行情类接口不可用，配置密钥后需重启）"
            )
            self.client = None
            self.yesterday_cache = YesterdayDataCache(None)
        else:
            logging.info("🔄 初始化币安客户端...")
            client_ready = False
            for attempt in range(3):
                try:
                    # ping=False：跳过构造函数内对现货 api 的 ping（策略只用期货；避免失败后走不完整 bypass）
                    try:
                        self.client = Client(
                            api_key, api_secret, tld="com", testnet=False, ping=False
                        )
                    except TypeError:
                        self.client = Client(
                            api_key, api_secret, tld="com", testnet=False
                        )
                    self.client.FUTURES_RECV_WINDOW = 10000
                    client_ready = True
                    break
                except Exception as e:
                    error_msg = str(e)
                    if (
                        "SSL" in error_msg
                        or "ping" in error_msg
                        or "api.binance.com" in error_msg
                    ):
                        logging.warning(
                            f"⚠️ 现货API连接失败（可忽略，我们只用期货API）: {error_msg[:80]}..."
                        )
                        try:
                            # 必须跑 Client.__init__ / BaseClient.__init__，否则无 testnet、tld、FUTURES_URL 等实例属性
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
                                    api_key=api_key,
                                    api_secret=api_secret,
                                    tld="com",
                                    testnet=False,
                                )
                            self.client = raw
                            self.client.FUTURES_RECV_WINDOW = 10000
                            client_ready = True
                            logging.info(
                                "✅ 已绕过现货 ping，完成 Client 初始化（testnet=False，主网期货）"
                            )
                            break
                        except Exception as bypass_error:
                            if attempt < 2:
                                logging.warning(
                                    f"⚠️ 尝试 {attempt + 1}/3 失败，2秒后重试..."
                                )
                                time.sleep(2)
                            else:
                                logging.error(f"❌ 客户端创建失败: {bypass_error}")
                                raise
                    else:
                        if attempt < 2:
                            logging.warning(
                                f"⚠️ 初始化失败 ({attempt + 1}/3): {error_msg[:80]}"
                            )
                            time.sleep(2)
                        else:
                            raise

            if not client_ready:
                raise RuntimeError("无法创建币安客户端")

            _configure_binance_http_adapter(getattr(self.client, "session", None))

            try:
                self.client.futures_ping()
                logging.info("✅ 期货API连接测试成功")
            except Exception as e:
                logging.warning(f"⚠️ 期货API ping失败: {e}")
                logging.warning("⚠️ 将在实际调用时重试")

            self.yesterday_cache = YesterdayDataCache(self.client)
            logging.info("✅ 昨日数据缓存初始化完成（API模式）")

        # 核心参数（从配置文件读取）
        self.leverage = config.getfloat("STRATEGY", "leverage", fallback=3.0)
        self.position_size_ratio = config.getfloat(
            "STRATEGY", "position_size_ratio", fallback=0.09
        )
        self.max_positions = config.getint("STRATEGY", "max_positions", fallback=10)
        self.max_daily_entries = config.getint(
            "STRATEGY", "max_daily_entries", fallback=6
        )
        # 与 hm1l 对齐：默认 false 允许同一 UTC 小时内多次建仓；true 则与旧版 ae 一致（每小时最多 1 单）
        self.enable_hourly_entry_limit = config.getboolean(
            "STRATEGY", "enable_hourly_entry_limit", fallback=False
        )
        # 单次扫描最多成功开仓数；0=不限制（在持仓/日额度内尽量多开），与 hm1l 按倍数排序连续尝试候选一致
        self.max_opens_per_scan = config.getint(
            "STRATEGY", "max_opens_per_scan", fallback=0
        )

        # 信号阈值
        self.sell_surge_threshold = config.getfloat(
            "SIGNAL", "sell_surge_threshold", fallback=10
        )
        self.sell_surge_max = config.getfloat(
            "SIGNAL", "sell_surge_max", fallback=14008
        )

        # 🆕 当日买量倍数风控（从hm1l.py移植）
        self.enable_intraday_buy_ratio_filter = True  # ✅ 启用：当日买量倍数风控
        # 当日买量倍数：信号发生前12小时，每小时买量相对前一小时的最大比值
        # 📊 根据实际回测数据优化：
        #   - 5-7x 表现最佳（止盈率16.7%，止损率20.8%）✅
        #   - 10-15x 表现差（止盈率6.2%，止损率56.2%）❌
        #   - >15x 表现差（止盈率10.0%，止损率50.0%）❌
        self.intraday_buy_ratio_danger_ranges = [
            (4.81, 6.61),  # 危险区间1：4.81-6.61倍（过滤多空博弈信号）
            (9.45, 11.1),  # 危险区间2：9.45-11.1倍（过滤高波动信号）
        ]

        # 动态止盈参数
        self.strong_coin_tp_pct = config.getfloat(
            "RISK", "strong_coin_tp_pct", fallback=33.0
        )
        self.medium_coin_tp_pct = config.getfloat(
            "RISK", "medium_coin_tp_pct", fallback=21.0
        )
        self.weak_coin_tp_pct = config.getfloat(
            "RISK", "weak_coin_tp_pct", fallback=10.0
        )

        # 2小时判断参数
        self.dynamic_tp_2h_ratio = 0.6  # 强势K线占比60%
        self.dynamic_tp_2h_growth_threshold = 0.055  # 单根跌幅5.5%

        # 12小时判断参数
        self.dynamic_tp_12h_ratio = 0.6  # 强势K线占比60%
        self.dynamic_tp_12h_growth_threshold = 0.075  # 单根跌幅7.5%

        # 🚨 12小时及早平仓参数（新增）
        self.enable_12h_early_stop = True  # 是否启用12小时及早平仓
        self.early_stop_12h_threshold = 0.037  # 12小时涨幅阈值（3.7%）

        # 止损参数
        self.stop_loss_pct = config.getfloat("RISK", "stop_loss_pct", fallback=18.0)
        # 与 hm1l 默认一致：False 时不做「约 24h 相对建仓价涨幅超阈值」平仓（hm1l enable_max_gain_24h_exit）
        self.enable_max_gain_24h_exit = config.getboolean(
            "RISK", "enable_max_gain_24h_exit", fallback=False
        )
        self.max_gain_24h_threshold = (
            config.getfloat("RISK", "max_gain_24h_threshold", fallback=6.3) / 100
        )
        self.max_hold_hours = config.getfloat("RISK", "max_hold_hours", fallback=72)

        # 🛡️ 组合级别风控
        self.max_daily_loss_pct = config.getfloat(
            "RISK", "max_daily_loss_pct", fallback=8.0
        )
        self.max_consecutive_losses = config.getint(
            "RISK", "max_consecutive_losses", fallback=3
        )
        self.cooldown_hours = config.getfloat("RISK", "cooldown_hours", fallback=4.0)

        # ========== BTC 日线风控（与 top2/hm1l.py 对齐，写在代码内不读 config.ini）==========
        # 昨日 BTC 日K 收>开 → 当日不建新仓；→ 新 UTC 日首次评估时一刀切平掉程序管理的空仓（市价）
        self.enable_btc_yesterday_yang_no_new_entry = True
        self.enable_btc_yesterday_yang_flatten_at_open = True
        self._last_btc_yang_flatten_eval_utc_date: Optional[date] = (
            None  # 本 UTC 日是否已成功拉取昨日日K并评估
        )

        # 持仓管理
        self.positions = []  # 当前持仓列表
        self.daily_entries = 0  # 今日建仓数
        self.last_entry_date = None  # 上次建仓日期
        self.last_entry_hour = None  # 上次建仓小时（用于每小时限制）

        # 🛡️ 组合风控状态
        self.consecutive_losses = 0  # 连续亏损计数
        self.last_loss_time = None  # 上次亏损时间（用于冷却期计算）
        self.cooldown_until = None  # 冷却期截止时间

        # 🔒 并发控制锁（防止重复建仓）
        self.position_locks = {}  # symbol -> Lock
        self.position_lock_master = threading.Lock()  # 保护locks字典本身
        self._positions_sync_lock = (
            threading.RLock()
        )  # 保护 positions 与 positions_record 同步
        self._entry_global_lock = (
            threading.Lock()
        )  # 建仓全局限流：检查持仓上限与下单原子化

        # 账户余额（与币安 futures_account 顶部字段一致）
        self.account_balance = 0.0
        self.account_available_balance = 0.0

        # exchange_info 缓存（避免每次建仓都调用重量级接口）
        self._exchange_info_cache: Dict[str, dict] = {}  # {symbol: symbol_info}
        self._exchange_info_updated_at: Optional[datetime] = None

        # intraday_buy_surge_ratio 缓存，key=(symbol, hour_str)，每小时自动失效
        self._intraday_ratio_cache: Dict[tuple, float] = {}
        self._intraday_ratio_lock = threading.Lock()

        # 加载现有持仓
        self.positions_loaded = False  # 标记是否已成功从交易所加载持仓
        try:
            self.server_load_existing_positions()
        except Exception as e:
            logging.error(f"❌ 启动时加载持仓失败: {e}，将在监控循环中重试")

        logging.info("✅ 策略引擎初始化完成")
        logging.info(
            f"   杠杆: {self.leverage}x, 单仓: {self.position_size_ratio * 100:.0f}%, 最大持仓: {self.max_positions}"
        )
        logging.info(
            f"   止盈: {self.strong_coin_tp_pct}/{self.medium_coin_tp_pct}/{self.weak_coin_tp_pct}%, 止损: {self.stop_loss_pct}%"
        )
        logging.info(
            f"   24h涨幅平仓: {self.enable_max_gain_24h_exit} (阈值 {self.max_gain_24h_threshold * 100:.2f}%) | "
            f"每小时单仓限制: {self.enable_hourly_entry_limit} | "
            f"单次扫描最多开仓: {self.max_opens_per_scan or '不限制'}"
        )
        logging.info(
            f"   BTC昨日阳线风控: 不建新仓={self.enable_btc_yesterday_yang_no_new_entry} | "
            f"UTC日初一刀切空仓={self.enable_btc_yesterday_yang_flatten_at_open}"
        )

    # ------------------------------------------------------------------
    # Simple utility helpers
    # ------------------------------------------------------------------

    def server_play_alert_sound(self):
        """播放报警声音 - 服务器版本"""
        try:
            # macOS系统声音
            subprocess.run(
                ["/usr/bin/afplay", "/System/Library/Sounds/Basso.aiff"],
                check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except OSError as e:
            logging.warning(f"播放报警声音失败: {e}")

    def server_log_position_change(
        self,
        change_type: str,
        symbol: str,
        details: Dict,
        before_state: Dict = None,
        after_state: Dict = None,
        success: bool = True,
        error_msg: str = None,
    ):
        """统一的仓位变动日志记录系统

        Args:
            change_type: 变动类型 ('dynamic_tp', 'manual_tp_sl', 'manual_close', 'auto_close')
            symbol: 交易对
            details: 变动详情字典
            before_state: 变动前状态 (可选)
            after_state: 变动后状态 (可选)
            success: 是否成功
            error_msg: 错误信息 (如果失败)
        """
        ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        type_names = {
            "dynamic_tp": "动态止盈调整",
            "manual_tp_sl": "手动修改止盈止损",
            "manual_close": "手动平仓",
            "auto_close": "自动平仓",
        }
        type_name = type_names.get(change_type, change_type)

        inner_lines: List[str] = [
            f"type={type_name} symbol={symbol} success={success} utc={ts_utc}",
        ]
        if details:
            for key, value in details.items():
                inner_lines.append(f"detail {key}={_position_change_fmt_value(value)}")
        if before_state:
            for key, value in before_state.items():
                inner_lines.append(f"before {key}={_position_change_fmt_value(value)}")
        if after_state:
            for key, value in after_state.items():
                inner_lines.append(f"after {key}={_position_change_fmt_value(value)}")
        if not success and error_msg:
            inner_lines.append(f"error {_position_change_fmt_value(error_msg)}")

        asctime = _log_asctime_local()
        for msg in inner_lines:
            logging.info("[仓位变动] %s", msg)

        try:
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            position_log_file = os.path.join(log_dir, "position_changes.log")
            with open(position_log_file, "a", encoding="utf-8") as f:
                for msg in inner_lines:
                    f.write(f"{asctime} - INFO - [仓位变动] {msg}\n")
                f.flush()
        except Exception as e:
            logging.warning(f"写入仓位变动日志失败: {e}")
