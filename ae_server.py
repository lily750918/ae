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


# 日报：每个 UTC 自然日最多自动发送一次（进程重启后仍可读此文件避免当天重复）
DAILY_REPORT_LAST_SENT_FILE = os.path.join(log_dir, "daily_report_last_sent_date.txt")


def _daily_report_load_last_sent_date() -> Optional[date]:
    try:
        if os.path.exists(DAILY_REPORT_LAST_SENT_FILE):
            with open(DAILY_REPORT_LAST_SENT_FILE, "r", encoding="utf-8") as f:
                s = f.read().strip()
            if s:
                return date.fromisoformat(s)
    except (ValueError, OSError) as e:
        logging.debug(f"读取日报日期标记失败: {e}")
    return None


def _daily_report_save_last_sent_date(d: date) -> None:
    try:
        with open(DAILY_REPORT_LAST_SENT_FILE, "w", encoding="utf-8") as f:
            f.write(d.isoformat())
    except Exception as e:
        logging.warning(f"⚠️ 写入日报日期标记失败: {e}")


def _seconds_sleep_until_after_next_utc_midnight(now: datetime) -> float:
    """休眠到下一 UTC 日 00:00 之后再过 120 秒，避免边界上重复触发。"""
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    next_midnight = start + timedelta(days=1)
    return max(60.0, (next_midnight - now).total_seconds() + 120.0)


# 数据库路径
# 算法单工具函数（从 utils 导入）
from utils.orders import (
    FUTURES_ALGO_TP_TYPES,
    FUTURES_ALGO_SL_TYPES,
    futures_algo_trigger_price,
    position_close_side,
    pick_tp_sl_algo_candidates,
    algo_order_id_from_dict,
    cancel_order_algo_or_regular,
)


# ==================== 邮件报警配置 ====================

def _fetch_futures_realized_pnl_window(
    client: Client, start_ms: int, end_ms: int
) -> List[dict]:
    """分页拉取 [start_ms, end_ms] 内全部 REALIZED_PNL 流水（单页最多 1000 条，按时间从旧到新）。"""
    merged: List[dict] = []
    cursor = start_ms
    page_limit = 1000
    while cursor < end_ms:
        try:
            batch = client.futures_income_history(
                startTime=cursor,
                endTime=end_ms,
                incomeType="REALIZED_PNL",
                limit=page_limit,
            )
        except Exception as e:
            logging.warning(f"⚠️ futures_income_history 分页失败 cursor={cursor}: {e}")
            break
        if not batch:
            break
        merged.extend(batch)
        if len(batch) < page_limit:
            break
        try:
            last_t = int(batch[-1]["time"])
        except (TypeError, KeyError, IndexError):
            break
        nxt = last_t + 1
        if nxt <= cursor:
            logging.warning("⚠️ REALIZED_PNL 分页游标未前进，停止拉取")
            break
        cursor = nxt
    return merged


def generate_daily_report() -> str:
    """生成每日交易报告"""
    try:
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("📊 AE交易系统 - 每日交易报告")
        report_lines.append("=" * 60)
        report_lines.append(
            f"📅 报告日期: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        )
        report_lines.append(
            f"⏰ 生成时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        report_lines.append("")

        # 检查strategy是否已初始化
        if strategy is None:
            report_lines.append("⚠️ 策略引擎未初始化，无法获取详细数据")
            report_lines.append("")
        else:
            if not getattr(strategy, "api_configured", True):
                report_lines.append(
                    "⚠️ 仅界面模式（未配置 API），以下不含实盘行情与成交。"
                )
                report_lines.append("")
            # 1. 账户信息
            report_lines.append("💰 账户信息")
            report_lines.append("-" * 30)
            try:
                account_info = strategy.server_get_account_info()
                if account_info:
                    report_lines.append(f"总余额: ${account_info['total_balance']:.2f}")
                    report_lines.append(
                        f"可用余额: ${account_info['available_balance']:.2f}"
                    )
                    report_lines.append(
                        f"未实现盈亏: ${account_info['unrealized_pnl']:.2f}"
                    )
                    report_lines.append(
                        f"维持保证金: ${account_info['maintenance_margin']:.2f}"
                    )
                else:
                    report_lines.append("❌ 无法获取账户信息")
            except Exception as e:
                report_lines.append(f"❌ 获取账户信息失败: {e}")
            report_lines.append("")

            cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
            income_history_24h: List[dict] = []

            # 2. 当前持仓（原「摘要 + 详情」合并为一节，避免重复）
            report_lines.append("📈 当前持仓")
            report_lines.append("-" * 30)
            try:
                if strategy and strategy.positions:
                    with strategy._positions_sync_lock:
                        _report_positions_snapshot = strategy.positions[:]
                    for pos in _report_positions_snapshot:
                        direction = "多头" if pos.get("direction") == "long" else "空头"
                        symbol = pos.get("symbol", "Unknown")
                        entry_time_str = pos.get("entry_time", "Unknown")
                        raw_ep = pos.get("entry_price")
                        quantity = abs(pos.get("quantity", 0))

                        current_price = None
                        pnl_display = "—"
                        pnl_color = "⚪"

                        try:
                            entry_disp = f"${float(raw_ep):.6f}"
                        except (TypeError, ValueError):
                            entry_disp = str(raw_ep) if raw_ep is not None else "—"

                        try:
                            ep = float(raw_ep)
                            if ep == 0:
                                raise ValueError("entry_price 为 0")
                            tk = strategy.client.futures_symbol_ticker(symbol=symbol)
                            current_price = float(tk["price"])
                            if pos.get("direction") == "long":
                                pnl_pct = (current_price - ep) / ep
                            else:
                                pnl_pct = (ep - current_price) / ep
                            position_value = quantity * ep
                            pnl_value = pnl_pct * position_value * strategy.leverage
                            pnl_display = f"${pnl_value:.2f} ({pnl_pct * 100:.2f}%)"
                            if pnl_value > 0:
                                pnl_color = "🟢"
                            elif pnl_value < 0:
                                pnl_color = "🔴"
                            else:
                                pnl_color = "⚪"
                        except Exception as e:
                            pnl_display = f"计算失败: {e}"

                        price_line = (
                            f"${current_price:.6f}"
                            if current_price is not None
                            else "获取失败"
                        )
                        report_lines.append(f"{pnl_color} {symbol} · {direction}")
                        report_lines.append(f"  建仓时间: {entry_time_str}")
                        report_lines.append(f"  建仓价格: {entry_disp}")
                        report_lines.append(f"  持仓数量: {quantity:.0f}")
                        report_lines.append(f"  当前价格: {price_line}")
                        report_lines.append(f"  当前盈亏: {pnl_display}")
                        report_lines.append("")
                else:
                    report_lines.append("无持仓")
            except Exception as e:
                report_lines.append(f"❌ 获取持仓信息失败: {e}")
            report_lines.append("")

            # 3. 过去24小时统计（与第6节均为 REALIZED_PNL 口径；分页拉取全部流水）
            report_lines.append("📊 过去24小时统计（币安 REALIZED_PNL 资金流水）")
            report_lines.append("-" * 30)
            try:
                if (
                    not getattr(strategy, "api_configured", False)
                    or strategy.client is None
                ):
                    report_lines.append("⚠️ 未配置 API，无法拉取流水")
                else:
                    start_ms = int(cutoff_24h.timestamp() * 1000)
                    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                    income_history_24h = _fetch_futures_realized_pnl_window(
                        strategy.client, start_ms, end_ms
                    )

                if income_history_24h:
                    total_24h_pnl = sum(
                        float(record["income"]) for record in income_history_24h
                    )
                    n = len(income_history_24h)

                    report_lines.append(f"已实现盈亏合计: ${total_24h_pnl:.2f}")
                    report_lines.append(
                        f"流水条数: {n}（已全部拉取；平仓分批等会产生多条，不等于策略开平仓次数）"
                    )

                    profitable = len(
                        [r for r in income_history_24h if float(r["income"]) > 0]
                    )
                    non_positive = len(
                        [r for r in income_history_24h if float(r["income"]) <= 0]
                    )

                    report_lines.append(f"盈利条数: {profitable}")
                    report_lines.append(f"非盈利条数: {non_positive}")
                    report_lines.append(
                        f"流水维度胜率: {profitable / n * 100:.1f}%（盈利条数/总条数，非策略胜率）"
                        if n > 0
                        else "流水维度胜率: —"
                    )

                    report_lines.append("")
                    report_lines.append("📌 按交易对汇总（REALIZED_PNL）")
                    report_lines.append("-" * 30)
                    by_sym_sum: Dict[str, float] = defaultdict(float)
                    by_sym_cnt: Dict[str, int] = defaultdict(int)
                    for r in income_history_24h:
                        sym = (r.get("symbol") or "").strip() or "—"
                        by_sym_sum[sym] += float(r["income"])
                        by_sym_cnt[sym] += 1
                    for sym in sorted(by_sym_sum.keys(), key=lambda k: (k == "—", k)):
                        report_lines.append(
                            f"  {sym}: ${by_sym_sum[sym]:.2f}  ({by_sym_cnt[sym]} 条)"
                        )
                    report_lines.append(f"  【总计】${total_24h_pnl:.2f}  共 {n} 条")
                elif (
                    getattr(strategy, "api_configured", False)
                    and strategy.client is not None
                ):
                    report_lines.append("过去24小时无 REALIZED_PNL 流水")
            except Exception as e:
                report_lines.append(f"❌ 获取交易统计失败: {e}")
            report_lines.append("")

            # 4. 最近的仓位变动记录
            report_lines.append("📋 最近仓位变动")
            report_lines.append("-" * 30)
            try:
                # 读取最近的仓位变动日志
                position_log_file = os.path.join(log_dir, "position_changes.log")
                if os.path.exists(position_log_file):
                    with open(position_log_file, "r", encoding="utf-8") as f:
                        lines = f.readlines()

                    # 获取最近24小时的记录
                    recent_changes = []
                    for line in reversed(lines):
                        if "时间:" in line:
                            try:
                                # 解析时间
                                time_str = line.split("时间:")[1].strip()
                                log_time = datetime.strptime(
                                    time_str, "%Y-%m-%d %H:%M:%S UTC"
                                ).replace(tzinfo=timezone.utc)
                                if log_time > cutoff_24h:
                                    recent_changes.append(line.strip())
                            except (ValueError, IndexError):
                                continue

                    if recent_changes:
                        for change in recent_changes[:10]:  # 最多显示10条
                            if "✅" in change and (
                                "手动平仓" in change or "自动平仓" in change
                            ):
                                report_lines.append(change.replace("✅", "•"))
                    else:
                        report_lines.append("过去24小时无仓位变动")
                else:
                    report_lines.append("仓位变动日志文件不存在")
            except Exception as e:
                report_lines.append(f"❌ 读取仓位变动日志失败: {e}")
            report_lines.append("")

            # 5. 系统状态
            report_lines.append("🔧 系统状态")
            report_lines.append("-" * 30)
            try:
                uptime_hours = (
                    (datetime.now(timezone.utc) - start_time).total_seconds() / 3600
                    if start_time
                    else 0
                )
                report_lines.append(f"系统运行时间: {uptime_hours:.1f} 小时")
                report_lines.append(
                    f"持仓监控状态: {'正常' if is_running else '已停止'}"
                )
                report_lines.append(
                    f"当前持仓数量: {len(strategy.positions) if strategy else 0}"
                )
            except Exception as e:
                report_lines.append(f"❌ 获取系统状态失败: {e}")

            # 6. 流水明细（与第3节同一批数据：仅 REALIZED_PNL，全部列出）
            report_lines.append("")
            report_lines.append(
                "📋 过去24小时已实现盈亏流水明细（REALIZED_PNL，按时间升序，不省略）"
            )
            report_lines.append("-" * 30)

            try:
                if not income_history_24h:
                    report_lines.append("无明细（与上一节统计一致）")
                else:
                    sorted_rows = sorted(
                        income_history_24h, key=lambda r: int(r.get("time") or 0)
                    )
                    report_lines.append(f"共 {len(sorted_rows)} 条:")
                    report_lines.append("")

                    for i, record in enumerate(sorted_rows, 1):
                        income = float(record["income"])
                        timestamp = datetime.fromtimestamp(
                            int(record["time"]) / 1000, tz=timezone.utc
                        )
                        symbol = record.get("symbol") or "—"
                        income_type = record.get("incomeType", "REALIZED_PNL")

                        pnl_str = f"+${income:.2f}" if income > 0 else f"${income:.2f}"
                        color = "🟢" if income > 0 else "🔴"

                        report_lines.append(
                            f"{i:4d}. {symbol} | {timestamp.strftime('%Y-%m-%d %H:%M')} UTC | "
                            f"{income_type} | {color}{pnl_str}"
                        )

            except Exception as e:
                report_lines.append(f"❌ 输出流水明细失败: {e}")

        report_lines.append("")
        report_lines.append("---")
        report_lines.append("此报告由AE交易系统自动生成")
        report_lines.append(
            f"服务器: {os.uname().nodename if hasattr(os, 'uname') else 'Unknown'}"
        )

        return "\n".join(report_lines)

    except Exception as e:
        return f"生成报告失败: {e}"


def send_daily_report() -> bool:
    """生成并保存日报，尝试发邮件。报告文件成功写入则返回 True 并标记本 UTC 日已发送。"""
    try:
        report_content = generate_daily_report()

        # 保存报告到文件
        report_file = (
            f"daily_report_{datetime.now(timezone.utc).strftime('%Y%m%d')}.txt"
        )
        report_path = os.path.join(log_dir, report_file)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)

        _daily_report_save_last_sent_date(datetime.now(timezone.utc).date())

        # 发送邮件
        subject = f"每日交易报告 - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        message = f"请查看附件中的每日交易报告。\n\n报告生成时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"

        # 创建带附件的邮件
        msg = MIMEMultipart()
        msg["From"] = os.getenv("SMTP_EMAIL")
        msg["To"] = ALERT_EMAIL
        msg["Subject"] = f"[AE交易系统] {subject}"

        # 邮件正文
        body = MIMEText(message, "plain", "utf-8")
        msg.attach(body)

        # 添加附件
        with open(report_path, "r", encoding="utf-8") as f:
            attachment = MIMEText(f.read(), "plain", "utf-8")
            attachment.add_header(
                "Content-Disposition", "attachment", filename=report_file
            )
            msg.attach(attachment)

        # 发送邮件
        sender_email = os.getenv("SMTP_EMAIL")
        sender_password = os.getenv("SMTP_PASSWORD")

        if not sender_email or not sender_password:
            logging.error("❌ 未配置邮件发送账号")
            return True

        server = smtplib.SMTP_SSL("smtp.163.com", 465)
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, ALERT_EMAIL, msg.as_string())
        server.quit()

        logging.info(f"✅ 每日交易报告已发送到 {ALERT_EMAIL}")
        return True

    except Exception as e:
        logging.error(f"❌ 发送每日报告失败: {e}")
        return False


# 邮件报警（从 utils 导入）
from utils.email import ALERT_EMAIL, send_email_alert


# 昨日数据缓存 + 备用交易对列表（从 utils 导入）
from utils.cache import YesterdayDataCache, BACKUP_SYMBOL_LIST

class AutoExchangeStrategy:
    """自动交易策略核心类"""

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
        import threading

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

    def server_prune_flat_positions_from_exchange(
        self, positions_info: Optional[List] = None
    ) -> int:
        """
        若币安上该合约已无持仓（positionAmt 绝对值视为 0），从 self.positions 移除并保存记录。
        解决：止盈/止损在交易所成交后未走 server_close_position 导致的「幽灵持仓」。

        🔧 v4 修复：
        1. 移除前写入 trade_history.json（修复 #1 交易历史丢失）
        2. 取消该 symbol 所有残留的 algo 挂单（修复 #4 TP/SL 死代码）
        """
        removed = 0
        try:
            if positions_info is None:
                positions_info = self.client.futures_position_information()
        except Exception as e:
            logging.error(f"❌ 同步持仓（拉取交易所）失败: {e}")
            return 0

        eps = 1e-8
        with self._positions_sync_lock:
            for p in self.positions[:]:
                sym = p["symbol"]
                bp = next((x for x in positions_info if x.get("symbol") == sym), None)
                try:
                    amt = (
                        float(bp.get("positionAmt", 0) or 0) if bp is not None else 0.0
                    )
                except (TypeError, ValueError):
                    amt = 0.0
                if abs(amt) >= eps:
                    continue
                logging.warning(
                    f"🧹 {sym} 交易所已无持仓（positionAmt={amt}），从本地 positions 移除"
                )

                # 🔧 修复 #1：写入交易历史（TP/SL 成交后补记）
                try:
                    ticker = self.client.futures_symbol_ticker(symbol=sym)
                    exit_price = float(ticker["price"])
                except Exception:
                    exit_price = p.get("entry_price", 0)

                entry_price = p.get("entry_price", 0)
                direction = p.get("direction", "short")
                if direction == "long":
                    pnl_pct = (
                        (exit_price - entry_price) / entry_price if entry_price else 0
                    )
                else:
                    pnl_pct = (
                        (entry_price - exit_price) / entry_price if entry_price else 0
                    )
                pnl_value = (
                    pnl_pct
                    * p.get("position_value", 0)
                    * p.get("leverage", self.leverage)
                )

                entry_time_dt = None
                try:
                    entry_time_dt = datetime.fromisoformat(p["entry_time"])
                    elapsed_hours = (
                        datetime.now(timezone.utc) - entry_time_dt
                    ).total_seconds() / 3600
                except Exception:
                    elapsed_hours = 0

                reason = "tp_sl_filled"
                reason_cn = "止盈/止损成交"

                try:
                    trade_record = {
                        "symbol": sym,
                        "direction": direction,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "quantity": p.get("quantity", 0),
                        "entry_time": p.get("entry_time", ""),
                        "close_time": datetime.now(timezone.utc).isoformat(),
                        "elapsed_hours": round(elapsed_hours, 2),
                        "pnl_pct": round(pnl_pct * 100, 4),
                        "pnl_value": round(pnl_value, 4),
                        "reason": reason,
                        "reason_cn": reason_cn,
                        "position_value": p.get("position_value", 0),
                        "leverage": p.get("leverage", self.leverage),
                    }
                    if os.path.exists(TRADE_HISTORY_FILE):
                        with open(TRADE_HISTORY_FILE, "r", encoding="utf-8") as f:
                            history = json.load(f)
                    else:
                        history = []
                    history.append(trade_record)
                    with open(TRADE_HISTORY_FILE, "w", encoding="utf-8") as f:
                        json.dump(history, f, ensure_ascii=False, indent=2)
                    logging.info(
                        f"📝 {sym} 补记交易历史: {reason_cn} PnL={pnl_value:.2f} USDT"
                    )

                    # 🛡️ 更新连续亏损计数
                    if pnl_value < 0:
                        self.consecutive_losses += 1
                        self.last_loss_time = datetime.now(timezone.utc)
                        if (
                            self.max_consecutive_losses > 0
                            and self.consecutive_losses >= self.max_consecutive_losses
                        ):
                            self.cooldown_until = datetime.now(
                                timezone.utc
                            ) + timedelta(hours=self.cooldown_hours)
                            logging.warning(
                                f"🔒 {sym} 连续亏损 {self.consecutive_losses} 笔，进入冷却期 {self.cooldown_hours}h"
                            )
                    else:
                        self.consecutive_losses = 0
                        self.cooldown_until = None
                except Exception as th_err:
                    logging.error(f"❌ {sym} 补记交易历史失败: {th_err}")

                # 🔧 修复 #4：取消该 symbol 所有残留的 algo 挂单（TP/SL 死代码补偿）
                try:
                    algo_orders = self.client.futures_get_open_algo_orders(symbol=sym)
                    if algo_orders:
                        for order in algo_orders:
                            self.client.futures_cancel_algo_order(
                                symbol=sym, algoId=order["algoId"]
                            )
                            logging.info(
                                f"✅ {sym} 取消残留挂单: {order['orderType']} (algoId: {order['algoId']})"
                            )
                except Exception as cancel_err:
                    logging.error(f"❌ {sym} 取消残留挂单失败: {cancel_err}")

                try:
                    self.positions.remove(p)
                    removed += 1
                except ValueError:
                    pass
            if removed:
                self.server_save_positions_record()
                logging.info(f"💾 已保存持仓记录（本次移除幽灵仓 {removed} 条）")
                notify_positions_changed()
        return removed

    def server_load_existing_positions(self):
        """启动时从交易所加载现有持仓（并从文件恢复真实建仓时间）- 服务器版本"""
        if not getattr(self, "api_configured", False) or self.client is None:
            logging.warning("🔕 未配置 API：跳过从交易所加载持仓")
            self.positions_loaded = True  # 无需加载
            return
        try:
            logging.info("🔍 加载交易所现有持仓...")

            # 先读取持仓记录文件
            positions_record = self.server_load_positions_record()

            # API调用重试机制
            positions_info = None
            max_retries = 5
            retry_delay = 3  # 秒

            for attempt in range(1, max_retries + 1):
                try:
                    positions_info = self.client.futures_position_information()
                    logging.info(f"✅ 第{attempt}次尝试获取持仓信息成功")
                    break
                except Exception as e:
                    if attempt < max_retries:
                        logging.warning(
                            f"⚠️ 第{attempt}次获取持仓信息失败: {e}，{retry_delay}秒后重试..."
                        )
                        time.sleep(retry_delay)
                    else:
                        logging.error(
                            f"❌ 尝试{max_retries}次后仍无法获取持仓信息: {e}"
                        )
                        self.positions_loaded = False
                        return

            if positions_info is None:
                raise Exception("无法从交易所获取持仓信息")

            loaded_count = 0
            for pos in positions_info:
                position_amt = float(pos["positionAmt"])

                # 加载所有有持仓的交易对（包括做多和做空）
                if position_amt != 0:
                    symbol = pos["symbol"]
                    entry_price = float(pos["entryPrice"])
                    quantity = abs(position_amt)
                    direction = "long" if position_amt > 0 else "short"  # 做多/做空方向

                    # 估算持仓价值（假设使用默认杠杆和仓位比例）
                    position_value = (quantity * entry_price) / self.leverage

                    # 尝试从记录文件获取真实建仓时间和方向（只对程序管理的仓位有效）
                    if symbol in positions_record:
                        # 从记录文件恢复方向信息
                        saved_direction = positions_record[symbol].get(
                            "direction", "short"
                        )
                        # 如果交易所方向与记录文件不一致，优先使用交易所数据
                        if saved_direction != direction:
                            logging.warning(
                                f"⚠️ {symbol} 记录文件方向({saved_direction})与交易所方向({direction})不一致，使用交易所方向"
                            )

                        signal_datetime = positions_record[symbol].get(
                            "signal_datetime"
                        )
                        entry_time_iso = positions_record[symbol]["entry_time"]
                        tp_pct = positions_record[symbol].get(
                            "tp_pct", self.strong_coin_tp_pct
                        )
                        tp_2h_checked = positions_record[symbol].get(
                            "tp_2h_checked", False
                        )
                        tp_12h_checked = positions_record[symbol].get(
                            "tp_12h_checked", False
                        )
                        # 🔧 修复：从记录文件恢复动态止盈标记
                        dynamic_tp_strong = positions_record[symbol].get(
                            "dynamic_tp_strong", False
                        )
                        dynamic_tp_medium = positions_record[symbol].get(
                            "dynamic_tp_medium", False
                        )
                        dynamic_tp_weak = positions_record[symbol].get(
                            "dynamic_tp_weak", False
                        )
                        is_consecutive_confirmed = positions_record[symbol].get(
                            "is_consecutive_confirmed", False
                        )
                        logging.info(
                            f"✅ {symbol} 从记录文件恢复建仓时间: {entry_time_iso}"
                        )

                        # 🔧 修复：即使从文件恢复，也要检查是否已超过窗口
                        try:
                            entry_time_dt = datetime.fromisoformat(entry_time_iso)
                            elapsed_hours = (
                                datetime.now(timezone.utc) - entry_time_dt
                            ).total_seconds() / 3600

                            # 如果持仓时间已超过检查窗口，强制标记为已检查
                            if elapsed_hours >= 2.5 and not tp_2h_checked:
                                tp_2h_checked = True
                                logging.info(
                                    f"  • {symbol} 持仓{elapsed_hours:.1f}h，已超过2h窗口，强制标记为已检查"
                                )

                            if elapsed_hours >= 12.5 and not tp_12h_checked:
                                tp_12h_checked = True
                                logging.info(
                                    f"  • {symbol} 持仓{elapsed_hours:.1f}h，已超过12h窗口，强制标记为已检查"
                                )
                        except Exception as e:
                            logging.warning(f"  • {symbol} 计算持仓时间失败: {e}")
                    else:
                        # 如果文件中没有记录，查询交易历史
                        signal_datetime = None
                        entry_time_iso = self.server_get_entry_time_from_trades(symbol)
                        tp_pct = self.strong_coin_tp_pct
                        tp_2h_checked = False
                        tp_12h_checked = False
                        if direction == "short":
                            logging.warning(
                                f"⚠️ {symbol} 做空仓位记录文件中无数据，从交易历史查询"
                            )
                        else:
                            logging.info(
                                f"ℹ️ {symbol} 做多仓位，不在程序管理范围内，使用交易所数据"
                            )

                    # 🔧 修复：计算持仓时间，如果已超过检查窗口，直接标记为已检查
                    try:
                        entry_time_dt = datetime.fromisoformat(entry_time_iso)
                        elapsed_hours = (
                            datetime.now(timezone.utc) - entry_time_dt
                        ).total_seconds() / 3600

                        # 如果持仓时间已超过检查窗口，标记为已检查（避免永远显示"未检查"）
                        if elapsed_hours >= 2.5:
                            tp_2h_checked = True
                            logging.info(
                                f"  • {symbol} 持仓{elapsed_hours:.1f}h，已超过2h窗口，标记为已检查"
                            )

                        if elapsed_hours >= 12.5:
                            tp_12h_checked = True
                            logging.info(
                                f"  • {symbol} 持仓{elapsed_hours:.1f}h，已超过12h窗口，标记为已检查"
                            )
                    except Exception as e:
                        logging.warning(f"  • {symbol} 计算持仓时间失败: {e}")

                    # 创建持仓记录
                    position = {
                        "symbol": symbol,
                        "direction": direction,  # 🔥 新增：仓位方向
                        "signal_datetime": signal_datetime,  # 🔥 新增：信号时间
                        "entry_price": entry_price,
                        "entry_time": entry_time_iso,
                        "quantity": quantity,
                        "position_value": position_value,
                        "surge_ratio": 0.0,  # 未知
                        "leverage": self.leverage,
                        "tp_pct": tp_pct,
                        "tp_2h_checked": tp_2h_checked,
                        "tp_12h_checked": tp_12h_checked,
                        # 🔧 修复：添加动态止盈标记（从文件恢复或初始化为False）
                        "dynamic_tp_strong": dynamic_tp_strong
                        if "dynamic_tp_strong" in locals()
                        else False,
                        "dynamic_tp_medium": dynamic_tp_medium
                        if "dynamic_tp_medium" in locals()
                        else False,
                        "dynamic_tp_weak": dynamic_tp_weak
                        if "dynamic_tp_weak" in locals()
                        else False,
                        "is_consecutive_confirmed": is_consecutive_confirmed
                        if "is_consecutive_confirmed" in locals()
                        else False,
                        "status": "normal",
                        "order_id": 0,
                        "loaded_from_exchange": True,  # 标记为从交易所加载
                    }

                    with self._positions_sync_lock:
                        self.positions.append(position)
                    loaded_count += 1

                    direction_cn = "多头" if direction == "long" else "空头"
                    logging.info(
                        f"✅ 加载持仓: {symbol} {direction_cn} 开仓价:{entry_price:.6f} 数量:{quantity:.0f}"
                    )

            if loaded_count > 0:
                logging.info(f"🎉 成功加载 {loaded_count} 个现有持仓")
            else:
                logging.info("📭 无现有持仓")

            # 重启后从交易历史恢复今日建仓计数（避免只统计未平仓仓位导致绕过风控）
            # 🔧 v4 修复 #11：从 trade_history.json 统计今日已建仓数
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            today_count = 0
            try:
                if os.path.exists(TRADE_HISTORY_FILE):
                    with open(TRADE_HISTORY_FILE, "r", encoding="utf-8") as f:
                        history = json.load(f)
                    today_count = sum(
                        1
                        for t in history
                        if t.get("close_time", "").startswith(today)
                        and t.get("reason") == "tp_sl_filled"
                    )
            except Exception as e:
                logging.error(f"❌ 从交易历史恢复建仓计数失败: {e}")

            # 加上当前仍持有的今日建仓
            today_open = sum(
                1 for p in self.positions if p.get("entry_time", "").startswith(today)
            )
            today_count = max(today_count, today_open)  # 取较大值避免历史文件缺失
            if today_count > 0:
                self.daily_entries = today_count
                self.last_entry_date = today
                logging.info(f"📅 恢复今日建仓计数: {today_count}")

            self.positions_loaded = True

        except Exception as e:
            logging.error(f"❌ 加载现有持仓失败: {e}")
            self.positions_loaded = False

    def server_load_positions_record(self) -> Dict:
        """从文件加载持仓记录（兼容旧版本数据，自动补充缺失的ID字段）- 服务器版本"""
        try:
            if os.path.exists(POSITIONS_RECORD_FILE):
                with open(POSITIONS_RECORD_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # ✨ 兼容性处理：为旧记录补充position_id
                modified = False
                for symbol, position in data.items():
                    if "position_id" not in position or not position["position_id"]:
                        position["position_id"] = str(uuid.uuid4())
                        modified = True
                        logging.info(
                            f"🔄 {symbol} 旧持仓记录已补充ID: {position['position_id'][:8]}"
                        )

                    # 补充direction字段（如果不存在，默认做空）
                    if "direction" not in position:
                        position["direction"] = "short"  # 默认做空，保持向后兼容
                        modified = True
                        logging.info(f"🔄 {symbol} 旧持仓记录已补充direction: short")

                    # 补充tp_order_id和sl_order_id字段（如果不存在）
                    if "tp_order_id" not in position:
                        position["tp_order_id"] = None
                        modified = True
                    if "sl_order_id" not in position:
                        position["sl_order_id"] = None
                        modified = True

                # 如果有修改，保存回文件
                if modified:
                    with open(POSITIONS_RECORD_FILE, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    logging.info("💾 已保存补充ID后的持仓记录")

                return data
            else:
                logging.info("📄 持仓记录文件不存在，将创建新文件")
                return {}
        except Exception as e:
            logging.error(f"❌ 读取持仓记录文件失败: {e}")
            return {}

    def server_load_position_record(self, symbol: str):
        """从文件加载单个持仓记录 - 服务器版本

        Args:
            symbol: 交易对符号

        Returns:
            持仓记录字典，如果不存在返回None
        """
        all_records = self.server_load_positions_record()
        return all_records.get(symbol)

    def server_save_positions_record(self):
        """保存持仓记录到文件 - 服务器版本

        🔧 v4 修复：使用全局写锁防止多线程并发损坏 JSON 文件
        """
        try:
            with self._positions_sync_lock:
                record = {}
                for position in self.positions:
                    symbol = position["symbol"]
                    record[symbol] = {
                        "symbol": symbol,
                        "direction": position.get("direction", "short"),
                        "signal_datetime": position.get("signal_datetime"),
                        "entry_time": position["entry_time"],
                        "entry_price": position["entry_price"],
                        "quantity": position["quantity"],
                        "tp_pct": position.get("tp_pct", self.strong_coin_tp_pct),
                        "tp_2h_checked": position.get("tp_2h_checked", False),
                        "tp_12h_checked": position.get("tp_12h_checked", False),
                        "dynamic_tp_strong": position.get("dynamic_tp_strong", False),
                        "dynamic_tp_medium": position.get("dynamic_tp_medium", False),
                        "dynamic_tp_weak": position.get("dynamic_tp_weak", False),
                        "is_consecutive_confirmed": position.get(
                            "is_consecutive_confirmed", False
                        ),
                        "tp_history": position.get("tp_history", []),
                        "last_update": datetime.now(timezone.utc).isoformat(),
                    }

                tmp_file = POSITIONS_RECORD_FILE + ".tmp"
                with open(tmp_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False)
                os.replace(tmp_file, POSITIONS_RECORD_FILE)

            logging.debug(f"💾 已保存 {len(record)} 个持仓记录")
        except Exception as e:
            logging.error(f"❌ 保存持仓记录失败: {e}")

    def server_get_entry_time_from_trades(self, symbol: str) -> str:
        """从交易历史查询建仓时间（备用方案）- 服务器版本"""
        try:
            trades = self.client.futures_account_trades(symbol=symbol, limit=50)
            if trades:
                # 找到最早的建仓交易
                sorted_trades = sorted(trades, key=lambda x: x["time"])
                entry_time = datetime.fromtimestamp(
                    sorted_trades[0]["time"] / 1000, tz=timezone.utc
                )
                logging.info(
                    f"📅 {symbol} 从交易历史查询到建仓时间: {entry_time.isoformat()}"
                )
                return entry_time.isoformat()
            else:
                # 如果查询失败，使用当前时间
                logging.warning(f"⚠️ {symbol} 交易历史为空，使用当前时间")
                return datetime.now(timezone.utc).isoformat()
        except Exception as e:
            logging.error(f"❌ {symbol} 查询交易历史失败: {e}")
            return datetime.now(timezone.utc).isoformat()

    def _server_check_consecutive_surge(self, position: Dict) -> bool:
        """检查该持仓在建仓时是否为连续2小时卖量暴涨（API版本）- 服务器版本

        判断逻辑（基于hm1l.py的逻辑）：
        1. 获取信号发生时间（第1小时）
        2. 建仓时间 = 信号时间 + 1小时（第2小时）
        3. 检查信号小时和建仓小时是否都有卖量>=10倍
        4. 如果是，返回True（连续确认）

        Args:
            position: 持仓信息

        Returns:
            bool: 是否为连续2小时确认
        """
        symbol = position.get("symbol", "Unknown")
        try:
            signal_datetime_str = position.get("signal_datetime")

            if not signal_datetime_str:
                logging.debug(f"❌ {symbol} 无signal_datetime，无法判断连续确认")
                return False

            # 解析信号时间（第1小时）
            if isinstance(signal_datetime_str, str):
                try:
                    signal_dt = datetime.strptime(
                        signal_datetime_str, "%Y-%m-%d %H:%M:%S UTC"
                    )
                    signal_dt = signal_dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    try:
                        signal_dt = datetime.fromisoformat(
                            signal_datetime_str.replace("Z", "+00:00")
                        )
                    except ValueError:
                        try:
                            signal_dt = datetime.strptime(
                                signal_datetime_str, "%Y-%m-%d %H:%M"
                            )
                            signal_dt = signal_dt.replace(tzinfo=timezone.utc)
                        except ValueError as e:
                            logging.warning(
                                f"无法解析信号时间格式 {signal_datetime_str}: {e}"
                            )
                            return False
            else:
                signal_dt = signal_datetime_str

            # 确保时区
            if signal_dt.tzinfo is None:
                signal_dt = signal_dt.replace(tzinfo=timezone.utc)

            # 建仓时间 = 信号时间 + 1小时（第2小时）
            entry_dt = signal_dt + timedelta(hours=1)

            # 步骤1：获取昨日平均小时卖量（从缓存）
            yesterday_avg_hour_sell = self.yesterday_cache.get_yesterday_avg_sell_api(
                symbol
            )
            if not yesterday_avg_hour_sell or yesterday_avg_hour_sell <= 0:
                logging.debug(f"❌ {symbol} 昨日数据缺失，无法判断连续确认")
                return False

            # 步骤2：从API获取信号小时和建仓小时的K线数据
            signal_hour_ms = int(signal_dt.timestamp() * 1000)
            entry_hour_ms = int(entry_dt.timestamp() * 1000)

            # 获取2小时的K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval="1h",
                startTime=signal_hour_ms,
                endTime=entry_hour_ms,
                limit=2,
            )

            if len(klines) < 2:
                logging.debug(
                    f"❌ {symbol} 小时数据不足（{len(klines)}条），无法判断连续确认"
                )
                return False

            # 计算每小时的卖量倍数
            threshold = self.sell_surge_threshold  # 10倍
            ratios = []
            hour_times = []

            for kline in klines:
                hour_volume = float(kline[5])  # 总成交量
                hour_active_buy = float(kline[9])  # 主动买入量
                hour_sell_volume = hour_volume - hour_active_buy
                ratio = hour_sell_volume / yesterday_avg_hour_sell
                ratios.append(ratio)
                hour_times.append(
                    datetime.fromtimestamp(
                        int(kline[0]) / 1000, tz=timezone.utc
                    ).strftime("%H:%M")
                )

            # 判断两个小时都>=10倍
            if len(ratios) >= 2 and all(r >= threshold for r in ratios[-2:]):
                logging.info(
                    f"✅ {symbol} 确认为连续2小时卖量暴涨：\n"
                    f"  • 信号小时({hour_times[-2]}): {ratios[-2]:.2f}x\n"
                    f"  • 建仓小时({hour_times[-1]}): {ratios[-1]:.2f}x\n"
                    f"  • 阈值: {threshold}x"
                )
                return True
            else:
                logging.debug(
                    f"❌ {symbol} 非连续确认（倍数: 信号{ratios[-2]:.2f}x, 建仓{ratios[-1]:.2f}x < {threshold}x）"
                )
                return False

        except Exception as e:
            logging.warning(f"⚠️ {symbol} 检查连续确认失败: {e}")
            import traceback

            logging.debug(f"异常堆栈:\n{traceback.format_exc()}")
            return False

    def server_calculate_intraday_buy_surge_ratio(
        self, symbol: str, signal_datetime: str
    ) -> float:
        """
        计算当日买量倍数：信号发生前12小时，每小时买量相对前一小时的最大比值

        这个指标反映了短期买量的爆发性，用于过滤多空博弈信号

        Args:
            symbol: 交易对
            signal_datetime: 信号时间 'YYYY-MM-DD HH:MM:SS UTC'

        Returns:
            float: 当日买量倍数（最大的小时间买量比值），如果数据不足返回0
        """
        try:
            # 解析信号时间
            signal_dt = datetime.strptime(
                signal_datetime, "%Y-%m-%d %H:%M:%S UTC"
            ).replace(tzinfo=timezone.utc)

            # 以 (symbol, 小时) 为 key 做缓存，同一小时内重复计算直接返回
            cache_key = (symbol, signal_dt.strftime("%Y-%m-%d %H"))
            with self._intraday_ratio_lock:
                if cache_key in self._intraday_ratio_cache:
                    logging.debug(
                        f"📊 {symbol} 当日买量倍数命中缓存: {self._intraday_ratio_cache[cache_key]:.2f}倍"
                    )
                    return self._intraday_ratio_cache[cache_key]

            # 计算时间范围：信号前12小时
            start_time = signal_dt - timedelta(hours=12)
            end_time = signal_dt

            logging.debug(
                f"📊 {symbol} 查询当日买量倍数，时间范围: {start_time} ~ {end_time}"
            )

            # 获取小时K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval="1h",
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=12,  # 获取最近12小时的数据
            )

            if not klines or len(klines) < 2:
                logging.debug(f"⚠️ {symbol} 数据不足（<2小时），无法计算当日买量倍数")
                return 0.0

            # 计算每小时的主动买量比值
            max_ratio = 0.0
            for i in range(1, len(klines)):
                prev_kline = klines[i - 1]
                curr_kline = klines[i]

                prev_buy_vol = float(prev_kline[9])  # taker_buy_volume
                curr_buy_vol = float(curr_kline[9])  # taker_buy_volume

                if prev_buy_vol > 0:
                    ratio = curr_buy_vol / prev_buy_vol
                    max_ratio = max(max_ratio, ratio)

            if max_ratio > 0:
                logging.debug(
                    f"📊 {symbol} 当日买量倍数: {max_ratio:.2f}倍（信号前12小时最大小时间比值）"
                )
            else:
                logging.debug(f"⚠️ {symbol} 未计算出有效的当日买量倍数（max_ratio=0）")

            # 写入缓存，同一小时内重复调用直接命中
            with self._intraday_ratio_lock:
                self._intraday_ratio_cache[cache_key] = max_ratio
                # 防止缓存无限增长，超过 500 条时清空过期条目
                if len(self._intraday_ratio_cache) > 500:
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
                    cutoff_str = cutoff.strftime("%Y-%m-%d %H")
                    self._intraday_ratio_cache = {
                        k: v
                        for k, v in self._intraday_ratio_cache.items()
                        if k[1] >= cutoff_str
                    }
            return max_ratio

        except Exception as e:
            logging.warning(f"⚠️ 计算当日买量倍数失败 {symbol}: {e}")
            return 0.0

    def server_get_account_balance(self) -> float:
        """获取账户USDT余额 - 服务器版本"""
        if not getattr(self, "api_configured", False) or self.client is None:
            return 0.0
        try:
            account = self.client.futures_account()
            for asset in account["assets"]:
                if asset["asset"] == "USDT":
                    balance = float(asset["walletBalance"])
                    logging.info(f"💰 账户余额: ${balance:.2f} USDT")
                    return balance
            return 0.0
        except Exception as e:
            logging.error(f"❌ 获取账户余额失败: {e}")
            return 0.0

    def server_sync_wallet_snapshot(self) -> bool:
        """从 futures_account 同步总钱包与可开仓余额（一次请求，不含今日盈亏等重逻辑）。"""
        if not getattr(self, "api_configured", False) or self.client is None:
            return False
        try:
            acc = self.client.futures_account()
            self.account_balance = float(acc["totalWalletBalance"])
            self.account_available_balance = float(acc["availableBalance"])
            return True
        except Exception as e:
            logging.warning(f"⚠️ 同步钱包快照失败（futures_account）: {e}")
            return False

    def server_get_account_info(self) -> Optional[Dict]:
        """获取账户详细信息（余额、可用余额、未实现盈亏、今日盈亏）- 服务器版本"""
        if not getattr(self, "api_configured", False) or self.client is None:
            return None
        try:
            # 获取账户信息
            account_info = self.client.futures_account()

            # 总余额
            total_balance = float(account_info["totalWalletBalance"])

            # 可用余额
            available_balance = float(account_info["availableBalance"])

            # 未实现盈亏
            unrealized_pnl = float(account_info["totalUnrealizedProfit"])

            # 今日盈亏（通过收入记录计算）
            daily_pnl = self.server_get_daily_pnl()

            # 维持保证金（可选字段，可能不存在）
            maintenance_margin = float(account_info.get("totalMaintMargin", 0))

            return {
                "total_balance": total_balance,
                "available_balance": available_balance,
                "unrealized_pnl": unrealized_pnl,
                "maintenance_margin": maintenance_margin,
                "daily_pnl": daily_pnl,
            }
        except Exception as e:
            logging.error(f"❌ 获取账户详细信息失败: {e}")
            return None

    def server_get_exchange_status(self) -> Dict:
        """探测币安 U 本位合约连通性（ping + 可选服务器时间），用于监控页展示。"""
        base = {
            "ok": False,
            "exchange": "Binance",
            "market": "USDT-M 合约",
            "message": "",
            "latency_ms": None,
            "server_time_iso": None,
        }
        if not getattr(self, "api_configured", False) or self.client is None:
            base["message"] = "未配置 API"
            return base
        t0 = time.perf_counter()
        try:
            self.client.futures_ping()
            latency_ms = (time.perf_counter() - t0) * 1000
        except Exception as e:
            base["message"] = str(e)[:300] or "futures_ping 失败"
            return base

        server_time_iso: Optional[str] = None
        ft = getattr(self.client, "futures_time", None)
        if callable(ft):
            try:
                r = ft()
                if isinstance(r, dict):
                    st = r.get("serverTime")
                    if st is not None:
                        server_time_iso = datetime.fromtimestamp(
                            int(st) / 1000, tz=timezone.utc
                        ).isoformat()
            except (ValueError, TypeError, OSError) as e:
                logging.debug(f"获取服务器时间失败: {e}")

        base["ok"] = True
        base["message"] = "连接正常"
        base["latency_ms"] = round(latency_ms, 2)
        base["server_time_iso"] = server_time_iso
        return base

    def server_get_daily_pnl(self) -> float:
        """获取今日盈亏（UTC 0点至今的已实现盈亏）- 服务器版本"""
        if not getattr(self, "api_configured", False) or self.client is None:
            return 0.0
        try:
            # 获取今日UTC 0:00的时间戳
            now_utc = datetime.now(timezone.utc)
            today_start = datetime(
                now_utc.year, now_utc.month, now_utc.day, 0, 0, 0, tzinfo=timezone.utc
            )
            start_timestamp = int(today_start.timestamp() * 1000)

            # 查询今日收入记录
            income_history = self.client.futures_income_history(
                startTime=start_timestamp, incomeType="REALIZED_PNL"
            )

            # 累计今日已实现盈亏
            daily_pnl = sum(float(record["income"]) for record in income_history)

            return daily_pnl
        except Exception as e:
            logging.warning(f"⚠️ 获取今日盈亏失败: {e}")
            return 0.0

    def _server_get_active_symbols(self) -> List[str]:
        """获取活跃交易对列表（API方式）- 服务器版本，同时更新 exchange_info 缓存"""
        try:
            # 获取所有U本位期货交易对
            exchange_info = self.client.futures_exchange_info()

            # 顺带更新 exchange_info 缓存，供 _get_symbol_info() 使用（避免建仓时重复调用）
            self._exchange_info_cache = {
                s["symbol"]: s for s in exchange_info["symbols"]
            }
            self._exchange_info_updated_at = datetime.now(timezone.utc)

            symbols = []
            for s in exchange_info["symbols"]:
                symbol = s["symbol"]
                # 只筛选USDT永续合约，并且状态为TRADING
                if (
                    symbol.endswith("USDT")
                    and s["status"] == "TRADING"
                    and s["contractType"] == "PERPETUAL"
                ):
                    symbols.append(symbol)

            logging.info(
                f"✅ 获取到 {len(symbols)} 个活跃USDT合约，exchange_info 缓存已更新"
            )
            return sorted(symbols)

        except Exception as e:
            logging.error(f"❌ 获取交易对列表失败: {e}，使用备用列表")
            return BACKUP_SYMBOL_LIST

    def _get_symbol_info(self, symbol: str) -> Optional[dict]:
        """获取单个交易对的 exchange_info（优先读本地缓存，过期则重新拉取）"""
        now = datetime.now(timezone.utc)
        cache_expired = (
            self._exchange_info_updated_at is None
            or (now - self._exchange_info_updated_at).total_seconds() > 3600
        )
        if cache_expired or symbol not in self._exchange_info_cache:
            try:
                info = self.client.futures_exchange_info()
                self._exchange_info_cache = {s["symbol"]: s for s in info["symbols"]}
                self._exchange_info_updated_at = now
                logging.info("🔄 exchange_info 缓存已刷新")
            except Exception as e:
                logging.error(f"❌ 刷新 exchange_info 缓存失败: {e}")
        return self._exchange_info_cache.get(symbol)

    def server_scan_sell_surge_signals(self) -> List[Dict]:
        """扫描卖量暴涨信号（API实时版本）- 服务器版本"""
        try:
            logging.info("🔍 开始扫描卖量暴涨信号（API模式）...")
            signals = []

            # 获取当前UTC时间
            now_utc = datetime.now(timezone.utc)
            current_hour = now_utc.replace(minute=0, second=0, microsecond=0)

            # 获取交易对列表（同时刷新 exchange_info 缓存）
            symbols = self._server_get_active_symbols()
            logging.info(f"📊 开始扫描 {len(symbols)} 个交易对...")

            # 并发预热昨日数据缓存，避免扫描主循环中逐个发 API 请求
            self.yesterday_cache.prefetch_all(symbols)

            check_hour = current_hour - timedelta(hours=1)
            check_hour_ms = int(check_hour.timestamp() * 1000)

            def _scan_one(symbol: str) -> Optional[Dict]:
                """扫描单个交易对，返回信号 dict 或 None"""
                try:
                    # 1. 从缓存获取昨日平均小时卖量（预热后命中缓存，无网络请求）
                    yesterday_avg_hour_sell = (
                        self.yesterday_cache.get_yesterday_avg_sell_api(symbol)
                    )
                    if not yesterday_avg_hour_sell or yesterday_avg_hour_sell <= 0:
                        return None

                    # 2. 获取上一个完整小时的K线（刚刚完成的小时）
                    klines = self.client.futures_klines(
                        symbol=symbol,
                        interval="1h",
                        startTime=check_hour_ms,
                        limit=2,  # 获取上一小时和当前小时
                    )

                    if not klines or len(klines) < 1:
                        return None

                    # 上一小时数据
                    hour_kline = klines[0]
                    # 校验K线时间戳，防止API因历史数据缺失顺延返回"当前小时"被误认为"上一小时"
                    if int(hour_kline[0]) != check_hour_ms:
                        return None

                    hour_volume = float(hour_kline[5])  # 总成交量
                    hour_active_buy = float(hour_kline[9])  # 主动买入量
                    hour_sell_volume = hour_volume - hour_active_buy
                    hour_close = float(hour_kline[4])

                    # 计算暴涨倍数
                    surge_ratio = hour_sell_volume / yesterday_avg_hour_sell

                    # 3. 检查是否满足阈值
                    if not (
                        self.sell_surge_threshold <= surge_ratio <= self.sell_surge_max
                    ):
                        return None

                    # 获取信号价格（使用下一小时开盘价，如果存在）
                    if len(klines) >= 2:
                        signal_price = float(klines[1][1])  # 下一小时开盘价
                        logging.info(
                            f"📊 {symbol} 信号价格: 使用下一小时开盘价 {signal_price:.6f}"
                        )
                    else:
                        signal_price = hour_close
                        logging.info(
                            f"📊 {symbol} 信号价格: 下一小时未生成，使用当前小时收盘价 {signal_price:.6f}"
                        )

                    # 🆕 检查当日买量倍数风控
                    signal_time_utc = datetime.fromtimestamp(
                        int(hour_kline[0]) / 1000, tz=timezone.utc
                    )
                    signal_time_str = signal_time_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

                    intraday_buy_ratio = 0.0
                    if self.enable_intraday_buy_ratio_filter:
                        try:
                            intraday_buy_ratio = (
                                self.server_calculate_intraday_buy_surge_ratio(
                                    symbol, signal_time_str
                                )
                            )
                        except Exception as e:
                            logging.debug(f"计算当日买量倍数失败 {symbol}: {e}")

                    # 🔥 风控：当日买量倍数区间过滤（过滤多空博弈信号）
                    if intraday_buy_ratio > 0 and self.enable_intraday_buy_ratio_filter:
                        for (
                            danger_min,
                            danger_max,
                        ) in self.intraday_buy_ratio_danger_ranges:
                            if danger_min <= intraday_buy_ratio <= danger_max:
                                logging.warning(
                                    f"🚫 {symbol} 当日买量倍数风控过滤信号: {intraday_buy_ratio:.2f}倍在危险区间[{danger_min}, {danger_max}]（卖量暴涨{surge_ratio:.2f}倍但买量也暴涨，疑似多空博弈信号）"
                                )
                                return None  # 跳过这个信号

                    logging.info(
                        f"🔥 发现信号: {symbol} 卖量暴涨 {surge_ratio:.2f}倍 @ {signal_price:.6f} (买量倍数:{intraday_buy_ratio:.2f}倍) (时间: {signal_time_utc.strftime('%Y-%m-%d %H:%M UTC')})"
                    )
                    return {
                        "symbol": symbol,
                        "surge_ratio": surge_ratio,
                        "price": signal_price,
                        "signal_time": signal_time_str,
                        "hour_sell_volume": hour_sell_volume,
                        "yesterday_avg": yesterday_avg_hour_sell,
                        "intraday_buy_ratio": intraday_buy_ratio,
                    }

                except BinanceAPIException as e:
                    logging.debug(f"扫描 {symbol} 失败(API): {e}")
                    return None
                except (OSError, TimeoutError) as e:
                    logging.warning(f"扫描 {symbol} 网络错误: {e}")
                    return None
                except Exception as e:
                    logging.warning(f"扫描 {symbol} 异常: {e}")
                    return None

            # 并发扫描所有交易对（max_workers=10 控制并发，匹配底层连接池以免阻塞）
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures_map = {executor.submit(_scan_one, sym): sym for sym in symbols}
                for future in as_completed(futures_map):
                    try:
                        result = future.result()
                        if result is not None:
                            signals.append(result)
                    except BinanceAPIException as e:
                        logging.warning(f"并发扫描异常(API): {e}")
                    except (OSError, TimeoutError) as e:
                        logging.warning(f"并发扫描网络错误: {e}")

            logging.info(f"✅ API扫描完成，共发现 {len(signals)} 个信号")
            return sorted(signals, key=lambda x: x["surge_ratio"], reverse=True)

        except Exception as e:
            logging.error(f"❌ API扫描信号失败: {e}")
            return []

    def server_check_position_limits(self) -> Tuple[bool, set]:
        """检查持仓限制 - 服务器版本

        Returns:
            (passed, exchange_symbols):
                passed: True 表示通过限制检查，可以继续开仓
                exchange_symbols: 交易所当前活跃持仓的 symbol 集合（用于去重）；
                                  API 失败降级时返回内存中的 symbol 集合
        """
        # 🔧 修复：从交易所API获取实际持仓数量，而不是仅检查内存中的记录
        exchange_symbols: set = set()
        try:
            # 🔧 API调用重试机制
            actual_positions = None
            max_retries = 3
            retry_delay = 2  # 秒

            for attempt in range(1, max_retries + 1):
                try:
                    actual_positions = self.client.futures_position_information()
                    break
                except Exception as e:
                    if attempt < max_retries:
                        logging.warning(
                            f"⚠️ 第{attempt}次获取持仓信息失败，{retry_delay}秒后重试..."
                        )
                        time.sleep(retry_delay)
                    else:
                        logging.error(
                            f"❌ 尝试{max_retries}次后仍无法获取持仓信息: {e}"
                        )
                        raise

            if actual_positions is None:
                raise Exception("无法从交易所获取持仓信息")

            # 过滤出真实持仓（持仓数量>0）
            active_positions = [
                p for p in actual_positions if float(p["positionAmt"]) != 0
            ]
            actual_count = len(active_positions)
            exchange_symbols = {p["symbol"] for p in active_positions}

            logging.info(
                f"📊 持仓检查: 内存记录={len(self.positions)}, 交易所实际={actual_count}, 上限={self.max_positions}"
            )

            if actual_count >= self.max_positions:
                logging.warning(
                    f"⚠️ 交易所实际持仓数 {actual_count} 已达到上限 {self.max_positions}"
                )
                return False, exchange_symbols
        except Exception as e:
            logging.error(f"❌ 获取交易所持仓信息失败: {e}，使用内存记录")
            # 降级：用内存中的 symbol 集合
            with self._positions_sync_lock:
                exchange_symbols = {p["symbol"] for p in self.positions}
            if len(exchange_symbols) >= self.max_positions:
                logging.warning(f"⚠️ 已达到最大持仓数 {self.max_positions}")
                return False, exchange_symbols

        # 检查每日建仓数（重置计数器）
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.last_entry_date != today:
            self.daily_entries = 0
            self.last_entry_date = today
            logging.info("📅 新的一天开始，建仓计数器已重置")

        if self.daily_entries >= self.max_daily_entries:
            logging.warning(
                f"⚠️ 今日已达到最大建仓数 {self.daily_entries}/{self.max_daily_entries}"
            )
            return False, exchange_symbols

        # 🛡️ 方案 1：每日最大亏损检查
        if self.max_daily_loss_pct > 0:
            try:
                if os.path.exists(TRADE_HISTORY_FILE):
                    with open(TRADE_HISTORY_FILE, "r", encoding="utf-8") as f:
                        history = json.load(f)
                    today_trades = [
                        t for t in history if t.get("close_time", "").startswith(today)
                    ]
                    today_pnl = sum(t.get("pnl_value", 0) for t in today_trades)
                    if today_pnl < 0:
                        daily_loss_pct = (
                            abs(today_pnl) / self.account_balance * 100
                            if self.account_balance > 0
                            else 0
                        )
                        if daily_loss_pct >= self.max_daily_loss_pct:
                            logging.warning(
                                f"🛑 当日亏损 {daily_loss_pct:.1f}% 已达上限 {self.max_daily_loss_pct}%，"
                                f"累计亏损 {today_pnl:.2f} USDT，停止开仓"
                            )
                            return False, exchange_symbols
                        logging.info(
                            f"📊 当日亏损 {daily_loss_pct:.1f}% / 上限 {self.max_daily_loss_pct}%"
                        )
            except Exception as e:
                logging.error(f"❌ 检查每日亏损失败: {e}")

        # 🛡️ 方案 2：连续亏损熔断
        if (
            self.max_consecutive_losses > 0
            and self.consecutive_losses >= self.max_consecutive_losses
        ):
            if self.cooldown_until and datetime.now(timezone.utc) < self.cooldown_until:
                remaining = (
                    self.cooldown_until - datetime.now(timezone.utc)
                ).total_seconds() / 3600
                logging.warning(
                    f"🔒 连续亏损 {self.consecutive_losses} 笔，冷却中（剩余 {remaining:.1f}h），停止开仓"
                )
                return False, exchange_symbols
            else:
                # 冷却期已过，重置
                self.consecutive_losses = 0
                self.cooldown_until = None
                logging.info("✅ 连续亏损冷却期已结束，恢复正常交易")

        # 每小时最多 1 单（旧版行为）；关闭后与 hm1l 默认一致，同一 UTC 小时可多次建仓
        if self.enable_hourly_entry_limit:
            current_hour = datetime.now(timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )
            if self.last_entry_hour == current_hour:
                logging.warning(
                    f"⚠️ 本小时已建仓，请等待下一个小时 (当前: {current_hour.strftime('%H:00 UTC')})"
                )
                return False, exchange_symbols

        return True, exchange_symbols

    def check_sufficient_funds(self, required_margin: float) -> bool:
        """检查是否有足够的可用资金（要求至少15%可用资金余量）"""
        try:
            account_info = self.client.futures_account()
            available_balance = float(account_info["availableBalance"])
            total_balance = float(account_info["totalWalletBalance"])

            # 计算需要的最小可用资金（除了建仓保证金，还要留15%余量）
            min_required = required_margin * 1.15

            # 同时检查绝对金额和比例
            available_ratio = (
                available_balance / total_balance if total_balance > 0 else 0
            )

            logging.info(
                f"💰 资金检查: 可用余额${available_balance:.2f} ({available_ratio * 100:.1f}%), 需要${min_required:.2f}"
            )

            if available_balance >= min_required:
                logging.info(
                    f"✅ 资金充足: 可用${available_balance:.2f} ≥ 需要${min_required:.2f}"
                )
                return True
            else:
                logging.warning(
                    f"❌ 资金不足: 可用${available_balance:.2f} < 需要${min_required:.2f}，跳过建仓"
                )
                return False

        except Exception as e:
            logging.error(f"❌ 检查资金失败: {e}")
            # 资金检查失败时保守处理，不建仓
            return False

    def server_get_btc_daily_open_close_for_utc_day(
        self, utc_day: date
    ) -> Optional[Tuple[float, float]]:
        """拉取 BTCUSDT 指定 UTC 日历日对应的 1d K 线 open、close（该日 00:00 UTC 开盘）。"""
        try:
            day_start = datetime(
                utc_day.year, utc_day.month, utc_day.day, 0, 0, 0, tzinfo=timezone.utc
            )
            start_ms = int(day_start.timestamp() * 1000)
            klines = self.client.futures_klines(
                symbol="BTCUSDT",
                interval="1d",
                startTime=start_ms,
                limit=1,
            )
            if not klines:
                return None
            k = klines[0]
            return float(k[1]), float(k[4])
        except Exception as e:
            logging.error(f"❌ 获取 BTC 日K {utc_day} 失败: {e}")
            return None

    def check_btc_yesterday_yang_blocks_entry_live(self) -> Tuple[bool, str]:
        """昨日 BTC 日K 收涨(close>open)时，当日不建新仓（UTC 日历日；与 hm1l 一致）。失败不拦截。"""
        if not self.enable_btc_yesterday_yang_no_new_entry:
            return False, ""
        try:
            now = datetime.now(timezone.utc)
            yday = now.date() - timedelta(days=1)
            oc = self.server_get_btc_daily_open_close_for_utc_day(yday)
            if oc is None:
                logging.warning(f"⚠️ BTC 日K 缺失 {yday}，不拦截建仓")
                return False, ""
            o, c = oc
            pct = (c - o) / o * 100 if o and o > 0 else 0.0
            if c > o:
                return True, (
                    f"BTC昨日({yday})收涨 {pct:+.2f}% (open={o:.2f} close={c:.2f})，"
                    f"风控：当日不建新仓"
                )
            return False, ""
        except Exception as e:
            logging.error(f"❌ BTC昨日收涨建仓检查异常: {e}")
            return False, ""

    def server_maybe_btc_yesterday_yang_flatten_at_new_utc_day(self) -> None:
        """每个 UTC 日最多评估一次：若昨日 BTC 为阳线，则市价平掉程序管理的所有空仓。"""
        if not self.enable_btc_yesterday_yang_flatten_at_open:
            return
        now = datetime.now(timezone.utc)
        today = now.date()
        if self._last_btc_yang_flatten_eval_utc_date == today:
            return

        yday = today - timedelta(days=1)
        oc = self.server_get_btc_daily_open_close_for_utc_day(yday)
        if oc is None:
            logging.warning(f"⚠️ BTC一刀切跳过：无法获取昨日日K {yday}，30s 后重试")
            return

        self._last_btc_yang_flatten_eval_utc_date = today
        o, c = oc
        pct = (c - o) / o * 100 if o and o > 0 else 0.0
        if c <= o:
            logging.info(
                f"ℹ️ 昨日BTC未收涨({yday} o={o:.2f} c={c:.2f} 涨跌{pct:+.2f}%)，跳过一刀切"
            )
            return

        with self._positions_sync_lock:
            to_close = [
                p for p in self.positions[:] if p.get("direction", "short") != "long"
            ]
        if not to_close:
            logging.info(
                f"📉 昨日BTC收涨 {pct:+.2f}%({yday} o={o:.2f} c={c:.2f})，本地无待平空仓，跳过一刀切"
            )
            return

        logging.info(
            f"📉 昨日BTC收涨 {pct:+.2f}%({yday} o={o:.2f} c={c:.2f}) → UTC {today} 市价平 {len(to_close)} 个空仓 "
            f"(reason=btc_yesterday_yang_flatten_open)"
        )
        for pos in to_close:
            sym = pos.get("symbol", "")
            try:
                self.server_close_position(pos, "btc_yesterday_yang_flatten_open")
            except Exception as e:
                logging.error(f"❌ 一刀切平仓失败 {sym}: {e}")

    def server_set_leverage(self, symbol: str):
        """设置杠杆倍数 - 服务器版本"""
        try:
            self.client.futures_change_leverage(
                symbol=symbol, leverage=int(self.leverage)
            )
            logging.info(f"✅ {symbol} 设置杠杆 {int(self.leverage)}x")
        except Exception as e:
            logging.error(f"❌ {symbol} 设置杠杆失败: {e}")

    def server_open_position(self, signal: Dict) -> bool:
        """开仓 - 服务器版本"""
        symbol = signal["symbol"]

        # 🔒 获取或创建该symbol的锁
        with self.position_lock_master:
            if symbol not in self.position_locks:
                self.position_locks[symbol] = threading.Lock()
            symbol_lock = self.position_locks[symbol]

        # 🔒 使用锁防止并发建仓
        acquired = symbol_lock.acquire(blocking=False)
        if not acquired:
            logging.warning(f"🔒 {symbol} 正在建仓中，跳过重复请求")
            return False

        entry_lock_acquired = False
        self._entry_global_lock.acquire()
        entry_lock_acquired = True
        try:
            signal_price = signal["price"]  # 信号价格（用于记录）

            # 获取当前市价作为建仓价格
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            price = float(ticker["price"])
            logging.info(
                f"💰 {symbol} 信号价格: {signal_price:.6f}, 当前市价: {price:.6f}"
            )

            # 🔧 v4 修复 #9：滑点检查 — 信号与市价偏差过大则拒绝建仓
            if signal_price > 0:
                slippage = abs(price - signal_price) / signal_price
                max_slippage = 0.03  # 最大 3% 滑点
                if slippage > max_slippage:
                    logging.warning(
                        f"🚫 {symbol} 滑点过大: {slippage * 100:.2f}% > {max_slippage * 100:.1f}%，"
                        f"信号价={signal_price:.6f}, 市价={price:.6f}"
                    )
                    return False

            # 检查持仓限制（同时获取交易所当前持仓 symbol 集合）
            limits_ok, exchange_symbols = self.server_check_position_limits()
            if not limits_ok:
                return False

            # BTC 昨日日K 阳线 → 当日不建新仓（与 hm1l 回测一致）
            if self.enable_btc_yesterday_yang_no_new_entry:
                skip, msg = self.check_btc_yesterday_yang_blocks_entry_live()
                if skip:
                    logging.warning(f"🚫 {symbol} 建仓被拒: {msg}")
                    return False

            # 🔧 v5 修复：以交易所为准检查是否已持仓（防止内存与交易所不一致时重复开仓）
            if symbol in exchange_symbols:
                logging.warning(
                    f"⚠️ {symbol} 交易所已存在持仓，跳过建仓"
                )
                return False

            # 内存侧也检查（双重保险，覆盖交易所 API 降级场景）
            with self._positions_sync_lock:
                existing_positions = [p for p in self.positions if p["symbol"] == symbol]
            if existing_positions:
                logging.warning(
                    f"⚠️ {symbol} 内存中已存在 {len(existing_positions)} 个持仓，跳过建仓"
                )
                for idx, pos in enumerate(existing_positions, 1):
                    pos_id = pos.get("position_id", "未知")[:8]
                    entry_time = pos.get("entry_time", "未知")
                    logging.warning(f"   持仓{idx}: ID={pos_id}, 建仓时间={entry_time}")
                return False

            # 计算建仓金额
            position_value = self.account_balance * self.position_size_ratio

            # 🔧 新增：资金充足性检查（要求至少15%余量）
            if not self.check_sufficient_funds(position_value):
                return False

            quantity = (position_value * self.leverage) / price

            logging.info(
                f"💰 {symbol} 初始计算: 账户{self.account_balance:.2f} × {self.position_size_ratio} × {self.leverage} / {price} = {quantity:.2f}"
            )

            # 获取交易对的精度要求（优先读本地缓存，避免每次建仓都发重量级 API 请求）
            symbol_info = self._get_symbol_info(symbol)

            if not symbol_info:
                logging.error(f"❌ 无法获取 {symbol} 的交易规则")
                return False

            # 获取LOT_SIZE过滤器
            lot_size_filter = next(
                (f for f in symbol_info["filters"] if f["filterType"] == "LOT_SIZE"),
                None,
            )
            if lot_size_filter:
                step_size = float(lot_size_filter["stepSize"])
                min_qty = float(lot_size_filter["minQty"])

                logging.info(
                    f"📏 {symbol} LOT_SIZE规则: stepSize={step_size}, minQty={min_qty}"
                )

                # 根据stepSize精度取整
                if step_size >= 1:
                    quantity = round(quantity / step_size) * step_size
                    quantity = int(quantity)
                    logging.info(f"🔢 {symbol} 按stepSize={step_size}取整: {quantity}")
                else:
                    precision = len(str(step_size).rstrip("0").split(".")[-1])
                    quantity = round(quantity / step_size) * step_size
                    quantity = round(quantity, precision)
                    logging.info(f"🔢 {symbol} 按精度{precision}取整: {quantity}")

                # 检查最小数量
                if quantity < min_qty:
                    logging.warning(
                        f"⚠️ {symbol} 计算数量 {quantity} 小于最小数量 {min_qty}"
                    )
                    return False
            else:
                # 如果没有LOT_SIZE过滤器，默认保留3位小数
                quantity = round(quantity, 3)

            logging.info(
                f"📊 {symbol} 最终建仓数量: {quantity}, 价格: {price}, 名义价值: ${quantity * price:.2f}"
            )

            # 设置杠杆
            self.server_set_leverage(symbol)

            # 设置逐仓模式
            try:
                self.client.futures_change_margin_type(
                    symbol=symbol, marginType="ISOLATED"
                )
            except BinanceAPIException as e:
                if "already" not in str(e).lower():
                    logging.warning(f"{symbol} 设置逐仓模式失败: {e}")
            except (OSError, TimeoutError) as e:
                logging.warning(f"{symbol} 设置逐仓模式网络错误: {e}")

            # 设置为单向持仓模式（如果是双向模式会失败，忽略）
            try:
                self.client.futures_change_position_mode(dualSidePosition=False)
            except BinanceAPIException as e:
                if (
                    "position mode" not in str(e).lower()
                    and "dual" not in str(e).lower()
                ):
                    logging.warning(f"{symbol} 设置单向持仓模式失败: {e}")
            except (OSError, TimeoutError) as e:
                logging.warning(f"{symbol} 设置单向持仓模式网络错误: {e}")

            # 下单（做空）
            order = self.client.futures_create_order(
                symbol=symbol, side="SELL", type="MARKET", quantity=quantity
            )

            # 🔧 v4 修复 #10：使用订单实际成交价（avgPrice）而非 ticker 价格
            try:
                actual_entry_price = float(order.get("avgPrice", 0))
                if actual_entry_price <= 0:
                    actual_entry_price = price  # fallback
                    logging.warning(
                        f"⚠️ {symbol} 订单无 avgPrice，使用 ticker 价格 {price:.6f}"
                    )
                else:
                    logging.info(
                        f"💰 {symbol} 实际成交价: {actual_entry_price:.6f} (ticker: {price:.6f})"
                    )
            except (TypeError, ValueError):
                actual_entry_price = price

            # 记录持仓
            current_time = datetime.now(timezone.utc)
            position_id = str(uuid.uuid4())  # ✨ 生成唯一持仓ID

            position = {
                "position_id": position_id,  # 唯一持仓ID
                "symbol": symbol,
                "direction": "short",  # 本策略只开空
                "signal_price": signal_price,  # 记录信号价格
                "signal_datetime": signal.get(
                    "signal_time"
                ),  # 信号发生时间（用于连续确认判断）
                "entry_price": actual_entry_price,  # 实际成交价（非 ticker）
                "entry_time": current_time.isoformat(),  # 实际建仓时间
                "quantity": quantity,
                "position_value": position_value,
                "surge_ratio": signal["surge_ratio"],
                "leverage": self.leverage,
                "tp_pct": self.strong_coin_tp_pct,  # 初始止盈33%
                "status": "normal",
                "order_id": order["orderId"],
                "tp_order_id": None,  # 止盈订单ID
                "sl_order_id": None,  # 止损订单ID
                "tp_price": None,  # 止盈价格
                "sl_price": None,  # 止损价格
            }

            # 🔧 v5 修复 #8：持仓列表操作加锁
            with self._positions_sync_lock:
                self.positions.append(position)
                self.daily_entries += 1

            # 记录建仓小时（用于每小时限制）
            current_hour = datetime.now(timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )
            self.last_entry_hour = current_hour

            # 🔧 v5 修复 #16：交易所仓位优先，本地持久化失败不影响仓位
            # 保存持仓记录到文件（重试 2 次）
            for _save_attempt in range(3):
                try:
                    self.server_save_positions_record()
                    break
                except Exception as save_err:
                    logging.error(
                        f"❌ {symbol} 保存持仓记录失败(尝试{_save_attempt + 1}/3): {save_err}"
                    )
                    if _save_attempt < 2:
                        time.sleep(0.5)
            else:
                # 3 次都失败：仓位已在交易所 + 已在内存列表，文件落盘丢失
                # 监控循环会从交易所重新对账，不影响止盈止损
                logging.error(
                    f"🚨 {symbol} 持仓记录持久化 3 次均失败！"
                    f"内存持仓已添加，监控循环将正常管理止盈止损。"
                    f"下次重启时将从交易所重新加载。"
                )
                send_email_alert(
                    "持仓记录落盘失败",
                    f"{symbol} 已在交易所成功开仓，内存中已记录，"
                    f"但 positions_record.json 写入 3 次均失败。\n"
                    f"止盈止损不受影响（下方继续创建）。\n"
                    f"风险：进程重启前如文件仍未更新，将从交易所重新加载持仓。",
                )

            logging.info(
                f"🚀 开仓成功: {symbol} 价格:{price:.6f} 数量:{quantity:.3f} 杠杆:{self.leverage}x"
            )

            # 🔧 强制刷新日志（确保开仓日志立即写入）
            flush_logging_handlers()
            logging.info(
                f"📊 建仓计数: 今日第{self.daily_entries}个 (日限额{self.max_daily_entries}, "
                f"小时限制={'开' if self.enable_hourly_entry_limit else '关'})"
            )
            logging.info("💾 已保存建仓记录到文件")

            # 建仓完成后立即创建止盈止损订单
            try:
                tp_sl_success = self.server_create_tp_sl_orders(position, symbol_info)
                if not tp_sl_success:
                    logging.warning(
                        f"⚠️ {symbol} 建仓成功但止盈止损创建失败，将在监控循环中重试"
                    )
            except Exception as tp_sl_error:
                logging.error(f"❌ {symbol} 创建止盈止损订单异常: {tp_sl_error}")
                import traceback

                logging.error(f"📄 错误详情: {traceback.format_exc()}")
                # 建仓成功但止盈止损创建失败，继续执行

            # 建仓完成摘要日志
            entry_time_str = current_time.isoformat()
            tp_price = position.get("tp_price")
            sl_price = position.get("sl_price")
            tp_str = f"${tp_price:.6f}" if tp_price else "未设置(将在监控循环创建)"
            sl_str = f"${sl_price:.6f}" if sl_price else "未设置(将在监控循环创建)"
            logging.info(f"""
╔════════════════════════════════════════════════════════════════════════════╗
║ 🎉 {symbol} 建仓完成摘要
╠════════════════════════════════════════════════════════════════════════════╣
║ 📅 建仓时间: {entry_time_str}
║ 💰 建仓价格: ${price:.6f}
║ 📊 持仓数量: {quantity}
║ 💵 投入金额: ${position_value:.2f} USDT
║ ⚡ 杠杆倍数: {self.leverage}x
║ 📈 止盈价格: {tp_str} ({self.strong_coin_tp_pct}%)
║ 📉 止损价格: {sl_str} ({self.stop_loss_pct}%)
║ 🔢 Position ID: {position.get("position_id", "N/A")[:8]}
╚════════════════════════════════════════════════════════════════════════════╝
""")

            notify_positions_changed()
            return True

        except BinanceAPIException as e:
            logging.error(f"❌ {symbol} 开仓失败(API): {e}")
            return False
        except (OSError, TimeoutError) as e:
            logging.error(f"❌ {symbol} 开仓失败(网络): {e}")
            return False
        except Exception as e:
            logging.error(f"❌ {symbol} 开仓失败: {e}")
            return False
        finally:
            if entry_lock_acquired:
                self._entry_global_lock.release()
            symbol_lock.release()

    def server_get_5min_klines_from_binance(
        self, symbol: str, start_time: datetime, end_time: datetime
    ) -> List[float]:
        """从币安API获取5分钟K线收盘价 - 服务器版本"""
        try:
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)

            logging.debug(
                f"📊 {symbol} 获取K线: {start_time} ~ {end_time} ({start_ms} ~ {end_ms})"
            )

            klines = self.client.futures_klines(
                symbol=symbol,
                interval="5m",
                startTime=start_ms,
                endTime=end_ms,
                limit=500,
            )

            # 提取收盘价
            closes = [float(k[4]) for k in klines]

            logging.debug(
                f"📊 {symbol} 获取到 {len(closes)} 根K线, 最后5个收盘价: {closes[-5:] if closes else '无数据'}"
            )

            if len(closes) < 2:
                logging.warning(f"⚠️ {symbol} K线数据不足: 只有 {len(closes)} 根")

            return closes

        except Exception as e:
            logging.error(f"❌ 获取5分钟K线失败 {symbol}: {e}")
            return []

    def server_get_exchange_tp_order(self, symbol: str) -> Optional[Dict]:
        """获取交易所当前的止盈订单 - 服务器版本"""
        try:
            algo_orders = self.client.futures_get_open_algo_orders(symbol=symbol)
            for order in algo_orders:
                if order.get("orderType") in FUTURES_ALGO_TP_TYPES:
                    return order
            return None
        except Exception as e:
            logging.error(f"❌ 获取 {symbol} 止盈订单失败: {e}")
            return None

    def server_play_alert_sound(self):
        """播放报警声音 - 服务器版本"""
        try:
            # macOS系统声音
            os.system("afplay /System/Library/Sounds/Basso.aiff")
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

    def check_order_history(self, symbol: str, order_id: str = None) -> dict:
        """
        检查订单历史，判断订单状态
        用于排查止损单是否被触发/取消/失败

        Args:
            symbol: 交易对
            order_id: 订单ID（可选，如果提供则查找特定订单）

        Returns:
            dict: 订单历史信息
        """
        try:
            # 查询历史订单（最近100条）
            orders = self.client.futures_get_all_orders(symbol=symbol, limit=100)

            result = {
                "symbol": symbol,
                "order_id": order_id,
                "found": False,
                "orders": [],
            }

            # 如果指定了order_id，查找特定订单
            if order_id:
                for order in orders:
                    if (
                        str(order.get("orderId")) == order_id
                        or str(order.get("algoId")) == order_id
                    ):
                        status = order["status"]
                        order_type = order.get("type", "UNKNOWN")
                        update_time = datetime.fromtimestamp(
                            order["updateTime"] / 1000, tz=timezone.utc
                        )

                        result["found"] = True
                        result["order"] = order

                        logging.info(f"""
╔════════════════════════════════════════════════════════════════════════════╗
║ 📋 {symbol} 订单历史查询结果
╠════════════════════════════════════════════════════════════════════════════╣
║ 订单ID: {order_id}
║ 订单类型: {order_type}
║ 订单状态: {status}
║ 更新时间: {update_time}
║ {f"成交价格: ${order['avgPrice']}" if status == "FILLED" and order.get("avgPrice") else ""}
║ {f"触发价格: ${order.get('stopPrice', 'N/A')}" if "stopPrice" in order else ""}
║ 
║ 状态说明:
║   - NEW: 未触发（还在等待）
║   - FILLED: 已成交（订单成功执行）
║   - CANCELED: 已取消（触发后未成交 或 被手动/程序取消）
║   - REJECTED: 被拒绝（保证金不足 或 风控拦截）
║   - EXPIRED: 已过期
╚════════════════════════════════════════════════════════════════════════════╝
""")

                        # 根据状态给出分析
                        if status == "CANCELED":
                            logging.error(
                                f"❌ {symbol} 订单被取消！可能原因：触发后成交失败 或 被手动/程序取消"
                            )
                        elif status == "REJECTED":
                            logging.error(
                                f"❌ {symbol} 订单被拒绝！可能原因：保证金不足 或 风控拦截"
                            )
                        elif status == "EXPIRED":
                            logging.error(f"❌ {symbol} 订单已过期！")
                        elif status == "FILLED":
                            logging.info(f"✅ {symbol} 订单已成功执行")

                        break

                if not result["found"]:
                    logging.warning(
                        f"⚠️ {symbol} 订单ID {order_id} 未在历史记录中找到（可能已被删除）"
                    )

            else:
                # 未指定order_id，返回所有算法订单
                algo_orders = [
                    o for o in orders if o.get("type") in ["STOP_MARKET", "TAKE_PROFIT"]
                ]
                result["orders"] = algo_orders

                if algo_orders:
                    logging.info(f"📋 {symbol} 找到 {len(algo_orders)} 个算法订单历史")
                    for order in algo_orders[:5]:  # 只显示最近5个
                        logging.info(
                            f"  - {order['type']} | {order['status']} | {order.get('stopPrice', 'N/A')}"
                        )

            return result

        except Exception as e:
            logging.error(f"❌ 查询 {symbol} 订单历史失败: {e}")
            return {"symbol": symbol, "error": str(e)}

    def server_update_exchange_tp_order(
        self, position: Dict, new_tp_pct: float
    ) -> bool:
        """更新交易所的止盈止损订单（方案B：先取消所有旧订单再创建）- 服务器版本"""
        try:
            symbol = position["symbol"]
            entry_price = position["entry_price"]
            old_tp_pct = position.get("tp_pct", self.strong_coin_tp_pct)

            # 🔧 动态获取价格精度
            try:
                exchange_info = self.client.futures_exchange_info()
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
                        tick_size = float(price_filter["tickSize"])
                        if tick_size >= 1:
                            pass
                        else:
                            pass
                    else:
                        tick_size = 0.000001
                else:
                    tick_size = 0.000001
            except (ValueError, TypeError, KeyError) as e:
                logging.debug(f"获取tick_size失败: {e}")
                tick_size = 0.000001

            # 计算新的止盈价格（做空：价格下跌触发止盈）
            tp_price_raw = entry_price * (1 - new_tp_pct / 100)
            # 🔧 使用Decimal直接量化原始值，避免浮点误差
            from decimal import Decimal, ROUND_HALF_UP

            tick_size_decimal = Decimal(str(tick_size))
            tp_price_decimal = Decimal(str(tp_price_raw))
            new_tp_price = float(
                tp_price_decimal.quantize(tick_size_decimal, rounding=ROUND_HALF_UP)
            )

            logging.info(
                f"🔄 {symbol} 准备更新止盈订单: {old_tp_pct}% → {new_tp_pct}% (价格: {new_tp_price})"
            )

            # 🔧 修复4：添加重复更新检查
            if hasattr(position, "_tp_updating") and position.get("_tp_updating"):
                logging.warning(f"⚠️ {symbol} 止盈订单正在更新中，跳过本次操作")
                return False
            position["_tp_updating"] = True  # 标记正在更新
            is_long = position.get("direction") == "long"
            close_side = position_close_side(is_long)

            try:
                # 步骤1：查询所有算法订单（止盈+止损）
                try:
                    algo_orders = self.client.futures_get_open_algo_orders(
                        symbol=symbol
                    )
                    tp_orders = [
                        o
                        for o in algo_orders
                        if o.get("orderType") in FUTURES_ALGO_TP_TYPES
                        and o.get("side") == close_side
                    ]
                    sl_orders = [
                        o
                        for o in algo_orders
                        if o.get("orderType") in FUTURES_ALGO_SL_TYPES
                        and o.get("side") == close_side
                    ]

                    logging.info(
                        f"📋 {symbol} 找到 {len(tp_orders)} 个止盈订单, {len(sl_orders)} 个止损订单"
                    )

                    # 步骤2：取消所有旧订单（止盈+止损）
                    cancel_success = 0
                    cancel_fail = 0
                    all_orders_to_cancel = tp_orders + sl_orders

                    if all_orders_to_cancel:
                        logging.info(
                            f"🔄 {symbol} 准备取消 {len(all_orders_to_cancel)} 个旧订单（止盈+止损）"
                        )

                        for old_order in all_orders_to_cancel:
                            try:
                                self.client.futures_cancel_algo_order(
                                    symbol=symbol, algoId=old_order["algoId"]
                                )
                                cancel_success += 1
                                order_type = (
                                    "止盈"
                                    if old_order.get("orderType")
                                    in FUTURES_ALGO_TP_TYPES
                                    else "止损"
                                )
                                logging.info(
                                    f"✅ {symbol} 已取消{order_type}订单 {cancel_success}/{len(all_orders_to_cancel)} (algoId: {old_order['algoId']})"
                                )
                            except Exception as cancel_error:
                                cancel_fail += 1
                                logging.error(
                                    f"❌ {symbol} 取消订单失败 (algoId: {old_order['algoId']}): {cancel_error}"
                                )

                        if cancel_fail > 0:
                            logging.warning(
                                f"⚠️ {symbol} 有 {cancel_fail} 个订单取消失败"
                            )
                            self.server_play_alert_sound()

                        # 🔧 修复5：等待订单取消生效
                        if cancel_success > 0:
                            time.sleep(0.5)  # 等待0.5秒确保取消生效
                            logging.info(f"⏰ {symbol} 等待订单取消生效...")
                except Exception as query_error:
                    logging.error(f"❌ {symbol} 查询旧订单失败: {query_error}")
                    # 查询失败，跳过取消步骤，直接创建新订单
                    pass

                # 🔧 修复6：创建新订单前再次检查是否还有残留订单
                try:
                    algo_orders_check = self.client.futures_get_open_algo_orders(
                        symbol=symbol
                    )
                    tp_orders_check = [
                        o
                        for o in algo_orders_check
                        if o.get("orderType") in FUTURES_ALGO_TP_TYPES
                        and o.get("side") == close_side
                    ]
                    sl_orders_check = [
                        o
                        for o in algo_orders_check
                        if o.get("orderType") in FUTURES_ALGO_SL_TYPES
                        and o.get("side") == close_side
                    ]

                    if tp_orders_check or sl_orders_check:
                        logging.warning(
                            f"⚠️ {symbol} 取消后仍有 {len(tp_orders_check)} 个止盈 + {len(sl_orders_check)} 个止损订单残留，强制再次取消"
                        )
                        for order in tp_orders_check + sl_orders_check:
                            try:
                                self.client.futures_cancel_algo_order(
                                    symbol=symbol, algoId=order["algoId"]
                                )
                                logging.info(
                                    f"✅ {symbol} 强制取消残留订单: {order['algoId']}"
                                )
                            except BinanceAPIException as e:
                                logging.warning(f"{symbol} 取消残留订单失败: {e}")
                            except Exception as e:
                                logging.debug(f"{symbol} 取消残留订单异常: {e}")
                        time.sleep(0.3)
                except BinanceAPIException as e:
                    logging.warning(f"{symbol} 同步TP/SL时API异常: {e}")
                except Exception as e:
                    logging.debug(f"{symbol} 同步TP/SL异常: {e}")

                # 步骤3：创建新订单（止盈+止损）
                tp_order_id = None
                sl_order_id = None

                # 创建新的止盈订单（算法单 TAKE_PROFIT_MARKET）
                try:
                    tp_trig = str(
                        Decimal(str(new_tp_price)).quantize(
                            tick_size_decimal, rounding=ROUND_HALF_UP
                        )
                    )
                    tp_order = self.client.futures_create_algo_order(
                        symbol=symbol,
                        side=close_side,
                        type="TAKE_PROFIT_MARKET",
                        triggerPrice=tp_trig,
                        algoType="CONDITIONAL",
                        closePosition=True,
                        workingType="CONTRACT_PRICE",
                        priceProtect="true",
                    )
                    tp_order_id = str(
                        tp_order.get("algoId") or tp_order.get("orderId") or ""
                    )
                    position["tp_order_id"] = tp_order_id
                    logging.info(
                        f"✅ {symbol} 新止盈算法单已创建: {new_tp_price:.6f} (algoId: {tp_order_id})"
                    )
                except Exception as tp_create_error:
                    logging.error(f"❌ {symbol} 创建新止盈订单失败: {tp_create_error}")

                # 创建新的止损订单（使用固定的止损比例）
                sl_price = entry_price * (1 + abs(self.stop_loss_pct) / 100)  # 止损价格
                # 🔧 使用Decimal直接量化原始值，避免浮点误差
                sl_price_decimal = Decimal(str(sl_price))
                sl_price_adjusted = float(
                    sl_price_decimal.quantize(tick_size_decimal, rounding=ROUND_HALF_UP)
                )

                try:
                    sl_trig = str(
                        Decimal(str(sl_price_adjusted)).quantize(
                            tick_size_decimal, rounding=ROUND_HALF_UP
                        )
                    )
                    sl_order = self.client.futures_create_algo_order(
                        symbol=symbol,
                        side=close_side,
                        type="STOP_MARKET",
                        triggerPrice=sl_trig,
                        algoType="CONDITIONAL",
                        closePosition=True,
                        workingType="CONTRACT_PRICE",
                        priceProtect="true",
                    )
                    sl_order_id = str(
                        sl_order.get("algoId") or sl_order.get("orderId") or ""
                    )
                    position["sl_order_id"] = sl_order_id
                    logging.info(
                        f"✅ {symbol} 新止损算法单已创建: {sl_price_adjusted:.6f} (algoId: {sl_order_id})"
                    )
                except Exception as sl_create_error:
                    logging.error(f"❌ {symbol} 创建新止损订单失败: {sl_create_error}")

                # 🔧 关键修复：原子性更新 - 只有在所有订单都创建成功后才更新position记录
                if tp_order_id and sl_order_id:
                    # 所有订单都成功，才更新position记录
                    old_tp_pct_before = position.get("tp_pct", self.strong_coin_tp_pct)
                    position["tp_pct"] = new_tp_pct
                    position["last_tp_update"] = datetime.now(timezone.utc).isoformat()
                    position["tp_order_id"] = tp_order_id  # 保存止盈订单ID
                    position["sl_order_id"] = sl_order_id  # 保存止损订单ID

                    # 记录止盈修改历史
                    if "tp_history" not in position:
                        position["tp_history"] = []
                    position["tp_history"].append(
                        {
                            "time": datetime.now(timezone.utc).isoformat(),
                            "from": old_tp_pct_before,
                            "to": new_tp_pct,
                            "reason": position.get("dynamic_tp_trigger", "manual"),
                        }
                    )

                    logging.info(
                        f"✅ {symbol} 订单更新完成: 止盈 {tp_order_id}, 止损 {sl_order_id}"
                    )

                    # 🆕 动态调整完成摘要日志
                    logging.info(f"""
╔════════════════════════════════════════════════════════════════════════════╗
║ 📊 {symbol} 止盈订单动态调整完成
╠════════════════════════════════════════════════════════════════════════════╣
║ 调整原因: {position.get("dynamic_tp_trigger", "未知")}
║ 止盈变化: {old_tp_pct_before:.1f}% → {new_tp_pct:.1f}%
║ 新止盈订单: 价格 ${new_tp_price:.6f} (ID: {tp_order_id})
║ 新止损订单: 价格 ${sl_price_adjusted:.6f} (ID: {sl_order_id})
║ ✅ 重要：止损订单同步更新，确保完整保护
╚════════════════════════════════════════════════════════════════════════════╝
""")

                    position["_tp_updating"] = False  # 🔧 清除更新标记
                    return True

                elif tp_order_id or sl_order_id:
                    # 部分成功：需要回滚已创建的订单
                    logging.warning(f"⚠️ {symbol} 订单创建部分失败，开始回滚...")

                    # 取消已创建的订单
                    rollback_success = True
                    if tp_order_id:
                        try:
                            if cancel_order_algo_or_regular(
                                self.client, symbol, tp_order_id
                            ):
                                logging.info(
                                    f"✅ {symbol} 已回滚止盈订单 {tp_order_id}"
                                )
                            else:
                                raise RuntimeError("cancel failed")
                        except Exception as rollback_error:
                            logging.error(
                                f"❌ {symbol} 回滚止盈订单失败: {rollback_error}"
                            )
                            rollback_success = False

                    if sl_order_id:
                        try:
                            if cancel_order_algo_or_regular(
                                self.client, symbol, sl_order_id
                            ):
                                logging.info(
                                    f"✅ {symbol} 已回滚止损订单 {sl_order_id}"
                                )
                            else:
                                raise RuntimeError("cancel failed")
                        except Exception as rollback_error:
                            logging.error(
                                f"❌ {symbol} 回滚止损订单失败: {rollback_error}"
                            )
                            rollback_success = False

                    if rollback_success:
                        logging.info(f"✅ {symbol} 订单回滚完成")
                    else:
                        logging.error(f"❌ {symbol} 订单回滚失败，可能存在残留订单")

                    logging.error(
                        f"❌ {symbol} 订单创建失败: 止盈 {'成功' if tp_order_id else '失败'}, 止损 {'成功' if sl_order_id else '失败'}"
                    )
                    position["_tp_updating"] = False
                    return False

                else:
                    # 全部失败
                    logging.error(f"❌ {symbol} 所有订单创建都失败了！")
                    position["_tp_updating"] = False
                    return False

            except Exception as create_error:
                logging.error(f"❌ {symbol} 创建新订单失败: {create_error}")
                # 播放报警声音
                self.server_play_alert_sound()
                position["_tp_updating"] = False  # 🔧 清除更新标记
                return False
            finally:
                # 🔧 修复8：确保无论如何都清除更新标记
                if "_tp_updating" in position:
                    position["_tp_updating"] = False

        except Exception as e:
            logging.error(f"❌ {symbol} 更新止盈订单失败: {e}")
            self.play_alert_sound()
            if "_tp_updating" in position:
                position["_tp_updating"] = False
            return False

    def server_calculate_dynamic_tp(self, position: Dict) -> tuple:
        """计算动态止盈阈值（完整实现2h和12h判断）- 服务器版本

        Returns:
            tuple: (adjusted_tp_pct, should_check_2h, should_check_12h, is_consecutive_confirmed)
            - adjusted_tp_pct: 计算出的止盈百分比
            - should_check_2h: 是否需要标记2h检查完成
            - should_check_12h: 是否需要标记12h检查完成
            - is_consecutive_confirmed: 是否确认为连续暴涨
        """
        try:
            symbol = position["symbol"]
            entry_price = position["entry_price"]
            entry_time = datetime.fromisoformat(position["entry_time"])
            current_time = datetime.now(timezone.utc)
            elapsed_hours = (current_time - entry_time).total_seconds() / 3600

            # 0-2小时：固定强势币止盈33%
            if elapsed_hours < 2.0:
                logging.debug(
                    f"{symbol} 持仓{elapsed_hours:.1f}h，使用强势币止盈{self.strong_coin_tp_pct}%"
                )
                return self.strong_coin_tp_pct, False, False, False

            # 2-12小时：2小时判断
            if 2.0 <= elapsed_hours < 12.0:
                if position.get("tp_2h_checked"):
                    cached_tp = position.get("tp_pct", self.strong_coin_tp_pct)
                    logging.debug(f"{symbol} 使用2h判断缓存结果: {cached_tp}%")
                    return cached_tp, False, False, False

                # 执行2小时判断
                logging.info(f"🔍 {symbol} 执行2小时动态止盈判断...")

                # 获取建仓后2小时的5分钟K线
                window_2h_end = entry_time + timedelta(hours=2)
                closes = self.server_get_5min_klines_from_binance(
                    symbol, entry_time, window_2h_end
                )

                if len(closes) >= 2:
                    # 做空策略：计算每根K线相对建仓价的跌幅
                    returns = [(close - entry_price) / entry_price for close in closes]

                    logging.debug(
                        f"🔍 {symbol} 跌幅分析: 建仓价={entry_price:.6f}, K线数量={len(closes)}"
                    )
                    logging.debug(
                        f"🔍 {symbol} 跌幅列表: {[f'{r * 100:.2f}%' for r in returns]}"
                    )

                    # 统计跌幅>5.5%的K线数量
                    threshold = self.dynamic_tp_2h_growth_threshold  # 5.5%
                    count_drop = sum(1 for r in returns if r < -threshold)
                    pct_drop = count_drop / len(closes)

                    logging.info(
                        f"📊 {symbol} 2h分析: {count_drop}/{len(closes)} 根K线跌幅>{threshold * 100:.1f}%, 占比{pct_drop * 100:.1f}%"
                    )

                    if pct_drop >= self.dynamic_tp_2h_ratio:
                        # 强势币：下跌K线≥60%
                        adjusted_tp = self.strong_coin_tp_pct
                        logging.info(
                            f"✅ {symbol} 2h判定为强势币: 下跌占比{pct_drop * 100:.1f}% ≥ {self.dynamic_tp_2h_ratio * 100:.1f}%, 止盈{adjusted_tp}%"
                        )
                    else:
                        # 中等币：下跌K线<60%
                        adjusted_tp = self.medium_coin_tp_pct
                        logging.warning(
                            f"⚠️ {symbol} 2h判定为中等币: 下跌占比{pct_drop * 100:.1f}% < {self.dynamic_tp_2h_ratio * 100:.1f}%, 止盈降至{adjusted_tp}%"
                        )

                    # 返回结果和标记更新指示
                    return adjusted_tp, True, False, False
                else:
                    # K线不足，保持强势币
                    logging.warning(
                        f"⚠️ {symbol} 2h K线不足({len(closes)}根)，保持强势币{self.strong_coin_tp_pct}%"
                    )
                    # 返回结果和标记更新指示
                    return self.strong_coin_tp_pct, True, False, False

            # 12小时后：12小时判断
            if elapsed_hours >= 12.0:
                if position.get("tp_12h_checked"):
                    cached_tp = position.get("tp_pct", self.weak_coin_tp_pct)
                    logging.debug(f"{symbol} 使用12h判断缓存结果: {cached_tp}%")
                    return cached_tp, False, False, False

                # 执行12小时判断
                logging.info(f"🔍 {symbol} 执行12小时动态止盈判断...")

                # 获取建仓后12小时的5分钟K线
                window_12h_end = entry_time + timedelta(hours=12)
                closes = self.server_get_5min_klines_from_binance(
                    symbol, entry_time, window_12h_end
                )

                if len(closes) >= 2:
                    # 做空策略：计算每根K线相对建仓价的跌幅
                    returns = [(close - entry_price) / entry_price for close in closes]

                    # 统计跌幅>7.5%的K线数量
                    count_drop = sum(
                        1 for r in returns if r < -self.dynamic_tp_12h_growth_threshold
                    )
                    pct_drop = count_drop / len(closes)

                    is_consecutive_confirmed = False

                    if pct_drop >= self.dynamic_tp_12h_ratio:
                        # 强势币：下跌K线≥60%（升级或保持）
                        adjusted_tp = self.strong_coin_tp_pct
                        logging.info(
                            f"⬆️ {symbol} 12h确认为强势币: 下跌占比{pct_drop * 100:.1f}% ≥ 60%, 止盈{adjusted_tp}%"
                        )
                    else:
                        # 下跌占比<60%：检查是否为连续暴涨
                        is_consecutive = self._server_check_consecutive_surge(position)

                        if is_consecutive:
                            is_consecutive_confirmed = True
                            # 🔥 连续暴涨保护：保持强势或中等币止盈，不降为弱势币
                            if position.get("dynamic_tp_strong"):
                                adjusted_tp = self.strong_coin_tp_pct  # 保持33%
                                logging.info(
                                    f"✅ {symbol} 12h判断：连续2小时暴涨，保持强势币止盈：\n"
                                    f"  • 下跌占比 {pct_drop * 100:.1f}% < 60%\n"
                                    f"  • 但为连续暴涨，保持强势币止盈={adjusted_tp}%"
                                )
                            else:
                                adjusted_tp = self.medium_coin_tp_pct  # 保持21%
                                logging.info(
                                    f"✅ {symbol} 12h判断：连续2小时暴涨，保持中等币止盈：\n"
                                    f"  • 下跌占比 {pct_drop * 100:.1f}% < 60%\n"
                                    f"  • 但为连续暴涨，保持中等币止盈={adjusted_tp}%"
                                )
                        else:
                            # 非连续暴涨：正常降为弱势币
                            adjusted_tp = self.weak_coin_tp_pct
                            logging.warning(
                                f"⚠️⚠️ {symbol} 12h判定为弱势币: 下跌占比{pct_drop * 100:.1f}% < 60%, 止盈降至{adjusted_tp}%"
                            )

                    return adjusted_tp, False, True, is_consecutive_confirmed
                else:
                    # K线不足，保持原判断
                    if position.get("dynamic_tp_strong"):
                        tp = self.strong_coin_tp_pct
                    else:
                        tp = self.medium_coin_tp_pct
                    logging.warning(
                        f"⚠️ {symbol} 12h K线不足({len(closes)}根)，保持{tp}%"
                    )
                    return tp, False, True, False

            return self.strong_coin_tp_pct, False, False, False

        except Exception as e:
            logging.error(f"❌ 计算动态止盈失败 {symbol}: {e}")
            return self.strong_coin_tp_pct, False, False, False

    def server_check_exit_conditions(self, position: Dict) -> Optional[str]:
        """检查平仓条件（完整实现）- 服务器版本"""
        try:
            symbol = position["symbol"]
            entry_price = position["entry_price"]
            entry_time = datetime.fromisoformat(position["entry_time"])
            current_time = datetime.now(timezone.utc)

            # 获取当前价格
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            current_price = float(ticker["price"])

            # 计算涨跌幅（做空策略：价格下跌=正收益）
            price_change_pct = (current_price - entry_price) / entry_price

            # 计算持仓时间
            elapsed_hours = (current_time - entry_time).total_seconds() / 3600

            # 1. 72小时强制平仓（最高优先级）
            if elapsed_hours >= self.max_hold_hours:
                logging.warning(
                    f"⏰ {symbol} 持仓{elapsed_hours:.1f}h 超过72h限制，强制平仓"
                )
                return "max_hold_time"

            # 🔧 新增：检查止损订单状态，避免重复下单
            sl_order_id = position.get("sl_order_id")
            if sl_order_id:
                try:
                    order_info = None
                    try:
                        order_info = self.client.futures_get_algo_order(
                            symbol=symbol, algoId=int(sl_order_id)
                        )
                    except BinanceAPIException as e:
                        logging.debug(f"{symbol} 查询算法订单失败，尝试普通订单: {e}")
                        try:
                            order_info = self.client.futures_get_order(
                                symbol=symbol, orderId=int(sl_order_id)
                            )
                        except BinanceAPIException as e2:
                            logging.warning(f"{symbol} 查询止损订单失败: {e2}")
                    except (OSError, TimeoutError) as e:
                        logging.warning(f"{symbol} 查询止损订单网络错误: {e}")
                    order_status = order_info.get("status") or order_info.get(
                        "algoStatus", ""
                    )
                    if order_status in (
                        "FILLED",
                        "PARTIALLY_FILLED",
                        "TRIGGERED",
                        "FINISHED",
                    ):
                        logging.info(f"✅ {symbol} 止损订单已执行，无需额外平仓")
                        return None  # 跳过主动检查
                    elif order_status == "CANCELED":
                        logging.warning(f"⚠️ {symbol} 止损订单被取消，需要重新检查价格")
                except Exception as e:
                    logging.debug(f"⚠️ 查询止损订单失败: {e}")

            # 2. 止损检查（做空：价格上涨触发止损）
            sl_threshold = self.stop_loss_pct / 100  # 18% -> 0.18
            if price_change_pct >= sl_threshold:
                actual_loss = price_change_pct * self.leverage * 100
                logging.warning(
                    f"🛑 {symbol} 触发止损: 价格涨幅{price_change_pct * 100:.2f}% ≥ {self.stop_loss_pct}%, 实际亏损{actual_loss:.1f}%"
                )
                return "stop_loss"

            # 3. 24小时涨幅止损（与 hm1l 对齐：默认关闭 enable_max_gain_24h_exit）
            if (
                self.enable_max_gain_24h_exit
                and 24.0 <= elapsed_hours < 25.0
                and not position.get("checked_24h")
            ):
                if price_change_pct > self.max_gain_24h_threshold:
                    logging.warning(
                        f"🚨 {symbol} 24h涨幅止损: 涨幅{price_change_pct * 100:.2f}% > {self.max_gain_24h_threshold * 100:.1f}%"
                    )
                    position["checked_24h"] = True
                    return "max_gain_24h"
                else:
                    position["checked_24h"] = True  # 标记已检查，避免重复

            # 🆕 4. 12小时及早平仓检查（精确在12小时整点）
            # 📌 修改逻辑与hm1l.py保持一致：从建仓时间开始获取144根K线，取第144根的收盘价判断
            # ⚠️ 只在12-13小时之间检查一次，判断的是"12小时整点时"的价格，不是之后的任意时刻
            if (
                self.enable_12h_early_stop
                and 12.0 <= elapsed_hours < 13.0
                and not position.get("checked_12h_early_stop")
            ):
                try:
                    # 从币安API获取建仓后的5分钟K线（只用startTime，不用endTime）
                    entry_time_ms = int(entry_time.timestamp() * 1000)

                    # 🔥 关键修改：只使用startTime和limit，不使用endTime
                    # 原因：同时指定startTime、endTime和limit会导致API返回最近的144根，而不是从startTime开始的144根
                    klines = self.client.futures_klines(
                        symbol=symbol, interval="5m", startTime=entry_time_ms, limit=144
                    )

                    if len(klines) >= 144:
                        # 取第144根K线的收盘价（12小时整点）
                        close_12h = float(klines[143][4])  # [4]是close价格
                        price_change_12h = (close_12h - entry_price) / entry_price

                        # 验证K线时间是否正确（第144根应该接近建仓后12小时）
                        kline_144_time = datetime.fromtimestamp(
                            klines[143][0] / 1000, tz=timezone.utc
                        )
                        expected_time = entry_time + timedelta(hours=12)
                        time_diff_minutes = abs(
                            (kline_144_time - expected_time).total_seconds() / 60
                        )

                        if (
                            time_diff_minutes > 30
                        ):  # 如果时间相差超过30分钟，说明数据不对
                            logging.warning(
                                f"⚠️ {symbol} 12h检查时间异常：第144根K线时间{kline_144_time}与预期{expected_time}相差{time_diff_minutes:.0f}分钟，跳过检查"
                            )
                        elif price_change_12h > self.early_stop_12h_threshold:
                            logging.warning(
                                f"🚨 {symbol} 12h及早平仓触发: 持仓{elapsed_hours:.1f}h\n"
                                f"  • 12h整点收盘价：{close_12h:.6f}\n"
                                f"  • 建仓价：{entry_price:.6f}\n"
                                f"  • 涨幅：{price_change_12h * 100:.2f}% > 阈值{self.early_stop_12h_threshold * 100:.2f}%"
                            )
                            position["checked_12h_early_stop"] = True
                            return "early_stop_loss_12h"
                        else:
                            logging.info(
                                f"✅ {symbol} 12h及早平仓检查通过: 涨幅{price_change_12h * 100:.2f}% ≤ {self.early_stop_12h_threshold * 100:.2f}%"
                            )
                    else:
                        logging.warning(
                            f"⚠️ {symbol} 12h K线不足({len(klines)}根)，跳过检查"
                        )

                    position["checked_12h_early_stop"] = True  # 标记已检查

                except Exception as e:
                    logging.error(f"❌ {symbol} 12h及早平仓检查失败: {e}")
                    position["checked_12h_early_stop"] = True  # 失败也标记，避免重复

            # 5. 止盈检查（做空：价格下跌触发止盈）
            # 🔧 修复：使用position记录的止盈比例，而不是动态计算（避免与交易所订单不一致）
            tp_pct = position.get("tp_pct", self.strong_coin_tp_pct)
            tp_threshold = -tp_pct / 100  # 33% -> -0.33
            if price_change_pct <= tp_threshold:
                actual_profit = abs(price_change_pct) * self.leverage * 100
                logging.info(
                    f"✨ {symbol} 触发止盈: 价格跌幅{abs(price_change_pct) * 100:.2f}% ≥ {tp_pct}%, "
                    f"实际收益{actual_profit:.1f}%"
                )
                return "take_profit"

            return None

        except Exception as e:
            logging.error(f"检查平仓条件失败 {symbol}: {e}")
            return None

    def server_create_tp_sl_orders(self, position: Dict, symbol_info: Dict):
        """创建独立的止盈和止损订单（替代不支持的OCO模式）"""
        try:
            symbol = position["symbol"]
            entry_price = position["entry_price"]
            tp_pct = position.get("tp_pct", self.strong_coin_tp_pct)

            logging.info(f"🎯 {symbol} 开始创建止盈止损订单...")

            # 获取价格精度
            price_filter = next(
                (
                    f
                    for f in symbol_info["filters"]
                    if f["filterType"] == "PRICE_FILTER"
                ),
                None,
            )
            if price_filter:
                tick_size = float(price_filter["tickSize"])
                # 计算精度位数（用于日志显示）
                tick_size_str = price_filter["tickSize"].rstrip("0")
                if "." in tick_size_str:
                    price_precision = len(tick_size_str.split(".")[-1])
                else:
                    price_precision = 0
            else:
                tick_size = 0.000001
                price_precision = 6

            logging.info(
                f"📏 {symbol} 价格精度: tick_size={tick_size}, precision={price_precision}"
            )

            # 计算止盈价格（做空：价格下跌触发止盈）
            from decimal import Decimal, ROUND_HALF_UP

            tp_price_raw = entry_price * (1 - tp_pct / 100)
            # 使用Decimal避免浮点误差 - 直接对原始值量化
            tp_price_decimal = Decimal(str(tp_price_raw))
            tick_size_decimal = Decimal(str(tick_size))
            tp_price = float(
                tp_price_decimal.quantize(tick_size_decimal, rounding=ROUND_HALF_UP)
            )

            # 计算止损价格（做空：价格上涨触发止损）
            sl_trigger_price_raw = entry_price * (1 + abs(self.stop_loss_pct) / 100)
            sl_trigger_price_decimal = Decimal(str(sl_trigger_price_raw))
            sl_trigger_price = float(
                sl_trigger_price_decimal.quantize(
                    tick_size_decimal, rounding=ROUND_HALF_UP
                )
            )

            # 止损执行价格（略高于触发价，确保成交）
            sl_price = sl_trigger_price * 1.0005  # 100.05%的价格
            sl_price = round(sl_price / tick_size) * tick_size
            sl_price = float(
                Decimal(str(sl_price)).quantize(
                    Decimal(str(tick_size)), rounding=ROUND_HALF_UP
                )
            )

            # 使用正确的精度显示价格
            tp_price_str = f"{tp_price:.{price_precision}f}"
            sl_price_str = f"{sl_trigger_price:.{price_precision}f}"
            logging.info(f"📋 {symbol} 订单参数: TP={tp_price_str}, SL={sl_price_str}")

            # 检查是否已有部分订单（仅非空 ID 视为已存在，与交易所对账后一致）
            def _norm_oid(v) -> Optional[str]:
                if v is None:
                    return None
                s = str(v).strip()
                return s or None

            tp_order_id = _norm_oid(position.get("tp_order_id"))
            sl_order_id = _norm_oid(position.get("sl_order_id"))

            close_side = position_close_side(position.get("direction") == "long")
            tp_trig = str(
                Decimal(str(tp_price)).quantize(
                    tick_size_decimal, rounding=ROUND_HALF_UP
                )
            )
            sl_trig = str(
                Decimal(str(sl_trigger_price)).quantize(
                    tick_size_decimal, rounding=ROUND_HALF_UP
                )
            )

            # 1. 创建算法单止盈 (TAKE_PROFIT_MARKET) - 只有在没有时才创建
            if not tp_order_id:
                try:
                    logging.info(
                        f"🚀 {symbol} 创建止盈算法单: TAKE_PROFIT_MARKET side={close_side} trigger={tp_trig}"
                    )
                    tp_order = self.client.futures_create_algo_order(
                        symbol=symbol,
                        side=close_side,
                        type="TAKE_PROFIT_MARKET",
                        triggerPrice=tp_trig,
                        algoType="CONDITIONAL",
                        closePosition=True,
                        workingType="CONTRACT_PRICE",
                        priceProtect="true",
                    )
                    tp_order_id = str(
                        tp_order.get("algoId") or tp_order.get("orderId") or ""
                    )
                    logging.info(
                        f"✅ {symbol} 止盈算法单创建成功: {tp_price:.6f} (algoId: {tp_order_id})"
                    )
                except Exception as tp_error:
                    logging.error(f"❌ {symbol} 创建止盈订单失败: {tp_error}")
                    import traceback

                    logging.error(f"📄 错误详情: {traceback.format_exc()}")
            else:
                logging.info(f"✅ {symbol} 已有止盈订单，跳过创建 (ID: {tp_order_id})")

            # 2. 创建算法单止损 (STOP_MARKET)
            if not sl_order_id:
                try:
                    logging.info(
                        f"🚀 {symbol} 创建止损算法单: STOP_MARKET side={close_side} trigger={sl_trig}"
                    )
                    sl_order = self.client.futures_create_algo_order(
                        symbol=symbol,
                        side=close_side,
                        type="STOP_MARKET",
                        triggerPrice=sl_trig,
                        algoType="CONDITIONAL",
                        closePosition=True,
                        workingType="CONTRACT_PRICE",
                        priceProtect="true",
                    )
                    sl_order_id = str(
                        sl_order.get("algoId") or sl_order.get("orderId") or ""
                    )
                    logging.info(
                        f"✅ {symbol} 止损算法单创建成功: {sl_trigger_price:.6f} (algoId: {sl_order_id})"
                    )
                except Exception as sl_error:
                    logging.error(f"❌ {symbol} 创建止损订单失败: {sl_error}")
                    import traceback

                    logging.error(f"📄 错误详情: {traceback.format_exc()}")
            else:
                logging.info(f"✅ {symbol} 已有止损订单，跳过创建 (ID: {sl_order_id})")

            # 3. 更新持仓记录 - 只要有有效 ID 就写回（空串视为 None）
            tp_order_id = _norm_oid(tp_order_id)
            sl_order_id = _norm_oid(sl_order_id)
            if tp_order_id or sl_order_id:
                position["tp_order_id"] = tp_order_id
                position["sl_order_id"] = sl_order_id
                position["tp_price"] = tp_price
                position["sl_price"] = sl_trigger_price
                self.server_save_positions_record()

                if tp_order_id and sl_order_id:
                    logging.info(f"🎉 {symbol} 止盈止损订单都创建完成!")
                    return True
                else:
                    missing = []
                    if not tp_order_id:
                        missing.append("止盈")
                    if not sl_order_id:
                        missing.append("止损")
                    logging.warning(
                        f"⚠️ {symbol} 订单创建不完整，缺少: {', '.join(missing)}，稍后监控循环会重试"
                    )
                    return False
            else:
                logging.error(f"❌ {symbol} 止盈和止损订单都创建失败")
                return False

        except Exception as e:
            logging.error(f"❌ {symbol} 创建止盈止损订单失败: {e}")
            return False

    def server_setup_tp_sl_orders(self, position: Dict):
        """重新设置止盈止损订单（用于平仓失败后的恢复或手动重置）"""
        try:
            symbol = position["symbol"]
            logging.info(f"🔄 {symbol} 重新设置止盈止损订单...")

            # 获取交易对信息
            exchange_info = self.client.futures_exchange_info()
            symbol_info = next(
                (s for s in exchange_info["symbols"] if s["symbol"] == symbol), None
            )
            if not symbol_info:
                raise Exception(f"无法获取{symbol}交易对信息")

            # 调用独立订单创建函数
            success = self.server_create_tp_sl_orders(position, symbol_info)

            if success:
                logging.info(f"✅ {symbol} 止盈止损订单重新设置成功")
            else:
                raise Exception("创建止盈止损订单失败")

        except Exception as e:
            logging.error(f"❌ 重新设置止盈止损订单失败 {position['symbol']}: {e}")
            raise

    def server_manage_tp_sl_orders(self, position: Dict):
        """管理独立的止盈止损订单：确保一个成交后取消另一个"""
        try:
            symbol = position["symbol"]

            # 获取该交易对的所有算法订单
            algo_orders = self.client.futures_get_open_algo_orders(symbol=symbol)
            if not algo_orders:
                return

            close_side = position_close_side(position.get("direction") == "long")
            # 筛选止盈和止损算法单
            tp_orders = [
                o
                for o in algo_orders
                if o.get("orderType") in FUTURES_ALGO_TP_TYPES
                and o.get("side") == close_side
            ]
            sl_orders = [
                o
                for o in algo_orders
                if o.get("orderType") in FUTURES_ALGO_SL_TYPES
                and o.get("side") == close_side
            ]

            # 检查是否有订单已成交
            tp_filled = any(o.get("status") == "FILLED" for o in tp_orders)
            sl_filled = any(o.get("status") == "FILLED" for o in sl_orders)

            # 如果止盈已成交，取消止损订单
            if tp_filled and sl_orders:
                logging.warning(f"🎯 {symbol} 止盈订单已成交，自动取消关联的止损订单")
                for sl_order in sl_orders:
                    try:
                        self.client.futures_cancel_algo_order(
                            symbol=symbol, algoId=sl_order["algoId"]
                        )
                        logging.info(
                            f"✅ {symbol} 已取消止损订单 (algoId: {sl_order['algoId']})"
                        )
                    except Exception as e:
                        logging.error(
                            f"❌ {symbol} 取消止损订单失败 (algoId: {sl_order['algoId']}): {e}"
                        )

            # 如果止损已成交，取消止盈订单
            elif sl_filled and tp_orders:
                logging.warning(f"🛑 {symbol} 止损订单已成交，自动取消关联的止盈订单")
                for tp_order in tp_orders:
                    try:
                        self.client.futures_cancel_algo_order(
                            symbol=symbol, algoId=tp_order["algoId"]
                        )
                        logging.info(
                            f"✅ {symbol} 已取消止盈订单 (algoId: {tp_order['algoId']})"
                        )
                    except Exception as e:
                        logging.error(
                            f"❌ {symbol} 取消止盈订单失败 (algoId: {tp_order['algoId']}): {e}"
                        )

        except Exception as e:
            logging.error(f"❌ {symbol} 止盈止损订单管理失败: {e}")

    def server_sync_tp_sl_ids_from_exchange(self, position: Dict) -> bool:
        """
        以交易所当前开放算法单为准，刷新 position 的 tp_order_id / sl_order_id。
        拉单失败返回 False（不改动本地，避免在网络错误时误清空 ID）。
        若与交易所不一致（含本地残留脏 ID）会修正并保存 positions_record。
        """
        symbol = position["symbol"]
        if not getattr(self, "api_configured", False) or self.client is None:
            return False
        try:
            raw = self.client.futures_get_open_algo_orders(symbol=symbol)
        except Exception as e:
            logging.error(f"❌ {symbol} 同步止盈止损：拉取开放算法单失败: {e}")
            return False

        orders = list(raw or [])
        close_side = position_close_side(position.get("direction") == "long")
        tp_o, sl_o = pick_tp_sl_algo_candidates(
            orders,
            close_side,
            position.get("tp_order_id"),
            position.get("sl_order_id"),
        )
        new_tp = algo_order_id_from_dict(tp_o)
        new_sl = algo_order_id_from_dict(sl_o)

        def _norm_local(v) -> Optional[str]:
            if v is None:
                return None
            s = str(v).strip()
            return s or None

        old_tp = _norm_local(position.get("tp_order_id"))
        old_sl = _norm_local(position.get("sl_order_id"))
        if old_tp == new_tp and old_sl == new_sl:
            return True

        position["tp_order_id"] = new_tp
        position["sl_order_id"] = new_sl
        self.server_save_positions_record()
        logging.info(
            f"📌 {symbol} 止盈/止损 ID 已与交易所对齐: TP={new_tp}, SL={new_sl}"
        )
        return True

    def server_check_and_create_tp_sl(self, position: Dict):
        """在监控循环中：先与交易所对账，再对交易所仍缺失的一侧补挂止盈/止损。"""
        try:
            symbol = position["symbol"]

            quantity = position.get("quantity", 0)
            if quantity <= 0:
                logging.debug(f"❌ {symbol} 仓位数量无效: {quantity}")
                return

            if not self.server_sync_tp_sl_ids_from_exchange(position):
                return

            if position.get("tp_order_id") and position.get("sl_order_id"):
                logging.debug(f"✅ {symbol} 交易所已存在止盈与止损单，跳过补挂")
                return

            logging.info(
                f"🔄 {symbol} 交易所缺少止盈或止损（对齐后 TP={position.get('tp_order_id')}, "
                f"SL={position.get('sl_order_id')}），准备补挂..."
            )

            exchange_info = self.client.futures_exchange_info()
            symbol_info = next(
                (s for s in exchange_info["symbols"] if s["symbol"] == symbol), None
            )

            if not symbol_info:
                logging.error(f"❌ {symbol} 无法获取交易规则")
                return

            success = self.server_create_tp_sl_orders(position, symbol_info)
            if success:
                logging.info(f"✅ {symbol} 监控循环中补挂止盈/止损成功")
            else:
                logging.warning(f"⚠️ {symbol} 补挂止盈/止损未完成，下次重试")

        except Exception as e:
            logging.error(f"❌ {symbol} 检查止盈止损订单失败: {e}")

    def server_check_and_recreate_missing_tp_sl(self):
        """服务器启动时检查并重新创建缺失的止盈止损订单"""
        if not getattr(self, "api_configured", False) or self.client is None:
            logging.info("🔕 未配置 API：跳过止盈止损订单检查")
            return
        logging.info("🔍 🚀 服务器启动：按交易所开放单对账止盈/止损，缺失则补挂...")
        with self._positions_sync_lock:
            positions_snapshot = self.positions[:]
        logging.info(f"   📊 当前持仓数量: {len(positions_snapshot)}")

        missing_count = 0
        recreated_count = 0

        for position in positions_snapshot:
            symbol = position["symbol"]

            try:
                if not self.server_sync_tp_sl_ids_from_exchange(position):
                    logging.warning(
                        f"⚠️ {symbol} 无法与交易所对账（API 失败），跳过本仓位补挂"
                    )
                    continue

                has_tp = bool(position.get("tp_order_id"))
                has_sl = bool(position.get("sl_order_id"))
                logging.info(
                    f"   🔍 {symbol} 交易所对齐后: 止盈={'有' if has_tp else '无'}, 止损={'有' if has_sl else '无'}"
                )

                if has_tp and has_sl:
                    continue

                missing_count += 1
                logging.info(f"⚠️ {symbol} 交易所仍缺止盈或止损，正在补挂...")

                exchange_info = self.client.futures_exchange_info()
                symbol_info = next(
                    (s for s in exchange_info["symbols"] if s["symbol"] == symbol), None
                )

                if symbol_info:
                    success = self.server_create_tp_sl_orders(position, symbol_info)
                    if success:
                        recreated_count += 1
                        logging.info(f"✅ {symbol} 止盈止损补挂成功")
                    else:
                        logging.warning(f"⚠️ {symbol} 补挂失败，稍后监控循环会重试")
                else:
                    logging.error(f"❌ {symbol} 无法获取交易规则，跳过创建")

            except Exception as e:
                logging.error(f"❌ {symbol} 启动补挂流程失败: {e}")

        logging.info(
            f"📊 止盈止损检查完成: 需补挂仓位 {missing_count} 个，本轮成功 {recreated_count} 个"
        )

    # 注意: 旧版的 _old_server_create_otoco_after_fill_deprecated 函数已移除
    # 现在使用 server_create_tp_sl_orders 直接创建独立止盈止损订单

    def server_close_position(self, position: Dict, reason: str):
        """平仓 - 服务器版本"""
        try:
            symbol = position["symbol"]

            # 记录变动前状态
            before_state = {
                "持仓数量": position["quantity"],
                "建仓价格": position["entry_price"],
                "当前价格": self.client.futures_symbol_ticker(symbol=symbol)["price"],
                "未实现盈亏": position.get("pnl", 0),
                "持仓时长": (
                    datetime.now(timezone.utc)
                    - datetime.fromisoformat(position["entry_time"])
                ).total_seconds()
                / 3600,
            }

            # 与交易所对账
            self.server_sync_tp_sl_ids_from_exchange(position)

            # 🔧 平仓前先取消所有未成交的止盈止损订单
            logging.info(f"🔄 {symbol} 平仓前取消所有未成交订单...")
            cancelled_orders = []  # 记录被取消的订单
            try:
                algo_orders = self.client.futures_get_open_algo_orders(symbol=symbol)
                if algo_orders:
                    logging.info(
                        f"📋 {symbol} 找到 {len(algo_orders)} 个未成交订单，准备取消"
                    )
                    for order in algo_orders:
                        order_type = order["orderType"]
                        order_id = order["algoId"]
                        trigger_price = order.get("triggerPrice", "N/A")

                        try:
                            self.client.futures_cancel_algo_order(
                                symbol=symbol, algoId=order_id
                            )
                            cancelled_orders.append(
                                {
                                    "type": order_type,
                                    "id": order_id,
                                    "price": trigger_price,
                                }
                            )
                            logging.info(
                                f"✅ {symbol} 已取消订单: {order_type} (ID: {order_id}, 价格: {trigger_price})"
                            )
                        except Exception as cancel_error:
                            logging.error(
                                f"❌ {symbol} 取消订单失败 (ID: {order_id}): {cancel_error}"
                            )
                else:
                    logging.info(f"✅ {symbol} 没有未成交订单")
            except Exception as cancel_all_error:
                logging.error(f"❌ {symbol} 查询/取消订单失败: {cancel_all_error}")

            # 🔧 修复2：从交易所获取实际持仓数量和方向（避免程序记录不准确）
            try:
                positions_info = self.client.futures_position_information(symbol=symbol)
                actual_position = next(
                    (p for p in positions_info if p["symbol"] == symbol), None
                )

                if actual_position:
                    actual_amt = float(actual_position["positionAmt"])
                    quantity = abs(actual_amt)  # 取绝对值作为平仓数量
                    is_long_position = actual_amt > 0  # 正数=做多，负数=做空

                    logging.info(
                        f"📊 {symbol} 从交易所获取实际持仓: 数量={actual_amt} (方向={'做多' if is_long_position else '做空'}, 记录数量: {position['quantity']})"
                    )
                else:
                    quantity = position["quantity"]
                    is_long_position = False  # 默认假设是做空（程序只开做空）
                    logging.warning(
                        f"⚠️ {symbol} 无法获取实际持仓，使用程序记录数量: {quantity} (假设做空)"
                    )
            except Exception as get_position_error:
                quantity = position["quantity"]
                is_long_position = False  # 默认假设是做空
                logging.warning(
                    f"⚠️ {symbol} 获取实际持仓失败: {get_position_error}，使用程序记录数量: {quantity} (假设做空)"
                )

            # 🔧 修复3：动态获取数量精度并调整（使用round而非int，避免丢失）
            try:
                exchange_info = self.client.futures_exchange_info()
                symbol_info = next(
                    (s for s in exchange_info["symbols"] if s["symbol"] == symbol), None
                )

                if symbol_info:
                    lot_size_filter = next(
                        (
                            f
                            for f in symbol_info["filters"]
                            if f["filterType"] == "LOT_SIZE"
                        ),
                        None,
                    )
                    if lot_size_filter:
                        step_size = float(lot_size_filter["stepSize"])
                        # 根据stepSize精度调整（使用round四舍五入，而非int向下截断）
                        if step_size >= 1:
                            quantity_adjusted = round(quantity / step_size) * step_size
                            quantity_adjusted = int(quantity_adjusted)
                            qty_precision = 0
                        else:
                            qty_precision = len(
                                str(step_size).rstrip("0").split(".")[-1]
                            )
                            # 四舍五入到stepSize的整数倍
                            quantity_adjusted = round(quantity / step_size) * step_size
                            quantity_adjusted = round(quantity_adjusted, qty_precision)

                        logging.info(
                            f"📏 {symbol} 数量精度调整: {quantity} → {quantity_adjusted} (stepSize={step_size})"
                        )
                        quantity = quantity_adjusted
                    else:
                        quantity = round(quantity, 3)
                else:
                    quantity = round(quantity, 3)
            except Exception as precision_error:
                logging.warning(
                    f"⚠️ {symbol} 获取精度失败: {precision_error}，使用默认精度"
                )
                quantity = round(quantity, 3)

            # 🔧 修复4：根据实际仓位方向决定平仓买卖方向
            if is_long_position:
                close_side = "SELL"  # 做多平仓 = 卖出
                logging.info(f"🔄 {symbol} 检测到做多仓位，将使用SELL订单平仓")
            else:
                close_side = "BUY"  # 做空平仓 = 买入
                logging.info(f"🔄 {symbol} 检测到做空仓位，将使用BUY订单平仓")

            # 🔧 先尝试带reduceOnly，如果失败则重试不带reduceOnly
            try:
                order = self.client.futures_create_order(
                    symbol=symbol,
                    side=close_side,
                    type="MARKET",
                    quantity=quantity,
                    reduceOnly=True,
                )
            except Exception as reduce_error:
                if "ReduceOnly Order is rejected" in str(reduce_error):
                    logging.warning(f"⚠️ {symbol} reduceOnly平仓被拒绝，尝试普通市价单")
                    try:
                        # 重试：不带reduceOnly
                        order = self.client.futures_create_order(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=quantity,
                        )
                    except Exception as margin_error:
                        if "Margin is insufficient" in str(margin_error):
                            logging.error(f"❌ {symbol} 保证金不足，尝试分批平仓")
                            # 尝试分批平仓：先平一半仓位
                            half_quantity = quantity / 2

                            # 🔧 修复：对分批数量也进行精度调整
                            try:
                                # 使用和之前相同的精度调整逻辑
                                if "step_size" in locals():
                                    half_quantity_adjusted = (
                                        round(half_quantity / step_size) * step_size
                                    )
                                    if step_size >= 1:
                                        half_quantity_adjusted = int(
                                            half_quantity_adjusted
                                        )
                                    else:
                                        qty_precision = len(
                                            str(step_size).rstrip("0").split(".")[-1]
                                        )
                                        half_quantity_adjusted = round(
                                            half_quantity_adjusted, qty_precision
                                        )
                                    half_quantity = half_quantity_adjusted
                                    logging.info(
                                        f"📏 {symbol} 分批数量精度调整: {half_quantity}"
                                    )
                            except (ValueError, TypeError, KeyError) as e:
                                logging.debug(f"{symbol} 精度调整异常: {e}")
                                half_quantity = round(half_quantity, 3)

                            try:
                                order = self.client.futures_create_order(
                                    symbol=symbol,
                                    side=close_side,
                                    type="MARKET",
                                    quantity=half_quantity,
                                )
                                logging.info(
                                    f"✅ {symbol} 成功平仓一半仓位 ({half_quantity})，等待再次尝试"
                                )

                                # 🔧 修复：重新获取实际剩余持仓数量，而不是假设还有一半
                                time.sleep(0.5)  # 等待订单执行

                                try:
                                    # 重新获取实际持仓
                                    positions_info = (
                                        self.client.futures_position_information(
                                            symbol=symbol
                                        )
                                    )
                                    actual_position = next(
                                        (
                                            p
                                            for p in positions_info
                                            if p["symbol"] == symbol
                                        ),
                                        None,
                                    )

                                    if actual_position:
                                        remaining_amt = float(
                                            actual_position["positionAmt"]
                                        )
                                        remaining_quantity = abs(remaining_amt)

                                        # 🔧 修复：对剩余数量也进行精度调整
                                        if (
                                            "step_size" in locals()
                                            and remaining_quantity > 0
                                        ):
                                            remaining_adjusted = (
                                                round(remaining_quantity / step_size)
                                                * step_size
                                            )
                                            if step_size >= 1:
                                                remaining_adjusted = int(
                                                    remaining_adjusted
                                                )
                                            else:
                                                remaining_adjusted = round(
                                                    remaining_adjusted, qty_precision
                                                )
                                            remaining_quantity = remaining_adjusted

                                        logging.info(
                                            f"📊 {symbol} 重新获取剩余持仓: {remaining_quantity}"
                                        )

                                        if remaining_quantity > 0:
                                            # 平仓剩余仓位
                                            remaining_order = (
                                                self.client.futures_create_order(
                                                    symbol=symbol,
                                                    side=close_side,
                                                    type="MARKET",
                                                    quantity=remaining_quantity,
                                                )
                                            )
                                            logging.info(
                                                f"✅ {symbol} 成功平仓剩余仓位 ({remaining_quantity})"
                                            )
                                        else:
                                            logging.info(
                                                f"✅ {symbol} 所有仓位已平仓完毕"
                                            )
                                    else:
                                        logging.warning(
                                            f"⚠️ {symbol} 无法获取剩余持仓信息，可能已全部平仓"
                                        )

                                except Exception as remaining_error:
                                    logging.error(
                                        f"❌ {symbol} 平仓剩余仓位失败: {remaining_error}"
                                    )
                                    send_email_alert(
                                        "平仓失败 - 需要人工干预",
                                        f"{symbol} 分批平仓仍失败，请立即检查账户状态并手动平仓\n"
                                        f"已平仓: {half_quantity}\n"
                                        f"剩余仓位: 未知\n"
                                        f"错误信息: {remaining_error}",
                                    )
                                    raise remaining_error

                            except Exception as half_error:
                                logging.error(
                                    f"❌ {symbol} 分批平仓也失败: {half_error}"
                                )
                                # 发送紧急报警
                                send_email_alert(
                                    "平仓完全失败 - 紧急",
                                    f"{symbol} 所有平仓尝试都失败，请立即检查账户\n"
                                    f"建仓价格: {position['entry_price']}\n"
                                    f"当前价格: {self.client.futures_symbol_ticker(symbol=symbol)['price']}\n"
                                    f"持仓数量: {quantity}\n"
                                    f"杠杆: {self.leverage}x\n"
                                    f"最后错误: {half_error}",
                                )
                                raise margin_error  # 重新抛出原错误
                        else:
                            raise margin_error  # 其他错误直接抛出
                else:
                    raise

            # 🔧 v4 修复 #10：使用订单实际成交价（avgPrice）而非 ticker
            try:
                actual_exit_price = float(order.get("avgPrice", 0))
                if actual_exit_price <= 0:
                    ticker = self.client.futures_symbol_ticker(symbol=symbol)
                    actual_exit_price = float(ticker["price"])
                    logging.warning(f"⚠️ {symbol} 平仓订单无 avgPrice，使用 ticker 价格")
                else:
                    logging.info(f"💰 {symbol} 实际平仓成交价: {actual_exit_price:.6f}")
            except (TypeError, ValueError, AttributeError):
                ticker = self.client.futures_symbol_ticker(symbol=symbol)
                actual_exit_price = float(ticker["price"])

            exit_price = actual_exit_price

            # 计算盈亏（根据实际仓位方向）
            entry_price = position["entry_price"]
            if is_long_position:
                # 做多：价格上涨=盈利
                pnl_pct = (exit_price - entry_price) / entry_price
            else:
                # 做空：价格下跌=盈利
                pnl_pct = (entry_price - exit_price) / entry_price
            pnl_value = pnl_pct * position["position_value"] * self.leverage

            # 计算持仓时长
            entry_time = datetime.fromisoformat(position["entry_time"])
            current_time = datetime.now(timezone.utc)
            elapsed_hours = (current_time - entry_time).total_seconds() / 3600

            # 从持仓列表移除（加锁保护）
            with self._positions_sync_lock:
                self.positions.remove(position)

            # 记录变动后状态
            after_state = {
                "持仓数量": 0,
                "状态": "已平仓",
                "平仓价格": exit_price,
                "盈亏金额": pnl_value,
                "盈亏比例": pnl_pct,
            }

            # 定义平仓原因中文映射
            reason_map = {
                "take_profit": "止盈",
                "stop_loss": "止损",
                "max_hold_time": "72小时强制平仓",
                "max_gain_24h": "24h涨幅止损",
                "early_stop_loss_2h": "2h及早止损",
                "early_stop_loss_12h": "12h及早止损",
                "manual_close": "手动平仓",
                "btc_yesterday_yang_flatten_open": "BTC昨日阳线(UTC日初一刀切)",
            }
            reason_cn = reason_map.get(reason, reason)

            # 统一日志记录
            change_type = "manual_close" if reason == "manual_close" else "auto_close"
            self.server_log_position_change(
                change_type,
                symbol,
                {
                    "平仓原因": reason_cn,
                    "持仓时长": f"{elapsed_hours:.1f}小时",
                    "成交价格": exit_price,
                    "盈亏比例": f"{pnl_pct * 100:.2f}%",
                    "盈亏金额": pnl_value,
                },
                before_state,
                after_state,
                success=True,
            )

            # 从记录文件中删除
            self.server_save_positions_record()

            # 写入交易历史记录
            try:
                trade_record = {
                    "symbol": symbol,
                    "direction": "long" if is_long_position else "short",
                    "entry_price": position["entry_price"],
                    "exit_price": exit_price,
                    "quantity": position["quantity"],
                    "entry_time": position["entry_time"],
                    "close_time": current_time.isoformat(),
                    "elapsed_hours": round(elapsed_hours, 2),
                    "pnl_pct": round(pnl_pct * 100, 4),
                    "pnl_value": round(pnl_value, 4),
                    "reason": reason,
                    "reason_cn": reason_cn,
                    "position_value": position.get("position_value", 0),
                    "leverage": position.get("leverage", self.leverage),
                }
                if os.path.exists(TRADE_HISTORY_FILE):
                    with open(TRADE_HISTORY_FILE, "r", encoding="utf-8") as f:
                        history = json.load(f)
                else:
                    history = []
                history.append(trade_record)
                with open(TRADE_HISTORY_FILE, "w", encoding="utf-8") as f:
                    json.dump(history, f, ensure_ascii=False, indent=2)
            except Exception as th_err:
                logging.error(f"❌ 写入交易历史失败: {th_err}")

            # 🛡️ 更新连续亏损计数
            if pnl_value < 0:
                self.consecutive_losses += 1
                self.last_loss_time = datetime.now(timezone.utc)
                if (
                    self.max_consecutive_losses > 0
                    and self.consecutive_losses >= self.max_consecutive_losses
                ):
                    self.cooldown_until = datetime.now(timezone.utc) + timedelta(
                        hours=self.cooldown_hours
                    )
                    logging.warning(
                        f"🔒 连续亏损 {self.consecutive_losses} 笔，进入冷却期 {self.cooldown_hours}h"
                    )
            else:
                self.consecutive_losses = 0
                self.cooldown_until = None

            # 🆕 平仓完成摘要日志（包含订单取消详情）
            cancelled_orders_str = ""
            if cancelled_orders:
                for co in cancelled_orders:
                    cancelled_orders_str += (
                        f"\n║   - {co['type']}: ID {co['id']}, 价格 {co['price']}"
                    )
            else:
                cancelled_orders_str = "\n║   - 无未成交订单"

            # reason_cn 已在前面定义

            logging.info(f"""
╔════════════════════════════════════════════════════════════════════════════╗
║ 💰 {symbol} 平仓完成                                                         ║
╠════════════════════════════════════════════════════════════════════════════╣
║ 平仓原因: {reason_cn}
║ 建仓时间: {entry_time}
║ 平仓时间: {current_time}
║ 持仓时长: {elapsed_hours:.1f}小时
║ 
║ 价格信息:
║   - 建仓价格: ${entry_price:.6f}
║   - 平仓价格: ${exit_price:.6f}
║   - 价格变化: {pnl_pct * 100:+.2f}%
║ 
║ 盈亏情况:
║   - 持仓数量: {quantity}
║   - 投入金额: ${position["position_value"]:.2f}
║   - 杠杆倍数: {self.leverage}x
║   - 盈亏金额: ${pnl_value:+.2f} USDT
║   - 盈亏比例: {pnl_pct * 100:+.2f}%
║ 
║ 取消的订单:{cancelled_orders_str}
║ 
║ 剩余持仓: {len(self.positions)}个
╚════════════════════════════════════════════════════════════════════════════╝
""")

            # 🔧 强制刷新日志（确保平仓日志立即写入）
            flush_logging_handlers()

            # 🔧 修复3：平仓后再次检查并清理残留订单
            try:
                time.sleep(0.5)  # 等待0.5秒确保订单状态同步
                algo_orders_after = self.client.futures_get_open_algo_orders(
                    symbol=symbol
                )
                if algo_orders_after:
                    logging.warning(
                        f"⚠️ {symbol} 平仓后仍有 {len(algo_orders_after)} 个残留订单，再次清理"
                    )
                    for order in algo_orders_after:
                        try:
                            self.client.futures_cancel_algo_order(
                                symbol=symbol, algoId=order["algoId"]
                            )
                            logging.info(
                                f"✅ {symbol} 已清理残留订单: {order['orderType']} (algoId: {order['algoId']})"
                            )
                        except Exception as cleanup_error:
                            logging.warning(
                                f"⚠️ {symbol} 清理残留订单失败: {cleanup_error}"
                            )
            except Exception as cleanup_check_error:
                logging.warning(f"⚠️ {symbol} 检查残留订单失败: {cleanup_check_error}")

            notify_positions_changed()

        except Exception as e:
            logging.error(f"❌ 平仓失败 {position['symbol']}: {e}")

            # 🚨 关键修复：平仓失败时重新设置止盈止损订单
            # 因为前面已经取消了所有订单，如果平仓失败，持仓还在但止盈止损没了
            try:
                logging.warning(
                    f"🔄 {position['symbol']} 平仓失败，尝试重新设置止盈止损订单..."
                )

                # 重新设置止盈止损订单
                self.server_setup_tp_sl_orders(position)

                logging.info(f"✅ {position['symbol']} 已重新设置止盈止损订单")

            except Exception as reset_error:
                logging.error(
                    f"❌ 重新设置止盈止损失败 {position['symbol']}: {reset_error}"
                )

                # 发送紧急告警
                send_email_alert(
                    "止盈止损重设失败 - 紧急",
                    f"{position['symbol']} 平仓失败且重新设置止盈止损也失败\n"
                    f"建仓价格: {position['entry_price']}\n"
                    f"当前价格: {self.client.futures_symbol_ticker(symbol=position['symbol'])['price']}\n"
                    f"请立即手动设置止盈止损！\n"
                    f"平仓错误: {e}\n"
                    f"重设错误: {reset_error}",
                )

    def server_monitor_positions(self):
        """监控持仓（集成动态止盈订单更新）- 服务器版本"""
        with self._positions_sync_lock:
            if not self.positions:
                return  # 没有持仓，直接返回

        # 与币安对齐：交易所已平仓则从本地移除（避免页面/逻辑残留）
        self.server_prune_flat_positions_from_exchange()
        with self._positions_sync_lock:
            if not self.positions:
                return

            # 🔒 并发控制：获取所有持仓的symbol锁
            symbols_to_process = [pos["symbol"] for pos in self.positions]
        acquired_locks = []

        try:
            # 尝试获取所有symbol的锁（非阻塞）
            for symbol in symbols_to_process:
                with self.position_lock_master:
                    if symbol not in self.position_locks:
                        self.position_locks[symbol] = threading.Lock()
                    symbol_lock = self.position_locks[symbol]

                if symbol_lock.acquire(blocking=False):
                    acquired_locks.append((symbol, symbol_lock))
                else:
                    logging.debug(f"🔒 {symbol} 正在被其他线程处理，跳过本次监控")
                    continue

            # 只处理成功获取锁的持仓
            locked_symbols = {symbol for symbol, _ in acquired_locks}

            with self._positions_sync_lock:
                positions_snapshot = self.positions[:]
            for position in positions_snapshot:  # 在锁外迭代快照
                symbol = position["symbol"]
                logging.debug(
                    f"🔍 检查锁状态 {symbol}: locked_symbols={list(locked_symbols)}"
                )
                if symbol not in locked_symbols:
                    logging.debug(f"⚠️ {symbol} 未获取锁，跳过处理")
                    continue  # 跳过未获取锁的持仓

                try:
                    # 监控每 30s 一轮，勿用 info（多仓时终端会刷屏）
                    logging.debug(f"✅ {symbol} 获取锁成功，开始处理")
                    # 1. 检查平仓条件
                    exit_reason = self.server_check_exit_conditions(position)
                    if exit_reason:
                        self.server_close_position(position, exit_reason)
                        continue

                    # 1.5. 检查并创建缺失的止盈止损订单
                    logging.debug(f"🔍 监控循环检查 {position['symbol']} 止盈止损")
                    self.server_check_and_create_tp_sl(position)

                    # 1.6. 管理止盈止损订单互斥（一个成交后取消另一个）
                    self.server_manage_tp_sl_orders(position)

                    # 2. 检查是否需要动态调整止盈订单
                    entry_time = datetime.fromisoformat(position["entry_time"])
                    current_time = datetime.now(timezone.utc)
                    elapsed_hours = (current_time - entry_time).total_seconds() / 3600

                    # 2小时检查窗口（2.0-2.5小时）
                    if 2.0 <= elapsed_hours < 3.0 and not position.get("tp_2h_checked"):
                        logging.info(
                            f"🕐 {position['symbol']} 进入2小时检查窗口 ({elapsed_hours:.2f}h)"
                        )

                        # 计算新止盈 - 现在返回元组
                        (
                            new_tp_pct,
                            should_check_2h,
                            should_check_12h,
                            is_consecutive,
                        ) = self.server_calculate_dynamic_tp(position)

                        # ✅ 关键修复：从交易所获取实际的止盈价格，而不是从position记录
                        symbol = position["symbol"]
                        entry_price = position["entry_price"]
                        exchange_tp_order = self.server_get_exchange_tp_order(symbol)

                        if exchange_tp_order:
                            # 从交易所订单反推止盈比例
                            exchange_tp_price = float(exchange_tp_order["triggerPrice"])
                            old_tp_pct = abs(
                                (entry_price - exchange_tp_price) / entry_price * 100
                            )  # 确保百分比总是正数
                            logging.info(
                                f"📊 {symbol} 当前交易所止盈: {old_tp_pct:.1f}%, 新止盈: {new_tp_pct:.1f}%"
                            )
                        else:
                            # 从position记录中获取当前止盈比例，避免使用错误的默认值
                            old_tp_pct = position.get("tp_pct", self.strong_coin_tp_pct)
                            exchange_tp_price = entry_price * (
                                1 - old_tp_pct / 100
                            )  # 计算默认止盈价格
                            logging.warning(
                                f"⚠️ {symbol} 未找到交易所止盈订单，使用记录值{old_tp_pct:.1f}%，价格{exchange_tp_price:.6f}"
                            )

                        # 如果止盈比例改变，更新交易所订单
                        if abs(new_tp_pct - old_tp_pct) > 0.5:  # 差异超过0.5%才更新
                            # 记录变动前状态
                            before_state = {
                                "止盈百分比": old_tp_pct,
                                "止盈价格": exchange_tp_price,
                            }

                            success = self.server_update_exchange_tp_order(
                                position, new_tp_pct
                            )
                            if success:
                                # 🔧 修复：只有在订单更新成功后才更新position状态和标记
                                position["tp_pct"] = new_tp_pct
                                if should_check_2h:
                                    position["tp_2h_checked"] = True

                                # 记录变动后状态
                                entry_price = position["entry_price"]
                                new_tp_price = entry_price * (1 - new_tp_pct / 100)
                                after_state = {
                                    "止盈百分比": new_tp_pct,
                                    "止盈价格": new_tp_price,
                                }

                                # 统一日志记录
                                self.server_log_position_change(
                                    "dynamic_tp",
                                    position["symbol"],
                                    {
                                        "触发类型": "2小时动态止盈",
                                        "判断结果": "中等币"
                                        if new_tp_pct == self.medium_coin_tp_pct
                                        else "强势币",
                                        "时长": f"{elapsed_hours:.1f}小时",
                                    },
                                    before_state,
                                    after_state,
                                    success=True,
                                )

                                # 保存更新后的记录
                                self.server_save_positions_record()
                            else:
                                # 记录失败 - 不更新position状态
                                self.server_log_position_change(
                                    "dynamic_tp",
                                    position["symbol"],
                                    {
                                        "触发类型": "2小时动态止盈",
                                        "操作": "更新止盈订单",
                                    },
                                    before_state,
                                    None,
                                    success=False,
                                    error_msg="止盈订单更新失败",
                                )
                        else:
                            # 即使没变化，也标记为已检查
                            if should_check_2h:
                                position["tp_2h_checked"] = True
                                # 保存标记状态
                                self.server_save_positions_record()
                            logging.info(
                                f"ℹ️ {position['symbol']} 2h判断完成，止盈维持{old_tp_pct:.1f}%"
                            )

                    # 12小时检查窗口（12.0-12.5小时）
                    if 12.0 <= elapsed_hours < 13.0 and not position.get(
                        "tp_12h_checked"
                    ):
                        logging.info(
                            f"🕐 {position['symbol']} 进入12小时检查窗口 ({elapsed_hours:.2f}h)"
                        )

                        # 计算新止盈 - 现在返回元组
                        (
                            new_tp_pct,
                            should_check_2h,
                            should_check_12h,
                            is_consecutive,
                        ) = self.server_calculate_dynamic_tp(position)

                        # ✅ 关键修复：从交易所获取实际的止盈价格，而不是从position记录
                        symbol = position["symbol"]
                        entry_price = position["entry_price"]
                        exchange_tp_order = self.server_get_exchange_tp_order(symbol)

                        if exchange_tp_order:
                            # 从交易所订单反推止盈比例
                            exchange_tp_price = float(exchange_tp_order["triggerPrice"])
                            old_tp_pct = abs(
                                (entry_price - exchange_tp_price) / entry_price * 100
                            )  # 确保百分比总是正数
                            logging.info(
                                f"📊 {symbol} 当前交易所止盈: {old_tp_pct:.1f}%, 新止盈: {new_tp_pct:.1f}%"
                            )
                        else:
                            # 从position记录中获取当前止盈比例，避免使用错误的默认值
                            old_tp_pct = position.get("tp_pct", self.medium_coin_tp_pct)
                            exchange_tp_price = entry_price * (
                                1 - old_tp_pct / 100
                            )  # 计算默认止盈价格
                            logging.warning(
                                f"⚠️ {symbol} 未找到交易所止盈订单，使用记录值{old_tp_pct:.1f}%，价格{exchange_tp_price:.6f}"
                            )

                        # 如果止盈比例改变，更新交易所订单
                        if abs(new_tp_pct - old_tp_pct) > 0.5:  # 差异超过0.5%才更新
                            # 记录变动前状态
                            before_state = {
                                "止盈百分比": old_tp_pct,
                                "止盈价格": exchange_tp_price,
                            }

                            success = self.server_update_exchange_tp_order(
                                position, new_tp_pct
                            )
                            if success:
                                # 🔧 修复：只有在订单更新成功后才更新position状态和标记
                                position["tp_pct"] = new_tp_pct
                                if should_check_12h:
                                    position["tp_12h_checked"] = True
                                if is_consecutive:
                                    position["is_consecutive_confirmed"] = True

                                # 记录变动后状态
                                entry_price = position["entry_price"]
                                new_tp_price = entry_price * (1 - new_tp_pct / 100)
                                after_state = {
                                    "止盈百分比": new_tp_pct,
                                    "止盈价格": new_tp_price,
                                }

                                # 统一日志记录
                                self.server_log_position_change(
                                    "dynamic_tp",
                                    position["symbol"],
                                    {
                                        "触发类型": "12小时动态止盈",
                                        "判断结果": "弱势币"
                                        if new_tp_pct == self.weak_coin_tp_pct
                                        else (
                                            "中等币"
                                            if new_tp_pct == self.medium_coin_tp_pct
                                            else "强势币"
                                        ),
                                        "连续确认": is_consecutive,
                                        "时长": f"{elapsed_hours:.1f}小时",
                                    },
                                    before_state,
                                    after_state,
                                    success=True,
                                )

                                # 保存更新后的记录
                                self.server_save_positions_record()
                            else:
                                # 记录失败 - 不更新position状态
                                self.server_log_position_change(
                                    "dynamic_tp",
                                    position["symbol"],
                                    {
                                        "触发类型": "12小时动态止盈",
                                        "操作": "更新止盈订单",
                                    },
                                    before_state,
                                    None,
                                    success=False,
                                    error_msg="止盈订单更新失败",
                                )
                        else:
                            # 即使没变化，也标记为已检查
                            if should_check_12h:
                                position["tp_12h_checked"] = True
                                # 保存标记状态
                                self.server_save_positions_record()
                            logging.info(
                                f"ℹ️ {position['symbol']} 12h判断完成，止盈维持{old_tp_pct:.1f}%"
                            )

                except Exception as pos_error:
                    logging.error(
                        f"❌ 处理持仓 {position['symbol']} 时发生错误: {pos_error}"
                    )

        finally:
            # 🔓 确保释放所有获取的锁
            for symbol, lock in acquired_locks:
                try:
                    if lock.locked():
                        lock.release()
                except RuntimeError as e:
                    logging.warning(f"释放锁失败 {symbol}: {e}")

    def server_get_tp_sl_from_binance(self, symbol: str) -> tuple:
        """从币安查询止盈止损价格 - 服务器版本"""
        try:
            tp_price_val = "N/A"
            sl_price_val = "N/A"
            logging.info(f"🔍 开始查询 {symbol} 的止盈止损价格...")

            # 优先从position对象中获取缓存的止盈止损价格
            current_position = next(
                (p for p in self.positions if p["symbol"] == symbol), None
            )
            if current_position:
                # 检查position中是否已有价格记录
                cached_tp = current_position.get("tp_price")
                cached_sl = current_position.get("sl_price")
                if cached_tp and cached_sl:
                    tp_price_val = f"{cached_tp:.6f}"
                    sl_price_val = f"{cached_sl:.6f}"
                    logging.info(
                        f"✅ 从position记录获取 {symbol} 止盈止损价格: TP={tp_price_val}, SL={sl_price_val}"
                    )
                    return tp_price_val, sl_price_val
                else:
                    logging.debug(f"🔍 {symbol} position中缺少价格信息，从交易所查询")

            # 从交易所查询止盈止损订单
            try:
                algo_orders = self.client.futures_get_open_algo_orders(symbol=symbol)
                logging.info(
                    f"🔍 {symbol} 算法订单查询结果: 找到 {len(algo_orders)} 个订单"
                )
                for order in algo_orders:
                    order_type = order.get("orderType", "")
                    if order_type in FUTURES_ALGO_TP_TYPES:
                        trig = futures_algo_trigger_price(order)
                        if trig is not None:
                            tp_price_val = f"{trig:.6f}"
                            logging.info(
                                f"✅ 从算法订单找到 {symbol} 止盈: {order_type}, 触发价={tp_price_val}"
                            )
                    elif order_type in FUTURES_ALGO_SL_TYPES:
                        trig = futures_algo_trigger_price(order)
                        if trig is not None:
                            sl_price_val = f"{trig:.6f}"
                            logging.info(
                                f"✅ 从算法订单找到 {symbol} 止损: {order_type}, 触发价={sl_price_val}"
                            )
            except Exception as algo_error:
                logging.warning(f"⚠️ {symbol} 查询算法订单失败: {algo_error}")

            logging.info(
                f"🔍 {symbol} 算法订单查询完成, TP={tp_price_val}, SL={sl_price_val}"
            )

            # 如果算法订单也查询失败，尝试查询普通订单 (限价止盈止损)
            logging.info(
                f"🔍 {symbol} 开始查询普通订单, 当前TP={tp_price_val}, SL={sl_price_val}"
            )
            if tp_price_val == "N/A" or sl_price_val == "N/A":
                try:
                    all_orders = self.client.futures_get_open_orders(symbol=symbol)
                    logging.info(
                        f"🔍 {symbol} 普通订单查询结果: 找到 {len(all_orders)} 个订单"
                    )
                    for order in all_orders:
                        order_type = order.get("type", "")
                        order_side = order.get("side", "")

                        # 检查止盈订单（普通挂单，兼容旧单）
                        if order_type in ("TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET"):
                            if tp_price_val == "N/A":
                                trigger_price = order.get("stopPrice") or order.get(
                                    "price"
                                )
                                logging.info(
                                    f"🔍 {symbol} 发现止盈类订单, 触发价={trigger_price}, 完整订单={order}"
                                )
                                if trigger_price and float(trigger_price) > 0:
                                    tp_price_val = f"{float(trigger_price):.6f}"
                                    logging.info(
                                        f"✅ 从普通订单找到 {symbol} 止盈: 触发价={tp_price_val}"
                                    )

                        # 检查止损订单 - STOP_LOSS (市场止损)
                        elif order_type == "STOP_MARKET":
                            if sl_price_val == "N/A":  # 只在没找到止损时才使用
                                trigger_price = order.get(
                                    "stopPrice"
                                )  # STOP_LOSS使用stopPrice作为触发价格
                                logging.info(
                                    f"🔍 {symbol} 发现STOP_LOSS订单, stopPrice={trigger_price}, 完整订单={order}"
                                )
                                if trigger_price and float(trigger_price) > 0:
                                    sl_price_val = f"{float(trigger_price):.6f}"
                                    logging.info(
                                        f"✅ 从普通订单找到 {symbol} 市场止损: 触发价={sl_price_val}"
                                    )

                        # 备选：检查限价止盈订单 (SELL 方向的 LIMIT 订单可能作为止盈)
                        elif order_type == "LIMIT" and order_side == "SELL":
                            if tp_price_val == "N/A":  # 只在没找到止盈时才使用
                                price = order.get("price")
                                if price:
                                    tp_price_val = f"{float(price):.6f}"
                                    logging.info(
                                        f"✅ 从普通订单找到 {symbol} 限价止盈: 价格={tp_price_val}"
                                    )

                        # 检查限价止损订单 (BUY 方向的 LIMIT 订单可能作为止损)
                        elif order_type == "LIMIT" and order_side == "BUY":
                            if sl_price_val == "N/A":  # 只在没找到止损时才使用
                                price = order.get("price")
                                if price:
                                    sl_price_val = f"{float(price):.6f}"
                                    logging.info(
                                        f"✅ 从普通订单找到 {symbol} 限价止损: 价格={sl_price_val}"
                                    )

                except Exception as orders_error:
                    logging.warning(f"⚠️ {symbol} 查询普通订单失败: {orders_error}")

            logging.info(
                f"📊 {symbol} 止盈止损查询完成: TP={tp_price_val}, SL={sl_price_val}"
            )
            return tp_price_val, sl_price_val

        except Exception as e:
            logging.warning(f"查询 {symbol} 止盈止损失败: {e}")
            return "N/A", "N/A"


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
    """写入 signal_history.json，保留最近 90 天。

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


# ==================== 后台线程 ====================
def scan_loop():
    """信号扫描循环（每小时3-8分钟扫描一次）

    ⚠️ 重要：每小时固定时间扫描，避免价格已经变化
    - 扫描时间窗口：每小时的第3-8分钟（UTC时间）
    - 每小时只扫描一次，避免重复
    - 检查上一个完整小时的卖量暴涨信号
    """
    global is_running

    logging.info("📡 信号扫描线程已启动")
    last_scan_hour = None  # 记录上次扫描的小时，避免重复
    consecutive_failures = 0  # 连续失败计数

    while True:
        try:
            if not is_running:
                time.sleep(10)
                continue

            # 获取当前UTC时间
            now = datetime.now(timezone.utc)
            current_hour = now.replace(minute=0, second=0, microsecond=0)

            # 每小时3-8分钟扫描，且本小时未扫描过（v4 修复：窗口从 2 分钟扩大到 5 分钟）
            if 3 <= now.minute < 8 and last_scan_hour != current_hour:
                logging.info(
                    f"🔍 [定时扫描] UTC {now.strftime('%Y-%m-%d %H:%M:%S')} 开始扫描..."
                )

                try:
                    if not strategy.server_sync_wallet_snapshot():
                        strategy.account_balance = strategy.server_get_account_balance()
                        strategy.account_available_balance = strategy.account_balance
                    logging.info(
                        f"💰 账户余额: ${strategy.account_balance:.2f}  可用余额: ${strategy.account_available_balance:.2f}"
                    )

                    # 🔧 强制刷新日志
                    flush_logging_handlers()

                    # 扫描信号
                    signals = strategy.server_scan_sell_surge_signals()

                    if signals:
                        logging.info(f"✅ 发现 {len(signals)} 个信号")
                        # 显示前5个信号
                        for signal in signals[:5]:
                            logging.info(
                                f"   {signal['symbol']}: {signal['surge_ratio']:.2f}倍 @ {signal['price']:.6f}"
                            )

                        # 🔧 强制刷新日志
                        flush_logging_handlers()

                        # 尝试建仓：与 hm1l 一致，按卖量倍数降序连续尝试（受 max_positions/max_daily_entries/可选 cap）
                        opened_count = 0
                        opened_syms = set()
                        cap = strategy.max_opens_per_scan
                        for signal in signals:
                            if not is_running:
                                break
                            if cap > 0 and opened_count >= cap:
                                break
                            if strategy.server_open_position(signal):
                                opened_count += 1
                                opened_syms.add(signal["symbol"])
                                logging.info(
                                    f"🚀 开仓成功: {signal['symbol']} (本轮第{opened_count}笔)"
                                )

                        save_signal_records(signals, now.isoformat(), opened_syms)

                        if opened_count == 0:
                            logging.warning(
                                "⚠️ 所有信号均无法建仓（已达到限制或已持有）"
                            )
                    else:
                        logging.info("⚠️ 未发现信号")

                    # 🔧 强制刷新日志
                    flush_logging_handlers()

                    # 扫描成功，重置失败计数
                    consecutive_failures = 0

                except Exception as scan_error:
                    consecutive_failures += 1
                    error_msg = str(scan_error)

                    # 判断是否为网络问题
                    is_network_error = any(
                        keyword in error_msg.lower()
                        for keyword in [
                            "network",
                            "connection",
                            "timeout",
                            "proxy",
                            "ssl",
                            "max retries",
                            "unreachable",
                            "timed out",
                        ]
                    )

                    if is_network_error:
                        if consecutive_failures == 1:
                            logging.warning(
                                f"🌐 网络异常 (第{consecutive_failures}次): {error_msg[:100]}"
                            )
                        elif consecutive_failures == 3:
                            logging.error(f"🚨 网络连续失败{consecutive_failures}次！")
                            send_email_alert(
                                "网络连续失败警告",
                                f"信号扫描网络连续失败{consecutive_failures}次\n\n错误信息：{error_msg}",
                            )
                        elif consecutive_failures >= 5:
                            logging.critical(
                                f"🚨🚨🚨 网络连续失败{consecutive_failures}次！系统可能无法正常交易！"
                            )
                            send_email_alert(
                                "【紧急】网络严重异常",
                                f"信号扫描网络连续失败{consecutive_failures}次！\n\n系统可能无法正常交易，请立即检查！\n\n错误信息：{error_msg}",
                            )
                    else:
                        logging.error(
                            f"❌ 扫描错误 (第{consecutive_failures}次): {error_msg[:100]}"
                        )
                        if consecutive_failures >= 3:
                            send_email_alert(
                                "信号扫描异常",
                                f"信号扫描连续失败{consecutive_failures}次\n\n错误信息：{error_msg}",
                            )

                # 标记本小时已扫描
                last_scan_hour = current_hour

                # 扫描完成后等待到下一分钟
                time.sleep(60)
            else:
                # 不在扫描时间窗口，等待30秒后再检查
                time.sleep(30)

        except Exception as e:
            logging.error(f"❌ 扫描循环异常: {e}")
            time.sleep(60)


def monitor_loop():
    """持仓监控循环（每30秒检查一次）"""
    global is_running

    logging.info("👁️ 持仓监控线程已启动")
    consecutive_failures = 0  # 连续失败计数
    check_count = 0  # 检查计数器

    while True:
        try:
            if not is_running:
                time.sleep(10)
                continue

            check_count += 1

            # BTC 昨日阳线 → 新 UTC 日一刀切空仓（早于常规止盈止损扫描）
            strategy.server_maybe_btc_yesterday_yang_flatten_at_new_utc_day()

            # 启动时加载失败 → 重试加载交易所持仓
            if not strategy.positions_loaded:
                logging.warning("🔄 启动时未加载到交易所持仓，尝试重新加载...")
                try:
                    strategy.server_load_existing_positions()
                    if strategy.positions_loaded:
                        logging.info("✅ 成功重新加载交易所持仓")
                    else:
                        logging.warning("⚠️ 重新加载失败，将在下次监控循环重试")
                except Exception as load_err:
                    logging.error(f"❌ 重新加载持仓失败: {load_err}")

            # 监控持仓
            strategy.server_monitor_positions()

            # 每10次检查（5分钟）输出一次状态
            if check_count % 10 == 0:
                logging.info(
                    f"👁️ [监控] 已检查{check_count}次，持仓{len(strategy.positions)}个"
                )
                # 🔧 强制刷新日志
                flush_logging_handlers()

            # 监控成功，重置失败计数
            consecutive_failures = 0

            # 每30秒检查一次（与ae.py保持一致）
            time.sleep(30)

        except Exception as e:
            consecutive_failures += 1
            error_msg = str(e)

            # 判断是否为网络问题
            is_network_error = any(
                keyword in error_msg.lower()
                for keyword in [
                    "network",
                    "connection",
                    "timeout",
                    "proxy",
                    "ssl",
                    "max retries",
                    "unreachable",
                    "timed out",
                ]
            )

            if is_network_error:
                if consecutive_failures == 1:
                    logging.warning(f"🌐 持仓监控网络异常 (第{consecutive_failures}次)")
                elif consecutive_failures >= 5:
                    logging.error(f"🚨 持仓监控网络连续失败{consecutive_failures}次！")
                    send_email_alert(
                        "持仓监控网络异常",
                        f"持仓监控网络连续失败{consecutive_failures}次\n\n持仓显示可能延迟！\n\n错误信息：{error_msg}",
                    )
            else:
                logging.error(
                    f"❌ 监控循环错误 (第{consecutive_failures}次): {error_msg[:100]}"
                )

            time.sleep(30)


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


def daily_report_loop():
    """每个 UTC 自然日自动发送至多一次；本日已发过则长休眠至下一 UTC 日（不按小时反复检查）。"""
    global is_running

    logging.info("📧 每日报告线程已启动（每 UTC 日最多自动 1 次）")

    while True:
        try:
            if not is_running:
                time.sleep(60)
                continue

            now = datetime.now(timezone.utc)
            current_date = now.date()
            last_sent = _daily_report_load_last_sent_date()

            if last_sent == current_date:
                time.sleep(_seconds_sleep_until_after_next_utc_midnight(now))
                continue

            logging.info("📧 开始生成每日交易报告...")
            ok = send_daily_report()
            if ok:
                logging.info(f"📧 每日报告已完成 ({current_date})")
                time.sleep(
                    _seconds_sleep_until_after_next_utc_midnight(
                        datetime.now(timezone.utc)
                    )
                )
            else:
                logging.warning("⚠️ 日报未成功落盘，1 小时后重试")
                time.sleep(3600)

        except Exception as e:
            logging.error(f"❌ 每日报告循环异常: {e}")
            time.sleep(300)  # 5分钟后重试


if __name__ == "__main__":
    main()
