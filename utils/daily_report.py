"""
Daily report related functions extracted from ae_server.py.

生成每日交易报告、保存/加载日报发送日期标记、计算休眠时间等。
"""

import logging
import os
import smtplib
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

from binance.client import Client

from utils.logging_config import log_dir
from utils.email import ALERT_EMAIL


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
    import ae_server
    strategy = ae_server.strategy
    is_running = ae_server.is_running
    start_time = ae_server.start_time

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
