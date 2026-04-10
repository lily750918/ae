"""Background loop functions extracted from ae_server.py."""

import logging
import time
from datetime import datetime, timezone

from utils.logging_config import flush_logging_handlers

# NOTE: These functions reference ae_server module globals (strategy, is_running, etc.)
# They are imported and called from ae_server.main() which sets up the globals first.
# Use late imports inside functions to avoid circular imports:
#   import ae_server
#   ae_server.strategy, ae_server.is_running, etc.


def scan_loop():
    """信号扫描循环（每小时3-8分钟扫描一次）

    ⚠️ 重要：每小时固定时间扫描，避免价格已经变化
    - 扫描时间窗口：每小时的第3-8分钟（UTC时间）
    - 每小时只扫描一次，避免重复
    - 检查上一个完整小时的卖量暴涨信号
    """
    import ae_server

    logging.info("📡 信号扫描线程已启动")
    last_scan_hour = None  # 记录上次扫描的小时，避免重复
    consecutive_failures = 0  # 连续失败计数

    while True:
        try:
            if not ae_server.is_running:
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
                    if not ae_server.strategy.server_sync_wallet_snapshot():
                        ae_server.strategy.account_balance = ae_server.strategy.server_get_account_balance()
                        ae_server.strategy.account_available_balance = ae_server.strategy.account_balance
                    logging.info(
                        f"💰 账户余额: ${ae_server.strategy.account_balance:.2f}  可用余额: ${ae_server.strategy.account_available_balance:.2f}"
                    )

                    # 🔧 强制刷新日志
                    flush_logging_handlers()

                    # 扫描信号
                    signals = ae_server.strategy.server_scan_sell_surge_signals()

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
                        cap = ae_server.strategy.max_opens_per_scan
                        for signal in signals:
                            if not ae_server.is_running:
                                break
                            if cap > 0 and opened_count >= cap:
                                break
                            if ae_server.strategy.server_open_position(signal):
                                opened_count += 1
                                opened_syms.add(signal["symbol"])
                                logging.info(
                                    f"🚀 开仓成功: {signal['symbol']} (本轮第{opened_count}笔)"
                                )

                        ae_server.save_signal_records(signals, now.isoformat(), opened_syms)

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
                            ae_server.send_email_alert(
                                "网络连续失败警告",
                                f"信号扫描网络连续失败{consecutive_failures}次\n\n错误信息：{error_msg}",
                            )
                        elif consecutive_failures >= 5:
                            logging.critical(
                                f"🚨🚨🚨 网络连续失败{consecutive_failures}次！系统可能无法正常交易！"
                            )
                            ae_server.send_email_alert(
                                "【紧急】网络严重异常",
                                f"信号扫描网络连续失败{consecutive_failures}次！\n\n系统可能无法正常交易，请立即检查！\n\n错误信息：{error_msg}",
                            )
                    else:
                        logging.error(
                            f"❌ 扫描错误 (第{consecutive_failures}次): {error_msg[:100]}"
                        )
                        if consecutive_failures >= 3:
                            ae_server.send_email_alert(
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
    import ae_server

    logging.info("👁️ 持仓监控线程已启动")
    consecutive_failures = 0  # 连续失败计数
    check_count = 0  # 检查计数器

    while True:
        try:
            if not ae_server.is_running:
                time.sleep(10)
                continue

            check_count += 1

            # BTC 昨日阳线 → 新 UTC 日一刀切空仓（早于常规止盈止损扫描）
            ae_server.strategy.server_maybe_btc_yesterday_yang_flatten_at_new_utc_day()

            # 启动时加载失败 → 重试加载交易所持仓
            if not ae_server.strategy.positions_loaded:
                logging.warning("🔄 启动时未加载到交易所持仓，尝试重新加载...")
                try:
                    ae_server.strategy.server_load_existing_positions()
                    if ae_server.strategy.positions_loaded:
                        logging.info("✅ 成功重新加载交易所持仓")
                    else:
                        logging.warning("⚠️ 重新加载失败，将在下次监控循环重试")
                except Exception as load_err:
                    logging.error(f"❌ 重新加载持仓失败: {load_err}")

            # 监控持仓
            ae_server.strategy.server_monitor_positions()

            # 每10次检查（5分钟）输出一次状态
            if check_count % 10 == 0:
                logging.info(
                    f"👁️ [监控] 已检查{check_count}次，持仓{len(ae_server.strategy.positions)}个"
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
                    ae_server.send_email_alert(
                        "持仓监控网络异常",
                        f"持仓监控网络连续失败{consecutive_failures}次\n\n持仓显示可能延迟！\n\n错误信息：{error_msg}",
                    )
            else:
                logging.error(
                    f"❌ 监控循环错误 (第{consecutive_failures}次): {error_msg[:100]}"
                )

            time.sleep(30)


def daily_report_loop():
    """每个 UTC 自然日自动发送至多一次；本日已发过则长休眠至下一 UTC 日（不按小时反复检查）。"""
    import ae_server

    logging.info("📧 每日报告线程已启动（每 UTC 日最多自动 1 次）")

    while True:
        try:
            if not ae_server.is_running:
                time.sleep(60)
                continue

            now = datetime.now(timezone.utc)
            current_date = now.date()
            last_sent = ae_server._daily_report_load_last_sent_date()

            if last_sent == current_date:
                time.sleep(ae_server._seconds_sleep_until_after_next_utc_midnight(now))
                continue

            logging.info("📧 开始生成每日交易报告...")
            ok = ae_server.send_daily_report()
            if ok:
                logging.info(f"📧 每日报告已完成 ({current_date})")
                time.sleep(
                    ae_server._seconds_sleep_until_after_next_utc_midnight(
                        datetime.now(timezone.utc)
                    )
                )
            else:
                logging.warning("⚠️ 日报未成功落盘，1 小时后重试")
                time.sleep(3600)

        except Exception as e:
            logging.error(f"❌ 每日报告循环异常: {e}")
            time.sleep(300)  # 5分钟后重试
