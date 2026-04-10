import os
import json
import logging
import time
import threading
import uuid
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple

from binance.exceptions import BinanceAPIException

from config import POSITIONS_RECORD_FILE, TRADE_HISTORY_FILE, CONFIG_INI_PATH
from utils.email import send_email_alert
from utils.logging_config import flush_logging_handlers


class EntryMixin:

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
