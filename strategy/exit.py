"""Exit-conditions and close-position mixin for AutoExchangeStrategy."""

import os
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from binance.exceptions import BinanceAPIException

from config import TRADE_HISTORY_FILE
from utils.email import send_email_alert
from utils.logging_config import flush_logging_handlers


class ExitMixin:

    # ------------------------------------------------------------------
    # Lines 3139-3302: server_check_exit_conditions
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Lines 3719-4246: server_close_position
    # ------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# notify_positions_changed is a module-level function defined in ae_server.py.
# At runtime the composed class lives inside ae_server, so the name is
# resolved from that module's global scope.  We import it lazily here so the
# mixin file can also be imported independently for linting / testing.
# ---------------------------------------------------------------------------
try:
    from ae_server import notify_positions_changed  # noqa: F401
except ImportError:
    def notify_positions_changed() -> None:  # type: ignore[misc]
        """Stub – real implementation lives in ae_server.py."""
        pass
