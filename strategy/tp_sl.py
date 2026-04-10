"""Take-profit / stop-loss mixin for AutoExchangeStrategy."""

import os
import subprocess
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import threading
from decimal import Decimal, ROUND_HALF_UP

from binance.exceptions import BinanceAPIException

from utils.orders import (
    FUTURES_ALGO_TP_TYPES,
    FUTURES_ALGO_SL_TYPES,
    futures_algo_trigger_price,
    position_close_side,
    pick_tp_sl_algo_candidates,
    algo_order_id_from_dict,
    cancel_order_algo_or_regular,
)
from utils.email import send_email_alert
from utils.logging_config import _log_asctime_local, _position_change_fmt_value


class TpSlMixin:

    # ------------------------------------------------------------------
    # Lines 2409-2625: server_get_5min_klines_from_binance … check_order_history
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Lines 2626-3138: server_update_exchange_tp_order, server_calculate_dynamic_tp
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Lines 3303-3658: server_create_tp_sl_orders … server_check_and_create_tp_sl
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Lines 4545-4674: server_get_tp_sl_from_binance
    # ------------------------------------------------------------------

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
