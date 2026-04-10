"""Position-monitoring mixin for AutoExchangeStrategy."""

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from utils.logging_config import flush_logging_handlers


class MonitorMixin:

    # ------------------------------------------------------------------
    # Lines 4247-4544: server_monitor_positions
    # ------------------------------------------------------------------

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
