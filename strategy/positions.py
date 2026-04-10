import os
import json
import logging
import time
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from binance.exceptions import BinanceAPIException

from config import POSITIONS_RECORD_FILE, TRADE_HISTORY_FILE


class PositionsMixin:

    def server_prune_flat_positions_from_exchange(
        self, positions_info: Optional[List] = None
    ) -> int:
        """
        若币安上该合约已无持仓（positionAmt 绝对值视为 0），从 self.positions 移除并保存记录。
        解决：止盈/止损在交易所成交后未走 server_close_position 导致的「幽灵持仓」。

        🔧 v4 修复：
        1. 移除前写入 data/trade_history.json（修复 #1 交易历史丢失）
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
                try:
                    from ae_server import notify_positions_changed
                    notify_positions_changed()
                except ImportError:
                    pass
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
            # 🔧 v4 修复 #11：从 data/trade_history.json 统计今日已建仓数
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
