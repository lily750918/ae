import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from binance.exceptions import BinanceAPIException

from utils.cache import BACKUP_SYMBOL_LIST


class ScannerMixin:

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

            # 步骤1：获取信号所在日前一天的平均小时卖量（基准 = signal_dt 的前一天）
            yesterday_avg_hour_sell = self.yesterday_cache.get_yesterday_avg_sell_api(
                symbol, signal_date=signal_dt.date()
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
        """扫描卖量暴涨信号（API实时版本）- 服务器版本（优化：分批并发 + 价格预取）"""
        try:
            logging.info("🔍 开始扫描卖量暴涨信号（API模式）...")
            signals = []

            # 获取当前UTC时间
            now_utc = datetime.now(timezone.utc)
            current_hour = now_utc.replace(minute=0, second=0, microsecond=0)

            # 获取交易对列表（同时刷新 exchange_info 缓存）
            symbols = self._server_get_active_symbols()
            logging.info(f"📊 开始扫描 {len(symbols)} 个交易对...")

            # 优化：批量获取当前所有交易对价格，供信号价格 fallback 使用
            try:
                tickers = self.client.futures_symbol_ticker()
                prices_map = {t["symbol"]: float(t["price"]) for t in tickers}
            except Exception as e:
                logging.warning(f"⚠️ 扫描预取所有价格失败: {e}")
                prices_map = {}

            # 信号 K 线所在小时（上一个完整小时）
            check_hour = current_hour - timedelta(hours=1)
            check_hour_ms = int(check_hour.timestamp() * 1000)
            # 信号所在日期（跨日边界时为昨天，其余时间为今天）
            signal_date = check_hour.date()

            # 并发预热昨日数据缓存，避免扫描主循环中逐个发 API 请求
            # 传入 signal_date 确保跨日边界时基准为前天而非昨天
            self.yesterday_cache.prefetch_all(symbols, signal_date=signal_date)


            def _scan_one(symbol: str) -> Optional[Dict]:
                """扫描单个交易对，返回信号 dict 或 None"""
                try:
                    # 1. 从缓存获取信号所在日前一天的平均小时卖量
                    yesterday_avg_hour_sell = (
                        self.yesterday_cache.get_yesterday_avg_sell_api(symbol, signal_date=signal_date)
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
                        logging.debug(f"⏭️ {symbol} 时间戳不匹配: 预期 {check_hour_ms}, 实际 {hour_kline[0]}")
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
                    else:
                        # 优化：优先使用批量预取的实时价格
                        signal_price = prices_map.get(symbol, hour_close)
                        logging.debug(f"📊 {symbol} 信号价格: 使用预取价格/收盘价 {signal_price:.6f}")

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

            # 优化：分批并发扫描（max_workers=5 控制并发，每批次增加 0.2s 延时）
            batch_size = 50
            all_ratios = [] # 记录所有交易对的倍数，用于诊断
            with ThreadPoolExecutor(max_workers=5) as executor:
                for i in range(0, len(symbols), batch_size):
                    batch = symbols[i : i + batch_size]
                    futures_map = {executor.submit(_scan_one, sym): sym for sym in batch}
                    for future in as_completed(futures_map):
                        try:
                            result = future.result()
                            if result is not None:
                                signals.append(result)
                                all_ratios.append((result["symbol"], result["surge_ratio"]))
                            else:
                                # 如果没有信号，也尝试记录一下该 symbol 的倍数（通过重读缓存）
                                sym = futures_map[future]
                                yesterday_avg = self.yesterday_cache.cache.get(sym)
                                if yesterday_avg and yesterday_avg > 0:
                                    # 这里只是为了记录，不重新发请求
                                    pass
                        except Exception as e:
                            logging.warning(f"并发扫描异常: {e}")
                    
                    # 批次间停顿，平滑 API 权重消耗
                    if i + batch_size < len(symbols):
                        time.sleep(0.3)

            logging.info(f"✅ API扫描完成，共发现 {len(signals)} 个信号")
            
            # 💡 诊断日志：显示本次扫描中倍数最高的前 5 个交易对（即使没达到阈值）
            # 注意：由于 _scan_one 内部过滤了不达标的，我们需要稍微修改 _scan_one 或者在这里收集数据
            # 为了不破坏 _scan_one 的效率，我们只在没有信号时，输出一些“近乎达标”的信息
            if not signals:
                logging.info("💡 扫描诊断：未发现达标信号。请检查 sell_surge_threshold (当前: %s)", self.sell_surge_threshold)
            else:
                top_5 = sorted(signals, key=lambda x: x["surge_ratio"], reverse=True)[:5]
                logging.info("📊 本轮倍数前 5 名:")
                for s in top_5:
                    logging.info(f"   • {s['symbol']}: {s['surge_ratio']:.2f}x")

            return sorted(signals, key=lambda x: x["surge_ratio"], reverse=True)

        except Exception as e:
            logging.error(f"❌ API扫描信号失败: {e}")
            return []
