"""Yesterday data cache and backup symbol list extracted from ae_server.py (lines 687-810)."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from binance.exceptions import BinanceAPIException


class YesterdayDataCache:
    """昨日数据缓存类（避免重复API调用）"""

    def __init__(self, client):
        self.client = client
        self.cache = {}
        self.cache_date = None
        logging.info("📦 初始化昨日数据缓存")

    def get_yesterday_avg_sell_api(self, symbol: str) -> Optional[float]:
        """获取昨日平均小时卖量（带缓存）- API版本"""
        try:
            if self.client is None:
                return None
            # 检查缓存是否过期
            today = datetime.now(timezone.utc).date()
            if self.cache_date != today:
                if self.cache_date:
                    logging.info(
                        f"🔄 清空昨日缓存（日期变更: {self.cache_date} -> {today}）"
                    )
                self.cache = {}
                self.cache_date = today

            # 从缓存读取
            if symbol in self.cache:
                return self.cache[symbol]

            # 从API获取昨日日K线
            yesterday = today - timedelta(days=1)
            yesterday_start = int(
                datetime.combine(yesterday, datetime.min.time())
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
            yesterday_end = int(
                datetime.combine(yesterday, datetime.max.time())
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )

            klines = self.client.futures_klines(
                symbol=symbol,
                interval="1d",
                startTime=yesterday_start,
                endTime=yesterday_end,
                limit=1,
            )

            if not klines:
                return None

            # 计算昨日平均小时卖量
            volume = float(klines[0][5])  # 总成交量
            active_buy_volume = float(klines[0][9])  # 主动买入量
            total_sell = volume - active_buy_volume
            avg_hour_sell = total_sell / 24.0

            # 缓存结果
            self.cache[symbol] = avg_hour_sell

            return avg_hour_sell

        except Exception as e:
            logging.error(f"❌ 获取 {symbol} 昨日数据失败: {e}")
            return None

    def prefetch_all(self, symbols: list):
        """扫描前并发预热所有 symbol 的昨日数据，避免扫描主循环中逐个发 API 请求"""
        missing = [s for s in symbols if s not in self.cache]
        if not missing:
            return
        logging.info(f"📦 开始并发预热昨日缓存，共 {len(missing)} 个交易对...")
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(self.get_yesterday_avg_sell_api, s): s for s in missing
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except BinanceAPIException as e:
                    logging.warning(f"预热缓存失败: {e}")
                except Exception as e:
                    logging.debug(f"预热缓存异常: {e}")
        logging.info(f"📦 昨日缓存预热完成")


# 备用交易对列表（API获取失败时使用）
BACKUP_SYMBOL_LIST = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "MATICUSDT",
    "DOTUSDT",
    "AVAXUSDT",
    "SHIBUSDT",
    "LTCUSDT",
    "LINKUSDT",
    "ATOMUSDT",
    "UNIUSDT",
    "ETCUSDT",
    "XLMUSDT",
    "NEARUSDT",
    "ALGOUSDT",
    "ICPUSDT",
    "APTUSDT",
    "FILUSDT",
    "LDOUSDT",
    "ARBUSDT",
    "OPUSDT",
    "SUIUSDT",
    "INJUSDT",
    "TIAUSDT",
    "ORDIUSDT",
    "RUNEUSDT",
]
