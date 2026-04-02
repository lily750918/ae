#!/usr/bin/env python3
"""
卖量暴涨做空策略回测程序 - 单小时触发版
基于单小时主动卖量暴涨信号的做空策略（快速响应，捕捉更多机会）

═══════════════════════════════════════════════════════════════════════════════
📊 策略核心逻辑
═══════════════════════════════════════════════════════════════════════════════

【🔍 信号发现】
  触发条件：单个小时主动卖量 >= 昨日平均小时卖量 × 10倍
  倍数范围：10-14008倍（过滤过低和极端信号）
  计算公式：昨日平均小时卖量 = 昨日日K线总卖量 / 24小时
  
  信号确认逻辑：
    • 单小时：卖量≥10倍 → 立即生成信号
    • 只记录第一个满足条件的小时（避免重复）
    • 风控机制会在建仓前拦截高风险信号
  
  建仓时机：
    • 信号小时后开始尝试建仓
    • 使用5分钟K线精确定位建仓时刻

【🛡️ 风控机制】
  建仓前拦截（可选启用）：
    • 建仓涨幅风控：信号价→建仓价涨幅需在[-10%, +10%]区间
    • 24h基差变动风控：Premium Index下跌过快拒绝建仓
    • 未平仓量比率风控：OI异常波动拒绝建仓
    • 当日卖量倍数风控：特定危险区间拒绝建仓
  
  说明：大部分风控默认禁用，策略核心依赖动态止盈止损

【🎯 建仓逻辑】
  资金管理：
    • 单仓比例：当前资金 × 10%
    • 杠杆倍数：3倍（配置可调）
    • 复利模式：每次盈利后资金增长，下次建仓金额随之增长
  
  持仓限制：
    • 最大并发持仓：10个
    • 每日最多建仓：6个
  
  超时机制：
    • 信号发现后48小时内未建仓则放弃

【📈 动态止盈机制（三档梯度）】
  
  止盈阈值：
    • 强势币：33%（做空策略：价格跌33%止盈，实际收益 33%×3倍=99%）
    • 中等币：21%（做空策略：价格跌21%止盈，实际收益 21%×3倍=63%）
    • 弱势币：10%（做空策略：价格跌10%止盈，实际收益 10%×3倍=30%）
  
  判定流程：
  
    ⏰ 0-2小时：固定使用强势币止盈33%
       • 所有币都给予33%的盈利机会
       • 不做判断，避免过早降低止盈目标
    
    ⏰ 2小时判断（第一次分化）：
       • 数据源：建仓后2小时内的24根5分钟K线
       • 判定标准：统计下跌>5.5%的K线占比
       • 强势币：下跌K线占比 ≥ 60% → 保持33%止盈
       • 中等币：下跌K线占比 < 60% → 降为21%止盈
    
    ⏰ 12小时判断（第二次分化）：
       • 数据源：建仓后12小时内的144根5分钟K线
       • 判定标准：统计下跌>7.5%的K线占比
       • 强势币：下跌K线占比 ≥ 60% → 保持或升级到33%止盈
       • 弱势币：下跌K线占比 < 60% → 降为10%止盈
  
  设计原理（做空策略）：
    • 价格持续下跌 = 强势币 → 给予更高止盈目标（33%）
    • 价格震荡反弹 = 中等币 → 适中止盈目标（21%）
    • 价格大幅反弹 = 弱势币 → 快速止盈离场（10%）

【🛑 止损机制】

  1️⃣ 常规止损（-18%）
     触发条件：价格涨幅 >= 18%（止损价 = 建仓价 × 1.18）
     实际亏损：18% × 3倍杠杆 = 54%
  
  2️⃣ 24小时收盘价涨幅止损
     触发时机：建仓后24-25小时窗口检查一次
     触发条件：24h收盘价涨幅 > 6.3%
     原理：做空策略预期价格下跌，24h后不跌反涨说明判断错误
  
  3️⃣ 最大持仓时间强制平仓
     限制时间：72小时（3天）
     原理：避免长期占用资金，提升资金周转效率

【📊 策略特点】
  ✅ 复利模式：盈利后资金增长，下次建仓金额随之增长
  ✅ 动态止盈：根据币的表现自动调整止盈目标（33% / 21% / 10%）
  ✅ 多重止损：常规止损 + 24h动态止损 + 72h强制平仓
  ✅ 风险控制：3倍杠杆 + 单仓10% + 最多10个并发持仓

【⚠️ 风险提示】
  1. 杠杆风险：3倍杠杆放大收益的同时也放大风险
  2. 单笔止损：最大亏损 18% × 3倍 = 54%
  3. 市场风险：策略基于历史数据回测，实盘表现可能不同

═══════════════════════════════════════════════════════════════════════════════

作者：量化交易助手
版本：v4.0（精简版，移除补仓和观察者模式）
最后更新：2026-02-09
"""

import csv
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import sqlite3
import argparse
import pandas as pd
import calendar  # 用于时间戳转换
from pandas.core.nanops import F
import requests  # 用于API请求
import time  # 用于API请求延迟

# 配置日志
# 配置日志：同时输出到终端和文件
import os
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"sell_surge_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# 创建日志格式
log_format = '%(asctime)s - %(levelname)s - %(message)s'

# 配置根日志器
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 清除已有的处理器（避免重复）
logger.handlers.clear()

# 文件处理器
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(file_handler)

# 终端处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(console_handler)

logging.info(f"📝 日志文件: {log_file}")

# 数据库路径（使用绝对路径，兼容从不同目录运行）
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CRYPTO_DB_PATH = os.path.join(SCRIPT_DIR, "db", "crypto_data.db")

class BuySurgeBacktest:
    """买量暴涨策略回测器"""

    def __init__(self):
        self.crypto_conn = sqlite3.connect(CRYPTO_DB_PATH)
        
        # 🆕 添加：连接顶级交易者数据库（使用绝对路径）
        self.trader_db_path = os.path.join(SCRIPT_DIR, "db", "top_trader_data.db")
        try:
            self.trader_conn = sqlite3.connect(self.trader_db_path)
            logging.info(f"✅ 已连接顶级交易者数据库: {self.trader_db_path}")
        except Exception as e:
            logging.warning(f"⚠️  无法连接顶级交易者数据库: {e}")
            self.trader_conn = None
        
        # 🔥 创建临时信号表（用于严格控制持仓数量）
        self._init_pending_signals_table()
        
        # 🆕 添加：顶级交易者风控参数
        self.enable_trader_filter = True   # 是否启用顶级交易者过滤（默认开启）
        # ============================================================================
        # 📊 核心参数配置区（所有可调参数集中在这里）
        # ============================================================================
        
        # 🎯 持仓与资金管理
        self.max_daily_positions = 10 # 🔧 并发持仓上限（从10提高到15）
        self.max_entries_per_day = 10  # 每天最多建仓数量
        # 回测 pending：是否启用「同一 UTC 自然小时内最多成交 1 笔」（按历史建仓时刻的小时桶统计）。
        # False=允许多个币种在同一小时成交（默认，避免误杀）；True=贴近 ae_server「本扫描小时只开一单」的保守节奏。
        self.enable_hourly_entry_limit = False
        # 每日从 PendingSignals 拉取多少条做「是否触价 / 是否进候选」（按倍数排序后的前 N 条）。
        # 若与 max_entries_per_day 绑死，会出现：信号 100+ 但每天只检查 5～9 条，其余永远排不到队。
        self.max_pending_signals_to_scan_per_day = 500
        self.initial_capital = 10000.0  # 初始资10
        self.leverage = 3.0  # 杠杆倍数（3倍）
        self.position_size_ratio = 0.05 # 单次建仓占资金比例（28%）
        
        # 💰 交易手续费配置
        self.trading_fee_rate = 0.0002  # 交易手续费率 0.02%（统一使用Maker费率：建仓、补仓、平仓）
        self.use_bnb_discount = False  # 是否使用BNB抵扣（开启后享受10%折扣，实际费率0.018%）
        
        # 🔥 信号触发阈值
        self.sell_surge_threshold = 10  # 小时主动卖量比昨日暴涨阈值（0=不限制）
        self.sell_surge_max =14008  # 卖量暴涨倍数上限（inf=不限制）
        
        # 🛡️ 三重风控阈值（建仓前拦截）【已全部禁用】
        self.enable_trader_filter = False  # ❌ 已禁用：顶级交易者风控
        self.min_account_ratio = 0.70  # 最小账户多空比（信号筛选）
        self.max_account_ratio = 4.1  # 最大账户多空比（建仓风控：>4.1拒绝建仓）
        
        # 🔴 基差率（Premium Index）风控开关 【可关可开】
        self.enable_premium_filter = False  # ✅ 已启用：Premium指数风控（建仓时检查）
        self.premium_min_threshold = -0.003  # Premium下限（-0.3%，低于此值拒绝建仓）
        # 原理：负基差过大 = 永续价格 < 现货价格 → 做空需支付资金费率，成本高
        # 示例：基差-2.01% < -0.3% → 拒绝建仓
        # 📊 2026-02-07优化：使用绝对基差值比变动率更准确
        
        self.enable_buy_accel_filter = False  # ❌ 已禁用：买量加速度风控
        # 买量加速度：最后6小时平均买卖比 - 前18小时平均买卖比
        self.buy_accel_danger_ranges = [
            (-0.05, -0.042),  # 危险区间1：-0.046 到 -0.042（买量加速下降）
            (0.118, 0.12),    # 危险区间2：0.118-0.12（买量加速过快，反转风险）
            (0.0117, 0.03),    # 危险区间3
            (0.2, 0.99),      # 危险区间4
        ]
        
        self.enable_intraday_buy_ratio_filter = True  # ❌ 已禁用：当日买量倍数风控（旧版）
        # 当日买量倍数：建仓前12小时，每小时买量相对前一小时的最大比值
        # 📊 根据实际回测数据优化（2026-02-02）：
        #   - 5-7x 表现最佳（止盈率16.7%，止损率20.8%）✅
        #   - 10-15x 表现差（止盈率6.2%，止损率56.2%）❌
        #   - >15x 表现差（止盈率10.0%，止损率50.0%）❌
        self.intraday_buy_ratio_danger_ranges = [
            (4.81, 6.61),  # 危险区间1：4.81-6.61倍
            #(4.89, 5.01),
            (9.45, 11.1),  # 危险区间2：9.45-11.1倍倍（高波动高止损风险）
        ]
        
        # 🆕 连续买量倍数风控（2026-02-03新增，2026-02-03优化）
        self.enable_consecutive_buy_ratio_filter = False   # ✅ 启用：连续买量倍数风控
        # 逻辑：12小时内，如果出现连续2个小时主动买量都>2倍，拒绝建仓（已从3小时降低为2小时）
        # 原理：
        #   - 一次性爆发（诱多）：买量瞬间暴涨后立即衰减 → 做空安全
        #   - 持续爆发（真突破）：买量连续2小时都>2倍 → 价格有持续上涨动能 → 危险！
        # 示例：
        #   - 2.5x, 2.3x → 连续2小时>2x → 拒绝建仓
        #   - 2.5x, 1.5x → 第2小时断档 → 允许建仓
        #   - 1.8x, 1.9x → 都没超过2x → 允许建仓
        self.consecutive_buy_ratio_hours = 3  # 连续小时数（从3降低为2，提高触发概率）
        self.consecutive_buy_ratio_threshold = 2.5  # 倍数阈值
        
        self.enable_buy_sell_ratio_filter =  False  # ✅ 已启用：买卖量比值风控
        # 买卖量比值 = 当日买量倍数 / 当日卖量倍数
        self.buy_sell_ratio_danger_ranges = [
            (0.8, 1.3),  # 危险区间：1.02-1.07（买卖量增长同步，方向不明确）
            #(1.42, 1.53),  # 危险区间：1.02-1.07（买卖量增长同步，方向不明确）
            
            #(0.37, 0.409),  # 危险区间：1.02-1.07（买卖量增长同步，方向不明确）       
        ]
        
        self.enable_oi_ratio_filter = False  # ❌ 已禁用：未平仓量比率风控
        # 未平仓量比率 = 信号时未平仓量 / 建仓时未平仓量
        self.oi_ratio_danger_ranges = [
            (0.72, 0.93),  # 危险区间：0.72-0.93（未平仓量下降，资金流出）
        ]
        
        self.enable_intraday_sell_ratio_filter = False  # ❌ 已禁用：当日卖量倍数风控
        # 当日卖量倍数：建仓前12小时，每小时卖量相对前一小时的最大比值
        # 📊 根据实际回测数据优化（2026-02-02）：
        #   - 3-5x 表现最佳（止盈率17.6%，止损率14.7%）✅
        #   - 10-15x 表现差（止盈率0.0%，止损率58.3%）❌
        #   - >15x 表现差（止盈率10.0%，止损率50.0%）❌
        self.intraday_sell_ratio_danger_ranges = [
            (3.89, 4.11),  
            (9, 999.0),  # 危险区间：>=10倍（卖量暴涨，市场恐慌）
        ]
        
        self.enable_account_ratio_change_filter = False  # ❌ 已禁用：多空比变化率风控
        
        # 🆕 建仓涨幅风控（2026-02-04新增）
        self.enable_entry_gain_filter = False  # ✅ 启用：建仓涨幅风控
        self.entry_gain_min = -3  # 建仓涨幅下限（%）
        self.entry_gain_max = 1.6  # 建仓涨幅上限（%）
        # 原理：建仓时涨幅在[-2.21%, 2.21%]区间内的交易表现最佳
        #   - 涨幅过高(>2.21%)：追高风险，容易触发止损
        #   - 涨幅过低(<-2.21%)：可能已经下跌过多，反弹空间有限
        
        # 🆕 24h基差变动风控（2026-02-04新增，2026-02-07禁用）
        self.enable_premium_change_filter = False  # ❌ 已禁用：24h基差变动风控
        self.premium_24h_drop_threshold = -100.0  # 24h基差下跌阈值（%）
        # ⚠️ 禁用原因（2026-02-07）：
        #   - 变动率对小基差值过于敏感：0.04% → -0.03%会算成-180%"暴跌"
        #   - 直接看建仓时基差绝对值更准确（使用enable_premium_filter）
        #   - 该风控在基差数据修复前从未真正生效，证明其必要性存疑
        # 原理（仅供参考）：基差 = 合约价格 - 现货价格
        
        # 🆕 CVD风控：禁止CVD创新低（2026-02-05新增）⭐⭐⭐⭐⭐
        self.enable_cvd_new_low_filter = False  # ✅ 启用：CVD创新低风控（强烈推荐）
        # 原理：CVD创新低 = 恐慌抛售尾声 = 容易被抄底资金反杀
        # 效果：止损过滤率88.9%，止盈保留率53.3%，单笔期望提升72%
        # 逻辑：
        #   - 计算建仓前24小时的CVD（累积成交量差）
        #   - CVD = Σ(主动买量 - 主动卖量)
        #   - 如果建仓时CVD <= 24h最低值 → 拒绝建仓
        # 数据验证：
        #   - 止盈交易中仅13.3%出现CVD创新低
        #   - 止损交易中高达55.6%出现CVD创新低
        #   - CVD创新低是最强的止损预警信号！
        self.cvd_lookback_hours = 24  # CVD回溯窗口（小时）
        
        # 多空比变化率 = 建仓时账户多空比 / 信号时账户多空比
        self.account_ratio_change_danger_ranges = [
            (0.98, 1.02),  # 危险区间：0.87-0.94（多空比下降，看空情绪增强）8321
            (0.84, 0.87),  
        ]
        
        # ⏰ 止盈止损与超时
        self.stop_loss_pct = -18.0  # 止损比例（-18%，注意：price_pct计算时乘以100，所以这里用-18而不是-0.18）
        self.max_hold_hours = 72  # 最大持仓小时数（72小时=3天）
        self.wait_timeout_hours = 120  # 信号等待超时时间（小时）
        
        # 🔴 信号延迟建仓时间 【可调参数】
        self.signal_delay_minutes = 0  # 发现信号后延迟多久才能建仓（分钟）
        # 说明：
        #   - 0分钟（立即建仓）：信号小时结束后立即检查建仓，价格=信号收盘价≈下小时开盘价
        #   - 5分钟：延迟5分钟建仓，价格可能偏离
        #   - 60分钟（1小时）：等小时K线收盘，数据最稳定，但价格可能偏离建仓区间
        #   - 推荐：0分钟（立即建仓，价格偏离最小，提高建仓成功率）
        
        # 📉 等待反弹策略（做空建仓）
        # 🔥 熊市适配：立即建仓，不等待反弹（2021年熊市中很难等到反弹）
        self.wait_drop_pct_config = [
            (9999, 0.00),  # 所有情况：立即建仓，不等待反弹
        ]
        
        # 🔴 其他风控（已禁用）
        self.account_ratio_stop_threshold = -999  # 账户多空比下降止损阈值（已禁用）
        self.add_position_size_ratio = 0.1  # 补仓占资金比例（补仓已禁用）
        
        # 💰 资金管理阈值
        self.min_capital_ratio = 0.2  # 爆仓保护：资金低于初始资金20%时停止交易
        self.max_position_value_ratio = 0.3  # 单笔建仓上限：不超过总资金30%
        self.capital_buffer_ratio = 0.95  # 资金使用率上限：保留5%缓冲
        
        # 📅 风控启用日期
        self.wind_control_start_date = datetime(2025, 12, 12, tzinfo=timezone.utc)  # 多空比等风控的启用日期
        
        # 📊 动态止盈数据要求
        self.min_klines_for_dynamic_tp = 20  # 动态止盈至少需要的K线数量（20根=2小时，降低数据要求）
        
        # 🚨 2小时提前止损风控（可选）
        self.enable_2h_early_stop = False  # ❌ 已禁用：回测显示导致收益暴跌-249%
        self.early_stop_2h_threshold = 0.036  # 2h收盘涨幅阈值（1.85%）
        # ⚠️ 禁用原因（2026-02-03回测验证）：
        # - 启用后最终收益：$22,026 (+120%)
        # - 未启用收益：$46,922 (+369%)
        # - 损失：-$24,896 (-249%) ❌
        # - 胜率下降：30.2% vs 62.4% (-32%)
        # - 问题：在做空策略中，价格短期上涨1.85%很正常，过早止损会错过后续下跌
        
        # 🚨 12小时提前止损风控（可选）
        self.enable_12h_early_stop = True    # ✅ 保留启用：单独测试12h动态止损效果
        self.early_stop_12h_threshold = 0.037  # 12h收盘涨幅阈值（3.5%）
        # 💡 测试目的：12小时比2小时更长，可能更准确判断趋势反转
        # 如果12h后价格涨了3.5%，说明做空逻辑可能失效，及时止损避免更大损失
        
        # 🚨 24小时收盘价涨幅动态平仓（2026-02-04新增 - 基于实盘分析优化）
        self.enable_max_gain_24h_exit = False  # ❌ 已屏蔽：24小时收盘价涨幅动态平仓（2026-02-05）
        self.max_gain_24h_threshold = 0.063  # 24h收盘价涨幅阈值（6.3%）
        # 📊 统计依据（2026-02-05深入分析结果）：
        #   - 分析183笔历史交易，发现8笔漏网交易（24h涨幅>6.3%但未触发止损）
        #   - 这8笔交易总亏损-35.27%，如在24h时止损可改善35.07%
        #   - 典型案例：PEOPLEUSDT（24h涨12.35%，最终亏-16.65%）、TSTUSDT（24h涨15.56%，最终亏-18%）
        #   - 结论：6.3%是更合理的风控阈值，可提前识别趋势反转
        # 💡 原理：
        #   - 做空策略最怕持续上涨
        #   - 建仓后24h收盘价涨幅 > 6.3% → 说明币价已破位上涨，继续持有风险极高
        #   - 及时止损，避免深度亏损（-18%）
        # ⚠️ 动态平仓 vs 建仓风控：
        #   - 建仓风控：拦截信号，减少建仓次数
        #   - 动态平仓：建仓后监控，及时止损出场
        
        # ============================================================================
        
        # ============================================
        # 🎯 BTC 相关风控
        # ============================================
        # 【建仓】昨日 BTC 日K 收 > 开（阳线）→ 当日不建新仓（UTC 日历日，与库表一致）
        self.enable_btc_yesterday_yang_no_new_entry = True
        
        # 【持仓】昨日 BTC 日K 收 > 开 → 当日 UTC 00:00:00 起按各币「当日」日K open 全部平仓（方案A，早于小时线止盈止损）
        # 若为 False，一刀切逻辑完全不执行，回测与未加该功能时一致
        self.enable_btc_yesterday_yang_flatten_at_open = True
        
        # 【动态止盈】24h 小时线“企稳”判定（回测效果一般时可关，仅影响止盈档位，不拦截建仓）
        self.enable_btc_stability_check = False  # 是否启用BTC企稳判断（True=启用，False=禁用）
        
        # 【企稳判断标准】（优化版：滚动24小时判断）
        # 从当前时间往前取24根小时K线
        # 基准价 = 第24根K线（24小时前）的close价
        # 统计这24根K线中，有多少根close > 基准价 + threshold → 判定为企稳
        self.btc_stability_up_ratio = 0.6   # 上涨小时占比阈值（60%）
        self.btc_stability_threshold = 0.02  # 相对24h前价格的涨幅阈值（1.5%）
        
        # 【BTC企稳时的止盈目标】（快速止盈）
        self.btc_stable_strong_tp = 10.0   # BTC企稳时-强势币止盈（11%）
        self.btc_stable_medium_tp = 8.0    # BTC企稳时-中等币止盈（8%）
        self.btc_stable_weak_tp = 5.0      # BTC企稳时-弱势币止盈（5%）
        
        # 📊 统计数据支持（2025-11-01至2026-02-15）：
        #   - BTC企稳期占比：3.8%（105天中仅4天）
        #   - 企稳期判断准确率：100%（4次全部实际上涨）
        #   - 企稳期平均涨幅：+5.03%
        #   - 原理：BTC企稳时山寨币跟随反弹，做空难以达到高止盈，应快速止盈离场
        # ============================================
        
        # ============================================
        # 🎯 动态止盈参数（三档止盈策略）
        # ============================================
        # 【三档止盈阈值】（正常市场下使用）
        # 基于实际回测优化（2026-02-02）
        # ⚠️ 注意：这些值是小数形式（0.33 = 33%），与price_pct计算（乘以100）数量级不匹配
        # ✅ 修复：统一使用百分比数值（33 = 33%），与price_pct计算保持一致
        self.strong_coin_tp_pct = 33.0    # 强势币止盈：33%（0-2小时内固定使用，2小时后动态判定）
        self.medium_coin_tp_pct = 21.0    # 中等币止盈：21%（2小时后动态判定）
        self.weak_coin_tp_pct = 11.0      # 弱势币止盈：10%（12小时后动态判定）
        
        # 【判定条件】
        # 第一阶段：2小时判断（建仓后2小时）
        self.dynamic_tp_2h_ratio = 0.6            # 2小时强势K线占比阈值：35%（从60%降低）
        self.dynamic_tp_2h_growth_threshold = 0.055 # 单根5分钟K线跌幅阈值：2%（从2.5%降低）
        
        # 第二阶段：12小时判断（建仓后12小时）
        self.dynamic_tp_12h_ratio = 0.6                # 12小时强势K线占比阈值：60%
        self.dynamic_tp_12h_growth_threshold = 0.075  # 12小时跌幅阈值：3.5%（做空策略：单根5分钟K线跌幅）
        self.dynamic_tp_lookback_minutes = 720         # 12小时窗口（720分钟）
        
        # 【判定逻辑（做空策略）】
        # 强势币：价格持续下跌（下跌K线≥60% & 12h涨幅<5.5%）→ 止盈33%
        # 中等币：价格小幅反弹（上涨K线≥60%）→ 止盈21%
        # 弱势币：价格大幅反弹（下跌K线≥60% 但 12h涨幅≥5.5%）→ 止盈9%
        # ============================================
        
        # 兼容旧参数（保持向后兼容）
        self.take_profit_pct = self.weak_coin_tp_pct  # 默认使用弱势币止盈
        self.dynamic_tp_close_up_pct = self.dynamic_tp_12h_growth_threshold
        self.dynamic_tp_ratio = self.dynamic_tp_2h_ratio
        self.dynamic_tp_min_5m_bars = 8
        
        # 备用参数（暂不使用）
        self.dynamic_tp_boost_pct = 0.09
        self.dynamic_tp_boost_config = [
            (2, 0.18),     # 2-3倍：19.5%总止盈
            (6, 0.18),     # 3-6倍：16.5%
            (10, 0.18),    # 6-10倍：13.5%
            (9999, 0.18),  # 10倍以上
        ]
        
        # ⚠️ 量化回测中不使用K线缓存，避免数据不一致
        # 只缓存交易对列表（回测期间不会变化）
        self._all_symbols_cache = None  # 交易对列表缓存
        
        # 🔴 补仓设置（已禁用）
        self.enable_add_position = False  # 禁用补仓（分析显示补仓导致严重亏损）
        self.add_position_trigger_pct = -0.18 # 补仓触发比例（跌15%触发）
        self.use_virtual_add_position = False  # 真实补仓模式（不使用虚拟补仓）
        self.virtual_add_compensation_multiplier = 1.0  # 禁用补偿倍数
        self.pending_virtual_compensations = 0  # 待补偿的虚拟补仓次数
        
        # 🔴 24小时弱势平仓（已禁用）
        self.enable_weak_24h_exit = False  # 禁用24小时弱势平仓

        
        # 🆕 信号记录（用于输出"发现信号但未成交"的反馈表）
        # 每条记录：信号时间、目标价、是否成交、未成交原因等
        self.signal_records = []

        # 交易记录
        self.capital = self.initial_capital
        self.positions = []  # 当前持仓
        self.trade_records = []  # 交易记录
        self.daily_capital = []  # 每日资金记录

    def __del__(self):
        """析构函数，确保数据库连接关闭"""
        try:
            if hasattr(self, 'crypto_conn'):
                self.crypto_conn.close()
            # 🆕 添加：关闭顶级交易者数据库连接
            if hasattr(self, 'trader_conn') and self.trader_conn:
                self.trader_conn.close()
        except:
            pass

    # ============================================================================
    # 🔥 临时信号表管理（用于严格控制持仓数量）
    # ============================================================================
    
    def _init_pending_signals_table(self):
        """初始化待建仓信号临时表"""
        try:
            cursor = self.crypto_conn.cursor()
            
            # 创建临时信号表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS PendingSignals (
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    signal_date TEXT,
                    signal_datetime TEXT,
                    earliest_entry_datetime TEXT,
                    signal_price REAL,
                    buy_surge_ratio REAL,
                    target_drop_pct REAL,
                    timeout_datetime TEXT,
                    signal_account_ratio REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引优化查询性能
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_buy_surge_ratio 
                ON PendingSignals(buy_surge_ratio DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeout 
                ON PendingSignals(timeout_datetime)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_symbol 
                ON PendingSignals(symbol)
            """)
            
            # 清空表（回测开始时清理旧数据）
            cursor.execute("DELETE FROM PendingSignals")
            
            self.crypto_conn.commit()
            logging.info("✅ 待建仓信号临时表初始化完成")
        except Exception as e:
            logging.error(f"❌ 初始化信号表失败: {e}")
            raise
    
    def _add_pending_signal(self, signal_data: dict):
        """添加待建仓信号到表"""
        try:
            cursor = self.crypto_conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO PendingSignals (
                    signal_id, symbol, signal_date, signal_datetime,
                    earliest_entry_datetime, signal_price, buy_surge_ratio,
                    target_drop_pct, timeout_datetime, signal_account_ratio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_data['signal_id'],
                signal_data['symbol'],
                signal_data['signal_date'],
                signal_data['signal_datetime'],
                signal_data['earliest_entry_datetime'],
                signal_data['signal_price'],
                signal_data['buy_surge_ratio'],
                signal_data['target_drop_pct'],
                signal_data['timeout_datetime'],
                signal_data.get('signal_account_ratio')
            ))
            self.crypto_conn.commit()
        except Exception as e:
            logging.error(f"❌ 添加信号失败: {e}")
    
    def _get_pending_signals(self, current_datetime: str, limit: int = None) -> list:
        """
        获取待建仓信号（按买量倍数降序排序）
        
        Args:
            current_datetime: 当前时间（用于过滤超时信号）
            limit: 最多返回多少个信号（用于严格控制建仓数量）
        
        Returns:
            信号列表（字典格式）
        """
        try:
            cursor = self.crypto_conn.cursor()
            
            # 🔥 关键：按卖量倍数降序排序，优先处理高倍数信号（信号质量优先）
            if limit:
                cursor.execute("""
                    SELECT * FROM PendingSignals
                    WHERE timeout_datetime > ?
                    ORDER BY buy_surge_ratio DESC
                    LIMIT ?
                """, (current_datetime, limit))
            else:
                cursor.execute("""
                    SELECT * FROM PendingSignals
                    WHERE timeout_datetime > ?
                    ORDER BY buy_surge_ratio DESC
                """, (current_datetime,))
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # 转换为字典列表
            signals = []
            for row in rows:
                signal = dict(zip(columns, row))
                signals.append(signal)
            
            return signals
        except Exception as e:
            logging.error(f"❌ 查询信号失败: {e}")
            return []
    
    def _remove_pending_signal(self, signal_id: str):
        """删除指定信号"""
        try:
            cursor = self.crypto_conn.cursor()
            cursor.execute("DELETE FROM PendingSignals WHERE signal_id = ?", (signal_id,))
            self.crypto_conn.commit()
        except Exception as e:
            logging.error(f"❌ 删除信号失败: {e}")
    
    def _remove_pending_signal_by_symbol(self, symbol: str):
        """删除指定交易对的所有信号"""
        try:
            cursor = self.crypto_conn.cursor()
            cursor.execute("DELETE FROM PendingSignals WHERE symbol = ?", (symbol,))
            self.crypto_conn.commit()
        except Exception as e:
            logging.error(f"❌ 删除信号失败: {e}")
    
    def _count_pending_signals(self) -> int:
        """统计待建仓信号数量"""
        try:
            cursor = self.crypto_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM PendingSignals")
            return cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"❌ 统计信号数量失败: {e}")
            return 0

    def _sweep_pending_timeouts(self, date_str: str) -> None:
        """对 PendingSignals 全表扫描：若当日结束时刻已超过 timeout，则移除并标记 timeout。
        修复：原先超时逻辑只在「LIMIT batch 取出的 topN 条」里执行，排序靠后的信号可能整段回测都进不了 batch，
        既不会成交也不会被判超时 → signal_records 一直 pending → 反馈表误报「未触发目标价/未成交」。"""
        try:
            current_date_end = datetime.strptime(date_str, '%Y-%m-%d').replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
            cursor = self.crypto_conn.cursor()
            cursor.execute(
                "SELECT signal_id, symbol, signal_date, buy_surge_ratio, timeout_datetime FROM PendingSignals"
            )
            rows = cursor.fetchall()
            removed = 0
            for row in rows:
                signal_id, symbol, signal_date, buy_surge_ratio, timeout_datetime_str = (
                    row[0], row[1], row[2], row[3], row[4]
                )
                if not timeout_datetime_str:
                    continue
                try:
                    timeout_dt = datetime.strptime(
                        str(timeout_datetime_str).strip(), '%Y-%m-%d %H:%M:%S'
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if current_date_end > timeout_dt:
                    br = float(buy_surge_ratio) if buy_surge_ratio is not None else 0.0
                    logging.info(
                        f"⏰ [全表超时] {symbol} 卖量{br:.1f}倍 signal_id={signal_id}"
                    )
                    self._update_signal_record(
                        symbol,
                        signal_date,
                        status='timeout',
                        note='超时未成交',
                        signal_id=signal_id,
                    )
                    cursor.execute(
                        "DELETE FROM PendingSignals WHERE signal_id = ?", (signal_id,)
                    )
                    removed += 1
            if removed:
                self.crypto_conn.commit()
                logging.info(f"📤 {date_str} 全表清除超时 pending: {removed} 条")
        except Exception as e:
            logging.error(f"❌ 全表扫描 pending 超时失败: {e}")

    # ============================================================================
    # 🔧 时间戳转换工具函数（用于 trade_date → open_time 迁移）
    # ============================================================================
    
    @staticmethod
    def date_str_to_timestamp_range(date_str: str) -> Tuple[int, int]:
        """
        将日期字符串转换为时间戳范围（毫秒级）
        
        Args:
            date_str: 日期字符串，格式如 '2025-01-01' 或 '2025-01-01 08:00:00'
        
        Returns:
            (start_ms, end_ms): 时间范围的开始和结束时间戳（毫秒）
        
        示例：
            '2025-01-01' -> (1735689600000, 1735776000000)  # 该日的 00:00 到次日 00:00
            '2025-01-01 08:00:00' -> (1735718400000, 1735722000000)  # 8:00-9:00
        """
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)  # 🔧 指定UTC
            is_hour = True
        except ValueError:
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)  # 🔧 指定UTC
                is_hour = False
            except ValueError:
                # 尝试其他格式
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                is_hour = ':' in date_str
        
        # 确保时区为 UTC（现在应该已经有了）
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        start_ms = int(dt.timestamp() * 1000)
        
        if is_hour:
            # 小时级别：返回该小时的范围
            end_ms = start_ms + 3600 * 1000
        else:
            # 日期级别：返回该天的范围
            end_ms = start_ms + 86400 * 1000
        
        return start_ms, end_ms
    
    @staticmethod
    def timestamp_to_datetime(timestamp_ms: int) -> datetime:
        """
        将毫秒级时间戳转换为 datetime 对象（UTC）
        
        Args:
            timestamp_ms: 毫秒级时间戳
        
        Returns:
            datetime 对象（UTC 时区）
        """
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    
    @staticmethod
    def datetime_to_timestamp(dt: datetime) -> int:
        """
        将 datetime 对象转换为毫秒级时间戳
        
        Args:
            dt: datetime 对象
        
        Returns:
            毫秒级时间戳
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def get_wait_drop_pct(self, sell_surge_ratio: float) -> float:
        """卖量暴涨不等待，直接建仓
        
        Args:
            sell_surge_ratio: 卖量暴涨倍数
        
        Returns:
            等待百分比（0.0表示不等待）
        """
        # 卖量暴涨后不等待，直接做空
        return 0.0
    
    def get_5m_close_price(self, symbol: str, target_datetime: datetime) -> Optional[float]:
        """获取指定时刻的5分钟K线收盘价作为实际建仓价
        
        Args:
            symbol: 交易对
            target_datetime: 目标时间（精确到分钟）
        
        Returns:
            5分钟K线收盘价，如果没有数据则返回None
        """
        try:
            cursor = self.crypto_conn.cursor()
            table_name = f'Kline5m_{symbol}'
            
            # 将目标时间转换为毫秒级时间戳
            target_ms = self.datetime_to_timestamp(target_datetime)
            
            # 查询该时刻的5分钟K线收盘价
            cursor.execute(f'''
                SELECT close
                FROM "{table_name}"
                WHERE open_time = ?
                LIMIT 1
            ''', (target_ms,))
            
            row = cursor.fetchone()
            if row and row[0]:
                return float(row[0])
            
            # 如果精确时刻没有数据，查询最近的一根K线
            cursor.execute(f'''
                SELECT close, open_time
                FROM "{table_name}"
                WHERE open_time <= ?
                ORDER BY open_time DESC
                LIMIT 1
            ''', (target_ms,))
            
            row = cursor.fetchone()
            if row and row[0]:
                nearest_dt = self.timestamp_to_datetime(row[1])
                logging.debug(f"📊 {symbol} 在{target_datetime}无数据，使用最近K线{nearest_dt}的收盘价{row[0]}")
                return float(row[0])
            
            return None
            
        except Exception as e:
            logging.warning(f"⚠️ 查询{symbol}的5分钟K线失败: {e}")
            return None
    
    def get_5min_kline_data(self, symbol: str) -> pd.DataFrame:
        """获取5分钟K线数据"""
        try:
            cursor = self.crypto_conn.cursor()
            table_name = f'Kline5m_{symbol}'
            
            cursor.execute(f'''
                SELECT open_time, open, high, low, close, volume
                FROM \"{table_name}\"
                ORDER BY open_time ASC
            ''')
            
            data = cursor.fetchall()
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])
            return df
            
        except Exception as e:
            logging.debug(f"获取5分钟K线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def calculate_trading_fee(self, trading_value: float) -> float:
        """
        计算交易手续费（统一使用Maker费率）
        
        Args:
            trading_value: 成交金额（position_value × leverage 或 exit_price × position_size）
        
        Returns:
            float: 手续费金额（USDT）
        
        说明：
        - 币安合约VIP 0级别 Maker费率：0.02%
        - 建仓、补仓、平仓均使用限价单（Maker）
        - 如果开启BNB抵扣，可享受10%折扣（实际费率0.018%）
        """
        fee = trading_value * self.trading_fee_rate
        if self.use_bnb_discount:
            fee *= 0.9  # BNB抵扣：10%折扣
        return round(fee, 6)  # 保留6位小数
    
    def _find_actual_entry_5m_time(self, symbol: str, entry_datetime_str: str, entry_price: float) -> Optional[str]:
        """
        查找实际建仓的5分钟K线时刻（做空策略：限价单触及的时刻）
        
        做空策略：当价格从下方上涨触及限价单时建仓（high >= entry_price）
        
        Args:
            symbol: 交易对
            entry_datetime_str: 建仓时间字符串，格式 'YYYY-MM-DD HH:MM'
            entry_price: 建仓价格（限价单价格）
        
        Returns:
            实际建仓的5分钟时刻字符串，格式 'YYYY-MM-DD HH:MM'，如果找不到则返回None
        """
        try:
            # 解析建仓时间（添加 UTC 时区）
            entry_datetime = datetime.strptime(entry_datetime_str, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            entry_hour_start = entry_datetime.replace(minute=0, second=0, microsecond=0)
            entry_hour_end = entry_hour_start + timedelta(hours=1)
            
            # 获取5分钟K线数据
            kline_5m_df = self.get_5min_kline_data(symbol)
            if kline_5m_df.empty:
                logging.debug(f"❌ {symbol} 无5分钟K线数据")
                return None
            
            # 将 open_time 转换为 datetime
            kline_5m_df['open_datetime'] = pd.to_datetime(kline_5m_df['open_time'], unit='ms', utc=True)
            
            # 筛选建仓小时内的5分钟K线
            mask_entry_hour = (kline_5m_df['open_datetime'] >= entry_hour_start) & \
                             (kline_5m_df['open_datetime'] < entry_hour_end)
            entry_hour_5m_data = kline_5m_df[mask_entry_hour].copy()
            
            if entry_hour_5m_data.empty:
                logging.debug(f"❌ {symbol} {entry_hour_start}~{entry_hour_end} 无5分钟K线")
                return None
            
            logging.debug(f"🔍 {symbol} 查找限价单{entry_price:.6f}触及时刻，共{len(entry_hour_5m_data)}根5分钟K线")
            
            # 🔥 做空策略：找到第一根最高价触及或超过限价单的5分钟K线
            # 价格上涨触及限价单 → 建仓做空
            for idx, row in entry_hour_5m_data.iterrows():
                high_price = float(row['high'])
                if high_price >= entry_price:
                    # 找到了！返回5分钟时刻字符串
                    # 🔧 手动格式化确保UTC（strftime可能受系统时区影响）
                    dt = row['open_datetime']
                    actual_time_str = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}"
                    logging.info(f"✅ {symbol} 建仓5分钟时刻: {actual_time_str} (high={high_price:.6f} >= 限价单{entry_price:.6f})")
                    return actual_time_str
            
            # 如果没有找到，说明限价单价格设置有问题
            max_high = entry_hour_5m_data['high'].max()
            logging.warning(f"⚠️ {symbol} 未找到触及点！限价单={entry_price:.6f}, 小时最高价={max_high:.6f}")
            return None
            
        except Exception as e:
            logging.error(f"❌ 查找{symbol}实际建仓5分钟时刻失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_dynamic_tp_boost_pct(self, buy_surge_ratio) -> float:
        """根据买量暴涨倍数获取动态止盈加成幅度（仅在"强势币"触发时使用）"""
        if buy_surge_ratio is None:
            return float(self.dynamic_tp_boost_pct)
        try:
            r = float(buy_surge_ratio)
        except Exception:
            return float(self.dynamic_tp_boost_pct)

        for max_ratio, boost_pct in getattr(self, 'dynamic_tp_boost_config', []) or []:
            if r < max_ratio:
                return float(boost_pct)
        return float(self.dynamic_tp_boost_pct)
    
    def get_top_trader_account_ratio(self, symbol: str, timestamp: datetime) -> Optional[Dict]:
        """获取顶级交易者账户多空比数据
        
        Args:
            symbol: 交易对
            timestamp: 查询时间
        
        Returns:
            包含账户多空比数据的字典，如果没有数据则返回None
        """
        if not hasattr(self, 'trader_conn') or self.trader_conn is None:
            return None
        
        try:
            # 转换为毫秒时间戳
            target_ts = int(timestamp.timestamp() * 1000)
            
            # 🔧 根据数据粒度演进动态调整查询容差
            # 2025-12-12 ~ 2026-01-03: 天级数据（每天08:00）
            # 2026-01-04 ~ 2026-01-09: 小时级数据（每小时）
            # 2026-01-10 至今: 5分钟级数据（每5分钟）
            cutoff_hour_data = int(datetime(2026, 1, 4, 0, 0, 0).timestamp() * 1000)  # 2026-01-04 00:00
            cutoff_minute_data = int(datetime(2026, 1, 10, 0, 0, 0).timestamp() * 1000)  # 2026-01-10 00:00
            
            if target_ts < cutoff_hour_data:
                # 天级数据阶段：使用24小时容差
                time_tolerance = 24 * 3600 * 1000
            elif target_ts < cutoff_minute_data:
                # 小时级数据阶段：使用2小时容差
                time_tolerance = 2 * 3600 * 1000
            else:
                # 5分钟级数据阶段：使用15分钟容差
                time_tolerance = 15 * 60 * 1000
            
            start_ts = target_ts - time_tolerance
            end_ts = target_ts + time_tolerance
            
            cursor = self.trader_conn.cursor()
            query = """
                SELECT timestamp, long_short_ratio, long_account, short_account
                FROM top_account_ratio
                WHERE symbol = ? AND timestamp BETWEEN ? AND ?
                ORDER BY ABS(timestamp - ?)
                LIMIT 1
            """
            
            cursor.execute(query, (symbol, start_ts, end_ts, target_ts))
            row = cursor.fetchone()
            
            if row:
                return {
                    'timestamp': row[0],
                    'long_short_ratio': row[1],
                    'long_account': row[2],
                    'short_account': row[3],
                    'datetime': datetime.fromtimestamp(row[0] / 1000)
                }
            
            return None
            
        except Exception as e:
            logging.debug(f"获取顶级交易者数据失败 {symbol}: {e}")
            return None

    def check_trader_signal_filter(self, symbol: str, signal_datetime: datetime) -> tuple:
        """检查信号是否通过顶级交易者数据筛选
        
        Args:
            symbol: 交易对
            signal_datetime: 信号时间
        
        Returns:
            (是否通过, 账户多空比值, 过滤原因)
            
        注意：
            即使 enable_trader_filter=False，也会查询并返回 account_ratio 用于记录分析
        """
        try:
            # 🔥 2025-12-12之前没有顶级交易者数据，直接放行
            if signal_datetime < self.wind_control_start_date:
                logging.debug(f"⏭️  {symbol} 信号时间{signal_datetime.strftime('%Y-%m-%d')}在顶级交易者数据可用日期之前，跳过风控")
                return True, None, ""
            
            # 🆕 始终查询账户多空比（用于分析，不管是否启用过滤）
            trader_data = self.get_top_trader_account_ratio(symbol, signal_datetime)
            
            if trader_data is None:
                # 没有数据时，放行但返回None
                logging.debug(f"⚠️  {symbol} 无顶级交易者数据")
                return True, None, ""
            
            account_ratio = trader_data['long_short_ratio']
            
            # 🔧 只有启用过滤时才进行风控检查
            if self.enable_trader_filter and hasattr(self, 'min_account_ratio'):
                if account_ratio < self.min_account_ratio:
                    return False, account_ratio, f"账户多空比{account_ratio:.4f} < {self.min_account_ratio}"
            
            return True, account_ratio, ""
            
        except Exception as e:
            logging.error(f"检查顶级交易者过滤失败 {symbol}: {e}")
            # 🆕 优化：出错时也放行，避免因技术问题错失机会
            return True, None, f"检查异常，放行"

    def check_trader_stop_loss(self, position: Dict, current_datetime: datetime) -> tuple:
        """检查是否因顶级交易者数据触发止损
        
        Args:
            position: 持仓信息
            current_datetime: 当前时间
        
        Returns:
            (是否触发止损, 原因说明)
        """
        # 🔴 已禁用：不使用顶级交易者数据止损（避免查询数据库）
        return False, ""
        
        # 🔥 2025-12-12之前没有顶级交易者数据，直接返回
        if current_datetime < self.wind_control_start_date:
            return False, ""
        
        if not self.enable_trader_filter:
            return False, ""
        
        try:
            symbol = position['symbol']
            
            # 获取建仓时的账户多空比（如果之前保存了）
            entry_account_ratio = position.get('entry_account_ratio')
            if entry_account_ratio is None:
                # 如果建仓时没有保存，尝试获取建仓时刻的数据
                entry_datetime = position.get('entry_datetime')
                if entry_datetime:
                    entry_trader_data = self.get_top_trader_account_ratio(symbol, entry_datetime)
                    if entry_trader_data:
                        entry_account_ratio = entry_trader_data['long_short_ratio']
                        position['entry_account_ratio'] = entry_account_ratio
            
            # 获取当前的账户多空比
            current_trader_data = self.get_top_trader_account_ratio(symbol, current_datetime)
            
            if current_trader_data is None or entry_account_ratio is None:
                # 数据不足，无法判断
                return False, ""
            
            current_account_ratio = current_trader_data['long_short_ratio']
            
            # 计算账户多空比变化
            ratio_change = current_account_ratio - entry_account_ratio
            
            # 保存当前值供后续分析
            position['current_account_ratio'] = current_account_ratio
            position['account_ratio_change'] = ratio_change
            
            # 判断是否触发止损
            if ratio_change <= self.account_ratio_stop_threshold:
                reason = (f"账户多空比从{entry_account_ratio:.4f}下降到{current_account_ratio:.4f}，"
                         f"变化{ratio_change:.4f} <= {self.account_ratio_stop_threshold}")
                return True, reason
            
            return False, ""
            
        except Exception as e:
            logging.error(f"检查顶级交易者止损失败 {symbol}: {e}")
            return False, ""
    
    def check_signal_surge(self, symbol: str, signal_date: str, signal_close: float) -> tuple:
        """检查信号触发前1小时是否暴涨
        
        Args:
            symbol: 交易对
            signal_date: 信号日期
            signal_close: 信号日收盘价
        
        Returns:
            (是否通过检查, 涨幅百分比)
        """
        try:
            # 获取信号日的时间戳
            signal_dt = datetime.strptime(signal_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            signal_ts = int(signal_dt.timestamp() * 1000)
            
            # 获取信号日之前的最后一个小时K线
            table_name = f'HourlyKline_{symbol}'
            cursor = self.crypto_conn.cursor()
            
            query = f"""
                SELECT close
                FROM "{table_name}"
                WHERE open_time < {signal_ts}
                ORDER BY open_time DESC
                LIMIT 1
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            if not result:
                # 如果没有小时数据，默认通过检查
                return True, 0.0
            
            prev_1h_close = result[0]
            
            # 计算1小时内的涨幅
            surge_pct = ((signal_close - prev_1h_close) / prev_1h_close * 100)
            
            # 如果1小时内涨幅<5%，拒绝信号（涨幅太低）
            if surge_pct < 5.0:
                return False, surge_pct
            
            # 如果1小时内暴涨超过48.5%，拒绝信号（追高风险）
            if surge_pct > 48.5:
                return False, surge_pct
            
            return True, surge_pct
            
        except Exception as e:
            logging.debug(f"检查信号暴涨失败 {symbol}: {e}")
            # 出错时默认通过检查
            return True, 0.0
    
    def calculate_dynamic_take_profit(
        self,
        position: Dict,
        hourly_df: pd.DataFrame,
        entry_datetime: datetime,
        current_datetime: datetime,
    ) -> float:
        """🔧 计算动态止盈阈值（弱势币梯度降低）
        
        双重判断机制（弱势币梯度降低止盈）：
        0. 【新增】BTC企稳判断：如果BTC企稳/反弹 → 快速止盈（11%/8%/5%）
        1. 2小时判断：2小时内<60%的5分钟K线涨幅>1.5% → 降低止盈到20%
        2. 12小时判断：12小时涨幅<2.5% → 降低止盈到11%
        
        梯度下调逻辑：
        - 强势币（2h强 & 12h强）：保持30%
        - 中等币（2h弱 & 12h强）：降到20%
        - 弱势币（12h弱）：降到11%
        
        Args:
            position: 持仓信息
            hourly_df: 小时K线数据
            entry_datetime: 建仓时间（完整的datetime对象，包含小时）
            current_datetime: 当前回测推进到的时间点（避免用未来数据做"强势判定"）
        
        Returns:
            动态止盈阈值（0.30=30%, 0.20=20%, 0.11=11%）
        """
        symbol = position.get('symbol', 'UNKNOWN')
        logging.debug(f"🎯 开始计算 {symbol} 动态止盈，entry_datetime={entry_datetime}, current_datetime={current_datetime}")
        
        # ============================================================================
        # 🆕 第0步：优先检查BTC是否企稳（最高优先级）
        # ============================================================================
        try:
            # 确保current_datetime是datetime对象
            if isinstance(current_datetime, str):
                try:
                    current_dt = datetime.strptime(current_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except:
                    current_dt = datetime.strptime(current_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            elif isinstance(current_datetime, datetime):
                current_dt = current_datetime if current_datetime.tzinfo else current_datetime.replace(tzinfo=timezone.utc)
            else:
                current_dt = current_datetime
            
            # 检查BTC是否企稳
            if self.check_btc_stability(current_dt):
                # 🚀 BTC企稳/反弹 → 快速止盈！
                # 根据建仓时长决定使用哪个档位
                if isinstance(entry_datetime, str):
                    try:
                        entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    except:
                        entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
                elif isinstance(entry_datetime, datetime):
                    entry_dt = entry_datetime if entry_datetime.tzinfo else entry_datetime.replace(tzinfo=timezone.utc)
                else:
                    entry_dt = entry_datetime
                
                elapsed_hours = (current_dt - entry_dt).total_seconds() / 3600
                
                # 判断币种档位（简化版：根据建仓时长）
                if elapsed_hours < 2.0:
                    # 2小时内：使用强势币企稳止盈
                    tp = self.btc_stable_strong_tp
                    logging.info(f"🚀 {symbol} BTC企稳！建仓{elapsed_hours:.2f}h，使用强势币企稳止盈={tp:.0f}%")
                elif elapsed_hours < 12.0:
                    # 2-12小时：使用中等币企稳止盈
                    tp = self.btc_stable_medium_tp
                    logging.info(f"🚀 {symbol} BTC企稳！建仓{elapsed_hours:.2f}h，使用中等币企稳止盈={tp:.0f}%")
                else:
                    # 12小时后：使用弱势币企稳止盈
                    tp = self.btc_stable_weak_tp
                    logging.info(f"🚀 {symbol} BTC企稳！建仓{elapsed_hours:.2f}h，使用弱势币企稳止盈={tp:.0f}%")
                
                return tp
        except Exception as e:
            logging.error(f"❌ {symbol} BTC企稳判断异常: {e}，继续使用正常止盈逻辑")
        
        # ============================================================================
        # 如果BTC未企稳，继续使用原有的动态止盈逻辑
        # ============================================================================
        try:
            # 🔥 关键修复：在建仓后2小时内，不使用缓存，每次都重新计算
            # 原因：币的强弱判定是实时变化的，需要每小时重新评估
            if isinstance(entry_datetime, str):
                try:
                    entry_dt_temp = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except:
                    entry_dt_temp = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            elif isinstance(entry_datetime, datetime):
                entry_dt_temp = entry_datetime if entry_datetime.tzinfo else entry_datetime.replace(tzinfo=timezone.utc)
            else:
                entry_dt_temp = entry_datetime
            
            if isinstance(current_datetime, str):
                try:
                    current_dt_temp = datetime.strptime(current_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except:
                    current_dt_temp = datetime.strptime(current_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            elif isinstance(current_datetime, datetime):
                current_dt_temp = current_datetime if current_datetime.tzinfo else current_datetime.replace(tzinfo=timezone.utc)
            else:
                current_dt_temp = current_datetime
            
            elapsed_hours_check = (current_dt_temp - entry_dt_temp).total_seconds() / 3600
            
            # 🔥 重要修改：完全去掉缓存，每次都重新计算！
            # 原因：止盈止损是关键逻辑，不能因为缓存导致判定过时
            # 币的表现是动态变化的，必须实时判断
            logging.debug(f"🔄 {symbol} 不使用缓存，重新计算止盈阈值（已过{elapsed_hours_check:.2f}小时）")

            # 获取建仓价格
            avg_price = position['avg_entry_price']
            symbol = position['symbol']
            
            # 🔧 安全检查：确保entry_datetime是有效的datetime对象
            if entry_datetime is None or (hasattr(entry_datetime, '__class__') and entry_datetime.__class__.__name__ == 'NaTType'):
                result = self.take_profit_pct
                logging.warning(f"{symbol} entry_datetime无效，使用默认止盈={result}")
                return result
            
            # 🔧 统一时区处理：确保是timezone-aware的（只处理一次）
            if isinstance(entry_datetime, str):
                try:
                    entry_datetime = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except:
                    entry_datetime = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            elif isinstance(entry_datetime, datetime) and entry_datetime.tzinfo is None:
                entry_datetime = entry_datetime.replace(tzinfo=timezone.utc)
            
            if isinstance(current_datetime, str):
                try:
                    current_datetime = datetime.strptime(current_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except:
                    current_datetime = datetime.strptime(current_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            elif isinstance(current_datetime, datetime) and current_datetime.tzinfo is None:
                current_datetime = current_datetime.replace(tzinfo=timezone.utc)
            
            # ============ 第1步：先判断2小时（短期表现）============
            # 🔥 用户要求的逻辑：
            #   - 0-2小时内：不判断，所有币都按强势币（21%）止盈
            #   - 2小时后：开始判断分化
            #     - 下跌占比≥35% → 确认为强势币（21%）
            #     - 下跌占比<35% → 降为中等币（20%）
            #   - 12小时后：中等币可能降到9%
            
            elapsed_time = current_datetime - entry_datetime
            elapsed_hours = elapsed_time.total_seconds() / 3600
            
            # 🔥 2小时内：固定返回强势币止盈，不做判断
            if elapsed_hours < 2.0:
                logging.debug(f"🎯 {symbol} 建仓{elapsed_hours:.2f}小时，2小时内固定使用强势币止盈={self.strong_coin_tp_pct:.0f}%")
                return self.strong_coin_tp_pct
            
            # 🚨 新增：建仓刚满2小时时，优先检查2h提前止损（在动态止盈判断之前）
            if self.enable_2h_early_stop and 2.0 <= elapsed_hours < 3.0:
                try:
                    cursor = self.crypto_conn.cursor()
                    kline_5m_table = f'Kline5m_{symbol}'
                    
                    # 🔥 只查询建仓后2小时那个时点的最近一根5分钟K线的close价
                    entry_time_2h = entry_datetime + timedelta(hours=2)
                    entry_time_2h_ts = int(entry_time_2h.timestamp() * 1000)
                    
                    query = f"""
                    SELECT close
                    FROM {kline_5m_table}
                    WHERE open_time <= ?
                    ORDER BY open_time DESC
                    LIMIT 1
                    """
                    cursor.execute(query, (entry_time_2h_ts,))
                    result = cursor.fetchone()
                    
                    if result:
                        # 计算2h时点收盘价相对建仓价的涨幅
                        close_2h = result[0]
                        price_change_2h = (close_2h - avg_price) / avg_price
                        
                        # 如果2h时点涨幅 > 阈值（默认1.85%），触发提前止损
                        if price_change_2h > self.early_stop_2h_threshold:
                            logging.warning(
                                f"🚨 {symbol} 2h提前止损触发！\n"
                                f"  • 2h时点收盘价：{close_2h:.6f}\n"
                                f"  • 建仓价：{avg_price:.6f}\n"
                                f"  • 涨幅：{price_change_2h*100:.2f}% > 阈值 {self.early_stop_2h_threshold*100:.2f}%\n"
                                f"  • 返回特殊值-0.999，触发立即平仓"
                            )
                            # 返回特殊值，表示需要立即平仓
                            position['early_stop_triggered'] = True
                            position['early_stop_reason'] = '2h_price_surge'
                            return -0.999  # 特殊值，在check_exit_conditions中识别
                except Exception as e:
                    logging.error(f"❌ {symbol} 2h提前止损检查失败: {e}")
                    import traceback
                    logging.error(f"异常堆栈:\n{traceback.format_exc()}")
            
            # 🔥 2小时后、12小时前：使用2小时判断结果
            if 2.0 <= elapsed_hours < 12.0:
                # 🔥 修复：如果12小时判断已经完成，优先使用12小时的结果
                if position.get('dynamic_tp_12h_checked'):
                    cached_tp = position.get('dynamic_tp_pct', self.medium_coin_tp_pct)
                    logging.debug(f"🎯 {symbol} 12小时判断已完成，优先使用12小时结果={cached_tp:.0f}%")
                    return cached_tp
                
                # 如果已经判定过了（仅2小时判断），直接返回判定结果
                # 🔧 2-12小时之间只检查2小时判断的结果（强/中），不检查12小时的结果（弱）
                if position.get('dynamic_tp_strong'):
                    logging.debug(f"🎯 {symbol} 已判定为强势币，返回止盈={self.strong_coin_tp_pct:.0f}%")
                    return self.strong_coin_tp_pct
                if position.get('dynamic_tp_medium'):
                    logging.debug(f"🎯 {symbol} 已判定为中等币，返回止盈={self.medium_coin_tp_pct:.0f}%")
                    return self.medium_coin_tp_pct
                
                # 还没判定过，现在开始判定
                try:
                    cursor = self.crypto_conn.cursor()
                    kline_5m_table = f'Kline5m_{symbol}'
                    
                    # 获取建仓后2小时的所有5分钟K线
                    start_ts = int(entry_datetime.timestamp() * 1000)
                    window_2h_end = entry_datetime + timedelta(hours=2)
                    end_ts = int(window_2h_end.timestamp() * 1000)
                    
                    query = f"""
                    SELECT close
                    FROM {kline_5m_table}
                    WHERE open_time >= ? AND open_time < ?
                    ORDER BY open_time
                    """
                    cursor.execute(query, (start_ts, end_ts))
                    closes = [row[0] for row in cursor.fetchall()]
                    
                    logging.info(f"🔍 {symbol} 2小时判断：查询到{len(closes)}根5分钟K线")
                    
                    if len(closes) >= 2:
                        actual_count = len(closes)
                        
                        # 做空策略：计算每根K线相对建仓价的跌幅
                        returns = [(close - avg_price) / avg_price for close in closes]
                        
                        # 统计跌幅>2%的K线数量
                        count_drop_threshold = sum(1 for r in returns if r < -self.dynamic_tp_2h_growth_threshold)
                        pct_drop = count_drop_threshold / actual_count
                        
                        logging.info(
                            f"🔍 {symbol} 2小时判断详情：\n"
                            f"  • K线数量：{actual_count}根\n"
                            f"  • 下跌>{self.dynamic_tp_2h_growth_threshold*100:.1f}%的K线：{count_drop_threshold}根\n"
                            f"  • 下跌占比：{pct_drop*100:.1f}%\n"
                            f"  • 阈值：{self.dynamic_tp_2h_ratio*100:.0f}%\n"
                            f"  • 建仓价：{avg_price:.6f}"
                        )
                        
                        position['dynamic_tp_2h_pct_drop'] = pct_drop * 100
                        
                        if pct_drop >= self.dynamic_tp_2h_ratio:
                            # 下跌占比≥35%：确认为强势币（30%）
                            adjusted_tp = self.strong_coin_tp_pct
                            position['dynamic_tp_pct'] = adjusted_tp
                            position['dynamic_tp_strong'] = True
                            position['dynamic_tp_trigger'] = '2h_strong_confirmed'
                            position['dynamic_tp_2h_result'] = 'strong'  # 🔧 单独保存2小时判断结果
                            
                            logging.info(
                                f"✅ {symbol} 2小时确认为强势币：\n"
                                f"  • 下跌占比 {pct_drop*100:.1f}% ≥ 阈值 {self.dynamic_tp_2h_ratio*100:.0f}%\n"
                                f"  • 固定止盈={adjusted_tp:.0f}%"
                            )
                            return adjusted_tp
                        else:
                            # 下跌占比<35%：降为中等币（20%）
                            adjusted_tp = self.medium_coin_tp_pct
                            position['dynamic_tp_pct'] = adjusted_tp
                            position['dynamic_tp_medium'] = True
                            position['dynamic_tp_trigger'] = '2h_medium'
                            position['dynamic_tp_2h_result'] = 'medium'  # 🔧 单独保存2小时判断结果
                            
                            logging.warning(
                                f"⚠️ {symbol} 2小时判定为中等币：\n"
                                f"  • 下跌占比 {pct_drop*100:.1f}% < 阈值 {self.dynamic_tp_2h_ratio*100:.0f}%\n"
                                f"  • 止盈从30%降至{adjusted_tp:.0f}%"
                            )
                            return adjusted_tp
                    else:
                        # K线数据不足，保持强势币
                        logging.warning(f"⚠️ {symbol} 2小时K线不足（{len(closes)}根），保持强势币止盈={self.strong_coin_tp_pct:.0f}%")
                        position['dynamic_tp_strong'] = True
                        position['dynamic_tp_2h_result'] = 'strong'  # 🔧 单独保存2小时判断结果
                        return self.strong_coin_tp_pct
                except Exception as e:
                    logging.error(f"❌ {symbol} 2小时判断失败: {e}")
                    import traceback
                    logging.error(f"异常堆栈:\n{traceback.format_exc()}")
                    # 异常时保持强势币
                    position['dynamic_tp_strong'] = True
                    return self.strong_coin_tp_pct

            # ============ 第2步：12小时后判断弱势币（10%止盈）============
            # 🔥 新逻辑（用户要求）：
            # - 如果12小时内60%的K线下跌>9.5% → 强势币（保持原止盈22%或继续2小时判断的止盈）
            # - 否则 → 弱势币（10%止盈）
            # 这样会让大部分币都降为弱势币10%止盈
            
            if elapsed_hours >= 12.0:
                # 🔥 对所有持仓（包括强势币和中等币）进行12小时判断
                logging.info(f"🕐 {symbol} 达到12小时，开始判断（elapsed_hours={elapsed_hours:.1f}h, dynamic_tp_weak={position.get('dynamic_tp_weak')}）")
                
                # 🔥 修复：如果12小时判断已经执行过，直接返回缓存的结果
                if position.get('dynamic_tp_12h_checked'):
                    cached_tp = position.get('dynamic_tp_pct', self.medium_coin_tp_pct)
                    logging.info(f"✅ {symbol} 已完成12小时判断，使用缓存结果={cached_tp:.0f}%")
                    return cached_tp
                
                if not position.get('dynamic_tp_weak'):  # 还没被判定为弱势币的都要判断
                    try:
                        cursor = self.crypto_conn.cursor()
                        kline_5m_table = f'Kline5m_{symbol}'
                        
                        # 获取建仓后12小时的所有5分钟K线
                        start_ts = int(entry_datetime.timestamp() * 1000)
                        window_12h_end = entry_datetime + timedelta(hours=12)
                        end_ts = int(window_12h_end.timestamp() * 1000)
                        
                        query = f"""
                        SELECT close
                        FROM {kline_5m_table}
                        WHERE open_time >= ? AND open_time < ?
                        ORDER BY open_time
                        """
                        cursor.execute(query, (start_ts, end_ts))
                        closes = [row[0] for row in cursor.fetchall()]
                        
                        logging.info(f"🔍 {symbol} 12小时判断：查询到{len(closes)}根5分钟K线")
                        
                        if len(closes) >= 2:
                            # ⚠️ 12h提前止损检查已移除，统一在 check_exit_conditions 中检查
                            # 原因：此处使用 closes[-1] 时间窗口不准确，与CSV统计的"12h整点"不一致
                            # 保留位置：check_exit_conditions 的小时K线循环（3392-3407行）
                            
                            actual_count = len(closes)
                            
                            # 做空策略：计算每根K线相对建仓价的跌幅
                            returns = [(close - avg_price) / avg_price for close in closes]
                            
                            # 🔥 修改：统计下跌>9.5%的K线数量（而不是5.5%）
                            count_drop_threshold = sum(1 for r in returns if r < -self.dynamic_tp_12h_growth_threshold)
                            pct_drop = count_drop_threshold / actual_count
                            
                            logging.info(
                                f"🔍 {symbol} 12小时判断详情：\n"
                                f"  • K线数量：{actual_count}根\n"
                                f"  • 下跌>{self.dynamic_tp_12h_growth_threshold*100:.1f}%的K线：{count_drop_threshold}根\n"
                                f"  • 下跌占比：{pct_drop*100:.1f}%\n"
                                f"  • 阈值：{self.dynamic_tp_12h_ratio*100:.0f}%\n"
                                f"  • 建仓价：{avg_price:.6f}"
                            )
                            
                            position['dynamic_tp_12h_pct_drop'] = pct_drop * 100
                            
                            if pct_drop >= self.dynamic_tp_12h_ratio:
                                # 🔥 下跌占比≥60%：确认为强势币（22%）
                                adjusted_tp = self.strong_coin_tp_pct  # 22%
                                position['dynamic_tp_pct'] = adjusted_tp
                                position['dynamic_tp_strong'] = True
                                position['dynamic_tp_medium'] = False
                                position['dynamic_tp_weak'] = False
                                position['dynamic_tp_trigger'] = '12h_strong_upgrade'
                                position['dynamic_tp_12h_checked'] = True  # 标记已完成12小时判断
                                
                                logging.info(
                                    f"⬆️ {symbol} 12小时确认为强势币：\n"
                                    f"  • 下跌占比 {pct_drop*100:.1f}% ≥ 阈值 {self.dynamic_tp_12h_ratio*100:.0f}%\n"
                                    f"  • 止盈调整为{adjusted_tp:.0f}%"
                                )
                                return adjusted_tp
                            else:
                                # 🔥 下跌占比<60%：检查是否为连续2小时确认
                                # 如果是连续确认，保留强势币或中等币状态，不降为弱势币
                                is_consecutive = self._check_consecutive_surge_at_entry(position)
                                
                                if is_consecutive:
                                    # 连续确认交易对：保留当前状态（强势或中等币）
                                    if position.get('dynamic_tp_strong'):
                                        adjusted_tp = self.strong_coin_tp_pct  # 保持33%
                                        position['dynamic_tp_trigger'] = '12h_consecutive_keep_strong'
                                        logging.info(
                                            f"✅ {symbol} 12小时检查：连续2小时确认，保持强势币止盈：\n"
                                            f"  • 下跌占比 {pct_drop*100:.1f}% < 阈值 {self.dynamic_tp_12h_ratio*100:.0f}%\n"
                                            f"  • 但为连续确认交易对，保持强势币止盈={adjusted_tp:.0f}%"
                                        )
                                    else:
                                        adjusted_tp = self.medium_coin_tp_pct  # 保持21%
                                        position['dynamic_tp_trigger'] = '12h_consecutive_keep_medium'
                                        logging.info(
                                            f"✅ {symbol} 12小时检查：连续2小时确认，保持中等币止盈：\n"
                                            f"  • 下跌占比 {pct_drop*100:.1f}% < 阈值 {self.dynamic_tp_12h_ratio*100:.0f}%\n"
                                            f"  • 但为连续确认交易对，保持中等币止盈={adjusted_tp:.0f}%"
                                        )
                                    
                                    position['dynamic_tp_pct'] = adjusted_tp
                                    position['dynamic_tp_12h_checked'] = True
                                    position['is_consecutive_confirmed'] = True  # 标记为连续确认
                                    return adjusted_tp
                                else:
                                    # 非连续确认：正常降为弱势币（10%止盈）
                                    adjusted_tp = self.weak_coin_tp_pct  # 10%
                                    position['dynamic_tp_pct'] = adjusted_tp
                                    position['dynamic_tp_weak'] = True
                                    position['dynamic_tp_strong'] = False
                                    position['dynamic_tp_medium'] = False
                                    position['dynamic_tp_trigger'] = '12h_weak'
                                    position['dynamic_tp_12h_checked'] = True  # 标记已完成12小时判断
                                    
                                    logging.warning(
                                        f"⚠️⚠️⚠️ {symbol} 12小时判定为弱势币：\n"
                                        f"  • 下跌占比 {pct_drop*100:.1f}% < 阈值 {self.dynamic_tp_12h_ratio*100:.0f}%\n"
                                        f"  • 止盈降至{adjusted_tp:.0f}%"
                                    )
                                    return adjusted_tp
                        else:
                            # K线数据不足，保持原判断
                            if position.get('dynamic_tp_strong'):
                                logging.warning(f"⚠️ {symbol} 12小时K线不足（{len(closes)}根），保持强势币止盈={self.strong_coin_tp_pct:.0f}%")
                                return self.strong_coin_tp_pct
                            else:
                                logging.warning(f"⚠️ {symbol} 12小时K线不足（{len(closes)}根），保持中等币止盈={self.medium_coin_tp_pct:.0f}%")
                                return self.medium_coin_tp_pct
                    except Exception as e:
                        logging.debug(f"❌ {symbol} 12小时判断失败: {e}")

            # ============ 兜底逻辑：建仓10分钟内使用默认强势币止盈 ============
            # 🔥 重要修复：不读取缓存的dynamic_tp_pct，每次都重新计算
            # 只在建仓后10分钟内（K线数据不足2根）才走兜底逻辑
            
            # 没有判定结果，使用强势币默认值（建仓后前10分钟）
            # 这个兜底逻辑只会在极少数情况下触发（K线数据不足2根）
            default_tp = self.strong_coin_tp_pct
            logging.info(f"🎯 {symbol} 使用强势币默认止盈={default_tp:.0f}%（建仓初期或数据不足）")
            return default_tp
                
        except Exception as e:
            logging.error(f"❌❌❌ {symbol} 计算动态止盈异常: {e}")
            import traceback
            logging.error(f"异常堆栈:\n{traceback.format_exc()}")
            result = self.strong_coin_tp_pct
            position['dynamic_tp_pct'] = result
            position['dynamic_tp_strong'] = True  # 异常时默认强势币
            logging.error(f"使用强势币默认止盈={result*100:.0f}%")
            return result

    def _check_consecutive_surge_at_entry(self, position: Dict) -> bool:
        """检查该持仓在建仓时是否为连续2小时卖量暴涨
        
        判断逻辑：
        1. 获取信号发生时间（第1小时）
        2. 建仓时间 = 信号时间 + 1小时（第2小时）
        3. 检查信号小时和建仓小时是否都有卖量>=10倍
        4. 如果是，返回True（连续确认）
        
        Args:
            position: 持仓信息
        
        Returns:
            bool: 是否为连续2小时确认
        """
        try:
            symbol = position['symbol']
            signal_datetime_str = position.get('signal_datetime')
            
            if not signal_datetime_str:
                return False
            
            # 解析信号时间（第1小时）
            if isinstance(signal_datetime_str, str):
                # 尝试两种格式：带秒和不带秒
                try:
                    signal_dt = datetime.strptime(signal_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    signal_dt = datetime.strptime(signal_datetime_str, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                signal_dt = signal_datetime_str
            
            # 🔥 修正：建仓时间 = 信号时间 + 1小时（第2小时）
            entry_dt = signal_dt + timedelta(hours=1)
            
            # 获取信号日期的前一天（用于计算昨日平均）
            signal_date = signal_dt.strftime('%Y-%m-%d')
            yesterday_dt = signal_dt - timedelta(days=1)
            yesterday_date = yesterday_dt.strftime('%Y-%m-%d')
            
            cursor = self.crypto_conn.cursor()
            
            # 步骤1：获取昨日日K线总卖量
            daily_table = f'DailyKline_{symbol}'
            yesterday_start_ms, yesterday_end_ms = self.date_str_to_timestamp_range(yesterday_date)
            cursor.execute(f'''
                SELECT volume, active_buy_volume
                FROM "{daily_table}"
                WHERE open_time >= ? AND open_time < ?
            ''', (yesterday_start_ms, yesterday_end_ms))
            
            yesterday_row = cursor.fetchone()
            if not yesterday_row or not yesterday_row[0] or not yesterday_row[1]:
                return False
            
            # 计算昨日平均小时卖量
            yesterday_daily_sell_volume = yesterday_row[0] - yesterday_row[1]
            yesterday_avg_hour_volume = yesterday_daily_sell_volume / 24.0
            
            if yesterday_avg_hour_volume <= 0:
                return False
            
            # 🔥 修正：步骤2：获取信号小时和建仓小时的数据
            hourly_table = f'HourlyKline_{symbol}'
            signal_hour_ts = int(signal_dt.timestamp() * 1000)
            entry_hour_ts = int(entry_dt.timestamp() * 1000)  # 建仓小时
            
            cursor.execute(f'''
                SELECT open_time, volume, active_buy_volume
                FROM "{hourly_table}"
                WHERE open_time >= ? AND open_time <= ?
                ORDER BY open_time ASC
            ''', (signal_hour_ts, entry_hour_ts))
            
            hours_data = cursor.fetchall()
            if len(hours_data) < 2:
                return False
            
            # 计算每小时的卖量倍数
            threshold = self.sell_surge_threshold  # 10倍
            ratios = []
            hour_times = []
            
            for hour_open_time, hour_total_volume, hour_buy_volume in hours_data:
                if not hour_total_volume or not hour_buy_volume:
                    return False
                
                hour_sell_volume = hour_total_volume - hour_buy_volume
                ratio = hour_sell_volume / yesterday_avg_hour_volume
                ratios.append(ratio)
                hour_times.append(datetime.fromtimestamp(hour_open_time/1000, tz=timezone.utc).strftime('%H:%M'))
            
            # 🔥 修正：判断两个小时都>=10倍
            if len(ratios) >= 2 and all(r >= threshold for r in ratios[-2:]):
                logging.info(
                    f"✅ {symbol} 确认为连续2小时卖量暴涨：\n"
                    f"  • 信号小时({hour_times[-2]}): {ratios[-2]:.2f}x\n"
                    f"  • 建仓小时({hour_times[-1]}): {ratios[-1]:.2f}x\n"
                    f"  • 阈值: {threshold}x"
                )
                return True
            else:
                logging.debug(f"❌ {symbol} 非连续确认（倍数: {ratios}）")
                return False
        
        except Exception as e:
            logging.warning(f"⚠️ 检查连续确认失败 {position.get('symbol')}: {e}")
            import traceback
            logging.warning(f"异常堆栈:\n{traceback.format_exc()}")
            return False

    def get_daily_buy_surge_coins(self, date_str: str) -> List[Dict]:
        """获取指定日期主动买量暴涨的合约
        
        Args:
            date_str: 日期字符串
        
        Returns:
            主动买量暴涨的合约列表
        """
        try:
            cursor = self.crypto_conn.cursor()
            
            # 获取所有交易对
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'DailyKline_%'")
            tables = cursor.fetchall()
            
            surge_contracts = []
            
            for table_name, in tables:
                symbol = table_name.replace('DailyKline_', '')
                
                if not symbol.endswith('USDT'):
                    continue
                
                try:
                    # 获取当日数据（使用 open_time）
                    start_ms, end_ms = self.date_str_to_timestamp_range(date_str)
                    cursor.execute(f'''
                        SELECT open_time, close, open, active_buy_volume
                        FROM "{table_name}"
                        WHERE open_time >= ? AND open_time < ?
                    ''', (start_ms, end_ms))
                    
                    today_result = cursor.fetchone()
                    if not today_result or not today_result[3]:
                        continue
                    
                    today_open_time, close_price, open_price, today_buy_volume = today_result
                    
                    # 获取昨日数据
                    yesterday_dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc) - timedelta(days=1)
                    yesterday_str = yesterday_dt.strftime('%Y-%m-%d')
                    yesterday_start_ms, yesterday_end_ms = self.date_str_to_timestamp_range(yesterday_str)
                    
                    cursor.execute(f'''
                        SELECT active_buy_volume
                        FROM "{table_name}"
                        WHERE open_time >= ? AND open_time < ?
                    ''', (yesterday_start_ms, yesterday_end_ms))
                    
                    yesterday_result = cursor.fetchone()
                    if not yesterday_result or not yesterday_result[0]:
                        continue
                    
                    yesterday_buy_volume = yesterday_result[0]
                    
                    # 计算买量暴涨倍数
                    if yesterday_buy_volume > 0:
                        buy_surge_ratio = today_buy_volume / yesterday_buy_volume
                        
                        # 如果买量暴涨超过阈值
                        if buy_surge_ratio >= self.sell_surge_threshold:
                            # 🆕 检查信号触发前1小时是否暴涨
                            passed, surge_pct = self.check_signal_surge(symbol, date_str, close_price)
                            
                            if not passed:
                                # 根据涨幅判断过滤原因
                                if surge_pct < 5.0:
                                    logging.info(f"⚠️ 过滤信号: {symbol} 在 {date_str} 买量暴涨 {buy_surge_ratio:.1f}倍，但1小时内涨幅仅{surge_pct:.1f}%（涨幅太低）")
                                else:
                                    logging.info(f"⚠️ 过滤信号: {symbol} 在 {date_str} 买量暴涨 {buy_surge_ratio:.1f}倍，但1小时内价格暴涨{surge_pct:.1f}%（追高风险）")
                                continue
                            
                            surge_contracts.append({
                                'symbol': symbol,
                                'close': close_price,
                                'open': open_price,
                                'today_buy_volume': today_buy_volume,
                                'yesterday_buy_volume': yesterday_buy_volume,
                                'buy_surge_ratio': buy_surge_ratio
                            })
                            
                            logging.info(f"🔥 发现买量暴涨: {symbol} 在 {date_str} 买量暴涨 {buy_surge_ratio:.1f}倍 (1小时涨幅{surge_pct:+.1f}%)")
                
                except Exception as e:
                    continue
            
            # 按买量暴涨倍数降序排序
            surge_contracts.sort(key=lambda x: x['buy_surge_ratio'], reverse=True)
            
            return surge_contracts
        
        except Exception as e:
            logging.error(f"获取 {date_str} 买量暴涨合约失败: {e}")
            return []

    def get_all_symbols(self) -> List[str]:
        """获取所有USDT交易对列表（缓存，回测期间交易对列表不变）"""
        if self._all_symbols_cache is not None:
            return self._all_symbols_cache
        
        cursor = self.crypto_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'DailyKline_%'")
        tables = cursor.fetchall()
        symbols = [
            table_name[0].replace('DailyKline_', '') 
            for table_name in tables 
            if table_name[0].replace('DailyKline_', '').endswith('USDT')
        ]
        self._all_symbols_cache = symbols
        logging.info(f"🔍 找到 {len(symbols)} 个USDT交易对")
        return symbols
    
    def get_daily_1hour_surge_signals(self, check_date: str) -> List[Dict]:
        """🆕 优化版：检测某天内哪些小时的卖量超过昨日平均小时卖量
        
        检测逻辑：
        1. 获取昨日日K线的 active_sell_volume（总卖量 = volume - active_buy_volume）
        2. 计算昨日平均小时卖量 = 总卖量 / 24（1天=24小时）
        3. 遍历今日24小时，找到第一个卖量 >= 昨日平均小时卖量 × 阈值的小时
        4. 那个小时就是信号时间
        
        Args:
            check_date: 检测日期 'YYYY-MM-DD'
        
        Returns:
            信号列表，包含symbol、信号时间、倍数等
        """
        try:
            cursor = self.crypto_conn.cursor()
            
            # 获取所有交易对列表
            all_symbols = self.get_all_symbols()
            total_symbols = len(all_symbols)
            
            signals = []
            # 🔥 使用配置的卖量倍数阈值（而非硬编码的2.0）
            threshold = self.sell_surge_threshold  # 某小时卖量 >= 昨日平均小时卖量 × threshold倍
            
            check_dt = datetime.strptime(check_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            yesterday_date = (check_dt - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 遍历所有交易对
            logging.info(f"🔍 开始扫描 {check_date} 的信号，共 {total_symbols} 个交易对...")
            for idx, symbol in enumerate(all_symbols, 1):
                
                try:
                    # 🚀 步骤1：获取昨日日K线总卖量（使用 open_time）
                    daily_table = f'DailyKline_{symbol}'
                    yesterday_start_ms, yesterday_end_ms = self.date_str_to_timestamp_range(yesterday_date)
                    cursor.execute(f'''
                        SELECT volume, active_buy_volume
                        FROM "{daily_table}"
                        WHERE open_time >= ? AND open_time < ?
                    ''', (yesterday_start_ms, yesterday_end_ms))
                    
                    yesterday_row = cursor.fetchone()
                    if not yesterday_row or not yesterday_row[0] or not yesterday_row[1]:
                        continue
                    
                    # 🔧 计算昨日总卖量 = 总成交量 - 主动买量
                    yesterday_daily_sell_volume = yesterday_row[0] - yesterday_row[1]
                    # 🔧 关键修复：计算昨日平均小时卖量（1天 = 24小时）
                    yesterday_avg_hour_volume = yesterday_daily_sell_volume / 24.0
                    
                    # 🚀 步骤2：获取今日所有小时K线（使用 open_time）
                    hourly_table = f'HourlyKline_{symbol}'
                    check_start_ms, check_end_ms = self.date_str_to_timestamp_range(check_date)
                    cursor.execute(f'''
                        SELECT open_time, volume, active_buy_volume, close
                        FROM "{hourly_table}"
                        WHERE open_time >= ? AND open_time < ?
                        ORDER BY open_time ASC
                    ''', (check_start_ms, check_end_ms))
                    
                    today_hours = cursor.fetchall()
                    if not today_hours:
                        continue
                    
                    # 🚀 步骤3：找到第一个满足条件的小时
                    # 🔧 使用收盘价（close）作为信号价格：
                    #    - 收盘价 = 小时结束时的价格（如15:59:59）
                    #    - 下一整点小时开盘价（16:00:00）≈ 上一小时收盘价（15:59:59）
                    #    - 配合立即建仓（delay=0），价格偏离最小
                    for hour_data in today_hours:
                        hour_open_time, hour_total_volume, hour_buy_volume, hour_price = hour_data
                        
                        if not hour_total_volume or not hour_buy_volume or not hour_price:
                            continue
                        
                        # 🔧 计算当前小时卖量 = 总成交量 - 主动买量
                        hour_sell_volume = hour_total_volume - hour_buy_volume
                        
                        # 🔧 修复后：某小时卖量 vs 昨日平均小时卖量
                        surge_ratio = hour_sell_volume / yesterday_avg_hour_volume
                        
                        # 🔥 满足阈值，记录信号（使用配置的 sell_surge_threshold）
                        if surge_ratio >= threshold and surge_ratio <= self.sell_surge_max:
                            signal_datetime = self.timestamp_to_datetime(hour_open_time)
                            
                            signals.append({
                                'symbol': symbol,
                                'signal_datetime': signal_datetime,
                                'signal_price': hour_price,
                                'surge_ratio': surge_ratio,
                                'signal_hour_volume': hour_sell_volume,
                                'yesterday_avg_hour_volume': yesterday_avg_hour_volume
                            })
                            
                            logging.info(f"🔥 发现卖量暴涨信号: {symbol} @{signal_datetime.strftime('%H:00')} 倍数{surge_ratio:.2f}x 价格{hour_price:.6f}")
                            break  # 只记录第一个满足条件的小时
                        elif surge_ratio > self.sell_surge_max:
                            hour_time_str = self.timestamp_to_datetime(hour_open_time).strftime('%H:00')
                            logging.debug(f"⚠️ 过滤高倍数卖量信号: {symbol} @{hour_time_str} 倍数{surge_ratio:.2f}x (>{self.sell_surge_max}倍)")
                            break  # 超过上限也跳过该交易对后续小时（保持原逻辑：只关心最早触发的小时）
                
                except Exception as e:
                    continue
                
                # 每扫描100个交易对显示一次进度
                if idx % 100 == 0 or idx == total_symbols:
                    logging.info(f"  扫描进度: {idx}/{total_symbols} ({idx*100//total_symbols}%) | 已找到信号: {len(signals)} 个")
            
            # 按倍数降序排序
            signals.sort(key=lambda x: x['surge_ratio'], reverse=True)
            
            logging.info(f"✅ {check_date} 扫描完成，共发现 {len(signals)} 个卖量暴涨信号")
            return signals
        
        except Exception as e:
            logging.error(f"❌ 获取信号失败: {e}")
            return []
    
    def get_consecutive_2hour_surge_signals(self, check_date: str) -> List[Dict]:
        """🆕 单小时卖量暴涨信号检测（已修改为单小时触发）
        
        策略要求：
        1. 单小时：卖量 >= 昨日平均小时卖量 × 10倍 → 立即生成信号
        2. 倍数范围：10-14008倍（过滤过低和极端信号）
        3. 建仓时机：信号小时后开始尝试建仓
        4. 只记录第一个满足条件的小时（避免重复）
        
        Args:
            check_date: 检测日期 'YYYY-MM-DD'
        
        Returns:
            卖量暴涨信号列表
        """
        try:
            cursor = self.crypto_conn.cursor()
            
            # 获取所有交易对列表
            all_symbols = self.get_all_symbols()
            total_symbols = len(all_symbols)
            
            signals = []
            threshold = self.sell_surge_threshold  # 卖量倍数阈值（默认10倍）
            
            check_dt = datetime.strptime(check_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            yesterday_date = (check_dt - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 遍历所有交易对
            logging.info(f"🔍 开始扫描 {check_date} 的连续2小时信号，共 {total_symbols} 个交易对...")
            
            for idx, symbol in enumerate(all_symbols, 1):
                try:
                    # 步骤1：获取昨日日K线总卖量
                    daily_table = f'DailyKline_{symbol}'
                    yesterday_start_ms, yesterday_end_ms = self.date_str_to_timestamp_range(yesterday_date)
                    cursor.execute(f'''
                        SELECT volume, active_buy_volume
                        FROM "{daily_table}"
                        WHERE open_time >= ? AND open_time < ?
                    ''', (yesterday_start_ms, yesterday_end_ms))
                    
                    yesterday_row = cursor.fetchone()
                    if not yesterday_row or not yesterday_row[0] or not yesterday_row[1]:
                        continue
                    
                    # 计算昨日平均小时卖量
                    yesterday_daily_sell_volume = yesterday_row[0] - yesterday_row[1]
                    yesterday_avg_hour_volume = yesterday_daily_sell_volume / 24.0
                    
                    if yesterday_avg_hour_volume <= 0:
                        continue
                    
                    # 步骤2：获取今日所有小时K线
                    hourly_table = f'HourlyKline_{symbol}'
                    check_start_ms, check_end_ms = self.date_str_to_timestamp_range(check_date)
                    cursor.execute(f'''
                        SELECT open_time, volume, active_buy_volume, close
                        FROM "{hourly_table}"
                        WHERE open_time >= ? AND open_time < ?
                        ORDER BY open_time ASC
                    ''', (check_start_ms, check_end_ms))
                    
                    today_hours = cursor.fetchall()
                    if not today_hours or len(today_hours) < 2:  # 至少需要2小时数据
                        continue
                    
                    # 步骤3：遍历每个小时，找到第一个满足条件的小时
                    found_signal = False
                    for hour_data in today_hours:
                        hour_open_time, hour_total_volume, hour_buy_volume, hour_price = hour_data
                        
                        if not hour_total_volume or not hour_buy_volume or not hour_price:
                            continue
                        
                        # 计算当前小时卖量
                        hour_sell_volume = hour_total_volume - hour_buy_volume
                        surge_ratio = hour_sell_volume / yesterday_avg_hour_volume
                        
                        # 🔥 单小时满足条件就生成信号
                        if threshold <= surge_ratio <= self.sell_surge_max:
                            signal_datetime = self.timestamp_to_datetime(hour_open_time)
                            
                            signals.append({
                                'symbol': symbol,
                                'signal_datetime': signal_datetime,
                                'signal_price': hour_price,
                                'surge_ratio': surge_ratio,
                                'signal_hour_volume': hour_sell_volume,
                                'yesterday_avg_hour_volume': yesterday_avg_hour_volume
                            })
                            
                            logging.info(
                                f"🔥 发现卖量暴涨信号: {symbol} "
                                f"@{signal_datetime.strftime('%H:00')} "
                                f"倍数{surge_ratio:.2f}x "
                                f"价格{hour_price:.6f}"
                            )
                            
                            found_signal = True
                            break  # 只记录第一个满足条件的小时
                        elif surge_ratio > self.sell_surge_max:
                            hour_time_str = self.timestamp_to_datetime(hour_open_time).strftime('%H:00')
                            logging.debug(
                                f"⚠️ 过滤高倍数卖量信号: {symbol} "
                                f"@{hour_time_str} 倍数{surge_ratio:.2f}x "
                                f"(>{self.sell_surge_max}倍)"
                            )
                            break  # 超过上限也跳过该交易对后续小时
                
                except Exception as e:
                    logging.debug(f"⚠️ {symbol} 检查失败: {e}")
                    continue
                
                # 每扫描100个交易对显示一次进度
                if idx % 100 == 0 or idx == total_symbols:
                    logging.info(
                        f"  扫描进度: {idx}/{total_symbols} ({idx*100//total_symbols}%) | "
                        f"已找到信号: {len(signals)} 个"
                    )
            
            # 按卖量倍数降序排序
            signals.sort(key=lambda x: x['surge_ratio'], reverse=True)
            
            logging.info(f"✅ {check_date} 扫描完成，共发现 {len(signals)} 个卖量暴涨信号")
            return signals
        
        except Exception as e:
            logging.error(f"获取 {check_date} 卖量暴涨信号失败: {e}")
            return []

    def _btc_daily_open_close_btc(self, utc_date_str: str):
        """查询 BTC 指定 UTC 日历日的日K open、close（DailyKline_BTCUSDT）。
        
        Returns:
            (open, close) 或 None（无数据）
        """
        try:
            start_ms, end_ms = self.date_str_to_timestamp_range(utc_date_str)
            cursor = self.crypto_conn.cursor()
            cursor.execute(
                '''
                SELECT open, close FROM "DailyKline_BTCUSDT"
                WHERE open_time >= ? AND open_time < ?
                ''',
                (start_ms, end_ms),
            )
            row = cursor.fetchone()
            if not row or row[0] is None or row[1] is None:
                return None
            return float(row[0]), float(row[1])
        except Exception as e:
            logging.error(f"❌ 读取 BTC 日K {utc_date_str} 失败: {e}")
            return None
    
    def check_btc_yesterday_yang_blocks_entry(self, entry_datetime) -> tuple:
        """昨日 BTC 日线 close > open（阳线）时，当日不建新仓。
        
        使用 UTC 日历日与 `DailyKline_BTCUSDT` 对齐（与回测其它日K 查询一致）。
        
        Returns:
            (skip: bool, reason: str)  skip=True 表示应拒绝建仓
        """
        if not self.enable_btc_yesterday_yang_no_new_entry:
            return False, ""
        try:
            if isinstance(entry_datetime, str):
                try:
                    edt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    edt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            elif isinstance(entry_datetime, datetime):
                edt = entry_datetime if entry_datetime.tzinfo else entry_datetime.replace(tzinfo=timezone.utc)
            else:
                return False, "建仓时间格式异常，跳过BTC昨日阳线检查"
            
            yday = (edt.astimezone(timezone.utc).date() - timedelta(days=1)).strftime('%Y-%m-%d')
            oc = self._btc_daily_open_close_btc(yday)
            if oc is None:
                logging.warning(f"⚠️ BTC 日K 缺失 {yday}，不拦截建仓")
                return False, ""
            o, c = oc
            if c > o:
                return True, f"昨日BTC日K阳线({yday} open={o:.2f} close={c:.2f})，当日不建新仓"
            return False, ""
        except Exception as e:
            logging.error(f"❌ BTC昨日阳线建仓检查异常: {e}")
            return False, ""
    
    def check_btc_stability(self, current_datetime: datetime) -> bool:
        """🆕 检查BTC是否处于企稳/反弹状态
        
        判断标准（优化版）：
        - 从当前时间往前取24根小时K线
        - 基准价 = 第24根K线（24小时前）的close价
        - 统计这24根K线中，有多少根close > 基准价 + 1.5%
        - 如果占比≥60% → 判定为企稳
        
        Args:
            current_datetime: 当前时间
            
        Returns:
            bool: True=BTC企稳，False=BTC下跌/震荡
        """
        if not self.enable_btc_stability_check:
            return False  # 功能未启用，返回False（使用正常止盈）
        
        try:
            # 🔧 从当前时间往前取26小时数据（多取2小时避免边界问题）
            start_time = current_datetime - timedelta(hours=26)
            start_date_str = start_time.strftime('%Y-%m-%d')
            end_date_str = current_datetime.strftime('%Y-%m-%d')
            
            btc_df = self.get_hourly_kline_data('BTCUSDT', start_date=start_date_str, end_date=end_date_str)
            
            if btc_df.empty:
                logging.warning(f"⚠️ 无法获取BTC数据，跳过企稳判断")
                return False
            
            # 转换时间
            btc_df['open_datetime'] = pd.to_datetime(btc_df['open_time'], unit='ms', utc=True)
            btc_df['close'] = btc_df['close'].astype(float)
            
            # 筛选：当前时间之前的数据
            btc_df = btc_df[btc_df['open_datetime'] <= current_datetime].sort_values('open_datetime')
            
            if len(btc_df) < 24:
                logging.debug(f"📊 BTC数据不足24小时({len(btc_df)}小时)，跳过企稳判断")
                return False
            
            # 🔥 取最近24根K线
            recent_24h = btc_df.tail(24).copy()
            
            # 🔥 基准价 = 第24根K线（即24小时前）的close价
            baseline_price = float(recent_24h.iloc[0]['close'])
            
            # 计算阈值价格
            threshold_price = baseline_price * (1 + self.btc_stability_threshold)
            
            # 统计有多少小时收盘价 > 阈值
            up_hours = (recent_24h['close'] > threshold_price).sum()
            total_hours = len(recent_24h)
            up_ratio = up_hours / total_hours
            
            # 判断是否企稳
            is_stable = up_ratio >= self.btc_stability_up_ratio
            
            if is_stable:
                baseline_time = recent_24h.iloc[0]['open_datetime'].strftime('%Y-%m-%d %H:%M')
                current_time = current_datetime.strftime('%Y-%m-%d %H:%M')
                logging.info(f"🚀 BTC企稳判断：{up_hours}/{total_hours}小时({up_ratio*100:.1f}%) > "
                           f"24h前价格${baseline_price:.2f}({baseline_time}) + {self.btc_stability_threshold*100:.1f}% "
                           f"→ 判定为企稳！(当前{current_time})")
            else:
                logging.debug(f"📉 BTC正常：{up_hours}/{total_hours}小时({up_ratio*100:.1f}%) 未达企稳阈值")
            
            return is_stable
            
        except Exception as e:
            logging.error(f"❌ BTC企稳判断异常: {e}")
            return False  # 异常时返回False（使用正常止盈）
    
    def get_hourly_kline_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取小时K线数据（安全版：不使用缓存，只查询需要的日期范围）
        
        Args:
            symbol: 交易对
            start_date: 开始日期（可选，格式：YYYY-MM-DD）
            end_date: 结束日期（可选，格式：YYYY-MM-DD）
        
        Note:
            量化回测中不使用缓存，确保数据准确性
        """
        table_name = f'HourlyKline_{symbol}'
        
        try:
            cursor = self.crypto_conn.cursor()
            
            # 构建带日期范围的查询（使用 open_time 时间戳）
            if start_date and end_date:
                start_ms, _ = self.date_str_to_timestamp_range(start_date)
                _, end_ms = self.date_str_to_timestamp_range(end_date)
                query = f'SELECT * FROM {table_name} WHERE open_time >= ? AND open_time <= ? ORDER BY open_time ASC'
                cursor.execute(query, (start_ms, end_ms))
            elif start_date:
                start_ms, _ = self.date_str_to_timestamp_range(start_date)
                query = f'SELECT * FROM {table_name} WHERE open_time >= ? ORDER BY open_time ASC'
                cursor.execute(query, (start_ms,))
            elif end_date:
                _, end_ms = self.date_str_to_timestamp_range(end_date)
                query = f'SELECT * FROM {table_name} WHERE open_time <= ? ORDER BY open_time ASC'
                cursor.execute(query, (end_ms,))
            else:
                # 没有指定范围时，查询全部（但会很慢）
                logging.warning(f"查询 {symbol} 全部小时K线数据，可能较慢")
                query = f'SELECT * FROM {table_name} ORDER BY open_time ASC'
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logging.warning(f"获取 {symbol} 小时K线数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_intraday_sell_surge_ratio(self, symbol: str, entry_datetime: str) -> float:
        """
        计算当日卖量倍数：建仓前12小时，每小时卖量相对前一小时的最大比值
        
        这个指标反映了短期卖量的爆发性
        
        Args:
            symbol: 交易对
            entry_datetime: 建仓时间 'YYYY-MM-DD HH:MM'
        
        Returns:
            float: 当日卖量倍数（最大的小时间卖量比值），如果数据不足返回0
        """
        try:
            # 解析建仓时间
            if ' ' in entry_datetime:
                try:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 获取建仓前12小时的小时K线数据
            start_time = entry_dt - timedelta(hours=12)
            end_time = entry_dt
            
            # 查询小时K线
            hourly_df = self.get_hourly_kline_data(
                symbol,
                start_date=start_time.strftime('%Y-%m-%d'),
                end_date=end_time.strftime('%Y-%m-%d')
            )
            
            if hourly_df.empty:
                return 0.0
            
            # 转换时间格式（从 open_time 毫秒级时间戳）
            hourly_df['open_datetime'] = pd.to_datetime(hourly_df['open_time'], unit='ms', utc=True)
            
            # 筛选建仓前12小时的数据
            mask = (hourly_df['open_datetime'] >= start_time) & (hourly_df['open_datetime'] < end_time)
            recent_hours = hourly_df[mask].sort_values('open_datetime').copy()
            
            if len(recent_hours) < 2:
                return 0.0
            
            # 计算主动卖量（总成交量 - 主动买量）
            recent_hours['active_buy_volume'] = recent_hours['active_buy_volume'].astype(float)
            recent_hours['volume'] = recent_hours['volume'].astype(float)
            recent_hours['active_sell_volume'] = recent_hours['volume'] - recent_hours['active_buy_volume']
            
            max_ratio = 0.0
            for i in range(1, len(recent_hours)):
                prev_sell_vol = recent_hours.iloc[i-1]['active_sell_volume']
                curr_sell_vol = recent_hours.iloc[i]['active_sell_volume']
                
                if prev_sell_vol > 0:
                    ratio = curr_sell_vol / prev_sell_vol
                    if ratio > max_ratio:
                        max_ratio = ratio
            
            return max_ratio
            
        except Exception as e:
            logging.debug(f"计算当日卖量倍数失败 {symbol}: {e}")
            return 0.0
    
    def calculate_intraday_buy_surge_ratio(self, symbol: str, entry_datetime: str) -> float:
        """
        计算当日买量倍数：建仓前12小时，每小时买量相对前一小时的最大比值
        
        这个指标反映了短期买量的爆发性，比值>10倍通常预示大涨
        
        Args:
            symbol: 交易对
            entry_datetime: 建仓时间 'YYYY-MM-DD HH:MM'
        
        Returns:
            float: 当日买量倍数（最大的小时间买量比值），如果数据不足返回0
        """
        try:
            # 解析建仓时间（添加 UTC 时区）
            if ' ' in entry_datetime:
                try:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 获取建仓前12小时的小时K线数据
            start_time = entry_dt - timedelta(hours=12)
            end_time = entry_dt
            
            logging.debug(f"📊 {symbol} 查询当日买量倍数，时间范围: {start_time} ~ {end_time}")
            
            # 查询小时K线
            hourly_df = self.get_hourly_kline_data(
                symbol,
                start_date=start_time.strftime('%Y-%m-%d'),
                end_date=end_time.strftime('%Y-%m-%d')
            )
            
            if hourly_df.empty:
                logging.warning(f"⚠️ {symbol} 获取小时K线数据为空，无法计算当日买量倍数")
                return 0.0
            
            # 转换时间格式（从 open_time 毫秒级时间戳）
            hourly_df['open_datetime'] = pd.to_datetime(hourly_df['open_time'], unit='ms', utc=True)
            
            # 筛选建仓前12小时的数据
            mask = (hourly_df['open_datetime'] >= start_time) & (hourly_df['open_datetime'] < end_time)
            recent_hours = hourly_df[mask].sort_values('open_datetime').copy()
            
            logging.debug(f"📊 {symbol} 筛选后有{len(recent_hours)}小时数据")
            
            if len(recent_hours) < 2:
                logging.warning(f"⚠️ {symbol} 数据不足（<2小时），无法计算当日买量倍数")
                return 0.0
            
            # 🔥 修复：小时K线字段名是 active_buy_volume，不是 taker_buy_volume
            recent_hours['active_buy_volume'] = recent_hours['active_buy_volume'].astype(float)
            
            max_ratio = 0.0
            for i in range(1, len(recent_hours)):
                prev_buy_vol = recent_hours.iloc[i-1]['active_buy_volume']
                curr_buy_vol = recent_hours.iloc[i]['active_buy_volume']
                
                if prev_buy_vol > 0:
                    ratio = curr_buy_vol / prev_buy_vol
                    if ratio > max_ratio:
                        max_ratio = ratio
                        logging.debug(
                            f"📊 {symbol} {recent_hours.iloc[i]['open_datetime'].strftime('%Y-%m-%d %H:%M')} "
                            f"买量{curr_buy_vol:.2f} / 前一小时{prev_buy_vol:.2f} = {ratio:.2f}倍"
                        )
            
            if max_ratio > 0:
                logging.info(f"📊 {symbol} 当日买量倍数: {max_ratio:.2f}倍（建仓前12小时最大小时间比值）")
            else:
                logging.warning(f"⚠️ {symbol} 未计算出有效的当日买量倍数（max_ratio=0）")
            
            return max_ratio
            
        except Exception as e:
            logging.warning(f"⚠️ 计算当日买量倍数失败 {symbol}: {e}")
            import traceback
            logging.warning(traceback.format_exc())
            return 0.0
    
    def get_open_interest_at_time(self, symbol: str, target_datetime: str) -> float:
        """
        获取指定时间点的合约未平仓量（sumOpenInterestValue）
        
        优先从本地数据库读取，失败时尝试调用API（仅限30天内数据）
        
        参数：
            symbol: 交易对（例如：BTCUSDT）
            target_datetime: 目标时间（格式：'YYYY-MM-DD HH:MM' 或 'YYYY-MM-DD HH:MM:SS'）
        
        返回：
            float: 未平仓量（美元价值），失败返回0
        """
        try:
            # 解析目标时间
            if ' ' in target_datetime:
                try:
                    target_dt = datetime.strptime(target_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    target_dt = datetime.strptime(target_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                target_dt = datetime.strptime(target_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 转换为毫秒时间戳
            target_timestamp = int(target_dt.timestamp() * 1000)
            
            # 🆕 方法1：从本地数据库读取（优先）
            try:
                conn = sqlite3.connect('db/top_trader_data.db')
                cursor = conn.cursor()
                
                # 查询最接近目标时间的数据（前后1小时范围内）
                time_tolerance = 3600000  # 1小时 = 3600秒 = 3600000毫秒
                cursor.execute('''
                    SELECT sum_open_interest_value, timestamp
                    FROM open_interest_history
                    WHERE symbol = ?
                    AND ABS(timestamp - ?) <= ?
                    ORDER BY ABS(timestamp - ?)
                    LIMIT 1
                ''', (symbol, target_timestamp, time_tolerance, target_timestamp))
                
                result = cursor.fetchone()
                conn.close()
                
                if result and result[0] is not None:
                    open_interest = float(result[0])
                    actual_timestamp = result[1]
                    time_diff = abs(actual_timestamp - target_timestamp) / 1000 / 60  # 转换为分钟
                    logging.debug(f"📊 {symbol} {target_datetime} 未平仓量: ${open_interest:,.0f} (数据库，误差{time_diff:.0f}分钟)")
                    return open_interest
                else:
                    logging.debug(f"⚠️ {symbol} {target_datetime} 数据库无数据，尝试API")
            
            except Exception as e:
                logging.debug(f"⚠️ {symbol} 数据库查询失败: {e}，尝试API")
            
            # 🆕 方法2：从Binance API读取（仅限30天内）
            days_ago = (datetime.now(timezone.utc) - target_dt).days
            if days_ago > 30:
                logging.debug(f"⚠️ {symbol} {target_datetime} 超过30天，API不支持，未平仓量=0")
                return 0.0
            
            url = 'https://fapi.binance.com/futures/data/openInterestHist'
            params = {
                'symbol': symbol,
                'period': '5m',
                'limit': 1,
                'endTime': target_timestamp
            }
            
            response = requests.get(url, params=params, timeout=10, proxies={'http': None, 'https': None})
            response.raise_for_status()
            data = response.json()
            
            if data and isinstance(data, list) and len(data) > 0:
                open_interest = float(data[0]['sumOpenInterestValue'])
                logging.debug(f"📊 {symbol} {target_datetime} 未平仓量: ${open_interest:,.0f} (API)")
                return open_interest
            else:
                logging.warning(f"⚠️ {symbol} {target_datetime} API返回数据为空")
                return 0.0
                
        except requests.exceptions.RequestException as e:
            logging.warning(f"⚠️ {symbol} 获取未平仓量失败（网络错误）: {e}")
            return 0.0
        except Exception as e:
            logging.warning(f"⚠️ {symbol} 获取未平仓量失败: {e}")
            return 0.0
    
    def calculate_funding_fee_cost(self, symbol: str, entry_datetime: datetime, exit_datetime: datetime, position_value: float, leverage: float = 3.0) -> Dict[str, float]:
        """
        计算持仓期间的资金费率总成本
        
        Args:
            symbol: 交易对（如BTCUSDT）
            entry_datetime: 建仓时间（UTC）
            exit_datetime: 平仓时间（UTC）
            position_value: 持仓价值/保证金（USDT）
            leverage: 杠杆倍数（默认3倍）
        
        Returns:
            {
                'total_fee': 总费用（正数=支出，负数=收入）,
                'fee_count': 收费次数,
                'avg_rate': 平均费率（百分比）
            }
        
        说明：
        - 资金费率每8小时收取一次（00:00、08:00、16:00 UTC）
        - 资金费按持仓名义价值计算：position_value × leverage
        - 正费率 = 做多方付给做空方（做空收费）
        - 负费率 = 做空方付给做多方（做空付费）
        - 对于做空策略：
          - 正费率是收入（减少成本）→ total_fee为负
          - 负费率是支出（增加成本）→ total_fee为正
        """
        try:
            cursor = self.crypto_conn.cursor()
            
            # 查询持仓期间的所有资金费率
            # funding_time格式：'2025-11-01 08:00:00'
            cursor.execute('''
                SELECT funding_time, funding_rate
                FROM funding_rate_history
                WHERE symbol = ?
                  AND funding_time >= ?
                  AND funding_time < ?
                ORDER BY funding_time
            ''', (symbol, entry_datetime.strftime('%Y-%m-%d %H:%M:%S'), exit_datetime.strftime('%Y-%m-%d %H:%M:%S')))
            
            rates = cursor.fetchall()
            
            if not rates:
                logging.debug(f"未找到{symbol}的资金费率数据（{entry_datetime} ~ {exit_datetime}）")
                return {'total_fee': 0.0, 'fee_count': 0, 'avg_rate': 0.0}
            
            total_fee = 0.0
            fee_count = len(rates)
            total_rate = 0.0
            
            # 🔧 关键修复：资金费按持仓名义价值计算（保证金 × 杠杆）
            nominal_value = position_value * leverage
            
            for funding_time, funding_rate in rates:
                if funding_rate is None:
                    continue
                
                rate = float(funding_rate)
                # 对于做空：
                # - 正费率（rate > 0）：做多付给做空，我们收入，减少成本（fee为负）
                # - 负费率（rate < 0）：做空付给做多，我们支出，增加成本（fee为正）
                # 🔧 修复：使用持仓名义价值（position_value × leverage）
                fee = -rate * nominal_value
                total_fee += fee
                total_rate += rate
            
            avg_rate = (total_rate / fee_count * 100) if fee_count > 0 else 0.0
            
            logging.debug(f"💰 {symbol} 资金费用: {total_fee:.4f} USDT（{fee_count}次，杠杆{leverage}x，平均费率{avg_rate:.4f}%）")
            
            return {
                'total_fee': round(total_fee, 4),  # 正数=支出，负数=收入
                'fee_count': fee_count,
                'avg_rate': round(avg_rate, 4)  # 平均费率（百分比）
            }
            
        except Exception as e:
            logging.warning(f"计算资金费率失败 {symbol}: {e}")
            return {'total_fee': 0.0, 'fee_count': 0, 'avg_rate': 0.0}
    
    def check_long_short_ratio_risk(self, symbol: str, entry_date: str) -> dict:
        """
        多空比风控检查（已禁用 - 不适用于买量暴涨策略）
        
        ⚠️  经过回测验证，多空比风控在买量暴涨策略中严重误伤：
        - 拦截43笔交易，其中38笔是盈利的（88.4%误伤率）
        - 损失+193.55%的收益
        - 原因：买量暴涨+多空比高 = 顶部形成，反而是做空的好时机
        
        策略性质差异：
        - backtrade8（涨幅第一）：多空比高→继续上涨→做空危险 ✅
        - hm1zk（买量暴涨）：多空比高→买盘爆发→顶部形成→做空机会 ❌
        
        保留此函数以备未来分析，但始终返回True
        
        Args:
            symbol: 交易对
            entry_date: 建仓日期 'YYYY-MM-DD'
        
        Returns:
            dict: {'should_trade': bool, 'reason': str, 'ratio': float}
        """
        result = {
            'should_trade': True,
            'reason': '',
            'ratio': None
        }
        
        # 查询多空比数据
        try:
            # 解析日期并转换为UTC毫秒时间戳（支持多种格式）
            if ' ' in entry_date:
                # 包含时间：'YYYY-MM-DD HH:MM' 或 'YYYY-MM-DD HH:MM:SS'
                try:
                    entry_dt = datetime.strptime(entry_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_date, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                # 只有日期：'YYYY-MM-DD'
                entry_dt = datetime.strptime(entry_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            check_ts = int(calendar.timegm(entry_dt.timetuple()) * 1000)
            time_tolerance = 86400000  # 1天的毫秒数（容差）
            
            # 查询顶级交易者多空比（使用timestamp字段）
            trader_cursor = self.trader_conn.cursor()
            
            query = """
            SELECT timestamp, long_short_ratio, long_account, short_account
            FROM top_account_ratio
            WHERE symbol = ?
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY ABS(timestamp - ?)
            LIMIT 1
            """
            
            trader_cursor.execute(query, [
                symbol,
                check_ts - time_tolerance,
                check_ts + time_tolerance,
                check_ts
            ])
            
            row = trader_cursor.fetchone()
            
            if row:
                account_ratio = float(row[1])
                result['ratio'] = account_ratio
                
                # 🔥 使用配置参数：多空比超过阈值拒绝建仓（仅2025-12-12之后）
                if entry_dt >= self.wind_control_start_date:
                    if account_ratio > self.max_account_ratio:
                        result['should_trade'] = False
                        result['reason'] = f"多空比{account_ratio:.2f} > {self.max_account_ratio}，市场过度做多"
                        logging.warning(f"🚫 {symbol} {result['reason']}")
                    else:
                        logging.info(f"✅ {symbol} 多空比{account_ratio:.2f}正常，可建仓")
                else:
                    logging.debug(f"📊 {symbol} 多空比{account_ratio:.2f}（12月12日前，风控未启用）")
        
        except Exception as e:
            logging.debug(f"查询多空比失败 {symbol}: {e}")
        
        return result
    
    def check_premium_index_risk(self, symbol: str, entry_datetime: str) -> dict:
        """
        Premium Index（基差率）风控检查
        
        风控阈值：建仓时最近的基差 < -0.3% 拒绝建仓
        
        使用最近的基差值（而非24小时平均），更能反映建仓时刻的市场状态
        
        Args:
            symbol: 交易对
            entry_datetime: 建仓时间 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD HH:MM'
        
        Returns:
            dict: {'should_trade': bool, 'reason': str, 'premium': float}
        """
        result = {
            'should_trade': True,
            'reason': '',
            'premium': None
        }
        
        try:
            # 解析时间
            if ' ' in entry_datetime:
                try:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 转换为UTC毫秒时间戳
            check_ts = int(calendar.timegm(entry_dt.timetuple()) * 1000)
            start_24h_ts = check_ts - 86400000  # 24小时前
            
            # 查询 premium_index_history 表，获取最近的基差
            cursor = self.crypto_conn.cursor()
            
            query = """
            SELECT open_time, close
            FROM premium_index_history
            WHERE symbol = ?
              AND open_time >= ?
              AND open_time <= ?
              AND interval = '1h'
            ORDER BY open_time DESC
            LIMIT 1
            """
            
            cursor.execute(query, (symbol, start_24h_ts, check_ts))
            row = cursor.fetchone()
            
            if row:
                # 使用最近的基差值
                latest_premium = float(row[1])
                result['premium'] = latest_premium
                
                # 🔥 使用配置参数判断：基差低于阈值拒绝建仓
                if latest_premium < self.premium_min_threshold:
                    result['should_trade'] = False
                    result['reason'] = f"最近基差{latest_premium*100:.2f}% < {self.premium_min_threshold*100:.1f}%，负基差过大风险高"
                    logging.warning(f"🚫 {symbol} {result['reason']}")
                else:
                    logging.info(f"✅ {symbol} 最近基差{latest_premium*100:.2f}% 正常，可建仓")
            else:
                logging.debug(f"📊 {symbol} 无Premium数据，跳过检查")
        
        except Exception as e:
            logging.debug(f"查询Premium失败 {symbol}: {e}")
        
        return result
    
    def check_cvd_new_low_risk(self, symbol: str, entry_datetime: str) -> dict:
        """
        CVD创新低风控检查（2026-02-05新增）⭐⭐⭐⭐⭐
        
        风控逻辑：
        1. 计算建仓前24小时的CVD（累积成交量差）
        2. CVD = Σ(主动买量 - 主动卖量)
        3. 如果建仓时CVD <= 24h最低值 → 拒绝建仓（CVD创新低）
        
        风控原理（基于50笔回测数据）：
        - 止盈交易中仅13.3%出现CVD创新低（2/15笔）
        - 止损交易中高达55.6%出现CVD创新低（5/9笔）
        - CVD创新低 = 恐慌抛售尾声 = 抄底资金进场 = 做空风险极高
        
        效果预期：
        - 止损过滤率：88.9%（过滤掉8/9笔止损）
        - 止盈保留率：53.3%（保留8/15笔止盈）
        - 单笔期望收益提升：72%（从+2.63%提升到+11.33%）
        
        典型案例：
        - NAORISUSDT：卖量暴涨45.6倍，CVD创新低，建仓后暴涨42%，-18%止损
        - LUMIAUSDT：卖量暴涨29.5倍，CVD创新低，建仓后反弹，-20%止损
        
        Args:
            symbol: 交易对
            entry_datetime: 建仓时间 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD HH:MM'
        
        Returns:
            dict: {
                'should_trade': bool,
                'reason': str,
                'cvd_current': float,  # 当前CVD值
                'cvd_min_24h': float,  # 24h最低CVD值
                'is_new_low': bool     # 是否创新低
            }
        """
        result = {
            'should_trade': True,
            'reason': '',
            'cvd_current': None,
            'cvd_min_24h': None,
            'is_new_low': False
        }
        
        try:
            # 解析建仓时间
            if ' ' in entry_datetime:
                try:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 转换为UTC毫秒时间戳
            entry_ts = int(calendar.timegm(entry_dt.timetuple()) * 1000)
            start_ts = entry_ts - (self.cvd_lookback_hours * 3600 * 1000)  # 回溯24小时
            
            # 查询小时K线数据
            table_name = f"HourlyKline_{symbol}"
            cursor = self.crypto_conn.cursor()
            
            query = f"""
            SELECT open_time, volume, active_buy_volume
            FROM {table_name}
            WHERE open_time >= ? AND open_time <= ?
            ORDER BY open_time ASC
            """
            
            cursor.execute(query, (start_ts, entry_ts))
            rows = cursor.fetchall()
            
            if not rows or len(rows) < 2:
                # 数据不足，放行（容错机制）
                logging.debug(f"📊 {symbol} CVD数据不足({len(rows) if rows else 0}笔)，跳过检查")
                return result
            
            # 计算CVD
            cvd_values = []
            cumulative_cvd = 0
            
            for row in rows:
                volume = float(row[1])
                active_buy_volume = float(row[2])
                active_sell_volume = volume - active_buy_volume
                
                # Volume Delta = 主动买量 - 主动卖量
                volume_delta = active_buy_volume - active_sell_volume
                
                # 累积CVD
                cumulative_cvd += volume_delta
                cvd_values.append(cumulative_cvd)
            
            # 最后一个CVD值为当前值
            cvd_current = cvd_values[-1]
            cvd_min_24h = min(cvd_values)
            
            result['cvd_current'] = cvd_current
            result['cvd_min_24h'] = cvd_min_24h
            
            # 判断是否创新低（允许小误差）
            if cvd_current <= cvd_min_24h:
                result['is_new_low'] = True
                result['should_trade'] = False
                result['reason'] = f"CVD创新低(当前:{cvd_current:.0f}, 24h最低:{cvd_min_24h:.0f})，恐慌抛售尾声风险高"
                logging.warning(f"🚫 {symbol} {result['reason']}")
            else:
                logging.info(f"✅ {symbol} CVD正常(当前:{cvd_current:.0f}, 24h最低:{cvd_min_24h:.0f})，可建仓")
        
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                logging.debug(f"📊 {symbol} 无小时K线表，跳过CVD检查")
            else:
                logging.debug(f"查询CVD失败 {symbol}: {e}")
        except Exception as e:
            logging.debug(f"CVD计算失败 {symbol}: {e}")
        
        return result
    
    def check_premium_24h_change_risk(self, symbol: str, entry_datetime: str) -> dict:
        """
        24小时基差变动风控检查（2026-02-04新增）
        
        风控逻辑：
        1. 获取建仓前24小时和建仓时的基差数据
        2. 计算24小时基差变动率 = (建仓时基差 - 24h前基差) / 24h前基差 * 100%
        3. 如果24小时基差下跌 > 阈值（默认30%），拒绝建仓
        
        风控原理（基于回测数据）：
        - 87.5%的止损交易在建仓前24h基差下跌>10%
        - 75%的止损交易在建仓前24h基差下跌>30%
        - 基差下跌说明合约价格相对现货下跌，做空风险高（市场已经看空）
        - 基差上涨时止损率仅12.5%（安全）
        
        Args:
            symbol: 交易对
            entry_datetime: 建仓时间 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD HH:MM' 或 'YYYY-MM-DD'
        
        Returns:
            dict: {
                'should_trade': bool,
                'reason': str,
                'premium_24h_change': float  # 24h基差变动率（%）
            }
        """
        result = {
            'should_trade': True,
            'reason': '',
            'premium_24h_change': None
        }
        
        try:
            # 解析建仓时间
            if ' ' in entry_datetime:
                try:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 转换为UTC毫秒时间戳
            entry_ts = int(calendar.timegm(entry_dt.timetuple()) * 1000)
            ts_24h_ago = entry_ts - 86400000  # 24小时前
            
            cursor = self.crypto_conn.cursor()
            
            # 查询建仓时的基差（最近1小时内）
            query_entry = """
            SELECT close
            FROM premium_index_history
            WHERE symbol = ?
              AND interval = '1h'
              AND open_time >= ?
              AND open_time <= ?
            ORDER BY open_time DESC
            LIMIT 1
            """
            cursor.execute(query_entry, (symbol, entry_ts - 3600000, entry_ts))
            entry_row = cursor.fetchone()
            
            if not entry_row:
                logging.debug(f"📊 {symbol} 无建仓时基差数据，跳过24h基差变动检查")
                return result
            
            entry_premium = float(entry_row[0])
            
            # 🆕 将建仓时基差保存到结果中（供CSV使用）
            result['entry_premium'] = entry_premium
            
            # 查询24小时前的基差（前后1小时范围）
            query_24h = """
            SELECT close
            FROM premium_index_history
            WHERE symbol = ?
              AND interval = '1h'
              AND open_time >= ?
              AND open_time <= ?
            ORDER BY ABS(open_time - ?) ASC
            LIMIT 1
            """
            cursor.execute(query_24h, (symbol, ts_24h_ago - 3600000, ts_24h_ago + 3600000, ts_24h_ago))
            ts_24h_row = cursor.fetchone()
            
            if not ts_24h_row:
                logging.debug(f"📊 {symbol} 无24h前基差数据，跳过24h基差变动检查")
                return result
            
            premium_24h_ago = float(ts_24h_row[0])
            
            # 🔧 优化：处理24h前基差为0的情况（2026-02-07）
            # 原逻辑：基差≈0时直接返回None，导致CSV显示"数据缺失"
            # 新逻辑：
            #   1. 如果24h前基差≈0，建仓时基差也≈0 → 变动率记为0%（无变化）
            #   2. 如果24h前基差≈0，建仓时基差>0 → 变动率记为9999%（巨大变化）
            #   3. 如果24h前基差≈0，建仓时基差<0 → 变动率记为-9999%（巨大变化）
            if abs(premium_24h_ago) < 1e-10:
                if abs(entry_premium) < 1e-10:
                    # 两个都≈0，记为无变化
                    premium_change_pct = 0.0
                    logging.info(
                        f"✅ {symbol} 24h基差变动0.00% "
                        f"(24h前≈0% → 建仓时≈0%，均接近0)"
                    )
                else:
                    # 24h前≈0，建仓时有值，记为极大变化（使用符号标识方向）
                    premium_change_pct = 9999.0 if entry_premium > 0 else -9999.0
                    logging.info(
                        f"✅ {symbol} 24h基差变动{premium_change_pct:.0f}% "
                        f"(24h前≈0% → 建仓时{entry_premium*100:.4f}%，巨大变化)"
                    )
                result['premium_24h_change'] = premium_change_pct
            else:
                # 正常计算24h基差变动率（%）
                premium_change_pct = ((entry_premium - premium_24h_ago) / abs(premium_24h_ago)) * 100
                result['premium_24h_change'] = premium_change_pct
            
            # 检查是否超过下跌阈值
            # 🔧 优化：如果变动率是特殊标记值(±9999)，跳过风控检查（2026-02-07修复）
            # 原因：±9999表示24h前基差≈0无法计算真实变动率，不应触发风控
            if abs(premium_change_pct) >= 9999:
                # 跳过风控：24h前基差≈0的情况无法准确判断风险
                logging.info(
                    f"⏭️  {symbol} 24h基差变动为特殊值{premium_change_pct:.0f}%，跳过风控检查 "
                    f"(24h前{premium_24h_ago*100:.4f}% → 建仓时{entry_premium*100:.4f}%)"
                )
            elif premium_change_pct < self.premium_24h_drop_threshold:
                result['should_trade'] = False
                result['reason'] = (
                    f"24h基差下跌{premium_change_pct:.2f}% < 阈值{self.premium_24h_drop_threshold:.1f}% "
                    f"(24h前{premium_24h_ago*100:.4f}% → 建仓时{entry_premium*100:.4f}%)，做空风险极高"
                )
                logging.warning(f"🚫 {symbol} {result['reason']}")
            else:
                # 正常情况：基差变动在安全范围内
                logging.info(
                    f"✅ {symbol} 24h基差变动{premium_change_pct:.2f}% 正常 "
                    f"(24h前{premium_24h_ago*100:.4f}% → 建仓时{entry_premium*100:.4f}%)"
                )
        
        except Exception as e:
            logging.debug(f"⚠️ {symbol} 计算24h基差变动失败: {e}")
            import traceback
            logging.debug(traceback.format_exc())
        
        return result
    
    def check_buy_volume_risk(self, symbol: str, signal_date: str, buy_surge_ratio: float) -> dict:
        """
        主动买量暴涨区间风控检查（已禁用）
        
        ⚠️  经过回测验证，买量区间风控误伤严重，已禁用：
        - 策略核心就是基于买量暴涨，拦截特定倍数区间会误伤盈利交易
        - 拦截2.0-2.5倍和>15倍导致收益从+226%降到+49%
        - 被拦截的25笔交易中，很多是盈利的
        
        保留此函数以备未来优化（如结合其他指标再启用）
        
        Args:
            symbol: 交易对
            signal_date: 信号日期
            buy_surge_ratio: 买量暴涨倍数
        
        Returns:
            dict: {'should_trade': bool, 'reason': str}
        """
        result = {
            'should_trade': True,  # 🔥 始终返回True，禁用此风控
            'reason': ''
        }
        
        # 保留日志以便追踪
        logging.debug(f"🔍 {symbol} 买量暴涨{buy_surge_ratio:.1f}倍（买量风控已禁用）")
        
        return result
    
    def check_buy_acceleration_risk(self, symbol: str, entry_datetime: str) -> dict:
        """
        买量加速度风控检查（参考backtrade8.py）
        
        风控逻辑：
        1. 获取建仓前24小时的小时K线数据
        2. 计算买量加速度 = 最后6小时平均买卖比 - 前18小时平均买卖比
        3. 如果买量加速度在 [0.118, 0.12) 区间，拒绝建仓
        
        风控原理：
        - 买量加速度过高（>0.12）：极端看涨，可能已经到顶
        - 买量加速度适中（0.118-0.12）：危险区间，容易反转 ⚠️
        - 买量加速度正常（<0.118）：相对安全
        
        Args:
            symbol: 交易对
            entry_datetime: 建仓时间 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD HH:MM'
        
        Returns:
            dict: {'should_trade': bool, 'reason': str, 'buy_acceleration': float}
        """
        result = {
            'should_trade': True,
            'reason': '',
            'buy_acceleration': 0.0
        }
        
        try:
            # 解析建仓时间（添加 UTC 时区）
            if ' ' in entry_datetime:
                try:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 计算24小时前的时间
            start_dt = entry_dt - timedelta(hours=24)
            
            # 获取24小时小时K线数据
            hourly_df = self.get_hourly_kline_data(
                symbol,
                start_date=start_dt.strftime('%Y-%m-%d'),
                end_date=entry_dt.strftime('%Y-%m-%d')
            )
            
            if hourly_df.empty:
                logging.debug(f"📊 {symbol} 无小时K线数据，跳过买量加速度风控")
                return result
            
            # 筛选建仓前24小时的数据
            hourly_df['open_datetime'] = pd.to_datetime(hourly_df['open_time'], unit='ms', utc=True)
            mask = (hourly_df['open_datetime'] >= start_dt) & (hourly_df['open_datetime'] < entry_dt)
            df = hourly_df[mask].copy()
            
            if len(df) < 12:
                result['reason'] = '数据不足(<12小时)'
                logging.debug(f"📊 {symbol} {result['reason']}")
                return result
            
            # 计算主动卖量
            df['volume'] = df['volume'].astype(float)
            df['active_buy_volume'] = df['active_buy_volume'].astype(float)
            df['active_sell_volume'] = df['volume'] - df['active_buy_volume']
            
            # 计算买卖比
            df['buy_sell_ratio'] = df['active_buy_volume'] / (df['active_sell_volume'] + 1e-10)
            
            # 计算买量加速度（最后6小时 vs 前18小时）
            last_6h = df.iloc[-6:] if len(df) >= 6 else df
            first_18h = df.iloc[:-6] if len(df) > 6 else df.iloc[:len(df)//2]
            
            last_6h_buy_ratio = last_6h['buy_sell_ratio'].mean()
            first_18h_buy_ratio = first_18h['buy_sell_ratio'].mean()
            
            buy_acceleration = last_6h_buy_ratio - first_18h_buy_ratio
            result['buy_acceleration'] = buy_acceleration
            
            # 🔥 使用配置参数判断多个危险区间
            for danger_min, danger_max in self.buy_accel_danger_ranges:
                if danger_min <= buy_acceleration <= danger_max:
                    result['should_trade'] = False
                    result['reason'] = f"买量加速度{buy_acceleration:.4f}在危险区间[{danger_min}, {danger_max}]，反转风险高"
                    logging.warning(f"🚫 {symbol} {result['reason']}")
                    return result
            
            # 如果不在任何危险区间内
            result['reason'] = f"买量加速度{buy_acceleration:.4f}正常"
            logging.info(f"✅ {symbol} 买量加速度{buy_acceleration:.4f}正常，可建仓")
            
        except Exception as e:
            logging.warning(f"⚠️ 计算买量加速度失败 {symbol}: {e}")
            import traceback
            logging.debug(traceback.format_exc())
        
        return result

    def check_consecutive_buy_ratio_risk(self, symbol: str, entry_datetime: str, 
                                        consecutive_hours: int = 2, threshold: float = 2.0) -> dict:
        """
        连续买量倍数风控检查（2026-02-03新增，2026-02-03优化）
        
        风控逻辑：
        1. 获取建仓前12小时的小时K线数据
        2. 计算每小时买量相对前一小时的倍数
        3. 检查是否存在连续N小时买量倍数都>阈值
        
        风控原理：
        - 一次性爆发（诱多）：买量瞬间暴涨后立即衰减 → 做空安全
        - 持续爆发（真突破）：买量连续2小时都>2倍 → 价格有持续上涨动能 → 危险！
        
        Args:
            symbol: 交易对
            entry_datetime: 建仓时间 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD HH:MM'
            consecutive_hours: 连续小时数（默认2，已从3降低）
            threshold: 倍数阈值（默认2.0）
        
        Returns:
            dict: {'should_trade': bool, 'reason': str, 'max_consecutive': int, 'ratios': list}
        """
        result = {
            'should_trade': True,
            'reason': '',
            'max_consecutive': 0,
            'ratios': []
        }
        
        try:
            # 解析建仓时间（添加 UTC 时区）
            if ' ' in entry_datetime:
                try:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                except ValueError:
                    entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
            else:
                entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 计算12小时前的时间
            start_dt = entry_dt - timedelta(hours=12)
            
            # 获取12小时小时K线数据
            hourly_df = self.get_hourly_kline_data(
                symbol,
                start_date=start_dt.strftime('%Y-%m-%d'),
                end_date=entry_dt.strftime('%Y-%m-%d')
            )
            
            if hourly_df.empty:
                logging.debug(f"📊 {symbol} 无小时K线数据，跳过连续买量倍数风控")
                return result
            
            # 筛选建仓前12小时的数据
            hourly_df['open_datetime'] = pd.to_datetime(hourly_df['open_time'], unit='ms', utc=True)
            mask = (hourly_df['open_datetime'] >= start_dt) & (hourly_df['open_datetime'] < entry_dt)
            df = hourly_df[mask].copy().sort_values('open_time')
            
            if len(df) < consecutive_hours + 1:
                result['reason'] = f'数据不足(<{consecutive_hours+1}小时)'
                logging.debug(f"📊 {symbol} {result['reason']}")
                return result
            
            # 计算主动买量
            df['active_buy_volume'] = df['active_buy_volume'].astype(float)
            
            # 计算每小时买量相对前一小时的倍数
            buy_ratios = []
            for i in range(1, len(df)):
                prev_buy = df.iloc[i-1]['active_buy_volume']
                curr_buy = df.iloc[i]['active_buy_volume']
                
                if prev_buy > 0:
                    ratio = curr_buy / prev_buy
                else:
                    ratio = 0.0
                
                buy_ratios.append(ratio)
            
            result['ratios'] = buy_ratios
            
            # 检查是否存在连续N小时买量倍数都>阈值
            max_consecutive = 0
            current_consecutive = 0
            consecutive_details = []
            
            for i, ratio in enumerate(buy_ratios):
                if ratio > threshold:
                    current_consecutive += 1
                    max_consecutive = max(max_consecutive, current_consecutive)
                    
                    # 记录连续区间
                    if current_consecutive == 1:
                        consecutive_details.append({
                            'start_idx': i,
                            'ratios': [ratio]
                        })
                    else:
                        consecutive_details[-1]['ratios'].append(ratio)
                else:
                    current_consecutive = 0
            
            result['max_consecutive'] = max_consecutive
            
            # 如果存在连续N小时都>阈值，拒绝建仓
            if max_consecutive >= consecutive_hours:
                # 找出触发的连续区间
                trigger_details = [d for d in consecutive_details if len(d['ratios']) >= consecutive_hours]
                if trigger_details:
                    detail = trigger_details[0]
                    ratios_str = ', '.join([f'{r:.2f}x' for r in detail['ratios'][:consecutive_hours]])
                    
                    result['should_trade'] = False
                    result['reason'] = (
                        f"连续{consecutive_hours}小时买量都>{threshold:.1f}倍 "
                        f"({ratios_str})，持续爆发动能=真突破风险高"
                    )
                    logging.warning(f"🚫 {symbol} {result['reason']}")
                    return result
            
            # 如果不存在连续超标
            result['reason'] = f"最大连续{max_consecutive}小时>{threshold:.1f}倍，未达到{consecutive_hours}小时阈值"
            logging.info(f"✅ {symbol} {result['reason']}，可建仓")
            
        except Exception as e:
            logging.warning(f"⚠️ 计算连续买量倍数失败 {symbol}: {e}")
            import traceback
            logging.debug(traceback.format_exc())
        
        return result

    def execute_trade(self, symbol: str, entry_price: float, entry_date: str, 
                     signal_date: str, buy_surge_ratio: float, position_type: str = "short", 
                     entry_datetime=None, theoretical_price: float = None, signal_account_ratio: float = None,
                     signal_price: float = None, signal_datetime: str = None):
        """执行交易 - 做空策略
        
        Args:
            entry_price: 实际建仓价（5分钟K线收盘价）
            entry_date: 实际建仓日期（回测执行建仓的日期）
            entry_datetime: 实际建仓时间戳（回测执行建仓的时刻），用于精确记录建仓时刻
            signal_date: 信号发生的日期
            signal_datetime: 信号发生的完整时间戳（信号小时的时间，如"2025-11-03 15:00:00"）
            theoretical_price: 理论建仓价（目标反弹价），用于止盈止损计算
            position_type: 仓位类型，默认"short"（做空）
            signal_account_ratio: 信号发生时的账户多空比
            signal_price: 信号发生时的价格
        """
        try:
            # 🔥 新增：持仓上限检查（最优先）
            if len(self.positions) >= self.max_daily_positions:
                logging.warning(f"⚠️ 持仓已满({len(self.positions)}/{self.max_daily_positions})，拒绝建仓: {symbol}")
                self._update_signal_record(symbol, signal_date, status='rejected_position_full',
                                          note=f'持仓已满{len(self.positions)}/{self.max_daily_positions}')
                return
            # 🔧 统一处理 entry_datetime：转换为 timezone-aware datetime 对象
            if entry_datetime is None:
                entry_datetime_obj = datetime.strptime(entry_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            elif isinstance(entry_datetime, str):
                if ' ' in entry_datetime:
                    try:
                        entry_datetime_obj = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    except ValueError:
                        entry_datetime_obj = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
                else:
                    entry_datetime_obj = datetime.strptime(entry_datetime, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            elif hasattr(entry_datetime, 'year'):
                # 已经是datetime对象
                if entry_datetime.tzinfo is None:
                    entry_datetime_obj = entry_datetime.replace(tzinfo=timezone.utc)
                else:
                    entry_datetime_obj = entry_datetime
            else:
                # 无法识别的类型，fallback
                entry_datetime_obj = datetime.strptime(entry_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 🆕 如果没有提供理论价格，使用实际价格
            if theoretical_price is None:
                theoretical_price = entry_price
            
            # 🔥 风控1：多空比风控（12月12日后启用，>4.1拒绝）【已屏蔽】
            # entry_datetime_str = entry_datetime.strftime('%Y-%m-%d %H:%M') if hasattr(entry_datetime, 'strftime') else str(entry_datetime)
            # long_short_risk = self.check_long_short_ratio_risk(symbol, entry_datetime_str)
            # if not long_short_risk['should_trade']:
            #     logging.warning(f"⚠️ {symbol} 多空比风控拒绝建仓: {long_short_risk['reason']}")
            #     self._update_signal_record(symbol, signal_date, status='rejected_long_short_ratio', note=long_short_risk['reason'])
            #     return
            
            # 🔧 爆仓保护：如果资金亏损超过80%，停止交易
            if self.capital <= self.initial_capital * self.min_capital_ratio:
                logging.warning(f"⚠️ 资金不足，停止交易: {symbol} 当前资金${self.capital:.2f} < 初始资金{self.min_capital_ratio*100:.0f}%")
                return
            
            # 🆕 剩余资金检查：计算已占用资金和剩余可用资金
            occupied_capital = sum(pos['position_value'] for pos in self.positions)
            available_capital = self.capital - occupied_capital
            
            # 💰 复利模式：基于当前资金余额的比例建仓（实现复利增长）
            base_position_value = self.capital * self.position_size_ratio
            
            # 🆕 剩余资金检查：确保有足够资金建新仓
            # 即使持仓数未达到上限，如果剩余资金不足也不建仓
            if available_capital < base_position_value:
                logging.warning(
                    f"⚠️ 剩余资金不足，拒绝建仓: {symbol} "
                    f"当前持仓{len(self.positions)}/{self.max_daily_positions}个，"
                    f"总资金${self.capital:.2f}，已占用${occupied_capital:.2f}，"
                    f"剩余${available_capital:.2f}，需要${base_position_value:.2f}"
                )
                self._update_signal_record(
                    symbol, signal_date, 
                    status='rejected_insufficient_capital',
                    note=f'剩余资金${available_capital:.2f} < 所需${base_position_value:.2f}，持仓{len(self.positions)}个'
                )
                return
            
            position_value = base_position_value
            
            # 检查当前资金是否足够建仓（兜底检查）
            if self.capital < position_value:
                logging.warning(f"⚠️ 资金不足，无法建仓: {symbol} 需要${position_value:.2f}，当前${self.capital:.2f}")
                return
            
            # 计算建仓数量 (考虑杠杆)
            position_size = (position_value * self.leverage) / entry_price
            
            # 🔧 优化：查找实际建仓的5分钟K线，记录精确时刻
            # 🔥 参考hm.py的做法：直接替换entry_datetime_str
            # 🔥🔥🔥 关键修复：应该使用theoretical_price（限价单价格），而不是entry_price（成交后的收盘价）
            # 🔥🔥🔥 关键修复：使用isoformat或手动格式化确保UTC时间（strftime可能受系统时区影响）
            entry_datetime_str = f"{entry_datetime_obj.year:04d}-{entry_datetime_obj.month:02d}-{entry_datetime_obj.day:02d} {entry_datetime_obj.hour:02d}:{entry_datetime_obj.minute:02d}"
            actual_entry_5m_time_str = self._find_actual_entry_5m_time(symbol, entry_datetime_str, theoretical_price)
            if actual_entry_5m_time_str:
                entry_datetime_str = actual_entry_5m_time_str  # 🔥 直接替换为实际5分钟建仓时刻
                logging.info(f"📍 {symbol} 实际建仓时刻: {entry_datetime_str} (限价单{theoretical_price:.6f}触及)")
            
            # 🆕 查询建仓时多空比
            entry_ls_ratio = None
            try:
                ls_result = self.check_long_short_ratio_risk(symbol, entry_datetime_str)
                if ls_result.get('ratio') is not None:
                    entry_ls_ratio = ls_result['ratio']
                    logging.debug(f"📊 {symbol} 建仓时多空比: {entry_ls_ratio:.2f}")
            except Exception as e:
                logging.debug(f"查询建仓时多空比失败 {symbol}: {e}")
            
            # 🆕 计算当日买量倍数（建仓前12小时，小时间买量最大比值）
            intraday_buy_ratio = 0.0
            try:
                intraday_buy_ratio = self.calculate_intraday_buy_surge_ratio(symbol, entry_datetime_str)
            except Exception as e:
                logging.debug(f"计算当日买量倍数失败 {symbol}: {e}")
            
            # 🆕 计算当日卖量倍数（建仓前12小时，小时间卖量最大比值）
            intraday_sell_ratio = 0.0
            try:
                intraday_sell_ratio = self.calculate_intraday_sell_surge_ratio(symbol, entry_datetime_str)
            except Exception as e:
                logging.debug(f"计算当日卖量倍数失败 {symbol}: {e}")
            
            # 🔥 风控1.8：当日卖量倍数区间风控（拦截危险区间）
            if intraday_sell_ratio > 0 and self.enable_intraday_sell_ratio_filter:
                for danger_min, danger_max in self.intraday_sell_ratio_danger_ranges:
                    if danger_min <= intraday_sell_ratio <= danger_max:
                        logging.warning(f"🚫 {symbol} 当日卖量倍数风控拒绝建仓: {intraday_sell_ratio:.2f}倍在危险区间[{danger_min}, {danger_max}]")
                        self._update_signal_record(symbol, signal_date, status='rejected_intraday_sell_ratio',
                                                  note=f'当日卖量倍数{intraday_sell_ratio:.2f}倍在危险区间[{danger_min}, {danger_max}]')
                        return
            
            # 🆕 获取信号时未平仓量
            signal_open_interest = 0.0
            try:
                signal_open_interest = self.get_open_interest_at_time(symbol, signal_date)
                time.sleep(0.15)  # API请求延迟
            except Exception as e:
                logging.debug(f"获取信号时未平仓量失败 {symbol}: {e}")
            
            # 🆕 获取建仓时未平仓量
            entry_open_interest = 0.0
            try:
                entry_open_interest = self.get_open_interest_at_time(symbol, entry_datetime_str)
                time.sleep(0.15)  # API请求延迟
            except Exception as e:
                logging.debug(f"获取建仓时未平仓量失败 {symbol}: {e}")
            
            # 🔥 风控1.5：未平仓量比率风控（拦截危险区间）
            if signal_open_interest > 0 and entry_open_interest > 0 and self.enable_oi_ratio_filter:
                oi_ratio = signal_open_interest / entry_open_interest
                for danger_min, danger_max in self.oi_ratio_danger_ranges:
                    if danger_min <= oi_ratio <= danger_max:
                        logging.warning(f"🚫 {symbol} 未平仓量比率风控拒绝建仓: {oi_ratio:.3f}在危险区间[{danger_min}, {danger_max}] (信号时${signal_open_interest:,.0f} / 建仓时${entry_open_interest:,.0f})")
                        self._update_signal_record(symbol, signal_date, status='rejected_oi_ratio',
                                                  note=f'未平仓量比率{oi_ratio:.3f}在危险区间[{danger_min}, {danger_max}]')
                        return
            
            # 🔥 风控2：当日买量倍数区间风控（拦截危险区间）
            if intraday_buy_ratio > 0 and self.enable_intraday_buy_ratio_filter:
                for danger_min, danger_max in self.intraday_buy_ratio_danger_ranges:
                    if danger_min <= intraday_buy_ratio <= danger_max:
                        logging.warning(f"⚠️ {symbol} 当日买量倍数风控拒绝建仓: {intraday_buy_ratio:.2f}倍在危险区间[{danger_min}, {danger_max}]")
                        self._update_signal_record(symbol, signal_date, status='rejected_intraday_buy_ratio', 
                                                  note=f'当日买量倍数{intraday_buy_ratio:.2f}倍在危险区间[{danger_min}, {danger_max}]')
                        return
            
            # 🆕 风控2.1：连续买量倍数风控（2026-02-03新增）
            if self.enable_consecutive_buy_ratio_filter:
                try:
                    consecutive_result = self.check_consecutive_buy_ratio_risk(
                        symbol, entry_datetime_str, 
                        self.consecutive_buy_ratio_hours,
                        self.consecutive_buy_ratio_threshold
                    )
                    
                    if not consecutive_result['should_trade']:
                        logging.warning(f"🚫 {symbol} 连续买量倍数风控拒绝建仓: {consecutive_result['reason']}")
                        self._update_signal_record(symbol, signal_date, status='rejected_consecutive_buy_ratio',
                                                  note=consecutive_result['reason'])
                        return
                except Exception as e:
                    logging.warning(f"⚠️ 计算连续买量倍数失败 {symbol}: {e}")
                    import traceback
                    logging.debug(traceback.format_exc())
            
            # 🔥 风控2.5：买卖量比值风控（拦截危险区间）
            if intraday_buy_ratio > 0 and intraday_sell_ratio > 0 and self.enable_buy_sell_ratio_filter:
                buy_sell_ratio = intraday_buy_ratio / intraday_sell_ratio
                for danger_min, danger_max in self.buy_sell_ratio_danger_ranges:
                    if danger_min <= buy_sell_ratio <= danger_max:
                        logging.warning(f"⚠️ {symbol} 买卖量比值风控拒绝建仓: {buy_sell_ratio:.3f}在危险区间[{danger_min}, {danger_max}]")
                        self._update_signal_record(symbol, signal_date, status='rejected_buy_sell_ratio', 
                                                  note=f'买卖量比值{buy_sell_ratio:.3f}在危险区间[{danger_min}, {danger_max}]')
                        return
            
            # 🔥 风控3：买量加速度风控（参考backtrade8.py）
            buy_acceleration = 0.0
            if self.enable_buy_accel_filter:  # 🔴 检查买量加速度风控开关
                try:
                    buy_accel_result = self.check_buy_acceleration_risk(symbol, entry_datetime_str)
                    buy_acceleration = buy_accel_result.get('buy_acceleration', 0.0)
                    
                    # 恢复风控判断
                    if not buy_accel_result['should_trade']:
                        logging.warning(f"⚠️ {symbol} 买量加速度风控拒绝建仓: {buy_accel_result['reason']}")
                        self._update_signal_record(symbol, signal_date, status='rejected_buy_acceleration', 
                                                  note=buy_accel_result['reason'])
                        return
                except Exception as e:
                    logging.warning(f"⚠️ 计算买量加速度失败 {symbol}: {e}")
                    import traceback
                    logging.debug(traceback.format_exc())
            
            # 🔥 风控4：多空比变化率风控（拦截危险区间，2025-12-12后启用）
            if signal_account_ratio and signal_account_ratio > 0 and self.enable_account_ratio_change_filter:
                # 解析建仓日期
                if hasattr(entry_datetime, 'year'):
                    entry_dt = entry_datetime
                elif ' ' in entry_datetime_str:
                    try:
                        entry_dt = datetime.strptime(entry_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    except ValueError:
                        entry_dt = datetime.strptime(entry_datetime_str, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
                else:
                    entry_dt = datetime.strptime(entry_datetime_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                
                # 🔥 仅在2025-12-12之后启用（之前没有顶级交易者数据）
                if entry_dt >= self.wind_control_start_date:
                    # 获取建仓时的账户多空比
                    trader_data = self.get_top_trader_account_ratio(symbol, entry_dt)
                    if trader_data:
                        entry_account_ratio_temp = trader_data['long_short_ratio']
                        account_ratio_change_rate = entry_account_ratio_temp / signal_account_ratio
                        
                        for danger_min, danger_max in self.account_ratio_change_danger_ranges:
                            if danger_min <= account_ratio_change_rate <= danger_max:
                                logging.warning(f"🚫 {symbol} 多空比变化率风控拒绝建仓: {account_ratio_change_rate:.3f}在危险区间[{danger_min}, {danger_max}] (信号时{signal_account_ratio:.4f} → 建仓时{entry_account_ratio_temp:.4f})")
                                self._update_signal_record(symbol, signal_date, status='rejected_account_ratio_change',
                                                          note=f'多空比变化率{account_ratio_change_rate:.3f}在危险区间[{danger_min}, {danger_max}]')
                                return
                else:
                    logging.debug(f"📊 {symbol} 多空比变化率风控未启用（12月12日前无数据）")
            
            # 🔥 风控5：建仓涨幅风控（2026-02-04新增）
            if self.enable_entry_gain_filter and signal_price and signal_price > 0:
                # 计算建仓涨幅（建仓价相对信号价的涨幅百分比）
                entry_gain_pct = ((entry_price - signal_price) / signal_price) * 100
                
                # 检查是否在允许区间内
                if entry_gain_pct < self.entry_gain_min or entry_gain_pct > self.entry_gain_max:
                    logging.warning(
                        f"🚫 {symbol} 建仓涨幅风控拒绝建仓: {entry_gain_pct:.2f}% 不在区间"
                        f"[{self.entry_gain_min:.2f}%, {self.entry_gain_max:.2f}%] "
                        f"(信号价{signal_price:.6f} → 建仓价{entry_price:.6f})"
                    )
                    self._update_signal_record(
                        symbol, signal_date, 
                        status='rejected_entry_gain',
                        note=f'建仓涨幅{entry_gain_pct:.2f}%不在区间[{self.entry_gain_min:.2f}%, {self.entry_gain_max:.2f}%]'
                    )
                    return
                else:
                    logging.info(
                        f"✅ {symbol} 建仓涨幅检查通过: {entry_gain_pct:.2f}% "
                        f"在区间[{self.entry_gain_min:.2f}%, {self.entry_gain_max:.2f}%]"
                    )
            
            # 🔥 风控6：24h基差变动风控（2026-02-04新增）
            premium_24h_change = None
            entry_premium = None  # 🆕 建仓时基差
            if self.enable_premium_change_filter:
                try:
                    premium_change_result = self.check_premium_24h_change_risk(symbol, entry_datetime_str)
                    premium_24h_change = premium_change_result.get('premium_24h_change')
                    entry_premium = premium_change_result.get('entry_premium')  # 🆕 获取建仓时基差
                    
                    if not premium_change_result['should_trade']:
                        logging.warning(f"🚫 {symbol} 24h基差变动风控拒绝建仓: {premium_change_result['reason']}")
                        self._update_signal_record(
                            symbol, signal_date, 
                            status='rejected_premium_24h_drop',
                            note=premium_change_result['reason']
                        )
                        return
                except Exception as e:
                    logging.warning(f"⚠️ {symbol} 计算24h基差变动失败: {e}")
            
            # 🆕 即使基差风控未启用，也获取建仓时基差（用于CSV报告）
            if entry_premium is None:
                try:
                    premium_check = self.check_premium_index_risk(symbol, entry_datetime_str)
                    entry_premium = premium_check.get('premium')  # 获取建仓时基差
                except Exception as e:
                    logging.debug(f"⚠️ {symbol} 获取建仓时基差失败: {e}")
                    import traceback
                    logging.debug(traceback.format_exc())
            
            # 🔧 处理 signal_datetime：转换为字符串格式
            signal_datetime_str = None
            if signal_datetime is not None:
                if isinstance(signal_datetime, str):
                    signal_datetime_str = signal_datetime
                elif hasattr(signal_datetime, 'strftime'):
                    signal_datetime_str = signal_datetime.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # 如果无法识别，使用signal_date作为备用
                    signal_datetime_str = signal_date + ' 00:00:00'
            else:
                # 如果没有提供signal_datetime，使用signal_date作为备用
                signal_datetime_str = signal_date + ' 00:00:00'
            
            # 💰 计算建仓手续费（Maker费率：限价单）
            entry_trading_value = position_value * self.leverage  # 成交金额 = 保证金 × 杠杆
            entry_fee = self.calculate_trading_fee(entry_trading_value)
            logging.info(f"💳 建仓手续费: ${entry_fee:.4f}（成交金额${entry_trading_value:.2f} × {self.trading_fee_rate*100:.2f}%）")
            
            # 记录交易
            trade_record = {
                'entry_date': entry_date,
                'entry_datetime': entry_datetime_str,  # ✅ 实际建仓时间（回测执行建仓的时刻）
                'symbol': symbol,
                'entry_price': entry_price,  # 实际建仓价（5分钟收盘价）
                'theoretical_entry_price': theoretical_price,  # 🆕 理论建仓价（用于止盈止损计算）
                'position_size': position_size,
                'position_value': position_value,
                'base_position_value': base_position_value,  # 🆕 记录未补偿的基础金额（用于计算real_pnl）
                'leverage': self.leverage,
                'position_type': position_type,
                'exit_date': None,
                'exit_price': None,
                'exit_reason': None,
                'pnl': 0,
                'pnl_pct': 0,
                'status': 'normal',
                'weak_24h_exit_price': None,
                'weak_24h_pnl': None,
                'avg_entry_price': entry_price,
                'signal_date': signal_date,
                'signal_datetime': signal_datetime_str,  # ✅ 信号发生的完整时间（信号小时的时间戳）
                'signal_price': signal_price,  # 🆕 信号发生时的价格
                'buy_surge_ratio': buy_surge_ratio,  # 买量暴涨倍数
                'intraday_buy_ratio': intraday_buy_ratio,  # 🆕 当日买量倍数（建仓前12小时小时间最大比值）
                'intraday_sell_ratio': intraday_sell_ratio,  # 🆕 当日卖量倍数（建仓前12小时小时间最大比值）
                'buy_acceleration': buy_acceleration,  # 🆕 买量加速度（最后6h vs 前18h买卖比差值）
                'signal_open_interest': signal_open_interest,  # 🆕 信号时未平仓量（美元价值）
                'entry_open_interest': entry_open_interest,  # 🆕 建仓时未平仓量（美元价值）
                'premium_24h_change': premium_24h_change,  # 🆕 24h基差变动率（%）（2026-02-04新增）
                'entry_premium': entry_premium,  # 🆕 建仓时基差（Premium Index）（2026-02-07新增）
                
                # 🆕 添加：顶级交易者数据字段
                'signal_account_ratio': signal_account_ratio,  # 🆕 信号时账户多空比
                'entry_account_ratio': entry_ls_ratio,  # 🆕 建仓时账户多空比（实际查询值）
                'account_ratio_change_rate': None,  # 🆕 多空比变化率（建仓时/信号时）
                'current_account_ratio': None,  # 当前账户多空比（已废弃，不再使用）
                'account_ratio_change': None,  # 账户多空比变化（已废弃，不再使用）
                
                'max_drawdown': 0,
                'max_up_2h': None,  # 🆕 建仓后2小时最大涨幅（ratio，用于分析）
                'max_up_24h': None,  # 🆕 建仓后24小时最大涨幅（ratio，用于分析）
                'hold_days': 0,

                # 动态止盈相关（用于后续分析 + CSV输出）
                # - dynamic_tp_pct: 本次交易"最终使用的动态止盈阈值"（会缓存）
                # - dynamic_tp_strong: 是否被判定为"强势币"（True=强势33%，False=其他）
                # - dynamic_tp_medium: 是否被判定为"中等币"（True=中等21%）
                # - dynamic_tp_weak: 是否被判定为"弱势币"（True=弱势9%）
                # - dynamic_tp_boost_used: 强势时实际使用的加成幅度（按买量暴涨倍数分档）
                # - tp_pct_used: 本次实际触发止盈时使用的阈值（仅在 take_profit 平仓时写入）
                'dynamic_tp_pct': None,
                'dynamic_tp_strong': None,
                'dynamic_tp_medium': None,
                'dynamic_tp_weak': None,
                'dynamic_tp_boost_used': None,
                'dynamic_tp_above_cnt': None,
                'dynamic_tp_total_cnt': None,
                'tp_pct_used': None,
                # 🆕 连续爆判断：是否为连续2小时卖量暴涨信号
                'is_consecutive_surge_at_entry': False,
                
                # 💰 交易手续费字段（2026-02-14新增）
                'entry_fee': entry_fee,  # 建仓手续费
                'exit_fee': 0.0,  # 平仓手续费（平仓时填充）
                'total_trading_fee': entry_fee,  # 总交易手续费（初始=建仓费，补仓和平仓时会累加）
            }
            
            # 🆕 添加：获取并保存建仓时的账户多空比（始终获取，用于分析）
            trader_data = self.get_top_trader_account_ratio(symbol, entry_datetime_obj)
            if trader_data:
                trade_record['entry_account_ratio'] = trader_data['long_short_ratio']
                logging.info(f"📊 {symbol} 建仓时账户多空比: {trader_data['long_short_ratio']:.4f}")
                
                # 🆕 计算多空比变化率（建仓时/信号时）
                if signal_account_ratio and signal_account_ratio > 0:
                    trade_record['account_ratio_change_rate'] = trade_record['entry_account_ratio'] / signal_account_ratio
                    logging.info(f"📊 {symbol} 多空比变化: 信号时{signal_account_ratio:.4f} → 建仓时{trade_record['entry_account_ratio']:.4f} = {trade_record['account_ratio_change_rate']:.4f}x")
            
            # 🔒 保险：写入持仓前再次检查并发上限（入口检查与 append 之间间隔大量逻辑，防回归/异常路径导致超限）
            if len(self.positions) >= self.max_daily_positions:
                logging.error(
                    f"🚫 【持仓上限·append前保险】拒绝建仓 {symbol}：当前持仓{len(self.positions)}/{self.max_daily_positions}，"
                    f"与 execute_trade 入口状态不一致，已丢弃本笔 trade_record（未扣款、未入 trade_records）。"
                )
                self._update_signal_record(
                    symbol,
                    signal_date,
                    status='rejected_position_full',
                    note=f'append前保险：持仓已满{len(self.positions)}/{self.max_daily_positions}',
                )
                return
            
            self.positions.append(trade_record)
            self.trade_records.append(trade_record)
            
            # 🆕 检查是否为连续2小时卖量暴涨信号（用于12小时动态止盈保护）
            is_consecutive = self._check_consecutive_surge_at_entry(trade_record)
            trade_record['is_consecutive_surge_at_entry'] = is_consecutive
            if is_consecutive:
                logging.info(f"✅ {symbol} 标记为连续2小时卖量暴涨信号")
            
            # 💰 复利模式：建仓时扣除投入资金
            self.capital -= position_value
            
            logging.info(f"🚀 建仓: {symbol} {entry_date} 价格:{entry_price:.4f} 卖量暴涨:{buy_surge_ratio:.1f}倍 杠杆:{self.leverage}x 仓位:${position_value:.2f} 剩余资金:${self.capital:.2f}")
            
            # 🔥🔥🔥 关键修复：建仓后立即检查当天的止损止盈
            # 原因：如果建仓当天就触发止损（如价格继续上涨），必须立即检查，不能等到第二天
            # 例如：11-01 07:00建仓，15:00触发止损，应该当天平仓
            try:
                logging.info(f"🔥 建仓后立即检查止损止盈: {symbol}")
                self.check_exit_conditions(trade_record, 0, entry_date)
            except Exception as e:
                logging.error(f"❌ 建仓后检查失败 {symbol}: {e}")
                import traceback
                logging.error(traceback.format_exc())
        except Exception as e:
            logging.error(f"执行交易失败: {e}")
            import traceback
            logging.error(f"错误详情:\n{traceback.format_exc()}")

    def check_exit_conditions(self, position: Dict, current_price: float, current_date: str) -> bool:
        """使用小时线数据检查是否满足平仓条件
        
        支持虚拟跟踪模式：
        - is_virtual_tracking=True时，使用virtual_entry_price判断止盈/止损
        - 虚拟平仓不影响资金（真实仓位已经止损清仓）
        - 虚拟平仓后，释放槽位
        """
        try:
            symbol = position['symbol']
            logging.debug(f"🔍 检查 {symbol} 平仓条件，当前日期={current_date}, max_hold_hours={self.max_hold_hours}")
            
            # 🆕 止盈止损基准价
            tp_sl_base_price = position['avg_entry_price']
            
            # ⚠️ 时间相关计算仍使用原始entry_date（首次建仓日期）
            entry_date = position['entry_date']
            
            # 获取小时线数据（优化：只查询建仓日到当前日的数据）
            hourly_df = self.get_hourly_kline_data(symbol, start_date=entry_date, end_date=current_date)
            logging.info(f"  📊 {symbol} 查询小时K线: entry_date={entry_date}, current_date={current_date}, 数据行数={len(hourly_df)}")
            
            if hourly_df.empty:
                logging.warning(f"⚠️⚠️⚠️ 无小时线数据: {symbol} (建仓日:{entry_date}, 当前日:{current_date})")
                # 🔧 添加调试信息：检查表是否存在
                try:
                    cursor = self.crypto_conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM HourlyKline_{symbol}")
                    total_count = cursor.fetchone()[0]
                    logging.warning(f"  📊 表 HourlyKline_{symbol} 共有 {total_count} 条数据")
                except Exception as e:
                    logging.warning(f"  ❌ 表 HourlyKline_{symbol} 不存在或查询失败: {e}")
                
                # 🔧🔧🔧 关键修复：即使没有小时数据，也要检查72小时强制平仓！
                # 计算持仓时间
                if position.get('entry_datetime'):
                    entry_datetime_temp = pd.to_datetime(position['entry_datetime'], utc=True)
                    if pd.isna(entry_datetime_temp):
                        entry_datetime = datetime.strptime(entry_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    else:
                        entry_datetime = entry_datetime_temp.to_pydatetime() if hasattr(entry_datetime_temp, 'to_pydatetime') else entry_datetime_temp
                        if isinstance(entry_datetime, datetime) and entry_datetime.tzinfo is None:
                            entry_datetime = entry_datetime.replace(tzinfo=timezone.utc)
                else:
                    entry_datetime = datetime.strptime(entry_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                
                current_datetime = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)).replace(tzinfo=timezone.utc)
                hours_held = (current_datetime - entry_datetime).total_seconds() / 3600
                
                if hours_held >= self.max_hold_hours:
                    # 🔧 72小时强制平仓（无小时数据时的备用逻辑）
                    # ✅ 修复：从日K线获取当天收盘价，而不是使用建仓价
                    exit_price = None
                    try:
                        # 从日K线获取当天收盘价
                        day_start = datetime.strptime(current_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        day_start_ts = int(day_start.timestamp() * 1000)
                        day_end_ts = day_start_ts + 86400000
                        daily_cursor = self.crypto_conn.execute(f"""
                            SELECT close FROM DailyKline_{symbol}
                            WHERE open_time >= ? AND open_time < ?
                        """, (day_start_ts, day_end_ts))
                        daily_row = daily_cursor.fetchone()
                        if daily_row and daily_row[0] is not None:
                            exit_price = float(daily_row[0])
                            logging.info(f"📊 {symbol} 从日K线获取平仓价: {exit_price:.6f}")
                    except Exception as e:
                        logging.warning(f"⚠️ {symbol} 无法从日K线获取平仓价: {e}")
                    
                    # 如果日K线也没有数据，使用current_price（虽然可能是0，但不应该用建仓价）
                    if exit_price is None:
                        exit_price = current_price if current_price > 0 else position['avg_entry_price']
                        logging.warning(f"⚠️ {symbol} 日K线无数据，使用备用价格: {exit_price:.6f}")
                    
                    # ❌ BUG修复（2026-02-05）：删除72小时后的止损/止盈判断
                    exit_reason = "max_hold_time"
                    
                    logging.warning(f"⏰⏰⏰ 72小时强制平仓（无小时数据）: {symbol} 持仓{hours_held:.1f}h，平仓价{exit_price:.6f}，原因{exit_reason}")
                    self.exit_position(position, exit_price, current_date + ' 23:59:59', exit_reason)
                    return True
                
                # 备用：使用日线数据（无法使用动态止盈，使用默认阈值）
                price_change_pct = (current_price - tp_sl_base_price) / tp_sl_base_price
                if price_change_pct >= self.take_profit_pct:
                    position['tp_pct_used'] = self.take_profit_pct
                    self.exit_position(position, current_price, current_date, "take_profit")
                    return True
                return False
            
            # 筛选建仓时刻之后的所有小时数据
            # 🔧 修复：使用保存的完整建仓时间戳，而不是只用日期
            # ⚠️ 关键修复：虚拟跟踪模式下，仍使用原始entry_datetime计算持仓时长、动态止盈等
            # 只有止盈止损价格使用virtual_entry_price
            if position.get('entry_datetime'):
                # 如果有完整的建仓时间戳，使用它
                #🔧 关键修复：解析时指定utc=True，确保时区一致
                entry_datetime_temp = pd.to_datetime(position['entry_datetime'], utc=True)
                if pd.isna(entry_datetime_temp):
                    # 如果转换失败，使用日期
                    entry_datetime = datetime.strptime(entry_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                else:
                    entry_datetime = entry_datetime_temp.to_pydatetime() if hasattr(entry_datetime_temp, 'to_pydatetime') else entry_datetime_temp
                    # 🔧 优化：统一确保timezone-aware（只判断一次）
                    if isinstance(entry_datetime, datetime) and entry_datetime.tzinfo is None:
                        entry_datetime = entry_datetime.replace(tzinfo=timezone.utc)
            else:
                # 向后兼容：如果没有时间戳，使用日期（旧数据）
                entry_datetime = datetime.strptime(entry_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            # 🔥 提取entry_date_only（用于5分钟检查）
            if ' ' in entry_date:
                entry_date_only = entry_date.split()[0]  # 提取日期部分
            else:
                entry_date_only = entry_date
            
            # 🔧 关键修复：将 current_date 设置为当天23:59:59，避免排除当天的小时数据
            current_datetime = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)).replace(tzinfo=timezone.utc)
            
            # 🔧🔧🔧 重要：直接使用 open_time（毫秒时间戳）筛选，不转换为 datetime
            # 原因：减少转换步骤，降低出错风险（项目规则：统一用 open_time）
            entry_hour_start = entry_datetime.replace(minute=0, second=0, microsecond=0)
            entry_hour_start_ms = int(entry_hour_start.timestamp() * 1000)
            current_datetime_ms = int(current_datetime.timestamp() * 1000)
            
            logging.info(f"🔧🔧🔧 {symbol} entry_datetime={entry_datetime}, entry_hour_start={entry_hour_start}")
            mask = (hourly_df['open_time'] >= entry_hour_start_ms) & (hourly_df['open_time'] <= current_datetime_ms)
            hold_period_data = hourly_df[mask].copy()
            logging.debug(f"📊 {symbol} hold_period_data获取完成: {len(hold_period_data)}行 (建仓小时:{entry_hour_start}, 当前:{current_datetime})")

            # 🆕 计算"建仓后2小时最大涨幅%"（用于分析：为何动态止盈触发少）
            # 口径：2小时内最高价(high)相对"建仓价(entry_price)"的涨幅
            # - 用建仓价而不是补仓后平均价
            # - 若2小时内最高价未高于建仓价，则记为0
            if position.get('max_up_2h') is None:
                try:
                    entry_price0 = float(position.get('entry_price') or position.get('avg_entry_price') or 0)
                    if entry_price0 > 0:
                        entry_datetime_ms = int(entry_datetime.timestamp() * 1000)
                        window_end_dt = entry_datetime + timedelta(hours=2)
                        window_end_ms = int(window_end_dt.timestamp() * 1000)
                        wmask = (hourly_df['open_time'] >= entry_datetime_ms) & (hourly_df['open_time'] < window_end_ms)
                        wdf = hourly_df[wmask]
                        if not wdf.empty and 'high' in wdf.columns:
                            max_high = wdf['high'].max()
                            if pd.notna(max_high):
                                up_ratio = (float(max_high) - entry_price0) / entry_price0
                                position['max_up_2h'] = max(0.0, float(up_ratio))
                            else:
                                position['max_up_2h'] = None
                        else:
                            position['max_up_2h'] = None
                    else:
                        position['max_up_2h'] = None
                except Exception:
                    position['max_up_2h'] = None

            # 🔧 注释：24h收盘价涨幅计算和检查已移到小时K线循环内（3873-3927行）
            # 这里不再提前计算，避免跳过止损检查
            
            # 获取建仓时的具体小时时间戳（用于精确计算持仓小时数）
            entry_hour_timestamp = None
            if not hold_period_data.empty:
                try:
                    # 从 open_time 转换为 datetime
                    entry_hour_timestamp = pd.to_datetime(hold_period_data.iloc[0]['open_time'], unit='ms', utc=True)
                    logging.info(f"🕒 {symbol} hold_period_data有{len(hold_period_data)}行，建仓时间={entry_hour_timestamp}, max_hold_hours={self.max_hold_hours}")
                except Exception as ex:
                    logging.error(f"获取entry_hour_timestamp失败: {ex}")
            else:
                # 🔧 关键修复：如果没有小时数据，使用entry_datetime作为建仓时间戳
                entry_hour_timestamp = entry_datetime
                logging.warning(f"⚠️ {symbol} 没有小时K线数据，使用建仓日期时间作为时间戳: {entry_datetime}")
            
            # ❌ 已禁用（2026-02-07）：72小时强制平仓检查 - 移动到小时循环内（4034-4067行）
            # 原因：
            #   1. 这里的检查使用 current_datetime（当天23:59:59），可能超过72小时但还没到
            #   2. 提前return True会导致小时循环（3481行）永远不执行，止盈/止损永远不触发
            #   3. 例如：KSMUSDT建仓2025-11-05 05:00，当前日期2025-11-08
            #      - hours_held = (2025-11-08 23:59:59 - 2025-11-05 05:00) ≈ 91h >= 72h ✅ 触发
            #      - 但第70小时(2025-11-08 02:00)价格已涨超-18%，应该止损
            #      - 由于这里提前return，止损永远不会执行
            # 修复方案：
            #   - 删除这里的72小时检查
            #   - 只保留小时循环内的72小时检查（4034-4067行），作为兜底机制
            
            # if entry_hour_timestamp:
            #     hours_held = (current_datetime - entry_hour_timestamp).total_seconds() / 3600
            #     if hours_held >= self.max_hold_hours:
            #         # ... 72小时强制平仓逻辑 ...
            #         return True
            
            # 检查每小时的价格是否满足止盈/补仓/止损条件
            if not hold_period_data.empty:
                for idx, row in hold_period_data.iterrows():
                    high_price = row['high']
                    low_price = row['low']
                    # 从 open_time 转换为 datetime
                    hour_datetime = pd.to_datetime(row['open_time'], unit='ms', utc=True)
                    hour_date = hour_datetime.strftime('%Y-%m-%d')
                    hour_datetime_str = hour_datetime.strftime('%Y-%m-%d %H:%M:%S')  # 🆕 完整的日期时间字符串
                    
                    
                    # 🎯 获取止盈止损基准价（优先理论价格，其次虚拟价格，最后实际价格）
                    # 🎯 确定当前平均价格（用于止盈止损计算）
                    current_avg_price = position['avg_entry_price']

                    
                    # 更新最大跌幅（做空策略：价格下跌是正数，价格上涨是负数）
                    # 修正公式：(建仓价 - 最低价) / 建仓价，价格下跌时为正
                    drawdown_pct = (current_avg_price - low_price) / current_avg_price
                    # 记录最大值（最大跌幅应该是最大的正数）
                    if drawdown_pct > position['max_drawdown']:
                        position['max_drawdown'] = drawdown_pct

                    # ==========================
                    # 🧠 回测执行价与顺序（避免“未来函数/过度乐观”）
                    # - 同一根小时K线里，high/low 只用来判断“是否触发”，成交价使用“阈值价”而不是直接用 high/low
                    # - 当同一根K线同时触发止损与止盈时，按“先止损后止盈”（更保守）
                    # ==========================
                    # 🆕 动态止盈阈值（避免"偷看未来"：只有窗口走完才允许触发动态加成）
                    dynamic_tp_pct = self.calculate_dynamic_take_profit(position, hourly_df, entry_datetime, hour_datetime)
                    
                    # 🚨 检查是否触发2h提前止损
                    if dynamic_tp_pct == -0.999:
                        # 触发2h提前止损，立即平仓
                        exit_price = row['close']
                        self.exit_position(position, exit_price, hour_datetime_str, "early_stop_loss")
                        logging.warning(
                            f"🚨🚨🚨 2h提前止损: {symbol} 价格涨幅超过阈值，立即平仓！\n"
                            f"  • 平仓价：{exit_price:.6f}\n"
                            f"  • 建仓价：{current_avg_price:.6f}\n"
                            f"  • 预计亏损：{(exit_price - current_avg_price) / current_avg_price * self.leverage * 100:.2f}%"
                        )
                        return True
                    
                    # ⚠️ 12h提前止损返回值 -0.998 已废弃（2026-02-04 删除）
                    # 原因：calculate_dynamic_take_profit 中的时间窗口不准确，导致误触发
                    # 统一使用 check_exit_conditions 中的小时K线检查（3392-3407行）
                    
                    # 计算止盈/止损价格（做空策略：价格下跌盈利，价格上涨亏损）
                    # 做空：止盈价格 = 建仓价 * (1 - 止盈百分比)，价格跌到这个位置止盈
                    # 做空：止损价格 = 建仓价 * (1 + |止损百分比|)，价格涨到这个位置止损
                    tp_price = current_avg_price * (1 - dynamic_tp_pct / 100)  # dynamic_tp_pct是百分比数值（如33.0）
                    sl_price = current_avg_price * (1 - self.stop_loss_pct / 100)  # stop_loss_pct=-18.0，所以1-(-18/100)=1.18

                    
                    # 🆕 24小时弱势平仓检查（已禁用）
                    # 之前的弱势平仓逻辑已移除

                    
                    # 🔧🔧🔧 关键修复：参考hm.py，在建仓小时内逐个检查5分钟K线
                    # 只在建仓当天、建仓小时内使用5分钟K线精确检查
                    # 🔥 修复：检查entry_datetime（完整时间戳），而不是entry_date（只有日期）
                    entry_has_time = position.get('entry_datetime') and ' ' in str(position.get('entry_datetime', ''))
                    # 🔥🔥🔥 关键修复：比较小时时间戳，而不是日期！
                    # 只有当前小时等于建仓小时时，才进入5分钟检查
                    entry_hour = entry_datetime.replace(minute=0, second=0, microsecond=0) if hasattr(entry_datetime, 'replace') else None
                    current_hour = hour_datetime.replace(minute=0, second=0, microsecond=0) if hasattr(hour_datetime, 'replace') else None
                    
                    #🔧 DEBUG：输出时间对比
                    logging.info(f"🔍🔍🔍 {symbol} idx={idx}, entry_has_time={entry_has_time}, entry_hour={entry_hour}, current_hour={current_hour}, 相等={current_hour == entry_hour}")
                    
                    if entry_has_time and entry_hour and current_hour and current_hour == entry_hour:
                        logging.debug(f"🔍 {symbol} 进入5分钟检查: hour_date={hour_date}, entry_date_only={entry_date_only}")
                        # entry_datetime已经是精确的5分钟建仓时刻（由execute_trade记录）
                        entry_5m_time = entry_datetime  # datetime对象
                        entry_hour_end = hour_datetime + timedelta(hours=1)
                        
                        logging.debug(f"🔍 {symbol} 建仓5分钟时刻: {entry_5m_time}, 建仓小时结束: {entry_hour_end}")
                        
                        # 查询当前小时的5分钟K线
                        kline_5m_df = self.get_5min_kline_data(symbol)
                        if not kline_5m_df.empty:
                            kline_5m_df['open_datetime'] = pd.to_datetime(kline_5m_df['open_time'], unit='ms', utc=True)
                            
                            # 🔥🔥🔥 关键修复：从建仓那根5分钟K线开始检查（包含建仓K线）
                            # 假设：建仓后立即可以检测止盈/止损（乐观假设）
                            # 例：13:10建仓 → 从13:10~13:15（建仓K线）开始检查
                            # 注意：这是乐观假设，实际可能会导致回测结果偏好
                            mask_after_entry = (kline_5m_df['open_datetime'] >= entry_5m_time) & \
                                              (kline_5m_df['open_datetime'] < entry_hour_end)
                            after_entry_5m_data = kline_5m_df[mask_after_entry].copy()
                            
                            # 🔥 如果包含建仓K线，需要排除第一根（就是建仓K线本身）
                            if not after_entry_5m_data.empty and len(after_entry_5m_data) > 0:
                                # 跳过第一根（建仓K线），从第二根开始
                                after_entry_5m_data = after_entry_5m_data.iloc[1:]
                            
                            if not after_entry_5m_data.empty:
                                logging.debug(
                                    f"📍 {symbol} 建仓小时有{len(after_entry_5m_data)}根5分钟K线 "
                                    f"(建仓5分钟={entry_5m_time.strftime('%H:%M')})"
                                )
                                
                                # 🔥🔥🔥 参考hm.py：逐个5分钟K线检查
                                current_avg_price = position['avg_entry_price']
                                has_add_in_5m = False  # 标记是否在5分钟检查中补仓了
                                
                                for _, row_5m in after_entry_5m_data.iterrows():
                                    high_5m = float(row_5m['high'])
                                    low_5m = float(row_5m['low'])
                                    close_5m = float(row_5m['close'])
                                    time_5m = row_5m['open_datetime']
                                    time_5m_str = time_5m.strftime('%Y-%m-%d %H:%M')
                                    
                                    # 先检查止盈（做空：基于最低价）
                                    # 🔥 修复：传递正确的参数
                                    take_profit_threshold = self.calculate_dynamic_take_profit(
                                        position, hold_period_data, entry_datetime, time_5m
                                    )
                                    take_profit_price = current_avg_price * (1 - take_profit_threshold / 100)  # take_profit_threshold是百分比数值（如33.0）
                                    
                                    if low_5m <= take_profit_price:
                                        actual_exit_time = time_5m_str
                                        logging.info(
                                            f"✨ 止盈: {symbol} 在 {time_5m_str} low={low_5m:.6f} 触发止盈阈值，"
                                            f"按止盈价{take_profit_price:.6f}成交（阈值{take_profit_threshold:.1f}%）"
                                        )
                                        self.exit_position(position, take_profit_price, time_5m_str, 'take_profit')
                                        return True
                                    
                                    # 🚨 提前止损检查：精确在2小时/12小时时点检查一次
                                    # ⚠️ 注意：5分钟K线循环只会在建仓后的第1个小时内执行
                                    # 2小时/12小时的检查会在小时K线循环中进行（3341-3408行）
                                    # 这里只保留逻辑，但由于建仓后第1小时不可能到2小时，所以不会触发
                                    # 保留代码是为了逻辑完整性，避免特殊情况（如5分钟K线数据缺失时才走这里）
                                    
                                    # 最后检查止损（做空：基于最高价）
                                    stop_loss_price = current_avg_price * (1 - self.stop_loss_pct / 100)  # stop_loss_pct=-18.0，所以1-(-18/100)=1.18
                                    if high_5m >= stop_loss_price:
                                        actual_exit_time = time_5m_str
                                        logging.warning(
                                            f"🛑 止损触发: {symbol} 在 {time_5m_str} high={high_5m:.6f} 触发止损阈值，"
                                            f"按止损价{stop_loss_price:.6f}成交"
                                        )
                                        self.exit_position(position, stop_loss_price, time_5m_str, 'stop_loss')
                                        return True
                                
                                # 建仓小时的5分钟检查完成，跳过小时K线检查
                                logging.debug(f"⏭️ {symbol} 建仓小时5分钟检查完成，跳过小时K线检查")
                                continue
                            else:
                                # 🔧 修复（2026-02-06）：当前小时没有建仓之后的5分钟K线，使用小时K线检查
                                # 原BUG：这里continue会跳过止损检查，导致持仓期间价格超过18%也不止损
                                # 修复：不continue，让程序继续执行后续的小时K线止损检查（3773行之后）
                                logging.debug(f"⏭️ {symbol} {hour_datetime_str}: 建仓5分钟之后无5分钟K线，继续使用小时K线检查")
                                # ✅ 不continue，继续执行后续的止损止盈检查
                    
                    # 🔧 非建仓小时：使用小时K线检查（原逻辑）
                    # 之前的补仓逻辑已移除

                    
                    # 🆕 添加：检查顶级交易者数据止损
                    if hasattr(self, 'enable_trader_filter') and self.enable_trader_filter:
                        should_stop, stop_reason = self.check_trader_stop_loss(position, hour_datetime)
                        if should_stop:
                            # 按当前小时收盘价止损
                            stop_price = row['close']
                            self.exit_position(position, stop_price, hour_datetime_str, "stop_loss_trader")
                            logging.warning(
                                f"🛑 顶级交易者止损: {symbol} 在 {hour_datetime_str} "
                                f"因{stop_reason}，按价格{stop_price:.6f}止损"
                            )
                            return True
                    
                    # 🚨 提前止损检查：精确在2小时/12小时时点检查一次
                    # ✅ 必须降级到5分钟K线，取恰好第24根/第144根的收盘价
                    # ⚠️ 不能用小时K线收盘价，因为小时K线时间范围与整点不一致
                    if self.enable_2h_early_stop or self.enable_12h_early_stop:
                        hours_held = (hour_datetime - entry_hour_timestamp).total_seconds() / 3600 if entry_hour_timestamp else 0
                        
                        # 2h提前止损检查：精确在建仓后2小时时检查
                        if self.enable_2h_early_stop and not position.get('checked_2h_early_stop', False):
                            if hours_held >= 2.0:
                                # 🔥 只查询建仓后2小时那个时点的最近一根5分钟K线的close价
                                try:
                                    cursor = self.crypto_conn.cursor()
                                    kline_5m_table = f'Kline5m_{symbol}'
                                    
                                    # 计算2小时时点的时间戳
                                    entry_time_2h = entry_hour_timestamp + timedelta(hours=2)
                                    entry_time_2h_ts = int(entry_time_2h.timestamp() * 1000)
                                    
                                    query = f"""
                                    SELECT close
                                    FROM {kline_5m_table}
                                    WHERE open_time <= ?
                                    ORDER BY open_time DESC
                                    LIMIT 1
                                    """
                                    cursor.execute(query, (entry_time_2h_ts,))
                                    result = cursor.fetchone()
                                    
                                    if result:
                                        close_2h = float(result[0])
                                        price_change_pct = (close_2h - current_avg_price) / current_avg_price
                                        
                                        if price_change_pct > self.early_stop_2h_threshold:
                                            logging.warning(
                                                f"🚨 2h提前止损触发: {symbol} 在2h时点（持仓{hours_held:.1f}h）\n"
                                                f"  • 2h时点收盘价：{close_2h:.6f}\n"
                                                f"  • 建仓价：{current_avg_price:.6f}\n"
                                                f"  • 涨幅：{price_change_pct*100:.2f}% > 阈值 {self.early_stop_2h_threshold*100:.2f}%"
                                            )
                                            self.exit_position(position, close_2h, hour_datetime_str, 'early_stop_loss')
                                            return True
                                    else:
                                        logging.warning(f"⚠️ {symbol} 2h时点未找到K线数据")
                                except Exception as e:
                                    logging.warning(f"⚠️ {symbol} 2h提前止损检查失败: {e}")
                                
                                # 检查完毕，标记避免重复检查
                                position['checked_2h_early_stop'] = True
                        
                        # 12h提前止损检查：精确在建仓后12小时时检查
                        if self.enable_12h_early_stop and not position.get('checked_12h_early_stop', False):
                            if hours_held >= 12.0:
                                # 🔥 降级到5分钟K线，取第144根（12小时整点）
                                try:
                                    kline_5m_df = self.get_5min_kline_data(symbol)
                                    if not kline_5m_df.empty:
                                        kline_5m_df['open_datetime'] = pd.to_datetime(kline_5m_df['open_time'], unit='ms', utc=True)
                                        target_time = entry_hour_timestamp + timedelta(hours=12)
                                        mask = (kline_5m_df['open_datetime'] >= entry_hour_timestamp) & \
                                               (kline_5m_df['open_datetime'] <= target_time)
                                        klines_12h = kline_5m_df[mask].copy()
                                        
                                        if len(klines_12h) >= 144:
                                            close_12h = float(klines_12h.iloc[143]['close'])  # 第144根
                                            price_change_pct = (close_12h - current_avg_price) / current_avg_price
                                            
                                            if price_change_pct > self.early_stop_12h_threshold:
                                                logging.warning(
                                                    f"🚨 12h提前止损触发: {symbol} 在12h整点（持仓{hours_held:.1f}h）\n"
                                                    f"  • 12h整点收盘价：{close_12h:.6f}\n"
                                                    f"  • 建仓价：{current_avg_price:.6f}\n"
                                                    f"  • 涨幅：{price_change_pct*100:.2f}% > 阈值 {self.early_stop_12h_threshold*100:.2f}%"
                                                )
                                                self.exit_position(position, close_12h, hour_datetime_str, 'early_stop_loss_12h')
                                                return True
                                except Exception as e:
                                    logging.debug(f"12h提前止损检查失败 {symbol}: {e}")
                                
                                # 检查完毕，标记避免重复检查
                                position['checked_12h_early_stop'] = True
                    
                    # 🔥 改进：检查是否同时触发止盈和止损
                    stop_loss_triggered = high_price >= sl_price
                    take_profit_triggered = low_price <= tp_price
                    
                    # 🔍 调试日志：检查止损条件（改为info级别，确保输出）
                    if stop_loss_triggered or hours_held >= 60:  # 止损触发或持仓60+小时时输出日志
                        logging.info(f"🔍 {symbol} {hour_datetime_str}: high={high_price:.6f}, sl_price={sl_price:.6f}, triggered={stop_loss_triggered}, hours_held={hours_held if 'hours_held' in locals() else 'N/A'}")
                    
                    # 🎯 如果同时触发，降级到5分钟K线精确判断先触发哪个
                    if stop_loss_triggered and take_profit_triggered:
                        logging.info(
                            f"⚠️ {symbol} 在 {hour_datetime_str} 同时触发止盈和止损，"
                            f"降级查询5分钟K线精确判断"
                        )
                        
                        # 查询这个小时的5分钟K线
                        try:
                            kline_5m_df = self.get_5min_kline_data(symbol)
                            if not kline_5m_df.empty:
                                kline_5m_df['open_datetime'] = pd.to_datetime(kline_5m_df['open_time'], unit='ms', utc=True)
                                
                                # 筛选这个小时的5分钟K线
                                hour_end = hour_datetime + timedelta(hours=1)
                                mask_hour = (kline_5m_df['open_datetime'] >= hour_datetime) & \
                                           (kline_5m_df['open_datetime'] < hour_end)
                                hour_5m_data = kline_5m_df[mask_hour].copy()
                                
                                if not hour_5m_data.empty:
                                    # 逐个5分钟K线检查，看哪个先触发
                                    for _, row_5m in hour_5m_data.iterrows():
                                        high_5m = float(row_5m['high'])
                                        low_5m = float(row_5m['low'])
                                        
                                        # 先检查止盈（与5分钟K线逻辑一致）
                                        if low_5m <= tp_price:
                                            position['tp_pct_used'] = dynamic_tp_pct
                                            self.exit_position(position, tp_price, hour_datetime_str, "take_profit")
                                            logging.info(
                                                f"✨ 止盈（5分钟精确）: {symbol} 在 {hour_datetime_str} 通过5分钟K线判断先触发止盈，"
                                                f"按止盈价{tp_price:.6f}成交（阈值{dynamic_tp_pct:.1f}%）"
                                            )
                                            return True
                                        
                                        # 再检查止损
                                        if high_5m >= sl_price:
                                            self.exit_position(position, sl_price, hour_datetime_str, "stop_loss")
                                            logging.warning(
                                                f"🛑 止损（5分钟精确）: {symbol} 在 {hour_datetime_str} 通过5分钟K线判断先触发止损，"
                                                f"按止损价{sl_price:.6f}成交"
                                            )
                                            return True
                                    
                                    # 如果5分钟K线都没触发（理论上不可能），默认止盈
                                    logging.warning(f"⚠️ {symbol} 5分钟K线未触发，默认止盈")
                                    position['tp_pct_used'] = dynamic_tp_pct
                                    self.exit_position(position, tp_price, hour_datetime_str, "take_profit")
                                    return True
                                else:
                                    # 没有5分钟数据，默认先止盈（保守）
                                    logging.warning(f"⚠️ {symbol} 无5分钟K线数据，默认止盈优先")
                                    position['tp_pct_used'] = dynamic_tp_pct
                                    self.exit_position(position, tp_price, hour_datetime_str, "take_profit")
                                    return True
                            else:
                                # 没有5分钟数据表，默认止盈优先（保守）
                                logging.warning(f"⚠️ {symbol} 无5分钟K线表，默认止盈优先")
                                position['tp_pct_used'] = dynamic_tp_pct
                                self.exit_position(position, tp_price, hour_datetime_str, "take_profit")
                                return True
                                
                        except Exception as e:
                            # 查询5分钟数据失败，默认止盈优先（保守）
                            logging.warning(f"⚠️ {symbol} 查询5分钟K线失败: {e}，默认止盈优先")
                            position['tp_pct_used'] = dynamic_tp_pct
                            self.exit_position(position, tp_price, hour_datetime_str, "take_profit")
                            return True
                    
                    # 🔥 如果只触发一个，直接判断（先止盈后止损，与5分钟K线一致）
                    if take_profit_triggered:
                        position['tp_pct_used'] = dynamic_tp_pct
                        self.exit_position(position, tp_price, hour_datetime_str, "take_profit")
                        logging.info(
                            f"✨ 止盈: {symbol} 在 {hour_datetime_str} low={low_price:.6f} 触发止盈阈值，"
                            f"按止盈价{tp_price:.6f}成交（阈值{dynamic_tp_pct:.1f}%）"
                        )
                        return True
                    
                    if stop_loss_triggered:
                        self.exit_position(position, sl_price, hour_datetime_str, "stop_loss")
                        logging.warning(
                            f"🛑 止损: {symbol} 在 {hour_datetime_str} high={high_price:.6f} 触发止损阈值，"
                            f"按止损价{sl_price:.6f}成交"
                        )
                        return True
                    
                    # 🚨 24小时收盘价涨幅检查（简化版 2026-02-05）
                    # 设计：只在24-25小时窗口检查一次，用当前小时的close价格判断
                    # 24h是特殊风控点，检查一次后不再检查，避免过度平仓
                    if self.enable_max_gain_24h_exit and entry_hour_timestamp:
                        hours_held_for_24h_check = (hour_datetime - entry_hour_timestamp).total_seconds() / 3600
                        # 只在24-25小时窗口检查一次
                        if 24.0 <= hours_held_for_24h_check < 25.0 and not position.get('checked_24h_max_gain', False):
                            entry_price0 = float(position.get('entry_price') or position.get('avg_entry_price') or 0)
                            if entry_price0 > 0:
                                # 直接用当前小时（第24小时）的close价格
                                close_24h = float(row['close'])
                                
                                # 计算24小时收盘价涨幅
                                up_ratio_24h = (close_24h - entry_price0) / entry_price0
                                position['close_gain_24h'] = float(up_ratio_24h)  # 🆕 记录24小时收盘价涨幅
                                position['checked_24h_max_gain'] = True  # 标记已检查，不再重复
                                
                                # 如果涨幅>6.3%，立即平仓
                                if up_ratio_24h > self.max_gain_24h_threshold:
                                    position['24h_close_price'] = close_24h  # 记录24h收盘价
                                    logging.warning(
                                        f"🚨🚨🚨 24h收盘价涨幅动态平仓: {symbol}\n"
                                        f"  • 检测时间：第{hours_held_for_24h_check:.1f}小时（{hour_datetime_str}）\n"
                                        f"  • 24h收盘价：{close_24h:.6f}\n"
                                        f"  • 建仓价：{entry_price0:.6f}\n"
                                        f"  • 涨幅：{up_ratio_24h*100:.2f}% > 阈值{self.max_gain_24h_threshold*100:.2f}%\n"
                                        f"  • 平仓价：{close_24h:.6f}\n"
                                        f"  • 预计亏损：{up_ratio_24h*100:.2f}%\n"
                                        f"  • 📊 24h收盘价涨幅>6.3%，做空逻辑失效，立即止损"
                                    )
                                    self.exit_position(position, close_24h, hour_datetime_str, "max_gain_24h_exit")
                                    return True
                                else:
                                    logging.info(
                                        f"📊 {symbol} 24h收盘价涨幅: {up_ratio_24h*100:.2f}% "
                                        f"(建仓价{entry_price0:.6f}, 24h收盘{close_24h:.6f}, 未超过阈值{self.max_gain_24h_threshold*100:.2f}%)"
                                    )
                    
                    # 🔧 最后检查：72小时强制平仓（兜底机制，优先级低于止盈/止损）
                    # ✅ 修复（2026-02-07）：将72小时检查移到止盈/止损之后
                    # 原因：
                    #   1. 原逻辑：72小时检查在循环最开始，一旦触发就直接return，止盈/止损永远不会执行
                    #   2. 导致问题：即使价格已经涨超-18%或跌超33%，也要等到72小时才平仓
                    #   3. 修复方案：72小时检查作为兜底，只有在没有触发止盈/止损的情况下才执行
                    if entry_hour_timestamp:
                        total_hours_held = (hour_datetime - entry_hour_timestamp).total_seconds() / 3600
                        
                        # 🐛 DEBUG：打印每个小时的检查日志（用于排查72小时不触发的问题）
                        if total_hours_held >= 60:  # 60小时以上才打印，避免日志过多
                            logging.info(
                                f"🕒🕒🕒 {symbol} idx={idx}, hour={hour_datetime.strftime('%Y-%m-%d %H:%M')}, "
                                f"total_hours={total_hours_held:.1f}, max_hold_hours={self.max_hold_hours}, "
                                f"status={position.get('status')}, 是否>=72h={total_hours_held >= self.max_hold_hours}"
                            )
                        
                        if total_hours_held >= self.max_hold_hours:
                            logging.warning(f"⏰⏰⏰ {symbol} 触发72小时检查! hours={total_hours_held:.1f}, max={self.max_hold_hours}")
                            # ✅ 使用当前小时的收盘价（这是第72小时或更晚的小时K线）
                            exit_price = row['close']
                            exit_reason = "max_hold_time"
                            if position.get('status') == 'observing':
                                exit_reason = "observing_timeout"
                            elif position.get('status') == 'virtual_tracking':
                                exit_reason = "virtual_max_hold_time"
                            
                            self.exit_position(position, exit_price, hour_datetime_str, exit_reason)
                            logging.warning(
                                f"⏰⏰⏰ 72小时强制平仓（兜底）: {symbol} 持有{total_hours_held:.1f}h，"
                                f"状态={position.get('status')}，平仓原因={exit_reason}\n"
                                f"  • 说明：持仓72小时未触发止盈/止损，强制平仓释放资金"
                            )
                            return True
            
            # 🔧 备用检查：如果小时K线循环没有捕获到72小时平仓，这里兜底
            # ⚠️ 注意：不能使用 current_datetime（当天23:59:59）计算，会提前触发
            # 正确做法：检查 hold_period_data 中是否有超过72小时的K线，如果有才触发
            # 
            # 例如：建仓2025-11-05 05:00，当前日期2025-11-08
            #   - 错误：hours_held = (2025-11-08 23:59:59 - 2025-11-05 05:00) ≈ 91h
            #   - 正确：检查hold_period_data中最后一根K线是否 >= 2025-11-08 05:00（72小时时刻）
            
            # 计算72小时时刻
            if entry_hour_timestamp:
                target_72h_timestamp = entry_hour_timestamp + timedelta(hours=self.max_hold_hours)
                target_72h_ms = int(target_72h_timestamp.timestamp() * 1000)
                
                # 检查 hold_period_data 中是否有 >= 72小时的K线
                if not hold_period_data.empty:
                    last_kline_time_ms = hold_period_data.iloc[-1]['open_time']
                    has_72h_kline = last_kline_time_ms >= target_72h_ms
                else:
                    has_72h_kline = False
            else:
                has_72h_kline = False
            
            if has_72h_kline:
                # 计算实际持仓时间（用于日志）
                last_kline_time = pd.to_datetime(hold_period_data.iloc[-1]['open_time'], unit='ms', utc=True)
                actual_hours_held = (last_kline_time - entry_hour_timestamp).total_seconds() / 3600
                
                logging.warning(f"⏰⏰⏰ {symbol} backup检查发现有72h K线数据（实际持仓{actual_hours_held:.1f}h）")
                
                # ✅ 关键修复：必须查找第72小时的K线，而不是最后一根K线！
                # 原因：hold_period_data可能包含第88小时的数据，使用last_row会导致平仓价错误
                if not hold_period_data.empty:
                    # 🔍 查找第72小时（或之后第一根）K线
                    candidate_rows = hold_period_data[hold_period_data['open_time'] >= target_72h_ms]
                    if not candidate_rows.empty:
                        exit_row = candidate_rows.iloc[0]
                        exit_price = float(exit_row['close'])
                        exit_time = pd.to_datetime(exit_row['open_time'], unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S')
                        exact_hours = (pd.to_datetime(exit_row['open_time'], unit='ms', utc=True) - entry_hour_timestamp).total_seconds() / 3600
                        logging.info(f"📊 {symbol} 找到第{exact_hours:.1f}h K线（72h时刻）: close={exit_price:.6f}")
                    else:
                        # 🔍 如果没有超过72h的数据，说明还没到72h，这个backup不应该触发
                        # 但为了兜底，使用最后一根K线
                        last_row = hold_period_data.iloc[-1]
                        exit_price = float(last_row['close'])
                        exit_time = pd.to_datetime(last_row['open_time'], unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S')
                        last_hour = (pd.to_datetime(last_row['open_time'], unit='ms', utc=True) - entry_hour_timestamp).total_seconds() / 3600
                        logging.warning(f"⚠️ {symbol} 没有找到72h K线，使用最后一根（第{last_hour:.1f}h）: close={exit_price:.6f}")
                else:
                    # 没有小时数据，使用当前价格（最差情况）
                    exit_price = current_price
                    exit_time = current_date
                    logging.warning(f"⚠️ {symbol} backup检查无小时数据，使用当前价{exit_price:.6f}")
                
                # ❌ BUG修复（2026-02-05）：删除72小时后的止损/止盈判断
                exit_reason = "max_hold_time"
                
                self.exit_position(position, exit_price, exit_time, exit_reason)
                logging.info(f"⏰ 72h强制平仓（backup）: {symbol} 平仓价{exit_price:.6f}，原因{exit_reason}")
                return True
            
            return False
        
        except Exception as e:
            import traceback
            import sys
            exc_info = sys.exc_info()
            logging.error(f"检查平仓条件失败: {e}")
            logging.error(f"异常类型: {exc_info[0]}")
            logging.error(f"异常值: {exc_info[1]}")
            logging.error(f"异常位置: {exc_info[2].tb_frame.f_code.co_filename}:{exc_info[2].tb_lineno}")
            logging.error(f"完整堆栈:\n{''.join(traceback.format_tb(exc_info[2]))}")
            return False

    def exit_position(self, position: Dict, exit_price: float, exit_date: str, exit_reason: str):
        """平仓操作"""
        try:
            symbol = position['symbol']
            
            # 真实平仓模式（做空策略）
            entry_price = position['avg_entry_price']
            position_size = position['position_size']
            
            # 计算盈亏（做空：价格下跌盈利，价格上涨亏损）
            pnl = (entry_price - exit_price) * position_size  # 做空：建仓价-平仓价
            pnl_pct = (entry_price - exit_price) / entry_price * 100  # 做空：价格跌了X%就赚X%
            
            # 🆕 智能解析exit_date，可能包含时间或只有日期
            exit_datetime = None
            try:
                # 尝试解析完整的日期时间
                if ' ' in exit_date:  # 包含时间
                    exit_datetime = pd.to_datetime(exit_date, utc=True)  # 🔧 指定UTC
                    exit_date_only = exit_datetime.strftime('%Y-%m-%d')
                else:  # 只有日期
                    exit_date_only = exit_date
                    # 🔧 修复：force_close时使用23:59:59，避免平仓时间早于建仓时间
                    exit_datetime = pd.to_datetime(exit_date + ' 23:59:59', utc=True)  # 🔧 指定UTC
            except:
                exit_date_only = exit_date.split(' ')[0] if ' ' in exit_date else exit_date
                # 🔧 修复：force_close时使用23:59:59，避免平仓时间早于建仓时间
                exit_datetime = pd.to_datetime(exit_date_only + ' 23:59:59', utc=True)  # 🔧 指定UTC
            
            # 计算持仓天数
            entry_date = datetime.strptime(position['entry_date'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            exit_dt = datetime.strptime(exit_date_only, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            hold_days = (exit_dt - entry_date).days
            
            # 🆕 补充计算24小时涨幅（如果还没有计算）
            # 适用场景：持仓时间≥24小时，但因为各种原因没有在小时循环中计算
            if position.get('close_gain_24h') is None:
                try:
                    # 计算持仓小时数
                    entry_dt = position.get('entry_datetime')
                    if entry_dt:
                        if isinstance(entry_dt, str):
                            entry_dt = pd.to_datetime(entry_dt, utc=True).to_pydatetime()
                        hours_held = (exit_datetime - entry_dt).total_seconds() / 3600 if exit_datetime else 0
                        
                        # 如果持仓≥24小时，补充计算24h涨幅
                        if hours_held >= 24.0:
                            entry_price0 = float(position.get('entry_price') or position.get('avg_entry_price') or 0)
                            if entry_price0 > 0:
                                # 查询24小时的小时K线数据
                                entry_hour_timestamp = entry_dt.replace(minute=0, second=0, microsecond=0)
                                window_end_dt = entry_hour_timestamp + timedelta(hours=24)
                                
                                hourly_df = self.get_hourly_kline_data(
                                    symbol,
                                    start_date=entry_hour_timestamp.strftime('%Y-%m-%d'),
                                    end_date=window_end_dt.strftime('%Y-%m-%d')
                                )
                                
                                if not hourly_df.empty and 'close' in hourly_df.columns:
                                    entry_datetime_ms = int(entry_hour_timestamp.timestamp() * 1000)
                                    window_end_ms = int(window_end_dt.timestamp() * 1000)
                                    wmask = (hourly_df['open_time'] >= entry_datetime_ms) & (hourly_df['open_time'] < window_end_ms)
                                    wdf = hourly_df[wmask]
                                    
                                    if not wdf.empty:
                                        # 取第24小时的收盘价
                                        if len(wdf) >= 24:
                                            close_24h = float(wdf.iloc[23]['close'])
                                        else:
                                            close_24h = float(wdf.iloc[-1]['close'])
                                        
                                        up_ratio_24h = (close_24h - entry_price0) / entry_price0
                                        position['close_gain_24h'] = float(up_ratio_24h)
                                        logging.info(f"📊 {symbol} 补充计算24h收盘价涨幅: {up_ratio_24h*100:.2f}% (建仓价{entry_price0:.6f}, 24h收盘{close_24h:.6f})")
                except Exception as e:
                    logging.warning(f"⚠️ 补充计算24h涨幅失败: {e}")
            
            # 💰 计算资金费率成本
            funding_fee_info = {'total_fee': 0.0, 'fee_count': 0, 'avg_rate': 0.0}
            try:
                if position.get('entry_datetime') and exit_datetime:
                    entry_dt = position['entry_datetime']
                    if isinstance(entry_dt, str):
                        entry_dt = pd.to_datetime(entry_dt, utc=True).to_pydatetime()
                    
                    # 🔧 修复：传入杠杆倍数
                    funding_fee_info = self.calculate_funding_fee_cost(
                        symbol=position['symbol'],
                        entry_datetime=entry_dt,
                        exit_datetime=exit_datetime,
                        position_value=position['position_value'],
                        leverage=position.get('leverage', self.leverage)  # 使用实际杠杆
                    )
                    
                    # 如果有资金费用，记录日志
                    if funding_fee_info['total_fee'] != 0:
                        if funding_fee_info['total_fee'] > 0:
                            logging.info(f"💸 资金费用支出: ${funding_fee_info['total_fee']:.4f}（{funding_fee_info['fee_count']}次）")
                        else:
                            logging.info(f"💰 资金费用收入: ${-funding_fee_info['total_fee']:.4f}（{funding_fee_info['fee_count']}次）")
            except Exception as e:
                logging.warning(f"计算资金费用失败: {e}")
            
            # 💰 计算平仓手续费（Maker费率：限价单）
            exit_trading_value = exit_price * position_size  # 平仓成交金额
            exit_fee = self.calculate_trading_fee(exit_trading_value)
            
            # 💰 累计总交易手续费（建仓 + 平仓）
            total_trading_fee = position.get('entry_fee', 0) + exit_fee
            
            # 💰 计算多层级盈亏
            pnl_after_trading_fee = pnl - total_trading_fee  # 扣除交易手续费后
            pnl_after_all_costs = pnl_after_trading_fee - funding_fee_info['total_fee']  # 扣除所有成本后
            
            logging.info(
                f"💳 平仓手续费: ${exit_fee:.4f}（成交金额${exit_trading_value:.2f} × {self.trading_fee_rate*100:.2f}%）"
            )
            logging.info(
                f"💰 盈亏明细: 原始${pnl:.2f} → 扣交易费${pnl_after_trading_fee:.2f} → 最终净利润${pnl_after_all_costs:.2f} "
                f"(交易费${total_trading_fee:.4f} + 资金费${funding_fee_info['total_fee']:.4f})"
            )
            
            # 💰 复利模式：平仓时返还本金+盈亏
            # 正常情况：返还全部本金 + 扣除所有成本后的净盈亏
            position_value = position['position_value']
            self.capital += position_value + pnl_after_all_costs
            logging.info(f"💭 正常平仓: {exit_reason} 仓位${position_value:.2f} 净盈亏${pnl_after_all_costs:.2f}（原始${pnl:.2f}-交易费${total_trading_fee:.4f}-资金费${funding_fee_info['total_fee']:.4f}）资金:{self.capital:.2f}")
            
            # 更新持仓记录
            # 🔧🔧🔧 关键修复：保存动态止盈判定字段到trade_records
            # 这些字段在calculate_dynamic_take_profit中设置，平仓时必须保留，否则CSV报告会丢失
            position.update({
                'exit_date': exit_date_only,
                'exit_datetime': exit_datetime.isoformat() if exit_datetime else None,  # 🆕 保存完整时间戳
                'exit_price': exit_price,
                'exit_reason': exit_reason,
                'pnl': pnl,
                'pnl_pct': pnl_pct,
                'hold_days': hold_days,
                # 💰 交易手续费相关（2026-02-14新增）
                'exit_fee': exit_fee,  # 平仓手续费
                'total_trading_fee': total_trading_fee,  # 总交易手续费
                'pnl_after_trading_fee': pnl_after_trading_fee,  # 扣除交易手续费后盈亏
                # 💰 资金费率成本
                'funding_fee': funding_fee_info['total_fee'],  # 正数=支出，负数=收入
                'funding_fee_count': funding_fee_info['fee_count'],  # 收费次数
                'funding_fee_avg_rate': funding_fee_info['avg_rate'],  # 平均费率（百分比）
                # 💰 最终盈亏（扣除所有成本）
                'pnl_after_all_costs': pnl_after_all_costs,  # 扣除交易手续费+资金费后的最终净利润
                'pnl_after_funding': pnl - funding_fee_info['total_fee'],  # 🔧 保留原字段兼容性
                # 🔧 保留动态止盈判定结果（如果已经判定过）
                'dynamic_tp_strong': position.get('dynamic_tp_strong'),
                'dynamic_tp_medium': position.get('dynamic_tp_medium'),
                'dynamic_tp_weak': position.get('dynamic_tp_weak'),
                'dynamic_tp_trigger': position.get('dynamic_tp_trigger'),
                'dynamic_tp_pct': position.get('dynamic_tp_pct'),
                'dynamic_tp_2h_pct_drop': position.get('dynamic_tp_2h_pct_drop'),
                'dynamic_tp_12h_pct_drop': position.get('dynamic_tp_12h_pct_drop'),
                'dynamic_tp_2h_result': position.get('dynamic_tp_2h_result')  # 🔧 新增：2小时判断结果
            })
            
            # 从持仓列表中移除
            if position in self.positions:
                self.positions.remove(position)
            
            logging.info(f"💰 平仓: {position['symbol']} {exit_date} 价格:{exit_price:.4f} 盈亏:${pnl:.2f} ({pnl_pct:+.1f}%) 原因:{exit_reason} 当前资金:${self.capital:.2f}")
        except Exception as e:
            logging.error(f"平仓失败: {e}")


    def get_entry_price(self, symbol: str, date_str: str) -> Optional[float]:
        """获取开盘价作为建仓价格"""
        try:
            cursor = self.crypto_conn.cursor()
            table_name = f'DailyKline_{symbol}'
            
            start_ms, end_ms = self.date_str_to_timestamp_range(date_str)
            cursor.execute(f'''
                SELECT open
                FROM "{table_name}"
                WHERE open_time >= ? AND open_time < ?
            ''', (start_ms, end_ms))
            
            result = cursor.fetchone()
            return result[0] if result and result[0] else None
        
        except Exception as e:
            logging.error(f"获取 {symbol} {date_str} 开盘价失败: {e}")
            return None

    def get_latest_5m_close(self, symbol: str, asof_dt: Optional[datetime] = None):
        """获取某交易对在 asof_dt 之前最近一根 5m K线的收盘价（用于持仓单的"当前浮盈亏"计算）

        数据来源：本地 SQLite `db/crypto_data.db` 的 `Kline5m_{symbol}` 表。
        返回：(open_time, close_price)；若缺数据返回 (None, None)。
        """
        try:
            if not symbol:
                return None, None

            table_name = f'Kline5m_{symbol}'
            cursor = self.crypto_conn.cursor()

            # 先检查表是否存在
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if cursor.fetchone() is None:
                return None, None

            if asof_dt is None:
                asof_dt = datetime.now(tz=timezone.utc)
            asof_ms = self.datetime_to_timestamp(asof_dt)

            cursor.execute(
                f'''
                SELECT open_time, close
                FROM "{table_name}"
                WHERE open_time <= ?
                ORDER BY open_time DESC
                LIMIT 1
                ''',
                (asof_ms,)
            )
            row = cursor.fetchone()
            if not row:
                return None, None
            open_time, close = row[0], row[1]
            if close is None:
                return open_time, None
            return open_time, float(close)
        except Exception:
            return None, None

    def get_5m_closes_in_window(self, symbol: str, start_dt: datetime, end_dt: datetime) -> List[float]:
        """获取指定时间窗口内的 5m K线 close 序列（按时间正序）。

        数据来源：本地 SQLite `db/crypto_data.db` 的 `Kline5m_{symbol}` 表。
        """
        try:
            if not symbol:
                return []

            table_name = f'Kline5m_{symbol}'
            cursor = self.crypto_conn.cursor()

            # 检查表是否存在
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if cursor.fetchone() is None:
                return []

            start_ms = self.datetime_to_timestamp(start_dt)
            end_ms = self.datetime_to_timestamp(end_dt)

            cursor.execute(
                f'''
                SELECT close
                FROM "{table_name}"
                WHERE open_time >= ? AND open_time < ?
                ORDER BY open_time ASC
                ''',
                (start_ms, end_ms)
            )
            rows = cursor.fetchall()
            closes: List[float] = []
            for (c,) in rows:
                if c is None:
                    continue
                closes.append(float(c))
            return closes
        except Exception as e:
            logging.debug(f"读取5m close失败 {symbol}: {e}")
            return []

    def calculate_post_entry_price_stats(self, symbol: str, entry_datetime: datetime, entry_price: float) -> dict:
        """
        计算建仓后的价格统计数据（用于CSV报告）
        
        返回字典包含：
        - '2h_close': 建仓2小时后的收盘价
        - '2h_rise_pct': 建仓后2小时收盘价涨幅%（相对建仓价）
        - '2h_max_drop_pct': 建仓后2小时最大跌幅%（做空盈利）
        - '12h_close': 建仓12小时后的收盘价
        - '12h_rise_pct': 建仓后12小时收盘价涨幅%（相对建仓价）
        - '12h_max_drop_pct': 建仓后12小时最大跌幅%
        - '24h_close': 建仓24小时后的收盘价
        - '24h_max_drop_pct': 建仓后24小时最大跌幅%
        - '72h_max_drop_pct': 建仓后72小时最大跌幅%
        """
        result = {
            '2h_close': '',
            '2h_rise_pct': '',
            '2h_max_drop_pct': '',
            '12h_close': '',
            '12h_rise_pct': '',
            '12h_max_drop_pct': '',
            '24h_close': '',
            '24h_max_drop_pct': '',
            '72h_max_drop_pct': ''
        }
        
        try:
            table_name = f'Kline5m_{symbol}'
            cursor = self.crypto_conn.cursor()
            
            # 检查表是否存在
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if cursor.fetchone() is None:
                return result
            
            # 获取建仓后72小时的所有5分钟K线
            end_72h = entry_datetime + timedelta(hours=72)
            start_ms = self.datetime_to_timestamp(entry_datetime)
            end_ms = self.datetime_to_timestamp(end_72h)
            
            cursor.execute(
                f'''
                SELECT open_time, close, low
                FROM "{table_name}"
                WHERE open_time >= ? AND open_time < ?
                ORDER BY open_time ASC
                ''',
                (start_ms, end_ms)
            )
            rows = cursor.fetchall()
            
            if not rows:
                return result
            
            # 解析数据：(时间, 收盘价, 最低价)
            klines = [(self.timestamp_to_datetime(row[0]), float(row[1]), float(row[2])) for row in rows if row[1] is not None and row[2] is not None]
            
            if not klines:
                return result
            
            # 计算各时间点数据
            # 1. 2小时数据 (24根5分钟K线)
            klines_2h = [kl for kl in klines if kl[0] <= entry_datetime + timedelta(hours=2)]
            if klines_2h:
                # 2小时收盘价（第24根K线的收盘价，或最接近的）
                close_2h = 0
                if len(klines_2h) >= 24:
                    close_2h = klines_2h[23][1]
                    result['2h_close'] = f"{close_2h:.6f}"
                else:
                    close_2h = klines_2h[-1][1]
                    result['2h_close'] = f"{close_2h:.6f}"
                
                # 🆕 2小时涨幅% = (2h收盘价 - 建仓价) / 建仓价 * 100
                if close_2h > 0:
                    rise_2h = (close_2h - entry_price) / entry_price * 100
                    result['2h_rise_pct'] = f"{rise_2h:.2f}%"
                
                # ✅ 2小时最大跌幅 = (entry_price - 最低价) / entry_price * 100（修复：使用low而不是close）
                min_price_2h = min(kl[2] for kl in klines_2h)  # kl[2]是low价
                max_drop_2h = (entry_price - min_price_2h) / entry_price * 100
                result['2h_max_drop_pct'] = f"{max_drop_2h:.2f}%"
            
            # 2. 12小时数据 (144根5分钟K线)
            klines_12h = [kl for kl in klines if kl[0] <= entry_datetime + timedelta(hours=12)]
            if klines_12h:
                # 12小时收盘价
                close_12h = 0
                if len(klines_12h) >= 144:
                    close_12h = klines_12h[143][1]
                    result['12h_close'] = f"{close_12h:.6f}"
                else:
                    close_12h = klines_12h[-1][1]
                    result['12h_close'] = f"{close_12h:.6f}"
                
                # 🆕 12小时涨幅% = (12h收盘价 - 建仓价) / 建仓价 * 100
                if close_12h > 0:
                    rise_12h = (close_12h - entry_price) / entry_price * 100
                    result['12h_rise_pct'] = f"{rise_12h:.2f}%"
                
                # ✅ 12小时最大跌幅（修复：使用low而不是close）
                min_price_12h = min(kl[2] for kl in klines_12h)  # kl[2]是low价
                max_drop_12h = (entry_price - min_price_12h) / entry_price * 100
                result['12h_max_drop_pct'] = f"{max_drop_12h:.2f}%"
            
            # 3. 24小时数据 (288根5分钟K线)
            klines_24h = [kl for kl in klines if kl[0] <= entry_datetime + timedelta(hours=24)]
            if klines_24h:
                # 24小时收盘价
                if len(klines_24h) >= 288:
                    result['24h_close'] = f"{klines_24h[287][1]:.6f}"
                else:
                    result['24h_close'] = f"{klines_24h[-1][1]:.6f}"
                
                # ✅ 24小时最大跌幅（修复：使用low而不是close）
                min_price_24h = min(kl[2] for kl in klines_24h)  # kl[2]是low价
                max_drop_24h = (entry_price - min_price_24h) / entry_price * 100
                result['24h_max_drop_pct'] = f"{max_drop_24h:.2f}%"
            
            # 4. 72小时最大跌幅 (所有数据)（修复：使用low而不是close）
            if klines:
                min_price_72h = min(kl[2] for kl in klines)  # kl[2]是low价
                max_drop_72h = (entry_price - min_price_72h) / entry_price * 100
                result['72h_max_drop_pct'] = f"{max_drop_72h:.2f}%"
            
            return result
            
        except Exception as e:
            logging.debug(f"计算建仓后价格统计失败 {symbol}: {e}")
            return result


    def run_backtest(self, start_date: str, end_date: str):
        """运行回测"""
        # 保存回测结束日期，供CSV里计算"未平仓持仓"的持仓时长使用
        self._backtest_end_date = end_date
        
        # 🔧 调试：打印接收到的参数
        logging.info(f"📊 回测参数检查：")
        logging.info(f"  start_date 原始值: '{start_date}' (type: {type(start_date).__name__})")
        logging.info(f"  end_date 原始值: '{end_date}' (type: {type(end_date).__name__})")
        
        logging.info(f"开始卖量暴涨策略回测: {start_date} 到 {end_date}")
        logging.info(f"初始资金: ${self.initial_capital:,.2f}")
        logging.info(f"杠杆倍数: {self.leverage}x")
        logging.info(f"卖量暴涨阈值: {self.sell_surge_threshold}倍")
        logging.info(f"建仓策略: 单小时卖量≥10倍立即建仓")
        logging.info(f"等待策略: 不等待，直接做空")
        logging.info(f"最大持仓时间: {self.max_hold_hours:.0f}小时（{self.max_hold_hours/24:.0f}天）")
        logging.info(
            f"BTC昨日阳线风控: 不建新仓={getattr(self, 'enable_btc_yesterday_yang_no_new_entry', False)} | "
            f"开盘一刀切平仓={getattr(self, 'enable_btc_yesterday_yang_flatten_at_open', False)}"
        )
        
        # 🔧 兼容两种日期格式：'2025-11-01' 或 '2025-11-01 00:00:00'
        start_date_only = start_date.split()[0] if ' ' in start_date else start_date
        end_date_only = end_date.split()[0] if ' ' in end_date else end_date
        
        # 🔧 修复：处理可能的日期格式问题（如'2025-11--5'有双连字符）
        # 这可能是由于某些地方使用了负数日期导致的
        if '--' in end_date_only:
            logging.warning(f"⚠️ 检测到异常日期格式: '{end_date_only}'，尝试修复...")
            # 将双连字符替换为单连字符，并补充日期为01
            parts = end_date_only.split('--')
            if len(parts) == 2:
                end_date_only = f"{parts[0]}-01"  # 使用当月第一天
                logging.info(f"✅ 日期已修复为: '{end_date_only}'")
        
        current_date = datetime.strptime(start_date_only, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date_only, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 🔧 每天建仓上限（从配置参数读取）
            max_entries_per_day = self.max_entries_per_day
            
            # 🆕 每天输出进度信息
            pending_count = self._count_pending_signals()
            logging.info(f"📅 处理日期: {date_str} | 当前资金: ${self.capital:,.2f} | 持仓数: {len(self.positions)} | 待建仓信号: {pending_count}")
            # 🔧 全表扫描超时（须在当日建仓逻辑之前，且与是否满仓无关）
            self._sweep_pending_timeouts(date_str)
            
            # 记录每日资金
            self.daily_capital.append({
                'date': date_str,
                'capital': self.capital,
                'positions_count': len(self.positions)
            })
            
            # 昨日 BTC 日K 阳线 → 当日 UTC 00:00:00 按各币当日日K open 一刀切平仓（早于小时线 check_exit）
            if self.enable_btc_yesterday_yang_flatten_at_open and self.positions:
                today_d = datetime.strptime(date_str, '%Y-%m-%d').date()
                yday = (today_d - timedelta(days=1)).strftime('%Y-%m-%d')
                y_oc = self._btc_daily_open_close_btc(yday)
                if y_oc is None:
                    logging.warning(f"⚠️ {date_str} 开盘一刀切跳过：BTC 日K 缺失 {yday}")
                else:
                    y_o, y_c = y_oc
                    if y_c > y_o:
                        logging.info(
                            f"📉 {date_str} 昨日BTC阳线({yday} o={y_o:.2f} c={y_c:.2f}) → "
                            f"UTC 00:00 按日K open 平掉 {len(self.positions)} 个持仓"
                        )
                        exit_ts = f"{date_str} 00:00:00"
                        for pos in list(self.positions):
                            sym = pos.get('symbol')
                            open_px = self.get_entry_price(sym, date_str)
                            if open_px is None:
                                logging.warning(
                                    f"⚠️ 开盘一刀切跳过 {sym}：{date_str} 日K open 缺失"
                                )
                                continue
                            self.exit_position(
                                pos,
                                float(open_px),
                                exit_ts,
                                "btc_yesterday_yang_flatten_open",
                            )
            
            # 检查现有持仓（直接使用小时K线，不依赖日K线）
            positions_to_check = self.positions.copy()
            logging.info(f"📅📅📅 {date_str}: 检查 {len(positions_to_check)} 个持仓")
            for position in positions_to_check:
                try:
                    symbol = position['symbol']
                    entry_date = position['entry_date']
                    logging.info(f"  🔍 检查 {symbol} (建仓日:{entry_date})")
                    
                    # 🔧 修复：不再依赖日K线，直接传入当前日期，check_exit_conditions内部会读取小时K线
                    # 传入一个虚拟price（不影响，因为函数内部用小时线数据）
                    self.check_exit_conditions(position, 0, date_str)
                
                except Exception as e:
                    logging.error(f"❌ 检查持仓失败 {position['symbol']}: {e}")
                    import traceback
                    logging.error(traceback.format_exc())
            
            # ============================================================================
            # 📌 与实盘一致：同一 UTC 自然日内，先把「当日卖量暴涨」写入 Pending，
            #    再跑 pending 触价扫描（数据窗口至当日 23:59）。避免「信号在当日末尾才入库、
            #    却要等次日循环才撮合」导致 BTC 次日开盘一刀切漏平隔夜应已成交的仓。
            # ============================================================================
            if len(self.positions) < self.max_daily_positions:
                logging.debug(f"🔍 [日内顺序] 先扫描 {date_str} 卖量暴涨信号并写入 pending...")
                daily_signals = self.get_daily_1hour_surge_signals(date_str)
                logging.debug(f"✅ 扫描完成，发现 {len(daily_signals)} 个信号")
                
                for signal in daily_signals:
                    symbol = signal['symbol']
                    surge_ratio = signal['surge_ratio']
                    signal_price = signal['signal_price']
                    signal_datetime = signal['signal_datetime']
                    
                    try:
                        cursor = self.crypto_conn.cursor()
                        cursor.execute("SELECT signal_id FROM PendingSignals WHERE symbol = ?", (symbol,))
                        if cursor.fetchone():
                            continue
                    except Exception:
                        pass
                    
                    if any(pos['symbol'] == symbol for pos in self.positions):
                        continue
                    
                    account_ratio = None
                    passed, account_ratio, filter_reason = self.check_trader_signal_filter(symbol, signal_datetime)
                    if not passed:
                        logging.info(
                            f"🚫 过滤信号: {symbol} 在 {signal_datetime} 卖量暴涨 {surge_ratio:.1f}倍，"
                            f"但{filter_reason}，跳过该信号"
                        )
                        try:
                            self._update_signal_record(
                                symbol,
                                signal_datetime.strftime('%Y-%m-%d %H:%M'),
                                status='filtered_trader',
                                note=filter_reason,
                            )
                        except Exception:
                            pass
                        continue
                    
                    earliest_entry_datetime = signal_datetime + timedelta(hours=1)
                    target_drop_pct = self.get_wait_drop_pct(surge_ratio)
                    timeout_datetime = earliest_entry_datetime + timedelta(hours=self.wait_timeout_hours)
                    signal_id = f"{symbol}_{signal_datetime.strftime('%Y%m%d%H%M')}"
                    
                    try:
                        cursor = self.crypto_conn.cursor()
                        cursor.execute("SELECT signal_id FROM PendingSignals WHERE signal_id = ?", (signal_id,))
                        if cursor.fetchone():
                            logging.debug(f"⚠️ 信号ID重复，跳过: {signal_id}")
                            continue
                    except Exception:
                        pass
                    
                    signal_data = {
                        'signal_id': signal_id,
                        'symbol': symbol,
                        'signal_date': signal_datetime.strftime('%Y-%m-%d %H:%M'),
                        'signal_datetime': signal_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        'earliest_entry_datetime': earliest_entry_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        'signal_price': float(signal_price),
                        'buy_surge_ratio': float(surge_ratio),
                        'target_drop_pct': float(target_drop_pct),
                        'timeout_datetime': timeout_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        'signal_account_ratio': account_ratio if account_ratio else None,
                    }
                    self._add_pending_signal(signal_data)
                    
                    try:
                        self.signal_records.append({
                            'signal_id': signal_id,
                            'symbol': symbol,
                            'signal_date': signal_datetime.strftime('%Y-%m-%d %H:%M'),
                            'signal_time': signal_datetime.strftime('%Y-%m-%d %H:00:00'),
                            'earliest_entry_time': earliest_entry_datetime.strftime('%Y-%m-%d %H:00:00'),
                            'signal_price': float(signal_price),
                            'buy_surge_ratio': float(surge_ratio),
                            'target_drop_pct': float(target_drop_pct),
                            'target_price': float(signal_price) * (1 + float(target_drop_pct)),
                            'timeout_time': timeout_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            'status': 'pending',
                            'entry_time': '',
                            'entry_price': '',
                            'note': '',
                        })
                    except Exception:
                        pass
                    
                    logging.info(
                        f"🔔 新信号[{signal_id}]: {symbol} @{signal_datetime.strftime('%H:%M')} "
                        f"卖量{surge_ratio:.2f}倍，可建仓时间: {earliest_entry_datetime.strftime('%H:%M')}"
                    )
            
            # 🔥🔥🔥 改进的建仓逻辑：使用数据库表严格控制持仓数量
            # 1. 从数据库按买量倍数降序查询信号（已排序，高倍数优先）
            # 2. 严格限制每次查询的数量（根据剩余仓位动态调整）
            # 3. 依次处理信号，直到达到持仓上限
            
            # 🔧 计算今天实际已建仓数（从trade_records统计）
            todays_entries_count = sum(1 for t in self.trade_records 
                                      if t.get('entry_date') == date_str)
            
            # 🔧🔧🔧 每日必须先清空（缩进 bug 修复）：
            # candidate_entries.sort / for candidate / 删 pending 在「if 满仓或今日额度用尽」时不会进入 else，
            # 若沿用上一交易日的列表，会错误重复处理候选、并从 DB 删掉不该删的待建仓信号 → 回测成交笔数异常偏少。
            candidate_entries = []
            signals_to_remove = []
            
            # 🔧 计算可建仓数量
            available_slots = self.max_daily_positions - len(self.positions)
            remaining_today = max_entries_per_day - todays_entries_count
            
            if available_slots <= 0:
                logging.debug(f"⏸️ 持仓已满({len(self.positions)}/{self.max_daily_positions})，跳过建仓")
            elif remaining_today <= 0:
                logging.debug(f"⏸️ 今日建仓已达上限({todays_entries_count}/{max_entries_per_day})，跳过建仓")
            else:
                # 🔥 触价扫描条数：与「今日还能建几仓」解耦，否则每天只检查极少数 pending，后排信号永远进不了 batch
                scan_limit = int(getattr(self, 'max_pending_signals_to_scan_per_day', 500) or 500)
                logging.info(
                    f"🎯 今日已建{todays_entries_count}个，剩余{remaining_today}个额度，持仓空位{available_slots}个，"
                    f"本轮扫描 pending≤{scan_limit}条（触价判断与成交上限分开）"
                )
                
                # 🔥 从数据库查询信号（已按买量倍数降序排序）
                current_datetime_str = datetime.strptime(date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                pending_signals_list = self._get_pending_signals(current_datetime_str, limit=scan_limit)
                
                logging.info(f"🔄 查询到 {len(pending_signals_list)} 个待建仓信号")
                
                for signal in pending_signals_list:
                    # 🔥 关键修复：每处理一个信号前都要检查持仓是否已满
                    if len(self.positions) >= self.max_daily_positions:
                        logging.warning(f"⚠️ 持仓已满({len(self.positions)}/{self.max_daily_positions})，停止处理剩余信号")
                        break
                    
                    symbol = signal['symbol']
                    logging.info(f"🔄 检查待建仓信号: {symbol}")
                    signal_price = float(signal['signal_price'])
                    buy_surge_ratio = float(signal['buy_surge_ratio'])
                    target_drop_pct = float(signal['target_drop_pct'])
                    target_price = signal_price * (1 + target_drop_pct)
                    
                    # 检查是否已持仓
                    if any(pos['symbol'] == symbol for pos in self.positions):
                        signal_id = signal['signal_id']
                        self._remove_pending_signal(signal_id)
                        continue
                    
                    # 🔧 检查是否超时
                    timeout_datetime_str = signal['timeout_datetime']
                    timeout_datetime = datetime.strptime(timeout_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    current_date_end = datetime.strptime(date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                    
                    if current_date_end > timeout_datetime:
                        signal_id = signal['signal_id']
                        logging.info(f"⏰ {symbol} 信号超时，放弃建仓（卖量{buy_surge_ratio:.1f}倍）")
                        self._update_signal_record(symbol, signal['signal_date'], status='timeout', 
                                                  note='超时未成交', signal_id=signal_id)
                        self._remove_pending_signal(signal_id)
                        continue
                    
                    # 获取小时线数据检查是否达到目标价格
                    signal_datetime_str = signal['signal_datetime']
                    signal_date_str = signal_datetime_str.split()[0] if ' ' in signal_datetime_str else signal_datetime_str
                    earliest_entry_datetime_str = signal['earliest_entry_datetime']
                    
                    # 转换为datetime对象
                    signal_datetime = datetime.strptime(signal_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    earliest_entry_datetime = datetime.strptime(earliest_entry_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    
                    hourly_df = self.get_hourly_kline_data(symbol, start_date=signal_date_str, end_date=date_str)
                    logging.info(f"🔍 {symbol} 获取小时K线: {signal_date_str} 到 {date_str}, 数据行数={len(hourly_df)}")
                    
                    if not hourly_df.empty:
                        hourly_df['open_datetime'] = pd.to_datetime(hourly_df['open_time'], unit='ms', utc=True)
                        
                        # 筛选可建仓时间段的数据
                        current_date_ts = pd.Timestamp(f"{date_str} 23:59:59", tz='UTC')
                        earliest_entry_ts = pd.Timestamp(earliest_entry_datetime)
                        
                        mask = (hourly_df['open_datetime'] >= earliest_entry_ts) & (hourly_df['open_datetime'] <= current_date_ts)
                        check_period_data = hourly_df[mask]
                        logging.info(f"🔍 {symbol} 筛选后小时数据: {len(check_period_data)}行, target_price={target_price:.6f}")
                        
                        # 检查是否有小时最高价达到目标反弹价
                        for idx, row in check_period_data.iterrows():
                            entry_hour_datetime = row['open_datetime']
                            logging.info(f"  ⏰ 检查小时: {entry_hour_datetime}, high={row['high']:.6f}, target={target_price:.6f}")
                            
                            # 检查是否超时
                            if entry_hour_datetime.to_pydatetime().replace(tzinfo=timezone.utc) > timeout_datetime:
                                logging.info(f"⏰ {symbol} 在{entry_hour_datetime.strftime('%H:00')}已超时，跳过该小时")
                                break
                            
                            # 判断是否应该建仓
                            should_enter = False
                            if abs(target_drop_pct) < 0.001:  # 立即建仓
                                should_enter = True
                                logging.info(f"📍 {symbol} 立即建仓模式（无需等待价格）")
                            elif row['high'] >= target_price:
                                should_enter = True
                            
                            if should_enter:
                                logging.info(f"  ✅ {symbol} should_enter=True, entry_hour={entry_hour_datetime}")
                                
                                # 使用该小时第一根5分钟K线的close价格
                                hour_start = entry_hour_datetime.to_pydatetime().replace(tzinfo=timezone.utc)
                                hour_end = hour_start + timedelta(hours=1)
                                hour_start_ts = int(hour_start.timestamp() * 1000)
                                hour_end_ts = int(hour_end.timestamp() * 1000)
                                
                                try:
                                    cursor = self.crypto_conn.cursor()
                                    kline_5m_table = f'Kline5m_{symbol}'
                                    cursor.execute(f"""
                                        SELECT open_time, high, close
                                        FROM {kline_5m_table}
                                        WHERE open_time >= ? AND open_time < ?
                                        ORDER BY open_time
                                    """, (hour_start_ts, hour_end_ts))
                                    hour_5m_rows = cursor.fetchall()
                                    hour_5m_data = pd.DataFrame(hour_5m_rows, columns=['open_time', 'high', 'close'])
                                    if not hour_5m_data.empty:
                                        hour_5m_data['open_datetime'] = pd.to_datetime(hour_5m_data['open_time'], unit='ms', utc=True)
                                except Exception as e:
                                    logging.debug(f"⚠️ {symbol} 查询5分钟K线失败: {e}")
                                    hour_5m_data = pd.DataFrame()
                                
                                if not hour_5m_data.empty:
                                    actual_entry_price = float(hour_5m_data.iloc[0]['close'])
                                    entry_datetime = hour_5m_data.iloc[0]['open_datetime'] + timedelta(minutes=5)
                                    logging.info(f"🎯 {symbol} 确定性建仓: {entry_datetime} 使用5分钟close价={actual_entry_price:.6f}")
                                else:
                                    actual_entry_price = float(row['close'])
                                    entry_datetime = entry_hour_datetime
                                    logging.warning(f"⚠️ {symbol} 无5分钟K线数据，使用小时K线收盘价建仓")
                                
                                theoretical_entry_price = target_price
                                
                                candidate_entries.append({
                                    'signal': signal,
                                    'entry_datetime': entry_datetime,
                                    'actual_price': actual_entry_price,
                                    'theoretical_price': theoretical_entry_price,
                                    'buy_surge_ratio': buy_surge_ratio,
                                    'target_drop_pct': target_drop_pct
                                })
                                
                                price_change_pct = (actual_entry_price - theoretical_entry_price) / theoretical_entry_price * 100
                                logging.info(f"🎯 候选建仓: {symbol} 买量{buy_surge_ratio:.1f}倍 价格变化{price_change_pct:+.2f}%")
                                break
            
            # 🎯 对候选列表按买量倍数降序排序（倍数优先）
            candidate_entries.sort(key=lambda x: x['buy_surge_ratio'], reverse=True)
            if candidate_entries:
                candidates_info = [(c['signal']['symbol'], f"{c['buy_surge_ratio']:.1f}倍") for c in candidate_entries]
                logging.info(f"📊 候选列表已按倍数排序: {candidates_info}")
            
            # 🎯 依次尝试建仓，直到达到持仓上限
            for candidate in candidate_entries:
                symbol = candidate['signal']['symbol']
                entry_datetime = candidate['entry_datetime']
                candidate_entry_date = entry_datetime.strftime('%Y-%m-%d')
                
                # 🔧 优先检查：持仓上限（最严格）
                if len(self.positions) >= self.max_daily_positions:
                    logging.warning(f"⚠️ 持仓数达到上限({len(self.positions)}/{self.max_daily_positions})，停止建仓")
                    break
                
                # 🔧🔧🔧 关键修复：统计candidate建仓日期那天已建仓数（而不是循环日期）
                todays_entries_count = sum(1 for t in self.trade_records 
                                          if t.get('entry_date') == candidate_entry_date)
                
                # 🔧 检查当天建仓额度（第二层限制）
                if todays_entries_count >= max_entries_per_day:
                    logging.warning(f"⚠️ {candidate_entry_date}建仓已达上限({todays_entries_count}/{max_entries_per_day})，跳过{symbol}")
                    continue  # 🔥 改为continue，因为其他candidate可能是不同日期
                
                signal = candidate['signal']
                symbol = signal['symbol']
                entry_datetime = candidate['entry_datetime']
                
                # 🔥 可选：每小时建仓限制（第三层）。默认关闭，避免多币种在同一历史 UTC 小时被误杀。
                if self.enable_hourly_entry_limit:
                    entry_hour = entry_datetime.strftime('%Y-%m-%d %H:00')
                    hourly_entries_count = sum(
                        1 for t in self.trade_records
                        if str(t.get('entry_datetime', '')).startswith(entry_hour)
                    )
                    if hourly_entries_count >= 1:
                        logging.warning(
                            f"⚠️ {entry_hour} 已建仓{hourly_entries_count}个，本小时不再建仓，跳过{symbol}"
                        )
                        continue
                actual_entry_price = candidate['actual_price']
                theoretical_entry_price = candidate['theoretical_price']
                buy_surge_ratio = candidate['buy_surge_ratio']
                target_drop_pct = candidate['target_drop_pct']
                entry_date = entry_datetime.strftime('%Y-%m-%d')
                entry_datetime_str = (
                    entry_datetime.strftime('%Y-%m-%d %H:%M')
                    if hasattr(entry_datetime, 'strftime')
                    else str(entry_datetime)
                )
                
                # 再次检查是否已持仓（可能在前面的循环中已经建仓了）
                if any(pos['symbol'] == symbol for pos in self.positions):
                    signals_to_remove.append(signal)
                    continue
                
                # 🔧 修复：检查资金是否充足（避免超额下单）
                required_capital = self.capital * self.position_size_ratio
                if required_capital > self.capital * self.capital_buffer_ratio:  # 保留5%作为缓冲
                    logging.warning(f"⚠️ 资金不足：需要${required_capital:,.2f}，可用${self.capital:,.2f}")
                    break
                
                # ========== 🆕 三重风控检查 ==========
                
                # 1️⃣ 多空比风控【已屏蔽】
                # ratio_check = self.check_long_short_ratio_risk(symbol, entry_date)
                # if not ratio_check['should_trade']:
                #     logging.warning(f"🚫 {symbol} 多空比风控拒绝：{ratio_check['reason']}")
                #     self._update_signal_record(
                #         symbol, signal.get('signal_date'),
                #         status='rejected_ratio',
                #         note=f"多空比风控拒绝：{ratio_check['reason']}"
                #     )
                #     signals_to_remove.append(signal)
                #     continue
                
                # 2️⃣ Premium Index风控（基差率风控）
                if self.enable_premium_filter:  # 🔴 检查基差风控开关
                    premium_check = self.check_premium_index_risk(symbol, entry_datetime_str)
                    if not premium_check['should_trade']:
                        logging.warning(f"🚫 {symbol} Premium风控拒绝：{premium_check['reason']}")
                        self._update_signal_record(
                            symbol, signal.get('signal_date'),
                            status='rejected_premium',
                            note=f"Premium风控拒绝：{premium_check['reason']}"
                        )
                        signals_to_remove.append(signal)
                        continue
                
                # 2.5️⃣ CVD创新低风控（2026-02-05新增）⭐⭐⭐⭐⭐
                if self.enable_cvd_new_low_filter:
                    cvd_check = self.check_cvd_new_low_risk(symbol, entry_datetime_str)
                    if not cvd_check['should_trade']:
                        logging.warning(f"🚫 {symbol} CVD风控拒绝：{cvd_check['reason']}")
                        self._update_signal_record(
                            symbol, signal.get('signal_date'),
                            status='rejected_cvd_new_low',
                            note=f"CVD创新低风控拒绝：{cvd_check['reason']}"
                        )
                        signals_to_remove.append(signal)
                        continue
                
                # 2.6️⃣ 昨日 BTC 日K 阳线 → 当日不建新仓
                if self.enable_btc_yesterday_yang_no_new_entry:
                    skip, reason = self.check_btc_yesterday_yang_blocks_entry(entry_datetime)
                    if skip:
                        logging.warning(f"🚫 {symbol} BTC昨日阳线风控拒绝：{reason}")
                        self._update_signal_record(
                            symbol,
                            signal.get('signal_date'),
                            status='rejected_btc_yesterday_yang',
                            note=reason,
                        )
                        signals_to_remove.append(signal)
                        continue
                
                # 3️⃣ 买量暴涨区间风控
                volume_check = self.check_buy_volume_risk(symbol, signal.get('signal_date'), buy_surge_ratio)
                if not volume_check['should_trade']:
                    logging.warning(f"🚫 {symbol} 买量区间风控拒绝：{volume_check['reason']}")
                    self._update_signal_record(
                        symbol, signal.get('signal_date'),
                        status='rejected_volume',
                        note=f"买量风控拒绝：{volume_check['reason']}"
                    )
                    signals_to_remove.append(signal)
                    continue
                
                # ========== 风控通过，执行建仓 ==========
                
                before_trades = len(self.trade_records)
                
                # 执行建仓
                self.execute_trade(
                    symbol, actual_entry_price, entry_date, 
                    signal['signal_date'], buy_surge_ratio, 
                    entry_datetime=entry_datetime,
                    theoretical_price=theoretical_entry_price,
                    signal_account_ratio=signal.get('signal_account_ratio'),  # 🆕 传递信号时的账户多空比
                    signal_price=signal.get('signal_price') or signal.get('signal_close'),  # 🔧 兼容：优先使用signal_price，如无则用signal_close
                    signal_datetime=signal.get('signal_date')  # 🔧 修复：传递真实信号时间（而不是earliest_entry_datetime）
                )
                
                if len(self.trade_records) > before_trades:
                    # 建仓成功
                    price_diff_pct = (actual_entry_price - theoretical_entry_price) / theoretical_entry_price * 100
                    
                    # 🔧🔧🔧 实时统计今日建仓数（使用entry_date而不是date_str！）
                    todays_entries_count = sum(1 for t in self.trade_records 
                                              if t.get('entry_date') == entry_date)
                    
                    logging.info(
                        f"✅ 【建仓#{len(self.positions)}】{symbol} 买量{buy_surge_ratio:.1f}倍 "
                        f"理论价{theoretical_entry_price:.6f} 实际价{actual_entry_price:.6f} "
                        f"(高出{price_diff_pct:.2f}%，做空更优) 时间{entry_datetime} [今日{todays_entries_count}/{max_entries_per_day}]"
                    )
                    self._update_signal_record(
                        symbol,
                        signal.get('signal_date'),
                        status='entered',
                        entry_datetime=entry_datetime,
                        entry_price=actual_entry_price,
                        note=f'触发目标价并建仓（理论{theoretical_entry_price:.6f}，实际{actual_entry_price:.6f}）'
                    )
                    signals_to_remove.append(signal)
                    
                    # 🆕 建仓后立即检查持仓上限（防止超限）
                    if len(self.positions) >= self.max_daily_positions:
                        logging.warning(
                            f"✅ 建仓后持仓数达到上限({len(self.positions)}/{self.max_daily_positions})，"
                            f"停止处理剩余{len(candidate_entries) - candidate_entries.index(candidate) - 1}个候选"
                        )
                        break  # 跳出 for candidate in candidate_entries 循环
                else:
                    # execute_trade 内部可能因为资金/风控拒绝
                    self._update_signal_record(
                        symbol,
                        signal.get('signal_date'),
                        status='reached_not_entered',
                        entry_datetime=entry_datetime,
                        entry_price=actual_entry_price,
                        note='触发目标价但未建仓（资金/风控）'
                    )
                    signals_to_remove.append(signal)
            
            # 移除已处理的信号
            # 🔥 改用数据库删除
            for signal in signals_to_remove:
                signal_id = signal.get('signal_id')
                if signal_id:
                    self._remove_pending_signal(signal_id)
            
            # 🔥 关键修复：建仓后立即检查新仓位的止损止盈
            # 原因：如果建仓当天就触发止损（如价格继续上涨），必须当天检查，不能等到第二天
            # 例如：11-02 07:00建仓，15:00触发止损，应该当天平仓，而不是等到11-03
            if len(self.positions) > 0:
                logging.info(f"🔥 建仓循环结束，检查当天新建仓位的止损止盈...")
                logging.info(f"   当前所有持仓: {[p['symbol'] + '(entry=' + p.get('entry_date', '') + ')' for p in self.positions]}")
                positions_to_check_after_entry = self.positions.copy()
                checked_count = 0
                for position in positions_to_check_after_entry:
                    try:
                        # 只检查今天建仓的仓位（避免重复检查旧仓位）
                        pos_entry_date = position.get('entry_date')
                        if pos_entry_date == date_str:
                            logging.info(f"🔥 检查今日新建仓位: {position['symbol']} (entry_date={pos_entry_date})")
                            self.check_exit_conditions(position, 0, date_str)
                            checked_count += 1
                    except Exception as e:
                        logging.error(f"❌ 建仓后检查失败 {position['symbol']}: {e}")
                        import traceback
                        logging.error(traceback.format_exc())
                if checked_count == 0:
                    logging.debug(f"   无今日建仓仓位需要检查")
            
            # 当日卖量扫描已移至「check_exit 之后、pending 触价之前」，见上文 [日内顺序]
            
            current_date += timedelta(days=1)
        
        # 最后一天：先用小时K线检查一次止盈/止损，避免错过应该止盈的机会
        logging.info(f"⏰ 回测结束，检查剩余{len(self.positions)}个持仓...")
        positions_to_check = self.positions.copy()
        for position in positions_to_check:
            try:
                # 先检查是否满足止盈/止损条件
                self.check_exit_conditions(position, 0, end_date)
            except Exception as e:
                logging.error(f"最后检查失败 {position['symbol']}: {e}")
        
        # 强制平仓剩余持仓（经过上面检查后还没平仓的）
        for position in self.positions.copy():
            try:
                cursor = self.crypto_conn.cursor()
                symbol = position["symbol"]
                
                # 🔧 修复force_close bug：使用当天最后一个小时K线的收盘价，而不是日K线
                # 这样可以获取更准确的平仓价格和平仓时间
                hourly_table = f'HourlyKline_{symbol}'
                
                # 查询回测结束日期当天的最后一个小时K线
                end_start_ms, end_end_ms = self.date_str_to_timestamp_range(end_date)
                cursor.execute(f'''
                    SELECT close, open_time
                    FROM "{hourly_table}"
                    WHERE open_time >= ? AND open_time < ?
                    ORDER BY open_time DESC
                    LIMIT 1
                ''', (end_start_ms, end_end_ms))
                
                result = cursor.fetchone()
                if result and result[0]:
                    exit_price = result[0]
                    exit_time_ms = result[1]
                    
                    # 🔧 关键修复：将毫秒时间戳转换为datetime，作为准确的平仓时间
                    exit_datetime = datetime.fromtimestamp(exit_time_ms / 1000, tz=timezone.utc)
                    exit_datetime_str = exit_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 记录当前应该使用的止盈阈值（用于CSV）
                    if position.get('entry_datetime'):
                        entry_datetime = pd.to_datetime(position['entry_datetime'], utc=True)  # 🔧 指定UTC
                        hourly_df = pd.DataFrame()  # 空的，因为只是为了获取当前止盈阈值
                        current_tp = self.calculate_dynamic_take_profit(position, hourly_df, entry_datetime, exit_datetime)
                        position['tp_pct_used'] = current_tp
                    
                    # 🔧 传入完整的日期时间字符串，而不是只传日期
                    self.exit_position(position, exit_price, exit_datetime_str, "force_close")
                    logging.warning(f"⚠️ 强制平仓: {symbol} 时间:{exit_datetime_str} 价格:{exit_price:.4f}")
                else:
                    # 如果没有小时K线数据，回退到日K线
                    logging.warning(f"⚠️ {symbol} 无小时K线数据，使用日K线")
                    table_name = f'DailyKline_{symbol}'
                    cursor.execute(f'''
                        SELECT close
                        FROM "{table_name}"
                        WHERE open_time >= ? AND open_time < ?
                        ORDER BY open_time DESC
                        LIMIT 1
                    ''', (end_start_ms, end_end_ms))
                    
                    result = cursor.fetchone()
                    if result and result[0]:
                        exit_price = result[0]
                        # 使用当天23:59:59作为平仓时间
                        exit_datetime_str = end_date + ' 23:59:59'
                        
                        if position.get('entry_datetime'):
                            entry_datetime = pd.to_datetime(position['entry_datetime'], utc=True)
                            end_datetime = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                            hourly_df = pd.DataFrame()
                            current_tp = self.calculate_dynamic_take_profit(position, hourly_df, entry_datetime, end_datetime)
                            position['tp_pct_used'] = current_tp
                        
                        self.exit_position(position, exit_price, exit_datetime_str, "force_close")
                        logging.warning(f"⚠️ 强制平仓: {symbol} 价格:{exit_price:.4f}")
            
            except Exception as e:
                logging.error(f"强制平仓失败: {e}")
        
        # 🔧 调试日志：回测结束时的统计
        logging.info("="*80)
        logging.info("回测完成")
        logging.info(f"📊 回测统计汇总:")
        logging.info(f"  - 总信号数: {len(self.signal_records)}")
        logging.info(f"  - 总交易记录数: {len(self.trade_records)}")
        logging.info(f"  - 已建仓交易: {len([t for t in self.trade_records if t.get('entry_date')])}")
        logging.info(f"  - 拒绝建仓: {len([s for s in self.signal_records if s.get('status') == 'rejected'])}")
        logging.info(f"  - 当前持仓数: {len(self.positions)}")
        logging.info(f"  - 最终资金: ${self.capital:.2f}")
        logging.info("="*80)

    def generate_report(self):
        """生成回测报告"""
        print("\n" + "="*80)
        print("🚀 卖量暴涨策略回测报告")
        print("="*80)
        
        # 基本统计
        total_trades = len(self.trade_records)
        winning_trades = len([t for t in self.trade_records if t['pnl'] > 0])
        losing_trades = len([t for t in self.trade_records if t['pnl'] < 0])
        
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
        
        # 资金统计
        final_capital = self.capital
        total_return = (final_capital - self.initial_capital) / self.initial_capital * 100
        
        # 最大回撤计算
        max_capital = self.initial_capital
        max_drawdown = 0
        
        for record in self.daily_capital:
            max_capital = max(max_capital, record['capital'])
            drawdown = (max_capital - record['capital']) / max_capital * 100
            max_drawdown = max(max_drawdown, drawdown)
        
        print(f"💰 初始资金: ${self.initial_capital:,.2f}")
        print(f"💰 最终资金: ${final_capital:,.2f}")
        print(f"📈 总收益率: {total_return:+.2f}%")
        print(f"📊 总交易次数: {total_trades}")
        print(f"✅ 盈利交易: {winning_trades}")
        print(f"❌ 亏损交易: {losing_trades}")
        print(f"🎯 胜率: {win_rate:.1f}%")
        print(f"📉 最大回撤: {max_drawdown:.2f}%")
        
        # 生成CSV详细报告
        self.generate_trade_csv_report()

        # 🆕 生成“信号反馈表”（包含发现但未成交的信号）
        self.generate_signal_csv_report()
        
        # 动态止盈统计分析
        print(f"\n{'='*80}")
        print("📊 动态止盈详细统计")
        print("="*80)
        
        # 统计不同止盈阈值的交易（只统计已平仓的）
        closed_trades = [t for t in self.trade_records if t.get('exit_reason') and t['exit_reason'] != 'holding']
        
        # 区分高止盈和普通止盈（使用tp_pct_used字段）
        # ⚠️ 修复：tp_pct_used现在是百分比数值（10 = 10%），而不是小数（0.10）
        trades_with_high_tp = [t for t in closed_trades if t.get('tp_pct_used') and t['tp_pct_used'] > 10.0]
        trades_with_normal_tp = [t for t in closed_trades if t.get('tp_pct_used') and t['tp_pct_used'] <= 10.0]
        
        high_tp_triggered = len(trades_with_high_tp)
        normal_tp_count = len(trades_with_normal_tp)
        total_closed = len(closed_trades)
        
        print(f"\n💰 止盈触发统计 (已平仓{total_closed}笔):")
        if total_closed > 0:
            print(f"  🚀 动态止盈(>10%)触发: {high_tp_triggered}次 ({high_tp_triggered/total_closed*100:.1f}%)")
            print(f"  📊 普通止盈(≤10%)触发: {normal_tp_count}次 ({normal_tp_count/total_closed*100:.1f}%)")
            print(f"  ⏳ 其他平仓: {total_closed-high_tp_triggered-normal_tp_count}次 ({(total_closed-high_tp_triggered-normal_tp_count)/total_closed*100:.1f}%)")
        
        # 动态止盈成功率分析
        if high_tp_triggered > 0:
            high_tp_success = len([t for t in trades_with_high_tp if t.get('exit_reason') == 'take_profit'])
            high_tp_profit = sum([t['pnl'] for t in trades_with_high_tp])
            high_tp_avg_profit = high_tp_profit / high_tp_triggered
            
            print(f"\n✅ 动态止盈表现:")
            print(f"  触发次数: {high_tp_triggered}次")
            print(f"  成功止盈: {high_tp_success}次")
            print(f"  成功率: {high_tp_success/high_tp_triggered*100:.1f}%")
            print(f"  总贡献: ${high_tp_profit:,.2f}")
            print(f"  平均收益: ${high_tp_avg_profit:,.2f}")
        
        # 普通止盈统计
        if normal_tp_count > 0:
            normal_tp_profit = sum([t['pnl'] for t in trades_with_normal_tp])
            normal_tp_avg = normal_tp_profit / normal_tp_count
            
            print(f"\n📈 普通止盈表现:")
            print(f"  触发次数: {normal_tp_count}次")
            print(f"  总贡献: ${normal_tp_profit:,.2f}")
            print(f"  平均收益: ${normal_tp_avg:,.2f}")
        
        # 止损、超时和强制平仓统计
        stop_loss_trades = [t for t in closed_trades if t.get('exit_reason') == 'stop_loss']
        max_hold_trades = [t for t in closed_trades if t.get('exit_reason') == 'max_hold_time']
        force_close_trades = [t for t in closed_trades if t.get('exit_reason') == 'force_close']
        
        if stop_loss_trades:
            stop_loss_total = sum([t['pnl'] for t in stop_loss_trades])
            print(f"\n⚠️ 止损统计:")
            print(f"  止损次数: {len(stop_loss_trades)}次 ({len(stop_loss_trades)/total_closed*100:.1f}%)")
            print(f"  止损损失: ${stop_loss_total:,.2f}")
        
        if max_hold_trades:
            max_hold_profit = sum([t['pnl'] for t in max_hold_trades])
            max_hold_positive = len([t for t in max_hold_trades if t['pnl'] > 0])
            print(f"\n⏰ 超时平仓统计:")
            print(f"  超时次数: {len(max_hold_trades)}次 ({len(max_hold_trades)/total_closed*100:.1f}%)")
            print(f"  盈利: {max_hold_positive}次, 亏损: {len(max_hold_trades)-max_hold_positive}次")
            print(f"  总盈亏: ${max_hold_profit:,.2f}")
        
        if force_close_trades:
            force_close_profit = sum([t['pnl'] for t in force_close_trades])
            force_close_positive = len([t for t in force_close_trades if t['pnl'] > 0])
            print(f"\n🔚 回测结束强制平仓:")
            print(f"  强制平仓: {len(force_close_trades)}次 ({len(force_close_trades)/total_closed*100:.1f}%)")
            print(f"  盈利: {force_close_positive}次, 亏损: {len(force_close_trades)-force_close_positive}次")
            print(f"  总盈亏: ${force_close_profit:,.2f}")
            print(f"  ℹ️ 注意：强制平仓的交易可能还未达到最佳止盈点")
        
        # 详细交易记录
        print(f"\n{'='*80}")
        print(f"📋 详细交易记录 (前20条):")
        print("-" * 120)
        print(f"{'序号':<4} {'交易对':<15} {'卖量倍数':<10} {'建仓日期':<12} {'建仓价':>10} {'平仓日期':<12} {'平仓价':>10} {'盈亏':>12} {'持仓天数':<10}")
        print("-" * 120)
        
        for i, trade in enumerate(self.trade_records[:20], 1):
            exit_info = f"{trade['exit_price']:.4f}" if trade['exit_price'] else "-"
            pnl_info = f"${trade['pnl']:+.2f}" if trade['pnl'] != 0 else "-"
            surge_ratio = f"{trade.get('buy_surge_ratio', 0):.1f}x"
            
            print(f"{i:<4} {trade['symbol']:<15} {surge_ratio:<10} {trade['entry_date']:<12} {trade['entry_price']:<10.4f} "
                  f"{trade['exit_date'] or '-':<12} {exit_info:>10} {pnl_info:>12} {trade.get('hold_days', 0):<10}")

    def _get_dynamic_tp_2h_result(self, trade):
        """获取2小时判断结果文本
        
        🔧 修复逻辑：根据持仓时长和止盈阈值，推断并显示判断结果
        1. 持仓<2小时：根据代码逻辑，固定使用强势币止盈（22%）→ 显示"强（2h内默认）"
        2. 持仓≥2小时：
           - 如果有判定字段，直接显示
           - 如果没有判定字段，根据止盈阈值推断（22%=强，20%=中，11%=弱）
        """
        # 计算持仓时长
        try:
            if trade.get('exit_datetime'):
                if isinstance(trade['entry_datetime'], str):
                    entry_dt = pd.to_datetime(trade['entry_datetime'], utc=True)
                else:
                    entry_dt = trade['entry_datetime']
                if isinstance(trade['exit_datetime'], str):
                    exit_dt = pd.to_datetime(trade['exit_datetime'], utc=True)
                else:
                    exit_dt = trade['exit_datetime']
                hold_hours = (exit_dt - entry_dt).total_seconds() / 3600
            else:
                # 还未平仓，使用当前时间计算
                if isinstance(trade['entry_datetime'], str):
                    entry_dt = pd.to_datetime(trade['entry_datetime'], utc=True)
                else:
                    entry_dt = trade['entry_datetime']
                now = datetime.now(timezone.utc)
                hold_hours = (now - entry_dt).total_seconds() / 3600
        except:
            hold_hours = 0
        
        # 如果持仓不足2小时，根据代码逻辑固定是强势币（22%）
        if hold_hours < 2.0:
            return "强（2h内默认）"
        
        # 持仓≥2小时，优先使用保存的2小时判断结果字段
        # 🔥 2小时判断只有两种结果：强势币或中等币（没有弱势币）
        # 🔧 优先检查 dynamic_tp_2h_result（专门保存2小时判断结果，不会被12小时判断覆盖）
        tp_2h_result = trade.get('dynamic_tp_2h_result')
        if tp_2h_result == 'strong':
            return "强（保持30%）"
        elif tp_2h_result == 'medium':
            return "中（改成20%）"
        
        # 兜底：使用旧逻辑（检查标记字段）
        if trade.get('dynamic_tp_strong') == True:
            return "强（保持30%）"
        elif trade.get('dynamic_tp_medium') == True:
            return "中（改成20%）"
        # ❌ 2小时判断不会产生弱势币，如果有dynamic_tp_weak，那是12小时判断设置的，不应该在这里显示
        
        # 如果没有判定字段，根据止盈阈值反推
        # 🔥 2小时判断只有两种结果：≥28%=强，<28%=中（21%）
        # ⚠️ 修复：tp_pct现在是百分比数值（28 = 28%），而不是小数（0.28）
        tp_pct = trade.get('tp_pct_used') or trade.get('dynamic_tp_pct')
        if tp_pct:
            tp_value = float(str(tp_pct).rstrip('%')) if isinstance(tp_pct, str) else float(tp_pct)
            if tp_value >= 28.0:  # 33%左右（强势币）
                return "强（推断30%）"
            elif tp_value >= 0.15:  # 20%左右
                return "中（推断20%）"
            # ❌ 如果止盈是10%左右，那一定是12小时判断设置的，不是2小时判断
            # 2小时判断不会产生10%止盈，所以这里不返回"弱"
        
        # 兜底：如果止盈阈值<15%，说明是12小时判断后的结果，2小时判断应该显示"中"
        return "中（推断20%）"
    
    def _get_dynamic_tp_12h_result(self, trade):
        """获取12小时判断结果文本
        
        🔧 修复逻辑：根据持仓时长、2小时判断和止盈阈值，推断并显示12小时判断结果
        1. 持仓<12小时：
           - 如果2小时判断为强 → 显示"强（保持22%）"
           - 如果2小时判断为中 → 显示"中（保持20%）"
           - 兜底根据止盈阈值推断
        2. 持仓≥12小时：
           - 优先使用保存的判定字段
           - 如果没有，根据止盈阈值推断
        """
        # 计算持仓时长
        try:
            if trade.get('exit_datetime'):
                if isinstance(trade['entry_datetime'], str):
                    entry_dt = pd.to_datetime(trade['entry_datetime'], utc=True)
                else:
                    entry_dt = trade['entry_datetime']
                if isinstance(trade['exit_datetime'], str):
                    exit_dt = pd.to_datetime(trade['exit_datetime'], utc=True)
                else:
                    exit_dt = trade['exit_datetime']
                hold_hours = (exit_dt - entry_dt).total_seconds() / 3600
            else:
                hold_hours = 0
        except:
            hold_hours = 0
        
        trigger = trade.get('dynamic_tp_trigger', '')
        
        # 持仓<12小时：继承2小时判断结果
        if hold_hours < 12.0:
            # 优先从2小时判断推断
            tp_2h = self._get_dynamic_tp_2h_result(trade)
            if "强" in tp_2h:
                return "强（保持30%）"
            elif "中" in tp_2h:
                return "中（保持20%）"
            # ❌ 2小时判断不会产生弱势币，如果出现则兜底处理
            
            # 兜底：根据止盈阈值推断
            # ⚠️ 修复：tp_pct现在是百分比数值
            tp_pct = trade.get('tp_pct_used') or trade.get('dynamic_tp_pct')
            if tp_pct:
                tp_value = float(str(tp_pct).rstrip('%')) if isinstance(tp_pct, str) else float(tp_pct)
                if tp_value >= 28.0:  # 33%左右（强势币）
                    return "强（保持30%）"
                elif tp_value >= 0.15:  # 20%左右
                    return "中（保持20%）"
            return "中（推断20%）"  # 兜底默认为中等币
        
        # 持仓≥12小时：优先使用保存的判定字段
        # 🔥 新逻辑：12小时判断后只有强（30%）或弱（12%），不再有中（20%）
        # 🔧 修复：明确检查 == True，避免 False 被当作缺失
        if trigger == '12h_strong_upgrade':
            return "强（30%）"
        elif trigger == '12h_weak':
            return "弱（12%）"
        elif trade.get('dynamic_tp_weak') == True:
            return "弱（12%）"
        elif trade.get('dynamic_tp_strong') == True:
            return "强（30%）"
        
        # 如果没有判定字段，根据止盈阈值反推
        # 🔥 新逻辑：12小时后只有33%或10%
        # ⚠️ 修复：tp_pct现在是百分比数值
        tp_pct = trade.get('tp_pct_used') or trade.get('dynamic_tp_pct')
        if tp_pct:
            tp_value = float(str(tp_pct).rstrip('%')) if isinstance(tp_pct, str) else float(tp_pct)
            if tp_value >= 28.0:  # 33%左右（强势币）
                return "强（30%）"
            elif tp_value >= 0.11:  # 12%左右
                return "弱（12%）"
            # 兜底：如果是15-20%之间的值（历史遗留数据），推断为中
            elif tp_value >= 0.15:
                return "中（20%历史遗留）"
        
        # 兜底：未知
        return "未知"

    def generate_trade_csv_report(self):
        """生成交易详细CSV报告"""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')  # 🔧 使用UTC时间
        csv_filename = f"sell_surge_backtest_report_{timestamp}.csv"
        
        # 🔧 调试日志：检查交易记录数量
        logging.info(f"📊 开始生成CSV报告: {csv_filename}")
        logging.info(f"📊 总交易记录数: {len(self.trade_records)}")
        if len(self.trade_records) == 0:
            logging.warning(f"⚠️  没有任何交易记录！无法生成CSV报告")
            # 仍然创建空文件，但只写入表头
            try:
                with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    fieldnames = ['序号', '交易对', '卖量暴涨倍数']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                logging.warning(f"⚠️  已创建空CSV文件（只有表头）: {csv_filename}")
            except Exception as e:
                logging.error(f"❌ 创建空CSV失败: {e}")
            return
        
        logging.info(f"📊 前3笔交易示例: {[t.get('symbol') for t in self.trade_records[:3]]}")
        
        try:
            with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                fieldnames = [
                    '序号', '交易对', '卖量暴涨倍数', '当日买量倍数', '当日卖量倍数', '买卖量比值', '信号时未平仓量', '建仓时未平仓量', '未平仓量比率', '买量加速度', '信号时间', 
                    # 🆕 信号价和建仓涨幅
                    '信号价', '建仓日期', '建仓具体时间', '建仓5分钟时刻', '建仓时多空比', '建仓价', '建仓涨幅%',
                    '平仓日期', '平仓具体时间', '平仓价', 
                    '持仓小时数', '盈亏百分比',
                    # 💰 交易手续费明细（2026-02-14新增）
                    '建仓手续费', '补仓手续费', '平仓手续费', '交易手续费合计',
                    # 💰 资金费
                    '资金费', '资金费次数', '平均费率%',
                    # 💰 多层级盈亏金额
                    '盈亏金额', '扣交易费后', '最终净盈亏', '真实盈亏金额', '余额',
                    # 🆕 连续爆判断
                    '连续爆',
                    # 🆕 动态止盈判断结果
                    '2小时判断', '12小时判断',
                    '平仓原因', '基差%', '杠杆倍数', '仓位金额',  # 🔧 改为"基差%"
                    '是否有补仓', '补仓价格', '补仓后平均价', 
                    # 🆕 建仓后价格走势分析（新增2h涨幅%和12h涨幅%）
                    '2h收盘价', '2h涨幅%', '2h最大跌幅%', '12h收盘价', '12h涨幅%', '12h最大跌幅%', '24h收盘价', '24小时涨幅%', '24h最大跌幅%', '72h最大跌幅%',
                    '最大跌幅%', '2小时最大涨幅%', '24小时最大涨幅%', '止盈阈值%',
                    # 🆕 添加顶级交易者数据字段
                    '建仓时账户多空比', '信号时账户多空比', '多空比变化率',
                    # 🆕 未平仓持仓的"当前浮盈亏"（按本地5m最新close做mark-to-market）
                    '当前5m时间', '当前5m收盘价', '当前浮盈金额', '当前浮盈百分比'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # 输出"已建仓"的交易：包含已平仓 + 回测结束时仍持仓（原来就是这样展示的）
                entered_trades = [
                    t for t in self.trade_records
                    if t.get('entry_date') and t.get('entry_price')
                ]
                
                # 🆕 累计余额计算（从初始资金10000开始）
                cumulative_balance = self.initial_capital

                for i, trade in enumerate(entered_trades, 1):
                    # 计算补仓后平均价
                    avg_price_after_add = ''
                    if trade.get('has_add_position', False) and trade.get('add_position_price'):
                        avg_price_after_add = f"{trade['avg_entry_price']:.6f}"
                    
                    # 🆕 获取建仓具体时间（小时）和建仓5分钟时刻
                    # 🔥 修复：entry_datetime已经是精确的5分钟时刻了
                    entry_datetime_str = ''
                    entry_5m_str = ''
                    if trade.get('entry_datetime'):
                        try:
                            if isinstance(trade['entry_datetime'], str):
                                entry_dt = pd.to_datetime(trade['entry_datetime'], utc=True)  # 🔧 指定UTC
                            else:
                                entry_dt = trade['entry_datetime']
                            # 🔧 手动格式化确保UTC（strftime可能受系统时区影响）
                            entry_datetime_str = f"{entry_dt.year:04d}-{entry_dt.month:02d}-{entry_dt.day:02d} {entry_dt.hour:02d}:{entry_dt.minute:02d}:{entry_dt.second:02d}"
                            entry_5m_str = entry_datetime_str  # 🔥 直接使用同一个值
                        except:
                            entry_datetime_str = trade.get('entry_date', '') + ' 00:00:00'
                            entry_5m_str = entry_datetime_str
                    else:
                        entry_datetime_str = trade.get('entry_date', '') + ' 00:00:00'
                        entry_5m_str = entry_datetime_str
                    
                    # 🆕 获取平仓具体时间
                    exit_datetime_str = ''
                    if trade.get('exit_datetime'):
                        try:
                            if isinstance(trade['exit_datetime'], str):
                                exit_dt = pd.to_datetime(trade['exit_datetime'], utc=True)  # 🔧 指定UTC
                            else:
                                exit_dt = trade['exit_datetime']
                            # 🔧 手动格式化确保UTC（strftime可能受系统时区影响）
                            exit_datetime_str = f"{exit_dt.year:04d}-{exit_dt.month:02d}-{exit_dt.day:02d} {exit_dt.hour:02d}:{exit_dt.minute:02d}:{exit_dt.second:02d}"
                        except:
                            exit_datetime_str = trade.get('exit_date', '') + ' 00:00:00' if trade.get('exit_date') else ''
                    else:
                        exit_datetime_str = trade.get('exit_date', '') + ' 00:00:00' if trade.get('exit_date') else ''
                    
                    # 🆕 计算持仓小时数（使用精确的5分钟建仓时刻，精确到小数点后2位）
                    # 🔥 entry_datetime已经是精确的5分钟时刻
                    hold_hours = 0
                    try:
                        if trade.get('entry_datetime'):
                            entry_dt = pd.to_datetime(trade['entry_datetime'], utc=True)  # 🔧 指定UTC
                        else:
                            entry_dt = datetime.strptime(trade.get('entry_date', ''), '%Y-%m-%d').replace(tzinfo=timezone.utc)

                        if trade.get('exit_datetime'):
                            exit_dt = pd.to_datetime(trade['exit_datetime'], utc=True)  # 🔧 指定UTC
                        elif trade.get('exit_date'):
                            exit_dt = pd.to_datetime(trade['exit_date'] + ' 23:59:59', utc=True)  # 🔧 指定UTC
                        else:
                            end_date = getattr(self, '_backtest_end_date', None)
                            exit_dt = pd.to_datetime((end_date or trade.get('entry_date')) + ' 23:59:59', utc=True)  # 🔧 指定UTC

                        # 精确到小数点后2位（对于小于1小时的交易特别重要）
                        hold_hours = round((exit_dt - entry_dt).total_seconds() / 3600, 2)
                    except Exception:
                        hold_hours = trade.get('hold_days', 0) * 24

                    # 🆕 若未平仓：用"当前时间最近一根5m close"计算浮盈亏（不会影响回测统计，仅用于观察）
                    m2m_trade_time = ''
                    m2m_close = ''
                    m2m_pnl_amt = ''
                    m2m_pnl_pct = ''
                    if not trade.get('exit_date'):
                        td, close = self.get_latest_5m_close(trade['symbol'])
                        if td and close is not None:
                            m2m_trade_time = td
                            m2m_close = f"{close:.6f}"
                            try:
                                entry_price_for_pnl = float(trade.get('avg_entry_price') or trade.get('entry_price') or 0)
                                position_size = float(trade.get('position_size') or 0)
                                if entry_price_for_pnl > 0 and position_size > 0:
                                    # 🔥 修复：做空盈亏公式 = (建仓价 - 当前价) * 持仓量
                                    upnl = (entry_price_for_pnl - close) * position_size
                                    upnl_pct = (entry_price_for_pnl - close) / entry_price_for_pnl * 100
                                    m2m_pnl_amt = f"{upnl:.2f}"
                                    m2m_pnl_pct = f"{upnl_pct:.2f}%"
                            except Exception:
                                pass
                    
                    # 🆕 计算累计余额（只对已平仓的交易累加盈亏）
                    # 💰 修改：使用扣除所有成本后的最终净盈亏（交易手续费+资金费）
                    trade_balance = ''
                    if trade.get('exit_date'):
                        # 已平仓：累加扣除所有成本后的净盈亏
                        pnl_final = trade.get('pnl_after_all_costs', trade.get('pnl_after_funding', trade.get('pnl', 0)))
                        cumulative_balance += pnl_final
                        trade_balance = f"{cumulative_balance:.2f}"
                    
                    # 🆕 计算建仓后价格统计数据（2h、12h、24h、72h）
                    price_stats = {}
                    if trade.get('entry_datetime') and trade.get('entry_price'):
                        try:
                            if isinstance(trade['entry_datetime'], str):
                                entry_dt = pd.to_datetime(trade['entry_datetime'], utc=True)  # 🔧 指定UTC
                            else:
                                entry_dt = trade['entry_datetime']
                            entry_price = float(trade['entry_price'])
                            price_stats = self.calculate_post_entry_price_stats(trade['symbol'], entry_dt, entry_price)
                        except Exception as e:
                            logging.debug(f"计算价格统计失败 {trade['symbol']}: {e}")
                            price_stats = {
                                '2h_close': '',
                                '2h_rise_pct': '',
                                '2h_max_drop_pct': '',
                                '12h_close': '',
                                '12h_rise_pct': '',
                                '12h_max_drop_pct': '',
                                '24h_close': '',
                                '24h_max_drop_pct': '',
                                '72h_max_drop_pct': ''
                            }
                    else:
                        price_stats = {
                            '2h_close': '',
                            '2h_rise_pct': '',
                            '2h_max_drop_pct': '',
                            '12h_close': '',
                            '12h_rise_pct': '',
                            '12h_max_drop_pct': '',
                            '24h_close': '',
                            '24h_max_drop_pct': '',
                            '72h_max_drop_pct': ''
                        }
                    
                    # 🆕 计算建仓涨幅%（建仓价相对信号价的涨幅）
                    signal_price = trade.get('signal_price', 0)
                    entry_rise_pct = ''
                    if signal_price and signal_price > 0:
                        entry_price_val = float(trade['entry_price'])
                        entry_rise_pct = f"{((entry_price_val - signal_price) / signal_price * 100):.2f}%"
                    
                    row = {
                        '序号': i,
                        '交易对': trade['symbol'],
                        '卖量暴涨倍数': f"{trade.get('buy_surge_ratio', 0):.1f}",  # 🔧 修复：字段名改为卖量暴涨倍数
                        '当日买量倍数': f"{trade.get('intraday_buy_ratio', 0):.1f}",  # 🆕 当日买量倍数（建仓前12小时小时间最大比值）
                        '当日卖量倍数': f"{trade.get('intraday_sell_ratio', 0):.1f}",  # 🆕 当日卖量倍数（建仓前12小时小时间最大比值）
                        '买卖量比值': (  # 🆕 买卖量比值（买量倍数/卖量倍数）
                            f"{trade.get('intraday_buy_ratio', 0) / trade.get('intraday_sell_ratio', 1):.3f}" 
                            if trade.get('intraday_sell_ratio', 0) > 0 
                            else "0.000"
                        ),
                        '信号时未平仓量': (  # 🆕 信号时未平仓量（美元价值）
                            f"${trade.get('signal_open_interest', 0):,.0f}" 
                            if trade.get('signal_open_interest', 0) > 0 
                            else ""
                        ),
                        '建仓时未平仓量': (  # 🆕 建仓时未平仓量（美元价值）
                            f"${trade.get('entry_open_interest', 0):,.0f}" 
                            if trade.get('entry_open_interest', 0) > 0 
                            else ""
                        ),
                        '未平仓量比率': (  # 🆕 信号时/建仓时未平仓量比率
                            f"{trade.get('signal_open_interest', 0) / trade.get('entry_open_interest', 1):.3f}" 
                            if trade.get('entry_open_interest', 0) > 0 and trade.get('signal_open_interest', 0) > 0
                            else ""
                        ),
                        '买量加速度': f"{trade.get('buy_acceleration', 0):.4f}",  # 🆕 买量加速度（最后6h vs 前18h买卖比差值）
                        '信号时间': trade.get('signal_date', ''),  # 🆕 信号时间（已经包含小时）
                        '信号价': f"{signal_price:.6f}" if signal_price and signal_price > 0 else '',  # 🆕 信号价
                        '建仓日期': trade['entry_date'],
                        '建仓具体时间': entry_datetime_str,  # 小时K线时间
                        '建仓5分钟时刻': entry_5m_str,  # 🆕 精确的5分钟建仓时刻
                        '建仓时多空比': f"{trade.get('entry_account_ratio', 0):.2f}" if trade.get('entry_account_ratio') else "",  # 🆕 建仓时多空比
                        '建仓价': f"{trade['entry_price']:.6f}",
                        '建仓涨幅%': entry_rise_pct,  # 🆕 建仓价相对信号价的涨幅
                        '平仓日期': trade.get('exit_date', ''),
                        # 🆕 平仓具体时间：未平仓时用估值5m时间（便于你看"按哪个时刻估值"）
                        '平仓具体时间': exit_datetime_str if trade.get('exit_date') else (m2m_trade_time or ''),
                        # 🆕 平仓价：未平仓时填入估值价（最新5m close）
                        '平仓价': (
                            f"{trade.get('exit_price', 0):.6f}" if trade.get('exit_price') else ''
                        ) if trade.get('exit_date') else (m2m_close or ''),
                        '持仓小时数': f"{hold_hours:.2f}" if hold_hours else '',
                        '盈亏百分比': f"{trade.get('pnl_pct', 0):.2f}%" if trade.get('exit_date') else (m2m_pnl_pct or ''),
                        # 💰 交易手续费明细（2026-02-14新增）
                        '建仓手续费': (
                            f"{trade.get('entry_fee', 0):.4f}" if trade.get('entry_fee') else ''
                        ),
                        '补仓手续费': (
                            f"{trade.get('add_position_fees', 0):.4f}" if trade.get('exit_date') and trade.get('add_position_fees', 0) > 0 else ''
                        ),
                        '平仓手续费': (
                            f"{trade.get('exit_fee', 0):.4f}" if trade.get('exit_date') else ''
                        ),
                        '交易手续费合计': (
                            f"{trade.get('total_trading_fee', 0):.4f}" if trade.get('exit_date') else ''
                        ),
                        # 💰 资金费
                        '资金费': (
                            f"{trade.get('funding_fee', 0):.4f}" if trade.get('exit_date') else ''
                        ),  # 正数=支出，负数=收入
                        '资金费次数': (
                            str(trade.get('funding_fee_count', 0)) if trade.get('exit_date') and trade.get('funding_fee_count') else ''
                        ),
                        '平均费率%': (
                            f"{trade.get('funding_fee_avg_rate', 0):.4f}%" if trade.get('exit_date') and trade.get('funding_fee_avg_rate') else ''
                        ),
                        # 💰 多层级盈亏金额
                        # 🆕 盈亏：虚拟补仓交易使用real_pnl（首仓实际亏损），否则使用pnl
                        '盈亏金额': (
                            f"{trade.get('real_pnl', trade.get('pnl', 0)):.2f}" 
                            if trade.get('is_virtual_tracking') and trade.get('exit_date')
                            else f"{trade.get('pnl', 0):.2f}"
                        ) if trade.get('exit_date') else (m2m_pnl_amt or ''),
                        # 💰 扣除交易手续费后盈亏
                        '扣交易费后': (
                            f"{trade.get('pnl_after_trading_fee', trade.get('pnl', 0)):.2f}"
                            if trade.get('exit_date')
                            else ''
                        ),
                        # 💰 最终净盈亏（扣除交易手续费+资金费）
                        '最终净盈亏': (
                            f"{trade.get('pnl_after_all_costs', trade.get('pnl', 0)):.2f}"
                            if trade.get('exit_date')
                            else ''
                        ),
                        # 💰 真实盈亏金额（兼容旧字段，保持与pnl_after_all_costs相同）
                        '真实盈亏金额': (
                            f"{trade.get('pnl_after_all_costs', trade.get('pnl_after_funding', trade.get('pnl', 0))):.2f}"
                            if trade.get('exit_date')
                            else ''
                        ),
                        '余额': trade_balance,  # 🆕 累计余额（使用真实盈亏）
                        # 🆕 连续爆判断（检查建仓时是否为连续2小时卖量暴涨）
                        '连续爆': '是' if trade.get('is_consecutive_surge_at_entry', False) else '否',
                        # 🆕 动态止盈判断结果
                        '2小时判断': self._get_dynamic_tp_2h_result(trade),
                        '12小时判断': self._get_dynamic_tp_12h_result(trade),
                        '平仓原因': trade.get('exit_reason', '') or ('holding' if not trade.get('exit_date') else ''),
                        # 🔧 显示建仓时基差（Premium Index）（2026-02-07改）
                        '基差%': (
                            f"{trade.get('entry_premium', 0)*100:.4f}%" 
                            if trade.get('entry_premium') is not None
                            else '数据缺失'
                        ),
                        '杠杆倍数': trade['leverage'],
                        '仓位金额': f"{trade['position_value']:.2f}",
                        '是否有补仓': '✅是' if trade.get('has_add_position', False) else '否',
                        '补仓价格': f"{trade.get('add_position_price', 0):.6f}" if trade.get('add_position_price') else '',
                        '补仓后平均价': avg_price_after_add,
                        '2h收盘价': price_stats.get('2h_close', ''),
                        '2h涨幅%': price_stats.get('2h_rise_pct', ''),  # 🆕 2小时涨幅
                        '2h最大跌幅%': price_stats.get('2h_max_drop_pct', ''),
                        '12h收盘价': price_stats.get('12h_close', ''),
                        '12h涨幅%': price_stats.get('12h_rise_pct', ''),  # 🆕 12小时涨幅
                        '12h最大跌幅%': price_stats.get('12h_max_drop_pct', ''),
                        '24h收盘价': price_stats.get('24h_close', ''),
                        '24小时涨幅%': (
                            f"{float(trade.get('close_gain_24h'))*100:.2f}%" if trade.get('close_gain_24h') is not None else ''
                        ),  # 🆕 24小时收盘价涨幅（第24小时close相对建仓价的涨幅）
                        '24h最大跌幅%': price_stats.get('24h_max_drop_pct', ''),
                        '72h最大跌幅%': price_stats.get('72h_max_drop_pct', ''),
                        # 原有的最大跌幅和涨幅字段
                        '最大跌幅%': f"{trade.get('max_drawdown', 0)*100:.2f}%" if trade.get('max_drawdown') else '0.00%',
                        '2小时最大涨幅%': (
                            f"{float(trade.get('max_up_2h'))*100:.2f}%" if trade.get('max_up_2h') is not None else ''
                        ),
                        '24小时最大涨幅%': (
                            f"{float(trade.get('max_up_24h'))*100:.2f}%" if trade.get('max_up_24h') is not None else ''
                        ),
                        # 真实止盈阈值（仅 take_profit 平仓时有意义）
                        # - 旧版这里用 .0f 会把 8.5% 四舍五入成 8%，导致误判"动态止盈没生效"
                        # ⚠️ 修复：tp_pct_used现在是百分比数值（10），不需要再乘以100
                        '止盈阈值%': (
                            f"{float(trade.get('tp_pct_used')):.1f}%" if trade.get('tp_pct_used') else ''
                        ),
                        # 🆕 添加顶级交易者数据
                        '建仓时账户多空比': f"{trade.get('entry_account_ratio', 0):.4f}" if trade.get('entry_account_ratio') else "",
                        '信号时账户多空比': f"{trade.get('signal_account_ratio', 0):.4f}" if trade.get('signal_account_ratio') else "",
                        '多空比变化率': f"{trade.get('account_ratio_change_rate', 0):.4f}" if trade.get('account_ratio_change_rate') else "",
                        '当前5m时间': m2m_trade_time,
                        '当前5m收盘价': m2m_close,
                        '当前浮盈金额': m2m_pnl_amt,
                        '当前浮盈百分比': m2m_pnl_pct
                    }
                    writer.writerow(row)
            
            logging.info(f"✅ CSV写入完成，共{len(entered_trades)}笔交易")
            print(f"📄 交易详细CSV报告已生成: {csv_filename}")
        
        except Exception as e:
            logging.error(f"❌ 生成CSV报告失败: {e}")
            import traceback
            logging.error(f"异常堆栈:\n{traceback.format_exc()}")
            print(f"❌ 生成CSV报告失败: {e}")

    def _update_signal_record(self, symbol: str, signal_date: str, status: str,
                              entry_datetime=None, entry_price=None, note: str = '', signal_id: str = None):
        """更新信号记录状态（用于反馈表）
        
        Args:
            signal_id: 🆕 唯一信号ID，如果提供则优先使用ID匹配（更精确）
            symbol: 交易对（作为备用匹配）
            signal_date: 信号时间（作为备用匹配）
        """
        if not symbol or not signal_date:
            return
        
        # 🆕 优先使用 signal_id 匹配（更精确，避免混淆）
        if signal_id:
            for rec in reversed(self.signal_records):
                if rec.get('signal_id') == signal_id:
                    rec['status'] = status
                    if entry_datetime is not None and hasattr(entry_datetime, 'strftime'):
                        rec['entry_time'] = entry_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    if entry_price is not None:
                        try:
                            rec['entry_price'] = f"{float(entry_price):.6f}"
                        except Exception:
                            rec['entry_price'] = str(entry_price)
                    if note:
                        rec['note'] = note
                    return
        
        # 🔙 备用方案：使用 symbol + signal_date 匹配（兼容旧代码）
        sd = str(signal_date)
        for rec in reversed(self.signal_records):
            if rec.get('symbol') == symbol and str(rec.get('signal_date')) == sd:
                rec['status'] = status
                if entry_datetime is not None and hasattr(entry_datetime, 'strftime'):
                    rec['entry_time'] = entry_datetime.strftime('%Y-%m-%d %H:%M:%S')
                if entry_price is not None:
                    try:
                        rec['entry_price'] = f"{float(entry_price):.6f}"
                    except Exception:
                        rec['entry_price'] = str(entry_price)
                if note:
                    rec['note'] = note
                return

    def generate_signal_csv_report(self):
        """生成信号反馈CSV（包含：发现信号但未成交）"""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')  # 🔧 使用UTC时间
        csv_filename = f"sell_surge_signal_feedback_{timestamp}.csv"
        try:
            # 反馈表允许包含“未成交信号”（用于核对FHE/BDXN等为什么没成交）
            for rec in self.signal_records:
                if rec.get('status') == 'pending':
                    rec['status'] = 'unfilled'
                    if not rec.get('note'):
                        rec['note'] = '回测区间内未触发目标价/未成交'

            import csv
            with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                fieldnames = [
                    'symbol', 'buy_surge_ratio', 'signal_time', 'signal_date', 'earliest_entry_time',
                    'signal_price', 'target_drop_pct', 'target_price', 'timeout_time',
                    'status', 'entry_time', 'entry_price', 'note'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for rec in self.signal_records:
                    writer.writerow({k: rec.get(k, '') for k in fieldnames})

            print(f"📄 信号反馈CSV已生成(含未成交): {csv_filename}")
        except Exception as e:
            print(f"❌ 生成信号反馈CSV失败: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='卖量暴涨策略回测程序')
    parser.add_argument(
        '--start-time',
        type=str,
        default='2025-11-01',
        help='开始日期(默认: 2025-11-01)'
    )
    parser.add_argument(
        '--end-time',
        type=str,
        default='2026-02-07',
        help='结束日期(默认: 2026-02-07)'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=None,
        help='卖量暴涨阈值(默认: 使用代码中的sell_surge_threshold参数)'
    )

    parser.add_argument(
        '--max-multiple',
        type=float,
        default=None,
        help='卖量暴涨倍数上限（默认: 使用代码中的sell_surge_max参数）'
    )

    parser.add_argument(
        '--dynamic-tp-boost',
        type=float,
        default=None,
        help='动态止盈加成幅度（传入则覆盖按倍数分档的加成；例如 0.05 表示统一 +5%%）'
    )

    parser.add_argument(
        '--dynamic-tp-lookback-minutes',
        type=int,
        default=None,
        help='动态止盈"强势判定"窗口长度（分钟; 默认: 使用代码中的参数）'
    )

    parser.add_argument(
        '--dynamic-tp-close-up-pct',
        type=float,
        default=None,
        help='动态止盈强势判定：5m close 需要高于建仓价的涨幅比例（默认: 使用代码中的参数）'
    )

    parser.add_argument(
        '--enable-trader-filter',
        action='store_true',
        default=None,
        help='启用顶级交易者数据风控（默认: 使用代码中的enable_trader_filter参数）'
    )

    parser.add_argument(
        '--min-account-ratio',
        type=float,
        default=None,
        help='最小账户多空比阈值（默认: 使用代码中的参数）'
    )

    parser.add_argument(
        '--account-stop-threshold',
        type=float,
        default=None,
        help='账户多空比下降止损阈值（默认: 使用代码中的参数）'
    )
    
    args = parser.parse_args()
    
    backtest = BuySurgeBacktest()
    
    # 🎯 只在命令行明确指定参数时才覆盖（否则使用代码中的默认值）
    if args.threshold is not None:
        backtest.sell_surge_threshold = args.threshold
        logging.info(f"卖量暴涨阈值设置为: {args.threshold}倍")

    if args.max_multiple is not None:
        backtest.sell_surge_max = float(args.max_multiple)
        logging.info(f"卖量暴涨倍数上限设置为: {backtest.sell_surge_max}倍")

    # 若显式传入 --dynamic-tp-boost，则用"统一加成"覆盖分档配置（便于做对照实验）
    if args.dynamic_tp_boost is not None:
        backtest.dynamic_tp_boost_pct = float(args.dynamic_tp_boost)
        backtest.dynamic_tp_boost_config = [(9999, backtest.dynamic_tp_boost_pct)]
        logging.info(f"动态止盈加成幅度设置为(覆盖分档): +{backtest.dynamic_tp_boost_pct*100:.1f}%")

    if args.dynamic_tp_lookback_minutes is not None:
        backtest.dynamic_tp_lookback_minutes = int(args.dynamic_tp_lookback_minutes)
        logging.info(f"动态止盈强势判定窗口设置为: {backtest.dynamic_tp_lookback_minutes}分钟")

    if args.dynamic_tp_close_up_pct is not None:
        backtest.dynamic_tp_close_up_pct = float(args.dynamic_tp_close_up_pct)
        logging.info(f"动态止盈强势判定涨幅阈值设置为: +{backtest.dynamic_tp_close_up_pct*100:.1f}%")
    
    # 🆕 应用顶级交易者风控参数
    if args.enable_trader_filter:
        backtest.enable_trader_filter = True
        if args.min_account_ratio is not None:
            backtest.min_account_ratio = args.min_account_ratio
        if args.account_stop_threshold is not None:
            backtest.account_ratio_stop_threshold = args.account_stop_threshold
        logging.info(f"✅ 启用顶级交易者风控")
        logging.info(f"   - 最小账户多空比: {backtest.min_account_ratio}")
        logging.info(f"   - 下降止损阈值: {backtest.account_ratio_stop_threshold}")
    
    try:
        # 运行回测
        backtest.run_backtest(args.start_time, args.end_time)
        
        # 生成报告
        backtest.generate_report()
    
    except KeyboardInterrupt:
        logging.info("用户中断回测")
    except Exception as e:
        logging.error(f"回测过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logging.info("回测程序结束")

if __name__ == "__main__":
    main()
