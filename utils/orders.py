"""Order utility functions extracted from ae_server.py (lines 160-261)."""

import logging
from typing import List, Optional, Tuple

from binance.exceptions import BinanceAPIException

# 币安 U 本位合约：算法单 orderType（与 futures_get_open_algo_orders 一致）
FUTURES_ALGO_TP_TYPES = frozenset(
    {
        "TAKE_PROFIT_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_LIMIT",
    }
)
FUTURES_ALGO_SL_TYPES = frozenset(
    {
        "STOP_MARKET",
        "STOP",
        "STOP_LIMIT",
    }
)


def futures_algo_trigger_price(order: dict):
    """读取算法单触发价（新接口多为 triggerPrice，部分字段为 stopPrice）。"""
    for key in ("triggerPrice", "stopPrice", "activatePrice"):
        v = order.get(key)
        if v is None or v == "":
            continue
        try:
            f = float(v)
            if f > 0:
                return f
        except (TypeError, ValueError):
            continue
    return None


def position_close_side(is_long: bool) -> str:
    """单向持仓下平仓方向：多仓用 SELL，空仓用 BUY。"""
    return "SELL" if is_long else "BUY"


def pick_tp_sl_algo_candidates(
    algo_orders: List[dict],
    close_side: str,
    preferred_tp_id: Optional[str] = None,
    preferred_sl_id: Optional[str] = None,
) -> Tuple[Optional[dict], Optional[dict]]:
    """在开放算法单中选与本仓平仓方向一致的止盈、止损单（各一张；多单位时优先匹配本地记录的 algoId）。"""
    tps = [
        o
        for o in algo_orders
        if o.get("orderType") in FUTURES_ALGO_TP_TYPES and o.get("side") == close_side
    ]
    sls = [
        o
        for o in algo_orders
        if o.get("orderType") in FUTURES_ALGO_SL_TYPES and o.get("side") == close_side
    ]

    def _pick(cands: List[dict], pref: Optional[str]) -> Optional[dict]:
        if not cands:
            return None
        if pref:
            ps = str(pref).strip()
            for o in cands:
                oid = str(o.get("algoId") or o.get("orderId") or "")
                if oid == ps:
                    return o
        return cands[0]

    return _pick(tps, preferred_tp_id), _pick(sls, preferred_sl_id)


def algo_order_id_from_dict(order: Optional[dict]) -> Optional[str]:
    if not order:
        return None
    s = str(order.get("algoId") or order.get("orderId") or "").strip()
    return s or None


def cancel_order_algo_or_regular(client, symbol: str, order_id_str: str) -> bool:
    """先按算法单 algoId 取消，再尝试普通 orderId。"""
    if not order_id_str:
        return False
    oid = str(order_id_str).strip()
    try:
        client.futures_cancel_algo_order(symbol=symbol, algoId=int(oid))
        return True
    except BinanceAPIException as e:
        logging.debug(f"取消算法单失败 {symbol} algoId={oid}: {e}")
    except (OSError, TimeoutError) as e:
        logging.warning(f"网络错误取消算法单 {symbol}: {e}")
    except Exception as e:
        logging.debug(f"取消算法单失败 {symbol}: {e}")
    try:
        client.futures_cancel_order(symbol=symbol, orderId=int(oid))
        return True
    except BinanceAPIException as e:
        logging.debug(f"取消普通单失败 {symbol} orderId={oid}: {e}")
        return False
    except (OSError, TimeoutError) as e:
        logging.warning(f"网络错误取消普通单 {symbol}: {e}")
        return False
    except Exception as e:
        logging.error(f"取消订单失败 {symbol}: {e}")
        return False
