from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from config import SETTINGS
from utils.slack import notify_trade


log = logging.getLogger("disclosure.engine.monitor")
_MONITOR_WARN_THROTTLE: dict[str, float] = {}


def _warn_monitor_throttled(key: str, message: str, *args, interval_sec: float = 120.0) -> None:
    now_ts = time.time()
    last_ts = _MONITOR_WARN_THROTTLE.get(key, 0.0)
    if now_ts - last_ts < interval_sec:
        return
    _MONITOR_WARN_THROTTLE[key] = now_ts
    log.warning(message, *args)


def monitor_sleep_jitter(engine, base_sec: float, jitter_sec: float = 0.25):
    time.sleep(max(0.0, base_sec + random.random() * jitter_sec))


def monitor_safe_last_price(engine, broker, symbol: str, retries: int = 2) -> Optional[float]:
    r = max(1, int(retries))
    for i in range(r):
        try:
            px = broker.get_last_price(symbol)
            if px:
                return float(px)
        except Exception as exc:
            _warn_monitor_throttled(f"last_price:{symbol}", "[LAST-PRICE] %s fetch failed: %s", symbol, str(exc)[:160])
        monitor_sleep_jitter(engine, 0.25 * (2 ** i), 0.15)
    return None


def monitor_safe_sellable_qty(engine, broker, symbol: str, retries: int = 1) -> Optional[int]:
    r = max(1, int(retries))
    for i in range(r):
        try:
            qty = broker.get_sellable_qty(symbol)
            if qty is None:
                return None
            return int(float(qty))
        except Exception as exc:
            _warn_monitor_throttled(f"sellable_qty:{symbol}", "[SELLABLE-QTY] %s fetch failed: %s", symbol, str(exc)[:160])
            monitor_sleep_jitter(engine, 0.25 * (2 ** i), 0.2)
    return None


def monitor_sell_all_with_confirm(
    engine,
    broker,
    symbol: str,
    qty: int,
    tag: str,
    note: str,
    confirm_retries: int = 1,
) -> bool:
    if qty <= 0:
        return True

    def _try_once(qty_to_sell: int) -> bool:
        try:
            if engine._is_nxt_session():
                price = monitor_safe_last_price(engine, broker, symbol)
                if not price:
                    return False
                result = broker.sell_limit(symbol, qty_to_sell, price)
            else:
                result = broker.sell_market(symbol, qty_to_sell)
            notify_trade(tag, symbol, qty_to_sell, getattr(result, "ok", False), note)
            return bool(getattr(result, "ok", False))
        except Exception as exc:
            log.warning("[SELL-ERR] %s %s", symbol, str(exc)[:160])
            notify_trade(tag, symbol, qty_to_sell, False, f"{note} | err={str(exc)[:120]}")
            return False

    ok = _try_once(qty)
    if ok:
        return True

    monitor_sleep_jitter(engine, 1.2, 0.5)
    refreshed_qty = monitor_safe_sellable_qty(engine, broker, symbol, retries=engine._monitor_call_retries)
    if refreshed_qty is not None and refreshed_qty <= 0:
        return True

    for _ in range(max(0, int(confirm_retries))):
        if refreshed_qty is not None and refreshed_qty > 0:
            ok_retry = _try_once(refreshed_qty)
            if ok_retry:
                return True
            monitor_sleep_jitter(engine, 1.2, 0.5)
            refreshed_qty = monitor_safe_sellable_qty(engine, broker, symbol, retries=engine._monitor_call_retries)
            if refreshed_qty is not None and refreshed_qty <= 0:
                return True
    return False


def monitor_get_tp_sl(engine) -> Tuple[float, float]:
    tp = float(getattr(SETTINGS, "TAKE_PROFIT_PCT", 5.0))
    sl = float(getattr(SETTINGS, "STOP_LOSS_PCT", -2.0))
    return tp, sl


def monitor_normalize_positions(engine, raw: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if raw is None:
        return out

    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            sym = row.get("symbol") or row.get("pdno") or row.get("code") or row.get("ticker")
            qty = row.get("qty") or row.get("hldg_qty") or row.get("hold_qty")
            avg = row.get("avg_price") or row.get("pchs_avg_pric") or row.get("avg")
            try:
                qty_i = int(float(qty or 0))
            except Exception:
                qty_i = 0
            try:
                avg_f = float(avg) if avg not in (None, "", "0") else None
            except Exception:
                avg_f = None
            if sym and qty_i > 0:
                out.append({"symbol": str(sym), "qty": qty_i, "avg_price": avg_f})
        return out

    if isinstance(raw, dict):
        if "stocks" in raw and isinstance(raw.get("stocks"), list):
            return monitor_normalize_positions(engine, raw["stocks"])
        if "output1" in raw and isinstance(raw.get("output1"), list):
            return monitor_normalize_positions(engine, raw["output1"])
    return out


def monitor_fetch_positions(engine, broker) -> List[Dict[str, Any]]:
    nxt_flag = "X" if engine._is_nxt_session() else "N"
    for name in ("get_positions", "get_balance", "list_positions"):
        fn = getattr(broker, name, None)
        if not callable(fn):
            continue
        try:
            raw = fn(afhr_flpr_yn=nxt_flag)
            pos = monitor_normalize_positions(engine, raw)
            if pos:
                return pos
        except TypeError:
            try:
                raw = fn()
                return monitor_normalize_positions(engine, raw)
            except Exception as exc:
                _warn_monitor_throttled(f"fetch_positions_fallback:{name}", "[%s] fallback position fetch failed: %s", name, str(exc)[:160])
                continue
        except Exception as exc:
            log.warning("[%s] 조회 실패: %s", name, exc)
            continue
    return []


def monitor_try_get_avg_price(engine, broker, symbol: str) -> Optional[float]:
    for name in ("get_avg_price", "get_avg_buy_price", "get_position_avg_price"):
        fn = getattr(broker, name, None)
        if callable(fn):
            try:
                value = fn(symbol)
                if value is None:
                    return None
                return float(value)
            except Exception as exc:
                _warn_monitor_throttled(f"avg_price:{name}:{symbol}", "[%s] avg price fetch failed for %s: %s", name, symbol, str(exc)[:160])
                return None
    return None


def monitor_update_trailing_stop(engine, symbol, current_price):
    pos = engine.portfolio.get(symbol)
    if not pos:
        return None
    if current_price > pos.max_price:
        pos.max_price = current_price
    yield_pct = (current_price - pos.base_price) / pos.base_price * 100
    activation_pct = 6.0
    drop_threshold = 2.0
    if yield_pct >= activation_pct:
        sell_trigger_price = pos.max_price * (1 - drop_threshold / 100)
        if current_price <= sell_trigger_price:
            reason = f"TS(Trailing Stop): 고점({pos.max_price}) 대비 -{drop_threshold}% 하락"
            return {"action": "SELL", "reason": reason}
    return None
