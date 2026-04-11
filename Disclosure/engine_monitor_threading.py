from __future__ import annotations

import datetime as dt
import logging
import threading
from typing import Optional

from config import SETTINGS
from engine_monitor_core import (
    monitor_fetch_positions,
    monitor_get_tp_sl,
    monitor_safe_sellable_qty,
    monitor_sell_all_with_confirm,
    monitor_sleep_jitter,
)


log = logging.getLogger("disclosure.engine.monitor")


def _default_close_bet_policy() -> dict:
    return {
        "entry_style": "close_bet",
        "skip_same_day_eod_liquidate": True,
        "require_recover_by_close": bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_BY_CLOSE", True)),
        "require_recover_after_open": bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_AFTER_OPEN", True)),
        "open_recovery_min": int(getattr(SETTINGS, "CLOSE_BET_OPEN_RECOVERY_MIN", 10) or 10),
        "max_hold_days": int(getattr(SETTINGS, "CLOSE_BET_MAX_HOLD_DAYS", 3) or 3),
        "time_exit_start": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_START", "15:10") or "15:10"),
        "time_exit_end": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_END", "15:20") or "15:20"),
        "force_eod_liquidate": bool(getattr(SETTINGS, "MONITOR_FORCE_EOD_LIQUIDATE", False)),
        "stop_grace_period_sec": int(getattr(SETTINGS, "CLOSE_BET_STOP_GRACE_MIN", 60) or 60) * 60,
        "bootstrap_existing_position": True,
    }


def monitor_bootstrap_position_monitors(engine, broker) -> None:
    if not engine._monitor_enabled():
        return
    try:
        now = engine._now_kst()
        if engine._monitor_last_bootstrap_at is not None:
            dt_sec = (now - engine._monitor_last_bootstrap_at).total_seconds()
            if dt_sec < engine._monitor_bootstrap_cooldown_sec:
                return

        positions = monitor_fetch_positions(engine, broker)
        for position in positions:
            symbol = position.get("symbol")
            qty = int(position.get("qty") or 0)
            if not symbol or qty <= 0:
                continue
            monitor_policy = None
            if bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False)) and str(getattr(SETTINGS, "MAIN_RUN_MODE", "") or "").strip().lower() == "event_log_watch":
                monitor_policy = _default_close_bet_policy()
            monitor_ensure_thread(
                engine,
                broker,
                symbol,
                qty_hint=qty,
                avg_price_hint=position.get("avg_price"),
                monitor_policy=monitor_policy,
            )
        engine._monitor_last_bootstrap_at = now
    except Exception as exc:
        log.warning("[MONITOR][BOOT-ERR] %s", str(exc)[:200])


def monitor_ensure_thread(
    engine,
    broker,
    symbol: str,
    qty_hint: Optional[int] = None,
    avg_price_hint: Optional[float] = None,
    tp_override: Optional[float] = None,
    monitor_policy: Optional[dict] = None,
) -> None:
    if not engine._monitor_enabled():
        return

    from engine_monitor_runtime import monitor_and_sell

    policy = dict(monitor_policy or {})
    if (
        not policy
        and bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False))
        and str(getattr(SETTINGS, "MAIN_RUN_MODE", "") or "").strip().lower() == "event_log_watch"
    ):
        policy = _default_close_bet_policy()

    with engine._monitor_lock:
        existing = engine._monitor_threads.get(symbol)
        if existing and existing.is_alive():
            return

        default_tp, default_sl = monitor_get_tp_sl(engine)
        if str(policy.get("entry_style") or "").strip() == "close_bet":
            runtime_tp = float(policy.get("take_profit_pct") or 0.0)
            runtime_sl = float(policy.get("stop_loss_pct") or 0.0)
            final_tp = tp_override if tp_override is not None else (runtime_tp or float(getattr(SETTINGS, "CLOSE_BET_TAKE_PROFIT_PCT", 8.0)))
            final_sl = runtime_sl or float(getattr(SETTINGS, "CLOSE_BET_STOP_LOSS_PCT", -4.0))
            policy.setdefault("force_eod_liquidate", bool(getattr(SETTINGS, "MONITOR_FORCE_EOD_LIQUIDATE", False)))
            policy.setdefault("stop_grace_period_sec", int(getattr(SETTINGS, "CLOSE_BET_STOP_GRACE_MIN", 60) or 60) * 60)
            policy.setdefault("skip_same_day_eod_liquidate", True)
            policy.setdefault("require_recover_by_close", bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_BY_CLOSE", True)))
            policy.setdefault("require_recover_after_open", bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_AFTER_OPEN", True)))
            policy.setdefault("open_recovery_min", int(getattr(SETTINGS, "CLOSE_BET_OPEN_RECOVERY_MIN", 10) or 10))
            policy.setdefault("max_hold_days", int(getattr(SETTINGS, "CLOSE_BET_MAX_HOLD_DAYS", 3) or 3))
            policy.setdefault("time_exit_start", str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_START", "15:10") or "15:10"))
            policy.setdefault("time_exit_end", str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_END", "15:20") or "15:20"))
        else:
            final_tp = tp_override if tp_override is not None else default_tp
            final_sl = default_sl
        th = threading.Thread(
            target=monitor_and_sell,
            args=(engine, broker, symbol, qty_hint, avg_price_hint, final_tp, final_sl, policy),
            daemon=True,
        )
        engine._monitor_threads[symbol] = th
        th.start()


def monitor_force_eod_liquidate(engine, broker, symbol: str, deadline: dt.datetime, qty_fallback: int, tag: str = "SELL(EOD)") -> bool:
    while engine._now_kst() < deadline:
        try:
            qty = monitor_safe_sellable_qty(engine, broker, symbol, retries=engine._monitor_call_retries)
            if qty is None:
                qty = qty_fallback
            if qty <= 0:
                return True
            ok = monitor_sell_all_with_confirm(
                engine,
                broker,
                symbol,
                int(qty),
                tag,
                f"{tag} 강제청산",
                confirm_retries=1,
            )
            if ok:
                return True
        except Exception as exc:
            log.warning("[%s-ERR] %s %s", tag, symbol, str(exc)[:120])
        monitor_sleep_jitter(engine, 2.0, 0.6)
    return False
