from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from typing import Optional

from engine_monitor_core import (
    monitor_safe_last_price,
    monitor_safe_sellable_qty,
    monitor_sell_all_with_confirm,
    monitor_sleep_jitter,
    monitor_try_get_avg_price,
)
from engine_monitor_threading import monitor_force_eod_liquidate


log = logging.getLogger("disclosure.engine.monitor")


def _parse_hhmm(value: str, default: str) -> dt.time:
    text = str(value or default).strip()
    try:
        hour, minute = text.split(":", 1)
        return dt.time(int(hour), int(minute))
    except Exception:
        hour, minute = default.split(":", 1)
        return dt.time(int(hour), int(minute))


def monitor_and_sell(
    engine,
    broker,
    symbol: str,
    qty_hint: Optional[int],
    avg_price_hint: Optional[float],
    target_pct: float,
    stop_pct: float,
    monitor_policy: Optional[dict] = None,
):
    try:
        wait_fill_attempts = 60
        qty = 0
        for i in range(wait_fill_attempts):
            q = monitor_safe_sellable_qty(engine, broker, symbol, retries=1)
            if q is not None and q > 0:
                qty = int(q)
                break
            if i == 0:
                log.info("⏳ [MONITOR] %s 체결 확인 중... (Attempt %s)", symbol, i + 1)
            time.sleep(2)

        if qty <= 0:
            if qty_hint and qty_hint > 0:
                log.warning("⚠️ [FORCE-MONITOR] %s 잔고 확인 지연! 하지만 주문 수량(%s주)으로 매도 감시를 강제 시작합니다.", symbol, qty_hint)
                qty = qty_hint
                last_good_qty = qty
            else:
                log.warning("⚠️ [MONITOR-FAIL] %s 체결 확인 실패 & 힌트 없음 -> 감시 취소", symbol)
                return

        monitor_sleep_jitter(engine, 0.8, 0.6)
        qty = int(qty_hint or 0)
        q0 = monitor_safe_sellable_qty(engine, broker, symbol, retries=engine._monitor_call_retries)
        if q0 is not None and q0 > 0:
            qty = q0
        if qty <= 0:
            return

        start_price = float(avg_price_hint) if avg_price_hint is not None else None
        if start_price is None:
            start_price = monitor_try_get_avg_price(engine, broker, symbol)
        if not start_price:
            for _ in range(6):
                px = monitor_safe_last_price(engine, broker, symbol, retries=engine._monitor_call_retries)
                if px:
                    start_price = px
                    break
                monitor_sleep_jitter(engine, 0.8, 0.5)
        if not start_price:
            log.error("[MONITOR] %s cannot resolve start_price -> stop", symbol)
            return

        target_price = start_price * (1 + target_pct / 100.0)
        stop_price = start_price * (1 + stop_pct / 100.0)
        max_holding_price = start_price
        trailing_activated = False
        sl_breach_start_at = None
        sl_grace_period_sec = 900
        log.info("🎯 [MONITOR-START] %s qty=%s base=%.0f TP=%.0f(%.2f%%) SL=%.0f(%.2f%%)", symbol, qty, start_price, target_price, target_pct, stop_price, stop_pct)
        policy = dict(monitor_policy or {})
        skip_same_day_eod = bool(policy.get("skip_same_day_eod_liquidate", False))
        require_recover_by_close = bool(policy.get("require_recover_by_close", False))
        require_recover_after_open = bool(policy.get("require_recover_after_open", False))
        force_eod_liquidate = bool(policy.get("force_eod_liquidate", False))
        policy_entry_date = str(policy.get("entry_date") or "")
        policy_entry_style = str(policy.get("entry_style") or "")
        bootstrap_existing_position = bool(policy.get("bootstrap_existing_position", False))
        eod_skip_logged = False
        same_day_close_checked = False
        open_recovery_checked = False
        open_recovery_wait_logged = False
        sl_grace_period_sec = int(policy.get("stop_grace_period_sec", 900) or 900)
        open_recovery_min = int(policy.get("open_recovery_min", 0) or 0)
        max_hold_days = max(0, int(policy.get("max_hold_days", 0) or 0))
        time_exit_start = _parse_hhmm(str(policy.get("time_exit_start") or "15:10"), "15:10")
        time_exit_end = _parse_hhmm(str(policy.get("time_exit_end") or "15:20"), "15:20")
        sl_grace_label_min = max(1, int(round(sl_grace_period_sec / 60.0)))

        last_good_qty = qty
        zero_confirm = 0
        price_fail = 0
        next_qty_refresh_at = engine._now_kst()
        price_interval = float(engine._monitor_price_interval_sec)
        while True:
            now = engine._now_kst()
            eod_start = now.replace(hour=15, minute=29, second=0, microsecond=0)
            eod_deadline = now.replace(hour=15, minute=30, second=0, microsecond=0)
            if force_eod_liquidate and eod_start <= now < eod_deadline:
                if skip_same_day_eod and policy_entry_date == now.strftime("%Y-%m-%d"):
                    if not eod_skip_logged:
                        log.info(
                            "🕒 [EOD-SKIP] %s same-day close entry -> keep overnight (%s)",
                            symbol,
                            policy_entry_style or "close_bet",
                        )
                        eod_skip_logged = True
                else:
                    log.warning("⏰ [EOD] %s 15:29 reached -> force liquidate", symbol)
                    monitor_force_eod_liquidate(engine, broker, symbol, deadline=eod_deadline, qty_fallback=last_good_qty, tag="SELL(EOD)")
                    return
            if now.hour >= 20:
                log.info("[MONITOR] %s after 20:00 -> stop monitor", symbol)
                return

            if now >= next_qty_refresh_at:
                q = monitor_safe_sellable_qty(engine, broker, symbol, retries=1)
                if q is not None and q <= 0:
                    zero_confirm += 1
                    if zero_confirm >= engine._monitor_zero_confirm_n:
                        log.info("[MONITOR] %s position confirmed closed -> stop", symbol)
                        return
                elif q is not None:
                    zero_confirm = 0
                    last_good_qty = int(q)
                    qty = int(q)
                next_qty_refresh_at = now + dt.timedelta(seconds=float(engine._monitor_qty_refresh_sec))

            curr = monitor_safe_last_price(engine, broker, symbol, retries=1)
            if not curr:
                price_fail += 1
                if price_fail >= engine._monitor_max_price_fail:
                    price_interval = 10.0
                monitor_sleep_jitter(engine, price_interval, 0.5)
                continue
            price_fail = 0
            profit_rate = ((curr - start_price) / start_price) * 100.0
            if max_hold_days > 0 and policy_entry_date:
                try:
                    entry_day = dt.date.fromisoformat(policy_entry_date)
                except Exception:
                    entry_day = None
                if entry_day is not None:
                    hold_days = (now.date() - entry_day).days
                    if (
                        hold_days >= max_hold_days
                        and now.time() >= time_exit_start
                        and now.time() < time_exit_end
                    ):
                        log.info(
                            "🕒 [TIME-EXIT] %s max hold reached: %s days | current %.0f | pnl %+.2f%%",
                            symbol,
                            hold_days,
                            curr,
                            profit_rate,
                        )
                        if monitor_sell_all_with_confirm(
                            engine,
                            broker,
                            symbol,
                            last_good_qty,
                            "SELL(TIME)",
                            f"최대 보유 {max_hold_days}일 도달",
                            confirm_retries=1,
                        ):
                            return
            if (
                require_recover_by_close
                and not same_day_close_checked
                and policy_entry_date == now.strftime("%Y-%m-%d")
                and now >= eod_deadline
            ):
                if curr <= stop_price:
                    log.warning(
                        "🧯 [CLOSE-SL] %s 15:30 close check failed: current %.0f <= stop %.0f",
                        symbol,
                        curr,
                        stop_price,
                    )
                    if monitor_sell_all_with_confirm(
                        engine,
                        broker,
                        symbol,
                        last_good_qty,
                        "SELL(CLOSE-SL)",
                        "15:30 종가 기준 손절선 미회복",
                        confirm_retries=1,
                    ):
                        return
                else:
                    same_day_close_checked = True
                    log.info(
                        "✅ [CLOSE-CHECK] %s 15:30 close check passed: current %.0f > stop %.0f",
                        symbol,
                        curr,
                        stop_price,
                    )
                    sl_breach_start_at = None
            if require_recover_after_open and not open_recovery_checked and open_recovery_min > 0:
                today_str = now.strftime("%Y-%m-%d")
                is_known_overnight = bool(policy_entry_date) and policy_entry_date < today_str
                open_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
                open_deadline = open_start + dt.timedelta(minutes=open_recovery_min)
                bootstrap_session_deadline = now.replace(hour=10, minute=0, second=0, microsecond=0)
                can_check_bootstrap_open = (
                    bootstrap_existing_position
                    and not policy_entry_date
                    and open_start <= now <= bootstrap_session_deadline
                )
                if is_known_overnight or can_check_bootstrap_open:
                    if now < open_start:
                        pass
                    elif curr > stop_price:
                        open_recovery_checked = True
                        open_recovery_wait_logged = False
                        log.info(
                            "🌅 [OPEN-CHECK] %s opening risk check passed: current %.0f > stop %.0f",
                            symbol,
                            curr,
                            stop_price,
                        )
                    elif now < open_deadline:
                        if not open_recovery_wait_logged:
                            log.warning(
                                "🌅 [OPEN-WAIT] %s opened below stop %.0f (current %.0f). waiting until %s for recovery.",
                                symbol,
                                stop_price,
                                curr,
                                open_deadline.strftime("%H:%M"),
                            )
                            open_recovery_wait_logged = True
                    else:
                        log.warning(
                            "🧯 [OPEN-SL] %s open recovery failed: current %.0f <= stop %.0f by %s",
                            symbol,
                            curr,
                            stop_price,
                            open_deadline.strftime("%H:%M"),
                        )
                        if monitor_sell_all_with_confirm(
                            engine,
                            broker,
                            symbol,
                            last_good_qty,
                            "SELL(OPEN-SL)",
                            f"익일 시초 {open_recovery_min}분 내 손절선 미회복",
                            confirm_retries=1,
                        ):
                            return
            if curr > max_holding_price:
                max_holding_price = curr
                if trailing_activated:
                    log.info("↗️ [TS-HIGH] %s 최고가 갱신: %s (%+.2f%%)", symbol, f"{max_holding_price:,.0f}", profit_rate)

            if curr >= target_price:
                log.info("🚀 [TP-SELL] %s 목표가(%.0f원) 도달! 즉시 시장가 매도 실행 (현재가: %.0f원)", symbol, target_price, curr)
                if monitor_sell_all_with_confirm(engine, broker, symbol, last_good_qty, "SELL(TP)", f"목표가({target_pct}%) 즉시 도달", confirm_retries=1):
                    return

            if not trailing_activated and curr <= stop_price:
                if sl_breach_start_at is None:
                    sl_breach_start_at = now
                    log.warning(
                        "⚠️ [SL-BREACH] %s 손절선 이탈! (현재:%.2f%%). %s분 유예 시작.",
                        symbol,
                        profit_rate,
                        sl_grace_label_min,
                    )
            elif sl_breach_start_at is not None:
                log.info("✅ [SL-RECOVER] %s 가격 회복 완료 (%.0f). 손절 유예 취소.", symbol, curr)
                sl_breach_start_at = None

            if sl_breach_start_at is not None:
                elapsed = (now - sl_breach_start_at).total_seconds()
                if elapsed >= sl_grace_period_sec:
                    log.info(
                        "💧 [SL-FINAL] %s %s분 경과 (총 %s초). 즉시 강제 매도 집행!",
                        symbol,
                        sl_grace_label_min,
                        int(elapsed),
                    )
                    if monitor_sell_all_with_confirm(
                        engine,
                        broker,
                        symbol,
                        last_good_qty,
                        "SELL(SL)",
                        f"{sl_grace_label_min}분 유예 손절",
                    ):
                        return
            monitor_sleep_jitter(engine, price_interval, 0.5)
    except Exception as exc:
        log.error("[MONITOR-ERR] %s %s", symbol, str(exc)[:200])
    finally:
        with engine._monitor_lock:
            thread = engine._monitor_threads.get(symbol)
            if thread is not None and thread is threading.current_thread():
                engine._monitor_threads.pop(symbol, None)
