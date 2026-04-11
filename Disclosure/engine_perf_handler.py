from __future__ import annotations

import logging
import time
from typing import Optional

try:
    from config import THRESHOLDS
    from dart_common import is_perf_report_title
    from dart_perf import compute_surprise, fetch_perf_report_playwright
    from engine_fast_analyzers import analyze_fast_performance
    from engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery
except Exception:
    from Disclosure.config import THRESHOLDS
    from Disclosure.dart_common import is_perf_report_title
    from Disclosure.dart_perf import compute_surprise, fetch_perf_report_playwright
    from Disclosure.engine_fast_analyzers import analyze_fast_performance
    from Disclosure.engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery


log = logging.getLogger("disclosure.engine.handlers")


def handle_perf_report(
    *,
    engine,
    broker,
    stock_code: str,
    rcp_no: str,
    title: str,
    src: str,
    allow_trade: bool,
    page,
    pw,
    nav_timeout_ms: int,
    event_type: str,
    finish: FinishFn,
    guard_yesterday_surge: GuardFn,
    fast_fetch_html: FastFetchFn,
    mark_recovery: MarkRecoveryFn,
) -> Optional[bool]:
    if not is_perf_report_title(title):
        return None

    log.info("[ANALYSIS] 실적공시 분석: %s (%s)", stock_code, title)
    trade_profile = engine._get_runtime_trade_profile(stock_code)
    budget_krw = int(trade_profile.get("budget_krw", 0) or 0) or None
    cons = engine._get_consensus_snapshot(stock_code)
    t_fast = time.time()
    fast_html = fast_fetch_html(rcp_no)

    if fast_html:
        beat, miss, reason, fast_tp = analyze_fast_performance(fast_html, cons)
        if beat:
            log.info("🚀 [FAST-HIT] %s 실적 서프라이즈! (%s) 소요시간:%.2fs", stock_code, reason, time.time() - t_fast)
            if allow_trade and broker:
                guarded = guard_yesterday_surge("perf_fast_hit", {"path": "fast_track", "tp_hint_pct": fast_tp})
                if guarded is not None:
                    return guarded
                base_px, started = register_recovery(
                    engine=engine,
                    broker=broker,
                    stock_code=stock_code,
                    event_type=event_type,
                    playbook="immediate",
                )
                mark_recovery(started)
                dec = engine.strategy.on_perf_signal(broker, stock_code, beat=True, miss=False, reason=reason, budget_krw=budget_krw)
                executed = engine._execute(broker, stock_code, dec, src="FAST_TRACK", tp_hint=fast_tp)
                return finish(
                    executed,
                    signal_bias="positive",
                    reason=reason,
                    strategy_name="perf_fast_hit",
                    trade_action=dec.action,
                    initial_price=base_px,
                    metrics={"path": "fast_track", "tp_hint_pct": fast_tp},
                )
            return finish(
                False,
                signal_bias="positive",
                reason=reason,
                strategy_name="perf_fast_hit",
                metrics={"path": "fast_track", "tp_hint_pct": fast_tp},
            )
        if miss:
            log.info("📉 [FAST-MISS] %s 어닝 쇼크 (%s) -> 스킵", stock_code, reason)
            return finish(False, signal_bias="negative", reason=reason, strategy_name="perf_fast_miss", metrics={"path": "fast_track"})
        log.info("ℹ️ [FAST-PASS] 특이사항 없음, 정밀 분석으로 전환 (%.2fs)", time.time() - t_fast)

    try:
        report = fetch_perf_report_playwright(
            rcp_no,
            page=page,
            client=pw,
            nav_timeout_ms=nav_timeout_ms,
            wait_until="domcontentloaded",
        )
    except Exception as exc:
        log.error("[PARSE-ERR] 실적공시 파싱 실패: %s | %s", rcp_no, str(exc)[:200])
        return finish(False, signal_bias="unknown", reason=f"parse_error:{str(exc)[:120]}", strategy_name="perf_parse_error")

    res = compute_surprise(report, cons, THRESHOLDS)
    log.info("[PERF-RESULT] %s | signal=%s (%s)", stock_code, res.has_signal, res.reason)
    if not res.has_signal:
        return finish(
            False,
            signal_bias="negative" if res.miss else "neutral",
            reason=res.reason,
            strategy_name="perf_prelim",
            metrics={"ratio_op": res.ratio_op, "ratio_ni": res.ratio_ni},
        )
    if not allow_trade or broker is None:
        log.info("[NOT-TRADING] %s (signal=%s)", stock_code, res.reason)
        return finish(
            False,
            signal_bias="positive" if res.beat else "negative",
            reason=res.reason,
            strategy_name="perf_prelim",
            metrics={"ratio_op": res.ratio_op, "ratio_ni": res.ratio_ni},
        )

    metrics = {"ratio_op": res.ratio_op, "ratio_ni": res.ratio_ni}
    guarded = guard_yesterday_surge("perf_prelim", metrics)
    if guarded is not None:
        return guarded

    custom_tp = 15.0 if (res.ratio_op and res.ratio_op >= 2.0) or (res.ratio_ni and res.ratio_ni >= 2.0) else None
    if custom_tp:
        log.info("🚀 [SUPER SURPRISE] %s 실적 100%% 이상 급등! TP 상향 -> 15%%", stock_code)

    base_px, started = register_recovery(
        engine=engine,
        broker=broker,
        stock_code=stock_code,
        event_type=event_type,
        playbook="immediate",
    )
    mark_recovery(started)
    dec = engine.strategy.on_perf_signal(broker, stock_code, beat=res.beat, miss=res.miss, reason=res.reason, budget_krw=budget_krw)
    executed = engine._execute(broker, stock_code, dec, src=src, tp_hint=custom_tp)
    return finish(
        executed,
        signal_bias="positive" if res.beat else "negative",
        reason=res.reason,
        strategy_name="perf_prelim",
        trade_action=dec.action,
        initial_price=base_px,
        metrics={"ratio_op": res.ratio_op, "ratio_ni": res.ratio_ni, "tp_hint_pct": custom_tp},
    )
