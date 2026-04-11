from __future__ import annotations

import logging
import time
from typing import Optional

try:
    from dart_common import is_order_received_report, is_perf_report_title
    from engine_fast_analyzers import analyze_fast_performance
    from engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery
    from signals.dart_sales_variation import analyze_sales_variation_with_page, is_sales_variation_report_title
except Exception:
    from Disclosure.dart_common import is_order_received_report, is_perf_report_title
    from Disclosure.engine_fast_analyzers import analyze_fast_performance
    from Disclosure.engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery
    from Disclosure.signals.dart_sales_variation import analyze_sales_variation_with_page, is_sales_variation_report_title


log = logging.getLogger("disclosure.engine.handlers")


def handle_sales_variation(
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
    if not (
        is_sales_variation_report_title(title)
        and not is_perf_report_title(title)
        and not is_order_received_report(title)
    ):
        return None

    log.info("[ANALYSIS] 매출/손익구조 변동 분석: %s (%s)", stock_code, title)
    trade_profile = engine._get_runtime_trade_profile(stock_code)
    budget_krw = int(trade_profile.get("budget_krw", 0) or 0) or None
    cons = engine._get_consensus_snapshot(stock_code)
    t_fast = time.time()
    fast_html = fast_fetch_html(rcp_no)

    if fast_html:
        beat, miss, reason, fast_tp = analyze_fast_performance(fast_html, cons)
        if beat:
            log.info("🚀 [FAST-TRACK-HIT] %s 매출 변동 서프라이즈! (%s) 소요시간:%.2fs", stock_code, reason, time.time() - t_fast)
            if allow_trade and broker:
                guarded = guard_yesterday_surge(
                    "sales_variation_fast_hit",
                    {"path": "fast_track", "tp_hint_pct": fast_tp},
                )
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
                    strategy_name="sales_variation_fast_hit",
                    trade_action=dec.action,
                    initial_price=base_px,
                    metrics={"path": "fast_track", "tp_hint_pct": fast_tp},
                )
            return finish(
                False,
                signal_bias="positive",
                reason=reason,
                strategy_name="sales_variation_fast_hit",
                metrics={"path": "fast_track", "tp_hint_pct": fast_tp},
            )
        if miss:
            log.info("📉 [FAST-TRACK-MISS] %s 실적 악화 포착 -> 스킵", stock_code)
            return finish(False, signal_bias="negative", reason=reason, strategy_name="sales_variation_fast_miss", metrics={"path": "fast_track"})
        log.info("ℹ️ [FAST-TRACK-PASS] 고속 분석 결과가 모호하여 정밀 분석으로 전환합니다.")

    try:
        sig = analyze_sales_variation_with_page(
            rcp_no,
            page=page,
            threshold_pct=10.0,
            nav_timeout_ms=nav_timeout_ms,
            timeout_ms=20000,
        )
    except Exception as exc:
        log.error("[PARSE-ERR] 매출/손익구조 변동 파싱 실패: %s | %s", rcp_no, str(exc)[:200])
        return finish(False, signal_bias="unknown", reason=f"parse_error:{str(exc)[:120]}", strategy_name="sales_variation_parse_error")

    for detail in sig.details:
        if "Sales" in detail.item and detail.ratio_effective is not None and detail.ratio_effective < 0:
            log.info("📉 [FILTER] %s 매출 감소(%.1f%%) -> 매수 안함", stock_code, detail.ratio_effective)
            return finish(False, signal_bias="negative", reason=f"sales_negative:{detail.ratio_effective:.1f}", strategy_name="sales_variation")
        if "OperatingProfit" in detail.item and detail.ratio_effective is not None and detail.ratio_effective < 0:
            log.info("📉 [FILTER] %s 영업이익 감소/적자(%.1f%%) -> 매수 안함", stock_code, detail.ratio_effective)
            return finish(False, signal_bias="negative", reason=f"op_negative:{detail.ratio_effective:.1f}", strategy_name="sales_variation")

    summary_parts = []
    for detail in sig.details:
        val_str = f"{detail.ratio_effective:.1f}%" if detail.ratio_effective is not None else "N/A"
        item_name = detail.item.replace("OperatingProfit", "OP").replace("NetIncome", "NI").replace("Sales", "Sales")
        summary_parts.append(f"{item_name}:{val_str}")
    all_values_str = ", ".join(summary_parts)
    if not sig.buy_signal:
        log.info("[SALES-VAR] %s | buy_signal=False (%s)", stock_code, all_values_str)
        return finish(False, signal_bias="neutral", reason=all_values_str, strategy_name="sales_variation")

    reason = f"SalesVar(HIT): {all_values_str}"
    log.info("[SALES-VAR] %s | buy_signal=True (%s)", stock_code, reason)
    if not allow_trade or broker is None:
        log.info("[NOT-TRADING] %s (signal=%s)", stock_code, reason)
        return finish(False, signal_bias="positive", reason=reason, strategy_name="sales_variation")

    guarded = guard_yesterday_surge("sales_variation")
    if guarded is not None:
        return guarded

    big_jump_items = [
        detail
        for detail in sig.details
        if detail.item in ("OperatingProfit", "NetIncome") and detail.ratio_effective and detail.ratio_effective >= 100.0
    ]
    custom_tp = 15.0 if big_jump_items else None
    if custom_tp:
        jump_names = [
            detail.item.replace("OperatingProfit", "OP").replace("NetIncome", "NI")
            for detail in big_jump_items
        ]
        log.info("🚀 [BIG VARIATION] %s %s 100%% 이상 폭증! TP 상향 -> %.1f%%", stock_code, jump_names, custom_tp)

    base_px, started = register_recovery(
        engine=engine,
        broker=broker,
        stock_code=stock_code,
        event_type=event_type,
        playbook="immediate",
    )
    mark_recovery(started)
    dec = engine.strategy.on_perf_signal(broker, stock_code, beat=True, miss=False, reason=reason, budget_krw=budget_krw)
    executed = engine._execute(broker, stock_code, dec, src=src, tp_hint=custom_tp)
    return finish(
        executed,
        signal_bias="positive",
        reason=reason,
        strategy_name="sales_variation",
        trade_action=dec.action,
        initial_price=base_px,
        metrics={"tp_hint_pct": custom_tp, "details": [detail.to_dict() for detail in sig.details]},
    )
