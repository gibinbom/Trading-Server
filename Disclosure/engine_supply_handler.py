from __future__ import annotations

import logging
import time
from typing import Optional

try:
    from config import ORDER_HIT
    from dart_common import is_order_received_report, is_perf_report_title
    from dart_order import compute_order_hit_v2, fetch_order_fields_playwright
    from engine_fast_analyzers import analyze_fast_supply_contract
    from engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery
except Exception:
    from Disclosure.config import ORDER_HIT
    from Disclosure.dart_common import is_order_received_report, is_perf_report_title
    from Disclosure.dart_order import compute_order_hit_v2, fetch_order_fields_playwright
    from Disclosure.engine_fast_analyzers import analyze_fast_supply_contract
    from Disclosure.engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery


log = logging.getLogger("disclosure.engine.handlers")


def handle_supply_contract(
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
    if not (is_order_received_report(title) and not is_perf_report_title(title)):
        return None

    log.info("[ANALYSIS] 수주공시 분석: %s (%s)", stock_code, title)
    trade_profile = engine._get_runtime_trade_profile(stock_code)
    budget_krw = int(trade_profile.get("budget_krw", 0) or 0) or None
    t_fast = time.time()
    fast_html = fast_fetch_html(rcp_no)

    if fast_html:
        is_hit, reason, ratio = analyze_fast_supply_contract(fast_html)
        if is_hit:
            log.info("🚀 [FAST-HIT] %s 대규모 수주 포착! (%s) 소요시간:%.2fs", stock_code, reason, time.time() - t_fast)
            if allow_trade and broker:
                guarded = guard_yesterday_surge(
                    "supply_fast_hit",
                    {"fast_ratio_pct": ratio, "path": "fast_track"},
                )
                if guarded is not None:
                    return guarded
                base_px, started = register_recovery(
                    engine=engine,
                    broker=broker,
                    stock_code=stock_code,
                    event_type=event_type,
                    playbook="drop_rebound_half",
                    start_method="start_tracking",
                    required_drop_pct=7.0,
                    required_bounce_pct=3.0,
                    required_recovery_ratio=0.5,
                )
                mark_recovery(started)
                dec = engine.strategy.on_perf_signal(broker, stock_code, beat=True, miss=False, reason=reason, budget_krw=budget_krw)
                executed = engine._execute(broker, stock_code, dec, src=src, tp_hint=None)
                return finish(
                    executed,
                    signal_bias="positive",
                    reason=reason,
                    strategy_name="supply_fast_hit",
                    trade_action=dec.action,
                    initial_price=base_px,
                    metrics={"fast_ratio_pct": ratio, "path": "fast_track"},
                    custom_tags=["supply_contract", "rebound_watch"],
                )
            return finish(
                False,
                signal_bias="positive",
                reason=reason,
                strategy_name="supply_fast_hit",
                metrics={"fast_ratio_pct": ratio, "path": "fast_track"},
            )

        if ratio > 0:
            log.info("📉 [FAST-MISS] %s 수주 규모 작음 (%.2f%%) -> 스킵", stock_code, ratio)
            return finish(
                False,
                signal_bias="neutral",
                reason=reason,
                strategy_name="supply_fast_miss",
                metrics={"fast_ratio_pct": ratio, "path": "fast_track"},
            )

        log.info("ℹ️ [FAST-PASS] 비율 식별 불가, 정밀 분석으로 전환 (%.2fs)", time.time() - t_fast)
    else:
        log.warning("⚠️ [FAST-FAIL] 고속 파싱 실패 -> Playwright 전환")

    try:
        parsed = fetch_order_fields_playwright(
            rcp_no,
            page=page,
            client=pw,
            nav_timeout_ms=nav_timeout_ms,
            wait_after_main_ms=0,
            view_ms=0,
        )
    except Exception as exc:
        log.error("[PARSE-ERR] 수주공시 파싱 실패: %s | %s", rcp_no, str(exc)[:200])
        return finish(False, signal_bias="unknown", reason=f"parse_error:{str(exc)[:120]}", strategy_name="supply_parse_error")

    cons = engine._get_consensus_snapshot(stock_code)
    res = compute_order_hit_v2(
        order_amount=parsed.contract_amount_won,
        cons_q_rev=cons.get("revenue"),
        sales_ratio_pct=parsed.sales_ratio_pct,
        hit_sales_ratio_threshold_pct=15.0,
        fallback_hit_ratio=ORDER_HIT.hit_ratio,
    )
    log.info("[ORDER-RESULT] %s | hit=%s (%s)", stock_code, res.hit, res.reason)

    metrics = {
        "sales_ratio_pct": res.sales_ratio_pct,
        "order_to_consensus_ratio": res.ratio,
        "order_amount_won": res.order_amount,
        "consensus_quarter_revenue_won": res.consensus_quarter_revenue,
    }
    if not res.hit:
        return finish(False, signal_bias="neutral", reason=res.reason, strategy_name="supply_contract", metrics=metrics)

    if not allow_trade:
        log.info("[NOT-TRADING] %s (signal=%s)", stock_code, res.reason)
        return finish(
            False,
            signal_bias="positive",
            reason=res.reason,
            strategy_name="supply_contract",
            metrics=metrics,
            custom_tags=["paper_signal", "rebound_watch"],
        )
    if broker is None:
        log.warning("[TRADE-ABORT] No Broker")
        return finish(False, signal_bias="positive", reason="no_broker", strategy_name="supply_contract")

    guarded = guard_yesterday_surge("supply_contract", metrics)
    if guarded is not None:
        return guarded

    base_px, started = register_recovery(
        engine=engine,
        broker=broker,
        stock_code=stock_code,
        event_type=event_type,
        playbook="drop_rebound_half",
        required_drop_pct=7.0,
        required_bounce_pct=3.0,
        required_recovery_ratio=0.5,
    )
    mark_recovery(started)
    dec = engine.strategy.on_order_hit(broker, stock_code, hit=True, reason=res.reason, budget_krw=budget_krw)
    executed = engine._execute(broker, stock_code, dec, src=src)
    return finish(
        executed,
        signal_bias="positive",
        reason=res.reason,
        strategy_name="supply_contract",
        trade_action=dec.action,
        initial_price=base_px,
        metrics=metrics,
        custom_tags=["rebound_watch"],
    )
