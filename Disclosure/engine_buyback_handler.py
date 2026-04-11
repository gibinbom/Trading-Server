from __future__ import annotations

import logging
import time
from typing import Optional

try:
    from engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery
    from signals.dart_buyback import analyze_fast_buyback, is_buyback_report_title
except Exception:
    from Disclosure.engine_handler_common import FastFetchFn, FinishFn, GuardFn, MarkRecoveryFn, register_recovery
    from Disclosure.signals.dart_buyback import analyze_fast_buyback, is_buyback_report_title


log = logging.getLogger("disclosure.engine.handlers")


def handle_buyback(
    *,
    engine,
    broker,
    stock_code: str,
    rcp_no: str,
    title: str,
    src: str,
    allow_trade: bool,
    page=None,
    pw=None,
    nav_timeout_ms: int = 0,
    event_type: str,
    finish: FinishFn,
    guard_yesterday_surge: GuardFn,
    fast_fetch_html: FastFetchFn,
    mark_recovery: MarkRecoveryFn,
) -> Optional[bool]:
    if not is_buyback_report_title(title):
        return None

    log.info("[ANALYSIS] 자사주/소각 공시 분석: %s (%s)", stock_code, title)
    trade_profile = engine._get_runtime_trade_profile(stock_code)
    budget_krw = int(trade_profile.get("budget_krw", 0) or 0) or None
    mcap = engine.marcap_cache.get(stock_code, 0)
    if mcap == 0:
        log.warning("⚠️ [SKIP] %s 시가총액 정보 없음 -> 비율 계산 불가", stock_code)
        return finish(False, signal_bias="unknown", reason="missing_market_cap", strategy_name="buyback")

    t_fast = time.time()
    fast_html = fast_fetch_html(rcp_no)
    if not fast_html:
        log.warning("⚠️ [FAST-FAIL] 자사주 파싱 실패 (Fast-Track)")
        return finish(False, signal_bias="unknown", reason="fast_parse_fail", strategy_name="buyback")

    is_hit, reason, ratio = analyze_fast_buyback(fast_html, title, mcap)
    if not is_hit:
        log.info("📉 [BUYBACK-MISS] %s 규모 미달 (%s) -> 스킵", stock_code, reason)
        return finish(False, signal_bias="neutral", reason=reason, strategy_name="buyback", metrics={"buyback_ratio_pct": ratio})

    if broker:
        try:
            change_rate = broker.get_price_change_rate(stock_code)
            if change_rate is not None and change_rate >= 15.0:
                log.warning("🚫 [PRE-REFLECTED] %s 이미 급등 중(%s%%) -> 선반영으로 간주하고 스킵", stock_code, change_rate)
                return finish(
                    False,
                    signal_bias="neutral",
                    reason=f"pre_reflected_{change_rate:.2f}",
                    strategy_name="buyback",
                    metrics={"buyback_ratio_pct": ratio, "change_rate_pct": change_rate},
                )
        except Exception as exc:
            log.warning("⚠️ [CHECK-FAIL] 등락률 확인 실패: %s -> 일단 진입 시도", exc)

    log.info("🚀 [BUYBACK-HIT] %s 주주환원 정책 포착! (%s) 소요시간:%.2fs", stock_code, reason, time.time() - t_fast)
    if allow_trade and broker:
        guarded = guard_yesterday_surge("buyback", {"buyback_ratio_pct": ratio})
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
        custom_tp = 15.0 if "소각" in title else 7.0
        dec = engine.strategy.on_order_hit(broker, stock_code, hit=True, reason=reason, budget_krw=budget_krw)
        executed = engine._execute(broker, stock_code, dec, src="FAST_TRACK", tp_hint=custom_tp)
        return finish(
            executed,
            signal_bias="positive",
            reason=reason,
            strategy_name="buyback",
            trade_action=dec.action,
            initial_price=base_px,
            metrics={"buyback_ratio_pct": ratio, "tp_hint_pct": custom_tp},
        )
    return finish(
        False,
        signal_bias="positive",
        reason=reason,
        strategy_name="buyback",
        metrics={"buyback_ratio_pct": ratio},
    )
