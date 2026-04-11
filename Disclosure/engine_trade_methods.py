from __future__ import annotations

import logging
import time
from typing import Any, Optional

from utils.slack import notify_trade, notify_trade_skip


log = logging.getLogger("disclosure.engine.trade")


def get_tick_size(price: float) -> int:
    if price < 2000:
        return 1
    if price < 5000:
        return 5
    if price < 20000:
        return 10
    if price < 50000:
        return 50
    if price < 200000:
        return 100
    if price < 500000:
        return 500
    return 1000


def execute_trade(
    engine,
    broker,
    symbol: str,
    decision,
    src: str,
    tp_hint: Optional[float] = None,
    context: Optional[dict[str, Any]] = None,
    monitor_policy: Optional[dict[str, Any]] = None,
) -> bool:
    if decision.action == "SKIP":
        log.info("[TRADE] SKIP %s (%s)", symbol, decision.reason)
        notify_trade_skip(symbol, decision.reason, decision.qty, context=context)
        return False

    if decision.action == "BUY":
        if engine._is_nxt_session():
            curr_price = engine._safe_last_price(broker, symbol)
            if not curr_price:
                log.error("[NXT-BUY] 현재가 조회 실패로 주문 불가")
                return False
            tick = get_tick_size(curr_price)
            limit_price = curr_price + (tick * 5)
            log.info("[NXT-BUY] 5호가 위 매수 시도: 현재가 %.0f -> 주문가 %.0f (+%d)", curr_price, limit_price, tick * 5)
            result = broker.buy_limit(symbol, decision.qty, limit_price)
            msg_tag = "BUY(NXT-LIMIT+5)"
        else:
            result = broker.buy_market(symbol, decision.qty)
            msg_tag = "BUY"

        log.info("[TRADE] %s %s x%s -> %s (%s)", msg_tag, symbol, decision.qty, result.ok, result.msg)
        notify_trade(msg_tag, symbol, decision.qty, result.ok, result.msg, context=context)
        engine.buy_cooldown_map[symbol] = time.time()

        if result.ok:
            real_avg_price = None
            log.info("🔍 [%s] 실제 체결 단가 확인 중...", symbol)
            for _ in range(5):
                time.sleep(1.0)
                try:
                    balance_data = broker.get_balance()
                    if balance_data and "stocks" in balance_data:
                        for stock in balance_data["stocks"]:
                            if stock["symbol"] == symbol and stock["qty"] > 0:
                                real_avg_price = stock["avg_price"]
                                break
                except Exception:
                    pass
                if real_avg_price and real_avg_price > 0:
                    break

            if real_avg_price:
                log.info("✅ [%s] 실제 진입가 확보 완료: %s원", symbol, f"{real_avg_price:,.0f}")
            else:
                log.warning("⚠️ [%s] 체결가 확인 지연! (추후 감시 모듈에서 재확인)", symbol)

            engine._ensure_monitor(
                broker,
                symbol,
                qty_hint=decision.qty,
                avg_price_hint=real_avg_price,
                tp_override=tp_hint,
                monitor_policy=monitor_policy,
            )
        return result.ok

    if decision.action == "SELL":
        if engine._is_nxt_session():
            price = engine._safe_last_price(broker, symbol)
            if not price:
                return False
            result = broker.sell_limit(symbol, decision.qty, price)
            msg_tag = "SELL(NXT-LIMIT)"
        else:
            result = broker.sell_market(symbol, decision.qty)
            msg_tag = "SELL"
        log.info("[TRADE] %s %s x%s -> %s (%s)", msg_tag, symbol, decision.qty, result.ok, result.msg)
        notify_trade(msg_tag, symbol, decision.qty, result.ok, result.msg, context=context)
        return bool(result.ok)

    return False
