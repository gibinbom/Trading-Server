from __future__ import annotations


class ColorLog:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    WHITE_ON_MAGENTA = "\033[45m\033[97m"
    RESET = "\033[0m"


def build_signal_events(base_event: dict, threshold_multiplier: float) -> tuple[list[tuple[str, str, dict]], bool]:
    stock_name = base_event["stock_name"]
    symbol = base_event["symbol"]
    market_state = base_event["market_state"]
    buy_broker = base_event["buy_broker"]
    sell_broker = base_event["sell_broker"]
    current_price = int(base_event["current_price"])
    prev_price = int(base_event["prev_price"])
    price_delta = int(base_event["price_delta"])
    tick_size = int(base_event["tick_size"])
    buy_qty = int(base_event["delta_buy_qty"])
    sell_qty = int(base_event["delta_sell_qty"])
    buy_amt_mil = int(base_event["buy_amt_mil"])
    delta_foreign = int(base_event["delta_foreign_qty"])
    foreign_net_buy = int(base_event["foreign_net_buy_qty"])

    target_amt = max(10, int(base_event.get("threshold_amt_mil") or int(300 * threshold_multiplier)))
    if buy_amt_mil < target_amt:
        return [], False

    events: list[tuple[str, str, dict]] = []
    if price_delta >= (tick_size * 2):
        msg = (
            f"🚀 [F:호가폭격|{market_state}] {stock_name}({symbol}) | {buy_broker} 호가창 붕괴! 상방 초입!\n"
            f"   ↳ +{buy_qty:,}주 (약 {buy_amt_mil:,}백만 원) | 현재가: {current_price:,}원 (+{price_delta:,.0f}원 갭업)"
        )
        events.append((ColorLog.WHITE_ON_MAGENTA, msg, {**base_event, "event_type": "F_BOMBARD", "event_label": "F:호가폭격"}))
    elif current_price >= prev_price:
        msg = (
            f"🔥 [D:공격|{market_state}] {stock_name}({symbol}) | {buy_broker} 위로 싹쓸이!\n"
            f"   ↳ +{buy_qty:,}주 (약 {buy_amt_mil:,}백만 원) | 현재가: {current_price:,}원 (직전대비 ↑)"
        )
        events.append((ColorLog.MAGENTA, msg, {**base_event, "event_type": "D_ATTACK", "event_label": "D:공격"}))
    else:
        msg = (
            f"🛡️ [D:방어|{market_state}] {stock_name}({symbol}) | {buy_broker} 밑에서 받아냄!\n"
            f"   ↳ +{buy_qty:,}주 (약 {buy_amt_mil:,}백만 원) | 현재가: {current_price:,}원 (직전대비 ↓)"
        )
        events.append((ColorLog.BLUE, msg, {**base_event, "event_type": "D_DEFENSE", "event_label": "D:방어"}))

    handover_threshold = max(100, int(1000 * threshold_multiplier))
    if base_event["is_retail_selling"] and base_event["is_foreign_buying"] and buy_qty >= handover_threshold:
        msg = (
            f"🎯 [A:손바뀜|{market_state}] {stock_name}({symbol}) | {sell_broker} 투매 ➡ {buy_broker} 흡수중!\n"
            f"   ↳ +{buy_qty:,}주 (약 {buy_amt_mil:,}백만 원) | 현재가: {current_price:,}원"
        )
        payload = {**base_event, "event_type": "A_HANDOVER", "event_label": "A:손바뀜", "threshold_qty": handover_threshold}
        events.append((ColorLog.GREEN, msg, payload))

    twin_threshold = max(200, int(2000 * threshold_multiplier))
    twin_foreign = max(100, int(1000 * threshold_multiplier))
    if base_event["is_inst_buying"] and buy_qty >= twin_threshold and delta_foreign >= twin_foreign:
        msg = (
            f"💛 [C:쌍끌이|{market_state}] {stock_name}({symbol}) | {buy_broker}(기관) + 외인 동시 진입!\n"
            f"   ↳ +{buy_qty:,}주 (약 {buy_amt_mil:,}백만 원) | 현재가: {current_price:,}원"
        )
        payload = {
            **base_event,
            "event_type": "C_TWIN",
            "event_label": "C:쌍끌이",
            "threshold_qty": twin_threshold,
            "threshold_foreign_qty": twin_foreign,
        }
        events.append((ColorLog.YELLOW, msg, payload))

    exhaust_threshold = max(100, int(1000 * threshold_multiplier))
    if base_event["is_retail_selling"] and sell_qty == 0 and buy_qty >= exhaust_threshold:
        msg = (
            f"🛑 [E:투매소화|{market_state}] {stock_name}({symbol}) | {sell_broker} 투매 멈춤 ➡ {buy_broker} 들어올림!\n"
            f"   ↳ +{buy_qty:,}주 (약 {buy_amt_mil:,}백만 원) | 현재가: {current_price:,}원"
        )
        payload = {**base_event, "event_type": "E_EXHAUST", "event_label": "E:투매소화", "threshold_qty": exhaust_threshold}
        events.append((ColorLog.RED, msg, payload))

    whale_threshold = max(300, int(3000 * threshold_multiplier) if threshold_multiplier > 0 else 3000)
    if delta_foreign >= whale_threshold:
        msg = (
            f"🌊 [B:외인유입|{market_state}] {stock_name}({symbol}) | 프로그램/외인 폭풍 유입!\n"
            f"   ↳ +{delta_foreign:,}주 (약 {buy_amt_mil:,}백만 원) | 누적: {foreign_net_buy:,}주"
        )
        payload = {
            **base_event,
            "event_type": "B_FOREIGN_WHALE",
            "event_label": "B:외인유입",
            "threshold_foreign_qty": whale_threshold,
        }
        events.append((ColorLog.CYAN, msg, payload))

    return events, True
