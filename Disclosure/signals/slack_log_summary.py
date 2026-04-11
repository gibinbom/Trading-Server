from __future__ import annotations

from collections import Counter


EVENT_SCORE_WEIGHTS = {
    "D_DEFENSE": 5,
    "A_HANDOVER": 7,
    "C_TWIN": 7,
    "E_EXHAUST": 6,
    "B_FOREIGN_WHALE": 4,
    "D_ATTACK": 1,
    "F_BOMBARD": -4,
}

EVENT_DISPLAY_LABELS = {
    "D_DEFENSE": "방어",
    "A_HANDOVER": "손바뀜",
    "C_TWIN": "쌍끌이",
    "E_EXHAUST": "투매소화",
    "B_FOREIGN_WHALE": "외인유입",
    "D_ATTACK": "공격",
    "F_BOMBARD": "호가폭격",
}


def _get_tick_size(price: int) -> int:
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


def _price_band(price: int) -> int:
    if not price:
        return 0
    tick = _get_tick_size(price)
    band = tick * 5
    if band <= 0:
        return int(price)
    return int(round(price / band) * band)


def summarize_structured_events(events: list[dict], top_n: int = 10) -> str:
    if not events:
        return ""

    symbol_stats: dict[str, dict] = {}
    event_totals = Counter()
    daily_totals: dict[str, Counter] = {}
    for event in events:
        symbol = event.get("symbol")
        if not symbol:
            continue
        event_type = event.get("event_type", "UNKNOWN")
        captured_at = event.get("captured_at", "")
        date_key = captured_at[:10] if captured_at else "unknown"
        event_totals[event_type] += 1
        daily_totals.setdefault(date_key, Counter())[event_type] += 1
        stat = symbol_stats.setdefault(
            symbol,
            {
                "symbol": symbol,
                "stock_name": event.get("stock_name", symbol),
                "event_counts": Counter(),
                "broker_amounts": Counter(),
                "price_bands": Counter(),
                "total_amt_mil": 0,
                "positive_event_count": 0,
                "breakout_event_count": 0,
                "accumulation_score": 0,
                "last_price": 0,
            },
        )
        stat["event_counts"][event_type] += 1
        amt_mil = int(event.get("amt_mil", 0) or 0)
        stat["total_amt_mil"] += amt_mil
        stat["last_price"] = int(event.get("current_price", stat["last_price"]) or 0)
        if event.get("buy_broker"):
            stat["broker_amounts"][event["buy_broker"]] += amt_mil
        band = _price_band(int(event.get("current_price", 0) or 0))
        if band:
            stat["price_bands"][band] += amt_mil
        weight = EVENT_SCORE_WEIGHTS.get(event_type, 0)
        stat["accumulation_score"] += weight
        if weight > 0:
            stat["accumulation_score"] += min(8, amt_mil // 300)
            stat["positive_event_count"] += 1
        elif event_type == "F_BOMBARD":
            stat["accumulation_score"] -= min(4, amt_mil // 500)
            stat["breakout_event_count"] += 1

    ranked = sorted(
        symbol_stats.values(),
        key=lambda item: (item["accumulation_score"], item["positive_event_count"], item["total_amt_mil"], -item["breakout_event_count"]),
        reverse=True,
    )
    lines = ["[구조화 이벤트 요약]", f"- 총 이벤트: {len(events)}건"]
    lines.append("- 이벤트 분포: " + ", ".join(f"{EVENT_DISPLAY_LABELS.get(key, key)} {value}" for key, value in event_totals.most_common()))
    if daily_totals:
        lines.append("- 일자별 요약:")
        for date_key in sorted(daily_totals):
            counter = daily_totals[date_key]
            lines.append(f"  {date_key}: " + ", ".join(f"{EVENT_DISPLAY_LABELS.get(key, key)} {value}" for key, value in counter.most_common()))
    lines.append("- 종목별 상위 후보:")
    for item in ranked[:top_n]:
        event_bits = ", ".join(f"{EVENT_DISPLAY_LABELS.get(key, key)} {value}" for key, value in item["event_counts"].most_common())
        broker_bits = ", ".join(f"{broker} {amt:,}백만" for broker, amt in item["broker_amounts"].most_common(2)) or "창구 데이터 부족"
        price_bits = ", ".join(f"{band:,}원대 {amt:,}백만" for band, amt in item["price_bands"].most_common(3)) or "가격대 데이터 부족"
        lines.append(f"* {item['stock_name']}({item['symbol']}) | score {item['accumulation_score']} | 누적 {item['total_amt_mil']:,}백만 | {event_bits}")
        lines.append(f"  창구집중: {broker_bits}")
        lines.append(f"  주요가격대: {price_bits}")
    return "\n".join(lines)


def summarize_flow_snapshots(snapshots: list[dict], top_n: int = 10) -> str:
    if not snapshots:
        return ""

    latest_by_symbol: dict[str, dict] = {}
    for snapshot in snapshots:
        symbol = snapshot.get("symbol")
        captured_at = snapshot.get("captured_at", "")
        if symbol and (symbol not in latest_by_symbol or captured_at > latest_by_symbol[symbol].get("captured_at", "")):
            latest_by_symbol[symbol] = snapshot

    ranked = sorted(
        latest_by_symbol.values(),
        key=lambda item: (int(item.get("flow_state_score", 0) or 0), int(item.get("cum_net_amt_mil", 0) or 0), int(item.get("gross_amt_since_snapshot", 0) or 0)),
        reverse=True,
    )
    lines = ["[누적 흐름 스냅샷 요약]", f"- 스냅샷 종목 수: {len(latest_by_symbol)}개", "- 종목별 상위 후보:"]
    for item in ranked[:top_n]:
        top_buys = ", ".join(f"{row['name']} {row['value']:,}백만" for row in (item.get("top_buy_brokers") or [])[:2]) or "창구 데이터 부족"
        bands = ", ".join(f"{row['name']}원대 {row['value']:,}백만" for row in (item.get("key_price_bands") or [])[:2]) or "가격대 데이터 부족"
        event_counts = item.get("event_counts") or {}
        event_bits = ", ".join(f"{EVENT_DISPLAY_LABELS.get(key, key)} {value}" for key, value in sorted(event_counts.items(), key=lambda kv: kv[1], reverse=True)[:3]) or "이벤트 누적 없음"
        lines.append(
            f"* {item.get('stock_name')}({item.get('symbol')}) | flow_score {item.get('flow_state_score', 0)} | "
            f"순유입 {int(item.get('cum_net_amt_mil', 0) or 0):,}백만 | 외인Δ {int(item.get('cum_foreign_delta_qty', 0) or 0):,}주"
        )
        lines.append(f"  집중창구: {top_buys}")
        lines.append(f"  주요가격대: {bands}")
        lines.append(f"  이벤트믹스: {event_bits}")
    return "\n".join(lines)
