from __future__ import annotations

from datetime import datetime
from typing import Any

from read_api_core import clean_text


def quote_status_priority(status: str) -> int:
    if status == "지연시세":
        return 4
    if status == "업데이트 지연":
        return 3
    if status == "공식종가 fallback":
        return 2
    return 1


def quote_timestamp_value(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0


def latest_quote_capture(rows: list[dict[str, Any]]) -> str:
    latest_text = ""
    latest_value = 0.0
    for row in rows:
        text = clean_text(row.get("price_captured_at") or row.get("captured_at"))
        value = quote_timestamp_value(text)
        if value >= latest_value:
            latest_value = value
            latest_text = text
    return latest_text


def build_quote_health(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    live_count = 0
    fallback_count = 0
    source_counts: dict[str, int] = {}
    for row in rows:
        status = clean_text(row.get("price_status"))
        source = clean_text(row.get("price_source") or row.get("source")) or "unknown"
        source_counts[source] = source_counts.get(source, 0) + 1
        if status == "지연시세":
            live_count += 1
        elif status == "공식종가 fallback":
            fallback_count += 1
    return {
        "quote_delayed_total": total,
        "quote_delayed_live_count": live_count,
        "quote_delayed_fallback_count": fallback_count,
        "quote_delayed_ratio": (live_count / total) if total else 0.0,
        "latest_price_captured_at": latest_quote_capture(rows),
        "source_counts": dict(sorted(source_counts.items(), key=lambda item: item[1], reverse=True)),
    }


def merge_quote_rows(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for rows in groups:
        for row in rows:
            symbol = str(row.get("symbol") or row.get("_id") or "").strip()
            if not symbol:
                continue
            current = merged.get(symbol)
            if current is None:
                merged[symbol] = row
                continue
            next_priority = quote_status_priority(str(row.get("price_status") or ""))
            current_priority = quote_status_priority(str(current.get("price_status") or ""))
            next_captured_at = quote_timestamp_value(row.get("price_captured_at") or row.get("captured_at"))
            current_captured_at = quote_timestamp_value(current.get("price_captured_at") or current.get("captured_at"))
            if next_priority > current_priority or (next_priority == current_priority and next_captured_at >= current_captured_at):
                merged[symbol] = row
    return list(merged.values())
