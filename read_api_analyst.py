from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

ANALYST_RAW_DIR = Path(__file__).resolve().parent / "Disclosure" / "analyst_reports" / "raw"
ANALYST_SUMMARY_PATH = Path(__file__).resolve().parent / "Disclosure" / "analyst_reports" / "summaries" / "analyst_report_summary_latest.json"


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def parse_target_price(value: Any) -> int | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return None
    try:
        parsed = int(digits)
        return parsed if parsed > 0 else None
    except Exception:
        return None


def to_timestamp(value: Any) -> float:
    text = clean_text(value)
    if not text:
        return 0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        try:
            return datetime.fromisoformat(text).timestamp()
        except Exception:
            return 0


def normalize_rating(value: Any) -> str:
    raw = clean_text(value).lower()
    if not raw or raw in {"nr", "없음", "none"}:
        return "없음/미제시"
    if any(token in raw for token in ("sell", "underperform", "reduce", "매도")):
        return "매도"
    if any(token in raw for token in ("hold", "neutral", "marketperform", "보유", "중립", "보류")):
        return "보류"
    if any(token in raw for token in ("buy", "outperform", "overweight", "trading buy", "매수")):
        return "매수"
    return "없음/미제시"


def median(values: list[int]) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) / 2
    return float(sorted_values[mid])


def derive_revision_direction(current_target_price: int | None, previous_target_price: int | None, has_previous_report: bool) -> str:
    if not has_previous_report:
        return "신규"
    if not current_target_price or not previous_target_price:
        return "비교불가"
    pct_diff = abs(current_target_price - previous_target_price) / previous_target_price if previous_target_price > 0 else 0
    if pct_diff < 0.005:
        return "유지"
    return "상향" if current_target_price > previous_target_price else "하향"


def get_stock_analyst_board(symbol: str, lookback_days: int = 90) -> dict[str, Any]:
    normalized = clean_text(symbol).zfill(6)
    cutoff = datetime.now().timestamp() - lookback_days * 24 * 60 * 60
    fallback_summary = {"report_count": 0, "broker_count": 0}
    try:
        summary_payload = json.loads(ANALYST_SUMMARY_PATH.read_text(encoding="utf-8"))
        for row in summary_payload.get("top_stocks", []):
            if clean_text(row.get("symbol")).zfill(6) == normalized:
                fallback_summary = {
                    "report_count": int(row.get("report_count") or 0),
                    "broker_count": int(row.get("broker_diversity") or 0),
                }
                break
    except Exception:
        pass

    reports: list[dict[str, Any]] = []
    previous_targets: dict[str, int | None] = {}
    previous_seen: set[str] = set()
    dedupe: set[str] = set()
    for file in sorted(ANALYST_RAW_DIR.glob("analyst_reports_*.jsonl")):
        try:
            content = file.read_text(encoding="utf-8")
        except Exception:
            continue
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            row_symbol = clean_text(row.get("symbol")).zfill(6)
            if row_symbol != normalized:
                continue
            broker = clean_text(row.get("broker"))
            title = clean_text(row.get("title"))
            published_at = clean_text(row.get("published_at"))
            target_price = parse_target_price(row.get("target_price"))
            dedupe_key = "|".join(
                [
                    row_symbol,
                    broker,
                    title,
                    published_at[:10],
                    str(target_price or ""),
                    clean_text(row.get("detail_url") or row.get("pdf_url") or row.get("report_idx") or row.get("nid")),
                ]
            )
            if dedupe_key in dedupe:
                continue
            dedupe.add(dedupe_key)
            published_ts = to_timestamp(published_at)
            broker_key = broker or clean_text(row.get("writer")) or clean_text(row.get("source")) or "unknown"
            revision_direction = derive_revision_direction(target_price, previous_targets.get(broker_key), broker_key in previous_seen)
            if target_price:
                previous_targets[broker_key] = target_price
            previous_seen.add(broker_key)
            if published_ts <= 0 or published_ts < cutoff:
                continue
            reports.append(
                {
                    "symbol": row_symbol,
                    "broker": broker,
                    "title": title,
                    "published_at": published_at,
                    "target_price": target_price,
                    "rating": clean_text(row.get("rating")),
                    "rating_label": normalize_rating(row.get("rating")),
                    "detail_url": clean_text(row.get("detail_url") or row.get("pdf_url")),
                    "source": clean_text(row.get("source")),
                    "revision_direction": revision_direction,
                }
            )

    reports.sort(key=lambda row: to_timestamp(row.get("published_at")), reverse=True)
    priced_targets = [int(row["target_price"]) for row in reports if isinstance(row.get("target_price"), int)]
    return {
        "reports": reports,
        "summary": {
            "average_tp": (sum(priced_targets) / len(priced_targets)) if priced_targets else None,
            "median_tp": median(priced_targets),
            "high_tp": max(priced_targets) if priced_targets else None,
            "low_tp": min(priced_targets) if priced_targets else None,
            "report_count": len(reports) or fallback_summary["report_count"],
            "broker_count": len({clean_text(row.get("broker")) for row in reports if clean_text(row.get("broker"))}) or fallback_summary["broker_count"],
            "priced_report_count": len(priced_targets),
        },
    }
