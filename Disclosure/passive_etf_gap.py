from __future__ import annotations

from typing import Any, Callable

import pandas as pd


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def extract_provider(name: str) -> str:
    text = _clean_text(name)
    if not text:
        return ""
    return text.split(" ", 1)[0]


def extract_tracking_index_name(name: str) -> str:
    text = _clean_text(name)
    if not text:
        return ""
    provider = extract_provider(text)
    if provider and text.startswith(provider):
        text = text[len(provider) :].strip()
    return text or _clean_text(name)


def classify_underlying_region(text: str, *, domestic_keywords: tuple[str, ...], foreign_keywords: tuple[str, ...]) -> str:
    value = _clean_text(text).upper()
    if any(token in value for token in domestic_keywords):
        return "domestic"
    if any(token in value for token in foreign_keywords):
        return "foreign"
    return "domestic"


def is_passive_etf(name: str, *, excluded_keywords: tuple[str, ...]) -> bool:
    text = _clean_text(name)
    if not text:
        return False
    upper = text.upper()
    if "ETN" in upper:
        return False
    return not any(keyword.upper() in upper for keyword in excluded_keywords)


def build_etf_gap_snapshot_from_listing(
    listing_df: pd.DataFrame,
    *,
    captured_at: str,
    normalize_symbol: Callable[[Any], str],
    safe_float: Callable[[Any], float | None],
    gap_threshold_pct: float,
    top_count: int,
    excluded_keywords: tuple[str, ...],
    domestic_region_keywords: tuple[str, ...],
    foreign_region_keywords: tuple[str, ...],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for raw in listing_df.to_dict(orient="records"):
        symbol = normalize_symbol(raw.get("Symbol"))
        name = _clean_text(raw.get("Name"))
        if not symbol or not name or not is_passive_etf(name, excluded_keywords=excluded_keywords):
            continue
        price = safe_float(raw.get("Price"))
        nav = safe_float(raw.get("NAV"))
        aum_million = safe_float(raw.get("MarCap"))
        aum_krw = int(round((aum_million or 0.0) * 1_000_000)) if aum_million and aum_million > 0 else 0
        tracking_index_name = extract_tracking_index_name(name)
        gap_pct = None
        if price and nav and nav > 0:
            gap_pct = round((price / nav - 1.0) * 100.0, 4)
        if gap_pct is None:
            gap_direction = "unknown"
            status = "nav_missing"
        elif gap_pct > 0.01:
            gap_direction = "premium"
            status = "ok"
        elif gap_pct < -0.01:
            gap_direction = "discount"
            status = "ok"
        else:
            gap_direction = "flat"
            status = "ok"
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "provider": extract_provider(name),
                "tracking_index_name": tracking_index_name,
                "underlying_region": classify_underlying_region(
                    tracking_index_name,
                    domestic_keywords=domestic_region_keywords,
                    foreign_keywords=foreign_region_keywords,
                ),
                "aum_krw": aum_krw,
                "aum_rank": 0,
                "price": round(price, 4) if price is not None else None,
                "nav": round(nav, 4) if nav is not None else None,
                "nav_gap_pct": gap_pct,
                "gap_direction": gap_direction,
                "captured_at": captured_at,
                "status": status,
            }
        )

    rows.sort(key=lambda item: int(item.get("aum_krw") or 0), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row["aum_rank"] = idx
    provider_counts: dict[str, int] = {}
    for row in rows:
        provider = _clean_text(row.get("provider")) or "unknown"
        provider_counts[provider] = provider_counts.get(provider, 0) + 1
    anomaly_count = sum(
        1
        for row in rows
        if row.get("nav_gap_pct") is not None and abs(float(row.get("nav_gap_pct") or 0.0)) >= gap_threshold_pct
    )
    return {
        "generated_at": captured_at,
        "as_of": captured_at[:10],
        "status": "live",
        "stale_since": None,
        "source_error": "",
        "default_aum_top_n": top_count,
        "default_gap_threshold_pct": gap_threshold_pct,
        "rows": rows,
        "summary": {
            "eligible_count": len(rows),
            "top30_count": min(top_count, len(rows)),
            "anomaly_count": anomaly_count,
            "provider_counts": dict(sorted(provider_counts.items(), key=lambda item: item[1], reverse=True)),
        },
    }
