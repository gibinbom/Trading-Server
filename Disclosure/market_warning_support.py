from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        text = str(value or "").replace(",", "").strip()
        if not text or text in {"-", "None", "nan"}:
            return default
        return float(text)
    except Exception:
        return default


def norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def slug_text(value: Any) -> str:
    return re.sub(r"[^0-9a-zA-Z가-힣]+", "_", clean_text(value)).strip("_")


def normalize_market(value: Any, *, market_alt_map: dict[str, str]) -> str:
    text = clean_text(value).upper()
    if text.startswith("KOSPI"):
        return "KOSPI"
    if text.startswith("KOSDAQ GLOBAL"):
        return "KOSDAQ GLOBAL"
    if text.startswith("KOSDAQ"):
        return "KOSDAQ"
    if text.startswith("KONEX"):
        return "KONEX"
    return market_alt_map.get(clean_text(value), text)


def market_group(market: str, *, market_alt_map: dict[str, str]) -> str:
    normalized = normalize_market(market, market_alt_map=market_alt_map)
    return "KOSDAQ" if normalized == "KOSDAQ GLOBAL" else normalized


def parse_iso_date(value: Any) -> date | None:
    text = clean_text(value)
    if not text or text in {"-"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def date_text(value: Any) -> str:
    parsed = parse_iso_date(value)
    return parsed.isoformat() if parsed else clean_text(value)


def next_business_day(value: str) -> str:
    parsed = parse_iso_date(value)
    if parsed is None:
        return ""
    current = parsed + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current.isoformat()


def is_warning_eligible_name(name: str, *, excluded_keywords: tuple[str, ...]) -> bool:
    text = clean_text(name)
    if not text:
        return False
    upper = text.upper()
    return not any(keyword.upper() in upper for keyword in excluded_keywords)


def load_projection_file(name: str, *, directories: tuple[Path, ...]) -> Any:
    for directory in directories:
        target = directory / f"{name}.json"
        try:
            return json.loads(target.read_text(encoding="utf-8"))
        except Exception:
            continue
    return None


def load_stock_master_map(
    *,
    payload: Any,
    listing_path: Path,
    excluded_keywords: tuple[str, ...],
    market_alt_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    rows = payload if isinstance(payload, list) else []
    mapping: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = norm_symbol(row.get("symbol") or row.get("code"))
        market = normalize_market(row.get("market"), market_alt_map=market_alt_map)
        name = clean_text(row.get("name"))
        if not symbol or not name or market not in {"KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"}:
            continue
        if not is_warning_eligible_name(name, excluded_keywords=excluded_keywords):
            continue
        mapping[symbol] = {"symbol": symbol, "name": name, "market": market}
    if mapping:
        return mapping

    df = pd.read_csv(listing_path, dtype={"Code": str}, encoding="utf-8-sig")
    for raw in df.to_dict(orient="records"):
        symbol = norm_symbol(raw.get("Code"))
        name = clean_text(raw.get("Name"))
        market = normalize_market(raw.get("Market"), market_alt_map=market_alt_map)
        if not symbol or market not in {"KOSPI", "KOSDAQ"} or not is_warning_eligible_name(name, excluded_keywords=excluded_keywords):
            continue
        mapping[symbol] = {"symbol": symbol, "name": name, "market": market}
    return mapping


def load_listing_frame(stock_master: dict[str, dict[str, Any]], *, listing_path: Path, market_alt_map: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(listing_path, dtype={"Code": str}, encoding="utf-8-sig")
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    df["Name"] = df["Name"].astype(str).str.strip()
    df["MarketNorm"] = df["Market"].apply(lambda value: normalize_market(value, market_alt_map=market_alt_map))
    df["ChagesRatio"] = pd.to_numeric(df["ChagesRatio"], errors="coerce")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df[df["Code"].isin(stock_master)].copy()
    return df.reset_index(drop=True)


def build_name_lookup(stock_master: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for row in stock_master.values():
        key = re.sub(r"\s+", "", row["name"])
        lookup.setdefault(key, []).append(row)
    return lookup


def resolve_symbol_by_name(
    name: str,
    market: str,
    *,
    name_lookup: dict[str, list[dict[str, Any]]],
    market_alt_map: dict[str, str],
) -> tuple[str, str]:
    normalized_name = re.sub(r"\s+", "", clean_text(name))
    candidates = list(name_lookup.get(normalized_name, []))
    market_normalized = normalize_market(market, market_alt_map=market_alt_map)
    if market_normalized:
        scoped = [row for row in candidates if normalize_market(row.get("market"), market_alt_map=market_alt_map) == market_normalized]
        if len(scoped) == 1:
            return scoped[0]["symbol"], normalize_market(scoped[0]["market"], market_alt_map=market_alt_map)
        if len(scoped) > 1:
            candidates = scoped
    if len(candidates) == 1:
        return candidates[0]["symbol"], normalize_market(candidates[0]["market"], market_alt_map=market_alt_map)
    if candidates:
        prioritized = sorted(
            candidates,
            key=lambda row: 0 if normalize_market(row.get("market"), market_alt_map=market_alt_map) == market_normalized else 1,
        )
        return prioritized[0]["symbol"], normalize_market(prioritized[0]["market"], market_alt_map=market_alt_map)
    return "", market_normalized


def build_stale_snapshot(
    name: str,
    previous_snapshot: dict[str, Any] | None,
    now_iso: str,
    error: Exception,
    *,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(defaults or {})
    payload.update(previous_snapshot or {})
    rows = payload.get("rows")
    payload["rows"] = rows if isinstance(rows, list) else []
    payload["generated_at"] = now_iso
    payload["status"] = "stale"
    payload["stale_since"] = clean_text(payload.get("stale_since")) or now_iso
    payload["source_error"] = str(error)[:300]
    payload["snapshot_name"] = name
    payload["as_of"] = clean_text(payload.get("as_of")) or now_iso[:10]
    return payload


def save_snapshot(
    name: str,
    payload: dict[str, Any],
    *,
    web_dir: Path,
    additional_web_dirs: tuple[Path, ...],
    ensure_runtime_dir: Any,
) -> str:
    ensure_runtime_dir()
    web_dir.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    target = web_dir / f"{name}.json"
    target.write_text(serialized, encoding="utf-8")
    for mirror_dir in additional_web_dirs:
        if not mirror_dir.parent.exists():
            continue
        mirror_dir.mkdir(parents=True, exist_ok=True)
        (mirror_dir / f"{name}.json").write_text(serialized, encoding="utf-8")
    return str(target)
