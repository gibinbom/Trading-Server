from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from runtime_paths import RUNTIME_DIR
    from stock_card_sources import load_analyst_frame, load_event_frame
except Exception:
    from Disclosure.runtime_paths import RUNTIME_DIR
    from Disclosure.stock_card_sources import load_analyst_frame, load_event_frame


ROOT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT_DIR = ROOT_DIR.parent
LISTING_PATH = PROJECT_ROOT_DIR / "krx_listing.csv"
QUOTE_PATH = Path(RUNTIME_DIR) / "web_projections" / "quote_delayed_source_latest.json"
QUOTE_HOTSET_PATH = Path(RUNTIME_DIR) / "web_projections" / "quote_delayed_hotset_latest.json"
LEGACY_QUOTE_PATH = Path(RUNTIME_DIR) / "web_projections" / "quote_delayed_latest.json"
VALUATION_PATH = ROOT_DIR / "valuation" / "fair_value_snapshot_latest.csv"
ELIGIBLE_MARKETS = {"KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"}
EXCLUDED_NAME_KEYWORDS = ("ETF", "ETN", "리츠", "스팩", "SPAC", "기업인수목적", "유동화전문유한회사", "유한회사")


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def _normalize_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _is_preferred(name: Any) -> bool:
    text = _clean_text(name)
    if not text:
        return False
    return text.endswith("우") or text.endswith("우B") or any(text.endswith(f"{digit}우B") for digit in "1234")


def _excluded_reason(name: Any, market: Any) -> str:
    market_text = _clean_text(market)
    name_text = _clean_text(name)
    if market_text not in ELIGIBLE_MARKETS:
        return market_text or "시장 제외"
    if _is_preferred(name_text):
        return "우선주"
    upper_name = name_text.upper()
    for keyword in EXCLUDED_NAME_KEYWORDS:
        if keyword and (keyword in name_text or keyword in upper_name):
            return keyword
    return ""


def load_eligible_listing_df() -> pd.DataFrame:
    if not LISTING_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(LISTING_PATH, dtype={"Code": str})
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"Code": "symbol", "Name": "name", "Market": "market", "Marcap": "marcap"}).copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["name"] = df.get("name", "").map(_clean_text)
    df["market"] = df.get("market", "").map(_clean_text)
    df["excluded_reason"] = df.apply(lambda row: _excluded_reason(row.get("name"), row.get("market")), axis=1)
    df = df[df["excluded_reason"].eq("")].copy()
    if "marcap" in df.columns:
        df["marcap"] = pd.to_numeric(df["marcap"], errors="coerce")
    return df


def _read_quote_rows() -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    def score(row: dict[str, Any]) -> tuple[int, int]:
        status = _clean_text(row.get("price_status"))
        status_score = 4 if status == "지연시세" else 3 if status == "업데이트 지연" else 2 if status == "공식종가 fallback" else 1
        captured = _clean_text(row.get("price_captured_at") or row.get("captured_at"))
        try:
            captured_score = int(pd.Timestamp(captured).timestamp()) if captured else 0
        except Exception:
            captured_score = 0
        return status_score, captured_score

    for path in (QUOTE_PATH, QUOTE_HOTSET_PATH, LEGACY_QUOTE_PATH):
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            rows = payload.get("rows") or []
        elif isinstance(payload, list):
            rows = payload
        else:
            rows = []
        for row in rows if isinstance(rows, list) else []:
            symbol = _normalize_symbol((row or {}).get("symbol"))
            if not symbol:
                continue
            current = merged.get(symbol)
            if current is None or score(row) >= score(current):
                merged[symbol] = row
    return list(merged.values())


def load_high_turnover_symbols(limit: int = 500) -> list[str]:
    rows = _read_quote_rows()
    if not rows:
        return []
    df = pd.DataFrame(rows)
    if df.empty or "symbol" not in df.columns:
        return []
    for col in ("amount", "marcap", "price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df = df.sort_values(["amount", "marcap", "price"], ascending=[False, False, False], na_position="last")
    return [symbol for symbol in df["symbol"].tolist()[: max(0, int(limit))] if symbol]


def load_tp_visible_symbols(limit: int = 500) -> list[str]:
    if not VALUATION_PATH.exists():
        return []
    try:
        df = pd.read_csv(VALUATION_PATH, dtype={"symbol": str})
    except Exception:
        return []
    if df.empty or "symbol" not in df.columns:
        return []
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    if "valuation_tp_visible" in df.columns:
        df = df[df["valuation_tp_visible"].fillna(False)].copy()
    if "marcap" in df.columns:
        df["marcap"] = pd.to_numeric(df["marcap"], errors="coerce")
        df = df.sort_values(["marcap"], ascending=[False], na_position="last")
    return [symbol for symbol in df["symbol"].tolist()[: max(0, int(limit))] if symbol]


def load_incremental_consensus_symbols(
    *,
    analyst_days: int = 21,
    event_days: int = 45,
    quote_limit: int = 500,
    tp_limit: int = 500,
) -> list[str]:
    symbols: list[str] = []

    def append_symbol(value: Any) -> None:
        symbol = _normalize_symbol(value)
        if symbol and symbol not in symbols:
            symbols.append(symbol)

    analyst_df = load_analyst_frame(days=analyst_days)
    if analyst_df is not None and not analyst_df.empty and "symbol" in analyst_df.columns:
        for symbol in analyst_df["symbol"].tolist():
            append_symbol(symbol)

    event_df = load_event_frame(days=event_days)
    if event_df is not None and not event_df.empty and "symbol" in event_df.columns:
        for symbol in event_df["symbol"].tolist():
            append_symbol(symbol)

    for symbol in load_high_turnover_symbols(limit=quote_limit):
        append_symbol(symbol)
    for symbol in load_tp_visible_symbols(limit=tp_limit):
        append_symbol(symbol)
    return symbols
