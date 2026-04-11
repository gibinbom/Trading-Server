from __future__ import annotations

import json
import os
import re
from functools import lru_cache

import pandas as pd
import requests

try:
    from naver_index_fallback import fetch_naver_index_history
except Exception:
    from Disclosure.naver_index_fallback import fetch_naver_index_history


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_DIR = os.path.dirname(ROOT_DIR)
LISTING_PATH = os.path.join(PROJECT_ROOT_DIR, "krx_listing.csv")
INDEX_CACHE_DIR = os.path.join(ROOT_DIR, "cache", "index")
SECTOR_CACHE_DIR = os.path.join(ROOT_DIR, "cache", "sector_benchmark")
SECTOR_LINK_CACHE_PATH = os.path.join(SECTOR_CACHE_DIR, "sector_links.json")
BENCHMARK_HTTP_TIMEOUT_SEC = max(2.0, float(os.getenv("BENCHMARK_HTTP_TIMEOUT_SEC", "5")))


def _load_listing() -> pd.DataFrame:
    if not os.path.exists(LISTING_PATH):
        return pd.DataFrame()
    try:
        listing = pd.read_csv(LISTING_PATH, dtype={"Code": str})
    except Exception:
        return pd.DataFrame()
    listing["Code"] = listing["Code"].astype(str).str.zfill(6)
    listing["Market"] = listing.get("Market", "").fillna("").astype(str).str.upper()
    return listing


def load_listing_snapshot() -> pd.DataFrame:
    return _load_listing().copy()


def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception:
        return default


def _save_json(path: str, payload) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def load_market_map(symbols: list[str]) -> dict[str, str]:
    target = {str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()}
    listing = _load_listing_cached()
    if not target or listing.empty:
        return {}
    listing = listing[listing["Code"].isin(target)].copy()
    return dict(zip(listing["Code"], listing["Market"]))


def _fetch_sector_link(symbol: str, session: requests.Session) -> dict[str, str]:
    resp = session.get(
        f"https://finance.naver.com/item/main.naver?code={str(symbol).zfill(6)}",
        timeout=BENCHMARK_HTTP_TIMEOUT_SEC,
    )
    resp.raise_for_status()
    match = re.search(r'업종명\s*:\s*<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', resp.text, re.I | re.S)
    if not match:
        return {}
    href = match.group(1).strip()
    sector_url = href if href.startswith("http") else "https://finance.naver.com" + href
    key_match = re.search(r"[?&]no=(\d+)", sector_url)
    return {
        "sector_key": f"upjong_{key_match.group(1)}" if key_match else re.sub(r"\s+", " ", match.group(2)).strip(),
        "sector_name": re.sub(r"\s+", " ", match.group(2)).strip(),
        "sector_url": sector_url,
    }


def _fetch_sector_members(sector_url: str, session: requests.Session) -> list[str]:
    resp = session.get(sector_url, timeout=BENCHMARK_HTTP_TIMEOUT_SEC)
    resp.raise_for_status()
    return sorted({str(code).zfill(6) for code in re.findall(r"/item/main\.naver\?code=(\d{6})", resp.text, re.I)})


def build_sector_peer_map(symbols: list[str]) -> tuple[dict[str, str], dict[str, str], dict[str, list[str]]]:
    symbols = [str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()]
    if not symbols:
        return {}, {}, {}
    listing = _load_listing_cached()
    market_map = listing.set_index("Code")["Market"].to_dict() if not listing.empty else {}
    allowed = {"KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"}
    link_cache = _load_json(SECTOR_LINK_CACHE_PATH, {})
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    for symbol in symbols:
        if symbol not in link_cache:
            try:
                payload = _fetch_sector_link(symbol, session)
            except Exception:
                payload = {}
            if payload:
                link_cache[symbol] = payload
    _save_json(SECTOR_LINK_CACHE_PATH, link_cache)

    sector_names: dict[str, str] = {}
    sector_members: dict[str, list[str]] = {}
    for symbol in symbols:
        payload = link_cache.get(symbol) or {}
        sector_key = str(payload.get("sector_key") or "").strip()
        sector_url = str(payload.get("sector_url") or "").strip()
        if not sector_key or not sector_url:
            continue
        sector_names[sector_key] = str(payload.get("sector_name") or sector_key)
        cache_path = os.path.join(SECTOR_CACHE_DIR, f"{sector_key}.json")
        members = _load_json(cache_path, [])
        if not members:
            try:
                members = _fetch_sector_members(sector_url, session)
            except Exception:
                members = []
            if members:
                _save_json(cache_path, members)
        filtered = [code for code in members if market_map.get(code, "") in allowed]
        if symbol not in filtered and market_map.get(symbol, "") in allowed:
            filtered.append(symbol)
        sector_members[sector_key] = sorted(set(filtered))

    symbol_to_sector_key = {symbol: str((link_cache.get(symbol) or {}).get("sector_key") or "") for symbol in symbols}
    symbol_to_sector_name = {
        symbol: sector_names.get(symbol_to_sector_key.get(symbol, ""), str((link_cache.get(symbol) or {}).get("sector_name") or ""))
        for symbol in symbols
    }
    return symbol_to_sector_key, symbol_to_sector_name, sector_members


def load_index_history(index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    return _load_index_history_cached(str(index_code).upper(), str(start_date), str(end_date)).copy()


@lru_cache(maxsize=8)
def _load_index_history_cached(index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    os.makedirs(INDEX_CACHE_DIR, exist_ok=True)
    path = os.path.join(INDEX_CACHE_DIR, f"{index_code}.csv")
    cached = pd.DataFrame()
    if os.path.exists(path):
        try:
            cached = pd.read_csv(path, parse_dates=["Date"])
        except Exception:
            cached = pd.DataFrame()

    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    needs_refresh = cached.empty
    if not cached.empty:
        cached["Date"] = pd.to_datetime(cached["Date"], errors="coerce")
        cached = cached.dropna(subset=["Date"]).sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
        needs_refresh = bool(cached["Date"].min() > start_ts or cached["Date"].max() < end_ts)
    if needs_refresh:
        fresh = fetch_naver_index_history(
            index_code,
            start_date=start_ts.date(),
            end_date=end_ts.date(),
            lookback_days=max(260, (end_ts - start_ts).days + 30),
            sleep_sec=0.0,
        )
        if fresh is not None and not fresh.empty:
            cached = pd.concat([cached, fresh], ignore_index=True)
            cached["Date"] = pd.to_datetime(cached["Date"], errors="coerce")
            cached = cached.dropna(subset=["Date"]).sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
            cached.to_csv(path, index=False, encoding="utf-8-sig")
    if cached.empty:
        return cached
    return cached[(cached["Date"] >= start_ts) & (cached["Date"] <= end_ts)].reset_index(drop=True)


@lru_cache(maxsize=1)
def _load_listing_cached() -> pd.DataFrame:
    return _load_listing()
