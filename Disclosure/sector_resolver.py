from __future__ import annotations

import json
import os
import re
import time
from typing import Iterable

import requests


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(ROOT_DIR, "cache")
SECTOR_CACHE_PATH = os.path.join(CACHE_DIR, "sector_cache.json")
SECTOR_HTTP_TIMEOUT_SEC = max(2.0, float(os.getenv("SECTOR_HTTP_TIMEOUT_SEC", "5")))


def _load_wics26_symbol_map() -> dict[str, str]:
    try:
        from signals.wics_universe import load_effective_wics_symbol_map
    except Exception:
        try:
            from Disclosure.signals.wics_universe import load_effective_wics_symbol_map
        except Exception:
            load_effective_wics_symbol_map = None

    if load_effective_wics_symbol_map is not None:
        try:
            effective = load_effective_wics_symbol_map()
            if effective:
                return effective
        except Exception:
            pass

    try:
        from signals.wics_monitor import WICS_26_SECTORS
    except Exception:
        try:
            from Disclosure.signals.wics_monitor import WICS_26_SECTORS
        except Exception:
            return {}
    out: dict[str, str] = {}
    for sector_name, stocks in (WICS_26_SECTORS or {}).items():
        normalized = str(sector_name).split(". ", 1)[-1].strip()
        for symbol in (stocks or {}).keys():
            out[str(symbol).zfill(6)] = normalized
    return out


def _load_cache() -> dict[str, str]:
    if not os.path.exists(SECTOR_CACHE_PATH):
        return {}
    try:
        with open(SECTOR_CACHE_PATH, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_cache(payload: dict[str, str]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(SECTOR_CACHE_PATH, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def _fetch_naver_sector(symbol: str, session: requests.Session | None = None) -> str:
    sess = session or requests.Session()
    resp = sess.get(
        f"https://finance.naver.com/item/main.naver?code={str(symbol).zfill(6)}",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=SECTOR_HTTP_TIMEOUT_SEC,
    )
    resp.raise_for_status()
    text = resp.text
    match = re.search(r"업종명\s*:\s*<a [^>]+>([^<]+)</a>", text, re.I | re.S)
    if match:
        return re.sub(r"\s+", " ", match.group(1)).strip()
    return ""


def resolve_sector_map(symbols: Iterable[str], sleep_sec: float = 0.01, max_fetch: int | None = None) -> dict[str, str]:
    symbols = [str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()]
    if not symbols:
        return {}
    resolved = _load_wics26_symbol_map()
    cache = _load_cache()
    resolved.update({str(k).zfill(6): str(v) for k, v in cache.items() if v})
    missing = [symbol for symbol in symbols if not resolved.get(symbol)]
    if not missing:
        return {symbol: resolved.get(symbol, "Unknown") for symbol in symbols}
    if max_fetch is not None and int(max_fetch) >= 0:
        missing = missing[: int(max_fetch)]

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    dirty = False
    for symbol in missing:
        try:
            sector = _fetch_naver_sector(symbol, session=session)
        except Exception:
            sector = ""
        if sector:
            resolved[symbol] = sector
            cache[symbol] = sector
            dirty = True
        if sleep_sec > 0:
            time.sleep(min(float(sleep_sec), 0.05))
    if dirty:
        _save_cache(cache)
    return {symbol: resolved.get(symbol, "Unknown") for symbol in symbols}
