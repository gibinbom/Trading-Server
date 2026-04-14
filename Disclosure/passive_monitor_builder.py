from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

_IMPORT_ROOT = Path(__file__).resolve().parent.parent


def _extend_local_venv_site_packages() -> None:
    candidates = list((_IMPORT_ROOT / ".venv" / "lib").glob("python*/site-packages"))
    candidates.extend((_IMPORT_ROOT / ".venv" / "Lib").glob("python*/site-packages"))
    candidates.append(_IMPORT_ROOT / ".venv" / "Lib" / "site-packages")
    for candidate in candidates:
        if candidate.exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.append(candidate_str)


_extend_local_venv_site_packages()

try:
    import requests
except Exception:  # pragma: no cover - runtime dependency
    requests = None

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - runtime dependency
    BeautifulSoup = None  # type: ignore[assignment]

try:
    import FinanceDataReader as fdr
except Exception:  # pragma: no cover - runtime dependency
    fdr = None

try:
    from pykrx import stock as pykrx_stock
except Exception:  # pragma: no cover - runtime dependency
    pykrx_stock = None

try:
    from config import SETTINGS
    from context_alignment import load_latest_symbol_sector_map
    from mongo_read_models import MongoReadModelStore
    from passive_etf_gap import (
        build_etf_gap_snapshot_from_listing as _build_etf_gap_snapshot_from_listing_impl,
        classify_underlying_region as _classify_underlying_region_impl,
        extract_provider as _extract_provider_impl,
        extract_tracking_index_name as _extract_tracking_index_name_impl,
        is_passive_etf as _is_passive_etf_impl,
    )
    from official_index_clone import OFFICIAL_INDEX_INPUT_DIR, OfficialCloneInputError, build_official_index_rebalance_snapshot, load_official_clone_bundle
    from passive_monitor_support import (
        build_special_change_event_map as _build_special_change_event_map_impl,
        cache_is_fresh as _cache_is_fresh_impl,
        fetch_public_float_profile as _fetch_public_float_profile_impl,
        load_json_cache as _load_json_cache_impl,
        load_projection_rows as _load_projection_rows_impl,
        load_public_float_profiles as _load_public_float_profiles_impl,
        parse_public_float_profile_html as _parse_public_float_profile_html_impl,
        projection_file_candidates as _projection_file_candidates_impl,
        write_json_cache as _write_json_cache_impl,
    )
    from runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from signals.wics_universe import load_effective_wics_symbol_map, normalize_sector_name
except Exception:  # pragma: no cover - package import fallback
    from Disclosure.config import SETTINGS
    try:
        from Disclosure.context_alignment import load_latest_symbol_sector_map
    except Exception:  # pragma: no cover - optional dependency fallback
        load_latest_symbol_sector_map = None  # type: ignore[assignment]
    from Disclosure.mongo_read_models import MongoReadModelStore
    from Disclosure.passive_etf_gap import (
        build_etf_gap_snapshot_from_listing as _build_etf_gap_snapshot_from_listing_impl,
        classify_underlying_region as _classify_underlying_region_impl,
        extract_provider as _extract_provider_impl,
        extract_tracking_index_name as _extract_tracking_index_name_impl,
        is_passive_etf as _is_passive_etf_impl,
    )
    from Disclosure.official_index_clone import OFFICIAL_INDEX_INPUT_DIR, OfficialCloneInputError, build_official_index_rebalance_snapshot, load_official_clone_bundle
    from Disclosure.passive_monitor_support import (
        build_special_change_event_map as _build_special_change_event_map_impl,
        cache_is_fresh as _cache_is_fresh_impl,
        fetch_public_float_profile as _fetch_public_float_profile_impl,
        load_json_cache as _load_json_cache_impl,
        load_projection_rows as _load_projection_rows_impl,
        load_public_float_profiles as _load_public_float_profiles_impl,
        parse_public_float_profile_html as _parse_public_float_profile_html_impl,
        projection_file_candidates as _projection_file_candidates_impl,
        write_json_cache as _write_json_cache_impl,
    )
    from Disclosure.runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    try:
        from Disclosure.signals.wics_universe import load_effective_wics_symbol_map, normalize_sector_name
    except Exception:  # pragma: no cover - optional dependency fallback
        load_effective_wics_symbol_map = None  # type: ignore[assignment]
        normalize_sector_name = None  # type: ignore[assignment]


log = logging.getLogger("disclosure.passive_monitor")

ROOT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT_DIR = ROOT_DIR.parent
REPO_ROOT_DIR = PROJECT_ROOT_DIR.parent
LISTING_PATH = PROJECT_ROOT_DIR / "krx_listing.csv"
WEB_DIR = Path(RUNTIME_DIR) / "web_projections"
ADDITIONAL_WEB_DIRS = tuple(
    path
    for path in (
        REPO_ROOT_DIR / "Disclosure" / "runtime" / "web_projections",
        REPO_ROOT_DIR / "trading-value-web" / "data" / "web_projections",
    )
    if path != WEB_DIR
)

INDEX_SNAPSHOT_NAME = "index_rebalance_latest"
ETF_SNAPSHOT_NAME = "etf_gap_monitor_latest"
DEFAULT_LOOKBACK_DAYS = 120
DEFAULT_FETCH_WORKERS = 16
DEFAULT_POOL_BUFFER = 80
DEFAULT_INDEX_BUFFER = 15
DEFAULT_HISTORY_MARGIN = 0
DEFAULT_ETF_GAP_THRESHOLD_PCT = 0.5
DEFAULT_TOP30_COUNT = 30
DEFAULT_HTTP_TIMEOUT_SEC = float(os.getenv("PASSIVE_MONITOR_HTTP_TIMEOUT_SEC", "2.5"))
DEFAULT_INDEX_METHODOLOGY_MODE = str(os.getenv("PASSIVE_INDEX_METHODOLOGY_MODE", "proxy")).strip().lower()
DEFAULT_LONG_HISTORY_LOOKBACK_DAYS = 380
DEFAULT_PUBLIC_FLOAT_CACHE_MAX_AGE_HOURS = int(os.getenv("PASSIVE_PUBLIC_FLOAT_CACHE_MAX_AGE_HOURS", "120"))
DEFAULT_SPECIAL_EVENT_LOOKBACK_DAYS = 120
DEFAULT_DOMESTIC_ENTRY_RATIO = 0.9
DEFAULT_DOMESTIC_KEEP_RATIO = 1.1
DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE = 0.85
DEFAULT_SPECIAL_LARGECAP_RANK = 50
PUBLIC_FLOAT_PROFILE_URL = "https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{symbol}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
PUBLIC_FLOAT_PROFILE_CACHE = Path(RUNTIME_DIR) / "passive_public_float_profiles.json"
PUBLIC_MARKET_CAP_CACHE = Path(RUNTIME_DIR) / "passive_public_market_cap_cache.json"

DOMESTIC_INDEX_CONFIGS = (
    {
        "index_name": "KS200",
        "market": "KOSPI",
        "cutoff": 200,
        "market_weight": 0.75,
        "liquidity_weight": 0.25,
        "buffer": DEFAULT_INDEX_BUFFER,
        "entry_ratio": DEFAULT_DOMESTIC_ENTRY_RATIO,
        "keep_ratio": DEFAULT_DOMESTIC_KEEP_RATIO,
        "liquidity_coverage": DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE,
        "special_largecap_rank": DEFAULT_SPECIAL_LARGECAP_RANK,
        "quota_strategy": "sector_bucket",
    },
    {
        "index_name": "KQ150",
        "market": "KOSDAQ",
        "cutoff": 150,
        "market_weight": 0.75,
        "liquidity_weight": 0.25,
        "buffer": DEFAULT_INDEX_BUFFER,
        "entry_ratio": DEFAULT_DOMESTIC_ENTRY_RATIO,
        "keep_ratio": DEFAULT_DOMESTIC_KEEP_RATIO,
        "liquidity_coverage": DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE,
        "special_largecap_rank": 30,
        "quota_strategy": "kq150_tech_split",
    },
)

MSCI_PROXY_CONFIG = {
    "index_name": "MSCI Proxy",
    "market": "ALL",
    "cutoff": 100,
    "market_weight": 0.85,
    "liquidity_weight": 0.15,
    "buffer": DEFAULT_INDEX_BUFFER,
}

EXCLUDED_NAME_KEYWORDS = ("ETF", "ETN", "리츠", "스팩", "SPAC")
EXCLUDED_SUFFIXES = ("우", "우B", "우C", "1우", "2우", "3우")
PASSIVE_ETF_EXCLUDED_KEYWORDS = (
    "액티브",
    "ACTIVE",
    "레버리지",
    "인버스",
    "2X",
    "3X",
    "울트라",
    "커버드콜",
    "커버드 콜",
    "BUFFER",
    "타겟위클리",
)
FOREIGN_REGION_KEYWORDS = (
    "미국",
    "US",
    "S&P",
    "NASDAQ",
    "나스닥",
    "다우",
    "중국",
    "차이나",
    "일본",
    "니케이",
    "유럽",
    "EURO",
    "독일",
    "프랑스",
    "영국",
    "홍콩",
    "항셍",
    "인도",
    "베트남",
    "대만",
    "선진국",
    "신흥국",
    "글로벌",
    "WORLD",
    "MSCI ACWI",
    "MSCI EM",
)
DOMESTIC_REGION_KEYWORDS = ("KOREA", "KOSPI", "KOSDAQ", "코리아", "코스피", "코스닥", "KRX")
DOMESTIC_METHODOLOGY_VERSION = "institutional_proxy_v2"
PUBLIC_FAITHFUL_METHODOLOGY_VERSION = "public_faithful_v1"
DOMESTIC_SELECTION_PATHS = ("bucket_quota", "global_fill", "buffer_keep")
SPECIAL_CHANGE_EVENT_TYPES = {
    "MERGER": "합병 이벤트",
    "SPINOFF": "분할 이벤트",
    "STOCK_SPLIT": "주식분할 이벤트",
    "REVERSE_SPLIT_REDUCTION": "감자/병합 이벤트",
}
KQ150_TECH_CORE_BUCKETS = frozenset({"technology", "healthcare"})
KQ150_BROAD_BUCKETS = ("technology_core", "non_technology")
KS200_NON_MANUFACTURING_BUCKETS = ("financials", "consumer", "healthcare", "industrials", "materials_energy", "other")
KQ150_PUBLIC_BUCKET_TARGETS = {
    "technology": 40,
    "healthcare": 50,
    "materials_energy": 12,
    "industrials": 18,
    "consumer": 18,
    "other": 12,
}
SECTOR_BUCKET_LABELS = {
    "technology": "기술",
    "healthcare": "헬스케어",
    "consumer": "소비재",
    "industrials": "산업재",
    "financials": "금융",
    "materials_energy": "소재·에너지",
    "other": "기타",
}
SECTOR_BUCKET_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "technology",
        (
            "반도체",
            "IT하드웨어",
            "디스플레이",
            "소프트웨어",
            "인터넷",
            "전자부품",
            "통신장비",
            "네트워크",
            "게임",
            "엔터테인먼트/게임",
            "미디어",
            "콘텐츠",
            "컨텐츠",
            "플랫폼",
            "디지털",
            "IT서비스",
            "IT 서비스",
            "핸드셋",
            "통신서비스",
            "반도체장비",
            "전자장비",
            "전자제품",
        ),
    ),
    (
        "healthcare",
        (
            "제약",
            "바이오",
            "건강관리",
            "의료",
            "헬스케어",
            "생물공학",
            "진단",
            "의료기기",
            "생명과학",
        ),
    ),
    (
        "consumer",
        (
            "화장품",
            "뷰티",
            "유통",
            "식품",
            "음료",
            "담배",
            "호텔",
            "레저",
            "교육",
            "의류",
            "패션",
            "생활용품",
            "소비재",
            "가정용기기",
            "주류",
        ),
    ),
    (
        "industrials",
        (
            "자동차",
            "부품",
            "타이어",
            "조선",
            "해양",
            "기계",
            "건설",
            "방위",
            "우주항공",
            "운송",
            "해운",
            "항공",
            "자본재",
            "상사",
            "산업재",
            "전기장비",
            "전기제품",
            "무역회사",
        ),
    ),
    (
        "financials",
        (
            "은행",
            "증권",
            "보험",
            "금융",
            "카드",
            "창투",
            "벤처투자",
            "IB",
        ),
    ),
    (
        "materials_energy",
        (
            "화학",
            "석유",
            "철강",
            "금속",
            "소재",
            "비철",
            "에너지",
            "전력",
            "유틸리티",
            "가스",
            "배터리",
            "2차전지",
            "신재생",
            "건축자재",
        ),
    ),
)


if requests is not None:  # pragma: no cover - runtime dependency
    _ORIGINAL_REQUEST = requests.sessions.Session.request

    def _request_with_timeout(self, method: str, url: str, **kwargs: Any):  # type: ignore[no-untyped-def]
        kwargs.setdefault("timeout", DEFAULT_HTTP_TIMEOUT_SEC)
        return _ORIGINAL_REQUEST(self, method, url, **kwargs)

    requests.sessions.Session.request = _request_with_timeout


def _load_json_cache(path: Path) -> dict[str, Any]:
    return _load_json_cache_impl(path)


def _write_json_cache(path: Path, payload: dict[str, Any]) -> None:
    _write_json_cache_impl(path, payload, logger=log)


def _cache_is_fresh(fetched_at: str, max_age_hours: int) -> bool:
    return _cache_is_fresh_impl(fetched_at, max_age_hours)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build passive index/ETF monitoring snapshots.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print a short build summary.")
    parser.add_argument("--skip-mongo", action="store_true", help="Only write local projection files.")
    parser.add_argument("--times", default="07:05,20:10", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    parser.add_argument("--fetch-workers", type=int, default=DEFAULT_FETCH_WORKERS, help="Concurrent FDR history worker count.")
    parser.add_argument(
        "--methodology-mode",
        default=DEFAULT_INDEX_METHODOLOGY_MODE,
        choices=["proxy", "public-faithful", "official"],
        help="Domestic index methodology mode. 'official' requires licensed/official input files.",
    )
    return parser.parse_args()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "None"):
            return default
        number = float(str(value).replace(",", ""))
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", "None"):
            return default
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def _safe_ratio(value: Any, default: float = 0.0) -> float:
    number = _safe_float(value, default)
    if number is None:
        return default
    if number > 1.0:
        number = number / 100.0
    return max(0.0, min(1.0, float(number)))


def _parse_iso_date(value: Any) -> datetime.date | None:
    text = _clean_text(value)
    if not text or text in {"-", "None"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def _norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _normalize_market(value: Any) -> str:
    text = _clean_text(value).upper()
    if text.startswith("KOSPI"):
        return "KOSPI"
    if text.startswith("KOSDAQ"):
        return "KOSDAQ"
    return text


def _extract_provider(name: str) -> str:
    return _extract_provider_impl(name)


def _extract_tracking_index_name(name: str) -> str:
    return _extract_tracking_index_name_impl(name)


def _normalize_sector_label(value: Any) -> str:
    text = _clean_text(value)
    if not text or text == "미분류":
        return ""
    if normalize_sector_name is not None:
        try:
            text = normalize_sector_name(text)
        except Exception:
            text = _clean_text(text)
    return _clean_text(text)


def _sector_bucket_from_sector(sector: Any) -> str:
    normalized = _normalize_sector_label(sector)
    if not normalized:
        return "other"
    upper = normalized.upper()
    for bucket_code, keywords in SECTOR_BUCKET_KEYWORDS:
        if any(keyword.upper() in upper for keyword in keywords):
            return bucket_code
    return "other"


def _projection_file_candidates(file_name: str) -> list[Path]:
    return _projection_file_candidates_impl(file_name, (WEB_DIR, *ADDITIONAL_WEB_DIRS))


def _load_projection_rows(file_name: str) -> list[dict[str, Any]]:
    return _load_projection_rows_impl(file_name, (WEB_DIR, *ADDITIONAL_WEB_DIRS))


def _build_special_change_event_map(listing_df: pd.DataFrame, *, lookback_days: int = DEFAULT_SPECIAL_EVENT_LOOKBACK_DAYS) -> dict[str, dict[str, Any]]:
    return _build_special_change_event_map_impl(
        listing_df,
        rows=_load_projection_rows("event_calendar_latest.json"),
        lookback_days=lookback_days,
        normalize_symbol=_norm_symbol,
        normalize_market=_normalize_market,
        clean_text=_clean_text,
        parse_iso_date=_parse_iso_date,
        special_change_event_types=SPECIAL_CHANGE_EVENT_TYPES,
    )


def _upsert_sector_map_entry(sector_map: dict[str, str], symbol: str, sector: Any) -> None:
    normalized_symbol = _norm_symbol(symbol)
    normalized_sector = _normalize_sector_label(sector)
    if not normalized_symbol or not normalized_sector:
        return
    sector_map[normalized_symbol] = normalized_sector


def _load_sector_reference_map() -> dict[str, str]:
    sector_map: dict[str, str] = {}
    if load_effective_wics_symbol_map is not None:
        try:
            for symbol, sector in load_effective_wics_symbol_map().items():
                _upsert_sector_map_entry(sector_map, symbol, sector)
        except Exception as exc:  # pragma: no cover - optional dependency
            log.debug("wics sector map unavailable: %s", str(exc)[:180])

    if load_latest_symbol_sector_map is not None:
        try:
            for symbol, sector in load_latest_symbol_sector_map().items():
                if _norm_symbol(symbol) not in sector_map:
                    _upsert_sector_map_entry(sector_map, symbol, sector)
        except Exception as exc:  # pragma: no cover - optional dependency
            log.debug("context sector map unavailable: %s", str(exc)[:180])

    for file_name in (
        "stock_master.json",
        "stock_context_latest.json",
        "stock_flow_latest.json",
        "stock_flow_latest_source.json",
        "dashboard_latest.json",
    ):
        for row in _load_projection_rows(file_name):
            symbol = row.get("symbol") or row.get("code") or row.get("_id")
            sector = row.get("sector") or row.get("industry")
            _upsert_sector_map_entry(sector_map, symbol, sector)
    return sector_map


def _parse_public_float_profile_html(html: str) -> dict[str, Any]:
    return _parse_public_float_profile_html_impl(html, BeautifulSoup)


def _fetch_public_float_profile(symbol: str) -> dict[str, Any]:
    return _fetch_public_float_profile_impl(
        symbol,
        requests_module=requests,
        beautiful_soup=BeautifulSoup,
        public_float_profile_url=PUBLIC_FLOAT_PROFILE_URL,
        now_iso=_now_iso,
        normalize_symbol=_norm_symbol,
    )


def _load_public_float_profiles(symbols: list[str], *, worker_count: int) -> dict[str, dict[str, Any]]:
    return _load_public_float_profiles_impl(
        symbols,
        worker_count=worker_count,
        cache_path=PUBLIC_FLOAT_PROFILE_CACHE,
        max_age_hours=DEFAULT_PUBLIC_FLOAT_CACHE_MAX_AGE_HOURS,
        fetch_profile=_fetch_public_float_profile,
        normalize_symbol=_norm_symbol,
        clean_text=_clean_text,
        logger=log,
    )


def classify_underlying_region(text: str) -> str:
    return _classify_underlying_region_impl(
        text,
        domestic_keywords=DOMESTIC_REGION_KEYWORDS,
        foreign_keywords=FOREIGN_REGION_KEYWORDS,
    )


def is_passive_etf(name: str) -> bool:
    return _is_passive_etf_impl(name, excluded_keywords=PASSIVE_ETF_EXCLUDED_KEYWORDS)


def _is_regular_stock_name(name: str) -> bool:
    text = _clean_text(name)
    if not text:
        return False
    if any(keyword in text for keyword in EXCLUDED_NAME_KEYWORDS):
        return False
    if any(text.endswith(suffix) for suffix in EXCLUDED_SUFFIXES):
        return False
    return "관리" not in text


def _load_listing_frame() -> pd.DataFrame:
    df = pd.read_csv(LISTING_PATH, dtype={"Code": str}, encoding="utf-8-sig")
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    df["Name"] = df["Name"].astype(str).str.strip()
    df["MarketNorm"] = df["Market"].apply(_normalize_market)
    df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df[df["MarketNorm"].isin(["KOSPI", "KOSDAQ"])].copy()
    df = df[df["Name"].apply(_is_regular_stock_name)].copy()
    df = df[df["Code"].str.match(r"^\d{6}$", na=False)].copy()
    return df.reset_index(drop=True)


def _percentile_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float)
    return series.fillna(0.0).rank(method="average", pct=True)


def _state_confidence(distance_to_cut: int, state: str, member_source: str = "") -> float:
    confidence = 0.42 + min(0.46, abs(distance_to_cut) / 35.0 * 0.46)
    if state in {"watch_add", "borderline"}:
        confidence -= 0.14
    if "proxy" in member_source and state == "likely_drop":
        confidence -= 0.05
    return round(max(0.15, min(0.95, confidence)), 2)


def _current_member_symbols(previous_snapshot: dict[str, Any] | None, index_name: str) -> set[str]:
    if not isinstance(previous_snapshot, dict):
        return set()
    rows = previous_snapshot.get("rows")
    if not isinstance(rows, list):
        return set()
    return {
        _norm_symbol(row.get("symbol"))
        for row in rows
        if _clean_text(row.get("index_name")) == index_name and bool(row.get("current_member"))
    }


def _current_member_rows(previous_snapshot: dict[str, Any] | None, index_name: str) -> list[dict[str, Any]]:
    if not isinstance(previous_snapshot, dict):
        return []
    rows = previous_snapshot.get("rows")
    if not isinstance(rows, list):
        return []
    return [
        row
        for row in rows
        if isinstance(row, dict) and _clean_text(row.get("index_name")) == index_name and bool(row.get("current_member"))
    ]


def _allocate_bucket_targets(source_counts: dict[str, int], cutoff: int, available_buckets: set[str]) -> dict[str, int]:
    buckets = sorted(bucket for bucket in available_buckets if bucket)
    if not buckets or cutoff <= 0:
        return {}
    filtered_counts = {bucket: max(0, int(source_counts.get(bucket, 0))) for bucket in buckets}
    total_count = sum(filtered_counts.values())
    if total_count <= 0:
        return {bucket: 0 for bucket in buckets}

    targets = {bucket: 0 for bucket in buckets}
    remainders: list[tuple[float, int, str]] = []
    allocated = 0
    for bucket in buckets:
        raw_target = filtered_counts[bucket] / total_count * cutoff
        base_target = int(math.floor(raw_target))
        targets[bucket] = base_target
        allocated += base_target
        remainders.append((raw_target - base_target, filtered_counts[bucket], bucket))

    remaining = max(0, cutoff - allocated)
    for _, _, bucket in sorted(remainders, key=lambda item: (item[0], item[1], item[2]), reverse=True)[:remaining]:
        targets[bucket] += 1
    return targets


def _select_candidate_pool(frame: pd.DataFrame, market: str, cutoff: int, pool_buffer: int = DEFAULT_POOL_BUFFER) -> pd.DataFrame:
    scoped = frame.copy()
    if market != "ALL":
        scoped = scoped[scoped["MarketNorm"] == market].copy()
    if scoped.empty:
        return scoped
    marcap_top = scoped.sort_values("Marcap", ascending=False).head(cutoff + pool_buffer)["Code"].tolist()
    amount_top = scoped.sort_values("Amount", ascending=False).head(cutoff + pool_buffer)["Code"].tolist()
    wanted = set(marcap_top) | set(amount_top)
    return scoped[scoped["Code"].isin(wanted)].copy().reset_index(drop=True)


def _summarize_amount_history(frame: pd.DataFrame, *, price_col: str, volume_col: str, source_name: str) -> dict[str, Any] | None:
    if frame is None or frame.empty or price_col not in frame.columns or volume_col not in frame.columns:
        return None
    tail = frame.tail(60).copy()
    if tail.empty:
        return None
    tail[price_col] = pd.to_numeric(tail[price_col], errors="coerce")
    tail[volume_col] = pd.to_numeric(tail[volume_col], errors="coerce")
    amount_series = tail[price_col].fillna(0.0) * tail[volume_col].fillna(0.0)
    amount_series = amount_series[amount_series > 0]
    if amount_series.empty:
        return None
    as_of = ""
    try:
        as_of = pd.Timestamp(tail.index.max()).strftime("%Y-%m-%d")
    except Exception:
        as_of = ""
    return {
        "avg_amount_60d_krw": float(amount_series.mean()),
        "as_of": as_of,
        "history_source": source_name,
    }


def _fetch_recent_amount_metrics(symbol: str, start_date: str, end_date: str) -> dict[str, Any]:
    if pykrx_stock is None and fdr is None:
        return {"symbol": symbol, "avg_amount_60d_krw": None, "as_of": "", "history_source": "history_source_missing"}
    if pykrx_stock is not None:
        try:
            pykrx_frame = pykrx_stock.get_market_ohlcv_by_date(
                start_date.replace("-", ""),
                end_date.replace("-", ""),
                symbol,
            )
            summarized = _summarize_amount_history(pykrx_frame, price_col="종가", volume_col="거래량", source_name="pykrx_60d")
            if summarized:
                return {"symbol": symbol, **summarized}
        except Exception as exc:  # pragma: no cover - network dependent
            log.debug("pykrx amount history fetch failed for %s: %s", symbol, str(exc)[:180])
    if fdr is None:
        return {"symbol": symbol, "avg_amount_60d_krw": None, "as_of": "", "history_source": "pykrx_empty"}
    try:
        frame = fdr.DataReader(symbol, start_date, end_date)
        summarized = _summarize_amount_history(frame, price_col="Close", volume_col="Volume", source_name="fdr_60d")
        if summarized:
            return {"symbol": symbol, **summarized}
        return {"symbol": symbol, "avg_amount_60d_krw": None, "as_of": "", "history_source": "fdr_empty"}
    except Exception as exc:  # pragma: no cover - network dependent
        log.debug("amount history fetch failed for %s: %s", symbol, str(exc)[:180])
        return {"symbol": symbol, "avg_amount_60d_krw": None, "as_of": "", "history_source": "fdr_error"}


def _load_amount_metrics(symbols: list[str], *, worker_count: int) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    start_text = start_date.isoformat()
    end_text = end_date.isoformat()
    results: dict[str, dict[str, Any]] = {}
    max_workers = max(1, min(worker_count, len(symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_recent_amount_metrics, symbol, start_text, end_text): symbol
            for symbol in symbols
        }
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                payload = future.result()
            except Exception as exc:  # pragma: no cover - concurrency guard
                log.warning("amount history future failed for %s: %s", symbol, str(exc)[:180])
                payload = {"symbol": symbol, "avg_amount_60d_krw": None, "as_of": "", "history_source": "future_error"}
            results[symbol] = payload
    return results


def _fetch_average_market_cap_metric(symbol: str, shares: int, start_date: str, end_date: str) -> dict[str, Any]:
    if shares <= 0:
        return {"symbol": symbol, "avg_market_cap_1y_krw": None, "market_cap_history_source": "shares_missing", "as_of": ""}
    if pykrx_stock is None and fdr is None:
        return {"symbol": symbol, "avg_market_cap_1y_krw": None, "market_cap_history_source": "history_source_missing", "as_of": ""}

    def _summarize(frame: pd.DataFrame, price_col: str, source_name: str) -> dict[str, Any] | None:
        if frame is None or frame.empty or price_col not in frame.columns:
            return None
        scoped = frame.copy()
        scoped[price_col] = pd.to_numeric(scoped[price_col], errors="coerce")
        scoped = scoped[scoped[price_col] > 0]
        if scoped.empty:
            return None
        market_cap_series = scoped[price_col] * float(shares)
        as_of = ""
        try:
            as_of = pd.Timestamp(scoped.index.max()).strftime("%Y-%m-%d")
        except Exception:
            as_of = ""
        return {
            "symbol": symbol,
            "avg_market_cap_1y_krw": float(market_cap_series.mean()),
            "market_cap_history_source": source_name,
            "as_of": as_of,
        }

    if pykrx_stock is not None:
        try:
            pykrx_frame = pykrx_stock.get_market_ohlcv_by_date(start_date.replace("-", ""), end_date.replace("-", ""), symbol)
            summarized = _summarize(pykrx_frame, "종가", "pykrx_1y")
            if summarized:
                return summarized
        except Exception as exc:  # pragma: no cover - network dependent
            log.debug("pykrx market cap history fetch failed for %s: %s", symbol, str(exc)[:180])
    if fdr is not None:
        try:
            fdr_frame = fdr.DataReader(symbol, start_date, end_date)
            summarized = _summarize(fdr_frame, "Close", "fdr_1y")
            if summarized:
                return summarized
            return {"symbol": symbol, "avg_market_cap_1y_krw": None, "market_cap_history_source": "fdr_empty", "as_of": ""}
        except Exception as exc:  # pragma: no cover - network dependent
            log.debug("market cap history fetch failed for %s: %s", symbol, str(exc)[:180])
            return {"symbol": symbol, "avg_market_cap_1y_krw": None, "market_cap_history_source": "fdr_error", "as_of": ""}
    return {"symbol": symbol, "avg_market_cap_1y_krw": None, "market_cap_history_source": "pykrx_empty", "as_of": ""}


def _load_average_market_cap_metrics(shares_by_symbol: dict[str, int], *, worker_count: int) -> dict[str, dict[str, Any]]:
    symbols = sorted(symbol for symbol, shares in shares_by_symbol.items() if _norm_symbol(symbol) and int(shares or 0) > 0)
    if not symbols:
        return {}
    cache = _load_json_cache(PUBLIC_MARKET_CAP_CACHE)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DEFAULT_LONG_HISTORY_LOOKBACK_DAYS)
    start_text = start_date.isoformat()
    end_text = end_date.isoformat()
    results: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for symbol in symbols:
        cached = cache.get(symbol) if isinstance(cache.get(symbol), dict) else None
        if (
            cached
            and _clean_text(cached.get("as_of")) == end_text
            and cached.get("shares") == int(shares_by_symbol.get(symbol, 0))
            and _safe_float(cached.get("avg_market_cap_1y_krw")) is not None
        ):
            results[symbol] = cached
        else:
            missing.append(symbol)
    if missing:
        max_workers = max(1, min(worker_count, len(missing), 8))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_fetch_average_market_cap_metric, symbol, int(shares_by_symbol.get(symbol, 0)), start_text, end_text): symbol
                for symbol in missing
            }
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    payload = future.result()
                except Exception as exc:  # pragma: no cover - concurrency guard
                    payload = {
                        "symbol": symbol,
                        "avg_market_cap_1y_krw": None,
                        "market_cap_history_source": "future_error",
                        "source_error": str(exc)[:180],
                        "as_of": "",
                    }
                payload["shares"] = int(shares_by_symbol.get(symbol, 0))
                results[symbol] = payload
                cache[symbol] = payload
        _write_json_cache(PUBLIC_MARKET_CAP_CACHE, cache)
    return results


def _resolve_listing_as_of(listing_path: Path = LISTING_PATH) -> str:
    try:
        modified_at = datetime.fromtimestamp(listing_path.stat().st_mtime)
        return modified_at.date().isoformat()
    except Exception:
        return ""


def _latest_weekday_as_of(now: datetime | None = None) -> str:
    current = now or datetime.now()
    current_date = current.date()
    while current_date.weekday() >= 5:
        current_date -= timedelta(days=1)
    return current_date.isoformat()


def _resolve_index_snapshot_as_of(amount_metrics: dict[str, dict[str, Any]], listing_path: Path = LISTING_PATH) -> str:
    listing_as_of = _resolve_listing_as_of(listing_path)
    metric_as_of_values = sorted(
        {
            _clean_text(metrics.get("as_of"))
            for metrics in amount_metrics.values()
            if _clean_text(metrics.get("as_of"))
        }
    )
    candidate_dates = [value for value in [listing_as_of, _latest_weekday_as_of()] if value]
    if metric_as_of_values:
        candidate_dates.append(metric_as_of_values[-1])
    return max(candidate_dates) if candidate_dates else _now_iso()[:10]


def _resolved_sector_for_symbol(symbol: str, raw_sector: Any, sector_map: dict[str, str]) -> str:
    normalized = _normalize_sector_label(raw_sector)
    if normalized:
        return normalized
    return sector_map.get(_norm_symbol(symbol), "")


def _resolved_bucket_for_symbol(symbol: str, raw_sector: Any, raw_bucket: Any, sector_map: dict[str, str]) -> str:
    resolved_sector = _resolved_sector_for_symbol(symbol, raw_sector, sector_map)
    derived_bucket = _sector_bucket_from_sector(resolved_sector)
    if derived_bucket != "other" or resolved_sector:
        return derived_bucket
    fallback_bucket = _clean_text(raw_bucket)
    return fallback_bucket or "other"


def _kq150_broad_bucket(bucket: str) -> str:
    return "technology_core" if _clean_text(bucket) in KQ150_TECH_CORE_BUCKETS else "non_technology"


def _selection_rank_limit(target: int, ratio: float, *, minimum: int = 1) -> int:
    if target <= 0:
        return 0
    return max(minimum, int(math.ceil(float(target) * float(ratio))))


def _broad_bucket_counts(source_counts: dict[str, int]) -> dict[str, int]:
    counts = {bucket: 0 for bucket in KQ150_BROAD_BUCKETS}
    for bucket, count in source_counts.items():
        counts[_kq150_broad_bucket(bucket)] = counts.get(_kq150_broad_bucket(bucket), 0) + max(0, int(count))
    return counts


def _derive_kq150_bucket_targets(source_counts: dict[str, int], cutoff: int, available_buckets: set[str]) -> dict[str, int]:
    if cutoff <= 0 or not available_buckets:
        return {}
    usable_counts = {
        bucket: max(0, int(source_counts.get(bucket, 0)))
        for bucket in sorted(available_buckets)
        if bucket
    }
    if not any(usable_counts.values()):
        return _allocate_bucket_targets({}, cutoff, available_buckets)

    broad_counts = _broad_bucket_counts(usable_counts)
    raw_tech_share = broad_counts.get("technology_core", 0) / max(1, sum(broad_counts.values()))
    tech_share = min(0.75, max(0.45, raw_tech_share))
    tech_target = int(round(cutoff * tech_share))
    non_tech_target = max(0, cutoff - tech_target)
    broad_targets = {
        "technology_core": tech_target,
        "non_technology": non_tech_target,
    }

    targets = {bucket: 0 for bucket in usable_counts}
    for broad_bucket in KQ150_BROAD_BUCKETS:
        group_buckets = {bucket for bucket in usable_counts if _kq150_broad_bucket(bucket) == broad_bucket}
        if not group_buckets:
            continue
        group_counts = {bucket: usable_counts.get(bucket, 0) for bucket in group_buckets}
        group_targets = _allocate_bucket_targets(group_counts, broad_targets.get(broad_bucket, 0), group_buckets)
        for bucket, count in group_targets.items():
            targets[bucket] = targets.get(bucket, 0) + int(count)

    allocated = sum(targets.values())
    if allocated < cutoff:
        remaining_targets = _allocate_bucket_targets(usable_counts, cutoff - allocated, available_buckets)
        for bucket, count in remaining_targets.items():
            targets[bucket] = targets.get(bucket, 0) + int(count)
    return dict(sorted(targets.items()))


def _allocate_fixed_targets(base_targets: dict[str, int], available_buckets: set[str], cutoff: int) -> dict[str, int]:
    available = {bucket for bucket in available_buckets if bucket}
    targets = {bucket: int(base_targets.get(bucket, 0)) for bucket in available}
    allocated = sum(targets.values())
    if allocated == cutoff:
        return dict(sorted(targets.items()))
    if allocated < cutoff:
        fallback = _allocate_bucket_targets({bucket: max(1, targets.get(bucket, 0)) for bucket in available}, cutoff - allocated, available)
        for bucket, count in fallback.items():
            targets[bucket] = targets.get(bucket, 0) + int(count)
    else:
        overflow = allocated - cutoff
        for bucket in sorted(targets, key=lambda item: (targets[item], item), reverse=True):
            if overflow <= 0:
                break
            removable = min(overflow, max(0, targets[bucket]))
            targets[bucket] -= removable
            overflow -= removable
    return dict(sorted(targets.items()))


def _eligible_bucket_order(frame: pd.DataFrame, bucket: str) -> pd.DataFrame:
    scoped = frame[frame["sector_bucket"] == bucket].copy()
    if scoped.empty:
        return scoped
    scoped["bucket_liquidity_rank_public"] = scoped["avg_amount_60d_krw"].rank(method="first", ascending=False)
    scoped["bucket_size_public"] = len(scoped)
    scoped["bucket_liquidity_limit_public"] = max(1, int(math.ceil(len(scoped) * DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE)))
    scoped["liquidity_gate_pass_public"] = scoped["bucket_liquidity_rank_public"] <= scoped["bucket_liquidity_limit_public"]
    scoped = scoped.sort_values(["size_proxy_krw", "avg_amount_60d_krw", "marcap_krw"], ascending=[False, False, False]).reset_index(drop=True)
    scoped["bucket_marcap_rank_public"] = scoped.index + 1
    return scoped


def _build_public_faithful_bucket_targets(index_name: str, available_buckets: set[str], eligible_frame: pd.DataFrame) -> dict[str, int]:
    if index_name == "KQ150":
        return _allocate_fixed_targets(KQ150_PUBLIC_BUCKET_TARGETS, available_buckets, 150)

    targets: dict[str, int] = {}
    for bucket in KS200_NON_MANUFACTURING_BUCKETS:
        if bucket not in available_buckets:
            continue
        scoped = _eligible_bucket_order(eligible_frame, bucket)
        if scoped.empty:
            continue
        total_marcap = float(scoped["size_proxy_krw"].sum() or 0.0)
        if total_marcap <= 0:
            continue
        cumulative = scoped["size_proxy_krw"].cumsum() / total_marcap
        selected = scoped[(cumulative <= 0.7) & scoped["liquidity_gate_pass_public"]]
        if selected.empty and not scoped.empty:
            selected = scoped.head(1)
        targets[bucket] = len(selected)

    non_mfg_count = sum(targets.values())
    manufacturing_slots = max(0, 200 - non_mfg_count)
    manufacturing_pool = eligible_frame[~eligible_frame["sector_bucket"].isin(KS200_NON_MANUFACTURING_BUCKETS)].copy()
    if not manufacturing_pool.empty and manufacturing_slots > 0:
        manufacturing_pool = manufacturing_pool.sort_values(["size_proxy_krw", "avg_amount_60d_krw", "marcap_krw"], ascending=[False, False, False]).reset_index(drop=True)
        manufacturing_pool["liq_rank"] = manufacturing_pool["avg_amount_60d_krw"].rank(method="first", ascending=False)
        manufacturing_pool["liq_limit"] = max(1, int(math.ceil(len(manufacturing_pool) * DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE)))
        manufacturing_selected = manufacturing_pool[manufacturing_pool["liq_rank"] <= manufacturing_pool["liq_limit"]].head(manufacturing_slots)
        for bucket, count in manufacturing_selected["sector_bucket"].value_counts().items():
            targets[str(bucket)] = targets.get(str(bucket), 0) + int(count)
    return dict(sorted(_allocate_fixed_targets(targets, available_buckets, 200).items()))


def build_public_faithful_index_rows(
    universe_df: pd.DataFrame,
    *,
    index_name: str,
    cutoff: int,
    member_symbols: set[str] | None = None,
    member_source: str = "previous_snapshot_proxy",
    special_event_map: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if universe_df.empty:
        return [], {"bucket_targets": {}, "target_source": "public_faithful"}

    working = universe_df.copy()
    working["sector"] = working.get("sector", pd.Series(dtype=str)).map(_normalize_sector_label).fillna("")
    working["sector_bucket"] = working.get("sector_bucket", pd.Series(dtype=str)).map(lambda value: _clean_text(value) or "other").fillna("other")
    working["size_proxy_krw"] = pd.to_numeric(working.get("size_proxy_krw", working.get("marcap_krw")), errors="coerce").fillna(0.0)
    working["avg_ffmc_1y_krw"] = pd.to_numeric(working.get("avg_ffmc_1y_krw", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    working["ffmc_proxy_krw"] = pd.to_numeric(working.get("ffmc_proxy_krw", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    working["free_float_ratio"] = pd.to_numeric(working.get("free_float_ratio", pd.Series(dtype=float)), errors="coerce")
    working = working.sort_values(["size_proxy_krw", "avg_amount_60d_krw", "marcap_krw"], ascending=[False, False, False]).reset_index(drop=True)
    working["market_cap_rank"] = working.index + 1
    working["special_largecap"] = working["market_cap_rank"] <= (30 if index_name == "KQ150" else 50)

    bucket_frames = [_eligible_bucket_order(working, bucket) for bucket in sorted(set(working["sector_bucket"].tolist()))]
    eligible_frame = pd.concat(bucket_frames, ignore_index=True) if bucket_frames else working.head(0).copy()
    if eligible_frame.empty:
        eligible_frame = working.copy()
        eligible_frame["liquidity_gate_pass_public"] = True

    bucket_targets = _build_public_faithful_bucket_targets(index_name, set(working["sector_bucket"].tolist()), eligible_frame)
    current_members = {_norm_symbol(symbol) for symbol in (member_symbols or set()) if _norm_symbol(symbol)}
    if not current_members:
        current_members = set(working.head(cutoff)["symbol"].tolist())
        member_source = "bootstrap_topcut_proxy"

    eligible_frame = eligible_frame.sort_values(["size_proxy_krw", "avg_amount_60d_krw", "marcap_krw"], ascending=[False, False, False]).reset_index(drop=True)
    ordered_symbols = [str(symbol) for symbol in eligible_frame["symbol"].tolist()]
    symbol_rows = {str(row["symbol"]): row for row in eligible_frame.to_dict(orient="records")}
    selected: set[str] = set()
    selected_paths: dict[str, str] = {}

    for bucket, target in bucket_targets.items():
        bucket_symbols = [symbol for symbol in ordered_symbols if str(symbol_rows[symbol].get("sector_bucket")) == bucket]
        for symbol in bucket_symbols[:target]:
            selected.add(symbol)
            selected_paths[symbol] = "bucket_quota"

    if len(selected) < cutoff:
        for symbol in ordered_symbols:
            if len(selected) >= cutoff:
                break
            if symbol in selected:
                continue
            row = symbol_rows[symbol]
            if not (bool(row.get("liquidity_gate_pass_public")) or bool(row.get("special_largecap")) or symbol in current_members):
                continue
            selected.add(symbol)
            selected_paths[symbol] = "global_fill"

    for symbol in ordered_symbols:
        if symbol in selected or symbol not in current_members:
            continue
        row = symbol_rows[symbol]
        bucket = str(row.get("sector_bucket"))
        keep_limit = max(_selection_rank_limit(int(bucket_targets.get(bucket, 0)), DEFAULT_DOMESTIC_KEEP_RATIO), _selection_rank_limit(cutoff, DEFAULT_DOMESTIC_KEEP_RATIO))
        bucket_rank = _safe_int(row.get("bucket_marcap_rank_public"), 999999)
        global_rank = _safe_int(row.get("market_cap_rank"), 999999)
        if min(bucket_rank, global_rank) <= keep_limit:
            selected.add(symbol)
            selected_paths[symbol] = "buffer_keep"

    reserve_candidates: list[dict[str, Any]] = []
    special_largecap_candidates: list[dict[str, Any]] = []
    reserve_counter = 0
    for symbol in ordered_symbols:
        if symbol in current_members:
            continue
        row = symbol_rows[symbol]
        if bool(row.get("special_largecap")) and len(special_largecap_candidates) < 5:
            special_largecap_candidates.append(
                {
                    "symbol": symbol,
                    "name": _clean_text(row.get("name")),
                    "state": "special_largecap_proxy",
                    "predicted_rank": _safe_int(row.get("market_cap_rank"), 999999),
                    "bucket_rank": _safe_int(row.get("bucket_marcap_rank_public"), 999999),
                    "sector_bucket": _clean_text(row.get("sector_bucket")) or "other",
                    "special_case_signal": "top30_special_proxy" if index_name == "KQ150" else "top50_special_proxy",
                }
            )
        if symbol in selected:
            continue
        if not (bool(row.get("liquidity_gate_pass_public")) or bool(row.get("special_largecap"))):
            continue
        reserve_counter += 1
        reserve_candidates.append(
            {
                "symbol": symbol,
                "name": _clean_text(row.get("name")),
                "state": "reserve_proxy",
                "predicted_rank": _safe_int(row.get("market_cap_rank"), 999999),
                "bucket_rank": _safe_int(row.get("bucket_marcap_rank_public"), 999999),
                "sector_bucket": _clean_text(row.get("sector_bucket")) or "other",
                "reserve_rank_proxy": reserve_counter,
            }
        )
        if reserve_counter >= 5:
            break
    reserve_rank_map = {item["symbol"]: int(item["reserve_rank_proxy"]) for item in reserve_candidates}
    special_change_candidates: list[dict[str, Any]] = []

    rows: list[dict[str, Any]] = []
    for row in eligible_frame.to_dict(orient="records"):
        symbol = _norm_symbol(row.get("symbol"))
        bucket = _clean_text(row.get("sector_bucket")) or "other"
        bucket_target = int(bucket_targets.get(bucket, 0))
        bucket_rank = _safe_int(row.get("bucket_marcap_rank_public"), 999999)
        global_rank = _safe_int(row.get("market_cap_rank"), 999999)
        current_member = symbol in current_members
        is_selected = symbol in selected
        entry_limit = max(_selection_rank_limit(bucket_target, DEFAULT_DOMESTIC_ENTRY_RATIO), _selection_rank_limit(cutoff, DEFAULT_DOMESTIC_ENTRY_RATIO))
        keep_limit = max(_selection_rank_limit(bucket_target, DEFAULT_DOMESTIC_KEEP_RATIO), _selection_rank_limit(cutoff, DEFAULT_DOMESTIC_KEEP_RATIO))
        if not current_member and is_selected and min(bucket_rank, global_rank) <= entry_limit:
            state = "likely_add"
        elif not current_member and (is_selected or min(bucket_rank, global_rank) <= keep_limit):
            state = "watch_add"
        elif current_member and not is_selected and min(bucket_rank, global_rank) > keep_limit:
            state = "likely_drop"
        else:
            state = "stable"
        special_event = (special_event_map or {}).get(symbol, {})
        special_case_signal = ""
        if not current_member and bool(row.get("special_largecap")):
            special_case_signal = "top30_special_proxy" if index_name == "KQ150" else "top50_special_proxy"
        rows.append(
            {
                "symbol": symbol,
                "name": _clean_text(row.get("name")),
                "market": _clean_text(row.get("market")),
                "sector": _clean_text(row.get("sector")) or "미분류",
                "sector_bucket": bucket,
                "index_name": index_name,
                "current_member": current_member,
                "current_member_source": member_source,
                "predicted_rank": global_rank,
                "bucket_rank": bucket_rank,
                "bucket_target_count": bucket_target,
                "distance_to_cut": min(global_rank - cutoff, bucket_rank - bucket_target if bucket_target > 0 else global_rank - cutoff),
                "state": state,
                "confidence": _state_confidence(min(global_rank - cutoff, bucket_rank - bucket_target if bucket_target > 0 else global_rank - cutoff), state, member_source),
                "as_of": _clean_text(row.get("as_of")),
                "market_cap_krw": int(round(_safe_float(row.get("marcap_krw"), 0.0) or 0.0)),
                "avg_amount_60d_krw": int(round(_safe_float(row.get("avg_amount_60d_krw"), 0.0) or 0.0)),
                "free_float_ratio": round(_safe_float(row.get("free_float_ratio"), 0.0) or 0.0, 6) or None,
                "float_shares": _safe_int(row.get("float_shares")),
                "ffmc_proxy_krw": int(round(_safe_float(row.get("ffmc_proxy_krw"), 0.0) or 0.0)),
                "avg_market_cap_1y_krw": int(round(_safe_float(row.get("avg_market_cap_1y_krw"), 0.0) or 0.0)),
                "avg_ffmc_1y_krw": int(round(_safe_float(row.get("avg_ffmc_1y_krw"), 0.0) or 0.0)),
                "market_cap_rank": global_rank,
                "liquidity_gate_pass": bool(row.get("liquidity_gate_pass_public")),
                "entry_rank_limit": entry_limit,
                "keep_rank_limit": keep_limit,
                "special_largecap": bool(row.get("special_largecap")),
                "special_case_signal": special_case_signal,
                "reserve_rank_proxy": reserve_rank_map.get(symbol),
                "special_change_signal": _clean_text(special_event.get("signal")),
                "special_change_event_date": _clean_text(special_event.get("event_date")),
                "special_change_title": _clean_text(special_event.get("title")),
                "selection_path": selected_paths.get(symbol, ""),
                "rank_method": PUBLIC_FAITHFUL_METHODOLOGY_VERSION,
            }
        )
        if special_event and len(special_change_candidates) < 5:
            special_change_candidates.append(
                {
                    "symbol": symbol,
                    "name": _clean_text(row.get("name")),
                    "predicted_rank": global_rank,
                    "state": state,
                    "signal": _clean_text(special_event.get("signal")),
                    "event_date": _clean_text(special_event.get("event_date")),
                    "title": _clean_text(special_event.get("title")),
                }
            )
    return rows, {
        "bucket_targets": bucket_targets,
        "target_source": "public_faithful",
        "reserve_candidates": reserve_candidates,
        "special_largecap_candidates": special_largecap_candidates,
        "special_change_candidates": special_change_candidates,
    }


def _previous_bucket_counts(
    previous_rows: list[dict[str, Any]],
    *,
    sector_map: dict[str, str],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in previous_rows:
        symbol = _norm_symbol(row.get("symbol"))
        bucket = _resolved_bucket_for_symbol(symbol, row.get("sector"), row.get("sector_bucket"), sector_map)
        counts[bucket or "other"] += 1
    return dict(counts)


def _derive_bucket_targets(
    working: pd.DataFrame,
    *,
    index_name: str,
    cutoff: int,
    previous_rows: list[dict[str, Any]],
    sector_map: dict[str, str],
) -> tuple[dict[str, int], str]:
    available_buckets = set(working["sector_bucket"].dropna().astype(str).tolist()) or {"other"}
    previous_counts = _previous_bucket_counts(previous_rows, sector_map=sector_map)
    usable_previous = {bucket: count for bucket, count in previous_counts.items() if bucket in available_buckets and count > 0}
    if usable_previous:
        if index_name == "KQ150":
            return _derive_kq150_bucket_targets(usable_previous, cutoff, available_buckets), "previous_snapshot_proxy"
        return _allocate_bucket_targets(usable_previous, cutoff, available_buckets), "previous_snapshot_proxy"

    bootstrap_source = working.copy()
    if "composite_score" in bootstrap_source.columns:
        bootstrap_source = bootstrap_source.sort_values(
            ["composite_score", "marcap_krw", "avg_amount_60d_krw"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
    bootstrap_counts = {
        str(bucket): int(count)
        for bucket, count in bootstrap_source.head(cutoff)["sector_bucket"].fillna("other").astype(str).value_counts().items()
        if bucket
    }
    if index_name == "KQ150":
        return _derive_kq150_bucket_targets(bootstrap_counts, cutoff, available_buckets), "bootstrap_topcut_proxy"
    return _allocate_bucket_targets(bootstrap_counts, cutoff, available_buckets), "bootstrap_topcut_proxy"


def build_domestic_index_rows(
    universe_df: pd.DataFrame,
    *,
    index_name: str,
    cutoff: int,
    member_symbols: set[str] | None = None,
    member_source: str = "rank_proxy",
    bucket_targets: dict[str, int] | None = None,
    buffer: int = DEFAULT_INDEX_BUFFER,
    market_weight: float = 0.75,
    liquidity_weight: float = 0.25,
    methodology_version: str = DOMESTIC_METHODOLOGY_VERSION,
    entry_ratio: float = DEFAULT_DOMESTIC_ENTRY_RATIO,
    keep_ratio: float = DEFAULT_DOMESTIC_KEEP_RATIO,
    liquidity_coverage: float = DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE,
    special_largecap_rank: int = DEFAULT_SPECIAL_LARGECAP_RANK,
) -> list[dict[str, Any]]:
    if universe_df.empty:
        return []
    working = universe_df.copy()
    working["sector"] = working.get("sector", pd.Series(dtype=str)).map(_normalize_sector_label).fillna("")
    working["sector_bucket"] = working.get("sector_bucket", pd.Series(dtype=str)).map(lambda value: _clean_text(value) or "other").fillna("other")
    working["marcap_percentile"] = _percentile_series(working["marcap_krw"])
    working["liquidity_percentile"] = _percentile_series(working["avg_amount_60d_krw"])
    working["composite_score"] = working["marcap_percentile"] * market_weight + working["liquidity_percentile"] * liquidity_weight
    working["market_cap_rank"] = working["marcap_krw"].rank(method="first", ascending=False).astype(int)
    working["special_largecap"] = working["market_cap_rank"] <= max(1, special_largecap_rank)
    working = working.sort_values(["composite_score", "marcap_krw", "avg_amount_60d_krw"], ascending=[False, False, False]).reset_index(drop=True)
    working["global_rank"] = working.index + 1

    bucket_order = working.sort_values(["sector_bucket", "composite_score", "marcap_krw", "avg_amount_60d_krw"], ascending=[True, False, False, False]).reset_index(drop=True)
    bucket_order["bucket_rank"] = bucket_order.groupby("sector_bucket").cumcount() + 1
    working = working.merge(bucket_order[["symbol", "bucket_rank"]], on="symbol", how="left")
    working["bucket_size"] = working.groupby("sector_bucket")["symbol"].transform("count")
    working["bucket_liquidity_rank"] = (
        working.groupby("sector_bucket")["avg_amount_60d_krw"].rank(method="first", ascending=False).astype(int)
    )
    working["bucket_liquidity_limit"] = working["bucket_size"].map(
        lambda size: max(1, int(math.ceil(max(1, int(size)) * float(liquidity_coverage))))
    )
    working["liquidity_gate_pass"] = (
        (working["bucket_liquidity_rank"] <= working["bucket_liquidity_limit"]) | working["special_largecap"].astype(bool)
    )

    current_members = {_norm_symbol(symbol) for symbol in (member_symbols or set()) if _norm_symbol(symbol)}
    resolved_member_source = _clean_text(member_source) or "rank_proxy"
    if not current_members:
        current_members = set(working.head(cutoff)["symbol"].tolist())
        resolved_member_source = "bootstrap_topcut_proxy"

    resolved_bucket_targets = {str(bucket): int(count) for bucket, count in (bucket_targets or {}).items() if str(bucket)}
    if not resolved_bucket_targets:
        resolved_bucket_targets = _allocate_bucket_targets(
            {
                str(bucket): int(count)
                for bucket, count in working.head(cutoff)["sector_bucket"].fillna("other").astype(str).value_counts().items()
            },
            cutoff,
            set(working["sector_bucket"].astype(str).tolist()),
        )

    working["bucket_target_count"] = working["sector_bucket"].map(lambda bucket: int(resolved_bucket_targets.get(str(bucket), 0)))
    symbol_rows = {str(row["symbol"]): row for row in working.to_dict(orient="records")}
    ordered_symbols = [str(symbol) for symbol in working["symbol"].tolist()]
    selected_paths: dict[str, str] = {}
    selected_symbols: set[str] = set()
    remaining_slots = cutoff

    for bucket, target in resolved_bucket_targets.items():
        if target <= 0:
            continue
        bucket_symbols = [symbol for symbol in ordered_symbols if str(symbol_rows[symbol].get("sector_bucket")) == bucket]
        eligible_bucket_symbols = [
            symbol
            for symbol in bucket_symbols
            if bool(symbol_rows[symbol].get("liquidity_gate_pass")) or symbol in current_members
        ]
        for symbol in eligible_bucket_symbols[:target]:
            if symbol in selected_symbols:
                continue
            selected_symbols.add(symbol)
            selected_paths[symbol] = "bucket_quota"
            remaining_slots -= 1

    if remaining_slots > 0:
        for symbol in ordered_symbols:
            if remaining_slots <= 0:
                break
            if symbol in selected_symbols:
                continue
            row = symbol_rows.get(symbol, {})
            if not (bool(row.get("liquidity_gate_pass")) or bool(row.get("special_largecap")) or symbol in current_members):
                continue
            selected_symbols.add(symbol)
            selected_paths[symbol] = "global_fill"
            remaining_slots -= 1

    for symbol in ordered_symbols:
        if symbol not in current_members or symbol in selected_symbols:
            continue
        row = symbol_rows.get(symbol, {})
        bucket_target = int(row.get("bucket_target_count") or 0)
        bucket_rank = int(row.get("bucket_rank") or 9999)
        global_rank = int(row.get("global_rank") or 9999)
        keep_bucket_limit = _selection_rank_limit(bucket_target, keep_ratio)
        keep_global_limit = _selection_rank_limit(cutoff, keep_ratio)
        near_bucket_cut = bucket_target > 0 and bucket_rank <= max(keep_bucket_limit, bucket_target + buffer)
        near_global_cut = global_rank <= max(keep_global_limit, cutoff + buffer)
        if near_bucket_cut or near_global_cut:
            selected_symbols.add(symbol)
            selected_paths[symbol] = "buffer_keep"

    rows: list[dict[str, Any]] = []
    for row in working.to_dict(orient="records"):
        symbol = _norm_symbol(row.get("symbol"))
        global_rank = _safe_int(row.get("global_rank"))
        bucket_rank = _safe_int(row.get("bucket_rank"))
        bucket_target = _safe_int(row.get("bucket_target_count"))
        distance_candidates = [global_rank - cutoff]
        if bucket_target > 0:
            distance_candidates.append(bucket_rank - bucket_target)
        distance_to_cut = sorted(distance_candidates, key=lambda value: (abs(value), value))[0]
        current_member = symbol in current_members
        selected = symbol in selected_symbols
        liquidity_gate_pass = bool(row.get("liquidity_gate_pass"))
        special_largecap_flag = bool(row.get("special_largecap"))
        entry_bucket_limit = _selection_rank_limit(bucket_target, entry_ratio)
        keep_bucket_limit = _selection_rank_limit(bucket_target, keep_ratio)
        entry_global_limit = _selection_rank_limit(cutoff, entry_ratio)
        keep_global_limit = _selection_rank_limit(cutoff, keep_ratio)
        near_bucket_cut = bucket_target > 0 and bucket_rank <= max(keep_bucket_limit, bucket_target + buffer)
        near_global_cut = global_rank <= max(keep_global_limit, cutoff + buffer)
        entry_ready = (
            (bucket_target > 0 and bucket_rank <= entry_bucket_limit)
            or global_rank <= entry_global_limit
            or special_largecap_flag
        )
        if not current_member and selected and liquidity_gate_pass and entry_ready:
            state = "likely_add"
        elif not current_member and (selected or near_bucket_cut or near_global_cut):
            state = "watch_add"
        elif current_member and not selected and (
            (bucket_target > 0 and bucket_rank > max(keep_bucket_limit, bucket_target + buffer))
            or global_rank > max(keep_global_limit, cutoff + buffer)
        ):
            state = "likely_drop"
        else:
            state = "stable"
        payload = {
            "symbol": symbol,
            "name": _clean_text(row.get("name")),
            "market": _clean_text(row.get("market")),
            "sector": _clean_text(row.get("sector")) or "미분류",
            "sector_bucket": _clean_text(row.get("sector_bucket")) or "other",
            "index_name": index_name,
            "current_member": current_member,
            "current_member_source": resolved_member_source,
            "predicted_rank": global_rank,
            "bucket_rank": bucket_rank,
            "bucket_target_count": bucket_target,
            "distance_to_cut": distance_to_cut,
            "state": state,
            "confidence": _state_confidence(distance_to_cut, state, resolved_member_source),
            "as_of": _clean_text(row.get("as_of")),
            "market_cap_krw": int(round(_safe_float(row.get("marcap_krw"), 0.0) or 0.0)),
            "avg_amount_60d_krw": int(round(_safe_float(row.get("avg_amount_60d_krw"), 0.0) or 0.0)),
            "marcap_percentile": round(_safe_float(row.get("marcap_percentile"), 0.0) or 0.0, 4),
            "liquidity_percentile": round(_safe_float(row.get("liquidity_percentile"), 0.0) or 0.0, 4),
            "composite_score": round(_safe_float(row.get("composite_score"), 0.0) or 0.0, 4),
            "market_cap_rank": _safe_int(row.get("market_cap_rank")),
            "liquidity_gate_pass": liquidity_gate_pass,
            "entry_rank_limit": min(filter(lambda value: value > 0, [entry_bucket_limit if bucket_target > 0 else 0, entry_global_limit])),
            "keep_rank_limit": min(filter(lambda value: value > 0, [keep_bucket_limit if bucket_target > 0 else 0, keep_global_limit])),
            "special_largecap": special_largecap_flag,
            "rank_method": methodology_version,
        }
        selection_path = selected_paths.get(symbol)
        if selection_path in DOMESTIC_SELECTION_PATHS:
            payload["selection_path"] = selection_path
        rows.append(payload)
    return rows


def build_msci_proxy_rows(
    universe_df: pd.DataFrame,
    *,
    cutoff: int = 100,
    buffer: int = DEFAULT_INDEX_BUFFER,
    market_weight: float = 0.85,
    liquidity_weight: float = 0.15,
) -> list[dict[str, Any]]:
    if universe_df.empty:
        return []
    working = universe_df.copy()
    working["marcap_percentile"] = _percentile_series(working["marcap_krw"])
    working["liquidity_percentile"] = _percentile_series(working["avg_amount_60d_krw"])
    working["composite_score"] = working["marcap_percentile"] * market_weight + working["liquidity_percentile"] * liquidity_weight
    working = working.sort_values(["composite_score", "marcap_krw", "avg_amount_60d_krw"], ascending=[False, False, False]).reset_index(drop=True)
    working["predicted_rank"] = working.index + 1

    rows: list[dict[str, Any]] = []
    for row in working.to_dict(orient="records"):
        rank = _safe_int(row.get("predicted_rank"))
        distance_to_cut = rank - cutoff
        if rank <= cutoff:
            state = "likely_in"
        elif rank <= cutoff + buffer:
            state = "borderline"
        else:
            state = "likely_out"
        rows.append(
            {
                "symbol": _norm_symbol(row.get("symbol")),
                "name": _clean_text(row.get("name")),
                "market": _clean_text(row.get("market")),
                "sector": _clean_text(row.get("sector")) or "미분류",
                "sector_bucket": _clean_text(row.get("sector_bucket")) or "other",
                "index_name": "MSCI Proxy",
                "current_member": None,
                "current_member_source": "proxy_only",
                "predicted_rank": rank,
                "bucket_rank": _safe_int(row.get("bucket_rank")),
                "bucket_target_count": _safe_int(row.get("bucket_target_count")),
                "distance_to_cut": distance_to_cut,
                "state": state,
                "confidence": _state_confidence(distance_to_cut, state, "proxy_only"),
                "as_of": _clean_text(row.get("as_of")),
                "market_cap_krw": int(round(_safe_float(row.get("marcap_krw"), 0.0) or 0.0)),
                "avg_amount_60d_krw": int(round(_safe_float(row.get("avg_amount_60d_krw"), 0.0) or 0.0)),
                "marcap_percentile": round(_safe_float(row.get("marcap_percentile"), 0.0) or 0.0, 4),
                "liquidity_percentile": round(_safe_float(row.get("liquidity_percentile"), 0.0) or 0.0, 4),
                "composite_score": round(_safe_float(row.get("composite_score"), 0.0) or 0.0, 4),
                "rank_method": "msci_proxy_v1",
            }
        )
    return rows


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
    payload["stale_since"] = _clean_text(payload.get("stale_since")) or now_iso
    payload["source_error"] = str(error)[:300]
    payload["snapshot_name"] = name
    payload["as_of"] = _clean_text(payload.get("as_of")) or now_iso[:10]
    return payload


def _index_state_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        state = _clean_text(row.get("state")) or "unknown"
        counts[state] = counts.get(state, 0) + 1
    return counts


def _enrich_candidate_frame(
    pool_df: pd.DataFrame,
    amount_metrics: dict[str, dict[str, Any]],
    *,
    sector_map: dict[str, str],
    float_profiles: dict[str, dict[str, Any]] | None = None,
    market_cap_metrics: dict[str, dict[str, Any]] | None = None,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in pool_df.to_dict(orient="records"):
        symbol = _norm_symbol(row.get("Code"))
        metrics = amount_metrics.get(symbol, {})
        float_profile = (float_profiles or {}).get(symbol, {})
        market_cap_metric = (market_cap_metrics or {}).get(symbol, {})
        avg_amount = _safe_float(metrics.get("avg_amount_60d_krw"))
        fallback_amount = _safe_float(row.get("Amount"), 0.0) or 0.0
        sector = sector_map.get(symbol, "")
        free_float_ratio = _safe_ratio(float_profile.get("free_float_ratio"), 0.0)
        listed_shares = _safe_int(float_profile.get("listed_common_shares") or row.get("Stocks"))
        current_marcap = _safe_float(row.get("Marcap"), 0.0) or 0.0
        avg_market_cap_1y_krw = _safe_float(market_cap_metric.get("avg_market_cap_1y_krw"), 0.0) or 0.0
        ffmc_proxy_krw = current_marcap * free_float_ratio if free_float_ratio > 0 else 0.0
        avg_ffmc_1y_krw = avg_market_cap_1y_krw * free_float_ratio if free_float_ratio > 0 and avg_market_cap_1y_krw > 0 else 0.0
        size_proxy_krw = avg_ffmc_1y_krw or ffmc_proxy_krw or current_marcap
        records.append(
            {
                "symbol": symbol,
                "name": _clean_text(row.get("Name")),
                "market": _normalize_market(row.get("Market")),
                "sector": sector or "미분류",
                "sector_bucket": _sector_bucket_from_sector(sector),
                "marcap_krw": current_marcap,
                "avg_amount_60d_krw": avg_amount if avg_amount and avg_amount > 0 else fallback_amount,
                "daily_amount_krw": fallback_amount,
                "listed_shares": listed_shares,
                "free_float_ratio": free_float_ratio if free_float_ratio > 0 else None,
                "float_shares": _safe_int(float_profile.get("float_shares")),
                "float_profile_source": _clean_text(float_profile.get("source")) or "",
                "major_holder_ratio": _safe_float(float_profile.get("major_holder_ratio"), 0.0) or 0.0,
                "treasury_ratio": _safe_float(float_profile.get("treasury_ratio"), 0.0) or 0.0,
                "employee_ratio": _safe_float(float_profile.get("employee_ratio"), 0.0) or 0.0,
                "avg_market_cap_1y_krw": avg_market_cap_1y_krw,
                "ffmc_proxy_krw": ffmc_proxy_krw,
                "avg_ffmc_1y_krw": avg_ffmc_1y_krw,
                "size_proxy_krw": size_proxy_krw,
                "market_cap_history_source": _clean_text(market_cap_metric.get("market_cap_history_source")) or "",
                "history_source": _clean_text(metrics.get("history_source")) or "current_day_fallback",
                "as_of": _clean_text(market_cap_metric.get("as_of")) or _clean_text(metrics.get("as_of")),
            }
        )
    return pd.DataFrame(records)


def _select_history_fetch_symbols(listing_df: pd.DataFrame) -> list[str]:
    symbols: set[str] = set()
    configs = list(DOMESTIC_INDEX_CONFIGS) + [MSCI_PROXY_CONFIG]
    for config in configs:
        pool_df = _select_candidate_pool(listing_df, config["market"], config["cutoff"])
        if pool_df.empty:
            continue
        working = pool_df.copy()
        working["marcap_percentile"] = _percentile_series(pd.to_numeric(working["Marcap"], errors="coerce"))
        working["liquidity_percentile"] = _percentile_series(pd.to_numeric(working["Amount"], errors="coerce"))
        working["composite_score"] = working["marcap_percentile"] * float(config["market_weight"]) + working["liquidity_percentile"] * float(config["liquidity_weight"])
        working = working.sort_values(["composite_score", "Marcap", "Amount"], ascending=[False, False, False]).reset_index(drop=True)
        shortlist_size = int(config["cutoff"]) + int(config.get("buffer", DEFAULT_INDEX_BUFFER)) + DEFAULT_HISTORY_MARGIN
        for symbol in working.head(shortlist_size)["Code"].tolist():
            normalized = _norm_symbol(symbol)
            if normalized:
                symbols.add(normalized)
    return sorted(symbols)


def build_index_rebalance_snapshot(previous_snapshot: dict[str, Any] | None, *, worker_count: int) -> dict[str, Any]:
    listing_df = _load_listing_frame()
    sector_map = _load_sector_reference_map()
    history_symbols = _select_history_fetch_symbols(listing_df)
    amount_metrics = _load_amount_metrics(history_symbols, worker_count=worker_count)

    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    for config in DOMESTIC_INDEX_CONFIGS:
        pool_df = _select_candidate_pool(listing_df, config["market"], config["cutoff"])
        enriched = _enrich_candidate_frame(pool_df, amount_metrics, sector_map=sector_map)
        previous_member_rows = _current_member_rows(previous_snapshot, config["index_name"])
        previous_members = {_norm_symbol(row.get("symbol")) for row in previous_member_rows if _norm_symbol(row.get("symbol"))}
        member_source = "previous_snapshot_proxy" if previous_members else "bootstrap_topcut_proxy"
        bucket_targets, target_source = _derive_bucket_targets(
            enriched,
            index_name=config["index_name"],
            cutoff=config["cutoff"],
            previous_rows=previous_member_rows,
            sector_map=sector_map,
        )
        index_rows = build_domestic_index_rows(
            enriched,
            index_name=config["index_name"],
            cutoff=config["cutoff"],
            member_symbols=previous_members,
            member_source=member_source,
            bucket_targets=bucket_targets,
            buffer=config["buffer"],
            market_weight=config["market_weight"],
            liquidity_weight=config["liquidity_weight"],
            entry_ratio=config.get("entry_ratio", DEFAULT_DOMESTIC_ENTRY_RATIO),
            keep_ratio=config.get("keep_ratio", DEFAULT_DOMESTIC_KEEP_RATIO),
            liquidity_coverage=config.get("liquidity_coverage", DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE),
            special_largecap_rank=config.get("special_largecap_rank", DEFAULT_SPECIAL_LARGECAP_RANK),
        )
        rows.extend(index_rows)
        liquidity_gate_fail_count = sum(1 for row in index_rows if not bool(row.get("liquidity_gate_pass")))
        tech_core_count = sum(1 for row in index_rows if _kq150_broad_bucket(_clean_text(row.get("sector_bucket"))) == "technology_core")
        summaries.append(
            {
                "index_name": config["index_name"],
                "cutoff": config["cutoff"],
                "member_source": member_source,
                "target_source": target_source,
                "row_count": len(index_rows),
                "state_counts": _index_state_counts(index_rows),
                "bucket_counts": {
                    bucket: int(count)
                    for bucket, count in enriched["sector_bucket"].fillna("other").astype(str).value_counts().sort_index().items()
                },
                "bucket_targets": dict(sorted(bucket_targets.items())),
                "methodology_version": DOMESTIC_METHODOLOGY_VERSION,
                "quota_strategy": config.get("quota_strategy", "sector_bucket"),
                "entry_ratio": config.get("entry_ratio", DEFAULT_DOMESTIC_ENTRY_RATIO),
                "keep_ratio": config.get("keep_ratio", DEFAULT_DOMESTIC_KEEP_RATIO),
                "liquidity_coverage": config.get("liquidity_coverage", DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE),
                "liquidity_gate_fail_count": liquidity_gate_fail_count,
                "broad_bucket_counts": {
                    "technology_core": tech_core_count,
                    "non_technology": max(0, len(index_rows) - tech_core_count),
                },
            }
        )

    msci_enriched = _enrich_candidate_frame(
        _select_candidate_pool(listing_df, MSCI_PROXY_CONFIG["market"], MSCI_PROXY_CONFIG["cutoff"]),
        amount_metrics,
        sector_map=sector_map,
    )
    msci_rows = build_msci_proxy_rows(
        msci_enriched,
        cutoff=MSCI_PROXY_CONFIG["cutoff"],
        buffer=MSCI_PROXY_CONFIG["buffer"],
        market_weight=MSCI_PROXY_CONFIG["market_weight"],
        liquidity_weight=MSCI_PROXY_CONFIG["liquidity_weight"],
    )
    rows.extend(msci_rows)
    summaries.append(
        {
            "index_name": MSCI_PROXY_CONFIG["index_name"],
            "cutoff": MSCI_PROXY_CONFIG["cutoff"],
            "member_source": "proxy_only",
            "row_count": len(msci_rows),
            "state_counts": _index_state_counts(msci_rows),
        }
    )

    rows.sort(key=lambda item: (_clean_text(item.get("index_name")), _safe_int(item.get("predicted_rank"))))
    return {
        "generated_at": _now_iso(),
        "as_of": _resolve_index_snapshot_as_of(amount_metrics),
        "status": "live",
        "stale_since": None,
        "source_error": "",
        "default_buffer": DEFAULT_INDEX_BUFFER,
        "rows": rows,
        "indexes": summaries,
    }


def build_public_faithful_index_rebalance_snapshot(previous_snapshot: dict[str, Any] | None, *, worker_count: int) -> dict[str, Any]:
    listing_df = _load_listing_frame()
    sector_map = _load_sector_reference_map()
    history_symbols = _select_history_fetch_symbols(listing_df)
    amount_metrics = _load_amount_metrics(history_symbols, worker_count=worker_count)
    domestic_markets = {config["market"] for config in DOMESTIC_INDEX_CONFIGS}
    market_by_symbol = {
        _norm_symbol(row.get("Code")): _normalize_market(row.get("Market"))
        for row in listing_df.to_dict(orient="records")
        if _norm_symbol(row.get("Code"))
    }
    domestic_symbols = sorted(
        {
            _norm_symbol(symbol)
            for symbol in history_symbols
            if _norm_symbol(symbol)
            and market_by_symbol.get(_norm_symbol(symbol)) in domestic_markets
        }
    )
    float_profiles = _load_public_float_profiles(domestic_symbols, worker_count=worker_count)
    shares_by_symbol = {
        _norm_symbol(row.get("Code")): _safe_int((float_profiles.get(_norm_symbol(row.get("Code")), {}) or {}).get("listed_common_shares") or row.get("Stocks"))
        for row in listing_df.to_dict(orient="records")
        if _norm_symbol(row.get("Code"))
    }
    special_event_map = _build_special_change_event_map(listing_df)
    market_cap_metrics = _load_average_market_cap_metrics(
        {symbol: shares_by_symbol.get(symbol, 0) for symbol in domestic_symbols},
        worker_count=worker_count,
    )

    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for config in DOMESTIC_INDEX_CONFIGS:
        pool_df = _select_candidate_pool(listing_df, config["market"], config["cutoff"])
        enriched = _enrich_candidate_frame(
            pool_df,
            amount_metrics,
            sector_map=sector_map,
            float_profiles=float_profiles,
            market_cap_metrics=market_cap_metrics,
        )
        previous_member_rows = _current_member_rows(previous_snapshot, config["index_name"])
        previous_members = {_norm_symbol(row.get("symbol")) for row in previous_member_rows if _norm_symbol(row.get("symbol"))}
        index_rows, meta = build_public_faithful_index_rows(
            enriched,
            index_name=config["index_name"],
            cutoff=config["cutoff"],
            member_symbols=previous_members,
            member_source="previous_snapshot_proxy" if previous_members else "bootstrap_topcut_proxy",
            special_event_map=special_event_map,
        )
        rows.extend(index_rows)
        summaries.append(
            {
                "index_name": config["index_name"],
                "cutoff": config["cutoff"],
                "member_source": "previous_snapshot_proxy" if previous_members else "bootstrap_topcut_proxy",
                "target_source": meta.get("target_source", "public_faithful"),
                "row_count": len(index_rows),
                "state_counts": _index_state_counts(index_rows),
                "bucket_counts": {
                    bucket: int(count)
                    for bucket, count in enriched["sector_bucket"].fillna("other").astype(str).value_counts().sort_index().items()
                },
                "bucket_targets": dict(sorted((meta.get("bucket_targets") or {}).items())),
                "methodology_version": PUBLIC_FAITHFUL_METHODOLOGY_VERSION,
                "quota_strategy": "official_public_rules",
                "entry_ratio": DEFAULT_DOMESTIC_ENTRY_RATIO,
                "keep_ratio": DEFAULT_DOMESTIC_KEEP_RATIO,
                "liquidity_coverage": DEFAULT_DOMESTIC_LIQUIDITY_COVERAGE,
                "source_mode": "public",
                "float_profile_count": sum(1 for row in index_rows if (_safe_float(row.get("free_float_ratio"), 0.0) or 0.0) > 0),
                "reserve_candidates": meta.get("reserve_candidates", []),
                "special_largecap_candidates": meta.get("special_largecap_candidates", []),
                "special_change_candidates": meta.get("special_change_candidates", []),
            }
        )

    msci_enriched = _enrich_candidate_frame(
        _select_candidate_pool(listing_df, MSCI_PROXY_CONFIG["market"], MSCI_PROXY_CONFIG["cutoff"]),
        amount_metrics,
        sector_map=sector_map,
    )
    msci_rows = build_msci_proxy_rows(
        msci_enriched,
        cutoff=MSCI_PROXY_CONFIG["cutoff"],
        buffer=MSCI_PROXY_CONFIG["buffer"],
        market_weight=MSCI_PROXY_CONFIG["market_weight"],
        liquidity_weight=MSCI_PROXY_CONFIG["liquidity_weight"],
    )
    rows.extend(msci_rows)
    summaries.append(
        {
            "index_name": MSCI_PROXY_CONFIG["index_name"],
            "cutoff": MSCI_PROXY_CONFIG["cutoff"],
            "member_source": "proxy_only",
            "row_count": len(msci_rows),
            "state_counts": _index_state_counts(msci_rows),
            "source_mode": "proxy",
        }
    )
    rows.sort(key=lambda item: (_clean_text(item.get("index_name")), _safe_int(item.get("predicted_rank"))))
    return {
        "generated_at": _now_iso(),
        "as_of": _resolve_index_snapshot_as_of(amount_metrics),
        "status": "live",
        "stale_since": None,
        "source_error": "",
        "default_buffer": DEFAULT_INDEX_BUFFER,
        "methodology_mode": "public-faithful",
        "rows": rows,
        "indexes": summaries,
    }


def build_etf_gap_snapshot() -> dict[str, Any]:
    if fdr is None:
        raise RuntimeError("FinanceDataReader가 없어 ETF 괴리 스냅샷을 만들 수 없습니다.")
    listing_df = fdr.StockListing("ETF/KR")
    if listing_df is None or listing_df.empty:
        raise RuntimeError("ETF/KR listing을 불러오지 못했습니다.")
    return _build_etf_gap_snapshot_from_listing_impl(
        listing_df,
        captured_at=_now_iso(),
        normalize_symbol=_norm_symbol,
        safe_float=_safe_float,
        gap_threshold_pct=DEFAULT_ETF_GAP_THRESHOLD_PCT,
        top_count=DEFAULT_TOP30_COUNT,
        excluded_keywords=PASSIVE_ETF_EXCLUDED_KEYWORDS,
        domestic_region_keywords=DOMESTIC_REGION_KEYWORDS,
        foreign_region_keywords=FOREIGN_REGION_KEYWORDS,
    )


def _load_previous_snapshot(name: str) -> dict[str, Any] | None:
    target = WEB_DIR / f"{name}.json"
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _save_snapshot(name: str, payload: dict[str, Any]) -> str:
    ensure_runtime_dir()
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    target = WEB_DIR / f"{name}.json"
    target.write_text(serialized, encoding="utf-8")
    for mirror_dir in ADDITIONAL_WEB_DIRS:
        if not mirror_dir.parent.exists():
            continue
        mirror_dir.mkdir(parents=True, exist_ok=True)
        (mirror_dir / f"{name}.json").write_text(serialized, encoding="utf-8")
    return str(target)


def build_snapshots(*, worker_count: int, methodology_mode: str = DEFAULT_INDEX_METHODOLOGY_MODE) -> dict[str, dict[str, Any]]:
    now_iso = _now_iso()
    snapshots: dict[str, dict[str, Any]] = {}
    previous_index = _load_previous_snapshot(INDEX_SNAPSHOT_NAME)
    previous_etf = _load_previous_snapshot(ETF_SNAPSHOT_NAME)

    try:
        if methodology_mode == "official":
            bundle = load_official_clone_bundle(OFFICIAL_INDEX_INPUT_DIR)
            snapshots[INDEX_SNAPSHOT_NAME] = build_official_index_rebalance_snapshot(bundle)
        elif methodology_mode == "public-faithful":
            snapshots[INDEX_SNAPSHOT_NAME] = build_public_faithful_index_rebalance_snapshot(previous_index, worker_count=worker_count)
        else:
            snapshots[INDEX_SNAPSHOT_NAME] = build_index_rebalance_snapshot(previous_index, worker_count=worker_count)
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        log.exception("index rebalance snapshot build failed: %s", exc)
        snapshots[INDEX_SNAPSHOT_NAME] = build_stale_snapshot(
            INDEX_SNAPSHOT_NAME,
            previous_index,
            now_iso,
            exc,
            defaults={
                "default_buffer": DEFAULT_INDEX_BUFFER,
                "indexes": [],
                "methodology_mode": methodology_mode,
            },
        )

    try:
        snapshots[ETF_SNAPSHOT_NAME] = build_etf_gap_snapshot()
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        log.exception("etf gap snapshot build failed: %s", exc)
        snapshots[ETF_SNAPSHOT_NAME] = build_stale_snapshot(
            ETF_SNAPSHOT_NAME,
            previous_etf,
            now_iso,
            exc,
            defaults={
                "default_aum_top_n": DEFAULT_TOP30_COUNT,
                "default_gap_threshold_pct": DEFAULT_ETF_GAP_THRESHOLD_PCT,
                "summary": {
                    "eligible_count": 0,
                    "top30_count": 0,
                    "anomaly_count": 0,
                    "provider_counts": {},
                },
            },
        )
    return snapshots


def publish_snapshots(snapshots: dict[str, dict[str, Any]], *, skip_mongo: bool = False) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    files: dict[str, str] = {}
    store = None if skip_mongo else MongoReadModelStore(SETTINGS.MONGO_URI, SETTINGS.DB_NAME)
    for name, payload in snapshots.items():
        files[name] = _save_snapshot(name, payload)
        if store is None:
            continue
        results.append(store.replace_singleton(name, payload, singleton_id="latest"))
    return {"files": files, "collections": results, "mongo_available": bool(store and store.available)}


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def run_once(args: argparse.Namespace) -> None:
    snapshots = build_snapshots(worker_count=args.fetch_workers, methodology_mode=args.methodology_mode)
    result = publish_snapshots(snapshots, skip_mongo=args.skip_mongo)
    if args.print_only:
        print("[passive-monitor] build complete")
        print(f"- mongo available: {result['mongo_available']}")
        for name, payload in snapshots.items():
            print(f"- {name}: status={payload.get('status')} rows={len(payload.get('rows') or [])}")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = ""
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            run_once(args)
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        run_once(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
