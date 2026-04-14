from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

try:
    import requests
except Exception:  # pragma: no cover - runtime dependency
    requests = None

try:
    from pykrx import stock as pykrx_stock
except Exception:  # pragma: no cover - runtime dependency
    pykrx_stock = None

try:
    from config import SETTINGS
    from market_warning_candidates import (
        build_market_warning_candidate_snapshot as _build_market_warning_candidate_snapshot_impl,
        evaluate_close_swing_candidate as _evaluate_close_swing_candidate_impl,
        evaluate_risk_designation as _evaluate_risk_designation_impl,
        evaluate_risk_halt_candidates as _evaluate_risk_halt_candidates_impl,
        evaluate_risk_pre_notice as _evaluate_risk_pre_notice_impl,
        evaluate_warning_designation as _evaluate_warning_designation_impl,
        evaluate_warning_halt_candidate as _evaluate_warning_halt_candidate_impl,
        evaluate_warning_pre_notice as _evaluate_warning_pre_notice_impl,
        evaluate_warning_redesignation as _evaluate_warning_redesignation_impl,
    )
    from market_warning_official import (
        build_market_warning_official_snapshot as _build_market_warning_official_snapshot_impl,
        build_official_state_map as _build_official_state_map_impl,
        build_event_halt_rows as _build_event_halt_rows_impl,
        classify_caution_notice_type as _classify_caution_notice_type_impl,
        dedupe_official_rows as _dedupe_official_rows_impl,
        parse_kind_trading_halt_html as _parse_kind_trading_halt_html_impl,
        parse_kind_warning_html as _parse_kind_warning_html_impl,
        snapshot_state_counts as _snapshot_state_counts_impl,
    )
    from market_warning_support import (
        build_name_lookup as _build_name_lookup_impl,
        build_stale_snapshot as _build_stale_snapshot_impl,
        clean_text as _clean_text_impl,
        date_text as _date_text_impl,
        load_listing_frame as _load_listing_frame_impl,
        load_projection_file as _load_projection_file_impl,
        load_stock_master_map as _load_stock_master_map_impl,
        market_group as _market_group_impl,
        next_business_day as _next_business_day_impl,
        norm_symbol as _norm_symbol_impl,
        normalize_market as _normalize_market_impl,
        parse_iso_date as _parse_iso_date_impl,
        resolve_symbol_by_name as _resolve_symbol_by_name_impl,
        safe_float as _safe_float_impl,
        save_snapshot as _save_snapshot_impl,
        slug_text as _slug_text_impl,
        is_warning_eligible_name as _is_warning_eligible_name_impl,
    )
    from mongo_read_models import MongoReadModelStore
    from runtime_paths import RUNTIME_DIR, ensure_runtime_dir
except Exception:  # pragma: no cover - package import fallback
    from Disclosure.config import SETTINGS
    from Disclosure.market_warning_candidates import (
        build_market_warning_candidate_snapshot as _build_market_warning_candidate_snapshot_impl,
        evaluate_close_swing_candidate as _evaluate_close_swing_candidate_impl,
        evaluate_risk_designation as _evaluate_risk_designation_impl,
        evaluate_risk_halt_candidates as _evaluate_risk_halt_candidates_impl,
        evaluate_risk_pre_notice as _evaluate_risk_pre_notice_impl,
        evaluate_warning_designation as _evaluate_warning_designation_impl,
        evaluate_warning_halt_candidate as _evaluate_warning_halt_candidate_impl,
        evaluate_warning_pre_notice as _evaluate_warning_pre_notice_impl,
        evaluate_warning_redesignation as _evaluate_warning_redesignation_impl,
    )
    from Disclosure.market_warning_official import (
        build_market_warning_official_snapshot as _build_market_warning_official_snapshot_impl,
        build_official_state_map as _build_official_state_map_impl,
        build_event_halt_rows as _build_event_halt_rows_impl,
        classify_caution_notice_type as _classify_caution_notice_type_impl,
        dedupe_official_rows as _dedupe_official_rows_impl,
        parse_kind_trading_halt_html as _parse_kind_trading_halt_html_impl,
        parse_kind_warning_html as _parse_kind_warning_html_impl,
        snapshot_state_counts as _snapshot_state_counts_impl,
    )
    from Disclosure.market_warning_support import (
        build_name_lookup as _build_name_lookup_impl,
        build_stale_snapshot as _build_stale_snapshot_impl,
        clean_text as _clean_text_impl,
        date_text as _date_text_impl,
        load_listing_frame as _load_listing_frame_impl,
        load_projection_file as _load_projection_file_impl,
        load_stock_master_map as _load_stock_master_map_impl,
        market_group as _market_group_impl,
        next_business_day as _next_business_day_impl,
        norm_symbol as _norm_symbol_impl,
        normalize_market as _normalize_market_impl,
        parse_iso_date as _parse_iso_date_impl,
        resolve_symbol_by_name as _resolve_symbol_by_name_impl,
        safe_float as _safe_float_impl,
        save_snapshot as _save_snapshot_impl,
        slug_text as _slug_text_impl,
        is_warning_eligible_name as _is_warning_eligible_name_impl,
    )
    from Disclosure.mongo_read_models import MongoReadModelStore
    from Disclosure.runtime_paths import RUNTIME_DIR, ensure_runtime_dir

try:
    from naver_intraday_fallback import fetch_naver_intraday_history
    from naver_price_fallback import fetch_naver_daily_price_history
    from price_history_loader import load_price_history
except Exception:  # pragma: no cover - package import fallback
    from Disclosure.naver_intraday_fallback import fetch_naver_intraday_history
    from Disclosure.naver_price_fallback import fetch_naver_daily_price_history
    from Disclosure.price_history_loader import load_price_history


log = logging.getLogger("disclosure.market_warning_monitor")
logging.raiseExceptions = False

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

OFFICIAL_SNAPSHOT_NAME = "market_warning_official_latest"
CANDIDATE_SNAPSHOT_NAME = "market_warning_candidates_latest"
STOCK_MASTER_NAME = "stock_master"
EVENT_SNAPSHOT_NAME = "event_calendar_latest"
DEFAULT_HTTP_TIMEOUT_SEC = float(os.getenv("MARKET_WARNING_HTTP_TIMEOUT_SEC", "10.0"))
DEFAULT_LOOKBACK_DAYS = 120
DEFAULT_HISTORY_SESSIONS = 35
DEFAULT_SUMMARY_DAYS = 30
DEFAULT_HISTORY_WORKERS = int(os.getenv("MARKET_WARNING_HISTORY_WORKERS", "12"))
CURRENT_STATE_KEYS = (
    "none",
    "warning_pre_notice",
    "warning_active",
    "risk_pre_notice",
    "risk_active",
    "halt_pre_notice",
    "halt_active",
)
EXCLUDED_NAME_KEYWORDS = ("ETF", "ETN", "리츠", "스팩", "SPAC")
MARKET_ALT_MAP = {
    "유가증권": "KOSPI",
    "코스닥": "KOSDAQ",
    "코넥스": "KONEX",
    "배출권": "ETS",
}
GENERAL_HALT_MATCHERS = ("매매거래정지", "매매거래정지해제", "정지기간변경", "정지예정", "정지예고")
KIND_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/147.0.0.0 Safari/537.36",
}
KIND_AJAX_HEADERS = {
    **KIND_HEADERS,
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "text/html, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}
KIND_WARN_MAIN_URL = "https://kind.krx.co.kr/investwarn/investattentwarnrisky.do?method=investattentwarnriskyMain"
KIND_WARN_POST_URL = "https://kind.krx.co.kr/investwarn/investattentwarnrisky.do"
KIND_TRADING_HALT_MAIN_URL = "https://kind.krx.co.kr/investwarn/tradinghaltissue.do?method=searchTradingHaltIssueMain"
KIND_TRADING_HALT_POST_URL = "https://kind.krx.co.kr/investwarn/tradinghaltissue.do"
KIND_WARNING_MENUS = (
    {"menu_index": "1", "forward": "invstcautnisu_sub", "order_mode": "4", "kind": "attention"},
    {"menu_index": "2", "forward": "invstwarnisu_sub", "order_mode": "3", "kind": "warning"},
    {"menu_index": "3", "forward": "invstriskisu_sub", "order_mode": "3", "kind": "risk"},
)
TRADING_HALT_MARKETS = (
    ("1", "KOSPI"),
    ("2", "KOSDAQ"),
    ("6", "KONEX"),
)
CAUTION_REASON_MAP = {
    "종가급변": ("attention", "design", "종가급변"),
    "투자경고 지정예고": ("warning", "pre_notice", "투자경고 지정예고"),
    "투자경고 지정해제": ("warning", "release", "투자경고 지정해제"),
    "투자위험 지정예고": ("risk", "pre_notice", "투자위험 지정예고"),
    "투자위험 지정해제": ("risk", "release", "투자위험 지정해제"),
    "매매거래정지 예고": ("trading_halt", "pre_notice", "매매거래정지 예고"),
    "매매거래정지": ("trading_halt", "halt", "매매거래정지"),
}
ACTION_PRIORITY = {
    "pre_notice": 1,
    "design": 2,
    "halt": 3,
    "resume": 4,
    "release": 5,
}
CURRENT_STATE_PRIORITY = {
    "none": 0,
    "warning_pre_notice": 1,
    "warning_active": 2,
    "risk_pre_notice": 3,
    "risk_active": 4,
    "halt_pre_notice": 5,
    "halt_active": 6,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build market warning monitor snapshots.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print a short build summary.")
    parser.add_argument("--skip-mongo", action="store_true", help="Only write local projection files.")
    parser.add_argument("--times", default="07:10,20:30", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="Official history lookback days.")
    return parser.parse_args()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clean_text(value: Any) -> str:
    return _clean_text_impl(value)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    return _safe_float_impl(value, default)


def _norm_symbol(value: Any) -> str:
    return _norm_symbol_impl(value)


def _slug_text(value: Any) -> str:
    return _slug_text_impl(value)


def _normalize_market(value: Any) -> str:
    return _normalize_market_impl(value, market_alt_map=MARKET_ALT_MAP)


def _market_group(market: str) -> str:
    return _market_group_impl(market, market_alt_map=MARKET_ALT_MAP)


def _parse_iso_date(value: Any) -> date | None:
    return _parse_iso_date_impl(value)


def _date_text(value: Any) -> str:
    return _date_text_impl(value)


def _next_business_day(value: str) -> str:
    return _next_business_day_impl(value)


def _is_warning_eligible_name(name: str) -> bool:
    return _is_warning_eligible_name_impl(name, excluded_keywords=EXCLUDED_NAME_KEYWORDS)


def _load_projection_file(name: str) -> Any:
    return _load_projection_file_impl(name, directories=(WEB_DIR, *ADDITIONAL_WEB_DIRS))


def _load_stock_master_map() -> dict[str, dict[str, Any]]:
    return _load_stock_master_map_impl(
        payload=_load_projection_file(STOCK_MASTER_NAME),
        listing_path=LISTING_PATH,
        excluded_keywords=EXCLUDED_NAME_KEYWORDS,
        market_alt_map=MARKET_ALT_MAP,
    )


def _load_listing_frame(stock_master: dict[str, dict[str, Any]]) -> pd.DataFrame:
    return _load_listing_frame_impl(stock_master, listing_path=LISTING_PATH, market_alt_map=MARKET_ALT_MAP)


def _build_name_lookup(stock_master: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return _build_name_lookup_impl(stock_master)


def _resolve_symbol_by_name(
    name: str,
    market: str,
    *,
    name_lookup: dict[str, list[dict[str, Any]]],
) -> tuple[str, str]:
    return _resolve_symbol_by_name_impl(name, market, name_lookup=name_lookup, market_alt_map=MARKET_ALT_MAP)


def build_stale_snapshot(
    name: str,
    previous_snapshot: dict[str, Any] | None,
    now_iso: str,
    error: Exception,
    *,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _build_stale_snapshot_impl(name, previous_snapshot, now_iso, error, defaults=defaults)


def _load_previous_snapshot(name: str) -> dict[str, Any] | None:
    payload = _load_projection_file(name)
    return payload if isinstance(payload, dict) else None


def _save_snapshot(name: str, payload: dict[str, Any]) -> str:
    return _save_snapshot_impl(name, payload, web_dir=WEB_DIR, additional_web_dirs=ADDITIONAL_WEB_DIRS, ensure_runtime_dir=ensure_runtime_dir)


def _kind_session() -> requests.Session:
    if requests is None:
        raise RuntimeError("requests가 없어 KIND 데이터를 조회할 수 없습니다.")
    session = requests.Session()
    session.headers.update(KIND_HEADERS)
    return session


def _post_kind_warn_html(
    session: requests.Session,
    *,
    menu_index: str,
    forward: str,
    order_mode: str,
    start_date: str,
    end_date: str,
) -> str:
    session.get(KIND_WARN_MAIN_URL, timeout=DEFAULT_HTTP_TIMEOUT_SEC)
    payload = {
        "method": "investattentwarnriskySub",
        "currentPageSize": "3000",
        "pageIndex": "1",
        "orderMode": order_mode,
        "orderStat": "D",
        "searchCodeType": "",
        "searchCorpName": "",
        "repIsuSrtCd": "",
        "menuIndex": menu_index,
        "forward": forward,
        "searchFromDate": end_date,
        "marketType": "",
        "searchCorpNameTmp": "",
        "etsIsuSrtCd": "",
        "startDate": start_date,
        "endDate": end_date,
    }
    response = session.post(
        KIND_WARN_POST_URL,
        data=payload,
        headers={**KIND_AJAX_HEADERS, "Referer": KIND_WARN_MAIN_URL},
        timeout=DEFAULT_HTTP_TIMEOUT_SEC,
    )
    response.raise_for_status()
    return response.text


def _post_kind_trading_halt_html(
    session: requests.Session,
    *,
    market_type: str,
) -> str:
    session.get(KIND_TRADING_HALT_MAIN_URL, timeout=DEFAULT_HTTP_TIMEOUT_SEC)
    payload = {
        "method": "searchTradingHaltIssueSub",
        "currentPageSize": "",
        "pageIndex": "1",
        "searchMode": "",
        "searchCodeType": "",
        "searchCorpName": "",
        "forward": "tradinghaltissue_sub",
        "paxreq": "",
        "outsvcno": "",
        "marketType": market_type,
        "repIsuSrtCd": "",
    }
    response = session.post(
        KIND_TRADING_HALT_POST_URL,
        data=payload,
        headers={**KIND_AJAX_HEADERS, "Referer": KIND_TRADING_HALT_MAIN_URL},
        timeout=DEFAULT_HTTP_TIMEOUT_SEC,
    )
    response.raise_for_status()
    return response.text


def classify_caution_notice_type(raw_type: str) -> tuple[str, str, str]:
    return _classify_caution_notice_type_impl(raw_type, caution_reason_map=CAUTION_REASON_MAP, clean_text=_clean_text)


def _parse_market_from_row(tr: Any) -> str:
    image = tr.select_one("img.legend")
    return MARKET_ALT_MAP.get(_clean_text(image.get("alt")) if image else "", "")


def _parse_kind_warning_html(
    html: str,
    *,
    menu_kind: str,
    name_lookup: dict[str, list[dict[str, Any]]],
    as_of: str,
) -> list[dict[str, Any]]:
    return _parse_kind_warning_html_impl(
        html,
        menu_kind=menu_kind,
        name_lookup=name_lookup,
        as_of=as_of,
        kind_warn_main_url=KIND_WARN_MAIN_URL,
        caution_reason_map=CAUTION_REASON_MAP,
        market_alt_map=MARKET_ALT_MAP,
        resolve_symbol_by_name=_resolve_symbol_by_name,
        clean_text=_clean_text,
        date_text=_date_text,
        next_business_day=_next_business_day,
    )


def _parse_kind_trading_halt_html(
    html: str,
    *,
    market: str,
) -> tuple[str, list[dict[str, Any]]]:
    return _parse_kind_trading_halt_html_impl(html, market=market, clean_text=_clean_text)


def _load_event_calendar_rows() -> list[dict[str, Any]]:
    payload = _load_projection_file(EVENT_SNAPSHOT_NAME)
    return payload if isinstance(payload, list) else []


def _extract_reason_group(text: str) -> str:
    cleaned = _clean_text(text)
    paren = re.search(r"\(([^)]+)\)", cleaned)
    if paren:
        return _clean_text(paren.group(1))
    return cleaned[:80]


def _build_event_halt_rows(
    events: list[dict[str, Any]],
    *,
    cutoff_date: str,
) -> list[dict[str, Any]]:
    return _build_event_halt_rows_impl(
        events,
        cutoff_date=cutoff_date,
        general_halt_matchers=GENERAL_HALT_MATCHERS,
        norm_symbol=_norm_symbol,
        normalize_market=_normalize_market,
        clean_text=_clean_text,
        parse_iso_date=_parse_iso_date,
        date_text=_date_text,
    )


def _dedupe_official_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _dedupe_official_rows_impl(rows, norm_symbol=_norm_symbol, clean_text=_clean_text, date_text=_date_text, action_priority=ACTION_PRIORITY)


def build_official_state_map(rows: list[dict[str, Any]], *, as_of: str | None = None) -> dict[str, dict[str, Any]]:
    return _build_official_state_map_impl(
        rows,
        as_of=as_of,
        norm_symbol=_norm_symbol,
        clean_text=_clean_text,
        normalize_market=_normalize_market,
        parse_iso_date=_parse_iso_date,
        date_text=_date_text,
        action_priority=ACTION_PRIORITY,
        current_state_keys=CURRENT_STATE_KEYS,
    )


def _snapshot_state_counts(state_map: dict[str, dict[str, Any]]) -> dict[str, int]:
    return _snapshot_state_counts_impl(state_map, clean_text=_clean_text, current_state_keys=CURRENT_STATE_KEYS)


def build_market_warning_official_snapshot(
    *,
    stock_master: dict[str, dict[str, Any]],
    as_of: str,
    lookback_days: int,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    return _build_market_warning_official_snapshot_impl(
        stock_master=stock_master,
        as_of=as_of,
        lookback_days=lookback_days,
        kind_session=_kind_session,
        kind_warning_menus=KIND_WARNING_MENUS,
        trading_halt_markets=TRADING_HALT_MARKETS,
        kind_warn_main_url=KIND_WARN_MAIN_URL,
        kind_trading_halt_main_url=KIND_TRADING_HALT_MAIN_URL,
        post_kind_warn_html=_post_kind_warn_html,
        post_kind_trading_halt_html=_post_kind_trading_halt_html,
        build_name_lookup=_build_name_lookup,
        parse_kind_warning_html=_parse_kind_warning_html,
        parse_kind_trading_halt_html=_parse_kind_trading_halt_html,
        resolve_symbol_by_name=_resolve_symbol_by_name,
        load_event_calendar_rows=_load_event_calendar_rows,
        build_event_halt_rows=_build_event_halt_rows,
        dedupe_official_rows=_dedupe_official_rows,
        build_official_state_map=build_official_state_map,
        snapshot_state_counts=_snapshot_state_counts,
        now_iso=_now_iso,
        parse_iso_date=_parse_iso_date,
        date_text=_date_text,
        default_summary_days=DEFAULT_SUMMARY_DAYS,
        clean_text=_clean_text,
    )


def _fetch_market_snapshot(date_text: str) -> pd.DataFrame:
    if pykrx_stock is None:
        return pd.DataFrame()
    previous_disable_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    frames: list[pd.DataFrame] = []
    try:
        for market_name in ("ALL", "KOSPI", "KOSDAQ"):
            try:
                frame = pykrx_stock.get_market_ohlcv_by_ticker(date_text, market=market_name)
            except Exception:
                frame = pd.DataFrame()
            if frame is None or frame.empty:
                continue
            working = frame.reset_index().rename(
                columns={"티커": "symbol", "종가": "close", "거래량": "volume"}
            )
            if "symbol" not in working.columns:
                continue
            working["symbol"] = working["symbol"].astype(str).str.zfill(6)
            working["close"] = pd.to_numeric(working.get("close"), errors="coerce")
            working["volume"] = pd.to_numeric(working.get("volume"), errors="coerce")
            frames.append(working[["symbol", "close", "volume"]].dropna(subset=["close"]))
            if market_name == "ALL":
                break
    finally:
        logging.disable(previous_disable_level)
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True)
    return merged.drop_duplicates(subset=["symbol"]).reset_index(drop=True)


def _build_market_history_bundle(stock_master: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if pykrx_stock is None:
        raise RuntimeError("pykrx가 없어 시장경보 후보를 계산할 수 없습니다.")
    target_symbols = set(stock_master)
    current_date = datetime.now().date()
    series_by_symbol: dict[str, dict[str, dict[str, float]]] = {
        symbol: {"close": {}, "volume": {}} for symbol in target_symbols
    }
    trading_dates: list[str] = []
    cursor = current_date
    scanned = 0
    max_scan_days = DEFAULT_HISTORY_SESSIONS * 5
    while len(trading_dates) < DEFAULT_HISTORY_SESSIONS and scanned < max_scan_days:
        date_text = cursor.strftime("%Y%m%d")
        iso_text = cursor.isoformat()
        snapshot = _fetch_market_snapshot(date_text)
        if not snapshot.empty:
            filtered = snapshot[snapshot["symbol"].isin(target_symbols)].copy()
            if not filtered.empty:
                trading_dates.append(iso_text)
                for row in filtered.to_dict(orient="records"):
                    symbol = row["symbol"]
                    series_by_symbol[symbol]["close"][iso_text] = float(row["close"])
                    series_by_symbol[symbol]["volume"][iso_text] = float(row.get("volume") or 0.0)
        elif scanned >= 6 and not trading_dates:
            raise RuntimeError("pykrx market snapshot 응답을 안정적으로 받지 못했습니다.")
        cursor -= timedelta(days=1)
        scanned += 1

    if len(trading_dates) < 16:
        raise RuntimeError("최근 거래일 스냅샷이 부족해 시장경보 후보를 계산할 수 없습니다.")

    trading_dates.sort()
    return {
        "as_of": trading_dates[-1],
        "trading_dates": trading_dates,
        "series_by_symbol": series_by_symbol,
        "market_return_3d": _fetch_market_index_returns(trading_dates[-4], trading_dates[-1]),
    }


def _shortlist_history_symbols(
    stock_master: dict[str, dict[str, Any]],
    official_state_map: dict[str, dict[str, Any]],
) -> list[str]:
    shortlist = {
        symbol
        for symbol, state in official_state_map.items()
        if _clean_text(state.get("current_state")) not in {"", "none"}
    }
    listing_df = _load_listing_frame(stock_master)
    movers = listing_df[listing_df["ChagesRatio"].abs() >= 6.0]
    for symbol in movers["Code"].tolist():
        shortlist.add(_norm_symbol(symbol))
    for symbol in listing_df.sort_values(["Amount", "ChagesRatio"], ascending=[False, False]).head(60)["Code"].tolist():
        shortlist.add(_norm_symbol(symbol))
    shortlist = {symbol for symbol in shortlist if symbol in stock_master}
    if len(shortlist) <= 260:
        return sorted(shortlist)

    amount_map = {
        _norm_symbol(row["Code"]): float(row.get("Amount") or 0.0)
        for row in listing_df.to_dict(orient="records")
    }
    change_map = {
        _norm_symbol(row["Code"]): abs(float(row.get("ChagesRatio") or 0.0))
        for row in listing_df.to_dict(orient="records")
    }
    state_priority = {
        "halt_active": 5,
        "halt_pre_notice": 4,
        "risk_active": 4,
        "risk_pre_notice": 3,
        "warning_active": 2,
        "warning_pre_notice": 1,
    }
    prioritized = sorted(
        shortlist,
        key=lambda symbol: (
            state_priority.get(_clean_text(official_state_map.get(symbol, {}).get("current_state")), 0),
            amount_map.get(symbol, 0.0),
            change_map.get(symbol, 0.0),
            symbol,
        ),
        reverse=True,
    )
    return prioritized[:260]


def _normalize_price_history_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    working = frame.copy()
    if "Date" not in working.columns:
        return pd.DataFrame()
    working["Date"] = pd.to_datetime(working["Date"], errors="coerce")
    working["Close"] = pd.to_numeric(working.get("Close"), errors="coerce")
    working["Volume"] = pd.to_numeric(working.get("Volume"), errors="coerce")
    working = working.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)
    return working[["Date", "Close", "Volume"]]


def _fetch_symbol_history(symbol: str) -> tuple[str, pd.DataFrame]:
    end_dt = datetime.now().date()
    start_dt = end_dt - timedelta(days=180)
    try:
        naver_frame = fetch_naver_daily_price_history(
            symbol,
            start_date=start_dt,
            end_date=end_dt,
            lookback_days=220,
            sleep_sec=0.0,
        )
        normalized = _normalize_price_history_frame(naver_frame)
        if not normalized.empty:
            return symbol, normalized
    except Exception:
        pass
    frame = load_price_history(symbol, end_dt=end_dt, lookback_days=420, sleep_sec=0.0)
    return symbol, _normalize_price_history_frame(frame)


def _build_market_history_bundle_fallback(
    stock_master: dict[str, dict[str, Any]],
    official_state_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    shortlist = _shortlist_history_symbols(stock_master, official_state_map)
    if not shortlist:
        raise RuntimeError("fallback shortlist가 비어 있습니다.")
    log.info("market history fallback shortlist size=%s", len(shortlist))
    series_by_symbol: dict[str, dict[str, dict[str, float]]] = {
        symbol: {"close": {}, "volume": {}} for symbol in shortlist
    }
    global_dates: set[str] = set()
    with ThreadPoolExecutor(max_workers=max(1, min(DEFAULT_HISTORY_WORKERS, len(shortlist)))) as executor:
        future_map = {executor.submit(_fetch_symbol_history, symbol): symbol for symbol in shortlist}
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                _, frame = future.result()
            except Exception as exc:
                log.debug("symbol history fallback failed for %s: %s", symbol, str(exc)[:160])
                continue
            if frame.empty:
                continue
            tail = frame.tail(DEFAULT_HISTORY_SESSIONS).copy()
            for row in tail.to_dict(orient="records"):
                date_text = pd.Timestamp(row["Date"]).date().isoformat()
                close = _safe_float(row.get("Close"))
                volume = _safe_float(row.get("Volume"), 0.0) or 0.0
                if close is None or close <= 0:
                    continue
                global_dates.add(date_text)
                series_by_symbol[symbol]["close"][date_text] = float(close)
                series_by_symbol[symbol]["volume"][date_text] = float(volume)

    trading_dates = sorted(global_dates)
    if len(trading_dates) < 16:
        raise RuntimeError("fallback symbol history도 최근 거래일 수가 부족합니다.")
    as_of = trading_dates[-1]
    return {
        "as_of": as_of,
        "trading_dates": trading_dates[-DEFAULT_HISTORY_SESSIONS:],
        "series_by_symbol": series_by_symbol,
        "market_return_3d": {"KOSPI": 0.0, "KOSDAQ": 0.0},
    }


def _fetch_market_index_returns(start_date: str, end_date: str) -> dict[str, float]:
    returns = {"KOSPI": 0.0, "KOSDAQ": 0.0}
    if pykrx_stock is None:
        return returns
    previous_disable_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    index_codes = {"KOSPI": "1001", "KOSDAQ": "2001"}
    try:
        for market, code in index_codes.items():
            try:
                frame = pykrx_stock.get_index_ohlcv_by_date(
                    _parse_iso_date(start_date).strftime("%Y%m%d"),
                    _parse_iso_date(end_date).strftime("%Y%m%d"),
                    code,
                )
            except Exception:
                frame = pd.DataFrame()
            if frame is None or frame.empty or "종가" not in frame.columns:
                continue
            series = pd.to_numeric(frame["종가"], errors="coerce").dropna()
            if len(series) >= 2 and float(series.iloc[0]) > 0:
                returns[market] = round((float(series.iloc[-1]) / float(series.iloc[0]) - 1.0) * 100.0, 2)
    finally:
        logging.disable(previous_disable_level)
    return returns


def evaluate_close_swing_candidate(**kwargs: Any) -> dict[str, Any] | None:
    return _evaluate_close_swing_candidate_impl(**kwargs, next_business_day=_next_business_day)


def evaluate_warning_pre_notice(**kwargs: Any) -> list[dict[str, Any]]:
    return _evaluate_warning_pre_notice_impl(**kwargs, next_business_day=_next_business_day)


def evaluate_warning_designation(**kwargs: Any) -> list[dict[str, Any]]:
    return _evaluate_warning_designation_impl(**kwargs, next_business_day=_next_business_day)


def evaluate_warning_redesignation(**kwargs: Any) -> dict[str, Any] | None:
    return _evaluate_warning_redesignation_impl(**kwargs, next_business_day=_next_business_day)


def evaluate_risk_pre_notice(**kwargs: Any) -> list[dict[str, Any]]:
    return _evaluate_risk_pre_notice_impl(**kwargs, next_business_day=_next_business_day)


def evaluate_risk_designation(**kwargs: Any) -> list[dict[str, Any]]:
    return _evaluate_risk_designation_impl(**kwargs, next_business_day=_next_business_day)


def evaluate_warning_halt_candidate(**kwargs: Any) -> dict[str, Any] | None:
    return _evaluate_warning_halt_candidate_impl(**kwargs, next_business_day=_next_business_day)


def evaluate_risk_halt_candidates(**kwargs: Any) -> list[dict[str, Any]]:
    return _evaluate_risk_halt_candidates_impl(**kwargs, next_business_day=_next_business_day)


def build_market_warning_candidate_snapshot(
    *,
    stock_master: dict[str, dict[str, Any]],
    market_history: dict[str, Any],
    official_state_map: dict[str, dict[str, Any]],
    official_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return _build_market_warning_candidate_snapshot_impl(
        stock_master=stock_master,
        market_history=market_history,
        official_state_map=official_state_map,
        official_rows=official_rows,
        next_business_day=_next_business_day,
        market_group=_market_group,
        safe_float=_safe_float,
        clean_text=_clean_text,
        now_iso=_now_iso,
        load_price_history=load_price_history,
        fetch_naver_intraday_history=fetch_naver_intraday_history,
        parse_iso_date=_parse_iso_date,
        current_state_priority=CURRENT_STATE_PRIORITY,
    )


def build_snapshots(*, lookback_days: int) -> dict[str, dict[str, Any]]:
    now_iso = _now_iso()
    previous_official = _load_previous_snapshot(OFFICIAL_SNAPSHOT_NAME)
    previous_candidates = _load_previous_snapshot(CANDIDATE_SNAPSHOT_NAME)
    stock_master = _load_stock_master_map()
    if not stock_master:
        raise RuntimeError("stock_master projection을 읽지 못했습니다.")

    official_state_map: dict[str, dict[str, Any]] = {}
    market_history: dict[str, Any] | None = None
    as_of = _clean_text(previous_official.get("as_of") if previous_official else "") or now_iso[:10]
    try:
        market_history = _build_market_history_bundle(stock_master)
        as_of = _clean_text(market_history.get("as_of")) or as_of
    except Exception as exc:
        log.exception("market history build failed: %s", exc)

    snapshots: dict[str, dict[str, Any]] = {}
    try:
        official_snapshot, official_state_map = build_market_warning_official_snapshot(
            stock_master=stock_master,
            as_of=as_of,
            lookback_days=lookback_days,
        )
        snapshots[OFFICIAL_SNAPSHOT_NAME] = official_snapshot
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        log.exception("official warning snapshot build failed: %s", exc)
        snapshots[OFFICIAL_SNAPSHOT_NAME] = build_stale_snapshot(
            OFFICIAL_SNAPSHOT_NAME,
            previous_official,
            now_iso,
            exc,
            defaults={
                "lookback_days": lookback_days,
                "default_recent_days": DEFAULT_SUMMARY_DAYS,
                "summary": {
                    "row_count": 0,
                    "today_count": 0,
                    "recent_count": 0,
                    "active_warning_count": 0,
                    "active_risk_count": 0,
                    "active_halt_count": 0,
                    "state_counts": {},
                    "kind_counts": {},
                    "action_counts": {},
                },
            },
        )
        official_state_map = build_official_state_map((previous_official or {}).get("rows") or [], as_of=as_of)

    try:
        if market_history is None:
            market_history = _build_market_history_bundle_fallback(stock_master, official_state_map)
        snapshots[CANDIDATE_SNAPSHOT_NAME] = build_market_warning_candidate_snapshot(
            stock_master=stock_master,
            market_history=market_history,
            official_state_map=official_state_map,
            official_rows=snapshots[OFFICIAL_SNAPSHOT_NAME].get("rows") or [],
        )
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        log.exception("candidate warning snapshot build failed: %s", exc)
        snapshots[CANDIDATE_SNAPSHOT_NAME] = build_stale_snapshot(
            CANDIDATE_SNAPSHOT_NAME,
            previous_candidates,
            now_iso,
            exc,
            defaults={
                "next_effective_date": _next_business_day(as_of),
                "summary": {
                    "row_count": 0,
                    "triggered_count": 0,
                    "near_trigger_count": 0,
                    "category_counts": {},
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
    snapshots = build_snapshots(lookback_days=args.lookback_days)
    result = publish_snapshots(snapshots, skip_mongo=args.skip_mongo)
    if args.print_only:
        print("[market-warning-monitor] build complete")
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
