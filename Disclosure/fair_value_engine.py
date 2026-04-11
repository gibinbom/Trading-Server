from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from typing import Any

import pandas as pd
try:
    import pymongo
except Exception:
    pymongo = None

try:
    from config import SETTINGS
    from runtime_paths import RUNTIME_DIR
    from stock_card_sources import load_analyst_frame, load_event_frame
    from signals.wics_universe import load_effective_wics_sector_meta, load_effective_wics_symbol_map, normalize_sector_name
except Exception:
    from Disclosure.config import SETTINGS
    from Disclosure.runtime_paths import RUNTIME_DIR
    from Disclosure.stock_card_sources import load_analyst_frame, load_event_frame
    from Disclosure.signals.wics_universe import load_effective_wics_sector_meta, load_effective_wics_symbol_map, normalize_sector_name


log = logging.getLogger("disclosure.fair_value")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_DIR = os.path.dirname(ROOT_DIR)
FACTOR_SNAPSHOT_PATH = os.path.join(ROOT_DIR, "factors", "snapshots", "factor_snapshot_latest.csv")
LISTING_PATH = os.path.join(PROJECT_ROOT_DIR, "krx_listing.csv")
QUOTE_PATH = os.path.join(RUNTIME_DIR, "web_projections", "quote_delayed_source_latest.json")
QUOTE_HOTSET_PATH = os.path.join(RUNTIME_DIR, "web_projections", "quote_delayed_hotset_latest.json")
LEGACY_QUOTE_PATH = os.path.join(RUNTIME_DIR, "web_projections", "quote_delayed_latest.json")
CARDS_CSV_PATH = os.path.join(ROOT_DIR, "cards", "stock_cards_latest.csv")
SECTOR_CACHE_PATH = os.path.join(ROOT_DIR, "cache", "sector_cache.json")
SECTOR_THESIS_PATH = os.path.join(ROOT_DIR, "runtime", "sector_thesis_latest.json")
MACRO_REGIME_PATH = os.path.join(ROOT_DIR, "macro", "cache", "macro_regime_latest.json")
VALUATION_DIR = os.path.join(ROOT_DIR, "valuation")
LATEST_CSV_PATH = os.path.join(VALUATION_DIR, "fair_value_snapshot_latest.csv")
LATEST_JSON_PATH = os.path.join(VALUATION_DIR, "fair_value_snapshot_latest.json")
ACTUAL_FINANCIAL_CSV_PATH = os.path.join(VALUATION_DIR, "actual_financial_snapshot_latest.csv")
ELIGIBLE_MARKETS = {"KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"}
EXCLUDED_NAME_KEYWORDS = ("ETF", "ETN", "리츠", "스팩", "SPAC", "기업인수목적", "유동화전문유한회사", "유한회사")
UNCLASSIFIED_SECTOR = "미분류"

_SECTOR_CANONICAL_ALIASES = {
    "반도체": "IT하드웨어 (반도체)",
    "반도체와반도체장비": "IT하드웨어 (반도체)",
    "디스플레이장비및부품": "디스플레이/IT부품",
    "전자부품": "디스플레이/IT부품",
    "전자장비와기기": "디스플레이/IT부품",
    "전자제품": "디스플레이/IT부품",
    "소프트웨어": "IT소프트웨어 (플랫폼/SI)",
    "인터넷서비스": "IT소프트웨어 (플랫폼/SI)",
    "게임": "엔터테인먼트/게임",
    "미디어": "엔터테인먼트/게임",
    "유통업": "유통/백화점",
    "백화점과일반상점": "유통/백화점",
    "화장품": "소비재/화장품",
    "전기제품": "전력기기/전선",
    "전기장비": "전력기기/전선",
    "조선": "조선/해양",
    "기계": "기계/공작기계",
    "건축제품": "건설/건자재",
    "건축자재": "건설/건자재",
    "생물공학": "제약/바이오 (대형)",
    "건강관리기술": "헬스케어/의료기기",
    "철강": "철강/비철금속",
    "비철금속": "철강/비철금속",
    "자동차": "자동차/완성차",
    "자동차부품": "자동차부품/타이어",
    "항공우주와방위산업": "방위산업/우주항공",
    "우주항공과국방": "방위산업/우주항공",
    "석유와가스": "화학/석유화학",
    "생명보험": "보험",
    "손해보험": "보험",
    "식품": "음식료/주류",
    "전기유틸리티": "전력/유틸리티",
    "통신장비": "통신장비/네트워크",
    "해운사": "운송/해운/항공",
    "항공사": "운송/해운/항공",
    "항공화물운송과물류": "운송/해운/항공",
    "도로와철도운송": "운송/해운/항공",
}

_FAMILY_BY_SECTOR = {
    "pbr_proxy": {
        "금융지주/은행",
        "은행",
        "증권",
        "보험",
        "통신서비스",
        "전력/유틸리티",
        "복합기업",
    },
    "ev_ebitda": {
        "조선/해양",
        "기계/공작기계",
        "방위산업/우주항공",
        "화학/석유화학",
        "철강/비철금속",
        "운송/해운/항공",
        "건설/건자재",
        "전력/유틸리티",
    },
    "psr": {
        "제약/바이오 (대형)",
        "헬스케어/의료기기",
        "엔터테인먼트/게임",
        "IT소프트웨어 (플랫폼/SI)",
        "2차전지/배터리",
    },
}

_ANCHOR_LABELS = {
    "analyst": "애널",
    "peer": "피어",
    "earnings": "실적",
}

_FAMILY_LABELS = {
    "pbr_proxy": "PBR/자본수익성",
    "ev_ebitda": "시가총액/영업이익",
    "psr": "PSR",
    "per": "PER",
}

_FAMILY_DRIVER = {
    "pbr_proxy": "net",
    "ev_ebitda": "op",
    "psr": "revenue",
    "per": "net",
}

_BASIS_LABELS = {
    "pbr_proxy": "PBR",
    "ev_ebitda": "시가총액/영업이익",
    "psr": "PSR",
    "per": "PER",
}

_USER_FACING_DIRECT_BASIS = {"FY1", "FY0", "실제 실적", "실제 실적 + 공시 보정"}

_FINANCIAL_ROE_ASSUMPTIONS = {
    "금융지주/은행": 0.09,
    "은행": 0.09,
    "증권": 0.08,
    "보험": 0.08,
    "통신서비스": 0.07,
    "전력/유틸리티": 0.055,
    "복합기업": 0.075,
}

_FINANCIAL_PBR_DEFAULTS = {
    "금융지주/은행": 0.62,
    "은행": 0.60,
    "증권": 0.72,
    "보험": 0.78,
    "통신서비스": 0.92,
    "전력/유틸리티": 0.80,
    "복합기업": 0.76,
}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _business_days_elapsed(value: Any) -> int | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        captured = pd.Timestamp(text).normalize()
        today = pd.Timestamp.now().normalize()
    except Exception:
        return None
    if pd.isna(captured) or pd.isna(today):
        return None
    if captured > today:
        return 0
    try:
        return max(len(pd.bdate_range(captured, today)) - 1, 0)
    except Exception:
        return None


def _is_stale_official_close(value: Any, *, max_business_days: int = 3) -> bool:
    elapsed = _business_days_elapsed(value)
    return elapsed is None or elapsed > int(max_business_days)


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def _normalize_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", "None"):
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", "None"):
            return default
        return int(float(value))
    except Exception:
        return default


def _clip(value: Any, low: float, high: float) -> float:
    return max(low, min(high, _safe_float(value, low)))


def _read_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _normalize_sector(value: Any, default: str = "") -> str:
    text = normalize_sector_name(value)
    if not text:
        return default
    return _SECTOR_CANONICAL_ALIASES.get(text, text)


def _classify_excluded_reason(row: pd.Series) -> str:
    market = _clean_text(row.get("market") or row.get("Market"))
    name = _clean_text(row.get("name") or row.get("Name"))
    if market not in ELIGIBLE_MARKETS:
        return market or "시장 제외"
    if _is_preferred(name):
        return "우선주"
    upper_name = name.upper()
    for keyword in EXCLUDED_NAME_KEYWORDS:
        if keyword and (keyword in name or keyword in upper_name):
            return keyword
    return ""


def _load_listing_frame() -> pd.DataFrame:
    if not os.path.exists(LISTING_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(LISTING_PATH, dtype={"Code": str})
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"Code": "symbol", "Name": "name", "Market": "market", "Close": "close", "Marcap": "marcap"}).copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["name"] = df.get("name", "").fillna("").map(_clean_text)
    df["market"] = df.get("market", "").fillna("").map(_clean_text)
    df["close"] = pd.to_numeric(df.get("close"), errors="coerce")
    df["marcap"] = pd.to_numeric(df.get("marcap"), errors="coerce")
    df["excluded_reason"] = df.apply(_classify_excluded_reason, axis=1)
    return df


def _load_cards_sector_frame() -> pd.DataFrame:
    if not os.path.exists(CARDS_CSV_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(CARDS_CSV_PATH, dtype={"symbol": str})
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["sector"] = df.get("sector", "").map(lambda value: _normalize_sector(value))
    keep = [col for col in ("symbol", "name", "sector") if col in df.columns]
    return df[keep].copy()


def _load_sector_cache_map() -> dict[str, str]:
    payload = _read_json(SECTOR_CACHE_PATH)
    out: dict[str, str] = {}
    for key, value in payload.items():
        symbol = _normalize_symbol(key)
        sector = _normalize_sector(value)
        if symbol and sector:
            out[symbol] = sector
    return out


def _build_sector_reference_maps(
    *,
    listing_df: pd.DataFrame,
    factor_df: pd.DataFrame,
    cards_df: pd.DataFrame,
    analyst_df: pd.DataFrame,
    event_df: pd.DataFrame,
) -> tuple[dict[str, str], dict[str, str]]:
    sector_map: dict[str, str] = {}
    source_map: dict[str, str] = {}
    source_priority = {"wics": 1, "cache": 2, "analyst": 3, "event": 4, "cards": 5, "factor": 6, "default": 99}

    def register(symbol: Any, sector: Any, source: str) -> None:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_sector = _normalize_sector(sector)
        if not normalized_symbol or not normalized_sector:
            return
        current_source = source_map.get(normalized_symbol)
        if current_source and source_priority.get(current_source, 99) <= source_priority.get(source, 99):
            return
        sector_map[normalized_symbol] = normalized_sector
        source_map[normalized_symbol] = source

    for symbol, sector in load_effective_wics_symbol_map().items():
        register(symbol, sector, "wics")

    for symbol, sector in _load_sector_cache_map().items():
        register(symbol, sector, "cache")

    for df, sector_col, source in (
        (analyst_df, "sector", "analyst"),
        (event_df, "event_sector", "event"),
        (cards_df, "sector", "cards"),
        (factor_df, "sector", "factor"),
    ):
        if df is None or df.empty or sector_col not in df.columns:
            continue
        for _, row in df.iterrows():
            register(row.get("symbol"), row.get(sector_col), source)

    for _, row in listing_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        if symbol in sector_map:
            continue
        sector_map[symbol] = UNCLASSIFIED_SECTOR
        source_map[symbol] = "default"
    return sector_map, source_map


def _is_preferred(name: Any) -> bool:
    text = _clean_text(name)
    if not text:
        return False
    return text.endswith("우") or text.endswith("우B") or any(text.endswith(f"{digit}우B") for digit in "1234")


def _preferred_share_penalty(name: Any) -> float:
    return 0.18 if _is_preferred(name) else 0.0


def _valuation_family(sector: Any) -> str:
    text = _normalize_sector(sector)
    if not text:
        return "per"
    for family, sectors in _FAMILY_BY_SECTOR.items():
        if text in sectors:
            return family
    return "per"


def _family_metric_multiple(row: pd.Series, family: str) -> float | None:
    if family == "pbr_proxy":
        pbr = _safe_float(row.get("cons_pbr"), float("nan"))
        return None if pbr <= 0 else pbr
    if family == "per":
        net_yield = _safe_float(row.get("cons_net_yield"), float("nan"))
        return None if net_yield <= 0 else 1.0 / net_yield
    if family == "ev_ebitda":
        op_yield = _safe_float(row.get("cons_op_yield"), float("nan"))
        return None if op_yield <= 0 else 1.0 / op_yield
    psr = _safe_float(row.get("cons_psr"), float("nan"))
    return None if psr <= 0 else psr


def _load_factor_snapshot_frame() -> pd.DataFrame:
    if not os.path.exists(FACTOR_SNAPSHOT_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(FACTOR_SNAPSHOT_PATH, dtype={"symbol": str})
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["sector"] = df.get("sector", "").map(lambda value: _normalize_sector(value))
    df["name"] = df.get("name", "").fillna("")
    df["close"] = pd.to_numeric(df.get("close"), errors="coerce")
    df["marcap"] = pd.to_numeric(df.get("marcap"), errors="coerce")
    for col in [
        "cons_revenue_q_krw",
        "cons_op_q_krw",
        "cons_net_q_krw",
        "cons_revenue_q_annualized_krw",
        "cons_op_q_annualized_krw",
        "cons_net_q_annualized_krw",
        "cons_revenue_y_krw",
        "cons_op_y_krw",
        "cons_net_y_krw",
        "cons_revenue_fy0_krw",
        "cons_op_fy0_krw",
        "cons_net_fy0_krw",
        "cons_revenue_fy1_krw",
        "cons_op_fy1_krw",
        "cons_net_fy1_krw",
        "cons_revenue_actual_krw",
        "cons_op_actual_krw",
        "cons_net_actual_krw",
        "cons_psr",
        "cons_op_yield",
        "cons_net_yield",
        "cons_op_margin_q",
        "cons_net_margin_q",
        "cons_actual_year",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA
    for col in [
        "cons_revenue_basis_period",
        "cons_op_basis_period",
        "cons_net_basis_period",
        "cons_revenue_input_source",
        "cons_op_input_source",
        "cons_net_input_source",
    ]:
        if col not in df.columns:
            df[col] = ""
    valuation_family = df["sector"].map(_valuation_family)
    preferred_share_penalty = df["name"].map(_preferred_share_penalty)
    liquidity_eligible = preferred_share_penalty.lt(0.01)
    current_multiple = df.assign(valuation_family=valuation_family).apply(
        lambda row: _family_metric_multiple(row, str(row.get("valuation_family") or "per")),
        axis=1,
    )
    return df.assign(
        valuation_family=valuation_family,
        preferred_share_penalty=preferred_share_penalty,
        liquidity_eligible=liquidity_eligible,
        current_multiple=current_multiple,
    ).copy()


def _load_quote_frame() -> pd.DataFrame:
    merged_rows: dict[str, dict[str, Any]] = {}

    def _quote_score(row: dict[str, Any]) -> tuple[int, float]:
        status = _clean_text(row.get("price_status"))
        status_score = 4 if status == "지연시세" else 3 if status == "업데이트 지연" else 2 if status == "공식종가 fallback" else 1
        captured_text = _clean_text(row.get("price_captured_at") or row.get("captured_at"))
        try:
            captured_score = datetime.fromisoformat(captured_text).timestamp() if captured_text else 0.0
        except Exception:
            captured_score = 0.0
        return status_score, captured_score

    for path in (QUOTE_PATH, QUOTE_HOTSET_PATH, LEGACY_QUOTE_PATH):
        payload = _read_json(path)
        candidate = payload.get("rows") if isinstance(payload, dict) else None
        if candidate is None and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fp:
                    raw = json.load(fp)
                candidate = raw if isinstance(raw, list) else raw.get("rows", []) if isinstance(raw, dict) else []
            except Exception:
                candidate = []
        if not candidate:
            continue
        for row in candidate:
            symbol = _normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            current = merged_rows.get(symbol)
            if current is None or _quote_score(row) >= _quote_score(current):
                merged_rows[symbol] = row
    rows = list(merged_rows.values())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    for col in ("price", "official_close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA
    keep = [
        col
        for col in (
            "symbol",
            "price",
            "price_source",
            "price_captured_at",
            "price_freshness",
            "official_close",
            "official_close_date",
            "price_status",
            "change_rate_pct",
        )
        if col in df.columns
    ]
    if not keep:
        return pd.DataFrame()
    return df[keep].copy()


def _load_actual_financial_frame() -> pd.DataFrame:
    if not os.path.exists(ACTUAL_FINANCIAL_CSV_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(ACTUAL_FINANCIAL_CSV_PATH, dtype={"symbol": str})
    except Exception:
        return pd.DataFrame()
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    for col in ("actual_revenue_krw", "actual_op_krw", "actual_net_krw", "actual_pbr", "actual_roe", "actual_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA
    keep = [
        "symbol",
        "actual_revenue_krw",
        "actual_op_krw",
        "actual_net_krw",
        "actual_pbr",
        "actual_roe",
        "actual_year",
        "source",
    ]
    return df[[col for col in keep if col in df.columns]].copy()


def _load_direct_consensus_frame(symbols: set[str] | None = None) -> pd.DataFrame:
    if pymongo is None:
        return pd.DataFrame()
    try:
        client = pymongo.MongoClient(
            SETTINGS.MONGO_URI,
            serverSelectionTimeoutMS=1500,
            connectTimeoutMS=1500,
            socketTimeoutMS=1500,
        )
        col = client[SETTINGS.DB_NAME][SETTINGS.CONSENSUS_COLLECTION]
        cursor = col.find(
            {},
            {
                "stock_code": 1,
                "date": 1,
                "revenue": 1,
                "operating_profit": 1,
                "net_profit": 1,
                "revenue_fy0": 1,
                "operating_profit_fy0": 1,
                "net_profit_fy0": 1,
                "revenue_fy1": 1,
                "operating_profit_fy1": 1,
                "net_profit_fy1": 1,
                "revenue_actual": 1,
                "operating_profit_actual": 1,
                "net_profit_actual": 1,
                "pbr_fy0": 1,
                "pbr_fy1": 1,
                "pbr_actual": 1,
                "roe_fy0": 1,
                "roe_fy1": 1,
                "roe_actual": 1,
                "actual_year": 1,
            },
        ).sort([("date", pymongo.DESCENDING)])
        latest_by_symbol: dict[str, dict[str, Any]] = {}
        for doc in cursor:
            symbol = _normalize_symbol(doc.get("stock_code"))
            if not symbol or (symbols and symbol not in symbols) or symbol in latest_by_symbol:
                continue
            latest_by_symbol[symbol] = doc
        client.close()
    except Exception as exc:
        log.debug("direct consensus load failed: %s", exc)
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    factor = 100_000_000.0
    for symbol, doc in latest_by_symbol.items():
        rows.append(
            {
                "symbol": symbol,
                "direct_cons_revenue_q_krw": _safe_float(doc.get("revenue"), float("nan")) * factor if _safe_float(doc.get("revenue"), float("nan")) > 0 else pd.NA,
                "direct_cons_op_q_krw": _safe_float(doc.get("operating_profit"), float("nan")) * factor if _safe_float(doc.get("operating_profit"), float("nan")) > 0 else pd.NA,
                "direct_cons_net_q_krw": _safe_float(doc.get("net_profit"), float("nan")) * factor if _safe_float(doc.get("net_profit"), float("nan")) > 0 else pd.NA,
                "direct_cons_revenue_fy0_krw": _safe_float(doc.get("revenue_fy0"), float("nan")) * factor if _safe_float(doc.get("revenue_fy0"), float("nan")) > 0 else pd.NA,
                "direct_cons_op_fy0_krw": _safe_float(doc.get("operating_profit_fy0"), float("nan")) * factor if _safe_float(doc.get("operating_profit_fy0"), float("nan")) > 0 else pd.NA,
                "direct_cons_net_fy0_krw": _safe_float(doc.get("net_profit_fy0"), float("nan")) * factor if _safe_float(doc.get("net_profit_fy0"), float("nan")) > 0 else pd.NA,
                "direct_cons_revenue_fy1_krw": _safe_float(doc.get("revenue_fy1"), float("nan")) * factor if _safe_float(doc.get("revenue_fy1"), float("nan")) > 0 else pd.NA,
                "direct_cons_op_fy1_krw": _safe_float(doc.get("operating_profit_fy1"), float("nan")) * factor if _safe_float(doc.get("operating_profit_fy1"), float("nan")) > 0 else pd.NA,
                "direct_cons_net_fy1_krw": _safe_float(doc.get("net_profit_fy1"), float("nan")) * factor if _safe_float(doc.get("net_profit_fy1"), float("nan")) > 0 else pd.NA,
                "direct_cons_revenue_actual_krw": _safe_float(doc.get("revenue_actual"), float("nan")) * factor if _safe_float(doc.get("revenue_actual"), float("nan")) > 0 else pd.NA,
                "direct_cons_op_actual_krw": _safe_float(doc.get("operating_profit_actual"), float("nan")) * factor if _safe_float(doc.get("operating_profit_actual"), float("nan")) > 0 else pd.NA,
                "direct_cons_net_actual_krw": _safe_float(doc.get("net_profit_actual"), float("nan")) * factor if _safe_float(doc.get("net_profit_actual"), float("nan")) > 0 else pd.NA,
                "direct_cons_pbr_fy0": _safe_float(doc.get("pbr_fy0"), float("nan")) if _safe_float(doc.get("pbr_fy0"), float("nan")) > 0 else pd.NA,
                "direct_cons_pbr_fy1": _safe_float(doc.get("pbr_fy1"), float("nan")) if _safe_float(doc.get("pbr_fy1"), float("nan")) > 0 else pd.NA,
                "direct_cons_pbr_actual": _safe_float(doc.get("pbr_actual"), float("nan")) if _safe_float(doc.get("pbr_actual"), float("nan")) > 0 else pd.NA,
                "direct_cons_roe_fy0": _safe_float(doc.get("roe_fy0"), float("nan")) if _safe_float(doc.get("roe_fy0"), float("nan")) > 0 else pd.NA,
                "direct_cons_roe_fy1": _safe_float(doc.get("roe_fy1"), float("nan")) if _safe_float(doc.get("roe_fy1"), float("nan")) > 0 else pd.NA,
                "direct_cons_roe_actual": _safe_float(doc.get("roe_actual"), float("nan")) if _safe_float(doc.get("roe_actual"), float("nan")) > 0 else pd.NA,
                "direct_cons_actual_year": _safe_int(doc.get("actual_year")) or pd.NA,
            }
        )
    return pd.DataFrame(rows)


def _pick_metric_value(
    fy1_value: Any,
    fy0_value: Any,
    actual_value: Any,
    annualized_quarter_value: Any,
) -> tuple[float | pd._libs.missing.NAType, str, str]:
    fy1 = _safe_float(fy1_value, float("nan"))
    fy0 = _safe_float(fy0_value, float("nan"))
    actual = _safe_float(actual_value, float("nan"))
    annualized_q = _safe_float(annualized_quarter_value, float("nan"))
    if math.isfinite(fy0) and fy0 > 0:
        return fy0, "FY0", "annual_consensus"
    if math.isfinite(fy1) and fy1 > 0:
        return fy1, "FY1", "annual_consensus"
    if math.isfinite(actual) and actual > 0:
        return actual, "실제 실적", "actual_annual"
    if math.isfinite(annualized_q) and annualized_q > 0:
        return annualized_q, "연환산 분기", "quarter_annualized"
    return pd.NA, "", ""


def _pick_ratio_value(
    fy1_value: Any,
    fy0_value: Any,
    actual_value: Any,
) -> tuple[float | pd._libs.missing.NAType, str, str]:
    fy1 = _safe_float(fy1_value, float("nan"))
    fy0 = _safe_float(fy0_value, float("nan"))
    actual = _safe_float(actual_value, float("nan"))
    if math.isfinite(fy0) and fy0 > 0:
        return fy0, "FY0", "annual_consensus"
    if math.isfinite(fy1) and fy1 > 0:
        return fy1, "FY1", "annual_consensus"
    if math.isfinite(actual) and actual > 0:
        return actual, "실제 실적", "actual_annual"
    return pd.NA, "", ""


def _load_sector_thesis_map() -> dict[str, dict[str, Any]]:
    payload = _read_json(SECTOR_THESIS_PATH)
    thesis_map = payload.get("by_sector") or {}
    if not isinstance(thesis_map, dict):
        return {}
    return {_normalize_sector(key): value for key, value in thesis_map.items() if _normalize_sector(key)}


def _load_macro_sector_scores() -> dict[str, float]:
    payload = _read_json(MACRO_REGIME_PATH)
    scores = payload.get("sector_scores") or {}
    if not isinstance(scores, dict):
        return {}
    normalized: dict[str, float] = {}
    for key, value in scores.items():
        sector = _normalize_sector(key)
        if sector:
            normalized[sector] = _safe_float(value)
    return normalized


def _event_type_weight(event_type: str) -> tuple[float, float, float]:
    text = _clean_text(event_type).upper()
    if any(token in text for token in ["SUPPLY", "CONTRACT", "ORDER"]):
        return 1.0, 0.9, 0.55
    if any(token in text for token in ["PERF", "RESULT", "EARN", "SALES"]):
        return 0.65, 1.0, 0.85
    if "DIVIDEND" in text:
        return 0.10, 0.15, 0.35
    if "STOCK_SPLIT" in text:
        return 0.12, 0.12, 0.15
    if any(token in text for token in ["MERGER", "SPINOFF"]):
        return 0.25, 0.30, 0.25
    if any(token in text for token in ["REVERSE_SPLIT", "REDUCTION"]):
        return -0.08, -0.12, -0.18
    if any(token in text for token in ["BUYBACK", "CANCELLATION"]):
        return 0.15, 0.20, 0.35
    if any(token in text for token in ["DILUTION", "BW", "CB", "RIGHTS"]):
        return -0.15, -0.30, -0.50
    return 0.35, 0.45, 0.40


def _build_revision_inputs(row: pd.Series) -> dict[str, Any]:
    target_revision_pct = _safe_float(row.get("analyst_target_revision_pct"))
    positive_revision_ratio = _clip(row.get("analyst_positive_revision_ratio"), 0.0, 1.0)
    revision_breadth_score = _clip(row.get("analyst_revision_breadth_score"), 0.0, 1.0)
    revision_breadth_count = _safe_int(row.get("analyst_revision_breadth_count"))
    peer_support_count = _safe_int(row.get("analyst_peer_support_count"))
    event_bias = _clean_text(row.get("event_last_bias")).lower()
    event_type = _clean_text(row.get("event_last_type"))
    event_alpha_5d = _safe_float(row.get("event_expected_alpha_5d"))
    event_interpretable = _safe_float(row.get("event_interpretable_score"))
    event_valid_sample_size = _safe_int(row.get("event_valid_sample_size"))
    event_price_coverage = _clip(row.get("event_price_coverage_pct"), 0.0, 100.0)

    analyst_component = _clip(target_revision_pct / 100.0, -0.18, 0.18) * 0.45
    breadth_component = ((positive_revision_ratio - 0.5) * 0.10) + ((revision_breadth_score - 0.5) * 0.08)
    peer_component = min(0.04, peer_support_count * 0.01)

    coverage_scale = _clip(max(event_valid_sample_size / 4.0, event_price_coverage / 100.0), 0.15, 1.0)
    alpha_component = _clip(event_alpha_5d / 100.0, -0.12, 0.12) * 0.60
    bias_component = 0.03 if event_bias == "positive" else -0.04 if event_bias == "negative" else 0.0
    interpret_component = _clip(event_interpretable, -1.0, 1.0) * 0.03
    base_event_signal = (alpha_component + bias_component + interpret_component) * coverage_scale
    revenue_weight, op_weight, net_weight = _event_type_weight(event_type)

    revenue_revision = _clip(analyst_component * 0.20 + breadth_component * 0.15 + base_event_signal * revenue_weight, -0.15, 0.18)
    op_revision = _clip(analyst_component * 0.45 + breadth_component * 0.40 + peer_component + base_event_signal * op_weight, -0.18, 0.20)
    net_revision = _clip(analyst_component * 0.45 + breadth_component * 0.45 + peer_component * 0.5 + base_event_signal * net_weight, -0.18, 0.20)

    reasons: list[str] = []
    if positive_revision_ratio >= 0.7 and revision_breadth_count > 0:
        reasons.append("목표가 상향")
    if event_bias == "positive" and event_type:
        if any(token in event_type.upper() for token in ["SUPPLY", "CONTRACT", "ORDER"]):
            reasons.append("수주 반영")
        elif any(token in event_type.upper() for token in ["PERF", "RESULT", "EARN", "SALES"]):
            reasons.append("실적 공시 반영")
        else:
            reasons.append("최근 공시 반영")
    elif event_bias == "negative" and event_type:
        reasons.append("최근 공시 보수 반영")
    if op_revision >= 0.03 or net_revision >= 0.03:
        reasons.append("영업이익 추정 상향")

    return {
        "revenue_revision": revenue_revision,
        "op_revision": op_revision,
        "net_revision": net_revision,
        "breadth_count": revision_breadth_count,
        "positive_revision_ratio": positive_revision_ratio,
        "event_valid_sample_size": event_valid_sample_size,
        "event_price_coverage_pct": event_price_coverage,
        "reasons": reasons[:3],
    }


def _build_analyst_anchor(row: pd.Series) -> dict[str, Any] | None:
    current_price = _safe_float(row.get("close"), float("nan"))
    report_count = _safe_int(row.get("analyst_report_count"))
    if not math.isfinite(current_price) or current_price <= 0 or report_count <= 0:
        return None

    upside_pct = _safe_float(row.get("analyst_target_upside_pct"))
    dispersion_pct = max(4.0, min(25.0, abs(_safe_float(row.get("analyst_target_dispersion_pct"), 6.0))))
    agreement_score = _clip(row.get("analyst_agreement_score"), 0.0, 1.0)
    recency_score = _clip(row.get("analyst_recency_score"), 0.0, 1.0)
    broker_diversity = _safe_int(row.get("analyst_broker_diversity"))

    base_price = current_price * (1.0 + (upside_pct / 100.0))
    base_price = current_price + (base_price - current_price) * max(0.35, recency_score)
    bear_price = current_price * (1.0 + ((upside_pct - dispersion_pct) / 100.0))
    bull_price = current_price * (1.0 + ((upside_pct + max(4.0, dispersion_pct * 0.8)) / 100.0))

    report_scale = min(1.0, report_count / 4.0)
    broker_scale = min(1.0, broker_diversity / 3.0)
    dispersion_penalty = _clip(1.0 - (dispersion_pct / 35.0), 0.35, 1.0)
    confidence = _clip(
        0.22
        + (report_scale * 0.23)
        + (broker_scale * 0.12)
        + (agreement_score * 0.20)
        + (recency_score * 0.18)
        + (dispersion_penalty * 0.10),
        0.25,
        0.92,
    )
    reasons = []
    if upside_pct >= 10.0:
        reasons.append("애널 목표가 상단")
    if _safe_float(row.get("analyst_positive_revision_ratio")) >= 0.7:
        reasons.append("목표가 상향")
    if agreement_score >= 0.75:
        reasons.append("애널 합의도 양호")
    return {
        "anchor": "analyst",
        "label": _ANCHOR_LABELS["analyst"],
        "base_price": round(base_price, 2),
        "bear_price": round(min(bear_price, base_price), 2),
        "bull_price": round(max(bull_price, base_price), 2),
        "confidence": round(confidence, 4),
        "reasons": reasons[:3],
    }


def _build_peer_anchor(row: pd.Series, factor_df: pd.DataFrame, wics_meta: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    current_price = _safe_float(row.get("close"), float("nan"))
    current_multiple = _safe_float(row.get("current_multiple"), float("nan"))
    sector = _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)
    family = _clean_text(row.get("valuation_family")) or "per"
    symbol = _normalize_symbol(row.get("symbol"))
    if not math.isfinite(current_price) or current_price <= 0 or not math.isfinite(current_multiple) or current_multiple <= 0 or not sector:
        return None

    peer_df = factor_df.copy()
    peer_df = peer_df[
        peer_df["symbol"].astype(str).ne(symbol)
        & peer_df["liquidity_eligible"].fillna(False)
        & peer_df["current_multiple"].notna()
        & peer_df["valuation_family"].astype(str).eq(family)
    ].copy()
    sector_peers = peer_df[peer_df["sector"].astype(str).eq(sector)].copy()
    peer_group = sector
    if len(sector_peers) >= 2:
        peer_df = sector_peers
    else:
        peer_group = f"{sector} / {_FAMILY_LABELS.get(family, family)} 확장"

    if len(peer_df) < 2:
        return None

    multiple = pd.to_numeric(peer_df["current_multiple"], errors="coerce").dropna()
    if len(multiple) < 2:
        return None

    q25, q50, q75 = [float(multiple.quantile(q)) for q in (0.25, 0.50, 0.75)]
    if min(q25, q50, q75) <= 0:
        return None

    base_price = current_price * (q50 / current_multiple)
    bear_price = current_price * (q25 / current_multiple)
    bull_price = current_price * (q75 / current_multiple)
    dispersion = abs(q75 - q25) / q50 if q50 > 0 else 1.0

    meta = wics_meta.get(sector) or {}
    history_label = _clean_text(meta.get("history_confidence_label")) or "없음"
    status_label = _clean_text(meta.get("universe_status_label")) or "유동형"
    dynamic_stability = _safe_float(meta.get("avg_dynamic_stability"))
    peer_count = int(len(peer_df))
    peer_scale = min(1.0, peer_count / 8.0)
    history_scale = {"충분": 1.0, "예비": 0.80, "없음": 0.65}.get(history_label, 0.75)
    status_scale = {"안정형": 1.0, "유동형": 0.88, "재점검": 0.70}.get(status_label, 0.82)
    stability_scale = 0.80 if dynamic_stability <= 0 else (0.65 + (0.35 * dynamic_stability))
    dispersion_penalty = _clip(1.0 - min(0.8, dispersion), 0.30, 1.0)
    confidence = _clip(
        0.24
        + (peer_scale * 0.24)
        + (history_scale * 0.16)
        + (status_scale * 0.12)
        + (stability_scale * 0.12)
        + (dispersion_penalty * 0.12),
        0.25,
        0.90,
    )

    gap_pct = ((base_price / current_price) - 1.0) * 100.0 if current_price > 0 else 0.0
    reasons = []
    if gap_pct >= 10.0:
        reasons.append("피어 멀티플 대비 할인")
    elif gap_pct <= -10.0:
        reasons.append("피어 멀티플 기준 선반영")
    if history_label == "충분" and status_label == "안정형":
        reasons.append("WICS 바스켓 안정")

    return {
        "anchor": "peer",
        "label": _ANCHOR_LABELS["peer"],
        "base_price": round(base_price, 2),
        "bear_price": round(min(bear_price, base_price), 2),
        "bull_price": round(max(bull_price, base_price), 2),
        "confidence": round(confidence, 4),
        "peer_group": peer_group,
        "peer_count": peer_count,
        "dispersion": round(dispersion, 4),
        "reasons": reasons[:3],
    }


def _build_earnings_anchor(row: pd.Series, revisions: dict[str, Any]) -> dict[str, Any] | None:
    current_price = _safe_float(row.get("close"), float("nan"))
    if not math.isfinite(current_price) or current_price <= 0:
        return None

    family = _clean_text(row.get("valuation_family")) or "per"
    driver = _FAMILY_DRIVER.get(family, "net")
    if driver == "revenue":
        revision = _safe_float(revisions.get("revenue_revision"))
        has_consensus = _safe_float(row.get("cons_revenue_y_krw")) > 0
    elif driver == "op":
        revision = _safe_float(revisions.get("op_revision"))
        has_consensus = _safe_float(row.get("cons_op_y_krw")) > 0
    else:
        revision = _safe_float(revisions.get("net_revision"))
        has_consensus = _safe_float(row.get("cons_net_y_krw")) > 0

    if not has_consensus:
        return None

    breadth_count = _safe_int(revisions.get("breadth_count"))
    positive_revision_ratio = _clip(revisions.get("positive_revision_ratio"), 0.0, 1.0)
    event_valid_sample_size = _safe_int(revisions.get("event_valid_sample_size"))
    event_coverage = _clip(revisions.get("event_price_coverage_pct"), 0.0, 100.0)
    parse_quality = _clip(row.get("analyst_parse_quality_score"), 0.0, 1.0)
    event_scale = _clip(max(event_valid_sample_size / 4.0, event_coverage / 100.0), 0.15, 1.0)

    uncertainty = 0.05 + (abs(revision) * 0.45) + (0.08 if breadth_count <= 0 else 0.0) + (0.05 if event_valid_sample_size <= 0 else 0.0)
    uncertainty = _clip(uncertainty, 0.05, 0.22)
    base_price = current_price * (1.0 + revision)
    bear_price = current_price * (1.0 + revision - uncertainty)
    bull_price = current_price * (1.0 + revision + uncertainty)

    confidence = _clip(
        0.24
        + (0.16 if has_consensus else 0.0)
        + (min(1.0, breadth_count / 3.0) * 0.16)
        + (positive_revision_ratio * 0.12)
        + (event_scale * 0.12)
        + (parse_quality * 0.10),
        0.25,
        0.88,
    )
    reasons = list(revisions.get("reasons") or [])
    if not reasons and abs(revision) > 0.01:
        reasons.append("실적 추정 보정")
    return {
        "anchor": "earnings",
        "label": _ANCHOR_LABELS["earnings"],
        "base_price": round(base_price, 2),
        "bear_price": round(min(bear_price, base_price), 2),
        "bull_price": round(max(bull_price, base_price), 2),
        "confidence": round(confidence, 4),
        "reasons": reasons[:3],
        "revision_pct": round(revision * 100.0, 2),
    }


def _dedupe_texts(items: list[str], limit: int = 3) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _clean_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _combine_anchor_prices(
    row: pd.Series,
    anchors: list[dict[str, Any]],
    *,
    sector_thesis_map: dict[str, dict[str, Any]],
    macro_sector_scores: dict[str, float],
    wics_meta: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    current_price = _safe_float(row.get("close"), float("nan"))
    sector = _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)
    family = _clean_text(row.get("valuation_family")) or "per"
    if not math.isfinite(current_price) or current_price <= 0:
        return {}

    available = [anchor for anchor in anchors if anchor]
    present_inputs = {
        {"analyst": "애널 목표가", "peer": "피어 멀티플", "earnings": "실적/추정치"}.get(anchor["anchor"], "")
        for anchor in available
    }
    missing_inputs = [name for name in ("애널 목표가", "피어 멀티플", "실적/추정치") if name not in present_inputs]
    if not available:
        return {
            "current_price": round(current_price, 2),
            "fair_value_bear": pd.NA,
            "fair_value_base": pd.NA,
            "fair_value_bull": pd.NA,
            "fair_value_gap_pct": pd.NA,
            "fair_value_confidence_score": 0.0,
            "fair_value_status_label": "미산출",
            "valuation_primary_method": "",
            "valuation_anchor_mix": "",
            "valuation_peer_group": sector if sector != UNCLASSIFIED_SECTOR else _FAMILY_LABELS.get(family, "PER"),
            "valuation_reason_summary": "직접 근거가 부족해 대체모형이나 추가 입력이 필요합니다.",
            "valuation_missing_inputs": ", ".join(missing_inputs) if missing_inputs else "입력 부족",
            "valuation_tier": "",
            "valuation_proxy_used": False,
            "valuation_coverage_reason": "직접 근거 부족",
        }

    thesis = sector_thesis_map.get(sector) or {}
    wics_row = wics_meta.get(sector) or {}
    history_label = _clean_text(wics_row.get("history_confidence_label")) or "없음"
    status_label = _clean_text(wics_row.get("universe_status_label")) or "유동형"
    dynamic_stability = _safe_float(wics_row.get("avg_dynamic_stability"))
    macro_lens_score = _safe_float((thesis or {}).get("macro_lens_score"), 50.0)
    macro_regime_score = _safe_float(macro_sector_scores.get(sector))

    raw_weights = []
    for anchor in available:
        importance = 1.0 if anchor["anchor"] != "earnings" else 0.9
        raw_weights.append(max(0.05, _safe_float(anchor.get("confidence"), 0.25) ** 1.35 * importance))
    weight_sum = sum(raw_weights) or 1.0
    normalized_weights = [weight / weight_sum for weight in raw_weights]

    base_price = sum(_safe_float(anchor.get("base_price")) * weight for anchor, weight in zip(available, normalized_weights))
    bear_price_raw = sum(_safe_float(anchor.get("bear_price")) * weight for anchor, weight in zip(available, normalized_weights))
    bull_price_raw = sum(_safe_float(anchor.get("bull_price")) * weight for anchor, weight in zip(available, normalized_weights))
    confidence = sum(_safe_float(anchor.get("confidence")) * weight for anchor, weight in zip(available, normalized_weights))

    confidence_scale = 1.0
    width_scale = 1.0
    if history_label == "예비":
        confidence_scale *= 0.92
        width_scale *= 1.08
    elif history_label == "없음":
        confidence_scale *= 0.84
        width_scale *= 1.16
    if status_label == "유동형":
        confidence_scale *= 0.96
        width_scale *= 1.05
    elif status_label == "재점검":
        confidence_scale *= 0.90
        width_scale *= 1.12
    if 0 < dynamic_stability < 0.7:
        confidence_scale *= 0.94
        width_scale *= 1.0 + ((0.7 - dynamic_stability) * 0.25)
    if macro_lens_score > 0:
        if macro_lens_score >= 65:
            confidence_scale *= 1.03
            width_scale *= 0.97
        elif macro_lens_score < 50:
            confidence_scale *= 0.96
            width_scale *= 1.06
    if macro_regime_score < -0.2:
        width_scale *= 1.08
    elif macro_regime_score > 0.2:
        width_scale *= 0.98

    if len(available) == 1:
        solo = available[0]
        base_price = _safe_float(solo.get("base_price"), current_price)
        bear_price = _safe_float(solo.get("bear_price"), base_price)
        bull_price = _safe_float(solo.get("bull_price"), base_price)
        bear_price, base_price, bull_price = _cap_price_range(current_price, base_price, bear_price, bull_price, tier="Tier 3")
        confidence = _clip(_safe_float(solo.get("confidence"), 0.25) * confidence_scale * 0.82, 0.18, 0.74)
        if history_label == "없음":
            confidence = _clip(confidence * 0.92, 0.16, 0.72)
        gap_pct = ((base_price / current_price) - 1.0) * 100.0 if current_price > 0 else 0.0
        if confidence >= 0.55 and gap_pct >= 12.0:
            status = "할인 구간"
        elif confidence >= 0.55 and gap_pct <= -10.0:
            status = "선반영 구간"
        else:
            status = "중립 구간"
        peer_group = _clean_text(solo.get("peer_group")) or (sector if sector != UNCLASSIFIED_SECTOR else _FAMILY_LABELS.get(family, "PER"))
        reasons = _dedupe_texts(list(solo.get("reasons") or []), limit=3)
        if not reasons:
            reasons = ["직접 근거가 한 축만 있어 보수적으로 읽습니다."]
        return {
            "current_price": round(current_price, 2),
            "fair_value_bear": round(min(bear_price, base_price), 2),
            "fair_value_base": round(base_price, 2),
            "fair_value_bull": round(max(bull_price, base_price), 2),
            "fair_value_gap_pct": round(gap_pct, 2),
            "fair_value_confidence_score": round(confidence, 4),
            "fair_value_status_label": status,
            "valuation_primary_method": _ANCHOR_LABELS.get(solo["anchor"], solo["label"]),
            "valuation_anchor_mix": f"{_ANCHOR_LABELS.get(solo['anchor'], solo['label'])} 100",
            "valuation_peer_group": peer_group,
            "valuation_reason_summary": " / ".join(reasons),
            "valuation_missing_inputs": ", ".join(missing_inputs),
            "valuation_tier": "Tier 3",
            "valuation_proxy_used": False,
            "valuation_coverage_reason": f"{_ANCHOR_LABELS.get(solo['anchor'], solo['label'])} 단독",
        }

    confidence = _clip(confidence * confidence_scale, 0.25, 0.92)

    downside = max(0.04, (base_price - bear_price_raw) / max(base_price, 1.0))
    upside = max(0.04, (bull_price_raw - base_price) / max(base_price, 1.0))
    downside *= width_scale
    upside *= max(0.92, width_scale * 0.94)

    bear_price = base_price * (1.0 - downside)
    bull_price = base_price * (1.0 + upside)
    bear_price = min(bear_price, base_price)
    bull_price = max(bull_price, base_price)
    if bear_price <= 0:
        bear_price = max(current_price * 0.5, 1.0)
    result_tier = "Tier 1" if len(available) >= 3 else "Tier 2"
    bear_price, base_price, bull_price = _cap_price_range(current_price, base_price, bear_price, bull_price, tier=result_tier)

    gap_pct = ((base_price / current_price) - 1.0) * 100.0 if current_price > 0 else 0.0
    if confidence >= 0.65 and gap_pct >= 12.0:
        status = "할인 구간"
    elif confidence >= 0.65 and gap_pct <= -10.0:
        status = "선반영 구간"
    else:
        status = "중립 구간"

    top_anchor = available[max(range(len(available)), key=lambda idx: normalized_weights[idx])]
    mix_parts = [f"{anchor['label']} {int(round(weight * 100.0))}" for anchor, weight in zip(available, normalized_weights)]
    peer_group = next(
        (_clean_text(anchor.get("peer_group")) for anchor in available if anchor["anchor"] == "peer" and _clean_text(anchor.get("peer_group"))),
        sector if sector != UNCLASSIFIED_SECTOR else _FAMILY_LABELS.get(family, "PER"),
    )
    reasons: list[str] = []
    for anchor in available:
        reasons.extend(anchor.get("reasons") or [])
    reason_summary = " / ".join(_dedupe_texts(reasons, limit=3)) or "애널·피어·실적 근거를 보수적으로 섞었습니다."

    return {
        "current_price": round(current_price, 2),
        "fair_value_bear": round(bear_price, 2),
        "fair_value_base": round(base_price, 2),
        "fair_value_bull": round(bull_price, 2),
        "fair_value_gap_pct": round(gap_pct, 2),
        "fair_value_confidence_score": round(confidence, 4),
        "fair_value_status_label": status,
        "valuation_primary_method": _ANCHOR_LABELS.get(top_anchor["anchor"], top_anchor["label"]),
        "valuation_anchor_mix": " | ".join(mix_parts),
        "valuation_peer_group": peer_group or sector,
        "valuation_reason_summary": reason_summary,
        "valuation_missing_inputs": ", ".join(missing_inputs),
        "valuation_tier": result_tier,
        "valuation_proxy_used": False,
        "valuation_coverage_reason": "·".join(anchor["label"] for anchor in available),
    }


def _build_proxy_templates(snapshot_df: pd.DataFrame) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, dict[str, float]], dict[str, float]]:
    if snapshot_df.empty:
        return {}, {}, {"gap": 0.0, "bear_ratio": 0.9, "bull_ratio": 1.1, "confidence": 0.22}

    direct_df = snapshot_df[
        pd.to_numeric(snapshot_df["fair_value_base"], errors="coerce").notna()
        & ~snapshot_df["valuation_proxy_used"].fillna(False).astype(bool)
    ].copy()
    if direct_df.empty:
        return {}, {}, {"gap": 0.0, "bear_ratio": 0.9, "bull_ratio": 1.1, "confidence": 0.22}

    direct_df["gap"] = pd.to_numeric(direct_df["fair_value_gap_pct"], errors="coerce")
    direct_df["confidence"] = pd.to_numeric(direct_df["fair_value_confidence_score"], errors="coerce")
    direct_df["bear_ratio"] = pd.to_numeric(direct_df["fair_value_bear"], errors="coerce") / pd.to_numeric(
        direct_df["fair_value_base"], errors="coerce"
    )
    direct_df["bull_ratio"] = pd.to_numeric(direct_df["fair_value_bull"], errors="coerce") / pd.to_numeric(
        direct_df["fair_value_base"], errors="coerce"
    )
    direct_df["sector"] = direct_df["sector"].map(lambda value: _normalize_sector(value, UNCLASSIFIED_SECTOR))
    direct_df["valuation_family_key"] = direct_df["valuation_family_key"].fillna("per")

    def pack(df: pd.DataFrame) -> dict[str, float]:
        if df.empty:
            return {}
        return {
            "gap": float(df["gap"].median()),
            "bear_ratio": float(df["bear_ratio"].median()),
            "bull_ratio": float(df["bull_ratio"].median()),
            "confidence": float(df["confidence"].median()),
            "count": float(len(df)),
        }

    sector_family_templates: dict[tuple[str, str], dict[str, float]] = {}
    family_templates: dict[str, dict[str, float]] = {}
    for (sector, family), group in direct_df.groupby(["sector", "valuation_family_key"], dropna=False):
        sector_family_templates[(str(sector), str(family))] = pack(group)
    for family, group in direct_df.groupby("valuation_family_key", dropna=False):
        family_templates[str(family)] = pack(group)
    market_template = pack(direct_df)
    if not market_template:
        market_template = {"gap": 0.0, "bear_ratio": 0.9, "bull_ratio": 1.1, "confidence": 0.22}
    return sector_family_templates, family_templates, market_template


def _apply_proxy_coverage(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df.empty:
        return snapshot_df

    sector_family_templates, family_templates, market_template = _build_proxy_templates(snapshot_df)

    def choose_template(row: pd.Series) -> tuple[dict[str, float], str]:
        sector = _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)
        family = _clean_text(row.get("valuation_family_key")) or "per"
        if sector_family_templates.get((sector, family)):
            return sector_family_templates[(sector, family)], "sector"
        if family_templates.get(family):
            return family_templates[family], "family"
        return market_template, "market"

    for idx, row in snapshot_df.iterrows():
        base_price = _safe_float(row.get("fair_value_base"), float("nan"))
        current_price = _safe_float(row.get("current_price"), float("nan"))
        if base_price > 0 or current_price <= 0:
            continue

        template, template_source = choose_template(row)
        raw_gap = _safe_float(template.get("gap"))
        template_confidence = _safe_float(template.get("confidence"), 0.22)
        template_bear_ratio = _clip(template.get("bear_ratio"), 0.72, 0.98)
        template_bull_ratio = _clip(template.get("bull_ratio"), 1.02, 1.36)
        if template_source == "sector":
            gap_scale = 0.70
            confidence_scale = 0.72
            coverage_reason = "섹터 대체모형"
        elif template_source == "family":
            gap_scale = 0.55
            confidence_scale = 0.60
            coverage_reason = "family 대체모형"
        else:
            gap_scale = 0.40
            confidence_scale = 0.48
            coverage_reason = "시장 대체모형"

        proxy_gap = _clip(raw_gap * gap_scale, -12.0, 12.0)
        proxy_base = current_price * (1.0 + (proxy_gap / 100.0))
        proxy_bear = proxy_base * (1.0 - ((1.0 - template_bear_ratio) * 0.75))
        proxy_bull = proxy_base * (1.0 + ((template_bull_ratio - 1.0) * 0.75))
        proxy_confidence = _clip(template_confidence * confidence_scale, 0.12, 0.54)
        if proxy_gap >= 10.0 and proxy_confidence >= 0.32:
            status = "할인 구간"
        elif proxy_gap <= -10.0 and proxy_confidence >= 0.32:
            status = "선반영 구간"
        else:
            status = "중립 구간"

        reason_tokens = _dedupe_texts(
            [
                _clean_text(row.get("valuation_reason_summary")),
                "대체모형 보수 적용",
            ],
            limit=3,
        )
        family = _clean_text(row.get("valuation_family_key")) or "per"
        snapshot_df.at[idx, "fair_value_bear"] = round(min(proxy_bear, proxy_base), 2)
        snapshot_df.at[idx, "fair_value_base"] = round(proxy_base, 2)
        snapshot_df.at[idx, "fair_value_bull"] = round(max(proxy_bull, proxy_base), 2)
        snapshot_df.at[idx, "fair_value_gap_pct"] = round(proxy_gap, 2)
        snapshot_df.at[idx, "fair_value_confidence_score"] = round(proxy_confidence, 4)
        snapshot_df.at[idx, "fair_value_status_label"] = status
        snapshot_df.at[idx, "valuation_primary_method"] = "대체모형"
        snapshot_df.at[idx, "valuation_anchor_mix"] = f"{coverage_reason} 100"
        proxy_group = _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)
        snapshot_df.at[idx, "valuation_peer_group"] = proxy_group if proxy_group != UNCLASSIFIED_SECTOR else _FAMILY_LABELS.get(family, "PER")
        snapshot_df.at[idx, "valuation_reason_summary"] = " / ".join(reason_tokens)
        snapshot_df.at[idx, "valuation_tier"] = "Tier 4"
        snapshot_df.at[idx, "valuation_proxy_used"] = True
        snapshot_df.at[idx, "valuation_coverage_reason"] = coverage_reason
    return snapshot_df


def _cap_price_range(current_price: float, base_price: float, bear_price: float, bull_price: float, *, tier: str) -> tuple[float, float, float]:
    if current_price <= 0 or base_price <= 0:
        return bear_price, base_price, bull_price
    if tier == "Tier 1":
        lower_ratio, upper_ratio = 0.62, 1.60
    elif tier == "Tier 2":
        lower_ratio, upper_ratio = 0.60, 1.55
    elif tier == "Tier 3":
        lower_ratio, upper_ratio = 0.70, 1.45
    else:
        lower_ratio, upper_ratio = 0.78, 1.18

    min_base = current_price * lower_ratio
    max_base = current_price * upper_ratio
    capped_base = _clip(base_price, min_base, max_base)
    capped_bear = min(_clip(bear_price, current_price * (lower_ratio * 0.92), capped_base), capped_base)
    capped_bull = max(_clip(bull_price, capped_base, current_price * (upper_ratio * 1.06)), capped_base)
    return capped_bear, capped_base, capped_bull


def _sector_roe_assumption(sector: Any) -> float:
    normalized = _normalize_sector(sector, UNCLASSIFIED_SECTOR)
    return _FINANCIAL_ROE_ASSUMPTIONS.get(normalized, 0.08)


def _clip_multiple_display(value: Any, family: str) -> float:
    ranges = {
        "pbr_proxy": (0.15, 2.0),
        "ev_ebitda": (2.0, 30.0),
        "psr": (0.3, 20.0),
        "per": (2.0, 40.0),
    }
    low, high = ranges.get(family, (0.5, 40.0))
    return _clip(value, low, high)


def _metric_basis_meta(row: pd.Series, family: str) -> tuple[str, str]:
    def _fallback_from_metric(metric_value: Any) -> tuple[str, str]:
        numeric = _safe_float(metric_value, float("nan"))
        if math.isfinite(numeric) and numeric > 0:
            return ("연환산 분기", "quarter_annualized")
        return ("", "")

    if family == "psr":
        basis_period = _clean_text(row.get("cons_revenue_basis_period"))
        input_source = _clean_text(row.get("cons_revenue_input_source"))
        if basis_period or input_source:
            return (basis_period, input_source)
        return _fallback_from_metric(row.get("cons_psr_raw"))
    if family == "pbr_proxy":
        basis_period = _clean_text(row.get("cons_pbr_basis_period"))
        input_source = _clean_text(row.get("cons_pbr_input_source"))
        if basis_period or input_source:
            return (basis_period, input_source)
        return _fallback_from_metric(row.get("cons_pbr_raw"))
    if family in {"ev_ebitda"}:
        basis_period = _clean_text(row.get("cons_op_basis_period"))
        input_source = _clean_text(row.get("cons_op_input_source"))
        if basis_period or input_source:
            return (basis_period, input_source)
        return _fallback_from_metric(row.get("cons_op_yield_raw"))
    basis_period = _clean_text(row.get("cons_net_basis_period"))
    input_source = _clean_text(row.get("cons_net_input_source"))
    if basis_period or input_source:
        return (basis_period, input_source)
    return _fallback_from_metric(row.get("cons_net_yield_raw"))


def _metric_basis_phrase(family: str, basis_period: str, actual_year: Any = None) -> str:
    normalized_period = _clean_text(basis_period) or "대체모형"
    actual_year_int = _safe_int(actual_year)
    if family == "psr":
        expected_metric = "예상 매출"
        actual_metric = "매출"
    elif family == "pbr_proxy":
        expected_metric = "PBR/ROE 기준"
        actual_metric = "PBR/ROE 기준"
    elif family == "ev_ebitda":
        expected_metric = "예상 영업이익"
        actual_metric = "영업이익"
    else:
        expected_metric = "예상 순이익"
        actual_metric = "순이익"
    if normalized_period == "FY1":
        if actual_year_int:
            return f"{actual_year_int + 2}년 예상 {actual_metric}"
        return f"FY1 {expected_metric}"
    if normalized_period == "FY0":
        if actual_year_int:
            return f"{actual_year_int + 1}년 예상 {actual_metric}"
        return f"FY0 {expected_metric}"
    if normalized_period == "실제 실적":
        if actual_year_int:
            return f"{actual_year_int}년 실제 {actual_metric}"
        return f"최신 확정 {actual_metric}"
    if normalized_period == "실제 실적 + 공시 보정":
        if actual_year_int:
            return f"{actual_year_int}년 실제 {actual_metric}에 최근 공시 보정을 반영한 값"
        return f"최신 확정 {actual_metric}에 최근 공시 보정을 반영한 값"
    if normalized_period == "연환산 분기":
        return f"최근 분기 연환산 {expected_metric}"
    return "섹터 대체모형"


def _build_formula_hint(basis_label: str, family: str, basis_period: str, actual_year: Any = None) -> str:
    basis_phrase = _metric_basis_phrase(family, basis_period, actual_year)
    if family == "pbr_proxy":
        return f"{basis_label} = 현재 PBR 기준입니다. 금융주는 현재 PBR, 기준 PBR, ROE를 함께 읽습니다. 기준 시점은 {basis_phrase}입니다."
    return f"{basis_label} = 현재 시가총액 ÷ {basis_phrase}."


def _build_profitability_hint(family: str, basis_period: str, actual_year: Any = None) -> str:
    basis_phrase = _metric_basis_phrase(family, basis_period, actual_year)
    if family == "pbr_proxy":
        return f"ROE = 자기자본 대비 수익성 기준선입니다. 기준 시점은 {basis_phrase}입니다."
    if family == "psr":
        return f"영업이익률 = {basis_phrase}에서 기대하는 마진 수준입니다."
    return f"예상 영업이익 수익률 = {basis_phrase} ÷ 현재 시가총액입니다."


def _input_source_phrase(value: Any) -> str:
    source = _clean_text(value)
    if source == "annual_consensus":
        return "연간 컨센서스"
    if source == "actual_annual":
        return "실제 실적 snapshot"
    if source == "actual_plus_disclosure":
        return "실제 실적 + 공시 보정"
    if source == "quarter_annualized":
        return "연환산 분기"
    if source == "proxy":
        return "대체모형"
    return source or "입력 불명"


def _format_multiple_text(value: Any) -> str:
    number = _safe_float(value, float("nan"))
    if not math.isfinite(number) or number <= 0:
        return "-"
    return f"{number:.2f}배"


def _format_price_text(value: Any) -> str:
    number = _safe_float(value, float("nan"))
    if not math.isfinite(number) or number <= 0:
        return "-"
    return f"{number:,.0f}원"


def _format_basis_metric_text(label: str, value: Any) -> str:
    number = _safe_float(value, float("nan"))
    if not math.isfinite(number) or number <= 0:
        return "-"
    metric_label = _clean_text(label)
    if any(token in metric_label for token in ("BPS", "EPS", "SPS", "주당")):
        return f"{number:,.2f}원" if number < 1000 else f"{number:,.0f}원"
    return f"{number:,.2f}"


def _has_disclosure_revision(row: pd.Series) -> bool:
    basis_period = _clean_text(row.get("valuation_basis_period"))
    if basis_period not in {"실제 실적", "실제 실적 + 공시 보정"}:
        return False
    event_type = _clean_text(row.get("event_last_type"))
    op_revision_pct = _safe_float(row.get("valuation_revision_op_pct"), float("nan"))
    return event_type in {"SUPPLY_CONTRACT", "SUPPLY_UPDATE", "SUPPLY_TERMINATION", "PERF_PRELIM", "SALES_VARIATION"} and abs(op_revision_pct) >= 0.5


def _build_display_multiple_templates(snapshot_df: pd.DataFrame) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, dict[str, float]]]:
    if snapshot_df.empty:
        return {}, {}

    work = snapshot_df.copy()
    work["sector"] = work["sector"].map(lambda value: _normalize_sector(value, UNCLASSIFIED_SECTOR))
    work["current_price"] = pd.to_numeric(work["current_price"], errors="coerce")
    work["fair_value_base"] = pd.to_numeric(work["fair_value_base"], errors="coerce")
    work["current_multiple_raw"] = pd.to_numeric(work.get("current_multiple_raw"), errors="coerce")
    usable = work[
        work["current_price"].gt(0)
        & work["fair_value_base"].gt(0)
        & work["current_multiple_raw"].gt(0)
    ].copy()
    if usable.empty:
        return {}, {}

    usable["target_multiple"] = usable["current_multiple_raw"] * (usable["fair_value_base"] / usable["current_price"])
    usable = usable[usable["target_multiple"].gt(0)].copy()
    if usable.empty:
        return {}, {}

    def pack(df: pd.DataFrame) -> dict[str, float]:
        return {
            "current_multiple": float(pd.to_numeric(df["current_multiple_raw"], errors="coerce").median()),
            "target_multiple": float(pd.to_numeric(df["target_multiple"], errors="coerce").median()),
        }

    sector_templates: dict[tuple[str, str], dict[str, float]] = {}
    family_templates: dict[str, dict[str, float]] = {}
    for (sector, family), group in usable.groupby(["sector", "valuation_family_key"], dropna=False):
        sector_templates[(str(sector), str(family))] = pack(group)
    for family, group in usable.groupby("valuation_family_key", dropna=False):
        family_templates[str(family)] = pack(group)
    return sector_templates, family_templates


def _basis_is_direct(period: Any) -> bool:
    return _clean_text(period) in _USER_FACING_DIRECT_BASIS


def _winsorized_median(values: list[float]) -> float | None:
    if not values:
        return None
    series = _winsorized_series(values)
    if series.empty:
        return None
    return float(series.median()) if not series.empty else None


def _winsorized_series(values: list[float]) -> pd.Series:
    series = pd.Series(values, dtype="float64").dropna()
    if series.empty:
        return series
    if len(series) >= 5:
        lower = float(series.quantile(0.10))
        upper = float(series.quantile(0.90))
        series = series.clip(lower=lower, upper=upper)
    return series


def _peer_multiple_stats(values: list[float]) -> dict[str, Any]:
    series = _winsorized_series(values)
    if series.empty:
        return {}
    return {
        "median": float(series.median()),
        "q25": float(series.quantile(0.25)),
        "q75": float(series.quantile(0.75)),
        "count": int(len(series)),
    }


def _tp_formula_label(family: str) -> str:
    if family == "pbr_proxy":
        return "기준 PBR × 주당순자산(BPS)"
    if family == "psr":
        return "기준 PSR × 주당매출(SPS)"
    if family == "ev_ebitda":
        return "기준 시가총액/영업이익 × 주당영업이익 환산치"
    return "기준 PER × 주당순이익(EPS)"


def _tp_basis_metric_label(family: str) -> str:
    if family == "pbr_proxy":
        return "주당순자산(BPS)"
    if family == "psr":
        return "주당매출(SPS)"
    if family == "ev_ebitda":
        return "주당영업이익 환산치"
    return "주당순이익(EPS)"


def _tp_basis_metric_value(row: pd.Series, *, current_price: float, current_multiple_raw: float, family: str) -> float | pd._libs.missing.NAType:
    if not math.isfinite(current_price) or current_price <= 0 or not math.isfinite(current_multiple_raw) or current_multiple_raw <= 0:
        return pd.NA
    per_share_value = current_price / current_multiple_raw
    if not math.isfinite(per_share_value) or per_share_value <= 0:
        return pd.NA
    basis_period = _clean_text(row.get("valuation_basis_period"))
    if basis_period == "실제 실적 + 공시 보정":
        if family == "psr":
            revision_pct = _safe_float(row.get("valuation_revision_revenue_pct"))
        elif family == "ev_ebitda":
            revision_pct = _safe_float(row.get("valuation_revision_op_pct"))
        elif family == "per":
            revision_pct = _safe_float(row.get("valuation_revision_net_pct"))
        else:
            revision_pct = 0.0
        per_share_value *= max(0.75, min(1.25, 1.0 + (revision_pct / 100.0)))
    if family == "pbr_proxy":
        return round(per_share_value, 2)
    return round(per_share_value, 4)


def _build_tp_revision_contributors(row: pd.Series) -> list[str]:
    contributors: list[str] = []
    if bool(row.get("valuation_proxy_used")):
        contributors.append("대체모형 보수 적용")
    if _safe_int(row.get("analyst_report_count")) > 0 and _safe_float(row.get("valuation_analyst_target_upside_pct")) >= 5.0:
        contributors.append("목표가 상향")
    if _has_disclosure_revision(row):
        contributors.append("수주/공시 반영")
    if _safe_float(row.get("valuation_revision_op_pct")) >= 3.0 or _safe_float(row.get("valuation_revision_net_pct")) >= 3.0:
        contributors.append("실적 추정 상향")
    current_multiple = _safe_float(row.get("valuation_multiple_current"), float("nan"))
    target_multiple = _safe_float(row.get("valuation_multiple_target"), float("nan"))
    if current_multiple > 0 and target_multiple > 0 and abs((target_multiple / current_multiple) - 1.0) >= 0.07:
        contributors.append("피어 멀티플 재평가")
    return _dedupe_texts(contributors, limit=3)


def _build_tp_hidden_reason(*, row: pd.Series, peer_count: int, basis_period: str, current_multiple_raw: float) -> str:
    if bool(row.get("valuation_proxy_used")):
        return "대체모형이라 기준 적정가 숨김"
    if basis_period == "연환산 분기":
        return "연환산 분기라 기준 적정가 숨김"
    if not _basis_is_direct(basis_period):
        return "FY1/FY0/실제 실적 기준 없음"
    if not math.isfinite(current_multiple_raw) or current_multiple_raw <= 0:
        return "FY1/FY0/실제 실적 기준 없음"
    if peer_count < 3:
        return "직접 피어 3개 미만"
    return ""


def _annual_growth_pct(current_value: Any, base_value: Any) -> float:
    current = _safe_float(current_value, float("nan"))
    base = _safe_float(base_value, float("nan"))
    if not math.isfinite(current) or current <= 0 or not math.isfinite(base) or base <= 0:
        return float("nan")
    return ((current / base) - 1.0) * 100.0


def _subject_growth_signal(row: pd.Series) -> float:
    revenue_growth = _annual_growth_pct(row.get("cons_revenue_fy1_krw_raw"), row.get("cons_revenue_actual_krw_raw"))
    op_growth = _annual_growth_pct(row.get("cons_op_fy1_krw_raw"), row.get("cons_op_actual_krw_raw"))
    signals = [value for value in (revenue_growth, op_growth) if math.isfinite(value)]
    if signals:
        return float(sum(signals) / len(signals))
    revenue_growth = _annual_growth_pct(row.get("cons_revenue_fy0_krw_raw"), row.get("cons_revenue_actual_krw_raw"))
    op_growth = _annual_growth_pct(row.get("cons_op_fy0_krw_raw"), row.get("cons_op_actual_krw_raw"))
    signals = [value for value in (revenue_growth, op_growth) if math.isfinite(value)]
    if signals:
        return float(sum(signals) / len(signals))
    return float("nan")


def _subject_profitability_signal(row: pd.Series, family: str) -> float:
    if family == "pbr_proxy":
        return _safe_float(row.get("roe_current"), float("nan"))
    revenue_fy1 = _safe_float(row.get("cons_revenue_fy1_krw_raw"), float("nan"))
    op_fy1 = _safe_float(row.get("cons_op_fy1_krw_raw"), float("nan"))
    if math.isfinite(revenue_fy1) and revenue_fy1 > 0 and math.isfinite(op_fy1):
        return (op_fy1 / revenue_fy1) * 100.0
    return _safe_float(row.get("operating_margin_pct"), float("nan"))


def _subject_revision_signal(row: pd.Series, family: str) -> float:
    if family == "pbr_proxy":
        return max(
            _safe_float(row.get("valuation_revision_net_pct"), float("nan")),
            _safe_float(row.get("valuation_revision_op_pct"), float("nan")),
        )
    return max(
        _safe_float(row.get("valuation_revision_op_pct"), float("nan")),
        _safe_float(row.get("valuation_revision_revenue_pct"), float("nan")),
    )


def _peer_metric_median(peer_rows: pd.DataFrame, metric_fn) -> float:
    values: list[float] = []
    for _, peer in peer_rows.iterrows():
        value = _safe_float(metric_fn(peer), float("nan"))
        if math.isfinite(value):
            values.append(value)
    return float(pd.Series(values).median()) if values else float("nan")


def _build_target_multiple_adjustment(
    *,
    row: pd.Series,
    peer_rows: pd.DataFrame,
    family: str,
    base_multiple: float,
    q25_multiple: float,
    q75_multiple: float,
) -> dict[str, float]:
    subject_growth = _subject_growth_signal(row)
    peer_growth = _peer_metric_median(peer_rows, _subject_growth_signal)
    subject_profitability = _subject_profitability_signal(row, family)
    peer_profitability = _peer_metric_median(peer_rows, lambda peer: _subject_profitability_signal(peer, family))
    subject_revision = _subject_revision_signal(row, family)
    peer_revision = _peer_metric_median(peer_rows, lambda peer: _subject_revision_signal(peer, family))

    growth_adj = _clip(((subject_growth - peer_growth) / 200.0) if math.isfinite(subject_growth) and math.isfinite(peer_growth) else 0.0, -0.08, 0.08)
    profitability_adj = _clip(
        ((subject_profitability - peer_profitability) / 200.0) if math.isfinite(subject_profitability) and math.isfinite(peer_profitability) else 0.0,
        -0.08,
        0.08,
    )
    revision_adj = _clip(((subject_revision - peer_revision) / 120.0) if math.isfinite(subject_revision) and math.isfinite(peer_revision) else 0.0, -0.05, 0.05)
    mix_adj = 0.0
    total_adj = _clip(growth_adj + profitability_adj + revision_adj + mix_adj, -0.12, 0.12)
    adjusted_multiple = base_multiple * (1.0 + total_adj)
    adjusted_multiple = _clip(adjusted_multiple, q25_multiple, q75_multiple)
    return {
        "growth_adj": round(growth_adj * 100.0, 1),
        "profitability_adj": round(profitability_adj * 100.0, 1),
        "revision_adj": round(revision_adj * 100.0, 1),
        "mix_adj": round(mix_adj * 100.0, 1),
        "total_adj": round(total_adj * 100.0, 1),
        "adjusted_multiple": round(adjusted_multiple, 4),
        "subject_growth": round(subject_growth, 2) if math.isfinite(subject_growth) else pd.NA,
        "peer_growth": round(peer_growth, 2) if math.isfinite(peer_growth) else pd.NA,
        "subject_profitability": round(subject_profitability, 2) if math.isfinite(subject_profitability) else pd.NA,
        "peer_profitability": round(peer_profitability, 2) if math.isfinite(peer_profitability) else pd.NA,
        "subject_revision": round(subject_revision, 2) if math.isfinite(subject_revision) else pd.NA,
        "peer_revision": round(peer_revision, 2) if math.isfinite(peer_revision) else pd.NA,
    }


def _build_direct_peer_lookup(snapshot_df: pd.DataFrame) -> dict[tuple[str, str], list[tuple[str, float]]]:
    if snapshot_df.empty:
        return {}

    lookup: dict[tuple[str, str], list[tuple[str, float]]] = {}
    work = snapshot_df.copy()
    work["sector"] = work["sector"].map(lambda value: _normalize_sector(value, UNCLASSIFIED_SECTOR))
    work["current_multiple_raw"] = pd.to_numeric(work.get("current_multiple_raw"), errors="coerce")
    work["valuation_proxy_used"] = work["valuation_proxy_used"].fillna(False).astype(bool)
    work["valuation_basis_period"] = work["valuation_basis_period"].fillna("").map(_clean_text)
    work["symbol"] = work["symbol"].map(_normalize_symbol)
    eligible = work[
        work["current_multiple_raw"].gt(0)
        & ~work["valuation_proxy_used"]
        & work["valuation_basis_period"].map(_basis_is_direct)
    ].copy()
    if eligible.empty:
        return {}

    for (sector, family), group in eligible.groupby(["sector", "valuation_family_key"], dropna=False):
        rows: list[tuple[str, float]] = []
        for _, peer in group.iterrows():
            symbol = _normalize_symbol(peer.get("symbol"))
            multiple = _safe_float(peer.get("current_multiple_raw"), float("nan"))
            if symbol and math.isfinite(multiple) and multiple > 0:
                rows.append((symbol, multiple))
        if rows:
            lookup[(str(sector), _clean_text(family) or "per")] = rows
    return lookup


def _build_tp_range(
    *,
    row: pd.Series,
    base_price: float,
    current_price: float,
    family: str,
) -> dict[str, Any]:
    revision_abs = max(
        abs(_safe_float(row.get("valuation_revision_op_pct"))),
        abs(_safe_float(row.get("valuation_revision_net_pct"))),
        abs(_safe_float(row.get("valuation_revision_revenue_pct"))),
    )
    width = 0.08 + min(0.05, revision_abs / 200.0)
    if family == "pbr_proxy":
        lower_ratio, upper_ratio = 0.70, 1.30
    else:
        lower_ratio, upper_ratio = 0.65, 1.35
    sanity_low = current_price * lower_ratio
    sanity_high = current_price * upper_ratio
    capped_base = _clip(base_price, sanity_low, sanity_high)
    bear_price = max(capped_base * (1.0 - width), sanity_low)
    bull_price = min(capped_base * (1.0 + width), sanity_high)
    bound_applied = abs(capped_base - base_price) >= max(50.0, current_price * 0.003)
    return {
        "bear_price": round(bear_price, 2),
        "base_price": round(capped_base, 2),
        "bull_price": round(max(bull_price, capped_base), 2),
        "sanity_low_price": round(sanity_low, 2),
        "sanity_high_price": round(sanity_high, 2),
        "sanity_bound_applied": bound_applied,
    }


def _pick_display_template(
    *,
    sector: str,
    family: str,
    sector_templates: dict[tuple[str, str], dict[str, float]],
    family_templates: dict[str, dict[str, float]],
) -> dict[str, float]:
    if sector_templates.get((sector, family)):
        return sector_templates[(sector, family)]
    if family_templates.get(family):
        return family_templates[family]
    return {}


def _derive_display_multiples(
    row: pd.Series,
    *,
    sector_templates: dict[tuple[str, str], dict[str, float]],
    family_templates: dict[str, dict[str, float]],
) -> dict[str, Any]:
    sector = _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)
    family = _clean_text(row.get("valuation_family_key")) or "per"
    basis_label = _BASIS_LABELS.get(family, "PER")
    basis_period, input_source = _metric_basis_meta(row, family)
    actual_year = row.get("cons_actual_year")
    current_multiple = _safe_float(row.get("current_multiple_raw"), float("nan"))
    profitability_label = ""
    profitability_value = float("nan")
    operating_profit_yield_pct = float("nan")
    operating_margin_pct = float("nan")
    roe_current = float("nan")

    if family == "pbr_proxy":
        roe_current = _safe_float(row.get("cons_roe_raw"), float("nan"))
        if not math.isfinite(roe_current) or roe_current <= 0:
            roe_current = _sector_roe_assumption(sector) * 100.0
        profitability_label = "ROE"
        profitability_value = roe_current
    elif family == "psr":
        current_multiple = _safe_float(row.get("cons_psr_raw"), float("nan"))
        if not math.isfinite(current_multiple) or current_multiple <= 0:
            current_multiple = _safe_float(row.get("current_multiple_raw"), float("nan"))
        operating_margin_candidate = _subject_profitability_signal(row, family)
        if math.isfinite(operating_margin_candidate) and operating_margin_candidate > 0:
            operating_margin_pct = operating_margin_candidate
            profitability_label = "영업이익률"
            profitability_value = operating_margin_pct
    else:
        operating_margin_candidate = _subject_profitability_signal(row, family)
        if math.isfinite(operating_margin_candidate) and operating_margin_candidate > 0:
            operating_margin_pct = operating_margin_candidate
        if _safe_float(row.get("cons_op_yield_raw"), float("nan")) > 0:
            operating_profit_yield_pct = _safe_float(row.get("cons_op_yield_raw")) * 100.0
            profitability_label = "예상 영업이익 수익률"
            profitability_value = operating_profit_yield_pct

    if bool(row.get("valuation_proxy_used")):
        basis_period = "대체모형"
        input_source = "proxy"

    if math.isfinite(current_multiple) and current_multiple > 0:
        current_multiple = round(_clip_multiple_display(current_multiple, family), 2)
    else:
        current_multiple = pd.NA

    return {
        "valuation_basis_label": basis_label,
        "valuation_basis_period": basis_period or ("대체모형" if bool(row.get("valuation_proxy_used")) else ""),
        "valuation_input_source": input_source or ("proxy" if bool(row.get("valuation_proxy_used")) else ""),
        "valuation_multiple_current": current_multiple,
        "valuation_multiple_target": pd.NA,
        "valuation_multiple_unit": "배",
        "operating_profit_yield_pct": round(operating_profit_yield_pct, 2) if math.isfinite(operating_profit_yield_pct) and operating_profit_yield_pct > 0 else pd.NA,
        "operating_margin_pct": round(operating_margin_pct, 2) if math.isfinite(operating_margin_pct) and operating_margin_pct > 0 else pd.NA,
        "roe_current": round(roe_current, 2) if math.isfinite(roe_current) and roe_current > 0 else pd.NA,
        "profitability_metric_label": profitability_label,
        "profitability_metric_value": round(profitability_value, 2) if math.isfinite(profitability_value) and profitability_value > 0 else pd.NA,
        "valuation_formula_hint": _build_formula_hint(
            basis_label,
            family,
            basis_period or ("대체모형" if bool(row.get("valuation_proxy_used")) else ""),
            actual_year,
        ),
        "profitability_formula_hint": _build_profitability_hint(
            family,
            basis_period or ("대체모형" if bool(row.get("valuation_proxy_used")) else ""),
            actual_year,
        ),
    }


def _select_driver_reason(row: pd.Series) -> str:
    contributors = _build_tp_revision_contributors(row)
    if contributors:
        return contributors[0]
    return "피어 멀티플 재평가"


def _apply_user_facing_tp_rules(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df.empty:
        return snapshot_df

    out = snapshot_df.copy()
    peer_lookup = _build_direct_peer_lookup(out)

    out["valuation_peer_direct_count"] = 0
    out["valuation_tp_visible"] = False
    out["valuation_tp_hidden_reason"] = ""
    out["tp_formula_label"] = ""
    out["tp_basis_metric_label"] = ""
    out["tp_basis_metric_value"] = pd.NA
    out["tp_peer_median_multiple"] = pd.NA
    out["tp_peer_q25_multiple"] = pd.NA
    out["tp_peer_q75_multiple"] = pd.NA
    out["tp_peer_count_used"] = 0
    out["tp_sanity_low_price"] = pd.NA
    out["tp_sanity_high_price"] = pd.NA
    out["tp_sanity_bound_applied"] = False
    out["tp_revision_contributors"] = ""
    out["tp_growth_adjustment_pct"] = pd.NA
    out["tp_profitability_adjustment_pct"] = pd.NA
    out["tp_revision_adjustment_pct"] = pd.NA
    out["tp_mix_adjustment_pct"] = pd.NA
    out["tp_total_adjustment_pct"] = pd.NA
    out["tp_subject_growth_pct"] = pd.NA
    out["tp_peer_growth_pct"] = pd.NA
    out["tp_subject_profitability_pct"] = pd.NA
    out["tp_peer_profitability_pct"] = pd.NA
    out["tp_subject_revision_pct"] = pd.NA
    out["tp_peer_revision_pct"] = pd.NA

    eligible_direct = out[
        out["current_multiple_raw"].fillna(0).astype(float).gt(0)
        & ~out["valuation_proxy_used"].fillna(False).astype(bool)
        & out["valuation_basis_period"].fillna("").map(_basis_is_direct)
    ].copy()

    for idx, row in out.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        sector = _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)
        family = _clean_text(row.get("valuation_family_key")) or "per"
        basis_period = _clean_text(row.get("valuation_basis_period"))
        current_price = _safe_float(row.get("current_price"), float("nan"))
        current_multiple_raw = _safe_float(row.get("current_multiple_raw"), float("nan"))
        peers = [
            multiple
            for peer_symbol, multiple in peer_lookup.get((sector, family), [])
            if peer_symbol != symbol and math.isfinite(multiple) and multiple > 0
        ]
        peer_count = len(peers)
        out.at[idx, "valuation_peer_direct_count"] = peer_count
        out.at[idx, "tp_formula_label"] = _tp_formula_label(family)
        out.at[idx, "tp_basis_metric_label"] = _tp_basis_metric_label(family)
        out.at[idx, "tp_basis_metric_value"] = _tp_basis_metric_value(
            row,
            current_price=current_price,
            current_multiple_raw=current_multiple_raw,
            family=family,
        )
        peer_stats = _peer_multiple_stats(peers) if peer_count >= 3 else {}
        target_multiple = _safe_float(peer_stats.get("median"), float("nan")) if peer_stats else float("nan")
        if peer_stats:
            out.at[idx, "tp_peer_median_multiple"] = round(_clip_multiple_display(peer_stats.get("median"), family), 2)
            out.at[idx, "tp_peer_q25_multiple"] = round(_clip_multiple_display(peer_stats.get("q25"), family), 2)
            out.at[idx, "tp_peer_q75_multiple"] = round(_clip_multiple_display(peer_stats.get("q75"), family), 2)
            out.at[idx, "tp_peer_count_used"] = _safe_int(peer_stats.get("count"))
        out.at[idx, "tp_revision_contributors"] = " / ".join(_build_tp_revision_contributors(row))

        hidden_reason = _build_tp_hidden_reason(
            row=row,
            peer_count=peer_count,
            basis_period=basis_period,
            current_multiple_raw=current_multiple_raw,
        )
        if hidden_reason:
            out.at[idx, "valuation_tp_visible"] = False
            out.at[idx, "valuation_tp_hidden_reason"] = hidden_reason
            out.at[idx, "valuation_multiple_target"] = pd.NA
            out.at[idx, "fair_value_bear"] = pd.NA
            out.at[idx, "fair_value_base"] = pd.NA
            out.at[idx, "fair_value_bull"] = pd.NA
            out.at[idx, "fair_value_gap_pct"] = pd.NA
            if bool(row.get("valuation_proxy_used")):
                out.at[idx, "fair_value_status_label"] = "대체모형"
            else:
                out.at[idx, "fair_value_status_label"] = hidden_reason
            continue

        peer_rows = eligible_direct[
            (eligible_direct["symbol"].map(_normalize_symbol) != symbol)
            & (
                (
                    eligible_direct["sector"].map(lambda value: _normalize_sector(value, UNCLASSIFIED_SECTOR))
                    == sector
                )
                | (
                    (sector == UNCLASSIFIED_SECTOR)
                    & (eligible_direct["valuation_family_key"].fillna("").map(_clean_text) == family)
                )
            )
        ].copy()
        q25_multiple = _safe_float(peer_stats.get("q25"), float("nan")) if peer_stats else float("nan")
        q75_multiple = _safe_float(peer_stats.get("q75"), float("nan")) if peer_stats else float("nan")
        adjustment = _build_target_multiple_adjustment(
            row=row,
            peer_rows=peer_rows,
            family=family,
            base_multiple=target_multiple,
            q25_multiple=q25_multiple,
            q75_multiple=q75_multiple,
        )
        target_multiple = _safe_float(adjustment.get("adjusted_multiple"), target_multiple)
        out.at[idx, "tp_growth_adjustment_pct"] = adjustment.get("growth_adj")
        out.at[idx, "tp_profitability_adjustment_pct"] = adjustment.get("profitability_adj")
        out.at[idx, "tp_revision_adjustment_pct"] = adjustment.get("revision_adj")
        out.at[idx, "tp_mix_adjustment_pct"] = adjustment.get("mix_adj")
        out.at[idx, "tp_total_adjustment_pct"] = adjustment.get("total_adj")
        out.at[idx, "tp_subject_growth_pct"] = adjustment.get("subject_growth")
        out.at[idx, "tp_peer_growth_pct"] = adjustment.get("peer_growth")
        out.at[idx, "tp_subject_profitability_pct"] = adjustment.get("subject_profitability")
        out.at[idx, "tp_peer_profitability_pct"] = adjustment.get("peer_profitability")
        out.at[idx, "tp_subject_revision_pct"] = adjustment.get("subject_revision")
        out.at[idx, "tp_peer_revision_pct"] = adjustment.get("peer_revision")
        target_multiple = round(_clip_multiple_display(target_multiple, family), 2)
        out.at[idx, "valuation_multiple_target"] = target_multiple
        basis_metric_value = _safe_float(out.at[idx, "tp_basis_metric_value"], float("nan"))
        if not math.isfinite(basis_metric_value) or basis_metric_value <= 0:
            out.at[idx, "valuation_tp_visible"] = False
            out.at[idx, "valuation_tp_hidden_reason"] = "FY1/FY0/실제 실적 기준 없음"
            out.at[idx, "valuation_multiple_target"] = pd.NA
            out.at[idx, "fair_value_bear"] = pd.NA
            out.at[idx, "fair_value_base"] = pd.NA
            out.at[idx, "fair_value_bull"] = pd.NA
            out.at[idx, "fair_value_gap_pct"] = pd.NA
            out.at[idx, "fair_value_status_label"] = "FY1/FY0/실제 실적 기준 없음"
            continue
        base_price = target_multiple * basis_metric_value
        tp_range = _build_tp_range(
            row=out.loc[idx],
            base_price=base_price,
            current_price=current_price,
            family=family,
        )
        gap_pct = ((tp_range["base_price"] / current_price) - 1.0) * 100.0 if current_price > 0 else float("nan")
        out.at[idx, "fair_value_bear"] = tp_range["bear_price"]
        out.at[idx, "fair_value_base"] = tp_range["base_price"]
        out.at[idx, "fair_value_bull"] = tp_range["bull_price"]
        out.at[idx, "fair_value_gap_pct"] = round(gap_pct, 2) if math.isfinite(gap_pct) else pd.NA
        out.at[idx, "tp_sanity_low_price"] = tp_range["sanity_low_price"]
        out.at[idx, "tp_sanity_high_price"] = tp_range["sanity_high_price"]
        out.at[idx, "tp_sanity_bound_applied"] = bool(tp_range["sanity_bound_applied"])
        out.at[idx, "valuation_tp_visible"] = True
        out.at[idx, "valuation_tp_hidden_reason"] = ""
        if math.isfinite(gap_pct) and gap_pct >= 10.0:
            out.at[idx, "fair_value_status_label"] = "TP 표시 가능"
        elif math.isfinite(gap_pct) and gap_pct <= -10.0:
            out.at[idx, "fair_value_status_label"] = "TP 표시 가능"
        else:
            out.at[idx, "fair_value_status_label"] = "TP 표시 가능"
    return out


def _build_valuation_method_detail(row: pd.Series) -> str:
    basis = _clean_text(row.get("valuation_basis_label")) or "주력 멀티플"
    family = _clean_text(row.get("valuation_family_key")) or "per"
    basis_period = _clean_text(row.get("valuation_basis_period")) or "대체모형"
    basis_phrase = _metric_basis_phrase(family, basis_period)
    hidden_reason = _clean_text(row.get("valuation_tp_hidden_reason"))
    formula_label = _clean_text(row.get("tp_formula_label")) or _tp_formula_label(family)
    peer_count = _safe_int(row.get("tp_peer_count_used"))
    peer_median = _safe_float(row.get("tp_peer_median_multiple"), float("nan"))
    peer_q25 = _safe_float(row.get("tp_peer_q25_multiple"), float("nan"))
    peer_q75 = _safe_float(row.get("tp_peer_q75_multiple"), float("nan"))
    contributors = _clean_text(row.get("tp_revision_contributors"))
    sanity_applied = bool(row.get("tp_sanity_bound_applied"))
    sanity_low = _safe_float(row.get("tp_sanity_low_price"), float("nan"))
    sanity_high = _safe_float(row.get("tp_sanity_high_price"), float("nan"))
    if hidden_reason:
        return f"{formula_label} 식으로 {basis_phrase} 기준선은 계산했지만, {hidden_reason} 상태라 기준 적정가는 화면에서 숨깁니다."
    parts = [f"TP 본식은 {formula_label}이다."]
    parts.append(f"기준 연도/입력은 {basis_phrase}이다.")
    if peer_count > 0 and peer_median > 0:
        range_text = f"직접 피어 {peer_count}개의 현재 멀티플을 10~90% winsorize한 뒤 중앙값 {peer_median:.2f}배"
        if peer_q25 > 0 and peer_q75 > 0:
            range_text += f", 분위수 범위 {peer_q25:.2f}~{peer_q75:.2f}배"
        parts.append(f"{range_text}를 기준 멀티플로 썼다.")
    if sanity_low > 0 and sanity_high > 0:
        parts.append(
            f"sanity bound는 현재가 대비 {sanity_low:,.0f}원~{sanity_high:,.0f}원이고"
            f"{' 실제로 적용됐다' if sanity_applied else ' 이번 계산에서는 바운드 안에 있었다'}."
        )
    if contributors:
        parts.append(f"변경 기여 요인은 {contributors}이다.")
    return " ".join(parts)


def _build_tp_basis_summary(row: pd.Series) -> str:
    family = _clean_text(row.get("valuation_family_key")) or "per"
    basis_period = _clean_text(row.get("valuation_basis_period")) or "대체모형"
    basis_phrase = _metric_basis_phrase(family, basis_period, row.get("cons_actual_year"))
    basis_metric_label = _clean_text(row.get("tp_basis_metric_label")) or _tp_basis_metric_label(family)
    basis_metric_text = _format_basis_metric_text(basis_metric_label, row.get("tp_basis_metric_value"))
    input_source = _input_source_phrase(row.get("valuation_input_source"))
    if basis_metric_text == "-":
        return f"기준 연도는 {basis_phrase}이고 입력 기준은 {input_source}다."
    return f"기준 연도는 {basis_phrase}이고, {basis_metric_label} {basis_metric_text}을 입력 기준 {input_source}에서 사용했다."


def _build_tp_peer_set_summary(row: pd.Series) -> str:
    peer_count = _safe_int(row.get("tp_peer_count_used"))
    peer_median = _safe_float(row.get("tp_peer_median_multiple"), float("nan"))
    peer_q25 = _safe_float(row.get("tp_peer_q25_multiple"), float("nan"))
    peer_q75 = _safe_float(row.get("tp_peer_q75_multiple"), float("nan"))
    adjusted_target = _safe_float(row.get("valuation_multiple_target"), float("nan"))
    total_adjustment = _safe_float(row.get("tp_total_adjustment_pct"), float("nan"))
    if peer_count < 3 or not math.isfinite(peer_median) or peer_median <= 0:
        if peer_count > 0:
            return f"직접 피어는 {peer_count}개까지 확보됐지만 사용자 TP 기준으로 쓰기에는 부족했다."
        return "직접 비교 가능한 피어가 아직 충분하지 않다."
    adjustment_note = ""
    if math.isfinite(adjusted_target) and adjusted_target > 0 and abs(adjusted_target - peer_median) >= 0.05:
        direction = "상향" if adjusted_target > peer_median else "하향"
        adjustment_note = f" 성장·수익성·리비전 조정을 거쳐 기준 멀티플은 {adjusted_target:.2f}배({direction}, {total_adjustment:+.1f}%p)로 잡았다."
    if peer_q25 > 0 and peer_q75 > 0:
        return f"직접 피어 {peer_count}개의 현재 멀티플을 winsorize해 중앙값 {peer_median:.2f}배, 분위수 범위 {peer_q25:.2f}~{peer_q75:.2f}배를 기준선으로 썼다.{adjustment_note}".strip()
    return f"직접 피어 {peer_count}개의 현재 멀티플 중앙값 {peer_median:.2f}배를 기준선으로 썼다.{adjustment_note}".strip()


def _build_tp_bound_summary(row: pd.Series) -> str:
    low = _safe_float(row.get("tp_sanity_low_price"), float("nan"))
    high = _safe_float(row.get("tp_sanity_high_price"), float("nan"))
    if not math.isfinite(low) or low <= 0 or not math.isfinite(high) or high <= 0:
        return ""
    if bool(row.get("tp_sanity_bound_applied")):
        return f"sanity bound는 {_format_price_text(low)} ~ {_format_price_text(high)}이고, 이번 기준 적정가에는 바운드가 실제로 적용됐다."
    return f"sanity bound는 {_format_price_text(low)} ~ {_format_price_text(high)}이고, 이번 계산은 그 범위 안에 있었다."


def _build_tp_hidden_reason_detail(row: pd.Series) -> str:
    hidden_reason = _clean_text(row.get("valuation_tp_hidden_reason"))
    if not hidden_reason:
        return ""
    family = _clean_text(row.get("valuation_family_key")) or "per"
    basis = _clean_text(row.get("valuation_basis_label")) or _BASIS_LABELS.get(family, "PER")
    current_multiple = _safe_float(row.get("valuation_multiple_current"), float("nan"))
    peer_count = _safe_int(row.get("tp_peer_count_used")) or _safe_int(row.get("valuation_peer_direct_count"))
    basis_period = _clean_text(row.get("valuation_basis_period")) or "대체모형"
    basis_phrase = _metric_basis_phrase(family, basis_period, row.get("cons_actual_year"))
    if hidden_reason == "직접 피어 3개 미만":
        prefix = f"현재 {basis} {_format_multiple_text(current_multiple)}까지는 계산되지만, " if current_multiple > 0 else ""
        return f"{prefix}같은 섹터·같은 family에서 직접 비교 가능한 피어가 {peer_count}개라 사용자 TP는 숨긴다."
    if hidden_reason == "FY1/FY0/실제 실적 기준 없음":
        return f"피어 후보는 있어도 {basis_phrase}처럼 직접 설명 가능한 기준 연도가 없어 사용자 TP를 숨긴다."
    if hidden_reason == "연환산 분기라 기준 적정가 숨김":
        return "현재 멀티플과 피어 설명은 유지하지만, 연환산 분기 기준은 변동성이 커 사용자 TP는 숨긴다."
    if hidden_reason == "대체모형이라 기준 적정가 숨김":
        return "현재 값은 대체모형과 섹터 기준선을 참고한 설명용 기준이라 사용자 TP는 노출하지 않는다."
    return hidden_reason


def _build_tp_explanation_steps(row: pd.Series) -> list[str]:
    family = _clean_text(row.get("valuation_family_key")) or "per"
    basis = _clean_text(row.get("valuation_basis_label")) or _BASIS_LABELS.get(family, "PER")
    current_multiple = _safe_float(row.get("valuation_multiple_current"), float("nan"))
    target_multiple = _safe_float(row.get("valuation_multiple_target"), float("nan"))
    contributors = _clean_text(row.get("tp_revision_contributors"))
    steps: list[str] = []
    if current_multiple > 0:
        if target_multiple > 0:
            steps.append(f"현재 멀티플은 {basis} {current_multiple:.2f}배이고, 기준 멀티플은 {target_multiple:.2f}배로 잡았다.")
        else:
            steps.append(f"현재 멀티플은 {basis} {current_multiple:.2f}배로 계산했다.")
    basis_summary = _build_tp_basis_summary(row)
    if basis_summary:
        steps.append(basis_summary)
    peer_summary = _build_tp_peer_set_summary(row)
    if peer_summary:
        steps.append(peer_summary)
    bound_summary = _build_tp_bound_summary(row)
    if bound_summary:
        steps.append(bound_summary)
    if contributors:
        steps.append(f"최근 변경 기여 요인은 {contributors}이다.")
    hidden_detail = _build_tp_hidden_reason_detail(row)
    if hidden_detail:
        steps.append(hidden_detail)
    return steps[:5]


def _build_valuation_summary_paragraph(row: pd.Series) -> str:
    basis = _clean_text(row.get("valuation_basis_label")) or "주력 멀티플"
    family = _clean_text(row.get("valuation_family_key")) or "per"
    basis_period = _clean_text(row.get("valuation_basis_period")) or ("대체모형" if bool(row.get("valuation_proxy_used")) else "")
    basis_phrase = _metric_basis_phrase(family, basis_period, row.get("cons_actual_year"))
    current_multiple = _safe_float(row.get("valuation_multiple_current"), float("nan"))
    target_multiple = _safe_float(row.get("valuation_multiple_target"), float("nan"))
    peer_group = _clean_text(row.get("valuation_peer_group"))
    driver = _clean_text(row.get("valuation_driver")) or "기준 재조정"
    profitability_label = _clean_text(row.get("profitability_metric_label"))
    profitability_value = _safe_float(row.get("profitability_metric_value"), float("nan"))
    hidden_reason = _clean_text(row.get("valuation_tp_hidden_reason"))
    peer_median = _safe_float(row.get("tp_peer_median_multiple"), float("nan"))
    peer_q25 = _safe_float(row.get("tp_peer_q25_multiple"), float("nan"))
    peer_q75 = _safe_float(row.get("tp_peer_q75_multiple"), float("nan"))
    peer_count = _safe_int(row.get("tp_peer_count_used"))
    contributors = _clean_text(row.get("tp_revision_contributors"))
    if not contributors:
        contributors = _clean_text(row.get("valuation_driver")) or "기준 재조정"

    parts: list[str] = []
    if hidden_reason:
        if current_multiple > 0:
            parts.append(f"현재 {basis} {current_multiple:.2f}배다.")
        if basis_period:
            parts.append(f"계산 기준은 {basis_phrase}이다.")
        if profitability_label and profitability_value > 0:
            if profitability_label == "ROE":
                parts.append(f"{profitability_label} 기준선은 {profitability_value:.1f}%다.")
            else:
                parts.append(f"{profitability_label}은 {profitability_value:.1f}%다.")
        parts.append(f"{hidden_reason}.")
        if contributors:
            parts.append(f"최근 변경 이유는 {contributors}이다.")
        return " ".join(parts)

    if current_multiple > 0 and target_multiple > 0:
        ratio = target_multiple / current_multiple if current_multiple > 0 else 1.0
        if ratio >= 1.07:
            relation = "피어 기준선보다 낮다"
        elif ratio <= 0.93:
            relation = "피어 기준선보다 높다"
        else:
            relation = "피어 기준선과 비슷하다"
        parts.append(f"현재 {basis} {current_multiple:.2f}배, 기준 {target_multiple:.2f}배로 {relation}.")
    if peer_median > 0 and target_multiple > 0 and abs(target_multiple - peer_median) >= 0.05:
        adjustment_pct = _safe_float(row.get("tp_total_adjustment_pct"), float("nan"))
        parts.append(f"피어 중앙값 {peer_median:.2f}배에서 성장·수익성·리비전 요인을 반영해 기준 멀티플을 {target_multiple:.2f}배로 조정했다{f' ({adjustment_pct:+.1f}%p)' if math.isfinite(adjustment_pct) else ''}.")
    if basis_period:
        if basis_period == "대체모형":
            parts.append("직접 입력이 부족해 대체모형 기준으로 계산했다.")
        else:
            parts.append(f"계산 기준은 {basis_phrase}이다.")
    if peer_count > 0 and peer_median > 0:
        if peer_q25 > 0 and peer_q75 > 0:
            parts.append(f"직접 피어 {peer_count}개의 멀티플 범위는 {peer_q25:.2f}~{peer_q75:.2f}배이고 중앙값은 {peer_median:.2f}배다.")
        else:
            parts.append(f"직접 피어 {peer_count}개의 멀티플 중앙값은 {peer_median:.2f}배다.")
    if profitability_label and profitability_value > 0:
        if profitability_label == "ROE":
            parts.append(f"{profitability_label} 기준선은 {profitability_value:.1f}%다.")
        else:
            parts.append(f"{profitability_label}은 {profitability_value:.1f}%다.")
    if peer_group:
        parts.append(f"비교 그룹은 {peer_group}이다.")
    parts.append(f"최근 적정가 변화의 주원인은 {contributors or driver}이다.")
    return " ".join(parts)


def _decorate_valuation_display(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df.empty:
        return snapshot_df

    out = snapshot_df.copy()
    for col in ["tp_basis_summary", "tp_peer_set_summary", "tp_bound_summary", "tp_hidden_reason_detail"]:
        if col not in out.columns:
            out[col] = ""
    if "tp_explanation_steps" not in out.columns:
        out["tp_explanation_steps"] = pd.Series([[] for _ in range(len(out))], index=out.index, dtype=object)
    for idx, row in out.iterrows():
        derived = _derive_display_multiples(row, sector_templates={}, family_templates={})
        for key, value in derived.items():
            out.at[idx, key] = value
        if _has_disclosure_revision(out.loc[idx]):
            family = _clean_text(out.at[idx, "valuation_family_key"]) or "per"
            basis_label = _clean_text(out.at[idx, "valuation_basis_label"]) or "PER"
            out.at[idx, "valuation_basis_period"] = "실제 실적 + 공시 보정"
            out.at[idx, "valuation_input_source"] = "actual_plus_disclosure"
            out.at[idx, "valuation_formula_hint"] = _build_formula_hint(
                basis_label,
                family,
                "실제 실적 + 공시 보정",
                out.loc[idx].get("cons_actual_year"),
            )
            out.at[idx, "profitability_formula_hint"] = _build_profitability_hint(
                family,
                "실제 실적 + 공시 보정",
                out.loc[idx].get("cons_actual_year"),
            )
    out = _apply_user_facing_tp_rules(out)
    for idx, _ in out.iterrows():
        out.at[idx, "valuation_driver"] = _select_driver_reason(out.loc[idx])
        out.at[idx, "valuation_method_detail"] = _build_valuation_method_detail(out.loc[idx])
        out.at[idx, "valuation_summary_paragraph"] = _build_valuation_summary_paragraph(out.loc[idx])
        out.at[idx, "tp_basis_summary"] = _build_tp_basis_summary(out.loc[idx])
        out.at[idx, "tp_peer_set_summary"] = _build_tp_peer_set_summary(out.loc[idx])
        out.at[idx, "tp_bound_summary"] = _build_tp_bound_summary(out.loc[idx])
        out.at[idx, "tp_hidden_reason_detail"] = _build_tp_hidden_reason_detail(out.loc[idx])
        out.at[idx, "tp_explanation_steps"] = _build_tp_explanation_steps(out.loc[idx])
    return out


def build_fair_value_snapshot(*, analyst_days: int = 30, event_days: int = 45) -> pd.DataFrame:
    listing_df = _load_listing_frame()
    if listing_df.empty:
        return pd.DataFrame()

    factor_df = _load_factor_snapshot_frame()
    quote_df = _load_quote_frame()
    actual_financial_df = _load_actual_financial_frame()
    cards_df = _load_cards_sector_frame()
    direct_consensus_df = _load_direct_consensus_frame(set(listing_df["symbol"].astype(str)))
    analyst_df = load_analyst_frame(days=analyst_days)
    event_df = load_event_frame(days=event_days)
    if analyst_df is None:
        analyst_df = pd.DataFrame()
    if event_df is None:
        event_df = pd.DataFrame()
    if not analyst_df.empty:
        analyst_df = analyst_df.copy()
        analyst_df["symbol"] = analyst_df["symbol"].astype(str).str.zfill(6)
        if "sector" in analyst_df.columns:
            analyst_df["sector"] = analyst_df["sector"].map(lambda value: _normalize_sector(value))
    if not event_df.empty:
        event_df = event_df.copy()
        event_df["symbol"] = event_df["symbol"].astype(str).str.zfill(6)
        if "event_sector" in event_df.columns:
            event_df["event_sector"] = event_df["event_sector"].map(lambda value: _normalize_sector(value))

    sector_thesis_map = _load_sector_thesis_map()
    macro_sector_scores = _load_macro_sector_scores()
    wics_meta = load_effective_wics_sector_meta()
    sector_map, sector_source_map = _build_sector_reference_maps(
        listing_df=listing_df,
        factor_df=factor_df,
        cards_df=cards_df,
        analyst_df=analyst_df,
        event_df=event_df,
    )

    base_df = listing_df.copy()
    base_df["sector"] = base_df["symbol"].map(lambda symbol: sector_map.get(symbol, UNCLASSIFIED_SECTOR))
    base_df["sector_source"] = base_df["symbol"].map(lambda symbol: sector_source_map.get(symbol, "default"))
    base_df["close"] = pd.to_numeric(base_df.get("close"), errors="coerce")
    base_df["marcap"] = pd.to_numeric(base_df.get("marcap"), errors="coerce")
    base_df["liquidity_eligible"] = base_df["excluded_reason"].eq("")
    base_df["preferred_share_penalty"] = base_df["name"].map(_preferred_share_penalty)
    base_df["valuation_family"] = base_df["sector"].map(_valuation_family)
    base_df["current_multiple"] = pd.NA
    for col in [
        "current_price_source",
        "current_price_captured_at",
        "current_price_freshness",
        "current_price_status",
        "official_close",
        "official_close_date",
    ]:
        base_df[col] = ""
    try:
        listing_close_date = datetime.fromtimestamp(os.path.getmtime(LISTING_PATH)).date().isoformat()
    except Exception:
        listing_close_date = ""
    base_df["current_price_source"] = "krx_listing_close"
    base_df["current_price_freshness"] = "official_close"
    base_df["current_price_status"] = "공식종가 fallback"
    base_df["official_close"] = pd.to_numeric(base_df["close"], errors="coerce")
    base_df["official_close_date"] = listing_close_date
    if _is_stale_official_close(listing_close_date):
        base_df["close"] = pd.NA
        base_df["current_price_freshness"] = "official_close_stale"
        base_df["current_price_status"] = "업데이트 지연"
        base_df["current_price_captured_at"] = listing_close_date
    for col in [
        "cons_revenue_q_krw",
        "cons_op_q_krw",
        "cons_net_q_krw",
        "cons_revenue_q_annualized_krw",
        "cons_op_q_annualized_krw",
        "cons_net_q_annualized_krw",
        "cons_revenue_y_krw",
        "cons_op_y_krw",
        "cons_net_y_krw",
        "cons_revenue_fy0_krw",
        "cons_op_fy0_krw",
        "cons_net_fy0_krw",
        "cons_revenue_fy1_krw",
        "cons_op_fy1_krw",
        "cons_net_fy1_krw",
        "cons_revenue_actual_krw",
        "cons_op_actual_krw",
        "cons_net_actual_krw",
        "cons_psr",
        "cons_pbr",
        "cons_roe",
        "cons_op_yield",
        "cons_net_yield",
        "cons_op_margin_q",
        "cons_net_margin_q",
    ]:
        base_df[col] = pd.NA
    for col in [
        "cons_revenue_basis_period",
        "cons_op_basis_period",
        "cons_net_basis_period",
        "cons_pbr_basis_period",
        "cons_revenue_input_source",
        "cons_op_input_source",
        "cons_net_input_source",
        "cons_pbr_input_source",
    ]:
        base_df[col] = ""
    base_df["cons_actual_year"] = pd.NA

    if not direct_consensus_df.empty:
        base_df = base_df.merge(direct_consensus_df, how="left", on="symbol")
        for metric in ("revenue", "op", "net"):
            direct_q_col = f"direct_cons_{metric}_q_krw"
            direct_q_annualized_col = f"direct_cons_{metric}_q_annualized_krw"
            direct_fy0_col = f"direct_cons_{metric}_fy0_krw"
            direct_fy1_col = f"direct_cons_{metric}_fy1_krw"
            direct_actual_col = f"direct_cons_{metric}_actual_krw"
            base_df[direct_q_annualized_col] = pd.to_numeric(base_df[direct_q_col], errors="coerce") * 4
            direct_value = base_df.apply(
                lambda item: _pick_metric_value(
                    item.get(direct_fy1_col),
                    item.get(direct_fy0_col),
                    item.get(direct_actual_col),
                    item.get(direct_q_annualized_col),
                ),
                axis=1,
                result_type="expand",
            )
            direct_value.columns = [f"direct_cons_{metric}_selected", f"direct_cons_{metric}_basis_period", f"direct_cons_{metric}_input_source"]
            base_df = pd.concat([base_df, direct_value], axis=1)

            selected_col = f"direct_cons_{metric}_selected"
            value_col = f"cons_{metric}_y_krw"
            base_df[value_col] = pd.to_numeric(base_df[selected_col], errors="coerce").combine_first(pd.to_numeric(base_df[value_col], errors="coerce"))
            for suffix in ("q_krw", "q_annualized_krw", "fy0_krw", "fy1_krw", "actual_krw"):
                src = f"direct_cons_{metric}_{suffix}"
                dst = f"cons_{metric}_{suffix}"
                if src in base_df.columns:
                    base_df[dst] = pd.to_numeric(base_df[src], errors="coerce").combine_first(pd.to_numeric(base_df[dst], errors="coerce"))
            basis_col = f"cons_{metric}_basis_period"
            input_col = f"cons_{metric}_input_source"
            direct_basis_col = f"direct_cons_{metric}_basis_period"
            direct_input_col = f"direct_cons_{metric}_input_source"
            base_df[basis_col] = base_df[direct_basis_col].where(base_df[direct_basis_col].astype(str).ne(""), base_df[basis_col])
            base_df[input_col] = base_df[direct_input_col].where(base_df[direct_input_col].astype(str).ne(""), base_df[input_col])
        if "direct_cons_actual_year" in base_df.columns:
            base_df["cons_actual_year"] = pd.to_numeric(base_df["direct_cons_actual_year"], errors="coerce").combine_first(pd.to_numeric(base_df["cons_actual_year"], errors="coerce"))
        direct_pbr_value = base_df.apply(
            lambda item: _pick_ratio_value(
                item.get("direct_cons_pbr_fy1"),
                item.get("direct_cons_pbr_fy0"),
                item.get("direct_cons_pbr_actual"),
            ),
            axis=1,
            result_type="expand",
        )
        direct_pbr_value.columns = ["direct_cons_pbr_selected", "direct_cons_pbr_basis_period", "direct_cons_pbr_input_source"]
        base_df = pd.concat([base_df, direct_pbr_value], axis=1)
        base_df["cons_pbr"] = pd.to_numeric(base_df["direct_cons_pbr_selected"], errors="coerce").combine_first(pd.to_numeric(base_df["cons_pbr"], errors="coerce"))
        base_df["cons_pbr_basis_period"] = base_df["direct_cons_pbr_basis_period"].where(base_df["direct_cons_pbr_basis_period"].astype(str).ne(""), base_df["cons_pbr_basis_period"])
        base_df["cons_pbr_input_source"] = base_df["direct_cons_pbr_input_source"].where(base_df["direct_cons_pbr_input_source"].astype(str).ne(""), base_df["cons_pbr_input_source"])

        direct_roe_value = base_df.apply(
            lambda item: _pick_ratio_value(
                item.get("direct_cons_roe_fy1"),
                item.get("direct_cons_roe_fy0"),
                item.get("direct_cons_roe_actual"),
            ),
            axis=1,
            result_type="expand",
        )
        direct_roe_value.columns = ["direct_cons_roe_selected", "direct_cons_roe_basis_period", "direct_cons_roe_input_source"]
        base_df = pd.concat([base_df, direct_roe_value], axis=1)
        base_df["cons_roe"] = pd.to_numeric(base_df["direct_cons_roe_selected"], errors="coerce").combine_first(pd.to_numeric(base_df["cons_roe"], errors="coerce"))

    if not actual_financial_df.empty:
        actual_df = actual_financial_df.rename(
            columns={
                "actual_revenue_krw": "snapshot_actual_revenue_krw",
                "actual_op_krw": "snapshot_actual_op_krw",
                "actual_net_krw": "snapshot_actual_net_krw",
                "actual_pbr": "snapshot_actual_pbr",
                "actual_roe": "snapshot_actual_roe",
                "actual_year": "snapshot_actual_year",
                "source": "snapshot_actual_source",
            }
        )
        base_df = base_df.merge(actual_df, how="left", on="symbol")
        for src, dst in (
            ("snapshot_actual_revenue_krw", "cons_revenue_actual_krw"),
            ("snapshot_actual_op_krw", "cons_op_actual_krw"),
            ("snapshot_actual_net_krw", "cons_net_actual_krw"),
            ("snapshot_actual_pbr", "cons_pbr"),
            ("snapshot_actual_roe", "cons_roe"),
        ):
            if src in base_df.columns:
                base_df[dst] = pd.to_numeric(base_df[dst], errors="coerce").combine_first(pd.to_numeric(base_df[src], errors="coerce"))
        if "snapshot_actual_year" in base_df.columns:
            base_df["cons_actual_year"] = pd.to_numeric(base_df["cons_actual_year"], errors="coerce").combine_first(pd.to_numeric(base_df["snapshot_actual_year"], errors="coerce"))

    if not factor_df.empty:
        factor_cols = [
            col
            for col in [
                "symbol",
                "sector",
                "valuation_family",
                "cons_revenue_q_krw",
                "cons_op_q_krw",
                "cons_net_q_krw",
                "cons_revenue_q_annualized_krw",
                "cons_op_q_annualized_krw",
                "cons_net_q_annualized_krw",
                "cons_revenue_y_krw",
                "cons_op_y_krw",
                "cons_net_y_krw",
                "cons_revenue_fy0_krw",
                "cons_op_fy0_krw",
                "cons_net_fy0_krw",
                "cons_revenue_fy1_krw",
                "cons_op_fy1_krw",
                "cons_net_fy1_krw",
                "cons_revenue_actual_krw",
                "cons_op_actual_krw",
                "cons_net_actual_krw",
                "cons_revenue_basis_period",
                "cons_op_basis_period",
                "cons_net_basis_period",
                "cons_revenue_input_source",
                "cons_op_input_source",
                "cons_net_input_source",
                "cons_psr",
                "cons_pbr",
                "cons_roe",
                "cons_op_yield",
                "cons_net_yield",
                "cons_op_margin_q",
                "cons_net_margin_q",
                "cons_actual_year",
            ]
            if col in factor_df.columns
        ]
        factor_merge_df = factor_df[factor_cols].copy().add_suffix("_factor")
        factor_merge_df = factor_merge_df.rename(columns={"symbol_factor": "symbol"})
        base_df = base_df.merge(factor_merge_df, how="left", on="symbol")
        if "sector_factor" in base_df.columns:
            base_df["sector"] = base_df["sector"].where(base_df["sector"].astype(str).ne(UNCLASSIFIED_SECTOR), base_df["sector_factor"].map(lambda value: _normalize_sector(value, UNCLASSIFIED_SECTOR)))
        if "valuation_family_factor" in base_df.columns:
            base_df["valuation_family"] = base_df["valuation_family"].where(base_df["valuation_family"].astype(str).ne("per"), base_df["valuation_family_factor"])
        for column in [
            "cons_revenue_q_krw",
            "cons_op_q_krw",
            "cons_net_q_krw",
            "cons_revenue_q_annualized_krw",
            "cons_op_q_annualized_krw",
            "cons_net_q_annualized_krw",
            "cons_revenue_y_krw",
            "cons_op_y_krw",
            "cons_net_y_krw",
            "cons_revenue_fy0_krw",
            "cons_op_fy0_krw",
            "cons_net_fy0_krw",
            "cons_revenue_fy1_krw",
            "cons_op_fy1_krw",
            "cons_net_fy1_krw",
            "cons_revenue_actual_krw",
            "cons_op_actual_krw",
            "cons_net_actual_krw",
            "cons_revenue_basis_period",
            "cons_op_basis_period",
            "cons_net_basis_period",
            "cons_pbr_basis_period",
            "cons_revenue_input_source",
            "cons_op_input_source",
            "cons_net_input_source",
            "cons_pbr_input_source",
            "cons_psr",
            "cons_pbr",
            "cons_roe",
            "cons_op_yield",
            "cons_net_yield",
            "cons_op_margin_q",
            "cons_net_margin_q",
            "cons_actual_year",
        ]:
            factor_col = f"{column}_factor"
            if factor_col in base_df.columns:
                base_df[column] = pd.to_numeric(base_df[column], errors="coerce").combine_first(pd.to_numeric(base_df[factor_col], errors="coerce")) if column not in {
                    "cons_revenue_basis_period",
                    "cons_op_basis_period",
                    "cons_net_basis_period",
                    "cons_pbr_basis_period",
                    "cons_revenue_input_source",
                    "cons_op_input_source",
                    "cons_net_input_source",
                    "cons_pbr_input_source",
                } else base_df[column].where(base_df[column].astype(str).ne(""), base_df[factor_col])

    if not quote_df.empty:
        quote_merge_df = quote_df.copy().rename(
            columns={
                "price": "quote_price",
                "price_source": "quote_price_source",
                "price_captured_at": "quote_price_captured_at",
                "price_freshness": "quote_price_freshness",
                "official_close": "quote_official_close",
                "official_close_date": "quote_official_close_date",
                "price_status": "quote_price_status",
                "change_rate_pct": "quote_change_rate_pct",
            }
        )
        base_df = base_df.merge(quote_merge_df, how="left", on="symbol")
        if "quote_price" in base_df.columns:
            base_df["close"] = pd.to_numeric(base_df["quote_price"], errors="coerce").combine_first(pd.to_numeric(base_df["close"], errors="coerce"))
            base_df["current_price_source"] = base_df["quote_price_source"].where(base_df["quote_price_source"].notna(), base_df["current_price_source"])
            base_df["current_price_captured_at"] = base_df["quote_price_captured_at"].where(base_df["quote_price_captured_at"].notna(), base_df["current_price_captured_at"])
            base_df["current_price_freshness"] = base_df["quote_price_freshness"].where(base_df["quote_price_freshness"].notna(), base_df["current_price_freshness"])
            base_df["current_price_status"] = base_df["quote_price_status"].where(base_df["quote_price_status"].notna(), base_df["current_price_status"])
            base_df["official_close"] = pd.to_numeric(base_df["quote_official_close"], errors="coerce").combine_first(pd.to_numeric(base_df["official_close"], errors="coerce"))
            base_df["official_close_date"] = base_df["quote_official_close_date"].where(base_df["quote_official_close_date"].notna(), base_df["official_close_date"])
            if "quote_price_status" in base_df.columns:
                stale_mask = base_df["quote_price_status"].astype(str).eq("업데이트 지연")
                base_df.loc[stale_mask, "close"] = pd.NA

    base_df["cons_psr"] = base_df.apply(
        lambda item: float(_safe_float(item.get("marcap"), float("nan")) / _safe_float(item.get("cons_revenue_y_krw"), float("nan")))
        if _safe_float(item.get("marcap"), float("nan")) > 0 and _safe_float(item.get("cons_revenue_y_krw"), float("nan")) > 0
        else pd.NA,
        axis=1,
    )
    base_df["cons_op_yield"] = base_df.apply(
        lambda item: float(_safe_float(item.get("cons_op_y_krw"), float("nan")) / _safe_float(item.get("marcap"), float("nan")))
        if _safe_float(item.get("marcap"), float("nan")) > 0 and _safe_float(item.get("cons_op_y_krw"), float("nan")) > 0
        else pd.NA,
        axis=1,
    )
    base_df["cons_net_yield"] = base_df.apply(
        lambda item: float(_safe_float(item.get("cons_net_y_krw"), float("nan")) / _safe_float(item.get("marcap"), float("nan")))
        if _safe_float(item.get("marcap"), float("nan")) > 0 and _safe_float(item.get("cons_net_y_krw"), float("nan")) > 0
        else pd.NA,
        axis=1,
    )
    if "cons_op_margin_q" not in base_df.columns:
        base_df["cons_op_margin_q"] = pd.NA
    if "cons_net_margin_q" not in base_df.columns:
        base_df["cons_net_margin_q"] = pd.NA
    base_df["cons_op_margin_q"] = base_df.apply(
        lambda item: float(_safe_float(item.get("cons_op_q_krw"), float("nan")) / _safe_float(item.get("cons_revenue_q_krw"), float("nan")))
        if _safe_float(item.get("cons_op_q_krw"), float("nan")) > 0 and _safe_float(item.get("cons_revenue_q_krw"), float("nan")) > 0
        else item.get("cons_op_margin_q"),
        axis=1,
    )
    base_df["cons_net_margin_q"] = base_df.apply(
        lambda item: float(_safe_float(item.get("cons_net_q_krw"), float("nan")) / _safe_float(item.get("cons_revenue_q_krw"), float("nan")))
        if _safe_float(item.get("cons_net_q_krw"), float("nan")) > 0 and _safe_float(item.get("cons_revenue_q_krw"), float("nan")) > 0
        else item.get("cons_net_margin_q"),
        axis=1,
    )
    base_df["current_multiple"] = base_df.apply(
        lambda item: _family_metric_multiple(item, str(item.get("valuation_family") or "per")),
        axis=1,
    )

    if not analyst_df.empty:
        base_df = base_df.merge(analyst_df, how="left", on="symbol", suffixes=("", "_analyst"))
    if not event_df.empty:
        base_df = base_df.merge(event_df, how="left", on="symbol", suffixes=("", "_event"))

    rows: list[dict[str, Any]] = []
    for _, row in base_df.iterrows():
        if _clean_text(row.get("excluded_reason")):
            continue
        revisions = _build_revision_inputs(row)
        anchors = [
            _build_analyst_anchor(row),
            _build_peer_anchor(row, base_df, wics_meta),
            _build_earnings_anchor(row, revisions),
        ]
        combined = _combine_anchor_prices(
            row,
            anchors,
            sector_thesis_map=sector_thesis_map,
            macro_sector_scores=macro_sector_scores,
            wics_meta=wics_meta,
        )
        rows.append(
            {
                "symbol": _normalize_symbol(row.get("symbol")),
                "name": _clean_text(row.get("name")) or _normalize_symbol(row.get("symbol")),
                "market": _clean_text(row.get("market")),
                "sector": _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR),
                "sector_source": _clean_text(row.get("sector_source")) or "default",
                "current_price": round(_safe_float(row.get("close"), float("nan")), 2) if _safe_float(row.get("close"), float("nan")) > 0 else pd.NA,
                "current_price_source": _clean_text(row.get("current_price_source")) or "krx_listing_close",
                "current_price_captured_at": _clean_text(row.get("current_price_captured_at")),
                "current_price_freshness": _clean_text(row.get("current_price_freshness")),
                "current_price_status": _clean_text(row.get("current_price_status")) or "자료 없음",
                "official_close": round(_safe_float(row.get("official_close")), 2) if _safe_float(row.get("official_close"), float("nan")) > 0 else pd.NA,
                "official_close_date": _clean_text(row.get("official_close_date")),
                "marcap": round(_safe_float(row.get("marcap")), 2),
                "valuation_family_key": _clean_text(row.get("valuation_family")) or "per",
                "valuation_driver": _FAMILY_DRIVER.get(_clean_text(row.get("valuation_family")) or "per", "net"),
                "valuation_analyst_target_upside_pct": round(_safe_float(row.get("analyst_target_upside_pct")), 2) if _safe_int(row.get("analyst_report_count")) > 0 else pd.NA,
                "analyst_report_count": _safe_int(row.get("analyst_report_count")) or pd.NA,
                "analyst_broker_diversity": _safe_int(row.get("analyst_broker_diversity")) or pd.NA,
                "analyst_latest_title": _clean_text(row.get("analyst_latest_title")),
                "valuation_revision_revenue_pct": round(_safe_float(revisions.get("revenue_revision")) * 100.0, 2),
                "valuation_revision_op_pct": round(_safe_float(revisions.get("op_revision")) * 100.0, 2),
                "valuation_revision_net_pct": round(_safe_float(revisions.get("net_revision")) * 100.0, 2),
                "event_last_type": _clean_text(row.get("event_last_type")),
                "event_last_bias": _clean_text(row.get("event_last_bias")),
                "current_multiple_raw": round(_safe_float(row.get("current_multiple")), 4) if _safe_float(row.get("current_multiple"), float("nan")) > 0 else pd.NA,
                "cons_op_yield_raw": round(_safe_float(row.get("cons_op_yield")), 6) if _safe_float(row.get("cons_op_yield"), float("nan")) > 0 else pd.NA,
                "cons_net_yield_raw": round(_safe_float(row.get("cons_net_yield")), 6) if _safe_float(row.get("cons_net_yield"), float("nan")) > 0 else pd.NA,
                "cons_psr_raw": round(_safe_float(row.get("cons_psr")), 4) if _safe_float(row.get("cons_psr"), float("nan")) > 0 else pd.NA,
                "cons_pbr_raw": round(_safe_float(row.get("cons_pbr")), 4) if _safe_float(row.get("cons_pbr"), float("nan")) > 0 else pd.NA,
                "cons_roe_raw": round(_safe_float(row.get("cons_roe")), 4) if _safe_float(row.get("cons_roe"), float("nan")) > 0 else pd.NA,
                "cons_op_margin_q_raw": round(_safe_float(row.get("cons_op_margin_q")), 6) if _safe_float(row.get("cons_op_margin_q"), float("nan")) > 0 else pd.NA,
                "cons_revenue_y_krw_raw": round(_safe_float(row.get("cons_revenue_y_krw")), 2) if _safe_float(row.get("cons_revenue_y_krw"), float("nan")) > 0 else pd.NA,
                "cons_op_y_krw_raw": round(_safe_float(row.get("cons_op_y_krw")), 2) if _safe_float(row.get("cons_op_y_krw"), float("nan")) > 0 else pd.NA,
                "cons_net_y_krw_raw": round(_safe_float(row.get("cons_net_y_krw")), 2) if _safe_float(row.get("cons_net_y_krw"), float("nan")) > 0 else pd.NA,
                "cons_revenue_fy0_krw_raw": round(_safe_float(row.get("cons_revenue_fy0_krw")), 2) if _safe_float(row.get("cons_revenue_fy0_krw"), float("nan")) > 0 else pd.NA,
                "cons_op_fy0_krw_raw": round(_safe_float(row.get("cons_op_fy0_krw")), 2) if _safe_float(row.get("cons_op_fy0_krw"), float("nan")) > 0 else pd.NA,
                "cons_net_fy0_krw_raw": round(_safe_float(row.get("cons_net_fy0_krw")), 2) if _safe_float(row.get("cons_net_fy0_krw"), float("nan")) > 0 else pd.NA,
                "cons_revenue_fy1_krw_raw": round(_safe_float(row.get("cons_revenue_fy1_krw")), 2) if _safe_float(row.get("cons_revenue_fy1_krw"), float("nan")) > 0 else pd.NA,
                "cons_op_fy1_krw_raw": round(_safe_float(row.get("cons_op_fy1_krw")), 2) if _safe_float(row.get("cons_op_fy1_krw"), float("nan")) > 0 else pd.NA,
                "cons_net_fy1_krw_raw": round(_safe_float(row.get("cons_net_fy1_krw")), 2) if _safe_float(row.get("cons_net_fy1_krw"), float("nan")) > 0 else pd.NA,
                "cons_revenue_actual_krw_raw": round(_safe_float(row.get("cons_revenue_actual_krw")), 2) if _safe_float(row.get("cons_revenue_actual_krw"), float("nan")) > 0 else pd.NA,
                "cons_op_actual_krw_raw": round(_safe_float(row.get("cons_op_actual_krw")), 2) if _safe_float(row.get("cons_op_actual_krw"), float("nan")) > 0 else pd.NA,
                "cons_net_actual_krw_raw": round(_safe_float(row.get("cons_net_actual_krw")), 2) if _safe_float(row.get("cons_net_actual_krw"), float("nan")) > 0 else pd.NA,
                "cons_revenue_basis_period": _clean_text(row.get("cons_revenue_basis_period")),
                "cons_op_basis_period": _clean_text(row.get("cons_op_basis_period")),
                "cons_net_basis_period": _clean_text(row.get("cons_net_basis_period")),
                "cons_pbr_basis_period": _clean_text(row.get("cons_pbr_basis_period")),
                "cons_revenue_input_source": _clean_text(row.get("cons_revenue_input_source")),
                "cons_op_input_source": _clean_text(row.get("cons_op_input_source")),
                "cons_net_input_source": _clean_text(row.get("cons_net_input_source")),
                "cons_pbr_input_source": _clean_text(row.get("cons_pbr_input_source")),
                "cons_actual_year": _safe_int(row.get("cons_actual_year")) or pd.NA,
                "cons_revenue_basis_period_raw": _clean_text(row.get("cons_revenue_basis_period")),
                "cons_op_basis_period_raw": _clean_text(row.get("cons_op_basis_period")),
                "cons_net_basis_period_raw": _clean_text(row.get("cons_net_basis_period")),
                "cons_revenue_input_source_raw": _clean_text(row.get("cons_revenue_input_source")),
                "cons_op_input_source_raw": _clean_text(row.get("cons_op_input_source")),
                "cons_net_input_source_raw": _clean_text(row.get("cons_net_input_source")),
                "cons_actual_year_raw": _safe_int(row.get("cons_actual_year")) or pd.NA,
                **combined,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = _apply_proxy_coverage(out)
    out = _decorate_valuation_display(out)
    out["valuation_family"] = out["valuation_family_key"].map(lambda value: _FAMILY_LABELS.get(_clean_text(value), _clean_text(value) or "PER"))
    out["valuation_proxy_used"] = out["valuation_proxy_used"].fillna(False).astype(bool)
    out = out.sort_values(
        ["valuation_proxy_used", "fair_value_confidence_score", "fair_value_gap_pct", "marcap"],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    out = out.drop(columns=["valuation_family_key"], errors="ignore")
    return out


def build_fair_value_summary(snapshot_df: pd.DataFrame, *, top_n: int = 20) -> dict[str, Any]:
    if snapshot_df is None or snapshot_df.empty:
        return {
            "generated_at": _now_iso(),
            "row_count": 0,
            "coverage_count": 0,
            "top_discount": [],
            "top_premium": [],
            "top_confident": [],
            "by_sector": [],
        }

    work_df = snapshot_df.copy()
    available = work_df[pd.to_numeric(work_df["fair_value_base"], errors="coerce").notna()].copy()
    available["fair_value_gap_pct"] = pd.to_numeric(available["fair_value_gap_pct"], errors="coerce")
    available["fair_value_confidence_score"] = pd.to_numeric(available["fair_value_confidence_score"], errors="coerce").fillna(0.0)
    available["valuation_proxy_used"] = available["valuation_proxy_used"].fillna(False).astype(bool)
    available["discount_rank_score"] = available["fair_value_gap_pct"].fillna(0.0) * (0.35 + available["fair_value_confidence_score"].clip(0.0, 1.0))
    available.loc[available["valuation_proxy_used"], "discount_rank_score"] *= 0.72
    available["premium_rank_score"] = available["fair_value_gap_pct"].fillna(0.0) * (0.35 + available["fair_value_confidence_score"].clip(0.0, 1.0))
    available.loc[available["valuation_proxy_used"], "premium_rank_score"] *= 0.72

    def _pack(df: pd.DataFrame) -> list[dict[str, Any]]:
        rows = []
        for row in df.to_dict("records"):
            rows.append(
                {
                    "symbol": _normalize_symbol(row.get("symbol")),
                    "name": _clean_text(row.get("name")),
                    "sector": normalize_sector_name(row.get("sector")),
                    "current_price": round(_safe_float(row.get("current_price")), 2),
                    "fair_value_base": round(_safe_float(row.get("fair_value_base")), 2),
                    "fair_value_gap_pct": round(_safe_float(row.get("fair_value_gap_pct")), 2),
                    "fair_value_confidence_score": round(_safe_float(row.get("fair_value_confidence_score")), 4),
                    "valuation_primary_method": _clean_text(row.get("valuation_primary_method")),
                    "valuation_basis_label": _clean_text(row.get("valuation_basis_label")),
                    "valuation_basis_period": _clean_text(row.get("valuation_basis_period")),
                    "valuation_input_source": _clean_text(row.get("valuation_input_source")),
                    "valuation_multiple_current": round(_safe_float(row.get("valuation_multiple_current")), 2) if _safe_float(row.get("valuation_multiple_current"), float("nan")) > 0 else None,
                    "valuation_multiple_target": round(_safe_float(row.get("valuation_multiple_target")), 2) if _safe_float(row.get("valuation_multiple_target"), float("nan")) > 0 else None,
                    "valuation_multiple_unit": _clean_text(row.get("valuation_multiple_unit")) or "배",
                    "operating_profit_yield_pct": round(_safe_float(row.get("operating_profit_yield_pct")), 2) if _safe_float(row.get("operating_profit_yield_pct"), float("nan")) > 0 else None,
                    "operating_margin_pct": round(_safe_float(row.get("operating_margin_pct")), 2) if _safe_float(row.get("operating_margin_pct"), float("nan")) > 0 else None,
                    "roe_current": round(_safe_float(row.get("roe_current")), 2) if _safe_float(row.get("roe_current"), float("nan")) > 0 else None,
                    "profitability_metric_label": _clean_text(row.get("profitability_metric_label")),
                    "profitability_metric_value": round(_safe_float(row.get("profitability_metric_value")), 2) if _safe_float(row.get("profitability_metric_value"), float("nan")) > 0 else None,
                    "valuation_summary_paragraph": _clean_text(row.get("valuation_summary_paragraph")),
                    "valuation_method_detail": _clean_text(row.get("valuation_method_detail")),
                    "valuation_formula_hint": _clean_text(row.get("valuation_formula_hint")),
                    "profitability_formula_hint": _clean_text(row.get("profitability_formula_hint")),
                    "tp_formula_label": _clean_text(row.get("tp_formula_label")),
                    "tp_basis_metric_label": _clean_text(row.get("tp_basis_metric_label")),
                    "tp_basis_metric_value": round(_safe_float(row.get("tp_basis_metric_value")), 4) if _safe_float(row.get("tp_basis_metric_value"), float("nan")) > 0 else None,
                    "tp_peer_median_multiple": round(_safe_float(row.get("tp_peer_median_multiple")), 2) if _safe_float(row.get("tp_peer_median_multiple"), float("nan")) > 0 else None,
                    "tp_peer_q25_multiple": round(_safe_float(row.get("tp_peer_q25_multiple")), 2) if _safe_float(row.get("tp_peer_q25_multiple"), float("nan")) > 0 else None,
                    "tp_peer_q75_multiple": round(_safe_float(row.get("tp_peer_q75_multiple")), 2) if _safe_float(row.get("tp_peer_q75_multiple"), float("nan")) > 0 else None,
                    "tp_peer_count_used": _safe_int(row.get("tp_peer_count_used")),
                    "tp_sanity_low_price": round(_safe_float(row.get("tp_sanity_low_price")), 2) if _safe_float(row.get("tp_sanity_low_price"), float("nan")) > 0 else None,
                    "tp_sanity_high_price": round(_safe_float(row.get("tp_sanity_high_price")), 2) if _safe_float(row.get("tp_sanity_high_price"), float("nan")) > 0 else None,
                    "tp_sanity_bound_applied": bool(row.get("tp_sanity_bound_applied")),
                    "tp_revision_contributors": _clean_text(row.get("tp_revision_contributors")),
                    "tp_explanation_steps": [item for item in (row.get("tp_explanation_steps") or []) if _clean_text(item)],
                    "tp_basis_summary": _clean_text(row.get("tp_basis_summary")),
                    "tp_peer_set_summary": _clean_text(row.get("tp_peer_set_summary")),
                    "tp_bound_summary": _clean_text(row.get("tp_bound_summary")),
                    "tp_hidden_reason_detail": _clean_text(row.get("tp_hidden_reason_detail")),
                    "valuation_reason_summary": _clean_text(row.get("valuation_reason_summary")),
                    "valuation_tp_visible": bool(row.get("valuation_tp_visible")),
                    "valuation_tp_hidden_reason": _clean_text(row.get("valuation_tp_hidden_reason")),
                    "valuation_peer_direct_count": _safe_int(row.get("valuation_peer_direct_count")),
                    "valuation_tier": _clean_text(row.get("valuation_tier")),
                    "valuation_proxy_used": bool(row.get("valuation_proxy_used")),
                    "valuation_driver": _clean_text(row.get("valuation_driver")),
                }
            )
        return rows

    discount_df = available.sort_values(["discount_rank_score", "fair_value_confidence_score"], ascending=[False, False]).head(top_n)
    premium_df = available.sort_values(["premium_rank_score", "fair_value_confidence_score"], ascending=[True, False]).head(min(top_n, 10))
    confident_df = available.sort_values(["valuation_proxy_used", "fair_value_confidence_score", "fair_value_gap_pct"], ascending=[True, False, False]).head(top_n)
    sector_summary = (
        available.groupby("sector", dropna=False)
        .agg(
            count=("symbol", "count"),
            avg_gap_pct=("fair_value_gap_pct", "mean"),
            avg_confidence=("fair_value_confidence_score", "mean"),
        )
        .reset_index()
        .sort_values(["avg_gap_pct", "avg_confidence"], ascending=[False, False])
    )

    return {
        "generated_at": _now_iso(),
        "row_count": int(len(work_df)),
        "coverage_count": int(len(available)),
        "top_discount": _pack(discount_df),
        "top_premium": _pack(premium_df),
        "top_confident": _pack(confident_df),
        "by_sector": [
            {
                "sector": normalize_sector_name(row.get("sector")),
                "count": int(row.get("count", 0) or 0),
                "avg_gap_pct": round(_safe_float(row.get("avg_gap_pct")), 2),
                "avg_confidence": round(_safe_float(row.get("avg_confidence")), 4),
            }
            for row in sector_summary.head(12).to_dict("records")
        ],
    }


def save_fair_value_snapshot(snapshot_df: pd.DataFrame, summary: dict[str, Any] | None = None) -> dict[str, str]:
    os.makedirs(VALUATION_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary = summary or build_fair_value_summary(snapshot_df)

    csv_path = os.path.join(VALUATION_DIR, f"fair_value_snapshot_{stamp}.csv")
    json_path = os.path.join(VALUATION_DIR, f"fair_value_snapshot_{stamp}.json")
    snapshot_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    snapshot_df.to_csv(LATEST_CSV_PATH, index=False, encoding="utf-8-sig")
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    with open(LATEST_JSON_PATH, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    return {
        "snapshot_csv": csv_path,
        "snapshot_json": json_path,
        "latest_snapshot_csv": LATEST_CSV_PATH,
        "latest_snapshot_json": LATEST_JSON_PATH,
    }


def load_latest_fair_value_frame() -> pd.DataFrame:
    if not os.path.exists(LATEST_CSV_PATH):
        return pd.DataFrame()
    try:
        return pd.read_csv(LATEST_CSV_PATH, dtype={"symbol": str})
    except Exception:
        return pd.DataFrame()


def load_latest_fair_value_summary() -> dict[str, Any]:
    if not os.path.exists(LATEST_JSON_PATH):
        return {}
    try:
        with open(LATEST_JSON_PATH, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def build_fair_value_digest(summary: dict[str, Any], *, top_n: int = 8) -> str:
    lines = ["[적정가 스냅샷] 혼합 적정가 요약"]
    lines.append(f"- 생성시각: {summary.get('generated_at') or _now_iso()}")
    lines.append(
        f"- 커버리지: 산출 {int(summary.get('coverage_count', 0) or 0)} / 전체 {int(summary.get('row_count', 0) or 0)}"
    )

    discount_rows = list(summary.get("top_discount") or [])[:top_n]
    premium_rows = list(summary.get("top_premium") or [])[: min(top_n, 5)]
    sector_rows = list(summary.get("by_sector") or [])[:5]

    if discount_rows:
        lines.append("*1. 기준 적정가 대비 할인 구간*")
        for row in discount_rows:
            lines.append(
                f"- {row.get('name')}({row.get('symbol')}) | {row.get('sector')} | "
                f"현재가 {int(round(_safe_float(row.get('current_price')))):,} | "
                f"기준 적정가 {int(round(_safe_float(row.get('fair_value_base')))):,} | "
                f"괴리 {round(_safe_float(row.get('fair_value_gap_pct')), 1)}% | "
                f"신뢰 {int(round(_safe_float(row.get('fair_value_confidence_score')) * 100.0))}/100 | "
                f"{row.get('valuation_reason_summary') or '-'}"
            )
    if premium_rows:
        lines.append("*2. 기준 적정가 대비 선반영 구간*")
        for row in premium_rows:
            lines.append(
                f"- {row.get('name')}({row.get('symbol')}) | {row.get('sector')} | "
                f"괴리 {round(_safe_float(row.get('fair_value_gap_pct')), 1)}% | "
                f"신뢰 {int(round(_safe_float(row.get('fair_value_confidence_score')) * 100.0))}/100"
            )
    if sector_rows:
        lines.append("*3. 적정가 기준 상단 섹터*")
        for row in sector_rows:
            lines.append(
                f"- {row.get('sector')} | 산출 {int(row.get('count', 0) or 0)} | "
                f"평균 괴리 {round(_safe_float(row.get('avg_gap_pct')), 1)}% | "
                f"평균 신뢰 {int(round(_safe_float(row.get('avg_confidence')) * 100.0))}/100"
            )
    return "\n".join(lines)
