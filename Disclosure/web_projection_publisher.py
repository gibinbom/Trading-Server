from __future__ import annotations

import argparse
import ast
import json
import logging
import math
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from config import SETTINGS
    from dart_common import classify_disclosure_event
    from fair_value_engine import ELIGIBLE_MARKETS, UNCLASSIFIED_SECTOR
    from event_detail_parser import get_parsed_event_document
    from stock_financial_profile_builder import build_stock_financial_profile_docs
    from mongo_read_models import MongoReadModelStore
    from disclosure_event_pipeline import build_symbol_summary, load_event_records
    from runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from signals.wics_universe import load_effective_wics_symbol_map, normalize_sector_name
except Exception:
    from Disclosure.config import SETTINGS
    from Disclosure.dart_common import classify_disclosure_event
    from Disclosure.fair_value_engine import ELIGIBLE_MARKETS, UNCLASSIFIED_SECTOR
    from Disclosure.event_detail_parser import get_parsed_event_document
    from Disclosure.stock_financial_profile_builder import build_stock_financial_profile_docs
    from Disclosure.mongo_read_models import MongoReadModelStore
    from Disclosure.disclosure_event_pipeline import build_symbol_summary, load_event_records
    from Disclosure.runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from Disclosure.signals.wics_universe import load_effective_wics_symbol_map, normalize_sector_name


log = logging.getLogger("disclosure.web_projection_publisher")
ROOT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT_DIR = ROOT_DIR.parent
LISTING_PATH = PROJECT_ROOT_DIR / "krx_listing.csv"
WEB_DIR = Path(RUNTIME_DIR) / "web_projections"
QUOTE_SOURCE_PATH = WEB_DIR / "quote_delayed_source_latest.json"
QUOTE_HOTSET_PATH = WEB_DIR / "quote_delayed_hotset_latest.json"
LEGACY_QUOTE_PATH = WEB_DIR / "quote_delayed_latest.json"
FLOW_SOURCE_PATH = WEB_DIR / "stock_flow_latest_source.json"
FAIR_VALUE_CSV_PATH = ROOT_DIR / "valuation" / "fair_value_snapshot_latest.csv"
CARDS_JSON_PATH = ROOT_DIR / "cards" / "stock_cards_latest.json"
MARKET_BRIEFING_PATH = ROOT_DIR / "runtime" / "market_briefing_latest.json"
SECTOR_THESIS_PATH = ROOT_DIR / "runtime" / "sector_thesis_latest.json"
MACRO_REGIME_PATH = ROOT_DIR / "macro" / "cache" / "macro_regime_latest.json"
EVENT_SYMBOLS_CSV_PATH = ROOT_DIR / "events" / "reports" / "disclosure_event_symbols_latest.csv"
EVENT_BACKTEST_CSV_PATH = ROOT_DIR / "events" / "reports" / "disclosure_event_backtest_latest.csv"
WICS_META_PATH = ROOT_DIR / "signals" / "reports" / "wics_effective_universe_latest.json"
NEWS_SUMMARY_DIR = ROOT_DIR / "news" / "summaries"
NEWS_LOG_DIR = ROOT_DIR / "news" / "logs"
ANALYST_SUMMARY_PATH = ROOT_DIR / "analyst_reports" / "summaries" / "analyst_report_summary_latest.json"
ANALYST_SCORED_PATH = ROOT_DIR / "analyst_reports" / "summaries" / "analyst_report_scored_latest.csv"


EVENT_LABELS = {
    "DIVIDEND": "배당",
    "STOCK_SPLIT": "주식분할",
    "REVERSE_SPLIT_REDUCTION": "병합·감자",
    "MERGER": "합병",
    "SPINOFF": "분할",
    "BUYBACK": "자사주 취득",
    "BUYBACK_DISPOSAL": "자사주 처분",
    "STOCK_CANCELLATION": "자사주 소각",
    "SUPPLY_CONTRACT": "수주",
    "SUPPLY_UPDATE": "수주 변경",
    "SUPPLY_TERMINATION": "수주 해지",
    "PERF_PRELIM": "잠정실적",
    "SALES_VARIATION": "매출·손익 변동",
    "DILUTION": "희석 이벤트",
    "INSIDER_OWNERSHIP": "임원·주요주주 변동",
    "LARGE_HOLDER": "대량보유 변동",
    "CORRECTION": "정정 공시",
    "OTHER_DISCLOSURE": "기타 공시",
}

CAPITAL_ACTION_EVENT_TYPES = {
    "DIVIDEND",
    "STOCK_SPLIT",
    "REVERSE_SPLIT_REDUCTION",
    "MERGER",
    "SPINOFF",
    "BUYBACK",
    "BUYBACK_DISPOSAL",
    "STOCK_CANCELLATION",
}

REVISION_EVENT_TYPES = {
    "SUPPLY_CONTRACT",
    "SUPPLY_UPDATE",
    "SUPPLY_TERMINATION",
    "PERF_PRELIM",
    "SALES_VARIATION",
}

ROUTINE_EVENT_TYPES = {"INSIDER_OWNERSHIP", "LARGE_HOLDER", "CORRECTION"}
PUBLIC_EVENT_TYPES = CAPITAL_ACTION_EVENT_TYPES | REVISION_EVENT_TYPES | {
    "DILUTION",
    "OTHER_DISCLOSURE",
}
EXCLUDED_PUBLIC_EVENT_TYPES = {"INSIDER_OWNERSHIP", "LARGE_HOLDER"}

RAW_EVENT_LOOKBACK_DAYS = 180

FAMILY_LABELS = {
    "pbr_proxy": "PBR/자본수익성",
    "ev_ebitda": "시가총액/영업이익",
    "psr": "PSR",
    "per": "PER",
}

FAMILY_BY_SECTOR = {
    "pbr_proxy": {"금융지주/은행", "은행", "증권", "보험", "통신서비스", "전력/유틸리티", "복합기업"},
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
    "psr": {"제약/바이오 (대형)", "헬스케어/의료기기", "엔터테인먼트/게임", "IT소프트웨어 (플랫폼/SI)", "2차전지/배터리"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish Trading snapshots into Mongo/web read models.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print a short publish summary.")
    parser.add_argument("--skip-mongo", action="store_true", help="Only write local projection files.")
    parser.add_argument("--times", default="07:00,12:00,15:45,20:25", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


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


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def _clean_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    text = _clean_text(value)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [_clean_text(item) for item in parsed if _clean_text(item)]
    return [text]


def _norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _normalize_title_key(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    text = re.sub(r"^\[[^\]]*(정정|수정|첨부정정|기재정정)[^\]]*\]\s*", "", text)
    text = re.sub(r"^(기재정정|정정|첨부정정|추가정정|정정신고|변경)\s*", "", text)
    text = text.replace("㈜", "")
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[ㆍ·･・,:;'\"]", "", text)
    text = re.sub(r"[\[\]\(\)\{\}<>\-_/]", "", text)
    for noise in (
        "주요사항보고서",
        "투자판단관련주요경영사항",
        "자회사의주요경영사항",
        "조회공시요구답변",
        "조회공시요구",
    ):
        text = text.replace(noise, "")
    return text.lower().strip()


def _event_numeric_detail_count(doc: dict[str, Any]) -> int:
    count = 0
    for key in (
        "liquidity_float_delta_pct",
        "share_count_before",
        "share_count_after",
        "merger_price",
        "dividend_per_share",
        "dividend_yield_pct",
        "buyback_amount",
        "buyback_pct_mcap",
        "dilution_pct",
        "contract_amount",
        "sales_ratio_pct",
    ):
        value = doc.get(key)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            count += 1
    parsed = doc.get("parsed_event_details") if isinstance(doc.get("parsed_event_details"), dict) else {}
    for key in (
        "merger_new_shares",
        "new_shares",
        "contract_amount",
        "sales_ratio_pct",
        "dividend_per_share",
        "dividend_yield_pct",
        "buyback_amount",
        "buyback_pct_mcap",
        "dilution_pct",
        "liquidity_float_delta_pct",
    ):
        value = parsed.get(key)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            count += 1
    return count


def _has_meaningful_other_disclosure(doc: dict[str, Any]) -> bool:
    metrics = doc.get("event_key_metrics")
    if isinstance(metrics, list) and len(metrics) > 0:
        return True
    if _event_numeric_detail_count(doc) > 0:
        return True
    summary = _clean_text(doc.get("event_detail_summary"))
    title = _clean_text(doc.get("title"))
    excerpt = _clean_text(doc.get("event_source_excerpt"))
    if summary and summary != title and len(summary) >= 16:
        return True
    if excerpt and excerpt != title and len(excerpt) >= 24:
        return True
    return False


def _should_include_public_event(doc: dict[str, Any]) -> bool:
    event_type = _clean_text(doc.get("event_type"))
    if event_type in EXCLUDED_PUBLIC_EVENT_TYPES:
        return False
    if event_type == "CORRECTION":
        return False
    if event_type == "OTHER_DISCLOSURE":
        return _has_meaningful_other_disclosure(doc)
    return event_type in PUBLIC_EVENT_TYPES


def _event_group_for_type(event_type: str) -> str:
    if event_type in CAPITAL_ACTION_EVENT_TYPES:
        return "capital_actions"
    if event_type in REVISION_EVENT_TYPES | {"DILUTION"}:
        return "operating_updates"
    return "other_disclosures"


def _event_display_key(doc: dict[str, Any]) -> str:
    symbol = _norm_symbol(doc.get("symbol"))
    event_type = _clean_text(doc.get("event_type"))
    title_key = _normalize_title_key(doc.get("title"))
    event_date = _clean_text(doc.get("event_date"))
    return "|".join([symbol, event_type, title_key, event_date])


def _event_sort_key(doc: dict[str, Any]) -> tuple[int, int, int, str]:
    detail_count = _event_numeric_detail_count(doc)
    metric_count = len(doc.get("event_key_metrics") or [])
    is_correction = 0 if bool(doc.get("_is_correction_record")) else 1
    latest_ts = f"{_clean_text(doc.get('event_date'))} {_clean_text(doc.get('event_time_hhmm'))} {_clean_text(doc.get('updated_at'))}"
    return (detail_count, metric_count, is_correction, latest_ts)


def _apply_correction_metadata(base_doc: dict[str, Any], correction_docs: list[dict[str, Any]]) -> dict[str, Any]:
    if not correction_docs:
        base_doc["correction_applied"] = False
        base_doc["correction_count"] = 0
        base_doc["latest_correction_date"] = ""
        base_doc["correction_summary"] = ""
        return base_doc
    latest = max(correction_docs, key=_event_sort_key)
    summaries = []
    for item in correction_docs:
        summary = _clean_text(item.get("event_detail_summary")) or _clean_text(item.get("title"))
        if summary and summary not in summaries:
            summaries.append(summary)
    base_doc["correction_applied"] = True
    base_doc["correction_count"] = len(correction_docs)
    base_doc["latest_correction_date"] = _clean_text(latest.get("event_date"))
    base_doc["correction_summary"] = " / ".join(summaries[:2])
    return base_doc


def _normalize_sector(value: Any, default: str = "") -> str:
    text = normalize_sector_name(value)
    if not text:
        return default
    return text


def _classify_excluded_reason(name: Any, market: Any) -> str:
    market_text = _clean_text(market)
    name_text = _clean_text(name)
    if market_text and market_text not in ELIGIBLE_MARKETS:
        return market_text
    if name_text.endswith("우") or name_text.endswith("우B") or any(name_text.endswith(f"{digit}우B") for digit in "1234"):
        return "우선주"
    upper_name = name_text.upper()
    for keyword in ("ETF", "ETN", "리츠", "스팩", "SPAC", "기업인수목적", "유동화전문유한회사", "유한회사"):
        if keyword and (keyword in name_text or keyword in upper_name):
            return keyword
    return ""


def _valuation_family_label(sector: Any) -> str:
    normalized = _normalize_sector(sector, UNCLASSIFIED_SECTOR)
    for family, sectors in FAMILY_BY_SECTOR.items():
        if normalized in sectors:
            return FAMILY_LABELS.get(family, family)
    return FAMILY_LABELS["per"]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _load_listing() -> pd.DataFrame:
    if not LISTING_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(LISTING_PATH, dtype={"Code": str})
    if df.empty:
        return pd.DataFrame()
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    return df


def _load_fair_value_frame() -> pd.DataFrame:
    if not FAIR_VALUE_CSV_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(FAIR_VALUE_CSV_PATH, dtype={"symbol": str})
    if df.empty:
        return pd.DataFrame()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    return df


def _load_cards_summary() -> dict[str, Any]:
    return _read_json(CARDS_JSON_PATH)


def _load_market_briefing() -> dict[str, Any]:
    return _read_json(MARKET_BRIEFING_PATH)


def _load_sector_thesis() -> dict[str, Any]:
    return _read_json(SECTOR_THESIS_PATH)


def _load_macro_regime() -> dict[str, Any]:
    return _read_json(MACRO_REGIME_PATH)


def _load_wics_meta() -> dict[str, Any]:
    return _read_json(WICS_META_PATH)


def _load_quote_rows() -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    def _quote_score(row: dict[str, Any]) -> tuple[int, int]:
        status = _clean_text(row.get("price_status"))
        status_score = 4 if status == "지연시세" else 3 if status == "업데이트 지연" else 2 if status == "공식종가 fallback" else 1
        captured = _clean_text(row.get("price_captured_at") or row.get("captured_at"))
        try:
            captured_score = int(datetime.fromisoformat(captured).timestamp()) if captured else 0
        except Exception:
            captured_score = 0
        return status_score, captured_score

    for path in (QUOTE_SOURCE_PATH, QUOTE_HOTSET_PATH, LEGACY_QUOTE_PATH):
        payload = _read_json(path)
        rows: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            maybe_rows = payload.get("rows") or []
            rows = maybe_rows if isinstance(maybe_rows, list) else []
        elif isinstance(payload, list):
            rows = payload
        elif path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    rows = raw
                elif isinstance(raw, dict):
                    maybe_rows = raw.get("rows") or []
                    rows = maybe_rows if isinstance(maybe_rows, list) else []
            except Exception:
                continue
        for row in rows:
            symbol = _norm_symbol((row or {}).get("symbol"))
            if not symbol:
                continue
            current = merged.get(symbol)
            if current is None or _quote_score(row) >= _quote_score(current):
                merged[symbol] = row
    return list(merged.values())


def _load_flow_source_rows() -> list[dict[str, Any]]:
    if not FLOW_SOURCE_PATH.exists():
        return []
    payload = _read_json(FLOW_SOURCE_PATH)
    rows = payload.get("rows") if isinstance(payload, dict) else []
    return rows if isinstance(rows, list) else []


def _latest_news_summary_path() -> Path | None:
    files = sorted(NEWS_SUMMARY_DIR.glob("stock_news_summary_*.json"))
    return files[-1] if files else None


def _latest_news_log_path() -> Path | None:
    files = sorted(NEWS_LOG_DIR.glob("stock_news_*.jsonl"))
    return files[-1] if files else None


def _hangul_initials(text: str) -> str:
    out: list[str] = []
    for char in str(text or "").strip():
        code = ord(char)
        if 0xAC00 <= code <= 0xD7A3:
            out.append("ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ"[(code - 0xAC00) // 588])
        elif char.strip():
            out.append(char)
    return "".join(out)


def _load_news_maps() -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    summary_path = _latest_news_summary_path()
    summary_map: dict[str, dict[str, Any]] = {}
    if summary_path:
        payload = _read_json(summary_path)
        for bucket_name in ("top_positive", "top_negative"):
            for item in payload.get(bucket_name, []) or []:
                symbol = _norm_symbol(item.get("symbol"))
                if not symbol:
                    continue
                summary_map[symbol] = {
                    "symbol": symbol,
                    "name": _clean_text(item.get("name")),
                    "latest_title": _clean_text(item.get("latest_title")),
                    "news_count": _safe_int(item.get("news_count")),
                    "conviction_score": _safe_float(item.get("conviction_score")),
                    "tone": "positive" if bucket_name == "top_positive" else "negative",
                }

    detail_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    log_path = _latest_news_log_path()
    if log_path and log_path.exists():
        try:
            with open(log_path, "r", encoding="utf-8") as fp:
                for raw in fp:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        row = json.loads(raw)
                    except Exception:
                        continue
                    symbol = _norm_symbol(row.get("symbol"))
                    if not symbol or len(detail_map[symbol]) >= 10:
                        continue
                    detail_map[symbol].append(
                        {
                            "symbol": symbol,
                            "title": _clean_text(row.get("title") or row.get("latest_title")),
                            "source": _clean_text(row.get("source")),
                            "published_at": _clean_text(row.get("published_at") or row.get("last_seen")),
                            "url": _clean_text(row.get("url") or row.get("detail_url")),
                            "score": _safe_float(row.get("score") or row.get("news_score")),
                        }
                    )
        except Exception as exc:
            log.warning("failed to read news log: %s", exc)
    return summary_map, detail_map


def _load_analyst_maps() -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    summary_map: dict[str, dict[str, Any]] = {}
    detail_map: dict[str, list[dict[str, Any]]] = defaultdict(list)

    summary_payload = _read_json(ANALYST_SUMMARY_PATH)
    for row in summary_payload.get("top_stocks") or []:
        symbol = _norm_symbol(row.get("symbol"))
        if not symbol:
            continue
        report_count = _safe_int(row.get("report_count"))
        broker_diversity = _safe_int(row.get("broker_diversity"))
        latest_title = _clean_text(row.get("latest_title"))
        target_upside = _safe_float(row.get("target_upside_pct"), float("nan"))
        if report_count > 0:
            if math.isfinite(target_upside):
                summary_text = (
                    f"최근 30일 리포트 {report_count}건, 증권사 {broker_diversity}곳입니다. "
                    f"최신 제목은 `{latest_title or '-'}`이고 목표가 여력은 {target_upside:.1f}%입니다."
                )
            else:
                summary_text = (
                    f"최근 30일 리포트 {report_count}건, 증권사 {broker_diversity}곳입니다. "
                    f"최신 제목은 `{latest_title or '-'}` 입니다."
                )
        else:
            summary_text = "최근 30일 애널 커버리지 부족"
        summary_map[symbol] = {
            "symbol": symbol,
            "name": _clean_text(row.get("name")),
            "report_count": report_count,
            "broker_diversity": broker_diversity,
            "latest_title": latest_title,
            "target_upside_pct": round(target_upside, 2) if math.isfinite(target_upside) else None,
            "summary_paragraph": summary_text,
        }

    if ANALYST_SCORED_PATH.exists():
        try:
            scored_df = pd.read_csv(ANALYST_SCORED_PATH, dtype={"symbol": str})
        except Exception as exc:
            log.warning("failed to read analyst scored snapshot: %s", exc)
            scored_df = pd.DataFrame()
        if not scored_df.empty:
            scored_df["symbol"] = scored_df["symbol"].astype(str).str.zfill(6)
            scored_df["published_at"] = pd.to_datetime(scored_df.get("published_at"), errors="coerce")
            scored_df = scored_df.sort_values(["symbol", "published_at"], ascending=[True, False])
            for symbol, group in scored_df.groupby("symbol", dropna=False):
                if not symbol:
                    continue
                rows: list[dict[str, Any]] = []
                for _, row in group.head(3).iterrows():
                    target_price = _safe_float(row.get("target_price"), float("nan"))
                    rows.append(
                        {
                            "title": _clean_text(row.get("title")),
                            "broker": _clean_text(row.get("broker")),
                            "published_at": _clean_text(row.get("published_at")),
                            "target_price": round(target_price, 2) if math.isfinite(target_price) and target_price > 0 else None,
                            "detail_url": _clean_text(row.get("detail_url")),
                        }
                    )
                detail_map[symbol] = rows
    return summary_map, detail_map


def _build_analyst_summary_paragraph(
    analyst: dict[str, Any],
    analyst_items: list[dict[str, Any]],
    fair: dict[str, Any],
) -> str:
    summary_text = _clean_text(analyst.get("summary_paragraph"))
    report_count = max(
        _safe_int(analyst.get("report_count")),
        _safe_int(fair.get("analyst_report_count")),
        len(analyst_items),
    )
    broker_diversity = max(
        _safe_int(analyst.get("broker_diversity")),
        _safe_int(fair.get("analyst_broker_diversity")),
    )
    latest_title = _clean_text(
        analyst.get("latest_title")
        or fair.get("analyst_latest_title")
        or (analyst_items[0].get("title") if analyst_items else "")
    )
    target_upside = _safe_float(
        analyst.get("target_upside_pct") or fair.get("analyst_target_upside_pct"),
        float("nan"),
    )
    analyst_contributors = [
        item.strip()
        for item in _clean_text(fair.get("tp_revision_contributors")).split("/")
        if item.strip() and ("목표가" in item or "실적 추정" in item)
    ]
    if not summary_text:
        if report_count <= 0 and not latest_title:
            return "최근 30일 애널 커버리지 부족"
        parts = [f"최근 30일 리포트 {report_count}건, 증권사 {broker_diversity or max(report_count, 1)}곳이다"]
        if latest_title:
            parts.append(f"최신 리포트 제목은 {latest_title}")
        if math.isfinite(target_upside):
            parts.append(f"목표가 여력은 {target_upside:.1f}%다")
        summary_text = ". ".join(parts).strip()
        if summary_text and not summary_text.endswith("."):
            summary_text = f"{summary_text}."
    if analyst_contributors:
        contributor_text = " / ".join(dict.fromkeys(analyst_contributors))
        if contributor_text not in summary_text:
            summary_text = f"{summary_text.rstrip('.')} TP 설명에는 {contributor_text}이 반영됐다."
    return summary_text.strip()


def _build_disclosure_revision_summary(
    revision_event: dict[str, Any],
    fair: dict[str, Any],
    revision_op_pct: float,
) -> tuple[bool, str]:
    basis_period = _clean_text(fair.get("valuation_basis_period"))
    contributor_text = " / ".join(
        item.strip()
        for item in _clean_text(fair.get("tp_revision_contributors")).split("/")
        if item.strip() and ("공시" in item or "수주" in item)
    )
    base_summary = _clean_text(revision_event.get("summary"))
    event_applied = bool(revision_event.get("applied"))
    basis_applied = basis_period == "실제 실적 + 공시 보정"
    applied = (event_applied and abs(revision_op_pct) >= 0.5) or basis_applied
    if base_summary and applied and math.isfinite(revision_op_pct):
        base_summary = f"{base_summary} ({revision_op_pct:+.1f}%p)"
    elif not base_summary and basis_applied:
        base_summary = "최근 직접 연결 공시를 실제 실적 기준 추정에 반영"
        if math.isfinite(revision_op_pct):
            base_summary = f"{base_summary} ({revision_op_pct:+.1f}%p)"
    if contributor_text:
        if base_summary:
            if contributor_text not in base_summary:
                base_summary = f"{base_summary}. 반영 이유: {contributor_text}"
        else:
            base_summary = contributor_text
    return applied, base_summary.strip()


def _load_event_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    detail_df = pd.read_csv(EVENT_BACKTEST_CSV_PATH, dtype={"stock_code": str}) if EVENT_BACKTEST_CSV_PATH.exists() else pd.DataFrame()
    if not detail_df.empty:
        detail_df["stock_code"] = detail_df["stock_code"].astype(str).str.zfill(6)

    raw_records = load_event_records(days=RAW_EVENT_LOOKBACK_DAYS)
    raw_df = pd.DataFrame(raw_records) if raw_records else pd.DataFrame()
    if raw_df.empty:
        symbol_df = pd.read_csv(EVENT_SYMBOLS_CSV_PATH, dtype={"symbol": str}) if EVENT_SYMBOLS_CSV_PATH.exists() else pd.DataFrame()
        if not symbol_df.empty:
            symbol_df["symbol"] = symbol_df["symbol"].astype(str).str.zfill(6)
        return symbol_df, detail_df

    for column in ("stock_code", "event_date", "event_time_hhmm", "signal_bias", "corp_name", "title", "rcp_no", "event_type", "sector"):
        if column not in raw_df.columns:
            raw_df[column] = ""
    if "metrics" not in raw_df.columns:
        raw_df["metrics"] = [{} for _ in range(len(raw_df))]
    raw_df["stock_code"] = raw_df["stock_code"].astype(str).str.zfill(6)
    raw_df["event_date"] = raw_df["event_date"].astype(str)
    raw_df["event_time_hhmm"] = raw_df["event_time_hhmm"].fillna("").astype(str)
    raw_df["signal_bias"] = raw_df["signal_bias"].fillna("").astype(str)
    raw_df["corp_name"] = raw_df["corp_name"].fillna("").astype(str)
    raw_df["title"] = raw_df["title"].fillna("").astype(str)
    raw_df["rcp_no"] = raw_df["rcp_no"].fillna("").astype(str)
    raw_df["event_type"] = raw_df["event_type"].fillna("").astype(str)
    raw_df["sector"] = raw_df["sector"].fillna("").astype(str)
    raw_df["is_correction_title"] = raw_df["metrics"].map(
        lambda metrics: bool(metrics.get("is_correction_title")) if isinstance(metrics, dict) else False
    )
    raw_df["event_group"] = raw_df["metrics"].map(
        lambda metrics: _clean_text(metrics.get("event_group")) if isinstance(metrics, dict) else ""
    )

    if not detail_df.empty:
        enrichment_cols = [
            "stock_code",
            "rcp_no",
            "event_type",
            "ret_1d",
            "ret_3d",
            "ret_5d",
            "ret_10d",
            "max_drawdown_5d",
            "sector",
        ]
        enrichment_df = detail_df[enrichment_cols].copy()
        enrichment_df["rcp_no"] = enrichment_df["rcp_no"].astype(str)
        enrichment_df["event_type"] = enrichment_df["event_type"].astype(str)
        enrichment_df = enrichment_df.drop_duplicates(subset=["stock_code", "rcp_no", "event_type"], keep="last")
        raw_df = raw_df.merge(
            enrichment_df,
            how="left",
            on=["stock_code", "rcp_no", "event_type"],
            suffixes=("", "_detail"),
        )
        raw_df["sector"] = raw_df["sector"].where(raw_df["sector"].astype(str).str.len() > 0, raw_df.get("sector_detail"))

    symbol_summary = build_symbol_summary(raw_df, top_n=max(500, raw_df["stock_code"].astype(str).nunique() + 20))
    symbol_df = pd.DataFrame(symbol_summary.get("top_symbols") or [])
    if not symbol_df.empty:
        symbol_df["symbol"] = symbol_df["symbol"].astype(str).str.zfill(6)
    return symbol_df, raw_df


def _event_impact_note(row: dict[str, Any]) -> tuple[str, float]:
    event_type = _clean_text(row.get("event_type") or row.get("latest_event_type")).upper()
    title = _clean_text(row.get("title") or row.get("latest_title"))
    signal_bias = _clean_text(row.get("signal_bias") or row.get("latest_signal_bias")).lower()
    avg_ret_5d = _safe_float(row.get("avg_ret_5d"))
    is_correction = bool(row.get("is_correction_title"))
    confidence = 0.45
    if event_type == "DIVIDEND":
        note = "배당 성격과 payout 변화에 따라 재평가가 붙을 수 있어 배당수익률과 특별배당 여부를 같이 확인하는 편이 낫습니다."
        confidence = 0.62
    elif event_type == "STOCK_SPLIT":
        note = "분할은 가치 자체보다 유동성과 접근성 변화를 먼저 보게 만드는 편이라 거래대금과 선반영 정도를 함께 보는 편이 낫습니다."
        confidence = 0.58
    elif event_type == "REVERSE_SPLIT_REDUCTION":
        note = "병합·감자는 유통주식 수와 희석·집중 구조를 함께 보게 하므로 단순 호재보다 자본구조 변화로 읽는 편이 낫습니다."
        confidence = 0.57
    elif event_type == "MERGER":
        note = "합병은 교환비율과 피합병 자산의 질에 따라 적정가 기준선이 달라질 수 있어 재무 구조와 시너지를 함께 보는 편이 낫습니다."
        confidence = 0.61
    elif event_type == "SPINOFF":
        note = "분할은 가치 분리와 할인 해소 기대를 만들 수 있지만 분할 뒤 체력 차이가 커질 수 있어 분할 구조를 같이 보는 편이 낫습니다."
        confidence = 0.59
    elif event_type in {"BUYBACK", "STOCK_CANCELLATION"}:
        note = "주주환원 이벤트라 주당가치에는 우호적이지만 이미 주가에 선반영됐는지 같이 보는 편이 낫습니다."
        confidence = 0.64
    elif event_type == "BUYBACK_DISPOSAL":
        note = "자사주 처분은 유통주식 확대나 오버행으로 읽히는 경우가 많아 취득·소각과 분리해서 보는 편이 낫습니다."
        confidence = 0.58
    elif event_type == "SUPPLY_CONTRACT":
        note = "수주 금액이 연매출 대비 의미 있는지와 이익률이 실제로 추정치를 바꿀 만큼 큰지가 핵심입니다."
        confidence = 0.66
    elif event_type == "PERF_PRELIM":
        note = "잠정실적은 영업이익 추정을 직접 다시 쓰는 재료에 가까워 surprise 폭과 다음 분기 연장선을 같이 보는 편이 낫습니다."
        confidence = 0.68
    elif event_type == "OTHER_DISCLOSURE":
        note = "핵심 제목은 잡았지만 정형 분류 밖의 공시라 원문 제목과 DART 본문을 함께 보는 편이 낫습니다."
        confidence = 0.44
    else:
        note = "이벤트 성격은 확인되지만 실제 가격 반영 폭은 과거 반응과 함께 읽는 편이 낫습니다."
    if signal_bias == "negative":
        confidence = max(0.35, confidence - 0.08)
    elif signal_bias == "positive":
        confidence = min(0.82, confidence + 0.04)
    if avg_ret_5d:
        note += f" 최근 5일 평균 반응은 {avg_ret_5d:+.1f}%였습니다."
    if is_correction:
        note += " 정정 공시가 포함된 제목입니다."
    if title:
        note += f" 공시 제목은 `{title}` 입니다."
    return note, confidence


def _event_revision_state(row: dict[str, Any]) -> tuple[bool, str]:
    event_type = _clean_text(row.get("event_type") or row.get("latest_event_type")).upper()
    title = _clean_text(row.get("title") or row.get("latest_title"))
    if event_type == "SUPPLY_CONTRACT":
        return True, "수주 금액과 이익률 가정을 영업이익 추정 보정에 반영"
    if event_type == "SUPPLY_UPDATE":
        return True, "수주 변경 공시를 영업이익 추정 보정에 반영"
    if event_type == "SUPPLY_TERMINATION":
        return True, "수주 해지 공시를 영업이익 추정 보정에 보수 반영"
    if event_type == "PERF_PRELIM":
        return True, "잠정실적을 실제 실적·연간 추정 보정에 반영"
    if event_type == "SALES_VARIATION":
        return True, "매출·손익구조 변동 공시를 연간 추정 보정에 반영"
    if event_type == "CORRECTION" and any(keyword in title for keyword in ("단일판매", "잠정", "실적", "매출액", "손익구조")):
        return True, "정정 공시 내용을 추정 보정 점검 대상으로 반영"
    return False, ""


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _fallback_excerpt(text: Any) -> str:
    clean = _clean_text(text)
    return clean[:280] if clean else ""


def _visible_quote_price(row: dict[str, Any]) -> float | None:
    status = _clean_text(row.get("price_status"))
    price = _safe_float(row.get("price"), float("nan"))
    if math.isfinite(price) and price > 0:
        return price
    if status == "공식종가 fallback":
        official_close = _safe_float(row.get("official_close"), float("nan"))
        if math.isfinite(official_close) and official_close > 0:
            return official_close
    return None


def _make_metric(label: str, value: str) -> dict[str, str]:
    return {"label": label, "value": value}


def _event_price_context(symbol: str, listing_map: dict[str, dict[str, Any]], quote_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    listing_row = listing_map.get(symbol, {})
    quote_row = quote_map.get(symbol, {})
    current_price = _safe_float(_visible_quote_price(quote_row), float("nan"))
    market_cap = _safe_float(listing_row.get("Marcap"), float("nan"))
    shares = _safe_float(listing_row.get("Stocks"), float("nan"))
    return {
        "current_price": current_price if math.isfinite(current_price) and current_price > 0 else None,
        "market_cap": market_cap if math.isfinite(market_cap) and market_cap > 0 else None,
        "share_count": shares if math.isfinite(shares) and shares > 0 else None,
    }


def _event_metric_value(metric: dict[str, Any]) -> str:
    return _clean_text(metric.get("value"))


def _event_metric_label(metric: dict[str, Any]) -> str:
    return _clean_text(metric.get("label"))


def _format_count(value: Any) -> str:
    number = _safe_float(value, float("nan"))
    if not math.isfinite(number) or number <= 0:
        return "-"
    return f"{number:,.0f}주"


def _format_eok(value: Any) -> str:
    number = _safe_float(value, float("nan"))
    if not math.isfinite(number):
        return "-"
    return f"{number / 100_000_000:+.1f}억원"


def _event_liquidity_effect(
    event_type: str,
    parsed_details: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    share_count_before = _safe_float(parsed_details.get("share_count_before"), float("nan"))
    if not math.isfinite(share_count_before) or share_count_before <= 0:
        share_count_before = _safe_float(context.get("share_count"), float("nan"))
    share_count_before = share_count_before if math.isfinite(share_count_before) and share_count_before > 0 else None

    share_count_after = _safe_float(parsed_details.get("share_count_after"), float("nan"))
    share_count_after = share_count_after if math.isfinite(share_count_after) and share_count_after > 0 else None

    delta_shares = None
    planned_shares = _safe_float(parsed_details.get("buyback_or_disposal_shares"), float("nan"))
    if share_count_before and share_count_after:
        delta_shares = share_count_after - share_count_before
    elif share_count_before:
        if event_type in {"MERGER"}:
            merger_new_shares = _safe_float(parsed_details.get("merger_new_shares"), float("nan"))
            if math.isfinite(merger_new_shares) and merger_new_shares > 0:
                delta_shares = merger_new_shares
                share_count_after = share_count_before + merger_new_shares
        elif event_type in {"DILUTION"}:
            new_shares = _safe_float(parsed_details.get("new_shares"), float("nan"))
            if math.isfinite(new_shares) and new_shares > 0:
                delta_shares = new_shares
                share_count_after = share_count_before + new_shares
        elif event_type in {"BUYBACK", "STOCK_CANCELLATION", "REVERSE_SPLIT_REDUCTION"} and math.isfinite(planned_shares) and planned_shares > 0:
            delta_shares = -planned_shares
            share_count_after = max(0.0, share_count_before - planned_shares)
        elif event_type in {"BUYBACK_DISPOSAL"} and math.isfinite(planned_shares) and planned_shares > 0:
            delta_shares = planned_shares
            share_count_after = share_count_before + planned_shares

    liquidity_float_delta_pct = None
    if share_count_before and delta_shares is not None:
        liquidity_float_delta_pct = delta_shares / share_count_before * 100.0
    elif share_count_before and share_count_after:
        liquidity_float_delta_pct = (share_count_after - share_count_before) / share_count_before * 100.0

    current_price = _safe_float(context.get("current_price"), float("nan"))
    liquidity_mcap_delta = None
    if liquidity_float_delta_pct is not None and share_count_before and math.isfinite(current_price) and current_price > 0:
        effective_delta_shares = (share_count_after - share_count_before) if share_count_after is not None else delta_shares
        if effective_delta_shares is not None:
            liquidity_mcap_delta = effective_delta_shares * current_price

    return {
        "share_count_before": share_count_before,
        "share_count_after": share_count_after,
        "liquidity_float_delta_pct": liquidity_float_delta_pct,
        "liquidity_mcap_delta": liquidity_mcap_delta,
    }


def _build_event_doc(
    row: dict[str, Any],
    sector_map: dict[str, str],
    listing_map: dict[str, dict[str, Any]],
    quote_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    symbol = _norm_symbol(row.get("stock_code"))
    title = _clean_text(row.get("title"))
    original_event_type = _clean_text(row.get("event_type")).upper()
    event_type = _clean_text(classify_disclosure_event(title) or original_event_type or "OTHER_DISCLOSURE").upper()
    revision_applied, revision_summary = _event_revision_state(dict(row))
    rcp_no = _clean_text(row.get("rcp_no"))
    parsed_payload = get_parsed_event_document(rcp_no, event_type, title)
    parsed_details = dict(parsed_payload.get("parsed_event_details") or {})
    context = _event_price_context(symbol, listing_map, quote_map)
    liquidity = _event_liquidity_effect(event_type, parsed_details, context)

    share_count_before = liquidity.get("share_count_before")
    share_count_after = liquidity.get("share_count_after")
    float_delta_pct = liquidity.get("liquidity_float_delta_pct")
    liquidity_mcap_delta = liquidity.get("liquidity_mcap_delta")
    buyback_amount = _safe_float(parsed_details.get("buyback_amount"), float("nan"))
    market_cap = _safe_float(context.get("market_cap"), float("nan"))
    buyback_pct_mcap = None
    if math.isfinite(buyback_amount) and buyback_amount > 0 and math.isfinite(market_cap) and market_cap > 0:
        buyback_pct_mcap = buyback_amount / market_cap * 100.0

    dividend_per_share = _safe_float(parsed_details.get("dividend_per_share"), float("nan"))
    dividend_yield_pct = _safe_float(parsed_details.get("dividend_yield_pct"), float("nan"))
    current_price = _safe_float(context.get("current_price"), float("nan"))
    if (not math.isfinite(dividend_yield_pct) or dividend_yield_pct <= 0) and math.isfinite(dividend_per_share) and dividend_per_share > 0 and math.isfinite(current_price) and current_price > 0:
        dividend_yield_pct = dividend_per_share / current_price * 100.0
    dividend_yield_pct = dividend_yield_pct if math.isfinite(dividend_yield_pct) and dividend_yield_pct > 0 else None
    dividend_per_share = dividend_per_share if math.isfinite(dividend_per_share) and dividend_per_share > 0 else None

    merger_price = _safe_float(parsed_details.get("merger_price"), float("nan"))
    merger_price = merger_price if math.isfinite(merger_price) and merger_price > 0 else None
    contract_amount = _safe_float(parsed_details.get("contract_amount"), float("nan"))
    contract_amount = contract_amount if math.isfinite(contract_amount) and contract_amount > 0 else None
    sales_ratio_pct = _safe_float(parsed_details.get("sales_ratio_pct"), float("nan"))
    sales_ratio_pct = sales_ratio_pct if math.isfinite(sales_ratio_pct) and sales_ratio_pct > 0 else None
    dilution_pct = _safe_float(parsed_details.get("dilution_pct"), float("nan"))
    dilution_pct = dilution_pct if math.isfinite(dilution_pct) and dilution_pct > 0 else None

    parsed_details.update(
        {
            "share_count_before": share_count_before,
            "share_count_after": share_count_after,
            "liquidity_float_delta_pct": float_delta_pct,
            "liquidity_mcap_delta": liquidity_mcap_delta,
            "buyback_pct_mcap": buyback_pct_mcap,
            "dividend_yield_pct": dividend_yield_pct,
        }
    )

    key_metrics = [metric for metric in parsed_payload.get("event_key_metrics") or [] if _event_metric_label(metric) and _event_metric_value(metric)]
    if float_delta_pct is not None and not any("유통주식" in _event_metric_label(metric) for metric in key_metrics):
        key_metrics.append(_make_metric("유통주식 변화", f"{float_delta_pct:+.2f}%"))
    if buyback_pct_mcap is not None and not any("시총 대비" in _event_metric_label(metric) for metric in key_metrics):
        key_metrics.append(_make_metric("시총 대비", f"{buyback_pct_mcap:.2f}%"))

    detail_summary = _clean_text(parsed_payload.get("event_detail_summary"))
    if float_delta_pct is not None and "유통주식" not in detail_summary and "희석률" not in detail_summary:
        detail_summary = f"{detail_summary} · 유통주식 {float_delta_pct:+.2f}%".strip(" ·")
    if buyback_pct_mcap is not None and "시총 대비" not in detail_summary:
        detail_summary = f"{detail_summary} · 시총 대비 {buyback_pct_mcap:.2f}%".strip(" ·")

    if share_count_before and share_count_after:
        structure_summary = f"보통주 기준 {share_count_before:,.0f}주 → {share_count_after:,.0f}주"
    else:
        structure_summary = _first_nonempty(
            _clean_text(parsed_details.get("merger_method")),
            _clean_text(parsed_details.get("capital_change_method")),
            _clean_text(parsed_details.get("dilution_method")),
            _clean_text(parsed_details.get("contract_name")),
        )

    liquidity_summary = ""
    if float_delta_pct is not None:
        liquidity_summary = f"유통주식 변화 {float_delta_pct:+.2f}%"
        if liquidity_mcap_delta is not None:
            liquidity_summary += f" · 현재가 기준 유동 시총 변화 {liquidity_mcap_delta / 100_000_000:+.1f}억원"
    elif buyback_pct_mcap is not None:
        liquidity_summary = f"예정 금액 {buyback_amount / 100_000_000:.1f}억원 · 현재 시가총액 대비 {buyback_pct_mcap:.2f}%"
    elif dividend_yield_pct is not None:
        liquidity_summary = f"현재가 기준 배당수익률 {dividend_yield_pct:.2f}%"

    source_excerpt = _clean_text(parsed_payload.get("event_source_excerpt"))
    if not source_excerpt:
        source_excerpt = _fallback_excerpt(title)

    summary_metrics = " · ".join(
        f"{_event_metric_label(metric)} {_event_metric_value(metric)}"
        for metric in key_metrics[:2]
    )
    core_metric = summary_metrics or detail_summary or "-"

    confidence = 0.36
    confidence += min(0.28, len(key_metrics) * 0.07)
    if source_excerpt:
        confidence += 0.08
    if revision_applied:
        confidence += 0.06
    if _clean_text(parsed_payload.get("document_format")):
        confidence += 0.04
    confidence = max(0.35, min(0.88, confidence))

    return {
        "_id": f"{symbol}:{rcp_no}:{event_type}",
        "symbol": symbol,
        "name": _clean_text(row.get("corp_name")),
        "sector": sector_map.get(symbol, _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)),
        "event_date": _clean_text(row.get("event_date")),
        "event_time_hhmm": _clean_text(row.get("event_time_hhmm")),
        "event_type": event_type,
        "event_type_original": original_event_type,
        "event_label": EVENT_LABELS.get(event_type, event_type or "기타"),
        "event_group": _event_group_for_type(event_type),
        "is_correction_title": bool(row.get("is_correction_title")),
        "title": title,
        "impact_note": detail_summary or source_excerpt or title,
        "impact_confidence": confidence,
        "signal_bias": _clean_text(row.get("signal_bias")),
        "disclosure_revision_applied": revision_applied,
        "disclosure_revision_summary": revision_summary,
        "core_metric": core_metric,
        "avg_ret_1d": _safe_float(row.get("ret_1d")),
        "avg_ret_5d": _safe_float(row.get("ret_5d")),
        "avg_mdd_5d": _safe_float(row.get("max_drawdown_5d")),
        "event_detail_summary": detail_summary,
        "event_structure_summary": structure_summary,
        "event_liquidity_summary": liquidity_summary,
        "event_key_metrics": key_metrics,
        "event_source_excerpt": source_excerpt,
        "parsed_event_details": parsed_details,
        "liquidity_float_delta_pct": float_delta_pct,
        "share_count_before": share_count_before,
        "share_count_after": share_count_after,
        "merger_ratio": _clean_text(parsed_details.get("merger_ratio")),
        "merger_price": merger_price,
        "dividend_per_share": dividend_per_share,
        "dividend_yield_pct": dividend_yield_pct,
        "buyback_amount": buyback_amount if math.isfinite(buyback_amount) and buyback_amount > 0 else None,
        "buyback_pct_mcap": buyback_pct_mcap,
        "dilution_pct": dilution_pct,
        "contract_amount": contract_amount,
        "sales_ratio_pct": sales_ratio_pct,
        "dart_url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}" if rcp_no else "",
        "correction_applied": False,
        "correction_count": 0,
        "latest_correction_date": "",
        "correction_summary": "",
        "_receipt_key": f"{symbol}:{rcp_no}",
        "_display_key": "|".join([symbol, event_type, _normalize_title_key(title), _clean_text(row.get("event_date"))]),
        "_is_correction_record": original_event_type == "CORRECTION" or bool(row.get("is_correction_title")),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _build_sector_projection_maps(
    fair_df: pd.DataFrame,
    cards_summary: dict[str, Any],
    event_detail_df: pd.DataFrame,
) -> tuple[dict[str, str], dict[str, str]]:
    sector_map: dict[str, str] = {}
    source_map: dict[str, str] = {}
    source_priority = {"wics": 1, "cache": 2, "event": 3, "cards": 4, "default": 99}

    def register(symbol: Any, sector: Any, source: str) -> None:
        normalized_symbol = _norm_symbol(symbol)
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

    sector_cache_payload = _read_json(ROOT_DIR / "cache" / "sector_cache.json")
    if isinstance(sector_cache_payload, dict):
        for symbol, sector in sector_cache_payload.items():
            register(symbol, sector, "cache")

    for item in cards_summary.get("cards", []) or []:
        register(item.get("symbol"), item.get("sector"), "cards")

    if event_detail_df is not None and not event_detail_df.empty and "sector" in event_detail_df.columns:
        for _, row in event_detail_df.iterrows():
            register(row.get("stock_code"), row.get("sector"), "event")

    return sector_map, source_map


def _build_stock_master_docs(listing_df: pd.DataFrame, sector_map: dict[str, str], sector_source_map: dict[str, str]) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    if listing_df.empty:
        return docs
    for _, row in listing_df.iterrows():
        symbol = _norm_symbol(row.get("Code"))
        name = _clean_text(row.get("Name"))
        if not symbol or not name:
            continue
        market = _clean_text(row.get("Market"))
        sector = sector_map.get(symbol, UNCLASSIFIED_SECTOR)
        excluded_reason = _classify_excluded_reason(name, market)
        if excluded_reason:
            continue
        docs.append(
            {
                "_id": symbol,
                "symbol": symbol,
                "code": symbol,
                "name": name,
                "market": market,
                "sector": sector,
                "sector_source": sector_source_map.get(symbol, "default"),
                "valuation_family": _valuation_family_label(sector),
                "excluded_reason": excluded_reason,
                "search_initials": _hangul_initials(name),
                "search_english": _clean_text(name).upper(),
                "search_aliases": [name, symbol, _hangul_initials(name)],
                "marcap": _safe_float(row.get("Marcap")),
                "close": _safe_float(row.get("Close")),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
    return docs


def _build_fair_value_docs(fair_df: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, str]]:
    docs: list[dict[str, Any]] = []
    sector_map: dict[str, str] = {}
    if fair_df.empty:
        return docs, sector_map
    for _, row in fair_df.iterrows():
        symbol = _norm_symbol(row.get("symbol"))
        if not symbol:
            continue
        sector = _clean_text(row.get("sector"))
        sector_map[symbol] = sector
        docs.append(
            {
                "_id": symbol,
                "symbol": symbol,
                "name": _clean_text(row.get("name")),
                "sector": sector,
                "sector_source": _clean_text(row.get("sector_source")) or "default",
                "current_price": (
                    _safe_float(row.get("current_price"), float("nan"))
                    if _clean_text(row.get("current_price_status")) != "업데이트 지연"
                    and _safe_float(row.get("current_price"), float("nan")) > 0
                    else None
                ),
                "current_price_source": _clean_text(row.get("current_price_source")) or "krx_listing_close",
                "current_price_captured_at": _clean_text(row.get("current_price_captured_at")),
                "current_price_freshness": _clean_text(row.get("current_price_freshness")),
                "current_price_status": _clean_text(row.get("current_price_status")) or "자료 없음",
                "official_close": _safe_float(row.get("official_close")) if _safe_float(row.get("official_close"), float("nan")) > 0 else None,
                "official_close_date": _clean_text(row.get("official_close_date")),
                "fair_value_bear": _safe_float(row.get("fair_value_bear")),
                "fair_value_base": _safe_float(row.get("fair_value_base")),
                "fair_value_bull": _safe_float(row.get("fair_value_bull")),
                "fair_value_gap_pct": _safe_float(row.get("fair_value_gap_pct")),
                "fair_value_confidence_score": _safe_float(row.get("fair_value_confidence_score")),
                "valuation_family": _clean_text(row.get("valuation_family")),
                "valuation_primary_method": _clean_text(row.get("valuation_primary_method")),
                "valuation_basis_label": _clean_text(row.get("valuation_basis_label")),
                "valuation_basis_period": _clean_text(row.get("valuation_basis_period")),
                "valuation_input_source": _clean_text(row.get("valuation_input_source")),
                "valuation_multiple_current": _safe_float(row.get("valuation_multiple_current")) if _safe_float(row.get("valuation_multiple_current"), float("nan")) > 0 else None,
                "valuation_multiple_target": _safe_float(row.get("valuation_multiple_target")) if _safe_float(row.get("valuation_multiple_target"), float("nan")) > 0 else None,
                "valuation_multiple_unit": _clean_text(row.get("valuation_multiple_unit")) or "배",
                "operating_profit_yield_pct": _safe_float(row.get("operating_profit_yield_pct")) if _safe_float(row.get("operating_profit_yield_pct"), float("nan")) > 0 else None,
                "operating_margin_pct": _safe_float(row.get("operating_margin_pct")) if _safe_float(row.get("operating_margin_pct"), float("nan")) > 0 else None,
                "roe_current": _safe_float(row.get("roe_current")) if _safe_float(row.get("roe_current"), float("nan")) > 0 else None,
                "profitability_metric_label": _clean_text(row.get("profitability_metric_label")),
                "profitability_metric_value": _safe_float(row.get("profitability_metric_value")) if _safe_float(row.get("profitability_metric_value"), float("nan")) > 0 else None,
                "valuation_summary_paragraph": _clean_text(row.get("valuation_summary_paragraph")),
                "valuation_method_detail": _clean_text(row.get("valuation_method_detail")),
                "valuation_formula_hint": _clean_text(row.get("valuation_formula_hint")),
                "profitability_formula_hint": _clean_text(row.get("profitability_formula_hint")),
                "tp_formula_label": _clean_text(row.get("tp_formula_label")),
                "tp_basis_metric_label": _clean_text(row.get("tp_basis_metric_label")),
                "tp_basis_metric_value": _safe_float(row.get("tp_basis_metric_value")) if _safe_float(row.get("tp_basis_metric_value"), float("nan")) > 0 else None,
                "tp_peer_median_multiple": _safe_float(row.get("tp_peer_median_multiple")) if _safe_float(row.get("tp_peer_median_multiple"), float("nan")) > 0 else None,
                "tp_peer_q25_multiple": _safe_float(row.get("tp_peer_q25_multiple")) if _safe_float(row.get("tp_peer_q25_multiple"), float("nan")) > 0 else None,
                "tp_peer_q75_multiple": _safe_float(row.get("tp_peer_q75_multiple")) if _safe_float(row.get("tp_peer_q75_multiple"), float("nan")) > 0 else None,
                "tp_peer_count_used": _safe_int(row.get("tp_peer_count_used")),
                "tp_sanity_low_price": _safe_float(row.get("tp_sanity_low_price")) if _safe_float(row.get("tp_sanity_low_price"), float("nan")) > 0 else None,
                "tp_sanity_high_price": _safe_float(row.get("tp_sanity_high_price")) if _safe_float(row.get("tp_sanity_high_price"), float("nan")) > 0 else None,
                "tp_sanity_bound_applied": bool(row.get("tp_sanity_bound_applied")),
                "tp_revision_contributors": _clean_text(row.get("tp_revision_contributors")),
                "tp_explanation_steps": _clean_text_list(row.get("tp_explanation_steps")),
                "tp_basis_summary": _clean_text(row.get("tp_basis_summary")),
                "tp_peer_set_summary": _clean_text(row.get("tp_peer_set_summary")),
                "tp_bound_summary": _clean_text(row.get("tp_bound_summary")),
                "tp_hidden_reason_detail": _clean_text(row.get("tp_hidden_reason_detail")),
                "valuation_anchor_mix": _clean_text(row.get("valuation_anchor_mix")),
                "valuation_peer_group": _clean_text(row.get("valuation_peer_group")),
                "valuation_reason_summary": _clean_text(row.get("valuation_reason_summary")),
                "valuation_missing_inputs": _clean_text(row.get("valuation_missing_inputs")),
                "valuation_status_label": _clean_text(row.get("fair_value_status_label")),
                "valuation_driver": _clean_text(row.get("valuation_driver")),
                "valuation_tp_visible": bool(row.get("valuation_tp_visible")),
                "valuation_tp_hidden_reason": _clean_text(row.get("valuation_tp_hidden_reason")),
                "valuation_peer_direct_count": _safe_int(row.get("valuation_peer_direct_count")),
                "valuation_tier": _clean_text(row.get("valuation_tier")),
                "valuation_proxy_used": bool(row.get("valuation_proxy_used")),
                "valuation_coverage_reason": _clean_text(row.get("valuation_coverage_reason")),
                "valuation_revision_op_pct": _safe_float(row.get("valuation_revision_op_pct")) if _safe_float(row.get("valuation_revision_op_pct"), float("nan")) else None,
                "valuation_revision_net_pct": _safe_float(row.get("valuation_revision_net_pct")) if _safe_float(row.get("valuation_revision_net_pct"), float("nan")) else None,
                "analyst_report_count": _safe_int(row.get("analyst_report_count")),
                "analyst_broker_diversity": _safe_int(row.get("analyst_broker_diversity")),
                "analyst_target_upside_pct": _safe_float(row.get("valuation_analyst_target_upside_pct")) if _safe_float(row.get("valuation_analyst_target_upside_pct"), float("nan")) else None,
                "analyst_latest_title": _clean_text(row.get("analyst_latest_title")),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
    return docs, sector_map


def _build_sector_dashboard_docs(
    listing_df: pd.DataFrame,
    fair_df: pd.DataFrame,
    sector_thesis: dict[str, Any],
    wics_meta: dict[str, Any],
    quote_map: dict[str, dict[str, Any]],
    flow_docs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    by_sector = sector_thesis.get("by_sector") or {}
    wics_sectors = wics_meta.get("sectors") or {}
    fair_sector_groups = defaultdict(list)
    flow_sector_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not fair_df.empty:
        for _, row in fair_df.iterrows():
            sector = _clean_text(row.get("sector"))
            symbol = _norm_symbol(row.get("symbol"))
            if sector and symbol:
                fair_sector_groups[sector].append(row)
    for row in flow_docs:
        sector = _normalize_sector(row.get("sector"), UNCLASSIFIED_SECTOR)
        symbol = _norm_symbol(row.get("symbol"))
        if sector and symbol:
            flow_sector_groups[sector].append(row)
    for sector, thesis in by_sector.items():
        sector_rows = fair_sector_groups.get(sector, [])
        flow_rows = flow_sector_groups.get(sector, [])
        change_values: list[float] = []
        highlight: list[dict[str, Any]] = []
        for row in sector_rows[:40]:
            symbol = _norm_symbol(row.get("symbol"))
            quote = quote_map.get(symbol) or {}
            if quote.get("change_rate_pct") is not None:
                change_values.append(_safe_float(quote.get("change_rate_pct")))
            highlight.append(
                {
                    "symbol": symbol,
                    "name": _clean_text(row.get("name")),
                    "change_rate_pct": _safe_float(quote.get("change_rate_pct")),
                    "fair_value_gap_pct": _safe_float(row.get("fair_value_gap_pct")),
                    "fair_value_confidence_score": _safe_float(row.get("fair_value_confidence_score")),
                }
            )
        highlight.sort(key=lambda item: (item["fair_value_gap_pct"], item["change_rate_pct"]), reverse=True)
        foreign_1d = sum(_safe_float(row.get("foreign_1d_eok")) for row in flow_rows)
        inst_1d = sum(_safe_float(row.get("inst_1d_eok")) for row in flow_rows)
        retail_1d = sum(_safe_float(row.get("retail_1d_eok")) for row in flow_rows)
        foreign_3d = sum(_safe_float(row.get("foreign_3d_eok")) for row in flow_rows)
        inst_3d = sum(_safe_float(row.get("inst_3d_eok")) for row in flow_rows)
        retail_3d = sum(_safe_float(row.get("retail_3d_eok")) for row in flow_rows)
        foreign_5d = sum(_safe_float(row.get("foreign_5d_eok")) for row in flow_rows)
        inst_5d = sum(_safe_float(row.get("inst_5d_eok")) for row in flow_rows)
        retail_5d = sum(_safe_float(row.get("retail_5d_eok")) for row in flow_rows)
        foreign_10d = sum(_safe_float(row.get("foreign_10d_eok")) for row in flow_rows)
        inst_10d = sum(_safe_float(row.get("inst_10d_eok")) for row in flow_rows)
        retail_10d = sum(_safe_float(row.get("retail_10d_eok")) for row in flow_rows)
        leaders = sorted(flow_rows, key=lambda item: _safe_float(item.get("combined_3d_eok")), reverse=True)[:6]
        laggards = sorted(flow_rows, key=lambda item: _safe_float(item.get("combined_3d_eok")))[:6]
        wics_info = wics_sectors.get(next((key for key in wics_sectors.keys() if key.endswith(sector)), ""), {})
        universe_status = (wics_info.get("universe_status") or {}) if isinstance(wics_info, dict) else {}
        docs.append(
            {
                "_id": sector,
                "sector": sector,
                "sector_change_pct": round(sum(change_values) / len(change_values), 2) if change_values else 0.0,
                "flow_score": _safe_float(thesis.get("flow_lens_score")),
                "quant_score": _safe_float(thesis.get("quant_lens_score")),
                "macro_score": _safe_float(thesis.get("macro_lens_score")),
                "final_score": _safe_float(thesis.get("final_sector_score")),
                "final_label": _clean_text(thesis.get("final_label")),
                "action_hint": _clean_text(thesis.get("action_hint")),
                "human_summary": _clean_text(thesis.get("human_summary")),
                "leader_name": _clean_text(thesis.get("leader_name")),
                "top_candidates": thesis.get("top_candidates") or [],
                "watch_symbols": highlight[:6],
                "foreign_1d_eok": round(foreign_1d, 1),
                "inst_1d_eok": round(inst_1d, 1),
                "retail_1d_eok": round(retail_1d, 1),
                "foreign_3d_eok": round(foreign_3d, 1),
                "inst_3d_eok": round(inst_3d, 1),
                "retail_3d_eok": round(retail_3d, 1),
                "foreign_5d_eok": round(foreign_5d, 1),
                "inst_5d_eok": round(inst_5d, 1),
                "retail_5d_eok": round(retail_5d, 1),
                "foreign_10d_eok": round(foreign_10d, 1),
                "inst_10d_eok": round(inst_10d, 1),
                "retail_10d_eok": round(retail_10d, 1),
                "combined_1d_eok": round(foreign_1d + inst_1d, 1),
                "combined_3d_eok": round(foreign_3d + inst_3d, 1),
                "combined_5d_eok": round(foreign_5d + inst_5d, 1),
                "combined_10d_eok": round(foreign_10d + inst_10d, 1),
                "positive_flow_count_3d": sum(1 for row in flow_rows if _safe_float(row.get("combined_3d_eok")) > 0),
                "negative_flow_count_3d": sum(1 for row in flow_rows if _safe_float(row.get("combined_3d_eok")) < 0),
                "positive_flow_count_10d": sum(1 for row in flow_rows if _safe_float(row.get("combined_10d_eok")) > 0),
                "negative_flow_count_10d": sum(1 for row in flow_rows if _safe_float(row.get("combined_10d_eok")) < 0),
                "sector_flow_leaders": [
                    {
                        "symbol": _norm_symbol(item.get("symbol")),
                        "name": _clean_text(item.get("name")),
                        "combined_3d_eok": _safe_float(item.get("combined_3d_eok")),
                        "foreign_3d_eok": _safe_float(item.get("foreign_3d_eok")),
                        "inst_3d_eok": _safe_float(item.get("inst_3d_eok")),
                        "retail_3d_eok": _safe_float(item.get("retail_3d_eok")),
                    }
                    for item in leaders
                ],
                "sector_flow_laggards": [
                    {
                        "symbol": _norm_symbol(item.get("symbol")),
                        "name": _clean_text(item.get("name")),
                        "combined_3d_eok": _safe_float(item.get("combined_3d_eok")),
                        "foreign_3d_eok": _safe_float(item.get("foreign_3d_eok")),
                        "inst_3d_eok": _safe_float(item.get("inst_3d_eok")),
                        "retail_3d_eok": _safe_float(item.get("retail_3d_eok")),
                    }
                    for item in laggards
                ],
                "wics_universe_status": _clean_text(universe_status.get("label")),
                "wics_universe_reason": _clean_text(universe_status.get("reason")),
                "wics_dynamic_count": _safe_int(universe_status.get("dynamic_count")),
                "wics_history_label": _clean_text(universe_status.get("history_label")),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
    docs.sort(key=lambda item: (item["final_score"], item["sector_change_pct"]), reverse=True)
    return docs


def _build_stock_flow_docs(
    flow_source_rows: list[dict[str, Any]],
    sector_map: dict[str, str],
) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for row in flow_source_rows:
        symbol = _norm_symbol(row.get("symbol"))
        if not symbol:
            continue
        docs.append(
            {
                "_id": symbol,
                "symbol": symbol,
                "name": _clean_text(row.get("name")),
                "market": _clean_text(row.get("market")),
                "sector": _normalize_sector(row.get("sector"), sector_map.get(symbol) or UNCLASSIFIED_SECTOR),
                "foreign_1d_eok": _safe_float(row.get("foreign_1d_eok")) if row.get("foreign_1d_eok") is not None else None,
                "inst_1d_eok": _safe_float(row.get("inst_1d_eok")) if row.get("inst_1d_eok") is not None else None,
                "retail_1d_eok": _safe_float(row.get("retail_1d_eok")) if row.get("retail_1d_eok") is not None else None,
                "foreign_3d_eok": _safe_float(row.get("foreign_3d_eok")) if row.get("foreign_3d_eok") is not None else None,
                "inst_3d_eok": _safe_float(row.get("inst_3d_eok")) if row.get("inst_3d_eok") is not None else None,
                "retail_3d_eok": _safe_float(row.get("retail_3d_eok")) if row.get("retail_3d_eok") is not None else None,
                "foreign_5d_eok": _safe_float(row.get("foreign_5d_eok")) if row.get("foreign_5d_eok") is not None else None,
                "inst_5d_eok": _safe_float(row.get("inst_5d_eok")) if row.get("inst_5d_eok") is not None else None,
                "retail_5d_eok": _safe_float(row.get("retail_5d_eok")) if row.get("retail_5d_eok") is not None else None,
                "foreign_10d_eok": _safe_float(row.get("foreign_10d_eok")) if row.get("foreign_10d_eok") is not None else None,
                "inst_10d_eok": _safe_float(row.get("inst_10d_eok")) if row.get("inst_10d_eok") is not None else None,
                "retail_10d_eok": _safe_float(row.get("retail_10d_eok")) if row.get("retail_10d_eok") is not None else None,
                "combined_1d_eok": _safe_float(row.get("combined_1d_eok")) if row.get("combined_1d_eok") is not None else None,
                "combined_3d_eok": _safe_float(row.get("combined_3d_eok")) if row.get("combined_3d_eok") is not None else None,
                "combined_5d_eok": _safe_float(row.get("combined_5d_eok")) if row.get("combined_5d_eok") is not None else None,
                "combined_10d_eok": _safe_float(row.get("combined_10d_eok")) if row.get("combined_10d_eok") is not None else None,
                "foreign_streak": _safe_int(row.get("foreign_streak")) if row.get("foreign_streak") is not None else None,
                "inst_streak": _safe_int(row.get("inst_streak")) if row.get("inst_streak") is not None else None,
                "flow_score": _safe_float(row.get("flow_score")) if row.get("flow_score") is not None else None,
                "flow_source": _clean_text(row.get("flow_source")),
                "flow_fallback_used": bool(row.get("flow_fallback_used")),
                "flow_source_confidence": _safe_float(row.get("flow_source_confidence")) if row.get("flow_source_confidence") is not None else None,
                "flow_coverage_ratio": _safe_float(row.get("flow_coverage_ratio")) if row.get("flow_coverage_ratio") is not None else None,
                "captured_at": _clean_text(row.get("captured_at")),
                "updated_at": _clean_text(row.get("updated_at")) or datetime.now().isoformat(timespec="seconds"),
            }
        )
    return docs


def _build_stock_context_docs(
    fair_docs: dict[str, dict[str, Any]],
    cards_summary: dict[str, Any],
    market_briefing: dict[str, Any],
    event_symbol_df: pd.DataFrame,
    event_detail_df: pd.DataFrame,
    analyst_summary_map: dict[str, dict[str, Any]],
    analyst_detail_map: dict[str, list[dict[str, Any]]],
    news_summary_map: dict[str, dict[str, Any]],
    quote_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    card_map = {_norm_symbol(item.get("symbol")): item for item in cards_summary.get("cards", []) or []}
    briefing_map = {_norm_symbol(item.get("symbol")): item for item in market_briefing.get("candidates", []) or []}
    event_map = {}
    if not event_symbol_df.empty:
        event_map = {
            _norm_symbol(row.get("symbol")): {
                "latest_event_type": _clean_text(row.get("latest_event_type")),
                "latest_title": _clean_text(row.get("latest_title")),
                "latest_signal_bias": _clean_text(row.get("latest_signal_bias")),
                "event_signal_score": _safe_float(row.get("event_signal_score")),
            }
            for _, row in event_symbol_df.iterrows()
        }
    revision_event_map: dict[str, dict[str, Any]] = {}
    if event_detail_df is not None and not event_detail_df.empty:
        revision_df = event_detail_df.copy()
        revision_df["stock_code"] = revision_df["stock_code"].astype(str).str.zfill(6)
        revision_df["event_date_ts"] = pd.to_datetime(revision_df.get("event_date"), errors="coerce")
        revision_df = revision_df.sort_values(["event_date_ts", "event_time_hhmm"], ascending=[False, False])
        revision_df = revision_df[
            revision_df["event_type"].astype(str).isin(REVISION_EVENT_TYPES | {"CORRECTION"})
        ]
        for _, row in revision_df.iterrows():
            symbol = _norm_symbol(row.get("stock_code"))
            if not symbol or symbol in revision_event_map:
                continue
            applied, summary = _event_revision_state(dict(row))
            revision_event_map[symbol] = {
                "applied": applied,
                "summary": summary,
                "event_type": _clean_text(row.get("event_type")),
                "title": _clean_text(row.get("title")),
            }
    symbols = sorted(set(fair_docs) | set(card_map) | set(briefing_map) | set(event_map) | set(analyst_summary_map) | set(analyst_detail_map) | set(revision_event_map))
    for symbol in symbols:
        fair = fair_docs.get(symbol) or {}
        card = card_map.get(symbol) or {}
        brief = briefing_map.get(symbol) or {}
        event = event_map.get(symbol) or {}
        revision_event = revision_event_map.get(symbol) or {}
        analyst = analyst_summary_map.get(symbol) or {}
        analyst_items = analyst_detail_map.get(symbol) or []
        quote = quote_map.get(symbol) or {}
        news = news_summary_map.get(symbol) or {}
        sector = _clean_text(fair.get("sector") or card.get("sector") or brief.get("sector"))
        revision_op_pct = _safe_float(fair.get("valuation_revision_op_pct"))
        disclosure_revision_applied, disclosure_revision_summary = _build_disclosure_revision_summary(
            revision_event,
            fair,
            revision_op_pct,
        )
        analyst_summary_paragraph = _build_analyst_summary_paragraph(analyst, analyst_items, fair)
        docs.append(
            {
                "_id": symbol,
                "symbol": symbol,
                "name": _clean_text(fair.get("name") or card.get("name") or brief.get("name")),
                "sector": sector,
                "current_price": _visible_quote_price(quote) if quote else (
                    fair.get("current_price") if _clean_text(fair.get("current_price_status")) != "업데이트 지연" else None
                ),
                "quote_change_rate_pct": quote.get("change_rate_pct"),
                "quote_source": _clean_text(quote.get("source")),
                "quote_freshness": _clean_text(quote.get("freshness")),
                "price_source": _clean_text(quote.get("price_source") or fair.get("current_price_source")),
                "price_captured_at": _clean_text(quote.get("price_captured_at") or fair.get("current_price_captured_at")),
                "price_freshness": _clean_text(quote.get("price_freshness") or fair.get("current_price_freshness")),
                "price_status": _clean_text(quote.get("price_status") or fair.get("current_price_status")) or "자료 없음",
                "official_close": quote.get("official_close") if quote.get("official_close") is not None else fair.get("official_close"),
                "official_close_date": _clean_text(quote.get("official_close_date") or fair.get("official_close_date")),
                "fair_value_base": fair.get("fair_value_base"),
                "fair_value_gap_pct": fair.get("fair_value_gap_pct"),
                "fair_value_confidence_score": fair.get("fair_value_confidence_score"),
                "valuation_basis_label": _clean_text(fair.get("valuation_basis_label")),
                "valuation_basis_period": _clean_text(fair.get("valuation_basis_period")),
                "valuation_input_source": _clean_text(fair.get("valuation_input_source")),
                "valuation_multiple_current": fair.get("valuation_multiple_current"),
                "valuation_multiple_target": fair.get("valuation_multiple_target"),
                "valuation_multiple_unit": _clean_text(fair.get("valuation_multiple_unit")) or "배",
                "operating_profit_yield_pct": fair.get("operating_profit_yield_pct") if _safe_float(fair.get("operating_profit_yield_pct"), float("nan")) > 0 else None,
                "operating_margin_pct": fair.get("operating_margin_pct") if _safe_float(fair.get("operating_margin_pct"), float("nan")) > 0 else None,
                "roe_current": fair.get("roe_current") if _safe_float(fair.get("roe_current"), float("nan")) > 0 else None,
                "profitability_metric_label": _clean_text(fair.get("profitability_metric_label")),
                "profitability_metric_value": fair.get("profitability_metric_value") if _safe_float(fair.get("profitability_metric_value"), float("nan")) > 0 else None,
                "valuation_summary_paragraph": _clean_text(fair.get("valuation_summary_paragraph")),
                "valuation_method_detail": _clean_text(fair.get("valuation_method_detail")),
                "valuation_formula_hint": _clean_text(fair.get("valuation_formula_hint")),
                "profitability_formula_hint": _clean_text(fair.get("profitability_formula_hint")),
                "tp_formula_label": _clean_text(fair.get("tp_formula_label")),
                "tp_basis_metric_label": _clean_text(fair.get("tp_basis_metric_label")),
                "tp_basis_metric_value": fair.get("tp_basis_metric_value") if _safe_float(fair.get("tp_basis_metric_value"), float("nan")) > 0 else None,
                "tp_peer_median_multiple": fair.get("tp_peer_median_multiple") if _safe_float(fair.get("tp_peer_median_multiple"), float("nan")) > 0 else None,
                "tp_peer_q25_multiple": fair.get("tp_peer_q25_multiple") if _safe_float(fair.get("tp_peer_q25_multiple"), float("nan")) > 0 else None,
                "tp_peer_q75_multiple": fair.get("tp_peer_q75_multiple") if _safe_float(fair.get("tp_peer_q75_multiple"), float("nan")) > 0 else None,
                "tp_peer_count_used": _safe_int(fair.get("tp_peer_count_used")),
                "tp_sanity_low_price": fair.get("tp_sanity_low_price") if _safe_float(fair.get("tp_sanity_low_price"), float("nan")) > 0 else None,
                "tp_sanity_high_price": fair.get("tp_sanity_high_price") if _safe_float(fair.get("tp_sanity_high_price"), float("nan")) > 0 else None,
                "tp_sanity_bound_applied": bool(fair.get("tp_sanity_bound_applied")),
                "tp_revision_contributors": _clean_text(fair.get("tp_revision_contributors")),
                "tp_explanation_steps": _clean_text_list(fair.get("tp_explanation_steps")),
                "tp_basis_summary": _clean_text(fair.get("tp_basis_summary")),
                "tp_peer_set_summary": _clean_text(fair.get("tp_peer_set_summary")),
                "tp_bound_summary": _clean_text(fair.get("tp_bound_summary")),
                "tp_hidden_reason_detail": _clean_text(fair.get("tp_hidden_reason_detail")),
                "valuation_tier": _clean_text(fair.get("valuation_tier")),
                "valuation_proxy_used": bool(fair.get("valuation_proxy_used")),
                "valuation_coverage_reason": _clean_text(fair.get("valuation_coverage_reason")),
                "valuation_driver": _clean_text(fair.get("valuation_driver")),
                "valuation_reason_summary": _clean_text(fair.get("valuation_reason_summary")),
                "valuation_primary_method": _clean_text(fair.get("valuation_primary_method")),
                "valuation_anchor_mix": _clean_text(fair.get("valuation_anchor_mix")),
                "valuation_peer_group": _clean_text(fair.get("valuation_peer_group")),
                "valuation_revision_op_pct": revision_op_pct if math.isfinite(revision_op_pct) else None,
                "sector_source": _clean_text(fair.get("sector_source")),
                "wics_status_label": _clean_text(card.get("universe_status_label") or brief.get("universe_status_label")),
                "wics_status_reason": _clean_text(card.get("universe_status_reason") or brief.get("universe_status_reason")),
                "wics_dynamic_stability": _safe_float(card.get("wics_dynamic_stability") or brief.get("wics_dynamic_stability")),
                "sector_final_label": _clean_text(card.get("sector_final_label") or brief.get("sector_final_label")),
                "sector_human_summary": _clean_text(card.get("sector_human_summary") or brief.get("sector_human_summary")),
                "latest_event_type": _clean_text(event.get("latest_event_type")),
                "latest_event_title": _clean_text(event.get("latest_title")),
                "latest_event_signal_bias": _clean_text(event.get("latest_signal_bias")),
                "event_signal_score": _safe_float(event.get("event_signal_score")),
                "disclosure_revision_applied": disclosure_revision_applied,
                "disclosure_revision_summary": disclosure_revision_summary,
                "disclosure_revision_op_pct": round(revision_op_pct, 2) if math.isfinite(revision_op_pct) else None,
                "analyst_report_count": _safe_int(analyst.get("report_count") or fair.get("analyst_report_count")),
                "analyst_broker_diversity": _safe_int(analyst.get("broker_diversity") or fair.get("analyst_broker_diversity")),
                "analyst_target_upside_pct": _safe_float(analyst.get("target_upside_pct") or fair.get("analyst_target_upside_pct")),
                "analyst_latest_title": _clean_text(analyst.get("latest_title") or fair.get("analyst_latest_title")),
                "analyst_summary_paragraph": analyst_summary_paragraph,
                "analyst_recent_titles": analyst_items,
                "latest_news_title": _clean_text(news.get("latest_title")),
                "latest_news_tone": _clean_text(news.get("tone")),
                "news_count": _safe_int(news.get("news_count")),
                "news_conviction_score": _safe_float(news.get("conviction_score")),
                "briefing_notes": brief.get("notes") or [],
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
    return docs


def _build_event_calendar_docs(
    detail_df: pd.DataFrame,
    sector_map: dict[str, str],
    listing_df: pd.DataFrame,
    quote_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    if detail_df.empty:
        return docs
    listing_map = {
        _norm_symbol(row.get("Code")): row.to_dict()
        for _, row in listing_df.iterrows()
        if _norm_symbol(row.get("Code"))
    } if not listing_df.empty else {}
    detail_df = detail_df.sort_values(["event_date", "event_time_hhmm"], ascending=[False, False])

    raw_docs: list[dict[str, Any]] = []
    for _, row in detail_df.iterrows():
        raw_docs.append(_build_event_doc(dict(row), sector_map, listing_map, quote_map))

    if not raw_docs:
        return docs

    receipt_best: dict[str, dict[str, Any]] = {}
    for doc in raw_docs:
        receipt_key = _clean_text(doc.get("_receipt_key"))
        if not receipt_key:
            continue
        current = receipt_best.get(receipt_key)
        if current is None or _event_sort_key(doc) > _event_sort_key(current):
            receipt_best[receipt_key] = doc
    deduped_docs = list(receipt_best.values())

    grouped_by_display: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for doc in deduped_docs:
        grouped_by_display[_event_display_key(doc)].append(doc)

    public_docs: list[dict[str, Any]] = []
    for group_docs in grouped_by_display.values():
        meaningful = [doc for doc in group_docs if _should_include_public_event(doc)]
        corrections = [doc for doc in group_docs if bool(doc.get("_is_correction_record"))]
        base_docs = [doc for doc in meaningful if not bool(doc.get("_is_correction_record"))]

        selected: dict[str, Any] | None = None
        if base_docs:
            selected = max(base_docs, key=_event_sort_key)
            selected = _apply_correction_metadata(dict(selected), corrections)
        elif meaningful:
            selected = max(meaningful, key=_event_sort_key)
            remaining_corrections = [doc for doc in corrections if doc is not selected]
            if bool(selected.get("_is_correction_record")):
                remaining_corrections = [selected, *remaining_corrections]
            selected = _apply_correction_metadata(dict(selected), remaining_corrections)

        if selected is None:
            continue

        selected.pop("_receipt_key", None)
        selected.pop("_display_key", None)
        selected.pop("_is_correction_record", None)
        public_docs.append(selected)

    public_docs.sort(
        key=lambda item: (
            _clean_text(item.get("event_date")),
            _clean_text(item.get("event_time_hhmm")),
            _clean_text(item.get("updated_at")),
        ),
        reverse=True,
    )
    return public_docs


def _build_macro_doc(macro: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": _clean_text(macro.get("generated_at")),
        "headline_count": _safe_int(macro.get("headline_count")),
        "risk_score": _safe_float(macro.get("risk_score")),
        "sector_scores": macro.get("sector_scores") or {},
        "headlines": macro.get("headlines") or [],
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _build_dashboard_doc(
    sector_docs: list[dict[str, Any]],
    fair_docs: list[dict[str, Any]],
    event_docs: list[dict[str, Any]],
    macro_doc: dict[str, Any],
    market_briefing: dict[str, Any],
) -> dict[str, Any]:
    visible_fair_docs = [
        item
        for item in fair_docs
        if bool(item.get("valuation_tp_visible")) and item.get("fair_value_gap_pct") is not None
    ]
    hidden_fair_docs = [
        item
        for item in fair_docs
        if not bool(item.get("valuation_tp_visible"))
        and _safe_float(item.get("valuation_multiple_current"), float("nan")) > 0
    ]
    fair_sorted = sorted(visible_fair_docs, key=lambda item: _safe_float(item.get("fair_value_gap_pct")), reverse=True)
    premium_sorted = sorted(visible_fair_docs, key=lambda item: _safe_float(item.get("fair_value_gap_pct")))
    hidden_sorted = sorted(
        hidden_fair_docs,
        key=lambda item: (
            _safe_float(item.get("marcap")),
            _safe_float(item.get("valuation_multiple_current")),
        ),
        reverse=True,
    )
    basis_counts: dict[str, int] = defaultdict(int)
    price_status_counts: dict[str, int] = defaultdict(int)
    hidden_reason_counts: dict[str, int] = defaultdict(int)
    for item in fair_docs:
        basis = _clean_text(item.get("valuation_basis_period")) or "미표시"
        basis_counts[basis] += 1
        price_status = _clean_text(item.get("current_price_status")) or "자료 없음"
        price_status_counts[price_status] += 1
        if not bool(item.get("valuation_tp_visible")):
            hidden_reason = _clean_text(item.get("valuation_tp_hidden_reason")) or "기타"
            hidden_reason_counts[hidden_reason] += 1
    capital_action_highlights = [item for item in event_docs if _clean_text(item.get("event_group")) == "capital_actions"]
    operating_update_highlights = [item for item in event_docs if _clean_text(item.get("event_group")) == "operating_updates"]
    other_disclosure_highlights = [item for item in event_docs if _clean_text(item.get("event_group")) == "other_disclosures"]
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "market_brief": market_briefing.get("trade_plan") or market_briefing.get("action_hints") or [],
        "sector_board": sector_docs[:12],
        "discount_leaders": fair_sorted[:10],
        "premium_leaders": premium_sorted[:10],
        "tp_hidden_watch": hidden_sorted[:10],
        "visible_tp_count": len(visible_fair_docs),
        "basis_counts": dict(sorted(basis_counts.items(), key=lambda item: item[1], reverse=True)),
        "price_status_counts": dict(sorted(price_status_counts.items(), key=lambda item: item[1], reverse=True)),
        "hidden_reason_counts": dict(sorted(hidden_reason_counts.items(), key=lambda item: item[1], reverse=True)),
        "watch_names": market_briefing.get("candidates", [])[:10],
        "event_highlights": event_docs[:12],
        "capital_action_highlights": capital_action_highlights[:12],
        "operating_update_highlights": operating_update_highlights[:12],
        "other_disclosure_highlights": other_disclosure_highlights[:12],
        "routine_filing_highlights": [],
        "macro_summary": macro_doc,
    }


def _save_projection_file(name: str, payload: Any) -> str:
    ensure_runtime_dir()
    os.makedirs(WEB_DIR, exist_ok=True)
    path = WEB_DIR / f"{name}.json"
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    return str(path)


def build_projection_payloads() -> dict[str, Any]:
    listing_df = _load_listing()
    fair_df = _load_fair_value_frame()
    fair_value_docs, sector_map = _build_fair_value_docs(fair_df)
    cards_summary = _load_cards_summary()
    market_briefing = _load_market_briefing()
    sector_thesis = _load_sector_thesis()
    macro_doc = _build_macro_doc(_load_macro_regime())
    wics_meta = _load_wics_meta()
    quote_rows = _load_quote_rows()
    flow_source_rows = _load_flow_source_rows()
    quote_map = {_norm_symbol(row.get("symbol")): row for row in quote_rows}
    news_summary_map, news_detail_map = _load_news_maps()
    analyst_summary_map, analyst_detail_map = _load_analyst_maps()
    event_symbol_df, event_detail_df = _load_event_frames()
    enriched_sector_map, sector_source_map = _build_sector_projection_maps(fair_df, cards_summary, event_detail_df)

    stock_master_docs = _build_stock_master_docs(listing_df, enriched_sector_map, sector_source_map)
    fair_doc_map = {doc["_id"]: doc for doc in fair_value_docs}
    stock_flow_docs = _build_stock_flow_docs(flow_source_rows, enriched_sector_map)
    stock_financial_profile_docs = build_stock_financial_profile_docs(fair_df)
    sector_docs = _build_sector_dashboard_docs(listing_df, fair_df, sector_thesis, wics_meta, quote_map, stock_flow_docs)
    context_docs = _build_stock_context_docs(
        fair_doc_map,
        cards_summary,
        market_briefing,
        event_symbol_df,
        event_detail_df,
        analyst_summary_map,
        analyst_detail_map,
        news_summary_map,
        quote_map,
    )
    event_docs = _build_event_calendar_docs(event_detail_df, enriched_sector_map, listing_df, quote_map)
    dashboard_doc = _build_dashboard_doc(sector_docs, fair_value_docs, event_docs, macro_doc, market_briefing)

    news_docs = []
    for symbol, items in news_detail_map.items():
        news_docs.append({"_id": symbol, "symbol": symbol, "items": items[:20], "updated_at": datetime.now().isoformat(timespec="seconds")})
    news_docs.sort(key=lambda item: item["_id"])

    payloads = {
        "stock_master": stock_master_docs,
        "quote_delayed_latest": quote_rows,
        "stock_fair_value_latest": fair_value_docs,
        "stock_context_latest": context_docs,
        "stock_financial_profile_latest": stock_financial_profile_docs,
        "stock_flow_latest": stock_flow_docs,
        "sector_dashboard_latest": sector_docs,
        "event_calendar_latest": event_docs,
        "macro_regime_latest": macro_doc,
        "news_latest": news_docs,
        "dashboard_latest": dashboard_doc,
    }
    return payloads


def publish_projection_payloads(payloads: dict[str, Any], *, skip_mongo: bool = False) -> dict[str, Any]:
    store = MongoReadModelStore(SETTINGS.MONGO_URI, SETTINGS.DB_NAME)
    results: list[dict[str, Any]] = []
    file_paths: dict[str, str] = {}
    for name, payload in payloads.items():
        file_paths[name] = _save_projection_file(name, payload)
        if skip_mongo:
            continue
        if isinstance(payload, list):
            results.append(store.replace_collection(name, payload, key_fields=["_id"]))
        elif isinstance(payload, dict):
            results.append(store.replace_singleton(name, payload, singleton_id="latest"))
    publish_run = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "collections": results,
        "files": file_paths,
    }
    file_paths["publish_runs"] = _save_projection_file("publish_runs_latest", publish_run)
    if not skip_mongo:
        store.replace_singleton("publish_runs", publish_run, singleton_id="latest")
    return {"files": file_paths, "collections": results, "mongo_available": store.available}


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def run_once(args: argparse.Namespace) -> None:
    payloads = build_projection_payloads()
    result = publish_projection_payloads(payloads, skip_mongo=args.skip_mongo)
    if args.print_only:
        print("[웹 투영] 발행 완료")
        print(f"- Mongo 사용 가능: {result['mongo_available']}")
        for name, path in sorted(result["files"].items()):
            print(f"- {name}: {path}")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
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
