from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from bson import ObjectId
from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pymongo import MongoClient

ROOT_DIR = Path(__file__).resolve().parent
PROJECTION_DIR = ROOT_DIR / "Disclosure" / "runtime" / "web_projections"
ANALYST_RAW_DIR = ROOT_DIR / "Disclosure" / "analyst_reports" / "raw"
ANALYST_SUMMARY_PATH = ROOT_DIR / "Disclosure" / "analyst_reports" / "summaries" / "analyst_report_summary_latest.json"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "stock_data")
READ_MODEL_SOURCE = str(os.getenv("READ_MODEL_SOURCE", "auto")).strip().lower()
REMOTE_ONLY = READ_MODEL_SOURCE == "strict-mongo"

app = FastAPI(title="Trading Read API", version="0.1.0")
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

_mongo_client: MongoClient | None = None


def sanitize(payload: Any) -> Any:
    return jsonable_encoder(
        payload,
        custom_encoder={
            ObjectId: str,
            Path: str,
            datetime: lambda value: value.isoformat(),
            date: lambda value: value.isoformat(),
        },
    )


def get_mongo_client() -> MongoClient | None:
    global _mongo_client
    if READ_MODEL_SOURCE == "file":
        return None
    if _mongo_client is not None:
        return _mongo_client
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=2500,
            connectTimeoutMS=2500,
        )
        client.admin.command("ping")
        _mongo_client = client
        return client
    except Exception:
        return None


def read_projection_file(name: str, fallback: Any) -> Any:
    target = PROJECTION_DIR / f"{name}.json"
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def read_projection_rows_file(name: str) -> list[dict[str, Any]]:
    payload = read_projection_file(name, [])
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]
    return []


def get_mongo_collection(name: str) -> list[dict[str, Any]] | None:
    client = get_mongo_client()
    if client is None:
        return None
    try:
        docs = list(client[DB_NAME][name].find({}))
        return sanitize(docs)
    except Exception:
        return None


def get_mongo_singleton(name: str) -> dict[str, Any] | None:
    client = get_mongo_client()
    if client is None:
        return None
    try:
        doc = client[DB_NAME][name].find_one({"_id": "latest"})
        return sanitize(doc) if doc else None
    except Exception:
        return None


def has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return len(value) > 0
    return True


def has_required_keys(doc: dict[str, Any] | None, required_keys: list[str]) -> bool:
    if not isinstance(doc, dict):
        return False
    if not required_keys:
        return has_meaningful_value(doc)
    return any(has_meaningful_value(doc.get(key)) for key in required_keys)


def quote_status_priority(status: str) -> int:
    if status == "지연시세":
        return 4
    if status == "업데이트 지연":
        return 3
    if status == "공식종가 fallback":
        return 2
    return 1


def quote_timestamp_value(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0


def merge_quote_rows(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for rows in groups:
        for row in rows:
            symbol = str(row.get("symbol") or row.get("_id") or "").strip()
            if not symbol:
                continue
            current = merged.get(symbol)
            if current is None:
                merged[symbol] = row
                continue
            next_status = str(row.get("price_status") or "")
            current_status = str(current.get("price_status") or "")
            next_priority = quote_status_priority(next_status)
            current_priority = quote_status_priority(current_status)
            next_captured_at = quote_timestamp_value(row.get("price_captured_at") or row.get("captured_at"))
            current_captured_at = quote_timestamp_value(current.get("price_captured_at") or current.get("captured_at"))
            if next_priority > current_priority or (
                next_priority == current_priority and next_captured_at >= current_captured_at
            ):
                merged[symbol] = row
    return list(merged.values())


def read_collection_model(name: str) -> list[dict[str, Any]]:
    if READ_MODEL_SOURCE == "file":
        return read_projection_file(name, [])
    docs = get_mongo_collection(name)
    if READ_MODEL_SOURCE in {"mongo", "strict-mongo"}:
        return docs or []
    fallback = read_projection_file(name, [])
    if isinstance(docs, list) and docs:
        merged: list[dict[str, Any]] = list(docs)
        seen: set[str] = set()
        for doc in docs:
            for key in ("_id", "symbol", "code"):
                value = doc.get(key)
                if has_meaningful_value(value):
                    seen.add(f"{key}:{value}")
                    break
        for doc in fallback if isinstance(fallback, list) else []:
            key = ""
            for field in ("_id", "symbol", "code"):
                value = doc.get(field)
                if has_meaningful_value(value):
                    key = f"{field}:{value}"
                    break
            if not key or key not in seen:
                merged.append(doc)
                if key:
                    seen.add(key)
        return merged
    return fallback if isinstance(fallback, list) else []


def read_singleton_model(name: str, required_keys: list[str]) -> dict[str, Any]:
    if READ_MODEL_SOURCE == "file":
        payload = read_projection_file(name, {})
        return payload if isinstance(payload, dict) else {}
    doc = get_mongo_singleton(name)
    if READ_MODEL_SOURCE in {"mongo", "strict-mongo"}:
        return doc or {}
    if has_required_keys(doc, required_keys):
        return doc or {}
    payload = read_projection_file(name, {})
    return payload if isinstance(payload, dict) else {}


def get_dashboard() -> dict[str, Any]:
    return read_singleton_model("dashboard_latest", ["market_brief", "sector_board"])


def get_stock_master() -> list[dict[str, Any]]:
    payload = read_collection_model("stock_master")
    return payload


def get_delayed_quotes() -> list[dict[str, Any]]:
    if READ_MODEL_SOURCE == "file":
        preferred = read_projection_rows_file("quote_delayed_source_latest")
        hotset = read_projection_rows_file("quote_delayed_hotset_latest")
        legacy = [] if preferred else read_projection_rows_file("quote_delayed_latest")
        return merge_quote_rows(preferred, hotset, legacy)
    mongo_docs = get_mongo_collection("quote_delayed_latest") or []
    if READ_MODEL_SOURCE in {"mongo", "strict-mongo"}:
        return mongo_docs
    preferred = read_projection_rows_file("quote_delayed_source_latest")
    hotset = read_projection_rows_file("quote_delayed_hotset_latest")
    legacy = [] if preferred else read_projection_rows_file("quote_delayed_latest")
    return merge_quote_rows(mongo_docs, preferred, hotset, legacy)


def get_fair_values() -> list[dict[str, Any]]:
    return read_collection_model("stock_fair_value_latest")


def get_stock_contexts() -> list[dict[str, Any]]:
    return read_collection_model("stock_context_latest")


def get_stock_financial_profiles() -> list[dict[str, Any]]:
    return read_collection_model("stock_financial_profile_latest")


def get_stock_flows() -> list[dict[str, Any]]:
    return read_collection_model("stock_flow_latest")


def get_sector_dashboard() -> list[dict[str, Any]]:
    return read_collection_model("sector_dashboard_latest")


def get_sector_rotation_history() -> dict[str, Any]:
    return read_singleton_model("sector_rotation_history_latest", ["weeks", "available_weeks"])


def get_events() -> list[dict[str, Any]]:
    return read_collection_model("event_calendar_latest")


def get_macro() -> dict[str, Any]:
    return read_singleton_model("macro_regime_latest", ["updated_at", "headline_count", "risk_score"])


def get_news() -> list[dict[str, Any]]:
    return read_collection_model("news_latest")


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


def derive_revision_direction(
    current_target_price: int | None,
    previous_target_price: int | None,
    has_previous_report: bool,
) -> str:
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
    now = datetime.now().timestamp()
    cutoff = now - lookback_days * 24 * 60 * 60

    fallback_summary = {"report_count": 0, "broker_count": 0}
    try:
        summary_payload = json.loads(ANALYST_SUMMARY_PATH.read_text(encoding="utf-8"))
        for row in summary_payload.get("top_stocks", []):
            row_symbol = clean_text(row.get("symbol")).zfill(6)
            if row_symbol == normalized:
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
    files = sorted(ANALYST_RAW_DIR.glob("analyst_reports_*.jsonl"))
    for file in files:
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
            revision_direction = derive_revision_direction(
                target_price,
                previous_targets.get(broker_key),
                broker_key in previous_seen,
            )
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
    summary = {
        "average_tp": (sum(priced_targets) / len(priced_targets)) if priced_targets else None,
        "median_tp": median(priced_targets),
        "high_tp": max(priced_targets) if priced_targets else None,
        "low_tp": min(priced_targets) if priced_targets else None,
        "report_count": len(reports) or fallback_summary["report_count"],
        "broker_count": len({clean_text(row.get("broker")) for row in reports if clean_text(row.get("broker"))})
        or fallback_summary["broker_count"],
        "priced_report_count": len(priced_targets),
    }
    return {"reports": reports, "summary": summary}


@app.get("/health")
def health() -> dict[str, Any]:
    client = get_mongo_client()
    return {
        "ok": True,
        "mongo_connected": client is not None,
        "mongo_uri": MONGO_URI,
        "db_name": DB_NAME,
        "projection_dir": str(PROJECTION_DIR),
        "projection_dir_exists": PROJECTION_DIR.exists(),
    }


@app.get("/api/source-status")
def source_status() -> dict[str, Any]:
    client = get_mongo_client()
    return {
        "read_model_source": READ_MODEL_SOURCE,
        "mongo_connected": client is not None,
        "projection_dir": str(PROJECTION_DIR),
        "projection_dir_exists": PROJECTION_DIR.exists(),
        "available_projection_files": sorted(path.name for path in PROJECTION_DIR.glob("*.json")),
    }


@app.get("/api/read-models/dashboard")
def api_dashboard() -> Any:
    return sanitize(get_dashboard())


@app.get("/api/read-models/stock-master")
def api_stock_master() -> Any:
    return sanitize(get_stock_master())


@app.get("/api/read-models/quote-delayed-latest")
def api_quote_delayed_latest() -> Any:
    return sanitize(get_delayed_quotes())


@app.get("/api/read-models/stock-fair-value-latest")
def api_stock_fair_value_latest() -> Any:
    return sanitize(get_fair_values())


@app.get("/api/read-models/stock-context-latest")
def api_stock_context_latest() -> Any:
    return sanitize(get_stock_contexts())


@app.get("/api/read-models/stock-financial-profile-latest")
def api_stock_financial_profile_latest() -> Any:
    return sanitize(get_stock_financial_profiles())


@app.get("/api/read-models/stock-flow-latest")
def api_stock_flow_latest() -> Any:
    return sanitize(get_stock_flows())


@app.get("/api/read-models/sector-dashboard-latest")
def api_sector_dashboard_latest() -> Any:
    return sanitize(get_sector_dashboard())


@app.get("/api/read-models/sector-rotation-history-latest")
def api_sector_rotation_history_latest() -> Any:
    return sanitize(get_sector_rotation_history())


@app.get("/api/read-models/event-calendar-latest")
def api_event_calendar_latest() -> Any:
    return sanitize(get_events())


@app.get("/api/read-models/macro-regime-latest")
def api_macro_regime_latest() -> Any:
    return sanitize(get_macro())


@app.get("/api/read-models/news-latest")
def api_news_latest() -> Any:
    return sanitize(get_news())


@app.get("/api/analyst-board/{symbol}")
def api_analyst_board(symbol: str, lookback_days: int = Query(default=90, ge=1, le=365)) -> Any:
    return sanitize(get_stock_analyst_board(symbol, lookback_days))
