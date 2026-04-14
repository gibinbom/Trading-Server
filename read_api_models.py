from __future__ import annotations

from typing import Any

from read_api_core import (
    READ_MODEL_SOURCE,
    get_mongo_collection,
    get_mongo_singleton,
    has_meaningful_value,
    has_required_keys,
    read_projection_file,
    read_projection_rows_file,
)
from read_api_quotes import merge_quote_rows


def read_collection_model(name: str) -> list[dict[str, Any]]:
    if READ_MODEL_SOURCE == "file":
        return read_projection_file(name, [])
    docs = get_mongo_collection(name)
    if READ_MODEL_SOURCE in {"mongo", "strict-mongo"}:
        return docs or []
    fallback = read_projection_file(name, [])
    if isinstance(docs, list) and docs:
        merged = list(docs)
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
    return read_collection_model("stock_master")


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


def get_index_rebalance() -> dict[str, Any]:
    return read_singleton_model("index_rebalance_latest", ["rows", "as_of", "status"])


def get_etf_gap_monitor() -> dict[str, Any]:
    return read_singleton_model("etf_gap_monitor_latest", ["rows", "as_of", "status"])


def get_market_warning_candidates() -> dict[str, Any]:
    return read_singleton_model("market_warning_candidates_latest", ["rows", "as_of", "status"])


def get_market_warning_official() -> dict[str, Any]:
    return read_singleton_model("market_warning_official_latest", ["rows", "as_of", "status"])
