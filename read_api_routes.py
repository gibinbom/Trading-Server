from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from read_api_analyst import get_stock_analyst_board
from read_api_core import (
    DB_NAME,
    MONGO_URI,
    PROJECTION_DIR,
    READ_MODEL_SOURCE,
    get_mongo_client,
    sanitize,
)
from read_api_models import (
    get_dashboard,
    get_delayed_quotes,
    get_etf_gap_monitor,
    get_events,
    get_fair_values,
    get_index_rebalance,
    get_macro,
    get_market_warning_candidates,
    get_market_warning_official,
    get_news,
    get_sector_dashboard,
    get_sector_rotation_history,
    get_stock_contexts,
    get_stock_financial_profiles,
    get_stock_flows,
    get_stock_master,
)
from read_api_quotes import build_quote_health


def create_app() -> FastAPI:
    app = FastAPI(title="Trading Read API", version="0.1.0")
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    _register_routes(app)
    return app


def _register_routes(app: FastAPI) -> None:
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
            **build_quote_health(get_delayed_quotes()),
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
            **build_quote_health(get_delayed_quotes()),
        }

    endpoints = {
        "/api/read-models/dashboard": get_dashboard,
        "/api/read-models/stock-master": get_stock_master,
        "/api/read-models/quote-delayed-latest": get_delayed_quotes,
        "/api/read-models/stock-fair-value-latest": get_fair_values,
        "/api/read-models/stock-context-latest": get_stock_contexts,
        "/api/read-models/stock-financial-profile-latest": get_stock_financial_profiles,
        "/api/read-models/stock-flow-latest": get_stock_flows,
        "/api/read-models/sector-dashboard-latest": get_sector_dashboard,
        "/api/read-models/sector-rotation-history-latest": get_sector_rotation_history,
        "/api/read-models/event-calendar-latest": get_events,
        "/api/read-models/macro-regime-latest": get_macro,
        "/api/read-models/news-latest": get_news,
        "/api/read-models/index-rebalance-latest": get_index_rebalance,
        "/api/read-models/etf-gap-monitor-latest": get_etf_gap_monitor,
        "/api/read-models/market-warning-candidates-latest": get_market_warning_candidates,
        "/api/read-models/market-warning-official-latest": get_market_warning_official,
    }
    for path, getter in endpoints.items():
        app.add_api_route(path, _sanitized_endpoint(getter), methods=["GET"], name=path.rsplit("/", 1)[-1])

    @app.get("/api/analyst-board/{symbol}")
    def api_analyst_board(symbol: str, lookback_days: int = Query(default=90, ge=1, le=365)) -> Any:
        return sanitize(get_stock_analyst_board(symbol, lookback_days))


def _sanitized_endpoint(getter: Any) -> Any:
    def endpoint() -> Any:
        return sanitize(getter())

    endpoint.__name__ = f"endpoint_{getattr(getter, '__name__', 'read_model')}"
    return endpoint
