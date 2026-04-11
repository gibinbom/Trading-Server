from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from investor_flow_provider import fetch_recent_investor_days
    from kis_broker_factory import build_kis_broker_from_settings
    from runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from signals.wics_universe import load_effective_wics_symbol_map, normalize_sector_name
    from valuation_refresh_support import (
        load_eligible_listing_df,
        load_high_turnover_symbols,
        load_incremental_consensus_symbols,
        load_tp_visible_symbols,
    )
except Exception:
    from Disclosure.investor_flow_provider import fetch_recent_investor_days
    from Disclosure.kis_broker_factory import build_kis_broker_from_settings
    from Disclosure.runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from Disclosure.signals.wics_universe import load_effective_wics_symbol_map, normalize_sector_name
    from Disclosure.valuation_refresh_support import (
        load_eligible_listing_df,
        load_high_turnover_symbols,
        load_incremental_consensus_symbols,
        load_tp_visible_symbols,
    )


log = logging.getLogger("disclosure.flow_snapshot_builder")
ROOT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT_DIR = ROOT_DIR.parent
WEB_PROJECTION_DIR = Path(RUNTIME_DIR) / "web_projections"
FLOW_SOURCE_JSON_PATH = WEB_PROJECTION_DIR / "stock_flow_latest_source.json"
FLOW_SOURCE_CSV_PATH = WEB_PROJECTION_DIR / "stock_flow_latest_source.csv"
STOCK_MASTER_PROJECTION_PATH = WEB_PROJECTION_DIR / "stock_master.json"
FAIR_VALUE_SNAPSHOT_PATH = ROOT_DIR / "valuation" / "fair_value_snapshot_latest.csv"
SECTOR_DASHBOARD_PATH = WEB_PROJECTION_DIR / "sector_dashboard_latest.json"

_THREAD_LOCAL = threading.local()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build stock-level investor-flow snapshots for the public web.")
    parser.add_argument("--mode", choices=("full", "incremental"), default="incremental", help="Build the full eligible universe or refresh an incremental subset.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print a short summary after saving.")
    parser.add_argument("--limit", type=int, default=0, help="Optional symbol cap for quick checks.")
    parser.add_argument("--workers", type=int, default=6, help="Concurrent flow fetch workers.")
    parser.add_argument("--quote-top-n", type=int, default=800, help="Incremental mode delayed-quote symbol cap.")
    parser.add_argument("--tp-top-n", type=int, default=500, help="Incremental mode TP-visible symbol cap.")
    parser.add_argument("--analyst-days", type=int, default=21, help="Incremental mode analyst lookback.")
    parser.add_argument("--event-days", type=int, default=45, help="Incremental mode event lookback.")
    parser.add_argument("--sector-watch-top-n", type=int, default=8, help="Incremental mode sector watch sector cap.")
    parser.add_argument("--sector-watch-per-sector", type=int, default=8, help="Incremental mode watched symbol cap per sector.")
    parser.add_argument("--disable-kis", action="store_true", help="Use public fallback sources only.")
    parser.add_argument("--times", default="11:35,15:20", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def _norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "None"):
            return default
        number = float(value)
        if pd.isna(number):
            return default
        return number
    except Exception:
        return default


def _pct_rank(series: pd.Series, *, higher_is_better: bool = True) -> pd.Series:
    valid = pd.to_numeric(series, errors="coerce")
    if higher_is_better:
        return valid.rank(pct=True, ascending=True)
    return valid.rank(pct=True, ascending=False)


def _load_sector_watch_symbols(*, top_n: int = 8, per_sector: int = 8) -> list[str]:
    if not SECTOR_DASHBOARD_PATH.exists():
        return []
    try:
        payload = json.loads(SECTOR_DASHBOARD_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    symbols: list[str] = []
    for sector_doc in payload[: max(0, int(top_n))]:
        for row in (sector_doc or {}).get("watch_symbols") or []:
            symbol = _norm_symbol((row or {}).get("symbol"))
            if symbol and symbol not in symbols:
                symbols.append(symbol)
            if len(symbols) >= max(0, int(top_n)) * max(0, int(per_sector)):
                return symbols
        per_sector_symbols = [
            _norm_symbol((row or {}).get("symbol"))
            for row in ((sector_doc or {}).get("sector_flow_leaders") or [])[: max(0, int(per_sector))]
        ]
        for symbol in per_sector_symbols:
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    return symbols


def _load_existing_rows() -> list[dict[str, Any]]:
    if not FLOW_SOURCE_JSON_PATH.exists():
        return []
    try:
        payload = json.loads(FLOW_SOURCE_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return rows if isinstance(rows, list) else []


def _load_sector_map() -> dict[str, str]:
    out: dict[str, str] = {}
    wics_map = load_effective_wics_symbol_map()
    for symbol, sector in wics_map.items():
        normalized = normalize_sector_name(sector)
        if symbol and normalized and symbol not in out:
            out[symbol] = normalized

    if STOCK_MASTER_PROJECTION_PATH.exists():
        try:
            stock_master = json.loads(STOCK_MASTER_PROJECTION_PATH.read_text(encoding="utf-8"))
        except Exception:
            stock_master = []
        if isinstance(stock_master, list):
            for row in stock_master:
                symbol = _norm_symbol((row or {}).get("symbol"))
                sector = normalize_sector_name((row or {}).get("sector"))
                if symbol and sector and symbol not in out:
                    out[symbol] = sector

    if FAIR_VALUE_SNAPSHOT_PATH.exists():
        try:
            fair_df = pd.read_csv(FAIR_VALUE_SNAPSHOT_PATH, dtype={"symbol": str})
        except Exception:
            fair_df = pd.DataFrame()
        if not fair_df.empty:
            for _, row in fair_df.iterrows():
                symbol = _norm_symbol(row.get("symbol"))
                sector = normalize_sector_name(row.get("sector"))
                if symbol and sector and symbol not in out:
                    out[symbol] = sector
    return out


def _load_target_rows(args: argparse.Namespace) -> pd.DataFrame:
    listing_df = load_eligible_listing_df()
    if listing_df.empty:
        return pd.DataFrame()
    listing_df = listing_df.copy()
    listing_df["symbol"] = listing_df["symbol"].astype(str).str.zfill(6)
    sector_map = _load_sector_map()
    listing_df["sector"] = listing_df["symbol"].map(lambda value: normalize_sector_name(sector_map.get(value)))
    if args.mode == "full":
        target_df = listing_df
    else:
        symbols: list[str] = []

        def append_symbol(value: Any) -> None:
            symbol = _norm_symbol(value)
            if symbol and symbol not in symbols:
                symbols.append(symbol)

        for symbol in load_incremental_consensus_symbols(
            analyst_days=args.analyst_days,
            event_days=args.event_days,
            quote_limit=args.quote_top_n,
            tp_limit=args.tp_top_n,
        ):
            append_symbol(symbol)
        for symbol in load_high_turnover_symbols(limit=args.quote_top_n):
            append_symbol(symbol)
        for symbol in load_tp_visible_symbols(limit=args.tp_top_n):
            append_symbol(symbol)
        for symbol in _load_sector_watch_symbols(top_n=args.sector_watch_top_n, per_sector=args.sector_watch_per_sector):
            append_symbol(symbol)
        target_df = listing_df[listing_df["symbol"].isin(symbols)].copy()
    if args.limit and args.limit > 0:
        target_df = target_df.head(int(args.limit)).copy()
    return target_df


def _get_thread_broker(disable_kis: bool):
    if disable_kis:
        return None
    marker = getattr(_THREAD_LOCAL, "broker_marker", None)
    if marker is None:
        try:
            _THREAD_LOCAL.broker = build_kis_broker_from_settings(is_virtual=False, dry_run=True)
        except Exception as exc:
            log.warning("flow snapshot broker init failed; public fallback only: %s", exc)
            _THREAD_LOCAL.broker = None
        _THREAD_LOCAL.broker_marker = True
    return getattr(_THREAD_LOCAL, "broker", None)


def _build_row(base_row: dict[str, Any], payload: dict[str, Any], *, captured_at: str) -> dict[str, Any]:
    symbol = _norm_symbol(base_row.get("symbol"))
    days = payload.get("days") or []
    source = _clean_text(payload.get("source")) or "missing"
    day_count = len(days)
    foreign_1d = foreign_3d = foreign_5d = foreign_10d = None
    inst_1d = inst_3d = inst_5d = inst_10d = None
    retail_1d = retail_3d = retail_5d = retail_10d = None
    foreign_streak = inst_streak = None
    combined_1d = combined_3d = combined_5d = combined_10d = None
    if day_count:
        foreign_1d = int(days[0].get("foreign_eok", 0))
        inst_1d = int(days[0].get("inst_eok", 0))
        retail_1d = int(days[0].get("retail_eok", 0))
        foreign_3d = sum(int(day.get("foreign_eok", 0)) for day in days[:3])
        inst_3d = sum(int(day.get("inst_eok", 0)) for day in days[:3])
        retail_3d = sum(int(day.get("retail_eok", 0)) for day in days[:3])
        foreign_5d = sum(int(day.get("foreign_eok", 0)) for day in days[:5])
        inst_5d = sum(int(day.get("inst_eok", 0)) for day in days[:5])
        retail_5d = sum(int(day.get("retail_eok", 0)) for day in days[:5])
        foreign_10d = sum(int(day.get("foreign_eok", 0)) for day in days[:10])
        inst_10d = sum(int(day.get("inst_eok", 0)) for day in days[:10])
        retail_10d = sum(int(day.get("retail_eok", 0)) for day in days[:10])
        foreign_streak = 0
        inst_streak = 0
        for idx, day in enumerate(days[:5]):
            f_amt = int(day.get("foreign_eok", 0))
            i_amt = int(day.get("inst_eok", 0))
            if f_amt > 0 and foreign_streak == idx:
                foreign_streak += 1
            if i_amt > 0 and inst_streak == idx:
                inst_streak += 1
        combined_1d = foreign_1d + inst_1d
        combined_3d = foreign_3d + inst_3d
        combined_5d = foreign_5d + inst_5d
        combined_10d = foreign_10d + inst_10d
    row = {
        "_id": symbol,
        "symbol": symbol,
        "name": _clean_text(base_row.get("name")),
        "market": _clean_text(base_row.get("market")),
        "sector": normalize_sector_name(base_row.get("sector")) or "미분류",
        "marcap": _safe_float(base_row.get("marcap")),
        "foreign_1d_eok": foreign_1d,
        "inst_1d_eok": inst_1d,
        "retail_1d_eok": retail_1d,
        "foreign_3d_eok": foreign_3d,
        "inst_3d_eok": inst_3d,
        "retail_3d_eok": retail_3d,
        "foreign_5d_eok": foreign_5d,
        "inst_5d_eok": inst_5d,
        "retail_5d_eok": retail_5d,
        "foreign_10d_eok": foreign_10d,
        "inst_10d_eok": inst_10d,
        "retail_10d_eok": retail_10d,
        "combined_1d_eok": combined_1d,
        "combined_3d_eok": combined_3d,
        "combined_5d_eok": combined_5d,
        "combined_10d_eok": combined_10d,
        "foreign_streak": foreign_streak,
        "inst_streak": inst_streak,
        "flow_score": None,
        "flow_source": source,
        "flow_fallback_used": bool(payload.get("fallback_used")),
        "flow_source_confidence": _safe_float(payload.get("source_confidence"), 0.0) or 0.0,
        "flow_coverage_ratio": _safe_float(payload.get("coverage_ratio"), 0.0) or 0.0,
        "flow_confidence_score": _safe_float(payload.get("confidence_score"), 0.0) or 0.0,
        "captured_at": captured_at,
        "updated_at": captured_at,
    }
    return row


def _derive_scores(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    df = pd.DataFrame(rows).copy()
    if df.empty:
        return rows
    for col in (
        "foreign_1d_eok",
        "inst_1d_eok",
        "retail_1d_eok",
        "foreign_3d_eok",
        "inst_3d_eok",
        "retail_3d_eok",
        "foreign_5d_eok",
        "inst_5d_eok",
        "retail_5d_eok",
        "foreign_10d_eok",
        "inst_10d_eok",
        "retail_10d_eok",
        "combined_1d_eok",
        "combined_3d_eok",
        "combined_5d_eok",
        "combined_10d_eok",
        "foreign_streak",
        "inst_streak",
        "flow_source_confidence",
        "flow_coverage_ratio",
        "flow_confidence_score",
        "marcap",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    marcap = df["marcap"].where(df["marcap"] > 0)
    df["smart_money_3d_to_mcap"] = df["combined_3d_eok"] * 100_000_000 / marcap
    df["smart_money_5d_to_mcap"] = df["combined_5d_eok"] * 100_000_000 / marcap
    df["flow_3d_score"] = _pct_rank(df["smart_money_3d_to_mcap"], higher_is_better=True)
    df["flow_5d_score"] = _pct_rank(df["smart_money_5d_to_mcap"], higher_is_better=True)
    df["flow_foreign_streak_score"] = _pct_rank(df["foreign_streak"], higher_is_better=True)
    df["flow_inst_streak_score"] = _pct_rank(df["inst_streak"], higher_is_better=True)
    score_cols = ["flow_3d_score", "flow_5d_score", "flow_foreign_streak_score", "flow_inst_streak_score"]
    df["flow_score_raw"] = df[score_cols].fillna(0.5).mean(axis=1)
    flow_conf = df["flow_confidence_score"].fillna(0.0).clip(lower=0.0, upper=1.0)
    df["flow_score"] = 0.5 + (df["flow_score_raw"].fillna(0.5) - 0.5) * flow_conf
    df["flow_score"] = (df["flow_score"].fillna(0.5) * 100).round(1)
    ordered_cols = [col for col in df.columns if col not in {"smart_money_3d_to_mcap", "smart_money_5d_to_mcap", "flow_3d_score", "flow_5d_score", "flow_foreign_streak_score", "flow_inst_streak_score", "flow_score_raw"}]
    cleaned_rows = df[ordered_cols].to_dict(orient="records")
    return cleaned_rows


def _fetch_snapshot_rows(target_df: pd.DataFrame, *, disable_kis: bool, workers: int) -> list[dict[str, Any]]:
    if target_df.empty:
        return []
    rows: list[dict[str, Any]] = []
    captured_at = datetime.now().isoformat(timespec="seconds")
    max_workers = max(1, min(int(workers), 12))

    def _fetch_one(meta: dict[str, Any]) -> dict[str, Any]:
        broker = _get_thread_broker(disable_kis)
        payload = fetch_recent_investor_days(broker, str(meta.get("symbol") or ""), max_days=10)
        return _build_row(meta, payload, captured_at=captured_at)

    metas = target_df[["symbol", "name", "market", "sector", "marcap"]].to_dict(orient="records")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_one, meta) for meta in metas]
        for future in as_completed(futures):
            try:
                rows.append(future.result())
            except Exception as exc:
                log.warning("flow snapshot row build failed: %s", exc)
    return rows


def build_flow_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    target_df = _load_target_rows(args)
    if target_df.empty:
        return {"generated_at": datetime.now().isoformat(timespec="seconds"), "mode": args.mode, "rows": []}

    fetched_rows = _fetch_snapshot_rows(target_df, disable_kis=args.disable_kis, workers=args.workers)
    target_symbols = set(target_df["symbol"].astype(str).str.zfill(6).tolist())
    eligible_listing_df = load_eligible_listing_df()
    eligible_symbols = set(eligible_listing_df["symbol"].astype(str).str.zfill(6).tolist()) if not eligible_listing_df.empty else target_symbols

    if args.mode == "full":
        merged_rows = fetched_rows
    else:
        existing_rows = [row for row in _load_existing_rows() if _norm_symbol((row or {}).get("symbol"))]
        existing_map = {
            _norm_symbol((row or {}).get("symbol")): dict(row)
            for row in existing_rows
            if _norm_symbol((row or {}).get("symbol"))
        }
        for row in fetched_rows:
            symbol = _norm_symbol(row.get("symbol"))
            if not symbol:
                continue
            existing_map[symbol] = row
        merged_rows = [row for symbol, row in existing_map.items() if symbol in eligible_symbols]
        missing_symbols = [symbol for symbol in target_symbols if symbol not in existing_map]
        if missing_symbols:
            target_map = {
                _norm_symbol(row.get("symbol")): row
                for row in target_df[["symbol", "name", "market", "sector", "marcap"]].to_dict(orient="records")
            }
            captured_at = datetime.now().isoformat(timespec="seconds")
            for symbol in missing_symbols:
                meta = target_map.get(symbol) or {"symbol": symbol}
                merged_rows.append(
                    {
                        "_id": symbol,
                        "symbol": symbol,
                        "name": _clean_text(meta.get("name")),
                        "market": _clean_text(meta.get("market")),
                        "sector": normalize_sector_name(meta.get("sector")) or "미분류",
                        "marcap": _safe_float(meta.get("marcap")),
                        "foreign_1d_eok": None,
                        "inst_1d_eok": None,
                        "retail_1d_eok": None,
                        "foreign_3d_eok": None,
                        "inst_3d_eok": None,
                        "retail_3d_eok": None,
                        "foreign_5d_eok": None,
                        "inst_5d_eok": None,
                        "retail_5d_eok": None,
                        "foreign_10d_eok": None,
                        "inst_10d_eok": None,
                        "retail_10d_eok": None,
                        "combined_1d_eok": None,
                        "combined_3d_eok": None,
                        "combined_5d_eok": None,
                        "combined_10d_eok": None,
                        "foreign_streak": None,
                        "inst_streak": None,
                        "flow_score": None,
                        "flow_source": "missing",
                        "flow_fallback_used": False,
                        "flow_source_confidence": 0.0,
                        "flow_coverage_ratio": 0.0,
                        "flow_confidence_score": 0.0,
                        "captured_at": captured_at,
                        "updated_at": captured_at,
                    }
                )

    merged_rows = _derive_scores(sorted(merged_rows, key=lambda item: (str(item.get("sector") or ""), str(item.get("name") or ""))))
    source_counts: dict[str, int] = {}
    for row in merged_rows:
        source = _clean_text(row.get("flow_source")) or "missing"
        source_counts[source] = source_counts.get(source, 0) + 1
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": args.mode,
        "source_counts": source_counts,
        "count": len(merged_rows),
        "rows": merged_rows,
    }


def save_flow_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_runtime_dir()
    os.makedirs(WEB_PROJECTION_DIR, exist_ok=True)
    FLOW_SOURCE_JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(payload.get("rows") or []).to_csv(FLOW_SOURCE_CSV_PATH, index=False, encoding="utf-8-sig")
    return {"json": str(FLOW_SOURCE_JSON_PATH), "csv": str(FLOW_SOURCE_CSV_PATH), "count": len(payload.get("rows") or [])}


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    payload = build_flow_snapshot(args)
    result = save_flow_snapshot(payload)
    if args.print_only:
        source_counts = payload.get("source_counts") or {}
        summary = " · ".join(f"{key} {value}" for key, value in source_counts.items()) or "-"
        print(f"[flow_snapshot:{args.mode}] 저장 {result['count']}종목")
        print(f"- source: {summary}")
        print(f"- json: {result['json']}")
        print(f"- csv: {result['csv']}")
    return result


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("flow snapshot scheduler started (%s): %s", args.mode, ", ".join(schedule_times))
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
