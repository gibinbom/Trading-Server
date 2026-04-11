from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

import pandas as pd

try:
    import pymongo
except Exception:
    pymongo = None

try:
    from config import SETTINGS
    from valuation_refresh_support import load_eligible_listing_df
except Exception:
    from Disclosure.config import SETTINGS
    from Disclosure.valuation_refresh_support import load_eligible_listing_df


log = logging.getLogger("disclosure.actual_financial_refresh")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(ROOT_DIR, "valuation")
LATEST_CSV_PATH = os.path.join(OUTPUT_DIR, "actual_financial_snapshot_latest.csv")
LATEST_JSON_PATH = os.path.join(OUTPUT_DIR, "actual_financial_snapshot_latest.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize latest confirmed annual actual metrics for fair-value fallback.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print refresh summary.")
    parser.add_argument("--times", default="06:45,11:45,15:30,20:10", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _normalize_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        number = float(value)
        return number if pd.notna(number) else None
    except Exception:
        return None


def load_actual_rows() -> list[dict[str, Any]]:
    listing_df = load_eligible_listing_df()
    if listing_df.empty or pymongo is None:
        return []
    symbols = set(listing_df["symbol"].astype(str).str.zfill(6))
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
                "revenue_actual": 1,
                "operating_profit_actual": 1,
                "net_profit_actual": 1,
                "pbr_actual": 1,
                "roe_actual": 1,
                "actual_year": 1,
            },
        ).sort([("date", pymongo.DESCENDING)])
        latest_by_symbol: dict[str, dict[str, Any]] = {}
        for doc in cursor:
            symbol = _normalize_symbol(doc.get("stock_code"))
            if not symbol or symbol not in symbols or symbol in latest_by_symbol:
                continue
            latest_by_symbol[symbol] = doc
        client.close()
    except Exception as exc:
        log.warning("actual financial refresh query failed: %s", exc)
        return []

    rows: list[dict[str, Any]] = []
    factor = 100_000_000.0
    generated_at = datetime.now().isoformat(timespec="seconds")
    for _, listing_row in listing_df.iterrows():
        symbol = str(listing_row.get("symbol") or "").zfill(6)
        doc = latest_by_symbol.get(symbol) or {}
        revenue_actual = _safe_float(doc.get("revenue_actual"))
        op_actual = _safe_float(doc.get("operating_profit_actual"))
        net_actual = _safe_float(doc.get("net_profit_actual"))
        pbr_actual = _safe_float(doc.get("pbr_actual"))
        roe_actual = _safe_float(doc.get("roe_actual"))
        rows.append(
            {
                "_id": symbol,
                "symbol": symbol,
                "name": str(listing_row.get("name") or "").strip(),
                "market": str(listing_row.get("market") or "").strip(),
                "actual_revenue_krw": revenue_actual * factor if revenue_actual and revenue_actual > 0 else None,
                "actual_op_krw": op_actual * factor if op_actual and op_actual > 0 else None,
                "actual_net_krw": net_actual * factor if net_actual and net_actual > 0 else None,
                "actual_pbr": pbr_actual if pbr_actual and pbr_actual > 0 else None,
                "actual_roe": roe_actual if roe_actual and roe_actual > 0 else None,
                "actual_year": int(doc.get("actual_year")) if doc.get("actual_year") else None,
                "source": "wise_report_actual",
                "generated_at": generated_at,
            }
        )
    return rows


def save_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "rows": rows}
    with open(LATEST_JSON_PATH, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    pd.DataFrame(rows).to_csv(LATEST_CSV_PATH, index=False, encoding="utf-8-sig")
    return {"count": len(rows), "json": LATEST_JSON_PATH, "csv": LATEST_CSV_PATH}


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    rows = load_actual_rows()
    result = save_rows(rows)
    if args.print_only:
        print(f"[actual_financial_refresh] saved={result['count']}")
        print(f"- json: {result['json']}")
        print(f"- csv: {result['csv']}")
    return result


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("actual financial refresh scheduler started: %s", ", ".join(schedule_times))
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
