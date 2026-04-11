from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime

import pandas as pd

try:
    from analyst_report_benchmarks import load_index_history
    from analyst_report_collector import AnalystReportCollector
    from analyst_report_pipeline import load_raw_reports
    from analyst_sector_benchmark import warm_sector_peer_price_cache
except Exception:
    from Disclosure.analyst_report_benchmarks import load_index_history
    from Disclosure.analyst_report_collector import AnalystReportCollector
    from Disclosure.analyst_report_pipeline import load_raw_reports
    from Disclosure.analyst_sector_benchmark import warm_sector_peer_price_cache


log = logging.getLogger("disclosure.analyst_cache_warmer")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_STATE_PATH = os.path.join(ROOT_DIR, "cache", "analyst_cache_warm_state.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Warm analyst sector-peer and index price caches ahead of snapshot generation.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print result locally.")
    parser.add_argument("--times", default="07:55,15:25,19:55", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    parser.add_argument("--days", type=int, default=90, help="Lookback window for raw reports.")
    parser.add_argument("--collect-before-warm", action="store_true", help="Collect Naver/Hankyung reports before warming.")
    parser.add_argument("--naver-pages", type=int, default=2, help="Naver research pages to collect when enabled.")
    parser.add_argument("--hankyung-pages", type=int, default=2, help="Hankyung pages to collect when enabled.")
    parser.add_argument("--hankyung-days", type=int, default=7, help="Hankyung lookback window.")
    parser.add_argument("--backfill-days", type=int, default=0, help="Historical analyst backfill window before warmup.")
    parser.add_argument("--max-pages-per-source", type=int, default=120, help="Maximum pages per source in backfill mode.")
    parser.add_argument("--sources", default="naver,hankyung,fnguide", help="Comma-separated sources: naver,hankyung,fnguide")
    parser.add_argument("--ignore-state", action="store_true", help="Ignore collector seen state during backfill collection.")
    parser.add_argument("--fnguide-symbol-limit", type=int, default=0, help="Optional FnGuide symbol cap. 0 means all candidates.")
    parser.add_argument("--fnguide-symbols", default="", help="Optional comma-separated FnGuide symbol override.")
    parser.add_argument("--warm-limit-per-sector", type=int, default=12, help="How many peer symbols to warm per sector.")
    parser.add_argument("--warm-limit-total", type=int, default=72, help="Total peer symbols to warm per run.")
    parser.add_argument("--lookback-pad-days", type=int, default=20, help="How many days before first report date to warm.")
    parser.add_argument("--forward-pad-days", type=int, default=50, help="How many days after latest report date to warm.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _save_state(payload: dict[str, object]) -> None:
    os.makedirs(os.path.dirname(CACHE_STATE_PATH), exist_ok=True)
    stored = dict(payload)
    stored["generated_at"] = datetime.now().isoformat(timespec="seconds")
    with open(CACHE_STATE_PATH, "w", encoding="utf-8") as fp:
        json.dump(stored, fp, ensure_ascii=False, indent=2)


def run_once(args: argparse.Namespace) -> dict[str, object]:
    collector_result = {}
    if args.collect_before_warm:
        collector_result = AnalystReportCollector(
            naver_pages=args.naver_pages,
            hankyung_pages=args.hankyung_pages,
            hankyung_days=args.hankyung_days,
            backfill_days=args.backfill_days,
            max_pages_per_source=args.max_pages_per_source,
            ignore_state=args.ignore_state,
            sources=tuple(item.strip().lower() for item in str(args.sources).split(",") if item.strip()),
            fnguide_symbol_limit=args.fnguide_symbol_limit,
            fnguide_symbols=tuple(item.strip() for item in str(args.fnguide_symbols).split(",") if item.strip()),
            sleep_sec=0.0,
        ).collect_once()
    raw_df = load_raw_reports(days=args.days)
    if raw_df.empty:
        return {"collector": collector_result, "report_symbols": 0, "warmed_symbols": 0, "status": "no_raw_reports"}

    raw_df["symbol"] = raw_df["symbol"].astype(str).str.zfill(6)
    raw_df["published_at"] = pd.to_datetime(raw_df.get("published_at"), errors="coerce")
    valid_published = raw_df["published_at"].dropna()
    if not valid_published.empty:
        start_date = (valid_published.min().normalize() - pd.Timedelta(days=max(5, int(args.lookback_pad_days)))).strftime("%Y-%m-%d")
        end_date = (valid_published.max().normalize() + pd.Timedelta(days=max(10, int(args.forward_pad_days)))).strftime("%Y-%m-%d")
    else:
        today = pd.Timestamp.now(tz="Asia/Seoul").normalize()
        start_date = (today - pd.Timedelta(days=max(5, int(args.lookback_pad_days)))).strftime("%Y-%m-%d")
        end_date = (today + pd.Timedelta(days=max(10, int(args.forward_pad_days)))).strftime("%Y-%m-%d")

    for index_code in ("KOSPI", "KOSDAQ"):
        load_index_history(index_code, start_date, end_date)
    _, stats = warm_sector_peer_price_cache(
        raw_df["symbol"].tolist(),
        start_date=start_date,
        end_date=end_date,
        warm_limit_per_sector=args.warm_limit_per_sector,
        warm_limit_total=args.warm_limit_total,
    )
    payload = {
        "collector": collector_result,
        "report_symbols": int(raw_df["symbol"].nunique()),
        "date_range": f"{start_date}~{end_date}",
        **stats,
        "status": "ok",
    }
    _save_state(payload)
    return payload


def _print_digest(payload: dict[str, object]) -> None:
    print("[Analyst Cache Warmup]")
    print(f"- generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"- status: {payload.get('status')}")
    print(f"- report symbols: {payload.get('report_symbols', 0)} | target: {payload.get('target_symbols', 0)} | warmed: {payload.get('warmed_symbols', 0)}")
    if payload.get("date_range"):
        print(f"- date range: {payload.get('date_range')}")
    collector = payload.get("collector") or {}
    if collector:
        print(f"- collector: naver={collector.get('naver', 0)} hankyung={collector.get('hankyung', 0)}")
    if payload.get("sample_symbols"):
        print("- sample symbols: " + ", ".join(payload.get("sample_symbols") or []))


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            try:
                payload = run_once(args)
                _print_digest(payload)
            except Exception:
                log.exception("analyst cache warmer failed")
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        _print_digest(run_once(args))
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
