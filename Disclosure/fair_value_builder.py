from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

try:
    from factor_pipeline import FactorSnapshotBuilder
    from fair_value_engine import build_fair_value_digest, build_fair_value_snapshot, build_fair_value_summary, save_fair_value_snapshot
except Exception:
    from Disclosure.factor_pipeline import FactorSnapshotBuilder
    from Disclosure.fair_value_engine import build_fair_value_digest, build_fair_value_snapshot, build_fair_value_summary, save_fair_value_snapshot


log = logging.getLogger("disclosure.fair_value_builder")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build mixed fair-value snapshots for the latest full-universe factor data.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print the digest locally after saving the snapshot.")
    parser.add_argument("--save-only", action="store_true", help="Save artifacts silently without printing the digest.")
    parser.add_argument("--top-n", type=int, default=20, help="How many names to keep in the digest/summary leaders.")
    parser.add_argument("--analyst-days", type=int, default=30, help="Analyst report lookback window.")
    parser.add_argument("--event-days", type=int, default=45, help="Disclosure/event lookback window.")
    parser.add_argument("--build-factor-before-run", action="store_true", help="Refresh the factor snapshot before fair-value calculation.")
    parser.add_argument("--factor-top-n", type=int, default=300, help="Universe size when factor refresh is enabled.")
    parser.add_argument("--full-universe", action="store_true", help="Refresh factor snapshot for the full filtered universe.")
    parser.add_argument("--markets", default="KOSPI,KOSDAQ", help="Comma-separated market list.")
    parser.add_argument("--min-marcap", type=int, default=0, help="Factor snapshot minimum market cap.")
    parser.add_argument("--price-lookback-days", type=int, default=260, help="Factor price lookback window.")
    parser.add_argument("--flow-top-n", type=int, default=800, help="Factor flow cap. 0 means all.")
    parser.add_argument("--consensus-top-n", type=int, default=1200, help="Factor consensus cap. 0 means all.")
    parser.add_argument("--news-lookback-days", type=int, default=7, help="Factor stock-news lookback.")
    parser.add_argument("--no-flow", action="store_true", help="Disable factor flow features during refresh.")
    parser.add_argument("--no-consensus", action="store_true", help="Disable factor consensus features during refresh.")
    parser.add_argument("--no-news", action="store_true", help="Disable factor stock-news features during refresh.")
    parser.add_argument("--exclude-construction", action="store_true", help="Apply existing construction filter in factor refresh.")
    parser.add_argument("--times", default="06:55,11:55,15:40,20:20", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _refresh_factor_snapshot(args: argparse.Namespace) -> None:
    top_n = 0 if args.full_universe else args.factor_top_n
    markets = [item.strip() for item in str(args.markets).split(",") if item.strip()]
    builder = FactorSnapshotBuilder(
        top_n=top_n,
        min_marcap_krw=args.min_marcap,
        markets=markets,
        price_lookback_days=args.price_lookback_days,
        include_flow=not args.no_flow,
        include_consensus=not args.no_consensus,
        include_news=not args.no_news,
        flow_top_n=args.flow_top_n,
        consensus_top_n=args.consensus_top_n,
        news_lookback_days=args.news_lookback_days,
        exclude_construction=args.exclude_construction,
    )
    snapshot_df = builder.build_snapshot()
    summary = builder.build_summary(snapshot_df)
    paths = builder.save_snapshot(snapshot_df, summary=summary)
    log.info("factor refreshed before fair-value build: %s", paths["latest_snapshot_csv"])


def build_and_save(args: argparse.Namespace) -> dict[str, object]:
    if args.build_factor_before_run:
        _refresh_factor_snapshot(args)
    snapshot_df = build_fair_value_snapshot(analyst_days=args.analyst_days, event_days=args.event_days)
    summary = build_fair_value_summary(snapshot_df, top_n=args.top_n)
    paths = save_fair_value_snapshot(snapshot_df, summary=summary)
    digest = build_fair_value_digest(summary, top_n=min(args.top_n, 10))
    log.info("fair-value snapshot saved: %s", paths["latest_snapshot_csv"])
    return {"paths": paths, "summary": summary, "digest": digest}


def run_once(args: argparse.Namespace) -> None:
    result = build_and_save(args)
    if args.save_only and not args.print_only:
        return
    title = f"[적정가] 혼합 적정가 스냅샷 {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
    print(title)
    print(result["digest"])


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Fair-value scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            build_and_save(args)
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
