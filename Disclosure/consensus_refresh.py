from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

try:
    from consensus_crawler.mongo_repo import ConsensusMongoRepo
    from consensus_crawler.naver_wisereport import fetch_quarter_consensus
    from valuation_refresh_support import load_eligible_listing_df, load_incremental_consensus_symbols
except Exception:
    from Disclosure.consensus_crawler.mongo_repo import ConsensusMongoRepo
    from Disclosure.consensus_crawler.naver_wisereport import fetch_quarter_consensus
    from Disclosure.valuation_refresh_support import load_eligible_listing_df, load_incremental_consensus_symbols


log = logging.getLogger("disclosure.consensus_refresh")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh annual consensus coverage for fair-value inputs.")
    parser.add_argument("--mode", choices=("full", "incremental"), default="full", help="Refresh the full eligible universe or an incremental subset.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print refresh summary.")
    parser.add_argument("--limit", type=int, default=0, help="Optional symbol limit for quick checks.")
    parser.add_argument("--workers", type=int, default=12, help="Concurrent refresh workers.")
    parser.add_argument("--quote-top-n", type=int, default=500, help="Incremental mode delayed-quote symbol cap.")
    parser.add_argument("--tp-top-n", type=int, default=500, help="Incremental mode TP-visible symbol cap.")
    parser.add_argument("--analyst-days", type=int, default=21, help="Incremental mode analyst lookback.")
    parser.add_argument("--event-days", type=int, default=45, help="Incremental mode event lookback.")
    parser.add_argument("--times", default="06:40", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _load_codes(args: argparse.Namespace) -> list[str]:
    if args.mode == "full":
        listing_df = load_eligible_listing_df()
        codes = listing_df["symbol"].astype(str).str.zfill(6).tolist() if not listing_df.empty else []
    else:
        codes = load_incremental_consensus_symbols(
            analyst_days=args.analyst_days,
            event_days=args.event_days,
            quote_limit=args.quote_top_n,
            tp_limit=args.tp_top_n,
        )
    if args.limit and args.limit > 0:
        codes = codes[: int(args.limit)]
    return codes


def _fetch_one(code: str) -> tuple[str, dict[str, Any] | None]:
    try:
        consensus = fetch_quarter_consensus(code)
        return code, consensus.to_dict()
    except Exception as exc:
        log.debug("consensus refresh failed for %s: %s", code, exc)
        return code, None


def refresh_codes(codes: list[str], *, workers: int = 12) -> dict[str, Any]:
    repo = ConsensusMongoRepo()
    if not codes:
        return {"requested": 0, "saved": 0, "skipped": 0, "failed": 0}

    saved = 0
    skipped = 0
    failed = 0
    max_workers = max(1, min(int(workers), 16))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, code): code for code in codes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                _, payload = future.result()
            except Exception:
                payload = None
            if not payload:
                failed += 1
                continue
            if repo.upsert_today(code, payload):
                saved += 1
            else:
                skipped += 1
    return {"requested": len(codes), "saved": saved, "skipped": skipped, "failed": failed}


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    codes = _load_codes(args)
    result = refresh_codes(codes, workers=args.workers)
    if args.print_only:
        print(
            f"[consensus_refresh:{args.mode}] requested={result['requested']} "
            f"saved={result['saved']} skipped={result['skipped']} failed={result['failed']}"
        )
    return result


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("consensus refresh scheduler started (%s): %s", args.mode, ", ".join(schedule_times))
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
