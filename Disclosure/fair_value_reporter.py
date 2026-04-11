from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

try:
    from utils.slack import send_slack
except Exception:
    from Disclosure.utils.slack import send_slack

try:
    from fair_value_engine import build_fair_value_digest, build_fair_value_summary, load_latest_fair_value_frame, load_latest_fair_value_summary
except Exception:
    from Disclosure.fair_value_engine import build_fair_value_digest, build_fair_value_summary, load_latest_fair_value_frame, load_latest_fair_value_summary


log = logging.getLogger("disclosure.fair_value_reporter")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read the latest fair-value snapshot and build a human-readable digest.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print locally instead of sending to Slack.")
    parser.add_argument("--top-n", type=int, default=12, help="How many names to keep in the digest.")
    parser.add_argument("--times", default="08:12,15:42,20:12", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _load_summary(top_n: int) -> dict:
    summary = load_latest_fair_value_summary()
    if summary:
        return summary
    snapshot_df = load_latest_fair_value_frame()
    return build_fair_value_summary(snapshot_df, top_n=top_n)


def build_and_send(args: argparse.Namespace) -> None:
    summary = _load_summary(args.top_n)
    digest = build_fair_value_digest(summary, top_n=min(args.top_n, 12))
    title = f"[적정가] 혼합 적정가 브리핑 {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
    if args.print_only:
        print(title)
        print(digest)
        return
    send_slack(digest, title=title, msg_type="info")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Fair-value reporter started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            build_and_send(args)
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        build_and_send(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
