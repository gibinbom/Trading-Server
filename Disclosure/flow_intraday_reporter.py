from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

from flow_intraday_backtest import build_intraday_backtest, build_intraday_digest, save_intraday_backtest
from utils.slack import notify_error, send_slack, upload_slack_file


log = logging.getLogger("disclosure.flow_intraday")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run intraday backtests on flow-event logs.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print digest locally.")
    parser.add_argument("--upload-files", action="store_true", help="Upload CSV files if token exists.")
    parser.add_argument("--days", type=int, default=3, help="How many recent log days to include.")
    parser.add_argument("--times", default="20:15", help="Comma-separated HH:MM scheduler list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def build_and_send_report(args: argparse.Namespace) -> None:
    detail, summary = build_intraday_backtest(days=args.days)
    paths = save_intraday_backtest(detail, summary)
    digest = build_intraday_digest(summary)
    title = f"[Flow] Intraday Backtest {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
    if args.print_only:
        print(title)
        print(digest)
        print(paths)
        return
    send_slack(digest, title=title, msg_type="info")
    if args.upload_files:
        upload_slack_file(paths["summary_csv"], title="flow_intraday_summary", initial_comment="Flow intraday backtest summary")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Flow intraday scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            try:
                build_and_send_report(args)
            except Exception as exc:
                log.exception("flow intraday backtest failed")
                if not args.print_only:
                    notify_error("Flow Intraday Reporter", str(exc))
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        build_and_send_report(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
