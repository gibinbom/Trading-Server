from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

try:
    from disclosure_event_pipeline import (
        build_backtest_frames,
        build_sector_summary,
        build_slack_digest,
        load_event_records,
        save_backtest_outputs,
    )
    from disclosure_event_collector import collect_once as collect_events_once
    from utils.slack import notify_error, send_slack, upload_slack_file
except Exception:
    from Disclosure.disclosure_event_pipeline import (
        build_backtest_frames,
        build_sector_summary,
        build_slack_digest,
        load_event_records,
        save_backtest_outputs,
    )
    from Disclosure.disclosure_event_collector import collect_once as collect_events_once
    from Disclosure.utils.slack import notify_error, send_slack, upload_slack_file


log = logging.getLogger("disclosure.event_reporter")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily disclosure-event backtests and send Slack summaries.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print locally instead of sending Slack.")
    parser.add_argument("--days", type=int, default=45, help="How many event-log days to include.")
    parser.add_argument("--start-date", default="", help="Optional inclusive start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default="", help="Optional inclusive end date YYYY-MM-DD.")
    parser.add_argument("--drop-pct", type=float, default=8.0, help="Drop threshold for supply rebound backtest.")
    parser.add_argument("--recovery-ratio", type=float, default=0.5, help="Recovered fraction of drawdown required for entry.")
    parser.add_argument("--rebound-search-days", type=int, default=10, help="Max days to wait for rebound entry.")
    parser.add_argument("--times", default="08:10,20:10", help="Comma-separated HH:MM scheduler list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    parser.add_argument("--upload-files", action="store_true", help="Try uploading HTML/CSV artifacts when Slack bot token is configured.")
    parser.add_argument("--collect-before-report", action="store_true", help="Run the lightweight disclosure event collector before building the report.")
    parser.add_argument("--collector-max-pages", type=int, default=2, help="How many recent DART pages to scan when collect-before-report is enabled.")
    parser.add_argument("--collector-markets", default="KOSPI,KOSDAQ", help="Comma-separated market filter for collect-before-report.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _collect_if_enabled(args: argparse.Namespace) -> None:
    if not args.collect_before_report:
        return
    existing_records = load_event_records(
        days=args.days,
        start_date=args.start_date or None,
        end_date=args.end_date or None,
    )
    collector_args = argparse.Namespace(
        once=True,
        poll_sec=0,
        off_hours_poll_sec=0,
        max_pages=args.collector_max_pages,
        backfill_days=args.days if not existing_records and not (args.start_date or args.end_date) else 0,
        start_date="",
        end_date="",
        markets=args.collector_markets,
        ignore_seen=False,
    )
    if not existing_records and (args.start_date or args.end_date):
        collector_args.start_date = args.start_date or ""
        collector_args.end_date = args.end_date or ""
    collected = collect_events_once(collector_args)
    log.info("collect-before-report appended %d classified rows", collected)


def build_and_send_report(args: argparse.Namespace) -> None:
    _collect_if_enabled(args)
    records = load_event_records(
        days=args.days,
        start_date=args.start_date or None,
        end_date=args.end_date or None,
    )
    detail_df, summary_df, metadata = build_backtest_frames(
        records,
        rebound_drop_pct=args.drop_pct,
        rebound_recovery_ratio=args.recovery_ratio,
        rebound_search_days=args.rebound_search_days,
    )
    label = datetime.now().strftime("%Y%m%d")
    paths = save_backtest_outputs(detail_df, summary_df, metadata, label=label)
    digest = build_slack_digest(summary_df, metadata, sector_summary_df=build_sector_summary(detail_df))
    title = f"[Disclosure] Event Backtest {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    if args.print_only:
        print(title)
        print(digest)
        for key, value in paths.items():
            print(f"{key}: {value}")
        return

    send_slack(digest, title=title, msg_type="info")
    log.info("event backtest summary saved: %s", paths["summary_csv"])

    if not args.upload_files:
        return

    uploaded_html = upload_slack_file(
        paths["report_html"],
        title="Disclosure Event Report (HTML)",
        initial_comment="Daily disclosure event backtest report",
    )
    uploaded_csv = upload_slack_file(
        paths["summary_csv"],
        title="Disclosure Event Summary (CSV)",
        initial_comment="Daily disclosure event summary table",
    )
    if not uploaded_html and not uploaded_csv:
        send_slack(
            "Slack file upload token/channel is missing. Artifacts are saved locally:\n"
            f"- HTML: `{paths['report_html']}`\n"
            f"- CSV: `{paths['summary_csv']}`",
            title="[Disclosure] Event Backtest Artifacts",
            msg_type="warning",
        )


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Disclosure event reporter scheduler started: %s", ", ".join(schedule_times))

    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")

        if hhmm in schedule_times and run_key != last_run_key:
            log.info("Disclosure event scheduled job triggered at %s", run_key)
            try:
                build_and_send_report(args)
            except Exception as exc:
                log.exception("Disclosure event scheduled job failed")
                if not args.print_only:
                    notify_error("Disclosure Event Reporter", str(exc))
            last_run_key = run_key

        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.once:
        build_and_send_report(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
