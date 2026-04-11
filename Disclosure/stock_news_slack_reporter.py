from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

try:
    from stock_news_pipeline import (
        build_stock_news_digest,
        build_stock_news_summary,
        load_raw_stock_news,
        save_stock_news_summary,
        score_stock_news,
    )
    from utils.slack import notify_error, send_slack, upload_slack_file
except Exception:
    from Disclosure.stock_news_pipeline import (
        build_stock_news_digest,
        build_stock_news_summary,
        load_raw_stock_news,
        save_stock_news_summary,
        score_stock_news,
    )
    from Disclosure.utils.slack import notify_error, send_slack, upload_slack_file


log = logging.getLogger("disclosure.stock_news_slack")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build stock-news scores and send Slack digest/files.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print digest locally.")
    parser.add_argument("--upload-files", action="store_true", help="Upload CSV/JSON if Slack bot token exists.")
    parser.add_argument("--days", type=int, default=7, help="News lookback window.")
    parser.add_argument("--top-n", type=int, default=30, help="Top names in summary.")
    parser.add_argument("--collect-before-report", action="store_true", help="Run stock news collector before summarizing.")
    parser.add_argument("--collector-top-n", type=int, default=0, help="Collector universe size. 0 means full universe.")
    parser.add_argument("--collector-min-marcap", type=int, default=0, help="Collector market-cap floor.")
    parser.add_argument("--collector-markets", default="KOSPI,KOSDAQ", help="Collector market list.")
    parser.add_argument("--collector-sources", default="naver,google", help="Collector sources.")
    parser.add_argument("--collector-google-top-n", type=int, default=300, help="Google collection cap.")
    parser.add_argument("--times", default="08:12,15:42,20:12", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _run_collector(args: argparse.Namespace) -> None:
    try:
        from stock_news_collector import StockNewsCollector
    except Exception:
        from Disclosure.stock_news_collector import StockNewsCollector

    collector = StockNewsCollector(
        top_n=args.collector_top_n,
        min_marcap=args.collector_min_marcap,
        markets=[item.strip() for item in str(args.collector_markets).split(",") if item.strip()],
        sources=[item.strip() for item in str(args.collector_sources).split(",") if item.strip()],
        google_top_n=args.collector_google_top_n,
    )
    result = collector.collect_once()
    log.info("stock news collect-before-report: %s", result)


def build_and_send_report(args: argparse.Namespace) -> None:
    if args.collect_before_report:
        _run_collector(args)

    raw_df = load_raw_stock_news(days=args.days)
    scored = score_stock_news(raw_df)
    summary = build_stock_news_summary(scored, top_n=args.top_n)
    digest = build_stock_news_digest(summary, top_n=min(args.top_n, 10))
    paths = save_stock_news_summary(scored, summary)
    title = f"[종목뉴스] 정제 스냅샷 {datetime.now().strftime('%Y%m%d %H:%M:%S')}"

    if args.print_only:
        print(title)
        print(digest)
    else:
        send_slack(digest, title=title, msg_type="info")

    if args.upload_files and not args.print_only:
        upload_slack_file(paths["scored_csv"], title="stock_news_scored", initial_comment="종목 뉴스 점수 CSV")
        upload_slack_file(paths["summary_json"], title="stock_news_summary", initial_comment="종목 뉴스 요약 JSON")


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Stock-news Slack scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            try:
                build_and_send_report(args)
            except Exception as exc:
                log.exception("Stock-news scheduled job failed")
                if not args.print_only:
                    notify_error("Stock News Slack Reporter", str(exc))
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
