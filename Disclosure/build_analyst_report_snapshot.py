from __future__ import annotations

import argparse
import logging

from analyst_report_collector import AnalystReportCollector
from analyst_report_digest import build_slack_digest
from analyst_report_pipeline import load_raw_reports, load_scored_reports_cache, score_reports
from analyst_report_summary import build_stock_summary, save_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score raw analyst reports and build a stock-level digest.")
    parser.add_argument("--days", type=int, default=90, help="Lookback window for raw reports.")
    parser.add_argument("--top-n", type=int, default=30, help="How many top stocks to keep in the summary.")
    parser.add_argument("--collect-before-report", action="store_true", help="Collect Naver/Hankyung reports before scoring.")
    parser.add_argument("--naver-pages", type=int, default=3, help="How many Naver research pages to scan.")
    parser.add_argument("--hankyung-pages", type=int, default=3, help="How many Hankyung pages to scan.")
    parser.add_argument("--hankyung-days", type=int, default=7, help="Hankyung lookback window in days.")
    parser.add_argument("--backfill-days", type=int, default=0, help="Historical analyst backfill window.")
    parser.add_argument("--max-pages-per-source", type=int, default=120, help="Maximum pages per source in backfill mode.")
    parser.add_argument("--sources", default="naver,hankyung,fnguide", help="Comma-separated sources: naver,hankyung,fnguide")
    parser.add_argument("--ignore-state", action="store_true", help="Ignore collector seen state during backfill collection.")
    parser.add_argument("--fnguide-symbol-limit", type=int, default=0, help="Optional FnGuide symbol cap. 0 means all candidates.")
    parser.add_argument("--fnguide-symbols", default="", help="Optional comma-separated FnGuide symbol override.")
    parser.add_argument("--skip-pdf", action="store_true", help="Skip PDF body enrichment for faster runs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.collect_before_report:
        collector = AnalystReportCollector(
            naver_pages=args.naver_pages,
            hankyung_pages=args.hankyung_pages,
            hankyung_days=args.hankyung_days,
            backfill_days=args.backfill_days,
            max_pages_per_source=args.max_pages_per_source,
            ignore_state=args.ignore_state,
            sources=tuple(item.strip().lower() for item in str(args.sources).split(",") if item.strip()),
            fnguide_symbol_limit=args.fnguide_symbol_limit,
            fnguide_symbols=tuple(item.strip() for item in str(args.fnguide_symbols).split(",") if item.strip()),
        )
        logging.getLogger("disclosure.analyst_snapshot").info("collector result: %s", collector.collect_once())

    scored_df = load_scored_reports_cache(
        days=args.days,
        require_fresh=not args.collect_before_report,
        require_pdf=not args.skip_pdf,
    )
    if scored_df.empty:
        raw_df = load_raw_reports(days=args.days)
        scored_df = score_reports(raw_df, use_pdf_text=not args.skip_pdf)
    else:
        logging.getLogger("disclosure.analyst_snapshot").info("using fresh scored analyst cache: rows=%s", len(scored_df))
    summary = build_stock_summary(scored_df, top_n=args.top_n)
    paths = save_summary(scored_df, summary)

    print(build_slack_digest(summary))
    print(f"\n- scored csv: {paths['scored_csv']}")
    print(f"- summary json: {paths['summary_json']}")


if __name__ == "__main__":
    main()
