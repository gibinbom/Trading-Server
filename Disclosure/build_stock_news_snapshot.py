from __future__ import annotations

import argparse

try:
    from stock_news_pipeline import (
        build_stock_news_digest,
        build_stock_news_summary,
        load_raw_stock_news,
        save_stock_news_summary,
        score_stock_news,
    )
except Exception:
    from Disclosure.stock_news_pipeline import (
        build_stock_news_digest,
        build_stock_news_summary,
        load_raw_stock_news,
        save_stock_news_summary,
        score_stock_news,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build stock-level news scores from raw news logs.")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days.")
    parser.add_argument("--top-n", type=int, default=30, help="How many symbols to keep in summary.")
    parser.add_argument("--print-only", action="store_true", help="Print digest without saving files.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_df = load_raw_stock_news(days=args.days)
    scored = score_stock_news(raw_df)
    summary = build_stock_news_summary(scored, top_n=args.top_n)
    digest = build_stock_news_digest(summary)
    print(digest)
    if not args.print_only:
        paths = save_stock_news_summary(scored, summary)
        print(f"\n[files] scored_csv={paths['scored_csv']}")
        print(f"[files] summary_json={paths['summary_json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
