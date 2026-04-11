from __future__ import annotations

import argparse
import logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and store daily factor snapshots for the Korea equity universe.")
    parser.add_argument("--top-n", type=int, default=300, help="Number of stocks from the watch universe to analyze.")
    parser.add_argument("--full-universe", action="store_true", help="Analyze the full filtered KOSPI/KOSDAQ universe.")
    parser.add_argument("--markets", default="KOSPI,KOSDAQ", help="Comma-separated market list, e.g. KOSPI,KOSDAQ.")
    parser.add_argument("--min-marcap", type=int, default=0, help="Minimum market cap filter in KRW.")
    parser.add_argument("--price-lookback-days", type=int, default=260, help="Trading lookback window for momentum/risk factors.")
    parser.add_argument("--flow-top-n", type=int, default=0, help="Only fetch flow factors for top-N names by market cap. 0 means all.")
    parser.add_argument("--consensus-top-n", type=int, default=0, help="Only fetch consensus factors for top-N names by market cap. 0 means all.")
    parser.add_argument("--no-flow", action="store_true", help="Disable KIS investor flow factors.")
    parser.add_argument("--no-consensus", action="store_true", help="Disable Mongo consensus-based factors.")
    parser.add_argument("--exclude-construction", action="store_true", help="Apply the existing construction-sector exclusion filter.")
    parser.add_argument("--summary-top-n", type=int, default=20, help="Number of top names to include in summary output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    from factor_pipeline import FactorSnapshotBuilder

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    top_n = 0 if args.full_universe else args.top_n
    markets = [item.strip() for item in str(args.markets).split(",") if item.strip()]

    builder = FactorSnapshotBuilder(
        top_n=top_n,
        min_marcap_krw=args.min_marcap,
        markets=markets,
        price_lookback_days=args.price_lookback_days,
        include_flow=not args.no_flow,
        include_consensus=not args.no_consensus,
        flow_top_n=args.flow_top_n,
        consensus_top_n=args.consensus_top_n,
        exclude_construction=args.exclude_construction,
    )

    snapshot_df = builder.build_snapshot()
    summary = builder.build_summary(snapshot_df, top_n=args.summary_top_n)
    paths = builder.save_snapshot(snapshot_df, summary=summary)

    print("Factor snapshot build complete.")
    print(f"- rows: {len(snapshot_df)}")
    print(f"- snapshot csv: {paths['snapshot_csv']}")
    print(f"- summary json: {paths['summary_json']}")

    print("\nTop composite names:")
    for item in summary["top_composite"][:10]:
        print(
            f"- {item['name']}({item['symbol']}) | {item['sector']} | "
            f"composite={item['composite_score']:.4f} | "
            f"value={item['value_score']:.4f} | momentum={item['momentum_score']:.4f} | "
            f"quality={item['quality_score']:.4f} | flow={item['flow_score']:.4f}"
        )

    print("\nTop sector-relative laggards:")
    for item in summary["top_reversion"][:10]:
        print(
            f"- {item['name']}({item['symbol']}) | {item['sector']} | "
            f"reversion={item['sector_reversion_signal']:.4f} | "
            f"value={item['value_score']:.4f} | momentum={item['momentum_score']:.4f} | "
            f"flow={item['flow_score']:.4f}"
        )


if __name__ == "__main__":
    main()
