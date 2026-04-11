from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

import pandas as pd

from stock_card_pipeline import build_stock_card_frame


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(ROOT_DIR, "ml", "snapshots")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ML-based stock and sector recommendation snapshot from unified cards.")
    parser.add_argument("--analyst-days", type=int, default=30)
    parser.add_argument("--flow-days", type=int, default=3)
    parser.add_argument("--top-n", type=int, default=10)
    return parser.parse_args()


def build_summary(card_df: pd.DataFrame, top_n: int = 10) -> dict:
    if card_df.empty:
        return {"snapshot_at": datetime.now().isoformat(timespec="seconds"), "status": "empty", "top_stocks": [], "top_sectors": []}
    ml_df = card_df.copy()
    ml_df["ml_pred_score"] = pd.to_numeric(ml_df.get("ml_pred_score"), errors="coerce")
    ml_df["ml_pred_return_5d"] = pd.to_numeric(ml_df.get("ml_pred_return_5d"), errors="coerce")
    ml_df["ml_sector_score"] = pd.to_numeric(ml_df.get("ml_sector_score"), errors="coerce")
    ml_df = ml_df.sort_values(["ml_pred_score", "card_score"], ascending=[False, False])
    top_stocks = []
    for row in ml_df.head(top_n).to_dict("records"):
        top_stocks.append(
            {
                "symbol": str(row.get("symbol", "")),
                "name": row.get("name") or "",
                "sector": row.get("sector") or "Unknown",
                "ml_pred_score": round(float(row.get("ml_pred_score", 0) or 0), 4),
                "ml_pred_return_5d_pct": round(float(row.get("ml_pred_return_5d", 0) or 0) * 100.0, 4),
                "card_score": round(float(row.get("card_score", 0) or 0), 4),
                "macro_micro_interaction_score": round(float(row.get("macro_micro_interaction_score", 0) or 0), 4),
            }
        )
    sector_df = (
        ml_df.groupby("sector", dropna=False)
        .agg(
            ml_sector_score=("ml_sector_score", "mean"),
            avg_card_score=("card_score", "mean"),
            leaders=("name", lambda s: ", ".join(list(s.head(2)))),
        )
        .reset_index()
        .sort_values(["ml_sector_score", "avg_card_score"], ascending=[False, False])
    )
    top_sectors = []
    for row in sector_df.head(top_n).to_dict("records"):
        top_sectors.append(
            {
                "sector": row.get("sector") or "Unknown",
                "ml_sector_score": round(float(row.get("ml_sector_score", 0) or 0), 4),
                "avg_card_score": round(float(row.get("avg_card_score", 0) or 0), 4),
                "leaders": row.get("leaders") or "",
            }
        )
    return {
        "snapshot_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ok" if ml_df["ml_pred_score"].notna().any() else "fallback",
        "top_stocks": top_stocks,
        "top_sectors": top_sectors,
    }


def save_outputs(card_df: pd.DataFrame, summary: dict) -> dict[str, str]:
    os.makedirs(REPORT_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(REPORT_DIR, f"ml_recommendation_{stamp}.csv")
    json_path = os.path.join(REPORT_DIR, f"ml_recommendation_{stamp}.json")
    latest_csv = os.path.join(REPORT_DIR, "ml_recommendation_latest.csv")
    latest_json = os.path.join(REPORT_DIR, "ml_recommendation_latest.json")
    card_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    card_df.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    with open(latest_json, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    return {"csv": csv_path, "json": json_path}


def main() -> None:
    args = parse_args()
    card_df = build_stock_card_frame(analyst_days=args.analyst_days, flow_days=args.flow_days)
    summary = build_summary(card_df, top_n=args.top_n)
    paths = save_outputs(card_df, summary)
    print(f"ML recommendation snapshot status: {summary['status']}")
    print(f"- csv: {paths['csv']}")
    print(f"- json: {paths['json']}")
    for item in summary.get("top_stocks", [])[: args.top_n]:
        print(f"- {item['name']}({item['symbol']}) | {item['sector']} | ml={item['ml_pred_score']} | pred5d={item['ml_pred_return_5d_pct']}%")
    if summary.get("top_sectors"):
        print("Top sectors:")
        for item in summary["top_sectors"][: args.top_n]:
            print(f"- {item['sector']} | ml={item['ml_sector_score']} | card={item['avg_card_score']} | leaders={item['leaders']}")


if __name__ == "__main__":
    main()
