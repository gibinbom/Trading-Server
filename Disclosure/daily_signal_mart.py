from __future__ import annotations

import json
import os
import shutil
from typing import Any

import pandas as pd


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
MART_DIR = os.path.join(ROOT_DIR, "marts")


def build_mart_summary(card_df: pd.DataFrame) -> dict[str, Any]:
    if card_df is None or card_df.empty:
        return {
            "generated_at": pd.Timestamp.now().isoformat(),
            "row_count": 0,
            "unknown_sector_count": 0,
            "top_symbols": [],
        }
    ranking_df = card_df.copy()
    for col in ["card_score", "event_alpha_score", "analyst_conviction_score"]:
        if col not in ranking_df.columns:
            ranking_df[col] = 0.0
        ranking_df[col] = pd.to_numeric(ranking_df[col], errors="coerce").fillna(0.0)
    ranked = ranking_df.sort_values(["card_score", "event_alpha_score", "analyst_conviction_score"], ascending=[False, False, False]).head(10)
    return {
        "generated_at": pd.Timestamp.now().isoformat(),
        "row_count": int(len(card_df)),
        "unknown_sector_count": int(card_df.get("sector", pd.Series(dtype=object)).fillna("Unknown").astype(str).eq("Unknown").sum()),
        "factor_coverage": int(card_df.get("composite_score", pd.Series(dtype=float)).notna().sum()),
        "analyst_coverage": int(card_df.get("analyst_conviction_score", pd.Series(dtype=float)).notna().sum()),
        "flow_coverage": int(card_df.get("flow_state_score", pd.Series(dtype=float)).notna().sum()),
        "intraday_coverage": int(card_df.get("flow_intraday_edge_score", pd.Series(dtype=float)).notna().sum()),
        "event_coverage": int(card_df.get("event_alpha_score", pd.Series(dtype=float)).notna().sum()),
        "ml_coverage": int(card_df.get("ml_pred_score", pd.Series(dtype=float)).notna().sum()),
        "valuation_coverage": int(pd.to_numeric(card_df.get("fair_value_base", pd.Series(dtype=float)), errors="coerce").notna().sum()),
        "top_symbols": [
            {
                "symbol": str(row.get("symbol") or "").zfill(6),
                "name": row.get("name") or "",
                "sector": row.get("sector") or "Unknown",
                "card_score": round(float(row.get("card_score", 0) or 0), 4),
                "event_alpha_score": round(float(row.get("event_alpha_score", 0) or 0), 4),
                "analyst_conviction_score": round(float(row.get("analyst_conviction_score", 0) or 0), 4),
                "fair_value_base": round(float(row.get("fair_value_base", 0) or 0), 2) if pd.notna(row.get("fair_value_base")) else None,
                "fair_value_gap_pct": round(float(row.get("fair_value_gap_pct", 0) or 0), 2) if pd.notna(row.get("fair_value_gap_pct")) else None,
                "fair_value_confidence_score": round(float(row.get("fair_value_confidence_score", 0) or 0), 4) if pd.notna(row.get("fair_value_confidence_score")) else None,
                "valuation_reason_summary": row.get("valuation_reason_summary") or "",
            }
            for row in ranked.to_dict("records")
        ],
    }


def save_daily_signal_mart(card_df: pd.DataFrame, summary: dict[str, Any] | None = None) -> dict[str, str]:
    os.makedirs(MART_DIR, exist_ok=True)
    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    summary = summary or build_mart_summary(card_df)
    csv_path = os.path.join(MART_DIR, f"daily_signal_mart_{stamp}.csv")
    json_path = os.path.join(MART_DIR, f"daily_signal_mart_{stamp}.json")
    latest_csv = os.path.join(MART_DIR, "daily_signal_mart_latest.csv")
    latest_json = os.path.join(MART_DIR, "daily_signal_mart_latest.json")
    card_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    card_df.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    with open(latest_json, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)

    parquet_path = ""
    latest_parquet = ""
    try:
        parquet_path = os.path.join(MART_DIR, f"daily_signal_mart_{stamp}.parquet")
        latest_parquet = os.path.join(MART_DIR, "daily_signal_mart_latest.parquet")
        card_df.to_parquet(parquet_path, index=False)
        shutil.copyfile(parquet_path, latest_parquet)
    except Exception:
        parquet_path = ""
        latest_parquet = ""

    return {
        "mart_csv": csv_path,
        "mart_json": json_path,
        "latest_mart_csv": latest_csv,
        "latest_mart_json": latest_json,
        "mart_parquet": parquet_path,
        "latest_mart_parquet": latest_parquet,
    }
