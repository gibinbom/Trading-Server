from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime
from typing import Any

import pandas as pd

try:
    from analyst_report_features import build_analyst_feature_frame
except Exception:
    from Disclosure.analyst_report_features import build_analyst_feature_frame


ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyst_reports")
SUMMARY_DIR = os.path.join(ROOT_DIR, "summaries")


def _mean_or_zero(series) -> float:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return float(numeric.mean()) if not numeric.empty else 0.0


def build_stock_summary(scored: pd.DataFrame, top_n: int = 30) -> dict[str, Any]:
    if scored.empty:
        return {
            "snapshot_at": datetime.now().isoformat(timespec="seconds"),
            "top_stocks": [],
            "top_terms": [],
            "coverage": {
                "report_count": 0,
                "broker_count": 0,
                "priced_report_count": 0,
                "priced_report_coverage_ratio": 0.0,
                "source_counts": {},
                "daily_counts": {},
            },
        }
    features = build_analyst_feature_frame(scored)
    latest_by_symbol = scored.sort_values("published_at").groupby("symbol", dropna=False).tail(1).copy()
    latest_by_symbol["symbol"] = latest_by_symbol["symbol"].astype(str).str.zfill(6)
    latest_cols = latest_by_symbol[["symbol", "title", "calibration_status", "pdf_text_status", "report_parse_quality_status"]].rename(
        columns={"title": "latest_title", "calibration_status": "latest_calibration_status", "pdf_text_status": "latest_pdf_status", "report_parse_quality_status": "latest_parse_quality_status"}
    )
    ranked = features.merge(latest_cols, how="left", on="symbol")
    ranked["latest_title"] = ranked["latest_title"].fillna(ranked["analyst_latest_title"])
    ranked = ranked.sort_values(["analyst_conviction_score", "analyst_avg_score"], ascending=[False, False])
    top_stocks = []
    for row in ranked.head(top_n).to_dict("records"):
        top_stocks.append(
            {
                "symbol": row["symbol"],
                "name": row["name"],
                "sector": row["sector"],
                "report_count": int(row["analyst_report_count"]),
                "broker_diversity": int(row["analyst_broker_diversity"]),
                "avg_report_score": round(float(row["analyst_avg_score"]), 4),
                "avg_target_revision_pct": round(float(row["analyst_target_revision_pct"]), 4),
                "avg_novelty_score": round(float(row["analyst_novelty_score"]), 4),
                "avg_alpha_ret_5d": round(float(row["analyst_alpha_ret_5d"]), 4),
                "avg_alpha_ret_20d": round(float(row["analyst_alpha_ret_20d"]), 4),
                "avg_broker_bias_adjustment": round(float(row["analyst_bias_adjustment"]), 4),
                "avg_parse_quality_score": round(float(row["analyst_parse_quality_score"]), 4),
                "target_upside_pct": round(float(row["analyst_target_upside_pct"]), 4),
                "target_dispersion_pct": round(float(row["analyst_target_dispersion_pct"]), 4),
                "agreement_score": round(float(row["analyst_agreement_score"]), 4),
                "recency_score": round(float(row["analyst_recency_score"]), 4),
                "revision_breadth_score": round(float(row.get("analyst_revision_breadth_score", 0) or 0), 4),
                "revision_breadth_count": int(row.get("analyst_revision_breadth_count", 0) or 0),
                "positive_revision_ratio": round(float(row.get("analyst_positive_revision_ratio", 0) or 0), 4),
                "peer_spillover_score": round(float(row.get("analyst_peer_spillover_score", 0) or 0), 4),
                "peer_alpha_5d": round(float(row.get("analyst_peer_alpha_5d", 0) or 0), 4),
                "peer_support_count": int(row.get("analyst_peer_support_count", 0) or 0),
                "latest_title": row.get("latest_title") or row.get("analyst_latest_title") or "",
                "latest_calibration_status": row.get("latest_calibration_status") or "",
                "latest_pdf_status": row.get("latest_pdf_status") or "",
                "latest_parse_quality_status": row.get("latest_parse_quality_status") or "",
                "conviction_score": round(float(row["analyst_conviction_score"]), 4),
            }
        )
    top_terms = Counter()
    for title in scored["title"].astype(str):
        for token in title.replace("/", " ").replace(",", " ").split():
            if len(token) >= 2:
                top_terms[token] += 1
    broker_rows = []
    for broker, group in scored.groupby("broker", dropna=False):
        broker_name = str(broker or "").strip()
        if not broker_name:
            continue
        broker_rows.append(
            {
                "broker": broker_name,
                "report_count": int(len(group)),
                "avg_report_score": round(_mean_or_zero(group["report_sentiment_score"]), 4),
                "avg_novelty_score": round(_mean_or_zero(group.get("novelty_score")), 4),
                "avg_alpha_ret_5d": round(_mean_or_zero(group.get("alpha_ret_5d")), 4),
                "avg_rolling_alpha_mean": round(_mean_or_zero(group.get("broker_rolling_alpha_mean")), 4),
            }
        )
    broker_rows = sorted(broker_rows, key=lambda item: (item["avg_alpha_ret_5d"], item["avg_report_score"], item["report_count"]), reverse=True)
    priced_mask = pd.to_numeric(scored.get("target_price"), errors="coerce").fillna(0).gt(0)
    source_counts = (
        scored.get("source", pd.Series(dtype=str))
        .fillna("UNKNOWN")
        .astype(str)
        .value_counts()
        .sort_index()
        .to_dict()
    )
    daily_counts = (
        pd.to_datetime(scored.get("published_at"), errors="coerce", utc=True)
        .dt.tz_convert("Asia/Seoul")
        .dt.strftime("%Y-%m-%d")
        .dropna()
        .value_counts()
        .sort_index()
        .to_dict()
    )
    return {
        "snapshot_at": datetime.now().isoformat(timespec="seconds"),
        "top_stocks": top_stocks,
        "top_brokers": broker_rows[:10],
        "top_terms": [{"term": term, "count": count} for term, count in top_terms.most_common(20)],
        "coverage": {
            "report_count": int(len(scored)),
            "broker_count": int(scored.get("broker", pd.Series(dtype=str)).fillna("").astype(str).replace("", pd.NA).dropna().nunique()),
            "priced_report_count": int(priced_mask.sum()),
            "priced_report_coverage_ratio": round(float(priced_mask.mean() * 100), 2) if len(scored) else 0.0,
            "source_counts": source_counts,
            "daily_counts": daily_counts,
        },
    }


def save_summary(scored: pd.DataFrame, summary: dict[str, Any]) -> dict[str, str]:
    os.makedirs(SUMMARY_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scored_csv = os.path.join(SUMMARY_DIR, f"analyst_report_scored_{timestamp}.csv")
    summary_json = os.path.join(SUMMARY_DIR, f"analyst_report_summary_{timestamp}.json")
    latest_scored = os.path.join(SUMMARY_DIR, "analyst_report_scored_latest.csv")
    latest_summary = os.path.join(SUMMARY_DIR, "analyst_report_summary_latest.json")
    scored.to_csv(scored_csv, index=False, encoding="utf-8-sig")
    scored.to_csv(latest_scored, index=False, encoding="utf-8-sig")
    with open(summary_json, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    with open(latest_summary, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    return {"scored_csv": scored_csv, "summary_json": summary_json}
