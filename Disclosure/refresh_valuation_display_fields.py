from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

try:
    from stock_financial_profile_builder import (
        _build_multiple_board,
        _build_tp_driver_breakdown,
        _normalize_symbol,
        _peer_group_key,
    )
except ModuleNotFoundError:
    from Disclosure.stock_financial_profile_builder import (
        _build_multiple_board,
        _build_tp_driver_breakdown,
        _normalize_symbol,
        _peer_group_key,
    )


ROOT = Path(__file__).resolve().parent
VALUATION_CSV_PATH = ROOT / "valuation" / "fair_value_snapshot_latest.csv"
VALUATION_JSON_PATH = ROOT / "valuation" / "fair_value_snapshot_latest.json"
WEB_PROJECTION_DIR = ROOT / "runtime" / "web_projections"
FAIR_PROJECTION_PATH = WEB_PROJECTION_DIR / "stock_fair_value_latest.json"
PROFILE_PROJECTION_PATH = WEB_PROJECTION_DIR / "stock_financial_profile_latest.json"


def refresh_display_fields(*, symbol: str | None = None) -> None:
    target_symbol = _normalize_symbol(symbol) if symbol else ""
    fair_df = pd.read_csv(VALUATION_CSV_PATH)
    fair_df["symbol"] = fair_df["symbol"].astype(str).str.zfill(6)

    non_pbr_mask = fair_df["valuation_basis_label"].fillna("").astype(str) != "PBR"
    profitability_series = pd.to_numeric(fair_df["tp_subject_profitability_pct"], errors="coerce")
    eligible_margin_mask = non_pbr_mask & profitability_series.gt(0)
    fair_df.loc[eligible_margin_mask, "operating_margin_pct"] = profitability_series[eligible_margin_mask]

    fair_df.to_csv(VALUATION_CSV_PATH, index=False, encoding="utf-8-sig")
    VALUATION_JSON_PATH.write_text(
        fair_df.to_json(orient="records", force_ascii=False, indent=2),
        encoding="utf-8",
    )

    with open(FAIR_PROJECTION_PATH, encoding="utf-8") as fp:
        fair_docs = json.load(fp)
    fair_doc_map = {str(doc.get("_id") or doc.get("symbol")).zfill(6): doc for doc in fair_docs}
    for _, row in fair_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        if target_symbol and symbol != target_symbol:
            continue
        doc = fair_doc_map.get(symbol)
        if not doc:
            continue
        if str(row.get("valuation_basis_label") or "") == "PBR":
            continue
        op_margin = row.get("operating_margin_pct")
        if pd.notna(op_margin):
            doc["operating_margin_pct"] = round(float(op_margin), 2)
    FAIR_PROJECTION_PATH.write_text(
        json.dumps(sorted(fair_doc_map.values(), key=lambda item: str(item.get("_id") or item.get("symbol")).zfill(6)), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    eligible_peer_mask = (~fair_df["valuation_proxy_used"].fillna(False).astype(bool)) & fair_df["valuation_basis_period"].fillna("").astype(str).isin(
        ["FY1", "FY0", "실제 실적", "실제 실적 + 공시 보정"]
    )
    eligible_peers = fair_df.loc[eligible_peer_mask].copy()
    eligible_peers["peer_group_key"] = eligible_peers.apply(_peer_group_key, axis=1)
    peer_groups = {
        key: group.drop(columns=["peer_group_key"]).copy()
        for key, group in eligible_peers.groupby("peer_group_key", dropna=False)
        if key
    }

    with open(PROFILE_PROJECTION_PATH, encoding="utf-8") as fp:
        profile_docs = json.load(fp)
    profile_map = {str(doc.get("_id") or doc.get("symbol")).zfill(6): doc for doc in profile_docs}
    for _, row in fair_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        if target_symbol and symbol != target_symbol:
            continue
        doc = profile_map.get(symbol)
        if not doc:
            continue
        doc["multiple_board"] = _build_multiple_board(peer_groups, row)
        doc["tp_driver_breakdown"] = _build_tp_driver_breakdown(
            peer_groups,
            row,
            doc.get("business_mix"),
            doc.get("geography_mix"),
        )
    PROFILE_PROJECTION_PATH.write_text(
        json.dumps(sorted(profile_map.values(), key=lambda item: str(item.get("_id") or item.get("symbol")).zfill(6)), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"updated operating_margin rows: {int(eligible_margin_mask.sum())}")
    print(f"updated: {VALUATION_CSV_PATH}")
    print(f"updated: {VALUATION_JSON_PATH}")
    print(f"updated: {FAIR_PROJECTION_PATH}")
    print(f"updated: {PROFILE_PROJECTION_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh valuation display fields and related projections.")
    parser.add_argument("--symbol", help="Optional 6-digit symbol to update only one stock in projection files.")
    args = parser.parse_args()
    target_symbol = _normalize_symbol(args.symbol) if args.symbol else None
    refresh_display_fields(symbol=target_symbol or None)
