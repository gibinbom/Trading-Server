from __future__ import annotations

import glob
import os

import pandas as pd

try:
    from analyst_report_features import build_analyst_feature_frame
    from analyst_report_pipeline import load_raw_reports, load_scored_reports_cache, score_reports
    from event_alpha_features import build_event_alpha_frame
    from signals.slack_log_loader import collect_flow_snapshots
except Exception:
    from Disclosure.analyst_report_features import build_analyst_feature_frame
    from Disclosure.analyst_report_pipeline import load_raw_reports, load_scored_reports_cache, score_reports
    from Disclosure.event_alpha_features import build_event_alpha_frame
    from Disclosure.signals.slack_log_loader import collect_flow_snapshots


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
FACTOR_DIR = os.path.join(ROOT_DIR, "factors", "snapshots")
SIGNAL_LOG_DIR = os.path.join(ROOT_DIR, "signals", "logs")
VALUATION_DIR = os.path.join(ROOT_DIR, "valuation")


def _latest_path(pattern: str) -> str:
    matches = sorted(glob.glob(pattern), reverse=True)
    return matches[0] if matches else ""


def load_factor_frame() -> pd.DataFrame:
    latest = os.path.join(FACTOR_DIR, "factor_snapshot_latest.csv")
    path = latest if os.path.exists(latest) else _latest_path(os.path.join(FACTOR_DIR, "factor_snapshot_*.csv"))
    if not path:
        return pd.DataFrame()
    df = pd.read_csv(path, dtype={"symbol": str})
    if "ranking_eligible" in df.columns:
        df = df[df["ranking_eligible"].fillna(False)].copy()
    keep = [
        col
        for col in [
            "symbol",
            "name",
            "sector",
            "composite_score",
            "value_score",
            "momentum_score",
            "quality_score",
            "flow_score",
            "news_score",
        ]
        if col in df.columns
    ]
    return df[keep].copy()


def load_analyst_frame(days: int = 30) -> pd.DataFrame:
    scored = load_scored_reports_cache(days=days, require_fresh=True)
    if scored.empty:
        scored = load_scored_reports_cache(days=days, require_fresh=False)
    if not scored.empty:
        return build_analyst_feature_frame(scored)
    raw_df = load_raw_reports(days=days)
    scored = score_reports(raw_df)
    return build_analyst_feature_frame(scored)


def load_flow_frame(days: int = 3, log_dir: str = SIGNAL_LOG_DIR) -> pd.DataFrame:
    snapshots, _ = collect_flow_snapshots(log_dir, days)
    if not snapshots:
        return pd.DataFrame()
    latest_by_symbol: dict[str, dict] = {}
    for item in snapshots:
        symbol = str(item.get("symbol") or "").zfill(6)
        if not symbol:
            continue
        if symbol not in latest_by_symbol or str(item.get("captured_at", "")) > str(latest_by_symbol[symbol].get("captured_at", "")):
            latest_by_symbol[symbol] = item
    rows = []
    for symbol, item in latest_by_symbol.items():
        top_brokers = ", ".join(row["name"] for row in (item.get("top_buy_brokers") or [])[:2])
        event_mix = ", ".join(
            f"{key}:{value}" for key, value in sorted((item.get("event_counts") or {}).items(), key=lambda kv: kv[1], reverse=True)[:3]
        )
        rows.append(
            {
                "symbol": symbol,
                "name": item.get("stock_name") or symbol,
                "flow_state_score": float(item.get("flow_state_score", 0) or 0),
                "cum_net_amt_mil": int(item.get("cum_net_amt_mil", 0) or 0),
                "cum_foreign_delta_qty": int(item.get("cum_foreign_delta_qty", 0) or 0),
                "flow_top_brokers": top_brokers,
                "flow_event_mix": event_mix,
            }
        )
    return pd.DataFrame(rows)


def load_event_frame(days: int = 45) -> pd.DataFrame:
    event_df = build_event_alpha_frame(days=days)
    if event_df is None or event_df.empty:
        return pd.DataFrame()
    keep = [
        col
        for col in [
            "symbol",
            "event_alpha_score",
            "event_expected_alpha_1d",
            "event_expected_alpha_3d",
            "event_expected_alpha_5d",
            "event_recent_count",
            "event_recent_positive_count",
            "event_recent_negative_count",
            "event_last_type",
            "event_last_bias",
            "event_last_days_ago",
            "event_best_strategy",
            "event_backtest_confidence",
            "event_valid_sample_size",
            "event_price_coverage_pct",
            "event_interpretation_label",
            "event_interpretation_note",
            "event_tactical_label",
            "event_tactical_note",
            "event_sector",
            "event_sector_valid_sample_size",
            "event_sector_price_coverage_pct",
            "event_sector_interpretation_label",
            "event_sector_tactical_label",
            "event_sector_interpretation_note",
            "event_sector_tactical_note",
            "event_sector_reaction_profile",
            "event_sector_interpretable_score",
            "event_interpretable_score",
            "event_win_rate_5d",
            "event_sample_size",
            "event_reaction_profile",
        ]
        if col in event_df.columns
    ]
    return event_df[keep].copy()


def load_valuation_frame() -> pd.DataFrame:
    latest = os.path.join(VALUATION_DIR, "fair_value_snapshot_latest.csv")
    path = latest if os.path.exists(latest) else _latest_path(os.path.join(VALUATION_DIR, "fair_value_snapshot_*.csv"))
    if not path:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, dtype={"symbol": str})
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    keep = [
        col
        for col in [
            "symbol",
            "current_price",
            "fair_value_bear",
            "fair_value_base",
            "fair_value_bull",
            "fair_value_gap_pct",
            "fair_value_confidence_score",
            "fair_value_status_label",
            "valuation_primary_method",
            "valuation_basis_label",
            "valuation_basis_period",
            "valuation_input_source",
            "valuation_multiple_current",
            "valuation_multiple_target",
            "valuation_multiple_unit",
            "operating_profit_yield_pct",
            "operating_margin_pct",
            "roe_current",
            "profitability_metric_label",
            "profitability_metric_value",
            "valuation_summary_paragraph",
            "valuation_method_detail",
            "valuation_formula_hint",
            "profitability_formula_hint",
            "valuation_anchor_mix",
            "valuation_peer_group",
            "valuation_reason_summary",
            "valuation_missing_inputs",
            "valuation_tier",
            "valuation_proxy_used",
            "valuation_coverage_reason",
            "valuation_driver",
            "sector",
            "sector_source",
            "valuation_family",
            "valuation_driver",
            "valuation_revision_revenue_pct",
            "valuation_revision_op_pct",
            "valuation_revision_net_pct",
            "valuation_analyst_target_upside_pct",
        ]
        if col in df.columns
    ]
    return df[keep].copy()
