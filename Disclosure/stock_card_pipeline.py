from __future__ import annotations

import os

import numpy as np
import pandas as pd

try:
    from event_alpha_features import build_event_alpha_frame
    from flow_microstructure_features import build_flow_microstructure_frame
    from flow_intraday_backtest import build_intraday_feature_frame
    from macro_regime_features import build_macro_interaction_frame
    from sector_resolver import resolve_sector_map
    from stock_card_sources import SIGNAL_LOG_DIR, load_analyst_frame, load_event_frame, load_factor_frame, load_flow_frame, load_valuation_frame
    from stock_recommender_ml import build_ml_recommendation_frame
except Exception:
    from Disclosure.event_alpha_features import build_event_alpha_frame
    from Disclosure.flow_microstructure_features import build_flow_microstructure_frame
    from Disclosure.flow_intraday_backtest import build_intraday_feature_frame
    from Disclosure.macro_regime_features import build_macro_interaction_frame
    from Disclosure.sector_resolver import resolve_sector_map
    from Disclosure.stock_card_sources import SIGNAL_LOG_DIR, load_analyst_frame, load_event_frame, load_factor_frame, load_flow_frame, load_valuation_frame
    from Disclosure.stock_recommender_ml import build_ml_recommendation_frame


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CARD_SECTOR_RESOLVE_LIMIT = max(0, int(os.getenv("STOCK_CARD_SECTOR_RESOLVE_LIMIT", "40")))


def _pct_rank(series: pd.Series) -> pd.Series:
    valid = pd.to_numeric(series, errors="coerce")
    return valid.rank(pct=True, ascending=True)


def _coalesce_duplicate_columns(base: pd.DataFrame) -> pd.DataFrame:
    for col in list(base.columns):
        if not col.endswith("_dup"):
            continue
        src = col[:-4]
        if src in base.columns:
            if src == "sector":
                src_series = base[src].astype(str)
                base[src] = base[src].where(src_series.notna() & (src_series != "") & (src_series != "Unknown"), base[col])
            elif src == "name":
                src_series = base[src].astype(str)
                base[src] = base[src].where(src_series.notna() & (src_series != ""), base[col])
            else:
                base[src] = base[src].fillna(base[col])
        else:
            base[src] = base[col]
        base = base.drop(columns=[col])
    return base


def _fill_defaults(base: pd.DataFrame) -> pd.DataFrame:
    for col, default in [
        ("flow_top_brokers", ""),
        ("flow_event_mix", ""),
        ("macro_sector_score", 0.0),
        ("macro_regime_score", 0.0),
        ("macro_micro_interaction_score", np.nan),
        ("micro_ofi", np.nan),
        ("micro_foreign_pressure", np.nan),
        ("micro_broker_concentration", np.nan),
        ("micro_queue_imbalance", np.nan),
        ("micro_absorption_score", np.nan),
        ("microstructure_score", np.nan),
        ("ml_pred_return_5d", np.nan),
        ("ml_pred_score", np.nan),
        ("ml_sector_score", np.nan),
        ("ml_model_type", ""),
        ("ml_train_rows", 0),
        ("flow_intraday_samples", 0),
        ("flow_intraday_avg_5m", np.nan),
        ("flow_intraday_avg_15m", np.nan),
        ("flow_intraday_avg_30m", np.nan),
        ("flow_intraday_avg_60m", np.nan),
        ("flow_intraday_last_event", ""),
        ("flow_intraday_last_captured_at", ""),
        ("event_alpha_score", np.nan),
        ("event_expected_alpha_1d", np.nan),
        ("event_expected_alpha_3d", np.nan),
        ("event_expected_alpha_5d", np.nan),
        ("event_recent_count", 0),
        ("event_recent_positive_count", 0),
        ("event_recent_negative_count", 0),
        ("event_last_type", ""),
        ("event_last_bias", ""),
        ("event_last_days_ago", np.nan),
        ("event_best_strategy", ""),
        ("event_backtest_confidence", ""),
        ("event_valid_sample_size", 0),
        ("event_price_coverage_pct", 0.0),
        ("event_interpretation_label", ""),
        ("event_interpretation_note", ""),
        ("event_tactical_label", ""),
        ("event_tactical_note", ""),
        ("event_sector", ""),
        ("event_sector_valid_sample_size", 0),
        ("event_sector_price_coverage_pct", 0.0),
        ("event_sector_interpretation_label", ""),
        ("event_sector_tactical_label", ""),
        ("event_sector_interpretation_note", ""),
        ("event_sector_tactical_note", ""),
        ("event_sector_reaction_profile", ""),
        ("event_sector_interpretable_score", 0.0),
        ("event_interpretable_score", 0.0),
        ("event_win_rate_5d", np.nan),
        ("event_sample_size", 0),
        ("event_reaction_profile", ""),
        ("current_price", np.nan),
        ("fair_value_bear", np.nan),
        ("fair_value_base", np.nan),
        ("fair_value_bull", np.nan),
        ("fair_value_gap_pct", np.nan),
        ("fair_value_confidence_score", 0.0),
        ("fair_value_status_label", ""),
        ("valuation_primary_method", ""),
        ("valuation_basis_label", ""),
        ("valuation_basis_period", ""),
        ("valuation_input_source", ""),
        ("valuation_multiple_current", np.nan),
        ("valuation_multiple_target", np.nan),
        ("valuation_multiple_unit", ""),
        ("operating_profit_yield_pct", np.nan),
        ("operating_margin_pct", np.nan),
        ("roe_current", np.nan),
        ("profitability_metric_label", ""),
        ("profitability_metric_value", np.nan),
        ("valuation_summary_paragraph", ""),
        ("valuation_method_detail", ""),
        ("valuation_formula_hint", ""),
        ("profitability_formula_hint", ""),
        ("valuation_anchor_mix", ""),
        ("valuation_peer_group", ""),
        ("valuation_reason_summary", ""),
        ("valuation_missing_inputs", ""),
        ("valuation_family", ""),
        ("valuation_driver", ""),
        ("valuation_revision_revenue_pct", np.nan),
        ("valuation_revision_op_pct", np.nan),
        ("valuation_revision_net_pct", np.nan),
        ("valuation_analyst_target_upside_pct", np.nan),
    ]:
        if col not in base.columns:
            base[col] = default
        else:
            base[col] = base[col].fillna(default)
    return base


def build_stock_card_frame(analyst_days: int = 30, flow_days: int = 3) -> pd.DataFrame:
    frames = [
        load_factor_frame(),
        load_valuation_frame(),
        load_analyst_frame(days=analyst_days),
        load_flow_frame(days=flow_days),
        load_event_frame(days=max(30, flow_days * 10)),
        build_flow_microstructure_frame(days=flow_days, log_dir=SIGNAL_LOG_DIR),
        build_intraday_feature_frame(days=flow_days, log_dir=SIGNAL_LOG_DIR),
    ]
    frames = [df for df in frames if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame()

    base = frames[0].copy()
    for df in frames[1:]:
        base = base.merge(df, how="outer", on="symbol", suffixes=("", "_dup"))
        base = _coalesce_duplicate_columns(base)

    base["name"] = base.get("name", "").fillna("")
    base["sector"] = base.get("sector", "Unknown").fillna("Unknown")
    unresolved = base["sector"].astype(str).isin(["", "Unknown"])
    if unresolved.any():
        resolved = resolve_sector_map(
            base.loc[unresolved, "symbol"].tolist(),
            sleep_sec=0.0,
            max_fetch=CARD_SECTOR_RESOLVE_LIMIT,
        )
        base.loc[unresolved, "sector"] = base.loc[unresolved, "symbol"].map(resolved).fillna("Unknown")

    macro_df = build_macro_interaction_frame(base)
    if not macro_df.empty:
        base = base.merge(macro_df, how="left", on=["symbol", "sector"])

    ml_df, ml_meta = build_ml_recommendation_frame(base)
    if not ml_df.empty:
        base = base.merge(ml_df, how="left", on="symbol")
    base["ml_model_type"] = ml_meta.get("model_type", "") if isinstance(ml_meta, dict) else ""
    base["ml_train_rows"] = ml_meta.get("train_rows", 0) if isinstance(ml_meta, dict) else 0
    base = _fill_defaults(base)

    for source_col, rank_col in [
        ("composite_score", "factor_rank_score"),
        ("analyst_conviction_score", "analyst_rank_score"),
        ("flow_state_score", "flow_rank_score"),
        ("flow_intraday_edge_score", "flow_intraday_rank_score"),
        ("microstructure_score", "micro_rank_score"),
        ("macro_micro_interaction_score", "macro_rank_score"),
        ("ml_pred_score", "ml_rank_score"),
        ("event_alpha_score", "event_rank_score"),
    ]:
        if source_col not in base.columns:
            base[source_col] = np.nan
        base[rank_col] = _pct_rank(base[source_col])

    weights = {
        "factor_rank_score": 0.25,
        "analyst_rank_score": 0.18,
        "flow_rank_score": 0.09,
        "flow_intraday_rank_score": 0.07,
        "micro_rank_score": 0.10,
        "macro_rank_score": 0.09,
        "ml_rank_score": 0.12,
        "event_rank_score": 0.10,
    }
    numerator = pd.Series(0.0, index=base.index)
    denominator = pd.Series(0.0, index=base.index)
    active_source_count = pd.Series(0, index=base.index, dtype=float)
    for col, weight in weights.items():
        valid = base[col].notna()
        numerator += base[col].fillna(0.0) * weight
        denominator += valid.astype(float) * weight
        active_source_count += valid.astype(float)
    base["active_source_count"] = active_source_count.astype(int)
    blended_score = numerator.div(denominator.replace(0, np.nan))
    coverage_bonus = base["active_source_count"].div(len(weights))
    base["card_score"] = blended_score * 0.85 + coverage_bonus * 0.15
    return base.sort_values(
        ["active_source_count", "card_score", "event_alpha_score", "ml_pred_score", "flow_intraday_edge_score", "composite_score", "analyst_conviction_score"],
        ascending=[False, False, False, False, False, False, False],
    ).reset_index(drop=True)
