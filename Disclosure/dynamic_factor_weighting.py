from __future__ import annotations

import glob
import math
import os
from datetime import timedelta
from typing import Any

import pandas as pd

try:
    from price_history_loader import load_price_history
except Exception:
    from Disclosure.price_history_loader import load_price_history


DEFAULT_BASE_WEIGHTS = {
    "value_score": 0.22,
    "momentum_score": 0.22,
    "quality_score": 0.18,
    "flow_score": 0.18,
    "liquidity_score": 0.10,
    "news_score": 0.10,
}


def _forward_return(price_df: pd.DataFrame, asof_date: pd.Timestamp, horizon: int) -> float | None:
    if price_df.empty:
        return None
    dates = pd.to_datetime(price_df["Date"], errors="coerce")
    later_idx = price_df.index[dates >= asof_date].tolist()
    if not later_idx:
        return None
    entry_idx = int(later_idx[0])
    exit_idx = entry_idx + int(horizon)
    if exit_idx >= len(price_df):
        return None
    entry_px = float(price_df.iloc[entry_idx]["Close"])
    exit_px = float(price_df.iloc[exit_idx]["Close"])
    if not entry_px:
        return None
    return (exit_px / entry_px) - 1.0


def _date_from_path(path: str) -> pd.Timestamp | None:
    stem = os.path.basename(path).replace("factor_snapshot_", "").replace(".csv", "")
    dt = pd.to_datetime(stem, format="%Y%m%d", errors="coerce")
    return None if pd.isna(dt) else dt


def compute_dynamic_factor_weights(
    snapshot_dir: str,
    price_cache_dir: str,
    *,
    base_weights: dict[str, float] | None = None,
    lookback_files: int = 40,
    horizon_days: int = 5,
    min_cross_section: int = 20,
) -> dict[str, Any]:
    base_weights = dict(base_weights or DEFAULT_BASE_WEIGHTS)
    snapshot_paths = sorted(glob.glob(os.path.join(snapshot_dir, "factor_snapshot_*.csv")))
    if not snapshot_paths:
        return {"status": "missing", "weights": base_weights, "base_weights": base_weights, "ic_history": {}}

    ic_history: dict[str, list[float]] = {key: [] for key in base_weights}
    for path in snapshot_paths[-int(max(lookback_files, 2)) :]:
        snapshot_date = _date_from_path(path)
        if snapshot_date is None or snapshot_date.date() > (pd.Timestamp.now().date() - timedelta(days=horizon_days + 1)):
            continue
        try:
            df = pd.read_csv(path, dtype={"symbol": str})
        except Exception:
            continue
        if "ranking_eligible" in df.columns:
            df = df[df["ranking_eligible"].fillna(False)].copy()
        if df.empty:
            continue
        target_rows = []
        for symbol in df["symbol"].astype(str).str.zfill(6).unique():
            price_df = load_price_history(symbol, cache_dir=price_cache_dir, lookback_days=520, sleep_sec=0.0)
            fwd = _forward_return(price_df, snapshot_date, horizon_days)
            if fwd is not None:
                target_rows.append({"symbol": symbol, "fwd_ret": fwd})
        if len(target_rows) < min_cross_section:
            continue
        merged = df.merge(pd.DataFrame(target_rows), how="inner", on="symbol")
        if len(merged) < min_cross_section:
            continue
        for factor_col in base_weights:
            if factor_col not in merged.columns:
                continue
            valid = merged[[factor_col, "fwd_ret"]].dropna()
            if len(valid) < min_cross_section:
                continue
            ic = valid[factor_col].rank(pct=True).corr(valid["fwd_ret"].rank(pct=True))
            if pd.notna(ic):
                ic_history[factor_col].append(float(ic))

    dyn_signal: dict[str, float] = {}
    for factor_col, hist in ic_history.items():
        if not hist:
            dyn_signal[factor_col] = 0.0
            continue
        ewma = pd.Series(hist).ewm(span=min(6, len(hist)), adjust=False).mean().iloc[-1]
        dyn_signal[factor_col] = max(0.0, float(ewma))

    total_signal = sum(dyn_signal.values())
    if total_signal <= 1e-9:
        return {"status": "fallback", "weights": base_weights, "base_weights": base_weights, "ic_history": ic_history}

    dynamic_weights = {key: value / total_signal for key, value in dyn_signal.items()}
    blended_weights = {}
    for key, base in base_weights.items():
        blended_weights[key] = round(base * 0.45 + dynamic_weights.get(key, 0.0) * 0.55, 6)
    norm = sum(blended_weights.values()) or 1.0
    blended_weights = {key: round(value / norm, 6) for key, value in blended_weights.items()}
    return {
        "status": "ok",
        "weights": blended_weights,
        "base_weights": base_weights,
        "dynamic_signal": {k: round(v, 6) for k, v in dyn_signal.items()},
        "ic_history": {k: [round(v, 6) for v in vals[-6:]] for k, vals in ic_history.items()},
        "horizon_days": int(horizon_days),
        "used_factor_count": int(sum(1 for values in ic_history.values() if values)),
    }
