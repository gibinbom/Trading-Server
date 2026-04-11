from __future__ import annotations

import glob
import os
import re
from datetime import timedelta

import numpy as np
import pandas as pd

try:
    from analyst_report_features import build_analyst_feature_frame
    from analyst_report_pipeline import load_scored_reports_cache
    from ml_models import NumpyMLPRegressor, RidgeRegressor
except Exception:
    from Disclosure.analyst_report_features import build_analyst_feature_frame
    from Disclosure.analyst_report_pipeline import load_scored_reports_cache
    from Disclosure.ml_models import NumpyMLPRegressor, RidgeRegressor


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CARD_DIR = os.path.join(ROOT_DIR, "cards")
FACTOR_DIR = os.path.join(ROOT_DIR, "factors", "snapshots")
PRICE_CACHE_DIR = os.path.join(ROOT_DIR, "factors", "cache", "prices")
FEATURE_COLS = [
    "composite_score", "value_score", "momentum_score", "quality_score", "flow_score", "news_score",
    "analyst_conviction_score", "analyst_novelty_score", "analyst_alpha_ret_5d",
    "analyst_revision_breadth_score", "analyst_peer_spillover_score",
    "flow_state_score", "flow_intraday_edge_score",
    "event_alpha_score", "event_expected_alpha_5d",
    "microstructure_score", "macro_sector_score", "macro_micro_interaction_score", "active_source_count",
]


def _parse_card_timestamp(path: str) -> pd.Timestamp | None:
    match = re.search(r"stock_cards_(\d{8})_(\d{6})\.csv$", os.path.basename(path))
    if not match:
        return None
    ts = pd.to_datetime("".join(match.groups()), format="%Y%m%d%H%M%S", errors="coerce")
    return None if pd.isna(ts) else ts


def _label_from_price(price_df: pd.DataFrame, asof_date: pd.Timestamp, horizon_days: int) -> float | None:
    if price_df.empty:
        return None
    dates = pd.to_datetime(price_df["Date"], errors="coerce")
    later_idx = price_df.index[dates >= asof_date.normalize()].tolist()
    if not later_idx or later_idx[0] + int(horizon_days) >= len(price_df):
        return None
    entry_idx = later_idx[0]
    exit_idx = entry_idx + int(horizon_days)
    entry_px = float(price_df.iloc[entry_idx]["Close"])
    exit_px = float(price_df.iloc[exit_idx]["Close"])
    return ((exit_px / entry_px) - 1.0) if entry_px else None


def _load_cached_price(symbol: str, price_cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    symbol = str(symbol).zfill(6)
    if symbol in price_cache:
        return price_cache[symbol]
    path = os.path.join(PRICE_CACHE_DIR, f"{symbol}.csv")
    if not os.path.exists(path):
        price_cache[symbol] = pd.DataFrame()
        return price_cache[symbol]
    try:
        df = pd.read_csv(path)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        price_cache[symbol] = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    except Exception:
        price_cache[symbol] = pd.DataFrame()
    return price_cache[symbol]


def _load_card_training_rows(horizon_days: int = 5, max_files: int = 160) -> pd.DataFrame:
    rows = []
    price_cache: dict[str, pd.DataFrame] = {}
    for path in sorted(glob.glob(os.path.join(CARD_DIR, "stock_cards_*.csv")))[-int(max_files):]:
        asof_ts = _parse_card_timestamp(path)
        if asof_ts is None or asof_ts > (pd.Timestamp.now() - timedelta(days=horizon_days + 1)):
            continue
        try:
            df = pd.read_csv(path, dtype={"symbol": str})
        except Exception:
            continue
        if df.empty:
            continue
        keep_cols = [col for col in ["symbol", "sector"] + FEATURE_COLS if col in df.columns]
        for row in df[keep_cols].to_dict("records"):
            symbol = str(row.get("symbol") or "").zfill(6)
            if not symbol:
                continue
            row["symbol"] = symbol
            row["asof_date"] = asof_ts.normalize()
            row["fwd_ret_5d"] = _label_from_price(_load_cached_price(symbol, price_cache), asof_ts, horizon_days)
            rows.append(row)
    return pd.DataFrame(rows)


def _load_analyst_training_rows(horizon_days: int = 5) -> pd.DataFrame:
    scored = load_scored_reports_cache(days=90, require_fresh=False)
    if scored.empty or "published_at" not in scored.columns:
        return pd.DataFrame()
    features = build_analyst_feature_frame(scored)
    latest_dates = (
        scored.assign(symbol=scored["symbol"].astype(str).str.zfill(6), published_at=pd.to_datetime(scored["published_at"], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul"))
        .sort_values("published_at")
        .groupby("symbol", dropna=False)["published_at"].last()
        .rename("asof_date")
        .reset_index()
    )
    df = features.merge(latest_dates, how="left", on="symbol")
    df["asof_date"] = pd.to_datetime(df["asof_date"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["asof_date"])
    df = df[df["asof_date"] <= (pd.Timestamp.now() - timedelta(days=horizon_days + 1))]
    if df.empty:
        return pd.DataFrame()
    rows = []
    price_cache: dict[str, pd.DataFrame] = {}
    keep_cols = [col for col in ["symbol", "sector"] + FEATURE_COLS if col in df.columns]
    for row in df[keep_cols + ["asof_date"]].to_dict("records"):
        symbol = str(row.get("symbol") or "").zfill(6)
        row["symbol"] = symbol
        row["fwd_ret_5d"] = _label_from_price(_load_cached_price(symbol, price_cache), pd.Timestamp(row["asof_date"]), horizon_days)
        rows.append(row)
    return pd.DataFrame(rows)


def _load_factor_training_rows(horizon_days: int = 5) -> pd.DataFrame:
    rows = []
    price_cache: dict[str, pd.DataFrame] = {}
    for path in sorted(glob.glob(os.path.join(FACTOR_DIR, "factor_snapshot_*.csv"))):
        match = re.search(r"factor_snapshot_(\d{8})\.csv$", os.path.basename(path))
        if not match:
            continue
        asof_date = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
        if pd.isna(asof_date) or asof_date > (pd.Timestamp.now() - timedelta(days=horizon_days + 1)):
            continue
        try:
            df = pd.read_csv(path, dtype={"symbol": str})
        except Exception:
            continue
        keep_cols = [col for col in ["symbol", "sector"] + FEATURE_COLS if col in df.columns]
        for row in df[keep_cols].to_dict("records"):
            symbol = str(row.get("symbol") or "").zfill(6)
            row["symbol"] = symbol
            row["asof_date"] = asof_date
            row["fwd_ret_5d"] = _label_from_price(_load_cached_price(symbol, price_cache), asof_date, horizon_days)
            rows.append(row)
    return pd.DataFrame(rows)


def _load_training_matrix(horizon_days: int = 5) -> pd.DataFrame:
    hist = pd.concat(
        [_load_card_training_rows(horizon_days=horizon_days), _load_analyst_training_rows(horizon_days=horizon_days), _load_factor_training_rows(horizon_days=horizon_days)],
        ignore_index=True,
    )
    if hist.empty:
        return hist
    hist = hist.dropna(subset=["fwd_ret_5d"]).copy()
    hist["feature_non_na"] = hist.reindex(columns=FEATURE_COLS).notna().sum(axis=1)
    hist = hist.sort_values(["symbol", "asof_date", "feature_non_na"]).drop_duplicates(["symbol", "asof_date"], keep="last")
    return hist.reset_index(drop=True)


def build_ml_recommendation_frame(current_df: pd.DataFrame, horizon_days: int = 5) -> tuple[pd.DataFrame, dict]:
    if current_df is None or current_df.empty:
        return pd.DataFrame(), {"status": "empty"}
    hist = _load_training_matrix(horizon_days=horizon_days)
    if hist.empty or len(hist) < 5:
        fallback = current_df[["symbol"]].copy()
        fallback["ml_pred_score"] = np.nan
        fallback["ml_sector_score"] = np.nan
        return fallback, {"status": "insufficient_history", "train_rows": int(len(hist))}

    X_train = hist.reindex(columns=FEATURE_COLS).apply(pd.to_numeric, errors="coerce")
    medians = X_train.median(numeric_only=True)
    X_train = X_train.fillna(medians).to_numpy(dtype=float)
    y_train = hist["fwd_ret_5d"].clip(-0.30, 0.30).to_numpy(dtype=float)
    X_now = current_df.reindex(columns=FEATURE_COLS).apply(pd.to_numeric, errors="coerce").fillna(medians).to_numpy(dtype=float)
    mean = X_train.mean(axis=0)
    std = X_train.std(axis=0)
    std[std == 0] = 1.0
    X_train = np.nan_to_num((X_train - mean) / std, nan=0.0, posinf=0.0, neginf=0.0)
    X_now = np.nan_to_num((X_now - mean) / std, nan=0.0, posinf=0.0, neginf=0.0)

    model_type = "ridge_fewshot" if len(hist) < 30 else "ridge"
    model = RidgeRegressor(alpha=2.0 if len(hist) < 30 else 1.2)
    if len(hist) >= 80:
        model = NumpyMLPRegressor(hidden_dim=16, lr=0.02, epochs=220, l2=2e-4)
        model_type = "numpy_mlp"
    model.fit(X_train, y_train)
    pred = model.predict(X_now)

    result = current_df[["symbol", "sector"]].copy()
    result["ml_pred_return_5d"] = pred
    result["ml_pred_score"] = pd.Series(pred).rank(pct=True, ascending=True).to_numpy(dtype=float)
    result["ml_sector_score"] = result.groupby("sector")["ml_pred_score"].transform("mean")
    return result[["symbol", "ml_pred_return_5d", "ml_pred_score", "ml_sector_score"]], {
        "status": "ok",
        "model_type": model_type,
        "train_rows": int(len(hist)),
        "feature_count": int(len(FEATURE_COLS)),
    }
