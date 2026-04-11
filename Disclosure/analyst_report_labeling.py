from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

try:
    from analyst_report_benchmarks import load_index_history, load_market_map
    from analyst_sector_benchmark import attach_universe_sector_benchmarks
    from disclosure_event_pipeline import load_price_history
    from sector_resolver import resolve_sector_map
except Exception:
    from Disclosure.analyst_report_benchmarks import load_index_history, load_market_map
    from Disclosure.analyst_sector_benchmark import attach_universe_sector_benchmarks
    from Disclosure.disclosure_event_pipeline import load_price_history
    from Disclosure.sector_resolver import resolve_sector_map


DEFAULT_FORWARD_HORIZONS = (1, 5, 20)


def _entry_index(price_df: pd.DataFrame, published_at: pd.Timestamp) -> int | None:
    if pd.isna(published_at):
        return None
    event_ts = pd.Timestamp(published_at)
    if event_ts.tzinfo is not None:
        event_ts = event_ts.tz_convert("Asia/Seoul").tz_localize(None)
    event_date = event_ts.normalize()
    same_day = price_df.index[price_df["Date"].dt.normalize() == event_date].tolist()
    if not same_day:
        later = price_df.index[price_df["Date"].dt.normalize() > event_date].tolist()
        return later[0] if later else None
    idx = same_day[0]
    if (event_ts.hour, event_ts.minute) >= (15, 30):
        return idx + 1 if idx + 1 < len(price_df) else None
    return idx


def _forward_return(price_df: pd.DataFrame, entry_idx: int | None, horizon: int) -> float | None:
    if entry_idx is None or entry_idx >= len(price_df):
        return None
    exit_idx = entry_idx + int(horizon)
    if exit_idx >= len(price_df):
        return None
    entry_px = pd.to_numeric(pd.Series([price_df.iloc[entry_idx]["Close"]]), errors="coerce").iloc[0]
    exit_px = pd.to_numeric(pd.Series([price_df.iloc[exit_idx]["Close"]]), errors="coerce").iloc[0]
    if pd.isna(entry_px) or pd.isna(exit_px) or float(entry_px) <= 0:
        return None
    return ((float(exit_px) / float(entry_px)) - 1.0) * 100.0


def _weighted_score(out: pd.DataFrame, prefix: str, target_col: str) -> None:
    score = pd.Series(0.0, index=out.index, dtype=float)
    valid = pd.Series(0, index=out.index, dtype=int)
    for horizon, weight in {1: 0.20, 5: 0.35, 20: 0.45}.items():
        col = f"{prefix}_{horizon}d"
        if col in out.columns:
            score += out[col].fillna(0.0) * weight
            valid += out[col].notna().astype(int)
    out[target_col] = score.where(valid > 0, np.nan)


def attach_forward_return_labels(df: pd.DataFrame, horizons: Iterable[int] = DEFAULT_FORWARD_HORIZONS) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["symbol"] = out["symbol"].astype(str).str.zfill(6)
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    valid_published = out["published_at"].dropna()
    if valid_published.empty:
        return out

    out["market"] = out["symbol"].map(load_market_map(out["symbol"].tolist())).fillna(out.get("market", ""))
    out["market"] = out["market"].replace("", "KOSPI")
    if "sector" not in out.columns:
        out["sector"] = "Unknown"
    missing_sector = out["sector"].fillna("").astype(str).isin(["", "Unknown"])
    if missing_sector.any():
        sector_map = resolve_sector_map(out.loc[missing_sector, "symbol"].tolist(), sleep_sec=0.0)
        out.loc[missing_sector, "sector"] = out.loc[missing_sector, "symbol"].map(sector_map).fillna("Unknown")

    horizons = tuple(sorted({int(h) for h in horizons if int(h) > 0}))
    start_date = (valid_published.min().normalize() - pd.Timedelta(days=20)).strftime("%Y-%m-%d")
    end_date = (valid_published.max().normalize() + pd.Timedelta(days=max(horizons) + 30)).strftime("%Y-%m-%d")
    price_cache: dict[str, pd.DataFrame | None] = {}
    index_cache = {
        "KOSPI": load_index_history("KOSPI", start_date, end_date),
        "KOSDAQ": load_index_history("KOSDAQ", start_date, end_date),
    }
    hours = out["published_at"].dt.hour.fillna(0).astype(int)
    minutes = out["published_at"].dt.minute.fillna(0).astype(int)
    out["entry_date"] = pd.NaT
    out["entry_session"] = np.where((hours > 15) | ((hours == 15) & (minutes >= 30)), "AFTER", "SAME")
    for horizon in horizons:
        for prefix in ("fwd_ret", "market_ret", "market_alpha", "sector_alpha", "alpha_ret"):
            out[f"{prefix}_{horizon}d"] = np.nan

    for idx, row in out.iterrows():
        symbol = str(row.get("symbol") or "").zfill(6)
        if not symbol:
            continue
        if symbol not in price_cache:
            price_cache[symbol] = load_price_history(symbol, start_date, end_date)
        price_df = price_cache[symbol]
        if price_df is None or price_df.empty:
            continue
        entry_idx = _entry_index(price_df, row["published_at"])
        if entry_idx is None:
            continue
        out.at[idx, "entry_date"] = price_df.iloc[entry_idx]["Date"]
        for horizon in horizons:
            out.at[idx, f"fwd_ret_{horizon}d"] = _forward_return(price_df, entry_idx, horizon)
        market_key = "KOSDAQ" if str(row.get("market") or "").upper().startswith("KOSDAQ") else "KOSPI"
        market_df = index_cache.get(market_key)
        market_entry_idx = _entry_index(market_df, row["published_at"]) if market_df is not None and not market_df.empty else None
        for horizon in horizons:
            out.at[idx, f"market_ret_{horizon}d"] = _forward_return(market_df, market_entry_idx, horizon)

    out, price_cache = attach_universe_sector_benchmarks(
        out,
        start_date=start_date,
        end_date=end_date,
        horizons=horizons,
        price_cache=price_cache,
    )
    for horizon in horizons:
        fwd_col = f"fwd_ret_{horizon}d"
        if f"sector_ret_{horizon}d" not in out.columns:
            out[f"sector_ret_{horizon}d"] = np.nan
        out[f"market_alpha_{horizon}d"] = out[fwd_col] - out[f"market_ret_{horizon}d"]
        out[f"sector_alpha_{horizon}d"] = out[fwd_col] - out[f"sector_ret_{horizon}d"]
        combined = out[f"market_ret_{horizon}d"].where(
            out[f"sector_ret_{horizon}d"].isna(),
            out[f"market_ret_{horizon}d"] * 0.4 + out[f"sector_ret_{horizon}d"] * 0.6,
        )
        out[f"alpha_ret_{horizon}d"] = out[fwd_col] - combined

    _weighted_score(out, "fwd_ret", "realized_return_score")
    _weighted_score(out, "alpha_ret", "realized_alpha_score")
    return out


def apply_return_calibration(
    df: pd.DataFrame,
    feature_col: str = "report_sentiment_prelabel",
    label_col: str = "realized_alpha_score",
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    train = out[[feature_col, label_col]].replace([np.inf, -np.inf], np.nan).dropna()
    out["report_sentiment_score"] = out[feature_col].fillna(0.0)
    out["calibration_intercept"] = np.nan
    out["calibration_slope"] = np.nan
    out["calibration_corr"] = np.nan
    out["calibration_status"] = "insufficient_labels"
    if len(train) < 20 or float(train[feature_col].std()) <= 1e-8:
        return out
    slope, intercept = np.polyfit(train[feature_col], train[label_col], 1)
    corr = float(np.corrcoef(train[feature_col], train[label_col])[0, 1]) if len(train) >= 3 else 0.0
    pred = intercept + slope * out[feature_col].fillna(0.0)
    blend = min(0.65, max(0.25, abs(corr) if np.isfinite(corr) else 0.25))
    out["report_sentiment_score"] = out[feature_col].fillna(0.0) * (1.0 - blend) + pred * blend
    out["calibration_intercept"] = float(intercept)
    out["calibration_slope"] = float(slope)
    out["calibration_corr"] = float(corr)
    out["calibration_status"] = f"alpha_linear_blend_{blend:.2f}"
    return out
