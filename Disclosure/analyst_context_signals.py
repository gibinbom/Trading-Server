from __future__ import annotations

import numpy as np
import pandas as pd

try:
    from sector_resolver import resolve_sector_map
except Exception:
    from Disclosure.sector_resolver import resolve_sector_map


def _weighted_mean(values: pd.Series, weights: pd.Series, default: float = 0.0) -> float:
    values = pd.to_numeric(values, errors="coerce")
    weights = pd.to_numeric(weights, errors="coerce")
    mask = values.notna() & weights.notna()
    if not mask.any():
        return float(default)
    return float(np.average(values[mask], weights=weights[mask]))


def build_analyst_context_frame(
    scored: pd.DataFrame,
    *,
    lookback_days: int = 21,
    peer_window_days: int = 14,
    half_life_days: float = 10.0,
) -> pd.DataFrame:
    columns = [
        "symbol",
        "analyst_revision_breadth_score",
        "analyst_revision_breadth_count",
        "analyst_positive_revision_ratio",
        "analyst_peer_spillover_score",
        "analyst_peer_alpha_5d",
        "analyst_peer_support_count",
    ]
    if scored is None or scored.empty:
        return pd.DataFrame(columns=columns)

    df = scored.copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["published_at"] = pd.to_datetime(df.get("published_at"), errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    df = df.dropna(subset=["published_at"]).copy()
    if df.empty:
        return pd.DataFrame(columns=columns)

    if "sector" not in df.columns:
        df["sector"] = "Unknown"
    else:
        df["sector"] = df["sector"].fillna("Unknown").astype(str)
    unresolved = df[df["sector"].isin(["", "Unknown"])]["symbol"].dropna().unique().tolist()
    if unresolved:
        sector_map = resolve_sector_map(unresolved, sleep_sec=0.0)
        df.loc[df["sector"].isin(["", "Unknown"]), "sector"] = (
            df.loc[df["sector"].isin(["", "Unknown"]), "symbol"].map(sector_map).fillna("Unknown")
        )

    latest_by_symbol = df.groupby("symbol", dropna=False)["published_at"].max().to_dict()
    recent_cutoff = df["published_at"].max() - pd.Timedelta(days=max(lookback_days, peer_window_days))
    recent_df = df[df["published_at"] >= recent_cutoff].copy()
    if recent_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for symbol, group in recent_df.groupby("symbol", dropna=False):
        symbol = str(symbol).zfill(6)
        anchor_ts = latest_by_symbol.get(symbol)
        if pd.isna(anchor_ts):
            continue
        symbol_window = group[group["published_at"] >= (anchor_ts - pd.Timedelta(days=lookback_days))].copy()
        if symbol_window.empty:
            continue

        age_days = (anchor_ts - symbol_window["published_at"]).dt.total_seconds().div(86400.0).clip(lower=0.0)
        weights = np.power(0.5, age_days / max(1.0, float(half_life_days)))

        rating_delta = pd.to_numeric(symbol_window.get("rating_delta_score"), errors="coerce").fillna(0.0)
        target_revision = pd.to_numeric(symbol_window.get("target_revision_pct"), errors="coerce").fillna(0.0)
        sentiment = pd.to_numeric(symbol_window.get("report_sentiment_score"), errors="coerce").fillna(0.0)
        positive_mask = (rating_delta > 0) | (target_revision > 0) | (sentiment > 0)
        negative_mask = (rating_delta < 0) | (target_revision < 0) | (sentiment < 0)

        unique_brokers = max(1, int(symbol_window["broker"].fillna("Unknown").astype(str).nunique()))
        positive_brokers = int(symbol_window.loc[positive_mask, "broker"].fillna("Unknown").astype(str).nunique())
        negative_brokers = int(symbol_window.loc[negative_mask, "broker"].fillna("Unknown").astype(str).nunique())
        positive_ratio = _weighted_mean(pd.Series(positive_mask.astype(float), index=symbol_window.index), pd.Series(weights, index=symbol_window.index))
        negative_ratio = _weighted_mean(pd.Series(negative_mask.astype(float), index=symbol_window.index), pd.Series(weights, index=symbol_window.index))
        breadth_score = np.clip(
            ((positive_brokers - negative_brokers) / unique_brokers) * 0.6 + (positive_ratio - negative_ratio) * 0.4,
            -1.0,
            1.0,
        )

        sector = str(symbol_window["sector"].iloc[-1] or "Unknown")
        peer_window = recent_df.iloc[0:0].copy() if sector in {"", "Unknown"} else recent_df[
            (recent_df["sector"].astype(str) == sector)
            & (recent_df["symbol"].astype(str) != symbol)
            & (recent_df["published_at"] >= (anchor_ts - pd.Timedelta(days=peer_window_days)))
            & (recent_df["published_at"] <= anchor_ts)
        ].copy()
        if peer_window.empty:
            peer_spillover = 0.0
            peer_alpha = 0.0
            peer_support = 0
        else:
            peer_age_days = (anchor_ts - peer_window["published_at"]).dt.total_seconds().div(86400.0).clip(lower=0.0)
            peer_weights = np.power(0.5, peer_age_days / max(1.0, float(half_life_days)))
            peer_sent = np.tanh(_weighted_mean(peer_window.get("report_sentiment_score", pd.Series(dtype=float)), peer_weights) / 6.0)
            peer_alpha = np.clip(_weighted_mean(peer_window.get("alpha_ret_5d", pd.Series(dtype=float)), peer_weights) / 5.0, -1.0, 1.0)
            peer_revision = np.clip(_weighted_mean(peer_window.get("target_revision_pct", pd.Series(dtype=float)), peer_weights) / 12.0, -1.0, 1.0)
            peer_positive_ratio = _weighted_mean(
                ((pd.to_numeric(peer_window.get("report_sentiment_score"), errors="coerce").fillna(0.0) > 0)
                 | (pd.to_numeric(peer_window.get("target_revision_pct"), errors="coerce").fillna(0.0) > 0)).astype(float),
                peer_weights,
            )
            peer_spillover = np.clip(
                peer_sent * 0.35 + peer_alpha * 0.35 + peer_revision * 0.15 + ((peer_positive_ratio * 2.0) - 1.0) * 0.15,
                -1.0,
                1.0,
            )
            peer_support = int(
                peer_window[
                    (pd.to_numeric(peer_window.get("report_sentiment_score"), errors="coerce").fillna(0.0) > 0)
                    | (pd.to_numeric(peer_window.get("target_revision_pct"), errors="coerce").fillna(0.0) > 0)
                ]["symbol"].astype(str).nunique()
            )

        rows.append(
            {
                "symbol": symbol,
                "analyst_revision_breadth_score": round(float(breadth_score), 4),
                "analyst_revision_breadth_count": int(positive_brokers - negative_brokers),
                "analyst_positive_revision_ratio": round(float(positive_ratio), 4),
                "analyst_peer_spillover_score": round(float(peer_spillover), 4),
                "analyst_peer_alpha_5d": round(float(peer_alpha) * 5.0, 4),
                "analyst_peer_support_count": int(peer_support),
            }
        )
    return pd.DataFrame(rows, columns=columns)
