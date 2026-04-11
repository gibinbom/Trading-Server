from __future__ import annotations

import numpy as np
import pandas as pd

try:
    from analyst_context_signals import build_analyst_context_frame
    from analyst_report_benchmarks import load_listing_snapshot
    from sector_resolver import resolve_sector_map
except Exception:
    from Disclosure.analyst_context_signals import build_analyst_context_frame
    from Disclosure.analyst_report_benchmarks import load_listing_snapshot
    from Disclosure.sector_resolver import resolve_sector_map


def enrich_prev_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    if "published_at" in out.columns:
        out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    else:
        out["published_at"] = pd.NaT
    out = out.sort_values(["symbol", "broker", "published_at"]).reset_index(drop=True)
    grouped = out.groupby(["symbol", "broker"], dropna=False)
    if "prev_target_price" not in out.columns:
        out["prev_target_price"] = grouped["target_price"].shift(1)
    else:
        out["prev_target_price"] = out["prev_target_price"].where(out["prev_target_price"].notna(), grouped["target_price"].shift(1))
    if "prev_rating" not in out.columns:
        out["prev_rating"] = grouped["rating"].shift(1)
    else:
        out["prev_rating"] = out["prev_rating"].where(out["prev_rating"].notna(), grouped["rating"].shift(1))
    return out


def _weighted_mean(series: pd.Series, weights: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce")
    mask = values.notna() & weights.notna()
    if not mask.any():
        return 0.0
    return float(np.average(values[mask], weights=weights[mask]))


def _current_close_map() -> dict[str, float]:
    listing = load_listing_snapshot()
    if listing.empty or "Code" not in listing.columns:
        return {}
    listing["Code"] = listing["Code"].astype(str).str.zfill(6)
    listing["Close"] = pd.to_numeric(listing.get("Close"), errors="coerce")
    return listing.dropna(subset=["Close"]).set_index("Code")["Close"].astype(float).to_dict()


def build_analyst_feature_frame(scored: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "symbol", "name", "sector", "analyst_conviction_score", "analyst_avg_score", "analyst_report_count",
        "analyst_broker_diversity", "analyst_target_revision_pct", "analyst_novelty_score", "analyst_fwd_ret_5d",
        "analyst_fwd_ret_20d", "analyst_alpha_ret_5d", "analyst_alpha_ret_20d", "analyst_bias_adjustment",
        "analyst_parse_quality_score", "analyst_pdf_coverage", "analyst_target_upside_pct",
        "analyst_target_dispersion_pct", "analyst_agreement_score", "analyst_recency_score", "analyst_latest_title",
        "analyst_revision_breadth_score", "analyst_revision_breadth_count", "analyst_positive_revision_ratio",
        "analyst_peer_spillover_score", "analyst_peer_alpha_5d", "analyst_peer_support_count",
    ]
    if scored.empty:
        return pd.DataFrame(columns=columns)

    scored = scored.copy()
    scored["published_at"] = pd.to_datetime(scored["published_at"], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    now = pd.Timestamp.now(tz="Asia/Seoul")
    latest_rows = scored.sort_values("published_at").groupby("symbol", dropna=False).tail(1).copy()
    unresolved = latest_rows[latest_rows.get("sector", pd.Series(index=latest_rows.index, dtype=object)).fillna("").astype(str).isin(["", "Unknown"])]["symbol"].astype(str).str.zfill(6).unique().tolist()
    sector_map = resolve_sector_map(unresolved, sleep_sec=0.0) if unresolved else {}
    close_map = _current_close_map()

    rows = []
    for symbol, group in scored.groupby("symbol", dropna=False):
        symbol = str(symbol).zfill(6)
        if not symbol:
            continue
        latest = group.sort_values("published_at").iloc[-1]
        age_days = (now - group["published_at"]).dt.total_seconds().div(86400.0).clip(lower=0.0).fillna(30.0)
        decay = np.power(0.5, age_days / 14.0)
        targets = pd.to_numeric(group.get("target_price"), errors="coerce").dropna()
        mean_target = float(targets.mean()) if not targets.empty else 0.0
        current_close = float(close_map.get(symbol, np.nan)) if symbol in close_map else np.nan
        target_upside = ((mean_target / current_close) - 1.0) * 100.0 if mean_target > 0 and np.isfinite(current_close) and current_close > 0 else 0.0
        target_dispersion = float((targets.std(ddof=0) / abs(mean_target)) * 100.0) if len(targets) >= 2 and np.isfinite(mean_target) and abs(mean_target) > 1e-9 else 0.0
        ratings = group.get("rating", pd.Series(dtype=object)).fillna("").astype(str).str.lower().str.strip()
        rating_mode_share = float(ratings.value_counts(normalize=True).iloc[0]) if not ratings.empty and ratings.ne("").any() else 0.5
        agreement_score = float(np.clip((rating_mode_share * 0.6) + (max(0.0, 1.0 - target_dispersion / 40.0) * 0.4), 0.0, 1.0))
        freshness_score = float(np.clip(np.power(0.5, float(age_days.min()) / 14.0), 0.0, 1.0)) if not age_days.empty else 0.0
        weighted_score = _weighted_mean(group["report_sentiment_score"], decay)
        weighted_novelty = _weighted_mean(group.get("novelty_score", pd.Series(dtype=float)), decay)
        weighted_alpha_5d = _weighted_mean(group.get("alpha_ret_5d", pd.Series(dtype=float)), decay)
        raw_sector = str(latest.get("sector") or "").strip()
        sector = raw_sector if raw_sector and raw_sector != "Unknown" else sector_map.get(symbol) or "Unknown"
        conviction = weighted_score + min(4.0, group["broker"].nunique() * 0.5) + min(3.0, len(group) * 0.2)
        conviction += min(1.5, weighted_novelty * 1.5) + max(-2.0, min(2.0, weighted_alpha_5d / 2.5))
        conviction += max(-1.0, min(2.0, target_upside / 20.0)) + (agreement_score - 0.5) * 1.5 + freshness_score * 1.0
        rows.append(
            {
                "symbol": symbol,
                "name": latest.get("name") or latest.get("stock_name") or symbol,
                "sector": sector,
                "analyst_conviction_score": round(conviction, 4),
                "analyst_avg_score": round(weighted_score, 4),
                "analyst_report_count": int(len(group)),
                "analyst_broker_diversity": int(group["broker"].nunique()),
                "analyst_target_revision_pct": round(_weighted_mean(group.get("target_revision_pct", pd.Series(dtype=float)), decay), 4),
                "analyst_novelty_score": round(weighted_novelty, 4),
                "analyst_fwd_ret_5d": round(_weighted_mean(group.get("fwd_ret_5d", pd.Series(dtype=float)), decay), 4),
                "analyst_fwd_ret_20d": round(_weighted_mean(group.get("fwd_ret_20d", pd.Series(dtype=float)), decay), 4),
                "analyst_alpha_ret_5d": round(weighted_alpha_5d, 4),
                "analyst_alpha_ret_20d": round(_weighted_mean(group.get("alpha_ret_20d", pd.Series(dtype=float)), decay), 4),
                "analyst_bias_adjustment": round(_weighted_mean(group.get("broker_bias_adjustment", pd.Series(dtype=float)), decay), 4),
                "analyst_parse_quality_score": round(_weighted_mean(group.get("report_parse_quality_score", pd.Series(dtype=float)), decay), 4),
                "analyst_pdf_coverage": round(float(group.get("pdf_text_length", pd.Series(dtype=float)).fillna(0).gt(0).mean()) if "pdf_text_length" in group.columns else 0.0, 4),
                "analyst_target_upside_pct": round(float(target_upside), 4),
                "analyst_target_dispersion_pct": round(float(target_dispersion), 4),
                "analyst_agreement_score": round(agreement_score, 4),
                "analyst_recency_score": round(freshness_score, 4),
                "analyst_latest_title": str(latest.get("title") or ""),
            }
        )
    out = pd.DataFrame(rows)
    context_df = build_analyst_context_frame(scored)
    if not context_df.empty:
        out = out.merge(context_df, how="left", on="symbol")
    for col, default in [
        ("analyst_revision_breadth_score", 0.0),
        ("analyst_revision_breadth_count", 0),
        ("analyst_positive_revision_ratio", 0.0),
        ("analyst_peer_spillover_score", 0.0),
        ("analyst_peer_alpha_5d", 0.0),
        ("analyst_peer_support_count", 0),
    ]:
        if col not in out.columns:
            out[col] = default
        else:
            out[col] = out[col].fillna(default)
    out["analyst_conviction_score"] = (
        pd.to_numeric(out["analyst_conviction_score"], errors="coerce").fillna(0.0)
        + pd.to_numeric(out["analyst_revision_breadth_score"], errors="coerce").fillna(0.0) * 1.6
        + pd.to_numeric(out["analyst_peer_spillover_score"], errors="coerce").fillna(0.0) * 1.2
        + pd.to_numeric(out["analyst_peer_support_count"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=6.0) * 0.08
    ).round(4)
    return out.reindex(columns=columns)
