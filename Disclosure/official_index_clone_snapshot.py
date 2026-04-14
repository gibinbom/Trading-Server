from __future__ import annotations

import math
from typing import Any

import pandas as pd

try:
    from official_index_clone_rows import build_rows, select_symbols
    from official_index_clone_support import (
        OFFICIAL_METHODOLOGY_VERSION,
        OfficialCloneBundle,
        OfficialCloneInputError,
        clean_text,
        norm_symbol,
        rank_limit,
        safe_float,
        safe_int,
        select_latest_reviews,
    )
except Exception:  # pragma: no cover - package import fallback
    from Disclosure.official_index_clone_rows import build_rows, select_symbols
    from Disclosure.official_index_clone_support import (
        OFFICIAL_METHODOLOGY_VERSION,
        OfficialCloneBundle,
        OfficialCloneInputError,
        clean_text,
        norm_symbol,
        rank_limit,
        safe_float,
        safe_int,
        select_latest_reviews,
    )


def build_official_index_rebalance_snapshot(bundle: OfficialCloneBundle) -> dict[str, Any]:
    review_rows = select_latest_reviews(bundle)
    latest_review_date = str(review_rows["review_date"].iloc[0]).strip()
    latest_effective_date = str(review_rows["effective_date"].iloc[0]).strip()
    rows: list[dict[str, Any]] = []
    indexes: list[dict[str, Any]] = []
    for review in review_rows.to_dict(orient="records"):
        index_name = clean_text(review.get("index_name"))
        if index_name not in {"KS200", "KQ150"}:
            continue
        cutoff = safe_int(review.get("cutoff"))
        entry_ratio = safe_float(review.get("entry_ratio"), 0.9)
        keep_ratio = safe_float(review.get("keep_ratio"), 1.1)
        liquidity_coverage = safe_float(review.get("liquidity_coverage"), 0.85)
        special_largecap_rank = safe_int(review.get("special_largecap_rank"), 50)
        universe = _prepare_universe(bundle, latest_review_date, index_name, liquidity_coverage, special_largecap_rank)
        target_rows = _load_bucket_targets(bundle, latest_review_date, index_name)
        bucket_targets = {
            clean_text(row.get("official_bucket")): safe_int(row.get("target_count"))
            for row in target_rows.to_dict(orient="records")
            if clean_text(row.get("official_bucket"))
        }
        universe_rows = universe.to_dict(orient="records")
        selected, selected_paths = select_symbols(universe_rows, bucket_targets, cutoff, keep_ratio)
        rows.extend(
            build_rows(
                universe_rows,
                bucket_targets,
                selected,
                selected_paths,
                cutoff,
                entry_ratio,
                keep_ratio,
                latest_review_date,
                latest_effective_date,
                index_name,
                OFFICIAL_METHODOLOGY_VERSION,
            )
        )
        indexes.append(
            {
                "index_name": index_name,
                "cutoff": cutoff,
                "member_source": "official_input",
                "target_source": "official_input",
                "row_count": sum(1 for row in rows if row.get("index_name") == index_name),
                "bucket_targets": dict(sorted(bucket_targets.items())),
                "methodology_version": OFFICIAL_METHODOLOGY_VERSION,
                "quota_strategy": "official_input",
                "entry_ratio": entry_ratio,
                "keep_ratio": keep_ratio,
                "liquidity_coverage": liquidity_coverage,
                "source_mode": "official",
                "review_date": latest_review_date,
                "effective_date": latest_effective_date,
            }
        )
    rows.sort(key=lambda item: (clean_text(item.get("index_name")), safe_int(item.get("predicted_rank"))))
    return {
        "generated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
        "as_of": latest_review_date,
        "status": "live",
        "stale_since": None,
        "source_error": "",
        "default_buffer": 0,
        "methodology_mode": "official",
        "input_dir": str(bundle.input_dir),
        "rows": rows,
        "indexes": indexes,
    }


def _prepare_universe(bundle: OfficialCloneBundle, review_date: str, index_name: str, liquidity_coverage: float, special_largecap_rank: int) -> pd.DataFrame:
    universe = bundle.universe.copy()
    universe = universe[(universe["review_date"].astype(str).str.strip() == review_date) & (universe["index_name"].astype(str).str.strip() == index_name)].copy()
    if universe.empty:
        raise OfficialCloneInputError(f"no universe rows for {index_name} review_date={review_date}")
    universe["symbol"] = universe["symbol"].map(norm_symbol)
    for numeric_column, default in (
        ("avg_ffmc_1y_krw", 0.0),
        ("avg_trading_value_1y_krw", 0.0),
        ("free_float_ratio", 0.0),
    ):
        universe[numeric_column] = universe[numeric_column].map(lambda value, d=default: safe_float(value, d))
    for numeric_column, default in (("market_cap_rank_all", 999999), ("listing_age_days", 0)):
        universe[numeric_column] = universe[numeric_column].map(lambda value, d=default: safe_int(value, d))
    universe["is_current_member"] = universe["is_current_member"].astype(str).str.strip().isin({"1", "true", "True", "Y", "y"})
    universe["is_eligible"] = universe["is_eligible"].astype(str).str.strip().isin({"1", "true", "True", "Y", "y"})
    universe["official_bucket"] = universe["official_bucket"].astype(str).str.strip()
    universe["official_sector"] = universe["official_sector"].astype(str).str.strip()
    universe = universe[universe["symbol"] != ""].copy()
    universe["liquidity_rank_in_bucket"] = universe.groupby("official_bucket")["avg_trading_value_1y_krw"].rank(method="first", ascending=False)
    universe["bucket_rank"] = universe.groupby("official_bucket")["avg_ffmc_1y_krw"].rank(method="first", ascending=False)
    universe = universe.sort_values(["avg_ffmc_1y_krw", "avg_trading_value_1y_krw"], ascending=[False, False]).reset_index(drop=True)
    universe["predicted_rank"] = universe.index + 1
    universe["special_largecap"] = universe["market_cap_rank_all"] <= max(1, special_largecap_rank)
    universe["bucket_size"] = universe.groupby("official_bucket")["symbol"].transform("count")
    universe["liquidity_limit"] = universe["bucket_size"].map(lambda size: max(1, int(math.ceil(max(1, int(size)) * liquidity_coverage))))
    universe["liquidity_gate_pass"] = (universe["liquidity_rank_in_bucket"] <= universe["liquidity_limit"]) | universe["special_largecap"]
    return universe
def _load_bucket_targets(bundle: OfficialCloneBundle, review_date: str, index_name: str) -> pd.DataFrame:
    target_rows = bundle.bucket_targets.copy()
    target_rows = target_rows[(target_rows["review_date"].astype(str).str.strip() == review_date) & (target_rows["index_name"].astype(str).str.strip() == index_name)].copy()
    if target_rows.empty:
        raise OfficialCloneInputError(f"no bucket_targets rows for {index_name} review_date={review_date}")
    return target_rows
