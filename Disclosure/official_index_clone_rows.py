from __future__ import annotations

from typing import Any

try:
    from official_index_clone_support import clean_text, rank_limit, safe_float, safe_int
except Exception:  # pragma: no cover - package import fallback
    from Disclosure.official_index_clone_support import clean_text, rank_limit, safe_float, safe_int


def select_symbols(universe_rows: list[dict[str, Any]], bucket_targets: dict[str, int], cutoff: int, keep_ratio: float) -> tuple[set[str], dict[str, str]]:
    selected: set[str] = set()
    selected_paths: dict[str, str] = {}
    symbol_rows = {str(row["symbol"]): row for row in universe_rows}
    for bucket, target in bucket_targets.items():
        bucket_symbols = [symbol for symbol, row in symbol_rows.items() if str(row.get("official_bucket")) == bucket and bool(row.get("is_eligible")) and bool(row.get("liquidity_gate_pass"))]
        for symbol in bucket_symbols[:target]:
            selected.add(symbol)
            selected_paths[symbol] = "bucket_quota"
    if len(selected) < cutoff:
        for symbol, row in symbol_rows.items():
            if len(selected) >= cutoff:
                break
            if symbol in selected or not bool(row.get("is_eligible")):
                continue
            if not (bool(row.get("liquidity_gate_pass")) or bool(row.get("is_current_member"))):
                continue
            selected.add(symbol)
            selected_paths[symbol] = "global_fill"
    for symbol, row in symbol_rows.items():
        if symbol in selected or not bool(row.get("is_current_member")):
            continue
        bucket_target = bucket_targets.get(str(row.get("official_bucket")), 0)
        keep_limit = max(rank_limit(bucket_target, keep_ratio), rank_limit(cutoff, keep_ratio))
        if safe_int(row.get("bucket_rank"), 999999) <= keep_limit or safe_int(row.get("predicted_rank"), 999999) <= keep_limit:
            selected.add(symbol)
            selected_paths[symbol] = "buffer_keep"
    return selected, selected_paths


def build_rows(
    universe_rows: list[dict[str, Any]],
    bucket_targets: dict[str, int],
    selected: set[str],
    selected_paths: dict[str, str],
    cutoff: int,
    entry_ratio: float,
    keep_ratio: float,
    review_date: str,
    effective_date: str,
    index_name: str,
    methodology_version: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in universe_rows:
        symbol = str(row["symbol"])
        bucket_target = bucket_targets.get(str(row.get("official_bucket")), 0)
        entry_limit = max(rank_limit(bucket_target, entry_ratio), rank_limit(cutoff, entry_ratio))
        keep_limit = max(rank_limit(bucket_target, keep_ratio), rank_limit(cutoff, keep_ratio))
        current_member = bool(row.get("is_current_member"))
        is_selected = symbol in selected
        bucket_rank = safe_int(row.get("bucket_rank"), 999999)
        predicted_rank = safe_int(row.get("predicted_rank"), 999999)
        if not current_member and is_selected and min(bucket_rank, predicted_rank) <= entry_limit:
            state = "likely_add"
        elif not current_member and (is_selected or min(bucket_rank, predicted_rank) <= keep_limit):
            state = "watch_add"
        elif current_member and not is_selected and min(bucket_rank, predicted_rank) > keep_limit:
            state = "likely_drop"
        else:
            state = "stable"
        rows.append(
            {
                "symbol": symbol,
                "name": clean_text(row.get("name")),
                "market": clean_text(row.get("market")),
                "sector": clean_text(row.get("official_sector")) or "미분류",
                "sector_bucket": clean_text(row.get("official_bucket")) or "other",
                "index_name": index_name,
                "current_member": current_member,
                "current_member_source": "official_input",
                "predicted_rank": predicted_rank,
                "bucket_rank": bucket_rank,
                "bucket_target_count": bucket_target,
                "distance_to_cut": min(predicted_rank - cutoff, bucket_rank - bucket_target if bucket_target > 0 else predicted_rank - cutoff),
                "state": state,
                "confidence": 0.9 if state in {"likely_add", "likely_drop"} else 0.7,
                "as_of": review_date,
                "effective_on": effective_date,
                "market_cap_krw": int(round(safe_float(row.get("avg_ffmc_1y_krw"), 0.0))),
                "avg_amount_60d_krw": int(round(safe_float(row.get("avg_trading_value_1y_krw"), 0.0))),
                "market_cap_rank": safe_int(row.get("market_cap_rank_all")),
                "liquidity_gate_pass": bool(row.get("liquidity_gate_pass")),
                "entry_rank_limit": entry_limit,
                "keep_rank_limit": keep_limit,
                "special_largecap": bool(row.get("special_largecap")),
                "selection_path": selected_paths.get(symbol, ""),
                "rank_method": methodology_version,
            }
        )
    return rows
