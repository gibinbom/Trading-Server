from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

try:
    from analyst_report_benchmarks import build_sector_peer_map, load_listing_snapshot
    from disclosure_event_pipeline import load_price_history
except Exception:
    from Disclosure.analyst_report_benchmarks import build_sector_peer_map, load_listing_snapshot
    from Disclosure.disclosure_event_pipeline import load_price_history


def _date_entry_index(price_df: pd.DataFrame, trade_date: pd.Timestamp) -> int | None:
    same_day = price_df.index[price_df["Date"].dt.normalize() == pd.Timestamp(trade_date).normalize()].tolist()
    if same_day:
        return same_day[0]
    later = price_df.index[price_df["Date"].dt.normalize() > pd.Timestamp(trade_date).normalize()].tolist()
    return later[0] if later else None


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


def attach_universe_sector_benchmarks(
    df: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    horizons: tuple[int, ...],
    price_cache: dict[str, pd.DataFrame | None] | None = None,
    warm_limit_per_sector: int = 12,
    warm_limit_total: int = 72,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame | None]]:
    if df.empty:
        return df.copy(), price_cache or {}
    out = df.copy()
    shared_cache = price_cache or {}
    symbol_sector_key, symbol_sector_name, sector_members = build_sector_peer_map(out["symbol"].astype(str).str.zfill(6).tolist())
    out["sector_key"] = out["symbol"].map(symbol_sector_key).fillna(out.get("sector_key", ""))
    out["benchmark_sector_name"] = out["symbol"].map(symbol_sector_name).fillna(out.get("sector", ""))
    unique_dates = sorted(pd.to_datetime(out["entry_date"], errors="coerce").dropna().dt.normalize().unique())
    if not unique_dates:
        return out, shared_cache
    shared_cache, _ = warm_sector_peer_price_cache(
        out["symbol"].astype(str).str.zfill(6).tolist(),
        start_date=start_date,
        end_date=end_date,
        price_cache=shared_cache,
        warm_limit_per_sector=warm_limit_per_sector,
        warm_limit_total=warm_limit_total,
        sector_payload=(symbol_sector_key, symbol_sector_name, sector_members),
    )

    bench_rows: list[dict] = []
    for sector_key, peers in sector_members.items():
        if not peers:
            continue
        valid_peers = [symbol for symbol in peers if shared_cache.get(symbol) is not None and not shared_cache.get(symbol).empty]
        if len(valid_peers) < 2:
            continue
        for trade_date in unique_dates:
            for horizon in horizons:
                returns = []
                for symbol in valid_peers:
                    ret = _forward_return(shared_cache[symbol], _date_entry_index(shared_cache[symbol], trade_date), horizon)
                    if ret is not None:
                        returns.append(ret)
                if len(returns) >= 2:
                    bench_rows.append(
                        {
                            "sector_key": sector_key,
                            "entry_date": pd.Timestamp(trade_date).normalize(),
                            f"sector_ret_{horizon}d": float(pd.Series(returns).median()),
                            f"sector_peer_count_{horizon}d": len(returns),
                        }
                    )
    if not bench_rows:
        return out, shared_cache
    bench = pd.DataFrame(bench_rows).groupby(["sector_key", "entry_date"], as_index=False).first()
    out = out.merge(bench, how="left", on=["sector_key", "entry_date"])
    return out, shared_cache


def warm_sector_peer_price_cache(
    symbols: list[str],
    *,
    start_date: str,
    end_date: str,
    price_cache: dict[str, pd.DataFrame | None] | None = None,
    warm_limit_per_sector: int = 12,
    warm_limit_total: int = 72,
    sector_payload: tuple[dict[str, str], dict[str, str], dict[str, list[str]]] | None = None,
) -> tuple[dict[str, pd.DataFrame | None], dict[str, object]]:
    shared_cache = price_cache or {}
    symbol_sector_key, _, sector_members = sector_payload or build_sector_peer_map(symbols)
    listing = load_listing_snapshot()
    marcap_map = listing.set_index("Code")["Marcap"].to_dict() if not listing.empty and "Marcap" in listing.columns else {}
    report_sector_symbols: dict[str, list[str]] = {}
    for symbol in [str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()]:
        sector_key = symbol_sector_key.get(symbol, "")
        if not sector_key:
            continue
        report_sector_symbols.setdefault(sector_key, [])
        if symbol not in report_sector_symbols[sector_key]:
            report_sector_symbols[sector_key].append(symbol)

    warm_targets: list[str] = []
    for sector_key, peers in sector_members.items():
        ranked = sorted(
            [symbol for symbol in peers if symbol not in shared_cache],
            key=lambda symbol: (-float(marcap_map.get(symbol, 0) or 0), symbol),
        )
        sector_top = list(dict.fromkeys((report_sector_symbols.get(sector_key) or []) + ranked[: max(4, int(warm_limit_per_sector))]))
        warm_targets.extend(sector_top)

    ordered_missing = list(dict.fromkeys([symbol for symbol in warm_targets if symbol not in shared_cache]))[: max(20, int(warm_limit_total))]
    if ordered_missing:
        def _load(symbol: str):
            return symbol, load_price_history(symbol, start_date, end_date)
        with ThreadPoolExecutor(max_workers=min(12, max(2, len(ordered_missing)))) as executor:
            for symbol, price_df in executor.map(_load, ordered_missing):
                shared_cache[symbol] = price_df

    warmed = [symbol for symbol in ordered_missing if shared_cache.get(symbol) is not None and not shared_cache.get(symbol).empty]
    return shared_cache, {
        "requested_symbols": len(symbols),
        "target_symbols": len(ordered_missing),
        "warmed_symbols": len(warmed),
        "sample_symbols": warmed[:10],
    }
