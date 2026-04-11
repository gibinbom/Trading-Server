from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta

import pandas as pd

try:
    from naver_price_fallback import fetch_naver_daily_price_history
except Exception:
    from Disclosure.naver_price_fallback import fetch_naver_daily_price_history

try:
    import FinanceDataReader as fdr
except Exception:
    fdr = None

try:
    from pykrx import stock as pykrx_stock
except Exception:
    pykrx_stock = None


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CACHE_DIR = os.path.join(ROOT_DIR, "factors", "cache", "prices")


def _normalize_price_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    price_df = df.copy()
    if "Date" not in price_df.columns:
        price_df = price_df.reset_index().rename(columns={price_df.index.name or "index": "Date"})
    for col in ["Date", "Close", "Open", "High", "Low", "Volume"]:
        if col not in price_df.columns:
            price_df[col] = pd.NA
    price_df["Date"] = pd.to_datetime(price_df["Date"], errors="coerce")
    price_df = price_df.dropna(subset=["Date"]).drop_duplicates(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    return price_df[["Date", "Open", "High", "Low", "Close", "Volume"]]


def _cache_path(symbol: str, cache_dir: str) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{str(symbol).zfill(6)}.csv")


def _read_cache(symbol: str, cache_dir: str) -> pd.DataFrame:
    path = _cache_path(symbol, cache_dir)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return _normalize_price_df(pd.read_csv(path))
    except Exception:
        return pd.DataFrame()


def _write_cache(symbol: str, df: pd.DataFrame, cache_dir: str) -> None:
    if df.empty:
        return
    path = _cache_path(symbol, cache_dir)
    _normalize_price_df(df).to_csv(path, index=False, encoding="utf-8-sig")


def _fetch_from_fdr(symbol: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    if fdr is None:
        return pd.DataFrame()
    try:
        return _normalize_price_df(fdr.DataReader(str(symbol).zfill(6), start_dt, end_dt))
    except Exception:
        return pd.DataFrame()


def _fetch_from_pykrx(symbol: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    if pykrx_stock is None:
        return pd.DataFrame()
    try:
        df = pykrx_stock.get_market_ohlcv_by_date(start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"), str(symbol).zfill(6))
        if df is None or df.empty:
            return pd.DataFrame()
        renamed = df.rename(
            columns={"시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume"}
        ).reset_index()
        return _normalize_price_df(renamed)
    except Exception:
        return pd.DataFrame()


def _fetch_from_naver(symbol: str, start_dt: date, end_dt: date, lookback_days: int, sleep_sec: float) -> pd.DataFrame:
    try:
        return _normalize_price_df(
            fetch_naver_daily_price_history(
                str(symbol).zfill(6),
                start_date=start_dt,
                end_date=end_dt,
                lookback_days=lookback_days,
                sleep_sec=sleep_sec,
            )
        )
    except Exception:
        return pd.DataFrame()


def load_price_history(
    symbol: str,
    *,
    start_dt: date | None = None,
    end_dt: date | None = None,
    lookback_days: int = 420,
    cache_dir: str = DEFAULT_CACHE_DIR,
    refresh: bool = False,
    sleep_sec: float = 0.0,
) -> pd.DataFrame:
    end_dt = end_dt or datetime.now().date()
    start_dt = start_dt or (end_dt - timedelta(days=int(max(lookback_days, 60) * 2.2)))
    cached = _read_cache(symbol, cache_dir)
    if not refresh and not cached.empty:
        if cached["Date"].max().date() >= end_dt and cached["Date"].min().date() <= start_dt:
            return cached[(cached["Date"].dt.date >= start_dt) & (cached["Date"].dt.date <= end_dt)].reset_index(drop=True)

    fetched = _fetch_from_fdr(symbol, start_dt, end_dt)
    if fetched.empty:
        fetched = _fetch_from_pykrx(symbol, start_dt, end_dt)
    if fetched.empty:
        fetched = _fetch_from_naver(symbol, start_dt, end_dt, lookback_days, sleep_sec)

    merged = pd.concat([cached, fetched], ignore_index=True) if not cached.empty else fetched
    merged = _normalize_price_df(merged)
    if not merged.empty:
        _write_cache(symbol, merged, cache_dir)
    if sleep_sec > 0:
        time.sleep(min(float(sleep_sec), 0.05))
    return merged[(merged["Date"].dt.date >= start_dt) & (merged["Date"].dt.date <= end_dt)].reset_index(drop=True)
