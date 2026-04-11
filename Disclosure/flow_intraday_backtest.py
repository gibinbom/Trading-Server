from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

try:
    from naver_intraday_fallback import fetch_naver_intraday_history
    from signals.slack_log_loader import collect_structured_events
except Exception:
    from Disclosure.naver_intraday_fallback import fetch_naver_intraday_history
    from Disclosure.signals.slack_log_loader import collect_structured_events


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SIGNAL_LOG_DIR = os.path.join(ROOT_DIR, "signals", "logs")
REPORT_DIR = os.path.join(ROOT_DIR, "signals", "reports")


def _load_flow_events(days: int = 3, log_dir: str = SIGNAL_LOG_DIR) -> list[dict[str, Any]]:
    events, _ = collect_structured_events(log_dir, days)
    return [row for row in events if str(row.get("symbol") or "").strip()]


def _normalize_timestamp(value: Any) -> pd.Timestamp | pd.NaT:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    try:
        if getattr(ts, "tzinfo", None) is not None:
            return ts.tz_convert("Asia/Seoul").tz_localize(None)
    except Exception:
        try:
            return ts.tz_localize(None)
        except Exception:
            return ts
    return ts


def _normalize_datetime_series(series: pd.Series) -> pd.Series:
    out = pd.to_datetime(series, errors="coerce")
    try:
        if getattr(out.dt, "tz", None) is not None:
            return out.dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    except Exception:
        try:
            return out.dt.tz_localize(None)
        except Exception:
            return out
    return out


def _find_entry_idx(price_df: pd.DataFrame, captured_at: str) -> int | None:
    event_ts = _normalize_timestamp(captured_at)
    if pd.isna(event_ts):
        return None
    dt_series = _normalize_datetime_series(price_df["DateTime"])
    later = price_df.index[dt_series >= event_ts].tolist()
    return later[0] if later else None


def _ret(price_df: pd.DataFrame, idx: int | None, minutes: int) -> float | None:
    if idx is None or idx >= len(price_df):
        return None
    entry = float(price_df.iloc[idx]["Close"])
    if not entry:
        return None
    event_ts = price_df.iloc[idx]["DateTime"]
    target_ts = event_ts + timedelta(minutes=int(minutes))
    later = price_df.index[price_df["DateTime"] >= target_ts].tolist()
    if not later:
        return None
    exit_px = float(price_df.iloc[later[0]]["Close"])
    return ((exit_px / entry) - 1.0) * 100.0


def build_intraday_backtest(days: int = 3, log_dir: str = SIGNAL_LOG_DIR) -> tuple[pd.DataFrame, pd.DataFrame]:
    events = _load_flow_events(days, log_dir=log_dir)
    if not events:
        return pd.DataFrame(), pd.DataFrame()
    rows = []
    cache: dict[tuple[str, str], pd.DataFrame] = {}
    for event in events:
        symbol = str(event.get("symbol") or "").zfill(6)
        captured_at = str(event.get("captured_at") or "")
        if not symbol or not captured_at:
            continue
        trade_date = captured_at[:10]
        key = (symbol, trade_date)
        if key not in cache:
            price_df = fetch_naver_intraday_history(symbol, trade_date, sleep_sec=0.0)
            if not price_df.empty and "DateTime" in price_df.columns:
                price_df = price_df.copy()
                price_df["DateTime"] = _normalize_datetime_series(price_df["DateTime"])
            cache[key] = price_df
        price_df = cache[key]
        if price_df.empty:
            continue
        idx = _find_entry_idx(price_df, captured_at)
        if idx is None:
            continue
        rows.append(
            {
                "captured_at": captured_at,
                "symbol": symbol,
                "stock_name": event.get("stock_name") or symbol,
                "event_type": event.get("event_type") or "",
                "buy_broker": event.get("buy_broker") or "",
                "sell_broker": event.get("sell_broker") or "",
                "net_amt_mil": int(event.get("net_amt_mil", 0) or 0),
                "entry_price": float(price_df.iloc[idx]["Close"]),
                "ret_5m": _ret(price_df, idx, 5),
                "ret_15m": _ret(price_df, idx, 15),
                "ret_30m": _ret(price_df, idx, 30),
                "ret_60m": _ret(price_df, idx, 60),
            }
        )
    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame()
    summary = (
        detail.groupby("event_type", dropna=False)
        .agg(
            sample_size=("symbol", "count"),
            avg_ret_5m=("ret_5m", "mean"),
            avg_ret_15m=("ret_15m", "mean"),
            avg_ret_30m=("ret_30m", "mean"),
            avg_ret_60m=("ret_60m", "mean"),
        )
        .reset_index()
        .sort_values(["avg_ret_30m", "sample_size"], ascending=[False, False])
    )
    return detail, summary


def build_intraday_feature_frame(days: int = 3, log_dir: str = SIGNAL_LOG_DIR) -> pd.DataFrame:
    detail, _ = build_intraday_backtest(days=days, log_dir=log_dir)
    if detail.empty:
        return pd.DataFrame()

    captured_ts = pd.to_datetime(detail["captured_at"], errors="coerce")
    detail = detail.copy()
    detail["captured_ts"] = captured_ts
    feature_df = (
        detail.sort_values(["symbol", "captured_ts"])
        .groupby("symbol", dropna=False)
        .agg(
            name=("stock_name", "last"),
            flow_intraday_samples=("symbol", "count"),
            flow_intraday_avg_5m=("ret_5m", "mean"),
            flow_intraday_avg_15m=("ret_15m", "mean"),
            flow_intraday_avg_30m=("ret_30m", "mean"),
            flow_intraday_avg_60m=("ret_60m", "mean"),
            flow_intraday_last_event=("event_type", "last"),
            flow_intraday_last_captured_at=("captured_at", "last"),
        )
        .reset_index()
    )
    raw_score = (
        feature_df["flow_intraday_avg_15m"].fillna(0.0) * 0.25
        + feature_df["flow_intraday_avg_30m"].fillna(0.0) * 0.45
        + feature_df["flow_intraday_avg_60m"].fillna(0.0) * 0.30
    )
    sample_boost = (1.0 + feature_df["flow_intraday_samples"].clip(lower=0).pow(0.5) * 0.15).clip(upper=1.6)
    feature_df["flow_intraday_edge_raw"] = raw_score
    feature_df["flow_intraday_edge_score"] = raw_score * sample_boost
    return feature_df


def save_intraday_backtest(detail: pd.DataFrame, summary: pd.DataFrame, report_dir: str = REPORT_DIR) -> dict[str, str]:
    os.makedirs(report_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    detail_csv = os.path.join(report_dir, f"flow_intraday_backtest_{stamp}.csv")
    summary_csv = os.path.join(report_dir, f"flow_intraday_summary_{stamp}.csv")
    detail.to_csv(detail_csv, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    return {"detail_csv": detail_csv, "summary_csv": summary_csv}


def build_intraday_digest(summary: pd.DataFrame, max_rows: int = 8) -> str:
    if summary.empty:
        return "*Flow intraday backtest*\n- no price-backed intraday rows"
    lines = ["*Flow intraday backtest*"]
    for _, row in summary.head(max_rows).iterrows():
        lines.append(
            f"- {row['event_type']}: n={int(row['sample_size'])}, "
            f"avg5m={row['avg_ret_5m']:.2f}%, avg15m={row['avg_ret_15m']:.2f}%, "
            f"avg30m={row['avg_ret_30m']:.2f}%, avg60m={row['avg_ret_60m']:.2f}%"
        )
    return "\n".join(lines)
