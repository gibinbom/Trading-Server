from __future__ import annotations

import math
import os

import numpy as np
import pandas as pd

try:
    from signals.slack_log_loader import collect_flow_snapshots
except Exception:
    from Disclosure.signals.slack_log_loader import collect_flow_snapshots


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SIGNAL_LOG_DIR = os.path.join(ROOT_DIR, "signals", "logs")


def _entropy(shares: list[float]) -> float:
    probs = [max(0.0, float(x)) for x in shares if x is not None and float(x) > 0]
    total = sum(probs)
    if total <= 0:
        return 0.0
    probs = [x / total for x in probs]
    return -sum(p * math.log(p + 1e-12) for p in probs) / math.log(max(len(probs), 2))


def build_flow_microstructure_frame(days: int = 3, log_dir: str = SIGNAL_LOG_DIR) -> pd.DataFrame:
    snapshots, _ = collect_flow_snapshots(log_dir, days)
    if not snapshots:
        return pd.DataFrame()
    df = pd.DataFrame(snapshots)
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame()
    df["captured_at"] = pd.to_datetime(df.get("captured_at"), errors="coerce")
    df = df.dropna(subset=["captured_at"]).sort_values(["symbol", "captured_at"]).groupby("symbol", as_index=False).tail(1).copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    numeric_cols = [
        "cum_buy_amt_mil",
        "cum_sell_amt_mil",
        "cum_net_amt_mil",
        "cum_buy_qty",
        "cum_sell_qty",
        "cum_foreign_delta_qty",
        "top_buy_broker_share",
        "top_price_band_share",
        "flow_state_score",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    broker_entropy = []
    absorption = []
    for _, row in df.iterrows():
        buy_entries = row.get("top_buy_brokers") or []
        sell_entries = row.get("top_sell_brokers") or []
        shares = [float(item.get("share", 0) or 0) for item in list(buy_entries) + list(sell_entries)]
        broker_entropy.append(_entropy(shares))

        counts = row.get("event_counts") or {}
        pos = float(counts.get("D_DEFENSE", 0) + counts.get("A_HANDOVER", 0) + counts.get("E_EXHAUST", 0) + counts.get("C_TWIN", 0))
        neg = float(counts.get("F_BOMBARD", 0))
        absorption.append(pos - neg * 0.75)

    gross_amt = df["cum_buy_amt_mil"].fillna(0).abs() + df["cum_sell_amt_mil"].fillna(0).abs()
    gross_qty = df["cum_buy_qty"].fillna(0).abs() + df["cum_sell_qty"].fillna(0).abs()
    df["micro_ofi"] = df["cum_net_amt_mil"].fillna(0).div(gross_amt.replace(0, np.nan)).clip(-1.0, 1.0)
    df["micro_foreign_pressure"] = df["cum_foreign_delta_qty"].fillna(0).div(gross_qty.replace(0, np.nan)).clip(-1.0, 1.0)
    df["micro_broker_entropy"] = pd.Series(broker_entropy, index=df.index).fillna(0.0)
    df["micro_broker_concentration"] = 1.0 - df["micro_broker_entropy"]
    df["micro_queue_imbalance"] = (df["top_price_band_share"].fillna(0.0) * 2.0 - 1.0).clip(-1.0, 1.0)
    df["micro_absorption_raw"] = pd.Series(absorption, index=df.index).fillna(0.0)
    df["micro_absorption_score"] = df["micro_absorption_raw"].rank(pct=True, ascending=True)
    df["microstructure_score"] = (
        df["micro_ofi"].fillna(0.0) * 0.30
        + df["micro_foreign_pressure"].fillna(0.0) * 0.20
        + df["micro_queue_imbalance"].fillna(0.0) * 0.15
        + df["micro_broker_concentration"].fillna(0.0) * 0.10
        + (df["micro_absorption_score"].fillna(0.5) * 2.0 - 1.0) * 0.25
    )
    keep = [
        "symbol",
        "micro_ofi",
        "micro_foreign_pressure",
        "micro_broker_concentration",
        "micro_queue_imbalance",
        "micro_absorption_score",
        "microstructure_score",
    ]
    return df[keep].reset_index(drop=True)
