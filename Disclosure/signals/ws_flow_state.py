from __future__ import annotations

import json
import os
import threading
from collections import Counter
from datetime import datetime
from typing import Any, Dict

try:
    from ws_flow_helpers import dated_jsonl_path, top_counter
except Exception:
    from .ws_flow_helpers import dated_jsonl_path, top_counter


FLOW_TICK_LOG_MIN_AMT_MIL = int(os.getenv("FLOW_TICK_LOG_MIN_AMT_MIL", "30"))
FLOW_TICK_LOG_MIN_FOREIGN_QTY = int(os.getenv("FLOW_TICK_LOG_MIN_FOREIGN_QTY", "150"))
SNAPSHOT_INTERVAL_SEC = int(os.getenv("FLOW_SNAPSHOT_INTERVAL_SEC", "300"))
SNAPSHOT_MIN_GROSS_AMT_MIL = int(os.getenv("FLOW_SNAPSHOT_MIN_GROSS_AMT_MIL", "80"))
SNAPSHOT_FORCE_GROSS_AMT_MIL = int(os.getenv("FLOW_SNAPSHOT_FORCE_GROSS_AMT_MIL", "500"))
FLOW_HEALTH_PATH_NAME = "trading_flow_health_latest.json"

class FlowStateTracker:
    def __init__(self, log_dir: str, lock: threading.Lock | None = None):
        self.log_dir = log_dir
        self.lock = lock or threading.Lock()
        self.states: dict[str, dict[str, Any]] = {}
        self.health: dict[str, Any] = {
            "generated_at": "",
            "total_updates": 0,
            "flow_tick_logged": 0,
            "snapshot_logged": 0,
            "skip_below_tick_threshold": 0,
            "skip_snapshot_interval_gate": 0,
            "skip_snapshot_min_gross": 0,
            "last_event_at": "",
            "last_snapshot_at": "",
            "latest_symbol": "",
            "flow_tick_log_min_amt_mil": FLOW_TICK_LOG_MIN_AMT_MIL,
            "flow_tick_log_min_foreign_qty": FLOW_TICK_LOG_MIN_FOREIGN_QTY,
            "snapshot_interval_sec": SNAPSHOT_INTERVAL_SEC,
            "snapshot_min_gross_amt_mil": SNAPSHOT_MIN_GROSS_AMT_MIL,
            "snapshot_force_gross_amt_mil": SNAPSHOT_FORCE_GROSS_AMT_MIL,
        }
        os.makedirs(self.log_dir, exist_ok=True)

    def _append_jsonl(self, path: str, payload: dict) -> None:
        with self.lock:
            with open(path, "a", encoding="utf-8") as fp:
                fp.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _flow_tick_path(self, captured_at: str) -> str:
        return dated_jsonl_path(self.log_dir, "trading_flow_ticks", captured_at)

    def _snapshot_path(self, captured_at: str) -> str:
        return dated_jsonl_path(self.log_dir, "trading_snapshots", captured_at)

    def _health_path(self) -> str:
        return os.path.join(self.log_dir, FLOW_HEALTH_PATH_NAME)

    def _flush_health(self) -> None:
        self.health["generated_at"] = datetime.now().isoformat(timespec="seconds")
        with self.lock:
            with open(self._health_path(), "w", encoding="utf-8") as fp:
                json.dump(self.health, fp, ensure_ascii=False, indent=2)

    def _new_state(self, event: dict) -> dict[str, Any]:
        captured_at = str(event.get("captured_at") or "")
        return {
            "symbol": event.get("symbol"),
            "stock_name": event.get("stock_name"),
            "first_seen_at": captured_at,
            "last_seen_at": captured_at,
            "last_snapshot_at": captured_at,
            "market_state": event.get("market_state"),
            "last_price": int(event.get("current_price", 0) or 0),
            "cum_buy_amt_mil": 0,
            "cum_sell_amt_mil": 0,
            "cum_net_amt_mil": 0,
            "cum_buy_qty": 0,
            "cum_sell_qty": 0,
            "cum_foreign_delta_qty": 0,
            "tick_count": 0,
            "interval_tick_count": 0,
            "gross_amt_since_snapshot": 0,
            "buy_broker_amounts": Counter(),
            "sell_broker_amounts": Counter(),
            "price_band_amounts": Counter(),
            "event_counts": Counter(),
        }

    def _state_score(self, state: dict[str, Any]) -> int:
        net_amt = int(state.get("cum_net_amt_mil", 0) or 0)
        foreign_qty = int(state.get("cum_foreign_delta_qty", 0) or 0)
        gross_amt = int(state.get("gross_amt_since_snapshot", 0) or 0)

        buy_total = sum(state["buy_broker_amounts"].values())
        top_buy = state["buy_broker_amounts"].most_common(1)
        top_buy_share = (top_buy[0][1] / buy_total) if top_buy and buy_total else 0.0

        band_total = sum(state["price_band_amounts"].values())
        top_band = state["price_band_amounts"].most_common(1)
        top_band_share = (top_band[0][1] / band_total) if top_band and band_total else 0.0

        positive_events = sum(
            state["event_counts"].get(key, 0)
            for key in ("D_DEFENSE", "A_HANDOVER", "C_TWIN", "E_EXHAUST", "B_FOREIGN_WHALE")
        )
        breakout_events = state["event_counts"].get("F_BOMBARD", 0)

        score = 0
        score += min(35, max(0, net_amt) // 80)
        score += min(15, max(0, foreign_qty) // 400)
        score += min(10, gross_amt // 120)
        score += int(round(top_buy_share * 12))
        score += int(round(top_band_share * 10))
        score += positive_events * 4
        score -= breakout_events * 3
        return int(score)

    def update(self, event: dict) -> None:
        symbol = str(event.get("symbol") or "")
        if not symbol:
            return

        self.health["total_updates"] += 1
        self.health["last_event_at"] = str(event.get("captured_at") or "")
        self.health["latest_symbol"] = symbol
        state = self.states.setdefault(symbol, self._new_state(event))
        state["last_seen_at"] = event.get("captured_at")
        state["market_state"] = event.get("market_state")
        state["last_price"] = int(event.get("current_price", 0) or 0)
        state["cum_buy_amt_mil"] += int(event.get("buy_amt_mil", 0) or 0)
        state["cum_sell_amt_mil"] += int(event.get("sell_amt_mil", 0) or 0)
        state["cum_net_amt_mil"] += int(event.get("net_amt_mil", 0) or 0)
        state["cum_buy_qty"] += int(event.get("delta_buy_qty", 0) or 0)
        state["cum_sell_qty"] += int(event.get("delta_sell_qty", 0) or 0)
        state["cum_foreign_delta_qty"] += int(event.get("delta_foreign_qty", 0) or 0)
        state["tick_count"] += 1
        state["interval_tick_count"] += 1
        state["gross_amt_since_snapshot"] += int(event.get("gross_amt_mil", 0) or 0)

        buy_broker = event.get("buy_broker")
        sell_broker = event.get("sell_broker")
        band = event.get("price_band")

        if buy_broker:
            state["buy_broker_amounts"][buy_broker] += int(event.get("buy_amt_mil", 0) or 0)
        if sell_broker:
            state["sell_broker_amounts"][sell_broker] += int(event.get("sell_amt_mil", 0) or 0)
        if band:
            state["price_band_amounts"][str(band)] += int(event.get("gross_amt_mil", 0) or 0)

        self._maybe_log_flow_tick(event, state)
        self._maybe_flush_snapshot(event, state)

    def mark_event(self, event: dict) -> None:
        symbol = str(event.get("symbol") or "")
        event_type = str(event.get("event_type") or "")
        if not symbol or not event_type:
            return
        state = self.states.setdefault(symbol, self._new_state(event))
        state["event_counts"][event_type] += 1

    def _maybe_log_flow_tick(self, event: dict, state: dict[str, Any]) -> None:
        gross_amt = int(event.get("gross_amt_mil", 0) or 0)
        foreign_qty = abs(int(event.get("delta_foreign_qty", 0) or 0))
        if gross_amt < FLOW_TICK_LOG_MIN_AMT_MIL and foreign_qty < FLOW_TICK_LOG_MIN_FOREIGN_QTY:
            self.health["skip_below_tick_threshold"] += 1
            if self.health["skip_below_tick_threshold"] % 200 == 0:
                self._flush_health()
            return

        payload = {
            "record_type": "FLOW_TICK",
            **event,
            "cum_net_amt_mil": state["cum_net_amt_mil"],
            "cum_foreign_delta_qty": state["cum_foreign_delta_qty"],
            "tick_count": state["tick_count"],
        }
        self._append_jsonl(self._flow_tick_path(str(event.get("captured_at") or "")), payload)
        self.health["flow_tick_logged"] += 1
        if self.health["flow_tick_logged"] % 20 == 0:
            self._flush_health()

    def _maybe_flush_snapshot(self, event: dict, state: dict[str, Any]) -> None:
        captured_at = str(event.get("captured_at") or "")
        if not captured_at:
            return
        try:
            now_dt = datetime.fromisoformat(captured_at)
            last_dt = datetime.fromisoformat(str(state.get("last_snapshot_at") or captured_at))
        except Exception:
            return

        elapsed_sec = (now_dt - last_dt).total_seconds()
        gross_amt = int(state.get("gross_amt_since_snapshot", 0) or 0)
        if elapsed_sec < SNAPSHOT_INTERVAL_SEC and gross_amt < SNAPSHOT_FORCE_GROSS_AMT_MIL:
            self.health["skip_snapshot_interval_gate"] += 1
            if self.health["skip_snapshot_interval_gate"] % 200 == 0:
                self._flush_health()
            return
        if gross_amt < SNAPSHOT_MIN_GROSS_AMT_MIL:
            self.health["skip_snapshot_min_gross"] += 1
            if self.health["skip_snapshot_min_gross"] % 100 == 0:
                self._flush_health()
            return

        buy_total = sum(state["buy_broker_amounts"].values())
        band_total = sum(state["price_band_amounts"].values())
        top_buy = state["buy_broker_amounts"].most_common(1)
        top_band = state["price_band_amounts"].most_common(1)

        payload = {
            "record_type": "FLOW_SNAPSHOT",
            "captured_at": captured_at,
            "market_state": state["market_state"],
            "symbol": state["symbol"],
            "stock_name": state["stock_name"],
            "last_price": state["last_price"],
            "tick_count": state["tick_count"],
            "interval_tick_count": state["interval_tick_count"],
            "cum_buy_amt_mil": state["cum_buy_amt_mil"],
            "cum_sell_amt_mil": state["cum_sell_amt_mil"],
            "cum_net_amt_mil": state["cum_net_amt_mil"],
            "cum_buy_qty": state["cum_buy_qty"],
            "cum_sell_qty": state["cum_sell_qty"],
            "cum_foreign_delta_qty": state["cum_foreign_delta_qty"],
            "gross_amt_since_snapshot": gross_amt,
            "top_buy_brokers": top_counter(state["buy_broker_amounts"], limit=3),
            "top_sell_brokers": top_counter(state["sell_broker_amounts"], limit=3),
            "key_price_bands": top_counter(state["price_band_amounts"], limit=3),
            "top_buy_broker_share": round((top_buy[0][1] / buy_total), 4) if top_buy and buy_total else 0.0,
            "top_price_band_share": round((top_band[0][1] / band_total), 4) if top_band and band_total else 0.0,
            "event_counts": dict(state["event_counts"]),
            "flow_state_score": self._state_score(state),
            "first_seen_at": state["first_seen_at"],
            "last_seen_at": state["last_seen_at"],
        }
        self._append_jsonl(self._snapshot_path(captured_at), payload)
        self.health["snapshot_logged"] += 1
        self.health["last_snapshot_at"] = captured_at
        self._flush_health()

        state["last_snapshot_at"] = captured_at
        state["gross_amt_since_snapshot"] = 0
        state["interval_tick_count"] = 0
