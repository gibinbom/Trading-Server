import json
import logging
import os
import re
import threading
import time as time_mod
from datetime import time

try:
    from ws_event_rules import ColorLog, build_signal_events
    from ws_flow_state import FlowStateTracker
    from ws_signal_runtime import _kst_now
except Exception:
    from .ws_event_rules import ColorLog, build_signal_events
    from .ws_flow_state import FlowStateTracker
    from .ws_signal_runtime import _kst_now


log = logging.getLogger("scanner")
current_dir = os.path.dirname(os.path.abspath(__file__))
STRUCTURED_LOG_DIR = os.path.join(current_dir, "logs")
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))


class NoColorFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record)
        return re.sub(r"\x1b\[[0-9;]*m", "", message)


class SignalAnalyzer:
    def __init__(self, watch_map, broker):
        self.foreign_brokers = ["모건스탠리", "메릴린치", "골드만삭스", "JP모간", "SG증권", "UBS", "CS증권", "맥쿼리", "씨티그룹", "다이와", "노무라"]
        self.inst_brokers = ["삼성증권", "한국증권", "KB증권", "신한증권", "NH투자증권"]
        self.retail_brokers = ["키움증권", "미래에셋"]
        self.watch_map = watch_map
        self.broker = broker
        self.prev_foreign_net_buy = {}
        self.prev_price = {}
        self.slack_queue = []
        self.ws_slack_enabled = os.getenv("WS_SIGNAL_SLACK_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
        self.price_cache = {}
        self.dropped_tick_count = 0
        self.last_drop_log_ts = 0.0
        self.last_analyze_error_ts = 0.0
        self.log_lock = threading.Lock()
        self.flow_tracker = FlowStateTracker(STRUCTURED_LOG_DIR, lock=self.log_lock)
        os.makedirs(STRUCTURED_LOG_DIR, exist_ok=True)

    def _safe_int(self, value):
        try:
            if not value or str(value).strip() == "":
                return 0
            return int(float(str(value).replace(",", "").strip()))
        except (ValueError, TypeError):
            return 0

    def _get_tick_size(self, price):
        if price < 2000:
            return 1
        if price < 5000:
            return 5
        if price < 20000:
            return 10
        if price < 50000:
            return 50
        if price < 200000:
            return 100
        if price < 500000:
            return 500
        return 1000

    def _get_market_state(self):
        now = _kst_now().time()
        if time(8, 0) <= now < time(8, 50):
            return "NXT_PRE", 0.2
        if time(9, 0) <= now < time(15, 30):
            return "KRX_REG", 1.0
        if time(15, 30) <= now < time(16, 0):
            return "NXT_POST", 0.3
        if time(16, 0) <= now < time(18, 0):
            return "AFTER_HOURS", 0.0
        if time(18, 0) <= now < time(20, 0):
            return "NXT_NIGHT", 0.2
        return "CLOSED", 0.0

    def _get_event_log_path(self):
        current_date = _kst_now().strftime("%Y%m%d")
        return os.path.join(STRUCTURED_LOG_DIR, f"trading_events_{current_date}.jsonl")

    def _get_legacy_log_paths(self):
        current_date = _kst_now().strftime("%Y%m%d")
        return [
            os.path.join(PROJECT_ROOT, f"trading_{current_date}.txt"),
            os.path.join(STRUCTURED_LOG_DIR, f"trading_{current_date}.txt"),
        ]

    def _price_band(self, price):
        if not price:
            return 0
        band = self._get_tick_size(price) * 5
        return int(round(price / band) * band) if band > 0 else int(price)

    def _log_and_notify(self, color, msg, event_payload=None, msg_type="info"):
        log.info("\n%s%s%s", color, msg, ColorLog.RESET)
        current_date = _kst_now().strftime("%Y%m%d")
        current_time = _kst_now().strftime("%H:%M:%S")
        clean_msg = re.sub(r"\x1b\[[0-9;]*m", "", msg)
        with self.log_lock:
            for legacy_path in self._get_legacy_log_paths():
                with open(legacy_path, "a", encoding="utf-8") as handle:
                    handle.write(f"{current_time} | {clean_msg}\n")
            if event_payload:
                with open(self._get_event_log_path(), "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(event_payload, ensure_ascii=False) + "\n")
                self.flow_tracker.mark_event(event_payload)
        if self.ws_slack_enabled:
            self.slack_queue.append({"text": msg, "msg_type": msg_type})

    def note_tick_drop(self) -> None:
        self.dropped_tick_count += 1
        now_ts = time_mod.time()
        if now_ts - self.last_drop_log_ts >= 30:
            log.warning("⚠️ tick queue overflow: dropped=%d", self.dropped_tick_count)
            self.last_drop_log_ts = now_ts

    def analyze_tick(self, data_fields: list):
        if len(data_fields) < 66:
            return

        market_state, threshold_multiplier = self._get_market_state()
        if market_state == "CLOSED":
            return

        try:
            symbol = data_fields[0]
            stock_name = self.watch_map.get(symbol, symbol)
            sell_broker = data_fields[1].strip()
            buy_broker = data_fields[6].strip()
            delta_sell_qty = self._safe_int(data_fields[51])
            delta_buy_qty = self._safe_int(data_fields[56])
            foreign_net_buy = self._safe_int(data_fields[65])
            prev_foreign_net = self.prev_foreign_net_buy.get(symbol, foreign_net_buy)
            delta_foreign = foreign_net_buy - prev_foreign_net
            self.prev_foreign_net_buy[symbol] = foreign_net_buy
            if delta_buy_qty == 0 and delta_sell_qty == 0 and delta_foreign == 0:
                return

            current_price = self.price_cache.get(symbol, 0)
            if current_price == 0:
                return

            prev_price = self.prev_price.get(symbol, current_price)
            base_event = {
                "captured_at": _kst_now().isoformat(timespec="seconds"),
                "market_state": market_state,
                "symbol": symbol,
                "stock_name": stock_name,
                "buy_broker": buy_broker,
                "sell_broker": sell_broker,
                "delta_buy_qty": delta_buy_qty,
                "delta_sell_qty": delta_sell_qty,
                "delta_foreign_qty": delta_foreign,
                "foreign_net_buy_qty": foreign_net_buy,
                "current_price": current_price,
                "prev_price": prev_price,
                "price_delta": current_price - prev_price,
                "tick_size": self._get_tick_size(prev_price),
                "price_band": self._price_band(current_price),
                "buy_amt_mil": (delta_buy_qty * current_price) // 1_000_000,
                "sell_amt_mil": (delta_sell_qty * current_price) // 1_000_000,
                "gross_amt_mil": ((delta_buy_qty + delta_sell_qty) * current_price) // 1_000_000,
                "net_amt_mil": ((delta_buy_qty - delta_sell_qty) * current_price) // 1_000_000,
                "threshold_amt_mil": max(10, int(300 * threshold_multiplier)),
                "threshold_multiplier": threshold_multiplier,
                "is_retail_selling": any(name in sell_broker for name in self.retail_brokers),
                "is_foreign_buying": any(name in buy_broker for name in self.foreign_brokers),
                "is_inst_buying": any(name in buy_broker for name in self.inst_brokers),
            }
            self.flow_tracker.update({**base_event, "amt_mil": int(base_event["buy_amt_mil"])})

            signal_events, should_update_prev_price = build_signal_events(
                {**base_event, "amt_mil": int(base_event["buy_amt_mil"])},
                threshold_multiplier,
            )
            if should_update_prev_price:
                self.prev_price[symbol] = current_price
            for color, msg, event_payload in signal_events:
                self._log_and_notify(color, msg, event_payload=event_payload)
        except Exception as exc:
            now_ts = time_mod.time()
            if now_ts - self.last_analyze_error_ts >= 60:
                preview = [str(item)[:20] for item in data_fields[:8]]
                log.warning("⚠️ analyze_tick failed: %s | preview=%s", str(exc)[:200], preview)
                self.last_analyze_error_ts = now_ts
