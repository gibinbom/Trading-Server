from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime, time as dt_time
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from config import SETTINGS
from close_swing_selector import evaluate_close_swing_candidate, load_close_swing_inputs
from disclosure_event_pipeline import load_event_records
from event_trade_state import EventTradeState
from hybrid_rotation_engine import (
    annotate_event_candidates_with_hybrid,
    append_trade_decision_ledger,
    build_hybrid_study_pack,
    compute_relative_value_candidates,
    compute_sector_rotation,
    load_hybrid_inputs,
    save_hybrid_runtime_artifacts,
    save_hybrid_study_pack,
    finalize_shadow_book,
)
try:
    from sector_thesis import build_sector_thesis, merge_sector_thesis_into_rotation
except Exception:
    from Disclosure.sector_thesis import build_sector_thesis, merge_sector_thesis_into_rotation
from utils.slack import (
    get_slack_runtime_meta,
    notify_execution_attribution,
    notify_sector_thesis,
    notify_trade_funnel,
    notify_trade_skip,
    send_slack,
)

try:
    from context_alignment import get_symbol_trade_context, load_latest_context_alignment
except Exception:
    from Disclosure.context_alignment import get_symbol_trade_context, load_latest_context_alignment

log = logging.getLogger("disclosure.event_trade")
KST = ZoneInfo("Asia/Seoul")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
EVENT_STATE_PATH = os.path.join(ROOT_DIR, "events", "main_trade_state.json")
TRADE_RUNTIME_STATUS_PATH = os.path.join(ROOT_DIR, "events", "trade_runtime_latest.json")
API_INTEGRATION_REPORT_PATH = os.path.join(ROOT_DIR, "runtime", "api_integrations_latest.json")
DEFAULT_ALLOWED_TYPES = {
    "SUPPLY_CONTRACT",
    "PERF_PRELIM",
    "SALES_VARIATION",
    "BUYBACK",
    "STOCK_CANCELLATION",
}
DEFAULT_ALLOWED_SOURCES = {"EVENT_COLLECTOR_HTML", "EVENT_COLLECTOR_API"}
ONE_SHARE_BUDGET_BLOCK_REASONS = {
    "max_trade_budget_below_one_share",
    "quote_fallback_budget_too_small",
    "daily_budget_below_one_share",
    "available_cash_below_one_share",
}


def _now_kst() -> datetime:
    return datetime.now(tz=KST)


def _parse_hhmm(value: str, fallback: str) -> dt_time:
    text = (value or fallback).strip()
    hour, minute = text.split(":", 1)
    return dt_time(int(hour), int(minute))


def _record_key(record: Dict[str, Any]) -> str:
    return ":".join(
        [
            str(record.get("stock_code") or "").zfill(6),
            str(record.get("rcp_no") or ""),
            str(record.get("src") or ""),
            str(record.get("event_date") or ""),
        ]
    )


def _normalize_csv_set(raw_value: str, default_values: set[str]) -> set[str]:
    if not raw_value:
        return set(default_values)
    values = {item.strip() for item in str(raw_value).split(",") if item.strip()}
    return values or set(default_values)


def _append_close_swing_note(base_note: Any, extra_note: str) -> str:
    base = str(base_note or "").strip()
    extra = str(extra_note or "").strip()
    if base and extra:
        return f"{base} | {extra}"
    return base or extra


class EventLogTradeWatcher:
    def __init__(self, engine):
        self.engine = engine
        self.state = EventTradeState(EVENT_STATE_PATH)
        self.poll_sec = max(5, int(getattr(SETTINGS, "EVENT_LOG_WATCH_POLL_SEC", 15)))
        self.lookback_days = max(1, int(getattr(SETTINGS, "EVENT_TRADE_LOOKBACK_DAYS", 2)))
        self.retry_cooldown_sec = max(60, int(getattr(SETTINGS, "EVENT_TRADE_RETRY_COOLDOWN_SEC", 900)))
        self.window_start = _parse_hhmm(
            getattr(SETTINGS, "EVENT_TRADE_WINDOW_START", "15:20"),
            "15:20",
        )
        self.window_end = _parse_hhmm(
            getattr(SETTINGS, "EVENT_TRADE_WINDOW_END", "15:30"),
            "15:30",
        )
        self.allowed_types = _normalize_csv_set(
            getattr(
                SETTINGS,
                "EVENT_TRADE_ALLOWED_TYPES",
                "SUPPLY_CONTRACT,PERF_PRELIM,SALES_VARIATION,BUYBACK,STOCK_CANCELLATION",
            ),
            DEFAULT_ALLOWED_TYPES,
        )
        self.allowed_sources = _normalize_csv_set(
            getattr(SETTINGS, "EVENT_TRADE_ALLOWED_SOURCES", "EVENT_COLLECTOR_HTML,EVENT_COLLECTOR_API"),
            DEFAULT_ALLOWED_SOURCES,
        )
        self._last_idle_log_at = 0.0
        self._last_runtime_payload: Dict[str, Any] = {}
        self._last_candidate_notify_digest = ""
        self._last_candidate_notify_at = 0.0
        self._last_result_notify_digest = ""
        self._last_result_notify_at = 0.0
        self._last_sector_thesis_digest = ""
        self._last_sector_thesis_at = 0.0
        self._last_broker_ready_checked_at = 0.0
        self._last_broker_ready_ok = True
        self._last_broker_ready_note = ""
        self._last_broker_cash_krw: Optional[int] = None
        self._last_broker_unready_notify_at = 0.0
        self._last_stop_sell_check_at = 0.0
        self._last_stop_sell_count = 0
        self._last_quote_probe_checked_at = 0.0
        self._last_quote_probe_primary_ok: Optional[bool] = None
        self._last_quote_probe_source = ""
        self._last_holding_symbol_count = 0
        self._last_holding_sector_count = 0
        self._last_holding_sector_summary = ""
        self._last_hybrid_active_sectors: list[str] = []
        self._last_hybrid_shadow_chosen = 0
        self._last_hybrid_live_selected = 0
        self._last_hybrid_live_only: list[str] = []
        self._last_hybrid_shadow_only: list[str] = []
        self._last_hybrid_quote_penalty_total = 0.0
        self._last_hybrid_blocked_histogram: dict[str, int] = {}
        self._last_close_swing_budget_adjusted_count = 0
        self._last_close_swing_one_share_blocked_histogram: dict[str, int] = {}
        self._monitor_window_start = dt_time(9, 0)
        self._monitor_window_end = dt_time(15, 29)

    def _write_runtime_status(
        self,
        phase: str,
        *,
        now: Optional[datetime] = None,
        candidate_count: int = 0,
        candidate_aligned_count: int = 0,
        candidate_risk_count: int = 0,
        candidate_approved_count: int = 0,
        candidate_blocked_count: int = 0,
        processed_count: int = 0,
        last_record: Optional[Dict[str, Any]] = None,
        last_result: str = "",
        error: str = "",
    ) -> None:
        now = now or _now_kst()
        handled = self.state.handled if isinstance(self.state.handled, dict) else {}
        traded_count = sum(1 for row in handled.values() if isinstance(row, dict) and row.get("traded") is True)
        broker = getattr(self.engine, "broker_live", None)
        slack_meta = get_slack_runtime_meta()
        payload = {
            "generated_at": now.isoformat(timespec="seconds"),
            "phase": str(phase or ""),
            "in_trade_window": bool(self._within_trade_window(now)),
            "poll_sec": int(self.poll_sec),
            "lookback_days": int(self.lookback_days),
            "auto_trade_enabled": bool(getattr(SETTINGS, "ENABLE_AUTO_TRADE", False)),
            "close_swing_enabled": bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False)),
            "close_swing_one_share_budget_floor_enabled": bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False)),
            "main_run_mode": str(getattr(SETTINGS, "MAIN_RUN_MODE", "") or ""),
            "window_start": self.window_start.strftime("%H:%M"),
            "window_end": self.window_end.strftime("%H:%M"),
            "broker_is_virtual": bool(getattr(broker, "is_virtual", getattr(SETTINGS, "KIS_IS_VIRTUAL", False))),
            "broker_dry_run": bool(getattr(broker, "dry_run", getattr(SETTINGS, "DRY_RUN", False))),
            "broker_ready": bool(self._last_broker_ready_ok),
            "broker_ready_note": str(self._last_broker_ready_note or ""),
            "broker_cash_krw": int(self._last_broker_cash_krw or 0),
            "today_stop_sell_count": int(self._last_stop_sell_count or 0),
            "quote_primary_ok": self._last_quote_probe_primary_ok,
            "quote_source": str(self._last_quote_probe_source or ""),
            "holding_symbol_count": int(self._last_holding_symbol_count or 0),
            "holding_sector_count": int(self._last_holding_sector_count or 0),
            "holding_sector_summary": str(self._last_holding_sector_summary or ""),
            "hybrid_rotation_enabled": bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)),
            "hybrid_shadow_only": bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)),
            "hybrid_active_sectors": list(self._last_hybrid_active_sectors or []),
            "hybrid_shadow_chosen_count": int(self._last_hybrid_shadow_chosen or 0),
            "hybrid_live_selected_count": int(self._last_hybrid_live_selected or 0),
            "hybrid_live_only_symbols": list(self._last_hybrid_live_only or []),
            "hybrid_shadow_only_symbols": list(self._last_hybrid_shadow_only or []),
            "hybrid_quote_penalty_total": float(self._last_hybrid_quote_penalty_total or 0.0),
            "hybrid_blocked_histogram": dict(self._last_hybrid_blocked_histogram or {}),
            "slack_notify_trade_skip": bool(slack_meta.get("notify_trade_skip", False)),
            "slack_webhook_hint": str(slack_meta.get("webhook_hint") or ""),
            "candidate_count": int(candidate_count or 0),
            "candidate_aligned_count": int(candidate_aligned_count or 0),
            "candidate_risk_count": int(candidate_risk_count or 0),
            "candidate_approved_count": int(candidate_approved_count or 0),
            "candidate_blocked_count": int(candidate_blocked_count or 0),
            "candidate_budget_adjusted_count": int(self._last_close_swing_budget_adjusted_count or 0),
            "candidate_one_share_blocked_histogram": dict(self._last_close_swing_one_share_blocked_histogram or {}),
            "processed_count": int(processed_count or 0),
            "handled_count": int(len(handled)),
            "traded_count": int(traded_count),
            "last_result": str(last_result or ""),
            "last_record": {
                "stock_code": str((last_record or {}).get("stock_code") or ""),
                "rcp_no": str((last_record or {}).get("rcp_no") or ""),
                "event_type": str((last_record or {}).get("event_type") or ""),
                "title": str((last_record or {}).get("title") or "")[:160],
                "sector": str((last_record or {}).get("context_sector") or ""),
                "alignment_label": str((last_record or {}).get("context_alignment_label") or ""),
                "close_swing_decision": str((last_record or {}).get("close_swing_decision") or ""),
                "close_swing_reason": str((last_record or {}).get("close_swing_reason") or ""),
                "close_swing_budget_krw": int((last_record or {}).get("close_swing_budget_krw", 0) or 0),
                "close_swing_min_order_budget_krw": int((last_record or {}).get("close_swing_min_order_budget_krw", 0) or 0),
                "close_swing_budget_adjusted": bool((last_record or {}).get("close_swing_budget_adjusted", False)),
                "hybrid_sector_regime_score": float((last_record or {}).get("hybrid_sector_regime_score", 0.0) or 0.0),
                "hybrid_sector_final_score": float((last_record or {}).get("hybrid_sector_final_score", 0.0) or 0.0),
                "hybrid_sector_final_label": str((last_record or {}).get("hybrid_sector_final_label") or ""),
                "hybrid_sector_agreement_level": str((last_record or {}).get("hybrid_sector_agreement_level") or ""),
                "hybrid_wics_status_label": str((last_record or {}).get("hybrid_wics_status_label") or ""),
                "hybrid_wics_history_confidence_label": str((last_record or {}).get("hybrid_wics_history_confidence_label") or ""),
                "hybrid_wics_dynamic_count": int((last_record or {}).get("hybrid_wics_dynamic_count", 0) or 0),
                "hybrid_wics_dynamic_stability": float((last_record or {}).get("hybrid_wics_dynamic_stability", 0.0) or 0.0),
                "hybrid_wics_penalty": float((last_record or {}).get("hybrid_wics_penalty", 0.0) or 0.0),
                "hybrid_wics_note": str((last_record or {}).get("hybrid_wics_note") or ""),
                "hybrid_relative_value_score": float((last_record or {}).get("hybrid_relative_value_score", 0.0) or 0.0),
                "hybrid_timing_score": float((last_record or {}).get("hybrid_timing_score", 0.0) or 0.0),
                "hybrid_final_trade_score": float((last_record or {}).get("hybrid_final_trade_score", 0.0) or 0.0),
                "hybrid_blocked_reason_code": str((last_record or {}).get("hybrid_blocked_reason_code") or ""),
            },
            "error": str(error or "")[:200],
        }
        if payload == self._last_runtime_payload:
            return
        self._last_runtime_payload = payload
        try:
            os.makedirs(os.path.dirname(TRADE_RUNTIME_STATUS_PATH), exist_ok=True)
            with open(TRADE_RUNTIME_STATUS_PATH, "w", encoding="utf-8") as fp:
                json.dump(payload, fp, ensure_ascii=False, indent=2)
        except Exception as exc:
            log.warning("[EVENT-TRADE] runtime status write failed: %s", exc)

    def _probe_broker_ready(self, *, force: bool = False) -> tuple[bool, str]:
        broker = getattr(self.engine, "broker_live", None)
        if broker is None:
            self._last_broker_ready_ok = False
            self._last_broker_ready_note = "broker_missing"
            self._last_broker_cash_krw = None
            return False, self._last_broker_ready_note
        if bool(getattr(broker, "dry_run", False)):
            self._last_broker_ready_ok = True
            self._last_broker_ready_note = "dry_run"
            self._last_broker_cash_krw = None
            return True, self._last_broker_ready_note

        cache_sec = max(15, int(getattr(SETTINGS, "EVENT_TRADE_BROKER_READY_CACHE_SEC", 60) or 60))
        now_ts = time.time()
        if not force and self._last_broker_ready_checked_at and (now_ts - self._last_broker_ready_checked_at) < cache_sec:
            return bool(self._last_broker_ready_ok), str(self._last_broker_ready_note or "")

        ok = False
        note = "balance_probe_failed"
        try:
            balance = broker.get_balance()
            if isinstance(balance, dict) and ("cash" in balance or "stocks" in balance):
                ok = True
                cash_krw = int(balance.get("cash", 0) or 0)
                self._last_broker_cash_krw = cash_krw
                stock_count = len(balance.get("stocks") or []) if isinstance(balance.get("stocks"), list) else 0
                note = f"balance_ok cash={cash_krw} stocks={stock_count}"
            else:
                self._last_broker_cash_krw = None
                note = "balance_empty"
        except Exception as exc:
            self._last_broker_cash_krw = None
            note = f"balance_error:{str(exc)[:120]}"

        self._last_broker_ready_checked_at = now_ts
        self._last_broker_ready_ok = bool(ok)
        self._last_broker_ready_note = str(note)
        return bool(ok), str(note)

    def _maybe_notify_broker_unready(self, *, approved_count: int, reason: str) -> None:
        now_ts = time.time()
        if (now_ts - self._last_broker_unready_notify_at) < 300:
            return
        body = (
            f"브로커 준비 상태 확인 실패로 이번 poll의 종가 배팅 주문을 보류했습니다.\n"
            f"- approved candidates: `{int(approved_count or 0)}`\n"
            f"- broker check: `{reason or '-'}`\n"
            f"- action: 다음 poll에서 자동 재평가"
        )
        send_slack(body, title="Main Trader Broker Check", msg_type="warning")
        self._last_broker_unready_notify_at = now_ts

    def _load_today_stop_sell_count(self, *, force: bool = False) -> int:
        now_ts = time.time()
        if not force and self._last_stop_sell_check_at and (now_ts - self._last_stop_sell_check_at) < 60:
            return int(self._last_stop_sell_count or 0)
        path = str(getattr(SETTINGS, "TRADE_ALERT_AUDIT_PATH", "") or "").strip()
        today = _now_kst().strftime("%Y-%m-%d")
        count = 0
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fp:
                    for line in fp:
                        text = line.strip()
                        if not text:
                            continue
                        try:
                            row = json.loads(text)
                        except Exception:
                            continue
                        if not isinstance(row, dict):
                            continue
                        timestamp = str(row.get("timestamp") or "")
                        action = str(row.get("action") or "").upper()
                        result = str(row.get("result") or "").lower()
                        if not timestamp.startswith(today):
                            continue
                        if not action.startswith("SELL"):
                            continue
                        if "SL" not in action:
                            continue
                        if result != "success":
                            continue
                        count += 1
            except Exception as exc:
                log.warning("[EVENT-TRADE] stop-sell audit parse failed: %s", exc)
        self._last_stop_sell_check_at = now_ts
        self._last_stop_sell_count = int(count)
        return int(count)

    def _load_quote_probe_state(self, *, force: bool = False) -> tuple[Optional[bool], str]:
        now_ts = time.time()
        if not force and self._last_quote_probe_checked_at and (now_ts - self._last_quote_probe_checked_at) < 60:
            return self._last_quote_probe_primary_ok, str(self._last_quote_probe_source or "")
        primary_ok: Optional[bool] = None
        source = ""
        if os.path.exists(API_INTEGRATION_REPORT_PATH):
            try:
                with open(API_INTEGRATION_REPORT_PATH, "r", encoding="utf-8") as fp:
                    payload = json.load(fp)
                kis = (payload or {}).get("kis") or {}
                if isinstance(kis, dict):
                    primary_ok = kis.get("primary_ok")
                    source = str(kis.get("source") or "")
            except Exception as exc:
                log.warning("[EVENT-TRADE] api integration report parse failed: %s", exc)
        self._last_quote_probe_checked_at = now_ts
        self._last_quote_probe_primary_ok = primary_ok if isinstance(primary_ok, bool) else None
        self._last_quote_probe_source = source
        return self._last_quote_probe_primary_ok, str(self._last_quote_probe_source or "")

    def _within_trade_window(self, now: Optional[datetime] = None) -> bool:
        now = now or _now_kst()
        current = now.time()
        return self.window_start <= current < self.window_end

    def _within_regular_monitor_window(self, now: Optional[datetime] = None) -> bool:
        now = now or _now_kst()
        current = now.time()
        return self._monitor_window_start <= current < self._monitor_window_end

    def _bootstrap_position_monitors_if_needed(self, now: Optional[datetime] = None) -> None:
        now = now or _now_kst()
        if not self._within_regular_monitor_window(now):
            return
        try:
            self.engine._bootstrap_position_monitors(self.engine.broker_live)
        except Exception as exc:
            log.warning("[EVENT-TRADE] position monitor bootstrap failed: %s", exc)

    def _today_only(self, record: Dict[str, Any], now: datetime) -> bool:
        return str(record.get("event_date") or "") == now.strftime("%Y-%m-%d")

    def _is_candidate(self, record: Dict[str, Any], now: datetime) -> bool:
        if str(record.get("src") or "") not in self.allowed_sources:
            return False
        if str(record.get("event_type") or "") not in self.allowed_types:
            return False
        if str(record.get("signal_bias") or "") != "positive":
            return False
        if not str(record.get("stock_code") or "").strip():
            return False
        if not str(record.get("rcp_no") or "").strip():
            return False
        if not self._today_only(record, now):
            return False
        return True

    def _load_candidates(self) -> list[Dict[str, Any]]:
        now = _now_kst()
        rows = load_event_records(days=self.lookback_days)
        alignment = load_latest_context_alignment()
        out: list[Dict[str, Any]] = []
        for row in rows:
            if not self._is_candidate(row, now):
                continue
            key = _record_key(row)
            if not self.state.should_process(key, self.retry_cooldown_sec):
                continue
            item = dict(row)
            context = get_symbol_trade_context(
                item.get("stock_code"),
                sector=item.get("sector"),
                alignment=alignment,
            )
            item["context_sector"] = str(context.get("sector") or "")
            item["context_alignment_label"] = str(context.get("alignment_label") or "중립")
            item["context_alignment_score"] = int(context.get("alignment_score", 0) or 0)
            item["context_market_mode"] = str(context.get("market_mode") or "")
            item["context_confidence_score"] = int(context.get("confidence_score", 0) or 0)
            item["context_note"] = str(context.get("note") or "")
            out.append(item)

        def _sort_key(item: Dict[str, Any]) -> tuple[str, str, str]:
            return (
                str(item.get("event_date") or ""),
                str(item.get("event_time_hhmm") or ""),
                str(item.get("rcp_no") or ""),
            )

        return sorted(out, key=_sort_key)

    def _trade_context_from_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        note_parts = [
            str(record.get("context_note") or "").strip(),
            str(record.get("close_swing_note") or "").strip(),
        ]
        hybrid_bits = []
        if record.get("hybrid_sector_regime_score") is not None:
            hybrid_bits.append(f"sector {float(record.get('hybrid_sector_regime_score', 0.0) or 0.0):.1f}")
        if record.get("hybrid_relative_value_score") is not None:
            hybrid_bits.append(f"relative {float(record.get('hybrid_relative_value_score', 0.0) or 0.0):.1f}")
        if record.get("hybrid_timing_score") is not None:
            hybrid_bits.append(f"timing {float(record.get('hybrid_timing_score', 0.0) or 0.0):.1f}")
        if record.get("hybrid_final_trade_score") is not None:
            hybrid_bits.append(f"final {float(record.get('hybrid_final_trade_score', 0.0) or 0.0):.1f}")
        if hybrid_bits:
            note_parts.append("hybrid " + " | ".join(hybrid_bits))
        note = " | ".join(part for part in note_parts if part)
        return {
            "sector": str(record.get("context_sector") or record.get("sector") or ""),
            "alignment_label": str(record.get("context_alignment_label") or "중립"),
            "alignment_score": int(record.get("context_alignment_score", 0) or 0),
            "market_mode": str(record.get("context_market_mode") or ""),
            "confidence_score": int(record.get("context_confidence_score", 0) or 0),
            "note": note,
            "hybrid_sector_regime_score": float(record.get("hybrid_sector_regime_score", 0.0) or 0.0),
            "hybrid_relative_value_score": float(record.get("hybrid_relative_value_score", 0.0) or 0.0),
            "hybrid_timing_score": float(record.get("hybrid_timing_score", 0.0) or 0.0),
            "hybrid_final_trade_score": float(record.get("hybrid_final_trade_score", 0.0) or 0.0),
            "blocked_reason_code": str(record.get("hybrid_blocked_reason_code") or ""),
            "quote_source": str(record.get("hybrid_quote_source") or ""),
        }

    def _maybe_notify_sector_thesis(
        self,
        sector_rotation: dict[str, Any],
        relative_value: dict[str, Any],
        shadow_book: dict[str, Any],
        sector_thesis: Optional[dict[str, Any]] = None,
    ) -> None:
        if not bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)):
            return
        digest = json.dumps(
            {
                "active": list(sector_rotation.get("active_sectors") or [])[:5],
                "shadow": list(shadow_book.get("shadow_top_symbols") or [])[:5],
                "live": list(shadow_book.get("live_top_symbols") or [])[:5],
                "thesis": [
                    {
                        "sector": row.get("sector"),
                        "label": row.get("final_label"),
                    }
                    for row in list((sector_thesis or {}).get("top_sectors") or [])[:4]
                ],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        now_ts = time.time()
        if digest == self._last_sector_thesis_digest and (now_ts - self._last_sector_thesis_at) < 300:
            return
        notify_sector_thesis(
            sector_rotation,
            relative_value,
            shadow_book,
            sector_thesis=sector_thesis or {},
        )
        self._last_sector_thesis_digest = digest
        self._last_sector_thesis_at = now_ts

    def _apply_hybrid_runtime(
        self,
        records: list[Dict[str, Any]],
        *,
        selector_inputs: Optional[Dict[str, Any]] = None,
        live_selected_keys: Optional[set[str]] = None,
        live_mode: str = "event_only",
        persist_ledger: bool = False,
    ) -> tuple[list[Dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
        if not bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)):
            empty_sector = {
                "generated_at": _now_kst().isoformat(timespec="seconds"),
                "active_sectors": [],
                "top_sectors": [],
                "sectors": [],
            }
            empty_relative = {
                "generated_at": _now_kst().isoformat(timespec="seconds"),
                "top_candidates": [],
                "candidates": [],
                "sector_buckets": [],
            }
            empty_shadow = {
                "generated_at": _now_kst().isoformat(timespec="seconds"),
                "active_sectors": [],
                "rows": [],
                "blocked_reason_histogram": {},
            }
            return records, empty_sector, empty_relative, empty_shadow

        hybrid_inputs = load_hybrid_inputs(
            {
                "market_briefing": (selector_inputs or {}).get("market_briefing"),
                "factor_df": None,
                "card_df": None,
            }
        )
        sector_rotation = compute_sector_rotation(hybrid_inputs)
        relative_value = compute_relative_value_candidates(hybrid_inputs, sector_rotation=sector_rotation)
        sector_thesis = build_sector_thesis(sector_rotation=sector_rotation, relative_value=relative_value)
        sector_rotation = merge_sector_thesis_into_rotation(sector_rotation, sector_thesis)
        annotated = annotate_event_candidates_with_hybrid(
            records,
            sector_rotation=sector_rotation,
            relative_value=relative_value,
            sector_thesis=sector_thesis,
            inputs=hybrid_inputs,
        )
        shadow_book = finalize_shadow_book(
            annotated,
            sector_rotation=sector_rotation,
            relative_value=relative_value,
            sector_thesis=sector_thesis,
            live_selected_keys=live_selected_keys,
            live_mode=live_mode,
        )
        save_hybrid_runtime_artifacts(sector_rotation, relative_value, shadow_book, sector_thesis=sector_thesis)
        if persist_ledger:
            append_trade_decision_ledger(
                annotated,
                sector_rotation=sector_rotation,
                sector_thesis=sector_thesis,
                live_selected_keys=live_selected_keys,
            )
        self._store_hybrid_runtime_meta(sector_rotation, shadow_book)
        return annotated, sector_rotation, relative_value, shadow_book

    def _store_hybrid_runtime_meta(self, sector_rotation: dict[str, Any], shadow_book: dict[str, Any]) -> None:
        self._last_hybrid_active_sectors = list(sector_rotation.get("active_sectors") or [])[:8]
        self._last_hybrid_shadow_chosen = int(shadow_book.get("shadow_chosen_count", 0) or 0)
        self._last_hybrid_live_selected = int(shadow_book.get("live_selected_count", 0) or 0)
        self._last_hybrid_live_only = list(shadow_book.get("live_only_symbols") or [])[:8]
        self._last_hybrid_shadow_only = list(shadow_book.get("shadow_only_symbols") or [])[:8]
        self._last_hybrid_quote_penalty_total = float(shadow_book.get("quote_fallback_penalty_total", 0.0) or 0.0)
        self._last_hybrid_blocked_histogram = dict(shadow_book.get("blocked_reason_histogram") or {})

    def _build_close_swing_budget_block(
        self,
        row: Dict[str, Any],
        *,
        reason: str,
        note: str,
        decision: str = "blocked",
    ) -> Dict[str, Any]:
        item = dict(row)
        item["close_swing_eligible"] = False
        item["close_swing_decision"] = str(decision or "blocked")
        item["close_swing_reason"] = str(reason or "close_swing_blocked")
        item["close_swing_note"] = _append_close_swing_note(item.get("close_swing_note"), note)
        return item

    def _store_close_swing_budget_runtime_meta(
        self,
        candidates: list[Dict[str, Any]],
        blocked_rows: list[Dict[str, Any]],
    ) -> None:
        adjusted_count = 0
        blocked_hist: dict[str, int] = {}
        for row in candidates or []:
            if isinstance(row, dict) and bool(row.get("close_swing_budget_adjusted")):
                adjusted_count += 1
        for row in blocked_rows or []:
            if not isinstance(row, dict):
                continue
            reason = str(row.get("close_swing_reason") or "")
            if reason not in ONE_SHARE_BUDGET_BLOCK_REASONS:
                continue
            blocked_hist[reason] = int(blocked_hist.get(reason, 0) or 0) + 1
        self._last_close_swing_budget_adjusted_count = adjusted_count
        self._last_close_swing_one_share_blocked_histogram = blocked_hist

    def _apply_quote_fallback_budget_guard(
        self,
        approved_ranked: list[Dict[str, Any]],
        blocked: list[Dict[str, Any]],
        *,
        degraded_limit: int,
        degraded_budget_cap: int,
        quote_source: str,
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
        trimmed: list[Dict[str, Any]] = []
        for idx, row in enumerate(approved_ranked):
            if idx >= degraded_limit:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "deferred"
                item["close_swing_reason"] = "quote_fallback_cycle_limit"
                item["close_swing_note"] = (
                    f"KIS 원본 시세가 불안정해(`{quote_source or 'fallback'}`) 이번 poll에서는 상위 후보만 먼저 처리합니다."
                )
                blocked.append(item)
                continue

            item = dict(row)
            budget_krw = int(item.get("close_swing_budget_krw", 0) or 0)
            min_order_budget_krw = int(item.get("close_swing_min_order_budget_krw", 0) or 0)
            if budget_krw > degraded_budget_cap:
                item["close_swing_budget_krw"] = degraded_budget_cap
                item["close_swing_note"] = _append_close_swing_note(
                    item.get("close_swing_note"),
                    f"KIS 원본 시세가 불안정해(`{quote_source or 'fallback'}`) 예산을 `{degraded_budget_cap:,}원`으로 줄였습니다.",
                )
            final_budget_krw = int(item.get("close_swing_budget_krw", 0) or 0)
            if min_order_budget_krw > 0 and final_budget_krw < min_order_budget_krw:
                blocked.append(
                    self._build_close_swing_budget_block(
                        item,
                        reason="quote_fallback_budget_too_small",
                        note=(
                            f"KIS 원본 시세가 불안정해(`{quote_source or 'fallback'}`) 적용한 예산 `{final_budget_krw:,}원`으로는 "
                            f"현재가 기준 최소 1주 예산 `{min_order_budget_krw:,}원`을 맞출 수 없어 이번 poll은 넘깁니다."
                        ),
                    )
                )
                continue
            trimmed.append(item)
        return trimmed, blocked

    def _maybe_notify_candidate_scores(
        self,
        approved_rows: list[Dict[str, Any]],
        *,
        blocked_count: int,
        remaining_slots: int,
        shadow_book: Optional[dict[str, Any]] = None,
    ) -> None:
        rows = [row for row in (approved_rows or []) if isinstance(row, dict)]
        if not rows:
            return
        digest_rows = [
            {
                "symbol": str(row.get("stock_code") or "").zfill(6),
                "support": int(row.get("close_swing_support_score", 0) or 0),
                "rank": round(float(row.get("close_swing_ranking_score", 0.0) or 0.0), 2),
                "budget": int(row.get("close_swing_budget_krw", 0) or 0),
                "tp": float(row.get("close_swing_take_profit_pct", 0.0) or 0.0),
                "sl": float(row.get("close_swing_stop_loss_pct", 0.0) or 0.0),
                "reason": str(row.get("close_swing_reason") or row.get("close_swing_decision") or ""),
                "wics_status": str(row.get("hybrid_wics_status_label") or ""),
                "wics_dynamic_count": int(row.get("hybrid_wics_dynamic_count", 0) or 0),
                "wics_dynamic_stability": round(float(row.get("hybrid_wics_dynamic_stability", 0.0) or 0.0), 3),
                "wics_penalty": round(float(row.get("hybrid_wics_penalty", 0.0) or 0.0), 2),
            }
            for row in rows[:5]
        ]
        digest = json.dumps(
            {
                "rows": digest_rows,
                "blocked": int(blocked_count or 0),
                "remaining": int(remaining_slots or 0),
                "shadow": int((shadow_book or {}).get("shadow_chosen_count", 0) or 0),
                "live_only": list((shadow_book or {}).get("live_only_symbols") or [])[:3],
                "shadow_only": list((shadow_book or {}).get("shadow_only_symbols") or [])[:3],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        now_ts = time.time()
        if digest == self._last_candidate_notify_digest and (now_ts - self._last_candidate_notify_at) < 300:
            return
        notify_trade_funnel(
            rows,
            blocked_count=blocked_count,
            remaining_slots=remaining_slots,
            shadow_book=shadow_book or {},
        )
        self._last_candidate_notify_digest = digest
        self._last_candidate_notify_at = now_ts

    def _traded_count_for_date(self, event_date: str) -> int:
        if not event_date:
            return 0
        handled = self.state.handled if isinstance(self.state.handled, dict) else {}
        count = 0
        for row in handled.values():
            if not isinstance(row, dict):
                continue
            if str(row.get("event_date") or "") != event_date:
                continue
            if row.get("traded") is True:
                count += 1
        return count

    def _symbol_traded_on_date(self, symbol: str, event_date: str) -> bool:
        if not symbol or not event_date:
            return False
        handled = self.state.handled if isinstance(self.state.handled, dict) else {}
        for row in handled.values():
            if not isinstance(row, dict):
                continue
            if str(row.get("event_date") or "") != event_date:
                continue
            if str(row.get("stock_code") or "").zfill(6) != str(symbol or "").zfill(6):
                continue
            if row.get("traded") is True:
                return True
        return False

    def _traded_budget_for_date(self, event_date: str) -> int:
        return int(self.state.traded_budget_for_date(event_date))

    def _sector_traded_count_for_date(self, sector: str, event_date: str) -> int:
        if not sector or not event_date:
            return 0
        handled = self.state.handled if isinstance(self.state.handled, dict) else {}
        count = 0
        for row in handled.values():
            if not isinstance(row, dict):
                continue
            if str(row.get("event_date") or "") != event_date:
                continue
            if str(row.get("sector") or "") != str(sector):
                continue
            if row.get("traded") is True:
                count += 1
        return count

    def _load_engine_outcome_map(self, records: list[Dict[str, Any]]) -> Dict[tuple[str, str, str], Dict[str, Any]]:
        event_dates = sorted(
            {
                str(row.get("event_date") or "").replace(".", "-")
                for row in (records or [])
                if isinstance(row, dict) and str(row.get("event_date") or "").strip()
            }
        )
        if not event_dates:
            return {}
        try:
            rows = load_event_records(
                days=max(1, len(event_dates)),
                start_date=event_dates[0],
                end_date=event_dates[-1],
            )
        except Exception as exc:
            log.warning("[EVENT-TRADE] engine outcome load failed: %s", exc)
            return {}
        out: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = (
                str(row.get("stock_code") or "").zfill(6),
                str(row.get("rcp_no") or ""),
                str(row.get("event_date") or row.get("record_date") or "").replace(".", "-"),
            )
            if not key[0] or not key[1] or not key[2]:
                continue
            out[key] = row
        return out

    def _maybe_notify_candidate_results(
        self,
        processed_rows: list[Dict[str, Any]],
        *,
        blocked_count: int = 0,
        shadow_book: Optional[dict[str, Any]] = None,
    ) -> None:
        rows = [row for row in (processed_rows or []) if isinstance(row, dict)]
        if not rows:
            return
        outcome_map = self._load_engine_outcome_map([row.get("record") or {} for row in rows])
        notify_rows: list[Dict[str, Any]] = []
        digest_rows: list[Dict[str, Any]] = []
        traded_count = 0
        for item in rows:
            record = dict(item.get("record") or {})
            traded = bool(item.get("traded"))
            traded_count += 1 if traded else 0
            key = (
                str(record.get("stock_code") or "").zfill(6),
                str(record.get("rcp_no") or ""),
                str(record.get("event_date") or "").replace(".", "-"),
            )
            outcome = outcome_map.get(key) or {}
            state_row = self.state.handled.get(_record_key(record), {})
            strategy_name = str(outcome.get("strategy_name") or "")
            reason = str(outcome.get("reason") or state_row.get("reason") or "")
            signal_bias = str(outcome.get("signal_bias") or "")
            result_label = "주문실행" if traded else ("매수없음" if signal_bias in {"neutral", "skip", "negative"} else "미체결")
            note = str(record.get("close_swing_note") or state_row.get("note") or "")
            notify_rows.append(
                {
                    "symbol": key[0],
                    "name": str(record.get("corp_name") or record.get("name") or key[0]),
                    "support": int(record.get("close_swing_support_score", 0) or 0),
                    "rank": float(record.get("close_swing_ranking_score", 0.0) or 0.0),
                    "budget": int(record.get("close_swing_budget_krw", 0) or 0),
                    "hybrid_sector_regime_score": float(record.get("hybrid_sector_regime_score", 0.0) or 0.0),
                    "hybrid_relative_value_score": float(record.get("hybrid_relative_value_score", 0.0) or 0.0),
                    "hybrid_timing_score": float(record.get("hybrid_timing_score", 0.0) or 0.0),
                    "hybrid_final_trade_score": float(record.get("hybrid_final_trade_score", 0.0) or 0.0),
                    "result_label": result_label,
                    "strategy_name": strategy_name or str(record.get("close_swing_decision") or ""),
                    "reason": reason or str(record.get("close_swing_reason") or ""),
                    "note": note,
                }
            )
            digest_rows.append(
                {
                    "symbol": key[0],
                    "result": result_label,
                    "strategy": strategy_name,
                    "reason": reason,
                    "support": int(record.get("close_swing_support_score", 0) or 0),
                    "rank": round(float(record.get("close_swing_ranking_score", 0.0) or 0.0), 2),
                }
            )
        digest = json.dumps(
            {
                "rows": digest_rows,
                "blocked": int(blocked_count or 0),
                "traded": int(traded_count or 0),
                "shadow": int((shadow_book or {}).get("shadow_chosen_count", 0) or 0),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        now_ts = time.time()
        if digest == self._last_result_notify_digest and (now_ts - self._last_result_notify_at) < 300:
            return
        notify_execution_attribution(
            notify_rows,
            total_processed=len(rows),
            traded_count=traded_count,
            blocked_count=blocked_count,
            shadow_book=shadow_book or {},
        )
        self._last_result_notify_digest = digest
        self._last_result_notify_at = now_ts

    def _current_holding_symbols(self) -> set[str]:
        return self._current_holding_profile().get("symbols", set())

    def _current_holding_profile(self) -> Dict[str, Any]:
        try:
            positions = self.engine._fetch_positions(self.engine.broker_live)
        except Exception as exc:
            log.warning("[EVENT-TRADE] fetch positions failed: %s", exc)
            self._last_holding_symbol_count = 0
            self._last_holding_sector_count = 0
            self._last_holding_sector_summary = ""
            return {"symbols": set(), "sector_counts": {}, "sector_symbols": {}}
        alignment = load_latest_context_alignment()
        symbols: set[str] = set()
        sector_counts: dict[str, int] = {}
        sector_symbols: dict[str, list[str]] = {}
        for row in positions or []:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").zfill(6)
            qty = int(row.get("qty") or 0)
            if symbol and qty > 0:
                symbols.add(symbol)
                context = get_symbol_trade_context(symbol, alignment=alignment)
                sector = str(context.get("sector") or "").strip()
                if sector:
                    sector_counts[sector] = sector_counts.get(sector, 0) + 1
                    bucket = sector_symbols.setdefault(sector, [])
                    if symbol not in bucket:
                        bucket.append(symbol)
        self._last_holding_symbol_count = len(symbols)
        self._last_holding_sector_count = len(sector_counts)
        summary_parts = [
            f"{sector}:{count}"
            for sector, count in sorted(
                sector_counts.items(),
                key=lambda item: (-int(item[1] or 0), str(item[0] or "")),
            )[:5]
        ]
        self._last_holding_sector_summary = ", ".join(summary_parts)
        return {
            "symbols": symbols,
            "sector_counts": sector_counts,
            "sector_symbols": sector_symbols,
        }

    def _derive_trade_limits(self, selector_inputs: Optional[Dict[str, Any]]) -> Dict[str, int]:
        briefing = (selector_inputs or {}).get("market_briefing") or {}
        confidence = int(float(((briefing.get("confidence") or {}).get("score")) or 0))
        positioning_mode = str((briefing.get("positioning") or {}).get("mode") or "")
        data_quality_label = str((briefing.get("data_quality") or {}).get("label") or "")
        per_day = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_TRADES_PER_DAY", 3)))
        per_cycle = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_CANDIDATES_PER_CYCLE", 3)))
        min_support = 4

        if positioning_mode == "보수" or confidence < 50:
            per_day = min(per_day, 1)
            per_cycle = min(per_cycle, 1)
            min_support = 8
        elif data_quality_label == "보수적" or confidence < 65:
            per_day = min(per_day, 2)
            per_cycle = min(per_cycle, 2)
            min_support = 6

        return {
            "per_day": int(per_day),
            "per_cycle": int(per_cycle),
            "min_support": int(min_support),
            "confidence": int(confidence),
        }

    def _apply_close_swing_gate(
        self,
        record: Dict[str, Any],
        *,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False)):
            record["close_swing_enabled"] = False
            record["close_swing_eligible"] = True
            record["close_swing_decision"] = "disabled"
            record["close_swing_reason"] = "close_swing_disabled"
            record["close_swing_note"] = "종가 스윙 게이트가 꺼져 있어 기존 공시 이벤트 로직으로만 처리합니다."
            record["close_swing_min_order_budget_krw"] = 0
            record["close_swing_budget_adjusted"] = False
            return record

        context = {
            "sector": record.get("context_sector"),
            "alignment_score": record.get("context_alignment_score"),
            "alignment_label": record.get("context_alignment_label"),
            "market_mode": record.get("context_market_mode"),
            "confidence_score": record.get("context_confidence_score"),
            "note": record.get("context_note"),
        }
        gate = evaluate_close_swing_candidate(
            record,
            broker=self.engine.broker_live,
            inputs=inputs,
            context=context,
        )
        record["close_swing_enabled"] = True
        record["close_swing_eligible"] = bool(gate.get("eligible"))
        record["close_swing_decision"] = str(gate.get("decision") or "")
        record["close_swing_reason"] = str(gate.get("reason") or "")
        record["close_swing_note"] = str(gate.get("note") or "")
        record["close_swing_support_score"] = int(gate.get("support_score", 0) or 0)
        record["close_swing_briefing_action"] = str(gate.get("briefing_action") or "")
        record["close_swing_price_change_pct"] = gate.get("price_change_pct")
        record["close_swing_recovering"] = bool(gate.get("recovering", False))
        record["close_swing_recovery_reasons"] = list(gate.get("recovery_reasons") or [])
        record["close_swing_support_reasons"] = list(gate.get("support_reasons") or [])
        record["close_swing_risk_reasons"] = list(gate.get("risk_reasons") or [])
        record["close_swing_card_score"] = gate.get("card_score")
        record["close_swing_factor_score"] = gate.get("factor_score")
        record["close_swing_active_sources"] = int(gate.get("active_sources", 0) or 0)
        record["close_swing_liquidity_score"] = gate.get("liquidity_score")
        record["close_swing_avg_turnover_20d"] = gate.get("avg_turnover_20d")
        record["close_swing_flow_state_score"] = gate.get("flow_state_score")
        record["close_swing_intraday_edge_score"] = gate.get("intraday_edge_score")
        record["close_swing_ml_pred_return_5d"] = gate.get("ml_pred_return_5d")
        record["close_swing_event_expected_alpha_5d"] = gate.get("event_expected_alpha_5d")
        record["close_swing_event_age_minutes"] = gate.get("event_age_minutes")
        record["close_swing_ranking_score"] = float(gate.get("ranking_score", 0.0) or 0.0)
        record["close_swing_budget_multiplier"] = float(gate.get("budget_multiplier", 0.0) or 0.0)
        record["close_swing_budget_krw"] = int(gate.get("budget_krw", 0) or 0)
        record["close_swing_min_order_budget_krw"] = int(gate.get("min_order_budget_krw", 0) or 0)
        record["close_swing_budget_adjusted"] = bool(gate.get("budget_adjusted", False))
        record["close_swing_take_profit_pct"] = gate.get("take_profit_pct")
        record["close_swing_stop_loss_pct"] = gate.get("stop_loss_pct")
        record["close_swing_stop_grace_min"] = int(gate.get("stop_grace_min", 0) or 0)
        record["close_swing_open_recovery_min"] = int(gate.get("open_recovery_min", 0) or 0)
        record["close_swing_risk_notes"] = list(gate.get("risk_notes") or [])
        return record

    def _filter_and_rank_approved(
        self,
        rows: list[Dict[str, Any]],
        *,
        selector_inputs: Optional[Dict[str, Any]] = None,
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], int]:
        if not rows:
            return [], [], 0

        today = str(rows[0].get("event_date") or _now_kst().strftime("%Y-%m-%d"))
        limit_profile = self._derive_trade_limits(selector_inputs)
        max_per_day = int(limit_profile.get("per_day", 1) or 1)
        max_per_cycle = int(limit_profile.get("per_cycle", 1) or 1)
        min_support = int(limit_profile.get("min_support", 4) or 4)
        max_open_positions = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_OPEN_POSITIONS", 4) or 4))
        max_per_sector_day = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_TRADES_PER_SECTOR_PER_DAY", 1) or 1))
        max_per_sector_cycle = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_CANDIDATES_PER_SECTOR_PER_CYCLE", 1) or 1))
        max_budget_per_day = max(50000, int(getattr(SETTINGS, "CLOSE_SWING_MAX_BUDGET_PER_DAY_KRW", 600000) or 600000))
        traded_today = self._traded_count_for_date(today)
        spent_budget_today = self._traded_budget_for_date(today)
        remaining_slots = max(0, max_per_day - traded_today)
        open_names_per_sector = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_OPEN_NAMES_PER_SECTOR", 1) or 1))
        holding_profile = self._current_holding_profile()
        holding_symbols = set(holding_profile.get("symbols") or set())
        holding_sector_counts = dict(holding_profile.get("sector_counts") or {})
        holding_sector_symbols = dict(holding_profile.get("sector_symbols") or {})
        respect_cash = bool(getattr(SETTINGS, "EVENT_TRADE_RESPECT_AVAILABLE_CASH", True))
        min_cash_buffer = max(0, int(getattr(SETTINGS, "EVENT_TRADE_MIN_CASH_BUFFER_KRW", 50000) or 50000))
        available_cash = int(self._last_broker_cash_krw or 0) if respect_cash and self._last_broker_cash_krw is not None else None

        blocked: list[Dict[str, Any]] = []
        best_by_symbol: dict[str, Dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("stock_code") or "").zfill(6)
            if symbol in holding_symbols:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = "already_holding_symbol"
                item["close_swing_note"] = "이미 보유 중인 종목은 종가 스윙 신규 진입 대상에서 제외합니다."
                blocked.append(item)
                continue
            if self._symbol_traded_on_date(symbol, today):
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = "same_day_symbol_already_traded"
                item["close_swing_note"] = "같은 종목은 같은 날 한 번만 진입해 종가 스윙 리스크를 줄입니다."
                blocked.append(item)
                continue
            support_score = int(row.get("close_swing_support_score", 0) or 0)
            if support_score < min_support:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = f"market_guard_support_{support_score}"
                item["close_swing_note"] = "현재 시장 확신도와 데이터 품질을 감안하면 더 강한 교집합 후보만 진입하는 편이 낫습니다."
                blocked.append(item)
                continue
            existing = best_by_symbol.get(symbol)
            current_rank = float(row.get("close_swing_ranking_score", 0.0) or 0.0)
            existing_rank = float((existing or {}).get("close_swing_ranking_score", 0.0) or 0.0)
            if existing is None or current_rank > existing_rank:
                if existing is not None:
                    replaced = dict(existing)
                    replaced["close_swing_eligible"] = False
                    replaced["close_swing_decision"] = "blocked"
                    replaced["close_swing_reason"] = "same_symbol_lower_ranked"
                    replaced["close_swing_note"] = "같은 종목 공시 중 종가 스윙 적합도가 더 높은 건만 남겼습니다."
                    blocked.append(replaced)
                best_by_symbol[symbol] = row
            else:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = "same_symbol_lower_ranked"
                item["close_swing_note"] = "같은 종목 공시 중 종가 스윙 적합도가 더 높은 건만 남겼습니다."
                blocked.append(item)

        ranked = sorted(
            best_by_symbol.values(),
            key=lambda row: (
                float(row.get("close_swing_ranking_score", 0.0) or 0.0),
                int(row.get("close_swing_support_score", 0) or 0),
                -int(row.get("close_swing_event_age_minutes", 999999) or 999999),
                str(row.get("event_time_hhmm") or ""),
            ),
            reverse=True,
        )
        if len(holding_symbols) >= max_open_positions:
            for row in ranked:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = "max_open_positions_reached"
                item["close_swing_note"] = (
                    f"현재 보유 종목 수 `{len(holding_symbols)}`개가 총 동시 보유 한도 `{max_open_positions}`개에 도달해 "
                    "새 종가 배팅 진입은 보수적으로 막습니다."
                )
                blocked.append(item)
            return [], blocked, remaining_slots
        if remaining_slots <= 0:
            for row in ranked:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = "daily_trade_limit_reached"
                item["close_swing_note"] = "오늘 종가 스윙 진입 한도에 도달해 추가 진입은 보수적으로 막습니다."
                blocked.append(item)
            return [], blocked, remaining_slots

        selected_limit = min(max_per_cycle, remaining_slots)
        selected: list[Dict[str, Any]] = []
        overflow: list[Dict[str, Any]] = []
        spendable_cash = None
        remaining_daily_budget = max(0, max_budget_per_day - spent_budget_today)
        selected_sector_counts: dict[str, int] = {}
        if available_cash is not None:
            spendable_cash = max(0, available_cash - min_cash_buffer)
        for row in ranked:
            if len(selected) >= selected_limit:
                overflow.append(row)
                continue
            sector = str(row.get("context_sector") or row.get("sector") or "").strip()
            if sector:
                held_sector_count = int(holding_sector_counts.get(sector, 0) or 0)
                open_sector_count = held_sector_count + int(selected_sector_counts.get(sector, 0) or 0)
                if open_sector_count >= open_names_per_sector:
                    held_symbols = ", ".join((holding_sector_symbols.get(sector) or [])[:3])
                    item = dict(row)
                    item["close_swing_eligible"] = False
                    item["close_swing_decision"] = "blocked"
                    item["close_swing_reason"] = "same_sector_open_holding_limit_reached"
                    item["close_swing_note"] = (
                        f"이미 보유 중인 섹터 `{sector}` 노출이 `{held_sector_count}`개 있어 "
                        f"총 동시 보유 `{open_names_per_sector}`개 한도를 넘기지 않도록 이번 종가 배팅은 넘깁니다."
                    )
                    if held_symbols:
                        item["close_swing_note"] = (
                            f"{item['close_swing_note']} | 현재 보유 `{held_symbols}`"
                        )
                    blocked.append(item)
                    continue
                traded_sector_count = self._sector_traded_count_for_date(sector, today)
                if traded_sector_count >= max_per_sector_day:
                    item = dict(row)
                    item["close_swing_eligible"] = False
                    item["close_swing_decision"] = "blocked"
                    item["close_swing_reason"] = "same_sector_daily_limit_reached"
                    item["close_swing_note"] = (
                        f"같은 섹터 `{sector}` 종목은 하루 `{max_per_sector_day}`건까지만 진입해 "
                        "종가 배팅의 섹터 쏠림을 줄입니다."
                    )
                    blocked.append(item)
                    continue
                if selected_sector_counts.get(sector, 0) >= max_per_sector_cycle:
                    item = dict(row)
                    item["close_swing_eligible"] = False
                    item["close_swing_decision"] = "deferred"
                    item["close_swing_reason"] = "same_sector_cycle_limit_reached"
                    item["close_swing_note"] = (
                        f"이번 poll에서는 섹터 `{sector}` 상위 후보만 먼저 처리하고 "
                        "같은 섹터의 나머지 후보는 다음 poll에서 다시 봅니다."
                    )
                    blocked.append(item)
                    continue
            budget_krw = int(row.get("close_swing_budget_krw", 0) or 0)
            min_order_budget_krw = int(row.get("close_swing_min_order_budget_krw", 0) or 0)
            if remaining_daily_budget <= 0:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = "daily_budget_limit_reached"
                item["close_swing_note"] = (
                    f"오늘 종가 배팅 총 예산 `{max_budget_per_day:,}원`을 이미 사용해 추가 진입은 막습니다."
                )
                blocked.append(item)
                continue
            adjusted_budget_krw = budget_krw
            daily_budget_capped = False
            cash_budget_capped = False
            if adjusted_budget_krw > remaining_daily_budget:
                adjusted_budget_krw = int((remaining_daily_budget // 10000) * 10000)
                daily_budget_capped = True
                if min_order_budget_krw > 0 and adjusted_budget_krw < min_order_budget_krw:
                    blocked.append(
                        self._build_close_swing_budget_block(
                            row,
                            reason="daily_budget_below_one_share",
                            note=(
                                f"오늘 남은 종가 배팅 예산 `{remaining_daily_budget:,}원` 기준으로는 "
                                f"현재가 1주 예산 `{min_order_budget_krw:,}원`을 맞출 수 없어 이번 후보는 넘깁니다."
                            ),
                        )
                    )
                    continue
            if adjusted_budget_krw < 50000:
                item = dict(row)
                item["close_swing_eligible"] = False
                item["close_swing_decision"] = "blocked"
                item["close_swing_reason"] = "daily_budget_too_small"
                item["close_swing_note"] = (
                    f"오늘 남은 종가 배팅 예산 `{remaining_daily_budget:,}원`이 최소 주문 규모보다 작아 "
                    "이번 후보는 넘깁니다."
                )
                blocked.append(item)
                continue
            if spendable_cash is not None and adjusted_budget_krw > 0 and adjusted_budget_krw > spendable_cash:
                adjusted_budget_krw = int((spendable_cash // 10000) * 10000)
                cash_budget_capped = True
                if min_order_budget_krw > 0 and adjusted_budget_krw < min_order_budget_krw:
                    blocked.append(
                        self._build_close_swing_budget_block(
                            row,
                            reason="available_cash_below_one_share",
                            note=(
                                f"현재 가용 현금 `{available_cash:,}원`에서 최소 버퍼 `{min_cash_buffer:,}원`을 남기면 "
                                f"현재가 1주 예산 `{min_order_budget_krw:,}원`을 감당하기 어렵습니다."
                            ),
                        )
                    )
                    continue
                if adjusted_budget_krw < 50000:
                    item = dict(row)
                    item["close_swing_eligible"] = False
                    item["close_swing_decision"] = "blocked"
                    item["close_swing_reason"] = "insufficient_available_cash"
                    item["close_swing_note"] = (
                        f"현재 가용 현금 `{available_cash:,}원`에서 최소 버퍼 `{min_cash_buffer:,}원`을 남기면 "
                        f"이번 후보 예산 `{budget_krw:,}원`을 감당하기 어렵습니다."
                    )
                    blocked.append(item)
                    continue
            item = dict(row)
            if adjusted_budget_krw != budget_krw:
                item["close_swing_budget_krw"] = int(adjusted_budget_krw)
                if daily_budget_capped:
                    item["close_swing_note"] = _append_close_swing_note(
                        item.get("close_swing_note"),
                        f"일일 총 예산 한도에 맞춰 예산을 `{budget_krw:,}원`에서 `{adjusted_budget_krw:,}원`으로 줄였습니다.",
                    )
                if cash_budget_capped:
                    item["close_swing_note"] = _append_close_swing_note(
                        item.get("close_swing_note"),
                        f"가용 현금 범위에 맞춰 예산을 `{budget_krw:,}원`에서 `{adjusted_budget_krw:,}원`으로 조정했습니다.",
                    )
            selected.append(item)
            if sector:
                selected_sector_counts[sector] = selected_sector_counts.get(sector, 0) + 1
            remaining_daily_budget = max(0, remaining_daily_budget - adjusted_budget_krw)
            if spendable_cash is not None and adjusted_budget_krw > 0:
                spendable_cash = max(0, spendable_cash - adjusted_budget_krw)
        for row in overflow:
            item = dict(row)
            item["close_swing_eligible"] = False
            item["close_swing_decision"] = "deferred"
            item["close_swing_reason"] = "rank_below_cycle_cut"
            item["close_swing_note"] = "이번 poll에서는 상위 종가 스윙 후보만 먼저 처리하고, 나머지는 다음 poll에서 다시 봅니다."
            blocked.append(item)
        return selected, blocked, remaining_slots

    def _is_transient_close_swing_block(self, record: Dict[str, Any]) -> bool:
        decision = str(record.get("close_swing_decision") or "")
        reason = str(record.get("close_swing_reason") or "")
        if decision == "deferred":
            return True
        transient_prefixes = (
            "failed_recovery_",
            "weak_quant_support_",
            "overextended_",
            "market_guard_support_",
            "watch_rebound",
            "broker_unready",
        )
        return reason.startswith(transient_prefixes)

    def _mark_blocked_candidate(self, record: Dict[str, Any]) -> None:
        key = _record_key(record)
        reason = str(record.get("close_swing_reason") or "close_swing_blocked")
        note = str(record.get("close_swing_note") or "")
        transient = self._is_transient_close_swing_block(record)
        attempts = self.state.attempts_for_key(key)
        max_retry = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_RETRY_PER_RECORD", 3) or 3))
        if transient and attempts >= max_retry:
            transient = False
            reason = f"retry_limit_reached:{reason}"
            note = (
                f"{note} | 같은 공시를 `{max_retry}`회 재평가했지만 조건이 좋아지지 않아 오늘은 더 보지 않습니다."
            ).strip(" |")
        if transient:
            self.state.mark_retry(
                key,
                record,
                error=reason,
                decision=str(record.get("close_swing_decision") or "close_swing_retry"),
                note=note,
                recheck_after_sec=int(getattr(SETTINGS, "CLOSE_SWING_RECHECK_COOLDOWN_SEC", 60) or 60),
            )
        else:
            self.state.mark_processed(
                key,
                record,
                traded=False,
                decision="close_swing_blocked",
                reason=reason,
                note=note,
            )
        self.state.save()
        if transient:
            log.info(
                "[EVENT-TRADE] transient block -> will recheck %s in %ss | %s",
                str(record.get("stock_code") or "").zfill(6),
                int(getattr(SETTINGS, "CLOSE_SWING_RECHECK_COOLDOWN_SEC", 60) or 60),
                reason,
            )
            return
        notify_trade_skip(
            str(record.get("stock_code") or "").zfill(6),
            f"[close-swing] {reason} | {note}".strip(" |"),
            0,
            context=self._trade_context_from_record(record),
        )

    def _process_record(self, record: Dict[str, Any]) -> bool:
        key = _record_key(record)
        stock_code = str(record.get("stock_code") or "").zfill(6)
        title = str(record.get("title") or "")
        rcp_no = str(record.get("rcp_no") or "")
        corp_name = str(record.get("corp_name") or "")
        event_date = str(record.get("event_date") or "")
        event_time_hhmm = str(record.get("event_time_hhmm") or "")

        log.info(
            "[EVENT-TRADE] evaluating %s %s | %s | %s",
            stock_code,
            rcp_no,
            record.get("event_type"),
            title,
        )
        self.engine._set_runtime_trade_profile(
            stock_code,
            {
                "budget_krw": int(record.get("close_swing_budget_krw", 0) or 0),
                "budget_multiplier": float(record.get("close_swing_budget_multiplier", 0.0) or 0.0),
                "support_score": int(record.get("close_swing_support_score", 0) or 0),
                "ranking_score": float(record.get("close_swing_ranking_score", 0.0) or 0.0),
                "take_profit_pct": float(record.get("close_swing_take_profit_pct", 0.0) or 0.0),
                "stop_loss_pct": float(record.get("close_swing_stop_loss_pct", 0.0) or 0.0),
                "stop_grace_min": int(record.get("close_swing_stop_grace_min", 0) or 0),
                "open_recovery_min": int(record.get("close_swing_open_recovery_min", 0) or 0),
                "risk_notes": list(record.get("close_swing_risk_notes") or []),
                "decision": str(record.get("close_swing_decision") or ""),
                "reason": str(record.get("close_swing_reason") or ""),
                "note": str(record.get("close_swing_note") or ""),
                "hybrid_sector_regime_score": float(record.get("hybrid_sector_regime_score", 0.0) or 0.0),
                "hybrid_relative_value_score": float(record.get("hybrid_relative_value_score", 0.0) or 0.0),
                "hybrid_timing_score": float(record.get("hybrid_timing_score", 0.0) or 0.0),
                "hybrid_final_trade_score": float(record.get("hybrid_final_trade_score", 0.0) or 0.0),
                "hybrid_shadow_decision": str(record.get("hybrid_shadow_decision") or ""),
                "hybrid_blocked_reason_code": str(record.get("hybrid_blocked_reason_code") or ""),
                "quote_source": str(record.get("hybrid_quote_source") or ""),
                "entry_style": "close_bet",
                "entry_date": event_date or _now_kst().strftime("%Y-%m-%d"),
                "skip_same_day_eod_liquidate": True,
            },
        )
        try:
            traded = self.engine._process_one(
                broker=self.engine.broker_live,
                stock_code=stock_code,
                rcp_no=rcp_no,
                title=title,
                src="EVENT_LOG_WATCH",
                allow_trade=bool(getattr(SETTINGS, "ENABLE_AUTO_TRADE", False)),
                corp_name=corp_name,
                event_date=event_date,
                event_time_hhmm=event_time_hhmm,
            )
            engine_reason = "trade_executed" if traded else "processed_no_trade"
            self.state.mark_processed(
                key,
                record,
                traded=traded,
                decision=str(record.get("close_swing_decision") or "engine_processed"),
                reason=engine_reason,
                note=str(record.get("close_swing_note") or ""),
            )
            self.state.save()
            self._write_runtime_status(
                "processed",
                last_record=record,
                processed_count=1,
                last_result="traded" if traded else "processed_no_trade",
            )
            return traded
        except Exception as exc:
            self.state.mark_retry(key, record, error=str(exc))
            self.state.save()
            self._write_runtime_status(
                "record_error",
                last_record=record,
                processed_count=1,
                last_result="retry",
                error=str(exc),
            )
            log.exception("[EVENT-TRADE-FAIL] %s %s", stock_code, rcp_no)
            return False
        finally:
            self.engine._clear_runtime_trade_profile(stock_code)

    def run_forever(self) -> None:
        log.info(
            "event-log trade watcher started | window=%s~%s | poll=%ss | types=%s | sources=%s",
            self.window_start.strftime("%H:%M"),
            self.window_end.strftime("%H:%M"),
            self.poll_sec,
            ",".join(sorted(self.allowed_types)),
            ",".join(sorted(self.allowed_sources)),
        )
        self._write_runtime_status("started", now=_now_kst())
        while True:
            now = _now_kst()
            try:
                if not self._within_trade_window(now):
                    self._bootstrap_position_monitors_if_needed(now)
                    self._write_runtime_status("waiting_window", now=now)
                    if time.time() - self._last_idle_log_at >= 900:
                        log.info(
                            "[EVENT-TRADE] waiting for trade window %s~%s (now=%s)",
                            self.window_start.strftime("%H:%M"),
                            self.window_end.strftime("%H:%M"),
                            now.strftime("%H:%M:%S"),
                        )
                        self._last_idle_log_at = time.time()
                    time.sleep(self.poll_sec)
                    continue

                candidates = self._load_candidates()
                selector_inputs = load_close_swing_inputs() if bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False)) else None
                broker_ready, broker_ready_note = self._probe_broker_ready()
                today_stop_sell_count = self._load_today_stop_sell_count()
                quote_primary_ok, quote_source = self._load_quote_probe_state()
                all_gated: list[Dict[str, Any]] = []
                approved: list[Dict[str, Any]] = []
                blocked: list[Dict[str, Any]] = []
                for record in candidates:
                    gated = self._apply_close_swing_gate(record, inputs=selector_inputs)
                    all_gated.append(gated)
                    if gated.get("close_swing_eligible", True):
                        approved.append(gated)
                    else:
                        blocked.append(gated)
                approved_ranked, extra_blocked, remaining_slots = self._filter_and_rank_approved(
                    approved,
                    selector_inputs=selector_inputs,
                )
                blocked.extend(extra_blocked)
                aligned_count = sum(1 for row in candidates if int(row.get("context_alignment_score", 0) or 0) > 0)
                risk_count = sum(1 for row in candidates if int(row.get("context_alignment_score", 0) or 0) < 0)
                sector_rotation: dict[str, Any] = {}
                relative_value: dict[str, Any] = {}
                shadow_book: dict[str, Any] = {}
                if bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)):
                    annotated_all, sector_rotation, relative_value, _ = self._apply_hybrid_runtime(
                        all_gated,
                        selector_inputs=selector_inputs,
                        live_selected_keys=set(),
                        live_mode="shadow" if bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)) else "hybrid",
                        persist_ledger=False,
                    )
                    by_key = {_record_key(row): row for row in annotated_all}
                    candidates = [by_key.get(_record_key(row), row) for row in all_gated]
                    approved_ranked = [by_key.get(_record_key(row), row) for row in approved_ranked]
                    blocked = [by_key.get(_record_key(row), row) for row in blocked]
                    if not bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)):
                        hybrid_selected: list[Dict[str, Any]] = []
                        hybrid_blocked: list[Dict[str, Any]] = []
                        for row in sorted(
                            approved_ranked,
                            key=lambda item: float(item.get("hybrid_final_trade_score", 0.0) or 0.0),
                            reverse=True,
                        ):
                            if bool(row.get("hybrid_shadow_pass")):
                                hybrid_selected.append(row)
                                continue
                            item = dict(row)
                            item["close_swing_eligible"] = False
                            item["close_swing_decision"] = "blocked"
                            item["close_swing_reason"] = str(item.get("hybrid_blocked_reason_code") or "hybrid_gate_blocked")
                            item["close_swing_note"] = (
                                f"{str(item.get('close_swing_note') or '').strip()} | "
                                "하이브리드 섹터/상대가치/타이밍 3단 필터를 모두 통과하지 못했습니다."
                            ).strip(" |")
                            hybrid_blocked.append(item)
                        approved_ranked = hybrid_selected
                        blocked.extend(hybrid_blocked)
                max_stop_sells = max(0, int(getattr(SETTINGS, "CLOSE_SWING_MAX_STOP_SELLS_PER_DAY", 1) or 1))
                if approved_ranked and max_stop_sells > 0 and today_stop_sell_count >= max_stop_sells:
                    for row in approved_ranked:
                        item = dict(row)
                        item["close_swing_eligible"] = False
                        item["close_swing_decision"] = "blocked"
                        item["close_swing_reason"] = "daily_stop_sell_limit_reached"
                        item["close_swing_note"] = (
                            f"오늘 손절 매도 `{today_stop_sell_count}`건이 이미 발생해 추가 종가 배팅 진입은 중단합니다."
                        )
                        blocked.append(item)
                    approved_ranked = []
                if (
                    approved_ranked
                    and bool(getattr(SETTINGS, "CLOSE_SWING_DEGRADE_ON_QUOTE_FALLBACK", True))
                    and quote_primary_ok is False
                ):
                    degraded_limit = max(1, int(getattr(SETTINGS, "CLOSE_SWING_DEGRADED_MAX_CANDIDATES_PER_CYCLE", 1) or 1))
                    degraded_budget_cap = max(50000, int(getattr(SETTINGS, "CLOSE_SWING_DEGRADED_MAX_BUDGET_PER_TRADE_KRW", 200000) or 200000))
                    approved_ranked, blocked = self._apply_quote_fallback_budget_guard(
                        approved_ranked,
                        blocked,
                        degraded_limit=degraded_limit,
                        degraded_budget_cap=degraded_budget_cap,
                        quote_source=quote_source,
                    )
                if approved_ranked and not broker_ready:
                    for row in approved_ranked:
                        item = dict(row)
                        item["close_swing_eligible"] = False
                        item["close_swing_decision"] = "deferred"
                        item["close_swing_reason"] = "broker_unready"
                        item["close_swing_note"] = "주문 경로 확인이 실패해 이번 poll은 건너뛰고 같은 창 안에서 다시 확인합니다."
                        blocked.append(item)
                    self._maybe_notify_broker_unready(
                        approved_count=len(approved_ranked),
                        reason=broker_ready_note,
                    )
                    approved_ranked = []
                self._store_close_swing_budget_runtime_meta(
                    candidates if bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)) else all_gated,
                    blocked,
                )
                live_selected_keys = {
                    f"{str(row.get('stock_code') or '').zfill(6)}:{str(row.get('rcp_no') or '')}"
                    for row in approved_ranked
                }
                if bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)):
                    if not sector_rotation:
                        annotated_all, sector_rotation, relative_value, _ = self._apply_hybrid_runtime(
                            all_gated,
                            selector_inputs=selector_inputs,
                            live_selected_keys=set(),
                            live_mode="shadow" if bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)) else "hybrid",
                            persist_ledger=False,
                        )
                        candidates = annotated_all
                    sector_thesis = dict(sector_rotation.get("sector_thesis") or {})
                    shadow_book = finalize_shadow_book(
                        candidates,
                        sector_rotation=sector_rotation,
                        relative_value=relative_value,
                        sector_thesis=sector_thesis,
                        live_selected_keys=live_selected_keys,
                        live_mode="shadow" if bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)) else "hybrid",
                    )
                    save_hybrid_runtime_artifacts(sector_rotation, relative_value, shadow_book, sector_thesis=sector_thesis)
                    append_trade_decision_ledger(
                        candidates,
                        sector_rotation=sector_rotation,
                        sector_thesis=sector_thesis,
                        live_selected_keys=live_selected_keys,
                    )
                    self._store_hybrid_runtime_meta(sector_rotation, shadow_book)
                if candidates:
                    log.info(
                        "[EVENT-TRADE] candidates=%d | approved=%d | blocked=%d | aligned=%d | risk=%d | remaining_slots=%d | broker_ready=%s",
                        len(candidates),
                        len(approved_ranked),
                        len(blocked),
                        aligned_count,
                        risk_count,
                        remaining_slots,
                        broker_ready_note if broker_ready else f"NO:{broker_ready_note}",
                    )
                    self._write_runtime_status(
                        "candidates_found",
                        now=now,
                        candidate_count=len(candidates),
                        candidate_aligned_count=aligned_count,
                        candidate_risk_count=risk_count,
                        candidate_approved_count=len(approved_ranked),
                        candidate_blocked_count=len(blocked),
                    )
                    if shadow_book:
                        self._maybe_notify_sector_thesis(
                            sector_rotation,
                            relative_value,
                            shadow_book,
                            sector_thesis=dict(sector_rotation.get("sector_thesis") or {}),
                        )
                    self._maybe_notify_candidate_scores(
                        approved_ranked,
                        blocked_count=len(blocked),
                        remaining_slots=remaining_slots,
                        shadow_book=shadow_book,
                    )
                else:
                    self._write_runtime_status(
                        "no_candidates",
                        now=now,
                        candidate_count=0,
                        candidate_aligned_count=0,
                        candidate_risk_count=0,
                        candidate_approved_count=0,
                        candidate_blocked_count=0,
                    )
                for record in blocked:
                    if str(record.get("close_swing_decision") or "") == "deferred":
                        log.info(
                            "[EVENT-TRADE] deferred %s %s | %s | %s",
                            str(record.get("stock_code") or "").zfill(6),
                            str(record.get("event_type") or ""),
                            str(record.get("close_swing_reason") or ""),
                            str(record.get("close_swing_note") or ""),
                        )
                        continue
                    log.info(
                        "[EVENT-TRADE] blocked %s %s | %s | %s",
                        str(record.get("stock_code") or "").zfill(6),
                        str(record.get("event_type") or ""),
                        str(record.get("close_swing_reason") or ""),
                        str(record.get("close_swing_note") or ""),
                    )
                    self._mark_blocked_candidate(record)
                    self._write_runtime_status(
                        "candidate_blocked",
                        now=now,
                        candidate_count=len(candidates),
                        candidate_aligned_count=aligned_count,
                        candidate_risk_count=risk_count,
                        candidate_approved_count=len(approved_ranked),
                        candidate_blocked_count=len(blocked),
                        processed_count=1,
                        last_record=record,
                        last_result="blocked_by_close_swing",
                    )
                processed_rows: list[Dict[str, Any]] = []
                for record in approved_ranked:
                    traded = self._process_record(record)
                    processed_rows.append({"record": record, "traded": traded})
                if processed_rows:
                    self._maybe_notify_candidate_results(
                        processed_rows,
                        blocked_count=len(blocked),
                        shadow_book=shadow_book,
                    )
                    if bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)):
                        try:
                            save_hybrid_study_pack(build_hybrid_study_pack())
                        except Exception as exc:
                            log.warning("[EVENT-TRADE] hybrid study pack refresh failed: %s", exc)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                self._write_runtime_status("loop_error", now=now, error=str(exc))
                log.exception("event-log trade watcher loop failed")

            time.sleep(self.poll_sec)
