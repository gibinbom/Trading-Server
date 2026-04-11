import json
import os
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from config import SETTINGS
from signals.wics_universe import load_effective_wics_sector_meta


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
EVENT_DIR = os.path.join(BASE_DIR, "events")
EVENT_STATE_PATH = os.path.join(EVENT_DIR, "main_trade_state.json")
DEFAULT_SLACK_DELIVERY_LOG_PATH = os.path.join(LOG_DIR, "slack_delivery.jsonl")
DEFAULT_SLACK_FALLBACK_LOG_PATH = os.path.join(LOG_DIR, "slack_fallback.jsonl")
DEFAULT_TRADE_ALERT_AUDIT_PATH = os.path.join(LOG_DIR, "trade_alert_audit.jsonl")
TRADE_RUNTIME_STATUS_PATH = os.path.join(EVENT_DIR, "trade_runtime_latest.json")
SECTOR_ROTATION_PATH = os.path.join(BASE_DIR, "runtime", "sector_rotation_latest.json")
RELATIVE_VALUE_PATH = os.path.join(BASE_DIR, "runtime", "relative_value_candidates_latest.json")
HYBRID_SHADOW_BOOK_PATH = os.path.join(BASE_DIR, "runtime", "hybrid_shadow_book_latest.json")
TRADE_DECISION_LEDGER_PATH = os.path.join(BASE_DIR, "runtime", "trade_decision_ledger_latest.jsonl")
SECTOR_THESIS_PATH = os.path.join(BASE_DIR, "runtime", "sector_thesis_latest.json")
WICS_UNIVERSE_PATH = os.path.join(BASE_DIR, "signals", "reports", "wics_effective_universe_latest.json")
FAIR_VALUE_SNAPSHOT_PATH = os.path.join(BASE_DIR, "valuation", "fair_value_snapshot_latest.json")
ENGINE_PATH = os.path.join(BASE_DIR, "engine.py")


def _now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _file_mtime(path: str) -> Optional[datetime]:
    if not path or not os.path.exists(path):
        return None
    return datetime.fromtimestamp(os.path.getmtime(path))


def _age_minutes(path: str) -> Optional[int]:
    mtime = _file_mtime(path)
    if mtime is None:
        return None
    return int((datetime.now() - mtime).total_seconds() // 60)


def _fmt_file_status(path: str) -> str:
    if not os.path.exists(path):
        return "없음"
    age = _age_minutes(path)
    stamp = _file_mtime(path)
    if stamp is None:
        return "정상"
    return f"정상 | 경과 {age}m | 수정시각 {stamp.isoformat(timespec='seconds')}"


def _fmt_optional_stability(count: Any, value: Any) -> str:
    try:
        count_int = int(count or 0)
    except Exception:
        count_int = 0
    try:
        value_float = float(value or 0.0)
    except Exception:
        value_float = 0.0
    if count_int <= 0:
        return "-"
    return f"{value_float:.2f}"


def _read_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _read_last_jsonl(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fp:
            lines = [line.strip() for line in fp if line.strip()]
        if not lines:
            return None
        data = json.loads(lines[-1])
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _read_recent_jsonl(path: str, *, minutes: int = 60) -> list[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    cutoff = datetime.now().timestamp() - (max(1, minutes) * 60)
    rows: list[Dict[str, Any]] = []
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
                try:
                    ts = datetime.fromisoformat(timestamp).timestamp()
                except Exception:
                    continue
                if ts >= cutoff:
                    rows.append(row)
    except Exception:
        return []
    return rows


def _extract_trade_state() -> Tuple[int, int, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    state = _read_json(EVENT_STATE_PATH)
    handled = state.get("handled", {})
    if not isinstance(handled, dict):
        return 0, 0, None, None

    items = list(handled.values())
    traded_items = [item for item in items if isinstance(item, dict) and item.get("traded") is True]

    def _sort_key(item: Dict[str, Any]) -> str:
        return str(item.get("attempted_at") or "")

    last_attempt = max(items, key=_sort_key) if items else None
    last_traded = max(traded_items, key=_sort_key) if traded_items else None
    return len(items), len(traded_items), last_attempt, last_traded


def _traded_budget_for_today() -> int:
    state = _read_json(EVENT_STATE_PATH)
    handled = state.get("handled", {})
    if not isinstance(handled, dict):
        return 0
    today = datetime.now().strftime("%Y-%m-%d")
    total = 0
    for row in handled.values():
        if not isinstance(row, dict):
            continue
        if str(row.get("event_date") or "") != today:
            continue
        if row.get("traded") is not True:
            continue
        total += int(row.get("budget_krw") or 0)
    return int(total)


def _fmt_record(record: Optional[Dict[str, Any]], fields: list[str]) -> str:
    if not record:
        return "-"
    parts = []
    for field in fields:
        value = record.get(field)
        if value in (None, ""):
            continue
        parts.append(f"{field}={value}")
    return " | ".join(parts) if parts else "-"


def _detect_engine_broker_mode() -> Dict[str, Any]:
    payload = {
        "hardcoded_live": False,
        "follows_settings": False,
        "hardcoded_is_virtual": None,
        "hardcoded_dry_run": None,
        "note": "",
    }
    if not os.path.exists(ENGINE_PATH):
        payload["note"] = "engine.py missing"
        return payload
    try:
        with open(ENGINE_PATH, "r", encoding="utf-8") as fp:
            text = fp.read()
        helper_call = re.search(
            r"self\.broker_live\s*=\s*build_kis_broker_from_settings\s*\((?P<body>.*?)\n\s*\)",
            text,
            flags=re.DOTALL,
        )
        if helper_call:
            payload["follows_settings"] = True
            payload["note"] = "engine broker_live follows SETTINGS via build_kis_broker_from_settings"
            return payload

        broker_block = re.search(
            r"self\.broker_live\s*=\s*KISBroker\s*\((?P<body>.*?)\n\s*\)",
            text,
            flags=re.DOTALL,
        )
        if not broker_block:
            payload["note"] = "broker_live init not found"
            return payload
        body = broker_block.group("body")
        match_virtual = re.search(r"is_virtual\s*=\s*(True|False)", body)
        match_dry_run = re.search(r"dry_run\s*=\s*(True|False)", body)
        follows_settings_virtual = 'is_virtual=broker_is_virtual' in body or 'is_virtual = broker_is_virtual' in body
        follows_settings_dry = 'dry_run=broker_dry_run' in body or 'dry_run = broker_dry_run' in body
        if match_virtual:
            payload["hardcoded_is_virtual"] = match_virtual.group(1) == "True"
        if match_dry_run:
            payload["hardcoded_dry_run"] = match_dry_run.group(1) == "True"
        if follows_settings_virtual and follows_settings_dry:
            payload["follows_settings"] = True
            payload["note"] = "engine broker_live follows SETTINGS.KIS_IS_VIRTUAL / SETTINGS.DRY_RUN"
            return payload
        payload["hardcoded_live"] = payload["hardcoded_is_virtual"] is False and payload["hardcoded_dry_run"] is False
        if payload["hardcoded_live"]:
            payload["note"] = "engine broker_live is hardcoded to live mode"
        elif payload["hardcoded_is_virtual"] is not None or payload["hardcoded_dry_run"] is not None:
            payload["note"] = "engine broker_live uses explicit constants"
        else:
            payload["note"] = "engine broker_live mode could not be parsed"
        return payload
    except Exception as exc:
        payload["note"] = f"engine parse failed: {exc}"
        return payload


def _pick_wics_focus_sector(*candidates: Any, meta: Dict[str, Dict[str, Any]]) -> str:
    for value in candidates:
        if isinstance(value, list):
            for item in value:
                text = str(item or "").strip()
                if text and text in meta:
                    return text
            continue
        text = str(value or "").strip()
        if text and text in meta:
            return text
    return ""


def load_trade_runtime_health() -> Dict[str, Any]:
    delivery_path = getattr(SETTINGS, "SLACK_DELIVERY_LOG_PATH", DEFAULT_SLACK_DELIVERY_LOG_PATH)
    fallback_path = getattr(SETTINGS, "SLACK_FALLBACK_LOG_PATH", DEFAULT_SLACK_FALLBACK_LOG_PATH)
    trade_audit_path = getattr(SETTINGS, "TRADE_ALERT_AUDIT_PATH", DEFAULT_TRADE_ALERT_AUDIT_PATH)

    handled_count, traded_count, last_attempt, last_traded = _extract_trade_state()
    last_delivery = _read_last_jsonl(delivery_path) or {}
    last_fallback = _read_last_jsonl(fallback_path) or {}
    last_trade_audit = _read_last_jsonl(trade_audit_path) or {}
    runtime_status = _read_json(TRADE_RUNTIME_STATUS_PATH) or {}
    sector_rotation = _read_json(SECTOR_ROTATION_PATH) or {}
    relative_value = _read_json(RELATIVE_VALUE_PATH) or {}
    shadow_book = _read_json(HYBRID_SHADOW_BOOK_PATH) or {}
    sector_thesis = _read_json(SECTOR_THESIS_PATH) or {}
    wics_universe = _read_json(WICS_UNIVERSE_PATH) or {}
    fair_value_summary = _read_json(FAIR_VALUE_SNAPSHOT_PATH) or {}
    wics_sector_meta = load_effective_wics_sector_meta()
    decision_rows = _read_recent_jsonl(TRADE_DECISION_LEDGER_PATH, minutes=60)
    engine_mode = _detect_engine_broker_mode()
    blocked_hist = Counter(
        str(row.get("blocked_reason_code") or "")
        for row in decision_rows
        if str(row.get("blocked_reason_code") or "").strip()
    )
    effective_hybrid_active_sectors = list(runtime_status.get("hybrid_active_sectors") or [])
    if not effective_hybrid_active_sectors:
        effective_hybrid_active_sectors = list(sector_rotation.get("active_sectors") or [])

    effective_hybrid_shadow_chosen_count = int(runtime_status.get("hybrid_shadow_chosen_count", 0) or 0)
    if effective_hybrid_shadow_chosen_count <= 0:
        effective_hybrid_shadow_chosen_count = int(shadow_book.get("shadow_chosen_count", 0) or 0)

    effective_hybrid_live_selected_count = int(runtime_status.get("hybrid_live_selected_count", 0) or 0)
    if effective_hybrid_live_selected_count <= 0:
        effective_hybrid_live_selected_count = int(shadow_book.get("live_selected_count", 0) or 0)

    effective_hybrid_live_only_symbols = list(runtime_status.get("hybrid_live_only_symbols") or [])
    if not effective_hybrid_live_only_symbols:
        effective_hybrid_live_only_symbols = list(shadow_book.get("live_only_symbols") or [])

    effective_hybrid_shadow_only_symbols = list(runtime_status.get("hybrid_shadow_only_symbols") or [])
    if not effective_hybrid_shadow_only_symbols:
        effective_hybrid_shadow_only_symbols = list(shadow_book.get("shadow_only_symbols") or [])

    effective_hybrid_quote_penalty_total = float(runtime_status.get("hybrid_quote_penalty_total", 0.0) or 0.0)
    if effective_hybrid_quote_penalty_total <= 0.0:
        effective_hybrid_quote_penalty_total = float(shadow_book.get("quote_fallback_penalty_total", 0.0) or 0.0)

    effective_blocked_hist = dict(blocked_hist.most_common(8))
    if not effective_blocked_hist:
        effective_blocked_hist = dict(shadow_book.get("blocked_reason_histogram") or {})

    sector_rotation_top_sector = str(((sector_rotation.get("top_sectors") or [{}])[0]).get("sector") or "")
    sector_thesis_top_sector = str(((sector_thesis.get("top_sectors") or [{}])[0]).get("sector") or "")
    sector_thesis_top_label = str(((sector_thesis.get("top_sectors") or [{}])[0]).get("final_label") or "")
    runtime_last_record_sector = str((runtime_status.get("last_record") or {}).get("sector") or "")
    wics_focus_sector = _pick_wics_focus_sector(
        sector_thesis_top_sector,
        runtime_last_record_sector,
        sector_rotation_top_sector,
        effective_hybrid_active_sectors,
        meta=wics_sector_meta,
    )
    wics_focus_meta = dict(wics_sector_meta.get(wics_focus_sector) or {})
    wics_summary = dict((wics_universe.get("summary") or {}))

    payload = {
        "auto_trade_enabled": bool(getattr(SETTINGS, "ENABLE_AUTO_TRADE", False)),
        "close_swing_enabled": bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False)),
        "close_swing_one_share_budget_floor_enabled": bool(
            runtime_status.get(
                "close_swing_one_share_budget_floor_enabled",
                getattr(SETTINGS, "CLOSE_SWING_ENABLE", False),
            )
        ),
        "hybrid_rotation_enabled": bool(getattr(SETTINGS, "HYBRID_ROTATION_ENABLE", False)),
        "hybrid_shadow_only": bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)),
        "hybrid_active_sector_min_score": int(getattr(SETTINGS, "HYBRID_ACTIVE_SECTOR_MIN_SCORE", 0) or 0),
        "hybrid_relative_value_min_score": int(getattr(SETTINGS, "HYBRID_RELATIVE_VALUE_MIN_SCORE", 0) or 0),
        "hybrid_wics_hard_min_days": int(getattr(SETTINGS, "HYBRID_WICS_HARD_MIN_DAYS", 0) or 0),
        "close_swing_max_trades_per_day": int(getattr(SETTINGS, "CLOSE_SWING_MAX_TRADES_PER_DAY", 0) or 0),
        "close_swing_max_candidates_per_cycle": int(getattr(SETTINGS, "CLOSE_SWING_MAX_CANDIDATES_PER_CYCLE", 0) or 0),
        "close_swing_max_open_positions": int(getattr(SETTINGS, "CLOSE_SWING_MAX_OPEN_POSITIONS", 0) or 0),
        "close_swing_max_trades_per_sector_per_day": int(getattr(SETTINGS, "CLOSE_SWING_MAX_TRADES_PER_SECTOR_PER_DAY", 0) or 0),
        "close_swing_max_candidates_per_sector_per_cycle": int(getattr(SETTINGS, "CLOSE_SWING_MAX_CANDIDATES_PER_SECTOR_PER_CYCLE", 0) or 0),
        "close_swing_max_open_names_per_sector": int(getattr(SETTINGS, "CLOSE_SWING_MAX_OPEN_NAMES_PER_SECTOR", 0) or 0),
        "close_swing_max_budget_per_day_krw": int(getattr(SETTINGS, "CLOSE_SWING_MAX_BUDGET_PER_DAY_KRW", 0) or 0),
        "close_swing_max_retry_per_record": int(getattr(SETTINGS, "CLOSE_SWING_MAX_RETRY_PER_RECORD", 0) or 0),
        "close_swing_require_price_signal": bool(getattr(SETTINGS, "CLOSE_SWING_REQUIRE_PRICE_SIGNAL", False)),
        "close_swing_max_stop_sells_per_day": int(getattr(SETTINGS, "CLOSE_SWING_MAX_STOP_SELLS_PER_DAY", 0) or 0),
        "close_swing_degrade_on_quote_fallback": bool(getattr(SETTINGS, "CLOSE_SWING_DEGRADE_ON_QUOTE_FALLBACK", False)),
        "close_swing_degraded_max_candidates_per_cycle": int(getattr(SETTINGS, "CLOSE_SWING_DEGRADED_MAX_CANDIDATES_PER_CYCLE", 0) or 0),
        "close_swing_degraded_max_budget_per_trade_krw": int(getattr(SETTINGS, "CLOSE_SWING_DEGRADED_MAX_BUDGET_PER_TRADE_KRW", 0) or 0),
        "close_swing_recheck_cooldown_sec": int(getattr(SETTINGS, "CLOSE_SWING_RECHECK_COOLDOWN_SEC", 0) or 0),
        "close_swing_min_liquidity_score": float(getattr(SETTINGS, "CLOSE_SWING_MIN_LIQUIDITY_SCORE", 0.0) or 0.0),
        "close_swing_soft_liquidity_score": float(getattr(SETTINGS, "CLOSE_SWING_SOFT_LIQUIDITY_SCORE", 0.0) or 0.0),
        "close_swing_min_avg_turnover_20d": float(getattr(SETTINGS, "CLOSE_SWING_MIN_AVG_TURNOVER_20D", 0.0) or 0.0),
        "event_trade_broker_ready_cache_sec": int(getattr(SETTINGS, "EVENT_TRADE_BROKER_READY_CACHE_SEC", 0) or 0),
        "event_trade_respect_available_cash": bool(getattr(SETTINGS, "EVENT_TRADE_RESPECT_AVAILABLE_CASH", False)),
        "event_trade_min_cash_buffer_krw": int(getattr(SETTINGS, "EVENT_TRADE_MIN_CASH_BUFFER_KRW", 0) or 0),
        "close_bet_take_profit_pct": float(getattr(SETTINGS, "CLOSE_BET_TAKE_PROFIT_PCT", 0.0) or 0.0),
        "close_bet_stop_loss_pct": float(getattr(SETTINGS, "CLOSE_BET_STOP_LOSS_PCT", 0.0) or 0.0),
        "close_bet_stop_grace_min": int(getattr(SETTINGS, "CLOSE_BET_STOP_GRACE_MIN", 0) or 0),
        "close_bet_require_recover_by_close": bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_BY_CLOSE", False)),
        "close_bet_require_recover_after_open": bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_AFTER_OPEN", False)),
        "close_bet_open_recovery_min": int(getattr(SETTINGS, "CLOSE_BET_OPEN_RECOVERY_MIN", 0) or 0),
        "close_bet_max_hold_days": int(getattr(SETTINGS, "CLOSE_BET_MAX_HOLD_DAYS", 0) or 0),
        "close_bet_time_exit_start": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_START", "") or ""),
        "close_bet_time_exit_end": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_END", "") or ""),
        "monitor_force_eod_liquidate": bool(getattr(SETTINGS, "MONITOR_FORCE_EOD_LIQUIDATE", False)),
        "main_run_mode": str(getattr(SETTINGS, "MAIN_RUN_MODE", "") or ""),
        "trade_window": f"{getattr(SETTINGS, 'EVENT_TRADE_WINDOW_START', '')}~{getattr(SETTINGS, 'EVENT_TRADE_WINDOW_END', '')}",
        "config_dry_run": bool(getattr(SETTINGS, "DRY_RUN", False)),
        "config_kis_virtual": bool(getattr(SETTINGS, "KIS_IS_VIRTUAL", False)),
        "engine_hardcoded_live": bool(engine_mode.get("hardcoded_live")),
        "engine_hardcoded_is_virtual": engine_mode.get("hardcoded_is_virtual"),
        "engine_hardcoded_dry_run": engine_mode.get("hardcoded_dry_run"),
        "engine_note": str(engine_mode.get("note") or ""),
        "broker_mode_mismatch": bool(
            engine_mode.get("hardcoded_live")
            and (bool(getattr(SETTINGS, "KIS_IS_VIRTUAL", False)) or bool(getattr(SETTINGS, "DRY_RUN", False)))
        ),
        "event_state_exists": os.path.exists(EVENT_STATE_PATH),
        "event_state_age_min": _age_minutes(EVENT_STATE_PATH),
        "runtime_status_exists": os.path.exists(TRADE_RUNTIME_STATUS_PATH),
        "runtime_status_age_min": _age_minutes(TRADE_RUNTIME_STATUS_PATH),
        "runtime_phase": str(runtime_status.get("phase") or ""),
        "runtime_in_trade_window": runtime_status.get("in_trade_window"),
        "runtime_candidate_count": int(runtime_status.get("candidate_count", 0) or 0),
        "runtime_candidate_aligned_count": int(runtime_status.get("candidate_aligned_count", 0) or 0),
        "runtime_candidate_risk_count": int(runtime_status.get("candidate_risk_count", 0) or 0),
        "runtime_candidate_approved_count": int(runtime_status.get("candidate_approved_count", 0) or 0),
        "runtime_candidate_blocked_count": int(runtime_status.get("candidate_blocked_count", 0) or 0),
        "runtime_candidate_budget_adjusted_count": int(runtime_status.get("candidate_budget_adjusted_count", 0) or 0),
        "runtime_candidate_one_share_blocked_histogram": dict(runtime_status.get("candidate_one_share_blocked_histogram") or {}),
        "runtime_processed_count": int(runtime_status.get("processed_count", 0) or 0),
        "runtime_last_result": str(runtime_status.get("last_result") or ""),
        "runtime_generated_at": str(runtime_status.get("generated_at") or ""),
        "runtime_broker_is_virtual": runtime_status.get("broker_is_virtual"),
        "runtime_broker_dry_run": runtime_status.get("broker_dry_run"),
        "runtime_broker_ready": runtime_status.get("broker_ready"),
        "runtime_broker_ready_note": str(runtime_status.get("broker_ready_note") or ""),
        "runtime_broker_cash_krw": int(runtime_status.get("broker_cash_krw", 0) or 0),
        "runtime_today_stop_sell_count": int(runtime_status.get("today_stop_sell_count", 0) or 0),
        "runtime_quote_primary_ok": runtime_status.get("quote_primary_ok"),
        "runtime_quote_source": str(runtime_status.get("quote_source") or ""),
        "runtime_holding_symbol_count": int(runtime_status.get("holding_symbol_count", 0) or 0),
        "runtime_holding_sector_count": int(runtime_status.get("holding_sector_count", 0) or 0),
        "runtime_holding_sector_summary": str(runtime_status.get("holding_sector_summary") or ""),
        "runtime_hybrid_active_sectors": effective_hybrid_active_sectors,
        "runtime_hybrid_shadow_chosen_count": effective_hybrid_shadow_chosen_count,
        "runtime_hybrid_live_selected_count": effective_hybrid_live_selected_count,
        "runtime_hybrid_live_only_symbols": effective_hybrid_live_only_symbols,
        "runtime_hybrid_shadow_only_symbols": effective_hybrid_shadow_only_symbols,
        "runtime_hybrid_quote_penalty_total": effective_hybrid_quote_penalty_total,
        "runtime_slack_notify_trade_skip": runtime_status.get("slack_notify_trade_skip"),
        "runtime_slack_webhook_hint": str(runtime_status.get("slack_webhook_hint") or ""),
        "runtime_last_record_sector": str((runtime_status.get("last_record") or {}).get("sector") or ""),
        "runtime_last_record_alignment_label": str((runtime_status.get("last_record") or {}).get("alignment_label") or ""),
        "runtime_last_record_close_swing_decision": str((runtime_status.get("last_record") or {}).get("close_swing_decision") or ""),
        "runtime_last_record_close_swing_reason": str((runtime_status.get("last_record") or {}).get("close_swing_reason") or ""),
        "runtime_last_record_close_swing_budget_krw": int((runtime_status.get("last_record") or {}).get("close_swing_budget_krw", 0) or 0),
        "runtime_last_record_close_swing_min_order_budget_krw": int((runtime_status.get("last_record") or {}).get("close_swing_min_order_budget_krw", 0) or 0),
        "runtime_last_record_close_swing_budget_adjusted": bool((runtime_status.get("last_record") or {}).get("close_swing_budget_adjusted", False)),
        "runtime_last_record_hybrid_sector_regime_score": float((runtime_status.get("last_record") or {}).get("hybrid_sector_regime_score", 0.0) or 0.0),
        "runtime_last_record_hybrid_sector_final_score": float((runtime_status.get("last_record") or {}).get("hybrid_sector_final_score", 0.0) or 0.0),
        "runtime_last_record_hybrid_sector_final_label": str((runtime_status.get("last_record") or {}).get("hybrid_sector_final_label") or ""),
        "runtime_last_record_hybrid_sector_agreement_level": str((runtime_status.get("last_record") or {}).get("hybrid_sector_agreement_level") or ""),
        "runtime_last_record_hybrid_relative_value_score": float((runtime_status.get("last_record") or {}).get("hybrid_relative_value_score", 0.0) or 0.0),
        "runtime_last_record_hybrid_timing_score": float((runtime_status.get("last_record") or {}).get("hybrid_timing_score", 0.0) or 0.0),
        "runtime_last_record_hybrid_final_trade_score": float((runtime_status.get("last_record") or {}).get("hybrid_final_trade_score", 0.0) or 0.0),
        "runtime_last_record_hybrid_blocked_reason_code": str((runtime_status.get("last_record") or {}).get("hybrid_blocked_reason_code") or ""),
        "runtime_broker_mismatch": bool(
            isinstance(runtime_status.get("broker_is_virtual"), bool)
            and isinstance(runtime_status.get("broker_dry_run"), bool)
            and (
                runtime_status.get("broker_is_virtual") != bool(getattr(SETTINGS, "KIS_IS_VIRTUAL", False))
                or runtime_status.get("broker_dry_run") != bool(getattr(SETTINGS, "DRY_RUN", False))
            )
        ),
        "handled_count": handled_count,
        "traded_count": traded_count,
        "spent_budget_today_krw": _traded_budget_for_today(),
        "last_attempted_at": str((last_attempt or {}).get("attempted_at") or ""),
        "last_attempted_symbol": str((last_attempt or {}).get("stock_code") or ""),
        "last_attempted_event_type": str((last_attempt or {}).get("event_type") or ""),
        "last_attempted_traded": bool((last_attempt or {}).get("traded", False)),
        "last_attempted_decision": str((last_attempt or {}).get("decision") or ""),
        "last_attempted_reason": str((last_attempt or {}).get("reason") or ""),
        "last_traded_at": str((last_traded or {}).get("attempted_at") or ""),
        "last_traded_symbol": str((last_traded or {}).get("stock_code") or ""),
        "last_traded_event_type": str((last_traded or {}).get("event_type") or ""),
        "trade_audit_exists": os.path.exists(trade_audit_path),
        "trade_audit_age_min": _age_minutes(trade_audit_path),
        "last_trade_audit_timestamp": str(last_trade_audit.get("timestamp") or ""),
        "last_trade_audit_action": str(last_trade_audit.get("action") or ""),
        "last_trade_audit_symbol": str(last_trade_audit.get("symbol") or ""),
        "last_trade_audit_result": str(last_trade_audit.get("result") or ""),
        "last_trade_audit_delivered": last_trade_audit.get("delivered"),
        "last_trade_audit_sector": str(last_trade_audit.get("sector") or ""),
        "last_trade_audit_alignment_label": str(last_trade_audit.get("alignment_label") or ""),
        "last_trade_audit_market_mode": str(last_trade_audit.get("market_mode") or ""),
        "slack_delivery_exists": os.path.exists(delivery_path),
        "slack_delivery_age_min": _age_minutes(delivery_path),
        "last_slack_delivery_timestamp": str(last_delivery.get("timestamp") or ""),
        "last_slack_delivery_status": str(last_delivery.get("status") or ""),
        "last_slack_delivery_title": str(last_delivery.get("title") or ""),
        "last_slack_delivery_webhook_hint": str(last_delivery.get("webhook_hint") or ""),
        "slack_fallback_exists": os.path.exists(fallback_path),
        "slack_fallback_age_min": _age_minutes(fallback_path),
        "last_slack_fallback_timestamp": str(last_fallback.get("timestamp") or ""),
        "last_slack_fallback_reason": str(last_fallback.get("reason") or ""),
        "sector_rotation_exists": os.path.exists(SECTOR_ROTATION_PATH),
        "sector_rotation_age_min": _age_minutes(SECTOR_ROTATION_PATH),
        "sector_rotation_market_mode": str(sector_rotation.get("market_mode") or ""),
        "sector_rotation_active_sectors": list(sector_rotation.get("active_sectors") or []),
        "sector_rotation_top_sector": sector_rotation_top_sector,
        "sector_thesis_exists": os.path.exists(SECTOR_THESIS_PATH),
        "sector_thesis_age_min": _age_minutes(SECTOR_THESIS_PATH),
        "sector_thesis_market_mode": str(sector_thesis.get("market_mode") or ""),
        "sector_thesis_top_sector": sector_thesis_top_sector,
        "sector_thesis_top_label": sector_thesis_top_label,
        "wics_universe_exists": os.path.exists(WICS_UNIVERSE_PATH),
        "wics_universe_age_min": _age_minutes(WICS_UNIVERSE_PATH),
        "fair_value_snapshot_exists": os.path.exists(FAIR_VALUE_SNAPSHOT_PATH),
        "fair_value_snapshot_age_min": _age_minutes(FAIR_VALUE_SNAPSHOT_PATH),
        "fair_value_row_count": int(fair_value_summary.get("row_count", 0) or 0),
        "fair_value_coverage_count": int(fair_value_summary.get("coverage_count", 0) or 0),
        "fair_value_top_discount_name": str(((fair_value_summary.get("top_discount") or [{}])[0]).get("name") or ""),
        "fair_value_top_discount_gap_pct": float(((fair_value_summary.get("top_discount") or [{}])[0]).get("fair_value_gap_pct", 0.0) or 0.0),
        "wics_universe_regime": str(wics_summary.get("universe_regime") or ""),
        "wics_universe_history_confidence_label": str(wics_summary.get("history_confidence_label") or ""),
        "wics_universe_dynamic_symbol_count": int(wics_summary.get("dynamic_symbol_count", 0) or 0),
        "wics_universe_review_sector_count": int(wics_summary.get("review_sector_count", 0) or 0),
        "wics_universe_avg_dynamic_stability": float(wics_summary.get("avg_dynamic_stability", 0.0) or 0.0),
        "wics_focus_sector": wics_focus_sector,
        "wics_focus_status_label": str(wics_focus_meta.get("universe_status_label") or ""),
        "wics_focus_reason": str(wics_focus_meta.get("universe_status_reason") or ""),
        "wics_focus_history_confidence_label": str(wics_focus_meta.get("history_confidence_label") or ""),
        "wics_focus_dynamic_count": int(wics_focus_meta.get("dynamic_count", 0) or 0),
        "wics_focus_dynamic_stability": float(wics_focus_meta.get("avg_dynamic_stability", 0.0) or 0.0),
        "wics_focus_final_count": int(wics_focus_meta.get("final_count", 0) or 0),
        "wics_focus_mismatch_count": int(wics_focus_meta.get("mismatch_count", 0) or 0),
        "relative_value_exists": os.path.exists(RELATIVE_VALUE_PATH),
        "relative_value_age_min": _age_minutes(RELATIVE_VALUE_PATH),
        "relative_value_top_symbol": str(((relative_value.get("top_candidates") or [{}])[0]).get("symbol") or ""),
        "hybrid_shadow_book_exists": os.path.exists(HYBRID_SHADOW_BOOK_PATH),
        "hybrid_shadow_book_age_min": _age_minutes(HYBRID_SHADOW_BOOK_PATH),
        "hybrid_shadow_chosen_count": int(shadow_book.get("shadow_chosen_count", 0) or 0),
        "hybrid_shadow_live_selected_count": int(shadow_book.get("live_selected_count", 0) or 0),
        "hybrid_shadow_live_only_symbols": list(shadow_book.get("live_only_symbols") or []),
        "hybrid_shadow_shadow_only_symbols": list(shadow_book.get("shadow_only_symbols") or []),
        "hybrid_shadow_quote_penalty_total": float(shadow_book.get("quote_fallback_penalty_total", 0.0) or 0.0),
        "trade_decision_ledger_exists": os.path.exists(TRADE_DECISION_LEDGER_PATH),
        "trade_decision_ledger_age_min": _age_minutes(TRADE_DECISION_LEDGER_PATH),
        "trade_decision_ledger_recent_rows": len(decision_rows),
        "trade_decision_blocked_histogram": effective_blocked_hist,
    }
    return payload


def render_report() -> str:
    health = load_trade_runtime_health()

    lines = [
        "[자동매매 점검표]",
        f"- 생성시각: {_now_str()}",
        f"- 자동매매 사용: {health['auto_trade_enabled']}",
        f"- 종가 매매 사용: {health['close_swing_enabled']}",
        f"- 하이브리드 모드: 사용={health['hybrid_rotation_enabled']} | 비교모드만={health['hybrid_shadow_only']} | 섹터 기준={health['hybrid_active_sector_min_score']} | 상대가치 기준={health['hybrid_relative_value_min_score']} | WICS 최소일수={health['hybrid_wics_hard_min_days']}",
        f"- 종가 매매 한도: 일일={health['close_swing_max_trades_per_day']} | 회차={health['close_swing_max_candidates_per_cycle']} | 총보유={health['close_swing_max_open_positions']} | 섹터일일={health['close_swing_max_trades_per_sector_per_day']} | 섹터회차={health['close_swing_max_candidates_per_sector_per_cycle']} | 섹터보유={health['close_swing_max_open_names_per_sector']} | 일일예산={health['close_swing_max_budget_per_day_krw']}krw(사용 {health['spent_budget_today_krw']}krw) | 재시도={health['close_swing_max_retry_per_record']} | 가격신호={health['close_swing_require_price_signal']} | 1주예산하한={health['close_swing_one_share_budget_floor_enabled']} | 일일손절={health['close_swing_max_stop_sells_per_day']}(현재 {health['runtime_today_stop_sell_count']}) | 보강시세가드={health['close_swing_degrade_on_quote_fallback']}({health['close_swing_degraded_max_candidates_per_cycle']}/{health['close_swing_degraded_max_budget_per_trade_krw']}krw) | 재점검={health['close_swing_recheck_cooldown_sec']}s | 브로커캐시={health['event_trade_broker_ready_cache_sec']}s | 현금가드={health['event_trade_respect_available_cash']}({health['event_trade_min_cash_buffer_krw']}krw) | 유동성={health['close_swing_min_liquidity_score']:.2f}/{health['close_swing_soft_liquidity_score']:.2f} | 20일회전율={health['close_swing_min_avg_turnover_20d']:.4f}",
        f"- 종가 매매 위험 기준: 익절={health['close_bet_take_profit_pct']:.1f}% | 손절={health['close_bet_stop_loss_pct']:.1f}% | 유예={health['close_bet_stop_grace_min']}m | 종가회복점검={health['close_bet_require_recover_by_close']} | 시초회복점검={health['close_bet_require_recover_after_open']}({health['close_bet_open_recovery_min']}m) | 최대보유={health['close_bet_max_hold_days']}d({health['close_bet_time_exit_start']}~{health['close_bet_time_exit_end']}) | 장마감정리={health['monitor_force_eod_liquidate']}",
        f"- 실행 모드: {health['main_run_mode']}",
        f"- 주문 시간창: {health['trade_window']}",
        f"- 설정: dry_run={health['config_dry_run']} | KIS 모의투자={health['config_kis_virtual']}",
        f"- 엔진 브로커 메모: {health['engine_note'] or '-'}",
        f"- 이벤트 거래 상태: {_fmt_file_status(EVENT_STATE_PATH)} | 처리 {health['handled_count']} | 체결 {health['traded_count']}",
        f"- 주문 감시 런타임: {_fmt_file_status(TRADE_RUNTIME_STATUS_PATH)} | 단계={health['runtime_phase']} | 주문창 안={health['runtime_in_trade_window']} | 후보={health['runtime_candidate_count']} | 통과={health['runtime_candidate_approved_count']} | 보류={health['runtime_candidate_blocked_count']} | 예산조정={health['runtime_candidate_budget_adjusted_count']} | 한주미달 차단={health['runtime_candidate_one_share_blocked_histogram'] or '-'} | 맥락 일치={health['runtime_candidate_aligned_count']} | 맥락 경계={health['runtime_candidate_risk_count']} | 마지막 결과={health['runtime_last_result']}",
        f"- 하이브리드 런타임: 활성섹터={','.join(health['runtime_hybrid_active_sectors'][:5]) or '-'} | 섹터결론={health['sector_thesis_top_sector'] or '-'}:{health['sector_thesis_top_label'] or '-'} | 비교모드 후보={health['runtime_hybrid_shadow_chosen_count']} | 실주문 후보={health['runtime_hybrid_live_selected_count']} | 실주문만={','.join(health['runtime_hybrid_live_only_symbols'][:4]) or '-'} | 비교모드만={','.join(health['runtime_hybrid_shadow_only_symbols'][:4]) or '-'} | 가격 보수조정={health['runtime_hybrid_quote_penalty_total']:.1f}",
        f"- 런타임 브로커/슬랙: 모의투자={health['runtime_broker_is_virtual']} | dry_run={health['runtime_broker_dry_run']} | 브로커준비={health['runtime_broker_ready']} | 브로커현금={health['runtime_broker_cash_krw']} | 브로커메모={health['runtime_broker_ready_note'] or '-'} | 보유종목수={health['runtime_holding_symbol_count']} | 보유섹터수={health['runtime_holding_sector_count']} | 보유요약={health['runtime_holding_sector_summary'] or '-'} | 원본시세정상={health['runtime_quote_primary_ok']} | 시세소스={health['runtime_quote_source'] or '-'} | 보류알림전송={health['runtime_slack_notify_trade_skip']} | 웹훅힌트={health['runtime_slack_webhook_hint'] or '-'}",
        f"- 마지막 시도 상태: 시각={health['last_attempted_at']} | 종목={health['last_attempted_symbol']} | 이벤트={health['last_attempted_event_type']} | 판단={health['last_attempted_decision'] or '-'} | 사유={health['last_attempted_reason'] or '-'} | 체결={health['last_attempted_traded']}",
        f"- 마지막 체결 상태: 시각={health['last_traded_at']} | 종목={health['last_traded_symbol']} | 이벤트={health['last_traded_event_type']}",
        f"- 마지막 종가 매매 기록: 판단={health['runtime_last_record_close_swing_decision'] or '-'} | 사유={health['runtime_last_record_close_swing_reason'] or '-'} | 예산={health['runtime_last_record_close_swing_budget_krw']} | 최소주문={health['runtime_last_record_close_swing_min_order_budget_krw']} | 예산조정={health['runtime_last_record_close_swing_budget_adjusted']} | 섹터={health['runtime_last_record_sector'] or '-'} | 맥락={health['runtime_last_record_alignment_label'] or '-'} | 섹터결론={health['runtime_last_record_hybrid_sector_final_label'] or '-'}({health['runtime_last_record_hybrid_sector_agreement_level'] or '-'}) | 하이브리드={health['runtime_last_record_hybrid_sector_final_score']:.1f}/{health['runtime_last_record_hybrid_relative_value_score']:.1f}/{health['runtime_last_record_hybrid_timing_score']:.1f}->{health['runtime_last_record_hybrid_final_trade_score']:.1f} | 보류코드={health['runtime_last_record_hybrid_blocked_reason_code'] or '-'}",
        f"- 하이브리드 산출물: 섹터순환={_fmt_file_status(SECTOR_ROTATION_PATH)} | 활성={','.join(health['sector_rotation_active_sectors'][:5]) or '-'} | 상단섹터={health['sector_rotation_top_sector'] or '-'} | 섹터결론={_fmt_file_status(SECTOR_THESIS_PATH)} | 결론상단={health['sector_thesis_top_sector'] or '-'}:{health['sector_thesis_top_label'] or '-'} | 상대가치={_fmt_file_status(RELATIVE_VALUE_PATH)} | 상단종목={health['relative_value_top_symbol'] or '-'} | 비교모드장부={_fmt_file_status(HYBRID_SHADOW_BOOK_PATH)} | 비교/실주문={health['hybrid_shadow_chosen_count']}/{health['hybrid_shadow_live_selected_count']} | 실주문만={','.join(health['hybrid_shadow_live_only_symbols'][:4]) or '-'} | 비교모드만={','.join(health['hybrid_shadow_shadow_only_symbols'][:4]) or '-'} | 가격 보수조정={health['hybrid_shadow_quote_penalty_total']:.1f}",
        f"- WICS 바스켓: {_fmt_file_status(WICS_UNIVERSE_PATH)} | 흐름={health['wics_universe_regime'] or '-'} | 표본={health['wics_universe_history_confidence_label'] or '-'} | 동적 후보수={health['wics_universe_dynamic_symbol_count']} | 평균 동적안정도={_fmt_optional_stability(health['wics_universe_dynamic_symbol_count'], health['wics_universe_avg_dynamic_stability'])} | 재점검 섹터={health['wics_universe_review_sector_count']} | 초점={health['wics_focus_sector'] or '-'}:{health['wics_focus_status_label'] or '-'} | 초점 표본={health['wics_focus_history_confidence_label'] or '-'} | 초점 동적 후보={health['wics_focus_dynamic_count']}/{_fmt_optional_stability(health['wics_focus_dynamic_count'], health['wics_focus_dynamic_stability'])} | 초점 최종 바스켓={health['wics_focus_final_count']} | 초점 섹터불일치 제외={health['wics_focus_mismatch_count']}",
        f"- 적정가 스냅샷: {_fmt_file_status(FAIR_VALUE_SNAPSHOT_PATH)} | 산출={health['fair_value_coverage_count']}/{health['fair_value_row_count']} | 상단 할인={health['fair_value_top_discount_name'] or '-'} {health['fair_value_top_discount_gap_pct']:+.1f}%",
        f"- 판단 장부: {_fmt_file_status(TRADE_DECISION_LEDGER_PATH)} | 최근행={health['trade_decision_ledger_recent_rows']} | 보류사유분포={health['trade_decision_blocked_histogram'] or '-'}",
        f"- 주문 감사 로그: {_fmt_file_status(getattr(SETTINGS, 'TRADE_ALERT_AUDIT_PATH', DEFAULT_TRADE_ALERT_AUDIT_PATH))}",
        f"- 마지막 주문 감사: 시각={health['last_trade_audit_timestamp']} | 동작={health['last_trade_audit_action']} | 종목={health['last_trade_audit_symbol']} | 섹터={health['last_trade_audit_sector'] or '-'} | 맥락={health['last_trade_audit_alignment_label'] or '-'} | 시장모드={health['last_trade_audit_market_mode'] or '-'} | 결과={health['last_trade_audit_result']} | 전송={health['last_trade_audit_delivered']}",
        f"- 슬랙 전송 로그: {_fmt_file_status(getattr(SETTINGS, 'SLACK_DELIVERY_LOG_PATH', DEFAULT_SLACK_DELIVERY_LOG_PATH))}",
        f"- 마지막 슬랙 전송: 시각={health['last_slack_delivery_timestamp']} | 상태={health['last_slack_delivery_status']} | 제목={health['last_slack_delivery_title']} | 웹훅힌트={health['last_slack_delivery_webhook_hint']}",
        f"- 슬랙 대체 전송 로그: {_fmt_file_status(getattr(SETTINGS, 'SLACK_FALLBACK_LOG_PATH', DEFAULT_SLACK_FALLBACK_LOG_PATH))}",
        f"- 마지막 슬랙 대체 전송: 시각={health['last_slack_fallback_timestamp']} | 사유={health['last_slack_fallback_reason']}",
    ]

    notes = []
    if health["handled_count"] == 0:
        notes.append("main_trade_state.json에 처리 완료 기록이 아직 없습니다")
    if health["traded_count"] == 0:
        notes.append("이 체크아웃에는 traded=true 성공 기록이 아직 없습니다")
    if not health["slack_delivery_exists"]:
        notes.append("이 체크아웃에는 슬랙 전송 로그가 없습니다")
    if not health["trade_audit_exists"]:
        notes.append("이 체크아웃에는 주문 감사 로그가 없습니다")
    if not health["runtime_status_exists"]:
        notes.append("이 체크아웃에는 주문 감시 런타임 상태 파일이 없습니다")
    if health["hybrid_rotation_enabled"] and not health["sector_rotation_exists"]:
        notes.append("이 체크아웃에는 하이브리드 섹터순환 산출물이 없습니다")
    if health["hybrid_rotation_enabled"] and not health["sector_thesis_exists"]:
        notes.append("이 체크아웃에는 섹터 결론 산출물이 없습니다")
    if health["hybrid_rotation_enabled"] and not health["wics_universe_exists"]:
        notes.append("이 체크아웃에는 WICS 최종 바스켓 산출물이 없습니다")
    if not health["fair_value_snapshot_exists"]:
        notes.append("이 체크아웃에는 적정가 스냅샷 산출물이 없습니다")
    if health["hybrid_rotation_enabled"] and not health["trade_decision_ledger_exists"]:
        notes.append("이 체크아웃에는 하이브리드 판단 장부가 없습니다")
    if health["wics_focus_status_label"] == "재점검":
        notes.append("상단 WICS 바스켓이 재점검 상태라 섹터 결론을 한 단계 보수적으로 읽는 편이 좋습니다")
    if health["wics_focus_dynamic_count"] > 0 and health["wics_focus_dynamic_stability"] < 0.6:
        notes.append("상단 WICS 바스켓의 동적안정도가 낮아 오늘 새로 붙은 종목은 노이즈일 수 있습니다")
    if health["broker_mode_mismatch"]:
        notes.append("엔진 브로커 모드가 현재 설정의 모의투자/드라이런 값과 어긋나 보입니다")
    if health["runtime_candidate_count"] > 0 and health["runtime_candidate_aligned_count"] == 0 and health["runtime_candidate_risk_count"] > 0:
        notes.append("현재 주문 감시 후보가 모두 맥락 경계 쪽이라 바로 진입할 후보가 없습니다")
    if (
        health["runtime_holding_symbol_count"] > 0
        and health["close_swing_max_open_positions"] > 0
        and health["runtime_holding_symbol_count"] >= health["close_swing_max_open_positions"]
    ):
        notes.append("현재 보유 종목 수가 설정한 최대 보유 한도에 닿아 있습니다")
    if health["runtime_candidate_blocked_count"] > 0 and health["runtime_candidate_approved_count"] == 0:
        notes.append("현재 종가 매매 가드가 모든 후보를 막고 있습니다")
    if health["runtime_broker_ready"] is False:
        notes.append(f"브로커 준비 상태 점검이 실패했습니다: {health['runtime_broker_ready_note'] or '-'}")
    if _file_mtime(EVENT_STATE_PATH) and _age_minutes(EVENT_STATE_PATH) and _age_minutes(EVENT_STATE_PATH) > 60 * 24:
        notes.append("거래 상태 파일이 오래돼 있어 지금 체크아웃이 라이브 PM2 작업본이 아닐 수 있습니다")

    if notes:
        lines.append("메모")
        for note in notes:
            lines.append(f"- {note}")

    return "\n".join(lines)


if __name__ == "__main__":
    print(render_report())
