from __future__ import annotations

import json
import math
import os
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from config import SETTINGS


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CARD_CSV_PATH = os.path.join(ROOT_DIR, "cards", "stock_cards_latest.csv")
FACTOR_CSV_PATH = os.path.join(ROOT_DIR, "factors", "snapshots", "factor_snapshot_latest.csv")
MARKET_BRIEFING_PATH = os.path.join(ROOT_DIR, "runtime", "market_briefing_latest.json")


def _norm_symbol(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "nan", "NaN"):
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def _safe_int(value: Any) -> int:
    if value in (None, "", "nan", "NaN"):
        return 0
    try:
        return int(float(value))
    except Exception:
        return 0

def _event_age_minutes(record: dict[str, Any]) -> Optional[int]:
    event_date = str(record.get("event_date") or "").strip()
    event_time = str(record.get("event_time_hhmm") or "").strip()
    if not event_date or not event_time or len(event_time) != 5 or ":" not in event_time:
        return None
    try:
        event_dt = datetime.fromisoformat(f"{event_date}T{event_time}:00")
    except Exception:
        return None
    now = datetime.now()
    age_min = int((now - event_dt).total_seconds() // 60)
    return max(age_min, 0)


def _read_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_lookup_from_csv(path: str, *, symbol_key: str = "symbol") -> dict[str, dict[str, Any]]:
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    if symbol_key not in df.columns:
        return rows
    for item in df.to_dict(orient="records"):
        symbol = _norm_symbol(item.get(symbol_key))
        if not symbol:
            continue
        rows[symbol] = item
    return rows


def _build_briefing_lookup(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in summary.get("candidates", []) or []:
        symbol = _norm_symbol(row.get("symbol"))
        if not symbol:
            continue
        lookup[symbol] = {
            "symbol": symbol,
            "name": row.get("name") or symbol,
            "sector": row.get("sector") or "",
            "score_total": _safe_float(row.get("score_total")),
            "source_count": _safe_int(row.get("source_count")),
            "source_hits": list(row.get("source_hits") or []),
        }
    for row in summary.get("candidate_actions", []) or []:
        symbol = _norm_symbol(row.get("symbol"))
        if not symbol:
            continue
        entry = lookup.setdefault(
            symbol,
            {
                "symbol": symbol,
                "name": row.get("name") or symbol,
                "sector": row.get("sector") or "",
                "score_total": None,
                "source_count": 0,
                "source_hits": list(row.get("source_hits") or []),
            },
        )
        entry["action"] = str(row.get("action") or "")
        entry["note"] = str(row.get("note") or "")
        entry["reason"] = str(row.get("reason") or "")
        if not entry.get("source_hits"):
            entry["source_hits"] = list(row.get("source_hits") or [])
    return lookup


def load_close_swing_inputs() -> dict[str, Any]:
    briefing = _read_json(MARKET_BRIEFING_PATH)
    return {
        "market_briefing": briefing,
        "briefing_lookup": _build_briefing_lookup(briefing),
        "card_lookup": _load_lookup_from_csv(CARD_CSV_PATH),
        "factor_lookup": _load_lookup_from_csv(FACTOR_CSV_PATH),
    }


def _price_change_pct(broker: Any, symbol: str) -> Optional[float]:
    if broker is None or not hasattr(broker, "get_price_change_rate"):
        return None
    try:
        return _safe_float(broker.get_price_change_rate(symbol))
    except Exception:
        return None


def _extract_last_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, dict):
        raw_price = value.get("output", {}).get("stck_prpr")
        try:
            return float(str(raw_price).replace(",", "").strip()) if raw_price is not None else None
        except Exception:
            return None
    return _safe_float(value)


def _last_price(broker: Any, symbol: str) -> Optional[float]:
    if broker is None or not hasattr(broker, "get_last_price"):
        return None
    try:
        return _extract_last_price(broker.get_last_price(symbol))
    except Exception:
        return None


def _one_share_budget_krw(price: Optional[float]) -> int:
    if price is None or price <= 0:
        return 0
    return int(math.ceil(float(price) / 10000.0) * 10000)


def _event_type_bonus(event_type: str) -> int:
    if event_type in {"PERF_PRELIM", "SUPPLY_CONTRACT", "BUYBACK", "STOCK_CANCELLATION"}:
        return 1
    return 0


def _briefing_action_points(action: str) -> int:
    if action == "직접 후보":
        return 3
    if action in {"눌림 후보", "관찰 후보"}:
        return 2
    if action == "보류":
        return -1
    return 0


def _briefing_action_label(action: str) -> str:
    return action or "미포함"


def _close_swing_budget(
    max_budget_krw: int,
    *,
    support_score: int,
    briefing_action: str,
    alignment_score: int,
    recovering: bool,
    price_change_pct: Optional[float],
    event_age_minutes: Optional[int],
    liquidity_score: Optional[float],
    avg_turnover_20d: Optional[float],
) -> tuple[float, int]:
    multiplier = 0.55
    if support_score >= 6:
        multiplier = 0.70
    if support_score >= 8:
        multiplier = 0.85
    if support_score >= 10:
        multiplier = 1.00

    if briefing_action == "직접 후보":
        multiplier += 0.08
    elif briefing_action == "보류":
        multiplier -= 0.12
    if alignment_score >= 1:
        multiplier += 0.05
    if recovering:
        multiplier += 0.05
    if price_change_pct is not None:
        if price_change_pct >= 6.0:
            multiplier -= 0.18
        elif price_change_pct >= 3.0:
            multiplier -= 0.08
        elif price_change_pct <= -1.5 and recovering:
            multiplier += 0.04
    if event_age_minutes is not None:
        if event_age_minutes <= 45:
            multiplier += 0.05
        elif event_age_minutes >= 210:
            multiplier -= 0.08
        elif event_age_minutes >= 360:
            multiplier -= 0.05
    min_liquidity = float(getattr(SETTINGS, "CLOSE_SWING_MIN_LIQUIDITY_SCORE", 0.45) or 0.45)
    soft_liquidity = float(getattr(SETTINGS, "CLOSE_SWING_SOFT_LIQUIDITY_SCORE", 0.65) or 0.65)
    min_turnover = float(getattr(SETTINGS, "CLOSE_SWING_MIN_AVG_TURNOVER_20D", 0.0025) or 0.0025)
    if liquidity_score is not None:
        if liquidity_score < min_liquidity:
            multiplier -= 0.18
        elif liquidity_score < soft_liquidity:
            multiplier -= 0.08
        elif liquidity_score >= 0.85:
            multiplier += 0.03
    if avg_turnover_20d is not None:
        if avg_turnover_20d < min_turnover:
            multiplier -= 0.15
        elif avg_turnover_20d < (min_turnover * 2.0):
            multiplier -= 0.06
        elif avg_turnover_20d >= 0.01:
            multiplier += 0.02

    multiplier = max(0.35, min(1.00, multiplier))
    budget = int(round(max_budget_krw * multiplier / 10000.0) * 10000)
    budget = max(50000, min(max_budget_krw, budget))
    return round(multiplier, 4), int(budget)


def _close_swing_risk_profile(
    *,
    event_type: str,
    support_score: int,
    recovering: bool,
    price_change_pct: Optional[float],
    event_age_minutes: Optional[int],
) -> dict[str, Any]:
    take_profit_pct = float(getattr(SETTINGS, "CLOSE_BET_TAKE_PROFIT_PCT", 8.0) or 8.0)
    stop_loss_pct = float(getattr(SETTINGS, "CLOSE_BET_STOP_LOSS_PCT", -4.0) or -4.0)
    stop_grace_min = int(getattr(SETTINGS, "CLOSE_BET_STOP_GRACE_MIN", 60) or 60)
    open_recovery_min = int(getattr(SETTINGS, "CLOSE_BET_OPEN_RECOVERY_MIN", 10) or 10)
    notes: list[str] = []

    if event_type in {"PERF_PRELIM", "SALES_VARIATION"}:
        take_profit_pct += 2.0
        stop_loss_pct -= 0.5
        stop_grace_min += 30
        open_recovery_min += 5
        notes.append("실적형 재료")
    elif event_type == "SUPPLY_CONTRACT":
        take_profit_pct += 1.0
        stop_loss_pct -= 0.3
        stop_grace_min += 15
        notes.append("수주형 재료")
    elif event_type in {"BUYBACK", "STOCK_CANCELLATION"}:
        take_profit_pct += 0.5
        stop_grace_min += 15
        notes.append("주주환원 재료")

    if support_score >= 9:
        take_profit_pct += 1.0
        stop_loss_pct -= 0.4
        stop_grace_min += 15
        notes.append("강한 교집합")
    elif support_score <= 5:
        take_profit_pct -= 1.0
        stop_loss_pct += 0.4
        open_recovery_min = max(5, open_recovery_min - 2)
        notes.append("약한 교집합")

    if recovering and price_change_pct is not None and price_change_pct <= -1.5:
        stop_loss_pct -= 0.3
        stop_grace_min += 15
        open_recovery_min += 5
        notes.append("눌림 후 회복")

    if price_change_pct is not None and price_change_pct >= 4.0:
        take_profit_pct -= 1.0
        stop_loss_pct += 0.5
        open_recovery_min = max(5, open_recovery_min - 3)
        notes.append("당일 상승 선반영")

    if event_age_minutes is not None and event_age_minutes >= int(getattr(SETTINGS, "CLOSE_SWING_STALE_EVENT_MIN", 210) or 210):
        take_profit_pct -= 0.5
        stop_loss_pct += 0.3
        notes.append("공시 경과")

    take_profit_pct = max(6.0, min(15.0, take_profit_pct))
    stop_loss_pct = max(-6.0, min(-3.0, stop_loss_pct))
    stop_grace_min = max(45, min(120, stop_grace_min))
    open_recovery_min = max(5, min(20, open_recovery_min))
    return {
        "take_profit_pct": round(take_profit_pct, 2),
        "stop_loss_pct": round(stop_loss_pct, 2),
        "stop_grace_min": int(stop_grace_min),
        "open_recovery_min": int(open_recovery_min),
        "risk_notes": notes,
    }


def evaluate_close_swing_candidate(
    record: dict[str, Any],
    *,
    broker: Any = None,
    inputs: Optional[dict[str, Any]] = None,
    context: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    inputs = inputs or load_close_swing_inputs()
    context = context or {}

    symbol = _norm_symbol(record.get("stock_code"))
    event_type = str(record.get("event_type") or "")
    signal_bias = str(record.get("signal_bias") or "")

    briefing = inputs.get("market_briefing") or {}
    briefing_lookup = inputs.get("briefing_lookup") or {}
    card_lookup = inputs.get("card_lookup") or {}
    factor_lookup = inputs.get("factor_lookup") or {}

    briefing_row = briefing_lookup.get(symbol) or {}
    card_row = card_lookup.get(symbol) or {}
    factor_row = factor_lookup.get(symbol) or {}

    briefing_action = str(briefing_row.get("action") or "")
    briefing_score_total = _safe_float(briefing_row.get("score_total"))
    briefing_source_count = _safe_int(briefing_row.get("source_count"))
    briefing_confidence = _safe_int((briefing.get("confidence") or {}).get("score"))
    data_quality_label = str((briefing.get("data_quality") or {}).get("label") or "")
    positioning_mode = str((briefing.get("positioning") or {}).get("mode") or "")

    card_score = _safe_float(card_row.get("card_score")) or 0.0
    factor_score = _safe_float(factor_row.get("composite_score")) or 0.0
    active_sources = _safe_int(card_row.get("active_source_count"))
    flow_state_score = _safe_float(card_row.get("flow_state_score")) or 0.0
    intraday_edge_score = _safe_float(card_row.get("flow_intraday_edge_score")) or 0.0
    ml_pred_return_5d = _safe_float(card_row.get("ml_pred_return_5d")) or 0.0
    event_expected_alpha_5d = _safe_float(card_row.get("event_expected_alpha_5d")) or 0.0
    liquidity_score = _safe_float(factor_row.get("liquidity_score"))
    avg_turnover_20d = _safe_float(factor_row.get("avg_turnover_20d"))
    alignment_score = _safe_int(context.get("alignment_score"))

    price_change_pct = _price_change_pct(broker, symbol)
    event_age_minutes = _event_age_minutes(record)

    support_score = 0
    support_reasons: list[str] = []
    risk_reasons: list[str] = []

    support_score += _briefing_action_points(briefing_action)
    if briefing_action:
        support_reasons.append(f"브리핑 {briefing_action}")
    elif briefing_score_total is not None and briefing_score_total >= 2.5 and briefing_source_count >= 2:
        support_score += 1
        support_reasons.append("브리핑 교집합 후보권")

    if card_score >= float(getattr(SETTINGS, "CLOSE_SWING_MIN_CARD_SCORE", 0.70)):
        support_score += 2
        support_reasons.append(f"카드 {card_score:.2f}")
    if factor_score >= float(getattr(SETTINGS, "CLOSE_SWING_MIN_FACTOR_SCORE", 0.58)):
        support_score += 1
        support_reasons.append(f"팩터 {factor_score:.2f}")
    if active_sources >= int(getattr(SETTINGS, "CLOSE_SWING_MIN_ACTIVE_SOURCES", 4)):
        support_score += 1
        support_reasons.append(f"소스 {active_sources}개")
    soft_liquidity_score = float(getattr(SETTINGS, "CLOSE_SWING_SOFT_LIQUIDITY_SCORE", 0.65) or 0.65)
    min_avg_turnover_20d = float(getattr(SETTINGS, "CLOSE_SWING_MIN_AVG_TURNOVER_20D", 0.0025) or 0.0025)
    if (liquidity_score is not None and liquidity_score >= soft_liquidity_score) or (
        avg_turnover_20d is not None and avg_turnover_20d >= (min_avg_turnover_20d * 2.0)
    ):
        support_score += 1
        if liquidity_score is not None:
            support_reasons.append(f"유동성 {liquidity_score:.2f}")
        elif avg_turnover_20d is not None:
            support_reasons.append(f"회전율 {avg_turnover_20d:.4f}")
    if alignment_score >= 1:
        support_score += 1
        support_reasons.append("맥락 정렬")
    if briefing_confidence >= 60:
        support_score += 1
        support_reasons.append(f"브리핑 확신도 {briefing_confidence}")
    if ml_pred_return_5d >= 0.03:
        support_score += 1
        support_reasons.append(f"ML +{ml_pred_return_5d * 100:.1f}%")
    if event_expected_alpha_5d > 0:
        support_score += 1
        support_reasons.append(f"이벤트 기대 {event_expected_alpha_5d * 100:.1f}%")
    event_bonus = _event_type_bonus(event_type)
    if event_bonus:
        support_score += event_bonus
        support_reasons.append(f"이벤트 {event_type}")

    if data_quality_label == "보수적":
        support_score -= 1
        risk_reasons.append("데이터 품질 보수적")
    if alignment_score <= -1:
        support_score -= 2
        risk_reasons.append("맥락 역행")
    if positioning_mode == "보수":
        support_score -= 1
        risk_reasons.append("브리핑 모드 보수")

    recovering = False
    recovery_reasons: list[str] = []
    if intraday_edge_score >= float(getattr(SETTINGS, "CLOSE_SWING_MIN_RECOVERY_EDGE", 0.05)):
        recovering = True
        recovery_reasons.append(f"장중 회복 {intraday_edge_score:.2f}")
    if flow_state_score >= float(getattr(SETTINGS, "CLOSE_SWING_MIN_FLOW_SCORE", 0.55)):
        recovering = True
        recovery_reasons.append(f"수급 {flow_state_score:.2f}")
    if briefing_action in {"직접 후보", "눌림 후보"}:
        recovering = True
        recovery_reasons.append(f"브리핑 {briefing_action}")
    if alignment_score >= 1:
        recovering = True
        recovery_reasons.append("맥락 정렬")

    if signal_bias != "positive":
        return {
            "eligible": False,
            "decision": "blocked",
            "reason": f"signal_bias={signal_bias or 'neutral'}",
            "note": "양수 재료 공시가 아니라 종가 스윙 매수 대상에서 제외합니다.",
            "support_score": support_score,
            "briefing_action": _briefing_action_label(briefing_action),
            "price_change_pct": price_change_pct,
            "recovering": recovering,
            "recovery_reasons": recovery_reasons,
        }

    max_chase_pct = float(getattr(SETTINGS, "CLOSE_SWING_MAX_CHASE_PCT", 8.0))
    soft_chase_pct = float(getattr(SETTINGS, "CLOSE_SWING_SOFT_CHASE_PCT", 5.0))
    fail_drop_pct = float(getattr(SETTINGS, "CLOSE_SWING_FAIL_DROP_PCT", -4.0))
    negative_entry_pct = float(getattr(SETTINGS, "CLOSE_SWING_NEGATIVE_ENTRY_PCT", -1.5))
    min_support_score = int(getattr(SETTINGS, "CLOSE_SWING_MIN_SUPPORT_SCORE", 4))
    stale_event_min = int(getattr(SETTINGS, "CLOSE_SWING_STALE_EVENT_MIN", 210))
    max_event_age_min = int(getattr(SETTINGS, "CLOSE_SWING_MAX_EVENT_AGE_MIN", 600))
    min_liquidity_score = float(getattr(SETTINGS, "CLOSE_SWING_MIN_LIQUIDITY_SCORE", 0.45) or 0.45)
    require_price_signal = bool(getattr(SETTINGS, "CLOSE_SWING_REQUIRE_PRICE_SIGNAL", True))

    if require_price_signal and price_change_pct is None:
        return {
            "eligible": False,
            "decision": "blocked",
            "reason": "missing_price_signal",
            "note": "현재가/등락률을 확인하지 못해 종가 배팅 판단을 보수적으로 보류합니다.",
            "support_score": support_score,
            "briefing_action": _briefing_action_label(briefing_action),
            "price_change_pct": price_change_pct,
            "event_age_minutes": event_age_minutes,
            "recovering": recovering,
            "recovery_reasons": recovery_reasons,
            "support_reasons": support_reasons,
            "risk_reasons": risk_reasons,
            "liquidity_score": liquidity_score,
            "avg_turnover_20d": avg_turnover_20d,
        }

    if (
        (liquidity_score is not None and liquidity_score < min_liquidity_score)
        or (avg_turnover_20d is not None and avg_turnover_20d < min_avg_turnover_20d)
    ):
        liquidity_text = f"{liquidity_score:.2f}" if liquidity_score is not None else "-"
        turnover_text = f"{avg_turnover_20d:.4f}" if avg_turnover_20d is not None else "-"
        return {
            "eligible": False,
            "decision": "blocked",
            "reason": "illiquid_close_bet",
            "note": (
                f"최근 유동성 점수 `{liquidity_text}` 또는 평균 회전율 `{turnover_text}` 이 낮아 "
                "종가 배팅 체결 리스크를 줄이기 위해 제외합니다."
            ),
            "support_score": support_score,
            "briefing_action": _briefing_action_label(briefing_action),
            "price_change_pct": price_change_pct,
            "event_age_minutes": event_age_minutes,
            "recovering": recovering,
            "recovery_reasons": recovery_reasons,
            "support_reasons": support_reasons,
            "risk_reasons": risk_reasons,
            "liquidity_score": liquidity_score,
            "avg_turnover_20d": avg_turnover_20d,
        }

    if event_age_minutes is not None and event_age_minutes >= max_event_age_min and not recovering:
        return {
            "eligible": False,
            "decision": "blocked",
            "reason": f"stale_event_{event_age_minutes}m",
            "note": "공시가 나온 지 오래됐고 회복 신호도 약해 종가 스윙 진입 대상으로 보기 어렵습니다.",
            "support_score": support_score,
            "briefing_action": _briefing_action_label(briefing_action),
            "price_change_pct": price_change_pct,
            "event_age_minutes": event_age_minutes,
            "recovering": recovering,
            "recovery_reasons": recovery_reasons,
            "support_reasons": support_reasons,
            "risk_reasons": risk_reasons,
        }

    if price_change_pct is not None and price_change_pct >= max_chase_pct:
        return {
            "eligible": False,
            "decision": "blocked",
            "reason": f"overextended_{price_change_pct:.2f}pct",
            "note": "이미 가격이 크게 선반영돼 종가 추격 매수는 보수적으로 보는 편이 낫습니다.",
            "support_score": support_score,
            "briefing_action": _briefing_action_label(briefing_action),
            "price_change_pct": price_change_pct,
            "event_age_minutes": event_age_minutes,
            "recovering": recovering,
            "recovery_reasons": recovery_reasons,
            "support_reasons": support_reasons,
            "risk_reasons": risk_reasons,
        }

    if price_change_pct is not None and price_change_pct <= fail_drop_pct and not recovering:
        return {
            "eligible": False,
            "decision": "blocked",
            "reason": f"failed_recovery_{price_change_pct:.2f}pct",
            "note": "재료는 있지만 눌린 뒤 회복 확인이 부족해 종가 진입은 한 번 더 확인하는 편이 낫습니다.",
            "support_score": support_score,
            "briefing_action": _briefing_action_label(briefing_action),
            "price_change_pct": price_change_pct,
            "event_age_minutes": event_age_minutes,
            "recovering": recovering,
            "recovery_reasons": recovery_reasons,
            "support_reasons": support_reasons,
            "risk_reasons": risk_reasons,
        }

    required_score = min_support_score + 1 if price_change_pct is not None and price_change_pct >= soft_chase_pct else min_support_score
    if event_age_minutes is not None and event_age_minutes >= stale_event_min:
        required_score += 1
        risk_reasons.append(f"공시 경과 {event_age_minutes}m")
    if support_score < required_score:
        return {
            "eligible": False,
            "decision": "blocked",
            "reason": f"weak_quant_support_{support_score}",
            "note": "공시는 보이지만 종가 스윙으로 넘기기엔 카드·팩터·브리핑 교집합이 아직 약합니다.",
            "support_score": support_score,
            "briefing_action": _briefing_action_label(briefing_action),
            "price_change_pct": price_change_pct,
            "event_age_minutes": event_age_minutes,
            "recovering": recovering,
            "recovery_reasons": recovery_reasons,
            "support_reasons": support_reasons,
            "risk_reasons": risk_reasons,
        }

    if price_change_pct is not None and price_change_pct <= negative_entry_pct:
        decision = "drop_recovery_ready" if recovering else "watch_rebound"
        note = (
            "종가 기준 눌림 뒤 회복 신호가 보여 종가 스윙 진입 후보로 볼 만합니다."
            if recovering
            else "충분히 눌린 상태지만 회복 확인이 약해 재반등 확인이 먼저입니다."
        )
        eligible = recovering
    else:
        decision = "close_swing_ready"
        note = "과열 구간은 아니고 퀀트 교집합도 받쳐줘 종가 스윙 진입 후보로 볼 만합니다."
        eligible = True

    ranking_score = float(support_score * 100)
    ranking_score += card_score * 25.0
    ranking_score += factor_score * 20.0
    ranking_score += max(flow_state_score, 0.0) * 12.0
    ranking_score += max(intraday_edge_score, 0.0) * 40.0
    ranking_score += max(event_expected_alpha_5d, 0.0) * 100.0
    ranking_score += max(ml_pred_return_5d, 0.0) * 60.0
    ranking_score += max(liquidity_score or 0.0, 0.0) * 12.0
    if avg_turnover_20d is not None:
        ranking_score += min(max(avg_turnover_20d, 0.0), 0.02) * 300.0
    if event_age_minutes is not None:
        ranking_score += max(0.0, 30.0 - (event_age_minutes / 20.0))
    if price_change_pct is not None and price_change_pct > 0:
        ranking_score -= price_change_pct * 2.0
    if recovering:
        ranking_score += 10.0

    max_budget_krw = int(getattr(SETTINGS, "MAX_KRW_PER_TRADE", 300000) or 300000)
    budget_multiplier, budget_krw = _close_swing_budget(
        max_budget_krw,
        support_score=support_score,
        briefing_action=briefing_action,
        alignment_score=alignment_score,
        recovering=recovering,
        price_change_pct=price_change_pct,
        event_age_minutes=event_age_minutes,
        liquidity_score=liquidity_score,
        avg_turnover_20d=avg_turnover_20d,
    )
    risk_profile = _close_swing_risk_profile(
        event_type=event_type,
        support_score=support_score,
        recovering=recovering,
        price_change_pct=price_change_pct,
        event_age_minutes=event_age_minutes,
    )
    last_price = _last_price(broker, symbol)
    min_order_budget_krw = _one_share_budget_krw(last_price)
    budget_adjusted = False
    if min_order_budget_krw > 0:
        if min_order_budget_krw > max_budget_krw:
            return {
                "eligible": False,
                "decision": "blocked",
                "reason": "max_trade_budget_below_one_share",
                "note": (
                    f"현재가 기준 1주 예산 `{min_order_budget_krw:,}원`이 "
                    f"1회 최대 예산 `{max_budget_krw:,}원`을 넘어 종가 스윙 진입에서 제외합니다."
                ),
                "support_score": support_score,
                "briefing_action": _briefing_action_label(briefing_action),
                "briefing_score_total": briefing_score_total,
                "price_change_pct": price_change_pct,
                "event_age_minutes": event_age_minutes,
                "recovering": recovering,
                "recovery_reasons": recovery_reasons,
                "support_reasons": support_reasons,
                "risk_reasons": risk_reasons,
                "card_score": card_score,
                "factor_score": factor_score,
                "active_sources": active_sources,
                "liquidity_score": liquidity_score,
                "avg_turnover_20d": avg_turnover_20d,
                "flow_state_score": flow_state_score,
                "intraday_edge_score": intraday_edge_score,
                "ml_pred_return_5d": ml_pred_return_5d,
                "event_expected_alpha_5d": event_expected_alpha_5d,
                "ranking_score": round(ranking_score, 4),
                "budget_multiplier": budget_multiplier,
                "budget_krw": budget_krw,
                "min_order_budget_krw": min_order_budget_krw,
                "budget_adjusted": False,
                "take_profit_pct": risk_profile.get("take_profit_pct"),
                "stop_loss_pct": risk_profile.get("stop_loss_pct"),
                "stop_grace_min": risk_profile.get("stop_grace_min"),
                "open_recovery_min": risk_profile.get("open_recovery_min"),
                "risk_notes": list(risk_profile.get("risk_notes") or []),
            }
        if budget_krw < min_order_budget_krw:
            budget_krw = min_order_budget_krw
            budget_adjusted = True
            note = (
                f"{note} | 현재가 기준 1주 체결 가능 수준으로 예산을 `{budget_krw:,}원`까지 보정했습니다."
            ).strip(" |")

    return {
        "eligible": bool(eligible),
        "decision": decision if eligible else "blocked",
        "reason": decision if eligible else "watch_rebound",
        "note": note,
        "support_score": support_score,
        "briefing_action": _briefing_action_label(briefing_action),
        "briefing_score_total": briefing_score_total,
        "price_change_pct": price_change_pct,
        "event_age_minutes": event_age_minutes,
        "recovering": recovering,
        "recovery_reasons": recovery_reasons,
        "support_reasons": support_reasons,
        "risk_reasons": risk_reasons,
        "card_score": card_score,
        "factor_score": factor_score,
        "active_sources": active_sources,
        "liquidity_score": liquidity_score,
        "avg_turnover_20d": avg_turnover_20d,
        "flow_state_score": flow_state_score,
        "intraday_edge_score": intraday_edge_score,
        "ml_pred_return_5d": ml_pred_return_5d,
        "event_expected_alpha_5d": event_expected_alpha_5d,
        "ranking_score": round(ranking_score, 4),
        "budget_multiplier": budget_multiplier,
        "budget_krw": budget_krw,
        "min_order_budget_krw": min_order_budget_krw,
        "budget_adjusted": budget_adjusted,
        "take_profit_pct": risk_profile.get("take_profit_pct"),
        "stop_loss_pct": risk_profile.get("stop_loss_pct"),
        "stop_grace_min": risk_profile.get("stop_grace_min"),
        "open_recovery_min": risk_profile.get("open_recovery_min"),
        "risk_notes": list(risk_profile.get("risk_notes") or []),
    }
