from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import time
import warnings
from datetime import datetime
from typing import Any

from utils.slack import notify_error, send_slack

try:
    from config import SETTINGS
except Exception:
    from Disclosure.config import SETTINGS

try:
    from hybrid_rotation_engine import compute_relative_value_candidates, compute_sector_rotation, load_hybrid_inputs
except Exception:
    from Disclosure.hybrid_rotation_engine import compute_relative_value_candidates, compute_sector_rotation, load_hybrid_inputs

try:
    from sector_thesis import build_sector_thesis, save_sector_thesis
except Exception:
    from Disclosure.sector_thesis import build_sector_thesis, save_sector_thesis

try:
    from signals.wics_universe import load_effective_wics_sector_meta
except Exception:
    from Disclosure.signals.wics_universe import load_effective_wics_sector_meta

try:
    from google import genai as google_genai
except Exception:
    google_genai = None

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        import google.generativeai as google_generativeai
except Exception:
    google_generativeai = None


log = logging.getLogger("disclosure.market_briefing")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
RUNTIME_DIR = os.path.join(ROOT_DIR, "runtime")
if google_generativeai is not None and getattr(SETTINGS, "GEMINI_API_KEY", ""):
    google_generativeai.configure(api_key=getattr(SETTINGS, "GEMINI_API_KEY", ""))

INPUT_PATHS = {
    "factor": os.path.join(ROOT_DIR, "factors", "snapshots", "factor_summary_latest.json"),
    "card": os.path.join(ROOT_DIR, "cards", "stock_cards_latest.json"),
    "mart": os.path.join(ROOT_DIR, "marts", "daily_signal_mart_latest.json"),
    "valuation": os.path.join(ROOT_DIR, "valuation", "fair_value_snapshot_latest.json"),
    "news": os.path.join(ROOT_DIR, "news", "summaries", "stock_news_summary_latest.json"),
    "event": os.path.join(ROOT_DIR, "events", "reports", "disclosure_event_summary_latest.json"),
    "event_symbols": os.path.join(ROOT_DIR, "events", "reports", "disclosure_event_symbols_latest.json"),
    "macro": os.path.join(ROOT_DIR, "signals", "reports", "macro_news_report_latest.json"),
    "wics": os.path.join(ROOT_DIR, "signals", "reports", "wics_ai_report_latest.json"),
    "analyst": os.path.join(ROOT_DIR, "analyst_reports", "summaries", "analyst_report_summary_latest.json"),
    "flow_health": os.path.join(ROOT_DIR, "signals", "logs", "trading_flow_health_latest.json"),
}
WICS_LOG_DIR = os.path.join(ROOT_DIR, "signals", "logs")

STALE_LIMIT_MIN = {
    "factor": 24 * 60,
    "card": 18 * 60,
    "mart": 18 * 60,
    "valuation": 18 * 60,
    "news": 12 * 60,
    "event": 12 * 60,
    "event_symbols": 12 * 60,
    "macro": 12 * 60,
    "wics": 24 * 60,
    "analyst": 24 * 60,
    "flow_health": 60,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build one integrated market briefing from the latest factor/card/news/event/macro outputs.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print locally instead of sending to Slack.")
    parser.add_argument("--with-ai", action="store_true", help="Append a short AI commentary on top of the structured briefing.")
    parser.add_argument("--top-n", type=int, default=8, help="How many cross-signal candidates to keep.")
    parser.add_argument("--times", default="08:25,15:55,20:25", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _load_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        log.warning("failed to load json %s: %s", path, exc)
        return {}


def _load_latest_wics_fallback() -> tuple[dict[str, Any], dict[str, Any]]:
    def _rotation_label(item: dict[str, Any]) -> str:
        sector = item.get("sector_name") or "-"
        top_pick = item.get("top_pick") or "-"
        score = _safe_float(item.get("score"))
        smart_money_net = _safe_float(item.get("smart_money_net"))
        if abs(score) > 0:
            return f"{sector}({top_pick}) score {round(score, 1)}"
        return f"{sector}({top_pick}) 스마트머니 {int(round(smart_money_net))}억"

    def _derive_summary_from_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        bullish: list[dict[str, Any]] = []
        risky: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            sector_name = _clean_text(item.get("sector_name"))
            if not sector_name:
                continue
            flow = item.get("sector_flow") or {}
            foreign = _safe_float(flow.get("foreign", flow.get("f_3d", 0)))
            inst = _safe_float(flow.get("inst", flow.get("i_3d", 0)))
            retail = _safe_float(flow.get("retail", flow.get("r_3d", 0)))
            smart_money_net = foreign + inst
            score = _safe_float(item.get("score"))

            top_pick = "-"
            leaderboard = ((item.get("sector_features") or {}).get("leaderboard") or [])[:1]
            if leaderboard and isinstance(leaderboard[0], dict):
                top_pick = _clean_text(leaderboard[0].get("stock_name")) or "-"
            if top_pick == "-":
                stock_data = item.get("stock_data") or {}
                if isinstance(stock_data, dict) and stock_data:
                    ranked = sorted(
                        stock_data.items(),
                        key=lambda kv: (
                            _safe_float((kv[1] or {}).get("score", (kv[1] or {}).get("accumulation_score", 0))),
                            _safe_float((kv[1] or {}).get("smart_money", 0)),
                            _safe_float((kv[1] or {}).get("foreign", (kv[1] or {}).get("f_3d", 0)))
                            + _safe_float((kv[1] or {}).get("inst", (kv[1] or {}).get("i_3d", 0))),
                        ),
                        reverse=True,
                    )
                    if ranked:
                        top_pick = _clean_text(ranked[0][0]) or "-"

            entry = {
                "sector_name": sector_name,
                "score": round(score, 1),
                "smart_money_net": round(smart_money_net, 1),
                "dominant_actor": _clean_text((item.get("sector_features") or {}).get("dominant_actor")) or "-",
                "top_pick": top_pick,
            }
            if smart_money_net > 0:
                bullish.append(entry)
            if retail > 0 and smart_money_net <= 0:
                risky.append(entry)

        bullish.sort(key=lambda row: (row["score"], row["smart_money_net"]), reverse=True)
        risky.sort(key=lambda row: (row["score"], -row["smart_money_net"]), reverse=True)
        return bullish[:3], risky[:2]

    try:
        candidates = sorted(
            [
                os.path.join(WICS_LOG_DIR, name)
                for name in os.listdir(WICS_LOG_DIR)
                if name.startswith("wics_log_") and name.endswith(".json")
            ]
        )
    except Exception:
        return {}, {}
    if not candidates:
        return {}, {}

    path = candidates[-1]
    try:
        try:
            from signals.wics_ai_report import _build_rotation_structured_summary
        except Exception:
            from Disclosure.signals.wics_ai_report import _build_rotation_structured_summary

        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        if not isinstance(payload, list) or not payload:
            return {}, {}
        latest = payload[-1] if isinstance(payload[-1], dict) else {}
        summary = latest.get("summary") or {}
        top_rotation = summary.get("top_rotation_sectors") or []
        risk_sectors = summary.get("risk_sectors") or []
        if not top_rotation and not risk_sectors:
            raw_rows = latest.get("data") or []
            if isinstance(raw_rows, list) and raw_rows:
                top_rotation, risk_sectors = _derive_summary_from_rows(raw_rows)
        synthetic_summary = _build_rotation_structured_summary(
            [
                {
                    "date": _clean_text(latest.get("captured_at")) or os.path.basename(path).replace("wics_log_", "").replace(".json", ""),
                    "market_phase": _clean_text(summary.get("market_phase")) or _clean_text(latest.get("market_phase")) or "unknown",
                    "top_rotation": top_rotation,
                    "risk_sectors": risk_sectors,
                }
            ]
        )
        lines = []
        if top_rotation:
            top_text = " / ".join(
                _rotation_label(item)
                for item in top_rotation[:3]
                if item.get("sector_name")
            )
            if top_text:
                lines.append(f"- 상위 순환 섹터: {top_text}")
        if risk_sectors:
            risk_text = " / ".join(
                f"{item.get('sector_name')}({item.get('top_pick') or '-'})"
                for item in risk_sectors[:2]
                if item.get("sector_name")
            )
            if risk_text:
                lines.append(f"- 경계 섹터: {risk_text}")
        fallback_payload = {
            "generated_at": latest.get("captured_at") or "",
            "report_text": "\n".join(lines),
            "source": "wics_raw_log",
            "top_rotation_sectors": top_rotation,
            "risk_sectors": risk_sectors,
            "repeat_leaders": synthetic_summary.get("repeat_leaders", []),
            "market_phase_counts": synthetic_summary.get("market_phase_counts", {}),
            "dominant_theme": synthetic_summary.get("dominant_theme", ""),
            "market_mode": synthetic_summary.get("market_mode", "중립"),
            "confidence_score": synthetic_summary.get("confidence_score", 0),
            "rotation_line": synthetic_summary.get("rotation_line", ""),
            "watch_ideas": synthetic_summary.get("watch_ideas", []),
            "caution_ideas": synthetic_summary.get("caution_ideas", []),
            "universe_summary": latest.get("universe_summary") or ((latest.get("universe_meta") or {}).get("summary") or {}),
        }
        freshness = {
            "exists": True,
            "age_min": int(max(0.0, time.time() - os.path.getmtime(path)) // 60),
            "status": "fresh",
            "path": path,
        }
        if freshness["age_min"] > STALE_LIMIT_MIN.get("wics", 24 * 60):
            freshness["status"] = "stale"
        return fallback_payload, freshness
    except Exception as exc:
        log.warning("failed to load WICS fallback from raw log: %s", exc)
        return {}, {}


def _load_inputs() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    payloads: dict[str, dict[str, Any]] = {}
    freshness: dict[str, dict[str, Any]] = {}
    now_ts = time.time()
    for name, path in INPUT_PATHS.items():
        payload = _load_json(path)
        if name == "factor" and payload:
            try:
                try:
                    from factor_pipeline import normalize_factor_summary
                except Exception:
                    from Disclosure.factor_pipeline import normalize_factor_summary

                payload = normalize_factor_summary(payload)
            except Exception as exc:
                log.debug("factor summary normalization skipped: %s", exc)
        payloads[name] = payload
        if not os.path.exists(path):
            freshness[name] = {"exists": False, "age_min": None, "status": "missing", "path": path}
            continue
        age_min = int(max(0.0, now_ts - os.path.getmtime(path)) // 60)
        freshness[name] = {
            "exists": True,
            "age_min": age_min,
            "status": "stale" if age_min > STALE_LIMIT_MIN.get(name, 24 * 60) else "fresh",
            "path": path,
        }
    if not payloads.get("wics"):
        wics_fallback, wics_meta = _load_latest_wics_fallback()
        if wics_fallback:
            payloads["wics"] = wics_fallback
            freshness["wics"] = wics_meta
    return payloads, freshness


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() == "nan":
        return ""
    return text


def _canonical_sector_name(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    text = re.sub(r"^\d+\.\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _report_excerpt(payload: dict[str, Any], max_lines: int = 5) -> list[str]:
    text = str(payload.get("report_text") or "").strip()
    if not text:
        return []
    lines: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("🌍") or line.startswith("🌪️"):
            continue
        if line.startswith("*1.") or line.startswith("*2.") or line.startswith("*3."):
            continue
        lines.append(line)
        if len(lines) >= max_lines:
            break
    return lines


def _build_wics_structured_lines(payload: dict[str, Any]) -> list[str]:
    if not payload:
        return []
    lines: list[str] = []
    market_mode = _clean_text(payload.get("market_mode"))
    confidence = _safe_int(payload.get("confidence_score"))
    dominant_theme = _clean_text(payload.get("dominant_theme"))
    rotation_line = _clean_text(payload.get("rotation_line"))
    if market_mode or confidence or dominant_theme:
        lines.append(
            f"- 시장 모드: {market_mode or '-'} | 확신도 {confidence}/100 | 중심 테마 {dominant_theme or '-'}"
        )
    if rotation_line:
        lines.append(f"- {rotation_line}")

    top_rotation = payload.get("top_rotation_sectors") or []
    if top_rotation:
        top_text = " / ".join(
            f"{_clean_text(item.get('sector_short') or item.get('sector_name'))}"
            f"({_clean_text(item.get('top_pick')) or '-'}|{_clean_text(item.get('leader_regime')) or '순환형'}"
            f"|{_clean_text(item.get('universe_status_label')) or '유동형'}) "
            f"{max(1, int(_safe_float(item.get('appearances'), 1)))}일"
            for item in top_rotation[:3]
            if _clean_text(item.get("sector_name"))
        )
        if top_text:
            lines.append(f"- 상위 순환 섹터: {top_text}")

    watch_ideas = payload.get("watch_ideas") or []
    if watch_ideas:
        idea_text = " / ".join(
            f"{_clean_text(item.get('sector'))}"
            f"({(_clean_text((item.get('leaders') or ['-'])[0]) or '-')}"
            f"|{_clean_text(item.get('universe_status_label')) or '-'})"
            for item in watch_ideas[:3]
            if _clean_text(item.get("sector"))
        )
        if idea_text:
            lines.append(f"- 지금 볼 섹터: {idea_text}")

    risk_sectors = payload.get("risk_sectors") or []
    if risk_sectors:
        risk_text = " / ".join(
            f"{_clean_text(item.get('sector_short') or item.get('sector_name'))}({_clean_text(item.get('top_pick')) or '-'})"
            for item in risk_sectors[:2]
            if _clean_text(item.get("sector_name"))
        )
        if risk_text:
            lines.append(f"- 경계 섹터: {risk_text}")
    universe_summary = payload.get("universe_summary") or {}
    if universe_summary:
        lines.append(
            f"- 유니버스 조정: 동적 편입 {universe_summary.get('dynamic_symbol_count', 0)}개 | "
            f"섹터 불일치 제외 {universe_summary.get('mismatch_symbol_count', 0)}개"
        )
        lines.append(
            f"- 유니버스 안정도: 안정형 {universe_summary.get('stable_sector_count', 0)}개 | "
            f"유동형 {universe_summary.get('adaptive_sector_count', 0)}개 | "
            f"재점검 {universe_summary.get('review_sector_count', 0)}개 | "
            f"상태 {universe_summary.get('universe_regime', '-')} | "
            f"평균 겹침률 {universe_summary.get('history_avg_overlap', 1.0)} | "
            f"표본 {universe_summary.get('history_confidence_label', '없음')} "
            f"({universe_summary.get('history_day_count', 1)}일)"
        )
        turnover = universe_summary.get("turnover") or {}
        if turnover:
            lines.append(
                f"- 전회 대비: 새 편입 {turnover.get('added_symbol_count', 0)}개 | "
                f"제외 {turnover.get('removed_symbol_count', 0)}개"
            )
        review_rows = universe_summary.get("review_sectors") or []
        if review_rows:
            review_text = " / ".join(row.get("sector") for row in review_rows[:3] if row.get("sector"))
            if review_text:
                lines.append(f"- 재점검 섹터: {review_text}")
    return lines


def _build_valuation_structured_lines(payload: dict[str, Any]) -> list[str]:
    if not payload:
        return []
    lines: list[str] = []
    coverage_count = _safe_int(payload.get("coverage_count"))
    row_count = _safe_int(payload.get("row_count"))
    if coverage_count or row_count:
        lines.append(f"- 적정가 산출: {coverage_count} / {row_count}종목")
    top_discount = payload.get("top_discount") or []
    if top_discount:
        discount_text = " / ".join(
            f"{_clean_text(row.get('name'))}({_safe_float(row.get('fair_value_gap_pct')):+.1f}%)"
            for row in top_discount[:3]
            if _clean_text(row.get("name"))
        )
        if discount_text:
            lines.append(f"- 기준 적정가 대비 할인: {discount_text}")
    top_premium = payload.get("top_premium") or []
    if top_premium:
        premium_text = " / ".join(
            f"{_clean_text(row.get('name'))}({_safe_float(row.get('fair_value_gap_pct')):+.1f}%)"
            for row in top_premium[:2]
            if _clean_text(row.get("name"))
        )
        if premium_text:
            lines.append(f"- 선반영 가능성: {premium_text}")
    sector_rows = payload.get("by_sector") or []
    if sector_rows:
        sector_text = " / ".join(
            f"{_clean_text(row.get('sector'))}(평균 {_safe_float(row.get('avg_gap_pct')):+.1f}%)"
            for row in sector_rows[:3]
            if _clean_text(row.get("sector"))
        )
        if sector_text:
            lines.append(f"- 적정가 기준 상단 섹터: {sector_text}")
    return lines


def _alignment_label(score: int) -> str:
    if score >= 3:
        return "강한 정렬"
    if score >= 1:
        return "우호 정렬"
    if score <= -3:
        return "강한 역행"
    if score <= -1:
        return "주의"
    return "중립"


def _build_context_alignment(
    macro_summary: dict[str, Any],
    wics_summary: dict[str, Any],
) -> dict[str, Any]:
    universe_summary = (wics_summary or {}).get("universe_summary") or {}
    history_label = _clean_text(universe_summary.get("history_confidence_label")) or ("없음" if wics_summary else "")
    history_day_count = _safe_int(universe_summary.get("history_day_count"))
    if wics_summary:
        history_day_count = max(history_day_count, 1)
    universe_regime = _clean_text(universe_summary.get("universe_regime")) or "-"
    history_penalty = 0
    history_note = ""
    if wics_summary:
        if history_label == "없음":
            history_penalty = 8
            history_note = "WICS 히스토리 표본이 거의 없어 섹터 해석은 보조적으로 보는 편이 낫습니다."
        elif history_label == "예비":
            history_penalty = 4
            history_note = "WICS 히스토리 표본이 아직 얕아 섹터 해석은 한 단계 보수적으로 보는 편이 낫습니다."
    by_sector: dict[str, dict[str, Any]] = {}

    def _ensure(sector: str) -> dict[str, Any] | None:
        key = _canonical_sector_name(sector)
        if not key:
            return None
        return by_sector.setdefault(
            key,
            {
                "sector": key,
                "score": 0,
                "support_sources": [],
                "risk_sources": [],
            },
        )

    def _add_support(sector: str, source: str, weight: int = 1) -> None:
        item = _ensure(sector)
        if item is None:
            return
        item["score"] += int(weight)
        if source not in item["support_sources"]:
            item["support_sources"].append(source)

    def _add_risk(sector: str, source: str, weight: int = 1) -> None:
        item = _ensure(sector)
        if item is None:
            return
        item["score"] -= int(weight)
        if source not in item["risk_sources"]:
            item["risk_sources"].append(source)

    for sector in macro_summary.get("watch_sectors", []) or []:
        _add_support(sector, "macro_watch")
    for item in macro_summary.get("watch_ideas", []) or []:
        _add_support(item.get("sector"), "macro_watch_idea")
    for sector in macro_summary.get("caution_sectors", []) or []:
        _add_risk(sector, "macro_caution")
    for item in macro_summary.get("caution_ideas", []) or []:
        _add_risk(item.get("sector"), "macro_caution_idea")

    for item in wics_summary.get("top_rotation_sectors", []) or []:
        _add_support(item.get("sector_name"), "wics_rotation")
    for item in wics_summary.get("watch_ideas", []) or []:
        _add_support(item.get("sector"), "wics_watch")
    for item in wics_summary.get("risk_sectors", []) or []:
        _add_risk(item.get("sector_name"), "wics_risk")
    for item in wics_summary.get("caution_ideas", []) or []:
        _add_risk(item.get("sector"), "wics_caution")

    rows = []
    for item in by_sector.values():
        item["label"] = _alignment_label(int(item.get("score", 0)))
        rows.append(item)
    rows.sort(
        key=lambda row: (
            abs(int(row.get("score", 0))),
            len(row.get("support_sources", [])),
            len(row.get("risk_sources", [])),
            row.get("sector", ""),
        ),
        reverse=True,
    )
    top_support = [row for row in rows if int(row.get("score", 0)) > 0][:4]
    top_risk = [row for row in rows if int(row.get("score", 0)) < 0][:3]
    macro_conf = _safe_int(macro_summary.get("confidence_score"))
    wics_conf = _safe_int(wics_summary.get("confidence_score"))
    confidence_score = max(macro_conf, wics_conf)
    if wics_summary and history_penalty > 0:
        confidence_score = max(0, confidence_score - history_penalty)
    return {
        "by_sector": by_sector,
        "top_support": top_support,
        "top_risk": top_risk,
        "market_mode": _clean_text(macro_summary.get("market_mode")) or _clean_text(wics_summary.get("market_mode")) or "중립",
        "confidence_score": confidence_score,
        "wics_history_confidence_label": history_label or "없음",
        "wics_history_day_count": history_day_count,
        "wics_universe_regime": universe_regime,
        "wics_confidence_note": history_note,
    }


def _decorate_sector_view_with_alignment(
    sector_view: list[dict[str, Any]],
    alignment: dict[str, Any],
) -> list[dict[str, Any]]:
    by_sector = alignment.get("by_sector", {}) if isinstance(alignment, dict) else {}
    decorated: list[dict[str, Any]] = []
    for row in sector_view:
        item = dict(row)
        key = _canonical_sector_name(item.get("sector"))
        align = by_sector.get(key, {})
        item["alignment_score"] = int(align.get("score", 0))
        item["alignment_label"] = align.get("label", "중립")
        decorated.append(item)
    decorated.sort(
        key=lambda row: (
            row.get("alignment_score", 0),
            row.get("factor_score", 0) + row.get("card_score", 0) + row.get("ml_score", 0),
            row.get("count", 0),
        ),
        reverse=True,
    )
    return decorated


def _decorate_candidates_with_alignment(
    candidates: list[dict[str, Any]],
    alignment: dict[str, Any],
) -> list[dict[str, Any]]:
    by_sector = alignment.get("by_sector", {}) if isinstance(alignment, dict) else {}
    decorated: list[dict[str, Any]] = []
    for row in candidates:
        item = dict(row)
        key = _canonical_sector_name(item.get("sector"))
        align = by_sector.get(key, {})
        score = int(align.get("score", 0))
        label = align.get("label", "중립")
        item["alignment_score"] = score
        item["alignment_label"] = label
        item["alignment_support"] = list(align.get("support_sources", []))
        item["alignment_risk"] = list(align.get("risk_sources", []))
        if score > 0:
            note = f"맥락 정렬 {label} / " + ", ".join(item["alignment_support"][:2])
            if note not in item["notes"]:
                item["notes"].append(note)
        elif score < 0:
            note = f"맥락 경계 {label} / " + ", ".join(item["alignment_risk"][:2])
            if note not in item["notes"]:
                item["notes"].append(note)
        decorated.append(item)
    return decorated


def _upsert_candidate(
    bucket: dict[str, dict[str, Any]],
    *,
    symbol: str,
    name: str,
    sector: str,
    source: str,
    score: float,
    note: str,
) -> None:
    if not symbol:
        return
    item = bucket.setdefault(
        symbol,
        {
            "symbol": symbol,
            "name": name or symbol,
            "sector": sector or "Unknown",
            "source_count": 0,
            "source_hits": [],
            "source_scores": {},
            "score_total": 0.0,
            "notes": [],
        },
    )
    if source not in item["source_hits"]:
        item["source_hits"].append(source)
        item["source_count"] += 1
    item["score_total"] += float(score)
    item["source_scores"][source] = round(float(score), 4)
    if note and note not in item["notes"]:
        item["notes"].append(note)
    if name and len(name) > len(item.get("name") or ""):
        item["name"] = name
    if sector and sector != "Unknown":
        item["sector"] = sector


def _build_sector_view(factor_summary: dict[str, Any], card_summary: dict[str, Any]) -> list[dict[str, Any]]:
    bucket: dict[str, dict[str, Any]] = {}

    for row in factor_summary.get("top_sectors", [])[:8]:
        sector = _clean_text(row.get("sector"))
        if not sector:
            continue
        item = bucket.setdefault(sector, {"sector": sector, "factor_score": 0.0, "card_score": 0.0, "ml_score": 0.0, "count": 0})
        item["factor_score"] = _safe_float(row.get("avg_composite_score"))
        item["count"] = max(item["count"], _safe_int(row.get("count")))

    for row in card_summary.get("sector_recommendations", [])[:8]:
        sector = _clean_text(row.get("sector"))
        if not sector:
            continue
        item = bucket.setdefault(sector, {"sector": sector, "factor_score": 0.0, "card_score": 0.0, "ml_score": 0.0, "count": 0})
        item["card_score"] = _safe_float(row.get("avg_card_score"))
        item["ml_score"] = _safe_float(row.get("avg_ml_sector_score"))
        item["count"] = max(item["count"], _safe_int(row.get("count")))

    rows = list(bucket.values())
    rows.sort(key=lambda row: (row["factor_score"] + row["card_score"] + row["ml_score"], row["count"]), reverse=True)
    return rows[:6]


def _build_candidate_view(
    factor_summary: dict[str, Any],
    card_summary: dict[str, Any],
    mart_summary: dict[str, Any],
    analyst_summary: dict[str, Any],
    news_summary: dict[str, Any],
    event_symbol_summary: dict[str, Any],
    top_n: int,
) -> list[dict[str, Any]]:
    bucket: dict[str, dict[str, Any]] = {}

    for row in factor_summary.get("portfolio_candidates", [])[:20]:
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector=_clean_text(row.get("sector")),
            source="factor",
            score=_safe_float(row.get("composite_score")) * 3.0,
            note=f"팩터 {round(_safe_float(row.get('composite_score')), 3)}",
        )

    for row in card_summary.get("cards", [])[:20]:
        card_score = round(_safe_float(row.get("card_score")), 3)
        card_note = f"종목 점검 {card_score}"
        flow_confidence = _safe_float(row.get("flow_confidence_score"))
        if flow_confidence > 0:
            card_note += f" / 수급확신 {int(round(flow_confidence * 100.0))}/100"
        if bool(row.get("flow_fallback_used")):
            card_note += " / 보강 수급(보수 반영)"
        if _clean_text(row.get("fair_value_base")):
            card_note += (
                f" / 기준 적정가 괴리 {_safe_float(row.get('fair_value_gap_pct')):+.1f}%"
                f" / 산출 신뢰 {int(round(_safe_float(row.get('fair_value_confidence_score')) * 100.0))}/100"
            )
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector=_clean_text(row.get("sector")),
            source="card",
            score=_safe_float(row.get("card_score")) * 3.0,
            note=card_note,
        )

    for row in mart_summary.get("top_symbols", [])[:20]:
        mart_note = f"장마감 기준 {round(_safe_float(row.get('card_score')), 3)}"
        if _clean_text(row.get("fair_value_base")):
            mart_note += f" / 기준 적정가 괴리 {_safe_float(row.get('fair_value_gap_pct')):+.1f}%"
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector=_clean_text(row.get("sector")),
            source="mart",
            score=_safe_float(row.get("card_score")) * 2.0,
            note=mart_note,
        )

    for row in analyst_summary.get("top_stocks", [])[:20]:
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector=_clean_text(row.get("sector")),
            source="analyst",
            score=min(_safe_float(row.get("conviction_score")), 15.0) / 10.0,
            note=f"애널 conviction {round(_safe_float(row.get('conviction_score')), 2)}",
        )

    for row in news_summary.get("top_positive", [])[:20]:
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector="",
            source="news",
            score=min(_safe_float(row.get("conviction_score")), 10.0) / 10.0,
            note=f"뉴스 conviction {round(_safe_float(row.get('conviction_score')), 2)}",
        )

    for row in event_symbol_summary.get("top_symbols", [])[:20]:
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector="",
            source="event",
            score=max(0.0, _safe_float(row.get("event_signal_score"))),
            note=(
                f"이벤트 {row.get('latest_event_type') or '-'} / "
                f"bias {row.get('latest_signal_bias') or '-'} / "
                f"5d {round(_safe_float(row.get('avg_ret_5d')), 2)}"
            ),
        )

    for row in card_summary.get("event_leaders", [])[:8]:
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector=_clean_text(row.get("sector")),
            source="event",
            score=abs(_safe_float(row.get("event_alpha_score"))),
            note=f"이벤트 {row.get('event_last_type') or '-'} / alpha {round(_safe_float(row.get('event_alpha_score')), 2)}",
        )

    for row in card_summary.get("intraday_leaders", [])[:8]:
        _upsert_candidate(
            bucket,
            symbol=_norm_symbol(row.get("symbol")),
            name=_clean_text(row.get("name")),
            sector=_clean_text(row.get("sector")),
            source="intraday",
            score=_safe_float(row.get("flow_intraday_edge_score")),
            note=f"장중수급 {round(_safe_float(row.get('flow_intraday_edge_score')), 2)}",
        )

    rows = list(bucket.values())
    rows.sort(
        key=lambda row: (
            row["source_count"],
            row["score_total"],
            "intraday" in row["source_hits"],
            "event" in row["source_hits"],
        ),
        reverse=True,
    )
    for row in rows:
        row["score_total"] = round(row["score_total"], 4)
    return rows[:top_n]


def _decorate_sector_view_with_thesis(
    sector_view: list[dict[str, Any]],
    sector_thesis: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    thesis_map = ((sector_thesis or {}).get("by_sector") or {}) if isinstance(sector_thesis, dict) else {}
    if not thesis_map:
        return sector_view
    decorated: list[dict[str, Any]] = []
    for row in sector_view:
        item = dict(row)
        thesis = thesis_map.get(_canonical_sector_name(item.get("sector"))) or {}
        if thesis:
            item["flow_lens_score"] = _safe_float(thesis.get("flow_lens_score"))
            item["quant_lens_score"] = _safe_float(thesis.get("quant_lens_score"))
            item["macro_lens_score"] = _safe_float(thesis.get("macro_lens_score"))
            item["sector_final_label"] = _clean_text(thesis.get("final_label"))
            item["sector_agreement_level"] = _clean_text(thesis.get("agreement_level"))
            item["sector_action_hint"] = _clean_text(thesis.get("action_hint"))
            item["sector_human_summary"] = _clean_text(thesis.get("human_summary"))
            item["sector_final_score"] = _safe_float(thesis.get("final_sector_score"))
        decorated.append(item)
    return decorated


def _decorate_with_wics_sector_meta(
    rows: list[dict[str, Any]],
    wics_sector_meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    meta_map = wics_sector_meta or {}
    if not meta_map:
        return rows
    decorated: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        meta = meta_map.get(_canonical_sector_name(item.get("sector"))) or {}
        if meta:
            item["universe_status_label"] = _clean_text(meta.get("universe_status_label"))
            item["universe_status_reason"] = _clean_text(meta.get("universe_status_reason"))
            item["wics_dynamic_stability"] = _safe_float(meta.get("avg_dynamic_stability"))
            item["wics_dynamic_count"] = _safe_int(meta.get("dynamic_count"))
            item["wics_history_confidence_label_sector"] = _clean_text(meta.get("history_confidence_label"))
        decorated.append(item)
    return decorated


def _decorate_candidates_with_sector_thesis(
    candidates: list[dict[str, Any]],
    sector_thesis: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    thesis_map = ((sector_thesis or {}).get("by_sector") or {}) if isinstance(sector_thesis, dict) else {}
    if not thesis_map:
        return candidates
    decorated: list[dict[str, Any]] = []
    for row in candidates:
        item = dict(row)
        thesis = thesis_map.get(_canonical_sector_name(item.get("sector"))) or {}
        if thesis:
            item["sector_final_label"] = _clean_text(thesis.get("final_label"))
            item["sector_action_hint"] = _clean_text(thesis.get("action_hint"))
            item["sector_human_summary"] = _clean_text(thesis.get("human_summary"))
            item["flow_lens_score"] = _safe_float(thesis.get("flow_lens_score"))
            item["quant_lens_score"] = _safe_float(thesis.get("quant_lens_score"))
            item["macro_lens_score"] = _safe_float(thesis.get("macro_lens_score"))
            note = f"공통 결론 {item['sector_final_label']} / {item['sector_action_hint'] or item['sector_human_summary']}"
            if note not in item["notes"]:
                item["notes"].append(note)
        decorated.append(item)
    return decorated


def _append_wics_stability_notes(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decorated: list[dict[str, Any]] = []
    for row in candidates:
        item = dict(row)
        status_label = _clean_text(item.get("universe_status_label"))
        status_reason = _clean_text(item.get("universe_status_reason"))
        dynamic_count = _safe_int(item.get("wics_dynamic_count"))
        dynamic_stability = _safe_float(item.get("wics_dynamic_stability"))
        if status_label:
            note = f"WICS 바스켓 {status_label}"
            if dynamic_count > 0:
                note += f" / 동적안정도 {int(round(dynamic_stability * 100.0))}/100"
            if status_reason:
                note += f" / {status_reason}"
            if note not in item["notes"]:
                item["notes"].append(note)
        decorated.append(item)
    return decorated


def _build_sector_thesis_lines(sector_thesis: dict[str, Any], *, limit: int = 4) -> list[str]:
    rows = list((sector_thesis or {}).get("top_sectors") or [])[:limit]
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"  • {row.get('sector')} | 수급 {round(_safe_float(row.get('flow_lens_score')), 1)} | "
            f"퀀트 {round(_safe_float(row.get('quant_lens_score')), 1)} | 매크로 {round(_safe_float(row.get('macro_lens_score')), 1)} | "
            f"결론 {row.get('final_label') or '-'}"
        )
        if _clean_text(row.get("human_summary")):
            lines.append(f"    {_clean_text(row.get('human_summary'))}")
    return lines


def _build_confidence(
    inputs: dict[str, dict[str, Any]],
    freshness: dict[str, dict[str, Any]],
    candidates: list[dict[str, Any]],
    sector_view: list[dict[str, Any]],
    risk_flags: list[str],
    context_alignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    score = 50

    for key in ("factor", "card", "mart", "analyst"):
        if inputs.get(key):
            score += 6
    for key in ("macro", "wics", "news", "event_symbols"):
        if inputs.get(key):
            score += 4

    wics_confidence = _safe_int((inputs.get("wics") or {}).get("confidence_score"))
    if wics_confidence >= 70:
        score += 3
    elif 0 < wics_confidence < 45:
        score -= 3

    if candidates:
        score += min(12, _safe_int(candidates[0].get("source_count")) * 4)
    if len(candidates) >= 3 and sum(_safe_int(row.get("source_count")) >= 2 for row in candidates[:3]) >= 2:
        score += 6
    if sector_view:
        score += 4
    if candidates:
        aligned_top = sum(int(row.get("alignment_score", 0)) > 0 for row in candidates[:3])
        risk_top = sum(int(row.get("alignment_score", 0)) < 0 for row in candidates[:3])
        score += min(6, aligned_top * 2)
        score -= min(6, risk_top * 2)

    wics_history_label = _clean_text((context_alignment or {}).get("wics_history_confidence_label"))
    if inputs.get("wics"):
        if wics_history_label == "예비":
            score -= 4
        elif wics_history_label == "없음":
            score -= 8

    stale_count = sum(1 for meta in freshness.values() if meta.get("status") == "stale")
    score -= min(16, stale_count * 3)

    flow_health = inputs.get("flow_health") or {}
    if _safe_int(flow_health.get("snapshot_logged")) > 0:
        score += 5
    elif _safe_int(flow_health.get("total_updates")) <= 0:
        score -= 8

    factor_dyn = ((inputs.get("factor") or {}).get("dynamic_factor_weights") or {}).get("status")
    if str(factor_dyn or "").lower() == "fallback":
        score -= 6

    score -= min(25, len(risk_flags) * 4)
    score = max(0, min(100, score))

    if score >= 80:
        label = "높음"
    elif score >= 60:
        label = "중간"
    else:
        label = "보수적"

    reason_parts: list[str] = []
    if candidates:
        reason_parts.append(f"상위 후보 교집합 {candidates[0].get('source_count', 0)}개")
    if candidates and any(int(row.get("alignment_score", 0)) > 0 for row in candidates[:3]):
        reason_parts.append("상위 후보 맥락 정렬")
    elif candidates and any(int(row.get("alignment_score", 0)) < 0 for row in candidates[:3]):
        reason_parts.append("상위 후보 맥락 엇갈림")
    if inputs.get("macro"):
        reason_parts.append("매크로 해설 있음")
    if inputs.get("wics"):
        reason_parts.append("섹터 순환 리포트 있음")
    if wics_history_label == "예비":
        reason_parts.append("WICS 표본 예비")
    elif wics_history_label == "없음":
        reason_parts.append("WICS 표본 없음")
    if not inputs.get("news"):
        reason_parts.append("뉴스 latest 비어 있음")
    if str(factor_dyn or "").lower() == "fallback":
        reason_parts.append("동적 팩터 기본값 복귀")
    if _safe_int(flow_health.get("snapshot_logged")) <= 0:
        reason_parts.append("실시간 스냅샷 약함")

    return {"score": score, "label": label, "reason": " | ".join(reason_parts[:4])}


def _build_risk_flags(
    inputs: dict[str, dict[str, Any]],
    freshness: dict[str, dict[str, Any]],
    sector_view: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    context_alignment: dict[str, Any] | None = None,
) -> list[str]:
    flags: list[str] = []
    missing = [name for name, payload in inputs.items() if not payload and name != "flow_health"]
    if missing:
        flags.append("최신 산출물이 비어 있습니다: " + ", ".join(missing))
    stale = [name for name, meta in freshness.items() if meta.get("status") == "stale"]
    if stale:
        flags.append("최신 산출물 중 오래된 파일이 있습니다: " + ", ".join(stale))

    factor_summary = inputs.get("factor") or {}
    card_summary = inputs.get("card") or {}
    mart_summary = inputs.get("mart") or {}
    event_summary = inputs.get("event") or {}
    flow_health = inputs.get("flow_health") or {}

    factor_dyn = ((factor_summary.get("dynamic_factor_weights") or {}).get("status") or "").lower()
    if factor_dyn and factor_dyn != "ok":
        flags.append(f"팩터 비중 조절이 `{factor_dyn}` 상태입니다.")

    counts = card_summary.get("counts") or {}
    if _safe_int(counts.get("flow")) == 0:
        flags.append("종목 점검표에 수급 점수가 비어 있습니다.")
    if _safe_int(counts.get("intraday")) == 0:
        flags.append("장중 수급 샘플이 비어 있어 타이밍 판단은 약합니다.")

    if _safe_int((event_summary.get("metadata") or {}).get("pending_price_records")) > 0:
        flags.append(f"이벤트 백테스트 미가격 반영 건이 {_safe_int((event_summary.get('metadata') or {}).get('pending_price_records'))}건 남아 있습니다.")

    if flow_health:
        if _safe_int(flow_health.get("snapshot_logged")) <= 0 and _safe_int(flow_health.get("flow_tick_logged")) > 0:
            flags.append("실시간 틱은 들어오지만 스냅샷 로그가 충분히 남지 않고 있습니다.")
        elif _safe_int(flow_health.get("total_updates")) <= 0:
            flags.append("실시간 웹소켓 입력이 거의 없어 수급 리포트 신뢰도가 낮습니다.")

    if sector_view and sector_view[0].get("count", 0) <= 2:
        flags.append("섹터 리더가 소수 종목에 집중돼 있어 편중 해석을 주의해야 합니다.")

    if candidates and candidates[0].get("source_count", 0) < 2:
        flags.append("교집합 후보가 약해 오늘은 단일 신호 과신을 피하는 편이 좋습니다.")
    if candidates and all(int(row.get("alignment_score", 0)) < 0 for row in candidates[:2]):
        flags.append("상위 후보가 매크로/WICS 맥락과 어긋나 있어 추격보다 관찰이 우선입니다.")
    top_risk = ((context_alignment or {}).get("top_risk") or [])[:2]
    if top_risk:
        risk_text = ", ".join(row.get("sector", "-") for row in top_risk if row.get("sector"))
        if risk_text:
            flags.append(f"맥락 경계 섹터가 남아 있습니다: {risk_text}")
    wics_history_label = _clean_text((context_alignment or {}).get("wics_history_confidence_label"))
    if inputs.get("wics") and wics_history_label in {"예비", "없음"}:
        flags.append(
            f"WICS 히스토리 표본이 `{wics_history_label}` 상태라 섹터 해석은 보조적으로만 두는 편이 낫습니다."
        )

    return flags


def _build_positioning_mode(
    confidence: dict[str, Any],
    candidates: list[dict[str, Any]],
    freshness: dict[str, dict[str, Any]],
) -> dict[str, str]:
    score = _safe_int(confidence.get("score"))
    top_source_count = _safe_int(candidates[0].get("source_count")) if candidates else 0
    stale_count = sum(1 for meta in freshness.values() if meta.get("status") == "stale")
    missing_count = sum(1 for meta in freshness.values() if meta.get("status") == "missing")

    if score >= 75 and top_source_count >= 3 and stale_count <= 1 and missing_count <= 1:
        return {"mode": "공격", "guidance": "교집합 상위 후보 중심으로 분할 진입을 허용할 만한 날입니다."}
    if score >= 58 and top_source_count >= 2:
        return {"mode": "중립", "guidance": "강한 후보만 좁게 보고, 새 재료주는 소액 탐색으로 접근하는 편이 좋습니다."}
    return {"mode": "보수", "guidance": "신호 확인용 관찰과 비중 축소가 우선입니다. 단일 리포트 과신은 피하는 편이 좋습니다."}


def _build_freshness_lines(freshness: dict[str, dict[str, Any]]) -> list[str]:
    order = ["factor", "card", "mart", "analyst", "news", "event", "event_symbols", "macro", "wics", "flow_health"]
    rows: list[str] = []
    for name in order:
        meta = freshness.get(name) or {}
        age = meta.get("age_min")
        rows.append(f"- {name}: {meta.get('status', 'missing')} | age {'-' if age is None else str(age) + 'm'}")
    return rows


def _build_action_hints(
    candidates: list[dict[str, Any]],
    confidence_score: int,
    data_quality_label: str,
    wics_history_confidence_label: str = "",
) -> tuple[list[str], list[dict[str, Any]]]:
    hints: list[str] = []
    structured: list[dict[str, Any]] = []
    for row in candidates[:5]:
        sources = row.get("source_hits", [])
        source_set = set(sources)
        action, note = _candidate_action(row, confidence_score, data_quality_label, wics_history_confidence_label)
        if {"factor", "card", "mart"}.issubset(source_set):
            reason = "팩터·종합진단·일일마트가 동시에 상위권인 기본 체력 교집합입니다."
        elif {"factor", "card", "analyst"}.issubset(source_set):
            reason = "팩터·진단표·애널이 동시에 받쳐주는 교집합 후보입니다."
        elif {"factor", "card", "event"}.issubset(source_set):
            reason = "체력 위에 이벤트 재료가 얹힌 케이스라 재료 소화 속도를 볼 만합니다."
        elif {"mart", "analyst"}.issubset(source_set):
            reason = "애널 확신과 종합 랭킹이 겹치는 이름이라 해석 근거를 만들기 쉬운 편입니다."
        elif {"card", "intraday"}.issubset(source_set):
            reason = "종합 점수보다 장중 타이밍 신호를 우선 확인하는 편이 낫습니다."
        else:
            reason = note
        hints.append(f"[{action}] {row['name']}({row['symbol']}) | {reason} | {note}")
        structured.append(
            {
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "action": action,
                "reason": reason,
                "note": note,
                "source_hits": list(sources),
            }
        )
    if not hints:
        hints.append("오늘은 강한 교집합보다 단일 소스 후보가 많아, 비중을 줄이고 분할 진입 관점이 더 적절합니다.")
        structured.append(
            {
                "symbol": "",
                "name": "",
                "action": "관찰 후보",
                "reason": "강한 교집합보다 단일 소스 후보가 많습니다.",
                "note": "비중을 줄이고 분할 진입 관점이 더 적절합니다.",
                "source_hits": [],
            }
        )
    return hints[:4], structured[:4]


def _candidate_action(
    row: dict[str, Any],
    confidence_score: int = 100,
    data_quality_label: str = "양호",
    wics_history_confidence_label: str = "",
) -> tuple[str, str]:
    source_set = set(row.get("source_hits", []) or [])
    source_count = _safe_int(row.get("source_count"))
    score_total = _safe_float(row.get("score_total"))
    if source_count >= 3 and {"factor", "card"}.issubset(source_set) and score_total >= 3.0:
        action = "직접 후보"
        note = "기본 체력과 교집합이 충분해 우선순위를 높게 둘 만합니다"
    elif {"card", "intraday"}.issubset(source_set) or {"news", "event"}.issubset(source_set):
        action = "눌림 후보"
        note = "재료는 분명하지만 타이밍 확인 후 접근이 낫습니다"
    elif source_count >= 2 and score_total >= 2.5:
        action = "관찰 후보"
        note = "신호는 있으나 바로 추격하기보다 확인이 먼저입니다"
    else:
        action = "보류"
        note = "단일 소스 성격이 강해 단독 매수 판단은 보수적으로 보는 편이 낫습니다"

    if confidence_score < 60:
        if action == "직접 후보":
            action = "관찰 후보"
            note = "교집합은 있지만 오늘 전체 확신도가 낮아 우선 관찰로 두는 편이 낫습니다"
        elif action == "눌림 후보":
            action = "관찰 후보"
            note = "재료는 있으나 오늘 전체 확신도가 낮아 확인 중심 접근이 적절합니다"
    if str(data_quality_label or "") == "보수적" and action == "직접 후보":
        action = "관찰 후보"
        note = "점수는 높지만 데이터 품질이 보수적이라 직접 매수보다 확인이 먼저입니다"
    alignment_score = int(row.get("alignment_score", 0))
    if alignment_score <= -2:
        if action == "직접 후보":
            action = "관찰 후보"
            note = "개별 점수는 높지만 매크로/WICS 맥락과 어긋나 관찰이 먼저입니다"
        elif action in {"눌림 후보", "관찰 후보"}:
            action = "보류"
            note = "개별 신호는 있으나 상위 맥락과 역행해 보수적으로 보는 편이 낫습니다"
    elif alignment_score >= 1 and str(wics_history_confidence_label or "") == "예비":
        if action == "직접 후보":
            action = "눌림 후보"
            note = "교집합은 좋지만 WICS 히스토리 표본이 얕아 눌림 확인이 먼저입니다"
    elif alignment_score >= 1 and str(wics_history_confidence_label or "") == "없음":
        if action == "직접 후보":
            action = "관찰 후보"
            note = "교집합은 좋지만 WICS 히스토리 표본이 거의 없어 우선 관찰로 두는 편이 낫습니다"
        elif action == "눌림 후보":
            action = "관찰 후보"
            note = "재료는 있으나 WICS 히스토리 표본이 거의 없어 관찰이 먼저입니다"
    elif alignment_score >= 2 and action == "관찰 후보" and confidence_score >= 65 and str(data_quality_label or "") != "보수적":
        note = "개별 점수에 더해 매크로/WICS 맥락도 우호적이라 우선순위를 높게 볼 만합니다"

    return action, note


def _candidate_bucket(row: dict[str, Any]) -> str:
    source_set = set(row.get("source_hits", []) or [])
    source_count = _safe_int(row.get("source_count"))
    if source_count >= 3 and {"factor", "card"}.issubset(source_set) and _safe_float(row.get("score_total")) >= 3.0:
        return "direct"
    if "intraday" in source_set or "event" in source_set or "news" in source_set or source_count >= 2:
        return "watch"
    return "hold"


def _candidate_bucket_from_action(action: str) -> str:
    if action == "직접 후보":
        return "direct"
    if action in {"눌림 후보", "관찰 후보"}:
        return "watch"
    return "hold"


def _candidate_name(row: dict[str, Any]) -> str:
    return f"{row.get('name')}({row.get('symbol')})"


def _build_trade_plan(
    candidates: list[dict[str, Any]],
    event_watchlist: list[dict[str, Any]],
    confidence_score: int,
    data_quality_label: str,
    wics_history_confidence_label: str = "",
) -> list[str]:
    direct: list[str] = []
    watch: list[str] = []
    hold: list[str] = []
    for row in candidates:
        action, _ = _candidate_action(row, confidence_score, data_quality_label, wics_history_confidence_label)
        bucket = _candidate_bucket_from_action(action)
        label = _candidate_name(row)
        if bucket == "direct" and label not in direct:
            direct.append(label)
        elif bucket == "watch" and label not in watch:
            watch.append(label)
        elif bucket == "hold" and label not in hold:
            hold.append(label)

    direct = direct[:2]
    watch = watch[:2]
    hold = hold[:2]

    if len(hold) < 2:
        for row in event_watchlist[:2]:
            label = f"{row.get('name')}({row.get('symbol')})"
            if label not in hold:
                hold.append(label)
            if len(hold) >= 2:
                break

    lines: list[str] = []
    if direct:
        direct_label = "1순위 직접 후보"
        lines.append(f"- {direct_label}: `" + ", ".join(direct) + "`")
    if watch:
        watch_label = "2순위 눌림 후보" if direct else "1순위 관찰 후보"
        lines.append(f"- {watch_label}: `" + ", ".join(watch) + "`")
    if hold:
        lines.append("- 보류/재료 관찰: `" + ", ".join(hold) + "`")
    if not lines:
        lines.append("- 아직은 강한 교집합보다 단일 재료가 많아, 우선 관찰 위주가 적절합니다.")
    return lines


def _build_event_watchlist(
    event_symbol_summary: dict[str, Any],
    candidates: list[dict[str, Any]],
    top_n: int = 3,
) -> list[dict[str, Any]]:
    candidate_symbols = {_norm_symbol(row.get("symbol")) for row in candidates}
    rows: list[dict[str, Any]] = []
    for row in event_symbol_summary.get("top_symbols", []):
        symbol = _norm_symbol(row.get("symbol"))
        if not symbol or symbol in candidate_symbols:
            continue
        rows.append(
            {
                "symbol": symbol,
                "name": _clean_text(row.get("name")) or symbol,
                "latest_event_type": _clean_text(row.get("latest_event_type")),
                "latest_signal_bias": _clean_text(row.get("latest_signal_bias")),
                "latest_title": _clean_text(row.get("latest_title")),
                "event_signal_score": round(_safe_float(row.get("event_signal_score")), 4),
            }
        )
        if len(rows) >= top_n:
            break
    return rows


def _build_data_quality_snapshot(
    freshness: dict[str, dict[str, Any]],
    risk_flags: list[str],
    inputs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    stale = [name for name, meta in freshness.items() if meta.get("status") == "stale"]
    missing = [name for name, meta in freshness.items() if meta.get("status") == "missing"]
    factor_dyn = ((inputs.get("factor") or {}).get("dynamic_factor_weights") or {}).get("status") or "unknown"
    if len(risk_flags) >= 4 or len(stale) >= 3 or len(missing) >= 2:
        label = "보수적"
    elif risk_flags or stale or missing:
        label = "중간"
    else:
        label = "양호"
    return {
        "label": label,
        "stale_inputs": stale,
        "missing_inputs": missing,
        "factor_dynamic_status": factor_dyn,
        "warnings": risk_flags[:5],
    }


def _build_ai_context(summary: dict[str, Any]) -> str:
    lines = []
    confidence = summary.get("confidence") or {}
    positioning = summary.get("positioning") or {}
    data_quality = summary.get("data_quality") or {}
    lines.append(f"- 확신도: {confidence.get('label')} {confidence.get('score')}/100")
    lines.append(f"- 오늘의 모드: {positioning.get('mode')} | {positioning.get('guidance')}")
    lines.append(f"- 데이터 품질: {data_quality.get('label')} | 동적팩터 {data_quality.get('factor_dynamic_status')}")
    for line in summary.get("trade_plan", [])[:3]:
        lines.append(line)
    lines.append("- 교집합 후보:")
    for row in summary.get("candidates", [])[:5]:
        action, note = _candidate_action(
            row,
            _safe_int(confidence.get("score")),
            str(data_quality.get("label") or ""),
        )
        lines.append(
            f"  {row.get('name')}({row.get('symbol')}) | {row.get('sector')} | 판단 {action} | "
            f"source {row.get('source_count')} | total {row.get('score_total')} | {note}"
        )
    if summary.get("risk_flags"):
        lines.append("- 주의:")
        for flag in summary.get("risk_flags", [])[:4]:
            lines.append(f"  {flag}")
    return "\n".join(lines)


def _generate_ai_commentary(summary: dict[str, Any]) -> str:
    api_key = getattr(SETTINGS, "GEMINI_API_KEY", "")
    if not api_key:
        return ""
    if google_genai is None and google_generativeai is None:
        return ""

    direct_names = ", ".join(
        row.get("name")
        for row in summary.get("candidate_actions", [])
        if row.get("action") == "직접 후보"
    ) or "없음"
    watch_names = ", ".join(
        row.get("name")
        for row in summary.get("candidate_actions", [])
        if row.get("action") in {"눌림 후보", "관찰 후보"}
    ) or "없음"
    hold_names = ", ".join(
        row.get("name")
        for row in summary.get("candidate_actions", [])
        if row.get("action") == "보류"
    ) or "없음"
    direct_names = ", ".join([name.strip() for name in direct_names.split(",") if name.strip()][:2]) or "없음"
    watch_names = ", ".join([name.strip() for name in watch_names.split(",") if name.strip()][:2]) or "없음"
    hold_names = ", ".join([name.strip() for name in hold_names.split(",") if name.strip()][:2]) or "없음"
    warnings_text = " / ".join((summary.get("data_quality") or {}).get("warnings", [])[:3]) or "데이터 경고는 제한적입니다."

    prompt = f"""
    [임무]
    당신은 한국 주식 데일리 브리핑을 쓰는 포트폴리오 매니저입니다.
    아래 입력을 바탕으로 후보를 바꾸지 말고, 해설 문장만 아주 짧게 작성하세요.

    [규칙]
    1. 위 후보 이름을 절대 바꾸지 마세요.
    2. `보유`, `비보유`, `포지션` 같은 표현은 금지합니다.
    3. 아래 두 줄만 작성하세요.
    4. 제공된 데이터 밖의 이유를 지어내지 마세요.

    [고정 후보]
    직접 후보: {direct_names}
    눌림 후보: {watch_names}
    보류: {hold_names}

    [출력 형식]
    해설: ...
    주의: ...

    [입력]
    {_build_ai_context(summary)}
    - 현재 데이터 경고: {warnings_text}
    """

    try:
        if google_genai is not None:
            client = google_genai.Client(api_key=api_key)
            response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
            text = getattr(response, "text", "") or ""
        else:
            model = google_generativeai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt)
            text = getattr(response, "text", "") or ""

        explanation = ""
        caution = ""
        for raw in text.splitlines():
            line = raw.strip().lstrip("-").strip()
            if not line:
                continue
            lower = line.lower()
            if lower.startswith("해설:"):
                explanation = line.split(":", 1)[1].strip()
            elif lower.startswith("주의:"):
                caution = line.split(":", 1)[1].strip()
        if not explanation:
            explanation = "오늘은 교집합 후보를 보더라도 데이터 공백과 신선도를 함께 보며 해석하는 편이 적절합니다."
        if not caution:
            caution = warnings_text

        return "\n".join(
            [
                "*AI 최종 코멘트*",
                f"- 직접 후보: {direct_names}",
                f"- 눌림 후보: {watch_names}",
                f"- 보류: {hold_names}",
                f"- 한 줄 해설: {explanation}",
                f"- 데이터 주의: {caution}",
            ]
        )
    except Exception as exc:
        log.warning("market briefing AI commentary failed: %s", exc)
        return ""


def build_market_briefing(top_n: int = 8) -> tuple[str, dict[str, Any]]:
    inputs, freshness = _load_inputs()

    factor_summary = inputs.get("factor") or {}
    card_summary = inputs.get("card") or {}
    mart_summary = inputs.get("mart") or {}
    valuation_summary = inputs.get("valuation") or {}
    news_summary = inputs.get("news") or {}
    event_summary = inputs.get("event") or {}
    event_symbol_summary = inputs.get("event_symbols") or {}
    macro_summary = inputs.get("macro") or {}
    wics_summary = inputs.get("wics") or {}
    analyst_summary = inputs.get("analyst") or {}

    raw_sector_view = _build_sector_view(factor_summary, card_summary)
    raw_candidates = _build_candidate_view(
        factor_summary=factor_summary,
        card_summary=card_summary,
        mart_summary=mart_summary,
        analyst_summary=analyst_summary,
        news_summary=news_summary,
        event_symbol_summary=event_symbol_summary,
        top_n=top_n,
    )
    context_alignment = _build_context_alignment(macro_summary, wics_summary)
    sector_view = _decorate_sector_view_with_alignment(raw_sector_view, context_alignment)
    candidates = _decorate_candidates_with_alignment(raw_candidates, context_alignment)
    risk_flags = _build_risk_flags(inputs, freshness, sector_view, candidates, context_alignment)
    confidence = _build_confidence(inputs, freshness, candidates, sector_view, risk_flags, context_alignment)
    positioning = _build_positioning_mode(confidence, candidates, freshness)
    event_watchlist = _build_event_watchlist(event_symbol_summary, candidates, top_n=3)
    data_quality = _build_data_quality_snapshot(freshness, risk_flags, inputs)
    trade_plan = _build_trade_plan(
        candidates,
        event_watchlist,
        _safe_int(confidence.get("score")),
        str(data_quality.get("label") or ""),
        _clean_text(context_alignment.get("wics_history_confidence_label")),
    )
    action_hints, action_hint_rows = _build_action_hints(
        candidates,
        _safe_int(confidence.get("score")),
        str(data_quality.get("label") or ""),
        _clean_text(context_alignment.get("wics_history_confidence_label")),
    )
    hybrid_inputs = load_hybrid_inputs(
        {
            "market_briefing": {
                "confidence": confidence,
                "positioning": positioning,
                "data_quality": data_quality,
                "context_alignment": context_alignment,
                "freshness": freshness,
            }
        }
    )
    sector_rotation = compute_sector_rotation(hybrid_inputs)
    relative_value = compute_relative_value_candidates(hybrid_inputs, sector_rotation=sector_rotation)
    sector_thesis = build_sector_thesis(sector_rotation=sector_rotation, relative_value=relative_value)
    save_sector_thesis(sector_thesis)
    wics_sector_meta = load_effective_wics_sector_meta()
    sector_view = _decorate_sector_view_with_thesis(sector_view, sector_thesis)
    sector_view = _decorate_with_wics_sector_meta(sector_view, wics_sector_meta)
    candidates = _decorate_candidates_with_sector_thesis(candidates, sector_thesis)
    candidates = _decorate_with_wics_sector_meta(candidates, wics_sector_meta)
    candidates = _append_wics_stability_notes(candidates)

    macro_excerpt = _report_excerpt(macro_summary, max_lines=4)
    wics_structured_lines = _build_wics_structured_lines(wics_summary)
    valuation_structured_lines = _build_valuation_structured_lines(valuation_summary)
    wics_excerpt = _report_excerpt(wics_summary, max_lines=4)
    sector_thesis_lines = _build_sector_thesis_lines(sector_thesis)

    lines = ["*[통합 시장 브리핑]*"]
    lines.append(f"- 생성시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("- 해석 순서: 매크로/섹터 → 팩터/종합점수 → 이벤트/애널/뉴스 → 장중 타이밍")
    lines.append(f"- 오늘의 확신도: {confidence['label']} ({confidence['score']}/100) | {confidence['reason'] or '주요 입력 기준 계산'}")
    lines.append(f"- 오늘의 모드: {positioning['mode']} | {positioning['guidance']}")
    lines.append(f"- 데이터 품질: {data_quality['label']} | stale {len(data_quality['stale_inputs'])} | missing {len(data_quality['missing_inputs'])} | 동적팩터 {data_quality['factor_dynamic_status']}")

    lines.append("*0. 오늘 뭐 할까*")
    lines.extend(trade_plan)

    lines.append("*1. 입력 신선도*")
    lines.extend(_build_freshness_lines(freshness))

    lines.append("*2. 시장 환경*")
    if macro_excerpt:
        lines.append("- 매크로:")
        for line in macro_excerpt:
            lines.append(f"  {line}")
    else:
        lines.append("- 매크로: 최신 해설 파일이 없어 상위 헤드라인 해석은 비어 있습니다.")
    if wics_structured_lines:
        lines.append("- 순환매/WICS:")
        for line in wics_structured_lines[:4]:
            lines.append(f"  {line}")
    elif wics_excerpt:
        lines.append("- 순환매/WICS:")
        for line in wics_excerpt:
            lines.append(f"  {line}")
    else:
        if _clean_text(wics_summary.get("source")) == "wics_raw_log":
            lines.append("- 순환매/WICS: 원시 로그는 있지만 요약 가능한 상단/경계 섹터가 아직 비어 있습니다.")
        else:
            lines.append("- 순환매/WICS: 최신 AI 리포트가 아직 생성되지 않았습니다.")
    if valuation_structured_lines:
        lines.append("- 적정가:")
        for line in valuation_structured_lines[:4]:
            lines.append(f"  {line}")

    if sector_view:
        lines.append("- 팩터/종목 점검 기준 상위 섹터:")
        for row in sector_view[:4]:
            lines.append(
                f"  • {row['sector']} | factor {round(_safe_float(row['factor_score']), 3)} | "
                f"점검 {round(_safe_float(row['card_score']), 3)} | ml {round(_safe_float(row['ml_score']), 3)} | "
                f"count {row['count']} | 맥락 {row.get('alignment_label', '중립')}"
            )
    top_support = context_alignment.get("top_support", []) if isinstance(context_alignment, dict) else []
    top_risk = context_alignment.get("top_risk", []) if isinstance(context_alignment, dict) else []
    if top_support:
        lines.append(
            "- 맥락 정렬 섹터: "
            + " / ".join(
                f"{row.get('sector')}({row.get('label')})"
                for row in top_support[:3]
                if row.get("sector")
            )
        )
    if top_risk:
        lines.append(
            "- 맥락 경계 섹터: "
            + " / ".join(
                f"{row.get('sector')}({row.get('label')})"
                for row in top_risk[:2]
                if row.get("sector")
            )
        )
    if context_alignment.get("wics_history_confidence_label"):
        lines.append(
            f"- WICS 표본: {context_alignment.get('wics_history_confidence_label')} "
            f"({int(context_alignment.get('wics_history_day_count', 0) or 0)}일) | "
            f"유니버스 {context_alignment.get('wics_universe_regime') or '-'}"
        )
    if context_alignment.get("wics_confidence_note"):
        lines.append(f"- 맥락 주의: {context_alignment.get('wics_confidence_note')}")
    if sector_thesis_lines:
        lines.append("- 공통 결론:")
        lines.extend(sector_thesis_lines[:8])

    lines.append("*3. 교집합 후보*")
    if candidates:
        for row in candidates:
            action, decision_note = _candidate_action(
                row,
                _safe_int(confidence.get("score")),
                str(data_quality.get("label") or ""),
                _clean_text(context_alignment.get("wics_history_confidence_label")),
            )
            source_labels = {"factor": "팩터", "card": "종목점검", "mart": "장마감기준", "analyst": "애널", "news": "뉴스", "event": "이벤트", "intraday": "장중수급"}
            lines.append(
                f"- {row['name']}({row['symbol']}) | {row['sector']} | "
                f"판단 {action} | source {row['source_count']}개 [{', '.join(source_labels.get(hit, hit) for hit in row['source_hits'])}] | "
                f"맥락 {row.get('alignment_label', '중립')} | 섹터결론 {row.get('sector_final_label') or '-'} | total {row['score_total']}"
            )
            lines.append(f"  {decision_note}")
            if row.get("notes"):
                lines.append("  " + " / ".join(row["notes"][:3]))
    else:
        lines.append("- 최신 산출물 기준으로 겹치는 후보가 아직 충분하지 않습니다.")

    lines.append("*4. 별도 재료 후보*")
    if event_watchlist:
        for row in event_watchlist:
            lines.append(
                f"- {row['name']}({row['symbol']}) | {row['latest_event_type']} | "
                f"bias {row['latest_signal_bias'] or '-'} | signal {row['event_signal_score']}"
            )
            if row["latest_title"]:
                lines.append(f"  {row['latest_title']}")
    else:
        lines.append("- 교집합 밖에서 따로 눈에 띄는 이벤트 후보는 제한적입니다.")

    lines.append("*5. 오늘의 액션 힌트*")
    for hint in action_hints:
        lines.append(f"- {hint}")

    lines.append("*6. 주의할 점*")
    if risk_flags:
        for flag in risk_flags:
            lines.append(f"- {flag}")
    else:
        lines.append("- 현재 최신 산출물 기준으로 큰 결측 경고는 없습니다.")

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "inputs_present": {name: bool(payload) for name, payload in inputs.items()},
        "freshness": freshness,
        "sector_view": sector_view,
        "candidates": candidates,
        "risk_flags": risk_flags,
        "data_quality": data_quality,
        "context_alignment": {
            "top_support": top_support[:4],
            "top_risk": top_risk[:3],
            "market_mode": context_alignment.get("market_mode") or "중립",
            "confidence_score": _safe_int(context_alignment.get("confidence_score")),
            "wics_history_confidence_label": context_alignment.get("wics_history_confidence_label") or "없음",
            "wics_history_day_count": _safe_int(context_alignment.get("wics_history_day_count")),
            "wics_universe_regime": context_alignment.get("wics_universe_regime") or "-",
            "wics_confidence_note": context_alignment.get("wics_confidence_note") or "",
        },
        "valuation_snapshot": {
            "generated_at": valuation_summary.get("generated_at") or "",
            "coverage_count": _safe_int(valuation_summary.get("coverage_count")),
            "row_count": _safe_int(valuation_summary.get("row_count")),
            "top_discount": (valuation_summary.get("top_discount") or [])[:5],
        },
        "confidence": confidence,
        "positioning": positioning,
        "trade_plan": trade_plan,
        "action_hints": action_hints,
        "action_hint_rows": action_hint_rows,
        "event_watchlist": event_watchlist,
        "macro_excerpt": macro_excerpt,
        "wics_structured_lines": wics_structured_lines,
        "wics_excerpt": wics_excerpt,
        "sector_thesis": sector_thesis,
        "event_metadata": event_summary.get("metadata") or {},
        "candidate_actions": [
            {
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "action": _candidate_action(
                    row,
                    _safe_int(confidence.get("score")),
                    str(data_quality.get("label") or ""),
                    _clean_text(context_alignment.get("wics_history_confidence_label")),
                )[0],
                "note": _candidate_action(
                    row,
                    _safe_int(confidence.get("score")),
                    str(data_quality.get("label") or ""),
                    _clean_text(context_alignment.get("wics_history_confidence_label")),
                )[1],
                "sector_final_label": row.get("sector_final_label") or "",
                "sector_action_hint": row.get("sector_action_hint") or "",
                "source_hits": list(row.get("source_hits") or []),
                "score_total": row.get("score_total"),
            }
            for row in candidates[: min(8, len(candidates))]
        ],
    }
    return "\n".join(lines), summary


def _save_briefing(digest: str, summary: dict[str, Any]) -> dict[str, str]:
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = os.path.join(RUNTIME_DIR, f"market_briefing_{stamp}.txt")
    json_path = os.path.join(RUNTIME_DIR, f"market_briefing_{stamp}.json")
    latest_txt = os.path.join(RUNTIME_DIR, "market_briefing_latest.txt")
    latest_json = os.path.join(RUNTIME_DIR, "market_briefing_latest.json")
    with open(txt_path, "w", encoding="utf-8") as fp:
        fp.write(digest)
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    with open(latest_txt, "w", encoding="utf-8") as fp:
        fp.write(digest)
    with open(latest_json, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    return {
        "txt_path": txt_path,
        "json_path": json_path,
        "latest_txt": latest_txt,
        "latest_json": latest_json,
    }


def build_and_send_report(args: argparse.Namespace) -> None:
    digest, summary = build_market_briefing(top_n=args.top_n)
    ai_commentary = _generate_ai_commentary(summary) if args.with_ai else ""
    if ai_commentary:
        digest = "\n".join([digest, "", ai_commentary.strip()])
        summary["ai_commentary"] = ai_commentary.strip()
    paths = _save_briefing(digest, summary)
    log.info("market briefing saved: %s", paths["latest_json"])
    title = f"[통합 브리핑] 오늘의 해석 순서 {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
    if args.print_only:
        print(title)
        print(digest)
        return
    send_slack(digest, title=title, msg_type="info")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Market briefing scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            try:
                build_and_send_report(args)
            except Exception as exc:
                log.exception("market briefing scheduled job failed")
                if not args.print_only:
                    notify_error("Market Briefing Reporter", str(exc))
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        build_and_send_report(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
