from __future__ import annotations

import json
import math
import os
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional

import pandas as pd

try:
    from config import SETTINGS
except Exception:
    from Disclosure.config import SETTINGS

try:
    from context_alignment import alignment_label, canonical_sector_name
except Exception:
    from Disclosure.context_alignment import alignment_label, canonical_sector_name

try:
    from sector_thesis import build_sector_thesis, merge_sector_thesis_into_rotation, save_sector_thesis
except Exception:
    from Disclosure.sector_thesis import build_sector_thesis, merge_sector_thesis_into_rotation, save_sector_thesis

try:
    from signals.wics_universe import load_effective_wics_sector_meta
except Exception:
    from Disclosure.signals.wics_universe import load_effective_wics_sector_meta


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
MART_CSV_PATH = os.path.join(ROOT_DIR, "marts", "daily_signal_mart_latest.csv")
FACTOR_CSV_PATH = os.path.join(ROOT_DIR, "factors", "snapshots", "factor_snapshot_latest.csv")
CARD_CSV_PATH = os.path.join(ROOT_DIR, "cards", "stock_cards_latest.csv")
MARKET_BRIEFING_PATH = os.path.join(ROOT_DIR, "runtime", "market_briefing_latest.json")
WICS_REPORT_PATH = os.path.join(ROOT_DIR, "signals", "reports", "wics_ai_report_latest.json")
API_INTEGRATION_REPORT_PATH = os.path.join(ROOT_DIR, "runtime", "api_integrations_latest.json")

SECTOR_ROTATION_PATH = os.path.join(ROOT_DIR, "runtime", "sector_rotation_latest.json")
RELATIVE_VALUE_PATH = os.path.join(ROOT_DIR, "runtime", "relative_value_candidates_latest.json")
HYBRID_SHADOW_BOOK_PATH = os.path.join(ROOT_DIR, "runtime", "hybrid_shadow_book_latest.json")
TRADE_DECISION_LEDGER_PATH = os.path.join(ROOT_DIR, "runtime", "trade_decision_ledger_latest.jsonl")
HYBRID_STUDY_PACK_PATH = os.path.join(ROOT_DIR, "runtime", "hybrid_study_pack_latest.json")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


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
    parsed = _safe_float(value)
    return int(parsed) if parsed is not None else 0


def _clip(value: Optional[float], low: float = 0.0, high: float = 1.0) -> float:
    if value is None:
        return low
    return max(low, min(high, float(value)))


def _scale(value: Optional[float], low: float, high: float) -> float:
    if value is None or high <= low:
        return 0.0
    ratio = (float(value) - low) / (high - low)
    return _clip(ratio, 0.0, 1.0)


def _wics_universe_penalty(status_label: str, dynamic_count: int, stability: Optional[float]) -> float:
    penalty = 0.0
    if status_label == "재점검":
        penalty += 6.0
    elif status_label == "유동형":
        penalty += 2.5
    if dynamic_count > 0 and stability is not None and stability > 0 and stability < 0.6:
        penalty += (0.6 - float(stability)) * 10.0
    return round(max(0.0, penalty), 2)


def _wics_universe_note(status_label: str, reason: str, dynamic_count: int, stability: Optional[float]) -> str:
    reason_text = str(reason or "").strip()
    if status_label == "재점검":
        return reason_text or "WICS 바스켓 재점검 상태라 섹터 점수는 보수적으로 읽습니다."
    if dynamic_count > 0 and stability is not None and stability > 0 and stability < 0.6:
        return f"동적 편입은 있었지만 안정도 {int(round(float(stability) * 100))}/100 수준이라 과신을 줄입니다."
    if status_label == "유동형":
        return reason_text or "WICS 바스켓 변동이 있어 섹터 확신을 한 단계 낮춰 읽습니다."
    return reason_text


def _norm_symbol(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def _read_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _write_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def _append_jsonl(path: str, rows: Iterable[dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _normalize_sector_series(series: pd.Series) -> pd.Series:
    return series.apply(canonical_sector_name)


def _freshness_component(briefing: dict[str, Any]) -> tuple[float, list[str]]:
    freshness = briefing.get("freshness") or {}
    if not isinstance(freshness, dict) or not freshness:
        return 0.5, ["freshness missing"]

    total = 0
    score = 0.0
    notes: list[str] = []
    for key, meta in freshness.items():
        if not isinstance(meta, dict):
            continue
        total += 1
        status = str(meta.get("status") or "")
        if status == "fresh":
            score += 1.0
        elif status == "stale":
            score += 0.4
            notes.append(f"{key} stale")
        else:
            score += 0.0
            notes.append(f"{key} missing")
    if total <= 0:
        return 0.5, ["freshness empty"]
    return _clip(score / total), notes[:4]


def _context_alignment_map(briefing: dict[str, Any]) -> dict[str, int]:
    payload = briefing.get("context_alignment") or {}
    out: dict[str, int] = {}
    for row in payload.get("top_support", []) or []:
        sector = canonical_sector_name(row.get("sector"))
        if sector:
            out[sector] = max(out.get(sector, 0), _safe_int(row.get("score")))
    for row in payload.get("top_risk", []) or []:
        sector = canonical_sector_name(row.get("sector"))
        if sector:
            out[sector] = min(out.get(sector, 0), -abs(_safe_int(row.get("score"))))
    return out


def _wics_maps(wics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    top_map: dict[str, dict[str, Any]] = {}
    risk_map: dict[str, dict[str, Any]] = {}
    for row in wics.get("top_rotation_sectors", []) or []:
        sector = canonical_sector_name(row.get("sector_short") or row.get("sector_name"))
        if sector:
            top_map[sector] = dict(row)
    for row in wics.get("risk_sectors", []) or []:
        sector = canonical_sector_name(row.get("sector_short") or row.get("sector_name"))
        if sector:
            risk_map[sector] = dict(row)
    return {"top": top_map, "risk": risk_map}


def _merge_base_frame(
    mart_df: pd.DataFrame,
    factor_df: pd.DataFrame,
    card_df: pd.DataFrame,
) -> pd.DataFrame:
    base = mart_df.copy() if not mart_df.empty else card_df.copy()
    if base.empty:
        return pd.DataFrame()

    if "symbol" not in base.columns:
        return pd.DataFrame()

    base["symbol"] = base["symbol"].map(_norm_symbol)
    if "sector" in base.columns:
        base["sector"] = _normalize_sector_series(base["sector"])
    else:
        base["sector"] = ""

    if not factor_df.empty and "symbol" in factor_df.columns:
        factor = factor_df.copy()
        factor["symbol"] = factor["symbol"].map(_norm_symbol)
        if "sector" in factor.columns:
            factor["sector"] = _normalize_sector_series(factor["sector"])
        factor_cols = [
            "symbol",
            "sector",
            "value_score",
            "momentum_score",
            "quality_score",
            "flow_score",
            "news_score",
            "composite_score",
            "liquidity_score",
            "avg_turnover_20d",
            "sector_leader_rank",
            "sector_reversion_signal",
            "factor_source_coverage_ratio",
        ]
        factor = factor[[col for col in factor_cols if col in factor.columns]].drop_duplicates("symbol")
        base = base.merge(factor, on="symbol", how="left", suffixes=("", "_factor"))
        if "sector_factor" in base.columns:
            base["sector"] = base["sector"].where(base["sector"].astype(str).str.len() > 0, base["sector_factor"])

    if not card_df.empty and "symbol" in card_df.columns:
        card = card_df.copy()
        card["symbol"] = card["symbol"].map(_norm_symbol)
        if "sector" in card.columns:
            card["sector"] = _normalize_sector_series(card["sector"])
        card_cols = [
            "symbol",
            "sector",
            "card_score",
            "active_source_count",
            "flow_state_score",
            "flow_intraday_edge_score",
            "ml_pred_return_5d",
            "event_expected_alpha_5d",
            "analyst_target_upside_pct",
            "analyst_peer_alpha_5d",
            "analyst_peer_support_count",
            "macro_sector_score",
        ]
        card = card[[col for col in card_cols if col in card.columns]].drop_duplicates("symbol")
        base = base.merge(card, on="symbol", how="left", suffixes=("", "_card"))
        if "sector_card" in base.columns:
            base["sector"] = base["sector"].where(base["sector"].astype(str).str.len() > 0, base["sector_card"])

    base["sector"] = _normalize_sector_series(base["sector"])
    base = base[base["symbol"].astype(str).str.len() > 0].copy()
    base = base[base["sector"].astype(str).str.len() > 0].copy()
    return base


def load_hybrid_inputs(selector_inputs: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    selector_inputs = selector_inputs or {}
    return {
        "mart_df": _read_csv(MART_CSV_PATH),
        "factor_df": selector_inputs.get("factor_df") if isinstance(selector_inputs.get("factor_df"), pd.DataFrame) else _read_csv(FACTOR_CSV_PATH),
        "card_df": selector_inputs.get("card_df") if isinstance(selector_inputs.get("card_df"), pd.DataFrame) else _read_csv(CARD_CSV_PATH),
        "market_briefing": selector_inputs.get("market_briefing") if isinstance(selector_inputs.get("market_briefing"), dict) else _read_json(MARKET_BRIEFING_PATH),
        "wics_report": _read_json(WICS_REPORT_PATH),
        "api_integration": _read_json(API_INTEGRATION_REPORT_PATH),
    }


def compute_sector_rotation(inputs: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    inputs = inputs or load_hybrid_inputs()
    mart_df = inputs.get("mart_df")
    factor_df = inputs.get("factor_df")
    card_df = inputs.get("card_df")
    briefing = inputs.get("market_briefing") or {}
    wics = inputs.get("wics_report") or {}

    base = _merge_base_frame(mart_df if isinstance(mart_df, pd.DataFrame) else pd.DataFrame(), factor_df if isinstance(factor_df, pd.DataFrame) else pd.DataFrame(), card_df if isinstance(card_df, pd.DataFrame) else pd.DataFrame())
    market_mode = str((briefing.get("context_alignment") or {}).get("market_mode") or (briefing.get("positioning") or {}).get("mode") or "")
    confidence_score = _safe_int((briefing.get("confidence") or {}).get("score"))
    data_quality = briefing.get("data_quality") or {}
    data_quality_label = str(data_quality.get("label") or "")
    data_quality_flags = list(data_quality.get("warnings") or [])
    active_threshold = int(getattr(SETTINGS, "HYBRID_ACTIVE_SECTOR_MIN_SCORE", 60) or 60)
    if "보수" in market_mode:
        active_threshold = max(active_threshold, 70)

    wics_history_days = _safe_int((briefing.get("context_alignment") or {}).get("wics_history_day_count"))
    wics_hard_days = max(1, int(getattr(SETTINGS, "HYBRID_WICS_HARD_MIN_DAYS", 10) or 10))
    wics_soft_scale = 1.0 if wics_history_days >= wics_hard_days else _clip(wics_history_days / float(wics_hard_days), 0.15, 1.0)

    freshness_score, freshness_notes = _freshness_component(briefing)
    context_map = _context_alignment_map(briefing)
    wics_map = _wics_maps(wics)
    wics_top = wics_map.get("top", {})
    wics_risk = wics_map.get("risk", {})
    max_wics_top = max([_safe_float((row or {}).get("score")) or 0.0 for row in wics_top.values()] or [1.0])
    max_wics_risk = max([abs(_safe_float((row or {}).get("score")) or 0.0) for row in wics_risk.values()] or [1.0])

    rows: list[dict[str, Any]] = []
    if base.empty:
        return {
            "generated_at": _now_iso(),
            "market_mode": market_mode or "-",
            "confidence_score": confidence_score,
            "data_quality_flags": data_quality_flags,
            "data_quality_label": data_quality_label or "-",
            "wics_history_day_count": wics_history_days,
            "wics_soft_scale": round(wics_soft_scale, 4),
            "active_sector_min_score": active_threshold,
            "sectors": rows,
            "active_sectors": [],
        }

    for sector, sector_df in base.groupby("sector"):
        sector_df = sector_df.copy()
        avg_composite = (_safe_float(sector_df.get("composite_score").mean()) or 0.0) if "composite_score" in sector_df else 0.0
        avg_card = (_safe_float(sector_df.get("card_score").mean()) or avg_composite) if "card_score" in sector_df else avg_composite
        factor_card_component = _clip((avg_composite + avg_card) / 2.0) * 100.0

        wics_component_raw = 0.5
        top_entry = wics_top.get(sector) or {}
        risk_entry = wics_risk.get(sector) or {}
        if top_entry:
            wics_component_raw = 0.5 + 0.5 * _scale(_safe_float(top_entry.get("score")), 0.0, max_wics_top)
        elif risk_entry:
            wics_component_raw = 0.5 - 0.45 * _scale(abs(_safe_float(risk_entry.get("score")) or 0.0), 0.0, max_wics_risk)
        wics_component = (0.5 + (wics_component_raw - 0.5) * wics_soft_scale) * 100.0

        avg_macro = (_safe_float(sector_df.get("macro_sector_score").mean()) or 0.0) if "macro_sector_score" in sector_df else 0.0
        macro_component = _scale(avg_macro, 0.0, 10.0) * 100.0
        align_score = int(context_map.get(sector, 0) or 0)
        align_component = _clip((align_score + 2.0) / 4.0) * 100.0
        macro_context_component = (macro_component * 0.55) + (align_component * 0.45)

        composite_series = sector_df.get("composite_score")
        composite_median = float(composite_series.median()) if composite_series is not None and not composite_series.empty else 0.0
        breadth_mask = []
        for _, row in sector_df.iterrows():
            composite = _safe_float(row.get("composite_score")) or 0.0
            active_sources = _safe_int(row.get("active_source_count"))
            breadth_mask.append(composite >= composite_median or active_sources >= 2)
        breadth_component = (sum(1 for flag in breadth_mask if flag) / max(1, len(breadth_mask))) * 100.0

        leader_rank_series = sector_df.get("sector_leader_rank")
        top_leaders = (
            sorted([_safe_float(value) or 0.0 for value in leader_rank_series.tolist()], reverse=True)[:2]
            if leader_rank_series is not None
            else [0.0]
        )
        leader_top = top_leaders[0] if top_leaders else 0.0
        leader_second = top_leaders[1] if len(top_leaders) > 1 else 0.0
        leader_gap = max(0.0, leader_top - leader_second)
        leader_stability_component = _clip((leader_top * 0.7) + (min(1.0, leader_gap * 2.0) * 0.3)) * 100.0
        breadth_stability_component = (breadth_component * 0.6) + (leader_stability_component * 0.4)

        freshness_confidence_component = (confidence_score * 0.6) + (freshness_score * 100.0 * 0.4)
        if data_quality_label == "보수적":
            freshness_confidence_component *= 0.78
        elif data_quality_label == "중간":
            freshness_confidence_component *= 0.90

        sector_regime_score = (
            factor_card_component * 0.30
            + wics_component * 0.25
            + macro_context_component * 0.20
            + breadth_stability_component * 0.15
            + freshness_confidence_component * 0.10
        )

        leader_row = sector_df.sort_values(
            by=["card_score", "composite_score", "flow_score"],
            ascending=False,
        ).head(1)
        leader_symbol = ""
        leader_name = ""
        if not leader_row.empty:
            leader_symbol = _norm_symbol(leader_row.iloc[0].get("symbol"))
            leader_name = str(leader_row.iloc[0].get("name") or leader_symbol)

        reasons = [
            f"factor/card {factor_card_component:.1f}",
            f"wics {wics_component:.1f}",
            f"macro/context {macro_context_component:.1f}",
            f"breadth/stability {breadth_stability_component:.1f}",
            f"fresh/conf {freshness_confidence_component:.1f}",
        ]
        if top_entry:
            reasons.append(f"WICS {str(top_entry.get('top_pick') or '-')} 순환")
        elif risk_entry:
            reasons.append(f"WICS 경계 {str(risk_entry.get('top_pick') or '-')}")
        if freshness_notes:
            reasons.extend(freshness_notes[:2])

        rows.append(
            {
                "sector": sector,
                "count": int(len(sector_df)),
                "leader_symbol": leader_symbol,
                "leader_name": leader_name,
                "avg_composite_score": round(avg_composite, 4),
                "avg_card_score": round(avg_card, 4),
                "median_composite_score": round(composite_median, 4),
                "context_alignment_score": int(align_score),
                "context_alignment_label": alignment_label(int(align_score)),
                "wics_rotation_score": round(_safe_float(top_entry.get("score")) or 0.0, 4),
                "wics_risk_score": round(_safe_float(risk_entry.get("score")) or 0.0, 4),
                "factor_card_component": round(factor_card_component, 2),
                "wics_component": round(wics_component, 2),
                "macro_context_component": round(macro_context_component, 2),
                "breadth_stability_component": round(breadth_stability_component, 2),
                "freshness_confidence_component": round(freshness_confidence_component, 2),
                "sector_regime_score": round(sector_regime_score, 2),
                "active": bool(sector_regime_score >= active_threshold),
                "active_threshold": int(active_threshold),
                "reasons": reasons[:6],
                "wics_soft_prior": bool(wics_history_days < wics_hard_days),
                "wics_soft_scale": round(wics_soft_scale, 4),
            }
        )

    rows = sorted(rows, key=lambda item: float(item.get("sector_regime_score", 0.0) or 0.0), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row["rank"] = int(idx)
    active_sectors = [row["sector"] for row in rows if row.get("active")]
    return {
        "generated_at": _now_iso(),
        "market_mode": market_mode or "-",
        "confidence_score": int(confidence_score),
        "data_quality_label": data_quality_label or "-",
        "data_quality_flags": data_quality_flags,
        "wics_history_day_count": int(wics_history_days),
        "wics_hard_min_days": int(wics_hard_days),
        "wics_soft_scale": round(wics_soft_scale, 4),
        "active_sector_min_score": int(active_threshold),
        "active_sectors": active_sectors,
        "top_sectors": rows[:10],
        "sectors": rows,
    }


def compute_relative_value_candidates(
    inputs: Optional[dict[str, Any]] = None,
    sector_rotation: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    inputs = inputs or load_hybrid_inputs()
    sector_rotation = sector_rotation or compute_sector_rotation(inputs)
    mart_df = inputs.get("mart_df")
    factor_df = inputs.get("factor_df")
    card_df = inputs.get("card_df")
    base = _merge_base_frame(mart_df if isinstance(mart_df, pd.DataFrame) else pd.DataFrame(), factor_df if isinstance(factor_df, pd.DataFrame) else pd.DataFrame(), card_df if isinstance(card_df, pd.DataFrame) else pd.DataFrame())
    active_sectors = set(sector_rotation.get("active_sectors") or [])
    min_relative = int(getattr(SETTINGS, "HYBRID_RELATIVE_VALUE_MIN_SCORE", 55) or 55)

    sector_candidate_rows: list[dict[str, Any]] = []
    sector_buckets: list[dict[str, Any]] = []
    if base.empty or not active_sectors:
        return {
            "generated_at": _now_iso(),
            "active_sectors": sorted(active_sectors),
            "min_relative_value_score": min_relative,
            "candidates": sector_candidate_rows,
            "sector_buckets": sector_buckets,
            "top_candidates": [],
        }

    for sector, sector_df in base.groupby("sector"):
        if sector not in active_sectors:
            continue
        sector_df = sector_df.copy()
        composite_median = float(sector_df["composite_score"].median()) if "composite_score" in sector_df.columns else 0.0
        sector_rows: list[dict[str, Any]] = []
        for _, row in sector_df.iterrows():
            composite = _safe_float(row.get("composite_score")) or 0.0
            flow_score = _safe_float(row.get("flow_score")) or 0.0
            event_alpha = _safe_float(row.get("event_expected_alpha_5d")) or 0.0
            news_score = _safe_float(row.get("news_score")) or 0.0
            strong_axis = flow_score >= 0.68 or event_alpha >= 0.015 or news_score >= 0.68
            pool_eligible = bool(composite >= composite_median or strong_axis)
            if not pool_eligible:
                continue

            value_component = (_safe_float(row.get("value_score")) or 0.0) * 100.0
            analyst_upside_component = _scale(_safe_float(row.get("analyst_target_upside_pct")), 0.0, 50.0) * 100.0
            event_component = _scale(event_alpha, 0.0, 0.10) * 100.0
            peer_alpha_component = _scale(_safe_float(row.get("analyst_peer_alpha_5d")), 0.0, 0.05)
            peer_support_component = _scale(_safe_float(row.get("analyst_peer_support_count")), 0.0, 5.0)
            peer_component = (peer_alpha_component * 0.7 + peer_support_component * 0.3) * 100.0
            quality_component = (_safe_float(row.get("quality_score")) or 0.0) * 100.0
            flow_component = flow_score * 100.0
            momentum_score = _safe_float(row.get("momentum_score")) or 0.0
            leader_rank = _safe_float(row.get("sector_leader_rank")) or 0.0
            lag_component = 0.0 if momentum_score < 0.45 else _clip(1.0 - leader_rank) * 100.0

            relative_value_score = (
                value_component * 0.25
                + analyst_upside_component * 0.20
                + event_component * 0.15
                + peer_component * 0.10
                + quality_component * 0.10
                + flow_component * 0.10
                + lag_component * 0.10
            )

            reasons = []
            if value_component >= 65:
                reasons.append(f"value {value_component:.0f}")
            if analyst_upside_component >= 50:
                reasons.append(f"upside {analyst_upside_component:.0f}")
            if event_component >= 40:
                reasons.append(f"event {event_component:.0f}")
            if lag_component >= 40:
                reasons.append(f"lag {lag_component:.0f}")
            if not reasons:
                reasons.append("복합 보강")

            sector_rows.append(
                {
                    "symbol": _norm_symbol(row.get("symbol")),
                    "name": str(row.get("name") or row.get("symbol") or ""),
                    "sector": sector,
                    "pool_eligible": True,
                    "relative_value_score": round(relative_value_score, 2),
                    "relative_value_pass": bool(relative_value_score >= min_relative),
                    "composite_score": round(composite, 4),
                    "value_score": round(_safe_float(row.get("value_score")) or 0.0, 4),
                    "momentum_score": round(momentum_score, 4),
                    "quality_score": round(_safe_float(row.get("quality_score")) or 0.0, 4),
                    "flow_score": round(flow_score, 4),
                    "news_score": round(news_score, 4),
                    "sector_leader_rank": round(leader_rank, 4),
                    "sector_reversion_signal": round(_safe_float(row.get("sector_reversion_signal")) or 0.0, 4),
                    "analyst_target_upside_pct": round(_safe_float(row.get("analyst_target_upside_pct")) or 0.0, 4),
                    "analyst_peer_alpha_5d": round(_safe_float(row.get("analyst_peer_alpha_5d")) or 0.0, 4),
                    "analyst_peer_support_count": int(_safe_int(row.get("analyst_peer_support_count"))),
                    "event_expected_alpha_5d": round(event_alpha, 4),
                    "lag_score": round(lag_component, 2),
                    "reasons": reasons[:4],
                }
            )
        if not sector_rows:
            continue
        sector_rows = sorted(sector_rows, key=lambda item: float(item.get("relative_value_score", 0.0) or 0.0), reverse=True)
        for idx, item in enumerate(sector_rows, start=1):
            item["sector_rank"] = int(idx)
        sector_candidate_rows.extend(sector_rows)
        sector_buckets.append(
            {
                "sector": sector,
                "count": len(sector_rows),
                "pass_count": sum(1 for row in sector_rows if row.get("relative_value_pass")),
                "leader": sector_rows[0]["name"],
                "leader_symbol": sector_rows[0]["symbol"],
                "top_candidates": sector_rows[:5],
            }
        )

    sector_candidate_rows = sorted(
        sector_candidate_rows,
        key=lambda item: (float(item.get("relative_value_score", 0.0) or 0.0), -int(item.get("sector_rank", 9999) or 9999)),
        reverse=True,
    )
    sector_buckets = sorted(sector_buckets, key=lambda item: int(item.get("pass_count", 0) or 0), reverse=True)
    return {
        "generated_at": _now_iso(),
        "active_sectors": sorted(active_sectors),
        "min_relative_value_score": min_relative,
        "candidates": sector_candidate_rows,
        "sector_buckets": sector_buckets,
        "top_candidates": sector_candidate_rows[:20],
    }


def _quote_source(inputs: dict[str, Any]) -> str:
    api = inputs.get("api_integration") or {}
    kis = api.get("kis") or {}
    source = str(kis.get("source") or "")
    return source or "unknown"


def _timing_score(record: dict[str, Any], *, quote_source: str) -> tuple[float, float, list[str]]:
    support = int(record.get("close_swing_support_score", 0) or 0)
    ranking = float(record.get("close_swing_ranking_score", 0.0) or 0.0)
    recovering = bool(record.get("close_swing_recovering"))
    price_change_pct = _safe_float(record.get("close_swing_price_change_pct"))

    support_component = _clip(support / 10.0) * 100.0
    rank_component = _scale(ranking, 0.0, 1000.0) * 100.0
    recovery_component = 100.0 if recovering else 0.0
    score = support_component * 0.55 + rank_component * 0.35 + recovery_component * 0.10
    penalty = 0.0
    notes: list[str] = []
    if price_change_pct is not None and price_change_pct >= 5.0:
        overheat_penalty = min(12.0, max(0.0, (price_change_pct - 5.0) * 2.5))
        penalty += overheat_penalty
        notes.append(f"overheat_penalty {overheat_penalty:.1f}")
    if quote_source and quote_source != "primary":
        penalty += 8.0
        notes.append(f"quote_penalty {quote_source}")
    return round(max(0.0, score - penalty), 2), round(penalty, 2), notes


def annotate_event_candidates_with_hybrid(
    records: list[dict[str, Any]],
    *,
    sector_rotation: dict[str, Any],
    relative_value: dict[str, Any],
    sector_thesis: Optional[dict[str, Any]] = None,
    inputs: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    inputs = inputs or {}
    sector_map = {str(row.get("sector") or ""): row for row in sector_rotation.get("sectors", []) or []}
    relative_map = {str(row.get("symbol") or ""): row for row in relative_value.get("candidates", []) or []}
    thesis_map = ((sector_thesis or {}).get("by_sector") or {}) if isinstance(sector_thesis, dict) else {}
    wics_meta_map = load_effective_wics_sector_meta()
    quote_source = _quote_source(inputs)
    relative_min = int(relative_value.get("min_relative_value_score", getattr(SETTINGS, "HYBRID_RELATIVE_VALUE_MIN_SCORE", 55)) or 55)
    active_threshold = int(sector_rotation.get("active_sector_min_score", getattr(SETTINGS, "HYBRID_ACTIVE_SECTOR_MIN_SCORE", 60)) or 60)
    shadow_only = bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True))

    annotated: list[dict[str, Any]] = []
    for record in records:
        item = dict(record)
        sector = canonical_sector_name(item.get("context_sector") or item.get("sector"))
        symbol = _norm_symbol(item.get("stock_code"))
        sector_row = sector_map.get(sector) or {}
        relative_row = relative_map.get(symbol) or {}
        thesis_row = thesis_map.get(sector) or {}
        wics_row = wics_meta_map.get(sector) or {}
        sector_score = float(sector_row.get("sector_regime_score", 0.0) or 0.0)
        relative_score = float(relative_row.get("relative_value_score", 0.0) or 0.0)
        timing_score, timing_penalty, timing_notes = _timing_score(item, quote_source=quote_source)

        sector_final_score = float(thesis_row.get("final_sector_score", 0.0) or 0.0)
        sector_final_label = str(thesis_row.get("final_label") or "")
        sector_trade_posture = str(thesis_row.get("trade_posture") or "")
        wics_status_label = str(wics_row.get("universe_status_label") or "")
        wics_history_label = str(wics_row.get("history_confidence_label") or "")
        wics_dynamic_count = _safe_int(wics_row.get("dynamic_count"))
        wics_dynamic_stability = _safe_float(wics_row.get("avg_dynamic_stability"))
        wics_universe_reason = str(wics_row.get("universe_status_reason") or "")
        wics_penalty = _wics_universe_penalty(wics_status_label, wics_dynamic_count, wics_dynamic_stability)
        wics_note = _wics_universe_note(wics_status_label, wics_universe_reason, wics_dynamic_count, wics_dynamic_stability)
        sector_pass = bool(sector_trade_posture == "candidate") and sector_final_score >= max(55.0, active_threshold - 5.0)
        relative_pass = bool(relative_row.get("pool_eligible")) and relative_score >= relative_min
        timing_pass = bool(item.get("close_swing_eligible"))
        sector_step_score_base = sector_final_score if sector_final_score > 0 else sector_score
        sector_step_score = max(0.0, sector_step_score_base - wics_penalty)
        final_score = round(sector_step_score * 0.40 + relative_score * 0.35 + timing_score * 0.25, 2)

        blocked_reason_code = ""
        if not sector_pass:
            if sector_trade_posture == "watch":
                blocked_reason_code = "sector_watch_only"
            elif sector_final_label == "보류":
                blocked_reason_code = "sector_hold"
            else:
                blocked_reason_code = "inactive_sector"
        elif not relative_row:
            blocked_reason_code = "relative_value_missing"
        elif not relative_pass:
            blocked_reason_code = "relative_value_below_threshold"
        elif not timing_pass:
            blocked_reason_code = str(item.get("close_swing_reason") or "timing_not_ready")

        data_quality_flags = list((sector_rotation.get("data_quality_flags") or [])[:6])
        shadow_pass = sector_pass and relative_pass and timing_pass
        item.update(
            {
                "hybrid_enabled": True,
                "hybrid_shadow_only": shadow_only,
                "hybrid_sector": sector,
                "hybrid_sector_regime_score": round(sector_score, 2),
                "hybrid_sector_final_score": round(sector_final_score, 2),
                "hybrid_sector_active": bool(sector_row.get("active")),
                "hybrid_sector_rank": _safe_int(sector_row.get("rank")),
                "hybrid_sector_label": "활성" if sector_pass else "비활성",
                "hybrid_sector_reasons": list(sector_row.get("reasons") or [])[:4],
                "hybrid_sector_flow_lens_score": round(float(thesis_row.get("flow_lens_score", 0.0) or 0.0), 2),
                "hybrid_sector_quant_lens_score": round(float(thesis_row.get("quant_lens_score", 0.0) or 0.0), 2),
                "hybrid_sector_macro_lens_score": round(float(thesis_row.get("macro_lens_score", 0.0) or 0.0), 2),
                "hybrid_sector_agreement_level": str(thesis_row.get("agreement_level") or ""),
                "hybrid_sector_final_label": sector_final_label,
                "hybrid_sector_action_hint": str(thesis_row.get("action_hint") or ""),
                "hybrid_sector_reason_codes": list(thesis_row.get("reason_codes") or [])[:4],
                "hybrid_sector_human_summary": str(thesis_row.get("human_summary") or ""),
                "hybrid_wics_status_label": wics_status_label,
                "hybrid_wics_history_confidence_label": wics_history_label,
                "hybrid_wics_dynamic_count": wics_dynamic_count,
                "hybrid_wics_dynamic_stability": round(float(wics_dynamic_stability or 0.0), 3) if wics_dynamic_stability is not None else 0.0,
                "hybrid_wics_final_count": _safe_int(wics_row.get("final_count")),
                "hybrid_wics_mismatch_count": _safe_int(wics_row.get("mismatch_count")),
                "hybrid_wics_penalty": round(wics_penalty, 2),
                "hybrid_wics_note": wics_note,
                "hybrid_relative_value_score": round(relative_score, 2),
                "hybrid_relative_pass": bool(relative_pass),
                "hybrid_relative_sector_rank": int(relative_row.get("sector_rank", 0) or 0),
                "hybrid_relative_reasons": list(relative_row.get("reasons") or [])[:4],
                "hybrid_timing_score": round(timing_score, 2),
                "hybrid_timing_pass": bool(timing_pass),
                "hybrid_timing_penalty": round(timing_penalty, 2),
                "hybrid_timing_notes": timing_notes[:4],
                "hybrid_final_trade_score": round(final_score, 2),
                "hybrid_shadow_pass": bool(shadow_pass),
                "hybrid_shadow_decision": "candidate" if shadow_pass else "blocked",
                "hybrid_blocked_reason_code": blocked_reason_code,
                "hybrid_data_quality_flags": data_quality_flags,
                "hybrid_quote_source": quote_source,
            }
        )
        annotated.append(item)
    return annotated


def finalize_shadow_book(
    records: list[dict[str, Any]],
    *,
    sector_rotation: dict[str, Any],
    relative_value: dict[str, Any],
    sector_thesis: Optional[dict[str, Any]] = None,
    live_selected_keys: Optional[set[str]] = None,
    live_mode: str = "event_only",
) -> dict[str, Any]:
    live_selected_keys = live_selected_keys or set()
    per_cycle = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_CANDIDATES_PER_CYCLE", 3) or 3))
    shadow_rows = sorted(
        [dict(row) for row in records],
        key=lambda item: float(item.get("hybrid_final_trade_score", 0.0) or 0.0),
        reverse=True,
    )
    chosen_keys: set[str] = set()
    sector_cycle_counts: dict[str, int] = {}
    max_sector_cycle = max(1, int(getattr(SETTINGS, "CLOSE_SWING_MAX_CANDIDATES_PER_SECTOR_PER_CYCLE", 1) or 1))
    chosen_count = 0
    for row in shadow_rows:
        key = str(row.get("stock_code") or "").zfill(6) + ":" + str(row.get("rcp_no") or "")
        sector = str(row.get("hybrid_sector") or row.get("context_sector") or row.get("sector") or "")
        if not bool(row.get("hybrid_shadow_pass")):
            row["hybrid_shadow_decision"] = "blocked"
            continue
        if chosen_count >= per_cycle:
            row["hybrid_shadow_decision"] = "deferred"
            row["hybrid_blocked_reason_code"] = "shadow_rank_below_cycle_cut"
            continue
        if sector and sector_cycle_counts.get(sector, 0) >= max_sector_cycle:
            row["hybrid_shadow_decision"] = "deferred"
            row["hybrid_blocked_reason_code"] = "shadow_sector_cycle_limit"
            continue
        row["hybrid_shadow_decision"] = "chosen"
        chosen_keys.add(key)
        chosen_count += 1
        if sector:
            sector_cycle_counts[sector] = sector_cycle_counts.get(sector, 0) + 1

    live_top = [str(row.get("stock_code") or "").zfill(6) for row in shadow_rows if (str(row.get("stock_code") or "").zfill(6) + ":" + str(row.get("rcp_no") or "")) in live_selected_keys]
    shadow_top = [str(row.get("stock_code") or "").zfill(6) for row in shadow_rows if str(row.get("hybrid_shadow_decision") or "") == "chosen"]

    for row in shadow_rows:
        key = str(row.get("stock_code") or "").zfill(6) + ":" + str(row.get("rcp_no") or "")
        row["live_selected"] = key in live_selected_keys

    live_only = [symbol for symbol in live_top if symbol not in shadow_top]
    shadow_only = [symbol for symbol in shadow_top if symbol not in live_top]
    blocked_hist = Counter(
        str(row.get("hybrid_blocked_reason_code") or "shadow_blocked")
        for row in shadow_rows
        if str(row.get("hybrid_shadow_decision") or "") == "blocked"
    )
    fallback_penalty_total = round(
        sum(float(row.get("hybrid_timing_penalty", 0.0) or 0.0) for row in shadow_rows),
        2,
    )
    return {
        "generated_at": _now_iso(),
        "shadow_only": bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)),
        "live_mode": live_mode,
        "market_mode": str(sector_rotation.get("market_mode") or "-"),
        "confidence_score": int(sector_rotation.get("confidence_score", 0) or 0),
        "active_sectors": list(sector_rotation.get("active_sectors") or []),
        "active_sector_count": len(sector_rotation.get("active_sectors") or []),
        "sector_thesis": {
            "top_sectors": list((sector_thesis or {}).get("top_sectors") or [])[:5],
        },
        "top_relative_candidates": list(relative_value.get("top_candidates") or [])[:10],
        "candidate_count": len(shadow_rows),
        "shadow_chosen_count": len(shadow_top),
        "live_selected_count": len(live_top),
        "live_top_symbols": live_top[:10],
        "shadow_top_symbols": shadow_top[:10],
        "live_only_symbols": live_only[:10],
        "shadow_only_symbols": shadow_only[:10],
        "blocked_reason_histogram": dict(blocked_hist.most_common(10)),
        "quote_fallback_penalty_total": fallback_penalty_total,
        "rows": shadow_rows[:50],
    }


def save_hybrid_runtime_artifacts(
    sector_rotation: dict[str, Any],
    relative_value: dict[str, Any],
    shadow_book: dict[str, Any],
    sector_thesis: Optional[dict[str, Any]] = None,
) -> None:
    _write_json(SECTOR_ROTATION_PATH, sector_rotation)
    _write_json(RELATIVE_VALUE_PATH, relative_value)
    _write_json(HYBRID_SHADOW_BOOK_PATH, shadow_book)
    if sector_thesis:
        save_sector_thesis(sector_thesis)


def append_trade_decision_ledger(
    records: list[dict[str, Any]],
    *,
    sector_rotation: dict[str, Any],
    sector_thesis: Optional[dict[str, Any]] = None,
    live_selected_keys: Optional[set[str]] = None,
) -> None:
    live_selected_keys = live_selected_keys or set()
    thesis_map = ((sector_thesis or {}).get("by_sector") or {}) if isinstance(sector_thesis, dict) else {}
    timestamp = _now_iso()
    rows: list[dict[str, Any]] = []
    market_mode = str(sector_rotation.get("market_mode") or "-")
    active_sectors = list(sector_rotation.get("active_sectors") or [])
    for record in records:
        key = str(record.get("stock_code") or "").zfill(6) + ":" + str(record.get("rcp_no") or "")
        live_selected = key in live_selected_keys
        shadow_decision = str(record.get("hybrid_shadow_decision") or "")
        live_status = "chosen" if live_selected else str(record.get("close_swing_decision") or "blocked")
        sector = str(record.get("hybrid_sector") or record.get("context_sector") or record.get("sector") or "")
        thesis_row = thesis_map.get(sector) or {}
        rows.append(
            {
                "timestamp": timestamp,
                "event_date": str(record.get("event_date") or ""),
                "event_time_hhmm": str(record.get("event_time_hhmm") or ""),
                "symbol": str(record.get("stock_code") or "").zfill(6),
                "name": str(record.get("corp_name") or record.get("name") or ""),
                "sector": sector,
                "event_type": str(record.get("event_type") or ""),
                "market_regime": market_mode,
                "active_sectors": active_sectors,
                "flow_lens_score": float((thesis_row.get("flow_lens_score") or 0.0)),
                "quant_lens_score": float((thesis_row.get("quant_lens_score") or 0.0)),
                "macro_lens_score": float((thesis_row.get("macro_lens_score") or 0.0)),
                "flow_confidence": float((thesis_row.get("flow_confidence") or 0.0)),
                "quant_confidence": float((thesis_row.get("quant_confidence") or 0.0)),
                "macro_confidence": float((thesis_row.get("macro_confidence") or 0.0)),
                "sector_agreement_level": str(thesis_row.get("agreement_level") or ""),
                "sector_final_label": str(thesis_row.get("final_label") or ""),
                "sector_reason_codes": list(thesis_row.get("reason_codes") or []),
                "wics_universe_status_label": str(record.get("hybrid_wics_status_label") or ""),
                "wics_history_confidence_label": str(record.get("hybrid_wics_history_confidence_label") or ""),
                "wics_dynamic_count": int(record.get("hybrid_wics_dynamic_count", 0) or 0),
                "wics_dynamic_stability": float(record.get("hybrid_wics_dynamic_stability", 0.0) or 0.0),
                "wics_final_count": int(record.get("hybrid_wics_final_count", 0) or 0),
                "wics_mismatch_count": int(record.get("hybrid_wics_mismatch_count", 0) or 0),
                "wics_penalty": float(record.get("hybrid_wics_penalty", 0.0) or 0.0),
                "wics_note": str(record.get("hybrid_wics_note") or ""),
                "sector_regime_score": float(record.get("hybrid_sector_regime_score", 0.0) or 0.0),
                "relative_value_score": float(record.get("hybrid_relative_value_score", 0.0) or 0.0),
                "timing_score": float(record.get("hybrid_timing_score", 0.0) or 0.0),
                "final_trade_score": float(record.get("hybrid_final_trade_score", 0.0) or 0.0),
                "chosen": bool(live_selected),
                "shadow_decision": shadow_decision,
                "live_status": live_status,
                "blocked_reason_code": str(record.get("hybrid_blocked_reason_code") or record.get("close_swing_reason") or ""),
                "data_quality_flags": list(record.get("hybrid_data_quality_flags") or []),
                "quote_source": str(record.get("hybrid_quote_source") or ""),
                "budget_krw": int(record.get("close_swing_budget_krw", 0) or 0),
                "take_profit_pct": float(record.get("close_swing_take_profit_pct", 0.0) or 0.0),
                "stop_loss_pct": float(record.get("close_swing_stop_loss_pct", 0.0) or 0.0),
                "hold_policy": {
                    "entry_style": "close_bet",
                    "max_hold_days": int(getattr(SETTINGS, "CLOSE_BET_MAX_HOLD_DAYS", 3) or 3),
                    "time_exit_start": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_START", "15:10") or "15:10"),
                    "time_exit_end": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_END", "15:20") or "15:20"),
                },
                "shadow_only": bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)),
            }
        )
    if rows:
        _append_jsonl(TRADE_DECISION_LEDGER_PATH, rows)


def _load_jsonl_rows(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    rows: list[dict[str, Any]] = []
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
                if isinstance(row, dict):
                    rows.append(row)
    except Exception:
        return []
    return rows


def _price_lookup_from_factor_archives() -> dict[tuple[str, str], float]:
    snapshot_dir = os.path.join(ROOT_DIR, "factors", "snapshots")
    rows: dict[tuple[str, str], float] = {}
    for path in sorted(
        [os.path.join(snapshot_dir, name) for name in os.listdir(snapshot_dir)]
        if os.path.isdir(snapshot_dir)
        else []
    ):
        name = os.path.basename(path)
        if not name.startswith("factor_snapshot_") or not name.endswith(".csv"):
            continue
        try:
            df = pd.read_csv(path, usecols=["snapshot_date", "symbol", "close"])
        except Exception:
            continue
        for _, row in df.iterrows():
            date_text = str(row.get("snapshot_date") or "").strip()
            symbol = _norm_symbol(row.get("symbol"))
            close = _safe_float(row.get("close"))
            if date_text and symbol and close is not None:
                rows[(symbol, date_text)] = float(close)
    return rows


def build_hybrid_study_pack() -> dict[str, Any]:
    sector_rotation = _read_json(SECTOR_ROTATION_PATH)
    relative_value = _read_json(RELATIVE_VALUE_PATH)
    shadow_book = _read_json(HYBRID_SHADOW_BOOK_PATH)
    sector_thesis = _read_json(os.path.join(ROOT_DIR, "runtime", "sector_thesis_latest.json"))
    generated_runtime = False
    if not sector_rotation:
        inputs = load_hybrid_inputs()
        sector_rotation = compute_sector_rotation(inputs)
        generated_runtime = True
    if not relative_value:
        inputs = load_hybrid_inputs()
        relative_value = compute_relative_value_candidates(inputs, sector_rotation)
        generated_runtime = True
    if not sector_thesis and sector_rotation and relative_value:
        sector_thesis = build_sector_thesis(sector_rotation=sector_rotation, relative_value=relative_value)
        sector_rotation = merge_sector_thesis_into_rotation(sector_rotation, sector_thesis)
        generated_runtime = True
    if not shadow_book:
        shadow_book = {
            "generated_at": _now_iso(),
            "shadow_only": bool(getattr(SETTINGS, "HYBRID_SHADOW_ONLY", True)),
            "market_mode": str(sector_rotation.get("market_mode") or "-"),
            "confidence_score": int(sector_rotation.get("confidence_score", 0) or 0),
            "active_sectors": list(sector_rotation.get("active_sectors") or []),
            "candidate_count": 0,
            "shadow_chosen_count": 0,
            "live_selected_count": 0,
            "live_top_symbols": [],
            "shadow_top_symbols": [],
            "live_only_symbols": [],
            "shadow_only_symbols": [],
            "blocked_reason_histogram": {},
            "quote_fallback_penalty_total": 0.0,
            "rows": [],
        }
        generated_runtime = True
    if generated_runtime:
        save_hybrid_runtime_artifacts(sector_rotation, relative_value, shadow_book, sector_thesis=sector_thesis)
    ledger_rows = _load_jsonl_rows(TRADE_DECISION_LEDGER_PATH)
    price_lookup = _price_lookup_from_factor_archives()

    now = datetime.now()
    active_sector_rows = list(sector_rotation.get("top_sectors") or [])[:5]
    relative_top = list(relative_value.get("top_candidates") or [])[:10]
    recent_rows = [row for row in ledger_rows if str(row.get("timestamp") or "") >= (now - timedelta(days=20)).isoformat(timespec="seconds")]

    labeled_rows: list[dict[str, Any]] = []
    sector_perf: dict[str, list[float]] = {}
    for row in recent_rows:
        symbol = _norm_symbol(row.get("symbol"))
        event_date = str(row.get("event_date") or "")
        if not symbol or not event_date:
            continue
        entry_close = price_lookup.get((symbol, event_date))
        if not entry_close:
            continue
        label_row = dict(row)
        for day in (1, 3, 5):
            target_dt = datetime.fromisoformat(event_date) + timedelta(days=day)
            future_close = price_lookup.get((symbol, target_dt.strftime("%Y-%m-%d")))
            if future_close:
                ret = (future_close / entry_close) - 1.0
                label_row[f"ret_d{day}"] = round(ret, 4)
                label_row[f"label_d{day}"] = "win" if ret > 0 else "loss"
        sector = str(row.get("sector") or "")
        if sector and "ret_d5" in label_row:
            sector_perf.setdefault(sector, []).append(float(label_row["ret_d5"]))
        labeled_rows.append(label_row)

    sector_recent_stats = []
    for sector, rets in sector_perf.items():
        if not rets:
            continue
        wins = sum(1 for item in rets if item > 0)
        sector_recent_stats.append(
            {
                "sector": sector,
                "count": len(rets),
                "win_rate_d5": round(wins / len(rets), 4),
                "avg_ret_d5": round(sum(rets) / len(rets), 4),
            }
        )
    sector_recent_stats.sort(key=lambda item: (float(item.get("avg_ret_d5", 0.0) or 0.0), float(item.get("win_rate_d5", 0.0) or 0.0)), reverse=True)

    study_pack = {
        "generated_at": _now_iso(),
        "active_sectors": active_sector_rows,
        "leaders_and_laggards": [
            {
                "sector": bucket.get("sector"),
                "leader": bucket.get("leader"),
                "top_candidates": bucket.get("top_candidates", [])[:3],
            }
            for bucket in (relative_value.get("sector_buckets") or [])[:8]
        ],
        "why_bought": [row for row in labeled_rows if row.get("chosen")][:10],
        "why_not_bought": [row for row in labeled_rows if not row.get("chosen")][:10],
        "shadow_vs_live": {
            "live_only_symbols": list(shadow_book.get("live_only_symbols") or [])[:10],
            "shadow_only_symbols": list(shadow_book.get("shadow_only_symbols") or [])[:10],
        },
        "performance_labels": labeled_rows[:30],
        "sector_recent_20d_stats": sector_recent_stats[:10],
        "top_relative_candidates": relative_top[:10],
    }
    return study_pack


def save_hybrid_study_pack(payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    payload = payload or build_hybrid_study_pack()
    _write_json(HYBRID_STUDY_PACK_PATH, payload)
    return payload
