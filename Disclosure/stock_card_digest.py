from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd

try:
    from context_alignment import decorate_items_with_alignment, load_latest_context_alignment
    from sector_thesis import load_latest_sector_thesis
    from signals.wics_universe import load_effective_wics_sector_meta
    from stock_card_render import render_card_lines, render_event_lines, render_intraday_lines, render_sector_lines
except Exception:
    from Disclosure.context_alignment import decorate_items_with_alignment, load_latest_context_alignment
    from Disclosure.sector_thesis import load_latest_sector_thesis
    from Disclosure.signals.wics_universe import load_effective_wics_sector_meta
    from Disclosure.stock_card_render import render_card_lines, render_event_lines, render_intraday_lines, render_sector_lines


CARD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cards")


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def _safe_float(value: Any, default: float = 0.0) -> float:
    text = _safe_text(value)
    if not text:
        return default
    try:
        return float(text)
    except Exception:
        return default


def _round_safe(value: Any, digits: int = 4, default: float = 0.0) -> float:
    return round(_safe_float(value, default=default), digits)


def _safe_symbol(value: Any) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    if text.isdigit():
        return text.zfill(6)
    return text


def _top_name_list(items: list[dict], limit: int = 3) -> str:
    names = []
    for item in items[:limit]:
        if item.get("name"):
            names.append(str(item.get("name")))
        elif item.get("symbol"):
            names.append(str(item.get("symbol")))
    return ", ".join(names) if names else "-"


def _top_event_type_list(items: list[dict], limit: int = 3) -> str:
    seen: list[str] = []
    for item in items:
        event_type = _safe_text(item.get("event_last_type"))
        if event_type and event_type not in seen:
            seen.append(event_type)
        if len(seen) >= limit:
            break
    return ", ".join(seen) if seen else "-"


def _decorate_with_sector_thesis(items: list[dict[str, Any]], sector_thesis: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    thesis_map = ((sector_thesis or {}).get("by_sector") or {}) if isinstance(sector_thesis, dict) else {}
    if not thesis_map:
        return items
    decorated: list[dict[str, Any]] = []
    for row in items:
        item = dict(row)
        sector = _safe_text(item.get("sector"))
        thesis = thesis_map.get(sector) or {}
        if thesis:
            item["sector_flow_lens_score"] = _round_safe(thesis.get("flow_lens_score"), digits=2)
            item["sector_quant_lens_score"] = _round_safe(thesis.get("quant_lens_score"), digits=2)
            item["sector_macro_lens_score"] = _round_safe(thesis.get("macro_lens_score"), digits=2)
            item["sector_agreement_level"] = _safe_text(thesis.get("agreement_level"))
            item["sector_final_label"] = _safe_text(thesis.get("final_label"))
            item["sector_action_hint"] = _safe_text(thesis.get("action_hint"))
            item["sector_human_summary"] = _safe_text(thesis.get("human_summary"))
            item["sector_reason_codes"] = list(thesis.get("reason_codes") or [])
            item["sector_final_score"] = _round_safe(thesis.get("final_sector_score"), digits=2)
        decorated.append(item)
    return decorated


def _decorate_with_wics_universe_meta(items: list[dict[str, Any]], wics_sector_meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    meta_map = wics_sector_meta or {}
    if not meta_map:
        return items
    decorated: list[dict[str, Any]] = []
    for row in items:
        item = dict(row)
        sector = _safe_text(item.get("sector"))
        meta = meta_map.get(sector) or {}
        if meta:
            item["universe_status_label"] = _safe_text(meta.get("universe_status_label"))
            item["universe_status_reason"] = _safe_text(meta.get("universe_status_reason"))
            item["wics_sector_history_confidence_label"] = _safe_text(meta.get("history_confidence_label"))
            item["wics_sector_history_avg_overlap"] = _round_safe(meta.get("history_avg_overlap"), digits=3, default=-1.0)
            item["wics_sector_history_sample_count"] = int(meta.get("history_sample_count", 0) or 0)
            item["wics_dynamic_count"] = int(meta.get("dynamic_count", 0) or 0)
            item["wics_dynamic_stability"] = _round_safe(meta.get("avg_dynamic_stability"), digits=3)
            item["wics_final_count"] = int(meta.get("final_count", 0) or 0)
            item["wics_mismatch_count"] = int(meta.get("mismatch_count", 0) or 0)
        decorated.append(item)
    return decorated


def _event_display_priority(row: dict[str, Any]) -> tuple[float, float, float, float, float, float]:
    tactical_rank = {
        "참고 가능": 5.0,
        "단기 반응형": 4.2,
        "지연 반응형": 4.0,
        "변동성 주의": 3.0,
        "보수적 관찰": 2.4,
        "보수적": 2.0,
        "존재 확인": 1.2,
        "표본 얕음": 1.5,
        "성과 대기": 0.8,
        "해석 보류": 0.5,
    }
    interpretation_rank = {
        "참고 가능": 4.0,
        "변동성 주의": 3.0,
        "보수적": 2.5,
        "표본 얕음": 1.5,
        "해석 보류": 0.5,
    }
    bias = _safe_text(row.get("event_last_bias")).lower()
    bias_rank = {"positive": 2.0, "negative": 1.5, "neutral": 0.5}.get(bias, 0.0)
    type_text = _safe_text(row.get("event_last_type")).upper()
    if bias == "neutral" and type_text in {"LARGE_HOLDER", "CORRECTION"}:
        bias_rank -= 0.25
    return (
        tactical_rank.get(_safe_text(row.get("event_tactical_label")), 0.0),
        bias_rank,
        interpretation_rank.get(_safe_text(row.get("event_interpretation_label")), 0.0),
        _safe_float(row.get("event_interpretable_score")),
        _safe_float(row.get("event_valid_sample_size")),
        _safe_float(row.get("event_recent_count")),
    )


def _card_bucket(item: dict) -> str:
    regime = str(item.get("decision_regime") or "normal").lower()
    active = int(item.get("active_source_count", 0) or 0)
    card_score = _safe_float(item.get("card_score"))
    intraday = _safe_float(item.get("flow_intraday_edge_score"))
    event_alpha = _safe_float(item.get("event_alpha_score"))
    if active >= 6 and card_score >= 0.72 and intraday > -0.15 and event_alpha > -0.15:
        bucket = "direct"
    elif intraday >= 0.25 or event_alpha >= 0.2 or (active >= 5 and card_score >= 0.68):
        bucket = "watch"
    else:
        bucket = "hold"

    if regime == "confirm_first" and bucket == "direct":
        bucket = "watch"
    if regime == "observe_only":
        bucket = "hold" if bucket != "hold" else bucket
    alignment = int(item.get("alignment_score", 0) or 0)
    if alignment <= -2:
        if bucket == "direct":
            return "watch"
        if bucket == "watch":
            return "hold"
    return bucket


def _build_action_buckets(cards: list[dict]) -> dict[str, list[dict]]:
    direct: list[dict] = []
    watch: list[dict] = []
    hold: list[dict] = []
    for item in cards[:8]:
        bucket = _card_bucket(item)
        if bucket == "direct":
            direct.append(item)
        elif bucket == "watch":
            watch.append(item)
        else:
            hold.append(item)
    return {"direct": direct, "watch": watch, "hold": hold}


def _build_action_overview(cards: list[dict]) -> list[str]:
    buckets = _build_action_buckets(cards)
    lines: list[str] = []
    if buckets["direct"]:
        lines.append(f"- 바로 사는 쪽: `{_top_name_list(buckets['direct'])}`")
    if buckets["watch"]:
        lines.append(f"- 눌림 확인 쪽: `{_top_name_list(buckets['watch'])}`")
    if buckets["hold"]:
        lines.append(f"- 보류/관찰 쪽: `{_top_name_list(buckets['hold'])}`")
    return lines


def _build_data_quality_summary(counts: dict[str, Any], cards: list[dict]) -> dict[str, Any]:
    warnings: list[str] = []
    ml_scores = [_safe_float(item.get("ml_pred_score")) for item in cards if _safe_text(item.get("ml_pred_score"))]
    ml_unique = len({round(value, 4) for value in ml_scores}) if ml_scores else 0
    if counts.get("intraday", 0) <= 0:
        warnings.append("장중 타이밍 신호가 비어 있습니다")
    if counts.get("event", 0) <= 0:
        warnings.append("공시 이벤트 점수가 비어 있습니다")
    if ml_scores and ml_unique <= 2:
        warnings.append("ML 점수 분산이 낮아 변별력이 약합니다")
    if counts.get("analyst", 0) <= 0:
        warnings.append("애널 커버리지가 얇습니다")
    if len(warnings) >= 3:
        label = "보수적"
    elif warnings:
        label = "중간"
    else:
        label = "양호"
    return {
        "label": label,
        "warnings": warnings,
        "ml_unique": ml_unique,
    }


def _build_decision_regime(
    counts: dict[str, Any],
    data_quality: dict[str, Any],
    context_alignment: dict[str, Any] | None = None,
) -> dict[str, str]:
    flow_cov = int(counts.get("flow", 0) or 0)
    intraday_cov = int(counts.get("intraday", 0) or 0)
    event_cov = int(counts.get("event", 0) or 0)
    label = str(data_quality.get("label") or "")
    wics_history_label = str((context_alignment or {}).get("wics_history_confidence_label") or "")

    if label == "보수적" and flow_cov <= 0 and intraday_cov <= 0:
        return {
            "name": "observe_only",
            "description": "실시간 수급 공백이 커서 오늘은 관찰 중심으로 한 단계 낮춰 해석합니다.",
        }
    if intraday_cov <= 0 or flow_cov <= 0 or event_cov <= 0:
        return {
            "name": "confirm_first",
            "description": "체력 신호는 보되, 체결/수급 확인 전까지는 눌림 확인 우선으로 해석합니다.",
        }
    if wics_history_label == "없음":
        return {
            "name": "confirm_first",
            "description": "WICS 히스토리 표본이 거의 없어 섹터 해석은 보조적으로만 두고, 종목 단위 확인을 우선합니다.",
        }
    if wics_history_label == "예비":
        return {
            "name": "confirm_first",
            "description": "WICS 히스토리 표본이 아직 얕아 섹터 강도보다 개별 종목 체력 확인을 우선합니다.",
        }
    return {
        "name": "normal",
        "description": "교집합 점수와 실시간 신호를 함께 반영하는 일반 해석 모드입니다.",
    }


def _build_overview_lines(summary: dict[str, Any]) -> list[str]:
    cards = summary.get("cards", []) or []
    if not cards:
        return []

    lines = ["", "*1. 한눈 요약*"]
    sector_counts: dict[str, int] = {}
    for item in cards[:6]:
        sector = _safe_text(item.get("sector")) or "기타"
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
    sector_text = ", ".join(
        f"{sector} {count}개"
        for sector, count in sorted(sector_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
    )
    if sector_text:
        lines.append(f"- 상위권은 `{sector_text}` 쪽에 먼저 몰려 있습니다.")

    strong_cross = [
        item for item in cards
        if int(item.get("active_source_count", 0) or 0) >= 6 and _safe_float(item.get("card_score")) >= 0.72
    ]
    if strong_cross:
        lines.append(f"- 교집합이 두꺼운 후보는 `{_top_name_list(strong_cross)}` 입니다.")

    timing_names = [
        item for item in cards
        if _safe_float(item.get("flow_intraday_edge_score")) >= 0.25
    ]
    if timing_names:
        lines.append(f"- 장중 타이밍이 상대적으로 살아 있는 종목은 `{_top_name_list(timing_names)}` 입니다.")

    caution_names = [
        item for item in cards
        if _safe_float(item.get("flow_intraday_edge_score")) <= -0.15
    ]
    if caution_names:
        lines.append(f"- 반대로 `{_top_name_list(caution_names)}` 쪽은 추격보다 눌림 확인이 더 낫습니다.")

    valuation_discount = [
        item
        for item in cards
        if _safe_text(item.get("fair_value_base"))
        and _safe_float(item.get("fair_value_confidence_score")) >= 0.55
        and _safe_float(item.get("fair_value_gap_pct")) >= 10.0
    ]
    valuation_premium = [
        item
        for item in cards
        if _safe_text(item.get("fair_value_base"))
        and _safe_float(item.get("fair_value_confidence_score")) >= 0.55
        and _safe_float(item.get("fair_value_gap_pct")) <= -10.0
    ]
    if valuation_discount:
        lines.append(f"- 기준 적정가 대비 할인 폭이 큰 후보는 `{_top_name_list(valuation_discount)}` 입니다.")
    if valuation_premium:
        lines.append(f"- 반대로 `{_top_name_list(valuation_premium)}` 쪽은 적정가 기준 선반영 가능성을 먼저 봅니다.")

    positive_event = [
        item for item in summary.get("event_leaders", [])
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_tactical_label") or "") == "참고 가능"
    ]
    short_term_event = [
        item for item in summary.get("event_leaders", [])
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_tactical_label") or "") == "단기 반응형"
    ]
    delayed_event = [
        item for item in summary.get("event_leaders", [])
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_tactical_label") or "") == "지연 반응형"
    ]
    shallow_event = [
        item for item in summary.get("event_leaders", [])
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_interpretation_label") or "") == "표본 얕음"
        and str(item.get("event_tactical_label") or "") not in {"단기 반응형", "지연 반응형", "보수적 관찰"}
    ]
    if positive_event:
        lines.append(f"- 공시 재료 중에선 `{_top_name_list(positive_event)}` 이 바로 참고할 만한 편입니다.")
    if short_term_event:
        lines.append(f"- 공시 재료 중 `단기 반응형`으로는 `{_top_name_list(short_term_event)}` 쪽이 먼저 보입니다.")
    if delayed_event:
        lines.append(f"- 하루 이틀 뒤 확인하는 `지연 반응형` 재료로는 `{_top_name_list(delayed_event)}` 쪽이 눈에 띕니다.")
    if not positive_event and shallow_event:
        event_types = _top_event_type_list(shallow_event)
        if event_types != "-":
            lines.append(f"- 공시 재료는 `{event_types}` 유형이 보이지만 아직 표본이 얕습니다.")
        else:
            lines.append(f"- 공시 재료는 `{_top_name_list(shallow_event)}` 이 눈에 띄지만 아직 표본이 얕습니다.")
    neutral_event = [
        item for item in summary.get("event_leaders", [])
        if str(item.get("event_tactical_label") or "") == "존재 확인"
    ]
    if neutral_event:
        lines.append(f"- 방향성이 약한 공시는 `{_top_name_list(neutral_event)}` 쪽이라 존재 확인 정도로만 보는 편이 낫습니다.")
    sector_event = [
        item for item in summary.get("event_leaders", [])
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_sector_tactical_label") or "") in {"참고 가능", "단기 반응형", "지연 반응형"}
        and int(item.get("event_sector_valid_sample_size", 0) or 0) >= 3
    ]
    if sector_event:
        lines.append(
            f"- 같은 공시라도 섹터까지 보면 `{_top_name_list(sector_event)}` 쪽 해석이 조금 더 또렷합니다."
        )
    context_alignment = summary.get("context_alignment") or {}
    top_support = context_alignment.get("top_support", []) or []
    top_risk = context_alignment.get("top_risk", []) or []
    if top_support:
        support_text = ", ".join(row.get("sector", "-") for row in top_support[:3] if row.get("sector"))
        if support_text:
            lines.append(f"- 시장 큰 흐름과 같은 방향의 섹터는 `{support_text}` 입니다.")
    if top_risk:
        risk_text = ", ".join(row.get("sector", "-") for row in top_risk[:2] if row.get("sector"))
        if risk_text:
            lines.append(f"- 반대로 `{risk_text}` 쪽은 점수가 높아도 한 단계 보수적으로 읽는 편이 낫습니다.")
    return lines


def build_stock_card_summary(card_df: pd.DataFrame, top_n: int = 12) -> dict[str, Any]:
    if card_df.empty:
        return {
            "snapshot_at": pd.Timestamp.now().isoformat(),
            "counts": {"total": 0, "factor": 0, "analyst": 0, "flow": 0, "intraday": 0, "event": 0, "micro": 0, "ml": 0, "valuation": 0},
            "cards": [],
            "intraday_leaders": [],
            "event_leaders": [],
            "sector_recommendations": [],
        }

    work_df = card_df.copy()
    for col, default in [
        ("event_alpha_score", 0.0),
        ("event_expected_alpha_1d", 0.0),
        ("event_expected_alpha_3d", 0.0),
        ("event_expected_alpha_5d", 0.0),
        ("event_recent_count", 0),
        ("event_last_type", ""),
        ("event_last_bias", ""),
        ("event_best_strategy", ""),
        ("event_backtest_confidence", ""),
        ("event_valid_sample_size", 0),
        ("event_price_coverage_pct", 0.0),
        ("event_interpretation_label", ""),
        ("event_interpretation_note", ""),
        ("event_tactical_label", ""),
        ("event_tactical_note", ""),
        ("event_sector", ""),
        ("event_sector_valid_sample_size", 0),
        ("event_sector_price_coverage_pct", 0.0),
        ("event_sector_interpretation_label", ""),
        ("event_sector_tactical_label", ""),
        ("event_sector_interpretation_note", ""),
        ("event_sector_tactical_note", ""),
        ("event_sector_reaction_profile", ""),
        ("event_sector_interpretable_score", 0.0),
        ("event_interpretable_score", 0.0),
        ("event_win_rate_5d", 0.0),
        ("event_sample_size", 0),
        ("event_reaction_profile", ""),
        ("flow_intraday_edge_score", 0.0),
        ("flow_intraday_samples", 0),
        ("flow_intraday_avg_30m", 0.0),
        ("flow_intraday_avg_60m", 0.0),
        ("flow_intraday_last_event", ""),
        ("flow_score_raw", 0.0),
        ("flow_confidence_score", 0.0),
        ("flow_coverage_ratio", 0.0),
        ("flow_source_confidence", 0.0),
        ("flow_data_source", ""),
        ("flow_fallback_used", False),
        ("ml_pred_score", 0.0),
        ("ml_sector_score", 0.0),
        ("card_score", 0.0),
        ("current_price", 0.0),
        ("fair_value_bear", pd.NA),
        ("fair_value_base", pd.NA),
        ("fair_value_bull", pd.NA),
        ("fair_value_gap_pct", pd.NA),
        ("fair_value_confidence_score", 0.0),
        ("fair_value_status_label", ""),
        ("valuation_primary_method", ""),
        ("valuation_anchor_mix", ""),
        ("valuation_peer_group", ""),
        ("valuation_reason_summary", ""),
        ("valuation_missing_inputs", ""),
    ]:
        if col not in work_df.columns:
            work_df[col] = default

    counts = {
        "total": int(len(work_df)),
        "factor": int(work_df.get("composite_score", pd.Series(dtype=float)).notna().sum()),
        "analyst": int(work_df.get("analyst_conviction_score", pd.Series(dtype=float)).notna().sum()),
        "flow": int(work_df.get("flow_state_score", pd.Series(dtype=float)).notna().sum()),
        "intraday": int(work_df.get("flow_intraday_edge_score", pd.Series(dtype=float)).notna().sum()),
        "event": int(work_df.get("event_alpha_score", pd.Series(dtype=float)).notna().sum()),
        "micro": int(work_df.get("microstructure_score", pd.Series(dtype=float)).notna().sum()),
        "ml": int(work_df.get("ml_pred_score", pd.Series(dtype=float)).notna().sum()),
        "valuation": int(pd.to_numeric(work_df.get("fair_value_base", pd.Series(dtype=float)), errors="coerce").notna().sum()),
    }
    cards = []
    for row in work_df.head(top_n).to_dict("records"):
        cards.append(
            {
                "symbol": _safe_symbol(row.get("symbol")),
                "name": _safe_text(row.get("name")),
                "sector": _safe_text(row.get("sector")) or "Unknown",
                "card_score": _round_safe(row.get("card_score")),
                "composite_score": _round_safe(row.get("composite_score")),
                "analyst_conviction_score": _round_safe(row.get("analyst_conviction_score")),
                "flow_state_score": _round_safe(row.get("flow_state_score")),
                "flow_intraday_edge_score": _round_safe(row.get("flow_intraday_edge_score")),
                "event_alpha_score": _round_safe(row.get("event_alpha_score")),
                "event_expected_alpha_1d": _round_safe(row.get("event_expected_alpha_1d")),
                "event_expected_alpha_3d": _round_safe(row.get("event_expected_alpha_3d")),
                "event_expected_alpha_5d": _round_safe(row.get("event_expected_alpha_5d")),
                "event_recent_count": int(row.get("event_recent_count", 0) or 0),
                "event_last_type": _safe_text(row.get("event_last_type")),
                "event_last_bias": _safe_text(row.get("event_last_bias")),
                "event_best_strategy": _safe_text(row.get("event_best_strategy")),
                "event_backtest_confidence": _safe_text(row.get("event_backtest_confidence")),
                "event_valid_sample_size": int(row.get("event_valid_sample_size", 0) or 0),
                "event_price_coverage_pct": _round_safe(row.get("event_price_coverage_pct")),
                "event_interpretation_label": _safe_text(row.get("event_interpretation_label")),
                "event_interpretation_note": _safe_text(row.get("event_interpretation_note")),
                "event_tactical_label": _safe_text(row.get("event_tactical_label")),
                "event_tactical_note": _safe_text(row.get("event_tactical_note")),
                "event_sector": _safe_text(row.get("event_sector")),
                "event_sector_valid_sample_size": int(row.get("event_sector_valid_sample_size", 0) or 0),
                "event_sector_price_coverage_pct": _round_safe(row.get("event_sector_price_coverage_pct")),
                "event_sector_interpretation_label": _safe_text(row.get("event_sector_interpretation_label")),
                "event_sector_tactical_label": _safe_text(row.get("event_sector_tactical_label")),
                "event_sector_interpretation_note": _safe_text(row.get("event_sector_interpretation_note")),
                "event_sector_tactical_note": _safe_text(row.get("event_sector_tactical_note")),
                "event_sector_reaction_profile": _safe_text(row.get("event_sector_reaction_profile")),
                "event_sector_interpretable_score": _round_safe(row.get("event_sector_interpretable_score")),
                "event_interpretable_score": _round_safe(row.get("event_interpretable_score")),
                "event_win_rate_5d": _round_safe(row.get("event_win_rate_5d")),
                "event_sample_size": int(row.get("event_sample_size", 0) or 0),
                "event_reaction_profile": _safe_text(row.get("event_reaction_profile")),
                "microstructure_score": _round_safe(row.get("microstructure_score")),
                "macro_micro_interaction_score": _round_safe(row.get("macro_micro_interaction_score")),
                "ml_pred_score": _round_safe(row.get("ml_pred_score")),
                "ml_pred_return_5d": _round_safe(_safe_float(row.get("ml_pred_return_5d")) * 100.0),
                "active_source_count": int(row.get("active_source_count", 0) or 0),
                "analyst_latest_title": _safe_text(row.get("analyst_latest_title")),
                "analyst_target_upside_pct": _round_safe(row.get("analyst_target_upside_pct")),
                "analyst_agreement_score": _round_safe(row.get("analyst_agreement_score")),
                "analyst_recency_score": _round_safe(row.get("analyst_recency_score")),
                "analyst_revision_breadth_score": _round_safe(row.get("analyst_revision_breadth_score")),
                "analyst_peer_spillover_score": _round_safe(row.get("analyst_peer_spillover_score")),
                "flow_top_brokers": _safe_text(row.get("flow_top_brokers")),
                "flow_event_mix": _safe_text(row.get("flow_event_mix")),
                "flow_score_raw": _round_safe(row.get("flow_score_raw")),
                "flow_confidence_score": _round_safe(row.get("flow_confidence_score")),
                "flow_coverage_ratio": _round_safe(row.get("flow_coverage_ratio")),
                "flow_source_confidence": _round_safe(row.get("flow_source_confidence")),
                "flow_data_source": _safe_text(row.get("flow_data_source")),
                "flow_fallback_used": bool(row.get("flow_fallback_used")),
                "flow_intraday_samples": int(row.get("flow_intraday_samples", 0) or 0),
                "flow_intraday_avg_30m": _round_safe(row.get("flow_intraday_avg_30m")),
                "flow_intraday_avg_60m": _round_safe(row.get("flow_intraday_avg_60m")),
                "flow_intraday_last_event": _safe_text(row.get("flow_intraday_last_event")),
                "current_price": _round_safe(row.get("current_price"), digits=2),
                "fair_value_bear": _round_safe(row.get("fair_value_bear"), digits=2) if _safe_text(row.get("fair_value_bear")) else None,
                "fair_value_base": _round_safe(row.get("fair_value_base"), digits=2) if _safe_text(row.get("fair_value_base")) else None,
                "fair_value_bull": _round_safe(row.get("fair_value_bull"), digits=2) if _safe_text(row.get("fair_value_bull")) else None,
                "fair_value_gap_pct": _round_safe(row.get("fair_value_gap_pct"), digits=2) if _safe_text(row.get("fair_value_gap_pct")) else None,
                "fair_value_confidence_score": _round_safe(row.get("fair_value_confidence_score"), digits=4),
                "fair_value_status_label": _safe_text(row.get("fair_value_status_label")),
                "valuation_primary_method": _safe_text(row.get("valuation_primary_method")),
                "valuation_basis_label": _safe_text(row.get("valuation_basis_label")),
                "valuation_basis_period": _safe_text(row.get("valuation_basis_period")),
                "valuation_input_source": _safe_text(row.get("valuation_input_source")),
                "valuation_multiple_current": _round_safe(row.get("valuation_multiple_current"), digits=2) if _safe_text(row.get("valuation_multiple_current")) else None,
                "valuation_multiple_target": _round_safe(row.get("valuation_multiple_target"), digits=2) if _safe_text(row.get("valuation_multiple_target")) else None,
                "valuation_multiple_unit": _safe_text(row.get("valuation_multiple_unit")) or "배",
                "operating_profit_yield_pct": _round_safe(row.get("operating_profit_yield_pct"), digits=2) if _safe_text(row.get("operating_profit_yield_pct")) else None,
                "operating_margin_pct": _round_safe(row.get("operating_margin_pct"), digits=2) if _safe_text(row.get("operating_margin_pct")) else None,
                "roe_current": _round_safe(row.get("roe_current"), digits=2) if _safe_text(row.get("roe_current")) else None,
                "profitability_metric_label": _safe_text(row.get("profitability_metric_label")),
                "profitability_metric_value": _round_safe(row.get("profitability_metric_value"), digits=2) if _safe_text(row.get("profitability_metric_value")) else None,
                "valuation_summary_paragraph": _safe_text(row.get("valuation_summary_paragraph")),
                "valuation_method_detail": _safe_text(row.get("valuation_method_detail")),
                "valuation_formula_hint": _safe_text(row.get("valuation_formula_hint")),
                "profitability_formula_hint": _safe_text(row.get("profitability_formula_hint")),
                "valuation_anchor_mix": _safe_text(row.get("valuation_anchor_mix")),
                "valuation_peer_group": _safe_text(row.get("valuation_peer_group")),
                "valuation_reason_summary": _safe_text(row.get("valuation_reason_summary")),
                "valuation_missing_inputs": _safe_text(row.get("valuation_missing_inputs")),
            }
        )

    context_alignment = load_latest_context_alignment()
    sector_thesis = load_latest_sector_thesis()
    wics_sector_meta = load_effective_wics_sector_meta()
    cards = decorate_items_with_alignment(cards, context_alignment)
    cards = _decorate_with_sector_thesis(cards, sector_thesis)
    cards = _decorate_with_wics_universe_meta(cards, wics_sector_meta)
    data_quality = _build_data_quality_summary(counts, cards)
    decision_regime = _build_decision_regime(counts, data_quality, context_alignment)
    for item in cards:
        item["decision_regime"] = decision_regime["name"]
        item["data_quality_label"] = data_quality["label"]
        item["decision_trace"] = {
            "active_sources": int(item.get("active_source_count", 0) or 0),
            "card_score": _round_safe(item.get("card_score")),
            "intraday_edge": _round_safe(item.get("flow_intraday_edge_score")),
            "event_alpha": _round_safe(item.get("event_alpha_score")),
            "flow_state": _round_safe(item.get("flow_state_score")),
            "flow_confidence_score": _round_safe(item.get("flow_confidence_score")),
            "flow_data_source": _safe_text(item.get("flow_data_source")) or "missing",
            "flow_fallback_used": bool(item.get("flow_fallback_used")),
        }

    intraday_signal = pd.to_numeric(work_df["flow_intraday_edge_score"], errors="coerce")
    intraday_df = work_df[intraday_signal.fillna(0) > 0].copy()
    intraday_df = intraday_df.sort_values(["flow_intraday_edge_score", "flow_intraday_samples"], ascending=[False, False])
    intraday_leaders = [
        {
            "symbol": _safe_symbol(row.get("symbol")),
            "name": _safe_text(row.get("name")),
            "sector": _safe_text(row.get("sector")) or "Unknown",
            "flow_intraday_edge_score": _round_safe(row.get("flow_intraday_edge_score")),
            "flow_intraday_samples": int(row.get("flow_intraday_samples", 0) or 0),
            "flow_intraday_avg_30m": _round_safe(row.get("flow_intraday_avg_30m")),
            "flow_intraday_avg_60m": _round_safe(row.get("flow_intraday_avg_60m")),
            "flow_intraday_last_event": _safe_text(row.get("flow_intraday_last_event")),
        }
        for row in intraday_df.head(min(5, top_n)).to_dict("records")
    ]
    intraday_leaders = decorate_items_with_alignment(intraday_leaders, context_alignment)

    event_signal = pd.to_numeric(work_df.get("event_interpretable_score", work_df.get("event_alpha_score")), errors="coerce")
    event_recent = pd.to_numeric(work_df.get("event_recent_count"), errors="coerce").fillna(0)
    event_df = work_df[(event_signal.fillna(0).abs() > 0) | event_recent.gt(0)].copy()
    for col, default in [
        ("event_alpha_score", 0.0),
        ("event_expected_alpha_1d", 0.0),
        ("event_expected_alpha_3d", 0.0),
        ("event_recent_count", 0),
        ("event_interpretable_score", 0.0),
        ("event_valid_sample_size", 0),
        ("event_backtest_confidence", ""),
        ("event_interpretation_label", ""),
        ("event_interpretation_note", ""),
        ("event_tactical_label", ""),
        ("event_tactical_note", ""),
        ("event_sector", ""),
        ("event_sector_valid_sample_size", 0),
        ("event_sector_price_coverage_pct", 0.0),
        ("event_sector_interpretation_label", ""),
        ("event_sector_tactical_label", ""),
        ("event_sector_interpretation_note", ""),
        ("event_sector_tactical_note", ""),
        ("event_sector_reaction_profile", ""),
        ("event_sector_interpretable_score", 0.0),
        ("event_price_coverage_pct", 0.0),
        ("event_sample_size", 0),
        ("event_best_strategy", ""),
        ("event_win_rate_5d", 0.0),
        ("event_reaction_profile", ""),
    ]:
        if col not in event_df.columns:
            event_df[col] = default
    event_df["_event_sort_key"] = event_df.apply(_event_display_priority, axis=1)
    event_df = event_df.sort_values("_event_sort_key", ascending=False).drop(columns=["_event_sort_key"])
    event_leaders = [
        {
            "symbol": _safe_symbol(row.get("symbol")),
            "name": _safe_text(row.get("name")),
            "sector": _safe_text(row.get("sector")) or "Unknown",
            "event_alpha_score": _round_safe(row.get("event_alpha_score")),
            "event_interpretable_score": _round_safe(row.get("event_interpretable_score")),
            "event_expected_alpha_1d": _round_safe(row.get("event_expected_alpha_1d")),
            "event_expected_alpha_3d": _round_safe(row.get("event_expected_alpha_3d")),
            "event_expected_alpha_5d": _round_safe(row.get("event_expected_alpha_5d")),
            "event_recent_count": int(row.get("event_recent_count", 0) or 0),
            "event_last_type": _safe_text(row.get("event_last_type")),
            "event_last_bias": _safe_text(row.get("event_last_bias")),
            "event_best_strategy": _safe_text(row.get("event_best_strategy")),
            "event_backtest_confidence": _safe_text(row.get("event_backtest_confidence")),
            "event_valid_sample_size": int(row.get("event_valid_sample_size", 0) or 0),
            "event_price_coverage_pct": _round_safe(row.get("event_price_coverage_pct")),
            "event_interpretation_label": _safe_text(row.get("event_interpretation_label")),
            "event_interpretation_note": _safe_text(row.get("event_interpretation_note")),
            "event_tactical_label": _safe_text(row.get("event_tactical_label")),
            "event_tactical_note": _safe_text(row.get("event_tactical_note")),
            "event_sector": _safe_text(row.get("event_sector")),
            "event_sector_valid_sample_size": int(row.get("event_sector_valid_sample_size", 0) or 0),
            "event_sector_price_coverage_pct": _round_safe(row.get("event_sector_price_coverage_pct")),
            "event_sector_interpretation_label": _safe_text(row.get("event_sector_interpretation_label")),
            "event_sector_tactical_label": _safe_text(row.get("event_sector_tactical_label")),
            "event_sector_interpretation_note": _safe_text(row.get("event_sector_interpretation_note")),
            "event_sector_tactical_note": _safe_text(row.get("event_sector_tactical_note")),
            "event_sector_reaction_profile": _safe_text(row.get("event_sector_reaction_profile")),
            "event_sector_interpretable_score": _round_safe(row.get("event_sector_interpretable_score")),
            "event_sample_size": int(row.get("event_sample_size", 0) or 0),
            "event_win_rate_5d": _round_safe(row.get("event_win_rate_5d")),
            "event_reaction_profile": _safe_text(row.get("event_reaction_profile")),
        }
        for row in event_df.head(min(5, top_n)).to_dict("records")
    ]
    event_leaders = decorate_items_with_alignment(event_leaders, context_alignment)
    event_leaders = _decorate_with_sector_thesis(event_leaders, sector_thesis)
    event_leaders = _decorate_with_wics_universe_meta(event_leaders, wics_sector_meta)

    sector_frame = work_df.assign(
        sector=work_df.get("sector", pd.Series(dtype=object)).fillna("Unknown"),
        ml_sector_score=pd.to_numeric(work_df.get("ml_sector_score"), errors="coerce"),
        macro_sector_score=pd.to_numeric(work_df.get("macro_sector_score"), errors="coerce"),
        card_score=pd.to_numeric(work_df.get("card_score"), errors="coerce"),
    )
    leader_map: dict[str, list[str]] = {}
    leader_source = sector_frame.sort_values(["sector", "card_score"], ascending=[True, False])
    for sector_name, group in leader_source.groupby("sector", dropna=False):
        clean_sector = _safe_text(sector_name) or "Unknown"
        leader_map[clean_sector] = [
            clean_name
            for clean_name in (_safe_text(name) for name in group.get("name", pd.Series(dtype=object)).tolist())
            if clean_name
        ][:2]

    sector_view = (
        sector_frame
        .groupby("sector", dropna=False)
        .agg(
            avg_card_score=("card_score", "mean"),
            avg_ml_sector_score=("ml_sector_score", "mean"),
            avg_macro_sector_score=("macro_sector_score", "mean"),
            count=("symbol", "count"),
        )
        .reset_index()
        .sort_values(["avg_ml_sector_score", "avg_card_score", "avg_macro_sector_score"], ascending=[False, False, False])
    )
    sector_recommendations = [
        {
            "sector": _safe_text(row.get("sector")) or "Unknown",
            "avg_card_score": _round_safe(row.get("avg_card_score")),
            "avg_ml_sector_score": _round_safe(row.get("avg_ml_sector_score")),
            "avg_macro_sector_score": _round_safe(row.get("avg_macro_sector_score")),
            "count": int(row.get("count", 0) or 0),
            "leaders": leader_map.get(_safe_text(row.get("sector")) or "Unknown", []),
        }
        for row in sector_view.head(min(5, top_n)).to_dict("records")
    ]
    sector_recommendations = decorate_items_with_alignment(sector_recommendations, context_alignment)
    sector_recommendations = _decorate_with_sector_thesis(sector_recommendations, sector_thesis)
    sector_recommendations = _decorate_with_wics_universe_meta(sector_recommendations, wics_sector_meta)
    action_buckets = _build_action_buckets(cards)
    return {
        "snapshot_at": pd.Timestamp.now().isoformat(),
        "counts": counts,
        "cards": cards,
        "intraday_leaders": intraday_leaders,
        "event_leaders": event_leaders,
        "sector_recommendations": sector_recommendations,
        "action_buckets": {
            key: [
                {"symbol": item.get("symbol"), "name": item.get("name"), "sector": item.get("sector")}
                for item in rows
            ]
            for key, rows in action_buckets.items()
        },
        "data_quality": data_quality,
        "decision_regime": decision_regime,
        "context_alignment": {
            "market_mode": context_alignment.get("market_mode", "중립"),
            "confidence_score": int(context_alignment.get("confidence_score", 0) or 0),
            "top_support": (context_alignment.get("top_support") or [])[:4],
            "top_risk": (context_alignment.get("top_risk") or [])[:3],
            "wics_history_confidence_label": context_alignment.get("wics_history_confidence_label") or "없음",
            "wics_history_day_count": int(context_alignment.get("wics_history_day_count", 0) or 0),
            "wics_universe_regime": context_alignment.get("wics_universe_regime") or "-",
            "wics_confidence_note": context_alignment.get("wics_confidence_note") or "",
        },
        "sector_thesis": {
            "top_sectors": list((sector_thesis or {}).get("top_sectors") or [])[:6],
            "market_mode": _safe_text((sector_thesis or {}).get("market_mode")),
            "confidence_score": int((sector_thesis or {}).get("confidence_score", 0) or 0),
        },
    }


def save_stock_card_summary(card_df: pd.DataFrame, summary: dict[str, Any]) -> dict[str, str]:
    os.makedirs(CARD_DIR, exist_ok=True)
    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(CARD_DIR, f"stock_cards_{stamp}.csv")
    json_path = os.path.join(CARD_DIR, f"stock_cards_{stamp}.json")
    latest_csv_path = os.path.join(CARD_DIR, "stock_cards_latest.csv")
    latest_json_path = os.path.join(CARD_DIR, "stock_cards_latest.json")
    card_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    card_df.to_csv(latest_csv_path, index=False, encoding="utf-8-sig")
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    with open(latest_json_path, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    return {"cards_csv": csv_path, "cards_json": json_path, "cards_latest_csv": latest_csv_path, "cards_latest_json": latest_json_path}


def build_stock_card_digest(summary: dict[str, Any], top_n: int = 8) -> str:
    counts = summary.get("counts", {})
    lines = ["[종목 점검표] 통합 브리핑", f"- 생성시각: {summary.get('snapshot_at', '')}"]
    if counts:
        lines.append(
            f"- 커버리지: 전체 {counts.get('total', 0)} | 팩터 {counts.get('factor', 0)} | 애널 {counts.get('analyst', 0)} | "
            f"flow {counts.get('flow', 0)} | intraday {counts.get('intraday', 0)} | event {counts.get('event', 0)} | "
            f"micro {counts.get('micro', 0)} | ml {counts.get('ml', 0)} | 적정가 {counts.get('valuation', 0)}"
        )
        warnings = []
        if counts.get("factor", 0) == 0:
            warnings.append("팩터 커버리지 0")
        if counts.get("flow", 0) == 0:
            warnings.append("flow 커버리지 0")
        if counts.get("intraday", 0) == 0:
            warnings.append("intraday 커버리지 0")
        if counts.get("event", 0) == 0:
            warnings.append("event 커버리지 0")
        if counts.get("ml", 0) == 0:
            warnings.append("ml 커버리지 0")
        if counts.get("valuation", 0) == 0:
            warnings.append("적정가 커버리지 0")
        if warnings:
            lines.append("- 주의: " + " | ".join(warnings))
    lines.append("")
    lines.append("*0. 오늘 뭐 할까*")
    action_lines = _build_action_overview(summary.get("cards", []))
    lines.extend(action_lines or ["- 아직은 직접 매수보다 관찰 위주가 적절합니다."])
    data_quality = summary.get("data_quality") or {}
    decision_regime = summary.get("decision_regime") or {}
    if data_quality.get("label"):
        lines.append(f"- 데이터 품질: `{data_quality.get('label')}`")
    if decision_regime.get("description"):
        lines.append(f"- 해석 모드: {decision_regime.get('description')}")
    context_alignment = summary.get("context_alignment") or {}
    if context_alignment.get("market_mode"):
        lines.append(
            f"- 시장 맥락: `{context_alignment.get('market_mode')}` | 확신도 `{int(context_alignment.get('confidence_score', 0) or 0)}/100`"
        )
    wics_history_label = context_alignment.get("wics_history_confidence_label")
    if wics_history_label:
        lines.append(
            f"- WICS 표본: `{wics_history_label}` ({int(context_alignment.get('wics_history_day_count', 0) or 0)}일) | 유니버스 `{context_alignment.get('wics_universe_regime') or '-'}`"
        )
    top_support = context_alignment.get("top_support", []) or []
    top_risk = context_alignment.get("top_risk", []) or []
    if top_support:
        lines.append("- 맥락 정렬 섹터: `" + ", ".join(row.get("sector", "-") for row in top_support[:3]) + "`")
    if top_risk:
        lines.append("- 맥락 경계 섹터: `" + ", ".join(row.get("sector", "-") for row in top_risk[:2]) + "`")
    for warning in (data_quality.get("warnings") or [])[:2]:
        lines.append(f"- 데이터 주의: {warning}")
    if context_alignment.get("wics_confidence_note"):
        lines.append(f"- 맥락 주의: {context_alignment.get('wics_confidence_note')}")
    lines.extend(_build_overview_lines(summary))

    lines.append("")
    lines.append("*2. 지금 먼저 볼 후보*")
    for item in summary.get("cards", [])[:top_n]:
        lines.extend(render_card_lines(item))
    lines.extend(render_intraday_lines(summary.get("intraday_leaders", []), top_n))
    lines.extend(render_event_lines(summary.get("event_leaders", []), top_n))
    lines.extend(render_sector_lines(summary.get("sector_recommendations", []), top_n))
    return "\n".join(lines)
