from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SECTOR_THESIS_PATH = os.path.join(ROOT_DIR, "runtime", "sector_thesis_latest.json")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, "", "nan", "NaN"):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _clip(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _lens_label(score: float) -> str:
    if score >= 70:
        return "강함"
    if score >= 58:
        return "우호"
    if score >= 48:
        return "중립"
    return "약함"


def _agreement_level(flow: float, quant: float, macro: float) -> str:
    strong = sum(1 for score in (flow, quant, macro) if score >= 60)
    weak = sum(1 for score in (flow, quant, macro) if score < 50)
    if strong >= 2 and weak == 0:
        return "정렬"
    if strong >= 1 and weak >= 1:
        return "충돌"
    return "혼합"


def _build_reason_codes(
    *,
    flow_score: float,
    quant_score: float,
    macro_score: float,
    flow_confidence: float,
    wics_soft_prior: bool,
    data_quality_label: str,
) -> list[str]:
    codes: list[str] = []
    if wics_soft_prior or flow_confidence < 60:
        codes.append("wics_shallow_history")
    if flow_score >= 65 and quant_score < 55:
        codes.append("flow_short_term_only")
    if quant_score >= 65 and flow_score < 55:
        codes.append("quant_strong_but_sector_cold")
    if macro_score >= 65 and flow_score < 55:
        codes.append("macro_support_but_no_flow")
    if data_quality_label == "보수적":
        codes.append("data_quality_conservative")
    elif data_quality_label == "중간":
        codes.append("data_quality_mixed")
    if sum(1 for score in (flow_score, quant_score, macro_score) if score < 50) >= 2:
        codes.append("multi_lens_weak")
    return codes[:4]


def _build_final_label(
    *,
    flow_score: float,
    quant_score: float,
    macro_score: float,
) -> str:
    if flow_score >= 65 and quant_score >= 60 and macro_score >= 55:
        return "공통 우호"
    if flow_score >= 65 and quant_score < 55:
        return "단기 순환 우세"
    if quant_score >= 65 and flow_score < 55:
        return "체력 우위"
    if macro_score >= 65 and flow_score < 55 and quant_score < 60:
        return "매크로 우호"
    if sum(1 for score in (flow_score, quant_score, macro_score) if score < 50) >= 2:
        return "보류"
    return "관찰"


def _build_action_hint(final_label: str) -> str:
    mapping = {
        "공통 우호": "대표주 추격보다 후발주와 타이밍 확인이 낫다",
        "단기 순환 우세": "추격보다 눌림과 유동성 확인이 먼저다",
        "체력 우위": "후발주 중심으로 종가 타이밍을 보는 편이 낫다",
        "매크로 우호": "섹터는 보되 흐름 확인 전까지는 관찰이 먼저다",
        "관찰": "후보만 유지하고 장중 확인을 기다리는 편이 낫다",
        "보류": "섹터 추적보다 보류가 맞다",
    }
    return mapping.get(final_label, "후보만 유지하고 추가 확인을 기다리는 편이 낫다")


def _trade_posture(final_label: str) -> str:
    if final_label in {"공통 우호", "단기 순환 우세", "체력 우위"}:
        return "candidate"
    if final_label in {"매크로 우호", "관찰"}:
        return "watch"
    return "hold"


def build_sector_thesis(*, sector_rotation: dict[str, Any], relative_value: dict[str, Any]) -> dict[str, Any]:
    sector_rows = list(sector_rotation.get("sectors") or [])
    bucket_map = {
        str(item.get("sector") or ""): item
        for item in (relative_value.get("sector_buckets") or [])
        if str(item.get("sector") or "")
    }
    data_quality_label = str(sector_rotation.get("data_quality_label") or "-")
    top_rows: list[dict[str, Any]] = []
    by_sector: dict[str, dict[str, Any]] = {}

    for row in sector_rows:
        sector = str(row.get("sector") or "")
        if not sector:
            continue
        bucket = bucket_map.get(sector) or {}
        top_candidates = list(bucket.get("top_candidates") or [])
        top_relative_scores = [
            _safe_float(item.get("relative_value_score"))
            for item in top_candidates[:3]
            if item.get("relative_value_score") is not None
        ]
        relative_component = sum(top_relative_scores) / len(top_relative_scores) if top_relative_scores else 35.0

        flow_lens_score = _clip(_safe_float(row.get("wics_component")) * 0.75 + _safe_float(row.get("breadth_stability_component")) * 0.25)
        quant_lens_score = _clip(
            _safe_float(row.get("factor_card_component")) * 0.60
            + _safe_float(row.get("breadth_stability_component")) * 0.15
            + relative_component * 0.25
        )
        macro_lens_score = _clip(_safe_float(row.get("macro_context_component")))

        has_wics_signal = abs(_safe_float(row.get("wics_rotation_score"))) > 0 or abs(_safe_float(row.get("wics_risk_score"))) > 0
        wics_soft_scale = _safe_float(row.get("wics_soft_scale"), 1.0)
        flow_confidence = 30.0 + (50.0 * wics_soft_scale) + (15.0 if has_wics_signal else 0.0)
        if data_quality_label == "보수적":
            flow_confidence *= 0.80
        elif data_quality_label == "중간":
            flow_confidence *= 0.90
        flow_confidence = _clip(flow_confidence, 20.0, 95.0)

        pass_count = _safe_int(bucket.get("pass_count"))
        candidate_count = _safe_int(bucket.get("count"))
        sector_count = _safe_int(row.get("count"))
        quant_confidence = 40.0 + min(25.0, sector_count * 4.0) + min(20.0, candidate_count * 3.0) + min(15.0, pass_count * 4.0)
        if not bucket:
            quant_confidence -= 15.0
        if data_quality_label == "보수적":
            quant_confidence *= 0.85
        elif data_quality_label == "중간":
            quant_confidence *= 0.93
        quant_confidence = _clip(quant_confidence, 25.0, 95.0)

        alignment_score = _safe_int(row.get("context_alignment_score"))
        base_conf = _safe_float(sector_rotation.get("confidence_score"))
        macro_confidence = 35.0 + min(55.0, base_conf * 0.6)
        if macro_lens_score >= 60:
            macro_confidence += 5.0
        if abs(alignment_score) >= 1:
            macro_confidence += 5.0
        if data_quality_label == "보수적":
            macro_confidence *= 0.90
        elif data_quality_label == "중간":
            macro_confidence *= 0.95
        macro_confidence = _clip(macro_confidence, 25.0, 95.0)

        flow_effective = _clip(flow_lens_score * (flow_confidence / 100.0))
        quant_effective = _clip(quant_lens_score * (quant_confidence / 100.0))
        macro_effective = _clip(macro_lens_score * (macro_confidence / 100.0))

        final_label = _build_final_label(
            flow_score=flow_lens_score,
            quant_score=quant_lens_score,
            macro_score=macro_lens_score,
        )
        agreement_level = _agreement_level(flow_lens_score, quant_lens_score, macro_lens_score)
        final_sector_score = round(
            flow_effective * 0.40 + quant_effective * 0.35 + macro_effective * 0.25,
            2,
        )
        reason_codes = _build_reason_codes(
            flow_score=flow_lens_score,
            quant_score=quant_lens_score,
            macro_score=macro_lens_score,
            flow_confidence=flow_confidence,
            wics_soft_prior=bool(row.get("wics_soft_prior")),
            data_quality_label=data_quality_label,
        )
        action_hint = _build_action_hint(final_label)
        human_summary = (
            f"수급 {_lens_label(flow_lens_score)} / 퀀트 {_lens_label(quant_lens_score)} / "
            f"매크로 {_lens_label(macro_lens_score)} -> {final_label}, {action_hint}."
        )

        item = {
            "sector": sector,
            "leader_name": str(row.get("leader_name") or row.get("leader_symbol") or "-"),
            "top_candidates": top_candidates[:3],
            "flow_lens_score": round(flow_lens_score, 2),
            "quant_lens_score": round(quant_lens_score, 2),
            "macro_lens_score": round(macro_lens_score, 2),
            "flow_confidence": round(flow_confidence, 2),
            "quant_confidence": round(quant_confidence, 2),
            "macro_confidence": round(macro_confidence, 2),
            "agreement_level": agreement_level,
            "final_sector_score": final_sector_score,
            "final_label": final_label,
            "trade_posture": _trade_posture(final_label),
            "action_hint": action_hint,
            "reason_codes": reason_codes,
            "human_summary": human_summary,
            "lens_breakdown": {
                "flow_effective": round(flow_effective, 2),
                "quant_effective": round(quant_effective, 2),
                "macro_effective": round(macro_effective, 2),
            },
        }
        top_rows.append(item)
        by_sector[sector] = item

    top_rows.sort(key=lambda item: float(item.get("final_sector_score", 0.0) or 0.0), reverse=True)
    for idx, row in enumerate(top_rows, start=1):
        row["rank"] = idx

    return {
        "generated_at": _now_iso(),
        "market_mode": str(sector_rotation.get("market_mode") or "-"),
        "confidence_score": _safe_int(sector_rotation.get("confidence_score")),
        "active_sectors": list(sector_rotation.get("active_sectors") or []),
        "top_sectors": top_rows[:10],
        "sectors": top_rows,
        "by_sector": by_sector,
    }


def merge_sector_thesis_into_rotation(
    sector_rotation: dict[str, Any],
    sector_thesis: dict[str, Any],
) -> dict[str, Any]:
    thesis_map = sector_thesis.get("by_sector") or {}
    if not thesis_map:
        return sector_rotation
    merged = dict(sector_rotation)
    rows = []
    for row in sector_rotation.get("sectors", []) or []:
        item = dict(row)
        thesis = thesis_map.get(str(item.get("sector") or "")) or {}
        if thesis:
            item["lens_breakdown"] = {
                "flow_lens_score": thesis.get("flow_lens_score"),
                "quant_lens_score": thesis.get("quant_lens_score"),
                "macro_lens_score": thesis.get("macro_lens_score"),
                "flow_confidence": thesis.get("flow_confidence"),
                "quant_confidence": thesis.get("quant_confidence"),
                "macro_confidence": thesis.get("macro_confidence"),
                "flow_effective": (thesis.get("lens_breakdown") or {}).get("flow_effective"),
                "quant_effective": (thesis.get("lens_breakdown") or {}).get("quant_effective"),
                "macro_effective": (thesis.get("lens_breakdown") or {}).get("macro_effective"),
            }
            item["agreement_level"] = thesis.get("agreement_level")
            item["final_label"] = thesis.get("final_label")
            item["action_hint"] = thesis.get("action_hint")
            item["reason_codes"] = list(thesis.get("reason_codes") or [])
            item["human_summary"] = thesis.get("human_summary") or ""
            item["trade_posture"] = thesis.get("trade_posture") or "watch"
            item["final_sector_score"] = thesis.get("final_sector_score")
        rows.append(item)
    rows.sort(
        key=lambda item: (
            _safe_float(item.get("final_sector_score")),
            _safe_float(item.get("sector_regime_score")),
        ),
        reverse=True,
    )
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    merged["sectors"] = rows
    merged["top_sectors"] = rows[:10]
    merged["sector_thesis"] = sector_thesis
    return merged


def load_latest_sector_thesis() -> dict[str, Any]:
    if not os.path.exists(SECTOR_THESIS_PATH):
        return {}
    try:
        with open(SECTOR_THESIS_PATH, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def save_sector_thesis(payload: dict[str, Any]) -> dict[str, Any]:
    os.makedirs(os.path.dirname(SECTOR_THESIS_PATH), exist_ok=True)
    with open(SECTOR_THESIS_PATH, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    return payload
