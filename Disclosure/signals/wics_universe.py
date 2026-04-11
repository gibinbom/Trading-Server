from __future__ import annotations

import json
import math
import os
import re
from typing import Any

import pandas as pd

try:
    from config import SETTINGS
except Exception:  # pragma: no cover
    SETTINGS = object()


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARD_CSV_PATH = os.path.join(ROOT_DIR, "cards", "stock_cards_latest.csv")
FACTOR_CSV_PATH = os.path.join(ROOT_DIR, "factors", "snapshots", "factor_snapshot_latest.csv")
REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
OVERRIDE_PATH = os.path.join(REPORT_DIR, "wics_universe_overrides.json")
LATEST_UNIVERSE_PATH = os.path.join(REPORT_DIR, "wics_effective_universe_latest.json")

SECTOR_ALIASES = {
    "엔터테인먼트/미디어": "엔터테인먼트/게임",
    "게임엔터테인먼트": "엔터테인먼트/게임",
    "항공우주와방위산업": "방위산업/우주항공",
    "우주항공과국방": "방위산업/우주항공",
    "조선": "조선/해양",
    "통신장비": "통신장비/네트워크",
    "자동차부품": "자동차부품/타이어",
    "전기유틸리티": "전력/유틸리티",
    "해운사": "운송/해운/항공",
    "화학": "화학/석유화학",
    "생명보험": "보험",
    "손해보험": "보험",
}


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def normalize_sector_name(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    if ". " in text:
        text = text.split(". ", 1)[-1].strip()
    return SECTOR_ALIASES.get(text, text)


def _normalize_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _read_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _list_universe_archives() -> list[str]:
    try:
        names = sorted(
            name
            for name in os.listdir(REPORT_DIR)
            if name.startswith("wics_effective_universe_")
            and name.endswith(".json")
            and name != os.path.basename(LATEST_UNIVERSE_PATH)
        )
    except Exception:
        return []
    return [os.path.join(REPORT_DIR, name) for name in names]


def _archive_day_key(path: str) -> str:
    name = os.path.basename(path)
    match = re.search(r"wics_effective_universe_(\d{8})_\d{6}\.json$", name)
    return match.group(1) if match else ""


def _payload_day_key(payload: dict[str, Any]) -> str:
    generated_at = _clean_text((payload or {}).get("generated_at"))
    if not generated_at:
        return ""
    return generated_at.split("T", 1)[0].replace("-", "")


def _load_overrides() -> dict[str, Any]:
    payload = _read_json(OVERRIDE_PATH)
    return payload if isinstance(payload, dict) else {}


def _load_card_candidates() -> dict[str, dict[str, Any]]:
    if not os.path.exists(CARD_CSV_PATH):
        return {}
    try:
        df = pd.read_csv(CARD_CSV_PATH, dtype={"symbol": str})
    except Exception:
        return {}
    if df.empty:
        return {}

    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        sector = normalize_sector_name(row.get("sector"))
        if not symbol or not sector:
            continue
        out[symbol] = {
            "symbol": symbol,
            "name": _clean_text(row.get("name")) or symbol,
            "sector": sector,
            "card_score": _safe_float(row.get("card_score")),
            "factor_score": _safe_float(row.get("composite_score")),
            "active_source_count": _safe_int(row.get("active_source_count")),
            "flow_score": _safe_float(row.get("flow_score")),
            "analyst_score": _safe_float(row.get("analyst_conviction_score")),
        }
    return out


def _load_factor_candidates() -> dict[str, dict[str, Any]]:
    if not os.path.exists(FACTOR_CSV_PATH):
        return {}
    try:
        df = pd.read_csv(FACTOR_CSV_PATH, dtype={"symbol": str})
    except Exception:
        return {}
    if df.empty:
        return {}

    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        sector = normalize_sector_name(row.get("sector"))
        if not symbol or not sector:
            continue
        out[symbol] = {
            "symbol": symbol,
            "name": _clean_text(row.get("name")) or symbol,
            "sector": sector,
            "factor_score": _safe_float(row.get("composite_score")),
            "sector_leader_signal": _safe_float(row.get("sector_leader_signal")),
            "sector_leader_rank": _safe_float(row.get("sector_leader_rank")),
            "sector_rank_composite": _safe_float(row.get("sector_rank_composite_score")),
        }
    return out


def _preferred_share_penalty(name: str) -> float:
    text = _clean_text(name)
    if not text:
        return 0.0
    if re.search(r"(우|우B|[0-9]우B?)$", text):
        return 0.18
    return 0.0


def _shrink_to_neutral(raw_score: float, confidence: float, neutral: float = 0.5) -> float:
    return round(float(neutral) + (float(raw_score) - float(neutral)) * _clamp_confidence(confidence), 4)


def _clamp_confidence(value: Any) -> float:
    return max(0.0, min(1.0, _safe_float(value, 0.0)))


def _weighted_component_average(components: list[dict[str, float]]) -> float:
    total_weight = sum(_safe_float(item.get("weight")) for item in components)
    if total_weight <= 0:
        return 0.0
    weighted_sum = sum(_safe_float(item.get("score")) * _safe_float(item.get("weight")) for item in components)
    return round(weighted_sum / total_weight, 4)


def _estimate_subsample_stability(card_score: float, factor_score: float, source_count: int, sector_leader_rank: float) -> float:
    activity_score = min(1.0, max(0.0, float(source_count or 0)) / 6.0)
    components: list[dict[str, float]] = []
    if card_score > 0:
        components.append({"name": "card", "score": card_score, "weight": 0.45})
    if factor_score > 0:
        components.append({"name": "factor", "score": factor_score, "weight": 0.35})
    if activity_score > 0:
        components.append({"name": "activity", "score": activity_score, "weight": 0.10})
    if sector_leader_rank > 0:
        components.append({"name": "leader", "score": sector_leader_rank, "weight": 0.10})

    if not components:
        return 0.0
    if len(components) == 1:
        return 0.58

    full_score = _weighted_component_average(components)
    variant_scores: list[float] = []
    for idx in range(len(components)):
        subset = components[:idx] + components[idx + 1 :]
        if subset:
            variant_scores.append(_weighted_component_average(subset))

    if len(components) >= 3:
        sorted_components = sorted(components, key=lambda item: _safe_float(item.get("weight")), reverse=True)
        subset = sorted_components[:-1]
        if subset:
            variant_scores.append(_weighted_component_average(subset))

    if not variant_scores:
        return 0.58

    deviations = [abs(score - full_score) for score in variant_scores]
    mean_deviation = sum(deviations) / len(deviations)
    max_deviation = max(deviations)
    normalized_deviation = min(1.0, ((mean_deviation * 0.7) + (max_deviation * 0.3)) / 0.25)
    stability = 1.0 - normalized_deviation
    if len(components) == 2:
        stability *= 0.92
    return round(max(0.0, min(1.0, stability)), 4)


def _merge_dynamic_candidates() -> list[dict[str, Any]]:
    card = _load_card_candidates()
    factor = _load_factor_candidates()
    symbols = sorted(set(card) | set(factor))
    merged: list[dict[str, Any]] = []
    for symbol in symbols:
        c = card.get(symbol, {})
        f = factor.get(symbol, {})
        sector = normalize_sector_name(c.get("sector") or f.get("sector"))
        if not sector:
            continue
        name = _clean_text(c.get("name") or f.get("name")) or symbol
        card_score = _safe_float(c.get("card_score"))
        factor_score = _safe_float(c.get("factor_score"))
        source_count = _safe_int(c.get("active_source_count"))
        sector_leader_rank = _safe_float(f.get("sector_leader_rank"))
        dynamic_score_raw = (
            (card_score * 0.45)
            + (factor_score * 0.35)
            + (min(1.0, source_count / 6.0) * 0.10)
            + (sector_leader_rank * 0.10)
        )
        has_card = card_score > 0
        has_factor = factor_score > 0
        if has_card and has_factor:
            agreement_confidence = max(0.0, 1.0 - abs(card_score - factor_score))
        elif has_card or has_factor:
            agreement_confidence = 0.75
        else:
            agreement_confidence = 0.0
        evidence_confidence = min(1.0, source_count / 4.0)
        stability_confidence = _estimate_subsample_stability(card_score, factor_score, source_count, sector_leader_rank)
        consensus_confidence = round(
            (agreement_confidence * 0.45) + (evidence_confidence * 0.20) + (stability_confidence * 0.35),
            4,
        )
        dynamic_score = _shrink_to_neutral(dynamic_score_raw, consensus_confidence)
        merged.append(
            {
                "symbol": symbol,
                "name": name,
                "sector": sector,
                "card_score": card_score,
                "factor_score": factor_score,
                "active_source_count": source_count,
                "sector_leader_rank": sector_leader_rank,
                "dynamic_score_raw": round(dynamic_score_raw, 4),
                "agreement_confidence": round(agreement_confidence, 4),
                "stability_confidence": stability_confidence,
                "consensus_confidence": consensus_confidence,
                "dynamic_score": round(dynamic_score, 4),
                "evidence_count": int(card_score > 0) + int(factor_score > 0) + int(source_count >= 2) + int(sector_leader_rank >= 0.8),
            }
        )
    return merged


def _manual_include_map(raw: dict[str, Any]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for sector, payload in (raw or {}).items():
        sector_name = normalize_sector_name(sector)
        if not sector_name:
            continue
        if isinstance(payload, dict):
            out[sector_name] = {_normalize_symbol(symbol): _clean_text(name) or _normalize_symbol(symbol) for symbol, name in payload.items() if _normalize_symbol(symbol)}
        elif isinstance(payload, list):
            out[sector_name] = {_normalize_symbol(symbol): _normalize_symbol(symbol) for symbol in payload if _normalize_symbol(symbol)}
    return out


def _manual_exclude_map(raw: dict[str, Any]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for sector, payload in (raw or {}).items():
        sector_name = normalize_sector_name(sector)
        if not sector_name:
            continue
        values = {_normalize_symbol(item) for item in (payload or []) if _normalize_symbol(item)}
        out[sector_name] = values
    return out


def build_effective_wics_universe(base_sectors: dict[str, dict[str, str]]) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    core_limit = max(3, _safe_int(getattr(SETTINGS, "WICS_CORE_LIMIT", 12), 12))
    dynamic_limit = max(0, _safe_int(getattr(SETTINGS, "WICS_DYNAMIC_LIMIT", 4), 4))
    total_limit = max(core_limit, _safe_int(getattr(SETTINGS, "WICS_TOTAL_LIMIT", 16), 16))
    min_dynamic_score = _safe_float(getattr(SETTINGS, "WICS_DYNAMIC_MIN_SCORE", 0.54), 0.54)
    min_sources = max(1, _safe_int(getattr(SETTINGS, "WICS_DYNAMIC_MIN_SOURCES", 2), 2))

    overrides = _load_overrides()
    manual_includes = _manual_include_map(overrides.get("include") or {})
    manual_excludes = _manual_exclude_map(overrides.get("exclude") or {})
    dynamic_candidates = _merge_dynamic_candidates()
    candidate_lookup = {row["symbol"]: row for row in dynamic_candidates}

    effective: dict[str, dict[str, str]] = {}
    metadata = {
        "generated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
        "core_limit": core_limit,
        "dynamic_limit": dynamic_limit,
        "total_limit": total_limit,
        "min_dynamic_score": min_dynamic_score,
        "min_dynamic_sources": min_sources,
        "sectors": {},
    }

    for raw_sector_name, stocks in (base_sectors or {}).items():
        sector_key = normalize_sector_name(raw_sector_name)
        excluded = manual_excludes.get(sector_key, set())
        base_items = []
        sector_mismatch_excluded = []
        for index, (symbol, name) in enumerate((stocks or {}).items()):
            symbol_text = _normalize_symbol(symbol)
            if not symbol_text or symbol_text in excluded:
                continue
            name_text = _clean_text(name) or symbol_text
            ref = candidate_lookup.get(symbol_text, {})
            ref_sector = normalize_sector_name(ref.get("sector"))
            if ref_sector and ref_sector != sector_key:
                sector_mismatch_excluded.append(
                    {
                        "symbol": symbol_text,
                        "name": name_text,
                        "latest_sector": ref_sector,
                    }
                )
                continue
            base_score = (
                (_safe_float(ref.get("card_score")) * 0.45)
                + (_safe_float(ref.get("factor_score")) * 0.35)
                + (min(1.0, _safe_int(ref.get("active_source_count")) / 6.0) * 0.10)
                + (_safe_float(ref.get("sector_leader_rank")) * 0.10)
                - _preferred_share_penalty(name_text)
            )
            base_items.append((symbol_text, name_text, round(base_score, 4), index, ref_sector))
        base_items.sort(key=lambda item: (item[2], -item[3]), reverse=True)
        core_items = [(symbol, name) for symbol, name, _, _, _ in base_items[:core_limit]]
        chosen: dict[str, str] = {symbol: name for symbol, name in core_items if symbol}

        include_items = manual_includes.get(sector_key, {})
        manual_added = []
        for symbol, name in include_items.items():
            if symbol and symbol not in chosen and len(chosen) < total_limit:
                chosen[symbol] = name
                manual_added.append({"symbol": symbol, "name": name, "reason": "manual_include"})

        dynamic_rows = []
        for row in dynamic_candidates:
            if row.get("sector") != sector_key:
                continue
            if row["symbol"] in chosen or row["symbol"] in excluded:
                continue
            if int(row.get("evidence_count", 0) or 0) < 2:
                continue
            if row["dynamic_score"] < min_dynamic_score:
                continue
            if row["active_source_count"] < min_sources and row["factor_score"] < (min_dynamic_score + 0.05):
                continue
            dynamic_rows.append(row)
        dynamic_rows.sort(
            key=lambda item: (
                item.get("dynamic_score", 0),
                item.get("card_score", 0),
                item.get("factor_score", 0),
                item.get("active_source_count", 0),
            ),
            reverse=True,
        )

        dynamic_added = []
        for row in dynamic_rows[:dynamic_limit]:
            if len(chosen) >= total_limit:
                break
            chosen[row["symbol"]] = row["name"]
            dynamic_added.append(
                {
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "dynamic_score": row["dynamic_score"],
                    "dynamic_score_raw": row.get("dynamic_score_raw", row["dynamic_score"]),
                    "agreement_confidence": row.get("agreement_confidence", 0.0),
                    "stability_confidence": row.get("stability_confidence", 0.0),
                    "consensus_confidence": row.get("consensus_confidence", 0.0),
                    "card_score": row["card_score"],
                    "factor_score": row["factor_score"],
                    "active_source_count": row["active_source_count"],
                    "evidence_count": row.get("evidence_count", 0),
                }
            )

        effective[raw_sector_name] = chosen
        metadata["sectors"][raw_sector_name] = {
            "normalized_sector": sector_key,
            "core_symbols": [{"symbol": symbol, "name": name} for symbol, name in core_items],
            "core_review": [
                {"symbol": symbol, "name": name, "priority_score": score, "latest_sector": ref_sector or sector_key}
                for symbol, name, score, _, ref_sector in base_items[: min(len(base_items), core_limit + 3)]
            ],
            "manual_added": manual_added,
            "dynamic_added": dynamic_added,
            "excluded": sorted(excluded),
            "sector_mismatch_excluded": sector_mismatch_excluded,
            "final_symbols": [{"symbol": symbol, "name": name} for symbol, name in chosen.items()],
            "final_count": len(chosen),
        }

    metadata["summary"] = summarize_universe_changes(metadata)
    return effective, metadata


def _extract_final_symbols(info: dict[str, Any]) -> list[dict[str, str]]:
    rows = info.get("final_symbols") or []
    if rows:
        return [
            {"symbol": _normalize_symbol((row or {}).get("symbol")), "name": _clean_text((row or {}).get("name"))}
            for row in rows
            if _normalize_symbol((row or {}).get("symbol"))
        ]

    merged: list[dict[str, str]] = []
    seen: set[str] = set()
    for key in ("core_symbols", "manual_added", "dynamic_added"):
        for row in info.get(key) or []:
            symbol = _normalize_symbol((row or {}).get("symbol"))
            if not symbol or symbol in seen:
                continue
            merged.append({"symbol": symbol, "name": _clean_text((row or {}).get("name")) or symbol})
            seen.add(symbol)
    return merged


def _build_universe_turnover(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    prev_sectors = (previous or {}).get("sectors") or {}
    curr_sectors = (current or {}).get("sectors") or {}

    prev_by_sector: dict[str, dict[str, str]] = {}
    curr_by_sector: dict[str, dict[str, str]] = {}

    for raw_sector, info in prev_sectors.items():
        sector = normalize_sector_name((info or {}).get("normalized_sector") or raw_sector)
        if not sector:
            continue
        prev_by_sector[sector] = {
            row["symbol"]: row.get("name") or row["symbol"]
            for row in _extract_final_symbols(info or {})
            if row.get("symbol")
        }

    for raw_sector, info in curr_sectors.items():
        sector = normalize_sector_name((info or {}).get("normalized_sector") or raw_sector)
        if not sector:
            continue
        curr_by_sector[sector] = {
            row["symbol"]: row.get("name") or row["symbol"]
            for row in _extract_final_symbols(info or {})
            if row.get("symbol")
        }

    added_rows: list[dict[str, Any]] = []
    removed_rows: list[dict[str, Any]] = []
    added_count = 0
    removed_count = 0
    per_sector: dict[str, dict[str, Any]] = {}

    for sector in sorted(set(prev_by_sector) | set(curr_by_sector)):
        prev_map = prev_by_sector.get(sector, {})
        curr_map = curr_by_sector.get(sector, {})
        added_symbols = [symbol for symbol in curr_map if symbol not in prev_map]
        removed_symbols = [symbol for symbol in prev_map if symbol not in curr_map]
        per_sector[sector] = {
            "added": [{"symbol": symbol, "name": curr_map[symbol]} for symbol in added_symbols],
            "removed": [{"symbol": symbol, "name": prev_map[symbol]} for symbol in removed_symbols],
        }
        if added_symbols:
            added_count += len(added_symbols)
            added_rows.append(
                {
                    "sector": sector,
                    "symbols": added_symbols,
                    "names": [curr_map[symbol] for symbol in added_symbols[:3]],
                }
            )
        if removed_symbols:
            removed_count += len(removed_symbols)
            removed_rows.append(
                {
                    "sector": sector,
                    "symbols": removed_symbols,
                    "names": [prev_map[symbol] for symbol in removed_symbols[:3]],
                }
            )

    changed_sector_count = sum(
        1
        for sector in sorted(set(prev_by_sector) | set(curr_by_sector))
        if set(prev_by_sector.get(sector, {})) != set(curr_by_sector.get(sector, {}))
    )
    return {
        "added_symbol_count": added_count,
        "removed_symbol_count": removed_count,
        "changed_sector_count": changed_sector_count,
        "added_sectors": added_rows[:4],
        "removed_sectors": removed_rows[:4],
        "per_sector": per_sector,
    }


def _extract_sector_symbol_sets(payload: dict[str, Any]) -> dict[str, set[str]]:
    sectors = (payload or {}).get("sectors") or {}
    out: dict[str, set[str]] = {}
    for raw_sector, info in sectors.items():
        sector = normalize_sector_name((info or {}).get("normalized_sector") or raw_sector)
        if not sector:
            continue
        symbols = {
            _normalize_symbol((row or {}).get("symbol"))
            for row in _extract_final_symbols(info or {})
            if _normalize_symbol((row or {}).get("symbol"))
        }
        out[sector] = symbols
    return out


def _load_universe_history(limit: int = 6) -> list[dict[str, Any]]:
    paths = _list_universe_archives()
    day_latest: dict[str, str] = {}
    ordered_days: list[str] = []
    for path in paths:
        day_key = _archive_day_key(path)
        if not day_key:
            continue
        if day_key not in day_latest:
            ordered_days.append(day_key)
        day_latest[day_key] = path
    paths = [day_latest[day_key] for day_key in ordered_days]
    if limit > 0:
        paths = paths[-limit:]
    history: list[dict[str, Any]] = []
    for path in paths:
        payload = _read_json(path)
        if payload:
            history.append(payload)
    return history


def _historical_overlap(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    union = len(a | b)
    if union <= 0:
        return 0.0
    return round(len(a & b) / union, 4)


def _history_confidence_label(sample_count: int) -> str:
    if sample_count >= 3:
        return "충분"
    if sample_count >= 1:
        return "예비"
    return "없음"


def _build_historical_sector_churn(history: list[dict[str, Any]], current: dict[str, Any]) -> dict[str, dict[str, Any]]:
    snapshots = list(history or []) + [current or {}]
    sector_series = [_extract_sector_symbol_sets(payload) for payload in snapshots if payload]
    if len(sector_series) <= 1:
        return {}

    sectors = sorted({sector for series in sector_series for sector in series})
    out: dict[str, dict[str, Any]] = {}
    for sector in sectors:
        overlaps: list[float] = []
        change_events = 0
        for prev, nxt in zip(sector_series, sector_series[1:]):
            prev_set = prev.get(sector, set())
            next_set = nxt.get(sector, set())
            overlap = _historical_overlap(prev_set, next_set)
            overlaps.append(overlap)
            if prev_set != next_set:
                change_events += 1
        avg_overlap = round(sum(overlaps) / len(overlaps), 3) if overlaps else 1.0
        sample_count = len(overlaps)
        confidence_label = _history_confidence_label(sample_count)
        if confidence_label != "충분":
            label = "예비"
            reason = "히스토리 표본이 아직 얕아 안정도는 예비 판정으로만 보는 편이 낫습니다."
        elif change_events == 0 and avg_overlap >= 0.85:
            label = "안정형"
            reason = "최근 여러 회차 동안 구성 변화가 거의 없어 바스켓 일관성이 높습니다."
        elif avg_overlap >= 0.65 and change_events <= max(1, sample_count // 2):
            label = "유동형"
            reason = "최근 구성 변화는 있었지만 바스켓의 중심축은 유지되는 편입니다."
        else:
            label = "재점검"
            reason = "최근 여러 회차에서 편출입이 반복돼 바스켓 안정성이 낮은 편입니다."
        out[sector] = {
            "label": label,
            "reason": reason,
            "avg_overlap": avg_overlap,
            "change_events": change_events,
            "sample_count": sample_count,
            "confidence_label": confidence_label,
        }
    return out


def _build_sector_status(info: dict[str, Any], turnover_sector: dict[str, Any], *, core_limit: int) -> dict[str, Any]:
    dynamic_count = len(info.get("dynamic_added") or [])
    mismatch_count = len(info.get("sector_mismatch_excluded") or [])
    final_count = _safe_int(info.get("final_count"))
    added_count = len((turnover_sector or {}).get("added") or [])
    removed_count = len((turnover_sector or {}).get("removed") or [])
    turnover_count = added_count + removed_count
    thin_threshold = max(4, core_limit - 1)

    if final_count < thin_threshold or mismatch_count >= 2 or turnover_count >= 3:
        label = "재점검"
        if mismatch_count >= 2:
            reason = "섹터 불일치 제외가 많아 바스켓 자체를 다시 보는 편이 낫습니다."
        elif turnover_count >= 3:
            reason = "전회 대비 편출입이 많아 아직 섹터 대표 구성이 흔들리는 구간입니다."
        else:
            reason = "유효 종목 수가 얇아 섹터보다 대표주 위주 해석이 더 안전합니다."
    elif dynamic_count > 0 or mismatch_count > 0 or turnover_count > 0:
        label = "유동형"
        if dynamic_count > 0:
            reason = "최근 강한 종목이 추가 편입된 적응형 바스켓이라 대장주 확인이 중요합니다."
        elif turnover_count > 0:
            reason = "전회 대비 일부 편출입이 있어 섹터 전체보다 현재 선두 종목을 먼저 보는 편이 낫습니다."
        else:
            reason = "경미한 섹터 정리는 있었지만 전체 틀은 유지되는 편입니다."
    else:
        label = "안정형"
        reason = "코어 구성 변화가 작아 섹터 단위 해석의 신뢰가 비교적 높은 편입니다."

    return {
        "label": label,
        "reason": reason,
        "dynamic_count": dynamic_count,
        "mismatch_count": mismatch_count,
        "turnover_count": turnover_count,
        "added_count": added_count,
        "removed_count": removed_count,
    }


def save_effective_wics_universe(metadata: dict[str, Any]) -> dict[str, Any]:
    os.makedirs(REPORT_DIR, exist_ok=True)
    enriched = dict(metadata or {})
    enriched["sectors"] = dict((metadata or {}).get("sectors") or {})
    enriched["summary"] = dict((metadata or {}).get("summary") or {})
    previous = _read_json(LATEST_UNIVERSE_PATH)
    history = _load_universe_history(limit=max(3, _safe_int(getattr(SETTINGS, "WICS_UNIVERSE_HISTORY_LIMIT", 6), 6)))
    current_day_key = _payload_day_key(enriched)
    history = [payload for payload in history if _payload_day_key(payload) != current_day_key]
    history_day_count = len(history) + 1
    turnover = _build_universe_turnover(previous, enriched)
    historical = _build_historical_sector_churn(history, enriched)
    core_limit = _safe_int(enriched.get("core_limit"), 6)
    for raw_sector, info in (enriched.get("sectors") or {}).items():
        sector = normalize_sector_name((info or {}).get("normalized_sector") or raw_sector)
        sector_turnover = (turnover.get("per_sector") or {}).get(sector, {})
        hist = historical.get(sector, {})
        info["turnover_added"] = sector_turnover.get("added") or []
        info["turnover_removed"] = sector_turnover.get("removed") or []
        info["historical_stability"] = hist
        current_status = _build_sector_status(info, sector_turnover, core_limit=core_limit)
        hist_label = hist.get("label", "")
        hist_reason = hist.get("reason", "")
        hist_confidence = hist.get("confidence_label", "없음")
        if hist_confidence == "충분" and hist_label == "재점검" and current_status.get("label") != "재점검":
            current_status["label"] = "재점검"
            current_status["reason"] = hist_reason or current_status.get("reason", "")
        elif hist_confidence == "충분" and hist_label == "유동형" and current_status.get("label") == "안정형":
            current_status["label"] = "유동형"
            current_status["reason"] = hist_reason or current_status.get("reason", "")
        current_status["history_label"] = hist_label or current_status.get("label", "")
        current_status["history_reason"] = hist_reason or current_status.get("reason", "")
        current_status["history_avg_overlap"] = _safe_float(hist.get("avg_overlap"), 1.0)
        current_status["history_change_events"] = _safe_int(hist.get("change_events"))
        current_status["history_sample_count"] = _safe_int(hist.get("sample_count"))
        current_status["history_confidence_label"] = hist_confidence
        info["universe_status"] = current_status

    enriched["summary"] = summarize_universe_changes(enriched, turnover=turnover)
    enriched["summary"]["history_day_count"] = history_day_count

    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(REPORT_DIR, f"wics_effective_universe_{stamp}.json")
    with open(archive_path, "w", encoding="utf-8") as fp:
        json.dump(enriched, fp, ensure_ascii=False, indent=2)
    with open(LATEST_UNIVERSE_PATH, "w", encoding="utf-8") as fp:
        json.dump(enriched, fp, ensure_ascii=False, indent=2)
    return enriched


def load_effective_wics_symbol_map() -> dict[str, str]:
    payload = _read_json(LATEST_UNIVERSE_PATH)
    sectors = payload.get("sectors") or {}
    if not isinstance(sectors, dict):
        return {}
    out: dict[str, str] = {}
    for raw_sector, info in sectors.items():
        sector_name = normalize_sector_name(info.get("normalized_sector") or raw_sector)
        final_rows = []
        for key in ("core_symbols", "manual_added", "dynamic_added"):
            final_rows.extend(info.get(key) or [])
        for row in final_rows:
            symbol = _normalize_symbol((row or {}).get("symbol"))
            if symbol and sector_name and symbol not in out:
                out[symbol] = sector_name
    return out


def load_effective_wics_sector_meta() -> dict[str, dict[str, Any]]:
    payload = _read_json(LATEST_UNIVERSE_PATH)
    sectors = payload.get("sectors") or {}
    if not isinstance(sectors, dict):
        return {}

    out: dict[str, dict[str, Any]] = {}
    for raw_sector, info in sectors.items():
        sector_name = normalize_sector_name((info or {}).get("normalized_sector") or raw_sector)
        if not sector_name:
            continue
        universe_status = (info or {}).get("universe_status") or {}
        dynamic_added = (info or {}).get("dynamic_added") or []
        stability_values = [
            _safe_float(row.get("stability_confidence"), -1.0)
            for row in dynamic_added
            if _safe_float(row.get("stability_confidence"), -1.0) >= 0
        ]
        avg_dynamic_stability = (
            round(sum(stability_values) / len(stability_values), 3)
            if stability_values
            else 0.0
        )
        out[sector_name] = {
            "sector": sector_name,
            "universe_status_label": _clean_text(universe_status.get("label")) or "유동형",
            "universe_status_reason": _clean_text(universe_status.get("reason")),
            "history_confidence_label": _clean_text(universe_status.get("history_confidence_label")) or "없음",
            "history_avg_overlap": _safe_float(universe_status.get("history_avg_overlap"), -1.0),
            "history_sample_count": _safe_int(universe_status.get("history_sample_count")),
            "dynamic_count": len(dynamic_added),
            "avg_dynamic_stability": avg_dynamic_stability,
            "final_count": _safe_int((info or {}).get("final_count")),
            "mismatch_count": len((info or {}).get("sector_mismatch_excluded") or []),
        }
    return out


def summarize_universe_changes(metadata: dict[str, Any], turnover: dict[str, Any] | None = None) -> dict[str, Any]:
    sectors = (metadata or {}).get("sectors") or {}
    dynamic_sectors = []
    mismatch_sectors = []
    thin_sectors = []
    stable_sectors = []
    adaptive_sectors = []
    review_sectors = []
    dynamic_count = 0
    mismatch_count = 0
    dynamic_stability_values: list[float] = []
    history_avg_overlap_values: list[float] = []
    history_sample_counts: list[int] = []
    history_ready_count = 0
    history_preliminary_count = 0
    for raw_sector, info in sectors.items():
        dynamic_added = info.get("dynamic_added") or []
        mismatch_rows = info.get("sector_mismatch_excluded") or []
        final_count = _safe_int(info.get("final_count"))
        status = ((info.get("universe_status") or {}).get("label")) or ""
        history_avg_overlap = _safe_float((info.get("universe_status") or {}).get("history_avg_overlap"), -1.0)
        history_sample_count = _safe_int((info.get("universe_status") or {}).get("history_sample_count"), 0)
        history_confidence = _clean_text((info.get("universe_status") or {}).get("history_confidence_label"))
        if history_avg_overlap >= 0:
            history_avg_overlap_values.append(history_avg_overlap)
        if history_sample_count > 0:
            history_sample_counts.append(history_sample_count)
        if history_confidence == "충분":
            history_ready_count += 1
        elif history_confidence == "예비":
            history_preliminary_count += 1
        if dynamic_added:
            dynamic_count += len(dynamic_added)
            dynamic_stability_values.extend(_safe_float(row.get("stability_confidence"), -1.0) for row in dynamic_added)
            names = [row.get("name") or row.get("symbol") or "-" for row in dynamic_added[:2]]
            dynamic_sectors.append({"sector": raw_sector, "names": names})
        if mismatch_rows:
            mismatch_count += len(mismatch_rows)
            names = [row.get("name") or row.get("symbol") or "-" for row in mismatch_rows[:2]]
            mismatch_sectors.append({"sector": raw_sector, "names": names})
        if final_count < max(4, _safe_int((metadata or {}).get("core_limit"), 6)):
            thin_sectors.append({"sector": raw_sector, "final_count": final_count})
        if status == "안정형":
            stable_sectors.append({"sector": raw_sector, "reason": (info.get("universe_status") or {}).get("reason", "")})
        elif status == "유동형":
            adaptive_sectors.append({"sector": raw_sector, "reason": (info.get("universe_status") or {}).get("reason", "")})
        elif status == "재점검":
            review_sectors.append({"sector": raw_sector, "reason": (info.get("universe_status") or {}).get("reason", "")})

    turnover = turnover or {}
    stable_count = len(stable_sectors)
    adaptive_count = len(adaptive_sectors)
    review_count = len(review_sectors)
    if review_count >= 4:
        universe_regime = "재점검 많음"
    elif adaptive_count >= max(4, stable_count):
        universe_regime = "유동형 혼합"
    else:
        universe_regime = "안정형 우세"
    history_avg_overlap = round(sum(history_avg_overlap_values) / len(history_avg_overlap_values), 3) if history_avg_overlap_values else 1.0
    history_avg_sample_count = round(sum(history_sample_counts) / len(history_sample_counts), 2) if history_sample_counts else 0.0
    filtered_dynamic_stability_values = [value for value in dynamic_stability_values if value >= 0]
    avg_dynamic_stability = (
        round(sum(filtered_dynamic_stability_values) / len(filtered_dynamic_stability_values), 3)
        if filtered_dynamic_stability_values
        else 0.0
    )
    if history_ready_count >= max(6, len(sectors) // 2):
        history_confidence_label = "충분"
    elif history_preliminary_count > 0 or history_sample_counts:
        history_confidence_label = "예비"
    else:
        history_confidence_label = "없음"

    return {
        "dynamic_sector_count": len(dynamic_sectors),
        "dynamic_symbol_count": dynamic_count,
        "avg_dynamic_stability": avg_dynamic_stability,
        "mismatch_sector_count": len(mismatch_sectors),
        "mismatch_symbol_count": mismatch_count,
        "dynamic_sectors": dynamic_sectors[:4],
        "mismatch_sectors": mismatch_sectors[:4],
        "thin_sectors": thin_sectors[:4],
        "stable_sector_count": stable_count,
        "adaptive_sector_count": adaptive_count,
        "review_sector_count": review_count,
        "stable_sectors": stable_sectors[:4],
        "adaptive_sectors": adaptive_sectors[:4],
        "review_sectors": review_sectors[:4],
        "universe_regime": universe_regime,
        "history_avg_overlap": history_avg_overlap,
        "history_avg_sample_count": history_avg_sample_count,
        "history_ready_sector_count": history_ready_count,
        "history_preliminary_sector_count": history_preliminary_count,
        "history_confidence_label": history_confidence_label,
        "turnover": turnover,
        "turnover_added_symbol_count": turnover.get("added_symbol_count", 0),
        "turnover_removed_symbol_count": turnover.get("removed_symbol_count", 0),
        "turnover_changed_sector_count": turnover.get("changed_sector_count", 0),
    }
