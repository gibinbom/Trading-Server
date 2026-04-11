from __future__ import annotations

import csv
import json
import os
import re
from typing import Any


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONTEXT_PATHS = {
    "macro": os.path.join(ROOT_DIR, "signals", "reports", "macro_news_report_latest.json"),
    "wics": os.path.join(ROOT_DIR, "signals", "reports", "wics_ai_report_latest.json"),
    "factor_snapshot_csv": os.path.join(ROOT_DIR, "factors", "snapshots", "factor_snapshot_latest.csv"),
    "card_csv": os.path.join(ROOT_DIR, "cards", "stock_cards_latest.csv"),
}

_SECTOR_ALIASES = {
    "엔터테인먼트/게임": "엔터테인먼트/미디어",
    "게임엔터테인먼트": "엔터테인먼트/미디어",
    "항공우주와방위산업": "방위산업/우주항공",
    "우주항공과국방": "방위산업/우주항공",
    "통신장비": "통신장비/네트워크",
    "자동차부품": "자동차부품/타이어",
    "전기유틸리티": "전력/유틸리티",
    "해운사": "운송/해운/항공",
}

_SYMBOL_SECTOR_CACHE: dict[str, Any] = {"mtimes": {}, "map": {}}


def _load_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def canonical_sector_name(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    text = re.sub(r"^\d+\.\s*", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if text in _SECTOR_ALIASES:
        return _SECTOR_ALIASES[text]

    if "반도체" in text:
        return "IT하드웨어 (반도체)"
    if "디스플레이" in text or "IT부품" in text:
        return "디스플레이/IT부품"
    if any(token in text for token in ("방위", "우주항공", "항공우주", "국방")):
        return "방위산업/우주항공"
    if any(token in text for token in ("엔터테인먼트", "미디어", "게임")):
        return "엔터테인먼트/미디어"
    if any(token in text for token in ("금융지주", "은행")):
        return "금융지주/은행"
    if "보험" in text:
        return "생명보험"
    if any(token in text for token in ("조선", "해양")):
        return "조선/해양"
    if any(token in text for token in ("기계", "공작기계")):
        return "기계/공작기계"
    if any(token in text for token in ("2차전지", "배터리")):
        return "2차전지/배터리"
    if any(token in text for token in ("건설", "건자재")):
        return "건설/건자재"
    if any(token in text for token in ("유통", "백화점")):
        return "유통/백화점"
    if any(token in text for token in ("통신장비", "네트워크")):
        return "통신장비/네트워크"
    if any(token in text for token in ("헬스케어", "의료기기")):
        return "헬스케어/의료기기"
    if any(token in text for token in ("제약", "바이오")):
        return "제약/바이오 (대형)"
    if any(token in text for token in ("전력", "유틸리티")):
        return "전력/유틸리티"
    if any(token in text for token in ("무역", "판매업체", "상사")):
        return "무역회사와판매업체"
    if any(token in text for token in ("자동차부품", "타이어")):
        return "자동차부품/타이어"
    if any(token in text for token in ("자동차", "완성차")):
        return "자동차/완성차"
    return text


def alignment_label(score: int) -> str:
    if score >= 3:
        return "강한 정렬"
    if score >= 1:
        return "우호 정렬"
    if score <= -3:
        return "강한 역행"
    if score <= -1:
        return "주의"
    return "중립"


def _build_wics_history_meta(wics_summary: dict[str, Any]) -> dict[str, Any]:
    universe_summary = (wics_summary or {}).get("universe_summary") or {}
    history_label = _clean_text(universe_summary.get("history_confidence_label")) or ("없음" if wics_summary else "")
    day_count = 0
    try:
        day_count = int(float(universe_summary.get("history_day_count") or 0))
    except Exception:
        day_count = 0
    regime = _clean_text(universe_summary.get("universe_regime")) or ""

    penalty = 0
    note = ""
    if wics_summary:
        if history_label == "없음":
            penalty = 8
            note = "WICS 히스토리 표본이 거의 없어 섹터 해석은 보조적으로만 보는 편이 낫습니다."
        elif history_label == "예비":
            penalty = 4
            note = "WICS 히스토리 표본이 아직 얕아 섹터 해석은 한 단계 보수적으로 보는 편이 낫습니다."

    effective_day_count = max(day_count, 1) if wics_summary else 0
    return {
        "history_confidence_label": history_label or "없음",
        "history_day_count": effective_day_count,
        "universe_regime": regime or "-",
        "confidence_penalty": penalty,
        "confidence_note": note,
    }


def load_latest_context_summaries() -> tuple[dict[str, Any], dict[str, Any]]:
    return _load_json(CONTEXT_PATHS["macro"]), _load_json(CONTEXT_PATHS["wics"])


def build_context_alignment(
    macro_summary: dict[str, Any] | None = None,
    wics_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    macro_summary = macro_summary or {}
    wics_summary = wics_summary or {}
    wics_meta = _build_wics_history_meta(wics_summary)
    by_sector: dict[str, dict[str, Any]] = {}

    def _ensure(sector: Any) -> dict[str, Any] | None:
        key = canonical_sector_name(sector)
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

    def _add_support(sector: Any, source: str, weight: int = 1) -> None:
        item = _ensure(sector)
        if item is None:
            return
        item["score"] += int(weight)
        if source not in item["support_sources"]:
            item["support_sources"].append(source)

    def _add_risk(sector: Any, source: str, weight: int = 1) -> None:
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

    rows: list[dict[str, Any]] = []
    for item in by_sector.values():
        item["label"] = alignment_label(int(item.get("score", 0)))
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

    macro_conf = int(float(macro_summary.get("confidence_score", 0) or 0))
    wics_conf = int(float(wics_summary.get("confidence_score", 0) or 0))
    confidence_score = max(macro_conf, wics_conf)
    if wics_summary and int(wics_meta.get("confidence_penalty", 0) or 0) > 0:
        confidence_score = max(0, confidence_score - int(wics_meta.get("confidence_penalty", 0) or 0))
    return {
        "by_sector": by_sector,
        "top_support": [row for row in rows if int(row.get("score", 0)) > 0][:4],
        "top_risk": [row for row in rows if int(row.get("score", 0)) < 0][:3],
        "market_mode": _clean_text(macro_summary.get("market_mode")) or _clean_text(wics_summary.get("market_mode")) or "중립",
        "confidence_score": confidence_score,
        "sources": {
            "macro": bool(macro_summary),
            "wics": bool(wics_summary),
        },
        "wics_history_confidence_label": wics_meta.get("history_confidence_label") or "없음",
        "wics_history_day_count": int(wics_meta.get("history_day_count", 0) or 0),
        "wics_universe_regime": wics_meta.get("universe_regime") or "-",
        "wics_confidence_note": wics_meta.get("confidence_note") or "",
    }


def load_latest_context_alignment() -> dict[str, Any]:
    macro_summary, wics_summary = load_latest_context_summaries()
    return build_context_alignment(macro_summary, wics_summary)


def _csv_mtime(path: str) -> float:
    try:
        return float(os.path.getmtime(path))
    except Exception:
        return -1.0


def load_latest_symbol_sector_map() -> dict[str, str]:
    paths = [CONTEXT_PATHS["factor_snapshot_csv"], CONTEXT_PATHS["card_csv"]]
    current_mtimes = {path: _csv_mtime(path) for path in paths}
    if _SYMBOL_SECTOR_CACHE.get("map") and _SYMBOL_SECTOR_CACHE.get("mtimes") == current_mtimes:
        return dict(_SYMBOL_SECTOR_CACHE.get("map") or {})

    symbol_map: dict[str, str] = {}
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as fp:
                reader = csv.DictReader(fp)
                for row in reader:
                    symbol = "".join(ch for ch in str((row or {}).get("symbol") or "") if ch.isdigit()).zfill(6)
                    sector = canonical_sector_name((row or {}).get("sector"))
                    if symbol and sector and symbol not in symbol_map:
                        symbol_map[symbol] = sector
        except Exception:
            continue

    _SYMBOL_SECTOR_CACHE["mtimes"] = current_mtimes
    _SYMBOL_SECTOR_CACHE["map"] = dict(symbol_map)
    return symbol_map


def get_symbol_trade_context(
    symbol: Any,
    *,
    sector: Any = None,
    alignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    symbol_text = "".join(ch for ch in str(symbol or "") if ch.isdigit()).zfill(6)
    symbol_sector_map = load_latest_symbol_sector_map()
    resolved_sector = canonical_sector_name(sector) or canonical_sector_name(symbol_sector_map.get(symbol_text))
    context = alignment or load_latest_context_alignment()
    by_sector = context.get("by_sector", {}) if isinstance(context, dict) else {}
    matched = by_sector.get(resolved_sector, {}) if resolved_sector else {}
    support_sources = list(matched.get("support_sources", []))
    risk_sources = list(matched.get("risk_sources", []))
    note_sources = support_sources[:2] if support_sources else risk_sources[:2]
    return {
        "symbol": symbol_text,
        "sector": resolved_sector or "",
        "alignment_score": int(matched.get("score", 0) or 0),
        "alignment_label": matched.get("label", "중립"),
        "market_mode": context.get("market_mode", "중립") if isinstance(context, dict) else "중립",
        "confidence_score": int((context or {}).get("confidence_score", 0) or 0),
        "wics_history_confidence_label": _clean_text((context or {}).get("wics_history_confidence_label")) or "없음",
        "wics_history_day_count": int((context or {}).get("wics_history_day_count", 0) or 0),
        "wics_universe_regime": _clean_text((context or {}).get("wics_universe_regime")) or "-",
        "wics_confidence_note": _clean_text((context or {}).get("wics_confidence_note")) or "",
        "support_sources": support_sources,
        "risk_sources": risk_sources,
        "note": ", ".join(note_sources),
    }


def decorate_items_with_alignment(
    items: list[dict[str, Any]],
    alignment: dict[str, Any] | None = None,
    *,
    sector_key: str = "sector",
) -> list[dict[str, Any]]:
    context = alignment or load_latest_context_alignment()
    by_sector = context.get("by_sector", {}) if isinstance(context, dict) else {}
    decorated: list[dict[str, Any]] = []
    for row in items or []:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        key = canonical_sector_name(item.get(sector_key))
        match = by_sector.get(key, {})
        item["alignment_score"] = int(match.get("score", 0))
        item["alignment_label"] = match.get("label", "중립")
        item["alignment_support"] = list(match.get("support_sources", []))
        item["alignment_risk"] = list(match.get("risk_sources", []))
        item["wics_history_confidence_label"] = _clean_text((context or {}).get("wics_history_confidence_label")) or "없음"
        item["wics_history_day_count"] = int((context or {}).get("wics_history_day_count", 0) or 0)
        item["wics_universe_regime"] = _clean_text((context or {}).get("wics_universe_regime")) or "-"
        item["wics_confidence_note"] = _clean_text((context or {}).get("wics_confidence_note")) or ""
        decorated.append(item)
    return decorated
