from __future__ import annotations

import json
import logging
import math
import os
import re
import time
import glob
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd

try:
    from broker_kis import KISBroker
    from config import SETTINGS
    from context_alignment import decorate_items_with_alignment, load_latest_context_alignment
    from investor_flow_provider import fetch_recent_investor_days
    from sector_thesis import load_latest_sector_thesis
    from kis_broker_factory import build_kis_broker_from_settings
    from dynamic_factor_weighting import compute_dynamic_factor_weights
    from naver_price_fallback import fetch_naver_daily_price_history
except Exception:
    from Disclosure.broker_kis import KISBroker
    from Disclosure.config import SETTINGS
    from Disclosure.context_alignment import decorate_items_with_alignment, load_latest_context_alignment
    from Disclosure.investor_flow_provider import fetch_recent_investor_days
    from Disclosure.sector_thesis import load_latest_sector_thesis
    from Disclosure.kis_broker_factory import build_kis_broker_from_settings
    from Disclosure.dynamic_factor_weighting import compute_dynamic_factor_weights
    from Disclosure.naver_price_fallback import fetch_naver_daily_price_history

try:
    import FinanceDataReader as fdr
except Exception:
    fdr = None

try:
    from pykrx import stock as pykrx_stock
except Exception:
    pykrx_stock = None

try:
    from consensus_repo import MongoConsensusRepo
except Exception:
    MongoConsensusRepo = None

log = logging.getLogger("disclosure.factor")

DEFAULT_MARKETS = ("KOSPI", "KOSDAQ")
DEFAULT_EXCLUDED_NAME_KEYWORDS = ("ETF", "ETN", "리츠", "스팩", "SPAC")
DEFAULT_EXCLUDED_EXACT_SUFFIXES = ("우", "우B", "우C", "1우", "2우", "3우")
DEFAULT_EXCLUDED_CONSTRUCTION_KEYWORDS = ("건설", "토건", "토목", "건축", "건설업", "건설기계")


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)

    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"nan", "none", "null", "n/a", "-"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _safe_int(value: Any) -> int:
    parsed = _safe_float(value)
    if parsed is None:
        return 0
    return int(parsed)


def _pick_consensus_value(
    fy1_value: Optional[float],
    fy0_value: Optional[float],
    actual_value: Optional[float],
    annualized_quarter_value: Optional[float],
) -> tuple[Optional[float], str, str]:
    if fy1_value not in (None, 0):
        return fy1_value, "FY1", "annual_consensus"
    if fy0_value not in (None, 0):
        return fy0_value, "FY0", "annual_consensus"
    if actual_value not in (None, 0):
        return actual_value, "실제 실적", "actual_annual"
    if annualized_quarter_value not in (None, 0):
        return annualized_quarter_value, "연환산 분기", "quarter_annualized"
    return None, "", ""


def _decorate_with_sector_thesis(items: list[dict[str, Any]], sector_thesis: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    thesis_map = ((sector_thesis or {}).get("by_sector") or {}) if isinstance(sector_thesis, dict) else {}
    if not thesis_map:
        return items
    decorated: list[dict[str, Any]] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        sector = str(item.get("sector") or "")
        thesis = thesis_map.get(sector) or {}
        if thesis:
            item["sector_final_label"] = str(thesis.get("final_label") or "")
            item["sector_action_hint"] = str(thesis.get("action_hint") or "")
            item["sector_human_summary"] = str(thesis.get("human_summary") or "")
            item["flow_lens_score"] = round(float(thesis.get("flow_lens_score") or 0.0), 2)
            item["quant_lens_score"] = round(float(thesis.get("quant_lens_score") or 0.0), 2)
            item["macro_lens_score"] = round(float(thesis.get("macro_lens_score") or 0.0), 2)
        decorated.append(item)
    return decorated


def _contains_any_keyword(series: pd.Series, keywords: Iterable[str]) -> pd.Series:
    if series.empty:
        return pd.Series(False, index=series.index)
    keyword_list = [str(keyword).strip() for keyword in keywords if str(keyword).strip()]
    if not keyword_list:
        return pd.Series(False, index=series.index)
    pattern = "|".join(re.escape(keyword) for keyword in keyword_list)
    return series.astype(str).str.contains(pattern, na=False, regex=True)


def _normalize_market_label(value: Any, market_id: Any = None) -> Optional[str]:
    text = str(value or "").strip().upper()
    market_id_text = str(market_id or "").strip().upper()

    if text.startswith("KOSPI") or market_id_text == "STK":
        return "KOSPI"
    if text.startswith("KOSDAQ") or market_id_text == "KSQ":
        return "KOSDAQ"
    if text.startswith("KONEX") or market_id_text == "KNX":
        return "KONEX"
    return text or None


def _normalize_markets(markets: Optional[Iterable[str]]) -> tuple[str, ...]:
    if not markets:
        return DEFAULT_MARKETS

    normalized = []
    for market in markets:
        value = _normalize_market_label(market)
        if value and value not in normalized:
            normalized.append(value)
    return tuple(normalized or DEFAULT_MARKETS)


def _load_wics26_symbol_map() -> Dict[str, str]:
    try:
        from signals.wics_monitor import WICS_26_SECTORS
    except Exception:
        try:
            from Disclosure.signals.wics_monitor import WICS_26_SECTORS
        except Exception:
            return {}

    symbol_to_sector: Dict[str, str] = {}
    for sector_name, stocks in (WICS_26_SECTORS or {}).items():
        normalized_sector = str(sector_name).split(". ", 1)[-1].strip()
        for symbol in (stocks or {}).keys():
            symbol_to_sector[str(symbol).zfill(6)] = normalized_sector
    return symbol_to_sector


def _is_excluded_name(name: Any) -> bool:
    if pd.isna(name):
        return True
    text = str(name).strip()
    if not text or "관리" in text:
        return True
    if any(keyword in text for keyword in DEFAULT_EXCLUDED_NAME_KEYWORDS):
        return True
    return any(text.endswith(suffix) for suffix in DEFAULT_EXCLUDED_EXACT_SUFFIXES)


def _pct_rank(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    valid = series.astype(float)
    if higher_is_better:
        return valid.rank(pct=True, ascending=True)
    return valid.rank(pct=True, ascending=False)


def _group_zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(0.0, index=series.index)
    return (series - series.mean()) / std


def _calc_return(close: pd.Series, lookback: int) -> Optional[float]:
    if len(close) <= lookback:
        return None
    base = close.iloc[-lookback - 1]
    if base == 0 or pd.isna(base):
        return None
    return float(close.iloc[-1] / base - 1.0)


def _calc_ma_gap(close: pd.Series, window: int) -> Optional[float]:
    if len(close) < window:
        return None
    ma = close.tail(window).mean()
    if ma == 0 or pd.isna(ma):
        return None
    return float(close.iloc[-1] / ma - 1.0)


def _calc_drawdown(close: pd.Series, window: int) -> Optional[float]:
    if len(close) < 2:
        return None
    ref = close.tail(window) if len(close) >= window else close
    peak = ref.max()
    if peak == 0 or pd.isna(peak):
        return None
    return float(close.iloc[-1] / peak - 1.0)


def _top_factor_mix_pairs(item: Dict[str, Any], limit: int = 3) -> list[tuple[str, float]]:
    factor_pairs = [
        ("value", float(item.get("value_score", 0) or 0)),
        ("momentum", float(item.get("momentum_score", 0) or 0)),
        ("quality", float(item.get("quality_score", 0) or 0)),
        ("flow", float(item.get("flow_score", 0) or 0)),
        ("news", float(item.get("news_score", 0) or 0)),
    ]
    return [pair for pair in sorted(factor_pairs, key=lambda x: x[1], reverse=True)[:limit] if pair[1] > 0]


def _top_factor_mix(item: Dict[str, Any], limit: int = 3) -> str:
    top_pairs = _top_factor_mix_pairs(item, limit=limit)
    return ", ".join(f"{name} {round(score, 3)}" for name, score in top_pairs) or "-"


def _dominant_style(weights: Dict[str, Any]) -> str:
    if not weights:
        return "-"
    ordered = sorted(
        ((key.replace("_score", ""), float(value or 0)) for key, value in weights.items()),
        key=lambda x: x[1],
        reverse=True,
    )
    top = [name for name, value in ordered[:3] if value > 0]
    return ", ".join(top) if top else "-"


def _base_factor_action(item: Dict[str, Any]) -> str:
    composite = float(item.get("composite_score", 0) or 0)
    coverage = float(item.get("factor_source_coverage_ratio", 0) or 0)
    leadership = max(
        float(item.get("momentum_score", 0) or 0),
        float(item.get("flow_score", 0) or 0),
        float(item.get("quality_score", 0) or 0),
    )
    if composite >= 0.72 and coverage >= 0.6 and leadership >= 0.65:
        return "direct"
    if composite >= 0.67:
        return "watch"
    return "hold"


def _build_factor_decision_regime(data_quality: Dict[str, Any]) -> Dict[str, str]:
    grade = str(data_quality.get("coverage_grade") or "")
    weak_sources = list(data_quality.get("weak_sources") or [])
    dynamic_status = str(data_quality.get("dynamic_status") or "unknown")

    if grade == "보수적":
        return {
            "name": "observe_only",
            "description": "데이터 공백이 커 직접 진입보다 관찰과 보수적 해석을 우선합니다.",
        }
    if grade == "중간" or (dynamic_status == "fallback" and weak_sources):
        return {
            "name": "confirm_first",
            "description": "상위 후보는 보되, 바로 추격보다 눌림과 추가 확인을 우선합니다.",
        }
    return {
        "name": "normal",
        "description": "상위 팩터 후보를 기본 체력 기준으로 해석합니다.",
    }


def _apply_factor_regime(action: str, regime_name: str) -> str:
    if regime_name == "confirm_first" and action == "direct":
        return "watch"
    if regime_name == "observe_only":
        if action == "direct":
            return "watch"
        if action == "watch":
            return "hold"
    return action


def _apply_factor_alignment(action: str, alignment_score: int) -> str:
    if alignment_score <= -2:
        if action == "direct":
            return "watch"
        if action == "watch":
            return "hold"
    return action


def _factor_action_label(action: str) -> str:
    return {
        "direct": "직접 후보",
        "watch": "눌림 후보",
        "hold": "보류",
    }.get(str(action or "").lower(), "보류")


def _sector_action_label(avg_score: float, count: int, alignment_score: int = 0) -> str:
    if avg_score >= 0.6 and count >= 3:
        action = "섹터 추적"
    elif avg_score >= 0.56:
        action = "중심주만"
    else:
        action = "보류"
    if alignment_score <= -2:
        if action == "섹터 추적":
            return "중심주만"
        if action == "중심주만":
            return "보류"
    return action


def _reversion_action_label(item: Dict[str, Any]) -> str:
    reversion = float(item.get("sector_reversion_signal", 0) or 0)
    value_score = float(item.get("value_score", 0) or 0)
    if reversion >= 8 and value_score >= 0.6:
        return "눌림 관찰"
    return "보수적 관찰"


def _factor_axes_label(item: Dict[str, Any], limit: int = 2) -> str:
    axes = list(item.get("strong_axes") or [name for name, _ in _top_factor_mix_pairs(item, limit=max(3, limit))])
    axes = [str(axis).strip() for axis in axes if str(axis).strip()]
    if not axes:
        return "복합"
    return "·".join(axes[:limit])


def _factor_hint_reason(item: Dict[str, Any], regime_name: str) -> tuple[str, str]:
    action = str(item.get("action") or "hold")
    axes_label = _factor_axes_label(item)
    coverage = round(float(item.get("factor_source_coverage_ratio", 0) or 0), 3)

    if action == "direct":
        reason = f"{axes_label} 축이 함께 강해 상단 후보로 볼 만합니다."
    elif action == "watch":
        reason = f"{axes_label} 축은 좋지만 확인 후 접근이 더 적절합니다."
    else:
        reason = f"{axes_label} 축은 보이지만 지금은 보수적으로 보는 편이 낫습니다."

    if regime_name == "observe_only":
        note = f"해석 모드가 관찰 중심이라 직접 진입보다 확인이 우선입니다. coverage {coverage}"
    elif regime_name == "confirm_first":
        note = f"상단 후보이지만 눌림과 유지력을 먼저 보는 편이 좋습니다. coverage {coverage}"
    else:
        note = f"현재 팩터 커버리지 기준 coverage {coverage}"

    return reason, note


def _build_factor_action_hints(
    candidates: list[Dict[str, Any]],
    decision_regime: Dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    hints: list[str] = []
    rows: list[dict[str, Any]] = []
    regime_name = str(decision_regime.get("name") or "normal")

    for item in candidates[:4]:
        action_label = str(item.get("action_label") or _factor_action_label(item.get("action") or "hold"))
        reason, note = _factor_hint_reason(item, regime_name)
        alignment = str(item.get("alignment_label") or "중립")
        if int(item.get("alignment_score", 0) or 0) > 0:
            note = f"맥락 {alignment} / {note}"
        elif int(item.get("alignment_score", 0) or 0) < 0:
            note = f"맥락 {alignment} / {note}"
        hint = f"[{action_label}] {item.get('name')}({item.get('symbol')}) | {reason} | {note}"
        hints.append(hint)
        rows.append(
            {
                "symbol": item.get("symbol"),
                "name": item.get("name"),
                "sector": item.get("sector"),
                "action": item.get("action"),
                "action_label": action_label,
                "reason": reason,
                "note": note,
                "strong_axes": list(item.get("strong_axes") or []),
                "alignment_label": alignment,
                "alignment_score": int(item.get("alignment_score", 0) or 0),
            }
        )

    if not hints:
        hints.append("오늘은 강한 팩터 후보보다 관찰 위주가 적절합니다.")
        rows.append(
            {
                "symbol": "",
                "name": "",
                "sector": "",
                "action": "hold",
                "action_label": "보류",
                "reason": "강한 팩터 후보가 제한적입니다.",
                "note": "관찰 위주가 적절합니다.",
                "strong_axes": [],
            }
        )

    return hints[:4], rows[:4]


def normalize_factor_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(summary, dict):
        return {}

    normalized = dict(summary)
    coverage = normalized.get("coverage") or {}
    candidates = [dict(item) for item in (normalized.get("portfolio_candidates") or []) if isinstance(item, dict)]

    weak_sources = list((normalized.get("data_quality") or {}).get("weak_sources") or [])
    if not weak_sources:
        if _safe_float(coverage.get("flow_factor_coverage_pct")) < 40:
            weak_sources.append(f"수급 {round(_safe_float(coverage.get('flow_factor_coverage_pct')), 1)}%")
        if _safe_float(coverage.get("consensus_factor_coverage_pct")) < 30:
            weak_sources.append(f"컨센서스 {round(_safe_float(coverage.get('consensus_factor_coverage_pct')), 1)}%")
        if _safe_float(coverage.get("news_factor_coverage_pct")) < 40:
            weak_sources.append(f"뉴스 {round(_safe_float(coverage.get('news_factor_coverage_pct')), 1)}%")

    data_quality = dict(normalized.get("data_quality") or {})
    if not data_quality:
        coverage_grade = "양호"
        if len(weak_sources) >= 2:
            coverage_grade = "보수적"
        elif weak_sources:
            coverage_grade = "중간"
        data_quality = {
            "coverage_grade": coverage_grade,
            "weak_sources": weak_sources,
            "dynamic_status": ((normalized.get("dynamic_factor_weights") or {}).get("status") or "unknown"),
        }

    warnings = list(data_quality.get("warnings") or [])
    if not warnings:
        warnings.extend(weak_sources)
        if str(data_quality.get("dynamic_status") or "unknown") == "fallback":
            warnings.append("비중 조절 기본값 복귀")
    top_sector_counter: Counter = Counter(item.get("sector") for item in candidates[:6] if item.get("sector"))
    if top_sector_counter and not data_quality.get("top_sector"):
        top_sector_name, top_sector_count = top_sector_counter.most_common(1)[0]
        denom = max(1, min(len(candidates), 6))
        data_quality["top_sector"] = top_sector_name or "-"
        data_quality["top_sector_share"] = round(top_sector_count / denom, 3)
        if top_sector_count >= 3 and f"상위 후보가 {top_sector_name}에 편중" not in warnings:
            warnings.append(f"상위 후보가 {top_sector_name}에 편중")
    data_quality["warnings"] = warnings[:4]

    decision_regime = dict(normalized.get("decision_regime") or {})
    if not decision_regime:
        decision_regime = _build_factor_decision_regime(data_quality)

    context_alignment = normalized.get("context_alignment")
    if not isinstance(context_alignment, dict) or not context_alignment.get("by_sector"):
        context_alignment = load_latest_context_alignment()
    sector_thesis = load_latest_sector_thesis()

    bucket_map = {"direct": [], "watch": [], "hold": []}
    decorated_candidates: list[Dict[str, Any]] = []
    for item in candidates:
        aligned_item = decorate_items_with_alignment([item], context_alignment)[0]
        base_action = _base_factor_action(item)
        action = _apply_factor_regime(base_action, decision_regime.get("name", "normal"))
        action = _apply_factor_alignment(action, int(aligned_item.get("alignment_score", 0) or 0))
        resolved_action = str(item.get("action") or action)
        resolved_action = _apply_factor_alignment(resolved_action, int(aligned_item.get("alignment_score", 0) or 0))
        decorated = {
            **aligned_item,
            "base_action": base_action,
            "action": resolved_action,
            "action_label": _factor_action_label(resolved_action),
            "strong_axes": item.get("strong_axes") or [name for name, _ in _top_factor_mix_pairs(item)],
            "decision_trace": item.get("decision_trace")
            or {
                "base_action": base_action,
                "coverage_ratio": round(float(item.get("factor_source_coverage_ratio", 0) or 0), 4),
                "dominant_axes": [name for name, _ in _top_factor_mix_pairs(item)],
                "composite_score": round(float(item.get("composite_score", 0) or 0), 4),
                "alignment_score": int(aligned_item.get("alignment_score", 0) or 0),
            },
        }
        decorated_candidates.append(decorated)
    bucket_map = {"direct": [], "watch": [], "hold": []}
    for item in decorated_candidates:
        bucket = str(item.get("action") or "hold")
        if bucket not in bucket_map:
            bucket = "hold"
        bucket_map[bucket].append(item)
    decorated_candidates = _decorate_with_sector_thesis(decorated_candidates, sector_thesis)

    normalized["portfolio_candidates"] = decorated_candidates
    normalized["portfolio_buckets"] = normalized.get("portfolio_buckets") or {
        key: rows[:3] for key, rows in bucket_map.items()
    }
    normalized["candidate_actions"] = normalized.get("candidate_actions") or [
        {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "sector": item.get("sector"),
            "action": item.get("action"),
            "action_label": item.get("action_label"),
            "strong_axes": item.get("strong_axes"),
            "alignment_label": item.get("alignment_label", "중립"),
            "alignment_score": int(item.get("alignment_score", 0) or 0),
        }
        for item in decorated_candidates
    ]
    normalized["data_quality"] = data_quality
    normalized["decision_regime"] = decision_regime
    action_hints, action_hint_rows = _build_factor_action_hints(decorated_candidates, decision_regime)
    normalized["action_hints"] = normalized.get("action_hints") or action_hints
    normalized["action_hint_rows"] = normalized.get("action_hint_rows") or action_hint_rows
    normalized["context_alignment"] = {
        "market_mode": context_alignment.get("market_mode", "중립"),
        "confidence_score": int(context_alignment.get("confidence_score", 0) or 0),
        "top_support": (context_alignment.get("top_support") or [])[:4],
        "top_risk": (context_alignment.get("top_risk") or [])[:3],
        "by_sector": context_alignment.get("by_sector", {}),
    }

    patched_top_sectors = []
    for sector in decorate_items_with_alignment(normalized.get("top_sectors", []) or [], context_alignment):
        if not isinstance(sector, dict):
            continue
        patched = dict(sector)
        patched["action"] = _sector_action_label(
            float(patched.get("avg_composite_score", 0) or 0),
            int(patched.get("count", 0) or 0),
            int(patched.get("alignment_score", 0) or 0),
        )
        patched_top_sectors.append(patched)
    if patched_top_sectors:
        normalized["top_sectors"] = _decorate_with_sector_thesis(patched_top_sectors, sector_thesis)

    patched_sector_cards = []
    for sector in decorate_items_with_alignment(normalized.get("sector_cards", []) or [], context_alignment):
        if not isinstance(sector, dict):
            continue
        patched = dict(sector)
        patched["action"] = _sector_action_label(
            float(patched.get("avg_composite_score", 0) or 0),
            int(patched.get("count", 0) or 0),
            int(patched.get("alignment_score", 0) or 0),
        )
        patched_sector_cards.append(patched)
    if patched_sector_cards:
        normalized["sector_cards"] = _decorate_with_sector_thesis(patched_sector_cards, sector_thesis)

    normalized["sector_thesis"] = {
        "top_sectors": list((sector_thesis or {}).get("top_sectors") or [])[:6],
        "market_mode": str((sector_thesis or {}).get("market_mode") or ""),
        "confidence_score": int((sector_thesis or {}).get("confidence_score", 0) or 0),
    }

    return normalized


@dataclass
class FactorPaths:
    root_dir: str
    snapshot_dir: str
    cache_dir: str
    price_cache_dir: str


class FactorSnapshotBuilder:
    def __init__(
        self,
        top_n: int = 300,
        min_marcap_krw: Optional[int] = None,
        markets: Optional[Iterable[str]] = None,
        price_lookback_days: int = 260,
        include_flow: bool = True,
        include_consensus: bool = True,
        include_news: bool = True,
        flow_top_n: Optional[int] = None,
        consensus_top_n: Optional[int] = None,
        news_lookback_days: int = 7,
        price_sleep_sec: float = 0.02,
        flow_sleep_sec: float = 0.05,
        exclude_construction: bool = False,
    ):
        self.top_n = int(top_n)
        default_min_marcap = getattr(SETTINGS, "WATCH_MIN_MARCAP", 0)
        self.min_marcap_krw = int(default_min_marcap if min_marcap_krw is None else min_marcap_krw)
        self.markets = _normalize_markets(markets)
        self.price_lookback_days = int(price_lookback_days)
        self.include_flow = include_flow
        self.include_consensus = include_consensus
        self.include_news = include_news
        self.flow_top_n = None if flow_top_n in (None, 0) else int(flow_top_n)
        self.consensus_top_n = None if consensus_top_n in (None, 0) else int(consensus_top_n)
        self.news_lookback_days = int(max(1, news_lookback_days))
        self.price_sleep_sec = float(price_sleep_sec)
        self.flow_sleep_sec = float(flow_sleep_sec)
        self.exclude_construction = exclude_construction
        self.snapshot_ts = datetime.now()
        self.snapshot_date = self.snapshot_ts.strftime("%Y%m%d")
        self._price_source_warning_logged = False
        self.dynamic_weight_info: Dict[str, Any] = {}
        self._consensus_disabled = False

        root_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "factors")
        snapshot_dir = os.path.join(root_dir, "snapshots")
        cache_dir = os.path.join(root_dir, "cache")
        price_cache_dir = os.path.join(cache_dir, "prices")
        for path in (root_dir, snapshot_dir, cache_dir, price_cache_dir):
            os.makedirs(path, exist_ok=True)
        self.paths = FactorPaths(
            root_dir=root_dir,
            snapshot_dir=snapshot_dir,
            cache_dir=cache_dir,
            price_cache_dir=price_cache_dir,
        )

        self.broker = self._build_broker() if include_flow else None
        self.consensus_repo = self._build_consensus_repo() if include_consensus else None
        self.wics26_symbol_map = _load_wics26_symbol_map()

    def _build_broker(self) -> Optional[KISBroker]:
        try:
            return build_kis_broker_from_settings(is_virtual=False, dry_run=True)
        except Exception as exc:
            log.warning("KIS broker init failed; flow factors disabled: %s", exc)
            return None

    def _build_consensus_repo(self) -> Optional[MongoConsensusRepo]:
        if MongoConsensusRepo is None:
            log.warning("pymongo/consensus_repo unavailable; consensus factors disabled.")
            return None
        try:
            return MongoConsensusRepo(
                mongo_uri=SETTINGS.MONGO_URI,
                db_name=SETTINGS.DB_NAME,
                collection_name=SETTINGS.CONSENSUS_COLLECTION,
            )
        except Exception as exc:
            log.warning("Consensus repo init failed; consensus factors disabled: %s", exc)
            return None

    def _price_cache_path(self, symbol: str) -> str:
        return os.path.join(self.paths.price_cache_dir, f"{symbol}.csv")

    def _fill_sector_fallbacks(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "symbol" not in df.columns:
            return df

        mapped_sector = df["symbol"].astype(str).str.zfill(6).map(self.wics26_symbol_map)
        sector_missing = df["sector"].isna() | (df["sector"].astype(str).str.strip() == "") | (df["sector"] == "Unknown")
        industry_missing = df["industry"].isna() | (df["industry"].astype(str).str.strip() == "") | (df["industry"] == "Unknown")

        df.loc[sector_missing & mapped_sector.notna(), "sector"] = mapped_sector[sector_missing & mapped_sector.notna()]
        df.loc[industry_missing & mapped_sector.notna(), "industry"] = mapped_sector[industry_missing & mapped_sector.notna()]
        unresolved_mask = df["sector"].isna() | (df["sector"].astype(str).str.strip() == "") | (df["sector"] == "Unknown")
        if unresolved_mask.any():
            unresolved_symbols = df.loc[unresolved_mask, "symbol"].astype(str).str.zfill(6).dropna().unique().tolist()
            if unresolved_symbols:
                try:
                    try:
                        from sector_resolver import resolve_sector_map
                    except Exception:
                        from Disclosure.sector_resolver import resolve_sector_map

                    resolved = resolve_sector_map(unresolved_symbols, sleep_sec=0.0)
                    if resolved:
                        mapped = df["symbol"].astype(str).str.zfill(6).map(resolved)
                        df.loc[unresolved_mask & mapped.notna(), "sector"] = mapped[unresolved_mask & mapped.notna()]
                        df.loc[industry_missing & mapped.notna(), "industry"] = mapped[industry_missing & mapped.notna()]
                except Exception as exc:
                    log.debug("sector_resolver fallback skipped: %s", exc)
        df["sector"] = df["sector"].fillna("Unknown").replace("", "Unknown")
        df["industry"] = df["industry"].fillna("Unknown").replace("", "Unknown")
        return df

    def _load_universe_df(self) -> pd.DataFrame:
        try:
            from universe import build_watchlist_by_marcap

            records = build_watchlist_by_marcap(
                min_marcap_krw=self.min_marcap_krw,
                markets=self.markets,
                top_n=None if self.top_n <= 0 else self.top_n,
                exclude_construction=self.exclude_construction,
                return_type="records",
            )
            df = pd.DataFrame(records)
        except Exception as exc:
            log.warning("Primary universe loader unavailable; using cached listing fallback: %s", exc)
            df = self._load_fallback_universe_df()

        if df.empty:
            raise ValueError("Universe is empty; cannot build factor snapshot.")

        rename_map = {
            "code": "symbol",
            "name": "name",
            "marcap": "marcap",
            "market": "market",
            "sector": "sector",
            "industry": "industry",
        }
        df = df.rename(columns=rename_map)
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        if "market" in df.columns:
            df["market"] = [
                _normalize_market_label(market, None) for market in df["market"]
            ]
        df = self._fill_sector_fallbacks(df)
        df["marcap"] = pd.to_numeric(df["marcap"], errors="coerce")
        if "market" in df.columns and self.markets:
            df = df[df["market"].isin(self.markets)].copy()
        df = df.sort_values("marcap", ascending=False).reset_index(drop=True)
        return df

    def _load_fallback_universe_df(self) -> pd.DataFrame:
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        fallback_candidates = []
        fallback_candidates.extend(sorted(glob.glob(os.path.join(repo_root, "krx_universe_merged_*.csv")), reverse=True))
        fallback_candidates.extend(sorted(glob.glob(os.path.join(repo_root, "krx_universe_*.csv")), reverse=True))
        fallback_candidates.append(os.path.join(repo_root, "krx_listing.csv"))

        for file_path in fallback_candidates:
            if not file_path or not os.path.exists(file_path):
                continue

            try:
                df = pd.read_csv(file_path, dtype={"Code": str, "Symbol": str})
            except Exception as exc:
                log.warning("fallback universe read failed (%s): %s", file_path, exc)
                continue

            if df.empty:
                continue

            if "Symbol" in df.columns and "Code" not in df.columns:
                df = df.rename(columns={"Symbol": "Code"})

            for missing_col in ("Code", "Name", "Marcap", "Market", "Sector", "Industry"):
                if missing_col not in df.columns:
                    df[missing_col] = None

            df["Code"] = df["Code"].astype(str).str.zfill(6)
            df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
            df = df[df["Code"].str.match(r"^\d{6}$", na=False)].copy()
            df["Market"] = [
                _normalize_market_label(market, market_id)
                for market, market_id in zip(df.get("Market", []), df.get("MarketId", [None] * len(df)))
            ]
            if "Market" in df.columns and self.markets:
                df = df[df["Market"].isin(self.markets)].copy()
            if "Name" in df.columns:
                df = df[~df["Name"].apply(_is_excluded_name)].copy()
                df["Name"] = df["Name"].astype(str).str.strip()
            if self.exclude_construction:
                name_mask = _contains_any_keyword(df["Name"], DEFAULT_EXCLUDED_CONSTRUCTION_KEYWORDS) if "Name" in df.columns else pd.Series(False, index=df.index)
                sector_mask = pd.Series(False, index=df.index)
                for col in ("Sector", "Industry"):
                    if col in df.columns:
                        sector_mask |= _contains_any_keyword(df[col], DEFAULT_EXCLUDED_CONSTRUCTION_KEYWORDS)
                df = df[~(name_mask | sector_mask)].copy()
            if self.min_marcap_krw:
                df = df[df["Marcap"] >= int(self.min_marcap_krw)].copy()
            df = df.sort_values("Marcap", ascending=False)
            if self.top_n > 0:
                df = df.head(int(self.top_n)).copy()

            return df.rename(
                columns={
                    "Code": "symbol",
                    "Name": "name",
                    "Marcap": "marcap",
                    "Market": "market",
                    "Sector": "sector",
                    "Industry": "industry",
                }
            )[["symbol", "name", "marcap", "market", "sector", "industry"]]

        raise ValueError("No fallback universe file was available.")

    def _attach_news_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.include_news or df.empty or "symbol" not in df.columns:
            if "has_news_factors" not in df.columns:
                df["has_news_factors"] = False
            return df

        try:
            try:
                from stock_news_features import build_stock_news_factor_frame
            except Exception:
                from Disclosure.stock_news_features import build_stock_news_factor_frame

            news_df = build_stock_news_factor_frame(days=self.news_lookback_days)
        except Exception as exc:
            log.warning("stock news factor load failed: %s", exc)
            news_df = pd.DataFrame()

        if news_df.empty:
            df["has_news_factors"] = False
            return df

        merged = df.merge(news_df, how="left", on="symbol")
        merged["has_news_factors"] = merged["news_avg_score"].notna()
        return merged

    def _should_fetch_flow_for_rank(self, rank_index: int) -> bool:
        if not self.include_flow:
            return False
        if self.flow_top_n is None:
            return True
        return rank_index < self.flow_top_n

    def _should_fetch_consensus_for_rank(self, rank_index: int) -> bool:
        if not self.include_consensus:
            return False
        if self.consensus_top_n is None:
            return True
        return rank_index < self.consensus_top_n

    def _fetch_price_history(self, symbol: str) -> pd.DataFrame:
        cache_path = self._price_cache_path(symbol)
        start_date = self.snapshot_ts.date() - timedelta(days=max(self.price_lookback_days * 2, 500))
        end_date = self.snapshot_ts.date()

        cached_df: Optional[pd.DataFrame] = None
        if os.path.exists(cache_path):
            try:
                cached_df = pd.read_csv(cache_path, parse_dates=["Date"])
                if not cached_df.empty:
                    cached_df = cached_df.drop_duplicates(subset=["Date"]).sort_values("Date")
            except Exception:
                cached_df = None

        if fdr is None:
            if pykrx_stock is not None:
                try:
                    fresh_df = pykrx_stock.get_market_ohlcv_by_date(
                        start_date.strftime("%Y%m%d"),
                        end_date.strftime("%Y%m%d"),
                        symbol,
                    ).reset_index()
                    fresh_df = fresh_df.rename(
                        columns={"날짜": "Date", "시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume"}
                    )
                except Exception as exc:
                    log.warning("pykrx price fetch failed for %s: %s", symbol, exc)
                    fresh_df = pd.DataFrame()
            else:
                fresh_df = pd.DataFrame()
            if fresh_df.empty:
                try:
                    fresh_df = fetch_naver_daily_price_history(
                        symbol,
                        start_date=start_date,
                        end_date=end_date,
                        lookback_days=max(self.price_lookback_days + 5, 260),
                        sleep_sec=self.price_sleep_sec,
                    )
                except Exception as exc:
                    log.warning("Naver price fallback failed for %s: %s", symbol, exc)
                    fresh_df = pd.DataFrame()
            if not fresh_df.empty:
                if cached_df is not None and not cached_df.empty:
                    price_df = pd.concat([cached_df, fresh_df], ignore_index=True)
                else:
                    price_df = fresh_df
                price_df = price_df.drop_duplicates(subset=["Date"]).sort_values("Date")
                price_df = price_df.tail(max(self.price_lookback_days + 5, 260))
                price_df.to_csv(cache_path, index=False, encoding="utf-8-sig")
                if not self._price_source_warning_logged:
                    source_name = "pykrx" if pykrx_stock is not None else "Naver daily-price fallback"
                    log.warning("FinanceDataReader unavailable; using %s.", source_name)
                    self._price_source_warning_logged = True
                return price_df
            if cached_df is not None and not cached_df.empty:
                if not self._price_source_warning_logged:
                    log.warning("FinanceDataReader unavailable; using cached price data when present.")
                    self._price_source_warning_logged = True
                return cached_df.tail(max(self.price_lookback_days + 5, 260))
            if not self._price_source_warning_logged:
                log.warning("FinanceDataReader unavailable; skipping price factors until the package is installed.")
                self._price_source_warning_logged = True
            return pd.DataFrame()

        try:
            fetch_start = start_date
            if cached_df is not None and not cached_df.empty:
                last_dt = cached_df["Date"].max().date()
                fetch_start = max(start_date, last_dt - timedelta(days=7))

            fresh_df = fdr.DataReader(symbol, fetch_start, end_date)
            if not fresh_df.empty:
                fresh_df = fresh_df.reset_index().rename(columns={"index": "Date"})
                fresh_df["Date"] = pd.to_datetime(fresh_df["Date"])
            else:
                fresh_df = pd.DataFrame()

            if cached_df is not None and not cached_df.empty:
                price_df = pd.concat([cached_df, fresh_df], ignore_index=True)
            else:
                price_df = fresh_df

            if price_df.empty:
                return price_df

            price_df = price_df.drop_duplicates(subset=["Date"]).sort_values("Date")
            price_df = price_df.tail(max(self.price_lookback_days + 5, 260))
            price_df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            return price_df
        except Exception as exc:
            if cached_df is not None and not cached_df.empty:
                log.warning("Price fetch failed for %s; using cached data: %s", symbol, exc)
                return cached_df.tail(max(self.price_lookback_days + 5, 260))
            log.warning("Price fetch failed for %s: %s", symbol, exc)
            return pd.DataFrame()

    def _compute_price_factors(self, price_df: pd.DataFrame, marcap: Optional[float]) -> Dict[str, Any]:
        if price_df.empty or "Close" not in price_df.columns:
            return {}

        price_df = price_df.sort_values("Date").copy()
        close = pd.to_numeric(price_df["Close"], errors="coerce").dropna()
        if close.empty:
            return {}

        high = pd.to_numeric(price_df["High"], errors="coerce") if "High" in price_df.columns else None
        low = pd.to_numeric(price_df["Low"], errors="coerce") if "Low" in price_df.columns else None
        volume = pd.to_numeric(price_df["Volume"], errors="coerce") if "Volume" in price_df.columns else None
        daily_ret = close.pct_change()

        latest_close = float(close.iloc[-1])
        latest_volume = float(volume.iloc[-1]) if volume is not None and len(volume.dropna()) else None
        avg_turnover_20d = None
        if marcap and marcap > 0 and volume is not None and len(volume.dropna()) >= 20:
            avg_turnover_20d = float(((close * volume).tail(20).mean()) / marcap)

        avg_range_20d = None
        if high is not None and low is not None and len(close) >= 20:
            avg_range_20d = float((((high - low) / close.replace(0, np.nan)).tail(20)).mean())

        out = {
            "close": latest_close,
            "volume": latest_volume,
            "ret_1d": _calc_return(close, 1),
            "ret_5d": _calc_return(close, 5),
            "ret_20d": _calc_return(close, 20),
            "ret_60d": _calc_return(close, 60),
            "ret_120d": _calc_return(close, 120),
            "ma_gap_20d": _calc_ma_gap(close, 20),
            "ma_gap_60d": _calc_ma_gap(close, 60),
            "ma_gap_120d": _calc_ma_gap(close, 120),
            "drawdown_60d": _calc_drawdown(close, 60),
            "drawdown_250d": _calc_drawdown(close, 250),
            "volatility_20d": float(daily_ret.tail(20).std(ddof=0) * math.sqrt(252)) if len(daily_ret.dropna()) >= 20 else None,
            "volatility_60d": float(daily_ret.tail(60).std(ddof=0) * math.sqrt(252)) if len(daily_ret.dropna()) >= 60 else None,
            "avg_turnover_20d": avg_turnover_20d,
            "avg_range_20d": avg_range_20d,
            "volume_ratio_20d": (
                float(volume.iloc[-1] / volume.tail(20).mean())
                if volume is not None and len(volume.dropna()) >= 20 and pd.notna(volume.tail(20).mean()) and volume.tail(20).mean() != 0
                else None
            ),
        }
        return out

    def _fetch_flow_factors(self, symbol: str, marcap: Optional[float]) -> Dict[str, Any]:
        if self.broker is None:
            return {}

        payload = fetch_recent_investor_days(self.broker, symbol, max_days=5)
        valid_days = payload.get("days") or []
        if not valid_days:
            errors = payload.get("errors") or []
            if errors:
                log.warning("Flow fetch failed for %s: %s", symbol, " | ".join(str(err) for err in errors[:2]))
            return {}

        f_1d = int(valid_days[0].get("foreign_eok", 0))
        i_1d = int(valid_days[0].get("inst_eok", 0))
        r_1d = int(valid_days[0].get("retail_eok", 0))

        f_3d = sum(int(day.get("foreign_eok", 0)) for day in valid_days[:3])
        i_3d = sum(int(day.get("inst_eok", 0)) for day in valid_days[:3])
        r_3d = sum(int(day.get("retail_eok", 0)) for day in valid_days[:3])

        f_5d = sum(int(day.get("foreign_eok", 0)) for day in valid_days[:5])
        i_5d = sum(int(day.get("inst_eok", 0)) for day in valid_days[:5])
        r_5d = sum(int(day.get("retail_eok", 0)) for day in valid_days[:5])

        f_streak = 0
        i_streak = 0
        for idx, day in enumerate(valid_days[:5]):
            f_amt = int(day.get("foreign_eok", 0))
            i_amt = int(day.get("inst_eok", 0))
            if f_amt > 0 and f_streak == idx:
                f_streak += 1
            if i_amt > 0 and i_streak == idx:
                i_streak += 1

        scale = (100_000_000 / marcap) if marcap and marcap > 0 else None
        smart_money_3d = f_3d + i_3d
        smart_money_5d = f_5d + i_5d
        flow_confidence_score = float(payload.get("confidence_score", 0.0) or 0.0)

        return {
            "foreign_1d_eok": f_1d,
            "inst_1d_eok": i_1d,
            "retail_1d_eok": r_1d,
            "foreign_3d_eok": f_3d,
            "inst_3d_eok": i_3d,
            "retail_3d_eok": r_3d,
            "foreign_5d_eok": f_5d,
            "inst_5d_eok": i_5d,
            "retail_5d_eok": r_5d,
            "foreign_streak": f_streak,
            "inst_streak": i_streak,
            "smart_money_3d_eok": smart_money_3d,
            "smart_money_5d_eok": smart_money_5d,
            "foreign_3d_to_mcap": float(f_3d * scale) if scale is not None else None,
            "inst_3d_to_mcap": float(i_3d * scale) if scale is not None else None,
            "smart_money_3d_to_mcap": float(smart_money_3d * scale) if scale is not None else None,
            "smart_money_5d_to_mcap": float(smart_money_5d * scale) if scale is not None else None,
            "flow_confidence_score": flow_confidence_score,
            "flow_coverage_ratio": float(payload.get("coverage_ratio", 0.0) or 0.0),
            "flow_source_confidence": float(payload.get("source_confidence", 0.0) or 0.0),
            "flow_data_source": str(payload.get("source") or "missing"),
            "flow_fallback_used": bool(payload.get("fallback_used")),
        }

    def _fetch_consensus_factors(self, symbol: str, marcap: Optional[float]) -> Dict[str, Any]:
        if self.consensus_repo is None or self._consensus_disabled:
            return {}

        try:
            cons = self.consensus_repo.get_quarter_consensus(symbol)
        except Exception as exc:
            log.warning("Consensus fetch failed for %s: %s", symbol, exc)
            self._consensus_disabled = True
            self.consensus_repo = None
            return {}

        revenue_q = _safe_float(cons.get("revenue"))
        op_q = _safe_float(cons.get("op"))
        net_q = _safe_float(cons.get("net"))
        revenue_q_annualized = revenue_q * 4 if revenue_q is not None else None
        op_q_annualized = op_q * 4 if op_q is not None else None
        net_q_annualized = net_q * 4 if net_q is not None else None

        revenue_fy0 = _safe_float(cons.get("revenue_fy0"))
        op_fy0 = _safe_float(cons.get("op_fy0"))
        net_fy0 = _safe_float(cons.get("net_fy0"))
        revenue_fy1 = _safe_float(cons.get("revenue_fy1"))
        op_fy1 = _safe_float(cons.get("op_fy1"))
        net_fy1 = _safe_float(cons.get("net_fy1"))
        revenue_actual = _safe_float(cons.get("revenue_actual"))
        op_actual = _safe_float(cons.get("op_actual"))
        net_actual = _safe_float(cons.get("net_actual"))

        revenue_y, revenue_basis_period, revenue_input_source = _pick_consensus_value(revenue_fy1, revenue_fy0, revenue_actual, revenue_q_annualized)
        op_y, op_basis_period, op_input_source = _pick_consensus_value(op_fy1, op_fy0, op_actual, op_q_annualized)
        net_y, net_basis_period, net_input_source = _pick_consensus_value(net_fy1, net_fy0, net_actual, net_q_annualized)

        return {
            "cons_revenue_q_krw": revenue_q,
            "cons_op_q_krw": op_q,
            "cons_net_q_krw": net_q,
            "cons_revenue_q_annualized_krw": revenue_q_annualized,
            "cons_op_q_annualized_krw": op_q_annualized,
            "cons_net_q_annualized_krw": net_q_annualized,
            "cons_revenue_fy0_krw": revenue_fy0,
            "cons_op_fy0_krw": op_fy0,
            "cons_net_fy0_krw": net_fy0,
            "cons_revenue_fy1_krw": revenue_fy1,
            "cons_op_fy1_krw": op_fy1,
            "cons_net_fy1_krw": net_fy1,
            "cons_revenue_actual_krw": revenue_actual,
            "cons_op_actual_krw": op_actual,
            "cons_net_actual_krw": net_actual,
            "cons_actual_year": _safe_int(cons.get("actual_year")) or None,
            "cons_revenue_y_krw": revenue_y,
            "cons_op_y_krw": op_y,
            "cons_net_y_krw": net_y,
            "cons_revenue_basis_period": revenue_basis_period,
            "cons_op_basis_period": op_basis_period,
            "cons_net_basis_period": net_basis_period,
            "cons_revenue_input_source": revenue_input_source,
            "cons_op_input_source": op_input_source,
            "cons_net_input_source": net_input_source,
            "cons_op_margin_q": float(op_q / revenue_q) if revenue_q not in (None, 0) and op_q is not None else None,
            "cons_net_margin_q": float(net_q / revenue_q) if revenue_q not in (None, 0) and net_q is not None else None,
            "cons_psr": float(marcap / revenue_y) if marcap and marcap > 0 and revenue_y not in (None, 0) else None,
            "cons_op_yield": float(op_y / marcap) if marcap and marcap > 0 and op_y is not None else None,
            "cons_net_yield": float(net_y / marcap) if marcap and marcap > 0 and net_y is not None else None,
        }

    def _apply_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        for col in [
            "marcap",
            "ret_20d",
            "ret_60d",
            "ret_120d",
            "ma_gap_20d",
            "drawdown_250d",
            "volatility_20d",
            "avg_turnover_20d",
            "smart_money_3d_to_mcap",
            "smart_money_5d_to_mcap",
            "foreign_streak",
            "inst_streak",
            "cons_op_yield",
            "cons_net_yield",
            "cons_psr",
            "cons_op_margin_q",
            "cons_net_margin_q",
            "news_score",
            "news_count",
            "news_avg_score",
            "news_max_score",
            "news_confidence_score",
            "news_buzz_score",
            "news_source_breadth_score",
            "news_diffusion_score",
            "news_novelty_score",
            "flow_confidence_score",
            "flow_coverage_ratio",
            "flow_source_confidence",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        score_map = {
            "size_score": ("marcap", True),
            "liquidity_score": ("avg_turnover_20d", True),
            "momentum_ret20_score": ("ret_20d", True),
            "momentum_ret60_score": ("ret_60d", True),
            "momentum_ret120_score": ("ret_120d", True),
            "momentum_ma20_score": ("ma_gap_20d", True),
            "momentum_drawdown_score": ("drawdown_250d", True),
            "stability_score": ("volatility_20d", False),
            "value_op_yield_score": ("cons_op_yield", True),
            "value_net_yield_score": ("cons_net_yield", True),
            "value_psr_score": ("cons_psr", False),
            "quality_op_margin_score": ("cons_op_margin_q", True),
            "quality_net_margin_score": ("cons_net_margin_q", True),
            "flow_3d_score": ("smart_money_3d_to_mcap", True),
            "flow_5d_score": ("smart_money_5d_to_mcap", True),
            "flow_foreign_streak_score": ("foreign_streak", True),
            "flow_inst_streak_score": ("inst_streak", True),
            "news_sentiment_factor_score": ("news_avg_score", True),
            "news_confidence_factor_score": ("news_confidence_score", True),
            "news_buzz_factor_score": ("news_buzz_score", True),
            "news_source_factor_score": ("news_source_breadth_score", True),
            "news_diffusion_factor_score": ("news_diffusion_score", True),
            "news_novelty_factor_score": ("news_novelty_score", True),
        }

        for score_col, (base_col, higher_is_better) in score_map.items():
            if base_col in df.columns:
                df[score_col] = _pct_rank(df[base_col], higher_is_better=higher_is_better)
            else:
                df[score_col] = np.nan

        def mean_score(columns: Iterable[str]) -> pd.Series:
            score_cols = [col for col in columns if col in df.columns]
            if not score_cols:
                return pd.Series(np.nan, index=df.index)
            return df[score_cols].fillna(0.5).mean(axis=1)

        df["value_score"] = mean_score(["value_op_yield_score", "value_net_yield_score", "value_psr_score"])
        df["momentum_score"] = mean_score([
            "momentum_ret20_score",
            "momentum_ret60_score",
            "momentum_ret120_score",
            "momentum_ma20_score",
            "momentum_drawdown_score",
        ])
        df["quality_score"] = mean_score(["quality_op_margin_score", "quality_net_margin_score", "stability_score"])
        df["flow_score_raw"] = mean_score(["flow_3d_score", "flow_5d_score", "flow_foreign_streak_score", "flow_inst_streak_score"])
        if "flow_confidence_score" in df.columns:
            flow_conf = df["flow_confidence_score"].fillna(0.0).clip(lower=0.0, upper=1.0)
            df["flow_score"] = 0.5 + (df["flow_score_raw"].fillna(0.5) - 0.5) * flow_conf
        else:
            df["flow_score"] = df["flow_score_raw"]
        df["news_score"] = mean_score([
            "news_sentiment_factor_score",
            "news_confidence_factor_score",
            "news_buzz_factor_score",
            "news_source_factor_score",
            "news_diffusion_factor_score",
            "news_novelty_factor_score",
        ])
        weighted_scores = {
            "value_score": 0.22,
            "momentum_score": 0.22,
            "quality_score": 0.18,
            "flow_score": 0.18,
            "liquidity_score": 0.10,
            "news_score": 0.10,
        }
        self.dynamic_weight_info = compute_dynamic_factor_weights(
            snapshot_dir=self.paths.snapshot_dir,
            price_cache_dir=self.paths.price_cache_dir,
            base_weights=weighted_scores,
        )
        weighted_scores = dict(self.dynamic_weight_info.get("weights") or weighted_scores)
        numerator = pd.Series(0.0, index=df.index)
        denominator = pd.Series(0.0, index=df.index)
        for score_col, weight in weighted_scores.items():
            valid_mask = df[score_col].notna()
            numerator = numerator + df[score_col].fillna(0.0) * weight
            denominator = denominator + valid_mask.astype(float) * weight
        df["composite_score"] = numerator.div(denominator.replace(0, np.nan))

        if "has_price_factors" not in df.columns:
            df["has_price_factors"] = df["ret_20d"].notna() if "ret_20d" in df.columns else False
        if "has_flow_factors" not in df.columns:
            df["has_flow_factors"] = df["smart_money_3d_to_mcap"].notna() if "smart_money_3d_to_mcap" in df.columns else False
        if "has_consensus_factors" not in df.columns:
            df["has_consensus_factors"] = df["cons_psr"].notna() if "cons_psr" in df.columns else False
        if "has_news_factors" not in df.columns:
            df["has_news_factors"] = df["news_avg_score"].notna() if "news_avg_score" in df.columns else False

        df["available_factor_sources"] = (
            df["has_price_factors"].fillna(False).astype(int)
            + df["has_flow_factors"].fillna(False).astype(int)
            + df["has_consensus_factors"].fillna(False).astype(int)
            + df["has_news_factors"].fillna(False).astype(int)
        )
        df["expected_factor_sources"] = 1 + int(self.include_flow) + int(self.include_consensus) + int(self.include_news)
        df["factor_source_coverage_ratio"] = df["available_factor_sources"] / df["expected_factor_sources"].replace(0, np.nan)
        df["ranking_eligible"] = df["has_price_factors"].fillna(False)
        min_sources_for_portfolio = 2 if (self.include_flow or self.include_consensus or self.include_news) else 1
        df["portfolio_eligible"] = df["ranking_eligible"] & (df["available_factor_sources"] >= min_sources_for_portfolio)

        for col in ["value_score", "momentum_score", "quality_score", "flow_score", "news_score", "composite_score"]:
            df[f"sector_z_{col}"] = df.groupby("sector")[col].transform(_group_zscore)
            df[f"sector_rank_{col}"] = df.groupby("sector")[col].rank(pct=True, ascending=True)

        df["sector_reversion_signal"] = (
            df["sector_z_value_score"].fillna(0.0)
            + df["sector_z_quality_score"].fillna(0.0)
            + df["sector_z_flow_score"].fillna(0.0)
            - df["sector_z_momentum_score"].fillna(0.0)
        )
        df["sector_reversion_rank"] = df.groupby("sector")["sector_reversion_signal"].rank(pct=True, ascending=True)
        df["sector_leader_signal"] = (
            df["sector_z_momentum_score"].fillna(0.0)
            + df["sector_z_flow_score"].fillna(0.0)
            + df["sector_z_quality_score"].fillna(0.0)
        )
        df["sector_leader_rank"] = df.groupby("sector")["sector_leader_signal"].rank(pct=True, ascending=True)
        return df

    def build_snapshot(self) -> pd.DataFrame:
        universe_df = self._load_universe_df().reset_index(drop=True)
        expected_sources = ["price"]
        if self.include_flow:
            expected_sources.append("flow")
        if self.include_consensus:
            expected_sources.append("consensus")
        if self.include_news:
            expected_sources.append("news")
        rows = []

        for idx, row in universe_df.iterrows():
            symbol = row["symbol"]
            marcap = _safe_float(row.get("marcap"))
            item = {
                "snapshot_date": self.snapshot_date,
                "snapshot_time": self.snapshot_ts.strftime("%H:%M:%S"),
                "symbol": symbol,
                "name": row.get("name"),
                "market": row.get("market"),
                "sector": row.get("sector") or "Unknown",
                "industry": row.get("industry") or "Unknown",
                "marcap": marcap,
            }

            price_df = self._fetch_price_history(symbol)
            price_factors = self._compute_price_factors(price_df, marcap)
            item.update(price_factors)
            if self.price_sleep_sec > 0:
                time.sleep(self.price_sleep_sec)

            if self._should_fetch_flow_for_rank(idx):
                item.update(self._fetch_flow_factors(symbol, marcap))
                if self.flow_sleep_sec > 0:
                    time.sleep(self.flow_sleep_sec)

            if self._should_fetch_consensus_for_rank(idx):
                item.update(self._fetch_consensus_factors(symbol, marcap))

            has_price = bool(price_factors)
            has_flow = bool(item.get("smart_money_3d_to_mcap") is not None or item.get("foreign_3d_eok") is not None)
            has_consensus = bool(item.get("cons_psr") is not None or item.get("cons_op_yield") is not None)
            missing_sources = []
            if not has_price:
                missing_sources.append("price")
            if self.include_flow and not has_flow:
                missing_sources.append("flow")
            if self.include_consensus and not has_consensus:
                missing_sources.append("consensus")
            item["has_price_factors"] = has_price
            item["has_flow_factors"] = has_flow
            item["has_consensus_factors"] = has_consensus
            item["available_factor_sources"] = int(has_price) + int(has_flow) + int(has_consensus)
            item["expected_factor_sources"] = len(expected_sources)
            item["factor_source_coverage_ratio"] = item["available_factor_sources"] / len(expected_sources)
            item["missing_factor_sources"] = ",".join(missing_sources)

            rows.append(item)
            if (idx + 1) % 25 == 0 or idx + 1 == len(universe_df):
                log.info("Factor snapshot progress: %s/%s", idx + 1, len(universe_df))

        df = pd.DataFrame(rows)
        if df.empty:
            raise ValueError("No factor rows were built.")

        df = self._attach_news_factors(df)
        df = self._apply_scores(df)
        df = df.sort_values(["composite_score", "sector", "marcap"], ascending=[False, True, False]).reset_index(drop=True)
        return df

    def build_summary(self, df: pd.DataFrame, top_n: int = 25, per_sector: int = 3) -> Dict[str, Any]:
        df = df.copy()
        top_n = int(top_n)
        per_sector = int(per_sector)
        ranking_df = df[df["ranking_eligible"]].copy() if "ranking_eligible" in df.columns else df.copy()
        portfolio_df = df[df["portfolio_eligible"]].copy() if "portfolio_eligible" in df.columns else ranking_df.copy()
        if portfolio_df.empty:
            portfolio_df = ranking_df.copy()

        top_composite = (
            ranking_df.sort_values(["composite_score", "factor_source_coverage_ratio", "marcap"], ascending=[False, False, False])
            .head(top_n)[
                [
                    "symbol",
                    "name",
                    "sector",
                    "composite_score",
                    "value_score",
                    "momentum_score",
                    "quality_score",
                    "flow_score",
                    "news_score",
                    "factor_source_coverage_ratio",
                ]
            ]
            .round(4)
            .to_dict("records")
        )

        top_reversion = (
            ranking_df.sort_values(["sector_reversion_signal", "factor_source_coverage_ratio"], ascending=[False, False])
            .head(top_n)[
                [
                    "symbol",
                    "name",
                    "sector",
                    "sector_reversion_signal",
                    "sector_reversion_rank",
                    "value_score",
                    "momentum_score",
                    "flow_score",
                    "news_score",
                    "factor_source_coverage_ratio",
                ]
            ]
            .round(4)
            .to_dict("records")
        )

        sector_cards = []
        for sector, sector_df in ranking_df.groupby("sector", dropna=False):
            avg_score = round(float(sector_df["composite_score"].mean()), 4)
            leaders = (
                sector_df.sort_values("sector_leader_signal", ascending=False)
                .head(per_sector)[["symbol", "name", "sector_leader_signal", "composite_score"]]
                .round(4)
                .to_dict("records")
            )
            laggards = (
                sector_df.sort_values("sector_reversion_signal", ascending=False)
                .head(per_sector)[["symbol", "name", "sector_reversion_signal", "value_score", "flow_score", "momentum_score"]]
                .round(4)
                .to_dict("records")
            )
            sector_cards.append(
                {
                    "sector": sector,
                    "count": int(len(sector_df)),
                    "avg_composite_score": avg_score,
                    "action": _sector_action_label(avg_score, int(len(sector_df))),
                    "leaders": leaders,
                    "laggards": laggards,
                }
            )

        sector_cards = sorted(sector_cards, key=lambda x: x["avg_composite_score"], reverse=True)
        coverage = {
            "total_names": int(len(df)),
            "markets": list(self.markets),
            "universe_mode": "full" if self.top_n <= 0 else "top_n",
            "min_marcap_krw": int(self.min_marcap_krw or 0),
            "flow_top_n": self.flow_top_n or 0,
            "consensus_top_n": self.consensus_top_n or 0,
            "price_factor_coverage_pct": round(float(df["has_price_factors"].fillna(False).mean() * 100), 1) if "has_price_factors" in df.columns else 0.0,
            "flow_factor_coverage_pct": round(float(df["has_flow_factors"].fillna(False).mean() * 100), 1) if self.include_flow and "has_flow_factors" in df.columns else 0.0,
            "consensus_factor_coverage_pct": round(float(df["has_consensus_factors"].fillna(False).mean() * 100), 1) if self.include_consensus and "has_consensus_factors" in df.columns else 0.0,
            "news_factor_coverage_pct": round(float(df["has_news_factors"].fillna(False).mean() * 100), 1) if self.include_news and "has_news_factors" in df.columns else 0.0,
            "ranking_eligible_count": int(ranking_df.shape[0]),
            "portfolio_eligible_count": int(portfolio_df.shape[0]),
            "missing_price_count": int((~df["has_price_factors"].fillna(False)).sum()) if "has_price_factors" in df.columns else int(len(df)),
            "missing_flow_count": int((~df["has_flow_factors"].fillna(False)).sum()) if self.include_flow and "has_flow_factors" in df.columns else 0,
            "missing_consensus_count": int((~df["has_consensus_factors"].fillna(False)).sum()) if self.include_consensus and "has_consensus_factors" in df.columns else 0,
            "missing_news_count": int((~df["has_news_factors"].fillna(False)).sum()) if self.include_news and "has_news_factors" in df.columns else 0,
        }

        selected = []
        sector_counter: Counter = Counter()
        ranked_df = portfolio_df.sort_values(
            ["composite_score", "factor_source_coverage_ratio", "flow_score", "quality_score"],
            ascending=[False, False, False, False],
        )
        for _, row in ranked_df.iterrows():
            sector = row.get("sector") or "Unknown"
            if sector_counter[sector] >= 2:
                continue
            selected.append(
                {
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "sector": sector,
                    "composite_score": round(float(row.get("composite_score", 0)), 4),
                    "value_score": round(float(row.get("value_score", 0)), 4),
                    "momentum_score": round(float(row.get("momentum_score", 0)), 4),
                    "quality_score": round(float(row.get("quality_score", 0)), 4),
                    "flow_score": round(float(row.get("flow_score", 0)), 4),
                    "news_score": round(float(row.get("news_score", 0)), 4),
                    "factor_source_coverage_ratio": round(float(row.get("factor_source_coverage_ratio", 0)), 4),
                }
            )
            sector_counter[sector] += 1
            if len(selected) >= min(top_n, 12):
                break

        weak_sources = []
        if coverage["flow_factor_coverage_pct"] < 40:
            weak_sources.append(f"수급 {coverage['flow_factor_coverage_pct']}%")
        if coverage["consensus_factor_coverage_pct"] < 30:
            weak_sources.append(f"컨센서스 {coverage['consensus_factor_coverage_pct']}%")
        if coverage["news_factor_coverage_pct"] < 40:
            weak_sources.append(f"뉴스 {coverage['news_factor_coverage_pct']}%")
        coverage_grade = "양호"
        if len(weak_sources) >= 2:
            coverage_grade = "보수적"
        elif weak_sources:
            coverage_grade = "중간"

        data_warnings = []
        if weak_sources:
            data_warnings.extend(weak_sources)
        dynamic_status = self.dynamic_weight_info.get("status", "unknown")
        if str(dynamic_status) == "fallback":
            data_warnings.append("팩터 비중조절 기본값 복귀")

        top_sector_counter = Counter(item["sector"] for item in selected[:6] if item.get("sector"))
        top_sector_name = None
        top_sector_share = 0.0
        if top_sector_counter:
            top_sector_name, top_sector_count = top_sector_counter.most_common(1)[0]
            denom = max(1, min(len(selected), 6))
            top_sector_share = round(top_sector_count / denom, 3)
            if top_sector_count >= 3:
                data_warnings.append(f"상위 후보가 {top_sector_name}에 편중")

        data_quality = {
            "coverage_grade": coverage_grade,
            "weak_sources": weak_sources,
            "dynamic_status": dynamic_status,
            "warnings": data_warnings[:4],
            "top_sector": top_sector_name or "-",
            "top_sector_share": top_sector_share,
        }
        decision_regime = _build_factor_decision_regime(data_quality)
        context_alignment = load_latest_context_alignment()

        candidate_actions = []
        bucket_map = {"direct": [], "watch": [], "hold": []}
        for item in selected:
            base_action = _base_factor_action(item)
            action = _apply_factor_regime(base_action, decision_regime["name"])
            aligned_item = decorate_items_with_alignment([item], context_alignment)[0]
            action = _apply_factor_alignment(action, int(aligned_item.get("alignment_score", 0) or 0))
            decorated = {
                **aligned_item,
                "base_action": base_action,
                "action": action,
                "action_label": _factor_action_label(action),
                "strong_axes": [name for name, _ in _top_factor_mix_pairs(item)],
                "decision_trace": {
                    "base_action": base_action,
                    "coverage_ratio": round(float(item.get("factor_source_coverage_ratio", 0) or 0), 4),
                    "dominant_axes": [name for name, _ in _top_factor_mix_pairs(item)],
                    "composite_score": round(float(item.get("composite_score", 0) or 0), 4),
                    "alignment_score": int(aligned_item.get("alignment_score", 0) or 0),
                },
            }
            candidate_actions.append(decorated)
            bucket_map[action].append(decorated)
        action_hints, action_hint_rows = _build_factor_action_hints(candidate_actions, decision_regime)
        sector_cards = decorate_items_with_alignment(sector_cards, context_alignment)
        for sector in sector_cards:
            sector["action"] = _sector_action_label(
                float(sector.get("avg_composite_score", 0) or 0),
                int(sector.get("count", 0) or 0),
                int(sector.get("alignment_score", 0) or 0),
            )

        return {
            "snapshot_date": self.snapshot_date,
            "snapshot_time": self.snapshot_ts.strftime("%H:%M:%S"),
            "coverage": coverage,
            "dynamic_factor_weights": self.dynamic_weight_info,
            "top_composite": top_composite,
            "top_reversion": top_reversion,
            "portfolio_candidates": candidate_actions,
            "portfolio_buckets": {
                "direct": bucket_map["direct"][:3],
                "watch": bucket_map["watch"][:3],
                "hold": bucket_map["hold"][:3],
            },
            "candidate_actions": [
                {
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "sector": item["sector"],
                    "action": item["action"],
                    "action_label": item["action_label"],
                    "strong_axes": item["strong_axes"],
                    "alignment_label": item.get("alignment_label", "중립"),
                    "alignment_score": int(item.get("alignment_score", 0) or 0),
                }
                for item in candidate_actions
            ],
            "data_quality": data_quality,
            "decision_regime": decision_regime,
            "context_alignment": {
                "market_mode": context_alignment.get("market_mode", "중립"),
                "confidence_score": int(context_alignment.get("confidence_score", 0) or 0),
                "top_support": (context_alignment.get("top_support") or [])[:4],
                "top_risk": (context_alignment.get("top_risk") or [])[:3],
            },
            "action_hints": action_hints,
            "action_hint_rows": action_hint_rows,
            "top_sectors": sector_cards[: min(len(sector_cards), 8)],
            "sector_cards": sector_cards,
        }

    def build_slack_digest(self, summary: Dict[str, Any], max_names: int = 6, max_laggards: int = 6, max_sectors: int = 5) -> str:
        summary = normalize_factor_summary(summary)
        coverage = summary.get("coverage", {})
        portfolio_candidates = summary.get("portfolio_candidates", [])[:max_names]
        laggards = summary.get("top_reversion", [])[:max_laggards]
        sectors = summary.get("top_sectors", [])[:max_sectors]
        buckets = summary.get("portfolio_buckets") or {}
        data_quality = summary.get("data_quality") or {}
        decision_regime = summary.get("decision_regime") or {}

        lines = []
        lines.append(
            f"*커버리지* 종목 `{coverage.get('total_names', 0)}` | 가격 `{coverage.get('price_factor_coverage_pct', 0)}%` "
            f"| 수급 `{coverage.get('flow_factor_coverage_pct', 0)}%` | 컨센서스 `{coverage.get('consensus_factor_coverage_pct', 0)}%` "
            f"| 뉴스 `{coverage.get('news_factor_coverage_pct', 0)}%`"
        )
        lines.append(
            f"- 적격 종목: 랭킹 `{coverage.get('ranking_eligible_count', 0)}`개 | 포트폴리오 `{coverage.get('portfolio_eligible_count', 0)}`개 "
            f"| 시장 `{', '.join(coverage.get('markets', []))}` | 모드 `{coverage.get('universe_mode', 'top_n')}` "
            f"| 최소시총 `{coverage.get('min_marcap_krw', 0):,}` | 수급상한 `{coverage.get('flow_top_n', 0)}` | 컨센상한 `{coverage.get('consensus_top_n', 0)}`"
        )
        dyn = summary.get("dynamic_factor_weights") or {}
        dyn_weights = dyn.get("weights") or {}
        if dyn_weights:
            lines.append(
                "- 비중 조절: "
                + ", ".join(f"`{key.replace('_score', '')}:{round(float(value), 3)}`" for key, value in dyn_weights.items())
                + f" | status `{dyn.get('status', 'unknown')}`"
            )
        if max(
            coverage.get("price_factor_coverage_pct", 0),
            coverage.get("flow_factor_coverage_pct", 0),
            coverage.get("consensus_factor_coverage_pct", 0),
            coverage.get("news_factor_coverage_pct", 0),
        ) < 50:
            lines.append("- `주의:` 현재 팩터 커버리지가 낮아 점수 신뢰도가 제한적입니다. 가격/수급/컨센서스/뉴스 소스를 먼저 채우는 것이 좋습니다.")

        lines.append("\n*0. 오늘 뭐 할까*")
        if buckets.get("direct"):
            lines.append("- 직접 후보: `" + ", ".join(item["name"] for item in buckets.get("direct", [])[:2]) + "`")
        else:
            lines.append("- 직접 후보: `없음`")
        if buckets.get("watch"):
            lines.append("- 눌림 후보: `" + ", ".join(item["name"] for item in buckets.get("watch", [])[:2]) + "`")
        else:
            lines.append("- 눌림 후보: `없음`")
        if buckets.get("hold"):
            lines.append("- 보류: `" + ", ".join(item["name"] for item in buckets.get("hold", [])[:2]) + "`")
        else:
            lines.append("- 보류: `없음`")
        if decision_regime.get("description"):
            lines.append(f"- 해석 모드: {decision_regime.get('description')}")
        if data_quality.get("coverage_grade"):
            lines.append(f"- 데이터 품질: `{data_quality.get('coverage_grade')}`")
        context_alignment = summary.get("context_alignment") or {}
        if context_alignment.get("market_mode"):
            lines.append(
                f"- 시장 맥락: `{context_alignment.get('market_mode')}` | 확신도 `{int(context_alignment.get('confidence_score', 0) or 0)}/100`"
            )
        if context_alignment.get("top_support"):
            lines.append(
                "- 맥락 정렬 섹터: `" + ", ".join(row.get("sector", "-") for row in context_alignment.get("top_support", [])[:3]) + "`"
            )
        if context_alignment.get("top_risk"):
            lines.append(
                "- 맥락 경계 섹터: `" + ", ".join(row.get("sector", "-") for row in context_alignment.get("top_risk", [])[:2]) + "`"
            )
        sector_thesis = summary.get("sector_thesis") or {}
        top_thesis = list(sector_thesis.get("top_sectors") or [])[:2]
        if top_thesis:
            lines.append(
                "- 공통 결론: `"
                + " / ".join(
                    f"{row.get('sector', '-')}:{row.get('final_label', '-')}"
                    for row in top_thesis
                )
                + "`"
            )
        for warning in (data_quality.get("warnings") or [])[:2]:
            lines.append(f"- 데이터 주의: {warning}")

        lines.append("\n*한눈 요약*")
        if sectors:
            lines.append("- 상위 후보는 `" + ", ".join(sector["sector"] for sector in sectors[:3]) + "` 쪽에 몰려 있습니다.")
        if portfolio_candidates:
            lines.append("- 바로 체크할 종목은 `" + ", ".join(item["name"] for item in portfolio_candidates[:3]) + "` 입니다.")
        if laggards:
            lines.append("- 리버전 관찰 후보는 `" + ", ".join(item["name"] for item in laggards[:2]) + "` 입니다.")
        if dyn_weights:
            lines.append(f"- 지금 점수를 가장 많이 끄는 축은 `{_dominant_style(dyn_weights)}` 입니다.")
        if data_quality.get("weak_sources"):
            lines.append("- 약한 데이터 축은 `" + ", ".join(data_quality.get("weak_sources", [])[:3]) + "` 입니다.")
        if str(dyn.get("status", "unknown")) == "fallback":
            lines.append("- 팩터 비중조절이 `fallback` 상태라 오늘은 기본 비중에 더 가깝게 읽는 편이 좋습니다.")

        lines.append("\n*1. 오늘 먼저 볼 후보*")
        for item in portfolio_candidates:
            lines.append(
                f"- *{item['name']}* ({item['sector']}) | 판단 `{item.get('action_label') or _factor_action_label(item.get('action', 'hold'))}` "
                f"| 맥락 `{item.get('alignment_label', '중립')}` | 섹터결론 `{item.get('sector_final_label') or '-'}` | composite `{item['composite_score']}` | 강점 `{_top_factor_mix(item)}`"
            )

        lines.append("\n*2. 눌림/리버전 후보*")
        for item in laggards:
            lines.append(
                f"- *{item['name']}* ({item['sector']}) | 판단 `{_reversion_action_label(item)}` "
                f"| reversion `{item['sector_reversion_signal']}` | 받쳐주는 축 `{_top_factor_mix(item)}`"
            )

        lines.append("\n*3. 섹터 체온*")
        for sector in sectors:
            leader_names = ", ".join(item["name"] for item in sector.get("leaders", [])[:2]) or "-"
            lines.append(
                f"- *{sector['sector']}* | 판단 `{sector.get('action') or _sector_action_label(float(sector.get('avg_composite_score', 0) or 0), int(sector.get('count', 0) or 0))}` "
                f"| 맥락 `{sector.get('alignment_label', '중립')}` | 공통결론 `{sector.get('sector_final_label') or '-'}` "
                f"| avg composite `{sector['avg_composite_score']}` | 중심주 `{leader_names}`"
            )

        lines.append("\n*4. 액션 힌트*")
        for hint in summary.get("action_hints", [])[:4]:
            lines.append(f"- {hint}")

        return "\n".join(lines)

    def build_ai_context(self, summary: Dict[str, Any], max_names: int = 8, max_sectors: int = 6) -> str:
        summary = normalize_factor_summary(summary)
        coverage = summary.get("coverage", {})
        lines = []
        lines.append("[팩터 스냅샷 요약]")
        lines.append(
            f"- 커버리지: 종목 {coverage.get('total_names', 0)}개 | 가격 {coverage.get('price_factor_coverage_pct', 0)}% | "
            f"수급 {coverage.get('flow_factor_coverage_pct', 0)}% | 컨센서스 {coverage.get('consensus_factor_coverage_pct', 0)}% | "
            f"뉴스 {coverage.get('news_factor_coverage_pct', 0)}%"
        )
        lines.append(
            f"- 적격 종목: 랭킹 {coverage.get('ranking_eligible_count', 0)}개 | 포트폴리오 {coverage.get('portfolio_eligible_count', 0)}개 | "
            f"시장 {', '.join(coverage.get('markets', []))} | 모드 {coverage.get('universe_mode', 'top_n')} | "
            f"최소시총 {coverage.get('min_marcap_krw', 0):,} | 수급상한 {coverage.get('flow_top_n', 0)} | 컨센상한 {coverage.get('consensus_top_n', 0)}"
        )
        dyn = summary.get("dynamic_factor_weights") or {}
        dyn_weights = dyn.get("weights") or {}
        if dyn_weights:
            lines.append(
                "- 시장별 비중 조절: "
                + ", ".join(f"{key.replace('_score', '')}:{round(float(value), 3)}" for key, value in dyn_weights.items())
                + f" | 상태 {dyn.get('status', 'unknown')}"
            )
        data_quality = summary.get("data_quality") or {}
        if data_quality:
            lines.append(
                f"- 데이터 품질: {data_quality.get('coverage_grade', '-')} | "
                f"약한 축 {', '.join(data_quality.get('weak_sources', [])[:3]) or '-'} | "
                f"경고 {', '.join(data_quality.get('warnings', [])[:3]) or '-'}"
            )
        decision_regime = summary.get("decision_regime") or {}
        if decision_regime:
            lines.append(f"- 해석 모드: {decision_regime.get('name', '-')} | {decision_regime.get('description', '-')}")
        context_alignment = summary.get("context_alignment") or {}
        if context_alignment:
            lines.append(
                f"- 시장 맥락: {context_alignment.get('market_mode', '중립')} | 확신도 {int(context_alignment.get('confidence_score', 0) or 0)}/100"
            )
            if context_alignment.get("top_support"):
                lines.append(
                    "- 맥락 정렬 섹터: " + ", ".join(row.get("sector", "-") for row in context_alignment.get("top_support", [])[:3])
                )
            if context_alignment.get("top_risk"):
                lines.append(
                    "- 맥락 경계 섹터: " + ", ".join(row.get("sector", "-") for row in context_alignment.get("top_risk", [])[:2])
                )
        buckets = summary.get("portfolio_buckets") or {}
        if buckets.get("direct"):
            lines.append("- 직접 후보: " + ", ".join(item["name"] for item in buckets.get("direct", [])[:3]))
        if buckets.get("watch"):
            lines.append("- 눌림 후보: " + ", ".join(item["name"] for item in buckets.get("watch", [])[:3]))
        if buckets.get("hold"):
            lines.append("- 보류/관찰: " + ", ".join(item["name"] for item in buckets.get("hold", [])[:3]))
        if summary.get("action_hints"):
            lines.append("- 액션 힌트:")
            for hint in summary.get("action_hints", [])[:3]:
                lines.append(f"  {hint}")
        lines.append("- 포트폴리오 후보:")
        for item in summary.get("portfolio_candidates", [])[:max_names]:
            lines.append(
                f"  {item['name']}({item['symbol']}) | {item['sector']} | "
                f"맥락 {item.get('alignment_label', '중립')} | composite {item['composite_score']} | value {item['value_score']} | "
                f"momentum {item['momentum_score']} | quality {item['quality_score']} | flow {item['flow_score']} | news {item.get('news_score', 0)}"
            )
        lines.append("- 섹터 리버전 후보:")
        for item in summary.get("top_reversion", [])[:max_names]:
            lines.append(
                f"  {item['name']}({item['symbol']}) | {item['sector']} | "
                f"reversion {item['sector_reversion_signal']} | value {item['value_score']} | "
                f"momentum {item['momentum_score']} | flow {item['flow_score']} | news {item.get('news_score', 0)}"
            )
        lines.append("- 상위 섹터 카드:")
        for sector in summary.get("top_sectors", [])[:max_sectors]:
            leaders = ", ".join(item["name"] for item in sector.get("leaders", [])[:2]) or "-"
            laggards = ", ".join(item["name"] for item in sector.get("laggards", [])[:2]) or "-"
            lines.append(
                f"  {sector['sector']} | 맥락 {sector.get('alignment_label', '중립')} | avg composite {sector['avg_composite_score']} | leaders {leaders} | laggards {laggards}"
            )
        return "\n".join(lines)

    def save_snapshot(self, df: pd.DataFrame, summary: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        snapshot_csv = os.path.join(self.paths.snapshot_dir, f"factor_snapshot_{self.snapshot_date}.csv")
        latest_csv = os.path.join(self.paths.snapshot_dir, "factor_snapshot_latest.csv")
        summary_json = os.path.join(self.paths.snapshot_dir, f"factor_summary_{self.snapshot_date}.json")
        latest_summary_json = os.path.join(self.paths.snapshot_dir, "factor_summary_latest.json")

        df.to_csv(snapshot_csv, index=False, encoding="utf-8-sig")
        df.to_csv(latest_csv, index=False, encoding="utf-8-sig")

        if summary is None:
            summary = self.build_summary(df)
        summary = normalize_factor_summary(summary)

        with open(summary_json, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
        with open(latest_summary_json, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)

        return {
            "snapshot_csv": snapshot_csv,
            "latest_csv": latest_csv,
            "summary_json": summary_json,
            "latest_summary_json": latest_summary_json,
        }
