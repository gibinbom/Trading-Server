from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent
VALUATION_DIR = ROOT_DIR / "valuation"
ANALYST_RAW_DIR = ROOT_DIR / "analyst_reports" / "raw"

METRIC_BOARD_SPECS = [
    {
        "label": "PER",
        "current_col": "cons_net_yield_raw",
        "basis_period_col": "cons_net_basis_period_raw",
        "input_source_col": "cons_net_input_source_raw",
        "mode": "inverse_yield",
    },
    {
        "label": "PBR",
        "current_col": "cons_pbr_raw",
        "basis_period_col": "cons_pbr_basis_period",
        "input_source_col": "cons_pbr_input_source",
        "mode": "direct",
    },
    {
        "label": "PSR",
        "current_col": "cons_psr_raw",
        "basis_period_col": "cons_revenue_basis_period_raw",
        "input_source_col": "cons_revenue_input_source_raw",
        "mode": "direct",
    },
    {
        "label": "시가총액/영업이익",
        "current_col": "cons_op_yield_raw",
        "basis_period_col": "cons_op_basis_period_raw",
        "input_source_col": "cons_op_input_source_raw",
        "mode": "inverse_yield",
    },
]

GEOGRAPHY_KEYWORDS = {
    "국내": "국내",
    "해외": "해외",
    "미국": "미국",
    "유럽": "유럽",
    "중국": "중국",
    "베트남": "베트남",
    "일본": "일본",
    "러시아": "러시아",
    "중동": "중동",
    "북미": "북미",
    "남미": "남미",
    "EMEA": "EMEA",
    "NA": "북미",
    "APAC": "아시아",
    "MENA": "중동/북아프리카",
}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def _normalize_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _safe_float(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, "", "None"):
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", "None"):
            return default
        return int(float(value))
    except Exception:
        return default


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(pd.Series(values).median())


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    try:
        return float(pd.Series(values).quantile(q))
    except Exception:
        return None


def _winsorized_series(values: list[float]) -> pd.Series:
    series = pd.Series(values, dtype="float64").dropna()
    if series.empty:
        return series
    if len(series) >= 5:
        lower = float(series.quantile(0.10))
        upper = float(series.quantile(0.90))
        series = series.clip(lower=lower, upper=upper)
    return series


def _winsorized_stats(values: list[float]) -> tuple[float | None, float | None, float | None]:
    series = _winsorized_series(values)
    if series.empty:
        return None, None, None
    return float(series.median()), float(series.quantile(0.25)), float(series.quantile(0.75))


def _format_input_source(value: Any) -> str:
    text = _clean_text(value)
    return {
        "annual_consensus": "연간 컨센서스",
        "actual_annual": "실제 실적 snapshot",
        "actual_plus_disclosure": "실제 실적 + 공시 보정",
        "quarter_annualized": "연환산 분기",
        "proxy": "대체모형",
    }.get(text, text or "-")


def _calc_multiple_from_spec(row: pd.Series, spec: dict[str, Any]) -> float | None:
    value = _safe_float(row.get(spec["current_col"]), float("nan"))
    if not math.isfinite(value) or value <= 0:
        return None
    if spec["mode"] == "inverse_yield":
        return None if value <= 0 else 1.0 / value
    return value


def _estimate_shares(row: pd.Series) -> float | None:
    marcap = _safe_float(row.get("marcap"), float("nan"))
    price = _safe_float(row.get("current_price"), float("nan"))
    if not math.isfinite(marcap) or marcap <= 0 or not math.isfinite(price) or price <= 0:
        return None
    shares = marcap / price
    return shares if math.isfinite(shares) and shares > 0 else None


def _krw_per_share(total_krw: float | None, shares: float | None) -> float | None:
    if total_krw is None or shares is None:
        return None
    if not math.isfinite(total_krw) or total_krw <= 0 or not math.isfinite(shares) or shares <= 0:
        return None
    return total_krw / shares


def _ratio_pct(num: float | None, den: float | None) -> float | None:
    if num is None or den is None:
        return None
    if not math.isfinite(num) or not math.isfinite(den) or den <= 0:
        return None
    return (num / den) * 100.0


def _growth_pct(current: float | None, base: float | None) -> float | None:
    if current is None or base is None:
        return None
    if not math.isfinite(current) or not math.isfinite(base) or base <= 0:
        return None
    return ((current / base) - 1.0) * 100.0


def _sort_reports(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(item: dict[str, Any]) -> float:
        raw = _clean_text(item.get("published_at"))
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() if raw else 0.0
        except Exception:
            return 0.0
    return sorted(reports, key=key, reverse=True)


def _load_analyst_reports(max_files: int = 21) -> dict[str, list[dict[str, Any]]]:
    reports: dict[str, list[dict[str, Any]]] = defaultdict(list)
    files = sorted(ANALYST_RAW_DIR.glob("analyst_reports_*.jsonl"))[-max_files:]
    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as fp:
                for raw in fp:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        row = json.loads(raw)
                    except Exception:
                        continue
                    symbol = _normalize_symbol(row.get("symbol"))
                    if not symbol:
                        continue
                    reports[symbol].append(
                        {
                            "symbol": symbol,
                            "title": _clean_text(row.get("title")),
                            "broker": _clean_text(row.get("broker")),
                            "published_at": _clean_text(row.get("published_at")),
                            "content": _clean_text(row.get("content")),
                            "detail_url": _clean_text(row.get("detail_url")),
                        }
                    )
        except Exception:
            continue
    return {symbol: _sort_reports(items) for symbol, items in reports.items()}


def _extract_pct_pairs(text: str) -> list[tuple[str, float]]:
    pairs: list[tuple[str, float]] = []
    seen: set[tuple[str, int]] = set()
    patterns = [
        r"([A-Za-z가-힣0-9·/()\- ]{2,30})\((?:20\d{2}년 )?(?:매출 )?비중\s*(\d{1,3}(?:\.\d+)?)%\)",
        r"([A-Za-z가-힣0-9·/()\- ]{2,30})[:：]\s*(\d{1,3}(?:\.\d+)?)%",
        r"([A-Za-z가-힣0-9·/()\- ]{2,30})\s+비중\s*(\d{1,3}(?:\.\d+)?)%",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            label = re.sub(r"\s+", " ", _clean_text(match.group(1))).strip(" ,")
            pct = _safe_float(match.group(2), float("nan"))
            if not label or not math.isfinite(pct) or pct <= 0 or pct > 100:
                continue
            key = (label.lower(), round(pct))
            if key in seen:
                continue
            seen.add(key)
            pairs.append((label, pct))
    return pairs


def _looks_like_business_label(label: str) -> bool:
    text = re.sub(r"\s+", " ", _clean_text(label)).strip(" ,")
    if len(text) < 2 or len(text) > 18:
        return False
    if text.count(" ") > 2:
        return False
    if re.search(r"\d{3,}", text):
        return False
    bad_tokens = (
        "이다",
        "있다",
        "합산",
        "주요고객",
        "수주잔고",
        "계약",
        "매출",
        "비중",
        "실적",
        "영업",
        "전망",
        "증가",
        "감소",
    )
    return not any(token in text for token in bad_tokens)


def _extract_business_mix(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    priority_keywords = ("사업부", "부문", "제품", "매출 비중", "전사 매출")
    for report in reports:
        content = _clean_text(report.get("content"))
        if not content:
            continue
        if not any(keyword in content for keyword in priority_keywords):
            continue
        pairs = _extract_pct_pairs(content)
        items: list[dict[str, Any]] = []
        for label, pct in pairs:
            if any(keyword in label for keyword in ("미국", "중국", "유럽", "국내", "해외", "베트남", "일본", "러시아", "북미", "남미", "EMEA", "NA", "MENA")):
                continue
            if not _looks_like_business_label(label):
                continue
            items.append({"label": label, "pct": round(pct, 1)})
        if items:
            return {
                "items": items[:6],
                "as_of": _extract_year_hint(content) or "",
                "source": "analyst_report",
                "source_title": _clean_text(report.get("title")),
            }
    return None


def _extract_geography_mix(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    for report in reports:
        content = _clean_text(report.get("content"))
        if not content:
            continue
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for label, pct in _extract_pct_pairs(content):
            normalized = GEOGRAPHY_KEYWORDS.get(label.strip(), "")
            if not normalized:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            items.append({"label": normalized, "pct": round(pct, 1)})
        overseas_match = re.search(
            r"해외 매출 비중(?:을|은)?\s*(?:20\d{2}년\s*)?(\d{1,3}(?:\.\d+)?)%(?:에서)?(?:\s*(?:20\d{2}년|26년|27년)\s*(\d{1,3}(?:\.\d+)?)%)?",
            content,
        )
        if overseas_match:
            current_pct = _safe_float(overseas_match.group(2) or overseas_match.group(1), float("nan"))
            if math.isfinite(current_pct) and 0 < current_pct <= 100 and "해외" not in seen:
                items.append({"label": "해외", "pct": round(current_pct, 1)})
                seen.add("해외")
                if "국내" not in seen:
                    items.append({"label": "국내", "pct": round(max(0.0, 100.0 - current_pct), 1)})
                    seen.add("국내")
        if items:
            return {
                "items": items[:6],
                "as_of": _extract_year_hint(content) or "",
                "source": "analyst_report",
                "source_title": _clean_text(report.get("title")),
            }
    return None


def _extract_year_hint(text: str) -> str:
    match = re.search(r"(20\d{2})년", text)
    return match.group(1) if match else ""


def _build_metric_rows(row: pd.Series) -> dict[str, dict[str, float | None]]:
    actual_year = _safe_int(row.get("cons_actual_year_raw") or row.get("cons_actual_year"))
    shares = _estimate_shares(row)
    revenue_actual = _safe_float(row.get("cons_revenue_actual_krw_raw"), float("nan"))
    op_actual = _safe_float(row.get("cons_op_actual_krw_raw"), float("nan"))
    net_actual = _safe_float(row.get("cons_net_actual_krw_raw"), float("nan"))
    revenue_fy1 = _safe_float(row.get("cons_revenue_fy1_krw_raw"), float("nan"))
    op_fy1 = _safe_float(row.get("cons_op_fy1_krw_raw"), float("nan"))
    net_fy1 = _safe_float(row.get("cons_net_fy1_krw_raw"), float("nan"))
    revenue_fy0 = _safe_float(row.get("cons_revenue_fy0_krw_raw"), float("nan"))
    op_fy0 = _safe_float(row.get("cons_op_fy0_krw_raw"), float("nan"))
    net_fy0 = _safe_float(row.get("cons_net_fy0_krw_raw"), float("nan"))
    current_price = _safe_float(row.get("current_price"), float("nan"))
    pbr_current = _safe_float(row.get("cons_pbr_raw"), float("nan"))
    roe_current = _safe_float(row.get("cons_roe_raw"), float("nan"))
    psr_current = _safe_float(row.get("cons_psr_raw"), float("nan"))
    op_yield = _safe_float(row.get("cons_op_yield_raw"), float("nan"))
    net_yield = _safe_float(row.get("cons_net_yield_raw"), float("nan"))

    years = {
        "fy_minus_2": actual_year - 2 if actual_year else None,
        "fy_minus_1": actual_year - 1 if actual_year else None,
        "fy0_actual": actual_year if actual_year else None,
        "fy1_estimate": (actual_year + 1) if actual_year else None,
        "fy2_estimate": (actual_year + 2) if actual_year else None,
    }

    actual_eps = _krw_per_share(net_actual if math.isfinite(net_actual) and net_actual > 0 else None, shares)
    fy1_eps = _krw_per_share(net_fy1 if math.isfinite(net_fy1) and net_fy1 > 0 else None, shares)
    actual_sps = _krw_per_share(revenue_actual if math.isfinite(revenue_actual) and revenue_actual > 0 else None, shares)
    fy1_sps = _krw_per_share(revenue_fy1 if math.isfinite(revenue_fy1) and revenue_fy1 > 0 else None, shares)
    actual_op_ps = _krw_per_share(op_actual if math.isfinite(op_actual) and op_actual > 0 else None, shares)
    fy1_op_ps = _krw_per_share(op_fy1 if math.isfinite(op_fy1) and op_fy1 > 0 else None, shares)
    current_bps = current_price / pbr_current if math.isfinite(current_price) and current_price > 0 and math.isfinite(pbr_current) and pbr_current > 0 else None

    return {
        "years": years,
        "rows": {
            "매출": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": revenue_actual if math.isfinite(revenue_actual) and revenue_actual > 0 else None,
                "fy1_estimate": revenue_fy0 if math.isfinite(revenue_fy0) and revenue_fy0 > 0 else None,
                "fy2_estimate": revenue_fy1 if math.isfinite(revenue_fy1) and revenue_fy1 > 0 else None,
                "format": "krw",
            },
            "영업이익": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": op_actual if math.isfinite(op_actual) and op_actual > 0 else None,
                "fy1_estimate": op_fy0 if math.isfinite(op_fy0) and op_fy0 > 0 else None,
                "fy2_estimate": op_fy1 if math.isfinite(op_fy1) and op_fy1 > 0 else None,
                "format": "krw",
            },
            "순이익": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": net_actual if math.isfinite(net_actual) and net_actual > 0 else None,
                "fy1_estimate": net_fy0 if math.isfinite(net_fy0) and net_fy0 > 0 else None,
                "fy2_estimate": net_fy1 if math.isfinite(net_fy1) and net_fy1 > 0 else None,
                "format": "krw",
            },
            "영업이익률": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": _ratio_pct(op_actual if math.isfinite(op_actual) else None, revenue_actual if math.isfinite(revenue_actual) else None),
                "fy1_estimate": _ratio_pct(op_fy0 if math.isfinite(op_fy0) else None, revenue_fy0 if math.isfinite(revenue_fy0) else None),
                "fy2_estimate": _ratio_pct(op_fy1 if math.isfinite(op_fy1) else None, revenue_fy1 if math.isfinite(revenue_fy1) else None),
                "format": "pct",
            },
            "EPS": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": actual_eps,
                "fy1_estimate": _krw_per_share(net_fy0 if math.isfinite(net_fy0) and net_fy0 > 0 else None, shares),
                "fy2_estimate": fy1_eps,
                "format": "krw_ps",
            },
            "BPS": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": current_bps,
                "fy1_estimate": current_bps,
                "fy2_estimate": current_bps,
                "format": "krw_ps",
            },
            "ROE": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": roe_current if math.isfinite(roe_current) and roe_current > 0 else None,
                "fy1_estimate": roe_current if math.isfinite(roe_current) and roe_current > 0 else None,
                "fy2_estimate": roe_current if math.isfinite(roe_current) and roe_current > 0 else None,
                "format": "pct",
            },
            "SPS": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": actual_sps,
                "fy1_estimate": _krw_per_share(revenue_fy0 if math.isfinite(revenue_fy0) and revenue_fy0 > 0 else None, shares),
                "fy2_estimate": fy1_sps,
                "format": "krw_ps",
            },
            "매출 성장률": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": None,
                "fy1_estimate": _growth_pct(
                    revenue_fy0 if math.isfinite(revenue_fy0) and revenue_fy0 > 0 else None,
                    revenue_actual if math.isfinite(revenue_actual) and revenue_actual > 0 else None,
                ),
                "fy2_estimate": _growth_pct(
                    revenue_fy1 if math.isfinite(revenue_fy1) and revenue_fy1 > 0 else None,
                    revenue_fy0 if math.isfinite(revenue_fy0) and revenue_fy0 > 0 else None,
                ),
                "format": "pct",
            },
            "주당영업이익 환산치": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": actual_op_ps,
                "fy1_estimate": _krw_per_share(op_fy0 if math.isfinite(op_fy0) and op_fy0 > 0 else None, shares),
                "fy2_estimate": fy1_op_ps,
                "format": "krw_ps",
            },
            "영업이익 성장률": {
                "fy_minus_2": None,
                "fy_minus_1": None,
                "fy0_actual": None,
                "fy1_estimate": _growth_pct(
                    op_fy0 if math.isfinite(op_fy0) and op_fy0 > 0 else None,
                    op_actual if math.isfinite(op_actual) and op_actual > 0 else None,
                ),
                "fy2_estimate": _growth_pct(
                    op_fy1 if math.isfinite(op_fy1) and op_fy1 > 0 else None,
                    op_fy0 if math.isfinite(op_fy0) and op_fy0 > 0 else None,
                ),
                "format": "pct",
            },
        },
        "context": {
            "revenue_actual": revenue_actual if math.isfinite(revenue_actual) and revenue_actual > 0 else None,
            "revenue_fy0": revenue_fy0 if math.isfinite(revenue_fy0) and revenue_fy0 > 0 else None,
            "revenue_fy1": revenue_fy1 if math.isfinite(revenue_fy1) and revenue_fy1 > 0 else None,
            "op_actual": op_actual if math.isfinite(op_actual) and op_actual > 0 else None,
            "op_fy0": op_fy0 if math.isfinite(op_fy0) and op_fy0 > 0 else None,
            "op_fy1": op_fy1 if math.isfinite(op_fy1) and op_fy1 > 0 else None,
            "net_actual": net_actual if math.isfinite(net_actual) and net_actual > 0 else None,
            "net_fy0": net_fy0 if math.isfinite(net_fy0) and net_fy0 > 0 else None,
            "net_fy1": net_fy1 if math.isfinite(net_fy1) and net_fy1 > 0 else None,
            "roe_current": roe_current if math.isfinite(roe_current) and roe_current > 0 else None,
            "psr_current": psr_current if math.isfinite(psr_current) and psr_current > 0 else None,
            "op_yield": op_yield if math.isfinite(op_yield) and op_yield > 0 else None,
            "net_yield": net_yield if math.isfinite(net_yield) and net_yield > 0 else None,
        },
    }


def _metric_row_to_payload(label: str, values: dict[str, Any]) -> dict[str, Any]:
    return {"label": label, **values}


def _peer_group_key(row: pd.Series) -> str:
    sector = _clean_text(row.get("sector"))
    family = _clean_text(row.get("valuation_family"))
    if sector and sector != "미분류":
        return f"sector:{sector}"
    if family:
        return f"family:{family}"
    return ""


def _peer_group_frame(peer_groups: dict[str, pd.DataFrame], row: pd.Series) -> pd.DataFrame:
    key = _peer_group_key(row)
    if not key:
        return pd.DataFrame()
    return peer_groups.get(key, pd.DataFrame())


def _peer_metric_values(peer_groups: dict[str, pd.DataFrame], row: pd.Series, spec: dict[str, Any]) -> list[float]:
    symbol = _normalize_symbol(row.get("symbol"))
    peers = _peer_group_frame(peer_groups, row)
    values: list[float] = []
    for _, peer in peers.iterrows():
        if _normalize_symbol(peer.get("symbol")) == symbol:
            continue
        current = _calc_multiple_from_spec(peer, spec)
        if current is not None and current > 0:
            values.append(current)
    return values


def _build_multiple_board(peer_groups: dict[str, pd.DataFrame], row: pd.Series) -> list[dict[str, Any]]:
    usage_basis = _clean_text(row.get("valuation_basis_label"))
    rows: list[dict[str, Any]] = []
    for spec in METRIC_BOARD_SPECS:
        current = _calc_multiple_from_spec(row, spec)
        peer_values = _peer_metric_values(peer_groups, row, spec)
        peer_median, peer_q25, peer_q75 = _winsorized_stats(peer_values)
        usage = "계산 불가"
        if current is not None:
            usage = "주력 멀티플" if usage_basis == spec["label"] else "보조 멀티플"
        if usage == "주력 멀티플":
            tp_peer_median = _safe_float(row.get("tp_peer_median_multiple"), float("nan"))
            tp_peer_q25 = _safe_float(row.get("tp_peer_q25_multiple"), float("nan"))
            tp_peer_q75 = _safe_float(row.get("tp_peer_q75_multiple"), float("nan"))
            if math.isfinite(tp_peer_median) and tp_peer_median > 0:
                peer_median = tp_peer_median
            if math.isfinite(tp_peer_q25) and tp_peer_q25 > 0:
                peer_q25 = tp_peer_q25
            if math.isfinite(tp_peer_q75) and tp_peer_q75 > 0:
                peer_q75 = tp_peer_q75
        rows.append(
            {
                "metric": spec["label"],
                "current": round(current, 2) if current is not None else None,
                "peer_median": round(peer_median, 2) if peer_median is not None else None,
                "peer_q25": round(peer_q25, 2) if peer_q25 is not None else None,
                "peer_q75": round(peer_q75, 2) if peer_q75 is not None else None,
                "basis_period": _clean_text(row.get(spec["basis_period_col"])) or "-",
                "input_basis": _format_input_source(row.get(spec["input_source_col"])),
                "usage": usage,
            }
        )
    return rows


def _driver_effect(adjustment_pct: float) -> str:
    if adjustment_pct >= 0.04:
        return "프리미엄"
    if adjustment_pct <= -0.04:
        return "디스카운트"
    return "중립"


def _peer_metric_median(values: list[float | None]) -> float | None:
    valid = [
        float(value)
        for value in values
        if value is not None and isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    return _median(valid) if valid else None


def _build_tp_driver_breakdown(peer_groups: dict[str, pd.DataFrame], row: pd.Series, business_mix: dict[str, Any] | None, geography_mix: dict[str, Any] | None) -> list[dict[str, Any]]:
    _ = peer_groups, business_mix, geography_mix

    subject_growth = _safe_float(row.get("tp_subject_growth_pct"), float("nan"))
    peer_growth = _safe_float(row.get("tp_peer_growth_pct"), float("nan"))
    growth_effect_pct = _safe_float(row.get("tp_growth_adjustment_pct"), float("nan"))

    if _clean_text(row.get("valuation_basis_label")) == "PBR":
        subject_profitability = _safe_float(row.get("tp_subject_profitability_pct"), float("nan"))
        peer_profitability = _safe_float(row.get("tp_peer_profitability_pct"), float("nan"))
        profitability_name = "ROE"
    else:
        subject_profitability = _safe_float(row.get("tp_subject_profitability_pct"), float("nan"))
        peer_profitability = _safe_float(row.get("tp_peer_profitability_pct"), float("nan"))
        profitability_name = "영업이익률"
    profitability_effect_pct = _safe_float(row.get("tp_profitability_adjustment_pct"), float("nan"))

    subject_revision = _safe_float(row.get("tp_subject_revision_pct"), float("nan"))
    peer_revision = _safe_float(row.get("tp_peer_revision_pct"), float("nan"))
    revision_effect_pct = _safe_float(row.get("tp_revision_adjustment_pct"), float("nan"))

    breakdown = [
        {
            "driver": "성장",
            "subject_value": round(subject_growth, 1) if subject_growth is not None and math.isfinite(subject_growth) else None,
            "peer_median": round(peer_growth, 1) if peer_growth is not None and math.isfinite(peer_growth) else None,
            "effect": _driver_effect(growth_effect_pct / 100.0) if math.isfinite(growth_effect_pct) else "중립",
            "adjustment_pct": round(growth_effect_pct, 1) if math.isfinite(growth_effect_pct) else None,
            "explanation": "FY1 매출·영업이익 성장률이 피어 중앙값 대비 어느 쪽에 있는지를 반영합니다.",
        },
        {
            "driver": profitability_name,
            "subject_value": round(subject_profitability, 1) if math.isfinite(subject_profitability) and subject_profitability > 0 else None,
            "peer_median": round(peer_profitability, 1) if peer_profitability is not None and math.isfinite(peer_profitability) else None,
            "effect": _driver_effect(profitability_effect_pct / 100.0) if math.isfinite(profitability_effect_pct) else "중립",
            "adjustment_pct": round(profitability_effect_pct, 1) if math.isfinite(profitability_effect_pct) else None,
            "explanation": f"{profitability_name}이 피어 중앙값보다 높으면 멀티플 프리미엄, 낮으면 디스카운트 요인으로 봅니다.",
        },
        {
            "driver": "리비전",
            "subject_value": round(subject_revision, 1) if math.isfinite(subject_revision) else None,
            "peer_median": round(peer_revision, 1) if peer_revision is not None and math.isfinite(peer_revision) else None,
            "effect": _driver_effect(revision_effect_pct / 100.0) if math.isfinite(revision_effect_pct) else "중립",
            "adjustment_pct": round(revision_effect_pct, 1) if math.isfinite(revision_effect_pct) else None,
            "explanation": "애널 추정치 상향과 공시 보정 반영 정도를 피어 중앙값과 비교합니다.",
        },
    ]
    return breakdown


def build_stock_financial_profile_docs(fair_df: pd.DataFrame) -> list[dict[str, Any]]:
    if fair_df is None or fair_df.empty:
        return []
    work = fair_df.copy()
    work["symbol"] = work["symbol"].astype(str).str.zfill(6)
    eligible_peer_mask = (
        (~work["valuation_proxy_used"].fillna(False).astype(bool))
        & (work["valuation_basis_period"].fillna("").astype(str).isin(["FY1", "FY0", "실제 실적", "실제 실적 + 공시 보정"]))
    )
    eligible_peers = work.loc[eligible_peer_mask].copy()
    eligible_peers["peer_group_key"] = eligible_peers.apply(_peer_group_key, axis=1)
    peer_groups = {
        key: group.drop(columns=["peer_group_key"]).copy()
        for key, group in eligible_peers.groupby("peer_group_key", dropna=False)
        if key
    }
    analyst_map = _load_analyst_reports()
    docs: list[dict[str, Any]] = []
    generated_at = _now_iso()
    for _, row in work.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        reports = analyst_map.get(symbol, [])
        business_mix = _extract_business_mix(reports)
        geography_mix = _extract_geography_mix(reports)
        profile = _build_metric_rows(row)
        multiple_board = _build_multiple_board(peer_groups, row)
        tp_driver_breakdown = _build_tp_driver_breakdown(peer_groups, row, business_mix, geography_mix)
        docs.append(
            {
                "_id": symbol,
                "symbol": symbol,
                "name": _clean_text(row.get("name")),
                "financial_profile": {
                    "actual_years": [
                        year
                        for year in [
                            profile["years"]["fy_minus_2"],
                            profile["years"]["fy_minus_1"],
                            profile["years"]["fy0_actual"],
                        ]
                        if year
                    ],
                    "fy1_year": profile["years"]["fy1_estimate"],
                    "fy2_year": profile["years"]["fy2_estimate"],
                    "column_years": profile["years"],
                    "rows": [_metric_row_to_payload(label, values) for label, values in profile["rows"].items()],
                    "source": {
                        "actual": "actual_financial_snapshot_latest / fair_value_snapshot_latest",
                        "estimate": "wise_report annual consensus",
                    },
                },
                "business_mix": business_mix,
                "geography_mix": geography_mix,
                "multiple_board": multiple_board,
                "tp_driver_breakdown": tp_driver_breakdown,
                "updated_at": generated_at,
            }
        )
    docs.sort(key=lambda item: item["_id"])
    return docs
