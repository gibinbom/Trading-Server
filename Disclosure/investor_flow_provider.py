from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("investor.flow")

_NAVER_HEADERS = {"User-Agent": "Mozilla/5.0"}
_KIS_TR_ID = "FHKST01010900"
_KIS_PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"
_KIS_FAILURE_STREAK_TO_OPEN = 5
_SOURCE_CONFIDENCE = {
    "kis": 1.0,
    "naver_volume_fallback": 0.72,
    "missing": 0.0,
}
_kis_failure_streak = 0
_kis_circuit_open = False
_kis_circuit_logged = False


def _safe_int(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    text = text.replace(",", "").replace("−", "-").replace("–", "-")
    digits = re.sub(r"[^0-9-]", "", text)
    if digits in {"", "-"}:
        return 0
    try:
        return int(digits)
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    text = text.replace(",", "").replace("%", "").replace("−", "-").replace("–", "-")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return 0.0
    try:
        return float(match.group(0))
    except Exception:
        return 0.0


def _signed_number(text: Any) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    sign = -1 if any(marker in raw for marker in ("하락", "-", "▼")) else 1
    value = _safe_int(raw)
    return -abs(value) if sign < 0 else abs(value)


def _to_eok_from_kis(pbmn_value: Any) -> int:
    # KIS 투자자 대금 필드는 백만원 단위여서 100으로 나누면 억원으로 정리됩니다.
    return _safe_int(pbmn_value) // 100


def _to_eok_from_close_volume(close_price: int, net_volume: int) -> int:
    approx_krw = int(close_price) * int(net_volume)
    return int(approx_krw / 100_000_000)


def _sort_days_desc(days: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(days, key=lambda row: str(row.get("date") or ""), reverse=True)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _flow_confidence_label(score: float) -> str:
    if score >= 0.9:
        return "높음"
    if score >= 0.7:
        return "보통"
    if score > 0:
        return "낮음"
    return "없음"


def estimate_investor_flow_confidence(source: str, *, day_count: int, target_days: int) -> Dict[str, Any]:
    source_key = str(source or "missing")
    source_confidence = _SOURCE_CONFIDENCE.get(source_key, 0.0)
    denominator = max(1, int(target_days or 1))
    coverage_ratio = _clamp(float(day_count) / float(denominator))
    confidence_score = round(_clamp(source_confidence * coverage_ratio), 3)
    return {
        "source_confidence": round(source_confidence, 3),
        "coverage_ratio": round(coverage_ratio, 3),
        "confidence_score": confidence_score,
        "confidence_label": _flow_confidence_label(confidence_score),
    }


def _record_kis_success() -> None:
    global _kis_failure_streak
    _kis_failure_streak = 0


def _record_kis_failure(exc: Exception) -> None:
    global _kis_failure_streak, _kis_circuit_open, _kis_circuit_logged
    _kis_failure_streak += 1
    if _kis_failure_streak >= _KIS_FAILURE_STREAK_TO_OPEN:
        _kis_circuit_open = True
        if not _kis_circuit_logged:
            log.warning(
                "KIS 투자자 API가 %d회 연속 실패해 이번 실행에서는 fallback 중심으로 전환합니다. last=%s",
                _kis_failure_streak,
                str(exc)[:160],
            )
            _kis_circuit_logged = True


def _fetch_kis_days(broker, symbol: str, max_days: int = 5) -> List[Dict[str, Any]]:
    url = f"{broker.base}{_KIS_PATH}"
    resp = broker._call_api_with_retry(
        "GET",
        url,
        _KIS_TR_ID,
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": str(symbol).zfill(6),
        },
    )
    raw = resp.json().get("output", []) or []
    days: List[Dict[str, Any]] = []
    for row in raw:
        if row.get("frgn_ntby_tr_pbmn") in (None, ""):
            continue
        close_price = _safe_int(row.get("stck_clpr"))
        change_abs = _safe_int(row.get("prdy_vrss"))
        prev_close = close_price - change_abs
        rate = round((change_abs / prev_close) * 100, 2) if prev_close > 0 else 0.0
        days.append(
            {
                "date": str(row.get("stck_bsop_date") or "").replace("/", "").replace(".", ""),
                "close_price": close_price,
                "change_abs": change_abs,
                "rate": rate,
                "foreign_eok": _to_eok_from_kis(row.get("frgn_ntby_tr_pbmn")),
                "inst_eok": _to_eok_from_kis(row.get("orgn_ntby_tr_pbmn")),
                "retail_eok": _to_eok_from_kis(row.get("prsn_ntby_tr_pbmn")),
                "source": "kis",
            }
        )
    days = _sort_days_desc(days)
    return days[:max_days]


def _find_naver_investor_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        summary = str(table.get("summary") or "")
        caption = table.find("caption")
        caption_text = caption.get_text(" ", strip=True) if caption else ""
        if "외국인 기관 순매매 거래량" in summary or "외국인 기관 순매매 거래량" in caption_text:
            return table
    return None


def _fetch_naver_days(symbol: str, max_days: int = 5, timeout_sec: float = 10.0) -> List[Dict[str, Any]]:
    url = f"https://finance.naver.com/item/frgn.naver?code={str(symbol).zfill(6)}"
    resp = requests.get(url, headers=_NAVER_HEADERS, timeout=timeout_sec)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    table = _find_naver_investor_table(soup)
    if table is None:
        raise ValueError("Naver investor table missing")

    days: List[Dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) != 9:
            continue
        close_price = _safe_int(cells[1])
        change_abs = _signed_number(cells[2])
        rate = _safe_float(cells[3])
        inst_qty = _signed_number(cells[5])
        foreign_qty = _signed_number(cells[6])
        days.append(
            {
                "date": cells[0].replace(".", ""),
                "close_price": close_price,
                "change_abs": change_abs,
                "rate": rate,
                "foreign_eok": _to_eok_from_close_volume(close_price, foreign_qty),
                "inst_eok": _to_eok_from_close_volume(close_price, inst_qty),
                "retail_eok": _to_eok_from_close_volume(close_price, -(foreign_qty + inst_qty)),
                "source": "naver_volume_fallback",
            }
        )
        if len(days) >= max_days:
            break
    if not days:
        raise ValueError("Naver investor rows missing")
    return _sort_days_desc(days)[:max_days]


def fetch_recent_investor_days(broker, symbol: str, max_days: int = 5) -> Dict[str, Any]:
    errors: List[str] = []

    if broker is not None and not _kis_circuit_open:
        try:
            days = _fetch_kis_days(broker, symbol, max_days=max_days)
            if days:
                _record_kis_success()
                confidence = estimate_investor_flow_confidence("kis", day_count=len(days), target_days=max_days)
                return {
                    "symbol": str(symbol).zfill(6),
                    "source": "kis",
                    "fallback_used": False,
                    "days": days,
                    "errors": [],
                    **confidence,
                }
        except Exception as exc:
            _record_kis_failure(exc)
            errors.append(f"kis:{str(exc)[:160]}")
    elif broker is not None and _kis_circuit_open:
        errors.append("kis:skipped_by_circuit_breaker")

    try:
        days = _fetch_naver_days(symbol, max_days=max_days)
        confidence = estimate_investor_flow_confidence("naver_volume_fallback", day_count=len(days), target_days=max_days)
        return {
            "symbol": str(symbol).zfill(6),
            "source": "naver_volume_fallback",
            "fallback_used": True,
            "days": days,
            "errors": errors,
            **confidence,
        }
    except Exception as exc:
        errors.append(f"naver:{str(exc)[:160]}")

    confidence = estimate_investor_flow_confidence("missing", day_count=0, target_days=max_days)
    return {
        "symbol": str(symbol).zfill(6),
        "source": "missing",
        "fallback_used": False,
        "days": [],
        "errors": errors,
        **confidence,
    }
