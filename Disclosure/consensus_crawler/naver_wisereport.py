import datetime
import os
import re
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests
import urllib3

try:
    from config import SETTINGS as _SETTINGS
    if not hasattr(_SETTINGS, "COLLECTION"):
        raise ImportError
    SETTINGS = _SETTINGS
except Exception:
    try:
        from .config import SETTINGS
    except Exception:
        from config import SETTINGS

try:
    from utils import safe_float
except Exception:
    try:
        from .utils import safe_float
    except Exception:
        from utils import safe_float


@dataclass
class Consensus:
    revenue: Optional[float] = None
    operating_profit: Optional[float] = None
    net_profit: Optional[float] = None
    eps: Optional[float] = None
    bps: Optional[float] = None
    per: Optional[float] = None
    pbr: Optional[float] = None
    roe: Optional[float] = None
    revenue_fy0: Optional[float] = None
    operating_profit_fy0: Optional[float] = None
    net_profit_fy0: Optional[float] = None
    eps_fy0: Optional[float] = None
    bps_fy0: Optional[float] = None
    per_fy0: Optional[float] = None
    pbr_fy0: Optional[float] = None
    roe_fy0: Optional[float] = None
    revenue_fy1: Optional[float] = None
    operating_profit_fy1: Optional[float] = None
    net_profit_fy1: Optional[float] = None
    eps_fy1: Optional[float] = None
    bps_fy1: Optional[float] = None
    per_fy1: Optional[float] = None
    pbr_fy1: Optional[float] = None
    roe_fy1: Optional[float] = None
    revenue_actual: Optional[float] = None
    operating_profit_actual: Optional[float] = None
    net_profit_actual: Optional[float] = None
    eps_actual: Optional[float] = None
    bps_actual: Optional[float] = None
    per_actual: Optional[float] = None
    pbr_actual: Optional[float] = None
    roe_actual: Optional[float] = None
    actual_year: Optional[int] = None

    def to_dict(self) -> Dict[str, Optional[float]]:
        return {
            "revenue": self.revenue,
            "operating_profit": self.operating_profit,
            "net_profit": self.net_profit,
            "eps": self.eps,
            "bps": self.bps,
            "per": self.per,
            "pbr": self.pbr,
            "roe": self.roe,
            "revenue_fy0": self.revenue_fy0,
            "operating_profit_fy0": self.operating_profit_fy0,
            "net_profit_fy0": self.net_profit_fy0,
            "eps_fy0": self.eps_fy0,
            "bps_fy0": self.bps_fy0,
            "per_fy0": self.per_fy0,
            "pbr_fy0": self.pbr_fy0,
            "roe_fy0": self.roe_fy0,
            "revenue_fy1": self.revenue_fy1,
            "operating_profit_fy1": self.operating_profit_fy1,
            "net_profit_fy1": self.net_profit_fy1,
            "eps_fy1": self.eps_fy1,
            "bps_fy1": self.bps_fy1,
            "per_fy1": self.per_fy1,
            "pbr_fy1": self.pbr_fy1,
            "roe_fy1": self.roe_fy1,
            "revenue_actual": self.revenue_actual,
            "operating_profit_actual": self.operating_profit_actual,
            "net_profit_actual": self.net_profit_actual,
            "eps_actual": self.eps_actual,
            "bps_actual": self.bps_actual,
            "per_actual": self.per_actual,
            "pbr_actual": self.pbr_actual,
            "roe_actual": self.roe_actual,
            "actual_year": self.actual_year,
        }


_AJAX_URL = "https://navercomp.wisereport.co.kr/company/ajax/c1050001_data.aspx"
_PAGE_URL = "https://navercomp.wisereport.co.kr/company/c1050001.aspx?cmp_cd={stock_code}&cn="
_REQUEST_TIMEOUT_SEC = max(5, int(getattr(SETTINGS, "PLAYWRIGHT_NAV_TIMEOUT_MS", 15000)) // 1000)
_VERIFY_SSL = os.getenv("WISE_REPORT_VERIFY_SSL", "0") == "1"
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

if not _VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _today_ymd() -> str:
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).strftime("%Y%m%d")


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _extract_year(text: str) -> int | None:
    match = re.search(r"(20\d{2})", _clean_text(text))
    return int(match.group(1)) if match else None


def _extract_month(text: str) -> int | None:
    match = re.search(r"\.(\d{2})", _clean_text(text))
    return int(match.group(1)) if match else None


def _row_sort_key(row: Dict[str, Any]) -> tuple[int, int]:
    label = _clean_text(row.get("YYMM"))
    year = _extract_year(label) or 0
    month = _extract_month(label) or 0
    return year, month


def _is_estimate_row(row: Dict[str, Any]) -> bool:
    label = _clean_text(row.get("YYMM")).upper()
    return "(E)" in label or "(F)" in label


def _is_actual_row(row: Dict[str, Any]) -> bool:
    return "(A)" in _clean_text(row.get("YYMM")).upper()


def _row_metric(row: Dict[str, Any], key: str) -> Optional[float]:
    return safe_float(row.get(key))


def _fetch_rows(stock_code: str, *, frq: int) -> list[dict[str, Any]]:
    referer = _PAGE_URL.format(stock_code=stock_code)
    response = requests.get(
        _AJAX_URL,
        params={
            "flag": "2",
            "cmp_cd": stock_code,
            "finGubun": "MAIN",
            "frq": frq,
            "sDT": _today_ymd(),
            "chartType": "svg",
        },
        headers={**_REQUEST_HEADERS, "Referer": referer},
        timeout=_REQUEST_TIMEOUT_SEC,
        verify=_VERIFY_SSL,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("JsonData") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _parse_annual_rows(rows: list[dict[str, Any]]) -> dict[str, Optional[float]]:
    if not rows:
        return {
            "revenue_fy0": None,
            "operating_profit_fy0": None,
            "net_profit_fy0": None,
            "eps_fy0": None,
            "bps_fy0": None,
            "per_fy0": None,
            "pbr_fy0": None,
            "roe_fy0": None,
            "revenue_fy1": None,
            "operating_profit_fy1": None,
            "net_profit_fy1": None,
            "eps_fy1": None,
            "bps_fy1": None,
            "per_fy1": None,
            "pbr_fy1": None,
            "roe_fy1": None,
            "revenue_actual": None,
            "operating_profit_actual": None,
            "net_profit_actual": None,
            "eps_actual": None,
            "bps_actual": None,
            "per_actual": None,
            "pbr_actual": None,
            "roe_actual": None,
            "actual_year": None,
        }

    actual_rows = sorted((row for row in rows if _is_actual_row(row)), key=_row_sort_key)
    estimate_rows = sorted((row for row in rows if _is_estimate_row(row)), key=_row_sort_key)

    latest_actual = actual_rows[-1] if actual_rows else {}
    fy0_row = estimate_rows[0] if estimate_rows else {}
    fy1_row = estimate_rows[1] if len(estimate_rows) > 1 else {}

    return {
        "revenue_fy0": _row_metric(fy0_row, "SALES"),
        "operating_profit_fy0": _row_metric(fy0_row, "OP"),
        "net_profit_fy0": _row_metric(fy0_row, "NP"),
        "eps_fy0": _row_metric(fy0_row, "EPS"),
        "bps_fy0": _row_metric(fy0_row, "BPS"),
        "per_fy0": _row_metric(fy0_row, "PER"),
        "pbr_fy0": _row_metric(fy0_row, "PBR"),
        "roe_fy0": _row_metric(fy0_row, "ROE"),
        "revenue_fy1": _row_metric(fy1_row, "SALES"),
        "operating_profit_fy1": _row_metric(fy1_row, "OP"),
        "net_profit_fy1": _row_metric(fy1_row, "NP"),
        "eps_fy1": _row_metric(fy1_row, "EPS"),
        "bps_fy1": _row_metric(fy1_row, "BPS"),
        "per_fy1": _row_metric(fy1_row, "PER"),
        "pbr_fy1": _row_metric(fy1_row, "PBR"),
        "roe_fy1": _row_metric(fy1_row, "ROE"),
        "revenue_actual": _row_metric(latest_actual, "SALES"),
        "operating_profit_actual": _row_metric(latest_actual, "OP"),
        "net_profit_actual": _row_metric(latest_actual, "NP"),
        "eps_actual": _row_metric(latest_actual, "EPS"),
        "bps_actual": _row_metric(latest_actual, "BPS"),
        "per_actual": _row_metric(latest_actual, "PER"),
        "pbr_actual": _row_metric(latest_actual, "PBR"),
        "roe_actual": _row_metric(latest_actual, "ROE"),
        "actual_year": _extract_year(latest_actual.get("YYMM")),
    }


def _parse_quarter_rows(rows: list[dict[str, Any]]) -> dict[str, Optional[float]]:
    if not rows:
        return {"revenue": None, "operating_profit": None, "net_profit": None}

    estimate_rows = sorted((row for row in rows if _is_estimate_row(row)), key=_row_sort_key)
    actual_rows = sorted((row for row in rows if _is_actual_row(row)), key=_row_sort_key)
    selected = estimate_rows[0] if estimate_rows else (actual_rows[-1] if actual_rows else rows[-1])

    return {
        "revenue": _row_metric(selected, "SALES"),
        "operating_profit": _row_metric(selected, "OP"),
        "net_profit": _row_metric(selected, "NP"),
        "eps": _row_metric(selected, "EPS"),
        "bps": _row_metric(selected, "BPS"),
        "per": _row_metric(selected, "PER"),
        "pbr": _row_metric(selected, "PBR"),
        "roe": _row_metric(selected, "ROE"),
    }


def fetch_quarter_consensus(stock_code: str) -> Consensus:
    annual_rows = _fetch_rows(stock_code, frq=0)
    quarter_rows = _fetch_rows(stock_code, frq=1)

    annual = _parse_annual_rows(annual_rows)
    quarter = _parse_quarter_rows(quarter_rows)

    return Consensus(
        revenue=quarter.get("revenue"),
        operating_profit=quarter.get("operating_profit"),
        net_profit=quarter.get("net_profit"),
        eps=quarter.get("eps"),
        bps=quarter.get("bps"),
        per=quarter.get("per"),
        pbr=quarter.get("pbr"),
        roe=quarter.get("roe"),
        revenue_fy0=annual.get("revenue_fy0"),
        operating_profit_fy0=annual.get("operating_profit_fy0"),
        net_profit_fy0=annual.get("net_profit_fy0"),
        eps_fy0=annual.get("eps_fy0"),
        bps_fy0=annual.get("bps_fy0"),
        per_fy0=annual.get("per_fy0"),
        pbr_fy0=annual.get("pbr_fy0"),
        roe_fy0=annual.get("roe_fy0"),
        revenue_fy1=annual.get("revenue_fy1"),
        operating_profit_fy1=annual.get("operating_profit_fy1"),
        net_profit_fy1=annual.get("net_profit_fy1"),
        eps_fy1=annual.get("eps_fy1"),
        bps_fy1=annual.get("bps_fy1"),
        per_fy1=annual.get("per_fy1"),
        pbr_fy1=annual.get("pbr_fy1"),
        roe_fy1=annual.get("roe_fy1"),
        revenue_actual=annual.get("revenue_actual"),
        operating_profit_actual=annual.get("operating_profit_actual"),
        net_profit_actual=annual.get("net_profit_actual"),
        eps_actual=annual.get("eps_actual"),
        bps_actual=annual.get("bps_actual"),
        per_actual=annual.get("per_actual"),
        pbr_actual=annual.get("pbr_actual"),
        roe_actual=annual.get("roe_actual"),
        actual_year=annual.get("actual_year"),
    )
