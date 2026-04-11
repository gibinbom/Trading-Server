import io
import re
import pandas as pd
from dataclasses import dataclass
from typing import Optional, Dict, Any

try:
    from playwright.sync_api import sync_playwright, Page
except Exception:
    sync_playwright = None
    Page = Any

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

# (선택) 재사용 클라이언트가 있을 때만 import
try:
    from playwright_client import PlaywrightClient
except Exception:  # pragma: no cover
    PlaywrightClient = Any  # type: ignore


BASE_URL_DART = "https://dart.fss.or.kr"
ENDPOINT_INFO = "/dsaf001/main.do"


class DocumentParseError(Exception):
    pass


def parse_unit(u_string: str) -> int:
    if "백만원" in u_string:
        return 1_000_000
    if "천만원" in u_string:
        return 10_000_000
    if "억원" in u_string:
        return 100_000_000
    if "십억원" in u_string:
        return 1_000_000_000
    if "백억원" in u_string:
        return 10_000_000_000
    if "천억원" in u_string:
        return 100_000_000_000
    if "조원" in u_string:
        return 1_000_000_000_000
    raise ValueError(f"Cannot parse unit from {u_string}")


def parse_cell(v: Any) -> Optional[float]:
    if v is None:
        return None
    v = str(v).strip()
    if v in ("-", ""):
        return None
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]
    v = v.replace(",", "")
    try:
        return float(v)
    except Exception:
        return None


def _norm(s: Any) -> str:
    return str(s).replace("\u00a0", " ").strip()


def _norm_compact(s: Any) -> str:
    return re.sub(r"\s+", "", _norm(s))


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [
            " ".join([_norm(c) for c in col if str(c) != "nan"]).strip() for col in df.columns
        ]
    else:
        df = df.copy()
        df.columns = [_norm(c) for c in df.columns]
    return df


class PerformanceReport:
    """
    DART 잠정실적 공시 화면의 테이블에서
    - 매출액 / 영업이익 / 당기순이익 "당해실적"(혹은 당기) 값을 WON 단위로 저장
    """

    def __init__(self, rcp_no: str, html_text: str):
        self.rcp_no = rcp_no
        self.revenue: Optional[float] = None
        self.operating_income: Optional[float] = None
        self.net_income: Optional[float] = None
        self.unit: Optional[int] = None  # 디버깅용

        try:
            tables = pd.read_html(io.StringIO(str(html_text)))
            if not tables:
                raise ValueError("No table found")

            # 여러 테이블 중 "매출/영업"이 있는 테이블 우선 탐색
            df = None
            for t in tables[::-1]:
                t2 = t.copy()
                text = " ".join(t2.astype(str).fillna("").values.flatten().tolist())
                if ("매출" in text) and ("영업" in text):
                    df = t2
                    break
            if df is None:
                df = tables[-1].copy()

            df = _flatten_columns(df)

            # ---- 단위 탐색 ----
            if BeautifulSoup is not None:
                soup_all = BeautifulSoup(str(html_text), "html.parser")
                all_txt = soup_all.get_text(" ")
            else:
                all_txt = str(html_text)
            compact = re.sub(r"\s+", "", all_txt)

            m = re.search(r"단위[:：]?(백만원|천만원|억원|십억원|백억원|천억원|조원)", compact)
            unit = None
            if m:
                unit = parse_unit(m.group(1))
            else:
                # fallback: 데이터프레임 셀에서 단위 추정
                for _, row in df.iterrows():
                    for cell in row.astype(str).tolist():
                        c = _norm_compact(cell)
                        if "단위" in c:
                            mm = re.search(r"(백만원|천만원|억원|십억원|백억원|천억원|조원)", c)
                            if mm:
                                unit = parse_unit(mm.group(1))
                                break
                    if unit:
                        break

            if unit is None:
                raise ValueError("Cannot identify unit")
            self.unit = unit

            # ---- 테이블 형태 대응 ----
            actual_col = None
            cols_compact = [_norm_compact(c) for c in df.columns]
            for i, c in enumerate(cols_compact):
                if c in ("당해실적", "당기", "당기실적", "금회", "당기실적(잠정)"):
                    actual_col = df.columns[i]
                    break
            if actual_col is None:
                for i, c in enumerate(cols_compact):
                    if ("당해" in c and "실적" in c) or (c == "당기"):
                        actual_col = df.columns[i]
                        break

            account_col = df.columns[0]

            REV_KEYS = {"매출액", "매출"}
            OP_KEYS = {"영업이익", "영업이익(손실)", "영업손익", "영업손익(손실)"}
            NI_KEYS = {"당기순이익", "당기순이익(손실)", "순이익", "당기순손익"}

            REV_KEYS_C = {_norm_compact(k) for k in REV_KEYS}
            OP_KEYS_C = {_norm_compact(k) for k in OP_KEYS}
            NI_KEYS_C = {_norm_compact(k) for k in NI_KEYS}

            def set_metric(acct_compact: str, v: Optional[float]):
                if v is None:
                    return
                if acct_compact in REV_KEYS_C:
                    self.revenue = v
                elif acct_compact in OP_KEYS_C:
                    self.operating_income = v
                elif acct_compact in NI_KEYS_C:
                    self.net_income = v

            if actual_col is not None:
                # ---- wide 형태 ----
                for _, row in df.iterrows():
                    acct = _norm_compact(row.get(account_col, ""))
                    acct = acct.replace(":", "").replace("：", "")
                    v = parse_cell(row.get(actual_col))
                    set_metric(acct, v)
            else:
                # ---- long 형태 ----
                for _, row in df.iterrows():
                    r0 = _norm_compact(row.iloc[0] if len(row) > 0 else "")
                    r1 = _norm_compact(row.iloc[1] if len(row) > 1 else "")
                    val = parse_cell(row.iloc[2] if len(row) > 2 else None)

                    is_actual = (
                        r1 in {"당해실적", "당기", "당기실적", "금회"}
                        or ("당해" in r1 and "실적" in r1)
                    )
                    if is_actual:
                        set_metric(r0, val)

            # 단위 적용(WON)
            if self.revenue is not None:
                self.revenue *= unit
            if self.operating_income is not None:
                self.operating_income *= unit
            if self.net_income is not None:
                self.net_income *= unit

        except Exception as e:
            raise DocumentParseError(str(e)) from e


def _dart_iframe_html_with_page(
    page: Page,
    rcp_no: str,
    *,
    wait_until: str = "domcontentloaded",
    nav_timeout_ms: int = 15000,
) -> str:
    url = f"{BASE_URL_DART}{ENDPOINT_INFO}?rcpNo={rcp_no}"

    page.set_default_timeout(nav_timeout_ms)
    page.set_default_navigation_timeout(nav_timeout_ms)

    page.goto(url, wait_until=wait_until, timeout=nav_timeout_ms)
    page.wait_for_selector("iframe#ifrm", timeout=nav_timeout_ms)

    src = page.get_attribute("iframe#ifrm", "src")
    if not src:
        html = page.content()
        if BeautifulSoup is not None:
            soup = BeautifulSoup(html, "html.parser")
            iframe = soup.find("iframe", id="ifrm")
            src = iframe.get("src") if iframe else None
        else:
            match = re.search(r'<iframe[^>]+id=["\']ifrm["\'][^>]+src=["\']([^"\']+)', html, re.I)
            src = match.group(1) if match else None

    if not src:
        raise RuntimeError("DART iframe not found (ifrm)")

    iframe_url = BASE_URL_DART + src
    page.goto(iframe_url, wait_until=wait_until, timeout=nav_timeout_ms)

    return page.content()


def fetch_perf_report_playwright(
    rcp_no: str,
    headless: bool = True,
    slow_mo_ms: int = 0,
    slow_mo: Optional[int] = None,
    *,
    client: Optional["PlaywrightClient"] = None,
    page: Optional[Page] = None,
    wait_until: str = "domcontentloaded",
    nav_timeout_ms: int = 15000,
    **kwargs,
) -> PerformanceReport:
    if slow_mo is not None:
        slow_mo_ms = slow_mo

    if page is not None:
        iframe_html = _dart_iframe_html_with_page(
            page, rcp_no, wait_until=wait_until, nav_timeout_ms=nav_timeout_ms
        )
        return PerformanceReport(rcp_no, iframe_html)

    if client is not None:
        with client.page() as p:
            iframe_html = _dart_iframe_html_with_page(
                p, rcp_no, wait_until=wait_until, nav_timeout_ms=nav_timeout_ms
            )
            return PerformanceReport(rcp_no, iframe_html)

    if sync_playwright is None:
        raise RuntimeError("playwright is unavailable")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, slow_mo=slow_mo_ms)
        context = browser.new_context()
        p = context.new_page()

        iframe_html = _dart_iframe_html_with_page(
            p, rcp_no, wait_until=wait_until, nav_timeout_ms=nav_timeout_ms
        )

        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

        return PerformanceReport(rcp_no, iframe_html)


# ----------------------------
# Surprise compute (업데이트됨)
# ----------------------------
@dataclass
class SurpriseResult:
    has_signal: bool
    beat: bool
    miss: bool
    reason: str
    metrics: dict
    # [추가됨] engine.py에서 사용하는 비율 필드
    ratio_op: Optional[float] = None
    ratio_ni: Optional[float] = None


def _to_num(x) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return float(x)
    except Exception:
        return None


def _get_threshold_op_pct(thresholds: Any, default_pct: float = 20.0) -> float:
    th = None
    if thresholds is None:
        th = None
    elif isinstance(thresholds, dict):
        th = thresholds.get("op")
    else:
        th = getattr(thresholds, "op", None)

    if th is None:
        return default_pct

    th = float(th)
    return th * 100.0 if th <= 1.0 else th


def compute_surprise(report: PerformanceReport, cons: dict, thresholds) -> SurpriseResult:
    """
    - OP surprise +20% 이상이면 signal
    - metrics에 원천/중간값 포함
    - ratio_op, ratio_ni 계산 추가 (TP 상향 로직용)
    """
    op_actual = _to_num(getattr(report, "operating_income", None))
    rev_actual = _to_num(getattr(report, "revenue", None))
    net_actual = _to_num(getattr(report, "net_income", None))

    # 1. 영업이익(OP) 컨센서스 추출
    op_cons = None
    if cons:
        for k in [
            "op", "operating_income", "operating_profit", "oper_profit",
            "op_won", "operating_profit_won"
        ]:
            if k in cons and cons[k] is not None:
                op_cons = _to_num(cons[k])
                break
    
    # 2. 순이익(NI) 컨센서스 추출 (추가됨)
    net_cons = None
    if cons:
        for k in [
            "ni", "net_income", "net_profit", "net_profit_won", 
            "ni_won", "controlling_interest_net_profit"
        ]:
            if k in cons and cons[k] is not None:
                net_cons = _to_num(cons[k])
                break

    metrics = {
        "op": {
            "actual": op_actual,
            "consensus": op_cons,
            "delta": None,
            "pct": None,
            "basis": "abs(consensus)",
        },
        "rev": {"actual": rev_actual},
        "revenue": {"actual": rev_actual},
        "net": {
            "actual": net_actual,
            "consensus": net_cons,
        },
        "unit": getattr(report, "unit", None),
    }

    # ratio 계산 (Actual / Consensus)
    # engine.py에서 ratio >= 2.0 (200%) 등을 체크하므로 절대값 대비 비율로 계산
    ratio_op = None
    if op_actual is not None and op_cons and op_cons != 0:
        ratio_op = op_actual / abs(op_cons)

    ratio_ni = None
    if net_actual is not None and net_cons and net_cons != 0:
        ratio_ni = net_actual / abs(net_cons)

    if op_actual is None:
        return SurpriseResult(False, False, False, "op actual n/a (skip)", metrics, ratio_op, ratio_ni)
    if op_cons is None or op_cons == 0:
        return SurpriseResult(False, False, False, "op consensus n/a (skip)", metrics, ratio_op, ratio_ni)

    delta = op_actual - op_cons
    pct = (delta / abs(op_cons)) * 100.0
    metrics["op"].update({"delta": delta, "pct": pct})

    TH = _get_threshold_op_pct(thresholds, default_pct=20.0)

    beat = (pct >= TH)
    miss = (pct <= -TH)
    has_signal = beat

    reason = f"op +{pct:.1f}% (>= {TH:.1f}%)" if beat else f"op {pct:.1f}% (< {TH:.1f}%)"

    return SurpriseResult(has_signal, beat, miss, reason, metrics, ratio_op, ratio_ni)
