# dart_order.py
import re
from typing import Optional, Any
from dataclasses import dataclass

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

try:
    from playwright.sync_api import sync_playwright, Page
except Exception:
    sync_playwright = None
    Page = Any

BASE_URL_DART = "https://dart.fss.or.kr"
ENDPOINT_INFO = "/dsaf001/main.do"


# ---------- parse helpers ----------
def _strip_ws(s: str) -> str:
    return re.sub(r"\s+", "", (s or ""))


def _parse_number(v: str) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "-", "nan"):
        return None
    # (123) -> -123
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    s = s.replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def _unit_from_label(label: str) -> float:
    """
    수주 공시 폼은 보통 라벨에 단위가 붙음:
    - 계약금액(원)
    - 최근매출액(원)
    - 계약금액(백만원) 등
    라벨 기준 단위가 가장 안전함.
    """
    t = _strip_ws(label)
    if "(원)" in t:
        return 1.0
    if "(백만원)" in t:
        return 1_000_000.0
    if "(천만원)" in t:
        return 10_000_000.0
    if "(억원)" in t:
        return 100_000_000.0
    if "(십억원)" in t:
        return 1_000_000_000.0
    if "(조원)" in t:
        return 1_000_000_000_000.0
    return 1.0


# ---------- fetch (playwright) ----------
@dataclass
class OrderDisclosureParsed:
    contract_amount_won: Optional[float]
    recent_sales_won: Optional[float]
    sales_ratio_pct: Optional[float]  # 매출액대비(%)


def _dart_iframe_html_with_page(
    page: Page,
    rcp_no: str,
    *,
    wait_until: str = "domcontentloaded",
    nav_timeout_ms: int = 15000,
    wait_after_main_ms: int = 0,
    view_ms: int = 0,
) -> str:
    """
    DART main -> iframe src -> iframe 페이지 html 반환
    """
    url = f"{BASE_URL_DART}{ENDPOINT_INFO}?rcpNo={rcp_no}"

    page.set_default_timeout(nav_timeout_ms)
    page.set_default_navigation_timeout(nav_timeout_ms)

    # 1) 메인 페이지
    page.goto(url, wait_until=wait_until, timeout=nav_timeout_ms)
    page.wait_for_selector("iframe#ifrm", timeout=nav_timeout_ms)

    if wait_after_main_ms > 0:
        page.wait_for_timeout(wait_after_main_ms)

    src = page.get_attribute("iframe#ifrm", "src")
    if not src:
        # 예외 fallback: content에서 한번 더 찾기
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

    # 2) iframe(본문)
    iframe_url = BASE_URL_DART + src
    page.goto(iframe_url, wait_until=wait_until, timeout=nav_timeout_ms)

    if view_ms > 0:
        page.wait_for_timeout(view_ms)

    return page.content()


def _parse_order_html(html: str) -> OrderDisclosureParsed:
    if BeautifulSoup is None:
        return OrderDisclosureParsed(None, None, None)
    soup = BeautifulSoup(html, "html.parser")

    # [수정] 특정 ID에 의존하지 않고, "계약내역"이나 "계약금액"이 포함된 테이블을 찾음
    target_table = None
    all_tables = soup.find_all("table")
    
    for tbl in all_tables:
        txt = tbl.get_text()
        if "계약금액" in txt and "최근매출액" in txt:
            target_table = tbl
            break
    
    if not target_table:
        #Fallback: 기존 방식
        target_table = soup.find("table", id=re.compile(r"XFormD\d_Form0_Table\d")) or soup.find("table")

    if not target_table:
        return OrderDisclosureParsed(None, None, None)

    contract_amount_won = None
    recent_sales_won = None
    sales_ratio_pct = None

    for tr in target_table.find_all("tr"):
        tds = tr.find_all(["td", "th"]) # th도 포함
        if len(tds) < 2: continue

        # 모든 셀의 텍스트를 공백 없이 합침 (매칭 확률 업)
        full_row_text = _strip_ws(tr.get_text())
        
        # 마지막 셀을 값으로 간주
        value_str = tds[-1].get_text(" ", strip=True)
        label_str = tds[0].get_text(" ", strip=True)

        # 1. 계약금액 추출 (총액 기준)
        if contract_amount_won is None and ("계약금액총액" in full_row_text or "확정계약금액" in full_row_text):
            unit = _unit_from_label(label_str)
            v = _parse_number(value_str)
            if v: contract_amount_won = v * unit

        # 2. 최근 매출액 추출
        if recent_sales_won is None and "최근매출액" in full_row_text:
            unit = _unit_from_label(label_str)
            v = _parse_number(value_str)
            if v: recent_sales_won = v * unit

        # 3. 매출액 대비 비율 (%) 추출
        if sales_ratio_pct is None and "매출액대비" in full_row_text:
            v = _parse_number(value_str)
            if v: sales_ratio_pct = v

    return OrderDisclosureParsed(contract_amount_won, recent_sales_won, sales_ratio_pct)


def fetch_order_fields_playwright(
    rcp_no: str,
    headless: bool = True,
    slow_mo_ms: int = 0,
    view_ms: int = 0,             # 화면으로 보고 싶은 시간(ms)
    wait_after_main_ms: int = 0,  # 메인 페이지 잠깐 보기(ms)
    *,
    # ✅ 재사용 구조
    client: Optional[Any] = None,     # PlaywrightClient (duck-typing)
    page: Optional[Page] = None,
    wait_until: str = "domcontentloaded",
    nav_timeout_ms: int = 15000,
    **kwargs,  # ✅ 예기치 않은 인자 들어와도 안전
) -> OrderDisclosureParsed:
    """
    우선순위:
    1) page 주입
    2) client 주입 (client.page() 컨텍스트로 빌림)
    3) 레거시: 내부에서 sync_playwright launch
    """

    # 1) page 주입
    if page is not None:
        try:
            html = _dart_iframe_html_with_page(
                page,
                rcp_no,
                wait_until=wait_until,
                nav_timeout_ms=nav_timeout_ms,
                wait_after_main_ms=wait_after_main_ms,
                view_ms=view_ms,
            )
            return _parse_order_html(html)
        except Exception:
            return OrderDisclosureParsed(None, None, None)

    # 2) client 주입
    if client is not None:
        try:
            with client.page() as p:
                html = _dart_iframe_html_with_page(
                    p,
                    rcp_no,
                    wait_until=wait_until,
                    nav_timeout_ms=nav_timeout_ms,
                    wait_after_main_ms=wait_after_main_ms,
                    view_ms=view_ms,
                )
                return _parse_order_html(html)
        except Exception:
            return OrderDisclosureParsed(None, None, None)

    # 3) 레거시 단독 모드
    url = f"{BASE_URL_DART}{ENDPOINT_INFO}?rcpNo={rcp_no}"
    try:
        if sync_playwright is None:
            raise RuntimeError("playwright is unavailable")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                slow_mo=slow_mo_ms,
                args=["--start-maximized"] if not headless else None,
            )
            context = browser.new_context()
            p0 = context.new_page()

            p0.set_default_timeout(nav_timeout_ms)
            p0.set_default_navigation_timeout(nav_timeout_ms)

            # 메인
            p0.goto(url, wait_until=wait_until, timeout=nav_timeout_ms)
            p0.wait_for_selector("iframe#ifrm", timeout=nav_timeout_ms)

            if (not headless) and wait_after_main_ms > 0:
                p0.wait_for_timeout(wait_after_main_ms)

            src = p0.get_attribute("iframe#ifrm", "src")
            if not src:
                soup = BeautifulSoup(p0.content(), "html.parser")
                iframe = soup.find("iframe", id="ifrm")
                src = iframe.get("src") if iframe else None

            if not src:
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass
                return OrderDisclosureParsed(None, None, None)

            iframe_url = BASE_URL_DART + src
            p0.goto(iframe_url, wait_until=wait_until, timeout=nav_timeout_ms)

            if (not headless) and view_ms > 0:
                p0.wait_for_timeout(view_ms)

            html = p0.content()

            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

        return _parse_order_html(html)
    except Exception:
        return OrderDisclosureParsed(None, None, None)


# ---------- hit logic ----------
@dataclass
class OrderHitResult:
    hit: bool
    ratio: Optional[float]  # fallback: order/cons_rev
    order_amount: Optional[float]
    consensus_quarter_revenue: Optional[float]
    sales_ratio_pct: Optional[float]  # 공시의 매출액대비(%)
    reason: str


def compute_order_hit_v2(
    order_amount: Optional[float],
    cons_q_rev: Optional[float],
    sales_ratio_pct: Optional[float],
    hit_sales_ratio_threshold_pct: float = 20.0,
    fallback_hit_ratio: float = 0.80,
) -> OrderHitResult:
    # 1) 공시의 "매출액대비(%)"가 있으면 그걸 최우선으로 판정
    if sales_ratio_pct is not None:
        hit = sales_ratio_pct >= hit_sales_ratio_threshold_pct
        reason = (
            f"sales_ratio={sales_ratio_pct:.2f}% (>= {hit_sales_ratio_threshold_pct:.2f}%)"
            if hit else f"sales_ratio={sales_ratio_pct:.2f}%"
        )
        return OrderHitResult(
            hit=hit,
            ratio=None,
            order_amount=order_amount,
            consensus_quarter_revenue=cons_q_rev,
            sales_ratio_pct=sales_ratio_pct,
            reason=reason,
        )

    # 2) 없으면 기존 로직(order/cons_rev)로 fallback
    if not order_amount or not cons_q_rev or cons_q_rev <= 0:
        return OrderHitResult(
            hit=False,
            ratio=None,
            order_amount=order_amount,
            consensus_quarter_revenue=cons_q_rev,
            sales_ratio_pct=sales_ratio_pct,
            reason="insufficient data",
        )

    ratio = order_amount / cons_q_rev
    hit = ratio >= fallback_hit_ratio
    reason = f"order/cons_rev={ratio:.0%} (>= {fallback_hit_ratio:.0%})" if hit else f"order/cons_rev={ratio:.0%}"
    return OrderHitResult(
        hit=hit,
        ratio=ratio,
        order_amount=order_amount,
        consensus_quarter_revenue=cons_q_rev,
        sales_ratio_pct=sales_ratio_pct,
        reason=reason,
    )
