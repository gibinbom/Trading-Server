import re
from typing import Optional, Dict, Any, List, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from bs4 import BeautifulSoup

DART_MAIN = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"

# ---------------------------------
# Parsing helpers
# ---------------------------------
def _parse_pct(text: str) -> Optional[float]:
    """'34.5%', '(34.5)', '△34.5', '▼ 34.5' 등 다양한 표기를 float로 통일."""
    if not text:
        return None
    t = text.strip()
    if t in ("-", "—", "N/A", "NA", ""):
        return None

    neg = False
    if "(" in t and ")" in t:
        neg = True
    if any(ch in t for ch in ["▼", "▽", "−", "-"]):
        neg = True

    num = re.sub(r"[^\d.,]", "", t)  # 숫자/점/콤마만 남김
    if not num:
        return None

    num = num.replace(",", "")
    try:
        v = float(num)
    except ValueError:
        return None

    return -v if neg else v


def _parse_amount(text: str) -> Optional[float]:
    """
    금액/수치 파서:
    - '1,234', '(1,234)', '△1,234', '▼ 1,234', '-'
    - 단위/공백/기호 섞여 있어도 최대한 숫자만 뽑음
    """
    if not text:
        return None
    t = text.strip()
    if t in ("-", "—", "N/A", "NA", ""):
        return None

    neg = False
    if "(" in t and ")" in t:
        neg = True
    if any(ch in t for ch in ["▼", "▽", "−", "-"]):
        neg = True

    # 숫자/점/콤마만 남기기
    num = re.sub(r"[^\d.,]", "", t)
    if not num:
        return None

    num = num.replace(",", "")
    try:
        v = float(num)
    except ValueError:
        return None

    return -v if neg else v


def _is_numberish(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"\d", text))


def _find_change_col(headers: List[str]) -> Optional[int]:
    """헤더에서 '증감률/증감비율/변동률/%' 같은 칼럼 인덱스를 찾아준다."""
    norm = [re.sub(r"\s+", "", h or "") for h in headers]

    for i, h in enumerate(norm):
        if "증감률" in h or "증감비율" in h or "변동률" in h:
            return i

    for i, h in enumerate(headers):
        if "%" in (h or ""):
            return i

    return None


def _find_value_cols(headers: List[str]) -> Tuple[Optional[int], Optional[int]]:
    """
    헤더에서 '당기/금기/현재' vs '전기/전년/직전' 칼럼을 추정.
    못 찾으면 (None, None) 반환하고 row 기반 fallback으로 해결.
    """
    norm = [re.sub(r"\s+", "", (h or "")) for h in headers]

    curr_keys = ("당기", "금기", "현재", "이번", "당해")
    prev_keys = ("전기", "전년", "직전", "이전", "전년동기")

    curr_idx = None
    prev_idx = None

    for i, h in enumerate(norm):
        if any(k in h for k in curr_keys):
            curr_idx = i
            break

    # prev는 '전년동기' 같이 긴 게 더 정확하니 우선순위
    for i, h in enumerate(norm):
        if "전년동기" in h:
            prev_idx = i
            break
    if prev_idx is None:
        for i, h in enumerate(norm):
            if any(k in h for k in prev_keys):
                prev_idx = i
                break

    return curr_idx, prev_idx


TARGET_PATTERNS = {
    "Sales": ["매출액", "매출"],
    "OperatingProfit": ["영업이익", "영업손실", "영업이익(손실)"],
    "NetIncome": ["당기순이익", "당기순손실", "순이익", "순손실"],
}


def _fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "-"
    # 소수점이 사실상 없는 경우 정수처럼
    if abs(x - round(x)) < 1e-9:
        return f"{int(round(x)):,}"
    return f"{x:,.2f}"


def parse_pl_variation(html: str, threshold_pct: float = 30.0) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    results = {"Signal": False, "Details": []}

    tables = soup.find_all("table")
    for t_i, table in enumerate(tables):
        # 헤더 추출(첫 tr 기준)
        headers = []
        first_tr = table.find("tr")
        if first_tr:
            header_cells = first_tr.find_all(["th", "td"])
            headers = [c.get_text(" ", strip=True) for c in header_cells]

        change_idx = _find_change_col(headers)
        curr_idx_h, prev_idx_h = _find_value_cols(headers)

        for r_i, tr in enumerate(table.find_all("tr")):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue

            row_texts = [c.get_text(" ", strip=True) for c in cells]
            label = row_texts[0] or ""
            if not label:
                continue

            matched_item = None
            for item, pats in TARGET_PATTERNS.items():
                if any(p in label for p in pats):
                    matched_item = item
                    break
            if not matched_item:
                continue

            # 1) 문서에 있는 증감률(%) 읽기 (있으면 우선)
            ratio_idx = change_idx if change_idx is not None else (len(row_texts) - 1)
            ratio_doc = None
            ratio_text = None
            if 0 <= ratio_idx < len(row_texts):
                ratio_text = row_texts[ratio_idx]
                ratio_doc = _parse_pct(ratio_text)

            # 2) 내부 값(당기/전기) 파싱
            curr_val = None
            prev_val = None
            used_curr_idx = None
            used_prev_idx = None

            # (A) 헤더 기반 인덱스가 있으면 그걸 사용
            if curr_idx_h is not None and 0 <= curr_idx_h < len(row_texts):
                v = _parse_amount(row_texts[curr_idx_h])
                if v is not None:
                    curr_val = v
                    used_curr_idx = curr_idx_h

            if prev_idx_h is not None and 0 <= prev_idx_h < len(row_texts):
                v = _parse_amount(row_texts[prev_idx_h])
                if v is not None:
                    prev_val = v
                    used_prev_idx = prev_idx_h

            # (B) 부족하면 row에서 숫자처럼 보이는 칼럼 2개를 fallback으로 뽑기
            if curr_val is None or prev_val is None:
                candidates = []
                for i in range(1, len(row_texts)):  # 0은 label
                    if change_idx is not None and i == change_idx:
                        continue
                    txt = row_texts[i]
                    if not _is_numberish(txt):
                        continue
                    v = _parse_amount(txt)
                    if v is None:
                        continue
                    candidates.append((i, v))

                # 보통: [당기, 전기, ...] 순서라서 앞의 두 개를 사용
                if len(candidates) >= 2:
                    if curr_val is None:
                        used_curr_idx, curr_val = candidates[0]
                    if prev_val is None:
                        used_prev_idx, prev_val = candidates[1]

            # 3) 계산 증감률
            calc_ratio = None
            calc_formula = None
            if curr_val is not None and prev_val is not None:
                if prev_val != 0:
                    # ✅ 음수 전기값에 대한 분모 부호 문제를 피하려고 abs(prev) 기준으로 계산
                    calc_ratio = ((curr_val - prev_val) / abs(prev_val)) * 100.0
                    calc_formula = "(curr - prev) / abs(prev) * 100"
                else:
                    calc_ratio = None
                    calc_formula = "prev==0 -> N/A"

            # 4) hit 판정: 문서 ratio가 있으면 그걸, 없으면 calc_ratio 사용
            effective_ratio = ratio_doc if ratio_doc is not None else calc_ratio
            ratio_source = "doc" if ratio_doc is not None else ("calc" if calc_ratio is not None else "none")
            hit = (effective_ratio is not None) and (abs(effective_ratio) >= float(threshold_pct))

            results["Details"].append({
                "item": matched_item,
                "label": label,

                # 문서 제공 증감률
                "ratio_doc": ratio_doc,
                "ratio_text": ratio_text,

                # 내부 값
                "current": curr_val,
                "previous": prev_val,
                "current_col_idx": used_curr_idx,
                "previous_col_idx": used_prev_idx,

                # 계산 증감률
                "ratio_calc": calc_ratio,
                "calc_formula": calc_formula,

                # 판정용
                "ratio_effective": effective_ratio,
                "ratio_source": ratio_source,
                "is_hit": hit,

                # 디버깅(어느 테이블/행에서 잡혔는지)
                "table_index": t_i,
                "row_index": r_i,
            })

            if hit:
                results["Signal"] = True

    return results


# ---------------------------------
# Playwright fetch
# ---------------------------------
def fetch_dart_iframe_html(
    url_or_rcp_no: str,
    *,
    headless: bool = True,
    timeout_ms: int = 20000,
    block_resources: bool = False,
) -> str:
    m = re.search(r"rcpNo=(\d+)", url_or_rcp_no)
    rcp_no = m.group(1) if m else (url_or_rcp_no if url_or_rcp_no.isdigit() else None)
    url = url_or_rcp_no if url_or_rcp_no.startswith("http") else DART_MAIN.format(rcp_no=rcp_no)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            viewport={"width": 1365, "height": 768},
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        page.set_default_navigation_timeout(timeout_ms)
        page.on("dialog", lambda d: d.accept())

        if block_resources:
            def _route(route):
                if route.request.resource_type in ("image", "font", "media"):
                    return route.abort()
                return route.continue_()
            page.route("**/*", _route)

        page.goto(url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PWTimeoutError:
            pass

        iframe_el = page.wait_for_selector("#ifrm", timeout=timeout_ms)
        frame = iframe_el.content_frame()
        if frame is None:
            frame = next((f for f in page.frames if f.name == "ifrm"), None)
        if frame is None:
            browser.close()
            raise RuntimeError("DART 본문 iframe(ifrm) 접근 실패")

        try:
            frame.wait_for_function(
                """() => {
                    const t = document.body ? (document.body.innerText || "") : "";
                    return !!document.querySelector("table")
                        || /증감(률|비율)/.test(t)
                        || /(매출액|영업이익|당기순이익)/.test(t);
                }""",
                timeout=timeout_ms
            )
        except PWTimeoutError:
            pass

        html = frame.content()
        browser.close()
        return html


def fetch_and_analyze_dart(url_or_rcp_no: str, threshold_pct: float = 30.0, headless: bool = True) -> Dict[str, Any]:
    html = fetch_dart_iframe_html(url_or_rcp_no, headless=headless)
    return parse_pl_variation(html, threshold_pct=threshold_pct)


# ---------------------------------
# Main
# ---------------------------------
if __name__ == "__main__":
    target = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260129900133"
    res = fetch_and_analyze_dart(target, threshold_pct=30.0, headless=True)

    print("\n" + "=" * 72)
    print("                     [분석 결과 리포트]                     ")
    print("=" * 72)

    for item in res["Details"]:
        mark = "🔴 변동" if item["is_hit"] else "⚪ 일반"

        # ratio 표시: 문서값 + 계산값 같이
        doc_r = item["ratio_doc"]
        cal_r = item["ratio_calc"]
        eff_r = item["ratio_effective"]
        src = item["ratio_source"]

        doc_txt = f"{doc_r:,.2f}%" if doc_r is not None else "-"
        cal_txt = f"{cal_r:,.2f}%" if cal_r is not None else "-"
        eff_txt = f"{eff_r:,.2f}%" if eff_r is not None else "-"

        print(
            f"{mark} | {item['item']:<16} | "
            f"당기={_fmt_num(item['current']):>14}  전기={_fmt_num(item['previous']):>14} | "
            f"doc={doc_txt:>10}  calc={cal_txt:>10}  eff={eff_txt:>10} ({src}) | "
            f"({item['label']})"
        )

    print("-" * 72)
    if res["Signal"]:
        print(">>> [TRADING BOT] 조건 만족! (|변동률| 30% 이상)")
    else:
        print(">>> [TRADING BOT] 조건 미달.")