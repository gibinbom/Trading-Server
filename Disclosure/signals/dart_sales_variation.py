# signals/dart_sales_variation.py
from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List, Tuple

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

DART_MAIN = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"


# -----------------------------------------------------------------------------
# Title filter
# -----------------------------------------------------------------------------
def is_sales_variation_report_title(title: str) -> bool:
    if not title:
        return False
    t = title.strip().replace("％", "%")
    t = re.sub(r"\s+", "", t)
    t = re.sub(r"[\[\]\(\)\{\}]", "", t)

    return (
        (("매출액" in t) or ("손익구조" in t))
        and (("30%" in t) or ("15%" in t) or ("30" in t and "%" in t) or ("대규모법인" in t))
        and (("변동" in t) or ("변경" in t))
    )


# -----------------------------------------------------------------------------
# Result types
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class SalesVariationDetail:
    item: str
    label: str
    ratio_doc: Optional[float]
    ratio_text: Optional[str]
    current: Optional[float]
    previous: Optional[float]
    current_col_idx: Optional[int]
    previous_col_idx: Optional[int]
    ratio_calc: Optional[float]
    calc_formula: Optional[str]
    ratio_effective: Optional[float]
    ratio_source: str
    is_vol_hit: bool
    is_buy_hit: bool  # 개별 항목 기준(단순 30% 등)
    table_index: int
    row_index: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SalesVariationSignalResult:
    buy_signal: bool       # ✅ 최종 전략 조건(A or B) 만족 여부
    threshold_pct: float
    unit_name: str
    details: List[SalesVariationDetail]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "BuySignal": self.buy_signal,
            "threshold_pct": self.threshold_pct,
            "Unit": self.unit_name,
            "Details": [d.to_dict() for d in self.details],
        }


# -----------------------------------------------------------------------------
# Parsing helpers
# -----------------------------------------------------------------------------
def _parse_unit_multiplier(text: str) -> Tuple[int, str]:
    if not text: return 1, ""
    t = text.replace(" ", "")
    if "조원" in t: return 1_000_000_000_000, "조원"
    if "천억원" in t: return 100_000_000_000, "천억원"
    if "백억원" in t: return 10_000_000_000, "백억원"
    if "십억원" in t: return 1_000_000_000, "십억원"
    if "억원" in t: return 100_000_000, "억원"
    if "천만원" in t: return 10_000_000, "천만원"
    if "백만원" in t: return 1_000_000, "백만원"
    if "천원" in t: return 1_000, "천원"
    if "원" in t: return 1, "원"
    return 1, "Unknown"

def _parse_pct(text: str) -> Optional[float]:
    if not text: return None
    t = text.strip()
    if t in ("-", "—", "N/A", "NA", ""): return None
    
    neg = False
    if "(" in t and ")" in t: neg = True
    if any(ch in t for ch in ["▼", "▽", "−"]): neg = True
    
    num = re.sub(r"[^\d.\-]", "", t)
    if not num: return None
    
    if num.count('-') > 1 or (num.count('-') == 1 and not num.startswith('-')):
         num = num.replace('-', '') 

    try:
        v = float(num)
        return -abs(v) if neg else v
    except ValueError:
        return None

def _parse_amount(text: str) -> Optional[float]:
    if not text: return None
    t = text.strip()
    if t in ("-", "—", "N/A", "NA", ""): return None

    neg = False
    if "(" in t and ")" in t: neg = True
    if any(ch in t for ch in ["▼", "▽", "−", "-"]): neg = True

    num = re.sub(r"[^\d.,]", "", t)
    if not num: return None
    num = num.replace(",", "")
    
    try:
        v = float(num)
        return -v if neg else v
    except ValueError:
        return None

def _find_change_col(headers: List[str]) -> Optional[int]:
    norm = [re.sub(r"\s+", "", h or "") for h in headers]
    for i, h in enumerate(norm):
        if ("증감" in h or "변동" in h) and ("율" in h or "비" in h): return i
    candidates = [i for i, h in enumerate(headers) if "%" in (h or "")]
    return candidates[-1] if candidates else None

def _find_value_cols(headers: List[str]) -> Tuple[Optional[int], Optional[int]]:
    norm = [re.sub(r"\s+", "", (h or "")) for h in headers]
    curr_keys = ("당기", "금기", "현재", "이번", "당해", "금회")
    prev_keys = ("전기", "전년", "직전", "이전")

    curr_idx, prev_idx = None, None

    for i, h in enumerate(norm):
        if any(k in h for k in curr_keys):
            curr_idx = i
            break
    
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

# -----------------------------------------------------------------------------
# Main Parser
# -----------------------------------------------------------------------------
def parse_sales_variation_html(html: str, threshold_pct: float = 30.0) -> SalesVariationSignalResult:
    if BeautifulSoup is None:
        raise RuntimeError("bs4 is unavailable for sales-variation parsing")
    soup = BeautifulSoup(html, "html.parser")
    details: List[SalesVariationDetail] = []
    
    all_text = soup.get_text()
    unit_mult, unit_name = 1, ""
    
    u_match = re.search(r"단위[:\s]*([천백십]*[만억조]?원)", all_text.replace(" ", ""))
    if u_match:
        unit_mult, unit_name = _parse_unit_multiplier(u_match.group(1))

    tables = soup.find_all("table")
    for t_i, table in enumerate(tables):
        rows = table.find_all("tr")
        if not rows: continue

        header_row_idx = -1
        headers = []
        for i in range(min(5, len(rows))):
            cells = rows[i].find_all(["th", "td"])
            row_txt = "".join([c.get_text() for c in cells])
            if ("당기" in row_txt or "당해" in row_txt) and ("전기" in row_txt or "직전" in row_txt):
                header_row_idx = i
                headers = [c.get_text(" ", strip=True) for c in cells]
                break
        
        if header_row_idx == -1: continue

        change_idx = _find_change_col(headers)
        curr_idx_h, prev_idx_h = _find_value_cols(headers)
        if curr_idx_h is None or prev_idx_h is None: continue

        for r_i in range(header_row_idx + 1, len(rows)):
            tr = rows[r_i]
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2: continue

            row_texts = [c.get_text(" ", strip=True) for c in cells]
            label = row_texts[0] or "" 
            
            matched_item = None
            for item, pats in TARGET_PATTERNS.items():
                if any(p in label for p in pats):
                    matched_item = item
                    break
            if not matched_item: continue

            ratio_doc = None
            ratio_text = None
            if change_idx is not None and change_idx < len(row_texts):
                ratio_text = row_texts[change_idx]
                ratio_doc = _parse_pct(ratio_text)

            curr_val, prev_val = None, None
            if curr_idx_h < len(row_texts):
                v = _parse_amount(row_texts[curr_idx_h])
                if v is not None: curr_val = v * unit_mult
            if prev_idx_h < len(row_texts):
                v = _parse_amount(row_texts[prev_idx_h])
                if v is not None: prev_val = v * unit_mult

            calc_ratio = None
            calc_formula = None
            if curr_val is not None and prev_val is not None and prev_val != 0:
                calc_ratio = ((curr_val - prev_val) / abs(prev_val)) * 100.0
                calc_formula = "calc"
            
            effective_ratio = None
            ratio_source = "none"
            if ratio_doc is not None:
                if abs(ratio_doc) > 100000 and calc_ratio is not None:
                    effective_ratio = calc_ratio
                    ratio_source = "calc (doc_abnormal)"
                else:
                    effective_ratio = ratio_doc
                    ratio_source = "doc"
            elif calc_ratio is not None:
                effective_ratio = calc_ratio
                ratio_source = "calc"

            # 1차 단순 필터 (threshold_pct 넘는지)
            is_vol_hit = (effective_ratio is not None) and (abs(effective_ratio) >= float(threshold_pct))
            is_buy_hit = False # 여기서는 일단 False, 아래에서 최종 판단

            details.append(SalesVariationDetail(
                item=matched_item,
                label=label,
                ratio_doc=ratio_doc,
                ratio_text=ratio_text,
                current=curr_val,
                previous=prev_val,
                current_col_idx=curr_idx_h,
                previous_col_idx=prev_idx_h,
                ratio_calc=calc_ratio,
                calc_formula=calc_formula,
                ratio_effective=effective_ratio,
                ratio_source=ratio_source,
                is_vol_hit=is_vol_hit,
                is_buy_hit=is_buy_hit,
                table_index=t_i,
                row_index=r_i,
            ))

    # =========================================================================
    # ✅ [CORE] 최종 매수 신호(Buy Signal) 판단 로직 (전체 데이터 종합)
    # =========================================================================
    final_buy_signal = False
    
    # 데이터 추출
    sales_r = -999.0
    op_r = -999.0
    
    # 여러 테이블에 걸쳐 있을 수 있으므로 가장 큰 값을 우선으로 함
    for d in details:
        if d.ratio_effective is None: continue
        
        # 영업이익/매출액의 현재값이 없거나 0 이하면(적자 등) 제외
        # (단, 흑자전환 등으로 인해 ratio가 양수면 괜찮음)
        if d.current is not None and d.current <= 0:
             # 적자 상태면 성장률이 좋아도 일단 보류 (상황에 따라 다름)
             # 여기서는 '현재 영업이익 적자'면 매수 제외
             if d.item == "OperatingProfit":
                 continue

        if d.item == "Sales":
            sales_r = max(sales_r, d.ratio_effective)
        elif d.item == "OperatingProfit":
            op_r = max(op_r, d.ratio_effective)

    # -----------------------------------------------------------
    # [조건] 
    # A: 매출액 100% 이상 AND 영업이익 10% 이상 (동반 성장)
    # B: 영업이익 50% 이상 (이익 급등)
    # -----------------------------------------------------------
    cond_a = (sales_r >= 100.0 and op_r >= 30.0)
    cond_b = (op_r >= 50.0)
    
    if cond_a or cond_b:
        final_buy_signal = True

    return SalesVariationSignalResult(
        buy_signal=final_buy_signal,
        threshold_pct=float(threshold_pct),
        unit_name=unit_name,
        details=details,
    )

def fetch_dart_iframe_html_with_page(rcp_no: str, *, page, nav_timeout_ms=15000, timeout_ms=20000) -> str:
    url = DART_MAIN.format(rcp_no=rcp_no)
    page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
    try: page.wait_for_selector("#ifrm", timeout=timeout_ms)
    except: pass
    try:
        iframe_src = page.get_attribute("iframe#ifrm", "src")
        if iframe_src:
            page.goto("https://dart.fss.or.kr" + iframe_src, wait_until="domcontentloaded")
    except:
        pass
    return page.content()

def analyze_sales_variation_with_page(rcp_no: str, *, page, threshold_pct=30.0, **kwargs) -> SalesVariationSignalResult:
    html = fetch_dart_iframe_html_with_page(rcp_no, page=page)
    return parse_sales_variation_html(html, threshold_pct=threshold_pct)
