import re
from typing import Tuple

def is_buyback_report_title(title: str) -> bool:
    """
    자사주 취득/소각 관련 공시인지 제목으로 판별
    - '신탁' 계약은 제외 (임팩트 약함)
    - '결정' 키워드 필수
    """
    if "신탁" in title: return False
    if "결정" not in title: return False
    
    keywords = ["자사주", "자기주식", "주식소각"]
    return any(k in title for k in keywords)

def analyze_fast_buyback(html: str, title: str, market_cap: float) -> Tuple[bool, str, float]:
    """
    [Fast-Track] 자사주 공시 분석
    Return: (is_hit, reason, ratio)
    """
    try:
        # 1. 태그 제거 및 텍스트 정제
        text = re.sub(r'<[^>]+>', ' ', html).replace("&nbsp;", " ").replace(",", "").strip()
        text = re.sub(r'\s+', ' ', text)

        # 2. 금액 추출 (10억 단위 이상 숫자)
        # "취득예정금액", "소각예정금액", "금 액" 등
        amount_match = re.search(r'(취득|소각).*?금\s*액.*?(\d{9,})', text)
        
        amount_val = 0.0
        if amount_match:
            amount_val = float(amount_match.group(2))
        
        # 3. 시총 대비 비율 계산
        if market_cap <= 0: 
            return False, "Market Cap missing", 0.0
            
        ratio = (amount_val / market_cap) * 100.0
        
        # 4. 판정 로직
        is_cancel = "소각" in title
        
        # [조건 A] 주식 소각: 0.5% 이상이면 무조건 호재
        if is_cancel:
            if ratio >= 0.5:
                return True, f"🔥 STOCK_CANCEL: {amount_val/100000000:.0f}억 (시총대비 {ratio:.2f}%)", ratio
            return False, f"Stock Cancel too small ({ratio:.2f}%)", ratio

        # [조건 B] 자사주 취득: 2.0% 이상일 때만 진입
        if "취득" in title:
            if ratio >= 2.0:
                return True, f"💰 BUYBACK: {amount_val/100000000:.0f}억 (시총대비 {ratio:.2f}%)", ratio
            return False, f"Buyback too small ({ratio:.2f}%)", ratio

        return False, "Unknown Type", 0.0

    except Exception as e:
        return False, "FAST_PARSE_ERR", 0.0