# test_order_fast.py
import time
import re
from bs4 import BeautifulSoup
from Disclosure.dart_fast_fetch_v3 import fetch_dart_html_fast

# --- 파싱 로직 (기존 개선판) ---
def _strip_ws(s): return re.sub(r"\s+", "", (s or ""))

def parse_order_fast(html):
    soup = BeautifulSoup(html, "html.parser")
    # "계약금액" 키워드가 있는 테이블 찾기
    tables = soup.find_all("table")
    target_table = None
    for t in tables:
        if "계약금액" in t.get_text() and "최근매출액" in t.get_text():
            target_table = t
            break
    
    if not target_table: return None

    contract_amt = None
    recent_sales = None
    ratio = None

    for tr in target_table.find_all("tr"):
        text = _strip_ws(tr.get_text())
        tds = tr.find_all(["td", "th"])
        if not tds: continue
        
        # 값 추출 (보통 마지막 컬럼)
        val_str = tds[-1].get_text(" ", strip=True).replace(",", "").replace("%", "")
        
        # 단위 보정 (단순화)
        unit = 1
        if "(백만원)" in text: unit = 1_000_000
        elif "(억원)" in text: unit = 100_000_000
        
        try:
            val = float(val_str)
        except:
            continue

        if "확정계약금액" in text or ("계약금액" in text and "총액" in text):
            contract_amt = val * unit
        elif "최근매출액" in text:
            recent_sales = val * unit
        elif "매출액대비" in text:
            ratio = val

    return {"amt": contract_amt, "sales": recent_sales, "ratio": ratio}

# --- 실행 ---
# 오늘자 씨앤지하이테크 수주 공시 RCP 번호 (로그에서 확인 필요, 여기선 예시)
# 만약 로그에 있는 rcp_no가 있다면 그걸 넣으세요.
TEST_RCP = "20260209800293"  # 예시 (오늘 날짜 공시 번호로 교체 추천)

# ※ 테스트를 위해 오늘 실제 공시 중 하나인 '한일화학공업'(20260210900139) 등을 써도 됨
# 여기서는 방금 로그에 있던 지역난방공사 실적말고 수주 공시 아무거나로 테스트
# (씨앤지하이테크 실제 RCP: 20260209900293 라고 가정)

start = time.time()
html = fetch_dart_html_fast("20260209900293") # <-- 여기에 실제 RCP 번호 입력
data = parse_order_fast(html)
end = time.time()

print(f"⏱️ 소요 시간: {end - start:.4f}초")
print(f"📊 파싱 결과: {data}")