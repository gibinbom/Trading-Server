import requests
import time
import re

def fetch_kind_v12_triple_jump(acpt_no):
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/main.do"
    }

    try:
        # Step 1: 메인 페이지에서 docNo(서류 고유번호) 추출 (0.1s)
        # ---------------------------------------------------------
        url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={acpt_no}"
        t1 = time.time()
        resp_main = session.get(url, headers=headers, timeout=3)
        
        # <option value='20260209002479|Y' ...> 에서 docNo 추출
        doc_no_match = re.search(r"value=['\"](\d+)\|Y['\"]", resp_main.text)
        if not doc_no_match:
            print("❌ [ERR] docNo를 찾을 수 없습니다.")
            return None
        
        doc_no = doc_no_match.group(1)
        print(f"🔑 [STEP 1] docNo 확보: {doc_no} ({(time.time()-t1)*1000:.1f}ms)")

        # Step 2: 브라우저가 내부적으로 부르는 searchContents 호출 (0.1s)
        # ---------------------------------------------------------
        # 이 호출 결과에 진짜 .htm 주소가 들어있습니다.
        t2 = time.time()
        search_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&acptno={acpt_no}&docNo={doc_no}"
        resp_search = session.get(search_url, headers=headers, timeout=3)
        
        # setPath('...', 'https://kind.krx.co.kr/external/.../99626.htm', ...) 에서 주소 추출
        # 정규식으로 .htm 주소만 쏙 빼옵니다.
        final_url_match = re.search(r"['\"](https?://kind\.krx\.co\.kr/external/[^'\"]+\.htm)['\"]", resp_search.text)
        
        if not final_url_match:
            print("❌ [ERR] 최종 .htm 주소 추출 실패")
            return None
        
        target_url = final_url_match.group(1)
        print(f"🚀 [STEP 2] 최종 URL 발견: {target_url} ({(time.time()-t2)*1000:.1f}ms)")

        # Step 3: 최종 .htm 파일 다운로드 (0.05s)
        # ---------------------------------------------------------
        t3 = time.time()
        resp_final = session.get(target_url, headers=headers, timeout=3)
        resp_final.encoding = resp_final.apparent_encoding
        html = resp_final.text
        
        if "영업이익" in html:
            print(f"✅ [SUCCESS] 0.2초대 데이터 파싱 성공! ({(time.time()-t3)*1000:.1f}ms)")
            # 샘플 출력
            match = re.search(r'영업이익\s*[\(\)가-힣]*\s*(-?\d[\d,.]*)', html.replace('&nbsp;', ' '))
            if match: print(f"📊 추출 결과: {match.group(1)}")
            return html
        else:
            print("⚠️ [EMPTY] 파일 로드 성공했으나 키워드 없음")
            return None

    except Exception as e:
        print(f"❌ [ERR] 통신 오류: {e}")
        return None

# 실행
print("----- [KIND V12 TRIPLE-JUMP TEST] -----")
start = time.time()

result_html = fetch_kind_v12_triple_jump("20260210000287")

if result_html:
    print("\n" + "="*60)
    print("📢 [실전 데이터 추출 결과]")
    print("="*60)
    
    # HTML 태그 싹 걷어내고 텍스트만 예쁘게 정리
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(result_html, "html.parser")
    
    # 텍스트만 추출해서 줄바꿈 정리
    lines = [line.strip() for line in soup.get_text().splitlines() if line.strip()]
    
    # '영업이익' 근처 10줄만 출력해보기
    for i, line in enumerate(lines):
        if "영업이익" in line:
            for j in range(max(0, i-2), min(len(lines), i+8)):
                print(f"> {lines[j]}")
            break
    print("="*60)