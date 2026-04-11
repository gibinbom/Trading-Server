# universe.py
from __future__ import annotations
import re
import os
import glob
import time
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

import pandas as pd
import logging

try:
    from FinanceDataReader import StockListing
except Exception:
    StockListing = None

log = logging.getLogger("scanner")

DEFAULT_EXCLUDED_NAME_KEYWORDS = ["ETF", "ETN", "리츠", "스팩", "SPAC"]
DEFAULT_EXCLUDED_EXACT_SUFFIXES = ["우", "우B", "우C", "1우", "2우", "3우"]
DEFAULT_EXCLUDED_CONSTRUCTION_KEYWORDS = ["건설", "토건", "토목", "건축", "건설업", "건설기계"]
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_LISTING_FALLBACK = os.path.join(PROJECT_ROOT_DIR, "krx_listing.csv")

def _contains_any_keyword(series: pd.Series, keywords: List[str]) -> pd.Series:
    """Series 안에 keywords 중 하나라도 포함되면 True"""
    if not keywords:
        return pd.Series(False, index=series.index)
    pattern = "|".join(re.escape(k) for k in keywords if k)
    if not pattern:
        return pd.Series(False, index=series.index)
    return series.astype(str).str.contains(pattern, na=False, regex=True)

def build_watchlist_by_marcap(
    min_marcap_krw: int = 0,
    markets=("KOSPI", "KOSDAQ"),
    top_n: Optional[int] = 2000,
    exclude_name_keywords=None,
    exclude_exact_suffixes=None,
    exclude_construction: bool = True,
    exclude_construction_keywords=None,
    return_type: Literal["codes", "map", "records"] = "map",
) -> Union[List[str], Dict[str, str], List[dict]]:
    
    today_str = datetime.now().strftime('%Y%m%d')
    cache_file = f"krx_universe_merged_{today_str}.csv"
    df = None

    # 💡 1. 든든한 캐시 방어막 (IP 차단 방지)
    if os.path.exists(cache_file):
        df = pd.read_csv(cache_file, dtype={"Code": str})
    elif StockListing is not None:
        for attempt in range(3):
            try:
                # 💡 2. 딜레마 해결: KRX-MARCAP(시총)과 KRX(섹터)를 둘 다 불러와서 합침!
                df_marcap = StockListing("KRX-MARCAP")
                df_sector = StockListing("KRX")
                
                if not df_marcap.empty and not df_sector.empty:
                    # 컬럼명 통일 (KRX는 'Symbol', MARCAP은 'Code'를 씀)
                    if "Symbol" in df_sector.columns:
                        df_sector = df_sector.rename(columns={"Symbol": "Code"})
                        
                    # 시총 데이터(df_marcap)에 섹터/산업 데이터(df_sector) 결합 (Left Join)
                    df = pd.merge(df_marcap, df_sector[['Code', 'Sector', 'Industry']], on='Code', how='left')
                    
                    # 파일로 저장
                    df.to_csv(cache_file, index=False, encoding='utf-8-sig')
                    # 이전 캐시 찌꺼기 청소
                    for old_file in glob.glob("krx_universe_merged_*.csv"):
                        if old_file != cache_file: os.remove(old_file)
                    break
            except Exception as e:
                time.sleep(2)
    elif os.path.exists(LOCAL_LISTING_FALLBACK):
        try:
            df = pd.read_csv(LOCAL_LISTING_FALLBACK, dtype={"Code": str})
            log.warning("⚠️ FinanceDataReader unavailable. Using local listing fallback: %s", LOCAL_LISTING_FALLBACK)
        except Exception as e:
            log.warning("⚠️ local listing fallback load failed: %s", e)

    if (df is None or df.empty) and os.path.exists(LOCAL_LISTING_FALLBACK):
        try:
            df = pd.read_csv(LOCAL_LISTING_FALLBACK, dtype={"Code": str})
            log.warning("⚠️ KRX 응답 불안정으로 로컬 listing fallback을 사용합니다: %s", LOCAL_LISTING_FALLBACK)
        except Exception as e:
            log.warning("⚠️ local listing fallback load failed after KRX retry: %s", e)
    
    # 캐시 실패 시 비상용 하드코딩
    if df is None or df.empty:
        log.warning("⚠️ 캐시 파일 없음! 최후의 보루인 [코스피/코스닥 핵심 주도주 150+선]으로 강제 가동합니다.")
        fallback_map = {
            # 💻 반도체 / IT 하드웨어
            "005930": "삼성전자", "000660": "SK하이닉스", "042700": "한미반도체", "403870": "HPSP", 
            "039030": "이수페타시스", "058470": "리노공업", "074600": "원익IPS", "252990": "에이피티씨", 
            "036540": "SFA반도체", "084370": "유진테크", "036930": "주성엔지니어링", "108320": "LX세미콘", 
            "036830": "솔브레인", "005935": "삼성전자우", "240810": "원익피앤이", "095610": "테스", 
            "222800": "심텍", "011070": "LG이노텍", "009150": "삼성전기", "090360": "로보티즈", 
            "277810": "레인보우로보틱스", "050090": "에스에프에이", "079370": "제우스", "000990": "DB하이텍", 
            "014680": "한솔케미칼", "066570": "LG전자",
            
            # ⚡ 2차전지 / 에너지 / 화학
            "373220": "LG에너지솔루션", "006400": "삼성SDI", "051910": "LG화학", "096770": "SK이노베이션", 
            "086520": "에코프로", "247540": "에코프로비엠", "003670": "포스코퓨처엠", "005490": "POSCO홀딩스", 
            "348370": "엔켐", "010130": "고려아연", "009830": "한화솔루션", "066970": "엘앤에프", 
            "011170": "롯데케미칼", "010950": "S-Oil", "004020": "현대제철", "001430": "금양", 
            "278280": "천보", "001440": "대한전선", "036460": "한국가스공사",
            
            # 🚀 방산 / 조선 / 기계 / 전력
            "012450": "한화에어로스페이스", "047810": "한국항공우주", "064350": "현대로템", "079550": "LIG넥스원", 
            "042660": "한화오션", "010140": "삼성중공업", "329180": "HD현대중공업", "009540": "HD한국조선해양", 
            "034020": "두산에너빌리티", "015760": "한국전력", "052690": "한전기술", "241560": "두산밥캣", 
            "272210": "한화시스템", "112610": "씨에스윈드", "047050": "포스코인터내셔널", "010620": "HD현대미포",
            "042670": "HD현대인프라코어", "267250": "HD현대", "000150": "두산",

            # 🏦 금융 / 밸류업 / 지주사
            "105560": "KB금융", "055550": "신한지주", "086790": "하나금융지주", "316140": "우리금융지주", 
            "138040": "메리츠금융지주", "006800": "미래에셋증권", "016360": "삼성증권", "005940": "NH투자증권", 
            "039490": "키움증권", "032830": "삼성생명", "000810": "삼성화재", "024110": "기업은행", 
            "323410": "카카오뱅크", "377300": "카카오페이", "003550": "LG", "028260": "삼성물산",
            "005830": "DB손해보험", "078930": "GS", "000880": "한화", "001040": "CJ",

            # 🌐 인터넷 / 게임 / 엔터
            "035420": "NAVER", "035720": "카카오", "018260": "삼성SDS", "034730": "SK", "402340": "SK스퀘어", 
            "035760": "CJ ENM", "036570": "엔씨소프트", "259960": "크래프톤", "251270": "넷마블", 
            "263750": "펄어비스", "293490": "카카오게임즈", "095660": "네오위즈", "352820": "하이브", 
            "035900": "JYP Ent.", "041510": "에스엠", "122870": "와이지엔터테인먼트", "253450": "스튜디오드래곤",

            # 🧬 바이오 / 헬스케어 / 화장품 / 소비재
            "207940": "삼성바이오로직스", "068270": "셀트리온", "196170": "알테오젠", "028300": "HLB", 
            "068240": "셀트리온제약", "008930": "한미사이언스", "128940": "한미약품", "006280": "녹십자", 
            "001630": "종근당", "096530": "씨젠", "090430": "아모레퍼시픽", "051900": "LG생활건강", 
            "214150": "클래시스", "290650": "엘앤씨바이오", "328130": "루닛", "145020": "휴젤", 
            "033780": "KT&G", "027410": "BGF리테일", "139480": "이마트", "023530": "롯데쇼핑", 
            "007070": "GS리테일", "002790": "아모레G",

            # 🚗 자동차 / 운송 / 통신
            "005380": "현대차", "000270": "기아", "012330": "현대모비스", "000120": "CJ대한통운", 
            "028670": "팬오션", "011200": "HMM", "005385": "현대차우", "005387": "현대차2우B", 
            "018880": "한온시스템", "011210": "현대위아", "161390": "한국타이어앤테크놀로지",
            "032640": "LG유플러스", "030200": "KT", "017670": "SK텔레콤"
        }
        df = pd.DataFrame([{"Code": k, "Name": v, "Marcap": 999999999999, "Market": "KOSPI", "Sector": "", "Industry": ""} for k, v in fallback_map.items()])
    # ==========================================
    # 💡 여기서부터 Sia님의 훌륭한 필터링 로직 시작
    # ==========================================
    
    # ✅ 시장 필터
    if markets and "Market" in df.columns:
        df = df[df["Market"].isin(list(markets))].copy()

    # ✅ 6자리 숫자 코드만
    if "Code" not in df.columns:
        raise ValueError("KRX listing에 'Code' 컬럼이 없습니다.")
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    df = df[df["Code"].str.match(r"^\d{6}$", na=False)].copy()

    exclude_name_keywords = exclude_name_keywords or DEFAULT_EXCLUDED_NAME_KEYWORDS
    exclude_exact_suffixes = exclude_exact_suffixes or DEFAULT_EXCLUDED_EXACT_SUFFIXES

    # ✅ Name 기반 제외(ETF/우선주/관리 등)
    if "Name" in df.columns:
        def is_excluded_name(name: str) -> bool:
            if pd.isna(name): return True
            name = str(name).strip()
            if not name or "관리" in name: return True
            for kw in exclude_name_keywords:
                if kw and kw in name: return True
            for suf in exclude_exact_suffixes:
                if suf and name.endswith(suf): return True
            return False

        df = df[~df["Name"].apply(is_excluded_name)].copy()
        df["Name"] = df["Name"].astype(str).str.strip()

    # ✅ 건설주 제외: 이제 Merge를 했으므로 Sector/Industry 컬럼이 확실하게 작동합니다!
    if exclude_construction:
        kws = exclude_construction_keywords or DEFAULT_EXCLUDED_CONSTRUCTION_KEYWORDS

        name_mask = pd.Series(False, index=df.index)
        if "Name" in df.columns:
            name_mask = _contains_any_keyword(df["Name"], kws)

        sector_mask = pd.Series(False, index=df.index)
        for col in ("Sector", "Industry"):
            if col in df.columns:
                sector_mask |= _contains_any_keyword(df[col], kws)

        df = df[~(name_mask | sector_mask)].copy()

    # ✅ 시총 필터/정렬 (이제 완벽하게 작동함)
    if "Marcap" in df.columns:
        df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
        df = df.dropna(subset=["Marcap"]).copy()
        df = df[df["Marcap"] >= int(min_marcap_krw)].copy()
        df = df.sort_values("Marcap", ascending=False)

    # ✅ top_n
    if top_n:
        df = df.head(int(top_n)).copy()

    # ✅ 리턴 형식
    if return_type == "codes":
        return df["Code"].tolist()
    elif return_type == "map":
        if "Name" in df.columns:
            return dict(zip(df["Code"].tolist(), df["Name"].tolist()))
        return {c: "" for c in df["Code"].tolist()}

    cols = ["Code", "Name", "Marcap", "Market", "Sector", "Industry"]
    for c in cols:
        if c not in df.columns: df[c] = None

    return (
        df[cols]
        .rename(columns={"Code": "code", "Name": "name", "Marcap": "marcap", "Market": "market", "Sector": "sector", "Industry": "industry"})
        .to_dict("records")
    )
