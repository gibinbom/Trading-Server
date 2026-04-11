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
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCAL_LISTING_FALLBACK = os.path.join(PROJECT_ROOT_DIR, "krx_listing.csv")

def build_watchlist_by_marcap(
    min_marcap_krw: int = 0,
    markets=("KOSPI", "KOSDAQ"),
    top_n: Optional[int] = 2000,
    exclude_name_keywords=None,
    exclude_exact_suffixes=None,
    return_type: Literal["codes", "map", "records"] = "map",
) -> Union[List[str], Dict[str, str], List[dict]]:
    
    today_str = datetime.today().strftime('%Y%m%d')
    cache_file = f"krx_universe_{today_str}.csv"
    
    df = None

    # 💡 1. 캐시 확인: 오늘 이미 다운받은 파일이 있다면 서버에 요청하지 않고 바로 로드 (IP 차단 완벽 방어)
    if os.path.exists(cache_file):
        df = pd.read_csv(cache_file, dtype={"Code": str})
    elif StockListing is not None:
        # 💡 2. 다운로드 시도: 최대 3번 재시도하며, KRX-MARCAP을 사용하여 시가총액 데이터를 확실히 가져옴
        for attempt in range(3):
            try:
                df = StockListing("KRX-MARCAP")
                if df is not None and not df.empty:
                    # 다운로드 성공 시 로컬에 저장하고, 어제자 캐시 파일은 청소
                    df.to_csv(cache_file, index=False, encoding='utf-8-sig')
                    for old_file in glob.glob("krx_universe_*.csv"):
                        if old_file != cache_file:
                            os.remove(old_file)
                    break
            except Exception as e:
                time.sleep(2) # 2초 대기 후 재시도
    elif os.path.exists(LOCAL_LISTING_FALLBACK):
        try:
            df = pd.read_csv(LOCAL_LISTING_FALLBACK, dtype={"Code": str})
            log.warning("⚠️ FinanceDataReader unavailable. Using local listing fallback: %s", LOCAL_LISTING_FALLBACK)
        except Exception as exc:
            log.warning("⚠️ local listing fallback load failed: %s", exc)
                
    # 💡 3. 서버 차단으로 완전 실패 시 최후의 보루 (가장 최근 캐시 파일 로드)
    if df is None or df.empty:
        old_caches = sorted(glob.glob("krx_universe_*.csv"), reverse=True)
        if old_caches:
            log.warning(f"⚠️ KRX 서버 응답 없음. 과거 데이터({old_caches[0]})로 유니버스를 강제 구성합니다.")
            df = pd.read_csv(old_caches[0], dtype={"Code": str})
        elif os.path.exists(LOCAL_LISTING_FALLBACK):
            log.warning("⚠️ KRX 서버 응답 없음. 로컬 listing fallback으로 유니버스를 구성합니다: %s", LOCAL_LISTING_FALLBACK)
            df = pd.read_csv(LOCAL_LISTING_FALLBACK, dtype={"Code": str})
        else:
            raise ValueError("KRX 서버 차단 및 로컬에 백업된 캐시 파일이 없어 유니버스 생성이 불가능합니다.")

    # 4. 시장 필터 (KOSPI, KOSDAQ)
    if markets and "Market" in df.columns:
        df = df[df["Market"].isin(list(markets))].copy()

    # 5. 6자리 정상적인 숫자 코드만 추출
    if "Code" not in df.columns:
        raise ValueError("데이터에 'Code' 컬럼이 없습니다.")
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    df = df[df["Code"].str.match(r"^\d{6}$", na=False)].copy()

    exclude_name_keywords = exclude_name_keywords or DEFAULT_EXCLUDED_NAME_KEYWORDS
    exclude_exact_suffixes = exclude_exact_suffixes or DEFAULT_EXCLUDED_EXACT_SUFFIXES

    # 6. 노이즈 필터링 (ETF, 우선주 등)
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

    # 7. 오직 '시가총액(Marcap)' 기준으로만 정렬!
    if "Marcap" in df.columns:
        df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
        df = df.dropna(subset=["Marcap"]).copy()
        df = df[df["Marcap"] >= int(min_marcap_krw)].copy()
        df = df.sort_values("Marcap", ascending=False)

    # 8. 상위 N개 자르기 (기본 2000개)
    if top_n:
        df = df.head(int(top_n)).copy()

    # 9. 원하는 타입으로 리턴
    if return_type == "codes":
        return df["Code"].tolist()
    elif return_type == "map":
        return dict(zip(df["Code"].tolist(), df["Name"].tolist()))
    else:
        return df.to_dict("records")
