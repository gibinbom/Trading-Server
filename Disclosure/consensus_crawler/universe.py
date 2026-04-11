import pandas as pd
import numpy as np
from FinanceDataReader import StockListing
from config import SETTINGS

def load_universe() -> pd.DataFrame:
    # 1. 데이터 로드
    df_all = StockListing("KRX")
    
    # 2. 시장 필터링
    df = df_all[df_all["Market"].isin(["KOSPI", "KOSDAQ"])].copy()

    # 3. 주가(Close) 처리 (필수: 가격 없는 건 의미 없으므로 제거)
    if "Close" in df.columns:
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df = df.dropna(subset=["Close"])
        df = df[df["Close"] > SETTINGS.PRICE_MIN].copy()
    else:
        # Close 컬럼조차 없으면 빈 깡통 반환
        return pd.DataFrame()

    # 4. 시가총액(Marcap) 처리 (유연하게: 데이터 없으면 필터링 스킵)
    # ✅ 기준 설정 (5000억)
    MARCAP_MIN_VAL = 500 * 100_000_000  

    if "Marcap" in df.columns:
        # 숫자 변환 (에러나면 NaN)
        df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
        
        # ✅ 핵심 로직: (시총이 기준 이상) 이거나 (시총 데이터가 NaN/없음) 인 경우 살림
        # 즉, 데이터가 있으면 검사하고, 없으면 봐준다.
        condition = (df["Marcap"] >= MARCAP_MIN_VAL) | (df["Marcap"].isna())
        df = df[condition].copy()
        
        # NaN으로 남은 애들은 추후 계산 오류 방지를 위해 0으로 채움
        df["Marcap"] = df["Marcap"].fillna(0)
    else:
        # Marcap 컬럼 자체가 아예 안 들어온 경우 -> 0으로 채워서 컬럼 생성 (필터링 패스)
        df["Marcap"] = 0

    # 5. 제외 종목명 필터링
    def is_excluded_name(name: str) -> bool:
        if pd.isna(name):
            return True
        for kw in SETTINGS.EXCLUDED_NAME_KEYWORDS:
            if kw in name:
                return True
        for suf in SETTINGS.EXCLUDED_EXACT_SUFFIXES:
            if name.endswith(suf):
                return True
        if "관리" in name:
            return True
        return False

    if "Name" in df.columns:
        df = df[~df["Name"].apply(is_excluded_name)].copy()

    # 6. 결과 반환 (컬럼 순서 정렬)
    # 필요한 컬럼이 있는지 확인하고 없으면 None 채워서 에러 방지
    req_cols = ["Marcap", "Close", "Code", "Name", "Market"]
    for c in req_cols:
        if c not in df.columns:
            df[c] = None

    return (
        df[req_cols]
        .sort_values(by=["Market", "Code"])
        .reset_index(drop=True)
    )