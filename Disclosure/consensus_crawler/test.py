import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
from config import SETTINGS

def _latest_business_day(max_lookback=14):
    d = datetime.now()
    for _ in range(max_lookback):
        ds = d.strftime("%Y%m%d")
        df = stock.get_market_cap_by_ticker(ds, market="KOSPI")
        if df is not None and not df.empty:
            return ds
        d -= timedelta(days=1)
    raise RuntimeError("최근 영업일을 찾지 못했습니다.")

def load_universe() -> pd.DataFrame:
    date = _latest_business_day()

    frames = []
    for mkt in ["KOSPI", "KOSDAQ"]:
        cap = stock.get_market_cap_by_ticker(date, market=mkt)
        cap = cap.rename(columns={"종가":"Close", "시가총액":"Marcap"}).copy()
        cap["Code"] = cap.index
        cap["Name"] = cap.index.map(stock.get_market_ticker_name)
        cap["Market"] = mkt
        frames.append(cap[["Marcap","Close","Code","Name","Market"]])

    df = pd.concat(frames, ignore_index=True)

    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
    df = df.dropna(subset=["Close","Marcap"])

    df = df[(df["Close"] > SETTINGS.PRICE_MIN) & (df["Marcap"] > SETTINGS.MARCAP_MIN)].copy()

    # 이름 필터(네 로직 유지)
    def is_excluded_name(name: str) -> bool:
        if pd.isna(name): return True
        for kw in SETTINGS.EXCLUDED_NAME_KEYWORDS:
            if kw in name: return True
        for suf in SETTINGS.EXCLUDED_EXACT_SUFFIXES:
            if name.endswith(suf): return True
        if "관리" in name: return True
        return False

    df = df[~df["Name"].apply(is_excluded_name)].copy()

    MARCAP_MIN = 2000 * 100_000_000  # 2000억
    df = df[df["Marcap"] >= MARCAP_MIN].copy()

    return df.sort_values(["Market","Code"]).reset_index(drop=True)