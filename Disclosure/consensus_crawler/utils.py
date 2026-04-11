import datetime

def kst_today_str() -> str:
    KST = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(KST).strftime("%Y-%m-%d")

def safe_float(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None
