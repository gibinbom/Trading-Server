from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests


def _safe_float(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def fetch_naver_intraday_history(symbol: str, trade_date: str, sleep_sec: float = 0.01) -> pd.DataFrame:
    symbol = str(symbol).zfill(6)
    if "-" in str(trade_date):
        ymd = pd.Timestamp(trade_date).strftime("%Y%m%d")
    else:
        ymd = str(trade_date)
    rows: list[dict[str, Any]] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    pattern = re.compile(
        r'<td align="center"><span class="tah p10 gray03">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num">.*?</td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>',
        re.I | re.S,
    )
    max_pages = 40
    thistime = f"{ymd}153000"
    for page in range(1, max_pages + 1):
        url = f"https://finance.naver.com/item/sise_time.naver?code={symbol}&thistime={thistime}&page={page}"
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        matches = list(pattern.finditer(resp.text))
        if not matches:
            break
        for match in matches:
            hhmm = match.group(1).strip()
            dt = pd.to_datetime(f"{ymd} {hhmm}", format="%Y%m%d %H:%M", errors="coerce")
            if pd.isna(dt):
                continue
            rows.append(
                {
                    "DateTime": dt,
                    "Close": _safe_float(match.group(2)),
                    "Sell": _safe_float(match.group(3)),
                    "Buy": _safe_float(match.group(4)),
                    "Volume": _safe_float(match.group(5)),
                    "DeltaVolume": _safe_float(match.group(6)),
                }
            )
        if sleep_sec > 0:
            time.sleep(min(float(sleep_sec), 0.05))
    if not rows:
        return pd.DataFrame()
    return (
        pd.DataFrame(rows)
        .dropna(subset=["DateTime"])
        .drop_duplicates(subset=["DateTime"])
        .sort_values("DateTime")
        .reset_index(drop=True)
    )
