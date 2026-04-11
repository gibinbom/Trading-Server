from __future__ import annotations

import math
import re
import time
from datetime import date
from typing import Any

import pandas as pd
import requests

INDEX_HTTP_TIMEOUT_SEC = 8


def _safe_float(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def fetch_naver_index_history(
    index_code: str,
    *,
    start_date: date,
    end_date: date,
    lookback_days: int = 260,
    sleep_sec: float = 0.02,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    headers = {"User-Agent": "Mozilla/5.0"}
    max_pages = max(12, int(math.ceil(max(lookback_days + 20, 260) / 10.0)) + 2)
    pattern = re.compile(
        r'<td class="date">([^<]+)</td>\s*'
        r'<td class="number_1">([^<]+)</td>\s*'
        r'<td class="rate_(?:up|down|same)".*?</td>\s*'
        r'<td class="number_1">.*?</td>\s*'
        r'<td class="number_1"[^>]*>([^<]+)</td>\s*'
        r'<td class="number_1"[^>]*>([^<]+)</td>',
        re.I | re.S,
    )
    for page in range(1, max_pages + 1):
        url = f"https://finance.naver.com/sise/sise_index_day.naver?code={index_code}&page={page}"
        try:
            resp = requests.get(url, headers=headers, timeout=INDEX_HTTP_TIMEOUT_SEC)
            resp.raise_for_status()
        except Exception:
            break
        matches = list(pattern.finditer(resp.text))
        if not matches:
            break
        stop_early = False
        for match in matches:
            dt = pd.to_datetime(match.group(1).strip(), format="%Y.%m.%d", errors="coerce")
            if pd.isna(dt):
                continue
            dt_date = dt.date()
            if dt_date < start_date:
                stop_early = True
                continue
            if dt_date > end_date:
                continue
            rows.append(
                {
                    "Date": dt,
                    "Close": _safe_float(match.group(2)),
                    "Open": _safe_float(match.group(2)),
                    "High": _safe_float(match.group(2)),
                    "Low": _safe_float(match.group(2)),
                    "Volume": _safe_float(match.group(3)),
                    "Amount": _safe_float(match.group(4)),
                }
            )
        if stop_early and rows and min(pd.to_datetime(row["Date"]).date() for row in rows) <= start_date:
            break
        if sleep_sec > 0:
            time.sleep(min(float(sleep_sec), 0.05))
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).dropna(subset=["Date"]).drop_duplicates(subset=["Date"]).sort_values("Date").reset_index(drop=True)
