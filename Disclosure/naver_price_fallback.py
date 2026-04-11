from __future__ import annotations

import math
import os
import re
import time
from datetime import date
from typing import Any

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup


_VERIFY_SSL = os.getenv("NAVER_FINANCE_VERIFY_SSL", "0") == "1"
if not _VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def fetch_naver_quote_snapshot(symbol: str, *, timeout_sec: float = 10.0) -> dict[str, Any]:
    url = f"https://finance.naver.com/item/main.naver?code={str(symbol).zfill(6)}"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=timeout_sec, verify=_VERIFY_SSL)
    resp.raise_for_status()
    text = resp.text
    soup = BeautifulSoup(text, "html.parser")
    today_node = soup.select_one("p.no_today")
    exday_node = soup.select_one("p.no_exday")

    price = None
    if today_node is not None:
        blind = today_node.select_one("span.blind")
        price = _safe_float(blind.get_text(strip=True) if blind else None)

    blind_values = [node.get_text(strip=True) for node in exday_node.select("span.blind")] if exday_node else []
    change_abs = _safe_float(blind_values[0]) if len(blind_values) >= 1 else None
    change_rate = _safe_float(blind_values[1]) if len(blind_values) >= 2 else None
    direction_node = exday_node.select_one("em.no_up, em.no_down, em.no_same, em.X") if exday_node else None
    direction = "same"
    if direction_node is not None:
        classes = {str(item).lower() for item in (direction_node.get("class") or [])}
        if "no_down" in classes:
            direction = "down"
        elif "no_up" in classes:
            direction = "up"
        elif "no_same" in classes or "x" in classes:
            direction = "same"

    if change_abs is not None and direction == "down" and change_abs > 0:
        change_abs = -change_abs
    if change_rate is not None:
        if direction == "down" and change_rate > 0:
            change_rate = -change_rate
        elif direction == "same":
            change_rate = 0.0

    return {
        "price": price,
        "change_abs": change_abs,
        "change_rate": change_rate,
        "source": "naver_main",
    }


def fetch_naver_daily_price_history(
    symbol: str,
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
        r'<td align="center"><span class="tah p10 gray03">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num">.*?</td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>\s*'
        r'<td class="num"><span class="tah p11">([^<]+)</span></td>',
        re.I | re.S,
    )

    for page in range(1, max_pages + 1):
        url = f"https://finance.naver.com/item/sise_day.naver?code={str(symbol).zfill(6)}&page={page}"
        resp = requests.get(url, headers=headers, timeout=15, verify=_VERIFY_SSL)
        resp.raise_for_status()
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
                    "Open": _safe_float(match.group(3)),
                    "High": _safe_float(match.group(4)),
                    "Low": _safe_float(match.group(5)),
                    "Volume": _safe_float(match.group(6)),
                }
            )

        if stop_early and rows and min(pd.to_datetime(row["Date"]).date() for row in rows) <= start_date:
            break
        if sleep_sec > 0:
            time.sleep(min(float(sleep_sec), 0.05))

    if not rows:
        return pd.DataFrame()
    return (
        pd.DataFrame(rows)
        .dropna(subset=["Date"])
        .drop_duplicates(subset=["Date"])
        .sort_values("Date")
        .reset_index(drop=True)
    )
