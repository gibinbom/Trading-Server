from __future__ import annotations

import html
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone


KST = timezone(timedelta(hours=9), name="KST")


def _now_kst() -> datetime:
    return datetime.now(KST)


def _to_iso_from_mmdd(mmdd: str) -> str:
    now = _now_kst()
    month, day = [int(part) for part in mmdd.split("/")]
    candidate = datetime(now.year, month, day, tzinfo=KST)
    if candidate > now + timedelta(days=3):
        candidate = datetime(now.year - 1, month, day, tzinfo=KST)
    return candidate.isoformat(timespec="seconds")


def collect_naver_headlines(session, symbol: str, stock_name: str, limit: int = 5) -> list[dict]:
    url = f"https://finance.naver.com/item/main.naver?code={symbol}"
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    text = resp.text
    section = re.search(r'section new_bbs">(.*?)<div class="section cop_analysis">', text, re.I | re.S)
    if not section:
        return []

    items = []
    for block in re.findall(r"<li>(.*?)</li>", section.group(1), re.I | re.S):
        link_match = re.search(r'href="(/item/news_read\.naver[^"]+)"[^>]*>(.*?)</a>', block, re.I | re.S)
        date_match = re.search(r"<em>\s*([0-9]{2}/[0-9]{2})\s*</em>", block)
        if not link_match or not date_match:
            continue
        title = html.unescape(re.sub(r"<[^>]+>", " ", link_match.group(2))).strip()
        title = re.sub(r"\s+", " ", title)
        if not title:
            continue
        items.append(
            {
                "source": "NAVER_FINANCE",
                "symbol": symbol,
                "stock_name": stock_name,
                "title": title,
                "published_at": _to_iso_from_mmdd(date_match.group(1)),
                "link": "https://finance.naver.com" + link_match.group(1),
                "provider": "NAVER_FINANCE",
            }
        )
        if len(items) >= max(1, int(limit)):
            break
    return items


def collect_google_headlines(session, symbol: str, stock_name: str, news_days: int = 7, limit: int = 3) -> list[dict]:
    query = urllib.parse.quote(f'"{stock_name}" when:{max(1, int(news_days))}d')
    url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)

    items = []
    for item in root.findall(".//item")[: max(1, int(limit))]:
        full_title = str(item.findtext("title") or "").strip()
        title, _, provider = full_title.rpartition(" - ")
        items.append(
            {
                "source": "GOOGLE_NEWS_RSS",
                "symbol": symbol,
                "stock_name": stock_name,
                "title": title or full_title,
                "published_at": str(item.findtext("pubDate") or _now_kst().isoformat(timespec="seconds")),
                "link": str(item.findtext("link") or ""),
                "provider": provider or "GOOGLE_NEWS",
            }
        )
    return items


def fetch_naver_stock_news(symbol: str, stock_name: str, limit: int = 5, sleep_sec: float = 0.0) -> list[dict]:
    import requests
    import time

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    rows = collect_naver_headlines(session, str(symbol).zfill(6), stock_name, limit=limit)
    if sleep_sec > 0:
        time.sleep(float(sleep_sec))
    return rows


def fetch_google_news_rss(symbol: str, stock_name: str, limit: int = 5, sleep_sec: float = 0.0, news_days: int = 7) -> list[dict]:
    import requests
    import time

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    rows = collect_google_headlines(session, str(symbol).zfill(6), stock_name, news_days=news_days, limit=limit)
    if sleep_sec > 0:
        time.sleep(float(sleep_sec))
    return rows
