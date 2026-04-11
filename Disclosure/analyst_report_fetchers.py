from __future__ import annotations

import html
import json
import re
from datetime import datetime, timedelta


def _clean_html_text(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(text or ""))
    cleaned = html.unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _to_iso_date_yy_mm_dd(text: str) -> str:
    digits = re.findall(r"\d+", str(text or ""))
    if len(digits) >= 3:
        year = int(digits[0])
        year += 2000 if year < 100 else 0
        return f"{year:04d}-{int(digits[1]):02d}-{int(digits[2]):02d}T00:00:00+09:00"
    return datetime.now().isoformat(timespec="seconds")


def _to_iso_date_yyyy_mm_dd(text: str) -> str:
    digits = re.findall(r"\d+", str(text or ""))
    if len(digits) >= 3:
        return f"{int(digits[0]):04d}-{int(digits[1]):02d}-{int(digits[2]):02d}T00:00:00+09:00"
    return datetime.now().isoformat(timespec="seconds")


def _to_iso_date_compact(date_text: str, time_text: str = "") -> str:
    date_digits = re.findall(r"\d+", str(date_text or ""))
    time_digits = re.findall(r"\d+", str(time_text or ""))
    if len(date_digits) >= 1 and len(date_digits[0]) == 8:
        raw = date_digits[0]
        year = int(raw[:4])
        month = int(raw[4:6])
        day = int(raw[6:8])
        hh = int((time_digits[0] if time_digits else "000000")[:2] or 0)
        mm = int((time_digits[0] if time_digits else "000000")[2:4] or 0)
        ss = int((time_digits[0] if time_digits else "000000")[4:6] or 0)
        return f"{year:04d}-{month:02d}-{day:02d}T{hh:02d}:{mm:02d}:{ss:02d}+09:00"
    return datetime.now().isoformat(timespec="seconds")


def _clean_summary_text(text: str) -> str:
    value = _clean_html_text(text or "")
    if not value:
        return ""
    value = value.replace("ㅁ", " · ")
    value = re.sub(r"\s*·\s*", " · ", value)
    value = re.sub(r"( · ){2,}", " · ", value)
    return value.strip(" ·")


def fetch_naver_company_list(session, page: int = 1) -> list[dict]:
    url = f"https://finance.naver.com/research/company_list.naver?page={int(page)}"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    text = resp.text
    table = re.search(r'class="type_1">(.*?)</table>', text, re.I | re.S)
    if not table:
        return []

    rows = []
    for row in re.findall(r"<tr>(.*?)</tr>", table.group(1), re.I | re.S):
        if "company_read.naver" not in row:
            continue
        symbol_match = re.search(r'/item/main\.naver\?code=(\d{6})"[^>]*title="([^"]+)"', row, re.I)
        detail_match = re.search(r'href="(company_read\.naver\?nid=(\d+)&page=\d+)"[^>]*>(.*?)</a>', row, re.I | re.S)
        pdf_match = re.search(r'href="(https://stock\.pstatic\.net/[^"]+\.pdf)"', row, re.I)
        broker_match = re.search(r"</td>\s*<td>([^<]+)</td>\s*<td class=\"file\">", row, re.I | re.S)
        date_match = re.search(r'class="date"[^>]*>([^<]+)</td>', row, re.I)
        if not symbol_match or not detail_match:
            continue
        rows.append(
            {
                "source": "NAVER_RESEARCH",
                "symbol": symbol_match.group(1),
                "name": _clean_html_text(symbol_match.group(2)),
                "title": _clean_html_text(detail_match.group(3)),
                "broker": _clean_html_text(broker_match.group(1) if broker_match else ""),
                "published_at": _to_iso_date_yy_mm_dd(date_match.group(1) if date_match else ""),
                "nid": detail_match.group(2),
                "detail_url": "https://finance.naver.com/research/" + detail_match.group(1),
                "pdf_url": pdf_match.group(1) if pdf_match else "",
            }
        )
    return rows


def fetch_naver_report_detail(session, detail_url: str) -> dict:
    resp = session.get(detail_url, timeout=15)
    resp.raise_for_status()
    text = resp.text
    source_match = re.search(r'<p class="source">([^<]+)<b class="bar">\|</b>([^<]+)<b class="bar">\|</b>조회', text, re.I)
    target_match = re.search(r"목표가\s*<em class=\"money\"><strong>([^<]+)</strong>", text, re.I)
    rating_match = re.search(r"투자의견\s*<em class=\"coment\">([^<]+)</em>", text, re.I)
    content_match = re.search(r'<td colspan="2" class="view_cnt">\s*<div[^>]*>(.*?)</div>', text, re.I | re.S)
    pdf_match = re.search(r'href="(https://stock\.pstatic\.net/[^"]+\.pdf)"', text, re.I)
    return {
        "broker": _clean_html_text(source_match.group(1) if source_match else ""),
        "published_at": _to_iso_date_yyyy_mm_dd(source_match.group(2) if source_match else ""),
        "target_price": _clean_html_text(target_match.group(1) if target_match else ""),
        "rating": _clean_html_text(rating_match.group(1) if rating_match else ""),
        "content": _clean_html_text(content_match.group(1) if content_match else ""),
        "pdf_url": pdf_match.group(1) if pdf_match else "",
    }


def fetch_hankyung_company_list(session, page: int = 1, lookback_days: int = 7) -> list[dict]:
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=max(1, int(lookback_days)))
    url = (
        "https://consensus.hankyung.com/analysis/list"
        f"?&sdate={start_date:%Y-%m-%d}&edate={end_date:%Y-%m-%d}&report_type=CO&order_type=&now_page={int(page)}"
    )
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    text = resp.text
    rows = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.I | re.S):
        if "/analysis/downpdf?report_idx=" not in row:
            continue
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.I | re.S)
        if len(cells) < 6:
            continue
        title_match = re.search(r'href="(/analysis/downpdf\?report_idx=(\d+))"[^>]*>(.*?)</a>', row, re.I | re.S)
        summary_match = re.search(r"<ul>\s*<li>(.*?)</li>\s*</ul>", row, re.I | re.S)
        symbol_match = re.search(r"([가-힣A-Za-z0-9&\.\-\s]+)\((\d{6})\)", _clean_html_text(title_match.group(3) if title_match else ""))
        if not title_match or not symbol_match:
            continue
        rows.append(
            {
                "source": "HANKYUNG_CONSENSUS",
                "symbol": symbol_match.group(2),
                "name": _clean_html_text(symbol_match.group(1)),
                "title": _clean_html_text(title_match.group(3)),
                "target_price": _clean_html_text(cells[2]),
                "rating": _clean_html_text(cells[3]),
                "writer": _clean_html_text(cells[4]),
                "broker": _clean_html_text(cells[5]),
                "published_at": _to_iso_date_yyyy_mm_dd(_clean_html_text(cells[0])),
                "content": _clean_html_text(summary_match.group(1) if summary_match else ""),
                "report_idx": title_match.group(2),
                "pdf_url": "https://consensus.hankyung.com" + title_match.group(1),
            }
        )
    return rows


def fetch_fnguide_report_summary(session, symbol: str) -> list[dict]:
    normalized = "".join(ch for ch in str(symbol or "") if ch.isdigit()).zfill(6)
    if not normalized:
        return []
    gicode = f"A{normalized}"
    page_url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Consensus.asp?NewMenuID=108&gicode={gicode}"
    json_url = f"https://comp.fnguide.com/SVO2/json/data/01_06/04_{gicode}.json"
    resp = session.get(json_url, timeout=20)
    resp.raise_for_status()
    payload = json.loads(resp.content.decode("utf-8-sig", errors="ignore"))
    rows = []
    for item in payload.get("comp", []) or []:
        title = _clean_html_text(item.get("TITLE"))
        name = _clean_html_text(item.get("CO_NM"))
        if not title or not name:
            continue
        rows.append(
            {
                "source": "FNGUIDE_CONSENSUS",
                "symbol": normalized,
                "name": name,
                "title": title,
                "broker": _clean_html_text(item.get("OFFER_INST_NM")),
                "writer": _clean_html_text(item.get("BEST_ANAL_NM") or item.get("NICK_NM")),
                "published_at": _to_iso_date_compact(item.get("BULLET_DT"), item.get("BULLET_TM")),
                "target_price": _clean_html_text(item.get("TARGET_PRC")),
                "rating": _clean_html_text(item.get("RECOMMEND")),
                "content": _clean_summary_text(item.get("SYNOPSIS")),
                "detail_url": page_url,
                "pdf_url": "",
                "report_id": _clean_html_text(item.get("BULLET_NO")),
                "close_price": _clean_html_text(item.get("CLS_PRC")),
            }
        )
    return rows


def fetch_naver_research_list(page: int = 1, sleep_sec: float = 0.0) -> list[dict]:
    import requests
    import time

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    rows = fetch_naver_company_list(session, page=page)
    if sleep_sec > 0:
        time.sleep(float(sleep_sec))
    return rows


def fetch_hankyung_consensus_list(page: int = 1, days: int = 7, sleep_sec: float = 0.0) -> list[dict]:
    import requests
    import time

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    rows = fetch_hankyung_company_list(session, page=page, lookback_days=days)
    if sleep_sec > 0:
        time.sleep(float(sleep_sec))
    return rows


def fetch_fnguide_consensus_list(symbol: str, sleep_sec: float = 0.0) -> list[dict]:
    import requests
    import time

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    rows = fetch_fnguide_report_summary(session, symbol=symbol)
    if sleep_sec > 0:
        time.sleep(float(sleep_sec))
    return rows
