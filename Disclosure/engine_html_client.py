from __future__ import annotations

import logging
import random
import re
import time
from html import unescape
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

try:
    from config import SETTINGS
except Exception:
    from Disclosure.config import SETTINGS


log = logging.getLogger("disclosure.engine")


def date_to_select_date(d) -> str:
    return d.strftime("%Y.%m.%d")


def norm_company_key(name: str) -> str:
    if not name:
        return ""
    s = name.strip()
    s = s.replace("㈜", "").replace("(주)", "").replace("주식회사", "")
    s = re.sub(r"[\s\u00A0]+", "", s)
    s = re.sub(r"[·\.\,\(\)\[\]\/\\\-_\u2022]", "", s)
    return s.lower()


@dataclass(frozen=True)
class HtmlDisclosureItem:
    select_date: str
    time_hhmm: str
    market_tag: str
    company: str
    title: str
    rcp_no: str
    submitter: str
    rcv_date: str


class DARTRecentHtmlClient:
    BASE_URL = "https://dart.fss.or.kr/dsac001/mainAll.do"

    def __init__(
        self,
        timeout_sec: float = 10.0,
        max_retries: int = 3,
        min_backoff_sec: float = 0.6,
        max_backoff_sec: float = 4.0,
        session: Optional[requests.Session] = None,
    ):
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.min_backoff_sec = min_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.sess = session or requests.Session()
        self.sess.headers.update(
            {
                "User-Agent": getattr(
                    SETTINGS,
                    "HTML_USER_AGENT",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
            }
        )

    def _build_url(self, select_date: str, page: int) -> str:
        return f"{self.BASE_URL}?selectDate={select_date}&sort=&series=&mdayCnt=0&currentPage={page}"

    def fetch_html(self, select_date: str, page: int) -> str:
        url = self._build_url(select_date, page)
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.sess.get(url, timeout=self.timeout_sec)
                r.raise_for_status()
                return r.text
            except Exception as e:
                last_err = e
                backoff = min(self.max_backoff_sec, self.min_backoff_sec * (2 ** (attempt - 1)))
                backoff *= (0.85 + random.random() * 0.4)
                log.warning(
                    "[HTML][FETCH-ERR] date=%s page=%s attempt=%s/%s err=%s sleep=%.2fs",
                    select_date, page, attempt, self.max_retries, str(e)[:180], backoff,
                )
                time.sleep(backoff)
        raise RuntimeError(f"HTML fetch failed: date={select_date} page={page} err={last_err}")

    @staticmethod
    def _extract_total_pages(html: str) -> Optional[int]:
        m = re.search(r"\[\s*\d+\s*/\s*(\d+)\s*\]", html)
        if not m:
            return None
        try:
            return max(1, int(m.group(1)))
        except Exception:
            return None

    @staticmethod
    def _extract_rcp_no_from_tag_attrs(tag: Any) -> Optional[str]:
        if tag is None:
            return None
        candidates: List[str] = []
        try:
            for attr in ("href", "onclick"):
                value = tag.get(attr)
                if value:
                    candidates.append(str(value))
        except Exception:
            return None

        for value in candidates:
            match = re.search(r"rcpNo=(\d{10,20})", value)
            if match:
                return match.group(1)
            match = re.search(r"['\"](\d{14})['\"]", value)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _clean_cell_text(text: str) -> str:
        if not text:
            return ""
        cleaned = unescape(text)
        cleaned = re.sub(r"<br\s*/?>", " ", cleaned, flags=re.I)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    def _parse_items_with_regex(self, html: str, select_date: str) -> List[HtmlDisclosureItem]:
        tbody_match = re.search(r"<tbody[^>]*>(.*?)</tbody>", html, flags=re.I | re.S)
        if not tbody_match:
            return []

        tbody_html = tbody_match.group(1)
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbody_html, flags=re.I | re.S)
        out: List[HtmlDisclosureItem] = []

        for row_html in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.I | re.S)
            if len(cells) < 4:
                continue

            time_text = self._clean_cell_text(cells[0])
            corp_text = self._clean_cell_text(cells[1])
            title_text = self._clean_cell_text(cells[2])
            submit_text = self._clean_cell_text(cells[3])
            rcv_date = self._clean_cell_text(cells[4]) if len(cells) >= 5 else ""

            market_tag = ""
            company = corp_text
            if corp_text:
                parts = corp_text.split()
                if parts and parts[0] in ("유", "코", "넥", "기", "채", "공", "정", "철", "연"):
                    market_tag = parts[0]
                    company = " ".join(parts[1:]).strip()

            rcp_no = None
            for match in re.finditer(r"<a\b[^>]*(?:href|onclick)=['\"][^'\"]*?['\"][^>]*>", row_html, flags=re.I | re.S):
                rcp_no = self._extract_rcp_no_from_tag_attrs(match.group(0))
                if rcp_no:
                    break

            if not rcp_no or not title_text:
                fallback_match = re.search(r"rcpNo=(\d{10,20})", row_html)
                if fallback_match:
                    rcp_no = fallback_match.group(1)

            if not rcp_no or not title_text:
                continue

            out.append(
                HtmlDisclosureItem(
                    select_date=select_date,
                    time_hhmm=time_text,
                    market_tag=market_tag,
                    company=company,
                    title=title_text,
                    rcp_no=rcp_no,
                    submitter=submit_text,
                    rcv_date=rcv_date,
                )
            )
        return out

    def parse_items(self, html: str, select_date: str) -> List[HtmlDisclosureItem]:
        try:
            from bs4 import BeautifulSoup
        except Exception as e:
            log.warning("[HTML][PARSE-FALLBACK] bs4 unavailable, using regex parser: %s", str(e)[:160])
            return self._parse_items_with_regex(html, select_date)

        soup = BeautifulSoup(html, "html.parser")
        tbody = soup.find("tbody")
        if not tbody:
            return []

        out: List[HtmlDisclosureItem] = []
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue

            time_text = self._clean_cell_text(tds[0].decode_contents())
            corp_text = self._clean_cell_text(tds[1].decode_contents())
            title_text = self._clean_cell_text(tds[2].decode_contents())
            submit_text = self._clean_cell_text(tds[3].decode_contents())
            rcv_date = self._clean_cell_text(tds[4].decode_contents()) if len(tds) >= 5 else ""

            market_tag = ""
            company = corp_text
            if corp_text:
                parts = corp_text.split()
                if parts and parts[0] in ("유", "코", "넥", "기", "채", "공", "정", "철", "연"):
                    market_tag = parts[0]
                    company = " ".join(parts[1:]).strip()

            rcp_no = None
            for a in tds[2].find_all("a"):
                rcp_no = self._extract_rcp_no_from_tag_attrs(a)
                if rcp_no:
                    break
            if not rcp_no:
                for a in tr.find_all("a"):
                    rcp_no = self._extract_rcp_no_from_tag_attrs(a)
                    if rcp_no:
                        break

            if not rcp_no or not title_text:
                continue

            out.append(
                HtmlDisclosureItem(
                    select_date=select_date,
                    time_hhmm=time_text,
                    market_tag=market_tag,
                    company=company,
                    title=title_text,
                    rcp_no=rcp_no,
                    submitter=submit_text,
                    rcv_date=rcv_date,
                )
            )

        return out

    def fetch_items_for_date(
        self,
        d,
        *,
        max_pages: int = 1,
        respect_total_pages: bool = True,
    ) -> List[HtmlDisclosureItem]:
        select_date = date_to_select_date(d)
        first_html = self.fetch_html(select_date, 1)
        total_pages = self._extract_total_pages(first_html) or 1
        page_limit = min(max_pages, total_pages) if respect_total_pages else max_pages

        items: List[HtmlDisclosureItem] = []
        items.extend(self.parse_items(first_html, select_date))
        for page in range(2, max(1, page_limit) + 1):
            items.extend(self.parse_items(self.fetch_html(select_date, page), select_date))

        dedup: Dict[str, HtmlDisclosureItem] = {}
        for item in items:
            dedup[item.rcp_no] = item
        return list(dedup.values())
