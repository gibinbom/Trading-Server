# dart_fast_fetch_v3.py
from __future__ import annotations

import logging
import re
from typing import Optional

import requests


log = logging.getLogger("disclosure.dart_fast_fetch")
_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        session = requests.Session()
        session.headers.update(
            {
                "Host": "dart.fss.or.kr",
                "Connection": "keep-alive",
                "Cache-Control": "max-age=0",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Referer": "https://dart.fss.or.kr/",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        _SESSION = session
    return _SESSION


def _extract_viewer_url(base_url: str, main_html: str) -> str:
    pattern = (
        r'viewDoc\(\s*["\']?(\d+)["\']?\s*,\s*["\']?(\d+)["\']?\s*,\s*["\']?(\d+)["\']?\s*,'
        r'\s*["\']?(\d+)["\']?\s*,\s*["\']?(\d+)["\']?\s*,\s*["\']?([A-Za-z]+)["\']?\s*,'
        r'\s*["\']?([^"\']*)["\']?\s*\)'
    )
    match = re.search(pattern, main_html)
    if match:
        rcp_no, dcm_no, ele_id, offset, length, dtd, toc_no = match.groups()
        viewer_url = (
            f"{base_url}/report/viewer.do?rcpNo={rcp_no}&dcmNo={dcm_no}"
            f"&eleId={ele_id}&offset={offset}&length={length}&dtd={dtd}"
        )
        if toc_no:
            viewer_url += f"&tocNo={toc_no}"
        return viewer_url

    src_match = re.search(r'document\.getElementById\(["\']ifrm["\']\)\.src\s*=\s*["\']([^"\']+)["\']', main_html)
    if src_match:
        src = src_match.group(1)
        if src.startswith("/"):
            return base_url + src
        if src.startswith("http"):
            return src

    iframe_match = re.search(r"<iframe[^>]+src=['\"]([^'\"]+)['\"]", main_html, flags=re.I)
    if iframe_match:
        src = iframe_match.group(1)
        if src.startswith("/"):
            return base_url + src
        if src.startswith("http"):
            return src

    return ""


def fetch_dart_html_fast(rcp_no: str, *, timeout_sec: float = 3.0, verbose: bool = False) -> str:
    base_url = "https://dart.fss.or.kr"
    main_url = f"{base_url}/dsaf001/main.do?rcpNo={rcp_no}"
    session = _get_session()

    if verbose:
        log.info("[DART-FAST] main fetch start rcp=%s", rcp_no)
    try:
        resp = session.get(main_url, timeout=timeout_sec)
        main_html = resp.text
        
        if "<title>거부</title>" in main_html:
            if verbose:
                log.warning("[DART-FAST] blocked by DART rcp=%s", rcp_no)
            return ""

        viewer_url = _extract_viewer_url(base_url, main_html)
        if not viewer_url:
            if verbose:
                log.warning("[DART-FAST] viewer URL not found rcp=%s", rcp_no)
            return ""

        resp_view = session.get(viewer_url, timeout=timeout_sec)
        html_content = resp_view.text
        
        if "문서가 존재하지 않습니다" in html_content:
             if verbose:
                 log.warning("[DART-FAST] missing document body rcp=%s", rcp_no)
             return ""

        return html_content

    except Exception as e:
        if verbose:
            log.warning("[DART-FAST] fetch error rcp=%s err=%s", rcp_no, str(e)[:160])
        return ""
