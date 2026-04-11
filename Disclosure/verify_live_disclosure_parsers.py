from __future__ import annotations

import argparse
import datetime as dt
import calendar
import os
import re
import sys
from typing import Dict, List

import requests


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from dart_fast_fetch_v3 import fetch_dart_html_fast
from engine_html_client import DARTRecentHtmlClient, date_to_select_date


def _looks_like_valid_html(html: str) -> bool:
    if not html:
        return False
    text = html.strip().lower()
    if len(text) < 1000:
        return False
    return any(marker in text for marker in ("<html", "<body", "<table", "<iframe"))


def _probe_kind_fast_track(rcp_no: str) -> Dict[str, object]:
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://kind.krx.co.kr/main.do",
    }

    out: Dict[str, object] = {
        "doc_no_found": False,
        "external_url_found": False,
        "final_html_len": 0,
        "valid": False,
        "path": "kind",
    }

    try:
        search_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={rcp_no}"
        resp_main = session.get(search_url, headers=headers, timeout=3)
        doc_no_match = re.search(r"value=['\"](\d+)\|Y['\"]", resp_main.text)
        if not doc_no_match:
            return out

        out["doc_no_found"] = True
        doc_no = doc_no_match.group(1)

        resp_search = session.get(
            f"https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&acptno={rcp_no}&docNo={doc_no}",
            headers=headers,
            timeout=3,
        )
        final_url_match = re.search(
            r"['\"](https?://kind\.krx\.co\.kr/external/[^'\"]+\.htm)['\"]",
            resp_search.text,
        )
        if not final_url_match:
            return out

        out["external_url_found"] = True
        target_url = final_url_match.group(1)
        resp_final = session.get(target_url, headers=headers, timeout=3)
        resp_final.encoding = resp_final.apparent_encoding
        final_html = resp_final.text
        out["final_html_len"] = len(final_html)
        out["valid"] = _looks_like_valid_html(final_html)
        return out
    except Exception as exc:
        out["error"] = str(exc)
        return out


def _probe_dart_detail(rcp_no: str) -> Dict[str, object]:
    out: Dict[str, object] = {"status_code": None, "length": 0, "has_iframe": False}
    try:
        url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"
        resp = requests.get(url, timeout=5)
        text = resp.text
        out["status_code"] = resp.status_code
        out["length"] = len(text)
        out["has_iframe"] = "iframe" in text.lower() and "ifrm" in text.lower()
        return out
    except Exception as exc:
        out["error"] = str(exc)
        return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify current DART/KIND disclosure parser compatibility.")
    parser.add_argument("--date", default=date_to_select_date(dt.datetime.now()), help="DART selectDate (YYYY.MM.DD)")
    parser.add_argument("--limit", type=int, default=5, help="How many recent disclosures to probe.")
    args = parser.parse_args()

    client = DARTRecentHtmlClient()
    html = client.fetch_html(args.date, 1)
    items = client.parse_items(html, args.date)
    print(f"[LIST] date={args.date} parsed_items={len(items)}")
    if not items:
        current = dt.datetime.strptime(args.date, "%Y.%m.%d")
        for _ in range(5):
            current -= dt.timedelta(days=1)
            if current.weekday() >= calendar.SATURDAY:
                continue
            fallback_date = date_to_select_date(current)
            html = client.fetch_html(fallback_date, 1)
            items = client.parse_items(html, fallback_date)
            print(f"[LIST][FALLBACK] date={fallback_date} parsed_items={len(items)}")
            if items:
                args.date = fallback_date
                break

    if not items:
        print("[FAIL] No items parsed from recent disclosure page.")
        return 1

    sample_items = items[: max(1, args.limit)]
    for idx, item in enumerate(sample_items, start=1):
        kind_probe = _probe_kind_fast_track(item.rcp_no)
        dart_probe = _probe_dart_detail(item.rcp_no)
        dart_fast = fetch_dart_html_fast(item.rcp_no)
        dart_fast_valid = _looks_like_valid_html(dart_fast)
        print(
            f"[{idx}] {item.time_hhmm} {item.company} | {item.title} | rcp={item.rcp_no}\n"
            f"    kind: valid={kind_probe.get('valid')} doc={kind_probe.get('doc_no_found')} "
            f"external={kind_probe.get('external_url_found')} len={kind_probe.get('final_html_len')}\n"
            f"    dart_main: status={dart_probe.get('status_code')} iframe={dart_probe.get('has_iframe')} "
            f"len={dart_probe.get('length')}\n"
            f"    dart_fast: valid={dart_fast_valid} len={len(dart_fast or '')}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
