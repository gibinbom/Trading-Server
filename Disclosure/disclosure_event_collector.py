from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Optional

import requests

try:
    from config import SETTINGS
    from dart_common import classify_disclosure_event, load_corp_code_maps, _is_correction_title
    from disclosure_event_pipeline import DisclosureEventLogger
    from engine_html_client import DARTRecentHtmlClient, date_to_select_date, norm_company_key
except Exception:
    from Disclosure.config import SETTINGS
    from Disclosure.dart_common import classify_disclosure_event, load_corp_code_maps, _is_correction_title
    from Disclosure.disclosure_event_pipeline import DisclosureEventLogger
    from Disclosure.engine_html_client import DARTRecentHtmlClient, date_to_select_date, norm_company_key


log = logging.getLogger("disclosure.event_collector")
KST = timezone(timedelta(hours=9), name="KST")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
EVENT_ROOT = os.path.join(ROOT_DIR, "events")
STATE_PATH = os.path.join(EVENT_ROOT, "event_collector_state.json")
NAME_MAP_CACHE_PATH = os.path.join(EVENT_ROOT, "event_collector_name_map.json")
MARKET_TAG_MAP = {
    "유": "KOSPI",
    "코": "KOSDAQ",
    "넥": "KONEX",
}


def _now_kst() -> datetime:
    return datetime.now(KST)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _default_signal_bias(event_type: str) -> str:
    if event_type in {
        "SUPPLY_CONTRACT",
        "PERF_PRELIM",
        "SALES_VARIATION",
        "BUYBACK",
        "STOCK_CANCELLATION",
        "DIVIDEND",
        "STOCK_SPLIT",
    }:
        return "positive"
    if event_type in {"DILUTION", "SUPPLY_TERMINATION", "REVERSE_SPLIT_REDUCTION", "BUYBACK_DISPOSAL"}:
        return "negative"
    return "neutral"


CAPITAL_ACTION_EVENT_TYPES = {
    "DIVIDEND",
    "STOCK_SPLIT",
    "REVERSE_SPLIT_REDUCTION",
    "MERGER",
    "SPINOFF",
    "BUYBACK",
    "BUYBACK_DISPOSAL",
    "STOCK_CANCELLATION",
}

OPERATING_EVENT_TYPES = {
    "SUPPLY_CONTRACT",
    "SUPPLY_UPDATE",
    "SUPPLY_TERMINATION",
    "PERF_PRELIM",
    "SALES_VARIATION",
    "DILUTION",
}

ROUTINE_EVENT_TYPES = {
    "INSIDER_OWNERSHIP",
    "LARGE_HOLDER",
    "CORRECTION",
}


def _event_group(event_type: str) -> str:
    if event_type in CAPITAL_ACTION_EVENT_TYPES:
        return "capital_actions"
    if event_type in OPERATING_EVENT_TYPES:
        return "operating_updates"
    if event_type in ROUTINE_EVENT_TYPES:
        return "routine_filings"
    return "other_disclosures"


class LocalEventState:
    def __init__(self, path: str = STATE_PATH):
        self.path = path
        self._payload = self._load()

    def _load(self) -> Dict[str, Dict[str, str]]:
        if not os.path.exists(self.path):
            return {"seen_rcp": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as fp:
                payload = json.load(fp)
                if isinstance(payload, dict):
                    payload.setdefault("seen_rcp", {})
                    return payload
        except Exception as exc:
            log.warning("collector state load failed: %s", exc)
        return {"seen_rcp": {}}

    @property
    def seen_rcp(self) -> Dict[str, Dict[str, str]]:
        return self._payload["seen_rcp"]

    def has_seen(self, stock_code: str, rcp_no: str) -> bool:
        return f"{stock_code}:{rcp_no}" in self.seen_rcp

    def mark_seen(self, stock_code: str, rcp_no: str, title: str, src: str) -> None:
        self.seen_rcp[f"{stock_code}:{rcp_no}"] = {
            "ts": _now_kst().isoformat(timespec="seconds"),
            "title": title,
            "src": src,
        }

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as fp:
            json.dump(self._payload, fp, ensure_ascii=False, indent=2)


class DisclosureEventCollector:
    def __init__(self, *, markets: Optional[list[str]] = None, max_pages: int = 2):
        self.markets = set(markets or ["KOSPI", "KOSDAQ"])
        self.max_pages = max_pages
        self.html = DARTRecentHtmlClient()
        self.event_logger = DisclosureEventLogger()
        self.state = LocalEventState()
        self.name_to_code = self._load_name_to_code_map()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def _load_name_to_code_map(self) -> Dict[str, str]:
        if os.path.exists(NAME_MAP_CACHE_PATH):
            try:
                with open(NAME_MAP_CACHE_PATH, "r", encoding="utf-8") as fp:
                    cached = json.load(fp)
                if isinstance(cached, dict) and cached.get("name_to_code"):
                    return dict(cached["name_to_code"])
            except Exception as exc:
                log.warning("name map cache load failed: %s", exc)

        log.info("building company-name map from corpCode.xml ...")
        _, stock_to_name = load_corp_code_maps(SETTINGS.OPEN_DART_API_KEY)
        name_to_code = {
            norm_company_key(name): code
            for code, name in stock_to_name.items()
            if name and norm_company_key(name)
        }
        try:
            os.makedirs(os.path.dirname(NAME_MAP_CACHE_PATH), exist_ok=True)
            with open(NAME_MAP_CACHE_PATH, "w", encoding="utf-8") as fp:
                json.dump(
                    {
                        "cached_at": _now_kst().isoformat(timespec="seconds"),
                        "size": len(name_to_code),
                        "name_to_code": name_to_code,
                    },
                    fp,
                    ensure_ascii=False,
                )
        except Exception as exc:
            log.warning("name map cache save failed: %s", exc)
        return name_to_code

    def _resolve_code(self, company_name: str) -> Optional[str]:
        return self.name_to_code.get(norm_company_key(company_name))

    def _fetch_api_items_for_date(self, target_date: date) -> list[dict]:
        ymd = target_date.strftime("%Y%m%d")
        params = {
            "crtfc_key": SETTINGS.OPEN_DART_API_KEY,
            "bgn_de": ymd,
            "end_de": ymd,
            "page_no": "1",
            "page_count": "100",
            "last_reprt_at": "Y",
            "sort": "date",
            "sort_mth": "desc",
        }
        out: list[dict] = []
        for corp_cls in ("Y", "K"):
            params["corp_cls"] = corp_cls
            try:
                resp = self.session.get("https://opendart.fss.or.kr/api/list.json", params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("OpenDART list fallback failed: date=%s cls=%s err=%s", ymd, corp_cls, exc)
                continue
            if str(data.get("status")) != "000":
                continue
            out.extend(data.get("list", []) or [])
        return out

    def collect_for_date(self, target_date: date, *, ignore_seen: bool = False) -> int:
        items = self.html.fetch_items_for_date(target_date, max_pages=self.max_pages, respect_total_pages=False)
        items_sorted = sorted(items, key=lambda x: (x.rcv_date, x.time_hhmm, x.rcp_no))
        collected = 0
        collected_keys: set[tuple[str, str]] = set()

        def append_event(
            *,
            stock_code: str,
            corp_name: str,
            rcp_no: str,
            title: str,
            src: str,
            event_date: str,
            event_time_hhmm: str,
            market_tag: str,
            market_name: str,
            submitter: str,
            collector_mode: str,
        ) -> int:
            nonlocal collected
            event_type = classify_disclosure_event(title or "")
            if event_type is None:
                return 0
            if not stock_code:
                return 0
            dedup_key = (stock_code, rcp_no)
            if dedup_key in collected_keys:
                return 0
            if not ignore_seen and self.state.has_seen(stock_code, rcp_no):
                return 0

            payload = {
                "stock_code": stock_code,
                "corp_name": corp_name,
                "rcp_no": rcp_no,
                "title": title,
                "src": src,
                "event_type": event_type,
                "signal_bias": _default_signal_bias(event_type),
                "reason": "classified_from_title",
                "strategy_name": "classified_only",
                "trade_executed": False,
                "trade_action": "",
                "allow_trade": False,
                "recovery_started": False,
                "event_date": event_date,
                "event_time_hhmm": event_time_hhmm,
                "metrics": {
                    "market_tag": market_tag,
                    "market_name": market_name,
                    "submitter": submitter,
                    "collector_mode": collector_mode,
                    "event_group": _event_group(event_type),
                    "is_correction_title": bool(_is_correction_title(title)),
                },
                "tags": ["event_collector", "title_only", _event_group(event_type)],
            }
            self.event_logger.append(payload)
            self.state.mark_seen(stock_code, rcp_no, title, src)
            collected_keys.add(dedup_key)
            collected += 1
            return 1

        for item in items_sorted:
            market_name = MARKET_TAG_MAP.get(item.market_tag, "")
            if self.markets and market_name and market_name not in self.markets:
                continue

            stock_code = self._resolve_code(item.company)
            if not stock_code:
                continue

            append_event(
                stock_code=stock_code,
                corp_name=item.company,
                rcp_no=item.rcp_no,
                title=item.title,
                src="EVENT_COLLECTOR_HTML",
                event_date=(item.rcv_date or date_to_select_date(target_date)).replace(".", "-"),
                event_time_hhmm=item.time_hhmm or "",
                market_tag=item.market_tag,
                market_name=market_name,
                submitter=item.submitter,
                collector_mode="title_classification",
            )

        api_items = sorted(
            self._fetch_api_items_for_date(target_date),
            key=lambda x: (str(x.get("rcept_dt") or ""), str(x.get("rcept_no") or "")),
        )
        for item in api_items:
            stock_code = str(item.get("stock_code") or "").strip().zfill(6)
            if not stock_code or not stock_code.isdigit():
                continue
            market_name = MARKET_TAG_MAP.get(str(item.get("rm") or "").strip(), "")
            if self.markets and market_name and market_name not in self.markets:
                continue
            title = str(item.get("report_nm") or "").strip()
            rcp_no = str(item.get("rcept_no") or "").strip()
            if not rcp_no:
                continue

            append_event(
                stock_code=stock_code,
                corp_name=str(item.get("corp_name") or stock_code),
                rcp_no=rcp_no,
                title=title,
                src="EVENT_COLLECTOR_API",
                event_date=f"{str(item.get('rcept_dt') or '')[:4]}-{str(item.get('rcept_dt') or '')[4:6]}-{str(item.get('rcept_dt') or '')[6:8]}",
                event_time_hhmm="",
                market_tag=str(item.get("rm") or ""),
                market_name=market_name,
                submitter=str(item.get("flr_nm") or ""),
                collector_mode="api_title_classification",
            )

        if collected:
            self.state.save()
        return collected

    def collect_live_with_fallback(self, *, ignore_seen: bool = False, lookback_days: int = 7) -> tuple[int, date]:
        today = _now_kst().date()
        for offset in range(max(1, int(lookback_days))):
            target_date = today - timedelta(days=offset)
            count = self.collect_for_date(target_date, ignore_seen=ignore_seen)
            if count > 0:
                return count, target_date
        return 0, today


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect classified DART disclosure events without trading.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--poll-sec", type=int, default=30, help="Polling interval during regular hours.")
    parser.add_argument("--off-hours-poll-sec", type=int, default=300, help="Polling interval outside regular hours.")
    parser.add_argument("--max-pages", type=int, default=2, help="How many recent DART pages to scan.")
    parser.add_argument("--backfill-days", type=int, default=0, help="Backfill N calendar days before live mode.")
    parser.add_argument("--start-date", default="", help="Optional inclusive start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default="", help="Optional inclusive end date YYYY-MM-DD.")
    parser.add_argument("--markets", default="KOSPI,KOSDAQ", help="Comma-separated market filter.")
    parser.add_argument("--ignore-seen", action="store_true", help="Ignore local seen state and append duplicates.")
    return parser.parse_args()


def _iter_dates(args: argparse.Namespace) -> list[date]:
    if args.start_date or args.end_date:
        end_d = _parse_date(args.end_date) if args.end_date else _now_kst().date()
        start_d = _parse_date(args.start_date) if args.start_date else end_d
    elif args.backfill_days > 0:
        end_d = _now_kst().date()
        start_d = end_d - timedelta(days=max(0, args.backfill_days - 1))
    else:
        return []

    dates = []
    current = start_d
    while current <= end_d:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _is_active_hours(now: Optional[datetime] = None) -> bool:
    now = now or _now_kst()
    return 7 <= now.hour < 21


def collect_once(args: argparse.Namespace | None = None, **kwargs) -> int:
    if args is None:
        args = argparse.Namespace(
            once=True,
            poll_sec=int(kwargs.pop("poll_sec", 30)),
            off_hours_poll_sec=int(kwargs.pop("off_hours_poll_sec", 300)),
            max_pages=int(kwargs.pop("max_pages", 2)),
            backfill_days=int(kwargs.pop("backfill_days", 0)),
            start_date=str(kwargs.pop("start_date", "")),
            end_date=str(kwargs.pop("end_date", "")),
            markets=",".join(kwargs.pop("markets", ["KOSPI", "KOSDAQ"])),
            ignore_seen=bool(kwargs.pop("ignore_seen", False)),
        )
        if kwargs:
            raise TypeError(f"unexpected keyword arguments: {', '.join(sorted(kwargs.keys()))}")

    markets = [item.strip() for item in str(args.markets).split(",") if item.strip()]
    collector = DisclosureEventCollector(markets=markets, max_pages=args.max_pages)
    total = 0

    for target_date in _iter_dates(args):
        count = collector.collect_for_date(target_date, ignore_seen=args.ignore_seen)
        log.info("backfill collected: date=%s count=%d", target_date.isoformat(), count)
        total += count

    today_count, used_date = collector.collect_live_with_fallback(ignore_seen=args.ignore_seen, lookback_days=7)
    log.info("live collected: requested_date=%s used_date=%s count=%d", _now_kst().date().isoformat(), used_date.isoformat(), today_count)
    total += today_count
    return total


def run_scheduler(args: argparse.Namespace) -> None:
    markets = [item.strip() for item in str(args.markets).split(",") if item.strip()]
    collector = DisclosureEventCollector(markets=markets, max_pages=args.max_pages)

    for target_date in _iter_dates(args):
        count = collector.collect_for_date(target_date, ignore_seen=args.ignore_seen)
        log.info("bootstrap collected: date=%s count=%d", target_date.isoformat(), count)

    while True:
        now = _now_kst()
        try:
            count, used_date = collector.collect_live_with_fallback(ignore_seen=args.ignore_seen, lookback_days=7)
            if count:
                log.info("scheduled collection appended %d rows (used_date=%s)", count, used_date.isoformat())
        except Exception:
            log.exception("scheduled event collection failed")

        sleep_sec = args.poll_sec if _is_active_hours(now) else args.off_hours_poll_sec
        time.sleep(max(5, int(sleep_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.once:
        collect_once(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
