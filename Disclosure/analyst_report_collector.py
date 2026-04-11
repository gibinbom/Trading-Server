from __future__ import annotations

import argparse
import csv
import glob
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Any

import requests

try:
    from analyst_report_fetchers import (
        fetch_fnguide_report_summary,
        fetch_hankyung_company_list,
        fetch_naver_company_list,
        fetch_naver_report_detail,
    )
    from analyst_report_pipeline import RAW_DIR, SUMMARY_DIR
except Exception:
    from Disclosure.analyst_report_fetchers import (
        fetch_fnguide_report_summary,
        fetch_hankyung_company_list,
        fetch_naver_company_list,
        fetch_naver_report_detail,
    )
    from Disclosure.analyst_report_pipeline import RAW_DIR, SUMMARY_DIR


log = logging.getLogger("disclosure.analyst_report_collector")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_DIR = os.path.dirname(ROOT_DIR)
LISTING_PATH = os.path.join(PROJECT_ROOT_DIR, "krx_listing.csv")
STATE_PATH = os.path.join(os.path.dirname(RAW_DIR), "collector_state.json")
COVERAGE_PATH = os.path.join(SUMMARY_DIR, "analyst_report_backfill_coverage_latest.json")
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
DEFAULT_SOURCES = ("naver", "hankyung", "fnguide")
DEFAULT_BACKFILL_MAX_PAGES = 120


def _now_local() -> datetime:
    return datetime.now().astimezone()


class CollectorState:
    def __init__(self, path: str = STATE_PATH):
        self.path = path
        self.seen = self._load()

    def _load(self) -> dict[str, str]:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if isinstance(payload, dict) and "source_seen" in payload and isinstance(payload["source_seen"], dict):
                return {str(key): str(value) for key, value in payload["source_seen"].items()}
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def has(self, key: str) -> bool:
        return key in self.seen

    def add(self, key: str, published_at: str) -> None:
        self.seen[key] = str(published_at)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as fp:
            json.dump({"source_seen": self.seen}, fp, ensure_ascii=False, indent=2)


def _append_raw_report(payload: dict[str, Any]) -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    stamp = str(payload.get("published_at", ""))[:10].replace("-", "") or time.strftime("%Y%m%d")
    path = os.path.join(RAW_DIR, f"analyst_reports_{stamp}.jsonl")
    with open(path, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_title_key(value: Any) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"^\[[^\]]+\]\s*", "", text)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[·ㆍ･・,:;'\"]", "", text)
    text = re.sub(r"[\(\)\[\]\{\}\-_/]", "", text)
    return text


def _normalize_broker_key(value: Any) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    text = text.replace("(주)", "").replace("주식회사", "")
    text = text.replace("증권중개", "").replace("투자", "투자")
    text = re.sub(r"\s+", "", text)
    replacements = {
        "kb증권": "kb증권",
        "케이비증권": "kb증권",
        "nh투자증권": "nh투자증권",
        "엔에이치투자증권": "nh투자증권",
        "미래에셋증권": "미래에셋증권",
        "한국투자증권": "한국투자증권",
        "메리츠증권": "메리츠증권",
        "신한투자증권": "신한투자증권",
        "대신증권": "대신증권",
        "삼성증권": "삼성증권",
        "현대차증권": "현대차증권",
    }
    return replacements.get(text, text)


def _parse_target_price(value: Any) -> str:
    digits = re.sub(r"[^\d]", "", str(value or ""))
    return digits or ""


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _source_key(item: dict[str, Any]) -> str:
    source = _clean_text(item.get("source")).upper()
    if source == "NAVER_RESEARCH" and _clean_text(item.get("nid")):
        return f"NAVER:{_clean_text(item.get('nid'))}"
    if source == "HANKYUNG_CONSENSUS" and _clean_text(item.get("report_idx")):
        return f"HANKYUNG:{_clean_text(item.get('report_idx'))}"
    if source == "FNGUIDE_CONSENSUS" and _clean_text(item.get("report_id")):
        return f"FNGUIDE:{_clean_text(item.get('report_id'))}"
    return ""


def _canonical_key(item: dict[str, Any]) -> str:
    symbol = "".join(ch for ch in str(item.get("symbol") or "") if ch.isdigit()).zfill(6)
    broker = _normalize_broker_key(item.get("broker") or item.get("writer"))
    title = _normalize_title_key(item.get("title"))
    published = _clean_text(item.get("published_at"))[:10]
    target = _parse_target_price(item.get("target_price"))
    if not symbol or not title or not published:
        return ""
    return "|".join([symbol, broker, title, published, target])


def _load_existing_canonical_keys(days: int) -> set[str]:
    cutoff = _now_local().replace(tzinfo=None) - timedelta(days=max(30, int(days)))
    seen: set[str] = set()
    for path in sorted(glob.glob(os.path.join(RAW_DIR, "analyst_reports_*.jsonl"))):
        stamp_match = re.search(r"(\d{8})", os.path.basename(path))
        if stamp_match:
            try:
                stamp_dt = datetime.strptime(stamp_match.group(1), "%Y%m%d")
                if stamp_dt < cutoff:
                    continue
            except Exception:
                pass
        try:
            with open(path, "r", encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    key = _canonical_key(payload)
                    if key:
                        seen.add(key)
        except Exception:
            continue
    return seen


def _load_listing_symbols(limit: int = 0) -> list[str]:
    if not os.path.exists(LISTING_PATH):
        return []
    symbols: list[str] = []
    try:
        with open(LISTING_PATH, "r", encoding="utf-8-sig") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                market = _clean_text(row.get("Market"))
                code = "".join(ch for ch in str(row.get("Code") or "") if ch.isdigit()).zfill(6)
                if not code or market not in {"KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"}:
                    continue
                symbols.append(code)
                if limit > 0 and len(symbols) >= limit:
                    break
    except Exception:
        return []
    return symbols


def _empty_source_stats(source: str, days_requested: int) -> dict[str, Any]:
    return {
        "source": source,
        "days_requested": days_requested,
        "rows_fetched": 0,
        "rows_saved": 0,
        "rows_deduped": 0,
        "rows_filtered_old": 0,
        "oldest_published_at": "",
        "newest_published_at": "",
    }


def _coverage_window_paths(days: int) -> list[str]:
    cutoff = datetime.now() - timedelta(days=max(1, int(days)))
    paths: list[str] = []
    for path in sorted(glob.glob(os.path.join(RAW_DIR, "analyst_reports_*.jsonl"))):
        stamp_match = re.search(r"(\d{8})", os.path.basename(path))
        if not stamp_match:
            paths.append(path)
            continue
        try:
            stamp_dt = datetime.strptime(stamp_match.group(1), "%Y%m%d")
        except Exception:
            paths.append(path)
            continue
        if stamp_dt >= cutoff:
            paths.append(path)
    return paths


def _build_coverage_summary(days: int) -> dict[str, Any]:
    canonical_rows: dict[str, dict[str, Any]] = {}
    cutoff_dt = _now_local() - timedelta(days=max(1, int(days)))
    for path in _coverage_window_paths(days):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    published_at = _clean_text(row.get("published_at"))
                    published_dt = _parse_iso_datetime(published_at)
                    if published_dt is None or published_dt < cutoff_dt:
                        continue
                    key = _canonical_key(row) or f"{_clean_text(row.get('source'))}|{published_at}|{_clean_text(row.get('title'))}"
                    current = canonical_rows.get(key)
                    candidate = row
                    if current is None:
                        canonical_rows[key] = candidate
                        continue
                    current_score = int(bool(_clean_text(current.get("detail_url")))) + int(bool(_clean_text(current.get("pdf_url"))))
                    current_score += int(bool(_parse_target_price(current.get("target_price")))) + int(bool(_clean_text(current.get("rating"))))
                    candidate_score = int(bool(_clean_text(candidate.get("detail_url")))) + int(bool(_clean_text(candidate.get("pdf_url"))))
                    candidate_score += int(bool(_parse_target_price(candidate.get("target_price")))) + int(bool(_clean_text(candidate.get("rating"))))
                    if candidate_score > current_score:
                        canonical_rows[key] = candidate
        except Exception:
            continue

    by_source: dict[str, int] = {}
    by_date: dict[str, int] = {}
    priced_count = 0
    for row in canonical_rows.values():
        source = _clean_text(row.get("source")) or "UNKNOWN"
        published_date = _clean_text(row.get("published_at"))[:10] or "unknown"
        by_source[source] = by_source.get(source, 0) + 1
        by_date[published_date] = by_date.get(published_date, 0) + 1
        if _parse_target_price(row.get("target_price")):
            priced_count += 1

    total_count = len(canonical_rows)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "days_requested": days,
        "report_count": total_count,
        "priced_report_count": priced_count,
        "priced_report_coverage_ratio": round((priced_count / total_count) * 100, 2) if total_count else 0.0,
        "source_counts": dict(sorted(by_source.items(), key=lambda item: item[0])),
        "daily_counts": dict(sorted(by_date.items(), key=lambda item: item[0])),
    }


def _save_coverage_summary(payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(COVERAGE_PATH), exist_ok=True)
    with open(COVERAGE_PATH, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


class AnalystReportCollector:
    def __init__(
        self,
        *,
        naver_pages: int = 3,
        hankyung_pages: int = 3,
        hankyung_days: int = 7,
        backfill_days: int = 0,
        max_pages_per_source: int = DEFAULT_BACKFILL_MAX_PAGES,
        sleep_sec: float = 0.05,
        ignore_state: bool = False,
        sources: tuple[str, ...] = DEFAULT_SOURCES,
        fnguide_symbol_limit: int = 0,
        fnguide_symbols: tuple[str, ...] = (),
    ):
        self.naver_pages = int(max(1, naver_pages))
        self.hankyung_pages = int(max(1, hankyung_pages))
        self.hankyung_days = int(max(1, hankyung_days))
        self.backfill_days = int(max(0, backfill_days))
        self.max_pages_per_source = int(max(1, max_pages_per_source))
        self.sleep_sec = float(max(0.0, sleep_sec))
        self.ignore_state = bool(ignore_state)
        self.sources = tuple(source for source in sources if source in DEFAULT_SOURCES) or DEFAULT_SOURCES
        self.fnguide_symbol_limit = int(max(0, fnguide_symbol_limit))
        self.fnguide_symbols = tuple("".join(ch for ch in str(item or "") if ch.isdigit()).zfill(6) for item in fnguide_symbols if _clean_text(item))
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.state = CollectorState()
        self.cutoff_dt = _now_local() - timedelta(days=self.backfill_days) if self.backfill_days > 0 else None
        self.canonical_seen = _load_existing_canonical_keys(max(self.backfill_days, 120))
        self.source_stats = {source: _empty_source_stats(source, self.backfill_days or self.hankyung_days) for source in self.sources}

    def _touch_stats(self, source: str, item: dict[str, Any], *, saved: bool = False, deduped: bool = False, old_filtered: bool = False) -> None:
        stats = self.source_stats[source]
        stats["rows_fetched"] += 1
        published_at = _clean_text(item.get("published_at"))
        if published_at:
            if not stats["oldest_published_at"] or published_at < stats["oldest_published_at"]:
                stats["oldest_published_at"] = published_at
            if not stats["newest_published_at"] or published_at > stats["newest_published_at"]:
                stats["newest_published_at"] = published_at
        if saved:
            stats["rows_saved"] += 1
        if deduped:
            stats["rows_deduped"] += 1
        if old_filtered:
            stats["rows_filtered_old"] += 1

    def _is_older_than_cutoff(self, published_at: Any) -> bool:
        if self.cutoff_dt is None:
            return False
        published_dt = _parse_iso_datetime(published_at)
        if published_dt is None:
            return False
        return published_dt < self.cutoff_dt

    def _save_if_new(self, item: dict[str, Any]) -> bool:
        source_key = _source_key(item)
        if source_key and not self.ignore_state and self.state.has(source_key):
            return False
        canonical_key = _canonical_key(item)
        if canonical_key and canonical_key in self.canonical_seen:
            if source_key:
                self.state.add(source_key, item.get("published_at", ""))
            return False
        _append_raw_report(item)
        if source_key:
            self.state.add(source_key, item.get("published_at", ""))
        if canonical_key:
            self.canonical_seen.add(canonical_key)
        return True

    def _collect_naver_rows(self) -> int:
        saved = 0
        max_pages = self.max_pages_per_source if self.backfill_days > 0 else self.naver_pages
        for page in range(1, max_pages + 1):
            try:
                rows = fetch_naver_company_list(self.session, page=page)
            except Exception as exc:
                log.warning("naver list fetch failed (page=%s): %s", page, exc)
                continue
            if not rows:
                break
            all_old = True
            for row in rows:
                try:
                    detail = fetch_naver_report_detail(self.session, row["detail_url"])
                except Exception as exc:
                    log.warning("naver detail fetch failed (%s): %s", row.get("detail_url", ""), exc)
                    continue
                item = {**row, **detail, "source": "NAVER_RESEARCH"}
                old_filtered = self._is_older_than_cutoff(item.get("published_at"))
                self._touch_stats("naver", item, old_filtered=old_filtered)
                if old_filtered:
                    continue
                all_old = False
                if self._save_if_new(item):
                    self.source_stats["naver"]["rows_saved"] += 1
                    saved += 1
                else:
                    self.source_stats["naver"]["rows_deduped"] += 1
                if self.sleep_sec > 0:
                    time.sleep(self.sleep_sec)
            if self.backfill_days > 0 and all_old:
                break
        return saved

    def _collect_hankyung_rows(self) -> int:
        saved = 0
        lookback_days = self.backfill_days or self.hankyung_days
        max_pages = self.max_pages_per_source if self.backfill_days > 0 else self.hankyung_pages
        for page in range(1, max_pages + 1):
            try:
                rows = fetch_hankyung_company_list(self.session, page=page, lookback_days=lookback_days)
            except Exception as exc:
                log.warning("hankyung list fetch failed (page=%s): %s", page, exc)
                continue
            if not rows:
                break
            all_old = True
            for item in rows:
                old_filtered = self._is_older_than_cutoff(item.get("published_at"))
                self._touch_stats("hankyung", item, old_filtered=old_filtered)
                if old_filtered:
                    continue
                all_old = False
                if self._save_if_new(item):
                    self.source_stats["hankyung"]["rows_saved"] += 1
                    saved += 1
                else:
                    self.source_stats["hankyung"]["rows_deduped"] += 1
                if self.sleep_sec > 0:
                    time.sleep(self.sleep_sec)
            if self.backfill_days > 0 and all_old:
                break
        return saved

    def _candidate_fnguide_symbols(self) -> list[str]:
        if self.fnguide_symbols:
            symbols = sorted(set(symbol for symbol in self.fnguide_symbols if symbol.strip("0")))
            if self.fnguide_symbol_limit > 0:
                symbols = symbols[: self.fnguide_symbol_limit]
            return symbols
        if self.backfill_days > 0:
            return _load_listing_symbols(limit=self.fnguide_symbol_limit)
        recent_symbols: list[str] = []
        for path in _coverage_window_paths(30):
            try:
                with open(path, "r", encoding="utf-8") as fp:
                    for line in fp:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except Exception:
                            continue
                        symbol = "".join(ch for ch in str(row.get("symbol") or "") if ch.isdigit()).zfill(6)
                        if symbol:
                            recent_symbols.append(symbol)
            except Exception:
                continue
        ordered = sorted(set(recent_symbols))
        if self.fnguide_symbol_limit > 0:
            ordered = ordered[: self.fnguide_symbol_limit]
        return ordered

    def _collect_fnguide_rows(self) -> int:
        saved = 0
        symbols = self._candidate_fnguide_symbols()
        if not symbols:
            return 0
        for index, symbol in enumerate(symbols, start=1):
            try:
                rows = fetch_fnguide_report_summary(self.session, symbol=symbol)
            except Exception as exc:
                log.debug("fnguide fetch failed (%s): %s", symbol, exc)
                continue
            for item in rows:
                old_filtered = self._is_older_than_cutoff(item.get("published_at"))
                self._touch_stats("fnguide", item, old_filtered=old_filtered)
                if old_filtered:
                    continue
                if self._save_if_new(item):
                    self.source_stats["fnguide"]["rows_saved"] += 1
                    saved += 1
                else:
                    self.source_stats["fnguide"]["rows_deduped"] += 1
            if self.sleep_sec > 0:
                time.sleep(self.sleep_sec)
            if index % 200 == 0:
                log.info("fnguide progress: %s/%s symbols", index, len(symbols))
        return saved

    def collect_once(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if "naver" in self.sources:
            result["naver"] = self._collect_naver_rows()
        if "hankyung" in self.sources:
            result["hankyung"] = self._collect_hankyung_rows()
        if "fnguide" in self.sources:
            result["fnguide"] = self._collect_fnguide_rows()
        self.state.save()
        coverage = _build_coverage_summary(self.backfill_days or 90)
        coverage["sources"] = {source: self.source_stats[source] for source in self.sources}
        _save_coverage_summary(coverage)
        result["coverage_path"] = COVERAGE_PATH
        result["source_stats"] = {source: self.source_stats[source] for source in self.sources}
        return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Korean analyst reports from Naver, Hankyung, and FnGuide public consensus pages.")
    parser.add_argument("--naver-pages", type=int, default=3, help="How many Naver research pages to scan for incremental collection.")
    parser.add_argument("--hankyung-pages", type=int, default=3, help="How many Hankyung list pages to scan for incremental collection.")
    parser.add_argument("--hankyung-days", type=int, default=7, help="Hankyung lookback window in days for incremental collection.")
    parser.add_argument("--backfill-days", type=int, default=0, help="Rebuild a historical window by scanning each source until this day cutoff.")
    parser.add_argument("--max-pages-per-source", type=int, default=DEFAULT_BACKFILL_MAX_PAGES, help="Maximum pages per source when backfill mode is enabled.")
    parser.add_argument("--ignore-state", action="store_true", help="Ignore source-level seen state during backfill collection.")
    parser.add_argument("--sources", default="naver,hankyung,fnguide", help="Comma-separated sources: naver,hankyung,fnguide")
    parser.add_argument("--fnguide-symbol-limit", type=int, default=0, help="Optional symbol cap for FnGuide collection. 0 means all available candidates.")
    parser.add_argument("--fnguide-symbols", default="", help="Optional comma-separated symbol override for FnGuide collection.")
    parser.add_argument("--sleep-sec", type=float, default=0.03, help="Delay between requests.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    sources = tuple(item.strip().lower() for item in str(args.sources).split(",") if item.strip())
    collector = AnalystReportCollector(
        naver_pages=args.naver_pages,
        hankyung_pages=args.hankyung_pages,
        hankyung_days=args.hankyung_days,
        backfill_days=args.backfill_days,
        max_pages_per_source=args.max_pages_per_source,
        sleep_sec=args.sleep_sec,
        ignore_state=args.ignore_state,
        sources=sources,
        fnguide_symbol_limit=args.fnguide_symbol_limit,
        fnguide_symbols=tuple(item.strip() for item in str(args.fnguide_symbols).split(",") if item.strip()),
    )
    result = collector.collect_once()
    flat_counts = {source: result.get(source, 0) for source in sources}
    log.info("analyst reports collected: %s", json.dumps(flat_counts, ensure_ascii=False))
    log.info("analyst coverage summary written: %s", result.get("coverage_path"))


if __name__ == "__main__":
    main()
