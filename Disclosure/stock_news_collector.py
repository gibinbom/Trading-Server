from __future__ import annotations
import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

try:
    from factor_pipeline import FactorSnapshotBuilder
    from stock_news_pipeline import ROOT_DIR as NEWS_ROOT_DIR, append_stock_news_packet
    from stock_news_fetchers import collect_google_headlines, collect_naver_headlines
except Exception:
    from Disclosure.factor_pipeline import FactorSnapshotBuilder
    from Disclosure.stock_news_pipeline import ROOT_DIR as NEWS_ROOT_DIR, append_stock_news_packet
    from Disclosure.stock_news_fetchers import collect_google_headlines, collect_naver_headlines
log = logging.getLogger("disclosure.stock_news_collector")
KST = timezone(timedelta(hours=9), name="KST")
STATE_PATH = os.path.join(NEWS_ROOT_DIR, "collector_state.json")
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
def _now_kst() -> datetime:
    return datetime.now(KST)
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
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def has(self, key: str) -> bool:
        return key in self.seen

    def add(self, key: str) -> None:
        self.seen[key] = _now_kst().isoformat(timespec="seconds")

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as fp:
            json.dump(self.seen, fp, ensure_ascii=False, indent=2)
class StockNewsCollector:
    def __init__(
        self,
        *,
        top_n: int = 0,
        min_marcap: int = 0,
        markets: list[str] | None = None,
        google_top_n: int = 300,
        google_limit: int = 3,
        naver_limit: int = 5,
        news_days: int = 7,
        sources: list[str] | None = None,
        sleep_sec: float = 0.05,
    ):
        self.top_n = int(top_n)
        self.min_marcap = int(min_marcap)
        self.markets = markets or ["KOSPI", "KOSDAQ"]
        self.google_top_n = int(max(0, google_top_n))
        self.google_limit = int(max(1, google_limit))
        self.naver_limit = int(max(1, naver_limit))
        self.news_days = int(max(1, news_days))
        self.sources = {item.lower().strip() for item in (sources or ["naver", "google"]) if item}
        self.sleep_sec = float(max(0.0, sleep_sec))
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.state = CollectorState()

    def _load_universe(self) -> list[dict]:
        builder = FactorSnapshotBuilder(
            top_n=self.top_n,
            min_marcap_krw=self.min_marcap,
            markets=self.markets,
            include_flow=False,
            include_consensus=False,
            include_news=False,
            price_sleep_sec=0.0,
            flow_sleep_sec=0.0,
        )
        df = builder._load_universe_df()[["symbol", "name", "market", "sector", "marcap"]].copy()
        return df.to_dict("records")

    def _append_if_new(self, source: str, symbol: str, stock_name: str, title: str, published_at: str, **extra) -> bool:
        key = "|".join([source, symbol, title.strip(), published_at[:10]])
        if self.state.has(key):
            return False
        ok = append_stock_news_packet(
            {
                "id": source,
                "code": symbol,
                "name": stock_name,
                "title": title,
                "published_at": published_at,
                **extra,
            },
            source=source,
        )
        if ok:
            self.state.add(key)
        return ok

    def _collect_naver(self, symbol: str, stock_name: str) -> int:
        count = 0
        for item in collect_naver_headlines(self.session, symbol, stock_name, limit=self.naver_limit):
            count += int(
                self._append_if_new(
                    item["source"],
                    item["symbol"],
                    item["stock_name"],
                    item["title"],
                    item["published_at"],
                    link=item.get("link", ""),
                    provider=item.get("provider", ""),
                )
            )
        return count

    def _collect_google(self, symbol: str, stock_name: str) -> int:
        count = 0
        for item in collect_google_headlines(self.session, symbol, stock_name, news_days=self.news_days, limit=self.google_limit):
            count += int(
                self._append_if_new(
                    item["source"],
                    item["symbol"],
                    item["stock_name"],
                    item["title"],
                    item["published_at"],
                    link=item.get("link", ""),
                    provider=item.get("provider", ""),
                )
            )
        return count

    def collect_once(self) -> dict[str, int]:
        universe = self._load_universe()
        collected = {"naver": 0, "google": 0, "symbols": len(universe)}
        for idx, row in enumerate(universe, start=1):
            symbol = str(row.get("symbol", "")).zfill(6)
            stock_name = str(row.get("name") or symbol)
            try:
                if "naver" in self.sources:
                    collected["naver"] += self._collect_naver(symbol, stock_name)
                if "google" in self.sources and (self.google_top_n <= 0 or idx <= self.google_top_n):
                    collected["google"] += self._collect_google(symbol, stock_name)
            except Exception as exc:
                log.warning("stock news collect failed for %s(%s): %s", stock_name, symbol, exc)
            if self.sleep_sec > 0:
                time.sleep(self.sleep_sec)
        self.state.save()
        return collected
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect stock-level headlines from Naver Finance and Google News RSS.")
    parser.add_argument("--top-n", type=int, default=0, help="Universe size. 0 means full filtered universe.")
    parser.add_argument("--min-marcap", type=int, default=0, help="Minimum market cap filter in KRW.")
    parser.add_argument("--markets", default="KOSPI,KOSDAQ", help="Comma-separated market list.")
    parser.add_argument("--sources", default="naver,google", help="Comma-separated sources: naver,google")
    parser.add_argument("--google-top-n", type=int, default=300, help="Only query Google for top-N names by marcap. 0 means all.")
    parser.add_argument("--google-limit", type=int, default=3, help="Max Google headlines per symbol.")
    parser.add_argument("--naver-limit", type=int, default=5, help="Max Naver headlines per symbol.")
    parser.add_argument("--news-days", type=int, default=7, help="Google RSS lookback window.")
    parser.add_argument("--sleep-sec", type=float, default=0.03, help="Sleep between symbol requests.")
    return parser.parse_args()
def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    collector = StockNewsCollector(
        top_n=args.top_n,
        min_marcap=args.min_marcap,
        markets=[item.strip() for item in str(args.markets).split(",") if item.strip()],
        sources=[item.strip() for item in str(args.sources).split(",") if item.strip()],
        google_top_n=args.google_top_n,
        google_limit=args.google_limit,
        naver_limit=args.naver_limit,
        news_days=args.news_days,
        sleep_sec=args.sleep_sec,
    )
    result = collector.collect_once()
    log.info("stock news collected: symbols=%s naver=%s google=%s", result["symbols"], result["naver"], result["google"])
if __name__ == "__main__":
    main()
