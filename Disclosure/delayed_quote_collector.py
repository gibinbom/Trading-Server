from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from kis_broker_factory import build_kis_broker_from_settings
    from naver_intraday_fallback import fetch_naver_intraday_history
    from naver_price_fallback import fetch_naver_daily_price_history, fetch_naver_quote_snapshot
    from runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from valuation_refresh_support import load_high_turnover_symbols, load_tp_visible_symbols
except Exception:
    from Disclosure.kis_broker_factory import build_kis_broker_from_settings
    from Disclosure.naver_intraday_fallback import fetch_naver_intraday_history
    from Disclosure.naver_price_fallback import fetch_naver_daily_price_history, fetch_naver_quote_snapshot
    from Disclosure.runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from Disclosure.valuation_refresh_support import load_high_turnover_symbols, load_tp_visible_symbols


log = logging.getLogger("disclosure.delayed_quote_collector")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LISTING_PATH = os.path.join(os.path.dirname(ROOT_DIR), "krx_listing.csv")
OUTPUT_DIR = os.path.join(RUNTIME_DIR, "web_projections")
LATEST_JSON_PATH = os.path.join(OUTPUT_DIR, "quote_delayed_source_latest.json")
LATEST_CSV_PATH = os.path.join(OUTPUT_DIR, "quote_delayed_source_latest.csv")
DASHBOARD_JSON_PATH = os.path.join(OUTPUT_DIR, "dashboard_latest.json")
SECTOR_DASHBOARD_JSON_PATH = os.path.join(OUTPUT_DIR, "sector_dashboard_latest.json")
HOTSET_JSON_PATH = os.path.join(OUTPUT_DIR, "quote_delayed_hotset_latest.json")
HOTSET_CSV_PATH = os.path.join(OUTPUT_DIR, "quote_delayed_hotset_latest.csv")
STALE_OFFICIAL_CLOSE_BUSINESS_DAYS = 3
MIN_DELAYED_COVERAGE_RATIO = 0.05
MAX_CARRY_FORWARD_BUSINESS_DAYS = 1
DEFAULT_QUOTE_WORKERS = 6
MAX_QUOTE_WORKERS = 12
KIS_FAILURES_TO_OPEN = 5
KIS_OPEN_STATUS_CODES = {401, 403, 429, 500, 502, 503, 504}
KIS_DISABLED = os.getenv("DELAYED_QUOTE_DISABLE_KIS", "0") in {"1", "true", "True", "YES", "yes"}
_KIS_BROKER_LOCAL = threading.local()


def _default_schedule_times(mode: str = "full") -> str:
    if mode == "hotset":
        slots: list[str] = []
        for hour in range(9, 16):
            for minute in range(0, 60):
                if hour == 9 and minute < 6:
                    continue
                if minute % 5 == 0:
                    continue
                if hour == 15 and minute > 44:
                    continue
                slots.append(f"{hour:02d}:{minute:02d}")
        return ",".join(slots)
    slots = ["08:15"]
    for hour in range(9, 16):
        for minute in range(0, 60, 5):
            if hour == 9 and minute < 5:
                continue
            if hour == 15 and minute > 45:
                continue
            slots.append(f"{hour:02d}:{minute:02d}")
    slots.append("20:15")
    return ",".join(slots)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect delayed public quote snapshots for web read models.")
    parser.add_argument("--mode", choices=("full", "hotset"), default="full", help="Collect a full-universe snapshot or a 1-minute hotset overlay.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print a short summary after saving.")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip remote quote fetch and build from KRX listing only.")
    parser.add_argument("--limit", type=int, default=0, help="Limit symbols for quick checks.")
    parser.add_argument("--workers", type=int, default=DEFAULT_QUOTE_WORKERS, help="Concurrent delayed quote fetch workers.")
    parser.add_argument("--quote-top-n", type=int, default=300, help="Hotset mode delayed-quote symbol cap from turnover leaders.")
    parser.add_argument("--tp-top-n", type=int, default=200, help="Hotset mode TP-visible symbol cap.")
    parser.add_argument("--sector-watch-top-n", type=int, default=6, help="Hotset mode watched sector cap.")
    parser.add_argument("--sector-watch-per-sector", type=int, default=6, help="Hotset mode per-sector symbol cap.")
    parser.add_argument("--times", default="", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    args = parser.parse_args()
    if not args.times:
        args.times = _default_schedule_times(args.mode)
    return args


def _load_listing() -> pd.DataFrame:
    if not os.path.exists(LISTING_PATH):
        return pd.DataFrame()
    df = pd.read_csv(LISTING_PATH, dtype={"Code": str})
    if df.empty:
        return pd.DataFrame()
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    return df


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except Exception:
        return None


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def _normalize_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _read_rows_payload(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(raw, dict):
        rows = raw.get("rows") or []
        return rows if isinstance(rows, list) else []
    return raw if isinstance(raw, list) else []


def _append_symbol(symbols: list[str], value: Any) -> None:
    symbol = _normalize_symbol(value)
    if symbol and symbol not in symbols:
        symbols.append(symbol)


def _load_dashboard_hotset_symbols() -> list[str]:
    try:
        payload = json.loads(Path(DASHBOARD_JSON_PATH).read_text(encoding="utf-8"))
    except Exception:
        return []
    symbols: list[str] = []
    for key in (
        "discount_leaders",
        "premium_leaders",
        "watch_names",
        "capital_action_highlights",
        "operating_update_highlights",
        "other_disclosure_highlights",
    ):
        for row in payload.get(key) or []:
            _append_symbol(symbols, (row or {}).get("symbol"))
    return symbols


def _load_sector_watch_symbols(*, top_n: int, per_sector: int) -> list[str]:
    if not os.path.exists(SECTOR_DASHBOARD_JSON_PATH):
        return []
    try:
        payload = json.loads(Path(SECTOR_DASHBOARD_JSON_PATH).read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    symbols: list[str] = []
    for sector_doc in payload[: max(0, int(top_n))]:
        for row in (sector_doc or {}).get("watch_symbols") or []:
            _append_symbol(symbols, (row or {}).get("symbol"))
        for bucket_name in ("sector_flow_leaders", "sector_flow_laggards"):
            for row in ((sector_doc or {}).get(bucket_name) or [])[: max(0, int(per_sector))]:
                _append_symbol(symbols, (row or {}).get("symbol"))
    return symbols


def _load_target_symbols(mode: str, *, quote_top_n: int, tp_top_n: int, sector_watch_top_n: int, sector_watch_per_sector: int) -> list[str]:
    if mode == "full":
        return []
    symbols: list[str] = []
    for symbol in load_high_turnover_symbols(limit=quote_top_n):
        _append_symbol(symbols, symbol)
    for symbol in load_tp_visible_symbols(limit=tp_top_n):
        _append_symbol(symbols, symbol)
    for symbol in _load_dashboard_hotset_symbols():
        _append_symbol(symbols, symbol)
    for symbol in _load_sector_watch_symbols(top_n=sector_watch_top_n, per_sector=sector_watch_per_sector):
        _append_symbol(symbols, symbol)
    return symbols


def _output_paths(mode: str) -> tuple[str, str]:
    if mode == "hotset":
        return HOTSET_JSON_PATH, HOTSET_CSV_PATH
    return LATEST_JSON_PATH, LATEST_CSV_PATH


def _load_existing_quote_rows(mode: str) -> list[dict[str, Any]]:
    json_path, _ = _output_paths(mode)
    return _read_rows_payload(json_path)


def _business_days_elapsed(value: Any) -> int | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        captured = pd.Timestamp(text).normalize()
        today = pd.Timestamp.now().normalize()
    except Exception:
        return None
    if pd.isna(captured) or pd.isna(today):
        return None
    if captured > today:
        return 0
    try:
        return max(len(pd.bdate_range(captured, today)) - 1, 0)
    except Exception:
        return None


def _is_stale_official_close(value: Any, *, max_business_days: int = STALE_OFFICIAL_CLOSE_BUSINESS_DAYS) -> bool:
    elapsed = _business_days_elapsed(value)
    return elapsed is None or elapsed > int(max_business_days)


def _is_recent_quote_capture(value: Any, *, max_business_days: int = MAX_CARRY_FORWARD_BUSINESS_DAYS) -> bool:
    elapsed = _business_days_elapsed(value)
    return elapsed is not None and elapsed <= int(max_business_days)


def _quote_workers(workers: int) -> int:
    return max(1, min(int(workers), MAX_QUOTE_WORKERS))


def _get_thread_kis_broker():
    broker = getattr(_KIS_BROKER_LOCAL, "broker", None)
    if broker is None:
        broker = build_kis_broker_from_settings(is_virtual=False, dry_run=True)
        _KIS_BROKER_LOCAL.broker = broker
    return broker


def _extract_http_status(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    text = str(exc or "")
    for code in sorted(KIS_OPEN_STATUS_CODES):
        if str(code) in text:
            return code
    return None


class _KisCircuit:
    def __init__(self, failure_threshold: int = KIS_FAILURES_TO_OPEN):
        self.failure_threshold = max(1, int(failure_threshold))
        self.lock = threading.Lock()
        self.open = False
        self.failures = 0
        self.last_status: int | None = None

    def can_use(self) -> bool:
        with self.lock:
            return not self.open

    def record_success(self) -> None:
        with self.lock:
            self.failures = 0
            self.last_status = None

    def record_failure(self, exc: Exception) -> None:
        status = _extract_http_status(exc)
        with self.lock:
            if status in KIS_OPEN_STATUS_CODES:
                self.failures += 1
                self.last_status = status
                if self.failures >= self.failure_threshold and not self.open:
                    self.open = True
                    log.warning(
                        "KIS delayed quote circuit opened after %s failures (status=%s); falling back to public sources for this run",
                        self.failures,
                        status,
                    )


def _fetch_kis_quote_snapshot(symbol: str) -> dict[str, Any] | None:
    broker = _get_thread_kis_broker()
    ctx = broker._get_market_context()
    mkt_code = "NX" if ctx["exch"] == "NXT" else "J"
    tr_id = "FHKST01010100"
    url = f"{broker.base}/uapi/domestic-stock/v1/quotations/inquire-price"
    params = {
        "fid_cond_mrkt_div_code": mkt_code,
        "fid_input_iscd": symbol,
    }
    resp = broker._call_api_with_retry("GET", url, tr_id, params=params)
    payload = resp.json() or {}
    output = payload.get("output") or {}
    price = _safe_float(output.get("stck_prpr"))
    change_rate = _safe_float(output.get("prdy_ctrt"))
    if price is None or price <= 0:
        return None
    return {
        "price": price,
        "change_rate": change_rate,
        "source": "kis_current",
    }


def _fetch_secondary_public_quote(symbol: str) -> dict[str, Any] | None:
    today = pd.Timestamp.now().date()
    try:
        intraday_df = fetch_naver_intraday_history(symbol, today.isoformat(), sleep_sec=0.0)
    except Exception as exc:
        log.debug("secondary intraday fetch failed for %s: %s", symbol, exc)
        return None
    if intraday_df is None or intraday_df.empty or "Close" not in intraday_df.columns:
        return None
    latest = intraday_df.dropna(subset=["Close"]).sort_values("DateTime").tail(1)
    if latest.empty:
        return None
    price = _safe_float(latest.iloc[0].get("Close"))
    if price is None or price <= 0:
        return None

    change_abs = None
    change_rate = None
    try:
        daily_df = fetch_naver_daily_price_history(
            symbol,
            start_date=today - pd.Timedelta(days=10),
            end_date=today,
            lookback_days=14,
            sleep_sec=0.0,
        )
    except Exception as exc:
        log.debug("secondary daily history fetch failed for %s: %s", symbol, exc)
        daily_df = pd.DataFrame()
    if daily_df is not None and not daily_df.empty and "Close" in daily_df.columns:
        daily_df = daily_df.dropna(subset=["Date", "Close"]).sort_values("Date")
        prev_close = None
        if not daily_df.empty:
            latest_daily_date = pd.Timestamp(daily_df.iloc[-1]["Date"]).date()
            if latest_daily_date == today and len(daily_df) >= 2:
                prev_close = _safe_float(daily_df.iloc[-2].get("Close"))
            else:
                prev_close = _safe_float(daily_df.iloc[-1].get("Close"))
        if prev_close is not None and prev_close > 0:
            change_abs = price - prev_close
            change_rate = (change_abs / prev_close) * 100.0
    return {
        "price": price,
        "change_abs": change_abs,
        "change_rate": change_rate,
        "source": "naver_intraday",
    }


def _fetch_quote_map(symbols: list[str], *, workers: int) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}

    kis_circuit = _KisCircuit()

    def _fetch_one(symbol: str) -> tuple[str, dict[str, Any] | None]:
        if not KIS_DISABLED and kis_circuit.can_use():
            try:
                payload = _fetch_kis_quote_snapshot(symbol)
                if isinstance(payload, dict) and payload.get("price") is not None:
                    kis_circuit.record_success()
                    return symbol, payload
            except Exception as exc:
                kis_circuit.record_failure(exc)
                log.debug("kis delayed quote fetch failed for %s: %s", symbol, exc)

        try:
            payload = fetch_naver_quote_snapshot(symbol)
            if isinstance(payload, dict) and payload.get("price") is not None:
                return symbol, payload
        except Exception as exc:
            log.debug("delayed quote fetch failed for %s: %s", symbol, exc)
        return symbol, _fetch_secondary_public_quote(symbol)

    fetched_map: dict[str, dict[str, Any]] = {}
    max_workers = _quote_workers(workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol, payload = future.result()
            if isinstance(payload, dict):
                fetched_map[symbol] = payload
    return fetched_map


def _rehydrate_recent_delayed_rows(
    rows: list[dict[str, Any]],
    *,
    mode: str,
    delayed_ratio: float,
) -> list[dict[str, Any]]:
    if not rows or delayed_ratio >= MIN_DELAYED_COVERAGE_RATIO:
        return rows

    existing_rows = _load_existing_quote_rows(mode)
    if not existing_rows:
        return rows

    existing_map: dict[str, dict[str, Any]] = {}
    for row in existing_rows:
        symbol = _normalize_symbol(row.get("symbol") or row.get("_id"))
        if not symbol:
            continue
        if _clean_text(row.get("price_status")) != "지연시세":
            continue
        if not _is_recent_quote_capture(row.get("price_captured_at") or row.get("captured_at")):
            continue
        existing_map[symbol] = row

    if not existing_map:
        return rows

    carried = 0
    updated_rows: list[dict[str, Any]] = []
    for row in rows:
        symbol = _normalize_symbol(row.get("symbol") or row.get("_id"))
        previous = existing_map.get(symbol)
        if previous and _clean_text(row.get("price_status")) != "지연시세":
            carried += 1
            updated = dict(row)
            updated["price"] = previous.get("price")
            updated["change_rate_pct"] = previous.get("change_rate_pct")
            updated["captured_at"] = previous.get("captured_at") or previous.get("price_captured_at") or row.get("captured_at")
            updated["source"] = previous.get("source") or row.get("source")
            updated["freshness"] = "carried_delayed_quote"
            updated["is_delayed"] = False
            updated["price_source"] = previous.get("price_source") or previous.get("source") or row.get("price_source")
            updated["price_captured_at"] = previous.get("price_captured_at") or previous.get("captured_at") or row.get("price_captured_at")
            updated["price_freshness"] = "carried_delayed_quote"
            updated["price_status"] = "업데이트 지연"
            updated_rows.append(updated)
            continue
        updated_rows.append(row)

    if carried:
        log.warning(
            "delayed quote refresh[%s] reused %s recent delayed rows because fresh delayed coverage was %.2f%%",
            mode,
            carried,
            delayed_ratio * 100.0,
        )
    return updated_rows


def build_delayed_quote_rows(
    *,
    mode: str = "full",
    skip_fetch: bool = False,
    limit: int = 0,
    workers: int = 12,
    quote_top_n: int = 300,
    tp_top_n: int = 200,
    sector_watch_top_n: int = 6,
    sector_watch_per_sector: int = 6,
) -> list[dict[str, Any]]:
    listing = _load_listing()
    if listing.empty:
        return []
    if mode == "hotset":
        target_symbols = _load_target_symbols(
            mode,
            quote_top_n=quote_top_n,
            tp_top_n=tp_top_n,
            sector_watch_top_n=sector_watch_top_n,
            sector_watch_per_sector=sector_watch_per_sector,
        )
        if target_symbols:
            listing = listing[listing["Code"].astype(str).str.zfill(6).isin(target_symbols)].copy()
        else:
            listing = listing.iloc[0:0].copy()
    if limit > 0:
        listing = listing.head(int(limit))
    if listing.empty:
        return []

    rows: list[dict[str, Any]] = []
    captured_at = datetime.now().isoformat(timespec="seconds")
    fetched_map = {} if skip_fetch else _fetch_quote_map(listing["Code"].astype(str).str.zfill(6).tolist(), workers=workers)
    try:
        official_close_date = datetime.fromtimestamp(os.path.getmtime(LISTING_PATH)).date().isoformat()
    except Exception:
        official_close_date = ""
    for _, row in listing.iterrows():
        symbol = str(row.get("Code") or "").zfill(6)
        name = str(row.get("Name") or "").strip()
        market = str(row.get("Market") or "").strip()
        official_close = _safe_float(row.get("Close"))
        change_rate = _safe_float(row.get("ChagesRatio"))
        price = None
        price_source = ""
        price_freshness = ""
        price_status = "자료 없음"
        price_captured_at = ""
        fetched = fetched_map.get(symbol)
        if isinstance(fetched, dict) and fetched.get("price") is not None:
            price = _safe_float(fetched.get("price"))
            change_rate = _safe_float(fetched.get("change_rate"))
            price_source = str(fetched.get("source") or "naver_main")
            price_freshness = "delayed_web"
            price_status = "지연시세"
            price_captured_at = captured_at
        elif official_close is not None and not _is_stale_official_close(official_close_date):
            price = official_close
            price_source = "krx_listing_close"
            price_freshness = "official_close"
            price_status = "공식종가 fallback"
            price_captured_at = official_close_date or captured_at
        elif official_close is not None:
            price = None
            change_rate = None
            price_source = "krx_listing_close"
            price_freshness = "official_close_stale"
            price_status = "업데이트 지연"
            price_captured_at = official_close_date or captured_at
        rows.append(
            {
                "_id": symbol,
                "symbol": symbol,
                "name": name,
                "market": market,
                "price": price,
                "change_rate_pct": change_rate,
                "volume": _safe_float(row.get("Volume")),
                "amount": _safe_float(row.get("Amount")),
                "marcap": _safe_float(row.get("Marcap")),
                "captured_at": price_captured_at or captured_at,
                "source": price_source,
                "freshness": price_freshness,
                "is_delayed": price_status == "지연시세",
                "price_source": price_source,
                "price_captured_at": price_captured_at or captured_at,
                "price_freshness": price_freshness,
                "official_close": official_close,
                "official_close_date": official_close_date,
                "price_status": price_status,
                "refresh_mode": mode,
            }
        )
    return rows


def _summarize_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "fetched_count": 0,
        "delayed_count": 0,
        "fallback_count": 0,
        "missing_count": 0,
    }
    for row in rows:
        status = _clean_text(row.get("price_status"))
        if row.get("price") is not None:
            summary["fetched_count"] += 1
        if status == "지연시세":
            summary["delayed_count"] += 1
        elif status == "공식종가 fallback":
            summary["fallback_count"] += 1
        else:
            summary["missing_count"] += 1
    return summary


def _source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        source = _clean_text(row.get("price_source") or row.get("source")) or "unknown"
        counts[source] = counts.get(source, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[1], reverse=True))


def save_delayed_quotes(rows: list[dict[str, Any]], *, mode: str = "full") -> dict[str, Any]:
    ensure_runtime_dir()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path, csv_path = _output_paths(mode)
    stats = _summarize_rows(rows)
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump({"generated_at": datetime.now().isoformat(timespec="seconds"), "stats": stats, "rows": rows}, fp, ensure_ascii=False, indent=2)
    pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8-sig")
    return {"json": json_path, "csv": csv_path, "count": len(rows), "mode": mode, **stats}


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def run_once(args: argparse.Namespace) -> None:
    rows = build_delayed_quote_rows(
        mode=args.mode,
        skip_fetch=args.skip_fetch,
        limit=args.limit,
        workers=args.workers,
        quote_top_n=args.quote_top_n,
        tp_top_n=args.tp_top_n,
        sector_watch_top_n=args.sector_watch_top_n,
        sector_watch_per_sector=args.sector_watch_per_sector,
    )
    initial_stats = _summarize_rows(rows)
    initial_ratio = (initial_stats["delayed_count"] / initial_stats["count"]) if initial_stats["count"] else 0.0
    rows = _rehydrate_recent_delayed_rows(rows, mode=args.mode, delayed_ratio=initial_ratio)
    result = save_delayed_quotes(rows, mode=args.mode)
    delayed_ratio = (result["delayed_count"] / result["count"]) if result["count"] else 0.0
    log.info(
        "delayed quote refresh[%s]: total=%s delayed=%s fallback=%s missing=%s",
        result["mode"],
        result["count"],
        result["delayed_count"],
        result["fallback_count"],
        result["missing_count"],
    )
    if result["count"] >= 100 and delayed_ratio < 0.05:
        log.error(
            "delayed quote refresh[%s] produced unusually low delayed coverage: %.2f%%",
            result["mode"],
            delayed_ratio * 100.0,
        )
    if args.print_only:
        print(f"[지연시세:{result['mode']}] 저장 {result['count']}종목")
        print(
            f"- delayed={result['delayed_count']} fallback={result['fallback_count']} "
            f"missing={result['missing_count']}"
        )
        print(f"- source_counts: {_source_counts(rows)}")
        print(f"- json: {result['json']}")
        print(f"- csv: {result['csv']}")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            run_once(args)
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        run_once(args)
        return
    run_scheduler(args)

if __name__ == "__main__":
    main()
