from __future__ import annotations

import argparse
import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

try:
    from runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from signals.wics_universe import normalize_sector_name
except Exception:
    from Disclosure.runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from Disclosure.signals.wics_universe import normalize_sector_name


log = logging.getLogger("disclosure.sector_rotation_backfill_builder")
ROOT_DIR = Path(__file__).resolve().parent
REPORT_DIR = ROOT_DIR / "signals" / "reports"
LATEST_UNIVERSE_PATH = REPORT_DIR / "wics_effective_universe_latest.json"
ARCHIVE_GLOB = "wics_effective_universe_*.json"
OUTPUT_PATH = Path(RUNTIME_DIR) / "web_projections" / "sector_rotation_backfill_latest.json"
CACHE_DIR = Path(RUNTIME_DIR) / "backfill" / "investor_history"
NAVER_HEADERS = {"User-Agent": "Mozilla/5.0"}
SOURCE_NAME = "backfill_naver_history"
SOURCE_CONFIDENCE = 0.72


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build approximate 52-week sector rotation backfill snapshots.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print a short summary after saving.")
    parser.add_argument("--days", type=int, default=380, help="Calendar day lookback used to approximate 52 weeks.")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent symbol history fetch workers.")
    parser.add_argument("--limit-symbols", type=int, default=0, help="Optional symbol cap for quick checks.")
    parser.add_argument("--max-pages", type=int, default=30, help="Maximum Naver investor pages per symbol.")
    parser.add_argument("--refresh", action="store_true", help="Ignore cached per-symbol investor history.")
    return parser.parse_args()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        text = str(value or "").strip().replace(",", "").replace("%", "").replace("−", "-").replace("–", "-")
        if not text:
            return default
        return int(float(text))
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip().replace(",", "").replace("%", "").replace("−", "-").replace("–", "-")
        if not text:
            return default
        number = float(text)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _parse_date(value: Any) -> date | None:
    text = _clean_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _signed_number(text: Any) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    sign = -1 if any(marker in raw for marker in ("하락", "-", "▼")) else 1
    digits = _safe_int(raw, 0)
    return -abs(digits) if sign < 0 else abs(digits)


def _to_eok_from_close_volume(close_price: int, net_volume: int) -> int:
    approx_krw = int(close_price) * int(net_volume)
    return int(round(approx_krw / 100_000_000))


def _history_cache_path(symbol: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{_norm_symbol(symbol)}.json"


def _read_history_cache(symbol: str) -> list[dict[str, Any]]:
    path = _history_cache_path(symbol)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("days") if isinstance(payload, dict) else payload
    return rows if isinstance(rows, list) else []


def _write_history_cache(symbol: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path = _history_cache_path(symbol)
    payload = {
        "symbol": _norm_symbol(symbol),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "days": rows,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_final_symbols(info: dict[str, Any]) -> list[dict[str, str]]:
    rows = info.get("final_symbols") or []
    if rows:
        return [
            {"symbol": _norm_symbol((row or {}).get("symbol")), "name": _clean_text((row or {}).get("name")) or _norm_symbol((row or {}).get("symbol"))}
            for row in rows
            if _norm_symbol((row or {}).get("symbol"))
        ]
    merged: list[dict[str, str]] = []
    seen: set[str] = set()
    for key in ("core_symbols", "manual_added", "dynamic_added"):
        for row in info.get(key) or []:
            symbol = _norm_symbol((row or {}).get("symbol"))
            if not symbol or symbol in seen:
                continue
            merged.append({"symbol": symbol, "name": _clean_text((row or {}).get("name")) or symbol})
            seen.add(symbol)
    return merged


def _load_universe_payload(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    sectors = payload.get("sectors") or {}
    if not isinstance(sectors, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for raw_sector, info in sectors.items():
        sector = normalize_sector_name((info or {}).get("normalized_sector") or raw_sector)
        if not sector:
            continue
        symbols = {
            row["symbol"]: row.get("name") or row["symbol"]
            for row in _extract_final_symbols(info or {})
            if row.get("symbol")
        }
        if symbols:
            out[sector] = symbols
    return out


def _load_archive_universes() -> list[tuple[date, dict[str, dict[str, str]]]]:
    payloads: list[tuple[date, dict[str, dict[str, str]]]] = []
    for path in sorted(REPORT_DIR.glob(ARCHIVE_GLOB)):
        if path.name.endswith("_latest.json"):
            continue
        match_day = None
        stem = path.stem
        parts = stem.split("_")
        if len(parts) >= 4:
            match_day = _parse_date(parts[-2])
        if not match_day:
            continue
        universe = _load_universe_payload(path)
        if universe:
            payloads.append((match_day, universe))
    return payloads


def _resolve_universe_for_day(day: date, base_universe: dict[str, dict[str, str]], archives: list[tuple[date, dict[str, dict[str, str]]]]) -> dict[str, dict[str, str]]:
    chosen = base_universe
    for archive_day, archive_universe in archives:
        if archive_day <= day:
            chosen = archive_universe
        else:
            break
    return chosen


def _find_naver_investor_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        summary = str(table.get("summary") or "")
        caption = table.find("caption")
        caption_text = caption.get_text(" ", strip=True) if caption else ""
        if "외국인 기관 순매매 거래량" in summary or "외국인 기관 순매매 거래량" in caption_text:
            return table
    return None


def _fetch_symbol_history(symbol: str, *, start_dt: date, end_dt: date, max_pages: int, refresh: bool) -> dict[str, Any]:
    cached = _read_history_cache(symbol)
    if cached and not refresh:
        cache_dates = [_parse_date(row.get("date")) for row in cached]
        cache_dates = [item for item in cache_dates if item]
        if cache_dates and min(cache_dates) <= start_dt and max(cache_dates) >= end_dt:
            rows = [
                row for row in cached
                if start_dt <= (_parse_date(row.get("date")) or end_dt) <= end_dt
            ]
            return {"symbol": _norm_symbol(symbol), "source": SOURCE_NAME, "days": rows}

    rows: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    last_oldest: str | None = None
    for page in range(1, max(1, int(max_pages)) + 1):
        url = f"https://finance.naver.com/item/frgn.naver?code={_norm_symbol(symbol)}&page={page}"
        response = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        table = _find_naver_investor_table(soup)
        if table is None:
            break
        page_rows: list[dict[str, Any]] = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(cells) != 9:
                continue
            day = _parse_date(cells[0])
            if not day:
                continue
            row = {
                "date": day.isoformat(),
                "close_price": _safe_int(cells[1]),
                "change_abs": _signed_number(cells[2]),
                "rate": round(_safe_float(cells[3], 0.0), 2),
                "foreign_eok": _to_eok_from_close_volume(_safe_int(cells[1]), _signed_number(cells[6])),
                "inst_eok": _to_eok_from_close_volume(_safe_int(cells[1]), _signed_number(cells[5])),
                "retail_eok": _to_eok_from_close_volume(_safe_int(cells[1]), -(_signed_number(cells[6]) + _signed_number(cells[5]))),
                "source": SOURCE_NAME,
            }
            if row["date"] not in seen_dates:
                page_rows.append(row)
                seen_dates.add(row["date"])
        if not page_rows:
            break
        rows.extend(page_rows)
        oldest = page_rows[-1]["date"]
        if oldest == last_oldest:
            break
        last_oldest = oldest
        if (_parse_date(oldest) or start_dt) < start_dt:
            break
        time.sleep(0.02)

    rows = sorted(rows, key=lambda row: row["date"], reverse=True)
    if rows:
        _write_history_cache(symbol, rows)
    filtered = [row for row in rows if start_dt <= (_parse_date(row.get("date")) or end_dt) <= end_dt]
    return {"symbol": _norm_symbol(symbol), "source": SOURCE_NAME, "days": filtered}


def _build_symbol_points(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    ordered = sorted(
        [
            {
                "date": _clean_text(row.get("date")),
                "rate": round(_safe_float(row.get("rate"), 0.0), 2),
                "foreign_eok": _safe_int(row.get("foreign_eok")),
                "inst_eok": _safe_int(row.get("inst_eok")),
                "retail_eok": _safe_int(row.get("retail_eok")),
                "source": _clean_text(row.get("source")) or SOURCE_NAME,
            }
            for row in rows
            if _parse_date(row.get("date"))
        ],
        key=lambda row: row["date"],
    )
    points: dict[str, dict[str, Any]] = {}
    window: list[dict[str, Any]] = []
    for row in ordered:
        window.append(row)
        if len(window) > 3:
            window.pop(0)
        f_3d = sum(int(item["foreign_eok"]) for item in window)
        i_3d = sum(int(item["inst_eok"]) for item in window)
        r_3d = sum(int(item["retail_eok"]) for item in window)
        f_streak = 0
        i_streak = 0
        for item in reversed(window):
            if int(item["foreign_eok"]) <= 0:
                break
            f_streak += 1
        for item in reversed(window):
            if int(item["inst_eok"]) <= 0:
                break
            i_streak += 1
        smart_money = f_3d + i_3d
        quiet_accumulation = smart_money > 0 and r_3d < 0 and -4 <= float(row["rate"]) <= 8
        raw_accumulation_score = (f_streak * 12) + (i_streak * 12)
        raw_accumulation_score += min(25, max(0, smart_money) // 50)
        if quiet_accumulation:
            raw_accumulation_score += 6
        if float(row["rate"]) >= 15:
            raw_accumulation_score -= 6
        confidence = SOURCE_CONFIDENCE
        accumulation_score = int(round(float(raw_accumulation_score) * confidence))
        points[row["date"]] = {
            "rate": round(float(row["rate"]), 2),
            "foreign": f_3d,
            "inst": i_3d,
            "retail": r_3d,
            "foreign_3d": f_3d,
            "inst_3d": i_3d,
            "retail_3d": r_3d,
            "f_3d": f_3d,
            "i_3d": i_3d,
            "r_3d": r_3d,
            "f_streak": f_streak,
            "i_streak": i_streak,
            "smart_money": smart_money,
            "score": accumulation_score,
            "accumulation_score": accumulation_score,
            "raw_accumulation_score": int(raw_accumulation_score),
            "quiet_accumulation": quiet_accumulation,
            "flow_confidence": confidence,
            "flow_confidence_label": "보통",
            "flow_coverage_ratio": round(len(window) / 3.0, 3),
            "fallback_used": True,
            "data_source": SOURCE_NAME,
        }
    return points


def _build_stock_leaderboard(stock_data_dict: dict[str, dict[str, Any]], limit: int = 3):
    ranked = sorted(
        stock_data_dict.items(),
        key=lambda item: (
            item[1].get("accumulation_score", 0),
            item[1].get("smart_money", 0),
            item[1].get("f_streak", 0) + item[1].get("i_streak", 0),
            -abs(item[1].get("rate", 0)),
        ),
        reverse=True,
    )
    leaderboard = []
    for stock_name, data in ranked[:limit]:
        leaderboard.append(
            {
                "stock_name": stock_name,
                "symbol": _norm_symbol(data.get("symbol")),
                "accumulation_score": data.get("accumulation_score", 0),
                "smart_money": data.get("smart_money", 0),
                "foreign": data.get("foreign", data.get("f_3d", 0)),
                "inst": data.get("inst", data.get("i_3d", 0)),
                "retail": data.get("retail", data.get("r_3d", 0)),
                "rate": data.get("rate", 0),
                "f_streak": data.get("f_streak", 0),
                "i_streak": data.get("i_streak", 0),
                "quiet_accumulation": data.get("quiet_accumulation", False),
            }
        )
    return leaderboard


def _dominant_actor_label(foreign: int, inst: int, retail: int) -> str:
    candidates = {"외인": abs(foreign), "기관": abs(inst), "개인": abs(retail)}
    winner = max(candidates, key=candidates.get)
    return "중립" if candidates[winner] == 0 else winner


def _build_sector_features(sec_name: str, sector_score: int, sec_f_3d: int, sec_i_3d: int, sec_r_3d: int, stock_data_dict: dict[str, dict[str, Any]]) -> dict[str, Any]:
    smart_money_net = sec_f_3d + sec_i_3d
    positive_smart_money_count = sum(1 for data in stock_data_dict.values() if data.get("smart_money", 0) > 0)
    quiet_accumulation_count = sum(1 for data in stock_data_dict.values() if data.get("quiet_accumulation"))
    double_streak_count = sum(1 for data in stock_data_dict.values() if data.get("f_streak", 0) >= 2 and data.get("i_streak", 0) >= 2)
    fallback_count = sum(1 for data in stock_data_dict.values() if data.get("fallback_used"))
    avg_flow_confidence = round(
        sum(float(data.get("flow_confidence", 0.0) or 0.0) for data in stock_data_dict.values()) / max(1, len(stock_data_dict)),
        3,
    )
    leaderboard = _build_stock_leaderboard(stock_data_dict)
    leader_score = leaderboard[0]["accumulation_score"] if leaderboard else 0
    leader_concentration = round(leader_score / sector_score, 4) if sector_score > 0 else 0.0
    return {
        "score": sector_score,
        "smart_money_net": smart_money_net,
        "positive_smart_money_count": positive_smart_money_count,
        "positive_smart_money_ratio": round(positive_smart_money_count / max(1, len(stock_data_dict)), 4),
        "quiet_accumulation_count": quiet_accumulation_count,
        "double_streak_count": double_streak_count,
        "fallback_count": fallback_count,
        "avg_flow_confidence": avg_flow_confidence,
        "dominant_actor": _dominant_actor_label(sec_f_3d, sec_i_3d, sec_r_3d),
        "leader_concentration": leader_concentration,
        "leaderboard": leaderboard,
        "sector_name": sec_name,
    }


def build_backfill(args: argparse.Namespace) -> dict[str, Any]:
    ensure_runtime_dir()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    base_universe = _load_universe_payload(LATEST_UNIVERSE_PATH)
    archives = _load_archive_universes()
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=max(365, int(args.days)))
    all_symbols: dict[str, str] = {}
    for universe in [base_universe, *[item[1] for item in archives]]:
        for sector_symbols in universe.values():
            for symbol, name in sector_symbols.items():
                if symbol and symbol not in all_symbols:
                    all_symbols[symbol] = name
    symbols = sorted(all_symbols)
    if args.limit_symbols and args.limit_symbols > 0:
        symbols = symbols[: int(args.limit_symbols)]

    symbol_histories: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _fetch_symbol_history,
                symbol,
                start_dt=start_dt,
                end_dt=end_dt,
                max_pages=max(1, int(args.max_pages)),
                refresh=bool(args.refresh),
            ): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                payload = future.result()
            except Exception as exc:
                log.warning("backfill history fetch failed (%s): %s", symbol, exc)
                continue
            days = payload.get("days") or []
            if days:
                symbol_histories[symbol] = days

    symbol_points = {symbol: _build_symbol_points(rows) for symbol, rows in symbol_histories.items()}
    available_days = sorted({day_key for points in symbol_points.values() for day_key in points})
    daily_sector_rows: dict[str, dict[str, dict[str, Any]]] = {}

    archive_cursor = 0
    sorted_archives = sorted(archives, key=lambda item: item[0])
    for day_key in available_days:
        current_day = _parse_date(day_key)
        if not current_day:
            continue
        while archive_cursor + 1 < len(sorted_archives) and sorted_archives[archive_cursor + 1][0] <= current_day:
            archive_cursor += 1
        if sorted_archives and sorted_archives[archive_cursor][0] <= current_day:
            day_universe = sorted_archives[archive_cursor][1]
        else:
            day_universe = base_universe
        sector_map: dict[str, dict[str, Any]] = {}
        for sector_name, sector_symbols in day_universe.items():
            sec_f_3d = sec_i_3d = sec_r_3d = 0
            sector_score = 0
            stock_data_dict: dict[str, dict[str, Any]] = {}
            for symbol, stock_name in sector_symbols.items():
                point = (symbol_points.get(symbol) or {}).get(day_key)
                if not point:
                    continue
                stock_point = dict(point)
                stock_point["symbol"] = symbol
                sec_f_3d += int(stock_point.get("f_3d", 0))
                sec_i_3d += int(stock_point.get("i_3d", 0))
                sec_r_3d += int(stock_point.get("r_3d", 0))
                sector_score += int(stock_point.get("accumulation_score", 0))
                stock_data_dict[stock_name] = stock_point
            if not stock_data_dict:
                continue
            sector_features = _build_sector_features(
                sector_name,
                sector_score,
                sec_f_3d,
                sec_i_3d,
                sec_r_3d,
                stock_data_dict,
            )
            leaderboard = sector_features.get("leaderboard") or []
            leader = leaderboard[0] if leaderboard else {}
            return_pct = round(
                sum(float(item.get("rate", 0.0) or 0.0) for item in stock_data_dict.values()) / max(1, len(stock_data_dict)),
                2,
            )
            sector_map[sector_name] = {
                "sector": sector_name,
                "return_pct": return_pct,
                "foreign_flow_eok": sec_f_3d,
                "inst_flow_eok": sec_i_3d,
                "retail_flow_eok": sec_r_3d,
                "combined_flow_eok": sec_f_3d + sec_i_3d,
                "net_flow_eok": sec_f_3d + sec_i_3d,
                "final_score": sector_score,
                "leader_name": _clean_text(leader.get("stock_name")) or "-",
                "leader_symbol": _norm_symbol(leader.get("symbol")),
                "source": "backfill",
            }
        if sector_map:
            daily_sector_rows[day_key] = sector_map

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_dt.isoformat(),
        "end_date": end_dt.isoformat(),
        "days_requested": int(max(365, int(args.days))),
        "source": SOURCE_NAME,
        "approximate": True,
        "symbols_requested": len(symbols),
        "symbols_with_history": len(symbol_histories),
        "available_days": len(daily_sector_rows),
        "days": daily_sector_rows,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    payload = build_backfill(args)
    if args.print_only:
        print(f"[sector_rotation_backfill] 저장 {payload.get('available_days', 0)}일")
        print(f"- symbols with history: {payload.get('symbols_with_history', 0)} / {payload.get('symbols_requested', 0)}")
        print(f"- json: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
