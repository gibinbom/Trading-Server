from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "None"):
            return default
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", "None"):
            return default
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def _safe_ratio(value: Any, default: float = 0.0) -> float:
    number = _safe_float(value, default)
    if number is None:
        return default
    if number > 1.0:
        number = number / 100.0
    return max(0.0, min(1.0, float(number)))


def _parse_iso_date(value: Any) -> datetime.date | None:
    text = _clean_text(value)
    if not text or text in {"-", "None"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def _table_text(table: Any) -> str:
    try:
        return " ".join(table.get_text(" ", strip=True).split())
    except Exception:
        return ""


def _parse_share_pair(text: str) -> tuple[int, float]:
    left, _, right = _clean_text(text).partition("/")
    return _safe_int(left), float(_safe_float(right, 0.0) or 0.0)


def load_json_cache(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_cache(path: Path, payload: dict[str, Any], *, logger: Any | None = None) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:  # pragma: no cover - cache best effort
        if logger is not None:
            logger.debug("passive cache write failed %s: %s", path.name, str(exc)[:180])


def cache_is_fresh(fetched_at: str, max_age_hours: int) -> bool:
    if not fetched_at:
        return False
    try:
        fetched_dt = datetime.fromisoformat(fetched_at)
    except Exception:
        return False
    return (datetime.now() - fetched_dt) <= timedelta(hours=max_age_hours)


def projection_file_candidates(file_name: str, bases: tuple[Path, ...]) -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()
    for base in bases:
        path = base / file_name
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(path)
    return candidates


def load_projection_rows(file_name: str, bases: tuple[Path, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in projection_file_candidates(file_name, bases):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, list):
            rows.extend(row for row in payload if isinstance(row, dict))
        elif isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            rows.extend(row for row in payload.get("rows") if isinstance(row, dict))
    return rows


def build_special_change_event_map(
    listing_df: pd.DataFrame,
    *,
    rows: list[dict[str, Any]],
    lookback_days: int,
    normalize_symbol: Callable[[Any], str],
    normalize_market: Callable[[Any], str],
    clean_text: Callable[[Any], str],
    parse_iso_date: Callable[[Any], datetime.date | None],
    special_change_event_types: dict[str, str],
) -> dict[str, dict[str, Any]]:
    listing_market_map = {
        normalize_symbol(row.get("Code")): normalize_market(row.get("Market"))
        for row in listing_df.to_dict(orient="records")
        if normalize_symbol(row.get("Code"))
    }
    listing_name_map = {
        normalize_symbol(row.get("Code")): clean_text(row.get("Name"))
        for row in listing_df.to_dict(orient="records")
        if normalize_symbol(row.get("Code"))
    }
    cutoff_date = datetime.now().date() - timedelta(days=lookback_days)
    special_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = normalize_symbol(row.get("symbol") or row.get("code"))
        event_type = clean_text(row.get("event_type")).upper()
        event_date = parse_iso_date(row.get("event_date"))
        if not symbol or event_type not in special_change_event_types or event_date is None or event_date < cutoff_date:
            continue
        payload = {
            "symbol": symbol,
            "name": clean_text(row.get("name")) or listing_name_map.get(symbol, ""),
            "market": normalize_market(row.get("market")) or listing_market_map.get(symbol, ""),
            "event_type": event_type,
            "signal": special_change_event_types[event_type],
            "event_date": event_date.isoformat(),
            "title": clean_text(row.get("title")),
            "dart_url": clean_text(row.get("dart_url")),
        }
        previous = special_map.get(symbol)
        previous_date = parse_iso_date(previous.get("event_date")) if previous else None
        if previous is None or previous_date is None or event_date > previous_date:
            special_map[symbol] = payload
    return special_map


def parse_public_float_profile_html(html: str, beautiful_soup: Any) -> dict[str, Any]:
    profile = {
        "listed_common_shares": 0,
        "float_shares": 0,
        "free_float_ratio": None,
        "major_holder_shares": 0,
        "major_holder_ratio": 0.0,
        "treasury_shares": 0,
        "treasury_ratio": 0.0,
        "employee_shares": 0,
        "employee_ratio": 0.0,
        "source": "fnguide_public",
    }
    if not html or beautiful_soup is None:
        return profile
    soup = beautiful_soup(html, "html.parser")
    for table in soup.find_all("table"):
        text = _table_text(table)
        if "유동주식수/비율" in text:
            for tr in table.find_all("tr"):
                headers = [_clean_text(th.get_text(" ", strip=True)) for th in tr.find_all("th")]
                cells = [_clean_text(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
                if not headers or not cells:
                    continue
                label = headers[0]
                value = cells[0]
                if "발행주식수" in label:
                    profile["listed_common_shares"] = _safe_int(value.split("/", 1)[0])
                elif "유동주식수/비율" in label:
                    float_shares, float_ratio = _parse_share_pair(value)
                    profile["float_shares"] = float_shares
                    profile["free_float_ratio"] = round(float_ratio / 100.0, 6) if float_ratio > 1 else round(float_ratio, 6)
        if "주주현황" in text and "최대주주등" in text:
            for tr in table.find_all("tr"):
                cells = [_clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["th", "td"])]
                if len(cells) < 4:
                    continue
                label = cells[0]
                shares = _safe_int(cells[2])
                ratio = float(_safe_float(cells[3], 0.0) or 0.0)
                if label.startswith("최대주주등"):
                    profile["major_holder_shares"] = shares
                    profile["major_holder_ratio"] = ratio
                elif label.startswith("자기주식"):
                    profile["treasury_shares"] = shares
                    profile["treasury_ratio"] = ratio
                elif label.startswith("우리사주조합"):
                    profile["employee_shares"] = shares
                    profile["employee_ratio"] = ratio
    ratio = _safe_ratio(profile.get("free_float_ratio"))
    if ratio <= 0:
        derived = 1.0 - (
            float(_safe_float(profile.get("major_holder_ratio"), 0.0) or 0.0)
            + float(_safe_float(profile.get("treasury_ratio"), 0.0) or 0.0)
            + float(_safe_float(profile.get("employee_ratio"), 0.0) or 0.0)
        ) / 100.0
        ratio = max(0.05, min(0.95, derived))
    profile["free_float_ratio"] = round(ratio, 6)
    return profile


def fetch_public_float_profile(
    symbol: str,
    *,
    requests_module: Any,
    beautiful_soup: Any,
    public_float_profile_url: str,
    now_iso: Callable[[], str],
    normalize_symbol: Callable[[Any], str],
) -> dict[str, Any]:
    if requests_module is None:
        return {"symbol": symbol, "source": "requests_missing", "free_float_ratio": None}
    url = public_float_profile_url.format(symbol=normalize_symbol(symbol))
    response = requests_module.get(url, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    parsed = parse_public_float_profile_html(response.text, beautiful_soup)
    parsed["symbol"] = normalize_symbol(symbol)
    parsed["fetched_at"] = now_iso()
    return parsed


def load_public_float_profiles(
    symbols: list[str],
    *,
    worker_count: int,
    cache_path: Path,
    max_age_hours: int,
    fetch_profile: Callable[[str], dict[str, Any]],
    normalize_symbol: Callable[[Any], str],
    clean_text: Callable[[Any], str],
    logger: Any | None = None,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    cache = load_json_cache(cache_path)
    results: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for raw_symbol in symbols:
        symbol = normalize_symbol(raw_symbol)
        cached = cache.get(symbol) if isinstance(cache.get(symbol), dict) else None
        if cached and cache_is_fresh(clean_text(cached.get("fetched_at")), max_age_hours):
            results[symbol] = cached
        else:
            missing.append(symbol)
    if missing:
        max_workers = max(1, min(worker_count, len(missing), 8))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(fetch_profile, symbol): symbol for symbol in missing}
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    payload = future.result()
                except Exception as exc:  # pragma: no cover - network dependent
                    payload = {
                        "symbol": symbol,
                        "source": "fnguide_error",
                        "free_float_ratio": None,
                        "source_error": str(exc)[:180],
                        "fetched_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    if logger is not None:
                        logger.debug("float profile fetch failed %s: %s", symbol, str(exc)[:180])
                results[symbol] = payload
                cache[symbol] = payload
        write_json_cache(cache_path, cache, logger=logger)
    return results
