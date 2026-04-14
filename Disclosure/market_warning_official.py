from __future__ import annotations

import re
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any

from bs4 import BeautifulSoup


def classify_caution_notice_type(raw_type: str, *, caution_reason_map: dict[str, tuple[str, str, str]], clean_text: Any) -> tuple[str, str, str]:
    text = clean_text(raw_type)
    if text in caution_reason_map:
        return caution_reason_map[text]
    return "attention", "design", text or "투자주의"


def parse_market_from_row(tr: Any, *, market_alt_map: dict[str, str], clean_text: Any) -> str:
    image = tr.select_one("img.legend")
    return market_alt_map.get(clean_text(image.get("alt")) if image else "", "")


def parse_kind_warning_html(
    html: str,
    *,
    menu_kind: str,
    name_lookup: dict[str, list[dict[str, Any]]],
    as_of: str,
    kind_warn_main_url: str,
    caution_reason_map: dict[str, tuple[str, str, str]],
    market_alt_map: dict[str, str],
    resolve_symbol_by_name: Any,
    clean_text: Any,
    date_text: Any,
    next_business_day: Any,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []
    for tr in soup.select("tbody tr"):
        cols = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if not cols or "데이터가 없습니다" in cols[0]:
            continue
        market = parse_market_from_row(tr, market_alt_map=market_alt_map, clean_text=clean_text)
        name = clean_text(cols[1] if len(cols) >= 2 else "")
        symbol, resolved_market = resolve_symbol_by_name(name, market, name_lookup=name_lookup)
        market_final = resolved_market or market
        if menu_kind == "attention":
            if len(cols) < 5:
                continue
            kind, action, reason_group = classify_caution_notice_type(
                cols[2],
                caution_reason_map=caution_reason_map,
                clean_text=clean_text,
            )
            act_dd = date_text(cols[3])
            design_dd = date_text(cols[4])
            rows.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "market": market_final,
                    "kind": kind,
                    "action": action,
                    "reason_group": reason_group,
                    "notice_title": reason_group,
                    "announced_at": act_dd,
                    "effective_date": design_dd or act_dd,
                    "active": action in {"pre_notice"} and (design_dd or act_dd) == as_of,
                    "source_url": kind_warn_main_url,
                }
            )
            if kind == "risk" and action == "pre_notice":
                rows.append(
                    {
                        "symbol": symbol,
                        "name": name,
                        "market": market_final,
                        "kind": "trading_halt",
                        "action": "pre_notice",
                        "reason_group": "투자위험 지정예고 연계",
                        "notice_title": "매매거래정지 예고 (투자위험 지정예고 연계)",
                        "announced_at": act_dd,
                        "effective_date": design_dd or act_dd,
                        "active": (design_dd or act_dd) == as_of,
                        "source_url": kind_warn_main_url,
                    }
                )
            continue

        if len(cols) < 5:
            continue
        act_dd = date_text(cols[2])
        design_dd = date_text(cols[3])
        free_dt = date_text(cols[4])
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "market": market_final,
                "kind": menu_kind,
                "action": "design",
                "reason_group": "시장경보 지정",
                "notice_title": "투자경고종목 지정" if menu_kind == "warning" else "투자위험종목 지정",
                "announced_at": act_dd,
                "effective_date": design_dd,
                "active": not free_dt or free_dt == "-",
                "source_url": kind_warn_main_url,
            }
        )
        if menu_kind == "risk":
            next_day = next_business_day(design_dd)
            rows.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "market": market_final,
                    "kind": "trading_halt",
                    "action": "halt",
                    "reason_group": "투자위험 지정연계",
                    "notice_title": "매매거래정지 (투자위험 지정연계)",
                    "announced_at": design_dd,
                    "effective_date": design_dd,
                    "active": design_dd == as_of,
                    "source_url": kind_warn_main_url,
                }
            )
            if next_day:
                rows.append(
                    {
                        "symbol": symbol,
                        "name": name,
                        "market": market_final,
                        "kind": "trading_halt",
                        "action": "resume",
                        "reason_group": "투자위험 지정연계",
                        "notice_title": "매매거래정지 해제 (투자위험 지정연계)",
                        "announced_at": next_day,
                        "effective_date": next_day,
                        "active": False,
                        "source_url": kind_warn_main_url,
                    }
                )
        if free_dt and free_dt != "-":
            rows.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "market": market_final,
                    "kind": menu_kind,
                    "action": "release",
                    "reason_group": "시장경보 해제",
                    "notice_title": "투자경고종목 지정해제" if menu_kind == "warning" else "투자위험종목 지정해제",
                    "announced_at": free_dt,
                    "effective_date": free_dt,
                    "active": False,
                    "source_url": kind_warn_main_url,
                }
            )
    return rows


def parse_kind_trading_halt_html(html: str, *, market: str, clean_text: Any) -> tuple[str, list[dict[str, Any]]]:
    soup = BeautifulSoup(html, "html.parser")
    match = re.search(r"(\\d{4})년\\s*(\\d{1,2})월\\s*(\\d{1,2})일", soup.get_text(" ", strip=True))
    as_of = ""
    if match:
        as_of = date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
    rows: list[dict[str, Any]] = []
    for tr in soup.select("tbody tr"):
        cols = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(cols) < 3:
            continue
        rows.append(
            {
                "name": clean_text(cols[1]),
                "market": market,
                "reason_group": clean_text(cols[2]) or "일반 매매거래정지",
            }
        )
    return as_of, rows


def extract_reason_group(text: str, *, clean_text: Any) -> str:
    cleaned = clean_text(text)
    paren = re.search(r"\\(([^)]+)\\)", cleaned)
    if paren:
        return clean_text(paren.group(1))
    return cleaned[:80]


def build_event_halt_rows(
    events: list[dict[str, Any]],
    *,
    cutoff_date: str,
    general_halt_matchers: tuple[str, ...],
    norm_symbol: Any,
    normalize_market: Any,
    clean_text: Any,
    parse_iso_date: Any,
    date_text: Any,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cutoff = parse_iso_date(cutoff_date)
    for event in events:
        event_date = date_text(event.get("event_date"))
        parsed_event_date = parse_iso_date(event_date)
        if cutoff and parsed_event_date and parsed_event_date < cutoff:
            continue
        text = " ".join(clean_text(event.get(key)) for key in ("title", "impact_note", "event_detail_summary", "event_source_excerpt"))
        if not any(token in text for token in general_halt_matchers):
            continue
        action = "halt"
        if "해제" in text or "재개" in text:
            action = "resume"
        elif "예고" in text or "예정" in text:
            action = "pre_notice"
        rows.append(
            {
                "symbol": norm_symbol(event.get("symbol")),
                "name": clean_text(event.get("name")),
                "market": normalize_market(event.get("market")),
                "kind": "trading_halt",
                "action": action,
                "reason_group": extract_reason_group(text, clean_text=clean_text),
                "notice_title": clean_text(event.get("title")) or extract_reason_group(text, clean_text=clean_text),
                "announced_at": event_date,
                "effective_date": event_date,
                "active": action == "halt",
                "source_url": clean_text(event.get("dart_url")),
            }
        )
    return rows


def dedupe_official_rows(rows: list[dict[str, Any]], *, norm_symbol: Any, clean_text: Any, date_text: Any, action_priority: dict[str, int]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            norm_symbol(row.get("symbol")),
            clean_text(row.get("name")),
            clean_text(row.get("kind")),
            clean_text(row.get("action")),
            date_text(row.get("effective_date")),
            clean_text(row.get("reason_group")),
        )
        current = deduped.get(key)
        if current is None or (not clean_text(current.get("source_url")) and clean_text(row.get("source_url"))):
            deduped[key] = row
    result = list(deduped.values())
    result.sort(
        key=lambda row: (
            date_text(row.get("effective_date")),
            date_text(row.get("announced_at")),
            action_priority.get(clean_text(row.get("action")), 99),
            clean_text(row.get("name")),
        ),
        reverse=True,
    )
    return result


def build_official_state_map(
    rows: list[dict[str, Any]],
    *,
    as_of: str | None,
    norm_symbol: Any,
    clean_text: Any,
    normalize_market: Any,
    parse_iso_date: Any,
    date_text: Any,
    action_priority: dict[str, int],
    current_state_keys: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    state_map: dict[str, dict[str, Any]] = {}
    as_of_date = parse_iso_date(as_of) if as_of else None
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            date_text(row.get("effective_date")),
            date_text(row.get("announced_at")),
            action_priority.get(clean_text(row.get("action")), 99),
            clean_text(row.get("name")),
        ),
    )
    for row in sorted_rows:
        symbol = norm_symbol(row.get("symbol"))
        if not symbol:
            continue
        effective_date = date_text(row.get("effective_date")) or date_text(row.get("announced_at"))
        effective_date_parsed = parse_iso_date(effective_date)
        if as_of_date and effective_date_parsed and effective_date_parsed > as_of_date:
            continue
        state = state_map.setdefault(
            symbol,
            {
                "symbol": symbol,
                "name": clean_text(row.get("name")),
                "market": normalize_market(row.get("market")),
                "base_state": "none",
                "current_state": "none",
                "warning_pre_notice_date": "",
                "warning_design_date": "",
                "warning_release_date": "",
                "risk_pre_notice_date": "",
                "risk_design_date": "",
                "risk_release_date": "",
                "halt_pre_notice_date": "",
                "halt_date": "",
                "halt_resume_date": "",
                "halt_active": False,
            },
        )
        kind = clean_text(row.get("kind"))
        action = clean_text(row.get("action"))
        if kind == "warning":
            if action == "pre_notice":
                state["base_state"] = "warning_pre_notice"
                state["warning_pre_notice_date"] = effective_date
            elif action == "design":
                state["base_state"] = "warning_active"
                state["warning_design_date"] = effective_date
            elif action == "release":
                state["base_state"] = "none"
                state["warning_release_date"] = effective_date
        elif kind == "risk":
            if action == "pre_notice":
                state["base_state"] = "risk_pre_notice"
                state["risk_pre_notice_date"] = effective_date
            elif action == "design":
                state["base_state"] = "risk_active"
                state["risk_design_date"] = effective_date
            elif action == "release":
                state["base_state"] = "warning_active"
                state["risk_release_date"] = effective_date
        elif kind == "trading_halt":
            if action == "pre_notice":
                state["halt_pre_notice_date"] = effective_date
                if not state["halt_active"]:
                    state["current_state"] = "halt_pre_notice"
            elif action == "halt":
                state["halt_date"] = effective_date
                state["halt_active"] = bool(row.get("active"))
            elif action in {"resume", "release"}:
                state["halt_resume_date"] = effective_date
                state["halt_active"] = False

    for state in state_map.values():
        current = state["base_state"]
        if state["halt_active"]:
            current = "halt_active"
        elif state["halt_pre_notice_date"]:
            halt_pre_notice = parse_iso_date(state["halt_pre_notice_date"])
            halt_date = parse_iso_date(state["halt_date"])
            halt_resume = parse_iso_date(state["halt_resume_date"])
            if halt_pre_notice and (not halt_date or halt_pre_notice >= halt_date) and (not halt_resume or halt_pre_notice > halt_resume):
                current = "halt_pre_notice"
        state["current_state"] = current if current in current_state_keys else "none"
    return state_map


def snapshot_state_counts(state_map: dict[str, dict[str, Any]], *, clean_text: Any, current_state_keys: tuple[str, ...]) -> dict[str, int]:
    counts = {key: 0 for key in current_state_keys if key != "none"}
    for state in state_map.values():
        current = clean_text(state.get("current_state"))
        if current in counts:
            counts[current] += 1
    return counts


def build_market_warning_official_snapshot(
    *,
    stock_master: dict[str, dict[str, Any]],
    as_of: str,
    lookback_days: int,
    kind_session: Any,
    kind_warning_menus: tuple[dict[str, str], ...],
    trading_halt_markets: tuple[tuple[str, str], ...],
    kind_warn_main_url: str,
    kind_trading_halt_main_url: str,
    post_kind_warn_html: Any,
    post_kind_trading_halt_html: Any,
    build_name_lookup: Any,
    parse_kind_warning_html: Any,
    parse_kind_trading_halt_html: Any,
    resolve_symbol_by_name: Any,
    load_event_calendar_rows: Any,
    build_event_halt_rows: Any,
    dedupe_official_rows: Any,
    build_official_state_map: Any,
    snapshot_state_counts: Any,
    now_iso: Any,
    parse_iso_date: Any,
    date_text: Any,
    default_summary_days: int,
    clean_text: Any,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    name_lookup = build_name_lookup(stock_master)
    end_date = parse_iso_date(as_of) or datetime.now().date()
    start_date = end_date - timedelta(days=max(lookback_days * 2, 180))
    start_text = start_date.isoformat()
    end_text = end_date.isoformat()
    session = kind_session()
    rows: list[dict[str, Any]] = []

    for menu in kind_warning_menus:
        html = post_kind_warn_html(
            session,
            menu_index=menu["menu_index"],
            forward=menu["forward"],
            order_mode=menu["order_mode"],
            start_date=start_text,
            end_date=end_text,
        )
        rows.extend(parse_kind_warning_html(html, menu_kind=menu["kind"], name_lookup=name_lookup, as_of=as_of))

    for market_type, market_name in trading_halt_markets:
        html = post_kind_trading_halt_html(session, market_type=market_type)
        current_as_of, parsed_rows = parse_kind_trading_halt_html(html, market=market_name)
        current_date = current_as_of or as_of
        for row in parsed_rows:
            symbol, resolved_market = resolve_symbol_by_name(row["name"], row["market"], name_lookup=name_lookup)
            rows.append(
                {
                    "symbol": symbol,
                    "name": row["name"],
                    "market": resolved_market or row["market"],
                    "kind": "trading_halt",
                    "action": "halt",
                    "reason_group": row["reason_group"],
                    "notice_title": row["reason_group"],
                    "announced_at": current_date,
                    "effective_date": current_date,
                    "active": True,
                    "source_url": kind_trading_halt_main_url,
                }
            )

    rows.extend(build_event_halt_rows(load_event_calendar_rows(), cutoff_date=(end_date - timedelta(days=lookback_days)).isoformat()))
    deduped_rows = dedupe_official_rows(rows)
    state_map = build_official_state_map(deduped_rows, as_of=as_of)
    state_counts = snapshot_state_counts(state_map)
    recent_cutoff = end_date - timedelta(days=default_summary_days)
    today_count = sum(1 for row in deduped_rows if date_text(row.get("effective_date")) == as_of)
    recent_count = sum(
        1
        for row in deduped_rows
        if (parse_iso_date(row.get("effective_date")) or parse_iso_date(row.get("announced_at")) or date.min) >= recent_cutoff
    )
    snapshot = {
        "generated_at": now_iso(),
        "as_of": as_of,
        "status": "live",
        "stale_since": None,
        "source_error": "",
        "lookback_days": lookback_days,
        "default_recent_days": default_summary_days,
        "rows": deduped_rows,
        "summary": {
            "row_count": len(deduped_rows),
            "today_count": today_count,
            "recent_count": recent_count,
            "active_warning_count": state_counts.get("warning_active", 0),
            "active_risk_count": state_counts.get("risk_active", 0),
            "active_halt_count": state_counts.get("halt_active", 0),
            "state_counts": state_counts,
            "kind_counts": dict(Counter(clean_text(row.get("kind")) for row in deduped_rows)),
            "action_counts": dict(Counter(clean_text(row.get("action")) for row in deduped_rows)),
        },
    }
    return snapshot, state_map
