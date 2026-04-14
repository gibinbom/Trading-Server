from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

import pandas as pd


def _series_values(series_map: dict[str, float], trading_dates: list[str], *, safe_float: Any) -> tuple[list[str], list[float]]:
    dates: list[str] = []
    values: list[float] = []
    for date_text in trading_dates:
        value = safe_float(series_map.get(date_text))
        if value is None or value <= 0:
            continue
        dates.append(date_text)
        values.append(float(value))
    return dates, values


def _pct_change(current: float | None, reference: float | None) -> float | None:
    if current is None or reference is None or reference <= 0:
        return None
    return round((current / reference - 1.0) * 100.0, 2)


def _nth_from_end(values: list[float], offset: int) -> float | None:
    return values[-1 - offset] if len(values) > offset else None


def _is_recent_high(values: list[float], window: int = 15) -> bool:
    if len(values) < 2:
        return False
    tail = values[-window:]
    if not tail:
        return False
    return tail[-1] >= max(tail)


def _threshold_gap(actual: float | None, threshold: float | None) -> float | None:
    if actual is None or threshold is None:
        return None
    return round(actual - threshold, 2)


def _within_session_window(trading_dates: list[str], event_date: str, max_sessions: int) -> bool:
    if not event_date or event_date not in trading_dates:
        return False
    idx = trading_dates.index(event_date)
    return len(trading_dates) - 1 - idx <= max_sessions


def _sessions_since(trading_dates: list[str], event_date: str) -> int | None:
    if not event_date or event_date not in trading_dates:
        return None
    return len(trading_dates) - 1 - trading_dates.index(event_date)


def _signal_confidence(candidate_state: str, *, gap_pct: float | None, high_quality: bool = True) -> float:
    base = 0.82 if candidate_state == "triggered" else 0.62
    if gap_pct is not None:
        base += min(0.12, max(-0.04, gap_pct / 40.0))
    if not high_quality:
        base -= 0.08
    return round(max(0.2, min(0.96, base)), 2)


def _candidate_row(
    *,
    symbol: str,
    name: str,
    market: str,
    category: str,
    signal_key: str,
    signal_label: str,
    current_official_state: str,
    candidate_state: str,
    as_of: str,
    effective_on: str,
    threshold_gap_pct: float | None,
    confidence: float,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "name": name,
        "market": market,
        "category": category,
        "signal_key": signal_key,
        "signal_label": signal_label,
        "current_official_state": current_official_state or "none",
        "candidate_state": candidate_state,
        "as_of": as_of,
        "effective_on": effective_on,
        "threshold_gap_pct": threshold_gap_pct,
        "confidence": confidence,
        "metrics": metrics,
    }


def evaluate_close_swing_candidate(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    close_today: float | None,
    close_prev: float | None,
    close_t3: float | None,
    market_return_3d: float,
    auction_volume_share_pct: float | None,
    next_business_day: Any,
) -> dict[str, Any] | None:
    swing_threshold = 25.0 if market_return_3d >= 8.0 else 15.0
    ret_1d = _pct_change(close_today, close_prev)
    ret_3d = _pct_change(close_today, close_t3)
    if ret_1d is None or ret_3d is None or auction_volume_share_pct is None or close_today is None or close_prev is None:
        return None
    if abs(close_today - close_prev) <= 1 or ret_1d < 5.0 or ret_3d < swing_threshold:
        return None
    if auction_volume_share_pct >= 5.0:
        candidate_state = "triggered"
    elif auction_volume_share_pct >= 4.75:
        candidate_state = "near_trigger"
    else:
        return None
    gap_pct = _threshold_gap(auction_volume_share_pct, 5.0)
    return _candidate_row(
        symbol=symbol,
        name=name,
        market=market,
        category="attention",
        signal_key="close_swing",
        signal_label="투자주의 종가급변",
        current_official_state=current_official_state,
        candidate_state=candidate_state,
        as_of=as_of,
        effective_on=next_business_day(as_of),
        threshold_gap_pct=gap_pct,
        confidence=_signal_confidence(candidate_state, gap_pct=gap_pct),
        metrics={
            "return_1d_pct": ret_1d,
            "return_3d_pct": ret_3d,
            "swing_threshold_pct": swing_threshold,
            "market_return_3d_pct": market_return_3d,
            "auction_volume_share_pct": round(auction_volume_share_pct, 2),
        },
    )


def _evaluate_threshold_group(
    *,
    specs: tuple[tuple[str, str, float, float | None], ...],
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    category: str,
    current_official_state: str,
    next_business_day: Any,
    extra_metrics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for signal_key, signal_label, threshold, actual in specs:
        if actual is None:
            continue
        if actual >= threshold:
            state = "triggered"
        elif actual >= threshold * 0.95:
            state = "near_trigger"
        else:
            continue
        gap_pct = _threshold_gap(actual, threshold)
        metrics = {"actual_return_pct": actual, "threshold_pct": threshold}
        metrics.update(extra_metrics or {})
        rows.append(
            _candidate_row(
                symbol=symbol,
                name=name,
                market=market,
                category=category,
                signal_key=signal_key,
                signal_label=signal_label,
                current_official_state=current_official_state,
                candidate_state=state,
                as_of=as_of,
                effective_on=next_business_day(as_of),
                threshold_gap_pct=gap_pct,
                confidence=_signal_confidence(state, gap_pct=gap_pct),
                metrics=metrics,
            )
        )
    return rows


def evaluate_warning_pre_notice(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    return_3d_pct: float | None,
    return_5d_pct: float | None,
    return_15d_pct: float | None,
    caution_count_15d: int,
    next_business_day: Any,
) -> list[dict[str, Any]]:
    if current_official_state not in {"none"}:
        return []
    rows = _evaluate_threshold_group(
        specs=(
            ("warning_pre_short", "투자경고 지정예고 · 초단기 급등", 100.0, return_3d_pct),
            ("warning_pre_medium", "투자경고 지정예고 · 단기 급등", 60.0, return_5d_pct),
            ("warning_pre_long", "투자경고 지정예고 · 중장기 급등", 100.0, return_15d_pct),
        ),
        as_of=as_of,
        symbol=symbol,
        name=name,
        market=market,
        category="warning",
        current_official_state=current_official_state,
        next_business_day=next_business_day,
    )
    if caution_count_15d >= 5 and return_15d_pct is not None and return_15d_pct >= 75.0 * 0.95:
        state = "triggered" if return_15d_pct >= 75.0 else "near_trigger"
        gap_pct = _threshold_gap(return_15d_pct, 75.0)
        rows.append(
            _candidate_row(
                symbol=symbol,
                name=name,
                market=market,
                category="warning",
                signal_key="warning_pre_repeat_attention",
                signal_label="투자경고 지정예고 · 투자주의 반복지정",
                current_official_state=current_official_state,
                candidate_state=state,
                as_of=as_of,
                effective_on=next_business_day(as_of),
                threshold_gap_pct=gap_pct,
                confidence=_signal_confidence(state, gap_pct=gap_pct),
                metrics={"actual_return_pct": return_15d_pct, "threshold_pct": 75.0, "attention_count_15d": caution_count_15d},
            )
        )
    return rows


def evaluate_warning_designation(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    warning_pre_notice_date: str,
    trading_dates: list[str],
    return_3d_pct: float | None,
    return_5d_pct: float | None,
    return_15d_pct: float | None,
    caution_count_15d: int,
    is_recent_high: bool,
    next_business_day: Any,
) -> list[dict[str, Any]]:
    if current_official_state != "warning_pre_notice" or not _within_session_window(trading_dates, warning_pre_notice_date, 10) or not is_recent_high:
        return []
    rows = _evaluate_threshold_group(
        specs=(
            ("warning_design_short", "투자경고 지정 · 초단기 급등", 100.0, return_3d_pct),
            ("warning_design_medium", "투자경고 지정 · 단기 급등", 60.0, return_5d_pct),
            ("warning_design_long", "투자경고 지정 · 중장기 급등", 100.0, return_15d_pct),
        ),
        as_of=as_of,
        symbol=symbol,
        name=name,
        market=market,
        category="warning",
        current_official_state=current_official_state,
        next_business_day=next_business_day,
        extra_metrics={"recent_high_15d": is_recent_high},
    )
    if caution_count_15d >= 5 and return_15d_pct is not None and return_15d_pct >= 75.0 * 0.95:
        state = "triggered" if return_15d_pct >= 75.0 else "near_trigger"
        gap_pct = _threshold_gap(return_15d_pct, 75.0)
        rows.append(
            _candidate_row(
                symbol=symbol,
                name=name,
                market=market,
                category="warning",
                signal_key="warning_design_repeat_attention",
                signal_label="투자경고 지정 · 투자주의 반복지정",
                current_official_state=current_official_state,
                candidate_state=state,
                as_of=as_of,
                effective_on=next_business_day(as_of),
                threshold_gap_pct=gap_pct,
                confidence=_signal_confidence(state, gap_pct=gap_pct),
                metrics={"actual_return_pct": return_15d_pct, "threshold_pct": 75.0, "attention_count_15d": caution_count_15d},
            )
        )
    return rows


def evaluate_warning_redesignation(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    current_close: float | None,
    return_2d_pct: float | None,
    warning_design_preclose: float | None,
    warning_release_preclose: float | None,
    next_business_day: Any,
) -> dict[str, Any] | None:
    if current_official_state != "none":
        return None
    if current_close is None or return_2d_pct is None or warning_design_preclose is None or warning_release_preclose is None:
        return None
    if current_close <= warning_design_preclose or current_close <= warning_release_preclose:
        return None
    if return_2d_pct >= 40.0:
        state = "triggered"
    elif return_2d_pct >= 38.0:
        state = "near_trigger"
    else:
        return None
    gap_pct = _threshold_gap(return_2d_pct, 40.0)
    return _candidate_row(
        symbol=symbol,
        name=name,
        market=market,
        category="warning",
        signal_key="warning_redesignation",
        signal_label="투자경고 재지정",
        current_official_state=current_official_state,
        candidate_state=state,
        as_of=as_of,
        effective_on=next_business_day(as_of),
        threshold_gap_pct=gap_pct,
        confidence=_signal_confidence(state, gap_pct=gap_pct),
        metrics={
            "return_2d_pct": return_2d_pct,
            "threshold_pct": 40.0,
            "warning_design_preclose": warning_design_preclose,
            "warning_release_preclose": warning_release_preclose,
        },
    )


def evaluate_risk_pre_notice(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    return_3d_pct: float | None,
    return_5d_pct: float | None,
    return_15d_pct: float | None,
    warning_design_date: str,
    trading_dates: list[str],
    is_recent_high: bool,
    next_business_day: Any,
) -> list[dict[str, Any]]:
    if current_official_state != "warning_active" or not is_recent_high:
        return []
    sessions_since_warning = _sessions_since(trading_dates, warning_design_date)
    if sessions_since_warning is None:
        return []
    rows: list[dict[str, Any]] = []
    for signal_key, signal_label, threshold, actual, minimum_sessions in (
        ("risk_pre_short", "투자위험 지정예고 · 초단기 급등", 45.0, return_3d_pct, 3),
        ("risk_pre_medium", "투자위험 지정예고 · 단기 급등", 60.0, return_5d_pct, 5),
        ("risk_pre_long", "투자위험 지정예고 · 중장기 급등", 100.0, return_15d_pct, 15),
    ):
        if sessions_since_warning < minimum_sessions or actual is None:
            continue
        if actual >= threshold:
            state = "triggered"
        elif actual >= threshold * 0.95:
            state = "near_trigger"
        else:
            continue
        gap_pct = _threshold_gap(actual, threshold)
        rows.append(
            _candidate_row(
                symbol=symbol,
                name=name,
                market=market,
                category="risk",
                signal_key=signal_key,
                signal_label=signal_label,
                current_official_state=current_official_state,
                candidate_state=state,
                as_of=as_of,
                effective_on=next_business_day(as_of),
                threshold_gap_pct=gap_pct,
                confidence=_signal_confidence(state, gap_pct=gap_pct),
                metrics={"actual_return_pct": actual, "threshold_pct": threshold, "warning_design_date": warning_design_date},
            )
        )
    return rows


def evaluate_risk_designation(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    risk_pre_notice_date: str,
    trading_dates: list[str],
    return_3d_pct: float | None,
    return_5d_pct: float | None,
    return_15d_pct: float | None,
    is_recent_high: bool,
    next_business_day: Any,
) -> list[dict[str, Any]]:
    if current_official_state != "risk_pre_notice" or not _within_session_window(trading_dates, risk_pre_notice_date, 10) or not is_recent_high:
        return []
    return _evaluate_threshold_group(
        specs=(
            ("risk_design_short", "투자위험 지정 · 초단기 급등", 45.0, return_3d_pct),
            ("risk_design_medium", "투자위험 지정 · 단기 급등", 60.0, return_5d_pct),
            ("risk_design_long", "투자위험 지정 · 중장기 급등", 100.0, return_15d_pct),
        ),
        as_of=as_of,
        symbol=symbol,
        name=name,
        market=market,
        category="risk",
        current_official_state=current_official_state,
        next_business_day=next_business_day,
        extra_metrics={"recent_high_15d": is_recent_high},
    )


def evaluate_warning_halt_candidate(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    current_close: float | None,
    prev_close: float | None,
    return_2d_pct: float | None,
    warning_design_preclose: float | None,
    next_business_day: Any,
) -> dict[str, Any] | None:
    if current_official_state != "warning_active":
        return None
    if current_close is None or prev_close is None or return_2d_pct is None or warning_design_preclose is None:
        return None
    if current_close <= prev_close or current_close <= warning_design_preclose:
        return None
    if return_2d_pct >= 40.0:
        state = "triggered"
    elif return_2d_pct >= 38.0:
        state = "near_trigger"
    else:
        return None
    gap_pct = _threshold_gap(return_2d_pct, 40.0)
    return _candidate_row(
        symbol=symbol,
        name=name,
        market=market,
        category="trading_halt",
        signal_key="warning_halt",
        signal_label="매매거래정지 · 투자경고 연계",
        current_official_state=current_official_state,
        candidate_state=state,
        as_of=as_of,
        effective_on=next_business_day(as_of),
        threshold_gap_pct=gap_pct,
        confidence=_signal_confidence(state, gap_pct=gap_pct),
        metrics={"return_2d_pct": return_2d_pct, "threshold_pct": 40.0, "warning_design_preclose": warning_design_preclose},
    )


def evaluate_risk_halt_candidates(
    *,
    as_of: str,
    symbol: str,
    name: str,
    market: str,
    current_official_state: str,
    closes: list[float],
    risk_design_preclose: float | None,
    next_business_day: Any,
) -> list[dict[str, Any]]:
    if current_official_state != "risk_active" or len(closes) < 4 or risk_design_preclose is None:
        return []
    count = 0
    for index in range(len(closes) - 1, 0, -1):
        if closes[index] > risk_design_preclose and closes[index] > closes[index - 1]:
            count += 1
        else:
            break
    rows: list[dict[str, Any]] = []
    if count >= 2:
        rows.append(
            _candidate_row(
                symbol=symbol,
                name=name,
                market=market,
                category="trading_halt",
                signal_key="risk_halt_pre_notice",
                signal_label="매매거래정지 예고 · 투자위험 연계",
                current_official_state=current_official_state,
                candidate_state="triggered",
                as_of=as_of,
                effective_on=next_business_day(as_of),
                threshold_gap_pct=None,
                confidence=0.78,
                metrics={"consecutive_higher_days": count, "risk_design_preclose": risk_design_preclose},
            )
        )
    if count >= 3:
        rows.append(
            _candidate_row(
                symbol=symbol,
                name=name,
                market=market,
                category="trading_halt",
                signal_key="risk_halt",
                signal_label="매매거래정지 · 투자위험 연계",
                current_official_state=current_official_state,
                candidate_state="triggered",
                as_of=as_of,
                effective_on=next_business_day(as_of),
                threshold_gap_pct=None,
                confidence=0.86,
                metrics={"consecutive_higher_days": count, "risk_design_preclose": risk_design_preclose},
            )
        )
    return rows


def _load_extra_price_history(symbol: str, cache: dict[str, pd.DataFrame], *, end_date: str, load_price_history: Any, parse_iso_date: Any) -> pd.DataFrame:
    if symbol in cache:
        return cache[symbol]
    frame = load_price_history(symbol, end_dt=parse_iso_date(end_date), lookback_days=420, sleep_sec=0.0)
    cache[symbol] = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    return cache[symbol]


def _previous_close_before(
    symbol: str,
    event_date: str,
    *,
    as_of: str,
    cache: dict[str, pd.DataFrame],
    load_price_history: Any,
    parse_iso_date: Any,
    safe_float: Any,
) -> float | None:
    if not event_date:
        return None
    frame = _load_extra_price_history(symbol, cache, end_date=as_of, load_price_history=load_price_history, parse_iso_date=parse_iso_date)
    if frame.empty or "Date" not in frame.columns or "Close" not in frame.columns:
        return None
    event_dt = parse_iso_date(event_date)
    if event_dt is None:
        return None
    scoped = frame[pd.to_datetime(frame["Date"]).dt.date < event_dt].copy()
    if scoped.empty:
        return None
    scoped = scoped.sort_values("Date")
    return safe_float(scoped.iloc[-1]["Close"])


def _auction_volume_share_pct(symbol: str, as_of: str, current_volume: float | None, *, fetch_naver_intraday_history: Any) -> float | None:
    if current_volume is None or current_volume <= 0:
        return None
    try:
        frame = fetch_naver_intraday_history(symbol, as_of)
    except Exception:
        return None
    if frame is None or frame.empty or "DateTime" not in frame.columns:
        return None
    working = frame.copy()
    working["DateTime"] = pd.to_datetime(working["DateTime"], errors="coerce")
    working["DeltaVolume"] = pd.to_numeric(working.get("DeltaVolume"), errors="coerce")
    close_window = working[working["DateTime"].dt.strftime("%H:%M").between("15:20", "15:30")]
    if close_window.empty:
        return None
    auction_volume = close_window["DeltaVolume"].fillna(0.0).sum()
    if auction_volume <= 0:
        return None
    return round(float(auction_volume) / float(current_volume) * 100.0, 2)


def build_market_warning_candidate_snapshot(
    *,
    stock_master: dict[str, dict[str, Any]],
    market_history: dict[str, Any],
    official_state_map: dict[str, dict[str, Any]],
    official_rows: list[dict[str, Any]],
    next_business_day: Any,
    market_group: Any,
    safe_float: Any,
    clean_text: Any,
    now_iso: Any,
    load_price_history: Any,
    fetch_naver_intraday_history: Any,
    parse_iso_date: Any,
    current_state_priority: dict[str, int],
) -> dict[str, Any]:
    as_of = market_history["as_of"]
    trading_dates = list(market_history["trading_dates"])
    market_return_3d = dict(market_history.get("market_return_3d") or {})
    series_by_symbol = market_history["series_by_symbol"]
    attention_dates_by_symbol: dict[str, set[str]] = defaultdict(set)
    for row in official_rows:
        if clean_text(row.get("kind")) == "attention" and clean_text(row.get("action")) == "design":
            symbol = clean_text(row.get("symbol")).zfill(6)
            effective_date = clean_text(row.get("effective_date"))
            if symbol and effective_date:
                attention_dates_by_symbol[symbol].add(effective_date)

    extra_price_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for symbol, meta in stock_master.items():
        stock_market = meta.get("market")
        if stock_market not in {"KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"}:
            continue
        symbol_series = series_by_symbol.get(symbol, {"close": {}, "volume": {}})
        date_list, closes = _series_values(symbol_series.get("close") or {}, trading_dates, safe_float=safe_float)
        _, volumes = _series_values(symbol_series.get("volume") or {}, trading_dates, safe_float=safe_float)
        if not closes or date_list[-1] != as_of:
            continue
        current_state = official_state_map.get(symbol, {}).get("current_state", "none")
        warning_pre_notice_date = clean_text(official_state_map.get(symbol, {}).get("warning_pre_notice_date"))
        warning_design_date = clean_text(official_state_map.get(symbol, {}).get("warning_design_date"))
        warning_release_date = clean_text(official_state_map.get(symbol, {}).get("warning_release_date"))
        risk_pre_notice_date = clean_text(official_state_map.get(symbol, {}).get("risk_pre_notice_date"))
        risk_design_date = clean_text(official_state_map.get(symbol, {}).get("risk_design_date"))
        current_close = _nth_from_end(closes, 0)
        prev_close = _nth_from_end(closes, 1)
        close_t2 = _nth_from_end(closes, 2)
        close_t3 = _nth_from_end(closes, 3)
        close_t5 = _nth_from_end(closes, 5)
        close_t15 = _nth_from_end(closes, 15)
        current_volume = _nth_from_end(volumes, 0)
        return_2d_pct = _pct_change(current_close, close_t2)
        return_3d_pct = _pct_change(current_close, close_t3)
        return_5d_pct = _pct_change(current_close, close_t5)
        return_15d_pct = _pct_change(current_close, close_t15)
        recent_high = _is_recent_high(closes, 15)
        grouped_market = market_group(stock_market)
        recent_attention_dates = {date_text for date_text in attention_dates_by_symbol.get(symbol, set()) if date_text in trading_dates[-15:]}
        attention_count_15d = len(recent_attention_dates)

        if (return_1d := _pct_change(current_close, prev_close)) and return_1d >= 5.0 and return_3d_pct is not None:
            auction_share = _auction_volume_share_pct(
                symbol,
                as_of,
                current_volume,
                fetch_naver_intraday_history=fetch_naver_intraday_history,
            )
            row = evaluate_close_swing_candidate(
                as_of=as_of,
                symbol=symbol,
                name=meta["name"],
                market=stock_market,
                current_official_state=current_state,
                close_today=current_close,
                close_prev=prev_close,
                close_t3=close_t3,
                market_return_3d=market_return_3d.get(grouped_market, 0.0),
                auction_volume_share_pct=auction_share,
                next_business_day=next_business_day,
            )
            if row:
                rows.append(row)

        rows.extend(
            evaluate_warning_pre_notice(
                as_of=as_of,
                symbol=symbol,
                name=meta["name"],
                market=stock_market,
                current_official_state=current_state,
                return_3d_pct=return_3d_pct,
                return_5d_pct=return_5d_pct,
                return_15d_pct=return_15d_pct,
                caution_count_15d=attention_count_15d,
                next_business_day=next_business_day,
            )
        )
        rows.extend(
            evaluate_warning_designation(
                as_of=as_of,
                symbol=symbol,
                name=meta["name"],
                market=stock_market,
                current_official_state=current_state,
                warning_pre_notice_date=warning_pre_notice_date,
                trading_dates=date_list,
                return_3d_pct=return_3d_pct,
                return_5d_pct=return_5d_pct,
                return_15d_pct=return_15d_pct,
                caution_count_15d=attention_count_15d,
                is_recent_high=recent_high,
                next_business_day=next_business_day,
            )
        )
        warning_design_preclose = _previous_close_before(
            symbol,
            warning_design_date,
            as_of=as_of,
            cache=extra_price_cache,
            load_price_history=load_price_history,
            parse_iso_date=parse_iso_date,
            safe_float=safe_float,
        )
        warning_release_preclose = _previous_close_before(
            symbol,
            warning_release_date,
            as_of=as_of,
            cache=extra_price_cache,
            load_price_history=load_price_history,
            parse_iso_date=parse_iso_date,
            safe_float=safe_float,
        )
        redesignation = evaluate_warning_redesignation(
            as_of=as_of,
            symbol=symbol,
            name=meta["name"],
            market=stock_market,
            current_official_state=current_state,
            current_close=current_close,
            return_2d_pct=return_2d_pct,
            warning_design_preclose=warning_design_preclose,
            warning_release_preclose=warning_release_preclose,
            next_business_day=next_business_day,
        )
        if redesignation:
            rows.append(redesignation)

        rows.extend(
            evaluate_risk_pre_notice(
                as_of=as_of,
                symbol=symbol,
                name=meta["name"],
                market=stock_market,
                current_official_state=current_state,
                return_3d_pct=return_3d_pct,
                return_5d_pct=return_5d_pct,
                return_15d_pct=return_15d_pct,
                warning_design_date=warning_design_date,
                trading_dates=date_list,
                is_recent_high=recent_high,
                next_business_day=next_business_day,
            )
        )
        rows.extend(
            evaluate_risk_designation(
                as_of=as_of,
                symbol=symbol,
                name=meta["name"],
                market=stock_market,
                current_official_state=current_state,
                risk_pre_notice_date=risk_pre_notice_date,
                trading_dates=date_list,
                return_3d_pct=return_3d_pct,
                return_5d_pct=return_5d_pct,
                return_15d_pct=return_15d_pct,
                is_recent_high=recent_high,
                next_business_day=next_business_day,
            )
        )
        warning_halt = evaluate_warning_halt_candidate(
            as_of=as_of,
            symbol=symbol,
            name=meta["name"],
            market=stock_market,
            current_official_state=current_state,
            current_close=current_close,
            prev_close=prev_close,
            return_2d_pct=return_2d_pct,
            warning_design_preclose=warning_design_preclose,
            next_business_day=next_business_day,
        )
        if warning_halt:
            rows.append(warning_halt)

        risk_design_preclose = _previous_close_before(
            symbol,
            risk_design_date,
            as_of=as_of,
            cache=extra_price_cache,
            load_price_history=load_price_history,
            parse_iso_date=parse_iso_date,
            safe_float=safe_float,
        )
        rows.extend(
            evaluate_risk_halt_candidates(
                as_of=as_of,
                symbol=symbol,
                name=meta["name"],
                market=stock_market,
                current_official_state=current_state,
                closes=closes,
                risk_design_preclose=risk_design_preclose,
                next_business_day=next_business_day,
            )
        )

    rows.sort(
        key=lambda row: (
            0 if clean_text(row.get("candidate_state")) == "triggered" else 1,
            -current_state_priority.get(clean_text(row.get("current_official_state")), 0),
            -float(row.get("confidence") or 0.0),
            abs(float(row.get("threshold_gap_pct") or 0.0)),
            clean_text(row.get("name")),
        )
    )
    return {
        "generated_at": now_iso(),
        "as_of": as_of,
        "status": "live",
        "stale_since": None,
        "source_error": "",
        "next_effective_date": next_business_day(as_of),
        "rows": rows,
        "summary": {
            "row_count": len(rows),
            "triggered_count": sum(1 for row in rows if clean_text(row.get("candidate_state")) == "triggered"),
            "near_trigger_count": sum(1 for row in rows if clean_text(row.get("candidate_state")) == "near_trigger"),
            "category_counts": dict(Counter(clean_text(row.get("category")) for row in rows)),
        },
    }
