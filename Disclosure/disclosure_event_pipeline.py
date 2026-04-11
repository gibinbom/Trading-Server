from __future__ import annotations

import argparse
import html
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

import pandas as pd

try:
    from naver_price_fallback import fetch_naver_daily_price_history
except Exception:
    from Disclosure.naver_price_fallback import fetch_naver_daily_price_history

try:
    import FinanceDataReader as fdr
except Exception:  # pragma: no cover
    fdr = None

try:
    from pykrx import stock as pykrx_stock
except Exception:
    pykrx_stock = None

try:
    from context_alignment import load_latest_symbol_sector_map
except Exception:
    from Disclosure.context_alignment import load_latest_symbol_sector_map

try:
    from sector_resolver import resolve_sector_map
except Exception:
    from Disclosure.sector_resolver import resolve_sector_map


log = logging.getLogger("disclosure.event_pipeline")
KST = timezone(timedelta(hours=9), name="KST")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
EVENT_ROOT_DIR = os.path.join(ROOT_DIR, "events")
EVENT_LOG_DIR = os.path.join(EVENT_ROOT_DIR, "logs")
EVENT_REPORT_DIR = os.path.join(EVENT_ROOT_DIR, "reports")
PRICE_CACHE_DIR = os.path.join(ROOT_DIR, "factors", "cache", "prices")
DEFAULT_FORWARD_HORIZONS = (1, 3, 5, 10)


def _ensure_dirs() -> None:
    os.makedirs(EVENT_LOG_DIR, exist_ok=True)
    os.makedirs(EVENT_REPORT_DIR, exist_ok=True)
    os.makedirs(PRICE_CACHE_DIR, exist_ok=True)


def _now_kst() -> datetime:
    return datetime.now(KST)


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(record or {})
    now = _now_kst()
    payload.setdefault("recorded_at", now.isoformat(timespec="seconds"))
    payload.setdefault("record_date", now.strftime("%Y-%m-%d"))
    payload.setdefault("record_time", now.strftime("%H:%M"))
    payload.setdefault("event_type", "UNCLASSIFIED")
    payload.setdefault("signal_bias", "neutral")
    payload.setdefault("strategy_name", "observe_only")
    payload.setdefault("trade_executed", False)
    payload.setdefault("metrics", {})
    payload.setdefault("tags", [])
    return payload


class DisclosureEventLogger:
    def __init__(self, log_dir: str = EVENT_LOG_DIR):
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

    def _path_for_date(self, date_str: str) -> str:
        return os.path.join(self.log_dir, f"disclosure_events_{date_str}.jsonl")

    def append(self, record: Dict[str, Any]) -> str:
        payload = _normalize_record(record)
        event_date = str(payload.get("event_date") or payload.get("record_date") or "").replace(".", "-")
        if not event_date:
            event_date = _now_kst().strftime("%Y-%m-%d")
        path = self._path_for_date(event_date.replace("-", ""))
        with open(path, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return path


def load_event_records(
    *,
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    log_dir: str = EVENT_LOG_DIR,
) -> list[Dict[str, Any]]:
    _ensure_dirs()
    deduped_records: dict[tuple[str, str], Dict[str, Any]] = {}

    if end_date:
        end_dt = pd.Timestamp(end_date).normalize()
    else:
        end_dt = pd.Timestamp(_now_kst().date())
    if start_date:
        start_dt = pd.Timestamp(start_date).normalize()
    else:
        start_dt = end_dt - pd.Timedelta(days=max(0, int(days) - 1))

    current = start_dt
    while current <= end_dt:
        path = os.path.join(log_dir, f"disclosure_events_{current.strftime('%Y%m%d')}.jsonl")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                        dedup_key = (
                            str(payload.get("stock_code") or "").zfill(6),
                            str(payload.get("rcp_no") or ""),
                        )
                        # Append-only logs may contain a corrected reclassification for the same receipt number.
                        # Keep the latest occurrence so recent backfills can override stale title classifications.
                        deduped_records[dedup_key] = payload
                    except json.JSONDecodeError as exc:
                        log.warning("event log decode failed (%s): %s", path, exc)
        current += pd.Timedelta(days=1)

    return list(deduped_records.values())


def _load_event_sector_map(symbols: Iterable[str]) -> dict[str, str]:
    symbol_list = [str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()]
    if not symbol_list:
        return {}
    sector_map = load_latest_symbol_sector_map()
    missing = [symbol for symbol in symbol_list if not sector_map.get(symbol)]
    if missing:
        try:
            resolved = resolve_sector_map(missing, sleep_sec=0.0, max_fetch=60)
        except Exception:
            resolved = {}
        for symbol, sector in (resolved or {}).items():
            if sector and str(sector).strip() and str(sector) != "Unknown":
                sector_map[str(symbol).zfill(6)] = str(sector)
    return {symbol: str(sector_map.get(symbol) or "Unknown") for symbol in symbol_list}


def load_price_history(symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    _ensure_dirs()
    symbol = str(symbol or "").zfill(6)
    cache_path = os.path.join(PRICE_CACHE_DIR, f"{symbol}.csv")
    df: Optional[pd.DataFrame] = None

    if os.path.exists(cache_path):
        try:
            df = pd.read_csv(cache_path, parse_dates=["Date"])
        except Exception as exc:
            log.warning("price cache load failed (%s): %s", cache_path, exc)

    if df is None or df.empty:
        if fdr is not None:
            try:
                df = fdr.DataReader(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    df = df.reset_index()
                    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            except Exception as exc:
                log.warning("price fetch failed (%s): %s", symbol, exc)
        if (df is None or df.empty) and pykrx_stock is not None:
            try:
                df = pykrx_stock.get_market_ohlcv_by_date(
                    pd.Timestamp(start_date).strftime("%Y%m%d"),
                    pd.Timestamp(end_date).strftime("%Y%m%d"),
                    symbol,
                ).reset_index()
                if df is not None and not df.empty:
                    df = df.rename(
                        columns={"날짜": "Date", "시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume"}
                    )
                    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            except Exception as exc:
                log.warning("pykrx price fetch failed (%s): %s", symbol, exc)
        if df is None or df.empty:
            try:
                fallback_df = fetch_naver_daily_price_history(
                    symbol,
                    start_date=pd.Timestamp(start_date).date(),
                    end_date=pd.Timestamp(end_date).date(),
                    lookback_days=max(260, (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 10),
                    sleep_sec=0.0,
                )
                if fallback_df is not None and not fallback_df.empty:
                    df = fallback_df
                    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            except Exception as exc:
                log.warning("naver price fallback failed (%s): %s", symbol, exc)

    if df is None or df.empty:
        return None

    if "Date" not in df.columns:
        df = df.reset_index()
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    df = df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")

    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col not in df.columns:
            df[col] = pd.NA

    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    return df[(df["Date"] >= start_ts) & (df["Date"] <= end_ts)].reset_index(drop=True)


def _event_row_index(price_df: pd.DataFrame, event_date: pd.Timestamp, event_time_hhmm: str) -> Optional[int]:
    same_day = price_df.index[price_df["Date"].dt.normalize() == event_date.normalize()].tolist()
    if not same_day:
        later = price_df.index[price_df["Date"].dt.normalize() > event_date.normalize()].tolist()
        return later[0] if later else None

    idx = same_day[0]
    if event_time_hhmm and str(event_time_hhmm) >= "15:30":
        next_idx = idx + 1
        return next_idx if next_idx < len(price_df) else None
    return idx


def _forward_return_map(price_df: pd.DataFrame, entry_idx: int, horizons: Iterable[int]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    if entry_idx is None or entry_idx >= len(price_df):
        for horizon in horizons:
            out[f"ret_{horizon}d"] = None
        return out

    entry_close = _safe_float(price_df.iloc[entry_idx]["Close"])
    if not entry_close or entry_close <= 0:
        for horizon in horizons:
            out[f"ret_{horizon}d"] = None
        return out

    for horizon in horizons:
        exit_idx = entry_idx + int(horizon)
        if exit_idx >= len(price_df):
            out[f"ret_{horizon}d"] = None
            continue
        exit_close = _safe_float(price_df.iloc[exit_idx]["Close"])
        if not exit_close or exit_close <= 0:
            out[f"ret_{horizon}d"] = None
            continue
        out[f"ret_{horizon}d"] = ((exit_close / entry_close) - 1.0) * 100.0
    return out


def _max_drawdown_pct(price_df: pd.DataFrame, entry_idx: int, horizon: int) -> Optional[float]:
    if entry_idx is None or entry_idx >= len(price_df):
        return None
    entry_close = _safe_float(price_df.iloc[entry_idx]["Close"])
    if not entry_close or entry_close <= 0:
        return None

    end_idx = min(len(price_df), entry_idx + int(horizon) + 1)
    min_low = _safe_float(price_df.iloc[entry_idx:end_idx]["Low"].min())
    if min_low is None:
        return None
    return ((min_low / entry_close) - 1.0) * 100.0


def _find_rebound_entry(
    price_df: pd.DataFrame,
    event_idx: int,
    *,
    drop_pct: float = 8.0,
    recovery_ratio: float = 0.5,
    search_days: int = 10,
) -> Optional[Dict[str, Any]]:
    if event_idx is None or event_idx >= len(price_df):
        return None

    event_close = _safe_float(price_df.iloc[event_idx]["Close"])
    if not event_close or event_close <= 0:
        return None

    threshold_drop_price = event_close * (1.0 - drop_pct / 100.0)
    trough_idx: Optional[int] = None
    trough_low: Optional[float] = None
    max_idx = min(len(price_df) - 1, event_idx + max(1, int(search_days)))

    for idx in range(event_idx + 1, max_idx + 1):
        low_px = _safe_float(price_df.iloc[idx]["Low"])
        close_px = _safe_float(price_df.iloc[idx]["Close"])
        if low_px is None or close_px is None:
            continue

        if trough_low is None or low_px < trough_low:
            trough_low = low_px
            trough_idx = idx

        if trough_low is None or trough_low > threshold_drop_price:
            continue

        recovery_level = trough_low + (event_close - trough_low) * float(recovery_ratio)
        if close_px >= recovery_level:
            return {
                "entry_idx": idx,
                "entry_date": price_df.iloc[idx]["Date"].strftime("%Y-%m-%d"),
                "entry_price": close_px,
                "trough_idx": trough_idx,
                "trough_date": price_df.iloc[trough_idx]["Date"].strftime("%Y-%m-%d") if trough_idx is not None else None,
                "trough_low": trough_low,
                "event_close": event_close,
                "recovery_level": recovery_level,
                "drop_pct": ((trough_low / event_close) - 1.0) * 100.0,
                "recovery_ratio": recovery_ratio,
            }

    return None


def _win_rate_pct(series: pd.Series) -> float:
    values = pd.to_numeric(pd.Series(series), errors="coerce").dropna()
    if values.empty:
        return math.nan
    return float((values > 0).mean() * 100.0)


def _fmt_pct_display(value: Any, digits: int = 2) -> str:
    number = _safe_float(value)
    if number is None or pd.isna(number):
        return "-"
    return f"{number:.{digits}f}%"


def _reaction_profile(avg_ret_1d: Any, avg_ret_3d: Any, avg_ret_5d: Any, valid_sample_size: int) -> str:
    if valid_sample_size <= 0:
        return "성과 대기"
    ret1 = _safe_float(avg_ret_1d) or 0.0
    ret3 = _safe_float(avg_ret_3d) or 0.0
    ret5 = _safe_float(avg_ret_5d) or 0.0
    if ret1 > 0 and ret3 > 0 and ret5 > 0:
        return "초기 반응과 유지력"
    if ret1 > 0 and (ret3 <= 0 or ret5 <= 0):
        return "초기 반응 후 둔화"
    if ret1 <= 0 and (ret3 > 0 or ret5 > 0):
        return "시간차 반응"
    return "반응 약함"


def _build_tactical_fields(
    interpretation_label: str,
    reaction_profile: str,
    avg_ret_1d: Any,
    avg_ret_3d: Any,
    avg_ret_5d: Any,
) -> Dict[str, str]:
    ret1 = _safe_float(avg_ret_1d) or 0.0
    ret3 = _safe_float(avg_ret_3d) or 0.0
    ret5 = _safe_float(avg_ret_5d) or 0.0

    if interpretation_label == "참고 가능":
        return {
            "tactical_label": "참고 가능",
            "tactical_note": "평균 성과와 승률, 낙폭을 함께 볼 때 기본 참고 재료로 삼을 만합니다.",
        }
    if interpretation_label == "표본 얕음":
        if reaction_profile == "초기 반응 후 둔화" and max(ret1, ret3) > 0:
            return {
                "tactical_label": "단기 반응형",
                "tactical_note": "초기 며칠 반응은 있었지만 오래 끌기보다 짧게만 보는 편이 낫습니다.",
            }
        if reaction_profile == "시간차 반응" and max(ret3, ret5) > 0:
            return {
                "tactical_label": "지연 반응형",
                "tactical_note": "당일보다 하루 이틀 뒤 반응이 붙는 경우가 있어 시간차 확인이 더 중요합니다.",
            }
        if max(ret1, ret3, ret5) <= 0:
            return {
                "tactical_label": "보수적 관찰",
                "tactical_note": "현재 표본만 보면 초기 반응과 5일 성과가 모두 약해 존재 여부만 체크하는 편이 낫습니다.",
            }
        return {
            "tactical_label": "표본 얕음",
            "tactical_note": "재료는 보이지만 표본이 적어 아직 단독 판단 근거로 쓰기에는 이릅니다.",
        }
    if interpretation_label == "변동성 주의":
        return {
            "tactical_label": "변동성 주의",
            "tactical_note": "평균 성과보다 중간 흔들림 관리가 더 중요해 눌림 확인이 먼저입니다.",
        }
    if interpretation_label == "보수적":
        return {
            "tactical_label": "보수적",
            "tactical_note": "과거 평균 기준으로는 기대보다 부담이 커 단독 재료로 보기 어렵습니다.",
        }
    if reaction_profile == "성과 대기":
        return {
            "tactical_label": "성과 대기",
            "tactical_note": "아직 5일 성과가 붙지 않아 방향 판단보다 기록 축적이 먼저입니다.",
        }
    return {
        "tactical_label": "해석 보류",
        "tactical_note": "숫자가 더 쌓이기 전까지는 존재 여부만 체크하는 편이 낫습니다.",
    }


def _dominant_bias(row: Dict[str, Any]) -> str:
    pos = int(_safe_int(row.get("positive_count")) or 0)
    neg = int(_safe_int(row.get("negative_count")) or 0)
    neu = int(_safe_int(row.get("neutral_count")) or 0)
    if pos <= 0 and neg <= 0 and neu <= 0:
        return ""
    ordered = sorted(
        [("positive", pos), ("negative", neg), ("neutral", neu)],
        key=lambda kv: (kv[1], 1 if kv[0] == "positive" else (0 if kv[0] == "neutral" else -1)),
        reverse=True,
    )
    return ordered[0][0]


def _build_interpretation_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    sample_size = int(_safe_int(row.get("sample_size")) or 0)
    valid_ret_5d_count = int(_safe_int(row.get("valid_ret_5d_count")) or 0)
    valid_ret_10d_count = int(_safe_int(row.get("valid_ret_10d_count")) or 0)
    pending_count = int(_safe_int(row.get("pending_count")) or max(0, sample_size - valid_ret_5d_count))
    price_coverage_pct = (
        float(valid_ret_5d_count) / float(sample_size) * 100.0
        if sample_size > 0
        else 0.0
    )
    avg_ret_1d = _safe_float(row.get("avg_ret_1d"))
    avg_ret_3d = _safe_float(row.get("avg_ret_3d"))
    avg_ret_5d = _safe_float(row.get("avg_ret_5d"))
    win_rate_5d = _safe_float(row.get("win_rate_5d"))
    avg_mdd_5d = _safe_float(row.get("avg_mdd_5d"))
    dominant_bias = str(row.get("dominant_bias") or _dominant_bias(row) or "").lower()
    reaction_profile = _reaction_profile(avg_ret_1d, avg_ret_3d, avg_ret_5d, valid_ret_5d_count)

    base_edge = (
        (avg_ret_5d or 0.0) * 0.60
        + (((win_rate_5d if win_rate_5d is not None else 50.0) - 50.0) / 10.0) * 0.25
        - abs(avg_mdd_5d or 0.0) * 0.15
        + math.log1p(max(valid_ret_5d_count, 0)) * 0.15
    )
    coverage_factor = min(1.0, valid_ret_5d_count / 8.0) * min(1.0, price_coverage_pct / 100.0)
    interpretable_score = round(base_edge * coverage_factor, 4)

    if valid_ret_5d_count <= 0:
        interpretation_label = "해석 보류"
        confidence_label = "낮음"
        interpretation_note = "아직 5일 성과가 붙은 표본이 없어 방향 판단을 내리기 어렵습니다."
    elif price_coverage_pct < 40.0:
        interpretation_label = "표본 얕음"
        confidence_label = "낮음"
        interpretation_note = "가격이 충분히 붙지 않아 아직 과신보다 관찰이 먼저입니다."
    elif valid_ret_5d_count < 8:
        interpretation_label = "표본 얕음"
        confidence_label = "낮음" if valid_ret_5d_count < 3 else "중간"
        interpretation_note = "초기 경향은 보이지만 아직 표본이 얕아 단독 근거로 쓰기에는 이릅니다."
    elif avg_ret_5d is None or pd.isna(avg_ret_5d) or win_rate_5d is None or pd.isna(win_rate_5d):
        interpretation_label = "해석 보류"
        confidence_label = "낮음"
        interpretation_note = "수익률 통계가 아직 비어 있어 방향 판단을 내리기 어렵습니다."
    elif avg_ret_5d <= 0 or win_rate_5d < 45.0:
        interpretation_label = "보수적"
        confidence_label = "높음" if price_coverage_pct >= 70.0 else "중간"
        interpretation_note = "평균 수익이나 승률이 약해 단독 재료로 해석하기 어렵습니다."
    elif (avg_mdd_5d or 0.0) <= -8.0 or win_rate_5d < 55.0:
        interpretation_label = "변동성 주의"
        confidence_label = "높음" if price_coverage_pct >= 70.0 else "중간"
        interpretation_note = "평균 수익은 괜찮지만 중간 흔들림이 커 눌림 확인이 먼저입니다."
    else:
        interpretation_label = "참고 가능"
        confidence_label = "높음" if price_coverage_pct >= 70.0 else "중간"
        interpretation_note = "유효 표본이 충분하고 평균 수익과 승률이 모두 우호적입니다."

    if pending_count > 0 and interpretation_label in {"참고 가능", "변동성 주의"}:
        interpretation_note += f" 아직 5일 성과가 붙지 않은 대기 표본 `{pending_count}`건이 남아 있습니다."

    tactical_fields = _build_tactical_fields(
        interpretation_label,
        reaction_profile,
        avg_ret_1d,
        avg_ret_3d,
        avg_ret_5d,
    )
    if dominant_bias == "neutral":
        tactical_fields = {
            "tactical_label": "존재 확인",
            "tactical_note": "방향성이 약한 공시가 많아 매수 재료보다 존재 여부만 확인하는 편이 낫습니다.",
        }
    elif dominant_bias == "negative" and tactical_fields["tactical_label"] in {"참고 가능", "단기 반응형", "지연 반응형", "표본 얕음"}:
        tactical_fields = {
            "tactical_label": "보수적",
            "tactical_note": "부정 공시 비중이 높아 반등이 나와도 보수적으로 읽는 편이 낫습니다.",
        }

    return {
        "valid_ret_5d_count": valid_ret_5d_count,
        "valid_ret_10d_count": valid_ret_10d_count,
        "pending_count": pending_count,
        "price_coverage_pct": round(price_coverage_pct, 2),
        "reaction_profile": reaction_profile,
        "dominant_bias": dominant_bias or "",
        "interpretation_label": interpretation_label,
        "confidence_label": confidence_label,
        "interpretation_note": interpretation_note,
        "tactical_label": tactical_fields["tactical_label"],
        "tactical_note": tactical_fields["tactical_note"],
        "human_summary": f"{tactical_fields['tactical_label']} | {tactical_fields['tactical_note']}",
        "interpretable_score": interpretable_score,
    }


def decorate_summary_frame(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df

    enriched_rows = []
    for row in summary_df.to_dict(orient="records"):
        enriched = dict(row)
        enriched.update(_build_interpretation_fields(row))
        enriched_rows.append(enriched)
    out = pd.DataFrame(enriched_rows)
    return out.sort_values(
        ["interpretable_score", "valid_ret_5d_count", "sample_size"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _build_human_summary(summary_df: pd.DataFrame, metadata: Dict[str, Any], max_rows: int = 8) -> Dict[str, Any]:
    if summary_df.empty:
        return {
            "overall_level": "낮음",
            "overall_note": "가격이 붙은 백테스트 행이 아직 없어 해석을 보류하는 편이 낫습니다.",
            "focus_rows": [],
            "short_term_rows": [],
            "delayed_rows": [],
            "shallow_rows": [],
            "hold_rows": [],
            "caution_rows": [],
            "pending_note": "",
        }

    work_df = summary_df.copy()
    valid_combo_count = int(pd.to_numeric(work_df.get("valid_ret_5d_count"), errors="coerce").fillna(0).gt(0).sum())
    focus_rows = work_df[work_df["tactical_label"] == "참고 가능"].head(max_rows).to_dict(orient="records")
    short_term_rows = work_df[work_df["tactical_label"] == "단기 반응형"].sort_values(
        ["valid_ret_5d_count", "interpretable_score", "sample_size"],
        ascending=[False, False, False],
    ).head(max_rows).to_dict(orient="records")
    delayed_rows = work_df[work_df["tactical_label"] == "지연 반응형"].sort_values(
        ["valid_ret_5d_count", "interpretable_score", "sample_size"],
        ascending=[False, False, False],
    ).head(max_rows).to_dict(orient="records")
    neutral_rows = work_df[work_df["tactical_label"] == "존재 확인"].sort_values(
        ["sample_size", "valid_ret_5d_count", "interpretable_score"],
        ascending=[False, False, False],
    ).head(max_rows).to_dict(orient="records")
    shallow_rows = work_df[
        (work_df["interpretation_label"] == "표본 얕음")
        & ~work_df["tactical_label"].isin(["단기 반응형", "지연 반응형", "보수적 관찰", "존재 확인"])
    ].sort_values(
        ["valid_ret_5d_count", "interpretable_score", "sample_size"],
        ascending=[False, False, False],
    ).head(max_rows).to_dict(orient="records")
    hold_rows = work_df[
        (work_df["interpretation_label"] == "해석 보류")
        & work_df["tactical_label"].ne("존재 확인")
    ].sort_values(
        ["sample_size", "pending_count"],
        ascending=[False, False],
    ).head(max_rows).to_dict(orient="records")
    caution_rows = work_df[
        work_df["interpretation_label"].isin(["변동성 주의", "보수적"])
        | work_df["tactical_label"].isin(["보수적 관찰"])
    ].sort_values(
        ["valid_ret_5d_count", "interpretable_score", "sample_size"],
        ascending=[False, False, False],
    ).head(max_rows).to_dict(orient="records")

    generated_at = pd.to_datetime(metadata.get("generated_at"), errors="coerce")
    age_days = 0
    if pd.notna(generated_at):
        age_days = max(0, int((_now_kst() - generated_at.to_pydatetime().replace(tzinfo=KST)).total_seconds() // 86400))

    pending_total = int(_safe_int(metadata.get("pending_price_records")) or 0)
    record_count = int(_safe_int(metadata.get("record_count")) or 0)
    if valid_combo_count <= 0:
        overall_level = "낮음"
        overall_note = "아직 사후 수익률이 충분히 쌓이지 않아 해석 신뢰도가 낮습니다."
    elif focus_rows:
        overall_level = "중간"
        overall_note = "일부 조합은 참고할 수 있지만, 나머지는 표본 품질과 낙폭을 함께 봐야 합니다."
    elif short_term_rows or delayed_rows:
        overall_level = "중간"
        overall_note = "초기 반응형 조합은 보이지만 아직 유지력 확인 전이라 짧은 관찰이 먼저입니다."
    else:
        overall_level = "중간"
        overall_note = "초기 경향은 보이지만 아직 과신보다 관찰이 먼저입니다."

    if age_days >= 2:
        overall_note += f" 최신 기준 시점 기록이 `{age_days}`일 전이라 현재 장세와 차이가 있을 수 있습니다."
    if pending_total > max(5, int(record_count * 0.2)):
        overall_note += f" 아직 5일 성과 대기 표본 `{pending_total}`건이 남아 있습니다."

    pending_note = ""
    if pending_total > 0:
        pending_note = f"아직 5일 성과가 붙지 않은 대기 표본이 `{pending_total}`건 있습니다."

    return {
        "overall_level": overall_level,
        "overall_note": overall_note,
        "focus_rows": focus_rows,
        "short_term_rows": short_term_rows,
        "delayed_rows": delayed_rows,
        "neutral_rows": neutral_rows,
        "shallow_rows": shallow_rows,
        "hold_rows": hold_rows,
        "caution_rows": caution_rows,
        "pending_note": pending_note,
    }


def _format_human_row(row: Dict[str, Any]) -> str:
    valid = int(_safe_int(row.get("valid_ret_5d_count")) or 0)
    sample = int(_safe_int(row.get("sample_size")) or 0)
    coverage = _fmt_pct_display(row.get("price_coverage_pct"), 1)
    avg1d = _fmt_pct_display(row.get("avg_ret_1d"), 2)
    avg3d = _fmt_pct_display(row.get("avg_ret_3d"), 2)
    avg5d = _fmt_pct_display(row.get("avg_ret_5d"), 2)
    win5d = _fmt_pct_display(row.get("win_rate_5d"), 1)
    mdd5d = _fmt_pct_display(row.get("avg_mdd_5d"), 2)
    bias = str(row.get("dominant_bias") or "-")
    return (
        f"{row.get('event_type') or '-'} / {row.get('backtest_strategy') or '-'} | "
        f"방향 `{bias}` | 판단 `{row.get('tactical_label') or '-'}` | 해석 `{row.get('interpretation_label') or '-'}` | 신뢰도 `{row.get('confidence_label') or '-'}` | "
        f"반응 `{row.get('reaction_profile') or '-'}` | 유효5일 `{valid}/{sample}` ({coverage}) | 초기 `{avg1d}` / `{avg3d}` | 평균5일 `{avg5d}` | 승률 `{win5d}` | 평균낙폭 `{mdd5d}`"
    )


def _build_sector_human_summary(sector_summary_df: pd.DataFrame, max_rows: int = 6) -> Dict[str, Any]:
    if sector_summary_df.empty:
        return {
            "overall_note": "",
            "focus_rows": [],
            "caution_rows": [],
        }

    work_df = sector_summary_df.copy()
    work_df["valid_ret_5d_count"] = pd.to_numeric(work_df.get("valid_ret_5d_count"), errors="coerce").fillna(0)
    actionable = work_df[
        work_df["tactical_label"].isin(["참고 가능", "단기 반응형", "지연 반응형"])
        & work_df["valid_ret_5d_count"].ge(3)
    ].sort_values(
        ["interpretable_score", "valid_ret_5d_count", "sample_size"],
        ascending=[False, False, False],
    )
    caution = work_df[
        work_df["interpretation_label"].isin(["변동성 주의", "보수적"])
        | work_df["tactical_label"].isin(["보수적 관찰"])
    ].sort_values(
        ["interpretable_score", "valid_ret_5d_count", "sample_size"],
        ascending=[False, False, False],
    )

    focus_rows: list[Dict[str, Any]] = []
    seen_focus: set[tuple[str, str]] = set()
    for row in actionable.to_dict(orient="records"):
        key = (str(row.get("event_type") or ""), str(row.get("sector") or ""))
        if key in seen_focus:
            continue
        seen_focus.add(key)
        focus_rows.append(row)
        if len(focus_rows) >= max_rows:
            break

    caution_rows: list[Dict[str, Any]] = []
    seen_caution: set[tuple[str, str]] = set()
    for row in caution.to_dict(orient="records"):
        key = (str(row.get("event_type") or ""), str(row.get("sector") or ""))
        if key in seen_caution:
            continue
        seen_caution.add(key)
        caution_rows.append(row)
        if len(caution_rows) >= max_rows:
            break

    overall_note = ""
    if focus_rows:
        overall_note = "같은 이벤트라도 섹터에 따라 반응 차이가 보여, 유형만 보지 말고 섹터까지 함께 읽는 편이 낫습니다."
    elif caution_rows:
        overall_note = "섹터까지 잘라 보면 보수적으로 읽어야 할 조합이 먼저 보입니다."
    return {
        "overall_note": overall_note,
        "focus_rows": focus_rows,
        "caution_rows": caution_rows,
    }


def _format_sector_human_row(row: Dict[str, Any]) -> str:
    valid = int(_safe_int(row.get("valid_ret_5d_count")) or 0)
    sample = int(_safe_int(row.get("sample_size")) or 0)
    coverage = _fmt_pct_display(row.get("price_coverage_pct"), 1)
    avg1d = _fmt_pct_display(row.get("avg_ret_1d"), 2)
    avg3d = _fmt_pct_display(row.get("avg_ret_3d"), 2)
    avg5d = _fmt_pct_display(row.get("avg_ret_5d"), 2)
    mdd5d = _fmt_pct_display(row.get("avg_mdd_5d"), 2)
    bias = str(row.get("dominant_bias") or "-")
    return (
        f"{row.get('event_type') or '-'} / {row.get('sector') or '-'} | "
        f"방향 `{bias}` | 판단 `{row.get('tactical_label') or '-'}` | 해석 `{row.get('interpretation_label') or '-'}` | "
        f"반응 `{row.get('reaction_profile') or '-'}` | 유효5일 `{valid}/{sample}` ({coverage}) | "
        f"초기 `{avg1d}` / `{avg3d}` | 평균5일 `{avg5d}` | 평균낙폭 `{mdd5d}`"
    )


def _aggregate_summary(detail_df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    work_df = detail_df.copy()
    if "signal_bias" not in work_df.columns:
        work_df["signal_bias"] = "neutral"
    summary_df = (
        work_df.groupby(group_cols, dropna=False)
        .agg(
            sample_size=("stock_code", "count"),
            valid_ret_5d_count=("ret_5d", "count"),
            valid_ret_10d_count=("ret_10d", "count"),
            pending_count=("ret_5d", lambda values: int(len(values) - values.notna().sum())),
            avg_ret_1d=("ret_1d", "mean"),
            avg_ret_3d=("ret_3d", "mean"),
            avg_ret_5d=("ret_5d", "mean"),
            avg_ret_10d=("ret_10d", "mean"),
            median_ret_5d=("ret_5d", "median"),
            win_rate_5d=("ret_5d", _win_rate_pct),
            avg_mdd_5d=("max_drawdown_5d", "mean"),
            positive_count=("signal_bias", lambda values: int(pd.Series(values).astype(str).str.lower().eq("positive").sum())),
            negative_count=("signal_bias", lambda values: int(pd.Series(values).astype(str).str.lower().eq("negative").sum())),
            neutral_count=("signal_bias", lambda values: int(pd.Series(values).astype(str).str.lower().eq("neutral").sum())),
        )
        .reset_index()
    )
    summary_df["dominant_bias"] = summary_df.apply(_dominant_bias, axis=1)
    return decorate_summary_frame(summary_df)


def build_sector_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty or "sector" not in detail_df.columns:
        return pd.DataFrame()
    sector_df = detail_df.copy()
    sector_df["sector"] = sector_df.get("sector", pd.Series(dtype=object)).fillna("Unknown").astype(str)
    sector_df = sector_df[sector_df["sector"].str.strip().ne("")].copy()
    if sector_df.empty:
        return pd.DataFrame()
    return _aggregate_summary(sector_df, ["event_type", "backtest_strategy", "sector"])


def build_backtest_frames(
    records: list[Dict[str, Any]],
    *,
    horizons: Iterable[int] = DEFAULT_FORWARD_HORIZONS,
    rebound_event_types: Optional[set[str]] = None,
    rebound_drop_pct: float = 8.0,
    rebound_recovery_ratio: float = 0.5,
    rebound_search_days: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    horizons = tuple(sorted({int(h) for h in horizons if int(h) > 0}))
    rebound_event_types = rebound_event_types or {"SUPPLY_CONTRACT"}
    detail_rows: list[Dict[str, Any]] = []
    price_cache: Dict[str, Optional[pd.DataFrame]] = {}
    missing_price_symbols: set[str] = set()
    pending_price_records = 0

    if not records:
        empty = pd.DataFrame()
        return empty, empty, {"record_count": 0, "priced_count": 0, "missing_price_symbols": []}

    event_dates = [
        pd.Timestamp(str(record.get("event_date") or record.get("record_date") or "")).normalize()
        for record in records
        if record.get("event_date") or record.get("record_date")
    ]
    if not event_dates:
        empty = pd.DataFrame()
        return empty, empty, {"record_count": len(records), "priced_count": 0, "missing_price_symbols": []}

    start_date = (min(event_dates) - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = (max(event_dates) + pd.Timedelta(days=max(horizons, default=10) + rebound_search_days + 10)).strftime("%Y-%m-%d")
    sector_map = _load_event_sector_map(str(record.get("stock_code") or "").zfill(6) for record in records)

    for record in records:
        symbol = str(record.get("stock_code") or "").zfill(6)
        if not symbol:
            continue

        if symbol not in price_cache:
            price_cache[symbol] = load_price_history(symbol, start_date, end_date)
        price_df = price_cache[symbol]
        if price_df is None or price_df.empty:
            missing_price_symbols.add(symbol)
            continue

        event_date_raw = str(record.get("event_date") or record.get("record_date") or "")
        if not event_date_raw:
            continue
        event_date = pd.Timestamp(event_date_raw).normalize()
        event_time_hhmm = str(record.get("event_time_hhmm") or record.get("record_time") or "")
        event_idx = _event_row_index(price_df, event_date, event_time_hhmm)
        if event_idx is None:
            if event_time_hhmm and str(event_time_hhmm) >= "15:30":
                pending_price_records += 1
            continue

        base_row = {
            "event_date": event_date.strftime("%Y-%m-%d"),
            "event_time_hhmm": event_time_hhmm,
            "stock_code": symbol,
            "corp_name": record.get("corp_name") or symbol,
            "title": record.get("title") or "",
            "rcp_no": record.get("rcp_no") or "",
            "event_type": record.get("event_type") or "UNCLASSIFIED",
            "signal_bias": record.get("signal_bias") or "neutral",
            "sector": sector_map.get(symbol, "Unknown"),
            "reason": record.get("reason") or "",
            "strategy_name": record.get("strategy_name") or "observe_only",
            "trade_executed": bool(record.get("trade_executed", False)),
            "metrics": json.dumps(record.get("metrics") or {}, ensure_ascii=False),
        }

        immediate_entry_date = price_df.iloc[event_idx]["Date"].strftime("%Y-%m-%d")
        immediate_entry_px = _safe_float(price_df.iloc[event_idx]["Close"])
        immediate_row = dict(base_row)
        immediate_row.update(
            {
                "backtest_strategy": "immediate_close",
                "entry_date": immediate_entry_date,
                "entry_price": immediate_entry_px,
                "max_drawdown_5d": _max_drawdown_pct(price_df, event_idx, 5),
            }
        )
        immediate_row.update(_forward_return_map(price_df, event_idx, horizons))
        detail_rows.append(immediate_row)

        if (
            base_row["event_type"] in rebound_event_types
            and base_row["signal_bias"] == "positive"
        ):
            rebound = _find_rebound_entry(
                price_df,
                event_idx,
                drop_pct=rebound_drop_pct,
                recovery_ratio=rebound_recovery_ratio,
                search_days=rebound_search_days,
            )
            if rebound:
                rebound_row = dict(base_row)
                rebound_row.update(
                    {
                        "backtest_strategy": "drop_rebound_half",
                        "entry_date": rebound["entry_date"],
                        "entry_price": rebound["entry_price"],
                        "event_close": rebound["event_close"],
                        "trough_date": rebound["trough_date"],
                        "trough_low": rebound["trough_low"],
                        "rebound_trigger_level": rebound["recovery_level"],
                        "max_drawdown_5d": _max_drawdown_pct(price_df, rebound["entry_idx"], 5),
                    }
                )
                rebound_row.update(_forward_return_map(price_df, rebound["entry_idx"], horizons))
                detail_rows.append(rebound_row)

    detail_df = pd.DataFrame(detail_rows)
    if detail_df.empty:
        empty = pd.DataFrame()
        return empty, empty, {
            "record_count": len(records),
            "priced_count": 0,
            "missing_price_symbols": sorted(missing_price_symbols),
            "pending_price_records": pending_price_records,
        }

    summary_df = _aggregate_summary(detail_df, ["event_type", "backtest_strategy"])

    metadata = {
        "record_count": len(records),
        "priced_count": int(detail_df["stock_code"].nunique()),
        "missing_price_symbols": sorted(missing_price_symbols),
        "pending_price_records": pending_price_records,
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "horizons": list(horizons),
    }
    return detail_df, summary_df, metadata


def render_html_report(
    detail_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    metadata: Dict[str, Any],
    *,
    sector_summary_df: Optional[pd.DataFrame] = None,
) -> str:
    human_summary = _build_human_summary(summary_df, metadata)
    if sector_summary_df is None:
        sector_summary_df = build_sector_summary(detail_df)
    sector_human_summary = _build_sector_human_summary(sector_summary_df)

    summary_html = "<p>No summary rows.</p>"
    if not summary_df.empty:
        display_df = summary_df.copy()
        display_df = display_df.where(pd.notna(display_df), "-")
        summary_html = display_df.round(2).to_html(index=False)

    recent_html = "<p>No recent trades.</p>"
    if not detail_df.empty:
        recent_cols = [
            "event_date", "stock_code", "corp_name", "event_type",
            "backtest_strategy", "entry_date", "entry_price", "ret_3d", "ret_5d", "ret_10d",
        ]
        recent_html = (
            detail_df.sort_values(["event_date", "event_type"], ascending=[False, True])[recent_cols]
            .head(30)
            .round(2)
            .to_html(index=False)
        )

    missing_symbols = ", ".join(metadata.get("missing_price_symbols", [])[:20]) or "-"
    briefing_sections: list[str] = []
    if human_summary.get("overall_note"):
        briefing_sections.append(
            f"<p><strong>이번 리포트의 해석 가능 수준:</strong> {html.escape(str(human_summary['overall_note']))}</p>"
        )
    for title, key in [
        ("지금 참고할 조합", "focus_rows"),
        ("짧게만 보는 단기 반응형 조합", "short_term_rows"),
        ("하루 이틀 뒤 확인할 지연 반응형 조합", "delayed_rows"),
        ("방향성보다 존재 확인이 먼저인 공시", "neutral_rows"),
        ("아직 표본이 얕은 조합", "shallow_rows"),
        ("아직 5일 성과가 안 쌓인 조합", "hold_rows"),
        ("주의할 조합", "caution_rows"),
    ]:
        rows = human_summary.get(key) or []
        if not rows:
            continue
        items = "".join(f"<li>{html.escape(_format_human_row(row))}</li>" for row in rows)
        briefing_sections.append(f"<h2>{title}</h2><ul>{items}</ul>")
    if human_summary.get("pending_note"):
        briefing_sections.append(
            f"<p><strong>미가격 반영 / 대기 표본:</strong> {html.escape(str(human_summary['pending_note']))}</p>"
        )
    if sector_human_summary.get("overall_note"):
        briefing_sections.append(
            f"<p><strong>이벤트와 섹터를 함께 보면:</strong> {html.escape(str(sector_human_summary['overall_note']))}</p>"
        )
    for title, key in [
        ("같은 이벤트라도 섹터까지 맞는 조합", "focus_rows"),
        ("섹터까지 보면 더 보수적인 조합", "caution_rows"),
    ]:
        rows = sector_human_summary.get(key) or []
        if not rows:
            continue
        items = "".join(f"<li>{html.escape(_format_sector_human_row(row))}</li>" for row in rows)
        briefing_sections.append(f"<h2>{title}</h2><ul>{items}</ul>")
    briefing_html = "\n  ".join(briefing_sections) if briefing_sections else "<p>해석 가능한 요약이 아직 없습니다.</p>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Disclosure Event Backtest</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px; color: #111; }}
    h1, h2 {{ margin-bottom: 12px; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
    th, td {{ border: 1px solid #d7d7d7; padding: 8px 10px; font-size: 13px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    .meta {{ margin-bottom: 20px; line-height: 1.6; }}
  </style>
</head>
<body>
  <h1>Disclosure Event Backtest</h1>
  <div class="meta">
    <div><strong>Generated:</strong> {metadata.get("generated_at", "-")}</div>
    <div><strong>Records loaded:</strong> {metadata.get("record_count", 0)}</div>
    <div><strong>Priced symbols:</strong> {metadata.get("priced_count", 0)}</div>
    <div><strong>Missing price symbols:</strong> {missing_symbols}</div>
  </div>
  <h2>Briefing</h2>
  {briefing_html}
  <h2>Summary by Event Type / Strategy</h2>
  {summary_html}
  <h2>Recent Detailed Rows</h2>
  {recent_html}
</body>
</html>
"""


def build_slack_digest(
    summary_df: pd.DataFrame,
    metadata: Dict[str, Any],
    max_rows: int = 8,
    *,
    sector_summary_df: Optional[pd.DataFrame] = None,
) -> str:
    if summary_df.empty:
        lines = [
            "*Disclosure event backtest*\n"
            f"- records loaded: {metadata.get('record_count', 0)}\n"
            "- no valid price-backed rows yet"
        ]
        if metadata.get("pending_price_records"):
            lines.append(f"- pending future-price records: {metadata['pending_price_records']}")
        if metadata.get("missing_price_symbols"):
            lines.append(f"- missing price symbols: {len(metadata['missing_price_symbols'])}")
        return "\n".join(lines)

    human_summary = _build_human_summary(summary_df, metadata, max_rows=max_rows)
    sector_human_summary = _build_sector_human_summary(sector_summary_df if sector_summary_df is not None else pd.DataFrame(), max_rows=max_rows)
    lines = [
        "*Disclosure event backtest*",
        f"- records loaded: {metadata.get('record_count', 0)}",
        f"- priced symbols: {metadata.get('priced_count', 0)}",
    ]
    if metadata.get("missing_price_symbols"):
        lines.append(f"- missing price symbols: {len(metadata['missing_price_symbols'])}")
    if metadata.get("pending_price_records"):
        lines.append(f"- pending future-price records: {metadata['pending_price_records']}")

    lines.append("*이번 리포트의 해석 가능 수준*")
    lines.append(f"- {human_summary.get('overall_note') or '해석 가능한 조합이 아직 많지 않습니다.'}")

    sections = [
        ("*지금 참고할 조합*", human_summary.get("focus_rows") or []),
        ("*짧게만 보는 단기 반응형 조합*", human_summary.get("short_term_rows") or []),
        ("*하루 이틀 뒤 확인할 지연 반응형 조합*", human_summary.get("delayed_rows") or []),
        ("*방향성보다 존재 확인이 먼저인 공시*", human_summary.get("neutral_rows") or []),
        ("*아직 표본이 얕은 조합*", human_summary.get("shallow_rows") or []),
        ("*아직 5일 성과가 안 쌓인 조합*", human_summary.get("hold_rows") or []),
        ("*주의할 조합*", human_summary.get("caution_rows") or []),
    ]
    for title, rows in sections:
        if not rows:
            continue
        lines.append(title)
        for row in rows[:max_rows]:
            lines.append(f"- {_format_human_row(row)}")

    if human_summary.get("pending_note"):
        lines.append("*미가격 반영 / 대기 표본*")
        lines.append(f"- {human_summary['pending_note']}")

    if sector_human_summary.get("overall_note"):
        lines.append("*이벤트와 섹터를 함께 보면*")
        lines.append(f"- {sector_human_summary['overall_note']}")
    for title, rows in [
        ("*같은 이벤트라도 섹터까지 맞는 조합*", sector_human_summary.get("focus_rows") or []),
        ("*섹터까지 보면 더 보수적인 조합*", sector_human_summary.get("caution_rows") or []),
    ]:
        if not rows:
            continue
        lines.append(title)
        for row in rows[:max_rows]:
            lines.append(f"- {_format_sector_human_row(row)}")

    return "\n".join(lines)


def build_symbol_summary(detail_df: pd.DataFrame, top_n: int = 30) -> dict[str, Any]:
    if detail_df.empty:
        return {"snapshot_at": _now_kst().isoformat(timespec="seconds"), "top_symbols": []}

    work_df = detail_df.copy()
    work_df["stock_code"] = work_df["stock_code"].astype(str).str.zfill(6)
    work_df["event_date"] = pd.to_datetime(work_df.get("event_date"), errors="coerce")
    work_df["event_time_hhmm"] = work_df.get("event_time_hhmm", "").fillna("").astype(str)
    work_df["ret_1d"] = pd.to_numeric(work_df.get("ret_1d"), errors="coerce")
    work_df["ret_3d"] = pd.to_numeric(work_df.get("ret_3d"), errors="coerce")
    work_df["ret_5d"] = pd.to_numeric(work_df.get("ret_5d"), errors="coerce")
    work_df["ret_10d"] = pd.to_numeric(work_df.get("ret_10d"), errors="coerce")
    work_df["max_drawdown_5d"] = pd.to_numeric(work_df.get("max_drawdown_5d"), errors="coerce")

    rows: list[dict[str, Any]] = []
    for symbol, group in work_df.groupby("stock_code", dropna=False):
        if not symbol:
            continue
        ordered = group.sort_values(["event_date", "event_time_hhmm"], ascending=[False, False]).copy()
        latest = ordered.iloc[0]

        positive_count = int((ordered.get("signal_bias", pd.Series(dtype=str)) == "positive").sum())
        negative_count = int((ordered.get("signal_bias", pd.Series(dtype=str)) == "negative").sum())
        neutral_count = int((ordered.get("signal_bias", pd.Series(dtype=str)) == "neutral").sum())
        event_count = int(len(ordered))
        avg_ret_5d = float(ordered["ret_5d"].dropna().mean()) if ordered["ret_5d"].notna().any() else 0.0
        avg_ret_1d = float(ordered["ret_1d"].dropna().mean()) if ordered["ret_1d"].notna().any() else 0.0
        avg_mdd_5d = float(ordered["max_drawdown_5d"].dropna().mean()) if ordered["max_drawdown_5d"].notna().any() else 0.0
        priced_event_count = int(ordered["ret_5d"].notna().sum())
        event_signal_score = (
            avg_ret_5d * 0.35
            + avg_ret_1d * 0.20
            + positive_count * 0.70
            - negative_count * 0.70
            + min(3.0, math.log1p(event_count))
        )
        rows.append(
            {
                "symbol": symbol,
                "name": latest.get("corp_name") or symbol,
                "latest_event_date": latest.get("event_date").strftime("%Y-%m-%d") if pd.notna(latest.get("event_date")) else "",
                "latest_event_type": latest.get("event_type") or "",
                "latest_signal_bias": latest.get("signal_bias") or "",
                "latest_title": latest.get("title") or "",
                "event_count": event_count,
                "priced_event_count": priced_event_count,
                "positive_count": positive_count,
                "negative_count": negative_count,
                "neutral_count": neutral_count,
                "avg_ret_1d": round(avg_ret_1d, 4),
                "avg_ret_5d": round(avg_ret_5d, 4),
                "avg_mdd_5d": round(avg_mdd_5d, 4),
                "event_signal_score": round(event_signal_score, 4),
            }
        )

    rows.sort(
        key=lambda item: (
            item["event_signal_score"],
            item["priced_event_count"],
            item["event_count"],
        ),
        reverse=True,
    )
    return {
        "snapshot_at": _now_kst().isoformat(timespec="seconds"),
        "top_symbols": rows[:top_n],
    }


def save_backtest_outputs(
    detail_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    metadata: Dict[str, Any],
    *,
    label: Optional[str] = None,
    report_dir: str = EVENT_REPORT_DIR,
) -> Dict[str, str]:
    _ensure_dirs()
    label = label or _now_kst().strftime("%Y%m%d")
    symbol_summary = build_symbol_summary(detail_df)
    human_summary = _build_human_summary(summary_df, metadata)
    sector_summary_df = build_sector_summary(detail_df)
    sector_human_summary = _build_sector_human_summary(sector_summary_df)

    detail_csv = os.path.join(report_dir, f"disclosure_event_backtest_{label}.csv")
    summary_csv = os.path.join(report_dir, f"disclosure_event_summary_{label}.csv")
    summary_json = os.path.join(report_dir, f"disclosure_event_summary_{label}.json")
    sector_summary_csv = os.path.join(report_dir, f"disclosure_event_sector_summary_{label}.csv")
    sector_summary_json = os.path.join(report_dir, f"disclosure_event_sector_summary_{label}.json")
    symbol_csv = os.path.join(report_dir, f"disclosure_event_symbols_{label}.csv")
    symbol_json = os.path.join(report_dir, f"disclosure_event_symbols_{label}.json")
    report_html = os.path.join(report_dir, f"disclosure_event_report_{label}.html")

    if detail_df.empty:
        pd.DataFrame().to_csv(detail_csv, index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(summary_csv, index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(sector_summary_csv, index=False, encoding="utf-8-sig")
    else:
        detail_df.to_csv(detail_csv, index=False, encoding="utf-8-sig")
        summary_df.to_csv(summary_csv, index=False, encoding="utf-8-sig")
        sector_summary_df.to_csv(sector_summary_csv, index=False, encoding="utf-8-sig")

    with open(summary_json, "w", encoding="utf-8") as fp:
        summary_rows = [] if summary_df.empty else summary_df.where(pd.notna(summary_df), None).round(4).to_dict(orient="records")
        json.dump(
            {
                "metadata": metadata,
                "human_summary": human_summary,
                "summary_rows": summary_rows,
            },
            fp,
            ensure_ascii=False,
            indent=2,
        )

    with open(sector_summary_json, "w", encoding="utf-8") as fp:
        sector_summary_rows = [] if sector_summary_df.empty else sector_summary_df.where(pd.notna(sector_summary_df), None).round(4).to_dict(orient="records")
        json.dump(
            {
                "metadata": metadata,
                "human_summary": sector_human_summary,
                "sector_summary_rows": sector_summary_rows,
            },
            fp,
            ensure_ascii=False,
            indent=2,
        )

    symbol_df = pd.DataFrame(symbol_summary.get("top_symbols", []))
    symbol_df.to_csv(symbol_csv, index=False, encoding="utf-8-sig")
    with open(symbol_json, "w", encoding="utf-8") as fp:
        json.dump(symbol_summary, fp, ensure_ascii=False, indent=2)

    with open(report_html, "w", encoding="utf-8") as fp:
        fp.write(render_html_report(detail_df, summary_df, metadata, sector_summary_df=sector_summary_df))

    latest_map = {
        detail_csv: os.path.join(report_dir, "disclosure_event_backtest_latest.csv"),
        summary_csv: os.path.join(report_dir, "disclosure_event_summary_latest.csv"),
        summary_json: os.path.join(report_dir, "disclosure_event_summary_latest.json"),
        sector_summary_csv: os.path.join(report_dir, "disclosure_event_sector_summary_latest.csv"),
        sector_summary_json: os.path.join(report_dir, "disclosure_event_sector_summary_latest.json"),
        symbol_csv: os.path.join(report_dir, "disclosure_event_symbols_latest.csv"),
        symbol_json: os.path.join(report_dir, "disclosure_event_symbols_latest.json"),
        report_html: os.path.join(report_dir, "disclosure_event_report_latest.html"),
    }
    for src_path, latest_path in latest_map.items():
        try:
            with open(src_path, "rb") as src_fp, open(latest_path, "wb") as dst_fp:
                dst_fp.write(src_fp.read())
        except OSError as exc:
            log.warning("latest artifact copy failed (%s -> %s): %s", src_path, latest_path, exc)

    return {
        "detail_csv": detail_csv,
        "summary_csv": summary_csv,
        "summary_json": summary_json,
        "sector_summary_csv": sector_summary_csv,
        "sector_summary_json": sector_summary_json,
        "symbol_csv": symbol_csv,
        "symbol_json": symbol_json,
        "report_html": report_html,
        "latest_detail_csv": latest_map[detail_csv],
        "latest_summary_csv": latest_map[summary_csv],
        "latest_summary_json": latest_map[summary_json],
        "latest_sector_summary_csv": latest_map[sector_summary_csv],
        "latest_sector_summary_json": latest_map[sector_summary_json],
        "latest_symbol_csv": latest_map[symbol_csv],
        "latest_symbol_json": latest_map[symbol_json],
        "latest_report_html": latest_map[report_html],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a lightweight disclosure event backtest.")
    parser.add_argument("--days", type=int, default=30, help="How many event-log days to load.")
    parser.add_argument("--start-date", default="", help="Optional inclusive start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default="", help="Optional inclusive end date YYYY-MM-DD.")
    parser.add_argument("--drop-pct", type=float, default=8.0, help="Drop threshold for rebound entries.")
    parser.add_argument("--recovery-ratio", type=float, default=0.5, help="Fraction of drawdown that must be recovered.")
    parser.add_argument("--rebound-search-days", type=int, default=10, help="Max days to wait for drop-and-rebound entry.")
    parser.add_argument("--print-only", action="store_true", help="Print digest locally.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    records = load_event_records(
        days=args.days,
        start_date=args.start_date or None,
        end_date=args.end_date or None,
    )
    detail_df, summary_df, metadata = build_backtest_frames(
        records,
        rebound_drop_pct=args.drop_pct,
        rebound_recovery_ratio=args.recovery_ratio,
        rebound_search_days=args.rebound_search_days,
    )
    paths = save_backtest_outputs(detail_df, summary_df, metadata)
    digest = build_slack_digest(summary_df, metadata, sector_summary_df=build_sector_summary(detail_df))
    print(digest)
    print(json.dumps(paths, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
