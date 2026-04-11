from __future__ import annotations

import os

import numpy as np
import pandas as pd

try:
    from disclosure_event_pipeline import EVENT_REPORT_DIR, load_event_records
except Exception:
    from Disclosure.disclosure_event_pipeline import EVENT_REPORT_DIR, load_event_records

try:
    from context_alignment import load_latest_symbol_sector_map
except Exception:
    from Disclosure.context_alignment import load_latest_symbol_sector_map


SUMMARY_PATH = os.path.join(EVENT_REPORT_DIR, "disclosure_event_summary_latest.csv")
SECTOR_SUMMARY_PATH = os.path.join(EVENT_REPORT_DIR, "disclosure_event_sector_summary_latest.csv")


def _event_timestamp(record: dict) -> pd.Timestamp | pd.NaT:
    date_text = str(record.get("event_date") or "").strip()
    time_text = str(record.get("event_time_hhmm") or "00:00").strip()
    if not date_text:
        return pd.NaT
    if len(time_text) == 4 and time_text.isdigit():
        time_text = f"{time_text[:2]}:{time_text[2:]}"
    stamp = pd.to_datetime(f"{date_text} {time_text}", errors="coerce")
    return stamp if not pd.isna(stamp) else pd.NaT


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        return int(float(value))
    except Exception:
        return default


def _compute_strategy_edge(ranked: pd.DataFrame) -> pd.Series:
    return (
        ranked["avg_ret_5d"].fillna(0.0) * 0.60
        + ((ranked["win_rate_5d"].fillna(50.0) - 50.0) / 10.0) * 0.25
        - ranked["avg_mdd_5d"].fillna(0.0).abs() * 0.15
        + np.log1p(ranked["sample_size"].fillna(0.0)) * 0.15
    )


def _reaction_profile(avg_ret_1d: float, avg_ret_3d: float, avg_ret_5d: float, valid_sample_size: int) -> str:
    if valid_sample_size <= 0:
        return "성과 대기"
    if avg_ret_1d > 0 and avg_ret_3d > 0 and avg_ret_5d > 0:
        return "초기 반응과 유지력"
    if avg_ret_1d > 0 and (avg_ret_3d <= 0 or avg_ret_5d <= 0):
        return "초기 반응 후 둔화"
    if avg_ret_1d <= 0 and (avg_ret_3d > 0 or avg_ret_5d > 0):
        return "시간차 반응"
    return "반응 약함"


def _tactical_fields(
    interpretation_label: str,
    reaction_profile: str,
    avg_ret_1d: float,
    avg_ret_3d: float,
    avg_ret_5d: float,
) -> dict[str, str]:
    if interpretation_label == "참고 가능":
        return {
            "tactical_label": "참고 가능",
            "tactical_note": "평균 성과와 승률이 함께 받쳐주는 기본 참고 재료입니다.",
        }
    if interpretation_label == "표본 얕음":
        if reaction_profile == "초기 반응 후 둔화" and max(avg_ret_1d, avg_ret_3d) > 0:
            return {
                "tactical_label": "단기 반응형",
                "tactical_note": "초기 반응은 있었지만 오래 끌기보다 짧게만 보는 편이 낫습니다.",
            }
        if reaction_profile == "시간차 반응" and max(avg_ret_3d, avg_ret_5d) > 0:
            return {
                "tactical_label": "지연 반응형",
                "tactical_note": "당일보다 하루 이틀 뒤 반응을 확인하는 쪽이 더 어울립니다.",
            }
        if max(avg_ret_1d, avg_ret_3d, avg_ret_5d) <= 0:
            return {
                "tactical_label": "보수적 관찰",
                "tactical_note": "현재 표본만 보면 초기와 5일 반응이 모두 약해 존재 여부만 보는 편이 낫습니다.",
            }
        return {
            "tactical_label": "표본 얕음",
            "tactical_note": "재료는 보이지만 아직 단독 판단 근거로 쓰기에는 표본이 적습니다.",
        }
    if interpretation_label == "변동성 주의":
        return {
            "tactical_label": "변동성 주의",
            "tactical_note": "중간 흔들림 관리가 더 중요해 눌림 확인이 먼저입니다.",
        }
    if interpretation_label == "보수적":
        return {
            "tactical_label": "보수적",
            "tactical_note": "과거 평균 기준으로는 단독 재료 해석이 조심스러운 편입니다.",
        }
    if reaction_profile == "성과 대기":
        return {
            "tactical_label": "성과 대기",
            "tactical_note": "아직 5일 성과가 붙지 않아 기록 축적이 먼저입니다.",
        }
    return {
        "tactical_label": "해석 보류",
        "tactical_note": "숫자가 더 쌓이기 전까지는 존재 여부만 체크하는 편이 낫습니다.",
    }


def _sector_context_weight(valid_sample_size: int, coverage_pct: float) -> float:
    if valid_sample_size < 3 or coverage_pct < 40.0:
        return 0.0
    sample_factor = min(1.0, valid_sample_size / 8.0)
    coverage_factor = min(1.0, coverage_pct / 100.0)
    return round(min(0.45, sample_factor * coverage_factor * 0.45), 4)


def _load_event_alpha_map() -> dict[str, dict[str, float]]:
    if not os.path.exists(SUMMARY_PATH):
        return {}
    try:
        summary_df = pd.read_csv(SUMMARY_PATH)
    except Exception:
        return {}
    if summary_df.empty:
        return {}

    out: dict[str, dict[str, float]] = {}
    for event_type, group in summary_df.groupby("event_type", dropna=False):
        if not event_type:
            continue
        ranked = group.copy()
        for col in [
            "avg_ret_1d",
            "avg_ret_3d",
            "avg_ret_5d",
            "win_rate_5d",
            "avg_mdd_5d",
            "sample_size",
            "valid_ret_5d_count",
            "price_coverage_pct",
            "interpretable_score",
        ]:
            ranked[col] = pd.to_numeric(ranked.get(col), errors="coerce")
        ranked["strategy_edge_score"] = _compute_strategy_edge(ranked)
        fallback_valid = pd.Series(
            np.where(ranked["avg_ret_5d"].notna(), ranked["sample_size"], 0),
            index=ranked.index,
            dtype=float,
        )
        ranked["valid_ret_5d_count"] = ranked["valid_ret_5d_count"].fillna(fallback_valid)
        fallback_coverage = pd.Series(
            np.where(
                ranked["sample_size"].fillna(0.0) > 0,
                ranked["valid_ret_5d_count"].fillna(0.0) / ranked["sample_size"].fillna(1.0) * 100.0,
                0.0,
            ),
            index=ranked.index,
            dtype=float,
        )
        ranked["price_coverage_pct"] = ranked["price_coverage_pct"].fillna(fallback_coverage)
        coverage_factor = np.minimum(1.0, ranked["valid_ret_5d_count"].fillna(0.0) / 8.0) * np.minimum(
            1.0, ranked["price_coverage_pct"].fillna(0.0) / 100.0
        )
        ranked["interpretable_score"] = ranked["interpretable_score"].fillna(ranked["strategy_edge_score"] * coverage_factor)
        ranked["interpretation_label"] = ranked.get("interpretation_label", pd.Series(dtype=object)).fillna("")
        ranked["confidence_label"] = ranked.get("confidence_label", pd.Series(dtype=object)).fillna("")
        ranked["interpretation_note"] = ranked.get("interpretation_note", pd.Series(dtype=object)).fillna("")

        best = ranked.sort_values(
            ["interpretable_score", "valid_ret_5d_count", "sample_size"],
            ascending=[False, False, False],
        ).iloc[0]
        valid_sample_size = _safe_int(best.get("valid_ret_5d_count"), 0)
        coverage_pct = _safe_float(best.get("price_coverage_pct"), 0.0)
        avg_ret_1d = _safe_float(best.get("avg_ret_1d"), 0.0)
        avg_ret_3d = _safe_float(best.get("avg_ret_3d"), 0.0)
        avg_ret_5d = _safe_float(best.get("avg_ret_5d"), 0.0)
        raw_edge_score = _safe_float(best.get("strategy_edge_score"), 0.0)
        event_edge_score = raw_edge_score
        if valid_sample_size < 3 or coverage_pct < 40.0:
            event_edge_score *= 0.15
        reaction = _reaction_profile(avg_ret_1d, avg_ret_3d, avg_ret_5d, valid_sample_size)
        tactical = _tactical_fields(
            str(best.get("interpretation_label") or ""),
            reaction,
            avg_ret_1d,
            avg_ret_3d,
            avg_ret_5d,
        )
        out[str(event_type)] = {
            "best_strategy": str(best.get("backtest_strategy") or "immediate_close"),
            "expected_alpha_1d": avg_ret_1d,
            "expected_alpha_3d": avg_ret_3d,
            "expected_alpha_5d": avg_ret_5d,
            "event_edge_score": round(float(event_edge_score), 4),
            "raw_event_edge_score": round(float(raw_edge_score), 4),
            "interpretable_score": round(_safe_float(best.get("interpretable_score"), 0.0), 4),
            "win_rate_5d": _safe_float(best.get("win_rate_5d"), np.nan),
            "sample_size": _safe_int(best.get("sample_size"), 0),
            "valid_sample_size": valid_sample_size,
            "price_coverage_pct": round(coverage_pct, 2),
            "confidence_label": str(best.get("confidence_label") or ""),
            "interpretation_label": str(best.get("interpretation_label") or ""),
            "interpretation_note": str(best.get("interpretation_note") or ""),
            "tactical_label": tactical["tactical_label"],
            "tactical_note": tactical["tactical_note"],
            "reaction_profile": reaction,
        }
    return out


def _load_sector_event_alpha_map() -> dict[tuple[str, str], dict[str, float]]:
    if not os.path.exists(SECTOR_SUMMARY_PATH):
        return {}
    try:
        summary_df = pd.read_csv(SECTOR_SUMMARY_PATH)
    except Exception:
        return {}
    if summary_df.empty:
        return {}

    out: dict[tuple[str, str], dict[str, float]] = {}
    for _, row in summary_df.iterrows():
        event_type = str(row.get("event_type") or "").strip()
        sector = str(row.get("sector") or "").strip()
        if not event_type or not sector or sector == "Unknown":
            continue
        key = (event_type, sector)
        current = out.get(key)
        candidate = {
            "sector_valid_sample_size": _safe_int(row.get("valid_ret_5d_count"), 0),
            "sector_price_coverage_pct": round(_safe_float(row.get("price_coverage_pct"), 0.0), 2),
            "sector_avg_ret_1d": _safe_float(row.get("avg_ret_1d"), 0.0),
            "sector_avg_ret_3d": _safe_float(row.get("avg_ret_3d"), 0.0),
            "sector_avg_ret_5d": _safe_float(row.get("avg_ret_5d"), 0.0),
            "sector_avg_mdd_5d": _safe_float(row.get("avg_mdd_5d"), 0.0),
            "sector_interpretation_label": str(row.get("interpretation_label") or ""),
            "sector_tactical_label": str(row.get("tactical_label") or ""),
            "sector_interpretation_note": str(row.get("interpretation_note") or ""),
            "sector_tactical_note": str(row.get("tactical_note") or ""),
            "sector_reaction_profile": str(row.get("reaction_profile") or ""),
            "sector_interpretable_score": round(_safe_float(row.get("interpretable_score"), 0.0), 4),
        }
        if current is None or (
            candidate["sector_interpretable_score"],
            candidate["sector_valid_sample_size"],
        ) > (
            current.get("sector_interpretable_score", 0.0),
            current.get("sector_valid_sample_size", 0),
        ):
            out[key] = candidate
    return out


def build_event_alpha_frame(days: int = 45) -> pd.DataFrame:
    columns = [
        "symbol",
        "event_alpha_score",
        "event_expected_alpha_1d",
        "event_expected_alpha_3d",
        "event_expected_alpha_5d",
        "event_recent_count",
        "event_recent_positive_count",
        "event_recent_negative_count",
        "event_last_type",
        "event_last_bias",
        "event_last_days_ago",
        "event_best_strategy",
        "event_backtest_confidence",
        "event_valid_sample_size",
        "event_price_coverage_pct",
        "event_interpretation_label",
        "event_interpretation_note",
        "event_tactical_label",
        "event_tactical_note",
        "event_sector",
        "event_sector_valid_sample_size",
        "event_sector_price_coverage_pct",
        "event_sector_interpretation_label",
        "event_sector_tactical_label",
        "event_sector_interpretation_note",
        "event_sector_tactical_note",
        "event_sector_reaction_profile",
        "event_sector_interpretable_score",
        "event_interpretable_score",
        "event_win_rate_5d",
        "event_sample_size",
        "event_reaction_profile",
    ]
    records = load_event_records(days=days)
    if not records:
        return pd.DataFrame(columns=columns)

    alpha_map = _load_event_alpha_map()
    sector_alpha_map = _load_sector_event_alpha_map()
    symbol_sector_map = load_latest_symbol_sector_map()
    now = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None)
    rows: list[dict[str, object]] = []

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=columns)
    df["symbol"] = (
        df.get("stock_code", df.get("symbol", pd.Series(dtype=object)))
        .fillna("")
        .astype(str)
        .str.zfill(6)
    )
    df["event_type"] = df.get("event_type", pd.Series(dtype=object)).fillna("UNCLASSIFIED").astype(str)
    df["signal_bias"] = df.get("signal_bias", pd.Series(dtype=object)).fillna("neutral").astype(str)
    df["event_ts"] = [
        _event_timestamp(record)
        for record in df.to_dict("records")
    ]
    df = df.dropna(subset=["symbol", "event_ts"]).copy()
    if df.empty:
        return pd.DataFrame(columns=columns)

    for symbol, group in df.groupby("symbol", dropna=False):
        group = group.sort_values("event_ts")
        age_days = (now - pd.to_datetime(group["event_ts"], errors="coerce")).dt.total_seconds().div(86400.0).clip(lower=0.0)
        weights = np.power(0.5, age_days / 10.0)
        expected_alpha = []
        expected_alpha_1d = []
        expected_alpha_3d = []
        signed_edges = []
        for _, row in group.iterrows():
            alpha_info = alpha_map.get(str(row["event_type"]) or "", {})
            exp_alpha_1d = float(alpha_info.get("expected_alpha_1d", 0.0) or 0.0)
            exp_alpha_3d = float(alpha_info.get("expected_alpha_3d", 0.0) or 0.0)
            exp_alpha = float(alpha_info.get("expected_alpha_5d", 0.0) or 0.0)
            edge = float(alpha_info.get("event_edge_score", 0.0) or 0.0)
            bias = str(row.get("signal_bias") or "neutral").lower()
            sign = 1.0 if bias == "positive" else (-1.0 if bias == "negative" else 0.0)
            expected_alpha_1d.append(exp_alpha_1d * sign)
            expected_alpha_3d.append(exp_alpha_3d * sign)
            expected_alpha.append(exp_alpha * sign)
            signed_edges.append(edge * sign)

        expected_alpha_1d_series = pd.Series(expected_alpha_1d, index=group.index, dtype=float)
        expected_alpha_3d_series = pd.Series(expected_alpha_3d, index=group.index, dtype=float)
        expected_alpha_series = pd.Series(expected_alpha, index=group.index, dtype=float)
        signed_edge_series = pd.Series(signed_edges, index=group.index, dtype=float)
        last_row = group.iloc[-1]
        last_alpha_info = alpha_map.get(str(last_row.get("event_type") or ""), {})
        last_bias = str(last_row.get("signal_bias") or "neutral").lower()
        current_sector = str(symbol_sector_map.get(str(symbol).zfill(6)) or "")
        sector_alpha_info = sector_alpha_map.get((str(last_row.get("event_type") or ""), current_sector), {})
        tactical_label = str(last_alpha_info.get("tactical_label") or "")
        tactical_note = str(last_alpha_info.get("tactical_note") or "")
        sector_weight = _sector_context_weight(
            _safe_int(sector_alpha_info.get("sector_valid_sample_size"), 0),
            _safe_float(sector_alpha_info.get("sector_price_coverage_pct"), 0.0),
        )
        event_alpha_score = float(np.average(signed_edge_series.fillna(0.0), weights=weights))
        event_expected_alpha_1d = float(np.average(expected_alpha_1d_series.fillna(0.0), weights=weights))
        event_expected_alpha_3d = float(np.average(expected_alpha_3d_series.fillna(0.0), weights=weights))
        event_expected_alpha_5d = float(np.average(expected_alpha_series.fillna(0.0), weights=weights))
        event_interpretable_score = _safe_float(last_alpha_info.get("interpretable_score"), 0.0)
        if sector_weight > 0:
            last_sign = 1.0 if last_bias == "positive" else (-1.0 if last_bias == "negative" else 0.0)
            sector_interpretable = _safe_float(sector_alpha_info.get("sector_interpretable_score"), 0.0)
            event_interpretable_score = (event_interpretable_score * (1.0 - sector_weight)) + (sector_interpretable * sector_weight)
            if last_sign != 0:
                event_alpha_score = (event_alpha_score * (1.0 - sector_weight)) + (last_sign * sector_interpretable * sector_weight)
                event_expected_alpha_1d = (event_expected_alpha_1d * (1.0 - sector_weight)) + (
                    last_sign * _safe_float(sector_alpha_info.get("sector_avg_ret_1d"), 0.0) * sector_weight
                )
                event_expected_alpha_3d = (event_expected_alpha_3d * (1.0 - sector_weight)) + (
                    last_sign * _safe_float(sector_alpha_info.get("sector_avg_ret_3d"), 0.0) * sector_weight
                )
                event_expected_alpha_5d = (event_expected_alpha_5d * (1.0 - sector_weight)) + (
                    last_sign * _safe_float(sector_alpha_info.get("sector_avg_ret_5d"), 0.0) * sector_weight
                )
        if last_bias == "neutral":
            tactical_label = "존재 확인"
            tactical_note = "방향성이 약한 공시라 단독 매수 재료보다 존재 여부만 확인하는 편이 낫습니다."
        elif last_bias == "negative" and tactical_label in {"참고 가능", "단기 반응형", "지연 반응형"}:
            tactical_label = "보수적"
            tactical_note = "과거 평균 통계가 일부 괜찮아 보여도 현재 공시 방향은 부정 쪽이라 보수적으로 읽는 편이 낫습니다."
        elif sector_weight > 0 and str(sector_alpha_info.get("sector_tactical_label") or ""):
            tactical_note = (
                tactical_note + " "
                + f"같은 `{current_sector}` 섹터 기준으로는 `{sector_alpha_info.get('sector_tactical_label')}` 쪽 해석이 더 가깝습니다."
            ).strip()
        rows.append(
            {
                "symbol": str(symbol).zfill(6),
                "event_alpha_score": round(event_alpha_score, 4),
                "event_expected_alpha_1d": round(event_expected_alpha_1d, 4),
                "event_expected_alpha_3d": round(event_expected_alpha_3d, 4),
                "event_expected_alpha_5d": round(event_expected_alpha_5d, 4),
                "event_recent_count": int(len(group)),
                "event_recent_positive_count": int(group["signal_bias"].astype(str).str.lower().eq("positive").sum()),
                "event_recent_negative_count": int(group["signal_bias"].astype(str).str.lower().eq("negative").sum()),
                "event_last_type": str(last_row.get("event_type") or "UNCLASSIFIED"),
                "event_last_bias": str(last_row.get("signal_bias") or "neutral"),
                "event_last_days_ago": round(float(age_days.iloc[-1]), 2) if not age_days.empty else None,
                "event_best_strategy": str(last_alpha_info.get("best_strategy") or ""),
                "event_backtest_confidence": str(last_alpha_info.get("confidence_label") or ""),
                "event_valid_sample_size": _safe_int(last_alpha_info.get("valid_sample_size"), 0),
                "event_price_coverage_pct": round(_safe_float(last_alpha_info.get("price_coverage_pct"), 0.0), 2),
                "event_interpretation_label": str(last_alpha_info.get("interpretation_label") or ""),
                "event_interpretation_note": str(last_alpha_info.get("interpretation_note") or ""),
                "event_tactical_label": tactical_label,
                "event_tactical_note": tactical_note,
                "event_sector": current_sector,
                "event_sector_valid_sample_size": _safe_int(sector_alpha_info.get("sector_valid_sample_size"), 0),
                "event_sector_price_coverage_pct": round(_safe_float(sector_alpha_info.get("sector_price_coverage_pct"), 0.0), 2),
                "event_sector_interpretation_label": str(sector_alpha_info.get("sector_interpretation_label") or ""),
                "event_sector_tactical_label": str(sector_alpha_info.get("sector_tactical_label") or ""),
                "event_sector_interpretation_note": str(sector_alpha_info.get("sector_interpretation_note") or ""),
                "event_sector_tactical_note": str(sector_alpha_info.get("sector_tactical_note") or ""),
                "event_sector_reaction_profile": str(sector_alpha_info.get("sector_reaction_profile") or ""),
                "event_sector_interpretable_score": round(_safe_float(sector_alpha_info.get("sector_interpretable_score"), 0.0), 4),
                "event_interpretable_score": round(event_interpretable_score, 4),
                "event_win_rate_5d": _safe_float(last_alpha_info.get("win_rate_5d"), np.nan),
                "event_sample_size": _safe_int(last_alpha_info.get("sample_size"), 0),
                "event_reaction_profile": str(last_alpha_info.get("reaction_profile") or ""),
            }
        )
    return pd.DataFrame(rows, columns=columns)
