from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

try:
    from analyst_report_terms import BEARISH_TERMS, BULLISH_TERMS, CONFIDENCE_TERMS, HESITATION_TERMS
except Exception:
    from Disclosure.analyst_report_terms import BEARISH_TERMS, BULLISH_TERMS, CONFIDENCE_TERMS, HESITATION_TERMS


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "-", "N/A"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _count_terms(text: str, terms: list[str]) -> int:
    lowered = f" {str(text or '').lower()} "
    return sum(1 for term in terms if term and term.lower() in lowered)


def score_text(text: str) -> dict[str, float]:
    bullish = _count_terms(text, BULLISH_TERMS["kr"] + BULLISH_TERMS["en"])
    bearish = _count_terms(text, BEARISH_TERMS["kr"] + BEARISH_TERMS["en"])
    confidence = _count_terms(text, CONFIDENCE_TERMS["kr"] + CONFIDENCE_TERMS["en"])
    hesitation = _count_terms(text, HESITATION_TERMS["kr"] + HESITATION_TERMS["en"])
    return {
        "sentiment": float(bullish - bearish),
        "confidence": float(confidence - hesitation),
        "uncertainty": float(hesitation),
    }


def rating_delta_score(rating: str, prev_rating: str) -> float:
    rating_text = str(rating or "").lower()
    prev_text = str(prev_rating or "").lower()
    positive = {"buy", "strong buy", "outperform", "overweight", "매수", "비중확대"}
    negative = {"sell", "underperform", "underweight", "reduce", "매도", "비중축소"}
    neutral = {"hold", "neutral", "중립"}

    def _bucket(value: str) -> int:
        if value in positive:
            return 1
        if value in negative:
            return -1
        if value in neutral:
            return 0
        return 0

    return float(_bucket(rating_text) - _bucket(prev_text))


def apply_broker_bias_adjustment(
    df: pd.DataFrame,
    score_col: str = "report_sentiment_raw",
    label_col: str = "realized_alpha_score",
    shrinkage: float = 12.0,
    span: int = 24,
    alpha_scale: float = 4.0,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    if "published_at" not in out.columns:
        out["published_at"] = pd.NaT
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce", utc=True)
    out[label_col] = pd.to_numeric(out.get(label_col), errors="coerce")
    out = out.sort_values(["broker", "published_at", "symbol"]).reset_index(drop=True)

    parts: list[pd.DataFrame] = []
    for _, group in out.groupby("broker", dropna=False, sort=False):
        shifted = pd.to_numeric(group[label_col], errors="coerce").shift(1)
        parts.append(
            pd.DataFrame(
                {
                    "broker_report_count": shifted.notna().cumsum(),
                    "broker_rolling_alpha_mean": shifted.ewm(
                        span=max(4, int(span)),
                        min_periods=1,
                        adjust=False,
                    ).mean(),
                    "broker_rolling_alpha_std": shifted.ewm(
                        span=max(4, int(span)),
                        min_periods=2,
                        adjust=False,
                    ).std(bias=False),
                },
                index=group.index,
            )
        )
    stats = pd.concat(parts).sort_index() if parts else pd.DataFrame(index=out.index)
    out = out.join(stats)
    out["broker_report_count"] = out["broker_report_count"].fillna(0).astype(int)
    out["broker_rolling_alpha_mean"] = out["broker_rolling_alpha_mean"].fillna(0.0)
    out["broker_rolling_alpha_std"] = out["broker_rolling_alpha_std"].fillna(0.0)
    out["broker_bias_weight"] = out["broker_report_count"].astype(float) / (
        out["broker_report_count"].astype(float) + float(max(1.0, shrinkage))
    )
    scale_floor = max(1.0, float(alpha_scale))
    scale = out["broker_rolling_alpha_std"].where(out["broker_rolling_alpha_std"].gt(0), scale_floor).clip(lower=scale_floor)
    out["broker_alpha_signal"] = out["broker_rolling_alpha_mean"].fillna(0.0).div(scale)
    out["broker_bias_adjustment"] = (
        np.tanh(out["broker_alpha_signal"]).astype(float) * 2.5 * out["broker_bias_weight"].fillna(0.0)
    ).clip(lower=-2.5, upper=2.5)
    out["broker_bias_mean"] = out["broker_rolling_alpha_mean"]
    out["broker_bias_std"] = out["broker_rolling_alpha_std"]
    out["report_sentiment_bias_adj"] = out[score_col].fillna(0.0) + out["broker_bias_adjustment"].fillna(0.0)
    return out


def _tokenize(text: str) -> set[str]:
    return {token for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", str(text or "").lower()) if token}


def _novelty_score(current_text: str, previous_text: str) -> float:
    current_tokens = _tokenize(current_text)
    previous_tokens = _tokenize(previous_text)
    if not current_tokens and not previous_tokens:
        return 0.0
    if current_tokens and not previous_tokens:
        return 1.0
    union = current_tokens | previous_tokens
    if not union:
        return 0.0
    overlap = len(current_tokens & previous_tokens) / len(union)
    return float(1.0 - overlap)


def apply_novelty_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    body_col = "content_full" if "content_full" in out.columns else "content"
    out["combined_text"] = (out["title"].fillna("") + " " + out[body_col].fillna("")).str.strip()
    out = out.sort_values(["symbol", "broker", "published_at"]).reset_index(drop=True)
    out["prev_combined_text_broker"] = out.groupby(["symbol", "broker"], dropna=False)["combined_text"].shift(1).fillna("")
    out["prev_combined_text_symbol"] = out.groupby(["symbol"], dropna=False)["combined_text"].shift(1).fillna("")
    out["novelty_broker"] = [_novelty_score(cur, prev) for cur, prev in zip(out["combined_text"], out["prev_combined_text_broker"])]
    out["novelty_symbol"] = [_novelty_score(cur, prev) for cur, prev in zip(out["combined_text"], out["prev_combined_text_symbol"])]
    out["rating_changed_flag"] = (
        out["prev_rating"].fillna("").astype(str).str.strip().ne("")
        & out["rating"].fillna("").astype(str).str.lower().ne(out["prev_rating"].fillna("").astype(str).str.lower())
    ).astype(float)
    out["target_changed_flag"] = out["target_revision_pct"].apply(_safe_float).fillna(0.0).abs().gt(0).astype(float)
    out["novelty_score"] = (
        out["novelty_broker"] * 0.55
        + out["novelty_symbol"] * 0.25
        + out["rating_changed_flag"] * 0.10
        + out["target_changed_flag"] * 0.10
    ).clip(lower=0.0, upper=1.0)
    pdf_bonus = np.log1p(pd.to_numeric(out.get("pdf_text_length", 0), errors="coerce").fillna(0.0)).div(20.0).clip(0.0, 0.18)
    out["report_sentiment_prelabel"] = out["report_sentiment_bias_adj"].fillna(0.0) * (
        0.72 + out["novelty_score"] * 0.45 + pdf_bonus
    )
    return out
