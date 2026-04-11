from __future__ import annotations

import glob
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Optional

import numpy as np
import pandas as pd

try:
    from analyst_pdf_parser import enrich_reports_with_pdf_text
    from analyst_report_features import enrich_prev_fields
    from analyst_report_labeling import apply_return_calibration, attach_forward_return_labels
    from analyst_report_scoring import apply_broker_bias_adjustment, apply_novelty_features, rating_delta_score, score_text
except Exception:
    from Disclosure.analyst_pdf_parser import enrich_reports_with_pdf_text
    from Disclosure.analyst_report_features import enrich_prev_fields
    from Disclosure.analyst_report_labeling import apply_return_calibration, attach_forward_return_labels
    from Disclosure.analyst_report_scoring import apply_broker_bias_adjustment, apply_novelty_features, rating_delta_score, score_text


log = logging.getLogger("disclosure.analyst_reports")
ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyst_reports")
RAW_DIR = os.path.join(ROOT_DIR, "raw")
SUMMARY_DIR = os.path.join(ROOT_DIR, "summaries")


def _ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(SUMMARY_DIR, exist_ok=True)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "-", "N/A"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _annotate_parse_quality(scored: pd.DataFrame) -> pd.DataFrame:
    out = scored.copy()
    out["content_length"] = out["content"].fillna("").astype(str).str.len()
    out["content_full_length"] = out["content_full"].fillna("").astype(str).str.len()
    out["pdf_text_length"] = pd.to_numeric(out.get("pdf_text_length", 0), errors="coerce").fillna(0.0)
    title_ok = out["title"].fillna("").astype(str).str.len().ge(4).astype(float)
    broker_ok = out["broker"].fillna("").astype(str).str.len().ge(2).astype(float)
    target_ok = (out["target_price"].isna() | out["target_price"].gt(0)).astype(float)
    body_score = out["content_full_length"].clip(lower=0).div(1400.0).clip(upper=1.0)
    pdf_score = out["pdf_text_length"].clip(lower=0).div(2500.0).clip(upper=1.0)
    parse_score = title_ok * 0.20 + broker_ok * 0.10 + target_ok * 0.20 + body_score * 0.35 + pdf_score * 0.15
    out["report_parse_quality_score"] = parse_score.round(4)
    out["report_parse_quality_status"] = np.select(
        [parse_score >= 0.85, parse_score >= 0.60, parse_score > 0.0],
        ["rich", "ok", "thin"],
        default="broken",
    )
    return out


def _raw_report_paths(days: int) -> list[str]:
    _ensure_dirs()
    min_stamp = (pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=max(1, int(days)))).strftime("%Y%m%d")
    paths: list[str] = []
    for path in sorted(glob.glob(os.path.join(RAW_DIR, "*"))):
        base = os.path.basename(path)
        stamp_match = next(iter(reversed(re.findall(r"(\d{8})", base))), "")
        if stamp_match and stamp_match < min_stamp:
            continue
        paths.append(path)
    return paths


def load_raw_reports(days: int = 90) -> pd.DataFrame:
    min_date = pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=max(1, int(days)))
    rows: list[dict] = []
    for path in _raw_report_paths(days):
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        elif path.endswith(".csv"):
            rows.extend(pd.read_csv(path).to_dict("records"))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "published_at" in df.columns:
        published = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
        min_date_utc = min_date.tz_convert("UTC")
        df["published_at"] = published.dt.tz_convert("Asia/Seoul")
        df = df[published.fillna(min_date_utc) >= min_date_utc].copy()
    return df.reset_index(drop=True)


def load_scored_reports_cache(days: int = 90, require_fresh: bool = True, require_pdf: bool = False) -> pd.DataFrame:
    _ensure_dirs()
    path = os.path.join(SUMMARY_DIR, "analyst_report_scored_latest.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    if require_fresh:
        raw_paths = _raw_report_paths(days)
        latest_raw_mtime = max((os.path.getmtime(raw_path) for raw_path in raw_paths), default=0.0)
        if latest_raw_mtime > os.path.getmtime(path):
            return pd.DataFrame()
    try:
        scored = pd.read_csv(path, dtype={"symbol": str})
    except Exception:
        return pd.DataFrame()
    if require_pdf:
        pdf_lengths = pd.to_numeric(scored.get("pdf_text_length"), errors="coerce")
        if pdf_lengths is None or int(pdf_lengths.fillna(0).gt(0).sum()) == 0:
            return pd.DataFrame()
    if "published_at" in scored.columns:
        published = pd.to_datetime(scored["published_at"], errors="coerce", utc=True)
        min_date = pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=max(1, int(days)))
        scored = scored[published.fillna(min_date.tz_convert("UTC")) >= min_date.tz_convert("UTC")].copy()
    return scored.reset_index(drop=True)


def score_reports(df: pd.DataFrame, attach_labels: bool = True, use_pdf_text: bool = True) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    scored = df.copy()
    defaults = [("symbol", ""), ("title", ""), ("content", ""), ("sector", "Unknown"), ("broker", "Unknown"), ("rating", ""), ("prev_rating", ""), ("pdf_url", "")]
    for col, default in defaults:
        if col not in scored.columns:
            scored[col] = default
        scored[col] = scored[col].fillna(default)
    scored["symbol"] = scored["symbol"].astype(str).str.zfill(6)
    scored["sector"] = scored["sector"].astype(str).replace("", "Unknown")
    for col in ["target_price", "prev_target_price"]:
        if col not in scored.columns:
            scored[col] = np.nan
        scored[col] = scored[col].apply(_safe_float)
        scored.loc[pd.to_numeric(scored[col], errors="coerce").le(0), col] = np.nan
    scored = enrich_prev_fields(scored)
    if use_pdf_text:
        scored = enrich_reports_with_pdf_text(scored)
    else:
        scored["pdf_text"] = ""
        scored["pdf_text_status"] = "skipped"
        scored["pdf_text_length"] = 0
        scored["content_full"] = scored["content"].fillna("").astype(str)
    scored = _annotate_parse_quality(scored)

    title_scores = scored["title"].apply(score_text).apply(pd.Series)
    body_scores = scored["content_full"].apply(score_text).apply(pd.Series)
    scored["title_sentiment"] = title_scores["sentiment"]
    scored["body_sentiment"] = body_scores["sentiment"]
    scored["title_confidence"] = title_scores["confidence"]
    scored["body_confidence"] = body_scores["confidence"]
    scored["uncertainty_score"] = title_scores["uncertainty"] + body_scores["uncertainty"]
    scored["target_revision_pct"] = ((scored["target_price"] / scored["prev_target_price"]) - 1.0) * 100.0
    scored.loc[scored["prev_target_price"].isna() | scored["prev_target_price"].eq(0), "target_revision_pct"] = np.nan
    scored["rating_delta_score"] = [rating_delta_score(rating, prev) for rating, prev in zip(scored["rating"], scored["prev_rating"])]
    scored["report_sentiment_raw"] = (
        scored["title_sentiment"] * 1.6
        + scored["body_sentiment"] * 1.0
        + scored["title_confidence"] * 1.1
        + scored["body_confidence"] * 0.7
        + scored["rating_delta_score"] * 2.5
        + scored["target_revision_pct"].fillna(0.0) / 7.5
        - scored["uncertainty_score"] * 1.2
    ) * (0.65 + scored["report_parse_quality_score"].fillna(0.0).clip(lower=0.0, upper=1.0) * 0.35)
    if attach_labels:
        scored = attach_forward_return_labels(scored)
    scored = apply_broker_bias_adjustment(scored, score_col="report_sentiment_raw", label_col="realized_alpha_score")
    scored = apply_novelty_features(scored)
    if attach_labels:
        scored = apply_return_calibration(scored, feature_col="report_sentiment_prelabel", label_col="realized_alpha_score")
    else:
        scored["report_sentiment_score"] = scored["report_sentiment_prelabel"]
        scored["calibration_status"] = "labels_skipped"
    scored["scored_at"] = datetime.now().isoformat(timespec="seconds")
    return scored
