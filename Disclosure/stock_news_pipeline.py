from __future__ import annotations

import glob
import json
import logging
import math
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import numpy as np
import pandas as pd


log = logging.getLogger("disclosure.stock_news")
KST = timezone(timedelta(hours=9), name="KST")
ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "news")
RAW_DIR = os.path.join(ROOT_DIR, "logs")
SUMMARY_DIR = os.path.join(ROOT_DIR, "summaries")

POSITIVE_TERMS = ["실적", "잠정", "상향", "매수", "비중확대", "수주", "단일판매", "공급계약", "허가", "승인", "자사주", "소각", "증설", "인수", "합병"]
NEGATIVE_TERMS = ["하향", "중립", "매도", "유상증자", "cb", "bw", "전환", "철회", "정정", "해지", "소송", "영업정지", "지연", "불성실"]
HESITATION_TERMS = ["가능성", "예정", "추진", "검토", "협의", "mou", "양해각서", "불확실", "우려"]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(SUMMARY_DIR, exist_ok=True)


def _norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _count_terms(text: str, terms: list[str]) -> int:
    lowered = f" {str(text or '').lower()} "
    return sum(1 for term in terms if term and term.lower() in lowered)


def _normalize_title(text: str) -> str:
    lowered = str(text or "").strip().lower()
    lowered = re.sub(r"\s+", " ", lowered)
    lowered = re.sub(r"[^0-9a-z가-힣 ]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _score_title(title: str) -> Dict[str, float]:
    positive = _count_terms(title, POSITIVE_TERMS)
    negative = _count_terms(title, NEGATIVE_TERMS)
    hesitation = _count_terms(title, HESITATION_TERMS)
    return {
        "sentiment": float(positive - negative),
        "confidence": float(positive - hesitation),
        "uncertainty": float(hesitation),
    }


def _parse_published_at(body: Dict[str, Any]) -> str:
    now = _now_kst()
    for key in ("published_at", "datetime", "dtm", "date_time"):
        value = str(body.get(key, "")).strip()
        if value:
            return value
    time_text = str(body.get("time", "")).strip()
    digits = "".join(ch for ch in time_text if ch.isdigit())
    if len(digits) >= 4:
        hhmm = digits[:4]
        return now.strftime("%Y-%m-%d") + f"T{hhmm[:2]}:{hhmm[2:4]}:00+09:00"
    return now.isoformat(timespec="seconds")


def append_stock_news_packet(body: Dict[str, Any], *, source: str = "LS_NEWS") -> bool:
    if not isinstance(body, dict):
        return False
    title = str(body.get("title", "")).strip()
    if not title:
        return False
    _ensure_dirs()
    payload = {
        "source": source,
        "news_id": str(body.get("id", "")).strip(),
        "symbol": _norm_symbol(body.get("code") or body.get("symbol")),
        "stock_name": str(body.get("name") or body.get("stock_name") or body.get("jongname") or "").strip(),
        "title": title,
        "time": str(body.get("time", "")).strip(),
        "published_at": _parse_published_at(body),
        "raw": body,
    }
    path = os.path.join(RAW_DIR, f"stock_news_{_now_kst().strftime('%Y%m%d')}.jsonl")
    with open(path, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return True


def load_raw_stock_news(days: int = 7) -> pd.DataFrame:
    _ensure_dirs()
    rows: list[dict] = []
    min_dt = _now_kst() - timedelta(days=max(1, int(days)))
    for path in sorted(glob.glob(os.path.join(RAW_DIR, "stock_news_*.jsonl"))):
        with open(path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["published_at"] = pd.to_datetime(df.get("published_at"), errors="coerce")
    df = df[df["published_at"].fillna(min_dt) >= min_dt].copy()
    return df.reset_index(drop=True)


def score_stock_news(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    scored = df.copy()
    scored["symbol"] = scored.get("symbol", "").astype(str).map(_norm_symbol)
    scored["title"] = scored.get("title", "").fillna("").astype(str)
    scored["stock_name"] = scored.get("stock_name", "").fillna("").astype(str)
    scored["source"] = scored.get("source", "").fillna("UNKNOWN").astype(str)
    scored = scored.drop_duplicates(subset=["symbol", "title", "published_at"]).reset_index(drop=True)
    scored["title_norm"] = scored["title"].map(_normalize_title)

    title_scores = scored["title"].apply(_score_title).apply(pd.Series)
    scored["title_sentiment"] = title_scores["sentiment"]
    scored["title_confidence"] = title_scores["confidence"]
    scored["uncertainty_score"] = title_scores["uncertainty"]

    age_hours = (_now_kst() - scored["published_at"].fillna(_now_kst())).dt.total_seconds().div(3600).clip(lower=0)
    recency_weight = np.clip(np.exp(-age_hours / 72.0), 0.25, 1.0)
    id15_bonus = (scored.get("news_id", "").astype(str) == "15").astype(float) * 0.8
    repeat_weight = scored.groupby("symbol")["title"].transform("count").clip(lower=1).map(lambda x: min(2.0, math.log1p(x)))
    story_key = scored["symbol"].astype(str) + "|" + scored["title_norm"].astype(str)
    scored["story_repeat_count"] = story_key.map(story_key.value_counts()).astype(float)
    scored["story_source_breadth"] = scored.groupby(story_key)["source"].transform("nunique").astype(float)
    scored["story_first_seen"] = scored.groupby(story_key)["published_at"].transform("min")
    story_age_hours = (_now_kst() - scored["story_first_seen"].fillna(_now_kst())).dt.total_seconds().div(3600).clip(lower=0)
    scored["news_novelty_score"] = (1.0 / np.sqrt(scored["story_repeat_count"].clip(lower=1.0))).clip(0.25, 1.0)
    scored["news_source_breadth_score"] = np.clip(np.log1p(scored["story_source_breadth"]) / math.log(4.0), 0.0, 1.0)
    scored["news_diffusion_score"] = (
        scored["news_source_breadth_score"].fillna(0.0) * np.exp(-story_age_hours / 36.0)
        + np.log1p(scored["story_repeat_count"].clip(lower=0.0)) / math.log(6.0)
    ).clip(0.0, 2.0)
    scored["news_score"] = (
        scored["title_sentiment"] * 2.0
        + scored["title_confidence"] * 1.1
        - scored["uncertainty_score"] * 1.4
        + id15_bonus
        + scored["news_diffusion_score"].fillna(0.0) * 1.0
        + scored["news_novelty_score"].fillna(0.0) * 0.8
    ) * recency_weight + repeat_weight
    return scored


def build_stock_news_summary(scored: pd.DataFrame, top_n: int = 30) -> Dict[str, Any]:
    if scored.empty:
        return {"snapshot_at": _now_kst().isoformat(timespec="seconds"), "top_positive": [], "top_negative": []}

    grouped = []
    for symbol, group in scored.groupby("symbol", dropna=False):
        if not symbol:
            continue
        latest = group.sort_values("published_at").iloc[-1]
        avg_score = float(group["news_score"].mean())
        max_score = float(group["news_score"].max())
        news_count = int(len(group))
        grouped.append(
            {
                "symbol": symbol,
                "name": latest.get("stock_name") or symbol,
                "latest_title": latest.get("title") or "",
                "news_count": news_count,
                "avg_news_score": round(avg_score, 4),
                "max_news_score": round(max_score, 4),
                "source_breadth": int(group["source"].fillna("UNKNOWN").astype(str).nunique()),
                "avg_diffusion_score": round(float(group.get("news_diffusion_score", pd.Series(dtype=float)).mean()), 4),
                "avg_novelty_score": round(float(group.get("news_novelty_score", pd.Series(dtype=float)).mean()), 4),
                "conviction_score": round(avg_score + min(3.0, math.log1p(news_count)), 4),
                "last_seen": str(latest.get("published_at") or ""),
            }
        )

    ranked = sorted(grouped, key=lambda item: item["conviction_score"], reverse=True)
    negative = sorted(grouped, key=lambda item: item["avg_news_score"])
    return {
        "snapshot_at": _now_kst().isoformat(timespec="seconds"),
        "top_positive": ranked[:top_n],
        "top_negative": negative[:top_n],
    }


def save_stock_news_summary(scored: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, str]:
    _ensure_dirs()
    stamp = _now_kst().strftime("%Y%m%d_%H%M%S")
    scored_csv = os.path.join(SUMMARY_DIR, f"stock_news_scored_{stamp}.csv")
    summary_json = os.path.join(SUMMARY_DIR, f"stock_news_summary_{stamp}.json")
    latest_scored_csv = os.path.join(SUMMARY_DIR, "stock_news_scored_latest.csv")
    latest_summary_json = os.path.join(SUMMARY_DIR, "stock_news_summary_latest.json")
    scored.to_csv(scored_csv, index=False, encoding="utf-8-sig")
    scored.to_csv(latest_scored_csv, index=False, encoding="utf-8-sig")
    with open(summary_json, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    with open(latest_summary_json, "w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)
    return {
        "scored_csv": scored_csv,
        "summary_json": summary_json,
        "latest_scored_csv": latest_scored_csv,
        "latest_summary_json": latest_summary_json,
    }


def build_stock_news_digest(summary: Dict[str, Any], top_n: int = 10) -> str:
    lines = ["[종목 뉴스 스코어 요약]"]
    lines.append(f"- 생성시각: {summary.get('snapshot_at', '')}")
    lines.append("- 상위 긍정 뉴스:")
    for item in summary.get("top_positive", [])[:top_n]:
        lines.append(
            f"* {item['name']}({item['symbol']}) | conviction {item['conviction_score']} | "
            f"avg {item['avg_news_score']} | 뉴스 {item['news_count']}건"
        )
        lines.append(f"  최근: {item['latest_title']}")
    lines.append("- 상위 부정 뉴스:")
    for item in summary.get("top_negative", [])[: max(3, min(top_n, 5))]:
        lines.append(f"* {item['name']}({item['symbol']}) | avg {item['avg_news_score']} | 최근: {item['latest_title']}")
    return "\n".join(lines)
