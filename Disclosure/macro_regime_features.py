from __future__ import annotations

import json
import os
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(ROOT_DIR, "macro", "cache")
LATEST_PATH = os.path.join(CACHE_DIR, "macro_regime_latest.json")
QUERIES = [
    "금리 OR 환율 OR 연준 OR 파월 OR CPI OR 인플레이션 when:1d",
    "유가 OR 지정학 OR 전쟁 OR 관세 OR 수출 OR 트럼프 when:1d",
    "반도체 OR AI OR 엔비디아 OR 애플 OR 테슬라 when:1d",
    "밸류업 OR 한은 OR 실적발표 OR 공급망 when:1d",
]
SECTOR_KEYWORDS = {
    "IT하드웨어 (반도체)": ["반도체", "HBM", "엔비디아", "AI", "메모리"],
    "IT소프트웨어 (플랫폼/SI)": ["플랫폼", "소프트웨어", "클라우드", "AI 서비스"],
    "에너지/화학": ["유가", "석유", "정유", "화학", "천연가스"],
    "은행/증권": ["금리", "밸류업", "은행", "증권", "자본규제"],
    "조선/기계": ["조선", "해양", "방산", "수주", "공급망"],
    "자동차/부품": ["자동차", "테슬라", "전기차", "배터리"],
    "전기/유틸리티": ["전력", "유틸리티", "원전", "가스", "한전"],
}


def _fetch_headlines(max_per_query: int = 6) -> list[str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    headlines: list[str] = []
    for query in QUERIES:
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item")[:max_per_query]:
                title = str(item.findtext("title") or "").rsplit("-", 1)[0].strip()
                if title and title not in headlines:
                    headlines.append(title)
        except Exception:
            continue
    return headlines


def _score_headlines(headlines: list[str]) -> dict:
    sector_scores = {key: 0.0 for key in SECTOR_KEYWORDS}
    risk_score = 0.0
    for title in headlines:
        lowered = title.lower()
        if any(keyword in title for keyword in ["관세", "전쟁", "지정학", "긴축", "인플레이션"]):
            risk_score -= 1.0
        if any(keyword in title for keyword in ["AI", "반도체", "밸류업", "실적", "수출 회복"]):
            risk_score += 0.8
        for sector, keywords in SECTOR_KEYWORDS.items():
            hits = sum(1 for keyword in keywords if keyword.lower() in lowered or keyword in title)
            if hits:
                sector_scores[sector] += float(hits)
    return {"risk_score": risk_score, "sector_scores": sector_scores}


def build_macro_regime_summary(force_refresh: bool = False) -> dict:
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(LATEST_PATH) and not force_refresh:
        age_min = (datetime.now().timestamp() - os.path.getmtime(LATEST_PATH)) / 60.0
        if age_min <= 180:
            try:
                with open(LATEST_PATH, "r", encoding="utf-8") as fp:
                    return json.load(fp)
            except Exception:
                pass
    headlines = _fetch_headlines()
    scored = _score_headlines(headlines)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "headline_count": len(headlines),
        "risk_score": round(float(scored["risk_score"]), 4),
        "sector_scores": {k: round(float(v), 4) for k, v in scored["sector_scores"].items()},
        "headlines": headlines[:20],
    }
    with open(LATEST_PATH, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    return payload


def build_macro_interaction_frame(card_df: pd.DataFrame, force_refresh: bool = False) -> pd.DataFrame:
    if card_df is None or card_df.empty:
        return pd.DataFrame()
    summary = build_macro_regime_summary(force_refresh=force_refresh)
    sector_scores = summary.get("sector_scores") or {}
    df = card_df[["symbol", "sector"]].copy()
    df["macro_sector_score"] = df["sector"].map(sector_scores).fillna(0.0)
    df["macro_regime_score"] = float(summary.get("risk_score", 0.0) or 0.0)
    micro_cols = [col for col in ["news_score", "flow_score", "analyst_avg_score", "microstructure_score"] if col in card_df.columns]
    if micro_cols:
        base_signal = card_df[micro_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).mean(axis=1)
    else:
        base_signal = pd.Series(0.0, index=card_df.index)
    df["macro_micro_interaction_score"] = (
        df["macro_sector_score"].fillna(0.0).rank(pct=True, ascending=True) * 0.55
        + base_signal.rank(pct=True, ascending=True) * 0.45
    )
    return df
