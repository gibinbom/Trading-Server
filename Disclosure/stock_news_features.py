from __future__ import annotations

import math

import pandas as pd

try:
    from stock_news_pipeline import load_raw_stock_news, score_stock_news
except Exception:
    from Disclosure.stock_news_pipeline import load_raw_stock_news, score_stock_news


def build_stock_news_factor_frame(days: int = 7) -> pd.DataFrame:
    raw_df = load_raw_stock_news(days=days)
    scored = score_stock_news(raw_df)
    if scored.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "news_score",
                "news_count",
                "news_avg_score",
                "news_max_score",
                "news_confidence_score",
                "news_buzz_score",
                "news_source_breadth_score",
                "news_diffusion_score",
                "news_novelty_score",
            ]
        )

    rows = []
    for symbol, group in scored.groupby("symbol", dropna=False):
        if not symbol:
            continue
        news_count = int(len(group))
        avg_score = float(group["news_score"].mean())
        max_score = float(group["news_score"].max())
        confidence_score = float((group["title_confidence"] - group["uncertainty_score"]).mean())
        buzz_score = min(5.0, math.log1p(news_count) + max_score / 10.0)
        source_breadth_score = float(pd.to_numeric(group.get("news_source_breadth_score"), errors="coerce").fillna(0.0).mean())
        diffusion_score = float(pd.to_numeric(group.get("news_diffusion_score"), errors="coerce").fillna(0.0).mean())
        novelty_score = float(pd.to_numeric(group.get("news_novelty_score"), errors="coerce").fillna(0.0).mean())
        rows.append(
            {
                "symbol": str(symbol).zfill(6),
                "news_score": round(avg_score, 4),
                "news_count": news_count,
                "news_avg_score": round(avg_score, 4),
                "news_max_score": round(max_score, 4),
                "news_confidence_score": round(confidence_score, 4),
                "news_buzz_score": round(buzz_score, 4),
                "news_source_breadth_score": round(source_breadth_score, 4),
                "news_diffusion_score": round(diffusion_score, 4),
                "news_novelty_score": round(novelty_score, 4),
            }
        )
    return pd.DataFrame(rows)
