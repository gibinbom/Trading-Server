from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any

import pandas as pd

OFFICIAL_INDEX_INPUT_DIR = Path(__file__).resolve().parent / "index_clone_inputs"
OFFICIAL_METHODOLOGY_VERSION = "official_clone_v1"
OFFICIAL_REQUIRED_FILES = {
    "reviews": "reviews.csv",
    "universe": "universe.csv",
    "bucket_targets": "bucket_targets.csv",
}
OFFICIAL_REQUIRED_COLUMNS = {
    "reviews": {
        "review_date",
        "index_name",
        "effective_date",
        "cutoff",
        "entry_ratio",
        "keep_ratio",
        "liquidity_coverage",
        "special_largecap_rank",
    },
    "universe": {
        "review_date",
        "index_name",
        "symbol",
        "name",
        "market",
        "official_sector",
        "official_bucket",
        "avg_ffmc_1y_krw",
        "avg_trading_value_1y_krw",
        "market_cap_rank_all",
        "listing_age_days",
        "free_float_ratio",
        "is_eligible",
        "is_current_member",
    },
    "bucket_targets": {
        "review_date",
        "index_name",
        "official_bucket",
        "target_count",
    },
}


class OfficialCloneInputError(RuntimeError):
    pass


@dataclass
class OfficialCloneBundle:
    reviews: pd.DataFrame
    universe: pd.DataFrame
    bucket_targets: pd.DataFrame
    input_dir: Path


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def norm_symbol(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def rank_limit(target: int, ratio: float) -> int:
    if target <= 0:
        return 0
    return max(1, int(math.ceil(target * ratio)))


def read_csv(base_dir: Path, key: str) -> pd.DataFrame:
    file_name = OFFICIAL_REQUIRED_FILES[key]
    target = base_dir / file_name
    if not target.exists():
        raise OfficialCloneInputError(f"official clone input missing: {target}")
    frame = pd.read_csv(target, dtype=str, encoding="utf-8-sig").fillna("")
    missing = sorted(OFFICIAL_REQUIRED_COLUMNS[key] - set(frame.columns))
    if missing:
        raise OfficialCloneInputError(f"{file_name} missing required columns: {', '.join(missing)}")
    return frame


def load_official_clone_bundle(base_dir: Path | None = None) -> OfficialCloneBundle:
    resolved = base_dir or OFFICIAL_INDEX_INPUT_DIR
    return OfficialCloneBundle(
        reviews=read_csv(resolved, "reviews"),
        universe=read_csv(resolved, "universe"),
        bucket_targets=read_csv(resolved, "bucket_targets"),
        input_dir=resolved,
    )


def select_latest_reviews(bundle: OfficialCloneBundle) -> pd.DataFrame:
    reviews = bundle.reviews.copy()
    if reviews.empty:
        raise OfficialCloneInputError("reviews.csv is empty")
    latest_review_date = max(clean_text(value) for value in reviews["review_date"].tolist() if clean_text(value))
    scoped = reviews[reviews["review_date"].astype(str).str.strip() == latest_review_date].copy()
    if scoped.empty:
        raise OfficialCloneInputError("no latest review rows were found")
    return scoped
