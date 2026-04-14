from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from official_index_clone import OFFICIAL_INDEX_INPUT_DIR, OFFICIAL_REQUIRED_COLUMNS
except Exception:  # pragma: no cover - package import fallback
    from Disclosure.official_index_clone import OFFICIAL_INDEX_INPUT_DIR, OFFICIAL_REQUIRED_COLUMNS


RAW_DEFAULT_DIR = OFFICIAL_INDEX_INPUT_DIR / "raw"
RAW_FILE_DEFAULTS = {
    "reviews": "review_metadata.csv",
    "universe": "universe_export.csv",
    "bucket_targets": "bucket_targets_export.csv",
}
ALIASES = {
    "reviews": {
        "review_date": ("review_date", "심사기준일", "정기심사일", "review_dt"),
        "index_name": ("index_name", "지수명", "index"),
        "effective_date": ("effective_date", "적용일", "변경적용일", "effective_dt"),
        "cutoff": ("cutoff", "구성종목수", "편입종목수"),
        "entry_ratio": ("entry_ratio", "신규진입비율", "entry_threshold_ratio"),
        "keep_ratio": ("keep_ratio", "기존유지비율", "keep_threshold_ratio"),
        "liquidity_coverage": ("liquidity_coverage", "유동성커버리지", "liquidity_threshold_ratio"),
        "special_largecap_rank": ("special_largecap_rank", "특례시총순위", "largecap_special_rank"),
    },
    "universe": {
        "review_date": ("review_date", "심사기준일", "정기심사일", "review_dt"),
        "index_name": ("index_name", "지수명", "index"),
        "symbol": ("symbol", "code", "종목코드", "ticker"),
        "name": ("name", "종목명", "company_name"),
        "market": ("market", "시장", "market_name"),
        "official_sector": ("official_sector", "공식산업군", "sector", "industry_name"),
        "official_bucket": ("official_bucket", "공식버킷", "bucket", "sector_bucket"),
        "avg_ffmc_1y_krw": ("avg_ffmc_1y_krw", "1년평균유동시총", "avg_ffmc", "avg_free_float_mcap"),
        "avg_trading_value_1y_krw": ("avg_trading_value_1y_krw", "1년평균거래대금", "avg_trading_value", "avg_value"),
        "market_cap_rank_all": ("market_cap_rank_all", "전체시총순위", "market_cap_rank", "mcap_rank"),
        "listing_age_days": ("listing_age_days", "상장경과일수", "listed_days"),
        "free_float_ratio": ("free_float_ratio", "유동주식비율", "float_ratio"),
        "is_eligible": ("is_eligible", "심사대상여부", "eligible"),
        "is_current_member": ("is_current_member", "현재구성종목여부", "current_member"),
    },
    "bucket_targets": {
        "review_date": ("review_date", "심사기준일", "정기심사일", "review_dt"),
        "index_name": ("index_name", "지수명", "index"),
        "official_bucket": ("official_bucket", "공식버킷", "bucket", "sector_bucket"),
        "target_count": ("target_count", "목표좌석수", "구성종목수", "quota"),
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize vendor exports for official index clone mode.")
    parser.add_argument("--raw-dir", default=str(RAW_DEFAULT_DIR), help="Directory holding raw vendor exports.")
    parser.add_argument("--output-dir", default=str(OFFICIAL_INDEX_INPUT_DIR), help="Directory to write canonical clone input CSVs.")
    parser.add_argument("--print-only", action="store_true", help="Print planned output rows without writing files.")
    return parser.parse_args()


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _resolve_source_path(raw_dir: Path, key: str) -> Path:
    return raw_dir / RAW_FILE_DEFAULTS[key]


def _pick_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    alias_map = {_clean_text(column).lower(): column for column in frame.columns}
    for alias in aliases:
        matched = alias_map.get(_clean_text(alias).lower())
        if matched:
            return matched
    return None


def _normalize_frame(frame: pd.DataFrame, key: str) -> pd.DataFrame:
    normalized = pd.DataFrame()
    missing: list[str] = []
    for canonical, aliases in ALIASES[key].items():
        source = _pick_column(frame, aliases)
        if source is None:
            missing.append(canonical)
            continue
        normalized[canonical] = frame[source]
    if missing:
        raise RuntimeError(f"{key} raw export is missing columns for: {', '.join(sorted(missing))}")
    for column in OFFICIAL_REQUIRED_COLUMNS[key]:
        normalized[column] = normalized[column].map(_clean_text)
    return normalized[list(sorted(OFFICIAL_REQUIRED_COLUMNS[key]))]


def _load_raw_csv(raw_dir: Path, key: str) -> pd.DataFrame:
    source = _resolve_source_path(raw_dir, key)
    if not source.exists():
        raise RuntimeError(f"raw export missing: {source}")
    return pd.read_csv(source, dtype=str, encoding="utf-8-sig").fillna("")


def normalize_official_raw_inputs(raw_dir: Path, output_dir: Path, *, print_only: bool = False) -> dict[str, int]:
    counts: dict[str, int] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for key in ("reviews", "universe", "bucket_targets"):
        raw = _load_raw_csv(raw_dir, key)
        normalized = _normalize_frame(raw, key)
        counts[key] = int(len(normalized))
        if print_only:
            continue
        target = output_dir / f"{key}.csv"
        normalized.to_csv(target, index=False, encoding="utf-8-sig")
    return counts


def main() -> None:
    args = parse_args()
    counts = normalize_official_raw_inputs(
        Path(args.raw_dir),
        Path(args.output_dir),
        print_only=args.print_only,
    )
    print("[official-index-clone-prepare] complete")
    for key in ("reviews", "universe", "bucket_targets"):
        print(f"- {key}: rows={counts.get(key, 0)}")


if __name__ == "__main__":
    main()
