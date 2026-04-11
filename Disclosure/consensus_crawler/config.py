import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    # universe filter
    PRICE_MIN: int = int(os.getenv("PRICE_MIN", "2000"))
    MARCAP_MIN: float = float(os.getenv("MARCAP_MIN", str(1000e8)))

    EXCLUDED_NAME_KEYWORDS: tuple = (
        "ETF", "ETN", "리츠", "스팩", "SPAC", "증권투자회사", "선박투자", "인프라투융자", "부동산투자회사"
    )
    EXCLUDED_EXACT_SUFFIXES: tuple = ("우", "우B", "우C", "우(전환)", "1우", "2우", "3우")

    # playwright
    HEADLESS: bool = os.getenv("HEADLESS", "1") == "1"
    NAV_TIMEOUT_MS: int = int(os.getenv("NAV_TIMEOUT_MS", "10000"))

    # mongo
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    DB_NAME: str = os.getenv("DB_NAME", "stock_data")
    COLLECTION: str = os.getenv("COLLECTION", "consensus_recent")

SETTINGS = Settings()
