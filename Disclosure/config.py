import os
from dataclasses import dataclass

HARDCODED_GEMINI_API_KEY = "AIzaSyDiyM1ju1pk8mLEcvAHYI79W9QMU0iY57k"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def env_bool(key: str, default: str = "0") -> bool:
    return os.getenv(key, default) in ("1", "true", "True", "YES", "yes")

def env_int(key: str, default: str = "0") -> int:
    return int(os.getenv(key, default))

@dataclass(frozen=True)
class SurpriseThreshold:
    revenue: float = 1.00
    op: float = 0.20
    net: float = 0.30

@dataclass(frozen=True)
class OrderHitThreshold:
    # 계약금액 / 분기매출 컨센 >= hit_ratio 이면 "히트"
    hit_ratio: float = 0.80

@dataclass(frozen=True)
class Settings:
    # --- DART/KIND ---
    OPEN_DART_API_KEY: str = os.getenv("OPEN_DART_API_KEY", "2374a57515abe0d4523a34666d661325284c3058" )
    POLL_SEC: float = float(os.getenv("POLL_SEC", "1"))
    BOOTSTRAP_WEEKS: int = int(os.getenv("BOOTSTRAP_WEEKS", "2"))

    PLAYWRIGHT_HEADLESS = False      # ✅ bootstrap에서 브라우저 보이게
    PLAYWRIGHT_SLOWMO_MS = 50        # ✅ 느리게 움직여서 눈에 보이게(원하면 0)
    BOOTSTRAP_SHOW_BROWSER = False    # ✅ bootstrap 동안만 강제로 보여주기

    # --- Playwright (재사용 구조용) ---
    # 재사용 ON이면: 브라우저 1개 + 컨텍스트 1개를 유지하고 page만 풀로 빌려 씀
    PLAYWRIGHT_REUSE_BROWSER: bool = env_bool("PLAYWRIGHT_REUSE_BROWSER", "1")
    PLAYWRIGHT_MAX_PAGES: int = env_int("PLAYWRIGHT_MAX_PAGES", "2")  # 동시 페이지 수(2~3 권장)

    # headless/slowmo는 “표시 여부”에 따라 engine에서 오버라이드 할 수 있음
    PLAYWRIGHT_HEADLESS_DEFAULT: bool = env_bool("PLAYWRIGHT_HEADLESS_DEFAULT", "1")
    PLAYWRIGHT_SLOWMO_MS: int = env_int("PLAYWRIGHT_SLOWMO_MS", "0")

    # 빠른 로딩을 위해 불필요 리소스 차단(성능/안정성 목적)
    PLAYWRIGHT_BLOCK_RESOURCES: bool = env_bool("PLAYWRIGHT_BLOCK_RESOURCES", "1")
    PLAYWRIGHT_NAV_TIMEOUT_MS: int = env_int("PLAYWRIGHT_NAV_TIMEOUT_MS", "15000")
    PLAYWRIGHT_WAIT_UNTIL: str = os.getenv("PLAYWRIGHT_WAIT_UNTIL", "domcontentloaded")  # domcontentloaded 권장

    # (선택) 고정 UA/로케일/타임존 (회전 X, 빈 값이면 기본 UA 사용)
    PLAYWRIGHT_USER_AGENT: str = os.getenv("PLAYWRIGHT_USER_AGENT", "")
    PLAYWRIGHT_LOCALE: str = os.getenv("PLAYWRIGHT_LOCALE", "ko-KR")
    PLAYWRIGHT_TIMEZONE_ID: str = os.getenv("PLAYWRIGHT_TIMEZONE_ID", "Asia/Seoul")

    # bootstrap 동안만 브라우저를 보여주고 싶으면
    BOOTSTRAP_SHOW_BROWSER: bool = env_bool("BOOTSTRAP_SHOW_BROWSER", "0")
    BOOTSTRAP_VERBOSE: bool = env_bool("BOOTSTRAP_VERBOSE", "0")

    # live에서 브라우저/로그 verbosity
    LIVE_SHOW_BROWSER: bool = env_bool("LIVE_SHOW_BROWSER", "0")
    LIVE_VERBOSE: bool = env_bool("LIVE_VERBOSE", "1")

    # list.json 파라미터(엔진에서 getattr로 쓰고 있어서 있으면 편함)
    DART_LIST_MAX_PAGES: int = env_int("DART_LIST_MAX_PAGES", "1")
    DART_LIST_PAGE_COUNT: int = env_int("DART_LIST_PAGE_COUNT", "100")

    # --- Mongo ---
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME: str = os.getenv("DB_NAME", "stock_data")
    CONSENSUS_COLLECTION: str = os.getenv("CONSENSUS_COLLECTION", "consensus_recent")
    STATE_COLLECTION: str = os.getenv("STATE_COLLECTION", "state")
    SIGNAL_LOG_COLLECTION: str = os.getenv("SIGNAL_LOG_COLLECTION", "signal_log")

    # --- AI / Gemini ---
    # 우선순위:
    # 1) PM2/env의 GEMINI_API_KEY
    # 2) PM2/env의 GOOGLE_API_KEY
    # 3) 코드에 직접 넣는 HARDCODED_GEMINI_API_KEY
    GEMINI_API_KEY: str = (
        os.getenv("GEMINI_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
        or HARDCODED_GEMINI_API_KEY
    )

    # --- Slack ---
    SLACK_ENABLED: bool = env_bool("SLACK_ENABLED", "1")
    SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")
    SLACK_BOT_TOKEN: str = os.getenv("SLACK_BOT_TOKEN", "")
    SLACK_REPORT_CHANNEL: str = os.getenv("SLACK_REPORT_CHANNEL", "")
    SLACK_NOTIFY_TRADE_SKIP: bool = env_bool("SLACK_NOTIFY_TRADE_SKIP", "0")
    SLACK_DELIVERY_LOG_PATH: str = os.getenv(
        "SLACK_DELIVERY_LOG_PATH",
        os.path.join(BASE_DIR, "logs", "slack_delivery.jsonl"),
    )
    SLACK_FALLBACK_LOG_PATH: str = os.getenv(
        "SLACK_FALLBACK_LOG_PATH",
        os.path.join(BASE_DIR, "logs", "slack_fallback.jsonl"),
    )
    TRADE_ALERT_AUDIT_PATH: str = os.getenv(
        "TRADE_ALERT_AUDIT_PATH",
        os.path.join(BASE_DIR, "logs", "trade_alert_audit.jsonl"),
    )

    # --- Trading ---
    # 실시간 자동매매
    ENABLE_AUTO_TRADE: bool = env_bool("ENABLE_AUTO_TRADE", "1")
    CLOSE_SWING_ENABLE: bool = env_bool("CLOSE_SWING_ENABLE", "1")
    CLOSE_SWING_MIN_SUPPORT_SCORE: int = env_int("CLOSE_SWING_MIN_SUPPORT_SCORE", "4")
    CLOSE_SWING_MIN_CARD_SCORE: float = float(os.getenv("CLOSE_SWING_MIN_CARD_SCORE", "0.70"))
    CLOSE_SWING_MIN_FACTOR_SCORE: float = float(os.getenv("CLOSE_SWING_MIN_FACTOR_SCORE", "0.58"))
    CLOSE_SWING_MIN_ACTIVE_SOURCES: int = env_int("CLOSE_SWING_MIN_ACTIVE_SOURCES", "4")
    CLOSE_SWING_MIN_LIQUIDITY_SCORE: float = float(os.getenv("CLOSE_SWING_MIN_LIQUIDITY_SCORE", "0.45"))
    CLOSE_SWING_SOFT_LIQUIDITY_SCORE: float = float(os.getenv("CLOSE_SWING_SOFT_LIQUIDITY_SCORE", "0.65"))
    CLOSE_SWING_MIN_AVG_TURNOVER_20D: float = float(os.getenv("CLOSE_SWING_MIN_AVG_TURNOVER_20D", "0.0025"))
    CLOSE_SWING_SOFT_CHASE_PCT: float = float(os.getenv("CLOSE_SWING_SOFT_CHASE_PCT", "5.0"))
    CLOSE_SWING_MAX_CHASE_PCT: float = float(os.getenv("CLOSE_SWING_MAX_CHASE_PCT", "8.0"))
    CLOSE_SWING_FAIL_DROP_PCT: float = float(os.getenv("CLOSE_SWING_FAIL_DROP_PCT", "-4.0"))
    CLOSE_SWING_NEGATIVE_ENTRY_PCT: float = float(os.getenv("CLOSE_SWING_NEGATIVE_ENTRY_PCT", "-1.5"))
    CLOSE_SWING_MIN_RECOVERY_EDGE: float = float(os.getenv("CLOSE_SWING_MIN_RECOVERY_EDGE", "0.05"))
    CLOSE_SWING_MIN_FLOW_SCORE: float = float(os.getenv("CLOSE_SWING_MIN_FLOW_SCORE", "0.55"))
    CLOSE_SWING_STALE_EVENT_MIN: int = env_int("CLOSE_SWING_STALE_EVENT_MIN", "210")
    CLOSE_SWING_MAX_EVENT_AGE_MIN: int = env_int("CLOSE_SWING_MAX_EVENT_AGE_MIN", "600")
    CLOSE_SWING_MAX_TRADES_PER_DAY: int = env_int("CLOSE_SWING_MAX_TRADES_PER_DAY", "3")
    CLOSE_SWING_MAX_CANDIDATES_PER_CYCLE: int = env_int("CLOSE_SWING_MAX_CANDIDATES_PER_CYCLE", "3")
    CLOSE_SWING_MAX_OPEN_POSITIONS: int = env_int("CLOSE_SWING_MAX_OPEN_POSITIONS", "4")
    CLOSE_SWING_MAX_TRADES_PER_SECTOR_PER_DAY: int = env_int("CLOSE_SWING_MAX_TRADES_PER_SECTOR_PER_DAY", "1")
    CLOSE_SWING_MAX_CANDIDATES_PER_SECTOR_PER_CYCLE: int = env_int("CLOSE_SWING_MAX_CANDIDATES_PER_SECTOR_PER_CYCLE", "1")
    CLOSE_SWING_MAX_OPEN_NAMES_PER_SECTOR: int = env_int("CLOSE_SWING_MAX_OPEN_NAMES_PER_SECTOR", "1")
    CLOSE_SWING_MAX_BUDGET_PER_DAY_KRW: int = env_int("CLOSE_SWING_MAX_BUDGET_PER_DAY_KRW", "600000")
    CLOSE_SWING_MAX_RETRY_PER_RECORD: int = env_int("CLOSE_SWING_MAX_RETRY_PER_RECORD", "3")
    CLOSE_SWING_REQUIRE_PRICE_SIGNAL: bool = env_bool("CLOSE_SWING_REQUIRE_PRICE_SIGNAL", "1")
    CLOSE_SWING_MAX_STOP_SELLS_PER_DAY: int = env_int("CLOSE_SWING_MAX_STOP_SELLS_PER_DAY", "1")
    CLOSE_SWING_DEGRADE_ON_QUOTE_FALLBACK: bool = env_bool("CLOSE_SWING_DEGRADE_ON_QUOTE_FALLBACK", "1")
    CLOSE_SWING_DEGRADED_MAX_CANDIDATES_PER_CYCLE: int = env_int("CLOSE_SWING_DEGRADED_MAX_CANDIDATES_PER_CYCLE", "1")
    CLOSE_SWING_DEGRADED_MAX_BUDGET_PER_TRADE_KRW: int = env_int("CLOSE_SWING_DEGRADED_MAX_BUDGET_PER_TRADE_KRW", "200000")
    CLOSE_SWING_RECHECK_COOLDOWN_SEC: int = env_int("CLOSE_SWING_RECHECK_COOLDOWN_SEC", "60")
    HYBRID_ROTATION_ENABLE: bool = env_bool("HYBRID_ROTATION_ENABLE", "1")
    HYBRID_SHADOW_ONLY: bool = env_bool("HYBRID_SHADOW_ONLY", "1")
    HYBRID_ACTIVE_SECTOR_MIN_SCORE: int = env_int("HYBRID_ACTIVE_SECTOR_MIN_SCORE", "60")
    HYBRID_RELATIVE_VALUE_MIN_SCORE: int = env_int("HYBRID_RELATIVE_VALUE_MIN_SCORE", "55")
    HYBRID_WICS_HARD_MIN_DAYS: int = env_int("HYBRID_WICS_HARD_MIN_DAYS", "10")
    CLOSE_BET_TAKE_PROFIT_PCT: float = float(os.getenv("CLOSE_BET_TAKE_PROFIT_PCT", "8.0"))
    CLOSE_BET_STOP_LOSS_PCT: float = float(os.getenv("CLOSE_BET_STOP_LOSS_PCT", "-4.0"))
    CLOSE_BET_STOP_GRACE_MIN: int = env_int("CLOSE_BET_STOP_GRACE_MIN", "60")
    CLOSE_BET_REQUIRE_RECOVER_BY_CLOSE: bool = env_bool("CLOSE_BET_REQUIRE_RECOVER_BY_CLOSE", "1")
    CLOSE_BET_REQUIRE_RECOVER_AFTER_OPEN: bool = env_bool("CLOSE_BET_REQUIRE_RECOVER_AFTER_OPEN", "1")
    CLOSE_BET_OPEN_RECOVERY_MIN: int = env_int("CLOSE_BET_OPEN_RECOVERY_MIN", "10")
    CLOSE_BET_MAX_HOLD_DAYS: int = env_int("CLOSE_BET_MAX_HOLD_DAYS", "3")
    CLOSE_BET_TIME_EXIT_START: str = os.getenv("CLOSE_BET_TIME_EXIT_START", "15:10")
    CLOSE_BET_TIME_EXIT_END: str = os.getenv("CLOSE_BET_TIME_EXIT_END", "15:20")
    MONITOR_FORCE_EOD_LIQUIDATE: bool = env_bool("MONITOR_FORCE_EOD_LIQUIDATE", "0")

    DRY_RUN: bool = os.getenv("DRY_RUN", "0") == "1"  # 기본은 안전하게 dry-run
    MAX_KRW_PER_TRADE: int = int(os.getenv("MAX_KRW_PER_TRADE", "300000"))  # 30만원
    MAX_POS_PER_NAME: int = int(os.getenv("MAX_POS_PER_NAME", "1"))  # 동일 종목 중복진입 제한

    # ✅ bootstrap에서 "테스트(가상)" 매매 실행
    BOOTSTRAP_TRADE: bool = env_bool("BOOTSTRAP_TRADE", "1")  # 기본 ON 추천
    BOOTSTRAP_IGNORE_SEEN: bool = env_bool("BOOTSTRAP_IGNORE_SEEN", "1")  # seen이어도 다시 테스트 매매
    BOOTSTRAP_SHOW_BROWSER: bool = env_bool("BOOTSTRAP_SHOW_BROWSER", "0")

    # ✅ bootstrap broker를 가상/모의로 강제
    BOOTSTRAP_IS_VIRTUAL: bool = env_bool("BOOTSTRAP_IS_VIRTUAL", "1")
    BOOTSTRAP_DRY_RUN: bool = env_bool("BOOTSTRAP_DRY_RUN", "0")  # 0이면 실제(가상계좌) 주문 전송

    # (선택) bootstrap에서 주문 너무 많이 나가는 걸 막고 싶으면
    BOOTSTRAP_MAX_TRADES: int = int(os.getenv("BOOTSTRAP_MAX_TRADES", "50"))

    # --- KIS (Korea Investment) ---
    KIS_IS_VIRTUAL: bool = os.getenv("KIS_IS_VIRTUAL", "1") == "1"

    # LIVE
    # KIS_APPKEY: str = os.getenv("KIS_APPKEY", "PSwIsPoRIqm8ttT5dNPb3nsLVKiLi42g4oPx")
    # KIS_APPSECRET: str = os.getenv("KIS_APPSECRET", "xchffRNeKLIvZO7j+dqcWD70XbDiuX5OfygL4aoT0pFX6/bCSSul6whE3+kA/tiDzry8w4thic8yFjT36xV9m8ONr4Y6T9J4oewIZINggcBqGSBJ5i3mQxL7IWiHayGN82SfYrtPQdAwqqQLCDnJC2Cc0qNPmPIjRiJW/Ct0EceCZ4iD2xM=")
    # KIS_CANO: str = os.getenv("KIS_CANO", "64236056") # 계좌번호 앞 8자리
    # KIS_ACNT_PRDT_CD: str = os.getenv("KIS_ACNT_PRDT_CD", "01") # 계좌번호 뒤 2자리

    KIS_APPKEY: str = os.getenv("KIS_APPKEY", "PSzVokk7g9i1F42E3ABUfLRIvMSw6m6WDc8v")
    KIS_APPSECRET: str = os.getenv("KIS_APPSECRET", "mS1GpnFqtHSBZ++NNe8cZR2vU93+Zwz4AiL0Bni+PBBbZD9XuVkeyHOV4/sk3N9cJ/5zNb946bn/tcCxYorcT9s/63UqujaghEKVlaKNB4IrU0wQucoK3X+w6r/86T3G0O8pCpZU4bSDJL72aMNHnHv4H0FeBACI2wXCD7r6bsnfENDt3oM=")
    KIS_CANO: str = os.getenv("KIS_CANO", "43744746") # 계좌번호 앞 8자리
    KIS_ACNT_PRDT_CD: str = os.getenv("KIS_ACNT_PRDT_CD", "01") # 계좌번호 뒤 2자리

    # VTS
    KIS_VTS_APPKEY: str = os.getenv("KIS_VTS_APPKEY", "PSsPZqzCbGrmwSMLV6a7zzD7gnJIdr1f9Mf1")
    KIS_VTS_APPSECRET: str = os.getenv("KIS_VTS_APPSECRET", "/65UgRF9plectWLSgSCBfmR4+azh0ai5ZvM8+npsDbUgTYW0rpN64QPDkP0uCDoYA528PQnqE/R52xdSOmSDDJhpjBgQtV7nv9J9QbXSZyj5QG6qZKE99sjM7Ntpy8pLMW0Ijzg8NNdj4W1r07RUPmMgQ6IdTnI2X4A1RpeA5XnqjYwOfCs=")
    KIS_VTS_CANO: str = os.getenv("KIS_VTS_CANO", "50158708")
    KIS_VTS_ACNT_PRDT_CD: str = os.getenv("KIS_VTS_ACNT_PRDT_CD", "01")

    WATCH_MIN_MARCAP = 1000 * 100_000_000  # 1,000억
    WATCH_TOP_N = 3000  # 너무 많으면 제한 (없으면 None)

    # --- Main Runtime Mode ---
    MAIN_RUN_MODE: str = os.getenv("MAIN_RUN_MODE", "event_log_watch")
    EVENT_LOG_WATCH_POLL_SEC: int = env_int("EVENT_LOG_WATCH_POLL_SEC", "15")
    EVENT_TRADE_LOOKBACK_DAYS: int = env_int("EVENT_TRADE_LOOKBACK_DAYS", "2")
    EVENT_TRADE_RETRY_COOLDOWN_SEC: int = env_int("EVENT_TRADE_RETRY_COOLDOWN_SEC", "900")
    EVENT_TRADE_BROKER_READY_CACHE_SEC: int = env_int("EVENT_TRADE_BROKER_READY_CACHE_SEC", "60")
    EVENT_TRADE_RESPECT_AVAILABLE_CASH: bool = env_bool("EVENT_TRADE_RESPECT_AVAILABLE_CASH", "1")
    EVENT_TRADE_MIN_CASH_BUFFER_KRW: int = env_int("EVENT_TRADE_MIN_CASH_BUFFER_KRW", "50000")
    EVENT_TRADE_WINDOW_START: str = os.getenv("EVENT_TRADE_WINDOW_START", "15:20")
    EVENT_TRADE_WINDOW_END: str = os.getenv("EVENT_TRADE_WINDOW_END", "15:30")
    EVENT_TRADE_ALLOWED_TYPES: str = os.getenv(
        "EVENT_TRADE_ALLOWED_TYPES",
        "SUPPLY_CONTRACT,PERF_PRELIM,SALES_VARIATION,BUYBACK,STOCK_CANCELLATION",
    )
    EVENT_TRADE_ALLOWED_SOURCES: str = os.getenv(
        "EVENT_TRADE_ALLOWED_SOURCES",
        "EVENT_COLLECTOR_HTML,EVENT_COLLECTOR_API",
    )
THRESHOLDS = SurpriseThreshold()
ORDER_HIT = OrderHitThreshold()
SETTINGS = Settings()
