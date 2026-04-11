# engine.py (BURST MODE ONLY)
# - Trigger-Based: 평소에는 대기하다가 외부 신호(뉴스 등) 발생 시에만 DART 조회
# - Burst Mode: run_burst_poll() 호출 시 5초간 Interval 없이 DART "최근공시"를 초고속 스캔
# - New disclosure 발견 시 Playwright로 파싱 -> 시그널 계산 -> 실거래
# - Auto Monitor: 별도 스레드로 보유 종목의 TP/SL/EOD(15:29)/Evening(19:59) 청산 감시
#
# Requirements:
#   pip install requests beautifulsoup4
#

from __future__ import annotations

import re
import threading
import time
import random
import logging
import os
import datetime as dt
from typing import List, Dict, Optional, Any, Tuple

import requests
import pandas as pd

from config import SETTINGS, THRESHOLDS, ORDER_HIT
from mongo_state import MongoStateStore
from consensus_repo import MongoConsensusRepo

from dart_common import (
    classify_disclosure_event,
)

from broker_kis import KISBroker
from kis_broker_factory import build_kis_broker_from_settings
from strategy import SimpleDisclosureStrategy
from universe import LOCAL_LISTING_FALLBACK, build_watchlist_by_marcap
from playwright_client import PlaywrightClient

from utils.slack import notify_disclosure
try:
    from context_alignment import get_symbol_trade_context
except Exception:
    from Disclosure.context_alignment import get_symbol_trade_context
from signals.dart_sales_variation import (
    is_sales_variation_report_title,
    analyze_sales_variation_with_page,
)
from signals.dart_buyback import (
    is_buyback_report_title,
    analyze_fast_buyback,
)
from signals.dart_recovery import DisclosureRecoveryMonitor
from disclosure_event_pipeline import DisclosureEventLogger
from dart_fast_fetch_v3 import fetch_dart_html_fast
from engine_event_processors import (
    handle_buyback,
    handle_perf_report,
    handle_sales_variation,
    handle_supply_contract,
)
from engine_monitor_methods import (
    monitor_and_sell,
    monitor_bootstrap_position_monitors,
    monitor_ensure_thread,
    monitor_fetch_positions,
    monitor_force_eod_liquidate,
    monitor_get_tp_sl,
    monitor_normalize_positions,
    monitor_safe_last_price,
    monitor_safe_sellable_qty,
    monitor_sell_all_with_confirm,
    monitor_sleep_jitter,
    monitor_try_get_avg_price,
    monitor_update_trailing_stop,
)
from engine_runtime_methods import (
    background_recovery_scan,
    run_burst_poll,
    scan_new_disclosures,
    start_monitors,
)
from engine_trade_methods import execute_trade, get_tick_size
from engine_html_client import DARTRecentHtmlClient, date_to_select_date, norm_company_key

log = logging.getLogger("disclosure.engine")
_WARN_THROTTLE: dict[str, float] = {}

KST = dt.timezone(dt.timedelta(hours=9), name="KST")

try:
    import FinanceDataReader as fdr  # [추가]
except Exception:
    fdr = None


def _warn_throttled(key: str, message: str, *args, interval_sec: float = 300.0) -> None:
    now_ts = time.time()
    last_ts = _WARN_THROTTLE.get(key, 0.0)
    if now_ts - last_ts < interval_sec:
        return
    _WARN_THROTTLE[key] = now_ts
    log.warning(message, *args)


class _LazyPageProxy:
    def __init__(self, client: PlaywrightClient):
        self._client = client
        self._page_cm = None
        self._page = None

    def _ensure_page(self):
        if self._page is None:
            self._page_cm = self._client.page()
            self._page = self._page_cm.__enter__()
        return self._page

    def close(self) -> None:
        if self._page_cm is None:
            return
        try:
            self._page_cm.__exit__(None, None, None)
        except Exception as exc:
            _warn_throttled("lazy_page_close", "[PLAYWRIGHT] lazy page close failed: %s", str(exc)[:160], interval_sec=60.0)
        self._page_cm = None
        self._page = None

    def __getattr__(self, name):
        return getattr(self._ensure_page(), name)

# ==============================================================================
# 🚀 KIND Fast-Track: 초고속 공시 파싱 유틸리티 (Playwright 대체용)
# ==============================================================================
def _looks_like_valid_disclosure_html(html: Optional[str]) -> bool:
    if not html:
        return False
    text = html.strip()
    if len(text) < 1000:
        return False
    lowered = text.lower()
    if "문서가 존재하지 않습니다" in text:
        return False
    return any(marker in lowered for marker in ("<html", "<body", "<table", "<iframe"))


def fetch_kind_fast_track(rcp_no: str) -> Optional[str]:
    """
    Playwright 대신 requests 기반 고속 파서를 사용합니다.
    현재는 DART direct fetch를 우선 사용하고, 실패할 때만 KIND 우회를 시도합니다.
    """
    dart_html = fetch_dart_html_fast(rcp_no)
    if _looks_like_valid_disclosure_html(dart_html):
        return dart_html

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/main.do"
    }
    try:
        # [Step 1] 메인 페이지에서 docNo(문서 고유번호) 추출
        url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={rcp_no}"
        resp_main = session.get(url, headers=headers, timeout=2)
        
        # <option value='2026...|Y'> 패턴 찾기
        doc_no_match = re.search(r"value=['\"](\d+)\|Y['\"]", resp_main.text)
        if not doc_no_match: return None
        doc_no = doc_no_match.group(1)

        # [Step 2] 브라우저 내부 호출(searchContents)을 흉내내어 진짜 .htm 주소 획득
        search_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&acptno={rcp_no}&docNo={doc_no}"
        resp_search = session.get(search_url, headers=headers, timeout=2)
        
        final_url_match = re.search(r"['\"](https?://kind\.krx\.co\.kr/external/[^'\"]+\.htm)['\"]", resp_search.text)
        if not final_url_match:
            return None
        
        # [Step 3] 최종 데이터 다운로드
        target_url = final_url_match.group(1)
        resp_final = session.get(target_url, headers=headers, timeout=2)
        resp_final.encoding = resp_final.apparent_encoding
        html = resp_final.text
        if _looks_like_valid_disclosure_html(html):
            return html

        return None

    except Exception as e:
        _warn_throttled(f"kind_fast_track:{rcp_no}", "[KIND] fast-track fetch failed for %s: %s", rcp_no, str(e)[:160], interval_sec=120.0)
        return None
class DisclosureEngine:
    def __init__(self, watch_stocks: Optional[List[str]] = None):
        # 5% 하락 후 2.5% 반등 시 매수, 감시 시간은 120분(2시간)으로 넉넉하게 설정!
        self.recovery_monitor = DisclosureRecoveryMonitor(
            required_drop_pct=7.0, 
            required_bounce_pct=3.0, 
            window_minutes=120,
            required_recovery_ratio=0.5,
        )
        self.event_logger = DisclosureEventLogger()
        # ✅ watch 자동 생성
        if not watch_stocks:
            min_marcap = getattr(SETTINGS, "WATCH_MIN_MARCAP", 500 * 100_000_000)
            top_n = getattr(SETTINGS, "WATCH_TOP_N", None)

            self.watch_map = build_watchlist_by_marcap(
                min_marcap_krw=min_marcap,
                markets=("KOSPI", "KOSDAQ"),
                top_n=top_n,
                exclude_name_keywords=getattr(SETTINGS, "EXCLUDED_NAME_KEYWORDS", []),
                exclude_exact_suffixes=getattr(SETTINGS, "EXCLUDED_EXACT_SUFFIXES", []),
                return_type="map",
            )
        else:
            if isinstance(watch_stocks, dict):
                self.watch_map = watch_stocks
            else:
                self.watch_map = {c: c for c in watch_stocks}

        self.watch = list(self.watch_map.keys())
        # =================================================================
        # [추가] 전체 종목 시가총액 정보 로딩 (로그 출력용)
        # =================================================================
        log.info("📊 [INIT] Loading full market cap data for logging...")
        try:
            # KRX 전체 종목 리스트 가져오기 (가격, 시총 포함)
            if fdr is None:
                raise RuntimeError("FinanceDataReader unavailable")
            df_krx = fdr.StockListing('KRX')

            # { '005930': 450000000000, ... } 형태로 변환
            # 'Code'가 인덱스가 되도록 하고 'Marcap' 컬럼만 딕셔너리로
            self.marcap_cache = df_krx.set_index('Code')['Marcap'].to_dict()

            log.info(f"📊 [INIT] Loaded {len(self.marcap_cache)} stocks marcap info.")
        except Exception as e:
            self.marcap_cache = {}
            try:
                if os.path.exists(LOCAL_LISTING_FALLBACK):
                    df_krx = pd.read_csv(LOCAL_LISTING_FALLBACK, dtype={"Code": str})
                    if "Code" in df_krx.columns and "Marcap" in df_krx.columns:
                        df_krx["Code"] = df_krx["Code"].astype(str).str.zfill(6)
                        df_krx["Marcap"] = pd.to_numeric(df_krx["Marcap"], errors="coerce")
                        df_krx = df_krx.dropna(subset=["Code", "Marcap"]).copy()
                        self.marcap_cache = df_krx.set_index("Code")["Marcap"].to_dict()
                        log.warning(
                            "⚠️ [INIT] Failed to load live market cap (%s). Using local fallback: %s (%d rows)",
                            e,
                            LOCAL_LISTING_FALLBACK,
                            len(self.marcap_cache),
                        )
                    else:
                        log.warning(f"⚠️ [INIT] Failed to load market cap: {e}")
                else:
                    log.warning(f"⚠️ [INIT] Failed to load market cap: {e}")
            except Exception as fallback_exc:
                log.warning(f"⚠️ [INIT] Failed to load market cap fallback: {fallback_exc}")

        log.info("watchlist ready: size=%d", len(self.watch))

        # ✅ 이름 기반 매칭 인덱스
        self.name_to_code: Dict[str, str] = {}
        self.name_collisions: Dict[str, List[str]] = {}
        for code, name in (self.watch_map or {}).items():
            k = norm_company_key(name)
            if not k:
                continue
            if k in self.name_to_code and self.name_to_code[k] != code:
                self.name_collisions.setdefault(k, []).extend([self.name_to_code[k], code])
            else:
                self.name_to_code[k] = code

        if self.name_collisions:
            log.warning("[NAME_MAP] collisions=%d (will use first match)", len(self.name_collisions))

        # --- state ---
        self.state = MongoStateStore(SETTINGS.MONGO_URI, SETTINGS.DB_NAME, SETTINGS.STATE_COLLECTION)
        st = self.state.load()
        self.seen_rcp: Dict = st.get("seen_rcp", {})
        self.corp_map: Dict = st.get("corp_map", {})

        # --- consensus ---
        self.consensus = MongoConsensusRepo(
            SETTINGS.MONGO_URI, SETTINGS.DB_NAME, SETTINGS.CONSENSUS_COLLECTION
        )

        # --- broker ---
        broker_is_virtual = bool(getattr(SETTINGS, "KIS_IS_VIRTUAL", False))
        broker_dry_run = bool(getattr(SETTINGS, "DRY_RUN", False))
        self.broker_live = build_kis_broker_from_settings(
            is_virtual=broker_is_virtual,
            dry_run=broker_dry_run,
        )
        log.info(
            "[BROKER] trade broker initialized | is_virtual=%s | dry_run=%s | account=%s-%s",
            broker_is_virtual,
            broker_dry_run,
            str(getattr(self.broker_live, "cano", "") or ""),
            str(getattr(self.broker_live, "acnt_prdt_cd", "") or ""),
        )
        if not broker_is_virtual and not broker_dry_run:
            log.warning("[BROKER] real-money live trading mode is enabled")

        self.strategy = SimpleDisclosureStrategy(max_krw_per_trade=SETTINGS.MAX_KRW_PER_TRADE)
        self._runtime_trade_profiles: Dict[str, Dict[str, Any]] = {}

        # ✅ Playwright Client
        self.pw_headless = PlaywrightClient.from_settings(
            SETTINGS,
            headless=True,
            slow_mo_ms=0,
            block_resources=bool(getattr(SETTINGS, "PLAYWRIGHT_BLOCK_RESOURCES", True)),
        )
        self.pw_debug: Optional[PlaywrightClient] = None

        # ✅ HTML client
        self.html = DARTRecentHtmlClient(
            timeout_sec=float(getattr(SETTINGS, "HTML_TIMEOUT_SEC", 10.0)),
            max_retries=int(getattr(SETTINGS, "HTML_MAX_RETRIES", 3)),
            min_backoff_sec=float(getattr(SETTINGS, "HTML_MIN_BACKOFF_SEC", 0.6)),
            max_backoff_sec=float(getattr(SETTINGS, "HTML_MAX_BACKOFF_SEC", 4.0)),
        )

        # ✅ 모니터(잔고/TP/SL/EOD) 관리 상태
        self._monitor_lock = threading.Lock()
        self._monitor_threads: Dict[str, threading.Thread] = {}
        self._monitor_last_bootstrap_at: Optional[dt.datetime] = None

        # ✅ 포지션 조회 메서드 캐시 (최초 1회만 로깅)
        self._found_pos_method: Optional[str] = None

        # ✅ 모니터 파라미터(기본값 안전 설정)
        self._monitor_bootstrap_cooldown_sec = int(getattr(SETTINGS, "MONITOR_BOOTSTRAP_COOLDOWN_SEC", 30))
        self._monitor_price_interval_sec = float(getattr(SETTINGS, "MONITOR_PRICE_INTERVAL_SEC", 2.0))
        self._monitor_qty_refresh_sec = float(getattr(SETTINGS, "MONITOR_QTY_REFRESH_SEC", 60.0))
        self._monitor_zero_confirm_n = int(getattr(SETTINGS, "MONITOR_ZERO_CONFIRM_N", 10))
        self._monitor_call_retries = int(getattr(SETTINGS, "MONITOR_CALL_RETRIES", 2))
        self._monitor_max_price_fail = int(getattr(SETTINGS, "MONITOR_MAX_PRICE_FAIL", 50))

        # [추가] 종목별 쿨다운 기록용 딕셔너리
        self.buy_cooldown_map: Dict[str, float] = {}
        
        # [추가] 재매수 금지 시간 (초 단위, 5분 = 300초)
        # 3분이면 연달아 올라오는 정정 공시 등은 다 피할 수 있습니다.
        self.BUY_COOLDOWN_SECONDS = 300.0 
        self._yesterday_surge_cache: Dict[Tuple[str, str], Tuple[bool, Optional[float]]] = {}
        self._consensus_cache: Dict[str, Dict[str, Any]] = {}

        log.info(
            "engine init: watch=%d seen_rcp=%d corp_map=%d (BURST MODE ONLY)",
            len(self.watch),
            len(self.seen_rcp),
            len(self.corp_map),
        )

    # --------------------
    # Playwright client
    # --------------------
    def _get_pw(self, show_browser: bool) -> PlaywrightClient:
        if not show_browser:
            return self.pw_headless

        if self.pw_debug is None:
            self.pw_debug = PlaywrightClient.from_settings(
                SETTINGS,
                headless=False,
                slow_mo_ms=int(getattr(SETTINGS, "PLAYWRIGHT_SLOWMO_MS", 50)),
                block_resources=bool(getattr(SETTINGS, "PLAYWRIGHT_DEBUG_BLOCK_RESOURCES", False)),
            )
            log.info("[PW] debug client created (headless=False)")
        return self.pw_debug

    def _passes_yesterday_surge_filter(self, stock_code: str) -> Tuple[bool, Optional[float]]:
        cache_key = (dt.date.today().isoformat(), str(stock_code).zfill(6))
        cached = self._yesterday_surge_cache.get(cache_key)
        if cached is not None:
            return cached

        if fdr is None:
            result = (True, None)
            self._yesterday_surge_cache[cache_key] = result
            return result

        try:
            start_d = (dt.date.today() - dt.timedelta(days=7)).strftime("%Y-%m-%d")
            df = fdr.DataReader(stock_code, start_d)
            if df is None or len(df) < 2:
                result = (True, None)
            else:
                if df.index[-1].date() == dt.date.today():
                    prev_day = df.iloc[-2]
                else:
                    prev_day = df.iloc[-1]

                prev_open = float(prev_day["Open"])
                prev_high = float(prev_day["High"])
                surge_rate = ((prev_high - prev_open) / prev_open) * 100.0 if prev_open > 0 else None
                result = (not (surge_rate is not None and surge_rate >= 20.0), surge_rate)
        except Exception as e:
            log.warning("⚠️ [YESTERDAY-SURGE-CHECK-FAIL] %s -> 일단 진행", str(e)[:180])
            result = (True, None)

        self._yesterday_surge_cache[cache_key] = result
        return result

    def _get_consensus_snapshot(self, stock_code: str) -> Dict[str, Any]:
        stock_code = str(stock_code or "").zfill(6)
        cached = self._consensus_cache.get(stock_code)
        if cached is not None:
            return cached

        try:
            snapshot = self.consensus.get_quarter_consensus(stock_code) or {}
        except Exception as e:
            log.warning("[CONSENSUS-CACHE] %s fetch failed: %s", stock_code, str(e)[:160])
            snapshot = {}

        self._consensus_cache[stock_code] = snapshot
        return snapshot

    def close(self):
        try:
            if self.pw_debug is not None:
                self.pw_debug.close()
                self.pw_debug = None
        except Exception as exc:
            _warn_throttled("engine_close_debug", "[ENGINE] pw_debug close failed: %s", str(exc)[:160], interval_sec=60.0)
        try:
            if self.pw_headless is not None:
                self.pw_headless.close()
        except Exception as exc:
            _warn_throttled("engine_close_headless", "[ENGINE] pw_headless close failed: %s", str(exc)[:160], interval_sec=60.0)

    def __del__(self):
        try:
            self.close()
        except Exception as exc:
            _warn_throttled("engine_del_close", "[ENGINE] __del__ close failed: %s", str(exc)[:160], interval_sec=60.0)

    # --------------------
    # Helpers
    # --------------------
    def _date_range_weeks(self, weeks: int) -> Tuple[dt.date, dt.date]:
        today = dt.date.today()
        bgn = today - dt.timedelta(days=7 * weeks)
        end = today
        return bgn, end

    def _ensure_corp_codes(self):
        if not self.corp_map:
            log.info("corp_code cache empty (HTML mode): skip filling corp_codes")
        else:
            log.info("corp_code cache present (HTML mode): size=%d", len(self.corp_map))

    def _match_company_to_code(self, company_name: str) -> Optional[str]:
        # ✅ NameError 수정: name -> company_name
        k = norm_company_key(company_name)
        if not k:
            return None
        return self.name_to_code.get(k)

    def _now_kst(self) -> dt.datetime:
        return dt.datetime.now(KST)

    def _is_nxt_session(self) -> bool:
        now = self._now_kst()
        # 15:30 ~ 20:00 사이를 야간 세션으로 간주
        if (now.hour == 15 and now.minute >= 30) or (16 <= now.hour < 20):
            return True
        return False

    # --------------------
    # (Optional) Bootstrap: 과거 공시 훑기 (거래 없음)
    # --------------------
    def bootstrap(self, weeks: int = 4):
        """
        과거 N주 기간을 날짜별로 훑어서:
        - 대상 공시(수주/실적) 파싱/분석을 시도하고
        - seen_rcp에 기록만 남김
        - ✅ 거래는 절대 하지 않음 (broker=None, allow_trade=False)
        """
        self._ensure_corp_codes()

        show_browser = bool(getattr(SETTINGS, "BOOTSTRAP_SHOW_BROWSER", False))
        ignore_seen = bool(getattr(SETTINGS, "BOOTSTRAP_IGNORE_SEEN", True))
        verbose = bool(getattr(SETTINGS, "BOOTSTRAP_VERBOSE", False))
        max_pages_per_day = int(getattr(SETTINGS, "HTML_BOOTSTRAP_MAX_PAGES_PER_DAY", 6))
        respect_total_pages = bool(getattr(SETTINGS, "HTML_RESPECT_TOTAL_PAGES", True))

        bgn_d, end_d = self._date_range_weeks(weeks)

        log.info(
            "bootstrap start (NO-TRADE): weeks=%d range=%s~%s watch=%d",
            weeks, date_to_select_date(bgn_d), date_to_select_date(end_d), len(self.watch)
        )

        watch_set = set(self.watch)
        cur = end_d
        days = 0
        t0 = time.time()

        while cur >= bgn_d:
            days += 1
            select_date = date_to_select_date(cur)

            try:
                items = self.html.fetch_items_for_date(
                    cur,
                    max_pages=max_pages_per_day,
                    respect_total_pages=respect_total_pages,
                )
            except Exception as e:
                log.warning("[BOOTSTRAP] date=%s err=%s", select_date, str(e)[:200])
                cur -= dt.timedelta(days=1)
                continue

            for it in items:
                code = self._match_company_to_code(it.company)
                if not code or code not in watch_set:
                    continue

                if code not in watch_set:
                    # [수정] 시가총액 정보 조회 및 포맷팅 (억 단위)
                    marcap_raw = self.marcap_cache.get(code, 0)
                    marcap_str = "N/A"
                    
                    if marcap_raw > 0:
                        # 1억 단위로 변환 (반올림)
                        marcap_uk = round(marcap_raw / 100_000_000)
                        marcap_str = f"{marcap_uk:,}억"

                    log.info(f"🚫 [WATCH-SKIP] {it.company}({code}) | 시총: {marcap_str} (기준 미달?)")
                    continue

                title = it.title or ""
                rcp_no = it.rcp_no
                if not rcp_no:
                    continue

                if classify_disclosure_event(title) is None:
                    continue

                seen_key = f"{code}:{rcp_no}"
                already_seen = (seen_key in self.seen_rcp)

                if (not ignore_seen) and already_seen:
                    continue

                if verbose:
                    log.info("[BOOTSTRAP] %s %s %s", it.rcv_date, code, title)

                try:
                    # ✅ NO-TRADE 강제
                    _ = self._process_one(
                        broker=None, stock_code=code, rcp_no=rcp_no,
                        title=title, src=f"BOOTSTRAP_HTML({it.rcv_date})",
                        allow_trade=False, show_browser=show_browser,
                        corp_name=it.company,
                        event_date=it.rcv_date,
                        event_time_hhmm=it.time_hhmm,
                    )

                    if not already_seen:
                        self.state.mark_seen(self.seen_rcp, code, rcp_no, ok=True, title=title, src="BOOTSTRAP_HTML")
                        self.state.save(self.seen_rcp, self.corp_map)

                except Exception as e:
                    log.exception("[BOOTSTRAP] process fail code=%s rcp=%s", code, rcp_no)
                    if not already_seen:
                        self.state.mark_seen(
                            self.seen_rcp, code, rcp_no, ok=False, title=title, src="BOOTSTRAP_HTML", err=str(e)[:200]
                        )
                        self.state.save(self.seen_rcp, self.corp_map)

            if days == 1 or days % 5 == 0 or cur == bgn_d:
                log.info("bootstrap progress: day=%s", select_date)
            cur -= dt.timedelta(days=1)

        log.info("bootstrap done: days=%d elapsed=%.1fs", days, time.time() - t0)

    # -------------------------------------------------------------------------
    # Public API: Monitors & Burst Mode
    # -------------------------------------------------------------------------
    def start_monitors(self, broker: Optional[KISBroker] = None):
        start_monitors(self, broker)

    def run_burst_poll(self, broker: Optional[KISBroker] = None, duration_sec: float = 30.0):
        run_burst_poll(self, broker, duration_sec)

    def _record_disclosure_event(
        self,
        *,
        stock_code: str,
        corp_name: str,
        rcp_no: str,
        title: str,
        src: str,
        event_type: str,
        signal_bias: str,
        reason: str,
        metrics: Optional[Dict[str, Any]] = None,
        strategy_name: str = "observe_only",
        trade_executed: bool = False,
        trade_action: str = "",
        allow_trade: bool = False,
        recovery_started: bool = False,
        event_date: str = "",
        event_time_hhmm: str = "",
        initial_price: Optional[float] = None,
        custom_tags: Optional[List[str]] = None,
    ) -> None:
        payload = {
            "stock_code": str(stock_code or "").zfill(6),
            "corp_name": corp_name or self.watch_map.get(stock_code, stock_code),
            "rcp_no": rcp_no,
            "title": title,
            "src": src,
            "event_type": event_type or "UNCLASSIFIED",
            "signal_bias": signal_bias,
            "reason": reason,
            "strategy_name": strategy_name,
            "trade_executed": bool(trade_executed),
            "trade_action": trade_action or "",
            "allow_trade": bool(allow_trade),
            "recovery_started": bool(recovery_started),
            "event_date": (event_date or str(dt.date.today())).replace(".", "-"),
            "event_time_hhmm": event_time_hhmm or "",
            "initial_price": initial_price,
            "market_cap_krw": self.marcap_cache.get(stock_code),
            "metrics": metrics or {},
            "tags": custom_tags or [],
        }
        try:
            self.event_logger.append(payload)
        except Exception as exc:
            log.warning("[EVENT-LOG-FAIL] %s %s: %s", stock_code, event_type, exc)

    # -------------------------------------------------------------------------
    # Internal Logic (Hidden)
    # -------------------------------------------------------------------------
    def _scan_new_disclosures(
        self,
        broker: KISBroker,
        max_pages_override: Optional[int] = None,
        skip_monitor: bool = False,
        pw_override: Optional[PlaywrightClient] = None
    ):
        scan_new_disclosures(self, broker, max_pages_override=max_pages_override, skip_monitor=skip_monitor, pw_override=pw_override)

    def background_recovery_scan(self, broker: KISBroker):
        background_recovery_scan(self, broker)

    # --------------------
    # Core processing
    # --------------------
    def _process_one(
        self,
        broker: Optional[KISBroker],
        stock_code: str,
        rcp_no: str,
        title: str,
        src: str,
        allow_trade: bool,
        show_browser: bool = False,
        pw_override: Optional[PlaywrightClient] = None,
        corp_name: str = "",
        event_date: str = "",
        event_time_hhmm: str = "",
    ) -> bool:
        # =================================================================
        # 🚫 [강력 필터] 무거운 산업군(건설, 물산, 상사 등) 원천 차단
        # =================================================================
        EXCLUDE_WORDS = [
            "건설", "물산", "상사", "엔지니어링", "개발", "토목", "플랜트", "유통", "물류",
            "자회사의 주요경영사항", "자회사의주요경영사항", "지주", "홀딩스"
        ]
        
        corp_name = corp_name or self.watch_map.get(stock_code, "")
        event_type = classify_disclosure_event(title) or "UNCLASSIFIED"
        runtime_flags = {"recovery_started": False}
        context_meta = get_symbol_trade_context(stock_code)

        def _finish(
            traded: bool,
            *,
            signal_bias: str,
            reason: str,
            metrics: Optional[Dict[str, Any]] = None,
            strategy_name: str = "observe_only",
            trade_action: str = "",
            initial_price: Optional[float] = None,
            custom_event_type: Optional[str] = None,
            custom_tags: Optional[List[str]] = None,
        ) -> bool:
            merged_metrics = dict(metrics or {})
            if context_meta.get("sector"):
                merged_metrics.setdefault("context_sector", context_meta.get("sector"))
            merged_metrics.setdefault("context_alignment_label", context_meta.get("alignment_label"))
            merged_metrics.setdefault("context_alignment_score", int(context_meta.get("alignment_score", 0) or 0))
            merged_metrics.setdefault("context_market_mode", context_meta.get("market_mode"))
            if context_meta.get("note"):
                merged_metrics.setdefault("context_note", context_meta.get("note"))
            merged_tags = list(custom_tags or [])
            if context_meta.get("sector"):
                merged_tags.append(f"context_sector:{context_meta.get('sector')}")
            alignment_score = int(context_meta.get("alignment_score", 0) or 0)
            if alignment_score > 0:
                merged_tags.append("context_aligned")
            elif alignment_score < 0:
                merged_tags.append("context_caution")
            merged_tags = list(dict.fromkeys(tag for tag in merged_tags if tag))
            self._record_disclosure_event(
                stock_code=stock_code,
                corp_name=corp_name,
                rcp_no=rcp_no,
                title=title,
                src=src,
                event_type=custom_event_type or event_type,
                signal_bias=signal_bias,
                reason=reason,
                metrics=merged_metrics,
                strategy_name=strategy_name,
                trade_executed=traded,
                trade_action=trade_action,
                allow_trade=allow_trade,
                recovery_started=runtime_flags["recovery_started"],
                event_date=event_date,
                event_time_hhmm=event_time_hhmm,
                initial_price=initial_price,
                custom_tags=merged_tags,
            )
            return traded
        
        # 기업명이나 공시 제목에 제외 키워드 검사
        if any(word in corp_name or word in title for word in EXCLUDE_WORDS):
            log.info(f"🚫 [SKIP] {corp_name} | 제외 키워드 발견: {title}")
            return _finish(False, signal_bias="skip", reason="excluded_keyword", strategy_name="filtered_out")
        
        # =================================================================
        # 🛡️ [쿨다운 체크] 최근 매수 종목 스킵 (CPU/돈 낭비 방지)
        # =================================================================
        last_buy_time = self.buy_cooldown_map.get(stock_code, 0.0)
        elapsed_sec = time.time() - last_buy_time
        
        if elapsed_sec < self.BUY_COOLDOWN_SECONDS:
            log.warning(f"❄️ [COOLDOWN] {stock_code} 최근 매수({int(elapsed_sec)}초 전)로 스킵. (제목: {title})")
            return _finish(False, signal_bias="skip", reason=f"cooldown_{int(elapsed_sec)}s", strategy_name="cooldown")

        def _guard_yesterday_surge(strategy_name: str, metrics: Optional[Dict[str, Any]] = None) -> Optional[bool]:
            passed, surge_rate = self._passes_yesterday_surge_filter(stock_code)
            if passed:
                return None
            merged_metrics = dict(metrics or {})
            if surge_rate is not None:
                merged_metrics["prev_day_surge_rate_pct"] = round(surge_rate, 4)
            log.warning(
                "🚫 [YESTERDAY-SURGE-SKIP] %s 어제 20%% 이상 고가 달성(%.1f%%) -> 매수 스킵",
                stock_code,
                surge_rate or 0.0,
            )
            return _finish(
                False,
                signal_bias="skip",
                reason=f"yesterday_surge_{(surge_rate or 0.0):.1f}",
                strategy_name=strategy_name,
                metrics=merged_metrics,
            )

        # Playwright 준비 (Fallback용)
        if pw_override:
            pw = pw_override
        else:
            pw = self._get_pw(show_browser=show_browser)

        nav_timeout_ms = int(getattr(SETTINGS, "PLAYWRIGHT_NAV_TIMEOUT_MS", 15000))
        page = _LazyPageProxy(pw)

        def _mark_recovery(started: bool) -> None:
            runtime_flags["recovery_started"] = runtime_flags["recovery_started"] or bool(started)

        try:
            for handler in (
                handle_supply_contract,
                handle_perf_report,
                handle_sales_variation,
                handle_buyback,
            ):
                result = handler(
                    engine=self,
                    broker=broker,
                    stock_code=stock_code,
                    rcp_no=rcp_no,
                    title=title,
                    src=src,
                    allow_trade=allow_trade,
                    page=page,
                    pw=pw,
                    nav_timeout_ms=nav_timeout_ms,
                    event_type=event_type,
                    finish=_finish,
                    guard_yesterday_surge=_guard_yesterday_surge,
                    fast_fetch_html=fetch_kind_fast_track,
                    mark_recovery=_mark_recovery,
                )
                if result is not None:
                    return result

            if event_type in ("DILUTION", "LARGE_HOLDER", "INSIDER_OWNERSHIP", "SUPPLY_TERMINATION", "SUPPLY_UPDATE", "CORRECTION"):
                default_bias = "negative" if event_type in ("DILUTION", "SUPPLY_TERMINATION") else "neutral"
                return _finish(False, signal_bias=default_bias, reason="classified_only", strategy_name="observe_only")
        except Exception as e:
            log.error("[CRITICAL-ERR] %s 분석 중 오류: %s", stock_code, str(e)[:200])
            raise
        finally:
            page.close()

        return _finish(False, signal_bias="neutral", reason="no_matching_handler", strategy_name="observe_only")

    # ---------------------------------------------------------------------
    # Execute & Monitor
    # ---------------------------------------------------------------------
    def _monitor_enabled(self) -> bool:
        return bool(getattr(SETTINGS, "ENABLE_POSITION_MONITOR", True))
    
    # 💡 가격대별 호가 단위(Tick Size)
    def _get_tick_size(self, price: float) -> int:
        return get_tick_size(price)

     # 💡 주문 집행 (NXT 매수 시 5호가 위로 주문)
    def _execute(
        self, 
        broker: KISBroker, 
        symbol: str, 
        decision, 
        src: str, 
        tp_hint: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        trade_context = dict(context or get_symbol_trade_context(symbol))
        runtime_profile = self._get_runtime_trade_profile(symbol)
        monitor_policy: Optional[Dict[str, Any]] = None
        if runtime_profile:
            note_parts = [str(trade_context.get("note") or "").strip()]
            profile_decision = str(runtime_profile.get("decision") or "").strip()
            profile_reason = str(runtime_profile.get("reason") or "").strip()
            budget_krw = int(runtime_profile.get("budget_krw", 0) or 0)
            support_score = int(runtime_profile.get("support_score", 0) or 0)
            ranking_score = float(runtime_profile.get("ranking_score", 0.0) or 0.0)
            runtime_tp_pct = float(runtime_profile.get("take_profit_pct", 0.0) or 0.0)
            runtime_sl_pct = float(runtime_profile.get("stop_loss_pct", 0.0) or 0.0)
            runtime_grace_min = int(runtime_profile.get("stop_grace_min", 0) or 0)
            runtime_open_recovery_min = int(runtime_profile.get("open_recovery_min", 0) or 0)
            entry_style = str(runtime_profile.get("entry_style") or "").strip()
            entry_date = str(runtime_profile.get("entry_date") or "").strip()
            skip_same_day_eod = bool(runtime_profile.get("skip_same_day_eod_liquidate", False))
            hybrid_sector = float(runtime_profile.get("hybrid_sector_regime_score", 0.0) or 0.0)
            hybrid_relative = float(runtime_profile.get("hybrid_relative_value_score", 0.0) or 0.0)
            hybrid_timing = float(runtime_profile.get("hybrid_timing_score", 0.0) or 0.0)
            hybrid_final = float(runtime_profile.get("hybrid_final_trade_score", 0.0) or 0.0)
            hybrid_decision = str(runtime_profile.get("hybrid_shadow_decision") or "").strip()
            close_swing_bits = []
            if profile_decision:
                close_swing_bits.append(profile_decision)
            if profile_reason:
                close_swing_bits.append(profile_reason)
            if support_score:
                close_swing_bits.append(f"score {support_score}")
            if ranking_score:
                close_swing_bits.append(f"rank {ranking_score:.1f}")
            if budget_krw:
                close_swing_bits.append(f"budget {budget_krw // 10000}만")
            if runtime_tp_pct:
                close_swing_bits.append(f"tp {runtime_tp_pct:.1f}%")
            if runtime_sl_pct:
                close_swing_bits.append(f"sl {runtime_sl_pct:.1f}%")
            if close_swing_bits:
                note_parts.append("close-swing " + " | ".join(close_swing_bits))
            hybrid_bits = []
            if hybrid_sector:
                hybrid_bits.append(f"sector {hybrid_sector:.1f}")
            if hybrid_relative:
                hybrid_bits.append(f"relative {hybrid_relative:.1f}")
            if hybrid_timing:
                hybrid_bits.append(f"timing {hybrid_timing:.1f}")
            if hybrid_final:
                hybrid_bits.append(f"final {hybrid_final:.1f}")
            if hybrid_decision:
                hybrid_bits.append(hybrid_decision)
            if hybrid_bits:
                note_parts.append("hybrid " + " | ".join(hybrid_bits))
            trade_context["note"] = " | ".join(part for part in note_parts if part)
            trade_context["hybrid_sector_regime_score"] = hybrid_sector
            trade_context["hybrid_relative_value_score"] = hybrid_relative
            trade_context["hybrid_timing_score"] = hybrid_timing
            trade_context["hybrid_final_trade_score"] = hybrid_final
            trade_context["blocked_reason_code"] = str(runtime_profile.get("hybrid_blocked_reason_code") or "")
            trade_context["quote_source"] = str(runtime_profile.get("quote_source") or trade_context.get("quote_source") or "")
            if skip_same_day_eod:
                monitor_policy = {
                    "entry_style": entry_style or "close_bet",
                    "entry_date": entry_date or self._now_kst().strftime("%Y-%m-%d"),
                    "skip_same_day_eod_liquidate": True,
                    "take_profit_pct": runtime_tp_pct or None,
                    "stop_loss_pct": runtime_sl_pct or None,
                    "require_recover_by_close": bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_BY_CLOSE", True)),
                    "require_recover_after_open": bool(getattr(SETTINGS, "CLOSE_BET_REQUIRE_RECOVER_AFTER_OPEN", True)),
                    "open_recovery_min": runtime_open_recovery_min or int(getattr(SETTINGS, "CLOSE_BET_OPEN_RECOVERY_MIN", 10) or 10),
                    "max_hold_days": int(getattr(SETTINGS, "CLOSE_BET_MAX_HOLD_DAYS", 3) or 3),
                    "time_exit_start": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_START", "15:10") or "15:10"),
                    "time_exit_end": str(getattr(SETTINGS, "CLOSE_BET_TIME_EXIT_END", "15:20") or "15:20"),
                    "force_eod_liquidate": bool(getattr(SETTINGS, "MONITOR_FORCE_EOD_LIQUIDATE", False)),
                    "stop_grace_period_sec": int(runtime_grace_min or int(getattr(SETTINGS, "CLOSE_BET_STOP_GRACE_MIN", 60) or 60)) * 60,
                }
        effective_tp_hint = tp_hint
        if effective_tp_hint is None and runtime_profile:
            runtime_tp_pct = float(runtime_profile.get("take_profit_pct", 0.0) or 0.0)
            effective_tp_hint = runtime_tp_pct or None
        return execute_trade(
            self,
            broker,
            symbol,
            decision,
            src,
            tp_hint=effective_tp_hint,
            context=trade_context,
            monitor_policy=monitor_policy,
        )

    def _set_runtime_trade_profile(self, symbol: str, profile: Optional[Dict[str, Any]]) -> None:
        key = str(symbol or "").zfill(6)
        if not key:
            return
        if not isinstance(profile, dict) or not profile:
            self._runtime_trade_profiles.pop(key, None)
            return
        self._runtime_trade_profiles[key] = dict(profile)

    def _get_runtime_trade_profile(self, symbol: str) -> Dict[str, Any]:
        key = str(symbol or "").zfill(6)
        row = self._runtime_trade_profiles.get(key)
        return dict(row) if isinstance(row, dict) else {}

    def _clear_runtime_trade_profile(self, symbol: str) -> None:
        key = str(symbol or "").zfill(6)
        if key:
            self._runtime_trade_profiles.pop(key, None)

    # ---------------------------------------------------------------------
    # Monitor helpers (safe calls / throttling)
    # ---------------------------------------------------------------------
    def _sleep_jitter(self, base_sec: float, jitter_sec: float = 0.25):
        monitor_sleep_jitter(self, base_sec, jitter_sec)

    def _safe_last_price(self, broker: KISBroker, symbol: str, retries: int = 2) -> Optional[float]:
        return monitor_safe_last_price(self, broker, symbol, retries=retries)

    def _safe_sellable_qty(self, broker: KISBroker, symbol: str, retries: int = 1) -> Optional[int]:
        return monitor_safe_sellable_qty(self, broker, symbol, retries=retries)

    def _sell_all_with_confirm(
        self,
        broker: KISBroker,
        symbol: str,
        qty: int,
        tag: str,
        note: str,
        confirm_retries: int = 1,
    ) -> bool:
        return monitor_sell_all_with_confirm(
            self,
            broker,
            symbol,
            qty,
            tag,
            note,
            confirm_retries=confirm_retries,
        )

    def _get_tp_sl(self) -> Tuple[float, float]:
        return monitor_get_tp_sl(self)

    # ---------------------------------------------------------------------
    # Monitor: start-of-run holdings scan + per-symbol thread
    # ---------------------------------------------------------------------
    def _bootstrap_position_monitors(self, broker: KISBroker) -> None:
        monitor_bootstrap_position_monitors(self, broker)

    def _normalize_positions(self, raw: Any) -> List[Dict[str, Any]]:
        return monitor_normalize_positions(self, raw)

    def _fetch_positions(self, broker: KISBroker) -> List[Dict[str, Any]]:
        return monitor_fetch_positions(self, broker)

    def _ensure_monitor(
        self,
        broker: KISBroker,
        symbol: str,
        qty_hint: Optional[int] = None,
        avg_price_hint: Optional[float] = None,
        tp_override: Optional[float] = None,  # <--- 인자 추가
        monitor_policy: Optional[Dict[str, Any]] = None,
    ) -> None:
        monitor_ensure_thread(
            self,
            broker,
            symbol,
            qty_hint=qty_hint,
            avg_price_hint=avg_price_hint,
            tp_override=tp_override,
            monitor_policy=monitor_policy,
        )

    def _try_get_avg_price(self, broker: KISBroker, symbol: str) -> Optional[float]:
        return monitor_try_get_avg_price(self, broker, symbol)

    def _force_eod_liquidate(self, broker: KISBroker, symbol: str, deadline: dt.datetime, qty_fallback: int, tag: str = "SELL(EOD)") -> bool:
        return monitor_force_eod_liquidate(self, broker, symbol, deadline, qty_fallback, tag=tag)
    
    def update_trailing_stop(self, symbol, current_price):
        return monitor_update_trailing_stop(self, symbol, current_price)

    def _monitor_and_sell_v2(
        self,
        broker: KISBroker,
        symbol: str,
        qty_hint: Optional[int],
        avg_price_hint: Optional[float],
        target_pct: float,
        stop_pct: float,
        monitor_policy: Optional[Dict[str, Any]] = None,
    ):
        return monitor_and_sell(self, broker, symbol, qty_hint, avg_price_hint, target_pct, stop_pct, monitor_policy)
