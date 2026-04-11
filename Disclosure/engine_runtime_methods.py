from __future__ import annotations

import logging
import re
import time
from typing import List, Optional

from config import SETTINGS
from dart_common import classify_disclosure_event
from engine_html_client import HtmlDisclosureItem
from playwright_client import PlaywrightClient
from utils.slack import notify_error


log = logging.getLogger("disclosure.engine.runtime")


def start_monitors(engine, broker=None):
    broker = broker if broker is not None else engine.broker_live
    log.info("🛡️ [MONITOR] Starting background position monitors...")
    engine._bootstrap_position_monitors(broker)


def run_burst_poll(engine, broker=None, duration_sec: float = 30.0):
    broker = broker if broker is not None else engine.broker_live
    deadline = time.time() + duration_sec
    count = 0
    log.warning("🔥🔥 [BURST START] Polling DART continuously for %ss... (Thread Safe Mode)", duration_sec)
    local_pw = PlaywrightClient.from_settings(
        SETTINGS,
        headless=True,
        slow_mo_ms=0,
        block_resources=bool(getattr(SETTINGS, "PLAYWRIGHT_BLOCK_RESOURCES", True)),
    )
    try:
        while time.time() < deadline:
            count += 1
            try:
                engine._scan_new_disclosures(
                    broker=broker,
                    max_pages_override=1,
                    skip_monitor=True,
                    pw_override=local_pw,
                )
            except Exception as exc:
                log.error("[BURST] Scan error: %s", exc)
            time.sleep(0.2)
    finally:
        local_pw.close()
    log.warning("🔥🔥 [BURST END] Finished. Loops executed: %s", count)


def scan_new_disclosures(engine, broker, max_pages_override: Optional[int] = None, skip_monitor: bool = False, pw_override=None):
    if not skip_monitor:
        engine._bootstrap_position_monitors(broker)

    watch_set = set(engine.watch)
    show_browser = bool(getattr(SETTINGS, "LIVE_SHOW_BROWSER", False))
    max_pages = max_pages_override if max_pages_override is not None else int(getattr(SETTINGS, "HTML_LIVE_MAX_PAGES", 1))
    allow_trade = bool(getattr(SETTINGS, "ENABLE_AUTO_TRADE", False) and broker is not None)
    target_date = engine._now_kst().date()

    items_all: List[HtmlDisclosureItem] = []
    try:
        items_all.extend(engine.html.fetch_items_for_date(target_date, max_pages=max_pages, respect_total_pages=False))
    except Exception:
        pass

    def _sort_key(item: HtmlDisclosureItem):
        hhmm = item.time_hhmm if re.match(r"^\d{2}:\d{2}$", item.time_hhmm or "") else "00:00"
        return (item.rcv_date, hhmm, item.rcp_no)

    loop_dedup = set()
    for item in sorted(items_all, key=_sort_key, reverse=True):
        title = item.title or ""
        rcp_no = item.rcp_no
        if not rcp_no:
            continue

        code = engine._match_company_to_code(item.company)
        if not code:
            continue

        if code not in watch_set:
            marcap_raw = engine.marcap_cache.get(code, 0)
            marcap_str = f"{round(marcap_raw / 100_000_000):,}억" if marcap_raw > 0 else "N/A"
            if classify_disclosure_event(title) is not None:
                log.info("🚫 [WATCH-SKIP] %s(%s) | 시총: %s (기준 미달?) | 제목: %s", item.company, code, marcap_str, title)
            continue

        loop_key = f"{code}:{rcp_no}"
        if loop_key in loop_dedup or loop_key in engine.seen_rcp:
            continue
        loop_dedup.add(loop_key)
        log.info("✨ [NEW] %s %s | %s(%s) | %s", item.rcv_date, item.time_hhmm, item.company, code, title)

        if classify_disclosure_event(title) is None:
            engine.state.mark_seen(engine.seen_rcp, code, rcp_no, ok=False, title=title, src="HTML", err="not target")
            continue

        try:
            traded = engine._process_one(
                broker=broker,
                stock_code=code,
                rcp_no=rcp_no,
                title=title,
                src="BURST_HTML",
                allow_trade=allow_trade,
                show_browser=show_browser,
                pw_override=pw_override,
                corp_name=item.company,
                event_date=item.rcv_date,
                event_time_hhmm=item.time_hhmm,
            )
            engine.state.mark_seen(engine.seen_rcp, code, rcp_no, ok=bool(traded), title=title, src="HTML")
        except Exception as exc:
            err_msg = str(exc)[:300]
            engine.state.mark_seen(engine.seen_rcp, code, rcp_no, ok=False, title=title, src="HTML", err=err_msg)
            log.error("❌ [PROCESS-ERR] Code=%s RCP=%s Err=%s", code, rcp_no, err_msg)
            notify_error(f"ProcessOne - {code}", err_msg)
        finally:
            engine.state.save(engine.seen_rcp, engine.corp_map)


def background_recovery_scan(engine, broker):
    log.info("🛡️ [RECOVERY] V-Recovery 백그라운드 감시 프로세스 시작")
    while True:
        try:
            target_symbols = list(engine.recovery_monitor.trackers.keys())
            if not target_symbols:
                time.sleep(10)
                continue

            positions = engine._fetch_positions(broker)
            holding_symbols = {position["symbol"] for position in positions}
            for symbol in target_symbols:
                if symbol in holding_symbols:
                    continue
                curr_px = engine._safe_last_price(broker, symbol)
                if not curr_px:
                    continue
                if engine.recovery_monitor.check_signal(symbol, curr_px):
                    log.info("💥 [RE-ENTRY-SIGNAL] %s V자 원복 포착! 즉시 재진입 매수 시도", symbol)
                    decision = engine.strategy.on_order_hit(
                        broker,
                        symbol,
                        hit=True,
                        reason="V-Recovery Re-entry (Drop & Recover)",
                    )
                    success = engine._execute(broker, symbol, decision, src="RECOVERY_LOGIC")
                    if success:
                        log.info("✅ [RE-ENTRY-SUCCESS] %s 재진입 주문 전송 완료", symbol)
                    else:
                        log.warning("⚠️ [RE-ENTRY-FAIL] %s 재진입 주문 실패 (잔고부족 혹은 API 오류)", symbol)
        except Exception as exc:
            log.error("🚨 [RECOVERY-LOOP-ERR] 감시 루프 중 예상치 못한 오류: %s", exc)
            time.sleep(5)
        time.sleep(5)
