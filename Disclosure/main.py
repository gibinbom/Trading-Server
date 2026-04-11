import asyncio
import logging
import threading

import nest_asyncio

from config import SETTINGS
from engine import DisclosureEngine
from event_trade_watcher import EventLogTradeWatcher
from utils.ls_news import connect_and_listen
from utils.slack import notify_error, notify_main_trader_start, notify_system_start


nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main")


def _start_background_threads(eng: DisclosureEngine) -> None:
    eng.start_monitors()
    log.info("🛡️ Position Monitors Started")

    recovery_thread = threading.Thread(
        target=eng.background_recovery_scan,
        args=(eng.broker_live,),
        daemon=True,
    )
    recovery_thread.start()
    log.info("💥 V-Recovery Re-entry Scanner Started")


def _run_news_burst_mode(eng: DisclosureEngine) -> None:
    def trigger_burst_sync():
        try:
            eng.run_burst_poll(broker=None, duration_sec=30.0)
        except Exception as exc:
            log.error("Burst Poll Error: %s", exc)

    async def trigger_burst_wrapper():
        loop = asyncio.get_running_loop()
        log.info("⚡ Triggering Burst Poll (in background thread)...")
        await loop.run_in_executor(None, trigger_burst_sync)

    log.info("👂 Starting News Listener...")
    asyncio.run(connect_and_listen(on_signal_callback=trigger_burst_wrapper))


def _run_event_log_watch_mode(eng: DisclosureEngine) -> None:
    log.info("📘 Starting Event Log Watch Mode...")
    watcher = EventLogTradeWatcher(eng)
    watcher.run_forever()


def _notify_main_trader_runtime_start(eng: DisclosureEngine, mode: str) -> None:
    broker = getattr(eng, "broker_live", None)
    trade_window = (
        f"{getattr(SETTINGS, 'EVENT_TRADE_WINDOW_START', '-')}"
        f"~{getattr(SETTINGS, 'EVENT_TRADE_WINDOW_END', '-')}"
    )
    notify_main_trader_start(
        main_run_mode=mode,
        auto_trade_enabled=bool(getattr(SETTINGS, "ENABLE_AUTO_TRADE", False)),
        close_swing_enabled=bool(getattr(SETTINGS, "CLOSE_SWING_ENABLE", False)),
        broker_is_virtual=bool(getattr(broker, "is_virtual", getattr(SETTINGS, "KIS_IS_VIRTUAL", False))),
        broker_dry_run=bool(getattr(broker, "dry_run", getattr(SETTINGS, "DRY_RUN", False))),
        trade_window=trade_window,
    )


def main():
    if not SETTINGS.OPEN_DART_API_KEY:
        raise RuntimeError("OPEN_DART_API_KEY env is required")

    log.info("🚀 System Initializing...")
    eng = DisclosureEngine()
    _start_background_threads(eng)

    mode = str(getattr(SETTINGS, "MAIN_RUN_MODE", "event_log_watch") or "event_log_watch").strip().lower()
    log.info("🎛️ Main run mode: %s", mode)
    if mode == "event_log_watch":
        _notify_main_trader_runtime_start(eng, mode)
    else:
        notify_system_start()

    try:
        if mode == "news_burst":
            _run_news_burst_mode(eng)
        elif mode == "event_log_watch":
            _run_event_log_watch_mode(eng)
        else:
            raise RuntimeError(f"Unsupported MAIN_RUN_MODE: {mode}")
    except KeyboardInterrupt:
        log.info("🛑 User Interrupted. Shutting down...")
    except Exception as exc:
        log.critical("Fatal Crash: %s", exc, exc_info=True)
        notify_error("System Fatal Crash", str(exc))


if __name__ == "__main__":
    main()
