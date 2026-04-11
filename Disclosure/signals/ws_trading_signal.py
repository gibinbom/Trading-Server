import asyncio
import json
import logging
import os
import sys
from time import sleep as blocking_sleep

import websockets


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config import SETTINGS
from kis_broker_factory import build_kis_broker_from_settings
try:
    from ws_signal_analyzer import SignalAnalyzer
    from ws_signal_runtime import _is_scanner_window_open, _kst_now, get_sleep_time_until_morning, slack_sender_worker
    from ws_watchlist import load_watch_map, request_approval_key
except Exception:
    from .ws_signal_analyzer import SignalAnalyzer
    from .ws_signal_runtime import _is_scanner_window_open, _kst_now, get_sleep_time_until_morning, slack_sender_worker
    from .ws_watchlist import load_watch_map, request_approval_key


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("scanner")

MAX_WORKERS = max(1, int(os.getenv("WS_MAX_WORKERS", "1")))
MAX_SYMBOLS_PER_WS = max(100, int(os.getenv("WS_MAX_SYMBOLS_PER_WS", "2000")))
WATCH_TOP_N = max(100, int(os.getenv("WS_WATCH_TOP_N", "2000")))
ANALYZE_CONSUMERS = max(1, int(os.getenv("WS_ANALYZE_CONSUMERS", "4")))
TICK_QUEUE_MAXSIZE = max(1000, int(os.getenv("WS_TICK_QUEUE_MAXSIZE", "20000")))
WS_PING_INTERVAL = max(10, int(os.getenv("WS_PING_INTERVAL", "60")))
WS_PING_TIMEOUT = max(20, int(os.getenv("WS_PING_TIMEOUT", "120")))
WS_URL = "ws://ops.koreainvestment.com:31000" if getattr(SETTINGS, "IS_VIRTUAL", False) else "ws://ops.koreainvestment.com:21000"


async def tick_consumer(consumer_id, tick_queue, analyzer):
    while True:
        tick_data = await tick_queue.get()
        try:
            await asyncio.to_thread(analyzer.analyze_tick, tick_data)
        except Exception as exc:
            log.warning("⚠️ [Tick-%02d] 분석 오류: %s", consumer_id, exc)
        finally:
            tick_queue.task_done()


async def kis_websocket_worker(worker_id, symbols_chunk, approval_key, analyzer, tick_queue):
    tr_volume = "H0UNMBC0"
    tr_price = "H0UNCNT0"
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                max_queue=None,
            ) as websocket:
                log.info("👷 [Worker-%02d] 통합 서버 접속 성공 (대상: %s종목)", worker_id, len(symbols_chunk))
                for symbol in symbols_chunk:
                    for tr_id in (tr_volume, tr_price):
                        await websocket.send(
                            json.dumps(
                                {
                                    "header": {
                                        "approval_key": approval_key,
                                        "custtype": "P",
                                        "tr_type": "1",
                                        "content-type": "utf-8",
                                    },
                                    "body": {"input": {"tr_id": tr_id, "tr_key": symbol}},
                                }
                            )
                        )
                        await asyncio.sleep(0.04)
                log.info("✅ [Worker-%02d] 듀얼 채널(수급+가격) 구독 완료! 감시를 시작합니다.", worker_id)

                while True:
                    if not _is_scanner_window_open():
                        log.info("🌙 [Worker-%02d] 감시 시간이 종료되어 연결을 정리합니다.", worker_id)
                        return
                    try:
                        data = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        continue
                    if not (data.startswith("0|") or data.startswith("1|")):
                        continue
                    parts = data.split("|")
                    if len(parts) < 4:
                        continue
                    tr_id = parts[1]
                    tick_data = parts[3].split("^")
                    if tr_id == tr_price and len(tick_data) >= 3:
                        analyzer.price_cache[tick_data[0]] = analyzer._safe_int(tick_data[2])
                        continue
                    if tr_id == tr_volume:
                        try:
                            tick_queue.put_nowait(tick_data)
                        except asyncio.QueueFull:
                            analyzer.note_tick_drop()
        except Exception as exc:
            log.warning("⚠️ [Worker-%02d] 연결 오류 (%s). 5초 후 재접속...", worker_id, exc)
            await asyncio.sleep(5)


async def main():
    log.info("🕵️‍♂️ All-Day 초고속 통합 스캐너 가동 (H0UNMBC0)")
    watch_map = load_watch_map(cache_file="universe_cache.json", top_n=WATCH_TOP_N)
    target_symbols = list(watch_map.keys())
    try:
        approval_key = request_approval_key()
    except Exception as exc:
        log.error("🚨 네트워크 및 API 키 발급 오류: %s", exc)
        return

    broker = build_kis_broker_from_settings(is_virtual=False, dry_run=True)
    analyzer = SignalAnalyzer(watch_map, broker)
    tick_queue = asyncio.Queue(maxsize=TICK_QUEUE_MAXSIZE)
    chunks = [target_symbols[i:i + MAX_SYMBOLS_PER_WS] for i in range(0, len(target_symbols), MAX_SYMBOLS_PER_WS)]
    worker_count = min(MAX_WORKERS, len(chunks))
    if len(chunks) > MAX_WORKERS:
        covered = sum(len(chunk) for chunk in chunks[:MAX_WORKERS])
        dropped = max(0, len(target_symbols) - covered)
        log.warning(
            "⚠️ 워커 상한으로 일부 종목이 제외됩니다. workers=%d chunk=%d covered=%d dropped=%d",
            MAX_WORKERS,
            MAX_SYMBOLS_PER_WS,
            covered,
            dropped,
        )
    else:
        log.info(
            "📡 WS 워커 구성: workers=%d | chunk=%d | target=%d | analyze_consumers=%d | tick_queue=%d",
            worker_count,
            MAX_SYMBOLS_PER_WS,
            len(target_symbols),
            ANALYZE_CONSUMERS,
            TICK_QUEUE_MAXSIZE,
        )
    tasks = [kis_websocket_worker(i + 1, chunk, approval_key, analyzer, tick_queue) for i, chunk in enumerate(chunks[:worker_count])]
    tasks.extend(tick_consumer(i + 1, tick_queue, analyzer) for i in range(ANALYZE_CONSUMERS))
    tasks.append(slack_sender_worker(analyzer))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    print("\n🚀 [올데이 무한 스캐너] 크론(Cron) 없이 파이썬 자체 스케줄러로 가동합니다!\n")
    while True:
        if not _is_scanner_window_open():
            sleep_sec = get_sleep_time_until_morning()
            hours, remainder = divmod(sleep_sec, 3600)
            minutes, _ = divmod(remainder, 60)
            print(f"🌙 감시 시간이 종료되었습니다. 다음 시작 시각 08:00까지 {int(hours)}시간 {int(minutes)}분 동안 수면 모드에 들어갑니다... zZ")
            blocking_sleep(sleep_sec)
            print(f"\n☀️ 아침이 밝았습니다! 오늘자 유니버스를 갱신하고 스캐너를 재가동합니다. ({_kst_now().strftime('%Y-%m-%d %H:%M:%S %Z')})")
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("🛑 사용자에 의해 강제 종료되었습니다.")
            break
        except Exception as exc:
            print(f"🚨 메인 루프 에러 발생, 1분 후 재시도합니다: {exc}")
            blocking_sleep(60)
