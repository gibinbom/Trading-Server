from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, time, timedelta, timezone

try:
    from utils.slack import send_slack
except Exception:
    from Disclosure.utils.slack import send_slack


log = logging.getLogger("scanner")
KST = timezone(timedelta(hours=9))
SCANNER_START = time(8, 0)
SCANNER_END = time(20, 0)
WS_SIGNAL_SLACK_ENABLED = os.getenv("WS_SIGNAL_SLACK_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}


def _kst_now() -> datetime:
    return datetime.now(KST)


def _is_scanner_window_open(now: datetime | None = None) -> bool:
    now = now or _kst_now()
    return SCANNER_START <= now.time() < SCANNER_END


def get_sleep_time_until_morning(start_hour: int = 8, start_minute: int = 0) -> float:
    now = _kst_now()
    target = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def slack_sender_worker(analyzer) -> None:
    if not WS_SIGNAL_SLACK_ENABLED:
        log.info("🔕 실시간 매집/체결 슬랙 알림은 비활성화 상태입니다. 로컬 로그만 기록합니다.")
        while True:
            if analyzer.slack_queue:
                analyzer.slack_queue.clear()
            await asyncio.sleep(1.0)
    log.info("👷 슬랙 전담 일꾼이 출근했습니다.")
    while True:
        if analyzer.slack_queue:
            job = analyzer.slack_queue.pop(0)
            try:
                await asyncio.to_thread(
                    send_slack,
                    text=job["text"],
                    title="",
                    msg_type=job["msg_type"],
                )
            except Exception as exc:
                log.warning("⚠️ 슬랙 전송 중 지연 발생: %s", exc)
            await asyncio.sleep(0.2)
            continue
        await asyncio.sleep(0.2)
