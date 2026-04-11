import asyncio
import logging
import os
import sys
from datetime import datetime

try:
    from google import genai
except Exception:  # pragma: no cover - optional runtime dependency
    genai = None


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
trading_root = os.path.dirname(parent_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config import SETTINGS
from slack_log_loader import collect_flow_snapshots, collect_legacy_logs, collect_structured_events
from slack_log_summary import summarize_flow_snapshots, summarize_structured_events
from slack_report_prompt import build_report_prompt
from utils.slack import send_slack


api_key = getattr(SETTINGS, "GEMINI_API_KEY", "")
client = genai.Client(api_key=api_key) if genai and api_key else None
log = logging.getLogger("signals.slack_reporter")
STRUCTURED_LOG_DIR = os.path.join(current_dir, "logs")


def _covered_range(covered_dates: list[str]) -> str:
    if not covered_dates:
        return ""
    start_date = f"{covered_dates[0][:4]}-{covered_dates[0][4:6]}-{covered_dates[0][6:]}"
    end_date = f"{covered_dates[-1][:4]}-{covered_dates[-1][4:6]}-{covered_dates[-1][6:]}"
    return f"{start_date} ~ {end_date} ({len(covered_dates)}일)"


async def analyze_logs(report_type="hourly", days_to_look_back=3):
    structured_events, structured_dates = collect_structured_events(STRUCTURED_LOG_DIR, days_to_look_back)
    flow_snapshots, snapshot_dates = collect_flow_snapshots(STRUCTURED_LOG_DIR, days_to_look_back)
    legacy_log_text, legacy_dates = collect_legacy_logs(trading_root, days_to_look_back)

    structured_summary = summarize_structured_events(structured_events)
    snapshot_summary = summarize_flow_snapshots(flow_snapshots)
    if not structured_summary and not snapshot_summary and not legacy_log_text.strip():
        return "수집된 최근 로그 데이터가 없습니다."
    if client is None:
        if not api_key:
            return "Gemini API 키가 비어 있습니다. `Disclosure/config.py`의 `HARDCODED_GEMINI_API_KEY` 또는 PM2/env의 `GEMINI_API_KEY`를 설정하세요."
        return "`google.genai` 모듈이 없어 AI 리포트를 생성할 수 없습니다."

    covered_dates = snapshot_dates or structured_dates or legacy_dates
    prompt = build_report_prompt(
        report_type,
        _covered_range(covered_dates),
        snapshot_summary,
        structured_summary,
        legacy_log_text,
    )

    try:
        response = await asyncio.to_thread(
            client.models.generate_content,
            model="gemini-2.5-flash",
            contents=prompt,
        )
        return response.text
    except Exception as exc:
        return f"AI 분석 중 에러 발생: {exc}"


async def run_scheduler():
    print("🤖 AI 슬랙 리포터 가동 (1시간 단위 / 15:20, 19:40 마감 브리핑 대기 중)")
    while True:
        now = datetime.now()
        if 9 <= now.hour <= 19 and now.minute == 0:
            print(f"[{now.strftime('%H:%M')}] 📊 1시간 AI 브리핑 준비 중...")
            report = await analyze_logs(report_type="hourly")
            send_slack(report, title=f"AI 퀀트 1시간 수급 브리핑 [{now.strftime('%H:00')}]", msg_type="info")
            await asyncio.sleep(60)
            continue
        if now.hour == 15 and now.minute == 20:
            print(f"[{now.strftime('%H:%M')}] 🔥 마감 직전 종가 베팅 픽 분석 중...")
            report = await analyze_logs(report_type="closing")
            send_slack(report, title="[긴급] 종가 베팅(Overnight) AI 픽", msg_type="success")
            await asyncio.sleep(60)
            continue
        if now.hour == 19 and now.minute == 40:
            print(f"[{now.strftime('%H:%M')}] 🔥 다음날 대비 종가 베팅 픽 분석 중...")
            report = await analyze_logs(report_type="closing")
            send_slack(report, title="다음날 세팅(Overnight) AI 픽", msg_type="success")
            await asyncio.sleep(60)
            continue
        await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(run_scheduler())
