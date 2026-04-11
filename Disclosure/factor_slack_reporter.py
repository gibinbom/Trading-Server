from __future__ import annotations

import argparse
import logging
import time
import warnings
from datetime import datetime

from utils.slack import notify_error, send_slack

try:
    from config import SETTINGS
except Exception:
    SETTINGS = None

try:
    from google import genai
except Exception:
    genai = None

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        import google.generativeai as google_generativeai
except Exception:
    google_generativeai = None


log = logging.getLogger("disclosure.factor_slack")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build factor snapshots and send structured/AI reports to Slack.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--with-ai", action="store_true", help="Send an additional AI interpretation after the structured digest.")
    parser.add_argument("--print-only", action="store_true", help="Print the digest locally instead of sending to Slack.")
    parser.add_argument("--top-n", type=int, default=300, help="Universe size to score.")
    parser.add_argument("--full-universe", action="store_true", help="Analyze the full filtered KOSPI/KOSDAQ universe.")
    parser.add_argument("--markets", default="KOSPI,KOSDAQ", help="Comma-separated market list, e.g. KOSPI,KOSDAQ.")
    parser.add_argument("--min-marcap", type=int, default=0, help="Minimum market cap filter in KRW.")
    parser.add_argument("--price-lookback-days", type=int, default=260, help="Lookback used for price factors.")
    parser.add_argument("--summary-top-n", type=int, default=20, help="How many top names to keep in summary JSON.")
    parser.add_argument("--flow-top-n", type=int, default=0, help="Only fetch flow factors for top-N names by market cap. 0 means all.")
    parser.add_argument("--consensus-top-n", type=int, default=0, help="Only fetch consensus factors for top-N names by market cap. 0 means all.")
    parser.add_argument("--news-lookback-days", type=int, default=7, help="Lookback window for stock-news factors.")
    parser.add_argument("--no-flow", action="store_true", help="Disable investor-flow factors.")
    parser.add_argument("--no-consensus", action="store_true", help="Disable consensus-based factors.")
    parser.add_argument("--no-news", action="store_true", help="Disable stock-news factors.")
    parser.add_argument("--exclude-construction", action="store_true", help="Apply existing construction filter.")
    parser.add_argument("--times", default="08:05,15:35,20:05", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _generate_ai_commentary(summary: dict, builder) -> str:
    try:
        from factor_pipeline import normalize_factor_summary
    except Exception:
        from Disclosure.factor_pipeline import normalize_factor_summary

    summary = normalize_factor_summary(summary)
    if SETTINGS is None or not getattr(SETTINGS, "GEMINI_API_KEY", ""):
        return ""
    if genai is None and google_generativeai is None:
        return ""
    coverage = summary.get("coverage", {})
    if max(
        coverage.get("price_factor_coverage_pct", 0),
        coverage.get("flow_factor_coverage_pct", 0),
        coverage.get("consensus_factor_coverage_pct", 0),
    ) < 50:
        log.warning("AI report skipped because factor coverage is too low.")
        return ""

    buckets = summary.get("portfolio_buckets") or {}
    direct_names = ", ".join(item["name"] for item in buckets.get("direct", [])[:2]) or "없음"
    watch_names = ", ".join(item["name"] for item in buckets.get("watch", [])[:2]) or "없음"
    hold_names = ", ".join(item["name"] for item in buckets.get("hold", [])[:2]) or "없음"
    warnings_text = " / ".join((summary.get("data_quality") or {}).get("warnings", [])[:3]) or "데이터 경고는 제한적입니다."
    context = builder.build_ai_context(summary)
    prompt = f"""
    [임무]
    당신은 한국 주식 장전/장후 브리핑을 쓰는 포트폴리오 매니저입니다.
    아래 입력을 바탕으로 후보를 바꾸지 말고, 해설 문장만 아주 짧게 작성하세요.

    [고정 후보]
    직접 후보: {direct_names}
    눌림 후보: {watch_names}
    보류: {hold_names}

    [규칙]
    1. 위 후보 이름을 절대 바꾸지 마세요.
    2. `보유`, `비보유`, `포지션` 같은 표현은 금지합니다.
    3. 아래 두 줄만 작성하세요.
    4. 제공된 데이터 밖의 이유를 지어내지 마세요.

    [출력 형식]
    해설: ...
    주의: ...

    [팩터 스냅샷 요약]
    {context}
    - 현재 데이터 경고: {warnings_text}
    """
    try:
        if genai is not None:
            client = genai.Client(api_key=SETTINGS.GEMINI_API_KEY)
            response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
            text = getattr(response, "text", "") or ""
        else:
            model = google_generativeai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt)
            text = getattr(response, "text", "") or ""

        explanation = ""
        caution = ""
        for raw in text.splitlines():
            line = raw.strip().lstrip("-").strip()
            if not line:
                continue
            lower = line.lower()
            if lower.startswith("해설:"):
                explanation = line.split(":", 1)[1].strip()
            elif lower.startswith("주의:"):
                caution = line.split(":", 1)[1].strip()
        if not explanation:
            explanation = "오늘은 팩터 상단 후보를 보되 직접 추격보다 강점 축이 유지되는지 확인하는 해석이 적절합니다."
        if not caution:
            caution = warnings_text

        return "\n".join(
            [
                "*AI 최종 코멘트*",
                f"- 직접 후보: {direct_names}",
                f"- 눌림 후보: {watch_names}",
                f"- 보류: {hold_names}",
                f"- 한 줄 해설: {explanation}",
                f"- 데이터 주의: {caution}",
            ]
        )
    except Exception as exc:
        log.warning("factor AI commentary failed: %s", exc)
        return ""


def build_and_send_report(args: argparse.Namespace) -> None:
    from factor_pipeline import FactorSnapshotBuilder

    top_n = 0 if args.full_universe else args.top_n
    markets = [item.strip() for item in str(args.markets).split(",") if item.strip()]
    builder = FactorSnapshotBuilder(
        top_n=top_n,
        min_marcap_krw=args.min_marcap,
        markets=markets,
        price_lookback_days=args.price_lookback_days,
        include_flow=not args.no_flow,
        include_consensus=not args.no_consensus,
        include_news=not args.no_news,
        flow_top_n=args.flow_top_n,
        consensus_top_n=args.consensus_top_n,
        news_lookback_days=args.news_lookback_days,
        exclude_construction=args.exclude_construction,
    )

    snapshot_df = builder.build_snapshot()
    summary = builder.build_summary(snapshot_df, top_n=args.summary_top_n)
    paths = builder.save_snapshot(snapshot_df, summary=summary)
    digest = builder.build_slack_digest(summary)

    snapshot_label = f"{summary['snapshot_date']} {summary['snapshot_time']}"
    digest_title = f"[팩터] 정제 스냅샷 {snapshot_label}"

    if args.print_only:
        print(digest_title)
        print(digest)
    else:
        send_slack(digest, title=digest_title, msg_type="info")

    log.info("factor snapshot saved: %s", paths["snapshot_csv"])
    log.info("factor summary saved: %s", paths["summary_json"])

    if not args.with_ai:
        return

    try:
        ai_report = _generate_ai_commentary(summary, builder)
        if not ai_report:
            log.warning("AI report skipped or empty.")
            return

        summary["ai_commentary"] = ai_report.strip()
        paths = builder.save_snapshot(snapshot_df, summary=summary)
        ai_title = f"[팩터 AI] 포트폴리오 브리핑 {snapshot_label}"
        if args.print_only:
            print(ai_title)
            print(ai_report)
        else:
            send_slack(ai_report, title=ai_title, msg_type="warning")
    except Exception as exc:
        log.exception("AI factor briefing failed")
        if not args.print_only:
            notify_error("Factor Slack Reporter", str(exc))


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Factor Slack scheduler started: %s", ", ".join(schedule_times))

    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")

        if hhmm in schedule_times and run_key != last_run_key:
            log.info("Factor scheduled job triggered at %s", run_key)
            try:
                build_and_send_report(args)
            except Exception as exc:
                log.exception("Factor scheduled job failed")
                if not args.print_only:
                    notify_error("Factor Scheduled Job", str(exc))
            last_run_key = run_key

        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.once:
        build_and_send_report(args)
        return

    run_scheduler(args)


if __name__ == "__main__":
    main()
