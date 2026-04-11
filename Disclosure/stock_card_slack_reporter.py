from __future__ import annotations

import argparse
import logging
import time
import warnings
from datetime import datetime

try:
    from analyst_report_collector import AnalystReportCollector
    from config import SETTINGS
    from daily_signal_mart import build_mart_summary, save_daily_signal_mart
    from factor_pipeline import FactorSnapshotBuilder
    from stock_card_digest import build_stock_card_digest, build_stock_card_summary, save_stock_card_summary
    from stock_card_pipeline import (
        build_stock_card_frame,
    )
    from utils.slack import notify_error, send_slack, upload_slack_file
except Exception:
    from Disclosure.analyst_report_collector import AnalystReportCollector
    from Disclosure.config import SETTINGS
    from Disclosure.daily_signal_mart import build_mart_summary, save_daily_signal_mart
    from Disclosure.factor_pipeline import FactorSnapshotBuilder
    from Disclosure.stock_card_digest import build_stock_card_digest, build_stock_card_summary, save_stock_card_summary
    from Disclosure.stock_card_pipeline import (
        build_stock_card_frame,
    )
    from Disclosure.utils.slack import notify_error, send_slack, upload_slack_file

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


log = logging.getLogger("disclosure.stock_card_slack")
if google_generativeai is not None and getattr(SETTINGS, "GEMINI_API_KEY", ""):
    google_generativeai.configure(api_key=getattr(SETTINGS, "GEMINI_API_KEY", ""))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build unified stock review snapshots from factor, analyst, and flow data.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print digest locally instead of sending to Slack.")
    parser.add_argument("--with-ai", action="store_true", help="Append a short AI reading note after the structured digest.")
    parser.add_argument("--upload-files", action="store_true", help="Upload CSV/JSON if Slack bot token exists.")
    parser.add_argument("--save-mart", action="store_true", help="Save a combined daily mart alongside the stock review outputs.")
    parser.add_argument("--top-n", type=int, default=20, help="How many names to keep in the stock review summary.")
    parser.add_argument("--analyst-days", type=int, default=30, help="Analyst report lookback window.")
    parser.add_argument("--flow-days", type=int, default=3, help="Flow snapshot lookback window.")
    parser.add_argument("--collect-analyst-before-report", action="store_true", help="Collect Naver/Hankyung analyst reports first.")
    parser.add_argument("--naver-pages", type=int, default=3, help="How many Naver research pages to scan.")
    parser.add_argument("--hankyung-pages", type=int, default=3, help="How many Hankyung list pages to scan.")
    parser.add_argument("--hankyung-days", type=int, default=7, help="Hankyung list lookback window.")
    parser.add_argument("--factor-build-before-report", action="store_true", help="Refresh factor snapshot before building cards.")
    parser.add_argument("--factor-top-n", type=int, default=300, help="Universe size when factor refresh is enabled.")
    parser.add_argument("--full-universe", action="store_true", help="Refresh factor snapshot for the full filtered universe.")
    parser.add_argument("--markets", default="KOSPI,KOSDAQ", help="Comma-separated market list.")
    parser.add_argument("--min-marcap", type=int, default=0, help="Factor snapshot minimum market cap.")
    parser.add_argument("--price-lookback-days", type=int, default=260, help="Factor price lookback window.")
    parser.add_argument("--flow-top-n", type=int, default=800, help="Factor flow cap. 0 means all.")
    parser.add_argument("--consensus-top-n", type=int, default=1200, help="Factor consensus cap. 0 means all.")
    parser.add_argument("--news-lookback-days", type=int, default=7, help="Factor stock-news lookback.")
    parser.add_argument("--no-flow", action="store_true", help="Disable factor flow features.")
    parser.add_argument("--no-consensus", action="store_true", help="Disable factor consensus features.")
    parser.add_argument("--no-news", action="store_true", help="Disable factor stock-news features.")
    parser.add_argument("--exclude-construction", action="store_true", help="Apply existing construction filter in factor refresh.")
    parser.add_argument("--times", default="08:07,15:37,20:07", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _collect_analyst_reports(args: argparse.Namespace) -> None:
    collector = AnalystReportCollector(
        naver_pages=args.naver_pages,
        hankyung_pages=args.hankyung_pages,
        hankyung_days=args.hankyung_days,
    )
    result = collector.collect_once()
    log.info("analyst collect-before-report: %s", result)


def _refresh_factor_snapshot(args: argparse.Namespace) -> None:
    top_n = 0 if args.full_universe else args.factor_top_n
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
    summary = builder.build_summary(snapshot_df)
    paths = builder.save_snapshot(snapshot_df, summary=summary)
    log.info("factor refreshed for stock review snapshot: %s", paths["snapshot_csv"])


def _build_ai_context(summary: dict) -> str:
    lines = []
    counts = summary.get("counts") or {}
    data_quality = summary.get("data_quality") or {}
    decision_regime = summary.get("decision_regime") or {}
    lines.append(
        f"- 커버리지: total {counts.get('total', 0)} | factor {counts.get('factor', 0)} | analyst {counts.get('analyst', 0)} | "
        f"flow {counts.get('flow', 0)} | intraday {counts.get('intraday', 0)} | event {counts.get('event', 0)} | ml {counts.get('ml', 0)}"
    )
    lines.append(f"- 데이터 품질: {data_quality.get('label', '-')} | 해석 모드: {decision_regime.get('name', '-')}")
    if data_quality.get("warnings"):
        lines.append("- 주의: " + " / ".join(data_quality.get("warnings", [])[:3]))
    for label in ("direct", "watch", "hold"):
        rows = (summary.get("action_buckets") or {}).get(label) or []
        if rows:
            names = ", ".join(row.get("name") or row.get("symbol") for row in rows[:3])
            lines.append(f"- {label}: {names}")
    lines.append("- 상위 후보:")
    for row in (summary.get("cards") or [])[:6]:
        lines.append(
            f"  {row.get('name')}({row.get('symbol')}) | {row.get('sector')} | "
            f"점검 {row.get('card_score')} | sources {row.get('active_source_count')} | "
            f"factor {row.get('composite_score')} | analyst {row.get('analyst_conviction_score')} | "
            f"flow {row.get('flow_state_score')} | intraday {row.get('flow_intraday_edge_score')} | event {row.get('event_alpha_score')}"
        )
    return "\n".join(lines)


def _generate_ai_commentary(summary: dict) -> str:
    api_key = getattr(SETTINGS, "GEMINI_API_KEY", "")
    if not api_key:
        return ""
    if genai is None and google_generativeai is None:
        return ""

    action_buckets = summary.get("action_buckets") or {}
    direct_names = ", ".join((row.get("name") or row.get("symbol") for row in action_buckets.get("direct", [])[:2])) or "없음"
    watch_names = ", ".join((row.get("name") or row.get("symbol") for row in action_buckets.get("watch", [])[:2])) or "없음"
    hold_names = ", ".join((row.get("name") or row.get("symbol") for row in action_buckets.get("hold", [])[:2])) or "없음"
    data_warnings = " / ".join((summary.get("data_quality") or {}).get("warnings", [])[:3]) or "데이터 경고는 제한적입니다."

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

    [입력]
    {_build_ai_context(summary)}
    - 현재 데이터 경고: {data_warnings}
    """

    try:
        if genai is not None:
            client = genai.Client(api_key=api_key)
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
            explanation = "오늘은 상단 종목 체력보다 실시간 확인을 더 우선하는 해석이 적절합니다."
        if not caution:
            caution = data_warnings

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
        log.warning("stock-review AI commentary failed: %s", exc)
        return ""


def build_and_send_report(args: argparse.Namespace) -> None:
    if args.collect_analyst_before_report:
        _collect_analyst_reports(args)
    if args.factor_build_before_report:
        _refresh_factor_snapshot(args)

    card_df = build_stock_card_frame(analyst_days=args.analyst_days, flow_days=args.flow_days)
    summary = build_stock_card_summary(card_df, top_n=args.top_n)
    paths = save_stock_card_summary(card_df, summary)
    if args.save_mart:
        mart_paths = save_daily_signal_mart(card_df, build_mart_summary(card_df))
        paths.update(mart_paths)
    digest = build_stock_card_digest(summary, top_n=min(args.top_n, 10))
    ai_commentary = _generate_ai_commentary(summary) if args.with_ai else ""
    if ai_commentary:
        digest = "\n".join([digest, "", ai_commentary.strip()])
        summary["ai_commentary"] = ai_commentary.strip()
        paths = save_stock_card_summary(card_df, summary)
        if args.save_mart:
            mart_paths = save_daily_signal_mart(card_df, build_mart_summary(card_df))
            paths.update(mart_paths)
    title = f"[종목 점검표] 통합 스냅샷 {datetime.now().strftime('%Y%m%d %H:%M:%S')}"

    if args.print_only:
        print(title)
        print(digest)
    else:
        send_slack(digest, title=title, msg_type="info")

    if args.upload_files and not args.print_only:
        upload_slack_file(paths["cards_csv"], title="stock_cards", initial_comment="통합 종목 점검표 CSV")
        upload_slack_file(paths["cards_json"], title="stock_cards_summary", initial_comment="통합 종목 점검표 JSON")
        if paths.get("mart_csv"):
            upload_slack_file(paths["mart_csv"], title="daily_signal_mart", initial_comment="통합 일일 마트 CSV")
        if paths.get("mart_json"):
            upload_slack_file(paths["mart_json"], title="daily_signal_mart_summary", initial_comment="통합 일일 마트 JSON")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Stock review Slack scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            try:
                build_and_send_report(args)
            except Exception as exc:
                log.exception("stock review scheduled job failed")
                if not args.print_only:
                    notify_error("Stock Card Slack Reporter", str(exc))
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        build_and_send_report(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
