from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

try:
    from hybrid_rotation_engine import build_hybrid_study_pack, save_hybrid_study_pack
    from utils.slack import notify_error, send_slack
except Exception:
    from Disclosure.hybrid_rotation_engine import build_hybrid_study_pack, save_hybrid_study_pack
    from Disclosure.utils.slack import notify_error, send_slack


log = logging.getLogger("disclosure.hybrid_study_pack")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build nightly hybrid study pack and optionally send a Slack digest.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print locally instead of sending to Slack.")
    parser.add_argument("--times", default="20:35", help="Comma-separated HH:MM scheduler list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _build_digest(payload: dict) -> str:
    lines = ["*Hybrid Nightly Study Pack*"]
    active = list(payload.get("active_sectors") or [])[:5]
    if active:
        lines.append("- 오늘 활성 섹터")
        for row in active:
            lines.append(
                f"  {row.get('sector') or '-'} | 섹터 점수 {float(row.get('sector_regime_score', 0.0) or 0.0):.1f} | 대장주 {row.get('leader_name') or row.get('leader_symbol') or '-'}"
            )
    leaders = list(payload.get("leaders_and_laggards") or [])[:5]
    if leaders:
        lines.append("- 섹터별 대장/후발주")
        for row in leaders:
            picks = ", ".join(str(item.get("name") or item.get("symbol") or "") for item in (row.get("top_candidates") or [])[:2])
            lines.append(f"  {row.get('sector') or '-'} | 대장 {row.get('leader') or '-'} | 후발주 {picks or '-'}")
    shadow = payload.get("shadow_vs_live") or {}
    lines.append(
        f"- 실주문만 남은 후보 {', '.join((shadow.get('live_only_symbols') or [])[:3]) or '-'} | 비교모드만 남은 후보 {', '.join((shadow.get('shadow_only_symbols') or [])[:3]) or '-'}"
    )
    perf_rows = list(payload.get("sector_recent_20d_stats") or [])[:5]
    if perf_rows:
        lines.append("- 최근 20일 섹터 성과")
        for row in perf_rows:
            lines.append(
                f"  {row.get('sector') or '-'} | n {int(row.get('count', 0) or 0)} | win {float(row.get('win_rate_d5', 0.0) or 0.0) * 100:.1f}% | avg5d {float(row.get('avg_ret_d5', 0.0) or 0.0) * 100:.2f}%"
            )
    return "\n".join(lines)


def build_and_send(args: argparse.Namespace) -> None:
    payload = save_hybrid_study_pack(build_hybrid_study_pack())
    digest = _build_digest(payload)
    title = f"[Hybrid Study] {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
    if args.print_only:
        print(title)
        print(digest)
        return
    send_slack(digest, title=title, msg_type="info")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Hybrid study pack scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            try:
                build_and_send(args)
            except Exception as exc:
                log.exception("hybrid study pack failed")
                if not args.print_only:
                    notify_error("Hybrid Study Pack", str(exc))
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        build_and_send(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
