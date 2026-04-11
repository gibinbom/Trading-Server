from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import time
from datetime import datetime

import pandas as pd

from check_api_integrations import run_checks as run_api_integration_checks, save_report as save_api_integration_report
from utils.slack import notify_error, send_slack
from verify_runtime import run_all_checks, run_lite_checks


log = logging.getLogger("disclosure.healthcheck")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYST_CACHE_STATE_PATH = os.path.join(ROOT_DIR, "cache", "analyst_cache_warm_state.json")
ANALYST_SUMMARY_PATH = os.path.join(ROOT_DIR, "analyst_reports", "summaries", "analyst_report_scored_latest.csv")
MART_SUMMARY_PATH = os.path.join(ROOT_DIR, "marts", "daily_signal_mart_latest.json")
API_INTEGRATION_REPORT_PATH = os.path.join(ROOT_DIR, "runtime", "api_integrations_latest.json")


def _fmt_age_min(value: object) -> str:
    return "-" if value in (None, "", "None") else str(value)


def _fmt_age_with_suffix(value: object) -> str:
    text = _fmt_age_min(value)
    return "-" if text == "-" else f"{text}m"


def _fmt_stability_score_100(count: object, value: object) -> str:
    try:
        count_int = int(count or 0)
    except Exception:
        count_int = 0
    try:
        value_float = float(value or 0.0)
    except Exception:
        value_float = 0.0
    if count_int <= 0:
        return "-"
    return f"{int(round(value_float * 100))}/100"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run runtime verification and PM2 health checks, then send a Slack digest.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print locally instead of sending to Slack.")
    parser.add_argument("--mode", choices=["lite", "full"], default="lite", help="Lite healthcheck avoids deep network-heavy verification.")
    parser.add_argument("--times", default="08:20,15:50,20:20", help="Comma-separated HH:MM scheduler list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _load_pm2_health() -> list[dict]:
    try:
        proc = subprocess.run(["pm2", "jlist"], capture_output=True, text=True, timeout=15, check=False)
    except Exception as exc:
        return [{"name": "pm2", "status": f"unavailable: {exc}"}]
    if proc.returncode != 0:
        return [{"name": "pm2", "status": f"error: {proc.stderr[:120]}"}]
    try:
        payload = json.loads(proc.stdout or "[]")
    except Exception as exc:
        return [{"name": "pm2", "status": f"decode-error: {exc}"}]
    rows = []
    for app in payload:
        env = app.get("pm2_env") or {}
        rows.append(
            {
                "name": app.get("name") or "",
                "status": env.get("status") or app.get("status") or "unknown",
                "restarts": int(env.get("restart_time", 0) or 0),
                "uptime_ms": int(env.get("pm_uptime", 0) or 0),
            }
        )
    return rows


def _load_analyst_cache_health() -> dict:
    payload = {
        "status": "missing",
        "generated_at": "",
        "minutes_since_warm": None,
        "report_symbols": 0,
        "target_symbols": 0,
        "warmed_symbols": 0,
        "sample_symbols": [],
        "latest_scored_age_min": None,
    }
    if os.path.exists(ANALYST_CACHE_STATE_PATH):
        try:
            with open(ANALYST_CACHE_STATE_PATH, "r", encoding="utf-8") as fp:
                stored = json.load(fp)
            payload.update(stored if isinstance(stored, dict) else {})
            generated = pd.to_datetime(payload.get("generated_at"), errors="coerce")
            if not pd.isna(generated):
                delta = pd.Timestamp.now() - generated.tz_localize(None) if getattr(generated, "tzinfo", None) else pd.Timestamp.now() - generated
                payload["minutes_since_warm"] = int(max(0, delta.total_seconds() // 60))
                payload["status"] = "ok"
        except Exception as exc:
            payload["status"] = f"decode-error: {exc}"
    if os.path.exists(ANALYST_SUMMARY_PATH):
        age_sec = max(0.0, time.time() - os.path.getmtime(ANALYST_SUMMARY_PATH))
        payload["latest_scored_age_min"] = int(age_sec // 60)
    warmed = int(payload.get("warmed_symbols", 0) or 0)
    minutes_since = payload.get("minutes_since_warm")
    if payload["status"] == "ok" and (minutes_since is None or minutes_since > 12 * 60):
        payload["status"] = "stale"
    if payload["status"] in {"ok", "stale"} and warmed < 20:
        payload["status"] = f"{payload['status']}_underfilled"
    return payload


def _build_analyst_cache_alerts(pm2_rows: list[dict], analyst_cache: dict) -> list[str]:
    alerts: list[str] = []
    warmer = next((row for row in pm2_rows if row.get("name") == "disclosure-analyst-cache-warmer"), None)
    warmer_status = str((warmer or {}).get("status") or "").lower()
    cache_status = str(analyst_cache.get("status") or "").lower()
    warmed = int(analyst_cache.get("warmed_symbols", 0) or 0)

    if warmer_status == "online" and cache_status != "ok":
        alerts.append(
            f"애널 워머는 살아 있지만 캐시 상태가 {analyst_cache.get('status')} 입니다"
        )
    if warmer_status == "online" and warmed <= 0:
        alerts.append("애널 워머는 살아 있지만 예열된 종목 수가 0입니다")
    if warmer_status != "online" and cache_status.startswith("stale"):
        alerts.append("애널 캐시가 오래됐고 워머도 떠 있지 않습니다")
    return alerts


def _load_mart_health() -> dict:
    payload = {
        "status": "missing",
        "row_count": 0,
        "unknown_sector_count": 0,
        "factor_coverage": 0,
        "analyst_coverage": 0,
        "flow_coverage": 0,
        "intraday_coverage": 0,
        "event_coverage": 0,
        "ml_coverage": 0,
        "age_min": None,
    }
    if not os.path.exists(MART_SUMMARY_PATH):
        return payload
    try:
        with open(MART_SUMMARY_PATH, "r", encoding="utf-8") as fp:
            stored = json.load(fp)
        payload.update(stored if isinstance(stored, dict) else {})
        payload["status"] = "ok"
        age_sec = max(0.0, time.time() - os.path.getmtime(MART_SUMMARY_PATH))
        payload["age_min"] = int(age_sec // 60)
        if payload["age_min"] is not None and payload["age_min"] > 18 * 60:
            payload["status"] = "stale"
    except Exception as exc:
        payload["status"] = f"decode-error: {exc}"
    return payload


def _load_api_integration_health() -> dict:
    payload = {
        "status": "missing",
        "generated_at": "",
        "age_min": None,
        "dart_ok": None,
        "kis_ok": None,
        "kis_primary_ok": None,
        "kis_source": "",
        "macro_ok": None,
        "slack_ok": None,
        "warning_count": 0,
        "warnings": [],
    }
    if not os.path.exists(API_INTEGRATION_REPORT_PATH):
        return payload
    try:
        with open(API_INTEGRATION_REPORT_PATH, "r", encoding="utf-8") as fp:
            stored = json.load(fp)
        generated = pd.to_datetime(stored.get("generated_at"), errors="coerce")
        if not pd.isna(generated):
            delta = pd.Timestamp.now() - generated.tz_localize(None) if getattr(generated, "tzinfo", None) else pd.Timestamp.now() - generated
            payload["age_min"] = int(max(0, delta.total_seconds() // 60))
        payload.update(
            {
                "status": str(stored.get("status") or "ok"),
                "generated_at": str(stored.get("generated_at") or ""),
                "dart_ok": (stored.get("dart") or {}).get("ok"),
                "kis_ok": (stored.get("kis") or {}).get("ok"),
                "kis_primary_ok": (stored.get("kis") or {}).get("primary_ok"),
                "kis_source": str((stored.get("kis") or {}).get("source") or ""),
                "macro_ok": (stored.get("macro") or {}).get("ok"),
                "slack_ok": (stored.get("slack") or {}).get("ok"),
                "warning_count": len(stored.get("warnings") or []),
                "warnings": list(stored.get("warnings") or []),
            }
        )
        if payload["age_min"] is not None and payload["age_min"] > 6 * 60:
            payload["status"] = "stale"
    except Exception as exc:
        payload["status"] = f"decode-error: {exc}"
    return payload


def _build_runtime_alerts(checks: dict, mart_health: dict, pm2_rows: list[dict], api_health: dict) -> list[str]:
    alerts: list[str] = []
    if int(checks.get("stock_card", {}).get("intraday", 0) or 0) == 0:
        alerts.append("종목 점검표 장중 수급 커버리지가 0입니다")
    if int(checks.get("stock_card", {}).get("flow", 0) or 0) == 0:
        alerts.append("종목 점검표 수급 커버리지가 0입니다")
    if int(checks.get("factor", {}).get("eligible", 0) or 0) <= 0:
        alerts.append("팩터 후보 유니버스가 0입니다")
    if str(checks.get("factor", {}).get("dynamic_weight_status", "") or "").lower() != "ok":
        alerts.append(f"팩터 비중 조절 상태가 {checks.get('factor', {}).get('dynamic_weight_status', 'missing')} 입니다")
    if int(checks.get("stock_card", {}).get("ml_train_rows", 0) or 0) < 20:
        alerts.append(f"ML 학습 행 수가 얕습니다 ({checks.get('stock_card', {}).get('ml_train_rows', 0)})")
    if mart_health.get("status") in {"missing", "stale"}:
        alerts.append(f"일일 시그널 마트 상태가 {mart_health.get('status')} 입니다")
    if int(mart_health.get("unknown_sector_count", 0) or 0) >= 20:
        alerts.append(f"일일 시그널 마트의 미분류 섹터 수가 많습니다 ({mart_health.get('unknown_sector_count')})")
    if int(mart_health.get("event_coverage", 0) or 0) == 0:
        alerts.append("일일 시그널 마트의 이벤트 커버리지가 0입니다")
    if int(checks.get("event", {}).get("log_pending", 0) or 0) > 0:
        alerts.append(f"이벤트 백테스트에 미래 가격 미반영 기록이 남아 있습니다 ({checks.get('event', {}).get('log_pending', 0)})")
    flow_logs = checks.get("flow_logs", {}) or {}
    if int(flow_logs.get("snapshot_files", 0) or 0) == 0:
        alerts.append("수급 스냅샷 로그 파일이 없습니다")
    if int(flow_logs.get("snapshot_lines", 0) or 0) == 0:
        alerts.append("수급 스냅샷 로그 줄 수가 0입니다")
    diagnosis = _diagnose_flow_logs(flow_logs)
    if diagnosis.get("status") not in {"ok", "idle"}:
        alerts.append(f"수급 진단: {diagnosis.get('status')} ({diagnosis.get('reason')})")

    trade_runtime = checks.get("trade_runtime", {}) or {}
    trader_row = next((row for row in pm2_rows if row.get("name") == "disclosure-main-trader"), None)
    trader_status = str((trader_row or {}).get("status") or "").lower()
    if trade_runtime.get("auto_trade_enabled") and trader_status and trader_status != "online":
        alerts.append(f"메인 트레이더 상태가 `{trader_row.get('status')}` 입니다")
    if trade_runtime.get("auto_trade_enabled") and not trade_runtime.get("event_state_exists"):
        alerts.append("메인 거래 상태 파일이 없습니다")
    if trade_runtime.get("auto_trade_enabled") and not trade_runtime.get("runtime_status_exists"):
        alerts.append("주문 감시 런타임 상태 파일이 없습니다")
    if trade_runtime.get("auto_trade_enabled") and not trade_runtime.get("trade_audit_exists"):
        alerts.append("거래 알림 감사 로그가 없습니다")
    if trade_runtime.get("auto_trade_enabled") and not trade_runtime.get("slack_delivery_exists"):
        alerts.append("거래 런타임용 Slack 전달 로그가 없습니다")
    if trade_runtime.get("auto_trade_enabled") and trade_runtime.get("runtime_status_exists") and not trade_runtime.get("runtime_slack_webhook_hint"):
        alerts.append("거래 런타임의 Slack webhook 힌트가 비어 있습니다")
    if trade_runtime.get("hybrid_rotation_enabled") and not trade_runtime.get("sector_rotation_exists"):
        alerts.append("하이브리드 섹터 순환 산출물이 없습니다")
    if trade_runtime.get("hybrid_rotation_enabled") and not trade_runtime.get("sector_thesis_exists"):
        alerts.append("하이브리드 섹터 결론 산출물이 없습니다")
    if trade_runtime.get("hybrid_rotation_enabled") and not trade_runtime.get("wics_universe_exists"):
        alerts.append("WICS 최종 바스켓 산출물이 없습니다")
    if trade_runtime.get("hybrid_rotation_enabled") and not trade_runtime.get("relative_value_exists"):
        alerts.append("하이브리드 상대가치 산출물이 없습니다")
    if trade_runtime.get("hybrid_rotation_enabled") and not trade_runtime.get("hybrid_shadow_book_exists"):
        alerts.append("하이브리드 비교모드 장부가 없습니다")
    if trade_runtime.get("hybrid_rotation_enabled") and not trade_runtime.get("trade_decision_ledger_exists"):
        alerts.append("하이브리드 판단 기록 장부가 없습니다")
    if not trade_runtime.get("fair_value_snapshot_exists"):
        alerts.append("적정가 스냅샷 산출물이 없습니다")
    elif int(trade_runtime.get("fair_value_coverage_count", 0) or 0) <= 0:
        alerts.append("적정가 산출 종목 수가 0입니다")
    elif int(trade_runtime.get("fair_value_snapshot_age_min", 0) or 0) > 18 * 60:
        alerts.append("적정가 스냅샷이 오래됐습니다")
    if trade_runtime.get("hybrid_rotation_enabled") and trade_runtime.get("runtime_hybrid_live_only_symbols"):
        alerts.append(f"실주문 후보와 비교모드 후보가 다릅니다 ({', '.join((trade_runtime.get('runtime_hybrid_live_only_symbols') or [])[:3])})")
    if (
        trade_runtime.get("auto_trade_enabled")
        and int(trade_runtime.get("runtime_candidate_count", 0) or 0) > 0
        and int(trade_runtime.get("runtime_candidate_aligned_count", 0) or 0) == 0
        and int(trade_runtime.get("runtime_candidate_risk_count", 0) or 0) > 0
    ):
        alerts.append("주문 감시 후보가 모두 맥락 경계 쪽입니다")
    runtime_age = trade_runtime.get("runtime_status_age_min")
    if trade_runtime.get("auto_trade_enabled") and isinstance(runtime_age, int) and runtime_age > 60:
        alerts.append(f"주문 감시 런타임 상태가 오래됐습니다 ({runtime_age}분)")
    state_age = trade_runtime.get("event_state_age_min")
    if trade_runtime.get("auto_trade_enabled") and isinstance(state_age, int) and state_age > 24 * 60:
        alerts.append(f"거래 상태 파일이 오래됐습니다 ({state_age}분)")
    if trade_runtime.get("broker_mode_mismatch"):
        alerts.append("엔진 브로커 모드가 현재 모의투자/드라이런 설정을 따르지 않는 것처럼 보입니다")
    if trade_runtime.get("runtime_broker_mismatch"):
        alerts.append("주문 감시 런타임의 브로커 모드가 현재 설정과 다릅니다")
    if trade_runtime.get("hybrid_rotation_enabled") and str(trade_runtime.get("wics_focus_status_label") or "") == "재점검":
        alerts.append(
            f"WICS 초점 바스켓이 재점검 상태입니다 ({trade_runtime.get('wics_focus_sector') or '-'})"
        )
    if (
        trade_runtime.get("hybrid_rotation_enabled")
        and int(trade_runtime.get("wics_focus_dynamic_count", 0) or 0) > 0
        and float(trade_runtime.get("wics_focus_dynamic_stability", 0.0) or 0.0) < 0.6
    ):
        alerts.append(
            f"WICS 초점 바스켓의 동적안정도가 낮습니다 ({int(round(float(trade_runtime.get('wics_focus_dynamic_stability', 0.0) or 0.0) * 100))}/100)"
        )
    if trade_runtime.get("auto_trade_enabled") and trade_runtime.get("runtime_broker_is_virtual") is False and trade_runtime.get("runtime_broker_dry_run") is False:
        alerts.append("메인 트레이더가 실주문 모드로 돌고 있습니다")
    if trade_runtime.get("auto_trade_enabled") and trade_runtime.get("runtime_status_exists") and trade_runtime.get("runtime_broker_ready") is False:
        alerts.append(f"주문 감시 브로커 준비 점검이 실패했습니다 ({trade_runtime.get('runtime_broker_ready_note') or '-'})")
    if (
        int(trade_runtime.get("close_swing_max_stop_sells_per_day", 0) or 0) > 0
        and int(trade_runtime.get("runtime_today_stop_sell_count", 0) or 0) >= int(trade_runtime.get("close_swing_max_stop_sells_per_day", 0) or 0)
    ):
        alerts.append(
            f"종가 매매 일일 손절 가드가 작동 중입니다 ({trade_runtime.get('runtime_today_stop_sell_count')}/{trade_runtime.get('close_swing_max_stop_sells_per_day')})"
        )
    if (
        int(trade_runtime.get("close_swing_max_open_positions", 0) or 0) > 0
        and int(trade_runtime.get("runtime_holding_symbol_count", 0) or 0) >= int(trade_runtime.get("close_swing_max_open_positions", 0) or 0)
    ):
        alerts.append(
            f"종가 매매 최대 보유 종목 수 가드가 작동 중입니다 ({trade_runtime.get('runtime_holding_symbol_count')}/{trade_runtime.get('close_swing_max_open_positions')})"
        )
    if api_health.get("status") == "missing":
        alerts.append("API 연동 점검 산출물이 없습니다")
    elif str(api_health.get("status") or "").lower() not in {"ok"}:
        alerts.append(f"API 연동 상태가 `{api_health.get('status')}` 입니다")
    if api_health.get("dart_ok") is False:
        alerts.append("DART 연동 점검이 실패했습니다")
    if api_health.get("kis_ok") is False:
        alerts.append("KIS 연동 점검이 실패했습니다")
    elif api_health.get("kis_primary_ok") is False:
        alerts.append(f"KIS 원본 시세 API가 흔들려 보강 경로로 읽고 있습니다 ({api_health.get('kis_source') or '-'})")
    if api_health.get("macro_ok") is False:
        alerts.append("매크로 연동 점검이 실패했습니다")
    if api_health.get("slack_ok") is False:
        alerts.append("Slack 연동 점검이 실패했습니다")
    return alerts


def _diagnose_flow_logs(flow_logs: dict) -> dict[str, str]:
    updates = int(flow_logs.get("health_total_updates", 0) or 0)
    tick_logged = int(flow_logs.get("health_flow_tick_logged", 0) or 0)
    snapshot_logged = int(flow_logs.get("health_snapshot_logged", 0) or 0)
    skip_tick = int(flow_logs.get("health_skip_below_tick_threshold", 0) or 0)
    skip_interval = int(flow_logs.get("health_skip_snapshot_interval_gate", 0) or 0)
    skip_gross = int(flow_logs.get("health_skip_snapshot_min_gross", 0) or 0)

    if updates <= 0:
        return {"status": "idle", "reason": "들어온 웹소켓 업데이트가 아직 없습니다", "action": "웹소켓 구독 상태나 시장 개장 여부 확인"}
    if tick_logged <= 0 and skip_tick > 0:
        return {"status": "tick_threshold_high", "reason": "업데이트는 있지만 수급 틱 문턱이 높아 모두 걸러집니다", "action": "FLOW_TICK_LOG_MIN_AMT_MIL 또는 FLOW_TICK_LOG_MIN_FOREIGN_QTY 하향"}
    if tick_logged > 0 and snapshot_logged <= 0 and skip_interval > skip_gross:
        return {"status": "snapshot_interval_gate", "reason": "수급 틱은 있지만 스냅샷 간격 또는 강제 저장 조건이 막고 있습니다", "action": "FLOW_SNAPSHOT_INTERVAL_SEC 또는 FLOW_SNAPSHOT_FORCE_GROSS_AMT_MIL 하향"}
    if tick_logged > 0 and snapshot_logged <= 0 and skip_gross >= skip_interval:
        return {"status": "snapshot_gross_threshold_high", "reason": "수급 틱은 있지만 총거래대금 문턱이 높아 스냅샷이 남지 않습니다", "action": "FLOW_SNAPSHOT_MIN_GROSS_AMT_MIL 하향"}
    if snapshot_logged > 0:
        return {"status": "ok", "reason": "수급 스냅샷이 정상적으로 기록되고 있습니다", "action": "없음"}
    return {"status": "unknown", "reason": "현재 카운터만으로 수급 로그 상태를 설명하기 어렵습니다", "action": "trading_flow_health_latest.json 확인"}


def build_digest(checks: dict, pm2_rows: list[dict], analyst_cache: dict, mart_health: dict, api_health: dict) -> str:
    scored_age_text = _fmt_age_with_suffix(analyst_cache.get("latest_scored_age_min"))
    mart_age_text = _fmt_age_with_suffix(mart_health.get("age_min"))
    lines = ["*상태 점검표*"]
    lines.append(f"- 생성시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(
        f"- 팩터 후보: {checks['factor']['eligible']} | "
        f"점검표 행수: {checks['stock_card']['rows']} | "
        f"장중 수급: {checks['stock_card'].get('intraday', 0)} | "
        f"미시구조: {checks['stock_card'].get('micro', 0)} | "
        f"ML 점수: {checks['stock_card'].get('ml', 0)} | "
        f"비중 조절: {checks['factor'].get('dynamic_weight_status', 'missing')} | "
        f"매크로 헤드라인: {checks['macro']['headlines']} ({checks['macro'].get('source', 'unknown')})"
    )
    lines.append(
        f"- ML 모델: {checks['stock_card'].get('ml_model_type', '-') or '-'} | "
        f"학습 행수: {checks['stock_card'].get('ml_train_rows', 0)} | "
        f"이벤트 미반영: {checks['event'].get('log_pending', 0)}"
    )
    trade_runtime = checks.get("trade_runtime", {}) or {}
    trade_state_age_text = _fmt_age_with_suffix(trade_runtime.get("event_state_age_min"))
    trade_runtime_age_text = _fmt_age_with_suffix(trade_runtime.get("runtime_status_age_min"))
    trade_audit_age_text = _fmt_age_with_suffix(trade_runtime.get("trade_audit_age_min"))
    slack_delivery_age_text = _fmt_age_with_suffix(trade_runtime.get("slack_delivery_age_min"))
    lines.append(
        f"- 거래 런타임: 자동매매 {trade_runtime.get('auto_trade_enabled', False)} | "
        f"종가매매 {trade_runtime.get('close_swing_enabled', False)} | "
        f"하이브리드 {trade_runtime.get('hybrid_rotation_enabled', False)}({'비교모드' if trade_runtime.get('hybrid_shadow_only', True) else '실주문'}) | "
        f"모드 {trade_runtime.get('main_run_mode', '-') or '-'} | "
        f"주문창 {trade_runtime.get('trade_window', '-') or '-'} | "
        f"처리 {trade_runtime.get('handled_count', 0)} | "
        f"체결 {trade_runtime.get('traded_count', 0)} | "
        f"상태 경과 {trade_state_age_text}"
    )
    lines.append(
        f"- 주문 감시: 런타임 {'ok' if trade_runtime.get('runtime_status_exists') else 'missing'} ({trade_runtime_age_text}) | "
        f"단계 {trade_runtime.get('runtime_phase', '-') or '-'} | "
        f"주문창 안 {trade_runtime.get('runtime_in_trade_window')} | "
        f"후보 {trade_runtime.get('runtime_candidate_count', 0)} | "
        f"통과 {trade_runtime.get('runtime_candidate_approved_count', 0)} | "
        f"보류 {trade_runtime.get('runtime_candidate_blocked_count', 0)} | "
        f"맥락 일치 {trade_runtime.get('runtime_candidate_aligned_count', 0)} | "
        f"맥락 경계 {trade_runtime.get('runtime_candidate_risk_count', 0)} | "
        f"마지막 결과 {trade_runtime.get('runtime_last_result', '-') or '-'}"
    )
    lines.append(
        f"- 하이브리드 비교모드: 활성 {', '.join((trade_runtime.get('runtime_hybrid_active_sectors') or [])[:4]) or '-'} | "
        f"섹터결론 {trade_runtime.get('sector_thesis_top_sector', '-') or '-'}:{trade_runtime.get('sector_thesis_top_label', '-') or '-'} | "
        f"WICS {trade_runtime.get('wics_focus_sector', '-') or '-'}:{trade_runtime.get('wics_focus_status_label', '-') or '-'} | "
        f"비교모드 후보 {trade_runtime.get('runtime_hybrid_shadow_chosen_count', 0)} | "
        f"실주문 후보 {trade_runtime.get('runtime_hybrid_live_selected_count', 0)} | "
        f"실주문만 {', '.join((trade_runtime.get('runtime_hybrid_live_only_symbols') or [])[:3]) or '-'} | "
        f"비교모드만 {', '.join((trade_runtime.get('runtime_hybrid_shadow_only_symbols') or [])[:3]) or '-'} | "
        f"가격 보수조정 {float(trade_runtime.get('runtime_hybrid_quote_penalty_total', 0.0) or 0.0):.1f}"
    )
    if trade_runtime.get("hybrid_rotation_enabled"):
        lines.append(
            f"- 하이브리드 장부: 최근 행 {trade_runtime.get('trade_decision_ledger_recent_rows', 0)} | "
            f"보류 사유 {trade_runtime.get('trade_decision_blocked_histogram') or '-'} | "
            f"상단 섹터 {trade_runtime.get('sector_rotation_top_sector', '-') or '-'} | "
            f"섹터결론 {trade_runtime.get('sector_thesis_top_sector', '-') or '-'}:{trade_runtime.get('sector_thesis_top_label', '-') or '-'} | "
            f"WICS 흐름 {trade_runtime.get('wics_universe_regime', '-') or '-'} | "
            f"WICS 초점 {trade_runtime.get('wics_focus_sector', '-') or '-'}:{trade_runtime.get('wics_focus_status_label', '-') or '-'} | "
            f"상단 종목 {trade_runtime.get('relative_value_top_symbol', '-') or '-'}"
        )
    if trade_runtime.get("hybrid_rotation_enabled"):
        lines.append(
            f"- WICS 바스켓: 파일 {'ok' if trade_runtime.get('wics_universe_exists') else 'missing'} | "
            f"흐름 {trade_runtime.get('wics_universe_regime', '-') or '-'} | "
            f"표본 {trade_runtime.get('wics_universe_history_confidence_label', '-') or '-'} | "
            f"동적 후보수 {trade_runtime.get('wics_universe_dynamic_symbol_count', 0)} | "
            f"평균 동적안정도 {_fmt_stability_score_100(trade_runtime.get('wics_universe_dynamic_symbol_count', 0), trade_runtime.get('wics_universe_avg_dynamic_stability', 0.0))} | "
            f"재점검 섹터 {trade_runtime.get('wics_universe_review_sector_count', 0)} | "
            f"초점 최종 바스켓 {trade_runtime.get('wics_focus_final_count', 0)}"
        )
    fair_value_age_text = _fmt_age_with_suffix(trade_runtime.get("fair_value_snapshot_age_min"))
    lines.append(
        f"- 적정가 스냅샷: 파일 {'ok' if trade_runtime.get('fair_value_snapshot_exists') else 'missing'} ({fair_value_age_text}) | "
        f"산출 {trade_runtime.get('fair_value_coverage_count', 0)}/{trade_runtime.get('fair_value_row_count', 0)} | "
        f"상단 할인 {trade_runtime.get('fair_value_top_discount_name', '-') or '-'} "
        f"{float(trade_runtime.get('fair_value_top_discount_gap_pct', 0.0) or 0.0):+.1f}%"
    )
    lines.append(
        f"- 주문 감시 메타: 브로커 모의투자={trade_runtime.get('runtime_broker_is_virtual')} | "
        f"브로커 dry_run={trade_runtime.get('runtime_broker_dry_run')} | "
        f"브로커 준비={trade_runtime.get('runtime_broker_ready')} | "
        f"브로커 현금={trade_runtime.get('runtime_broker_cash_krw', 0)} | "
        f"보유 종목수={trade_runtime.get('runtime_holding_symbol_count', 0)} | "
        f"보유 섹터수={trade_runtime.get('runtime_holding_sector_count', 0)} | "
        f"보유 요약={trade_runtime.get('runtime_holding_sector_summary', '-') or '-'} | "
        f"브로커 메모={trade_runtime.get('runtime_broker_ready_note', '-') or '-'} | "
        f"보류 알림 전송={trade_runtime.get('runtime_slack_notify_trade_skip')} | "
        f"웹훅 힌트={trade_runtime.get('runtime_slack_webhook_hint', '-') or '-'}"
    )
    lines.append(
        f"- 주문 알림: 감사로그 {'ok' if trade_runtime.get('trade_audit_exists') else 'missing'} ({trade_audit_age_text}) | "
        f"마지막 감사 {trade_runtime.get('last_trade_audit_action', '-') or '-'} {trade_runtime.get('last_trade_audit_symbol', '')} {trade_runtime.get('last_trade_audit_result', '')}".strip()
        + (
            f" | 섹터 {trade_runtime.get('last_trade_audit_sector', '-') or '-'}"
            f" | 맥락 {trade_runtime.get('last_trade_audit_alignment_label', '-') or '-'}"
            f" | 시장 {trade_runtime.get('last_trade_audit_market_mode', '-') or '-'}"
        )
        + " | "
        + f"슬랙 {'ok' if trade_runtime.get('slack_delivery_exists') else 'missing'} ({slack_delivery_age_text}) | "
        f"마지막 슬랙 {trade_runtime.get('last_slack_delivery_status', '-') or '-'}"
    )
    if trade_runtime.get("runtime_last_record_close_swing_decision") or trade_runtime.get("runtime_last_record_close_swing_reason"):
        lines.append(
            f"- 최근 종가 매매: 판단 {trade_runtime.get('runtime_last_record_close_swing_decision', '-') or '-'} | "
            f"사유 {trade_runtime.get('runtime_last_record_close_swing_reason', '-') or '-'} | "
            f"섹터 {trade_runtime.get('runtime_last_record_sector', '-') or '-'} | "
            f"맥락 {trade_runtime.get('runtime_last_record_alignment_label', '-') or '-'} | "
            f"섹터결론 {trade_runtime.get('runtime_last_record_hybrid_sector_final_label', '-') or '-'}"
        )
    if trade_runtime.get("engine_note"):
        lines.append(
            f"- 브로커 모드: 설정 모의투자={trade_runtime.get('config_kis_virtual')} | "
            f"설정 dry_run={trade_runtime.get('config_dry_run')} | "
            f"엔진={trade_runtime.get('engine_note')}"
        )
    lines.append(
        f"- 이벤트 재생 가격반영: {checks['event']['replay_priced']} | "
        f"감시 호출: {checks['event_watcher']['engine_calls']} | "
        f"WS 재생 스냅샷: {checks['ws_replay']['snapshot_logs']}"
    )
    flow_logs = checks.get("flow_logs", {}) or {}
    flow_snapshot_age_text = _fmt_age_with_suffix(flow_logs.get("latest_snapshot_age_min"))
    lines.append(
        f"- 수급 로그: 이벤트 {flow_logs.get('event_lines', 0)} | "
        f"틱 {flow_logs.get('flow_tick_lines', 0)} | "
        f"스냅샷 {flow_logs.get('snapshot_lines', 0)} | "
        f"스냅샷 경과 {flow_snapshot_age_text} | "
        f"업데이트 {flow_logs.get('health_total_updates', 0)}"
    )
    diagnosis = _diagnose_flow_logs(flow_logs)
    lines.append(
        f"- 수급 진단: {diagnosis.get('status')} | "
        f"이유: {diagnosis.get('reason')} | "
        f"조치: {diagnosis.get('action')}"
    )
    lines.append(
        f"- 애널 캐시: {analyst_cache.get('status')} | "
        f"예열 {analyst_cache.get('warmed_symbols', 0)}/{analyst_cache.get('target_symbols', 0)} | "
        f"경과 {analyst_cache.get('minutes_since_warm', '-')}m | "
        f"점수기준 경과 {scored_age_text}"
    )
    lines.append(
        f"- 일일 마트: {mart_health.get('status')} | 행 {mart_health.get('row_count', 0)} | "
        f"이벤트 {mart_health.get('event_coverage', 0)} | 장중 {mart_health.get('intraday_coverage', 0)} | "
        f"미분류섹터 {mart_health.get('unknown_sector_count', 0)} | 경과 {mart_age_text}"
    )
    api_age_text = _fmt_age_with_suffix(api_health.get("age_min"))
    lines.append(
        f"- api integrations: {api_health.get('status')} | age {api_age_text} | "
        f"DART {api_health.get('dart_ok')} | KIS {api_health.get('kis_ok')} ({api_health.get('kis_source') or '-'}) | "
        f"매크로 {api_health.get('macro_ok')} | 슬랙 {api_health.get('slack_ok')} | "
        f"경고 {api_health.get('warning_count', 0)}"
    )
    if analyst_cache.get("sample_symbols"):
        lines.append("- 애널 캐시 표본: " + ", ".join((analyst_cache.get("sample_symbols") or [])[:8]))
    analyst_alerts = _build_analyst_cache_alerts(pm2_rows, analyst_cache)
    runtime_alerts = _build_runtime_alerts(checks, mart_health, pm2_rows, api_health)
    if analyst_alerts or runtime_alerts:
        lines.append("*경고*")
        for alert in analyst_alerts + runtime_alerts:
            lines.append(f"- {alert}")
    lines.append("*PM2*")
    for row in pm2_rows[:12]:
        lines.append(f"- {row.get('name')}: {row.get('status')} | restarts={row.get('restarts', 0)}")
    return "\n".join(lines)


def build_and_send(args: argparse.Namespace) -> None:
    checks = run_lite_checks() if args.mode == "lite" else run_all_checks()
    pm2_rows = _load_pm2_health()
    analyst_cache = _load_analyst_cache_health()
    mart_health = _load_mart_health()
    api_health = _load_api_integration_health()
    should_refresh_api = (
        args.mode == "full"
        or api_health.get("status") in {"missing", "stale"}
        or api_health.get("age_min") in (None, "", "None")
    )
    if should_refresh_api:
        try:
            save_api_integration_report(run_api_integration_checks("005930"))
        except Exception as exc:
            log.warning("api integration refresh failed during full healthcheck: %s", str(exc)[:160])
    api_health = _load_api_integration_health()
    digest = build_digest(checks, pm2_rows, analyst_cache, mart_health, api_health)
    title = f"[Health:{args.mode}] Runtime Check {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
    if args.print_only:
        print(title)
        print(digest)
        return
    send_slack(digest, title=title, msg_type="info")


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("Healthcheck scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            try:
                build_and_send(args)
            except Exception as exc:
                log.exception("healthcheck reporter failed")
                if not args.print_only:
                    notify_error("Healthcheck Reporter", str(exc))
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
