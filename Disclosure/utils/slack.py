import json
import os
import logging
from collections import Counter
from datetime import datetime
from typing import Optional, List, Any, Dict

import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SLACK_DELIVERY_LOG_PATH = os.path.join(BASE_DIR, "logs", "slack_delivery.jsonl")
DEFAULT_SLACK_FALLBACK_LOG_PATH = os.path.join(BASE_DIR, "logs", "slack_fallback.jsonl")
DEFAULT_TRADE_ALERT_AUDIT_PATH = os.path.join(BASE_DIR, "logs", "trade_alert_audit.jsonl")

# 설정 파일이 있으면 가져오고, 없으면 환경변수 사용
try:
    from config import SETTINGS
    SLACK_WEBHOOK_URL = getattr(SETTINGS, "SLACK_WEBHOOK_URL", os.getenv("SLACK_WEBHOOK_URL", ""))
    SLACK_ENABLED = getattr(SETTINGS, "SLACK_ENABLED", True)
    SLACK_BOT_TOKEN = getattr(SETTINGS, "SLACK_BOT_TOKEN", os.getenv("SLACK_BOT_TOKEN", ""))
    SLACK_REPORT_CHANNEL = getattr(SETTINGS, "SLACK_REPORT_CHANNEL", os.getenv("SLACK_REPORT_CHANNEL", ""))
    SLACK_NOTIFY_TRADE_SKIP = getattr(SETTINGS, "SLACK_NOTIFY_TRADE_SKIP", False)
    SLACK_DELIVERY_LOG_PATH = getattr(SETTINGS, "SLACK_DELIVERY_LOG_PATH", DEFAULT_SLACK_DELIVERY_LOG_PATH)
    SLACK_FALLBACK_LOG_PATH = getattr(SETTINGS, "SLACK_FALLBACK_LOG_PATH", DEFAULT_SLACK_FALLBACK_LOG_PATH)
    TRADE_ALERT_AUDIT_PATH = getattr(SETTINGS, "TRADE_ALERT_AUDIT_PATH", DEFAULT_TRADE_ALERT_AUDIT_PATH)
except ImportError:
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
    SLACK_ENABLED = True
    SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
    SLACK_REPORT_CHANNEL = os.getenv("SLACK_REPORT_CHANNEL", "")
    SLACK_NOTIFY_TRADE_SKIP = os.getenv("SLACK_NOTIFY_TRADE_SKIP", "0").strip().lower() in {"1", "true", "yes", "on"}
    SLACK_DELIVERY_LOG_PATH = os.getenv("SLACK_DELIVERY_LOG_PATH", DEFAULT_SLACK_DELIVERY_LOG_PATH)
    SLACK_FALLBACK_LOG_PATH = os.getenv("SLACK_FALLBACK_LOG_PATH", DEFAULT_SLACK_FALLBACK_LOG_PATH)
    TRADE_ALERT_AUDIT_PATH = os.getenv("TRADE_ALERT_AUDIT_PATH", DEFAULT_TRADE_ALERT_AUDIT_PATH)

# --------------------
# Config & Globals
# --------------------
SLACK_TIMEOUT = 5
SLACK_CONNECT_TIMEOUT = 2

# 세션 재사용 (TCP Keep-Alive)
_SESSION: Optional[requests.Session] = None
log = logging.getLogger("bot.slack")

def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
    return _SESSION


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_preview(text: str, limit: int = 240) -> str:
    return (str(text or "").replace("\n", " "))[:limit]


def _mask_webhook(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    parts = [part for part in text.split("/") if part]
    tail = parts[-3:] if len(parts) >= 3 else parts
    if not tail:
        return ""
    masked = []
    for idx, part in enumerate(tail):
        if idx < 2:
            masked.append(part)
            continue
        if len(part) <= 8:
            masked.append("*" * len(part))
        else:
            masked.append(f"{part[:4]}...{part[-4:]}")
    return "/".join(masked)


def get_slack_runtime_meta(webhook_url: Optional[str] = None) -> Dict[str, Any]:
    url = str(webhook_url or SLACK_WEBHOOK_URL or "").strip()
    return {
        "enabled": bool(SLACK_ENABLED),
        "has_webhook": bool(url),
        "webhook_hint": _mask_webhook(url),
        "report_channel": str(SLACK_REPORT_CHANNEL or "").strip(),
        "notify_trade_skip": bool(SLACK_NOTIFY_TRADE_SKIP),
    }


def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as exc:
        log.warning("[SLACK] append log failed: %s", exc)


def _record_delivery(
    status: str,
    *,
    title: str = "",
    msg_type: str = "info",
    detail: str = "",
    http_status: Optional[int] = None,
    webhook_url: str = "",
) -> None:
    _append_jsonl(
        SLACK_DELIVERY_LOG_PATH,
        {
            "timestamp": _now_iso(),
            "status": status,
            "title": title,
            "msg_type": msg_type,
            "has_webhook": bool(webhook_url or SLACK_WEBHOOK_URL),
            "webhook_hint": _mask_webhook(webhook_url or SLACK_WEBHOOK_URL),
            "http_status": http_status,
            "detail": _safe_preview(detail),
        },
    )


def _mirror_failed_alert(*, title: str, text: str, msg_type: str, reason: str, http_status: Optional[int] = None) -> None:
    _append_jsonl(
        SLACK_FALLBACK_LOG_PATH,
        {
            "timestamp": _now_iso(),
            "title": title,
            "msg_type": msg_type,
            "reason": reason,
            "http_status": http_status,
            "text": str(text or ""),
        },
    )


def _record_trade_audit(
    action: str,
    symbol: str,
    qty: int,
    result: str,
    msg: str,
    *,
    delivered: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    context = context or {}
    _append_jsonl(
        TRADE_ALERT_AUDIT_PATH,
        {
            "timestamp": _now_iso(),
            "action": str(action or ""),
            "symbol": str(symbol or ""),
            "qty": int(qty or 0),
            "result": result,
            "delivered": delivered,
            "message": _safe_preview(msg, limit=400),
            "sector": str(context.get("sector") or ""),
            "alignment_label": str(context.get("alignment_label") or ""),
            "alignment_score": int(context.get("alignment_score", 0) or 0),
            "market_mode": str(context.get("market_mode") or ""),
            "context_note": _safe_preview(str(context.get("note") or ""), limit=120),
            "hybrid_sector_regime_score": float(context.get("hybrid_sector_regime_score", 0.0) or 0.0),
            "hybrid_relative_value_score": float(context.get("hybrid_relative_value_score", 0.0) or 0.0),
            "hybrid_timing_score": float(context.get("hybrid_timing_score", 0.0) or 0.0),
            "hybrid_final_trade_score": float(context.get("hybrid_final_trade_score", 0.0) or 0.0),
            "blocked_reason_code": str(context.get("blocked_reason_code") or ""),
            "quote_source": str(context.get("quote_source") or ""),
        },
    )


def _trade_context_lines(context: Optional[Dict[str, Any]]) -> List[str]:
    payload = context or {}
    lines: List[str] = []
    sector = str(payload.get("sector") or "").strip()
    alignment = str(payload.get("alignment_label") or "").strip()
    market_mode = str(payload.get("market_mode") or "").strip()
    confidence = int(payload.get("confidence_score", 0) or 0)
    note = str(payload.get("note") or "").strip()
    if sector:
        lines.append(f"*섹터:* `{sector}`")
    if alignment or market_mode:
        detail = alignment or "-"
        if market_mode:
            detail += f" | {market_mode}"
        if confidence:
            detail += f" {confidence}/100"
        lines.append(f"*맥락:* `{detail}`")
    if note:
        lines.append(f"*맥락 근거:* {note}")
    sector_regime = float(payload.get("hybrid_sector_regime_score", 0.0) or 0.0)
    relative_value = float(payload.get("hybrid_relative_value_score", 0.0) or 0.0)
    timing_score = float(payload.get("hybrid_timing_score", 0.0) or 0.0)
    final_score = float(payload.get("hybrid_final_trade_score", 0.0) or 0.0)
    blocked_reason_code = str(payload.get("blocked_reason_code") or "").strip()
    quote_source = str(payload.get("quote_source") or "").strip()
    if sector_regime or relative_value or timing_score or final_score:
        lines.append(
            f"*하이브리드:* 섹터 `{sector_regime:.1f}` | 상대가치 `{relative_value:.1f}` | 타이밍 `{timing_score:.1f}` | 최종 `{final_score:.1f}`"
        )
    if blocked_reason_code:
        lines.append(f"*차단코드:* `{blocked_reason_code}`")
    if quote_source:
        lines.append(f"*시세소스:* `{quote_source}`")
    return lines

def _chunk_text(s: str, limit: int = 3000) -> List[str]:
    """긴 메시지를 슬랙 제한에 맞춰 자름"""
    s = s or ""
    return [s[i:i + limit] for i in range(0, len(s), limit)] or [""]

# --------------------
# Core: Send Function
# --------------------
def send_slack(
    text: str, 
    title: str = "", 
    msg_type: str = "info", 
    webhook_url: Optional[str] = None
) -> bool:
    """
    기본 슬랙 전송 함수
    :param text: 본문 내용
    :param title: 제목 (굵은 글씨)
    :param msg_type: "info"(기본), "success"(성공), "error"(에러/경고) -> 이모지 자동 추가
    :param webhook_url: 특정 URL로 보내야 할 경우 지정 (없으면 기본값 사용)
    """
    if not SLACK_ENABLED:
        _record_delivery("disabled", title=title, msg_type=msg_type, detail="slack disabled", webhook_url=webhook_url or "")
        _mirror_failed_alert(title=title, text=text, msg_type=msg_type, reason="disabled")
        return False

    url = webhook_url or SLACK_WEBHOOK_URL
    if not url:
        _record_delivery("missing_webhook", title=title, msg_type=msg_type, detail="missing webhook url", webhook_url=url)
        _mirror_failed_alert(title=title, text=text, msg_type=msg_type, reason="missing_webhook")
        return False

    # 아이콘/헤더 장식
    icon_map = {
        "info": "📢",
        "success": "✅",
        "warning": "⚠️",
        "error": "🔥",
        "trade": "💰"
    }
    icon = icon_map.get(msg_type, "📢")
    
    # 메시지 구성
    full_text = ""
    if title:
        full_text += f"{icon} *{title}*\n"
    else:
        full_text += f"{icon} "
        
    full_text += text

    # 긴 메시지는 분할 전송
    chunks = _chunk_text(full_text)
    delivered = True
    
    try:
        sess = _get_session()
        for i, chunk in enumerate(chunks):
            payload = {"text": chunk}
            
            # 첫 번째 청크 이후는 '이어짐' 표시 (선택 사항)
            if i > 0:
                payload["text"] = "..." + chunk

            r = sess.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=(SLACK_CONNECT_TIMEOUT, SLACK_TIMEOUT),
            )
            
            if not r.ok:
                log.warning(f"[SLACK] Send failed: {r.status_code} {r.text[:100]}")
                delivered = False
                _record_delivery(
                    "http_error",
                    title=title,
                    msg_type=msg_type,
                    detail=r.text[:300],
                    http_status=r.status_code,
                    webhook_url=url,
                )
                _mirror_failed_alert(
                    title=title,
                    text=text,
                    msg_type=msg_type,
                    reason=r.text[:300] or "http_error",
                    http_status=r.status_code,
                )
            else:
                _record_delivery(
                    "ok",
                    title=title,
                    msg_type=msg_type,
                    detail=f"chunk={i + 1}/{len(chunks)}",
                    http_status=r.status_code,
                    webhook_url=url,
                )

    except Exception as e:
        log.warning(f"[SLACK] Connection error: {e}")
        delivered = False
        _record_delivery("exception", title=title, msg_type=msg_type, detail=str(e), webhook_url=url)
        _mirror_failed_alert(title=title, text=text, msg_type=msg_type, reason=str(e))

    return delivered

def upload_slack_file(
    file_path: str,
    *,
    title: str = "",
    initial_comment: str = "",
    channels: Optional[Any] = None,
    token: Optional[str] = None,
) -> bool:
    """
    Upload a file to Slack when a bot token is available.
    Falls back quietly when token/channel/file is missing.
    """
    if not SLACK_ENABLED:
        return False

    token = token or SLACK_BOT_TOKEN
    if not token:
        log.info("[SLACK] file upload skipped: SLACK_BOT_TOKEN missing")
        return False
    if not file_path or not os.path.exists(file_path):
        log.warning("[SLACK] file upload skipped: missing file %s", file_path)
        return False

    if channels is None:
        channels = [SLACK_REPORT_CHANNEL] if SLACK_REPORT_CHANNEL else []
    elif isinstance(channels, str):
        channels = [channels]
    else:
        channels = [item for item in channels if item]

    if not channels:
        log.info("[SLACK] file upload skipped: SLACK_REPORT_CHANNEL missing")
        return False

    try:
        sess = _get_session()
        with open(file_path, "rb") as fp:
            response = sess.post(
                "https://slack.com/api/files.upload",
                headers={"Authorization": f"Bearer {token}"},
                data={
                    "channels": ",".join(channels),
                    "title": title or os.path.basename(file_path),
                    "initial_comment": initial_comment or "",
                    "filename": os.path.basename(file_path),
                },
                files={"file": (os.path.basename(file_path), fp)},
                timeout=(SLACK_CONNECT_TIMEOUT, max(SLACK_TIMEOUT, 15)),
            )

        payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        if not response.ok or not payload.get("ok", False):
            log.warning("[SLACK] file upload failed: %s %s", response.status_code, str(payload)[:200])
            return False
        return True
    except Exception as e:
        log.warning(f"[SLACK] file upload error: {e}")
        return False


# --------------------
# Business Helpers
# --------------------

def notify_system_start():
    """봇 시작 알림"""
    send_slack(
        "DART 공시 감시 봇이 시작되었습니다.\n(08:00 ~ 20:00 모니터링)",
        title="System Start",
        msg_type="info"
    )


def notify_main_trader_start(
    *,
    main_run_mode: str,
    auto_trade_enabled: bool,
    close_swing_enabled: bool,
    broker_is_virtual: bool,
    broker_dry_run: bool,
    trade_window: str,
) -> bool:
    """메인 자동매매 런타임 시작 정보를 슬랙에 남깁니다."""
    slack_meta = get_slack_runtime_meta()
    broker_mode = "virtual" if broker_is_virtual else "live"
    order_mode = "dry-run" if broker_dry_run else "real-order"
    lines = [
        "자동매매 메인 트레이더가 시작되었습니다.",
        f"- 실행 모드: {str(main_run_mode or '-').strip() or '-'}",
        f"- 자동매매: {'ON' if auto_trade_enabled else 'OFF'}",
        f"- 진입 방식: {'종가 매매' if close_swing_enabled else '이벤트 전용'}",
        f"- 하이브리드: {'ON' if bool(getattr(SETTINGS, 'HYBRID_ROTATION_ENABLE', False)) else 'OFF'} | {'비교모드만' if bool(getattr(SETTINGS, 'HYBRID_SHADOW_ONLY', True)) else '실주문 게이트'} | 섹터>={int(getattr(SETTINGS, 'HYBRID_ACTIVE_SECTOR_MIN_SCORE', 60) or 60)} | 상대가치>={int(getattr(SETTINGS, 'HYBRID_RELATIVE_VALUE_MIN_SCORE', 55) or 55)}",
        f"- 브로커: {broker_mode} | {order_mode}",
        f"- 주문 시간창: {str(trade_window or '-').strip() or '-'}",
        f"- 종가 매매 위험 기준: 익절 {float(getattr(SETTINGS, 'CLOSE_BET_TAKE_PROFIT_PCT', 8.0)):.1f}% | 손절 {float(getattr(SETTINGS, 'CLOSE_BET_STOP_LOSS_PCT', -4.0)):.1f}% | 유예 {int(getattr(SETTINGS, 'CLOSE_BET_STOP_GRACE_MIN', 60) or 60)}m | 종가회복점검 {'15:30' if bool(getattr(SETTINGS, 'CLOSE_BET_REQUIRE_RECOVER_BY_CLOSE', True)) else 'OFF'} | 장마감정리 {'ON' if bool(getattr(SETTINGS, 'MONITOR_FORCE_EOD_LIQUIDATE', False)) else 'OFF'}",
        f"- 슬랙: {'ON' if slack_meta['enabled'] else 'OFF'} | 웹훅 {slack_meta['webhook_hint'] or 'missing'}",
        f"- 보류 알림 전송: {'ON' if slack_meta['notify_trade_skip'] else 'OFF'}",
    ]
    if slack_meta["report_channel"]:
        lines.append(f"- 슬랙 채널: {slack_meta['report_channel']}")
    return send_slack("\n".join(lines), title="Main Trader Start", msg_type="info")


def notify_trade(
    action: str,
    symbol: str,
    qty: int,
    result_ok: bool,
    msg: str,
    *,
    context: Optional[Dict[str, Any]] = None,
):
    """매매 결과 알림"""
    action_upper = str(action or "").upper()
    if action_upper.startswith("BUY"):
        act_str = "매수(BUY)"
    elif action_upper.startswith("SELL"):
        act_str = "매도(SELL)"
    elif action_upper.startswith("SKIP"):
        act_str = "스킵(SKIP)"
    else:
        act_str = str(action or "UNKNOWN")
    
    body = (
        f"*종목:* `{symbol}`\n"
        f"*수량:* `{qty}`주\n"
        f"*결과:* `{'성공' if result_ok else '실패'}`\n"
        f"*메시지:* {msg}"
    )
    extra = _trade_context_lines(context)
    if extra:
        body += "\n" + "\n".join(extra)
    delivered = send_slack(body, title=f"주문 체결 - {act_str}", msg_type="trade")
    _record_trade_audit(
        action,
        symbol,
        qty,
        "success" if result_ok else "failed",
        msg,
        delivered=delivered,
        context=context,
    )


def notify_trade_skip(symbol: str, reason: str, qty: int = 0, *, context: Optional[Dict[str, Any]] = None):
    """주문 스킵도 감사 로그에 남겨서 알림 누락처럼 보이지 않게 합니다."""
    body = (
        f"*종목:* `{symbol}`\n"
        f"*수량:* `{qty}`주\n"
        f"*결과:* `스킵`\n"
        f"*메시지:* {reason}"
    )
    extra = _trade_context_lines(context)
    if extra:
        body += "\n" + "\n".join(extra)
    delivered = None
    if SLACK_NOTIFY_TRADE_SKIP:
        delivered = send_slack(body, title="주문 보류 - 스킵(SKIP)", msg_type="warning")
    _record_trade_audit("SKIP", symbol, qty, "skipped", reason, delivered=delivered, context=context)


def notify_sector_thesis(
    sector_rotation: Dict[str, Any],
    relative_value: Dict[str, Any],
    shadow_book: Dict[str, Any],
    *,
    sector_thesis: Optional[Dict[str, Any]] = None,
) -> bool:
    active_rows = list((sector_thesis or {}).get("top_sectors") or [])[:5]
    if not active_rows:
        active_rows = list(sector_rotation.get("top_sectors") or [])[:5]
    if not active_rows:
        return False
    lines = [
        f"시장 모드 `{sector_rotation.get('market_mode') or '-'}` | 확신도 `{int(sector_rotation.get('confidence_score', 0) or 0)}` | 활성 섹터 `{len(sector_rotation.get('active_sectors') or [])}`개",
    ]
    for row in active_rows[:4]:
        sector = str(row.get("sector") or "-")
        leader = str(row.get("leader_name") or row.get("leader_symbol") or "-")
        final_score = float(row.get("final_sector_score", row.get("sector_regime_score", 0.0)) or 0.0)
        flow = float(row.get("flow_lens_score", ((row.get("lens_breakdown") or {}).get("flow_lens_score") or 0.0)) or 0.0)
        quant = float(row.get("quant_lens_score", ((row.get("lens_breakdown") or {}).get("quant_lens_score") or 0.0)) or 0.0)
        macro = float(row.get("macro_lens_score", ((row.get("lens_breakdown") or {}).get("macro_lens_score") or 0.0)) or 0.0)
        final_label = str(row.get("final_label") or row.get("agreement_level") or "-")
        lines.append(f"- `{sector}` | 대장주 `{leader}` | 섹터 점수 `{final_score:.1f}`")
        lines.append(f"  세 렌즈 요약: 수급 `{flow:.1f}` / 퀀트 `{quant:.1f}` / 매크로 `{macro:.1f}`")
        lines.append(f"  공통 결론: {final_label}")
        if row.get("action_hint"):
            lines.append(f"  실전 행동: {row.get('action_hint')}")
        elif row.get("human_summary"):
            lines.append(f"  실전 행동: {row.get('human_summary')}")
        bucket = next((item for item in (relative_value.get('sector_buckets') or []) if str(item.get('sector') or '') == sector), None)
        if bucket:
            picks = ", ".join(str(item.get("name") or item.get("symbol") or "") for item in (bucket.get("top_candidates") or [])[:2])
            if picks:
                lines.append(f"  후발주 후보: {picks}")
    live_only = list(shadow_book.get("live_only_symbols") or [])[:3]
    shadow_only = list(shadow_book.get("shadow_only_symbols") or [])[:3]
    if live_only or shadow_only:
        lines.append(
            f"- 실주문만 남은 후보: {', '.join(live_only) if live_only else '-'} | 비교모드만 남은 후보: {', '.join(shadow_only) if shadow_only else '-'}"
        )
    return send_slack("\n".join(lines), title="섹터 결론", msg_type="info")


def notify_trade_candidates(
    approved_rows: List[Dict[str, Any]],
    *,
    blocked_count: int = 0,
    remaining_slots: int = 0,
) -> bool:
    rows = [row for row in (approved_rows or []) if isinstance(row, dict)]
    if not rows:
        return False
    lines = [
        f"주문 전 후보 `{len(rows)}`개 | 보류 `{int(blocked_count or 0)}` | 남은 슬롯 `{int(remaining_slots or 0)}`",
        "아직 실제 주문은 아닙니다. 아래 후보는 종가 매매 필터를 통과한 종목이며, 실제 매수는 별도 `주문 체결 - 매수(BUY)` 알림에서만 확정됩니다.",
    ]
    for row in rows[:5]:
        symbol = str(row.get("stock_code") or "").zfill(6)
        name = str(row.get("corp_name") or row.get("name") or symbol)
        sector = str(row.get("context_sector") or row.get("sector") or "-")
        support = int(row.get("close_swing_support_score", 0) or 0)
        rank = float(row.get("close_swing_ranking_score", 0.0) or 0.0)
        budget = int(row.get("close_swing_budget_krw", 0) or 0)
        tp = row.get("close_swing_take_profit_pct")
        sl = row.get("close_swing_stop_loss_pct")
        grace = int(row.get("close_swing_stop_grace_min", 0) or 0)
        chg = row.get("close_swing_price_change_pct")
        reason = str(row.get("close_swing_reason") or row.get("close_swing_decision") or "-")
        action_note = str(row.get("close_swing_note") or "")
        chg_text = "-" if chg in (None, "") else f"{float(chg):+.2f}%"
        support_reasons = list(row.get("close_swing_support_reasons") or [])
        risk_notes = list(row.get("close_swing_risk_notes") or [])
        hint = ", ".join(str(item) for item in support_reasons[:2] if item)
        body = (
            f"- `{name}({symbol})` | {sector} | 점수 `{support}` | 순위 `{rank:.1f}` | "
            f"예산 `{budget // 10000}만` | 등락 `{chg_text}` | 익절/손절 `{float(tp or 0.0):.1f}/{float(sl or 0.0):.1f}` | {reason}"
        )
        if hint:
            body += f"\n  근거: {hint}"
        if risk_notes:
            body += f"\n  리스크: {', '.join(str(item) for item in risk_notes[:2] if item)}"
        elif grace:
            body += f"\n  리스크: 손절 유예 {grace}분"
        elif action_note:
            body += f"\n  메모: {action_note[:100]}"
        lines.append(body)
    return send_slack("\n".join(lines), title="주문 전 후보 퍼널", msg_type="info")


def notify_trade_funnel(
    approved_rows: List[Dict[str, Any]],
    *,
    blocked_count: int = 0,
    remaining_slots: int = 0,
    shadow_book: Optional[Dict[str, Any]] = None,
) -> bool:
    rows = [row for row in (approved_rows or []) if isinstance(row, dict)]
    shadow_book = shadow_book or {}
    if not rows and not shadow_book:
        return False
    lines = [
        f"실주문 승인 `{len(rows)}`개 | 보류 `{int(blocked_count or 0)}` | 남은 슬롯 `{int(remaining_slots or 0)}` | 비교모드 후보 `{int(shadow_book.get('shadow_chosen_count', 0) or 0)}`",
        "현재 알림은 주문 전 후보 퍼널입니다. 실제 주문 여부는 `체결 결과와 장후 복기` 또는 `주문 체결 - 매수(BUY)` 알림에서만 확정됩니다.",
    ]
    live_only = list(shadow_book.get("live_only_symbols") or [])[:3]
    shadow_only = list(shadow_book.get("shadow_only_symbols") or [])[:3]
    if rows:
        sectors = []
        thesis_labels = []
        wics_statuses = []
        penalties = []
        for row in rows[:5]:
            sector = str(row.get("context_sector") or row.get("sector") or "").strip()
            if sector and sector not in sectors:
                sectors.append(sector)
            thesis = str(row.get("hybrid_sector_final_label") or "").strip()
            if thesis:
                thesis_labels.append(thesis)
            wics_status = str(row.get("hybrid_wics_status_label") or "").strip()
            if wics_status:
                wics_statuses.append(wics_status)
            penalties.append(float(row.get("hybrid_wics_penalty", 0.0) or 0.0))

        top_thesis = Counter(thesis_labels).most_common(1)[0][0] if thesis_labels else "-"
        top_wics = Counter(wics_statuses).most_common(1)[0][0] if wics_statuses else "-"
        avg_penalty = (sum(penalties) / len(penalties)) if penalties else 0.0
        lines.append(
            f"- 오늘 요약: 섹터 결론 `{top_thesis}` | WICS `{top_wics}` | 평균 가격 보수조정 `{avg_penalty:.1f}` | 주요 섹터 `{', '.join(sectors[:3]) or '-'}`"
        )
    if live_only or shadow_only:
        lines.append(
            f"- 실주문만 남은 후보: {', '.join(live_only) if live_only else '-'} | 비교모드만 남은 후보: {', '.join(shadow_only) if shadow_only else '-'}"
        )
    for row in rows[:5]:
        symbol = str(row.get("stock_code") or "").zfill(6)
        name = str(row.get("corp_name") or row.get("name") or symbol)
        sector = str(row.get("context_sector") or row.get("sector") or "-")
        support = int(row.get("close_swing_support_score", 0) or 0)
        rank = float(row.get("close_swing_ranking_score", 0.0) or 0.0)
        budget = int(row.get("close_swing_budget_krw", 0) or 0)
        final_score = float(row.get("hybrid_final_trade_score", 0.0) or 0.0)
        sector_regime = float(row.get("hybrid_sector_regime_score", 0.0) or 0.0)
        sector_final = str(row.get("hybrid_sector_final_label") or "-")
        relative_value = float(row.get("hybrid_relative_value_score", 0.0) or 0.0)
        timing_score = float(row.get("hybrid_timing_score", 0.0) or 0.0)
        flow_lens = float(row.get("hybrid_sector_flow_lens_score", 0.0) or 0.0)
        quant_lens = float(row.get("hybrid_sector_quant_lens_score", 0.0) or 0.0)
        macro_lens = float(row.get("hybrid_sector_macro_lens_score", 0.0) or 0.0)
        wics_status = str(row.get("hybrid_wics_status_label") or "-")
        wics_history = str(row.get("hybrid_wics_history_confidence_label") or "-")
        wics_dynamic_count = int(row.get("hybrid_wics_dynamic_count", 0) or 0)
        wics_dynamic_stability = float(row.get("hybrid_wics_dynamic_stability", 0.0) or 0.0)
        wics_penalty = float(row.get("hybrid_wics_penalty", 0.0) or 0.0)
        wics_note = str(row.get("hybrid_wics_note") or "").strip()
        reason = str(row.get("close_swing_reason") or row.get("close_swing_decision") or "-")
        lines.append(
            f"- `{name}({symbol})` | {sector} | 실주문 `{support}/{rank:.1f}` | 하이브리드 `{sector_regime:.1f}/{relative_value:.1f}/{timing_score:.1f}->{final_score:.1f}` | 예산 `{budget // 10000}만` | {reason}"
        )
        lines.append(
            f"  세 렌즈: 수급 `{flow_lens:.1f}` / 퀀트 `{quant_lens:.1f}` / 매크로 `{macro_lens:.1f}`"
        )
        lines.append(f"  공통 결론: {sector_final}")
        if wics_dynamic_count > 0:
            lines.append(
                f"  WICS 바스켓: 상태 `{wics_status}` / 표본 `{wics_history}` / 동적 후보 `{wics_dynamic_count}` / 동적안정도 `{int(round(wics_dynamic_stability * 100))}/100` / 가격 보수조정 `{wics_penalty:.1f}`"
            )
        else:
            lines.append(
                f"  WICS 바스켓: 상태 `{wics_status}` / 표본 `{wics_history}` / 동적 후보 `-` / 가격 보수조정 `{wics_penalty:.1f}`"
            )
        if wics_note:
            lines.append(f"  WICS 메모: {wics_note}")
        if row.get("hybrid_sector_action_hint"):
            lines.append(f"  실전 행동: {row.get('hybrid_sector_action_hint')}")
    return send_slack("\n".join(lines), title="주문 전 후보 퍼널", msg_type="info")


def notify_trade_candidate_results(
    processed_rows: List[Dict[str, Any]],
    *,
    total_processed: int = 0,
    traded_count: int = 0,
    blocked_count: int = 0,
) -> bool:
    rows = [row for row in (processed_rows or []) if isinstance(row, dict)]
    if not rows:
        return False
    lines = [
        f"후보 처리 `{int(total_processed or len(rows))}`건 | 실제 주문 `{int(traded_count or 0)}`건 | 미체결/스킵 `{max(0, int(total_processed or len(rows)) - int(traded_count or 0))}`건 | 보류 `{int(blocked_count or 0)}`",
        "아래는 후보 알림 이후 실제 엔진 처리 결과입니다.",
    ]
    for row in rows[:5]:
        symbol = str(row.get("symbol") or "").zfill(6)
        name = str(row.get("name") or symbol)
        support = int(row.get("support", 0) or 0)
        rank = float(row.get("rank", 0.0) or 0.0)
        budget = int(row.get("budget", 0) or 0)
        result_label = str(row.get("result_label") or "-")
        strategy = str(row.get("strategy_name") or "")
        reason = str(row.get("reason") or "")
        note = str(row.get("note") or "")
        body = (
            f"- `{name}({symbol})` | 점수 `{support}` | 순위 `{rank:.1f}` | 예산 `{budget // 10000}만` | 결과 `{result_label}`"
        )
        if strategy or reason:
            body += f"\n  엔진: {strategy or '-'} | {reason or '-'}"
        if note:
            body += f"\n  메모: {note[:120]}"
        lines.append(body)
    return send_slack("\n".join(lines), title="체결 결과와 장후 복기", msg_type="info")


def notify_execution_attribution(
    processed_rows: List[Dict[str, Any]],
    *,
    total_processed: int = 0,
    traded_count: int = 0,
    blocked_count: int = 0,
    shadow_book: Optional[Dict[str, Any]] = None,
) -> bool:
    rows = [row for row in (processed_rows or []) if isinstance(row, dict)]
    shadow_book = shadow_book or {}
    if not rows:
        return False
    lines = [
        f"처리 `{int(total_processed or len(rows))}`건 | 실제 주문 `{int(traded_count or 0)}`건 | 보류 `{int(blocked_count or 0)}` | 비교모드 후보 `{int(shadow_book.get('shadow_chosen_count', 0) or 0)}`",
    ]
    sectors = []
    thesis_labels = []
    wics_statuses = []
    penalties = []
    for row in rows[:5]:
        sector = str(row.get("context_sector") or row.get("sector") or "").strip()
        if sector and sector not in sectors:
            sectors.append(sector)
        thesis = str(row.get("hybrid_sector_final_label") or "").strip()
        if thesis:
            thesis_labels.append(thesis)
        wics_status = str(row.get("hybrid_wics_status_label") or "").strip()
        if wics_status:
            wics_statuses.append(wics_status)
        penalties.append(float(row.get("hybrid_wics_penalty", 0.0) or 0.0))
    top_thesis = Counter(thesis_labels).most_common(1)[0][0] if thesis_labels else "-"
    top_wics = Counter(wics_statuses).most_common(1)[0][0] if wics_statuses else "-"
    avg_penalty = (sum(penalties) / len(penalties)) if penalties else 0.0
    lines.append(
        f"- 오늘 요약: 섹터 결론 `{top_thesis}` | WICS `{top_wics}` | 평균 가격 보수조정 `{avg_penalty:.1f}` | 주요 섹터 `{', '.join(sectors[:3]) or '-'}`"
    )
    for row in rows[:5]:
        symbol = str(row.get("symbol") or "").zfill(6)
        name = str(row.get("name") or symbol)
        support = int(row.get("support", 0) or 0)
        rank = float(row.get("rank", 0.0) or 0.0)
        budget = int(row.get("budget", 0) or 0)
        final_score = float(row.get("hybrid_final_trade_score", 0.0) or 0.0)
        sector_regime = float(row.get("hybrid_sector_regime_score", 0.0) or 0.0)
        sector_final = str(row.get("hybrid_sector_final_label") or "-")
        relative_value = float(row.get("hybrid_relative_value_score", 0.0) or 0.0)
        timing_score = float(row.get("hybrid_timing_score", 0.0) or 0.0)
        flow_lens = float(row.get("hybrid_sector_flow_lens_score", 0.0) or 0.0)
        quant_lens = float(row.get("hybrid_sector_quant_lens_score", 0.0) or 0.0)
        macro_lens = float(row.get("hybrid_sector_macro_lens_score", 0.0) or 0.0)
        wics_status = str(row.get("hybrid_wics_status_label") or "-")
        wics_history = str(row.get("hybrid_wics_history_confidence_label") or "-")
        wics_dynamic_count = int(row.get("hybrid_wics_dynamic_count", 0) or 0)
        wics_dynamic_stability = float(row.get("hybrid_wics_dynamic_stability", 0.0) or 0.0)
        wics_penalty = float(row.get("hybrid_wics_penalty", 0.0) or 0.0)
        wics_note = str(row.get("hybrid_wics_note") or "").strip()
        result_label = str(row.get("result_label") or "-")
        strategy = str(row.get("strategy_name") or "")
        reason = str(row.get("reason") or "")
        lines.append(
            f"- `{name}({symbol})` | 결과 `{result_label}` | 실주문 `{support}/{rank:.1f}` | 하이브리드 `{sector_regime:.1f}/{relative_value:.1f}/{timing_score:.1f}->{final_score:.1f}` | 예산 `{budget // 10000}만`"
        )
        lines.append(
            f"  세 렌즈: 수급 `{flow_lens:.1f}` / 퀀트 `{quant_lens:.1f}` / 매크로 `{macro_lens:.1f}`"
        )
        lines.append(f"  공통 결론: {sector_final}")
        if wics_dynamic_count > 0:
            lines.append(
                f"  WICS 바스켓: 상태 `{wics_status}` / 표본 `{wics_history}` / 동적 후보 `{wics_dynamic_count}` / 동적안정도 `{int(round(wics_dynamic_stability * 100))}/100` / 가격 보수조정 `{wics_penalty:.1f}`"
            )
        else:
            lines.append(
                f"  WICS 바스켓: 상태 `{wics_status}` / 표본 `{wics_history}` / 동적 후보 `-` / 가격 보수조정 `{wics_penalty:.1f}`"
            )
        if wics_note:
            lines.append(f"  WICS 메모: {wics_note}")
        if row.get("hybrid_sector_action_hint"):
            lines.append(f"  실전 행동: {row.get('hybrid_sector_action_hint')}")
        if strategy or reason:
            lines.append(f"  엔진: {strategy or '-'} | {reason or '-'}")
    return send_slack("\n".join(lines), title="체결 결과와 장후 복기", msg_type="info")

def notify_error(context: str, error_msg: str):
    """에러 발생 알림"""
    body = f"*Context:* {context}\n```{error_msg[:1000]}```"
    send_slack(body, title="치명적 오류", msg_type="error")

def notify_disclosure(time_str: str, company: str, title: str, link: str):
    """(옵션) 관심 공시 발견 알림"""
    body = f"[{time_str}] {company}\n<{link}|{title}>"
    send_slack(body, title="새 공시 감지", msg_type="info")
