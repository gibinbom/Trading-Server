from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict
from zoneinfo import ZoneInfo


log = logging.getLogger("disclosure.event_trade_state")
KST = ZoneInfo("Asia/Seoul")


def _now_kst() -> datetime:
    return datetime.now(tz=KST)


class EventTradeState:
    def __init__(self, path: str):
        self.path = path
        self._payload = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if not os.path.exists(self.path):
            return {"handled": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if isinstance(payload, dict):
                payload.setdefault("handled", {})
                return payload
        except Exception as exc:
            log.warning("trade state load failed: %s", exc)
        return {"handled": {}}

    @property
    def handled(self) -> Dict[str, Dict[str, Any]]:
        return self._payload["handled"]

    def should_process(self, key: str, retry_cooldown_sec: int) -> bool:
        row = self.handled.get(key)
        if not row:
            return True

        if str(row.get("status") or "") == "processed":
            return False

        attempted_at = row.get("attempted_at")
        if not attempted_at:
            return True

        try:
            attempted_dt = datetime.fromisoformat(str(attempted_at))
            elapsed = (_now_kst() - attempted_dt).total_seconds()
            row_cooldown = int(row.get("recheck_after_sec") or 0)
            effective_cooldown = row_cooldown if row_cooldown > 0 else int(retry_cooldown_sec)
            return elapsed >= max(15, effective_cooldown)
        except Exception:
            return True

    def mark_processed(
        self,
        key: str,
        record: Dict[str, Any],
        *,
        traded: bool,
        decision: str = "",
        reason: str = "",
        note: str = "",
    ) -> None:
        self.handled[key] = {
            "status": "processed",
            "attempted_at": _now_kst().isoformat(timespec="seconds"),
            "traded": bool(traded),
            "stock_code": str(record.get("stock_code") or "").zfill(6),
            "rcp_no": str(record.get("rcp_no") or ""),
            "event_type": str(record.get("event_type") or ""),
            "event_date": str(record.get("event_date") or ""),
            "sector": str(record.get("context_sector") or record.get("sector") or ""),
            "alignment_label": str(record.get("context_alignment_label") or ""),
            "hybrid_sector": str(record.get("hybrid_sector") or record.get("context_sector") or record.get("sector") or ""),
            "hybrid_shadow_decision": str(record.get("hybrid_shadow_decision") or ""),
            "hybrid_blocked_reason_code": str(record.get("hybrid_blocked_reason_code") or ""),
            "hybrid_sector_regime_score": float(record.get("hybrid_sector_regime_score", 0.0) or 0.0),
            "hybrid_relative_value_score": float(record.get("hybrid_relative_value_score", 0.0) or 0.0),
            "hybrid_timing_score": float(record.get("hybrid_timing_score", 0.0) or 0.0),
            "hybrid_final_trade_score": float(record.get("hybrid_final_trade_score", 0.0) or 0.0),
            "budget_krw": int(record.get("close_swing_budget_krw", 0) or 0),
            "src": str(record.get("src") or ""),
            "decision": str(decision or ""),
            "reason": str(reason or "")[:200],
            "note": str(note or "")[:240],
        }

    def mark_retry(
        self,
        key: str,
        record: Dict[str, Any],
        *,
        error: str,
        decision: str = "",
        note: str = "",
        recheck_after_sec: int = 0,
    ) -> None:
        prev_attempts = int(self.handled.get(key, {}).get("attempts") or 0)
        self.handled[key] = {
            "status": "retry",
            "attempted_at": _now_kst().isoformat(timespec="seconds"),
            "attempts": prev_attempts + 1,
            "error": error[:200],
            "decision": str(decision or "")[:120],
            "note": str(note or "")[:240],
            "recheck_after_sec": int(max(0, recheck_after_sec or 0)),
            "stock_code": str(record.get("stock_code") or "").zfill(6),
            "rcp_no": str(record.get("rcp_no") or ""),
            "event_type": str(record.get("event_type") or ""),
            "event_date": str(record.get("event_date") or ""),
            "sector": str(record.get("context_sector") or record.get("sector") or ""),
            "alignment_label": str(record.get("context_alignment_label") or ""),
            "hybrid_sector": str(record.get("hybrid_sector") or record.get("context_sector") or record.get("sector") or ""),
            "hybrid_shadow_decision": str(record.get("hybrid_shadow_decision") or ""),
            "hybrid_blocked_reason_code": str(record.get("hybrid_blocked_reason_code") or ""),
            "hybrid_sector_regime_score": float(record.get("hybrid_sector_regime_score", 0.0) or 0.0),
            "hybrid_relative_value_score": float(record.get("hybrid_relative_value_score", 0.0) or 0.0),
            "hybrid_timing_score": float(record.get("hybrid_timing_score", 0.0) or 0.0),
            "hybrid_final_trade_score": float(record.get("hybrid_final_trade_score", 0.0) or 0.0),
            "budget_krw": int(record.get("close_swing_budget_krw", 0) or 0),
            "src": str(record.get("src") or ""),
        }

    def traded_budget_for_date(self, event_date: str) -> int:
        if not event_date:
            return 0
        total = 0
        for row in self.handled.values():
            if not isinstance(row, dict):
                continue
            if str(row.get("event_date") or "") != event_date:
                continue
            if row.get("traded") is not True:
                continue
            total += int(row.get("budget_krw") or 0)
        return int(total)

    def attempts_for_key(self, key: str) -> int:
        row = self.handled.get(key) or {}
        return int(row.get("attempts") or 0)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as fp:
            json.dump(self._payload, fp, ensure_ascii=False, indent=2)
