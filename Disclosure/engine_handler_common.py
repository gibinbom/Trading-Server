from __future__ import annotations

import logging
from typing import Any, Callable, Optional


log = logging.getLogger("disclosure.engine.handlers")

FinishFn = Callable[..., bool]
GuardFn = Callable[[str, Optional[dict[str, Any]]], Optional[bool]]
FastFetchFn = Callable[[str], Optional[str]]
MarkRecoveryFn = Callable[[bool], None]


def register_recovery(
    *,
    engine,
    broker,
    stock_code: str,
    event_type: str,
    playbook: str,
    start_method: str = "register",
    required_drop_pct: float | None = None,
    required_bounce_pct: float | None = None,
    required_recovery_ratio: float | None = None,
) -> tuple[Optional[float], bool]:
    try:
        base_px = engine._safe_last_price(broker, stock_code)
        if not base_px:
            return None, False
        register_fn = getattr(engine.recovery_monitor, start_method)
        kwargs: dict[str, Any] = {"event_type": event_type, "playbook": playbook}
        if required_drop_pct is not None:
            kwargs["required_drop_pct"] = required_drop_pct
        if required_bounce_pct is not None:
            kwargs["required_bounce_pct"] = required_bounce_pct
        if required_recovery_ratio is not None:
            kwargs["required_recovery_ratio"] = required_recovery_ratio
        register_fn(stock_code, base_px, **kwargs)
        return base_px, True
    except Exception as exc:
        log.warning("⚠️ [RECOVERY-REG-FAIL] %s: %s", stock_code, exc)
        return None, False
