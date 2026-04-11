from __future__ import annotations

try:
    from engine_buyback_handler import handle_buyback
    from engine_perf_handler import handle_perf_report
    from engine_sales_handler import handle_sales_variation
    from engine_supply_handler import handle_supply_contract
except Exception:
    from Disclosure.engine_buyback_handler import handle_buyback
    from Disclosure.engine_perf_handler import handle_perf_report
    from Disclosure.engine_sales_handler import handle_sales_variation
    from Disclosure.engine_supply_handler import handle_supply_contract


__all__ = [
    "handle_buyback",
    "handle_perf_report",
    "handle_sales_variation",
    "handle_supply_contract",
]
