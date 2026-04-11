from __future__ import annotations

import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import close_swing_selector as selector
import event_trade_watcher as watcher_mod


class _FakeBroker:
    def __init__(self, *, last_price: float, price_change_rate: float = 1.2) -> None:
        self._last_price = last_price
        self._price_change_rate = price_change_rate

    def get_last_price(self, symbol: str):
        return self._last_price

    def get_price_change_rate(self, symbol: str):
        return self._price_change_rate


def _selector_inputs() -> dict:
    symbol = "005930"
    return {
        "market_briefing": {
            "confidence": {"score": 50},
            "positioning": {"mode": "중립"},
            "data_quality": {"label": "양호"},
        },
        "briefing_lookup": {},
        "card_lookup": {
            symbol: {
                "card_score": 0.72,
                "active_source_count": 3,
                "flow_state_score": 0.20,
                "flow_intraday_edge_score": 0.03,
                "ml_pred_return_5d": 0.0,
                "event_expected_alpha_5d": 0.0,
            }
        },
        "factor_lookup": {
            symbol: {
                "composite_score": 0.60,
                "liquidity_score": 0.50,
                "avg_turnover_20d": 0.003,
            }
        },
    }


def _selector_record() -> dict:
    return {
        "stock_code": "005930",
        "event_type": "PERF_PRELIM",
        "signal_bias": "positive",
        "event_date": "",
        "event_time_hhmm": "",
    }


def _approved_row(*, stock_code: str = "005930", budget_krw: int = 180000, min_order_budget_krw: int = 180000) -> dict:
    return {
        "stock_code": stock_code,
        "rcp_no": f"R{stock_code}",
        "src": "EVENT_COLLECTOR_API",
        "event_type": "PERF_PRELIM",
        "event_date": "2026-04-03",
        "event_time_hhmm": "15:25",
        "close_swing_eligible": True,
        "close_swing_decision": "close_swing_ready",
        "close_swing_reason": "close_swing_ready",
        "close_swing_note": "base note",
        "close_swing_support_score": 6,
        "close_swing_ranking_score": 500.0,
        "close_swing_event_age_minutes": 10,
        "close_swing_budget_krw": budget_krw,
        "close_swing_min_order_budget_krw": min_order_budget_krw,
        "close_swing_budget_adjusted": True,
        "context_sector": "",
        "sector": "",
    }


def _watcher_selector_inputs() -> dict:
    return {
        "market_briefing": {
            "confidence": {"score": 80},
            "positioning": {"mode": "중립"},
            "data_quality": {"label": "양호"},
        }
    }


class CloseSwingBudgetFloorTests(unittest.TestCase):
    def test_selector_adjusts_budget_to_one_share_floor(self) -> None:
        broker = _FakeBroker(last_price=176300)
        settings = replace(selector.SETTINGS, MAX_KRW_PER_TRADE=300000)
        with patch.object(selector, "SETTINGS", settings):
            result = selector.evaluate_close_swing_candidate(
                _selector_record(),
                broker=broker,
                inputs=_selector_inputs(),
                context={"alignment_score": 0},
            )
        self.assertTrue(result["eligible"])
        self.assertEqual(result["budget_krw"], 180000)
        self.assertEqual(result["min_order_budget_krw"], 180000)
        self.assertTrue(result["budget_adjusted"])

    def test_selector_rounds_one_share_floor_to_next_10000(self) -> None:
        broker = _FakeBroker(last_price=199900)
        settings = replace(selector.SETTINGS, MAX_KRW_PER_TRADE=300000)
        with patch.object(selector, "SETTINGS", settings):
            result = selector.evaluate_close_swing_candidate(
                _selector_record(),
                broker=broker,
                inputs=_selector_inputs(),
                context={"alignment_score": 0},
            )
        self.assertTrue(result["eligible"])
        self.assertEqual(result["budget_krw"], 200000)
        self.assertEqual(result["min_order_budget_krw"], 200000)
        self.assertTrue(result["budget_adjusted"])

    def test_selector_blocks_when_one_share_exceeds_max_trade_budget(self) -> None:
        broker = _FakeBroker(last_price=310100)
        settings = replace(selector.SETTINGS, MAX_KRW_PER_TRADE=300000)
        with patch.object(selector, "SETTINGS", settings):
            result = selector.evaluate_close_swing_candidate(
                _selector_record(),
                broker=broker,
                inputs=_selector_inputs(),
                context={"alignment_score": 0},
            )
        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "max_trade_budget_below_one_share")
        self.assertEqual(result["min_order_budget_krw"], 320000)
        self.assertFalse(result["budget_adjusted"])

    def test_filter_blocks_when_daily_budget_falls_below_one_share(self) -> None:
        settings = replace(
            watcher_mod.SETTINGS,
            CLOSE_SWING_MAX_BUDGET_PER_DAY_KRW=200000,
            CLOSE_SWING_MAX_TRADES_PER_DAY=3,
            CLOSE_SWING_MAX_CANDIDATES_PER_CYCLE=3,
            CLOSE_SWING_MAX_OPEN_POSITIONS=4,
            CLOSE_SWING_MAX_TRADES_PER_SECTOR_PER_DAY=2,
            CLOSE_SWING_MAX_CANDIDATES_PER_SECTOR_PER_CYCLE=2,
            CLOSE_SWING_MAX_OPEN_NAMES_PER_SECTOR=2,
            EVENT_TRADE_RESPECT_AVAILABLE_CASH=True,
            EVENT_TRADE_MIN_CASH_BUFFER_KRW=50000,
        )
        with patch.object(watcher_mod, "SETTINGS", settings):
            watcher = watcher_mod.EventLogTradeWatcher(SimpleNamespace(broker_live=None))
            watcher._current_holding_profile = lambda: {"symbols": set(), "sector_counts": {}, "sector_symbols": {}}
            watcher._last_broker_cash_krw = 500000
            watcher.state._payload = {
                "handled": {
                    "prev": {
                        "event_date": "2026-04-03",
                        "traded": True,
                        "budget_krw": 50000,
                        "stock_code": "000001",
                        "sector": "",
                    }
                }
            }
            selected, blocked, _ = watcher._filter_and_rank_approved(
                [_approved_row()],
                selector_inputs=_watcher_selector_inputs(),
            )
        self.assertEqual(selected, [])
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["close_swing_reason"], "daily_budget_below_one_share")

    def test_quote_fallback_budget_cap_blocks_below_one_share(self) -> None:
        settings = replace(watcher_mod.SETTINGS, CLOSE_SWING_DEGRADED_MAX_BUDGET_PER_TRADE_KRW=150000)
        with patch.object(watcher_mod, "SETTINGS", settings):
            watcher = watcher_mod.EventLogTradeWatcher(SimpleNamespace(broker_live=None))
            trimmed, blocked = watcher._apply_quote_fallback_budget_guard(
                [_approved_row()],
                [],
                degraded_limit=1,
                degraded_budget_cap=150000,
                quote_source="naver_fallback",
            )
        self.assertEqual(trimmed, [])
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["close_swing_reason"], "quote_fallback_budget_too_small")

    def test_filter_blocks_when_available_cash_falls_below_one_share(self) -> None:
        settings = replace(
            watcher_mod.SETTINGS,
            CLOSE_SWING_MAX_BUDGET_PER_DAY_KRW=600000,
            CLOSE_SWING_MAX_TRADES_PER_DAY=3,
            CLOSE_SWING_MAX_CANDIDATES_PER_CYCLE=3,
            CLOSE_SWING_MAX_OPEN_POSITIONS=4,
            CLOSE_SWING_MAX_TRADES_PER_SECTOR_PER_DAY=2,
            CLOSE_SWING_MAX_CANDIDATES_PER_SECTOR_PER_CYCLE=2,
            CLOSE_SWING_MAX_OPEN_NAMES_PER_SECTOR=2,
            EVENT_TRADE_RESPECT_AVAILABLE_CASH=True,
            EVENT_TRADE_MIN_CASH_BUFFER_KRW=50000,
        )
        with patch.object(watcher_mod, "SETTINGS", settings):
            watcher = watcher_mod.EventLogTradeWatcher(SimpleNamespace(broker_live=None))
            watcher._current_holding_profile = lambda: {"symbols": set(), "sector_counts": {}, "sector_symbols": {}}
            watcher._last_broker_cash_krw = 200000
            watcher.state._payload = {"handled": {}}
            selected, blocked, _ = watcher._filter_and_rank_approved(
                [_approved_row()],
                selector_inputs=_watcher_selector_inputs(),
            )
        self.assertEqual(selected, [])
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["close_swing_reason"], "available_cash_below_one_share")


if __name__ == "__main__":
    unittest.main()
