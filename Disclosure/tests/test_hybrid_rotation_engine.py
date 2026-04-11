from __future__ import annotations

import unittest

import pandas as pd

from Disclosure.hybrid_rotation_engine import (
    annotate_event_candidates_with_hybrid,
    compute_relative_value_candidates,
    compute_sector_rotation,
    finalize_shadow_book,
)
from Disclosure.sector_thesis import build_sector_thesis


def _base_inputs(*, history_days: int = 1, conservative: bool = False) -> dict:
    mart_df = pd.DataFrame(
        [
            {
                "symbol": "000001",
                "name": "리더",
                "sector": "조선/해양",
                "composite_score": 0.72,
                "card_score": 0.78,
                "value_score": 0.45,
                "momentum_score": 0.78,
                "quality_score": 0.55,
                "flow_score": 0.62,
                "news_score": 0.40,
                "active_source_count": 4,
                "macro_sector_score": 6.0,
                "analyst_target_upside_pct": 8.0,
                "analyst_peer_alpha_5d": 0.01,
                "analyst_peer_support_count": 1,
                "event_expected_alpha_5d": 0.00,
            },
            {
                "symbol": "000002",
                "name": "후발주",
                "sector": "조선/해양",
                "composite_score": 0.67,
                "card_score": 0.74,
                "value_score": 0.88,
                "momentum_score": 0.62,
                "quality_score": 0.62,
                "flow_score": 0.71,
                "news_score": 0.55,
                "active_source_count": 3,
                "macro_sector_score": 6.0,
                "analyst_target_upside_pct": 36.0,
                "analyst_peer_alpha_5d": 0.03,
                "analyst_peer_support_count": 3,
                "event_expected_alpha_5d": 0.03,
            },
        ]
    )
    factor_df = pd.DataFrame(
        [
            {
                "symbol": "000001",
                "sector": "조선/해양",
                "value_score": 0.45,
                "momentum_score": 0.78,
                "quality_score": 0.55,
                "flow_score": 0.62,
                "news_score": 0.40,
                "composite_score": 0.72,
                "liquidity_score": 0.82,
                "avg_turnover_20d": 0.01,
                "sector_leader_rank": 0.96,
                "sector_reversion_signal": 1.0,
                "factor_source_coverage_ratio": 0.9,
            },
            {
                "symbol": "000002",
                "sector": "조선/해양",
                "value_score": 0.88,
                "momentum_score": 0.62,
                "quality_score": 0.62,
                "flow_score": 0.71,
                "news_score": 0.55,
                "composite_score": 0.67,
                "liquidity_score": 0.76,
                "avg_turnover_20d": 0.008,
                "sector_leader_rank": 0.28,
                "sector_reversion_signal": 4.0,
                "factor_source_coverage_ratio": 0.9,
            },
        ]
    )
    card_df = mart_df.copy()
    briefing = {
        "confidence": {"score": 76},
        "positioning": {"mode": "중립"},
        "data_quality": {
            "label": "보수적" if conservative else "양호",
            "warnings": ["stale inputs"] if conservative else [],
        },
        "context_alignment": {
            "market_mode": "공격적",
            "wics_history_day_count": history_days,
            "top_support": [{"sector": "조선/해양", "score": 2}],
            "top_risk": [],
        },
        "freshness": {
            "factor": {"status": "fresh"},
            "card": {"status": "fresh"},
            "mart": {"status": "fresh"},
            "event": {"status": "fresh"},
            "macro": {"status": "fresh"},
            "wics": {"status": "fresh"},
        },
    }
    wics_report = {
        "top_rotation_sectors": [
            {"sector_short": "조선/해양", "score": 22.0, "top_pick": "리더"},
        ],
        "risk_sectors": [],
    }
    return {
        "mart_df": mart_df,
        "factor_df": factor_df,
        "card_df": card_df,
        "market_briefing": briefing,
        "wics_report": wics_report,
        "api_integration": {"kis": {"source": "primary"}},
    }


class HybridRotationEngineTests(unittest.TestCase):
    def test_sector_thesis_marks_common_bullish_when_three_lenses_align(self) -> None:
        inputs = _base_inputs(history_days=20)
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        thesis = build_sector_thesis(sector_rotation=sector, relative_value=relative)
        row = thesis["top_sectors"][0]
        self.assertEqual(row["final_label"], "공통 우호")
        self.assertEqual(row["agreement_level"], "정렬")

    def test_sector_thesis_marks_flow_only_case_as_short_term_rotation(self) -> None:
        inputs = _base_inputs(history_days=20)
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        sector["sectors"][0]["wics_component"] = 84.0
        sector["sectors"][0]["breadth_stability_component"] = 78.0
        sector["sectors"][0]["factor_card_component"] = 43.0
        sector["sectors"][0]["macro_context_component"] = 48.0
        sector["sectors"][0]["wics_rotation_score"] = 22.0
        thesis = build_sector_thesis(sector_rotation=sector, relative_value=relative)
        row = thesis["top_sectors"][0]
        self.assertEqual(row["final_label"], "단기 순환 우세")
        self.assertIn("flow_short_term_only", row["reason_codes"])

    def test_sector_thesis_marks_quant_advantage_when_flow_is_weak(self) -> None:
        inputs = _base_inputs(history_days=20)
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        sector["sectors"][0]["wics_component"] = 40.0
        sector["sectors"][0]["breadth_stability_component"] = 42.0
        sector["sectors"][0]["factor_card_component"] = 83.0
        sector["sectors"][0]["macro_context_component"] = 63.0
        sector["sectors"][0]["wics_rotation_score"] = 0.0
        thesis = build_sector_thesis(sector_rotation=sector, relative_value=relative)
        row = thesis["top_sectors"][0]
        self.assertEqual(row["final_label"], "체력 우위")
        self.assertIn("quant_strong_but_sector_cold", row["reason_codes"])

    def test_sector_thesis_marks_macro_only_case_as_macro_supportive(self) -> None:
        inputs = _base_inputs(history_days=20)
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        sector["sectors"][0]["wics_component"] = 38.0
        sector["sectors"][0]["breadth_stability_component"] = 40.0
        sector["sectors"][0]["factor_card_component"] = 49.0
        sector["sectors"][0]["macro_context_component"] = 84.0
        sector["sectors"][0]["wics_rotation_score"] = 0.0
        thesis = build_sector_thesis(sector_rotation=sector, relative_value=relative)
        row = thesis["top_sectors"][0]
        self.assertEqual(row["final_label"], "매크로 우호")
        self.assertIn("macro_support_but_no_flow", row["reason_codes"])

    def test_sector_thesis_marks_wics_shallow_history(self) -> None:
        inputs = _base_inputs(history_days=1)
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        thesis = build_sector_thesis(sector_rotation=sector, relative_value=relative)
        row = thesis["top_sectors"][0]
        self.assertLess(row["flow_confidence"], 70.0)
        self.assertIn("wics_shallow_history", row["reason_codes"])

    def test_wics_soft_prior_reduces_extreme_scores_with_low_history(self) -> None:
        shallow = compute_sector_rotation(_base_inputs(history_days=1))
        deep = compute_sector_rotation(_base_inputs(history_days=20))
        shallow_row = shallow["sectors"][0]
        deep_row = deep["sectors"][0]
        self.assertLess(shallow_row["wics_component"], deep_row["wics_component"])
        self.assertGreaterEqual(shallow_row["wics_component"], 50.0)

    def test_data_quality_conservative_lowers_sector_score(self) -> None:
        normal = compute_sector_rotation(_base_inputs(conservative=False))
        conservative = compute_sector_rotation(_base_inputs(conservative=True))
        self.assertLess(
            conservative["sectors"][0]["sector_regime_score"],
            normal["sectors"][0]["sector_regime_score"],
        )

    def test_lag_score_is_zero_when_momentum_is_weak(self) -> None:
        inputs = _base_inputs(history_days=20)
        inputs["mart_df"].loc[1, "momentum_score"] = 0.30
        inputs["factor_df"].loc[1, "momentum_score"] = 0.30
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        target = next(row for row in relative["candidates"] if row["symbol"] == "000002")
        self.assertEqual(target["lag_score"], 0.0)

    def test_underleader_can_rank_above_leader(self) -> None:
        inputs = _base_inputs(history_days=20)
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        top = relative["top_candidates"][0]
        self.assertEqual(top["symbol"], "000002")

    def test_quote_fallback_penalizes_timing_and_marks_source(self) -> None:
        inputs = _base_inputs(history_days=20)
        inputs["api_integration"] = {"kis": {"source": "naver_fallback"}}
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        thesis = build_sector_thesis(sector_rotation=sector, relative_value=relative)
        records = [
            {
                "stock_code": "000002",
                "sector": "조선/해양",
                "context_sector": "조선/해양",
                "close_swing_support_score": 7,
                "close_swing_ranking_score": 620.0,
                "close_swing_recovering": True,
                "close_swing_price_change_pct": 1.2,
                "close_swing_eligible": True,
            }
        ]
        annotated = annotate_event_candidates_with_hybrid(
            records,
            sector_rotation=sector,
            relative_value=relative,
            sector_thesis=thesis,
            inputs=inputs,
        )
        row = annotated[0]
        self.assertEqual(row["hybrid_quote_source"], "naver_fallback")
        self.assertGreater(row["hybrid_timing_penalty"], 0.0)
        self.assertIn("quote_penalty naver_fallback", row["hybrid_timing_notes"])
        self.assertTrue(row["hybrid_sector_final_label"])

    def test_finalize_shadow_book_applies_cycle_and_sector_limits(self) -> None:
        inputs = _base_inputs(history_days=20)
        sector = compute_sector_rotation(inputs)
        relative = compute_relative_value_candidates(inputs, sector)
        thesis = build_sector_thesis(sector_rotation=sector, relative_value=relative)
        records = [
            {
                "stock_code": "000002",
                "rcp_no": "A",
                "sector": "조선/해양",
                "context_sector": "조선/해양",
                "close_swing_support_score": 8,
                "close_swing_ranking_score": 700.0,
                "close_swing_recovering": True,
                "close_swing_price_change_pct": 1.1,
                "close_swing_eligible": True,
            },
            {
                "stock_code": "000002",
                "rcp_no": "B",
                "sector": "조선/해양",
                "context_sector": "조선/해양",
                "close_swing_support_score": 6,
                "close_swing_ranking_score": 590.0,
                "close_swing_recovering": True,
                "close_swing_price_change_pct": 1.3,
                "close_swing_eligible": True,
            },
        ]
        annotated = annotate_event_candidates_with_hybrid(
            records,
            sector_rotation=sector,
            relative_value=relative,
            sector_thesis=thesis,
            inputs=inputs,
        )
        shadow = finalize_shadow_book(
            annotated,
            sector_rotation=sector,
            relative_value=relative,
            sector_thesis=thesis,
            live_selected_keys=set(),
            live_mode="event_only",
        )
        rows = {f"{row['stock_code']}:{row['rcp_no']}": row for row in shadow["rows"]}
        self.assertEqual(rows["000002:A"]["hybrid_shadow_decision"], "chosen")
        self.assertEqual(rows["000002:B"]["hybrid_shadow_decision"], "deferred")
        self.assertEqual(rows["000002:B"]["hybrid_blocked_reason_code"], "shadow_sector_cycle_limit")


if __name__ == "__main__":
    unittest.main()
