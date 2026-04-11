from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from Disclosure.disclosure_event_pipeline import build_sector_summary, build_slack_digest, decorate_summary_frame
from Disclosure.stock_card_render import render_event_lines
import Disclosure.event_alpha_features as event_alpha_features


class DisclosureEventInterpretationTests(unittest.TestCase):
    def test_empty_5d_returns_are_marked_as_hold(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sample_size": 5,
                    "valid_ret_5d_count": 0,
                    "valid_ret_10d_count": 0,
                    "pending_count": 5,
                    "avg_ret_5d": float("nan"),
                    "win_rate_5d": float("nan"),
                    "avg_mdd_5d": -2.0,
                }
            ]
        )
        row = decorate_summary_frame(frame).iloc[0]
        self.assertEqual(row["interpretation_label"], "해석 보류")
        self.assertTrue(pd.isna(row["win_rate_5d"]))

    def test_small_valid_sample_stays_shallow(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sample_size": 5,
                    "valid_ret_5d_count": 2,
                    "valid_ret_10d_count": 1,
                    "pending_count": 3,
                    "avg_ret_5d": 6.2,
                    "win_rate_5d": 100.0,
                    "avg_mdd_5d": -1.0,
                }
            ]
        )
        row = decorate_summary_frame(frame).iloc[0]
        self.assertEqual(row["interpretation_label"], "표본 얕음")

    def test_shallow_sample_can_be_short_term_or_delayed(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sample_size": 6,
                    "valid_ret_5d_count": 4,
                    "valid_ret_10d_count": 4,
                    "pending_count": 2,
                    "avg_ret_1d": 2.4,
                    "avg_ret_3d": 0.8,
                    "avg_ret_5d": -0.5,
                    "win_rate_5d": 50.0,
                    "avg_mdd_5d": -2.0,
                },
                {
                    "event_type": "STOCK_CANCELLATION",
                    "backtest_strategy": "immediate_close",
                    "sample_size": 6,
                    "valid_ret_5d_count": 4,
                    "valid_ret_10d_count": 4,
                    "pending_count": 2,
                    "avg_ret_1d": -1.5,
                    "avg_ret_3d": 1.8,
                    "avg_ret_5d": 0.7,
                    "win_rate_5d": 55.0,
                    "avg_mdd_5d": -2.5,
                },
            ]
        )
        rows = decorate_summary_frame(frame).to_dict(orient="records")
        by_type = {row["event_type"]: row for row in rows}
        self.assertEqual(by_type["SUPPLY_CONTRACT"]["tactical_label"], "단기 반응형")
        self.assertEqual(by_type["STOCK_CANCELLATION"]["tactical_label"], "지연 반응형")

    def test_good_sample_is_referenceable(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "event_type": "BUYBACK",
                    "backtest_strategy": "immediate_close",
                    "sample_size": 10,
                    "valid_ret_5d_count": 10,
                    "valid_ret_10d_count": 10,
                    "pending_count": 0,
                    "avg_ret_5d": 4.8,
                    "win_rate_5d": 60.0,
                    "avg_mdd_5d": -3.0,
                }
            ]
        )
        row = decorate_summary_frame(frame).iloc[0]
        self.assertEqual(row["interpretation_label"], "참고 가능")

    def test_large_drawdown_is_flagged_as_volatility_caution(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "event_type": "BUYBACK",
                    "backtest_strategy": "immediate_close",
                    "sample_size": 10,
                    "valid_ret_5d_count": 10,
                    "valid_ret_10d_count": 10,
                    "pending_count": 0,
                    "avg_ret_5d": 4.8,
                    "win_rate_5d": 62.0,
                    "avg_mdd_5d": -12.0,
                }
            ]
        )
        row = decorate_summary_frame(frame).iloc[0]
        self.assertEqual(row["interpretation_label"], "변동성 주의")

    def test_event_render_lines_show_interpretation_and_confidence(self) -> None:
        lines = render_event_lines(
            [
                {
                    "symbol": "053350",
                    "name": "이니텍",
                    "sector": "소프트웨어",
                    "alignment_label": "중립",
                    "alignment_score": 0,
                    "event_last_type": "SUPPLY_CONTRACT",
                    "event_last_bias": "positive",
                    "event_interpretation_label": "표본 얕음",
                    "event_tactical_label": "단기 반응형",
                    "event_tactical_note": "초기 반응은 있었지만 오래 끌기보다 짧게만 보는 편이 낫습니다.",
                    "event_backtest_confidence": "중간",
                    "event_valid_sample_size": 4,
                    "event_sample_size": 6,
                    "event_expected_alpha_5d": 6.75,
                    "event_recent_count": 4,
                }
            ],
            top_n=3,
        )
        digest = "\n".join(lines)
        self.assertIn("해석 `표본 얕음`", digest)
        self.assertIn("신뢰도 `중간`", digest)
        self.assertIn("유효5일 `4/6`", digest)

    def test_event_alpha_map_neutralizes_low_confidence_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "summary.csv"
            pd.DataFrame(
                [
                    {
                        "event_type": "SUPPLY_CONTRACT",
                        "backtest_strategy": "immediate_close",
                        "avg_ret_5d": 7.5,
                        "win_rate_5d": 80.0,
                        "avg_mdd_5d": -2.0,
                        "sample_size": 8,
                        "valid_ret_5d_count": 2,
                        "price_coverage_pct": 25.0,
                        "interpretable_score": 0.9,
                        "interpretation_label": "표본 얕음",
                        "confidence_label": "낮음",
                        "interpretation_note": "표본이 아직 적습니다.",
                    }
                ]
            ).to_csv(csv_path, index=False)
            with patch.object(event_alpha_features, "SUMMARY_PATH", str(csv_path)):
                alpha_map = event_alpha_features._load_event_alpha_map()
        info = alpha_map["SUPPLY_CONTRACT"]
        self.assertLess(info["event_edge_score"], info["raw_event_edge_score"])
        self.assertEqual(info["interpretation_label"], "표본 얕음")

    def test_neutral_summary_rows_become_presence_check(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "event_type": "LARGE_HOLDER",
                    "backtest_strategy": "immediate_close",
                    "sample_size": 5,
                    "valid_ret_5d_count": 4,
                    "valid_ret_10d_count": 4,
                    "pending_count": 1,
                    "avg_ret_1d": -1.0,
                    "avg_ret_3d": 1.5,
                    "avg_ret_5d": 2.0,
                    "win_rate_5d": 75.0,
                    "avg_mdd_5d": -3.0,
                    "positive_count": 0,
                    "negative_count": 0,
                    "neutral_count": 5,
                }
            ]
        )
        row = decorate_summary_frame(frame).iloc[0]
        self.assertEqual(row["dominant_bias"], "neutral")
        self.assertEqual(row["tactical_label"], "존재 확인")

    def test_sector_summary_and_digest_show_sector_specific_rows(self) -> None:
        detail_df = pd.DataFrame(
            [
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sector": "조선",
                    "stock_code": "439260",
                    "signal_bias": "positive",
                    "ret_1d": 1.2,
                    "ret_3d": 2.5,
                    "ret_5d": -0.5,
                    "ret_10d": 5.0,
                    "max_drawdown_5d": -2.0,
                },
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sector": "조선",
                    "stock_code": "010620",
                    "signal_bias": "positive",
                    "ret_1d": 0.8,
                    "ret_3d": 1.9,
                    "ret_5d": -0.2,
                    "ret_10d": 4.0,
                    "max_drawdown_5d": -2.5,
                },
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sector": "조선",
                    "stock_code": "005880",
                    "signal_bias": "positive",
                    "ret_1d": 0.5,
                    "ret_3d": 1.0,
                    "ret_5d": -0.1,
                    "ret_10d": 3.0,
                    "max_drawdown_5d": -2.0,
                },
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sector": "소프트웨어",
                    "stock_code": "053350",
                    "signal_bias": "negative",
                    "ret_1d": -0.7,
                    "ret_3d": -0.4,
                    "ret_5d": -1.2,
                    "ret_10d": -0.5,
                    "max_drawdown_5d": -7.5,
                },
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sector": "소프트웨어",
                    "stock_code": "030250",
                    "signal_bias": "negative",
                    "ret_1d": -0.3,
                    "ret_3d": -0.6,
                    "ret_5d": -0.8,
                    "ret_10d": -0.2,
                    "max_drawdown_5d": -6.8,
                },
                {
                    "event_type": "SUPPLY_CONTRACT",
                    "backtest_strategy": "immediate_close",
                    "sector": "소프트웨어",
                    "stock_code": "058860",
                    "signal_bias": "negative",
                    "ret_1d": -0.5,
                    "ret_3d": -0.7,
                    "ret_5d": -1.0,
                    "ret_10d": -0.6,
                    "max_drawdown_5d": -6.2,
                },
            ]
        )
        sector_summary = build_sector_summary(detail_df)
        digest = build_slack_digest(
            decorate_summary_frame(
                pd.DataFrame(
                    [
                        {
                            "event_type": "SUPPLY_CONTRACT",
                            "backtest_strategy": "immediate_close",
                            "sample_size": 6,
                            "valid_ret_5d_count": 6,
                            "valid_ret_10d_count": 6,
                            "pending_count": 0,
                            "avg_ret_1d": 0.2,
                            "avg_ret_3d": 0.6,
                            "avg_ret_5d": 1.2,
                            "win_rate_5d": 50.0,
                            "avg_mdd_5d": -4.1,
                        }
                    ]
                )
            ),
            {"record_count": 6, "priced_count": 2, "pending_price_records": 0},
            sector_summary_df=sector_summary,
        )
        self.assertIn("이벤트와 섹터를 함께 보면", digest)
        self.assertIn("섹터까지 보면 더 보수적인 조합", digest)
        self.assertIn("SUPPLY_CONTRACT / 조선", digest)
        self.assertIn("SUPPLY_CONTRACT / 소프트웨어", digest)

    def test_event_alpha_frame_blends_sector_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.csv"
            sector_summary_path = Path(tmpdir) / "sector_summary.csv"
            pd.DataFrame(
                [
                    {
                        "event_type": "SUPPLY_CONTRACT",
                        "backtest_strategy": "immediate_close",
                        "avg_ret_1d": 0.2,
                        "avg_ret_3d": 0.5,
                        "avg_ret_5d": 1.0,
                        "win_rate_5d": 55.0,
                        "avg_mdd_5d": -3.0,
                        "sample_size": 10,
                        "valid_ret_5d_count": 10,
                        "price_coverage_pct": 100.0,
                        "interpretable_score": 0.4,
                        "interpretation_label": "참고 가능",
                        "confidence_label": "중간",
                        "interpretation_note": "기본 참고 재료입니다.",
                        "tactical_label": "참고 가능",
                        "tactical_note": "기본 참고 재료입니다.",
                        "reaction_profile": "초기 반응과 유지력",
                    }
                ]
            ).to_csv(summary_path, index=False)
            pd.DataFrame(
                [
                    {
                        "event_type": "SUPPLY_CONTRACT",
                        "backtest_strategy": "immediate_close",
                        "sector": "조선",
                        "avg_ret_1d": 1.4,
                        "avg_ret_3d": 2.3,
                        "avg_ret_5d": 4.8,
                        "win_rate_5d": 70.0,
                        "avg_mdd_5d": -2.2,
                        "sample_size": 6,
                        "valid_ret_5d_count": 6,
                        "price_coverage_pct": 100.0,
                        "interpretable_score": 0.9,
                        "interpretation_label": "참고 가능",
                        "confidence_label": "중간",
                        "interpretation_note": "조선 섹터에서는 반응이 더 낫습니다.",
                        "tactical_label": "참고 가능",
                        "tactical_note": "조선 섹터 기준으로는 더 강합니다.",
                        "reaction_profile": "초기 반응과 유지력",
                    }
                ]
            ).to_csv(sector_summary_path, index=False)
            records = [
                {
                    "stock_code": "439260",
                    "event_type": "SUPPLY_CONTRACT",
                    "signal_bias": "positive",
                    "event_date": "2026-03-30",
                    "event_time_hhmm": "1530",
                }
            ]
            with patch.object(event_alpha_features, "SUMMARY_PATH", str(summary_path)), patch.object(
                event_alpha_features, "SECTOR_SUMMARY_PATH", str(sector_summary_path)
            ), patch.object(event_alpha_features, "load_event_records", return_value=records), patch.object(
                event_alpha_features, "load_latest_symbol_sector_map", return_value={"439260": "조선"}
            ):
                frame = event_alpha_features.build_event_alpha_frame()
        row = frame.iloc[0]
        self.assertGreater(float(row["event_interpretable_score"]), 0.4)
        self.assertGreater(float(row["event_expected_alpha_5d"]), 1.0)
        self.assertIn("조선", str(row["event_tactical_note"]))


if __name__ == "__main__":
    unittest.main()
