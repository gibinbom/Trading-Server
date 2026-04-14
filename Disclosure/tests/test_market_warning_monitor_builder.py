from __future__ import annotations

import unittest

from Disclosure.market_warning_monitor_builder import (
    _build_name_lookup,
    _parse_kind_warning_html,
    build_official_state_map,
    build_stale_snapshot,
    evaluate_close_swing_candidate,
    evaluate_risk_designation,
    evaluate_risk_halt_candidates,
    evaluate_risk_pre_notice,
    evaluate_warning_designation,
    evaluate_warning_halt_candidate,
    evaluate_warning_pre_notice,
)


class MarketWarningMonitorBuilderTests(unittest.TestCase):
    def test_close_swing_candidate_triggered_when_price_and_auction_rules_match(self) -> None:
        row = evaluate_close_swing_candidate(
            as_of="2026-04-10",
            symbol="000001",
            name="테스트",
            market="KOSPI",
            current_official_state="none",
            close_today=1320.0,
            close_prev=1200.0,
            close_t3=1100.0,
            market_return_3d=2.4,
            auction_volume_share_pct=5.4,
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["signal_key"], "close_swing")
        self.assertEqual(row["candidate_state"], "triggered")

    def test_warning_pre_notice_and_designation_signals_respect_thresholds(self) -> None:
        pre_rows = evaluate_warning_pre_notice(
            as_of="2026-04-10",
            symbol="000001",
            name="테스트",
            market="KOSDAQ",
            current_official_state="none",
            return_3d_pct=102.0,
            return_5d_pct=58.0,
            return_15d_pct=80.0,
            caution_count_15d=5,
        )
        by_key = {row["signal_key"]: row for row in pre_rows}
        self.assertEqual(by_key["warning_pre_short"]["candidate_state"], "triggered")
        self.assertEqual(by_key["warning_pre_repeat_attention"]["candidate_state"], "triggered")

        design_rows = evaluate_warning_designation(
            as_of="2026-04-10",
            symbol="000001",
            name="테스트",
            market="KOSDAQ",
            current_official_state="warning_pre_notice",
            warning_pre_notice_date="2026-04-07",
            trading_dates=["2026-04-04", "2026-04-07", "2026-04-08", "2026-04-09", "2026-04-10"],
            return_3d_pct=98.0,
            return_5d_pct=61.0,
            return_15d_pct=78.0,
            caution_count_15d=5,
            is_recent_high=True,
        )
        by_key = {row["signal_key"]: row for row in design_rows}
        self.assertEqual(by_key["warning_design_short"]["candidate_state"], "near_trigger")
        self.assertEqual(by_key["warning_design_medium"]["candidate_state"], "triggered")

    def test_risk_pre_design_and_halt_sequence_uses_warning_and_risk_state(self) -> None:
        trading_dates = [
            "2026-03-31",
            "2026-04-01",
            "2026-04-02",
            "2026-04-03",
            "2026-04-06",
            "2026-04-07",
            "2026-04-08",
            "2026-04-09",
            "2026-04-10",
        ]
        pre_rows = evaluate_risk_pre_notice(
            as_of="2026-04-10",
            symbol="000001",
            name="테스트",
            market="KOSDAQ",
            current_official_state="warning_active",
            return_3d_pct=46.0,
            return_5d_pct=62.0,
            return_15d_pct=101.0,
            warning_design_date="2026-04-01",
            trading_dates=trading_dates,
            is_recent_high=True,
        )
        self.assertGreaterEqual(len(pre_rows), 2)

        design_rows = evaluate_risk_designation(
            as_of="2026-04-10",
            symbol="000001",
            name="테스트",
            market="KOSDAQ",
            current_official_state="risk_pre_notice",
            risk_pre_notice_date="2026-04-08",
            trading_dates=trading_dates,
            return_3d_pct=44.0,
            return_5d_pct=58.0,
            return_15d_pct=97.0,
            is_recent_high=True,
        )
        self.assertEqual({row["candidate_state"] for row in design_rows}, {"near_trigger"})

        halt_rows = evaluate_risk_halt_candidates(
            as_of="2026-04-10",
            symbol="000001",
            name="테스트",
            market="KOSDAQ",
            current_official_state="risk_active",
            closes=[100.0, 110.0, 121.0, 133.1],
            risk_design_preclose=95.0,
        )
        self.assertEqual({row["signal_key"] for row in halt_rows}, {"risk_halt_pre_notice", "risk_halt"})

    def test_warning_halt_candidate_uses_two_day_return_and_design_reference(self) -> None:
        row = evaluate_warning_halt_candidate(
            as_of="2026-04-10",
            symbol="000001",
            name="테스트",
            market="KOSPI",
            current_official_state="warning_active",
            current_close=150.0,
            prev_close=130.0,
            return_2d_pct=41.5,
            warning_design_preclose=120.0,
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["candidate_state"], "triggered")
        self.assertEqual(row["signal_key"], "warning_halt")

    def test_kind_warning_parser_and_state_machine_normalize_rows(self) -> None:
        stock_master = {
            "000001": {"symbol": "000001", "name": "테스트1", "market": "KOSDAQ"},
            "000002": {"symbol": "000002", "name": "테스트2", "market": "KOSDAQ"},
        }
        lookup = _build_name_lookup(stock_master)
        attention_html = """
        <section><table><tbody>
          <tr>
            <td>1</td>
            <td title="테스트1"><img class="legend" alt="코스닥"/> 테스트1</td>
            <td>투자경고 지정예고</td>
            <td>2026-04-08</td>
            <td>2026-04-09</td>
          </tr>
          <tr>
            <td>2</td>
            <td title="테스트2"><img class="legend" alt="코스닥"/> 테스트2</td>
            <td>투자위험 지정예고</td>
            <td>2026-04-09</td>
            <td>2026-04-10</td>
          </tr>
        </tbody></table></section>
        """
        risk_html = """
        <section><table><tbody>
          <tr>
            <td>1</td>
            <td title="테스트2"><img class="legend" alt="코스닥"/> 테스트2</td>
            <td>2026-04-09</td>
            <td>2026-04-10</td>
            <td>2026-04-17</td>
          </tr>
        </tbody></table></section>
        """

        rows = []
        rows.extend(_parse_kind_warning_html(attention_html, menu_kind="attention", name_lookup=lookup, as_of="2026-04-10"))
        rows.extend(_parse_kind_warning_html(risk_html, menu_kind="risk", name_lookup=lookup, as_of="2026-04-10"))
        state_map = build_official_state_map(rows, as_of="2026-04-10")

        self.assertEqual(state_map["000001"]["base_state"], "warning_pre_notice")
        self.assertEqual(state_map["000002"]["risk_pre_notice_date"], "2026-04-10")
        self.assertEqual(state_map["000002"]["risk_design_date"], "2026-04-10")
        self.assertEqual(state_map["000002"]["current_state"], "halt_active")

    def test_stale_fallback_keeps_defaults_and_rows(self) -> None:
        previous = {
            "generated_at": "2026-04-10T20:30:00",
            "as_of": "2026-04-10",
            "status": "live",
            "rows": [{"symbol": "000001", "kind": "warning"}],
        }
        fallback = build_stale_snapshot(
            "market_warning_official_latest",
            previous,
            "2026-04-11T07:10:00",
            RuntimeError("boom"),
            defaults={"summary": {"row_count": 0}},
        )

        self.assertEqual(fallback["status"], "stale")
        self.assertEqual(fallback["rows"][0]["symbol"], "000001")
        self.assertEqual(fallback["summary"]["row_count"], 0)
        self.assertIn("boom", fallback["source_error"])


if __name__ == "__main__":
    unittest.main()
