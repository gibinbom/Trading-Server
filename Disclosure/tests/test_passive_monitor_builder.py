from __future__ import annotations

import unittest

import pandas as pd

from Disclosure.passive_monitor_builder import (
    _parse_public_float_profile_html,
    _build_special_change_event_map,
    _derive_kq150_bucket_targets,
    _latest_weekday_as_of,
    _previous_bucket_counts,
    build_public_faithful_index_rows,
    build_domestic_index_rows,
    build_msci_proxy_rows,
    build_stale_snapshot,
    is_passive_etf,
    _resolve_index_snapshot_as_of,
)


class PassiveMonitorBuilderTests(unittest.TestCase):
    def test_passive_etf_filter_excludes_active_and_leverage(self) -> None:
        self.assertTrue(is_passive_etf("KODEX 200"))
        self.assertTrue(is_passive_etf("TIGER 미국S&P500"))
        self.assertFalse(is_passive_etf("KoAct 배당성장액티브"))
        self.assertFalse(is_passive_etf("KODEX 200선물인버스2X"))

    def test_domestic_index_states_use_bucket_cut_and_buffer(self) -> None:
        frame = pd.DataFrame(
            [
                {"symbol": "000001", "name": "기존안정", "market": "KOSPI", "sector": "유통", "sector_bucket": "consumer", "marcap_krw": 95.0, "avg_amount_60d_krw": 95.0, "as_of": "2026-04-10"},
                {"symbol": "000002", "name": "기존편출", "market": "KOSPI", "sector": "은행", "sector_bucket": "financials", "marcap_krw": 10.0, "avg_amount_60d_krw": 10.0, "as_of": "2026-04-10"},
                {"symbol": "000003", "name": "신규편입", "market": "KOSPI", "sector": "반도체", "sector_bucket": "technology", "marcap_krw": 100.0, "avg_amount_60d_krw": 100.0, "as_of": "2026-04-10"},
                {"symbol": "000004", "name": "신규관찰", "market": "KOSPI", "sector": "소프트웨어", "sector_bucket": "technology", "marcap_krw": 80.0, "avg_amount_60d_krw": 80.0, "as_of": "2026-04-10"},
            ]
        )

        rows = build_domestic_index_rows(
            frame,
            index_name="KS200",
            cutoff=2,
            member_symbols={"000001", "000002"},
            member_source="previous_snapshot_proxy",
            bucket_targets={"technology": 1, "consumer": 1, "financials": 0},
            buffer=1,
        )
        by_symbol = {row["symbol"]: row for row in rows}

        self.assertEqual(by_symbol["000003"]["state"], "likely_add")
        self.assertEqual(by_symbol["000003"]["selection_path"], "bucket_quota")
        self.assertEqual(by_symbol["000004"]["state"], "watch_add")
        self.assertEqual(by_symbol["000002"]["state"], "likely_drop")
        self.assertEqual(by_symbol["000001"]["state"], "stable")
        self.assertEqual(by_symbol["000001"]["selection_path"], "bucket_quota")

    def test_bucket_quota_can_select_non_global_top_name(self) -> None:
        frame = pd.DataFrame(
            [
                {"symbol": "000001", "name": "기술1", "market": "KOSDAQ", "sector": "반도체", "sector_bucket": "technology", "marcap_krw": 100.0, "avg_amount_60d_krw": 100.0, "as_of": "2026-04-10"},
                {"symbol": "000002", "name": "기술2", "market": "KOSDAQ", "sector": "소프트웨어", "sector_bucket": "technology", "marcap_krw": 95.0, "avg_amount_60d_krw": 95.0, "as_of": "2026-04-10"},
                {"symbol": "000003", "name": "소비1", "market": "KOSDAQ", "sector": "화장품", "sector_bucket": "consumer", "marcap_krw": 80.0, "avg_amount_60d_krw": 80.0, "as_of": "2026-04-10"},
            ]
        )

        rows = build_domestic_index_rows(
            frame,
            index_name="KQ150",
            cutoff=2,
            member_symbols={"999999"},
            member_source="previous_snapshot_proxy",
            bucket_targets={"technology": 1, "consumer": 1},
            buffer=0,
        )
        by_symbol = {row["symbol"]: row for row in rows}

        self.assertEqual(by_symbol["000003"]["state"], "likely_add")
        self.assertEqual(by_symbol["000003"]["selection_path"], "bucket_quota")
        self.assertNotIn("selection_path", by_symbol["000002"])

    def test_buffer_keep_preserves_current_member_near_bucket_cut(self) -> None:
        frame = pd.DataFrame(
            [
                {"symbol": "000001", "name": "기술대표", "market": "KOSDAQ", "sector": "반도체", "sector_bucket": "technology", "marcap_krw": 100.0, "avg_amount_60d_krw": 100.0, "as_of": "2026-04-10"},
                {"symbol": "000002", "name": "현구성유지", "market": "KOSDAQ", "sector": "화장품", "sector_bucket": "consumer", "marcap_krw": 70.0, "avg_amount_60d_krw": 70.0, "as_of": "2026-04-10"},
                {"symbol": "000003", "name": "신규소비", "market": "KOSDAQ", "sector": "화장품", "sector_bucket": "consumer", "marcap_krw": 85.0, "avg_amount_60d_krw": 85.0, "as_of": "2026-04-10"},
            ]
        )

        rows = build_domestic_index_rows(
            frame,
            index_name="KQ150",
            cutoff=2,
            member_symbols={"000001", "000002"},
            member_source="previous_snapshot_proxy",
            bucket_targets={"technology": 1, "consumer": 1},
            buffer=1,
        )
        by_symbol = {row["symbol"]: row for row in rows}

        self.assertEqual(by_symbol["000002"]["state"], "stable")
        self.assertEqual(by_symbol["000002"]["selection_path"], "buffer_keep")

    def test_global_fill_uses_remaining_slots_when_bucket_is_short(self) -> None:
        frame = pd.DataFrame(
            [
                {"symbol": "000001", "name": "기술대표", "market": "KOSPI", "sector": "반도체", "sector_bucket": "technology", "marcap_krw": 100.0, "avg_amount_60d_krw": 100.0, "as_of": "2026-04-10"},
                {"symbol": "000002", "name": "소비대표", "market": "KOSPI", "sector": "유통", "sector_bucket": "consumer", "marcap_krw": 90.0, "avg_amount_60d_krw": 90.0, "as_of": "2026-04-10"},
                {"symbol": "000003", "name": "산업후보", "market": "KOSPI", "sector": "기계", "sector_bucket": "industrials", "marcap_krw": 80.0, "avg_amount_60d_krw": 80.0, "as_of": "2026-04-10"},
            ]
        )

        rows = build_domestic_index_rows(
            frame,
            index_name="KS200",
            cutoff=3,
            member_symbols={"999999"},
            member_source="previous_snapshot_proxy",
            bucket_targets={"technology": 1, "consumer": 1, "financials": 1},
            buffer=0,
        )
        by_symbol = {row["symbol"]: row for row in rows}

        self.assertEqual(by_symbol["000003"]["selection_path"], "global_fill")
        self.assertEqual(by_symbol["000003"]["state"], "likely_add")

    def test_unclassified_sector_falls_back_to_other_bucket(self) -> None:
        frame = pd.DataFrame(
            [
                {"symbol": "000001", "name": "미분류", "market": "KOSDAQ", "sector": "", "sector_bucket": "other", "marcap_krw": 50.0, "avg_amount_60d_krw": 50.0, "as_of": "2026-04-10"},
            ]
        )

        rows = build_domestic_index_rows(
            frame,
            index_name="KQ150",
            cutoff=1,
            member_symbols=set(),
            member_source="bootstrap_topcut_proxy",
            bucket_targets={"other": 1},
            buffer=0,
        )

        self.assertEqual(rows[0]["sector"], "미분류")
        self.assertEqual(rows[0]["sector_bucket"], "other")

    def test_previous_bucket_counts_rebuilds_bucket_from_new_sector_map(self) -> None:
        counts = _previous_bucket_counts(
            [
                {"symbol": "000001", "sector": "", "sector_bucket": "other"},
                {"symbol": "000002", "sector": "", "sector_bucket": "other"},
            ],
            sector_map={"000001": "제약", "000002": "반도체"},
        )

        self.assertEqual(counts["healthcare"], 1)
        self.assertEqual(counts["technology"], 1)
        self.assertNotIn("other", counts)

    def test_kq150_target_derivation_keeps_tech_core_majority(self) -> None:
        targets = _derive_kq150_bucket_targets(
            {
                "technology": 40,
                "healthcare": 30,
                "industrials": 15,
                "consumer": 10,
                "other": 5,
            },
            cutoff=150,
            available_buckets={"technology", "healthcare", "industrials", "consumer", "other"},
        )

        tech_core = targets["technology"] + targets["healthcare"]
        non_tech = targets["industrials"] + targets["consumer"] + targets["other"]
        self.assertEqual(sum(targets.values()), 150)
        self.assertGreater(tech_core, non_tech)

    def test_public_faithful_kq150_uses_fixed_public_bucket_targets(self) -> None:
        frame = pd.DataFrame(
            [
                {"symbol": "000001", "name": "BT1", "market": "KOSDAQ", "sector": "제약", "sector_bucket": "healthcare", "marcap_krw": 100.0, "avg_amount_60d_krw": 100.0, "size_proxy_krw": 100.0, "as_of": "2026-04-10"},
                {"symbol": "000002", "name": "IT1", "market": "KOSDAQ", "sector": "반도체", "sector_bucket": "technology", "marcap_krw": 95.0, "avg_amount_60d_krw": 95.0, "size_proxy_krw": 95.0, "as_of": "2026-04-10"},
                {"symbol": "000003", "name": "소재1", "market": "KOSDAQ", "sector": "화학", "sector_bucket": "materials_energy", "marcap_krw": 80.0, "avg_amount_60d_krw": 80.0, "size_proxy_krw": 80.0, "as_of": "2026-04-10"},
                {"symbol": "000004", "name": "소비1", "market": "KOSDAQ", "sector": "화장품", "sector_bucket": "consumer", "marcap_krw": 70.0, "avg_amount_60d_krw": 70.0, "size_proxy_krw": 70.0, "as_of": "2026-04-10"},
            ]
        )

        rows, meta = build_public_faithful_index_rows(frame, index_name="KQ150", cutoff=4, member_symbols={"000001", "000002"})

        self.assertGreaterEqual(meta["bucket_targets"]["healthcare"], 50)
        self.assertGreaterEqual(meta["bucket_targets"]["technology"], 40)
        self.assertEqual(sum(meta["bucket_targets"].values()), 150)
        self.assertTrue(any(row["state"] in {"likely_add", "stable"} for row in rows))

    def test_public_faithful_prefers_ffmc_proxy_over_raw_marcap(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "symbol": "000001",
                    "name": "저유동 대형주",
                    "market": "KOSPI",
                    "sector": "반도체",
                    "sector_bucket": "technology",
                    "marcap_krw": 150.0,
                    "avg_amount_60d_krw": 80.0,
                    "free_float_ratio": 0.25,
                    "ffmc_proxy_krw": 37.5,
                    "avg_ffmc_1y_krw": 36.0,
                    "size_proxy_krw": 36.0,
                    "as_of": "2026-04-10",
                },
                {
                    "symbol": "000002",
                    "name": "고유동 중형주",
                    "market": "KOSPI",
                    "sector": "반도체",
                    "sector_bucket": "technology",
                    "marcap_krw": 120.0,
                    "avg_amount_60d_krw": 90.0,
                    "free_float_ratio": 0.8,
                    "ffmc_proxy_krw": 96.0,
                    "avg_ffmc_1y_krw": 92.0,
                    "size_proxy_krw": 92.0,
                    "as_of": "2026-04-10",
                },
            ]
        )

        rows, _ = build_public_faithful_index_rows(frame, index_name="KS200", cutoff=1, member_symbols={"999998"})
        by_symbol = {row["symbol"]: row for row in rows}

        self.assertEqual(by_symbol["000002"]["predicted_rank"], 1)
        self.assertEqual(by_symbol["000002"]["state"], "likely_add")
        self.assertEqual(by_symbol["000001"]["predicted_rank"], 2)

    def test_parse_public_float_profile_html_extracts_float_and_holders(self) -> None:
        html = """
        <table>
          <tr><th>발행주식수<span>(보통주/ 우선주)</span></th><td>1,000,000 / 0</td></tr>
          <tr><th>유동주식수/비율<span>(보통주)</span></th><td>620,000 / 62.0</td></tr>
        </table>
        <table>
          <caption>주주현황</caption>
          <tr><th>주주구분</th><th>대표주주수</th><th>보통주</th><th>지분율</th><th>최종변동일</th></tr>
          <tr><td>최대주주등 (본인+특별관계자)</td><td>1</td><td>250,000</td><td>25.0</td><td>2026/04/01</td></tr>
          <tr><td>자기주식 (자사주+자사주신탁)</td><td>1</td><td>80,000</td><td>8.0</td><td>2026/04/01</td></tr>
          <tr><td>우리사주조합</td><td>1</td><td>50,000</td><td>5.0</td><td>2026/04/01</td></tr>
        </table>
        """

        profile = _parse_public_float_profile_html(html)

        self.assertEqual(profile["listed_common_shares"], 1000000)
        self.assertEqual(profile["float_shares"], 620000)
        self.assertAlmostEqual(profile["free_float_ratio"], 0.62)
        self.assertEqual(profile["major_holder_shares"], 250000)
        self.assertAlmostEqual(profile["treasury_ratio"], 8.0)

    def test_build_special_change_event_map_picks_recent_structural_events(self) -> None:
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from unittest.mock import patch

        frame = pd.DataFrame(
            [
                {"Code": "000001", "Name": "합병후보", "Market": "KOSPI"},
                {"Code": "000002", "Name": "분할후보", "Market": "KOSDAQ"},
            ]
        )
        fake_rows = [
            {"symbol": "000001", "name": "합병후보", "event_type": "MERGER", "event_date": "2026-04-01", "title": "회사합병결정"},
            {"symbol": "000002", "name": "분할후보", "event_type": "STOCK_SPLIT", "event_date": "2026-03-25", "title": "주식분할결정"},
            {"symbol": "000003", "name": "오래된건", "event_type": "MERGER", "event_date": "2025-01-01", "title": "오래된 이벤트"},
        ]

        with patch("Disclosure.passive_monitor_builder._load_projection_rows", return_value=fake_rows):
            special_map = _build_special_change_event_map(frame, lookback_days=120)

        self.assertEqual(special_map["000001"]["signal"], "합병 이벤트")
        self.assertEqual(special_map["000002"]["signal"], "주식분할 이벤트")
        self.assertNotIn("000003", special_map)

    def test_msci_proxy_uses_cutoff_and_borderline_buffer(self) -> None:
        frame = pd.DataFrame(
            [
                {"symbol": "000001", "name": "A", "market": "KOSPI", "marcap_krw": 100.0, "avg_amount_60d_krw": 100.0, "as_of": "2026-04-10"},
                {"symbol": "000002", "name": "B", "market": "KOSPI", "marcap_krw": 90.0, "avg_amount_60d_krw": 90.0, "as_of": "2026-04-10"},
                {"symbol": "000003", "name": "C", "market": "KOSDAQ", "marcap_krw": 80.0, "avg_amount_60d_krw": 80.0, "as_of": "2026-04-10"},
                {"symbol": "000004", "name": "D", "market": "KOSDAQ", "marcap_krw": 10.0, "avg_amount_60d_krw": 10.0, "as_of": "2026-04-10"},
            ]
        )

        rows = build_msci_proxy_rows(frame, cutoff=2, buffer=1)
        by_symbol = {row["symbol"]: row for row in rows}

        self.assertEqual(by_symbol["000001"]["state"], "likely_in")
        self.assertEqual(by_symbol["000002"]["state"], "likely_in")
        self.assertEqual(by_symbol["000003"]["state"], "borderline")
        self.assertEqual(by_symbol["000004"]["state"], "likely_out")

    def test_stale_fallback_keeps_previous_rows(self) -> None:
        previous = {
            "generated_at": "2026-04-10T20:10:00",
            "as_of": "2026-04-10",
            "status": "live",
            "rows": [{"symbol": "000001", "state": "stable"}],
        }
        fallback = build_stale_snapshot("index_rebalance_latest", previous, "2026-04-11T07:05:00", RuntimeError("boom"))

        self.assertEqual(fallback["status"], "stale")
        self.assertEqual(fallback["stale_since"], "2026-04-11T07:05:00")
        self.assertEqual(fallback["rows"][0]["symbol"], "000001")
        self.assertIn("boom", fallback["source_error"])

    def test_stale_fallback_applies_defaults_without_previous_snapshot(self) -> None:
        fallback = build_stale_snapshot(
            "etf_gap_monitor_latest",
            None,
            "2026-04-11T07:05:00",
            RuntimeError("source down"),
            defaults={
                "default_aum_top_n": 30,
                "default_gap_threshold_pct": 0.5,
                "summary": {"eligible_count": 0},
            },
        )

        self.assertEqual(fallback["status"], "stale")
        self.assertEqual(fallback["rows"], [])
        self.assertEqual(fallback["default_aum_top_n"], 30)
        self.assertEqual(fallback["default_gap_threshold_pct"], 0.5)
        self.assertEqual(fallback["summary"]["eligible_count"], 0)

    def test_index_snapshot_as_of_prefers_listing_snapshot_date(self) -> None:
        from pathlib import Path
        from tempfile import TemporaryDirectory
        import datetime as dt
        import os

        with TemporaryDirectory() as temp_dir:
            listing_path = Path(temp_dir) / "krx_listing.csv"
            listing_path.write_text("Code,Name\n005930,삼성전자\n", encoding="utf-8")
            target_ts = pd.Timestamp("2026-04-13T15:45:00").timestamp()
            os.utime(listing_path, (target_ts, target_ts))
            expected = dt.datetime.fromtimestamp(target_ts).date().isoformat()

            as_of = _resolve_index_snapshot_as_of(
                {
                    "005930": {"as_of": "2026-04-09"},
                    "000660": {"as_of": "2026-04-10"},
                },
                listing_path=listing_path,
            )

        self.assertEqual(as_of, expected)

    def test_index_snapshot_as_of_uses_latest_weekday_when_listing_is_missing(self) -> None:
        from pathlib import Path

        as_of = _resolve_index_snapshot_as_of(
            {
                "005930": {"as_of": "2026-04-09"},
                "000660": {"as_of": "2026-04-10"},
            },
            listing_path=Path("__missing_listing__.csv"),
        )

        self.assertEqual(as_of, _latest_weekday_as_of())

    def test_latest_weekday_as_of_rolls_weekend_back_to_friday(self) -> None:
        import datetime as dt

        self.assertEqual(_latest_weekday_as_of(dt.datetime(2026, 4, 12, 9, 0, 0)), "2026-04-10")


if __name__ == "__main__":
    unittest.main()
