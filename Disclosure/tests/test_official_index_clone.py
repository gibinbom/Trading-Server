from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from Disclosure.official_index_clone import OfficialCloneInputError, build_official_index_rebalance_snapshot, load_official_clone_bundle


class OfficialIndexCloneTests(unittest.TestCase):
    def test_load_bundle_requires_all_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "reviews.csv").write_text("review_date,index_name,effective_date,cutoff,entry_ratio,keep_ratio,liquidity_coverage,special_largecap_rank\n", encoding="utf-8")
            with self.assertRaises(OfficialCloneInputError):
                load_official_clone_bundle(base)

    def test_build_snapshot_from_official_inputs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "reviews.csv").write_text(
                "\n".join(
                    [
                        "review_date,index_name,effective_date,cutoff,entry_ratio,keep_ratio,liquidity_coverage,special_largecap_rank",
                        "2026-04-30,KS200,2026-06-12,2,0.9,1.1,0.5,50",
                    ]
                ),
                encoding="utf-8",
            )
            (base / "bucket_targets.csv").write_text(
                "\n".join(
                    [
                        "review_date,index_name,official_bucket,target_count",
                        "2026-04-30,KS200,financials,1",
                        "2026-04-30,KS200,manufacturing_electronics,1",
                    ]
                ),
                encoding="utf-8",
            )
            (base / "universe.csv").write_text(
                "\n".join(
                    [
                        "review_date,index_name,symbol,name,market,official_sector,official_bucket,avg_ffmc_1y_krw,avg_trading_value_1y_krw,market_cap_rank_all,listing_age_days,free_float_ratio,is_eligible,is_current_member",
                        "2026-04-30,KS200,005930,삼성전자,KOSPI,전기전자,manufacturing_electronics,1200000000000000,9000000000000,1,8000,0.75,1,1",
                        "2026-04-30,KS200,105560,KB금융,KOSPI,금융,financials,180000000000000,500000000000,8,8000,0.82,1,0",
                        "2026-04-30,KS200,055550,신한지주,KOSPI,금융,financials,175000000000000,200000000000,9,8000,0.79,1,1",
                        "2026-04-30,KS200,000001,저유동후보,KOSPI,금융,financials,170000000000000,1,120,8000,0.82,1,0",
                    ]
                ),
                encoding="utf-8",
            )

            bundle = load_official_clone_bundle(base)
            snapshot = build_official_index_rebalance_snapshot(bundle)

        self.assertEqual(snapshot["methodology_mode"], "official")
        self.assertEqual(snapshot["as_of"], "2026-04-30")
        self.assertEqual(snapshot["indexes"][0]["methodology_version"], "official_clone_v1")
        by_symbol = {row["symbol"]: row for row in snapshot["rows"]}
        self.assertEqual(by_symbol["105560"]["state"], "likely_add")
        self.assertTrue(by_symbol["105560"]["liquidity_gate_pass"])
        self.assertFalse(by_symbol["000001"]["liquidity_gate_pass"])


if __name__ == "__main__":
    unittest.main()
