from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from Disclosure.official_index_clone_prepare import normalize_official_raw_inputs


class OfficialIndexClonePrepareTests(unittest.TestCase):
    def test_prepare_normalizes_alias_columns(self) -> None:
        with TemporaryDirectory() as temp_dir:
            raw_dir = Path(temp_dir) / "raw"
            out_dir = Path(temp_dir) / "out"
            raw_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "심사기준일": "2026-04-30",
                        "지수명": "KS200",
                        "적용일": "2026-06-12",
                        "구성종목수": "200",
                        "신규진입비율": "0.9",
                        "기존유지비율": "1.1",
                        "유동성커버리지": "0.85",
                        "특례시총순위": "50",
                    }
                ]
            ).to_csv(raw_dir / "review_metadata.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {
                        "심사기준일": "2026-04-30",
                        "지수명": "KS200",
                        "종목코드": "005930",
                        "종목명": "삼성전자",
                        "시장": "KOSPI",
                        "공식산업군": "전기전자",
                        "공식버킷": "manufacturing_electronics",
                        "1년평균유동시총": "1200000000000000",
                        "1년평균거래대금": "9000000000000",
                        "전체시총순위": "1",
                        "상장경과일수": "8000",
                        "유동주식비율": "0.75",
                        "심사대상여부": "1",
                        "현재구성종목여부": "1",
                    }
                ]
            ).to_csv(raw_dir / "universe_export.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {
                        "심사기준일": "2026-04-30",
                        "지수명": "KS200",
                        "공식버킷": "manufacturing_electronics",
                        "목표좌석수": "24",
                    }
                ]
            ).to_csv(raw_dir / "bucket_targets_export.csv", index=False, encoding="utf-8-sig")

            counts = normalize_official_raw_inputs(raw_dir, out_dir)
            reviews = pd.read_csv(out_dir / "reviews.csv", dtype=str, encoding="utf-8-sig")
            universe = pd.read_csv(out_dir / "universe.csv", dtype=str, encoding="utf-8-sig")

        self.assertEqual(counts["reviews"], 1)
        self.assertEqual(reviews.loc[0, "review_date"], "2026-04-30")
        self.assertEqual(universe.loc[0, "symbol"], "005930")
        self.assertEqual(universe.loc[0, "official_bucket"], "manufacturing_electronics")


if __name__ == "__main__":
    unittest.main()
