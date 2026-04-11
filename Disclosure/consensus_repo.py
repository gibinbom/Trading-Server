import pymongo
from typing import Optional, Dict, Any, Union


class MongoConsensusRepo:
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        self.client = pymongo.MongoClient(mongo_uri)
        self.col = self.client[db_name][collection_name]

    def _norm_code(self, stock_code: Union[str, int]) -> str:
        """
        종목코드를 항상 6자리 문자열로 정규화.
        - '9420' -> '009420'
        - 9420 -> '009420'
        - 'A005930' 같은 형태가 섞이면 뒤 6자리 사용
        """
        s = "" if stock_code is None else str(stock_code).strip()

        if s.startswith("A") and len(s) >= 7:
            s = s[-6:]

        digits = "".join(ch for ch in s if ch.isdigit())
        return digits.zfill(6) if digits else s.zfill(6)

    def _to_float(self, x: Any) -> Optional[float]:
        """
        안전한 float 변환:
        None, '-', 'N/A', '', 'nan' 등은 None 반환
        """
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)

        s = str(x).strip()
        if not s or s in ("-", "N/A", "n/a", "NA", "NaN", "nan", "null", "None"):
            return None

        s = s.replace(",", "")
        try:
            return float(s)
        except Exception:
            return None

    def get_quarter_consensus(self, stock_code: str) -> Dict[str, Optional[float]]:
        """
        DB에서 최신(date desc) 컨센서스 1개를 가져와서
        '억' 단위를 '원'으로 환산해 반환한다.
        레거시 quarter 값과 annual FY0/FY1 값을 함께 넘긴다.
        """
        code6 = self._norm_code(stock_code)

        # ✅ 1) stock_code가 문자열(6자리)로 저장된 경우 + int로 저장된 레거시까지 커버
        query = {"stock_code": code6}
        doc = self.col.find_one(query, sort=[("date", pymongo.DESCENDING)])

        if not doc:
            # 레거시: int로 저장(예: 009420 -> 9420)된 경우
            try:
                doc = self.col.find_one(
                    {"stock_code": int(code6)},
                    sort=[("date", pymongo.DESCENDING)]
                )
            except Exception:
                doc = None

        if not doc:
            return {"revenue": None, "op": None, "net": None}

        factor = 100_000_000.0  # 억 -> 원

        revenue_uk = self._to_float(doc.get("revenue"))

        # ✅ 2) 영업이익 필드명 혼재 커버: operating_profit / operating_load_profit(오타) / op(레거시)
        op_uk = self._to_float(
            doc.get("operating_profit")
            or doc.get("operating_load_profit")   # 오타 레거시
            or doc.get("op")                      # 레거시 축약
        )

        # ✅ 3) 순이익도 혼재 가능성 커버: net_profit / net(레거시)
        net_uk = self._to_float(
            doc.get("net_profit")
            or doc.get("net")
        )

        revenue_fy0_uk = self._to_float(doc.get("revenue_fy0"))
        op_fy0_uk = self._to_float(doc.get("operating_profit_fy0") or doc.get("op_fy0"))
        net_fy0_uk = self._to_float(doc.get("net_profit_fy0") or doc.get("net_fy0"))
        revenue_fy1_uk = self._to_float(doc.get("revenue_fy1"))
        op_fy1_uk = self._to_float(doc.get("operating_profit_fy1") or doc.get("op_fy1"))
        net_fy1_uk = self._to_float(doc.get("net_profit_fy1") or doc.get("net_fy1"))
        pbr_fy1 = self._to_float(doc.get("pbr_fy1"))
        roe_fy1 = self._to_float(doc.get("roe_fy1"))
        revenue_actual_uk = self._to_float(doc.get("revenue_actual"))
        op_actual_uk = self._to_float(doc.get("operating_profit_actual") or doc.get("op_actual"))
        net_actual_uk = self._to_float(doc.get("net_profit_actual") or doc.get("net_actual"))
        pbr_actual = self._to_float(doc.get("pbr_actual"))
        roe_actual = self._to_float(doc.get("roe_actual"))
        actual_year = doc.get("actual_year")

        revenue_won = None if revenue_uk is None else revenue_uk * factor
        op_won = None if op_uk is None else op_uk * factor
        net_won = None if net_uk is None else net_uk * factor
        revenue_fy0_won = None if revenue_fy0_uk is None else revenue_fy0_uk * factor
        op_fy0_won = None if op_fy0_uk is None else op_fy0_uk * factor
        net_fy0_won = None if net_fy0_uk is None else net_fy0_uk * factor
        revenue_fy1_won = None if revenue_fy1_uk is None else revenue_fy1_uk * factor
        op_fy1_won = None if op_fy1_uk is None else op_fy1_uk * factor
        net_fy1_won = None if net_fy1_uk is None else net_fy1_uk * factor
        revenue_actual_won = None if revenue_actual_uk is None else revenue_actual_uk * factor
        op_actual_won = None if op_actual_uk is None else op_actual_uk * factor
        net_actual_won = None if net_actual_uk is None else net_actual_uk * factor

        return {
            "revenue": revenue_won,
            "op": op_won,
            "net": net_won,
            "revenue_fy0": revenue_fy0_won,
            "op_fy0": op_fy0_won,
            "net_fy0": net_fy0_won,
            "revenue_fy1": revenue_fy1_won,
            "op_fy1": op_fy1_won,
            "net_fy1": net_fy1_won,
            "pbr_fy1": pbr_fy1,
            "roe_fy1": roe_fy1,
            "revenue_actual": revenue_actual_won,
            "op_actual": op_actual_won,
            "net_actual": net_actual_won,
            "pbr_actual": pbr_actual,
            "roe_actual": roe_actual,
            "actual_year": actual_year,
        }
