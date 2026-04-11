from typing import Dict, Optional
import pymongo

try:
    from config import SETTINGS as _SETTINGS
    if not hasattr(_SETTINGS, "COLLECTION"):
        raise ImportError
    SETTINGS = _SETTINGS
except Exception:
    try:
        from .config import SETTINGS
    except Exception:
        from config import SETTINGS

try:
    from utils import kst_today_str
except Exception:
    try:
        from .utils import kst_today_str
    except Exception:
        from utils import kst_today_str

class ConsensusMongoRepo:
    def __init__(self):
        self.client = pymongo.MongoClient(SETTINGS.MONGO_URI)
        self.col = self.client[SETTINGS.DB_NAME][SETTINGS.COLLECTION]

    def upsert_today(self, stock_code: str, consensus: Dict) -> bool:
        """
        네 기존 save_to_mongo의 핵심만 단순화:
        - date(as_of_date) 기준으로 1일 1회 upsert
        - 분기 또는 annual 입력이 전혀 없으면 스킵
        """
        today = kst_today_str()
        revenue = consensus.get("revenue")
        operating_profit = consensus.get("operating_profit")
        net_profit = consensus.get("net_profit")
        eps = consensus.get("eps")
        bps = consensus.get("bps")
        per = consensus.get("per")
        pbr = consensus.get("pbr")
        roe = consensus.get("roe")
        revenue_fy0 = consensus.get("revenue_fy0")
        operating_profit_fy0 = consensus.get("operating_profit_fy0")
        net_profit_fy0 = consensus.get("net_profit_fy0")
        eps_fy0 = consensus.get("eps_fy0")
        bps_fy0 = consensus.get("bps_fy0")
        per_fy0 = consensus.get("per_fy0")
        pbr_fy0 = consensus.get("pbr_fy0")
        roe_fy0 = consensus.get("roe_fy0")
        revenue_fy1 = consensus.get("revenue_fy1")
        operating_profit_fy1 = consensus.get("operating_profit_fy1")
        net_profit_fy1 = consensus.get("net_profit_fy1")
        eps_fy1 = consensus.get("eps_fy1")
        bps_fy1 = consensus.get("bps_fy1")
        per_fy1 = consensus.get("per_fy1")
        pbr_fy1 = consensus.get("pbr_fy1")
        roe_fy1 = consensus.get("roe_fy1")
        revenue_actual = consensus.get("revenue_actual")
        operating_profit_actual = consensus.get("operating_profit_actual")
        net_profit_actual = consensus.get("net_profit_actual")
        eps_actual = consensus.get("eps_actual")
        bps_actual = consensus.get("bps_actual")
        per_actual = consensus.get("per_actual")
        pbr_actual = consensus.get("pbr_actual")
        roe_actual = consensus.get("roe_actual")
        actual_year = consensus.get("actual_year")

        if all(
            value is None
            for value in (
                revenue,
                operating_profit,
                pbr,
                revenue_fy0,
                operating_profit_fy0,
                pbr_fy0,
                revenue_fy1,
                operating_profit_fy1,
                pbr_fy1,
                revenue_actual,
                operating_profit_actual,
                pbr_actual,
            )
        ):
            return False

        doc = {
            "stock_code": stock_code,
            "date": today,
            "as_of_date": today,
            "revenue": revenue,
            "operating_profit": operating_profit,
            "net_profit": net_profit,
            "eps": eps,
            "bps": bps,
            "per": per,
            "pbr": pbr,
            "roe": roe,
            "revenue_fy0": revenue_fy0,
            "operating_profit_fy0": operating_profit_fy0,
            "net_profit_fy0": net_profit_fy0,
            "eps_fy0": eps_fy0,
            "bps_fy0": bps_fy0,
            "per_fy0": per_fy0,
            "pbr_fy0": pbr_fy0,
            "roe_fy0": roe_fy0,
            "revenue_fy1": revenue_fy1,
            "operating_profit_fy1": operating_profit_fy1,
            "net_profit_fy1": net_profit_fy1,
            "eps_fy1": eps_fy1,
            "bps_fy1": bps_fy1,
            "per_fy1": per_fy1,
            "pbr_fy1": pbr_fy1,
            "roe_fy1": roe_fy1,
            "revenue_actual": revenue_actual,
            "operating_profit_actual": operating_profit_actual,
            "net_profit_actual": net_profit_actual,
            "eps_actual": eps_actual,
            "bps_actual": bps_actual,
            "per_actual": per_actual,
            "pbr_actual": pbr_actual,
            "roe_actual": roe_actual,
            "actual_year": actual_year,
        }

        self.col.update_one(
            {"stock_code": stock_code, "date": today},
            {"$set": doc},
            upsert=True
        )
        return True
