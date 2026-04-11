import datetime as dt
import pymongo
from typing import Dict, Optional

class MongoStateStore:
    """
    - seen_rcp: { "005930:2023123456": {...} }
    - corp_map: { "005930": "00123456" }
    """
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        self.client = pymongo.MongoClient(mongo_uri)
        self.col = self.client[db_name][collection_name]

    def load(self) -> Dict:
        doc = self.col.find_one({"_id": "state_data"}) or {}
        return {
            "seen_rcp": doc.get("seen_rcp", {}),
            "corp_map": doc.get("corp_map", {}),
        }

    def save(self, seen_rcp: Dict, corp_map: Dict) -> None:
        payload = {"seen_rcp": seen_rcp, "corp_map": corp_map}
        self.col.update_one({"_id": "state_data"}, {"$set": payload}, upsert=True)

    def mark_seen(self, seen_rcp: Dict, stock_code: str, rcp_no: str, ok: bool, title: str, src: str, err: Optional[str] = None):
        k = f"{stock_code}:{rcp_no}"
        seen_rcp[k] = {
            "ts": dt.datetime.now().isoformat(),
            "ok": bool(ok),
            "title": title,
            "src": src,
            **({"err": err[:200]} if err else {})
        }
