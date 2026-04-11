from __future__ import annotations

import logging
from typing import Any

try:
    import pymongo
except Exception:  # pragma: no cover - graceful fallback
    pymongo = None


log = logging.getLogger("disclosure.mongo_read_models")


class MongoReadModelStore:
    def __init__(self, mongo_uri: str, db_name: str):
        self.mongo_uri = str(mongo_uri or "").strip()
        self.db_name = str(db_name or "").strip()
        self.client = None
        self.db = None
        if pymongo is None:
            return
        if not self.mongo_uri or not self.db_name:
            return
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            log.warning("mongo read-model init failed: %s", exc)
            self.client = None
            self.db = None

    @property
    def available(self) -> bool:
        return self.db is not None

    def replace_collection(self, name: str, docs: list[dict[str, Any]], *, key_fields: list[str] | None = None) -> dict[str, Any]:
        if not self.available:
            return {"collection": name, "available": False, "count": len(docs)}
        key_fields = key_fields or ["_id"]
        col = self.db[name]
        replaced = 0
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            query: dict[str, Any] = {}
            for key in key_fields:
                value = doc.get(key)
                if value in (None, ""):
                    query = {}
                    break
                query[key] = value
            if not query:
                continue
            col.replace_one(query, doc, upsert=True)
            replaced += 1
        return {"collection": name, "available": True, "count": replaced}

    def replace_singleton(self, name: str, doc: dict[str, Any], *, singleton_id: str = "latest") -> dict[str, Any]:
        if not self.available:
            return {"collection": name, "available": False, "count": 1}
        payload = dict(doc or {})
        payload["_id"] = singleton_id
        self.db[name].replace_one({"_id": singleton_id}, payload, upsert=True)
        return {"collection": name, "available": True, "count": 1}
