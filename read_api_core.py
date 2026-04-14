from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from bson import ObjectId
from fastapi.encoders import jsonable_encoder
from pymongo import MongoClient

ROOT_DIR = Path(__file__).resolve().parent
PROJECTION_DIR = ROOT_DIR / "Disclosure" / "runtime" / "web_projections"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "stock_data")
READ_MODEL_SOURCE = str(os.getenv("READ_MODEL_SOURCE", "auto")).strip().lower()

_mongo_client: MongoClient | None = None


def sanitize(payload: Any) -> Any:
    return jsonable_encoder(
        payload,
        custom_encoder={
            ObjectId: str,
            Path: str,
            datetime: lambda value: value.isoformat(),
            date: lambda value: value.isoformat(),
        },
    )


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def get_mongo_client() -> MongoClient | None:
    global _mongo_client
    if READ_MODEL_SOURCE == "file":
        return None
    if _mongo_client is not None:
        return _mongo_client
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2500, connectTimeoutMS=2500)
        client.admin.command("ping")
        _mongo_client = client
        return client
    except Exception:
        return None


def read_projection_file(name: str, fallback: Any) -> Any:
    target = PROJECTION_DIR / f"{name}.json"
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def read_projection_rows_file(name: str) -> list[dict[str, Any]]:
    payload = read_projection_file(name, [])
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]
    return []


def get_mongo_collection(name: str) -> list[dict[str, Any]] | None:
    client = get_mongo_client()
    if client is None:
        return None
    try:
        return sanitize(list(client[DB_NAME][name].find({})))
    except Exception:
        return None


def get_mongo_singleton(name: str) -> dict[str, Any] | None:
    client = get_mongo_client()
    if client is None:
        return None
    try:
        doc = client[DB_NAME][name].find_one({"_id": "latest"})
        return sanitize(doc) if doc else None
    except Exception:
        return None


def has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


def has_required_keys(doc: dict[str, Any] | None, required_keys: list[str]) -> bool:
    if not isinstance(doc, dict):
        return False
    if not required_keys:
        return has_meaningful_value(doc)
    return any(has_meaningful_value(doc.get(key)) for key in required_keys)
