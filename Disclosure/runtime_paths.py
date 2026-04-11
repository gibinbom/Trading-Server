from __future__ import annotations

import os


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_DIR = os.path.dirname(ROOT_DIR)
RUNTIME_DIR = os.getenv("DISCLOSURE_RUNTIME_DIR", os.path.join(ROOT_DIR, "runtime"))
KIS_TOKEN_FILE = os.getenv("KIS_TOKEN_FILE", os.path.join(RUNTIME_DIR, "kis_token.json"))
LEGACY_KIS_TOKEN_FILES = [
    os.path.join(ROOT_DIR, "kis_token.json"),
    os.path.join(PROJECT_ROOT_DIR, "kis_token.json"),
]


def ensure_runtime_dir() -> str:
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    return RUNTIME_DIR
