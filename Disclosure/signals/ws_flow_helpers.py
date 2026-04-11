from __future__ import annotations

import os
from collections import Counter


def dated_jsonl_path(log_dir: str, prefix: str, captured_at: str) -> str:
    date_str = str(captured_at)[:10].replace("-", "")
    return os.path.join(log_dir, f"{prefix}_{date_str}.jsonl")


def top_counter(counter: Counter, limit: int = 3) -> list[dict]:
    return [
        {"name": name, "value": int(value)}
        for name, value in counter.most_common(limit)
        if name and value
    ]
