from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta


log = logging.getLogger("signals.slack_reporter")


def _existing_date_keys(
    base_dir: str,
    prefix: str,
    suffix: str,
    days_to_look_back: int,
    max_calendar_days: int | None = None,
) -> list[str]:
    target_count = max(1, int(days_to_look_back))
    scan_limit = max_calendar_days or max(target_count * 4, target_count + 3)
    found: list[str] = []
    for i in range(scan_limit):
        target_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        file_path = os.path.join(base_dir, f"{prefix}{target_date}{suffix}")
        if os.path.exists(file_path):
            found.append(target_date)
            if len(found) >= target_count:
                break
    return sorted(found)


def collect_structured_events(log_dir: str, days_to_look_back: int = 3) -> tuple[list[dict], list[str]]:
    events: list[dict] = []
    loaded_dates = _existing_date_keys(log_dir, "trading_events_", ".jsonl", days_to_look_back)
    for target_date in loaded_dates:
        file_path = os.path.join(log_dir, f"trading_events_{target_date}.jsonl")
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        log.warning("structured event decode failed (%s): %s", file_path, exc)
        except OSError as exc:
            log.warning("structured event read failed (%s): %s", file_path, exc)
    return events, loaded_dates


def collect_flow_snapshots(log_dir: str, days_to_look_back: int = 3) -> tuple[list[dict], list[str]]:
    snapshots: list[dict] = []
    loaded_dates = _existing_date_keys(log_dir, "trading_snapshots_", ".jsonl", days_to_look_back)
    for target_date in loaded_dates:
        file_path = os.path.join(log_dir, f"trading_snapshots_{target_date}.jsonl")
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        snapshots.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        log.warning("snapshot decode failed (%s): %s", file_path, exc)
        except OSError as exc:
            log.warning("snapshot read failed (%s): %s", file_path, exc)
    return snapshots, loaded_dates


def collect_legacy_logs(trading_root: str, days_to_look_back: int = 3) -> tuple[str, list[str]]:
    blocks: list[str] = []
    candidate_dirs = [trading_root]
    alt_dir = os.path.join(trading_root, "Disclosure", "signals", "logs")
    if alt_dir not in candidate_dirs:
        candidate_dirs.append(alt_dir)

    loaded_dates: list[str] = []
    file_paths: list[str] = []
    seen: set[tuple[str, str]] = set()
    for base_dir in candidate_dirs:
        dates = _existing_date_keys(base_dir, "trading_", ".txt", days_to_look_back)
        for target_date in dates:
            key = (base_dir, target_date)
            if key in seen:
                continue
            seen.add(key)
            loaded_dates.append(target_date)
            file_paths.append(os.path.join(base_dir, f"trading_{target_date}.txt"))

    for file_path in file_paths:
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                target_date = os.path.basename(file_path).replace("trading_", "").replace(".txt", "")
                blocks.append(f"--- [🗓️ {target_date}] 수급 로그 ---\n{handle.read()}\n")
        except OSError as exc:
            log.warning("legacy log read failed (%s): %s", file_path, exc)
    return "\n".join(blocks), sorted(set(loaded_dates))
