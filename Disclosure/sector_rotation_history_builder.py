from __future__ import annotations

import argparse
import json
import logging
import math
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from signals.wics_universe import normalize_sector_name
except Exception:
    from Disclosure.runtime_paths import RUNTIME_DIR, ensure_runtime_dir
    from Disclosure.signals.wics_universe import normalize_sector_name


log = logging.getLogger("disclosure.sector_rotation_history_builder")
ROOT_DIR = Path(__file__).resolve().parent
WEB_PROJECTION_DIR = Path(RUNTIME_DIR) / "web_projections"
SECTOR_DASHBOARD_PATH = WEB_PROJECTION_DIR / "sector_dashboard_latest.json"
BACKFILL_PATH = WEB_PROJECTION_DIR / "sector_rotation_backfill_latest.json"
OUTPUT_PATH = WEB_PROJECTION_DIR / "sector_rotation_history_latest.json"
WICS_LOG_DIR = ROOT_DIR / "signals" / "logs"
LIVE_SNAPSHOT_SOURCES = {"wics_log", "sector_dashboard"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build weekly sector rotation history for the public flows view.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--print-only", action="store_true", help="Print a short summary after saving.")
    parser.add_argument("--weeks", type=int, default=52, help="Maximum recent weeks to retain.")
    parser.add_argument("--times", default="06:58,11:58,15:43,20:23", help="Comma-separated HH:MM schedule list.")
    parser.add_argument("--poll-sec", type=int, default=20, help="Scheduler polling interval.")
    return parser.parse_args()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "None"):
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _parse_date(value: str) -> date | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def _parse_timestamp(value: Any) -> float:
    text = _clean_text(value)
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _week_label(week_start: date) -> str:
    return f"{week_start.month:02d}.{week_start.day:02d}"


def _log_date_from_path(path: Path) -> date | None:
    stem = path.stem
    suffix = stem.rsplit("_", 1)[-1]
    return _parse_date(suffix)


def _rank_pct_map(items: list[tuple[str, float | None]]) -> dict[str, float]:
    valid = [(name, float(value)) for name, value in items if value is not None]
    if not valid:
        return {}
    valid.sort(key=lambda item: item[1])
    total = len(valid)
    return {name: idx / total for idx, (name, _) in enumerate(valid, start=1)}


def _average_rate(stock_data: dict[str, Any]) -> float | None:
    values: list[float] = []
    for payload in stock_data.values():
        rate = _safe_float((payload or {}).get("rate"))
        if rate is not None:
            values.append(rate)
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _extract_log_leader(sector_payload: dict[str, Any]) -> tuple[str, str]:
    leaderboard = ((sector_payload.get("sector_features") or {}).get("leaderboard") or [])[:1]
    if leaderboard and isinstance(leaderboard[0], dict):
        leader = leaderboard[0]
        return (
            _clean_text(leader.get("stock_name") or leader.get("name")),
            _clean_text(leader.get("symbol") or leader.get("stock_symbol")),
        )
    return "", ""


def _extract_dashboard_leader(row: dict[str, Any]) -> tuple[str, str]:
    leader_name = _clean_text(row.get("leader_name"))
    leader_symbol = _clean_text(row.get("leader_symbol"))
    if leader_name and leader_symbol:
        return leader_name, leader_symbol

    flow_leaders = (row.get("sector_flow_leaders") or [])[:1]
    if flow_leaders and isinstance(flow_leaders[0], dict):
        leader = flow_leaders[0]
        return (
            leader_name or _clean_text(leader.get("name")),
            leader_symbol or _clean_text(leader.get("symbol")),
        )
    return leader_name, leader_symbol


def _dominant_actor(foreign_flow_eok: float, inst_flow_eok: float, retail_flow_eok: float) -> tuple[str, float]:
    actor, value = max(
        [
            ("foreign", float(foreign_flow_eok)),
            ("inst", float(inst_flow_eok)),
            ("retail", float(retail_flow_eok)),
        ],
        key=lambda item: item[1],
    )
    if value <= 0:
        return "none", 0.0
    return actor, round(value, 1)


def _source_family(source: Any) -> str:
    source_name = _clean_text(source)
    if source_name == "backfill":
        return "backfill"
    if source_name in LIVE_SNAPSHOT_SOURCES:
        return "live"
    return "other"


def _classify_week_source(source_counts: dict[str, Any] | None) -> str:
    counts = source_counts or {}
    live_count = int(counts.get("live") or 0)
    backfill_count = int(counts.get("backfill") or 0)
    if live_count > 0 and backfill_count > 0:
        return "mixed"
    if backfill_count > 0:
        return "backfill"
    return "live"


def _extract_log_sector_snapshot(sector_payload: dict[str, Any]) -> dict[str, Any] | None:
    sector_name = normalize_sector_name(sector_payload.get("sector_name"))
    if not sector_name:
        return None
    flow = sector_payload.get("sector_flow") or {}
    features = sector_payload.get("sector_features") or {}
    leader_name, leader_symbol = _extract_log_leader(sector_payload)
    foreign_flow_eok = _safe_float(flow.get("foreign"), 0.0) or 0.0
    inst_flow_eok = _safe_float(flow.get("inst"), 0.0) or 0.0
    retail_flow_eok = _safe_float(flow.get("retail"), 0.0) or 0.0
    combined_flow_eok = _safe_float(features.get("smart_money_net"))
    if combined_flow_eok is None:
        combined_flow_eok = foreign_flow_eok + inst_flow_eok
    return_pct = _average_rate(sector_payload.get("stock_data") or {})
    final_score = _safe_float(features.get("score"))
    if final_score is None:
        final_score = _safe_float(sector_payload.get("score"))
    return {
        "sector": sector_name,
        "return_pct": return_pct,
        "foreign_flow_eok": foreign_flow_eok,
        "inst_flow_eok": inst_flow_eok,
        "retail_flow_eok": retail_flow_eok,
        "combined_flow_eok": combined_flow_eok,
        "net_flow_eok": combined_flow_eok,
        "final_score": final_score,
        "leader_name": leader_name,
        "leader_symbol": leader_symbol,
        "source": "wics_log",
    }


def _load_log_snapshots() -> dict[str, dict[str, dict[str, Any]]]:
    snapshots: dict[str, dict[str, dict[str, Any]]] = {}
    for path in sorted(WICS_LOG_DIR.glob("wics_log_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        entries = payload if isinstance(payload, list) else []
        if not entries:
            continue
        latest_entry = max(
            entries,
            key=lambda entry: _parse_timestamp(entry.get("timestamp") or entry.get("captured_at")),
        )
        log_date = (
            _parse_date(_clean_text(latest_entry.get("timestamp"))[:10])
            or _parse_date(_clean_text(latest_entry.get("captured_at"))[:10])
            or _log_date_from_path(path)
        )
        if not log_date:
            continue
        sector_map: dict[str, dict[str, Any]] = {}
        for sector_payload in latest_entry.get("data") or []:
            row = _extract_log_sector_snapshot(sector_payload or {})
            if not row:
                continue
            sector_map[row["sector"]] = row
        if sector_map:
            snapshots[log_date.isoformat()] = sector_map
    return snapshots


def _load_sector_dashboard_snapshot() -> dict[str, dict[str, dict[str, Any]]]:
    if not SECTOR_DASHBOARD_PATH.exists():
        return {}
    try:
        payload = json.loads(SECTOR_DASHBOARD_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = payload if isinstance(payload, list) else []
    if not rows:
        return {}
    updated_at = _clean_text(rows[0].get("updated_at"))
    snapshot_date = _parse_date(updated_at[:10])
    if not snapshot_date:
        return {}
    sector_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        sector_name = normalize_sector_name(row.get("sector"))
        if not sector_name:
            continue
        leader_name, leader_symbol = _extract_dashboard_leader(row)
        sector_map[sector_name] = {
            "sector": sector_name,
            "return_pct": _safe_float(row.get("sector_change_pct")),
            "foreign_flow_eok": _safe_float(row.get("foreign_1d_eok"), 0.0) or 0.0,
            "inst_flow_eok": _safe_float(row.get("inst_1d_eok"), 0.0) or 0.0,
            "retail_flow_eok": _safe_float(row.get("retail_1d_eok"), 0.0) or 0.0,
            "combined_flow_eok": _safe_float(row.get("combined_1d_eok"), 0.0) or 0.0,
            "net_flow_eok": _safe_float(row.get("combined_1d_eok"), 0.0) or 0.0,
            "final_score": _safe_float(row.get("final_score"))
            if _safe_float(row.get("final_score")) is not None
            else _safe_float(row.get("flow_score")),
            "leader_name": leader_name,
            "leader_symbol": leader_symbol,
            "source": "sector_dashboard",
        }
    return {snapshot_date.isoformat(): sector_map} if sector_map else {}


def _load_backfill_snapshots() -> dict[str, dict[str, dict[str, Any]]]:
    if not BACKFILL_PATH.exists():
        return {}
    try:
        payload = json.loads(BACKFILL_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = payload.get("days") or {}
    if not isinstance(rows, dict):
        return {}
    snapshots: dict[str, dict[str, dict[str, Any]]] = {}
    for raw_day_key, sector_map in rows.items():
        day = _parse_date(raw_day_key)
        if not day or not isinstance(sector_map, dict):
            continue
        normalized_sector_map: dict[str, dict[str, Any]] = {}
        for raw_sector_name, row in sector_map.items():
            if not isinstance(row, dict):
                continue
            sector_name = normalize_sector_name(row.get("sector") or raw_sector_name)
            if not sector_name:
                continue
            foreign_flow_eok = _safe_float(row.get("foreign_flow_eok"), 0.0) or 0.0
            inst_flow_eok = _safe_float(row.get("inst_flow_eok"), 0.0) or 0.0
            retail_flow_eok = _safe_float(row.get("retail_flow_eok"), 0.0) or 0.0
            combined_flow_eok = _safe_float(row.get("combined_flow_eok"))
            if combined_flow_eok is None:
                combined_flow_eok = foreign_flow_eok + inst_flow_eok
            normalized_sector_map[sector_name] = {
                "sector": sector_name,
                "return_pct": _safe_float(row.get("return_pct")),
                "foreign_flow_eok": foreign_flow_eok,
                "inst_flow_eok": inst_flow_eok,
                "retail_flow_eok": retail_flow_eok,
                "combined_flow_eok": combined_flow_eok,
                "net_flow_eok": combined_flow_eok,
                "final_score": _safe_float(row.get("final_score")),
                "leader_name": _clean_text(row.get("leader_name")) or "-",
                "leader_symbol": _clean_text(row.get("leader_symbol")),
                "source": "backfill",
            }
        if normalized_sector_map:
            snapshots[day.isoformat()] = normalized_sector_map
    return snapshots


def _merge_daily_snapshots(*groups: dict[str, dict[str, dict[str, Any]]]) -> dict[str, dict[str, dict[str, Any]]]:
    merged: dict[str, dict[str, dict[str, Any]]] = {}
    for group in groups:
        for day_key, sector_map in group.items():
            bucket = merged.setdefault(day_key, {})
            for sector_name, payload in sector_map.items():
                bucket[sector_name] = payload
    return merged


def _annotate_daily_rotation(snapshots: dict[str, dict[str, dict[str, Any]]]) -> dict[str, dict[str, dict[str, Any]]]:
    for sector_map in snapshots.values():
        items = list(sector_map.items())
        if not items:
            continue
        flow_rank = _rank_pct_map([(sector, payload.get("combined_flow_eok")) for sector, payload in items])
        return_rank = _rank_pct_map([(sector, payload.get("return_pct")) for sector, payload in items])
        final_rank = _rank_pct_map([(sector, payload.get("final_score")) for sector, payload in items])
        top_cutoff = max(1, min(len(items), math.ceil(len(items) * 0.25)))
        ordered = []
        for sector_name, payload in items:
            rank_values = [flow_rank.get(sector_name, 0.0), return_rank.get(sector_name, 0.0)]
            if final_rank:
                rank_values.append(final_rank.get(sector_name, 0.0))
            rotation_score = round(sum(rank_values) / len(rank_values) * 100, 1) if rank_values else 0.0
            payload["rotation_score"] = rotation_score
            ordered.append(
                (
                    sector_name,
                    rotation_score,
                    _safe_float(payload.get("combined_flow_eok"), 0.0) or 0.0,
                    _safe_float(payload.get("return_pct"), 0.0) or 0.0,
                )
            )
        ordered.sort(key=lambda item: (item[1], item[2], item[3]), reverse=True)
        top_ranked = {sector for sector, *_ in ordered[:top_cutoff]}
        for sector_name, payload in items:
            return_pct = _safe_float(payload.get("return_pct"), 0.0) or 0.0
            net_flow_eok = _safe_float(payload.get("combined_flow_eok"), 0.0) or 0.0
            payload["daily_rotation_hit"] = bool(
                return_pct > 0 and net_flow_eok > 0 and sector_name in top_ranked
            )
    return snapshots


def build_rotation_history(*, weeks_limit: int) -> dict[str, Any]:
    backfill_snapshots = _load_backfill_snapshots()
    daily_snapshots = _annotate_daily_rotation(
        _merge_daily_snapshots(
            backfill_snapshots,
            _load_sector_dashboard_snapshot(),
            _load_log_snapshots(),
        )
    )
    weekly_stats: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    weekly_source_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"live": 0, "backfill": 0})

    for day_key in sorted(daily_snapshots):
        day = _parse_date(day_key)
        if not day:
            continue
        week_start = _week_start(day)
        week_key = week_start.isoformat()
        week_label = _week_label(week_start)
        for sector_name, payload in (daily_snapshots.get(day_key) or {}).items():
            bucket = weekly_stats[week_key].setdefault(
                sector_name,
                {
                    "sector": sector_name,
                    "week_key": week_key,
                    "week_label": week_label,
                    "days_observed": 0,
                    "return_sum": 0.0,
                    "foreign_flow_sum": 0.0,
                    "inst_flow_sum": 0.0,
                    "retail_flow_sum": 0.0,
                    "combined_flow_sum": 0.0,
                    "rotation_hit_count": 0,
                    "rotation_score_sum": 0.0,
                    "leader_observations": {},
                    "source_counts": {"live": 0, "backfill": 0},
                },
            )
            bucket["days_observed"] += 1
            bucket["return_sum"] += _safe_float(payload.get("return_pct"), 0.0) or 0.0
            bucket["foreign_flow_sum"] += _safe_float(payload.get("foreign_flow_eok"), 0.0) or 0.0
            bucket["inst_flow_sum"] += _safe_float(payload.get("inst_flow_eok"), 0.0) or 0.0
            bucket["retail_flow_sum"] += _safe_float(payload.get("retail_flow_eok"), 0.0) or 0.0
            bucket["combined_flow_sum"] += _safe_float(payload.get("combined_flow_eok"), 0.0) or 0.0
            bucket["rotation_score_sum"] += _safe_float(payload.get("rotation_score"), 0.0) or 0.0
            if payload.get("daily_rotation_hit"):
                bucket["rotation_hit_count"] += 1
            source_family = _source_family(payload.get("source"))
            if source_family in {"live", "backfill"}:
                bucket["source_counts"][source_family] = int(bucket["source_counts"].get(source_family) or 0) + 1
                weekly_source_counts[week_key][source_family] = int(weekly_source_counts[week_key].get(source_family) or 0) + 1
            leader_name = _clean_text(payload.get("leader_name"))
            leader_symbol = _clean_text(payload.get("leader_symbol"))
            if leader_name or leader_symbol:
                leader_key = f"{leader_name}::{leader_symbol}"
                leader_stats = bucket["leader_observations"].setdefault(
                    leader_key,
                    {
                        "leader_name": leader_name or leader_symbol or "-",
                        "leader_symbol": leader_symbol,
                        "count": 0,
                        "last_day": "",
                    },
                )
                leader_stats["count"] += 1
                leader_stats["last_day"] = max(_clean_text(leader_stats.get("last_day")), day_key)

    all_week_keys = sorted(weekly_stats)
    selected_week_keys = all_week_keys[-max(1, int(weeks_limit)) :] if all_week_keys else []
    sector_series: dict[str, list[dict[str, Any]]] = defaultdict(list)
    week_rows: list[dict[str, Any]] = []

    for week_key in selected_week_keys:
        sector_rows: list[dict[str, Any]] = []
        for bucket in weekly_stats.get(week_key, {}).values():
            days_observed = int(bucket["days_observed"])
            return_pct = round((bucket["return_sum"] / days_observed), 2) if days_observed else 0.0
            rotation_score = round((bucket["rotation_score_sum"] / days_observed), 1) if days_observed else 0.0
            foreign_flow_eok = round(float(bucket["foreign_flow_sum"]), 1)
            inst_flow_eok = round(float(bucket["inst_flow_sum"]), 1)
            retail_flow_eok = round(float(bucket["retail_flow_sum"]), 1)
            combined_flow_eok = round(float(bucket["combined_flow_sum"]), 1)
            rotation_hit_count = int(bucket["rotation_hit_count"])
            rotation_active = bool(return_pct > 0 and combined_flow_eok > 0 and rotation_hit_count >= 2)
            week_source = _classify_week_source(bucket.get("source_counts"))
            dominant_actor, dominant_actor_flow_eok = _dominant_actor(
                foreign_flow_eok,
                inst_flow_eok,
                retail_flow_eok,
            )
            leader_name = "-"
            leader_symbol = ""
            leader_observations = list((bucket.get("leader_observations") or {}).values())
            if leader_observations:
                leader = max(
                    leader_observations,
                    key=lambda item: (
                        int(item.get("count") or 0),
                        _clean_text(item.get("last_day")),
                        _clean_text(item.get("leader_name")),
                    ),
                )
                leader_name = _clean_text(leader.get("leader_name")) or "-"
                leader_symbol = _clean_text(leader.get("leader_symbol"))
            sector_rows.append(
                {
                    "sector": bucket["sector"],
                    "week_key": bucket["week_key"],
                    "week_label": bucket["week_label"],
                    "return_pct": return_pct,
                    "foreign_flow_eok": foreign_flow_eok,
                    "inst_flow_eok": inst_flow_eok,
                    "retail_flow_eok": retail_flow_eok,
                    "combined_flow_eok": combined_flow_eok,
                    "net_flow_eok": combined_flow_eok,
                    "rotation_active": rotation_active,
                    "rotation_score": rotation_score,
                    "rotation_hit_count": rotation_hit_count,
                    "days_observed": days_observed,
                    "leader_name": leader_name,
                    "leader_symbol": leader_symbol,
                    "dominant_actor": dominant_actor,
                    "dominant_actor_flow_eok": dominant_actor_flow_eok,
                    "week_source": week_source,
                }
            )

        sector_rows.sort(
            key=lambda item: (
                item["return_pct"],
                item["combined_flow_eok"],
                item["rotation_score"],
            ),
            reverse=True,
        )
        dominant = sector_rows[0] if sector_rows else None
        runner_up = sector_rows[1] if len(sector_rows) > 1 else None
        weekly_rankings = [
            {
                "rank": idx,
                "sector": row["sector"],
                "weekly_return_pct": row["return_pct"],
                "dominant_actor": row["dominant_actor"],
                "dominant_actor_flow_eok": row["dominant_actor_flow_eok"],
                "combined_flow_eok": row["combined_flow_eok"],
                "leader_name": row["leader_name"],
                "leader_symbol": row["leader_symbol"],
                "week_source": row["week_source"],
            }
            for idx, row in enumerate(sector_rows[:3], start=1)
        ]
        week_source = _classify_week_source(weekly_source_counts.get(week_key))
        for row in sector_rows:
            row["dominant_this_week"] = bool(dominant and row["sector"] == dominant["sector"])
            sector_series[row["sector"]].append(row)
        week_rows.append(
            {
                "week_key": week_key,
                "week_label": dominant["week_label"] if dominant else _week_label(_parse_date(week_key) or date.today()),
                "dominant_sector": dominant["sector"] if dominant else "",
                "runner_up_sector": runner_up["sector"] if runner_up else "",
                "rotation_hit_count": dominant["rotation_hit_count"] if dominant else 0,
                "weekly_return_pct": dominant["return_pct"] if dominant else 0.0,
                "avg_return_pct": dominant["return_pct"] if dominant else 0.0,
                "foreign_flow_eok": dominant["foreign_flow_eok"] if dominant else 0.0,
                "inst_flow_eok": dominant["inst_flow_eok"] if dominant else 0.0,
                "retail_flow_eok": dominant["retail_flow_eok"] if dominant else 0.0,
                "combined_flow_eok": dominant["combined_flow_eok"] if dominant else 0.0,
                "net_flow_eok": dominant["combined_flow_eok"] if dominant else 0.0,
                "leader_name": dominant["leader_name"] if dominant else "-",
                "leader_symbol": dominant["leader_symbol"] if dominant else "",
                "dominant_actor": dominant["dominant_actor"] if dominant else "none",
                "dominant_actor_flow_eok": dominant["dominant_actor_flow_eok"] if dominant else 0.0,
                "week_source": week_source,
                "weekly_rankings": weekly_rankings,
            }
        )

    available_weeks = len(week_rows)
    backfill_enabled = bool(backfill_snapshots)
    backfill_weeks = sum(1 for row in week_rows if _clean_text(row.get("week_source")) in {"backfill", "mixed"})
    history_note = ""
    if available_weeks <= 0:
        history_note = "히스토리 축적 중"
    elif available_weeks < weeks_limit:
        history_note = f"최근 {available_weeks}주 데이터만 확보되어 히스토리 축적 중"

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "weeks_requested": int(weeks_limit),
        "available_weeks": available_weeks,
        "backfill_enabled": backfill_enabled,
        "backfill_weeks": backfill_weeks,
        "history_ready": available_weeks > 0,
        "history_note": history_note,
        "weeks": week_rows,
        "sectors": dict(sector_series),
    }


def save_rotation_history(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_runtime_dir()
    WEB_PROJECTION_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"json": str(OUTPUT_PATH), "available_weeks": int(payload.get("available_weeks") or 0)}


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    payload = build_rotation_history(weeks_limit=max(1, int(args.weeks)))
    result = save_rotation_history(payload)
    if args.print_only:
        dominant = ((payload.get("weeks") or [])[-1] or {}).get("dominant_sector") or "-"
        print(f"[sector_rotation_history] 저장 {result['available_weeks']}주")
        print(f"- latest dominant: {dominant}")
        print(f"- json: {result['json']}")
    return result


def _parse_schedule_times(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def run_scheduler(args: argparse.Namespace) -> None:
    schedule_times = _parse_schedule_times(args.times)
    last_run_key = None
    log.info("sector rotation history scheduler started: %s", ", ".join(schedule_times))
    while True:
        now = datetime.now()
        hhmm = now.strftime("%H:%M")
        run_key = now.strftime("%Y%m%d %H:%M")
        if hhmm in schedule_times and run_key != last_run_key:
            run_once(args)
            last_run_key = run_key
        time.sleep(max(5, int(args.poll_sec)))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.once:
        run_once(args)
        return
    run_scheduler(args)


if __name__ == "__main__":
    main()
