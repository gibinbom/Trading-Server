from __future__ import annotations

import argparse
import json
import os
import tempfile
import glob
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import pandas as pd

from analyst_report_collector import AnalystReportCollector
from daily_signal_mart import build_mart_summary, save_daily_signal_mart
from analyst_report_pipeline import load_raw_reports, score_reports
from analyst_report_summary import build_stock_summary
from check_trade_runtime import load_trade_runtime_health
from disclosure_event_collector import collect_once as collect_events_once
from disclosure_event_pipeline import build_backtest_frames, load_event_records
from event_trade_watcher import EventLogTradeWatcher
from factor_pipeline import FactorSnapshotBuilder
from flow_intraday_backtest import build_intraday_backtest
from kis_broker_factory import build_kis_broker_from_settings
from signals.macro_news_monitor import fetch_macro_news, load_latest_macro_report
from signals.wics_monitor import get_individual_stock_data
from stock_card_digest import build_stock_card_summary
from stock_card_pipeline import build_stock_card_frame
from stock_news_collector import StockNewsCollector
from stock_news_pipeline import build_stock_news_summary, load_raw_stock_news, score_stock_news
from stock_card_sources import SIGNAL_LOG_DIR


CARD_SUMMARY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cards", "stock_cards_latest.json")
CARD_CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cards", "stock_cards_latest.csv")
EVENT_SUMMARY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "events", "reports", "disclosure_event_summary_latest.json")


def _latest_weekday(base: datetime | None = None, min_days_ago: int = 1) -> datetime:
    cursor = (base or datetime.now()) - timedelta(days=min_days_ago)
    while cursor.weekday() >= 5:
        cursor -= timedelta(days=1)
    return cursor


@contextmanager
def _temp_chdir():
    prev = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            yield tmpdir
        finally:
            os.chdir(prev)


def _run_analyst_smoke() -> dict:
    result = AnalystReportCollector(naver_pages=1, hankyung_pages=1, hankyung_days=7, sleep_sec=0.0).collect_once()
    raw_df = load_raw_reports(days=30)
    if not raw_df.empty:
        raw_df = raw_df.sort_values("published_at").head(12).copy()
    scored = score_reports(raw_df, attach_labels=True, use_pdf_text=True)
    summary = build_stock_summary(scored, top_n=3)
    return {
        "collected": result,
        "scored_rows": int(len(scored)),
        "top_stocks": len(summary.get("top_stocks", [])),
        "alpha_labeled": int(scored["realized_alpha_score"].notna().sum()) if not scored.empty and "realized_alpha_score" in scored.columns else 0,
        "pdf_enriched": int(scored["pdf_text_length"].fillna(0).gt(0).sum()) if not scored.empty and "pdf_text_length" in scored.columns else 0,
        "parse_ok": int(scored["report_parse_quality_status"].isin(["ok", "rich"]).sum()) if not scored.empty and "report_parse_quality_status" in scored.columns else 0,
    }


def _run_stock_news_smoke() -> dict:
    result = StockNewsCollector(top_n=10, markets=["KOSPI", "KOSDAQ"], sources=["naver", "google"], google_top_n=10).collect_once()
    scored = score_stock_news(load_raw_stock_news(days=7))
    summary = build_stock_news_summary(scored, top_n=3)
    return {"collected": result, "scored_rows": int(len(scored)), "positive": len(summary.get("top_positive", []))}


def _run_factor_smoke() -> dict:
    builder = FactorSnapshotBuilder(top_n=3, markets=["KOSPI", "KOSDAQ"], include_flow=False, include_consensus=False, include_news=False, price_sleep_sec=0.0)
    df = builder.build_snapshot()
    return {
        "rows": int(len(df)),
        "eligible": int(df["ranking_eligible"].fillna(False).sum()),
        "dynamic_weight_status": str((builder.dynamic_weight_info or {}).get("status") or "missing"),
    }


def _run_card_smoke() -> dict:
    df = build_stock_card_frame(analyst_days=30, flow_days=3)
    summary = build_stock_card_summary(df, top_n=5)
    counts = summary.get("counts", {})
    mart_meta = build_mart_summary(df)
    ml_model_type = ""
    ml_train_rows = 0
    if not df.empty:
        ml_model_type = str(df.get("ml_model_type", "").iloc[0] if "ml_model_type" in df.columns else "")
        ml_train_rows = int(pd.to_numeric(df.get("ml_train_rows", 0), errors="coerce").fillna(0).max()) if "ml_train_rows" in df.columns else 0
    return {
        "rows": int(len(df)),
        "factor": int(counts.get("factor", 0)),
        "analyst": int(counts.get("analyst", 0)),
        "flow": int(counts.get("flow", 0)),
        "intraday": int(counts.get("intraday", 0)),
        "event": int(counts.get("event", 0)),
        "micro": int(counts.get("micro", 0)),
        "ml": int(counts.get("ml", 0)),
        "mart_rows": int(mart_meta.get("row_count", 0)),
        "ml_model_type": ml_model_type,
        "ml_train_rows": ml_train_rows,
    }


def _run_macro_smoke() -> dict:
    text = fetch_macro_news() or ""
    live_count = len([line for line in text.splitlines() if line.strip()])
    if live_count > 0:
        return {"headlines": live_count, "source": "live"}

    latest = load_latest_macro_report()
    cached_headlines = latest.get("headlines") or []
    return {"headlines": int(len(cached_headlines)), "source": "cached" if cached_headlines else "missing"}


def _run_wics_smoke() -> dict:
    broker = build_kis_broker_from_settings(is_virtual=False, dry_run=True)
    payload = get_individual_stock_data(broker, "005930")
    return {"dominant_actor": payload.get("dominant_actor"), "smart_money": payload.get("smart_money")}


def _run_event_smoke() -> dict:
    from argparse import Namespace

    collected = collect_events_once(
        Namespace(
            once=True,
            poll_sec=0,
            off_hours_poll_sec=0,
            max_pages=1,
            backfill_days=0,
            start_date="",
            end_date="",
            markets="KOSPI,KOSDAQ",
            ignore_seen=True,
        )
    )
    records = load_event_records(days=45)
    detail, summary, meta = build_backtest_frames(records)
    synthetic = [
        {
            "stock_code": "005930",
            "corp_name": "삼성전자",
            "rcp_no": "REPLAY001",
            "title": "단일판매ㆍ공급계약체결",
            "event_type": "SUPPLY_CONTRACT",
            "signal_bias": "positive",
            "strategy_name": "replay_test",
            "event_date": (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d"),
            "event_time_hhmm": "10:00",
            "metrics": {},
        }
    ]
    replay_detail, replay_summary, replay_meta = build_backtest_frames(synthetic)
    return {
        "collected": collected,
        "log_records": meta["record_count"],
        "log_priced": meta["priced_count"],
        "log_pending": int(meta.get("pending_price_records", 0) or 0),
        "replay_rows": int(len(replay_detail)),
        "replay_summary_rows": int(len(replay_summary)),
        "replay_priced": replay_meta["priced_count"],
        "replay_pending": int(replay_meta.get("pending_price_records", 0) or 0),
    }


def _run_event_watcher_smoke() -> dict:
    import event_trade_watcher as etw

    class StubEngine:
        def __init__(self):
            self.broker_live = None
            self.calls = []

        def _process_one(self, **kwargs):
            self.calls.append(kwargs)
            return True

    with tempfile.TemporaryDirectory() as tmpdir:
        prev_state = etw.EVENT_STATE_PATH
        etw.EVENT_STATE_PATH = os.path.join(tmpdir, "main_trade_state.json")
        try:
            engine = StubEngine()
            watcher = EventLogTradeWatcher(engine)
            ok = watcher._process_record(
                {
                    "stock_code": "005930",
                    "corp_name": "삼성전자",
                    "rcp_no": "WATCH001",
                    "title": "단일판매ㆍ공급계약체결",
                    "src": "EVENT_COLLECTOR_HTML",
                    "event_type": "SUPPLY_CONTRACT",
                    "signal_bias": "positive",
                    "event_date": datetime.now().strftime("%Y-%m-%d"),
                    "event_time_hhmm": "15:20",
                }
            )
        finally:
            etw.EVENT_STATE_PATH = prev_state
    return {"processed": bool(ok), "engine_calls": len(engine.calls)}


def _run_ws_replay_smoke() -> dict:
    import signals.ws_signal_analyzer as analyzer_mod

    symbol = "005930"
    tick = [""] * 66
    tick[0] = symbol
    tick[1] = "키움증권"
    tick[6] = "모건스탠리"
    tick[51] = "0"
    tick[56] = "4000"
    tick[65] = "6000"

    with _temp_chdir() as tmpdir:
        prev_log_dir = analyzer_mod.STRUCTURED_LOG_DIR
        prev_clock = analyzer_mod._kst_now
        current = {"dt": datetime.now().replace(hour=9, minute=5, second=0, microsecond=0)}
        analyzer_mod.STRUCTURED_LOG_DIR = tmpdir
        analyzer_mod._kst_now = lambda: current["dt"]
        try:
            analyzer = analyzer_mod.SignalAnalyzer({symbol: "삼성전자"}, broker=None)
            analyzer.price_cache[symbol] = 200000
            analyzer.analyze_tick(tick[:])
            current["dt"] = current["dt"] + timedelta(minutes=6)
            tick[65] = "12000"
            analyzer.analyze_tick(tick[:])
            event_files = [name for name in os.listdir(tmpdir) if name.startswith("trading_events_")]
            flow_files = [name for name in os.listdir(tmpdir) if name.startswith("trading_flow_ticks_")]
            snapshot_files = [name for name in os.listdir(tmpdir) if name.startswith("trading_snapshots_")]
            legacy_files = [name for name in os.listdir(tmpdir) if name.startswith("trading_") and name.endswith(".txt")]
        finally:
            analyzer_mod.STRUCTURED_LOG_DIR = prev_log_dir
            analyzer_mod._kst_now = prev_clock
    return {"event_logs": len(event_files), "flow_logs": len(flow_files), "snapshot_logs": len(snapshot_files), "legacy_logs": len(legacy_files)}


def _run_flow_intraday_smoke() -> dict:
    with tempfile.TemporaryDirectory() as tmpdir:
        target_date = _latest_weekday(min_days_ago=2).strftime("%Y%m%d")
        path = os.path.join(tmpdir, f"trading_events_{target_date}.jsonl")
        payload = {
            "captured_at": f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}T10:00:00",
            "symbol": "005930",
            "stock_name": "삼성전자",
            "event_type": "D_ATTACK",
            "buy_broker": "모건스탠리",
            "sell_broker": "키움증권",
            "net_amt_mil": 120,
        }
        with open(path, "w", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
        detail, summary = build_intraday_backtest(days=3, log_dir=tmpdir)
    return {"rows": int(len(detail)), "summary_rows": int(len(summary))}


def _run_mart_smoke() -> dict:
    df = build_stock_card_frame(analyst_days=30, flow_days=3).head(10).copy()
    paths = save_daily_signal_mart(df, build_mart_summary(df))
    return {
        "rows": int(len(df)),
        "csv_saved": int(bool(paths.get("mart_csv") and os.path.exists(paths["mart_csv"]))),
        "json_saved": int(bool(paths.get("mart_json") and os.path.exists(paths["mart_json"]))),
    }


def _count_lines(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as fp:
            return sum(1 for _ in fp)
    except Exception:
        return 0


def _load_json_file(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _recent_trading_date_keys(days: int = 3) -> set[str]:
    target = max(1, int(days))
    cursor = datetime.now()
    keys: set[str] = set()
    while len(keys) < target:
        if cursor.weekday() < 5:
            keys.add(cursor.strftime("%Y%m%d"))
        cursor -= timedelta(days=1)
    return keys


def _run_flow_log_health(days: int = 3, log_dir: str = SIGNAL_LOG_DIR) -> dict:
    current = datetime.now()
    rows = {
        "event_files": 0,
        "flow_tick_files": 0,
        "snapshot_files": 0,
        "event_lines": 0,
        "flow_tick_lines": 0,
        "snapshot_lines": 0,
        "latest_snapshot_age_min": None,
        "health_total_updates": 0,
        "health_flow_tick_logged": 0,
        "health_snapshot_logged": 0,
        "health_last_event_at": "",
        "health_skip_below_tick_threshold": 0,
        "health_skip_snapshot_interval_gate": 0,
        "health_skip_snapshot_min_gross": 0,
        "flow_tick_log_min_amt_mil": 0,
        "flow_tick_log_min_foreign_qty": 0,
        "snapshot_interval_sec": 0,
        "snapshot_min_gross_amt_mil": 0,
        "snapshot_force_gross_amt_mil": 0,
    }
    if not os.path.isdir(log_dir):
        return rows
    date_keys = _recent_trading_date_keys(days=max(1, int(days)))
    patterns = {
        "event": "trading_events_*.jsonl",
        "flow_tick": "trading_flow_ticks_*.jsonl",
        "snapshot": "trading_snapshots_*.jsonl",
    }
    latest_snapshot_mtime = None
    for prefix, pattern in patterns.items():
        matched = [
            path for path in glob.glob(os.path.join(log_dir, pattern))
            if any(path.endswith(f"_{date_key}.jsonl") for date_key in date_keys)
        ]
        rows[f"{prefix}_files"] = len(matched)
        rows[f"{prefix}_lines"] = sum(_count_lines(path) for path in matched)
        if prefix == "snapshot" and matched:
            latest_snapshot_mtime = max(os.path.getmtime(path) for path in matched)
    if latest_snapshot_mtime is not None:
        rows["latest_snapshot_age_min"] = int(max(0.0, time.time() - latest_snapshot_mtime) // 60)
    health_path = os.path.join(log_dir, "trading_flow_health_latest.json")
    if os.path.exists(health_path):
        try:
            with open(health_path, "r", encoding="utf-8") as fp:
                payload = json.load(fp)
            rows["health_total_updates"] = int(payload.get("total_updates", 0) or 0)
            rows["health_flow_tick_logged"] = int(payload.get("flow_tick_logged", 0) or 0)
            rows["health_snapshot_logged"] = int(payload.get("snapshot_logged", 0) or 0)
            rows["health_last_event_at"] = str(payload.get("last_event_at") or "")
            rows["health_skip_below_tick_threshold"] = int(payload.get("skip_below_tick_threshold", 0) or 0)
            rows["health_skip_snapshot_interval_gate"] = int(payload.get("skip_snapshot_interval_gate", 0) or 0)
            rows["health_skip_snapshot_min_gross"] = int(payload.get("skip_snapshot_min_gross", 0) or 0)
            rows["flow_tick_log_min_amt_mil"] = int(payload.get("flow_tick_log_min_amt_mil", 0) or 0)
            rows["flow_tick_log_min_foreign_qty"] = int(payload.get("flow_tick_log_min_foreign_qty", 0) or 0)
            rows["snapshot_interval_sec"] = int(payload.get("snapshot_interval_sec", 0) or 0)
            rows["snapshot_min_gross_amt_mil"] = int(payload.get("snapshot_min_gross_amt_mil", 0) or 0)
            rows["snapshot_force_gross_amt_mil"] = int(payload.get("snapshot_force_gross_amt_mil", 0) or 0)
        except Exception:
            pass
    return rows


def run_lite_checks() -> dict:
    mart_summary = build_mart_summary(pd.DataFrame())
    latest_mart_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "marts", "daily_signal_mart_latest.json")
    if os.path.exists(latest_mart_path):
        try:
            with open(latest_mart_path, "r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if isinstance(payload, dict):
                mart_summary.update(payload)
        except Exception:
            pass

    card_summary = _load_json_file(CARD_SUMMARY_PATH)
    card_counts = (card_summary.get("counts") or {}) if isinstance(card_summary, dict) else {}
    macro_summary = load_latest_macro_report()
    macro_headlines = macro_summary.get("headlines") or []
    event_summary = _load_json_file(EVENT_SUMMARY_PATH)

    ml_model_type = ""
    ml_train_rows = 0
    if os.path.exists(CARD_CSV_PATH):
        try:
            card_df = pd.read_csv(CARD_CSV_PATH)
            if "ml_model_type" in card_df.columns and not card_df["ml_model_type"].dropna().empty:
                ml_model_type = str(card_df["ml_model_type"].dropna().iloc[0] or "")
            if "ml_train_rows" in card_df.columns:
                ml_train_rows = int(pd.to_numeric(card_df["ml_train_rows"], errors="coerce").fillna(0).max())
        except Exception:
            pass

    macro_source = "missing"
    if macro_headlines:
        macro_source = str(macro_summary.get("source") or "cached")

    return {
        "factor": {
            "eligible": int(mart_summary.get("factor_coverage", 0) or 0),
            "dynamic_weight_status": "unknown",
        },
        "stock_card": {
            "rows": int(card_counts.get("total", mart_summary.get("row_count", 0)) or 0),
            "flow": int(card_counts.get("flow", mart_summary.get("flow_coverage", 0)) or 0),
            "intraday": int(card_counts.get("intraday", mart_summary.get("intraday_coverage", 0)) or 0),
            "event": int(card_counts.get("event", mart_summary.get("event_coverage", 0)) or 0),
            "micro": int(card_counts.get("micro", 0) or 0),
            "ml": int(card_counts.get("ml", mart_summary.get("ml_coverage", 0)) or 0),
            "ml_model_type": ml_model_type,
            "ml_train_rows": ml_train_rows,
        },
        "macro": {"headlines": int(len(macro_headlines)), "source": macro_source},
        "event": {
            "log_pending": int(((event_summary.get("metadata") or {}).get("pending_price_records", 0)) or 0),
            "replay_priced": 0,
        },
        "event_watcher": {"engine_calls": 0},
        "ws_replay": {"snapshot_logs": 0},
        "flow_logs": _run_flow_log_health(days=3),
        "trade_runtime": load_trade_runtime_health(),
    }


def run_all_checks() -> dict:
    return {
        "analyst": _run_analyst_smoke(),
        "stock_news": _run_stock_news_smoke(),
        "factor": _run_factor_smoke(),
        "stock_card": _run_card_smoke(),
        "macro": _run_macro_smoke(),
        "wics": _run_wics_smoke(),
        "event": _run_event_smoke(),
        "event_watcher": _run_event_watcher_smoke(),
        "ws_replay": _run_ws_replay_smoke(),
        "flow_intraday": _run_flow_intraday_smoke(),
        "mart": _run_mart_smoke(),
        "flow_logs": _run_flow_log_health(days=3),
        "trade_runtime": load_trade_runtime_health(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run disclosure runtime smoke checks.")
    parser.add_argument(
        "--mode",
        choices=["lite", "full"],
        default="full",
        help="lite reads local latest artifacts only; full runs external/network smoke checks too.",
    )
    args = parser.parse_args()

    results = run_lite_checks() if args.mode == "lite" else run_all_checks()
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
