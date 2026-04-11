from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from typing import Any, Dict, List

import requests


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
RUNTIME_DIR = os.path.join(CURRENT_DIR, "runtime")
API_REPORT_LATEST_PATH = os.path.join(RUNTIME_DIR, "api_integrations_latest.json")

from config import SETTINGS
from dart_fast_fetch_v3 import fetch_dart_html_fast
from engine_html_client import DARTRecentHtmlClient, date_to_select_date
from kis_broker_factory import build_kis_broker_from_settings
from signals.macro_news_monitor import fetch_macro_news, load_latest_macro_report


def _load_ecosystem_trader_env() -> Dict[str, Any]:
    root_dir = os.path.dirname(CURRENT_DIR)
    ecosystem_path = os.path.join(root_dir, "ecosystem.config.js")
    if not os.path.exists(ecosystem_path):
        return {}
    node_script = r"""
const ecosystemPath = process.argv[1];
try {
  const config = require(ecosystemPath);
  const app = (config.apps || []).find((row) => row.name === 'disclosure-main-trader');
  const env = (app && app.env) || {};
  process.stdout.write(JSON.stringify(env));
} catch (err) {
  process.stdout.write('{}');
}
"""
    try:
        out = subprocess.check_output(
            ["node", "-e", node_script, ecosystem_path],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        payload = json.loads(out or "{}")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _looks_like_valid_html(html: str) -> bool:
    if not html:
        return False
    text = html.strip().lower()
    if len(text) < 1000:
        return False
    return any(marker in text for marker in ("<html", "<body", "<table", "<iframe"))


def _mask_webhook(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return "-"
    parts = text.split("/")
    if len(parts) < 3:
        return text[:8] + "..."
    tail = parts[-1]
    masked_tail = tail[:6] + "..." + tail[-4:] if len(tail) > 10 else tail
    return "/".join(parts[-3:-1] + [masked_tail])


def _recent_business_dates(limit: int = 5) -> List[str]:
    out: List[str] = []
    cursor = dt.datetime.now()
    while len(out) < max(1, int(limit)):
        if cursor.weekday() < 5:
            out.append(date_to_select_date(cursor))
        cursor -= dt.timedelta(days=1)
    return out


def _check_dart_recent(limit: int = 3) -> Dict[str, Any]:
    client = DARTRecentHtmlClient(timeout_sec=5.0, max_retries=2)
    out: Dict[str, Any] = {
        "ok": False,
        "source_date": "",
        "parsed_items": 0,
        "sample_title": "",
        "sample_rcp_no": "",
        "detail_status": None,
        "detail_has_iframe": False,
        "fast_valid": False,
        "errors": [],
    }

    items = []
    used_date = ""
    for candidate_date in _recent_business_dates(limit=limit):
        try:
            html = client.fetch_html(candidate_date, 1)
            items = client.parse_items(html, candidate_date)
            used_date = candidate_date
            if items:
                break
        except Exception as exc:
            out["errors"].append(f"list_fetch:{candidate_date}:{str(exc)[:120]}")

    out["source_date"] = used_date
    out["parsed_items"] = len(items)
    if not items:
        return out

    first = items[0]
    out["sample_title"] = str(first.title or "")
    out["sample_rcp_no"] = str(first.rcp_no or "")

    try:
        resp = requests.get(f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={first.rcp_no}", timeout=5)
        text = resp.text or ""
        out["detail_status"] = resp.status_code
        out["detail_has_iframe"] = "iframe" in text.lower() and "ifrm" in text.lower()
    except Exception as exc:
        out["errors"].append(f"detail_fetch:{str(exc)[:120]}")

    try:
        fast_html = fetch_dart_html_fast(first.rcp_no) or ""
        out["fast_valid"] = _looks_like_valid_html(fast_html)
    except Exception as exc:
        out["errors"].append(f"fast_fetch:{str(exc)[:120]}")

    out["ok"] = bool(out["parsed_items"] > 0 and out["detail_status"] == 200)
    return out


def _check_kis_quote(symbol: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": False,
        "symbol": symbol,
        "is_virtual": None,
        "dry_run": None,
        "configured_is_virtual": bool(getattr(SETTINGS, "KIS_IS_VIRTUAL", False)),
        "configured_dry_run": bool(getattr(SETTINGS, "DRY_RUN", False)),
        "last_price": None,
        "primary_ok": False,
        "fallback_used": False,
        "source": "",
        "errors": [],
    }
    try:
        broker = build_kis_broker_from_settings(is_virtual=False, dry_run=True)
        out["is_virtual"] = bool(getattr(broker, "is_virtual", False))
        out["dry_run"] = bool(getattr(broker, "dry_run", False))
        ctx = broker._get_market_context()
        mkt_code = "NX" if ctx["exch"] == "NXT" else "J"
        tr_id = "FHKST01010100"
        url = f"{broker.base}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {
            "fid_cond_mrkt_div_code": mkt_code,
            "fid_input_iscd": symbol,
        }
        try:
            resp = broker._call_api_with_retry("GET", url, tr_id, params=params)
            data = resp.json()
            pr = data.get("output", {}).get("stck_prpr")
            if pr is not None:
                out["last_price"] = float(str(pr).replace(",", "").strip())
                out["primary_ok"] = bool(out["last_price"] and float(out["last_price"]) > 0)
                out["ok"] = out["primary_ok"]
                out["source"] = "kis"
        except Exception as exc:
            out["errors"].append(f"primary:{str(exc)[:200]}")

        if not out["ok"]:
            price = broker.get_last_price(symbol)
            out["last_price"] = price
            out["ok"] = bool(price and float(price) > 0)
            out["fallback_used"] = bool(out["ok"])
            out["source"] = "naver_fallback" if out["ok"] else ""
    except Exception as exc:
        out["errors"].append(str(exc)[:200])
    return out


def _check_macro() -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": False, "source": "missing", "headline_count": 0, "errors": []}
    try:
        live_text = fetch_macro_news() or ""
        live_count = len([line for line in live_text.splitlines() if line.strip()])
        if live_count > 0:
            out["source"] = "live"
            out["headline_count"] = live_count
            out["ok"] = True
            return out
    except Exception as exc:
        out["errors"].append(f"live:{str(exc)[:120]}")

    try:
        latest = load_latest_macro_report() or {}
        headlines = latest.get("headlines") or []
        out["source"] = "cached" if headlines else "missing"
        out["headline_count"] = len(headlines)
        out["ok"] = bool(headlines)
    except Exception as exc:
        out["errors"].append(f"cached:{str(exc)[:120]}")
    return out


def _check_slack() -> Dict[str, Any]:
    ecosystem_env = _load_ecosystem_trader_env()
    process_url = str(getattr(SETTINGS, "SLACK_WEBHOOK_URL", "") or "")
    url = process_url or str(ecosystem_env.get("SLACK_WEBHOOK_URL") or "")
    process_skip = bool(getattr(SETTINGS, "SLACK_NOTIFY_TRADE_SKIP", False))
    ecosystem_skip = str(ecosystem_env.get("SLACK_NOTIFY_TRADE_SKIP") or "") in {"1", "true", "True", "YES", "yes"}
    return {
        "ok": bool(url),
        "enabled": bool(getattr(SETTINGS, "SLACK_ENABLED", False)),
        "webhook_hint": _mask_webhook(url),
        "notify_trade_skip": process_skip or ecosystem_skip,
        "config_source": "process_env" if process_url else ("ecosystem" if url else "missing"),
    }


def run_checks(symbol: str) -> Dict[str, Any]:
    dart = _check_dart_recent()
    kis = _check_kis_quote(symbol)
    macro = _check_macro()
    slack = _check_slack()

    warnings: List[str] = []
    if not dart["ok"]:
        warnings.append("DART recent disclosure probe is not healthy")
    if not kis["ok"]:
        warnings.append("KIS quote probe failed")
    elif kis.get("fallback_used"):
        warnings.append("KIS primary quote failed; Naver fallback is active")
    if not macro["ok"]:
        warnings.append("macro news source is empty")
    if not slack["ok"]:
        warnings.append("Slack webhook is missing")

    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": "ok" if not warnings else "warn",
        "warnings": warnings,
        "dart": dart,
        "kis": kis,
        "macro": macro,
        "slack": slack,
    }


def save_report(payload: Dict[str, Any]) -> str:
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    with open(API_REPORT_LATEST_PATH, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    return API_REPORT_LATEST_PATH


def render_text(payload: Dict[str, Any]) -> str:
    dart = payload.get("dart") or {}
    kis = payload.get("kis") or {}
    macro = payload.get("macro") or {}
    slack = payload.get("slack") or {}
    lines = [
        "[API Integration Check]",
        f"- generated: {payload.get('generated_at')}",
        f"- overall status: {payload.get('status')}",
        f"- DART: ok={dart.get('ok')} | date={dart.get('source_date') or '-'} | items={dart.get('parsed_items')} | detail={dart.get('detail_status')} | fast_valid={dart.get('fast_valid')} | sample={dart.get('sample_rcp_no') or '-'} {dart.get('sample_title') or ''}".rstrip(),
        f"- KIS: ok={kis.get('ok')} | primary={kis.get('primary_ok')} | source={kis.get('source') or '-'} | symbol={kis.get('symbol')} | probe_virtual={kis.get('is_virtual')} | probe_dry_run={kis.get('dry_run')} | config_virtual={kis.get('configured_is_virtual')} | config_dry_run={kis.get('configured_dry_run')} | last_price={kis.get('last_price')}",
        f"- Macro: ok={macro.get('ok')} | source={macro.get('source')} | headlines={macro.get('headline_count')}",
        f"- Slack: ok={slack.get('ok')} | enabled={slack.get('enabled')} | skip_notify={slack.get('notify_trade_skip')} | source={slack.get('config_source')} | webhook={slack.get('webhook_hint')}",
    ]
    warnings = payload.get("warnings") or []
    if warnings:
        lines.append("Warnings")
        for item in warnings:
            lines.append(f"- {item}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run external API/integration probes for Disclosure runtime.")
    parser.add_argument("--symbol", default="005930", help="Symbol to use for KIS quote probe.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text.")
    parser.add_argument("--no-save", action="store_true", help="Do not write latest report artifact.")
    args = parser.parse_args()

    payload = run_checks(args.symbol)
    if not args.no_save:
        payload["report_path"] = save_report(payload)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(render_text(payload))
    return 0 if payload.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
