#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PM2_BIN="$ROOT_DIR/node_modules/.bin/pm2"
ECOSYSTEM_PATH="$ROOT_DIR/ecosystem.config.cjs"

WORKER_APPS=(
  worker-read-api
  worker-consensus-refresh-full
  worker-consensus-refresh-incremental
  worker-actual-financial-refresh
  worker-fair-value-builder
  worker-delayed-quote
  worker-flow-snapshot-full
  worker-flow-snapshot-incremental
  worker-sector-rotation-history
  worker-event-collector
  worker-web-projection
  worker-macro-news
)

need_pm2() {
  if [[ ! -x "$PM2_BIN" ]]; then
    echo "pm2 not installed. Run: npm install" >&2
    exit 1
  fi
}

app_exists() {
  local target="$1"
  local jlist
  jlist="$("$PM2_BIN" jlist 2>/dev/null || printf '[]')"
  PM2_JLIST="$jlist" python3 - "$target" <<'PY'
import json
import os
import sys

target = sys.argv[1]
try:
    rows = json.loads(os.environ.get("PM2_JLIST", "[]"))
except Exception:
    print("0")
    raise SystemExit(0)

found = any(str(row.get("name") or "") == target for row in rows if isinstance(row, dict))
print("1" if found else "0")
PY
}

ensure_started() {
  local app
  for app in "${WORKER_APPS[@]}"; do
    if [[ "$(app_exists "$app")" == "1" ]]; then
      echo "[restart] $app"
      "$PM2_BIN" restart "$app" --update-env
    else
      echo "[start] $app"
      "$PM2_BIN" start "$ECOSYSTEM_PATH" --only "$app"
    fi
  done
}

cmd="${1:-status}"
case "$cmd" in
  up|start)
    need_pm2
    ensure_started
    ;;
  reload)
    need_pm2
    "$PM2_BIN" reload "$ECOSYSTEM_PATH" --update-env
    ;;
  stop)
    need_pm2
    for app in "${WORKER_APPS[@]}"; do
      "$PM2_BIN" stop "$app" >/dev/null 2>&1 || true
    done
    "$PM2_BIN" ls
    ;;
  delete)
    need_pm2
    for app in "${WORKER_APPS[@]}"; do
      "$PM2_BIN" delete "$app" >/dev/null 2>&1 || true
    done
    "$PM2_BIN" ls
    ;;
  status)
    need_pm2
    "$PM2_BIN" ls
    ;;
  logs)
    need_pm2
    app="${2:-worker-web-projection}"
    lines="${3:-100}"
    "$PM2_BIN" logs "$app" --lines "$lines"
    ;;
  *)
    cat <<'EOF'
Usage:
  ./scripts/worker_pm2.sh up
  ./scripts/worker_pm2.sh reload
  ./scripts/worker_pm2.sh stop
  ./scripts/worker_pm2.sh delete
  ./scripts/worker_pm2.sh status
  ./scripts/worker_pm2.sh logs [app] [lines]
EOF
    exit 1
    ;;
esac
