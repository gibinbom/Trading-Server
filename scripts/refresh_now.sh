#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${WORKER_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python worker runtime not found at $PYTHON_BIN" >&2
  exit 1
fi

cd "$ROOT_DIR"

echo "[seed] consensus incremental"
"$PYTHON_BIN" Disclosure/consensus_refresh.py --mode incremental --once --workers 12

echo "[seed] actual financial"
"$PYTHON_BIN" Disclosure/actual_financial_refresh.py --once

echo "[seed] delayed quote"
"$PYTHON_BIN" Disclosure/delayed_quote_collector.py --once

echo "[seed] fair value"
"$PYTHON_BIN" Disclosure/fair_value_builder.py --once --top-n 20

echo "[seed] flow snapshot"
"$PYTHON_BIN" Disclosure/flow_snapshot_builder.py --mode full --disable-kis --once

echo "[seed] sector rotation history"
"$PYTHON_BIN" Disclosure/sector_rotation_history_builder.py --weeks 52 --once

echo "[seed] web projection publish"
"$PYTHON_BIN" Disclosure/web_projection_publisher.py --once

echo "[seed] done"
