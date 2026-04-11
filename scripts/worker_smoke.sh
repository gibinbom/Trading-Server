#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${WORKER_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python worker runtime not found at $PYTHON_BIN" >&2
  exit 1
fi

cd "$ROOT_DIR"

"$PYTHON_BIN" Disclosure/delayed_quote_collector.py --once --print-only --limit 20
"$PYTHON_BIN" Disclosure/fair_value_builder.py --once --print-only --top-n 5
"$PYTHON_BIN" Disclosure/web_projection_publisher.py --once --print-only
