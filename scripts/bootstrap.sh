#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

mkdir -p \
  "$ROOT_DIR/Disclosure/runtime/web_projections" \
  "$ROOT_DIR/Disclosure/events/cache/parsed_details" \
  "$ROOT_DIR/Disclosure/events/logs" \
  "$ROOT_DIR/Disclosure/events/reports" \
  "$ROOT_DIR/Disclosure/analyst_reports/raw" \
  "$ROOT_DIR/Disclosure/analyst_reports/pdf_cache" \
  "$ROOT_DIR/Disclosure/analyst_reports/summaries" \
  "$ROOT_DIR/Disclosure/valuation"

if [[ ! -d "$VENV_DIR" ]]; then
  echo "[bootstrap] creating virtualenv at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

echo "[bootstrap] upgrading pip"
"$VENV_DIR/bin/pip" install --upgrade pip

echo "[bootstrap] installing python requirements"
"$VENV_DIR/bin/pip" install -r "$ROOT_DIR/requirements.txt"

if [[ "${PLAYWRIGHT_SKIP_BROWSER_INSTALL:-0}" != "1" ]]; then
  echo "[bootstrap] installing playwright chromium"
  "$VENV_DIR/bin/python" -m playwright install chromium
fi

echo "[bootstrap] installing local node dependencies"
cd "$ROOT_DIR"
npm install

echo "[bootstrap] done"
