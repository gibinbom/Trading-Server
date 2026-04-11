#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "[1/2] upgrading pip"
"${PYTHON_BIN}" -m pip install --upgrade pip

echo "[2/2] installing market/runtime dependencies"
"${PYTHON_BIN}" -m pip install \
  FinanceDataReader \
  pykrx \
  google-genai \
  beautifulsoup4 \
  html5lib

echo "done"
