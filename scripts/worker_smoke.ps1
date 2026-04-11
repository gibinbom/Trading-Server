$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$Python = if ($env:WORKER_PYTHON_BIN) { $env:WORKER_PYTHON_BIN } else { Join-Path $RootDir ".venv\Scripts\python.exe" }

if (-not (Test-Path $Python)) {
    throw "python worker runtime not found at $Python"
}

Push-Location $RootDir
try {
    & $Python "Disclosure/delayed_quote_collector.py" --once --print-only --limit 20
    & $Python "Disclosure/fair_value_builder.py" --once --print-only --top-n 5
    & $Python "Disclosure/web_projection_publisher.py" --once --print-only
}
finally {
    Pop-Location
}
