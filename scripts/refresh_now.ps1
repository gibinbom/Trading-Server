$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$Python = if ($env:WORKER_PYTHON_BIN) { $env:WORKER_PYTHON_BIN } else { Join-Path $RootDir ".venv\Scripts\python.exe" }

if (-not (Test-Path $Python)) {
    throw "python worker runtime not found at $Python"
}

Push-Location $RootDir
try {
    Write-Host "[seed] consensus incremental"
    & $Python "Disclosure/consensus_refresh.py" --mode incremental --once --workers 12

    Write-Host "[seed] actual financial"
    & $Python "Disclosure/actual_financial_refresh.py" --once

    Write-Host "[seed] delayed quote"
    & $Python "Disclosure/delayed_quote_collector.py" --once

    Write-Host "[seed] fair value"
    & $Python "Disclosure/fair_value_builder.py" --once --top-n 20

    Write-Host "[seed] flow snapshot"
    & $Python "Disclosure/flow_snapshot_builder.py" --mode full --disable-kis --once

    Write-Host "[seed] sector rotation history"
    & $Python "Disclosure/sector_rotation_history_builder.py" --weeks 52 --once

    Write-Host "[seed] web projection publish"
    & $Python "Disclosure/web_projection_publisher.py" --once

    Write-Host "[seed] done"
}
finally {
    Pop-Location
}
