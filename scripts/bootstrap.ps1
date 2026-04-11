$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$VenvDir = if ($env:VENV_DIR) { $env:VENV_DIR } else { Join-Path $RootDir ".venv" }

function Get-PythonInvocation {
    if ($env:PYTHON_BIN) {
        return @{
            Command = $env:PYTHON_BIN
            Args = @()
        }
    }

    if (Get-Command python3.11 -ErrorAction SilentlyContinue) {
        return @{
            Command = "python3.11"
            Args = @()
        }
    }

    if (Get-Command py -ErrorAction SilentlyContinue) {
        return @{
            Command = "py"
            Args = @("-3.11")
        }
    }

    if (Get-Command python -ErrorAction SilentlyContinue) {
        return @{
            Command = "python"
            Args = @()
        }
    }

    throw "Python 3.11 launcher not found. Install Python 3.11 first."
}

$Python = Get-PythonInvocation

if (-not (Test-Path $VenvDir)) {
    Write-Host "[bootstrap] creating virtualenv at $VenvDir"
    & $Python.Command @($Python.Args + @("-m", "venv", $VenvDir))
}

$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Virtualenv python not found at $VenvPython"
}

$dirs = @(
    "Disclosure\runtime\web_projections",
    "Disclosure\events\cache\parsed_details",
    "Disclosure\events\logs",
    "Disclosure\events\reports",
    "Disclosure\analyst_reports\raw",
    "Disclosure\analyst_reports\pdf_cache",
    "Disclosure\analyst_reports\summaries",
    "Disclosure\valuation"
)
foreach ($dir in $dirs) {
    New-Item -ItemType Directory -Force -Path (Join-Path $RootDir $dir) | Out-Null
}

Write-Host "[bootstrap] upgrading pip"
& $VenvPython -m pip install --upgrade pip

Write-Host "[bootstrap] installing python requirements"
& $VenvPython -m pip install -r (Join-Path $RootDir "requirements.txt")

if ($env:PLAYWRIGHT_SKIP_BROWSER_INSTALL -ne "1") {
    Write-Host "[bootstrap] installing playwright chromium"
    & $VenvPython -m playwright install chromium
}

Write-Host "[bootstrap] installing local node dependencies"
Push-Location $RootDir
try {
    npm install
}
finally {
    Pop-Location
}

Write-Host "[bootstrap] done"
