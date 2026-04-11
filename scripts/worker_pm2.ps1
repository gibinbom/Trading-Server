$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$Pm2Bin = Join-Path $RootDir "node_modules\.bin\pm2.cmd"
$EcosystemPath = Join-Path $RootDir "ecosystem.config.cjs"

$WorkerApps = @(
    "worker-consensus-refresh-full",
    "worker-consensus-refresh-incremental",
    "worker-actual-financial-refresh",
    "worker-fair-value-builder",
    "worker-delayed-quote",
    "worker-flow-snapshot-full",
    "worker-flow-snapshot-incremental",
    "worker-sector-rotation-history",
    "worker-event-collector",
    "worker-web-projection",
    "worker-macro-news"
)

function Need-Pm2 {
    if (-not (Test-Path $Pm2Bin)) {
        throw "pm2 not installed. Run npm install first."
    }
}

function App-Exists([string]$Name) {
    $json = & $Pm2Bin jlist 2>$null
    if (-not $json) { return $false }
    $rows = $json | ConvertFrom-Json
    return [bool]($rows | Where-Object { $_.name -eq $Name })
}

function Ensure-Started {
    foreach ($app in $WorkerApps) {
        if (App-Exists $app) {
            Write-Host "[restart] $app"
            & $Pm2Bin restart $app --update-env
        }
        else {
            Write-Host "[start] $app"
            & $Pm2Bin start $EcosystemPath --only $app
        }
    }
}

$Command = if ($args.Count -gt 0) { $args[0] } else { "status" }

Need-Pm2

switch ($Command) {
    { $_ -in @("up", "start") } { Ensure-Started }
    "reload" { & $Pm2Bin reload $EcosystemPath --update-env }
    "stop" {
        foreach ($app in $WorkerApps) {
            & $Pm2Bin stop $app 2>$null
        }
        & $Pm2Bin ls
    }
    "delete" {
        foreach ($app in $WorkerApps) {
            & $Pm2Bin delete $app 2>$null
        }
        & $Pm2Bin ls
    }
    "status" { & $Pm2Bin ls }
    "logs" {
        $app = if ($args.Count -gt 1) { $args[1] } else { "worker-web-projection" }
        $lines = if ($args.Count -gt 2) { $args[2] } else { "100" }
        & $Pm2Bin logs $app --lines $lines
    }
    default {
        @"
Usage:
  .\scripts\worker_pm2.ps1 up
  .\scripts\worker_pm2.ps1 reload
  .\scripts\worker_pm2.ps1 stop
  .\scripts\worker_pm2.ps1 delete
  .\scripts\worker_pm2.ps1 status
  .\scripts\worker_pm2.ps1 logs [app] [lines]
"@ | Write-Host
        exit 1
    }
}
