[CmdletBinding()]
param(
    [string]$TraceRoot = "experiments/m5/traces",
    [string]$DurableRoot = "experiments/m5/durable",
    [string]$CheckerPath = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
$temporaryChecker = $false
try {
    if ([string]::IsNullOrWhiteSpace($CheckerPath)) {
        $buildRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("durable-m5-checker-" + [guid]::NewGuid().ToString("N"))
        New-Item -ItemType Directory -Force $buildRoot | Out-Null
        $CheckerPath = Join-Path $buildRoot "fault-checker.exe"
        & go build -o $CheckerPath ./cmd/fault-checker
        if ($LASTEXITCODE -ne 0) { throw "Could not build the fault checker." }
        $temporaryChecker = $true
    }
    $failures = @()
    $count = 0
    foreach ($trace in Get-ChildItem -LiteralPath $TraceRoot -Filter "*.jsonl" -File) {
        $snapshot = Join-Path $DurableRoot ($trace.BaseName + ".json")
        $output = & $CheckerPath -offline -trace $trace.FullName -durable-trace $snapshot 2>&1 | Out-String
        if ($LASTEXITCODE -ne 0) {
            $failures += "$($trace.Name): $($output.Trim())"
        }
        $count++
    }
    if ($failures.Count -gt 0) {
        throw "Archived M5 checker validation failed: $($failures -join '; ')"
    }
    Write-Host "Archived M5 checker validation passed for $count traces."
}
finally {
    if ($temporaryChecker -and (Test-Path -LiteralPath $buildRoot)) {
        Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    Pop-Location
}
