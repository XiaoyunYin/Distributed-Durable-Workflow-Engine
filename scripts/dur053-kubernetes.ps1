#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateSet("compose", "kind")][string]$Context = "compose",
    [string]$Scenario = "prerequisites"
)
$ErrorActionPreference = "Stop"
if ($Context -ne "compose" -or $Scenario -ne "prerequisites") {
    throw "Only -Context compose -Scenario prerequisites is implemented in K2."
}
$RepoRoot = Split-Path -Parent $PSScriptRoot
$createdScratch = $false
$scratch = $null
Push-Location $RepoRoot
try {
    # Set before uv can create cache/download scratch. The Python driver owns
    # a child directory and removes only that directory after exporting evidence.
    $scratch = if ($IsWindows) { "D:\coding agent files\durable-engine\codex\k2c" } else {
        if (-not $env:RUNNER_TEMP) { throw "RUNNER_TEMP is required on Linux." }
        Join-Path $env:RUNNER_TEMP "dur053-k2c"
    }
    $createdScratch = -not (Test-Path -LiteralPath $scratch)
    New-Item -ItemType Directory -Force -Path $scratch | Out-Null
    $env:TMP = $scratch
    $env:TEMP = $scratch
    $env:TMPDIR = $scratch
    $env:GOTMPDIR = $scratch
    $env:GOCACHE = Join-Path $scratch "go-cache"
    $env:UV_NO_CACHE = "1"
    $env:PYTHONDONTWRITEBYTECODE = "1"
    & uv run python "$PSScriptRoot/dur053_prerequisites.py" --scratch-root $scratch
    if ($LASTEXITCODE -ne 0) { throw "DUR-053 Compose prerequisites FAIL; see the evidence manifest." }
} finally {
    if ($createdScratch -and $scratch -and (Test-Path -LiteralPath $scratch) -and @(Get-ChildItem -LiteralPath $scratch -Force).Count -eq 0) {
        Remove-Item -LiteralPath $scratch
    }
    Pop-Location
}
