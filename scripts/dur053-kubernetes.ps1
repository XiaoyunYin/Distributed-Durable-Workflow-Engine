#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateSet("compose", "kind")][string]$Context = "compose",
    [string]$Scenario = "prerequisites",
    [string]$OutputRoot,
    [string]$PilotJob,
    [string]$K4Job,
    [string]$ReplacementFor,
    [string]$Checker
)
$ErrorActionPreference = "Stop"
if ($Context -eq "kind") {
    if ($Scenario -notin @("package-check", "smoke", "cleanup", "pilot", "pilot-smoke", "lifecycle", "lifecycle-smoke", "verify-evidence")) { throw "Scenario is not implemented." }
    if (-not $OutputRoot) { throw "Kind scenarios require a fresh -OutputRoot outside the repository." }
    $kindScratch = if ($IsWindows) { "D:\coding agent files\durable-engine\codex\k3a" } else {
        if (-not $env:RUNNER_TEMP) { throw "RUNNER_TEMP is required." }
        Join-Path $env:RUNNER_TEMP "dur053-k3a"
    }
    New-Item -ItemType Directory -Force -Path $kindScratch | Out-Null
    $env:TMP = $kindScratch
    $env:TEMP = $kindScratch
    $env:TMPDIR = $kindScratch
    $env:PYTHONDONTWRITEBYTECODE = "1"
    if ($Scenario -eq "pilot" -and -not $PilotJob) { throw "Pilot requires an exact -PilotJob from Amendment B.2." }
    if ($Scenario -eq "lifecycle" -and -not $K4Job) { throw "Lifecycle requires an exact -K4Job from Amendment B.7." }
    if ($Scenario -eq "verify-evidence" -and -not $Checker) { throw "Offline verification requires -Checker." }
    if ($Scenario -in @("lifecycle", "lifecycle-smoke", "verify-evidence") -or ($Scenario -eq "cleanup" -and $K4Job)) {
        $k4Argument = if ($Scenario -eq "verify-evidence") { $Checker } elseif ($Scenario -eq "cleanup") { "k4" } else { $K4Job }
        & bash "$PSScriptRoot/dur053-k3.sh" $Scenario $OutputRoot $kindScratch $k4Argument $ReplacementFor
    } else {
        & bash "$PSScriptRoot/dur053-k3.sh" $Scenario $OutputRoot $kindScratch $PilotJob
    }
    if ($LASTEXITCODE -ne 0) { throw "DUR-053 K3a FAIL; see retained evidence." }
    return
}
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
    $driverArgs = @("--scratch-root", $scratch)
    if ($OutputRoot) { $driverArgs += @("--output", $OutputRoot) }
    & uv run python "$PSScriptRoot/dur053_prerequisites.py" @driverArgs
    if ($LASTEXITCODE -ne 0) { throw "DUR-053 Compose prerequisites FAIL; see the evidence manifest." }
} finally {
    if ($createdScratch -and $scratch -and (Test-Path -LiteralPath $scratch) -and @(Get-ChildItem -LiteralPath $scratch -Force).Count -eq 0) {
        Remove-Item -LiteralPath $scratch
    }
    Pop-Location
}
