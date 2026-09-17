[CmdletBinding()]
param(
    [switch]$WithServices,
    [switch]$WithRace
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    & $PSScriptRoot/check.ps1
    if ($LASTEXITCODE -ne 0) { throw "Shared checks failed." }

    if ($WithRace) {
        & go test -race ./...
        if ($LASTEXITCODE -ne 0) { throw "Go race checks failed." }
    } else {
        Write-Host "Skipped Go race checks; rerun with -WithRace."
    }

    if ($WithServices) {
        Write-Host "PostgreSQL/Kafka integration tests are not implemented at M0; running service health and durability smoke checks only."
        & $PSScriptRoot/smoke.ps1
        if ($LASTEXITCODE -ne 0) { throw "Real dependency smoke checks failed." }
    } else {
        Write-Host "Skipped PostgreSQL/Kafka smoke checks; rerun with -WithServices."
    }

    Write-Host "CI checks passed. Model and paid-provider checks are not part of this entry point."
} finally {
    Pop-Location
}
