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
        Write-Host "Applying numbered migrations and running DUR-005 PostgreSQL integration tests."
        & $PSScriptRoot/migrate.ps1
        if ($LASTEXITCODE -ne 0) { throw "Database migrations failed." }
        & go test ./internal/state -run '^TestPostgresStateRepository$' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "DUR-005 PostgreSQL integration tests failed." }
        & $PSScriptRoot/smoke.ps1
        if ($LASTEXITCODE -ne 0) { throw "Real dependency smoke checks failed." }
    } else {
        Write-Host "Skipped PostgreSQL/Kafka smoke checks; rerun with -WithServices."
    }

    Write-Host "CI checks passed. Model and paid-provider checks are not part of this entry point."
} finally {
    Pop-Location
}
