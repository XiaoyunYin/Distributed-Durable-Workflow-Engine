[CmdletBinding()]
param(
    [switch]$WithServices,
    [switch]$WithRace
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$previousRequireDatabase = $env:DURABLE_REQUIRE_DATABASE
if ($WithServices) { $env:DURABLE_REQUIRE_DATABASE = "1" }
Push-Location $RepoRoot
try {
    if ($WithServices) {
        Write-Host "Applying numbered migrations before database-backed checks."
        & $PSScriptRoot/migrate.ps1
        if ($LASTEXITCODE -ne 0) { throw "Database migrations failed." }
    }

    & $PSScriptRoot/check.ps1
    if ($LASTEXITCODE -ne 0) { throw "Shared checks failed." }

    if ($WithRace) {
        & go test -race ./...
        if ($LASTEXITCODE -ne 0) { throw "Go race checks failed." }
    } else {
        Write-Host "Skipped Go race checks; rerun with -WithRace."
    }

    if ($WithServices) {
        Write-Host "Running DUR-005 and DUR-006 PostgreSQL integration tests."
        & go test ./internal/state -run '^TestPostgresStateRepository$' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "DUR-005 PostgreSQL integration tests failed." }
        & go test ./internal/api -run '^TestWorkflowAPIResponseLossHistoryAndRetention$' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "DUR-006 API integration tests failed." }
        & $PSScriptRoot/smoke.ps1
        if ($LASTEXITCODE -ne 0) { throw "Real dependency smoke checks failed." }
    } else {
        Write-Host "Skipped PostgreSQL/Kafka smoke checks; rerun with -WithServices."
    }

    Write-Host "CI checks passed. Model and paid-provider checks are not part of this entry point."
} finally {
    if ($null -eq $previousRequireDatabase) {
        Remove-Item Env:DURABLE_REQUIRE_DATABASE -ErrorAction SilentlyContinue
    } else {
        $env:DURABLE_REQUIRE_DATABASE = $previousRequireDatabase
    }
    Pop-Location
}
