[CmdletBinding()]
param(
    [switch]$WithServices,
    [switch]$WithRace,
    [switch]$WithM5
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$previousRequireDatabase = $env:DURABLE_REQUIRE_DATABASE
$previousKafkaBrokers = $env:DURABLE_KAFKA_BROKERS
$ciRelaysStopped = $false
$composeArgs = @("--env-file", ".env", "-f", "deploy/local/compose.yaml")
if ($WithM5 -and -not $WithServices) {
    throw "-WithM5 requires -WithServices so the campaign cannot silently skip database-backed cases."
}
if ($WithServices) {
    $env:DURABLE_REQUIRE_DATABASE = "1"
    $kafkaPort = (Get-Content -LiteralPath (Join-Path $RepoRoot ".env") |
        Where-Object { $_ -match '^KAFKA_PORT=' } | Select-Object -First 1) -replace '^KAFKA_PORT=', ''
    if ([string]::IsNullOrWhiteSpace($kafkaPort)) { $kafkaPort = "9092" }
    $env:DURABLE_KAFKA_BROKERS = "127.0.0.1:$kafkaPort"
}
Push-Location $RepoRoot
try {
    if ($WithServices) {
        Write-Host "Applying numbered migrations before database-backed checks."
        & $PSScriptRoot/migrate.ps1
        if ($LASTEXITCODE -ne 0) { throw "Database migrations failed." }

        # Database-backed package tests create claimable outbox rows. Keep
        # live relays/workers from consuming those fixtures while the shared
        # and race suites run; restore them before smoke and campaign checks.
        Write-Host "Stopping runtime/worker relays to isolate service-backed test fixtures."
        & docker compose @composeArgs stop runtime-a runtime-b worker-a worker-b
        if ($LASTEXITCODE -ne 0) { throw "Could not isolate runtime/worker relays." }
        $ciRelaysStopped = $true
    }

    if ($WithServices) {
        & $PSScriptRoot/check.ps1 -SerialPackages
    } else {
        & $PSScriptRoot/check.ps1
    }
    if ($LASTEXITCODE -ne 0) { throw "Shared checks failed." }

    if ($WithRace) {
        if ($WithServices) {
            # Database-backed tests in several packages share the local
            # PostgreSQL lease table. Serialize package test binaries in
            # service mode so fixture cleanup and lease races cannot make the
            # required validation nondeterministic.
            Write-Host "Running race tests serially in service mode to isolate shared database fixtures."
            & go test -race -p 1 ./...
        } else {
            & go test -race ./...
        }
        if ($LASTEXITCODE -ne 0) { throw "Go race checks failed." }
    } else {
        Write-Host "Skipped Go race checks; rerun with -WithRace."
    }

    if ($WithServices) {
        Write-Host "Running DUR-005 through DUR-018 PostgreSQL/Kafka integration tests."
        & go test ./internal/state -run '^TestPostgresStateRepository$' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "DUR-005 PostgreSQL integration tests failed." }
        & go test ./internal/api -run '^TestWorkflowAPIResponseLossHistoryAndRetention$' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "DUR-006 API integration tests failed." }
        & go test ./internal/engine -run '^TestM1' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "DUR-007 interpreter integration tests failed." }
        & go test ./internal/state -run '^TestM3' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "M3 state integration tests failed." }
        & go test ./internal/transport -run '^TestM3' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "M3 transport integration tests failed." }
        & go test ./internal/state -run '^TestM4' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "M4 state integration tests failed." }
        & go test ./internal/engine -run '^TestM4' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "M4 engine integration tests failed." }
        & go test ./internal/invariants -run '^TestM4' -count=1 -v
        if ($LASTEXITCODE -ne 0) { throw "M4 invariant integration tests failed." }
        if ($ciRelaysStopped) {
            Write-Host "Restoring runtime/worker services before dependency smoke checks."
            & docker compose @composeArgs up -d --wait runtime-a runtime-b worker-a worker-b
            if ($LASTEXITCODE -ne 0) { throw "Could not restore runtime/worker services." }
            $ciRelaysStopped = $false
        }
        & $PSScriptRoot/smoke.ps1
        if ($LASTEXITCODE -ne 0) { throw "Real dependency smoke checks failed." }
        if ($WithM5) {
            Write-Host "Running M5 bounded two-scheduler smoke."
            & go test ./internal/engine -run '^TestM5BoundedTwoSchedulerSmoke$' -count=1 -v
            if ($LASTEXITCODE -ne 0) { throw "M5 two-scheduler smoke failed." }
            Write-Host "Running M5 F01-F11 correctness campaign."
            & $PSScriptRoot/m5-campaign.ps1 -StopRuntimeRelays
            if ($LASTEXITCODE -ne 0) { throw "M5 F01-F11 campaign failed." }
        }
    } else {
        Write-Host "Skipped PostgreSQL/Kafka smoke checks; rerun with -WithServices."
    }

    Write-Host "CI checks passed. Model and paid-provider checks are not part of this entry point."
} finally {
    if ($ciRelaysStopped) {
        & docker compose @composeArgs up -d --wait runtime-a runtime-b worker-a worker-b
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Could not restore runtime/worker services after CI failure."
        }
    }
    if ($null -eq $previousRequireDatabase) {
        Remove-Item Env:DURABLE_REQUIRE_DATABASE -ErrorAction SilentlyContinue
    } else {
        $env:DURABLE_REQUIRE_DATABASE = $previousRequireDatabase
    }
    if ($null -eq $previousKafkaBrokers) {
        Remove-Item Env:DURABLE_KAFKA_BROKERS -ErrorAction SilentlyContinue
    } else {
        $env:DURABLE_KAFKA_BROKERS = $previousKafkaBrokers
    }
    Pop-Location
}
