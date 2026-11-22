[CmdletBinding()]
param(
    [switch]$SerialPackages,
    [switch]$KafkaPythonOnly
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    $unformatted = & gofmt -l cmd internal
    if ($LASTEXITCODE -ne 0) { throw "gofmt failed." }
    if ($unformatted) { throw "Go files need formatting: $($unformatted -join ', ')" }
    & go vet ./...
    if ($LASTEXITCODE -ne 0) { throw "go vet failed." }
    if ($SerialPackages) {
        Write-Host "Running shared Go checks serially to isolate database-backed fixtures."
        & go test -p 1 ./...
    } else {
        & go test ./...
    }
    if ($LASTEXITCODE -ne 0) { throw "Go tests failed." }
    & go build ./cmd/runtime
    if ($LASTEXITCODE -ne 0) { throw "Go build failed." }

    & uv run ruff check python tests
    if ($LASTEXITCODE -ne 0) { throw "Ruff lint failed." }
    & uv run ruff format --check python tests
    if ($LASTEXITCODE -ne 0) { throw "Ruff formatting check failed." }
    & uv run mypy
    if ($LASTEXITCODE -ne 0) { throw "Mypy failed." }
    if ($KafkaPythonOnly) {
        if ([string]::IsNullOrWhiteSpace($env:DURABLE_KAFKA_BROKERS)) {
            throw "The Kafka Python check requires a real broker."
        }
        # The parallel repository checks run the full Python suite.
        & uv run pytest tests/test_worker_kafka_lifecycle.py::test_real_kafka_assignment_during_long_activity
    } else {
        & uv run pytest
    }
    if ($LASTEXITCODE -ne 0) { throw "Python tests failed." }
} finally {
    Pop-Location
}
