$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    $config = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
    $databaseUser = (Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_USER=' }) -replace '^POSTGRES_USER=', ''
    $databaseName = (Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_DB=' }) -replace '^POSTGRES_DB=', ''
    $topic = "dur001-restart-smoke"

    & docker @config exec -T postgres psql -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName -c "CREATE TABLE IF NOT EXISTS engine.environment_restart_smoke (id integer PRIMARY KEY); INSERT INTO engine.environment_restart_smoke VALUES (1) ON CONFLICT DO NOTHING;"
    if ($LASTEXITCODE -ne 0) { throw "Could not create PostgreSQL restart marker." }
    & docker @config exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
    if ($LASTEXITCODE -ne 0) { throw "Could not create Kafka restart marker topic." }

    & docker @config up -d --force-recreate --wait postgres kafka kafka-init
    if ($LASTEXITCODE -ne 0) { throw "Dependencies did not recover after container recreation." }

    & docker @config exec -T postgres psql -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName -c "SELECT id FROM engine.environment_restart_smoke WHERE id = 1; DROP TABLE engine.environment_restart_smoke;"
    if ($LASTEXITCODE -ne 0) { throw "PostgreSQL restart marker did not survive." }
    $topics = & docker @config exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
    if ($LASTEXITCODE -ne 0 -or $topics -notcontains $topic) { throw "Kafka restart marker topic did not survive." }
    & docker @config exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --delete --topic $topic
    if ($LASTEXITCODE -ne 0) { throw "Could not clean up Kafka restart marker topic." }
    Write-Host "PostgreSQL and Kafka retained their markers across container recreation."
} finally {
    Pop-Location
}
