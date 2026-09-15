$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    $config = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
    $localConfig = Get-Content -LiteralPath ".env"
    function Get-LocalValue([string]$Name, [string]$Fallback) {
        $value = ($localConfig | Where-Object { $_ -match "^$Name=" }) -replace "^$Name=", ''
        if ($value) { return $value }
        return $Fallback
    }
    $databaseUser = Get-LocalValue "POSTGRES_USER" "durable"
    $databaseName = Get-LocalValue "POSTGRES_DB" "durable"
    $runtimeAPort = Get-LocalValue "RUNTIME_A_PORT" "8080"
    $runtimeBPort = Get-LocalValue "RUNTIME_B_PORT" "8081"
    $workerAPort = Get-LocalValue "WORKER_A_PORT" "8181"
    $workerBPort = Get-LocalValue "WORKER_B_PORT" "8182"
    $otelHealthPort = Get-LocalValue "OTEL_HEALTH_PORT" "13133"
    $prometheusPort = Get-LocalValue "PROMETHEUS_PORT" "9090"

    & docker @config exec -T postgres psql -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName -c "SELECT version(), current_setting('data_checksums'), current_setting('fsync'), current_setting('synchronous_commit'), current_setting('full_page_writes');"
    if ($LASTEXITCODE -ne 0) { throw "PostgreSQL smoke check failed." }

    $topics = & docker @config exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
    if ($LASTEXITCODE -ne 0) { throw "Kafka smoke check failed." }
    foreach ($topic in @("durable-agent.tasks.v1", "durable-agent.events.v1")) {
        if ($topics -notcontains $topic) { throw "Kafka topic '$topic' is missing." }
    }

    foreach ($url in @(
        "http://localhost:$runtimeAPort/healthz",
        "http://localhost:$runtimeBPort/healthz",
        "http://localhost:$workerAPort/healthz",
        "http://localhost:$workerBPort/healthz",
        "http://localhost:$otelHealthPort/",
        "http://localhost:$prometheusPort/-/healthy"
    )) {
        $lastError = $null
        foreach ($attempt in 1..12) {
            try {
                $response = Invoke-WebRequest -UseBasicParsing -Uri $url -TimeoutSec 5
                if ($response.StatusCode -eq 200) {
                    $lastError = $null
                    break
                }
                $lastError = "HTTP status $($response.StatusCode)"
            } catch {
                $lastError = $_.Exception.Message
            }
            Start-Sleep -Seconds 1
        }
        if ($null -ne $lastError) { throw "Health check failed for $($url): $lastError" }
    }
    $runtimeTargets = Invoke-RestMethod -Uri "http://localhost:$prometheusPort/api/v1/query?query=up%7Bjob%3D%22runtime%22%7D" -TimeoutSec 5
    if ($runtimeTargets.status -ne "success" -or $runtimeTargets.data.result.Count -ne 2) {
        throw "Prometheus did not return both runtime targets."
    }
    foreach ($target in $runtimeTargets.data.result) {
        if ($target.value[1] -ne "1") { throw "Prometheus reports a runtime target as down." }
    }
    Write-Host "PostgreSQL, Kafka, two runtimes, two workers, OpenTelemetry, and Prometheus are healthy; both runtimes are scraped."
} finally {
    Pop-Location
}
