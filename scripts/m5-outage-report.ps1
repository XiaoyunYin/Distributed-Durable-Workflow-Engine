[CmdletBinding()]
param(
    [string]$OutputPath = "experiments/m5/outage-recovery.json",
    [string]$RuntimeBase = "http://127.0.0.1:8080"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
$composeArgs = @("--env-file", ".env", "-f", "deploy/local/compose.yaml")
$stopped = @{}

function EnvValue([string]$Name, [string]$Fallback) {
    $line = Get-Content -LiteralPath ".env" | Where-Object { $_ -match ("^" + [regex]::Escape($Name) + "=") } | Select-Object -First 1
    if ($line) { return ($line -replace ("^" + [regex]::Escape($Name) + "="), "") }
    return $Fallback
}

function ProbeRuntime {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri ($RuntimeBase + "/v1/workflows/m5-outage-probe") -TimeoutSec 3
        return [pscustomobject]@{ status = [int]$response.StatusCode; dependency = if ($response.StatusCode -eq 404) { "reachable" } else { "unexpected" } }
    } catch {
        $status = 0
        if ($_.Exception.Response) { $status = [int]$_.Exception.Response.StatusCode.value__ }
        return [pscustomobject]@{ status = $status; dependency = if ($status -eq 404) { "reachable" } elseif ($status -eq 503) { "unavailable" } else { "error" } }
    }
}

function ProbeURL([string]$URL) {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $URL -TimeoutSec 3
        return [pscustomobject]@{ status = [int]$response.StatusCode; dependency = "reachable" }
    } catch {
        $status = 0
        if ($_.Exception.Response) { $status = [int]$_.Exception.Response.StatusCode.value__ }
        return [pscustomobject]@{ status = $status; dependency = "unavailable" }
    }
}

function ProbeKafka {
    try {
        & docker compose @composeArgs exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { return [pscustomobject]@{ status = 200; dependency = "reachable" } }
    } catch { }
    return [pscustomobject]@{ status = 503; dependency = "unavailable" }
}

function ProbeDependency([string]$Dependency) {
    switch ($Dependency) {
        "kafka" { return (ProbeKafka) }
        "postgres" {
            $snapshot = Snapshot "dependency-probe"
            return [pscustomobject]@{ status = if ($snapshot.status -eq "available") { 200 } else { 503 }; dependency = $snapshot.status }
        }
        "worker-a" { return (ProbeURL "http://127.0.0.1:8181/healthz") }
        "runtime-a" { return (ProbeRuntime) }
        default { return [pscustomobject]@{ status = 0; dependency = "unknown" } }
    }
}

function Snapshot([string]$Label) {
    $user = EnvValue "POSTGRES_USER" "postgres"
    $database = EnvValue "POSTGRES_DB" "durable_agent"
    $sql = @"
SELECT json_build_object(
  'pending_outbox_rows', (SELECT count(*) FROM engine.outbox WHERE publish_state IN ('PENDING','CLAIMED')),
  'open_reconciliation_items', (SELECT count(*) FROM engine.reconciliation_items WHERE status = 'OPEN'),
  'quarantined_outbox_rows', (SELECT count(*) FROM engine.outbox WHERE publish_state = 'QUARANTINED'),
  'poison_records', (SELECT count(*) FROM engine.transport_quarantine),
  'inflight_attempts', (SELECT count(*) FROM engine.activity_attempts WHERE state = 'CLAIMED'),
  'oldest_backlog_age_seconds', COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - LEAST(
    (SELECT min(created_at) FROM engine.outbox WHERE publish_state IN ('PENDING','CLAIMED')),
    (SELECT min(created_at) FROM engine.reconciliation_items WHERE status = 'OPEN'),
    (SELECT min(recorded_at) FROM engine.transport_quarantine)
  ))), 0)
)::text;
"@
    try {
        $raw = & docker compose @composeArgs exec -T postgres psql -v ON_ERROR_STOP=1 -At -U $user -d $database -c $sql 2>$null | Out-String
    } catch {
        return [pscustomobject]@{ label = $Label; captured_at = (Get-Date).ToUniversalTime().ToString("o"); status = "unavailable"; unresolved_work = $null }
    }
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw)) {
        return [pscustomobject]@{ label = $Label; captured_at = (Get-Date).ToUniversalTime().ToString("o"); status = "unavailable"; unresolved_work = $null }
    }
    $counts = $raw.Trim() | ConvertFrom-Json
    return [pscustomobject]@{ label = $Label; captured_at = (Get-Date).ToUniversalTime().ToString("o"); status = "available"; unresolved_work = $counts }
}

function StopComposeService([string]$Name) {
    & docker compose @composeArgs stop $Name | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Could not stop $Name" }
    $stopped[$Name] = $true
}

function StartComposeService([string]$Name) {
    & docker compose @composeArgs up -d --wait $Name | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Could not start $Name" }
    $stopped.Remove($Name)
}

function Episode([string]$Id, [string]$Dependency, [scriptblock]$Stop, [scriptblock]$Start) {
    $beforeDependency = ProbeDependency $Dependency
    $before = Snapshot "${Id}:before"
    & $Stop
    Start-Sleep -Seconds 3
    $duringDependency = ProbeDependency $Dependency
    $duringProbe = ProbeRuntime
    $during = Snapshot "${Id}:during"
    & $Start
    Start-Sleep -Seconds 5
    $afterDependency = ProbeDependency $Dependency
    $afterProbe = ProbeRuntime
    $after = Snapshot "${Id}:after"
    [pscustomobject]@{
        id = $Id
        dependency = $Dependency
        before = $before
        during = $during
        after = $after
        probe = [pscustomobject]@{ dependency_before = $beforeDependency; dependency_during = $duringDependency; dependency_after = $afterDependency; runtime_during = $duringProbe; runtime_after = $afterProbe }
        unresolved_work_report = [pscustomobject]@{
            before = $before.unresolved_work
            during = $during.unresolved_work
            after = $after.unresolved_work
            after_status = $after.status
        }
    }
}

try {
    $episodes = @()
    $episodes += Episode "kafka-outage" "kafka" { StopComposeService "kafka" } { StartComposeService "kafka"; & docker compose @composeArgs run --rm kafka-init | Out-Null }
    $episodes += Episode "postgres-outage" "postgres" { StopComposeService "postgres" } { StartComposeService "postgres" }
    $episodes += Episode "worker-partition" "worker-a" { StopComposeService "worker-a" } { StartComposeService "worker-a" }
    $episodes += Episode "runtime-restart" "runtime-a" {
        & docker compose @composeArgs restart runtime-a | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Could not restart runtime-a" }
    } { & docker compose @composeArgs up -d --wait runtime-a | Out-Null }

    $report = [pscustomobject]@{
        schema_version = "m5-outage-recovery.v1"
        generated_at = (Get-Date).ToUniversalTime().ToString("o")
        runtime_probe = "$RuntimeBase/v1/workflows/m5-outage-probe"
        episodes = $episodes
        unresolved_work_report = $episodes | ForEach-Object {
            [pscustomobject]@{ id = $_.id; dependency = $_.dependency; after_status = $_.unresolved_work_report.after_status; after = $_.unresolved_work_report.after }
        }
        notes = @(
            "Counts are captured separately from dependency health so recovery does not imply that obligations were resolved.",
            "The two-scheduler throughput smoke uses four disjoint partitions and is preliminary, not a lease-contention measurement."
        )
    }
    $parent = Split-Path -Parent $OutputPath
    if ($parent) { New-Item -ItemType Directory -Force $parent | Out-Null }
    $report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $OutputPath -Encoding utf8
    Write-Host "M5 outage evidence written to $OutputPath"
} finally {
    foreach ($name in @($stopped.Keys)) {
        try { StartComposeService $name } catch { Write-Warning "Could not restore ${name}: $_" }
    }
    Pop-Location
}
