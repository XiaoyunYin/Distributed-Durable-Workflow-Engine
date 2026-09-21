[CmdletBinding()]
param(
    [ValidateSet("pilot", "normal", "recovery")]
    [string]$Mode = "pilot",
    [string]$OutputRoot = "experiments/m8/dur042-pilot"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$composeArgs = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
$servicesChanged = $false
$previousDatabase = $env:DATABASE_URL
$previousDelay = $env:DUR048_ACTIVITY_DELAY_MS
$previousFixture = $env:DUR048_ALLOW_FIXTURE_ACTIVITY
$recoveryProcess = $null

function Invoke-Required([string]$Command, [string[]]$Arguments) {
    $previousErrorAction = $ErrorActionPreference
    try {
        # Compose progress is written to stderr even for successful commands;
        # inspect the native exit code instead of treating that progress as a
        # PowerShell terminating error.
        $ErrorActionPreference = "Continue"
        $output = & $Command @Arguments 2>&1 | Out-String
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
    if ($LASTEXITCODE -ne 0) { throw "$Command failed ($LASTEXITCODE): $output" }
    return $output.Trim()
}

function Invoke-Compose([string[]]$Arguments) {
    return Invoke-Required "docker" ($composeArgs + $Arguments)
}

function Reset-TraceBackend {
    $collectorID = (Invoke-Compose @("ps", "-a", "-q", "otel-collector")).Trim()
    if ([string]::IsNullOrWhiteSpace($collectorID)) { throw "otel collector container was not found" }
    $mounts = (Invoke-Required "docker" @("inspect", "-f", '{{range .Mounts}}{{.Name}}|{{.Destination}};{{end}}', $collectorID)).Trim()
    $traceVolume = (($mounts -split ';' | Where-Object { $_ -match '\|/var/lib/otel$' } | Select-Object -First 1) -split '\|')[0]
    if ([string]::IsNullOrWhiteSpace($traceVolume)) { throw "otel trace volume was not found" }
    # The volume contains only this campaign's bounded local trace backend.
    # Recreate it instead of relying on exporter append/truncate defaults.
    Invoke-Required "docker" @("rm", "-f", $collectorID) | Out-Null
    Invoke-Required "docker" @("volume", "rm", $traceVolume) | Out-Null
    Invoke-Compose @("up", "-d", "--force-recreate", "--wait", "otel-collector") | Out-Null
}

function Read-EnvFile {
    $values = @{}
    foreach ($line in Get-Content -LiteralPath ".env") {
        if ($line -match '^([A-Z_]+)=(.*)$') { $values[$matches[1]] = $matches[2] }
    }
    return $values
}

Push-Location $RepoRoot
try {
    if (-not (Test-Path -LiteralPath ".env")) { throw ".env is required; run bootstrap first." }
    if ([System.IO.Path]::IsPathRooted($OutputRoot) -eq $false) { $OutputRoot = Join-Path $RepoRoot $OutputRoot }
    if (Test-Path -LiteralPath $OutputRoot) { throw "Refusing to overwrite existing evidence: $OutputRoot" }
    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

    $values = Read-EnvFile
    $dbUser = [uri]::EscapeDataString($values["POSTGRES_USER"])
    $dbPassword = [uri]::EscapeDataString($values["POSTGRES_PASSWORD"])
    $dbName = [uri]::EscapeDataString($values["POSTGRES_DB"])
    $dbPort = if ($values["POSTGRES_PORT"]) { $values["POSTGRES_PORT"] } else { "5432" }
    $runtimePort = if ($values["RUNTIME_A_PORT"]) { $values["RUNTIME_A_PORT"] } else { "8080" }
    $env:DATABASE_URL = "postgresql://${dbUser}:${dbPassword}@127.0.0.1:${dbPort}/${dbName}?sslmode=disable"
    $env:DURABLE_SOURCE_COMMIT = (git rev-parse HEAD).Trim()
    $env:DUR048_ACTIVITY_DELAY_MS = "0"
    $env:DUR048_ALLOW_FIXTURE_ACTIVITY = "0"

    $binary = Join-Path $RepoRoot "bin/dur048-load.exe"
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $binary) | Out-Null
    Invoke-Required "go" @("build", "-o", $binary, "./cmd/dur048-load") | Out-Null
    Reset-TraceBackend

    if ($Mode -eq "normal" -or $Mode -eq "pilot") {
        Invoke-Required $binary @("-mode", "normal", "-url", "http://127.0.0.1:${runtimePort}", "-output", (Join-Path $OutputRoot "normal"), "-namespace", "local-runtime", "-rate", "1", "-count", "4", "-wait", "45s", "-worker-slots", "4") | Out-Null
    }

    if ($Mode -eq "recovery" -or $Mode -eq "pilot") {
        # Recreate the fixed four-slot worker capacity with the opt-in delay.
        # Stop the second worker so the preserved first process is the only
        # consumer before the deliberate process-tree kill.
        $env:DUR048_ACTIVITY_DELAY_MS = "5000"
        $env:DUR048_ALLOW_FIXTURE_ACTIVITY = "1"
        $servicesChanged = $true
        Invoke-Compose @("up", "-d", "--force-recreate", "runtime-a", "runtime-b", "worker-a", "worker-b") | Out-Null
        Invoke-Compose @("stop", "worker-b") | Out-Null
        # Allow the stopped worker's Kafka group members to expire and verify
        # from Kafka's group assignment that the preserved worker owns every
        # task partition before the harness submits the delayed activity.
        $assignmentDeadline = (Get-Date).AddSeconds(60)
        $assignedPartitions = @()
        while ((Get-Date) -lt $assignmentDeadline) {
            $groupOutput = Invoke-Compose @("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--describe", "--group", "runtime-workers-v1")
            $assignedPartitions = @($groupOutput -split "`r?`n" | Where-Object { $_ -match "durable-agent\.tasks\.v1" -and $_ -match "worker-a-" })
            if ($assignedPartitions.Count -eq 8) { break }
            Start-Sleep -Seconds 1
        }
        if ($assignedPartitions.Count -ne 8) { throw "worker-a did not own all eight task partitions after worker-b stopped" }
        $recoveryOutput = Join-Path $OutputRoot "recovery"
        $recoveryProcess = Start-Process -FilePath $binary -ArgumentList @(
            "-mode", "recovery", "-url", "http://127.0.0.1:${runtimePort}", "-output", $recoveryOutput,
            "-namespace", "local-runtime", "-rate", "1", "-count", "1", "-wait", "150s", "-worker-slots", "4", "-activity", "dur048.sleep"
        ) -PassThru -NoNewWindow
        # Do not infer that the fault reached the intended boundary from the
        # controller's timing. Observe the durable attempt becoming CLAIMED
        # before killing the preserved worker process.
        $claimDeadline = (Get-Date).AddSeconds(30)
        $claimState = ""
        while ((Get-Date) -lt $claimDeadline) {
            $claimQuery = "SELECT COALESCE((SELECT a.state FROM engine.activity_attempts a WHERE a.workflow_id = (SELECT workflow_id FROM engine.workflow_executions WHERE namespace = 'local-runtime' AND workflow_id LIKE 'dur048-recovery-%' ORDER BY created_at DESC LIMIT 1) ORDER BY a.updated_at DESC LIMIT 1), 'MISSING');"
            $claimState = (Invoke-Compose @("exec", "-T", "postgres", "psql", "-U", $values["POSTGRES_USER"], "-d", $values["POSTGRES_DB"], "-At", "-c", $claimQuery)).Trim()
            if ($claimState -eq "CLAIMED") { break }
            Start-Sleep -Milliseconds 500
        }
        if ($claimState -ne "CLAIMED") { throw "recovery fault boundary was not observed; latest attempt state: $claimState" }
        Invoke-Compose @("kill", "worker-a") | Out-Null
        Invoke-Compose @("stop", "worker-a") | Out-Null
        $workerAID = (Invoke-Compose @("ps", "-a", "-q", "worker-a")).Trim()
        if ([string]::IsNullOrWhiteSpace($workerAID)) { throw "recovery target container disappeared after kill" }
        $workerAState = (Invoke-Required "docker" @("inspect", "-f", "{{.State.Running}}", $workerAID)).Trim()
        if ($workerAState -ne "false") { throw "recovery target was not observed dead after kill: $workerAState" }
        # The delayed worker is stopped; no new consumer can hide the
        # replacement attempt until the fault is durably observable.
        Invoke-Compose @("start", "worker-b") | Out-Null
        Invoke-Compose @("up", "-d", "--wait", "worker-b") | Out-Null
        $recoveryProcess.WaitForExit()
        $recoveryProcess.Refresh()
        $recoveryStatus = (Get-Content -LiteralPath (Join-Path $recoveryOutput "recovery.json") | ConvertFrom-Json).status
        if ($recoveryStatus -ne "PASS") { throw "recovery harness artifact status was $recoveryStatus" }
        if ($null -ne $recoveryProcess.ExitCode -and $recoveryProcess.ExitCode -ne 0) {
            throw "recovery harness failed with exit code $($recoveryProcess.ExitCode)"
        }
    }

    # The collector is the trace backend. Copy its JSON export beside the raw
    # reconciliation rows so the trace can be reviewed offline.
    Start-Sleep -Seconds 3
    Invoke-Compose @("cp", "otel-collector:/var/lib/otel/traces.jsonl", (Join-Path $OutputRoot "traces.jsonl")) | Out-Null
    $protocol = [ordered]@{
        schema = "dur048-deployed-pilot.v1"
        status = "PASS"
        source_commit = $env:DURABLE_SOURCE_COMMIT
        modes = @("normal", "recovery")
        worker_slots = 4
        transport = "HTTP submission -> PostgreSQL -> outbox -> Kafka -> worker -> receipt"
        trace_backend = "OpenTelemetry Collector file exporter"
        effect_path = "not_exercised"
        effect_span_scope = "pure pilot activities do not reach the approved effect service; no effect-span claim is made"
        recovery_fault = "worker-a process kill after confirmed delayed activity dispatch"
        fault_confirmation = "dur048.sleep attempt was observed CLAIMED in PostgreSQL before worker-a was killed and stopped; worker-b resumed the durable replacement"
        evidence = @("normal/normal.json", "recovery/recovery.json", "traces.jsonl")
        limitations = @(
            "local Compose pilot only; not AWS or production throughput",
            "recovery is an application-process kill, not a host or database failure",
            "durations use client UTC plus durable terminal state; no cross-host clock claim"
        )
    }
    $protocol | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $OutputRoot "protocol.json") -Encoding UTF8
} finally {
    if ($null -ne $recoveryProcess -and -not $recoveryProcess.HasExited) {
        try { Stop-Process -Id $recoveryProcess.Id -Force } catch { Write-Warning "Could not stop recovery harness: $_" }
    }
    $env:DATABASE_URL = $previousDatabase
    if ($null -eq $previousDelay) { Remove-Item Env:DUR048_ACTIVITY_DELAY_MS -ErrorAction SilentlyContinue } else { $env:DUR048_ACTIVITY_DELAY_MS = $previousDelay }
    if ($null -eq $previousFixture) { Remove-Item Env:DUR048_ALLOW_FIXTURE_ACTIVITY -ErrorAction SilentlyContinue } else { $env:DUR048_ALLOW_FIXTURE_ACTIVITY = $previousFixture }
    if ($servicesChanged) { try { Invoke-Compose @("up", "-d", "--force-recreate", "runtime-a", "runtime-b", "worker-a", "worker-b") | Out-Null } catch { Write-Warning "Could not restore worker services: $_" } }
    Pop-Location
}
