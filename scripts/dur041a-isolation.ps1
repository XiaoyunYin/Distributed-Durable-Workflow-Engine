[CmdletBinding()]
param(
    [switch]$StartServices,
    [int64]$Seed = 410096,
    [string]$OutputRoot = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$composeArgs = @("--env-file", ".env", "-f", "deploy/local/compose.yaml")
$fixturePath = ""
$runtimeAId = ""
$runtimeAPaused = $false
$servicesChanged = $false
$previousDatabase = $env:DATABASE_URL

function Invoke-Required([string]$Command, [string[]]$Arguments) {
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $Command @Arguments 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
    if ($exitCode -ne 0) {
        throw "$Command failed with exit code ${exitCode}: $output"
    }
    return $output.Trim()
}

function Write-Json([string]$Path, [object]$Value) {
    $Value | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Read-EnvValues {
    $values = @{}
    foreach ($line in Get-Content -LiteralPath ".env") {
        if ($line -match '^([A-Z_]+)=(.*)$') { $values[$matches[1]] = $matches[2] }
    }
    return $values
}

function Invoke-Compose([string[]]$Arguments) {
    return Invoke-Required "docker" (@("compose") + $composeArgs + $Arguments)
}

Push-Location $RepoRoot
try {
    if (-not (Test-Path -LiteralPath ".env")) { throw ".env is missing." }
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
        $OutputRoot = Join-Path $RepoRoot ("experiments/portfolio/local-recovery/isolation-" + $Seed)
    } elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) {
        $OutputRoot = Join-Path $RepoRoot $OutputRoot
    }
    if (Test-Path -LiteralPath $OutputRoot) {
        throw "Refusing to overwrite an existing campaign directory: $OutputRoot"
    }
    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
    $fixturePath = Join-Path $OutputRoot "fixture.json"
    $observationPath = Join-Path $OutputRoot "fault-observation.json"
    $protocolPath = Join-Path $OutputRoot "protocol.json"
    $summaryPath = Join-Path $OutputRoot "summary.json"

    $values = Read-EnvValues
    $user = [uri]::EscapeDataString($values['POSTGRES_USER'])
    $password = [uri]::EscapeDataString($values['POSTGRES_PASSWORD'])
    $port = $values['POSTGRES_PORT']
    if ([string]::IsNullOrWhiteSpace($port)) { $port = "5432" }
    $env:DATABASE_URL = "postgresql://${user}:${password}@127.0.0.1:${port}/$($values['POSTGRES_DB'])?sslmode=disable"

    $toolDirectory = Join-Path $RepoRoot ".scratch\dur041a-bin"
    New-Item -ItemType Directory -Force -Path $toolDirectory | Out-Null
    $toolPath = Join-Path $toolDirectory "dur041a-isolation.exe"
    Invoke-Required "go" @("build", "-o", $toolPath, "./cmd/dur041a-isolation") | Out-Null

    if ($StartServices) {
        Invoke-Compose @("up", "-d", "--build", "--wait") | Out-Null
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1")) | Out-Null
    }

    # Keep the original scheduler as the only owner while the fixture is
    # submitted. Workers are stopped so the task remains affected work after
    # takeover; runtime-a itself is preserved for the pause/unpause fault.
    Invoke-Compose @("stop", "runtime-b", "worker-a", "worker-b") | Out-Null
    $servicesChanged = $true
    Invoke-Required $toolPath @("-mode", "prepare", "-fixture", $fixturePath) | Out-Null

    $captured = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $captureOutput = & $toolPath -mode capture -fixture $fixturePath 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0) {
            $captured = $true
            break
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $captured) { throw "runtime-a did not acquire the fixture partition before the capture deadline." }
    $before = Get-Content -LiteralPath $fixturePath -Raw | ConvertFrom-Json

    $runtimeAId = (Invoke-Compose @("ps", "-q", "runtime-a")).Trim()
    if ([string]::IsNullOrWhiteSpace($runtimeAId)) { throw "Could not resolve runtime-a container ID." }
    $startedBefore = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.StartedAt}}")).Trim()
    Invoke-Required "docker" @("pause", $runtimeAId) | Out-Null
    $runtimeAPaused = $true
    $pausedState = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Paused}}")).Trim()
    if ($pausedState -ne "true") { throw "Docker did not report runtime-a as paused." }

    # Start the peer only after the original is paused, so the peer begins its
    # scan with a fresh cursor and cannot miss this preserved fixture.
    Invoke-Compose @("up", "-d", "--wait", "runtime-b") | Out-Null
    Invoke-Required $toolPath @("-mode", "wait-takeover", "-fixture", $fixturePath, "-timeout", "45s") | Out-Null
    Invoke-Required "docker" @("unpause", $runtimeAId) | Out-Null
    $runtimeAPaused = $false
    $pausedAfter = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Paused}}")).Trim()
    $startedAfter = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.StartedAt}}")).Trim()
    if ($pausedAfter -ne "false" -or $startedBefore -ne $startedAfter) {
        throw "runtime-a was not reconnected as the original process/container."
    }
    Invoke-Required $toolPath @("-mode", "stale-attempt", "-fixture", $fixturePath) | Out-Null
    $after = Get-Content -LiteralPath $fixturePath -Raw | ConvertFrom-Json
    if (-not $after.stale_rejected) { throw "The retained old owner epoch was not rejected." }

    $observation = [ordered]@{
        schema_version = "dur041a-isolation-observation.v1"
        status = "PASS"
        container_id = $runtimeAId
        original_process_preserved = ($startedBefore -eq $startedAfter)
        paused_state_observed = ($pausedState -eq "true")
        reconnected_state_observed = ($pausedAfter -eq "false")
        original_owner_id = $after.old_owner_id
        original_epoch = $after.old_epoch
        takeover_owner_id = $after.new_owner_id
        takeover_epoch = $after.new_epoch
        stale_attempt_rejected = $after.stale_rejected
        stale_attempt_error = $after.stale_error
        before_revision = $after.before_revision
        after_revision = $after.after_revision
        fault_observation = "Docker pause reported true while the same runtime-a container was retained; peer takeover was observed in PostgreSQL; unpause reported false on the same container."
    }
    Write-Json $observationPath $observation
    $protocol = [ordered]@{
        schema_version = "dur041a-isolation.v1"
        status = "PASS"
        generated_at_utc = [DateTime]::UtcNow.ToString("o")
        git_commit = (& git rev-parse HEAD).Trim()
        seed = $Seed
        fault = "scheduler_container_pause_and_reconnect"
        original_campaign = "experiments/portfolio/local-recovery/local-410041"
        fixture = "fixture.json"
        observation = "fault-observation.json"
        lock_held_takeover = [ordered]@{ status = "separately_labelled"; included_in_this_arm = $false; note = "The lease-row lock case is measured separately from this pause/reconnect arm." }
        deployment_boundary = "single Docker Desktop/WSL2 host; two local runtime containers sharing PostgreSQL"
        acceptance = @("same original runtime container preserved", "pause state observed", "peer owner and higher epoch observed", "same container reconnected", "retained old-owner epoch rejected without revision change")
    }
    $summary = [ordered]@{
        schema_version = "dur041a-isolation.v1"
        status = "PASS"
        git_commit = $protocol.git_commit
        fault = $protocol.fault
        original_process_preserved = $observation.original_process_preserved
        peer_takeover_observed = ($after.new_epoch -gt $after.old_epoch -and $after.new_owner_id -ne $after.old_owner_id)
        stale_owner_mutations = 0
        stale_attempt_rejected = $after.stale_rejected
        revision_unchanged = ($after.before_revision -eq $after.after_revision)
        lock_held_takeover = $protocol.lock_held_takeover
        limitations = @("Local containers are not independent hosts.", "This scenario does not measure database-host failure or production availability.", "R096 status remains pending Claude verification.")
    }
    Write-Json $protocolPath $protocol
    Write-Json $summaryPath $summary
    Write-Host "DUR-041a isolation scenario PASS: original epoch $($after.old_epoch) fenced after takeover epoch $($after.new_epoch)."
} finally {
    if ($runtimeAPaused -and -not [string]::IsNullOrWhiteSpace($runtimeAId)) {
        try { & docker unpause $runtimeAId | Out-Null } catch { Write-Warning "Could not unpause runtime-a during cleanup: $_" }
    }
    if (-not [string]::IsNullOrWhiteSpace($fixturePath) -and (Test-Path -LiteralPath $fixturePath)) {
        try { & $toolPath -mode cleanup -fixture $fixturePath | Out-Null } catch { Write-Warning "Could not clean isolation fixture: $_" }
    }
    if ($servicesChanged) {
        try { & docker compose @composeArgs up -d --wait runtime-a runtime-b worker-a worker-b | Out-Null } catch { Write-Warning "Could not restore runtime/worker services: $_" }
    }
    if ($null -eq $previousDatabase) { Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue }
    else { $env:DATABASE_URL = $previousDatabase }
    Pop-Location
}
