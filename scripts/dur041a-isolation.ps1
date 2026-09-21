[CmdletBinding()]
param(
    [switch]$StartServices,
    [int64]$Seed = 410096,
    [string]$OutputRoot = "",
    [ValidateSet("pause", "network")]
    [string]$Fault = "pause"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$composeArgs = @("--env-file", ".env", "-f", "deploy/local/compose.yaml")
$fixturePath = ""
$runtimeAId = ""
$runtimeBId = ""
$runtimeAPaused = $false
$runtimeBPaused = $false
$networkDisconnected = $false
$runtimeANetwork = ""
$networkFaultObserved = $false
$servicesChanged = $false
$previousDatabase = $env:DATABASE_URL
$previousLeaseHold = $env:RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS

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

function Get-ContainerNetworkNames([string]$ContainerId) {
    $inspect = (Invoke-Required "docker" @("inspect", $ContainerId) | ConvertFrom-Json)[0]
    return @($inspect.NetworkSettings.Networks.PSObject.Properties.Name | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

Push-Location $RepoRoot
try {
    if (-not (Test-Path -LiteralPath ".env")) { throw ".env is missing." }
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
        $prefix = if ($Fault -eq "network") { "network" } else { "isolation" }
        $OutputRoot = Join-Path $RepoRoot ("experiments/portfolio/local-recovery/" + $prefix + "-" + $Seed)
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
    # This diagnostic hold is zero in normal Compose operation. It makes the
    # ownership boundary observable for this fault probe only.
    $env:RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS = "3000"

    $toolDirectory = Join-Path $RepoRoot ".scratch\dur041a-bin"
    New-Item -ItemType Directory -Force -Path $toolDirectory | Out-Null
    $toolPath = Join-Path $toolDirectory "dur041a-isolation.exe"
    Invoke-Required "go" @("build", "-o", $toolPath, "./cmd/dur041a-isolation") | Out-Null

    if ($StartServices) {
        Invoke-Compose @("up", "-d", "--build", "--wait") | Out-Null
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1")) | Out-Null
    }

    # Stop both schedulers before seeding so their scan cursors cannot miss the
    # fixture and their old lease rows can expire. The runtime-a process that
    # starts below is the same process later paused and reconnected.
    Invoke-Compose @("stop", "runtime-a", "runtime-b", "worker-a", "worker-b") | Out-Null
    $servicesChanged = $true
    Start-Sleep -Seconds 16
    Invoke-Required $toolPath @("-mode", "prepare", "-fixture", $fixturePath) | Out-Null
    Invoke-Compose @("up", "-d", "--wait", "runtime-a") | Out-Null

    $captured = $false
    for ($attempt = 0; $attempt -lt 300; $attempt++) {
        $captureOutput = & $toolPath -mode capture -fixture $fixturePath 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0) {
            $captured = $true
            break
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $captured) { throw "runtime-a did not acquire the fixture partition before the capture deadline." }
    $before = Get-Content -LiteralPath $fixturePath -Raw | ConvertFrom-Json

    $runtimeAId = (Invoke-Compose @("ps", "-q", "runtime-a")).Trim()
    if ([string]::IsNullOrWhiteSpace($runtimeAId)) { throw "Could not resolve runtime-a container ID." }
    $startedBefore = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.StartedAt}}")).Trim()
    $pidBefore = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Pid}}")).Trim()
    $pausedState = "false"
    $networkNamesBefore = @(Get-ContainerNetworkNames $runtimeAId)
    $networkNamesAfter = @($networkNamesBefore)
    $containerStateAfterFault = "unknown"
    if ($Fault -eq "pause") {
        Invoke-Required "docker" @("pause", $runtimeAId) | Out-Null
        $runtimeAPaused = $true
        $pausedState = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Paused}}")).Trim()
        if ($pausedState -ne "true") { throw "Docker did not report runtime-a as paused." }
        $containerStateAfterFault = "paused"
    } else {
        $postgresId = (Invoke-Compose @("ps", "-q", "postgres")).Trim()
        if ([string]::IsNullOrWhiteSpace($postgresId)) { throw "Could not resolve the PostgreSQL container ID." }
        $postgresNetworks = @(Get-ContainerNetworkNames $postgresId)
        $sharedNetworks = @($networkNamesBefore | Where-Object { $postgresNetworks -contains $_ })
        if ($sharedNetworks.Count -ne 1) {
            throw "Expected exactly one runtime-a/PostgreSQL network, found: $($sharedNetworks -join ', ')"
        }
        $runtimeANetwork = $sharedNetworks[0]
        Invoke-Required "docker" @("network", "disconnect", "--force", $runtimeANetwork, $runtimeAId) | Out-Null
        $networkDisconnected = $true
        $networkNamesAfter = @(Get-ContainerNetworkNames $runtimeAId)
        $containerStateAfterFault = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Status}}")).Trim()
        if ($networkNamesAfter -contains $runtimeANetwork -or $containerStateAfterFault -ne "running") {
            throw "The network fault did not isolate the running runtime-a container. networks=$($networkNamesAfter -join ',') state=$containerStateAfterFault"
        }
        $networkFaultObserved = $true
    }

    # Start the peer only after the original fault is observed, so the peer
    # begins its scan with a fresh cursor and cannot miss this fixture.
    Invoke-Compose @("up", "-d", "--wait", "runtime-b") | Out-Null
    Invoke-Required $toolPath @("-mode", "wait-takeover", "-fixture", $fixturePath, "-timeout", "45s") | Out-Null
    if ($Fault -eq "network") {
        $runtimeBId = (Invoke-Compose @("ps", "-q", "runtime-b")).Trim()
        if ([string]::IsNullOrWhiteSpace($runtimeBId)) { throw "Could not resolve runtime-b container ID." }
        Invoke-Required "docker" @("pause", $runtimeBId) | Out-Null
        $runtimeBPaused = $true
        $peerPausedState = (Invoke-Required "docker" @("inspect", $runtimeBId, "--format", "{{.State.Paused}}")).Trim()
        if ($peerPausedState -ne "true") { throw "Docker did not report runtime-b as paused for stale-write synchronization." }
    }
    if ($Fault -eq "pause") {
        Invoke-Required "docker" @("unpause", $runtimeAId) | Out-Null
        $runtimeAPaused = $false
    } else {
        Invoke-Required "docker" @("network", "connect", $runtimeANetwork, $runtimeAId) | Out-Null
        $networkDisconnected = $false
    }
    $pausedAfter = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Paused}}")).Trim()
    $startedAfter = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.StartedAt}}")).Trim()
    $pidAfter = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Pid}}")).Trim()
    $networkNamesReconnected = @(Get-ContainerNetworkNames $runtimeAId)
    if ($pausedAfter -ne "false" -or $startedBefore -ne $startedAfter -or $pidBefore -ne $pidAfter) {
        throw "runtime-a was not reconnected as the original process/container."
    }
    if ($Fault -eq "network" -and -not ($networkNamesReconnected -contains $runtimeANetwork)) {
        throw "runtime-a did not reconnect to the PostgreSQL network."
    }
    if ($Fault -eq "network") {
        Invoke-Required "docker" @("pause", $runtimeAId) | Out-Null
        $runtimeAPaused = $true
        $reconnectedPausedState = (Invoke-Required "docker" @("inspect", $runtimeAId, "--format", "{{.State.Paused}}")).Trim()
        if ($reconnectedPausedState -ne "true") { throw "Docker did not report reconnected runtime-a as paused for stale-write synchronization." }
    }
    Invoke-Required $toolPath @("-mode", "stale-attempt", "-fixture", $fixturePath) | Out-Null
    $after = Get-Content -LiteralPath $fixturePath -Raw | ConvertFrom-Json
    if (-not $after.stale_rejected) { throw "The retained old owner epoch was not rejected." }

    $faultName = if ($Fault -eq "pause") { "scheduler_container_pause_and_reconnect" } else { "scheduler_postgres_network_disconnect_and_reconnect" }
    $faultObservation = if ($Fault -eq "pause") {
        "Docker pause reported true while the same runtime-a container was retained; peer takeover was observed in PostgreSQL; unpause reported false on the same container."
    } else {
        "Docker removed runtime-a from the network shared with PostgreSQL while the container and PID remained running; peer takeover was observed in PostgreSQL; the same container and PID were reconnected to that network."
    }
    $acceptance = if ($Fault -eq "pause") {
        @("same original runtime container preserved", "pause state observed", "peer owner and higher epoch observed", "same container reconnected", "retained old-owner epoch rejected without revision change")
    } else {
        @("same original runtime container and PID preserved", "runtime-a removed from the PostgreSQL network while still running", "peer owner and higher epoch observed", "same container reconnected to the PostgreSQL network", "retained old-owner epoch rejected without revision change")
    }
    $observationSynchronization = if ($Fault -eq "pause") {
        "runtime-a used a zero-default diagnostic 3000ms hold after lease acquisition so Docker pause could be applied to an observed owner; the hold is excluded from takeover and recovery timing."
    } else {
        "runtime-a used a zero-default diagnostic 3000ms hold after lease acquisition so its PostgreSQL network could be disconnected from an observed owner; the hold is excluded from takeover and recovery timing."
    }
    $lockHeldNote = if ($Fault -eq "pause") {
        "The lease-row lock case is measured separately from this pause/reconnect arm."
    } else {
        "The lease-row lock case is measured separately from this network-disconnect/reconnect arm."
    }
    $armLimitation = if ($Fault -eq "pause") {
        "The pause arm is a container pause/resume fault, not an independent-host or database-host failure."
    } else {
        "The network arm is a container-network fault, not an independent-host or database-host failure."
    }

    $observation = [ordered]@{
        schema_version = "dur041a-isolation-observation.v1"
        status = "PASS"
        container_id = $runtimeAId
        original_process_preserved = ($startedBefore -eq $startedAfter)
        fault_arm = $Fault
        paused_state_observed = ($pausedState -eq "true")
        network_disconnect_observed = $networkFaultObserved
        postgres_network = $runtimeANetwork
        network_names_before_fault = $networkNamesBefore
        network_names_after_fault = $networkNamesAfter
        network_names_after_reconnect = $networkNamesReconnected
        container_state_after_fault = $containerStateAfterFault
        reconnected_state_observed = ($pausedAfter -eq "false")
        original_container_pid = $pidBefore
        reconnected_container_pid = $pidAfter
        original_owner_id = $after.old_owner_id
        original_epoch = $after.old_epoch
        takeover_owner_id = $after.new_owner_id
        takeover_epoch = $after.new_epoch
        stale_attempt_rejected = $after.stale_rejected
        stale_attempt_error = $after.stale_error
        before_revision = $after.before_revision
        after_revision = $after.after_revision
        fault_observation = $faultObservation
    }
    Write-Json $observationPath $observation
    $protocol = [ordered]@{
        schema_version = "dur041a-isolation.v1"
        status = "PASS"
        generated_at_utc = [DateTime]::UtcNow.ToString("o")
        git_commit = (& git rev-parse HEAD).Trim()
        seed = $Seed
        fault = $faultName
        fault_arm = $Fault
        original_campaign = "experiments/portfolio/local-recovery/local-410041"
        fixture = "fixture.json"
        observation = "fault-observation.json"
        lock_held_takeover = [ordered]@{ status = "separately_labelled"; included_in_this_arm = $false; note = $lockHeldNote }
        observation_synchronization = $observationSynchronization
        deployment_boundary = "single Docker Desktop/WSL2 host; two local runtime containers sharing PostgreSQL"
        acceptance = $acceptance
    }
    $summary = [ordered]@{
        schema_version = "dur041a-isolation.v1"
        status = "PASS"
        git_commit = $protocol.git_commit
        fault = $protocol.fault
        fault_arm = $Fault
        original_process_preserved = $observation.original_process_preserved
        peer_takeover_observed = ($after.new_epoch -gt $after.old_epoch -and $after.new_owner_id -ne $after.old_owner_id)
        stale_owner_mutations = 0
        stale_attempt_rejected = $after.stale_rejected
        revision_unchanged = ($after.before_revision -eq $after.after_revision)
        lock_held_takeover = $protocol.lock_held_takeover
        observation_synchronization = $protocol.observation_synchronization
        limitations = @("Local containers are not independent hosts.", "This scenario does not measure database-host failure or production availability.", "The diagnostic observation hold is not a recovery-time measurement.", $armLimitation)
    }
    Write-Json $protocolPath $protocol
    Write-Json $summaryPath $summary
    Write-Host "DUR-041a isolation scenario PASS: original epoch $($after.old_epoch) fenced after takeover epoch $($after.new_epoch)."
} finally {
    if ($runtimeBPaused -and -not [string]::IsNullOrWhiteSpace($runtimeBId)) {
        try { & docker unpause $runtimeBId | Out-Null } catch { Write-Warning "Could not unpause runtime-b during cleanup: $_" }
    }
    if ($runtimeAPaused -and -not [string]::IsNullOrWhiteSpace($runtimeAId)) {
        try { & docker unpause $runtimeAId | Out-Null } catch { Write-Warning "Could not unpause runtime-a during cleanup: $_" }
    }
    if (-not [string]::IsNullOrWhiteSpace($fixturePath) -and (Test-Path -LiteralPath $fixturePath)) {
        try { & $toolPath -mode cleanup -fixture $fixturePath | Out-Null } catch { Write-Warning "Could not clean isolation fixture: $_" }
    }
    if ($null -eq $previousLeaseHold) {
        $env:RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS = "0"
    } else {
        $env:RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS = $previousLeaseHold
    }
    if ($servicesChanged) {
        try { & docker compose @composeArgs up -d --wait runtime-a runtime-b worker-a worker-b | Out-Null } catch { Write-Warning "Could not restore runtime/worker services: $_" }
    }
    if ($null -eq $previousLeaseHold) { Remove-Item Env:RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS -ErrorAction SilentlyContinue }
    if ($null -eq $previousDatabase) { Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue }
    else { $env:DATABASE_URL = $previousDatabase }
    Pop-Location
}
