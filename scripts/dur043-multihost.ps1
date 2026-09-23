[CmdletBinding()]
param(
    [ValidateSet("all", "network", "host")]
    [string]$Scenario = "all",
    [string]$NetworkWorkflowId = "",
    [string]$HostWorkflowId = "",
    [string]$AwsProfile = $(if ($env:AWS_PROFILE) { $env:AWS_PROFILE } else { "portfolio-dev" }),
    [string]$Region = "us-west-1",
    [string]$TerraformDir = "deploy/aws",
    [string]$OutputRoot = "",
    [switch]$SelfTest
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$AppCompose = "/opt/durable-agent-execution-engine/deploy/aws/app-compose.yaml"
$DependencyCompose = "/opt/durable-agent-execution-engine/deploy/aws/dependency-compose.yaml"
$RemoteEnv = "/opt/durable-agent-execution-engine/deploy/aws/.env"
$DependencyUser = "durable"
$DependencyDatabase = "durable"
$Results = @()
$StartedApp1 = $false
$StartedApp2 = $false
$NetworkRulesInserted = $false
$App1WasStopped = $false
# Cross-host SSM observations can take several seconds. Keep the activity
# claimed long enough to observe the durable pre-fault boundary before the
# network or host fault is injected.
$FixtureDelayMS = 60000
# Keep the observation hold below the runtime scheduler's fixed 5s iteration
# deadline.  A hold equal to that deadline makes every fixture pass expire
# before the scheduler can schedule the activity, producing a false liveness
# failure instead of a claimed attempt.
$ObservationHoldMS = 1000
# Hold the takeover owner briefly after acquiring the lease so the controller
# can observe the durable takeover before the replacement attempt is created.
# This separates observation latency from useful-progress latency.
$TakeoverObservationHoldMS = 1000
$FixtureActivityEnabled = $true
$WorkerSlots = 2
$FaultPorts = @(5432, 9092)

function Invoke-Required([string]$Command, [string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $Command @Arguments 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0) { throw "$Command failed with exit code ${exitCode}: $output" }
    return $output.Trim()
}

function Invoke-Aws([string[]]$Arguments) {
    return Invoke-Required "aws" (@("--profile", $AwsProfile, "--region", $Region) + $Arguments)
}

function Write-Json([string]$Path, [object]$Value) {
    # Windows PowerShell's Set-Content -Encoding UTF8 writes a BOM. AWS CLI
    # rejects that BOM when the file is supplied through --cli-input-json.
    $json = $Value | ConvertTo-Json -Depth 16
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $json, $utf8NoBom)
}

function Resolve-Terraform {
    if ($env:TERRAFORM_EXE -and (Test-Path -LiteralPath $env:TERRAFORM_EXE)) { return $env:TERRAFORM_EXE }
    $command = Get-Command terraform -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    $candidate = Join-Path $RepoRoot ".tools/terraform-1.16.3/terraform.exe"
    if (Test-Path -LiteralPath $candidate) { return $candidate }
    throw "Terraform is required; set TERRAFORM_EXE or put terraform on PATH."
}

function Get-TerraformOutputs {
    $terraform = Resolve-Terraform
    $raw = Invoke-Required $terraform @("-chdir=$TerraformDir", "output", "-json")
    return $raw | ConvertFrom-Json
}

function Get-OutputValue([object]$Outputs, [string]$Name) {
    $property = $Outputs.PSObject.Properties[$Name]
    if ($null -eq $property) { throw "Terraform output '$Name' is missing." }
    return $property.Value.value
}

function Send-Ssm([string]$InstanceId, [string[]]$Commands, [int]$TimeoutSeconds = 120) {
    $requestPath = Join-Path $RepoRoot (".scratch/dur043-ssm-" + [guid]::NewGuid().ToString("N") + ".json")
    $request = [ordered]@{
        DocumentName = "AWS-RunShellScript"
        InstanceIds = @($InstanceId)
        Comment = "DUR-043 verified multi-host recovery probe"
        TimeoutSeconds = $TimeoutSeconds
        Parameters = [ordered]@{ commands = $Commands }
    }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $requestPath) | Out-Null
    Write-Json $requestPath $request
    try {
        $commandID = (Invoke-Aws @("ssm", "send-command", "--cli-input-json", "file://$requestPath", "--query", "Command.CommandId", "--output", "text")).Trim()
        if ([string]::IsNullOrWhiteSpace($commandID)) { throw "SSM returned no command ID for $InstanceId." }
        $deadline = (Get-Date).AddSeconds($TimeoutSeconds + 30)
        do {
            Start-Sleep -Seconds 2
            $invocation = Invoke-Aws @("ssm", "get-command-invocation", "--command-id", $commandID, "--instance-id", $InstanceId, "--output", "json") | ConvertFrom-Json
            if ($invocation.Status -in @("Success", "Failed", "TimedOut", "Canceled", "Undeliverable")) {
                if ($invocation.Status -ne "Success") {
                    throw "SSM command $commandID on $InstanceId ended $($invocation.Status): $($invocation.StandardErrorContent) $($invocation.StandardOutputContent)"
                }
                return [string]$invocation.StandardOutputContent
            }
        } while ((Get-Date) -lt $deadline)
        throw "SSM command $commandID on $InstanceId did not complete before the deadline."
    } finally {
        Remove-Item -LiteralPath $requestPath -Force -ErrorAction SilentlyContinue
    }
}

function Get-SsmState([string]$InstanceId) {
    $info = Invoke-Aws @("ssm", "describe-instance-information", "--filters", "Key=InstanceIds,Values=$InstanceId", "--query", "InstanceInformationList[0].PingStatus", "--output", "text")
    return $info.Trim()
}

function Wait-Ssm([string]$InstanceId) {
    $deadline = (Get-Date).AddMinutes(5)
    do {
        if ((Get-SsmState $InstanceId) -eq "Online") { return }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "SSM target $InstanceId did not become Online."
}

function Invoke-DbSql([string]$DependencyInstanceId, [string]$Sql) {
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Sql))
    $command = "echo '$encoded' | base64 -d | docker compose --env-file '$RemoteEnv' -f '$DependencyCompose' exec -T postgres psql -U '$DependencyUser' -d '$DependencyDatabase' -At -F '|' -f -"
    return (Send-Ssm $DependencyInstanceId @("set -e", $command)).Trim()
}

function Get-Lease([string]$DependencyInstanceId, [int]$PartitionID) {
    $row = Invoke-DbSql $DependencyInstanceId "SELECT COALESCE(owner_id::text,''), epoch, COALESCE(lease_expires_at::text,''), updated_at::text FROM engine.partition_leases WHERE partition_id=$PartitionID;"
    $parts = $row -split '\|', 4
    if ($parts.Count -lt 4) { throw "Could not read partition lease ${PartitionID}: $row" }
    return [ordered]@{ owner_id = $parts[0]; epoch = [int64]$parts[1]; lease_expires_at = $parts[2]; updated_at = $parts[3] }
}

function Get-Workflow([string]$DependencyInstanceId, [string]$WorkflowID) {
    $sql = "SELECT state, revision, COALESCE((SELECT state FROM engine.activity_attempts a WHERE a.workflow_id=w.workflow_id ORDER BY attempt_number DESC LIMIT 1),'NONE'), COALESCE((SELECT attempt_number FROM engine.activity_attempts a WHERE a.workflow_id=w.workflow_id ORDER BY attempt_number DESC LIMIT 1),0), COALESCE((SELECT created_at::text FROM engine.activity_attempts a WHERE a.workflow_id=w.workflow_id ORDER BY attempt_number DESC LIMIT 1),''), updated_at::text FROM engine.workflow_executions w WHERE workflow_id='$WorkflowID';"
    $row = Invoke-DbSql $DependencyInstanceId $sql
    $parts = $row -split '\|', 6
    if ($parts.Count -lt 6) { throw "Workflow $WorkflowID was not found in PostgreSQL." }
    return [ordered]@{ state = $parts[0]; revision = [int64]$parts[1]; attempt_state = $parts[2]; attempt_number = [int64]$parts[3]; attempt_created_at = $parts[4]; updated_at = $parts[5] }
}

function Get-PartitionID([string]$WorkflowID) {
    if ([string]::IsNullOrWhiteSpace($WorkflowID)) { throw "Workflow IDs must be supplied for the fault campaign." }
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $digest = $sha256.ComputeHash([Text.Encoding]::UTF8.GetBytes($WorkflowID))
    } finally {
        $sha256.Dispose()
    }
    [uint64]$value = 0
    for ($index = 0; $index -lt 8; $index++) { $value = ($value -shl 8) -bor [uint64]$digest[$index] }
    return [int]($value % 16)
}

function Wait-FirstUsefulProgress([string]$DependencyInstanceId, [string]$WorkflowID, [int64]$PreviousAttemptNumber, [int]$TimeoutSeconds = 150) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        if ($workflow.attempt_number -gt $PreviousAttemptNumber) {
            return [ordered]@{ workflow = $workflow; observed_at_utc = ([DateTimeOffset]::Parse($workflow.attempt_created_at)).UtcDateTime }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID did not commit a replacement attempt after the fault."
}

function Wait-FirstUsefulRecoveryProgress([string]$DependencyInstanceId, [string]$WorkflowID, [int64]$PreviousAttemptNumber, [int64]$OldEpoch, [int]$TimeoutSeconds = 150) {
    $sql = "SELECT scheduler_epoch, reason, COALESCE(attempt_number,0), created_at::text FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND reason='TIMEOUT_REPLACEMENT' ORDER BY revision LIMIT 1;"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $row = Invoke-DbSql $DependencyInstanceId $sql
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        $parts = $row.Trim() -split '\|', 4
        if ($parts.Count -eq 4 -and [int64]$parts[0] -gt $OldEpoch -and [int64]$parts[2] -ge $PreviousAttemptNumber) {
            $progressAt = ([DateTimeOffset]::Parse($parts[3])).UtcDateTime
            return [ordered]@{ workflow = $workflow; observed_at_utc = $progressAt; observed_after_takeover = $true; transition = [ordered]@{ scheduler_epoch = [int64]$parts[0]; reason = $parts[1]; attempt_number = [int64]$parts[2]; created_at_utc = $progressAt.ToString("o") } }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID did not commit a durable TIMEOUT_REPLACEMENT transition after takeover."
}

function Wait-ClaimedWorkflow([string]$DependencyInstanceId, [string]$WorkflowID, [int]$PartitionID, [int]$TimeoutSeconds = 45) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        $lease = Get-Lease $DependencyInstanceId $PartitionID
        if ($workflow.attempt_state -eq "CLAIMED" -and -not [string]::IsNullOrWhiteSpace($lease.owner_id)) {
            return [ordered]@{ workflow = $workflow; lease = $lease }
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID was not observed CLAIMED with an owning partition lease before the deadline."
}

function Wait-LeaseTakeover([string]$DependencyInstanceId, [int]$PartitionID, [string]$OldOwnerID, [int64]$OldEpoch, [int]$TimeoutSeconds = 90) {
    $sql = "SELECT owner_id::text, epoch, lease_expires_at::text, updated_at::text FROM engine.partition_leases WHERE partition_id=$PartitionID;"
    $command = 'set -e; for i in $(seq 1 {0}); do row=$(docker compose --env-file ''{1}'' -f ''{2}'' exec -T postgres psql -U ''{3}'' -d ''{4}'' -At -F ''|'' -c ''{5}''); epoch=$(echo "$row" | cut -d''|'' -f2); owner=$(echo "$row" | cut -d''|'' -f1); if [ "$epoch" -gt ''{6}'' ] && [ "$owner" != ''{7}'' ]; then echo "$row|$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"; exit 0; fi; sleep 1; done; exit 1' -f $TimeoutSeconds, $RemoteEnv, $DependencyCompose, $DependencyUser, $DependencyDatabase, $sql, $OldEpoch, $OldOwnerID
    $output = Send-Ssm $DependencyInstanceId @($command) ($TimeoutSeconds + 30)
    $parts = $output.Trim() -split '\|', 5
    if ($parts.Count -lt 5) { throw "Lease takeover observer returned malformed output: $output" }
    return [ordered]@{
        lease = [ordered]@{ owner_id = $parts[0]; epoch = [int64]$parts[1]; lease_expires_at = $parts[2]; updated_at = $parts[3] }
        observed_at_utc = ([DateTimeOffset]::Parse($parts[4])).UtcDateTime
    }
}

function DurationMilliseconds([DateTime]$Started, [DateTime]$Finished) {
    return [math]::Round(($Finished - $Started).TotalMilliseconds, 3)
}

function Wait-Workflow([string]$DependencyInstanceId, [string]$WorkflowID, [int]$TimeoutSeconds = 120) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        if ($workflow.state -in @("SUCCEEDED", "FAILED", "RECONCILIATION_REQUIRED", "CANCELED")) { return $workflow }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID did not reach a terminal state before the deadline."
}

function Set-AppEnvironment([string]$InstanceId, [int]$DelayMS, [int]$HoldMS) {
    $commands = @(
        "set -e",
        "cd /opt/durable-agent-execution-engine",
        "grep -q '^DUR048_ACTIVITY_DELAY_MS=' deploy/aws/.env && sed -i 's/^DUR048_ACTIVITY_DELAY_MS=.*/DUR048_ACTIVITY_DELAY_MS=$DelayMS/' deploy/aws/.env || echo 'DUR048_ACTIVITY_DELAY_MS=$DelayMS' >> deploy/aws/.env",
        "grep -q '^DUR048_ALLOW_FIXTURE_ACTIVITY=' deploy/aws/.env && sed -i 's/^DUR048_ALLOW_FIXTURE_ACTIVITY=.*/DUR048_ALLOW_FIXTURE_ACTIVITY=1/' deploy/aws/.env || echo 'DUR048_ALLOW_FIXTURE_ACTIVITY=1' >> deploy/aws/.env",
        "grep -q '^RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS=' deploy/aws/.env && sed -i 's/^RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS=.*/RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS=$HoldMS/' deploy/aws/.env || echo 'RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS=$HoldMS' >> deploy/aws/.env"
    )
    Send-Ssm $InstanceId $commands 300 | Out-Null
}

function Set-AppMode([string]$InstanceId, [int]$DelayMS, [int]$HoldMS) {
    Set-AppEnvironment $InstanceId $DelayMS $HoldMS
    Start-AppRuntime $InstanceId
}

function Get-AppConfiguration([string]$InstanceId) {
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'cd /opt/durable-agent-execution-engine',
        "grep -E '^(DUR048_ACTIVITY_DELAY_MS|DUR048_ALLOW_FIXTURE_ACTIVITY|RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS|WORKER_SLOTS)=' deploy/aws/.env"
    )
    $values = @{}
    foreach ($line in ($output.Trim() -split "\r?\n")) {
        if ($line -match '^(?<name>[A-Z0-9_]+)=(?<value>.*)$') { $values[$Matches.name] = $Matches.value }
    }
    foreach ($name in @('DUR048_ACTIVITY_DELAY_MS', 'DUR048_ALLOW_FIXTURE_ACTIVITY', 'RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS', 'WORKER_SLOTS')) {
        if (-not $values.ContainsKey($name)) { throw "Remote app configuration is missing $name." }
    }
    return [ordered]@{
        activity_delay_ms = [int]$values['DUR048_ACTIVITY_DELAY_MS']
        fixture_activity_enabled = ([int]$values['DUR048_ALLOW_FIXTURE_ACTIVITY']) -eq 1
        scheduler_hold_after_acquire_ms = [int]$values['RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS']
        worker_slots = [int]$values['WORKER_SLOTS']
        runtime_engine_mode = "disabled"
    }
}

function Stop-AppRuntime([string]$InstanceId) {
    Send-Ssm $InstanceId @("set -e", "cd /opt/durable-agent-execution-engine", "docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml stop runtime worker") 120 | Out-Null
}

function Start-AppRuntime([string]$InstanceId) {
    Send-Ssm $InstanceId @("set -e", "cd /opt/durable-agent-execution-engine", "docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml up -d --wait runtime worker") 300 | Out-Null
}

function Start-AppRuntimeNoWait([string]$InstanceId) {
    Send-Ssm $InstanceId @("set -e", "cd /opt/durable-agent-execution-engine", "docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml up -d runtime worker") 120 | Out-Null
}

function Observe-Runtime([string]$InstanceId) {
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'cd /opt/durable-agent-execution-engine',
        'cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -q runtime)',
        'test -n "$cid"',
        'docker inspect --format ''{{.State.Status}}|{{.State.Running}}|{{.State.Pid}}'' "$cid"'
    )
    return $output.Trim()
}

function Insert-NetworkBlock([string]$AppInstanceId, [string]$DependencyInstanceId, [string]$DependencyIP, [string]$AppPrivateIP) {
    $faultCommandAt = [DateTime]::UtcNow
    $terminateSql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE client_addr='$AppPrivateIP' AND pid <> pg_backend_pid();"
    $terminateSqlEncoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($terminateSql))
    $probe = "import socket; s=socket.create_connection(('$DependencyIP', 5432), 2); s.close(); print('reachable')"
    $workerProbe = "docker compose --env-file '$RemoteEnv' -f '$AppCompose' exec -T worker python -c `"$probe`""
    $appBeforeOutput = Send-Ssm $AppInstanceId @(
        "set -e",
        "worker_cid=`$(docker compose --env-file '$RemoteEnv' -f '$AppCompose' ps -q worker)",
        'test -n "$worker_cid"',
        "printf 'connectivity_before='",
        $workerProbe,
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        'worker_pid=$(docker inspect --format "{{.State.Pid}}" "$worker_cid")',
        'test "$worker_pid" -gt 0',
        "command -v conntrack",
        "sudo ip route replace blackhole '$DependencyIP/32'",
        "cid=`$(docker compose --env-file '$RemoteEnv' -f '$AppCompose' ps -q runtime)",
        'test -n "$cid"',
        "printf 'container_state='",
        'docker inspect --format ''{{.State.Status}}|{{.State.Running}}|{{.State.Pid}}'' "$cid"',
        'test "$(docker inspect --format ''{{.State.Running}}'' "$cid")" = "true"',
        "sudo conntrack -D -d '$DependencyIP' || true",
        "ip route show '$DependencyIP/32'"
    )
    $dependencyOutput = Send-Ssm $DependencyInstanceId @(
        "set -e",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 5432 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 5432 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 9092 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 9092 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        "command -v conntrack",
        "sudo conntrack -D -s '$AppPrivateIP' || true",
        "echo '$terminateSqlEncoded' | base64 -d | docker compose --env-file '$RemoteEnv' -f '$DependencyCompose' exec -T postgres psql -U '$DependencyUser' -d '$DependencyDatabase' -At -f -"
    )
    $appAfterOutput = Send-Ssm $AppInstanceId @(
        "set -e",
        "if $workerProbe >/tmp/dur043-connectivity-after.log 2>&1; then echo 'connectivity_after=reachable'; exit 1; else echo 'connectivity_after=blocked'; fi"
    )
    $output = "$appBeforeOutput`n$dependencyOutput`n$appAfterOutput"
    $faultObservedAt = [DateTime]::UtcNow
    if ($output -notmatch "connectivity_before=reachable") { throw "Network fault precondition was not observed from the worker container: $output" }
    if ($output -notmatch "connectivity_after=blocked") { throw "Network fault was not observed from the worker container: $output" }
    if ($output -notmatch "container_state=running\|true\|") { throw "Network fault was not observed with the runtime container still running: $output" }
    return [ordered]@{
        raw = $output.Trim()
        command_at_utc = $faultCommandAt
        observed_at_utc = $faultObservedAt
        network_chain = "DOCKER-USER plus dependency INPUT"
        network_match_states = @("NEW", "ESTABLISHED", "RELATED")
        established_flow_termination = "host conntrack deletion plus dependency-side PostgreSQL session termination"
        dependency_session_termination = "pg_terminate_backend for sessions whose client address is the isolated app host"
        route_fault = "app-host blackhole route for dependency /32"
        dependency_source_ip = $AppPrivateIP
        connectivity_before = "reachable"
        connectivity_after = "blocked"
        ports = $FaultPorts
    }
}

function Remove-NetworkBlock([string]$AppInstanceId, [string]$DependencyInstanceId, [string]$DependencyIP, [string]$AppPrivateIP) {
    Send-Ssm $AppInstanceId @(
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset || true"
        "sudo ip route del blackhole '$DependencyIP/32' || true"
    ) 120 | Out-Null
    Send-Ssm $DependencyInstanceId @(
        "sudo iptables -D INPUT -s '$AppPrivateIP' -p tcp --dport 5432 -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D INPUT -s '$AppPrivateIP' -p tcp --dport 5432 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D INPUT -s '$AppPrivateIP' -p tcp --dport 9092 -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D INPUT -s '$AppPrivateIP' -p tcp --dport 9092 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset || true"
    ) 120 | Out-Null
}

function Invoke-StaleProbe([string]$InstanceId, [string]$WorkflowID, [int]$PartitionID, [string]$OwnerID, [int64]$Epoch) {
    $probeCommand = "docker exec `"`$cid`" /dur043-stale-probe -workflow-id '$WorkflowID' -partition-id $PartitionID -owner-id '$OwnerID' -epoch $Epoch"
    $output = Send-Ssm $InstanceId @(
        'set -e',
        "cd /opt/durable-agent-execution-engine",
        'cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -q runtime)',
        'test -n "$cid"',
        $probeCommand
    )
    $jsonLine = ($output.Trim() -split "\r?\n" | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($jsonLine)) { throw "Stale-owner probe returned no JSON: $output" }
    $probe = $jsonLine | ConvertFrom-Json
    if (-not $probe.stale_rejected -or $probe.before_revision -ne $probe.after_revision) {
        throw "Stale-owner probe did not prove fencing: $output"
    }
    return $probe
}

function Run-NetworkArm([string]$App1, [string]$App2, [string]$Dependency, [string]$DependencyIP, [string]$AppPrivateIP, [string]$WorkflowID, [string]$OutputPath) {
    $partition = Get-PartitionID $WorkflowID
    Stop-AppRuntime $App2
    Set-AppMode $App1 $FixtureDelayMS $ObservationHoldMS
    $app1Configuration = Get-AppConfiguration $App1
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID $partition
    $oldLease = $claimed.lease
    $before = $claimed.workflow
    if ([string]::IsNullOrWhiteSpace($oldLease.owner_id)) { throw "Application host 1 did not own partition $partition before network isolation." }
    if ($before.attempt_state -ne "CLAIMED") { throw "Network fixture must be observed CLAIMED before isolation; observed $($before.attempt_state)." }
    $script:NetworkRulesInserted = $true
    $faultObserved = Insert-NetworkBlock $App1 $Dependency $DependencyIP $AppPrivateIP
    Set-AppEnvironment $App2 $FixtureDelayMS $TakeoverObservationHoldMS
    Start-AppRuntimeNoWait $App2
    $app2Configuration = Get-AppConfiguration $App2
    $takeover = Wait-LeaseTakeover $Dependency $partition $oldLease.owner_id $oldLease.epoch
    $newLease = $takeover.lease
    $takeoverObservedAt = $takeover.observed_at_utc
    $takeoverAt = ([DateTimeOffset]::Parse($newLease.updated_at)).UtcDateTime
    Remove-NetworkBlock $App1 $Dependency $DependencyIP $AppPrivateIP
    $script:NetworkRulesInserted = $false
    $reconnected = Observe-Runtime $App1
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch
    $usefulProgress = Wait-FirstUsefulRecoveryProgress $Dependency $WorkflowID $before.attempt_number $oldLease.epoch 150
    $terminal = Wait-Workflow $Dependency $WorkflowID 150
    $terminalAt = ([DateTimeOffset]::Parse($terminal.updated_at)).UtcDateTime
    if ($terminal.state -ne "SUCCEEDED") { throw "Network-isolated workflow did not recover to SUCCEEDED: $($terminal.state)" }
    # Revision is the durable pre-fault boundary.  Do not use wall-clock
    # timestamps here: transition_history.created_at is not a commit timestamp
    # and the old owner may have made legitimate transitions before isolation.
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND scheduler_epoch=$($oldLease.epoch) AND revision > $($before.revision);")
    if ($historyCount -ne 0) { throw "A transition carrying the superseded epoch was committed after takeover: $historyCount" }
    $result = [ordered]@{
        fault = "app-host-postgres-network-isolation-and-reconnect"
        workflow_id = $WorkflowID
        partition_id = $partition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        takeover_owner_id = $newLease.owner_id
        takeover_epoch = $newLease.epoch
        fault_observed = $faultObserved
        original_runtime_observed = $reconnected
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ observed_at_utc = $usefulProgress.observed_at_utc.ToString("o"); workflow = $usefulProgress.workflow }
        superseded_epoch_transitions_after_takeover = $historyCount
        stale_rejection = $staleProbe
        takeover_observed_at_utc = $takeoverObservedAt.ToString("o")
        takeover_row_updated_at_utc = $takeoverAt.ToString("o")
        terminal_completion_at_utc = $terminalAt.ToString("o")
        fault_command_to_observed_ms = DurationMilliseconds $faultObserved.command_at_utc $faultObserved.observed_at_utc
        fault_observed_to_takeover_ms = DurationMilliseconds $faultObserved.observed_at_utc $takeoverObservedAt
        fault_observed_to_first_useful_progress_ms = DurationMilliseconds $faultObserved.observed_at_utc $usefulProgress.observed_at_utc
        takeover_to_first_useful_progress_ms = $null
        takeover_to_first_useful_progress_note = "The exact lease takeover instant is not persisted independently; TIMEOUT_REPLACEMENT is the first committed transition by the new epoch and fault-to-progress is measured directly."
        first_useful_progress_transition = $usefulProgress.transition
        fault_observed_to_terminal_completion_ms = DurationMilliseconds $faultObserved.observed_at_utc $terminalAt
        configuration = [ordered]@{ activity = "dur048.sleep"; app1 = $app1Configuration; app2 = $app2Configuration; fault_network_ports = $FaultPorts; network_chain = "DOCKER-USER plus dependency INPUT"; network_match_states = @("NEW", "ESTABLISHED", "RELATED"); established_flow_termination = "host conntrack deletion plus dependency-side PostgreSQL session termination"; route_fault = "app-host blackhole route for dependency /32"; dependency_source_ip = $AppPrivateIP }
        limitation = "This arm isolates the app host from PostgreSQL while preserving its process; it is not a host-stop or database-host durability claim."
    }
    Write-Json $OutputPath $result
    return $result
}

function Run-HostArm([string]$App1, [string]$App2, [string]$Dependency, [string]$WorkflowID, [string]$OutputPath) {
    $partition = Get-PartitionID $WorkflowID
    Stop-AppRuntime $App2
    Set-AppMode $App1 $FixtureDelayMS $ObservationHoldMS
    $app1Configuration = Get-AppConfiguration $App1
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID $partition
    $oldLease = $claimed.lease
    $before = $claimed.workflow
    if ([string]::IsNullOrWhiteSpace($oldLease.owner_id)) { throw "Application host 1 did not own partition $partition before host stop." }
    if ($before.attempt_state -ne "CLAIMED") { throw "Host-stop fixture must be observed CLAIMED before the stop; observed $($before.attempt_state)." }
    $stopRequestedAt = [DateTime]::UtcNow
    Invoke-Aws @("ec2", "stop-instances", "--instance-ids", $App1, "--force") | Out-Null
    $deadline = (Get-Date).AddMinutes(3)
    $stoppedObserved = $false
    do {
        Start-Sleep -Seconds 5
        $state = (Invoke-Aws @("ec2", "describe-instances", "--instance-ids", $App1, "--query", "Reservations[0].Instances[0].State.Name", "--output", "text")).Trim()
    } while ($state -ne "stopped" -and (Get-Date) -lt $deadline)
    if ($state -ne "stopped") { throw "The application host did not reach stopped state after forced stop: $state" }
    $stoppedObserved = $true
    $stoppedAt = [DateTime]::UtcNow
    $script:App1WasStopped = $true
    Set-AppEnvironment $App2 $FixtureDelayMS $TakeoverObservationHoldMS
    $app2Configuration = Get-AppConfiguration $App2
    Start-AppRuntimeNoWait $App2
    $takeover = Wait-LeaseTakeover $Dependency $partition $oldLease.owner_id $oldLease.epoch
    $newLease = $takeover.lease
    $takeoverObservedAt = $takeover.observed_at_utc
    $takeoverAt = ([DateTimeOffset]::Parse($newLease.updated_at)).UtcDateTime
    $usefulProgress = Wait-FirstUsefulRecoveryProgress $Dependency $WorkflowID $before.attempt_number $oldLease.epoch 180
    $restartRequestedAt = [DateTime]::UtcNow
    Invoke-Aws @("ec2", "start-instances", "--instance-ids", $App1) | Out-Null
    $deadline = (Get-Date).AddMinutes(5)
    do {
        Start-Sleep -Seconds 5
        $state = (Invoke-Aws @("ec2", "describe-instances", "--instance-ids", $App1, "--query", "Reservations[0].Instances[0].State.Name", "--output", "text")).Trim()
    } while ($state -ne "running" -and (Get-Date) -lt $deadline)
    if ($state -ne "running") { throw "The application host did not return to running state: $state" }
    $runningObservedAt = [DateTime]::UtcNow
    Wait-Ssm $App1
    $originalRuntimeObserved = Observe-Runtime $App1
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch
    $terminal = Wait-Workflow $Dependency $WorkflowID 180
    $terminalAt = ([DateTimeOffset]::Parse($terminal.updated_at)).UtcDateTime
    if ($terminal.state -ne "SUCCEEDED") { throw "Host-stop workflow did not recover to SUCCEEDED: $($terminal.state)" }
    # The workflow revision captured before the stop is the authoritative
    # boundary for post-fault mutations; wall-clock ordering is not.
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND scheduler_epoch=$($oldLease.epoch) AND revision > $($before.revision);")
    if ($historyCount -ne 0) { throw "A transition carrying the stopped host's epoch was committed after takeover: $historyCount" }
    $result = [ordered]@{
        fault = "application-host-forced-stop-and-restart"
        workflow_id = $WorkflowID
        partition_id = $partition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        takeover_owner_id = $newLease.owner_id
        takeover_epoch = $newLease.epoch
        recovery_transition_epoch = $usefulProgress.transition.scheduler_epoch
        recovery_transition_matches_observed_epoch = ($usefulProgress.transition.scheduler_epoch -eq $newLease.epoch)
        fault_observed = [ordered]@{ stop_requested_at_utc = $stopRequestedAt.ToString("o"); stopped_at_utc = $stoppedAt.ToString("o"); stopped_state = $stoppedObserved; restart_requested_at_utc = $restartRequestedAt.ToString("o"); running_observed_at_utc = $runningObservedAt.ToString("o"); ssm_online_after_restart = $true; original_runtime_observed = $originalRuntimeObserved }
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ observed_at_utc = $usefulProgress.observed_at_utc.ToString("o"); workflow = $usefulProgress.workflow }
        superseded_epoch_transitions_after_takeover = $historyCount
        stale_rejection = $staleProbe
        takeover_observed_at_utc = $takeoverObservedAt.ToString("o")
        takeover_row_updated_at_utc = $takeoverAt.ToString("o")
        terminal_completion_at_utc = $terminalAt.ToString("o")
        stop_requested_to_running_ms = DurationMilliseconds $stopRequestedAt $runningObservedAt
        takeover_to_original_reconnect_ms = DurationMilliseconds $takeoverObservedAt $runningObservedAt
        stop_requested_to_first_useful_progress_ms = DurationMilliseconds $stopRequestedAt $usefulProgress.observed_at_utc
        takeover_to_first_useful_progress_ms = $null
        takeover_to_first_useful_progress_note = "partition_leases stores only the current lease, not takeover history; its observed epoch may be newer than the historical recovery transition. TIMEOUT_REPLACEMENT is the first committed transition for the recovered attempt and stop-to-progress is measured directly."
        first_useful_progress_transition = $usefulProgress.transition
        stop_requested_to_terminal_completion_ms = DurationMilliseconds $stopRequestedAt $terminalAt
        configuration = [ordered]@{ activity = "dur048.sleep"; app1 = $app1Configuration; app2 = $app2Configuration; fault_network_ports = $FaultPorts; network_chain = "not_applicable_host_stop"; network_match_states = @("not_applicable") }
        limitation = "Host-stop recovery is measured separately from the preserved-process network arm; database-host durability remains out of scope."
    }
    Write-Json $OutputPath $result
    return $result
}

if ($SelfTest) {
    $vectors = @(
        [ordered]@{ workflow_id = "workflow-0001"; partition = 8 },
        [ordered]@{ workflow_id = "workflow-0002"; partition = 14 },
        [ordered]@{ workflow_id = "incident-2026-09-14-a"; partition = 0 },
        [ordered]@{ workflow_id = "retry-key/abc"; partition = 0 },
        [ordered]@{ workflow_id = "workflow-0003"; partition = 14 }
    )
    foreach ($vector in $vectors) {
        $actual = Get-PartitionID $vector.workflow_id
        if ($actual -ne $vector.partition) { throw "partition self-test failed for $($vector.workflow_id): got $actual, want $($vector.partition)" }
    }
    $jsonSelfTestPath = Join-Path $RepoRoot ".scratch/dur043-json-selftest.json"
    try {
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $jsonSelfTestPath) | Out-Null
        Write-Json $jsonSelfTestPath ([ordered]@{ self_test = $true })
        $jsonBytes = [System.IO.File]::ReadAllBytes($jsonSelfTestPath)
        if ($jsonBytes.Length -ge 3 -and $jsonBytes[0] -eq 0xEF -and $jsonBytes[1] -eq 0xBB -and $jsonBytes[2] -eq 0xBF) {
            throw "JSON self-test wrote a UTF-8 BOM; AWS CLI input JSON must be BOM-free."
        }
    } finally {
        Remove-Item -LiteralPath $jsonSelfTestPath -Force -ErrorAction SilentlyContinue
    }
    Write-Host "DUR-043 PowerShell 5.1 self-test PASS: 5 independent partition vectors and BOM-free AWS JSON."
    return
}

Push-Location $RepoRoot
try {
    if ([string]::IsNullOrWhiteSpace($AwsProfile)) { throw "AWS profile is required." }
    if ($Scenario -in @("all", "network") -and [string]::IsNullOrWhiteSpace($NetworkWorkflowId)) { throw "-NetworkWorkflowId is required for the network arm; it must be an active workflow fixture." }
    if ($Scenario -in @("all", "host") -and [string]::IsNullOrWhiteSpace($HostWorkflowId)) { throw "-HostWorkflowId is required for the host-stop arm; it must be an active workflow fixture." }
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) { $OutputRoot = Join-Path $RepoRoot ("experiments/portfolio/cloud-recovery/dur049-aws-" + [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")) }
    elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) { $OutputRoot = Join-Path $RepoRoot $OutputRoot }
    if (Test-Path -LiteralPath $OutputRoot) { throw "Refusing to overwrite existing evidence: $OutputRoot" }
    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

    $outputs = Get-TerraformOutputs
    $dependency = [string](Get-OutputValue $outputs "dependency_instance_id")
    $dependencyIP = [string](Get-OutputValue $outputs "dependency_private_ip")
    $appIDs = @((Get-OutputValue $outputs "app_instance_ids") | ForEach-Object { [string]$_ })
    $appPrivateIPs = @((Get-OutputValue $outputs "app_private_ips") | ForEach-Object { [string]$_ })
    if ($appIDs.Count -ne 2 -or $appPrivateIPs.Count -ne 2 -or [string]::IsNullOrWhiteSpace($dependency) -or [string]::IsNullOrWhiteSpace($dependencyIP)) { throw "Terraform outputs do not describe two app hosts and one dependency host." }
    foreach ($instance in @($dependency) + $appIDs) {
        $state = (Invoke-Aws @("ec2", "describe-instances", "--instance-ids", $instance, "--query", "Reservations[0].Instances[0].State.Name", "--output", "text")).Trim()
        if ($state -ne "running") { throw "Instance $instance is not running: $state" }
        Wait-Ssm $instance
    }

    $protocol = [ordered]@{
        schema_version = "dur043-multihost.v2"
        status = "IN_PROGRESS"
        generated_at_utc = [DateTime]::UtcNow.ToString("o")
        git_commit = (git rev-parse HEAD).Trim()
        region = $Region
        topology = [ordered]@{ application_hosts = 2; dependency_hosts = 1; app_instance_ids = $appIDs; app_private_ips = $appPrivateIPs; dependency_instance_id = $dependency; dependency_private_ip = $dependencyIP }
        scenarios = @($Scenario)
        configuration = [ordered]@{ activity = "dur048.sleep"; requested_activity_delay_ms = $FixtureDelayMS; requested_scheduler_hold_after_acquire_ms = $ObservationHoldMS; takeover_observation_hold_ms = $TakeoverObservationHoldMS; requested_fixture_activity_enabled = $FixtureActivityEnabled; requested_worker_slots = $WorkerSlots; runtime_engine_mode = "disabled"; fault_network_ports = $FaultPorts; network_chain = "DOCKER-USER plus dependency INPUT for network arm; not_applicable_host_stop for host arm"; network_match_states = @("NEW", "ESTABLISHED", "RELATED"); established_flow_termination = "host conntrack deletion plus dependency-side PostgreSQL session termination"; route_fault = "app-host blackhole route for dependency /32" }
        required_observation = "SSM, EC2, Docker, and PostgreSQL state must confirm each fault; controller intent alone never produces PASS."
        results = @()
    }
    Write-Json (Join-Path $OutputRoot "protocol.json") $protocol

    if ($Scenario -in @("all", "network")) {
        $networkResult = Run-NetworkArm $appIDs[0] $appIDs[1] $dependency $dependencyIP $appPrivateIPs[0] $NetworkWorkflowId (Join-Path $OutputRoot "network-isolation.json")
        $Results += $networkResult
    }
    if ($Scenario -in @("all", "host")) {
        $hostResult = Run-HostArm $appIDs[0] $appIDs[1] $dependency $HostWorkflowId (Join-Path $OutputRoot "host-stop.json")
        $Results += $hostResult
    }
    $protocol.status = "PASS"
    $protocol.results = $Results
    $protocol.acceptance = [ordered]@{ fault_confirmation = $true; peer_takeover = $true; superseded_epoch_transitions = 0; useful_work_checked = $true; no_cleanup_claim = "Terraform teardown remains a separate operator step and is recorded after the campaign." }
    Write-Json (Join-Path $OutputRoot "protocol.json") $protocol
    Write-Host "DUR-043 multi-host campaign PASS: $($Results.Count) fault arm(s) completed with zero superseded-epoch transitions."
} catch {
    $protocol = [ordered]@{ schema_version = "dur043-multihost.v2"; status = "FAIL"; generated_at_utc = [DateTime]::UtcNow.ToString("o"); git_commit = (git rev-parse HEAD).Trim(); scenarios = @($Scenario); error = $_.Exception.Message; results = $Results }
    if (Test-Path -LiteralPath $OutputRoot) { Write-Json (Join-Path $OutputRoot "protocol.json") $protocol }
    throw
} finally {
    if ($NetworkRulesInserted) { try { Remove-NetworkBlock $appIDs[0] $dependency $dependencyIP $appPrivateIPs[0] } catch { Write-Warning "Could not remove network rules during cleanup: $_" } }
    if ($App1WasStopped) { try { Invoke-Aws @("ec2", "start-instances", "--instance-ids", $appIDs[0]) | Out-Null } catch { Write-Warning "Could not restart app host 1 during cleanup: $_" } }
    Pop-Location
}
