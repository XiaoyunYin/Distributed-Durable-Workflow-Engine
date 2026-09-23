[CmdletBinding()]
param(
    [ValidateSet("all", "network", "host", "lock-held")]
    [string]$Scenario = "all",
    [string]$NetworkWorkflowId = "",
    [string]$HostWorkflowId = "",
    [string]$LockWorkflowId = "",
    [string]$LockProgressWorkflowId = "",
    [string]$AwsProfile = $(if ($env:AWS_PROFILE) { $env:AWS_PROFILE } else { "admin-learning" }),
    [string]$Region = "us-west-1",
    [string]$TerraformDir = "deploy/aws",
    [string]$OutputRoot = "",
    [switch]$TerminateDatabaseSessions,
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
$script:CurrentEpisodePath = $null
$script:CurrentEpisodeState = $null
$script:OutputRootCreatedByThisRun = $false
$script:AttemptLedgerPath = $null
$script:AttemptLedgerSequence = 0
$protocol = $null
$StartedApp1 = $false
$StartedApp2 = $false
$NetworkRulesInserted = $false
$App1WasStopped = $false
$App2MayBeStopped = $false
# Cross-host SSM observations can take several seconds. Keep the activity
# claimed long enough to observe the durable pre-fault boundary before the
# network or host fault is injected.
$FixtureDelayMS = 60000
# The lock-held arm must outlast standby activation, Kafka group assignment,
# and one useful result. A shorter hold made the fixture time out before its
# progress assertion and could not establish overlap with the locked row.
$LockHolderDurationSeconds = 180
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

function Save-FailureProtocolIfOwned([bool]$CreatedByThisRun, [string]$OutputPath, [object]$Protocol) {
    if (-not $CreatedByThisRun -or -not (Test-Path -LiteralPath $OutputPath -PathType Container)) { return $false }
    Write-Json (Join-Path $OutputPath 'protocol.json') $Protocol
    return $true
}

function Append-AttemptLedger([System.Collections.IDictionary]$Episode) {
    if ([string]::IsNullOrWhiteSpace($script:AttemptLedgerPath)) { return }
    $script:AttemptLedgerSequence++
    $entry = [ordered]@{
        sequence = $script:AttemptLedgerSequence
        observed_at_utc = [DateTime]::UtcNow.ToString("o")
        campaign_run = [System.IO.Path]::GetFileName((Split-Path -Parent $script:AttemptLedgerPath))
        episode_file = [System.IO.Path]::GetFileName($script:CurrentEpisodePath)
        workflow_id = $Episode.workflow_id
        arm = $Episode.fault
        source_commit = $Episode.source_commit
        stage = $Episode.stage
        status = $Episode.status
    }
    if ($Episode.Contains("error")) { $entry.error = $Episode.error }
    $line = ($entry | ConvertTo-Json -Depth 8 -Compress) + [Environment]::NewLine
    [System.IO.File]::AppendAllText($script:AttemptLedgerPath, $line, (New-Object System.Text.UTF8Encoding($false)))
}

function Save-EpisodeCheckpoint([string]$Path, [System.Collections.IDictionary]$Episode, [string]$Stage) {
    $Episode["stage"] = $Stage
    $Episode["updated_at_utc"] = [DateTime]::UtcNow.ToString("o")
    Write-Json $Path $Episode
    $script:CurrentEpisodePath = $Path
    $script:CurrentEpisodeState = $Episode
    Append-AttemptLedger $Episode
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

function Get-AppRuntimeComposeLookupCommand() {
    return "docker compose --env-file '$RemoteEnv' -f '$AppCompose' ps -q runtime"
}

function Get-AppRuntimeComposeLookupLine() {
    return 'cid=$(' + (Get-AppRuntimeComposeLookupCommand) + ')'
}

function Get-AppRuntimePartitionLockProbeCommand([int]$PartitionID) {
    if ($PartitionID -lt 0 -or $PartitionID -ge 16) { throw "Partition ID must be 0..15 for a database lock probe." }
    return 'probe=$(docker exec "$cid" /dur049-db-probe -partition-id ' + $PartitionID + '); printf "lock_probe_json=%s\n" "$probe"'
}

function Assert-LeaseRowLockProbe([string]$Output) {
    $match = [regex]::Match($Output, '(?m)^lock_probe_json=(\{[^\r\n]*\})$')
    if (-not $match.Success) { throw "The in-container PostgreSQL row-lock probe returned no JSON observation: $Output" }
    $probe = $match.Groups[1].Value | ConvertFrom-Json
    if ($probe.status -ne 'row_lock_held') { throw "The lease row was not observed locked during other-partition progress: $($match.Groups[1].Value)" }
    return $probe
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

function Ensure-FixtureDefinition([string]$DependencyInstanceId) {
    $definitionID = "dur049-fault-fixture-v1"
    $graph = '{"entry":"dur048.sleep","nodes":[{"id":"dur048.sleep","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}'
    $versions = '{"dur048.sleep":"v1"}'
    $effects = '{"dur048.sleep":"PURE_ACTIVITY"}'
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $definitionBytes = [Text.Encoding]::UTF8.GetBytes("$graph`n$versions`n$effects")
        $definitionHash = ([BitConverter]::ToString($sha.ComputeHash($definitionBytes))).Replace('-', '').ToLowerInvariant()
    } finally {
        $sha.Dispose()
    }
    # Keep creation and verification as separate statements. A data-modifying
    # CTE and a sibling SELECT share one PostgreSQL statement snapshot, so on a
    # fresh database the SELECT cannot see the row inserted by that CTE and
    # returns no output. That made the first campaign run fail after inserting
    # an otherwise-correct fixture definition.
    $insert = @"
INSERT INTO engine.workflow_definitions (definition_id, version, definition_hash, graph, activity_versions, effect_classes)
VALUES ('$definitionID', 1, '$definitionHash', '$graph'::jsonb, '$versions'::jsonb, '$effects'::jsonb)
ON CONFLICT (definition_id, version) DO NOTHING;
"@
    [void](Invoke-DbSql $DependencyInstanceId $insert)
    $verify = @"
SELECT CASE WHEN definition_hash='$definitionHash' AND graph='$graph'::jsonb AND activity_versions='$versions'::jsonb AND effect_classes='$effects'::jsonb THEN 'MATCH' ELSE 'MISMATCH' END
FROM engine.workflow_definitions WHERE definition_id='$definitionID' AND version=1;
"@
    $observed = Invoke-DbSql $DependencyInstanceId $verify
    if ($observed.Trim() -ne "MATCH") { throw "DUR-049 fixture definition differs from the frozen campaign definition: $observed" }
    $script:FixtureDefinitionID = $definitionID
    $script:FixtureDefinitionHash = $definitionHash
}

function Submit-FixtureWorkflow([string]$InstanceId, [string]$WorkflowID) {
    $body = [ordered]@{
        workflow_id = $WorkflowID
        namespace = "dur049-aws"
        submission_key = $WorkflowID
        payload = [ordered]@{ campaign = "dur049-round71"; workflow_id = $WorkflowID }
        definition_id = $script:FixtureDefinitionID
        definition_version = 1
        initial_node_id = "dur048.sleep"
        initial_input = [ordered]@{ campaign = "dur049-round71"; workflow_id = $WorkflowID }
        actor_id = "dur049-round71-controller"
    }
    $json = $body | ConvertTo-Json -Depth 8 -Compress
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
    $output = Send-Ssm $InstanceId @(
        "set -e",
        "printf '%s' '$encoded' | base64 -d | curl --fail-with-body --silent --show-error --max-time 15 -X POST http://127.0.0.1:8080/v1/workflows -H 'Content-Type: application/json' --data-binary @-"
    ) 60
    $jsonLine = ($output.Trim() -split "\r?\n" | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($jsonLine)) { throw "Workflow submission returned no JSON for $WorkflowID`: $output" }
    $response = $jsonLine | ConvertFrom-Json
    if ($response.workflow.workflow_id -ne $WorkflowID) { throw "Workflow API returned a different workflow ID for $WorkflowID`: $jsonLine" }
    return $response.workflow
}

function Wait-AppApi([string]$InstanceId, [int]$TimeoutSeconds = 90) {
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'for i in $(seq 1 30); do if curl --fail --silent --show-error --max-time 2 http://127.0.0.1:8080/healthz >/dev/null; then echo api_ready=true; exit 0; fi; sleep 2; done; exit 1'
    ) $TimeoutSeconds
    if ($output -notmatch 'api_ready=true') { throw "Runtime API did not become ready on ${InstanceId}: $output" }
}

function Get-ClaimedAttempt([string]$DependencyInstanceId, [string]$WorkflowID) {
    $sql = "SELECT node_id, iteration, attempt_number, state, COALESCE(worker_id,'') FROM engine.activity_attempts WHERE workflow_id='$WorkflowID' AND is_current ORDER BY attempt_number DESC LIMIT 1;"
    $row = Invoke-DbSql $DependencyInstanceId $sql
    $parts = $row -split '\|', 5
    if ($parts.Count -lt 5 -or $parts[3] -ne 'CLAIMED') { throw "Workflow $WorkflowID was not observed with a current CLAIMED attempt: $row" }
    if ([string]::IsNullOrWhiteSpace($parts[4])) { throw "Workflow $WorkflowID current attempt has no worker identity." }
    return [ordered]@{ node_id = $parts[0]; iteration = [int]$parts[1]; attempt_number = [int64]$parts[2]; state = $parts[3]; worker_id = $parts[4] }
}

function Get-AttemptScheduleLease([string]$DependencyInstanceId, [string]$WorkflowID, [int64]$AttemptNumber) {
    # The scheduler releases its partition lease at the end of each pass.
    # Recover the owner/epoch that durably created this attempt from history,
    # rather than requiring a transient current lease row at worker-claim time.
    $sql = @"
SELECT a.partition_id, a.acquisition_id::text, a.owner_id::text, a.epoch,
       a.acquired_at::text, a.lease_expires_at::text
FROM engine.transition_history h
JOIN engine.workflow_executions w ON w.workflow_id=h.workflow_id
JOIN engine.lease_acquisitions a
  ON a.partition_id=w.partition_id AND a.epoch=h.scheduler_epoch
WHERE h.workflow_id='$WorkflowID' AND h.reason='ATTEMPT_CREATED'
  AND h.attempt_number=$AttemptNumber
ORDER BY h.revision DESC LIMIT 1;
"@
    $row = Invoke-DbSql $DependencyInstanceId $sql
    $parts = $row -split '\|', 6
    if ($parts.Count -ne 6) { throw "No durable scheduler acquisition matches ATTEMPT_CREATED for $WorkflowID attempt ${AttemptNumber}: $row" }
    return [ordered]@{
        partition_id = [int]$parts[0]
        acquisition_id = $parts[1]
        owner_id = $parts[2]
        epoch = [int64]$parts[3]
        acquired_at_utc = ([DateTimeOffset]::Parse($parts[4])).UtcDateTime.ToString('o')
        lease_expires_at_utc = ([DateTimeOffset]::Parse($parts[5])).UtcDateTime.ToString('o')
    }
}

function Get-AttemptSummary([string]$DependencyInstanceId, [string]$WorkflowID) {
    $sql = @"
SELECT attempt_number, state, is_current, effect_class, outcome_disposition,
       (result IS NOT NULL), COALESCE(worker_id,''), COALESCE(worker_request_id,''),
       created_at::text, updated_at::text
FROM engine.activity_attempts
WHERE workflow_id='$WorkflowID'
ORDER BY attempt_number;
"@
    $rows = Invoke-DbSql $DependencyInstanceId $sql
    $attempts = @()
    foreach ($row in ($rows.Trim() -split "\r?\n")) {
        if ([string]::IsNullOrWhiteSpace($row)) { continue }
        $parts = $row -split '\|', 10
        if ($parts.Count -ne 10) { throw "Attempt summary row is malformed: $row" }
        $attempts += [ordered]@{
            attempt_number = [int64]$parts[0]
            state = $parts[1]
            is_current = $parts[2] -eq 't'
            effect_class = $parts[3]
            outcome_disposition = $parts[4]
            result_recorded = $parts[5] -eq 't'
            worker_id = $parts[6]
            worker_request_id = $parts[7]
            created_at_utc = ([DateTimeOffset]::Parse($parts[8])).UtcDateTime.ToString('o')
            updated_at_utc = ([DateTimeOffset]::Parse($parts[9])).UtcDateTime.ToString('o')
        }
    }
    return $attempts
}

function Get-LeaseAcquisitionSummary([string]$DependencyInstanceId, [int]$PartitionID, [int64]$AfterEpoch) {
	# The runtime records an acquisition on every scheduler pass. Exporting the
	# full lifetime history via SSM can exceed its output limit and truncate a
	# row; keep this diagnostic bounded and preserve the exact first takeover in
	# first_new_owner_acquisition separately.
	$sql = "SELECT owner_id::text, count(*), min(epoch), max(epoch), min(acquired_at)::text, max(acquired_at)::text FROM engine.lease_acquisitions WHERE partition_id=$PartitionID AND epoch > $AfterEpoch GROUP BY owner_id ORDER BY min(epoch), owner_id;"
	$rows = Invoke-DbSql $DependencyInstanceId $sql
	$items = @()
	foreach ($row in ($rows.Trim() -split "\r?\n")) {
		if ([string]::IsNullOrWhiteSpace($row)) { continue }
		$parts = $row -split '\|', 6
		if ($parts.Count -lt 6) { throw "Lease acquisition summary row is malformed: $row" }
		$items += [ordered]@{ owner_id = $parts[0]; acquisition_count = [int64]$parts[1]; first_epoch = [int64]$parts[2]; last_epoch = [int64]$parts[3]; first_database_recorded_at_utc = ([DateTimeOffset]::Parse($parts[4])).UtcDateTime.ToString("o"); last_database_recorded_at_utc = ([DateTimeOffset]::Parse($parts[5])).UtcDateTime.ToString("o") }
	}
	return $items
}

function Get-LeaseEpochBoundary([string]$DependencyInstanceId, [int]$PartitionID) {
    $value = Invoke-DbSql $DependencyInstanceId "SELECT COALESCE(max(epoch),0) FROM engine.lease_acquisitions WHERE partition_id=$PartitionID;"
    return [int64]$value.Trim()
}

function Wait-FirstNewOwnerAcquisition([string]$DependencyInstanceId, [int]$PartitionID, [string]$OldOwnerID, [int64]$EpochBoundary, [DateTime]$FaultObservedAt, [int]$TimeoutSeconds = 150) {
    # The database acquisition row is the durable event identity. Bound it by
    # both the pre-fault epoch and observed fault time so an earlier standby
    # pass cannot be mislabeled as takeover. acquired_at is diagnostic (inside
    # the transaction), not a commit timestamp.
    $faultISO = ([DateTimeOffset]$FaultObservedAt).ToUniversalTime().ToString('o')
    $sql = "SELECT acquisition_id::text, owner_id::text, epoch, acquired_at::text, lease_expires_at::text FROM engine.lease_acquisitions WHERE partition_id=$PartitionID AND owner_id::text <> '$OldOwnerID' AND epoch > $EpochBoundary AND acquired_at >= '$faultISO'::timestamptz ORDER BY epoch, acquired_at, acquisition_id LIMIT 1;"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $row = Invoke-DbSql $DependencyInstanceId $sql
        if (-not [string]::IsNullOrWhiteSpace($row)) {
            $parts = $row.Trim() -split '\|', 5
            if ($parts.Count -eq 5) {
                $observedAt = [DateTime]::UtcNow
                $recordedAt = ([DateTimeOffset]::Parse($parts[3])).UtcDateTime
                return [ordered]@{ acquisition_id = $parts[0]; owner_id = $parts[1]; epoch = [int64]$parts[2]; database_recorded_at_utc = $recordedAt.ToString("o"); controller_observed_at_utc = $observedAt.ToString("o"); observed_at = $observedAt.ToString("o"); lease_expires_at_utc = ([DateTimeOffset]::Parse($parts[4])).UtcDateTime.ToString("o"); fault_observed_at_utc = ([DateTimeOffset]$FaultObservedAt).ToUniversalTime().ToString("o") }
            }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "No first post-fault acquisition by a new owner with epoch above $EpochBoundary was observed after fault at $($FaultObservedAt.ToString('o'))."
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
            return [ordered]@{ workflow = $workflow; observed_at_utc = ([DateTimeOffset]::Parse($workflow.attempt_created_at)).UtcDateTime.ToString('o') }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID did not commit a replacement attempt after the fault."
}

function Wait-FirstUsefulRecoveryProgress([string]$DependencyInstanceId, [string]$WorkflowID, [int64]$PreviousAttemptNumber, [int64]$PreviousRevision, [string]$RecoveryOwnerID, [int64]$PreviousEpoch, [int]$TimeoutSeconds = 180) {
    # Useful progress means the replacement attempt returned an accepted result,
    # not merely that the scheduler created it or that the workflow terminated.
    # Tie TIMEOUT_REPLACEMENT to a durable acquisition by the same scheduler
    # owner. Scheduler epochs advance across passes, so the progress transition
    # need not use the first takeover epoch. Revisions establish replacement
    # before result; timestamps below are diagnostic only.
    $sql = @"
SELECT h.revision, COALESCE(h.scheduler_epoch,0), r.revision, r.scheduler_epoch,
       a.acquisition_id::text, a.owner_id::text, a.acquired_at::text,
       h.reason, COALESCE(h.attempt_number,0), h.created_at::text, h.actor_kind,
       old.state, old.is_current, replacement.state, replacement.is_current
FROM engine.transition_history h
JOIN engine.transition_history r
  ON r.workflow_id=h.workflow_id AND r.revision > $PreviousRevision AND r.revision < h.revision
 AND r.reason='TIMEOUT_REPLACEMENT' AND r.scheduler_epoch > $PreviousEpoch
 AND r.attempt_number + 1 = h.attempt_number
JOIN engine.workflow_executions w ON w.workflow_id=h.workflow_id
JOIN engine.lease_acquisitions a
  ON a.partition_id=w.partition_id AND a.epoch=r.scheduler_epoch
 AND a.owner_id::text='$RecoveryOwnerID'
JOIN engine.activity_attempts old
  ON old.workflow_id=r.workflow_id AND old.node_id=r.node_id
 AND old.iteration=r.iteration AND old.attempt_number=r.attempt_number
JOIN engine.activity_attempts replacement
  ON replacement.workflow_id=h.workflow_id AND replacement.node_id=h.node_id
 AND replacement.iteration=h.iteration AND replacement.attempt_number=h.attempt_number
WHERE h.workflow_id='$WorkflowID' AND h.revision > $PreviousRevision
  AND h.reason='ATTEMPT_RESULT_RECORDED' AND h.actor_kind='worker'
  AND h.attempt_number > $PreviousAttemptNumber
  AND old.state='TIMED_OUT' AND NOT old.is_current
ORDER BY h.revision LIMIT 1;
"@
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $row = Invoke-DbSql $DependencyInstanceId $sql
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        $parts = $row.Trim() -split '\|', 15
        if ($parts.Count -eq 15 -and [int64]$parts[8] -gt $PreviousAttemptNumber) {
            $observedAt = [DateTime]::UtcNow
            $resultRecordedAt = ([DateTimeOffset]::Parse($parts[9])).UtcDateTime
            $replacementAcquiredAt = ([DateTimeOffset]::Parse($parts[6])).UtcDateTime
            $resultEpoch = if ([int64]$parts[1] -eq 0) { $null } else { [int64]$parts[1] }
            return [ordered]@{
                workflow = $workflow
                controller_observed_at_utc = $observedAt.ToString("o")
                observed_at = $observedAt.ToString("o")
                progress_definition = "accepted result recorded for replacement attempt"
                transition = [ordered]@{
                    revision = [int64]$parts[0]
                    scheduler_epoch = $resultEpoch
                    reason = $parts[7]
                    attempt_number = [int64]$parts[8]
                    database_recorded_at_utc = $resultRecordedAt.ToString("o")
                    actor_kind = $parts[10]
                }
                timeout_replacement_transition = [ordered]@{
                    revision = [int64]$parts[2]
                    scheduler_epoch = [int64]$parts[3]
                    lease_acquisition_id = $parts[4]
                    lease_owner_id = $parts[5]
                    lease_acquired_at_utc = $replacementAcquiredAt.ToString("o")
                    prior_attempt_state = $parts[11]
                    prior_attempt_is_current = $parts[12] -eq 't'
                    replacement_attempt_state = $parts[13]
                    replacement_attempt_is_current = $parts[14] -eq 't'
                }
            }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID did not record an accepted result from a replacement attempt after first new-owner acquisition."
}

function Wait-LockHeldResultConsumption([string]$DependencyInstanceId, [string]$WorkflowID, [int64]$AttemptNumber, [int64]$PreviousRevision, [int64]$PreviousEpoch, [object]$Acquisition, [int]$TimeoutSeconds = 180) {
    # Under the lease-row lock, the worker may still finish and persist its
    # result. The scheduler must not consume it until a new lease acquisition
    # has committed. This arm therefore expects same-attempt receipt then
    # consumption, not a timeout/replacement that did not occur in the run.
    $sql = @"
SELECT consumed.revision, consumed.scheduler_epoch, consumed.attempt_number,
       consumed.actor_kind, consumed.reason, consumed.created_at::text,
       receipt.revision, receipt.attempt_number, receipt.actor_kind, receipt.reason, receipt.created_at::text,
       acquisition.acquisition_id::text, acquisition.owner_id::text,
       acquisition.epoch, acquisition.acquired_at::text
FROM engine.transition_history consumed
JOIN engine.transition_history receipt
  ON receipt.workflow_id=consumed.workflow_id
 AND receipt.node_id=consumed.node_id
 AND receipt.iteration=consumed.iteration
 AND receipt.attempt_number=consumed.attempt_number
 AND receipt.reason='ATTEMPT_RESULT_RECORDED'
 AND receipt.actor_kind='worker'
 AND receipt.revision < consumed.revision
 AND receipt.revision > $PreviousRevision
JOIN engine.workflow_executions workflow
  ON workflow.workflow_id=consumed.workflow_id
JOIN engine.lease_acquisitions acquisition
  ON acquisition.partition_id=workflow.partition_id
 AND acquisition.acquisition_id::text='$($Acquisition.acquisition_id)'
 AND acquisition.owner_id::text='$($Acquisition.owner_id)'
 AND acquisition.epoch=consumed.scheduler_epoch
WHERE consumed.workflow_id='$WorkflowID'
  AND consumed.revision > $PreviousRevision
  AND consumed.reason='RESULT_CONSUMED'
  AND consumed.attempt_number=$AttemptNumber
  AND acquisition.epoch >= $PreviousEpoch
ORDER BY consumed.revision LIMIT 1;
"@
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $row = Invoke-DbSql $DependencyInstanceId $sql
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        $parts = $row.Trim() -split '\|', 15
        if ($parts.Count -eq 15) {
            $observedAt = [DateTime]::UtcNow
            return [ordered]@{
                workflow = $workflow
                controller_observed_at_utc = $observedAt.ToString('o')
                observed_at = $observedAt.ToString('o')
                progress_definition = 'scheduler consumed the original attempt result under the first new-owner lease'
                transition = [ordered]@{
                    revision = [int64]$parts[0]
                    scheduler_epoch = [int64]$parts[1]
                    attempt_number = [int64]$parts[2]
                    actor_kind = $parts[3]
                    reason = $parts[4]
                    database_recorded_at_utc = ([DateTimeOffset]::Parse($parts[5])).UtcDateTime.ToString('o')
                    lease_acquisition_id = $parts[11]
                    lease_owner_id = $parts[12]
                    lease_epoch = [int64]$parts[13]
                }
                result_receipt_transition = [ordered]@{
                    revision = [int64]$parts[6]
                    attempt_number = [int64]$parts[7]
                    actor_kind = $parts[8]
                    reason = $parts[9]
                    database_recorded_at_utc = ([DateTimeOffset]::Parse($parts[10])).UtcDateTime.ToString('o')
                }
                consumed_under_acquisition = [ordered]@{
                    acquisition_id = $parts[11]
                    owner_id = $parts[12]
                    epoch = [int64]$parts[13]
                    database_recorded_at_utc = ([DateTimeOffset]::Parse($parts[14])).UtcDateTime.ToString('o')
                }
            }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID did not consume its recorded result under the first new-owner acquisition."
}

function Assert-LockHeldResultOrdering([object]$Acquisition, [object]$Progress) {
    $receipt = $Progress.result_receipt_transition
    $consumed = $Progress.transition
    if ($consumed.reason -ne 'RESULT_CONSUMED' -or $consumed.actor_kind -ne 'scheduler') {
        throw 'Lock-held recovery progress is not a scheduler result-consumption transition.'
    }
    if ($receipt.reason -ne 'ATTEMPT_RESULT_RECORDED' -or $receipt.actor_kind -ne 'worker') {
        throw 'Lock-held recovery has no worker result receipt for the consumed attempt.'
    }
    if ([int64]$receipt.revision -le 0 -or [int64]$receipt.revision -ge [int64]$consumed.revision) {
        throw 'The worker result receipt does not durably precede scheduler consumption.'
    }
    if ([int64]$consumed.attempt_number -ne [int64]$receipt.attempt_number) {
        throw 'The consumed result does not match the result-receipt attempt.'
    }
    if ($consumed.lease_acquisition_id -ne $Acquisition.acquisition_id -or $consumed.lease_owner_id -ne $Acquisition.owner_id -or [int64]$consumed.lease_epoch -ne [int64]$Acquisition.epoch -or [int64]$consumed.scheduler_epoch -ne [int64]$Acquisition.epoch) {
        throw 'The result was not consumed under the first post-fault owner acquisition.'
    }
    if ([int64]$consumed.revision -le [int64]$receipt.revision) {
        throw 'Scheduler consumption did not follow the original attempt result receipt.'
    }
    return [ordered]@{
        first_new_owner_acquisition_id = $Acquisition.acquisition_id
        first_new_owner_id = $Acquisition.owner_id
        first_new_owner_epoch = [int64]$Acquisition.epoch
        result_receipt_revision = [int64]$receipt.revision
        result_consumed_revision = [int64]$consumed.revision
        result_receipt_precedes_consumption = $true
        result_consumed_under_first_new_owner_acquisition = $true
        same_original_attempt_completed_without_replacement = $true
    }
}

function Assert-AcquisitionPrecedesProgress([object]$Acquisition, [object]$Progress) {
    $replacement = $Progress.timeout_replacement_transition
    $result = $Progress.transition
    if ($replacement.lease_owner_id -ne $Acquisition.owner_id) {
        throw "Timeout replacement owner $($replacement.lease_owner_id) differs from first new owner $($Acquisition.owner_id)."
    }
    if ([int64]$replacement.scheduler_epoch -lt [int64]$Acquisition.epoch) {
        throw "Timeout replacement epoch $($replacement.scheduler_epoch) predates first new-owner epoch $($Acquisition.epoch)."
    }
    if ([int64]$replacement.revision -ge [int64]$result.revision) {
        throw "Durable timeout replacement revision $($replacement.revision) does not precede accepted result revision $($result.revision)."
    }
    if ($replacement.prior_attempt_state -ne 'TIMED_OUT' -or $replacement.prior_attempt_is_current) {
        throw "Prior attempt was not durably timed out and settled before the replacement result."
    }
    return [ordered]@{
        first_new_owner_acquisition_id = $Acquisition.acquisition_id
        first_new_owner_id = $Acquisition.owner_id
        first_new_owner_epoch = [int64]$Acquisition.epoch
        recovery_transition_acquisition_id = $replacement.lease_acquisition_id
        recovery_transition_owner_id = $replacement.lease_owner_id
        recovery_transition_epoch = [int64]$replacement.scheduler_epoch
        timeout_replacement_revision = [int64]$replacement.revision
        accepted_result_revision = [int64]$result.revision
        durable_replacement_precedes_accepted_result = $true
        controller_observation_times_are_diagnostic = $true
    }
}

function Wait-ClaimedWorkflow([string]$DependencyInstanceId, [string]$WorkflowID, [int]$TimeoutSeconds = 300) {
    $expectedPartition = Get-PartitionID $WorkflowID
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        if ($workflow.attempt_state -eq "CLAIMED") {
            $attempt = Get-ClaimedAttempt $DependencyInstanceId $WorkflowID
            $schedulerAcquisition = Get-AttemptScheduleLease $DependencyInstanceId $WorkflowID $attempt.attempt_number
            if ($schedulerAcquisition.partition_id -ne $expectedPartition) { throw "Attempt scheduler acquisition partition $($schedulerAcquisition.partition_id) differs from expected $expectedPartition." }
            return [ordered]@{ workflow = $workflow; attempt = $attempt; scheduler_acquisition = $schedulerAcquisition }
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Workflow $WorkflowID was not observed CLAIMED within ${TimeoutSeconds}s."
}

function Test-WorkerStaleResultLog([string]$Line, [string]$WorkflowID, [object]$Attempt) {
    if ([string]::IsNullOrWhiteSpace($Line)) { return $false }
    $required = @(
        "workflow_id=$WorkflowID",
        "node_id=$($Attempt.node_id)",
        "attempt_number=$($Attempt.attempt_number)",
        "worker_id=$($Attempt.worker_id)",
        "operation=result",
        "code=STALE_ATTEMPT"
    )
    foreach ($fragment in $required) {
        if (-not $Line.Contains($fragment)) { return $false }
    }
    return $true
}

function Test-WorkerResultSubmissionLog([string]$Line, [string]$WorkflowID, [object]$Attempt) {
    if ([string]::IsNullOrWhiteSpace($Line)) { return $false }
    $required = @(
        'activity result submission',
        "workflow_id=$WorkflowID",
        "node_id=$($Attempt.node_id)",
        "attempt_number=$($Attempt.attempt_number)",
        "worker_id=$($Attempt.worker_id)",
        'attempt_state=SUCCEEDED',
        'outcome=rejected status=409 code=STALE_ATTEMPT'
    )
    foreach ($fragment in $required) {
        if (-not $Line.Contains($fragment)) { return $false }
    }
    return ($Line -match 'payload_sha256=[0-9a-f]{64}' -and $Line -match 'retries=\d+')
}

function Wait-WorkerStaleResultObservation([string]$InstanceId, [string]$WorkflowID, [object]$Attempt, [int]$TimeoutSeconds = 90) {
    # iteration is emitted between node_id and attempt_number on result
    # submission records, so grep only the contiguous workflow token and let
    # the local predicate validate every identity field independently.
    $identityNeedle = "workflow_id=$WorkflowID"
        $command = "docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml logs --timestamps --no-color worker | grep -F '$identityNeedle' || true"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $output = Send-Ssm $InstanceId @(
            'set -e',
            'cd /opt/durable-agent-execution-engine',
            $command
        )
        $lines = @($output -split "\r?\n")
        $submissionLine = ($lines | Where-Object { Test-WorkerResultSubmissionLog $_ $WorkflowID $Attempt } | Select-Object -Last 1)
        $rejectionLine = ($lines | Where-Object { Test-WorkerStaleResultLog $_ $WorkflowID $Attempt } | Select-Object -Last 1)
        if (-not [string]::IsNullOrWhiteSpace($submissionLine) -and -not [string]::IsNullOrWhiteSpace($rejectionLine)) {
            $fingerprintMatch = [regex]::Match($submissionLine, 'payload_sha256=(?<hash>[0-9a-f]{64})')
            if (-not $fingerprintMatch.Success) { throw 'Rejected result log did not contain a SHA-256 payload fingerprint.' }
            $retryMatch = [regex]::Match($submissionLine, 'retries=(?<count>\d+)')
            if (-not $retryMatch.Success) { throw 'Rejected result log did not contain a retry count.' }
            return [ordered]@{
                observed = $true
                source = 'original worker container stdout; matching result submission metadata and result-operation rejection after reconnect'
                workflow_id = $WorkflowID
                attempt_number = [int64]$Attempt.attempt_number
                worker_id = [string]$Attempt.worker_id
                operation = 'result'
                api_rejection = 'STALE_ATTEMPT'
                attempt_state = 'SUCCEEDED'
                payload_sha256 = $fingerprintMatch.Groups['hash'].Value
                submission_retry_count = [int]$retryMatch.Groups['count'].Value
                retry_behavior = 'HTTP 409 is not retried; preceding uncertain 5xx outcomes are retried with the identical body.'
                observed_at_utc = [DateTime]::UtcNow.ToString('o')
                submission_log_line = $submissionLine.Trim()
                rejection_log_line = $rejectionLine.Trim()
            }
        }
        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)
    throw "Original worker did not log the stale-result rejection for $WorkflowID attempt $($Attempt.attempt_number) within ${TimeoutSeconds}s."
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

function DurationMilliseconds([object]$Started, [object]$Finished) {
    $startTime = ([DateTimeOffset]$Started).UtcDateTime
    $finishTime = ([DateTimeOffset]$Finished).UtcDateTime
    return [math]::Round(($finishTime - $startTime).TotalMilliseconds, 3)
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
    # A prior network-isolation arm can leave a long-lived Kafka connection
    # waiting for the client's request timeout. Environment values may be
    # unchanged in the next arm, so ordinary `compose up` could retain that
    # stuck process. Reset the original app stack before creating a new fault
    # fixture and wait for both services to become healthy.
    $commands = @(
        'set -e',
        'cd /opt/durable-agent-execution-engine',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml up -d --force-recreate --wait --wait-timeout 240 runtime worker'
    )
    Send-Ssm $InstanceId $commands 300 | Out-Null
}

function Get-AppConfiguration([string]$InstanceId) {
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'cd /opt/durable-agent-execution-engine',
        "grep -E '^(DUR048_ACTIVITY_DELAY_MS|DUR048_ALLOW_FIXTURE_ACTIVITY|RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS|WORKER_SLOTS)=' deploy/aws/.env",
        'runtime_cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -q runtime)',
        'worker_cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -q worker)',
        'test -n "$runtime_cid" && test -n "$worker_cid"',
        'printf "GIT_COMMIT=%s\n" "$(git rev-parse HEAD)"',
        'printf "RUNTIME_CONTAINER_ID=%s\n" "$runtime_cid"',
        'printf "RUNTIME_STARTED_AT=%s\n" "$(docker inspect --format ''{{.State.StartedAt}}'' "$runtime_cid")"',
        'printf "WORKER_CONTAINER_ID=%s\n" "$worker_cid"',
        'printf "WORKER_STARTED_AT=%s\n" "$(docker inspect --format ''{{.State.StartedAt}}'' "$worker_cid")"',
        'printf "RUNTIME_IMAGE_ID=%s\n" "$(docker inspect --format ''{{.Image}}'' "$runtime_cid")"',
        'printf "WORKER_IMAGE_ID=%s\n" "$(docker inspect --format ''{{.Image}}'' "$worker_cid")"'
    )
    $values = @{}
    foreach ($line in ($output.Trim() -split "\r?\n")) {
        if ($line -match '^(?<name>[A-Z0-9_]+)=(?<value>.*)$') { $values[$Matches.name] = $Matches.value }
    }
    foreach ($name in @('DUR048_ACTIVITY_DELAY_MS', 'DUR048_ALLOW_FIXTURE_ACTIVITY', 'RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS', 'WORKER_SLOTS', 'GIT_COMMIT', 'RUNTIME_CONTAINER_ID', 'RUNTIME_STARTED_AT', 'WORKER_CONTAINER_ID', 'WORKER_STARTED_AT', 'RUNTIME_IMAGE_ID', 'WORKER_IMAGE_ID')) {
        if (-not $values.ContainsKey($name)) { throw "Remote app configuration is missing $name." }
    }
    if ($values['GIT_COMMIT'] -notmatch '^[0-9a-f]{40}$') { throw "Remote app has no full checked-out source commit: $($values['GIT_COMMIT'])" }
    foreach ($name in @('RUNTIME_IMAGE_ID', 'WORKER_IMAGE_ID')) {
        if ($values[$name] -notmatch '^sha256:[0-9a-f]{64}$') { throw "Remote app reports an invalid image ID for ${name}: $($values[$name])" }
    }
    return [ordered]@{
        repo_commit = $values['GIT_COMMIT']
        runtime_container_id = $values['RUNTIME_CONTAINER_ID']
        runtime_started_at_utc = $values['RUNTIME_STARTED_AT']
        worker_container_id = $values['WORKER_CONTAINER_ID']
        worker_started_at_utc = $values['WORKER_STARTED_AT']
        runtime_image_id = $values['RUNTIME_IMAGE_ID']
        worker_image_id = $values['WORKER_IMAGE_ID']
        activity_delay_ms = [int]$values['DUR048_ACTIVITY_DELAY_MS']
        fixture_activity_enabled = ([int]$values['DUR048_ALLOW_FIXTURE_ACTIVITY']) -eq 1
        scheduler_hold_after_acquire_ms = [int]$values['RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS']
        worker_slots = [int]$values['WORKER_SLOTS']
        runtime_engine_mode = "disabled"
    }
}

function Stop-AppRuntime([string]$InstanceId) {
    $output = Send-Ssm $InstanceId @(
        "set -e",
        "cd /opt/durable-agent-execution-engine",
        "docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml stop runtime worker",
        'for service in runtime worker; do cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -aq "$service"); test -n "$cid"; status=$(docker inspect --format ''{{.State.Status}}'' "$cid"); test "$status" = "exited"; printf "%s=%s\n" "$service" "$status"; done'
    ) 120
    if ($output -notmatch 'runtime=exited' -or $output -notmatch 'worker=exited') { throw "Standby runtime did not stop cleanly: $output" }
    return $output.Trim()
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

function Observe-Worker([string]$InstanceId) {
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'cd /opt/durable-agent-execution-engine',
        'cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -q worker)',
        'test -n "$cid"',
        'docker inspect --format ''{{.Id}}|{{.State.Status}}|{{.State.Running}}|{{.State.Pid}}'' "$cid"'
    )
    return $output.Trim()
}

function Assert-OriginalWorkerPreserved([object]$Before, [string]$After) {
    $fields = $After.Trim() -split '\|', 4
    if ($fields.Count -ne 4) { throw "Worker observation is malformed: $After" }
    if ($fields[1] -ne 'running' -or $fields[2] -ne 'true') { throw "Original worker is not running after reconnect: $After" }
    if ($fields[0] -ne [string]$Before.original_worker_container_id) {
        throw "Worker container changed across network isolation: before=$($Before.original_worker_container_id), after=$($fields[0])"
    }
    if ([int]$fields[3] -ne [int]$Before.original_worker_pid) {
        throw "Worker process changed across network isolation: before=$($Before.original_worker_pid), after=$($fields[3])"
    }
    return [ordered]@{
        container_id = $fields[0]
        state = $fields[1]
        running = $fields[2] -eq 'true'
        host_pid = [int]$fields[3]
        same_container = $true
        same_process = $true
    }
}

function Get-InstanceDetails([string[]]$InstanceIDs) {
	$arguments = @("ec2", "describe-instances", "--instance-ids") + $InstanceIDs + @("--query", "Reservations[].Instances[].{id:InstanceId,type:InstanceType,availability_zone:Placement.AvailabilityZone,private_ip:PrivateIpAddress,launch_time_utc:LaunchTime}", "--output", "json")
	$instances = @((Invoke-Aws $arguments | ConvertFrom-Json))
	foreach ($instance in $instances) {
		$creditJson = Invoke-Aws @("ec2", "describe-instance-credit-specifications", "--instance-id", [string]$instance.id, "--query", "InstanceCreditSpecifications[0].CpuCredits", "--output", "text")
		$credits = $creditJson.Trim()
		if ($credits -eq "None" -or [string]::IsNullOrWhiteSpace($credits)) { $credits = "not_applicable" }
		$instance | Add-Member -NotePropertyName credit_specification -NotePropertyValue $credits -Force
	}
	return $instances
}

function Get-AccountIdentity() {
    return (Invoke-Aws @("sts", "get-caller-identity", "--output", "json") | ConvertFrom-Json)
}

function Get-ObligationSummary([string]$DependencyInstanceId, [string]$WorkflowID) {
    $sql = @"
SELECT
  (SELECT count(*) FROM engine.reconciliation_items WHERE workflow_id='$WorkflowID') || '|' ||
  (SELECT count(*) FROM engine.reconciliation_items WHERE workflow_id='$WorkflowID' AND status='OPEN') || '|' ||
  (SELECT count(*) FROM engine.reconciliation_items WHERE workflow_id IS NULL) || '|' ||
  (SELECT count(*) FROM engine.reconciliation_items WHERE workflow_id IS NULL AND status='OPEN') || '|' ||
  (SELECT count(*) FROM engine.reconciliation_items) || '|' ||
  (SELECT count(*) FROM engine.reconciliation_items WHERE status='OPEN') || '|' ||
  (SELECT count(*) FROM engine.outbox WHERE workflow_id='$WorkflowID') || '|' ||
  (SELECT count(*) FROM engine.outbox WHERE workflow_id='$WorkflowID' AND publish_state IN ('PENDING','CLAIMED')) || '|' ||
  (SELECT count(*) FROM engine.outbox) || '|' ||
  (SELECT count(*) FROM engine.outbox WHERE publish_state IN ('PENDING','CLAIMED')) || '|' ||
  (SELECT count(*) FROM effects.effect_records WHERE workflow_id='$WorkflowID') || '|' ||
  (SELECT COALESCE(sum(n-1),0) FROM (SELECT logical_effect_key, count(*) AS n FROM effects.effect_call_attempts WHERE workflow_id='$WorkflowID' GROUP BY logical_effect_key) duplicates);
"@
    return Convert-ObligationSummaryRow (Invoke-DbSql $DependencyInstanceId $sql).Trim()
}

function Convert-ObligationSummaryRow([string]$Row) {
    $parts = $Row -split '\|', 12
    if ($parts.Count -ne 12) { throw "Obligation summary was malformed: $Row" }
    return [ordered]@{
        workflow_reconciliation_items_total = [int]$parts[0]
        workflow_reconciliation_items_open = [int]$parts[1]
        global_reconciliation_items_total = [int]$parts[2]
        global_reconciliation_items_open = [int]$parts[3]
        database_reconciliation_items_total = [int]$parts[4]
        database_reconciliation_items_open = [int]$parts[5]
        workflow_outbox_rows = [int]$parts[6]
        workflow_outbox_pending_or_claimed = [int]$parts[7]
        database_outbox_rows = [int]$parts[8]
        database_outbox_pending_or_claimed = [int]$parts[9]
        workflow_effect_records = [int]$parts[10]
        workflow_duplicate_effect_calls = [int]$parts[11]
        observed_at_utc = [DateTime]::UtcNow.ToString('o')
        scope_note = "Reconciliation counts include workflow-scoped items for this workflow, global NULL-workflow poison obligations, and a database-wide snapshot; outbox and effect counts are reported for this workflow and outbox pending counts also database-wide."
    }
}

function Invoke-DurableChecker([string]$InstanceId, [string]$WorkflowID, [string]$OutputPath, [int64]$PreFaultRevision, [int64]$RecoveryRevision, [int64]$RecoveryEpoch, [int64]$StaleAttemptNumber) {
    $checkerFlags = "-workflow-ids '$WorkflowID' -pre-fault-revision $PreFaultRevision -recovery-revision $RecoveryRevision -recovery-epoch $RecoveryEpoch -stale-attempt-number $StaleAttemptNumber"
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'cid=$(docker compose --env-file /opt/durable-agent-execution-engine/deploy/aws/.env -f /opt/durable-agent-execution-engine/deploy/aws/app-compose.yaml ps -q runtime)',
        'test -n "$cid"',
        "docker exec `"`$cid`" /dur049-checker $checkerFlags"
    )
    $jsonLine = ($output.Trim() -split "\r?\n" | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($jsonLine)) { throw "Durable checker returned no JSON: $output" }
    $report = $jsonLine | ConvertFrom-Json
    $trace = $report.trace
    if ($null -eq $trace) { throw "Durable checker returned no trace snapshot: $output" }
    Write-Json $OutputPath $trace
    $offlineOutput = Invoke-Required "go" @(
        "run", "./cmd/dur049-checker", "-snapshot", $OutputPath,
        "-pre-fault-revision", [string]$PreFaultRevision,
        "-recovery-revision", [string]$RecoveryRevision,
        "-recovery-epoch", [string]$RecoveryEpoch,
        "-stale-attempt-number", [string]$StaleAttemptNumber
    )
    $offline = $offlineOutput | ConvertFrom-Json
    if (-not $report.valid -or -not $offline.valid) { throw "Invariant checker rejected DUR-049 durable evidence: live=$($report.valid) offline=$($offline.valid)" }
    if ($report.fence.pre_fault_revision -ne $offline.fence.pre_fault_revision -or $report.fence.recovery_revision -ne $offline.fence.recovery_revision -or $report.fence.recovery_epoch -ne $offline.fence.recovery_epoch -or $report.fence.superseded_epoch_transitions_after_recovery_boundary -ne $offline.fence.superseded_epoch_transitions_after_recovery_boundary -or $report.fence.stale_attempt_number -ne $offline.fence.stale_attempt_number -or $report.fence.stale_attempt_found -ne $offline.fence.stale_attempt_found -or $report.fence.stale_attempt_current -ne $offline.fence.stale_attempt_current -or $report.fence.stale_result_recorded -ne $offline.fence.stale_result_recorded) {
        throw "Live/offline fencing verdicts disagree: live=$($report.fence | ConvertTo-Json -Compress) offline=$($offline.fence | ConvertTo-Json -Compress)"
    }
    return [ordered]@{
        live = [ordered]@{ valid = [bool]$report.valid; violations = @($report.violations) }
        offline = [ordered]@{ valid = [bool]$offline.valid; violations = @($offline.violations) }
        fence = $offline.fence
        snapshot = [System.IO.Path]::GetFileName($OutputPath)
    }
}

function Insert-NetworkBlock([string]$AppInstanceId, [string]$DependencyInstanceId, [string]$DependencyIP, [string]$AppPrivateIP, [bool]$TerminateSessions) {
    $probe = "import socket; s=socket.create_connection(('$DependencyIP', 5432), 2); s.close(); print('reachable')"
    $workerProbe = "docker compose --env-file '$RemoteEnv' -f '$AppCompose' exec -T worker python -c `"$probe`""
    $faultCommandAt = [DateTime]::UtcNow
    $appBeforeOutput = Send-Ssm $AppInstanceId @(
        "set -e",
        "worker_cid=`$(docker compose --env-file '$RemoteEnv' -f '$AppCompose' ps -q worker)",
        'test -n "$worker_cid"',
        "runtime_cid=`$(docker compose --env-file '$RemoteEnv' -f '$AppCompose' ps -q runtime)",
        'test -n "$runtime_cid"',
        "printf 'connectivity_before='",
        $workerProbe,
        "printf 'runtime_database_before='",
        'docker exec "$runtime_cid" /dur049-db-probe -timeout 3s',
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        'worker_pid=$(docker inspect --format "{{.State.Pid}}" "$worker_cid")',
        'test "$worker_pid" -gt 0',
        "printf 'worker_container_state='",
        'docker inspect --format ''{{.Id}}|{{.State.Status}}|{{.State.Running}}|{{.State.Pid}}'' "$worker_cid"',
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
    $dependencyCommands = @(
        "set -e",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 5432 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 5432 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 9092 -j REJECT --reject-with tcp-reset",
        "sudo iptables -I INPUT -s '$AppPrivateIP' -p tcp --dport 9092 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset",
        "command -v conntrack",
        "sudo conntrack -D -s '$AppPrivateIP' || true"
    )
    if ($TerminateSessions) {
        $terminateSql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE client_addr='$AppPrivateIP' AND pid <> pg_backend_pid();"
        $terminateSqlEncoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($terminateSql))
        $dependencyCommands += "echo '$terminateSqlEncoded' | base64 -d | docker compose --env-file '$RemoteEnv' -f '$DependencyCompose' exec -T postgres psql -U '$DependencyUser' -d '$DependencyDatabase' -At -f -"
    }
    $dependencyOutput = Send-Ssm $DependencyInstanceId $dependencyCommands
    $appAfterOutput = Send-Ssm $AppInstanceId @(
        "set -e",
        "runtime_cid=`$(docker compose --env-file '$RemoteEnv' -f '$AppCompose' ps -q runtime)",
        'test -n "$runtime_cid"',
        "if $workerProbe >/tmp/dur043-connectivity-after.log 2>&1; then echo 'connectivity_after=reachable'; exit 1; else echo 'connectivity_after=blocked'; fi",
        'if docker exec "$runtime_cid" /dur049-db-probe -timeout 3s >/tmp/dur049-db-probe-after.json 2>&1; then echo "runtime_database_after=reachable"; cat /tmp/dur049-db-probe-after.json; exit 1; else grep -q ''"status":"unreachable"'' /tmp/dur049-db-probe-after.json; echo "runtime_database_after=blocked"; cat /tmp/dur049-db-probe-after.json; fi'
    )
    $output = "$appBeforeOutput`n$dependencyOutput`n$appAfterOutput"
    $faultObservedAt = [DateTime]::UtcNow
    $runtimeStateMatch = [regex]::Match($output, 'container_state=running\|true\|(?<pid>[0-9]+)')
    $workerStateMatch = [regex]::Match($output, 'worker_container_state=(?<id>[0-9a-f]{64})\|running\|true\|(?<pid>[0-9]+)')
    if ($output -notmatch "connectivity_before=reachable") { throw "Network fault precondition was not observed from the worker container: $output" }
    if ($output -notmatch "connectivity_after=blocked") { throw "Network fault was not observed from the worker container: $output" }
    if ($output -notmatch 'runtime_database_before=\{"status":"reachable"') { throw "Runtime-container PostgreSQL probe did not succeed before isolation: $output" }
    if ($output -notmatch 'runtime_database_after=blocked') { throw "Runtime-container PostgreSQL probe did not confirm isolation: $output" }
    if (-not $runtimeStateMatch.Success) { throw "Network fault was not observed with the runtime container still running: $output" }
    if (-not $workerStateMatch.Success) { throw "Network fault was not observed with the worker container still running: $output" }
    return [ordered]@{
        raw = $output.Trim()
        command_at_utc = $faultCommandAt.ToString("o")
        observed_at_utc = $faultObservedAt.ToString("o")
        network_chain = "DOCKER-USER plus dependency INPUT"
        network_match_states = @("NEW", "ESTABLISHED", "RELATED")
        established_flow_termination = "host conntrack deletion"
        dependency_session_termination = $TerminateSessions
        dependency_session_termination_method = $(if ($TerminateSessions) { "pg_terminate_backend optional variant" } else { "none; network fault only" })
        route_fault = "app-host blackhole route for dependency /32"
        dependency_source_ip = $AppPrivateIP
        connectivity_before = "reachable"
        connectivity_after = "blocked"
        runtime_container_database_probe_before = "reachable"
        runtime_container_database_probe_after = "unreachable"
        original_runtime_pid = [int]$runtimeStateMatch.Groups['pid'].Value
        original_worker_container_id = $workerStateMatch.Groups['id'].Value
        original_worker_pid = [int]$workerStateMatch.Groups['pid'].Value
        ports = $FaultPorts
    }
}

function Remove-NetworkBlock([string]$AppInstanceId, [string]$DependencyInstanceId, [string]$DependencyIP, [string]$AppPrivateIP) {
    Send-Ssm $AppInstanceId @(
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 5432 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -j REJECT --reject-with tcp-reset || true",
        "sudo iptables -D DOCKER-USER -d '$DependencyIP' -p tcp --dport 9092 -m conntrack --ctstate ESTABLISHED,RELATED -j REJECT --reject-with tcp-reset || true",
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
    $probeArguments = "-workflow-id '$WorkflowID' -partition-id $PartitionID -owner-id '$OwnerID' -epoch $Epoch"
    $probeCommand = "docker exec `"`$cid`" /dur043-stale-probe $probeArguments"
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

function Invoke-SameOwnerEpochProbe([string]$InstanceId, [string]$WorkflowID) {
    $command = "docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml run --rm --no-deps --entrypoint /dur043-stale-probe runtime -same-owner-stale-epoch -workflow-id '$WorkflowID'"
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'cd /opt/durable-agent-execution-engine',
        $command
    ) 240
    $jsonLine = ($output.Trim() -split "\r?\n" | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($jsonLine)) { throw "Same-owner stale-epoch probe returned no JSON: $output" }
    $probe = $jsonLine | ConvertFrom-Json
    if ($probe.status -ne 'PASS' -or -not $probe.same_owner -or [int64]$probe.current_epoch -le [int64]$probe.stale_epoch -or [int64]$probe.before_revision -ne [int64]$probe.after_revision) {
        throw "Same-owner stale-epoch fence control failed: $jsonLine"
    }
    return $probe
}

function Start-LockHolder([string]$InstanceId, [int]$PartitionID, [int]$DurationSeconds = 180) {
    # The runtime image is FROM scratch: it has no shell, cat, or /tmp. Have
    # the lock-holder write its marker to stdout and capture that stream in a
    # host-side log while docker exec remains attached in the background.
    $marker = "/tmp/dur049-lock-holder-$PartitionID.log"
    $runtimeLookupLine = Get-AppRuntimeComposeLookupLine
    $startHolderCommand = 'nohup docker exec "$cid" /dur049-lock-holder -partition-id ' + $PartitionID + ' -duration ' + $DurationSeconds + 's -marker /dev/stdout > ' + "'$marker'" + ' 2>&1 < /dev/null & holder_pid=$!; printf "lock_holder_pid=%s\n" "$holder_pid"'
    $output = Send-Ssm $InstanceId @(
        'set -e',
        $runtimeLookupLine,
        'test -n "$cid"',
        "rm -f '$marker'",
        $startHolderCommand
    )
    if ($output -notmatch 'lock_holder_pid=(?<pid>\d+)') { throw "Lock-holder launch did not report its host-side process ID: $output" }
    $hostProcessID = [int64]$Matches.pid
    $deadline = (Get-Date).AddSeconds(30)
    do {
        Start-Sleep -Seconds 2
        $markerOutput = Send-Ssm $InstanceId @(
            'set -e',
            "if grep -m1 'locked_at=' '$marker'; then exit 0; elif grep -q 'fatal:' '$marker'; then cat '$marker'; exit 1; else printf 'lock_holder_waiting=true\n'; fi"
        ) 30
        if (-not [string]::IsNullOrWhiteSpace($markerOutput) -and $markerOutput -match 'locked_at=') {
            $lockedAtText = ($markerOutput.Trim() -split 'locked_at=', 2)[1].Trim()
            return [ordered]@{ marker_log = $marker; host_process_id = $hostProcessID; launch_output = $output.Trim(); observed_at_utc = [DateTime]::UtcNow.ToString('o'); locked_at_utc = ([DateTimeOffset]::Parse($lockedAtText)).UtcDateTime.ToString('o'); marker_contents = $markerOutput.Trim(); duration_seconds = $DurationSeconds; marker_transport = "lock-holder stdout captured in host-side file; runtime image has no shell or /tmp" }
        }
    } while ((Get-Date) -lt $deadline)
    throw "Lock-holder's locked-at marker was not observed in the host-side log before the deadline."
}

function Observe-LockHolder([string]$InstanceId, [int64]$HostProcessID, [int]$PartitionID) {
    $runtimeLookupLine = Get-AppRuntimeComposeLookupLine
    $lockProbeCommand = Get-AppRuntimePartitionLockProbeCommand $PartitionID
    $commands = @(
        'set -e',
        $runtimeLookupLine,
        'test -n "$cid"',
        "kill -0 $HostProcessID",
        'printf "host_lock_holder_pid_alive=true\n"',
        'docker top "$cid" -eo pid,args | grep -F -- "/dur049-lock-holder"',
        $lockProbeCommand
    )
    $output = Send-Ssm $InstanceId $commands 30
    if ($output -notmatch 'host_lock_holder_pid_alive=true' -or $output -notmatch '/dur049-lock-holder') {
        throw "The lock-holder process was not observed alive in the runtime container while progress was observed: $output"
    }
    $probe = Assert-LeaseRowLockProbe $output
    return [ordered]@{ observed = $true; controller_observed_at_utc = [DateTime]::UtcNow.ToString('o'); host_process_id = $HostProcessID; output = $output.Trim(); lock_holder_binary_visible_in_container_process_list = $true; row_lock_probe = $probe; row_lock_confirmed_by_partition_id = $PartitionID }
}

function Run-LockHeldArm([string]$App1, [string]$App2, [string]$Dependency, [string]$WorkflowID, [string]$ProgressWorkflowID, [string]$OutputPath) {
    $partition = Get-PartitionID $WorkflowID
    $progressPartition = Get-PartitionID $ProgressWorkflowID
    if ($partition -eq $progressPartition) { throw "Lock-held progress workflow must map to a different partition." }
    $checkpoint = [ordered]@{ schema_version = "dur049-episode.v1"; status = "IN_PROGRESS"; fault = "lease-row-lock-held-beyond-expiry"; workflow_id = $WorkflowID; progress_workflow_id = $ProgressWorkflowID; partition_id = $partition; progress_partition_id = $progressPartition; source_commit = (git rev-parse HEAD).Trim(); started_at_utc = [DateTime]::UtcNow.ToString("o") }
    Save-EpisodeCheckpoint $OutputPath $checkpoint "starting"
    $script:App2MayBeStopped = $true
    $app2Stopped = Stop-AppRuntime $App2
    Set-AppMode $App1 $FixtureDelayMS $ObservationHoldMS
    $app1Configuration = Get-AppConfiguration $App1
    [void](Submit-FixtureWorkflow $App1 $WorkflowID)
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID
    $oldLease = $claimed.scheduler_acquisition
    $before = $claimed.workflow
    $oldAttempt = $claimed.attempt
    if ($before.attempt_state -ne "CLAIMED") { throw "Lock-held fixture must be observed CLAIMED before the lock fault." }
    $checkpoint.original_owner_id = $oldLease.owner_id
    $checkpoint.original_epoch = $oldLease.epoch
    $checkpoint.workflow_before = $before
    $checkpoint.attempt_before = $oldAttempt
    $checkpoint.app2_stopped_before_owner_capture = $app2Stopped
    Save-EpisodeCheckpoint $OutputPath $checkpoint "claimed"
    $epochBoundary = Get-LeaseEpochBoundary $Dependency $partition
    $lockHolder = Start-LockHolder $App1 $partition $LockHolderDurationSeconds
    $lockObservedAt = $lockHolder.observed_at_utc
    $checkpoint.lock_holder = $lockHolder
    Save-EpisodeCheckpoint $OutputPath $checkpoint "lease-row-lock-observed"
    Set-AppEnvironment $App2 1000 $TakeoverObservationHoldMS
    Start-AppRuntimeNoWait $App2
    $script:App2MayBeStopped = $false
    $app2Configuration = Get-AppConfiguration $App2
    Wait-AppApi $App2
    [void](Submit-FixtureWorkflow $App2 $ProgressWorkflowID)
    $progress = Wait-Workflow $Dependency $ProgressWorkflowID 120
    $progressObservedAt = [DateTime]::UtcNow
    if ($progress.state -ne "SUCCEEDED") { throw "Other partition did not complete successfully while lock-held partition was contended: $($progress.state)." }
    $progressResults = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$ProgressWorkflowId' AND reason='RESULT_CONSUMED';")
    if ($progressResults -lt 1) { throw "Other partition reached SUCCEEDED without a durable consumed result while lock-held partition was contended." }
    $progressLatency = DurationMilliseconds $lockObservedAt $progressObservedAt
    if ($progressLatency -ge ($lockHolder.duration_seconds * 1000)) { throw "Other-partition progress was not observed before the lock-holder's bounded hold elapsed: ${progressLatency}ms." }
    $lockHolderObservedDuringProgress = Observe-LockHolder $App1 $lockHolder.host_process_id $partition
    $checkpoint.other_partition_progress = $progress
    $checkpoint.other_partition_progress_observed_at_utc = $progressObservedAt.ToString("o")
    $checkpoint.other_partition_progress_ms = $progressLatency
    $checkpoint.lock_holder_observed_during_other_partition_progress = $lockHolderObservedDuringProgress
    Save-EpisodeCheckpoint $OutputPath $checkpoint "other-partition-progress-confirmed"
    $takeover = Wait-FirstNewOwnerAcquisition $Dependency $partition $oldLease.owner_id $epochBoundary $lockObservedAt 150
    $checkpoint.first_new_owner_acquisition = $takeover
    Save-EpisodeCheckpoint $OutputPath $checkpoint "new-owner-observed"
    $usefulProgress = Wait-LockHeldResultConsumption $Dependency $WorkflowID $oldAttempt.attempt_number $before.revision $epochBoundary $takeover 180
    $orderedProgress = Assert-LockHeldResultOrdering $takeover $usefulProgress
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch
    $checkpoint.first_useful_progress = $usefulProgress
    $checkpoint.stale_owner_probe = $staleProbe
    Save-EpisodeCheckpoint $OutputPath $checkpoint "same-attempt-result-consumed-and-stale-rejection-observed"
    $terminal = Wait-Workflow $Dependency $WorkflowID 180
    if ($terminal.state -ne "SUCCEEDED") { throw "Lock-held workflow did not recover to SUCCEEDED: $($terminal.state)" }
    $recoveryRevision = [int64]$usefulProgress.transition.revision
    $recoveryEpoch = [int64]$usefulProgress.transition.scheduler_epoch
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND revision > $recoveryRevision AND scheduler_epoch IS NOT NULL AND scheduler_epoch < $recoveryEpoch;")
    if ($historyCount -ne 0) { throw "A superseded epoch transition was committed after the durable recovery boundary: $historyCount" }
    $acquisitionSummary = @(Get-LeaseAcquisitionSummary $Dependency $partition $epochBoundary)
    $attempts = @(Get-AttemptSummary $Dependency $WorkflowID)
    $obligations = Get-ObligationSummary $Dependency $WorkflowID
    $snapshotPath = Join-Path (Split-Path -Parent $OutputPath) "lock-held-durable-trace.json"
    # This arm's original attempt legitimately completed; it has no stale
    # attempt to assert absent. The independent stale-identity probe is
    # recorded separately above, while the checker validates the durable
    # transition prefix and superseded epochs.
    $checker = Invoke-DurableChecker $App1 $WorkflowID $snapshotPath $before.revision $recoveryRevision $recoveryEpoch 0
    $result = [ordered]@{
        fault = "lease-row-lock-held-beyond-expiry"
        workflow_id = $WorkflowID
        partition_id = $partition
        progress_workflow_id = $ProgressWorkflowID
        progress_partition_id = $progressPartition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        pre_fault_lease_acquisition = $oldLease
        pre_fault_epoch_boundary = $epochBoundary
        app2_stopped_before_owner_capture = $app2Stopped
        lock_holder = $lockHolder
        first_new_owner_acquisition = [ordered]@{ acquisition_id = $takeover.acquisition_id; owner_id = $takeover.owner_id; epoch = $takeover.epoch; database_recorded_at_utc = $takeover.database_recorded_at_utc; controller_observed_at_utc = $takeover.controller_observed_at_utc; lease_expires_at_utc = $takeover.lease_expires_at_utc }
        recovery_transition_epoch = $recoveryEpoch
        recovery_transition_revision = $recoveryRevision
        recovery_transition_is_durably_after_first_new_owner = $orderedProgress.result_consumed_under_first_new_owner_acquisition
        progress_while_lock_held = $progress
        other_partition_progress_observed_at_utc = $progressObservedAt.ToString("o")
        lock_observed_to_other_partition_progress_ms = $progressLatency
        lock_holder_observed_during_other_partition_progress = $lockHolderObservedDuringProgress
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ definition = $usefulProgress.progress_definition; controller_observed_at_utc = $usefulProgress.controller_observed_at_utc; result_receipt_transition = $usefulProgress.result_receipt_transition; transition = $usefulProgress.transition; workflow = $usefulProgress.workflow }
        first_useful_progress_transition = $usefulProgress.transition
        recovery_ordering = $orderedProgress
        superseded_epoch_transitions_after_recovery_boundary = $historyCount
        deployed_stale_identity_smoke_test = $staleProbe
        attempts = $attempts
        lease_acquisition_summary_after_pre_fault_epoch_boundary = $acquisitionSummary
        obligations = $obligations
        durable_invariant_checker = $checker
        lock_observed_to_first_new_owner_acquisition_observed_ms = DurationMilliseconds $lockObservedAt $takeover.observed_at
        first_new_owner_acquisition_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $takeover.observed_at $usefulProgress.observed_at
        timestamp_semantics = "Intervals use controller UTC observation times after 2-second polling; DB timestamps are transaction-recorded diagnostics, not commit times."
        recovery_timing_note = "This is the lock-held isolation arm. The lease fence remains row-locked; the bounded lock timeout lets other partitions progress while the lock holder retains the target row. The original worker durably records its result, and the first new owner consumes that same attempt after the lock is released; this run does not demonstrate a replacement attempt. App2 is a controller-started standby, not a continuously active peer. Report separately from pure network isolation and do not treat this as peer failure-detection latency."
        configuration = [ordered]@{ activity = "dur048.sleep"; app1 = $app1Configuration; app2 = $app2Configuration; lock_progress_activity_delay_ms = 1000; lock_progress_observation_timeout_seconds = 120; lock_holder_duration_seconds = $LockHolderDurationSeconds; lock_timeout_seconds = 2; attempt_lease_seconds = 30 }
    }
    $checkpoint.status = "PASS"
    $checkpoint.result = $result
    Save-EpisodeCheckpoint $OutputPath $checkpoint "complete"
    $script:CurrentEpisodePath = $null
    $script:CurrentEpisodeState = $null
    return $result
}

function Run-NetworkArm([string]$App1, [string]$App2, [string]$Dependency, [string]$DependencyIP, [string]$AppPrivateIP, [string]$WorkflowID, [string]$OutputPath) {
    $partition = Get-PartitionID $WorkflowID
    $checkpoint = [ordered]@{ schema_version = "dur049-episode.v1"; status = "IN_PROGRESS"; fault = "app-host-postgres-network-isolation-and-reconnect"; workflow_id = $WorkflowID; partition_id = $partition; source_commit = (git rev-parse HEAD).Trim(); started_at_utc = [DateTime]::UtcNow.ToString("o") }
    Save-EpisodeCheckpoint $OutputPath $checkpoint "starting"
    $script:App2MayBeStopped = $true
    $app2Stopped = Stop-AppRuntime $App2
    Set-AppMode $App1 $FixtureDelayMS $ObservationHoldMS
    $app1Configuration = Get-AppConfiguration $App1
    [void](Submit-FixtureWorkflow $App1 $WorkflowID)
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID
    $oldLease = $claimed.scheduler_acquisition
    $before = $claimed.workflow
    $oldAttempt = $claimed.attempt
    if ([string]::IsNullOrWhiteSpace($oldLease.owner_id)) { throw "Application host 1 did not own partition $partition before network isolation." }
    if ($before.attempt_state -ne "CLAIMED") { throw "Network fixture must be observed CLAIMED before isolation; observed $($before.attempt_state)." }
    $checkpoint.original_owner_id = $oldLease.owner_id
    $checkpoint.original_epoch = $oldLease.epoch
    $checkpoint.workflow_before = $before
    $checkpoint.attempt_before = $oldAttempt
    $checkpoint.app2_stopped_before_owner_capture = $app2Stopped
    Save-EpisodeCheckpoint $OutputPath $checkpoint "claimed"
    $epochBoundary = Get-LeaseEpochBoundary $Dependency $partition
    $checkpoint.pre_fault_epoch_boundary = $epochBoundary
    $script:NetworkRulesInserted = $true
    $faultObserved = Insert-NetworkBlock $App1 $Dependency $DependencyIP $AppPrivateIP ([bool]$TerminateDatabaseSessions)
    $checkpoint.fault_observed = $faultObserved
    Save-EpisodeCheckpoint $OutputPath $checkpoint "network-isolation-confirmed"
    Set-AppEnvironment $App2 $FixtureDelayMS $TakeoverObservationHoldMS
    Start-AppRuntimeNoWait $App2
    $script:App2MayBeStopped = $false
    $app2Configuration = Get-AppConfiguration $App2
    $takeover = Wait-FirstNewOwnerAcquisition $Dependency $partition $oldLease.owner_id $epochBoundary $faultObserved.observed_at_utc
    $checkpoint.first_new_owner_acquisition = $takeover
    Save-EpisodeCheckpoint $OutputPath $checkpoint "new-owner-observed"
    # Keep the original host isolated until the peer has durably timed out the
    # old claim and accepted a result for its replacement attempt. Reconnecting
    # immediately after lease acquisition can let an unfinished, still-current
    # attempt report first; that is not evidence of recovery from stale work.
    $usefulProgress = Wait-FirstUsefulRecoveryProgress $Dependency $WorkflowID $before.attempt_number $before.revision $takeover.owner_id $epochBoundary 240
    $orderedProgress = Assert-AcquisitionPrecedesProgress $takeover $usefulProgress
    $checkpoint.first_useful_progress = $usefulProgress
    Save-EpisodeCheckpoint $OutputPath $checkpoint "replacement-result-observed-while-original-isolated"
    Remove-NetworkBlock $App1 $Dependency $DependencyIP $AppPrivateIP
    $script:NetworkRulesInserted = $false
    $reconnected = Observe-Runtime $App1
    $reconnectedParts = $reconnected -split '\|', 3
    if ($reconnectedParts.Count -ne 3 -or $reconnectedParts[0] -ne 'running' -or $reconnectedParts[1] -ne 'true' -or [int]$reconnectedParts[2] -ne $faultObserved.original_runtime_pid) {
        throw "The original runtime process did not remain alive across network isolation: before=$($faultObserved.original_runtime_pid), after=$reconnected"
    }
    $workerReconnected = Observe-Worker $App1
    $workerPreserved = Assert-OriginalWorkerPreserved $faultObserved $workerReconnected
    $lateResultObservation = Wait-WorkerStaleResultObservation $App1 $WorkflowID $oldAttempt
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch
    $checkpoint.original_runtime_observed_after_reconnect = $reconnected
    $checkpoint.original_worker_observed_after_reconnect = $workerPreserved
    $checkpoint.original_worker_late_result_rejection = $lateResultObservation
    $checkpoint.deployed_stale_identity_smoke_test = $staleProbe
    $checkpoint.first_useful_progress = $usefulProgress
    Save-EpisodeCheckpoint $OutputPath $checkpoint "reconnected-stale-rejection-and-progress-observed"
    $terminal = Wait-Workflow $Dependency $WorkflowID 150
    $terminalObservedAt = [DateTime]::UtcNow
    if ($terminal.state -ne "SUCCEEDED") { throw "Network-isolated workflow did not recover to SUCCEEDED: $($terminal.state)" }
    # Count only scheduler transitions after the durable recovery boundary.
    # Pre-fault pass epochs are expected to be lower and are not violations.
    $recoveryRevision = [int64]$usefulProgress.timeout_replacement_transition.revision
    $recoveryEpoch = [int64]$usefulProgress.timeout_replacement_transition.scheduler_epoch
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND revision > $recoveryRevision AND scheduler_epoch IS NOT NULL AND scheduler_epoch < $recoveryEpoch;")
    if ($historyCount -ne 0) { throw "A scheduler transition with an epoch older than recovery was committed after the recovery boundary: $historyCount" }
    $acquisitionSummary = @(Get-LeaseAcquisitionSummary $Dependency $partition $epochBoundary)
    $attempts = @(Get-AttemptSummary $Dependency $WorkflowID)
    $obligations = Get-ObligationSummary $Dependency $WorkflowID
    $snapshotPath = Join-Path (Split-Path -Parent $OutputPath) "network-durable-trace.json"
    $checker = Invoke-DurableChecker $App1 $WorkflowID $snapshotPath $before.revision $recoveryRevision $recoveryEpoch $oldAttempt.attempt_number
    $result = [ordered]@{
        fault = "app-host-postgres-network-isolation-and-reconnect"
        workflow_id = $WorkflowID
        partition_id = $partition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        pre_fault_lease_acquisition = $oldLease
        pre_fault_epoch_boundary = $epochBoundary
        app2_stopped_before_owner_capture = $app2Stopped
        first_new_owner_acquisition = [ordered]@{ acquisition_id = $takeover.acquisition_id; owner_id = $takeover.owner_id; epoch = $takeover.epoch; database_recorded_at_utc = $takeover.database_recorded_at_utc; controller_observed_at_utc = $takeover.controller_observed_at_utc; lease_expires_at_utc = $takeover.lease_expires_at_utc }
        recovery_transition_epoch = $recoveryEpoch
        recovery_transition_revision = $recoveryRevision
        recovery_transition_is_durably_after_first_new_owner = $orderedProgress.durable_replacement_precedes_accepted_result
        fault_observed = $faultObserved
        original_runtime_observed = $reconnected
        original_worker_observed_after_reconnect = $workerPreserved
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ definition = $usefulProgress.progress_definition; controller_observed_at_utc = $usefulProgress.controller_observed_at_utc; workflow = $usefulProgress.workflow }
        recovery_ordering = $orderedProgress
        superseded_epoch_transitions_after_recovery_boundary = $historyCount
        original_worker_late_result_rejection = $lateResultObservation
        deployed_stale_identity_smoke_test = $staleProbe
        attempts = $attempts
        lease_acquisition_summary_after_pre_fault_epoch_boundary = $acquisitionSummary
        obligations = $obligations
        durable_invariant_checker = $checker
        terminal_observed_at_utc = $terminalObservedAt.ToString("o")
        fault_command_to_observed_ms = DurationMilliseconds $faultObserved.command_at_utc $faultObserved.observed_at_utc
        fault_observed_to_first_new_owner_acquisition_observed_ms = DurationMilliseconds $faultObserved.observed_at_utc $takeover.observed_at
        fault_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $faultObserved.observed_at_utc $usefulProgress.observed_at
        first_new_owner_acquisition_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $takeover.observed_at $usefulProgress.observed_at
        timestamp_semantics = "Intervals use controller UTC observation times after 2-second polling; DB timestamps are transaction-recorded diagnostics, not commit times."
        first_useful_progress_transition = $usefulProgress.transition
        fault_observed_to_terminal_observed_ms = DurationMilliseconds $faultObserved.observed_at_utc $terminalObservedAt
        configuration = [ordered]@{ activity = "dur048.sleep"; app1 = $app1Configuration; app2 = $app2Configuration; fault_network_ports = $FaultPorts; network_chain = "DOCKER-USER plus dependency INPUT"; network_match_states = @("NEW", "ESTABLISHED", "RELATED"); established_flow_termination = "host conntrack deletion"; session_termination_enabled = [bool]$TerminateDatabaseSessions; session_termination_method = $(if ($TerminateDatabaseSessions) { "pg_terminate_backend optional variant" } else { "none; network-only arm" }); route_fault = "app-host blackhole route for dependency /32"; dependency_source_ip = $AppPrivateIP }
        peer_mode = "controller-started cold standby; app2 runtime is started after confirmed isolation, not a continuously active peer"
        recovery_timing_note = "Useful progress follows the 30-second AttemptLease and activity execution; these measurements are bounded recovery observations for this configuration, not general failover performance."
        limitation = "This arm isolates the app host from PostgreSQL while preserving the original runtime and worker processes; it is not a host-stop or database-host durability claim. The stale worker result rejection is observed from the original worker's correlated log, while the separate scheduler identity probe is only a deployed fence smoke test."
    }
    $checkpoint.status = "PASS"
    $checkpoint.result = $result
    Save-EpisodeCheckpoint $OutputPath $checkpoint "complete"
    $script:CurrentEpisodePath = $null
    $script:CurrentEpisodeState = $null
    return $result
}

function Run-HostArm([string]$App1, [string]$App2, [string]$Dependency, [string]$WorkflowID, [string]$OutputPath) {
    $partition = Get-PartitionID $WorkflowID
    $checkpoint = [ordered]@{ schema_version = "dur049-episode.v1"; status = "IN_PROGRESS"; fault = "application-host-forced-stop-and-restart"; workflow_id = $WorkflowID; partition_id = $partition; source_commit = (git rev-parse HEAD).Trim(); started_at_utc = [DateTime]::UtcNow.ToString("o") }
    Save-EpisodeCheckpoint $OutputPath $checkpoint "starting"
    $script:App2MayBeStopped = $true
    $app2Stopped = Stop-AppRuntime $App2
    Set-AppMode $App1 $FixtureDelayMS $ObservationHoldMS
    $app1Configuration = Get-AppConfiguration $App1
    [void](Submit-FixtureWorkflow $App1 $WorkflowID)
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID
    $oldLease = $claimed.scheduler_acquisition
    $before = $claimed.workflow
    $oldAttempt = $claimed.attempt
    if ([string]::IsNullOrWhiteSpace($oldLease.owner_id)) { throw "Application host 1 did not own partition $partition before host stop." }
    if ($before.attempt_state -ne "CLAIMED") { throw "Host-stop fixture must be observed CLAIMED before the stop; observed $($before.attempt_state)." }
    $checkpoint.original_owner_id = $oldLease.owner_id
    $checkpoint.original_epoch = $oldLease.epoch
    $checkpoint.workflow_before = $before
    $checkpoint.attempt_before = $oldAttempt
    $checkpoint.app2_stopped_before_owner_capture = $app2Stopped
    $epochBoundary = Get-LeaseEpochBoundary $Dependency $partition
    $checkpoint.pre_fault_epoch_boundary = $epochBoundary
    Save-EpisodeCheckpoint $OutputPath $checkpoint "claimed"
    $stopRequestedAt = [DateTime]::UtcNow
    $script:App1WasStopped = $true
    Invoke-Aws @("ec2", "stop-instances", "--instance-ids", $App1, "--force") | Out-Null
    $deadline = (Get-Date).AddMinutes(3)
    do {
        Start-Sleep -Seconds 5
        $state = (Invoke-Aws @("ec2", "describe-instances", "--instance-ids", $App1, "--query", "Reservations[0].Instances[0].State.Name", "--output", "text")).Trim()
    } while ($state -ne "stopped" -and (Get-Date) -lt $deadline)
    if ($state -ne "stopped") { throw "The application host did not reach stopped state after forced stop: $state" }
    $stoppedAt = [DateTime]::UtcNow
    $checkpoint.host_stopped_at_utc = $stoppedAt.ToString("o")
    Save-EpisodeCheckpoint $OutputPath $checkpoint "host-stop-confirmed"
    Set-AppEnvironment $App2 $FixtureDelayMS $TakeoverObservationHoldMS
    Start-AppRuntimeNoWait $App2
    $script:App2MayBeStopped = $false
    $app2Configuration = Get-AppConfiguration $App2
    $takeover = Wait-FirstNewOwnerAcquisition $Dependency $partition $oldLease.owner_id $epochBoundary $stoppedAt
    $checkpoint.first_new_owner_acquisition = $takeover
    Save-EpisodeCheckpoint $OutputPath $checkpoint "new-owner-observed"
    $usefulProgress = Wait-FirstUsefulRecoveryProgress $Dependency $WorkflowID $before.attempt_number $before.revision $takeover.owner_id $epochBoundary 180
    $orderedProgress = Assert-AcquisitionPrecedesProgress $takeover $usefulProgress
    $checkpoint.first_useful_progress = $usefulProgress
    Save-EpisodeCheckpoint $OutputPath $checkpoint "replacement-result-observed"
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
    $restartedRuntimeObserved = Observe-Runtime $App1
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch
    $checkpoint.restarted_runtime_observed_after_host_stop = $restartedRuntimeObserved
    $checkpoint.deployed_stale_identity_smoke_test = $staleProbe
    Save-EpisodeCheckpoint $OutputPath $checkpoint "restart-and-stale-rejection-observed"
    $terminal = Wait-Workflow $Dependency $WorkflowID 180
    $terminalObservedAt = [DateTime]::UtcNow
    if ($terminal.state -ne "SUCCEEDED") { throw "Host-stop workflow did not recover to SUCCEEDED: $($terminal.state)" }
    $recoveryRevision = [int64]$usefulProgress.timeout_replacement_transition.revision
    $recoveryEpoch = [int64]$usefulProgress.timeout_replacement_transition.scheduler_epoch
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND revision > $recoveryRevision AND scheduler_epoch IS NOT NULL AND scheduler_epoch < $recoveryEpoch;")
    if ($historyCount -ne 0) { throw "A scheduler transition with an epoch older than recovery was committed after the recovery boundary: $historyCount" }
    $acquisitionSummary = @(Get-LeaseAcquisitionSummary $Dependency $partition $epochBoundary)
    $attempts = @(Get-AttemptSummary $Dependency $WorkflowID)
    $obligations = Get-ObligationSummary $Dependency $WorkflowID
    $snapshotPath = Join-Path (Split-Path -Parent $OutputPath) "host-durable-trace.json"
    $checker = Invoke-DurableChecker $App1 $WorkflowID $snapshotPath $before.revision $recoveryRevision $recoveryEpoch $oldAttempt.attempt_number
    $result = [ordered]@{
        fault = "application-host-forced-stop-and-restart"
        workflow_id = $WorkflowID
        partition_id = $partition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        pre_fault_lease_acquisition = $oldLease
        pre_fault_epoch_boundary = $epochBoundary
        app2_stopped_before_owner_capture = $app2Stopped
        first_new_owner_acquisition = [ordered]@{ acquisition_id = $takeover.acquisition_id; owner_id = $takeover.owner_id; epoch = $takeover.epoch; database_recorded_at_utc = $takeover.database_recorded_at_utc; controller_observed_at_utc = $takeover.controller_observed_at_utc; lease_expires_at_utc = $takeover.lease_expires_at_utc }
        recovery_transition_epoch = $recoveryEpoch
        recovery_transition_revision = $recoveryRevision
        recovery_transition_is_durably_after_first_new_owner = $orderedProgress.durable_replacement_precedes_accepted_result
        fault_observed = [ordered]@{ stop_requested_at_utc = $stopRequestedAt.ToString("o"); stopped_at_utc = $stoppedAt.ToString("o"); stopped_state = $true; restart_requested_at_utc = $restartRequestedAt.ToString("o"); running_observed_at_utc = $runningObservedAt.ToString("o"); ssm_online_after_restart = $true; restarted_runtime_observed = $restartedRuntimeObserved }
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ definition = $usefulProgress.progress_definition; controller_observed_at_utc = $usefulProgress.controller_observed_at_utc; workflow = $usefulProgress.workflow }
        recovery_ordering = $orderedProgress
        superseded_epoch_transitions_after_recovery_boundary = $historyCount
        original_worker_late_result_observation = [ordered]@{ expected = $false; reason = "The forced EC2 host stop terminated the original worker process before it could submit a late result." }
        deployed_stale_identity_smoke_test = $staleProbe
        attempts = $attempts
        lease_acquisition_summary_after_pre_fault_epoch_boundary = $acquisitionSummary
        obligations = $obligations
        durable_invariant_checker = $checker
        terminal_observed_at_utc = $terminalObservedAt.ToString("o")
        stop_requested_to_running_ms = DurationMilliseconds $stopRequestedAt $runningObservedAt
        stop_observed_to_first_new_owner_acquisition_observed_ms = DurationMilliseconds $stoppedAt $takeover.observed_at
        stop_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $stoppedAt $usefulProgress.observed_at
        first_new_owner_acquisition_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $takeover.observed_at $usefulProgress.observed_at
        timestamp_semantics = "Intervals use controller UTC observation times after 2-second polling; DB timestamps are transaction-recorded diagnostics, not commit times."
        first_useful_progress_transition = $usefulProgress.transition
        peer_mode = "controller-started cold standby; app2 runtime is started after the host is observed stopped, not a continuously active peer"
        recovery_timing_note = "App2 is a controller-started standby, not a continuously active peer. Useful progress is bounded by the 30-second AttemptLease plus cold standby activation; it is not a host failover performance claim."
        configuration = [ordered]@{ activity = "dur048.sleep"; app1 = $app1Configuration; app2 = $app2Configuration; fault_network_ports = $FaultPorts; network_chain = "not_applicable_host_stop"; network_match_states = @("not_applicable") }
        limitation = "Host-stop recovery is measured separately from the preserved-process network arm; database-host durability remains out of scope."
    }
    $checkpoint.status = "PASS"
    $checkpoint.result = $result
    Save-EpisodeCheckpoint $OutputPath $checkpoint "complete"
    $script:CurrentEpisodePath = $null
    $script:CurrentEpisodeState = $null
    return $result
}

if ($SelfTest) {
    if ($LockHolderDurationSeconds -lt 150) { throw "Lock-holder duration must leave margin over measured cold-standby progress latency." }
    $composeLookup = Get-AppRuntimeComposeLookupCommand
    $composeLookupLine = Get-AppRuntimeComposeLookupLine
    $lockProbeCommand = Get-AppRuntimePartitionLockProbeCommand 13
    $markerLogPath = "/tmp/dur049-lock-holder-13.log"
    $startHolderCommand = 'nohup docker exec "$cid" /dur049-lock-holder -partition-id 13 -duration 180s -marker /dev/stdout > ' + "'$markerLogPath'" + ' 2>&1 < /dev/null & holder_pid=$!; printf "lock_holder_pid=%s\n" "$holder_pid"'
    $markerPollCommand = "if grep -m1 'locked_at=' '$markerLogPath'; then exit 0; elif grep -q 'fatal:' '$markerLogPath'; then cat '$markerLogPath'; exit 1; else printf 'lock_holder_waiting=true\n'; fi"
    if ($composeLookup -notmatch [regex]::Escape("--env-file '$RemoteEnv' -f '$AppCompose' ps -q runtime")) {
        throw "Lock-holder compose lookup must specify the environment file and compose file independently: $composeLookup"
    }
    if ($composeLookup -match "--env-file '$AppCompose'") {
        throw "Lock-holder compose lookup incorrectly treats app-compose.yaml as its environment file: $composeLookup"
    }
    if ($composeLookupLine -ne "cid=`$($composeLookup)") { throw "Lock-holder remote lookup shell line is malformed: $composeLookupLine" }
    if ($lockProbeCommand -notmatch 'docker exec "\$cid" /dur049-db-probe -partition-id 13' -or $lockProbeCommand -notmatch 'lock_probe_json=') {
        throw "The lock-held arm must verify the actual PostgreSQL row lock from inside the runtime container."
    }
    $positiveLockProbe = Assert-LeaseRowLockProbe 'lock_probe_json={"status":"row_lock_held","sqlstate":"55P03"}'
    if ($positiveLockProbe.status -ne 'row_lock_held' -or $positiveLockProbe.sqlstate -ne '55P03') { throw "Lock-held probe positive control was not retained." }
    $availableLockRejected = $false
    try { $null = Assert-LeaseRowLockProbe 'lock_probe_json={"status":"row_lock_available"}' } catch { $availableLockRejected = $true }
    if (-not $availableLockRejected) { throw "Lock-held probe accepted a free lease row." }
    if ($startHolderCommand -notmatch '-marker /dev/stdout' -or $startHolderCommand -notmatch 'nohup docker exec' -or $markerPollCommand -notmatch "grep -m1 'locked_at='") {
        throw "Lock-holder marker must be captured and polled on the host, not by shell utilities inside the scratch runtime container."
    }
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
    $testAcquisition = [ordered]@{ acquisition_id = "acq-17"; owner_id = "owner-new"; epoch = 17 }
    $testProgress = [ordered]@{
        timeout_replacement_transition = [ordered]@{ revision = 8; scheduler_epoch = 18; lease_acquisition_id = "acq-18"; lease_owner_id = "owner-new"; prior_attempt_state = "TIMED_OUT"; prior_attempt_is_current = $false }
        transition = [ordered]@{ revision = 9; scheduler_epoch = $null; reason = "ATTEMPT_RESULT_RECORDED"; attempt_number = 2 }
    }
    $ordering = Assert-AcquisitionPrecedesProgress $testAcquisition $testProgress
    if (-not $ordering.durable_replacement_precedes_accepted_result) { throw "recovery-order self-test did not return a positive result" }
    $wrongOwnerProgress = [ordered]@{
        timeout_replacement_transition = [ordered]@{ revision = 8; scheduler_epoch = 18; lease_acquisition_id = "acq-18"; lease_owner_id = "other-owner"; prior_attempt_state = "TIMED_OUT"; prior_attempt_is_current = $false }
        transition = [ordered]@{ revision = 9; scheduler_epoch = $null; reason = "ATTEMPT_RESULT_RECORDED"; attempt_number = 2 }
    }
    $wrongOwnerRejected = $false
    try { $null = Assert-AcquisitionPrecedesProgress $testAcquisition $wrongOwnerProgress } catch { $wrongOwnerRejected = $true }
    if (-not $wrongOwnerRejected) { throw "recovery-order self-test accepted a replacement from a different owner" }
    $reversedProgress = [ordered]@{
        timeout_replacement_transition = [ordered]@{ revision = 10; scheduler_epoch = 18; lease_acquisition_id = "acq-18"; lease_owner_id = "owner-new"; prior_attempt_state = "TIMED_OUT"; prior_attempt_is_current = $false }
        transition = [ordered]@{ revision = 9; scheduler_epoch = $null; reason = "ATTEMPT_RESULT_RECORDED"; attempt_number = 2 }
    }
    $reversedRejected = $false
    try { $null = Assert-AcquisitionPrecedesProgress $testAcquisition $reversedProgress } catch { $reversedRejected = $true }
    if (-not $reversedRejected) { throw "recovery-order self-test accepted a result before its replacement transition" }
    $lockHeldProgress = [ordered]@{
        result_receipt_transition = [ordered]@{ revision = 4; reason = 'ATTEMPT_RESULT_RECORDED'; actor_kind = 'worker'; attempt_number = 1 }
        transition = [ordered]@{ revision = 5; reason = 'RESULT_CONSUMED'; actor_kind = 'scheduler'; attempt_number = 1; scheduler_epoch = 17; lease_epoch = 17; lease_acquisition_id = 'acq-17'; lease_owner_id = 'owner-new' }
    }
    $lockHeldOrdering = Assert-LockHeldResultOrdering $testAcquisition $lockHeldProgress
    if (-not $lockHeldOrdering.same_original_attempt_completed_without_replacement) { throw 'Lock-held same-attempt recovery self-test did not return a positive result.' }
    $wrongLockHeldOrderingRejected = $false
    $lockHeldProgress.transition.revision = 3
    try { $null = Assert-LockHeldResultOrdering $testAcquisition $lockHeldProgress } catch { $wrongLockHeldOrderingRejected = $true }
    if (-not $wrongLockHeldOrderingRejected) { throw 'Lock-held recovery self-test accepted consumption before the result receipt.' }
    $lockHeldProgress.transition.revision = 5
    $lockHeldProgress.transition.lease_acquisition_id = 'acq-wrong'
    $wrongAcquisitionRejected = $false
    try { $null = Assert-LockHeldResultOrdering $testAcquisition $lockHeldProgress } catch { $wrongAcquisitionRejected = $true }
    if (-not $wrongAcquisitionRejected) { throw 'Lock-held recovery self-test accepted consumption under a different acquisition.' }
    $workerBefore = [ordered]@{ original_worker_container_id = ('a' * 64); original_worker_pid = 1234 }
    $workerAfter = ('a' * 64) + '|running|true|1234'
    $workerPreserved = Assert-OriginalWorkerPreserved $workerBefore $workerAfter
    if (-not $workerPreserved.same_container -or -not $workerPreserved.same_process) { throw "worker identity self-test did not confirm the original container/process" }
    $workerReplacementRejected = $false
    try { $null = Assert-OriginalWorkerPreserved $workerBefore (('b' * 64) + '|running|true|1234') } catch { $workerReplacementRejected = $true }
    if (-not $workerReplacementRejected) { throw "worker identity self-test accepted a replacement container" }
    $workerRestartRejected = $false
    try { $null = Assert-OriginalWorkerPreserved $workerBefore (('a' * 64) + '|running|true|5678') } catch { $workerRestartRejected = $true }
    if (-not $workerRestartRejected) { throw "worker identity self-test accepted a restarted worker process" }
    $staleLogAttempt = [ordered]@{ node_id = 'dur048.sleep'; attempt_number = 7; worker_id = 'worker-1' }
    $staleResultLog = 'delivery control rejected workflow_id=wf-stale node_id=dur048.sleep attempt_number=7 worker_id=worker-1 operation=result code=STALE_ATTEMPT'
    $staleHeartbeatLog = 'delivery control rejected workflow_id=wf-stale node_id=dur048.sleep attempt_number=7 worker_id=worker-1 operation=heartbeat code=STALE_ATTEMPT'
    $resultSubmissionLog = 'activity result submission workflow_id=wf-stale node_id=dur048.sleep iteration=0 attempt_number=7 worker_id=worker-1 attempt_state=SUCCEEDED payload_sha256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef outcome=rejected status=409 code=STALE_ATTEMPT retries=0'
    $resultSubmissionRetriedLog = $resultSubmissionLog.Replace('retries=0', 'retries=3')
    $genericLeaseLog = 'scheduler lease observation hold enabled'
    if (-not (Test-WorkerStaleResultLog $staleResultLog 'wf-stale' $staleLogAttempt)) { throw 'stale-result log self-test rejected the positive result-operation control' }
    if (Test-WorkerStaleResultLog $staleHeartbeatLog 'wf-stale' $staleLogAttempt) { throw 'stale-result log self-test accepted a heartbeat rejection as a result rejection' }
    if (Test-WorkerStaleResultLog $genericLeaseLog 'wf-stale' $staleLogAttempt) { throw 'stale-result log self-test accepted an unrelated lease log line' }
    if (-not (Test-WorkerResultSubmissionLog $resultSubmissionLog 'wf-stale' $staleLogAttempt)) { throw 'result-submission self-test rejected valid redacted metadata' }
    if (-not (Test-WorkerResultSubmissionLog $resultSubmissionRetriedLog 'wf-stale' $staleLogAttempt)) { throw 'result-submission self-test rejected a retried request with a recorded retry count' }
    if (Test-WorkerResultSubmissionLog ($resultSubmissionLog.Replace('payload_sha256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef','payload=plaintext')) 'wf-stale' $staleLogAttempt) { throw 'result-submission self-test accepted an unhashed payload' }
    $jsonSelfTestPath = Join-Path $RepoRoot ".scratch/dur043-json-selftest.json"
    try {
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $jsonSelfTestPath) | Out-Null
        $jsonStart = "2026-09-23T00:00:00.0000000Z"
        $jsonFinish = "2026-09-23T00:00:01.0000000Z"
        if ((DurationMilliseconds $jsonStart $jsonFinish) -ne 1000) { throw "ISO timestamp duration self-test failed." }
        Write-Json $jsonSelfTestPath ([ordered]@{ self_test = $true; command_at_utc = $jsonStart; observed_at_utc = $jsonFinish })
        $jsonText = [System.IO.File]::ReadAllText($jsonSelfTestPath)
        if ($jsonText -match '/Date\(' -or $jsonText -notmatch '2026-09-23T00:00:00\.0000000Z') { throw "JSON self-test did not preserve ISO-8601 timestamps." }
        $jsonBytes = [System.IO.File]::ReadAllBytes($jsonSelfTestPath)
        if ($jsonBytes.Length -ge 3 -and $jsonBytes[0] -eq 0xEF -and $jsonBytes[1] -eq 0xBB -and $jsonBytes[2] -eq 0xBF) {
            throw "JSON self-test wrote a UTF-8 BOM; AWS CLI input JSON must be BOM-free."
        }
    } finally {
        Remove-Item -LiteralPath $jsonSelfTestPath -Force -ErrorAction SilentlyContinue
    }
    $failureProtocolPath = Join-Path $RepoRoot ".scratch/dur043-failure-protocol-selftest"
    try {
        New-Item -ItemType Directory -Force -Path $failureProtocolPath | Out-Null
        $originalProtocol = [ordered]@{ status = 'IN_PROGRESS'; marker = 'preserve-existing' }
        Write-Json (Join-Path $failureProtocolPath 'protocol.json') $originalProtocol
        $replacementProtocol = [ordered]@{ status = 'FAIL'; marker = 'must-not-overwrite' }
        if (Save-FailureProtocolIfOwned $false $failureProtocolPath $replacementProtocol) { throw 'Failure handler wrote into an output directory not created by this invocation.' }
        $preservedProtocol = Get-Content (Join-Path $failureProtocolPath 'protocol.json') -Raw | ConvertFrom-Json
        if ($preservedProtocol.status -ne 'IN_PROGRESS' -or $preservedProtocol.marker -ne 'preserve-existing') { throw 'Failure handler modified pre-existing evidence.' }
        if (-not (Save-FailureProtocolIfOwned $true $failureProtocolPath $replacementProtocol)) { throw 'Failure handler refused to write into its owned output directory.' }
        $ownedProtocol = Get-Content (Join-Path $failureProtocolPath 'protocol.json') -Raw | ConvertFrom-Json
        if ($ownedProtocol.status -ne 'FAIL' -or $ownedProtocol.marker -ne 'must-not-overwrite') { throw 'Failure handler did not write the owned failure protocol.' }
    } finally {
        Remove-Item -LiteralPath $failureProtocolPath -Recurse -Force -ErrorAction SilentlyContinue
    }
    $obligationFixture = Convert-ObligationSummaryRow '1|0|2|1|4|1|6|0|99|2|3|0'
    if ($obligationFixture.workflow_reconciliation_items_total -ne 1 -or
        $obligationFixture.global_reconciliation_items_total -ne 2 -or
        $obligationFixture.global_reconciliation_items_open -ne 1 -or
        $obligationFixture.database_reconciliation_items_open -ne 1 -or
        $obligationFixture.database_outbox_pending_or_claimed -ne 2 -or
        $obligationFixture.workflow_duplicate_effect_calls -ne 0) {
        throw "Obligation-summary self-test failed to retain workflow, global, and database-wide counts."
    }
    Write-Host "DUR-043 PowerShell 5.1 self-test PASS: 5 partition vectors, recovery ordering controls, original-worker identity controls, operation-specific stale-result log controls, row-lock probe controls, global obligation accounting, no-overwrite failure-protocol controls, and BOM-free AWS JSON."
    return
}

Push-Location $RepoRoot
try {
    if ([string]::IsNullOrWhiteSpace($AwsProfile)) { throw "AWS profile is required." }
    if ($Scenario -in @("all", "network") -and [string]::IsNullOrWhiteSpace($NetworkWorkflowId)) { throw "-NetworkWorkflowId is required; provide a unique run-scoped workflow ID." }
    if ($Scenario -in @("all", "host") -and [string]::IsNullOrWhiteSpace($HostWorkflowId)) { throw "-HostWorkflowId is required; provide a unique run-scoped workflow ID." }
    if ($Scenario -in @("all", "lock-held") -and ([string]::IsNullOrWhiteSpace($LockWorkflowId) -or [string]::IsNullOrWhiteSpace($LockProgressWorkflowId))) { throw "-LockWorkflowId and -LockProgressWorkflowId are required; provide unique run-scoped workflow IDs." }
    foreach ($workflowID in @($NetworkWorkflowId, $HostWorkflowId, $LockWorkflowId, $LockProgressWorkflowId)) {
    if (-not [string]::IsNullOrWhiteSpace($workflowID) -and $workflowID -notmatch '^dur049-r71-[a-z0-9-]{1,72}$') { throw "Workflow IDs must use the safe run-scoped form dur049-r71-<label>-<unique-suffix>: $workflowID" }
    }
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) { $OutputRoot = Join-Path $RepoRoot ("experiments/portfolio/cloud-recovery/dur049-aws-" + [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")) }
    elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) { $OutputRoot = Join-Path $RepoRoot $OutputRoot }
    if (Test-Path -LiteralPath $OutputRoot) { throw "Refusing to overwrite existing evidence: $OutputRoot" }
    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
    $script:OutputRootCreatedByThisRun = $true
    $script:AttemptLedgerPath = Join-Path $OutputRoot "attempt-ledger.jsonl"
    if (Test-Path -LiteralPath $script:AttemptLedgerPath) { throw "Refusing to append to existing attempt ledger: $script:AttemptLedgerPath" }

    $outputs = Get-TerraformOutputs
    $identity = Get-AccountIdentity
    $dependency = [string](Get-OutputValue $outputs "dependency_instance_id")
    $dependencyIP = [string](Get-OutputValue $outputs "dependency_private_ip")
    $appIDs = @((Get-OutputValue $outputs "app_instance_ids") | ForEach-Object { [string]$_ })
    $appPrivateIPs = @((Get-OutputValue $outputs "app_private_ips") | ForEach-Object { [string]$_ })
    if ($appIDs.Count -ne 2 -or $appPrivateIPs.Count -ne 2 -or [string]::IsNullOrWhiteSpace($dependency) -or [string]::IsNullOrWhiteSpace($dependencyIP)) { throw "Terraform outputs do not describe two app hosts and one dependency host." }
    Ensure-FixtureDefinition $dependency
    $instanceDetails = @(Get-InstanceDetails (@($dependency) + $appIDs))
    foreach ($instance in @($dependency) + $appIDs) {
        $state = (Invoke-Aws @("ec2", "describe-instances", "--instance-ids", $instance, "--query", "Reservations[0].Instances[0].State.Name", "--output", "text")).Trim()
        if ($state -ne "running") { throw "Instance $instance is not running: $state" }
        Wait-Ssm $instance
    }

    $protocol = [ordered]@{
        schema_version = "dur049-multihost.v6"
        status = "IN_PROGRESS"
        generated_at_utc = [DateTime]::UtcNow.ToString("o")
        git_commit = (git rev-parse HEAD).Trim()
        region = $Region
        aws_account_id = [string]$identity.Account
        aws_principal = [string]$identity.Arn
        aws_profile = $AwsProfile
        topology = [ordered]@{ application_hosts = 2; dependency_hosts = 1; app_instance_ids = $appIDs; app_private_ips = $appPrivateIPs; dependency_instance_id = $dependency; dependency_private_ip = $dependencyIP; instances = $instanceDetails }
        scenarios = @($Scenario)
        fixture_workflow_ids = [ordered]@{ network = $NetworkWorkflowId; host_stop = $HostWorkflowId; lock_held = $LockWorkflowId; lock_held_progress = $LockProgressWorkflowId }
        configuration = [ordered]@{
            activity = "dur048.sleep"
            fixture_definition_id = $FixtureDefinitionID
            fixture_definition_hash = $FixtureDefinitionHash
            requested_activity_delay_ms = $FixtureDelayMS
            claim_observation_timeout_seconds = 300
            lock_progress_activity_delay_ms = 1000
            lock_progress_observation_timeout_seconds = 120
            lock_holder_duration_seconds = $LockHolderDurationSeconds
            requested_scheduler_hold_after_acquire_ms = $ObservationHoldMS
            takeover_observation_hold_ms = $TakeoverObservationHoldMS
            requested_fixture_activity_enabled = $FixtureActivityEnabled
            requested_worker_slots = $WorkerSlots
            runtime_engine_mode = "disabled"
            runtime_scheduler = "enabled"
            fault_network_ports = $FaultPorts
            network_chain = "DOCKER-USER plus dependency INPUT for network arm; not_applicable host stop; row lock for lock-held"
            network_match_states = @("NEW", "ESTABLISHED", "RELATED")
            established_flow_termination = "host conntrack deletion"
            dependency_session_termination = [bool]$TerminateDatabaseSessions
            dependency_session_termination_method = $(if ($TerminateDatabaseSessions) { "pg_terminate_backend optional variant" } else { "none; network-only arm" })
            route_fault = "app-host blackhole route for dependency /32"
            lock_timeout_seconds = 2
            attempt_lease_seconds = 30
            tcp_keepalives_idle_seconds = 60
            idle_in_transaction_session_timeout_seconds = 600
            claim_observation_semantics = "Wait for a durable CLAIMED attempt; scheduler lease identity is joined from the ATTEMPT_CREATED history epoch because the scheduler releases its lease at pass end."
            peer_mode = "controller-started cold standby for takeover arms; not a continuously active peer"
            pre_fault_refresh = "The original app host runtime and worker are force-recreated and health-checked before each fault fixture; container IDs and start times are recorded in the episode configuration."
        }
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
    if ($Scenario -in @("all", "lock-held")) {
        $lockResult = Run-LockHeldArm $appIDs[0] $appIDs[1] $dependency $LockWorkflowId $LockProgressWorkflowId (Join-Path $OutputRoot "lock-held.json")
        $Results += $lockResult
    }
    $epochProbeWorkflowID = "dur049-r71-epoch-fence-" + [guid]::NewGuid().ToString("N").Substring(0, 8)
    $sameOwnerEpochProbe = Invoke-SameOwnerEpochProbe $appIDs[0] $epochProbeWorkflowID
    Write-Json (Join-Path $OutputRoot "same-owner-stale-epoch.json") $sameOwnerEpochProbe
    $protocol.status = "PASS"
    $protocol.results = $Results
    $protocol.negative_controls = [ordered]@{ same_owner_stale_epoch_fence = $sameOwnerEpochProbe; interpretation = "A disposable RUNNABLE workflow was protected by the same owner at a strictly newer lease epoch; an owner transition using that same owner's prior epoch was rejected without changing revision." }
    $protocol.acceptance = [ordered]@{ fault_confirmation = $true; peer_takeover = $true; superseded_epoch_transitions = 0; useful_work_checked = $true; result_rejections_recorded = $true; same_owner_stale_epoch_control = $true; durable_invariant_checker = $true; obligations_recorded = $true; lock_held_arm_separate = $true; session_termination_variant = [bool]$TerminateDatabaseSessions; no_cleanup_claim = "Terraform teardown remains a separate operator step and is recorded after the campaign." }
    Write-Json (Join-Path $OutputRoot "protocol.json") $protocol
    Write-Host "DUR-043 multi-host campaign PASS: $($Results.Count) fault arm(s) completed with zero superseded-epoch transitions."
} catch {
    if ($null -ne $script:CurrentEpisodeState -and $script:CurrentEpisodeState.status -eq "IN_PROGRESS") {
        $script:CurrentEpisodeState.status = "FAIL"
        $script:CurrentEpisodeState.error = $_.Exception.Message
        Save-EpisodeCheckpoint $script:CurrentEpisodePath $script:CurrentEpisodeState "failed"
    }
    if ($null -eq $protocol) {
    $protocol = [ordered]@{ schema_version = "dur049-multihost.v6"; status = "FAIL"; generated_at_utc = [DateTime]::UtcNow.ToString("o"); git_commit = (git rev-parse HEAD).Trim(); scenarios = @($Scenario) }
    }
    $protocol.status = "FAIL"
    $protocol.failed_at_utc = [DateTime]::UtcNow.ToString("o")
    $protocol.error = $_.Exception.Message
    if ($null -ne $script:CurrentEpisodeState) { $protocol.last_episode_stage = $script:CurrentEpisodeState.stage }
    $protocol.results = $Results
    $null = Save-FailureProtocolIfOwned $script:OutputRootCreatedByThisRun $OutputRoot $protocol
    throw
} finally {
    if ($NetworkRulesInserted) { try { Remove-NetworkBlock $appIDs[0] $dependency $dependencyIP $appPrivateIPs[0] } catch { Write-Warning "Could not remove network rules during cleanup: $_" } }
    if ($App1WasStopped) { try { Invoke-Aws @("ec2", "start-instances", "--instance-ids", $appIDs[0]) | Out-Null; Wait-Ssm $appIDs[0]; Start-AppRuntime $appIDs[0] } catch { Write-Warning "Could not restart app host 1 during cleanup: $_" } }
    if ($App2MayBeStopped) { try { Start-AppRuntime $appIDs[1] } catch { Write-Warning "Could not restart app host 2 during cleanup: $_" } }
    Pop-Location
}
