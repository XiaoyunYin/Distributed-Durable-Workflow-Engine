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
    $sql = "SELECT node_id, iteration, attempt_number, COALESCE(claim_token::text,''), state FROM engine.activity_attempts WHERE workflow_id='$WorkflowID' AND is_current ORDER BY attempt_number DESC LIMIT 1;"
    $row = Invoke-DbSql $DependencyInstanceId $sql
    $parts = $row -split '\|', 5
    if ($parts.Count -lt 5 -or $parts[4] -ne 'CLAIMED') { throw "Workflow $WorkflowID was not observed with a current CLAIMED attempt: $row" }
    if ([string]::IsNullOrWhiteSpace($parts[3])) { throw "Workflow $WorkflowID current attempt has no claim token." }
    return [ordered]@{ node_id = $parts[0]; iteration = [int]$parts[1]; attempt_number = [int64]$parts[2]; claim_token = $parts[3]; state = $parts[4] }
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

function Get-LeaseAcquisitions([string]$DependencyInstanceId, [int]$PartitionID) {
	$rows = Invoke-DbSql $DependencyInstanceId "SELECT acquisition_id::text, owner_id::text, epoch, acquired_at::text, lease_expires_at::text FROM engine.lease_acquisitions WHERE partition_id=$PartitionID ORDER BY epoch, acquired_at, acquisition_id;"
	$items = @()
	foreach ($row in ($rows.Trim() -split "\r?\n")) {
		if ([string]::IsNullOrWhiteSpace($row)) { continue }
		$parts = $row -split '\|', 5
		if ($parts.Count -lt 5) { throw "Lease acquisition ledger row is malformed: $row" }
		$items += [ordered]@{ acquisition_id = $parts[0]; owner_id = $parts[1]; epoch = [int64]$parts[2]; database_recorded_at_utc = ([DateTimeOffset]::Parse($parts[3])).UtcDateTime.ToString("o"); lease_expires_at_utc = ([DateTimeOffset]::Parse($parts[4])).UtcDateTime.ToString("o") }
	}
	return $items
}

function Wait-FirstNewOwnerAcquisition([string]$DependencyInstanceId, [int]$PartitionID, [string]$OldOwnerID, [int64]$OldEpoch, [DateTime]$FaultObservedAt, [int]$TimeoutSeconds = 120) {
    # Epoch is the durable ordering key. acquired_at is set inside the transaction,
    # not a commit timestamp, so do not compare it with the controller's clock.
    $sql = "SELECT acquisition_id::text, owner_id::text, epoch, acquired_at::text, lease_expires_at::text FROM engine.lease_acquisitions WHERE partition_id=$PartitionID AND owner_id::text <> '$OldOwnerID' AND epoch > $OldEpoch ORDER BY epoch, acquired_at, acquisition_id LIMIT 1;"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $row = Invoke-DbSql $DependencyInstanceId $sql
        if (-not [string]::IsNullOrWhiteSpace($row)) {
            $parts = $row.Trim() -split '\|', 5
            if ($parts.Count -eq 5) {
                $observedAt = [DateTime]::UtcNow
                $recordedAt = ([DateTimeOffset]::Parse($parts[3])).UtcDateTime
                return [ordered]@{ acquisition_id = $parts[0]; owner_id = $parts[1]; epoch = [int64]$parts[2]; database_recorded_at_utc = $recordedAt.ToString("o"); controller_observed_at_utc = $observedAt.ToString("o"); observed_at = $observedAt; lease_expires_at_utc = ([DateTimeOffset]::Parse($parts[4])).UtcDateTime.ToString("o"); fault_observed_at_utc = ([DateTimeOffset]$FaultObservedAt).ToUniversalTime().ToString("o") }
            }
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "No first acquisition by a new owner with epoch above $OldEpoch was observed after fault at $($FaultObservedAt.ToString('o'))."
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
                observed_at = $observedAt
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

function Wait-ClaimedWorkflow([string]$DependencyInstanceId, [string]$WorkflowID, [int]$PartitionID, [int]$TimeoutSeconds = 120) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $workflow = Get-Workflow $DependencyInstanceId $WorkflowID
        $lease = Get-Lease $DependencyInstanceId $PartitionID
        $leaseExpiry = [DateTimeOffset]::Parse($lease.lease_expires_at)
        if ($workflow.attempt_state -eq "CLAIMED" -and -not [string]::IsNullOrWhiteSpace($lease.owner_id) -and $leaseExpiry -gt [DateTimeOffset]::UtcNow) {
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
        "grep -E '^(DUR048_ACTIVITY_DELAY_MS|DUR048_ALLOW_FIXTURE_ACTIVITY|RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS|WORKER_SLOTS)=' deploy/aws/.env",
        'runtime_cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -q runtime)',
        'worker_cid=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml ps -q worker)',
        'test -n "$runtime_cid" && test -n "$worker_cid"',
        'printf "GIT_COMMIT=%s\n" "$(git rev-parse HEAD)"',
        'printf "RUNTIME_IMAGE_ID=%s\n" "$(docker inspect --format ''{{.Image}}'' "$runtime_cid")"',
        'printf "WORKER_IMAGE_ID=%s\n" "$(docker inspect --format ''{{.Image}}'' "$worker_cid")"'
    )
    $values = @{}
    foreach ($line in ($output.Trim() -split "\r?\n")) {
        if ($line -match '^(?<name>[A-Z0-9_]+)=(?<value>.*)$') { $values[$Matches.name] = $Matches.value }
    }
    foreach ($name in @('DUR048_ACTIVITY_DELAY_MS', 'DUR048_ALLOW_FIXTURE_ACTIVITY', 'RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS', 'WORKER_SLOTS', 'GIT_COMMIT', 'RUNTIME_IMAGE_ID', 'WORKER_IMAGE_ID')) {
        if (-not $values.ContainsKey($name)) { throw "Remote app configuration is missing $name." }
    }
    if ($values['GIT_COMMIT'] -notmatch '^[0-9a-f]{40}$') { throw "Remote app has no full checked-out source commit: $($values['GIT_COMMIT'])" }
    foreach ($name in @('RUNTIME_IMAGE_ID', 'WORKER_IMAGE_ID')) {
        if ($values[$name] -notmatch '^sha256:[0-9a-f]{64}$') { throw "Remote app reports an invalid image ID for ${name}: $($values[$name])" }
    }
    return [ordered]@{
        repo_commit = $values['GIT_COMMIT']
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
  (SELECT count(*) FROM engine.outbox WHERE workflow_id='$WorkflowID') || '|' ||
  (SELECT count(*) FROM engine.outbox WHERE workflow_id='$WorkflowID' AND publish_state IN ('PENDING','CLAIMED')) || '|' ||
  (SELECT count(*) FROM effects.effect_records WHERE workflow_id='$WorkflowID') || '|' ||
  (SELECT COALESCE(sum(n-1),0) FROM (SELECT logical_effect_key, count(*) AS n FROM effects.effect_call_attempts WHERE workflow_id='$WorkflowID' GROUP BY logical_effect_key) duplicates);
"@
    $parts = (Invoke-DbSql $DependencyInstanceId $sql).Trim() -split '\|', 6
    if ($parts.Count -lt 6) { throw "Obligation summary was malformed: $($parts -join '|')" }
    return [ordered]@{
        reconciliation_items_total = [int]$parts[0]
        reconciliation_items_open = [int]$parts[1]
        outbox_rows = [int]$parts[2]
        outbox_pending_or_claimed = [int]$parts[3]
        effect_records = [int]$parts[4]
        duplicate_effect_calls = [int]$parts[5]
    }
}

function Invoke-DurableChecker([string]$InstanceId, [string]$WorkflowID, [string]$OutputPath, [int64]$PreFaultRevision, [int64]$RecoveryEpoch, [int64]$StaleAttemptNumber) {
    $checkerFlags = "-workflow-ids '$WorkflowID' -pre-fault-revision $PreFaultRevision -recovery-epoch $RecoveryEpoch -stale-attempt-number $StaleAttemptNumber"
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
        "-recovery-epoch", [string]$RecoveryEpoch,
        "-stale-attempt-number", [string]$StaleAttemptNumber
    )
    $offline = $offlineOutput | ConvertFrom-Json
    if (-not $report.valid -or -not $offline.valid) { throw "Invariant checker rejected DUR-049 durable evidence: live=$($report.valid) offline=$($offline.valid)" }
    if ($report.fence.pre_fault_revision -ne $offline.fence.pre_fault_revision -or $report.fence.recovery_epoch -ne $offline.fence.recovery_epoch -or $report.fence.superseded_epoch_transitions_after_pre_fault_boundary -ne $offline.fence.superseded_epoch_transitions_after_pre_fault_boundary -or $report.fence.stale_attempt_number -ne $offline.fence.stale_attempt_number -or $report.fence.stale_attempt_found -ne $offline.fence.stale_attempt_found -or $report.fence.stale_attempt_current -ne $offline.fence.stale_attempt_current -or $report.fence.stale_result_recorded -ne $offline.fence.stale_result_recorded) {
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
    if ($output -notmatch "connectivity_before=reachable") { throw "Network fault precondition was not observed from the worker container: $output" }
    if ($output -notmatch "connectivity_after=blocked") { throw "Network fault was not observed from the worker container: $output" }
    if ($output -notmatch 'runtime_database_before=\{"status":"reachable"') { throw "Runtime-container PostgreSQL probe did not succeed before isolation: $output" }
    if ($output -notmatch 'runtime_database_after=blocked') { throw "Runtime-container PostgreSQL probe did not confirm isolation: $output" }
    if (-not $runtimeStateMatch.Success) { throw "Network fault was not observed with the runtime container still running: $output" }
    return [ordered]@{
        raw = $output.Trim()
        command_at_utc = $faultCommandAt
        observed_at_utc = $faultObservedAt
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

function Invoke-StaleProbe([string]$InstanceId, [string]$WorkflowID, [int]$PartitionID, [string]$OwnerID, [int64]$Epoch, [object]$Attempt = $null) {
    $probeArguments = "-workflow-id '$WorkflowID' -partition-id $PartitionID -owner-id '$OwnerID' -epoch $Epoch"
    if ($null -ne $Attempt) {
        $probeArguments += " -node-id '$($Attempt.node_id)' -iteration $($Attempt.iteration) -attempt-number $($Attempt.attempt_number) -claim-token '$($Attempt.claim_token)'"
    }
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

function Start-LockHolder([string]$InstanceId, [int]$PartitionID, [int]$DurationSeconds = 45) {
    $marker = "/tmp/dur049-lock-holder-$PartitionID.ready"
    $output = Send-Ssm $InstanceId @(
        'set -e',
        'cid=$(docker compose --env-file /opt/durable-agent-execution-engine/deploy/aws/.env -f /opt/durable-agent-execution-engine/deploy/aws/app-compose.yaml ps -q runtime)',
        'test -n "$cid"',
        "docker exec `"`$cid`" rm -f '$marker' || true",
        "docker exec -d `"`$cid`" /dur049-lock-holder -partition-id $PartitionID -duration ${DurationSeconds}s -marker '$marker'"
    )
    $deadline = (Get-Date).AddSeconds(30)
    do {
        Start-Sleep -Seconds 2
        $markerOutput = Send-Ssm $InstanceId @(
            'set -e',
            'cid=$(docker compose --env-file /opt/durable-agent-execution-engine/deploy/aws/app-compose.yaml ps -q runtime)',
            'test -n "$cid"',
            "if docker exec `"`$cid`" test -f '$marker'; then docker exec `"`$cid`" cat '$marker'; fi"
        ) 30
        if (-not [string]::IsNullOrWhiteSpace($markerOutput) -and $markerOutput -match 'locked_at=') {
            $lockedAtText = ($markerOutput.Trim() -split 'locked_at=', 2)[1].Trim()
            return [ordered]@{ marker = $marker; launch_output = $output.Trim(); observed_at_utc = [DateTime]::UtcNow; locked_at_utc = ([DateTimeOffset]::Parse($lockedAtText)).UtcDateTime; marker_contents = $markerOutput.Trim(); duration_seconds = $DurationSeconds }
        }
    } while ((Get-Date) -lt $deadline)
    throw "Lock-holder marker was not observed inside the running runtime container."
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
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID $partition
    $oldLease = $claimed.lease
    $before = $claimed.workflow
    $oldAttempt = Get-ClaimedAttempt $Dependency $WorkflowID
    if ($before.attempt_state -ne "CLAIMED") { throw "Lock-held fixture must be observed CLAIMED before the lock fault." }
    $checkpoint.original_owner_id = $oldLease.owner_id
    $checkpoint.original_epoch = $oldLease.epoch
    $checkpoint.workflow_before = $before
    $checkpoint.attempt_before = $oldAttempt
    $checkpoint.app2_stopped_before_owner_capture = $app2Stopped
    Save-EpisodeCheckpoint $OutputPath $checkpoint "claimed"
    $lockHolder = Start-LockHolder $App1 $partition 45
    $lockObservedAt = $lockHolder.observed_at_utc
    $checkpoint.lock_holder = $lockHolder
    Save-EpisodeCheckpoint $OutputPath $checkpoint "lease-row-lock-observed"
    Set-AppEnvironment $App2 1000 $TakeoverObservationHoldMS
    Start-AppRuntimeNoWait $App2
    $script:App2MayBeStopped = $false
    $app2Configuration = Get-AppConfiguration $App2
    Wait-AppApi $App2
    [void](Submit-FixtureWorkflow $App2 $ProgressWorkflowID)
    $progress = Wait-Workflow $Dependency $ProgressWorkflowID 75
    $progressObservedAt = [DateTime]::UtcNow
    if ($progress.state -ne "SUCCEEDED") { throw "Other partition did not complete successfully while lock-held partition was contended: $($progress.state)." }
    $progressResults = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$ProgressWorkflowId' AND reason='RESULT_CONSUMED';")
    if ($progressResults -lt 1) { throw "Other partition reached SUCCEEDED without a durable consumed result while lock-held partition was contended." }
    $progressLatency = DurationMilliseconds $lockObservedAt $progressObservedAt
    if ($progressLatency -ge ($lockHolder.duration_seconds * 1000)) { throw "Other-partition progress was not observed before the lock-holder's bounded hold elapsed: ${progressLatency}ms." }
    $checkpoint.other_partition_progress = $progress
    $checkpoint.other_partition_progress_observed_at_utc = $progressObservedAt.ToString("o")
    $checkpoint.other_partition_progress_ms = $progressLatency
    Save-EpisodeCheckpoint $OutputPath $checkpoint "other-partition-progress-confirmed"
    $takeover = Wait-FirstNewOwnerAcquisition $Dependency $partition $oldLease.owner_id $oldLease.epoch $lockObservedAt 150
    $checkpoint.first_new_owner_acquisition = $takeover
    Save-EpisodeCheckpoint $OutputPath $checkpoint "new-owner-observed"
    $usefulProgress = Wait-FirstUsefulRecoveryProgress $Dependency $WorkflowID $before.attempt_number $before.revision $takeover.owner_id $oldLease.epoch 180
    $orderedProgress = Assert-AcquisitionPrecedesProgress $takeover $usefulProgress
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch $oldAttempt
    $checkpoint.first_useful_progress = $usefulProgress
    $checkpoint.stale_owner_probe = $staleProbe
    Save-EpisodeCheckpoint $OutputPath $checkpoint "replacement-result-and-stale-rejection-observed"
    $terminal = Wait-Workflow $Dependency $WorkflowID 180
    if ($terminal.state -ne "SUCCEEDED") { throw "Lock-held workflow did not recover to SUCCEEDED: $($terminal.state)" }
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND revision > $($before.revision) AND scheduler_epoch IS NOT NULL AND scheduler_epoch < $($takeover.epoch);")
    if ($historyCount -ne 0) { throw "A superseded epoch transition was committed after the lock-held pre-fault boundary: $historyCount" }
    $acquisitions = @(Get-LeaseAcquisitions $Dependency $partition)
    $attempts = @(Get-AttemptSummary $Dependency $WorkflowID)
    $obligations = Get-ObligationSummary $Dependency $WorkflowID
    $snapshotPath = Join-Path (Split-Path -Parent $OutputPath) "lock-held-durable-trace.json"
    $checker = Invoke-DurableChecker $App1 $WorkflowID $snapshotPath $before.revision $takeover.epoch $oldAttempt.attempt_number
    $result = [ordered]@{
        fault = "lease-row-lock-held-beyond-expiry"
        workflow_id = $WorkflowID
        partition_id = $partition
        progress_workflow_id = $ProgressWorkflowID
        progress_partition_id = $progressPartition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        app2_stopped_before_owner_capture = $app2Stopped
        lock_holder = $lockHolder
        first_new_owner_acquisition = [ordered]@{ acquisition_id = $takeover.acquisition_id; owner_id = $takeover.owner_id; epoch = $takeover.epoch; database_recorded_at_utc = $takeover.database_recorded_at_utc; controller_observed_at_utc = $takeover.controller_observed_at_utc; lease_expires_at_utc = $takeover.lease_expires_at_utc }
        recovery_transition_epoch = $usefulProgress.transition.scheduler_epoch
        recovery_transition_matches_first_new_owner_epoch = ($usefulProgress.transition.scheduler_epoch -eq $takeover.epoch)
        progress_while_lock_held = $progress
        other_partition_progress_observed_at_utc = $progressObservedAt.ToString("o")
        lock_observed_to_other_partition_progress_ms = $progressLatency
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ definition = $usefulProgress.progress_definition; controller_observed_at_utc = $usefulProgress.controller_observed_at_utc; workflow = $usefulProgress.workflow }
        recovery_ordering = $orderedProgress
        superseded_epoch_transitions_after_pre_fault_boundary = $historyCount
        stale_owner_probe = $staleProbe
        attempts = $attempts
        lease_acquisitions = $acquisitions
        obligations = $obligations
        durable_invariant_checker = $checker
        lock_observed_to_first_new_owner_acquisition_observed_ms = DurationMilliseconds $lockObservedAt $takeover.observed_at
        first_new_owner_acquisition_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $takeover.observed_at $usefulProgress.observed_at
        timestamp_semantics = "Intervals use controller UTC observation times after 2-second polling; DB timestamps are transaction-recorded diagnostics, not commit times."
        recovery_timing_note = "This is the lock-held isolation arm. The lease fence remains row-locked; the bounded lock timeout lets other partitions progress while takeover waits. It is reported separately from pure network isolation."
        configuration = [ordered]@{ activity = "dur048.sleep"; app1 = $app1Configuration; app2 = $app2Configuration; lock_progress_activity_delay_ms = 1000; lock_holder_duration_seconds = 45; lock_timeout_seconds = 2; attempt_lease_seconds = 30 }
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
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID $partition
    $oldLease = $claimed.lease
    $before = $claimed.workflow
    $oldAttempt = Get-ClaimedAttempt $Dependency $WorkflowID
    if ([string]::IsNullOrWhiteSpace($oldLease.owner_id)) { throw "Application host 1 did not own partition $partition before network isolation." }
    if ($before.attempt_state -ne "CLAIMED") { throw "Network fixture must be observed CLAIMED before isolation; observed $($before.attempt_state)." }
    $checkpoint.original_owner_id = $oldLease.owner_id
    $checkpoint.original_epoch = $oldLease.epoch
    $checkpoint.workflow_before = $before
    $checkpoint.attempt_before = $oldAttempt
    $checkpoint.app2_stopped_before_owner_capture = $app2Stopped
    Save-EpisodeCheckpoint $OutputPath $checkpoint "claimed"
    $script:NetworkRulesInserted = $true
    $faultObserved = Insert-NetworkBlock $App1 $Dependency $DependencyIP $AppPrivateIP ([bool]$TerminateDatabaseSessions)
    $checkpoint.fault_observed = $faultObserved
    Save-EpisodeCheckpoint $OutputPath $checkpoint "network-isolation-confirmed"
    Set-AppEnvironment $App2 $FixtureDelayMS $TakeoverObservationHoldMS
    Start-AppRuntimeNoWait $App2
    $script:App2MayBeStopped = $false
    $app2Configuration = Get-AppConfiguration $App2
    $takeover = Wait-FirstNewOwnerAcquisition $Dependency $partition $oldLease.owner_id $oldLease.epoch $faultObserved.observed_at_utc
    $checkpoint.first_new_owner_acquisition = $takeover
    Save-EpisodeCheckpoint $OutputPath $checkpoint "new-owner-observed"
    # Keep the original host isolated until the peer has durably timed out the
    # old claim and accepted a result for its replacement attempt. Reconnecting
    # immediately after lease acquisition can let an unfinished, still-current
    # attempt report first; that is not evidence of recovery from stale work.
    $usefulProgress = Wait-FirstUsefulRecoveryProgress $Dependency $WorkflowID $before.attempt_number $before.revision $takeover.owner_id $oldLease.epoch 240
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
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch $oldAttempt
    $checkpoint.original_runtime_observed_after_reconnect = $reconnected
    $checkpoint.stale_owner_probe = $staleProbe
    $checkpoint.first_useful_progress = $usefulProgress
    Save-EpisodeCheckpoint $OutputPath $checkpoint "reconnected-stale-rejection-and-progress-observed"
    $terminal = Wait-Workflow $Dependency $WorkflowID 150
    $terminalAt = ([DateTimeOffset]::Parse($terminal.updated_at)).UtcDateTime
    if ($terminal.state -ne "SUCCEEDED") { throw "Network-isolated workflow did not recover to SUCCEEDED: $($terminal.state)" }
    # Revision is the durable pre-fault boundary.  Do not use wall-clock
    # timestamps here: transition_history.created_at is not a commit timestamp
    # and the old owner may have made legitimate transitions before isolation.
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND revision > $($before.revision) AND scheduler_epoch IS NOT NULL AND scheduler_epoch < $($takeover.epoch);")
    if ($historyCount -ne 0) { throw "A transition carrying the superseded epoch was committed after the network-fault pre-fault boundary: $historyCount" }
    $acquisitions = @(Get-LeaseAcquisitions $Dependency $partition)
    $attempts = @(Get-AttemptSummary $Dependency $WorkflowID)
    $obligations = Get-ObligationSummary $Dependency $WorkflowID
    $snapshotPath = Join-Path (Split-Path -Parent $OutputPath) "network-durable-trace.json"
    $checker = Invoke-DurableChecker $App1 $WorkflowID $snapshotPath $before.revision $takeover.epoch $oldAttempt.attempt_number
    $result = [ordered]@{
        fault = "app-host-postgres-network-isolation-and-reconnect"
        workflow_id = $WorkflowID
        partition_id = $partition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        app2_stopped_before_owner_capture = $app2Stopped
        first_new_owner_acquisition = [ordered]@{ acquisition_id = $takeover.acquisition_id; owner_id = $takeover.owner_id; epoch = $takeover.epoch; database_recorded_at_utc = $takeover.database_recorded_at_utc; controller_observed_at_utc = $takeover.controller_observed_at_utc; lease_expires_at_utc = $takeover.lease_expires_at_utc }
        recovery_transition_epoch = $usefulProgress.transition.scheduler_epoch
        recovery_transition_matches_first_new_owner_epoch = ($usefulProgress.transition.scheduler_epoch -eq $takeover.epoch)
        fault_observed = $faultObserved
        original_runtime_observed = $reconnected
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ definition = $usefulProgress.progress_definition; controller_observed_at_utc = $usefulProgress.controller_observed_at_utc; workflow = $usefulProgress.workflow }
        recovery_ordering = $orderedProgress
        superseded_epoch_transitions_after_pre_fault_boundary = $historyCount
        stale_owner_probe = $staleProbe
        attempts = $attempts
        lease_acquisitions = $acquisitions
        obligations = $obligations
        durable_invariant_checker = $checker
        terminal_completion_at_utc = $terminalAt.ToString("o")
        fault_command_to_observed_ms = DurationMilliseconds $faultObserved.command_at_utc $faultObserved.observed_at_utc
        fault_observed_to_first_new_owner_acquisition_observed_ms = DurationMilliseconds $faultObserved.observed_at_utc $takeover.observed_at
        fault_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $faultObserved.observed_at_utc $usefulProgress.observed_at
        first_new_owner_acquisition_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $takeover.observed_at $usefulProgress.observed_at
        timestamp_semantics = "Intervals use controller UTC observation times after 2-second polling; DB timestamps are transaction-recorded diagnostics, not commit times."
        first_useful_progress_transition = $usefulProgress.transition
        fault_observed_to_terminal_completion_ms = DurationMilliseconds $faultObserved.observed_at_utc $terminalAt
        configuration = [ordered]@{ activity = "dur048.sleep"; app1 = $app1Configuration; app2 = $app2Configuration; fault_network_ports = $FaultPorts; network_chain = "DOCKER-USER plus dependency INPUT"; network_match_states = @("NEW", "ESTABLISHED", "RELATED"); established_flow_termination = "host conntrack deletion"; session_termination_enabled = [bool]$TerminateDatabaseSessions; session_termination_method = $(if ($TerminateDatabaseSessions) { "pg_terminate_backend optional variant" } else { "none; network-only arm" }); route_fault = "app-host blackhole route for dependency /32"; dependency_source_ip = $AppPrivateIP }
        limitation = "This arm isolates the app host from PostgreSQL while preserving its process; it is not a host-stop or database-host durability claim."
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
    $claimed = Wait-ClaimedWorkflow $Dependency $WorkflowID $partition
    $oldLease = $claimed.lease
    $before = $claimed.workflow
    $oldAttempt = Get-ClaimedAttempt $Dependency $WorkflowID
    if ([string]::IsNullOrWhiteSpace($oldLease.owner_id)) { throw "Application host 1 did not own partition $partition before host stop." }
    if ($before.attempt_state -ne "CLAIMED") { throw "Host-stop fixture must be observed CLAIMED before the stop; observed $($before.attempt_state)." }
    $checkpoint.original_owner_id = $oldLease.owner_id
    $checkpoint.original_epoch = $oldLease.epoch
    $checkpoint.workflow_before = $before
    $checkpoint.attempt_before = $oldAttempt
    $checkpoint.app2_stopped_before_owner_capture = $app2Stopped
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
    $takeover = Wait-FirstNewOwnerAcquisition $Dependency $partition $oldLease.owner_id $oldLease.epoch $stoppedAt
    $checkpoint.first_new_owner_acquisition = $takeover
    Save-EpisodeCheckpoint $OutputPath $checkpoint "new-owner-observed"
    $usefulProgress = Wait-FirstUsefulRecoveryProgress $Dependency $WorkflowID $before.attempt_number $before.revision $takeover.owner_id $oldLease.epoch 180
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
    $originalRuntimeObserved = Observe-Runtime $App1
    $staleProbe = Invoke-StaleProbe $App1 $WorkflowID $partition $oldLease.owner_id $oldLease.epoch $oldAttempt
    $checkpoint.original_runtime_observed_after_restart = $originalRuntimeObserved
    $checkpoint.stale_owner_probe = $staleProbe
    Save-EpisodeCheckpoint $OutputPath $checkpoint "restart-and-stale-rejection-observed"
    $terminal = Wait-Workflow $Dependency $WorkflowID 180
    $terminalAt = ([DateTimeOffset]::Parse($terminal.updated_at)).UtcDateTime
    if ($terminal.state -ne "SUCCEEDED") { throw "Host-stop workflow did not recover to SUCCEEDED: $($terminal.state)" }
    $historyCount = [int](Invoke-DbSql $Dependency "SELECT count(*) FROM engine.transition_history WHERE workflow_id='$WorkflowID' AND revision > $($before.revision) AND scheduler_epoch IS NOT NULL AND scheduler_epoch < $($takeover.epoch);")
    if ($historyCount -ne 0) { throw "A transition carrying the stopped host's superseded epoch was committed after the host-stop pre-fault boundary: $historyCount" }
    $acquisitions = @(Get-LeaseAcquisitions $Dependency $partition)
    $attempts = @(Get-AttemptSummary $Dependency $WorkflowID)
    $obligations = Get-ObligationSummary $Dependency $WorkflowID
    $snapshotPath = Join-Path (Split-Path -Parent $OutputPath) "host-durable-trace.json"
    $checker = Invoke-DurableChecker $App1 $WorkflowID $snapshotPath $before.revision $takeover.epoch $oldAttempt.attempt_number
    $result = [ordered]@{
        fault = "application-host-forced-stop-and-restart"
        workflow_id = $WorkflowID
        partition_id = $partition
        original_owner_id = $oldLease.owner_id
        original_epoch = $oldLease.epoch
        app2_stopped_before_owner_capture = $app2Stopped
        first_new_owner_acquisition = [ordered]@{ acquisition_id = $takeover.acquisition_id; owner_id = $takeover.owner_id; epoch = $takeover.epoch; database_recorded_at_utc = $takeover.database_recorded_at_utc; controller_observed_at_utc = $takeover.controller_observed_at_utc; lease_expires_at_utc = $takeover.lease_expires_at_utc }
        recovery_transition_epoch = $usefulProgress.transition.scheduler_epoch
        recovery_transition_matches_first_new_owner_epoch = ($usefulProgress.transition.scheduler_epoch -eq $takeover.epoch)
        fault_observed = [ordered]@{ stop_requested_at_utc = $stopRequestedAt.ToString("o"); stopped_at_utc = $stoppedAt.ToString("o"); stopped_state = $true; restart_requested_at_utc = $restartRequestedAt.ToString("o"); running_observed_at_utc = $runningObservedAt.ToString("o"); ssm_online_after_restart = $true; original_runtime_observed = $originalRuntimeObserved }
        workflow_before = $before
        workflow_after = $terminal
        first_useful_progress = [ordered]@{ definition = $usefulProgress.progress_definition; controller_observed_at_utc = $usefulProgress.controller_observed_at_utc; workflow = $usefulProgress.workflow }
        recovery_ordering = $orderedProgress
        superseded_epoch_transitions_after_pre_fault_boundary = $historyCount
        stale_owner_probe = $staleProbe
        attempts = $attempts
        lease_acquisitions = $acquisitions
        obligations = $obligations
        durable_invariant_checker = $checker
        terminal_completion_at_utc = $terminalAt.ToString("o")
        stop_requested_to_running_ms = DurationMilliseconds $stopRequestedAt $runningObservedAt
        stop_observed_to_first_new_owner_acquisition_observed_ms = DurationMilliseconds $stoppedAt $takeover.observed_at
        stop_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $stoppedAt $usefulProgress.observed_at
        first_new_owner_acquisition_observed_to_first_useful_progress_observed_ms = DurationMilliseconds $takeover.observed_at $usefulProgress.observed_at
        timestamp_semantics = "Intervals use controller UTC observation times after 2-second polling; DB timestamps are transaction-recorded diagnostics, not commit times."
        first_useful_progress_transition = $usefulProgress.transition
        recovery_timing_note = "App2 is a controller-started standby, not a continuously active peer. Recovery timing is bounded by the 30-second AttemptLease plus cold standby activation; it is not a host failover performance claim."
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
    Write-Host "DUR-043 PowerShell 5.1 self-test PASS: 5 partition vectors, recovery owner/revision ordering controls, and BOM-free AWS JSON."
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
        schema_version = "dur049-multihost.v4"
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
        configuration = [ordered]@{ activity = "dur048.sleep"; fixture_definition_id = $FixtureDefinitionID; fixture_definition_hash = $FixtureDefinitionHash; requested_activity_delay_ms = $FixtureDelayMS; claim_observation_timeout_seconds = 120; lock_progress_activity_delay_ms = 1000; requested_scheduler_hold_after_acquire_ms = $ObservationHoldMS; takeover_observation_hold_ms = $TakeoverObservationHoldMS; requested_fixture_activity_enabled = $FixtureActivityEnabled; requested_worker_slots = $WorkerSlots; runtime_engine_mode = "disabled"; runtime_scheduler = "enabled"; fault_network_ports = $FaultPorts; network_chain = "DOCKER-USER plus dependency INPUT for network arm; not_applicable host stop; row lock for lock-held"; network_match_states = @("NEW", "ESTABLISHED", "RELATED"); established_flow_termination = "host conntrack deletion"; dependency_session_termination = [bool]$TerminateDatabaseSessions; dependency_session_termination_method = $(if ($TerminateDatabaseSessions) { "pg_terminate_backend optional variant" } else { "none; network-only arm" }); route_fault = "app-host blackhole route for dependency /32"; lock_timeout_seconds = 2; attempt_lease_seconds = 30; tcp_keepalives_idle_seconds = 60; idle_in_transaction_session_timeout_seconds = 600 }
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
    $protocol.status = "PASS"
    $protocol.results = $Results
    $protocol.acceptance = [ordered]@{ fault_confirmation = $true; peer_takeover = $true; superseded_epoch_transitions = 0; useful_work_checked = $true; result_rejections_recorded = $true; durable_invariant_checker = $true; obligations_recorded = $true; lock_held_arm_separate = $true; session_termination_variant = [bool]$TerminateDatabaseSessions; no_cleanup_claim = "Terraform teardown remains a separate operator step and is recorded after the campaign." }
    Write-Json (Join-Path $OutputRoot "protocol.json") $protocol
    Write-Host "DUR-043 multi-host campaign PASS: $($Results.Count) fault arm(s) completed with zero superseded-epoch transitions."
} catch {
    if ($null -ne $script:CurrentEpisodeState -and $script:CurrentEpisodeState.status -eq "IN_PROGRESS") {
        $script:CurrentEpisodeState.status = "FAIL"
        $script:CurrentEpisodeState.error = $_.Exception.Message
        Save-EpisodeCheckpoint $script:CurrentEpisodePath $script:CurrentEpisodeState "failed"
    }
    if ($null -eq $protocol) {
    $protocol = [ordered]@{ schema_version = "dur049-multihost.v4"; status = "FAIL"; generated_at_utc = [DateTime]::UtcNow.ToString("o"); git_commit = (git rev-parse HEAD).Trim(); scenarios = @($Scenario) }
    }
    $protocol.status = "FAIL"
    $protocol.failed_at_utc = [DateTime]::UtcNow.ToString("o")
    $protocol.error = $_.Exception.Message
    if ($null -ne $script:CurrentEpisodeState) { $protocol.last_episode_stage = $script:CurrentEpisodeState.stage }
    $protocol.results = $Results
    if (Test-Path -LiteralPath $OutputRoot) { Write-Json (Join-Path $OutputRoot "protocol.json") $protocol }
    throw
} finally {
    if ($NetworkRulesInserted) { try { Remove-NetworkBlock $appIDs[0] $dependency $dependencyIP $appPrivateIPs[0] } catch { Write-Warning "Could not remove network rules during cleanup: $_" } }
    if ($App1WasStopped) { try { Invoke-Aws @("ec2", "start-instances", "--instance-ids", $appIDs[0]) | Out-Null; Wait-Ssm $appIDs[0]; Start-AppRuntime $appIDs[0] } catch { Write-Warning "Could not restart app host 1 during cleanup: $_" } }
    if ($App2MayBeStopped) { try { Start-AppRuntime $appIDs[1] } catch { Write-Warning "Could not restart app host 2 during cleanup: $_" } }
    Pop-Location
}
