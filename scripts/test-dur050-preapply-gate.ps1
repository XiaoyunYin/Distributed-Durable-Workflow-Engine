$ErrorActionPreference = 'Stop'
if (-not $IsLinux) { throw 'DUR-050 pre-apply gate tests require Ubuntu/Linux.' }
$repoRoot = Split-Path -Parent $PSScriptRoot
$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('dur050-preapply-test-' + [guid]::NewGuid().ToString('N'))
$bin = Join-Path $temp 'bin'
$state = Join-Path $temp 'state'
$utf8 = [System.Text.UTF8Encoding]::new($false)
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
New-Item -ItemType Directory -Force -Path $bin, $state | Out-Null

$awsStub = @'
#!/bin/sh
set -eu
args="$*"
printf '%s\n' "$args" >> "$DUR050_AWS_LOG"
case "$args" in
  *" ssm "*)
    printf '%s\n' "$args" >> "$DUR050_SSM_LOG"
    echo "SSM is forbidden in the DUR-050 pre-apply gate" >&2
    exit 91 ;;
  *" sts get-caller-identity "*)
    account=372206265946
    if [ "$DUR050_SCENARIO" = account ]; then account=000000000000; fi
    printf '{"UserId":"AIDATEST","Account":"%s","Arn":"arn:aws:iam::%s:user/test"}\n' "$account" "$account" ;;
  *" service-quotas get-service-quota "*)
    quota=32
    if [ "$DUR050_SCENARIO" = quota ]; then quota=16; fi
    printf '{"Quota":{"QuotaArn":"arn:aws:servicequotas:us-west-1:372206265946:ec2/L-1216C47A","QuotaCode":"L-1216C47A","ServiceCode":"ec2","ServiceName":"Amazon Elastic Compute Cloud","Value":%s,"Unit":"None","Adjustable":true,"GlobalQuota":false}}\n' "$quota" ;;
  *" ec2 describe-instances "*)
    if printf '%s' "$args" | grep -Fq 'stopping,stopped,shutting-down'; then
      if [ "$DUR050_SCENARIO" = instance ] || [ "$DUR050_SCENARIO" = stopped-instance ]; then
      state=running; if [ "$DUR050_SCENARIO" = stopped-instance ]; then state=stopped; fi
      printf '{"Reservations":[{"Instances":[{"InstanceId":"i-11111111111111111","InstanceType":"c7i.large","State":{"Name":"%s"},"Tags":[{"Key":"Task","Value":"DUR-050"}]}]}]}\n' "$state"
      else printf '{"Reservations":[]}\n'; fi
    elif [ "$DUR050_SCENARIO" = quota ]; then
      printf '{"Reservations":[{"Instances":[{"InstanceId":"i-22222222222222222","InstanceType":"m7i.4xlarge","InstanceLifecycle":null,"State":{"Name":"running"}}]}]}\n'
    else
      printf '{"Reservations":[]}\n'
    fi ;;
  *" ec2 describe-instance-types "*)
    printf '{"InstanceTypes":[{"InstanceType":"m7i.4xlarge","VCpuInfo":{"DefaultVCpus":16}}]}\n' ;;
  *" ec2 describe-volumes "*)
    if [ "$DUR050_SCENARIO" = volume ]; then
      printf '{"Volumes":[{"VolumeId":"vol-0123456789abcdef0","State":"in-use","Size":40,"Tags":[{"Key":"Task","Value":"DUR-050"}]}]}\n'
    elif [ "$DUR050_SCENARIO" = project-volume ]; then
      printf '{"Volumes":[{"VolumeId":"vol-0123456789abcdef2","State":"in-use","Size":40,"Tags":[{"Key":"Project","Value":"durable-engine"}]}]}\n'
    elif [ "$DUR050_SCENARIO" = available-volume ]; then
      printf '{"Volumes":[{"VolumeId":"vol-0123456789abcdef1","State":"available","Size":40,"Tags":[]}]}\n'
    else
      printf '{"Volumes":[]}\n'
    fi ;;
  *" budgets describe-budget "*)
    printf '{"Budget":{"BudgetName":"durable-engine-D022-aggregate-20260923","BudgetLimit":{"Amount":"200","Unit":"USD"},"CalculatedSpend":{"ActualSpend":{"Amount":"1.75","Unit":"USD"},"ForecastedSpend":{"Amount":"5.25","Unit":"USD"}}}}\n' ;;
  *" budgets describe-notifications-for-budget "*)
    state=OK
    if [ "$DUR050_SCENARIO" = alarm ]; then state=ALARM; fi
    printf '{"Notifications":[{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":160.0,"ThresholdType":"ABSOLUTE_VALUE","NotificationState":"OK"},{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":76.47,"ThresholdType":"ABSOLUTE_VALUE","NotificationState":"%s"},{"NotificationType":"FORECASTED","ComparisonOperator":"GREATER_THAN","Threshold":160.0,"ThresholdType":"ABSOLUTE_VALUE","NotificationState":"OK"}]}\n' "$state" ;;
  *" ce list-cost-allocation-tags "*)
    task=Active
    if [ "$DUR050_SCENARIO" = tags ]; then task=Inactive; fi
    printf '{"CostAllocationTags":[{"TagKey":"Task","Type":"UserDefined","Status":"%s","LastUpdatedDate":"2026-09-25T07:06:39Z"},{"TagKey":"Environment","Type":"UserDefined","Status":"Active","LastUpdatedDate":"2026-09-25T07:06:39Z"}]}\n' "$task" ;;
  *)
    echo "unexpected AWS command: $args" >&2
    exit 90 ;;
esac
'@
[IO.File]::WriteAllText((Join-Path $bin 'aws'), $awsStub.Replace("`r", '') + [Environment]::NewLine, $utf8)
& chmod 0755 (Join-Path $bin 'aws')
if ($LASTEXITCODE -ne 0) { throw 'Could not mark AWS stub executable.' }
$terraformStub = @'
#!/bin/sh
set -eu
if [ "${1:-}" = version ] && [ "${2:-}" = -json ]; then
  printf '{"terraform_version":"1.16.4"}\n'
  exit 0
fi
echo "unexpected Terraform invocation: $*" >&2
exit 89
'@
$terraformStubPath = Join-Path $bin 'terraform'
[IO.File]::WriteAllText($terraformStubPath, $terraformStub.Replace("`r", '') + [Environment]::NewLine, $utf8)
& chmod 0755 $terraformStubPath
if ($LASTEXITCODE -ne 0) { throw 'Could not mark Terraform stub executable.' }

function Write-TestJson([string]$Path, $Value) {
    [IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth 30) + [Environment]::NewLine), $utf8)
}

function Write-TestPlanZip([string]$Path, [string]$Lineage, [long]$Serial) {
    $archive = [System.IO.Compression.ZipFile]::Open($Path, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        $entry = $archive.CreateEntry('tfstate')
        $stream = $entry.Open()
        try {
            $writer = [System.IO.StreamWriter]::new($stream, $utf8)
            try { $writer.Write((([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = $Serial; lineage = $Lineage; resources = @() }) | ConvertTo-Json -Compress)) }
            finally { $writer.Dispose() }
        } finally { $stream.Dispose() }
    } finally { $archive.Dispose() }
}

function Invoke-Gate([string]$Scenario, [string]$Name, [string]$ScriptPath, [string]$ExpectedHash, [string]$InspectionOverridePath, [int]$ReserveMinutes = 480, [string]$LedgerOverride, [scriptblock]$ManifestMutation, [switch]$RehashInspection, [switch]$DryRun) {
    $isMutant = -not [string]::IsNullOrWhiteSpace($ScriptPath)
    if (-not $ScriptPath) { $ScriptPath = Join-Path $PSScriptRoot 'dur050-preapply-gate.ps1' }
    $outputPath = Join-Path $temp "$Name-gate.json"
    $ledgerOutput = Join-Path $temp "$Name-ledger.json"
    $gateInspectionPath = if ($InspectionOverridePath) { $InspectionOverridePath } else { $inspectionPath }
    $gateManifest = Get-Content -LiteralPath $manifestTemplatePath -Raw | ConvertFrom-Json
    $gateManifest.approved_saved_plan.saved_plan_sha256 = $ExpectedHash.ToUpperInvariant()
    $gateManifest.approved_saved_plan.inspection_path = [System.IO.Path]::GetFullPath($gateInspectionPath)
    if ($ManifestMutation) { & $ManifestMutation $gateManifest }
    if ($RehashInspection) {
        if (-not (Test-Path -LiteralPath $gateInspectionPath -PathType Leaf)) { throw "Cannot rehash missing test inspection: $gateInspectionPath" }
        $gateManifest.approved_saved_plan.inspection_sha256 = (Get-FileHash -LiteralPath $gateInspectionPath -Algorithm SHA256).Hash.ToUpperInvariant()
    }
    $manifestForInvocation = Join-Path $temp "$Name-manifest.json"
    Write-TestJson $manifestForInvocation $gateManifest
    $arguments = @(
        '-NoProfile', '-File', $ScriptPath,
        '-SavedPlanPath', $planPath,
        '-PlanInspectionPath', $gateInspectionPath,
        '-CampaignManifestPath', $manifestForInvocation,
        '-LedgerCheckPath', $ledgerOutput,
        '-ReserveMinutes', [string]$ReserveMinutes,
        '-OutputPath', $outputPath,
        '-TerraformStatePath', $statePath,
        '-TerraformExe', $terraformStubPath,
        '-RepositoryRoot', $repoRoot,
        '-PythonExe', 'python3'
    )
    if (-not $LedgerOverride -and $isMutant) { $LedgerOverride = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json' }
    if ($LedgerOverride) { $arguments += @('-LedgerPath', $LedgerOverride) }
    if ($DryRun) { $arguments += '-DryRun' }
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = Join-Path $PSHOME 'pwsh'
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($arg in $arguments) { $start.ArgumentList.Add($arg) }
    $start.Environment['PATH'] = $bin + ':/usr/bin:/bin'
    $start.Environment['DUR050_AWS_LOG'] = $awsLog
    $start.Environment['DUR050_SSM_LOG'] = $ssmLog
    $start.Environment['DUR050_SCENARIO'] = $Scenario
    $start.Environment['DUR050_ENABLE_TEST_LEDGER_OVERRIDE'] = '1'
    $start.Environment['DUR050_ENABLE_TEST_STATE_OVERRIDE'] = '1'
    $start.Environment['DUR050_ENABLE_TEST_HOOKS'] = '1'
    $proc = [Diagnostics.Process]::new()
    $proc.StartInfo = $start
    [void]$proc.Start()
    $stdout = $proc.StandardOutput.ReadToEnd()
    $stderr = $proc.StandardError.ReadToEnd()
    $proc.WaitForExit()
    return [pscustomobject]@{ ExitCode = $proc.ExitCode; OutputPath = $outputPath; LedgerPath = $ledgerOutput; Text = ($stderr + [Environment]::NewLine + $stdout) }
}

function Assert-ControlFailed($Result, [string]$Name, [string]$MessagePattern) {
    if ($Result.ExitCode -eq 0) { throw "Negative control '$Name' was accepted." }
    if ($Result.Text -notmatch $MessagePattern) { throw "Negative control '$Name' failed for an unexpected reason: $($Result.Text)" }
    if (-not (Test-Path -LiteralPath $Result.OutputPath)) { throw "Negative control '$Name' did not leave a FAIL record." }
    $failure = Get-Content -LiteralPath $Result.OutputPath -Raw | ConvertFrom-Json
    if ($failure.schema -ne 'dur050-pre-apply-gate.v1' -or $failure.status -ne 'FAIL' -or $failure.apply_authority -ne 'NONE; Claude go/no-go required') {
        throw "Negative control '$Name' did not write the expected FAIL record."
    }
}

function Assert-GateMutationSensitive($Result, [string]$Name) {
    if ($Result.ExitCode -ne 0) { throw "Removing the '$Name' guard did not let its negative control pass: $($Result.Text)" }
    $record = Get-Content -LiteralPath $Result.OutputPath -Raw | ConvertFrom-Json
    if ($record.status -ne 'PASS') { throw "Removing the '$Name' guard did not produce a PASS record for its negative: $($Result.Text)" }
    try { Assert-ControlFailed $Result $Name '.'; throw "The $Name negative assertion unexpectedly survived its guard-removal mutation." }
    catch { if ($_.Exception.Message -notmatch [regex]::Escape("Negative control '$Name' was accepted")) { throw } }
}

function Assert-PlannedProjection($Record) {
    if ($Record.ledger_check.open_interval_count -ne 0 -or [decimal]$Record.ledger_check.planned_host_projection_usd -ne [decimal]3.612) {
        throw 'Pre-apply ledger omitted the four-host planned reserve projection.'
    }
    $projectedDelta = [decimal]$Record.ledger_check.projected_instance_cost_usd - [decimal]$Record.ledger_check.accrued_instance_cost_usd
    if ([math]::Abs($projectedDelta - [decimal]3.612) -gt [decimal]0.000001) { throw "Planned-host projection delta $projectedDelta did not equal 0.4515*480/60 ($([decimal]3.612))." }
}

try {
    $campaignDir = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f'
    $manifest = Get-Content (Join-Path $campaignDir 'cost-manifest.json') -Raw | ConvertFrom-Json
    $manifestTemplatePath = Join-Path $temp 'campaign-manifest-template.json'

    $planPath = Join-Path $temp 'reviewed-plan.tfplan'
    Write-TestPlanZip $planPath 'gate-fixture-lineage' 0
    $planHash = (Get-FileHash -LiteralPath $planPath -Algorithm SHA256).Hash.ToUpperInvariant()
    $inspection = Get-Content (Join-Path $campaignDir 'cycles/preflight-r173-2330417/plan-inspection.json') -Raw | ConvertFrom-Json
    $inspection.plan_sha256 = $planHash
    $inspection | Add-Member -NotePropertyName plan_prior_state_lineage -NotePropertyValue 'gate-fixture-lineage'
    $inspection | Add-Member -NotePropertyName plan_prior_state_serial -NotePropertyValue 0
    $inspection | Add-Member -NotePropertyName plan_prior_state_terraform_version -NotePropertyValue '1.16.4'
    $inspection | Add-Member -NotePropertyName local_state_lineage -NotePropertyValue 'gate-fixture-lineage'
    $inspection | Add-Member -NotePropertyName local_state_serial -NotePropertyValue 0
    $inspection | Add-Member -NotePropertyName local_state_file_present -NotePropertyValue $true
    $inspection | Add-Member -NotePropertyName terraform_state_path -NotePropertyValue (Join-Path $temp 'terraform.tfstate')
    $inspection | Add-Member -NotePropertyName terraform_executable_path -NotePropertyValue $terraformStubPath
    $inspection | Add-Member -NotePropertyName terraform_executable_sha256 -NotePropertyValue ((Get-FileHash -LiteralPath $terraformStubPath -Algorithm SHA256).Hash.ToUpperInvariant())
    $inspectionPath = Join-Path $temp 'inspection.json'
    $inspection | Add-Member -NotePropertyName compared_with -NotePropertyValue ([ordered]@{
        path = [string]$manifest.preapply_round_104_2330417_plan_inspection.offline_reference_path
        sha256 = 'fixture-baseline-sha256'
        same_resource_addresses_types_actions = $true
    })
    Write-TestJson $inspectionPath $inspection
    $manifest.approved_saved_plan = [ordered]@{
        source_commit = [string]$manifest.terraform_profile.repo_ref
        saved_plan_sha256 = $planHash
        inspection_path = [System.IO.Path]::GetFullPath($inspectionPath)
        inspection_sha256 = (Get-FileHash -LiteralPath $inspectionPath -Algorithm SHA256).Hash.ToUpperInvariant()
        baseline_inspection_path = [string]$manifest.preapply_round_104_2330417_plan_inspection.offline_reference_path
        expected_plan_prior_state_lineage = 'gate-fixture-lineage'
        expected_plan_prior_state_serial = 0
        review_state = 'CANDIDATE_FOR_CLAUDE_REVIEW'
    }
    Write-TestJson $manifestTemplatePath $manifest
    $statePath = Join-Path $temp 'terraform.tfstate'
    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 0; lineage = 'gate-fixture-lineage'; resources = @() })

    $sourceLedger = Get-Content (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Raw | ConvertFrom-Json
    $ledgerPath = Join-Path $temp 'task-wide-ledger.json'
    $awsLog = Join-Path $state 'aws-calls.log'
    $ssmLog = Join-Path $state 'ssm-calls.log'

    $positive = Invoke-Gate -Scenario pass -Name positive -ExpectedHash $planHash
    if ($positive.ExitCode -ne 0) { throw "Pre-apply PASS case failed: $($positive.Text)" }
    $record = Get-Content -LiteralPath $positive.OutputPath -Raw | ConvertFrom-Json
    if ($record.schema -ne 'dur050-pre-apply-gate.v1' -or $record.status -ne 'PASS' -or $record.account_id -ne '372206265946' -or
        $record.region -ne 'us-west-1' -or $record.planned_peak_vcpu -ne 8 -or $record.terraform_state_resource_count -ne 0 -or
        $record.checks.dur050_inventory.tagged_dur050_instance_count -ne 0 -or $record.checks.dur050_inventory.region_wide_non_terminated_instance_count -ne 0 -or $record.checks.dur050_inventory.region_wide_volume_count -ne 0 -or
        $record.checks.budget.notifications.Count -ne 3 -or @($record.checks.budget.notifications | Where-Object { $_.state -ne 'OK' -or $_.comparison_operator -ne 'GREATER_THAN' }).Count -ne 0 -or
        $record.checks.cost_allocation_tags.Task.status -ne 'Active' -or $record.checks.cost_allocation_tags.Environment.status -ne 'Active' -or
        $record.ledger_check.reserve_minutes -ne 480 -or $record.ledger_check.open_interval_count -ne 0 -or
        [decimal]$record.ledger_check.planned_host_projection_usd -ne [decimal]3.612 -or
        -not $record.checks.planned_host_projection.below_cap -or $record.apply_authority -ne 'NONE; Claude go/no-go required' -or
        $record.checks.plan_state_provenance.plan_prior_state_lineage -ne 'gate-fixture-lineage' -or
        $record.checks.plan_state_provenance.plan_prior_state_serial -ne 0 -or
        $record.checks.plan_state_provenance.local_state_lineage -ne 'gate-fixture-lineage' -or
        $record.checks.plan_state_provenance.local_state_serial -ne 0 -or
        $record.checks.plan_state_provenance.terraform_executable_sha256 -ne (Get-FileHash -LiteralPath $terraformStubPath -Algorithm SHA256).Hash.ToUpperInvariant()) {
        throw 'Pre-apply PASS record omitted a required observed value or authority boundary.'
    }
    if ($record.ledger_check.ledger_path_override_used -or $record.ledger_check.ledger_path -ne 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') { throw 'Positive pre-apply case did not use the real task-wide ledger default path.' }
    Assert-PlannedProjection $record
    Write-Host 'PASS: account-only gate records reviewed plan, empty state/inventory, quota, budget, tags, full ledger reserve, and no apply authority.'

    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 1; lineage = 'gate-fixture-lineage'; resources = @() })
    $stateProvenanceFailure = Invoke-Gate -Scenario pass -Name plan-state-serial-mismatch -ExpectedHash $planHash
    Assert-ControlFailed $stateProvenanceFailure 'plan-state-serial-mismatch' 'prior-state serial 0 does not match local-state serial 1'
    $stateMismatchRecord = Get-Content -LiteralPath $stateProvenanceFailure.OutputPath -Raw | ConvertFrom-Json
    if ($stateMismatchRecord.checks.plan_state_provenance.state_matches -or
        $stateMismatchRecord.checks.plan_state_provenance.plan_prior_state_serial -ne 0 -or
        $stateMismatchRecord.checks.plan_state_provenance.local_state_serial -ne 1) {
        throw 'Gate FAIL record did not preserve both mismatching plan and local state serials.'
    }
    Write-Host 'PASS: pre-apply gate re-reads and rejects a local-state serial change after plan inspection.'
    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 0; lineage = 'gate-fixture-lineage'; resources = @() })

    $dryAwsCallsBefore = if (Test-Path $awsLog) { (Get-Content $awsLog).Count } else { 0 }
    $dry = Invoke-Gate -Scenario pass -Name dry-run -ExpectedHash $planHash -DryRun
    if ($dry.ExitCode -ne 0 -or $dry.Text -notmatch 'dur050-pre-apply-gate-dry-run.v1' -or $dry.Text -notmatch 'all non-terminated instance states') {
        throw "Pre-apply DryRun failed to list planned reads: $($dry.Text)"
    }
    $dryAwsCallsAfter = if (Test-Path $awsLog) { (Get-Content $awsLog).Count } else { 0 }
    if ($dryAwsCallsAfter -ne $dryAwsCallsBefore -or (Test-Path $dry.OutputPath) -or (Test-Path $dry.LedgerPath)) { throw 'Pre-apply DryRun performed a read/write or created an evidence record.' }
    Write-Host 'PASS: DryRun lists reads only and creates no AWS, ledger, SSM, or evidence activity.'

    . (Join-Path $PSScriptRoot 'dur050-d022-validator.ps1')
    $preApplyRejected = $false
    try { [void](Read-Dur050D022Preflight -Path $positive.OutputPath -CycleID 'ci-preapply') }
    catch { if ($_.Exception.Message -match 'schema must be dur050-d022-preflight.v1') { $preApplyRejected = $true } else { throw } }
    if (-not $preApplyRejected) { throw 'Shared preparation/reset validator accepted the pre-apply schema.' }
    Write-Host 'PASS: shared preparation/reset validator rejects dur050-pre-apply-gate.v1.'

    foreach ($case in @(
        @{ scenario = 'instance'; name = 'tagged-instance'; message = 'Found 1 non-terminated instance' },
        @{ scenario = 'stopped-instance'; name = 'stopped-tagged-instance'; message = 'Found 1 non-terminated instance' },
        @{ scenario = 'volume'; name = 'tagged-volume'; message = 'Found 1 volume.*Task=DUR-050' },
        @{ scenario = 'project-volume'; name = 'project-tagged-volume'; message = 'Found 1 volume.*Project=durable-engine' },
        @{ scenario = 'available-volume'; name = 'untagged-available-volume'; message = 'available unattached EBS volume' },
        @{ scenario = 'alarm'; name = 'alarm-notification'; message = 'state is ALARM' },
        @{ scenario = 'quota'; name = 'other-account-usage-over-quota'; message = 'Current standard On-Demand use 16 plus planned 8 exceeds quota 16' },
        @{ scenario = 'tags'; name = 'inactive-cost-tag'; message = "Cost-allocation tag 'Task' is not Active" }
    )) {
        $result = Invoke-Gate -Scenario $case.scenario -Name $case.name -ExpectedHash $planHash
        Assert-ControlFailed $result $case.name $case.message
        Write-Host "PASS: $($case.name) is recorded FAIL."
    }

    $badHash = '0000000000000000000000000000000000000000000000000000000000000000'
    $hashMismatch = Invoke-Gate -Scenario pass -Name plan-hash-mismatch -ExpectedHash $badHash
    Assert-ControlFailed $hashMismatch 'plan-hash-mismatch' 'Saved plan SHA-256 does not match approved_saved_plan'
    Write-Host 'PASS: plan hash mismatch is recorded FAIL.'

    $invalidatedPlan = Invoke-Gate -Scenario pass -Name invalidated-approved-plan -ExpectedHash $planHash -ManifestMutation {
        param($candidate)
        $candidate.approved_saved_plan.review_state = 'INVALIDATED_ROUND_110'
    }
    Assert-ControlFailed $invalidatedPlan 'invalidated-approved-plan' 'approved_saved_plan is invalidated'
    Write-Host 'PASS: an otherwise-valid plan marked INVALIDATED is refused.'

    $approvedSerialMismatch = Invoke-Gate -Scenario pass -Name approved-serial-mismatch -ExpectedHash $planHash -ManifestMutation {
        param($candidate)
        $candidate.approved_saved_plan.expected_plan_prior_state_serial = 1
    }
    Assert-ControlFailed $approvedSerialMismatch 'approved-serial-mismatch' 'prior-state serial does not match approved_saved_plan.expected_plan_prior_state_serial'
    Write-Host 'PASS: approved expected serial must match the plan and current state.'

    $wrongExecutableInspectionPath = Join-Path $temp 'inspection-wrong-executable-sha.json'
    $wrongExecutableInspection = Get-Content -LiteralPath $inspectionPath -Raw | ConvertFrom-Json
    $wrongExecutableInspection.terraform_executable_sha256 = 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
    Write-TestJson $wrongExecutableInspectionPath $wrongExecutableInspection
    $wrongExecutableInspectionResult = Invoke-Gate -Scenario pass -Name wrong-inspected-executable-sha -ExpectedHash $planHash `
        -InspectionOverridePath $wrongExecutableInspectionPath -RehashInspection
    Assert-ControlFailed $wrongExecutableInspectionResult 'wrong-inspected-executable-sha' 'Terraform executable SHA-256 at gate time does not match'
    Write-Host 'PASS: an inspection pinned to a different Terraform executable SHA-256 is refused.'

    $nonempty = [ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 0; lineage = 'gate-fixture-lineage'; resources = @([ordered]@{ mode = 'managed'; type = 'aws_vpc'; name = 'unexpected'; provider = 'provider[registry.terraform.io/hashicorp/aws]'; instances = @() }) }
    Write-TestJson $statePath $nonempty
    $stateFailure = Invoke-Gate -Scenario pass -Name nonempty-state -ExpectedHash $planHash
    Assert-ControlFailed $stateFailure 'nonempty-state' 'Terraform state contains 1 resource'
    Write-Host 'PASS: nonempty Terraform state is recorded FAIL.'
    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 0; lineage = 'gate-fixture-lineage'; resources = @() })

    $ledgerCopy = $sourceLedger | ConvertTo-Json -Depth 30 | ConvertFrom-Json
    $ledgerCopy.roles.'app-1'[-1].destroy_completed_at_utc = $null
    Write-TestJson $ledgerPath $ledgerCopy
    $ledgerFailure = Invoke-Gate -Scenario pass -Name ledger-open -ExpectedHash $planHash -LedgerOverride $ledgerPath
    Assert-ControlFailed $ledgerFailure 'ledger-open' 'open_interval_count == 0'
    Write-Host 'PASS: open task-ledger intervals block pre-apply.'
    $longReserve = Invoke-Gate -Scenario pass -Name ledger-10080 -ExpectedHash $planHash -ReserveMinutes 10080
    Assert-ControlFailed $longReserve 'ledger-10080' 'not below the DUR-050 cap'
    Write-Host 'PASS: 10080-minute planned-host reserve exceeds the task cap.'

    $gateSource = Get-Content (Join-Path $PSScriptRoot 'dur050-preapply-gate.ps1') -Raw
    $mutantDir = Join-Path $temp 'mutant/scripts'
    New-Item -ItemType Directory -Force -Path $mutantDir | Out-Null
    Copy-Item (Join-Path $PSScriptRoot 'dur050-d022-shared.ps1') (Join-Path $mutantDir 'dur050-d022-shared.ps1')
    Copy-Item (Join-Path $PSScriptRoot 'dur050-cost-ledger.py') (Join-Path $mutantDir 'dur050-cost-ledger.py')
    Copy-Item (Join-Path $PSScriptRoot 'dur050-d022-validator.ps1') (Join-Path $mutantDir 'dur050-d022-validator.ps1')
    Copy-Item (Join-Path $PSScriptRoot 'dur050-plan-state.psm1') (Join-Path $mutantDir 'dur050-plan-state.psm1')
    $hashGuard = 'if ($planHash -ne ([string]$approvedPlan.saved_plan_sha256).ToUpperInvariant()) { throw "Saved plan SHA-256 does not match approved_saved_plan (actual $planHash)." }'
    $instanceGuard = 'if ($instanceRows.Count -ne 0) { throw "Found $($instanceRows.Count) non-terminated instance(s) tagged Task=DUR-050." }'
    $accountGuard = 'if ([string]$identity.Account -ne ''372206265946'') { throw "AWS account $($identity.Account) does not match required account 372206265946." }'
    $inspectionGuard = 'if ([string]$approvedPlan.inspection_sha256 -ne $inspectionHash) {'
    $projectionGuard = '$ledgerCheck = Invoke-Dur050LedgerCheck -PlannedInstanceTypes $plannedTypes'
    $reviewGuard = '        if ([string]$approvedPlan.review_state -match ''^INVALIDATED'') { throw ''approved_saved_plan is invalidated and cannot be used for a pre-apply gate.'' }'
    $approvedSerialGuardPattern = '(?s)        if \(\[long\]\$planStateSnapshot\.plan_prior_state_serial -ne \[long\]\$approvedPriorSerial\) \{\s*throw ''Saved plan prior-state serial does not match approved_saved_plan\.expected_plan_prior_state_serial\.''\s*\}\s*'
    $executableShaGuardPattern = '(?s)        if \(\[string\]\$inspection\.terraform_executable_sha256 -cne \[string\]\$planStateSnapshot\.terraform_executable_sha256\) \{\s*throw ''Terraform executable SHA-256 at gate time does not match the inspected plan producer executable\.''\s*\}\s*'
    $hashMutant = $gateSource.Replace($hashGuard, '# mutation: expected-hash guard removed')
    $instanceMutant = $gateSource.Replace($instanceGuard, '# mutation: zero-instance guard removed')
    $accountMutant = $gateSource.Replace($accountGuard, '# mutation: early account guard removed').Replace("if (`$record.account_id -ne '372206265946' -or `$record.region -ne 'us-west-1' -or `$record.apply_authority -ne 'NONE; Claude go/no-go required') {", "if (`$record.region -ne 'us-west-1' -or `$record.apply_authority -ne 'NONE; Claude go/no-go required') {")
    $inspectionMutant = $gateSource.Replace($inspectionGuard, 'if ($false) {')
    $projectionMutant = $gateSource.Replace($projectionGuard, '$ledgerCheck = Invoke-Dur050LedgerCheck')
    $reviewMutant = $gateSource.Replace($reviewGuard, '        # mutation: approved review-state guard removed')
    $approvedSerialMutant = [regex]::Replace($gateSource, $approvedSerialGuardPattern, '', 1)
    $executableShaMutant = [regex]::Replace($gateSource, $executableShaGuardPattern, '', 1)
    if ($hashMutant -eq $gateSource -or $instanceMutant -eq $gateSource -or $accountMutant -eq $gateSource -or $inspectionMutant -eq $gateSource -or $projectionMutant -eq $gateSource -or
        $reviewMutant -eq $gateSource -or $approvedSerialMutant -eq $gateSource -or $executableShaMutant -eq $gateSource) { throw 'Could not apply all focused guard-removal mutations.' }
    $hashMutantPath = Join-Path $mutantDir 'dur050-preapply-hash-mutant.ps1'
    $instanceMutantPath = Join-Path $mutantDir 'dur050-preapply-instance-mutant.ps1'
    $accountMutantPath = Join-Path $mutantDir 'dur050-preapply-account-mutant.ps1'
    $inspectionMutantPath = Join-Path $mutantDir 'dur050-preapply-inspection-mutant.ps1'
    $projectionMutantPath = Join-Path $mutantDir 'dur050-preapply-projection-mutant.ps1'
    $reviewMutantPath = Join-Path $mutantDir 'dur050-preapply-review-state-mutant.ps1'
    $approvedSerialMutantPath = Join-Path $mutantDir 'dur050-preapply-approved-serial-mutant.ps1'
    $executableShaMutantPath = Join-Path $mutantDir 'dur050-preapply-executable-sha-mutant.ps1'
    [IO.File]::WriteAllText($hashMutantPath, $hashMutant, $utf8)
    [IO.File]::WriteAllText($instanceMutantPath, $instanceMutant, $utf8)
    [IO.File]::WriteAllText($accountMutantPath, $accountMutant, $utf8)
    [IO.File]::WriteAllText($inspectionMutantPath, $inspectionMutant, $utf8)
    [IO.File]::WriteAllText($projectionMutantPath, $projectionMutant, $utf8)
    [IO.File]::WriteAllText($reviewMutantPath, $reviewMutant, $utf8)
    [IO.File]::WriteAllText($approvedSerialMutantPath, $approvedSerialMutant, $utf8)
    [IO.File]::WriteAllText($executableShaMutantPath, $executableShaMutant, $utf8)
    $expectedHashTest = Invoke-Gate -Scenario pass -Name mutant-hash-guard -ScriptPath $hashMutantPath -ExpectedHash $badHash
    if ($expectedHashTest.ExitCode -ne 0 -or (Get-Content $expectedHashTest.OutputPath -Raw | ConvertFrom-Json).status -ne 'PASS') {
        throw "Removing the plan-hash guard did not make the mismatch control pass: $($expectedHashTest.Text)"
    }
    try { Assert-ControlFailed $expectedHashTest 'plan-hash-mismatch mutation' 'Saved plan SHA-256' ; throw 'The plan-hash negative assertion unexpectedly survived its guard-removal mutation.' }
    catch { if ($_.Exception.Message -notmatch "Negative control 'plan-hash-mismatch mutation' was accepted") { throw } }
    Write-Host 'PASS: removing the plan-hash guard makes the negative assertion fail.'

    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 0; lineage = 'gate-fixture-lineage'; resources = @() })
    $instanceMutantResult = Invoke-Gate -Scenario instance -Name mutant-zero-instance-guard -ScriptPath $instanceMutantPath -ExpectedHash $planHash
    if ($instanceMutantResult.ExitCode -ne 0 -or (Get-Content $instanceMutantResult.OutputPath -Raw | ConvertFrom-Json).status -ne 'PASS') {
        throw "Removing the zero-instance guard did not make the tagged-instance case pass: $($instanceMutantResult.Text)"
    }
    try { Assert-ControlFailed $instanceMutantResult 'tagged-instance mutation' 'Found 1 non-terminated instance' ; throw 'The tagged-instance negative assertion unexpectedly survived its guard-removal mutation.' }
    catch { if ($_.Exception.Message -notmatch "Negative control 'tagged-instance mutation' was accepted") { throw } }
    Write-Host 'PASS: removing the zero-instance guard makes the negative assertion fail.'

    $accountFailure = Invoke-Gate -Scenario account -Name wrong-account -ExpectedHash $planHash
    Assert-ControlFailed $accountFailure 'wrong-account' 'does not match required account'
    $accountMutantResult = Invoke-Gate -Scenario account -Name mutant-account-guard -ScriptPath $accountMutantPath -ExpectedHash $planHash
    if ($accountMutantResult.ExitCode -ne 0 -or (Get-Content $accountMutantResult.OutputPath -Raw | ConvertFrom-Json).status -ne 'PASS') { throw "Removing account guards did not make account negative pass: $($accountMutantResult.Text)" }
    try { Assert-ControlFailed $accountMutantResult 'account mutation' 'account' ; throw 'The account negative assertion unexpectedly survived its guard-removal mutation.' }
    catch { if ($_.Exception.Message -notmatch "Negative control 'account mutation' was accepted") { throw } }
    Write-Host 'PASS: account mismatch is rejected, and removing its guards makes the negative assertion fail.'

    $inspectionCopyPath = Join-Path $temp 'inspection-mutated.json'
    [IO.File]::WriteAllText($inspectionCopyPath, ([IO.File]::ReadAllText($inspectionPath) + " `n"), $utf8)
    $inspectionFailure = Invoke-Gate -Scenario pass -Name inspection-hash-mismatch -ExpectedHash $planHash -InspectionOverridePath $inspectionCopyPath
    Assert-ControlFailed $inspectionFailure 'inspection-hash-mismatch' 'Plan inspection SHA-256 does not match'
    $inspectionMutantResult = Invoke-Gate -Scenario pass -Name mutant-inspection-hash -ScriptPath $inspectionMutantPath -ExpectedHash $planHash -InspectionOverridePath $inspectionCopyPath
    if ($inspectionMutantResult.ExitCode -ne 0 -or (Get-Content $inspectionMutantResult.OutputPath -Raw | ConvertFrom-Json).status -ne 'PASS') { throw "Removing inspection hash guard did not make its negative pass: $($inspectionMutantResult.Text)" }
    try { Assert-ControlFailed $inspectionMutantResult 'inspection hash mutation' 'inspection SHA-256' ; throw 'The inspection-hash negative assertion unexpectedly survived its guard-removal mutation.' }
    catch { if ($_.Exception.Message -notmatch "Negative control 'inspection hash mutation' was accepted") { throw } }
    Write-Host 'PASS: inspection-byte mismatch is rejected, and removing its hash guard makes the negative assertion fail.'

    $projectionMutantResult = Invoke-Gate -Scenario pass -Name mutant-planned-projection -ScriptPath $projectionMutantPath -ExpectedHash $planHash
    if ($projectionMutantResult.ExitCode -ne 0) { throw "Projection mutant did not reach the positive assertion: $($projectionMutantResult.Text)" }
    $mutantRecord = Get-Content $projectionMutantResult.OutputPath -Raw | ConvertFrom-Json
    $projectionAssertionFailed = $false
    try { Assert-PlannedProjection $mutantRecord } catch { if ($_.Exception.Message -match 'omitted the four-host planned reserve') { $projectionAssertionFailed = $true } else { throw } }
    if (-not $projectionAssertionFailed) { throw 'Positive planned-projection assertion survived removal of the projection guard.' }
    Write-Host 'PASS: removing planned-host projection makes the positive projection assertion fail.'

    $reviewMutantResult = Invoke-Gate -Scenario pass -Name mutant-invalidated-review-state -ScriptPath $reviewMutantPath -ExpectedHash $planHash -ManifestMutation {
        param($candidate)
        $candidate.approved_saved_plan.review_state = 'INVALIDATED_ROUND_110'
    }
    Assert-GateMutationSensitive $reviewMutantResult 'invalidated-approved-plan'
    Write-Host 'PASS: removing the review_state guard makes the invalidated-plan negative assertion fail.'

    $approvedSerialMutantResult = Invoke-Gate -Scenario pass -Name mutant-approved-serial -ScriptPath $approvedSerialMutantPath -ExpectedHash $planHash -ManifestMutation {
        param($candidate)
        $candidate.approved_saved_plan.expected_plan_prior_state_serial = 1
    }
    Assert-GateMutationSensitive $approvedSerialMutantResult 'approved-serial-mismatch'
    Write-Host 'PASS: removing the approved expected-serial guard makes its negative assertion fail.'

    $executableShaMutantResult = Invoke-Gate -Scenario pass -Name mutant-executable-sha -ScriptPath $executableShaMutantPath -ExpectedHash $planHash `
        -InspectionOverridePath $wrongExecutableInspectionPath -RehashInspection
    Assert-GateMutationSensitive $executableShaMutantResult 'wrong-inspected-executable-sha'
    Write-Host 'PASS: removing the executable-SHA guard makes its negative assertion fail.'

    if (Test-Path $ssmLog) { throw "A forbidden SSM call was attempted: $(Get-Content $ssmLog -Raw)" }
    Write-Host 'PASS: AWS stub observed zero SSM calls across all positive, negative, and mutation cases.'
} finally {
    if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Recurse -Force }
}
