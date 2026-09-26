$ErrorActionPreference = 'Stop'
if (-not $IsLinux) { throw 'DUR-050 pre-apply gate tests require Ubuntu/Linux.' }
$repoRoot = Split-Path -Parent $PSScriptRoot
$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('dur050-preapply-test-' + [guid]::NewGuid().ToString('N'))
$bin = Join-Path $temp 'bin'
$state = Join-Path $temp 'state'
$utf8 = [System.Text.UTF8Encoding]::new($false)
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
    if printf '%s' "$args" | grep -Fq 'Name=tag:Task,Values=DUR-050'; then
      if [ "$DUR050_SCENARIO" = instance ]; then
        printf '{"Reservations":[{"Instances":[{"InstanceId":"i-11111111111111111","InstanceType":"c7i.large","State":{"Name":"running"},"Tags":[{"Key":"Task","Value":"DUR-050"}]}]}]}\n'
      else
        printf '{"Reservations":[]}\n'
      fi
    elif [ "$DUR050_SCENARIO" = quota ]; then
      printf '{"Reservations":[{"Instances":[{"InstanceId":"i-22222222222222222","InstanceType":"m7i.4xlarge","InstanceLifecycle":null,"State":{"Name":"running"}}]}]}\n'
    else
      printf '{"Reservations":[]}\n'
    fi ;;
  *" ec2 describe-instance-types "*)
    printf '{"InstanceTypes":[{"InstanceType":"m7i.4xlarge","VCpuInfo":{"DefaultVCpus":16}}]}\n' ;;
  *" ec2 describe-volumes "*)
    if [ "$DUR050_SCENARIO" = volume ]; then
      printf '{"Volumes":[{"VolumeId":"vol-0123456789abcdef0","State":"available","Size":40,"Tags":[{"Key":"Task","Value":"DUR-050"}]}]}\n'
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
[IO.File]::WriteAllText((Join-Path $bin 'aws'), $awsStub + [Environment]::NewLine, $utf8)
& chmod 0755 (Join-Path $bin 'aws')
if ($LASTEXITCODE -ne 0) { throw 'Could not mark AWS stub executable.' }

function Write-TestJson([string]$Path, $Value) {
    [IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth 30) + [Environment]::NewLine), $utf8)
}

function Invoke-Gate([string]$Scenario, [string]$Name, [string]$ScriptPath, [string]$ExpectedHash, [switch]$DryRun) {
    if (-not $ScriptPath) { $ScriptPath = Join-Path $PSScriptRoot 'dur050-preapply-gate.ps1' }
    $outputPath = Join-Path $temp "$Name-gate.json"
    $ledgerOutput = Join-Path $temp "$Name-ledger.json"
    $arguments = @(
        '-NoProfile', '-File', $ScriptPath,
        '-SavedPlanPath', $planPath,
        '-ExpectedPlanSha256', $ExpectedHash,
        '-PlanInspectionPath', $inspectionPath,
        '-CampaignManifestPath', $manifestPath,
        '-LedgerCheckPath', $ledgerOutput,
        '-ReserveMinutes', '15',
        '-OutputPath', $outputPath,
        '-TerraformStatePath', $statePath,
        '-LedgerPath', $ledgerPath,
        '-RepositoryRoot', $repoRoot,
        '-PythonExe', 'python3'
    )
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

try {
    $campaignDir = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f'
    $manifest = Get-Content (Join-Path $campaignDir 'cost-manifest.json') -Raw | ConvertFrom-Json
    $manifestPath = Join-Path $temp 'campaign-manifest.json'
    Write-TestJson $manifestPath $manifest

    $planPath = Join-Path $temp 'reviewed-plan.tfplan'
    [IO.File]::WriteAllBytes($planPath, [Text.Encoding]::UTF8.GetBytes('R177 test plan bytes'))
    $planHash = (Get-FileHash -LiteralPath $planPath -Algorithm SHA256).Hash.ToUpperInvariant()
    $inspection = Get-Content (Join-Path $campaignDir 'cycles/preflight-r173-2330417/plan-inspection.json') -Raw | ConvertFrom-Json
    $inspection.plan_sha256 = $planHash
    $inspectionPath = Join-Path $temp 'inspection.json'
    Write-TestJson $inspectionPath $inspection
    $manifest.preapply_round_104_2330417_plan_inspection.inspection_sha256 = (Get-FileHash -LiteralPath $inspectionPath -Algorithm SHA256).Hash.ToUpperInvariant()
    Write-TestJson $manifestPath $manifest
    $statePath = Join-Path $temp 'terraform.tfstate'
    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 0; resources = @() })

    $sourceLedger = Get-Content (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Raw | ConvertFrom-Json
    $ledgerPath = Join-Path $temp 'task-wide-ledger.json'
    $cheapStart = [DateTimeOffset]::UtcNow.AddMinutes(-2).ToString('yyyy-MM-ddTHH:mm:ssZ')
    foreach ($role in @('app-1', 'app-2', 'dependency', 'load-generator')) {
        $type = if ($role -eq 'dependency') { 'm7i.large' } else { 'c7i.large' }
        $sourceLedger.roles.$role = @([ordered]@{
            cycle_id = 'ci-preapply'
            instance_id = 'i-{0}' -f [guid]::NewGuid().ToString('N').Substring(0, 17)
            instance_type = $type
            apply_started_at_utc = $cheapStart
            destroy_completed_at_utc = $null
        })
    }
    Write-TestJson $ledgerPath $sourceLedger
    $awsLog = Join-Path $state 'aws-calls.log'
    $ssmLog = Join-Path $state 'ssm-calls.log'

    $positive = Invoke-Gate -Scenario pass -Name positive -ExpectedHash $planHash
    if ($positive.ExitCode -ne 0) { throw "Pre-apply PASS case failed: $($positive.Text)" }
    $record = Get-Content -LiteralPath $positive.OutputPath -Raw | ConvertFrom-Json
    if ($record.schema -ne 'dur050-pre-apply-gate.v1' -or $record.status -ne 'PASS' -or $record.account_id -ne '372206265946' -or
        $record.region -ne 'us-west-1' -or $record.planned_peak_vcpu -ne 8 -or $record.terraform_state_resource_count -ne 0 -or
        $record.checks.dur050_inventory.instance_count -ne 0 -or $record.checks.dur050_inventory.volume_count -ne 0 -or
        $record.checks.budget.notifications.Count -ne 3 -or @($record.checks.budget.notifications | Where-Object { $_.state -ne 'OK' -or $_.comparison_operator -ne 'GREATER_THAN' }).Count -ne 0 -or
        $record.checks.cost_allocation_tags.Task.status -ne 'Active' -or $record.checks.cost_allocation_tags.Environment.status -ne 'Active' -or
        $record.ledger_check.reserve_minutes -ne 15 -or $record.apply_authority -ne 'NONE; Claude go/no-go required') {
        throw 'Pre-apply PASS record omitted a required observed value or authority boundary.'
    }
    Write-Host 'PASS: account-only gate records reviewed plan, empty state/inventory, quota, budget, tags, full ledger reserve, and no apply authority.'

    $dryAwsCallsBefore = if (Test-Path $awsLog) { (Get-Content $awsLog).Count } else { 0 }
    $dry = Invoke-Gate -Scenario pass -Name dry-run -ExpectedHash $planHash -DryRun
    if ($dry.ExitCode -ne 0 -or $dry.Text -notmatch 'dur050-pre-apply-gate-dry-run.v1' -or $dry.Text -notmatch 'DUR-050 pending/running instances and tagged volumes') {
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
        @{ scenario = 'instance'; name = 'tagged-instance'; message = 'Found 1 pending/running instance' },
        @{ scenario = 'volume'; name = 'tagged-volume'; message = 'Found 1 volume.*Task=DUR-050' },
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
    Assert-ControlFailed $hashMismatch 'plan-hash-mismatch' 'Saved plan SHA-256 does not match expected'
    Write-Host 'PASS: plan hash mismatch is recorded FAIL.'

    $nonempty = [ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 3; resources = @([ordered]@{ mode = 'managed'; type = 'aws_vpc'; name = 'unexpected'; provider = 'provider[registry.terraform.io/hashicorp/aws]'; instances = @() }) }
    Write-TestJson $statePath $nonempty
    $stateFailure = Invoke-Gate -Scenario pass -Name nonempty-state -ExpectedHash $planHash
    Assert-ControlFailed $stateFailure 'nonempty-state' 'Terraform state contains 1 resource'
    Write-Host 'PASS: nonempty Terraform state is recorded FAIL.'
    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 0; resources = @() })

    $ledgerCopy = $sourceLedger | ConvertTo-Json -Depth 30 | ConvertFrom-Json
    foreach ($role in @('app-1', 'app-2', 'dependency', 'load-generator')) {
        foreach ($interval in $ledgerCopy.roles.$role) {
            $interval.apply_started_at_utc = '2020-01-01T00:00:00Z'
            $interval.destroy_completed_at_utc = $null
        }
    }
    Write-TestJson $ledgerPath $ledgerCopy
    $ledgerFailure = Invoke-Gate -Scenario pass -Name ledger-fail -ExpectedHash $planHash
    Assert-ControlFailed $ledgerFailure 'ledger-fail' 'cost-ledger check failed'
    Write-Host 'PASS: ledger FAIL blocks the account gate.'
    Write-TestJson $ledgerPath $sourceLedger

    $gateSource = Get-Content (Join-Path $PSScriptRoot 'dur050-preapply-gate.ps1') -Raw
    $mutantDir = Join-Path $temp 'mutant/scripts'
    New-Item -ItemType Directory -Force -Path $mutantDir | Out-Null
    Copy-Item (Join-Path $PSScriptRoot 'dur050-d022-shared.ps1') (Join-Path $mutantDir 'dur050-d022-shared.ps1')
    Copy-Item (Join-Path $PSScriptRoot 'dur050-cost-ledger.py') (Join-Path $mutantDir 'dur050-cost-ledger.py')
    Copy-Item (Join-Path $PSScriptRoot 'dur050-d022-validator.ps1') (Join-Path $mutantDir 'dur050-d022-validator.ps1')
    $hashGuard = 'if ($planHash -ne $ExpectedPlanSha256.ToUpperInvariant()) { throw "Saved plan SHA-256 does not match expected value (actual $planHash)." }'
    $instanceGuard = 'if ($instanceRows.Count -ne 0) { throw "Found $($instanceRows.Count) pending/running instance(s) tagged Task=DUR-050." }'
    $hashMutant = $gateSource.Replace($hashGuard, '# mutation: expected-hash guard removed')
    $instanceMutant = $gateSource.Replace($instanceGuard, '# mutation: zero-instance guard removed')
    if ($hashMutant -eq $gateSource -or $instanceMutant -eq $gateSource) { throw 'Could not apply both focused guard-removal mutations.' }
    $hashMutantPath = Join-Path $mutantDir 'dur050-preapply-hash-mutant.ps1'
    $instanceMutantPath = Join-Path $mutantDir 'dur050-preapply-instance-mutant.ps1'
    [IO.File]::WriteAllText($hashMutantPath, $hashMutant, $utf8)
    [IO.File]::WriteAllText($instanceMutantPath, $instanceMutant, $utf8)
    $expectedHashTest = Invoke-Gate -Scenario pass -Name mutant-hash-guard -ScriptPath $hashMutantPath -ExpectedHash $badHash
    if ($expectedHashTest.ExitCode -ne 0 -or (Get-Content $expectedHashTest.OutputPath -Raw | ConvertFrom-Json).status -ne 'PASS') {
        throw "Removing the plan-hash guard did not make the mismatch control pass: $($expectedHashTest.Text)"
    }
    try { Assert-ControlFailed $expectedHashTest 'plan-hash-mismatch mutation' 'Saved plan SHA-256' ; throw 'The plan-hash negative assertion unexpectedly survived its guard-removal mutation.' }
    catch { if ($_.Exception.Message -notmatch "Negative control 'plan-hash-mismatch mutation' was accepted") { throw } }
    Write-Host 'PASS: removing the plan-hash guard makes the negative assertion fail.'

    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 3; resources = @() })
    $instanceMutantResult = Invoke-Gate -Scenario instance -Name mutant-zero-instance-guard -ScriptPath $instanceMutantPath -ExpectedHash $planHash
    if ($instanceMutantResult.ExitCode -ne 0 -or (Get-Content $instanceMutantResult.OutputPath -Raw | ConvertFrom-Json).status -ne 'PASS') {
        throw "Removing the zero-instance guard did not make the tagged-instance case pass: $($instanceMutantResult.Text)"
    }
    try { Assert-ControlFailed $instanceMutantResult 'tagged-instance mutation' 'Found 1 pending/running instance' ; throw 'The tagged-instance negative assertion unexpectedly survived its guard-removal mutation.' }
    catch { if ($_.Exception.Message -notmatch "Negative control 'tagged-instance mutation' was accepted") { throw } }
    Write-Host 'PASS: removing the zero-instance guard makes the negative assertion fail.'

    if (Test-Path $ssmLog) { throw "A forbidden SSM call was attempted: $(Get-Content $ssmLog -Raw)" }
    Write-Host 'PASS: AWS stub observed zero SSM calls across all positive, negative, and mutation cases.'
} finally {
    if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Recurse -Force }
}
