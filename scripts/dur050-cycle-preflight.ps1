[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$TerraformOutputsPath,
    [Parameter(Mandatory)] [string]$PlanInspectionPath,
    [Parameter(Mandatory)] [string]$CycleID,
    [Parameter(Mandatory)] [string]$CampaignManifestPath,
    [Parameter(Mandatory)] [string]$LedgerCheckPath,
    [Parameter(Mandatory)] [string]$OutputPath,
    [Parameter(Mandatory)] [ValidateRange(1, 1440)] [int]$ReserveMinutes,
    [string]$LedgerPath,
    [string]$PythonExe = 'python',
    [int]$SsmTimeoutMinutes = 10,
    [switch]$DryRun,
    [string]$DryRunOutputPath
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'dur050-ssm-wrapper.ps1')
. (Join-Path $PSScriptRoot 'dur050-d022-validator.ps1')
. (Join-Path $PSScriptRoot 'dur050-d022-shared.ps1')
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$script:region = 'us-west-1'
$script:stageChecks = [System.Collections.Generic.List[object]]::new()
$script:awsSnapshot = [ordered]@{}
$script:remoteCalls = [System.Collections.Generic.List[object]]::new()
$script:status = 'FAIL'
$script:cycleRecordWritten = $false
$utf8 = [System.Text.UTF8Encoding]::new($false)

function Write-Dur050Json([string]$Path, $Value) {
    if (Test-Path -LiteralPath $Path) { throw "Refusing to overwrite evidence: $Path" }
    $parent = Split-Path -Parent $Path
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    [System.IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth 24) + "`n"), $utf8)
}

function Get-Dur050OutputValue($Outputs, [string]$Name) {
    if ($null -eq $Outputs.$Name -or $null -eq $Outputs.$Name.value) { throw "Terraform output '$Name' is missing." }
    return $Outputs.$Name.value
}

function Get-Dur050HostSet($Outputs) {
    $appIds = @(Get-Dur050OutputValue $Outputs 'app_instance_ids')
    $dependencyId = [string](Get-Dur050OutputValue $Outputs 'dependency_instance_id')
    $generatorIds = @(Get-Dur050OutputValue $Outputs 'load_generator_instance_ids')
    if ($appIds.Count -ne 2 -or $generatorIds.Count -ne 1) { throw 'Terraform outputs must identify two app instances, one dependency, and one load generator.' }
    $all = @($appIds) + @($dependencyId) + @($generatorIds[0])
    if (($all | Select-Object -Unique).Count -ne 4 -or @($all | Where-Object { $_ -notmatch '^i-[0-9a-f]{17}$' }).Count -ne 0) {
        throw 'Terraform outputs must contain four distinct canonical EC2 instance IDs.'
    }
    return @(
        [pscustomobject]@{ role = 'app-1'; instance_id = [string]$appIds[0]; instance_type = 'c7i.large'; generator = $false },
        [pscustomobject]@{ role = 'app-2'; instance_id = [string]$appIds[1]; instance_type = 'c7i.large'; generator = $false },
        [pscustomobject]@{ role = 'dependency'; instance_id = $dependencyId; instance_type = 'm7i.large'; generator = $false },
        [pscustomobject]@{ role = 'load-generator'; instance_id = [string]$generatorIds[0]; instance_type = 'c7i.large'; generator = $true }
    )
}

function Assert-Dur050LedgerMatchesOutputs($Outputs, [string]$ExpectedCycleID) {
    $ledgerFile = if ($LedgerPath) { $LedgerPath } else { Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json' }
    if (-not (Test-Path -LiteralPath $ledgerFile -PathType Leaf)) { throw "Task-wide ledger is missing: $ledgerFile" }
    $ledger = Get-Content -LiteralPath $ledgerFile -Raw | ConvertFrom-Json -ErrorAction Stop
    if ([string]$ledger.schema -ne 'dur050-task-cost-ledger.v1' -or [string]$ledger.task_id -ne 'DUR-050') { throw 'Task-wide ledger has an unsupported schema or task ID.' }
    $expected = @(Get-Dur050HostSet $Outputs)
    $observed = @()
    foreach ($hostSpec in $expected) {
        $rows = @($ledger.roles.($hostSpec.role) | Where-Object { $null -eq $_.destroy_completed_at_utc })
        if ($rows.Count -ne 1) { throw "Ledger must contain exactly one open interval for $($hostSpec.role); found $($rows.Count)." }
        $row = $rows[0]
        if ([string]$row.instance_id -ne $hostSpec.instance_id) { throw "Open ledger interval for $($hostSpec.role) does not match Terraform instance $($hostSpec.instance_id)." }
        if ([string]$row.instance_type -ne $hostSpec.instance_type) { throw "Open ledger interval for $($hostSpec.role) has type $($row.instance_type), expected $($hostSpec.instance_type)." }
        if ([string]$row.cycle_id -ne $ExpectedCycleID) { throw "Open ledger interval for $($hostSpec.role) has a different cycle_id." }
        $observed += [ordered]@{ role = $hostSpec.role; instance_id = $hostSpec.instance_id; instance_type = $hostSpec.instance_type; cycle_id = [string]$row.cycle_id }
    }
    $otherOpen = @()
    foreach ($role in @('app-1','app-2','dependency','load-generator')) {
        $otherOpen += @($ledger.roles.$role | Where-Object { $null -eq $_.destroy_completed_at_utc -and [string]$_.cycle_id -ne $ExpectedCycleID })
    }
    if ($otherOpen.Count -ne 0) { throw 'Task-wide ledger contains open intervals from another cycle.' }
    return [ordered]@{ state = 'OK'; path = [System.IO.Path]::GetFullPath($ledgerFile); expected_cycle_id = $ExpectedCycleID; open_interval_count = $observed.Count; hosts = $observed }
}

function Get-Dur050RootVolumeObservation($Hosts) {
    $instanceIds = @($Hosts | ForEach-Object { $_.instance_id })
    $instances = Invoke-Dur050AwsJson (@('ec2','describe-instances','--instance-ids') + $instanceIds + @('--output','json'))
    $instanceRows = @($instances.Reservations | ForEach-Object { $_.Instances } | ForEach-Object { $_ })
    $volumes = Invoke-Dur050AwsJson (@('ec2','describe-volumes','--filters','Name=attachment.instance-id,Values=' + ($instanceIds -join ','),'--output','json'))
    $rootRows = @()
    foreach ($hostSpec in $Hosts) {
        $instance = @($instanceRows | Where-Object { [string]$_.InstanceId -eq $hostSpec.instance_id })
        if ($instance.Count -ne 1) { throw "Cannot identify exactly one applied EC2 instance for root-volume audit: $($hostSpec.instance_id)." }
        $rootDevice = [string]$instance[0].RootDeviceName
        if (-not $rootDevice) { throw "EC2 did not report RootDeviceName for $($hostSpec.instance_id)." }
        $matchingVolumes = @($volumes.Volumes | Where-Object {
            @($_.Attachments | Where-Object { [string]$_.InstanceId -eq $hostSpec.instance_id -and [string]$_.Device -eq $rootDevice }).Count -gt 0
        })
        if ($matchingVolumes.Count -ne 1) { throw "Cannot identify exactly one root EBS volume for $($hostSpec.instance_id) at $rootDevice." }
        $taskTag = @($matchingVolumes[0].Tags | Where-Object Key -eq 'Task' | Select-Object -First 1)
        $rootRows += [ordered]@{ role = $hostSpec.role; instance_id = $hostSpec.instance_id; root_device = $rootDevice; volume_id = [string]$matchingVolumes[0].VolumeId; task_tag_present = ($taskTag.Count -gt 0); task_tag_value = if ($taskTag.Count -gt 0) { [string]$taskTag[0].Value } else { $null } }
    }
    return [ordered]@{ root_volumes = $rootRows; all_root_volumes_tagged_task_dur050 = (@($rootRows | Where-Object { -not $_.task_tag_present -or $_.task_tag_value -ne 'DUR-050' }).Count -eq 0) }
}

function Get-Dur050PeakVcpu($Plan) {
    $value = $Plan.planned_peak_vcpu
    if ($null -eq $value -and $null -ne $Plan.terraform_plan.inspection) { $value = $Plan.terraform_plan.inspection.planned_peak_vcpu }
    if ($null -eq $value -and $null -ne $Plan.terraform_plan.inspection.instances) { $value = $Plan.terraform_plan.inspection.instances.peak_vcpu }
    if ($null -eq $value -and $null -ne $Plan.terraform_plan.source_profile) { $value = $Plan.terraform_plan.source_profile.planned_peak_vcpu }
    if ($null -eq $value -and $null -ne $Plan.source_profile) { $value = $Plan.source_profile.planned_peak_vcpu }
    if ($null -eq $value -and $null -ne $Plan.profile) { $value = $Plan.profile.planned_peak_vcpu }
    $peak = 0.0
    if ($null -eq $value -or $value -is [bool] -or $value -isnot [ValueType] -or
        -not [double]::TryParse([string]$value, [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$peak) -or
        [double]::IsNaN($peak) -or [double]::IsInfinity($peak)) {
        throw 'Recorded Terraform plan inspection must contain numeric planned_peak_vcpu.'
    }
    if ($peak -le 0 -or $peak -gt 32) { throw 'Recorded planned_peak_vcpu must be in (0, 32].' }
    return $peak
}

function Get-Dur050BootstrapLines($HostSpec) {
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add('set -euo pipefail')
    $lines.Add('root="${DUR050_BOOTSTRAP_ROOT:-/}"')
    $lines.Add('cloud_init_rc=0')
    $lines.Add('cloud_init=$(cloud-init status --long 2>&1) || cloud_init_rc=$?')
    $lines.Add('printf ''DUR050_CLOUD_INIT_STATUS_BEGIN\n%s\nDUR050_CLOUD_INIT_STATUS_END\n'' "$cloud_init"')
    $lines.Add('test "$cloud_init_rc" -eq 0')
    $lines.Add('printf ''%s\n'' "$cloud_init" | grep -Fxq ''status: done''')
    $lines.Add('printf ''%s\n'' "$cloud_init" | grep -Fxq ''errors: []''')
    $lines.Add('printf ''%s\n'' "$cloud_init" | grep -Eq ''^recoverable_errors: (\[\]|\{\})$''')
    if ($HostSpec.generator) {
        $lines.Add('test -e "$root/var/lib/durable-dur050-generator-bootstrap-complete"')
        $lines.Add('test -x "$root/opt/durable-agent-execution-engine/bin/dur050-observer"')
        $lines.Add('test -x "$root/opt/durable-agent-execution-engine/bin/dur050-loadgen"')
        $lines.Add('test -x "$root/opt/durable-agent-execution-engine/bin/dur050-sink"')
    } else {
        $lines.Add('test -e "$root/var/lib/durable-dur050-bootstrap-complete"')
    }
    $lines.Add(("printf 'DUR050_BOOTSTRAP_PASS role={0}\n'" -f $HostSpec.role))
    return $lines.ToArray()
}

function Invoke-Dur050SsmBootstrap($HostSpec) {
    $lines = @(Get-Dur050BootstrapLines $HostSpec)
    $wrapped = New-Dur050SsmBashCommand -Stage ("cycle-preflight-bootstrap-{0}" -f $HostSpec.role) -RemoteLines $lines
    $inputFile = Join-Path ([System.IO.Path]::GetTempPath()) ("dur050-preflight-{0}.json" -f [guid]::NewGuid().ToString('N'))
    $started = [DateTimeOffset]::UtcNow
    try {
        $request = @{ DocumentName = 'AWS-RunShellScript'; InstanceIds = @($HostSpec.instance_id); Comment = "DUR-050 cycle preflight $CycleID $($HostSpec.role)"; Parameters = @{ commands = @($wrapped) } } | ConvertTo-Json -Depth 8 -Compress
        [System.IO.File]::WriteAllText($inputFile, $request, $utf8)
        $sent = Invoke-Dur050AwsJson @('ssm', 'send-command', '--cli-input-json', "file://$inputFile", '--output', 'json')
        $commandId = [string]$sent.Command.CommandId
        if (-not $commandId) { throw "SSM did not return a command ID for $($HostSpec.role)." }
        $deadline = [DateTimeOffset]::UtcNow.AddMinutes($SsmTimeoutMinutes)
        $result = $null
        $pollMilliseconds = if ($env:DUR050_ENABLE_TEST_HOOKS -eq '1') { 20 } else { 2000 }
        do {
            Start-Sleep -Milliseconds $pollMilliseconds
            try { $result = Invoke-Dur050AwsJson @('ssm', 'get-command-invocation', '--command-id', $commandId, '--instance-id', $HostSpec.instance_id, '--output', 'json') }
            catch { if ([DateTimeOffset]::UtcNow -ge $deadline) { throw }; continue }
            if ($result.Status -in @('Success', 'Failed', 'Cancelled', 'TimedOut', 'Undeliverable', 'Terminated')) { break }
        } while ([DateTimeOffset]::UtcNow -lt $deadline)
        if ($null -eq $result -or $result.Status -ne 'Success') {
            $state = if ($null -eq $result) { 'no invocation response before timeout' } else { "$($result.Status): $($result.StandardErrorContent)" }
            throw "Bootstrap gate failed on $($HostSpec.role) ($($HostSpec.instance_id)): $state"
        }
        if ([string]$result.StandardOutputContent -notmatch ("DUR050_BOOTSTRAP_PASS role={0}" -f [regex]::Escape($HostSpec.role))) { throw "Bootstrap output omitted the role marker for $($HostSpec.role)." }
        $remoteCall = [ordered]@{ role = $HostSpec.role; instance_id = $HostSpec.instance_id; command_id = $commandId; status = $result.Status; checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o'); remote_shell = 'New-Dur050SsmBashCommand; UTF-8/LF temp-file wrapper; bash'; remote_lines = $lines; standard_output = [string]$result.StandardOutputContent }
        $script:remoteCalls.Add($remoteCall)
        return $remoteCall
    } finally {
        Remove-Item -LiteralPath $inputFile -Force -ErrorAction SilentlyContinue
    }
}

try {
    $script:plannedVcpu = $null
    if ($CycleID -notmatch '^[A-Za-z0-9-]{1,32}$') { throw 'CycleID must be a simple 1-32 character identifier.' }
    foreach ($requiredPath in @($TerraformOutputsPath, $PlanInspectionPath, $CampaignManifestPath)) {
        if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) { throw "Required preflight input is missing: $requiredPath" }
    }
    if (Test-Path -LiteralPath $OutputPath) { throw "Refusing to overwrite preflight record: $OutputPath" }
    if (Test-Path -LiteralPath $LedgerCheckPath) { throw "Refusing to overwrite ledger check: $LedgerCheckPath" }
    if ($LedgerPath -and $env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE -ne '1') { throw 'A non-default ledger path is test-only and requires DUR050_ENABLE_TEST_LEDGER_OVERRIDE=1.' }

    $outputs = Get-Content -LiteralPath $TerraformOutputsPath -Raw | ConvertFrom-Json -ErrorAction Stop
    $hosts = @(Get-Dur050HostSet $outputs)
    $planInspection = Get-Content -LiteralPath $PlanInspectionPath -Raw | ConvertFrom-Json -ErrorAction Stop
    $plannedVcpu = Get-Dur050PeakVcpu $planInspection
    $script:plannedVcpu = $plannedVcpu
    $script:awsSnapshot.ledger_instance_crosscheck = Assert-Dur050LedgerMatchesOutputs -Outputs $outputs -ExpectedCycleID $CycleID
    $ledgerCheck = Invoke-Dur050LedgerCheck

    $bootstrapCommands = @($hosts | ForEach-Object {
        $remote = @(Get-Dur050BootstrapLines $_)
        [ordered]@{ role = $_.role; instance_id = $_.instance_id; remote_lines = $remote; wrapped_command = (New-Dur050SsmBashCommand -Stage ("cycle-preflight-bootstrap-{0}" -f $_.role) -RemoteLines $remote) }
    })
    $baseRecord = [ordered]@{
        schema = 'dur050-d022-preflight.v1'
        status = 'FAIL'
        account_id = 'unknown'
        region = $script:region
        planned_peak_vcpu = $plannedVcpu
        cycle_id = $CycleID
        checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        ledger_check_path = [System.IO.Path]::GetFullPath($LedgerCheckPath)
        reserve_minutes = $ReserveMinutes
        ledger_check = $ledgerCheck
        checks = [ordered]@{}
        bootstrap = @()
    }
    $baseRecord.checks.ledger_instance_crosscheck = $script:awsSnapshot.ledger_instance_crosscheck

    if ($DryRun) {
        $dry = [ordered]@{
            schema = 'dur050-cycle-preflight-dry-run.v1'
            classification = 'DRY RUN ONLY - AWS calls and SSM dispatch are not performed; this is not a PASS D022 preflight.'
            cycle_id = $CycleID
            planned_peak_vcpu = $plannedVcpu
            ledger_check_path = $baseRecord.ledger_check_path
            ledger_check = $ledgerCheck
            terraform_outputs_sha256 = (Get-FileHash -LiteralPath $TerraformOutputsPath -Algorithm SHA256).Hash.ToLowerInvariant()
            plan_inspection_sha256 = (Get-FileHash -LiteralPath $PlanInspectionPath -Algorithm SHA256).Hash.ToLowerInvariant()
            planned_aws_checks = @('STS account', 'EC2 On-Demand Standard vCPU quota and current usage', 'D022 budget cap and three notifications', 'Task/Environment cost-allocation tags')
            bootstrap_commands = $bootstrapCommands
        }
        if ($DryRunOutputPath) { Write-Dur050Json $DryRunOutputPath $dry }
        $dry | ConvertTo-Json -Depth 24
        exit 0
    }

    $identity = Invoke-Dur050AwsJson @('sts', 'get-caller-identity', '--output', 'json')
    $baseRecord.account_id = [string]$identity.Account
    if ($baseRecord.account_id -ne '372206265946') { throw "AWS identity account $($baseRecord.account_id) does not match D022 account 372206265946." }
    $baseRecord.checks.sts = [ordered]@{ state = 'OK'; account_id = $baseRecord.account_id; arn = [string]$identity.Arn }
    $script:awsSnapshot.sts = $baseRecord.checks.sts

    $quota = Invoke-Dur050AwsJson @('service-quotas', 'get-service-quota', '--service-code', 'ec2', '--quota-code', 'L-1216C47A', '--output', 'json')
    $baseRecord.checks.quota = Test-Dur050Quota -PlannedPeakVcpu $plannedVcpu -Quota $quota
    $script:awsSnapshot.quota = $baseRecord.checks.quota

    $budget = Invoke-Dur050AwsJson @('budgets', 'describe-budget', '--account-id', '372206265946', '--budget-name', 'durable-engine-D022-aggregate-20260923', '--output', 'json')
    $notifications = Invoke-Dur050AwsJson @('budgets', 'describe-notifications-for-budget', '--account-id', '372206265946', '--budget-name', 'durable-engine-D022-aggregate-20260923', '--output', 'json')
    $script:awsSnapshot.budget_notification_observations = @($notifications.Notifications | ForEach-Object {
        [ordered]@{
            notification_type = [string]$_.NotificationType
            comparison_operator = [string]$_.ComparisonOperator
            threshold_type = [string]$_.ThresholdType
            threshold_usd = if ($null -eq $_.Threshold) { $null } else { [decimal]$_.Threshold }
            state = if ($null -eq $_.PSObject.Properties['NotificationState']) { $null } else { [string]$_.NotificationState }
        }
    })
    $baseRecord.checks.budget = Test-Dur050Budget $budget $notifications
    $script:awsSnapshot.budget = $baseRecord.checks.budget

    $tags = Invoke-Dur050AwsJson -Arguments @('ce', 'list-cost-allocation-tags', '--tag-keys', 'Task', 'Environment', '--output', 'json') -Region 'us-east-1'
    $baseRecord.checks.cost_allocation_tags = Test-Dur050CostAllocationTags $tags
    $script:awsSnapshot.cost_allocation_tags = $baseRecord.checks.cost_allocation_tags

    $baseRecord.checks.root_volume_tags = Get-Dur050RootVolumeObservation -Hosts $hosts
    $script:awsSnapshot.root_volume_tags = $baseRecord.checks.root_volume_tags

    foreach ($hostSpec in $hosts) { [void](Invoke-Dur050SsmBootstrap $hostSpec) }
    $baseRecord.bootstrap = @($script:remoteCalls)
    $baseRecord.checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    $baseRecord.status = 'PASS'
    $outputParent = Split-Path -Parent $OutputPath
    if ($outputParent) { New-Item -ItemType Directory -Force -Path $outputParent | Out-Null }
    $script:passTempPath = "$OutputPath.$([guid]::NewGuid().ToString('N')).tmp"
    Write-Dur050Json $script:passTempPath $baseRecord
    [void](Read-Dur050D022Preflight -Path $script:passTempPath -CycleID $CycleID)
    [System.IO.File]::Move($script:passTempPath, $OutputPath)
    $script:passTempPath = $null
    $script:cycleRecordWritten = $true
    $baseRecord | ConvertTo-Json -Depth 24
    Write-Host 'DUR-050 D022 cycle preflight PASS.'
    exit 0
} catch {
    $errorMessage = $_.Exception.Message
    if ($script:passTempPath -and (Test-Path -LiteralPath $script:passTempPath)) {
        Remove-Item -LiteralPath $script:passTempPath -Force -ErrorAction SilentlyContinue
        $script:passTempPath = $null
    }
    $failure = [ordered]@{
        schema = 'dur050-d022-preflight.v1'
        status = 'FAIL'
        account_id = if ($script:awsSnapshot.account_id) { $script:awsSnapshot.account_id } else { 'unknown' }
        region = $script:region
        cycle_id = $CycleID
        checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        planned_peak_vcpu = $script:plannedVcpu
        error = $errorMessage
        checks = $script:awsSnapshot
        bootstrap = @($script:remoteCalls)
    }
    if (-not $script:cycleRecordWritten -and -not (Test-Path -LiteralPath $OutputPath)) {
        try { Write-Dur050Json $OutputPath $failure } catch { Write-Error $_ }
    }
    Write-Error $errorMessage
    exit 1
}
