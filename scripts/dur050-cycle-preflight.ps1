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
        [pscustomobject]@{ role = 'app-1'; instance_id = [string]$appIds[0]; generator = $false },
        [pscustomobject]@{ role = 'app-2'; instance_id = [string]$appIds[1]; generator = $false },
        [pscustomobject]@{ role = 'dependency'; instance_id = $dependencyId; generator = $false },
        [pscustomobject]@{ role = 'load-generator'; instance_id = [string]$generatorIds[0]; generator = $true }
    )
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

function Invoke-Dur050AwsJson {
    param([Parameter(Mandatory)] [string[]]$Arguments, [string]$Region = $script:region)
    $raw = & aws --region $Region @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "Read-only AWS command failed: aws --region $Region $($Arguments -join ' '): $raw" }
    try { return ($raw | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "AWS CLI returned invalid JSON for '$($Arguments -join ' ')'." }
}

function Get-Dur050BootstrapLines($HostSpec) {
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add('set -euo pipefail')
    $lines.Add('root="${DUR050_BOOTSTRAP_ROOT:-/}"')
    $lines.Add('cloud_init=$(cloud-init status --long 2>&1)')
    $lines.Add('printf ''DUR050_CLOUD_INIT_STATUS_BEGIN\n%s\nDUR050_CLOUD_INIT_STATUS_END\n'' "$cloud_init"')
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
        do {
            Start-Sleep -Seconds 2
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

function Invoke-Dur050LedgerCheck {
    $ledgerScript = Join-Path $PSScriptRoot 'dur050-cost-ledger.py'
    $args = @($ledgerScript, $CampaignManifestPath, '--reserve-minutes', [string]$ReserveMinutes, '--output', $LedgerCheckPath)
    if ($LedgerPath) { $args += @('--ledger-path', $LedgerPath) }
    $output = & $PythonExe @args 2>&1 | Out-String
    $exit = $LASTEXITCODE
    $parsed = $null
    try { $parsed = $output | ConvertFrom-Json -ErrorAction Stop } catch { }
    if ($exit -ne 0 -or $parsed.status -ne 'PASS') { throw "Task-wide cost-ledger check failed (exit $exit): $output" }
    return [ordered]@{ status = 'PASS'; path = [System.IO.Path]::GetFullPath($LedgerCheckPath); reserve_minutes = $ReserveMinutes; ledger_path = $parsed.ledger_path_used; ledger_path_override_used = [bool]$parsed.ledger_path_override_used; projected_instance_cost_usd = $parsed.projected_instance_cost_usd; budget_cap_usd = $parsed.budget_cap_usd; check_schema = $parsed.schema }
}

function Test-Dur050Budget($Budget, $Notifications) {
    $budgetLimit = 0.0
    if ($Budget.Budget.BudgetLimit.Unit -ne 'USD' -or -not [double]::TryParse([string]$Budget.Budget.BudgetLimit.Amount, [ref]$budgetLimit) -or $budgetLimit -ne 200.0) {
        throw 'D022 aggregate budget must remain USD 200.'
    }
    $expected = @(
        'ACTUAL|ABSOLUTE_VALUE|160',
        'ACTUAL|ABSOLUTE_VALUE|76.47',
        'FORECASTED|ABSOLUTE_VALUE|160'
    )
    $observed = @($Notifications.Notifications | ForEach-Object { '{0}|{1}|{2}' -f $_.NotificationType, $_.ThresholdType, ([decimal]$_.Threshold).ToString('0.##', [Globalization.CultureInfo]::InvariantCulture) } | Sort-Object)
    if ($observed.Count -ne 3 -or (($observed -join ',') -ne (@($expected | Sort-Object) -join ','))) { throw 'D022 notification set does not exactly match the three approved ACTUAL/FORECASTED thresholds.' }
    $rows = foreach ($item in $Notifications.Notifications) {
        $subscriberCount = @($item.Subscribers).Count
        if ($subscriberCount -lt 1) { throw "Budget notification $($item.NotificationType) $($item.Threshold) has no subscriber." }
        [ordered]@{ type = $item.NotificationType; threshold_type = $item.ThresholdType; threshold_usd = [decimal]$item.Threshold; subscriber_count = $subscriberCount; state = 'OK' }
    }
    return [ordered]@{ name = 'durable-engine-D022-aggregate-20260923'; limit_usd = $budgetLimit; actual_spend_usd = [decimal]$Budget.Budget.CalculatedSpend.ActualSpend.Amount; forecasted_spend_usd = [decimal]$Budget.Budget.CalculatedSpend.ForecastedSpend.Amount; notifications = @($rows) }
}

function Test-Dur050Quota([double]$PlannedPeakVcpu, $Quota) {
    if ([string]$Quota.Quota.QuotaCode -ne 'L-1216C47A' -or [string]$Quota.Quota.ServiceCode -ne 'ec2') {
        throw 'AWS returned a quota other than EC2 L-1216C47A.'
    }
    $quotaValue = 0.0
    if (-not [double]::TryParse([string]$Quota.Quota.Value, [ref]$quotaValue) -or $quotaValue -lt $PlannedPeakVcpu) { throw "EC2 standard On-Demand quota is less than the plan's $PlannedPeakVcpu vCPU." }
    $instances = Invoke-Dur050AwsJson @('ec2', 'describe-instances', '--filters', 'Name=instance-state-name,Values=pending,running', '--output', 'json')
    $standard = @($instances.Reservations | ForEach-Object { $_.Instances } | ForEach-Object { $_ } | Where-Object { $_.InstanceLifecycle -ne 'spot' -and $_.InstanceType -match '^[ACDHIMRTZ]' })
    $typeNames = @($standard | ForEach-Object { [string]$_.InstanceType } | Sort-Object -Unique)
    $usedVcpu = 0.0
    if ($typeNames.Count -gt 0) {
        $typeArgs = @('ec2', 'describe-instance-types', '--instance-types') + $typeNames + @('--output', 'json')
        $types = Invoke-Dur050AwsJson $typeArgs
        $vcpuByType = @{}
        foreach ($type in $types.InstanceTypes) { $vcpuByType[[string]$type.InstanceType] = [double]$type.VCpuInfo.DefaultVCpus }
        foreach ($instance in $standard) {
            if (-not $vcpuByType.ContainsKey([string]$instance.InstanceType)) { throw "Cannot determine vCPU use for active instance type $($instance.InstanceType)." }
            $usedVcpu += $vcpuByType[[string]$instance.InstanceType]
        }
    }
    if (($usedVcpu + $PlannedPeakVcpu) -gt $quotaValue) { throw "Current standard On-Demand use $usedVcpu plus planned $PlannedPeakVcpu exceeds quota $quotaValue." }
    return [ordered]@{ quota_code = 'L-1216C47A'; quota_vcpu = $quotaValue; current_on_demand_standard_vcpu = $usedVcpu; planned_peak_vcpu = $PlannedPeakVcpu; projected_vcpu = $usedVcpu + $PlannedPeakVcpu; state = 'OK' }
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
    $baseRecord.checks.budget = Test-Dur050Budget $budget $notifications
    $script:awsSnapshot.budget = $baseRecord.checks.budget

    $tags = Invoke-Dur050AwsJson -Arguments @('ce', 'list-cost-allocation-tags', '--tag-keys', 'Task', 'Environment', '--output', 'json') -Region 'us-east-1'
    $tagMap = @{}
    foreach ($tag in $tags.CostAllocationTags) { $tagMap[[string]$tag.TagKey] = $tag }
    foreach ($key in @('Task', 'Environment')) {
        if (-not $tagMap.ContainsKey($key) -or $tagMap[$key].Status -ne 'Active') { throw "Cost-allocation tag '$key' is not Active." }
    }
    $baseRecord.checks.cost_allocation_tags = [ordered]@{
        Task = [ordered]@{ status = [string]$tagMap.Task.Status; last_updated_at = [string]$tagMap.Task.LastUpdatedDate }
        Environment = [ordered]@{ status = [string]$tagMap.Environment.Status; last_updated_at = [string]$tagMap.Environment.LastUpdatedDate }
        state = 'OK'
    }
    $script:awsSnapshot.cost_allocation_tags = $baseRecord.checks.cost_allocation_tags

    foreach ($hostSpec in $hosts) { [void](Invoke-Dur050SsmBootstrap $hostSpec) }
    $baseRecord.bootstrap = @($script:remoteCalls)
    $baseRecord.checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    $baseRecord.status = 'PASS'
    Write-Dur050Json $OutputPath $baseRecord
    $script:cycleRecordWritten = $true
    [void](Read-Dur050D022Preflight -Path $OutputPath -CycleID $CycleID)
    $baseRecord | ConvertTo-Json -Depth 24
    Write-Host 'DUR-050 D022 cycle preflight PASS.'
    exit 0
} catch {
    $errorMessage = $_.Exception.Message
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
