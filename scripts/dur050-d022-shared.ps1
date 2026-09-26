# Shared D022 checks used by both the account-only pre-apply gate and the
# post-apply cycle/bootstrap gate. Keep the budget, quota, and ledger contracts
# in one place so neither gate can drift from the other.

function Invoke-Dur050AwsJson {
    param([Parameter(Mandatory)] [string[]]$Arguments, [string]$Region = $script:region)
    $raw = & aws --region $Region @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "Read-only AWS command failed: aws --region $Region $($Arguments -join ' '): $raw" }
    try { return ($raw | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "AWS CLI returned invalid JSON for '$($Arguments -join ' ')'." }
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
    $ledgerCheckedAt = $parsed.checked_at_utc
    if ($ledgerCheckedAt -is [DateTime]) { $ledgerCheckedAt = $ledgerCheckedAt.ToUniversalTime().ToString('o') }
    if ($ledgerCheckedAt -is [DateTimeOffset]) { $ledgerCheckedAt = $ledgerCheckedAt.ToUniversalTime().ToString('o') }
    if ([string]$ledgerCheckedAt -notmatch '(?:Z|[+-][0-9]{2}:[0-9]{2})$') { throw 'Task-wide ledger check omitted checked_at_utc with an explicit UTC offset.' }
    return [ordered]@{ status = 'PASS'; path = [System.IO.Path]::GetFullPath($LedgerCheckPath); checked_at_utc = [string]$ledgerCheckedAt; reserve_minutes = $ReserveMinutes; ledger_path = $parsed.ledger_path_used; ledger_path_override_used = [bool]$parsed.ledger_path_override_used; projected_instance_cost_usd = $parsed.projected_instance_cost_usd; budget_cap_usd = $parsed.budget_cap_usd; check_schema = $parsed.schema }
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
    $items = @($Notifications.Notifications)
    $observed = @($items | ForEach-Object { '{0}|{1}|{2}' -f $_.NotificationType, $_.ThresholdType, ([decimal]$_.Threshold).ToString('0.##', [Globalization.CultureInfo]::InvariantCulture) } | Sort-Object)
    if ($observed.Count -ne 3 -or (($observed -join ',') -ne (@($expected | Sort-Object) -join ','))) { throw 'D022 notification set does not exactly match the three approved ACTUAL/FORECASTED thresholds.' }
    $rows = foreach ($item in $Notifications.Notifications) {
        if ($null -eq $item.PSObject.Properties['NotificationState'] -or [string]::IsNullOrWhiteSpace([string]$item.NotificationState)) {
            throw "Budget notification $($item.NotificationType) $($item.Threshold) is missing NotificationState."
        }
        if ([string]$item.NotificationState -ne 'OK') {
            throw "Budget notification $($item.NotificationType) $($item.Threshold) state is $($item.NotificationState), expected OK."
        }
        if ([string]$item.ComparisonOperator -ne 'GREATER_THAN') {
            throw "Budget notification $($item.NotificationType) $($item.Threshold) ComparisonOperator is '$($item.ComparisonOperator)', expected GREATER_THAN."
        }
        [ordered]@{
            type = [string]$item.NotificationType
            comparison_operator = [string]$item.ComparisonOperator
            threshold_type = [string]$item.ThresholdType
            threshold_usd = [decimal]$item.Threshold
            state = [string]$item.NotificationState
        }
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

function Test-Dur050CostAllocationTags($Tags) {
    $tagMap = @{}
    foreach ($tag in $Tags.CostAllocationTags) { $tagMap[[string]$tag.TagKey] = $tag }
    foreach ($key in @('Task', 'Environment')) {
        if (-not $tagMap.ContainsKey($key) -or $tagMap[$key].Status -ne 'Active') { throw "Cost-allocation tag '$key' is not Active." }
    }
    return [ordered]@{
        Task = [ordered]@{ status = [string]$tagMap.Task.Status; last_updated_at = [string]$tagMap.Task.LastUpdatedDate }
        Environment = [ordered]@{ status = [string]$tagMap.Environment.Status; last_updated_at = [string]$tagMap.Environment.LastUpdatedDate }
        state = 'OK'
    }
}
