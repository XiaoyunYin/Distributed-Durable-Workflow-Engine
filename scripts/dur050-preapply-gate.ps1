#Requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$SavedPlanPath,
    [Parameter(Mandatory)] [string]$PlanInspectionPath,
    [Parameter(Mandatory)] [string]$CampaignManifestPath,
    [Parameter(Mandatory)] [string]$LedgerCheckPath,
    [Parameter(Mandatory)] [ValidateRange(1, 10080)] [int]$ReserveMinutes,
    [Parameter(Mandatory)] [string]$OutputPath,
    [string]$TerraformStatePath,
    [string]$TerraformExe = 'terraform',
    [string]$LedgerPath,
    [string]$RepositoryRoot,
    [string]$PythonExe = 'python',
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$defaultRepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if ($RepositoryRoot) {
    if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'RepositoryRoot override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
    $repoRoot = (Resolve-Path -LiteralPath $RepositoryRoot).Path
} else { $repoRoot = $defaultRepoRoot }
$script:region = 'us-west-1'
$script:CampaignManifestPath = $CampaignManifestPath
$script:ReserveMinutes = $ReserveMinutes
$script:LedgerCheckPath = $LedgerCheckPath
$script:LedgerPath = $LedgerPath
$script:PythonExe = $PythonExe
$script:observed = [ordered]@{}
$script:observed.powershell = [ordered]@{
    path = [System.IO.Path]::GetFullPath([Environment]::ProcessPath)
    version = $PSVersionTable.PSVersion.ToString()
    edition = [string]$PSVersionTable.PSEdition
}
$script:recordWritten = $false
$utf8 = [System.Text.UTF8Encoding]::new($false)
if (-not $TerraformStatePath) { $TerraformStatePath = Join-Path $repoRoot 'deploy/aws/terraform.tfstate' }

. (Join-Path $PSScriptRoot 'dur050-d022-shared.ps1')
Import-Module (Join-Path $PSScriptRoot 'dur050-plan-state.psm1') -Force

function Write-Dur050PreApplyJson([string]$Path, $Value) {
    $parent = Split-Path -Parent $Path
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    $json = ($Value | ConvertTo-Json -Depth 28).Replace("`r", '')
    [System.IO.File]::WriteAllText($Path, ($json + "`n"), $utf8)
}

function Get-Dur050Numeric([object]$Value, [string]$Name) {
    $number = 0.0
    if ($null -eq $Value -or $Value -is [bool] -or $Value -isnot [ValueType] -or
        -not [double]::TryParse([string]$Value, [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$number) -or
        [double]::IsNaN($number) -or [double]::IsInfinity($number)) {
        throw "$Name must be numeric."
    }
    return $number
}

function Resolve-Dur050ManifestPath([string]$Path) {
    if ([System.IO.Path]::IsPathRooted($Path)) { return [System.IO.Path]::GetFullPath($Path) }
    return [System.IO.Path]::GetFullPath((Join-Path $repoRoot $Path))
}

function Assert-Dur050PlanInspection($Inspection, [string]$ActualPlanHash, $Manifest, [string]$BaselinePath) {
    $approved = $Manifest.approved_saved_plan
    $expectedRepoRef = [string]$approved.source_commit
    if ($expectedRepoRef -notmatch '^[0-9a-f]{40}$') { throw 'approved_saved_plan.source_commit must be a full lowercase commit SHA.' }
    if ([string]$Inspection.schema -ne 'dur050-recorded-plan-inspection.v1') { throw 'Plan inspection schema must be dur050-recorded-plan-inspection.v1.' }
    if ([string]$Inspection.source_commit -ne $expectedRepoRef) { throw 'Plan inspection source_commit does not match the campaign manifest repo_ref.' }
    if ([string]$Inspection.aws_region -ne 'us-west-1' -or [string]$Inspection.repo_ref -ne $expectedRepoRef) { throw 'Plan inspection region/repo_ref does not match the approved campaign profile.' }
    if ([string]$Inspection.plan_sha256 -ne $ActualPlanHash) { throw 'Plan inspection plan_sha256 does not match the saved plan.' }
    if ([string]$Inspection.compared_with.path -ne [string]$approved.baseline_inspection_path) { throw 'Plan inspection baseline path does not match approved_saved_plan.baseline_inspection_path.' }
    $changes = $Inspection.resource_changes
    $counts = [ordered]@{
        resource_changes = [int]$Inspection.resource_change_count
        creates = [int]$changes.creates
        updates = [int]$changes.updates
        deletes = [int]$changes.deletes
        replacements = [int]$changes.replacements
        instances = [int]$Inspection.instances.total
        c7i_large = [int]$Inspection.instances.'c7i.large'
        m7i_large = [int]$Inspection.instances.'m7i.large'
        planned_peak_vcpu = Get-Dur050Numeric $Inspection.planned_peak_vcpu 'Plan inspection planned_peak_vcpu'
        taggable_resources = [int]$Inspection.taggable_resources_with_effective_tags
        correctly_tagged_resources = [int]$Inspection.taggable_resources_with_expected_task_environment_expiry_tags
        prohibited_resources = [int]$Inspection.prohibited_eks_nat_gateway_or_load_balancer_resources
        state_resources = [int]$Inspection.terraform_state_resource_count
        addresses_and_types = @($Inspection.resource_addresses_and_types).Count
    }
    if ($counts.resource_changes -ne 31 -or $counts.creates -ne 31 -or $counts.updates -ne 0 -or $counts.deletes -ne 0 -or $counts.replacements -ne 0) {
        throw "Plan inspection actions differ from 31 creates and zero other actions: $($counts | ConvertTo-Json -Compress)."
    }
    if ($counts.instances -ne 4 -or $counts.c7i_large -ne 3 -or $counts.m7i_large -ne 1 -or $counts.planned_peak_vcpu -ne 8) {
        throw "Plan inspection instance sizes/count/peak vCPU differ from the reviewed 4-host, 8-vCPU profile: $($counts | ConvertTo-Json -Compress)."
    }
    if ($counts.taggable_resources -ne 23 -or $counts.correctly_tagged_resources -ne 23 -or $counts.prohibited_resources -ne 0 -or $counts.state_resources -ne 0 -or $counts.addresses_and_types -ne 31) {
        throw "Plan inspection tags, state, resource-address, or prohibited-resource checks differ from the reviewed plan: $($counts | ConvertTo-Json -Compress)."
    }
    $addresses = @($Inspection.resource_addresses_and_types | ForEach-Object { [string]$_.address })
    if (($addresses | Select-Object -Unique).Count -ne 31) { throw 'Plan inspection contains duplicate resource addresses.' }
    if (-not (Test-Path -LiteralPath $BaselinePath -PathType Leaf)) { throw "Reviewed offline plan baseline is missing: $BaselinePath" }
    $baseline = Get-Content -LiteralPath $BaselinePath -Raw | ConvertFrom-Json -ErrorAction Stop
    $currentSignature = @($Inspection.resource_addresses_and_types | ForEach-Object {
        '{0}|{1}|{2}' -f $_.address, $_.type, (@($_.actions) -join ',')
    } | Sort-Object)
    $baselineSignature = @($baseline.resource_addresses_and_types | ForEach-Object {
        '{0}|{1}|{2}' -f $_.address, $_.type, (@($_.actions) -join ',')
    } | Sort-Object)
    if (($currentSignature -join [Environment]::NewLine) -cne ($baselineSignature -join [Environment]::NewLine)) {
        throw 'Plan inspection resource addresses, types, or actions differ from the reviewed offline inspection.'
    }
    $script:observed.plan_inspection_baseline = [ordered]@{
        path = [System.IO.Path]::GetFullPath($BaselinePath)
        resource_addresses_types_actions_match = $true
        resource_count = $baselineSignature.Count
    }
    return $counts
}

function Test-Dur050NoTaggedResources($Instances, $Volumes) {
    $reservationInstances = @($Instances.Reservations | ForEach-Object { $_.Instances } | ForEach-Object { $_ })
    $states = @('pending', 'running', 'stopping', 'stopped', 'shutting-down')
    $nonTerminated = @($reservationInstances | Where-Object { $_.State.Name -in $states })
    $taskInstances = @($nonTerminated | Where-Object { @($_.Tags | Where-Object { $_.Key -eq 'Task' -and $_.Value -eq 'DUR-050' }).Count -gt 0 })
    $allVolumes = @($Volumes.Volumes)
    $forbiddenVolumes = @($allVolumes | Where-Object {
        $tags = @($_.Tags)
        (@($tags | Where-Object { $_.Key -eq 'Task' -and $_.Value -eq 'DUR-050' }).Count -gt 0) -or
        (@($tags | Where-Object { $_.Key -eq 'Project' -and $_.Value -eq 'durable-engine' }).Count -gt 0) -or
        ([string]$_.State -eq 'available')
    })
    $instanceRows = @($taskInstances | ForEach-Object { [ordered]@{ instance_id = [string]$_.InstanceId; state = [string]$_.State.Name; instance_type = [string]$_.InstanceType } })
    $volumeRows = @($forbiddenVolumes | ForEach-Object { [ordered]@{ volume_id = [string]$_.VolumeId; state = [string]$_.State; size_gib = [int]$_.Size; tags = @($_.Tags) } })
    $snapshot = [ordered]@{
        task_tag = 'DUR-050'; instance_filter_states = $states
        region_wide_non_terminated_instance_count = $nonTerminated.Count
        region_wide_volume_count = $allVolumes.Count
        tagged_dur050_instance_count = $instanceRows.Count; instances = $instanceRows
        forbidden_volume_count = $volumeRows.Count; volumes = $volumeRows; state = 'OK'
    }
    $script:observed.dur050_inventory = $snapshot
    if ($instanceRows.Count -ne 0) { throw "Found $($instanceRows.Count) non-terminated instance(s) tagged Task=DUR-050." }
    $taskVolumes = @($volumeRows | Where-Object { @($_.tags | Where-Object { $_.Key -eq 'Task' -and $_.Value -eq 'DUR-050' }).Count -gt 0 })
    if ($taskVolumes.Count -ne 0) { throw "Found $($taskVolumes.Count) volume(s) tagged Task=DUR-050." }
    $projectVolumes = @($volumeRows | Where-Object { @($_.tags | Where-Object { $_.Key -eq 'Project' -and $_.Value -eq 'durable-engine' }).Count -gt 0 })
    if ($projectVolumes.Count -ne 0) { throw "Found $($projectVolumes.Count) volume(s) tagged Project=durable-engine." }
    $availableVolumes = @($volumeRows | Where-Object state -eq 'available')
    if ($availableVolumes.Count -ne 0) { throw "Found $($availableVolumes.Count) available unattached EBS volume(s) in $script:region." }
    return $snapshot
}

function Invoke-Dur050PreApplyGate {
    $startedAt = [DateTimeOffset]::UtcNow
    $manifest = $null
    $inspection = $null
    $planHash = $null
    try {
        if ([string]$PSVersionTable.PSEdition -ne 'Core') { throw 'DUR-050 operator tools require PowerShell Core (pwsh 7 or newer).' }
        foreach ($path in @($SavedPlanPath, $PlanInspectionPath, $CampaignManifestPath)) {
            if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Required pre-apply input is missing: $path" }
        }
        if (Test-Path -LiteralPath $OutputPath) { throw "Refusing to overwrite pre-apply gate record: $OutputPath" }
        if (Test-Path -LiteralPath $LedgerCheckPath) { throw "Refusing to overwrite ledger check: $LedgerCheckPath" }
        if ($LedgerPath -and $env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE -ne '1') { throw 'A non-default ledger path is test-only and requires DUR050_ENABLE_TEST_LEDGER_OVERRIDE=1.' }
        if ($RepositoryRoot -and $env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'RepositoryRoot override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
        if ($env:DUR050_ENABLE_TEST_STATE_OVERRIDE -ne '1' -and [System.IO.Path]::GetFullPath($TerraformStatePath) -ne [System.IO.Path]::GetFullPath((Join-Path $repoRoot 'deploy/aws/terraform.tfstate'))) {
            throw 'A non-default Terraform state path is test-only and requires DUR050_ENABLE_TEST_STATE_OVERRIDE=1.'
        }
        if ($env:TF_WORKSPACE -and $env:TF_WORKSPACE -ne 'default') { throw 'DUR-050 pre-apply gate refuses a non-default Terraform workspace.' }

        $manifest = Get-Content -LiteralPath $CampaignManifestPath -Raw | ConvertFrom-Json -ErrorAction Stop
        if ([string]$manifest.schema -ne 'dur050-cost-manifest.v2' -or [string]$manifest.campaign_id -ne 'pilot-20260924-e3d780f' -or
            [string]$manifest.terraform_profile.task_id -ne 'DUR-050' -or [string]$manifest.terraform_profile.campaign_slug -ne 'dur050' -or
            [string]$manifest.terraform_profile.environment_name -ne 'portfolio-capacity' -or [string]$manifest.terraform_profile.aws_region -ne 'us-west-1') {
            throw 'Campaign manifest is not the reviewed DUR-050 profile.'
        }
        $manifestHash = (Get-FileHash -LiteralPath $CampaignManifestPath -Algorithm SHA256).Hash.ToUpperInvariant()
        $inspection = Get-Content -LiteralPath $PlanInspectionPath -Raw | ConvertFrom-Json -ErrorAction Stop
        $inspectionHash = (Get-FileHash -LiteralPath $PlanInspectionPath -Algorithm SHA256).Hash.ToUpperInvariant()
        $planHash = (Get-FileHash -LiteralPath $SavedPlanPath -Algorithm SHA256).Hash.ToUpperInvariant()
        $approvedPlan = $manifest.approved_saved_plan
        if ($null -eq $approvedPlan) { throw 'Campaign manifest is missing approved_saved_plan.' }
        if ([string]$approvedPlan.review_state -match '^INVALIDATED') { throw 'approved_saved_plan is invalidated and cannot be used for a pre-apply gate.' }
        if ([string]$approvedPlan.source_commit -notmatch '^[0-9a-f]{40}$' -or
            [string]$approvedPlan.source_commit -ne [string]$manifest.terraform_profile.repo_ref) {
            throw 'approved_saved_plan.source_commit must be the full campaign terraform_profile.repo_ref.'
        }
        if ([string]$approvedPlan.saved_plan_sha256 -notmatch '^[A-Fa-f0-9]{64}$') { throw 'approved_saved_plan.saved_plan_sha256 must be a SHA-256 hex digest.' }
        if ([string]$approvedPlan.inspection_sha256 -notmatch '^[A-Fa-f0-9]{64}$') { throw 'approved_saved_plan.inspection_sha256 must be a SHA-256 hex digest.' }
        if ($null -eq $approvedPlan.PSObject.Properties['expected_plan_prior_state_lineage'] -or
            $null -eq $approvedPlan.expected_plan_prior_state_lineage -or
            $null -eq $approvedPlan.PSObject.Properties['expected_plan_prior_state_serial']) {
            throw 'approved_saved_plan must declare expected plan prior-state lineage and serial.'
        }
        $approvedPriorSerial = Get-Dur050Numeric $approvedPlan.expected_plan_prior_state_serial 'approved_saved_plan.expected_plan_prior_state_serial'
        if ($approvedPriorSerial -lt 0 -or $approvedPriorSerial -ne [math]::Floor($approvedPriorSerial)) {
            throw 'approved_saved_plan.expected_plan_prior_state_serial must be a non-negative integer.'
        }
        $expectedInspectionPath = Resolve-Dur050ManifestPath ([string]$approvedPlan.inspection_path)
        $expectedBaselinePath = Resolve-Dur050ManifestPath ([string]$approvedPlan.baseline_inspection_path)
        if ([System.IO.Path]::GetFullPath($PlanInspectionPath) -ne $expectedInspectionPath) {
            throw 'PlanInspectionPath does not match approved_saved_plan.inspection_path.'
        }
        if ([string]$approvedPlan.inspection_sha256 -ne $inspectionHash) {
            throw 'Plan inspection SHA-256 does not match approved_saved_plan.inspection_sha256.'
        }
        $script:observed.inputs = [ordered]@{
            saved_plan_path = [System.IO.Path]::GetFullPath($SavedPlanPath)
            actual_plan_sha256 = $planHash
            expected_plan_sha256 = ([string]$approvedPlan.saved_plan_sha256).ToUpperInvariant()
            plan_inspection_path = [System.IO.Path]::GetFullPath($PlanInspectionPath)
            plan_inspection_sha256 = $inspectionHash
            manifest_expected_inspection_sha256 = [string]$approvedPlan.inspection_sha256
            approved_saved_plan_source_commit = [string]$approvedPlan.source_commit
            baseline_inspection_path = $expectedBaselinePath
            campaign_manifest_path = [System.IO.Path]::GetFullPath($CampaignManifestPath)
            campaign_manifest_sha256 = $manifestHash
            terraform_state_path = [System.IO.Path]::GetFullPath($TerraformStatePath)
            ledger_check_path = [System.IO.Path]::GetFullPath($LedgerCheckPath)
            reserve_minutes = $ReserveMinutes
            aws_profile_name = if ($env:AWS_PROFILE) { $env:AWS_PROFILE } else { $null }
        }
        if ($planHash -ne ([string]$approvedPlan.saved_plan_sha256).ToUpperInvariant()) { throw "Saved plan SHA-256 does not match approved_saved_plan (actual $planHash)." }
        if ([string]$inspection.source_commit -ne [string]$approvedPlan.source_commit) { throw 'Plan inspection source_commit does not match approved_saved_plan.source_commit.' }
        $script:observed.plan_inspection = Assert-Dur050PlanInspection -Inspection $inspection -ActualPlanHash $planHash -Manifest $manifest -BaselinePath $expectedBaselinePath

        $workspaceFile = Join-Path $repoRoot 'deploy/aws/.terraform/environment'
        $workspace = if ($env:TF_WORKSPACE) { $env:TF_WORKSPACE } elseif (Test-Path -LiteralPath $workspaceFile) { (Get-Content -LiteralPath $workspaceFile -Raw).Trim() } else { 'default' }
        if ($workspace -and $workspace -ne 'default') { throw "Pre-apply gate supports only Terraform's default local workspace; observed '$workspace'." }
        $planStateSnapshot = Get-Dur050PlanStateSnapshot -SavedPlanPath $SavedPlanPath -TerraformStatePath $TerraformStatePath -TerraformExe $TerraformExe -AllowMismatch
        $script:observed.plan_state_provenance = $planStateSnapshot
        if (-not $planStateSnapshot.state_matches) { throw [string]$planStateSnapshot.state_mismatch }
        if ([string]$planStateSnapshot.plan_prior_state_lineage -cne [string]$approvedPlan.expected_plan_prior_state_lineage) {
            throw 'Saved plan prior-state lineage does not match approved_saved_plan.expected_plan_prior_state_lineage.'
        }
        if ([long]$planStateSnapshot.plan_prior_state_serial -ne [long]$approvedPriorSerial) {
            throw 'Saved plan prior-state serial does not match approved_saved_plan.expected_plan_prior_state_serial.'
        }
        if ([string]$inspection.plan_prior_state_lineage -cne [string]$planStateSnapshot.plan_prior_state_lineage -or
            [long]$inspection.plan_prior_state_serial -ne [long]$planStateSnapshot.plan_prior_state_serial -or
            [string]$inspection.local_state_lineage -cne [string]$planStateSnapshot.local_state_lineage -or
            [long]$inspection.local_state_serial -ne [long]$planStateSnapshot.local_state_serial) {
            throw 'Plan inspection state provenance does not match the plan ZIP and current local state.'
        }
        if ([string]$inspection.terraform_executable_sha256 -cne [string]$planStateSnapshot.terraform_executable_sha256) {
            throw 'Terraform executable SHA-256 at gate time does not match the inspected plan producer executable.'
        }
        if ([System.IO.Path]::GetFullPath([string]$inspection.terraform_executable_path) -ne [System.IO.Path]::GetFullPath([string]$planStateSnapshot.terraform_executable_path)) {
            throw 'Terraform executable path at gate time does not match the inspected plan producer executable path.'
        }
        if ([string]$inspection.terraform_version -ne [string]$planStateSnapshot.terraform_executable_version) {
            throw 'Terraform executable version at gate time does not match the inspected plan.'
        }
        $state = [ordered]@{
            path = $planStateSnapshot.terraform_state_path
            state_file_present = $planStateSnapshot.local_state_file_present
            lineage = $planStateSnapshot.local_state_lineage
            serial = $planStateSnapshot.local_state_serial
            resource_count = $planStateSnapshot.local_state_resource_count
            state = if ($planStateSnapshot.local_state_resource_count -eq 0) { 'EMPTY' } else { 'NON_EMPTY' }
        }
        $script:observed.terraform_state = $state
        if ($state.resource_count -ne 0) { throw "Terraform state contains $($state.resource_count) resource(s); pre-apply requires an empty state." }

        $identity = Invoke-Dur050AwsJson @('sts', 'get-caller-identity', '--output', 'json')
        $script:observed.identity = [ordered]@{ account_id = [string]$identity.Account; arn = [string]$identity.Arn; region_used_for_regional_checks = $script:region }
        if ([string]$identity.Account -ne '372206265946') { throw "AWS account $($identity.Account) does not match required account 372206265946." }

        $script:observed.service_quota_response = Invoke-Dur050AwsJson @('service-quotas', 'get-service-quota', '--service-code', 'ec2', '--quota-code', 'L-1216C47A', '--output', 'json')
        try { $script:observed.quota = Test-Dur050Quota -PlannedPeakVcpu 8 -Quota $script:observed.service_quota_response }
        catch {
            if ($_.Exception.Message -match 'Current standard On-Demand use ([0-9.]+) plus planned ([0-9.]+) exceeds quota ([0-9.]+)') {
                $script:observed.quota = [ordered]@{
                    quota_code = 'L-1216C47A'
                    current_on_demand_standard_vcpu = [double]$Matches[1]
                    planned_peak_vcpu = [double]$Matches[2]
                    projected_vcpu = ([double]$Matches[1] + [double]$Matches[2])
                    quota_vcpu = [double]$Matches[3]
                    state = 'FAIL'
                }
            }
            throw
        }

        $regionalInstances = Invoke-Dur050AwsJson @('ec2', 'describe-instances', '--filters', 'Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down', '--output', 'json')
        $regionalVolumes = Invoke-Dur050AwsJson @('ec2', 'describe-volumes', '--output', 'json')
        $script:observed.dur050_inventory = Test-Dur050NoTaggedResources -Instances $regionalInstances -Volumes $regionalVolumes

        $budget = Invoke-Dur050AwsJson @('budgets', 'describe-budget', '--account-id', '372206265946', '--budget-name', 'durable-engine-D022-aggregate-20260923', '--output', 'json')
        $notifications = Invoke-Dur050AwsJson @('budgets', 'describe-notifications-for-budget', '--account-id', '372206265946', '--budget-name', 'durable-engine-D022-aggregate-20260923', '--output', 'json')
        $script:observed.budget_response = $budget
        $script:observed.notification_response = $notifications
        $script:observed.budget_notification_observations = @($notifications.Notifications | ForEach-Object {
            [ordered]@{
                notification_type = [string]$_.NotificationType
                comparison_operator = [string]$_.ComparisonOperator
                threshold_type = [string]$_.ThresholdType
                threshold_usd = if ($null -eq $_.Threshold) { $null } else { [decimal]$_.Threshold }
                state = if ($null -eq $_.PSObject.Properties['NotificationState']) { $null } else { [string]$_.NotificationState }
            }
        })
        $script:observed.budget = Test-Dur050Budget $budget $notifications

        $tags = Invoke-Dur050AwsJson -Arguments @('ce', 'list-cost-allocation-tags', '--tag-keys', 'Task', 'Environment', '--output', 'json') -Region 'us-east-1'
        $script:observed.cost_allocation_tags_response = $tags
        $script:observed.cost_allocation_tags = Test-Dur050CostAllocationTags $tags

        $plannedTypes = @()
        for ($i = 0; $i -lt [int]$script:observed.plan_inspection.c7i_large; $i++) { $plannedTypes += 'c7i.large' }
        for ($i = 0; $i -lt [int]$script:observed.plan_inspection.m7i_large; $i++) { $plannedTypes += 'm7i.large' }
        $ledgerCheck = Invoke-Dur050LedgerCheck -PlannedInstanceTypes $plannedTypes
        if ($ledgerCheck.open_interval_count -ne 0) { throw 'Pre-apply ledger requires open_interval_count == 0.' }
        $script:observed.ledger_check = $ledgerCheck
        $script:observed.planned_host_projection = [ordered]@{
            instance_types = $plannedTypes
            reserve_minutes = $ReserveMinutes
            projected_usd = $ledgerCheck.planned_host_projection_usd
            total_projected_usd = $ledgerCheck.projected_instance_cost_usd
            below_cap = ([decimal]$ledgerCheck.projected_instance_cost_usd -lt [decimal]$ledgerCheck.budget_cap_usd)
        }
        $record = [ordered]@{
            schema = 'dur050-pre-apply-gate.v1'
            status = 'PASS'
            account_id = [string]$identity.Account
            region = $script:region
            campaign_id = [string]$manifest.campaign_id
            checked_at_utc_start = $startedAt.ToString('o')
            checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
            planned_peak_vcpu = 8
            plan_sha256 = $planHash
            plan_inspection_path = [System.IO.Path]::GetFullPath($PlanInspectionPath)
            plan_inspection_sha256 = $inspectionHash
            campaign_manifest_path = [System.IO.Path]::GetFullPath($CampaignManifestPath)
            campaign_manifest_sha256 = $manifestHash
            terraform_state_path = [System.IO.Path]::GetFullPath($TerraformStatePath)
            terraform_state_resource_count = $state.resource_count
            checks = $script:observed
            ledger_check_path = [System.IO.Path]::GetFullPath($LedgerCheckPath)
            ledger_check = $ledgerCheck
            apply_authority = 'NONE; Claude go/no-go required'
        }
        if ($record.account_id -ne '372206265946' -or $record.region -ne 'us-west-1' -or $record.apply_authority -ne 'NONE; Claude go/no-go required') {
            throw 'Pre-apply PASS record failed its self-check.'
        }
        $tempPath = "$OutputPath.$([guid]::NewGuid().ToString('N')).tmp"
        try {
            Write-Dur050PreApplyJson -Path $tempPath -Value $record
            [System.IO.File]::Move($tempPath, $OutputPath)
        } finally {
            Remove-Item -LiteralPath $tempPath -Force -ErrorAction SilentlyContinue
        }
        $script:recordWritten = $true
        $record | ConvertTo-Json -Depth 28
        Write-Host 'DUR-050 pre-apply account gate PASS; apply authority remains NONE pending Claude go/no-go.'
        return 0
    } catch {
        $failure = [ordered]@{
            schema = 'dur050-pre-apply-gate.v1'
            status = 'FAIL'
            account_id = if ($script:observed.identity.account_id) { $script:observed.identity.account_id } else { 'unknown' }
            region = $script:region
            campaign_id = if ($manifest) { [string]$manifest.campaign_id } else { $null }
            checked_at_utc_start = $startedAt.ToString('o')
            checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
            plan_sha256 = $planHash
            plan_inspection_path = [System.IO.Path]::GetFullPath($PlanInspectionPath)
            campaign_manifest_path = [System.IO.Path]::GetFullPath($CampaignManifestPath)
            checks = $script:observed
            ledger_check_path = [System.IO.Path]::GetFullPath($LedgerCheckPath)
            ledger_check_observed = if (Test-Path -LiteralPath $LedgerCheckPath -PathType Leaf) {
                try { Get-Content -LiteralPath $LedgerCheckPath -Raw | ConvertFrom-Json -ErrorAction Stop } catch { [ordered]@{ parse_error = $_.Exception.Message } }
            } else { $null }
            error = $_.Exception.Message
            apply_authority = 'NONE; Claude go/no-go required'
        }
        if (-not (Test-Path -LiteralPath $OutputPath)) {
            try { Write-Dur050PreApplyJson -Path $OutputPath -Value $failure; $script:recordWritten = $true }
            catch { Write-Error "Could not write pre-apply FAIL record: $($_.Exception.Message)" }
        }
        Write-Host ("ERROR: " + $_.Exception.Message) -ForegroundColor Red
        return 1
    }
}

if ($DryRun) {
    foreach ($path in @($SavedPlanPath, $PlanInspectionPath, $CampaignManifestPath)) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Required pre-apply input is missing: $path" }
    }
    if (Test-Path -LiteralPath $OutputPath) { throw "Refusing to overwrite pre-apply gate record: $OutputPath" }
    $dryRunRecord = [ordered]@{
        schema = 'dur050-pre-apply-gate-dry-run.v1'
        classification = 'DRY RUN ONLY; no AWS API, Terraform state command, ledger execution, SSM, apply, or record write is performed.'
        planned_reads = @(
            [ordered]@{ source = 'local'; item = 'saved plan bytes and SHA-256' },
            [ordered]@{ source = 'local'; item = 'plan inspection JSON and SHA-256' },
            [ordered]@{ source = 'local'; item = 'DUR-050 campaign manifest and repo_ref' },
            [ordered]@{ source = 'local'; item = "Terraform state JSON at $([System.IO.Path]::GetFullPath($TerraformStatePath))" },
            [ordered]@{ source = 'AWS us-west-1'; item = 'STS identity, EC2 quota, current regional standard on-demand instance use, all non-terminated instance states, region-wide volume count, DUR-050/Project volumes, and unattached available volumes' },
            [ordered]@{ source = 'AWS us-west-1'; item = 'D022 USD 200 budget and exactly three notifications' },
            [ordered]@{ source = 'AWS us-east-1'; item = 'Task and Environment cost-allocation tag activation status' },
            [ordered]@{ source = 'local'; item = "closed task-wide ledger and campaign manifest, with $ReserveMinutes minute reserve plus projected 3 c7i.large and 1 m7i.large host cost" }
        )
        forbidden = @('SSM', 'Terraform plan/apply', 'AWS mutations')
        apply_authority = 'NONE; Claude go/no-go required'
    }
    $dryRunRecord | ConvertTo-Json -Depth 10
    exit 0
}

if (Test-Path -LiteralPath $OutputPath) { throw "Refusing to overwrite pre-apply gate record: $OutputPath" }
if (Test-Path -LiteralPath $LedgerCheckPath) { throw "Refusing to overwrite ledger check: $LedgerCheckPath" }
if ($LedgerPath -and $env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE -ne '1') { throw 'A non-default ledger path is test-only and requires DUR050_ENABLE_TEST_LEDGER_OVERRIDE=1.' }
if ($RepositoryRoot -and $env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'RepositoryRoot override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
if ($env:DUR050_ENABLE_TEST_STATE_OVERRIDE -ne '1' -and [System.IO.Path]::GetFullPath($TerraformStatePath) -ne [System.IO.Path]::GetFullPath((Join-Path $repoRoot 'deploy/aws/terraform.tfstate'))) {
    throw 'A non-default Terraform state path is test-only and requires DUR050_ENABLE_TEST_STATE_OVERRIDE=1.'
}
exit (Invoke-Dur050PreApplyGate)
