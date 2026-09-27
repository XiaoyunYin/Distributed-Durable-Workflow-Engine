[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$SavedPlanPath,
    [Parameter(Mandatory)] [ValidatePattern('^[A-Fa-f0-9]{64}$')] [string]$ExpectedSha256,
    [Parameter(Mandatory)] [ValidatePattern('^[A-Fa-f0-9]{40}$')] [string]$SourceCommit,
    [Parameter(Mandatory)] [string]$CampaignManifestPath,
    [Parameter(Mandatory)] [string]$OutputPath,
    [string]$TerraformExe = 'terraform',
    [string]$TerraformStatePath
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$planStateModule = Join-Path $PSScriptRoot 'dur050-plan-state.psm1'
Import-Module $planStateModule -Force
$approvedPlan = $null
$utf8 = [System.Text.UTF8Encoding]::new($false)

function Get-PlanModuleResourceCount($Module) {
    if ($null -eq $Module) { return 0 }
    $count = if ($null -eq $Module.resources) { 0 } else { @($Module.resources).Count }
    foreach ($child in @($Module.child_modules)) { $count += Get-PlanModuleResourceCount $child }
    return $count
}

if (-not (Test-Path -LiteralPath $SavedPlanPath -PathType Leaf)) { throw "Saved plan not found: $SavedPlanPath" }
if (-not (Test-Path -LiteralPath $CampaignManifestPath -PathType Leaf)) { throw "Campaign manifest not found: $CampaignManifestPath" }
if (Test-Path -LiteralPath $OutputPath) { throw "Refusing to overwrite plan inspection: $OutputPath" }

$actualHash = (Get-FileHash -LiteralPath $SavedPlanPath -Algorithm SHA256).Hash.ToUpperInvariant()
if ($actualHash -ne $ExpectedSha256.ToUpperInvariant()) { throw "Saved plan hash mismatch: expected $($ExpectedSha256.ToUpperInvariant()), observed $actualHash." }
$manifest = Get-Content -LiteralPath $CampaignManifestPath -Raw | ConvertFrom-Json -ErrorAction Stop
$approvedPlan = $manifest.approved_saved_plan
if ($null -eq $approvedPlan -or [string]::IsNullOrWhiteSpace([string]$approvedPlan.baseline_inspection_path)) {
    throw 'Campaign manifest must contain approved_saved_plan with baseline_inspection_path.'
}
if ([string]$manifest.terraform_profile.repo_ref -ne $SourceCommit.ToLowerInvariant() -or [string]$approvedPlan.source_commit -ne $SourceCommit.ToLowerInvariant()) { throw 'SourceCommit must match terraform_profile.repo_ref and approved_saved_plan.source_commit.' }
if ([string]$approvedPlan.saved_plan_sha256 -ne $ExpectedSha256.ToUpperInvariant()) { throw 'ExpectedSha256 must match approved_saved_plan.saved_plan_sha256.' }
$approvedInspectionPath = [string]$approvedPlan.inspection_path
if (-not [System.IO.Path]::IsPathRooted($approvedInspectionPath)) { $approvedInspectionPath = Join-Path $repoRoot $approvedInspectionPath }
if ([System.IO.Path]::GetFullPath($OutputPath) -ne [System.IO.Path]::GetFullPath($approvedInspectionPath)) { throw 'OutputPath must match approved_saved_plan.inspection_path.' }
$baselinePath = [string]$approvedPlan.baseline_inspection_path
if (-not [System.IO.Path]::IsPathRooted($baselinePath)) { $baselinePath = Join-Path $repoRoot $baselinePath }
if (-not (Test-Path -LiteralPath $baselinePath -PathType Leaf)) { throw "Reviewed offline inspection baseline not found: $baselinePath" }
if (-not $TerraformStatePath) { $TerraformStatePath = Join-Path $repoRoot 'deploy/aws/terraform.tfstate' }
$planStateSnapshot = Get-Dur050PlanStateSnapshot -SavedPlanPath $SavedPlanPath -TerraformStatePath $TerraformStatePath -TerraformExe $TerraformExe
$resolvedPlanPath = (Resolve-Path -LiteralPath $SavedPlanPath).Path
$repoPrefix = $repoRoot.TrimEnd([char[]]@('\', '/')) + [System.IO.Path]::DirectorySeparatorChar
if (-not $resolvedPlanPath.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The inspected saved plan must be inside the repository so the record can use a portable relative path.'
}
$planPathForRecord = $resolvedPlanPath.Substring($repoPrefix.Length).Replace('\', '/')
$terraformOutput = & ([string]$planStateSnapshot.terraform_executable_path) '-chdir=deploy/aws' 'show' '-json' $resolvedPlanPath 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) { throw "terraform show -json failed: $terraformOutput" }
try { $plan = $terraformOutput | ConvertFrom-Json -ErrorAction Stop }
catch { throw "terraform show -json returned invalid JSON: $($_.Exception.Message)" }
if ([string]$plan.terraform_version -ne '1.16.4') { throw "Plan was inspected with Terraform $($plan.terraform_version), expected 1.16.4." }

$changes = @($plan.resource_changes)
$createCount = 0
$updateCount = 0
$deleteCount = 0
$replacementCount = 0
$resourceRows = @()
foreach ($change in $changes) {
    $actions = @($change.change.actions)
    if ($actions.Count -eq 1 -and $actions[0] -eq 'create') { $createCount++ }
    if ($actions -contains 'update') { $updateCount++ }
    if (($actions -contains 'delete') -and ($actions -contains 'create')) { $replacementCount++; $deleteCount++ }
    elseif ($actions -contains 'delete') { $deleteCount++ }
    $resourceRows += [ordered]@{ address = [string]$change.address; type = [string]$change.type; actions = $actions }
}
$resourceRows = @($resourceRows)
$instances = @($changes | Where-Object { $_.type -eq 'aws_instance' })
$instanceTypeCounts = @{}
$vcpusByType = @{ 'c7i.large' = 2; 'm7i.large' = 2 }
foreach ($instance in $instances) {
    $type = [string]$instance.change.after.instance_type
    if (-not $vcpusByType.ContainsKey($type)) { throw "Unrecognized planned instance type: $type" }
    if (-not $instanceTypeCounts.ContainsKey($type)) { $instanceTypeCounts[$type] = 0 }
    $instanceTypeCounts[$type]++
}
$taggable = @($changes | Where-Object { $null -ne $_.change.after.tags_all })
$expectedTagged = @($taggable | Where-Object {
    $_.change.after.tags_all.Task -eq 'DUR-050' -and
    $_.change.after.tags_all.Environment -eq 'portfolio-capacity' -and
    $_.change.after.tags_all.ExpiresAt -eq [string]$manifest.terraform_profile.expires_at
})
$prohibited = @($changes | Where-Object { $_.type -match 'eks|nat_gateway|load_balancer|(^|_)lb($|_)' })
$root = $plan.prior_state.values.root_module
$priorStateResources = Get-PlanModuleResourceCount $root
$peakVcpu = 0
foreach ($type in $instanceTypeCounts.Keys) { $peakVcpu += ([int]$instanceTypeCounts[$type] * [int]$vcpusByType[$type]) }

$baseline = Get-Content -LiteralPath $baselinePath -Raw | ConvertFrom-Json -ErrorAction Stop
$signature = @($resourceRows | ForEach-Object { '{0}|{1}|{2}' -f $_.address, $_.type, (@($_.actions) -join ',') } | Sort-Object)
$baselineSignature = @($baseline.resource_addresses_and_types | ForEach-Object { '{0}|{1}|{2}' -f $_.address, $_.type, (@($_.actions) -join ',') } | Sort-Object)
if (($signature -join [Environment]::NewLine) -cne ($baselineSignature -join [Environment]::NewLine)) { throw 'Saved-plan resources/actions differ from the reviewed offline inspection.' }

$record = [ordered]@{
    schema = 'dur050-recorded-plan-inspection.v1'
    inspection_kind = 'LIVE-SAVED-PLAN SHOW ONLY; NOT APPLY AUTHORITY'
    recorded_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    plan_file_last_write_time_utc = (Get-Item -LiteralPath $SavedPlanPath).LastWriteTimeUtc.ToString('o')
    source_commit = $SourceCommit.ToLowerInvariant()
    terraform_version = [string]$plan.terraform_version
    plan_path = $planPathForRecord
    plan_sha256 = $actualHash
    plan_prior_state_lineage = $planStateSnapshot.plan_prior_state_lineage
    plan_prior_state_serial = $planStateSnapshot.plan_prior_state_serial
    plan_prior_state_terraform_version = $planStateSnapshot.plan_prior_state_terraform_version
    local_state_lineage = $planStateSnapshot.local_state_lineage
    local_state_serial = $planStateSnapshot.local_state_serial
    local_state_file_present = $planStateSnapshot.local_state_file_present
    terraform_state_path = $planStateSnapshot.terraform_state_path
    terraform_executable_path = $planStateSnapshot.terraform_executable_path
    terraform_executable_sha256 = $planStateSnapshot.terraform_executable_sha256
    planned_peak_vcpu = $peakVcpu
    instance_vcpu_basis = [ordered]@{
        'c7i.large' = [ordered]@{ count = [int]$instanceTypeCounts['c7i.large']; vcpus_each = 2 }
        'm7i.large' = [ordered]@{ count = [int]$instanceTypeCounts['m7i.large']; vcpus_each = 2 }
    }
    resource_changes = [ordered]@{ creates = $createCount; updates = $updateCount; deletes = $deleteCount; replacements = $replacementCount }
    resource_change_count = $changes.Count
    instances = [ordered]@{ total = $instances.Count; 'c7i.large' = [int]$instanceTypeCounts['c7i.large']; 'm7i.large' = [int]$instanceTypeCounts['m7i.large'] }
    taggable_resources_with_effective_tags = $taggable.Count
    taggable_resources_with_expected_task_environment_expiry_tags = $expectedTagged.Count
    effective_tag_source = 'Terraform 1.16.4 show -json resource_changes[].change.after.tags_all'
    prohibited_eks_nat_gateway_or_load_balancer_resources = $prohibited.Count
    terraform_state_resource_count = $priorStateResources
    terraform_state_count_basis = 'Terraform 1.16.4 show -json prior_state values module resource count'
    aws_region = [string]$manifest.terraform_profile.aws_region
    repo_ref = [string]$manifest.terraform_profile.repo_ref
    resource_addresses_and_types = $resourceRows
    compared_with = [ordered]@{
        path = [string]$approvedPlan.baseline_inspection_path
        sha256 = (Get-FileHash -LiteralPath $baselinePath -Algorithm SHA256).Hash.ToUpperInvariant()
        same_resource_addresses_types_actions = $true
    }
    profile = [ordered]@{
        task_id = [string]$manifest.terraform_profile.task_id
        campaign_slug = [string]$manifest.terraform_profile.campaign_slug
        environment_name = [string]$manifest.terraform_profile.environment_name
        repo_ref = [string]$manifest.terraform_profile.repo_ref
        expires_at = [string]$manifest.terraform_profile.expires_at
        instance_type = [string]$manifest.terraform_profile.instance_type
        dependency_instance_type = [string]$manifest.terraform_profile.dependency_instance_type
        load_generator_instance_type = [string]$manifest.terraform_profile.load_generator_instance_type
        enable_dur050_load_generator = [bool]$manifest.terraform_profile.enable_dur050_load_generator
        root_volume_size_gb = [int]$manifest.terraform_profile.root_volume_size_gb
        worker_slots = [int]$manifest.terraform_profile.worker_slots
    }
    apply_authority = 'NONE; Claude go/no-go required'
}
if ($record.resource_change_count -ne 31 -or $createCount -ne 31 -or $updateCount -ne 0 -or $deleteCount -ne 0 -or $replacementCount -ne 0 -or
    $instances.Count -ne 4 -or $record.instances.'c7i.large' -ne 3 -or $record.instances.'m7i.large' -ne 1 -or $peakVcpu -ne 8 -or
    $taggable.Count -ne 23 -or $expectedTagged.Count -ne 23 -or $prohibited.Count -ne 0 -or $priorStateResources -ne 0) {
    throw "Saved-plan inspection differs from the reviewed topology: $($record | ConvertTo-Json -Depth 8 -Compress)"
}
$parent = Split-Path -Parent $OutputPath
if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
[System.IO.File]::WriteAllText($OutputPath, (($record | ConvertTo-Json -Depth 18) + [Environment]::NewLine), $utf8)
Write-Output ($record | ConvertTo-Json -Depth 8)
Write-Host "Inspection written: $OutputPath"
