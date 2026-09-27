#Requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9-]{1,32}$')][string]$CycleID,
    [Parameter(Mandatory)][string]$CampaignManifestPath,
    [Parameter(Mandatory)][string]$OutputPath,
    [string]$TerraformExe,
    [string]$TerraformDirectory,
    [string]$TerraformStatePath
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not $TerraformDirectory) { $TerraformDirectory = Join-Path $repoRoot 'deploy/aws' }
elseif ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'TerraformDirectory override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
if (-not $TerraformStatePath) { $TerraformStatePath = Join-Path $TerraformDirectory 'terraform.tfstate' }
elseif ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'TerraformStatePath override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
if (-not (Test-Path -LiteralPath $CampaignManifestPath -PathType Leaf)) { throw "Campaign manifest is missing: $CampaignManifestPath" }
if (Test-Path -LiteralPath $OutputPath) { throw "Refusing to overwrite destroy record: $OutputPath" }

Import-Module (Join-Path $PSScriptRoot 'dur050-plan-state.psm1') -Force

function New-Dur050ThrowawaySecret([int]$Length) {
    $alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    $builder = [System.Text.StringBuilder]::new($Length)
    for ($index = 0; $index -lt $Length; $index++) {
        [void]$builder.Append($alphabet[[Security.Cryptography.RandomNumberGenerator]::GetInt32($alphabet.Length)])
    }
    return $builder.ToString()
}

$requiredProfileKeys = @(
    'aws_region','availability_zones','project_name','campaign_slug','task_id','environment_name',
    'ami_id','repo_url','repo_ref','expires_at','admin_cidrs','instance_type','dependency_instance_type',
    'enable_dur050_load_generator','load_generator_instance_type','root_volume_size_gb','worker_slots',
    'postgres_user','postgres_db','postgres_image','kafka_image'
)
$terraformDirectoryFull = [System.IO.Path]::GetFullPath($TerraformDirectory)
$terraformStateFull = [System.IO.Path]::GetFullPath($TerraformStatePath)
$tempVarsPath = $null
$started = [DateTimeOffset]::UtcNow
$terraformInfo = $null
$terraformExitCode = $null
$destroyOutput = ''
$destroyedCount = $null
$postState = $null
$failure = $null
$approvedPlan = $null

try {
    if ([string]$PSVersionTable.PSEdition -ne 'Core') { throw 'DUR-050 operator tools require PowerShell Core (pwsh 7 or newer).' }
    if (-not (Test-Path -LiteralPath $terraformDirectoryFull -PathType Container)) { throw "Terraform working directory is missing: $terraformDirectoryFull" }
    if (-not (Test-Path -LiteralPath $CampaignManifestPath -PathType Leaf)) { throw "Campaign manifest is missing: $CampaignManifestPath" }
    $manifest = Get-Content -LiteralPath $CampaignManifestPath -Raw | ConvertFrom-Json -ErrorAction Stop
    if ([string]$manifest.schema -ne 'dur050-cost-manifest.v2' -or [string]$manifest.terraform_profile.task_id -ne 'DUR-050') { throw 'Campaign manifest is not a DUR-050 cost manifest.' }
    $profile = $manifest.terraform_profile
    foreach ($key in $requiredProfileKeys) {
        if ($null -eq $profile.PSObject.Properties[$key]) { throw "Campaign terraform_profile is missing required variable '$key'." }
    }
    $approvedPlan = $manifest.approved_saved_plan
    if ($null -eq $approvedPlan -or [string]$approvedPlan.review_state -notmatch '^APPLIED_') { throw 'Top-level approved_saved_plan review_state must match APPLIED_ before destroy.' }
    $pinnedPath = [string]$approvedPlan.terraform_executable_path
    $pinnedHash = [string]$approvedPlan.terraform_executable_sha256
    if (-not $pinnedPath -or $pinnedHash -notmatch '^[A-Fa-f0-9]{64}$') { throw 'Top-level approved_saved_plan does not pin a Terraform executable path and SHA-256.' }
    if (-not $TerraformExe) { $TerraformExe = $pinnedPath }
    if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1' -and [System.IO.Path]::GetFullPath($TerraformExe) -ine [System.IO.Path]::GetFullPath($pinnedPath)) {
        throw 'Terraform executable path differs from the top-level approved_saved_plan pin.'
    }
    $terraformInfo = Resolve-Dur050TerraformExecutable -TerraformExe $TerraformExe
    if ([string]$terraformInfo.sha256 -cne $pinnedHash) { throw 'Terraform executable SHA-256 differs from the top-level approved_saved_plan pin.' }
    if ([string]$terraformInfo.terraform_version -ne [string]$approvedPlan.terraform_version) { throw 'Terraform version differs from the top-level approved_saved_plan pin.' }

    $variables = [ordered]@{}
    foreach ($property in $profile.PSObject.Properties) { $variables[$property.Name] = $property.Value }
    $postgresPassword = if ($env:TF_VAR_postgres_password -match '^[A-Za-z0-9]{20,64}$') { $env:TF_VAR_postgres_password } else { New-Dur050ThrowawaySecret 32 }
    $observerPassword = if ($env:TF_VAR_dur050_observer_password -match '^[A-Za-z0-9]{24,64}$') { $env:TF_VAR_dur050_observer_password } else { New-Dur050ThrowawaySecret 32 }
    $variables.postgres_password = $postgresPassword
    $variables.dur050_observer_password = $observerPassword

    $tempVarsPath = Join-Path ([System.IO.Path]::GetTempPath()) ("dur050-destroy-$CycleID-$([guid]::NewGuid().ToString('N')).tfvars.json")
    $tempFull = [System.IO.Path]::GetFullPath($tempVarsPath)
    if ($tempFull.StartsWith($repoRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Temporary Terraform variables must be outside the repository.'
    }
    $json = $variables | ConvertTo-Json -Depth 30
    [System.IO.File]::WriteAllText($tempVarsPath, $json, [System.Text.UTF8Encoding]::new($false))
    $check = Get-Content -LiteralPath $tempVarsPath -Raw | ConvertFrom-Json -ErrorAction Stop
    if ($check.root_volume_size_gb -isnot [ValueType] -or $check.enable_dur050_load_generator -isnot [bool] -or
        @($check.availability_zones).Count -ne 2 -or [string]$check.postgres_password -notmatch '^[A-Za-z0-9]{20,64}$' -or
        [string]$check.dur050_observer_password -notmatch '^[A-Za-z0-9]{24,64}$') { throw 'Generated Terraform variable file failed typed-value or secret validation.' }

    $started = [DateTimeOffset]::UtcNow
    Push-Location $repoRoot
    try {
        $destroyOutput = & $terraformInfo.path "-chdir=$terraformDirectoryFull" destroy -auto-approve "-var-file=$tempFull" 2>&1 | Out-String
        $terraformExitCode = $LASTEXITCODE
    } finally { Pop-Location }
    if ($destroyOutput -match '(?m)Resources:\s*(\d+)\s+destroyed\b') { $destroyedCount = [int]$Matches[1] }
    if ($null -eq $destroyedCount) { throw "Terraform destroy output did not contain a destroyed-resource count: $destroyOutput" }
    $postState = Read-Dur050LocalTerraformState -TerraformStatePath $terraformStateFull
    if ($terraformExitCode -ne 0) { throw "Terraform destroy exited ${terraformExitCode}: $destroyOutput" }
    if (-not $postState.state_file_present) { throw 'Terraform state file is absent after destroy; post-destroy serial cannot be verified.' }
    if ($postState.resource_count -ne 0) { throw "Terraform destroy returned success but state still contains $($postState.resource_count) resources." }
} catch {
    $failure = $_.Exception.Message
    if ($null -eq $terraformExitCode) { $terraformExitCode = 1 }
    if ($null -eq $postState) {
        try { $postState = Read-Dur050LocalTerraformState -TerraformStatePath $terraformStateFull } catch { $postState = $null }
    }
} finally {
    if ($tempVarsPath -and (Test-Path -LiteralPath $tempVarsPath)) { Remove-Item -LiteralPath $tempVarsPath -Force -ErrorAction SilentlyContinue }
}

$finished = [DateTimeOffset]::UtcNow
$record = [ordered]@{
    schema = 'dur050-destroy.v1'
    status = if ($failure) { 'FAIL' } else { 'PASS' }
    cycle_id = $CycleID
    destroy_started_at_utc = $started.ToUniversalTime().ToString('o')
    destroy_completed_at_utc = $finished.ToUniversalTime().ToString('o')
    terraform_executable_path = if ($terraformInfo) { $terraformInfo.path } else { $TerraformExe }
    terraform_executable_sha256 = if ($terraformInfo) { $terraformInfo.sha256 } else { $null }
    terraform_version = if ($terraformInfo) { $terraformInfo.terraform_version } else { $null }
    exit_code = $terraformExitCode
    destroyed_count = $destroyedCount
    post_destroy_state = if ($postState) { [ordered]@{ path=$postState.path; lineage=$postState.lineage; serial=$postState.serial; resource_count=$postState.resource_count } } else { $null }
    error = $failure
}
$outputParent = Split-Path -Parent $OutputPath
if ($outputParent) { New-Item -ItemType Directory -Force -Path $outputParent | Out-Null }
$recordTemp = "$OutputPath.$([guid]::NewGuid().ToString('N')).tmp"
try {
    [System.IO.File]::WriteAllText($recordTemp, (($record | ConvertTo-Json -Depth 16) + [Environment]::NewLine), [System.Text.UTF8Encoding]::new($false))
    [System.IO.File]::Move($recordTemp, $OutputPath)
} finally { Remove-Item -LiteralPath $recordTemp -Force -ErrorAction SilentlyContinue }
$record | ConvertTo-Json -Depth 16
if ($failure) { exit 1 }
