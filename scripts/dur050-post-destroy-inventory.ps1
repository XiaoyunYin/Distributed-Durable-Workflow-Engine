[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9-]{1,48}$')][string]$CycleID,
    [Parameter(Mandatory)][string]$CampaignManifestPath,
    [Parameter(Mandatory)][string]$OutputPath,
    [string]$TerraformExe = 'terraform',
    [string]$TerraformStatePath,
    [string]$LedgerPath,
    [string]$RepositoryRoot
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$defaultRepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if ($RepositoryRoot) {
    if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'RepositoryRoot override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
    $repoRoot = (Resolve-Path -LiteralPath $RepositoryRoot).Path
} else { $repoRoot = $defaultRepoRoot }
if (-not $TerraformStatePath) { $TerraformStatePath = Join-Path $repoRoot 'deploy/aws/terraform.tfstate' }
if (-not $LedgerPath) { $LedgerPath = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json' }
if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1' -and [System.IO.Path]::GetFullPath($TerraformStatePath) -ne [System.IO.Path]::GetFullPath((Join-Path $repoRoot 'deploy/aws/terraform.tfstate'))) {
    throw 'A non-default Terraform state path is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.'
}
if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1' -and [System.IO.Path]::GetFullPath($LedgerPath) -ne [System.IO.Path]::GetFullPath((Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json'))) {
    throw 'A non-default ledger path is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.'
}

Import-Module (Join-Path $PSScriptRoot 'dur050-plan-state.psm1') -Force
$script:region = 'us-west-1'
$script:rawAwsResponses = [ordered]@{}
$script:checks = [ordered]@{}
$script:violations = [System.Collections.Generic.List[string]]::new()
$utf8 = [System.Text.UTF8Encoding]::new($false)
$pinnedTerraformSha256 = 'D1F5754B41B44C7E4CE7283780D2CCB9492F2C32C9629555686FC29FA8067349'

function ConvertTo-Dur050ExplicitArray {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { $items = [object[]]::new(0) }
    elseif ($Value -is [array]) { $items = [object[]]$Value }
    else { $items = [object[]]@($Value) }
    return [pscustomobject]@{ items = $items; count = $items.Length }
}

function Invoke-Dur050InventoryAws {
    param([Parameter(Mandatory)][string]$Name, [Parameter(Mandatory)][string[]]$Arguments)
    $raw = & aws --region $script:region @Arguments 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    $script:rawAwsResponses[$Name] = $raw.TrimEnd()
    if ($exitCode -ne 0) { throw "AWS read '$Name' failed with exit ${exitCode}: $($raw.Trim())" }
    try { return ($raw | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "AWS read '$Name' returned invalid JSON: $($_.Exception.Message)" }
}

function Test-Dur050TaskTag {
    param([object]$Tags)
    $tagArray = ConvertTo-Dur050ExplicitArray $Tags
    foreach ($tag in $tagArray.items) {
        if ([string]$tag.Key -eq 'Task' -and [string]$tag.Value -eq 'DUR-050') { return $true }
    }
    return $false
}

function Get-Dur050TaggedRows {
    param([object]$Rows, [string]$ListProperty, [string]$IdProperty)
    $rowArray = ConvertTo-Dur050ExplicitArray $Rows.$ListProperty
    $found = [System.Collections.Generic.List[object]]::new()
    foreach ($row in $rowArray.items) {
        if (Test-Dur050TaskTag $row.Tags) {
            $found.Add([ordered]@{ resource_id = [string]$row.$IdProperty; tags = $row.Tags })
        }
    }
    return [pscustomobject]@{ count = $found.Count; resources = $found.ToArray() }
}

function Get-Dur050LedgerOpenIntervals {
    param([object]$Ledger, [string]$RequestedCycleID)
    if ([string]$Ledger.schema -ne 'dur050-task-cost-ledger.v1' -or [string]$Ledger.task_id -ne 'DUR-050' -or $null -eq $Ledger.roles) {
        throw 'Task-wide cost ledger has an unsupported schema or task identity.'
    }
    $open = [System.Collections.Generic.List[object]]::new()
    foreach ($roleProperty in $Ledger.roles.PSObject.Properties) {
        $intervalArray = ConvertTo-Dur050ExplicitArray $roleProperty.Value
        foreach ($interval in $intervalArray.items) {
            if ([string]$interval.cycle_id -eq $RequestedCycleID -and [string]::IsNullOrWhiteSpace([string]$interval.destroy_completed_at_utc)) {
                $open.Add([ordered]@{ role = [string]$roleProperty.Name; instance_id = [string]$interval.instance_id; cycle_id = [string]$interval.cycle_id })
            }
        }
    }
    return $open.ToArray()
}

function Write-Dur050InventoryRecord([string]$Path, $Record) {
    $parent = Split-Path -Parent $Path
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    $json = ($Record | ConvertTo-Json -Depth 30) + [Environment]::NewLine
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try {
        $bytes = $utf8.GetBytes($json)
        $stream.Write($bytes, 0, $bytes.Length)
    } finally { $stream.Dispose() }
}

function Invoke-Dur050PostDestroyInventory {
    $startedAt = [DateTimeOffset]::UtcNow
    $manifest = $null
    $terraformIdentity = $null
    $record = [ordered]@{
        schema = 'dur050-post-destroy-inventory.v4'
        status = 'FAIL'
        cycle_id = $CycleID
        started_at_utc = $startedAt.ToString('o')
        checked_at_utc = $null
        account_id = $null
        region = $script:region
        raw_aws_responses = $script:rawAwsResponses
        checks = $script:checks
        violations = @()
        error = $null
    }
    try {
        if (Test-Path -LiteralPath $OutputPath) { throw "Refusing to overwrite post-destroy inventory record: $OutputPath" }
        foreach ($path in @($CampaignManifestPath, $TerraformStatePath, $LedgerPath)) {
            if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Required post-destroy input is missing: $path" }
        }
        $manifest = Get-Content -LiteralPath $CampaignManifestPath -Raw | ConvertFrom-Json -ErrorAction Stop
        if ([string]$manifest.schema -ne 'dur050-cost-manifest.v2' -or [string]$manifest.terraform_profile.task_id -ne 'DUR-050' -or
            [string]$manifest.terraform_profile.aws_region -ne $script:region) { throw 'Campaign manifest is not the DUR-050 us-west-1 profile.' }
        $record.campaign_id = [string]$manifest.campaign_id
        $record.campaign_manifest_path = [System.IO.Path]::GetFullPath($CampaignManifestPath)
        $record.campaign_manifest_sha256 = (Get-FileHash -LiteralPath $CampaignManifestPath -Algorithm SHA256).Hash.ToUpperInvariant()
        $record.terraform_state_path = [System.IO.Path]::GetFullPath($TerraformStatePath)
        $record.ledger_path = [System.IO.Path]::GetFullPath($LedgerPath)

        $terraformIdentity = Resolve-Dur050TerraformExecutable -TerraformExe $TerraformExe
        if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1' -and $terraformIdentity.sha256 -cne $pinnedTerraformSha256) {
            throw "Terraform executable SHA-256 $($terraformIdentity.sha256) does not match the pinned 1.16.4 binary."
        }
        $record.terraform_executable = $terraformIdentity
        $stateListRaw = & $terraformIdentity.path "-chdir=$(Join-Path $repoRoot 'deploy/aws')" state list 2>&1 | Out-String
        $stateListExitCode = $LASTEXITCODE
        $stateAddresses = @($stateListRaw -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $record.checks.terraform_state_list = [ordered]@{ exit_code = $stateListExitCode; raw_output = $stateListRaw.TrimEnd(); resource_count = $stateAddresses.Count; empty = ($stateListExitCode -eq 0 -and $stateAddresses.Count -eq 0) }
        if ($stateListExitCode -ne 0) { $script:violations.Add("terraform state list failed with exit $stateListExitCode") }
        elseif ($stateAddresses.Count -ne 0) { $script:violations.Add("terraform state still contains $($stateAddresses.Count) resource(s)") }

        $instanceResponse = Invoke-Dur050InventoryAws 'ec2.describe-instances' @('ec2', 'describe-instances', '--filters', 'Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down', '--output', 'json')
        $instances = [System.Collections.Generic.List[object]]::new()
        $reservations = ConvertTo-Dur050ExplicitArray $instanceResponse.Reservations
        foreach ($reservation in $reservations.items) {
            $reservationInstances = ConvertTo-Dur050ExplicitArray $reservation.Instances
            foreach ($instance in $reservationInstances.items) { $instances.Add($instance) }
        }
        $nonTerminated = [System.Collections.Generic.List[object]]::new()
        foreach ($instance in $instances) {
            if ([string]$instance.State.Name -in @('pending', 'running', 'stopping', 'stopped', 'shutting-down')) { $nonTerminated.Add($instance) }
        }
        $record.checks.region_instances = [ordered]@{ non_terminated_count = $nonTerminated.Count; instances = @($nonTerminated.ToArray()) }
        if ($nonTerminated.Count -ne 0) { $script:violations.Add("region has $($nonTerminated.Count) non-terminated instance(s)") }

        $volumeResponse = Invoke-Dur050InventoryAws 'ec2.describe-volumes' @('ec2', 'describe-volumes', '--output', 'json')
        $volumes = ConvertTo-Dur050ExplicitArray $volumeResponse.Volumes
        $record.checks.region_volumes = [ordered]@{ volume_count = $volumes.count; volumes = $volumes.items }
        if ($volumes.count -ne 0) { $script:violations.Add("region has $($volumes.count) volume(s)") }

        $resourceChecks = @(
            @{ name = 'task_vpcs'; service = 'ec2'; operation = 'describe-vpcs'; list = 'Vpcs'; id = 'VpcId' },
            @{ name = 'task_subnets'; service = 'ec2'; operation = 'describe-subnets'; list = 'Subnets'; id = 'SubnetId' },
            @{ name = 'task_security_groups'; service = 'ec2'; operation = 'describe-security-groups'; list = 'SecurityGroups'; id = 'GroupId' },
            @{ name = 'task_network_interfaces'; service = 'ec2'; operation = 'describe-network-interfaces'; list = 'NetworkInterfaces'; id = 'NetworkInterfaceId' },
            @{ name = 'task_eips'; service = 'ec2'; operation = 'describe-addresses'; list = 'Addresses'; id = 'AllocationId' },
            @{ name = 'task_snapshots'; service = 'ec2'; operation = 'describe-snapshots'; list = 'Snapshots'; id = 'SnapshotId'; owner = $true },
            @{ name = 'task_nat_gateways'; service = 'ec2'; operation = 'describe-nat-gateways'; list = 'NatGateways'; id = 'NatGatewayId'; nat = $true }
        )
        foreach ($spec in $resourceChecks) {
            $awsArguments = @($spec.service, $spec.operation)
            if ($spec.ContainsKey('owner') -and $spec.owner) { $awsArguments += @('--owner-ids', 'self', '--filters', 'Name=tag:Task,Values=DUR-050') }
            elseif ($spec.ContainsKey('nat') -and $spec.nat) { $awsArguments += @('--filter', 'Name=tag:Task,Values=DUR-050') }
            else { $awsArguments += @('--filters', 'Name=tag:Task,Values=DUR-050') }
            $awsArguments += @('--output', 'json')
            $response = Invoke-Dur050InventoryAws "ec2.$($spec.operation)" $awsArguments
            $result = Get-Dur050TaggedRows -Rows $response -ListProperty $spec.list -IdProperty $spec.id
            $record.checks[$spec.name] = [ordered]@{ count = $result.count; resources = $result.resources }
            if ($result.count -ne 0) { $script:violations.Add("$($spec.name) has $($result.count) remaining resource(s)") }
        }

        $elbv2 = Invoke-Dur050InventoryAws 'elbv2.describe-load-balancers' @('elbv2', 'describe-load-balancers', '--output', 'json')
        $elbv2Rows = ConvertTo-Dur050ExplicitArray $elbv2.LoadBalancers
        $elbv2Arns = [System.Collections.Generic.List[string]]::new()
        foreach ($row in $elbv2Rows.items) { if (-not [string]::IsNullOrWhiteSpace([string]$row.LoadBalancerArn)) { $elbv2Arns.Add([string]$row.LoadBalancerArn) } }
        if ($elbv2Arns.Count -gt 0) {
            $elbv2Tags = Invoke-Dur050InventoryAws 'elbv2.describe-tags' (@('elbv2', 'describe-tags', '--resource-arns') + $elbv2Arns.ToArray() + @('--output', 'json'))
        } else { $elbv2Tags = [pscustomobject]@{ TagDescriptions = [object[]]::new(0) }; $script:rawAwsResponses['elbv2.describe-tags'] = '{"TagDescriptions":[]}' }
        $taskElbv2 = [System.Collections.Generic.List[object]]::new()
        foreach ($tagDescription in (ConvertTo-Dur050ExplicitArray $elbv2Tags.TagDescriptions).items) {
            if (Test-Dur050TaskTag $tagDescription.Tags) { $taskElbv2.Add([ordered]@{ resource_id = [string]$tagDescription.ResourceArn; tags = $tagDescription.Tags }) }
        }
        $record.checks.task_elbv2_load_balancers = [ordered]@{ count = $taskElbv2.Count; resources = $taskElbv2.ToArray() }
        if ($taskElbv2.Count -ne 0) { $script:violations.Add("task_elbv2_load_balancers has $($taskElbv2.Count) remaining resource(s)") }

        $classic = Invoke-Dur050InventoryAws 'elb.describe-load-balancers' @('elb', 'describe-load-balancers', '--output', 'json')
        $classicRows = ConvertTo-Dur050ExplicitArray $classic.LoadBalancerDescriptions
        $classicNames = [System.Collections.Generic.List[string]]::new()
        foreach ($row in $classicRows.items) { if (-not [string]::IsNullOrWhiteSpace([string]$row.LoadBalancerName)) { $classicNames.Add([string]$row.LoadBalancerName) } }
        if ($classicNames.Count -gt 0) {
            $classicTags = Invoke-Dur050InventoryAws 'elb.describe-tags' (@('elb', 'describe-tags', '--load-balancer-names') + $classicNames.ToArray() + @('--output', 'json'))
        } else { $classicTags = [pscustomobject]@{ TagDescriptions = [object[]]::new(0) }; $script:rawAwsResponses['elb.describe-tags'] = '{"TagDescriptions":[]}' }
        $taskClassic = [System.Collections.Generic.List[object]]::new()
        foreach ($tagDescription in (ConvertTo-Dur050ExplicitArray $classicTags.TagDescriptions).items) {
            if (Test-Dur050TaskTag $tagDescription.Tags) { $taskClassic.Add([ordered]@{ resource_id = [string]$tagDescription.LoadBalancerName; tags = $tagDescription.Tags }) }
        }
        $record.checks.task_classic_load_balancers = [ordered]@{ count = $taskClassic.Count; resources = $taskClassic.ToArray() }
        if ($taskClassic.Count -ne 0) { $script:violations.Add("task_classic_load_balancers has $($taskClassic.Count) remaining resource(s)") }

        $namePrefix = "$($manifest.terraform_profile.project_name)-$($manifest.terraform_profile.campaign_slug)"
        $roles = Invoke-Dur050InventoryAws 'iam.list-roles' @('iam', 'list-roles', '--output', 'json')
        $roleRows = ConvertTo-Dur050ExplicitArray $roles.Roles
        $remainingRoles = @($roleRows.items | Where-Object { ([string]$_.RoleName).StartsWith($namePrefix, [StringComparison]::Ordinal) })
        $record.checks.iam_roles = [ordered]@{ prefix = $namePrefix; count = $remainingRoles.Count; names = @($remainingRoles | ForEach-Object { [string]$_.RoleName }) }
        if ($remainingRoles.Count -ne 0) { $script:violations.Add("$($remainingRoles.Count) DUR-050 IAM role(s) remain") }

        $profiles = Invoke-Dur050InventoryAws 'iam.list-instance-profiles' @('iam', 'list-instance-profiles', '--output', 'json')
        $profileRows = ConvertTo-Dur050ExplicitArray $profiles.InstanceProfiles
        $remainingProfiles = @($profileRows.items | Where-Object { ([string]$_.InstanceProfileName).StartsWith($namePrefix, [StringComparison]::Ordinal) })
        $record.checks.iam_instance_profiles = [ordered]@{ prefix = $namePrefix; count = $remainingProfiles.Count; names = @($remainingProfiles | ForEach-Object { [string]$_.InstanceProfileName }) }
        if ($remainingProfiles.Count -ne 0) { $script:violations.Add("$($remainingProfiles.Count) DUR-050 IAM instance profile(s) remain") }

        $parameterNames = @("/$namePrefix/dur050-observer-password", "/$namePrefix/postgres-password")
        $parameters = Invoke-Dur050InventoryAws 'ssm.get-parameters' (@('ssm', 'get-parameters', '--names') + $parameterNames + @('--output', 'json'))
        $parameterRows = ConvertTo-Dur050ExplicitArray $parameters.Parameters
        $invalidRows = ConvertTo-Dur050ExplicitArray $parameters.InvalidParameters
        $returnedNames = @($parameterRows.items | ForEach-Object { [string]$_.Name })
        $invalidNames = @($invalidRows.items | ForEach-Object { [string]$_ })
        $missingNames = @($parameterNames | Where-Object { $_ -notin $invalidNames })
        $record.checks.ssm_parameters = [ordered]@{ expected_names = $parameterNames; returned_names = $returnedNames; invalid_parameters = $invalidNames; missing_expected_names = $missingNames; absent = ($returnedNames.Count -eq 0 -and $missingNames.Count -eq 0) }
        if ($returnedNames.Count -ne 0 -or $missingNames.Count -ne 0) { $script:violations.Add("SSM parameters are not all absent: returned=$($returnedNames -join ',') invalid-missing=$($missingNames -join ',')") }

        $ledger = Get-Content -LiteralPath $LedgerPath -Raw | ConvertFrom-Json -ErrorAction Stop
        $openIntervals = @(Get-Dur050LedgerOpenIntervals -Ledger $ledger -RequestedCycleID $CycleID)
        $record.checks.ledger_open_intervals = [ordered]@{ cycle_id = $CycleID; open_interval_count = $openIntervals.Count; intervals = $openIntervals; ledger_sha256 = (Get-FileHash -LiteralPath $LedgerPath -Algorithm SHA256).Hash.ToUpperInvariant() }
        if ($openIntervals.Count -ne 0) { $script:violations.Add("task ledger has $($openIntervals.Count) open interval(s) for cycle $CycleID") }

        $record.violations = $script:violations.ToArray()
        $record.status = if ($script:violations.Count -eq 0) { 'PASS' } else { 'FAIL' }
    } catch {
        $record.error = $_.Exception.Message
        if ($script:violations.Count -gt 0) { $record.violations = $script:violations.ToArray() }
    }
    $record.checked_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    $record.raw_aws_responses = $script:rawAwsResponses
    $record.checks = $script:checks
    try { Write-Dur050InventoryRecord -Path $OutputPath -Record $record }
    catch { Write-Error "Could not write post-destroy inventory record: $($_.Exception.Message)"; return 1 }
    $record | ConvertTo-Json -Depth 30
    if ($record.status -ne 'PASS') { Write-Error ("DUR-050 post-destroy inventory FAIL: " + (@($record.violations) -join '; ') + " " + [string]$record.error); return 1 }
    Write-Host 'DUR-050 post-destroy inventory PASS.'
    return 0
}

if (-not (Test-Path -LiteralPath $OutputPath)) { exit (Invoke-Dur050PostDestroyInventory) }
throw "Refusing to overwrite post-destroy inventory record: $OutputPath"
