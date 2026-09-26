[CmdletBinding(DefaultParameterSetName='Open')]
param(
    [Parameter(Mandatory, ParameterSetName='Open')][switch]$Open,
    [Parameter(Mandatory, ParameterSetName='Close')][switch]$Close,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9-]{1,32}$')][string]$CycleID,
    [Parameter(Mandatory, ParameterSetName='Open')][string]$TerraformOutputsPath,
    [Parameter(Mandatory, ParameterSetName='Open')][string]$ApplyStartedAtUtc,
    [Parameter(ParameterSetName='Close')][string]$DestroyCompletedAtUtc = ([DateTimeOffset]::UtcNow.ToString('o')),
    [string]$LedgerPath,
    [string]$PythonExe = 'python'
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$defaultLedger = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json'
if (-not $LedgerPath) { $LedgerPath = $defaultLedger }
elseif ($env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE -ne '1') { throw 'LedgerPath override is test-only and requires DUR050_ENABLE_TEST_LEDGER_OVERRIDE=1.' }
if (-not (Test-Path -LiteralPath $LedgerPath -PathType Leaf)) { throw "Task-wide ledger is missing: $LedgerPath" }

function Read-Dur050LedgerUtc([object]$Value, [string]$Field) {
    if ($Value -is [DateTime]) {
        if ($Value.Kind -eq [DateTimeKind]::Unspecified) { throw "$Field must have an explicit UTC offset." }
        return [DateTimeOffset]::new($Value.ToUniversalTime())
    }
    if ($Value -is [DateTimeOffset]) { return $Value.ToUniversalTime() }
    $text = [string]$Value
    $parsed = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse($text, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::RoundtripKind, [ref]$parsed) -or
        -not $text -or $text -notmatch '(?:Z|[+-][0-9]{2}:[0-9]{2})$') { throw "$Field must be an RFC3339 timestamp with an explicit offset." }
    return $parsed.ToUniversalTime()
}

function Get-Dur050TerraformHosts($Outputs) {
    foreach ($name in @('app_instance_ids','dependency_instance_id','load_generator_instance_ids')) {
        if ($null -eq $Outputs.$name -or $null -eq $Outputs.$name.value) { throw "Terraform output '$name' is missing." }
    }
    $apps = @($Outputs.app_instance_ids.value)
    $gens = @($Outputs.load_generator_instance_ids.value)
    if ($apps.Count -ne 2 -or $gens.Count -ne 1) { throw 'Terraform outputs must contain two app IDs and one load-generator ID.' }
    $hosts = @(
        [pscustomobject]@{ role='app-1'; id=[string]$apps[0]; type='c7i.large' },
        [pscustomobject]@{ role='app-2'; id=[string]$apps[1]; type='c7i.large' },
        [pscustomobject]@{ role='dependency'; id=[string]$Outputs.dependency_instance_id.value; type='m7i.large' },
        [pscustomobject]@{ role='load-generator'; id=[string]$gens[0]; type='c7i.large' }
    )
    $ids = @($hosts | ForEach-Object id)
    if (($ids | Select-Object -Unique).Count -ne 4 -or @($ids | Where-Object { $_ -notmatch '^i-[0-9a-f]{17}$' }).Count -ne 0) { throw 'Terraform output host IDs must be four unique canonical instance IDs.' }
    return $hosts
}

function Invoke-Dur050LedgerAws([string[]]$Arguments) {
    $raw = & aws --region us-west-1 @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "Read-only EC2 lookup failed: $raw" }
    try { return $raw | ConvertFrom-Json -ErrorAction Stop } catch { throw 'EC2 describe-instances returned invalid JSON.' }
}

function Assert-Dur050LedgerCandidate([string]$Path) {
    $candidateLedger = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -ErrorAction Stop
    if ([string]$candidateLedger.schema -ne 'dur050-task-cost-ledger.v1' -or [string]$candidateLedger.task_id -ne 'DUR-050') { throw 'Serialized candidate ledger has an invalid schema or task ID.' }
    $roleTypes = [ordered]@{ 'app-1'='c7i.large'; 'app-2'='c7i.large'; 'dependency'='m7i.large'; 'load-generator'='c7i.large' }
    $allIds = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($role in $roleTypes.Keys) {
        if ($null -eq $candidateLedger.roles.$role) { throw "Serialized candidate ledger is missing role $role." }
        $rows = @($candidateLedger.roles.$role)
        $priorEnd = $null
        for ($index = 0; $index -lt $rows.Count; $index++) {
            $row = $rows[$index]
            if ([string]$row.instance_type -ne $roleTypes[$role] -or [string]$row.instance_id -notmatch '^i-[0-9a-f]{17}$') { throw "Serialized candidate ledger has an invalid host for $role." }
            if (-not $allIds.Add([string]$row.instance_id)) { throw "Serialized candidate ledger reuses instance ID $($row.instance_id)." }
            if ([string]$row.cycle_id -notmatch '^[A-Za-z0-9-]{1,32}$') { throw "Serialized candidate ledger has an invalid cycle ID for $role." }
            $started = Read-Dur050LedgerUtc $row.apply_started_at_utc "$role.apply_started_at_utc"
            if ($null -ne $priorEnd -and $started -lt $priorEnd) { throw "Serialized candidate ledger has overlapping intervals for $role." }
            $isOpen = $null -eq $row.destroy_completed_at_utc
            if ($isOpen -and $index -ne ($rows.Count - 1)) { throw "Only the final $role interval may remain open." }
            if (-not $isOpen) {
                $ended = Read-Dur050LedgerUtc $row.destroy_completed_at_utc "$role.destroy_completed_at_utc"
                if ($ended -lt $started) { throw "Serialized candidate ledger has a negative interval for $role." }
                $priorEnd = $ended
            } else {
                $priorEnd = $null
            }
            if ($null -ne $row.instance_launch_at_utc) {
                $launched = Read-Dur050LedgerUtc $row.instance_launch_at_utc "$role.instance_launch_at_utc"
                if ($started -gt $launched) { throw "Serialized apply start is later than LaunchTime for $role." }
            }
        }
    }
}

$originalBytes = [IO.File]::ReadAllBytes($LedgerPath)
$ledger = [Text.Encoding]::UTF8.GetString($originalBytes) | ConvertFrom-Json -ErrorAction Stop
if ($ledger.schema -ne 'dur050-task-cost-ledger.v1' -or $ledger.task_id -ne 'DUR-050') { throw 'Task ledger schema or task ID is invalid.' }
$timestamp = if ($Open) { Read-Dur050LedgerUtc $ApplyStartedAtUtc 'ApplyStartedAtUtc' } else { Read-Dur050LedgerUtc $DestroyCompletedAtUtc 'DestroyCompletedAtUtc' }

if ($Open) {
    if (-not (Test-Path -LiteralPath $TerraformOutputsPath -PathType Leaf)) { throw "Terraform outputs file is missing: $TerraformOutputsPath" }
    $outputs = Get-Content -LiteralPath $TerraformOutputsPath -Raw | ConvertFrom-Json -ErrorAction Stop
    $hosts = @(Get-Dur050TerraformHosts $outputs)
    foreach ($role in @('app-1','app-2','dependency','load-generator')) {
        if (@($ledger.roles.$role | Where-Object { $null -eq $_.destroy_completed_at_utc }).Count -ne 0) { throw "Cannot open cycle ${CycleID}: role $role already has an open interval." }
        if (@($ledger.roles.$role | Where-Object { [string]$_.cycle_id -eq $CycleID }).Count -ne 0) { throw "Refusing to overwrite existing ledger cycle $CycleID for $role." }
    }
    $allIds = @($ledger.roles.PSObject.Properties | ForEach-Object { $_.Value } | ForEach-Object { $_ } | ForEach-Object { [string]$_.instance_id })
    foreach ($hostSpec in $hosts) { if ($allIds -contains $hostSpec.id) { throw "Refusing reused EC2 instance ID $($hostSpec.id)." } }
    $idArgs = @('ec2','describe-instances','--instance-ids') + @($hosts | ForEach-Object id) + @('--output','json')
    $response = Invoke-Dur050LedgerAws $idArgs
    $instances = @($response.Reservations | ForEach-Object { $_.Instances } | ForEach-Object { $_ })
    if ($instances.Count -ne 4) { throw "EC2 returned $($instances.Count) instances; expected exactly four." }
    foreach ($hostSpec in $hosts) {
        $found = @($instances | Where-Object { [string]$_.InstanceId -eq $hostSpec.id })
        if ($found.Count -ne 1 -or [string]$found[0].InstanceType -ne $hostSpec.type) { throw "EC2 instance ID/type does not match Terraform output for $($hostSpec.role)." }
        if (-not $found[0].LaunchTime) { throw "EC2 omitted LaunchTime for $($hostSpec.id)." }
        $launch = ([DateTimeOffset]$found[0].LaunchTime).ToUniversalTime()
        if ($timestamp -gt $launch) { throw "Apply start timestamp is later than EC2 LaunchTime for $($hostSpec.id)." }
    }
    foreach ($hostSpec in $hosts) {
        $instance = @($instances | Where-Object { [string]$_.InstanceId -eq $hostSpec.id })[0]
        $ledger.roles.($hostSpec.role) = @($ledger.roles.($hostSpec.role)) + @([ordered]@{
            cycle_id = $CycleID
            instance_id = $hostSpec.id
            instance_type = $hostSpec.type
            apply_started_at_utc = $timestamp.ToString('yyyy-MM-ddTHH:mm:ss.fffffffZ', [Globalization.CultureInfo]::InvariantCulture)
            destroy_completed_at_utc = $null
            instance_launch_at_utc = ([DateTimeOffset]$instance.LaunchTime).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffffffZ', [Globalization.CultureInfo]::InvariantCulture)
            apply_start_source = 'operator-recorded apply start, validated no later than EC2 LaunchTime'
        })
    }
} else {
    $openRows = @()
    foreach ($role in @('app-1','app-2','dependency','load-generator')) {
        $rows = @($ledger.roles.$role | Where-Object { $null -eq $_.destroy_completed_at_utc -and [string]$_.cycle_id -eq $CycleID })
        if ($rows.Count -ne 1) { throw "Cannot close cycle ${CycleID}: expected exactly one open $role interval, found $($rows.Count)." }
        $started = Read-Dur050LedgerUtc $rows[0].apply_started_at_utc "$role.apply_started_at_utc"
        if ($timestamp -lt $started) { throw "Destroy completion precedes apply start for $role." }
        $openRows += [pscustomobject]@{ role=$role; row=$rows[0] }
    }
    foreach ($item in $openRows) {
        $item.row.destroy_completed_at_utc = $timestamp.ToString('yyyy-MM-ddTHH:mm:ss.fffffffZ', [Globalization.CultureInfo]::InvariantCulture)
        $item.row | Add-Member -NotePropertyName destroy_timestamp_source -NotePropertyValue 'ledger-record -Close observation' -Force
    }
}

# Validate the complete candidate ledger before any file replacement.
$tempLedger = "$LedgerPath.$([guid]::NewGuid().ToString('N')).tmp"
try {
    $json = ($ledger | ConvertTo-Json -Depth 30) + "`n"
    [IO.File]::WriteAllText($tempLedger, $json, [Text.UTF8Encoding]::new($false))
    if ((Get-Item -LiteralPath $tempLedger).Length -eq 0) { throw 'Serialized candidate ledger is empty.' }
    Assert-Dur050LedgerCandidate $tempLedger
    [IO.File]::Move($tempLedger, $LedgerPath, $true)
    Write-Output ([ordered]@{ schema='dur050-ledger-record.v1'; status='PASS'; operation=if($Open){'OPEN'}else{'CLOSE'}; cycle_id=$CycleID; ledger_path=[IO.Path]::GetFullPath($LedgerPath); host_count=4; recorded_at_utc=[DateTimeOffset]::UtcNow.ToString('o') } | ConvertTo-Json -Depth 8)
} finally {
    Remove-Item -LiteralPath $tempLedger -Force -ErrorAction SilentlyContinue
}
