[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$TerraformOutputsPath,
    [Parameter(Mandatory)] [string]$PreparationDryRunPath,
    [Parameter(Mandatory)] [ValidateSet('unloaded', 'window', 'sink-check')] [string]$Mode,
    [Parameter(Mandatory)] [string]$DatabaseName,
    [Parameter(Mandatory)] [string]$OutputDirectory,
    [string]$BlockID,
    [ValidateSet('seq-8', 'fanout-8')] [string]$Family,
    [string]$RunID,
    [string]$Rate,
    [ValidateSet('count', 'duration')] [string]$RunMode,
    [string]$Value,
    [string]$RecordPath,
    [string]$Region = 'us-west-1',
    [ValidateRange(1, 240)] [int]$TimeoutMinutes = 60,
    [switch]$StagePlanOnly
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'dur050-ssm-wrapper.ps1')

function Get-OutputValue($Outputs, [string]$Name) {
    $property = $Outputs.PSObject.Properties[$Name]
    if ($null -eq $property -or $null -eq $property.Value -or $null -eq $property.Value.value) {
        throw "Terraform output '$Name' is missing or null."
    }
    return $property.Value.value
}

function ConvertTo-SafeBashWord([string]$Value, [string]$Label) {
    if ([string]::IsNullOrWhiteSpace($Value) -or $Value -match "['\r\n\x00]") {
        throw "$Label is empty or contains characters unsafe for a single Bash argument."
    }
    return "'$Value'"
}

function Get-RemoteRepositoryRoot {
    $productionRoot = '/opt/durable-agent-execution-engine'
    if ($env:DUR050_REMOTE_REPO_ROOT) {
        if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') {
            throw 'DUR050_REMOTE_REPO_ROOT requires DUR050_ENABLE_TEST_HOOKS=1.'
        }
        if ($env:DUR050_REMOTE_REPO_ROOT -notmatch '^/[A-Za-z0-9._/-]+$' -or $env:DUR050_REMOTE_REPO_ROOT -match '(^|/)\.\.?(/|$)') {
            throw 'DUR050_REMOTE_REPO_ROOT must be a normalized absolute test path.'
        }
        return $env:DUR050_REMOTE_REPO_ROOT.TrimEnd('/')
    }
    return $productionRoot
}

if ($Region -ne 'us-west-1') { throw 'DUR-050 generator dispatch is restricted to us-west-1.' }
if (-not (Test-Path -LiteralPath $TerraformOutputsPath -PathType Leaf)) { throw "Terraform outputs JSON not found: $TerraformOutputsPath" }
if (-not (Test-Path -LiteralPath $PreparationDryRunPath -PathType Leaf)) { throw "Preparation dry-run record not found: $PreparationDryRunPath" }
if ($DatabaseName -notmatch '^[A-Za-z_][A-Za-z0-9_]{0,62}$') { throw 'DatabaseName must be a valid simple PostgreSQL identifier.' }
if ($OutputDirectory -notmatch '^/var/tmp/dur050-[A-Za-z0-9._/-]+$' -or $OutputDirectory -match '(^|/)\.\.?(/|$)') {
    throw 'OutputDirectory must be a normalized path below /var/tmp/dur050-*.'
}

$outputs = Get-Content -LiteralPath $TerraformOutputsPath -Raw | ConvertFrom-Json
$generatorIDs = @(Get-OutputValue $outputs 'load_generator_instance_ids')
if ($generatorIDs.Count -ne 1 -or [string]$generatorIDs[0] -notmatch '^i-[0-9a-f]{8,17}$') {
    throw 'Terraform must contain exactly one valid DUR-050 generator instance ID.'
}
$instanceID = [string]$generatorIDs[0]
$databaseIP = [string](Get-OutputValue $outputs 'dependency_private_ip')
$parsedIP = $null
if (-not [System.Net.IPAddress]::TryParse($databaseIP, [ref]$parsedIP) -or $parsedIP.AddressFamily -ne [System.Net.Sockets.AddressFamily]::InterNetwork) {
    throw 'Terraform dependency_private_ip must be an IPv4 address.'
}
$bytes = $parsedIP.GetAddressBytes()
$privateAddress = ($bytes[0] -eq 10) -or ($bytes[0] -eq 172 -and $bytes[1] -ge 16 -and $bytes[1] -le 31) -or ($bytes[0] -eq 192 -and $bytes[1] -eq 168)
if (-not $privateAddress) { throw 'Terraform dependency_private_ip must be RFC1918 private IPv4.' }
$secretParameter = [string](Get-OutputValue $outputs 'dur050_observer_secret_parameter_name')
if ($secretParameter -notmatch '^/dur050/[A-Za-z0-9_./-]{1,180}$' -or $secretParameter -match '(^|/)\.\.?(/|$)') {
    throw 'Terraform observer secret parameter must be a normalized /dur050/* SSM parameter name.'
}

$prepared = Get-Content -LiteralPath $PreparationDryRunPath -Raw | ConvertFrom-Json
if ($prepared.schema -ne 'dur050-pilot-preparation-dry-run.v1' -or @($prepared.stages).Count -lt 1) {
    throw 'Preparation record must be a DUR-050 pilot preparation dry-run with recorded stages.'
}
$generatorStage = @($prepared.stages | Where-Object { $_.stage -eq 'generator-stage' })
if ($generatorStage.Count -ne 1) { throw 'Preparation record must contain exactly one generator-stage record.' }
$configPath = [string]$generatorStage[0].rendered_config_path
$configHash = [string]$generatorStage[0].rendered_config_sha256
if ($configHash -notmatch '^[0-9a-f]{64}$') { throw 'Preparation record lacks a valid rendered-config SHA-256.' }
$repoRoot = Get-RemoteRepositoryRoot
if ($configPath -notmatch ('^' + [regex]::Escape($repoRoot) + '/[A-Za-z0-9._/-]+$') -or $configPath -match '(^|/)\.\.?(/|$)') {
    throw 'Recorded rendered_config_path must be a normalized file below the deployed repository root.'
}

$script = switch ($Mode) {
    'unloaded' { "$repoRoot/scripts/dur050-run-unloaded-block.sh" }
    'window' { "$repoRoot/scripts/dur050-run-window.sh" }
    'sink-check' { "$repoRoot/scripts/dur050-run-sink-check.sh" }
}
$runnerArgs = [System.Collections.Generic.List[string]]::new()
$runnerArgs.Add($script)
$runnerArgs.Add($configPath)
switch ($Mode) {
    'unloaded' {
        if ($BlockID -notmatch '^[A-Za-z0-9-]{1,32}$' -or -not $Family) { throw 'Unloaded mode requires a valid BlockID and Family.' }
        $runnerArgs.Add($BlockID); $runnerArgs.Add($Family); $runnerArgs.Add($OutputDirectory)
    }
    'window' {
        if ($RunID -notmatch '^[A-Za-z0-9-]{1,48}$' -or $Rate -notmatch '^[0-9]+(\.[0-9]+)?$' -or [decimal]$Rate -le 0) { throw 'Window mode requires a valid RunID and positive decimal Rate.' }
        if (-not $RunMode) { throw 'Window mode requires RunMode=count or duration.' }
        if ($Value -notmatch '^[1-9][0-9]{0,8}$') { throw 'Window mode requires a positive integer Value.' }
        $numericValue = [int]$Value
        if ($RunMode -eq 'count' -and ($numericValue -lt 2 -or $numericValue % 2 -ne 0)) { throw 'Count mode requires an even Value >= 2.' }
        if ($RunMode -eq 'duration' -and [decimal]$Rate * $numericValue -lt 2) { throw 'Duration mode must schedule at least two requests.' }
        $runnerArgs.Add($RunID); $runnerArgs.Add($Rate); $runnerArgs.Add($RunMode); $runnerArgs.Add($Value); $runnerArgs.Add($OutputDirectory)
    }
    'sink-check' { $runnerArgs.Add($OutputDirectory) }
}

foreach ($word in $runnerArgs) { [void](ConvertTo-SafeBashWord $word 'Runner argument') }
$remoteLines = @(
    'set -euo pipefail',
    "export DUR050_OBSERVER_SECRET_PARAMETER=$(ConvertTo-SafeBashWord $secretParameter 'Observer parameter')",
    "export DUR050_DATABASE_PRIVATE_IP=$(ConvertTo-SafeBashWord $databaseIP 'Database IP')",
    "export DUR050_DATABASE_NAME=$(ConvertTo-SafeBashWord $DatabaseName 'Database name')",
    ('test "$(sha256sum -- {0} | cut -d '' '' -f1)" = {1}' -f (ConvertTo-SafeBashWord $configPath 'Rendered config path'), (ConvertTo-SafeBashWord $configHash 'Rendered config SHA-256')),
    ('bash ' + (($runnerArgs | ForEach-Object { ConvertTo-SafeBashWord $_ 'Runner argument' }) -join ' '))
)
$stage = "generator-$Mode-" + ([IO.Path]::GetFileName($OutputDirectory) -replace '[^A-Za-z0-9_-]', '-')
$wrapped = New-Dur050SsmBashCommand -Stage $stage -RemoteLines $remoteLines
$stagePlan = [ordered]@{
    schema = 'dur050-generator-dispatch-plan.v1'
    stage = $stage
    mode = $Mode
    instance_id = $instanceID
    database_name = $DatabaseName
    rendered_config_path = $configPath
    rendered_config_sha256 = $configHash
    remote_output_directory = $OutputDirectory
    timeout_minutes = $TimeoutMinutes
    remote_lines = $remoteLines
    wrapped_command = $wrapped
}
if ($StagePlanOnly) {
    $stagePlan | ConvertTo-Json -Depth 6
    exit 0
}

if (-not $RecordPath) { throw 'RecordPath is required for a live dispatch so the SSM command ID is retained locally.' }
if (Test-Path -LiteralPath $RecordPath) { throw "Refusing to overwrite dispatch record: $RecordPath" }
$resultPath = Join-Path ([IO.Path]::GetTempPath()) ("dur050-dispatch-$([guid]::NewGuid().ToString('N')).json")
try {
    $null = & (Join-Path $PSScriptRoot 'dur050-invoke-ssm-command.ps1') -Stage $stage -InstanceID $instanceID -RemoteLines $remoteLines -Region $Region -TimeoutMinutes $TimeoutMinutes -ResultPath $resultPath
    $result = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
    if ($result.status -ne 'Success') { throw "Generator runner SSM command did not succeed: $($result.status)." }
    $stdoutBytes = [Text.UTF8Encoding]::new($false).GetBytes([string]$result.standard_output_content)
    $record = [ordered]@{
        schema = 'dur050-generator-dispatch.v1'
        status = 'PASS'
        created_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        mode = $Mode
        instance_id = $instanceID
        ssm_command_id = [string]$result.command_id
        ssm_status = [string]$result.status
        rendered_config_path = $configPath
        rendered_config_sha256 = $configHash
        remote_output_directory = $OutputDirectory
        remote_lines = $remoteLines
        standard_output_bytes = $stdoutBytes.Length
        standard_output_sha256 = [BitConverter]::ToString(([Security.Cryptography.SHA256]::Create()).ComputeHash($stdoutBytes)).Replace('-', '').ToLowerInvariant()
        timeout_minutes = $TimeoutMinutes
    }
    $parent = Split-Path -Parent ([IO.Path]::GetFullPath($RecordPath))
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    $tempRecord = "$RecordPath.$([guid]::NewGuid().ToString('N')).tmp"
    [IO.File]::WriteAllText($tempRecord, ($record | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $tempRecord -Destination $RecordPath
    $record | ConvertTo-Json -Depth 8
} catch {
    if (-not (Test-Path -LiteralPath $RecordPath)) {
        $failedCommandID = $null
        if (Test-Path -LiteralPath $resultPath -PathType Leaf) {
            try { $failedCommandID = [string](Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json).command_id } catch { }
        }
        $failedRecord = [ordered]@{
            schema = 'dur050-generator-dispatch.v1'
            status = 'FAIL'
            created_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
            mode = $Mode
            instance_id = $instanceID
            ssm_command_id = $failedCommandID
            rendered_config_path = $configPath
            rendered_config_sha256 = $configHash
            remote_output_directory = $OutputDirectory
            error = $_.Exception.Message
        }
        $parent = Split-Path -Parent ([IO.Path]::GetFullPath($RecordPath))
        if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
        [IO.File]::WriteAllText($RecordPath, ($failedRecord | ConvertTo-Json -Depth 6), [Text.UTF8Encoding]::new($false))
    }
    throw
} finally {
    Remove-Item -LiteralPath $resultPath -Force -ErrorAction SilentlyContinue
}
