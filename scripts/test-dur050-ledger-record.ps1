#Requires -Version 7.5
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$sourceScript = Join-Path $PSScriptRoot 'dur050-ledger-record.ps1'
$pwsh = [Environment]::ProcessPath
$temp = Join-Path $repoRoot ('.dur050-ledger-backfill-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
$utf8 = [Text.UTF8Encoding]::new($false)
$env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE = '1'
$priorTz = $env:TZ
$env:TZ = 'America/Los_Angeles'

function Write-Json([string]$Path, $Value) {
    $json = ($Value | ConvertTo-Json -Depth 30).Replace("`r", '') + "`n"
    [IO.File]::WriteAllText($Path, $json, $utf8)
}
function Get-RowJsonText([string]$Path, [string]$InstanceId) {
    $text = [IO.File]::ReadAllText($Path, [Text.Encoding]::UTF8)
    $needle = '"instance_id"\s*:\s*"' + [regex]::Escape($InstanceId) + '"'
    $rowMatches = [regex]::Matches($text, $needle)
    if ($rowMatches.Count -ne 1) { throw "Expected one ledger row for $InstanceId; found $($rowMatches.Count)." }
    $start = $text.LastIndexOf('{', $rowMatches[0].Index)
    if ($start -lt 0) { throw "Could not locate ledger row start for $InstanceId." }
    $depth = 0; $quoted = $false; $escaped = $false
    for ($index = $start; $index -lt $text.Length; $index++) {
        $ch = $text[$index]
        if ($quoted) {
            if ($escaped) { $escaped = $false; continue }
            if ($ch -eq '\') { $escaped = $true; continue }
            if ($ch -eq '"') { $quoted = $false }
            continue
        }
        if ($ch -eq '"') { $quoted = $true; continue }
        if ($ch -eq '{') { $depth++ }
        elseif ($ch -eq '}') {
            $depth--
            if ($depth -eq 0) { return $text.Substring($start, $index - $start + 1) }
        }
    }
    throw "Could not locate ledger row end for $InstanceId."
}
function Add-SentinelRows([string]$Path) {
    $ledger = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable -DateKind String
    $sentinels = [ordered]@{
        'app-1' = 'i-aaaaaaaaaaaaaaaaa'
        'app-2' = 'i-bbbbbbbbbbbbbbbbb'
        'dependency' = 'i-ccccccccccccccccc'
        'load-generator' = 'i-ddddddddddddddddd'
    }
    $types = [ordered]@{ 'app-1'='c7i.large'; 'app-2'='c7i.large'; 'dependency'='m7i.large'; 'load-generator'='c7i.large' }
    foreach ($role in $sentinels.Keys) {
        $start = if ($role -eq 'app-1') { '2020-01-01T00:00:00.1234000+00:00' } else { '2020-01-01T00:00:00.1234000Z' }
        $end = if ($role -eq 'app-1') { '2020-01-01T01:00:00.4572760Z' } else { '2020-01-01T01:00:00.4572760+00:00' }
        $sentinel = [ordered]@{
            cycle_id='r192-byte-preservation'
            instance_id=$sentinels[$role]
            instance_type=$types[$role]
            apply_started_at_utc=$start
            destroy_completed_at_utc=$end
            interval_source='test sentinel for exact row-byte preservation'
        }
        $ledger.roles[$role] = @($sentinel) + @($ledger.roles[$role])
    }
    Write-Json $Path $ledger
    $snapshots = [ordered]@{}
    foreach ($role in $sentinels.Keys) { $snapshots[$role] = Get-RowJsonText $Path $sentinels[$role] }
    return [pscustomobject]@{ ids=$sentinels; rows=$snapshots }
}
function Assert-SentinelRowsByteIdentical([string]$Path, $Snapshot, [string]$Operation) {
    foreach ($role in $Snapshot.ids.Keys) {
        $current = Get-RowJsonText $Path $Snapshot.ids[$role]
        if ($current -cne [string]$Snapshot.rows[$role]) {
            $beforeEscaped = ConvertTo-Json -InputObject ([string]$Snapshot.rows[$role]) -Compress
            $afterEscaped = ConvertTo-Json -InputObject $current -Compress
            throw "Pre-existing $role ledger row changed byte-for-byte during $Operation.`nBefore: $beforeEscaped`nAfter:  $afterEscaped"
        }
    }
    Write-Host "PASS: pre-existing sentinel rows remain byte-identical after $Operation."
}
function Test-RecordedEvidenceHash([string]$Path, [string]$ExpectedHash) {
    $rawHash = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToUpperInvariant()
    if ($rawHash -ceq $ExpectedHash) { return $true }

    # Git may normalize these committed text records from CRLF to LF on Linux.
    # Reconstruct the producer's CRLF lines plus final LF before comparing the
    # unchanged provenance digest; content edits still change the normalized hash.
    $text = [IO.File]::ReadAllText($Path, [Text.Encoding]::UTF8)
    $lfText = [regex]::Replace($text, "`r`n|`r|`n", "`n")
    $hasFinalLf = $lfText.EndsWith("`n", [StringComparison]::Ordinal)
    $body = if ($hasFinalLf) { $lfText.Substring(0, $lfText.Length - 1) } else { $lfText }
    $finalLf = if ($hasFinalLf) { "`n" } else { '' }
    $producerText = $body.Replace("`n", "`r`n") + $finalLf
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($producerText)
    $normalizedHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes))
    return $normalizedHash -ceq $ExpectedHash
}
function Invoke-Backfill([string]$Script, [string]$Name, [string]$Ledger, [string]$Outputs, [string]$Apply, [string]$Destroy, [string]$Cycle = 'cycle-r187-test') {
    $args = @('-NoProfile','-File',$Script,'-Backfill','-CycleID',$Cycle,'-TerraformOutputsPath',$Outputs,'-ApplyRecordPath',$Apply,'-DestroyRecordPath',$Destroy,'-LedgerPath',$Ledger)
    $result = & $pwsh @args 2>&1 | Out-String
    return [pscustomobject]@{ ExitCode=$LASTEXITCODE; Text=$result; Name=$Name }
}
function Invoke-LedgerOperation([string]$Script, [string[]]$Arguments, [string]$AwsStubDir) {
    $oldPath = $env:PATH
    try {
        if ($AwsStubDir) { $env:PATH = $AwsStubDir + [IO.Path]::PathSeparator + $oldPath }
        $result = & $pwsh -NoProfile -File $Script @Arguments 2>&1 | Out-String
        return [pscustomobject]@{ ExitCode=$LASTEXITCODE; Text=$result }
    } finally { $env:PATH = $oldPath }
}
function Write-LedgerAwsStub([string]$Directory) {
    $unix = @'
#!/bin/sh
printf '%s\n' '{"Reservations":[{"Instances":[{"InstanceId":"i-11111111111111111","InstanceType":"c7i.large","LaunchTime":"2030-01-02T00:00:00Z"},{"InstanceId":"i-22222222222222222","InstanceType":"c7i.large","LaunchTime":"2030-01-02T00:00:00Z"},{"InstanceId":"i-33333333333333333","InstanceType":"m7i.large","LaunchTime":"2030-01-02T00:00:00Z"},{"InstanceId":"i-44444444444444444","InstanceType":"c7i.large","LaunchTime":"2030-01-02T00:00:00Z"}]}]}'
'@
    $windows = @'
@echo off
echo {"Reservations":[{"Instances":[{"InstanceId":"i-11111111111111111","InstanceType":"c7i.large","LaunchTime":"2030-01-02T00:00:00Z"},{"InstanceId":"i-22222222222222222","InstanceType":"c7i.large","LaunchTime":"2030-01-02T00:00:00Z"},{"InstanceId":"i-33333333333333333","InstanceType":"m7i.large","LaunchTime":"2030-01-02T00:00:00Z"},{"InstanceId":"i-44444444444444444","InstanceType":"c7i.large","LaunchTime":"2030-01-02T00:00:00Z"}]}]}
exit /b 0
'@
    $path = Join-Path $Directory 'aws'
    if ([OperatingSystem]::IsWindows()) {
        [IO.File]::WriteAllText($path + '.cmd', $windows, $utf8)
        return $Directory
    }
    [IO.File]::WriteAllText($path, ($unix -replace "`r`n", "`n" -replace "`r", "`n"), $utf8)
    [IO.File]::SetUnixFileMode($path, [IO.UnixFileMode]::UserRead -bor [IO.UnixFileMode]::UserWrite -bor [IO.UnixFileMode]::UserExecute -bor [IO.UnixFileMode]::GroupRead -bor [IO.UnixFileMode]::GroupExecute -bor [IO.UnixFileMode]::OtherRead -bor [IO.UnixFileMode]::OtherExecute)
    return $Directory
}
function Assert-BackfillNegative($Result, [string]$Pattern, [string]$Name) {
    if ($Result.ExitCode -eq 0 -or $Result.Text -notmatch $Pattern) { throw "Backfill negative '$Name' failed or returned the wrong guard: $($Result.Text)" }
}
function Copy-BaseLedger([string]$Name) {
    $path = Join-Path $temp "$Name-ledger.json"
    Copy-Item -LiteralPath (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Destination $path
    return $path
}

try {
    $committedLedger = Get-Content -LiteralPath (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Raw | ConvertFrom-Json -DateKind String
    $cycle3bRows = @($committedLedger.roles.PSObject.Properties | ForEach-Object { $_.Value } | ForEach-Object { $_ } | Where-Object { $_.cycle_id -eq 'cycle-3b-2330417' })
    if ($cycle3bRows.Count -ne 4) { throw "Committed cycle-3b ledger must have four intervals; found $($cycle3bRows.Count)." }
    foreach ($row in $cycle3bRows) {
        foreach ($pair in @(
            [pscustomobject]@{ path=[string]$row.evidence.apply_record_path; hash=[string]$row.evidence.apply_record_sha256 },
            [pscustomobject]@{ path=[string]$row.evidence.destroy_record_path; hash=[string]$row.evidence.destroy_record_sha256 },
            [pscustomobject]@{ path=[string]$row.evidence.terraform_outputs_path; hash=[string]$row.evidence.terraform_outputs_sha256 }
        )) {
            if ([IO.Path]::IsPathRooted($pair.path)) { throw "Committed cycle-3b evidence path is not relative: $($pair.path)" }
            $sourcePath = [IO.Path]::Combine($repoRoot, $pair.path.Replace('/', [IO.Path]::DirectorySeparatorChar))
            if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf) -or -not (Test-RecordedEvidenceHash $sourcePath $pair.hash)) {
                throw "Committed cycle-3b evidence path/hash does not resolve: $($pair.path)"
            }
        }
    }
    Write-Host 'PASS: all four committed cycle-3b rows use repository-relative evidence paths whose source hashes still match.'

    $outputsPath = Join-Path $temp 'terraform-outputs.json'
    $outputs = [ordered]@{
        app_instance_ids = @{ value=@('i-11111111111111111','i-22222222222222222') }
        dependency_instance_id = @{ value='i-33333333333333333' }
        load_generator_instance_ids = @{ value=@('i-44444444444444444') }
    }
    Write-Json $outputsPath $outputs
    $applyPath = Join-Path $temp 'apply-record.json'
    $destroyPath = Join-Path $temp 'destroy-record.json'
    Write-Json $applyPath ([ordered]@{ schema='dur050-apply-record.v1'; status='PASS'; cycle_id='cycle-r187-test'; apply_started_at_utc='2026-09-28T04:46:54.1291096Z' })
    Write-Json $destroyPath ([ordered]@{ schema='dur050-destroy-record.v1'; status='PASS'; cycle_id='cycle-r187-test'; destroy_completed_at_utc='2026-09-28T04:55:45.4572760Z' })

    $positiveLedger = Copy-BaseLedger 'positive'
    $positiveSnapshot = Add-SentinelRows $positiveLedger
    $positive = Invoke-Backfill $sourceScript 'positive' $positiveLedger $outputsPath $applyPath $destroyPath
    if ($positive.ExitCode -ne 0) { throw "Backfill positive control failed: $($positive.Text)" }
    Assert-SentinelRowsByteIdentical $positiveLedger $positiveSnapshot 'Backfill'
    $ledger = Get-Content -LiteralPath $positiveLedger -Raw | ConvertFrom-Json -DateKind String
    $rows = @($ledger.roles.PSObject.Properties | ForEach-Object { $_.Value } | ForEach-Object { $_ } | Where-Object cycle_id -eq 'cycle-r187-test')
    if ($rows.Count -ne 4 -or @($rows | Where-Object { $null -eq $_.destroy_completed_at_utc -or $_.interval_source -cne 'backfill: apply start -> destroy completion (R187)' }).Count -ne 0 -or
        @($rows | Where-Object { $_.evidence.apply_record_sha256 -ne (Get-FileHash $applyPath -Algorithm SHA256).Hash.ToUpperInvariant() -or $_.evidence.destroy_record_sha256 -ne (Get-FileHash $destroyPath -Algorithm SHA256).Hash.ToUpperInvariant() -or $_.evidence.terraform_outputs_sha256 -ne (Get-FileHash $outputsPath -Algorithm SHA256).Hash.ToUpperInvariant() }).Count -ne 0) {
        throw 'Backfill did not write four closed, source-labelled intervals with all evidence hashes.'
    }
    if (@($rows | Where-Object { [IO.Path]::IsPathRooted([string]$_.evidence.apply_record_path) -or [IO.Path]::IsPathRooted([string]$_.evidence.destroy_record_path) -or [IO.Path]::IsPathRooted([string]$_.evidence.terraform_outputs_path) }).Count -ne 0) {
        throw 'Backfill evidence paths must be relative to the repository root.'
    }
    Write-Host 'PASS: backfill records four closed intervals with IDs, types, exact timestamps, and source-file SHA-256 values.'

    $awsStubDir = Join-Path $temp 'aws-stub'
    New-Item -ItemType Directory -Path $awsStubDir | Out-Null
    $null = Write-LedgerAwsStub $awsStubDir
    $openCloseLedger = Copy-BaseLedger 'open-close'
    $openCloseSnapshot = Add-SentinelRows $openCloseLedger
    $openCycle = 'cycle-r192-open-close'
    $openArgs = @('-Open','-CycleID',$openCycle,'-TerraformOutputsPath',$outputsPath,'-ApplyStartedAtUtc','2030-01-01T00:00:00.0000000Z','-LedgerPath',$openCloseLedger)
    $openResult = Invoke-LedgerOperation $sourceScript $openArgs $awsStubDir
    if ($openResult.ExitCode -ne 0) { throw "Ledger open timestamp-preservation test failed: $($openResult.Text)" }
    Assert-SentinelRowsByteIdentical $openCloseLedger $openCloseSnapshot 'Open'
    $closeArgs = @('-Close','-CycleID',$openCycle,'-DestroyCompletedAtUtc','2030-01-03T00:00:00.0000000Z','-LedgerPath',$openCloseLedger)
    $closeResult = Invoke-LedgerOperation $sourceScript $closeArgs $null
    if ($closeResult.ExitCode -ne 0) { throw "Ledger close timestamp-preservation test failed: $($closeResult.Text)" }
    Assert-SentinelRowsByteIdentical $openCloseLedger $openCloseSnapshot 'Close'
    Write-Host 'PASS: ledger open and close preserve pre-existing timestamp rows byte-for-byte under TZ=America/Los_Angeles.'

    $applyFailPath = Join-Path $temp 'apply-fail-record.json'
    Write-Json $applyFailPath ([ordered]@{ schema='dur050-apply-record.v1'; status='FAIL'; cycle_id='cycle-r187-test'; apply_started_at_utc='2026-09-28T04:46:54.1291096Z' })
    $applyFail = Invoke-Backfill $sourceScript 'apply-fail' (Copy-BaseLedger 'apply-fail') $outputsPath $applyFailPath $destroyPath
    Assert-BackfillNegative $applyFail 'Apply record schema, status, or cycle ID' 'FAIL apply record'
    Write-Host 'PASS: an apply record with status FAIL is rejected.'

    $applyWrongCyclePath = Join-Path $temp 'apply-wrong-cycle-record.json'
    Write-Json $applyWrongCyclePath ([ordered]@{ schema='dur050-apply-record.v1'; status='PASS'; cycle_id='cycle-other-record'; apply_started_at_utc='2026-09-28T04:46:54.1291096Z' })
    $applyWrongCycle = Invoke-Backfill $sourceScript 'apply-wrong-cycle' (Copy-BaseLedger 'apply-wrong-cycle') $outputsPath $applyWrongCyclePath $destroyPath
    Assert-BackfillNegative $applyWrongCycle 'Apply record schema, status, or cycle ID' 'wrong apply record cycle'
    Write-Host 'PASS: an apply record from a different cycle is rejected.'

    $destroyFailPath = Join-Path $temp 'destroy-fail-record.json'
    Write-Json $destroyFailPath ([ordered]@{ schema='dur050-destroy-record.v1'; status='FAIL'; cycle_id='cycle-r187-test'; destroy_completed_at_utc='2026-09-28T04:55:45.4572760Z' })
    $destroyFail = Invoke-Backfill $sourceScript 'destroy-fail' (Copy-BaseLedger 'destroy-fail') $outputsPath $applyPath $destroyFailPath
    Assert-BackfillNegative $destroyFail 'Destroy record schema, status, or cycle ID' 'FAIL destroy record'
    Write-Host 'PASS: a destroy record with status FAIL is rejected.'

    $reusedOutputsPath = Join-Path $temp 'reused-outputs.json'
    $reused = $outputs | ConvertTo-Json -Depth 20 | ConvertFrom-Json
    $existingLedger = Get-Content (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Raw | ConvertFrom-Json
    $reused.app_instance_ids.value[0] = [string]$existingLedger.roles.'app-1'[0].instance_id
    Write-Json $reusedOutputsPath $reused
    $reusedLedger = Copy-BaseLedger 'reused'
    $reusedResult = Invoke-Backfill $sourceScript 'reused-id' $reusedLedger $reusedOutputsPath $applyPath $destroyPath
    Assert-BackfillNegative $reusedResult 'Refusing reused EC2 instance ID' 'reused instance ID'
    Write-Host 'PASS: a reused instance ID is rejected before ledger replacement.'

    $reversedDestroyPath = Join-Path $temp 'reversed-destroy.json'
    Write-Json $reversedDestroyPath ([ordered]@{ schema='dur050-destroy-record.v1'; status='PASS'; cycle_id='cycle-r187-test'; destroy_completed_at_utc='2026-09-27T04:00:00Z' })
    $reversedLedger = Copy-BaseLedger 'reversed'
    $reversedResult = Invoke-Backfill $sourceScript 'reversed' $reversedLedger $outputsPath $applyPath $reversedDestroyPath
    Assert-BackfillNegative $reversedResult 'Backfill apply start must precede destroy completion' 'reversed timestamps'
    Write-Host 'PASS: reversed apply/destroy timestamps are rejected before ledger replacement.'

    $existingLedgerPath = Copy-BaseLedger 'existing-cycle'
    $existingCycle = 'cycle-1-aa7de7e'
    $existingApplyPath = Join-Path $temp 'existing-cycle-apply.json'
    $existingDestroyPath = Join-Path $temp 'existing-cycle-destroy.json'
    Write-Json $existingApplyPath ([ordered]@{ schema='dur050-apply-record.v1'; status='PASS'; cycle_id=$existingCycle; apply_started_at_utc='2026-09-28T04:46:54.1291096Z' })
    Write-Json $existingDestroyPath ([ordered]@{ schema='dur050-destroy-record.v1'; status='PASS'; cycle_id=$existingCycle; destroy_completed_at_utc='2026-09-28T04:55:45.4572760Z' })
    $existingResult = Invoke-Backfill $sourceScript 'existing-cycle' $existingLedgerPath $outputsPath $existingApplyPath $existingDestroyPath $existingCycle
    Assert-BackfillNegative $existingResult 'Refusing to overwrite existing ledger cycle' 'existing cycle'
    Write-Host 'PASS: an existing cycle is rejected before ledger replacement.'

    $source = Get-Content -LiteralPath $sourceScript -Raw
    $mutants = @(
        [pscustomobject]@{ name='reused-id'; pattern='if \(\$allIds -contains \$hostSpec\.id\) \{ throw "Refusing reused EC2 instance ID[^\r\n]+'; replacement='# removed R187 reused-ID guard'; outputs=$reusedOutputsPath; destroy=$destroyPath; expected='Refusing reused EC2 instance ID' },
        [pscustomobject]@{ name='reversed-time'; pattern='if \(\$start -ge \$end\) \{ throw ''Backfill apply start must precede destroy completion\.'' \}'; replacement='# removed R187 timestamp-order guard'; outputs=$outputsPath; destroy=$reversedDestroyPath; expected='Backfill apply start must precede destroy completion' },
        [pscustomobject]@{ name='existing-cycle'; pattern='if \(@\(\$ledger\.roles\.\$role \| Where-Object \{ \[string\]\$_.cycle_id -eq \$CycleID \}\)\.Count -ne 0\) \{ throw "Refusing to overwrite existing ledger cycle[^\r\n]+'; replacement='# removed R187 existing-cycle guard'; outputs=$outputsPath; apply=$existingApplyPath; destroy=$existingDestroyPath; cycle=$existingCycle; expected='Refusing to overwrite existing ledger cycle'; expected_mutant_success=$true },
        [pscustomobject]@{ name='apply-status'; pattern=" -or \[string\]\`$applyRecord\.status -ne 'PASS'"; replacement=''; outputs=$outputsPath; apply=$applyFailPath; destroy=$destroyPath; expected='Apply record schema, status, or cycle ID'; expected_mutant_success=$true },
        [pscustomobject]@{ name='apply-cycle'; pattern=' -or \[string\]\$applyRecord\.cycle_id -ne \$CycleID'; replacement=''; outputs=$outputsPath; apply=$applyWrongCyclePath; destroy=$destroyPath; expected='Apply record schema, status, or cycle ID'; expected_mutant_success=$true },
        [pscustomobject]@{ name='destroy-status'; pattern=" -or \[string\]\`$destroyRecord\.status -ne 'PASS'"; replacement=''; outputs=$outputsPath; apply=$applyPath; destroy=$destroyFailPath; expected='Destroy record schema, status, or cycle ID'; expected_mutant_success=$true }
    )
    foreach ($mutant in $mutants) {
        $mutated = [regex]::Replace($source, $mutant.pattern, $mutant.replacement, 1)
        if ($mutated -eq $source) { throw "Could not construct $($mutant.name) backfill mutant." }
        $mutantPath = Join-Path $temp ("dur050-ledger-record-$($mutant.name)-mutant.ps1")
        [IO.File]::WriteAllText($mutantPath, $mutated, $utf8)
        $mutantLedger = Copy-BaseLedger "mutant-$($mutant.name)"
        $mutantApply = if ($mutant.apply) { $mutant.apply } else { $applyPath }
        $mutantCycle = if ($mutant.cycle) { $mutant.cycle } else { 'cycle-r187-test' }
        $mutantResult = Invoke-Backfill $mutantPath $mutant.name $mutantLedger $mutant.outputs $mutantApply $mutant.destroy $mutantCycle
        if ($mutant.expected_mutant_success) {
            if ($mutantResult.ExitCode -ne 0) { throw "Removing the $($mutant.name) guard did not make its negative control pass: $($mutantResult.Text)" }
        } elseif ($mutantResult.ExitCode -eq 0 -or $mutantResult.Text -match [regex]::Escape($mutant.expected)) {
            throw "The test did not detect removal of the $($mutant.name) guard: $($mutantResult.Text)"
        }
        Write-Host "PASS: the $($mutant.name) guard-removal mutant is detected by the targeted diagnostic assertion."
    }
} finally {
    Remove-Item Env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE -ErrorAction SilentlyContinue
    if ($null -eq $priorTz) { Remove-Item Env:TZ -ErrorAction SilentlyContinue } else { $env:TZ = $priorTz }
    Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue
}
