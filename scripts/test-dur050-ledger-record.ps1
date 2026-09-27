#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$sourceScript = Join-Path $PSScriptRoot 'dur050-ledger-record.ps1'
$pwsh = [Environment]::ProcessPath
$temp = Join-Path $repoRoot ('.dur050-ledger-backfill-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
$utf8 = [Text.UTF8Encoding]::new($false)
$env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE = '1'

function Write-Json([string]$Path, $Value) { [IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth 30) + "`n"), $utf8) }
function Invoke-Backfill([string]$Script, [string]$Name, [string]$Ledger, [string]$Outputs, [string]$Apply, [string]$Destroy, [string]$Cycle = 'cycle-r187-test') {
    $args = @('-NoProfile','-File',$Script,'-Backfill','-CycleID',$Cycle,'-TerraformOutputsPath',$Outputs,'-ApplyRecordPath',$Apply,'-DestroyRecordPath',$Destroy,'-LedgerPath',$Ledger)
    $result = & $pwsh @args 2>&1 | Out-String
    return [pscustomobject]@{ ExitCode=$LASTEXITCODE; Text=$result; Name=$Name }
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
    $committedLedger = Get-Content -LiteralPath (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Raw | ConvertFrom-Json
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
            if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf) -or (Get-FileHash -LiteralPath $sourcePath -Algorithm SHA256).Hash.ToUpperInvariant() -cne $pair.hash) {
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
    $positive = Invoke-Backfill $sourceScript 'positive' $positiveLedger $outputsPath $applyPath $destroyPath
    if ($positive.ExitCode -ne 0) { throw "Backfill positive control failed: $($positive.Text)" }
    $ledger = Get-Content -LiteralPath $positiveLedger -Raw | ConvertFrom-Json
    $rows = @($ledger.roles.PSObject.Properties | ForEach-Object { $_.Value } | ForEach-Object { $_ } | Where-Object cycle_id -eq 'cycle-r187-test')
    if ($rows.Count -ne 4 -or @($rows | Where-Object { $null -eq $_.destroy_completed_at_utc -or $_.interval_source -cne 'backfill: apply start -> destroy completion (R187)' }).Count -ne 0 -or
        @($rows | Where-Object { $_.evidence.apply_record_sha256 -ne (Get-FileHash $applyPath -Algorithm SHA256).Hash.ToUpperInvariant() -or $_.evidence.destroy_record_sha256 -ne (Get-FileHash $destroyPath -Algorithm SHA256).Hash.ToUpperInvariant() -or $_.evidence.terraform_outputs_sha256 -ne (Get-FileHash $outputsPath -Algorithm SHA256).Hash.ToUpperInvariant() }).Count -ne 0) {
        throw 'Backfill did not write four closed, source-labelled intervals with all evidence hashes.'
    }
    if (@($rows | Where-Object { [IO.Path]::IsPathRooted([string]$_.evidence.apply_record_path) -or [IO.Path]::IsPathRooted([string]$_.evidence.destroy_record_path) -or [IO.Path]::IsPathRooted([string]$_.evidence.terraform_outputs_path) }).Count -ne 0) {
        throw 'Backfill evidence paths must be relative to the repository root.'
    }
    Write-Host 'PASS: backfill records four closed intervals with IDs, types, exact timestamps, and source-file SHA-256 values.'

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
    Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue
}
