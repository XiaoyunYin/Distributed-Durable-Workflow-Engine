#Requires -Version 7.5
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9-]{1,32}$')][string]$CycleID,
    [Parameter(Mandatory)][string]$CampaignManifestPath,
    [Parameter(Mandatory)][string]$PreApplyGatePath,
    [string]$TerraformDirectory,
    [string]$TerraformStatePath
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
Set-StrictMode -Version Latest
if ([string]$PSVersionTable.PSEdition -cne 'Core') { throw 'DUR-050 apply requires PowerShell Core 7 or later.' }

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$manifestFullPath = [IO.Path]::GetFullPath($CampaignManifestPath)
if (-not (Test-Path -LiteralPath $manifestFullPath -PathType Leaf)) { throw "DUR-050 campaign manifest is missing: $manifestFullPath" }
if (-not $TerraformDirectory) { $TerraformDirectory = Join-Path $repoRoot 'deploy/aws' }
elseif ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'TerraformDirectory override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
if (-not $TerraformStatePath) { $TerraformStatePath = Join-Path $TerraformDirectory 'terraform.tfstate' }
elseif ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'TerraformStatePath override is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
$terraformDirectoryFull = [IO.Path]::GetFullPath($TerraformDirectory)
$terraformStateFullPath = [IO.Path]::GetFullPath($TerraformStatePath)
if (-not (Test-Path -LiteralPath $terraformDirectoryFull -PathType Container)) { throw "Terraform directory is missing: $terraformDirectoryFull" }
if (-not (Test-Path -LiteralPath $terraformStateFullPath -PathType Leaf)) { throw "Terraform state file is missing: $terraformStateFullPath" }

Import-Module (Join-Path $PSScriptRoot 'dur050-plan-state.psm1') -Force

function Write-JsonAtomic([string]$Path, [object]$Value, [switch]$CreateOnly) {
    $parent = Split-Path -Parent $Path
    if (-not (Test-Path -LiteralPath $parent -PathType Container)) { throw "Evidence parent directory is missing: $parent" }
    if ($CreateOnly -and (Test-Path -LiteralPath $Path)) { throw "Refusing to overwrite evidence: $Path" }
    $temp = Join-Path $parent ('.' + [IO.Path]::GetFileName($Path) + '.' + [guid]::NewGuid().ToString('N') + '.tmp')
    try {
        $json = (ConvertTo-Json -InputObject $Value -Depth 100) + "`n"
        [IO.File]::WriteAllText($temp, $json, [Text.UTF8Encoding]::new($false))
        if ($CreateOnly) { [IO.File]::Move($temp, $Path) }
        else { [IO.File]::Move($temp, $Path, $true) }
    } finally {
        if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Force -ErrorAction SilentlyContinue }
    }
}

function Find-ByteSequence([byte[]]$Bytes, [byte[]]$Needle) {
    $found = -1
    for ($offset = 0; $offset -le ($Bytes.Length - $Needle.Length); $offset++) {
        $sequenceMatches = $true
        for ($index = 0; $index -lt $Needle.Length; $index++) {
            if ($Bytes[$offset + $index] -ne $Needle[$index]) { $sequenceMatches = $false; break }
        }
        if ($sequenceMatches) {
            if ($found -ge 0) { throw 'Manifest review_state marker occurs more than once.' }
            $found = $offset
        }
    }
    return $found
}

function Test-ByteArraysEqual([byte[]]$Left, [byte[]]$Right) {
    if ($Left.Length -ne $Right.Length) { return $false }
    for ($index = 0; $index -lt $Left.Length; $index++) {
        if ($Left[$index] -ne $Right[$index]) { return $false }
    }
    return $true
}

function Get-ApprovedPlanObjectSpan([string]$JsonText) {
    # The manifest also keeps historical per-cycle objects with this key; the
    # two-space indentation identifies the one top-level approved pin object.
    $propertyMatches = [regex]::Matches($JsonText, '(?m)^  "approved_saved_plan"\s*:')
    if ($propertyMatches.Count -ne 1) { throw 'Manifest must contain exactly one top-level approved_saved_plan property.' }
    $colon = $JsonText.IndexOf(':', $propertyMatches[0].Index + $propertyMatches[0].Length - 1)
    $open = $colon + 1
    while ($open -lt $JsonText.Length -and [char]::IsWhiteSpace($JsonText[$open])) { $open++ }
    if ($open -ge $JsonText.Length -or $JsonText[$open] -ne '{') { throw 'approved_saved_plan must be a JSON object.' }
    $depth = 0
    $inString = $false
    $escaped = $false
    for ($index = $open; $index -lt $JsonText.Length; $index++) {
        $character = $JsonText[$index]
        if ($inString) {
            if ($escaped) { $escaped = $false; continue }
            if ($character -eq '\') { $escaped = $true; continue }
            if ($character -eq '"') { $inString = $false }
            continue
        }
        if ($character -eq '"') { $inString = $true; continue }
        if ($character -eq '{') { $depth++ }
        elseif ($character -eq '}') {
            $depth--
            if ($depth -eq 0) { return [pscustomobject]@{ Start=$open; End=$index + 1 } }
        }
    }
    throw 'approved_saved_plan object is not terminated.'
}

function New-ReviewStateManifestBytes([byte[]]$OriginalBytes, [string]$ReviewState) {
    $strictUtf8 = [Text.UTF8Encoding]::new($false, $true)
    $jsonText = $strictUtf8.GetString($OriginalBytes)
    $oldLiteral = '"review_state": "PENDING_CLAUDE_GO"'
    $newLiteral = '"review_state": "' + $ReviewState + '"'
    $oldBytes = [Text.Encoding]::ASCII.GetBytes($oldLiteral)
    $newBytes = [Text.Encoding]::ASCII.GetBytes($newLiteral)
    $literalOffset = Find-ByteSequence $OriginalBytes $oldBytes
    if ($literalOffset -lt 0) { throw 'Manifest must contain the exact literal "review_state": "PENDING_CLAUDE_GO" exactly once.' }
    $span = Get-ApprovedPlanObjectSpan $jsonText
    $objectStartByte = $strictUtf8.GetByteCount($jsonText.Substring(0, $span.Start))
    $objectEndByte = $strictUtf8.GetByteCount($jsonText.Substring(0, $span.End))
    if ($literalOffset -lt $objectStartByte -or ($literalOffset + $oldBytes.Length) -gt $objectEndByte) {
        throw 'The exact PENDING_CLAUDE_GO literal is not inside approved_saved_plan.'
    }

    $output = [byte[]]::new($OriginalBytes.Length - $oldBytes.Length + $newBytes.Length)
    [Array]::Copy($OriginalBytes, 0, $output, 0, $literalOffset)
    [Array]::Copy($newBytes, 0, $output, $literalOffset, $newBytes.Length)
    [Array]::Copy($OriginalBytes, $literalOffset + $oldBytes.Length, $output, $literalOffset + $newBytes.Length, $OriginalBytes.Length - $literalOffset - $oldBytes.Length)

    $reverseOffset = Find-ByteSequence $output $newBytes
    if ($reverseOffset -ne $literalOffset) { throw 'Review-state byte substitution could not be located uniquely for reversal.' }
    $reversed = [byte[]]::new($output.Length - $newBytes.Length + $oldBytes.Length)
    [Array]::Copy($output, 0, $reversed, 0, $reverseOffset)
    [Array]::Copy($oldBytes, 0, $reversed, $reverseOffset, $oldBytes.Length)
    [Array]::Copy($output, $reverseOffset + $newBytes.Length, $reversed, $reverseOffset + $oldBytes.Length, $output.Length - $reverseOffset - $newBytes.Length)
    if (-not (Test-ByteArraysEqual $reversed $OriginalBytes)) { throw 'Reversing the review-state substitution did not reproduce the original manifest bytes.' }

    $parsed = $strictUtf8.GetString($output) | ConvertFrom-Json -AsHashtable -DateKind String -Depth 100 -ErrorAction Stop
    if ([string]$parsed.approved_saved_plan.review_state -cne $ReviewState) { throw 'Byte-edited manifest did not parse with the expected review_state.' }
    return ,$output
}

function Test-ManifestContainsCycle([object]$Value, [string]$Candidate) {
    if ($null -eq $Value -or $Value -is [string]) { return $false }
    if ($Value -is [System.Collections.IDictionary]) {
        if ($Value.Contains('cycle_id') -and [string]$Value['cycle_id'] -ceq $Candidate) { return $true }
        foreach ($key in $Value.Keys) {
            if (Test-ManifestContainsCycle $Value[$key] $Candidate) { return $true }
        }
        return $false
    }
    if ($Value -is [System.Collections.IEnumerable]) {
        foreach ($item in $Value) {
            if (Test-ManifestContainsCycle $item $Candidate) { return $true }
        }
    }
    return $false
}

function Get-FileSha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToUpperInvariant()
}

function Get-GitResult([string[]]$Arguments) {
    Push-Location $repoRoot
    try {
        $text = & git @Arguments 2>&1 | Out-String
        $code = $LASTEXITCODE
        return [pscustomobject]@{ exit_code = $code; output = $text }
    } finally { Pop-Location }
}

function Format-UtcStamp([DateTimeOffset]$Value) {
    return $Value.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", [Globalization.CultureInfo]::InvariantCulture)
}

function Get-TeardownCommand([string]$Cycle, [string]$Manifest, [string]$Output, [string]$Terraform) {
    $pwsh = [Environment]::ProcessPath
    $tool = Join-Path $repoRoot 'scripts/dur050-destroy.ps1'
    return "& '$pwsh' -NoProfile -File '$tool' -CycleID '$Cycle' -CampaignManifestPath '$Manifest' -OutputPath '$Output' -TerraformExe '$Terraform'"
}

# All checks through the executable version query are read-only. No manifest,
# cycle directory, or Terraform state is changed before every precheck passes.
$manifestOriginalBytes = [IO.File]::ReadAllBytes($manifestFullPath)
$manifestStrictUtf8 = [Text.UTF8Encoding]::new($false, $true)
$manifestBefore = $manifestStrictUtf8.GetString($manifestOriginalBytes) | ConvertFrom-Json -AsHashtable -DateKind String -Depth 100 -ErrorAction Stop
if ([string]$manifestBefore.schema -cne 'dur050-cost-manifest.v2') { throw 'Campaign manifest schema is not dur050-cost-manifest.v2.' }
$pin = $manifestBefore.approved_saved_plan
if ($null -eq $pin) { throw 'Campaign manifest lacks approved_saved_plan.' }
if ([string]$pin.review_state -cne 'PENDING_CLAUDE_GO') { throw "review_state must be exactly PENDING_CLAUDE_GO; observed '$($pin.review_state)'" }
if ([string]$pin.source_commit -notmatch '^[0-9a-f]{40}$') { throw 'approved_saved_plan.source_commit must be a full commit SHA.' }
if ([string]$pin.saved_plan_sha256 -notmatch '^[A-Fa-f0-9]{64}$') { throw 'approved_saved_plan.saved_plan_sha256 is malformed.' }
if ([string]$pin.expected_plan_prior_state_lineage -notmatch '^[0-9a-f-]{36}$' -or $null -eq $pin.expected_plan_prior_state_serial) { throw 'approved_saved_plan expected prior-state pins are missing or malformed.' }
if ([string]$pin.terraform_executable_path -notmatch '^(?:[A-Za-z]:[\\/]|/)' -or
    [string]$pin.terraform_executable_sha256 -notmatch '^[A-Fa-f0-9]{64}$' -or
    [string]$pin.terraform_version -cne '1.16.4') { throw 'approved_saved_plan Terraform executable pins are missing or malformed.' }

if (Test-ManifestContainsCycle $manifestBefore $CycleID) { throw "Cycle ID already exists in the campaign manifest: $CycleID" }
$campaignRoot = Split-Path -Parent $manifestFullPath
$cycleDirectory = Join-Path (Join-Path $campaignRoot 'cycles') $CycleID
$applyRecordPath = Join-Path $cycleDirectory 'apply-record.json'
$outputsPath = Join-Path $cycleDirectory 'terraform-outputs.json'
if (Test-Path -LiteralPath $cycleDirectory) {
    $cycleEntries = @(Get-ChildItem -LiteralPath $cycleDirectory -Force)
    if ($cycleEntries.Count -ne 0) { throw "Cycle directory is not empty; refusing reuse: $cycleDirectory" }
}
if (Test-Path -LiteralPath $applyRecordPath) { throw "Apply record already exists: $applyRecordPath" }
if (Test-Path -LiteralPath $outputsPath) { throw "Terraform outputs already exist: $outputsPath" }

$identityRaw = & aws sts get-caller-identity --output json 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) { throw "AWS caller identity check failed: $identityRaw" }
try { $identity = $identityRaw | ConvertFrom-Json -ErrorAction Stop } catch { throw 'AWS caller identity response was not valid JSON.' }
if ([string]$identity.Account -cne '372206265946') { throw "AWS account must be 372206265946; observed '$($identity.Account)'" }

$planPathText = [string]$pin.saved_plan_path
if ([IO.Path]::IsPathFullyQualified($planPathText)) { $savedPlanPath = [IO.Path]::GetFullPath($planPathText) }
else { $savedPlanPath = [IO.Path]::GetFullPath((Join-Path $repoRoot $planPathText)) }
if (-not (Test-Path -LiteralPath $savedPlanPath -PathType Leaf)) { throw "Approved saved plan is missing: $savedPlanPath" }
$planHash = Get-FileSha256 $savedPlanPath
if ($planHash -cne ([string]$pin.saved_plan_sha256).ToUpperInvariant()) { throw "Saved plan SHA-256 mismatch: observed $planHash." }

$preApplyGateFullPath = [IO.Path]::GetFullPath($PreApplyGatePath)
if (-not (Test-Path -LiteralPath $preApplyGateFullPath -PathType Leaf)) { throw "Passing pre-apply gate record is missing: $preApplyGateFullPath" }
$preApplyGateBytes = [IO.File]::ReadAllBytes($preApplyGateFullPath)
$preApplyGateHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($preApplyGateBytes))
try { $preApplyGate = ([Text.UTF8Encoding]::new($false, $true).GetString($preApplyGateBytes)) | ConvertFrom-Json -DateKind String -ErrorAction Stop }
catch { throw "Pre-apply gate record is not valid JSON: $($_.Exception.Message)" }
if ([string]$preApplyGate.schema -cne 'dur050-pre-apply-gate.v1' -or [string]$preApplyGate.status -cne 'PASS') { throw 'Pre-apply gate record must have schema dur050-pre-apply-gate.v1 and status PASS.' }
if ([string]$preApplyGate.plan_sha256 -cne $planHash) { throw 'Pre-apply gate plan_sha256 does not match the approved saved plan.' }
$gateStampText = [string]$preApplyGate.checked_at_utc
$gateStamp = [DateTimeOffset]::MinValue
if (-not $gateStampText -or $gateStampText -notmatch '(?:Z|[+-][0-9]{2}:[0-9]{2})$' -or
    -not [DateTimeOffset]::TryParse($gateStampText, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::RoundtripKind, [ref]$gateStamp)) {
    throw 'Pre-apply gate checked_at_utc must be an RFC3339 timestamp with an explicit offset.'
}
$gateAge = [DateTimeOffset]::UtcNow - $gateStamp.ToUniversalTime()
if ($gateAge -lt [TimeSpan]::Zero -or $gateAge -gt [TimeSpan]::FromMinutes(240)) { throw 'Pre-apply gate is in the future or older than 240 minutes.' }

$terraformExeText = [string]$pin.terraform_executable_path
$terraformExe = [IO.Path]::GetFullPath($terraformExeText)
if (-not (Test-Path -LiteralPath $terraformExe -PathType Leaf)) { throw "Pinned Terraform executable is missing: $terraformExe" }
$terraformExeHash = Get-FileSha256 $terraformExe
if ($terraformExeHash -cne ([string]$pin.terraform_executable_sha256).ToUpperInvariant()) { throw "Pinned Terraform executable SHA-256 mismatch: observed $terraformExeHash." }
$terraformExeResolved = (Resolve-Path -LiteralPath $terraformExe).Path
if ([IO.Path]::GetFullPath($terraformExeResolved) -ine [IO.Path]::GetFullPath($terraformExeText)) { throw 'Resolved Terraform executable path differs from the approved_saved_plan path pin.' }

$localState = Read-Dur050LocalTerraformState -TerraformStatePath $terraformStateFullPath
$planPriorState = Read-Dur050PlanPriorState -SavedPlanPath $savedPlanPath
$stateMismatch = $null
try { [void](Assert-Dur050PlanStateMatchesLocal -PlanPriorState $planPriorState -LocalState $localState) }
catch { $stateMismatch = $_.Exception.Message }
if (-not $stateMismatch -and
    ([string]$planPriorState.lineage -cne [string]$pin.expected_plan_prior_state_lineage -or
     [long]$planPriorState.serial -ne [long]$pin.expected_plan_prior_state_serial -or
     [string]$localState.lineage -cne [string]$pin.expected_plan_prior_state_lineage -or
     [long]$localState.serial -ne [long]$pin.expected_plan_prior_state_serial)) {
    $stateMismatch = 'Saved-plan and local-state lineage/serial do not match approved_saved_plan expectations.'
}
if (-not $stateMismatch -and [int]$localState.resource_count -ne 0) { $stateMismatch = "Terraform state must contain zero resources; found $($localState.resource_count)." }
if ($stateMismatch) { throw "Terraform state precheck failed: $stateMismatch" }
if ([string]$planPriorState.terraform_version -cne [string]$pin.terraform_version) { throw 'Saved plan prior-state Terraform version differs from the approved executable version.' }
$localStateSha = Get-FileSha256 $terraformStateFullPath

$sourceCommit = [string]$pin.source_commit
$gitObject = Get-GitResult @('cat-file','-e',($sourceCommit + '^{commit}'))
if ($gitObject.exit_code -ne 0) { throw "Approved source commit is unavailable: $sourceCommit" }
$committedDiff = Get-GitResult @('diff','--quiet',$sourceCommit,'HEAD','--','deploy/aws',':!deploy/aws/README.md')
if ($committedDiff.exit_code -ne 0) { throw "Terraform-consumed deploy/aws has a committed diff from approved source $sourceCommit." }
$workingDiff = Get-GitResult @('diff','--quiet',$sourceCommit,'--','deploy/aws',':!deploy/aws/README.md')
if ($workingDiff.exit_code -ne 0) { throw "Terraform-consumed deploy/aws working tree differs from approved source $sourceCommit." }

# Version execution is intentionally last among the prechecks so all negative
# controls above can assert that no Terraform process was invoked.
$terraformVersionRaw = & $terraformExe version -json 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) { throw "Pinned Terraform version query failed: $terraformVersionRaw" }
try { $terraformVersionInfo = $terraformVersionRaw | ConvertFrom-Json -ErrorAction Stop }
catch { throw 'Pinned Terraform version response was not valid JSON.' }
if ([string]$terraformVersionInfo.terraform_version -cne [string]$pin.terraform_version) { throw "Pinned Terraform version mismatch: observed '$($terraformVersionInfo.terraform_version)'" }
# All read-only validation has passed. From here onward failures retain the
# APPLIED_ marker and produce an apply record plus the committed teardown command.
$gateAge = [DateTimeOffset]::UtcNow - $gateStamp.ToUniversalTime()
if ($gateAge -lt [TimeSpan]::Zero -or $gateAge -gt [TimeSpan]::FromMinutes(240)) { throw 'Pre-apply gate is in the future or older than 240 minutes.' }
$startedAt = [DateTimeOffset]::UtcNow
$startedAtText = Format-UtcStamp $startedAt
$reviewStateAfter = 'APPLIED_' + $startedAtText
$manifestAfterBytes = New-ReviewStateManifestBytes $manifestOriginalBytes $reviewStateAfter
if (-not (Test-Path -LiteralPath $cycleDirectory)) { New-Item -ItemType Directory -Path $cycleDirectory -Force | Out-Null }
$manifestTemp = Join-Path (Split-Path -Parent $manifestFullPath) ('.cost-manifest.' + [guid]::NewGuid().ToString('N') + '.tmp')
$markerApplied = $false
$record = $null
$failure = $null
$outputsWritten = $false
try {
    [IO.File]::WriteAllBytes($manifestTemp, $manifestAfterBytes)
    [IO.File]::Move($manifestTemp, $manifestFullPath, $true)
    $markerApplied = $true
    $manifestReadBackBytes = [IO.File]::ReadAllBytes($manifestFullPath)
    if (-not (Test-ByteArraysEqual $manifestReadBackBytes $manifestAfterBytes)) { throw 'Atomic manifest write differed from the verified byte-level substitution.' }
    $manifestReadBack = $manifestStrictUtf8.GetString($manifestReadBackBytes) | ConvertFrom-Json -AsHashtable -DateKind String -Depth 100 -ErrorAction Stop
    if ([string]$manifestReadBack.approved_saved_plan.review_state -cne $reviewStateAfter) { throw 'Atomic manifest write did not retain the intended review_state.' }

    $record = [ordered]@{
        schema = 'dur050-apply-record.v1'
        status = 'RUNNING'
        cycle_id = $CycleID
        started_at_utc = $startedAtText
        completed_at_utc = $null
        aws_identity = [ordered]@{ account_id = [string]$identity.Account; arn = [string]$identity.Arn }
        saved_plan_path = [IO.Path]::GetRelativePath($repoRoot, $savedPlanPath).Replace('\','/')
        saved_plan_sha256 = $planHash
        pre_apply_gate_path = [IO.Path]::GetRelativePath($repoRoot, $preApplyGateFullPath).Replace('\','/')
        pre_apply_gate_sha256 = $preApplyGateHash
        pre_apply_gate_checked_at_utc = $gateStampText
        plan_prior_state = [ordered]@{ lineage = [string]$planPriorState.lineage; serial = [long]$planPriorState.serial; terraform_version = [string]$planPriorState.terraform_version }
        local_state_before = [ordered]@{ lineage = [string]$localState.lineage; serial = [long]$localState.serial; resource_count = [int]$localState.resource_count; sha256 = $localStateSha }
        terraform_executable = [ordered]@{ path = [string]$terraformExeResolved; sha256 = $terraformExeHash; version = [string]$terraformVersionInfo.terraform_version }
        command = @(('-chdir=' + $terraformDirectoryFull), 'apply', '-input=false', $savedPlanPath)
        exit_code = $null
        counts = [ordered]@{ added = $null; changed = $null; destroyed = $null }
        output = ''
        post_apply_state = $null
        terraform_outputs_path = [IO.Path]::GetRelativePath($repoRoot, $outputsPath).Replace('\','/')
        terraform_outputs_sha256 = $null
        error = $null
    }
    Write-JsonAtomic $applyRecordPath $record -CreateOnly

    $applyOutput = & $terraformExeResolved ("-chdir=$terraformDirectoryFull") apply -input=false $savedPlanPath 2>&1 | Out-String
    $terraformExitCode = $LASTEXITCODE
    $record.exit_code = $terraformExitCode
    $record.output = $applyOutput.TrimEnd("`r", "`n")
    $countMatch = [regex]::Match($record.output, 'Resources:\s*(\d+)\s+added,\s*(\d+)\s+changed,\s*(\d+)\s+destroyed\b')
    if ($countMatch.Success) {
        $record.counts = [ordered]@{ added = [int]$countMatch.Groups[1].Value; changed = [int]$countMatch.Groups[2].Value; destroyed = [int]$countMatch.Groups[3].Value }
    }
    try {
        $postState = Read-Dur050LocalTerraformState -TerraformStatePath $terraformStateFullPath
        $record.post_apply_state = [ordered]@{ state_file_present = [bool]$postState.state_file_present; lineage = [string]$postState.lineage; serial = [long]$postState.serial; resource_count = [int]$postState.resource_count }
    } catch { $failure = "Could not read post-apply Terraform state: $($_.Exception.Message)" }
    if ($terraformExitCode -ne 0 -and -not $failure) { $failure = "Terraform apply exited $terraformExitCode." }
    if (-not $countMatch.Success -and -not $failure) { $failure = 'Terraform apply output did not report added/changed/destroyed counts.' }
    if ($terraformExitCode -eq 0 -and -not $failure -and
        (-not $postState.state_file_present -or [string]$postState.lineage -cne [string]$localState.lineage -or [long]$postState.serial -le [long]$localState.serial -or [int]$postState.resource_count -le 0)) {
        $failure = 'Terraform apply returned success but the post-apply state did not show the expected lineage, advanced serial, and non-empty resource set.'
    }

    if (-not $failure) {
        $outputsRaw = & $terraformExeResolved ("-chdir=$terraformDirectoryFull") output -json 2>&1 | Out-String
        $outputsExitCode = $LASTEXITCODE
        if ($outputsExitCode -ne 0) { $failure = "terraform output -json exited ${outputsExitCode}: $outputsRaw" }
        else {
            try { $null = $outputsRaw | ConvertFrom-Json -ErrorAction Stop }
            catch { $failure = 'terraform output -json did not return valid JSON.' }
            if (-not $failure) {
                $outputsTemp = Join-Path $cycleDirectory ('.terraform-outputs.' + [guid]::NewGuid().ToString('N') + '.tmp')
                try {
                    [IO.File]::WriteAllText($outputsTemp, ($outputsRaw.TrimEnd("`r", "`n") + "`n"), [Text.UTF8Encoding]::new($false))
                    [IO.File]::Move($outputsTemp, $outputsPath)
                    $outputsWritten = $true
                    $record.terraform_outputs_sha256 = Get-FileSha256 $outputsPath
                } finally { if (Test-Path -LiteralPath $outputsTemp) { Remove-Item -LiteralPath $outputsTemp -Force -ErrorAction SilentlyContinue } }
            }
        }
    }
} catch {
    if ($markerApplied -and -not $failure) { $failure = $_.Exception.Message }
} finally {
    if (Test-Path -LiteralPath $manifestTemp) { Remove-Item -LiteralPath $manifestTemp -Force -ErrorAction SilentlyContinue }
}

if ($markerApplied) {
    if ($null -eq $record) {
        $record = [ordered]@{
            schema = 'dur050-apply-record.v1'; status = 'FAIL'; cycle_id = $CycleID
            started_at_utc = $startedAtText; completed_at_utc = (Format-UtcStamp ([DateTimeOffset]::UtcNow))
            aws_identity = [ordered]@{ account_id = [string]$identity.Account; arn = [string]$identity.Arn }
            saved_plan_path = $planPathText; saved_plan_sha256 = $planHash
            plan_prior_state = [ordered]@{ lineage = [string]$planPriorState.lineage; serial = [long]$planPriorState.serial; terraform_version = [string]$planPriorState.terraform_version }
            local_state_before = [ordered]@{ lineage = [string]$localState.lineage; serial = [long]$localState.serial; resource_count = [int]$localState.resource_count; sha256 = $localStateSha }
            terraform_executable = [ordered]@{ path = $terraformExe; sha256 = $terraformExeHash; version = [string]$pin.terraform_version }
            command = @(('-chdir=' + $terraformDirectoryFull), 'apply', '-input=false', $savedPlanPath)
            exit_code = $null; counts = [ordered]@{ added = $null; changed = $null; destroyed = $null }; output = ''
            post_apply_state = $null; terraform_outputs_path = $outputsPath; terraform_outputs_sha256 = $null; error = $failure
        }
    }
    if (-not $failure -and -not $outputsWritten) { $failure = 'Apply path ended without writing terraform-outputs.json.' }
    if ($failure) {
        $record.status = 'FAIL'
        $record.error = $failure
        $record.completed_at_utc = Format-UtcStamp ([DateTimeOffset]::UtcNow)
        try {
            if (Test-Path -LiteralPath $applyRecordPath) { Write-JsonAtomic $applyRecordPath $record } else { Write-JsonAtomic $applyRecordPath $record -CreateOnly }
        } catch { Write-Output "FAIL RECORD WRITE ERROR: $($_.Exception.Message)" }
        Write-Output "FAIL: $failure"
        $destroyRecordPath = Join-Path $cycleDirectory 'destroy-record.json'
        Write-Output ('TEARDOWN COMMAND: ' + (Get-TeardownCommand $CycleID $manifestFullPath $destroyRecordPath $terraformExe))
        exit 1
    }
    $record.status = 'PASS'
    $record.error = $null
    $record.completed_at_utc = Format-UtcStamp ([DateTimeOffset]::UtcNow)
    try { Write-JsonAtomic $applyRecordPath $record }
    catch {
        $failure = "Could not finalize apply record: $($_.Exception.Message)"
        $record.status = 'FAIL'
        $record.error = $failure
        try { Write-JsonAtomic $applyRecordPath $record } catch { Write-Output "FAIL RECORD WRITE ERROR: $($_.Exception.Message)" }
        $destroyRecordPath = Join-Path $cycleDirectory 'destroy-record.json'
        Write-Output "FAIL: $failure"
        Write-Output ('TEARDOWN COMMAND: ' + (Get-TeardownCommand $CycleID $manifestFullPath $destroyRecordPath $terraformExe))
        exit 1
    }
    $record | ConvertTo-Json -Depth 30
    exit 0
}

# An exception before the marker is a clean refusal; no evidence or state is changed.
if ($failure) { throw $failure }
