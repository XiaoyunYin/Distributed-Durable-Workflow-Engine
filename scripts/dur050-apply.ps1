#Requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9-]{1,32}$')][string]$CycleID,
    [Parameter(Mandatory)][string]$CampaignManifestPath,
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

function ConvertTo-CanonicalJson([object]$Value) {
    return ConvertTo-Json -InputObject $Value -Depth 100 -Compress
}

function Copy-JsonObject([object]$Value) {
    return (ConvertTo-CanonicalJson $Value) | ConvertFrom-Json -AsHashtable -Depth 100 -ErrorAction Stop
}

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
$manifestBefore = Get-Content -LiteralPath $manifestFullPath -Raw | ConvertFrom-Json -AsHashtable -Depth 100 -ErrorAction Stop
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
$beforeCanonical = ConvertTo-CanonicalJson $manifestBefore
$manifestAfter = Copy-JsonObject $manifestBefore
$originalReviewState = [string]$pin.review_state
$manifestAfter['approved_saved_plan']['review_state'] = 'APPLIED_VALIDATION_PLACEHOLDER'
$normalizedAfter = Copy-JsonObject $manifestAfter
$normalizedAfter['approved_saved_plan']['review_state'] = $originalReviewState
if ((ConvertTo-CanonicalJson $normalizedAfter) -cne $beforeCanonical) { throw 'Structured manifest edit changed fields other than approved_saved_plan.review_state.' }
$startedAt = [DateTimeOffset]::UtcNow
$startedAtText = Format-UtcStamp $startedAt
$reviewStateAfter = 'APPLIED_' + $startedAtText
$manifestAfter['approved_saved_plan']['review_state'] = $reviewStateAfter
if (-not (Test-Path -LiteralPath $cycleDirectory)) { New-Item -ItemType Directory -Path $cycleDirectory -Force | Out-Null }
$manifestTemp = Join-Path (Split-Path -Parent $manifestFullPath) ('.cost-manifest.' + [guid]::NewGuid().ToString('N') + '.tmp')
$markerApplied = $false
$record = $null
$failure = $null
$outputsWritten = $false
try {
    [IO.File]::WriteAllText($manifestTemp, ((ConvertTo-Json -InputObject $manifestAfter -Depth 100) + "`n"), [Text.UTF8Encoding]::new($false))
    [IO.File]::Move($manifestTemp, $manifestFullPath, $true)
    $markerApplied = $true
    $manifestReadBack = Get-Content -LiteralPath $manifestFullPath -Raw | ConvertFrom-Json -AsHashtable -Depth 100 -ErrorAction Stop
    if ((ConvertTo-CanonicalJson $manifestReadBack) -cne (ConvertTo-CanonicalJson $manifestAfter)) { throw 'Atomic manifest write did not round-trip to the intended structured value.' }
    $normalizedReadBack = Copy-JsonObject $manifestReadBack
    $normalizedReadBack['approved_saved_plan']['review_state'] = [string]$pin.review_state
    if ((ConvertTo-CanonicalJson $normalizedReadBack) -cne $beforeCanonical) { throw 'Manifest verification found changes outside approved_saved_plan.review_state.' }

    $record = [ordered]@{
        schema = 'dur050-apply-record.v1'
        status = 'RUNNING'
        cycle_id = $CycleID
        started_at_utc = $startedAtText
        completed_at_utc = $null
        aws_identity = [ordered]@{ account_id = [string]$identity.Account; arn = [string]$identity.Arn }
        saved_plan_path = [IO.Path]::GetRelativePath($repoRoot, $savedPlanPath).Replace('\','/')
        saved_plan_sha256 = $planHash
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
