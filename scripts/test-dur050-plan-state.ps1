param([switch]$FixturesOnly)
$ErrorActionPreference = 'Stop'
if (-not $IsLinux) { throw 'DUR-050 plan-state tests require Ubuntu/Linux.' }
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$repoRoot = Split-Path -Parent $PSScriptRoot
$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('dur050-plan-state-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $temp | Out-Null
$modulePath = Join-Path $PSScriptRoot 'dur050-plan-state.psm1'
Import-Module $modulePath -Force

function Write-StateZip([string]$Path, [string]$Lineage, [long]$Serial) {
    $archive = [System.IO.Compression.ZipFile]::Open($Path, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        $entry = $archive.CreateEntry('tfstate')
        $stream = $entry.Open()
        try {
            $writer = [System.IO.StreamWriter]::new($stream, [System.Text.UTF8Encoding]::new($false))
            try {
                $state = [ordered]@{ version = 4; terraform_version = '1.16.4'; serial = $Serial; lineage = $Lineage; resources = @() }
                $writer.Write(($state | ConvertTo-Json -Compress))
            } finally { $writer.Dispose() }
        } finally { $stream.Dispose() }
    } finally { $archive.Dispose() }
}

function Write-LocalState([string]$Path, [string]$Lineage, [long]$Serial) {
    $state = [ordered]@{ version = 4; terraform_version = '1.16.4'; serial = $Serial; lineage = $Lineage; resources = @() }
    [System.IO.File]::WriteAllText($Path, ($state | ConvertTo-Json -Compress), [System.Text.UTF8Encoding]::new($false))
}

function Assert-Throws([scriptblock]$Action, [string]$Pattern, [string]$Name) {
    try { & $Action; throw "Expected $Name to reject the mismatch." }
    catch { if ($_.Exception.Message -notmatch $Pattern) { throw "${Name} failed for an unexpected reason: $($_.Exception.Message)" } }
}

try {
    $matchingPlan = Join-Path $temp 'matching.tfplan'
    $localStatePath = Join-Path $temp 'terraform.tfstate'
    Write-StateZip $matchingPlan 'lineage-fixture-a' 7
    Write-LocalState $localStatePath 'lineage-fixture-a' 7
    $planState = Read-Dur050PlanPriorState -SavedPlanPath $matchingPlan
    $localState = Read-Dur050LocalTerraformState -TerraformStatePath $localStatePath
    [void](Assert-Dur050PlanStateMatchesLocal -PlanPriorState $planState -LocalState $localState)
    Write-Host 'PASS: a saved-plan tfstate lineage and serial matching local state is accepted.'

    $emptyLineagePlan = Join-Path $temp 'empty-lineage.tfplan'
    Write-StateZip $emptyLineagePlan '' 0
    $emptyPlanState = Read-Dur050PlanPriorState -SavedPlanPath $emptyLineagePlan
    Assert-Throws { Assert-Dur050PlanStateMatchesLocal -PlanPriorState $emptyPlanState -LocalState $localState } 'lineage.*does not match' 'empty-lineage mismatch'
    Write-Host 'PASS: empty embedded lineage is rejected against an existing lineage-bearing local state.'

    $wrongSerialPlan = Join-Path $temp 'wrong-serial.tfplan'
    Write-StateZip $wrongSerialPlan 'lineage-fixture-a' 6
    $wrongSerialState = Read-Dur050PlanPriorState -SavedPlanPath $wrongSerialPlan
    Assert-Throws { Assert-Dur050PlanStateMatchesLocal -PlanPriorState $wrongSerialState -LocalState $localState } 'serial 6 does not match local-state serial 7' 'serial mismatch'
    Write-Host 'PASS: matching lineage with a different embedded serial is rejected.'

    $missingStatePath = Join-Path $temp 'absent.tfstate'
    $emptyLocal = Read-Dur050LocalTerraformState -TerraformStatePath $missingStatePath
    [void](Assert-Dur050PlanStateMatchesLocal -PlanPriorState $emptyPlanState -LocalState $emptyLocal)
    Write-Host 'PASS: no local state accepts only an empty plan lineage and serial 0.'

    $moduleSource = Get-Content -LiteralPath $modulePath -Raw
    $lineageGuard = '(?s)    if \(\[string\]\$PlanPriorState\.lineage -cne \[string\]\$LocalState\.lineage\) \{\s*throw "Saved plan prior-state lineage[^\"]+"\s*\}\s*'
    $serialGuard = '(?s)    if \(\[long\]\$PlanPriorState\.serial -ne \[long\]\$LocalState\.serial\) \{\s*throw "Saved plan prior-state serial[^\"]+"\s*\}\s*'
    $lineageMutant = [regex]::Replace($moduleSource, $lineageGuard, '', 1)
    $serialMutant = [regex]::Replace($moduleSource, $serialGuard, '', 1)
    if ($lineageMutant -eq $moduleSource -or $serialMutant -eq $moduleSource) { throw 'Could not construct both plan-state comparison mutants.' }
    $lineageMutantPath = Join-Path $temp 'lineage-mutant.psm1'
    $serialMutantPath = Join-Path $temp 'serial-mutant.psm1'
    [System.IO.File]::WriteAllText($lineageMutantPath, $lineageMutant, [System.Text.UTF8Encoding]::new($false))
    [System.IO.File]::WriteAllText($serialMutantPath, $serialMutant, [System.Text.UTF8Encoding]::new($false))

    Remove-Module dur050-plan-state -ErrorAction SilentlyContinue
    $lineageMutantModule = Import-Module $lineageMutantPath -Force -PassThru
    $lineageMutantSurvived = $false
    $lineageMutantError = $null
    $emptyLineageSameSerialPath = Join-Path $temp 'empty-lineage-same-serial.tfplan'
    Write-StateZip $emptyLineageSameSerialPath '' 7
    $emptyLineageSameSerial = Read-Dur050PlanPriorState -SavedPlanPath $emptyLineageSameSerialPath
    try { [void](& "$($lineageMutantModule.Name)\Assert-Dur050PlanStateMatchesLocal" -PlanPriorState $emptyLineageSameSerial -LocalState $localState); $lineageMutantSurvived = $true } catch { $lineageMutantError = $_.Exception.Message }
    if (-not $lineageMutantSurvived) { throw "Removing the lineage comparison did not make the lineage negative control pass: $lineageMutantError" }
    Write-Host 'PASS: removing the lineage comparison makes the empty-lineage negative control pass (test detects the mutant).'

    Remove-Module $lineageMutantModule.Name -ErrorAction SilentlyContinue
    $serialMutantModule = Import-Module $serialMutantPath -Force -PassThru
    $serialMutantSurvived = $false
    $serialMutantError = $null
    try { [void](& "$($serialMutantModule.Name)\Assert-Dur050PlanStateMatchesLocal" -PlanPriorState $wrongSerialState -LocalState $localState); $serialMutantSurvived = $true } catch { $serialMutantError = $_.Exception.Message }
    if (-not $serialMutantSurvived) { throw "Removing the serial comparison did not make the serial negative control pass: $serialMutantError" }
    Write-Host 'PASS: removing the serial comparison makes the wrong-serial negative control pass (test detects the mutant).'

    Remove-Module $serialMutantModule.Name -ErrorAction SilentlyContinue
    Import-Module $modulePath -Force

    if (-not $FixturesOnly) {
        if (-not $env:DUR050_NATIVE_PLAN_PATH) { throw 'CI must supply DUR050_NATIVE_PLAN_PATH for a Terraform-produced saved plan.' }
        if (-not $env:DUR050_NATIVE_STATE_PATH) { throw 'CI must supply DUR050_NATIVE_STATE_PATH for the absent local state path.' }
        $terraform = if ($env:DUR050_TERRAFORM_EXE) { $env:DUR050_TERRAFORM_EXE } else { 'terraform' }
        $native = Get-Dur050PlanStateSnapshot -SavedPlanPath $env:DUR050_NATIVE_PLAN_PATH -TerraformStatePath $env:DUR050_NATIVE_STATE_PATH -TerraformExe $terraform
        if ($native.plan_prior_state_lineage -cne '' -or $native.plan_prior_state_serial -ne 0 -or $native.local_state_file_present) {
            throw "Terraform-native empty-config plan did not carry the expected empty prior state: $($native | ConvertTo-Json -Compress)"
        }
        Write-Host "PASS: Terraform 1.16.4 native plan ZIP inspected; prior state lineage='$($native.plan_prior_state_lineage)', serial=$($native.plan_prior_state_serial), executable SHA-256=$($native.terraform_executable_sha256)."

        foreach ($name in @('DUR050_NATIVE_LIFECYCLE_WORKDIR', 'DUR050_NATIVE_LIFECYCLE_P1_PATH', 'DUR050_NATIVE_LIFECYCLE_P2_PATH', 'DUR050_NATIVE_LIFECYCLE_STATE_PATH')) {
            if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) { throw "CI must provide $name for the Terraform-produced state lifecycle check." }
        }
        $lifecycleWorkdir = $env:DUR050_NATIVE_LIFECYCLE_WORKDIR
        $lifecycleStatePath = $env:DUR050_NATIVE_LIFECYCLE_STATE_PATH
        $p1Path = $env:DUR050_NATIVE_LIFECYCLE_P1_PATH
        $p2Path = $env:DUR050_NATIVE_LIFECYCLE_P2_PATH
        $p2Snapshot = Get-Dur050PlanStateSnapshot -SavedPlanPath $p2Path -TerraformStatePath $lifecycleStatePath -TerraformExe $terraform
        if (-not $p2Snapshot.state_matches -or $p2Snapshot.plan_prior_state_lineage -ne $p2Snapshot.local_state_lineage -or
            $p2Snapshot.plan_prior_state_serial -ne $p2Snapshot.local_state_serial) {
            throw "Terraform-native p2 did not match the current state: $($p2Snapshot | ConvertTo-Json -Compress)"
        }
        Write-Host "PASS: p2 embedded prior state matches the local lifecycle state's lineage and serial $($p2Snapshot.local_state_serial)."

        $p1Snapshot = Get-Dur050PlanStateSnapshot -SavedPlanPath $p1Path -TerraformStatePath $lifecycleStatePath -TerraformExe $terraform -AllowMismatch
        if ($p1Snapshot.state_matches -or $p1Snapshot.plan_prior_state_lineage -ne $p1Snapshot.local_state_lineage -or
            $p1Snapshot.plan_prior_state_serial -eq $p1Snapshot.local_state_serial -or $p1Snapshot.state_mismatch -notmatch 'serial') {
            throw "Terraform-native p1 did not report only the expected stale serial mismatch: $($p1Snapshot | ConvertTo-Json -Compress)"
        }
        Write-Host "PASS: p1 retains the same lineage but reports its prior serial $($p1Snapshot.plan_prior_state_serial) differs from current serial $($p1Snapshot.local_state_serial)."

        $p2ApplyOutput = & $terraform "-chdir=$lifecycleWorkdir" apply -auto-approve -input=false $p2Path 2>&1 | Out-String
        $p2ApplyExitCode = $LASTEXITCODE
        if ($p2ApplyExitCode -ne 0) { throw "Terraform rejected matching p2 (exit $p2ApplyExitCode): $p2ApplyOutput" }
        Write-Host 'PASS: Terraform 1.16.4 applied matching p2 successfully.'

        $p1ApplyOutput = & $terraform "-chdir=$lifecycleWorkdir" apply -auto-approve -input=false $p1Path 2>&1 | Out-String
        $p1ApplyExitCode = $LASTEXITCODE
        if ($p1ApplyExitCode -eq 0 -or $p1ApplyOutput -notmatch 'Saved plan is stale') {
            throw "Terraform did not reject stale p1 with the expected diagnostic (exit $p1ApplyExitCode): $p1ApplyOutput"
        }
        Write-Host 'PASS: Terraform itself rejects stale p1 with "Saved plan is stale".'
    } else {
        Write-Host 'SKIP: native Terraform plan check excluded by FixturesOnly; CI must run it without that switch.'
    }
} finally {
    Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue
}
