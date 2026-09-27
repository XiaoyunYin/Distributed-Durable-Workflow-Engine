#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$applyTool = Join-Path $PSScriptRoot 'dur050-apply.ps1'
$manifestPath = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/cost-manifest.json'
$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ('dur050-apply-test-' + [guid]::NewGuid().ToString('N'))
$mutants = [System.Collections.Generic.List[string]]::new()
$pwsh = [Environment]::ProcessPath

function Write-Utf8([string]$Path, [string]$Text) {
    [IO.File]::WriteAllText($Path, $Text, [Text.UTF8Encoding]::new($false))
}

function To-JsonText([object]$Value) {
    return (ConvertTo-Json -InputObject $Value -Depth 100) + "`n"
}

function Copy-Json([object]$Value) {
    return (ConvertTo-Json -InputObject $Value -Depth 100 -Compress) | ConvertFrom-Json -AsHashtable -Depth 100
}

function Set-StubExecutable([string]$Path, [string]$UnixBody, [string]$WindowsBody) {
    if ([OperatingSystem]::IsWindows()) {
        Write-Utf8 ($Path + '.cmd') $WindowsBody
        return ($Path + '.cmd')
    }
    Write-Utf8 $Path ($UnixBody -replace "`r`n", "`n" -replace "`r", "`n")
    [IO.File]::SetUnixFileMode($Path, [IO.UnixFileMode]::UserRead -bor [IO.UnixFileMode]::UserWrite -bor [IO.UnixFileMode]::UserExecute -bor [IO.UnixFileMode]::GroupRead -bor [IO.UnixFileMode]::GroupExecute -bor [IO.UnixFileMode]::OtherRead -bor [IO.UnixFileMode]::OtherExecute)
    return $Path
}

function Invoke-Child([string]$ScriptPath, [string[]]$Arguments, [hashtable]$Environment) {
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $pwsh
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.ArgumentList.Add('-NoProfile')
    $start.ArgumentList.Add('-File')
    $start.ArgumentList.Add($ScriptPath)
    foreach ($argument in $Arguments) { $start.ArgumentList.Add([string]$argument) }
    foreach ($key in $Environment.Keys) { $start.Environment[[string]$key] = [string]$Environment[$key] }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    return [pscustomobject]@{ ExitCode = $process.ExitCode; Stdout = $stdout; Stderr = $stderr; Combined = $stdout + $stderr }
}

function Write-AwsStub([string]$Directory, [string]$Name) {
    $log = Join-Path $Directory ($Name + '-aws.log')
    $unix = @'
#!/bin/sh
printf '%s\n' "$*" >> "$DUR050_AWS_STUB_LOG"
if [ "$DUR050_AWS_SCENARIO" = "wrong-account" ]; then
  printf '%s\n' '{"UserId":"test","Account":"000000000000","Arn":"arn:aws:iam::000000000000:user/test"}'
else
  printf '%s\n' '{"UserId":"test","Account":"372206265946","Arn":"arn:aws:iam::372206265946:user/test"}'
fi
'@
    $windows = @'
@echo off
>>"%DUR050_AWS_STUB_LOG%" echo %*
if "%DUR050_AWS_SCENARIO%"=="wrong-account" (
  echo {"UserId":"test","Account":"000000000000","Arn":"arn:aws:iam::000000000000:user/test"}
) else (
  echo {"UserId":"test","Account":"372206265946","Arn":"arn:aws:iam::372206265946:user/test"}
)
exit /b 0
'@
    $stub = Set-StubExecutable (Join-Path $Directory 'aws') $unix $windows
    return [pscustomobject]@{ Path = $stub; Log = $log }
}

function Write-TerraformFailureStub([string]$Directory, [string]$Name) {
    $log = Join-Path $Directory ($Name + '-terraform.log')
    $unix = @'
#!/bin/sh
printf '%s\n' "$*" >> "$DUR050_TERRAFORM_STUB_LOG"
if [ "$1" = "version" ]; then
  printf '%s\n' '{"terraform_version":"1.16.4"}'
  exit 0
fi
printf '%s\n' 'Apply failed! Resources: 0 added, 0 changed, 0 destroyed.' >&2
exit 1
'@
    $windows = @'
@echo off
>>"%DUR050_TERRAFORM_STUB_LOG%" echo %*
if "%~1"=="version" (
  echo {"terraform_version":"1.16.4"}
  exit /b 0
)
echo Apply failed! Resources: 0 added, 0 changed, 0 destroyed.
exit /b 1
'@
    $stub = Set-StubExecutable (Join-Path $Directory 'terraform-fail') $unix $windows
    return [pscustomobject]@{ Path = $stub; Log = $log }
}

function Invoke-ApplyScenario([string]$Tool, [string]$Cycle, [string]$Manifest, [string]$TerraformDir, [string]$StatePath, [string]$StubDir, [string]$AwsLog, [string]$TerraformLog, [string]$AwsScenario = 'pass') {
    $pathSeparator = [IO.Path]::PathSeparator
    $environment = @{
        PATH = $StubDir + $pathSeparator + $env:PATH
        DUR050_ENABLE_TEST_HOOKS = '1'
        DUR050_AWS_STUB_LOG = $AwsLog
        DUR050_AWS_SCENARIO = $AwsScenario
        DUR050_TERRAFORM_STUB_LOG = $TerraformLog
        AWS_PROFILE = 'apply-test-no-credentials'
    }
    return Invoke-Child $Tool @('-CycleID', $Cycle, '-CampaignManifestPath', $Manifest, '-TerraformDirectory', $TerraformDir, '-TerraformStatePath', $StatePath) $environment
}

function Assert-NoTerraformPrecheck([string]$Label, [string]$Tool, [string]$Cycle, [string]$Manifest, [string]$TerraformDir, [string]$StatePath, [string]$StubDir, [string]$AwsLog, [string]$TerraformLog, [string]$ExpectedMessage, [string]$AwsScenario = 'pass') {
    $beforeBytes = [IO.File]::ReadAllBytes($Manifest)
    $beforeHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($beforeBytes))
    if (Test-Path -LiteralPath $TerraformLog) { Remove-Item -LiteralPath $TerraformLog -Force }
    $result = Invoke-ApplyScenario $Tool $Cycle $Manifest $TerraformDir $StatePath $StubDir $AwsLog $TerraformLog $AwsScenario
    if ($result.ExitCode -eq 0 -or $result.Combined -notmatch [regex]::Escape($ExpectedMessage)) {
        throw "$Label did not fail with the expected precheck: $($result.Combined)"
    }
    $afterHash = (Get-FileHash -LiteralPath $Manifest -Algorithm SHA256).Hash
    if ($beforeHash -ine $afterHash) { throw "$Label edited the manifest before refusing." }
    if (Test-Path -LiteralPath $TerraformLog) { throw "$Label invoked Terraform before refusing: $(Get-Content $TerraformLog -Raw)" }
    Write-Output "PASS: $Label refused before manifest mutation or Terraform invocation."
    return $result
}

function New-Scenario([string]$Name, [string]$TemplateTerraformDir, [string]$PlanPath, [string]$PlanSha, [string]$StateLineage, [long]$StateSerial, [string]$TerraformExePath, [string]$TerraformExeSha) {
    $scenarioRoot = Join-Path $tempRoot $Name
    $terraformDir = Join-Path $scenarioRoot 'terraform'
    $campaignDir = Join-Path $scenarioRoot 'campaign'
    New-Item -ItemType Directory -Path $terraformDir, $campaignDir | Out-Null
    Copy-Item -LiteralPath (Join-Path $TemplateTerraformDir 'main.tf') -Destination $terraformDir
    $templateLock = Join-Path $TemplateTerraformDir '.terraform.lock.hcl'
    if (Test-Path -LiteralPath $templateLock) { Copy-Item -LiteralPath $templateLock -Destination $terraformDir }
    $templateMeta = Join-Path $TemplateTerraformDir '.terraform'
    if (Test-Path -LiteralPath $templateMeta -PathType Container) { Copy-Item -LiteralPath $templateMeta -Destination $terraformDir -Recurse }
    Copy-Item -LiteralPath (Join-Path $TemplateTerraformDir 'terraform.tfstate') -Destination (Join-Path $terraformDir 'terraform.tfstate')
    $state = Get-Content -LiteralPath (Join-Path $terraformDir 'terraform.tfstate') -Raw | ConvertFrom-Json
    $state.lineage = $StateLineage
    $state.serial = $StateSerial
    Write-Utf8 (Join-Path $terraformDir 'terraform.tfstate') (To-JsonText $state)

    $shape = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json -AsHashtable -Depth 100
    $manifest = Copy-Json $shape
    $head = (& git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Could not read current Git commit for the test manifest.' }
    $pin = $manifest['approved_saved_plan']
    $pin['source_commit'] = $head
    $pin['saved_plan_path'] = [IO.Path]::GetFullPath($PlanPath)
    $pin['saved_plan_sha256'] = $PlanSha
    $pin['inspection_path'] = 'test-only/inspection.json'
    $pin['inspection_sha256'] = ('0' * 64)
    $pin['expected_plan_prior_state_lineage'] = $StateLineage
    $pin['expected_plan_prior_state_serial'] = $StateSerial
    $pin['review_state'] = 'PENDING_CLAUDE_GO'
    $pin['terraform_executable_path'] = [IO.Path]::GetFullPath($TerraformExePath)
    $pin['terraform_executable_sha256'] = $TerraformExeSha
    $pin['terraform_version'] = '1.16.4'
    $manifestFile = Join-Path $campaignDir 'cost-manifest.json'
    Write-Utf8 $manifestFile (To-JsonText $manifest)
    $cycle = 'ci-apply-' + $Name
    return [pscustomobject]@{ Root=$scenarioRoot; TerraformDir=$terraformDir; StatePath=(Join-Path $terraformDir 'terraform.tfstate'); CampaignDir=$campaignDir; Manifest=$manifestFile; Cycle=$cycle }
}

try {
    New-Item -ItemType Directory -Path $tempRoot | Out-Null
    $stubDir = Join-Path $tempRoot 'stubs'
    New-Item -ItemType Directory -Path $stubDir | Out-Null
    $terraformCommand = $env:DUR050_TEST_TERRAFORM_EXE
    if (-not $terraformCommand) {
        $windowsTerraform = 'C:\Users\yxyfz\AppData\Local\Programs\Terraform\1.16.4\terraform.exe'
        if ([OperatingSystem]::IsWindows() -and (Test-Path -LiteralPath $windowsTerraform)) { $terraformCommand = $windowsTerraform }
        else { $terraformCommand = (Get-Command terraform -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source }
    }
    $terraformExe = (Resolve-Path -LiteralPath $terraformCommand).Path
    $terraformVersionRaw = & $terraformExe version -json 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -or [string](($terraformVersionRaw | ConvertFrom-Json).terraform_version) -cne '1.16.4') { throw 'DUR-050 apply tests require Terraform 1.16.4.' }
    $terraformExeSha = (Get-FileHash -LiteralPath $terraformExe -Algorithm SHA256).Hash.ToUpperInvariant()

    $templateTerraformDir = Join-Path $tempRoot 'native/terraform'
    New-Item -ItemType Directory -Path $templateTerraformDir | Out-Null
    Write-Utf8 (Join-Path $templateTerraformDir 'main.tf') @'
terraform {
  required_version = ">= 1.16.4, < 2.0.0"
}

variable "v" {
  type    = string
  default = "dur050-apply-native-test"
}

resource "terraform_data" "marker" {
  input = var.v
}

output "o" {
  value = terraform_data.marker.output
}
'@
    $initOutput = & $terraformExe ("-chdir=$templateTerraformDir") init -backend=false -input=false 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "Terraform test fixture init failed: $initOutput" }
    $stateLineage = [guid]::NewGuid().ToString()
    $initialState = [ordered]@{ version=4; terraform_version='1.16.4'; serial=231; lineage=$stateLineage; outputs=[ordered]@{}; resources=@(); check_results=$null }
    Write-Utf8 (Join-Path $templateTerraformDir 'terraform.tfstate') (To-JsonText $initialState)
    $planPath = Join-Path $templateTerraformDir 'native.tfplan'
    $planOutput = & $terraformExe ("-chdir=$templateTerraformDir") plan -refresh=false -input=false "-out=$planPath" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "Terraform test fixture plan failed: $planOutput" }
    $planSha = (Get-FileHash -LiteralPath $planPath -Algorithm SHA256).Hash.ToUpperInvariant()

    $awsInfo = Write-AwsStub $stubDir 'native'
    $terraformFailure = Write-TerraformFailureStub $stubDir 'native'
    $native = New-Scenario 'native-positive' $templateTerraformDir $planPath $planSha $stateLineage 231 $terraformExe $terraformExeSha
    $nativeAwsLog = $awsInfo.Log
    $nativeTerraformLog = $terraformFailure.Log
    $nativeManifestBefore = Get-Content -LiteralPath $native.Manifest -Raw | ConvertFrom-Json -AsHashtable -Depth 100
    $nativeResult = Invoke-ApplyScenario $applyTool $native.Cycle $native.Manifest $native.TerraformDir $native.StatePath $stubDir $nativeAwsLog $nativeTerraformLog
    if ($nativeResult.ExitCode -ne 0) { throw "Native Terraform apply case failed: $($nativeResult.Combined)" }
    $nativeManifestAfter = Get-Content -LiteralPath $native.Manifest -Raw | ConvertFrom-Json -AsHashtable -Depth 100
    $nativeManifestReset = Copy-Json $nativeManifestAfter
    $nativeManifestReset['approved_saved_plan']['review_state'] = 'PENDING_CLAUDE_GO'
    if ((ConvertTo-Json -InputObject $nativeManifestReset -Depth 100 -Compress) -cne (ConvertTo-Json -InputObject $nativeManifestBefore -Depth 100 -Compress)) { throw 'Native apply changed manifest fields other than review_state.' }
    if ([string]$nativeManifestAfter.approved_saved_plan.review_state -notmatch '^APPLIED_\d{4}-\d\d-\d\dT.*Z$') { throw 'Native apply did not retain an APPLIED_ timestamp.' }
    $nativeCycleDir = Join-Path (Join-Path $native.CampaignDir 'cycles') $native.Cycle
    $nativeRecord = Get-Content -LiteralPath (Join-Path $nativeCycleDir 'apply-record.json') -Raw | ConvertFrom-Json
    $nativeOutputs = Get-Content -LiteralPath (Join-Path $nativeCycleDir 'terraform-outputs.json') -Raw | ConvertFrom-Json
    if ($nativeRecord.status -ne 'PASS' -or $nativeRecord.exit_code -ne 0 -or $nativeRecord.counts.added -ne 1 -or $nativeRecord.counts.changed -ne 0 -or $nativeRecord.counts.destroyed -ne 0 -or $nativeRecord.post_apply_state.resource_count -ne 1) { throw 'Native apply record did not capture a passing apply and expected counts/state.' }
    if ($nativeRecord.terraform_outputs_sha256 -ne (Get-FileHash -LiteralPath (Join-Path $nativeCycleDir 'terraform-outputs.json') -Algorithm SHA256).Hash.ToUpperInvariant() -or $nativeOutputs.o.value -ne 'dur050-apply-native-test') { throw 'Native outputs or output hash did not match the provider-less fixture.' }
    if ((Get-Content -LiteralPath $nativeAwsLog -Raw) -notmatch 'sts get-caller-identity') { throw 'Native positive did not use the AWS identity stub.' }
    Write-Output 'PASS: provider-less Terraform 1.16.4 plan/apply writes the structured marker, apply record, and hashed outputs.'

    $failureAws = Write-AwsStub $stubDir 'apply-failure'
    $failureTerraform = Write-TerraformFailureStub $stubDir 'apply-failure'
    $failedScenario = New-Scenario 'apply-failure' $templateTerraformDir $planPath $planSha $stateLineage 231 $failureTerraform.Path ((Get-FileHash $failureTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
    $failureResult = Invoke-ApplyScenario $applyTool $failedScenario.Cycle $failedScenario.Manifest $failedScenario.TerraformDir $failedScenario.StatePath $stubDir $failureAws.Log $failureTerraform.Log
    if ($failureResult.ExitCode -eq 0) { throw 'Terraform failure stub unexpectedly passed.' }
    $failedManifest = Get-Content -LiteralPath $failedScenario.Manifest -Raw | ConvertFrom-Json
    $failedRecord = Get-Content -LiteralPath (Join-Path (Join-Path $failedScenario.CampaignDir ('cycles/' + $failedScenario.Cycle)) 'apply-record.json') -Raw | ConvertFrom-Json
    if ($failedRecord.status -ne 'FAIL' -or $failedRecord.exit_code -ne 1 -or [string]$failedManifest.approved_saved_plan.review_state -notmatch '^APPLIED_' -or $failureResult.Combined -notmatch 'dur050-destroy\.ps1') { throw 'Apply failure did not retain the APPLIED_ marker, FAIL record, exit code, and teardown command.' }
    Write-Output 'PASS: apply exit 1 leaves an APPLIED_ marker, durable FAIL record, and exact teardown command.'

    $negativeAws = Write-AwsStub $stubDir 'negative'
    $negativeTerraform = Write-TerraformFailureStub $stubDir 'negative'
    $badHash = New-Scenario 'bad-plan-hash' $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ((Get-FileHash $negativeTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
    $badHashManifest = Get-Content -LiteralPath $badHash.Manifest -Raw | ConvertFrom-Json -AsHashtable -Depth 100
    $badHashManifest.approved_saved_plan.saved_plan_sha256 = ('0' * 64)
    Write-Utf8 $badHash.Manifest (To-JsonText $badHashManifest)
    $null = Assert-NoTerraformPrecheck 'plan hash mismatch' $applyTool $badHash.Cycle $badHash.Manifest $badHash.TerraformDir $badHash.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log 'Saved plan SHA-256 mismatch'

    $stale = New-Scenario 'stale-serial' $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ((Get-FileHash $negativeTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
    $staleState = Get-Content -LiteralPath $stale.StatePath -Raw | ConvertFrom-Json
    $staleState.serial = 232
    Write-Utf8 $stale.StatePath (To-JsonText $staleState)
    $null = Assert-NoTerraformPrecheck 'stale local serial' $applyTool $stale.Cycle $stale.Manifest $stale.TerraformDir $stale.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log 'Terraform state precheck failed'

    $wrongLineageValue = ([guid]::NewGuid()).ToString()
    $wrongLineage = New-Scenario 'wrong-lineage' $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ((Get-FileHash $negativeTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
    $wrongLineageState = Get-Content -LiteralPath $wrongLineage.StatePath -Raw | ConvertFrom-Json
    $wrongLineageState.lineage = $wrongLineageValue
    Write-Utf8 $wrongLineage.StatePath (To-JsonText $wrongLineageState)
    $null = Assert-NoTerraformPrecheck 'wrong local lineage' $applyTool $wrongLineage.Cycle $wrongLineage.Manifest $wrongLineage.TerraformDir $wrongLineage.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log 'Terraform state precheck failed'

    $badReview = New-Scenario 'bad-review-state' $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ((Get-FileHash $negativeTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
    $badReviewManifest = Get-Content -LiteralPath $badReview.Manifest -Raw | ConvertFrom-Json -AsHashtable -Depth 100
    $badReviewManifest.approved_saved_plan.review_state = 'APPLIED_old'
    Write-Utf8 $badReview.Manifest (To-JsonText $badReviewManifest)
    $null = Assert-NoTerraformPrecheck 'review-state mismatch' $applyTool $badReview.Cycle $badReview.Manifest $badReview.TerraformDir $badReview.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log 'review_state must be exactly PENDING_CLAUDE_GO'

    $wrongAccount = New-Scenario 'wrong-account' $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ((Get-FileHash $negativeTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
    $null = Assert-NoTerraformPrecheck 'wrong AWS account' $applyTool $wrongAccount.Cycle $wrongAccount.Manifest $wrongAccount.TerraformDir $wrongAccount.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log 'AWS account must be 372206265946' 'wrong-account'

    $wrongExeHash = New-Scenario 'wrong-exe-hash' $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ('0' * 64)
    $null = Assert-NoTerraformPrecheck 'wrong executable hash' $applyTool $wrongExeHash.Cycle $wrongExeHash.Manifest $wrongExeHash.TerraformDir $wrongExeHash.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log 'Pinned Terraform executable SHA-256 mismatch'

    $existingRecord = New-Scenario 'existing-record' $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ((Get-FileHash $negativeTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
    $existingCycleDir = Join-Path (Join-Path $existingRecord.CampaignDir 'cycles') $existingRecord.Cycle
    New-Item -ItemType Directory -Path $existingCycleDir -Force | Out-Null
    Write-Utf8 (Join-Path $existingCycleDir 'apply-record.json') '{"status":"sentinel"}'
    $null = Assert-NoTerraformPrecheck 'existing apply record' $applyTool $existingRecord.Cycle $existingRecord.Manifest $existingRecord.TerraformDir $existingRecord.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log 'Cycle directory is not empty'

    function Invoke-GuardMutant([string]$Label, [string]$SourceNeedle, [string]$ScenarioName, [bool]$MakeStaleState) {
        $mutantSource = Get-Content -LiteralPath $applyTool -Raw
        if (-not $mutantSource.Contains($SourceNeedle)) { throw "$Label mutation target was not found exactly once." }
        if (($mutantSource.Split([string[]]@($SourceNeedle), [StringSplitOptions]::None).Length - 1) -ne 1) { throw "$Label mutation target is not unique." }
        $mutantPath = Join-Path $PSScriptRoot ('.dur050-apply-mutant-' + [guid]::NewGuid().ToString('N') + '.ps1')
        $mutants.Add($mutantPath)
        Write-Utf8 $mutantPath $mutantSource.Replace($SourceNeedle, '# MUTATION: removed ' + $Label + ' guard')
        $scenario = New-Scenario $ScenarioName $templateTerraformDir $planPath $planSha $stateLineage 231 $negativeTerraform.Path ((Get-FileHash $negativeTerraform.Path -Algorithm SHA256).Hash.ToUpperInvariant())
        if ($MakeStaleState) {
            $state = Get-Content -LiteralPath $scenario.StatePath -Raw | ConvertFrom-Json
            $state.serial = 232
            Write-Utf8 $scenario.StatePath (To-JsonText $state)
        } else {
            $manifest = Get-Content -LiteralPath $scenario.Manifest -Raw | ConvertFrom-Json -AsHashtable -Depth 100
            $manifest.approved_saved_plan.saved_plan_sha256 = ('0' * 64)
            Write-Utf8 $scenario.Manifest (To-JsonText $manifest)
        }
        if (Test-Path -LiteralPath $negativeTerraform.Log) { Remove-Item -LiteralPath $negativeTerraform.Log -Force }
        $result = Invoke-ApplyScenario $mutantPath $scenario.Cycle $scenario.Manifest $scenario.TerraformDir $scenario.StatePath $stubDir $negativeAws.Log $negativeTerraform.Log
        $after = Get-Content -LiteralPath $scenario.Manifest -Raw | ConvertFrom-Json
        $terraformWasCalled = Test-Path -LiteralPath $negativeTerraform.Log
        if ([string]$after.approved_saved_plan.review_state -ceq 'PENDING_CLAUDE_GO' -and -not $terraformWasCalled) {
            throw "$Label guard-removal mutant survived the precheck test."
        }
        Write-Output "PASS: removing the $Label guard violates the no-mutation/no-Terraform precheck contract."
    }

    $planGuard = 'if ($planHash -cne ([string]$pin.saved_plan_sha256).ToUpperInvariant()) { throw "Saved plan SHA-256 mismatch: observed $planHash." }'
    Invoke-GuardMutant 'plan-hash' $planGuard 'mutant-plan-hash' $false
    $stateGuard = 'if ($stateMismatch) { throw "Terraform state precheck failed: $stateMismatch" }'
    Invoke-GuardMutant 'state' $stateGuard 'mutant-state' $true

    Write-Output 'DUR-050 apply tool tests: PASS'
} finally {
    foreach ($mutant in $mutants) { Remove-Item -LiteralPath $mutant -Force -ErrorAction SilentlyContinue }
    $resolvedTemp = [IO.Path]::GetFullPath($tempRoot)
    $expectedTempPrefix = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if ($resolvedTemp.StartsWith($expectedTempPrefix, [StringComparison]::OrdinalIgnoreCase) -and (Test-Path -LiteralPath $resolvedTemp)) {
        Remove-Item -LiteralPath $resolvedTemp -Recurse -Force
    }
}
