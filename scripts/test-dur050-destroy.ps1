#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $PSScriptRoot 'dur050-plan-state.psm1') -Force
$pwsh = [Environment]::ProcessPath
$terraform = if ($env:DUR050_TERRAFORM_EXE) { $env:DUR050_TERRAFORM_EXE } else { (Get-Command terraform -CommandType Application -ErrorAction Stop).Source }
$terraformInfoRaw = & $terraform version -json 2>&1 | Out-String
if ($LASTEXITCODE -ne 0 -or ($terraformInfoRaw | ConvertFrom-Json).terraform_version -ne '1.16.4') { throw 'DUR-050 destroy tests require Terraform 1.16.4.' }
$terraformHash = (Get-FileHash -LiteralPath $terraform -Algorithm SHA256).Hash.ToUpperInvariant()
$temp = Join-Path ([IO.Path]::GetTempPath()) ('dur050-destroy-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
$utf8 = [Text.UTF8Encoding]::new($false)
$manifestSource = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/cost-manifest.json'
$baseManifest = Get-Content -LiteralPath $manifestSource -Raw | ConvertFrom-Json
$freshCycleID = 'cycle-4-2330417'
$reviewMutantPath = $null
$hashMutantPath = $null
if (@($baseManifest.PSObject.Properties | Where-Object { [string]$_.Value.cycle_id -eq $freshCycleID }).Count -ne 0) {
    throw 'Destroy test CycleID must be fresh and absent from the committed manifest.'
}

function Write-Json([string]$Path, $Value) { [IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth 40) + "`n"), $utf8) }
function New-Manifest([string]$Path, [string]$TerraformPath, [string]$Hash, [string]$ReviewState = 'APPLIED_2026-09-27T04:46:54.1291096Z', [switch]$OmitAmi) {
    $manifest = Get-Content -LiteralPath $manifestSource -Raw | ConvertFrom-Json
    $profile = $manifest.terraform_profile
    if ($OmitAmi) { $profile.PSObject.Properties.Remove('ami_id') }
    $manifest.approved_saved_plan.terraform_executable_path = $TerraformPath
    $manifest.approved_saved_plan.terraform_executable_sha256 = $Hash
    $manifest.approved_saved_plan.terraform_version = '1.16.4'
    $manifest.approved_saved_plan.review_state = $ReviewState
    Write-Json $Path $manifest
}
function Invoke-Destroy([string]$Name, [string]$Manifest, [string]$TerraformPath, [string]$TfDir, [string]$StatePath, [string]$Cycle = $freshCycleID, [string]$Script = (Join-Path $PSScriptRoot 'dur050-destroy.ps1')) {
    $record = Join-Path $temp "$Name-record.json"
    $log = Join-Path $temp "$Name-argv.log"
    $args = @('-NoProfile','-File',$Script,'-CycleID',$Cycle,'-CampaignManifestPath',$Manifest,'-OutputPath',$record,'-TerraformExe',$TerraformPath,'-TerraformDirectory',$TfDir,'-TerraformStatePath',$StatePath)
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $pwsh; $start.UseShellExecute = $false; $start.RedirectStandardOutput = $true; $start.RedirectStandardError = $true
    foreach ($arg in $args) { $start.ArgumentList.Add($arg) }
    $start.Environment['DUR050_ENABLE_TEST_HOOKS'] = '1'
    $start.Environment['DUR050_DESTROY_TEST_ARGV_LOG'] = $log
    $start.Environment['DUR050_DESTROY_TEST_STATE'] = $StatePath
    $proc = [Diagnostics.Process]::new(); $proc.StartInfo = $start; [void]$proc.Start()
    $stdout = $proc.StandardOutput.ReadToEnd(); $stderr = $proc.StandardError.ReadToEnd(); $proc.WaitForExit()
    return [pscustomobject]@{ ExitCode=$proc.ExitCode; OutputPath=$record; LogPath=$log; Text=$stderr + "`n" + $stdout }
}

try {
    $stubPath = Join-Path $temp 'terraform-stub.ps1'
    $stub = @'
param([Parameter(ValueFromRemainingArguments=$true)][string[]]$CliArgs)
$ErrorActionPreference = 'Stop'
if ($CliArgs.Count -ge 2 -and $CliArgs[0] -eq 'version' -and $CliArgs[1] -eq '-json') { '{"terraform_version":"1.16.4"}'; exit 0 }
Add-Content -LiteralPath $env:DUR050_DESTROY_TEST_ARGV_LOG -Value $CliArgs
$varArgument = @($CliArgs | Where-Object { $_ -like '-var-file=*' } | Select-Object -First 1)
if ($varArgument.Count -ne 1) { throw 'var-file argument missing' }
$varFile = $varArgument[0].Substring(10)
if (-not (Test-Path -LiteralPath $varFile -PathType Leaf)) { throw 'var-file does not exist' }
$values = Get-Content -LiteralPath $varFile -Raw | ConvertFrom-Json
if ($values.availability_zones -isnot [array] -or $values.availability_zones.Count -ne 2 -or
    $values.enable_dur050_load_generator -isnot [bool] -or $values.root_volume_size_gb -isnot [ValueType] -or
    $values.postgres_password.Length -lt 20 -or $values.dur050_observer_password.Length -lt 24) { throw 'typed Terraform variables are invalid' }
$state = Get-Content -LiteralPath $env:DUR050_DESTROY_TEST_STATE -Raw | ConvertFrom-Json
$state.serial = [long]$state.serial + 1
$state.resources = @()
[IO.File]::WriteAllText($env:DUR050_DESTROY_TEST_STATE, ($state | ConvertTo-Json -Depth 20), [Text.UTF8Encoding]::new($false))
'Destroy complete! Resources: 31 destroyed.'
'@
    [IO.File]::WriteAllText($stubPath, $stub.Replace("`r", '') + [Environment]::NewLine, $utf8)
    $stubHash = (Get-FileHash $stubPath -Algorithm SHA256).Hash.ToUpperInvariant()
    $stubTfDir = Join-Path $temp 'stub-deploy'
    New-Item -ItemType Directory -Path $stubTfDir | Out-Null
    $stubState = Join-Path $stubTfDir 'terraform.tfstate'
    Write-Json $stubState ([ordered]@{ version=4; terraform_version='1.16.4'; serial=12; lineage='destroy-fixture'; resources=@(@{type='terraform_data'; name='example'}) })
    $stubManifest = Join-Path $temp 'stub-manifest.json'
    New-Manifest $stubManifest $stubPath $stubHash
    $stubResult = Invoke-Destroy 'stub-positive' $stubManifest $stubPath $stubTfDir $stubState
    if ($stubResult.ExitCode -ne 0) { throw "Stub destroy wrapper failed: $($stubResult.Text)" }
    $record = Get-Content $stubResult.OutputPath -Raw | ConvertFrom-Json
    $argv = Get-Content $stubResult.LogPath
    if ($record.schema -ne 'dur050-destroy.v1' -or $record.status -ne 'PASS' -or $record.exit_code -ne 0 -or
        $record.destroyed_count -ne 31 -or $record.post_destroy_state.serial -ne 13 -or $record.post_destroy_state.resource_count -ne 0 -or
        $argv[0] -ne "-chdir=$stubTfDir" -or $argv[1] -ne 'destroy' -or $argv[2] -ne '-auto-approve' -or
        $argv[3] -notmatch '^-var-file=' -or (Test-Path -LiteralPath $argv[3].Substring(10))) {
        throw 'Stub destroy did not verify argv, pinning, state record, and temporary var-file cleanup.'
    }
    Write-Host 'PASS: the pinned Terraform stub receives -chdir, destroy, auto-approve and an external typed var-file; record and cleanup are correct.'

    $wrongPinManifest = Join-Path $temp 'wrong-pin-manifest.json'
    New-Manifest $wrongPinManifest $stubPath ('0' * 64)
    $wrongPin = Invoke-Destroy 'wrong-pin' $wrongPinManifest $stubPath $stubTfDir $stubState
    if ($wrongPin.ExitCode -eq 0 -or (Test-Path $wrongPin.LogPath) -or (Get-Content $wrongPin.OutputPath -Raw | ConvertFrom-Json).status -ne 'FAIL') {
        throw 'Destroy wrapper did not fail closed on a Terraform executable hash mismatch before destroy.'
    }
    Write-Host 'PASS: a Terraform executable that does not match the manifest pin is rejected before destroy.'

    $notAppliedManifest = Join-Path $temp 'not-applied-manifest.json'
    New-Manifest $notAppliedManifest $stubPath $stubHash 'PENDING_CLAUDE_GO'
    $notApplied = Invoke-Destroy 'not-applied' $notAppliedManifest $stubPath $stubTfDir $stubState
    if ($notApplied.ExitCode -eq 0 -or (Test-Path $notApplied.LogPath) -or
        (Get-Content $notApplied.OutputPath -Raw | ConvertFrom-Json).status -ne 'FAIL' -or
        $notApplied.Text -notmatch 'review_state must match APPLIED_') {
        throw 'A non-APPLIED top-level review state did not fail before Terraform.'
    }
    Write-Host 'PASS: destroy refuses a non-APPLIED top-level plan before invoking Terraform.'

    $destroySource = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'dur050-destroy.ps1') -Raw
    $reviewMutant = [regex]::Replace($destroySource, "if \(\`$null -eq \`$approvedPlan -or \[string\]\`$approvedPlan\.review_state -notmatch '\^APPLIED_'\) \{ throw 'Top-level approved_saved_plan review_state must match APPLIED_ before destroy\.' \}", '# removed R188 applied-review-state guard', 1)
    if ($reviewMutant -eq $destroySource) { throw 'Could not construct the review-state guard-removal mutant.' }
    $reviewMutantPath = Join-Path $PSScriptRoot ('.dur050-destroy-review-state-mutant-' + [guid]::NewGuid().ToString('N') + '.ps1')
    [IO.File]::WriteAllText($reviewMutantPath, $reviewMutant, $utf8)
    $reviewMutantRun = Invoke-Destroy 'review-state-mutant' $notAppliedManifest $stubPath $stubTfDir $stubState $freshCycleID $reviewMutantPath
    if ($reviewMutantRun.ExitCode -ne 0 -or -not (Test-Path $reviewMutantRun.LogPath)) {
        throw "The review-state negative did not detect removal of its guard: $($reviewMutantRun.Text)"
    }
    Write-Host 'PASS: the non-APPLIED test detects its guard-removal mutant.'

    $hashMutant = [regex]::Replace($destroySource, "if \(\[string\]\`$terraformInfo\.sha256 -cne \`$pinnedHash\) \{ throw 'Terraform executable SHA-256 differs from the top-level approved_saved_plan pin\.' \}", '# removed R188 executable-hash guard', 1)
    if ($hashMutant -eq $destroySource) { throw 'Could not construct the executable-hash guard-removal mutant.' }
    $hashMutantPath = Join-Path $PSScriptRoot ('.dur050-destroy-hash-mutant-' + [guid]::NewGuid().ToString('N') + '.ps1')
    [IO.File]::WriteAllText($hashMutantPath, $hashMutant, $utf8)
    $hashMutantRun = Invoke-Destroy 'hash-mutant' $wrongPinManifest $stubPath $stubTfDir $stubState $freshCycleID $hashMutantPath
    if ($hashMutantRun.ExitCode -ne 0 -or -not (Test-Path $hashMutantRun.LogPath)) {
        throw "The hash-mismatch negative did not detect removal of its guard: $($hashMutantRun.Text)"
    }
    Write-Host 'PASS: the hash-mismatch test detects its guard-removal mutant.'

    $missingManifest = Join-Path $temp 'missing-profile-manifest.json'
    New-Manifest $missingManifest $stubPath $stubHash -OmitAmi
    $missing = Invoke-Destroy 'missing-profile' $missingManifest $stubPath $stubTfDir $stubState
    if ($missing.ExitCode -eq 0 -or (Test-Path $missing.LogPath) -or $missing.Text -notmatch "missing required variable 'ami_id'") {
        throw 'Missing Terraform profile input did not fail before Terraform ran.'
    }
    Write-Host 'PASS: a missing required profile variable is rejected before Terraform is invoked.'

    $nativeInfo = Resolve-Dur050TerraformExecutable -TerraformExe $terraform
    $nativeDir = Join-Path $temp 'native-config'
    New-Item -ItemType Directory -Path $nativeDir | Out-Null
    $hcl = [Text.StringBuilder]::new()
    [void]$hcl.AppendLine('terraform { required_version = "= 1.16.4" }')
    foreach ($property in $baseManifest.terraform_profile.PSObject.Properties) {
        $type = if ($property.Value -is [bool]) { 'bool' } elseif ($property.Value -is [array]) { 'list(string)' } elseif ($property.Value -is [string] -or $property.Value -is [DateTime] -or $property.Value -is [DateTimeOffset]) { 'string' } elseif ($property.Value -is [ValueType]) { 'number' } else { 'string' }
        [void]$hcl.AppendLine("variable `"$($property.Name)`" { type = $type }")
    }
    [void]$hcl.AppendLine('variable "postgres_password" { type = string }')
    [void]$hcl.AppendLine('variable "dur050_observer_password" { type = string }')
    [void]$hcl.AppendLine('resource "terraform_data" "typed" { input = { az = var.availability_zones[0], enabled = var.enable_dur050_load_generator, volume = var.root_volume_size_gb } }')
    [IO.File]::WriteAllText((Join-Path $nativeDir 'main.tf'), $hcl.ToString(), $utf8)
    $nativeVars = [ordered]@{}
    foreach ($property in $baseManifest.terraform_profile.PSObject.Properties) { $nativeVars[$property.Name] = $property.Value }
    $nativeVars.postgres_password = 'NativeDestroyTestPassword1234'
    $nativeVars.dur050_observer_password = 'NativeObserverTestPassword123456'
    $nativeVarsPath = Join-Path $nativeDir 'initial.tfvars.json'
    Write-Json $nativeVarsPath $nativeVars
    Push-Location $nativeDir
    try {
        $nativeInitOutput = & $nativeInfo.path init -backend=false -input=false 2>&1 | Out-String
        if ($LASTEXITCODE -ne 0) { throw "Provider-less native Terraform init failed: $nativeInitOutput" }
        $nativeApplyOutput = & $nativeInfo.path apply -auto-approve -input=false "-var-file=$nativeVarsPath" 2>&1 | Out-String
        if ($LASTEXITCODE -ne 0) { throw "Provider-less typed var-file apply failed: $nativeApplyOutput" }
    } finally { Pop-Location }
    $nativeManifest = Join-Path $temp 'native-manifest.json'
    $nativeTerraformPath = $nativeInfo.path
    $committedPinPath = [string]$baseManifest.approved_saved_plan.terraform_executable_path
    if (Test-Path -LiteralPath $committedPinPath -PathType Leaf) {
        $committedPin = Resolve-Dur050TerraformExecutable -TerraformExe $committedPinPath
        if ([string]$committedPin.sha256 -ceq [string]$baseManifest.approved_saved_plan.terraform_executable_sha256) {
            $nativeManifest = $manifestSource
            $nativeTerraformPath = $committedPin.path
        }
    }
    if ($nativeManifest -ne $manifestSource) { New-Manifest $nativeManifest $nativeTerraformPath $nativeInfo.sha256 }
    $nativeResult = Invoke-Destroy 'native-providerless' $nativeManifest $nativeTerraformPath $nativeDir (Join-Path $nativeDir 'terraform.tfstate') $freshCycleID
    if ($nativeResult.ExitCode -ne 0) { throw "Provider-less native destroy failed: $($nativeResult.Text)" }
    $nativeRecord = Get-Content $nativeResult.OutputPath -Raw | ConvertFrom-Json
    if ($nativeRecord.status -ne 'PASS' -or $nativeRecord.destroyed_count -ne 1 -or $nativeRecord.post_destroy_state.resource_count -ne 0) {
        throw 'Provider-less native Terraform destroy did not accept the generated typed tfvars JSON end-to-end.'
    }
    Write-Host 'PASS: Terraform 1.16.4 provider-less native config accepted the wrapper-generated typed JSON -var-file and destroyed its fixture resource.'
    if ($nativeManifest -eq $manifestSource) {
        Write-Host "PASS: the native destroy used the committed manifest and fresh, unrecorded cycle ID $freshCycleID."
    } else {
        Write-Host "PASS: the native destroy used a test copy preserving committed manifest structure and fresh cycle ID $freshCycleID."
    }
} finally {
    foreach ($mutantFile in @($reviewMutantPath,$hashMutantPath) | Where-Object { $_ -and (Test-Path -LiteralPath $_) }) {
        Remove-Item -LiteralPath $mutantFile -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue
}
