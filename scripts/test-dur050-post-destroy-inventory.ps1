$ErrorActionPreference = 'Stop'
if (-not $IsLinux) { throw 'DUR-050 post-destroy inventory tests require Ubuntu/Linux.' }
$repoRoot = Split-Path -Parent $PSScriptRoot
$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('dur050-post-destroy-test-' + [guid]::NewGuid().ToString('N'))
$bin = Join-Path $temp 'bin'
$utf8 = [System.Text.UTF8Encoding]::new($false)
New-Item -ItemType Directory -Force -Path $bin | Out-Null

function Write-TestJson([string]$Path, $Value) {
    [System.IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth 40) + [Environment]::NewLine), $utf8)
}

function Invoke-Inventory([string]$Scenario, [string]$Name, [string]$ToolPath = $inventoryScript, [string]$LedgerFile = $ledgerPath) {
    $output = Join-Path $temp "$Name-inventory.json"
    $log = Join-Path $temp "$Name-aws.log"
    $childArgs = @('-NoProfile', '-File', $ToolPath, '-CycleID', 'cycle-r183-test', '-CampaignManifestPath', $manifestPath,
        '-OutputPath', $output, '-TerraformExe', $terraformStub, '-TerraformStatePath', $statePath,
        '-LedgerPath', $LedgerFile, '-RepositoryRoot', $repoRoot)
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = Join-Path $PSHOME 'pwsh'
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($arg in $childArgs) { $start.ArgumentList.Add($arg) }
    $start.Environment['PATH'] = $bin + ':/usr/bin:/bin'
    $start.Environment['DUR050_AWS_STUB'] = Join-Path $PSScriptRoot 'test-support/dur050-post-destroy-aws-stub.py'
    $start.Environment['DUR050_INVENTORY_AWS_LOG'] = $log
    $start.Environment['DUR050_INVENTORY_SCENARIO'] = $Scenario
    $start.Environment['DUR050_ENABLE_TEST_HOOKS'] = '1'
    $proc = [Diagnostics.Process]::new()
    $proc.StartInfo = $start
    [void]$proc.Start()
    $stdout = $proc.StandardOutput.ReadToEnd()
    $stderr = $proc.StandardError.ReadToEnd()
    $proc.WaitForExit()
    return [pscustomobject]@{ ExitCode = $proc.ExitCode; OutputPath = $output; AwsLogPath = $log; Text = $stderr + [Environment]::NewLine + $stdout }
}

function Get-RecordValue($Record, [string]$DottedPath) {
    $value = $Record
    foreach ($part in $DottedPath.Split('.')) { $value = $value.$part }
    return $value
}

function Assert-InventoryFail($Result, [string]$Name, [string]$Path, [string]$ExpectedPattern = '') {
    if ($Result.ExitCode -eq 0) { throw "Inventory negative '$Name' unexpectedly passed." }
    if (-not (Test-Path -LiteralPath $Result.OutputPath -PathType Leaf)) { throw "Inventory negative '$Name' did not write a record." }
    $record = Get-Content -LiteralPath $Result.OutputPath -Raw | ConvertFrom-Json
    if ($record.schema -ne 'dur050-post-destroy-inventory.v4' -or $record.status -ne 'FAIL') { throw "Inventory negative '$Name' did not write a FAIL record: $($record | ConvertTo-Json -Depth 8 -Compress)" }
    if ($Path -and -not (Get-RecordValue $record $Path)) { throw "Inventory negative '$Name' did not expose its leftover in $Path." }
    if ($ExpectedPattern -and (@($record.violations) -join '; ') -notmatch $ExpectedPattern) { throw "Inventory negative '$Name' recorded unexpected violations: $(@($record.violations) -join '; ')" }
}

try {
    $awsStub = @'
#!/bin/sh
set -eu
exec python3 "$DUR050_AWS_STUB" "$@"
'@
    $awsPath = Join-Path $bin 'aws'
    [IO.File]::WriteAllText($awsPath, $awsStub.Replace("`r", '') + [Environment]::NewLine, $utf8)
    & chmod 0755 $awsPath
    if ($LASTEXITCODE -ne 0) { throw 'Could not mark the AWS stub executable.' }

    $terraformStub = Join-Path $bin 'terraform'
    $terraformBody = @'
#!/bin/sh
set -eu
if [ "${1:-}" = version ] && [ "${2:-}" = -json ]; then printf '{"terraform_version":"1.16.4"}\n'; exit 0; fi
case " $* " in
  *" state list "*)
    if [ "${DUR050_INVENTORY_SCENARIO:-}" = state ]; then echo 'aws_vpc.campaign'; fi
    exit 0 ;;
esac
echo "unexpected Terraform invocation: $*" >&2
exit 89
'@
    [IO.File]::WriteAllText($terraformStub, $terraformBody.Replace("`r", '') + [Environment]::NewLine, $utf8)
    & chmod 0755 $terraformStub
    if ($LASTEXITCODE -ne 0) { throw 'Could not mark the Terraform stub executable.' }

    $inventoryScript = Join-Path $PSScriptRoot 'dur050-post-destroy-inventory.ps1'
    $manifestPath = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/cost-manifest.json'
    $ledgerPath = Join-Path $temp 'task-ledger.json'
    Copy-Item -LiteralPath (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Destination $ledgerPath
    $statePath = Join-Path $temp 'terraform.tfstate'
    Write-TestJson $statePath ([ordered]@{ version = 4; terraform_version = '1.16.4'; serial = 167; lineage = 'fixture-state-lineage'; resources = @() })

    foreach ($scenario in @('empty', 'null')) {
        $result = Invoke-Inventory $scenario "positive-$scenario"
        if ($result.ExitCode -ne 0) { throw "Empty inventory case '$scenario' failed: $($result.Text)" }
        $record = Get-Content -LiteralPath $result.OutputPath -Raw | ConvertFrom-Json
        if ($record.status -ne 'PASS' -or $record.checks.region_instances.non_terminated_count -ne 0 -or
            $record.checks.region_volumes.volume_count -ne 0 -or $record.checks.task_vpcs.count -ne 0 -or
            $record.checks.task_nat_gateways.count -ne 0 -or $record.checks.ssm_parameters.missing_expected_names.Count -ne 0 -or
            -not $record.raw_aws_responses.'ec2.describe-vpcs') {
            throw "Empty inventory case '$scenario' did not preserve raw responses and explicit zero counts."
        }
        Write-Host "PASS: $scenario empty/null AWS arrays count as zero and raw responses are retained."
    }

    $cases = @(
        @{ scenario = 'state'; name = 'terraform-state'; path = 'checks.terraform_state_list.resource_count'; pattern = 'state still contains' },
        @{ scenario = 'instance'; name = 'region-instance'; path = 'checks.region_instances.non_terminated_count'; pattern = 'non-terminated instance' },
        @{ scenario = 'volume'; name = 'region-volume'; path = 'checks.region_volumes.volume_count'; pattern = 'region has 1 volume' },
        @{ scenario = 'vpc'; name = 'vpc'; path = 'checks.task_vpcs.count'; pattern = 'task_vpcs has 1' },
        @{ scenario = 'subnet'; name = 'subnet'; path = 'checks.task_subnets.count'; pattern = 'task_subnets has 1' },
        @{ scenario = 'security-group'; name = 'security-group'; path = 'checks.task_security_groups.count'; pattern = 'task_security_groups has 1' },
        @{ scenario = 'eni'; name = 'eni'; path = 'checks.task_network_interfaces.count'; pattern = 'task_network_interfaces has 1' },
        @{ scenario = 'eip'; name = 'eip'; path = 'checks.task_eips.count'; pattern = 'task_eips has 1' },
        @{ scenario = 'snapshot'; name = 'snapshot'; path = 'checks.task_snapshots.count'; pattern = 'task_snapshots has 1' },
        @{ scenario = 'nat'; name = 'nat-gateway'; path = 'checks.task_nat_gateways.count'; pattern = 'task_nat_gateways has 1' },
        @{ scenario = 'elbv2-lb'; name = 'elbv2-load-balancer'; path = 'checks.task_elbv2_load_balancers.count'; pattern = 'task_elbv2_load_balancers has 1' },
        @{ scenario = 'classic-lb'; name = 'classic-load-balancer'; path = 'checks.task_classic_load_balancers.count'; pattern = 'task_classic_load_balancers has 1' },
        @{ scenario = 'iam-role'; name = 'iam-role'; path = 'checks.iam_roles.count'; pattern = 'DUR-050 IAM role' },
        @{ scenario = 'iam-profile'; name = 'iam-profile'; path = 'checks.iam_instance_profiles.count'; pattern = 'IAM instance profile' },
        @{ scenario = 'ssm-observer'; name = 'ssm-observer-parameter'; path = 'checks.ssm_parameters.returned_names.Count'; pattern = 'SSM parameters are not all absent' },
        @{ scenario = 'ssm-postgres'; name = 'ssm-postgres-parameter'; path = 'checks.ssm_parameters.returned_names.Count'; pattern = 'SSM parameters are not all absent' }
    )
    foreach ($case in $cases) {
        $result = Invoke-Inventory $case.scenario $case.name
        Assert-InventoryFail $result $case.name $case.path $case.pattern
        $record = Get-Content -LiteralPath $result.OutputPath -Raw | ConvertFrom-Json
        $rawResponseCount = @($record.raw_aws_responses.PSObject.Properties).Count
        if ($rawResponseCount -lt 10) { throw "Inventory case '$($case.name)' retained only $rawResponseCount raw read-only AWS responses." }
        Write-Host "PASS: leftover $($case.name) is recorded FAIL with raw AWS responses."
    }

    $ledgerWithOpen = Get-Content -LiteralPath $ledgerPath -Raw | ConvertFrom-Json
    $appOneIntervals = @($ledgerWithOpen.roles.'app-1')
    $appOneIntervals += [ordered]@{ cycle_id = 'cycle-r183-test'; instance_id = 'i-0123456789abcdef0'; instance_type = 'c7i.large'; apply_started_at_utc = '2026-09-27T00:00:00Z'; destroy_completed_at_utc = $null }
    $ledgerWithOpen.roles.'app-1' = $appOneIntervals
    $openLedgerPath = Join-Path $temp 'open-ledger.json'
    Write-TestJson $openLedgerPath $ledgerWithOpen
    $persistedLedger = Get-Content -LiteralPath $openLedgerPath -Raw | ConvertFrom-Json
    $fixtureOpen = @($persistedLedger.roles.PSObject.Properties | ForEach-Object { $_.Value } | ForEach-Object { $_ } | Where-Object { $_.cycle_id -eq 'cycle-r183-test' -and [string]::IsNullOrWhiteSpace([string]$_.destroy_completed_at_utc) })
    if ($fixtureOpen.Count -ne 1) { throw "Open-ledger test fixture persisted $($fixtureOpen.Count) open intervals instead of one." }
    $openLedger = Invoke-Inventory 'empty' 'open-ledger' -LedgerFile $openLedgerPath
    Assert-InventoryFail $openLedger 'open-ledger' 'checks.ledger_open_intervals.open_interval_count' 'open interval'
    Write-Host 'PASS: an open task-ledger interval for the cycle is recorded FAIL.'

    $existingOutput = Join-Path $temp 'overwrite.json'
    [IO.File]::WriteAllText($existingOutput, 'preserve-me', $utf8)
    $overwriteArgs = @('-NoProfile', '-File', $inventoryScript, '-CycleID', 'cycle-r183-test', '-CampaignManifestPath', $manifestPath,
        '-OutputPath', $existingOutput, '-TerraformExe', $terraformStub, '-TerraformStatePath', $statePath,
        '-LedgerPath', $ledgerPath, '-RepositoryRoot', $repoRoot)
    $overwriteStart = [Diagnostics.ProcessStartInfo]::new()
    $overwriteStart.FileName = Join-Path $PSHOME 'pwsh'; $overwriteStart.UseShellExecute = $false
    $overwriteStart.RedirectStandardError = $true; $overwriteStart.RedirectStandardOutput = $true
    foreach ($arg in $overwriteArgs) { $overwriteStart.ArgumentList.Add($arg) }
    $overwriteStart.Environment['PATH'] = $bin + ':/usr/bin:/bin'
    $overwriteStart.Environment['DUR050_AWS_STUB'] = Join-Path $PSScriptRoot 'test-support/dur050-post-destroy-aws-stub.py'
    $overwriteStart.Environment['DUR050_INVENTORY_SCENARIO'] = 'empty'
    $overwriteStart.Environment['DUR050_ENABLE_TEST_HOOKS'] = '1'
    $overwriteProc = [Diagnostics.Process]::new(); $overwriteProc.StartInfo = $overwriteStart; [void]$overwriteProc.Start()
    $overwriteText = $overwriteProc.StandardError.ReadToEnd() + $overwriteProc.StandardOutput.ReadToEnd(); $overwriteProc.WaitForExit()
    if ($overwriteProc.ExitCode -eq 0 -or [IO.File]::ReadAllText($existingOutput) -ne 'preserve-me') { throw "Inventory tool overwrote an existing record: $overwriteText" }
    Write-Host 'PASS: the inventory tool refuses to overwrite an existing evidence path.'

    $source = Get-Content -LiteralPath $inventoryScript -Raw
    $guard = 'if ($nonTerminated.Count -ne 0) { $script:violations.Add("region has $($nonTerminated.Count) non-terminated instance(s)") }'
    $mutant = $source.Replace($guard, 'if ($false) { $script:violations.Add("region has $($nonTerminated.Count) non-terminated instance(s)") }')
    if ($mutant -eq $source) { throw 'Could not construct the region-instance guard-removal mutant.' }
    $mutantDir = Join-Path $temp 'mutant/scripts'
    New-Item -ItemType Directory -Force -Path $mutantDir | Out-Null
    $mutantScript = Join-Path $mutantDir 'dur050-post-destroy-inventory.ps1'
    [IO.File]::WriteAllText($mutantScript, $mutant, $utf8)
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'dur050-plan-state.psm1') -Destination (Join-Path $mutantDir 'dur050-plan-state.psm1')
    $mutantResult = Invoke-Inventory 'instance' 'mutant-instance-zero-guard' -ToolPath $mutantScript
    $mutantRecord = Get-Content -LiteralPath $mutantResult.OutputPath -Raw | ConvertFrom-Json
    if ($mutantResult.ExitCode -ne 0 -or $mutantRecord.status -ne 'PASS') { throw "Removing the region-instance guard did not make the negative control appear to pass: $($mutantResult.Text)" }
    try { Assert-InventoryFail $mutantResult 'instance mutation' 'checks.region_instances.non_terminated_count' 'non-terminated instance'; throw 'The leftover-instance assertion survived guard removal.' }
    catch { if ($_.Exception.Message -notmatch 'unexpectedly passed') { throw } }
    Write-Host 'PASS: removing the region-instance guard makes its negative assertion fail.'
} finally {
    Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue
}
