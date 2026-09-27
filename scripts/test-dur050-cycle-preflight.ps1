$ErrorActionPreference = 'Stop'
if (-not $IsLinux) { throw 'DUR-050 cycle-preflight tests require Ubuntu/Linux.' }
$repoRoot = Split-Path -Parent $PSScriptRoot
$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('dur050-preflight-test-' + [guid]::NewGuid().ToString('N'))
$bin = Join-Path $temp 'bin'; $state = Join-Path $temp 'state'; $utf8 = [System.Text.UTF8Encoding]::new($false)
New-Item -ItemType Directory -Force -Path $bin,$state | Out-Null
$aws = @'
#!/bin/sh
set -eu
args="$*"
printf '%s\n' "$args" >> "$DUR050_AWS_STATE/calls.log"
case "$args" in
  *'sts get-caller-identity'*)
    if [ "${DUR050_SCENARIO:-pass}" = account ]; then account=000000000000; else account=372206265946; fi
    printf '{"Account":"%s","Arn":"arn:aws:iam::%s:user/test"}\n' "$account" "$account" ;;
  *'service-quotas get-service-quota'*)
    if [ "${DUR050_SCENARIO:-pass}" = quota ]; then quota=4; else quota=32; fi
    printf '{"Quota":{"QuotaCode":"L-1216C47A","ServiceCode":"ec2","Value":%s}}\n' "$quota" ;;
  *'ec2 describe-instances --instance-ids'*)
    python3 - <<'PY'
import json
from datetime import datetime, timezone, timedelta
ids = [
    ("i-11111111111111111", "c7i.large"),
    ("i-22222222222222222", "c7i.large"),
    ("i-0123456789abcdef0", "m7i.large"),
    ("i-33333333333333333", "c7i.large"),
]
launch = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
print(json.dumps({"Reservations":[{"Instances":[{"InstanceId":i,"InstanceType":t,"LaunchTime":launch,"RootDeviceName":"/dev/xvda","State":{"Name":"running"}} for i,t in ids]}]}))
PY
    ;;
  *'ec2 describe-instances'*) printf '{"Reservations":[]}\n' ;;
  *'ec2 describe-volumes'*)
    printf '{"Volumes":[{"VolumeId":"vol-00000000000000001","State":"in-use","Tags":[{"Key":"Task","Value":"DUR-050"}],"Attachments":[{"InstanceId":"i-11111111111111111","Device":"/dev/xvda"}]},{"VolumeId":"vol-00000000000000002","State":"in-use","Tags":[{"Key":"Task","Value":"DUR-050"}],"Attachments":[{"InstanceId":"i-22222222222222222","Device":"/dev/xvda"}]},{"VolumeId":"vol-00000000000000003","State":"in-use","Tags":[{"Key":"Task","Value":"DUR-050"}],"Attachments":[{"InstanceId":"i-0123456789abcdef0","Device":"/dev/xvda"}]},{"VolumeId":"vol-00000000000000004","State":"in-use","Tags":[{"Key":"Task","Value":"DUR-050"}],"Attachments":[{"InstanceId":"i-33333333333333333","Device":"/dev/xvda"}]}]}\n' ;;
  *'budgets describe-budget'*) printf '{"Budget":{"BudgetLimit":{"Amount":"200","Unit":"USD"},"CalculatedSpend":{"ActualSpend":{"Amount":"1"},"ForecastedSpend":{"Amount":"5"}}}}\n' ;;
  *'budgets describe-notifications-for-budget'*)
    python3 - "${DUR050_SCENARIO:-pass}" <<'PY'
import json, sys
scenario = sys.argv[1]
rows = [
    {"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN", "Threshold": 160, "ThresholdType": "ABSOLUTE_VALUE", "NotificationState": "OK"},
    {"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN", "Threshold": 76.47, "ThresholdType": "ABSOLUTE_VALUE", "NotificationState": "OK"},
    {"NotificationType": "FORECASTED", "ComparisonOperator": "GREATER_THAN", "Threshold": 160, "ThresholdType": "ABSOLUTE_VALUE", "NotificationState": "OK"},
]
if scenario == "budget":
    rows = rows[:1]
elif scenario.startswith("budget-alarm-"):
    index = {"budget-alarm-actual-160": 0, "budget-alarm-actual-76": 1, "budget-alarm-forecasted-160": 2}[scenario]
    rows[index]["NotificationState"] = "ALARM"
elif scenario == "budget-missing-state":
    del rows[1]["NotificationState"]
elif scenario == "budget-less-than":
    rows[2]["ComparisonOperator"] = "LESS_THAN"
print(json.dumps({"Notifications": rows}, separators=(",", ":")))
PY
    ;;
  *'ce list-cost-allocation-tags'*)
    if [ "${DUR050_SCENARIO:-pass}" = tags ]; then task=Inactive; else task=Active; fi
    printf '{"CostAllocationTags":[{"TagKey":"Task","Status":"%s","LastUpdatedDate":"2026-09-25T07:06:39Z"},{"TagKey":"Environment","Status":"Active","LastUpdatedDate":"2026-09-25T07:06:39Z"}]}\n' "$task" ;;
  *'ssm send-command'*)
    request=$(printf '%s\n' "$args" | sed -n 's/.*file:\/\/\([^ ]*\).*/\1/p')
    comment=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["Comment"])' "$request")
    role=${comment##* }
    count=$(cat "$DUR050_AWS_STATE/count" 2>/dev/null || echo 0); count=$((count+1)); printf '%s' "$count" > "$DUR050_AWS_STATE/count"; printf '%s' "$role" > "$DUR050_AWS_STATE/role-$count"
    printf '{"Command":{"CommandId":"cmd-%s"}}\n' "$count" ;;
  *'ssm get-command-invocation'*)
    id=$(printf '%s\n' "$args" | sed -n 's/.*--command-id \([^ ]*\).*/\1/p'); count=${id#cmd-}; role=$(cat "$DUR050_AWS_STATE/role-$count")
    if [ "${DUR050_SCENARIO:-pass}" = bootstrap ]; then printf '{"Status":"Failed","StandardOutputContent":"","StandardErrorContent":"bootstrap rejected"}\n'
    else printf '{"Status":"Success","StandardOutputContent":"DUR050_BOOTSTRAP_PASS role=%s\\n","StandardErrorContent":""}\n' "$role"; fi ;;
  *) echo "unexpected aws arguments: $args" >&2; exit 90 ;;
esac
'@
[IO.File]::WriteAllText((Join-Path $bin 'aws'),$aws.Replace("`r",'')+"`n",$utf8); & chmod 0755 (Join-Path $bin 'aws'); if($LASTEXITCODE){throw 'chmod aws stub failed'}
function Write-Json([string]$Path,$Value){[IO.File]::WriteAllText($Path,(($Value|ConvertTo-Json -Depth 30)+"`n"),$utf8)}
function Invoke-Tool([string]$Scenario,[string]$Name,[string]$PlanPath,[switch]$DryRun,[string]$Timezone,[string]$ScriptPath,[switch]$NestedLedgerOutput){
  $out=Join-Path $temp "$Name-preflight.json"; $ledgerOut=if($NestedLedgerOutput){Join-Path $temp "$Name/new/deep/ledger-check.json"}else{Join-Path $temp "$Name-ledger-check.json"}
  if(-not $ScriptPath){$ScriptPath=Join-Path $PSScriptRoot 'dur050-cycle-preflight.ps1'}
  $start=[Diagnostics.ProcessStartInfo]::new(); $start.FileName=Join-Path $PSHOME 'pwsh'; $start.UseShellExecute=$false; $start.RedirectStandardOutput=$true; $start.RedirectStandardError=$true
  $arguments=@('-NoProfile','-File',$ScriptPath,'-TerraformOutputsPath',(Join-Path $repoRoot 'tests/fixtures/dur050-terraform-outputs.json'),'-PlanInspectionPath',$PlanPath,'-CycleID','ci-r163r164','-CampaignManifestPath',$manifestPath,'-LedgerCheckPath',$ledgerOut,'-OutputPath',$out,'-ReserveMinutes','15','-LedgerPath',$ledgerPath,'-PythonExe','python3')
  if($DryRun){$arguments+=@('-DryRun','-DryRunOutputPath',$out)}
  foreach($arg in $arguments){$start.ArgumentList.Add($arg)}
  $start.Environment['PATH']="$bin`:/usr/bin:/bin"; $start.Environment['DUR050_SCENARIO']=$Scenario; $start.Environment['DUR050_AWS_STATE']=$state; $start.Environment['DUR050_ENABLE_TEST_LEDGER_OVERRIDE']='1'; $start.Environment['DUR050_ENABLE_TEST_HOOKS']='1'; $start.Environment['DUR050_BOOTSTRAP_ROOT']=$fakeRoot
  if($Timezone){$start.Environment['TZ']=$Timezone}
  $proc=[Diagnostics.Process]::new(); $proc.StartInfo=$start; [void]$proc.Start(); $stdout=$proc.StandardOutput.ReadToEnd(); $stderr=$proc.StandardError.ReadToEnd(); $proc.WaitForExit()
  return [pscustomobject]@{ExitCode=$proc.ExitCode;Out=$out;Ledger=$ledgerOut;Text=($stderr+"`n"+$stdout)}
}
function Assert-CycleFailure($Result,[string]$Name,[string]$Pattern){
  if($Result.ExitCode -eq 0 -or $Result.Text -notmatch $Pattern){throw "Expected $Name to fail the ledger/output cross-check: $($Result.Text)"}
  if(-not(Test-Path $Result.Out) -or (Get-Content $Result.Out -Raw|ConvertFrom-Json).status -ne 'FAIL'){throw "$Name did not write a FAIL record."}
}
function Invoke-LedgerRecord([string]$Mode,[string]$Name,[string]$ApplyStart,[string]$DestroyAt,[string]$RequestedCycleID='ci-r163r164'){
  $arguments=@('-NoProfile','-File',(Join-Path $PSScriptRoot 'dur050-ledger-record.ps1'),"-$Mode",'-CycleID',$RequestedCycleID,'-LedgerPath',$ledgerPath)
  if($Mode -eq 'Open'){$arguments+=@('-TerraformOutputsPath',(Join-Path $repoRoot 'tests/fixtures/dur050-terraform-outputs.json'),'-ApplyStartedAtUtc',$ApplyStart)}else{$arguments+=@('-DestroyCompletedAtUtc',$DestroyAt)}
  $start=[Diagnostics.ProcessStartInfo]::new();$start.FileName=Join-Path $PSHOME 'pwsh';$start.UseShellExecute=$false;$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true
  foreach($arg in $arguments){$start.ArgumentList.Add($arg)}
  $start.Environment['PATH']="$bin`:/usr/bin:/bin";$start.Environment['DUR050_AWS_STATE']=$state;$start.Environment['DUR050_ENABLE_TEST_LEDGER_OVERRIDE']='1'
  $proc=[Diagnostics.Process]::new();$proc.StartInfo=$start;[void]$proc.Start();$stdout=$proc.StandardOutput.ReadToEnd();$stderr=$proc.StandardError.ReadToEnd();$proc.WaitForExit()
  return [pscustomobject]@{ExitCode=$proc.ExitCode;Text=($stderr+"`n"+$stdout)}
}
try {
  $sourceLedger=Get-Content (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Raw | ConvertFrom-Json
  $ledgerPath=Join-Path $temp 'ledger.json'; Copy-Item (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') $ledgerPath
  $startTime=[DateTimeOffset]::UtcNow.AddMinutes(-2).ToString('o')
  $openRecord=Invoke-LedgerRecord 'Open' 'open-ledger' $startTime $null
  if($openRecord.ExitCode -ne 0){throw "ledger-record -Open failed: $($openRecord.Text)"}
  $openedLedger=Get-Content $ledgerPath -Raw|ConvertFrom-Json
  if(@($openedLedger.roles.PSObject.Properties|ForEach-Object{$_.Value}|ForEach-Object{$_}|Where-Object{$_.cycle_id -eq 'ci-r163r164' -and $null -eq $_.destroy_completed_at_utc}).Count -ne 4){throw 'ledger-record -Open did not append four open intervals.'}
  $openedHash=(Get-FileHash $ledgerPath -Algorithm SHA256).Hash
  $duplicateOpen=Invoke-LedgerRecord 'Open' 'duplicate-open' $startTime $null
  if($duplicateOpen.ExitCode -eq 0 -or (Get-FileHash $ledgerPath -Algorithm SHA256).Hash -ne $openedHash){throw 'ledger-record -Open did not refuse an existing open cycle without changing the file.'}
  Write-Host 'PASS: duplicate ledger open is refused and leaves the task-wide file byte-identical.'
  $manifest=Get-Content (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/cost-manifest.json') -Raw | ConvertFrom-Json
  $manifestPath=Join-Path $temp 'manifest.json'; Write-Json $manifestPath $manifest
  $fakeRoot=Join-Path $temp 'rootfs'; New-Item -ItemType Directory -Force -Path (Join-Path $fakeRoot 'var/lib'),(Join-Path $fakeRoot 'opt/durable-agent-execution-engine/bin')|Out-Null
  [IO.File]::WriteAllText((Join-Path $fakeRoot 'var/lib/durable-dur050-bootstrap-complete'),'')
  [IO.File]::WriteAllText((Join-Path $fakeRoot 'var/lib/durable-dur050-generator-bootstrap-complete'),'')
  foreach($exe in @('dur050-observer','dur050-loadgen','dur050-sink')){[IO.File]::WriteAllText((Join-Path $fakeRoot "opt/durable-agent-execution-engine/bin/$exe"),''); & chmod 0755 (Join-Path $fakeRoot "opt/durable-agent-execution-engine/bin/$exe")}
  $cloud=Join-Path $bin 'cloud-init'; [IO.File]::WriteAllText($cloud,"#!/bin/sh`nprintf 'status: done\nerrors: []\nrecoverable_errors: {}\n'`n",$utf8); & chmod 0755 $cloud
  $plan=Join-Path $repoRoot 'tests/fixtures/dur050-plan-inspection.json'
  $pass=Invoke-Tool 'pass' 'pass' $plan -NestedLedgerOutput; if($pass.ExitCode -ne 0){throw "PASS fixture failed: $($pass.Text)"}
  $record=Get-Content $pass.Out -Raw | ConvertFrom-Json
  if($record.schema -ne 'dur050-d022-preflight.v1' -or $record.status -ne 'PASS' -or $record.account_id -ne '372206265946' -or $record.region -ne 'us-west-1' -or $record.planned_peak_vcpu -ne 8 -or $record.bootstrap.Count -ne 4 -or -not $record.ledger_check.checked_at_utc -or $record.checks.ledger_instance_crosscheck.open_interval_count -ne 4){throw "PASS record omitted required fields: schema=$($record.schema) status=$($record.status) account=$($record.account_id) region=$($record.region) peak=$($record.planned_peak_vcpu) bootstrap=$($record.bootstrap.Count) ledger=$($record.ledger_check.checked_at_utc) crosscheck=$($record.checks.ledger_instance_crosscheck|ConvertTo-Json -Compress)."}
  if($record.checks.root_volume_tags.root_volumes.Count -ne 4 -or -not $record.checks.root_volume_tags.all_root_volumes_tagged_task_dur050){throw 'PASS record did not report all four root-volume Task tags.'}
  if($record.checks.budget.notifications.Count -ne 3 -or @($record.checks.budget.notifications|Where-Object {$_.state -ne 'OK' -or $_.comparison_operator -ne 'GREATER_THAN'}).Count -ne 0 -or $record.checks.budget.notifications[0].PSObject.Properties['subscriber_count']){throw 'Budget PASS record did not preserve observed notification states/operators without invented subscriber counts.'}
  if((Get-Content $pass.Ledger -Raw | ConvertFrom-Json).reserve_minutes -ne 15){throw 'PASS record omitted its explicit ledger reserve check.'}
  if(-not(Test-Path -LiteralPath $pass.Ledger -PathType Leaf) -or (Get-Content $pass.Ledger -Raw|ConvertFrom-Json).status -ne 'PASS' -or -not(Test-Path -LiteralPath (Split-Path -Parent $pass.Ledger) -PathType Container)){throw 'Cycle preflight did not write a PASS ledger check into its previously nonexistent nested output directory.'}
  Write-Host 'PASS: D022 preflight stub checks account, quota, budget notifications, tags, ledger reserve and four bootstrap hosts.'
  Write-Host 'PASS: cycle preflight creates a nested ledger-check directory and writes the check.'
  foreach($zone in @('America/Los_Angeles','Asia/Shanghai')){
    $zoneName=$zone.Replace('/','-');$zonePass=Invoke-Tool 'pass' "timezone-$zoneName" $plan -Timezone $zone
    if($zonePass.ExitCode -ne 0){throw "PASS preflight failed under TZ=${zone}: $($zonePass.Text)"}
    $zoneRecord=Get-Content $zonePass.Out -Raw|ConvertFrom-Json
    if($zoneRecord.status -ne 'PASS'){throw "Timezone preflight under $zone left no valid PASS record."}
    Write-Host "PASS: cycle-preflight PASS fixture self-validates under TZ=$zone."
  }
  $validOpenLedger=Get-Content $ledgerPath -Raw|ConvertFrom-Json
  $ledgerVariants=@(
    @{name='no-open-intervals';value=$sourceLedger;pattern='exactly one open interval for app-1'},
    @{name='wrong-instance-id';value=($validOpenLedger|ConvertTo-Json -Depth 30|ConvertFrom-Json);pattern='does not match Terraform instance'},
    @{name='wrong-instance-type';value=($validOpenLedger|ConvertTo-Json -Depth 30|ConvertFrom-Json);pattern='expected c7i.large'},
    @{name='only-three-open-intervals';value=($validOpenLedger|ConvertTo-Json -Depth 30|ConvertFrom-Json);pattern='exactly one open interval for app-2'}
  )
  $ledgerVariants[1].value.roles.'app-1'[-1].instance_id='i-44444444444444444'
  $ledgerVariants[2].value.roles.'app-1'[-1].instance_type='m7i.large'
  $ledgerVariants[3].value.roles.'app-2'=@($ledgerVariants[3].value.roles.'app-2'|Select-Object -First ($ledgerVariants[3].value.roles.'app-2'.Count-1))
  foreach($variant in $ledgerVariants){Write-Json $ledgerPath $variant.value;$failed=Invoke-Tool 'pass' $variant.name $plan;if($variant.name -eq 'no-open-intervals'){$expected='exactly one open interval for app-1'}else{$expected=$variant.pattern};Assert-CycleFailure $failed $variant.name $expected;Write-Host "PASS: ledger/output mismatch '$($variant.name)' is recorded FAIL."}
  $validOpenLedger|ConvertTo-Json -Depth 30|Set-Content -LiteralPath $ledgerPath -Encoding utf8
  $mutantDir=Join-Path $temp 'mutant/scripts';New-Item -ItemType Directory -Force -Path $mutantDir|Out-Null
  foreach($file in @('dur050-cycle-preflight.ps1','dur050-ssm-wrapper.ps1','dur050-d022-validator.ps1','dur050-d022-shared.ps1','dur050-cost-ledger.py')){Copy-Item (Join-Path $PSScriptRoot $file) (Join-Path $mutantDir $file)}
  $cycleSource=Get-Content (Join-Path $PSScriptRoot 'dur050-cycle-preflight.ps1') -Raw
  $ledgerCliSource=Get-Content (Join-Path $PSScriptRoot 'dur050-cost-ledger.py') -Raw
  $ledgerMkdirPattern='(?m)^[ \t]*args\.output\.parent\.mkdir\(parents=True, exist_ok=True\)\r?\n'
  if([regex]::Matches($ledgerCliSource,$ledgerMkdirPattern).Count -ne 1){throw 'Expected exactly one parent-directory mkdir in the cost-ledger CLI.'}
  $ledgerCliMutant=[regex]::Replace($ledgerCliSource,$ledgerMkdirPattern,'')
  [IO.File]::WriteAllText((Join-Path $mutantDir 'dur050-cost-ledger.py'),$ledgerCliMutant,$utf8)
  $ledgerOutputPreflightMutant=Join-Path $mutantDir 'dur050-cycle-preflight-ledger-output-mutant.ps1'
  [IO.File]::WriteAllText($ledgerOutputPreflightMutant,$cycleSource,$utf8)
  $crossCheckCall='$script:awsSnapshot.ledger_instance_crosscheck = Assert-Dur050LedgerMatchesOutputs -Outputs $outputs -ExpectedCycleID $CycleID'
  $crossCheckMutant=$cycleSource.Replace($crossCheckCall,"`$script:awsSnapshot.ledger_instance_crosscheck = [ordered]@{ state = 'OK'; open_interval_count = 4 }")
  if($crossCheckMutant -eq $cycleSource){throw 'Could not apply ledger cross-check guard-removal mutant.'}
  $mutantPath=Join-Path $mutantDir 'dur050-cycle-preflight.ps1';[IO.File]::WriteAllText($mutantPath,$crossCheckMutant,$utf8)
  $badIdLedger=$validOpenLedger|ConvertTo-Json -Depth 30|ConvertFrom-Json;$badIdLedger.roles.'app-1'[-1].instance_id='i-44444444444444444';Write-Json $ledgerPath $badIdLedger
  $mutantResult=Invoke-Tool 'pass' 'mutant-ledger-crosscheck' $plan -ScriptPath $mutantPath
  if($mutantResult.ExitCode -ne 0 -or (Get-Content $mutantResult.Out -Raw|ConvertFrom-Json).status -ne 'PASS'){throw "Removing the ledger/output cross-check did not make the mismatched ledger pass: $($mutantResult.Text)"}
  try{Assert-CycleFailure $mutantResult 'ledger cross-check mutation' 'does not match Terraform';throw 'The ledger/output cross-check negative unexpectedly survived guard removal.'}catch{if($_.Exception.Message -notmatch 'Expected ledger cross-check mutation to fail'){throw}}
  Write-Host 'PASS: removing the ledger/output guard makes its negative assertion fail.'
  Write-Json $ledgerPath $validOpenLedger
  $nestedLedgerMutant=Invoke-Tool 'pass' 'mutant-nested-ledger-output' $plan -ScriptPath $ledgerOutputPreflightMutant -NestedLedgerOutput
  Assert-CycleFailure $nestedLedgerMutant 'nested-ledger-parent-mkdir mutation' 'No such file or directory'
  Write-Host 'PASS: removing parent mkdir makes the cycle-preflight nested-output positive case fail.'
  Write-Json (Join-Path $temp 'ledger.json') $validOpenLedger
  $ledgerPath=Join-Path $temp 'ledger-close-test.json';Copy-Item (Join-Path $temp 'ledger.json') $ledgerPath
  $beforeBadClose=(Get-FileHash $ledgerPath -Algorithm SHA256).Hash
  $badClose=Invoke-LedgerRecord 'Close' 'bad-close' $null ([DateTimeOffset]::UtcNow.AddMinutes(1).ToString('o')) 'other-cycle'
  if($badClose.ExitCode -eq 0 -or (Get-FileHash $ledgerPath -Algorithm SHA256).Hash -ne $beforeBadClose){throw 'Invalid ledger close changed the ledger file.'}
  Write-Host 'PASS: invalid ledger close leaves the file byte-identical.'
  $close=Invoke-LedgerRecord 'Close' 'close-ledger' $null ([DateTimeOffset]::UtcNow.AddMinutes(1).ToString('o'))
  if($close.ExitCode -ne 0){throw "ledger-record -Close failed: $($close.Text)"}
  $closed=Get-Content $ledgerPath -Raw|ConvertFrom-Json
  if(@($closed.roles.PSObject.Properties|ForEach-Object{$_.Value}|ForEach-Object{$_}|Where-Object{$_.cycle_id -eq 'ci-r163r164' -and $null -eq $_.destroy_completed_at_utc}).Count -ne 0){throw 'ledger-record -Close left a cycle interval open.'}
  Write-Host 'PASS: ledger-record -Close closes exactly the helper-created cycle intervals.'
  $ledgerPath=Join-Path $temp 'ledger.json'
  $callLog=Join-Path $state 'calls.log';$callsBefore=(Get-Content $callLog).Count;$dry=Invoke-Tool 'pass' 'dry-run' $plan -DryRun;if($dry.ExitCode -ne 0){throw "Cycle-preflight dry-run failed: $($dry.Text)"};$dryRecord=Get-Content $dry.Out -Raw|ConvertFrom-Json;$callsAfter=(Get-Content $callLog).Count;if($dryRecord.classification -notmatch 'DRY RUN ONLY' -or $callsAfter -ne $callsBefore){throw 'Cycle-preflight dry-run called AWS or did not label itself review-only.'};foreach($bootstrap in $dryRecord.bootstrap_commands){$remote=$bootstrap.remote_lines -join "`n";$statusPrint=$remote.IndexOf('DUR050_CLOUD_INIT_STATUS_BEGIN');$statusGate=$remote.IndexOf('test "$cloud_init_rc" -eq 0');if($remote -notmatch 'cloud_init=\$\(cloud-init status --long 2>&1\) \|\| cloud_init_rc=\$\?' -or $statusPrint -lt 0 -or $statusGate -lt $statusPrint){throw "cloud-init status is not printed before its failure gate for $($bootstrap.role)."}};Write-Host 'PASS: dry-run emits status-preserving cloud-init checks before their failure gate, without AWS or SSM calls.'
  foreach($case in @(
    @{s='account';n='account';m='account'},
    @{s='quota';n='quota';m='quota'},
    @{s='budget';n='budget';m='notification'},
    @{s='budget-alarm-actual-160';n='alarm-actual-160';m='state is ALARM'},
    @{s='budget-alarm-actual-76';n='alarm-actual-76';m='state is ALARM'},
    @{s='budget-alarm-forecasted-160';n='alarm-forecasted-160';m='state is ALARM'},
    @{s='budget-missing-state';n='missing-notification-state';m='missing NotificationState'},
    @{s='budget-less-than';n='less-than-operator';m='ComparisonOperator'},
    @{s='tags';n='tags';m='cost-allocation tag'},
    @{s='bootstrap';n='bootstrap';m='Bootstrap gate failed'}
  )){
    $result=Invoke-Tool $case.s $case.n $plan; if($result.ExitCode -eq 0 -or $result.Text -notmatch $case.m){throw "Expected $($case.n) control to fail: $($result.Text)"}; $failureRecord=Get-Content $result.Out -Raw | ConvertFrom-Json; if($failureRecord.status -ne 'FAIL'){throw "$($case.n) did not record FAIL."}
    if($case.s -like 'budget-*'){
      $observed=@($failureRecord.checks.budget_notification_observations)
      if($observed.Count -ne 3){throw "$($case.n) FAIL record omitted observed notification values."}
      if($case.s -like 'budget-alarm-*' -and @($observed|Where-Object state -eq 'ALARM').Count -ne 1){throw "$($case.n) FAIL record did not preserve the observed ALARM state."}
      if($case.s -eq 'budget-missing-state' -and $null -ne $observed[1].state){throw 'Missing NotificationState was not recorded as missing.'}
      if($case.s -eq 'budget-less-than' -and $observed[2].comparison_operator -ne 'LESS_THAN'){throw 'Observed LESS_THAN operator was not preserved.'}
    }
    Write-Host "PASS: $($case.n) preflight control fails closed."
  }
  $expensive=$validOpenLedger|ConvertTo-Json -Depth 30|ConvertFrom-Json
  $expensive.prices_usd_per_hour.'c7i.large'=100
  $expensive.prices_usd_per_hour.'m7i.large'=100
  Write-Json $ledgerPath $expensive
  $result=Invoke-Tool 'pass' 'ledger-cap' $plan
  if($result.ExitCode -eq 0 -or $result.Text -notmatch 'cost-ledger'){throw 'Projected spend above the DUR-050 cap was accepted.'}
  if((Get-Content $result.Out -Raw|ConvertFrom-Json).status -ne 'FAIL'){throw 'Ledger-cap control did not leave a FAIL artifact.'}
  Write-Host 'PASS: explicit-reserve ledger over cap blocks D022 preflight.'
  $missing=Join-Path $temp 'missing-plan.json'; Write-Json $missing ([ordered]@{schema='fixture'})
  $result=Invoke-Tool 'pass' 'missing-peak' $missing; if($result.ExitCode -eq 0 -or $result.Text -notmatch 'planned_peak_vcpu'){throw 'Missing planned_peak_vcpu was accepted.'}; Write-Host 'PASS: missing planned_peak_vcpu is rejected.'
  . (Join-Path $PSScriptRoot 'dur050-d022-validator.ps1')
  $validPath=Join-Path $temp 'validator.json'; $valid=[ordered]@{schema='dur050-d022-preflight.v1';status='PASS';account_id='372206265946';region='us-west-1';planned_peak_vcpu=8;cycle_id='ci-cycle-r163r164';checked_at_utc=[DateTimeOffset]::UtcNow.ToString('o');ledger_check_path='ledger.json';reserve_minutes=60;ledger_check=[ordered]@{checked_at_utc=[DateTimeOffset]::UtcNow.ToString('o')}}
  foreach($case in @(@{name='cycle mismatch';edit='cycle';pattern='cycle_id'},@{name='stale preflight';edit='stale';pattern='stale'},@{name='string vCPU';edit='string';pattern='numeric planned_peak_vcpu'})){
    $copy=$valid|ConvertTo-Json -Depth 5|ConvertFrom-Json; if($case.edit -eq 'cycle'){$copy.cycle_id='wrong'}elseif($case.edit -eq 'stale'){$copy.checked_at_utc=[DateTimeOffset]::UtcNow.AddHours(-5).ToString('o')}else{$copy.planned_peak_vcpu='8'}; Write-Json $validPath $copy; $caught=$false; try{[void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164')}catch{if($_.Exception.Message -match $case.pattern){$caught=$true}else{throw}}; if(-not $caught){throw "Validator accepted $($case.name)."}; Write-Host "PASS: shared validator rejects $($case.name)."
  }
  $reserveNow=[DateTimeOffset]::UtcNow
  $valid.checked_at_utc=$reserveNow.AddMinutes(-10).ToString('o');Write-Json $validPath $valid
  [void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164' -Now $reserveNow -BlockDurationMinutes 30)
  Write-Host 'PASS: block freshness inside the recorded ledger reserve is accepted.'
  $valid.checked_at_utc=$reserveNow.AddMinutes(-40).ToString('o');Write-Json $validPath $valid
  $reserveRejected=$false;try{[void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164' -Now $reserveNow -BlockDurationMinutes 30)}catch{if($_.Exception.Message -match 'exceeds its ledger reserve'){$reserveRejected=$true}else{throw}}
  if(-not$reserveRejected){throw 'Preflight older than its reserve after adding the block duration was accepted.'}
  Write-Host 'PASS: block freshness beyond the recorded ledger reserve is rejected.'
  $valid.checked_at_utc=$reserveNow.AddMinutes(-1).ToString('o');$valid.ledger_check.checked_at_utc=$reserveNow.AddMinutes(-40).ToString('o');Write-Json $validPath $valid
  $ledgerReserveRejected=$false;try{[void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164' -Now $reserveNow -BlockDurationMinutes 30)}catch{if($_.Exception.Message -match 'exceeds its ledger reserve'){$ledgerReserveRejected=$true}else{throw}}
  if(-not$ledgerReserveRejected){throw 'A fresh top-level timestamp hid a ledger timestamp beyond the block reserve.'}
  Write-Host 'PASS: the ledger-check timestamp participates in reserve-age validation.'
  $valid.checked_at_utc=[DateTimeOffset]::UtcNow.ToString('o');$missingLedgerStamp=$valid|ConvertTo-Json -Depth 5|ConvertFrom-Json;$missingLedgerStamp.ledger_check.PSObject.Properties.Remove('checked_at_utc');Write-Json $validPath $missingLedgerStamp
  $ledgerTimestampMissing=$false;try{[void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164')}catch{if($_.Exception.Message -match 'ledger_check.checked_at_utc is required'){$ledgerTimestampMissing=$true}else{throw}}
  if(-not$ledgerTimestampMissing){throw 'Preflight without a ledger-check timestamp was accepted.'}
  Write-Host 'PASS: a missing ledger-check timestamp is rejected.'
  $valid.checked_at_utc='2026-09-26T04:00:00';$valid.ledger_check.checked_at_utc=[DateTimeOffset]::UtcNow.ToString('o');Write-Json $validPath $valid
  $offsetlessRecord=Get-Content $validPath -Raw|ConvertFrom-Json
  if($offsetlessRecord.checked_at_utc -isnot [DateTime] -or $offsetlessRecord.checked_at_utc.Kind -ne [DateTimeKind]::Unspecified){throw 'PowerShell did not parse the offset-less control as DateTime Kind=Unspecified.'}
  $offsetRejected=$false;try{[void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164')}catch{if($_.Exception.Message -match 'explicit UTC offset or Z'){$offsetRejected=$true}else{throw}}
  if(-not$offsetRejected){throw 'Offset-less DateTime Kind=Unspecified was accepted.'}
  Write-Host 'PASS: offset-less DateTime Kind=Unspecified is rejected.'
  $valid.checked_at_utc=[DateTimeOffset]::UtcNow.ToString('o')
  $valid.ledger_check.checked_at_utc=$valid.checked_at_utc
  foreach($badReserve in @('missing','zero')){
    $copy=$valid|ConvertTo-Json -Depth 5|ConvertFrom-Json
    if($badReserve -eq 'missing'){$copy.PSObject.Properties.Remove('reserve_minutes')}else{$copy.reserve_minutes=0}
    Write-Json $validPath $copy;$reserveFieldRejected=$false
    try{[void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164')}catch{if($_.Exception.Message -match 'reserve_minutes must be present and positive'){$reserveFieldRejected=$true}else{throw}}
    if(-not$reserveFieldRejected){throw "Validator accepted $badReserve reserve_minutes."}
  }
  Write-Host 'PASS: a missing or non-positive ledger reserve is rejected.'
} finally {
  Remove-Item Env:DUR050_SCENARIO,Env:DUR050_AWS_STATE,Env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE,Env:DUR050_BOOTSTRAP_ROOT -ErrorAction SilentlyContinue
  if(Test-Path $temp){Remove-Item $temp -Recurse -Force}
}
