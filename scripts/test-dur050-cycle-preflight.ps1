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
  *'ec2 describe-instances'*) printf '{"Reservations":[]}\n' ;;
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
function Invoke-Tool([string]$Scenario,[string]$Name,[string]$PlanPath,[switch]$DryRun,[string]$Timezone){
  $out=Join-Path $temp "$Name-preflight.json"; $ledgerOut=Join-Path $temp "$Name-ledger-check.json"
  $start=[Diagnostics.ProcessStartInfo]::new(); $start.FileName=Join-Path $PSHOME 'pwsh'; $start.UseShellExecute=$false; $start.RedirectStandardOutput=$true; $start.RedirectStandardError=$true
  $arguments=@('-NoProfile','-File',(Join-Path $PSScriptRoot 'dur050-cycle-preflight.ps1'),'-TerraformOutputsPath',(Join-Path $repoRoot 'tests/fixtures/dur050-terraform-outputs.json'),'-PlanInspectionPath',$PlanPath,'-CycleID','ci-r163r164','-CampaignManifestPath',$manifestPath,'-LedgerCheckPath',$ledgerOut,'-OutputPath',$out,'-ReserveMinutes','15','-LedgerPath',$ledgerPath,'-PythonExe','python3')
  if($DryRun){$arguments+=@('-DryRun','-DryRunOutputPath',$out)}
  foreach($arg in $arguments){$start.ArgumentList.Add($arg)}
  $start.Environment['PATH']="$bin`:/usr/bin:/bin"; $start.Environment['DUR050_SCENARIO']=$Scenario; $start.Environment['DUR050_AWS_STATE']=$state; $start.Environment['DUR050_ENABLE_TEST_LEDGER_OVERRIDE']='1'; $start.Environment['DUR050_BOOTSTRAP_ROOT']=$fakeRoot
  if($Timezone){$start.Environment['TZ']=$Timezone}
  $proc=[Diagnostics.Process]::new(); $proc.StartInfo=$start; [void]$proc.Start(); $stdout=$proc.StandardOutput.ReadToEnd(); $stderr=$proc.StandardError.ReadToEnd(); $proc.WaitForExit()
  return [pscustomobject]@{ExitCode=$proc.ExitCode;Out=$out;Ledger=$ledgerOut;Text=($stderr+"`n"+$stdout)}
}
try {
  # Temporary low-cost ledger: four open intervals, isolated from campaign accounting.
  $sourceLedger=Get-Content (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json') -Raw | ConvertFrom-Json
  $startTime=[DateTimeOffset]::UtcNow.AddMinutes(-5).ToString('yyyy-MM-ddTHH:mm:ssZ')
  foreach($role in @('app-1','app-2','dependency','load-generator')){$type=if($role -eq 'dependency'){'m7i.large'}else{'c7i.large'};$sourceLedger.roles.$role=@([ordered]@{cycle_id='ci-cycle-r163r164';instance_id=('i-'+[guid]::NewGuid().ToString('N').Substring(0,17));instance_type=$type;apply_started_at_utc=$startTime;destroy_completed_at_utc=$null})}
  $ledgerPath=Join-Path $temp 'ledger.json'; Write-Json $ledgerPath $sourceLedger
  $manifest=Get-Content (Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/cost-manifest.json') -Raw | ConvertFrom-Json
  $manifestPath=Join-Path $temp 'manifest.json'; Write-Json $manifestPath $manifest
  $fakeRoot=Join-Path $temp 'rootfs'; New-Item -ItemType Directory -Force -Path (Join-Path $fakeRoot 'var/lib'),(Join-Path $fakeRoot 'opt/durable-agent-execution-engine/bin')|Out-Null
  [IO.File]::WriteAllText((Join-Path $fakeRoot 'var/lib/durable-dur050-bootstrap-complete'),'')
  [IO.File]::WriteAllText((Join-Path $fakeRoot 'var/lib/durable-dur050-generator-bootstrap-complete'),'')
  foreach($exe in @('dur050-observer','dur050-loadgen','dur050-sink')){[IO.File]::WriteAllText((Join-Path $fakeRoot "opt/durable-agent-execution-engine/bin/$exe"),''); & chmod 0755 (Join-Path $fakeRoot "opt/durable-agent-execution-engine/bin/$exe")}
  $cloud=Join-Path $bin 'cloud-init'; [IO.File]::WriteAllText($cloud,"#!/bin/sh`nprintf 'status: done\nerrors: []\nrecoverable_errors: {}\n'`n",$utf8); & chmod 0755 $cloud
  $plan=Join-Path $repoRoot 'tests/fixtures/dur050-plan-inspection.json'
  $pass=Invoke-Tool 'pass' 'pass' $plan; if($pass.ExitCode -ne 0){throw "PASS fixture failed: $($pass.Text)"}
  $record=Get-Content $pass.Out -Raw | ConvertFrom-Json
  if($record.schema -ne 'dur050-d022-preflight.v1' -or $record.status -ne 'PASS' -or $record.account_id -ne '372206265946' -or $record.region -ne 'us-west-1' -or $record.planned_peak_vcpu -ne 8 -or $record.bootstrap.Count -ne 4 -or -not $record.ledger_check.checked_at_utc){throw 'PASS record omitted required D022 fields, ledger timestamp, or one of four bootstrap results.'}
  if($record.checks.budget.notifications.Count -ne 3 -or @($record.checks.budget.notifications|Where-Object {$_.state -ne 'OK' -or $_.comparison_operator -ne 'GREATER_THAN'}).Count -ne 0 -or $record.checks.budget.notifications[0].PSObject.Properties['subscriber_count']){throw 'Budget PASS record did not preserve observed notification states/operators without invented subscriber counts.'}
  if((Get-Content $pass.Ledger -Raw | ConvertFrom-Json).reserve_minutes -ne 15){throw 'PASS record omitted its explicit ledger reserve check.'}
  Write-Host 'PASS: D022 preflight stub checks account, quota, budget notifications, tags, ledger reserve and four bootstrap hosts.'
  foreach($zone in @('America/Los_Angeles','Asia/Shanghai')){
    $zoneName=$zone.Replace('/','-');$zonePass=Invoke-Tool 'pass' "timezone-$zoneName" $plan -Timezone $zone
    if($zonePass.ExitCode -ne 0){throw "PASS preflight failed under TZ=${zone}: $($zonePass.Text)"}
    $zoneRecord=Get-Content $zonePass.Out -Raw|ConvertFrom-Json
    if($zoneRecord.status -ne 'PASS'){throw "Timezone preflight under $zone left no valid PASS record."}
    Write-Host "PASS: cycle-preflight PASS fixture self-validates under TZ=$zone."
  }
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
  $expensive=$sourceLedger|ConvertTo-Json -Depth 30|ConvertFrom-Json
  foreach($role in @('app-1','app-2','dependency','load-generator')){foreach($interval in $expensive.roles.$role){$interval.apply_started_at_utc='2026-01-01T00:00:00Z';$interval.destroy_completed_at_utc=$null}}
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
