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
    if [ "${DUR050_SCENARIO:-pass}" = budget ]; then
      printf '{"Notifications":[{"NotificationType":"ACTUAL","ThresholdType":"ABSOLUTE_VALUE","Threshold":160,"Subscribers":[{"Address":"hidden"}]}]}\n'
    else
      printf '{"Notifications":[{"NotificationType":"ACTUAL","ThresholdType":"ABSOLUTE_VALUE","Threshold":160,"Subscribers":[{"Address":"hidden"}]},{"NotificationType":"ACTUAL","ThresholdType":"ABSOLUTE_VALUE","Threshold":76.47,"Subscribers":[{"Address":"hidden"}]},{"NotificationType":"FORECASTED","ThresholdType":"ABSOLUTE_VALUE","Threshold":160,"Subscribers":[{"Address":"hidden"}]}]}\n'
    fi ;;
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
function Invoke-Tool([string]$Scenario,[string]$Name,[string]$PlanPath,[switch]$DryRun){
  $out=Join-Path $temp "$Name-preflight.json"; $ledgerOut=Join-Path $temp "$Name-ledger-check.json"
  $start=[Diagnostics.ProcessStartInfo]::new(); $start.FileName=Join-Path $PSHOME 'pwsh'; $start.UseShellExecute=$false; $start.RedirectStandardOutput=$true; $start.RedirectStandardError=$true
  $arguments=@('-NoProfile','-File',(Join-Path $PSScriptRoot 'dur050-cycle-preflight.ps1'),'-TerraformOutputsPath',(Join-Path $repoRoot 'tests/fixtures/dur050-terraform-outputs.json'),'-PlanInspectionPath',$PlanPath,'-CycleID','ci-r163r164','-CampaignManifestPath',$manifestPath,'-LedgerCheckPath',$ledgerOut,'-OutputPath',$out,'-ReserveMinutes','15','-LedgerPath',$ledgerPath,'-PythonExe','python3')
  if($DryRun){$arguments+=@('-DryRun','-DryRunOutputPath',$out)}
  foreach($arg in $arguments){$start.ArgumentList.Add($arg)}
  $start.Environment['PATH']="$bin`:/usr/bin:/bin"; $start.Environment['DUR050_SCENARIO']=$Scenario; $start.Environment['DUR050_AWS_STATE']=$state; $start.Environment['DUR050_ENABLE_TEST_LEDGER_OVERRIDE']='1'; $start.Environment['DUR050_BOOTSTRAP_ROOT']=$fakeRoot
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
  if($record.schema -ne 'dur050-d022-preflight.v1' -or $record.status -ne 'PASS' -or $record.account_id -ne '372206265946' -or $record.region -ne 'us-west-1' -or $record.planned_peak_vcpu -ne 8 -or $record.bootstrap.Count -ne 4){throw 'PASS record omitted required D022 fields or one of four bootstrap results.'}
  if((Get-Content $pass.Ledger -Raw | ConvertFrom-Json).reserve_minutes -ne 15){throw 'PASS record omitted its explicit ledger reserve check.'}
  Write-Host 'PASS: D022 preflight stub checks account, quota, budget notifications, tags, ledger reserve and four bootstrap hosts.'
  $callLog=Join-Path $state 'calls.log';$callsBefore=(Get-Content $callLog).Count;$dry=Invoke-Tool 'pass' 'dry-run' $plan -DryRun;if($dry.ExitCode -ne 0){throw "Cycle-preflight dry-run failed: $($dry.Text)"};$dryRecord=Get-Content $dry.Out -Raw|ConvertFrom-Json;$callsAfter=(Get-Content $callLog).Count;if($dryRecord.classification -notmatch 'DRY RUN ONLY' -or $callsAfter -ne $callsBefore){throw 'Cycle-preflight dry-run called AWS or did not label itself review-only.'};Write-Host 'PASS: cycle-preflight dry run emits wrapped bootstrap commands without AWS or SSM calls.'
  foreach($case in @(@{s='account';n='account';m='account'},@{s='quota';n='quota';m='quota'},@{s='budget';n='budget';m='notification'},@{s='tags';n='tags';m='cost-allocation tag'},@{s='bootstrap';n='bootstrap';m='Bootstrap gate failed'})){
    $result=Invoke-Tool $case.s $case.n $plan; if($result.ExitCode -eq 0 -or $result.Text -notmatch $case.m){throw "Expected $($case.n) control to fail: $($result.Text)"}; if((Get-Content $result.Out -Raw | ConvertFrom-Json).status -ne 'FAIL'){throw "$($case.n) did not record FAIL."}; Write-Host "PASS: $($case.n) preflight control fails closed."
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
  $validPath=Join-Path $temp 'validator.json'; $valid=[ordered]@{schema='dur050-d022-preflight.v1';status='PASS';account_id='372206265946';region='us-west-1';planned_peak_vcpu=8;cycle_id='ci-cycle-r163r164';checked_at_utc=[DateTimeOffset]::UtcNow.ToString('o');ledger_check_path='ledger.json'}
  foreach($case in @(@{name='cycle mismatch';edit='cycle';pattern='cycle_id'},@{name='stale preflight';edit='stale';pattern='stale'},@{name='string vCPU';edit='string';pattern='numeric planned_peak_vcpu'})){
    $copy=$valid|ConvertTo-Json -Depth 5|ConvertFrom-Json; if($case.edit -eq 'cycle'){$copy.cycle_id='wrong'}elseif($case.edit -eq 'stale'){$copy.checked_at_utc=[DateTimeOffset]::UtcNow.AddHours(-5).ToString('o')}else{$copy.planned_peak_vcpu='8'}; Write-Json $validPath $copy; $caught=$false; try{[void](Read-Dur050D022Preflight -Path $validPath -CycleID 'ci-cycle-r163r164')}catch{if($_.Exception.Message -match $case.pattern){$caught=$true}else{throw}}; if(-not $caught){throw "Validator accepted $($case.name)."}; Write-Host "PASS: shared validator rejects $($case.name)."
  }
} finally {
  Remove-Item Env:DUR050_SCENARIO,Env:DUR050_AWS_STATE,Env:DUR050_ENABLE_TEST_LEDGER_OVERRIDE,Env:DUR050_BOOTSTRAP_ROOT -ErrorAction SilentlyContinue
  if(Test-Path $temp){Remove-Item $temp -Recurse -Force}
}
