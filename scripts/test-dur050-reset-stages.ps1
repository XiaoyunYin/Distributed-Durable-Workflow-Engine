$ErrorActionPreference='Stop'
if(-not $IsLinux){throw 'DUR-050 reset-stage tests require Ubuntu/Linux and dash.'}
$repoRoot=Split-Path -Parent $PSScriptRoot; $temp=Join-Path ([IO.Path]::GetTempPath()) ('dur050-reset-stage-'+[guid]::NewGuid().ToString('N')); $bin=Join-Path $temp 'bin'; $state=Join-Path $temp 'aws-state'; $archiveRoot=Join-Path $temp 'archive-source'; $remote=Join-Path $temp 'remote-repo'; $utf8=[Text.UTF8Encoding]::new($false)
New-Item -ItemType Directory -Force -Path $bin,$state,$archiveRoot,(Join-Path $remote 'deploy/aws'),(Join-Path $temp 'remote-scripts')|Out-Null
$stubSource=Join-Path $PSScriptRoot 'test-support/dur050-argv-stub.sh'; $awsSource=Join-Path $PSScriptRoot 'test-support/dur050-aws-stub.py'
$stubText=(Get-Content -LiteralPath $stubSource -Raw).Replace("`r",'');$awsText=(Get-Content -LiteralPath $awsSource -Raw).Replace("`r",'')
foreach($tool in @('docker','psql','grep','sha256sum','bash','kafka-topics.sh','kafka-consumer-groups.sh','cloud-init')){[IO.File]::WriteAllText((Join-Path $bin $tool),$stubText,$utf8); & chmod 0755 (Join-Path $bin $tool)}
$bashStub=@'
#!/bin/sh
set -eu
if [ -n "${DUR050_ARGV_LOG:-}" ]; then
  python3 -c 'import json,os,sys; open(os.environ["DUR050_ARGV_LOG"],"a",encoding="utf-8").write(json.dumps(["bash",*sys.argv[1:]])+"\n")' "$@"
fi
exec /bin/bash "$@"
'@
[IO.File]::WriteAllText((Join-Path $bin 'bash'),$bashStub.Replace("`r",'').TrimStart()+"`n",$utf8); & chmod 0755 (Join-Path $bin 'bash')
[IO.File]::WriteAllText((Join-Path $bin 'aws'),$awsText,$utf8); & chmod 0755 (Join-Path $bin 'aws')
$envFile=Join-Path $remote 'deploy/aws/.env'; [IO.File]::WriteAllText($envFile,"POSTGRES_USER=durable`nPOSTGRES_DB=durable`nPOSTGRES_IMAGE=postgres:test`n",$utf8)
$argvLog=Join-Path $temp 'argv.jsonl'; $dockerLog=Join-Path $temp 'docker.jsonl'; $observerLog=Join-Path $temp 'observer-dsn.txt'
foreach($name in @('postgres-data','kafka-data')){ $source=Join-Path $archiveRoot ($name+'.source'); [IO.File]::WriteAllText($source,"fixture-$name",$utf8); $p=Start-Process -FilePath /bin/tar -ArgumentList @('-cf',(Join-Path $archiveRoot ($name+'.tar')),'-C',$archiveRoot,($name+'.source')) -Wait -PassThru -NoNewWindow; if($p.ExitCode){throw "Could not create $name archive fixture."} }
$cycle='cycle-ci-'+[guid]::NewGuid().ToString('N').Substring(0,8); $api='http://127.0.0.1:8080/'; $namespace="dur050-pilot-$cycle"
$warmup=Join-Path $temp 'remote-scripts/dur050-warmup.sh'; [IO.File]::WriteAllText($warmup,"#!/usr/bin/env bash`nset -euo pipefail`n: > `"`$DUR050_WARMUP_WORKFLOW_IDS_FILE`"`nfor n in `$(seq 1 8); do printf 'wf-%02d\n' `"`$n`" >> `"`$DUR050_WARMUP_WORKFLOW_IDS_FILE`"; done`n",$utf8); & chmod 0755 $warmup
$observer=Join-Path $temp 'remote-scripts/dur050-observer'; $observerScript=@'
#!/usr/bin/env bash
set -euo pipefail
printf '%s' "${DUR050_OBSERVER_DATABASE_URL:-missing}" > "$DUR050_TEST_OBSERVER_LOG"
output=''
while (($#)); do if [[ "$1" == -output ]]; then output="$2"; shift 2; else shift; fi; done
printf 'workflow_id,state\nwf-01,SUCCEEDED\n' > "$output"
'@
[IO.File]::WriteAllText($observer,$observerScript.Replace("`r",'')+"`n",$utf8); & chmod 0755 $observer
$processEnv=@{PATH="$bin`:/usr/bin:/bin";DUR050_ARGV_LOG=$argvLog;DUR050_DOCKER_LOG=$dockerLog;DUR050_AWS_STATE=$state;DUR050_REMOTE_REPO_ROOT=$remote;DUR050_TEST_ARCHIVE_ROOT=$archiveRoot;DUR050_TEST_API_URL=$api;DUR050_TEST_NAMESPACE=$namespace;DUR050_TEST_OBSERVER_LOG=$observerLog;DUR050_AWS_SCENARIO='pass'}
function Invoke-Child([string]$ScriptPath,[string[]]$Arguments,[hashtable]$Environment){$start=[Diagnostics.ProcessStartInfo]::new();$start.FileName=Join-Path $PSHOME 'pwsh';$start.UseShellExecute=$false;$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true;$start.ArgumentList.Add('-NoProfile');$start.ArgumentList.Add('-File');$start.ArgumentList.Add($ScriptPath);foreach($arg in $Arguments){$start.ArgumentList.Add($arg)};foreach($key in $Environment.Keys){$start.Environment[$key]=[string]$Environment[$key]};$proc=[Diagnostics.Process]::new();$proc.StartInfo=$start;[void]$proc.Start();$out=$proc.StandardOutput.ReadToEnd();$err=$proc.StandardError.ReadToEnd();$proc.WaitForExit();[pscustomobject]@{ExitCode=$proc.ExitCode;Stdout=$out;Stderr=$err}}
function Invoke-Dash([string]$Command,[hashtable]$Environment){$start=[Diagnostics.ProcessStartInfo]::new();$start.FileName='/bin/sh';$start.UseShellExecute=$false;$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true;$start.ArgumentList.Add('-c');$start.ArgumentList.Add($Command);foreach($key in $Environment.Keys){$start.Environment[$key]=[string]$Environment[$key]};$proc=[Diagnostics.Process]::new();$proc.StartInfo=$start;[void]$proc.Start();$out=$proc.StandardOutput.ReadToEnd();$err=$proc.StandardError.ReadToEnd();$proc.WaitForExit();[pscustomobject]@{ExitCode=$proc.ExitCode;Stdout=$out;Stderr=$err}}
function Get-Body([string]$Wrapper){$match=[regex]::Match($Wrapper,"printf '%s' '([A-Za-z0-9+/=]+)' \| base64 -d");if(-not $match.Success){throw 'Could not extract the SSM wrapper payload.'};[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($match.Groups[1].Value))}
function Assert-ChildPass($Result,[string]$Label){if($Result.ExitCode -ne 0){$stateDetails='';if(Test-Path $state){$stateDetails=(Get-ChildItem $state -File|ForEach-Object {"$($_.Name): $((Get-Content $_.FullName -Raw).Trim())"}) -join "`n"};throw "$Label failed ($($Result.ExitCode)): $($Result.Stderr) $($Result.Stdout) SSM stub state: $stateDetails"}}
try{
  $d022=Join-Path $temp 'd022.json'; Write-Output ''|Out-Null
  $stamp=[DateTimeOffset]::UtcNow.ToString('o');[IO.File]::WriteAllText($d022,(@{schema='dur050-d022-preflight.v1';status='PASS';account_id='372206265946';region='us-west-1';planned_peak_vcpu=8;cycle_id=$cycle;checked_at_utc=$stamp;ledger_check_path='ledger.json';reserve_minutes=60;ledger_check=@{checked_at_utc=$stamp}}|ConvertTo-Json -Depth 5),$utf8)
  $campaign=Join-Path $temp 'campaign'; $prepareArgs=@('-TerraformOutputsPath',(Join-Path $repoRoot 'tests/fixtures/dur050-terraform-outputs.json'),'-CycleID',$cycle,'-D022PreflightPath',$d022,'-TestOutputRoot',$campaign)
  $prepareEnv=@{}+$processEnv; $prepareEnv['DUR050_ENABLE_TEST_HOOKS']='1'
  $prepared=Invoke-Child (Join-Path $PSScriptRoot 'dur050-prepare-pilot.ps1') $prepareArgs $prepareEnv; Assert-ChildPass $prepared 'end-to-end prepare-pilot'
  $baseline=Join-Path $campaign "cycles/$cycle/baseline-manifest.json"; if(-not(Test-Path $baseline)){throw 'prepare-pilot did not create baseline-manifest.json.'}
  $hashes=@{};foreach($name in @('postgres-data','kafka-data')){$source=Join-Path $archiveRoot ($name+'.tar');$hash=(Get-FileHash $source -Algorithm SHA256).Hash.ToLowerInvariant();$hashes[$name]=$hash}
  $planBase=Join-Path $temp 'plan-base';$warmupIDs=Join-Path $temp 'remote-scripts/warmup-ids.txt';$output=$null
  $resetScript=Join-Path $PSScriptRoot 'dur050-reset-block.ps1'
  $resetArgs=@('-D022PreflightPath',$d022,'-CampaignID','pilot-ci','-CycleID',$cycle,'-BlockID','reset-ci','-DependencyInstanceID','i-0123456789abcdef0','-AppInstanceIDs','i-11111111111111111,i-22222222222222222','-GeneratorInstanceID','i-33333333333333333','-DatabasePrivateIP','10.49.1.11','-DatabaseName','durable','-ObserverSecretParameter','/dur050/observer/password','-BaselineManifestPath',$baseline,'-WarmupScriptPath',$warmup,'-WarmupWorkflowIDsPath',$warmupIDs,'-ObserverBinaryPath',$observer,'-OutputDirectory',$planBase,'-BlockDurationMinutes','30','-StagePlanOnly')
  $stageEnv=@{}+$processEnv
  $planResult=Invoke-Child $resetScript $resetArgs $stageEnv;Assert-ChildPass $planResult 'reset StagePlanOnly';$stages=@($planResult.Stdout|ConvertFrom-Json)
  if($stages.Count -ne 11 -or @($stages.stage|Select-Object -Unique).Count -ne 8){throw "Expected 11 dispatches and eight stage kinds; found $($stages.Count)/$(@($stages.stage|Select-Object -Unique).Count)."}
  foreach($mode in @('ON','OFF')){
    $remoteEnv=@{}+$processEnv;$remoteEnv['DUR050_REMOTE_REPO_ROOT']=$remote;$remoteEnv['DUR050_TEST_ARCHIVE_ROOT']=$archiveRoot
    [IO.File]::WriteAllText($envFile,"POSTGRES_USER=durable`nPOSTGRES_DB=durable`nPOSTGRES_IMAGE=postgres:test`n",$utf8)
    $modeArgs=@($resetArgs);$modeOutputIndex=[Array]::IndexOf($modeArgs,'-OutputDirectory');$modeArgs[$modeOutputIndex+1]=Join-Path $temp "plan-$mode";$modeArgs+=@('-AdmissionGateMode',$mode)
    $modePlanResult=Invoke-Child $resetScript $modeArgs $remoteEnv;Assert-ChildPass $modePlanResult "StagePlanOnly gate $mode";$modeStages=@($modePlanResult.Stdout|ConvertFrom-Json)
    foreach($stage in $modeStages){
      $body=Get-Body $stage.wrapped_command;$bodyPath=Join-Path $temp ('body-'+$mode+'-'+$stage.stage+'.sh');[IO.File]::WriteAllText($bodyPath,$body,$utf8)
      $syntax=Start-Process -FilePath /bin/bash -ArgumentList @('-n',$bodyPath) -Wait -PassThru -NoNewWindow;if($syntax.ExitCode){throw "bash -n failed for $mode/$($stage.stage)."}
      $result=Invoke-Dash $stage.wrapped_command $remoteEnv; if($result.ExitCode -ne 0){throw "Wrapped real stage $mode/$($stage.stage) failed under dash: $($result.Stderr) $($result.Stdout)"}
      $removed=[regex]::Match($result.Stdout,'DUR050_SSM_TEMP_REMOVED=([^\r\n]+)');if(-not $removed.Success -or (Test-Path $removed.Groups[1].Value.Trim())){throw "SSM temp cleanup failed for $mode/$($stage.stage); marker=[$($removed.Groups[1].Value)] exists=$(Test-Path $removed.Groups[1].Value.Trim()); output=[$($result.Stdout)]"}
    }
    $gateActive=if($mode -eq 'ON'){'1000'}else{'0'};$gateOutbox=if($mode -eq 'ON'){'50000'}else{'0'}
    $configBody=$modeStages|Where-Object stage -eq 'configure-admission-and-capture-mode'|Select-Object -First 1
    foreach($line in @("set_env DUR050_ADMISSION_MAX_ACTIVE $gateActive","set_env DUR050_ADMISSION_MAX_PENDING_OUTBOX $gateOutbox","set_env DUR050_RECORD_TRANSACTION_TIMINGS 0")){if($configBody.remote_lines -notcontains $line){throw "Missing exact set_env argv line: $line"}}
    $startBody=$modeStages|Where-Object stage -eq 'restart-runtime-worker'|Select-Object -First 1
    foreach($pattern in @("DUR050_ADMISSION_MAX_ACTIVE=$gateActive","DUR050_ADMISSION_MAX_PENDING_OUTBOX=$gateOutbox","DUR050_RECORD_TRANSACTION_TIMINGS=0")){if(@($startBody.remote_lines|Where-Object {$_ -match ('grep -Fx.*'+[regex]::Escape($pattern))}).Count -ne 1){throw "Missing single-line grep -Fx pattern: $pattern"}}
    if(-not($modeStages|Where-Object { $_.stage -eq 'submit-eight-warmups' -and ($_.remote_lines -match 'export DUR050_LOADGEN_CONFIG_FILE=') -and ($_.remote_lines -match '^bash ".*dur050-warmup.sh"$') })){throw 'Warmup config export or bash <script> invocation is missing.'}
    $drain=$modeStages|Where-Object stage -eq 'observe-warmup-drain'|Select-Object -First 1;$drainText=$drain.remote_lines -join "`n";if($drainText -notmatch "ssm get-parameter --name '/dur050/observer/password'" -or $drainText -notmatch 'DUR050_OBSERVER_DATABASE_URL="postgresql://dur050_observer:\$\{encoded_password\}@10\.49\.1\.11:5432/durable\?sslmode=disable"'){throw 'Drain parameter name or observer DSN is not exact.'}
    $restore=$modeStages|Where-Object stage -eq 'restore-db-kafka-volumes'|Select-Object -First 1;if(-not($restore.remote_lines|Where-Object {$_ -match '-v ".*postgres-data\.tar:/baseline\.tar:ro"'})){throw 'Restore mount argv line was split or malformed.'}
    Write-Host "PASS: eight generated reset stage kinds (11 dispatches), bash -n and dash wrapper execution for gate $mode."
  }
  $dockerRows=@(Get-Content $dockerLog|ForEach-Object { ,($_|ConvertFrom-Json) });$restoreRow=$dockerRows|Where-Object {$_[0] -eq 'docker' -and (($_ -join ' ') -match 'postgres-data\.tar:/baseline\.tar:ro')}|Select-Object -First 1;if(-not $restoreRow){throw "Positive matching-hash restore did not reach docker with -v <archive>:/baseline.tar:ro. Captured argv: $(Get-Content $dockerLog -Raw)"};$mountIndex=-1;for($i=0;$i -lt $restoreRow.Count;$i++){if([string]$restoreRow[$i] -match 'postgres-data\.tar:/baseline\.tar:ro'){$mountIndex=$i;break}};if($mountIndex -lt 1 -or $restoreRow[$mountIndex-1] -ne '-v'){throw 'Restore archive mount argv is not the exact adjacent pair -v <archive>:/baseline.tar:ro.'}
  $argvRows=@(Get-Content $argvLog|ForEach-Object { ,($_|ConvertFrom-Json) });foreach($mode in @('ON','OFF')){$active=if($mode -eq 'ON'){'1000'}else{'0'};$pending=if($mode -eq 'ON'){'50000'}else{'0'};foreach($pattern in @("DUR050_ADMISSION_MAX_ACTIVE=$active","DUR050_ADMISSION_MAX_PENDING_OUTBOX=$pending","DUR050_RECORD_TRANSACTION_TIMINGS=0")){if(-not($argvRows|Where-Object {$_[0] -eq 'grep' -and $_ -contains '-Fx' -and $_ -contains $pattern})){throw "grep -Fx argv did not include exact $mode pattern $pattern. Captured argv: $(Get-Content $argvLog -Raw)"}}}
  if((Get-Content $observerLog -Raw) -notmatch '^postgresql://dur050_observer:ci-observer-password@10\.49\.1\.11:5432/durable\?sslmode=disable$'){throw 'Observer executable did not receive the exact expected DSN.'}
  if(-not($argvRows|Where-Object {$_[0] -eq 'bash' -and $_ -contains $warmup})){throw 'argv log does not show bash <warmup script>.'}
  $awsRows=@(Get-Content $argvLog|ForEach-Object { ,($_|ConvertFrom-Json) });if(-not($awsRows|Where-Object {$_[0] -eq 'aws' -and $_ -contains '/dur050/observer/password'})){throw 'Observer SSM parameter name was not recorded in AWS argv.'}
  if(-not($argvRows|Where-Object {$_[0] -eq 'psql' -and $_ -contains '-Atc' -and ($_ -join ' ') -match 'SELECT json_build_object'})){throw 'PostgreSQL guard SQL argv was not captured by the psql stub.'}
  foreach($tool in @('sha256sum','kafka-topics.sh','kafka-consumer-groups.sh')){if(-not($argvRows|Where-Object {$_[0] -eq $tool})){throw "Expected tool argv was not captured by the $tool stub."}}
  # Run the complete orchestration path: prepare creates the baseline manifest, then reset dispatches all 11 stages via the stubbed AWS SSM API.
  $resetOutput=Join-Path $temp 'reset-e2e';$fullArgs=@($resetArgs|Where-Object {$_ -ne '-StagePlanOnly'});$index=[Array]::IndexOf($fullArgs,'-OutputDirectory');$fullArgs[$index+1]=$resetOutput;$fullArgs+=@('-AdmissionGateMode','ON')
  $endToEnd=Invoke-Child $resetScript $fullArgs $processEnv;Assert-ChildPass $endToEnd 'reset end-to-end orchestration';$artifact=Get-Content (Join-Path $resetOutput 'reset-sequence.json') -Raw|ConvertFrom-Json;if($artifact.status -ne 'PASS' -or $artifact.ssm_commands.Count -ne 11){throw 'E2E reset did not dispatch all 11 SSM stages to PASS.'}
  Write-Host 'PASS: stubbed AWS send-command/get-command-invocation drove prepare-pilot through baseline-manifest.json and reset through all 11 stages to PASS.'
  # Negative control: remove the restore-mount parentheses in an isolated script copy; stage generation must reject the split fragments.
  $mutantDir=Join-Path $temp 'mutant-scripts';New-Item -ItemType Directory -Path $mutantDir|Out-Null;foreach($name in @('dur050-reset-block.ps1','dur050-ssm-wrapper.ps1','dur050-baseline-manifest.ps1','dur050-d022-validator.ps1')){Copy-Item (Join-Path $PSScriptRoot $name) (Join-Path $mutantDir $name)}
  $mutantPath=Join-Path $mutantDir 'dur050-reset-block.ps1';$source=Get-Content $mutantPath -Raw;$needle="        ('docker run --rm --user 0:0 -v durable-aws-dependencies_postgres-data:/restore -v `"' + `$PostgresBaselineArchive + ':/baseline.tar:ro`" --entrypoint bash `"`$postgres_image`" -ec ''tar -xpf /baseline.tar -C /restore'''),";$replacement="        'docker run --rm --user 0:0 -v durable-aws-dependencies_postgres-data:/restore -v `"' + `$PostgresBaselineArchive + ':/baseline.tar:ro`" --entrypoint bash `"`$postgres_image`" -ec ''tar -xpf /baseline.tar -C /restore''',";if(-not $source.Contains($needle)){throw 'Mutation fixture did not locate the first restore-mount parenthesis.'};[IO.File]::WriteAllText($mutantPath,$source.Replace($needle,$replacement),$utf8)
  $mutantOut=Join-Path $temp 'mutant-plan';$mutantArgs=@($resetArgs);$mutantIndex=[Array]::IndexOf($mutantArgs,'-OutputDirectory');$mutantArgs[$mutantIndex+1]=$mutantOut
  $mutant=Invoke-Child $mutantPath $mutantArgs $stageEnv;$mutationDetected=$mutant.ExitCode -ne 0
  if(-not $mutationDetected){$mutantRows=@($mutant.Stdout|ConvertFrom-Json);$mutantRestore=$mutantRows|Where-Object stage -eq 'restore-db-kafka-volumes'|Select-Object -First 1;$expectedMounts=@($mutantRestore.remote_lines|Where-Object {$_ -match '-v ".*(postgres|kafka)-data\.tar:/baseline\.tar:ro"'}).Count -eq 2;$mutationDetected=-not $expectedMounts}
  if(-not $mutationDetected){throw 'Removing the restore-mount parentheses survived stage generation and argv-shape checks.'}
  if($mutant.ExitCode -ne 0 -and (($mutant.Stderr + $mutant.Stdout) -match 'split quote/colon fragment')){
    Write-Host 'NEGATIVE CONTROL: SSM wrapper fragment guard rejected the malformed restore line.'
  } elseif ($mutant.ExitCode -eq 0) {
    Write-Host 'NEGATIVE CONTROL: restore-mount argv-shape assertion rejected the split mount.'
  } else {
    $mutantRows=@($mutant.Stdout|ConvertFrom-Json);$mutantRestore=$mutantRows|Where-Object stage -eq 'restore-db-kafka-volumes'|Select-Object -First 1
    $expectedMounts=@($mutantRestore.remote_lines|Where-Object {$_ -match '-v ".*(postgres|kafka)-data\.tar:/baseline\.tar:ro"'}).Count -eq 2
    if($expectedMounts){throw "Restore mutation failed for an unrelated reason: $($mutant.Stderr) $($mutant.Stdout)"}
    Write-Host 'NEGATIVE CONTROL: restore-mount argv-shape assertion rejected the split mount.'
  }

  # Malform only the final planned stage. Every stage is wrapped before the first SSM dispatch.
  $lastMutantDir=Join-Path $temp 'last-stage-mutant';New-Item -ItemType Directory -Path $lastMutantDir|Out-Null
  foreach($name in @('dur050-reset-block.ps1','dur050-ssm-wrapper.ps1','dur050-baseline-manifest.ps1','dur050-d022-validator.ps1')){Copy-Item (Join-Path $PSScriptRoot $name) (Join-Path $lastMutantDir $name)}
  $lastMutantPath=Join-Path $lastMutantDir 'dur050-reset-block.ps1';$lastSource=Get-Content $lastMutantPath -Raw
  $lastNeedle="    [void]`$stages.Add([pscustomobject]@{ stage = 'snapshot-and-kafka-assignment'"
  $lastReplacement='$snapshot += @([string][char]34)' + "`n" + $lastNeedle
  if(-not $lastSource.Contains($lastNeedle)){throw 'Could not inject malformed quote into the final snapshot stage.'}
  [IO.File]::WriteAllText($lastMutantPath,$lastSource.Replace($lastNeedle,$lastReplacement),$utf8)
  $lastArgs=@($fullArgs);$lastOutIndex=[Array]::IndexOf($lastArgs,'-OutputDirectory');$lastArgs[$lastOutIndex+1]=Join-Path $temp 'last-stage-malformed-output'
  $beforeCalls=@(Get-Content $argvLog)
  $lastStageResult=Invoke-Child $lastMutantPath $lastArgs $processEnv
  $lastFailureText=($lastStageResult.Stderr+$lastStageResult.Stdout) -replace '\s+',' '
  if($lastStageResult.ExitCode -eq 0 -or $lastFailureText -notmatch 'split quote/colon'){throw "Malformed final stage was not rejected by the wrapper guard: $($lastStageResult.Stderr) $($lastStageResult.Stdout)"}
  $afterCalls=@(Get-Content $argvLog);$newCalls=@($afterCalls|Select-Object -Skip $beforeCalls.Count)
  if(@($newCalls|Where-Object {$_ -match 'ssm send-command'}).Count -ne 0){throw "Malformed final stage was discovered after an SSM dispatch: $($newCalls -join '; ')"}
  Write-Host 'PASS: malformed final stage is rejected before any stubbed SSM send-command.'
} finally {Remove-Item Env:DUR050_ARGV_LOG,Env:DUR050_DOCKER_LOG,Env:DUR050_AWS_STATE,Env:DUR050_REMOTE_REPO_ROOT,Env:DUR050_TEST_ARCHIVE_ROOT,Env:DUR050_TEST_API_URL,Env:DUR050_TEST_NAMESPACE,Env:DUR050_TEST_OBSERVER_LOG,Env:DUR050_AWS_SCENARIO,Env:DUR050_ENABLE_TEST_HOOKS -ErrorAction SilentlyContinue;if(Test-Path $temp){Remove-Item $temp -Recurse -Force};foreach($path in @("/var/tmp/dur050-$cycle","/var/tmp/dur050-$cycle-baseline")){if(Test-Path $path){Remove-Item $path -Recurse -Force}}}
