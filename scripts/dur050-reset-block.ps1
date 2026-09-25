[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$D022PreflightPath,
    [Parameter(Mandatory)] [string]$CampaignID,
    [Parameter(Mandatory)] [string]$BlockID,
    [Parameter(Mandatory)] [string]$DependencyInstanceID,
    [Parameter(Mandatory)] [string[]]$AppInstanceIDs,
    [Parameter(Mandatory)] [string]$GeneratorInstanceID,
    [Parameter(Mandatory)] [string]$DatabasePrivateIP,
    [Parameter(Mandatory)] [string]$DatabaseName,
    [Parameter(Mandatory)] [string]$ObserverSecretParameter,
    [Parameter(Mandatory)] [string]$PostgresBaselineArchive,
    [Parameter(Mandatory)] [string]$KafkaBaselineArchive,
    [Parameter(Mandatory)] [string]$WarmupScriptPath,
    [Parameter(Mandatory)] [string]$WarmupWorkflowIDsPath,
    [Parameter(Mandatory)] [string]$ObserverBinaryPath,
    [Parameter(Mandatory)] [string]$OutputDirectory,
    [ValidateSet("ON", "OFF")] [string]$AdmissionGateMode = "ON",
    [ValidateSet("0", "1")] [string]$TransactionTimingCapture = "0",
    [int]$TimeoutMinutes = 20
)

$ErrorActionPreference = "Stop"
$script:events = [System.Collections.Generic.List[object]]::new()
$script:commands = [System.Collections.Generic.List[object]]::new()
$script:status = "FAIL"
$script:createdOutputDirectory = $false

function Invoke-AwsJson([string[]]$Arguments) {
    $raw = & aws --region $script:awsRegion @Arguments
    if ($LASTEXITCODE -ne 0) { throw "AWS CLI failed: aws $($Arguments -join ' ')" }
    return ($raw | ConvertFrom-Json)
}

function Invoke-Ssm([string]$Stage, [string]$InstanceID, [string[]]$RemoteLines) {
    $started = [DateTime]::UtcNow
    $inputFile = Join-Path $OutputDirectory ("ssm-{0}-{1}.json" -f $Stage, $InstanceID)
    $body = @{ DocumentName = "AWS-RunShellScript"; InstanceIds = @($InstanceID); Comment = "DUR-050 $CampaignID/$BlockID $Stage"; Parameters = @{ commands = @(($RemoteLines -join "`n")) } } | ConvertTo-Json -Depth 8 -Compress
    [System.IO.File]::WriteAllText($inputFile, $body, (New-Object System.Text.UTF8Encoding -ArgumentList $false))
    try {
        $sent = Invoke-AwsJson @("ssm", "send-command", "--cli-input-json", "file://$inputFile", "--output", "json")
        $commandID = [string]$sent.Command.CommandId
        if (-not $commandID) { throw "SSM returned no command ID for $Stage/$InstanceID." }
        $deadline = [DateTime]::UtcNow.AddMinutes($TimeoutMinutes)
        $invocation = $null
        do {
            Start-Sleep -Seconds 2
            try { $invocation = Invoke-AwsJson @("ssm", "get-command-invocation", "--command-id", $commandID, "--instance-id", $InstanceID, "--output", "json") }
            catch { if ([DateTime]::UtcNow -ge $deadline) { throw }; continue }
            if ($invocation.Status -in @("Success", "Failed", "Cancelled", "TimedOut", "Undeliverable", "Terminated")) { break }
        } while ([DateTime]::UtcNow -lt $deadline)
        $stem = "$Stage-$InstanceID"
        if ($null -ne $invocation) {
            [System.IO.File]::WriteAllText((Join-Path $OutputDirectory "$stem.stdout.txt"), [string]$invocation.StandardOutputContent)
            [System.IO.File]::WriteAllText((Join-Path $OutputDirectory "$stem.stderr.txt"), [string]$invocation.StandardErrorContent)
            $script:commands.Add([pscustomobject]@{ stage = $Stage; instance_id = $InstanceID; command_id = $commandID; status = $invocation.Status; remote_lines = @($RemoteLines) })
        }
        if ($null -eq $invocation -or $invocation.Status -ne "Success") {
            $state = if ($null -eq $invocation) { "no response" } else { "$($invocation.Status): $($invocation.StandardErrorContent)" }
            $script:events.Add([pscustomobject]@{ stage = $Stage; instance_id = $InstanceID; started_at_utc = $started.ToString("o"); finished_at_utc = [DateTime]::UtcNow.ToString("o"); status = "FAIL"; error = $state })
            throw "SSM stage $Stage failed on ${InstanceID}: $state"
        }
        $script:events.Add([pscustomobject]@{ stage = $Stage; instance_id = $InstanceID; started_at_utc = $started.ToString("o"); finished_at_utc = [DateTime]::UtcNow.ToString("o"); status = "PASS" })
        return [string]$invocation.StandardOutputContent
    } finally {
        Remove-Item -LiteralPath $inputFile -Force -ErrorAction SilentlyContinue
    }
}

try {
    if ($AppInstanceIDs.Count -ne 2) { throw "Exactly two app hosts are required." }
    $allInstanceIDs = @($AppInstanceIDs) + @($DependencyInstanceID, $GeneratorInstanceID)
    if (($allInstanceIDs | Select-Object -Unique).Count -ne $allInstanceIDs.Count) { throw "App, dependency, and generator instance IDs must be distinct." }
    if ($CampaignID -notmatch '^[A-Za-z0-9-]+$' -or $BlockID -notmatch '^[A-Za-z0-9-]+$') { throw "CampaignID and BlockID must be simple identifiers." }
    foreach ($path in @($PostgresBaselineArchive, $KafkaBaselineArchive, $WarmupScriptPath, $WarmupWorkflowIDsPath, $ObserverBinaryPath)) {
        if ($path -notmatch '^/[A-Za-z0-9._/-]+$') { throw "Unsafe remote path: $path" }
    }
    if ($DatabasePrivateIP -notmatch '^[0-9.]+$' -or $DatabaseName -notmatch '^[A-Za-z0-9_]+$' -or $ObserverSecretParameter -notmatch '^/[A-Za-z0-9/_-]+$') { throw "Database or observer parameter input is malformed." }
    if (-not (Test-Path -LiteralPath $D022PreflightPath -PathType Leaf)) { throw "D022 preflight file is required." }
    $preflight = Get-Content -LiteralPath $D022PreflightPath -Raw | ConvertFrom-Json
    if ($preflight.status -ne "PASS") { throw "D022 preflight must have status PASS." }
    if ([string]$preflight.region -ne "us-west-1") { throw "D022 preflight region must be us-west-1." }
    $expectedAccount = [string]$preflight.account_id
    if (-not $expectedAccount) { $expectedAccount = [string]$preflight.account }
    if ($expectedAccount -ne "372206265946") { throw "D022 preflight account does not match the authorized account." }
    $peakVcpu = 0.0
    if ($null -eq $preflight.planned_peak_vcpu -or -not [double]::TryParse([string]$preflight.planned_peak_vcpu, [ref]$peakVcpu)) { throw "D022 preflight must record numeric planned_peak_vcpu." }
    if ($peakVcpu -le 0 -or $peakVcpu -gt 32) { throw "Planned concurrent vCPU must be in (0, 32]." }
    $script:awsRegion = [string]$preflight.region
    $admissionMaxActive = if ($AdmissionGateMode -eq "ON") { 1000 } else { 0 }
    $admissionMaxPendingOutbox = if ($AdmissionGateMode -eq "ON") { 50000 } else { 0 }
    if (Test-Path -LiteralPath $OutputDirectory) { throw "Refusing to overwrite existing evidence: $OutputDirectory" }
    New-Item -ItemType Directory -Path $OutputDirectory | Out-Null
    $script:createdOutputDirectory = $true
    $identity = Invoke-AwsJson @("sts", "get-caller-identity", "--output", "json")
    if ([string]$identity.Account -ne $expectedAccount) { throw "Active AWS account does not match D022 preflight." }

    $stopApps = @('set -euo pipefail', 'cd /opt/durable-agent-execution-engine', 'docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml -f deploy/aws/dur050-app-compose.yaml down')
    foreach ($id in $AppInstanceIDs) { [void](Invoke-Ssm "stop-app" $id $stopApps) }

    $restore = @(
        'set -euo pipefail',
        'cd /opt/durable-agent-execution-engine',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml down',
        'for volume in durable-aws-dependencies_postgres-data durable-aws-dependencies_kafka-data; do if docker volume inspect "$volume" >/dev/null 2>&1; then docker volume rm "$volume"; fi; docker volume create "$volume" >/dev/null; done',
        'postgres_image=$(sed -n ''s/^POSTGRES_IMAGE=//p'' deploy/aws/.env)',
        'postgres_user=$(sed -n ''s/^POSTGRES_USER=//p'' deploy/aws/.env)',
        'postgres_db=$(sed -n ''s/^POSTGRES_DB=//p'' deploy/aws/.env)',
        'test -n "$postgres_image"',
        'test -n "$postgres_user" && test -n "$postgres_db"',
        'test -s "' + $PostgresBaselineArchive + '" && test -s "' + $KafkaBaselineArchive + '"',
        'docker run --rm --user 0:0 -v durable-aws-dependencies_postgres-data:/restore -v "' + $PostgresBaselineArchive + ':/baseline.tar:ro" --entrypoint bash "$postgres_image" -ec ''tar -xpf /baseline.tar -C /restore''',
        'docker run --rm --user 0:0 -v durable-aws-dependencies_kafka-data:/restore -v "' + $KafkaBaselineArchive + ':/baseline.tar:ro" --entrypoint bash "$postgres_image" -ec ''tar -xpf /baseline.tar -C /restore''',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml up -d postgres kafka',
        'for attempt in $(seq 1 60); do if docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T postgres pg_isready -U "$postgres_user" -d "$postgres_db" >/dev/null 2>&1; then break; fi; if [ "$attempt" -eq 60 ]; then exit 1; fi; sleep 2; done',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml up -d kafka-init',
        'for attempt in $(seq 1 60); do init_id=$(docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml ps -aq kafka-init); init_status=$(docker inspect --format ''{{.State.Status}}:{{.State.ExitCode}}'' "$init_id" 2>/dev/null || true); if [ "$init_status" = ''exited:0'' ]; then break; fi; if [[ "$init_status" == exited:* && "$init_status" != ''exited:0'' ]]; then exit 1; fi; if [ "$attempt" -eq 60 ]; then exit 1; fi; sleep 2; done',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T postgres psql -v ON_ERROR_STOP=1 -U "$postgres_user" -d "$postgres_db" -Atc "SELECT max(version) FROM engine.schema_migrations" | grep -qx ''18''',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T postgres psql -v ON_ERROR_STOP=1 -U "$postgres_user" -d "$postgres_db" -Atc "SELECT current_setting(''shared_preload_libraries'') LIKE ''%pg_stat_statements%'' AND EXISTS (SELECT 1 FROM pg_extension WHERE extname=''pg_stat_statements'')" | grep -qx ''t'''
    )
    [void](Invoke-Ssm "restore-db-kafka-volumes" $DependencyInstanceID $restore)

    $configureApps = @(
        'set -euo pipefail',
        'cd /opt/durable-agent-execution-engine',
        'env_file=deploy/aws/.env',
        'set_env() { key="$1"; value="$2"; count=$(grep -c "^${key}=" "$env_file" || true); if [ "$count" -gt 1 ]; then echo "duplicate $key in $env_file" >&2; return 1; fi; if [ "$count" -eq 1 ]; then sed -i "s|^${key}=.*|${key}=${value}|" "$env_file"; else printf ''%s=%s\n'' "$key" "$value" >> "$env_file"; fi; }',
        'set_env DUR050_ADMISSION_MAX_ACTIVE ' + $admissionMaxActive,
        'set_env DUR050_ADMISSION_MAX_PENDING_OUTBOX ' + $admissionMaxPendingOutbox,
        'set_env DUR050_RECORD_TRANSACTION_TIMINGS ' + $TransactionTimingCapture,
        'set_env DUR049_RECORD_LEASE_ACQUISITIONS 0',
        'chmod 0600 "$env_file"',
        'grep -E ''^(DUR050_ADMISSION_MAX_ACTIVE|DUR050_ADMISSION_MAX_PENDING_OUTBOX|DUR050_RECORD_TRANSACTION_TIMINGS|DUR049_RECORD_LEASE_ACQUISITIONS)='' "$env_file"'
    )
    foreach ($id in $AppInstanceIDs) { [void](Invoke-Ssm "configure-admission-and-capture-mode" $id $configureApps) }

    $startApps = @(
        'set -euo pipefail',
        'cd /opt/durable-agent-execution-engine',
        '# The scheduler loop runs inside the runtime service; start it and every worker after dependency-volume restore.',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml -f deploy/aws/dur050-app-compose.yaml up -d --wait --no-build runtime worker',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml -f deploy/aws/dur050-app-compose.yaml ps -q runtime worker | xargs -r docker inspect --format ''{{.Id}} {{.Name}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}''',
        'runtime_id=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml -f deploy/aws/dur050-app-compose.yaml ps -q runtime)',
        'test -n "$runtime_id"',
        'runtime_env=$(docker inspect --format ''{{range .Config.Env}}{{println .}}{{end}}'' "$runtime_id")',
        'printf "%s\n" "$runtime_env" | grep -Fx ''DUR050_ADMISSION_MAX_ACTIVE=' + $admissionMaxActive + '''',
        'printf "%s\n" "$runtime_env" | grep -Fx ''DUR050_ADMISSION_MAX_PENDING_OUTBOX=' + $admissionMaxPendingOutbox + '''',
        'printf "%s\n" "$runtime_env" | grep -Fx ''DUR050_RECORD_TRANSACTION_TIMINGS=' + $TransactionTimingCapture + '''',
        'printf "%s\n" "$runtime_env" | grep -Fx ''DUR049_RECORD_LEASE_ACQUISITIONS=0''',
        'printf ''DUR050_EFFECTIVE_RUNTIME_SETTINGS\n%s\n'' "$runtime_env" | grep -E ''^(DUR050_ADMISSION_MAX_ACTIVE|DUR050_ADMISSION_MAX_PENDING_OUTBOX|DUR050_RECORD_TRANSACTION_TIMINGS|DUR049_RECORD_LEASE_ACQUISITIONS)='' '
    )
    foreach ($id in $AppInstanceIDs) { [void](Invoke-Ssm "restart-runtime-worker" $id $startApps) }

    $verifyGroup = @(
        'set -euo pipefail',
        'cd /opt/durable-agent-execution-engine',
        'for group in runtime-workers-v1 runtime-schedulers-v1; do',
        '  group_output=""',
        '  found_group=0',
        '  for attempt in $(seq 1 30); do',
        '    group_output=$(docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group "$group" --members --verbose 2>&1 || true)',
        '    if printf "%s\n" "$group_output" | awk -v expected="$group" ''NF >= 6 && $1 == expected && $2 != "-" && $5 ~ /^[0-9]+$/ && $5 > 0 && $6 != "-" { found = 1 } END { exit !found }''; then printf ''DUR050_CONSUMER_GROUP_ACTIVE %s\n%s\n'' "$group" "$group_output"; found_group=1; break; fi',
        '    sleep 2',
        '  done',
        '  if [ "$found_group" -ne 1 ]; then printf "%s\n" "$group_output" >&2; echo "Kafka consumer group $group did not show an assigned member after runtime/worker restart." >&2; exit 1; fi',
        'done'
    )
    [void](Invoke-Ssm "verify-worker-group-assignment" $DependencyInstanceID $verifyGroup)

    $warmup = @(
        'set -euo pipefail',
        'export DUR050_WARMUP_WORKFLOW_IDS_FILE="' + $WarmupWorkflowIDsPath + '"',
        'test -f "' + $WarmupScriptPath + '"',
        'bash "' + $WarmupScriptPath + '"',
        'test -s "' + $WarmupWorkflowIDsPath + '"',
        'warmup_count=$(awk ''NF {print $1}'' "' + $WarmupWorkflowIDsPath + '" | wc -l); warmup_unique=$(awk ''NF {print $1}'' "' + $WarmupWorkflowIDsPath + '" | sort -u | wc -l); test "$warmup_count" -eq 8 && test "$warmup_unique" -eq 8'
    )
    [void](Invoke-Ssm "submit-eight-warmups" $GeneratorInstanceID $warmup)

    $observerOutput = "/opt/durable-agent-execution-engine/dur050-$CampaignID-$BlockID-warmup-observer.csv"
    $drain = @('set -euo pipefail', 'observer_password=$(aws ssm get-parameter --name ''' + $ObserverSecretParameter + ''' --with-decryption --region us-west-1 --query Parameter.Value --output text)', 'export DUR050_OBSERVER_PASSWORD="$observer_password"', 'encoded_password=$(python3 -c ''import os, urllib.parse; print(urllib.parse.quote(os.environ["DUR050_OBSERVER_PASSWORD"], safe=""))'')', 'unset observer_password DUR050_OBSERVER_PASSWORD', 'export DUR050_OBSERVER_DATABASE_URL="postgresql://dur050_observer:${encoded_password}@' + $DatabasePrivateIP + ':5432/' + $DatabaseName + '?sslmode=disable"', 'unset encoded_password', 'test -x "' + $ObserverBinaryPath + '"', '"' + $ObserverBinaryPath + '" -mode batch -workflow-ids-file "' + $WarmupWorkflowIDsPath + '" -output "' + $observerOutput + '" -timeout 15m', 'printf ''DUR050_OBSERVER_CSV_GZIP_BASE64:''', 'gzip -c "' + $observerOutput + '" | base64 -w0', 'printf ''\\n''')
    $drainOutput = Invoke-Ssm "observe-warmup-drain" $GeneratorInstanceID $drain
    $encodedCSV = [regex]::Match($drainOutput, '(?m)^DUR050_OBSERVER_CSV_GZIP_BASE64:([A-Za-z0-9+/=]+)\s*$')
    if (-not $encodedCSV.Success) { throw "Warmup batch observer did not return its raw CSV artifact." }
    $compressedCSV = [Convert]::FromBase64String($encodedCSV.Groups[1].Value)
    $inputStream = [System.IO.MemoryStream]::new($compressedCSV)
    $gzipStream = [System.IO.Compression.GZipStream]::new($inputStream, [System.IO.Compression.CompressionMode]::Decompress)
    $outputStream = [System.IO.MemoryStream]::new()
    try {
        $gzipStream.CopyTo($outputStream)
        [System.IO.File]::WriteAllBytes((Join-Path $OutputDirectory "warmup-observer.csv"), $outputStream.ToArray())
    } finally {
        $outputStream.Dispose()
        $gzipStream.Dispose()
        $inputStream.Dispose()
    }

    $snapshotSql = @'
SELECT json_build_object(
  'database_size_bytes', pg_database_size(current_database()),
  'key_tables', json_build_object(
    'workflow_executions', json_build_object('rows', (SELECT count(*) FROM engine.workflow_executions), 'bytes', pg_total_relation_size('engine.workflow_executions')),
    'node_instances', json_build_object('rows', (SELECT count(*) FROM engine.node_instances), 'bytes', pg_total_relation_size('engine.node_instances')),
    'activity_attempts', json_build_object('rows', (SELECT count(*) FROM engine.activity_attempts), 'bytes', pg_total_relation_size('engine.activity_attempts')),
    'transition_history', json_build_object('rows', (SELECT count(*) FROM engine.transition_history), 'bytes', pg_total_relation_size('engine.transition_history')),
    'outbox', json_build_object('rows', (SELECT count(*) FROM engine.outbox), 'bytes', pg_total_relation_size('engine.outbox')),
    'event_inbox', json_build_object('rows', (SELECT count(*) FROM engine.event_inbox), 'bytes', pg_total_relation_size('engine.event_inbox')),
    'consumer_offsets', json_build_object('rows', (SELECT count(*) FROM engine.consumer_offsets), 'bytes', pg_total_relation_size('engine.consumer_offsets'))
  ),
  'consumer_offsets', COALESCE((SELECT json_agg(json_build_object('consumer_id', consumer_id, 'topic', topic, 'partition', kafka_partition, 'next_offset', next_offset) ORDER BY consumer_id, topic, kafka_partition) FROM engine.consumer_offsets), '[]'::json),
  'pg_stat_statements', json_build_object('entries', (SELECT count(*) FROM pg_stat_statements), 'calls', (SELECT COALESCE(sum(calls), 0) FROM pg_stat_statements), 'total_exec_time_ms', (SELECT COALESCE(sum(total_exec_time), 0) FROM pg_stat_statements), 'rows', (SELECT COALESCE(sum(rows), 0) FROM pg_stat_statements))
);
'@ -replace "`r?`n", ' '
    $snapshotSql = $snapshotSql.Trim()
    $snapshot = @('set -euo pipefail', 'cd /opt/durable-agent-execution-engine', 'postgres_user=$(sed -n ''s/^POSTGRES_USER=//p'' deploy/aws/.env)', 'postgres_db=$(sed -n ''s/^POSTGRES_DB=//p'' deploy/aws/.env)', 'test -n "$postgres_user" && test -n "$postgres_db"', 'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T postgres psql -v ON_ERROR_STOP=1 -U "$postgres_user" -d "$postgres_db" -Atc "' + $snapshotSql + '"', 'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group runtime-workers-v1')
    [void](Invoke-Ssm "snapshot-and-kafka-assignment" $DependencyInstanceID $snapshot)
    $script:status = "PASS"
} catch {
    $script:events.Add([pscustomobject]@{ stage = "reset-block"; status = "FAIL"; error = $_.Exception.Message; at_utc = [DateTime]::UtcNow.ToString("o") })
    throw
} finally {
    if ($script:createdOutputDirectory -and (Test-Path -LiteralPath $OutputDirectory)) {
        $artifact = [pscustomobject]@{
            schema = "dur050-reset-sequence.v1"
            campaign_id = $CampaignID
            block_id = $BlockID
            status = $script:status
            admission_gate_mode = $AdmissionGateMode
            admission_max_active = $admissionMaxActive
            admission_max_pending_outbox = $admissionMaxPendingOutbox
            transaction_timing_capture = $TransactionTimingCapture
            durable049_lease_acquisition_ledger = 0
            app_instance_ids = $AppInstanceIDs
            dependency_instance_id = $DependencyInstanceID
            generator_instance_id = $GeneratorInstanceID
            events = $script:events
            ssm_commands = $script:commands
            recorded_at_utc = [DateTime]::UtcNow.ToString("o")
        }
        [System.IO.File]::WriteAllText((Join-Path $OutputDirectory "reset-sequence.json"), ($artifact | ConvertTo-Json -Depth 12))
    }
}
