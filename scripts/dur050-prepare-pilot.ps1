[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$TerraformOutputsPath,
    [Parameter(Mandatory)] [ValidatePattern('^[A-Za-z0-9-]{1,32}$')] [string]$CycleID,
    [string]$D022PreflightPath,
    [string]$FrozenConfigPath,
    [string]$TestOutputRoot,
    [switch]$DryRun,
    [string]$DryRunOutputPath,
    [int]$TimeoutMinutes = 20
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'dur050-ssm-wrapper.ps1')
. (Join-Path $PSScriptRoot 'dur050-d022-validator.ps1')
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if ($TestOutputRoot -and $env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'TestOutputRoot is test-only and requires DUR050_ENABLE_TEST_HOOKS=1.' }
$campaignRoot = if ($TestOutputRoot) { [System.IO.Path]::GetFullPath($TestOutputRoot) } else { Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f' }
$cycleDirectory = Join-Path $campaignRoot ("cycles/{0}" -f $CycleID)
$utf8 = [System.Text.UTF8Encoding]::new($false)
if ([string]::IsNullOrWhiteSpace($FrozenConfigPath)) {
    $FrozenConfigPath = Join-Path $repoRoot 'experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/pilot-calibration/frozen-config.json'
}

function Get-OutputValue($Outputs, [string]$Name) {
    if ($null -eq $Outputs.$Name -or $null -eq $Outputs.$Name.value) {
        throw "Terraform output '$Name' is missing or has no value."
    }
    return $Outputs.$Name.value
}

function ConvertTo-BashSingleQuoted([string]$Value) {
    $quote = [string][char]39 + [string][char]34 + [string][char]39 + [string][char]34 + [string][char]39
    return [string][char]39 + $Value.Replace([string][char]39, $quote) + [string][char]39
}

function Write-JsonFile([string]$Path, $Value) {
    $json = $Value | ConvertTo-Json -Depth 30
    [System.IO.File]::WriteAllText($Path, $json + [Environment]::NewLine, $utf8)
}

function Get-Sha256Hex([byte[]]$Bytes) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($sha.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Read-Marker([string]$Text, [string]$Name) {
    $match = [regex]::Match($Text, '(?m)^' + [regex]::Escape($Name) + '=([A-Za-z0-9+/=]+)\s*$')
    if (-not $match.Success) { throw "Remote stage omitted marker $Name." }
    return [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($match.Groups[1].Value))
}

function Get-StageDefinitions($Frozen, $Outputs) {
    $appIDs = @(Get-OutputValue $Outputs 'app_instance_ids')
    $appIPs = @(Get-OutputValue $Outputs 'app_private_ips')
    $generatorIDs = @(Get-OutputValue $Outputs 'load_generator_instance_ids')
    $dependencyID = [string](Get-OutputValue $Outputs 'dependency_instance_id')
    if ($appIDs.Count -ne 2 -or $appIPs.Count -ne 2 -or $generatorIDs.Count -ne 1) {
        throw 'Terraform outputs must contain exactly two app hosts, two app private IPs, one dependency host, and one load generator.'
    }
    $allIDs = @($appIDs) + @($generatorIDs) + @($dependencyID)
    if (($allIDs | Select-Object -Unique).Count -ne 4 -or $allIDs.Where({ $_ -notmatch '^i-[0-9a-f]{17}$' }).Count -ne 0) {
        throw 'Terraform instance IDs must be four distinct EC2 IDs.'
    }
    foreach ($ip in $appIPs) {
        $parsedIP = $null
        if (-not [System.Net.IPAddress]::TryParse([string]$ip, [ref]$parsedIP) -or $parsedIP.AddressFamily -ne [System.Net.Sockets.AddressFamily]::InterNetwork) {
            throw "Terraform app private IP is not IPv4: $ip"
        }
    }
    if (-not $Frozen.namespace_pattern.Contains('{cycle_id}')) { throw 'frozen-config.json must contain the {cycle_id} namespace token.' }
    $namespace = $Frozen.namespace_pattern.Replace('{cycle_id}', $CycleID)
    if ($namespace -notmatch '^dur050-[A-Za-z0-9-]{1,41}$') { throw "Rendered namespace is invalid: $namespace" }
    $apiURL = "http://$($appIPs[0]):8080/"
    if ($env:DUR050_TEST_API_URL) {
        if ($env:DUR050_ENABLE_TEST_HOOKS -ne '1') { throw 'DUR050_TEST_API_URL requires DUR050_ENABLE_TEST_HOOKS=1.' }
        $testApi = $null
        if ($env:DUR050_TEST_API_URL -notmatch '^http://127\.0\.0\.1:[1-9][0-9]{0,4}/?$' -or
            -not [Uri]::TryCreate($env:DUR050_TEST_API_URL, [UriKind]::Absolute, [ref]$testApi) -or
            $testApi.Scheme -ne 'http' -or $testApi.Host -ne '127.0.0.1' -or $testApi.Port -gt 65535) {
            throw 'DUR050_TEST_API_URL must be an absolute loopback HTTP URL with an explicit port.'
        }
        $apiURL = $testApi.AbsoluteUri
    }
    $parsedURL = $null
    if (-not [Uri]::TryCreate($apiURL, [UriKind]::Absolute, [ref]$parsedURL) -or $parsedURL.Scheme -notin @('http', 'https') -or [string]::IsNullOrWhiteSpace($parsedURL.Host)) {
        throw "Terraform-derived API URL is invalid: $apiURL"
    }

    $rendered = [ordered]@{
        api_url = $apiURL
        namespace = $namespace
        run_id = [string]$Frozen.run_id
        seed = [long]$Frozen.seed
        families = @($Frozen.families)
    }
    if ([string]::IsNullOrWhiteSpace($rendered.run_id) -or $rendered.run_id -notmatch '^[A-Za-z0-9-]{1,48}$') {
        throw 'frozen-config.json run_id must be a valid fixed run identifier.'
    }
    if ($rendered.families.Count -ne 2) { throw 'The frozen DUR-050 pilot config must define exactly two families.' }
    $expectedDefinitions = @($rendered.families | ForEach-Object { '{0}@{1}' -f $_.definition_id, $_.definition_version } | Sort-Object)
    $remoteRepoLine = 'cd "${DUR050_REMOTE_REPO_ROOT:-/opt/durable-agent-execution-engine}"'

    $fixtureScript = @(
        'set -euo pipefail',
        $remoteRepoLine,
        ('fixture_config=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml -f deploy/aws/dur050-app-compose.yaml exec -T runtime /dur050-fixture -api-url ' + (ConvertTo-BashSingleQuoted $apiURL) + ' -namespace ' + (ConvertTo-BashSingleQuoted $namespace) + ' -run-id ' + (ConvertTo-BashSingleQuoted $rendered.run_id) + ' -seed ' + [string]$rendered.seed + ')'),
        'printf ''DUR050_FIXTURE_CONFIG_B64=%s\n'' "$(printf ''%s'' "$fixture_config" | base64 -w0)"'
    )

    $renderedJSON = ($rendered | ConvertTo-Json -Depth 30) + "`n"
    $renderedBytes = $utf8.GetBytes($renderedJSON)
    $configB64 = [Convert]::ToBase64String($renderedBytes)
    $configHash = Get-Sha256Hex $renderedBytes
    $remoteDirectory = "/var/tmp/dur050-$CycleID"
    $remoteConfig = "$remoteDirectory/frozen-config.json"
    $generatorScript = @(
        'set -euo pipefail',
        "install -d -m 0700 '$remoteDirectory'",
        "printf '%s' '$configB64' | base64 -d > '$remoteConfig'",
        "chmod 0600 '$remoteConfig'",
        "actual_sha256=`$(sha256sum -- '$remoteConfig' | awk '{print `$1}')" ,
        "test `"`$actual_sha256`" = '$configHash'",
        "printf 'DUR050_CONFIG_PATH=%s\n' '$remoteConfig'",
        "printf 'DUR050_CONFIG_SHA256=%s\n' `"`$actual_sha256`""
    )

    $expectedB64 = [Convert]::ToBase64String($utf8.GetBytes(($expectedDefinitions | ConvertTo-Json -Compress)))
    $guardSQL = @'
SELECT json_build_object(
  'schema_version', (SELECT COALESCE(max(version), 0) FROM engine.schema_migrations),
  'pg_stat_statements_preloaded', current_setting('shared_preload_libraries') LIKE '%pg_stat_statements%',
  'pg_stat_statements_installed', EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'),
  'definition_ids', COALESCE((SELECT json_agg(definition_id || '@' || version::text ORDER BY definition_id) FROM engine.workflow_definitions WHERE definition_id LIKE 'dur050-%'), '[]'::json),
  'workflows', (SELECT count(*) FROM engine.workflow_executions),
  'attempts', (SELECT count(*) FROM engine.activity_attempts),
  'outbox', (SELECT count(*) FROM engine.outbox),
  'inbox', (SELECT count(*) FROM engine.event_inbox)
)::text;
'@ -replace "`r?`n", ' '
    $guardSQLB64 = [Convert]::ToBase64String($utf8.GetBytes($guardSQL))
    $pythonGuard = "import base64,json,sys; d=json.load(sys.stdin); expected=json.loads(base64.b64decode('$expectedB64')); assert d['schema_version']==18, 'schema must be 18'; assert d['pg_stat_statements_preloaded'] and d['pg_stat_statements_installed'], 'pg_stat_statements missing'; assert sorted(d['definition_ids'])==sorted(expected), 'definition set differs'; assert all(d[k]==0 for k in ('workflows','attempts','outbox','inbox')), 'baseline contains durable work'; print('DUR050_GUARD_JSON_B64='+base64.b64encode(json.dumps(d,sort_keys=True,separators=(',',':')).encode()).decode())"
    $expectedTopics = @('__consumer_offsets', 'durable-agent.events.v1', 'durable-agent.tasks.v1')
    $expectedTopicsB64 = [Convert]::ToBase64String($utf8.GetBytes(($expectedTopics | ConvertTo-Json -Compress)))
    $guardLines = @(
        'set -euo pipefail',
        $remoteRepoLine,
        'postgres_user=$(sed -n ''s/^POSTGRES_USER=//p'' deploy/aws/.env)',
        'postgres_db=$(sed -n ''s/^POSTGRES_DB=//p'' deploy/aws/.env)',
        'test -n "$postgres_user" && test -n "$postgres_db"',
        "guard_sql=`$(printf '%s' '$guardSQLB64' | base64 -d)",
        'guard_json=$(docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T postgres psql -v ON_ERROR_STOP=1 -U "$postgres_user" -d "$postgres_db" -Atc "$guard_sql")',
        ('printf ''%s'' "$guard_json" | python3 -c ' + (ConvertTo-BashSingleQuoted $pythonGuard)),
        'topics=$(docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list)',
        'printf ''%s\n'' "$topics" | sed ''/^$/d'' | sort > /tmp/dur050-actual-topics.txt',
        'printf ''%s\n'' __consumer_offsets durable-agent.events.v1 durable-agent.tasks.v1 | sort > /tmp/dur050-expected-topics.txt',
        'diff -u /tmp/dur050-expected-topics.txt /tmp/dur050-actual-topics.txt',
        'topics_b64=$(printf ''%s\n'' "$topics" | sed ''/^$/d'' | sort | base64 -w0)',
        'printf ''DUR050_TOPICS_B64=%s\n'' "$topics_b64"'
    )

    $appQuiesce = @(
        'set -euo pipefail',
        $remoteRepoLine,
        'docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml -f deploy/aws/dur050-app-compose.yaml down',
        'remaining=$(docker compose --env-file deploy/aws/.env -f deploy/aws/app-compose.yaml -f deploy/aws/dur050-app-compose.yaml ps -q runtime worker)',
        'test -z "$remaining"',
        'printf ''DUR050_APP_STACK_QUIESCED=1\n'''
    )

    $archiveDirectory = "/var/tmp/dur050-$CycleID-baseline"
    $captureLines = @(
        'set -euo pipefail',
        $remoteRepoLine,
        "archive_dir='$archiveDirectory'",
        'mkdir -p "$archive_dir" && chmod 0700 "$archive_dir"',
        'baseline_started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)',
        'postgres_image=$(sed -n ''s/^POSTGRES_IMAGE=//p'' deploy/aws/.env)',
        'test -n "$postgres_image"',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml stop postgres kafka',
        'running=$(docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml ps --services --filter status=running)',
        'if printf ''%s\n'' "$running" | grep -Exq ''(postgres|kafka)''; then echo ''dependency still running during volume capture'' >&2; exit 1; fi',
        'docker run --rm --user 0:0 -v durable-aws-dependencies_postgres-data:/data:ro -v "$archive_dir:/backup" --entrypoint bash "$postgres_image" -ec ''tar -cpf /backup/postgres-data.tar -C /data .''',
        'docker run --rm --user 0:0 -v durable-aws-dependencies_kafka-data:/data:ro -v "$archive_dir:/backup" --entrypoint bash "$postgres_image" -ec ''tar -cpf /backup/kafka-data.tar -C /data .''',
        'postgres_archive="$archive_dir/postgres-data.tar"',
        'kafka_archive="$archive_dir/kafka-data.tar"',
        'test -s "$postgres_archive" && test -s "$kafka_archive"',
        'tar -tf "$postgres_archive" > "$archive_dir/postgres-data.list"',
        'sed -n ''1,3p'' "$archive_dir/postgres-data.list" > "$archive_dir/postgres-data.preview"',
        'tar -tf "$kafka_archive" > "$archive_dir/kafka-data.list"',
        'sed -n ''1,3p'' "$archive_dir/kafka-data.list" > "$archive_dir/kafka-data.preview"',
        'postgres_sha=$(sha256sum -- "$postgres_archive" | awk ''{print $1}'')',
        'kafka_sha=$(sha256sum -- "$kafka_archive" | awk ''{print $1}'')',
        'postgres_size=$(stat -c %s "$postgres_archive")',
        'kafka_size=$(stat -c %s "$kafka_archive")',
        'docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml up -d postgres kafka',
        'postgres_user=$(sed -n ''s/^POSTGRES_USER=//p'' deploy/aws/.env)',
        'postgres_db=$(sed -n ''s/^POSTGRES_DB=//p'' deploy/aws/.env)',
        'postgres_ready=0',
        'for attempt in $(seq 1 60); do if docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T postgres pg_isready -U "$postgres_user" -d "$postgres_db" >/dev/null 2>&1; then postgres_ready=1; break; fi; sleep 2; done',
        'test "$postgres_ready" -eq 1',
        'kafka_ready=0',
        'for attempt in $(seq 1 60); do topics=$(docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list 2>/dev/null || true); if printf ''%s\n'' "$topics" | grep -Fxq durable-agent.tasks.v1 && printf ''%s\n'' "$topics" | grep -Fxq durable-agent.events.v1; then kafka_ready=1; break; fi; sleep 2; done',
        'test "$kafka_ready" -eq 1',
        'printf ''DUR050_BASELINE_STARTED_AT=%s\n'' "$baseline_started_at"',
        'printf ''DUR050_POSTGRES_ARCHIVE_PATH=%s\nDUR050_POSTGRES_ARCHIVE_SHA256=%s\nDUR050_POSTGRES_ARCHIVE_SIZE=%s\n'' "$postgres_archive" "$postgres_sha" "$postgres_size"',
        'printf ''DUR050_KAFKA_ARCHIVE_PATH=%s\nDUR050_KAFKA_ARCHIVE_SHA256=%s\nDUR050_KAFKA_ARCHIVE_SIZE=%s\n'' "$kafka_archive" "$kafka_sha" "$kafka_size"',
        'printf ''DUR050_POSTGRES_PREVIEW_B64=%s\n'' "$(base64 -w0 "$archive_dir/postgres-data.preview")"',
        'printf ''DUR050_KAFKA_PREVIEW_B64=%s\n'' "$(base64 -w0 "$archive_dir/kafka-data.preview")"',
        'printf ''DUR050_BASELINE_DEPENDENCIES_RESTARTED=1\n'''
    )

    $stages = @(
        [pscustomobject]@{ id = 'fixture-install'; instance_id = [string]$appIDs[0]; remote_lines = $fixtureScript },
        [pscustomobject]@{ id = 'generator-stage'; instance_id = [string]$generatorIDs[0]; remote_lines = $generatorScript },
        [pscustomobject]@{ id = 'clean-baseline-guard'; instance_id = $dependencyID; remote_lines = $guardLines },
        [pscustomobject]@{ id = 'quiesce-app-1'; instance_id = [string]$appIDs[0]; remote_lines = $appQuiesce },
        [pscustomobject]@{ id = 'quiesce-app-2'; instance_id = [string]$appIDs[1]; remote_lines = $appQuiesce },
        [pscustomobject]@{ id = 'capture-baseline'; instance_id = $dependencyID; remote_lines = $captureLines }
    )
    foreach ($stage in $stages) {
        $stage | Add-Member -NotePropertyName wrapped_command -NotePropertyValue (New-Dur050SsmBashCommand -Stage $stage.id -RemoteLines $stage.remote_lines)
        $stage | Add-Member -NotePropertyName expected_definitions -NotePropertyValue $expectedDefinitions
        $stage | Add-Member -NotePropertyName rendered_config -NotePropertyValue $rendered
        $stage | Add-Member -NotePropertyName rendered_config_json -NotePropertyValue $renderedJSON
        $stage | Add-Member -NotePropertyName rendered_config_sha256 -NotePropertyValue $configHash
        $stage | Add-Member -NotePropertyName rendered_config_path -NotePropertyValue $remoteConfig
        $stage | Add-Member -NotePropertyName expected_topics -NotePropertyValue $expectedTopics
    }
    return $stages
}

function Invoke-AwsJson([string[]]$Arguments) {
    $raw = & aws --region us-west-1 @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "AWS CLI failed: aws $($Arguments -join ' '): $raw" }
    return $raw | ConvertFrom-Json
}

function Invoke-RemoteStage($Stage, [int]$Index, [int]$Total) {
    $started = [DateTime]::UtcNow
    $wrappedHash = Get-Sha256Hex ($utf8.GetBytes($Stage.wrapped_command))
    $record = [ordered]@{
        schema = 'dur050-pilot-preparation-stage.v1'
        cycle_id = $CycleID
        stage = $Stage.id
        instance_id = $Stage.instance_id
        requested_at_utc = $started.ToString('o')
        wrapped_command_sha256 = $wrappedHash
        remote_shell = 'New-Dur050SsmBashCommand; UTF-8/LF base64 temp-file wrapper; bash'
        status = 'FAIL'
    }
    $inputPath = Join-Path ([System.IO.Path]::GetTempPath()) ("dur050-prepare-{0}.json" -f [guid]::NewGuid().ToString('N'))
    try {
        $body = @{ DocumentName = 'AWS-RunShellScript'; InstanceIds = @($Stage.instance_id); Comment = "DUR-050 $CycleID $($Stage.id)"; Parameters = @{ commands = @($Stage.wrapped_command) } } | ConvertTo-Json -Depth 8 -Compress
        [System.IO.File]::WriteAllText($inputPath, $body, $utf8)
        $sent = Invoke-AwsJson @('ssm', 'send-command', '--cli-input-json', "file://$inputPath", '--output', 'json')
        $commandID = [string]$sent.Command.CommandId
        if (-not $commandID) { throw "SSM returned no command id for $($Stage.id)." }
        $record.ssm_command_id = $commandID
        $deadline = [DateTime]::UtcNow.AddMinutes($TimeoutMinutes)
        $invocation = $null
        do {
            Start-Sleep -Seconds 2
            try { $invocation = Invoke-AwsJson @('ssm', 'get-command-invocation', '--command-id', $commandID, '--instance-id', $Stage.instance_id, '--output', 'json') }
            catch { if ([DateTime]::UtcNow -ge $deadline) { throw }; continue }
            if ($invocation.Status -in @('Success', 'Failed', 'Cancelled', 'TimedOut', 'Undeliverable', 'Terminated')) { break }
        } while ([DateTime]::UtcNow -lt $deadline)
        if ($null -eq $invocation) { throw "SSM stage $($Stage.id) timed out without an invocation response." }
        $record.ssm_status = [string]$invocation.Status
        $record.finished_at_utc = [DateTime]::UtcNow.ToString('o')
        $record.standard_output = [string]$invocation.StandardOutputContent
        $record.stderr_sha256 = Get-Sha256Hex ($utf8.GetBytes([string]$invocation.StandardErrorContent))
        if ($invocation.Status -ne 'Success') { throw "SSM stage $($Stage.id) finished as $($invocation.Status)." }
        $record.status = 'PASS'
        Write-JsonFile (Join-Path $cycleDirectory ("stage-{0}.json" -f $Stage.id)) $record
        Write-Progress -Activity 'DUR-050 pilot preparation' -Status $Stage.id -PercentComplete (100 * $Index / $Total)
        return [string]$invocation.StandardOutputContent
    } catch {
        $record.error = $_.Exception.Message
        $record.finished_at_utc = [DateTime]::UtcNow.ToString('o')
        Write-JsonFile (Join-Path $cycleDirectory ("stage-{0}.json" -f $Stage.id)) $record
        throw
    } finally {
        Remove-Item -LiteralPath $inputPath -Force -ErrorAction SilentlyContinue
    }
}

try {
    if (-not (Test-Path -LiteralPath $TerraformOutputsPath -PathType Leaf)) { throw "Terraform outputs JSON is missing: $TerraformOutputsPath" }
    if (-not (Test-Path -LiteralPath $FrozenConfigPath -PathType Leaf)) { throw "Cycle-independent frozen config is missing: $FrozenConfigPath" }
    $outputs = Get-Content -LiteralPath $TerraformOutputsPath -Raw | ConvertFrom-Json
    $frozen = Get-Content -LiteralPath $FrozenConfigPath -Raw | ConvertFrom-Json
    $stages = @(Get-StageDefinitions $frozen $outputs)
    $dryRunRecord = [ordered]@{
        schema = 'dur050-pilot-preparation-dry-run.v1'
        classification = 'DRY RUN ONLY - AWS calls and SSM dispatch are not performed'
        cycle_id = $CycleID
        terraform_outputs_sha256 = (Get-FileHash -LiteralPath $TerraformOutputsPath -Algorithm SHA256).Hash.ToLowerInvariant()
        frozen_config_sha256 = (Get-FileHash -LiteralPath $FrozenConfigPath -Algorithm SHA256).Hash.ToLowerInvariant()
        rendered_config_sha256 = $stages[0].rendered_config_sha256
        stages = @($stages | ForEach-Object { [ordered]@{ stage = $_.id; instance_id = $_.instance_id; remote_lines = $_.remote_lines; wrapped_command = $_.wrapped_command; expected_definitions = $_.expected_definitions; expected_topics = $_.expected_topics; rendered_config = $_.rendered_config; rendered_config_json = $_.rendered_config_json; rendered_config_path = $_.rendered_config_path; rendered_config_sha256 = $_.rendered_config_sha256 } })
    }
    if ($DryRun) {
        if ($DryRunOutputPath) { Write-JsonFile $DryRunOutputPath $dryRunRecord }
        $dryRunRecord | ConvertTo-Json -Depth 20
        exit 0
    }

    if (-not $D022PreflightPath -or -not (Test-Path -LiteralPath $D022PreflightPath -PathType Leaf)) { throw 'A committed D022 preflight with status PASS is required before SSM preparation.' }
    $preflight = Read-Dur050D022Preflight -Path $D022PreflightPath -CycleID $CycleID
    if (Test-Path -LiteralPath (Join-Path $cycleDirectory 'preparation.json')) { throw "Preparation evidence already exists for $CycleID." }
    New-Item -ItemType Directory -Force -Path $cycleDirectory | Out-Null
    Write-JsonFile (Join-Path $cycleDirectory 'preparation-dry-run.json') $dryRunRecord
    $identity = Invoke-AwsJson @('sts', 'get-caller-identity', '--output', 'json')
    if ([string]$identity.Account -ne '372206265946') { throw 'Active AWS account is not the D022-authorized account.' }

    $allEvents = [System.Collections.Generic.List[object]]::new()
    $fixtureOutput = $null
    $guardOutput = $null
    $captureOutput = $null
    for ($index = 0; $index -lt $stages.Count; $index++) {
        $stage = $stages[$index]
        $stdout = Invoke-RemoteStage $stage ($index + 1) $stages.Count
        $stageRecord = Get-Content -LiteralPath (Join-Path $cycleDirectory ("stage-{0}.json" -f $stage.id)) -Raw | ConvertFrom-Json
        $allEvents.Add($stageRecord)
        switch ($stage.id) {
            'fixture-install' {
                $fixtureJSON = Read-Marker $stdout 'DUR050_FIXTURE_CONFIG_B64'
                $fixtureOutput = $fixtureJSON | ConvertFrom-Json
                if ($fixtureOutput.api_url -ne $stage.rendered_config.api_url -or $fixtureOutput.namespace -ne $stage.rendered_config.namespace -or [long]$fixtureOutput.seed -ne [long]$stage.rendered_config.seed) {
                    throw 'Fixture installer returned a stale API URL, namespace, or seed.'
                }
                $installed = @($fixtureOutput.families | ForEach-Object { '{0}@{1}' -f $_.definition_id, $_.definition_version } | Sort-Object)
                if (($installed -join ',') -ne ($stage.expected_definitions -join ',')) { throw 'Installed DUR-050 definitions differ from frozen-config.json.' }
                $allEvents[$allEvents.Count - 1] | Add-Member -Force -NotePropertyName installed_definitions -NotePropertyValue $installed
                Write-JsonFile (Join-Path $cycleDirectory ("stage-{0}.json" -f $stage.id)) $allEvents[$allEvents.Count - 1]
            }
            'clean-baseline-guard' {
                $guardOutput = Read-Marker $stdout 'DUR050_GUARD_JSON_B64' | ConvertFrom-Json
                $topicJSON = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String(([regex]::Match($stdout, '(?m)^DUR050_TOPICS_B64=([A-Za-z0-9+/=]+)\s*$')).Groups[1].Value))
                $allEvents[$allEvents.Count - 1] | Add-Member -Force -NotePropertyName guard -NotePropertyValue $guardOutput
                $allEvents[$allEvents.Count - 1] | Add-Member -Force -NotePropertyName topics -NotePropertyValue @($topicJSON -split "`n" | Where-Object { $_ })
                Write-JsonFile (Join-Path $cycleDirectory ("stage-{0}.json" -f $stage.id)) $allEvents[$allEvents.Count - 1]
            }
            'capture-baseline' { $captureOutput = $stdout }
        }
    }

    $manifestValues = @{}
    foreach ($name in @('DUR050_BASELINE_STARTED_AT', 'DUR050_POSTGRES_ARCHIVE_PATH', 'DUR050_POSTGRES_ARCHIVE_SHA256', 'DUR050_POSTGRES_ARCHIVE_SIZE', 'DUR050_KAFKA_ARCHIVE_PATH', 'DUR050_KAFKA_ARCHIVE_SHA256', 'DUR050_KAFKA_ARCHIVE_SIZE', 'DUR050_POSTGRES_PREVIEW_B64', 'DUR050_KAFKA_PREVIEW_B64')) {
        $match = [regex]::Match($captureOutput, '(?m)^' + [regex]::Escape($name) + '=([^\r\n]*)$')
        if (-not $match.Success) { throw "Baseline capture omitted marker $name." }
        $manifestValues[$name] = $match.Groups[1].Value
    }
    foreach ($name in @('DUR050_POSTGRES_ARCHIVE_SHA256', 'DUR050_KAFKA_ARCHIVE_SHA256')) {
        if ($manifestValues[$name] -notmatch '^[0-9a-f]{64}$') { throw "Invalid SHA-256 emitted for $name." }
    }
    $baselineManifest = [ordered]@{
        schema = 'dur050-baseline-manifest.v1'
        status = 'PASS'
        cycle_id = $CycleID
        created_at_utc = [DateTime]::UtcNow.ToString('o')
        clean_baseline_guard = $guardOutput
        expected_kafka_topics = $stage.expected_topics
        app_quiesce = @($allEvents | Where-Object { $_.stage -like 'quiesce-app-*' } | ForEach-Object { [ordered]@{ stage = $_.stage; instance_id = $_.instance_id; ssm_command_id = $_.ssm_command_id; status = $_.status } })
        capture = [ordered]@{
            dependency_instance_id = $stages[2].instance_id
            started_at_utc = $manifestValues.DUR050_BASELINE_STARTED_AT
            completed_at_utc = [DateTime]::UtcNow.ToString('o')
            dependencies_restarted_and_healthy = $captureOutput -match 'DUR050_BASELINE_DEPENDENCIES_RESTARTED=1'
            postgres = [ordered]@{ remote_path = $manifestValues.DUR050_POSTGRES_ARCHIVE_PATH; size_bytes = [long]$manifestValues.DUR050_POSTGRES_ARCHIVE_SIZE; sha256 = $manifestValues.DUR050_POSTGRES_ARCHIVE_SHA256; listing_preview = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($manifestValues.DUR050_POSTGRES_PREVIEW_B64)) }
            kafka = [ordered]@{ remote_path = $manifestValues.DUR050_KAFKA_ARCHIVE_PATH; size_bytes = [long]$manifestValues.DUR050_KAFKA_ARCHIVE_SIZE; sha256 = $manifestValues.DUR050_KAFKA_ARCHIVE_SHA256; listing_preview = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($manifestValues.DUR050_KAFKA_PREVIEW_B64)) }
        }
        generator_config = [ordered]@{ local_path = 'generator-config.json'; remote_path = $stages[1].rendered_config_path; sha256 = $stages[1].rendered_config_sha256; api_url = $stages[1].rendered_config.api_url; namespace = $stages[1].rendered_config.namespace }
        stages = @($allEvents | ForEach-Object { [ordered]@{ stage = $_.stage; instance_id = $_.instance_id; ssm_command_id = $_.ssm_command_id; status = $_.status; requested_at_utc = $_.requested_at_utc; finished_at_utc = $_.finished_at_utc } })
        note = 'Preparation only; no reset, warmup, or paid measurement block is authorized or performed by this script.'
    }
    Write-JsonFile (Join-Path $cycleDirectory 'generator-config.json') $stages[1].rendered_config
    Write-JsonFile (Join-Path $cycleDirectory 'baseline-manifest.json') $baselineManifest
    Write-JsonFile (Join-Path $cycleDirectory 'preparation.json') ([ordered]@{ schema = 'dur050-pilot-preparation.v1'; status = 'PASS'; cycle_id = $CycleID; completed_at_utc = [DateTime]::UtcNow.ToString('o'); fixture_definitions = $stage.expected_definitions; generator_config_sha256 = $stages[1].rendered_config_sha256; baseline_manifest = 'baseline-manifest.json'; stage_records = @($allEvents | ForEach-Object { "stage-$($_.stage).json" }); reset_started = $false; paid_block_started = $false })
    Write-Host "DUR-050 pilot preparation PASS: $cycleDirectory"
} catch {
    if (-not $DryRun -and (Test-Path -LiteralPath $cycleDirectory)) {
        Write-JsonFile (Join-Path $cycleDirectory 'preparation.json') ([ordered]@{ schema = 'dur050-pilot-preparation.v1'; status = 'FAIL'; cycle_id = $CycleID; recorded_at_utc = [DateTime]::UtcNow.ToString('o'); error = $_.Exception.Message; reset_started = $false; paid_block_started = $false })
    }
    throw
}
