$ErrorActionPreference = 'Stop'
if (-not $IsLinux) { throw 'DUR-050 pilot preparation integration tests require Ubuntu/Linux and /bin/sh (dash).' }
. (Join-Path $PSScriptRoot 'dur050-ssm-wrapper.ps1')
. (Join-Path $PSScriptRoot 'dur050-baseline-manifest.ps1')

$repoRoot = Split-Path -Parent $PSScriptRoot
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("dur050-pilot-prep-test-" + [guid]::NewGuid().ToString('N'))
$stubBin = Join-Path $tempRoot 'bin'
$repoStub = Join-Path $tempRoot 'remote-repo'
$archiveRoot = Join-Path $tempRoot 'archive-source'
$fileRoot = Join-Path $archiveRoot 'files'
$logPath = Join-Path $tempRoot 'docker.log'
$awsLog = Join-Path $tempRoot 'aws.log'
$utf8 = [System.Text.UTF8Encoding]::new($false)
New-Item -ItemType Directory -Force -Path $stubBin, (Join-Path $repoStub 'deploy/aws'), $fileRoot | Out-Null
[System.IO.File]::WriteAllText((Join-Path $repoStub 'deploy/aws/.env'), "POSTGRES_USER=durable`nPOSTGRES_DB=durable`nPOSTGRES_IMAGE=postgres:test`n", $utf8)

$psqlStub = @'
#!/bin/sh
cat <<'JSON'
{"schema_version":18,"pg_stat_statements_preloaded":true,"pg_stat_statements_installed":true,"definition_ids":["dur050-fanout-8-v1@1","dur050-seq-8-v1@1"],"workflows":0,"attempts":0,"outbox":0,"inbox":0}
JSON
'@
$dockerStub = @'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
printf '%s\n' "$args" >> "$DUR050_TEST_DOCKER_LOG"
if [[ "$args" == *"/dur050-fixture"* ]]; then
  cat <<'JSON'
{"api_url":"http://10.49.1.11:8080/","namespace":"dur050-pilot-cycle-test","run_id":"pilot-calibration","seed":50050,"families":[{"name":"seq-8","definition_id":"dur050-seq-8-v1","definition_version":1},{"name":"fanout-8","definition_id":"dur050-fanout-8-v1","definition_version":1}]}
JSON
elif [[ "$args" == *"psql"* ]]; then
  psql "$@"
elif [[ "$args" == *"kafka-topics.sh"* ]]; then
  printf '%s\n' '__consumer_offsets' 'durable-agent.events.v1' 'durable-agent.tasks.v1'
elif [[ "$args" == *"pg_isready"* ]]; then
  echo 'accepting connections'
elif [[ "${1:-}" == 'run' ]]; then
  backup=''
  previous=''
  for value in "$@"; do
    if [[ "$previous" == '-v' && "$value" == *':/backup' ]]; then backup="${value%:/backup}"; fi
    previous="$value"
  done
  test -n "$backup"
  mkdir -p "$backup"
  if [[ "$args" == *'postgres-data.tar'* ]]; then
    cp "$DUR050_TEST_ARCHIVE_ROOT/postgres-data.tar" "$backup/postgres-data.tar"
  elif [[ "$args" == *'kafka-data.tar'* ]]; then
    cp "$DUR050_TEST_ARCHIVE_ROOT/kafka-data.tar" "$backup/kafka-data.tar"
  else
    echo 'unrecognized archive command' >&2
    exit 88
  fi
fi
exit 0
'@
$awsStub = @'
#!/bin/sh
printf '%s\n' "$*" >> "$DUR050_TEST_AWS_LOG"
exit 99
'@
$shaStub = @'
#!/bin/sh
if [ "${DUR050_TEST_SHA_MISMATCH:-0}" = 1 ]; then
  printf '%064d  %s\n' 0 "${2:-archive}"
else
  exec /usr/bin/sha256sum "$@"
fi
'@
foreach ($entry in @(@('docker', $dockerStub), @('psql', $psqlStub), @('aws', $awsStub), @('sha256sum', $shaStub))) {
    [System.IO.File]::WriteAllText((Join-Path $stubBin $entry[0]), $entry[1].Replace("`r", '') + "`n", $utf8)
    & chmod 0755 (Join-Path $stubBin $entry[0])
    if ($LASTEXITCODE -ne 0) { throw "Could not chmod test stub $($entry[0])." }
}

function Invoke-Dash([string]$Command, [hashtable]$Environment) {
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = '/bin/sh'
    $start.ArgumentList.Add('-c')
    $start.ArgumentList.Add($Command)
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.UseShellExecute = $false
    $start.Environment['PATH'] = "$stubBin`:/usr/bin:/bin"
    foreach ($key in $Environment.Keys) { $start.Environment[$key] = [string]$Environment[$key] }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    return [pscustomobject]@{ ExitCode = $process.ExitCode; Stdout = $stdout; Stderr = $stderr }
}

function Invoke-Bash([string]$Command, [hashtable]$Environment) {
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = '/bin/bash'
    $start.ArgumentList.Add('-c')
    $start.ArgumentList.Add($Command)
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.UseShellExecute = $false
    foreach ($key in $Environment.Keys) { $start.Environment[$key] = [string]$Environment[$key] }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    return [pscustomobject]@{ ExitCode = $process.ExitCode; Stdout = $stdout; Stderr = $stderr }
}

try {
    for ($index = 0; $index -lt 12000; $index++) {
        [System.IO.File]::WriteAllText((Join-Path $fileRoot ("item-{0:D5}" -f $index)), '', $utf8)
    }
    foreach ($name in @('postgres-data', 'kafka-data')) {
        $tarStart = [System.Diagnostics.ProcessStartInfo]::new()
        $tarStart.FileName = '/bin/tar'
        $tarStart.ArgumentList.Add('-cf')
        $tarStart.ArgumentList.Add((Join-Path $archiveRoot "$name.tar"))
        $tarStart.ArgumentList.Add('-C')
        $tarStart.ArgumentList.Add($fileRoot)
        $tarStart.ArgumentList.Add('.')
        $tarStart.UseShellExecute = $false
        $tarProcess = [System.Diagnostics.Process]::Start($tarStart)
        $tarProcess.WaitForExit()
        if ($tarProcess.ExitCode -ne 0) { throw "Could not prepare the $name test archive." }
    }

    $cycleID = 'cycle-test-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
    $dryRunRaw = & (Join-Path $PSHOME 'pwsh') -NoProfile -File (Join-Path $PSScriptRoot 'dur050-prepare-pilot.ps1') -TerraformOutputsPath (Join-Path $repoRoot 'tests/fixtures/dur050-terraform-outputs.json') -CycleID $cycleID -DryRun 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "DUR-050 dry-run failed: $dryRunRaw" }
    $dryRun = $dryRunRaw | ConvertFrom-Json
    if ($dryRun.stages.Count -ne 6) { throw "Expected six wrapped SSM stages, saw $($dryRun.stages.Count)." }
    $expectedNames = @('fixture-install', 'generator-stage', 'clean-baseline-guard', 'quiesce-app-1', 'quiesce-app-2', 'capture-baseline')
    if (($dryRun.stages.stage -join ',') -ne ($expectedNames -join ',')) { throw 'Dry-run stage order or names differ from the required preparation sequence.' }
    if ($dryRun.stages[0].rendered_config.api_url -ne 'http://10.49.1.11:8080/' -or $dryRun.stages[0].rendered_config.namespace -ne "dur050-pilot-$cycleID") { throw 'Dry-run did not render the API URL and namespace from current Terraform outputs plus the cycle token.' }
    if (($dryRun.stages[0].expected_definitions -join ',') -ne 'dur050-fanout-8-v1@1,dur050-seq-8-v1@1' -or $dryRun.stages[2].expected_topics.Count -ne 3) { throw 'Dry-run omitted its exact definition/topic contract.' }
    if ($dryRun.stages[1].rendered_config_sha256 -notmatch '^[0-9a-f]{64}$' -or $dryRun.stages[1].rendered_config_path -notmatch [regex]::Escape($cycleID)) { throw 'Per-cycle generator config path or hash is missing.' }
    $guardBody = $dryRun.stages[2].remote_lines -join "`n"
    if ($guardBody -notmatch 'pg_stat_statements' -or $guardBody -notmatch 'event_inbox') { throw 'Clean-baseline guard omits required database checks.' }
    $captureBody = $dryRun.stages[5].remote_lines -join "`n"
    if ($captureBody -notmatch 'tar -tf .* > .*\.list' -or $captureBody -notmatch "sed -n ''1,3p''" -or $captureBody -match '\|\s*head') {
        throw 'Baseline capture must fully write tar listings before previewing them; early-closing pipes are forbidden.'
    }
    if (-not (Test-Path -LiteralPath $awsLog)) { Write-Host 'PASS: dry-run produced all six wrapped SSM stages without calling AWS.' }

    $envForStages = @{
        DUR050_REMOTE_REPO_ROOT = $repoStub
        DUR050_TEST_ARCHIVE_ROOT = $archiveRoot
        DUR050_TEST_DOCKER_LOG = $logPath
        DUR050_TEST_AWS_LOG = $awsLog
    }
    foreach ($stage in $dryRun.stages) {
        $result = Invoke-Dash $stage.wrapped_command $envForStages
        if ($result.ExitCode -ne 0) { throw "Wrapped preparation stage $($stage.stage) failed under dash ($($result.ExitCode)): $($result.Stderr) $($result.Stdout)" }
        $removed = [regex]::Match($result.Stdout, '(?m)^DUR050_SSM_TEMP_REMOVED=(.+)$')
        if (-not $removed.Success -or (Test-Path -LiteralPath $removed.Groups[1].Value)) { throw "Stage $($stage.stage) did not remove its wrapper temporary file." }
        if ($stage.stage -eq 'clean-baseline-guard' -and $result.Stdout -notmatch 'DUR050_GUARD_JSON_B64=') { throw 'Baseline guard did not preserve its checked JSON.' }
        if ($stage.stage -eq 'capture-baseline' -and $result.Stdout -notmatch 'DUR050_BASELINE_DEPENDENCIES_RESTARTED=1') { throw 'Baseline capture did not restart and verify dependencies.' }
    }
    if (Test-Path -LiteralPath $awsLog) { throw 'A dry-run stage unexpectedly invoked AWS.' }
    Write-Host 'PASS: all six actual wrapped stage bodies ran under dash with docker/aws/psql stubs; the baseline stage archived, listed, hashed, and restarted dependencies.'

    $invalidOutputPath = Join-Path $tempRoot 'invalid-terraform-outputs.json'
    $invalidOutputs = Get-Content -LiteralPath (Join-Path $repoRoot 'tests/fixtures/dur050-terraform-outputs.json') -Raw | ConvertFrom-Json
    $invalidOutputs.app_private_ips[0] = 'not-an-ip'
    [System.IO.File]::WriteAllText($invalidOutputPath, ($invalidOutputs | ConvertTo-Json -Depth 8), $utf8)
    $invalidRun = & (Join-Path $PSHOME 'pwsh') -NoProfile -File (Join-Path $PSScriptRoot 'dur050-prepare-pilot.ps1') -TerraformOutputsPath $invalidOutputPath -CycleID $cycleID -DryRun 2>&1 | Out-String
    if ($LASTEXITCODE -eq 0 -or $invalidRun -notmatch 'not IPv4') { throw 'Preparation dry-run accepted a malformed Terraform app IP/API URL.' }
    Write-Host 'PASS: malformed Terraform app address is rejected before any SSM/AWS dispatch.'

    $largeArchive = Join-Path $archiveRoot 'postgres-data.tar'
    $headMutation = Invoke-Bash ('set -euo pipefail; tar -tf "{0}" | head -n 3 >/dev/null; echo MUTATION_SURVIVED' -f $largeArchive) @{}
    if ($headMutation.ExitCode -ne 141) { throw "The early-closing-head mutation was not detected; expected exit 141, got $($headMutation.ExitCode). $($headMutation.Stderr)" }
    $fullList = Join-Path $tempRoot 'full-list'
    $sedControl = Invoke-Bash ('set -euo pipefail; tar -tf "{0}" > "{1}"; sed -n ''1,3p'' "{1}" >/dev/null' -f $largeArchive, $fullList) @{}
    if ($sedControl.ExitCode -ne 0) { throw "The full-read tar listing control failed: $($sedControl.Stderr)" }
    Write-Host 'NEGATIVE CONTROL: replacing tar -tf > list; sed -n 1,3p list with tar -tf | head -n 3 exited 141; the full-read listing control exited 0.'

    $hashLines = Get-Dur050BaselineHashVerificationLines -PostgresArchive '/var/tmp/test/postgres.tar' -PostgresSHA256 ('f' * 64) -KafkaArchive '/var/tmp/test/kafka.tar' -KafkaSHA256 ('e' * 64)
    $resetSource = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'dur050-reset-block.ps1') -Raw
    $hashVerifierPosition = $resetSource.IndexOf('Get-Dur050BaselineHashVerificationLines', [StringComparison]::Ordinal)
    $dependencyStopPosition = $resetSource.IndexOf("docker compose --env-file deploy/aws/.env -f deploy/aws/dependency-compose.yaml down", [StringComparison]::Ordinal)
    if ($hashVerifierPosition -lt 0 -or $dependencyStopPosition -lt 0 -or $hashVerifierPosition -ge $dependencyStopPosition -or $resetSource -notmatch '\$BaselineManifestPath') {
        throw 'Reset helper is not wired to the manifest-backed archive verification function.'
    }
    if ($resetSource -notmatch 'generator_config\.remote_path' -or $resetSource -notmatch 'generator_config\.sha256' -or $resetSource -notmatch 'export DUR050_LOADGEN_CONFIG_FILE=') {
        throw 'Reset helper does not pass the manifest-rendered per-cycle config to the warmup script.'
    }
    $hashBody = @('set -euo pipefail') + $hashLines + @('docker should-not-run')
    $hashWrapped = New-Dur050SsmBashCommand -Stage 'reset-hash-mismatch-test' -RemoteLines $hashBody
    $hashEnvironment = @{} + $envForStages
    $hashEnvironment['DUR050_TEST_SHA_MISMATCH'] = '1'
    $hashResult = Invoke-Dash $hashWrapped $hashEnvironment
    if ($hashResult.ExitCode -ne 41 -or $hashResult.Stderr -notmatch 'PostgreSQL baseline archive SHA-256 mismatch') { throw "Reset helper hash mismatch was not rejected before extraction: $($hashResult.ExitCode) $($hashResult.Stderr)" }
    if ((Get-Content -LiteralPath $logPath -Raw) -match 'should-not-run') { throw 'The reset helper proceeded beyond an archive hash mismatch.' }
    Write-Host 'PASS: reset helper verification lines stop before restore/extraction when the archive hash differs from the manifest.'
} finally {
    if (Test-Path -LiteralPath $tempRoot) { Remove-Item -LiteralPath $tempRoot -Recurse -Force }
    $remoteCycleDir = "/var/tmp/dur050-$cycleID"
    if ($cycleID -and (Test-Path -LiteralPath $remoteCycleDir)) { Remove-Item -LiteralPath $remoteCycleDir -Recurse -Force }
    $remoteBaselineDir = "/var/tmp/dur050-$cycleID-baseline"
    if ($cycleID -and (Test-Path -LiteralPath $remoteBaselineDir)) { Remove-Item -LiteralPath $remoteBaselineDir -Recurse -Force }
}
