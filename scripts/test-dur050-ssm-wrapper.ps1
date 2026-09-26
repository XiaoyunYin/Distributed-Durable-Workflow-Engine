$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "dur050-ssm-wrapper.ps1")

if (-not $IsLinux) { throw "This wrapper integration test must run on Ubuntu/Linux with /bin/sh (dash)." }

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("dur050-ssm-wrapper-" + [guid]::NewGuid().ToString("N"))
$stubBin = Join-Path $tempRoot "bin"
New-Item -ItemType Directory -Path $stubBin -Force | Out-Null
$utf8 = [System.Text.UTF8Encoding]::new($false)
$dockerStub = [string]::Join([char]10, @('#!/bin/sh', 'printf ''DOCKER_STUB:%s\n'' "$*"', ''))
$awsStub = [string]::Join([char]10, @('#!/bin/sh', 'printf ''AWS_STUB:%s\n'' "$*"', ''))
[System.IO.File]::WriteAllText((Join-Path $stubBin "docker"), $dockerStub, $utf8)
[System.IO.File]::WriteAllText((Join-Path $stubBin "aws"), $awsStub, $utf8)
& chmod 0755 (Join-Path $stubBin "docker") (Join-Path $stubBin "aws")
if ($LASTEXITCODE -ne 0) { throw "Could not make the docker/aws test stubs executable." }

function Invoke-PosixWrapper([string]$Command) {
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = "/bin/sh"
    $start.ArgumentList.Add("-c")
    $start.ArgumentList.Add($Command)
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.UseShellExecute = $false
    $start.Environment["PATH"] = $stubBin + ":/usr/bin:/bin"
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    return [pscustomobject]@{ ExitCode = $process.ExitCode; Stdout = $stdout; Stderr = $stderr }
}

try {
    $unwrapped = Invoke-PosixWrapper "set -euo pipefail"
    if ($unwrapped.ExitCode -eq 0 -or $unwrapped.Stderr -notmatch 'Illegal option -o pipefail') {
        throw "The /bin/sh baseline did not reproduce the original Bash-option failure."
    }
    $resetSource = Get-Content -LiteralPath (Join-Path $PSScriptRoot "dur050-reset-block.ps1") -Raw
    $ssmInvokerSource = Get-Content -LiteralPath (Join-Path $PSScriptRoot "dur050-invoke-ssm-command.ps1") -Raw
    if ($resetSource -notmatch '\$wrappedCommand\s*=\s*New-Dur050SsmBashCommand' -or
        $resetSource -notmatch 'commands\s*=\s*@\(\$wrappedCommand\)' -or
        $ssmInvokerSource -notmatch 'New-Dur050SsmBashCommand') {
        throw "A DUR-050 SSM path bypasses the shared Bash wrapper."
    }
    if ($resetSource -notmatch 'function\s+Get-Dur050ResetStageLines' -or $resetSource -notmatch 'Invoke-Ssm\s+\$stage\.stage\s+\$stage\.instance_id') {
        throw 'Reset stage bodies must be constructed by Get-Dur050ResetStageLines and dispatched by the generic Invoke-Ssm loop.'
    }

    $body = @(
        'set -euo pipefail',
        '[[ -n "$BASH_VERSION" ]] || exit 91',
        'set -o | grep -Eq ''^pipefail[[:space:]]+on$'' || exit 92',
        'if false | true; then echo PIPEFAIL_MISSING >&2; exit 93; else echo PIPEFAIL_ACTIVE; fi',
        'docker dur050-wrapper-test',
        'aws dur050-wrapper-test'
    )
    $stages = @(
        "stop-app", "restore-db-kafka-volumes", "configure-admission-and-capture-mode",
        "restart-runtime-worker", "verify-worker-group-assignment", "submit-eight-warmups",
        "observe-warmup-drain", "snapshot-and-kafka-assignment"
    )
    foreach ($stage in $stages) {
        $command = New-Dur050SsmBashCommand -Stage $stage -RemoteLines $body
        $result = Invoke-PosixWrapper $command
        if ($result.ExitCode -ne 0) { throw "Wrapper stage $stage failed ($($result.ExitCode)): $($result.Stderr) $($result.Stdout)" }
        foreach ($expected in @("PIPEFAIL_ACTIVE", "DOCKER_STUB:dur050-wrapper-test", "AWS_STUB:dur050-wrapper-test")) {
            if (-not $result.Stdout.Contains($expected)) { throw "Wrapper stage $stage omitted '$expected'." }
        }
        $removed = [regex]::Match($result.Stdout, '(?m)^DUR050_SSM_TEMP_REMOVED=(.+)$')
        if (-not $removed.Success -or (Test-Path -LiteralPath $removed.Groups[1].Value)) {
            throw "Wrapper stage $stage did not remove its temporary script."
        }
    }

    $rejectedDirectScript = $false
    try {
        [void](New-Dur050SsmBashCommand -Stage "bad-script" -RemoteLines @("/opt/durable-agent-execution-engine/scripts/not-bash.sh"))
    } catch {
        $rejectedDirectScript = $_.Exception.Message -match "must be invoked as bash"
    }
    if (-not $rejectedDirectScript) { throw "The SSM wrapper accepted a .sh command not explicitly invoked through bash." }

    foreach ($fragment in @('"', "'", ':"broken', ":'broken")) {
        $rejectedFragment = $false
        try { [void](New-Dur050SsmBashCommand -Stage 'split-fragment' -RemoteLines @('set -euo pipefail', $fragment)) }
        catch { $rejectedFragment = $_.Exception.Message -match 'split quote/colon fragment' }
        if (-not $rejectedFragment) { throw "SSM wrapper accepted split quote/colon fragment '$fragment'." }
    }

    $failure = Invoke-PosixWrapper (New-Dur050SsmBashCommand -Stage "exit-code" -RemoteLines @("set -euo pipefail", "exit 23"))
    if ($failure.ExitCode -ne 23) { throw "SSM wrapper changed remote exit code 23 to $($failure.ExitCode)." }
    $failureTemp = [regex]::Match($failure.Stdout, '(?m)^DUR050_SSM_TEMP_REMOVED=(.+)$')
    if (-not $failureTemp.Success -or (Test-Path -LiteralPath $failureTemp.Groups[1].Value)) {
        throw "SSM wrapper did not remove the temporary script after a failing body."
    }

    Write-Host "NEGATIVE CONTROL: direct /bin/sh rejects set -euo pipefail as expected."
    Write-Host "PASS: 8 DUR-050 SSM stages execute via bash under /bin/sh, pipefail works, stubs run, and temporary scripts are removed."
    Write-Host "PASS: remote exit code 23 is preserved and its temporary script is removed."
} finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}
