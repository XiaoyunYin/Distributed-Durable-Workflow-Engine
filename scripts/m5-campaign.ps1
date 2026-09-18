[CmdletBinding()]
param(
    [string]$OutputPath = "experiments/m5/f01-f11-results.json",
    [switch]$StopRuntimeRelays
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
$relaysStopped = $false
$composeArgs = @("--env-file", ".env", "-f", "deploy/local/compose.yaml")
try {
    if ($StopRuntimeRelays) {
        # The transport cases create claimable outbox rows.  A live runtime
        # relay would legitimately consume those rows before the test owns
        # them, making the campaign nondeterministic.  Keep PostgreSQL/Kafka
        # running and suspend only the services that publish/dispatch them.
        & docker compose @composeArgs stop runtime-a runtime-b worker-a worker-b
        if ($LASTEXITCODE -ne 0) { throw "Could not isolate runtime/worker relays." }
        $relaysStopped = $true
    }
    $cases = @(
        [pscustomobject]@{ Id = "F01"; Package = "./internal/api"; Pattern = "^TestWorkflowAPIResponseLossHistoryAndRetention$" },
        [pscustomobject]@{ Id = "F02"; Package = "./internal/state"; Pattern = "^TestPostgresStateRepository$" },
        [pscustomobject]@{ Id = "F03"; Package = "./internal/transport"; Pattern = "^TestM3RelayFailureIsDurablyRetryable$" },
        [pscustomobject]@{ Id = "F04"; Package = "./internal/transport"; Pattern = "^TestM3RelayCrashAfterBrokerAckRepublishesStableEvent$" },
        [pscustomobject]@{ Id = "F05"; Package = "./internal/api"; Pattern = "^TestM2WorkerAPIConcurrencyRetriesAndStaleResult$" },
        [pscustomobject]@{ Id = "F06"; Package = "./internal/effects"; Pattern = "^TestNonCooperatingEndpointMakesAmbiguityVisibleToCaller$" },
        [pscustomobject]@{ Id = "F07"; Package = "./internal/engine"; Pattern = "^TestM1CrashResumeWithinNode$|^TestM4CheckpointSurvivesCrashBeforeResult$" },
        [pscustomobject]@{ Id = "F08"; Package = "./internal/state"; Pattern = "^TestM2LeaseRenewalAndTakeoverFence$|^TestM2TakeoverWaitsForLockedLeaseTransaction$" },
        [pscustomobject]@{ Id = "F09"; Package = "./internal/state"; Pattern = "^TestM2ConcurrentExpiredLeaseHasOneWinner$" },
        [pscustomobject]@{ Id = "F10"; Package = "./internal/engine"; Pattern = "^TestM1InterpreterTimersAndRestart$|^TestM1ConcurrentBranchCompletionCreatesJoinOnce$" },
        [pscustomobject]@{ Id = "F11"; Package = "./internal/engine"; Pattern = "^TestM1FanoutCancellationRace$" }
    )
    $results = @()
    foreach ($case in $cases) {
        $started = Get-Date
        $output = & go test -race -p 1 $case.Package -run $case.Pattern -count=1 -v 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
        $results += [pscustomobject]@{
            id = $case.Id
            package = $case.Package
            pattern = $case.Pattern
            status = if ($exitCode -eq 0) { "PASS" } else { "FAIL" }
            exit_code = $exitCode
            started_at = $started.ToUniversalTime().ToString("o")
            output = $output.Trim()
        }
        if ($exitCode -ne 0) {
            $parent = Split-Path -Parent $OutputPath
            if ($parent) { New-Item -ItemType Directory -Force $parent | Out-Null }
            $results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $OutputPath -Encoding utf8
            throw "$($case.Id) failed. See $OutputPath"
        }
    }
    $parent = Split-Path -Parent $OutputPath
    if ($parent) { New-Item -ItemType Directory -Force $parent | Out-Null }
    $results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $OutputPath -Encoding utf8
    Write-Host "M5 F01-F11 campaign passed; evidence written to $OutputPath"
} finally {
    if ($relaysStopped) {
        & docker compose @composeArgs up -d --wait runtime-a runtime-b worker-a worker-b
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Could not restore runtime/worker services after the M5 campaign."
        }
    }
    Pop-Location
}
