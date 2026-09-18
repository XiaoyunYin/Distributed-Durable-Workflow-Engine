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
$previousCampaignSeed = $env:DURABLE_CAMPAIGN_SEED
$previousRequireDatabase = $env:DURABLE_REQUIRE_DATABASE
$previousPythonPath = $env:PYTHONPATH
$env:DURABLE_REQUIRE_DATABASE = "1"
$env:PYTHONPATH = Join-Path $RepoRoot "python"

function Write-Results([object[]]$Items, [string]$Path) {
    $parent = Split-Path -Parent $Path
    if ($parent) { New-Item -ItemType Directory -Force $parent | Out-Null }
    $Items | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Path -Encoding utf8
}

function Remove-FixtureRows {
    # The fixture is intentionally killed at the named boundary. Remove only
    # its deterministic namespace after each campaign, including when a case
    # fails, so evidence runs do not contaminate the developer database.
    $sql = @"
DELETE FROM engine.workflow_executions WHERE workflow_id LIKE 'm5-fixture-%';
DELETE FROM engine.workflow_definitions WHERE definition_id LIKE 'm5-fixture-def-%';
"@
    & docker compose @composeArgs exec -T postgres psql -v ON_ERROR_STOP=1 `
        -U ((Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_USER=' } | Select-Object -First 1) -replace '^POSTGRES_USER=', '') `
        -d ((Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_DB=' } | Select-Object -First 1) -replace '^POSTGRES_DB=', '') `
        -c $sql | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Warning "M5 fixture cleanup failed." }
}

try {
    if ($StopRuntimeRelays) {
        # The transport cases create claimable outbox rows. A live runtime
        # relay would legitimately consume those rows before the test owns
        # them, making the campaign nondeterministic. Keep PostgreSQL/Kafka
        # running and suspend only the services that publish/dispatch them.
        & docker compose @composeArgs stop runtime-a runtime-b worker-a worker-b
        if ($LASTEXITCODE -ne 0) { throw "Could not isolate runtime/worker relays." }
        $relaysStopped = $true
    }

    $traceRoot = Join-Path $RepoRoot "experiments/m5/traces"
    if (Test-Path -LiteralPath $traceRoot) {
        Remove-Item -LiteralPath $traceRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Force $traceRoot | Out-Null
    $seeds = @(11, 23, 47)
    $cases = @(
        [pscustomobject]@{ Id = "F01-response-loss"; Family = "F01"; Ordering = "response-lost"; Package = "./internal/api"; Pattern = "^TestWorkflowAPIResponseLossHistoryAndRetention$"; Boundary = "submission_committed"; Observation = "retry returns the original workflow" },
        [pscustomobject]@{ Id = "F02-rollback"; Family = "F02"; Ordering = "before-commit"; Package = "./internal/state"; Pattern = "^TestPostgresStateRepository$"; Boundary = "outbox_insert"; Observation = "rollback leaves no half-committed state" },
        [pscustomobject]@{ Id = "F03-relay-retry"; Family = "F03"; Ordering = "before-publish"; Package = "./internal/transport"; Pattern = "^TestM3RelayFailureIsDurablyRetryable$"; Boundary = "before_publish"; Observation = "pending work publishes after recovery" },
        [pscustomobject]@{ Id = "F04-after-ack"; Family = "F04"; Ordering = "after-broker-ack-before-db-commit"; Package = "./internal/transport"; Pattern = "^TestM3RelayCrashAfterBrokerAckRepublishesStableEvent$"; Boundary = "after_broker_ack"; Observation = "stable event identity is accepted once" },
        [pscustomobject]@{ Id = "F05-claim"; Family = "F05"; Ordering = "after-claim-before-result"; Package = "./internal/api"; Pattern = "^TestM2WorkerAPIConcurrencyRetriesAndStaleResult$"; Boundary = "attempt_claimed"; Observation = "claimed work has one durable winner and stale work is rejected" },
        [pscustomobject]@{ Id = "F05-offset"; Family = "F05"; Ordering = "after-inbox-before-offset-commit"; Package = "./internal/state"; Pattern = "^TestM3ConsumerOffsetsAreContiguous$"; Boundary = "before_offset_ack"; Observation = "consumer offset remains contiguous across delivery recovery" },
        [pscustomobject]@{ Id = "F06-cooperating"; Family = "F06"; Ordering = "effect-applied-before-receipt"; Package = "./internal/state"; Pattern = "^TestM4EffectLedgerAndFencing$"; Boundary = "effect_applied"; Observation = "cooperating retry returns the original receipt" },
        [pscustomobject]@{ Id = "F06-noncooperating"; Family = "F06"; Ordering = "effect-applied-before-result"; Package = "./internal/state"; Pattern = "^TestM4NonCooperatingTimeoutIsReconciliationOnly$"; Boundary = "effect_unknown"; Observation = "non-cooperating uncertainty reaches reconciliation" },
        [pscustomobject]@{ Id = "F07-crash-resume"; Family = "F07"; Ordering = "result-recorded-before-consume"; Package = "./internal/engine"; Pattern = "^TestM1CrashResumeWithinNode$|^TestM4CheckpointSurvivesCrashBeforeResult$"; Boundary = "result_recorded"; Observation = "durable receipt or checkpoint is consumed once" },
        [pscustomobject]@{ Id = "F08-lease-fence"; Family = "F08"; Ordering = "takeover-old-owner"; Package = "./internal/state"; Pattern = "^TestM2LeaseRenewalAndTakeoverFence$|^TestM2TakeoverWaitsForLockedLeaseTransaction$"; Boundary = "lease_takeover"; Observation = "old-owner writes are fenced after takeover" },
        [pscustomobject]@{ Id = "F09-stale-result"; Family = "F09"; Ordering = "replacement-old-worker"; Package = "./internal/api"; Pattern = "^TestM2WorkerAPIConcurrencyRetriesAndStaleResult$"; Boundary = "attempt_replaced"; Observation = "old worker result cannot overwrite replacement" },
        [pscustomobject]@{ Id = "F10-timer-join"; Family = "F10"; Ordering = "timer-and-join-duplicate"; Package = "./internal/engine"; Pattern = "^TestM1InterpreterTimersAndRestart$|^TestM1ConcurrentBranchCompletionCreatesJoinOnce$"; Boundary = "timer_consumed"; Observation = "timer and join downstream work are created once" },
        [pscustomobject]@{ Id = "F11-cancel-grant"; Family = "F11"; Ordering = "cancel-before-grant"; Package = "./internal/state"; Pattern = "^TestM4CancellationPreventsApprovalGrant$"; Boundary = "approval_grant"; Observation = "cancellation blocks an approval grant" },
        [pscustomobject]@{ Id = "F11-deadline"; Family = "F11"; Ordering = "deadline-before-completion"; Package = "./internal/state"; Pattern = "^TestM4NonCooperatingTimeoutIsReconciliationOnly$"; Boundary = "attempt_timeout"; Observation = "deadline preserves uncertain effects" },
        [pscustomobject]@{ Id = "F11-dispatch-grant"; Family = "F11"; Ordering = "grant-before-dispatch"; Package = "./internal/state"; Pattern = "^TestM4EffectLedgerAndFencing$|^TestM4CheckpointRetryAndApprovalPersistence$"; Boundary = "approval_grant"; Observation = "dispatch requires the matching approval grant" },
        [pscustomobject]@{ Id = "F11-cancel-completion"; Family = "F11"; Ordering = "cancel-versus-completion"; Package = "./internal/engine"; Pattern = "^TestM1FanoutCancellationRace$"; Boundary = "cancel_completion"; Observation = "cancellation and completion have one terminal outcome" }
    )

    $results = @()
    foreach ($case in $cases) {
        foreach ($seed in $seeds) {
            $tracePath = Join-Path $traceRoot ("{0}-seed{1}.jsonl" -f $case.Id, $seed)
            $controlArgs = @("-m", "faults.control", "--seed", "$seed", "--boundary", $case.Boundary,
                "--action", "kill", "--trace", $tracePath)
            $controlArgs += @("--command", "go", "run", "./cmd/m5-fixture", "--case-id", $case.Id, "--seed", "$seed", "--boundary", $case.Boundary)
            $controllerOutput = & uv run python @controlArgs 2>&1 | Out-String
            $controllerExit = $LASTEXITCODE
            if ($controllerExit -ne 0) {
                $results += [pscustomobject]@{ case_id = $case.Id; family = $case.Family; ordering = $case.Ordering; seed = $seed; run_count = 1; status = "FAIL"; controller_status = "FAIL"; checker_status = "NOT_RUN"; go_status = "NOT_RUN"; trace_path = $tracePath.Replace($RepoRoot + "\", ""); observation = $case.Observation; controller_output = $controllerOutput.Trim(); go_output = "" }
                Write-Results $results $OutputPath
                throw "$($case.Id) controller failed for seed $seed. See $OutputPath"
            }

            $checkerOutput = & go run ./cmd/fault-checker -trace $tracePath 2>&1 | Out-String
            $checkerExit = $LASTEXITCODE
            if ($checkerExit -ne 0) {
                $results += [pscustomobject]@{ case_id = $case.Id; family = $case.Family; ordering = $case.Ordering; seed = $seed; run_count = 1; status = "FAIL"; controller_status = "PASS"; checker_status = "FAIL"; go_status = "NOT_RUN"; trace_path = $tracePath.Replace($RepoRoot + "\", ""); observation = $case.Observation; controller_output = $controllerOutput.Trim(); checker_output = $checkerOutput.Trim(); go_output = "" }
                Write-Results $results $OutputPath
                throw "$($case.Id) checker failed for seed $seed. See $OutputPath"
            }

            # The checker has already loaded and joined the durable prefix.
            # Remove only this campaign namespace before the independent
            # package test so its global outbox fixtures cannot collide with
            # a killed target's deliberately incomplete rows.
            Remove-FixtureRows

            $env:DURABLE_CAMPAIGN_SEED = "$seed"
            $goOutput = & go test -race -p 1 $case.Package -run $case.Pattern -count=1 -v 2>&1 | Out-String
            $goExit = $LASTEXITCODE
            $skipped = $goOutput -match '(?m)^--- SKIP'
            $status = if ($goExit -eq 0 -and -not $skipped) { "PASS" } else { "FAIL" }
            $results += [pscustomobject]@{
                case_id = $case.Id
                family = $case.Family
                ordering = $case.Ordering
                seed = $seed
                run_count = 1
                status = $status
                controller_status = "PASS"
                checker_status = "PASS"
                go_status = $status
                package = $case.Package
                pattern = $case.Pattern
                boundary = $case.Boundary
                observation = $case.Observation
                trace_path = $tracePath.Replace($RepoRoot + "\", "")
                controller_output = $controllerOutput.Trim()
                checker_output = $checkerOutput.Trim()
                go_output = $goOutput.Trim()
            }
            if ($goExit -ne 0 -or $skipped) {
                Write-Results $results $OutputPath
                throw "$($case.Id) Go case failed or skipped for seed $seed. See $OutputPath"
            }
        }
    }
    Write-Results $results $OutputPath
    Write-Host "M5 F01-F11 campaign passed: $($cases.Count) cases x $($seeds.Count) seeded runs; evidence written to $OutputPath"
} finally {
    if ($null -eq $previousCampaignSeed) {
        Remove-Item Env:DURABLE_CAMPAIGN_SEED -ErrorAction SilentlyContinue
    } else {
        $env:DURABLE_CAMPAIGN_SEED = $previousCampaignSeed
    }
    if ($null -eq $previousRequireDatabase) {
        Remove-Item Env:DURABLE_REQUIRE_DATABASE -ErrorAction SilentlyContinue
    } else {
        $env:DURABLE_REQUIRE_DATABASE = $previousRequireDatabase
    }
    if ($relaysStopped) {
        & docker compose @composeArgs up -d --wait runtime-a runtime-b worker-a worker-b
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Could not restore runtime/worker services after the M5 campaign."
        }
    }
    Remove-FixtureRows
    if ($null -eq $previousPythonPath) {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    } else {
        $env:PYTHONPATH = $previousPythonPath
    }
    Pop-Location
}
