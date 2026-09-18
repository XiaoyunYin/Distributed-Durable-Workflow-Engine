[CmdletBinding()]
param(
    [switch]$StartServices,
    [switch]$Pilot,
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
$buildRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("durable-dur026-" + [guid]::NewGuid().ToString("N"))
$benchmarkBinary = Join-Path $buildRoot "dur026-benchmark.exe"
$workerBinary = Join-Path $buildRoot "dur026-worker.exe"
$composeArgs = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
$previousEngineMode = $env:RUNTIME_ENGINE_MODE

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = if ($Pilot) { "experiments/m7/dur026/pilot.json" } else { "experiments/m7/dur026/results.json" }
}

function Invoke-Captured([string]$FilePath, [string[]]$Arguments) {
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $FilePath @Arguments 2>&1 | Out-String
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
    [pscustomobject]@{
        command = (($FilePath + " " + ($Arguments -join " ")).Trim())
        exit_code = $LASTEXITCODE
        output = $output.Trim()
    }
}

function Invoke-Required([string]$FilePath, [string[]]$Arguments) {
    $result = Invoke-Captured $FilePath $Arguments
    if ($result.exit_code -ne 0) {
        throw "Command failed ($($result.exit_code)): $($result.command)`n$($result.output)"
    }
    return $result
}

function EnvValue([string]$Name, [string]$Fallback) {
    $line = Get-Content -LiteralPath ".env" | Where-Object {
        $_ -match ("^" + [regex]::Escape($Name) + "=")
    } | Select-Object -First 1
    if ($line) { return ($line -replace ("^" + [regex]::Escape($Name) + "="), "") }
    return $Fallback
}

function Add-Property([object]$Object, [string]$Name, [object]$Value) {
    $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value -Force
    return $Object
}

function Run-Child([string]$Binary, [string[]]$Arguments, [string]$Label) {
    $stdoutPath = Join-Path $buildRoot ($Label + ".json")
    $stderrPath = Join-Path $buildRoot ($Label + ".err")
    $started = Get-Date
    $process = Start-Process -FilePath $Binary -ArgumentList $Arguments -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
    $process.WaitForExit()
    $process.Refresh()
    $finished = Get-Date
    $totalProcessorTime = $process.TotalProcessorTime
    if ($null -eq $totalProcessorTime) {
        throw "DUR-026 child process did not expose CPU time for $Label."
    }
    $stdout = if (Test-Path -LiteralPath $stdoutPath) { Get-Content -Raw -LiteralPath $stdoutPath } else { "" }
    $stderr = if (Test-Path -LiteralPath $stderrPath) { Get-Content -Raw -LiteralPath $stderrPath } else { "" }
    $stderrText = if ($null -eq $stderr) { "" } else { [string]$stderr }
    $run = $null
    if (-not [string]::IsNullOrWhiteSpace($stdout)) {
        try { $run = $stdout | ConvertFrom-Json } catch { $run = $null }
    }
    if ($null -eq $run) {
        $run = [pscustomobject]@{
            schema_version = "dur026-run.v1"
            status = "FAIL"
            failure = ($stderrText.Trim() + " " + ([string]$stdout).Trim()).Trim()
        }
    }
    Add-Property $run "scheduler_cpu_seconds" ([double]$totalProcessorTime.TotalSeconds) | Out-Null
    Add-Property $run "scheduler_wall_seconds" (($finished - $started).TotalSeconds) | Out-Null
    Add-Property $run "child_exit_code" $process.ExitCode | Out-Null
    Add-Property $run "stderr" $stderrText.Trim() | Out-Null
    [pscustomobject]@{ run = $run; stdout = $stdout; stderr = $stderrText }
}

try {
    if (-not (Test-Path -LiteralPath ".env")) {
        throw ".env is missing; run scripts/bootstrap.ps1 before DUR-026."
    }
    $status = & git status --porcelain --untracked-files=all
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect Git status." }
    if (-not [string]::IsNullOrWhiteSpace(($status | Out-String))) {
        throw "DUR-026 requires a clean working tree before a measurement run."
    }
    $commit = (Invoke-Required "git" @("rev-parse", "HEAD")).output.Trim()

    if ($StartServices) {
        Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait")) | Out-Null
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1")) | Out-Null
    }

    New-Item -ItemType Directory -Force -Path $buildRoot | Out-Null

    # R069 cleanup is scoped to the readiness fixture namespace. Keep this
    # sweep in the measurement runner so an aborted readiness run cannot
    # become part of the throughput database population.
    $databaseUser = EnvValue "POSTGRES_USER" "postgres"
    $databaseName = EnvValue "POSTGRES_DB" "durable_agent"
    $dur036WorkflowCount = [int](Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", $databaseUser, "-d", $databaseName, "-c", "SELECT count(*) FROM engine.workflow_executions WHERE workflow_id LIKE 'dur036-runtime-%';"))).output.Trim()
    $dur036DefinitionCount = [int](Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", $databaseUser, "-d", $databaseName, "-c", "SELECT count(*) FROM engine.workflow_definitions WHERE definition_id LIKE 'dur036-runtime-def-%';"))).output.Trim()
    if ($dur036WorkflowCount -gt 0) {
        Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", $databaseUser, "-d", $databaseName, "-c", "DELETE FROM engine.workflow_executions WHERE workflow_id LIKE 'dur036-runtime-%';")) | Out-Null
    }
    if ($dur036DefinitionCount -gt 0) {
        Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", $databaseUser, "-d", $databaseName, "-c", "DELETE FROM engine.workflow_definitions WHERE definition_id LIKE 'dur036-runtime-def-%';")) | Out-Null
    }
    $dur026WorkflowCount = [int](Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", $databaseUser, "-d", $databaseName, "-c", "SELECT count(*) FROM engine.workflow_executions WHERE namespace LIKE 'dur026-bench-%';"))).output.Trim()
    if ($dur026WorkflowCount -gt 0) {
        Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", $databaseUser, "-d", $databaseName, "-c", "DELETE FROM engine.workflow_executions WHERE namespace LIKE 'dur026-bench-%';")) | Out-Null
    }

    Invoke-Required "go" @("build", "-o", $benchmarkBinary, "./cmd/dur026-benchmark") | Out-Null
    Invoke-Required "go" @("build", "-o", $workerBinary, "./cmd/dur026-worker") | Out-Null

    $schedulerCounts = if ($Pilot) { @(1) } else { @(1, 2) }
    $workloads = @("t1", "t2")
    $pilotRates = @(0.25, 0.5, 1.0, 2.0, 4.0, 8.0)
    $rates = if ($Pilot) {
        $pilotRates
    } else {
        $pilotPath = Join-Path $RepoRoot "experiments/m7/dur026/pilot.json"
        if (-not (Test-Path -LiteralPath $pilotPath)) {
            throw "Final DUR-026 run requires a committed calibration pilot at $pilotPath."
        }
        $pilotArtifact = Get-Content -Raw -LiteralPath $pilotPath | ConvertFrom-Json
        if ($pilotArtifact.status -ne "PASS" -or $pilotArtifact.calibration.frozen_rates.below_saturation -le 0 -or $pilotArtifact.calibration.frozen_rates.near_saturation -le 0) {
            throw "Pilot does not contain valid frozen arrival rates."
        }
        @([double]$pilotArtifact.calibration.frozen_rates.below_saturation, [double]$pilotArtifact.calibration.frozen_rates.near_saturation)
    }
    $repeats = if ($Pilot) { @(1) } else { @(1, 2, 3) }
    $workerSlots = 4
    $warmupWorkflowCount = 4
    $workflowCount = if ($Pilot) { 8 } else { 24 }
    $activityWork = 5000
    $completionSLOSeconds = 120
    $maximumRunSeconds = 900
    $results = @()
    $caseIndex = 0
    foreach ($schedulerCount in $schedulerCounts) {
        foreach ($workload in $workloads) {
            foreach ($rate in $rates) {
                foreach ($repeat in $repeats) {
                    $caseIndex++
                    $configID = "DUR026-${workload}-s${schedulerCount}-r$($rate.ToString('0.###'))"
                    $caseID = "$configID-rep$repeat"
                    $runID = ("m7-{0}-{1:D2}-{2}" -f $caseID.ToLowerInvariant(), $caseIndex, ([guid]::NewGuid().ToString("N")))
                    $commonArguments = @(
                        "-workload", $workload,
                        "-scheduler-count", "$schedulerCount",
                        "-rate", "$rate",
                        "-seed", "$(10000 + $caseIndex)",
                        "-activity-work", "$activityWork",
                        "-worker-binary", $workerBinary,
                        "-worker-slots", "$workerSlots",
                        "-completion-slo-seconds", "$completionSLOSeconds"
                    )
                    $warmupArguments = $commonArguments + @("-workflow-count", "$warmupWorkflowCount", "-run-id", "$runID-warmup")
                    $warmup = Run-Child $benchmarkBinary $warmupArguments ("{0:D3}-warmup" -f $caseIndex)
                    if (($null -ne $warmup.run.child_exit_code -and $warmup.run.child_exit_code -ne 0) -or $warmup.run.status -ne "PASS") {
                        throw "DUR-026 warmup $caseID failed: $($warmup.run.failure)"
                    }
                    $measuredArguments = $commonArguments + @("-workflow-count", "$workflowCount", "-run-id", $runID)
                    $measured = Run-Child $benchmarkBinary $measuredArguments ("{0:D3}" -f $caseIndex)
                    $run = $measured.run
                    Add-Property $run "case_id" $caseID | Out-Null
                    Add-Property $run "config_id" $configID | Out-Null
                    Add-Property $run "repeat" $repeat | Out-Null
                    Add-Property $run "warmup" $warmup.run | Out-Null
                    $results += $run
                    if (($null -ne $run.child_exit_code -and $run.child_exit_code -ne 0) -or $run.status -ne "PASS") {
                        $partialParent = Split-Path -Parent $OutputPath
                        if ($partialParent) { New-Item -ItemType Directory -Force -Path $partialParent | Out-Null }
                        [ordered]@{
                            status = "FAIL"
                            commit = $commit
                            case_id = $caseID
                            run = $run
                            runs = $results
                        } | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $OutputPath -Encoding utf8
                        throw "DUR-026 case $caseID failed. See $OutputPath."
                    }
                }
            }
        }
    }

    $summary = @($results | Group-Object config_id | ForEach-Object {
        $group = @($_.Group)
        $throughputs = @($group | ForEach-Object { [double]$_.timing.terminal_workflows_per_second }) | Sort-Object
        $schedulerCPU = @($group | ForEach-Object { [double]$_.scheduler_cpu_seconds }) | Measure-Object -Average -Sum
        $workerCPU = @($group | ForEach-Object { [double]$_.worker_cpu_seconds }) | Measure-Object -Average -Sum
        $terminalTotal = [int](($group | ForEach-Object { $_.cohort.terminal } | Measure-Object -Sum).Sum)
        [ordered]@{
            config_id = $_.Name
            runs = $group.Count
            throughput_min = $throughputs[0]
            throughput_median = $throughputs[[int][math]::Floor($throughputs.Count / 2)]
            throughput_max = $throughputs[$throughputs.Count - 1]
            mean_scheduler_cpu_seconds = $schedulerCPU.Average
            mean_worker_cpu_seconds = $workerCPU.Average
            total_scheduler_cpu_seconds = $schedulerCPU.Sum
            total_worker_cpu_seconds = $workerCPU.Sum
            terminal_total = $terminalTotal
            pending_total = [int](($group | ForEach-Object { $_.cohort.pending } | Measure-Object -Sum).Sum)
            scheduler_cpu_seconds_per_terminal_workflow = if ($terminalTotal -gt 0) { $schedulerCPU.Sum / $terminalTotal } else { $null }
            worker_cpu_seconds_per_terminal_workflow = if ($terminalTotal -gt 0) { $workerCPU.Sum / $terminalTotal } else { $null }
        }
    })
    $calibrationRows = @($results | Group-Object config_id | ForEach-Object {
        $group = @($_.Group)
        $first = $group[0]
        $throughput = ($group | ForEach-Object { [double]$_.timing.terminal_workflows_per_second } | Measure-Object -Average).Average
        [ordered]@{
            workload = $first.config.workload
            scheduler_count = [int]$first.config.scheduler_count
            offered_rate = [double]$first.config.arrival_rate_per_second
            mean_throughput = $throughput
            offered_to_measured_ratio = if ([double]$first.config.arrival_rate_per_second -gt 0) { $throughput / [double]$first.config.arrival_rate_per_second } else { $null }
        }
    })
    if ($Pilot) {
        $belowCandidates = @($pilotRates | Where-Object {
            $rate = [double]$_
            @($calibrationRows | Where-Object { $_.offered_rate -eq $rate -and $_.offered_to_measured_ratio -ge 0.8 }).Count -eq $workloads.Count
        })
        $belowRate = if ($belowCandidates.Count -gt 0) { [double]($belowCandidates | Sort-Object | Select-Object -Last 1) } else { 0.0 }
        $nearCandidates = @($pilotRates | Where-Object { [double]$_ -gt $belowRate })
        $nearRate = if ($nearCandidates.Count -gt 0) { [double]($nearCandidates | Sort-Object | Select-Object -First 1) } else { 0.0 }
        if ($belowRate -le 0 -or $nearRate -le 0) {
            throw "Pilot could not identify both below-saturation and near-saturation rates."
        }
        $calibration = [ordered]@{
            baseline = "one scheduler; both frozen workloads"
            criterion = "below-saturation means measured/offered >= 0.8 for every workload; near-saturation is the next higher offered rate"
            rows = $calibrationRows
            frozen_rates = [ordered]@{ below_saturation = $belowRate; near_saturation = $nearRate }
        }
    } else {
        $calibration = [ordered]@{
            source = "experiments/m7/dur026/pilot.json"
            frozen_rates = [ordered]@{ below_saturation = [double]$rates[0]; near_saturation = [double]$rates[1] }
            rows = @()
        }
    }
    $artifact = [ordered]@{
        schema_version = "dur026-throughput.v2"
        status = "PASS"
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        git = [ordered]@{ commit = $commit; clean_worktree_verified = $true }
        fixture_sweep = [ordered]@{
            dur036_runtime_workflows_before = $dur036WorkflowCount
            dur036_runtime_definitions_before = $dur036DefinitionCount
            dur026_benchmark_workflows_before = $dur026WorkflowCount
            scope = "reserved DUR-036 readiness and DUR-026 benchmark namespaces only"
        }
        calibration = $calibration
        protocol = [ordered]@{
            study = "engine throughput"
            scheduler_counts = $schedulerCounts
            workloads = [ordered]@{ t1 = "8 sequential deterministic pure activities"; t2 = "8-branch fan-out/fan-in deterministic pure activities" }
            arrival_rates_per_second = $rates
            repeats = $repeats.Count
            warmup_workflows = $warmupWorkflowCount
            measured_workflows_per_run = $workflowCount
            measurement_window = "open-loop scheduled arrival window plus separately reported drain to the final terminal workflow"
            maximum_run_seconds = $maximumRunSeconds
            completion_slo_seconds = $completionSLOSeconds
            activity_work_units = $activityWork
            worker_slots = $workerSlots
            queue_capacity = $workflowCount
            worker_model = "four fixed subprocess workers; scheduler CPU excludes worker process CPU"
            database_and_broker_capacity = "Compose profile held constant; this harness exercises the Store/Engine path directly and records that limitation."
            llm_latency = "excluded"
            seed_rule = "10000 + case index"
        }
        validation = [ordered]@{
            measured_runs = $results.Count
            expected_runs = $schedulerCounts.Count * $workloads.Count * $rates.Count * $repeats.Count
            terminal_workflows = [int](($results | ForEach-Object { $_.cohort.terminal } | Measure-Object -Sum).Sum)
            pending_workflows = [int](($results | ForEach-Object { $_.cohort.pending } | Measure-Object -Sum).Sum)
            completion_slo_violations = [int](($results | ForEach-Object { $_.cohort.completion_slo_violations } | Measure-Object -Sum).Sum)
            all_runs_passed = (($results | Where-Object { $_.status -ne "PASS" }).Count -eq 0)
        }
        summary = $summary
        runs = $results
        limitations = @(
            "This is bounded single-node Docker Desktop/WSL2 evidence under the DUR-036 host declaration.",
            "The current runtime does not construct a production scheduler Engine; this study invokes the committed Engine/Store harness directly.",
            "Scheduler and worker CPU are reported separately; scheduler count is not a fixed-total-CPU efficiency claim.",
            "The queue and arrival producer are open-loop, but the study does not claim multi-host production throughput or maximum sustainable capacity."
        )
    }
    $parent = Split-Path -Parent $OutputPath
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    $artifact | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $OutputPath -Encoding utf8
    $label = if ($Pilot) { "pilot" } else { "final" }
    Write-Host "DUR-026 $label throughput run passed: $($results.Count) measured runs; evidence written to $OutputPath"
} finally {
    if ($null -eq $previousEngineMode) { Remove-Item Env:RUNTIME_ENGINE_MODE -ErrorAction SilentlyContinue }
    else { $env:RUNTIME_ENGINE_MODE = $previousEngineMode }
    if (Test-Path -LiteralPath $buildRoot) { Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue }
    Pop-Location
}
