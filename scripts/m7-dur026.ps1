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

function Add-Property([object]$Object, [string]$Name, [object]$Value) {
    $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value -Force
    return $Object
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
    Invoke-Required "go" @("build", "-o", $benchmarkBinary, "./cmd/dur026-benchmark") | Out-Null

    $schedulerCounts = @(1, 2)
    $workloads = @("t1", "t2")
    $rates = @(2.0, 8.0)
    $repeats = if ($Pilot) { @(1) } else { @(1, 2, 3) }
    $workflowCount = 12
    $activityWork = 5000
    $results = @()
    $caseIndex = 0
    foreach ($schedulerCount in $schedulerCounts) {
        foreach ($workload in $workloads) {
            foreach ($rate in $rates) {
                foreach ($repeat in $repeats) {
                    $caseIndex++
                    $caseID = "DUR026-${workload}-s${schedulerCount}-r$($rate.ToString('0.###'))-rep$repeat"
                    $runID = ("m7-{0}-{1:D2}-{2}" -f $caseID.ToLowerInvariant(), $caseIndex, ([guid]::NewGuid().ToString("N")))
                    $stdoutPath = Join-Path $buildRoot ($caseIndex.ToString("D3") + ".json")
                    $stderrPath = Join-Path $buildRoot ($caseIndex.ToString("D3") + ".err")
                    $arguments = @(
                        "-workload", $workload,
                        "-scheduler-count", "$schedulerCount",
                        "-rate", "$rate",
                        "-workflow-count", "$workflowCount",
                        "-seed", "$(10000 + $caseIndex)",
                        "-activity-work", "$activityWork",
                        "-run-id", $runID
                    )
                    $started = Get-Date
                    $process = Start-Process -FilePath $benchmarkBinary -ArgumentList $arguments -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
                    $process.WaitForExit()
                    $process.Refresh()
                    $finished = Get-Date
                    $totalProcessorTime = $process.TotalProcessorTime
                    if ($null -eq $totalProcessorTime) {
                        throw "DUR-026 child process did not expose CPU time for $caseID."
                    }
                    $cpuSeconds = [double]$totalProcessorTime.TotalSeconds
                    $stdout = if (Test-Path -LiteralPath $stdoutPath) { Get-Content -Raw -LiteralPath $stdoutPath } else { "" }
                    $stderr = if (Test-Path -LiteralPath $stderrPath) { Get-Content -Raw -LiteralPath $stderrPath } else { "" }
                    $stderrText = if ($null -eq $stderr) { "" } else { [string]$stderr }
                    $run = $null
                    if (-not [string]::IsNullOrWhiteSpace($stdout)) {
                        try { $run = $stdout | ConvertFrom-Json } catch { $run = $null }
                    }
                    if ($null -eq $run) {
                        $run = [pscustomobject]@{ schema_version = "dur026-run.v1"; status = "FAIL"; failure = ($stderrText.Trim() + " " + ([string]$stdout).Trim()).Trim() }
                    }
                    Add-Property $run "case_id" $caseID | Out-Null
                    Add-Property $run "repeat" $repeat | Out-Null
                    Add-Property $run "process_cpu_seconds" $cpuSeconds | Out-Null
                    Add-Property $run "process_wall_seconds" (($finished - $started).TotalSeconds) | Out-Null
                    Add-Property $run "stderr" $stderrText.Trim() | Out-Null
                    $results += $run
                    if ($process.ExitCode -ne 0 -or $run.status -ne "PASS") {
                        $partialParent = Split-Path -Parent $OutputPath
                        if ($partialParent) { New-Item -ItemType Directory -Force -Path $partialParent | Out-Null }
                        [ordered]@{ status = "FAIL"; commit = $commit; case_id = $caseID; run = $run; runs = $results } | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $OutputPath -Encoding utf8
                        throw "DUR-026 case $caseID failed. See $OutputPath and temporary stderr $stderrPath."
                    }
                }
            }
        }
    }

    $summary = @($results | Group-Object case_id | ForEach-Object {
        $group = @($_.Group)
        $throughputs = @($group | ForEach-Object { [double]$_.timing.throughput }) | Sort-Object
        $cpu = @($group | ForEach-Object { [double]$_.process_cpu_seconds }) | Measure-Object -Average -Sum
        [ordered]@{
            case_id = $_.Name
            runs = $group.Count
            throughput_min = $throughputs[0]
            throughput_median = $throughputs[[int][math]::Floor($throughputs.Count / 2)]
            throughput_max = $throughputs[$throughputs.Count - 1]
            mean_process_cpu_seconds = $cpu.Average
            total_process_cpu_seconds = $cpu.Sum
            terminal_total = [int](($group | ForEach-Object { $_.cohort.terminal } | Measure-Object -Sum).Sum)
            pending_total = [int](($group | ForEach-Object { $_.cohort.pending } | Measure-Object -Sum).Sum)
        }
    })
    $artifact = [ordered]@{
        schema_version = "dur026-throughput.v1"
        status = "PASS"
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        git = [ordered]@{ commit = $commit; clean_worktree_verified = $true }
        protocol = [ordered]@{
            study = "engine throughput"
            scheduler_counts = $schedulerCounts
            workloads = [ordered]@{ t1 = "8 sequential deterministic pure activities"; t2 = "8-branch fan-out/fan-in deterministic pure activities" }
            arrival_rates_per_second = $rates
            repeats = $repeats.Count
            workflow_count_per_run = $workflowCount
            activity_work_units = $activityWork
            worker_slots = 4
            database_and_broker_capacity = "Compose profile held constant; this harness exercises the Store/Engine path directly and records that limitation."
            llm_latency = "excluded"
            seed_rule = "10000 + case index"
        }
        validation = [ordered]@{
            measured_runs = $results.Count
            expected_runs = $schedulerCounts.Count * $workloads.Count * $rates.Count * $repeats.Count
            terminal_workflows = [int](($results | ForEach-Object { $_.cohort.terminal } | Measure-Object -Sum).Sum)
            pending_workflows = [int](($results | ForEach-Object { $_.cohort.pending } | Measure-Object -Sum).Sum)
            all_runs_passed = (($results | Where-Object { $_.status -ne "PASS" }).Count -eq 0)
        }
        summary = $summary
        runs = $results
        limitations = @(
            "This is bounded single-node Docker Desktop/WSL2 evidence under the DUR-036 host declaration.",
            "The current runtime does not construct a production scheduler Engine; this study invokes the committed Engine/Store harness directly.",
            "Scheduler CPU is the benchmark process CPU time, not a multi-process production scheduler allocation.",
            "No maximum sustainable throughput or constant-total-CPU speedup claim is made."
        )
    }
    $parent = Split-Path -Parent $OutputPath
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    $artifact | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $OutputPath -Encoding utf8
    $label = if ($Pilot) { "pilot" } else { "final" }
    Write-Host "DUR-026 $label throughput run passed: $($results.Count) measured runs; evidence written to $OutputPath"
} finally {
    if ($null -eq $previousEngineMode) { Remove-Item Env:RUNTIME_ENGINE_MODE -ErrorAction SilentlyContinue }
    else { $env:RUNTIME_ENGINE_MODE = $previousEngineMode }
    if (Test-Path -LiteralPath $buildRoot) { Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue }
    Pop-Location
}
