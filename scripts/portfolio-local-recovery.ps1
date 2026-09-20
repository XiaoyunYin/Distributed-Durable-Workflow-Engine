[CmdletBinding()]
param(
    [switch]$StartServices,
    [int64]$Seed = 410041,
    [string]$OutputRoot = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot

function Get-Stats([double[]]$Values) {
    if ($null -eq $Values -or $Values.Count -eq 0) {
        throw "Cannot summarize an empty measurement set."
    }
    $ordered = @($Values | Sort-Object)
    $middle = [int][math]::Floor($ordered.Count / 2)
    if (($ordered.Count % 2) -eq 0) {
        $median = ($ordered[$middle - 1] + $ordered[$middle]) / 2.0
    } else {
        $median = $ordered[$middle]
    }
    [ordered]@{
        count = $ordered.Count
        min = [double]$ordered[0]
        median = [double]$median
        max = [double]$ordered[$ordered.Count - 1]
    }
}

try {
    if ($Seed -eq 0) { throw "Seed must be non-zero." }
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
        $OutputRoot = Join-Path $RepoRoot ("experiments/portfolio/local-recovery/local-" + $Seed)
    } elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) {
        $OutputRoot = Join-Path $RepoRoot $OutputRoot
    }
    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

    $pilotPath = Join-Path $OutputRoot "pilot.json"
    $sourcePath = Join-Path $OutputRoot "dur027-source.json"
    $summaryPath = Join-Path $OutputRoot "summary.json"
    $protocolPath = Join-Path $OutputRoot "protocol.json"

    $runnerArgs = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
        (Join-Path $PSScriptRoot "m7-dur027.ps1"),
        "-Seed", "$Seed",
        "-PilotOutputPath", $pilotPath,
        "-OutputPath", $sourcePath
    )
    if ($StartServices) { $runnerArgs += "-StartServices" }
    & powershell.exe @runnerArgs
    if ($LASTEXITCODE -ne 0) { throw "The DUR-027 source harness failed." }

    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
        throw "The source harness did not produce $sourcePath."
    }
    $artifact = Get-Content -LiteralPath $sourcePath -Raw | ConvertFrom-Json
    if ($artifact.status -ne "PASS") {
        throw "The source harness artifact is not PASS."
    }

    $inScopeFaults = @("owner_crash", "owner_pause")
    $episodes = @($artifact.runs | ForEach-Object { @($_.episodes) })
    $inScopeEpisodes = @($episodes | Where-Object { $inScopeFaults -contains $_.fault_type })
    if ($inScopeEpisodes.Count -ne 60) {
        throw "Expected 60 in-scope episodes, got $($inScopeEpisodes.Count)."
    }
    if (@($inScopeEpisodes | Where-Object { $_.status -ne "PASS" }).Count -ne 0) {
        throw "An in-scope episode did not pass its source-harness checks."
    }
    if (@($inScopeEpisodes | Where-Object { -not $_.takeover -or -not $_.useful_progress }).Count -ne 0) {
        throw "An in-scope episode lacks takeover or useful-progress evidence."
    }
    $fencedWrites = [int](@($inScopeEpisodes | Measure-Object -Property fenced_old_owner_writes -Sum).Sum)
    if ($fencedWrites -ne 180) { throw "Expected 180 fenced stale-owner writes, got $fencedWrites." }
    if (@($inScopeEpisodes | Where-Object { $_.fault_type -eq "owner_crash" -and -not $_.target_dead }).Count -ne 0) {
        throw "A crash episode lacks confirmed target death."
    }
    if (@($inScopeEpisodes | Where-Object { $_.fault_type -eq "owner_pause" -and -not $_.target_resumed_stale }).Count -ne 0) {
        throw "A pause episode lacks confirmed stale-owner resumption."
    }

    $arms = @()
    foreach ($faultType in $inScopeFaults) {
        $faultEpisodes = @($inScopeEpisodes | Where-Object { $_.fault_type -eq $faultType })
        $arms += [ordered]@{
            fault_type = $faultType
            episodes = $faultEpisodes.Count
            takeover_delay_ms = Get-Stats @($faultEpisodes | ForEach-Object { [double]$_.takeover_delay_ms })
            useful_progress_delay_ms = Get-Stats @($faultEpisodes | ForEach-Object { [double]$_.useful_progress_delay_ms })
            confirmed_faults = $faultEpisodes.Count
            takeovers = @($faultEpisodes | Where-Object { $_.takeover }).Count
            useful_progress = @($faultEpisodes | Where-Object { $_.useful_progress }).Count
            fenced_old_owner_writes = [int](@($faultEpisodes | Measure-Object -Property fenced_old_owner_writes -Sum).Sum)
        }
    }

    $commit = (& git rev-parse HEAD).Trim()
    $relativeSource = [System.IO.Path]::GetRelativePath($RepoRoot, $sourcePath).Replace("\", "/")
    $relativePilot = [System.IO.Path]::GetRelativePath($RepoRoot, $pilotPath).Replace("\", "/")
    $protocol = [ordered]@{
        schema_version = "dur041a-local.v1"
        status = "PASS"
        generated_at_utc = [DateTime]::UtcNow.ToString("o")
        git_commit = $commit
        seed = $Seed
        source_harness = "scripts/m7-dur027.ps1"
        source_artifact = $relativeSource
        pilot_artifact = $relativePilot
        r096_deferred = $true
        in_scope_faults = $inScopeFaults
        excluded_faults = @("lock_held_takeover")
        exclusion_reason = "R096 is explicitly deferred; no bounded lock-held takeover claim is made."
        local_boundary = "Docker Desktop/WSL2 local process fixtures; not independent-host failure."
        required_checks = @(
            "confirmed fault observation",
            "stale-owner fencing after takeover",
            "useful affected-work progress",
            "matched no-fault control and independent checker in the source campaign",
            "run-scoped cleanup"
        )
    }
    $summary = [ordered]@{
        schema_version = "dur041a-local.v1"
        status = "PASS"
        generated_at_utc = [DateTime]::UtcNow.ToString("o")
        git_commit = $commit
        seed = $Seed
        r096_deferred = $true
        episodes = $inScopeEpisodes.Count
        arms = $arms
        safety = [ordered]@{
            takeovers = @($inScopeEpisodes | Where-Object { $_.takeover }).Count
            useful_progress = @($inScopeEpisodes | Where-Object { $_.useful_progress }).Count
            fenced_old_owner_writes = $fencedWrites
            false_takeovers = @($inScopeEpisodes | Where-Object { $_.false_takeover }).Count
        }
        limitations = @(
            "R096 lock-held takeover is excluded and remains unresolved.",
            "Local process/container loss is not independent-host or database-host failure.",
            "The source harness includes a lock-contention probe, but this wrapper does not promote it as an R096 result.",
            "This artifact does not claim production availability, multi-host durability, or exactly-once external effects."
        )
    }
    $protocol | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $protocolPath -Encoding UTF8
    $summary | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
    Write-Host "DUR-041a local recovery scope validated: $($inScopeEpisodes.Count) episodes, $fencedWrites fenced stale-owner writes."
    Write-Host "R096 remains deferred; lock-held takeover is excluded from the promoted result."
} finally {
    Pop-Location
}
