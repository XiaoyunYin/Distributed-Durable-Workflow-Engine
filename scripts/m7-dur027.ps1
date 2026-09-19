[CmdletBinding()]
param(
    [switch]$StartServices,
    [int64]$Seed = 270027,
    [string]$PilotOutputPath = "experiments/m7/dur027/pilot.json",
    [string]$OutputPath = "experiments/m7/dur027/results.json"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
$buildRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("durable-dur027-" + [guid]::NewGuid().ToString("N"))
$campaignBinary = Join-Path $buildRoot "dur027-lease.exe"
$fixtureBinary = Join-Path $buildRoot "dur027-crash-fixture.exe"
$composeArgs = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
$servicesStopped = $false

function Invoke-Captured([string]$FilePath, [string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { $text = & $FilePath @Arguments 2>&1 | Out-String } finally { $ErrorActionPreference = $previous }
    [pscustomobject]@{ command = (($FilePath + " " + ($Arguments -join " ")).Trim()); exit_code = $LASTEXITCODE; output = $text.Trim() }
}

function Invoke-Required([string]$FilePath, [string[]]$Arguments) {
    $result = Invoke-Captured $FilePath $Arguments
    if ($result.exit_code -ne 0) { throw "Command failed ($($result.exit_code)): $($result.command)`n$($result.output)" }
    $result
}

function EnvValue([string]$Name, [string]$Fallback) {
    $line = Get-Content -LiteralPath ".env" | Where-Object { $_ -match ("^" + [regex]::Escape($Name) + "=") } | Select-Object -First 1
    if ($line) { return ($line -replace ("^" + [regex]::Escape($Name) + "="), "") }
    $Fallback
}

function Sweep-DUR027([string]$DatabaseUser, [string]$DatabaseName) {
    $sql = @"
DELETE FROM engine.workflow_executions WHERE namespace LIKE 'dur027-crash-%' OR namespace LIKE 'dur027-lease-%';
DELETE FROM engine.workflow_definitions WHERE definition_id LIKE 'dur027-crash-%' OR definition_id LIKE 'dur027-lease-%';
"@
    Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", $DatabaseUser, "-d", $DatabaseName, "-c", $sql)) | Out-Null
}

try {
    if (-not (Test-Path -LiteralPath ".env")) { throw ".env is missing; run scripts/bootstrap.ps1 first." }
    if ($Seed -eq 0) { throw "Seed must be non-zero." }
    $status = & git status --porcelain --untracked-files=all
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect Git status." }
    if (-not [string]::IsNullOrWhiteSpace(($status | Out-String))) { throw "DUR-027 requires a clean working tree before measurement." }
    $commit = (Invoke-Required "git" @("rev-parse", "HEAD")).output.Trim()

    if ($StartServices) {
        Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait")) | Out-Null
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1")) | Out-Null
    }

    New-Item -ItemType Directory -Force -Path $buildRoot | Out-Null
    $databaseUser = EnvValue "POSTGRES_USER" "postgres"
    $databaseName = EnvValue "POSTGRES_DB" "durable_agent"
    $databasePassword = EnvValue "POSTGRES_PASSWORD" ""
    $databasePort = EnvValue "POSTGRES_PORT" "5432"
    if ([string]::IsNullOrWhiteSpace($databasePassword)) { throw "POSTGRES_PASSWORD is missing from .env." }
    $databaseURL = "postgresql://${databaseUser}:$databasePassword@127.0.0.1:${databasePort}/${databaseName}?sslmode=disable"

    # Foundation runtimes renew leases for their own partitions. Stop only
    # those processes around the bounded measurement; PostgreSQL and Kafka
    # remain available for the fixture and the lock-contention probe.
    Invoke-Required "docker" ($composeArgs + @("stop", "runtime-a", "runtime-b", "worker-a", "worker-b")) | Out-Null
    $servicesStopped = $true
    Sweep-DUR027 $databaseUser $databaseName

    Invoke-Required "go" @("build", "-o", $campaignBinary, "./cmd/dur027-lease") | Out-Null
    Invoke-Required "go" @("build", "-o", $fixtureBinary, "./cmd/dur027-crash-fixture") | Out-Null
    foreach ($path in @($PilotOutputPath, $OutputPath)) {
        $parent = Split-Path -Parent $path
        if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    }

    $pilot = Invoke-Captured $campaignBinary @("-database-url", $databaseURL, "-output", $PilotOutputPath, "-pilot", "-seed", "$Seed")
    if ($pilot.exit_code -ne 0) { throw "DUR-027 pilot failed: $($pilot.output)" }
    if (-not (Test-Path -LiteralPath $PilotOutputPath)) { throw "DUR-027 did not produce $PilotOutputPath." }
    $pilotArtifact = Get-Content -Raw -LiteralPath $PilotOutputPath | ConvertFrom-Json
    if ($pilotArtifact.status -ne "PASS" -or @($pilotArtifact.arms).Count -ne 3) { throw "DUR-027 pilot is incomplete." }
    if ($pilotArtifact.git_commit -ne $commit) { throw "Pilot commit $($pilotArtifact.git_commit) does not match runner commit $commit." }

    $campaign = Invoke-Captured $campaignBinary @("-database-url", $databaseURL, "-fixture-binary", $fixtureBinary, "-pilot-input", $PilotOutputPath, "-output", $OutputPath, "-seed", "$Seed", "-episodes-per-config", "10")
    if ($campaign.exit_code -ne 0) { throw "DUR-027 campaign failed: $($campaign.output)" }
    if (-not (Test-Path -LiteralPath $OutputPath)) { throw "DUR-027 did not produce $OutputPath." }
    $artifact = Get-Content -Raw -LiteralPath $OutputPath | ConvertFrom-Json
    if ($artifact.status -ne "PASS") { throw "DUR-027 artifact is not PASS: $($artifact.failure)" }
    if ($artifact.git_commit -ne $commit) { throw "Artifact commit $($artifact.git_commit) does not match runner commit $commit." }
    if (@($artifact.runs).Count -ne 6) { throw "Expected 6 runs, got $(@($artifact.runs).Count)." }
    if (@($artifact.runs | Where-Object { $_.status -ne "PASS" }).Count -ne 0) { throw "At least one DUR-027 configuration is not PASS." }
    if ($artifact.validation.expected_configurations -ne 6 -or $artifact.validation.completed_episodes -ne 60) { throw "DUR-027 matrix reconciliation is incomplete." }
    if ($artifact.validation.takeovers -ne 60 -or $artifact.validation.useful_progress -ne 60 -or $artifact.validation.false_takeovers -ne 0) { throw "DUR-027 takeover/progress safety counts are invalid." }
    if ($artifact.validation.fenced_old_owner_writes -ne 180 -or $artifact.validation.lock_contention_cases -ne 60) { throw "DUR-027 fencing or lock-contention counts are incomplete." }
    if ($artifact.validation.crash_targets_dead -ne 30 -or $artifact.validation.paused_targets_resumed_stale -ne 30 -or $artifact.validation.lease_held_after_injection -ne 60) { throw "DUR-027 fault-boundary counts are incomplete." }
    if ($artifact.validation.clean_workflow_rows -ne 0 -or $artifact.validation.clean_definition_rows -ne 0) { throw "DUR-027 left reserved durable rows behind." }
    Sweep-DUR027 $databaseUser $databaseName
    Write-Host "DUR-027 PASS at ${commit}: pilot + 6 configurations, 60 episodes, 60 takeovers, 60 useful recoveries, zero false takeovers, 30 crash targets dead, 30 pause targets resumed stale."
} finally {
    if ($servicesStopped) {
        try { Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait", "runtime-a", "runtime-b", "worker-a", "worker-b")) | Out-Null }
        catch { Write-Error "Could not restore runtime/worker services: $_" }
    }
    if (Test-Path -LiteralPath $buildRoot) { Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue }
    Pop-Location
}
