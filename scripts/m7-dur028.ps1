[CmdletBinding()]
param(
    [switch]$StartServices,
    [int64]$Seed = 280028,
    [string]$PilotOutputPath = "experiments/m7/dur028/pilot.json",
    [string]$OutputPath = "experiments/m7/dur028/results.json"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
$buildRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("durable-dur028-" + [guid]::NewGuid().ToString("N"))
$campaignBinary = Join-Path $buildRoot "dur028-checkpoint.exe"
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

function Sweep-DUR028([string]$User, [string]$Database) {
    $sql = @"
DELETE FROM engine.workflow_executions WHERE namespace = 'dur028-checkpoint';
DELETE FROM engine.workflow_definitions WHERE definition_id LIKE 'dur028-checkpoint-def-%';
"@
    Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", $User, "-d", $Database, "-c", $sql)) | Out-Null
}

try {
    if (-not (Test-Path -LiteralPath ".env")) { throw ".env is missing; run scripts/bootstrap.ps1 first." }
    if ($Seed -eq 0) { throw "Seed must be non-zero." }
    $status = & git status --porcelain --untracked-files=all
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect Git status." }
    if (-not [string]::IsNullOrWhiteSpace(($status | Out-String))) { throw "DUR-028 requires a clean working tree before measurement." }
    $commit = (Invoke-Required "git" @("rev-parse", "HEAD")).output.Trim()
    if ($StartServices) {
        Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait")) | Out-Null
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1")) | Out-Null
    }
    New-Item -ItemType Directory -Force -Path $buildRoot | Out-Null
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $PilotOutputPath) | Out-Null
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutputPath) | Out-Null
    $databaseUser = EnvValue "POSTGRES_USER" "postgres"
    $databaseName = EnvValue "POSTGRES_DB" "durable_agent"
    $databasePassword = EnvValue "POSTGRES_PASSWORD" ""
    $databasePort = EnvValue "POSTGRES_PORT" "5432"
    if ([string]::IsNullOrWhiteSpace($databasePassword)) { throw "POSTGRES_PASSWORD is missing from .env." }
    $databaseURL = "postgresql://${databaseUser}:$databasePassword@127.0.0.1:${databasePort}/${databaseName}?sslmode=disable"

    # The bounded Store/Engine campaign does not need the deployed schedulers;
    # stop them so they cannot renew or consume the reserved fixture lease.
    Invoke-Required "docker" ($composeArgs + @("stop", "runtime-a", "runtime-b", "worker-a", "worker-b")) | Out-Null
    $servicesStopped = $true
    Sweep-DUR028 $databaseUser $databaseName
    Invoke-Required "go" @("build", "-o", $campaignBinary, "./cmd/dur028-checkpoint") | Out-Null

    $pilot = Invoke-Captured $campaignBinary @("-database-url", $databaseURL, "-output", $PilotOutputPath, "-pilot", "-seed", "$Seed", "-git-commit", $commit)
    if ($pilot.exit_code -ne 0) { throw "DUR-028 pilot failed: $($pilot.output)" }
    if (-not (Test-Path -LiteralPath $PilotOutputPath)) { throw "DUR-028 did not produce $PilotOutputPath." }
    $pilotArtifact = Get-Content -Raw -LiteralPath $PilotOutputPath | ConvertFrom-Json
    if ($pilotArtifact.status -ne "PASS" -or @($pilotArtifact.runs).Count -ne 6 -or @($pilotArtifact.summary).Count -ne 6 -or $pilotArtifact.validation.status -ne "PASS" -or [string]::IsNullOrWhiteSpace($pilotArtifact.conclusions.statement) -or @($pilotArtifact.limitations).Count -lt 1) { throw "DUR-028 pilot is incomplete." }
    if ($pilotArtifact.git_commit -ne $commit) { throw "Pilot commit $($pilotArtifact.git_commit) does not match runner commit $commit." }

    $campaign = Invoke-Captured $campaignBinary @("-database-url", $databaseURL, "-output", $OutputPath, "-repeats", "3", "-seed", "$Seed", "-git-commit", $commit)
    if ($campaign.exit_code -ne 0) { throw "DUR-028 campaign failed: $($campaign.output)" }
    if (-not (Test-Path -LiteralPath $OutputPath)) { throw "DUR-028 did not produce $OutputPath." }
    $artifact = Get-Content -Raw -LiteralPath $OutputPath | ConvertFrom-Json
    if ($artifact.status -ne "PASS") { throw "DUR-028 artifact is not PASS: $($artifact.failure)" }
    if ($artifact.git_commit -ne $commit) { throw "Artifact commit $($artifact.git_commit) does not match runner commit $commit." }
    if (@($artifact.runs).Count -ne 18) { throw "Expected 18 measured rows, got $(@($artifact.runs).Count)." }
    if (@($artifact.summary).Count -ne 6 -or [string]::IsNullOrWhiteSpace($artifact.conclusions.statement) -or @($artifact.limitations).Count -lt 1) { throw "DUR-028 summary/conclusion/limitations are incomplete." }
    if (@($artifact.runs | Where-Object { $_.status -ne "PASS" }).Count -ne 0) { throw "At least one DUR-028 row is not PASS." }
    if ($artifact.validation.expected_runs -ne 18 -or $artifact.validation.completed_runs -ne 18) { throw "DUR-028 matrix reconciliation is incomplete." }
    if (-not $artifact.validation.all_final_hashes_match -or -not $artifact.validation.all_terminal_succeeded -or -not $artifact.validation.checkpoint_prefixes_valid) { throw "DUR-028 correctness validation is incomplete." }
    if ($artifact.validation.clean_workflow_rows -ne 0 -or $artifact.validation.clean_definition_rows -ne 0) { throw "DUR-028 left reserved durable rows behind." }
    Sweep-DUR028 $databaseUser $databaseName
    Write-Host "DUR-028 PASS at ${commit}: pilot + 18 measured rows, 3 checkpoint settings, 2 failure conditions, deterministic hash and cleanup verified."
} finally {
    if ($servicesStopped) {
        try { Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait", "runtime-a", "runtime-b", "worker-a", "worker-b")) | Out-Null }
        catch { Write-Error "Could not restore runtime/worker services: $_" }
    }
    if (Test-Path -LiteralPath $buildRoot) { Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue }
    Pop-Location
}
