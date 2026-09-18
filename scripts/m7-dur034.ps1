[CmdletBinding()]
param(
    [switch]$StartServices,
    [string]$OutputPath = "experiments/m7/dur034/results.json"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
$buildRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("durable-dur034-" + [guid]::NewGuid().ToString("N"))
$ablationBinary = Join-Path $buildRoot "dur034-ablation.exe"
$workerBinary = Join-Path $buildRoot "dur034-worker.exe"
$composeArgs = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")

function Invoke-Required([string]$FilePath, [string[]]$Arguments) {
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed ($LASTEXITCODE): $FilePath $($Arguments -join ' ')"
    }
}

function EnvValue([string]$Name, [string]$Fallback) {
    $line = Get-Content -LiteralPath ".env" | Where-Object {
        $_ -match ("^" + [regex]::Escape($Name) + "=")
    } | Select-Object -First 1
    if ($line) { return ($line -replace ("^" + [regex]::Escape($Name) + "="), "") }
    return $Fallback
}

function Invoke-Sweep {
    $databaseUser = EnvValue "POSTGRES_USER" "postgres"
    $databaseName = EnvValue "POSTGRES_DB" "durable_agent"
    $sql = @"
DELETE FROM engine.workflow_executions
WHERE namespace LIKE 'dur034-ablation-%'
   OR namespace LIKE 'dur034-lease-control-%';
DELETE FROM engine.workflow_definitions
WHERE definition_id LIKE 'dur034-ablation-%-def'
   OR definition_id LIKE 'dur034-lease-control-%-def';
"@
    Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", $databaseUser, "-d", $databaseName, "-c", $sql))
}

try {
    if (-not (Test-Path -LiteralPath ".env")) {
        throw ".env is missing; run scripts/bootstrap.ps1 before DUR-034."
    }
    $status = & git status --porcelain --untracked-files=all
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect Git status." }
    if (-not [string]::IsNullOrWhiteSpace(($status | Out-String))) {
        throw "DUR-034 requires a clean working tree before a measurement run."
    }
    if ($StartServices) {
        Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait"))
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1"))
    }

    New-Item -ItemType Directory -Force -Path $buildRoot | Out-Null
    Invoke-Sweep
    Invoke-Required "go" @("build", "-tags", "dur034_ablation", "-o", $ablationBinary, "./cmd/dur034-ablation")
    Invoke-Required "go" @("build", "-o", $workerBinary, "./cmd/dur026-worker")
    Invoke-Required $ablationBinary @(
        "-output", $OutputPath,
        "-worker-binary", $workerBinary,
        "-worker-slots", "4",
        "-completion-slo-seconds", "120"
    )

    if (-not (Test-Path -LiteralPath $OutputPath)) {
        throw "DUR-034 did not write $OutputPath."
    }
    $artifact = Get-Content -Raw -LiteralPath $OutputPath | ConvertFrom-Json
    if ($artifact.status -ne "PASS") {
        throw "DUR-034 artifact status is $($artifact.status): $($artifact.failure)"
    }
    if ([int]$artifact.validation.measured_runs -ne 12 -or
        -not [bool]$artifact.validation.all_runs_reconciled -or
        -not [bool]$artifact.validation.history_disabled_has_no_history -or
        -not [bool]$artifact.validation.unsafe_negative_control_fails -or
        -not [bool]$artifact.validation.no_outbox_delay_measured -or
        -not [bool]$artifact.validation.all_slo_values_within_limit -or
        $null -eq $artifact.cost_effects_resolved -or
        [string]::IsNullOrWhiteSpace([string]$artifact.cost_interpretation) -or
        $null -eq $artifact.resolved_cost_effects -or
        $null -eq $artifact.summary) {
        throw "DUR-034 artifact failed its independent acceptance checks."
    }
    Invoke-Sweep
    Write-Host "DUR-034 safeguard ablation passed: 12 measured runs; evidence written to $OutputPath"
} finally {
    if (Test-Path -LiteralPath $buildRoot) {
        Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    Pop-Location
}
