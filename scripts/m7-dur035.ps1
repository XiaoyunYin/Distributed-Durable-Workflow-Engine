[CmdletBinding()]
param(
    [switch]$StartServices,
    [string]$OutputPath = "experiments/m7/dur035/results.json"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$buildRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("durable-dur035-" + [guid]::NewGuid().ToString("N"))
$dispatchBinary = Join-Path $buildRoot "dur035-dispatch.exe"
$composeArgs = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
$relaysStopped = $false

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
DELETE FROM engine.workflow_executions WHERE namespace LIKE 'dur035-dispatch-%';
DELETE FROM engine.workflow_definitions WHERE definition_id LIKE 'dur035-dispatch-%-def';
"@
    Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", $databaseUser, "-d", $databaseName, "-c", $sql))
}

Push-Location $RepoRoot
try {
    if (-not (Test-Path -LiteralPath ".env")) {
        throw ".env is missing; run scripts/bootstrap.ps1 before DUR-035."
    }
    $status = & git status --porcelain --untracked-files=all
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect Git status." }
    if (-not [string]::IsNullOrWhiteSpace(($status | Out-String))) {
        throw "DUR-035 requires a clean working tree before a measurement run."
    }
    if ($StartServices) {
        Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait"))
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1"))
    }

    # The live runtime/worker consumers claim task outbox rows globally. Stop
    # them for the bounded study so only the selected arm can deliver its
    # reserved namespace's task rows; restore them in the outer finally.
    Invoke-Required "docker" ($composeArgs + @("stop", "runtime-a", "runtime-b", "worker-a", "worker-b"))
    $relaysStopped = $true

    New-Item -ItemType Directory -Force -Path $buildRoot | Out-Null
    Invoke-Sweep
    Invoke-Required "go" @("build", "-o", $dispatchBinary, "./cmd/dur035-dispatch")
    Invoke-Required $dispatchBinary @("-output", $OutputPath, "-repeats", "3")

    if (-not (Test-Path -LiteralPath $OutputPath)) { throw "DUR-035 did not write $OutputPath." }
    $artifact = Get-Content -Raw -LiteralPath $OutputPath | ConvertFrom-Json
    if ($artifact.status -ne "PASS") { throw "DUR-035 artifact status is $($artifact.status): $($artifact.failure)" }
    if ([int]$artifact.runs.Count -ne 12 -or [int]$artifact.protocol.measured_workflows -ne 12 -or
        [int]$artifact.protocol.worker_slots -ne 4 -or -not [bool]$artifact.protocol.same_task_outbox_record -or
        -not [bool]$artifact.protocol.same_worker_claim_api) {
        throw "DUR-035 artifact failed its protocol acceptance checks."
    }
    foreach ($run in $artifact.runs) {
        if ($run.status -ne "PASS" -or [int]$run.terminal -ne [int]$run.workflow_count -or [int]$run.pending -ne 0 -or
            [int]$run.timings.terminal_latency_ms.count -ne [int]$run.workflow_count) {
            throw "DUR-035 run $($run.configuration)/$($run.repeat) failed reconciliation."
        }
    }
    Invoke-Sweep
    Write-Host "DUR-035 dispatch-path study passed: 12 measured runs; evidence written to $OutputPath"
} finally {
    if ($relaysStopped) {
        try {
            Invoke-Required "docker" ($composeArgs + @("up", "-d", "--wait", "runtime-a", "runtime-b", "worker-a", "worker-b"))
        } catch {
            Write-Error "Could not restore runtime/worker services after DUR-035: $($_.Exception.Message)"
        }
    }
    if (Test-Path -LiteralPath $buildRoot) {
        Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    Pop-Location
}
