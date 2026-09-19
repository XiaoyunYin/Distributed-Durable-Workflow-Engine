[CmdletBinding()]
param(
    [string]$OutputPath = "experiments/m7/dur029"
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = Join-Path (Get-Location) "python"
uv run python -m incident_agent dur029-preflight --output $OutputPath
if ($LASTEXITCODE -ne 0) { throw "DUR-029 preflight failed with exit code $LASTEXITCODE" }
uv run python -m incident_agent dur029-live-status --output $OutputPath
if ($LASTEXITCODE -ne 0) { throw "DUR-029 live status failed with exit code $LASTEXITCODE" }
Write-Host "DUR-029 deterministic retrieval final and controls written to $OutputPath"
