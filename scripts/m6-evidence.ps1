[CmdletBinding()]
param(
    [string]$OutputPath = "experiments/m6"
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = Join-Path (Get-Location) "python"
uv run python -m incident_agent fixtures --output $OutputPath
uv run python -m incident_agent benchmark --output $OutputPath
uv run python -m incident_agent continuity --output $OutputPath
uv run python -m incident_agent adversarial --output $OutputPath
Write-Host "M6 deterministic evidence written to $OutputPath"
