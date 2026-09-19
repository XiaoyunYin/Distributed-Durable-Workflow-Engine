[CmdletBinding()]
param(
    [string]$OutputPath = "experiments/m7/dur029"
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = Join-Path (Get-Location) "python"

if (Test-Path -LiteralPath ".env") {
    foreach ($line in Get-Content -LiteralPath ".env") {
        if ($line -match '^\s*([^#][^=]*)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            if ($value.Length -ge 2 -and $value.StartsWith('"') -and $value.EndsWith('"')) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            Set-Item -Path "Env:$name" -Value $value
        }
    }
}

if (-not $env:OPENAI_API_KEY) { throw "OPENAI_API_KEY is required for DUR-029 live evaluation" }
$env:INCIDENT_LIVE_APPROVED = "1"
$env:DUR029_PROVIDER = "openai"
$env:DUR029_MODEL = "gpt-4o-mini"
$env:DUR029_BUDGET_CENTS = "3000"

uv run python -m incident_agent dur029-live --output $OutputPath
if ($LASTEXITCODE -ne 0) { throw "DUR-029 live evaluation failed with exit code $LASTEXITCODE" }
Write-Host "DUR-029 live evaluation written to $OutputPath"
