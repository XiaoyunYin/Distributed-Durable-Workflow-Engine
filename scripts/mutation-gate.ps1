[CmdletBinding()]
param(
    [string]$Output = "mutations/results-powershell.json"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    & python $PSScriptRoot/mutation-runner.py --output $Output
    if ($LASTEXITCODE -ne 0) { throw "DUR-046 mutation gate failed." }
    Write-Host "DUR-046 PowerShell mutation gate passed."
} finally {
    Pop-Location
}
