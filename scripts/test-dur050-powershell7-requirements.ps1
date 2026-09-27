#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot

function Assert-Dur050PowerShellRequirements([string]$Root) {
    $operatorFiles = @(Get-ChildItem -LiteralPath $Root -File | Where-Object { $_.Name -match '^dur050-.*\.(ps1|psm1)$' })
    if ($operatorFiles.Count -eq 0) { throw "No DUR-050 operator PowerShell files found under $Root." }
    foreach ($file in $operatorFiles) {
        $firstLine = Get-Content -LiteralPath $file.FullName -TotalCount 1
        if ($firstLine -cne '#Requires -Version 7.0') { throw "PowerShell 7 requirement is missing as the first line of $($file.Name)." }
    }
    return $operatorFiles.Count
}

$count = Assert-Dur050PowerShellRequirements -Root $PSScriptRoot
Write-Host "PASS: all $count DUR-050 PowerShell operator/module files require PowerShell 7.0 or newer."

$temp = Join-Path ([IO.Path]::GetTempPath()) ('dur050-ps7-lint-mutant-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
try {
    $source = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'dur050-ledger-record.ps1') -Raw
    $mutated = [regex]::Replace($source, '(?m)^#Requires -Version 7\.0\r?\n', '', 1)
    if ($mutated -eq $source) { throw 'Could not create the requirement-removal mutant.' }
    [IO.File]::WriteAllText((Join-Path $temp 'dur050-ledger-record.ps1'), $mutated, [Text.UTF8Encoding]::new($false))
    try { [void](Assert-Dur050PowerShellRequirements -Root $temp); throw 'The requirement-removal mutant passed the lint.' }
    catch { if ($_.Exception.Message -notmatch 'requirement is missing') { throw } }
    Write-Host 'PASS: removing the PowerShell 7 requirement from a DUR-050 script is rejected by the lint.'
} finally { Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue }
