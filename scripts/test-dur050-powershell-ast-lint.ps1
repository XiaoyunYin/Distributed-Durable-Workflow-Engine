$ErrorActionPreference = 'Stop'
$lint = Join-Path $PSScriptRoot 'lint-powershell-array-concatenation.ps1'
$current = & (Join-Path $PSHOME 'pwsh') -NoProfile -File $lint
if ($LASTEXITCODE -ne 0) { throw "Repository PowerShell AST lint failed: $current" }

$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('dur050-ast-lint-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
try {
    $legacy = Join-Path $temp 'legacy-reset-fragment.ps1'
    $legacySource = @'
$restore = @(
    'docker run -v "' + $PostgresBaselineArchive + ':/baseline.tar:ro"'
)
'@
    [System.IO.File]::WriteAllText($legacy, $legacySource, [System.Text.UTF8Encoding]::new($false))
    $rejected = & (Join-Path $PSHOME 'pwsh') -NoProfile -File $lint -Path $legacy 2>&1
    if ($LASTEXITCODE -eq 0 -or ($rejected | Out-String) -notmatch 'ambiguous.*array operand/element') {
        throw "Historical unparenthesized array concatenation survived the AST lint: $rejected"
    }
    Write-Host 'PASS: AST lint negative control rejects the unparenthesized R163-style array element.'

    $gitCommand = Get-Command git -ErrorAction SilentlyContinue
    if ($gitCommand) {
        $repoRoot = Split-Path -Parent $PSScriptRoot
        $historicalSource = & $gitCommand.Source -C $repoRoot show '49663c8:scripts/dur050-reset-block.ps1' 2>$null
        if ($LASTEXITCODE -eq 0 -and $historicalSource) {
            $historicalPath = Join-Path $temp 'reset-block-49663c8.ps1'
            [System.IO.File]::WriteAllText($historicalPath, ($historicalSource -join "`n"), [System.Text.UTF8Encoding]::new($false))
            $historicalRejected = & (Join-Path $PSHOME 'pwsh') -NoProfile -File $lint -Path $historicalPath 2>&1
            if ($LASTEXITCODE -eq 0 -or ($historicalRejected | Out-String) -notmatch 'ambiguous.*array operand/element') {
                throw "R163's 49663c8 reset helper survived the AST lint: $historicalRejected"
            }
            Write-Host 'PASS: AST lint rejects the actual 49663c8 reset helper.'
        } else {
            Write-Host 'NOTE: 49663c8 is not present in this checkout; the equivalent R163 syntax fixture was rejected.'
        }
    } else {
        Write-Host 'NOTE: git is not installed in this test image; the equivalent R163 syntax fixture was rejected.'
    }
} finally {
    if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Recurse -Force }
}
