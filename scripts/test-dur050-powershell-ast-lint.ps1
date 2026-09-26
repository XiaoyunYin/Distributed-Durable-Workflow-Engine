$ErrorActionPreference = 'Stop'
$lint = Join-Path $PSScriptRoot 'lint-powershell-array-concatenation.ps1'
$pwsh = Join-Path $PSHOME 'pwsh'
$current = & $pwsh -NoProfile -File $lint
if ($LASTEXITCODE -ne 0) { throw "Repository PowerShell AST lint failed: $current" }

$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('dur050-ast-lint-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
function Write-Utf8NoBom([string]$Path, [string]$Text) {
    [System.IO.File]::WriteAllText($Path, $Text, [System.Text.UTF8Encoding]::new($false))
}
function Invoke-Lint([string]$Path, [string]$Linter = $lint) {
    $output = & $pwsh -NoProfile -File $Linter -Path $Path 2>&1
    return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Text = ($output | Out-String) }
}
function Assert-LintRejects([string]$Name, [string]$Source, [string]$Linter = $lint) {
    $path = Join-Path $temp ($Name + '.ps1')
    Write-Utf8NoBom $path $Source
    $result = Invoke-Lint $path $Linter
    if ($result.ExitCode -eq 0 -or $result.Text -notmatch 'ambiguous.*array operand/element') {
        throw "$Name survived the PowerShell AST lint: $($result.Text)"
    }
    return $path
}
try {
    $case1 = @'
$x = @('a', 'x "' + $v + '"')
'@
    $case2 = @'
$x = 'x', $v + $w
'@
    $case3 = @'
$x = @('x', $v + $w)
'@
    $ancestorOnly = @'
$x = @('x "' + $v + '"')
'@
    $legacy = @'
$restore = @(
        'docker run --rm --user 0:0 -v durable-aws-dependencies_postgres-data:/restore -v "' + $PostgresBaselineArchive + ':/baseline.tar:ro" --entrypoint bash "$postgres_image" -ec ''tar -xpf /baseline.tar -C /restore''',
        'docker run --rm --user 0:0 -v durable-aws-dependencies_kafka-data:/restore -v "' + $KafkaBaselineArchive + ':/baseline.tar:ro" --entrypoint bash "$postgres_image" -ec ''tar -xpf /baseline.tar -C /restore''',
        'set_env DUR050_ADMISSION_MAX_ACTIVE ' + $admissionMaxActive,
        'set_env DUR050_ADMISSION_MAX_PENDING_OUTBOX ' + $admissionMaxPendingOutbox,
        'set_env DUR050_RECORD_TRANSACTION_TIMINGS ' + $TransactionTimingCapture,
        'set_env DUR049_RECORD_LEASE_ACQUISITIONS 0'
)
'@
    $invalid = @($case1, $case2, $case3, $legacy, $ancestorOnly)
    $invalidPaths = @()
    for ($index = 0; $index -lt $invalid.Count; $index++) {
        $invalidPaths += Assert-LintRejects ("invalid-{0}" -f $index) $invalid[$index]
    }
    Write-Host 'PASS: AST lint rejects all three requested forms and the verbatim 49663c8 restore/configure expressions.'

    $validPath = Join-Path $temp 'parenthesized.ps1'
    Write-Utf8NoBom $validPath @'
$x = @('a', ('x "' + $v + '"'))
$y = 'x', ($v + $w)
$z = @('x', ($v + $w))
$restore = @(
        ('docker run --rm -v "' + $PostgresBaselineArchive + ':/baseline.tar:ro"'),
        ('set_env DUR050_ADMISSION_MAX_ACTIVE ' + $admissionMaxActive)
)
'@
    $valid = Invoke-Lint $validPath
    if ($valid.ExitCode -ne 0) { throw "Parenthesized array expressions were rejected: $($valid.Text)" }
    Write-Host 'PASS: parenthesized expressions remain accepted.'

    $lintSource = Get-Content -LiteralPath $lint -Raw
    $withoutDirect = Join-Path $temp 'lint-without-direct-array-branch.ps1'
    $directCondition = 'if ($directArrayOperand -or ($insideUnparenthesizedArrayElement -and $stringOperand)) {'
    if (-not $lintSource.Contains($directCondition)) { throw 'Could not find the direct-array branch for its mutation control.' }
    Write-Utf8NoBom $withoutDirect ($lintSource.Replace($directCondition, 'if (($insideUnparenthesizedArrayElement -and $stringOperand)) {'))
    $directMutant = Invoke-Lint $invalidPaths[1] $withoutDirect
    if ($directMutant.ExitCode -ne 0) { throw 'The direct-array fixture overlaps the ancestor branch; mutation control is not isolated.' }
    Write-Host 'PASS: removing the direct-array branch makes its dedicated fixture survive (mutation detected).'

    $withoutAncestor = Join-Path $temp 'lint-without-array-element-branch.ps1'
    $ancestorCondition = '($insideUnparenthesizedArrayElement -and $stringOperand)'
    if (-not $lintSource.Contains($ancestorCondition)) { throw 'Could not find the array-element branch for its mutation control.' }
    Write-Utf8NoBom $withoutAncestor ($lintSource.Replace($ancestorCondition, '$false'))
    $ancestorMutant = Invoke-Lint $invalidPaths[4] $withoutAncestor
    if ($ancestorMutant.ExitCode -ne 0) { throw 'The ancestor fixture overlaps the direct-array branch; mutation control is not isolated.' }
    Write-Host 'PASS: removing the array-element branch makes its dedicated fixture survive (mutation detected).'
} finally {
    if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Recurse -Force }
}
