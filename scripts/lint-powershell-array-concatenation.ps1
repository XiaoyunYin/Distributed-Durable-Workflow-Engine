[CmdletBinding()]
param(
    [string[]]$Path
)

$ErrorActionPreference = 'Stop'
$files = if ($Path) {
    @($Path | ForEach-Object { (Resolve-Path -LiteralPath $_ -ErrorAction Stop).Path })
} else {
    @(Get-ChildItem -LiteralPath $PSScriptRoot -Filter '*.ps1' -File | ForEach-Object FullName)
}
$violations = [System.Collections.Generic.List[string]]::new()

foreach ($file in $files) {
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile($file, [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count -gt 0) {
        $violations.Add("$($file): PowerShell parse error: $($parseErrors[0].Message)")
        continue
    }

    $plusNodes = $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.BinaryExpressionAst] -and
        $node.Operator -eq [System.Management.Automation.Language.TokenKind]::Plus
    }, $true)
    foreach ($node in $plusNodes) {
        $directArrayOperand =
            $node.Left -is [System.Management.Automation.Language.ArrayLiteralAst] -or
            $node.Right -is [System.Management.Automation.Language.ArrayLiteralAst]
        $stringOperand =
            $node.Left -is [System.Management.Automation.Language.StringConstantExpressionAst] -or
            $node.Left -is [System.Management.Automation.Language.ExpandableStringExpressionAst] -or
            $node.Right -is [System.Management.Automation.Language.StringConstantExpressionAst] -or
            $node.Right -is [System.Management.Automation.Language.ExpandableStringExpressionAst]
        $ancestor = $node.Parent
        $insideUnparenthesizedArrayElement = $false
        while ($ancestor) {
            if ($ancestor -is [System.Management.Automation.Language.ParenExpressionAst]) { break }
            if ($ancestor -is [System.Management.Automation.Language.ArrayExpressionAst] -or
                $ancestor -is [System.Management.Automation.Language.ArrayLiteralAst]) {
                $insideUnparenthesizedArrayElement = $true
                break
            }
            $ancestor = $ancestor.Parent
        }
        if ($directArrayOperand -or ($insideUnparenthesizedArrayElement -and $stringOperand)) {
            $violations.Add("$($file):$($node.Extent.StartLineNumber): ambiguous '+' with an array operand/element; parenthesize a single expression or use -f. Expression: $($node.Extent.Text)")
        }
    }
}

if ($violations.Count -gt 0) {
    $violations | ForEach-Object { Write-Error $_ }
    exit 1
}
Write-Host "PASS: PowerShell AST array-concatenation lint checked $($files.Count) file(s)."
