[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$Stage,
    [Parameter(Mandatory)] [string]$InstanceID,
    [Parameter(Mandatory)] [string[]]$RemoteLines,
    [string]$Region = "us-west-1",
    [int]$TimeoutMinutes = 20,
    [ValidateRange(0, 30)] [int]$PollIntervalSeconds = 2,
    [string]$ResultPath
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "dur050-ssm-wrapper.ps1")
if ($Stage -notmatch '^[A-Za-z0-9_-]+$') { throw "Stage must be a simple identifier." }
if ($InstanceID -notmatch '^i-[0-9a-f]+$') { throw "InstanceID must be an EC2 instance ID." }
if ($Region -ne "us-west-1") { throw "DUR-050 SSM commands are restricted to us-west-1." }
if ($TimeoutMinutes -lt 1 -or $TimeoutMinutes -gt 240) { throw "TimeoutMinutes must be between 1 and 240." }
if ($ResultPath -and (Test-Path -LiteralPath $ResultPath)) { throw "Refusing to overwrite SSM result record: $ResultPath" }

$inputFile = Join-Path ([System.IO.Path]::GetTempPath()) ("dur050-ssm-" + [guid]::NewGuid().ToString("N") + ".json")
try {
    $command = New-Dur050SsmBashCommand -Stage $Stage -RemoteLines $RemoteLines
    $body = @{
        DocumentName = "AWS-RunShellScript"
        InstanceIds = @($InstanceID)
        Comment = "DUR-050 $Stage"
        Parameters = @{ commands = @($command) }
    } | ConvertTo-Json -Depth 8 -Compress
    [System.IO.File]::WriteAllText($inputFile, $body, [System.Text.UTF8Encoding]::new($false))

    $sent = & aws --region $Region ssm send-command --cli-input-json "file://$inputFile" --output json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or -not $sent.Command.CommandId) { throw "Could not submit DUR-050 SSM stage '$Stage'." }
    $commandID = [string]$sent.Command.CommandId
    $deadline = [DateTime]::UtcNow.AddMinutes($TimeoutMinutes)
    $invocation = $null
    do {
        if ($PollIntervalSeconds -gt 0) { Start-Sleep -Seconds $PollIntervalSeconds }
        $raw = & aws --region $Region ssm get-command-invocation --command-id $commandID --instance-id $InstanceID --output json 2>$null
        if ($LASTEXITCODE -eq 0) { $invocation = $raw | ConvertFrom-Json } else { $invocation = $null }
        if ($null -ne $invocation -and $invocation.Status -in @("Success", "Failed", "Cancelled", "TimedOut", "Undeliverable", "Terminated")) { break }
    } while ([DateTime]::UtcNow -lt $deadline)

    if ($ResultPath) {
        $result = [ordered]@{
            schema = 'dur050-ssm-command-result.v1'
            stage = $Stage
            instance_id = $InstanceID
            command_id = $commandID
            status = if ($null -eq $invocation) { 'UNKNOWN' } else { [string]$invocation.Status }
            standard_output_content = if ($null -eq $invocation) { '' } else { [string]$invocation.StandardOutputContent }
            standard_error_content = if ($null -eq $invocation) { '' } else { [string]$invocation.StandardErrorContent }
        }
        $parent = Split-Path -Parent ([System.IO.Path]::GetFullPath($ResultPath))
        if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
        $tempResult = "$ResultPath.$([guid]::NewGuid().ToString('N')).tmp"
        [System.IO.File]::WriteAllText($tempResult, ($result | ConvertTo-Json -Depth 5), [System.Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $tempResult -Destination $ResultPath
    }
    if ($null -eq $invocation) { throw "No SSM invocation result before timeout for '$Stage' (command $commandID)." }
    if ($invocation.StandardOutputContent) { Write-Output $invocation.StandardOutputContent }
    if ($invocation.Status -ne "Success") {
        if ($invocation.StandardErrorContent) { Write-Error $invocation.StandardErrorContent }
        throw "DUR-050 SSM stage '$Stage' ended with $($invocation.Status)."
    }
} finally {
    Remove-Item -LiteralPath $inputFile -Force -ErrorAction SilentlyContinue
}
