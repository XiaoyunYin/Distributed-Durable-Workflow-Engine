[CmdletBinding()]
param(
    [Parameter(Mandatory)] [ValidatePattern('^i-[0-9a-f]{8,17}$')] [string]$InstanceID,
    [Parameter(Mandatory)] [string]$RemoteDirectory,
    [Parameter(Mandatory)] [string]$DestinationPath,
    [Parameter(Mandatory)] [string]$RecordPath,
    [string]$Region = 'us-west-1',
    [ValidateRange(1, 240)] [int]$TimeoutMinutes = 10,
    [ValidateRange(1000, 17000)] [int]$ChunkBytes = 16500,
    [ValidateRange(0, 30)] [int]$PollIntervalSeconds = 2
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'dur050-ssm-wrapper.ps1')

if ($Region -ne 'us-west-1') { throw 'DUR-050 artifact retrieval is restricted to us-west-1.' }
if ($RemoteDirectory -notmatch '^/var/tmp/dur050-[A-Za-z0-9._/-]+$' -or $RemoteDirectory -match '(^|/)\.\.?(/|$)') {
    throw 'RemoteDirectory must be a normalized path below /var/tmp/dur050-*.'
}
$DestinationPath = [IO.Path]::GetFullPath($DestinationPath)
$RecordPath = [IO.Path]::GetFullPath($RecordPath)
if ($RecordPath -eq $DestinationPath -or $RecordPath.StartsWith($DestinationPath.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'RecordPath must be outside the extracted destination directory.'
}
if (Test-Path -LiteralPath $DestinationPath) { throw "Refusing to overwrite retrieval destination: $DestinationPath" }
if (Test-Path -LiteralPath $RecordPath) { throw "Refusing to overwrite retrieval record: $RecordPath" }
$destinationParent = Split-Path -Parent $DestinationPath
if (-not $destinationParent) { throw 'DestinationPath must have a parent directory.' }
New-Item -ItemType Directory -Force -Path $destinationParent | Out-Null

function Invoke-Dur050Read([string]$Stage, [string[]]$RemoteLines, [string]$InstanceID, [int]$TimeoutMinutes, [int]$PollIntervalSeconds) {
    $resultPath = Join-Path ([IO.Path]::GetTempPath()) ("dur050-transfer-$([guid]::NewGuid().ToString('N')).json")
    try {
        $null = & (Join-Path $PSScriptRoot 'dur050-invoke-ssm-command.ps1') -Stage $Stage -InstanceID $InstanceID -RemoteLines $RemoteLines -Region $Region -TimeoutMinutes $TimeoutMinutes -PollIntervalSeconds $PollIntervalSeconds -ResultPath $resultPath
        if (-not (Test-Path -LiteralPath $resultPath -PathType Leaf)) { throw "SSM stage '$Stage' returned no invocation record." }
        $result = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
        if ($result.status -ne 'Success') { throw "SSM stage '$Stage' ended as $($result.status)." }
        return $result
    } finally {
        Remove-Item -LiteralPath $resultPath -Force -ErrorAction SilentlyContinue
    }
}

function BashQuote([string]$Value) {
    if ($Value -notmatch '^/[A-Za-z0-9._/-]+$' -or $Value -match '(^|/)\.\.?(/|$)') { throw "Unsafe remote path: $Value" }
    return "'$Value'"
}

$chunkSize = $ChunkBytes
$archiveID = [guid]::NewGuid().ToString('N')
$archivePath = "/var/tmp/dur050-transfer-$archiveID.tar.gz"
$archiveQuote = BashQuote $archivePath
$directoryQuote = BashQuote $RemoteDirectory
$archiveRemote = @(
    'set -euo pipefail',
    "source_dir=$directoryQuote",
    "archive=$archiveQuote",
    'test -d "$source_dir"',
    'test ! -e "$archive"',
    'tar -czf "$archive" -C "$(dirname -- "$source_dir")" "$(basename -- "$source_dir")"',
    'chmod 0600 "$archive"',
    'size=$(stat -c %s -- "$archive")',
    'sha=$(sha256sum -- "$archive" | cut -d '' '' -f1)',
    'printf ''DUR050_ARCHIVE_V1 size=%s sha256=%s\n'' "$size" "$sha"'
)
$commands = [System.Collections.Generic.List[object]]::new()
$tempRoot = Join-Path $destinationParent ('.dur050-transfer-' + [guid]::NewGuid().ToString('N'))
$archiveLocal = Join-Path $tempRoot 'payload.tar.gz'
$stagedOutput = Join-Path $tempRoot 'extracted'

try {
    New-Item -ItemType Directory -Path $tempRoot | Out-Null
    $archiveResult = Invoke-Dur050Read "transfer-meta-$archiveID" $archiveRemote $InstanceID $TimeoutMinutes $PollIntervalSeconds
    $commands.Add([ordered]@{ command_id = [string]$archiveResult.command_id; purpose = 'archive-metadata' })
    $archiveStdout = [string]$archiveResult.standard_output_content
    $manifestMatch = [regex]::Matches($archiveStdout, '(?m)^DUR050_ARCHIVE_V1 size=([0-9]+) sha256=([0-9a-f]{64})\s*$')
    if ($manifestMatch.Count -ne 1) { throw 'Remote archive response lacks exactly one valid size/SHA-256 marker.' }
    $remoteSize = [long]$manifestMatch[0].Groups[1].Value
    $remoteSHA = $manifestMatch[0].Groups[2].Value
    if ($remoteSize -le 0) { throw 'Remote archive is empty.' }
    $chunks = [int][Math]::Ceiling($remoteSize / [double]$chunkSize)
    $stream = [IO.File]::Open($archiveLocal, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try {
        for ($index = 0; $index -lt $chunks; $index++) {
            $offset = [long]$index * $chunkSize
            $length = [int][Math]::Min($chunkSize, $remoteSize - $offset)
            $chunkID = '{0}-{1:D6}' -f $archiveID, ($index + 1)
            $chunkLines = @(
                'set -euo pipefail',
                "archive=$archiveQuote",
                "test -f `"`$archive`"",
                "data=`$(dd if=`"`$archive`" iflag=skip_bytes,count_bytes skip=$offset count=$length status=none | base64 -w0)",
                "printf 'DUR050_CHUNK_V1 index=%s offset=%s length=%s data=%s\n' '$($index + 1)' '$offset' '$length' `"`$data`""
            )
            $result = Invoke-Dur050Read "transfer-chunk-$chunkID" $chunkLines $InstanceID $TimeoutMinutes $PollIntervalSeconds
            $commands.Add([ordered]@{ command_id = [string]$result.command_id; purpose = 'archive-chunk'; index = $index + 1; offset = $offset; length = $length })
            $stdout = [string]$result.standard_output_content
            if ($stdout.Length -gt 23900) { throw "SSM chunk response exceeds the safe 23,900-character envelope at chunk $($index + 1)." }
            $pattern = '(?m)^DUR050_CHUNK_V1 index=' + ($index + 1) + ' offset=' + $offset + ' length=' + $length + ' data=([A-Za-z0-9+/=]+)\s*$'
            $matches = [regex]::Matches($stdout, $pattern)
            if ($matches.Count -ne 1) { throw "Chunk $($index + 1) is missing its unique transfer marker or has inconsistent metadata." }
            try { $bytes = [Convert]::FromBase64String($matches[0].Groups[1].Value) } catch { throw "Chunk $($index + 1) is not valid base64: $($_.Exception.Message)" }
            if ($bytes.Length -ne $length) { throw "Chunk $($index + 1) is truncated: received $($bytes.Length), expected $length bytes." }
            $stream.Write($bytes, 0, $bytes.Length)
        }
    } finally { $stream.Dispose() }

    $localSize = (Get-Item -LiteralPath $archiveLocal).Length
    $localSHA = (Get-FileHash -LiteralPath $archiveLocal -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($localSize -ne $remoteSize) { throw "Reassembled archive size mismatch: $localSize != $remoteSize." }
    if ($localSHA -cne $remoteSHA) { throw "Reassembled archive SHA-256 mismatch: $localSHA != $remoteSHA." }

    $python = Get-Command python3 -ErrorAction SilentlyContinue
    if (-not $python) { $python = Get-Command python -ErrorAction SilentlyContinue }
    if (-not $python) { throw 'Python 3 is required for safe, atomic tar extraction.' }
    $extractor = Join-Path $PSScriptRoot 'dur050-safe-extract.py'
    $extractOutput = & $python.Source $extractor --archive $archiveLocal --destination $stagedOutput --expected-size $remoteSize --expected-sha256 $remoteSHA 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $stagedOutput -PathType Container)) { throw "Verified archive extraction failed: $extractOutput" }
    if (Test-Path -LiteralPath $DestinationPath) { throw "Refusing to overwrite retrieval destination: $DestinationPath" }

    $record = [ordered]@{
        schema = 'dur050-generator-block-retrieval.v1'
        status = 'PASS'
        completed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        instance_id = $InstanceID
        remote_directory = $RemoteDirectory
        remote_archive_path = $archivePath
        archive_size_bytes = $remoteSize
        archive_sha256 = $remoteSHA
        verified_local_sha256 = $localSHA
        chunk_bytes = $chunkSize
        chunk_count = $chunks
        ssm_command_ids = @($commands)
        destination = $DestinationPath
        standard_output_limit_chars = 24000
    }
    $recordParent = Split-Path -Parent $RecordPath
    if ($recordParent) { New-Item -ItemType Directory -Force -Path $recordParent | Out-Null }
    $recordTemp = "$RecordPath.$([guid]::NewGuid().ToString('N')).tmp"
    [IO.File]::WriteAllText($recordTemp, ($record | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $stagedOutput -Destination $DestinationPath
    Move-Item -LiteralPath $recordTemp -Destination $RecordPath
    $record | ConvertTo-Json -Depth 8
} catch {
    if (-not (Test-Path -LiteralPath $RecordPath)) {
        $failure = [ordered]@{
            schema = 'dur050-generator-block-retrieval.v1'
            status = 'FAIL'
            failed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
            instance_id = $InstanceID
            remote_directory = $RemoteDirectory
            remote_archive_path = $archivePath
            expected_archive_size_bytes = $remoteSize
            expected_archive_sha256 = $remoteSHA
            chunks_expected = $chunks
            chunks_completed = @($commands | Where-Object purpose -eq 'archive-chunk').Count
            ssm_command_ids = @($commands)
            destination = $DestinationPath
            error = $_.Exception.Message
        }
        $recordParent = Split-Path -Parent $RecordPath
        if ($recordParent) { New-Item -ItemType Directory -Force -Path $recordParent | Out-Null }
        [IO.File]::WriteAllText($RecordPath, ($failure | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))
    }
    throw
} finally {
    if (Test-Path -LiteralPath $tempRoot) { Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue }
}
