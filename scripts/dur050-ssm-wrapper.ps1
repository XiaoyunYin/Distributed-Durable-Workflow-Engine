function New-Dur050SsmBashCommand {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$Stage,
        [Parameter(Mandatory)] [string[]]$RemoteLines
    )

    if ($RemoteLines.Count -eq 0) { throw "SSM stage '$Stage' has no remote commands." }
    $stageSlug = $Stage -replace '[^A-Za-z0-9_-]', '-'
    if ([string]::IsNullOrWhiteSpace($stageSlug)) { throw "SSM stage name is invalid." }
    foreach ($line in $RemoteLines) {
        if ($line -match '^\s*(?:/|\.?/|scripts/)[^\s;|&]*\.sh(?:\s|$)' -and $line -notmatch '^\s*bash\s+') {
            throw "SSM shell scripts must be invoked as bash <script>: $line"
        }
    }

    # AWS-RunShellScript invokes /bin/sh. Send source as base64 data, materialize
    # it remotely, and explicitly run Bash so pipefail and Bash syntax work.
    $scriptText = [string]::Join([string][char]10, $RemoteLines).TrimEnd([char[]]@([char]13, [char]10)) + [char]10
    $encoded = [Convert]::ToBase64String([System.Text.UTF8Encoding]::new($false).GetBytes($scriptText))
    return ('umask 077; tmp=$(mktemp /tmp/dur050-{0}.XXXXXX) || exit 125; trap ''rm -f "$tmp"'' 0 HUP INT TERM; printf ''%s'' ''{1}'' | base64 -d > "$tmp" || {{ rc=$?; exit "$rc"; }}; bash "$tmp"; rc=$?; rm -f "$tmp" || exit $?; trap - 0 HUP INT TERM; printf ''DUR050_SSM_TEMP_REMOVED=%s\n'' "$tmp"; exit "$rc"' -f $stageSlug, $encoded)
}
