function Read-Dur050D022Preflight {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$Path,
        [Parameter(Mandatory)] [ValidatePattern('^[A-Za-z0-9-]{1,32}$')] [string]$CycleID,
        [int]$MaxAgeMinutes = 240,
        [DateTimeOffset]$Now = [DateTimeOffset]::UtcNow
    )

    if ($MaxAgeMinutes -le 0) { throw 'D022 preflight maximum age must be positive.' }
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "D022 preflight file is missing: $Path" }
    try { $record = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "D022 preflight JSON is invalid: $Path ($($_.Exception.Message))" }
    if ([string]$record.schema -ne 'dur050-d022-preflight.v1') { throw 'D022 preflight schema must be dur050-d022-preflight.v1.' }
    if ([string]$record.status -ne 'PASS') { throw 'D022 preflight status must be PASS.' }
    if ([string]$record.cycle_id -ne $CycleID) { throw "D022 preflight cycle_id must match $CycleID." }
    if ([string]$record.account_id -ne '372206265946') { throw 'D022 preflight account_id must be 372206265946.' }
    if ([string]$record.region -ne 'us-west-1') { throw 'D022 preflight region must be us-west-1.' }

    $peak = 0.0
    $peakValue = $record.planned_peak_vcpu
    if ($null -eq $peakValue -or $peakValue -is [bool] -or $peakValue -isnot [ValueType] -or
        -not [double]::TryParse([string]$peakValue, [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$peak) -or
        [double]::IsNaN($peak) -or [double]::IsInfinity($peak)) {
        throw 'D022 preflight must record numeric planned_peak_vcpu.'
    }
    if ($peak -le 0 -or $peak -gt 32) { throw 'planned_peak_vcpu must be in (0, 32].' }

    $checked = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse([string]$record.checked_at_utc, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$checked)) {
        throw 'D022 preflight checked_at_utc must be a valid timestamp.'
    }
    $age = ($Now.ToUniversalTime() - $checked.ToUniversalTime()).TotalMinutes
    if ($age -lt -5) { throw 'D022 preflight timestamp is more than five minutes in the future.' }
    if ($age -gt $MaxAgeMinutes) { throw "D022 preflight is stale ($([math]::Round($age, 1)) minutes; maximum $MaxAgeMinutes)." }
    if ([string]$record.ledger_check_path -notmatch '\S') { throw 'D022 preflight ledger_check_path is required.' }
    return $record
}
