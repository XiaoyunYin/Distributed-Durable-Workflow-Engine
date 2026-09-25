function Get-Dur050BaselineHashVerificationLines {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$PostgresArchive,
        [Parameter(Mandatory)] [ValidatePattern('^[0-9a-f]{64}$')] [string]$PostgresSHA256,
        [Parameter(Mandatory)] [string]$KafkaArchive,
        [Parameter(Mandatory)] [ValidatePattern('^[0-9a-f]{64}$')] [string]$KafkaSHA256
    )

    foreach ($path in @($PostgresArchive, $KafkaArchive)) {
        if ($path -notmatch '^/[A-Za-z0-9._/-]+$') { throw "Unsafe archive path: $path" }
    }
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add('postgres_archive="' + $PostgresArchive + '"')
    $lines.Add('kafka_archive="' + $KafkaArchive + '"')
    $lines.Add('postgres_expected_sha256="' + $PostgresSHA256 + '"')
    $lines.Add('kafka_expected_sha256="' + $KafkaSHA256 + '"')
    $lines.Add('postgres_actual_sha256=$(sha256sum -- "$postgres_archive" | awk ''{print $1}'')')
    $lines.Add('kafka_actual_sha256=$(sha256sum -- "$kafka_archive" | awk ''{print $1}'')')
    $lines.Add('test "$postgres_actual_sha256" = "$postgres_expected_sha256" || { echo "PostgreSQL baseline archive SHA-256 mismatch" >&2; exit 41; }')
    $lines.Add('test "$kafka_actual_sha256" = "$kafka_expected_sha256" || { echo "Kafka baseline archive SHA-256 mismatch" >&2; exit 42; }')
    return $lines.ToArray()
}
