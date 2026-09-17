$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    $config = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
    $databaseUser = (Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_USER=' }) -replace '^POSTGRES_USER=', ''
    $databaseName = (Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_DB=' }) -replace '^POSTGRES_DB=', ''

    foreach ($migration in Get-ChildItem -LiteralPath "migrations" -Filter "*.up.sql" | Sort-Object Name) {
        $match = [regex]::Match($migration.BaseName, '^\d+')
        if (-not $match.Success) { throw "Migration filename '$($migration.Name)' has no numeric version prefix." }
        $version = [long]$match.Value
        $schemaPresent = & docker @config exec -T postgres psql -At -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName -c "SELECT CASE WHEN to_regclass('engine.schema_migrations') IS NULL THEN '0' ELSE '1' END;"
        if ($LASTEXITCODE -ne 0) { throw "Could not inspect the migration ledger." }
        if (($schemaPresent | Out-String).Trim() -eq '1') {
            $applied = & docker @config exec -T postgres psql -At -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName -c "SELECT CASE WHEN EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version = $version) THEN '1' ELSE '0' END;"
            if ($LASTEXITCODE -ne 0) { throw "Could not inspect migration version $version." }
        } else {
            $applied = '0'
        }
        if (($applied | Out-String).Trim() -eq '1') {
            Write-Host "Skipping already applied $($migration.Name)"
            continue
        }
        Write-Host "Applying $($migration.Name)"
        Get-Content -LiteralPath $migration.FullName -Raw |
            & docker @config exec -T postgres psql -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName
        if ($LASTEXITCODE -ne 0) { throw "Migration $($migration.Name) failed." }
        $confirmed = & docker @config exec -T postgres psql -At -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName -c "SELECT CASE WHEN EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version = $version) THEN '1' ELSE '0' END;"
        if ($LASTEXITCODE -ne 0 -or (($confirmed | Out-String).Trim() -ne '1')) {
            throw "Migration $($migration.Name) did not record version $version in engine.schema_migrations."
        }
    }
} finally {
    Pop-Location
}
