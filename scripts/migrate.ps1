$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    $config = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
    $databaseUser = (Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_USER=' }) -replace '^POSTGRES_USER=', ''
    $databaseName = (Get-Content -LiteralPath ".env" | Where-Object { $_ -match '^POSTGRES_DB=' }) -replace '^POSTGRES_DB=', ''

    foreach ($migration in Get-ChildItem -LiteralPath "migrations" -Filter "*.up.sql" | Sort-Object Name) {
        Write-Host "Applying $($migration.Name)"
        Get-Content -LiteralPath $migration.FullName -Raw |
            & docker @config exec -T postgres psql -v ON_ERROR_STOP=1 -U $databaseUser -d $databaseName
        if ($LASTEXITCODE -ne 0) { throw "Migration $($migration.Name) failed." }
    }
} finally {
    Pop-Location
}
