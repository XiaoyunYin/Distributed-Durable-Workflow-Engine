[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$previousDatabase = $env:DATABASE_URL
$previousGoCache = $env:GOCACHE
Push-Location $RepoRoot
try {
    $values = @{}
    foreach ($line in Get-Content -LiteralPath '.env') {
        if ($line -match '^([A-Z_]+)=(.*)$') { $values[$matches[1]] = $matches[2] }
    }
    $user = [uri]::EscapeDataString($values['POSTGRES_USER'])
    $password = [uri]::EscapeDataString($values['POSTGRES_PASSWORD'])
    $env:DATABASE_URL = 'postgresql://{0}:{1}@127.0.0.1:{2}/{3}?sslmode=disable' -f $user,$password,$values['POSTGRES_PORT'],$values['POSTGRES_DB']
    $env:GOCACHE = Join-Path $RepoRoot '.scratch/dur045/gocache-local-demo'
    New-Item -ItemType Directory -Force -Path $env:GOCACHE | Out-Null
    & go run ./cmd/local-demo -api "http://127.0.0.1:$($values['RUNTIME_A_PORT'])"
    if ($LASTEXITCODE -ne 0) { throw 'Deployed workflow demo failed.' }
} finally {
    $env:DATABASE_URL = $previousDatabase
    $env:GOCACHE = $previousGoCache
    Pop-Location
}
