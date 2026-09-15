[CmdletBinding()]
param(
    [switch]$StartServices
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    foreach ($command in @("git", "go", "python", "uv", "docker")) {
        if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
            throw "Required command '$command' was not found on PATH."
        }
    }

    if (-not (Test-Path -LiteralPath ".env")) {
        $password = [guid]::NewGuid().ToString("N")
        $example = Get-Content -LiteralPath ".env.example" -Raw
        $local = $example.Replace("replace-with-a-local-secret", $password)
        [System.IO.File]::WriteAllText((Join-Path $RepoRoot ".env"), $local)
        Write-Host "Created gitignored .env with a generated local database password."
    }

    & go version
    if ($LASTEXITCODE -ne 0) { throw "Go toolchain setup failed." }

    & uv sync --frozen
    if ($LASTEXITCODE -ne 0) { throw "Python dependency installation failed." }

    & docker compose --env-file .env -f deploy/local/compose.yaml config --quiet
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose configuration is invalid." }

    if ($StartServices) {
        & docker compose --env-file .env -f deploy/local/compose.yaml up -d --build --wait
        if ($LASTEXITCODE -ne 0) { throw "Local services failed to start." }
        & $PSScriptRoot/migrate.ps1
        & $PSScriptRoot/smoke.ps1
    }
} finally {
    Pop-Location
}
