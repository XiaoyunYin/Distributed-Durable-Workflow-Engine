[CmdletBinding()]
param([string]$Commit = 'HEAD', [switch]$KeepOnFailure)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$project = 'dur032-check-' + [guid]::NewGuid().ToString('N').Substring(0,8)
$checkout = Join-Path $RepoRoot "bin/$project"
$variables = @('COMPOSE_PROJECT_NAME','GOCACHE','UV_CACHE_DIR','UV_PYTHON_PREFERENCE',
    'PYTEST_ADDOPTS','DATABASE_URL','DURABLE_DATABASE_URL','DURABLE_REQUIRE_DATABASE','DURABLE_RUN_INTEGRATION')
$previous = @{}
foreach ($name in $variables) { $previous[$name] = [Environment]::GetEnvironmentVariable($name,'Process') }
$created = $false
$passed = $false
Push-Location $RepoRoot
try {
    & git merge-base --is-ancestor $Commit HEAD
    if ($LASTEXITCODE -ne 0) { throw 'Reproduction target must be reachable from HEAD.' }
    & git worktree add --detach $checkout $Commit
    if ($LASTEXITCODE -ne 0) { throw 'Could not create clean worktree.' }
    Set-Location $checkout
    if (& git status --porcelain) { throw 'Fresh worktree is dirty.' }
    $env:COMPOSE_PROJECT_NAME = $project
    $existing = & docker ps -a --filter "label=com.docker.compose.project=$project" --format '{{.ID}}'
    if ($LASTEXITCODE -ne 0 -or $existing) { throw 'Docker unavailable or project is not fresh.' }
    $existingVolumes = & docker volume ls --filter "label=com.docker.compose.project=$project" --format '{{.Name}}'
    if ($LASTEXITCODE -ne 0 -or $existingVolumes) { throw 'Project volumes must not already exist.' }
    $portNames = @('POSTGRES_PORT','KAFKA_PORT','RUNTIME_A_PORT','RUNTIME_B_PORT',
        'WORKER_A_PORT','WORKER_B_PORT','OTEL_GRPC_PORT','OTEL_HTTP_PORT','OTEL_HEALTH_PORT','PROMETHEUS_PORT')
    $basePort = Get-Random -Minimum 22000 -Maximum 29000
    $config = Get-Content '.env.example' -Raw
    $password = [guid]::NewGuid().ToString('N')
    $config = $config.Replace('replace-with-a-local-secret',$password)
    for ($index=0; $index -lt $portNames.Count; $index++) {
        $port = $basePort+$index
        $listener = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback,$port)
        try { $listener.Start() } finally { $listener.Stop() }
        $config = [regex]::Replace($config,"(?m)^$($portNames[$index])=.*$","$($portNames[$index])=$port")
    }
    [IO.File]::WriteAllText((Join-Path $checkout '.env'),$config)
    $env:GOCACHE = Join-Path $RepoRoot 'bin/dur032-selfcheck/go-cache'
    $env:UV_CACHE_DIR = Join-Path $RepoRoot 'bin/dur032-selfcheck/uv-cache'
    $env:UV_PYTHON_PREFERENCE = 'only-system'
    New-Item -ItemType Directory -Force -Path (Join-Path $checkout 'bin') | Out-Null
    $env:PYTEST_ADDOPTS = '--basetemp=bin/pytest-temp -o cache_dir=bin/pytest-cache'
    $env:DATABASE_URL = "postgresql://durable:${password}@127.0.0.1:${basePort}/durable?sslmode=disable"
    $env:DURABLE_DATABASE_URL = $env:DATABASE_URL
    $env:DURABLE_REQUIRE_DATABASE = '0'
    $env:DURABLE_RUN_INTEGRATION = '0'
    $created = $true
    & ./scripts/bootstrap.ps1 -StartServices
    if ($LASTEXITCODE -ne 0) { throw 'Clean bootstrap failed.' }
    & ./scripts/local-demo.ps1
    if ($LASTEXITCODE -ne 0) { throw 'Deployed demo failed.' }
    & ./scripts/ci.ps1 -WithServices -WithRace
    if ($LASTEXITCODE -ne 0) { throw 'Final service CI failed.' }
    & ./scripts/restart-smoke.ps1
    if ($LASTEXITCODE -ne 0) { throw 'Restart smoke failed.' }
    & ./scripts/local-demo.ps1
    if ($LASTEXITCODE -ne 0) { throw 'Post-restart deployed demo failed.' }
    & ./scripts/smoke.ps1
    if ($LASTEXITCODE -ne 0) { throw 'Post-restart smoke failed.' }
    if (& git status --porcelain) { throw 'Reproduction changed tracked files.' }
    $passed = $true
    Write-Host "PASS: clean checkout $Commit, fresh project $project, fresh volumes, deployed demo, service/race CI, restart."
} finally {
    # Only the uniquely generated test project can be torn down. Preserve a
    # failed environment on request for diagnosis; never touch the user's stack.
    if ($created -and ($passed -or -not $KeepOnFailure)) {
        if ($project -notmatch '^dur032-check-[0-9a-f]{8}$') { throw 'Unsafe cleanup project.' }
        Set-Location $checkout
        & docker compose -p $project --env-file .env -f deploy/local/compose.yaml down --volumes
        if ($LASTEXITCODE -ne 0) { Write-Warning "Test-project cleanup failed: $project" }
    }
    Set-Location $RepoRoot
    foreach ($name in $variables) { [Environment]::SetEnvironmentVariable($name,$previous[$name],'Process') }
    # Keep the ignored checkout (including logs/caches) for audit. No broad
    # recursive filesystem removal is needed to validate or release the code.
    Write-Host "Reproduction checkout: $checkout (project $project; passed=$passed)."
    Pop-Location
}
