[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deploy/local/compose.yaml'
$localEnv = Join-Path $repoRoot '.env'
$image = 'ghcr.io/charmbracelet/vhs@sha256:b1afb4f9d0d27000d5feb7e93882dc5b983574fc19462308ee1d94e843afc67d'
$scratch = Join-Path $repoRoot '.scratch/dur045'
$helper = Join-Path $scratch 'local-demo-linux'
$renderer = Join-Path $scratch 'vhs-linux'
$vhsSource = Join-Path $scratch ('vhs-source-' + [guid]::NewGuid().ToString('N'))
$rendererEnv = Join-Path $scratch ('renderer-' + [guid]::NewGuid().ToString('N') + '.env')
$locationPushed = $false
$helperBuilt = $false
$rendererBuilt = $false
$previousGoos = $env:GOOS
$previousGoarch = $env:GOARCH
$previousCGO = $env:CGO_ENABLED
$previousGoCache = $env:GOCACHE
$previousGoToolchain = $env:GOTOOLCHAIN

if (-not (Test-Path -LiteralPath $localEnv)) {
    throw 'Missing .env. Run scripts/bootstrap.ps1 first.'
}
New-Item -ItemType Directory -Force -Path $scratch | Out-Null

try {
    Push-Location $repoRoot
    $locationPushed = $true

    $values = @{}
    foreach ($line in Get-Content -LiteralPath $localEnv) {
        if ($line -match '^([A-Z_]+)=(.*)$') { $values[$matches[1]] = $matches[2] }
    }
    foreach ($required in @('POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB')) {
        if (-not $values.ContainsKey($required) -or [string]::IsNullOrEmpty($values[$required])) {
            throw "Missing $required in .env."
        }
    }
    if ($values.ContainsKey('RUNTIME_A_PORT') -and $values['RUNTIME_A_PORT'] -ne '8080') {
        throw 'The demo tape expects the Compose-default runtime API port 8080.'
    }

    $runtimeOutput = & docker compose --env-file $localEnv --project-directory $repoRoot -f $composeFile ps -q runtime-a
    if ($LASTEXITCODE -ne 0) { throw 'Could not query runtime-a from Compose.' }
    $runtimeContainer = [string]($runtimeOutput | Select-Object -First 1)
    $runtimeContainer = $runtimeContainer.Trim()
    if ([string]::IsNullOrWhiteSpace($runtimeContainer)) {
        throw 'runtime-a is not running; start the local Compose stack first.'
    }
    $postgresOutput = & docker compose --env-file $localEnv --project-directory $repoRoot -f $composeFile ps -q postgres
    if ($LASTEXITCODE -ne 0) { throw 'Could not query PostgreSQL from Compose.' }
    $postgresContainer = [string]($postgresOutput | Select-Object -First 1)
    $postgresContainer = $postgresContainer.Trim()
    if ([string]::IsNullOrWhiteSpace($postgresContainer)) {
        throw 'PostgreSQL is not running; start the local Compose stack first.'
    }

    $runtimeJson = (& docker inspect $runtimeContainer | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect runtime-a.' }
    $postgresJson = (& docker inspect $postgresContainer | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect PostgreSQL.' }
    $runtimeInfo = @((ConvertFrom-Json -InputObject $runtimeJson))[0]
    $postgresInfo = @((ConvertFrom-Json -InputObject $postgresJson))[0]
    $runtimeNetworks = @($runtimeInfo.NetworkSettings.Networks.PSObject.Properties.Name)
    $postgresNetworks = @($postgresInfo.NetworkSettings.Networks.PSObject.Properties.Name)
    $sharedNetwork = $runtimeNetworks | Where-Object { $postgresNetworks -contains $_ } | Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace($sharedNetwork)) {
        throw 'runtime-a and PostgreSQL do not share a Docker network.'
    }

    $env:GOCACHE = Join-Path $scratch 'gocache'
    $env:GOOS = 'linux'
    $env:GOARCH = 'amd64'
    $env:CGO_ENABLED = '0'
    & go build -o $helper ./cmd/local-demo
    if ($LASTEXITCODE -ne 0) { throw 'Could not build the Linux demo helper.' }
    $helperBuilt = $true

    # VHS v0.12.0 cancels the recording context before its deferred renderer
    # starts FFmpeg. Build the pinned source with a one-line context fix so a
    # successful render cannot silently omit the output file.
    $moduleMetadataText = & go mod download -json 'github.com/charmbracelet/vhs@v0.12.0'
    if ($LASTEXITCODE -ne 0) { throw 'Could not download the pinned VHS v0.12.0 source module.' }
    $moduleMetadata = ($moduleMetadataText | Out-String) | ConvertFrom-Json
    $vhsModule = [string]$moduleMetadata.Dir
    if (-not (Test-Path -LiteralPath (Join-Path $vhsModule 'evaluator.go'))) {
        throw 'VHS v0.12.0 source is not in the Go module cache.'
    }
    Copy-Item -LiteralPath $vhsModule -Destination $vhsSource -Recurse
    & attrib -R (Join-Path $vhsSource '*') /S /D
    if ($LASTEXITCODE -ne 0) { throw 'Could not prepare the pinned VHS source copy.' }
    $evaluatorPath = Join-Path $vhsSource 'evaluator.go'
    $evaluator = [System.IO.File]::ReadAllText($evaluatorPath)
    $lineEnding = if ($evaluator.Contains("`r`n")) { "`r`n" } else { "`n" }
    $contextLine = "`tctx, cancel := context.WithCancel(ctx)"
    if (($evaluator.Split(@($contextLine), [StringSplitOptions]::None).Length - 1) -ne 1) {
        throw 'Pinned VHS context patch did not find exactly one recording-context line.'
    }
    $evaluator = $evaluator.Replace($contextLine, "`trenderCtx := ctx$lineEnding$contextLine")
    $renderCall = 'v.Render(ctx)'
    if (($evaluator.Split(@($renderCall), [StringSplitOptions]::None).Length - 1) -ne 1) {
        throw 'Pinned VHS context patch did not find exactly one deferred render call.'
    }
    $evaluator = $evaluator.Replace($renderCall, 'v.Render(renderCtx)')
    [System.IO.File]::WriteAllText($evaluatorPath, $evaluator, [System.Text.UTF8Encoding]::new($false))

    $goVersionOutput = (& go version | Select-Object -First 1)
    if ($goVersionOutput -notmatch 'go version go([0-9]+\.[0-9]+\.[0-9]+)') {
        throw "Could not parse Go toolchain version: $goVersionOutput"
    }
    $toolchainVersion = [version]$matches[1]
    if ($toolchainVersion -lt [version]'1.26.7') {
        throw 'Building pinned VHS v0.12.0 requires Go 1.26.7 or later.'
    }
    $env:GOTOOLCHAIN = 'go' + $matches[1]
    Push-Location $vhsSource
    try {
        & go build -o $renderer .
        if ($LASTEXITCODE -ne 0) { throw 'Could not build the patched VHS renderer.' }
        $rendererBuilt = $true
    }
    finally {
        Pop-Location
    }

    $dbUser = [uri]::EscapeDataString($values['POSTGRES_USER'])
    $dbPassword = [uri]::EscapeDataString($values['POSTGRES_PASSWORD'])
    $dbName = [uri]::EscapeDataString($values['POSTGRES_DB'])
    $databaseUrl = 'postgresql://{0}:{1}@postgres:5432/{2}?sslmode=disable' -f $dbUser, $dbPassword, $dbName
    $runtimeLines = @(
        "DATABASE_URL=$databaseUrl",
        'VHS_API_URL=http://runtime-a:8080',
        # The renderer joins the Compose network; use Kafka's internal listener,
        # not the host-mapped EXTERNAL port.
        'KAFKA_BROKERS=kafka:19092'
    )
    [System.IO.File]::WriteAllLines($rendererEnv, $runtimeLines, [System.Text.UTF8Encoding]::new($false))

    $mount = 'type=bind,source={0},target=/vhs' -f $repoRoot
    $dockerArgs = @('run', '--rm', '--network', $sharedNetwork, '--mount', $mount,
        '--env-file', $rendererEnv, '--entrypoint', '/bin/bash', $image,
        '-lc', 'chmod +x /vhs/.scratch/dur045/local-demo-linux /vhs/.scratch/dur045/vhs-linux && /vhs/.scratch/dur045/vhs-linux demos/recruiter-demo.tape')
    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) { throw 'VHS failed to render demos/recruiter-demo.gif.' }

    $gif = Join-Path $repoRoot 'demos/recruiter-demo.gif'
    if (-not (Test-Path -LiteralPath $gif) -or (Get-Item -LiteralPath $gif).Length -eq 0) {
        throw 'VHS returned success but did not create demos/recruiter-demo.gif.'
    }
    $probeArgs = @(
        'run', '--rm', '--mount', $mount,
        '--entrypoint', 'ffprobe', $image,
        '-v', 'error', '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1', '/vhs/demos/recruiter-demo.gif'
    )
    $probeOutput = @(& docker @probeArgs 2>&1)
    $probeStatus = $LASTEXITCODE
    if ($probeStatus -ne 0) {
        throw "Could not verify the generated GIF duration (ffprobe exit $probeStatus): $($probeOutput -join ' ')"
    }
    $durationText = [string]($probeOutput | Select-Object -First 1)
    $duration = [double]::Parse($durationText, [Globalization.CultureInfo]::InvariantCulture)
    if ($duration -ge 120) { throw "Demo duration ${duration}s exceeds the two-minute limit." }
    Write-Host ("Rendered demos/recruiter-demo.gif from the checked-in tape ({0:N1}s)." -f $duration)
}
finally {
    $env:GOOS = $previousGoos
    $env:GOARCH = $previousGoarch
    $env:CGO_ENABLED = $previousCGO
    $env:GOCACHE = $previousGoCache
    $env:GOTOOLCHAIN = $previousGoToolchain
    if (Test-Path -LiteralPath $rendererEnv) { Remove-Item -LiteralPath $rendererEnv -Force }
    if ($helperBuilt -and (Test-Path -LiteralPath $helper)) { Remove-Item -LiteralPath $helper -Force }
    if ($rendererBuilt -and (Test-Path -LiteralPath $renderer)) { Remove-Item -LiteralPath $renderer -Force }
    if (Test-Path -LiteralPath $vhsSource) { Remove-Item -LiteralPath $vhsSource -Recurse -Force }
    if ($locationPushed) { Pop-Location }
}
