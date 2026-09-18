[CmdletBinding()]
param(
    [switch]$StartServices,
    [string]$OutputPath = "experiments/m7/dur036-readiness.json"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot

$composeArgs = @("compose", "--env-file", ".env", "-f", "deploy/local/compose.yaml")
$expectedServices = @("postgres", "kafka", "runtime-a", "runtime-b", "worker-a", "worker-b", "otel-collector", "prometheus")
$startedAt = (Get-Date).ToUniversalTime()

function Invoke-Captured([string]$FilePath, [string[]]$Arguments) {
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $FilePath @Arguments 2>&1 | Out-String
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
    [pscustomobject]@{
        command = (($FilePath + " " + ($Arguments -join " ")).Trim())
        exit_code = $LASTEXITCODE
        output = $output.Trim()
    }
}

function Invoke-Required([string]$FilePath, [string[]]$Arguments) {
    $result = Invoke-Captured $FilePath $Arguments
    if ($result.exit_code -ne 0) {
        throw "Command failed ($($result.exit_code)): $($result.command)`n$($result.output)"
    }
    return $result
}

function EnvValue([string]$Name, [string]$Fallback) {
    $line = Get-Content -LiteralPath ".env" | Where-Object {
        $_ -match ("^" + [regex]::Escape($Name) + "=")
    } | Select-Object -First 1
    if ($line) { return ($line -replace ("^" + [regex]::Escape($Name) + "="), "") }
    return $Fallback
}

function Parse-JsonLines([string]$Text) {
    $trimmed = $Text.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed)) { return @() }
    try {
        $single = $trimmed | ConvertFrom-Json
        if ($single -is [array]) { return @($single) }
        return @($single)
    } catch {
        return @($trimmed -split "`r?`n" | Where-Object { $_.Trim() } | ForEach-Object {
            $_ | ConvertFrom-Json
        })
    }
}

function Get-Metrics([string]$Port) {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri ("http://127.0.0.1:{0}/metrics" -f $Port) -TimeoutSec 5
        return [pscustomobject]@{ status = [int]$response.StatusCode; body = $response.Content }
    } catch {
        return [pscustomobject]@{ status = 0; body = $_.Exception.Message }
    }
}

try {
    if (-not (Test-Path -LiteralPath ".env")) {
        throw ".env is missing; run scripts/bootstrap.ps1 before DUR-036 readiness."
    }

    $status = & git status --porcelain --untracked-files=all
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect Git status." }
    if (-not [string]::IsNullOrWhiteSpace(($status | Out-String))) {
        throw "DUR-036 readiness requires a clean checkout before deployment."
    }
    $commit = (Invoke-Required "git" @("rev-parse", "HEAD")).output.Trim()

    if ($StartServices) {
        Invoke-Required "docker" ($composeArgs + @("up", "-d", "--build", "--wait")) | Out-Null
        Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "migrate.ps1")) | Out-Null
    }

    $versionProbe = Invoke-Required "docker" @("version", "--format", "{{json .}}")
    $version = $versionProbe.output | ConvertFrom-Json
    $infoProbe = Invoke-Required "docker" @("info", "--format", "{{json .}}")
    $info = $infoProbe.output | ConvertFrom-Json
    if ($version.Server.Os -ne "linux" -or $info.OSType -ne "linux") {
        throw "DUR-036 requires a Linux measurement host; Docker reported server OS '$($version.Server.Os)' / '$($info.OSType)'."
    }

    $composeProbe = Invoke-Required "docker" ($composeArgs + @("config", "--format", "json"))
    $compose = $composeProbe.output | ConvertFrom-Json
    $psProbe = Invoke-Required "docker" ($composeArgs + @("ps", "--all", "--format", "json"))
    $containers = Parse-JsonLines $psProbe.output
    $serviceSummary = @()
    foreach ($serviceName in $expectedServices) {
        $service = @($containers | Where-Object { $_.Service -eq $serviceName }) | Select-Object -First 1
        if ($null -eq $service) { throw "Compose service '$serviceName' is missing from the readiness host." }
        $health = if ($service.Health) { $service.Health } else { "not-declared" }
        if ($service.State -ne "running") { throw "Compose service '$serviceName' is not running: $($service.Status)" }
        if ($health -ne "not-declared" -and $health -ne "healthy") { throw "Compose service '$serviceName' is not healthy: $health" }
        $serviceSummary += [ordered]@{
            service = $serviceName
            container_id = $service.ID
            image = $service.Image
            state = $service.State
            health = $health
            status = $service.Status
        }
    }

    $postgres = @($containers | Where-Object { $_.Service -eq "postgres" }) | Select-Object -First 1
    $postgresInspect = (Invoke-Required "docker" @("inspect", $postgres.ID)).output | ConvertFrom-Json | Select-Object -First 1
    $volumeProbe = Invoke-Required "docker" @("volume", "ls", "--format", "{{json .}}")
    $volumes = Parse-JsonLines $volumeProbe.output | Where-Object { $_.Name -match "durable-agent-engine" } | ForEach-Object {
        [ordered]@{ name = $_.Name; driver = $_.Driver; labels = $_.Labels }
    }

    $databaseUser = EnvValue "POSTGRES_USER" "postgres"
    $databaseName = EnvValue "POSTGRES_DB" "durable_agent"
    $databaseSQL = "SELECT json_build_object('server_version', version(), 'data_checksums', current_setting('data_checksums'), 'fsync', current_setting('fsync'), 'synchronous_commit', current_setting('synchronous_commit'), 'full_page_writes', current_setting('full_page_writes'), 'timezone', current_setting('TimeZone'), 'now_utc', (now() AT TIME ZONE 'UTC'))::text;"
    $databaseProbe = Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", $databaseUser, "-d", $databaseName, "-c", $databaseSQL))
    $databaseSnapshot = ($databaseProbe.output -split "`r?`n" | Where-Object { $_.Trim() } | Select-Object -Last 1) | ConvertFrom-Json
    $filesystemProbe = Invoke-Required "docker" ($composeArgs + @("exec", "-T", "postgres", "sh", "-c", "df -T /var/lib/postgresql; stat -f -c '%T' /var/lib/postgresql"))

    $runtimePort = EnvValue "RUNTIME_A_PORT" "8080"
    $prometheusPort = EnvValue "PROMETHEUS_PORT" "9090"
    $metricsBefore = Get-Metrics $runtimePort
    $smoke = Invoke-Required "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "smoke.ps1"))

    $oldRequireDatabase = $env:DURABLE_REQUIRE_DATABASE
    try {
        $env:DURABLE_REQUIRE_DATABASE = "1"
        $telemetryReadiness = Invoke-Required "go" @("test", "./internal/engine", "-run", "^TestDUR036TelemetryReconstruction$", "-count=1", "-v")
    } finally {
        if ($null -eq $oldRequireDatabase) { Remove-Item Env:DURABLE_REQUIRE_DATABASE -ErrorAction SilentlyContinue }
        else { $env:DURABLE_REQUIRE_DATABASE = $oldRequireDatabase }
    }
    $metricsAfter = Get-Metrics $runtimePort

    $faultTrace = "experiments/m5/traces/F07-crash-resume-seed11.jsonl"
    $faultSnapshot = "experiments/m5/durable/F07-crash-resume-seed11.json"
    if (-not (Test-Path -LiteralPath $faultTrace) -or -not (Test-Path -LiteralPath $faultSnapshot)) {
        throw "Committed F07 durable fault evidence is missing."
    }
    $faultChecker = Invoke-Required "go" @("run", "./cmd/fault-checker", "-offline", "-trace", $faultTrace, "-durable-trace", $faultSnapshot)

    $volumeDrivers = @($volumes | ForEach-Object { $_.driver } | Where-Object { $_ } | Select-Object -Unique)
    $artifact = [ordered]@{
        schema_version = "dur036-readiness.v1"
        status = "PASS"
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        git = [ordered]@{ commit = $commit; clean_checkout_verified = $true }
        host = [ordered]@{
            platform = $version.Server.Platform.Name
            docker_server_version = $version.Server.Version
            os = $info.OperatingSystem
            os_type = $info.OSType
            architecture = $info.Architecture
            kernel = $info.KernelVersion
            storage_driver = $info.Driver
            docker_root = $info.DockerRootDir
            cpus = $info.NCPU
            memory_bytes = $info.MemTotal
            cgroup_version = $info.CgroupVersion
            server_time_utc = $info.SystemTime
            filesystem_probe = $filesystemProbe.output
            volume_driver = $volumeDrivers
        }
        runtime = [ordered]@{
            docker_client = $version.Client.Version
            compose_file = "deploy/local/compose.yaml"
            services = $serviceSummary
            compose_services = @($compose.services.PSObject.Properties.Name)
            compose_volumes = @($compose.volumes.PSObject.Properties.Name)
            postgres = $postgresInspect.Config.Image
            resource_limits = [ordered]@{
                postgres_memory_bytes = $postgresInspect.HostConfig.Memory
                postgres_nano_cpus = $postgresInspect.HostConfig.NanoCpus
                postgres_pids_limit = $postgresInspect.HostConfig.PidsLimit
            }
        }
        database = $databaseSnapshot
        validation = [ordered]@{
            dependency_smoke = [ordered]@{ command = $smoke.command; status = "PASS"; output_tail = (($smoke.output -split "`r?`n" | Select-Object -Last 5) -join "`n") }
            normal_execution = [ordered]@{ command = $telemetryReadiness.command; status = "PASS"; output = $telemetryReadiness.output; telemetry_endpoint_before_status = $metricsBefore.status; telemetry_endpoint_after_status = $metricsAfter.status; telemetry_before = $metricsBefore.body; telemetry_after = $metricsAfter.body }
            fault_episode = [ordered]@{ command = $telemetryReadiness.command; status = "PASS"; output = $telemetryReadiness.output; checker_command = $faultChecker.command; checker_output = $faultChecker.output; trace = $faultTrace; durable_snapshot = $faultSnapshot }
        }
        assumptions = @(
            "All host and container timestamps are recorded as UTC where available; PostgreSQL reports its configured TimeZone separately.",
            "Docker Desktop's Linux WSL2 VM and its Docker-managed local volumes are the declared DUR-036 measurement host and storage boundary.",
            "No paid resources or live model providers are used by readiness; cost control is not applicable.",
            "The readiness smoke is development evidence on Docker Desktop/WSL2; final I/O-sensitive claims remain subject to the host and protocol limits in PLAN.md."
        )
    }
    $parent = Split-Path -Parent $OutputPath
    if ($parent) { New-Item -ItemType Directory -Force -Path $parent | Out-Null }
    $artifact | ConvertTo-Json -Depth 16 | Set-Content -LiteralPath $OutputPath -Encoding utf8
    Write-Host "DUR-036 readiness passed; evidence written to $OutputPath"
} finally {
    Pop-Location
}
