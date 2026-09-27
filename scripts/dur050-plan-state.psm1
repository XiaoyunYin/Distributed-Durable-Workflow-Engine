Set-StrictMode -Version Latest

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

function Resolve-Dur050TerraformExecutable {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$TerraformExe)

    if (Test-Path -LiteralPath $TerraformExe -PathType Leaf) {
        $path = (Resolve-Path -LiteralPath $TerraformExe).Path
    } else {
        $command = Get-Command -Name $TerraformExe -CommandType Application -ErrorAction Stop | Select-Object -First 1
        $path = [System.IO.Path]::GetFullPath($command.Source)
    }
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Terraform executable does not exist: $path" }

    $versionRaw = & $path version -json 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "Terraform version query failed for '$path': $versionRaw" }
    try { $version = $versionRaw | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "Terraform version query returned invalid JSON for '$path'." }
    if ([string]$version.terraform_version -ne '1.16.4') {
        throw "Terraform executable '$path' reports version '$($version.terraform_version)'; expected 1.16.4."
    }
    return [ordered]@{
        path = $path
        sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToUpperInvariant()
        terraform_version = [string]$version.terraform_version
    }
}

function Read-Dur050LocalTerraformState {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$TerraformStatePath)

    $fullPath = [System.IO.Path]::GetFullPath($TerraformStatePath)
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        return [ordered]@{
            path = $fullPath
            state_file_present = $false
            lineage = ''
            serial = [long]0
            terraform_version = $null
            resource_count = 0
        }
    }
    try { $state = Get-Content -LiteralPath $fullPath -Raw | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "Terraform local state is invalid JSON: $fullPath ($($_.Exception.Message))" }
    if ([int]$state.version -ne 4 -or $null -eq $state.resources -or
        $null -eq $state.PSObject.Properties['lineage'] -or $null -eq $state.PSObject.Properties['serial']) {
        throw "Terraform local state has an unsupported shape or lacks lineage/serial: $fullPath."
    }
    $resourceCount = 0
    if ($state.resources -is [array]) { $resourceCount = $state.resources.Length }
    elseif ($null -ne $state.resources) { $resourceCount = 1 }
    return [ordered]@{
        path = $fullPath
        state_file_present = $true
        lineage = [string]$state.lineage
        serial = [long]$state.serial
        terraform_version = [string]$state.terraform_version
        resource_count = $resourceCount
    }
}

function Read-Dur050PlanPriorState {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$SavedPlanPath)

    $fullPath = (Resolve-Path -LiteralPath $SavedPlanPath -ErrorAction Stop).Path
    $archive = $null
    $entryStream = $null
    $reader = $null
    try {
        $archive = [System.IO.Compression.ZipFile]::OpenRead($fullPath)
        $entry = $archive.GetEntry('tfstate')
        if ($null -eq $entry) { throw "Saved plan archive has no tfstate entry: $fullPath" }
        $entryStream = $entry.Open()
        $reader = [System.IO.StreamReader]::new($entryStream, [System.Text.Encoding]::UTF8, $true)
        $json = $reader.ReadToEnd()
        try { $state = $json | ConvertFrom-Json -ErrorAction Stop }
        catch { throw "Saved plan tfstate entry is invalid JSON: $($_.Exception.Message)" }
        if ($null -eq $state.PSObject.Properties['lineage'] -or $null -eq $state.PSObject.Properties['serial'] -or
            $null -eq $state.PSObject.Properties['terraform_version']) {
            throw 'Saved plan tfstate entry lacks lineage, serial, or terraform_version.'
        }
        return [ordered]@{
            path = $fullPath
            lineage = [string]$state.lineage
            serial = [long]$state.serial
            terraform_version = [string]$state.terraform_version
        }
    } catch {
        if ($_.Exception.Message -match 'Saved plan archive|Saved plan tfstate entry') { throw }
        throw "Could not read embedded tfstate from saved plan '$fullPath': $($_.Exception.Message)"
    } finally {
        if ($null -ne $reader) { $reader.Dispose() }
        elseif ($null -ne $entryStream) { $entryStream.Dispose() }
        if ($null -ne $archive) { $archive.Dispose() }
    }
}

function Assert-Dur050PlanStateMatchesLocal {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$PlanPriorState,
        [Parameter(Mandatory)]$LocalState
    )

    if ([string]$PlanPriorState.lineage -cne [string]$LocalState.lineage) {
        throw "Saved plan prior-state lineage '$($PlanPriorState.lineage)' does not match local-state lineage '$($LocalState.lineage)'."
    }
    if ([long]$PlanPriorState.serial -ne [long]$LocalState.serial) {
        throw "Saved plan prior-state serial $($PlanPriorState.serial) does not match local-state serial $($LocalState.serial)."
    }
    if (-not [bool]$LocalState.state_file_present -and
        ([string]$PlanPriorState.lineage -cne '' -or [long]$PlanPriorState.serial -ne 0)) {
        throw 'No local Terraform state exists, so the saved plan must have empty lineage and serial 0.'
    }
    return $true
}

function Get-Dur050PlanStateSnapshot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$SavedPlanPath,
        [Parameter(Mandatory)][string]$TerraformStatePath,
        [Parameter(Mandatory)][string]$TerraformExe,
        [switch]$AllowMismatch
    )

    $terraform = Resolve-Dur050TerraformExecutable -TerraformExe $TerraformExe
    $planPrior = Read-Dur050PlanPriorState -SavedPlanPath $SavedPlanPath
    $localState = Read-Dur050LocalTerraformState -TerraformStatePath $TerraformStatePath
    $stateMatches = $true
    $stateMismatch = $null
    try { [void](Assert-Dur050PlanStateMatchesLocal -PlanPriorState $planPrior -LocalState $localState) }
    catch {
        if (-not $AllowMismatch) { throw }
        $stateMatches = $false
        $stateMismatch = $_.Exception.Message
    }
    return [ordered]@{
        plan_prior_state_lineage = [string]$planPrior.lineage
        plan_prior_state_serial = [long]$planPrior.serial
        plan_prior_state_terraform_version = [string]$planPrior.terraform_version
        local_state_lineage = [string]$localState.lineage
        local_state_serial = [long]$localState.serial
        local_state_file_present = [bool]$localState.state_file_present
        local_state_resource_count = [int]$localState.resource_count
        terraform_state_path = [string]$localState.path
        terraform_executable_path = [string]$terraform.path
        terraform_executable_sha256 = [string]$terraform.sha256
        terraform_executable_version = [string]$terraform.terraform_version
        state_matches = $stateMatches
        state_mismatch = $stateMismatch
    }
}

Export-ModuleMember -Function Resolve-Dur050TerraformExecutable, Read-Dur050LocalTerraformState, Read-Dur050PlanPriorState, Assert-Dur050PlanStateMatchesLocal, Get-Dur050PlanStateSnapshot
