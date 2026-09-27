# DUR-049 AWS topology

This directory is the bounded Terraform topology for the private multi-host
recovery campaign. It creates exactly two application hosts and one dependency
host in a dedicated VPC. PostgreSQL and Kafka are reachable from the application
security group only; operator API access is optional and restricted by
`admin_cidrs`. SSM is enabled so the fault harness can observe and control the
original host process without replacing it.

The topology is intentionally not a production deployment. It is a reproducible
experiment with a single dependency host, one Kafka broker, two bounded
`t3.micro` application instances and a bounded dependency instance whose type
is recorded per run, encrypted gp3 roots, and an explicit expiration tag. It cannot
establish database-host durability, Kafka HA, or multi-region availability.

## Preflight

Every DUR-050 PowerShell operator tool requires PowerShell 7 Core. Use the
absolute path to the verified `pwsh.exe` with `-NoProfile -File`; do not run
these scripts under Windows PowerShell 5.1. For example, set the path for the
operator host and invoke tools as follows:

```powershell
$pwshExe = 'C:\Program Files\PowerShell\7\pwsh.exe' # substitute the verified absolute path if installed elsewhere
if (-not [IO.Path]::IsPathRooted($pwshExe)) { throw 'pwsh.exe path must be absolute.' }
& $pwshExe -NoProfile -File .\scripts\dur050-ledger-record.ps1 -Backfill -CycleID <cycle-id> -TerraformOutputsPath <outputs.json> -ApplyRecordPath <apply-record.json> -DestroyRecordPath <destroy-record.json>
```

For the DUR-050 third provisioning cycle, the order is:
**live saved plan → account-only pre-apply gate → Claude go/no-go → apply →
ledger-record -Open → post-apply cycle preflight → preparation → destroy →
ledger-record -Close → post-destroy inventory**. Do not apply based only on a
successful pre-apply gate; its record explicitly grants no apply authority.
The teardown word in that summary means the committed
`dur050-destroy.ps1` wrapper, followed by ledger close and inventory—not a
hand-written `terraform destroy` command.

Generate the live read-only plan from this checkout's `deploy/aws` directory
against its local `terraform.tfstate`. Use the same checksum-verified Terraform
1.16.4 executable to create, inspect, gate, apply, list state, and destroy the
plan. The known Windows executable SHA-256 is
`D1F5754B41B44C7E4CE7283780D2CCB9492F2C32C9629555686FC29FA8067349`; if it is
re-downloaded, verify it against HashiCorp's checksum file first. Do not create
the plan in a container or copied directory.

```powershell
$terraformExe = 'C:\path\to\verified\terraform.exe'
if ((Get-FileHash $terraformExe -Algorithm SHA256).Hash -ne 'D1F5754B41B44C7E4CE7283780D2CCB9492F2C32C9629555686FC29FA8067349') { throw 'Unexpected Terraform binary.' }
if ((& $terraformExe version -json | ConvertFrom-Json).terraform_version -ne '1.16.4') { throw 'Terraform 1.16.4 is required.' }
Push-Location deploy/aws
try {
    & $terraformExe plan -input=false -out='.terraform/dur050-candidate.tfplan'
    if ($LASTEXITCODE -ne 0) { throw 'Terraform plan failed.' }
} finally { Pop-Location }
```

Put the candidate's full source commit, saved-plan SHA-256, inspection path,
baseline inspection path, and expected prior-state lineage/serial in the
manifest's single `approved_saved_plan` entry. Inspect the exact candidate
with `& $pwshExe -NoProfile -File .\scripts\dur050-inspect-saved-plan.ps1` using that same executable and
state file; it reads the ZIP's `tfstate` entry and fails if lineage/serial
differs from the local state. Then record the produced inspection SHA-256 in
the same manifest entry. Run `& $pwshExe -NoProfile -File
.\scripts\dur050-preapply-gate.ps1` with the saved
plan, inspection, manifest, unique ledger-check/output paths, and full-cycle
reserve; the gate takes expected hashes and paths only from
`approved_saved_plan` and repeats the state comparison at gate time. The
account-only gate checks the empty local Terraform state, AWS
identity, regional inventory/quota, D022 budget/notifications, cost-allocation
tags, and ledger; it does not require Terraform outputs and does not call SSM.
Its record also captures the absolute PowerShell executable path, version, and
edition, and the gate fails unless the edition is Core.
Wait for Claude's explicit go/no-go before applying the exact reviewed plan.

Immediately before invoking apply, capture the apply-start UTC timestamp and
set the manifest's top-level `approved_saved_plan.review_state` to
`APPLIED_<apply-start-utc>`; this records an attempted apply so teardown remains
available after a partial failure. Apply the exact reviewed plan. After apply,
use the current parsed outputs and that timestamp with
`& $pwshExe -NoProfile -File .\scripts\dur050-ledger-record.ps1 -Open ...`. The helper uses read-only
DescribeInstances data to check all four IDs/types and that the start is no
later than each EC2 LaunchTime. It appends intervals to the task-wide ledger
and refuses duplicate/open cycles. Then invoke
`& $pwshExe -NoProfile -File .\scripts\dur050-cycle-preflight.ps1 ...`.
It requires the four open ledger intervals to match the parsed Terraform
outputs and cycle ID before checking cost; it also records whether each
attached root volume has Task=DUR-050. Run preparation only after that cycle
preflight passes. Teardown order is `dur050-destroy.ps1` →
`dur050-ledger-record.ps1 -Close` → `dur050-post-destroy-inventory.ps1`. Invoke
the destroy wrapper as:

```powershell
& $pwshExe -NoProfile -File .\scripts\dur050-destroy.ps1 -CycleID <cycle-id> -CampaignManifestPath <manifest.json> -OutputPath <destroy-record.json>
```

It builds a typed temporary `.tfvars.json` from the manifest profile outside
the repository, uses env-provided or valid throwaway secret values, and
removes the temporary file in `finally`. Then invoke
`& $pwshExe -NoProfile -File .\scripts\dur050-ledger-record.ps1 -Close ...`
with the cycle ID and observed destroy-completion UTC timestamp. Finally run
the read-only post-destroy inventory with `& $pwshExe -NoProfile -File
.\scripts\dur050-post-destroy-inventory.ps1` and that cycle ID, the campaign
manifest, a unique output path, and the same pinned Terraform executable. It
requires an empty Terraform state, no regional instances or volumes, no
Task-tagged campaign network/storage/load-balancer resources, no prefixed
campaign IAM roles or profiles, no campaign SSM parameters, and no open ledger
intervals for the cycle. It preserves the raw AWS responses in a non-overwrite
PASS/FAIL record; retain that record with cycle closeout evidence.

Use the non-root portfolio identity and a full 40-character immutable source
SHA. A short SHA, tag, or branch cannot be fetched as the pinned campaign
input and is rejected by Terraform validation:

```powershell
$env:AWS_PROFILE = "admin-learning"
aws sts get-caller-identity
terraform -chdir=deploy/aws fmt -check -recursive
terraform -chdir=deploy/aws init -backend=false
terraform -chdir=deploy/aws validate
```

Resolve and record a concrete Ubuntu AMI and two valid availability zones for
the selected region, then provide the variables without committing them.
`postgres_password` is sensitive and is stored in an encrypted SSM parameter;
cloud-init retrieves it with the instance role instead of embedding it in
EC2 user-data. The value is accepted only as an alphanumeric secret so the
generated connection URLs cannot be ambiguous. Terraform state and local
plans remain sensitive and must stay ignored.

```powershell
$env:TF_VAR_ami_id = "ami-..."
$env:TF_VAR_instance_type = "t3.micro"
$env:TF_VAR_dependency_instance_type = "t3.small"
$env:TF_VAR_repo_ref = "<full-40-character-reviewed-commit-sha>"
$env:TF_VAR_expires_at = "2026-10-05T00:00:00Z"
$env:TF_VAR_postgres_password = "<ephemeral-alphanumeric-secret>"
$env:TF_VAR_admin_cidrs = '["203.0.113.10/32"]'
terraform -chdir=deploy/aws plan -out=dur049.tfplan
```

For DUR-050, the separate load-generator security group is the only additional
source allowed to reach PostgreSQL on 5432 and the private runtime API on 8080.
The database bootstrap creates `dur050_observer` with a distinct SCRAM
credential, read-only transactions, a two-connection limit, and column-level
SELECT grants only on workflow status and terminal-history fields. App hosts
cannot read that credential; the separately provisioned generator profile can
read only its SSM parameter. No observer grant includes write, sequence, or
effect-service access.

The DUR-050 host is opt-in: set `enable_dur050_load_generator = true` (or
`TF_VAR_enable_dur050_load_generator=true`) before planning its four-host
topology. It creates one `c7i.large` generator/observer host in the first
campaign AZ, builds the observer from the exact full-SHA repository checkout,
and exposes its instance ID as `load_generator_instance_ids`. The normal
DUR-049 recovery topology leaves this host disabled. For DUR-050, also set the
two app hosts to `c7i.large` and the dependency host to `m7i.large` as required
by the reviewed protocol; the complete four-host plan is still subject to the
D022 quota and cost preflight.

For a DUR-050 plan, also set `TF_VAR_campaign_slug=dur050`,
`TF_VAR_task_id=DUR-050`, and `TF_VAR_environment_name=portfolio-capacity`.
These values keep names, resource tags, and bootstrap logs attributed to the
capacity campaign; the defaults preserve the original DUR-049 topology.

Only when enabling this host, provide a separate ephemeral observer password
before planning; the observer SSM parameter, load-generator SG rules, IAM
profile, and host are all conditional on `enable_dur050_load_generator=true`.
The default DUR-049 plan does not create observer credentials or open the
generator-only network paths. The DUR-050 app overlay enables the fixed
`dur050-*` admission caps and sets `DUR049_RECORD_LEASE_ACQUISITIONS=0`; the
base app compose retains the DUR-049 default. PostgreSQL preloads
`pg_stat_statements` via `shared_preload_libraries`, then migration 000018
installs the extension. After a reviewed apply, run
`dur050-prepare-pilot.ps1` with `& $pwshExe -NoProfile -File
.\scripts\dur050-prepare-pilot.ps1` and that cycle's parsed Terraform outputs
and D022 preflight. It installs the two definitions, stages the API config from
the current app-host address, verifies the clean baseline, captures and hashes
the stopped dependency volumes, then restarts and health-checks PostgreSQL and
Kafka. Preserve its per-stage JSON plus `baseline-manifest.json`; do not hand-
type SSM setup commands. After each volume restore, run the reviewed
`& $pwshExe -NoProfile -File .\scripts\dur050-reset-block.ps1` procedure with the same `-CycleID` and
`-BaselineManifestPath` to verify both archive hashes before restore and restart
the application hosts,
verify the Kafka worker group has active consumer assignment, submit and drain
warmups, and retain the per-block reset and database snapshot artifacts before
measured work begins. The protocol-matched warmup submitter and its eight fresh
workflow IDs are supplied for each block; the reset script fails if the IDs
are missing, duplicated, or do not drain to terminal state. Pass the planned
maximum block duration as `-BlockDurationMinutes` (reset start through end of drain); reset rejects a preflight
whose age plus that duration exceeds its recorded positive ledger reserve (and
still enforces the 240-minute outer freshness cap).

Before preparation and again before every paid block, generate the current
cycle's read-only gate record with `& $pwshExe -NoProfile -File
.\scripts\dur050-cycle-preflight.ps1`. Supply
the parsed Terraform outputs, recorded plan inspection, task-wide cost-ledger
manifest, a unique output path, and explicit reserve minutes. The tool rechecks
account/quota/budget/tags, ledger reserve, and all four hosts' cloud-init and
bootstrap artifacts; it writes PASS only if every check succeeds. Preparation
and reset share the same schema, cycle-ID, vCPU, and freshness validator. Its
`-DryRun` mode prints planned checks and wrapped bootstrap commands only; it is
not a passing D022 preflight and makes no AWS or SSM calls.

```powershell
$env:TF_VAR_campaign_slug = "dur050"
$env:TF_VAR_task_id = "DUR-050"
$env:TF_VAR_environment_name = "portfolio-capacity"
$env:TF_VAR_enable_dur050_load_generator = "true"
$env:TF_VAR_instance_type = "c7i.large"
$env:TF_VAR_dependency_instance_type = "m7i.large"
$env:TF_VAR_load_generator_instance_type = "c7i.large"
$env:TF_VAR_root_volume_size_gb = "40"
$env:TF_VAR_dur050_observer_password = "<different-24-to-64-character-secret>"
```

Every AWS-RunShellScript command uses the DUR-050 base64/temp-file Bash
wrapper (`scripts/dur050-ssm-wrapper.ps1`), because SSM otherwise starts
`/bin/sh`. The reset helper sends its remote stages through that wrapper.
Dispatch unloaded, load-window and sink-check runners with
`& $pwshExe -NoProfile -File .\scripts\dur050-run-generator-block.ps1`. Pass the current cycle's parsed
`terraform-outputs.json` and the `preparation-dry-run.json` emitted by
`dur050-prepare-pilot.ps1 -DryRun`, plus the matching `-CycleID`; the dispatcher
rejects a cycle mismatch and requires the recorded generator config path
`/var/tmp/dur050-<CycleID>/frozen-config.json`. Do not hand-write either input.
Its `-StagePlanOnly` mode validates the prepared config and prints the exact
command without AWS access. Retrieve
the resulting block directory with
`& $pwshExe -NoProfile -File .\scripts\dur050-retrieve-generator-block.ps1`, which transfers bounded SSM
chunks and verifies size and SHA-256 before extraction. Both tools record SSM
command IDs. Do not hand-write runner `-RemoteLines` or rely on SSM's capped
`StandardOutputContent` for block artifacts. The retrieval record names the
remote archive under `/var/tmp`; it remains available for retry until host
teardown.

Terraform rejects a partially configured DUR-050 profile: when the generator
is enabled, all three campaign attribution values and the reviewed instance
sizes/root-volume size are required. The matching values are also asserted by
`terraform test` in `dur050.tftest.hcl`.

The three default DUR-049 recovery instances are burstable T3 hosts and use
`cpu_credits = "standard"`, so timings are not silently changed by unlimited-
credit billing. DUR-050 uses three `c7i.large` hosts and one `m7i.large`
dependency host, for 8 vCPUs. Its preflight must show that the account's
concurrent use plus these 8 vCPUs stays within EC2 quota `L-1216C47A` (32 in
the reviewed account). A 1-vCPU quota cannot launch even one `t3.micro`.

Before applying, save the plan output, resource IDs, pinned AMI, repository
commit, image digests, expected duration and cost estimate in the campaign
directory. Do not apply until the DUR-050 ACTUAL budget notification is set to
the apply-time aggregate actual spend plus $75, `Task` and `Environment`
cost-allocation tags are active, and the no-apply plan has been freshly
reviewed. The pre-apply gate projects the closed task-wide ledger plus the four
planned host types for the full cycle reserve; it requires zero open intervals
and a projection strictly below the $75 task cap. After apply, ledger-record
opens the four instance intervals; after destroy it closes them. Before each
paid block, run the cost-ledger CLI against the task-wide ledger:

```sh
python scripts/dur050-cost-ledger.py <campaign>/cost-manifest.json \
  --reserve-minutes <planned-maximum-block-minutes> \
  --output <campaign>/ledger-check-<block-id>.json
```

Proceed only when the command exits 0 and records `PASS`; the projected EC2
instance-hours including the block reserve must remain strictly below $75.
This ledger is paired with the aggregate ACTUAL alert because it does not
measure storage, IP addresses, transfer, tax, or other billable services. The
aggregate D022 maximum is $200, with the existing $160 stop alert as a
backstop. Destroy active resources within 24 hours of each run and remove
retained volumes/snapshots/logs within seven days.

## Apply and teardown

The bare commands in this subsection are **DUR-049 only**. For DUR-050, use
`scripts/dur050-destroy.ps1`; it reads the applied Terraform executable pin
from the top-level `approved_saved_plan` and writes a cycle-specific record.

```powershell
terraform -chdir=deploy/aws apply dur049.tfplan
terraform -chdir=deploy/aws output -json
terraform -chdir=deploy/aws destroy
```

Do not run `apply` with the root AWS identity. After `destroy`, verify the
instances, volumes, security groups, VPC, IAM profile, and any retained logs are
gone. The recovery harness must separately record fault-command time, takeover
time, and first useful progress, and must confirm network/process/backend state
rather than treating a requested command as proof that a fault occurred.

After recording the Terraform state list and AWS absence checks, remove local
state, backups, and plan files before publishing the closeout. The final local
artifact check must recursively find no `*.tfstate*` or `*.tfplan` files,
including under Terraform's ignored `.terraform/` directory:

```powershell
$terraformArtifacts = @(Get-ChildItem -LiteralPath deploy/aws -File -Force -Recurse |
  Where-Object { $_.Name -like "*.tfstate*" -or $_.Extension -eq ".tfplan" })
if ($terraformArtifacts.Count -ne 0) { throw "Sensitive Terraform artifacts remain." }
```

## Multi-host fault harness

Before using AWS, run the local PowerShell 5.1-compatible partition self-test;
it makes the campaign's independent partition mapping executable without any
AWS calls:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dur043-multihost.ps1 -SelfTest
```

Pass five fresh, unused workflow IDs; the harness submits each fixture and
waits until each activity fixture is observed `CLAIMED` before its fault. The
other-partition progress arm uses its own separate fixture. Then run the
SSM-backed harness from the repository root:

```powershell
& $pwshExe -NoProfile -File .\scripts\dur043-multihost.ps1 -Scenario all `
  -NetworkWorkflowId <fresh-network-id> `
  -HostWorkflowId <fresh-host-stop-id> `
  -LockWorkflowId <fresh-live-holder-id> `
  -LockProgressWorkflowId <fresh-progress-id> `
  -OwnerLockWorkflowId <fresh-owner-lock-id>
```

The harness obtains Terraform outputs, requires all three instances to be
`running` and SSM `Online`, preserves the original app process during the
network arm, drops only its PostgreSQL/Kafka path with host firewall rules,
and uses a forced EC2 stop for the host arm. It writes `PASS` only after SSM,
EC2, Docker, PostgreSQL lease/epoch rows, workflow progress, and superseded
epoch history agree. A requested fault without observed state is a failure.
The `all` run includes the R131 owner-lock isolation scenario: a campaign-only
tagged runtime pauses inside `ConsumeResult` after it holds the lease-row lock,
then the app host is isolated without terminating sessions. In the recorded
campaign, TCP keepalive failure reaped the backend about 62 seconds after its
last activity; the peer consumed the already-recorded result, and the same
original runtime process reconnected and attempted a stale `ReleaseLease`.
That write received `ErrLeaseNotOwned` without changing the peer lease or
completed workflow. The campaign set a transaction-local two-minute
`idle_in_transaction_session_timeout`, overriding the deployment's 10-second
setting; therefore the production 10-second idle-timeout reaping path was not
measured. The normal `/runtime` target excludes the fault hook. The AWS-only
`dur049-campaign` image target additionally includes `/runtime-dur049-owner-lock`,
the `dur034_ablation`-tagged `/dur043-stale-probe`, and campaign checker and
diagnostic binaries. Local/default images contain only `/runtime`.
