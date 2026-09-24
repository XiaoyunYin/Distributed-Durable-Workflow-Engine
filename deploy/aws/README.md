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

The three burstable instances use `cpu_credits = "standard"` so campaign
timings are not silently changed by unlimited-credit billing. The current
account must allow EC2 quota `L-1216C47A` at or above 6 vCPUs; a 1-vCPU limit
cannot launch even one `t3.micro`.

Before applying, save the plan output, resource IDs, pinned AMI, repository
commit, image digests, expected duration and cost estimate in the local ignored
campaign directory. Stop if projected spend reaches $160 or the remaining D022
cap cannot be determined. The absolute cap is $200; destroy active resources
within 24 hours of each run and remove retained volumes/snapshots/logs within
seven days.

## Apply and teardown

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

Create two active workflow fixtures whose activity is observed `CLAIMED`, one
for each arm, then run the SSM-backed harness from the repository root:

```powershell
pwsh -File scripts/dur043-multihost.ps1 -Scenario all `
  -NetworkWorkflowId <claimed-network-fixture> `
  -HostWorkflowId <claimed-host-stop-fixture> `
  -LockWorkflowId <live-holder-contention-fixture> `
  -LockProgressWorkflowId <other-partition-progress-fixture> `
  -OwnerLockWorkflowId <owner-lock-isolation-fixture>
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
