# DUR-049 AWS topology

This directory is the bounded Terraform topology for the private multi-host
recovery campaign. It creates exactly two application hosts and one dependency
host in a dedicated VPC. PostgreSQL and Kafka are reachable from the application
security group only; operator API access is optional and restricted by
`admin_cidrs`. SSM is enabled so the fault harness can observe and control the
original host process without replacing it.

The topology is intentionally not a production deployment. It is a reproducible
experiment with a single dependency host, one Kafka broker, bounded `t3.micro`
instances, encrypted gp3 roots, and an explicit expiration tag. It cannot
establish database-host durability, Kafka HA, or multi-region availability.

## Preflight

Use the non-root portfolio identity and an immutable source ref:

```powershell
$env:AWS_PROFILE = "portfolio-dev"
aws sts get-caller-identity
terraform -chdir=deploy/aws fmt -check -recursive
terraform -chdir=deploy/aws init -backend=false
terraform -chdir=deploy/aws validate
```

Resolve and record a concrete Ubuntu AMI and two valid availability zones for
the selected region, then provide the variables without committing them.
`postgres_password` is sensitive and is
accepted only as an alphanumeric value so generated connection URLs cannot be
ambiguous.

```powershell
$env:TF_VAR_ami_id = "ami-..."
$env:TF_VAR_repo_ref = "<reviewed-commit>"
$env:TF_VAR_expires_at = "2026-10-05T00:00:00Z"
$env:TF_VAR_postgres_password = "<ephemeral-alphanumeric-secret>"
$env:TF_VAR_admin_cidrs = '["203.0.113.10/32"]'
terraform -chdir=deploy/aws plan -out=dur049.tfplan
```

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
