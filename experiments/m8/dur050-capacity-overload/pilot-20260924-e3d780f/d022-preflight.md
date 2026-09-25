# DUR-050 D022 preflight and first provisioning cycle

**Observed:** initial technical preflight 2026-09-24; R156 controls refreshed
2026-09-25, `admin-learning`, `us-west-1`
**Source commit:** `aa7de7e05a6dc534f9771063a2e20729f0f10520`
**Status:** D022 preflight and approved R156 account controls verified. The
reviewed first provisioning cycle was applied and then fully destroyed after a
pre-block bootstrap failure. No paid measurement block ran; this is operational
and cost-accounting evidence, not capacity or pilot measurement evidence.

## Authorization and budget

- Budget: `durable-engine-D022-aggregate-20260923`, custom period
  2026-09-23 00:00 UTC through 2026-10-07 00:00 UTC.
- Aggregate maximum: **$200**, unchanged. The original round-90 snapshot was
  actual **$1.476** and forecast **$5.257**; the existing $160 ACTUAL and
  FORECASTED alerts were `OK`.
- R156 was explicitly authorized and applied on 2026-09-25. Immediately before
  creating the new alert, aggregate actual spend was re-read as **$1.476**
  between `2026-09-25T07:07:38.3317029Z` and
  `2026-09-25T07:07:39.5617735Z`. The requested formula is
  `floor((actual spend + $75) * 100) / 100`; the resulting ACTUAL,
  `ABSOLUTE_VALUE`, `GREATER_THAN` threshold is **$76.47**. AWS read-back at
  `2026-09-25T07:07:44.0381321Z` showed its notification state `OK`, with the
  same single subscriber as both existing alerts. The address is omitted.
- `aws budgets describe-notifications-for-budget` read-back (all states `OK`):

  | Type | Threshold type | Threshold | State |
  |---|---|---:|---|
  | ACTUAL | ABSOLUTE_VALUE | $160.00 | OK |
  | ACTUAL | ABSOLUTE_VALUE | $76.47 | OK |
  | FORECASTED | ABSOLUTE_VALUE | $160.00 | OK |

- `aws ce update-cost-allocation-tags-status --region us-east-1` returned no
  errors. `aws ce list-cost-allocation-tags --tag-keys Task Environment`
  reported both user-defined tags `Active`, with `LastUpdatedDate` of
  `2026-09-25T07:06:39Z`; active status was observed at
  `2026-09-25T07:06:55.2956541Z`. If any campaign spend begins before tag
  activation takes effect in billing reports, billing backfill is needed. No
  campaign spend has started, so no backfill is currently indicated.
- The $76.47 alert is an aggregate-spend stop signal, not an instantaneous
  hard cap. The per-host instance-hour ledger remains the pre-block control;
  do not start any paid block unless accrued plus reserved EC2 cost is
  strictly below $75. The aggregate alert covers other AWS charges as a
  backstop. Destroy active resources within 24 hours and remove retained
  volumes/snapshots/logs within seven days. Finish within the authorized
  budget period.
- Budget cost types include tax, subscriptions, upfront, recurring, other
  subscription, and support; discounts, credits, and refunds are excluded.
- No free-tier or credit assumption is used. Before every paid block, run the
  campaign's instance-hour ledger with a reserve for that block; do not start
  unless accrued plus reserved EC2 instance cost is strictly below $75. The
  aggregate ACTUAL alert is the account-level backstop for other AWS charges,
  not a substitute for the ledger. Destroy active resources within 24 hours
  and remove retained volumes/snapshots/logs within seven days. Finish within
  the authorized budget period.

## Quota, account state, and topology

- EC2 `L-1216C47A` in `us-west-1`: **32 vCPU**. No pending or running EC2
  instances, EBS volumes, allocated addresses, snapshots, or resources tagged
  `Task=DUR-050` were found in the campaign region at preflight.
- `c7i.large` and `m7i.large` are offered in both selected zones, `us-west-1a`
  and `us-west-1c`.
- Current Ubuntu 24.04 SSM AMI resolved to `ami-0150109d3dd43737d`.
- Reviewed plan: two `c7i.large` app hosts, one `m7i.large` dependency host,
  one `c7i.large` load-generator/observer host; two worker slots per app host;
  four encrypted 40-GiB gp3 root volumes. Peak planned concurrency is **8
  vCPU**, below the 32-vCPU quota. The plan contains 31 creates, zero changes,
  and zero destroys. All 23 taggable resources carry `Task=DUR-050`,
  `Environment=portfolio-capacity`, and `ExpiresAt=2026-10-07T00:00:00Z`.
- Fresh no-apply plan was generated from source commit `aa7de7e` with Terraform
  **1.16.4** at `deploy/aws/.terraform/dur050-preflight-aa7de7e.tfplan`.
  SHA-256: `C22D814002CE14D5D6B1A6DDA42355E96291B0DCDB5969F8E435BEBEED4FAD6D`.
  The plan JSON was inspected without printing secret values: **31 creates, 0
  updates, 0 deletes; 4 instances (3 `c7i.large`, 1 `m7i.large`); 23/23
  taggable creates have the expected three tags; 8 planned vCPU**. This saved
  plan is local and ignored. It is not authorization to apply and must be
  regenerated if inputs, state, or source change. Apply, if later authorized,
  must use Terraform 1.16.4.
- The plan contains no NAT gateway, load balancer, or EKS resources. Inbound
  rules include only app API port 8080 from the load-generator security group,
  PostgreSQL 5432 from app and load-generator security groups, and Kafka 9092
  from the app security group. No public CIDR is allowed for PostgreSQL.
- At preflight, no `terraform apply` had run. The authorized first apply later
  used this exact saved plan; see the cycle record below. The plan was local,
  ignored, and contained ephemeral generated credentials. It was not reused
  after apply or destroy.

## First provisioning cycle (2026-09-25; no measurement block)

R158 implementation migrates the four first-cycle host IDs and unchanged
apply/destroy intervals from the campaign snapshot into the single task-wide
ledger at
experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json. The historical
host array remains labelled non-authoritative. No second apply, replacement,
paid block, or new campaign directory is allowed until Claude verifies R158,
R159, and R160.

- Live pre-apply checks ran from `2026-09-25T10:34:09.4189134Z` through
  `2026-09-25T10:34:15.4851534Z`: account `372206265946`, quota 32 vCPU,
  0 existing regional instances, actual spend `$1.476`, budget `$200`, all
  three notifications `OK`, both tags `Active`, and reviewed plan SHA-256
  unchanged. Terraform 1.16.4 applied the saved plan starting at
  `2026-09-25T10:34:38.8773119Z`; Terraform reported **31 added, 0 changed,
  0 destroyed**. Completion was observed by `10:35:28Z`.
- Host inventory from EC2:

  | Role | Instance | Type | AZ | EC2 launch time (UTC) |
  |---|---|---|---|---|
  | app-1 | `i-03a77d4d089b70cb8` | `c7i.large` | `us-west-1a` | `2026-09-25T10:35:05Z` |
  | app-2 | `i-03fc3b13714dc4134` | `c7i.large` | `us-west-1c` | `2026-09-25T10:35:05Z` |
  | dependency | `i-05027ac0bdf9f3816` | `m7i.large` | `us-west-1a` | `2026-09-25T10:34:53Z` |
  | load-generator | `i-09813309975a6d24b` | `c7i.large` | `us-west-1a` | `2026-09-25T10:34:51Z` |

- Bootstrap observations via SSM at the plan's exact checkout
  `aa7de7e05a6dc534f9771063a2e20729f0f10520`: both app hosts reported
  cloud-init done and healthy runtime/worker containers; the dependency host
  reported healthy PostgreSQL and Kafka. The load-generator reported
  `cloud-init status: error`, no bootstrap-complete marker, and
  `go: module cache not found: neither GOMODCACHE nor GOPATH is set` during
  `go build`; its required pilot binaries were therefore unavailable.
- A separate read-only SSM probe confirmed `AWS-RunShellScript` runs under
  `/bin/sh` and rejects Bash-only `set -euo pipefail` with
  `set: Illegal option -o pipefail`. The reset helper sends that same setting
  at the start of its remote command, so it would fail before restoring the
  volumes. The reset helper itself was not invoked. No reset sequence or
  unloaded calibration block was started.
  No pilot-calibration raw-data directory was created or edited.
- Teardown started at `2026-09-25T10:44:12.4253236Z`. Terraform reported
  **31 destroyed**, exit 0. The conservative common destroy-complete timestamp
  recorded for all host intervals is `2026-09-25T10:45:53Z`; individual
  `TerminateInstances` API event times are in `cost-manifest.json`. Afterward,
  Terraform state was empty and AWS returned no campaign-tagged volumes,
  addresses, snapshots, VPCs, security groups, IAM roles, or SSM parameters;
  all four instances were `terminated`.
- `cost-manifest.json` records the first-cycle IDs and timestamps. No paid
  block ran, so there is no pre-block ledger check artifact. Any future
  provisioning cycle is barred until R158's task-wide interval ledger and
  tests are independently verified by Claude; no replacement or second apply
  is authorized by this record.
- The post-destroy ledger audit is retained at
  `ledger-check-post-destroy-audit.json`: `PASS`, **$0.084656** accrued and
  projected EC2 instance cost, with zero additional reserve because all host
  intervals were closed. This is an accounting audit, not a paid-block check;
  non-EC2 charges and billing-data lag remain outside this ledger.

## Refreshed cost estimate

AWS Price List API was queried for `US West (N. California)` on 2026-09-24:
Linux On-Demand `c7i.large` **$0.1113/hour**, `m7i.large` **$0.1176/hour**;
gp3 **$0.096/GB-month**. AWS's current VPC price page lists public IPv4 at
**$0.005/address-hour**. The protocol's 72-hour baseline recomputes as:

| Cost component | Planning calculation | Estimate |
|---|---:|---:|
| EC2 | `(3 × $0.1113 + 1 × $0.1176) × 72 h` | $32.508 |
| gp3 | `4 × 40 GiB × $0.096/GB-month × 72/720 h` | $1.536 |
| Public IPv4 | `4 × $0.005 × 72 h` | $1.440 |
| **Direct 72-hour subtotal** |  | **$35.484** |
| Internet egress reserve | `50 GB × $0.09/GB`, full rate with no free-tier credit assumed | $4.500 |
| Cross-AZ transfer reserve | 100 GB each direction × $0.01/GB/direction | $2.000 |
| Bootstrap/retry reserve | 12 additional hours for all four hosts, including compute, gp3, IPv4 | $5.914 |
| Log/diagnostic allowance | conservative reserve; no managed log service is in this plan | $3.000 |
| Tax reserve | 10% of the $50.898 pre-tax estimate above | $5.090 |
| **Planning estimate** | rounded up | **$56.00** |

The planning estimate remains below the **$75** task cap by $19. The estimate
is not a bill or a cost guarantee; actual taxes, egress, retries, runtime, and
AWS billing attribution may differ. The per-host instance-hour ledger is at
`cost-manifest.json`; its checker requires all four instance IDs and apply
timestamps and reserves the planned block duration before returning `PASS`.
The ledger measures EC2 instance cost only; the aggregate ACTUAL alert covers
other account charges. Both R156 controls are configured and verified. One
first-cycle apply/destroy occurred, with the post-destroy ledger audit at
`$0.084656` EC2 cost; no paid measurement block ran because bootstrap tooling
was not ready. The populated pilot calibration must still be reviewed by
Claude before any final campaign runs.

Price references: [EC2 On-Demand](https://aws.amazon.com/ec2/pricing/on-demand/),
[EBS](https://aws.amazon.com/ebs/pricing/),
[VPC public IPv4 and data transfer pricing](https://aws.amazon.com/vpc/pricing/).

## Post-fix no-apply plan inspection (2026-09-25)

After the R158/R159/R160 implementation was pushed and hosted CI run
[`36164501714`](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/36164501714)
completed green, Terraform **1.16.4** generated a new saved plan from source commit
`4c625b3536da9f0334aa86bed01127914382fb34`. The local ignored plan is
`deploy/aws/.terraform/dur050-preflight-4c625b3.tfplan`, created at
`2026-09-25T17:33:41.5101373Z`, SHA-256
`6FE7CD0FAB90007AF6787BD5B7BD36927FAE34E8510B380995B718B4FD6AFCF2`.
The plan's inspection found **31 creates, 0 updates, 0 deletes, 0
replacements**; four instances (three `c7i.large`, one `m7i.large`), **8
planned vCPU** against the 32-vCPU quota; and all 23 resources with effective
tags carry `Task=DUR-050`, `Environment=portfolio-capacity`, and
`ExpiresAt=2026-10-07T00:00:00Z`. It contains no EKS, NAT gateway, or load
balancer resources. Before planning, read-only inventory found no
`Task=DUR-050` instances or volumes in `us-west-1`, and the local Terraform
state list was empty. The non-secret variables used for this plan are recorded
in `cost-manifest.json`; generated sensitive passwords are omitted. The plan
is local and ignored, contains sensitive inputs, and is **not authorization to
apply**. No apply or paid block followed this inspection.

## Second-cycle pre-apply gate (2026-09-25)

Claude round 96 authorized the second provisioning cycle using only the saved
plan above. Read-only checks ran from `2026-09-25T18:21:37.8588156Z` through
`2026-09-25T18:21:45.2250018Z`: STS account `372206265946`; Terraform 1.16.4;
plan SHA-256 unchanged at
`6FE7CD0FAB90007AF6787BD5B7BD36927FAE34E8510B380995B718B4FD6AFCF2`;
Terraform state empty; zero regional EC2 instances and EBS volumes. The D022
budget limit was `$200`, actual spend `$1.571`, and forecast `$5.268`. Its
three notifications were all `OK`: ACTUAL `$160`, ACTUAL `$76.47`, and
FORECASTED `$160`. `Task` and `Environment` cost-allocation tags were both
`Active` (last updated `2026-09-25T07:06:39Z`). The Terraform/runtime source
was unchanged from the reviewed plan's source commit. This gate matched; the
exact saved plan, and no regenerated plan, was the only authorized apply
input.

## Second-cycle apply and open intervals (2026-09-25)

Terraform 1.16.4 applied the reviewed saved plan at
`2026-09-25T18:23:07.2055684Z`; completion was observed at
`2026-09-25T18:23:44.7915956Z`. Terraform reported 31 added, 0 changed, and
0 destroyed. The four EC2 intervals are open in the task-wide ledger under
cycle `cycle-2-4c625b3`: app-1 `i-08b8632f1c3ef80d0` (`c7i.large`,
us-west-1a), app-2 `i-0eda5443fea88fe0a` (`c7i.large`, us-west-1c),
dependency `i-07b53b0242fa9851f` (`m7i.large`, us-west-1a), and load-generator
`i-0872b6791d08cb577` (`c7i.large`, us-west-1a). Each interval uses the
Terraform apply invocation start as its conservative billing start. No reset
or paid calibration block has started yet.

## Cycle-2 bootstrap gate (2026-09-25)

Before reset, read-only SSM commands returned `Success` on all four hosts;
`cloud-init status --long` reported `status: done`, `extended_status: done`,
and empty `errors` and `recoverable_errors`. On the load-generator host, the
completion marker
`/var/lib/durable-dur050-generator-bootstrap-complete` was present and each of
`bin/dur050-observer`, `bin/dur050-loadgen`, and `bin/dur050-sink` was
executable. Command IDs and per-host timestamps are retained in
`bootstrap-preflight-cycle-2.json`. No reset or paid block had started at the
time of this check.
