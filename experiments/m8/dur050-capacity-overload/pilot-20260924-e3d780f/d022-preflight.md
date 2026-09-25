# DUR-050 D022 preflight (no apply)

**Observed:** initial technical preflight 2026-09-24; R156 controls refreshed
2026-09-25, `admin-learning`, `us-west-1`
**Source commit:** `aa7de7e05a6dc534f9771063a2e20729f0f10520`
**Status:** no-apply technical preflight and approved R156 account controls
verified; no resources created; no measurement run; not measurement evidence.

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
- No `terraform apply` was run. The saved plan is local, ignored, and contains
  ephemeral generated credentials; it must never be committed or reused after
  state, variables, or source change.

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
other account charges. Both R156 controls are configured and verified. No
Terraform apply or paid measurement run has occurred. The populated pilot
calibration must still be reviewed by Claude before any final campaign runs.

Price references: [EC2 On-Demand](https://aws.amazon.com/ec2/pricing/on-demand/),
[EBS](https://aws.amazon.com/ebs/pricing/),
[VPC public IPv4 and data transfer pricing](https://aws.amazon.com/vpc/pricing/).
