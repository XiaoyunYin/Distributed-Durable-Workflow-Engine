# DUR-050 D022 preflight (no apply)

**Observed:** 2026-09-24, `admin-learning`, `us-west-1`
**Source commit:** `aa7de7e05a6dc534f9771063a2e20729f0f10520`
**Status:** read-only technical preflight passed; account-control prerequisites
pending approval; no resources created; not measurement evidence.

## Authorization and budget

- Budget: `durable-engine-D022-aggregate-20260923`, custom period
  2026-09-23 00:00 UTC through 2026-10-07 00:00 UTC.
- Aggregate maximum: **$200**. Actual and forecast notification thresholds:
  **$160**, absolute value, both in `OK` state at preflight. The existing
  actual alert is aggregate-only; it does not enforce DUR-050's $75 cap.
  The Task and Environment cost-allocation tags were `Inactive` in Claude's
  round-90 read-only check. No account change has been made: the additional
  ACTUAL alert and tag activation are pending explicit user approval. The
  subscriber was confirmed as the user-supplied email; the address is omitted.
- At check time: actual **$1.476**, forecast **$5.257**. Forecast headroom to
  the $160 stop alert: **$154.743**. DUR-050's own maximum remains **$75**;
  using the full task cap would leave $79.743 of aggregate forecast headroom
  to the $160 alert.
- Once approved, set the aggregate ACTUAL absolute notification to the
  aggregate actual spend re-read immediately before apply plus **$75** (about
  **$76.48** at this preflight snapshot), and record its state. Activate
  `Task` and `Environment` cost-allocation tags before apply; record the UTC
  activation time and whether billing backfill is needed. Do not apply while
  either control is pending or the task-specific tag remains inactive.
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
other account charges. Neither the added alert nor cost-tag activation has
been performed pending user approval. No final paid measurement run is
authorized by this preflight record; the populated pilot calibration must
still be reviewed before any final campaign runs.

Price references: [EC2 On-Demand](https://aws.amazon.com/ec2/pricing/on-demand/),
[EBS](https://aws.amazon.com/ebs/pricing/),
[VPC public IPv4 and data transfer pricing](https://aws.amazon.com/vpc/pricing/).
