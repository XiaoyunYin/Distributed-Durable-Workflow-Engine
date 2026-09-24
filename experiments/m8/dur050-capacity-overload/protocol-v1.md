# DUR-050 capacity, optimization, overload, and bounded-soak protocol

- Protocol: `dur050-capacity-overload.v1`
- Frozen: 2026-09-24
- State: frozen for Claude review; **not yet approved for final paid runs**
- Scope authority: D025; aggregate cloud authority and stop rules: D022;
  account/region selection: D023
- Final-run gate: Claude reviews this exact committed protocol, then Codex
  performs the D022 preflight. No Terraform apply or final AWS measurement before
  both gates pass. If a protocol parameter changes after review, issue a new
  version and obtain review before collecting final data.

## Question and claim boundary

Measure the maximum offered workflow arrival rate at which this fixed AWS
deployment meets the frozen end-to-end p99 SLO; identify one observed bottleneck;
make one change and compare it at matched offered rates; then measure bounded
overload, drain, and a one-hour soak. This is one-region, one-dependency-host
evidence for the exact images, topology, synthetic workload, and fault-free
performance conditions below. It is not a multi-AZ failover, database HA,
general production-capacity, or customer-workload claim. DUR-053 owns replica
scale-out comparisons.

## Frozen topology and quota guard

Region: `us-west-1`; app hosts occupy the two declared AZs `us-west-1a` and
`us-west-1c`. The dependency and load-generator hosts use `us-west-1a`. Record
actual subnet/AZ placement and cross-AZ bytes in the resource manifest.

| Role | Count and type | Fixed configuration |
|---|---:|---|
| Runtime + scheduler + Python worker | 2 × `c7i.large` (2 vCPU, 4 GiB each) | One scheduler per host; 2 worker slots per host; 4 worker slots total |
| PostgreSQL + single Kafka broker | 1 × `m7i.large` (2 vCPU, 8 GiB) | One dependency host; pinned images; no managed database or broker |
| Open-loop load generator | 1 × `c7i.large` (2 vCPU, 4 GiB) | Separate host, no workflow execution; private API access only |
| **Peak planned total** | **4 hosts, 8 vCPU** | Includes no other study hosts |

The preflight must add these 8 vCPU to all concurrent On-Demand Standard vCPU
usage in account `372206265946` and prove the total is at most 32, quota
`L-1216C47A`. If current account usage, quota, or instance placement cannot be
verified, stop before apply. Do not request a quota increase under this task.

Use encrypted 40-GiB gp3 root volumes on all four hosts, baseline gp3 IOPS and
throughput only, immutable full-SHA application source, pinned dependency image
digests, and an AMI ID recorded before apply. Add a load-generator security-group
rule for private app API traffic; do not expose the API publicly. Database and
Kafka ingress stays restricted to the application security group. No NAT
Gateway, EKS, load balancer, public managed service, or retained snapshot is in
this topology. Keep all campaign rows in a fresh campaign-only database/volume;
retain evidence until the independent checker and reconciliation finish, then
destroy the full stack rather than raw-deleting live outbox/inbox rows.

### Cost envelope

An indicative 72-running-hour estimate, using the AWS Price List API values
queried for `us-west-1` on 2026-09-24, is:

| Item | Calculation | Estimate |
|---|---:|---:|
| EC2 On-Demand | 3 × `c7i.large` × $0.1113/h + 1 × `m7i.large` × $0.1176/h, × 72 h | $32.51 |
| gp3 storage | 4 × 40 GiB × $0.096/GiB-month × 72/720 h | $1.54 |
| Public IPv4 | 4 × $0.005/IP-hour × 72 h | $1.44 |
| **Direct subtotal** | excludes transfer, logs, tax, and bootstrap retries | **$35.49** |
| **DUR-050 task envelope** | direct subtotal plus up to $39.51 contingency | **$75.00 maximum** |

The $75 task envelope is an allocation inside—not an addition to or change of—
D022's $200 aggregate cap. Stop provisioning/operation if the updated estimate
or actual attribution would exceed $75 for DUR-050, if D022's aggregate
accounting reaches its $160 stop threshold, or before the aggregate $200 cap;
follow D022's 14-day maximum paid window and retained-resource rules. Re-query
all prices and include data transfer, logs, taxes where applicable, bootstrap
time, and any concurrent resources in the preflight. This table is an estimate,
not a bill or an assertion about credits.

Price sources: [AWS EC2 On-Demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/),
[AWS EBS pricing](https://aws.amazon.com/ebs/pricing/),
[AWS VPC public IPv4 pricing](https://aws.amazon.com/vpc/pricing/), and the
regional AWS Price List API response retained with preflight evidence. AWS says
EBS volume charges continue until storage is released and public IPv4 is billed
hourly; revalidate exact regional rates immediately before provisioning.

## Frozen workflow and arrival workload

Exercise the deployed HTTP submission path and all intervening production
components: API → PostgreSQL transaction/outbox → Kafka relay/consumer →
scheduler → Python worker → result/terminal receipt. Do not call the in-process
Store/Engine path directly. No model/provider calls or external effects.

Two equally weighted workflow families, deterministically shuffled per run:

1. `seq-8`: eight sequential pure-activity nodes.
2. `fanout-8`: eight parallel pure-activity branches and one join.

Each activity is a campaign-only `dur050.sha256@v1` pure activity: 5,000
deterministic SHA-256 rounds over a fixed 256-byte input, returning a digest.
Register it only in the campaign worker configuration; it must not become a
default production activity. The activity graph, definition version, payload,
activity implementation, image digests, and seed algorithm are identical across
all arms. Each measured repeat schedules 2,000 unique workflows (1,000 per
family), with deterministic seeds `50050`, `50051`, and `50052`.

Arrivals are open-loop, evenly spaced against monotonic deadlines, not blocked
on prior completions. The separate load generator uses a fixed 64-request HTTP
client pool and records scheduled-versus-actual submit delay. If p99 submit
schedule delay exceeds 50 ms, generator CPU exceeds 80% for over 1% of the
measurement window, or the client misses a scheduled submission, mark that run
invalid; do not call it backend saturation. A client error or dropped response
is reconciled by retry/query with the same submission key before counting the
unique workflow. Run eight warmup workflows (four per family), drain them, and
exclude them from all measured rows.

### Durable admission and bounded backlog requirement

The current `CheckBackpressure` is a reconciliation/dispatch-side guard; it is
not evidence of a global submission admission cap. Before any final run, add or
identify and test a namespace-scoped durable gate with these frozen limits:

- At most **1,000 nonterminal accepted workflows** per `dur050-*` campaign
  namespace.
- At most **50,000 pending outbox event rows** for that namespace.
- The check/reservation and workflow/outbox insert are atomic under concurrent
  submissions; the limit cannot be overshot by racing requests.
- At the workflow limit return HTTP `429 ADMISSION_LIMIT`; at the pending-event
  limit return HTTP `503 BACKPRESSURE`, with `Retry-After: 1`. Rejected calls
  create no workflow, attempt, or dispatch event. An already accepted identical
  idempotency key still resolves to its original workflow even at the limit;
  retrying a rejected key after capacity frees can create one workflow.
- Terminal completion releases an outstanding-work slot exactly once. Export
  bounded-label metrics for accepted/rejected submissions and slots in use.

These are DUR-050 implementation/acceptance requirements, not features claimed
to exist today. Prove race behavior at both limits, idempotency, release on
terminal completion, and no partial writes before the paid workload. If this
gate cannot be implemented without changing protected scope, stop and request a
decision rather than running an unbounded overload test.

## SLO, capacity search, and latency points

Frozen SLO: empirical **p99 end-to-end latency ≤ 5.0 seconds separately for
each workflow family in every repeat**. Latency starts at the scheduled
open-loop arrival (monotonic clock on the load-generator host) and ends at the
first successful status read observing a terminal state; poll every 100 ms.
Keep the client on one host so duration does not depend on cross-host clock
synchronization. A workflow not terminal within 120 seconds fails that repeat.

Capacity search: test offered rates 0.25, 0.5, 1, 2, 4, 8, 16, 32, and 64
workflows/s in ascending order, three repeats per rate. If the passing/failing
boundary is bracketed, bisect up to four times until the bracket is at most 10%
wide. A rate passes only if all three repeats meet the per-family p99 SLO, no
admission/backpressure rejection occurs, all scheduled requests are accounted
for, generator validity checks pass, and all accepted workflows terminalize
within 120 seconds. Report λ* as the highest tested passing rate and show the
nearest higher failing rate; if 64/s passes, report only `at least 64/s`, not a
maximum. Report empirical p99 per repeat and min/median/max across repeats; do
not use tail percentiles across only three run-level values.

At 50%, 80%, and 100% of λ*, run three new measured repeats at each rate, with
the same 2,000-workflow cohort and seeds rotated among repeats. Report per-family
and pooled p50/p95/p99, accepted terminal workflows/s, rejection/error counts,
and drain time. If λ* is below the lowest grid rate, report that the SLO was not
established; do not invent a capacity number.

## Metrics and diagnosis

Capture synchronized 10-second samples and raw per-workflow rows for:

- Offered, accepted (`201` unique), idempotent replay (`200`), rejected (`429`/
  `503`), transport/error, and terminal workflow rates.
- End-to-end p50/p95/p99 and completion/drain times at 50%, 80%, and 100% λ*.
- Per scheduler and per worker process/container CPU and memory (RSS/cgroup),
  plus host CPU, memory, disk, and network.
- PostgreSQL transactions/s, `pg_stat_statements` deltas, active/maximum pool
  connections, lock-wait count and duration, and database CPU/memory.
- Kafka consumer lag per topic/partition, plus pending outbox count/oldest age.
- Durable accepted/runnable workflows, active claims, terminal state counts,
  outstanding admission slots, and open obligations.
- Go CPU/heap pprof profiles from each scheduler at baseline saturation; keep
  profiler endpoints private and disabled outside the campaign.
- Independent invariant-checker output and exact accepted-ID-to-terminal
  reconciliation; fail the run for any lost/duplicated accepted ID, failed
  terminal, unaccounted accepted request, or checker violation.

Include an observed interval where Kafka consumer lag is zero while activities
remain claimed/running or durable runnable work remains; record all three
signals together. Kafka lag alone is not a workflow-backlog measure. Reset
`pg_stat_statements`/metric deltas at each run boundary or explicitly subtract
the recorded before-snapshot. Record clock source and scrape gaps.

## One optimization, matched comparison, overload, and soak

After the baseline is complete, identify one bottleneck from pprof and PostgreSQL
statistics. Before changing code/configuration, record a short optimization
registration: evidence, one hypothesis, exactly one change, expected affected
metric, source commit, and rollback. Do not try several candidates and publish
only the winner. Compare baseline and optimized builds at identical offered
rates of 80% and 100% of baseline λ*, with the same topology, worker slots,
workload, seeds, cohort, and three repeats. Publish all run rows and compute
paired differences/intervals; if evidence does not resolve an improvement, say
so. Then repeat the capacity search and 50/80/100% latency points on the single
optimized build so its λ* is independently measured.

Overload the optimized system at `min(5 × optimized λ*, 64)` workflows/s for
up to 20 minutes or until the frozen admission limit rejects work. Record offered,
accepted and rejected rates, outstanding count (never above 1,000), pending
events (never above 50,000), Kafka lag, oldest-work age, and the point each
backpressure response occurs. Stop submissions after the ramp; require every
accepted workflow to reach a terminal state and the durable/outbox/Kafka work to
drain within 10 minutes, with the independent checker passing. If the cap is not
reached, label the backpressure demonstration incomplete rather than claiming
the rejection path was exercised.

Run one bounded **60-minute soak** at 80% of optimized λ*, sample the metric set
every 10 seconds, then allow at most 10 minutes to drain. Report resource,
connection, backlog-age, lag, and goroutine trends; require complete accepted
work reconciliation and no nonterminal work after drain. Stop the run on the
task cost/time cap, any hard queue cap breach, host disk under 20% free, or
dependency health failure; record the trigger and classify the run rather than
discarding it.

## Evidence and review gates

Store each campaign under a new unique directory at
`experiments/m8/dur050-capacity-overload/<campaign-id>/`; never overwrite pilot
or prior evidence. Include this protocol and Claude's review reference, D022
preflight and live quota accounting, current price queries and cost manifest,
Terraform plan/apply/destroy transcripts, image/source digests, raw submissions
and status rows, 10-second metrics, pprof files, PostgreSQL/Kafka snapshots,
optimization registration/diff and paired rows, overload/soak time series,
checker output, accepted-work reconciliation, and post-destroy inventory/bill
attribution. Mark missing signals or partial campaigns `INCOMPLETE`/`FAIL`, never
`PASS` by construction.

Before apply, revalidate the D022 aggregate spend-to-date and $160 stop point,
the $75 DUR-050 envelope, remaining time in the 14-day paid window, current
`admin-learning` login, us-west-1 availability/prices, and total concurrent
On-Demand vCPU ≤32. Notify the user before any AWS login refresh is needed. No
resource is retained past the authorized cleanup window.
