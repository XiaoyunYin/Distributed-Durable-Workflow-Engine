# DUR-050 capacity, optimization, overload, and bounded-soak protocol v2

- Protocol: `dur050-capacity-overload.v2`
- Design frozen for pilot-method review: 2026-09-24
- State: **pilot protocol only; not final-run-ready**. The numeric p99 SLO is
  deliberately pending measurement by the pilot below.
- Scope authority: D025; aggregate cloud authority and stop rules: D022;
  account/region selection: D023
- Review gates: Claude reviews this pilot design; then the pilot runs only after
  D022 preflight. Its durable output is the pilot summary and derived numeric SLO
  inserted into this protocol (no separate pilot-results artifact). Claude must
  review that updated, committed protocol before any final capacity, optimization,
  overload, or soak run. No Terraform apply before the first review and D022
  preflight; no final paid run before the second review.

## Question and claim boundary

Measure the maximum offered workflow arrival rate at which the fixed AWS
deployment meets a numeric end-to-end p99 SLO; identify one observed bottleneck;
make one change and compare it at matched offered rates; then measure bounded
overload, drain, and a one-hour soak. This is one-region evidence for the exact
images, topology, synthetic workload, campaign admission gate, and fault-free
performance conditions below. It is not a multi-AZ failover, database HA,
general product admission-policy, universal capacity, or customer-workload
claim. DUR-053 owns replica scale-out comparisons.

## Frozen topology, placement, and quota guard

Region: `us-west-1`. Choose the **split app-host** design: app host 1 in
`us-west-1a`, app host 2 in `us-west-1c`; dependency and load-generator hosts in
`us-west-1a`. Record actual AZ placement and cross-AZ bytes. This is a measured
network placement, not an HA/failover claim.

| Role | Count and type | Fixed configuration |
|---|---:|---|
| Runtime + scheduler + Python worker | 2 × `c7i.large` (2 vCPU, 4 GiB each) | One scheduler per host; 2 worker slots per host; 4 total |
| PostgreSQL + single Kafka broker | 1 × `m7i.large` (2 vCPU, 8 GiB) | One dependency host; pinned images; no managed database or broker |
| Open-loop load generator | 1 × `c7i.large` (2 vCPU, 4 GiB) | Separate host; private API access only; no workflow execution |
| **Peak planned total** | **4 hosts, 8 vCPU** | Includes no other study hosts |

Keep `m7i.large`, not `m7i.xlarge`: it matches one app host's vCPU count,
provides twice the app-host memory, and keeps the dependency role visible as a
possible measured bottleneck without consuming extra quota/cost. Do not resize
it mid-study. The preflight adds these 8 vCPU to all concurrent On-Demand
Standard vCPU in account `372206265946` and proves total <=32 under
`L-1216C47A`; otherwise stop. No quota request is authorized.

Use encrypted 40-GiB gp3 root volumes on all four hosts, baseline gp3 IOPS and
throughput only, immutable full-SHA application source, pinned dependency image
digests, and a pinned AMI. Add a load-generator security-group rule for private
app API traffic; never expose the API publicly. Database and Kafka ingress stays
restricted to the application security group. No NAT Gateway, EKS, load
balancer, public managed service, or retained snapshot. Campaign state lives on
dedicated disposable PostgreSQL/Kafka volumes, not the local development stack.

### Cost envelope

The topology and 72-running-hour estimate are unchanged from v1. Regional AWS
Price List API values queried for `us-west-1` on 2026-09-24:

| Item | Calculation | Estimate |
|---|---:|---:|
| EC2 On-Demand | 3 × `c7i.large` × $0.1113/h + 1 × `m7i.large` × $0.1176/h, × 72 h | $32.51 |
| gp3 storage | 4 × 40 GiB × $0.096/GiB-month × 72/720 h | $1.54 |
| Public IPv4 | 4 × $0.005/IP-hour × 72 h | $1.44 |
| **Direct subtotal** | excludes transfer, logs, tax, and bootstrap retries | **$35.49** |
| **DUR-050 task envelope** | includes pilot, final runs, teardown, plus contingency | **$75.00 maximum** |

The $75 task envelope is inside, not additional to, D022's $200 aggregate cap.
Stop at the $75 DUR-050 attribution limit, D022's $160 stop threshold, or
before its $200 hard cap; obey its 14-day paid window and cleanup rules. Before
apply, re-query prices and include data transfer, logs, applicable tax,
bootstrap/retry time, and all concurrent resources. The direct subtotal is an
estimate, not a bill or an assertion about credits. Price sources:
[AWS EC2 On-Demand](https://aws.amazon.com/ec2/pricing/on-demand/),
[AWS EBS](https://aws.amazon.com/ebs/pricing/),
[AWS VPC public IPv4](https://aws.amazon.com/vpc/pricing/), and the regional
Price List API response retained with D022 preflight evidence.

## Workload and measurement invariants

Exercise the deployed HTTP API -> PostgreSQL transaction/outbox -> Kafka relay/
consumer -> scheduler -> Python worker -> result/terminal path. Never call the
in-process Store/Engine path for final measurements. No model/provider calls or
external effects.

Two equally weighted, deterministically shuffled workflow families:

1. `seq-8`: eight sequential pure-activity nodes.
2. `fanout-8`: eight parallel pure-activity branches and one join.

Each activity is a campaign-only `dur050.sha256@v1` pure activity: 5,000
deterministic SHA-256 rounds over a fixed 256-byte input, returning a digest.
Register it only in the campaign worker configuration. Keep graph, version,
payload, activity implementation, image digests, and seed algorithm fixed.
Final measured repeats contain 2,000 unique workflows (1,000/family) with seeds
`50050`, `50051`, and `50052`.

Arrivals are open-loop against monotonic deadlines. The separate load generator
uses a fixed 64-request HTTP pool. A run is invalid if p99 scheduled-to-submit
delay exceeds 50 ms, generator CPU exceeds 80% for over 1% of its window, or a
scheduled request is omitted. Reconcile a dropped/uncertain response with the
same submission key before counting unique acceptance. Run eight warmups (four
per family) and drain them before a measured run.

## Campaign-scoped admission gate (R143)

The admission gate is **campaign-only behavior**, enabled only for `dur050-*`
namespaces by an explicit campaign configuration. It is not a new general
product/API guarantee. Its fixed limits are:

- at most 1,000 nonterminal accepted workflows per `dur050-*` namespace;
- at most 50,000 pending outbox events per such namespace.

The reservation check and workflow/outbox insertion are atomic under concurrent
submissions. Return HTTP `429 ADMISSION_LIMIT` at the workflow limit and `503
BACKPRESSURE` at the pending-event limit, both with `Retry-After: 1`; a rejection
creates no workflow, attempt, or dispatch event. An already accepted identical
key resolves to its original workflow even at the cap; an unaccepted key may be
retried after capacity frees. Slot release occurs **in the same transaction as
the terminal state transition**, exactly once. The limits/metrics are identical
in every final arm: capacity search; 50/80/100% points; baseline and optimized
matched comparison; overload; and soak. The gate is never disabled for a final
arm and is not eligible to be the one optimization. If a future optimization
would alter it, register that as campaign-code tuning and get a new protocol
review first.

Before the pilot, fault-inject between writing the terminal workflow state and
releasing its admission slot. The injected transaction must roll back both
changes; after retry, both must commit once. Also test both caps concurrently,
idempotent duplicate submissions at the cap, and slot release after terminal
commit. The campaign gate must not affect namespaces outside `dur050-*`.

The gate ON/OFF comparison exists **only in the low-rate pilot** to measure
per-submission and per-completion overhead; all final arms remain ON with the
same limits. Report paired median and p95 transaction durations and deltas.
Pilot gate-OFF data is calibration only and is not mixed into final results.

## Pilot and numeric SLO freeze (R145)

After Claude approves this pilot design and D022 preflight passes, use the exact
topology, images, worker capacity, workload, and observer below. The pilot has
no separate durable results artifact: record only its summary, validity, and
derived SLO in this protocol; temporary raw pilot rows are deleted after the
summary is checked.

1. **Unloaded latency:** with the gate ON, one workflow in flight at a time;
   collect 300 observations per family in three fresh 100-workflow blocks.
   Record per-family p50/p95/range and all sample counts.
2. **Gate overhead:** at 0.25 workflows/s, use six fresh 100-workflow blocks,
   50 per family each, ordered OFF/ON/ON/OFF/OFF/ON (three blocks per mode).
   Record API CreateWorkflow transaction duration and terminal-transition
   transaction duration; pair adjacent ON/OFF blocks and summarize median/p95
   deltas. No workflow from this comparison enters final data.
3. **Generator validity:** run its private HTTP sink check for five minutes at
   64 scheduled requests/s. Require no missed request, p99 schedule lag <=50 ms,
   and generator CPU <=80%; record observer QPS and any schedule gaps.
4. **Pilot knee:** run a short 60-second open-loop staircase at
   0.25/0.5/1/2/4/8/16/32/64 workflows/s, stopping at the first invalid
   generator condition or sustained growth in outstanding work. This is only
   for selecting the final grid's starting rate; its throughput/latencies are
   not results and do not count toward capacity.

Derive numeric SLO as `ceil_to_0.5s(max(2.5s, 2 × max(unloaded family p95) +
1.0s))`. The factor of two is explicit headroom above the measured unloaded
baseline; the extra second covers the batch observer's maximum sampling
interval. Record the exact two pilot p95s, arithmetic, resulting numeric SLO,
and rationale here. Do not substitute an expected value for a measurement. If a
family's pilot has fewer than 300 valid observations or the generator check
fails, repeat/fix the pilot before setting the SLO. After populating the pilot
summary and numeric SLO, commit the updated v2 and obtain Claude review of that
exact commit before final paid runs.

## End-to-end latency and observer model (R144)

Do not poll workflow status per workflow. A dedicated observer uses a separate
read-only database pool capped at two connections and runs **one batched status
query per second** over the measured IDs. Record actual observer QPS, query
duration, failed polls, and maximum sampling gap. If it misses a one-second
interval by more than one interval, invalidate that run. No 100-ms API GET loop
and no observer connection from the 64-request submitter pool.

Primary end-to-end latency is load-generator monotonic time from each scheduled
arrival to the first batch snapshot seeing that workflow terminal. This includes
submission, durable work, execution, and at most one second of observer delay;
the numeric per-family p99 SLO applies to this metric in each repeat. Also
report scheduled-arrival-to-HTTP-accept latency separately. As a diagnostic,
report `workflow_executions.created_at` to the final terminal
`transition_history.created_at` using the same PostgreSQL clock. These are
database event timestamps, not transaction commit timestamps or a cross-host
clock comparison; do not label them commit-to-commit or substitute them for the
primary SLO.

## Database and telemetry controls (R144)

- Set `DUR049_RECORD_LEASE_ACQUISITIONS=0` in the DUR-050 AWS Compose config and
  assert the effective value is zero in every pilot/final arm. Do not write the
  append-only DUR-049 lease-acquisition ledger during this study.
- Enable PostgreSQL `pg_stat_statements` using `shared_preload_libraries` and
  create the extension identically in every pilot/final database. Record
  settings and extension version.
- Every measured run starts from a **fresh campaign DB and Kafka data volume**
  restored to the same migrated baseline. Run the same eight warmups, then
  snapshot key table row counts/bytes, database size, consumer offsets, and
  extension counters immediately before the measured window. Preserve each
  result through the invariant check and accepted-work reconciliation; destroy
  that run's campaign volumes before the next block. Never raw-delete live
  outbox/inbox rows. This makes AB comparisons start from a fixed-size state.
- The only app AZ split, instance sizes, four worker slots, database settings,
  Kafka settings, admission-gate limits, `pg_stat_statements`, observer, and
  lease-ledger-off setting remain constant in every final arm.

## Capacity search and rate points (R145)

Use the pilot knee only to select the starting point: `r0` is the largest
power-of-two rate <= half the last pilot staircase rate with no sustained
outstanding-work growth, bounded to [0.25, 32] workflows/s. If no such point is
observed, stop and revise the pilot/protocol; do not start final measurements.
Starting at `r0` avoids spending the search on arbitrary rates already shown to
be far below or above this system's useful range.

At each candidate rate, each repeat uses the fixed 2,000-workflow cohort and
three repeats. Test ascending powers of two beginning at `r0`, doubling to 64,
and **stop the ascending grid at the first failing rate**. A rate passes only if
all three repeats meet the per-family numeric p99 SLO, no 429/503 rejection
occurs, all scheduled requests are accounted for, generator/observer validity
checks pass, and accepted workflows terminalize within 120 seconds. After the
first failure, bisect the last-pass/first-fail interval up to four times; do not
continue testing higher grid points.

Define `lambda*` as the highest rate in the contiguous tested prefix where every
lower tested rate passed and all three repeats pass. If a bisection contradicts
monotonicity (a higher tested rate passes after a lower one failed), report
`NON_MONOTONIC / lambda* UNRESOLVED` and do not promote a single capacity number
without a new protocol-reviewed investigation. If 64/s passes, report `at least
64/s`; the maximum is not located. In that case the overload campaign is
**INCOMPLETE** because it cannot offer above the tested ceiling. Do not silently
raise the ceiling or claim an overload result.

At 50%, 80%, and 100% of the established `lambda*`, run three repeats per point
with 2,000 workflows each and report per-family/pooled p50/p95/p99, accepted
terminal workflows/s, rejections/errors, observer QPS, and drain time. Rotate
the recorded seeds among repeats. If `lambda*` is unresolved, do not calculate
these points as if capacity had been established.

## Required metrics and correctness

Capture 10-second samples and raw per-workflow rows for:

- offered, unique accepted (`201`), idempotent replay (`200`), rejected
  (`429`/`503`), transport/error, and terminal rates;
- p50/p95/p99 at 50%, 80%, and 100% of `lambda*`, plus scheduled-to-accept,
  observer QPS/latency/gaps, and drain time;
- each scheduler and worker process/container CPU and memory (RSS/cgroup), host
  CPU/memory/disk/network, PostgreSQL TPS/`pg_stat_statements`/connections/lock
  waits, and Kafka lag per topic/partition;
- durable accepted/runnable workflows, active claims, terminal counts,
  admission slots, pending outbox count/oldest age, and open obligations;
- independent invariant-checker output and exact accepted-ID-to-terminal
  reconciliation.

Include an observed interval where Kafka lag is zero while activity is
claimed/running or durable runnable work remains; report all those signals
together. Kafka lag alone is not the backlog. Fail any run with a lost or
duplicated accepted ID, failed/unaccounted accepted workflow, nonterminal work
after the 10-minute drain, or checker violation. Do not claim exactly-once
activity execution.

## One optimization and matched comparison

After baseline `lambda*` is measured, use Go pprof, the identically enabled
`pg_stat_statements`, lock waits, and the separate worker profile below to
identify one bottleneck. Register the evidence, one hypothesis, exactly one
change, expected metric, source commit, and rollback **before** the optimized
run. Admission limits, gate logic, `pg_stat_statements`, worker slots, observer,
lease-ledger setting, and topology cannot be the optimization and cannot change
between arms. Compare baseline A and optimized B at 80% and 100% of baseline
`lambda*` in the order A-B-A-B-A-B for each rate; each block gets the fresh
fixed-size DB/Kafka baseline above and three matched pairs total. Publish all
rows and paired differences/intervals. If the change does not resolve an
improvement, say so. Then re-run the rate search and 50/80/100% points on the
optimized build to independently establish its `lambda*`.

For diagnosis only, collect Go CPU/heap pprof at baseline saturation and a
60-second built-in Python `cProfile` capture from one worker process. The
Python profile is a separate diagnostic run and its overhead is excluded from
all capacity/optimization rows; record its exact process, window, and setting.
Do not leave profiler instrumentation active in final measured arms.

## Overload and bounded soak

With the optimized build and the gate still ON, offer `min(5 × optimized
lambda*, 64)` workflows/s for up to 20 minutes or until the admission limit
rejects work. Record offered/accepted/rejected rates, outstanding count (never
above 1,000), pending events (never above 50,000), Kafka lag, oldest age, and
the exact point of each rejection. Stop submissions and require all accepted
work to terminalize and durable/outbox/Kafka work to drain within 10 minutes,
with the independent checker passing. If no rejection occurs before the limit,
call the backpressure demonstration incomplete. If `lambda* >=64`, the overload
arm is incomplete as stated above.

Run a 60-minute soak at 80% of optimized `lambda*`, sample every 10 seconds,
then drain for at most 10 minutes. Report resource, connection, backlog-age,
lag, and goroutine trends; require complete reconciliation and no nonterminal
work after drain. Stop on the task cost/time cap, any hard queue-limit breach,
disk under 20% free, or dependency health failure; classify, do not discard, a
stopped run.

## Evidence, preflight, and handoff

Use a new campaign ID under
`experiments/m8/dur050-capacity-overload/<campaign-id>/`; never overwrite v1 or
prior evidence. The only durable pilot deliverable is the updated v2 protocol
with pilot measurements, numeric SLO, and derivation; final campaigns preserve
the reviewed protocol, D022 preflight/prices, Terraform transcripts, image and
source digests, per-run rows, metrics/profiles, optimization registration/diff,
checker output, reconciliation, and teardown/billing attribution. Partial or
missing signals are `INCOMPLETE`/`FAIL`, never `PASS` by construction.

Before any pilot or final apply, revalidate D022 spend-to-date/$160 stop/$200
cap, the $75 DUR-050 envelope, 14-day window, current `admin-learning` login,
us-west-1 availability/prices, and total concurrent On-Demand vCPU <=32. Notify
the user before an AWS login refresh or GitHub push is needed. No resource is
retained past the authorized cleanup window.
