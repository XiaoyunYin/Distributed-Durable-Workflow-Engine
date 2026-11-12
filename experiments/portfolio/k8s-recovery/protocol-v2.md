# DUR-053 Kubernetes recovery protocol v2

Registered 2026-11-03, before pilot or measurement, against review base
`d1245dec098ae4102d6a2ec5362497ea72919c48`. Supersedes
[protocol v1](protocol-v1.md) for K3-K7 under Round 150 R229/R230. Authority: user-authorized D035;
Codex implements and Claude reviews each exact milestone SHA. This is a
protocol, with no measured results. K0 changes records only.

## Amendment A (registered before any measurement, 2026-11-03)

Amendment A, directed by Claude in Round 151 under R229, changes the long-arm
measured window, the per-episode reset/export allowance, the family execution
order and the schedule arithmetic below. It is registered before any pilot or
measurement; no results are changed. The long-arm measured window is
`W_long=max(180 s,20/(0.5*r))`; every other arm keeps its previously registered
window. The reset/export allowance is 60 s per episode. Execute families in
this order: K3 -> K4 -> K6 -> K5 -> K7. Thus duplicate-effect evidence in K6
precedes the K5 scaling comparisons. The existing 72 h cumulative hard stop
and rule to retain/report unperformed cells remain in force.

## Scope and authority

Run only on single-host kind on Docker Desktop/WSL2, with pinned tools, node
image, add-ons and application image digests in each manifest. PostgreSQL and
the single Kafka broker remain outside kind, on the host Docker network;
reuse one kind cluster per campaign family (K3, K4, K5, K6, K7). Each
episode uses a fresh engine database and engine namespace, with independent
IDs and no inherited engine work; K6 also isolates the sink ledger per episode.
Export and reconcile before deleting an episode. Reset Kafka group state and
verify no previous-episode records or consumers remain in the shared group;
do not rebuild kind for each episode. Stop Compose application services because they
share `runtime-workers-v1`. Kubernetes objects, probes, controllers and pod
deletion never grant workflow ownership: PostgreSQL leases, epochs, revisions,
attempts and claim tokens retain that authority. No scheduler leader election.

Evidence supports lifecycle and correctness within this topology. It supports
neither independent-host capacity nor HA, storage-loss durability, replicated
Kafka, production security/readiness, universal exactly-once execution, or EKS.
There is no cloud or model spend. No milestone requires a user action, demo,
login, payment or further approval. Claude handles publication when Codex's
sandbox cannot authenticate, under D035's exact-head review and CI conditions.

## Workload and R223 disposition

The primary workload has one activity node followed by success. Its node ID
is the actual allowlisted activity name, with version `v1` and immutable
`PURE_ACTIVITY`. In every consecutive ten submissions, positions 0-7 use
`dur048.sleep`, position 8 uses `pure.echo`, and position 9 uses `pure.add`.
Input is a deterministic object containing run ID and sequence for echo/sleep,
and `[2,3]` for add. The expected results are the original object or `5`.
Definitions, input bytes and hashes are frozen in the K3 manifest. Distinct
submission keys identify scheduled arrivals; a transport retry reuses the same
key and body. Register no `dur050.sha256.*` alias for this workload.

| Duration arm | Worker setting `DUR048_ACTIVITY_DELAY_MS` | Purpose |
| --- | --- | --- |
| short | 100 | Short work relative to drain and heartbeat budgets |
| medium | 5000 | Work near the heartbeat interval |
| long | 45000 | Work exceeding the 30 s worker drain deadline |

Set `DUR048_ALLOW_FIXTURE_ACTIVITY=1` in schedulers and all runtime roles that
resolve activities; the Python registry requires the positive delay above.
Freeze the worker drain deadline at 30 s and Kubernetes termination grace at
40 s; the 45 s long arm crosses the drain deadline. Heartbeats and existing
fenced timeout recovery remain required.
Use separate runs and homogeneous worker settings for each duration arm;
never change the duration within a run. Two slots per pod, eight task/event
partitions, one shared worker group, fixed two schedulers except scheduler
replacement drills. Maximum three schedulers is an explicit rollout control,
not a scheduler scale-out experiment.

These names/versions are already accepted by `internal/state/runtime.go` and
`python/workers/registry.py` under those settings. K2 must prove each submitted
definition reaches terminal state through Compose. This resolves the R223
workload choice for DUR-053 only; deferred DUR-050's Go/Python allowlist gap
remains OPEN and its protocols/results are unchanged.

For a fair K5 CPU-HPA control, include a separate CPU-only arm using new
`dur053.cpu/v1`, immutable `PURE_ACTIVITY`, with deterministic SHA-256 work and
no external mutation. This needs a new entry in **both** Go and Python
allowlists, compatibility tests and a Compose submit-to-terminal test in K2.
It is required for the K5 CPU comparison, not part of the already-allowlisted
primary workload or the pre-K1 baseline comparison. K3 pilot chooses the
smallest of 10000, 100000, 1000000 work units whose median invocation is at
least 1 s; if none is at least 1 s, use 1000000 and disclose that limitation.
Freeze units and image before measured runs. Do not infer CPU-HPA fairness
from a sleep-only comparison. Voluntary claim release at drain abort is
excluded: existing fenced timeout recovery remains the policy.

## Open-loop arrivals, pilot, SLO and stopping

K2 implements an absolute monotonic arrival schedule with bounded concurrent
HTTP submission; a slow response must not delay later scheduled arrivals.
Existing `cmd/dur048-load` sleeps after a synchronous submission, so it is a
reference for graph shape only, not the open-loop generator or final oracle.
Retain scheduled time, actual send time, response, durable submission key and
workflow ID for every arrival, including rejected/uncertain submissions.
Never silently drop arrivals or replace rejection with accepted-only latency.

K3 pilot uses four total worker slots and no faults, separately for each
duration and CPU arm. Start at rate `1 / max(1, duration_seconds)` workflows/s
(CPU duration starts at 1 s); collect 200 completed pilot workflows after
warmup. Let `b` be mean total slot-busy seconds per pilot workflow, including
zero-sleep echo/add members. Define reference rate `r = 4 / max(b, 0.01)`.
This is a scheduling reference on one host, not a sustainable-capacity claim.
Run three additional 200-workflow pilot repeats at `0.5*r`. Freeze per-arm
SLO `L = ceil(1000 * 2 * max(repeat p99 scheduled-arrival-to-terminal seconds))
/ 1000` seconds; nearest-rank quantiles are defined below. Also freeze the
measured `b`, `r`, arrival concurrency cap, observed resource limits and hashes.
Pilot results are excluded from all K4-K7 comparisons. Pilot incompleteness,
generator saturation or safety failure blocks freezing; fix the cause and
retain the failed pilot before a versioned new freeze, never choose a nicer SLO
from measured fault outcomes.

For each K3 pilot run warm for 120 s at its registered initial rate or,
for the three repeats, `0.25*r`. Stop warmup arrivals and reconcile warmup
before collecting the 200 measured pilot arrivals. The pilot alone retains
the 200-arrival minimum to define `L`; measured completions must also be 200.

Lifecycle and fault episodes (K4, K6, K7) warm for 120 s, then intervene once
the target has confirmed in-flight work (or an acknowledged held boundary).
Warm at `0.5*r`; K7 may fill four pods at `0.75*r` during warmup, then uses
`0.5*r` from intervention onward. Do not drain warmup work before intervention:
those active invocations are the affected population. Start the measured
window at intervention and offer at `0.5*r`. For the long arm use
`W_long = max(180 s, 20/(0.5*r))`, i.e. 3 min or 20 arrivals, whichever is
longer. For every other regular arm retain `W = max(180 s, 60/(0.5*r))`.
These windows apply to K4/K6/K7 fault and scale-down episodes at their
corresponding duration; K5 follows its separately stated medium/CPU scenario
windows below. At the planning rate the long window is 360 s (about 20
arrivals). Other arms are unchanged.
Confirmation may extend warmup by at most `2*L`; an unmet condition is an
unexercised episode, retained and never counted as a passing fault replicate.
Apply the in-flight confirmation and warmup rule to K5's fault/scale-down
scenarios too; K5 measured windows follow its separate rules below. Warmup arrivals are
excluded from measured latency but included in submission reconciliation,
invariants and affected-work recovery/abandonment metrics. No intervention
changes scheduled arrivals; interventions are no longer at arrival 100.

K5 uses medium sleep and the separate CPU arm, not a long-arm factorial.
All policies get both workloads. The step-load scenario has three windows at
`0.25*r`, `1.25*r`, `0.25*r`, each `max(120 s, 4*duration_seconds)`; extend
the final window only if needed to reach 60 measured arrivals. The other
five scenarios and fixed sweeps use `W` at `0.5*r` after 120 s warmup. Fixed
1/2/4-pod sweeps have identical offered load, never tuned per replica. The
lag-zero/busy scenario keeps its name but uses 5 s medium activities (and
the CPU comparison), not 45 s work; confirm running claims with zero lag.
For steady no-fault episodes measure immediately after warmup without an
intervention, and reconcile all warmup and measured work at closeout.

SLO attainment requires at least 99% of accepted workflows to succeed within
frozen `L` from scheduled arrival and zero incomplete accepted work at
reconciliation. Publish offered, accepted, rejected and uncertain counts;
rejecting work is not success. Report p50/p90/max with the actual population
size. Report p99 (and p99 differences) only when each contributing population
has `n >= 200`; otherwise label it NOT ESTIMATED, never substitute max or pool
episodes to manufacture a tail sample. This rule also applies to time-sampled
age metrics. The K3 pilot keeps 200; about two top-1% observations still give
an imprecise descriptive tail estimate.

After arrivals stop and the fault is confirmed removed, reconciliation ends
as soon as **all accepted work (including warmup and uncertain submissions
found durably accepted) is terminal and the external invocation/effect witness
has been quiet for 30 s**. Any witness activity resets the quiet interval;
missing witness access fails completeness. The deadline
`max(600 s, 10*L, 4*duration_seconds)` after the final arrival or confirmed
fault removal, whichever is later, is a timeout only, never a fixed wait.
Timeouts fail the run with censored/unfinished rows retained. Continue watching
abandoned attempts through this reconciliation end or timeout, not a mandatory
600 s idle wait. Stop new injections on safety violations, connection-budget
breaches or uncontrolled resource growth; export evidence and bound cleanup.
No automatic rerun replaces a failure. Infrastructure-invalid episodes remain
in the registry and may get one linked replacement with a new ID if budget
allows; fault-induced failures are valid outcomes, not exclusions.

Non-scaling lifecycle runs start with two worker pods; worker scale-down and
K7 start at four and go to two. The reference rate remains the four-slot pilot
rate. Disable autoscalers for fixed-replica/K4/K6/K7 runs; only K5 policy
comparisons use autoscaling. Held hooks, network partitions, skew and relay
stalls are planned to be removed within 60 s after intervention; record actual
confirmation/removal and recovery. Restarts/rollouts must be confirmed complete,
not assumed complete after 60 s. Delays consume the shared time budget.

## Sample allocation and comparisons

An episode has a fresh database and engine namespace within the reused family
cluster, one intervention (none for controls), independent IDs and seed.
Freeze seeds `530001+i` and alternate baseline/treatment order per matched
pair. Shared-host trends remain a limitation; workflow rows are not independent
fault replicates. Episode counts below are minimums, not targets that can be
silently reduced to meet the budget. A budget-short family is incomplete.

| Milestone | Experimental cells | Minimum independent episodes per cell |
| --- | --- | --- |
| K4 core | 7 core events x short/long x baseline/treatment | 5 matched pairs (10 episodes) per event/duration |
| K4 coverage | 7 coverage events, short only, treatment only | 3 episodes per event |
| K5 policies | 6 scenarios x durable-demand KEDA / Kafka-lag KEDA / CPU HPA x medium/CPU | 3 episodes per scenario/policy/workload |
| K5 fixed | 1/2/4 pods x medium/CPU | 3 repeats per replica/workload |
| K6 deployed hooks | 6 hooks, short only | 5 episodes per hook |
| K6 chaos | 12 cells enumerated below, medium only | 5 episodes per cell |
| K6 effects | 4 cells enumerated below, medium only | 5 episodes per cell |
| K6 controls | No fault, medium | 5 episodes; 3 PURE and 2 grant-bound effect controls, reported separately |
| K7 if GO | 4-to-2 scale-down, long only, controller off/on | 10 matched pairs (20 episodes) |

K4 core events: worker rollout, worker scale-down 4 to 2, scheduler rollout,
API rollout, worker SIGKILL, scheduler SIGKILL, node drain/PDB eviction.
K4 coverage events: relay rollout, relay SIGKILL, API SIGKILL, and per-role
rollback of worker, scheduler, API and relay. The latter are within each
lifecycle arm's compatible image lineage; matched K4 core baseline/treatment
uses the same digest and only the campaign legacy flags differ (see below).

K5 scenarios: step load, lag-zero busy activity, partition skew, relay stall
with healthy broker, broker down, and scale-down with in-flight work.
All three policies run the CPU arm for a matched CPU-HPA comparison.

K6 deployed-process hooks: `before_publish`, `after_broker_ack`,
`attempt_claimed`, `before_offset_ack`, `result_recorded`, `lease_takeover`.
Each episode requires durable boundary acknowledgment before injection.
The archive's remaining distinct hooks `submission_committed`, `outbox_insert`,
`attempt_replaced`, `timer_consumed`, `attempt_timeout`, `approval_grant`,
`effect_applied`, `effect_unknown`, `cancel_completion` stay in-process evidence:
NOT COVERED as deployed hook cells. The archive has 16 cases and 15 distinct
boundary names (two cases use `approval_grant`). K6 effect cells below test
sink boundaries with independent evidence; they do not claim execution of
these excluded archive hooks. No deployed non-cooperating sink or cancellation
route is added.

K6 chaos cells (12): worker pod kill; scheduler container kill; scheduler to
external PostgreSQL partition with lease-row lock held; the same partition
without a held lock; worker to API partition; relay stall with healthy broker;
external PostgreSQL restart; external Kafka restart; scheduler TimeChaos +30 s
and -30 s; worker TimeChaos +30 s and -30 s. The network direction to external
PostgreSQL is `to`; worker-to-API partition is also `to`. K4 covers other
role SIGKILL and node drain; do not multiply these K6 cells by every role.
K6 effect cells (4): kill before request; kill after sink commit/before
response; duplicate request; lost response with same-key lookup. Grant
negative cases remain required validation in the K6 contract below.
For effect cells and effect controls, replace the eight sleep positions in
each ten-workflow block with one grant-bound cooperating activity each, with
a 5 s worker delay before the sink call; keep echo/add positions unchanged.
This retains the planning 80/10/10 busy-time mix; grant/sink overhead must be
measured and included in the K3/K6 schedule recheck, never assumed zero.

Report raw counts, episode distributions, Wilson 95% proportions and paired
differences descriptively. Zero observed failures in `n` independent episodes
has exact one-sided 95% upper binomial bound `1 - 0.05**(1/n)`: 63.2% for 3,
45.1% for 5, and 25.9% for 10. Do not treat the two arms of a matched pair as
independent trials or pool unlike cells for a tighter bound. Ten K7 pairs
permit a descriptive paired comparison, not rare-failure certification or a
production power calculation. No significance or universal safety claim follows
from zero observed violations, or from unperformed cells.

## K3-K7 schedule and hard 72 h stop

This is a premeasurement planning estimate, not a result. Let `D` be nominal
activity duration. Planning assumes `b=0.8*D` for the 80/10/10 sleep mix and
`b=D=1 s` for the CPU arm (100% CPU activity). Thus planning `r=4/b` is
50, 1, 1/9 and 4 workflows/s for short, medium, long and CPU. Echo/add and
instrumentation overhead are not measured yet. K3 must recompute the table
using measured `b`, CPU duration and `r` before freezing; a slower CPU candidate
or witness overhead is not silently ignored. Keep the initial CPU pilot rate
at 1/s as registered above even if its chosen duration is longer.

Let expected post-window reconciliation `Q=D+30 s` (last invocation plus
witness quiet), not the timeout. This assumes fault recovery finishes during
the measured window; extra recovery, intervention confirmation waits, pilot
warmup reconciliation and timeouts consume the allowance or remaining budget.
For short/medium/CPU regular episodes `E=120+max(180,60/(0.5*r))+Q`, giving
330.1 s, 335 s and 331 s respectively. For long regular episodes use
`E_long=120+max(180,20/(0.5*r))+Q=555 s`. For K5 step episodes let
`V=max(120,4*D)` and `S=120+3*V+max(0,60-1.75*r*V)/(0.25*r)+Q`:
three full windows, then only any extra arrivals needed to reach 60 in the
last low-rate window. Here both workloads
already exceed 60, so `S=120+360+Q`: medium 515 s, CPU 511 s.

For each pilot arm, `u=1/max(1,D)` for sleep and `u=1` for CPU. One initial
200-workflow run plus three 200-workflow repeats costs
`P=(120+200/u+Q)+3*(120+200/(0.5*r)+Q)`. Each is a separate episode; additional
warmup reconciliation beyond these planning durations uses the allowance.
Each table row represents the stated number of identical cells; per-cell
time and row arithmetic are explicit. All times below are seconds.

| Family / cell grouping | Cells | Episodes/cell | Expected s/cell | Row total s |
| --- | ---: | ---: | ---: | ---: |
| K3 short pilot | 1 | 4 | P = 824.4 | 824.4 |
| K3 medium pilot | 1 | 4 | P = 2820 | 2820 |
| K3 long pilot | 1 | 4 | P = 20580 | 20580 |
| K3 CPU pilot | 1 | 4 | P = 1104 | 1104 |
| K4 core short event | 7 | 10 | 10*330.1 = 3301 | 23107 |
| K4 core long event | 7 | 10 | 10*555 = 5550 | 38850 |
| K4 coverage short event | 7 | 3 | 3*330.1 = 990.3 | 6932.1 |
| K6 short deployed hook | 6 | 5 | 5*330.1 = 1650.5 | 9903 |
| K6 medium chaos cell | 12 | 5 | 5*335 = 1675 | 20100 |
| K6 medium effect cell | 4 | 5 | 5*335 = 1675 | 6700 |
| K6 medium no-fault controls | 1 | 5 | 5*335 = 1675 | 1675 |
| K5 step medium policy | 3 | 3 | 3*515 = 1545 | 4635 |
| K5 step CPU policy | 3 | 3 | 3*511 = 1533 | 4599 |
| K5 other medium scenario/policy | 5*3 = 15 | 3 | 3*335 = 1005 | 15075 |
| K5 other CPU scenario/policy | 5*3 = 15 | 3 | 3*331 = 993 | 14895 |
| K5 fixed medium replica point | 3 | 3 | 3*335 = 1005 | 3015 |
| K5 fixed CPU replica point | 3 | 3 | 3*331 = 993 | 2979 |
| K7 long scale-down, if GO | 1 | 20 | 20*555 = 11100 | 11100 |
| Episode subtotal (including K7 GO) | | 438 total | | 188893.5 |
| Shared setup/reset/export/validation allowance | | | | 29490 |
| Scheduled total | | | | 218383.5 |

Arithmetic by execution order: K3 25328.4 s; K4 `23107 + 38850 + 6932.1 =
68889.1 s`; K6 `9903 + 20100 + 6700 + 1675 = 38378 s`; K5 `4635 + 4599 +
15075 + 14895 + 3015 + 2979 = 45198 s`; K7 11100 s. Sum `25328.4 + 68889.1
+ 38378 + 45198 + 11100 = 188893.5 s`. Add the allowance `5*300 + 438*60 +
1710 = 1500 + 26280 + 1710 = 29490 s` for five family cluster setups,
reset/export per episode, package/smoke checks, scaler/negative controls, K7
crash/stale-cost tests, extra warmup reconciliation and other delay. Total
`188893.5 + 29490 = 218383.5 s`; divide by 3600 for **60.6621 h (about
60.7 h)**, below the 72 h cap. These are explicit planning allowances, not
measured setup or recovery promises. On NO-GO subtract 11100 s; do not spend
the saved time on new cells.

Hard cap: total live cluster time for K3-K7 is at most 72 h cumulatively,
including pilots, warmup, confirmation waits, setup, reconciliation, export,
invalid/replacement runs and K7 validation. Count one host wall-clock second
once, including simultaneous family clusters (normally only one is live).
Suspend the clock only when all campaign clusters are destroyed; retain a
durable cumulative ledger across family teardown/recreation and offline review.
Before each episode, recompute remaining schedule with the K3 freeze and
actual elapsed overhead; do not start if its registered window plus timeout
and bounded export/teardown reserve cannot fit remaining time. Reserve at least
120 s for export/teardown and stop new arrivals/injections early enough to use
it. If the full remaining schedule exceeds the cap, stop and report completed,
failed, invalid and **unperformed** cells with IDs/reasons; do not trim minima,
accelerate arrivals or add hours. At the cap terminate campaign resources,
retain censored records, fail incomplete reconciliation and publish the
budget-limited outcome. Offline analysis may continue; no blanket K3-K7
completion claim is allowed. Safety/resource stops take precedence.

## Evidence and metric definitions

Use identity `(namespace, workflow_id, node_id, iteration)` for logical node
work and add `(attempt_number, claim_token_hash)` for an attempt. No raw token
or credential is published. At activity entry, before workload logic, an independent
append-only witness outside the engine database durably acknowledges a unique
invocation ID with identity, worker pod UID, start timestamp and duration arm;
append finish/abort records when observable. If witness persistence fails,
abort the run, never infer that an unobserved invocation did not occur. This
witness is campaign instrumentation; record its overhead in both matched arms.
Effect call attempts and applications come from the independently owned sink.
Invocation starts measure instrumented activity entry, including a crash after
the start acknowledgment and before useful payload work. They do not imply a
completed computation or effect; finish/application records measure those.

Persist times at source: host/campaign monotonic times for elapsed intervals,
PostgreSQL commit evidence for ordering, Kafka assignment callbacks for
membership, and pod UID for identity. Export cross-clock uncertainty and
sampling interval (1 s for demand/pods/DB observers). In TimeChaos arms use
unskewed observer time for durations; target wall clocks only confirm skew.
Do not subtract scheduler/worker wall clocks to produce recovery intervals.
Nearest-rank `p(q)` sorts all `n` finite observations and selects rank
`ceil(q*n)`; report missing/censored counts separately, with SLO failures for
timeouts. Unobservable required metrics fail completeness, not become zero.

| Metric | Exact population, calculation and source |
| --- | --- |
| Duplicate executions | Sum `max(0, invocation_start_count-1)` per logical node iteration from the external witness, across attempts and redeliveries; distinct invocation IDs, not receipt retries. Allowed; report executed-node denominator and canceled stratum. |
| Stale results committed | Count distinct accepted state-changing result receipts whose attempt/token had been superseded or canceled before their commit, reconstructed from persisted replacement/cancellation/receipt ordering. Must be 0; stale rejections and late evidence-only rows are separate counts. |
| Accepted results per executed non-canceled node iteration | Distinct winning final successful result receipts divided by distinct externally witnessed node iterations with no applied cancellation. For this expected-success workload, each such completed iteration must have exactly one winner; reconcile each identity, not just the ratio. Report intermediate retryable/final-failure receipts separately and fail unexpected unsuccessful outcomes; canceled and unexecuted nodes have separate denominators. HTTP retries returning one receipt do not add winners. |
| Duplicate effects | Sum `max(0, applied_resource_mutations-1)` per stable logical effect key in the independent cooperating ledger; same-key receipts and call-attempt rows are not new applications. Must be 0. In PURE runs report NOT COVERED, never inferred zero. |
| Abandoned attempts | Attempts with witnessed starts active at intervention that exit/abort without an accepted terminal receipt before claim replacement/expiry recovery. Count per termination/scale-down and per active-at-intervention attempt; continue observation until reconciliation ends or times out. Separate cancellations and unknown outcomes. |
| Re-executed activity-seconds | Sum elapsed slot-busy seconds on invocations after the first start for each logical node iteration. Also report wasted original busy seconds on abandoned invocations; unfinished intervals carry observed lower bound and last-confirmed-alive-to-exit upper bound. Never substitute configured sleep for measured time. |
| Partition re-acquisition time | For a partition held at scheduler termination: competing owner's first committed acquisition time minus confirmed old-owner release commit time. If unreleased, use intervention time and label TTL-expiry recovery separately. Persist acquisition ledger (opt-in, bounded campaign) and old epoch; an observed NULL owner is insufficient. Also report first useful fenced transition and terminal progress. |
| Superseded-owner transitions | Committed owner-authorized transitions with an epoch older than a prior committed takeover for that partition. Independent history/lease reconstruction; must be 0. |
| Assignment stall | Per membership change, elapsed unskewed observer time from first revoke/rebalance-start callback until all surviving consumers have a stable assignment and a runnable sentinel delivery is durably disposed by the group. Report assignment-only and useful-disposition components, trigger UID, consumers and longest active invocation; an idle group needs the sentinel to make progress observable. |
| Drain | SIGTERM-to-process-exit duration; accepted results committed after SIGTERM/before exit; readiness-to-endpoint-removal delay; per-role control HTTP errors/retries and client submission errors. Use same request identities across retries. |
| Recovery | Intervention-to-replacement-attempt creation, intervention-to-first-useful-transition and intervention-to-terminal for affected workflows; right-censor at the registered deadline. Report stale rejections by typed code. |
| Latency/throughput | Scheduled-arrival-to-durable-terminal p50/p90/max for accepted work, and p99 only for n >= 200, successful completions per measurement second, SLO success fraction, and separate offered/accepted/rejected rates. Show fault/rollout and no-fault windows plus p99 difference only where both windows have n >= 200; polling observation error accompanies terminal timestamps if commit time is unavailable. |
| Scale-up delay | Demand threshold first exceeded at a fresh sample to first additional pod ready AND taking a durable claim; report desired/current/ready/claiming replica traces. |
| Oldest claimable age | At each DB sample, max age since authoritative attempt became claimable among distinct currently claimable attempt identities in the campaign namespace; empty fresh sample is 0, stale/error is missing. Report time-sampled p50/p90/max and p99 only for n >= 200, never max of per-partition last-writer gauges. |
| Busy pods removed | Removed pod UIDs with one or more current durable claims at deletion-start, and total interrupted claims; use per-pod claim join and witnessed invocations, not pod-name reuse. |
| Replica-seconds / events | Integral of existing worker replica count over monotonic measurement time (piecewise constant pod UID lifetime); also integral of ready replicas. Count desired-replica changes, increases and decreases separately; record KEDA fallback episodes. |
| Slot utilisation / limiting component | Sum witnessed busy-slot seconds / integral of existing slots over time. Identify limiting component only with CPU/memory, DB waits/pool usage, relay publication, assignment and demand traces; otherwise report unresolved. |

## Demand and scaling contract

K2 exports a bounded, timed, indexed namespace query: active claims plus
claimable attempts, distinct attempt identity via `EXISTS`, with sample time.
No outbox-row counting; redispatch cannot inflate the scalar. Failed samples
or samples older than 5 s return an error, never zero. Isolate the read-only
demand listener on port 8091 from submission/worker-control and operator routes.
Use KEDA `AverageValue` target 2 slots/pod, minimum 1, maximum `8/2=4` pods.
K3 freezes pinned-version fallback/behavior settings with a scripted test:
fallback 2 pods after 3 failed polls, polling 5 s, scale-up stabilization 0 s,
scale-down stabilization 60 s, maximum one replica change per 30 s. Confirm
the pinned version actually implements these settings before K5.

Diagnostics include pending outbox count/oldest age, publication state,
expired-but-unreplaced claims and scheduler backlog. They do not count toward
the scalar. Compare durable-demand KEDA against Kafka-lag KEDA and plain CPU
HPA (target 60% request CPU, same 1-4 bounds and behavior). Kafka lag misses
claimed work, unpublished outbox and timer waits; a zero lag reading is not
completion. CPU control is evaluated on both sleep and CPU work. Keep fixed
replica sweeps distinct from autoscaling results.

## Fault confirmation and checker completeness

| Fault | Required confirmation and removal evidence |
| --- | --- |
| Rollout/rollback/scale-down | Record image digest, pod UID, deletion timestamp, signal/process exit and replacement ready/claim evidence. Distinguish SIGTERM from SIGKILL. PDB scope is evictions only, not rollout or scale-down. |
| Node drain/PDB | Scripted eviction request, PDB allowed-disruption/denial events, affected UIDs, eventual drain and recovery. Merely rendering a PDB is insufficient. |
| Pod/container kill | Chaos backend acknowledgment plus pod UID deletion or container ID/restart count and observed process death; replacement progress independently confirmed. |
| Scheduler to external PostgreSQL partition | Campaign hook confirms held lease-row transaction, PID/backend and `pg_stat_activity`/lock-holder evidence; Chaos direction `to` external DB endpoint, packet/drop counters or failed connections, preserved scheduler PID, committed peer takeover, reconnect, and rejected stale write. Keep lock-held and no-lock variants separate. |
| Worker to API partition | Confirm target pod UID and process continuity, request failures plus packet/drop counters, continued activity witness, reconnection and typed stale rejection after replacement commits. |
| Relay stall, broker healthy | Hook acknowledgment at named relay boundary, no publication progress, successful independent broker metadata/produce probe and rising durable pending outbox. |
| PostgreSQL / Kafka restart | Docker container identity, backend/broker process disappearance and availability failure from an independent probe, restart identity and restored connectivity. Retained disk only; no database-volume-loss or broker-HA claim. |
| TimeChaos | Positive and negative 30 s skew on scheduler and worker separately; target process emits a clock sample compared with unskewed observer before/during/after. `kubectl exec` time does not confirm TimeChaos. Static-Go compatibility must be demonstrated in process; unsupported injection is NOT EXERCISED, never a passing skew arm. |
| Named boundary / effect uncertainty | Hook ID, invocation ID, durable acknowledgment before injecting; for lost response prove sink commit and absence of delivered response; for duplicate delivery prove stable message identity and multiple observed calls. A configured hook without acknowledgment does not count. |

Every run requires a consistent engine snapshot including all campaign
submissions, definitions, workflow/node iterations, attempts/claims, leases,
history, receipts, timers, outbox and dispositions, plus external invocation
witness and, for effects, sink call/application ledger. Export counts and
hashes before any deletion. R228 fix must page all history or fail closed on
overflow; a 1001-row fixture must never pass on the first 1000. Independently
reconcile loadgen keys (including uncertain responses) with durable accepted
submissions and terminal rows; missing/extra/duplicate accepted workflows or
missing terminal outcomes fail. A consistent snapshot alone does not prove
submission coverage. Missing witness rows, exports, required metric fields,
fault confirmations or snapshot-count reconciliation fail completeness.

Run the independent invariant checker without calling production transition
validators or copying their verdict logic. Test negative controls for truncated
history, missing terminal work, stale epoch/claim acceptance and effect double
application. Publish `invariants_covered` and explicit `invariants_not_covered`
per arm: PURE runs do not cover I8-I10 effects/approval; no cancellation drill
does not cover cancellation races; restart-only arms do not cover storage
loss. Record all I1-I13 outcomes as covered/pass, covered/fail or not-covered.
A complete passing subset never implies coverage of all invariants.

## K7 controller go/no-go, decided before measurement

Use K4 treatment-only scale-down episodes, not pre-K1 baseline, to decide.
GO only if: all required safety/completeness checks pass; in at least one
short or long duration arm at least 2 of its 5 treatment episodes remove a busy pod while another
eligible idle pod survives; and that arm's median wasted-original plus
re-executed activity-seconds per scale-down exceeds `max(5 s, 0.10 * total
busy seconds on removed pods)`. Apply the comparison episode-wise before
taking the median of the excess. Freeze the decision table at K4 closeout,
before controller implementation. Otherwise record NO-GO/not justified and
perform no controller implementation or controller efficacy claim.

On GO, a controller reads fresh per-pod claims by UID and patches only coarse
`controller.kubernetes.io/pod-deletion-cost` (idle 0, busy 100), expires values
after 10 s, and clears stale annotations after restart. It cannot change
replicas, claims or leases; KEDA owns replicas. Test controller crash/stale
annotations on real kind, since envtest cannot prove victim selection.
Use all 5 treatment episodes per K4 duration arm, including zero excess;
require median episode-wise excess strictly greater than 0. A missing or
incomplete 5-pair cell cannot yield GO. K7 runs 10 matched off/on scale-down
pairs at the long arm only, even if the short arm triggered GO.
Report paired abandonment and wasted/re-executed seconds, including zero and
negative improvements. The outcome may be no benefit; no post-hoc threshold
or workload tuning to justify the controller.

## PostgreSQL connection budget

K3 configures explicit per-process pool maxima; no implicit pgx defaults.
Engine PostgreSQL `max_connections=100`; 3 reserved plus 5 administrative
connections are unavailable to application budgets. All campaign Jobs have
parallelism 1; observers share bounded pools. Sum worst-case surge, not just
steady replicas. New roles must fit this table before deployment.

| Engine DB client | Steady + surge processes | Pool per process | Maximum |
| --- | --- | --- | --- |
| API | 2 + 1 | 6 | 18 |
| scheduler/ingestor | 3 + 1 | 6 shared | 24 |
| relay | 1 + 1 | 3 + 1 dedicated LISTEN | 8 |
| demand listener | 1 + 1 | 2 | 4 |
| migrate, loadgen, observer, checker, campaign hook/approver Jobs | 1 each, no surge | 2 each | 10 |
| PostgreSQL monitoring exporter | 1 | 2 | 2 |
| K6 sink grant validator (engine grant read only) | 1 + 1 | 2 | 4 |
| workers, KEDA, Prometheus/Grafana, chaos, drain-cost controller | any bounded replicas | 0 (HTTP/metrics only) | 0 |
| Total clients | | | 70 |
| Reserved + administration | | | 8 |
| Unallocated headroom | | | 22 |

This deliberately budgets simultaneous rollouts and Jobs even when migration
is normally serialized before workload. Bound connection acquisition and
query time; observer checks peak `pg_stat_activity` by application name. A
role split must not accidentally duplicate pools or omit relay LISTEN cleanup.
K6 sink uses a separately owned PostgreSQL instance/database, `max_connections=30`:
two sink processes including surge x 4 = 8; migrate 2, checker 2, exporter 1,
administration 3, reserved 3 = 19, leaving 11 headroom. No cross-database
transaction or foreign key is introduced. Invocation witness uses durable
host storage, no unbudgeted engine DB connection.

## K6 cooperating-effect contract decision: grant required

Retain the existing bounded approval grant. `docs/CONTRACTS.md` identity and
attempt rules and `docs/PROTOCOLS.md` effect protocol require the sandbox to
validate approved target, canonical arguments, logical effect key, expected
resource revision, request ID and resource-local fence token. A no-grant sink
would bypass I10 and require an unauthorized contract change. K6 adds a
versioned `dur053.cooperating/v1` COOPERATING_EFFECT on the Kafka path with
both registries and mixed-version tests; it is not smuggled into K0/K2 PURE
allowlists. K4 uses PURE work only; it does not compare K6 effects in legacy mode.

A private campaign harness outside application pods uses the existing
scripted approver and lease-owning engine path to record exact decisions and
apply grants. It does not insert fabricated grants or serve operator write
routes in-cluster. Before K6 implementation, document its authenticated,
restricted identity and grant-read interface; neither workers nor untrusted
tool output can authorize a grant. The sandbox Deployment owns only its
separate effect database and has read-only access to the exact engine grant
records it validates. Bound grant lifetime over the reconciliation deadline;
an expired/mismatched grant rejects, never silently renews during recovery.

The sink validates the grant before its own transaction, recomputes canonical
argument hash, locks resource fence/revision, and atomically stores protected
state and one stable-key receipt. Call attempts are separate rows. Marking
the engine grant DISPATCHED remains a separate best-effort update; no shared
transaction is claimed. Same key/same arguments returns the receipt;
changed arguments/resource/scope conflicts. Replacement attempts reuse the
effect key and grant scope; uncertain outcomes use receipt lookup and existing
reconciliation rules. Scripted negative cases cover missing/expired/mismatched
grant, fence regression and response loss. Duplicate effect counts come from
actual independent sink applications, not engine receipt counts.

## Milestone gates, commands and evidence layout

K0: `git diff --check`; `python scripts/check-public-links.py`. K1/K2 run
existing race, lint/type, Python, mutation and Compose checks; PLAN records
their exact commands. K3 must wait for Claude VERIFIED R218-R221 and R225.
K2 checker/identity/clock/compatibility prerequisites are accepted before
measured campaigns. Each milestone needs exact-commit Claude review and
green hosted CI before acceptance. No K0 test/CI/deployment implementation.
K1 must preserve the exact pre-K1 shutdown behind explicit campaign-only
`RUNTIME_SHUTDOWN_MODE=legacy` and `WORKER_DRAIN_MODE=legacy` flags. In
Kubernetes, absent flags select the new lifecycle behavior. Legacy mode must
reproduce pre-K1 behavior exactly, including no worker result after SIGTERM
and scheduler leases left to TTL expiry. K1 tests compare those observable
behaviors with the pre-K1 reference, including its hook and shutdown paths;
these flags are not general production/Compose defaults or security bypasses.

K4 baseline and treatment use the **same image digest**, including identical
K2 authentication, route restrictions and witness instrumentation. Baseline
sets the two legacy flags; treatment uses the new lifecycle defaults. Use
identical role topology in both matched core arms, with legacy flags changing
only shutdown behavior. Record digest, resolved
flags and role configuration in both manifests. No backported historical
baseline image is built or deployed. K1 legacy parity tests are a prerequisite,
not a claim that such tests ran at records-only K0.

The following is the **reserved future CLI contract**, to be implemented and
CI-tested in K2/K3; these commands have not been run and do not exist at K0:

```powershell
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario package-check
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario smoke
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario pilot
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario lifecycle
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario scaling
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario faults
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario controller
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario observability
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario verify-evidence
pwsh -File scripts/dur053-kubernetes.ps1 -Context kind -Scenario cleanup
```

Each scenario reserves a new campaign ID and reads this protocol and the K3
freeze. Durable published evidence goes under
`experiments/portfolio/k8s-recovery/<campaign-id>/`: manifest (commit/digests,
tools, versions, topology, dependency addresses, limits, connection budget,
seed, exact invocation), freeze/pilot reference, arrivals, invocation witness,
fault confirmations, pod/claim/assignment/metric traces, snapshot counts and
hashes, independent checker output, submission reconciliation, coverage list,
per-episode metrics, analysis and cleanup. K3 freeze lives under
`experiments/portfolio/k8s-recovery/k3-pilot-v1/`; K4/K5/K6/K7/K8 prefixes
identify separate families. Failed/invalid/aborted runs are retained in the
registry with reasons. Protocol amendments are versioned and reviewed before
affected measurements, without erasing prior registrations or outcomes.

Scratch scripts, caches, raw temporary logs and test directories created by
Codex stay under `D:\coding agent files\durable-engine\codex\`; only intended
durable evidence is written to the tracked evidence tree. Cleanup scripts
remove the dedicated kind cluster, campaign dependency containers/networks and
volumes after verified export; record retained evidence and resource inventory.
README/claim map updates wait for verified results. Claude alone updates the
three external career files from verified milestone evidence.


## Amendment B (registered before any pilot or measurement, 2026-11-04)

Authority: D035 and the user campaign-host decision recorded in REVIEW.md
Round 161 addendum, reaffirmed by the K3a task. This append-only amendment
supersedes only the host, cluster reuse, job partition, evidence-transfer and
connection-allocation details below. Amendment A's workload, episode minima,
family order K3 -> K4 -> K6 -> K5 -> K7, SLO derivation, fault confirmation,
checker independence, stopping rules and 72 h hard limit remain in force.
K3a implements packaging and smoke only. All pilots, calibration and SLO freeze
are K3b; no pilot execution or result is authorized by a K3a smoke verdict.

### Host, tools and limitations

The campaign host is single-host kind on a GitHub-hosted ubuntu-24.04 runner.
This replaces Docker Desktop/WSL2 because the approved host cannot execute the
required downloaded tools. Each job records ImageOS and ImageVersion, observed
vCPU count, /proc/meminfo, kernel, Docker/Python versions and exact application
image digests. GitHub's hosted image label is mutable: the observed image
version is evidence, not a claim that GitHub lets this workflow pin an immutable
runner VM. Source, node, tools and add-ons are pinned independently.

[Tool lock](../../../deploy/k8s/tools.lock.json) pins kind 0.27.0,
kubectl 1.32.2, Kustomize 5.6.0, kubeconform 0.6.7, KEDA 2.18.0,
Calico 3.29.3, Chaos Mesh 2.7.2, node/dependency images, release archive
checksums, add-on manifest checksums and image digests. The driver verifies
bytes before execution. Chaos Mesh is registered only; K3a does not install or
exercise it. Kubernetes and KEDA schemas are vendored and hash checked; strict
kubeconform validation never skips an unknown CRD. Application images are built
from the candidate tree in CI, pushed to an ephemeral loopback registry, loaded
into kind's containerd by the manifest digest and deployed by that digest with
imagePullPolicy Never. The package-check scenario binds the same built digests
into both Kustomize overlays in scratch; unbound source image markers cannot
pass policy or be deployed by the driver. This avoids inventing a future image
digest before a build.

PostgreSQL and Kafka are separate Docker containers on the kind network,
outside Kubernetes. Selector-less Services and explicit Endpoints use inspected
container IPv4 addresses. All engine pools, fresh database isolation and Kafka
consumer/group reset rules still apply. Calico replaces kind's default CNI;
merely rendering a NetworkPolicy is insufficient. Smoke must show an allowed
positive connection and failed worker-to-operator/demand connections, including
an ingress-only test with explicit temporary egress, followed by independent
HTTP 401 tests using the worker's real projected credential. Temporary probe
allows are deleted and the registered policy restored. No untrusted worker
receives the campaign bearer secret.

Shared runner scheduling, CPU frequency, image updates and co-tenancy add noise.
Every result remains single-host lifecycle/correctness evidence. No capacity,
independent-host availability/HA, EKS, storage-loss, or production-readiness
claim follows. Different jobs are different host allocations; never interpret
cross-job timing as a paired host-controlled capacity comparison.

### Job limit and recomputed schedule

Live time per job is capped at 18900 s (5 h 15 min), leaving 45 min inside
GitHub's six-hour limit for image build, export/upload and bounded teardown.
The campaign workflow has a 350 min overall timeout. K3a smoke instead reserves
3600 s live, with a 70 min job timeout. The live deadline includes setup and
cleanup, with at least 600 s left for bounded closeout/teardown. A job unable
to fit its registered windows, reconciliation timeout and this reserve refuses
to start. Pilot reservations use their registered initial rate and nominal
duration, then
the observed b/r when available. Before L exists, use the existing 600 s
reconciliation floor as the pilot closeout timeout (all D <= 45 s, so 4D <=
180 s); a timeout still blocks freezing. This makes the previously unspecified
pre-SLO deadline finite without changing measured-run timeouts. After the SLO
freeze, K3b recomputes measured-family reservations from frozen b/r/L before
measurement admission. A duration estimate is never a timeout exemption.

Every live job gets a fresh kind cluster. This supersedes one cluster per
family when a family spans jobs, because families exceed the host job limit.
Episodes within a job still use a fresh engine database/namespace and reset
Kafka group/data. Preserve matched pairs inside a job and alternate their
order as already registered. Split only at the listed episode/pair boundaries;
never split one pilot run or one fault episode. Persist the initial pilot's
measured b/r before a repeat job and apply it unchanged; fresh clusters/runners
between repeats add host variance and are disclosed. A longer measured pilot
that cannot fit one job is unperformed; do not lower its 200-completion minimum
or accelerate the initial arrival rate to squeeze it in.

The long pilot is 20580 s before reset/setup and cannot fit one job. Its
initial run is 9195 s; its three repeats total 11385 s. The twelve non-long
pilot runs get the first job, the long initial run a second job, and its three
repeats a third job. This also leaves room for their
full 600 s pre-SLO closeout deadlines at the planning rates. [Reproducible schedule](amendment-b-schedule.json) is generated by
`scripts/dur053_schedule.py`: next-fit packing in the registered family order,
300 s setup per job, 60 s per episode reset/export, and at least 600 s unused
at the ceiling when packing expected durations. Those 600 s are slack within
the ceiling, not an additional promise about reconciliation. All values here
are planning estimates, not observed live times.

| Family | Jobs | Expected seconds per job (includes setup and episode reset) |
| --- | ---: | --- |
| K3 | 3 | 5768.4, 9555.0, 11865.0 |
| K4 | 5 | 18244.6, 18272.4, 17520.0, 17910.1, 8102.0 |
| K6 | 3 | 17928.0, 18075.0, 10175.0 |
| K5 | 3 | 18119.0, 18035.0, 17504.0 |
| K7 | 1 | 12600.0 |

Amendment A's 188893.5 s episode subtotal and 438 episodes are unchanged.
Fifteen setups replace five: 15*300 = 4500 s, an increase of 3000 s.
Reset/export is still 438*60 = 26280 s. Preserve the other 1710 s validation
allowance conservatively, and add one K3a smoke reservation of 3600 s:
188893.5 + 4500 + 26280 + 1710 + 3600 = **224983.5 s = 62.4954 h**.
Remaining planning margin is 34216.5 s = 9.5046 h below the unchanged 72 h cap.
Additional failed/smoke/replacement jobs spend this margin; it is not renewed
on a PR or family boundary. On K7 NO-GO, subtract its 11100 s episode time,
1200 s resets and one 300 s setup; no new cells are funded with that saving.

### Durable budget and evidence transfer

[Live ledger](live-time-ledger.json) is the cumulative repository record, with
limit_seconds=259200. Before a smoke/live job starts any cluster, tooling checks
registered live duration against remaining budget and rejects missing, duplicate,
negative or incomplete ledger entries. Jobs are serialized in the shared
`dur053-live-ledger` concurrency group. Before another smoke run, all preceding
non-skipped smoke job IDs returned by Actions history must have committed ledger
entries. API/history errors fail closed; rerun attempts are refused. K3b must
extend this history check to its new live job names before implementing them.
The dispatch stub currently starts no resources and refuses every measurement
scenario with K3b, even if budget is available.

Each entry carries run ID, numeric job ID, PR head SHA, checked-out merge-ref
SHA, merge-ref tree SHA, artifact manifest hash, PASS/FAIL status and charged
live seconds. Charge elapsed setup-through-verified-teardown time rounded up, with a conservative one-second minimum for a refused attempt that starts no resources.
If termination prevents a trustworthy end time or artifact, charge the entire
registered reservation; missing evidence can never refund time. Failures and
invalid runs are retained. The runner has contents:read and cannot commit or
reset its own ledger. Claude downloads with `gh run download <run-id>`, checks
artifact hashes/provenance and appends the ledger record before another live
job. The job refuses to proceed until that evidence is committed; this is a
fail-closed automation dependency under D035, not a human approval gate.

All scenarios take a new -OutputRoot outside the checkout, CI uses RUNNER_TEMP,
and existing directories are refused before any tool runs. Artifacts include a
PASS/FAIL manifest, versions, digests, recorded commands, checks, raw snapshot,
arrivals, external witness, coverage and cleanup; artifact hashes exclude the
manifest itself to avoid a self-hash cycle. Claude commits smoke artifacts
byte-for-byte under `k3-smoke-v1/<run-id>/`, recording numeric job ID, head SHA
and merge-ref tree identity. Failed evidence is retained, never automatically
rerun or overwritten. A source-only package result is not a smoke result.

### Role pools and same-image baseline

The new explicit split reserves pools for public API, operator and worker
control separately. This supersedes the old combined API allocation while
remaining within PostgreSQL max_connections=100. The budget checker uses the
rendered worst-case surge counts and the registered future clients, not only
steady-state counts. Both lifecycle arms have identical role/image topology;
only the two registered legacy flags change behavior. The namespace changes
only isolate the two runs and their bearer Secrets. They are sequential,
with treatment namespace/database removed before baseline starts.

| Client | Steady + surge | Pool | Maximum |
| --- | --- | ---: | ---: |
| Public API | 2 + 1 | 4 | 12 |
| Operator | 1 + 1 | 2 | 4 |
| Worker-control | 2 + 1 | 4 | 12 |
| Scheduler + ingestor | 2 + 1 | 6 shared | 18 |
| Relay | 1 + 1 | 3 + 1 LISTEN | 8 |
| Demand | 1 + 1 | 2 read-only | 4 |
| Migrate, compatibility, loadgen, checker | 1 each | 2 each | 8 |
| Future monitor / sink grant reader / hook | bounded | 2 / 4 / 2 total | 8 |
| Workers / KEDA / observer HTTP inventory | bounded | 0 | 0 |
| Total applications | | | 74 |
| Reserved + administration | | | 8 |
| Remaining headroom | | | 18 |

Compatibility runs as a required pre-rollout Job on the exact runtime image,
with registry exported by its exact candidate worker initContainer. No public
submission Deployment/client exists until migrate and compatibility Jobs pass.
K3a supports fresh-namespace installation only; in-place upgrade/rollback and
surviving-old-image admission are refused/unimplemented campaign scenarios,
not an inferred compatibility guarantee. Runtime drain worst-case ~32 s and
worker abort 30 s fit the unchanged 40 s termination grace. PDBs constrain
evictions only. Smoke temporarily prevents replica changes to retain witness sessions
while testing KEDA polling; it is no scaling comparison or SLO freeze. K3b/K5
must remove that smoke annotations and verify the registered fallback and HPA
behavior under their own frozen campaign settings before measurement.


## Amendment B.1 - Engineering validation and registered campaign accounting (2026-11-05)

Authority: D035 and the explicit Round 165 R240 correction. This append-only
amendment supersedes Amendment B's charging and admission rules for engineering
CI smoke. The 72 h cap bounds the registered campaign; iterative engineering
smoke produces no reported measurement numbers and must not consume the margin
reserved for registered experiment cells.

A `ci.yml` pull_request smoke is engineering validation: no campaign charge,
no ledger or prior-run history admission check, and no measurement claim.
Current-job provenance may be read from the public Actions endpoint; it is not
a history gate and requires no Actions read permission. Package and smoke CI
jobs have contents:read only. Success, failure, cancellation and subsequent PR
pushes do not create ledger debt. These engineering artifacts retain their
observed setup-through-teardown duration for diagnosis and possible selection;
`charged_seconds=0` and `measurement_claim=false` describe their CI status.
The bounded 3600 s smoke execution ceiling and teardown verification still apply.

Only `dur053-campaign.yml` workflow_dispatch jobs and one registered K3
acceptance smoke are charged and gated by the cumulative ledger. The acceptance
smoke is the successful run whose byte-identical artifact Claude commits as K3
evidence. Intake permits one such selection, requires successful cleanup, and
charges its actual rounded-up setup-through-verified-teardown duration (at most
the existing 3600 s reservation). The ledger identifies this entry as
`k3-acceptance-smoke`; it does not rewrite the engineering artifact's zero charge.
Selection is a correctness acceptance record, not a pilot, SLO freeze, capacity
measurement or reported performance result. No other engineering run is selected
or charged, and failure run 41993310305 remains an engineering failure.

Campaign jobs remain serialized in `dur053-live-ledger` with
cancel-in-progress:false. Before dispatch can start live resources, committed
artifact/ledger validation and complete prior campaign-job history are required;
a missing prior entry or unavailable history fails closed. History includes
both the original `campaign` job name and `DUR-053 campaign`, excludes CI smoke,
and refuses automatic rerun attempts. Campaign failures and replacements retain
Amendment B's actual-time charge or full registered reservation when termination
prevents trustworthy accounting. The current K3b stub still refuses all live
measurement scenarios after the ledger gate. Neither the 259200 s cap, the
18900 s campaign-job ceiling, the episode families/minima, nor the reserved
3600 s acceptance smoke changes. No campaign or measurement ran for this fix.


## Amendment B.2 - R245 full pilot reservations and dependencies (2026-11-05)

Authority: D035, REVIEW.md Round 172/R245 items 1-6, with item 1's dependency
sentence replaced by the Round 173 ruling. This is append-only and registered
before any calibration or pilot. Earlier amendment text and amendment-b-schedule.json
remain byte-identical. This text supersedes their conflicting K3 job partition
and reservation statements only. All values below are planning, not measurements.

1. **Jobs and dependencies.** Jobs are `k3-pilot-1a` (short initial + 3 repeats,
   then medium initial + 3 repeats), `k3-pilot-1b` (CPU calibration, then CPU
   initial + 3 repeats), `k3-pilot-2` (long initial), `k3-pilot-3` (three long
   repeats). Ledger categories and dispatch inputs use these names.
   Before same-job repeats, persist the initial run's complete arrivals, witness,
   snapshots, checker verdicts and derived b/r in the job artifact and record
   their byte hashes. Before every repeat re-verify those hashes and every
   initial-run acceptance condition, applying b/r unchanged. Such same-job
   repeats do not require an intervening repository commit. Cross-job repeats
   (k3-pilot-3 and cross-job repeats in any -cN carry-over job) require the
   accepted initial artifact and b/r in the committed repository, verified by
   hash before any live resource. No repeat starts after a failed, timed-out,
   saturated, incomplete or unsafe initial. Freeze independently verifies
   initials, repeats and calibration from committed artifacts only. Persist and
   seal the same-job CPU calibration record in 1b's artifact before CPU initial;
   freeze verifies it. The campaign job cannot commit its own evidence.
2. **Admission.** Each pilot run reserves 120 s warmup + measured window +
   600 s after warmup + 600 s after measured arrivals + 60 s reset/export.
   Each job adds 300 s setup and 600 s reserve. Use planning rates without
   committed b/r and committed b/r otherwise. Recompute before every run using
   actual elapsed time and the persisted accepted b/r when it has become
   available in-job. At planning rates the original twelve-run job needs
   20384 s before calibration and is refused. Jobs 1a/1b/2/3 need respectively
   14364/14840/11280/15840 s. No job starts unless its registered full windows,
   both reconciliation deadlines, setup and reserve fit the 18900 s ceiling
   and remaining cumulative ledger budget.
3. **Carry-over.** A pilot run that per-run admission refuses only for lack of
   remaining job time (never a failed, timed-out, saturated, incomplete or unsafe
   run) moves unchanged, in registered order, to a follow-up job named
   `<job>-cN`, which gets a fresh cluster, the same gates and its own setup
   charge. A job that cannot start even its first run at the full reservation
   refuses and is reported unperformed. This is a mechanical rule from b/r and
   elapsed time, never from SLO or latency outcomes. Retain the predecessor's
   committed hashed pending suffix, never re-execute completed cells.
4. **CPU calibration.** Candidates 10000, 100000, 1000000, ascending. Per
   candidate: 1 discarded warm invocation, then 21 sequential measured
   invocations at concurrency 1 on the pinned image, no faults, each with a
   120 s timeout. Duration is witnessed monotonic slot-busy time, the same
   source as b. The measured median is the 11th of 21. Stop at the first
   candidate whose median is at least 1 s; otherwise select 1000000 and record
   that no candidate reached 1 s. Any timeout or failure blocks the CPU arm and
   is retained. Reserve the worst case 3 * 22 * 120 = 7920 s; 1b's full
   planning reservation is 14840 s. Persist the sealed calibration record before
   CPU initial starts and independently verify the underlying witness at freeze.
5. **Schedule.** `python scripts/dur053_schedule.py --amendment b2` reproduces
   amendment-b2-schedule.json: 16 jobs, 438 episodes, 225283.5 s expected.
   Calibration has no measured expected duration; full admission separately
   reserves 7920 s and charges actual calibration time against the ledger.
   No expected-time estimate authorizes live admission. The old schedule is
   retained unchanged.
6. **Unchanged.** Hard cumulative 72 h cap, live ceiling 18900 s/job, 350 min
   workflow timeout, 600 s floor, 438 episodes, 200 measured arrivals AND
   completions, all warmup/rate/L formulas, family order K3 -> K4 -> K6 -> K5 ->
   K7, engineering-smoke exclusion and full-reservation charging when termination
   prevents trustworthy accounting remain in force. Four fixed worker slots,
   no pilot faults; hold two worker replicas with ScaledObject paused at two
   and retain observed slot/resource/connection samples.

### Round 173 conservative interpretations used by tooling

Ambiguities are resolved toward more reservation, more refusal and more retained
evidence, never a weaker gate, minimum or formula. Stop only when no conservative
reading exists or it would make a registered cell impossible. Record further
interpretations here and in the handoff.

- Reserve the full 18900 s against the ledger for each dispatch, including a
  carry-over, even when its computed job-fit requirement is smaller. Trustworthy
  verified teardown permits the actual rounded-up charge; otherwise charge the
  full reservation. No-resource refusal has the existing one-second minimum.
- Count live setup from the build registry (the first owned resource), retaining
  build/setup overruns in actual elapsed admission. Command execution also keeps
  a 300 s diagnostics reserve in addition to the 600 s teardown reserve.
- Require successful hosted CI with byte-identical committed tooling and its
  pilot-smoke job before dispatch. Evidence/ledger-only commits may use that
  proof; changed tooling requires a new proof. PR engineering pilot-smoke uses
  the same execution path, short/CPU arms, 20 s warmup, 10 measured arrivals,
  one repeat, and reduced calibration (one discarded + three measured per
  candidate). It has budget_scope engineering-validation, charged_seconds 0,
  measurement_claim false and cannot be accepted by intake/dependencies/freeze.
- Include the 30 s quiet period and verification overhead inside each calibration
  invocation's 120 s bound; stop on an overrun. Preserve all invocation witnesses.
- Require all offered pilot arrivals accepted and all accepted workflows successful;
  retain rejection/uncertainty instead of selecting an accepted-only pilot.
  Report conservative first-observed terminal latency upper bounds, retaining
  DB timestamps, observed clocks and sampling gaps. Never infer a finish from
  configured sleep or from worker wall-clock subtraction.
- A committed failed or interrupted pilot job, except a content-defined B.3
  pre-start refusal, blocks this freeze version. No
  automatic replacement or selection among nicer runs exists. A correction
  requires retained failed evidence and a reviewed versioned new freeze.


### B.2 evidence and implementation clarifications (Round 173 conservative rule)

- Preserve warmup and measured arrivals as distinct absolute schedules. The
  checker additionally accepts their explicit union as a non-recursive windows
  array, verifies byte-equivalent concatenation and each window's schedule,
  then reconciles all durable submissions and witness rows. No arrival is
  re-timestamped to invent one continuous schedule. Measured pilot b/p99 use
  only the measured identities; warmup stays in completeness/safety checks.
- Observe fixed slots, resources, connections and terminal identities with a
  nominal 1 s polling interval, retain actual monotonic sample times and maximum
  gaps, and fail observer access or a changed/restarted worker session. Preserve
  the clock probe bracketing observer UTC with host UTC; terminal upper bounds
  include that uncertainty and are tied to the first retained terminal sample.
  Persisted database timestamps remain in snapshots. This is an explicitly
  bounded observation estimate, not an exact commit-clock claim or SLO tuning.
- Calibration's 120 s bound includes all its quiet/verification overhead. The
  measured closeout's 600 s floor is measured from the final scheduled arrival,
  so a slow final HTTP response never grants an additional timeout. Teardown
  commands share one deadline of at most 600 s and cannot overrun the live
  ceiling; inability to verify teardown blocks acceptance and consumes the
  full reservation. There is no silent retry or automatic dispatch.
- Assign pilot episode seeds 530001+i in the B.2 job order: short initial and
  repeats, medium initial and repeats, CPU initial and repeats, long initial
  and repeats. Warmup/measured submission keys also include disjoint window
  run IDs. Calibration has distinct deterministic invocation IDs/seeds and
  never contributes pilot workflows or fault-comparison samples.
- The offline freeze includes the schedule recheck using observed b/r/L and,
  conservatively, the larger of CPU calibration median and CPU initial mean
  busy duration. Calibration's actual time is ledger charged, with no invented
  expected calibration duration. This estimate grants no later-family admission;
  the per-job windows/timeouts, cumulative cap and unperformed-cell rules still
  apply, without reducing a cell, speeding initial arrivals or changing order.


B.2 implementation detail: observer polls target absolute one-second deadlines,
so RPC duration consumes the interval instead of adding a one-second sleep after
every poll. Actual sample times/gaps remain evidence. Every refusal or carry-over
manifest explicitly lists unstarted/unperformed cell identities and the reason;
a failed started cell retains its own failure record and is never relabelled as
an unstarted time-only carry-over. These retain evidence without changing any
registered workload, deadline, minimum, formula or budget.


B.2 conservative CI-proof clarification: a pull_request run can check out a
merge tree while the Actions run identifies the PR head. Therefore registered
admission accepts its byte-identical tooling proof only from successful CI on
an immutable main-branch push, including a successful pilot-smoke job. The same
uncharged engineering pilot-smoke also runs on main pushes, with the same
parameters and exclusions. PR smoke remains mandatory engineering validation.
Thus reviewed tooling must be merged and green on main before a registered
job can start; evidence/ledger-only commits may reuse that exact-tooling proof.
This adds validation/refusal, not a campaign run, paid charge, weaker gate,
changed minimum/formula or extended cap. No workflow is dispatched in this pass.


B.2 latency-source clarification (Round 174 / R246, 2026-11-05): the
scheduled-arrival-to-durable-terminal definition at protocol line 374 uses
`created_at` from exactly one SUCCEEDED `engine.transition_history` row at the
workflow's final persisted revision, read from the checker-verified committed
snapshot. This source applies to the pilot and every K4-K7 latency/SLO metric.
It supersedes B.2's first-observed upper-bound implementation detail above;
first-observed polling never determines L when durable timestamps exist.
Inside the observer pod, bracket `SELECT clock_timestamp()` with observer UTC
before and after the query, after connecting; persist probes at setup and
after measured live/offline reconciliation. Add the larger bracket width u to
every durable-minus-scheduled latency. Both ordered brackets must enclose the
DB timestamp and bracket the measured window; otherwise fail closed. Retained
first-observed samples independently require scheduled_at - u <= created_at <=
first_observed + u. Report max and nearest-rank p99 polling-minus-durable
differences and actual sampling gaps, without using them to compute L.
The history timestamp is inserted inside the transaction that subsequently
commits; it is not a separately measured commit instant. The observer, DB and
arrival generator share kind's host kernel clock; these probes do not establish
a cross-host clock-drift guarantee. Missing/duplicate final terminal rows,
missing probes or a failed clock/polling cross-check block this freeze version.
All previous protocol bytes, minima, workload/rate formulas, 72 h campaign cap,
job ceilings, ordering and existing evidence remain unchanged. No registered
pilot or K4-K7 measurement is claimed by this tooling correction.


B.2 conservative reconciliation-deadline clarification (Round 174): the
existing warmup/measured reconciliation deadlines include witness export,
live/offline checker verification and the final DB clock probe. A poll or
verification that returns after its deadline cannot authorize success.
The measured deadline remains anchored to the final scheduled arrival;
this grants no extra time or reduced completeness requirement. General
bounded setup/reset waits likewise reject late predicate success.


## Amendment B.3 - retained pre-start admission refusals (Round 180 authorization)

Claude authorized this mechanical recovery rule under D035 in Round 180.
Classification depends only on the immutable, hash-verified artifact content,
never the error label, Actions conclusion, or a manually supplied override.

1. A pre-start refusal has `status == "FAIL"`, exactly
   `cleanup == {"status": "PASS", "no_resources_started": true,
   "elapsed_live_seconds": 0}`, top-level `elapsed_live_seconds == 0`,
   `charged_seconds == 1`, and `completed == []`. Its `unperformed` covers
   exactly every `planned` cell, each with `started: false`. There is no run,
   calibration or seal directory. `commands.jsonl` contains only commands whose
   argv[0] is in the explicit allowlist `{git}`. Every other artifact is an
   ordinary FAIL, subject to all existing blocking rules.
2. Retain every refusal under `EVIDENCE/<job>/<run_id>/` with its one-second
   ledger charge. It counts in campaign history; intake rejects nothing new.
3. A pre-start refusal contributes no cell and does not block freeze v1. Freeze
   still requires its committed evidence directory and ledger entry, verifies
   all hashes and the content definition, includes its manifest hash among
   source artifacts, and lists each refusal by job name, run ID and job ID in
   `pre_start_refusals`. Only these artifacts skip the `ci_proof`,
   `versions.json` and `images.json` requirements. All other acceptance and
   completeness checks remain in force.
4. A same-name dispatch (including a `-cN` name) is permitted only if every
   previous artifact with that name is a pre-start refusal. The bound is three
   pre-start refusals per exact job name: after three have been retained, the
   next dispatch refuses with `pilot-pre-start-replay-limit` and stops. Any
   ordinary FAIL or interrupted artifact still forbids same-name replay.
   Carry-over predecessor and initial-dependency checks remain unchanged.
5. These rules change no cap, ceiling, reservation, minimum, formula or cell.
   No automatic dispatch, measurement replacement or outcome selection is
   authorized. The first refusal (42166833080, job 127725150943) must be recorded
   by Claude after this correction lands, before another dispatch.

### B.3 conservative interpretations (Round 173 standing instruction)

Round 180's validation wording could allow a fourth dispatch after three
refusals; its correction instead bounds refusals at three. Use the stricter
bound: replay after one or two refusals, refuse after three (and after four
if such evidence is encountered). Require nonempty unique valid planned cells,
matching unperformed cells in planned order, strict integer zero/one charges
and timings, a nonempty parseable command log, literal `git` as argv[0], and no
subdirectories at all (even empty/unknown ones) or calibration/seal files. Missing
or malformed evidence receives ordinary FAIL treatment; it is still retained.
There is no permissive path or executable-basename normalization.

Git-content admission applies Git's path attributes to checkout files and
compares their object IDs with HEAD, returning HEAD blob bytes. CI compares
HEAD and tested-run blobs and refuses untracked or status-reported tooling
changes. `.gitattributes` is included in that tooling inventory because it
controls normalization. Raw artifact hashes remain raw: committed pilot evidence
roots have `-text` attributes, including the engineering tooling evidence root.

## Amendment B.4 - archived immutable evidence (Round 184 authorization)

Claude authorized this storage-only amendment under D035 in Round 184/R257.
The downloaded artifact, manifest bytes, raw-file hash set, all measurement
rules and prior evidence remain immutable. New normal and interrupted intake
uses the following representation at `EVIDENCE/<job>/<run_id>/`:

1. Keep `manifest.json` raw and byte-identical. Keep `payload-index.json` raw,
   schema `dur053.payload.v1`, listing archive paths, SHA-256, compressed byte
   sizes and ordered member names. The ledger retains the original manifest
   path/hash and adds `payload_index_sha256` over the raw index bytes.
2. Store the other files in `payload/0000.tar.xz`, etc. Group root files
   together, and each top-level directory separately, in first-member lexical
   order; members within each group are sorted. Use Python stdlib USTAR tar
   and XZ/LZMA2 preset 6 with a 128 MiB dictionary. Every member is a regular
   file with mtime/uid/gid zero, empty uname/gname, and mode 0644. No links,
   sparse files, devices, absolute paths, backslashes, drive paths, dot or
   parent components are admitted. Archives are marked `binary` in Git.
3. The per-archive maximum is **45,000,000 bytes** (decimal MB). A larger
   group splits recursively at its sorted member-list midpoint, left before
   right, until every archive fits. A single member that cannot fit refuses
   intake; no file bytes may be dropped or transformed to meet the bound.
   The total compressed payload maximum is **90,000,000 bytes**. Compress and
   validate all limits in a temporary directory outside the repository before
   creating the evidence directory or changing the ledger. This bound is four
   times the measured 18,504,236-byte pilot payload, rounded up to a 45 MB
   increment. It is a refusal bound, never permission to omit evidence.
4. Every archive reader (manifest reader, initial dependencies, freeze and
   offline recheck) verifies the committed manifest/index/archives, the raw
   index hash against its committed ledger row, archive hashes and sizes,
   exact layout and member inventory, then materialises regular files into a
   fresh external temporary directory. Before data is consumed, the complete
   extracted set must equal `artifact_sha256` and every raw hash must match.
   Extra, duplicate, missing, unsafe or different content refuses. Readers
   remove temporary materialisations when their scoped operation ends.
   Offline recheck within freeze reads the already verified materialisation;
   direct committed recheck supplies the repository for ledger verification.
5. Already committed raw artifacts, including the 1 s pre-start refusal and
   k3-smoke-v1, retain their original layout and validation. Exactly one layout
   is accepted: an index or payload directory selects archived layout, which
   allows only manifest, index and indexed archives. Raw-plus-archived content
   refuses. Raw readers also copy verified files to fresh external temporary
   directories before consumption. New intake always archives, including
   interrupted partial bytes. Refuse empty source directories or non-regular
   entries rather than silently erase directory evidence relevant to B.3.
6. Retain every downloaded file byte, including checker/stdout duplicates.
   Archive deduplication is compression only. No campaign dispatch, rate, L,
   formula, minimum, cell, ceiling, reservation or execution behavior changes.

Measured on the read-only run 42181882847 download via an external scratch
copy (2026-11-06): 1,629,250,878 raw bytes, 534 files (590 entries counting
directories), archived as root files 394,080; medium-0 651,872; medium-1
787,712; medium-2 753,512; medium-3 757,096; short-0 687,844; short-1
4,844,876; short-2 4,814,816; short-3 4,812,428 bytes. Total payload
18,504,236 bytes. These are measured sizes for this run, not predictions for
future jobs. Raw manifest and index sizes are reported separately in the
Round 185 measurement record. Actual repository intake remains Claude's work
after this amendment is reviewed and merged.

### R256 additive engine-path saturation disclosure

For every initial and repeat cell, freeze derives metrics only from the
verified snapshot, arrivals and external witness retained in the artifact:
offered rate; completion throughput; first-completion delay; carried warmup
outbox backlog; measured outbox publish-lag p50/p99/max plus published and
unpublished counts; latency medians in four consecutive arrival-order groups;
and the p99 latency workflow's four phases (scheduled to ATTEMPT_CREATED,
to witness start, to ATTEMPT_RESULT_RECORDED, to final-revision SUCCEEDED).
Also report each phase's individual p99, explicitly separate from the
additive decomposition of the single p99 workflow. Phase identities must
match; a missing or ambiguous winning-attempt phase refuses disclosure rather
than fabricating data. Multiple attempts alone do not refuse the freeze.

Round 188 / R259 conservative reading under the Round 173 standing instruction:
use the unique final-revision SUCCEEDED transition, then the unique
ATTEMPT_RESULT_RECORDED row with the greatest revision strictly below that
transition. Revision order is authoritative even when timestamps tie or history
rows are stored out of order. Match its ATTEMPT_CREATED row at an earlier
revision and its single witness start by node_id, iteration and attempt_number.
Do not select the first witness invocation or assume that only one attempt ran;
earlier invocations may also be witnessed and SUCCEEDED. Missing or duplicate
winner/result/created/start evidence cannot identify unique phases and refuses;
unrelated earlier attempts do not. Existing evidence-integrity and phase-order
checks remain in force for the verified inputs.

Report per cell multi_attempt_workflows (number of measured workflows with
more than one ATTEMPT_CREATED row) and max_attempts (maximum such row count
for one measured workflow). Both are disclosure only, never saturation
triggers, acceptance criteria or a new campaign blocker. This resolves the
single-attempt assumption conservatively without changing L or its population.

Conservative definitions under Round 173: first-to-last completion throughput
is (N-1)/(last completion - first completion), avoiding an inflated N over N-1
intervals. Arrival-span throughput counts measured completions in the inclusive
first-to-last scheduled-arrival interval, divided by that interval's duration.
Either throughput below 0.9 times offered rate triggers disclosure. Backlog
counts warmup outbox rows created at/before the first measured scheduled
arrival and published strictly after it or still unpublished. Lag percentiles
use published measured-workflow rows only; unpublished rows are explicitly
counted, never assigned zero lag. Quartile medians use raw durable-minus-
scheduled latencies in arrival order; p99 uses nearest rank, with arrival-order
tie breaking. These diagnostic phases exclude the separate clock-bracket
addition; registered latency/L calculation still includes that bracket.

An arm's `engine_path_saturation` boolean is true whenever any repeat has
either throughput below 0.9 times offered rate, nonzero carried warmup outbox
backlog, or last-quartile median latency strictly greater than 1.5 times its
first-quartile median. Equality at 0.9/1.5 does not trigger. The frozen output
retains all initial/repeat metrics and triggers, the arm flag, and a limitation
stating that L for the arm is derived under engine-path overload, depends on
the 200-arrival window length and is not a steady-state latency target.
K4-K7 reports must surface this flag alongside R251's existing disclosure.
This adds no blocker or change to execution, rates, L, formulas or minima.

### B.5 toolchain security patch disclosure (2026-11-10)

The k3-pilot-1a images were built with Go 1.27.3. Following the GO-2026-6859
standard-library advisory, later jobs use Go 1.27.4. Each arm's cells come
from its registered jobs, and no arm mixes toolchains. The per-job image
digests are recorded in each manifest's `images.json`. This toolchain patch
changes no rate, L, formula, cell, ceiling or reservation.

### Amendment B.6 - sustained-rate re-pilot for overloaded arms

Registered by Claude Round 195 under D035, with the binding Round 197 precision clarification, before any v2 measurement. B.6 supersedes the earlier overload-disclosure-only permission for the short/CPU freeze. All v1 evidence remains immutable.

1. **Retain v1 unchanged.**
   - The short-1..3 and cpu-1..3 v1 repeats become overload-calibration evidence. They are
     excluded from L and from all comparisons, and still count in the ledger.
   - The v1 initials (short-0, cpu-0), the CPU calibration (1000000 units) and the medium
     arm are unchanged and stay valid freeze inputs.
2. **Operating rate.** For short and CPU, register q = 0.5 x X_min. X_min is the minimum
   v1-repeat `completion_throughput_first_to_last` from `engine_path_metrics`, recomputed
   mechanically from committed evidence:
   - short: X_min = `14.272201`/s and q = `7.136101`/s (six-decimal rounded).
   - cpu: X_min = `1.738647`/s and q = `0.869323`/s (six-decimal rounded).

   This is capacity data from no-fault pilots, never fault outcomes. q is fixed before any v2
   data exists.
3. **v2 pilot.** For each of short and CPU, run three no-fault repeats with unchanged
   workload, resources, slots and CPU units, and new deterministic seeds that do not overlap
   v1.
   - Each repeat warms for 120 s at q, continuously and without draining, then measures for
     T = max(600 s, 200/q) at q. Retain every scheduled arrival; warmup arrivals stay in
     completeness, safety and reconciliation.
   - All existing generator, safety, completeness, reconciliation and teardown gates apply.
   - The CPU job must not re-select units. Before its repeats it re-measures the
     1000000-unit median with the registered calibration procedure (1 discarded + 21
     measured, 120 s bound) as a disclosed drift check against 0.514 s, and records the
     image digest.
4. **Stationarity gate.** Prespecified; each repeat must pass all of:
   - (a) **Throughput.** Split T into six equal bins by scheduled time. In bins 2-6,
     measured completions whose durable terminal time falls in the bin must be at least
     0.95 x the measured arrivals scheduled in the bin. Bin 1 is the pipeline fill.
   - (b) **Latency drift.** For thirds of measured arrivals in scheduled order, the last
     third's median and p90 must each be at most max(1.25 x first third, first third +
     0.5 s).
   - (c) **Engine path.** From the observer's actual samples, the last bin's mean outstanding
     accepted workflows may exceed bin 1's by at most 4. The mean unpublished outbox rows may
     exceed bin 1's by at most 20.
5. **Freeze.** When all three v2 repeats of an arm pass, freeze that arm's
   r = 2q (= X_min), so every downstream 0.25r, 0.5r, 0.75r and 1.25r multiple is relative
   to the sustained rate.
   - L = ceil(1000 x 2 x max v2 repeat p99)/1000, the formula unchanged.
   - Also freeze b0 and r0 from the v1 initial, and the v2 repeats' b, as disclosed
     references.
6. **Bounded fallback.**
   - If any v2 repeat of an arm fails the gate, retain it and run v3 once at q/2 with the
     identical procedure. No further versions.
   - Accept the first passing version; never choose among passing versions.
   - If v3 also fails, or the budget refuses it, that arm is unperformed for stationary-SLO
     comparisons. Its K4-K7 cells are recorded as unperformed and never silently dropped.
7. **Medium and long.**
   - Medium is unchanged: throughput 1.00 at 0.5r, no backlog.
   - Long jobs 2 and 3 run as registered. At freeze, gates 4(a) and 4(b) are evaluated
     offline on the long repeats, and 4(c) is replaced by
     `warmup_outbox_unpublished_at_start == 0`. If long fails, it follows steps 3-6 with
     X_min from its repeats.
   - Medium is evaluated the same way and disclosed, without changing its freeze.
8. **Budget.**
   - Recompute the expected schedule and every admission reservation with the new jobs and
     frozen rates before dispatch. No cell minimum is reduced, and a budget-short family is
     incomplete under the existing rules.
   - Current state: 10,108 s charged; the B.2 expected total is 225,283.5 s against the
     259,200 s cap. Pilots 1a and 1b exceeded their expectation by 3,653 s.
9. **Scope and ordering.**
   - B.6 must not change any long-arm or medium-arm execution path, so jobs 2 and 3 may run on
     either side of its merge.
   - The v2 jobs must be versioned (for example `k3-pilot-v2` evidence root, jobs
     `k3-pilot-4a` short and `k3-pilot-4b` CPU, split if the 18900 s ceiling requires it),
     history-gated and recorded like v1.

Precision (Round 197): X_min is the IEEE-754 double returned by
`dur053_saturation.engine_path_metrics` for the minimum-throughput v1 repeat,
read from committed evidence through `dur053_payload.materialize`. Execution,
arrival schedules, T, reservations and freeze use this double: q = X_min / 2,
r = 2q, and the v3 operating q is the original q / 2. Frozen r equals X_min
exactly for v2; for v3 it equals twice that version's operating q.
The registered literals above are six-decimal rounded check strings only.
Before admission or freeze, `format(X_min, ".6f")` and `format(q, ".6f")`
are string-compared with the registered values. Either mismatch refuses with
`b6-rate-registration`; no float tolerance. Both run and freeze records store
repr() of X_min, q and r, their six-decimal rounded strings and the v1 source
run/cell. For both short and CPU the full-precision T is 600 s.

Implementation/evidence routing: short v2 is `k3-pilot-4a`, CPU v2 is
`k3-pilot-4b`; the conditional short/CPU v3 jobs are `k3-pilot-5a/5b`.
Long v2/v3, only after its offline gate fails, uses one repeat per job
`k3-pilot-4c1/4c2/4c3` and `k3-pilot-5c1/5c2/5c3` to retain honest admission
against the ceiling at potentially slow rates. Their roots are
`k3-pilot-v2` and `k3-pilot-v3`, including B.4 archives, history gating,
same-tooling ci_proof and the unchanged cumulative ledger. Carry-over jobs
retain exact pending cells and do not replay started work. A fourth version
is refused. A gate failure is retained evidence, never a reason to replay a
version. The existing v1 medium/long runner is unchanged.

A single absolute loadgen schedule runs through warmup and measurement;
all arrivals remain in the checker and witness reconciliation. Gate bins
partition the measured scheduled-time window; completion counts use final
SUCCEEDED-revision durable history times. Engine means use the observer's
actual recorded count samples in the first and last bins. Gates are
re-derived from retained bytes at freeze; a stored verdict alone is not
trusted. The reduced CI smoke exercises the same continuous v2 runner with
6 s warmup and 60 s measurement, `budget_scope=engineering-validation`,
`charged_seconds=0`, `measurement_claim=false`; it supplies no campaign data.

R265 freeze limitation: each worker slot is a separate Kafka consumer and
pauses all its assigned partitions while busy. This causes head-of-line
queueing even when other slots are idle. The reviewed medium-1 decomposition
reports about 20.9 s waiting before 5.0 s execution, p99 26.0 s at implied
utilisation 0.50, versus p99 5.6 s at 0.20. Medium L (about 52 s) and K5
comparisons inherit this queueing. This is a latency limitation; claims,
fencing and safety are unchanged. Do not change the engine during DUR-053.
Post-DUR-053 follow-up is recorded in PLAN.md's deferred list: shared dispatch
queue or one consumer feeding a local slot pool, preserving claim fencing
and offset-commit-after-durable-ack, with its own pilot.


### Amendment B.6.1 - pooled throughput and offline engine-path criterion

Registered by Claude Round 199 under D035 on 2026-11-11, after the committed
medium v1 disclosure and before any long-arm or v2 data existed. All three
medium repeats had throughput about 1.00 and zero warmup backlog, but the
per-bin 4(a) rule rejected them: with about 33 arrivals per bin, latency jitter
moves completions between adjacent bins by more than the 5% tolerance. Pooled
bins 2-6 contain 166 arrivals and 168 completions in each repeat. This
correction avoids a false long re-pilot trigger and unnecessary budget use.

1. Gate 4(a) is pooled over bins 2-6: measured completions whose durable
   terminal time falls in those bins combined must be at least 0.95 x measured
   arrivals scheduled in those bins combined. Retain each bin's arrival and
   completion counts as disclosure only. Gate 4(b), gate 4(c), the bin
   definition and every other threshold are unchanged. Latency drift and
   observer drift continue to cover late-onset trends.
2. For the original 200-arrival long and medium v1 windows, the offline
   decision is `completion_throughput_first_to_last >= 0.95 x measured_rate`
   and `warmup_outbox_unpublished_at_start == 0`, derived through
   `engine_path_metrics`. That helper's `offered_rate` is the measured-arrival
   rate (`measured-arrivals.json`), recorded here as `measured_rate`.
   Pooled 4(a) and 4(b) are computed and disclosed offline but decide nothing.
   Long follows B.6 steps 3-6 only if any long repeat fails this criterion;
   all three passing long repeats refuse long v2 admission. Medium remains
   disclosure-only and its freeze is unchanged. Admission, freeze and schedule
   rechecks use the same offline criterion.
3. Precedence: B.6.1 supersedes Round 195 items 4(a) and 7, and the matching
   protocol text, only where they conflict. Nothing else in B.6 or Round 197
   changes.

Fallback admission retains all three v2 repeats (indices 1, 2, 3) before v3
may start, with at least one failing, as clarified by Round 199 R267. This
keeps admission consistent with the unchanged three-repeat freeze requirement.


## Amendment B.6.2 (Round 206 registration; implementation Round 207)

Registered 2026-11-12 under R271, before the short v2 replay. This supersedes
B.6 only for harness capacity and its bounded, content-defined replay. The
rates, seeds, stationarity rules, workload, cell minima and budgets are unchanged.

R271 diagnosis: k3-pilot-4a run 43159046254 started short-1, then the observer
was OOMKilled (exit 137, restart count 1). Its 512Mi limit could not hold the
live namespace checker at the continuous 720 s short workload (5138 warmup
plus measured workflows). The exec failed to find the restarting container;
checker.json is empty. There is no invariant verdict to interpret. The smoke's
6 s + 60 s schedule is too small to establish this memory capacity.

Non-peeking basis: Round 206 used the manifest, command logs, pod diagnostics
and file sizes only, without reading or computing the failed short cell's
latencies, stationarity bins, throughput or gate inputs. This implementation
also classifies that run solely from those diagnostic bytes through materialize;
it does not inspect its performance outcomes. No registered or paid run tunes
the limit.

1. **Observer capacity.** Only the observer container's memory changes:
   request 128Mi -> 512Mi, limit 512Mi -> 2Gi. The actual checked-in CPU
   request is 100m (unchanged), and its CPU limit remains 1000m. No engine,
   worker, scheduler, relay, PostgreSQL or Kafka resource changes. Historical
   package hashes and run manifests remain immutable; there is no active
   static package hash pin for these bytes. New runs capture the changed bytes
   in their existing tooling inventory and applied-object evidence.
2. **Capacity evidence before dispatch.** The Linux CI job "Go, Python,
   Terraform, and offline evidence" builds cmd/dur049-checker, then runs
   scripts/dur053_checker_capacity.py on materialized, committed v1
   k3-pilot-1a/42181882847 short-1, the largest committed short snapshot
   (approximately 42.6 MB). The script invokes /usr/bin/time -v with LC_ALL=C
   and the offline -snapshot path, with -arrivals measured-combined-arrivals.json,
   -witness-export witness.json and -require-terminal. Checker stdout goes to
   RUNNER_TEMP; its valid verdict and zero exit are required. Linux maximum RSS
   is parsed as KiB and converted to bytes. The JSON and raw time report are
   uploaded even on failure. Reproduce on Linux:

   ```sh
   go build -o "$RUNNER_TEMP/dur049-checker" ./cmd/dur049-checker
   uv run python scripts/dur053_checker_capacity.py --checker "$RUNNER_TEMP/dur049-checker" --output "$RUNNER_TEMP/b62-capacity.json"
   ```

   The offline path includes decoding, campaign checks and full JSON emission,
   but omits PostgreSQL row decoding and construction of raw tables and checker
   projections inside LoadNamespace. It is an incomplete proxy for live peak
   memory. Use the larger estimate: max(measured offline RSS, the entire former
   512Mi v1 allocation), then multiply by the registered combined-arrivals ratio
   1.50 and a 2x margin. Assert both the resulting estimate and, consequently,
   measured RSS x 1.50 x 2 are <= 2Gi. The 512Mi floor is a conservative planning
   estimate, not a measurement or a proven upper bound for the live loader.
   At that floor the projected v2 allocation before margin is 768Mi, below half
   the new limit; CI must still pass if measured RSS is larger. The live guard
   remains necessary. No memory number is inferred from pilot timing metrics.

   The measured offline RSS, selected estimate and projection are recorded in
   each CI run's `b62-checker-capacity-<run_id>` artifact
   (`b62-capacity.json`). For a registered replay, the binding run is the push
   CI run named by that replay manifest's `ci_proof.run_id`. That same CI job
   asserts the projection is <= 2Gi, so a green `ci_proof` run implies the
   capacity check passed for the exact tooling bytes. Claude records the
   numbers in REVIEW.md. Local Windows unit tests validate parsing, the
   larger-estimate rule and both sides of the threshold; they are not a Linux
   capacity measurement.
3. **Specific guard, versioned runner only.** Capture observer pod JSON after
   setup as the cell baseline. On checker nonzero exit or missing verdict,
   read pod JSON through the driver's kubectl helper, including restarting pods.
   If lastState.terminated.reason is OOMKilled or restartCount increased during
   the cell, retain the diagnostics and raise pilot-checker-capacity. A returned
   verdict with valid != true always stays pilot-live-checker, a hard failure.
   The v1 PilotDriver file, its SHA-256 pin and its execution are unchanged.
4. **Content-defined harness-capacity replay (v2/v3 only).** After committed
   bytes, archive hashes and ledger/index bindings pass materialize, classify
   a versioned manifest iff status FAIL, cleanup PASS, error exactly
   `K3 guard: pilot-checker-capacity`, the failing cell's checker.json exists
   and is zero bytes, and committed observer diagnostics show
   lastState.terminated.reason == OOMKilled. The sole legacy error exception
   is k3-pilot-4a run 43159046254 with `K3 guard: pilot-live-checker`.
   A restart alone can select the guard but cannot authorize replay.
   Every failed-run cell, completed or not, remains retained and charged and
   contributes to no gate, L, choose() population or operating record. One
   same-name replay is allowed after one such failure, with every planned
   index and the existing registered seeds. It requires committed B.6.2 tooling
   and green main push CI through the existing history/tooling/ci_proof gates.
   A second harness-capacity failure with that name is a stop requiring a new
   review round. B.3 pre-start-refusal eligibility and its limit are unchanged.
5. **Freeze and schedule.** finish(), retained() and both directions of the
   ledger inventory retain the failure as provenance while excluding its cells.
   A second capacity failure blocks admission and freeze. plan_b6 retains the
   actual ledger charge (including the 1021 s failure) and replaces the pending
   short replay's expected estimate with one full 5040 s reservation. It does
   not double-count a separate successful pilot or grant more campaign time.
6. **Ordering disclosure.** Each versioned run's observer resources are those
   of its own tooling bytes, recorded through its `ci_proof` and applied-object
   evidence. Runs admitted before B.6.2 merged use the 512Mi observer; runs
   admitted after use 2Gi. The freeze and any comparison disclose the per-run
   observer resources. The observer is a harness pod hosting both loadgen and
   the checker via exec. Its CPU request/limit are unchanged; the memory
   estimates target operation below about half of the new capacity before the
   2x margin, not a measured claim that either resource never binds.


## Amendment B.6.3 - harness deadline (Round 211 authorization)

Registered by Claude Round 211 on 2026-11-12 under R273. This amendment
changes Plan cost, the sustained loadgen exec timeout and one legacy harness
failure classification. B.6/B.6.1 rates, seeds, units, gates, cell minima,
18900 s per-job ceiling and 259200 s cumulative cap remain fixed.

Evidence: k3-pilot-4b run 43173232814, dispatched on main 3c2ded1, failed
cpu-1 with command-179-kubectl-timeout. The /loadgen exec for arm cpu and
run ID cpu-1-continuous lasted 780.2 s; cleanup passed and 1917 s is charged.
Its 626 arrivals at q = 0.869323365519607/s cover about 720 s. The old Plan
computed the same million-iteration SHA-256 digest for every arrival, before
loadgen started its own count/rate + 2 x 10 + 30 s deadline. The inherited
kubectl timeout was count/rate + 60 s. This is a harness failure before any
stationarity verdict, so B.6's q/2 fallback does not apply.

Claude's Round 211 disclosure: v1 k3-pilot-1b run 43033536602 showed loadgen
wall time minus schedule of 13.3 s for 120 rows and 22.8-23.6 s for 200/201
rows, about 0.11 s per row, extrapolated to about 71 s for 626 rows. Claude
inspected the 4b terminal-count series only to locate the stall: first
terminals appeared about 80-119 s after exec start and reached 609 of 626 at
the kill. The arrivals file remained the initial IN_PROGRESS plan. No gate
metric (throughput bins, latency thirds or engine-path means) was computed
from this run. q was already registered, so this inspection cannot influence
a rate or gate. These are the reviewer's diagnosis and disclosure, not a new
performance measurement by this implementation.

1. Plan computes the CPU expected digest once per call and reuses it for all
   rows. The output bytes are identical to the per-row reference algorithm.
   Tests compare serialized bytes and count digest calls on the same Plan
   implementation; the registered per-row mutant checks the cost regression.
2. SustainedDriver.sustained passes an explicit continuous-loadgen timeout:
   count/q + 2 x 10 + 30 + 120 s. The first terms match loadgen's unchanged
   deadline, with a 120 s margin for Plan and kubectl exec startup. This orders
   loadgen's deadline and final arrivals encoding ahead of the outer kill
   within that margin. The SHA-pinned v1 PilotDriver remains unchanged.
3. Only cmd/dur053-loadgen imports internal/dur053. The runtime, migrate and
   checker Linux/amd64 binaries are compared byte for byte using CGO_ENABLED=0,
   go build -trimpath -ldflags="-s -w", on aefa5d3 and the implementation tree.
   Exact SHA-256 values and build commands are retained in
   k3b-tooling-v1/round212-b63-validation.json. The Dockerfile packages these
   binaries with loadgen; its runtime and worker image identities change with
   the loadgen bytes. This image change must accompany the CPU drift
   disclosure against the 0.514 s reference. Each run's existing images.json,
   versions.json, applied objects and ci_proof bind its actual environment;
   historical drift images and evidence remain immutable.
4. A harness-deadline failure is legacy-pinned to k3-pilot-4b run 43173232814.
   After materialize verifies committed bytes, archive hashes and ledger/index
   bindings, every condition is required: version 2; status FAIL; cleanup PASS;
   error command-179-kubectl-timeout; exactly one matching timed-out command
   row in commands.jsonl, a kubectl exec of /loadgen with -arm cpu and
   -run-id cpu-1-continuous; exactly one started-but-unperformed cell, cpu-1.
   No later loadgen kubectl timeout qualifies; it is an ordinary failure.
   Across B.6.2 capacity and B.6.3 deadline failures combined, each job name
   receives at most one harness replay. A second harness failure stops with
   a named replay-limit guard, including mixed-class failures. B.3 refusals
   retain their independent eligibility and limit. Replay runs all planned
   indices with registered seeds. retained(), dependency_plan and freeze
   exclude every failed-run cell from every population, including completed
   cells, while retaining provenance, both ledger-inventory checks and charges.
   Freeze records deadline exclusions separately from capacity exclusions.
5. plan_b6 retains the charged failure and reserves the pending 4b replay at
   its full_admission_seconds (7680 s), just as the 4a replay reserves 5040 s.
   A completed PASS job removes the pending reservation; its actual charge
   stays in the ledger. The schedule recheck and cap comparison, both before
   and with PR #22's 1917 s row, are recorded in the validation record above.
6. Ordering: PR #22 carries immutable 4b evidence and merges first. B.6.3 is
   implemented on PR #21 over aefa5d3, followed by main into that branch.
   Existing history, tooling and green main push ci_proof gates bind both
   harness replays to the combined B.6.2/B.6.3 tooling. No dispatch is
   authorized by a local schedule estimate or provisional review.
