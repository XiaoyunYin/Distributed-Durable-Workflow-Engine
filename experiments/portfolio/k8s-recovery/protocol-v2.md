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
