# DUR-053 Kubernetes recovery protocol v1

Registered 2026-11-03, before pilot or measurement, against base
`4728edea1cea7ec73b74cf57d51d7f93a64e56f2`. Authority: user-authorized D035;
Codex implements and Claude reviews each exact milestone SHA. This is a
protocol, with no measured results. K0 changes records only.

## Scope and authority

Run only on single-host kind on Docker Desktop/WSL2, with pinned tools, node
image, add-ons and application image digests in each manifest. PostgreSQL and
the single Kafka broker remain outside kind, on the host Docker network;
use a fresh campaign database. Stop Compose application services because they
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
| long | 90000 | Work extending beyond a 60 s lease without heartbeats |

Set `DUR048_ALLOW_FIXTURE_ACTIVITY=1` in schedulers and all runtime roles that
resolve activities; the Python registry requires the positive delay above.
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

For the first pilot, warm at its initial rate above; for every subsequent
fresh run warm for `max(120 s, 2*duration_seconds)` at `0.25*r`,
then stop warmup arrivals and reconcile all warmup work before measurement.
Steady lifecycle/fault runs use `0.5*r`. K5 step windows use `0.25*r`, `1.25*r`,
`0.25*r`, each for `max(120 s, 4*duration_seconds)`. K5 fixed sweeps use the
same `0.5*r` offered load at 1/2/4 pods, never tune offered load per replica.
K7 fills four pods at `0.75*r`, then scales 4 to 2 with active work present.
All runs offer at least 200 measured arrivals; extend the final window to
reach 200, without shortening earlier windows or waiting on completions.
Record accepted, rejected and uncertain counts separately. SLO attainment
requires at least 99% of accepted workflows to succeed within `L` from their
scheduled arrival and zero incomplete accepted work at reconciliation.
Report rejection rate alongside SLO attainment; rejecting work is not success.

Reconcile until `max(600 s, 10*L, 4*duration_seconds)` after the final arrival
or confirmed fault removal, whichever is later. A timeout is a failed run with
censored rows retained, never a dropped sample. Stop new injections on any
safety violation, connection-budget breach or uncontrolled resource growth;
export evidence, bound cleanup, and report the unperformed arms. No automatic
rerun replaces a failure. Infrastructure-invalid episodes remain in the run
registry and may receive one linked replacement using a new ID; fault-induced
failures are valid outcomes, not infrastructure exclusions.

Intervene at scheduled arrival 100, after confirming the target has in-flight
work (or the named boundary is held). Allow at most `2*L` to satisfy that
condition; if it is not met, preserve an unexercised episode, not a safety
pass. Non-scaling lifecycle runs start with two worker pods; worker scale-down
and K7 start at four and go to two. The reference rate remains the four-slot
pilot rate throughout. Disable autoscalers in fixed-replica/K4/K6/K7 runs;
only K5 policy comparisons use autoscaling. No intervention changes the frozen
arrival schedule.

## Sample allocation and comparisons

An episode is a fresh campaign namespace/database population with one
intervention, independent IDs and seed. Freeze seeds `530001+i` and an
alternating baseline/treatment order per paired episode. Shared-host time
trends remain a limitation; do not treat workflow rows as independent fault
replicates. Each episode follows the arrival minimum above.

| Milestone | Experimental cell | Independent episodes per cell |
| --- | --- | --- |
| K4 | Each event x short/medium/long x baseline/treatment | 12 matched pairs |
| K5 | Each of six scenarios x three demand/lag/CPU policies x duration | 12 per policy; CPU-only workload also gets all policies |
| K5 | Fixed 1/2/4 pods x duration, plus CPU arm | 12 per replica point |
| K6 | Each fault/target/direction or skew sign x duration; effect faults separately | 12 per cell, including 12 no-fault matched controls |
| K7 | Scale-down x duration, controller off/on, if GO | 30 matched pairs |

K4 events: worker rollout, worker scale-down 4 to 2, scheduler/API/relay
rollout separately, PDB eviction via node drain, SIGKILL of worker/scheduler/
API/relay separately, and rollback of each application role separately.
K5 scenarios: step load, lag-zero long activity, partition skew, relay stall
with healthy broker, broker down, and scale-down with work in flight.
K6 cells are enumerated by the fault table below; effect cells cover kill
before request, after sink commit/before response, duplicate request, and lost
response with same-key lookup. Named PURE hooks are preselected now from the
existing 16-case archive: `submission_committed`, `outbox_insert`,
`before_publish`, `after_broker_ack`, `attempt_claimed`, `before_offset_ack`,
`result_recorded`, `lease_takeover`, `attempt_replaced`, `timer_consumed`,
`attempt_timeout`. Effect hooks add `approval_grant` and `effect_applied`.
Each hook gets 12 independently confirmed episodes per duration arm. The
archive's `effect_unknown` and `cancel_completion` cases remain separate
in-process evidence; this extension adds no deployed non-cooperating sink or
cancellation route. Report those exclusions in coverage. The archive has 16
cases and 15 distinct boundary names (two cases use `approval_grant`).

Twelve episodes give bounded repeatability and expose variability without a
claim of rare-failure certification (zero failures in 12 still has an upper
one-sided 95% binomial bound about 22%). Thirty K7 pairs support a directional
paired comparison but are not a production power calculation. Report raw
counts, episode distributions, Wilson 95% proportions and paired differences;
no significance or universal safety claim from zero observed violations.
Report p50/p99 descriptively with sample counts; 200 arrivals provide only
about two observations in the top 1%, so tail estimates remain imprecise.

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
| Abandoned attempts | Attempts with witnessed starts active at intervention that exit/abort without an accepted terminal receipt before claim replacement/expiry recovery. Count per termination/scale-down and per active-at-intervention attempt; continue observation until the reconciliation deadline. Separate cancellations and unknown outcomes. |
| Re-executed activity-seconds | Sum elapsed slot-busy seconds on invocations after the first start for each logical node iteration. Also report wasted original busy seconds on abandoned invocations; unfinished intervals carry observed lower bound and last-confirmed-alive-to-exit upper bound. Never substitute configured sleep for measured time. |
| Partition re-acquisition time | For a partition held at scheduler termination: competing owner's first committed acquisition time minus confirmed old-owner release commit time. If unreleased, use intervention time and label TTL-expiry recovery separately. Persist acquisition ledger (opt-in, bounded campaign) and old epoch; an observed NULL owner is insufficient. Also report first useful fenced transition and terminal progress. |
| Superseded-owner transitions | Committed owner-authorized transitions with an epoch older than a prior committed takeover for that partition. Independent history/lease reconstruction; must be 0. |
| Assignment stall | Per membership change, elapsed unskewed observer time from first revoke/rebalance-start callback until all surviving consumers have a stable assignment and a runnable sentinel delivery is durably disposed by the group. Report assignment-only and useful-disposition components, trigger UID, consumers and longest active invocation; an idle group needs the sentinel to make progress observable. |
| Drain | SIGTERM-to-process-exit duration; accepted results committed after SIGTERM/before exit; readiness-to-endpoint-removal delay; per-role control HTTP errors/retries and client submission errors. Use same request identities across retries. |
| Recovery | Intervention-to-replacement-attempt creation, intervention-to-first-useful-transition and intervention-to-terminal for affected workflows; right-censor at the registered deadline. Report stale rejections by typed code. |
| Latency/throughput | Scheduled-arrival-to-durable-terminal p50/p99 for accepted work, successful completions per measurement second, SLO success fraction, and separate offered/accepted/rejected rates. Show fault/rollout and no-fault windows plus p99 difference; polling observation error accompanies terminal timestamps if commit time is unavailable. |
| Scale-up delay | Demand threshold first exceeded at a fresh sample to first additional pod ready AND taking a durable claim; report desired/current/ready/claiming replica traces. |
| Oldest claimable age | At each DB sample, max age since authoritative attempt became claimable among distinct currently claimable attempt identities in the campaign namespace; empty fresh sample is 0, stale/error is missing. Report time-sampled p99, never max of per-partition last-writer gauges. |
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
duration arm at least 4 of the 12 episodes remove a busy pod while another
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
K7 runs 30 matched off/on scale-down pairs for all three duration arms.
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
allowlists and is not supported by the historical baseline image.

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
The K4 baseline preserves pre-K1 lifecycle behavior in combined mode but must
carry K2 authentication/route restrictions and the same witness instrumentation.
An unpatched historical image must not be deployed in-cluster. Record those
non-lifecycle backports and their digest; paired arms differ in lifecycle
behavior, never in required security or evidence coverage.

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
