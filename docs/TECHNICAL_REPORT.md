# Distributed Durable Execution Engine — technical report

Status: READY_FOR_REVIEW for DUR-030  
Evidence baseline: M0 through M7 reviewed task artifacts, plus DUR-033A production-path integration  
Report date: 2026-09-19

## Executive summary

This project is a Go/Python durable-execution engine whose authoritative state is
in PostgreSQL. Scheduler ownership is fenced by partition leases and epochs;
workflow changes, attempt state, history, and dispatch obligations are committed
together. A transactional outbox feeds direct notification and Kafka transport;
the inbox, offsets, duplicate identities, quarantine obligations, and
reconciliation paths keep transport failure from becoming an invisible state
transition. Worker claims, result receipts, cooperating effects, and approval
grants have separate identities and fences.

The strongest bounded findings are:

1. On the declared single-node Docker Desktop/WSL2 host, two scheduler goroutines
   with a fixed four-process worker pool sustained the calibrated 2 workflows/s
   offered rate (median 1.967 and 1.984 workflows/s across two 2-scheduler
   configurations), while one scheduler reached only 1.370 and 1.636. At 1/s,
   all configurations tracked the offered rate. This is an engine-path result,
   not end-to-end API/Kafka throughput.
2. In the quoted DUR-035 campaign, notification-direct was faster than Kafka on
   terminal latency: medians were 35.6 ms versus 51.4 ms, with separated
   run-level intervals. Ready-to-claim latency was lower for direct notification
   too, but its direct-versus-Kafka intervals overlapped, so no overall dispatch
   winner is claimed. Kafka was not shown to improve the resolved terminal
   stage at this tested scale; the study does not claim an overall dispatch
   winner. Backlog decoupling, retained transport, connection count, and
   multi-host properties were not measured here.
3. The held-out retrieval study found keyword and hybrid ranking Recall@K of
   1.000 versus dense 0.956, with MRR 1.000, 0.948, and 0.784 respectively.
   Those retrieval differences did not translate into a live-agent safe outcome:
   all three arms were 4/20 overall and 0/16 on the document-dependent subset.
   The deterministic fixture control was 20/20 per arm, localizing the observed
   quality gap to the live model/configuration rather than the retrieval harness.

These are measurements under named configurations, not claims of exactly-once
execution for arbitrary external effects, production availability, multi-host
durability, or parity with another durable-execution product.

## System and guarantee boundary

The M7 measurements predate the deployed scheduler and Kafka-worker wiring
added in DUR-032 and have not been rerun on it; they describe each study's
named harness, not the current deployment. Any deployed rerun must be recorded
as a separate campaign rather than replacing these measurements in place.

PostgreSQL is the source of truth for workflow state, attempts, leases, history,
outbox/inbox records, approvals, effects, evidence, and reconciliation
obligations. Kafka is a transport layer. A committed outbox row is the durable
obligation; LISTEN/NOTIFY is a wake-up hint, and the relay fallback poll is the
recovery path when notifications or a relay process are lost.

The runtime separates several problems that are often conflated:

- submission idempotency prevents duplicate workflow creation;
- scheduler lease epochs fence stale scheduler writes;
- worker claim tokens and attempt identity fence stale progress;
- cooperating effect keys, grant scope, argument hashes, resource revisions,
  and receipts fence repeatable side effects;
- non-cooperating effects do not get an invented success or failure when the
  outcome is uncertain; they become reconciliation obligations with evidence;
- approval grants bind the effect key, resource, canonical arguments, workflow
  revision, fence token, and proposal intent, and the grant is consumed on use.

The DUR-033A integration exercised the production PostgreSQL workflow path from
POST /v1/workflows, through the real engine and scheduler-owned approval grant,
to effects.Service. It also queried the PostgreSQL source_corpus boundary; an
empty evidence query fails the investigation rather than silently producing a
remediation. This is a bounded integration test, not a deployed production
service or external-action authorization.

## Correctness and failure evidence

### Named-boundary campaign

The M5 campaign contains 16 named cases across F01–F11, three seeds each: 48
controller runs, all with controller, Go assertion, and independent checker
status PASS. The cases include response loss, rollback, relay retry, broker
acknowledgment loss, worker claim/offset ordering, cooperating and
non-cooperating effects, crash resume, lease fencing, stale results, timer/join,
and cancellation/deadline/dispatch-grant orderings.

| Evidence | Observed result | Boundary |
|---|---:|---|
| F01–F11 campaign | 48/48 passing traces | [campaign results](../experiments/m5/f01-f11-results.json) |
| Committed fault traces | 48 traces with offline durable snapshots | [trace directory](../experiments/m5/traces/) and [durable snapshots](../experiments/m5/durable/) |
| Outage evidence | Four separately reported episodes with before/during/after unresolved-work snapshots | [outage recovery](../experiments/m5/outage-recovery.json) |
| Runtime readiness | Normal and deployed fault/resume episodes reached SUCCEEDED; deployed counters increased | [DUR-036 readiness](../experiments/m7/dur036-readiness.json) |

The checker is intentionally independent of the production transition validator:
it parses the controller trace, loads the referenced durable snapshot, checks
sequence/schema/boundary facts, and verifies persisted rows. It is an evidence
oracle, not a second call into engine decision logic. The corrected F01
`submission_committed` boundary stops immediately after the durable submission,
before lease and attempt setup. The regenerated snapshots contain zero activity
attempts there, and the independent checker rejects any snapshot with durable
activity work at that boundary. [R057, R083, and R088](REVIEW_STATUS.md#current-status)
were verified closed in Claude's round-50 review; the public status record
preserves their scoped meanings while the historical review text remains the
audit record.

### Runtime readiness and production integration

DUR-036 exercised the deployed runtime on the declared Linux desktop-linux WSL2
host. The normal endpoint produced a SUCCEEDED workflow and positive
lease-acquisition, claim, result, and worker-completion deltas. The fault endpoint
used the deployed registry, injected a bounded in-process crash after result
recording, and resumed to SUCCEEDED with revision 7. A deployed fault episode
showed two lease acquisitions for one resumed claim, which is consistent with a
crash followed by recovery. This is a readiness gate, not a process-kill or
multi-host durability experiment.

DUR-033A separately proved that one incident remediation can traverse the
production engine/effect path with PostgreSQL source evidence and a matching
approval. The attack matrix rejects changed resources, changed canonical
arguments, changed revisions, grant reuse, and pre-approval dispatch. No live
model call or external action occurred in this integration.

## M7 measurement results

All M7 results below are bounded to the declared host and fixture path in each
artifact. Medians are run-level summaries; they are not pooled tail percentiles.

### Scheduler count and fixed worker capacity — DUR-026

The study uses an open-loop buffered cohort, a fixed pool of four worker
subprocesses, 24 measured workflows per configuration, four warmup workflows,
three repeats, and a frozen 120-second completion SLO. It covers T1 (eight
sequential pure activities) and T2 (eight-branch fan-out/fan-in pure
activities). The 24 configuration runs completed 576 workflows with zero
pending workflows and zero SLO violations.

| Workload | Offered rate | Schedulers | Median throughput/s | Scheduler CPU s/workflow | Worker CPU s/workflow |
|---|---:|---:|---:|---:|---:|
| T1 | 1/s | 1 | 1.006 | 0.0955 | 0.0039 |
| T1 | 2/s | 1 | 1.370 | 0.0981 | 0.0026 |
| T1 | 1/s | 2 | 1.012 | 0.0911 | 0.0037 |
| T1 | 2/s | 2 | 1.967 | 0.0909 | 0.0039 |
| T2 | 1/s | 1 | 1.017 | 0.0788 | 0.0020 |
| T2 | 2/s | 1 | 1.636 | 0.0868 | 0.0028 |
| T2 | 1/s | 2 | 1.015 | 0.0894 | 0.0033 |
| T2 | 2/s | 2 | 1.984 | 0.0974 | 0.0037 |

The fixed worker CPU range and open-loop queue make the 2/s contrast an observed
scheduler-capacity result rather than a second scheduler also adding execution
capacity. The harness uses the in-process Store/Engine path and does not include
the API, relay, Kafka, or production container topology.

Evidence: [DUR-026 results](../experiments/m7/dur026/results.json),
[study README](../experiments/m7/README.md).

### Lease takeover and useful progress — DUR-027

The 3 TTLs × 2 fault types × 10 episodes matrix contains 60 episodes. The crash
fixture self-exits with status 137 at the fault boundary, leaving the lease held;
pause/resume stops renewal and resumes with a stale owner. The useful-progress
boundary is a durable replacement attempt, not a boolean assertion.

| TTL / renewal | Crash takeover median ms | Pause takeover median ms | Crash useful progress ms | Pause useful progress ms |
|---:|---:|---:|---:|---:|
| 100 / 33 ms | 69.5 | 77.6 | 182.9 | 189.6 |
| 250 / 83 ms | 171.7 | 228.6 | 474.5 | 538.9 |
| 750 / 250 ms | 503.4 | 728.6 | 1311.4 | 1532.6 |

Safety controls recorded 60 takeovers, 60 useful recoveries, zero false
takeovers, 180 fenced stale-owner writes, and 60 lock-contention cases. The
crash takeover clock starts after confirmed fixture death; signal-to-death is
reported separately, so the external kill tool is not in the recovery clock.
Short TTLs are measurement settings, not deployment recommendations.

Evidence: [DUR-027 results](../experiments/m7/dur027/results.json).

### Checkpoint tradeoff — DUR-028

This is one pure 200-chunk SHA-256 activity with one work unit per chunk. The
failure model is an in-process panic after chunk 100 computation and before its
next checkpoint/result commit; it is not an OS process-kill test.

| Checkpoint policy | Crash median completion | Recomputed chunks | Checkpoint writes | Checkpoint bytes |
|---|---:|---:|---:|---:|
| Activity boundary only | 0.291 s | 200 | 0 | 0 |
| Every chunk | 2.201 s | 101 | 200 | 19,692 |

The observed ratio is 7.6× for this workload: every-chunk checkpointing saves
99 chunks of replay but does not pay at one SHA-256 work unit per chunk. That is
not a general checkpoint policy or a crossover estimate.

Evidence: [DUR-028 results](../experiments/m7/dur028/results.json).

### Safeguard ablation — DUR-034

The full profile emitted 1,176 history rows and 952 outbox rows per run. The
history-disabled and no-outbox negative controls removed their targeted evidence
or dispatch mechanism; the unsafe lease control produced the check-to-commit
takeover ordering that the safe profile rejects. The safety mechanisms are the
result here, not deployable performance alternatives.

| Profile | Median throughput/s | Spread | Mechanism observation |
|---|---:|---:|---|
| Full | 1.176 | 15.6% | 1,176 history rows; 952 outbox rows |
| History disabled | 1.224 | 11.7% | 0 history rows |
| Unsafe lease check | 1.079 | 12.5% | stale write can commit in negative control |
| No outbox | 1.237 | 13.2% | 0 outbox rows; delayed recovery path |

No cost effect is promoted: repeated clean runs had 3.2–29.3% spread, so the
within-profile variability did not support a safeguard-cost comparison. The
observed F08 unsafe arm committed superseded epoch 173 after epoch 174 takeover;
the safe arm rejected the old owner. Evidence: [DUR-034 results](../experiments/m7/dur034/results.json).

### Dispatch path decomposition — DUR-035

All four arms start from the same committed task-outbox record and end at the
same worker claim/result/consume path. The cohort is 24 workflows, four worker
subprocesses, three repeats per arm, and 288 terminal workflows with no pending
or failed rows.

| Arm | Ready-to-claim median ms | Terminal median ms |
|---|---:|---:|
| PostgreSQL poll, 250 ms | 142.7 | 193.3 |
| PostgreSQL poll, 1 s | 603.5 | 636.8 |
| LISTEN/NOTIFY direct | 5.25 | 35.6 |
| LISTEN/NOTIFY + Kafka | 6.01 | 51.4 |

The computed wake-mechanism comparison resolves polling versus notification
direct. The current campaign's ready-to-claim direct-versus-Kafka intervals
overlap, so that stage is unresolved. Terminal intervals separate with Kafka
slower by 15.8 ms at the reported medians. Thus the citable claim is terminal
latency, not a fixed dispatch-stage increment; a prior campaign resolved a larger
dispatch difference, showing that number is not stable across runs.

At this tested scale, notification-direct was faster on the resolved terminal
stage. The study therefore does not demonstrate Kafka as a latency optimization
for overall dispatch; the ready-to-claim stage remained unresolved. Kafka's
architectural rationale—transport decoupling, retained backlog, and possible
multi-host topology—was not measured by this local campaign.

Evidence: [DUR-035 results](../experiments/m7/dur035/results.json).

## Retrieval, incident-agent, and approval evidence — M6/M7

The retrieval benchmark is frozen and held out from development tuning. It
contains 120 queries, 90 answerable and 30 no-answer, with separate ranking and
delivered metrics.

| Arm | Ranking Recall@K | Delivered Recall@K | MRR | No-answer false-positive rate |
|---|---:|---:|---:|---:|
| Keyword | 1.000 | 1.000 | 1.000 | 0.000 |
| Hybrid | 1.000 | 1.000 | 0.948 | 0.033 |
| Dense | 0.956 | 0.956 | 0.784 | 0.033 |

The held-out retrieval result does not identify a live-agent winner. In the paid
live evaluation, each retrieval arm had 20 executions; live safe end-to-end
success was 4/20 overall and 0/16 on the document-dependent subset in all three
arms. The deterministic fixture control was 20/20 per arm. The sample uses one
model (openai/gpt-4o-mini), one prompt/schema and one sampling regime, so the
rates should be quoted with their counts.

The adversarial protocol used 120 executions with clean-clean baselines. Excess
injection-associated proposal change was 0.10 defended (2/20 above its
clean-clean baseline) versus 0.30 plain (6/20), a 0.20 difference equivalent
to four cases out of twenty per profile. The underlying clean-clean and
clean-injected counts were 2/20 and 4/20 defended, versus 4/20 and 10/20 plain.
Redacted profiles had zero canary leaks; the redaction-off
negative control produced five, proving the scanner can fire. Across the live
agent executions, 27 proposals were made, 27 were approved, and zero completed
without approval. Approval enforcement is therefore reported separately from
model quality and prompt-injection behavior.

The paid ledger recorded $0.04798995 against the explicitly authorized $30 cap,
with no remaining reservation. No external action or production incident was
executed.

Evidence: [final retrieval](../experiments/m7/dur029/retrieval-final.json),
[live evaluation](../experiments/m7/dur029/live-evaluation.json),
[M6 experiment index](../experiments/m6/README.md).

Citation provenance is checked programmatically against evidence IDs returned by
authorized tools. That is distinct from semantic claim support: a citation can
be provenance-valid without proving the prose claim. Similarly, correct
abstention and false-abstention are kept separate because a model can appear
safe by refusing every actionable case.

## Structured operations, MCP, and recovery reasoning

Structured history, metrics, outbox/inbox rows, and reconciliation items are
operational evidence. Retrieval is a separate measured tool for runbooks and
postmortems. MCP contributes a schema-constrained interoperability boundary for
tools; it does not own workflow state, lease fencing, effect authorization,
approval semantics, or receipt idempotency. Those responsibilities remain in the
durable engine and effect service.

An interrupted scheduler resumes from committed database state, not from an
in-memory stack. A result or checkpoint committed before a lost wake-up is
recovered by scanning durable state. A crash before the result is committed can
cause at-least-once re-execution of pure work; a non-cooperating effect with an
uncertain outcome is reconciled rather than automatically repeated. A lease
takeover fences the old scheduler but does not imply that already-valid worker
work can be discarded. Cancellation settles active attempts and records
OUTCOME_UNKNOWN for claimed effects when a request may already have been sent.

The outbox and Kafka are not one transaction: PostgreSQL commits the obligation,
the relay publishes it, the broker acknowledges it, and the relay records
publication afterward. A crash between publication and recording can duplicate
transport delivery; inbox identity and consumer offsets make redelivery a
no-op, while reconciliation handles poison or quarantined obligations.

## Current reference context — no parity claim

The following is architectural context from current official documentation,
not a feature scorecard or a claim that this project matches any reference.

| Reference | Verified context | Relevance here |
|---|---|---|
| [Temporal workflow execution](https://docs.temporal.io/workflow-execution) and [Activities](https://docs.temporal.io/activities) | Temporal documents durable workflow execution, event-history replay after failure, worker processes, and Activities dispatched through task queues; its Activity guidance recommends idempotence because retries can repeat work. | Context for the separation between deterministic orchestration and fallible activity execution. This project uses explicit PostgreSQL state, leases, attempts, and effect classes instead of claiming Temporal compatibility. |
| [DBOS architecture](https://docs.dbos.dev/architecture) | DBOS documents workflows and steps checkpointed in PostgreSQL, recovery from the last completed step, distributed executors, and an optional Conductor control plane for distributed recovery. | Context for a PostgreSQL-centered durable-workflow model and checkpoint/recovery tradeoffs. No performance or semantic equivalence is asserted. |
| [Restate services and workflows](https://docs.restate.dev/foundations/services) | Restate documents durable execution, workflow instances with once-per-ID execution, durable promises/signals, service versioning, and stateful service forms. | Context for durable service/workflow interactions and approval-like waits. This project does not claim Restate API, runtime, or guarantee parity. |
| [River](https://riverqueue.com/) | River documents a Go job queue backed by PostgreSQL or SQLite, transaction-safe enqueueing, retries, unique jobs, and optional workflow features. | Context for the job-queue/outbox boundary. River is not treated as a workflow-engine baseline, and no throughput comparison was run. |

## Claim-to-evidence register

| Published claim | Exact evidence | Configuration and limit |
|---|---|---|
| Scheduler count changes capacity when worker capacity is fixed | experiments/m7/dur026/results.json | 24 workflows/configuration, 4 worker subprocesses, 1/s and 2/s calibrated rates, single WSL2 host |
| Lease takeover is fenced and useful work resumes | experiments/m7/dur027/results.json | 60 local crash/pause episodes, TTL 100/250/750 ms; not host/storage failure |
| Checkpoints trade persistence for less replay | experiments/m7/dur028/results.json | 18 runs, 200 SHA-256 chunks, 1 work unit/chunk, in-process panic |
| Safeguards expose measurable mechanisms; cost delta withheld | experiments/m7/dur034/results.json | 12 runs, 24 workflows, four test-only profiles; unstable repeated spread |
| Notification direct is lower-latency than Kafka on terminal stage in the quoted run | experiments/m7/dur035/results.json | 12 runs, 288 workflows, four workers; ready-to-claim comparison unresolved |
| Retrieval arms differ in held-out ranking but not live safe success | experiments/m7/dur029/retrieval-final.json, experiments/m7/dur029/live-evaluation.json | 120 retrieval queries; 20 live cases/arm; one paid model/configuration |
| Approval and effect binding hold on the production path | `TestDUR033AProductionPath` in [production_integration_test.go](../internal/incident/production_integration_test.go), reviewed implementation commit 95948cb | PostgreSQL test server, synthetic source corpus, no external action |

## Scope, cost, and remaining limitations

- The final host evidence is a single Docker Desktop Linux/WSL2 development
  environment with one PostgreSQL/Kafka topology. It is not multi-host,
  replicated-storage, production-availability, or sustained-load evidence.
- Several studies use an in-process Store/Engine path; DUR-035 includes the real
  Kafka broker and production relay, while DUR-036 readiness exercises the
  deployed runtime. These scopes are not interchangeable.
- The round-50 residual corrections verified [R057, R083, and R088](REVIEW_STATUS.md#current-status):
  the F01 boundary/checker alignment, the reachable DUR-027 handoff target,
  and the focused `ValidateApprovalGrant` guard coverage. The claim-retry
  coverage was mutation-verified in Claude's round-51 review; this report does
  not treat it as an unverified gap.
- The live-model study used the separately authorized $30 aggregate cap and
  spent $0.04798995. Local CPU, storage, hosting, and operator time were not
  monetized.
- The report does not claim exactly-once execution for arbitrary external
  effects. Cooperating sinks require their idempotency contract; uncertain
  non-cooperating outcomes stop automatic retry and require reconciliation.
- The control API and worker endpoints remain development-only and
  unauthenticated, bound to localhost by default. Authentication is a prerequisite
  for broader exposure.
- Subsequent review identified and verified a liveness repair ([R096](REVIEW_STATUS.md#current-status)):
  a stalled scheduler transaction can hold the lease-row lock beyond lease
  expiry and block takeover. Claude's round-55 review verified the repair at
  `41bb9bc`, including the bounded-wait regression and scheduler-continuation
  test; preserved-process isolation evidence is in
  `experiments/portfolio/local-recovery/isolation-410099/`. The historical
  campaign's lock-held arm remains excluded from that artifact's scope, and
  the result is still not a multi-host scheduler-liveness claim.

## Reproduction index

Published reproduction instructions are in [README.md](../README.md),
the [runbook](RUNBOOK.md), and scripts/. The evidence index is:

    go test -race -p 1 ./...
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File ./scripts/ci.ps1 -WithServices -WithRace
    experiments/m5/f01-f11-results.json
    experiments/m5/outage-recovery.json
    experiments/m7/dur036-readiness.json
    experiments/m7/dur026/results.json
    experiments/m7/dur027/results.json
    experiments/m7/dur028/results.json
    experiments/m7/dur034/results.json
    experiments/m7/dur035/results.json
    experiments/m7/dur029/retrieval-final.json
    experiments/m7/dur029/live-evaluation.json

The first command is the serial database-safe form used for the reviewed
service-backed suites; the complete service command additionally starts or
expects the local topology. Reproduction may require Docker, PostgreSQL,
Kafka, the pinned toolchains, and the environment described by the relevant
artifact. No paid run is part of ordinary report reproduction.

## Report conclusion

The evidence supports a durable engine with explicit ownership, recovery,
dispatch, effect, approval, and reconciliation boundaries, and it supports the
bounded local findings above. It does not support a universal exactly-once
claim, a production-scale throughput claim, a multi-host durability claim, or
parity with Temporal, DBOS, Restate, or River. The useful portfolio statement is
therefore specific: the project demonstrates and independently checks the
failure boundaries it names, measures the tradeoffs under frozen local
configurations, and keeps model quality, retrieval quality, approval enforcement,
and external-effect safety as separate dimensions.
