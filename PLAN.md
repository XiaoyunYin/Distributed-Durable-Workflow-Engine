# Distributed Durable Execution Engine — Agent Workflow Runtime — PLAN.md

**Stack:** Go, Python, PostgreSQL, Apache Kafka, Docker Compose, OpenTelemetry, Prometheus.

**Status:** Planning. Every task is TODO. No correctness, performance, or agent-quality result is claimed.

**First task:** DUR-001. This project has its own repository and evidence. Project 1 is not a dependency.

## 1. Purpose and protected objectives

Build a durable workflow runtime and answer:

**How can multiple schedulers and workers recover from crashes, duplicate messages, and stale ownership without losing committed progress or silently repeating external effects? What do the safeguards cost?**

The deliverable combines a working execution engine, reproducible failure experiments, and an incident-investigation agent that uses the engine.

The central demonstration is a crash at an inconvenient boundary: a tool applies a remediation, but the worker dies before reporting success. Recovery must discover or safely reuse the existing effect, preserve the approval record, and explain any uncertainty.

### Required engineering outcomes

- At least two Go scheduler replicas coordinate through PostgreSQL lease-based ownership of fixed logical partitions.
- Python worker pools receive task dispatch through Kafka. Durable events also travel through Kafka.
- PostgreSQL is authoritative for workflows, attempts, checkpoints, timers, approvals, and pending dispatch.
- Retries, idempotency keys, scheduler fencing, worker-attempt validation, checkpoint recovery, and cancellation have explicit contracts.
- A deterministic fault harness controls crash boundaries and checks durable state against an independent effect ledger.
- An incident agent gathers logs, metrics, and runbooks; proposes a diagnosis and action; waits for approval; applies an allowlisted sandbox action; verifies the outcome.
- Measurements cover recovery time, workflow throughput, duplicate/lost effects, checkpoint overhead, and agent success.

### Scope protection

Codex may update task status, evidence links, and implementation details within the agreed scope. Claude Code reviews this plan but does not edit it. Neither agent may change guarantees, release criteria, experiment families, paid budgets, or deferred scope without an explicit user decision recorded in `docs/DECISIONS.md`; Claude must flag unauthorized protected-scope drift as a review finding.

A negative finding is useful. Do not weaken an invariant to make a failing test pass or rewrite a hypothesis after seeing final results. Version changed protocols and preserve earlier results.

This is an educational runtime with documented operating limits. Do not claim production readiness, universal exactly-once execution, or parity with an established workflow platform.

## 2. Questions and evidence

| Question | Prediction or obligation | Evidence that rejects or limits it |
|---|---|---|
| RQ1: Correct recovery across failure boundaries | Fenced transitions, durable dispatch, and cooperating effect endpoints preserve the stated invariants under the declared fault model. | A forbidden state transition, a stale accepted completion, an unexplained missing effect, or a duplicate effect where deduplication was promised. |
| RQ2: Ownership recovery versus coordination overhead | Shorter scheduler leases reduce takeover delay but can increase renewal traffic and unnecessary ownership changes during pauses. | Little recovery improvement, unacceptable database contention, or excessive ownership churn under the tested settings. |
| RQ3: Checkpoint frequency versus repeated work | More frequent checkpoints reduce repeated pure computation after a worker crash while increasing persistence work. | Checkpoint overhead outweighs the saved work or the expected relationship is absent in the measured workload. |
| RQ4: Incident-agent continuity | Persisted tool/model results and approval state allow an interrupted investigation to resume without losing committed evidence, skipping approval, or repeating protected effects. | A deterministic interrupted run diverges from its uninterrupted committed outcome, or live-model recovery materially degrades safe end-to-end success under the frozen evaluation. |
| RQ5: Safeguard cost | History, per-transition lease validation, and transactional outbox machinery impose measurable persistence/coordination cost; negative controls may be faster but must expose the specific auditability, safety, or bounded-dispatch property that the corresponding safeguard provides. | The full profile has no measurable overhead in the tested regime, or a disabled safeguard does not produce the expected loss of auditability, stale-owner safety, or dispatch-latency bound under its matched test. |
| RQ6: What does Kafka buy beyond PostgreSQL wake-up? | Decompose dispatch into three designs that begin from the same committed outbox record and end at the same worker claim API: periodic PostgreSQL polling plus direct worker notification; `LISTEN/NOTIFY`-woken direct notification with no broker; and the production `LISTEN/NOTIFY`-woken outbox relay -> Kafka -> workers path. Arm 1->2 isolates the wake mechanism; arm 2->3 isolates the incremental transport. | If notification-driven direct dispatch matches or beats Kafka on local latency/overhead, conclude that Kafka is not a latency optimization at the tested scale; describe any remaining Kafka rationale as architectural unless separately measured. If Kafka materially improves a measured property, name that property and keep the claim within the tested topology. |

Scheduler scaling is a supporting performance study, not a central thesis. More schedulers may stop helping when PostgreSQL, a hot logical partition, Kafka, or workers become the bottleneck.

**Primary evidence:** named failure traces, independently checked state transitions, effect-ledger reconciliation, measured recovery distributions, and matched safeguard ablations.

**Supporting evidence:** throughput and latency at matched workload/worker capacity, three-arm dispatch-path measurements, persistence overhead, and a frozen agent evaluation set. No target improvement percentage is promised.

## 3. Execution model and architecture

### Starting execution model

Implement a versioned declarative workflow graph interpreted by Go:

- activity nodes;
- durable timers;
- bounded fan-out/fan-in;
- conditions over persisted results;
- an explicit approval wait;
- terminal success, failure, or cancellation, plus a blocked reconciliation-required state.

The incident workflow has a bounded investigation loop, represented by persisted iteration numbers and node instances. Every loop and fan-out has a configured limit.

Go applies state transitions using durable inputs. Python performs external I/O and computation as activities. LLM calls are activities; their committed responses become data for subsequent transitions.

Recovery loads persisted state and unfinished work. It does not serialize a Python stack or transparently replay arbitrary application code. An append-only transition history supports audits and the test checker; it is not a promise that arbitrary old code can replay forever.

Temporal is a useful design reference for separating orchestration from fallible activities. This project's explicit state-machine/checkpoint model should be compared with its documented execution model, without claiming equivalent capabilities. [Temporal workflow execution](https://docs.temporal.io/workflow-execution), [Temporal activities](https://docs.temporal.io/activities)

### Process topology

```text
CLI / workload generator / human approval client
                         |
              Go control API replicas
                         |
               PostgreSQL source of truth
          workflows, attempts, leases, timers,
          results, checkpoints, approvals, outbox
                 ^                    |
                 |              Go outbox relays
        Go scheduler replicas         |
        own logical partitions        v
                                 Apache Kafka
                            task and event topics
                                |           |
                      Python worker pools   Go event ingestors
                                |
                       Go worker-control API
                      claim / heartbeat / result
                                |
                   tools / LLM / sandbox effect service

OpenTelemetry + Prometheus <- all components
Fault controller + invariant checker -> traces and independent evidence
```

The Go binary may expose separate API, scheduler, relay, and ingestor roles. Keep role boundaries testable; separate deployments are not a requirement for every role.

### Fixed starting choices

- Local development: Docker Compose with PostgreSQL, Kafka in KRaft mode, two Go control replicas, at least two Python worker processes, and telemetry.
- Pin supported server/client versions, images, dependency locks, and schema versions in DUR-001. Do not select versions from an unverified assumption.
- Start with 16 logical scheduler partitions. Map each workflow permanently with a versioned, explicitly specified stable hash; do not use a language/runtime hash whose output can change. DUR-002 freezes the algorithm, partition-map version, and test vectors. Changing the algorithm or count requires migration and is deferred.
- Kafka partitioning and PostgreSQL logical ownership are independent. Kafka consumer-group assignment does not grant scheduler ownership.
- Start with separate task and event topics. Key task records by workflow ID; make correctness independent of cross-topic delivery order.
- Worker claims, heartbeats, checkpoints, and results go through authenticated internal Go APIs. Workers do not receive broad database credentials.
- Control APIs can accept valid worker results on any replica. The current scheduler later advances the workflow.
- Limit request sizes, activity input/output sizes, outstanding workflows, worker concurrency, outbox batches, connection pools, and retry budgets.
- Use HTTP/JSON for control APIs and a Python client initially. gRPC, a graphical editor, and a general SDK ecosystem are deferred.
- CPU-only correctness and load tests come first. A live LLM is unnecessary for engine development.

## 4. Guarantees and failure model

### Guarantee vocabulary

| Boundary | Target guarantee | Limit |
|---|---|---|
| Workflow submission | A repeated client idempotency key with identical canonical input returns the same workflow. | A different payload with that key is a conflict; the retention contract must be stated. |
| PostgreSQL state | Each accepted transition is atomic, version-checked, and attributable to an authorized actor. | Durability depends on the declared PostgreSQL storage and commit configuration. |
| Scheduler ownership | A previous owner cannot commit an owner-authorized transition after a newer owner has committed takeover. | Lease expiry alone cannot stop a paused process or an external action. |
| Task dispatch | Durable pending work is eventually dispatchable after recovery, under the stated availability assumptions. | Kafka delivery and relay publication may repeat; finite retention requires reconciliation. |
| Worker attempts | Only the current accepted attempt/claim may add progress or a new result. | A timed-out worker may still be running outside the engine. |
| Cooperating external sink | Repeated requests for one logical effect reuse one result under the sink's atomic idempotency contract. | This is not a guarantee for arbitrary APIs. |
| Non-cooperating sink | Ambiguous outcomes become reconciliation-required; automatic retries stop. | The runtime cannot infer whether an unobservable external effect happened. |
| Workflow completion | The engine reports one accepted terminal workflow outcome. | An activity can execute more than once; terminal cancellation cannot undo earlier effects. |

Kafka transactions do not by themselves make a PostgreSQL write or an arbitrary tool action exactly once. Use the database transaction/outbox boundary and explicit endpoint cooperation. [Kafka delivery semantics](https://kafka.apache.org/41/design/design/)

### Declared fault model

Required tests cover process crash/restart, long process pause, duplicate/delayed/reordered messages, lost responses, consumer rebalance, temporary PostgreSQL/Kafka unavailability, and worker/control-network separation.

Assume:

- a single authoritative PostgreSQL primary, with acknowledged commits preserved in the tested configuration;
- non-Byzantine engine components;
- bounded transaction duration and eventual restoration of the services needed for progress;
- sufficient retained database state to reconcile unfinished work;
- the cooperating sandbox service honors its documented transactional contract.

Pin and record PostgreSQL durability settings. Do not disable durability to improve benchmarks.

A single local database and broker do not demonstrate storage replication, disk-loss survival, automatic database failover, or host-level availability. Those claims require separate deployments and tests. Clock jumps can affect lease timing; database serialization/fencing remains the safety boundary, while timing conclusions must state clock assumptions.

During database unavailability, reject new durable acceptance and new worker claims. Existing external calls can finish with uncertain outcomes; reconcile them after recovery. During Kafka unavailability, accept only within the configured durable backlog limit and report delayed execution rather than silently dropping work.

## 5. Durable state, identities, and versioning

### Minimum data model

| Entity | Required information |
|---|---|
| Workflow definition | Immutable version/hash, graph, supported activity versions, limits. |
| Workflow execution | ID, namespace, submission key/hash, definition version, logical partition, state, revision, timestamps. |
| Node instance | Stable workflow/node/iteration identity, dependencies, inputs, accepted result, retry and deadline state. |
| Activity attempt | Monotonic attempt number, dispatch state, worker claim token/identity, heartbeat deadline, terminal outcome. |
| Partition lease | Partition ID, owner process-incarnation UUID, monotonic epoch, database lease expiry. |
| Timer | Durable due time, purpose, consumed state, owning workflow/node revision. |
| Checkpoint | Logical node ID, schema version, sequence, source attempt, validated payload/hash. |
| Transition history | Unique transition ID, workflow revision, actor, relevant epoch/attempt, old/new state and reason. |
| Outbox | Stable event ID, aggregate/version, payload schema, publish state, retry and relay-claim metadata. |
| Event inbox | Consumer identity, event ID, accepted/rejected disposition and durable reconciliation reference. |
| Approval/action intent | Canonical proposed action/hash, target, decision, approver, validity window, dispatch status. |
| Effect record | Stable effect key, argument hash, attempt/grant metadata, receipt or unknown outcome. |

Use unique constraints for identities that must be unique and compare-and-set or locked transitions for legal state changes. JSON payloads do not replace relational constraints on ownership, identity, or lifecycle.

### Keep these identities separate

1. **Client submission key:** deduplicates workflow creation.
2. **Logical activity/effect key:** stable across retries of that logical operation.
3. **Attempt number and claim token:** change when the engine authorizes a replacement attempt.
4. **Scheduler epoch:** identifies a partition-ownership generation.
5. **Message/event ID:** identifies one durable transport record.

A repeated Kafka record is not permission to create a new attempt. A new retry attempt is not permission to invent a new idempotency key for the same effect.

### Workflow and checkpoint compatibility

Persist definition, activity, and checkpoint schema versions. Existing workflows keep their original definition. Unsupported versions must pause with an explicit reason.

Checkpoints represent committed progress in a specific activity, such as the next chunk index and accumulated artifact hash. A new attempt may restore the latest compatible checkpoint; the superseded attempt cannot overwrite it.

Keep bounded inline JSON/artifacts initially. If large external artifacts are added, persist durable content plus checksums before committing references and document cleanup. Retention must cover the retry/redelivery/reconciliation horizon; do not delete deduplication state while related work can still resume.

## 6. Scheduler leases and fencing

### Ownership protocol

- Acquire an unowned/expired partition in a short transaction.
- Lock the partition lease row, use database time, and increment its epoch on acquisition.
- Renew only when owner UUID, epoch, and unexpired ownership still match. An expired owner reacquires through the acquisition path and receives a new epoch.
- Every scheduler-authorized state change locks and validates the lease row in the same transaction as the affected workflow changes.
- Validate owner/epoch/expiry after acquiring the lock, then apply version-checked changes and insert history/outbox rows atomically.
- Use a consistent lock order: lease, workflow, node/attempt, then subordinate records.
- Set transaction/lock timeouts, bound batch sizes, and retry eligible database conflicts without repeating external I/O.
- Never hold a database transaction open while publishing to Kafka, calling a tool, or awaiting human input.

PostgreSQL row locks are held until transaction end and serialize competing writes. This is the basis for the takeover boundary, not an in-memory lease check. [PostgreSQL row locking](https://www.postgresql.org/docs/current/explicit-locking.html)

Define the guarantee at the committed takeover boundary. A transaction that validated ownership before expiry may finish before a blocked takeover obtains the row; do not claim that wall-clock expiry instantaneously cancels it. A resumed old process attempting a new transaction after takeover must be rejected.

Use database time for lease and timer comparisons. Account for the difference between transaction-start time and an actual current-time sample; choose the expression deliberately and test lock-wait behavior. [PostgreSQL time functions](https://www.postgresql.org/docs/current/functions-datetime.html)

### Partition distribution

Use bounded acquisition and an explicit cooperative balancing policy so one replica does not retain every partition indefinitely. Process restarts use new incarnation UUIDs. Orderly transfer stops new owner-authorized scheduling before release.

Keep valid worker attempts running across scheduler handoff. A scheduler epoch change does not automatically invalidate an activity that PostgreSQL still considers current. Worker completion is checked against activity attempt state, not merely the epoch copied into an old dispatch message.

### Worker-attempt fencing

The claim API atomically verifies that an attempt is dispatchable and grants one worker a unique claim token. Claim retries use a stable worker request ID and return the existing grant when appropriate.

Heartbeat, checkpoint, and result operations validate the claim token, current attempt, workflow state, and documented deadline policy. Timeouts/replacement and completion lock the same relevant rows, so their race has one recorded winner.

After timeout and replacement, a late result cannot change workflow state. A retry of a result that already committed returns its durable receipt even if the response was lost. Distinguish this receipt lookup from accepting new progress by an expired claim.

Scheduler fencing, worker fencing, and external sink fencing are different mechanisms. Document which mutation each one protects.

## 7. Kafka dispatch, events, and reconciliation

### Transactional outbox

Creating dispatchable work and its outbox record happens in the same PostgreSQL transaction. The normal relay path uses PostgreSQL `LISTEN/NOTIFY` as a low-latency wake-up hint when new outbox work commits, plus a bounded fallback poll so notification loss or relay reconnect cannot strand durable work. The notification is not the durable record and grants no correctness guarantee by itself. Multiple relays claim bounded batches, publish outside the state transaction, wait for broker acknowledgment, and record publication. Freeze and record the fallback-poll interval before RQ6 measurements.

A crash after broker acknowledgment but before marking publication creates a duplicate. Preserve event identity and make consumers safe under repetition. An idempotent Kafka producer may reduce some duplicates; it does not eliminate the outbox crash window.

Relay claims need expiry and recovery. Use `SKIP LOCKED` only for suitable queue-like selection; it is not a consistent snapshot or a replacement for workflow invariants. [PostgreSQL SELECT locking clauses](https://www.postgresql.org/docs/current/sql-select.html)

### Worker task consumption

1. Receive a task notification.
2. Ask the control API to claim its persisted attempt.
3. Execute only after a valid durable grant.
4. Commit the Kafka offset after the notification has a durable disposition: granted, already handled, stale, or durably quarantined.
5. Run work in a bounded execution pool and maintain heartbeats independently of Kafka polling.
6. Persist checkpoint/result through the control API.

Do not commit past unfinished lower offsets on the same Kafka partition. Implement a contiguous acknowledgment watermark or keep processing/acknowledgment serial until a correct concurrent implementation is justified.

Acknowledging a notification before activity completion is safe only because PostgreSQL retains the claimed attempt and its recovery deadline. A worker crash must be detected from durable attempt state; Kafka redelivery is not the only recovery mechanism.

### Durable events

Accepted worker results are stored in PostgreSQL together with an outbox completion event. Event ingestors persist a deduplicated workflow wake-up before acknowledging Kafka.

A wake-up may reach a replica that does not own the workflow's logical partition. It records the hint; the current owner handles the state transition. Never depend on Kafka assigning that event to the lease owner.

Periodic bounded scans of PostgreSQL recover missed wake-ups, due timers, expired attempts, and dispatchable work. Workflow-specific reconciliation is scoped to logical partitions currently owned by that scheduler; a scan does not grant authority to transition workflows outside the owner's fenced boundary. Global queue-maintenance scans, if any, use bounded claims and still cannot bypass workflow ownership checks. Polling reconciles authoritative state; it does not directly execute activities around the Kafka path.

If a dispatch notification is lost through retention or an acknowledged message is followed by worker death, reconciliation re-notifies still-dispatchable work or schedules the appropriate new attempt. Redispatching an unclaimed attempt preserves its identity.

Bound and monitor backlog age, not just record count. Quarantine malformed/unsupported records durably and link them to affected work. A dead-letter topic alone does not resolve a workflow.

## 8. Retries, checkpoints, external effects, and approvals

### Retries and deadlines

- Classify retryable, permanent, canceled, and outcome-unknown failures.
- Persist attempt count, selected jitter/backoff, next due time, total deadline, and retry budget.
- No process sleeps are the authoritative retry/timer state.
- A timeout says the engine stopped accepting progress; it does not prove the worker stopped.
- Retry pure work and cooperating idempotent calls according to policy. An ambiguous non-idempotent effect requires reconciliation.
- Bound all investigation/tool/model loops and account for possibly repeated provider charges.

### Checkpoint boundary

Commit checkpoint progress only after the corresponding work is actually durable. For pure chunked computation, restart from the latest committed chunk. For an external mutation, a checkpoint alone does not make the mutation atomic; use an effect receipt/idempotency contract.

Committed LLM outputs and tool results are reused during recovery. A provider response received only in worker memory can be lost and the provider call can repeat. Do not promise identical regenerated text or exactly one billable request.

### Cooperating sandbox effect service

Build a separate service with its own database/ledger and no cross-database transaction with the engine. A second database on the same local PostgreSQL server is acceptable for the API atomicity-gap test; label the shared failure domain.

Support one small set of synthetic operations, such as restoring a fixture service's previous configuration revision. The endpoint must:

- accept a stable logical effect key and canonical argument hash;
- atomically apply its sandbox-state change and store the effect receipt;
- return the previous receipt for the same key/payload;
- reject key reuse with different arguments;
- support receipt/status lookup;
- record every call attempt separately from actual applied mutations.

Study sink-side fencing in this controlled service using a monotonic token scoped to the protected resource. Allocate tokens durably; never compare unrelated partition epochs as if they ordered all resource mutations. The sink rejects a lower token after observing a higher token.

A sink cannot reject a newly obsolete token it has not yet learned about. Fencing does not instantly revoke an already issued request. Stable-key deduplication handles retries of one effect; approval validity and resource-version preconditions handle separate concerns.

Keep external targets isolated per test workflow by default. Concurrent remediation of the same resource by different workflows is deferred unless its conflict policy is explicitly added.

Idempotency includes the meaning of the request and how long keys/results remain available, not merely generating a UUID. [AWS guidance on idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)

### Non-cooperating endpoint

Include a deliberately non-idempotent test endpoint with no reliable lookup contract. Drop the response after applying a change.

The expected engine outcome is `RECONCILIATION_REQUIRED`, with the ambiguity visible. Do not retry automatically, claim failure means no effect, or report unknown effects as zero duplicates. An authenticated operator may attach independently verified evidence and resolve or abandon the workflow through an audited API; resuming an effect requires a documented safe basis. The test controller may inspect the endpoint's private ledger for scoring, but the runtime must not receive a lookup capability the endpoint contract does not provide.

### Approval contract

- Persist a proposed action with workflow/node identity, exact target, canonical arguments, expected resource revision, and expiry.
- A separate authenticated approver accepts or rejects that exact proposal. Model output and tool text cannot approve an action.
- Approval waits hold no database lock and consume no worker execution slot.
- Changed arguments/target create a new proposal requiring a new decision.
- Before dispatching a remediation, atomically validate and record the approved intent and its execution grant.
- The sandbox service validates a signed, bounded grant covering the action hash and stable effect key, and checks the expected resource version.
- Approval expiry prevents a new mutation; retrieving a receipt for an already applied mutation remains possible.
- Duplicate approval submissions are idempotent. Reject unauthorized, missing, expired, or mismatched approval.
- Cancellation before the durable dispatch grant prevents dispatch. After a grant/request has been issued, cancellation is best effort and cannot undo an applied effect. Record this boundary explicitly.

The core safety claim is that no new remediation is dispatched without a matching approval and no cooperating sink applies the same logical effect twice under its contract. Real production remediation is outside this project's default scope.

## 9. Incident-investigation agent

### Demonstration environment

Create synthetic services and a versioned incident corpus with structured logs, time-series metric snapshots, runbooks, and hidden ground truth.

Use five incident families:

1. bad configuration/deployment revision;
2. exhausted connection pool;
3. downstream latency or errors;
4. disk-pressure scenario;
5. benign/transient symptoms or insufficient evidence requiring restraint.

Provide 30 cases: 10 development cases and 20 held-out evaluation cases, balanced by family. Freeze the held-out inputs and scoring rules before tuning final prompts. Ground-truth labels must not be exposed through tool responses.

Before freezing the held-out set, perform a difficulty audit. Cases must contain plausible distractor evidence; some must require combining more than one source or using the bounded second investigation round; restraint cases must share superficial symptoms with actionable families. Reject fixtures whose answer is directly exposed by one log line, metric label, filename, or runbook sentence. Record the audit rather than using a high success rate as proof that the corpus is meaningful.

Use local tool services for logs/metrics/runbooks. Adding hosted observability connectors, a vector database, or a separate search platform is not required.

### Workflow

1. Accept incident and persist its inputs.
2. Gather bounded logs, metrics, and relevant runbook excerpts.
3. Persist a structured diagnosis, cited evidence IDs, uncertainty, and proposed next step.
4. Permit a bounded additional investigation round when evidence is insufficient.
5. Either conclude without remediation or create the exact approval proposal.
6. Wait for an authorized human decision.
7. Execute the approved sandbox operation through the effect contract.
8. Verify service state and persist a final incident report.

Record tool/model versions, prompts or retrievable prompt hashes, inputs, outputs, token usage where available, latency, call counts, and effect/approval references. Keep credentials outside workflow payloads.

Treat logs and runbooks as untrusted data. A document saying to ignore instructions or approve an action has no authority. Tools use allowlisted operations and validated arguments; the model does not receive unrestricted shell access.

### Deterministic mode and live-model mode

- **Deterministic mode:** scripted or recorded, versioned activity outputs for engine tests and repeatable recovery comparisons. Label these as fixtures.
- **Live-model mode:** one pinned available model configuration through a provider adapter, enabled only with credentials and an approved spending limit.
- Persist nondeterministic outputs as activity results. Temperature settings are not a guarantee of reproducibility.
- A live run that crashes after a committed model result must reuse that result. A crash before the commit can incur another call.
- Do not connect Project 1 by default. A later integration can use its HTTP endpoint without changing the engine's correctness claims.

Deliver a CLI walkthrough and readable execution timeline. A custom web application is deferred.

## 10. Releases, schedule, and resource envelope

### Releases

**Core Engine MVP:** two schedulers and worker processes, PostgreSQL ownership/state, Kafka task/event paths, retries/checkpoints, fenced recovery, cooperating and non-cooperating effect demonstrations, approval gates, the engine-only F01-F11 correctness campaign, and a small throughput smoke report. This release does not depend on the incident agent.

**Portfolio MVP:** Core Engine MVP plus the deterministic incident workflow, observability/timeline, agent-specific F12 checks, and a human-approval walkthrough.

Both MVP levels must exercise real local PostgreSQL and Kafka. Mock-only tests or a happy-path demo do not satisfy them.

**Full v1:** Portfolio MVP plus frozen throughput, safeguard-cost, dispatch-path decomposition, lease, and checkpoint studies; the bounded live-model incident evaluation; a reproducible report; and interview evidence.

A required study not run is **not evaluated**. A study that ran but cannot settle its question is **inconclusive**. A release missing full-v1 requirements remains the highest completed MVP/partial release unless the user explicitly revises scope.

### Budget and deployment

- No GPU, EKS, managed Kafka, or managed PostgreSQL is required for the default local release.
- Project 1's proposed $100 pilot budget does not authorize spending on this project.
- Paid model-call and cloud-compute caps are unset. Obtain a bounded cap before using either.
- Before paid work, record current prices, planned volume/duration, maximum spend, stop conditions, and any remaining billable resources.
- Use measured development runs to estimate cost; provider retries and ambiguous responses can still incur charges.
- Separate agent coding subscriptions/API usage from application benchmark costs.
- An optional multi-host CPU deployment may validate network placement or host-failure claims. Record host/resource topology and authorize its cost separately.
- Local multi-process results must be labeled as sharing one host. Do not present them as an infrastructure-HA result.
- Final I/O-sensitive throughput, safeguard, dispatch-path, lease, and checkpoint comparisons should run on the native Linux or clearly specified Linux VM/volume declared and validated in DUR-036, with recorded filesystem and storage settings. Docker Desktop/WSL2 results are development evidence unless the same conclusion is reproduced on the declared measurement host; virtualized filesystem behavior must not be mistaken for engine behavior.

### Provisional checkpoints

| Checkpoint | Intended outcome |
|---|---|
| 1 | Contracts, schemas, fixture harness, repeatable development setup |
| 2 | Durable state, submission idempotency, leases, worker claims |
| 3 | Kafka task/event transport and database reconciliation |
| 4 | Retries/checkpoints, effects, approvals |
| 5 | Engine-only correctness campaign and Core Engine MVP |
| 6 | Incident demo, agent-specific correctness, Portfolio MVP |
| 7 | Throughput, safeguard-cost, dispatch-path, lease, checkpoint, and live-agent studies |
| 8 | Final report, clean reproduction, portfolio release |

These are provisional weekly checkpoints, not promised dates. Reforecast after the first working durable activity. Continuous coding-agent availability does not remove review and personal-understanding checkpoints.

**Deferred:** arbitrary-code replay, custom consensus, database automatic failover, multi-region execution, dynamic repartitioning, distributed transactions with arbitrary tools, automatic compensation/sagas, general multi-tenant billing, workflow UI, multiple model providers, production remediation, and direct integration with Project 1.

## 11. Roles and durable build records

- **Codex:** implement the selected task, run checks, record evidence and tradeoffs, and prepare `REVIEW.md`.
- **Claude Code:** independently review the fixed target commit when available, assess acceptance criteria and published claims, write findings, and verify fixes. Provisional working-tree reviews are allowed when commit history is unavailable or work is uncommitted, but they do not satisfy DONE.
- **User:** own design choices, approve paid scope, perform the human-approval demo, and build personal explanations of the engine.

At each task/fix cycle, read the applicable agent instructions, this plan, applicable contracts, and unresolved `REVIEW.md` findings. Take turns editing the shared review file.

**Lifecycle:** `TODO -> IN_PROGRESS -> READY_FOR_REVIEW -> DONE`. A task may receive provisional review while `IN_PROGRESS`, but `READY_FOR_REVIEW` requires the authorized target work to be committed so Claude has a fixed target.

DONE requires acceptance criteria, relevant passing checks, evidence, and a completed COMMITTED, non-provisional Claude review whose target is the final code commit and whose verdict is `NO_BLOCKING_FINDINGS`. Codex records proposed resolutions as ADDRESSED or DISPUTED. Claude independently reviews them and may set the finding to VERIFIED, WITHDRAWN, or OPEN as appropriate. Any unresolved findings permitted under that verdict must be documented; deferred P2 findings need a reason and linked follow-up, and remaining P3 findings must be stated. Missing reviews and unavailable tests are never passes. Codex moves the task to DONE only after these conditions are satisfied.

Review feedback needs stable IDs, severity, file/line, triggering scenario, evidence, and expected correction. Record the review basis and fixed base/target commits when available; preserve the original finding and append round-labeled Codex responses and Claude verifications rather than overwriting history.

| File | Required contents |
|---|---|
| `README.md` | Measured findings, architecture, quickstart, limits, reproduction |
| `docs/BUILD_LOG.md` | What changed, why, tests/results, failed approaches, lessons, commits |
| `docs/DECISIONS.md` | Alternatives, tradeoffs, guarantee/scope changes, reconsideration conditions |
| `docs/CONTRACTS.md` | State machines, actor permissions, invariants, failure model, cancellation |
| `docs/PROTOCOLS.md` | API/message schemas, lease/claim/outbox/approval protocols, identities |
| `docs/BENCHMARKS.md` | Frozen questions, workloads, numerical targets, run registry, calculations |
| `docs/RUNBOOK.md` | Start/stop, migrations, backlog investigation, recovery, retained resources |
| `docs/INTERVIEW_NOTES.md` | Personal walkthroughs and claim-to-evidence links |
| `experiments/<run-id>/` | Manifest, seeds, commands, fault schedule, raw traces, ledger, analysis |

For each meaningful change, explain what was built, the alternative rejected, why, what was tested, the result, the known limitation, and how to reproduce it. Record surprising failures rather than only successful milestones.

Large artifacts can live outside Git with checksums and retrieval instructions. Do not store secrets or fabricate personal authorship/understanding.

## 12. Milestones and tasks

All tasks are TODO. Later tasks are expanded with exact commands, fixtures, and evidence paths immediately before implementation. Add named subtasks when a task exceeds one focused implementation/review cycle; retain its parent ID.

### M0 — Foundation

#### DUR-001 — Repository and reproducible toolchain

- **Status:** TODO.
- **Dependencies:** None.
- **Goal:** A clean checkout supports Go/Python development and starts the real local dependencies.
- **Scope:** Agent/review files; pinned toolchains/images; dependency locks; configuration examples; Compose; migrations entry point; setup guide.
- **Layout:** `cmd/runtime/`, `internal/`, `api/`, `migrations/`, `python/workers/`, `python/incident_agent/`, `tests/integration/`, `tests/faults/`, `deploy/local/`, `docs/`, `experiments/`.
- **Acceptance:** Go builds; Python installs reproducibly; PostgreSQL/Kafka health checks pass; credentials stay out of version control; stop/start behavior is documented.
- **Validation:** Clean bootstrap, build/import checks, durable dependency restart smoke checks. No directory-layout tests.
- **Evidence:** Versions, commands, results, first build-log entry.

#### DUR-002 — Contracts and invariant catalogue

- **Status:** TODO.
- **Dependencies:** DUR-001.
- **Goal:** Define exactly what each actor can change before implementing concurrency and freeze the workflow-to-partition mapping contract required by M2.
- **Scope:** Workflow/attempt state diagrams; identity scopes; transition permissions; lease and worker-fencing boundaries; acknowledgment rules; failure model; versioned stable workflow-to-partition hash algorithm; partition-map version; canonical test vectors.
- **Acceptance:** Walk through submission retry, ownership handoff, worker timeout/result race, outbox duplicate, ambiguous effect, and approval/cancellation race with one explicit outcome per ordering. The workflow-to-partition algorithm, map version, and test vectors are explicit, deterministic across supported Go/Python implementations, and frozen before partition ownership work begins.
- **Validation:** Paper traces include transaction boundaries and the location of every durable record; contradictions become blocking findings. Run the frozen partition-map test vectors across every implementation that computes or verifies the mapping and reject runtime/language hashes whose outputs are not part of the declared contract.
- **Evidence:** Contracts, protocol diagrams, partition-map specification/test vectors, cross-implementation vector results, and initial decision records.

#### DUR-003 — Fixtures and test-control interface

- **Status:** TODO.
- **Dependencies:** DUR-002.
- **Goal:** Make failures reproducible before the engine grows.
- **Scope:** Seeded workflow fixtures, fake activities, named barrier/failpoint API, controller process, trace schema.
- **Acceptance:** The controller can pause, release, and kill a target after a reported boundary without relying on arbitrary sleep durations.
- **Validation:** Repeat one controlled crash with the same seed; confirm the trace identifies the intended boundary and reports a timeout if never reached.
- **Evidence:** Example fault schedule and raw trace.

#### DUR-004 — Shared validation and CI

- **Status:** TODO.
- **Dependencies:** DUR-001, DUR-003.
- **Goal:** Local and CI checks use the same entry points.
- **Scope:** Format/lint/build; Go unit and relevant race checks; Python checks; real database/Kafka integration; small fault smoke suite.
- **Acceptance:** Failed checks propagate; paid/model tests are opt-in; unimplemented or unavailable checks are explicit.
- **Validation:** Execute existing commands and demonstrate failure reporting. Remote CI is pending until a remote exists.
- **Evidence:** Commands and CI results where available.

### M1 — Durable workflow core

**Dependencies:** M0.

| Task | Scope and acceptance |
|---|---|
| DUR-005 — Schema and state repository | Migrations, constraints, revisions, transition history. Invalid transitions and conflicting writes fail without partial state. Test real concurrent PostgreSQL transactions and rollback. |
| DUR-006 — Submission and query APIs | Idempotent create, payload conflict, status/history. Drop the response after commit and retry; observe one workflow. Define retention and ambiguous client outcome behavior. |
| DUR-007 — Interpreter, timers, and joins | Execute a small versioned graph using persisted results; support durable timer and bounded fan-out/join. Restart between nodes and race final branch completions; downstream work is created once. |
| DUR-023A — Independent invariant checker skeleton | Implement an independent trace/state checker before the engine grows. In M1 it checks legal revision sequences, terminal-state monotonicity, unique accepted node results, and submission identity. Seed invalid traces and prove they fail without calling production transition validators. Extend the checker through the named DUR-023A-M2, DUR-023A-M3, and DUR-023A-M4 subtasks in later milestones. Mark DUR-023A complete when the M1 checker contract and seeded-negative tests pass. |

**Exit:** A database-backed graph can resume from committed state, and the independent checker can detect seeded M1 violations. A test-only activity driver is acceptable until Kafka integration; it is not the final dispatch path.

### M2 — Multi-replica ownership and worker attempts

**Dependencies:** M1.

| Task | Scope and acceptance |
|---|---|
| DUR-008 — Partition leases and scheduler fencing | Acquisition, renewal, balancing, release, epoch-checked transactions. Pause owner A, allow B to take over, resume A; A's new owner-authorized writes fail. Test lock-wait/expiry ordering. |
| DUR-009 — Claim and result APIs | Atomic worker claims, idempotent claim/result retries, attempt-token validation, result receipts. Duplicate claims grant only one active claimant; stale completion cannot overwrite newer state. |
| DUR-010 — Python activity runner | Versioned activity registry, bounded concurrency, heartbeat/control client, pure deterministic fixture. Process restart and lost claim response have documented behavior. |
| DUR-023A-M2 — Invariant-checker ownership/attempt extension | Extend the DUR-023A checker with scheduler-epoch monotonicity, ownership-authority checks, attempt/claim generation rules, and stale-result detection. Seed at least one invalid old-owner write and stale worker result. Acceptance requires the checker to reject those seeded violations without calling production transition validators; record fixtures and checker evidence under this subtask. |

**Exit:** Two schedulers own disjoint logical partitions, valid in-flight attempts survive scheduler handoff, and the checker rejects seeded ownership/attempt violations. Direct driver dispatch remains test-only.

### M3 — Kafka and reconciliation

**Dependencies:** M2.

| Task | Scope and acceptance |
|---|---|
| DUR-011 — Outbox and relay | Atomically create dispatch/result events, recover relay claims, retry publication. Crash after broker acknowledgment; duplicate publication is visible and harmless. |
| DUR-012 — Task consumer and acknowledgment | Kafka worker adapter, durable disposition, bounded execution pool, contiguous offset commits. Rebalance or kill the consumer after claim; accepted work remains recoverable. |
| DUR-013 — Event inbox and scheduler wake-ups | Deduplicated event ingestion and persisted hints independent of consumer/lease placement. Reordered/duplicate completion events cannot repeat transitions. |
| DUR-014 — Database reconciliation and backpressure | Recover expired attempts, lost wake-ups, pending dispatch, due timers, poison records, and backlog limits. Workflow-specific scans are partition-owner scoped. Remove a notification in an isolated test and prove unfinished database work is redispatched. |
| DUR-023A-M3 — Invariant-checker transport/reconciliation extension | Extend the DUR-023A checker with outbox/inbox identity reconciliation, duplicate-message dispositions, pending-obligation checks, and detection of a state change that lacks its required durable outbox record. Seed representative transport/reconciliation violations and require independent checker rejection; record fixtures and evidence under this subtask. |

**Exit:** Normal task and event dispatch uses real Kafka. PostgreSQL alone identifies every unfinished obligation after recovery, and the checker rejects seeded transport/reconciliation violations.

### M4 — Recovery semantics, effects, and approvals

**Dependencies:** M3.

| Task | Scope and acceptance |
|---|---|
| DUR-015 — Retry policy and checkpoints | Persist retry timing/budgets and compatible checkpoint progress; reject stale checkpoint updates. Crash a chunked activity and verify output plus measured repeated work. |
| DUR-016 — Cancellation and ambiguous outcomes | Race cancellation, completion, and timeout; persist one accepted ordering. Unsafe retries stop in reconciliation-required state; audited operator resolution records its evidence. Cancellation never claims to undo an effect. |
| DUR-017 — Sandbox effect service | Independent transactional effect/receipt ledger, argument conflict, status lookup, and scoped fencing. Crash after mutation but before engine receipt; recovery applies no duplicate protected effect. Include the non-cooperating control endpoint. |
| DUR-018 — Approval gates | Persist exact proposals and approver decisions, bounded grants, version preconditions, duplicate handling. Missing/altered/expired approval cannot cause a new mutation; waiting survives full control-process restart. |
| DUR-023A-M4 — Invariant-checker checkpoint/effect/approval extension | Extend the DUR-023A checker with checkpoint monotonicity, engine-to-effect-ledger reconciliation, unknown-effect classification, and approval/action matching. Seed duplicate effects, mismatched approvals, and stale checkpoints; require independent checker rejection and record fixtures/evidence under this subtask. |

**Exit:** Demonstrate both a safely retried cooperating effect and an explicitly unresolved non-cooperating effect. Produce a transaction-boundary diagram for each; the checker detects seeded M4 violations.

### M5 — Engine correctness campaign

**Dependencies:** M4. The incident agent is not a dependency. Relevant failpoints and checker obligations should already exist from earlier milestones.

| Task | Scope and acceptance |
|---|---|
| DUR-022 — Complete fault controller | Named boundaries, process kill/pause, network proxy cuts, message manipulation, deterministic schedules, bounded cleanup. Persist what actually happened, not only the requested fault. |
| DUR-023B — Finalize and mutation-test invariant checker | Starting from the completed DUR-023A skeleton and the named DUR-023A-M2/M3/M4 extensions, integrate all M1-M4 obligations, reconcile controller input, engine history/state, and sandbox ledger, then seed known-invalid traces for every major obligation. The checker must remain independent of production transition validation. DUR-023B has its own completion state and does not retroactively redefine DUR-023A. |
| DUR-024 — Engine boundary matrix | Execute F01-F11 from section 13, including both sides of race orderings where applicable. Resolve invariant violations before performance claims. F12 is agent-specific and runs in M6 under DUR-033. |
| DUR-025 — Real dependency outages and Core Engine MVP smoke | PostgreSQL restart/unavailability, Kafka restart/unavailability, rebalance, worker/control partition, whole-process restart with retained volumes. Recovery and unresolved work are reported separately. Finish with a bounded two-scheduler throughput smoke run; label it preliminary rather than a final performance study. |
| DUR-021A — Engine telemetry prerequisite | Instrument traces/metrics needed by later engine measurements: durable-readiness and accepted-claim timestamps, scheduler/lease activity, DB transaction/query/lock metrics, reconciliation activity, outbox/relay/Kafka timing, backlog age, worker utilization, and bounded resource labels. Validate that one engine execution and one fault episode can be reconstructed from durable evidence plus telemetry. This task must be DONE before any final M7 engine measurement begins. |

**Exit:** Core Engine MVP evidence is complete, and DUR-021A provides validated engine telemetry sufficient for later measurements. Unavailable infrastructure tests remain visibly pending; do not substitute a mock exception for an actual restart claim.

### M6 — Incident workflow, observability, and agent-specific correctness

**Dependencies:** M5 for Portfolio MVP completion. DUR-019 and DUR-020 may begin after M4, but they cannot weaken or delay the engine correctness gate. DUR-021B requires DUR-021A. DUR-033 requires DUR-019, DUR-020, DUR-022, and the relevant M4 effect/approval contracts before executing F12 and continuity checks.

| Task | Scope and acceptance |
|---|---|
| DUR-019 — Incident fixtures and tool adapters | Versioned logs/metrics/runbooks, ground truth, read-only tools, schema validation, prompt-injection fixtures, and the difficulty audit from section 9. Tools expose only authorized synthetic data. |
| DUR-020 — Durable investigation workflow | Evidence collection, bounded model/tool steps, persisted diagnosis, approval wait, sandbox action, verification, final report. Deterministic mode works end-to-end; live mode is budget-gated. |
| DUR-021B — Incident timeline and agent observability | Depends on DUR-021A. Extend the engine telemetry foundation with incident/model/tool/approval spans and a readable workflow timeline. One incident execution can be reconstructed without inferring success from logs alone. |
| DUR-033 — Agent-specific correctness and continuity | Depends on DUR-019, DUR-020, DUR-022, and the relevant M4 effect/approval contracts. Execute F12 plus deterministic interrupted/uninterrupted continuity checks using frozen recorded/scripted activity outputs. A committed deterministic investigation interrupted after a declared boundary must resume to the same durable report and action/no-action decision as its matched uninterrupted run, or the case fails. The equality assertion is only over the durable report content and final action/no-action decision (plus explicitly declared committed semantic outputs); transition history, attempt count, retry records, timestamps, and timing are expected to differ after interruption and are not required to match. |

**Exit:** Portfolio MVP evidence is complete: a human can inspect, approve, interrupt, and resume a synthetic incident investigation, and agent-specific safety/continuity cases are independently checked.

### M7 — Controlled measurements

**Dependencies:** M5. DUR-036 is the readiness gate for the final engine measurement studies DUR-026, DUR-027, DUR-028, DUR-034, and DUR-035. DUR-029 additionally depends on M6/DUR-021B and uses its separately frozen live-model budget/protocol. No final throughput, safeguard, dispatch-path, lease, or checkpoint run starts before DUR-036 is DONE.

| Task | Scope and acceptance |
|---|---|
| DUR-036 — Declared Linux measurement host readiness | Depends on M5/DUR-021A. Declare and, if necessary, provision the native Linux host or explicitly specified Linux VM/volume used for final I/O-sensitive studies. Record CPU/memory, OS/kernel, filesystem/volume type, container/runtime versions, PostgreSQL/Kafka placement and durability settings, clock/timestamp assumptions, resource limits, and paid cost/teardown controls when applicable. From a clean checkout, deploy the real dependencies, run a bounded smoke workload, and validate that the DUR-021A telemetry can reconstruct one normal execution and one fault episode on this host. Preserve setup/reproduction evidence. Must be DONE before DUR-026, DUR-027, DUR-028, DUR-034, or DUR-035 final runs. |
| DUR-026 — Throughput protocol and core runs | Depends on DUR-036. Freeze workloads, arrival rates, capacity, durations, SLOs and seeds; execute the reduced scheduler-count study in section 14; reconcile all submitted/accepted/terminal work. Report scheduler CPU-seconds per completed workflow. |
| DUR-034 — Safeguard-cost ablation | Depends on DUR-036. Execute the test-profile ablations in section 14. Negative controls are test-only: pair history-disabled with audit-evidence loss, unsafe lease validation with the dedicated check-to-commit takeover failpoint, and no-outbox with measured reconciliation-delayed dispatch. Never present them as deployable alternatives. |
| DUR-035 — Three-arm dispatch-path decomposition | Depends on DUR-036. Implement and compare three testable paths that consume the same committed task-outbox record and end at the same worker claim API: (A) periodic PostgreSQL polling plus direct worker notification; (B) `LISTEN/NOTIFY`-woken direct notification with no broker; and (C) the production `LISTEN/NOTIFY`-woken outbox relay -> Kafka -> workers path. Use two frozen polling intervals so the default study stays at four configurations / twelve measured runs. Freeze fallback-poll intervals and the direct-dispatch worker-selection rule. Measure outbox-ready-to-claim delay, PostgreSQL load, direct-dispatch overhead, Kafka/broker overhead, backlog behavior, and terminal correctness. Interpret A->B as wake-mechanism effect and B->C as incremental transport effect. |
| DUR-027 — Lease tradeoff study | Depends on DUR-036. Compare matched lease settings under crashes/pauses; measure takeover, useful recovery, renewal traffic, false takeovers, and lock contention. |
| DUR-028 — Checkpoint tradeoff study | Depends on DUR-036. Compare frozen checkpoint intervals on one pure chunked workload; measure persistence overhead, repeated computation, correctness, and recovery. |
| DUR-029 — Live agent evaluation | Freeze held-out rubric and run the bounded live-model study; deterministic continuity is already a correctness obligation in DUR-033. Separate model quality, runtime recovery, approvals, and unknown effects. |

**Exit:** Each stated question has actual evidence and an appropriately limited verdict. Failed/incomplete runs remain in the registry.

### M8 — Report and portfolio release

**Dependencies:** Relevant MVP/full-v1 criteria.

| Task | Scope and acceptance |
|---|---|
| DUR-030 — Technical report and reference context | Publish failure traces, performance/recovery charts, safeguard and dispatch-path findings, scope limits, cost, and concise verified comparisons with current durable-execution/work-queue references. At minimum cover Temporal, DBOS, Restate, and River; add Hatchet only if its relevant architecture is verified during report work. No parity claims. |
| DUR-031 — Interview evidence and personal walkthroughs | Map each proposed resume claim to code and a reproducible artifact. User explains and changes one lease/attempt rule, walks through the ambiguous-effect case, and independently reproduces one result. |
| DUR-032 — Reproduction and final review | Clean-checkout demo, applicable checks, migrations/start/stop verification, final Claude review of code and claims; release/tag when authorized. |

**Exit:** Someone else can reproduce the bounded demo and inspect evidence supporting every published claim.

## 13. Deterministic failure campaign

A deterministic crash schedule specifies a named barrier and acknowledgment from the target process. It does not promise identical operating-system timing.

F01-F11 are the engine campaign required for the Core Engine MVP after M4. F12 is agent-specific and is executed after the incident workflow exists. Add the relevant checker obligation and regression fixture as soon as each mechanism is implemented; M5/M6 execute the full campaigns rather than inventing the oracle at the end.

For each applicable family, test explicit before/after orderings using at least three seeded fixture traces unless a deterministic binary continuity case specifies a stricter matched check. Some rows contain several atomic cases; enumerate case IDs before execution and report actual case/run counts.

| Family | Injection boundary | Required observation |
|---|---|---|
| F01 | Submission committed; API response lost | Retry returns the original workflow; no duplicate creation. |
| F02 | Workflow change/outbox creation before database commit | Rollback leaves neither half committed. |
| F03 | State/outbox committed; relay dies before publish | Pending work is published after recovery. |
| F04 | Kafka acknowledges publish; relay dies before marking sent | Repeated event identity produces no duplicate accepted transition. |
| F05 | Worker claim committed; process dies before/after Kafka offset acknowledgment | Attempt is recovered without relying solely on redelivery; no permanent stranded claim. |
| F06 | Sandbox effect committed; worker loses response or dies before saving receipt | Cooperating sink returns the original receipt; non-cooperating case becomes reconciliation-required. |
| F07 | Result/checkpoint committed; response or completion wake-up lost | Existing receipt/progress is recovered; the transition occurs once. |
| F08 | Old scheduler paused beyond expiry; new owner commits takeover; old owner resumes | Old owner-authorized writes are rejected; already valid worker work is not discarded solely due to handoff. |
| F09 | Worker loses heartbeats; replacement starts; old worker resumes | Old progress/result cannot overwrite the new attempt; sink fencing limits and deduplication are observed separately. |
| F10 | Timer/join becomes ready during scheduler crash or duplicated events | Each logical timer consumption/join transition creates downstream work once. |
| F11 | Cancellation or deadline races with completion/dispatch grant | Each ordering matches the contract; applied effects remain visible. |
| F12 | Approval wait/restart, duplicate decision, altered/expired proposal, injected tool instructions | Only a matching authorized decision permits a new action; malicious tool text cannot act as approval. |

For the unsafe-lease negative control in DUR-034, add a special F08-ablation barrier *inside* the vulnerable window: stale owner A performs its out-of-transaction owner/epoch check, then pauses before the mutation transaction commits; owner B is allowed to attempt takeover; then the controller releases the relevant barrier. Under the unsafe profile, B can commit takeover between A's stale check and A's mutation commit, exposing the stale-write failure. Under the normal row-locked protocol, do **not** require B to take over while A still holds the lease-row lock: the valid outcomes are (a) A commits its mutation before B's takeover can commit, or (b) A's transaction aborts/releases the lock, B commits takeover, and any subsequent old-owner operation from A is rejected. The invariant is: **no old-owner mutation may commit after the newer owner's takeover has committed.** Standard F08 remains required for the production path, and the controller must avoid waiting on an impossible lock ordering.

Also run dependency outages and message faults from DUR-025. Use finite cut windows and observe recovery after connectivity/service restoration.

### Checker obligations

- Every acknowledged submission maps to a durable workflow or a recorded violation.
- Workflow revisions and accepted transitions form a legal sequence.
- Ownership epochs and attempt generations never move backward.
- No previous-owner transition commits after the relevant takeover.
- Each logical node has at most one accepted successful result and each dependency transition is applied once.
- Expected effects, actual applied effects, call attempts, receipts, and engine outcomes reconcile.
- Approval records match the action actually requested/applied.
- Terminal failure/cancellation is not silently counted as successful completion.
- Pending work at the observation deadline is identified, not erased from the denominator.

Seed the checker with fabricated duplicate effects, missing transitions, stale writes, and mismatched approvals. A checker that always returns success is not evidence.

Negative-control implementations or disabled safeguards are confined to test profiles. Their failures demonstrate test sensitivity; their throughput is not a production alternative.

## 14. Benchmark protocol and metrics

### A. Engine throughput: 8 configurations, 24 measured runs

**2 scheduler counts × 2 workloads × 2 arrival rates × 3 repeats.**

- Scheduler counts: 1 and 2. A four-scheduler run is optional only if instrumentation shows scheduler capacity remains a meaningful bottleneck.
- Workload T1: short sequential workflows, initially eight pure activities per workflow.
- Workload T2: bounded fan-out/fan-in workflows, initially eight branches plus a join.
- Freeze exact step costs, graph sizes, payloads, and worker concurrency after pilots.
- Keep four worker processes, their total concurrency, PostgreSQL/Kafka capacity, logical partitions, and API/relay capacity fixed across scheduler counts.
- Calibrate below-saturation and near-saturation rates using the one-scheduler baseline, then apply the same rates to both scheduler counts for that workload.
- Record per-process and total CPU/memory allocations plus scheduler CPU-seconds per completed workflow. Adding a scheduler is a capacity change, not a fixed-total-CPU efficiency experiment.
- Use deterministic CPU/synthetic activities. Keep LLM latency out of the engine throughput study.

A fixed-rate study shows whether adding scheduler capacity helps in the tested regime and where another component becomes limiting; it does not establish maximum sustainable throughput or algorithmic speedup at constant resources.

### B. Safeguard-cost ablation: 4 configurations, 12 measured runs

**4 test profiles × 1 workload × 1 fixed near-saturation rate × 3 repeats.**

Use T1 with the same workers, database/broker capacity, payloads, and scheduler count. Compare:

1. **Full safeguards** — normal production-path history, row-locked owner validation on scheduler-authorized transitions, and transactional outbox.
2. **History disabled** — test-profile removal of append-only transition-history writes only; this measures audit/persistence cost and is not expected to preserve forensic evidence.
3. **Unsafe lease check** — test-profile owner/epoch check outside the row-locked mutation transaction instead of validating the lease row in the same transaction.
4. **Direct publish/no outbox** — test-profile Kafka publication without the transactional outbox boundary.

Negative-control profiles are never supported deployment modes. Run the no-fault throughput/latency comparison, then pair each control with the property it is intended to expose. For history-disabled, show the loss of independent transition/audit evidence rather than inventing a safety failure. For unsafe lease validation, use the dedicated check-to-commit failpoint: the unsafe profile must permit a schedule where B commits takeover after A's stale check but before A's mutation commit, while the normal row-locked protocol is judged by commit ordering—not by forcing A to lose. Valid normal outcomes are A-before-B, or A aborts/releases then B takes over and later A writes are rejected; no A mutation may commit after B's takeover. For direct publish/no outbox, do not claim a safety failure: crash after the database makes work dispatchable but before direct Kafka publication, then measure how long reconciliation takes to rediscover and dispatch that work. The expected loss is the outbox's bounded fast-path dispatch behavior, not durable correctness under the declared reconciliation assumptions.

Report throughput, completion latency, database transactions/queries/WAL where measurable, lock waits, history bytes, outbox work, and CPU. The useful claim is the measured cost of specific protections together with the concrete failure/audit property each protection buys.

### C. Dispatch-path decomposition — 4 configurations, 12 measured runs

**2 periodic-polling intervals + 1 notification-driven direct path + 1 notification-driven Kafka path × 1 workload × 1 fixed rate × 3 repeats.**

All four configurations begin from the **same committed task-outbox row** created with the workflow transition and end at the same authoritative worker claim API. Hold worker count/concurrency, workload, database configuration, outbox schema, claim semantics, and terminal correctness checks fixed. The outbox record is durable state; neither `NOTIFY`, a direct hint, nor a Kafka record grants execution authority by itself. The purpose is to separate **wake-up** from **transport** rather than compare confounded durable-source designs.

Compare:

1. **Periodic PostgreSQL polling + direct notification** at two frozen intervals selected after development measurements. A Go test-profile dispatcher polls pending task-outbox rows, claims a bounded batch, sends lightweight task hints directly to workers, and records delivery according to a frozen test-profile rule. Workers still obtain execution authority only through the normal claim API.
2. **`LISTEN/NOTIFY`-woken direct notification, no broker.** The same direct dispatcher consumes the same task-outbox rows and uses the same worker-notification/delivery rule, but a commit-time notification hint wakes it. A bounded fallback poll protects against missed notifications or reconnects.
3. **Production `LISTEN/NOTIFY` -> outbox relay -> Kafka -> workers.** The production relay consumes the same class of task-outbox rows after the same commit-time notification hint, publishes Kafka task notifications, and workers use the same claim API. A bounded fallback poll protects against notification loss/reconnects.

Interpretation is deliberately decomposed:

- **Arm 1 -> Arm 2 isolates the wake mechanism:** periodic scan versus notification-driven wake-up, while avoiding a broker.
- **Arm 2 -> Arm 3 isolates the incremental transport path:** direct task hint versus Kafka, with the wake mechanism held notification-driven.

Freeze and record both periodic-poll intervals, the direct-dispatch fallback interval, the relay fallback interval, notification/reconnect behavior, and the direct worker-selection rule. If Arm 2 and Arm 3 use different fallback intervals or worker-selection semantics, treat that as a protocol change and rerun the matched baseline.

Measure task-outbox-ready-to-claim dispatch delay, PostgreSQL query rate/time, dispatcher CPU/network, broker traffic/CPU where applicable, backlog age, worker-notification failures/retries, and terminal correctness. Attribute path stages separately where possible: outbox commit -> wake, wake -> direct notification or Kafka publication, notification/consumption -> accepted worker claim.

The B->C comparison can support a bounded claim about **Kafka's incremental measured cost or benefit in this topology**. It cannot by itself prove large-scale worker decoupling, connection-count, retained-backlog, or multi-host scaling advantages that were not exercised. If notification-direct matches or beats Kafka locally, report that result directly: at the tested four-worker/single-host scale, Kafka was not a latency optimization, and its rationale remains architectural unless a separate experiment demonstrates otherwise.

### D. Lease study: 6 configurations, 60 fault episodes

**3 lease settings × 2 fault types × 10 episodes.**

Use owner crash and owner pause/resume beyond expiry. Keep two schedulers, worker capacity, workload, and offered rate fixed. Freeze three settings after pilot measurements of transaction and scheduling delays; record renewal interval along with TTL.

Measure each episode from controller-observed injection through takeover and first useful affected-work progress. Record episodes that fail to recover within the deadline. Reset to a documented stable condition between episodes.

Scheduler leases and worker heartbeat deadlines are different controls. This study varies scheduler lease settings only.

Ten episodes per configuration are a small sample. Report individual values, range, median, and uncertainty; do not advertise a stable p99 estimate.

### E. Checkpoint study: 6 configurations, 18 measured runs

**3 checkpoint settings × 2 failure conditions × 3 repeats.**

Use one pure 200-chunk activity with a known final output hash:

- activity-boundary-only persistence;
- checkpoint every five chunks;
- checkpoint every chunk.

Compare no failure with one crash at a frozen mid-activity barrier. Hold chunk work and all other settings fixed. Record committed progress, recomputed chunks, bytes/writes, total completion time, and recovery time.

This study measures pure-work checkpointing. It cannot establish atomicity for unrelated external effects.

### F. Agent evaluation

**Deterministic continuity:** Treat this as correctness evidence in DUR-033, not a quality comparison. For each of the 20 held-out cases, run one uninterrupted fixture execution and one matched interrupted execution at the declared committed-output or approval-wait boundary. With versioned scripted/recorded activity outputs, the final durable report and action/no-action decision must match exactly or the case fails. Transition history, attempt counts, retry rows, timestamps, and other recovery bookkeeping may legitimately differ and are not comparison targets. Report pass/fail counts and any semantic divergence; do not multiply identical deterministic fixtures merely to inflate N.

**Live-model study:** 20 held-out cases × 2 conditions = 40 incident executions initially. Use one pinned model/prompt/tool configuration, randomize matched case order, and report stochastic variation and the small sample. Freeze the interruption schedule and ensure all incident families appear in both conditions.

The live comparison is exploratory; different model outputs may confound quality differences. Additional live repetitions require a cost estimate and authorization.

Use a scripted authenticated approver for batch evaluation under a frozen oracle policy. Label it as a test actor. Demonstrate an actual human approval separately. Approval waiting time must be reported separately from execution/model latency.

### Protocol freeze

Before final measurements:

1. Pass the correctness campaign.
2. Record commit/image/schema versions, host placement, operating system/kernel, filesystem/volume type, database durability, broker settings, direct-dispatch and outbox-relay wake mechanisms/fallback intervals, reconciliation interval, worker-selection rule, resource limits, worker counts, partitions, and timeout/backoff settings. For RQ6, preserve matched settings so polling->NOTIFY isolates wake-up and NOTIFY-direct->NOTIFY+Kafka isolates transport. Final I/O-sensitive comparisons use the Linux measurement host declared and validated in DUR-036; development-only Docker Desktop/WSL2 results are labeled accordingly.
3. Use separate development traces to choose SLOs, practical-effect thresholds, durations, fault deadlines, and sampling targets.
4. Freeze seeds, workloads, approval policy, evaluator rubric, and model configuration where applicable.
5. Use open-loop arrivals with bounded client resources. Record scheduled arrivals, dispatch lag, missed sends, rejections, and ambiguous submission responses.
6. Match seeds across compared configurations and randomize order within repeated blocks.
7. Define a measured arrival cohort and drain period. Reconcile every accepted workflow and expected effect.
8. Record reset/warm-up, measured interval, drain, failed attempts, and whole-session resource cost separately.
9. Rerun affected baselines after a material implementation or protocol change.
10. Preserve raw results and failures. Report insufficient evidence as limited/inconclusive.

### Metrics

| Metric | Definition |
|---|---|
| Workflow throughput | Successfully completed measured-cohort workflows divided by elapsed cohort-plus-drain time; also report activity completions separately. |
| Sustainable progress | Accepted arrival rate versus terminal rate and backlog-age slope during a declared steady interval. |
| Completion latency | Client-observed submission to durable terminal outcome; report success/failure/cancellation populations separately. |
| Dispatch delay | Durable activity readiness to accepted worker claim, using comparable timestamps with stated clock assumptions. |
| Ownership recovery | Controller-observed fault to committed new ownership of affected partitions. |
| Useful-work recovery | Fault to first accepted post-fault progress for affected work. A new lease alone is not recovery. |
| Backlog recovery | Fault or dependency restoration to return within the predeclared backlog/latency band for a sustained interval. |
| Duplicate execution | Multiple physical activity/tool invocations for one logical activity; distinguish these from duplicate effects. |
| Duplicate effects | Sum of applied mutations beyond the one allowed per logical effect, checked against the independent sink ledger. |
| Missing/lost effects | Required effects absent after quiescence and reconciliation; distinguish proven engine loss from pending, timed-out, intentionally canceled, and unknown outcomes. |
| Repeated work | Pure chunks or external calls repeated after recovery; include possibly repeated billable LLM calls. |
| Fencing | Attempted and rejected stale scheduler writes, worker progress, and sink requests, reported separately. |
| Persistence overhead | Transactions, queries, bytes, WAL where measured, lock waits, and checkpoint writes per completed workflow/activity. |
| Coordination overhead | Lease renewals, takeovers, ownership churn, reconciliation scans, and their resource cost. |
| Scheduler resource efficiency | Scheduler CPU-seconds and database time/queries per completed workflow for the scheduler-count study. |
| Dispatch-path cost | Task-outbox-ready-to-claim delay plus PostgreSQL, direct-dispatch, network, and broker resource load across periodic polling+direct, `LISTEN/NOTIFY`+direct, and `LISTEN/NOTIFY`+Kafka paths. Report A->B as wake-up effect and B->C as incremental transport effect. |
| Kafka health | Consumer lag, oldest outbox age, duplicate messages, rebalances, acknowledgment failures, quarantine. |
| Agent diagnosis success | Cases meeting the frozen ground-truth and evidence rubric divided by all evaluated cases. |
| Safe end-to-end agent success | Cases with correct diagnosis/abstention, appropriate approval behavior, expected action or no-action, and successful verification divided by all evaluated cases. |
| Approval violations | New action dispatches/applied mutations lacking the required matching authorization. Count both boundaries. |
| Cost per completed incident | Attributable model/tool/hosting cost divided by completed incidents; separately report cost per successful incident and failed work. |

For effects, report counts plus denominators. Zero observed duplicates in N eligible operations is a bounded result under the tested faults, not a universal proof.

Use monotonic clocks for durations measured within one process. Do not subtract unsynchronized host timestamps. Keep controller-observed recovery measurements separate from database and service timing.

Report p50/p95/p99 only with sample counts and adequate resolution. Never average p99 values and label the result a pooled p99. Use run-level summaries/uncertainty and disclose tail limitations.

## 15. Portfolio and interview evidence

Lead the final README with at most three measured findings, followed by the failure-boundary diagram, architecture, method, limitations, and reproduction. Treat the correctness campaign separately as a bounded validation claim (for example, N named fault executions with independently checked violations/unknowns), not as one of the three comparative findings. Prefer the safeguard-cost result, lease tradeoff, and either checkpoint or dispatch-path result if the evidence is informative.

Prioritize explanations the user can defend:

1. Why Kafka and PostgreSQL do not form one transaction, and how outbox/inbox plus reconciliation recover the gap.
2. Why a lease is insufficient without a fenced mutation boundary.
3. Why scheduler takeover does not necessarily cancel valid worker execution.
4. Why idempotency, worker fencing, and sink fencing solve different problems.
5. What happens when an effect succeeds but its response is lost.
6. Why checkpoints preserve some work while uncommitted LLM outputs may be regenerated.
7. How approval/cancellation races are serialized and where revocation stops being guaranteed.
8. Which component limits scheduler scaling and what the measurements show.
9. Why Kafka is present when PostgreSQL is authoritative, what periodic polling -> `LISTEN/NOTIFY`-direct -> `LISTEN/NOTIFY`+Kafka showed, and whether Kafka bought a measured benefit at the tested scale or only architectural properties that remain outside that benchmark.
10. What throughput/latency cost the tested safeguards impose and which concrete failure each unsafe control exposes.

At report time, briefly position the project against verified current references rather than claiming novelty or parity: Temporal for orchestration/activity separation; DBOS and Restate for durable-execution models; River for a PostgreSQL-backed Go job-processing design; and Hatchet only after verifying the relevant current architecture. Keep this to architectural context, not feature-scorecard marketing.

For each resume statement, record the implementation commit, exact test/experiment, measured population, configuration, limitation, and reproduction command.

Use placeholders until results exist. Example claim structures:

- Built a Go/Python workflow runtime with PostgreSQL partition leases and Kafka dispatch; measured [recovery statistic] under [named fault conditions].
- Validated [N] deterministic failure executions with [observed duplicate/missing/unknown effect counts] under [cooperating endpoint contract].
- Demonstrated an approval-gated incident agent on [N] held-out synthetic incidents with [success measure], separating model quality from runtime recovery.

Do not insert invented numbers or call fixture-only evaluation a live-model result.

### Final checklist

- [ ] Clean-checkout local environment with real PostgreSQL/Kafka.
- [ ] Two scheduler replicas and Python worker pools.
- [ ] Reviewed lease, attempt, dispatch, checkpoint, effect, and approval contracts.
- [ ] Named failure traces and an independently checked effect ledger.
- [ ] Required invariant/race/outage cases executed and reconciled.
- [ ] Declared Linux measurement host and DUR-021A telemetry readiness evidence completed under DUR-036 for final I/O-sensitive studies.
- [ ] Frozen throughput, safeguard-cost, dispatch-path, lease, and checkpoint studies for full v1.
- [ ] Deterministic incident demo and held-out live-model evaluation for full v1.
- [ ] All unknown effects, pending tests, and limited conclusions visible.
- [ ] Build log, decision records, personal walkthroughs, and claim-to-evidence table.
- [ ] Claude review covers final implementation and published claims.
- [ ] No resume claim exceeds the tested failure model or measured deployment.

## 16. Immediate next action

Start **DUR-001 — Repository and reproducible toolchain** in this project's repository.

Complete its checks, evidence, and review before continuing. Local foundation work does not require a cloud or model-call budget.

For each subsequent task, add status, dependencies, goal, scope, acceptance scenarios, exact validation commands, evidence paths, commits, review round, and remaining limitations before starting implementation.
