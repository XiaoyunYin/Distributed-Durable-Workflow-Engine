# J4 slice 0 — Recorded-provider recovery protocol

Protocol version 1. Review by Claude is required before hosted measurement.
This protocol registers the J4 recovery demonstration and D037 B2 provider
resilience from PLAN.md §§18–19, D036 amendment 1, D037, and REVIEW.md
Rounds 613 and 615. It contains the planned thresholds and expected outcomes.
The source baseline is `cd4f806adf493f545c2d069ce1a8ed5e4cb67d81`.

## Deployed path and fixture

Submit one workflow per cell through `POST /v1/workflows` on the Go API.
Seed an immutable two-node definition `j4.recorded-llm-recovery@v1` with
`j4.recorded-llm@v1` followed by the existing `pure.echo@v1`, both at
iteration 0. Register the provider node in Go and Python as a replayable
fixture under the existing `PURE_ACTIVITY` class. Its HTTP invocations can
repeat and are counted externally. The Go engine takes the accepted provider
result from PostgreSQL and uses it as the successor input. Echo must return
that value from its own worker execution. Use the Go scheduler, transactional
outbox, Kafka, Python worker claim/heartbeat/result handling, and the engine's
receipt consumption and graph advancement path. Use a new group state per
cell. Require the worker's synchronous broker offset commit (`offset + 1`
on the single partition) to precede the first activity execution and every
replacement execution. Capture the broker group offset and the worker's
post-commit boundary. A durable delivery disposition alone is insufficient.
[Go registry](../../../internal/state/registry.go)
[Worker delivery](../../../python/workers/kafka_worker.py)
[Receipt and consumption](../../../internal/state/store.go)
[Successor input](../../../internal/engine/engine.go)

The out-of-process HTTP stub serves `POST /v1/recorded-completions`.
The request body is a JSON object with exactly two string fields, `model`
and `prompt`. Limit both to printable ASCII. Reject duplicate keys, malformed
JSON, extra fields and non-string values. Canonical bytes are UTF-8, keys
sorted lexicographically, compact separators, normal JSON string escaping
and no trailing newline. The stub recomputes SHA-256 from the body. The
matching fixture and wrong-input control are fixed.

| Input | Canonical body | SHA-256 |
| --- | --- | --- |
| Matching | `{"model":"recorded-llm-v1","prompt":"Return fixture alpha."}` | `92b3bc136a6617117cfac28e4324b272b6bf6b72ab5d137a948cb9fc0d25e751` |
| Wrong | `{"model":"recorded-llm-v1","prompt":"Return fixture beta."}` | `7fd339f77e3935f75c39a7436f7c71dd74f4644f87c3f5968120ce9095e06522` |

A matched scripted HTTP 200 returns the JSON fields `completion` (literal
`fixture alpha`), `request_sha256` and `provider_call_id`. The latter two
contain the stub's recomputed request hash and this specific call ID. The
worker persists that object as the activity result. The stub returns HTTP 422
with `RECORDED_REQUEST_MISMATCH` for the canonical wrong input. The worker
sends the wrong input unchanged and records a final failure with this reason.
Scripts are indexed by declared cell and call ID/ordinal, with identity
metadata outside the hashed body. Unexpected or duplicate IDs are recorded
as separate arrivals and cause checker failure. A mismatch never returns
the fixture or advances the matching script.

The stub appends and fsyncs its own receipt journal before acknowledging
receipt to the controller or emitting response bytes. Use a host-backed
volume dedicated to this job, with stub-only writes, surviving worker and
stub container replacement. Record append sequence, shared-host timestamp,
cell and step IDs, worker attempt and provider call ID, recomputed hash,
match decision, script branch, and response/withhold/close events. The
checker reads the journal independently of the worker and engine. The
stub remains up during measured worker kills. Qualification must reopen
the journal with identical records after one stub replacement.

### Provider egress and credentials

Run the one-shot HTTP caller as a dedicated non-root UID in the Python worker
container. Pass only the fixed request fields and per-call metadata over an
inherited Unix socket. The caller has no inherited IP sockets, mounted
credentials, ambient capabilities, ability to change UID or network rules,
or proxy/redirect/DNS/transport-retry path. A J4-only init step installs an
output chain for that UID with default-deny IPv4 and IPv6 and exactly one
allow tuple, the stub pod IPv4 address on TCP 8082. Reject access to the
stub from the worker's other UIDs. Match the exact stub pod and port in
Calico too. The Kafka/control parent uses separate, explicitly enumerated
broker and Go API lanes under Calico and cannot open a provider connection.
It supplies neither a proxy nor a generic network command over IPC.
Revalidate policy on every worker replacement. Use no host networking.

There are no provider API keys. Verify the effective environment, mounted
secrets/files and request header names in the parent, caller and stub.
Use an explicit synthetic-only fixture environment. Expose any engine-only
credential to the parent through its private control path, and exclude it
from the caller's environment and filesystem view. Inventory the credential
by name and type, with no credential values in evidence.
Reject provider credentials, proxy variables, credential mounts in the
caller or stub, and provider authentication headers. Verify the installed
rules, a successful connection from the caller to the stub, and denied
connects from the same UID to live decoys for wrong pod, wrong port, broker,
API and DNS. Use an isolated reachable decoy to test a destination outside
the allow tuple and require zero decoy receipts under the policy. A positive
decoy reachability check from a separate qualification probe establishes
that the targets are live. Run the positive stub probe on TCP 8082 via a
path that emits no provider receipt, before the cell. Reject any broader
effective allow or alternate provider path before submitting the workflow.

## Opt-ins and K isolation

These are the two J4 entries to carry into the single tracked D037 switch
registry when implemented. Profile names below also label the evidence.
Only the literal value `1` enables a switch. Unset and explicit `0` select
the listed off behavior. Reject other values, a Go/Python mode mismatch,
and B2 enabled without J4 or the registered B2 schema. Render and inspect
the actual process modes before workloads start.

| Switch | Unset behavior | Permitted enabled profiles | Migration and routes |
| --- | --- | --- | --- |
| `J4_RECORDED_LLM_ACTIVITY` | Go and Python omit `j4.recorded-llm@v1`. Existing dispatch and activity behavior apply. | Dedicated J4 tooling, qualification and measured profiles `j4-core` and `j4-b2`. | No migration. Existing Go workflow and worker routes. The private stub listener serves `POST /v1/recorded-completions` on 8082. No new engine route for this switch. |
| `J4_PROVIDER_RESILIENCE` | No reservation/retry policy or B2 routes. With the activity enabled, one send per claimed attempt with no provider-level retries. | Dedicated J4 tooling, qualification and measured `j4-b2` only, with the activity switch enabled. The fixed-delay mutant is a harness-only variant of this profile. | Apply only `migrations/j4_b2/000025_j4_provider_resilience.up.sql` beyond the base profile. Add internal worker POST actions `provider-attempts/reserve` and `provider-attempts/outcome` under `/v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/` only in B2. |

The B2 routes use the internal worker listener and current claim authority,
including namespace, node, iteration, attempt and token checks. They are
unavailable with B2 off and absent from the public listener. Preflight
compares the complete method/listener inventory. The stub is private to the
J4 network and uses the same route in both profiles. Fault and mutant
selectors are harness inputs sealed into the J4 job. They add no runtime
opt-in variables or public control routes.

Use exact-image Go and Python `activity-registry` exports and the existing
`compatibility-check` before enabling work. Freeze submissions across the
check through rollout, and use fresh consumer provenance or verify every
surviving worker. Check parity with J4 on and off and prove one-sided opt-in
fails. The J4 core expected migration inventory is the baseline's root
`000001`–`000023` upward migrations, with `pg_stat_statements` preloaded
for migration 18. B2 adds only the named opt-in migration
in its subdirectory, using a fresh DB for each cell. Bind all expected
migration paths, numeric IDs and hashes to the reviewed profile, and
reconcile the ledger and schema with the actual DB.

Both switches are unset in K5/K6 and J5a/J5b. Keep registered K manifests
byte-identical. Those jobs must fail closed on unregistered opt-in
variables, mode or route drift in rendered or running resources, and
missing or extra intended or applied migrations. Compare against each
registered profile's complete inventory, including
`migrations/k6_capture/000024` and `dur055` and `dur056` where applicable.
Require an unset legacy-equivalence test for each switch outside the
PostgreSQL K6 fixtures job. With the activity unset, compare registry
exports and existing workflow outcomes to baseline. With B2 unset, compare
legacy routes, schema and existing workflow outcomes to baseline and
exercise the J4 core single-send path. The repaired relay is selected
explicitly for J4 under §19. Record its mode and leave the J19 policy unset.
A reviewed append-only K protocol-v2 disclosure is required before running
registered K profiles from a changed checkout.
[K2 parity and rollout](../../../cmd/runtime/registry.go)
[Registry comparator](../../../scripts/check-activity-registry.py)
[Registered K protocol](../k8s-recovery/protocol-v2.md)

## Core cells

Both kill cells target the process that handled the recorded-provider
node. Send SIGKILL to the worker container's process, terminate its caller
with it, and verify exit 137, the old process gone, and a different worker
process/container identity before release. Keep the Go runtime, broker,
database and stub up. A workflow-scoped scheduler gate holds
`BeforeResultConsumption` in cell C2. Witness its arrival and confirm no
committed consumption or successor creation while it is held. Use a
post-receipt worker hold to preserve the kill target. The provider caller
cannot outlive the worker. In C1 the stub withholds all response bytes
through the kill and records connection termination. Check committed DB
state before releasing either barrier. A fresh call ID identifies each
core send, including replacement sends.

| ID | Cell and required proof | Stub arrivals | Successful provider results consumed by the successor |
| --- | --- | ---: | ---: |
| C0 | No fault. One matched 200 through the deployed path. | 1 | 1 |
| C1 | SIGKILL while call 1 is in flight. Require persisted stub receipt, committed Kafka offset, held response socket, current claim, and zero committed result receipts before kill. Require committed expiry/replacement of the killed attempt, a new worker claiming a new attempt through the normal engine path, and call 2 returning 200. | 2 (1 after kill) | 1 |
| C2 | SIGKILL after a separate committed read sees the result-bearing attempt, `ATTEMPT_RESULT_RECORDED` and result outbox event, with the successor held behind the scheduler gate. Verify replacement readiness, then release the gate. Consume the original attempt's result and witness an old provider-event redelivery. | 1 (0 after kill) | 1 |
| C3 | Wrong input. A log entry contains the wrong hash and HTTP 422. Workflow reaches durable final failure with the mismatch reason. No successor starts. | 1 rejected | 0 |
| C4 | Idle-worker kill. Prove zero accepted workflow, active claim, open provider call and result receipt at the cut. Start the same baseline workflow only after replacement readiness. The checker must reject the attempted crash-resume demonstration for absence of a cut overlapping the target workflow's execution. | 1 after the idle kill | 1 from the subsequent ordinary workflow |

B2 is off throughout C0–C4. The core worker's one-send-per-claim behavior
does not imply a cross-claim provider cap. C1 observes engine replacement
after claim expiry. C2 requires no replacement claim for the provider node
and no new provider arrival from kill through the final observation
boundary, including after redelivery. Retain evidence at the gate and on
both sides of the kill. An API response or worker log alone is insufficient
proof of the receipt commit.

## B2 durable attempt budget and retry timing

Budget key is `(workflow_id, node_id, iteration)`. The cap is **four
reservations per step**, including the initial send and unused reservations.
Issue sequences 1–4 under a transactional row lock with current claim
validation. Each reservation has a stable unique provider call ID and claim
identity, and commits in PostgreSQL before the caller can send. Reserve
idempotently for a repeated control request within a live claim so a lost API
acknowledgment cannot open a second permit. A live caller may use a permit at
most once. A replacement claim never transmits an earlier claim's ID. Keep
retry eligibility and the step budget durable across worker restarts.
Concurrent, stale, duplicate and terminal requests must leave the cap and
any recorded outcome intact. Persist terminal success with its call ID and
response payload, or terminal exhaustion with its reason, before handing
that outcome to the existing result path. A recovered worker with a
persisted terminal outcome may finish reporting it using its current claim
and cannot reserve again. The provider client makes no automatic retries.

On a new reservation, commit an absolute HTTP deadline `D = T_reserve +
2,000 ms`, with `T_reserve` sampled at issuance for that transaction.
Record the first outbound request byte within 250 ms of issuance when
sending. For a hanging response with a live worker, it must enforce the
deadline at `D` with at most 100 ms of observation lag. A reservation
without a recorded outcome after a replaced claim is treated as ambiguous
at the original `D`, including a pre-send crash. Charge it, and preserve
that origin and schedule on recovery. The production retry path never
queries the stub journal. The checker uses it to label unused reservations
and distinguish the two crash cuts.

Use one shared host monotonic clock and boot identity for receipt, response,
deadline, readiness and cut timestamps. Persist the absolute B2 deadlines
with that clock ID, plus DB row identities and commit witnesses for causal
ordering. Require clock domain probes at start and end with a shared boot
ID and round-trip brackets at most 20 ms between the controller, worker,
stub and Go API. A boot ID change or unverified mapping invalidates the
episode and admission fails closed. This clock contract covers the registered
single-host worker process restarts.

Let `i` be the reservation sequence that just failed. Seed is **37**.
Jitter in milliseconds is the unsigned big-endian integer from the first
four SHA-256 bytes of ASCII `j4-b2-v1:37:<i>`, modulo 1,001. Nominal backoff
`B_i = 2,000 × 2^(i-1) + jitter_i` ms. Use the time `O_i` when the caller
fully receives the 429 headers, or the timeout deadline `D` as defined
above. For scripted 429, the valid delta-seconds `Retry-After` is the earliest
permissible retry measured from `O_i`. The eligible time is
`E_i = O_i + max(B_i, Retry-After_i)` with `Retry-After_i = 0` for a
timeout. Persist `O_i` and `E_i` with the outcome or derive them from the
old `D` on replacement and persist them before a further reservation.

| Sequence i | Jitter (ms) | B_i (ms) |
| --- | ---: | ---: |
| 1 | 302 | 2,302 |
| 2 | 484 | 4,484 |
| 3 | 486 | 8,486 |
| 4 | 39 | 16,039 (exhaustion observation only) |

Require the next reservation issuance, first outbound byte and stub
receipt, in that order, within **[E_i, E_i + 750 ms + A]**, with committed
reservation proof before outbound bytes. B1 ordinal 1 is armed but never
sent and uses its persisted deadline to anchor the first retry. For uninterrupted retries
`A = 0`. For a planned restart, `A = max(0, T_ready - E_i)` with a hard
cap of **18,000 ms**. `T_ready` is the new process's committed claim and
read of the persisted step schedule, witnessed before it may reserve.
Require `T_ready` within 18 s of confirmed death and, in B3, by
`O_1 + 17 s`. B3 is therefore still judged with `A = 0`. Restart never
relaxes the lower bound. Verify eligibility against independently decoded
response/timeout witnesses and the frozen vector. Retain observed delays
from `O_i`, recorded schedule, any restart allowance, and deadline slack.
A 429 delivered after the absolute HTTP deadline cannot count as a 429
input to the script.

| ID | Script by reservation ordinal, starting at 1 | Cut or control | Reservations / arrivals / unused | Completions / exhausted |
| --- | --- | --- | --- | --- |
| B0 | 429(1 s), timeout, 429(12 s), 200 | Transient recovery without a kill. The first two intervals exercise jittered exponential backoff. The third exercises the `Retry-After` floor. | 4 / 4 / 0 | 1 / 0 |
| B1 | Unused, 429(1 s), 200 | Kill after verified committed reservation 1 and before its deadline, with a gate before the caller starts. Witness zero stub receipts for ID 1 during the cell. Treat it as ambiguous at its original deadline and continue with ID 2 under the new claim. | 3 / 2 / 1 | 1 / 0 |
| B2 | Timeout, 429(1 s), 200 | Kill after the stub fsyncs receipt for ID 1 and withholds all response bytes, before the worker records any outcome and before its deadline. Continue with ID 2 under the new claim. | 3 / 3 / 0 | 1 / 0 |
| B3 | 429(20 s), 200 | Kill during durable backoff. Request SIGKILL 500 ms after observed 429 receipt and confirm death by `O_1 + 1 s`, after committed eligibility and before ID 2 exists. Replacement sees the same `O_1` and `E_1` before eligibility. | 2 / 2 / 0 | 1 / 0 |
| B4 | 429(1 s), timeout, 429(12 s), timeout | Exhaust all four reservations and witness durable final failure. Run both post-terminal redeliveries and observe past the latest eligible time as defined below. | 4 / 4 / 0 | 0 / 1 |
| B5 | 429(1 s), timeout, 429(12 s), 200 | In an isolated control, substitute 3,400 ms fixed delay for every `B_i`, keeping `Retry-After` and the other checks. The candidate must execute the script. The original oracle must reject interval 1 as late and interval 2 as early, with the third still honoring 12 s. | 4 / 4 / 0 | 1 / 0 |

For each timeout script entry, the stub fsyncs the request and sends zero
response bytes, leaving the worker unable to infer whether the provider
received it from the timeout. Later connection closure does not remove
the receipt. Reconcile all reservations and all stub arrivals by ID.
Every arrival has exactly one prior committed reservation, and each
reservation has either one arrival or an explicitly witnessed unused
disposition. The only expected unused reservation is B1 ordinal 1. Do
not infer an unused reservation from missing worker logs. Retain the stub
journal through each observation window. The uncut timeouts must actually
reach the worker deadline before the next reservation.

For B4, on timeout 4 require a committed exhausted step outcome
`PROVIDER_BUDGET_EXHAUSTED`, a final failure receipt and terminal failed
workflow by `D_4 + 5 s`. Conservatively define the latest would-be retry
`L = D_4 + 16,039 ms + 750 ms + 18,000 ms`. The window includes the full
restart allowance even though the budget is depleted. Redeliver the
original provider dispatch notice first within 2 s of committed terminal
state, then once in `[L + 1 s, L + 3 s]`. Observe through `L + 10 s` and
at least 5 s after the second broker delivery disposition. Retain evidence
of both real worker deliveries and require zero new calls, reservations,
successor nodes or successful results through the window. Require exactly
one exhausted step and one failed workflow, with exhaustion surviving
redelivery. The extra waiting interval is an observation window only.
The controller republishes the original event identity, headers and body
at a new offset, and never creates a task or claim directly. Use that same
mechanism once in C2 within 2 s of the successor's terminal commit.
Within 5 s of each republish, witness the durable duplicate/stale/terminal
disposition, group offset advancement, and no new claim.

## Hosted schedule, admission and stopping

Use standard public GitHub-hosted `ubuntu-24.04` at **$0**. Schedule
dedicated J4 jobs, with serial cells, one Linux host and one kind node
using Calico. Pin the existing kind node, PostgreSQL, Kafka and Calico
digests and tool hashes in the base `deploy/k8s/tools.lock.json`. Seal the
built Go runtime, Python worker, stub, init/probe and harness/checker
digests, source revision, rendered J4 overlay, switch/migration inventories,
scripts and checker expectations before qualification and use the same
sealed inputs for measurement. Record actual runner image, boot ID,
resource inventory, pod image IDs and effective policies. Use one Go
runtime (API, scheduler and relay), one PostgreSQL, one Kafka broker with
one task partition and replication factor 1, one Python worker slot in
bounded-executor mode, one independent stub and one controller/checker.
Keep exactly one worker execution process active or being replaced.
Use a 12 s J4 worker claim lease with a 2 s heartbeat, a 1 s scheduler scan
interval, and a 10 s core HTTP deadline. Set pod resource limits of
1 CPU / 1 GiB for Go runtime, 1 CPU / 1 GiB for PostgreSQL, 1 CPU / 2 GiB
for Kafka, 500m CPU / 512 MiB for the worker and 250m CPU / 256 MiB for
the stub. The runner must have at least 4 logical CPUs, 12 GiB RAM and
20 GiB free disk after checkout, with at least 10 GiB free before each
cell. No cloud services, paid runners or K jobs or ledger time are used.

- **Order and denominator.** One qualification job runs C0–C4 then B0–B5
  once each. One later measured job runs that order three times on the
  same host, yielding **33 scheduled cells**, **three workflows and three
  provider steps per cell ID**. C4's workflow begins after the idle kill.
  Keep rejected and exhausted workflows in the denominator. Count only
  accepted identities as accepted and retain every scheduled ID and
  disposition. Newly seed/reset DB, topics, consumer offsets and
  controller state between cells, with a new stub journal segment and a
  read-only export of the previous cell. No overlap between cells. Claim
  populations contain only the measured job, with qualification reported
  separately.
- **Time limits.** Each cell gets 5 min from start to reconciled evidence,
  including final observation. Require first claim and initial receipt or
  reservation within 30 s of accepted submission. For C1/C2/B1/B2 require
  kill request within 500 ms of the acknowledged cut boundary and verified
  exit within a further 500 ms. For B3 apply its tighter outcome-relative
  bound. Core replacement worker readiness is due within 18 s of death.
  C1 replacement claim and observed reinvocation are due within 30 s of
  death. C2 release is due within 2 s of replacement readiness. Core
  workflow termination is due within 90 s of submission. Watch core
  continuations for 30 s after terminal state and, in C2, also at least
  30 s after the kill and after the redelivery disposition. B2 workflow
  termination is due within 120 s of submission, with the additional B4
  cutoff and observation rules above.
- **Qualification.** Require all 11 cells, including expected rejections,
  successful replay of their verdicts, credential and connectivity probes,
  exact switches and routes, schema and registry parity, evidence upload
  and complete teardown. Verify at least one stub log re-open before the
  cells and one successful cleanup/reset between every two cells. Before
  measurement, require this qualification and explicit review against the
  exact candidate and sealed inputs. Check reverse joins and demonstrate
  that the checker rejects a missing call row, missing receipt/consumption
  history and a missing attempt reservation, using copies of qualification
  evidence only.
- **Forecast.** Use the repository's **350 min per-job cap** for the J4
  qualification and measurement jobs. Reserve 25 min bootstrap/preflight,
  5 min per cell, 2 min per between-cell reset and 25 min for final
  reconciliation, evidence upload and teardown. Qualification is
  `25 + 11 × 5 + 10 × 2 + 25 = 125 min` (225 min headroom).
  Measurement is `25 + 33 × 5 + 32 × 2 + 25 = 279 min` (71 min
  headroom). Tooling and legacy-equivalence checks use a separate J4 job
  capped at 30 min. These allowances are forecasts. Before dispatch and
  each cell, estimate remaining work using the greater of the registered
  allowance and qualification or observed job time for each matching
  stage/cell, retaining the full final allowance. Refuse admission if that
  total exceeds the remaining cap. An actual local stage or cell deadline
  breach also stops as specified below.
- **Stop and retain.** Stop new cells on the first unexpected outcome,
  violated invariant, failed calibration/preflight, missed deadline,
  unreconciled identity, incomplete witness or failed cleanup/reset. Only
  the registered C3/C4/B5 rejection and B4 exhaustion count as expected
  outcomes and continue when all of their positive assertions hold. Record
  conclusive wrong behavior as failed and missing proof or infrastructure
  failure as invalid, preserving any known failure across export or cleanup
  trouble. Mark all unstarted cells unperformed with IDs and reasons.
  Zero substitutions, retries, excluded outliers or rescheduled cells in
  the registered population. Always export partial evidence, attempt
  teardown and verify absence of job-owned pods, DB volumes, broker state,
  local journal data, injected rules and other job resources. Qualification
  failure blocks measurement. A changed schedule, thresholds or frozen
  inputs requires a reviewed amendment and new qualification before any
  new measurement.

[Infrastructure pins](../../../deploy/k8s/tools.lock.json)
[Campaign cap](../../../.github/workflows/dur053-campaign.yml)

## Evidence, checker and claims

Store reviewed evidence under this directory at `<campaign-id>/`, with B2
under `<campaign-id>/provider-resilience/`. Keep job/run/attempt and
artifact IDs, source and image digests, script and overlay hashes,
qualification and forecast, ordered cell ledger, signed-off checker version,
and a checksum index for independent replay. Retain raw accepted API
identities, broker events and committed offsets, process cut/hold evidence,
stub journal, and read-only PostgreSQL snapshots and transactional traces.
Redact tokens and credentials before export. Publish per cell and aggregate
counts for scheduled, accepted, passed, failed, invalid and unperformed
workflows and steps, HTTP arrivals (matched, rejected and after kill),
reservations, unused reservations, successful receipts, result consumptions,
successor completions and exhaustion, with timing origin, bounds and
observed delays. Report one step denominator per accepted workflow, with
denominators and exceptions explicit.

The independent checker joins submission identity, workflow revision and
step identity, Kafka notice and offset, worker/claim identity, reservation
and provider call ID, request hash, response payload and committed result.
Include the accepted receipt and matching outbox event,
`RESULT_CONSUMED` history, the successor's stored input, claim and returned
result, and terminal history. Join evidence in both directions. For every
successful cell require exactly one accepted success receipt, one source
result consumption and one successful successor consuming that same payload.
Require zero extra results, unexpected invocations or unexplained rows.
For a failed provider cell require zero successful receipts, consumptions
of a successful payload and successor nodes. For C1 show the killed call
had no result and the new call produced the accepted payload. For C2 join
the sole, pre-kill call to the original receipt and the post-kill
consumption. C4 and B5 pass as controls only if the checker returns the
specified rejection reason with all unrelated assertions passing.

Only after evidence review and a claim-map update may the following
sentences be published. Require all three measured repeats per applicable
cell, the registered controls, and complete reconciliation and cleanup.

- **Core:** On one GitHub-hosted kind node using a recorded provider, the
  engine demonstrated at-least-once provider calls with exactly one durable
  result consumed by the successor. In all three witnessed pre-receipt
  worker kills it re-invoked the provider once, and in all three
  post-receipt worker kills it made zero further calls.
- **B2:** On that single-host fixture, the opt-in policy bounded provider
  work to four durable reservations per step across the scripted worker
  restarts, including unused reservations, and met the seeded retry
  windows and `Retry-After` minima. Each transient/restart cell produced
  one consumed result in every repeat. After each of three exhausted steps,
  no additional calls were observed during redelivery and the registered
  observation window.

Out of scope for public claims are exactly-once provider invocation or
side effects, real-provider behavior, model quality, general egress
security, effects against a kind target, independent-host or dependency
failure recovery, HA, capacity, production readiness, and behavior beyond
the tested budget, scripts, fault cuts and observation windows. If a gate
fails, publish the reviewed outcome and unperformed population with its
limits. Withhold the corresponding success sentence.
[Public claim map](../recruiter/claim-evidence.md)
