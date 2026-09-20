# Foundation contracts (DUR-002)

This document freezes the actor boundaries and failure outcomes that later
engine tasks implement. It is a protocol document, not a claim that the
durable engine already exists.

## Version and scope

- Contract version: `dur-002.v4` (the effect-class-aware timeout correction to
  `dur-002.v3`).
- Workflow partition-map version: `sha256-u64-be-v1`.
- Partition count: 16.
- Workflow IDs are non-empty UTF-8 strings.
- The partition is `big_endian_u64(SHA-256(workflow_id)[0:8]) % 16`.
- The canonical vectors live in `api/partition-map-v1.vectors.json` and are
  loaded independently by the Go and Python tests.

Changing the algorithm, partition count, byte order, or map version requires a
new migration/contract version and is outside this milestone.

## Durable actors and authority

The local Compose deployment wires these actors for the `local-runtime`
namespace. Automatic activities are currently limited to `pure.echo/v1` and
`pure.add/v1`; unsupported definitions pause before dispatch. External effects
and approval attacks retain the separate DUR-033A integration-test scope.
This is not permission to execute an arbitrary activity named by broker text.

Fresh Kafka groups read earliest retained records; existing groups resume from
committed offsets. Python slots persist a delivery's inbox/poison disposition
through the control API before committing its offset. They claim with an
`expected_attempt` fence and use only authoritative input/version returned by
the API. An uncertain HTTP result is retried with the same body, without
rerunning the activity. A process crash after offset acknowledgment can still
leave execution unfinished: the scheduler's PostgreSQL scan redispatches an
unclaimed attempt, or applies the existing claimed-attempt timeout policy.
Events accelerate scans; missing events cannot be the only path to recovery.
Terminal workflows with pending wakeups remain scan-visible until acknowledged.

| Actor | May write | May not decide |
| --- | --- | --- |
| Client | Submit request with a client submission key; write a durable cancellation request or approval request | Workflow revision, attempt ownership, or external effects |
| Scheduler holding `(partition, owner UUID, epoch)` | Fenced workflow/node/timer transitions, request/decision application, retries, and outbox records | Work outside its current lease; worker results without attempt validation |
| Worker with a current claim token | Heartbeat, compatible checkpoint, and one terminal result receipt plus its completion wake-up through the control API | Scheduler ownership, workflow/node transitions, retry creation, or direct database writes |
| Relay/consumer | Transport claim/publication/disposition records | Workflow transitions or activity authorization |
| Approver | Record approval/rejection of the exact canonical proposal within its validity window | Workflow transitions, dispatch grants, a different proposal, or an effect directly |
| Effect service | Its own effect ledger and sandbox state under a stable effect key/token | Engine workflow state or approval records |
| Reconciler/operator | Bounded scans; audited reconciliation resolution | Pretending an unknown effect did not happen |

Client cancellation/approval requests and approver decisions are durable
intent records. The scheduler holding the lease is the only actor that applies
their workflow-state transitions or creates a dispatch grant. Every durable mutation is attributed to an actor identity and records its
relevant submission key, workflow revision, scheduler epoch, attempt/claim, or
message ID. The engine PostgreSQL database is authoritative for engine state;
Kafka is transport; the cooperating effect service's ledger is authoritative
for its own applied state.

## Identity separation

1. A client submission key deduplicates workflow creation.
2. A logical activity/effect key stays stable across retries of one operation.
3. An attempt number and claim token identify one authorized worker execution.
4. A scheduler epoch identifies one partition-ownership generation.
5. A message/event ID identifies one transport record.

Repetition of a message does not create an attempt. Replacing an attempt does
not create a new logical effect key.

Every activity definition declares an immutable effect class for its version:
`PURE_ACTIVITY`, `COOPERATING_EFFECT`, or `NON_COOPERATING_EFFECT`. A pure
activity has no irreversible external mutation. A cooperating effect accepts a
stable logical effect key and exposes an idempotent receipt/lookup contract. A
non-cooperating effect may mutate an unobservable target without a receipt
lookup. Timeout handling and retry eligibility use this declared class; a
worker claim or heartbeat cannot change it.

The cooperating effect service validates a bounded approval grant covering the
approved target resource, canonical action/state hash, and stable logical
effect key before mutating protected state. It recomputes the argument hash
from the submitted effect state and rejects a resource or argument that is not
in the approval. The first successful application marks the grant
`DISPATCHED`; a retry may return the existing receipt but cannot alter the
approved resource or arguments. A missing, expired, or mismatched grant is
rejected before the sink ledger changes; the grant is separate from the effect
receipt and resource fence.

## Workflow state machine

The following are workflow states, not process-local states:

| State | Terminal? | Meaning and permitted exits |
| --- | --- | --- |
| `RUNNABLE` | No | The current lease owner may schedule the next node; exits to `WAITING_ACTIVITY`, `WAITING_APPROVAL`, `WAITING_TIMER`, `CANCELED`, or a terminal state for an empty/complete graph. |
| `WAITING_ACTIVITY` | No | A current node has dispatchable/claimed work. The worker records only the attempt result; the lease owner later advances to the next node or terminal state. An unclaimed attempt may be redispatched without a new attempt identity. A claimed pure activity may be replaced after timeout; a claimed cooperating effect may be replaced only with the same logical effect key and grant scope; a claimed non-cooperating effect becomes outcome-unknown and exits to `RECONCILIATION_REQUIRED` with no replacement. Cancellation may prevent an unclaimed dispatch or become a best-effort request for an in-flight attempt: a claimed effect attempt is settled as `CANCELED` with `OUTCOME_UNKNOWN`, and a later report is retained as evidence without progress. Retryable failure exits to `WAITING_TIMER`. |
| `WAITING_TIMER` | No | A durable timer is pending. `purpose=RETRY_BACKOFF` represents a retryable activity result; `purpose=WORKFLOW_TIMER` represents an explicit graph timer. A due timer returns the node/workflow to `RUNNABLE`, or the lease owner applies cancellation to `CANCELED`. No process-local sleep is authoritative. |
| `WAITING_APPROVAL` | No | Exact proposal is durable and no lock or worker slot is held. An approver records a decision; the lease owner applies approval by creating a dispatch grant and moving to `RUNNABLE`, or applies rejection as `REJECTED`/no-action. Cancellation prevents a grant if the owner applies it first. |
| `PAUSED_UNSUPPORTED_VERSION` | No | Definition/activity/checkpoint version is unsupported; the lease owner applies an audited version-availability decision to return to `RUNNABLE`, cancellation to `CANCELED`, or operator abandonment to `ABANDONED`. |
| `RECONCILIATION_REQUIRED` | No | A non-cooperating effect is outcome-unknown, or a cooperating receipt lookup remains unavailable past the declared recovery policy; audited receipt evidence can return to the next node, an audited cancellation can exit to `CANCELED` without claiming the effect was undone, and audited abandonment exits to `ABANDONED`. |
| `SUCCEEDED` | Yes | Workflow completed with an accepted result. |
| `FAILED` | Yes | Permanent failure or exhausted retry budget was recorded. |
| `REJECTED` | Yes | Approval was denied and the workflow completed with a recorded no-action result; this is restraint, not an execution failure. |
| `CANCELED` | Yes | Cancellation won before a new effect dispatch, or cancellation was accepted as the final workflow outcome; it does not undo an already-applied effect or imply that an in-flight effect had no outcome. |
| `ABANDONED` | Yes | An operator recorded that an unresolved outcome will not be resumed. |

```text
RUNNABLE --schedule node-------------------------> WAITING_ACTIVITY
    |                                              |
    +--empty/complete--> SUCCEEDED                 +--worker result--> WAITING_ACTIVITY
    +--owner cancellation--> CANCELED              +--owner advances--> RUNNABLE/SUCCEEDED/FAILED
                                                   +--retryable--> WAITING_TIMER --due--> RUNNABLE
                                                   +--non-cooperating unknown--> RECONCILIATION_REQUIRED
                                                   +--owner cancellation--> CANCELED

RUNNABLE --schedule approval---------------------> WAITING_APPROVAL
RUNNABLE --schedule retry/backoff----------------> WAITING_TIMER
RUNNABLE --schedule graph timer------------------> WAITING_TIMER
WAITING_APPROVAL --owner applies approval + grant--> RUNNABLE
WAITING_APPROVAL --owner applies rejection--------> REJECTED/no-action
WAITING_APPROVAL --owner applies cancellation-----> CANCELED
WAITING_TIMER --owner applies cancellation--------> CANCELED
PAUSED_UNSUPPORTED_VERSION --owner applies version> RUNNABLE
PAUSED_UNSUPPORTED_VERSION --owner cancellation---> CANCELED
RECONCILIATION_REQUIRED --audited resolution------> RUNNABLE/CANCELED/ABANDONED

RUNNABLE --fan-out owner transition----------------> branch nodes RUNNABLE
branch node --accepted result----------------------> SUCCEEDED
all declared branches SUCCEEDED --join ready------> JOIN/RUNNABLE
any non-terminal branch --owner cancellation------> CANCELED/OUTCOME_UNKNOWN
terminal workflow decision ------------------------> no branch advancement
```

Cancellation and approval are serialized owner transitions. A client request
or approver decision never changes workflow state by itself. Before a durable
dispatch grant, an owner-applied cancellation prevents new dispatch; after a
grant or external request is issued it is best effort and cannot undo an
effect. Approval waits hold no database lock and consume no worker execution
slot. A rejected approval is `REJECTED`, not `FAILED`, so restraint remains a
separate outcome.

### Activity-attempt state machine

```text
CREATED --> DISPATCHABLE --> CLAIMED --> SUCCEEDED --owner consumes--> next node/terminal
  |              |             |  |----> FAILED_RETRYABLE --> replacement attempt
  |              |             |  |----> FAILED_FINAL --owner consumes--> FAILED
  |              |             |  |----> TIMED_OUT --pure--> REPLACED --> replacement attempt
  |              |             |  |                 \-cooperating--> REPLACED(same effect key) --> replacement attempt
  |              |             |  |                 \-non-cooperating--> OUTCOME_UNKNOWN --> RECONCILIATION_REQUIRED
  |              |             |  +----> OUTCOME_UNKNOWN --> receipt-confirmed result
  |              |             |                         \-> reconciled/abandoned outcome
  |              |             +------ late progress/result is rejected
  |              |             +------ claimed effect cancellation --> CANCELED/OUTCOME_UNKNOWN --> evidence only
  |              |             +------ claimed pure cancellation --> CANCELED/NONE
  |              +------------------> redispatch same attempt identity
  |              +------------------> CANCELED before claim
  +---------------------------------> CANCELED before dispatch
```

An owner timeout of `DISPATCHABLE` work is a lost-notification/dispatch
condition, not evidence of execution: the owner redispatches the same attempt
identity. Once an attempt is `CLAIMED`, the declared effect class controls the
timeout branch. `PURE_ACTIVITY` may use a replacement attempt; a
`COOPERATING_EFFECT` replacement must reuse the same logical effect key and
grant scope; a `NON_COOPERATING_EFFECT` timeout records an unknown outcome and
moves the workflow to `RECONCILIATION_REQUIRED` without a replacement.

Only the current attempt/claim token may add progress. A cooperating sink's
`OUTCOME_UNKNOWN` attempt is recovered with the same logical effect key or a
receipt lookup; if the receipt is confirmed, the attempt becomes `SUCCEEDED`
without a second applied mutation. A non-cooperating unknown effect moves the
workflow to `RECONCILIATION_REQUIRED` and is never automatically retried. A
dispatchable or created attempt may be canceled before claim; a claimed
in-flight attempt is not forcefully undone and its workflow cancellation is
best effort. A claimed effect attempt settled by cancellation is recorded as
`CANCELED`/`OUTCOME_UNKNOWN`; a late report may be recorded as evidence only,
without changing the terminal workflow or revision. Operator resolution of a
non-cooperating unknown attempt records
either an observed/applied outcome for owner advancement or `ABANDONED`; it
does not silently convert the attempt into a successful receipt. The operator,
disposition, argument identity, and any supplied receipt are retained in an
effect-resolution audit row.

An explicit graph timer uses the node's persisted `timer_fired` marker rather
than `retry_count`: scheduling creates one pending `WORKFLOW_TIMER`, the due
owner transition consumes it and sets `timer_fired`, and only then may the
timer node advance to its declared successor. Retry backoff timers remain
separate and continue to use retry metadata.

## State and transaction boundaries

The state machine is interpreted from persisted state; a Python process stack
is never the source of truth. A scheduler transaction takes locks in this
order: lease, workflow, node/attempt, then subordinate records. It validates
owner UUID, epoch, and unexpired lease using database time, then validates the
workflow revision before writing transition history and any outbox rows in the
same engine-PostgreSQL transaction. It never holds that transaction while
calling a worker, Kafka, a tool, or an approver.

The worker control API locks `workflow` first and then `node/attempt` in an
engine-PostgreSQL transaction. For progress it validates that the workflow is
non-terminal, re-reads the current attempt and claim token, and applies the attempt-specific
deadline policy. It does not reject a valid in-flight result solely because an
unrelated owner transition changed the workflow revision after the claim; it
rejects a replaced/terminal attempt, a mismatched claim token, or a policy
violation. A timed-out `NON_COOPERATING_EFFECT` attempt whose disposition is
`OUTCOME_UNKNOWN`, or a canceled claimed effect attempt with that disposition,
is an evidence-only exception: the API inserts an attempt-result-evidence row
linked to the reconciliation reference and returns `RECORDED_AS_EVIDENCE`, not
accepted. It does not change the attempt or workflow state, revision, or outbox.
It records a heartbeat,
checkpoint, or result receipt and, for a terminal result, a completion
wake-up/outbox row. It never changes the workflow state, creates a replacement
attempt, or schedules a node. A result retry after commit returns the durable
receipt; a late result after replacement is rejected without changing state.
This `workflow -> node/attempt` order is an ordered subsequence of the scheduler's
`lease -> workflow -> node/attempt -> subordinate` order, so result and
timeout transactions cannot acquire those rows in reverse order.

The M2 direct worker control surface is transport-neutral and is also the seam
used by the Python runner:

```text
POST /v1/workflows/{workflow}/nodes/{node}/iterations/{iteration}/claim
  {"worker_id", "request_id", "attempt_lease_ms"}
POST .../heartbeat
  {"attempt_number", "claim_token", "extension_ms"}
POST .../result
  {"attempt_number", "claim_token", "attempt_state", "payload"}
```

Claim retries use the same `request_id`; result retries use the same attempt
number and claim token. A lost claim response is retried with the same request
ID after restart. A result response lost after commit is retried with the same
token and returns the durable receipt without a second result event. A stale
claim is surfaced for scheduler recovery rather than executed outside the
claim.

A client cancellation request and an approver decision are intent records, not
workflow transitions. Their API transactions may lock `workflow` to capture
the observed revision and then insert the request/decision row, but do not
lock or write `node/attempt` and do not change workflow state. A lease-owning
scheduler later applies the request or decision in the declared
`lease -> workflow -> node/attempt -> subordinate` order, records history, and
commits any workflow transition and outbox row together.

## Transition permissions

| Transition | Required evidence | Rejected when |
| --- | --- | --- |
| Create workflow | New submission key, payload hash | Same key has a different payload |
| Acquire lease | Unowned/expired lease row, new incarnation UUID | Current owner is unexpired |
| Renew lease | Matching owner UUID and epoch, unexpired row | Owner/epoch mismatch or expiry |
| Schedule node | Matching lease and workflow revision | Stale epoch/revision |
| Claim attempt | Dispatchable current attempt and stable worker request ID | Already claimed by a different request |
| Heartbeat/checkpoint | Current claim token and compatible schema | Stale token, attempt, or checkpoint sequence |
| Accept result | Current claim token and one terminal attempt outcome | Timed out/replaced attempt |
| Record late reconciliation evidence | Timed-out non-cooperating attempt or canceled claimed effect attempt, unknown disposition, source claim, and evidence payload | Pure/unclaimed attempt, known outcome, or missing reconciliation reference |
| Timeout/redispatch attempt | Matching lease, owner/epoch/expiry, attempt state, deadline, and declared effect class | Stale lease, active claim not eligible, or missing effect-class policy |
| Publish event | Existing outbox row and stable event ID | Missing/unsupported event |
| Record approval decision | Approver identity, exact proposal hash/target/scope, and validity window | Changed, expired, or unauthorized proposal |
| Apply approval + create grant | Matching lease, current workflow revision, approved decision, exact proposal, and grant scope | Stale lease/revision, rejected/expired decision, or changed proposal |
| Record cancellation request | Client key and observed workflow revision | Duplicate/conflicting request or terminal workflow |
| Apply cancellation | Matching lease, current workflow state, and durable request | Stale lease/revision or an already-issued grant/effect that cannot be undone |
| Apply effect | Stable effect key, approved target resource, recomputed canonical argument hash, valid fence/grant | Resource/argument mismatch, key conflict, or lower sink fence token |

## Required ordering traces

The records below name the store explicitly: `EDB` is the engine PostgreSQL
database; `K` is the Kafka broker; `EL` is the cooperating effect service's
separate ledger/database. Each `BEGIN`/`COMMIT` is a real future transaction
boundary. External calls and Kafka publication occur outside the EDB state
transaction.

### 1. Submission retry

1. Client A sends `(submission_key, payload_hash)`. EDB transaction T1 locks
   the unique submission key, inserts `workflow`, `node`, `transition_history`,
   and the initial `outbox` row, then commits. The response is lost after
   commit.
2. Client A retries the identical pair. EDB transaction T2 reads the unique
   submission row and commits a receipt lookup. Outcome: the original workflow
   ID is returned and no second workflow/outbox row is created.
3. If the retry carries a different payload hash, T2 rolls back with a
   conflict; no workflow or history mutation is accepted.

### 2. Scheduler handoff, including a lock-held transaction

1. Owner A begins T_A, locks `partition_lease` and `workflow`, validates
   `(owner=A, epoch=e, unexpired)`, writes the workflow revision,
   `transition_history`, and any `outbox` row, and keeps the lease row lock.
2. Owner B begins T_B and attempts to lock the same `partition_lease`; it
   waits. If T_A commits first, B then obtains the row lock, checks database
   time, increments the epoch to `e+1`, and commits the new lease owner.
   Outcome: A's already-authorized transition is valid; B owns subsequent
   transitions.
3. If A starts a new transaction after B commits, it locks the lease and sees
   `(owner=B, epoch=e+1)`. T_A2 rolls back before writing workflow/history or
   outbox rows. Outcome: a stale owner cannot commit an owner-authorized write.
4. If A's T_A rolls back, B obtains the row after the lock wait and either
   acquires the expired/unowned lease or records that it is still unexpired;
   no partial A transition remains.

### 3. Worker timeout/result race

The worker result transaction uses `workflow -> node/attempt` because it must
validate the current revision and claim. The scheduler timeout and advancement
transactions use `lease -> workflow -> node/attempt -> subordinate`. The worker
never acquires the lease and never advances the workflow.

**Timeout wins:**

1. The lease-owning scheduler begins T_timeout, locks `partition_lease` first,
   validates `(owner, epoch, unexpired)` using database time, then locks
   `workflow`, `node`, and `attempt` in EDB. It validates the deadline and reads
   the immutable effect class before choosing one of the following branches.
2. If the attempt is `DISPATCHABLE` and unclaimed, T_timeout keeps that attempt
   identity dispatchable, records a lost-notification/redispatch history row,
   and writes a dispatch outbox row for the same attempt. It does not create a
   replacement attempt: an unclaimed deadline does not prove that an activity
   ran.
3. If the attempt is `CLAIMED` and `PURE_ACTIVITY`, T_timeout writes
   `TIMED_OUT`, a replacement attempt, history, and a dispatch outbox row, then
   commits. No irreversible effect is being retried.
4. If the attempt is `CLAIMED` and `COOPERATING_EFFECT`, T_timeout writes
   `TIMED_OUT`, a replacement attempt carrying the same logical effect key and
   grant scope, history, and a dispatch outbox row, then commits. The
   cooperating sink must deduplicate or return the existing receipt.
5. If the attempt is `CLAIMED` and `NON_COOPERATING_EFFECT`, T_timeout records
   `TIMED_OUT` with an `OUTCOME_UNKNOWN` effect disposition, reconciliation
   history/reference, and `WAITING_ACTIVITY -> RECONCILIATION_REQUIRED` in the
   same lease-first transaction. It creates no replacement attempt and no
   automatic effect dispatch.
6. A late old-worker T_result locks `workflow`, then `node/attempt`, and is
   accepted only if the current attempt/claim still permits that result. For a
   pure/cooperating replacement it returns a stale receipt. For a
   non-cooperating timeout, the API instead inserts an attempt-result-evidence
   row linked to the reconciliation reference, leaves the timed-out attempt
   and workflow unchanged, and returns `RECORDED_AS_EVIDENCE`. Outcome: no
   non-cooperating replacement can invoke the irreversible endpoint a second
   time, while an operator can use the late report to resolve the ambiguity.

**Result wins:**

1. Worker T_result locks `workflow`, then `node/attempt`, validates the current
   claim, and writes the result/checkpoint, attempt `SUCCEEDED`, a result
   history row, and a completion wake-up/outbox row. It commits without changing
   the workflow or node state.
2. Scheduler T_timeout then locks `partition_lease` first and validates its
   owner/epoch/expiry, then locks `workflow`, `node`, and `attempt`. It sees the
   terminal attempt, records an observed-race history if required, and commits
   without creating a replacement.
3. The lease owner consumes the completion wake-up in T_advance, taking locks
   in `lease -> workflow -> node/attempt` order, validates the workflow
   revision, and advances the next node to `RUNNABLE` or records the terminal
   workflow state with its history/outbox rows. Outcome: one accepted result
   and one owner-authorized downstream scheduling decision.

### 4. Outbox publication and duplicate delivery

1. Relay T_claim locks a bounded `outbox` batch in EDB, records a lease/claim,
   and commits. It publishes the stable event ID to K outside the transaction.
2. If it crashes before broker acknowledgment, the claim expires and a later
   relay republishes the same event ID. If it crashes after K acknowledgment
   but before its EDB publication mark, K contains a duplicate with the same
   ID.
3. The consumer receives the event and begins T_inbox in EDB. It inserts the
   `(consumer, event_id)` `event_inbox` disposition and applies one durable
   workflow wake-up/hint, then commits. Only after this durable disposition
   does it commit the corresponding K offset.
4. A duplicate begins T_duplicate; the unique event ID returns the existing
   disposition, no second wake-up/transition is applied, T_duplicate commits,
   and its K offset is acknowledged. Outcome: duplicate transport is visible
   and harmless.

Event types use one explicit producer/relay/checker registry. Task commands
(`attempt.dispatch` and `attempt.redispatch`) use the task topic; completion
and workflow events, including `activity.result`, use the event topic. The
optional worker result `event_type` normalizes to `activity.result`; no topic
is inferred from an event-name prefix.

Malformed or unknown broker records cannot be linked to an outbox foreign key.
The consumer stores their raw bytes and delivery identity in the durable
transport-quarantine table, marks the disposition `QUARANTINED`, and advances
the contiguous offset only after that commit. Each quarantine path also creates
an open `POISON_RECORD` reconciliation obligation: relay quarantine links it to
the workflow, while a broker record with no trustworthy event ID is a global
operator obligation keyed by consumer/topic/partition/offset. Quarantine is
evidence and acknowledgment, not permission to execute the record. Backlog
reporting includes poison count and oldest-obligation age, and the checker
rejects a quarantined outbox row that lacks its poison obligation.

### 5. Ambiguous effect response

**Cooperating sink:**

1. The worker calls EL outside EDB with the stable logical effect key. EL
   validates the grant against EDB before starting transaction E_effect.
   E_effect then writes only the independently owned effect-service ledger:
   it atomically applies the sandbox change and inserts the effect receipt,
   then commits. There is no cross-table transaction with EDB. The response is
   lost before the worker writes the EDB result, leaving the current attempt
   `OUTCOME_UNKNOWN` in EDB.
2. Recovery calls EL with the same key/payload or its receipt lookup. EL returns
   the existing receipt; it does not apply a second mutation. EDB transaction
   T_receipt locks `workflow`, then `node/attempt`, records the receipt/result
   and history/outbox, and commits without changing workflow state. The lease
   owner later runs T_advance in lease-first order and applies the accepted
   result. Outcome: `SUCCEEDED`; manual reconciliation is not required merely
   because the cooperating response was lost.
3. If EL is unavailable, EDB keeps the attempt pending/unknown and retries the
   lookup within the declared policy. Only if that policy expires without the
   cooperating contract becoming observable does the workflow enter
   `RECONCILIATION_REQUIRED`.

**Non-cooperating sink:**

1. The external call may mutate an unobservable target and lose its response;
   there is no EL receipt lookup. Worker/control transaction T_unknown locks
   `workflow`, then `node/attempt`, records `OUTCOME_UNKNOWN`, history, and the
   reconciliation reference, and commits without changing workflow state.
2. The lease owner starts T_reconcile, locks `partition_lease` first and
   validates owner/epoch/expiry, then locks `workflow` and `node/attempt`,
   applies `RECONCILIATION_REQUIRED`, and commits. No automatic retry is
   issued.
3. An operator records independently verified receipt evidence or an abandon
   decision. The lease owner starts T_resolve in lease-first order and either
   advances to the next node or commits `ABANDONED`; the engine never claims
   that a missing response proves no effect.

### 6. Approval/cancellation race

**Cancellation before grant:**

1. Client transaction T_cancel_request locks `workflow` only to capture its
   revision, inserts a durable cancellation request in EDB, and commits. It
   does not write `CANCELED`.
2. The lease owner begins T_apply_cancel, locks `partition_lease` first and
   validates owner/epoch/expiry, then locks `workflow` and the relevant
   approval/attempt rows. It sees no durable dispatch grant, applies
   `CANCELED`, records history, marks the request applied, and commits. No
   task/outbox effect dispatch is created.
3. Approver transaction T_approve_decision may later record a decision for the
   exact proposal, but the owner sees `CANCELED` and records the decision as
   rejected/no grant. The approver never changes workflow state.

**Grant before cancellation:**

1. Approver transaction T_approve_decision validates the approver, exact
   proposal hash, expiry, and resource version, writes an approval decision in
   EDB, and commits. It does not write `RUNNABLE` or a dispatch grant.
2. The lease owner begins T_apply_approval, locks `partition_lease` first and
   validates owner/epoch/expiry, then locks `workflow`, the exact proposal, and
   node/attempt rows. It writes the approved intent and durable dispatch grant,
   applies `WAITING_APPROVAL -> RUNNABLE`, records history/outbox, and commits.
   The grant is the authorization boundary; dispatch is later work.
3. Client transaction T_cancel_request later records a cancellation request.
   The lease owner begins T_apply_cancel, takes the same lease-first order,
   sees the grant/request issued, and records a cancellation request and
   best-effort status. It cannot revoke an already-issued external request or
   undo an applied effect.
4. The worker dispatches only the exact granted action. Its EDB result and the
   EL receipt are recorded under the stable effect key. Outcome: the effect may
   complete despite cancellation, and the timeline shows decision, grant,
   cancellation, and effect ordering separately.

If both intent records exist before the owner runs, the lease-first owner
transaction serializes them. The row/revision order determines one outcome:
`CANCELED` before grant, or `RUNNABLE` with a grant followed by a best-effort
cancellation request. No client or approver transaction can bypass that owner
decision.

## Failure-model boundary

The contracts cover process crashes, duplicate/reordered delivery, lease and
claim expiry, database transaction conflicts, broker unavailability, and
cooperating or explicitly non-cooperating effect endpoints. They do not claim
storage replication, automatic database failover, arbitrary-code replay,
exactly-once execution, or immediate cancellation of already-issued external
requests.
