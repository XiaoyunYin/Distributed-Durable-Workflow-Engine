# Foundation contracts (DUR-002)

This document freezes the actor boundaries and failure outcomes that later
engine tasks implement. It is a protocol document, not a claim that the
durable engine already exists.

## Version and scope

- Contract version: `dur-002.v3` (the authority, lock-order, and state-table
  correction to `dur-002.v2`).
- Workflow partition-map version: `sha256-u64-be-v1`.
- Partition count: 16.
- Workflow IDs are non-empty UTF-8 strings.
- The partition is `big_endian_u64(SHA-256(workflow_id)[0:8]) % 16`.
- The canonical vectors live in `api/partition-map-v1.vectors.json` and are
  loaded independently by the Go and Python tests.

Changing the algorithm, partition count, byte order, or map version requires a
new migration/contract version and is outside this milestone.

## Durable actors and authority

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

## Workflow state machine

The following are workflow states, not process-local states:

| State | Terminal? | Meaning and permitted exits |
| --- | --- | --- |
| `RUNNABLE` | No | The current lease owner may schedule the next node; exits to `WAITING_ACTIVITY`, `WAITING_APPROVAL`, `WAITING_TIMER`, `CANCELED`, or a terminal state for an empty/complete graph. |
| `WAITING_ACTIVITY` | No | A current node has dispatchable/claimed work. The worker records only the attempt result; the lease owner later advances to the next node or terminal state. Cancellation may prevent an unclaimed dispatch or become a best-effort request for an in-flight attempt. Retryable failure exits to `WAITING_TIMER`; non-cooperating unknown effect exits to `RECONCILIATION_REQUIRED`. |
| `WAITING_TIMER` | No | A durable retry/backoff expiry is pending; due expiry returns to `RUNNABLE`, or the lease owner applies a cancellation to `CANCELED`. This state is not used for a successful result or a general-purpose business timer. |
| `WAITING_APPROVAL` | No | Exact proposal is durable and no lock or worker slot is held. An approver records a decision; the lease owner applies approval by creating a dispatch grant and moving to `RUNNABLE`, or applies rejection as `REJECTED`/no-action. Cancellation prevents a grant if the owner applies it first. |
| `PAUSED_UNSUPPORTED_VERSION` | No | Definition/activity/checkpoint version is unsupported; the lease owner applies an audited version-availability decision to return to `RUNNABLE`, cancellation to `CANCELED`, or operator abandonment to `ABANDONED`. |
| `RECONCILIATION_REQUIRED` | No | A non-cooperating effect is outcome-unknown, or a cooperating receipt lookup remains unavailable past the declared recovery policy; audited receipt evidence can return to the next node, an audited cancellation can exit to `CANCELED` without claiming the effect was undone, and audited abandonment exits to `ABANDONED`. |
| `SUCCEEDED` | Yes | Workflow completed with an accepted result. |
| `FAILED` | Yes | Permanent failure or exhausted retry budget was recorded. |
| `REJECTED` | Yes | Approval was denied and the workflow completed with a recorded no-action result; this is restraint, not an execution failure. |
| `CANCELED` | Yes | Cancellation won before a new effect dispatch, or cancellation was accepted as the final workflow outcome; it does not undo an already-applied effect. |
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
WAITING_APPROVAL --owner applies approval + grant--> RUNNABLE
WAITING_APPROVAL --owner applies rejection--------> REJECTED/no-action
WAITING_APPROVAL --owner applies cancellation-----> CANCELED
WAITING_TIMER --owner applies cancellation--------> CANCELED
PAUSED_UNSUPPORTED_VERSION --owner applies version> RUNNABLE
PAUSED_UNSUPPORTED_VERSION --owner cancellation---> CANCELED
RECONCILIATION_REQUIRED --audited resolution------> RUNNABLE/CANCELED/ABANDONED
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
  |              |             |  |----> TIMED_OUT --> REPLACED --> replacement attempt
  |              |             |  +----> OUTCOME_UNKNOWN --> receipt-confirmed result
  |              |             |                         \-> reconciled/abandoned outcome
  |              |             +------ late progress/result is rejected
  |              +------------------> CANCELED before claim
  +---------------------------------> CANCELED before dispatch
```

Only the current attempt/claim token may add progress. A cooperating sink's
`OUTCOME_UNKNOWN` attempt is recovered with the same logical effect key or a
receipt lookup; if the receipt is confirmed, the attempt becomes `SUCCEEDED`
without a second applied mutation. A non-cooperating unknown effect moves the
workflow to `RECONCILIATION_REQUIRED` and is never automatically retried. A
dispatchable or created attempt may be canceled before claim; a claimed
in-flight attempt is not forcefully undone and its workflow cancellation is
best effort. Operator resolution of a non-cooperating unknown attempt records
either an observed/applied outcome for owner advancement or `ABANDONED`; it
does not silently convert the attempt into a successful receipt.

## State and transaction boundaries

The state machine is interpreted from persisted state; a Python process stack
is never the source of truth. A scheduler transaction takes locks in this
order: lease, workflow, node/attempt, then subordinate records. It validates
owner UUID, epoch, and unexpired lease using database time, then validates the
workflow revision before writing transition history and any outbox rows in the
same engine-PostgreSQL transaction. It never holds that transaction while
calling a worker, Kafka, a tool, or an approver.

The worker control API validates the current workflow revision, current
attempt, claim token, and deadline policy in an engine-PostgreSQL transaction
that locks `workflow` first and then `node/attempt`. It records a heartbeat,
checkpoint, or result receipt and, for a terminal result, a completion
wake-up/outbox row. It never changes the workflow state, creates a replacement
attempt, or schedules a node. A result retry after commit returns the durable
receipt; a late result after replacement is rejected without changing state.
This `workflow -> node/attempt` order is a prefix of the scheduler's
`lease -> workflow -> node/attempt -> subordinate` order, so result and
timeout transactions cannot acquire those rows in reverse order.

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
| Publish event | Existing outbox row and stable event ID | Missing/unsupported event |
| Approve action | Exact proposal hash, target, scope, and valid grant | Changed, expired, or rejected proposal |
| Apply effect | Stable effect key, canonical argument hash, valid fence/grant | Key conflict or lower sink fence token |

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
   `workflow`, `node`, and `attempt` in EDB. It validates the deadline, writes
   attempt `TIMED_OUT`, a replacement attempt, `transition_history`, and a
   dispatch `outbox` row, then commits.
2. The old worker's T_result later locks `workflow`, then `node/attempt`, finds
   the old claim token/current-attempt check false, rolls back, and returns a
   stale receipt. Outcome: no result, checkpoint, or downstream node is changed
   by the late worker; the owner-created replacement is the only dispatchable
   attempt.

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

### 5. Ambiguous effect response

**Cooperating sink:**

1. The worker calls EL outside EDB with the stable logical effect key. EL
   transaction E_effect atomically applies the sandbox change and inserts the
   effect receipt, then commits. The response is lost before the worker writes
   the EDB result, leaving the current attempt `OUTCOME_UNKNOWN` in EDB.
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
