# Foundation contracts (DUR-002)

This document freezes the actor boundaries and failure outcomes that later
engine tasks implement. It is a protocol document, not a claim that the
durable engine already exists.

## Version and scope

- Contract version: `dur-002.v2` (the state-machine and trace clarification
  correction to the unreviewed `dur-002.v1` draft).
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
| Client | Submit request with a client submission key; request cancellation/approval | Workflow revision, attempt ownership, or external effects |
| Scheduler holding `(partition, owner UUID, epoch)` | Fenced workflow/node/timer transitions, retries, and outbox records | Work outside its current lease; worker results without attempt validation |
| Worker with a current claim token | Heartbeat, compatible checkpoint, and one terminal result through the control API | Scheduler ownership, retry creation, or direct database writes |
| Relay/consumer | Transport claim/publication/disposition records | Workflow transitions or activity authorization |
| Approver | Approve/reject the exact canonical proposal within its validity window | A different proposal or an effect directly |
| Effect service | Its own effect ledger and sandbox state under a stable effect key/token | Engine workflow state or approval records |
| Reconciler/operator | Bounded scans; audited reconciliation resolution | Pretending an unknown effect did not happen |

Every durable mutation is attributed to an actor identity and records its
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
| `RUNNABLE` | No | Current owner may schedule the next node; exits to `WAITING_ACTIVITY`, `WAITING_APPROVAL`, `WAITING_TIMER`, or a terminal state for an empty/complete graph. |
| `WAITING_ACTIVITY` | No | A current node has dispatchable/claimed work; accepted result advances directly to the next node or terminal state. Retryable failure exits to `WAITING_TIMER`; non-cooperating unknown effect exits to `RECONCILIATION_REQUIRED`. |
| `WAITING_TIMER` | No | A durable retry backoff or explicit timer is pending; due timer returns to `RUNNABLE`. It is not required after every successful result. |
| `WAITING_APPROVAL` | No | Exact proposal is durable and no lock or worker slot is held; approval creates a durable dispatch grant, rejection returns to a recorded no-action terminal result, and cancellation prevents a grant if it wins first. |
| `PAUSED_UNSUPPORTED_VERSION` | No | Definition/activity/checkpoint version is unsupported; an audited version-availability decision returns to `RUNNABLE`, otherwise operator abandonment is terminal. |
| `RECONCILIATION_REQUIRED` | No | A non-cooperating effect is outcome-unknown, or a cooperating receipt lookup remains unavailable past the declared recovery policy; audited receipt evidence can return to the next node, while audited abandonment exits to `ABANDONED`. |
| `SUCCEEDED` | Yes | Workflow completed with an accepted result. |
| `FAILED` | Yes | Permanent failure or exhausted retry budget was recorded. |
| `CANCELED` | Yes | Cancellation won before a new effect dispatch, or cancellation was accepted as the final workflow outcome; it does not undo an already-applied effect. |
| `ABANDONED` | Yes | An operator recorded that an unresolved outcome will not be resumed. |

```text
RUNNABLE --schedule node--> WAITING_ACTIVITY --accepted result--> RUNNABLE
    |                              |                               |
    |                              +--> SUCCEEDED/FAILED             +--> WAITING_APPROVAL
    |                              +--> WAITING_TIMER --due---------> RUNNABLE
    |                              +--> RECONCILIATION_REQUIRED ----> RUNNABLE/ABANDONED
    |                                                                  |
    +---------------------------------------------------------------> CANCELED

WAITING_APPROVAL --approve + grant--> RUNNABLE --schedule effect--> WAITING_ACTIVITY
WAITING_APPROVAL --reject---------------------------------------> FAILED/no-action
PAUSED_UNSUPPORTED_VERSION --supported version-------------------> RUNNABLE
```

Cancellation is a serialized workflow transition. Before a durable dispatch
grant it prevents new dispatch; after a grant/request is issued it is best
effort and cannot undo an effect. Approval waits hold no database lock and
consume no worker execution slot.

### Activity-attempt state machine

```text
CREATED --> DISPATCHABLE --> CLAIMED --> SUCCEEDED
                              |   |----> FAILED_RETRYABLE --> replacement attempt
                              |   |----> FAILED_FINAL
                              |   |----> TIMED_OUT --> REPLACED
                              |   +----> OUTCOME_UNKNOWN --> receipt-confirmed result
                              +------ late progress/result is rejected
```

Only the current attempt/claim token may add progress. A cooperating sink's
`OUTCOME_UNKNOWN` attempt is recovered with the same logical effect key or a
receipt lookup; if the receipt is confirmed, the attempt becomes `SUCCEEDED`
without a second applied mutation. A non-cooperating unknown effect moves the
workflow to `RECONCILIATION_REQUIRED` and is never automatically retried.

## State and transaction boundaries

The state machine is interpreted from persisted state; a Python process stack
is never the source of truth. A scheduler transaction takes locks in this
order: lease, workflow, node/attempt, then subordinate records. It validates
owner UUID, epoch, and unexpired lease using database time, then validates the
workflow revision before writing transition history and any outbox rows in the
same engine-PostgreSQL transaction. It never holds that transaction while
calling a worker, Kafka, a tool, or an approver.

The worker control API validates the current attempt, claim token, workflow
state, and deadline policy in the same engine-PostgreSQL transaction as a
heartbeat, checkpoint, or result receipt. A result retry after commit returns
the durable receipt; a late result after replacement is rejected without
changing state.

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

**Timeout wins:**

1. Scheduler T_timeout locks `workflow`, `node`, and `attempt` in EDB,
   validates the timeout, writes attempt `TIMED_OUT`, a replacement attempt,
   `transition_history`, and a dispatch `outbox` row, then commits.
2. The old worker's T_result later locks the same attempt, finds the old claim
   token/current-attempt check false, rolls back, and returns a stale receipt.
   Outcome: no result, checkpoint, or downstream node is changed by the late
   worker.

**Result wins:**

1. Worker T_result locks the current attempt and claim token in EDB, writes the
   result/checkpoint, attempt `SUCCEEDED`, `transition_history`, and a
   completion `outbox` row, then commits. The next node is made `RUNNABLE` or
   the workflow becomes terminal in this same transaction.
2. Scheduler T_timeout then locks the attempt, sees the terminal result, records
   a no-op/observed-race history if required, and commits without replacement.
   Outcome: one accepted result and one downstream scheduling decision.

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
   T_receipt locks the attempt, records the receipt/result and history/outbox,
   and commits. Outcome: `SUCCEEDED`; manual reconciliation is not required
   merely because the cooperating response was lost.
3. If EL is unavailable, EDB keeps the attempt pending/unknown and retries the
   lookup within the declared policy. Only if that policy expires without the
   cooperating contract becoming observable does the workflow enter
   `RECONCILIATION_REQUIRED`.

**Non-cooperating sink:**

1. The external call may mutate an unobservable target and lose its response;
   there is no EL receipt lookup. EDB transaction T_unknown locks the attempt,
   records `OUTCOME_UNKNOWN`, history, and the reconciliation reference, then
   commits the workflow in `RECONCILIATION_REQUIRED`.
2. No automatic retry is issued. An operator later starts EDB transaction
   T_resolve with independently verified receipt evidence and either advances
   to the next node or commits `ABANDONED`; the engine never claims that a
   missing response proves no effect.

### 6. Approval/cancellation race

**Cancellation before grant:**

1. T_cancel locks `workflow` and `approval/action_intent` in EDB, sees no
   durable dispatch grant, records cancellation and history, and commits.
2. T_approve later locks the same rows, sees `CANCELED`, records a rejected
   approval/no grant, and commits. No task/outbox effect dispatch is created.

**Grant before cancellation:**

1. T_approve locks the workflow and exact proposal, validates approver,
   proposal hash, expiry, and resource version, writes the approved intent and
   durable dispatch grant in EDB, then commits. The grant is the authorization
   boundary; dispatch is later work.
2. T_cancel later locks the same rows, sees the grant/request issued, records a
   cancellation request and best-effort status, and commits. It cannot revoke
   an already-issued external request or undo an applied effect.
3. The worker dispatches only the exact granted action. Its EDB result and the
   EL receipt are recorded under the stable effect key. Outcome: the effect may
   complete despite cancellation, and the timeline shows grant, cancellation,
   and effect ordering separately.

## Failure-model boundary

The contracts cover process crashes, duplicate/reordered delivery, lease and
claim expiry, database transaction conflicts, broker unavailability, and
cooperating or explicitly non-cooperating effect endpoints. They do not claim
storage replication, automatic database failover, arbitrary-code replay,
exactly-once execution, or immediate cancellation of already-issued external
requests.
