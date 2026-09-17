# Foundation contracts (DUR-002)

This document freezes the actor boundaries and failure outcomes that later
engine tasks implement. It is a protocol document, not a claim that the
durable engine already exists.

## Version and scope

- Contract version: `dur-002.v1`.
- Workflow partition-map version: `sha256-u64-be-v1`.
- Partition count: 16.
- Workflow IDs are non-empty UTF-8 strings.
- The partition is `big_endian_u64(SHA-256(workflow_id)[0:8]) % 16`.
- Canonical vectors are implemented independently in Go and Python and tested
  in `internal/partition/partition_test.go` and
  `tests/test_partition_vectors.py`.

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
message ID. The database transaction owns the authoritative ordering.

## Identity separation

1. A client submission key deduplicates workflow creation.
2. A logical activity/effect key stays stable across retries of one operation.
3. An attempt number and claim token identify one authorized worker execution.
4. A scheduler epoch identifies one partition-ownership generation.
5. A message/event ID identifies one transport record.

Repetition of a message does not create an attempt. Replacing an attempt does
not create a new logical effect key.

## State and transaction boundaries

The initial workflow state progression is:

```text
                 +--------------------+
                 |      RUNNABLE      |
                 +----------+---------+
                            |
                 scheduler transition
                            v
                 +--------------------+
                 |   WAITING_ACTIVITY |<----+
                 +----------+---------+     |
                            |                | retry
                  accepted result           |
                            v                |
                 +--------------------+     |
                 |   WAITING_TIMER    +-----+
                 +----------+---------+
                            |
                            v
                 +--------------------+
                 | SUCCEEDED/FAILED   |
                 +--------------------+

  any non-terminal state -- cancellation winner --> CANCELED
  ambiguous external outcome ----------------------> RECONCILIATION_REQUIRED
```

The state machine is interpreted from persisted state; a Python process stack
is never the source of truth. A scheduler transaction takes locks in this
order: lease, workflow, node/attempt, then subordinate records. It validates
owner UUID, epoch, and unexpired lease using database time, then validates the
workflow revision before writing the transition history and any outbox rows in
the same transaction. It never holds that transaction while calling a worker,
Kafka, a tool, or an approver.

The worker control API validates the current attempt, claim token, workflow
state, and deadline policy in the same transaction as a heartbeat, checkpoint,
or result receipt. A result retry after commit returns the durable receipt; a
late result after replacement is rejected without changing state.

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

| Scenario | Ordering | Durable outcome |
| --- | --- | --- |
| Submission retry | Commit create, lose response, retry same key/hash | Return the one existing workflow; different hash is a conflict |
| Scheduler handoff | Owner A lease expires; B acquires a higher epoch; A starts a new write | A is rejected at the fenced transaction; B may proceed |
| Worker timeout/result race | Timeout/replacement and old result contend on the attempt row | One lock winner is recorded; the loser is a rejected stale operation |
| Outbox duplicate | Relay publishes, crashes before publication mark, republishes | Same event ID is visible twice; consumer disposition is idempotent |
| Ambiguous effect | Effect commits, worker loses response, engine sees no receipt | `RECONCILIATION_REQUIRED`; no automatic non-idempotent retry |
| Approval/cancellation race | Cancellation and approval compete before dispatch | One serialized winner is recorded; no new effect without a matching grant |

The effect ledger is authoritative for the cooperating sink's applied state;
the engine never converts a missing response into proof of no effect. An
operator may resolve an unknown outcome only with an audited evidence record.

## Failure-model boundary

The contracts cover process crashes, duplicate/reordered delivery, lease and
claim expiry, database transaction conflicts, broker unavailability, and
cooperating or explicitly non-cooperating effect endpoints. They do not claim
storage replication, automatic database failover, arbitrary-code replay,
exactly-once execution, or immediate cancellation of already-issued external
requests.
