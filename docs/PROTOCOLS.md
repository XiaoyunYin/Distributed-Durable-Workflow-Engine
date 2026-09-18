# Durable protocol records

This document is the implementation-facing companion to `CONTRACTS.md`. The
engine database is authoritative for workflow state; the effect ledger is
authoritative only for the cooperating sink's protected state.

## Retry and checkpoint protocol

Each node has a durable retry policy `(max_retries, retries_used,
total_deadline_at, checkpoint_schema_version)`. A retryable result is consumed
by the lease owner into a due `RETRY_BACKOFF` timer. The budget is incremented
in the same transaction; an exhausted budget becomes `FAILED` and cannot loop.

Checkpoint writes require the current attempt and claim token, lock rows in
`workflow -> node -> attempt` order, and carry `(sequence, schema_version,
payload_hash, source_attempt_number)`. Sequence zero starts a stream. Repeating
the same sequence and hash is idempotent; a gap, stale sequence, or schema
version mismatch is rejected. Checkpoint payloads are progress evidence, not an
external-effect receipt.

## Effect protocol

The cooperating sandbox accepts an `EffectApplyInput` only with a matching
approved grant, canonical argument hash recomputed from the submitted state,
stable logical effect key, expected resource revision, request ID, and
resource-local fence token. The grant scope is derived from the exact
proposal, approved target resource, expected resource revision, and logical
effect key. Grant validation reads the engine's grant record before the sink
transaction begins; the sink transaction itself writes only the independently
owned `effects` schema. It locks the resource fence, checks the expected
revision and token, updates sandbox state, stores one effect receipt, and
stores a separate call-attempt row. The first successful application marks the
engine grant `DISPATCHED` in a separate best-effort update; a retry can return
the receipt but cannot change the approved resource or arguments. There is
deliberately no foreign key or cross-table transaction from the effect ledger
back into `engine`.

Same-key/same-argument calls return the stored receipt; a different argument or
grant scope is a conflict. A lower resource token is rejected. An
`OUTCOME_UNKNOWN` record blocks automatic re-application until an operator
confirms independently verified receipt evidence or records abandonment. Both
resolutions are actor-attributed in `effect_resolution_audit`.

The non-cooperating fixture intentionally exposes no lookup or deduplication
API. If its response is lost, the engine records `OUTCOME_UNKNOWN` and an open
reconciliation item and does not call it again automatically.

## Approval and cancellation protocol

An approval proposal stores the exact target, canonical arguments, expected
resource revision, proposal hash/signature, and expiry. An approver records a
decision for that hash. Only the lease owner can apply an approved decision and
issue the bounded grant. A rejected decision is applied as terminal
`REJECTED`/no-action; it never issues a grant.

Cancellation is a two-step client intent/owner application protocol. The owner
locks the lease and workflow before checking for an already issued grant. A
pending cancellation prevents approval grant creation. If a grant already
exists, cancellation is recorded as `BEST_EFFORT`; it never claims to undo an
effect. Otherwise the owner settles non-terminal nodes and marks the request
`APPLIED` in the same transaction.

## API identity and error policy

The approval and worker control endpoints are localhost-bound, development-only
surfaces. Production authentication and binding of actor/worker identity are
prerequisites for exposing them beyond localhost. Permanent client mistakes
map to typed 4xx responses; database availability failures map to 503, whose
outcome is retried only with the same durable identity.
