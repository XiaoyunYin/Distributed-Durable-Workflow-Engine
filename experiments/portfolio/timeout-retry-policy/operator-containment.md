# J18 D14 poison workflow containment

## Identify the workflow

Use `GET /v1/workflows/{workflowID}` for current state and revision and
`GET /v1/workflows/{workflowID}/history` for transitions. Page through
history and look for repeated `TIMEOUT_REPLACEMENT` records for the same
node and iteration, with increasing attempt numbers and no accepted result.
Correlate timeout times with worker exit reasons, OOM events, heartbeat gaps,
rollouts and scale-downs. A replacement sequence identifies repeated claim
loss, which can come from either infrastructure or the activity. Sustained
oldest-claimable age can point to backlog. A workflow that is quickly claimed
and kills each worker can churn while claimable age stays low.

## Policy enabled

Look for workflow `FAILED` and a final history entry with
`TIMEOUT_REPLACEMENT_BUDGET_EXHAUSTED` or
`TIMEOUT_REPLACEMENT_DEADLINE_EXCEEDED`. Record that revision, node, iteration
and last attempt. The final attempt is noncurrent and the terminal revision
creates no dispatch. A notice emitted before failure may still arrive, and
the claim fence will reject it. Stop admitting the suspected definition or
input at the submitting client or ingress. For cooperating effects, inspect
the effect ledger by logical effect key and resolve any ambiguous outcome
before submitting corrected work with a new submission key. Check any
canceled sibling effect marked `OUTCOME_UNKNOWN` against its external
records before reissuing it.

## Policy unset

Stop admitting the suspected definition or input and contain active churn at
the smallest worker pool that can serve it. If the workflow shares a pool and
the worker crashes threaten the service, pause that pool during containment
and account for the other workflows that share it.

On the operator listener, submit
`POST /v1/workflows/{workflowID}/cancel-request` with a stable `client_key`
and the last observed `observed_revision`. Have the current partition owner
apply the returned request ID using `POST /v1/cancellations/{requestID}/apply`
with the current `partition_id`, `owner_id`, `epoch` and operator identity,
or let the external-activity scheduler apply it on its repair scan.
An acknowledged request is pending until applied. Verify `CANCELED` and a
cancellation history entry before resuming the affected pool. A granted or
dispatched approval can yield `BEST_EFFORT` and leave the workflow active.
Keep the affected execution isolated and use the approval and effect records
to establish the outcome before recovery. Preserve attempt, history and
outbox evidence for incident review.

In either mode, repeated `REDISPATCH_UNCLAIMED` for the same attempt signals
a delivery or worker-capacity problem. `TIMEOUT_RECONCILIATION_REQUIRED`
signals a non-cooperating effect with an unknown outcome. Investigate the
retained reconciliation reference and verify the external outcome before
reissuing the operation.
