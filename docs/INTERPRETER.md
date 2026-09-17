# M1 interpreter contract

The DUR-007 interpreter is a test-only scheduler over the durable PostgreSQL
state repository. PostgreSQL state, not an in-process call stack, is the
resume point. The interpreter may be stopped after any committed repository
operation and a fresh instance can continue from the workflow, node, attempt,
and timer rows.

## Versioned graph shape

Definitions store an immutable JSON graph. The minimum object form is:

```json
{
  "entry": "start",
  "nodes": [
    {"id": "start", "kind": "activity", "next": "fanout"},
    {"id": "fanout", "kind": "fanout", "branches": ["left", "right"], "join": "join"},
    {"id": "left", "kind": "activity", "next": "join"},
    {"id": "right", "kind": "activity", "next": "join"},
    {"id": "join", "kind": "join", "next": "done"},
    {"id": "done", "kind": "success"}
  ]
}
```

Supported node kinds are `activity`, `fanout`/`fan_out`, `join`, `timer`,
`success`/`succeed`, and `failure`/`fail`. `next` is one node ID or an array;
an activity returning multiple choices must return a JSON `{"next":"..."}`
selection. A timer may declare `delay_ms`; its due time is persisted in
`engine.timers` with `purpose=WORKFLOW_TIMER`.

The legacy `{"nodes":["root"]}` shape remains readable for the repository's
earlier fixtures and treats each string node as an activity. Every activity's
effect class remains authoritative in the immutable definition metadata.

## Durable execution rules

- The lease owner schedules and advances nodes. Worker drivers only return an
  attempt result; `ConsumeResult` records it before graph advancement.
- Retryable results create a `RETRY_BACKOFF` timer. Explicit graph timers use
  `WORKFLOW_TIMER`. Both are due-checked and consumed in the owner transaction.
- Fan-out creates bounded branch node instances and one join node using a
  uniqueness key `(workflow_id,node_id,iteration)`. Repeated branch completion
  therefore cannot create a second join instance or downstream node.
- A join runs only after every persisted dependency node is `SUCCEEDED`.
- Cancellation settles every non-terminal node in the workflow. A claimed
  effect attempt becomes `CANCELED/OUTCOME_UNKNOWN`; a late report is evidence
  only and cannot change the terminal workflow revision.

The activity driver in `internal/engine` is deliberately a test fixture. Kafka
relay, production effect services, and paid/model execution are later scope.

## M2 worker runner seam

`python/workers/runner.py` is the bounded direct-dispatch runner for M2. It
resolves only versioned registry entries, claims through the control API,
heartbeats while an activity is running, and submits one terminal result. Its
`run_many` method bounds local activity concurrency. It does not own workflow
transitions or connect to Kafka; the Kafka adapter is M3.

If a process loses a claim response, the request ID and attempt token remain
the durable retry identity. A restarted process retries the claim with the
same request ID; the repository returns the committed claim or a stale-claim
error. A result response lost after commit is retried with the same token and
returns the durable receipt without a second result event.
