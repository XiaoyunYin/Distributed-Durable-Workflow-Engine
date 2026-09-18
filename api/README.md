# API

DUR-006 adds the first versioned client API. The runtime serves these routes
when `DATABASE_URL` is configured. This is a development-only API: it has no
authentication, and the standalone runtime binds to `127.0.0.1` by default.
The local Compose ports are also published only on localhost; do not expose
the API directly to an untrusted network.

- `POST /v1/workflows` submits a workflow. The server canonicalizes and hashes
  the execution-defining submission `{definition_id, definition_version,
  initial_node_id, initial_input, payload}`; `namespace` plus
  `submission_key` is the idempotency scope. `workflow_id` and `actor_id` are
  transport/audit fields and are not part of that identity.
- `GET /v1/workflows/{workflow_id}` returns status and accepts optional
  `expected_revision` for a stale-read check.
- `GET /v1/workflows/{workflow_id}/history` returns ordered transition history.
  `after_revision` is a cursor and `limit` is 1–1000 (default 100).

The worker control seam is:

- `POST /v1/workflows/{workflow_id}/nodes/{node_id}/iterations/{iteration}/claim`
  with `worker_id`, `request_id`, and optional `attempt_lease_ms`.
- `POST .../heartbeat` with `attempt_number`, `claim_token`, and optional
  `extension_ms`.
- `POST .../result` with `attempt_number`, `claim_token`, `attempt_state`, and
  `payload`, and optional `event_type`; it returns the durable result receipt. An
  omitted event type is normalized to the publishable `activity.result` event.
  An explicit unsupported event type is rejected with `422 INVALID_EVENT_TYPE`.
  Claim retries reuse
  `request_id`; result retries reuse the attempt number and token.

These endpoints are the direct M2 control seam, not the M3 Kafka transport.
They are unauthenticated and accept caller-declared `worker_id` because this
is a localhost-bound development API only. Authentication and identity
binding are prerequisites for the M4 approval/effect work; do not expose this
write-capable API to an untrusted network before that work lands.

If a response is lost after commit, retry the same execution-defining
submission and idempotency key. The API returns the existing workflow with
`created: false`. A changed execution-defining field returns `409
PAYLOAD_CONFLICT`; an unknown definition returns `404 DEFINITION_NOT_FOUND`;
an undeclared initial node returns `422 UNKNOWN_INITIAL_NODE`; and a reused
`workflow_id` returns `409 WORKFLOW_ID_CONFLICT`. Unknown workflows return
`404 NOT_FOUND`; a stale revision or cursor returns `409 STALE_REVISION`.
Database connection failures return `503 DATABASE_UNAVAILABLE`.

JSON object keys must be unique; duplicate keys are rejected. Canonicalization
sorts object keys, while JSON number spellings remain distinct (`1` and `1.0`)
because the decoder preserves their lexical form. The stored hash is prefixed
`sub-v1:`; a future canonicalization change must use a new version prefix.

A `503 DATABASE_UNAVAILABLE` response means the request did not receive a
definitive database outcome. A connection or commit failure can be ambiguous,
so retry the same execution-defining submission and idempotency key rather than
creating a new workflow identity.

The DUR-006 retention policy is conservative: workflow and transition-history
rows are not automatically pruned by this API, and history remains queryable
for the lifetime of the workflow. Retention/deletion is a later operational
policy and must preserve the documented ambiguous-client lookup behavior.
