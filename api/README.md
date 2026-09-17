# API

DUR-006 adds the first versioned client API. The runtime serves these routes
when `DATABASE_URL` is configured:

- `POST /v1/workflows` submits a workflow. The server canonicalizes and hashes
  `payload`; `namespace` plus `submission_key` is the idempotency scope.
- `GET /v1/workflows/{workflow_id}` returns status and accepts optional
  `expected_revision` for a stale-read check.
- `GET /v1/workflows/{workflow_id}/history` returns ordered transition history.
  `after_revision` is a cursor and `limit` is 1–1000 (default 100).

If a response is lost after commit, retry the exact namespace, submission key,
and payload. The API returns the existing workflow with `created: false`. A
different payload for that key returns `409 PAYLOAD_CONFLICT`; an unknown
workflow returns `404 NOT_FOUND`; a stale revision or cursor returns `409
STALE_REVISION`. Other stale repository errors use `STALE_CLAIM` or
`STALE_ATTEMPT`.

The DUR-006 retention policy is conservative: workflow and transition-history
rows are not automatically pruned by this API, and history remains queryable
for the lifetime of the workflow. Retention/deletion is a later operational
policy and must preserve the documented ambiguous-client lookup behavior.
