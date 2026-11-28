# J12 public API authentication and namespace authorisation

Slice 0 registration is pending independent review. The campaign budget is $0.
Use one isolated public GitHub-hosted Actions job on `ubuntu-24.04`, with
disposable PostgreSQL and Kafka beside a single-node kind cluster for the
Kubernetes identity controls. Pin the tools, kind node, PostgreSQL and Kafka
from `deploy/k8s/tools.lock.json` at the candidate commit and use its `go.mod`
toolchain. Use fixture credentials and keep the job outside the K ledger. Record
source commit, workflow/run/attempt IDs, runner and service/image identities,
rendered and running listener inventory, effective switches and intended and
applied migration versions. Select `RUNTIME_PUBLIC_API_AUTH=1` on every J12 runtime
process and `RUNTIME_RELAY_FULL_BATCH_LOOP=1` explicitly. Keep unrelated opt-ins
off. Apply base 000001–000023 and only the J12 000026 addition. The relay switch
comes from J9a and is required before hosted qualification.

## Method, listener and credential matrix

This inventory is frozen from `internal/api/server.go` at J12's base. The table
describes J12-on `/v1` listeners. `GET` includes `HEAD` as provided by Go's
ServeMux. An unlisted method, route or listener pairing returns 404. On a listed
pairing, the wrong or missing credential returns 401 before body handling or
storage access. J12 requires split listeners in drain lifecycle mode. Roles without a
versioned API expose no `/v1` route. Health, readiness and metrics stay on their
existing operational surfaces. J4's two optional provider-control routes belong
to the separate J4 B2 profile and are absent in J12.

| Method | Route | Public `api` | Internal `operator` | Internal `worker-control` | Dedicated `demand` |
| --- | --- | --- | --- | --- | --- |
| POST | `/v1/workflows` | Tenant JWT | — | — | — |
| GET, HEAD | `/v1/workflows/{workflowID}` | Tenant JWT | — | — | — |
| GET, HEAD | `/v1/workflows/{workflowID}/history` | Tenant JWT | — | — | — |
| POST | `/v1/workflows/{workflowID}/cancel-request` | Tenant JWT | — | — | — |
| POST | `/v1/approvals/{intentID}/decision` | Tenant JWT | — | — | — |
| POST | `/v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/approval` | — | TokenReview | — | — |
| POST | `/v1/approvals/{intentID}/apply` | — | TokenReview | — | — |
| POST | `/v1/cancellations/{requestID}/apply` | — | TokenReview | — | — |
| POST | `/v1/worker-deliveries` | — | — | TokenReview | — |
| POST | `/v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/claim` | — | — | TokenReview | — |
| POST | `/v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/heartbeat` | — | — | TokenReview | — |
| POST | `/v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/result` | — | — | TokenReview | — |
| GET, HEAD | `/v1/demand` | — | — | — | Dedicated bearer |

The operator and worker listeners require `RUNTIME_TOKEN_AUTH=enabled` and the
existing HTTPS Kubernetes TokenReview trust, issuer, audience, service account
and pod identity checks. Configure the expected account per listener. Demand
requires `RUNTIME_TOKEN_AUTH=enabled` and a dedicated opaque bearer mounted at
`RUNTIME_DEMAND_BEARER_TOKEN_FILE`, using at least 32 base64url characters and
no JWT or TokenReview fallback. The
public listener uses its own verifier exclusively. Keep the operator, worker and
demand listeners private and serve public credentials over trusted TLS at ingress
or use isolated loopback for fixtures. With `RUNTIME_PUBLIC_API_AUTH` unset or
`0`, the existing route and credential configuration applies. An explicitly
empty value or any value other than `0` or `1` is a startup error.

## Tenant JWT and requester binding

- Configure exact issuer, audience and local public JWKS path through
  `RUNTIME_PUBLIC_JWT_ISSUER`, `RUNTIME_PUBLIC_JWT_AUDIENCE` and
  `RUNTIME_PUBLIC_JWT_JWKS_FILE` on the public listener. Register hosted fixture
  issuer `https://j12-fixture.invalid`, audience `durable-j12-api`, namespaces
  `tenant-a` and `tenant-b`, and distinct subjects `requester` and `approver`.
  Generate independent 2048-bit RSA fixture signing keys on the runner with
  Go `crypto/rand`, publish only the public JWKS to the runtime, and destroy the
  ephemeral private keys during cleanup. J8 generates its own fixture keys.
- Accept compact, unpadded JWS with `alg=RS256` only, with a `kid` selected from
  the local JWKS. Reject `none`, `HS*`, other algorithms, embedded keys,
  token-directed key references and unsupported JOSE fields. A JWKS contains
  unique `kid` values, RSA signing keys with `alg=RS256`, 2048–4096-bit modulus
  and exponent 65537. If present, `use` must be `sig` and `key_ops` must be
  `["verify"]`. The verifier bounds tokens at 16 KiB and JWKS files at 64 KiB.
- Require exact `iss`, matching `aud` as a string or array, `sub`, `namespace`
  and positive integral Unix-second `exp`. The subject is 1–255 visible ASCII
  characters without spaces. `namespace` matches
  `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Optional `iat` and `nbf` are positive
  integral Unix seconds, strictly earlier than `exp` and at most 30 seconds
  in the future. Reject at or after `exp + 30s`. Skew is measured against the
  API process wall clock. Reject duplicate JSON members and malformed time
  claims, including explicit nulls. The issuer determines token lifetime.
- Validate the local JWKS before startup and reread it for each public credential
  check. Publish a complete file by atomic replacement and overlap old and new
  `kid` values during rotation. A removed or unknown `kid` fails immediately.
  Missing, unreadable, invalid or empty JWKS prevents startup or denies subsequent
  requests. Credentials never select their own issuer, keys or auth path.
- Submission derives namespace, actor and requester from the token. A nonempty
  body namespace differing from the claim returns 403. The legacy body actor
  field carries no authority. Create the requester row in the workflow
  transaction, keyed by workflow ID, with the trusted issuer and subject.
  Bind authenticated submission replay to that issuer and subject. An intent
  inherits the requester via its stored workflow ID. Legacy workflows have no
  requester binding, including after a conflicting authenticated submission.
- Resolve status, history and cancel-request through the path workflow ID and
  its stored namespace. Resolve decisions from the path intent ID through the
  intent's stored workflow ID and namespace. Only the internal listener applies
  a cancellation, using the workflow ID stored under the path request ID. The
  internal approval apply path resolves the stored intent from the path and
  retains its durable workflow/node/revision and lease binding. Conflicting
  body identifiers are denied. Authenticated lookups of absent and foreign
  resources return indistinguishable 404 responses for the same route. The JWT subject is
  the approver. Deny both decision values with 403 when the requester is absent,
  was issued under a different trust domain, or equals the approver. Approval
  decisions on this listener never disclose dispatch grant credentials.
  Public submission preserves the existing globally unique workflow ID contract,
  including a conflict for a reused workflow ID.

## Hosted controls and verdicts

Execute the registered matrix once with fresh fixtures. Exercise each listed
method on every listener with its matching credential, a valid tenant JWT, a
valid projected Kubernetes credential, the dedicated demand bearer, and no
credential. Use valid state and requests for allowed cells, with independent
fixtures where a prior action changes state. Record status, stable error code,
bound durable IDs, per-control SQL observations and verdict. Attempt at least
one unlisted method per route, and verify startup rejects combined mode and
split internal roles without their assigned authenticator.

| Control | Required observation |
| --- | --- |
| Forged body identity and namespace | Submit records the token subject and namespace despite a forged `actor_id`. A mismatched body namespace is denied with no submission. A forged `approver_id` cannot change the decision identity. Forged path alternatives in body fields fail decoding, including `workflow_id` on cancellation and `intent_id` on decision. Conflicting body IDs on the internal approval apply path return 409. |
| Cross-namespace workflow IDs | Foreign status and history (including HEAD) and cancel-request match the absent-ID denial. No foreign cancellation or revision change. Same-namespace status, history and cancel-request succeed. |
| Foreign intent ID | Foreign decision matches the absent-intent denial, and leaves the foreign intent pending with no grant or effect. |
| Wrong namespace | A valid token for `tenant-b` cannot submit to `tenant-a` or operate on its stored workflows or intents. |
| Token rejection | Missing token, expiry beyond skew, bad signature under a registered `kid`, wrong issuer or audience, missing subject or namespace, unknown `kid`, `none` and `HS*` all return 401. Run verifier tests on the hosted job with a fixed clock for the exact skew edges. |
| Key availability and rotation | Missing JWKS refuses startup and denies a previously valid token after removal. Invalid replacement denies authentication. Both keys work during overlap and the retired key is rejected after removal. |
| Self-approval and legacy requester | A valid requester token with a correct proposal hash and a forged distinct `approver_id` cannot decide. A valid distinct token also cannot decide a legacy intent. Decisions remain pending with no grant or effect. |
| Distinct in-namespace approval | A different valid subject with the exact proposal hash records its JWT subject as approver. Only the internal apply path can grant. A repeated public decision after grant does not expose the grant credential. |
| Tenant on internal routes | A valid tenant JWT is denied on approval creation, both applies, worker delivery, claim, heartbeat, result and demand. Verify no repository side effects and no writable fallback. |
| Internal and demand positives | Actual pod-bound tokens are accepted via TokenReview on the intended operator and worker paths. A separate mounted demand credential yields a fresh read-only sample. Present each internal credential to the public listener and swap credentials among internal credential paths to verify isolation. |
| Legacy equivalence | In ordinary unit CI outside PostgreSQL K6 fixtures, compare unset and explicit `0` against the pre-J12 listener and credential contract. Test invalid values as startup errors. |

Pass requires every control and each matrix cell to match, along with the durable
requester, cancellation, decision and no-grant/no-effect observations and cleanup.
Any witnessed contract mismatch under a qualified setup is a fail. Missing setup,
inventory or witnesses, fixture or runner failure, or loss of cleanup evidence
makes an affected control invalid. Record unstarted controls as unperformed and
retain all outcomes. Use a 30-minute job limit and stop on setup or trust-boundary
failure. Reserve the final five minutes for evidence and cleanup. Retain up to
16 MiB of scrubbed evidence per job and mark oversize evidence invalid. Failed
and invalid runs have no automatic replacement. Publish run-bound summaries and scrubbed
evidence under `experiments/portfolio/public-api-auth/<run-id>/` following
review. Omit token strings, private keys, database credentials and grant tokens.
Publish a success claim only for a complete reviewed pass. Limit the claim to the
registered single-host fixture, identities and routes.

The PostgreSQL-backed handler fixture is `TestJ12PostgresTenantAuthorisation` in
`internal/api`. Set `J12_TEST_DATABASE_URL` to an isolated, empty database with
the base migration inventory. The test applies and rolls back 000026 and
uses a TLS TokenReview fixture. The hosted campaign uses actual pod-bound tokens
for the Kubernetes trust verdict.
