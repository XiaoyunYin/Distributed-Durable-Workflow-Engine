# K2b demand and registry prerequisites

Date: 2026-11-04. Base: `c36a2dd0d794d17b918c18dab19c7d6cdcfb5314`.
Working tree on `codex/k8s-k2`, uncommitted; Claude commits. PROVISIONAL,
DUR-053 K2 IN_PROGRESS. R224 and R226 are proposed ADDRESSED, not VERIFIED.

## Demand

`RUNTIME_ROLES=demand` serves only `GET /v1/demand` as a business route on
port 8091, with a configured engine namespace. The in-cluster demand role must
set `RUNTIME_TOKEN_AUTH=enabled`; there is no auth-off in-cluster default. A
campaign-scoped static bearer Secret is mounted at
`RUNTIME_DEMAND_BEARER_TOKEN_FILE`; a missing/unreadable/empty
secret fails startup, and requests reread the mounted file for rotation. Missing,
wrong and empty bearer values receive 401 before querying. Other listeners keep
K2a pod-bound TokenReview. The process has a
read-only two-connection pool; pool acquisition and query share a 2 s bound.
Database failure is HTTP 503, with no scalar in the error body. Samples are
not cached. A missing timestamp or response taking over 5 s is an error.

The numeric `demand` field equals `active_claims + claimable_attempts`.
A current attempt in a WAITING_ACTIVITY workflow and with a matching current
node pointer counts once: CLAIMED only before heartbeat expiry, or
DISPATCHABLE regardless of task publication/dispatch-lease expiry. Namespace
and current-node predicates use EXISTS. Pending/published redispatch records
cannot multiply attempts. PostgreSQL computes `sampled_at` and namespace
maximum `oldest_claimable_age_seconds` in the same statement. Empty fresh
samples are zero; expired-but-unreplaced claims and timers are diagnostic
recovery obligations. No Kafka lag is read.

The old `durable_reconciliation_oldest_age_seconds` metric and setter were
removed. Tests place attempts 120 s and 20 s old in different partitions,
call partition diagnostics in both orders and require the global maximum;
they also require the misleading metric to remain absent. Other cases cover
namespace separation, many redispatch rows, an active claim after deleting
all its task notifications, expired claims, dispatch expiry, terminal work,
stale node pointers, non-current attempts, cancellation/query failure,
connection exhaustion and server read-only/timeout settings. Migration 19
adds only supporting indexes, with down/up and ledger/index verification.

KEDA's reserved mapping is `metrics-api`, `metricType: AverageValue`,
`format: json`, `valueLocation: demand`, `targetValue: "2"`, bearer auth,
minimum 1 and maximum 4 workers. Registered fallback is 2 replicas after
3 failed polls, with 5 s polling and protocol-v2 stabilization/rate limits.
This is a response contract, not measured scaler behavior. The
`metrics-api` trigger uses `authMode: bearer` and a namespaced
`TriggerAuthentication.secretTargetRef` mapping parameter `token` to the
campaign Secret's `token` key. Generate a high-entropy secret per campaign,
rotate the Kubernetes Secret and projected file, and verify the polls before
retiring the old value. K3 must freeze/test the actual KEDA version and add a
NetworkPolicy restricting ingress on 8091 to the KEDA operator. Official references: [Metrics API](https://keda.sh/docs/2.18/scalers/metrics-api/),
[ScaledObject](https://keda.sh/docs/2.18/reference/scaledobject-spec/).

## Registry compatibility

The Go runtime and Python worker expose `activity-registry` commands derived
from their effective production registries. CI checks default and DUR048
profiles via `uv run python scripts/check-activity-registry.py`. File options
compare manifests exported from exact candidate images. Name/version changes,
missing or extra entries and invalid/duplicate manifests fail. The deferred
DUR050 extension remains a rejected mismatch; no new production activity is
introduced in K2b.

The candidate runtime's `compatibility-check --namespace <ns>
--worker-registry <candidate-worker-json>` requires DATABASE_URL, compares
the complete effective candidate sets, then checks all version-map entries in
immutable definitions referenced by non-terminal namespace work. It includes
future nodes and paused/approval/timer/reconciliation work; terminal references
and other namespaces are excluded. Missing versions, malformed maps, outages
and query deadlines fail nonzero. The Job has two read-only connections.
K3 must make it a required pre-rollout/rollback Job and freeze submissions
through check/rollout; new submissions cannot use versions absent from any
surviving old image. This is an explicit deployment precondition, not an atomic
admission lock or version-aware queue.

The native mixed-version test runs two existing v1 two-activity workflows:
an old registry worker followed by a v1-retaining candidate with a test-only
v2, then the reverse order. It uses the real Python ActivityRunner and HTTP
claim/result client, Go external-activity scheduler and PostgreSQL. Each must
reach SUCCEEDED with two persisted successful results. The old registry is
the exact pre-K2b default set, not an archived container image. Negative controls
remove a version referenced by non-terminal work and require gate rejection;
the command is also tested against mismatched candidate manifests and an
unreachable DB. This is not a Kafka/Compose/cluster rollout measurement.

## Validation and scope

Exact executed commands, outcomes, skips and timings: validation-k2b.json.
Full logs, mutation results and source hashes are separately suffixed `k2b`.
No Docker, PowerShell 7, kind, real TokenReview API server, deployed KEDA,
Compose workload proof or hosted CI was run in this pass. Two optional DUR055
Python integrations and the real Kafka witness depend on separate configured
services. K2c remains unstarted. Cleanup is recorded in cleanup-k2b.json.
