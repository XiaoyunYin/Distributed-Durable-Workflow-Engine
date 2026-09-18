# Architecture and scope decisions

## D001 - DUR-001 development pins

- Date: 2026-09-14
- Status: accepted for local development

Pin Go 1.27.1, CPython 3.12.7, PostgreSQL 18.6, Apache Kafka 4.3.1,
OpenTelemetry Collector Contrib 0.160.0, and Prometheus 3.13.0 LTS.
Python development dependencies are resolved exactly in `uv.lock`; container
base and service images use explicit version tags plus pulled manifest digests
rather than floating or mutable tag-only references.

Go 1.27.1 and PostgreSQL/Kafka versions were selected after checking their
official release sources on 2026-09-14. Python 3.12.7 and uv 0.9.5 match the
already installed host tools and remain within an intentionally narrow Python
compatibility range. Prometheus uses the supported LTS line rather than the
fast-moving latest line. The collector uses the most recently confirmed
released version rather than a scheduled same-day version.

Reconsider these pins for security releases, unsupported versions, or before a
frozen benchmark protocol. A version change requires rerunning clean bootstrap
and restart-smoke evidence; it does not change a protected runtime guarantee.

## D002 - Single-node local dependencies

- Date: 2026-09-14
- Status: accepted for DUR-001 development only

Use one PostgreSQL container and one combined-mode KRaft Kafka container with
named volumes. This is the minimum topology that exercises the real database
and broker locally. It is not evidence of database replication, Kafka broker
failover, host-level availability, or final Linux I/O performance.

The alternative of mocks or embedded substitutes was rejected because the plan
requires real PostgreSQL and Kafka. A multi-node broker/database deployment is
unnecessary for the foundation task and would imply guarantees not yet tested.

## D003 - Versioned stable partition mapping

- Date: 2026-09-16
- Status: accepted for DUR-002

Use `sha256-u64-be-v1`: SHA-256 of the UTF-8 workflow ID, read as an unsigned
big-endian 64-bit integer from the first eight digest bytes, reduced modulo 16.
The map version and partition count are part of the durable workflow identity
contract. Go and Python keep independent implementations and tests so a
runtime-specific hash cannot silently change ownership after a restart.

## D004 - Named-boundary fault control and shared checks

- Date: 2026-09-16
- Status: accepted for M0

Fault fixtures report named boundaries over a process pipe and block until the
controller sends an explicit release or kills the target. The controller uses
event waits with bounded timeouts; it does not infer a boundary from sleeps.
Trace records use `fault-trace.v1` and carry the run ID, seed, and sequence
number. A trace path is exclusive to one controller run so evidence from
separate executions cannot be silently concatenated.

`scripts/ci.ps1` composes the existing checks and exposes race and real-service
checks as explicit switches. Paid/model checks remain outside the foundation
entry point. This is a local/shared validation contract; no remote CI service
is claimed when the repository has no configured remote.

## D005 - Retrieval, MCP, and adversarial-scope expansion

- Date: 2026-09-16
- Status: accepted by the user for the portfolio plan; paid/model execution still requires separate approval

The user confirmed authorship and authorization of the protected PLAN.md scope
changes that were present before the M0 implementation pass. This includes the
new research questions RQ7 (retrieval strategy) and RQ8 (defense profile and
injection), the MCP, PostgreSQL full-text-search, pgvector, and Grafana scope,
the separate retrieval benchmark with at least 40 development and at least 120
held-out queries, the corresponding Portfolio MVP and Full v1 release criteria,
and the DUR-029 live-model and adversarial studies. The revised execution
counts are 60 matched live retrieval-arm executions and 120 adversarial
executions (20 incidents x 2 defense profiles x 3 fixtures).

This decision records planned scope and release criteria; it does not claim
that any retrieval, model, MCP, or adversarial evidence already exists. It also
does not authorize paid-provider execution or set a spending cap. Before
DUR-029, the model configuration, cost estimate, and explicit paid-run cap
must be approved and recorded separately.

Alternatives considered were deferring RQ7/RQ8 to a later milestone, retaining
the earlier 40-execution live study, or limiting the portfolio plan to the
durable-engine incident path without retrieval/MCP and adversarial comparisons.
The broader comparison scope was retained because the user authorized it as
part of the intended portfolio evidence, while keeping execution and spending
approval separate from the plan change.

## D006 - DUR-002 contract correction after round-1 review

- Date: 2026-09-16
- Status: accepted for the M0 fix pass

Correct the pre-DUR-005 foundation contract to distinguish cooperating and
non-cooperating effect outcomes, make retry/backoff the only use of
`WAITING_TIMER`, add approval/version-paused/abandoned states, define the
attempt lifecycle, and expand all race traces with transaction boundaries and
durable-record locations. This is a clarification of the guarantees already
specified in PLAN.md, not a relaxation or expansion of them. The contract
version advances from `dur-002.v1` to `dur-002.v2` before schema work begins.

R010/R012 amendment: the corrected contract advances to `dur-002.v3` before
DUR-005. Worker result receipts and client/approver intent records do not apply
workflow transitions; the lease-owning scheduler does. All scheduler writes
use the lease-first lock order, and `WAITING_TIMER` means retry/backoff expiry
only, not a general-purpose explicit timer.

R013/R014 amendment: the contract advances to `dur-002.v4` before DUR-005.
Every activity definition must declare `PURE_ACTIVITY`,
`COOPERATING_EFFECT`, or `NON_COOPERATING_EFFECT`. An unclaimed timeout
redispatches the same attempt; a claimed pure activity may be replaced; a
claimed cooperating effect may be replaced only with the same effect key and
grant scope; a claimed non-cooperating effect becomes outcome-unknown and
requires `RECONCILIATION_REQUIRED` with no automatic replacement. The
transition-permission table names approval/cancellation intent records and
owner-applied transitions separately.
## D007 - M4 recovery and approval boundary

- Date: 2026-09-17
- Status: accepted as the implementation boundary pending Claude review

M4 keeps retry policy, checkpoint progress, approval intents, and engine-side
transition evidence in durable PostgreSQL tables. The cooperating sandbox
ledger is exposed through a separate service package and the independently
owned `effects` schema. Grant validation reads the engine grant before the
effect transaction, while the effect transaction writes only the effect
schema; this is intentionally local-development infrastructure rather than a
cross-database transaction. The effect service requires a grant bound to the
exact approved target resource, canonical state/argument hash, resource
revision, and logical effect key before a mutation; first use consumes the
grant as `DISPATCHED`. A non-cooperating endpoint has no runtime lookup or
deduplication capability and therefore remains reconciliation-only after an
uncertain call.

The rejected alternative was to let the engine infer approval from a caller's
actor string or to make cancellation and grant application separate commits.
That would make authorization and the cancellation/grant race dependent on
caller behavior. The selected design keeps the owner-side lease/workflow
transition atomic and records operator resolution separately, while leaving
authentication and production deployment explicitly outside this milestone.

## D008 - Bind effect grants to the complete approved action

- Date: 2026-09-17
- Status: accepted as the R049 fix pending Claude review

The approval target is the protected resource and the canonical effect state is
the approved action payload. The effect service recomputes the argument hash
from submitted state, requires it and the resource to match the approved
intent, and includes the resource in the grant scope. Applied receipts retain
the approving intent/resource so the independent checker can reconcile sink
mutations to authorization evidence. First successful use changes the grant to
`DISPATCHED`; retries may return the same receipt but cannot authorize a new
resource or payload.

The effect ledger remains a separate service-schema transaction. Marking the
engine grant dispatched is a separate post-commit update; if that acknowledgement
is lost, the stable effect receipt makes the retry safe.

## D009 - M6 deterministic local-first incident profile

- Date: 2026-09-18
- Status: accepted as the M6 implementation boundary pending Claude review

M6 correctness evidence uses a pinned deterministic local embedding adapter,
scripted fixture decisions, bounded schema-constrained MCP calls, and a
portable SQLite workflow store. The local workflow materializes and reads an
attached `source_corpus` database with the same documents/chunks/fixture-cases
boundary as the PostgreSQL migration; the migration owns the production
schema/full-text index, while the native pgvector column/index is created only
when the pinned development image exposes the extension. The JSON embedding
column remains the explicit fallback. Live provider use is not implied by
deterministic evidence: any future provider adapter must pass a positive
budget and an explicit `INCIDENT_LIVE_APPROVED=1` gate.

The local approval/effect adapter deliberately mirrors the M4 authorization
boundary: a grant binds the run, target resource, canonical proposal hash,
workflow revision, and fence token; first use records a receipt and marks the
grant dispatched, and retries return only that receipt. This is evidence for
the contract seam, not a claim that the Python fixture has replaced the
production PostgreSQL engine or Go effect service.

This keeps retrieval, citation, approval, redaction, and interruption
properties reproducible without paid calls or importing model behavior into
the engine correctness claim. The rejected alternative was to make M6's
acceptance depend on network credentials or a semantic model whose weights and
latency could change between runs.

## D010 - Keep M6 local while naming the production integration boundary

- Date: 2026-09-18
- Status: accepted as the R064 follow-up boundary pending Claude review

M6's deterministic acceptance evidence remains local and reproducible. Its
SQLite workflow/source adapter mirrors the M4 approval/effect semantics and is
tested for approval-before-dispatch, resource and proposal binding, revision
fencing, and idempotent receipts, but it does not replace the production Go
engine, PostgreSQL workflow state, or `effects.Service`. The production path is
named `DUR-033A`: before any external or live-model incident execution is
claimed, it must route an incident remediation through the versioned submission
API, scheduler-owned approval intent/grant, production effect service, and
seeded/queryable `source_corpus` boundary, then repeat the R049 attack matrix.

This keeps the M6 evidence honest without expanding the current milestone into
a second production integration project. The rejected alternative was to call
the local adapter the verified engine path or to leave the integration gap
unnamed.

## D011 - Declare the bounded M7 readiness host

- Date: 2026-09-18
- Status: proposed for DUR-036 review

DUR-036 uses Docker Desktop's `desktop-linux` Linux VM as the explicitly
declared measurement host: Docker Engine 29.7.2, Docker Desktop 4.86.0,
WSL2 kernel 6.6.87.2, amd64, overlayfs, and Docker-managed local volumes for
the PostgreSQL and Kafka services. The readiness artifact records the host,
container/runtime versions, resource limits, filesystem/volume probe,
PostgreSQL durability settings, UTC/timestamp assumptions, and the no-paid-
resource cost control. It also runs the real dependency smoke, the
service-backed telemetry smoke, crash recovery, and the independent F07 fault
checker from a clean checkout.

This is a bounded development readiness host, not replicated storage or a
bare-metal Linux performance claim. Final I/O-sensitive results remain labeled
Docker Desktop/WSL2 evidence unless reproduced on a separately declared host.
The rejected alternative was to silently treat Windows-hosted Docker output as
native Linux evidence or to begin final measurements before the readiness gate.
