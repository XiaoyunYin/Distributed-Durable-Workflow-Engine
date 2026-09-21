# Architecture and scope decisions

## D022 - Authorize the bounded cloud portfolio campaign

- Date: 2026-09-20
- Status: user-authorized; maximum cloud-compute and retained-resource cap is
  **$200 USD**; no model-call budget or release authorization.

The user authorized a finite cloud campaign for the new portfolio tasks covering
AWS multi-host recovery, the cloud capacity comparison, overload/soak work,
operational recovery drills, and the required Kubernetes deployment. The cap
covers all resources and charges for steps 4–8: compute, Kubernetes control
planes, networking and traffic, storage, snapshots, registries, observability,
recovery-drill environments, and retained resources during the campaign. It is
separate from D013's $30 OpenAI evaluation cap and does not authorize paid model
calls, production access, or use of Project 1's EKS clusters.

Before provisioning, the task must record a Terraform plan, resource manifest,
expected cost, region, image digests, bounded replica/node limits, and teardown
commands. Use the existing `portfolio-dev` AWS identity, never a root profile.
Tag every resource with the task and expiration. The campaign has a maximum
14-day paid window; destroy active resources within 24 hours after each run and
remove retained snapshots, volumes, logs, and registry artifacts within seven
days unless a new decision names them. Configure spend alerts and stop creating
resources when projected spend reaches $160; the absolute authorization is $200
and no work may continue when the remaining cap cannot be established.

The stop conditions are: an unbounded autoscaling or networking charge, a
resource that cannot be attributed to this project, a failed teardown, a
projected breach of the cap, or evidence that the experiment requires a larger
topology or longer window. In those cases, stop the campaign, preserve the
minimal evidence needed to explain the stop, and do not infer a reliability or
performance result from an incomplete run. Each campaign must report actual
cost, retained resources, cleanup status, and the exact deployment topology.

## D021 - Bring Kubernetes into the portfolio scope

- Date: 2026-09-20
- Status: user-authorized scope change; supersedes the conditional Kubernetes
  language in PLAN:414 for this portfolio completion program.

The user authorized Kubernetes as a required deliverable rather than an
optional extension. The implementation must include a reproducible dedicated
cluster path, application manifests, probes, resource limits, graceful
termination, Kustomize overlays, KEDA-backed worker scaling, and a stabilized
Helm chart. PostgreSQL and Kafka remain outside the cluster for the first
deployment so their durability claims are not conflated with Kubernetes
orchestration claims.

The deployment must not use either of Project 1's us-west-1 EKS clusters or
their budget. The task must choose EKS or a self-managed cluster explicitly,
record versions, topology, add-ons, dependency connectivity, teardown, and
cost under D022, and validate pod loss, process isolation, graceful drain,
rolling update/rollback, and scaling with measured progress plus the existing
invariant checker. Kafka lag is not accepted as a workflow-backlog proxy
without validating it against durable runnable work and active claims.

This decision changes scope only for the portfolio tasks; it does not alter the
engine guarantees, prior experiment matrices, release criteria, or production
readiness claims. Kubernetes evidence remains bounded to the tested cluster,
workload, and failure model.

## D020 - Evidence-led portfolio completion under standing authorization

- Date: 2026-09-21
- Authority: the user's session instruction authorizes improvements needed to
  complete the backend/distributed-systems portfolio, implementation, validation,
  and subsequent pushes. ATS wording is deferred to a later user task.

Select work by an observable gap and the evidence it can produce. Begin with
automated mutation checks for critical safety guards, parser fuzzing, portable
reproduction, and dependency/security CI. Then profile the deployed path and
measure one justified optimization; prepare a private three-host AWS recovery
campaign; exercise backup/restore and telemetry-led operations; and add scoped
tokens and roles before untrusted write access. Keep applied AI bounded to the
existing incident application and regression fixtures.

This authorizes new tasks and protocols to be recorded before implementation.
It does not reclassify prior campaigns, extend their claims, or anticipate an
independent review. Detailed planning and teaching records remain local under
the user's docs-directory publication policy. Reproduction scripts, tests,
workflow configuration, and newly measured evidence remain public.

Use the existing portfolio-dev AWS profile, never the root default profile.
Prepare cost estimates, a finite campaign cap and automatic teardown controls
before provisioning; no cloud resources are provisioned by this initial quality
pass. Kubernetes/Helm, Redis, gRPC, automatic database failover, dynamic
repartitioning, and additional model providers remain conditional on a concrete
requirement. Tool count is not an acceptance criterion.

## D019 - Reassign DUR-031 walkthroughs to Codex engineering evidence

- Date: 2026-09-20
- Status: user-authorized; supersedes the personal-performance portion of
  DUR-031 without fabricating user authorship.

The user explicitly reassigned DUR-031's three walkthrough exercises—the
lease/attempt fence mutation, the ambiguous non-cooperating effect walkthrough,
and the DUR-028 offline reproduction—from personal performance to Codex
performance. DUR-031 and M8 move to DONE for their engineering-evidence scope.
The interview pack is engineering evidence produced by Codex; it does not
establish the user's personal fluency, and no document may imply that it does.

R092 closes as WITHDRAWN by this scope change, not as satisfied. PLAN's rule
against fabricating personal authorship or understanding remains binding. The
pack and release records must retain the attribution boundary and explain that
the three exercises are reproducible engineering evidence rather than a user
walkthrough claim.

## D018 - Authorize the packet-level DUR-041a isolation arm

- Date: 2026-09-20
- Status: user-authorized; no cloud budget or release authorization.

The user authorized a second, separately labelled DUR-041a local fault arm:
remove the original scheduler container from the Docker network shared with
PostgreSQL while preserving its running container and process, let the peer
take over, reconnect the original, and verify that the retained epoch cannot
commit. This adds a packet-level/container-network fault to the existing
pause-and-reconnect arm without overwriting its artifact.

The arm must confirm the network membership change and process/container
continuity from Docker state, record takeover and stale-write observations,
and retain the lock-held takeover case as separately labelled. It is local
Docker Desktop/WSL2 evidence, not multi-host or database-host durability.

## D017 - Authorize and supersede the R096 repair

- Date: 2026-09-20
- Status: user-authorized; supersedes D016 for the R096 repair and its
  lock-held takeover campaign exclusion.

The user authorized Codex to repair R096: a scheduler transaction that stalls
while holding the PostgreSQL lease-row lock must not block takeover without a
bounded retryable outcome. The repair must preserve the in-transaction lease
row lock and epoch fence; it may add PostgreSQL lock/session bounds and a
scheduler iteration deadline, but it must not weaken ownership checks.

Once the repair is accepted by Claude, the DUR-041a campaign may lift its
lock-held-takeover exclusion and run that scenario as a separately labelled
arm. Existing local-410041 evidence remains unchanged and must not be
overwritten. Until Claude accepts the repair, no R096 status is changed to
VERIFIED and no campaign result may claim the bound.

The user did not authorize hosted CI execution or a GitHub push here. A remote
workflow run still requires the user's GitHub account, remote, and push.

## D016 - Defer R096 while advancing the portfolio minimum

- Date: 2026-09-20
- Status: superseded for R096 repair authorization by D017; retained as the
  historical deferral decision for the earlier portfolio package.

The user explicitly chose to defer the R096 lease-row lock liveness repair and
proceed with the other portfolio work. This authorizes implementation of the
minimum package proposed in `docs/PORTFOLIO_ROADMAP.md`: DUR-037 reproducible
Linux CI, a bounded local recovery campaign under DUR-041a, and DUR-045
recruiter-facing presentation. It does not authorize claiming that takeover is
bounded while a scheduler transaction still holds the lease row lock.

Until R096 is repaired, DUR-041a must exclude or separately label the
lock-held-takeover case. It may still measure the other declared local fault
cases when their safety and progress evidence is valid. Any result must carry
the R096 limitation and must not be presented as general scheduler liveness.

Cloud infrastructure, multi-host DUR-041b, and any paid provider/model use are
not activated by this decision. A separate finite cloud budget and topology
decision is required before provisioning. Authentication remains optional for
private trusted experiments and required before untrusted write exposure.

The rejected alternative was to silently ignore R096 while publishing the full
recovery claim. That would turn a known blocking liveness limitation into an
unsupported resume or recruiter claim. The user may later authorize the repair
or a separate explicit deferral; neither is inferred here.

## D015 - Separate published technical evidence from local process records

- Date: 2026-09-20
- Status: implementation choice under the user's request to exclude unnecessary
  files before GitHub publication; no guarantee or release criterion changed.

Use README.md as the recruiter-facing entry point. Keep code, tests, migrations,
locked dependencies, API documentation, contracts, the runbook, architecture
decisions, the technical report, and reproducible experiment evidence tracked.
Keep agent instructions, the work plan, review transcript, build diary, interview
exercise pack, portfolio proposal, and release checklist on disk for continued
local work, but remove them from the current Git index and ignore future edits.
They are process records, not build or runtime dependencies.

This is a current-tree presentation change, not a history rewrite or privacy
guarantee: prior commits and the existing release tag still contain these files.
No local record is deleted, no review finding is closed, and no release, remote
push, or cloud deployment is authorized by this cleanup. Future local process
records need a separate backup if they must be retained beyond this workspace.
Personal walkthrough attribution and the pending R096 repair remain unchanged.

The rejected alternative was ignoring every Markdown file, which would remove
the evidence and operating instructions behind the README's claims. Public
documentation links must resolve to tracked files, and generated caches/results
must not displace the committed fixtures or snapshots needed for reproduction.

## D014 - Wire the local deployment through existing durable boundaries

- Date: 2026-09-19
- Status: implementation decision; user authorized fixing the self-check gaps.

Both runtime replicas run lease-fenced external-activity interpreters and
Kafka event ingestion. A bounded namespace-scoped PostgreSQL repair scan
remains authoritative when delivery is lost. Python slots consume Kafka,
record inbox/poison disposition through the local API, commit offsets only
after that disposition, and claim the exact delivered attempt before work.
Uncertain HTTP results retry identical bodies. New groups start at earliest
retained offsets; existing groups resume from committed offsets. The previous
LastOffset study default and test-only constructor are not deployment defaults.

Automatic work uses the local-runtime namespace and allowlists pure.echo/v1
and pure.add/v1. Unsupported definitions pause before dispatch. Approval/effect
integration remains the separately reviewed DUR-033A path; no paid calls or
new external actions are enabled. The API remains unauthenticated and
localhost-published. The rejected alternative was claiming deployed operation
from a readiness fixture or trusting broker-supplied execution input/version.

Client reference: https://kafka-python.readthedocs.io/en/2.2.16/apidoc/KafkaConsumer.html

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
- Status: accepted; Claude's committed round-29 review returned
  `NO_BLOCKING_FINDINGS` for target `6325d1f`. R069 remains a nonblocking
  fixture-cleanup follow-up before the first measurement study.

DUR-036 uses Docker Desktop's `desktop-linux` Linux VM as the explicitly
declared measurement host: Docker Engine 29.7.2, Docker Desktop 4.86.0,
WSL2 kernel 6.6.87.2, amd64, overlayfs, and Docker-managed local volumes for
the PostgreSQL and Kafka services. The readiness artifact records the host,
container/runtime versions, resource limits, filesystem/volume probe,
PostgreSQL durability settings, UTC/timestamp assumptions, and the no-paid-
resource cost control. It runs the real dependency smoke and a bounded
readiness workload through the deployed runtime's explicit readiness profile;
the runtime constructs the interpreter and exports the resulting engine
metrics. The artifact distinguishes a clean version-controlled worktree from
a fresh clone and records when Compose may have reused services or volumes.
It also records crash recovery and the independent F07 fault checker.

This is a bounded development readiness host, not replicated storage or a
bare-metal Linux performance claim. Final I/O-sensitive results remain labeled
Docker Desktop/WSL2 evidence unless reproduced on a separately declared host.
The rejected alternative was to silently treat Windows-hosted Docker output as
native Linux evidence or to begin final measurements before the readiness gate.

## D012 - Keep DUR-034 safeguard ablations test-only and independently paired

- Date: 2026-09-18
- Status: proposed for Claude review with DUR-034

DUR-034 uses an explicit context-scoped `TestSafeguardProfile` seam rather than
global flags or runtime configuration, and the complete seam is compiled only
with the `dur034_ablation` build tag. The default state package has no profile
constructor or weakened implementation. The full profile calls the normal
store protocol. The history-disabled profile suppresses only transition-history
writes, the unsafe profile deliberately moves owner validation outside the
mutation transaction and exposes a check-to-commit barrier, and the no-outbox
profile suppresses outbox writes while the harness records a bounded delayed
recovery before dispatch. None of these profiles is constructed by a runtime
binary.

Each weakened profile is paired with an independent observable property: audit
rows disappear when history is disabled, a stale owner can commit after a
takeover only under the unsafe negative control, and the no-outbox arm reports
recovery delay without treating direct dispatch as a safety replacement for
the transactional outbox. The rejected alternative was to change production
defaults or report a synthetic profile comparison without a negative control.

## D013 - Authorize the bounded DUR-029 OpenAI evaluation

- Date: 2026-09-19
- Status: accepted by the user for this run

The user authorized the DUR-029 live provider as OpenAI with the exact model
`gpt-4o-mini`, with an aggregate spending cap of $30.00 across the 60 matched
retrieval-arm executions, the 120-execution adversarial matrix, and the
separate redaction-off negative control. The implementation uses the
Responses API with structured JSON output, `store:false`, a shared local
reservation ledger, and the explicit `INCIDENT_LIVE_APPROVED=1` gate. The
model, prompt version, MCP schema, retrieval arms, defense profiles, and
canonical proposal signature remain frozen by DUR-029; flexible ordering means
the study may use the provider's actual response order without changing the
case/arm matrix.

This authorizes only the bounded synthetic evaluation and its measured API
cost. It does not authorize external incident actions, production engine
integration, broader data, or a higher cap. The rejected alternative was to
claim live-model evidence from the deterministic fixture provider or to run a
provider without an enforceable aggregate cap.
