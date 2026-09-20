# Backend and distributed-systems portfolio execution guide

Date: 2026-09-20. Status: PROPOSAL for future implementation, not active scope.
Repository baseline inspected: `ec2501f`; reviewer rounds 51/52 are currently
uncommitted in REVIEW.md. This guide records proposed work, not completed work.
Committing the guide preserves a proposal; it does not activate PLAN tasks,
authorize spending, change release criteria, or satisfy their review gates.

## Objective and boundaries

Make the existing engine easy to evaluate as evidence of backend engineering:
API and data design, concurrency, failure recovery, cloud deployment,
performance investigation, operating practices, and clear technical judgment.
Keep applied AI as one bounded application of the runtime.

The project should answer a concrete question in a demonstration: can an
operator submit work, lose a scheduler or worker, recover committed progress,
and account for the outcome of an approved sandbox action?

**R096 is a separate repair.** This guide does not include its implementation,
timeout selection, or regression work. Accepted R096 evidence is a prerequisite
for the isolation/recovery campaigns and their reliability claims. Other work
below can proceed independently. Record the accepted fix commit when available.

**R092 remains a separate personal task.** The user intends to do it later.
This guide preserves that attribution and schedules personal practice before
interviews; agent-performed work does not close it.

Cost and deployment are acceptable to the user when they materially support
the hiring objective. Use that direction to make concrete deployment choices,
record finite phase budgets and teardown rules, and avoid unnecessary services.
This document provisions nothing and changes no existing spending record.

Do not treat completion of this roadmap as a guarantee of interviews. Start
using the accepted existing evidence while the highest-value additions mature.

## Minimum shippable subset and effort estimates

The shortest useful package is:

1. Record only the selected scope under Step 1.
2. Consume the separately accepted R096 repair; do not implement it here.
3. **DUR-041a: local isolation campaign** using the existing deployed engine.
4. **DUR-037: reproducible Linux CI**, including the real dependency-backed smoke.
5. **DUR-045: presentation** of the local failure investigation and already
   accepted results, then start applications and seek reader feedback.

This package does not require cloud VMs, application authentication, a richer
incident workflow, another performance study, or more paid model calls. Start
the README outline now; CI can proceed while the campaign awaits review.
Do not delay publishing accepted evidence until the optional work is finished.

Budget **about 5-9 focused engineering days** for this subset: 0.5 day for the
selected scope record, 2-4 for the local campaign, 1-2 for CI, and 1-2 for the
presentation. These are planning ranges, not delivery promises. A focused day
means roughly six productive hours; estimates include implementation,
documentation, and applicable validation, but exclude R096, review turnaround,
account/access delays, and unexpected defects. Re-estimate after the first
campaign pilot. They assume the existing accepted harnesses can be reused.

No new paid infrastructure or model use is needed for the minimum subset.
Use existing local resources and confirm the chosen hosted CI allowance before
enabling paid features. The optional cloud envelope below is not an entry fee.

## Delivery map

The task IDs below are proposed and were unused in the inspected PLAN. Confirm
them when recording task entries. Existing task statuses and release criteria
remain authoritative until those entries and scope decisions are recorded.

| Proposed task | Deliverable | Priority | Focused days | Prerequisites |
|---|---|---|---|---|
| DUR-041a | Local isolation and recovery campaign | Minimum subset | 2-4 | R096 accepted; existing local runtime/workers; no cloud, CI, auth, or expanded application dependency |
| DUR-037 | Reproducible Linux CI and repository entry point | Minimum subset | 1-2 | Existing accepted code; repository publication/hosted CI access |
| DUR-045 | Evidence, demo, report, and resume package | Minimum subset | 1-2 initially | Accepted evidence; local campaign and hosted CI for the final minimum package |
| DUR-040 | Reproducible private cloud topology | Optional | 2-3 | DUR-041a accepted, DUR-037, recorded cloud budget; no DUR-038 dependency |
| DUR-041b | Multi-host recovery campaign | Optional | 1-3 | DUR-041a, DUR-040, remote pilot; no expanded application dependency for core cases |
| DUR-039 | Useful deployed incident workflow and operator CLI | Optional | 3-5 | Existing deployed path; private trusted sandbox, or DUR-038 before untrusted access |
| DUR-042 | Deployed load study and measured optimization | Optional | 3-6 | Stable isolated deployed path, local or cloud; fixed resources and instrumentation |
| DUR-043 | Observable operation and release recovery | Optional | 2-4 | Stable deployed path and inspection tools; DUR-039/040 only if that application/topology is selected |
| DUR-038 | Scoped API tokens and roles for control/workers | Optional; required before untrusted write access | 1-3 | Explicit permission matrix and selected access model; not a prerequisite for private campaigns |
| DUR-044 | Small deployed applied-AI evaluation | Optional | 1-2 | DUR-039, frozen evaluation scope and any required model budget; DUR-038 only for untrusted access |

These are incremental estimates, not a commitment to implement every row.
Additional OIDC integration, if justified, needs its own estimate and scope;
it is not included in DUR-038's token-based estimate. Later DUR-045 updates can
be small follow-ups rather than restarting the presentation task.

The numbered sections below are reference instructions, not a mandatory
execution order. Use the minimum sequence above. After it ships, choose the
next optional task from target-role requirements or actual reader/screening
feedback. Run faults and benchmarks in dedicated windows so they do not
contaminate each other's data. Core lease campaigns use existing pure activities;
effect-specific scenarios can follow DUR-039 without blocking that evidence.

## Step 1 - Record the extension and make the work reviewable

1. Before implementation, record the selected extension scope in
   docs/DECISIONS.md. Start with the minimum subset, including the new local
   experiment family, not the entire optional backlog. Committing this
   proposal alone needs no activation decision. A zero-spend local phase does
   not authorize a later paid phase; cloud/model budgets are recorded before use.
2. Add each selected task to PLAN with status, dependencies, goal, scope,
   acceptance, exact validation commands, evidence paths, and known limits
   before coding. Keep DUR-041a and DUR-041b separate; leave the cloud phase
   unstarted until the local campaign reports and its own scope is selected.
3. Preserve M5/M7 protocols and artifacts. New deployment measurements get new
   directories, schema versions, and source commits. They do not replace old
   numbers or inherit their validation automatically.
4. Address R095's record alignment using Claude's round-51 verification; retain
   original reviewer text and keep R092 visible as the pending personal item.
5. If selecting a new release, name its candidate and baseline without moving
   `v0.1.0`; record any release-criteria change through DECISIONS. Publishing
   a proposal or accepted case study is not authorization to create a release.

Delivery convention for every task: small implementation commits; relevant
tests; actual commands and outcomes in BUILD_LOG; complete handoff in REVIEW;
READY_FOR_REVIEW only after the implementation is committed; DONE only after
the required independent review. Do not create process-only milestones when
one focused implementation/review can deliver the result.

**Done when:** the selected work has explicit scope, task records, and separate
provenance from the historical release. The minimum subset records no new paid
use; optional paid phases get finite budgets before provisioning or model calls.

## Step 2 - Make a fresh machine and remote CI reproduce the engine

1. Provide a Linux quick start using the existing scripts with PowerShell 7
   or a thin, documented container entry point. Avoid rewriting the tooling
   solely for aesthetics. Verify every command on fresh source and volumes.
2. Add GitHub Actions jobs for Go formatting/vet/build/race checks, Python
   Ruff/mypy/tests, migrations, offline trace validation, and a real
   PostgreSQL/Kafka deployed smoke test.
3. Require dependencies in integration mode: unavailable PostgreSQL/Kafka
   must fail the job rather than skip it. Keep live model calls out of CI.
4. Isolate fixture data. Give each concurrent package/run its own database
   and Kafka topic/group namespace, or serialize shared packages explicitly.
   Cleanup must be scoped to the run's IDs. Do not stop another job's services.
5. Build runtime/worker images tagged by commit and retain test logs on failure.
   Run longer fault checks on demand or on a bounded schedule; keep ordinary
   pull-request checks practical.
6. Prepare the repository for publication: verify license choice, remove
   accidental credentials from publishable content, document supported
   platforms, and supply sample configuration. Publish with an authentic CI
   badge once the actual hosted job has run successfully.

Existing validation entry points include:

```powershell
pwsh ./scripts/bootstrap.ps1 -StartServices
pwsh ./scripts/ci.ps1 -WithServices -WithRace
pwsh ./scripts/m5-archive-check.ps1
pwsh ./scripts/local-demo.ps1
```

Use these only in the task's isolated environment. The current CI script stops
that environment's runtime/worker services while testing shared fixtures.

**Done when:** a clean Linux runner builds and tests the real dependencies,
executes a deployed workflow, and publishes inspectable passing evidence.

## Step 3 - Optional: add scoped API tokens and a small authorization model

Do this after the recovery campaign unless untrusted access is actually needed
sooner. Private campaigns may retain the documented development-only API inside
an isolated trusted network, using restricted host access. Network isolation
does not authenticate application actors; caller-supplied identities remain a
known limitation. Do not expose write endpoints to the public or untrusted peers
until this step is accepted. No public live service is needed for the portfolio.

1. Write a permission matrix for viewer, submitter, approver, worker, and
   scheduler identities. Scope access to explicit namespaces/resources.
2. Start with scoped, high-entropy API tokens mapped server-side to a principal
   and role, with expiry, rotation, revocation, and secure secret handling.
   Use maintained security libraries; do not build a custom token protocol.
   OIDC-backed operator access is a later option only if the deployment needs it.
3. Derive durable actor/worker identity from verified credentials. Ignore or
   reject conflicting caller-supplied identity fields. Do not let a human
   client gain scheduler authority by supplying a lease reference.
4. Enforce authorization on status/history, submissions, cancellations,
   approvals, deliveries, claims, heartbeats, results, and effect operations.
   Keep grant creation and workflow mutations subject to scheduler ownership.
5. Add TLS at the ingress and authenticate internal control calls. PostgreSQL
   and Kafka remain reachable only by the application/deployment network.
6. Add request size limits, finite HTTP timeouts, and bounded request rates.
   Document which responses are definitive versus uncertain, retaining the
   existing same-key retry policy for ambiguous submissions.

Test valid access plus missing/expired credentials, wrong role/namespace,
forged actor identity, worker-token misuse, direct ingress bypass, and every
existing approval-binding attack through the authenticated path.

**Done when:** identities and permissions are enforced at the real API, and
authorization failure cannot become an approved or executed remediation.
This is scoped access control, not a claim of a full multi-tenant platform.

## Step 4 - Optional: turn the incident workflow into a useful deployed application

Use one narrow scenario: a simulated service incident, evidence-backed
diagnosis, approval of an exact sandbox configuration change, and verification.
Start inside the private trusted sandbox; DUR-038 is not a prerequisite there.
Until identity enforcement is added, demonstrate approval binding and ordering,
not authenticated approver identity. Do not expand the network access boundary.

1. Expose a documented definition-registration path or controlled deployment
   step so a user can submit the example without manually inserting database
   rows. Pin graph and activity versions.
2. Run the complete path through the actual API, deployed schedulers,
   outbox/Kafka, Python workers, result API, and production effects.Service.
   Extend the current pure.echo/pure.add allowlist only with the reviewed
   investigation and sandbox activities needed for this scenario.
3. Reuse PostgreSQL source_corpus and persist retrieved evidence, tool/model
   outputs, proposal, approval, and receipt. Require document-grounded
   citations and reuse committed outputs on resume.
4. Provide CLI commands for submit, status, timeline, approve, reject, cancel,
   and reconcile. Reconciliation must show unknown outcomes and require an
   audited operator decision; it must not silently redispatch unsafe effects.
5. Make a lightweight read-only timeline available for demonstration, either
   rendered from the CLI or as one server-rendered page. Limit it to state,
   attempts, owner epochs, retries, approvals, receipts, and evidence. A visual
   workflow editor or large frontend is outside this extension.
6. Demonstrate success, rejection, insufficient-evidence abstention, and
   uncertain external outcome. Keep a deterministic provider as the default
   reproducible example and an explicitly selected live-model mode.

Acceptance includes a worker interruption after a cooperating sandbox effect
commits but before reporting its result: recovery returns the original receipt
and the independent sink ledger records one mutation. A non-cooperating sandbox
endpoint must instead retain an unknown outcome and require reconciliation.
Do not call real production remediation endpoints.

**Done when:** someone following the quick start can submit, inspect, approve,
and recover an incident workflow without modifying tests or database fixtures.

## Step 5 - Optional: prepare a repeatable private cloud deployment

Select this after DUR-041a reports, when independent-machine recovery evidence
adds value for the target roles. The core cloud campaign uses existing deployed
pure activities; neither DUR-038 nor DUR-039 is on its critical path.

Recommended initial target: AWS EC2 on-demand Linux VMs, Terraform, and the
existing container images. Use one provider and one region. Start with enough
memory for measured workloads, then choose exact types from a sizing pilot.
Avoid CPU-credit confounding in performance studies: prefer non-burstable
instances or explicitly measure and control credit behavior.

| Role | Placement | Purpose |
|---|---|---|
| Application A | VM A | Scheduler/API and part of the fixed worker pool |
| Application B | VM B | Peer scheduler/API and remaining workers |
| Durable dependencies | VM C | PostgreSQL, Kafka, and scoped monitoring |
| Controller/load source | Outside fault victims; separate VM if needed | Fault observation and load generation |

Two worker slots on each application VM is a useful initial fixed pool.
Document the resulting loss of worker capacity when an application VM stops.
VM C is a single dependency failure domain; the campaign does not establish
database failover or full infrastructure HA. Separate guest VMs also do not
establish physical-host separation unless placement is explicitly controlled.

1. Record region, VM/volume types, CPU/memory limits, image digests, filesystem,
   PostgreSQL durability settings, network topology, and clock observations.
2. Provision network rules, roles, volumes, and instances with Terraform.
   Bootstrap pinned images/configuration. Configure remote Kafka advertised
   addresses and database access explicitly; do not inherit localhost values.
3. Keep application APIs, PostgreSQL, Kafka, and telemetry on a private network
   with explicit security-group rules. Use authenticated SSH/VPN access for the
   trusted operator; do not open unauthenticated write endpoints to the internet.
   This host-access boundary is not API-level identity enforcement. Record that
   limitation until DUR-038 is selected and accepted. A public portfolio page
   uses recorded/read-only evidence; the cluster need not run continuously.
4. Exercise create, verify, update, and destroy on tagged resources. Export
   evidence before teardown. Check for remaining volumes, snapshots, addresses,
   logs, and other billable resources afterward.
5. Prepare a cost manifest with current region-specific prices, instance-hours,
   storage, addresses, data transfer, monitoring, artifact retention, and tax
   assumptions. Billing alerts supplement resource deadlines; they are not
   guaranteed hard spending stops.

Planning recommendation: start with a **$200 total engineering envelope** for
this extension, allocated as $150 cloud experiments, $20 additional model use,
and $30 contingency. This is a proposed ceiling, not a provider quote or a
previously approved numerical cap. Record the finite execution budget under
the user's spending direction before provisioning. Keep it separate from D013's
existing $30 evaluation authorization. Use teardown deadlines and instance-hour
limits; revisit spending only for a concrete missing result.

**Done when:** the topology can be created from source, runs an existing deployed
workflow, exports telemetry, and can be torn down with its costs accounted for.

## Step 6 - Deliver the local campaign first; select multi-host work separately

### Step 6a / DUR-041a - Minimum: local isolation and recovery

Prerequisite: R096 is independently fixed and accepted. This step consumes
that fix; it does not implement it or redo its regression work. Record its
accepted commit and timeout/recovery limits. Use the existing runtime, workers,
and dependencies in an isolated local container/network environment. Do not
wait for DUR-037, DUR-038, DUR-039, or DUR-040 to begin this campaign.

Pilot fault injection and observation, freeze the protocol, then run the matrix
below. Publish the local campaign as a complete, independently reviewable result,
not as provisional evidence awaiting cloud execution. Local namespaces/containers
can establish network-isolation behavior; they do not establish independent-host
failure behavior. Keep those claims distinct.

**Done when:** the local matrix, matched controls, negative control, reconciliation,
and cleanup pass independent review, with a reproducible failure/progress timeline.
This closes DUR-041a alone; cloud work may remain unselected indefinitely.

### Step 6b / DUR-041b - Optional: multi-host extension

Prerequisites: accepted DUR-041a, DUR-040 topology, and a recorded finite budget.
State the incremental question before spending: independent kernels, cross-host
network behavior, and application-host loss, not rebranding a local partition
test as something impossible on one host. Do not claim injected clock skew
merely because hosts have independently observed clock offsets.

Reuse the protocol and checker with a separate campaign ID and artifacts.
Pilot remote transaction/scheduling delays and recalibrate lease/progress bounds
before freezing final runs. Do not import short local TTLs unchanged.

**Done when:** the cloud matrix passes independent review with confirmed host
faults, useful-progress measurements, explicit dependency-host exclusions,
cleanup, and actual costs. Publication of DUR-041a does not depend on this.

### Shared campaign protocol

Required core matrix: three fault scenarios, ten episodes each, alternating
the victim so both application replicas are exercised. Add ten matched no-fault
control episodes per campaign with the same workload, topology, and settings.
Report observed sample counts and retain every final episode,
including failures. Extra TTL arms are optional if they answer a clear question.

| Scenario | Required observation |
|---|---|
| A loses PostgreSQL access; B takes over; A reconnects | The old process retains its obsolete epoch and cannot commit a later mutation |
| A is isolated at an acknowledged boundary while holding the lease-row lock | Healthy work continues; abandoned-lock cleanup permits bounded takeover and affected-work progress |
| Application A stops abruptly with a worker claim in flight | Local: kill the declared application container/process group. Cloud: abruptly stop VM A. Confirm victim processes stop without graceful cleanup and B recovers work with dependencies intact |

The local campaign uses container/process loss and must be labelled accordingly.
Cloud runs must record a provider operation that bypasses graceful shutdown
and independent evidence of the observed loss. Preserve the old process during
the isolation/reconnect case; replacing it with a fresh owner cannot test
stale-identity fencing.

Per episode retain source/config hashes, workload, fault target, confirmation,
old/new identities and epochs, lease/attempt deadlines, durable takeover and
mutation evidence, first useful affected-work progress, final states, outstanding
obligations, sink receipts, and cleanup status. Confirm injections with packet
or connection observations and backend/process state, not controller intent.

Use a single observer's monotonic clock for elapsed durations. Record command
latency and fault-observation uncertainty separately. Prove transaction ordering
through committed observations; timestamps generated inside a transaction are
not automatically commit timestamps. Observed clock offsets do not constitute
an injected clock-skew campaign.

Freeze safety and progress checks before final runs:

- Zero superseded-owner mutations after committed takeover.
- Healthy-partition progress within its declared bound while contention persists.
- Affected-work progress within a bound accounting for lease expiry, abandoned
  transaction cleanup, worker-claim expiry, and scheduling delays.
- Reconciled accepted work and effect outcomes; waiting approval or unknown
  outcomes must be separately enumerated, not counted as completed or lost.
- Test-only negative control fails the independent checker when fencing is
  weakened. Unknown boundary, missing evidence, or failed injection fails a run.

Report episode counts and median/min/max, plus failures and assumptions. Avoid
tail-percentile claims from ten episodes. Preserve pilot and final artifacts
separately; do not rerun until a convenient result appears.

For each campaign, report exactly which scheduler and worker processes were
lost and what dependencies survived. A killed scheduler alone is not a killed
worker, and application-host loss is not a database-host durability test.

## Step 7 - Optional: measure deployed capacity and improve one actual bottleneck

Select this for a demonstrated performance question, not to fill out a feature
list. It can use a stable isolated local deployment; cloud and the richer incident
application are not prerequisites for measuring existing deployed activities.

1. Exercise the real API-to-worker-to-completion path with an open-loop load
   generator outside the application processes/fault victims. Keep worker slots,
   application/DB resources, graph, payloads, and activity cost fixed within
   comparisons.
2. Pilot at least two workloads: cheap sequential activities exposing runtime
   overhead, and bounded I/O/fan-out work representing the application. Keep
   human-approval and live-model latency out of scheduler-capacity comparisons.
3. Calibrate below-saturation, near-capacity, and overload offered rates.
   Freeze warmup, steady-state duration, drain deadline, and SLOs. Start with
   three independent repeats and at least ten steady-state minutes per cell;
   increase duration only when needed for useful precision within the budget.
4. Separate offered/accepted/completed/rejected work, queue age, dispatch and
   completion latency, scheduler/worker CPU, DB time and lock waits, connection
   use, broker lag, and outstanding obligations. Publish sample sizes with
   p95/p99; label sparse or unstable tail estimates as inconclusive.
5. Profile the bottleneck with CPU profiles, query statistics, and query plans.
   Candidate areas include repair scans, repeated lease work, indexes, serial
   scheduling, and connection pressure. Treat them as hypotheses until measured.
6. Implement one justified improvement. Preserve the before commit, repeat the
   matched study, and rerun relevant safety/liveness regressions. Retain noisy
   or negative results; do not promise a percentage improvement in advance.
7. Establish an overload contract: bounded admission/dispatch, finite resource
   use, clear retry responses, and drain after load subsides. Count rejected
   requests separately. Unknown submission outcomes must keep the same-key
   retry semantics. Do not drop accepted durable work to improve latency.

Derive conclusions from the measurements and their variation. If uncertainty
overlaps the effect, report the measured limit and unresolved optimization.

**Done when:** a reviewer can trace one bottleneck from profile to design change
to matched results, or inspect a useful, explicitly inconclusive investigation.
The study must also show what happens when offered load exceeds capacity.

## Step 8 - Optional: demonstrate observable operation and release recovery

1. Correlate API requests, workflow/attempt IDs, outbox events, worker execution,
   and receipts in traces/logs. Keep unbounded workflow IDs out of metric labels.
2. Provide dashboards for queue age, completion latency, retries, stale-owner
   rejections, lock contention, poison/reconciliation obligations, and resources.
   A counter must be connected to real behavior and render its zero state.
3. Trigger a stalled-work alert and a backlog alert with actual faults/load.
   Record detection time, diagnosis using the dashboard/logs, recovery action,
   and return to normal. Idle snapshots alone do not validate recovery.
4. Run a bounded soak at a measured below-saturation rate; an initial two-hour
   window is enough to investigate leaks/backlog without implying a long-term
   reliability record. Report resource drift and unfinished work.
5. Demonstrate a rolling runtime/worker update and rollback with workflows in
   flight. Preserve activity-version compatibility; unsupported versions pause
   predictably. Use new forward migrations and record rollback compatibility.
6. Validate a backup restored into a separate database at a declared backup
   point. Reconcile records included at that point and report the recovery-point
   boundary. Do not equate backup restoration with database-host crash durability.

**Done when:** a runbook can guide someone from an observed symptom through
diagnosis and recovery, and one incident report links the behavior to raw evidence.

## Step 9 - Optional: keep the applied-AI addition small and useful

Existing accepted AI evidence is already sufficient as a supporting portfolio
element. Select this only if deployed integration fills an actual demonstration
gap. A private trusted evaluation does not depend on DUR-038; public or untrusted
access does. Do not imply authenticated approval from a private dev-only run.

1. Run the incident example through the deployed workflow using the existing
   model configuration initially. Preserve deterministic replay as the default.
2. Persist a model result before advancing; test that recovery reuses it and
   accounts for any ambiguous pre-commit call cost separately.
3. Use a small frozen set covering supported diagnosis, abstention, malicious
   retrieved instructions, unauthorized citations, and rejected approval.
   Keep runtime enforcement separate from the model's proposed behavior.
4. Retain demonstrated negative controls: unredacted canary exposure,
   unsupported/empty citations, and unsafe evidence affecting an undefended
   provider. Use synthetic data and sandbox actions.
5. Report task success/false abstention, approval enforcement, citation/leakage
   outcomes, calls/tokens/cost, and sample counts. Keep historical DUR-029 results
   intact; these are supplemental deployed results with their own source/config.
6. If the current model is unhelpful for the demonstration, analyze a small
   development set and change one factor at a time. Record any revised provider,
   model, prompt, or budget before running it. Do not turn this into another
   large model-selection or retrieval project.

**Done when:** the application shows how model uncertainty is contained by
durable execution, citations, and approval, with modest measured AI claims.

## Step 10 - Minimum: publish a hiring-facing presentation and get a real reader

1. Rewrite the README opening around problem, architecture, three accepted
   findings, a brief demo, and quick start. Keep audit history linked, while
   detailed task/finding IDs live in the engineering records. Put material
   limitations beside the corresponding claim.
2. Publish a small architecture diagram, a two-to-three-minute overview video,
   and one failure timeline. The operator should visibly submit, interrupt,
   recover, and inspect the result. A recorded/read-only demo is sufficient;
   continuous public access to paid compute is not required.
3. For the minimum package, write one focused case study connecting the separate
   R096 diagnosis/fix to DUR-041a's local isolation and progress evidence. Link
   the accepted repair evidence without taking credit for unperformed work.
   Include rejected alternatives and how the test can fail. Add multi-host or
   bottleneck case studies only if those optional tasks are delivered. Explain
   why an educational custom engine was appropriate here and when a mature
   workflow product would be the better choice for a business.
4. Update the claim-to-evidence register with exact accepted commits, artifact
   paths, workload, host, sample size, and scope. Separate old local harness
   measurements, deployed cloud evidence, and model results.
5. Ask a peer to follow the quick start and reproduce one fault without private
   guidance. Record observed difficulties and fix them. Describe this as peer
   usability/reproduction feedback; do not invent customers or adoption.
6. Publish source, CI results, commit-addressed evidence, and the demo once their
   relevant review gates pass. Do not wait for optional projects or a new release
   tag to start applications. If creating a release, apply its separate review
   and authorization rules; keep the original tag as its historical snapshot.

**Done when:** an unfamiliar reader can understand the system quickly, watch
its recovery, inspect a claim, and reproduce one example from public materials.

## Step 11 - Convert the evidence into applications and interview fluency

1. Position the project as "Durable Workflow Runtime | Go, PostgreSQL, Kafka,
   Python" with AWS/Terraform/CI/observability added only after actually used.
2. Prepare two resume variants: backend generalist (APIs, data, operations,
   performance) and distributed/platform (fencing, failure recovery, messaging).
   Keep the project to a few strong bullets; applied AI is a supporting element.
3. For each bullet use problem -> design choice -> measured evidence -> scope.
   Do not fill the resume with review rounds, task IDs, tests passed, or planned
   features. A scoped failure investigation can be stronger than a large number.
4. Match actual skills to a small set of current target job descriptions. Use
   conventional headings and searchable text; no keyword stuffing, invented
   employment, or fabricated business impact. No ATS score promises selection.
5. Before interviews, the user completes R092 under their own attribution.
   Practice a short architecture explanation and deeper discussions of lock
   lifetime versus lease expiry, ambiguous commits/effects, Kafka's measured
   tradeoffs, overload, and one debugging case personally reproduced.
6. Start applying while optional refinements continue. Track roles, resume
   version, stage reached, and technical feedback. Adjust presentation or a
   demonstrated gap before adding another subsystem.

**Done when:** each resume claim is backed by accessible evidence and the user
can explain the relevant code, tradeoff, failure, and limitation unaided.

## Common evidence and validation contract

New command names must be implemented and documented in each task record;
do not present a proposed command as already runnable. Every task should expose
a repeatable validation entry point and an isolated, run-ID-scoped cleanup path.

Suggested future evidence layout:

```text
experiments/portfolio/
  local-recovery/<campaign-id>/
  cloud-recovery/<campaign-id>/
  deployed-load/<campaign-id>/
  operations/<campaign-id>/
  deployed-agent/<campaign-id>/
```

Each campaign contains protocol/configuration, exact source and image hashes,
host manifest, raw per-run data, computed summaries, independent checker output,
negative controls where relevant, cleanup report, and cost manifest. Export
durable snapshots before deleting fixtures. Redact secrets while retaining
enough identity and ordering evidence to reproduce the checks.

Use service-backed Go race tests, Python checks, fresh/upgrade migration tests,
container builds, deployed smoke, and targeted fault tests proportionate to the
change. Do not rerun every historical paid study after unrelated code changes.
Do not call production transition validators to decide the same invariant in
the independent checker.

## Stop rule and explicitly conditional work

The minimum package is shippable when the separately repaired R096 has accepted
evidence, DUR-041a demonstrates local safety and useful progress, DUR-037 has a
passing hosted run, and DUR-045 makes that work inspectable and reproducible.
This is a presentation milestone, not a waiver of existing release criteria or
the user's R092 exercise. Begin applications; do not make the optional backlog
a prerequisite for using the project.

Choose at most one next extension at a time, with a concrete reason:

- Independent-machine recovery evidence for distributed/platform roles:
  DUR-040 then DUR-041b, with a newly recorded finite budget.
- A reader cannot use or understand the end-to-end path: DUR-039.
- A measured bottleneck or performance-oriented role: DUR-042.
- An operational debugging/release requirement: DUR-043.
- Actual untrusted access or an explicit security requirement: DUR-038 before
  that exposure; private trusted campaigns do not need to wait for it.
- A relevant applied-AI integration gap: DUR-044 after DUR-039, retaining the
  backend/distributed-systems emphasis.

After each accepted increment, update the presentation and reassess against
screening feedback. If none addresses a concrete gap, stop building and invest
the time in applications, peer reproduction, and personal interview practice.

Do not delay applications for Kubernetes, custom consensus, multi-region
replication, database automatic failover, generalized workflow editing, broad
multi-tenancy, or more AI frameworks. Add any of these only for a specific role
requirement or observed system limitation and with its own scoped acceptance.
Database-host hard-crash durability is also a separate possible study; application
VM loss must not silently close that caveat.

## External references checked while drafting

- [GitHub standard hosted runners](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job): standard hosted execution is free for public repositories; account/storage and private usage conditions still need checking for the chosen setup.
- [EC2 on-demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/): price the selected region/types and include non-compute resources; the planning envelope above is not a quote.
- [EC2 stopping behavior](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Stop_Start.html): ordinary stop attempts graceful shutdown; the abrupt-loss experiment must select and verify the intended stop behavior.

Hiring priorities in this guide are engineering judgments based on the current
repository and the user's target. They are not measured estimates of interview
probability.
