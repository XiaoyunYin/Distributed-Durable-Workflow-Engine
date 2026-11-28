# J8 verifiable release registration

Slice 0 is pending Claude review. The registration base is
`bd5664f660a4f23a1b239edc4bd484c6ea1c54d8`. J8 uses the scoped DUR-054
pull-forward in PLAN §18 and the D037 B4 amendment. D036 preserves the release
tier, DUR-046, scan disposition and owner authorization gates. D037 and D038
set the hosted-only budget and opt-in profile rules. J13 owns the later second
release and the cross-release contract check.

The claim to test is that a reader can identify the exact released Linux image
bytes, verify their provenance, complete the checked Compose quickstart, use
the documented authenticated contract and replay one checked historical result
offline. CI must reject the registered breaking changes and accept the additive
change. The release measurements cover one Compose stack on one GitHub-hosted
runner per repeat.

## Eligible tier and reviewed evidence

**Full v1 is the highest evidenced candidate tier under PLAN §10.** The table
maps the completed historical requirements and their review basis. Eligibility
for the J8 tag still depends on Claude reviewing this mapping, the exact
release SHA and its current claims. Use the historical scope on each result.
If a tier requirement lacks evidence or its reproduction is missing from the
candidate package, stop and review the eligible lower tier before tagging.
The reviewed D036 pull-forward keeps full DUR-045 refresh and accepted DUR-050,
DUR-052 or DUR-053 results as prerequisites for claims that depend on them.

| Tier requirement | Reviewed evidence and scope |
| --- | --- |
| Core Engine MVP deployment, transport and bounded throughput smoke | M5/DUR-025, Claude Round 23 at `acb28ba`, covers the [real dependency outages](../../m5/outage-recovery.json) and [four-workflow two-owner smoke](../../../internal/engine/m5_smoke_test.go). DUR-032 Round 49 covers two schedulers and the deployed Python/Kafka path. The [DUR-048 pilot](../../m8/dur042-pilot/) was accepted in Round 75 and covers a pure deployed path through one host. |
| Core Engine MVP durable correctness | M5 [48 F01–F11 cases and checker evidence](../../m5/README.md), including retries, checkpoints, fencing, both effect classes and approval boundaries. Round 50 verified the corrected snapshots at `0002e75`. The [production-path approval and effect fixture](../../../internal/incident/production_integration_test.go) is DUR-033A, Round 45, with a synthetic incident and a sandbox action. |
| Portfolio MVP deterministic incident, tool and retrieval paths | M6 Round 26 at `1048ad0` covers the [deterministic incident workflow, separate retrieval benchmark, frozen gates and provenance checks](../../m6/README.md). DUR-055 Rounds 136 and 138 cover the later [real PostgreSQL FTS/pgvector and official SDK MCP evidence](../../m8/dur055-applied-ai-upgrade/README.md). Preserve the distinction between the SQLite continuity fixture and the PostgreSQL retrieval and integration cases. |
| Portfolio MVP approval walkthrough, F12, adversarial controls and observability | M6 Round 26 accepts the [continuity and F12 record](../../m6/continuity.json), [adversarial/redaction results](../../m6/adversarial.json), [citation checks](../../m6/citation-check.json) and [model-behavior dashboard](../../m6/dashboard.json). Link the deterministic approval walkthrough and the separate DUR-033A integration from the release claim map. |
| Full v1 frozen engine studies | Reviewed M7 [scheduler throughput](../../m7/dur026/results.json), [safeguard cost](../../m7/dur034/results.json), [dispatch decomposition](../../m7/dur035/results.json), [lease recovery](../../m7/dur027/results.json) and [checkpoint tradeoff](../../m7/dur028/results.json). Use PLAN's M7 closeouts and the [reviewed public claim map](../recruiter/claim-evidence.md). Retain the unresolved safeguard effect, unresolved dispatch increment and historical topology limits. |
| Full v1 retrieval and bounded live/adversarial studies | DUR-029 Round 44 at `5b9d65c` covers the [development-frozen held-out retrieval comparison](../../m7/dur029/retrieval-final.json), [live retrieval-arm results with the document-dependent subset](../../m7/dur029/live-evaluation.json) and defended-versus-plain injection ablation. The release can link these historical results at their recorded limits. |
| Full v1 report and interview evidence | DUR-030 Round 46 at `51e1957`, DUR-031/M8 under D019 and DUR-032 Rounds 49–51 cover the report, evidence reproduction and engineering walkthroughs. The tracked reader entry points are [DESIGN.md](../../../DESIGN.md), the [public claim map](../recruiter/claim-evidence.md) and the [failure case study](../recruiter/failure-case-study.md). The walkthroughs are Codex-performed engineering evidence. |

Before any release write, bind the tier, claim map, scan and workflow policy,
and `DUR-046` CI receipt to one reviewed protected-branch commit. Require green
exact-SHA hosted Go/Python/service checks and the registered DUR-046 gates,
including passing restored baselines and named mutation detections, fuzz,
property and reference-model coverage and the existing vulnerability check.
Required integration skips and substitutions invalidate admission. Require
reviewed J12 qualification and J19 evidence, the merged J9a reader, and the
applicable merge order before accepting the J8 release candidate. At slice 0,
the J12 implementation and harness have a nonblocking review (Round 681).
The current checkout lacks the J9a reader and registry row. The existing
`v0.1.0` tag at `7e4137d` is a historical fixture source. This registration
chooses no tag value and supplies no publication authority.

## Release images, modes and migration inventory

Build a single publication manifest for Linux/amd64 per application image from
the exact candidate source export, with the production `runtime` target in
[`deploy/local/Dockerfile.runtime`](../../../deploy/local/Dockerfile.runtime)
and [`deploy/local/Dockerfile.worker`](../../../deploy/local/Dockerfile.worker).
Use the pinned base images, Go module/toolchain versions and hash-locked Python
requirements. Require source revision labels and retain resolved build inputs,
target, platform, manifest, config and layer identities. Check the runtime build
settings and both image file inventories against their production targets.
Use plain release builds without campaign tags, campaign targets, overlays or
instrumentation switches. Transfer sealed OCI image bytes into publication,
using digest-preserving registry copy. A rebuild or manifest rewrite requires a
new candidate. BuildKit-generated auxiliary attestation manifests stay outside
the selected image manifest. Attach GitHub provenance to that manifest digest.

Target `ghcr.io/<repo-owner-lowercase>/distributed-durable-workflow-engine-runtime`
and the corresponding `-worker` package. Resolve exact full names and
`@sha256:<manifest-digest>` references in the reviewed candidate manifest. An
OCI version alias may be staged only under the registry authorization. All
consumer instructions, Compose images and verification targets use digests.
On readback, compare registry manifest bytes and platform against the sealed
candidate, then independently join the pulled digests to every relevant
container's config/image identity. Apply the same pinning to PostgreSQL and
Kafka using the `postgres` and `kafka` entries in the candidate's
[`deploy/k8s/tools.lock.json`](../../../deploy/k8s/tools.lock.json).

| Release setting | Registered value or inventory |
| --- | --- |
| J12 public authentication | `RUNTIME_PUBLIC_API_AUTH=1` on all Go runtimes. Separate public and worker-control listeners in drain mode. Operator and demand roles remain private and receive only their registered credentials when enabled. |
| J19 expiry handling | `RUNTIME_TIMEOUT_RETRY_POLICY=1`, already permitted for `J8`. |
| J9a relay | `RUNTIME_RELAY_FULL_BATCH_LOOP=1`, after the unique J9a registry entry and production reader are present and permit `J8`. |
| Other runtime opt-ins | Off, including J4 activity/resilience, J9a stage timings, J7 drill mode, K6 capture and fault hooks. Admission limits are off in the release and quickstart. Two separate contract-only checks explicitly turn them on. |
| Engine database | Exactly base `migrations/000001` through `000023` up files and `migrations/j12/000026_j12_workflow_requesters.up.sql`, in that order. Expect ledger versions `1..23,26` and the J12 requester table. Run the profile migrator before definition seeding or listener startup. |
| Separate databases | Quickstart provisions PostgreSQL for the engine only. K6 capture 000024, J4 B2 000025, separate DUR-055/DUR-056 retrieval inventories and sink migrations are outside the J8 profile. |

The implementation slice must register `J8` as inheriting the J12 migration
profile and permitting J12 auth in the single switch registry. The J12 file
stays outside the base migrator's existing top-level glob. Check the reviewed
file list and hashes, rendered settings, running process modes, route
inventory and actual schema ledger. Reject omissions, additions, misleading
tag-to-digest matches and unregistered overrides. Preserve default-off
equivalence and K manifests.

## Supply-chain policy

Use [Syft v1.57.2](https://github.com/anchore/syft/releases/tag/v1.57.2) to
catalog runtime/worker final image contents and emit SPDX JSON for publication
and Syft JSON for scanning. Use [Grype v0.122.0](https://github.com/anchore/grype/releases/tag/v0.122.0)
on each image's bound inventory with a full JSON report. Before execution,
pin the upstream Linux/amd64 archive checksums in the candidate's tool lock
and verify the checksums and installed versions. Include installed OS
packages, Go executable modules/toolchain metadata and Python dependencies.
An expected application catalog absent from the SBOM invalidates the scan.
The bound record includes the exact image digest, architecture, both SBOM
hashes, Grype version and the exact vulnerability database checksum, schema,
build time and source. Acquire and validate one compatible database snapshot
for the pair, at most 72 hours old at evaluation and final publication. Pin
that snapshot across the candidate and registry-readback scans. Missing,
unusable or stale data blocks the release. Refresh under the same policy and
review both reports again if the snapshot expires. [Anchore scan targets](https://oss.anchore.com/docs/guides/vulnerability/scan-targets/)

Apply the following policy to both shipped images, with fixed and unfixed
findings visible. Cross-check the report against the manifest/SBOM pair before
registry push and recheck after anonymous registry pull, before tag creation.
Use the complete raw findings and an independent disposition table.

| Finding | Disposition before release |
| --- | --- |
| Confirmed Critical | Blocks release until remediated on a newly scanned candidate. |
| Confirmed High with a fix | Blocks release until remediated. |
| Confirmed High without a fix | Blocks release unless the owner records a bounded exception in `REVIEW.md` for that digest, advisory and package, with Claude review, an exposure analysis, compensating control, remediation owner and expiry within 14 days of approval. |
| Medium or Low | Retain and triage with an owner and tracking reference. These findings alone do not block the severity gate. |
| Unknown or disputed | Hold until reviewed classification and disposition. A false-positive or demonstrably unaffected decision needs image-specific technical evidence, independent review and an explicit record keyed to the finding and digest. |

An exception is valid for the recorded image and window only. Record severity,
advisory and package identity, affected/fixed versions, rationale, reviewer and
expiry. Scanner errors, stale/partial reports, omitted expected packages and
unresolved finding dispositions fail closed. A cleared report supports only
the registered scan decision at its database cut.

Generate a GitHub SLSA build provenance attestation for each sealed `subject-name`
and `subject-digest` from the same hosted workflow run that built the image.
Bind staged archives to that build run, candidate SHA and reviewed workflow
identity before approval. Write the attestations to GitHub's attestation store.
The registered workflow uses GitHub's `actions/attest` with a reviewed immutable
action pin and `push-to-registry: false`. Verify with `gh attestation verify`
against the actual digest-addressed OCI image and same-run bundle, matching the
expected repository, signer workflow, source SHA and GitHub-hosted builder.
Both runtime and worker must pass. For the negative, keep the valid runtime
bundle, expected signer and source fixed and substitute the independently
resolved worker manifest digest. Require both OCI manifests to exist and the
worker's own bundle to pass before running the substitution. Require a
subject-digest rejection with a healthy verifier and registry; a missing image,
bad signer input or tool failure yields no negative-control pass. Record
command inputs, verified subjects, exit statuses and scrubbed diagnostics.
[GitHub provenance documentation](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations) · [GitHub CLI verification](https://cli.github.com/manual/gh_attestation_verify)

## B4 cold release quickstart

Ship a standalone release Compose file and README commands for Linux/amd64.
The file names the released runtime and worker digests, pinned PostgreSQL and
Kafka, one public API on a loopback-published port, two scheduler runtime
replicas, one private worker-control listener and two Python workers with one
slot each. The scheduler roles supply the relay and event ingestion paths.
Use one Compose project per run, a private fixture network, and fresh
database and broker volumes. Rendered services must have no `build` sections,
select no candidate-local tag, and preserve the registered modes and schema.

Ship a checksummed Linux/amd64 helper bundle built from the reviewed candidate
without special build tags. One-off migration, seed and checker services can
mount those helpers read-only with explicit entrypoints. Reuse the runtime
image by its released digest. The release README registers their prerequisites
and mounts. Generate fixture RSA-2048 tenant keys locally, publish only the
public JWKS to the API, and use issuer `https://j8-fixture.invalid`, audience
`durable-j8-api`, subject `requester` and namespace `j8-quickstart`. Destroy key
material and credentials during teardown. Use loopback for tenant traffic.
The Compose-only private worker setup uses a separate TLS fixture TokenReview
service, trust root, issuer, signing key, reviewer credential and bound caller
token for a dedicated worker audience. The fixture verifies the signature,
issuer, audience, expiry and expected service account and returns the fixture
pod identity. Mount the worker token through the existing `WORKER_TOKEN_FILE`
seam. Verify both a valid worker credential and a denied tenant token on the
worker listener. J12's separate qualification is the real Kubernetes
TokenReview evidence.

Seed exactly one workflow definition version in the migrated store before
submission, named `j8-quickstart` version `1`, running `pure.add@v1` then
`pure.echo@v1` and success. Use the `v0.1.0` demo's execution inputs `[1,2,3]`
and an empty payload with a fresh fixture workflow ID and submission key.
Submit through authenticated HTTP. Poll the released status route, then use
the candidate-bound independent checker and a separate read-only result
witness. Pass requires `SUCCEEDED`, result `6`, one accepted submission,
one durable successful receipt per expected node and no unexpected workflow
or effect in the project. Keep the verifier's accepted-ID and terminal checks
in the timed boundary. [Historical submit fixture](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/blob/7e4137d/cmd/local-demo/main.go) · [Independent checker](../../../cmd/dur049-checker/main.go)

Run **three** independent fresh `ubuntu-24.04` GitHub-hosted Linux jobs per
candidate, each with a 15-minute job cap and two minutes reserved for teardown.
Use an empty job-owned image/content store from a fresh Docker daemon and
empty volumes. Prove the initial cache and volume checks before pulls. No
image pre-pull, local image build or warm retry belongs in the measured run.
Record the runner image release, CPU, memory, Docker/Compose and helper
versions, job start, every image/digest, and tool installation or inherited
tools and caches. Time helper/checkout acquisition separately before `t0`.
Disclose registry-side cache state as unobserved; the cold gate covers the
runner's local image layers.

| Clock | Boundary and cutoff |
| --- | --- |
| Pull, `t1 − t0` | `t0` is immediately before `compose pull` for all pinned images. `t1` is after successful pull and digest checks. At most **300 seconds**. |
| Start-to-check, `t2 − t1` | Starts before local fixture key setup, PostgreSQL/Kafka startup, migration, seeding, listener/worker startup, submission, terminal observation and independent checking. `t2` is the finished checker verdict with the reconciled workflow ID. At most **180 seconds**. |
| Combined, `t2 − t0` | At most **480 seconds**. Use monotonic elapsed clocks on the same runner. |

Record actual running digest/config IDs, the ledger, readiness, submitted and
terminal IDs, checker verdict and teardown. Report both elapsed times for each
valid run, the minimum and maximum for each clock, `[max − min]` spread, and
the valid/failed/invalid/unperformed denominators. A witnessed cutoff miss
under valid conditions is a fail. A cache or topology violation, missing
evidence or runner failure is invalid. There are no automatic replacements.

## Shipped HTTP contract and seeded controls

Publish OpenAPI 3.1 in `api/openapi.yaml` for the tenant `/v1` service. Publish
`api/openapi-internal.yaml` for the shipped private role variants. Each
operation declares its listener and credential, with role-specific servers in
the internal spec. Document `/healthz`, `/readyz` and `/metrics` in the
operational inventory. The frozen tenant contract covers the five paths and
seven methods below. Internal/demand routes appear in their own inventory.
Use the long-form path parameter names consistently across the code and
documents, or normalize names while comparing their path positions.

| Listener and credential | Shipped `/v1` methods and paths |
| --- | --- |
| Public tenant RS256 bearer | `POST /v1/workflows`; `GET, HEAD /v1/workflows/{workflowID}`; `GET, HEAD /v1/workflows/{workflowID}/history`; `POST /v1/workflows/{workflowID}/cancel-request`; `POST /v1/approvals/{intentID}/decision`. |
| Private operator TokenReview | `POST /v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/approval`; `POST /v1/approvals/{intentID}/apply`; `POST /v1/cancellations/{requestID}/apply`. |
| Private worker TokenReview | `POST /v1/worker-deliveries`; `POST /v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/{claim,heartbeat,result}` as three operations. |
| Private demand bearer | `GET, HEAD /v1/demand` with its dedicated mounted credential. |

Compare the OpenAPI operation sets and operation-level security against an
independently enumerated method/listener inventory of the candidate's
[`internal/api/server.go`](../../../internal/api/server.go) and the
[J12 protocol](../public-api-auth/protocol.md). Resolve implicit ServeMux
`HEAD` methods explicitly. Use running-role tests to confirm route presence,
404 on wrong roles or methods, and 401 for wrong credentials on the assigned
role. Confirm representative status codes, headers, required response fields
and types against the spec using samples from real handlers. Keep J4 B2 routes,
combined defaults, and J13's list API outside the J8-on versioned inventory.

Freeze one HTTP-only contract client and expectation corpus on the reviewed
J8 SHA before testing mutants. Fixture setup may supply authenticated
principals, seeded definitions, intent IDs/hashes or a known revision. The
client observes results by HTTP only. Exercise at least the following on the
candidate and preserve the client and SHA-256 for the later J13 run.

| Area | Required contract observations |
| --- | --- |
| Submit and idempotency | `201` with `created:true`, canonical same-subject replay `200` with `created:false` and exactly the same workflow ID, changed execution input `409 PAYLOAD_CONFLICT`, and a different subject's replay of the same namespace/key denied. Document canonical input hashing and namespace/key plus authenticated requester binding. Wrong body namespace returns `403 NAMESPACE_FORBIDDEN`; body actor has no authority. |
| Status and history | `200` status and ordered history with cursor/limit, schema-valid revision/IDs, bodyless HEAD and stale revision/cursor `409 STALE_REVISION`. Absent and foreign workflows return the same `404 NOT_FOUND` representation, including a foreign-ID history probe. |
| Cancellation | `202` for the path workflow with the stable request ID on same-key replay. Foreign workflow returns `404 NOT_FOUND`. A conflicting body identity field is rejected. |
| Approval decisions | A distinct in-namespace JWT subject commits the exact proposal `200`; a replay keeps the decision and reveals no dispatch grant credential. Same requester and legacy requester-less intent receive `403 APPROVAL_UNAUTHORIZED`. Foreign intent returns `404 APPROVAL_NOT_FOUND`. Body approver identity cannot change the authenticated actor. |
| Credentials and private paths | Missing or expired JWT and bad signature, audience or missing/malformed namespace claim return `401 UNAUTHENTICATED` on the tenant service. A valid JWT for a different namespace sees the same resource `404` as for a missing ID. Use a valid tenant JWT on every internal method and demand. Require 404 on public and 401 on the owning private listener. Use separate role fixtures for internal positive examples with the J12 authenticators. |
| Admission when enabled | Two isolated `dur050-j8-contract` fixtures set `DUR050_ADMISSION_MAX_ACTIVE` and `DUR050_ADMISSION_MAX_PENDING_OUTBOX` to `1/16` and `16/1` respectively. Hold scheduler and relay, submit one accepted fixture workflow, then submit a different key. Require `429 ADMISSION_LIMIT` for the active limit and `503 BACKPRESSURE` for the pending-event limit, both with `Retry-After: 1`. Replay the first key and require the idempotent `200`. Check the ordinary release profile with both limits off. |

Run the frozen client on the unmodified candidate, each of these four
isolated single-change controls, and a restored baseline. Every control runs
against its own fresh fixture with the same client and fixture identities or
equivalent deterministic replacements. Fault injection belongs only in
contract tests. Its patched service, adapter or image must never be eligible
for the release image transfer.

| Seeded change | Expected client result |
| --- | --- |
| Status contract | Change a successful new submit status from `201` to `200` while retaining its body and operation. Fail on the registered submit-status assertion. |
| Response field | Rename the required successful status field `workflow.revision` to `workflow.rev` with the same value and status. Fail on the missing required field. |
| Authentication contract | In the isolated HTTP contract fixture, substitute the authenticated status success representation for the absent-bearer `401`, preserving the route and shape. Fail on the absent-credential denial assertion. |
| Additive response | Add optional top-level `server_note` to a successful status response, preserving existing values, headers and status. The same client must pass. |

Require a healthy positive control and the named contract mismatch for each
break. Setup failures, timeouts, unrelated assertions and skipped cases cannot
count as detections. Record spec and client hashes, case inputs/expected and
observed codes, and a scrubbed schema verdict. The J8 CI claim covers these
seeded changes. J13 closes cross-release compatibility with the unchanged
client and a separately authorized later release.

## Offline replay and public repository controls

Publish `j8-offline-replay-linux-amd64.tar.gz` and its SHA-256 sidecar as
release assets. Fix archive member order and metadata for reproducibility.
Include `README`, `SHA256SUMS`, fixture provenance, a member inventory,
expected results, and the static `fault-checker` built from
`bd5664f660a4f23a1b239edc4bd484c6ea1c54d8` with Go `1.27.4`,
`CGO_ENABLED=0`, Linux/amd64 and no build tags. Pin its final binary hash in
the manifest. Include the historical [F01 trace](../../m5/traces/F01-response-loss-seed11.jsonl)
(`42a94b548191cbbccdb6014674f175f01c8588601f2641f309ad7b0760885922`)
and [durable snapshot](../../m5/durable/F01-response-loss-seed11.json)
(`3a4c0d3672e4cfc7af07917af5fa70efa4a69ab54f19d2bfdb46ce5eea2c1235`).
These are the corrected historical M5 fixtures accepted in Round 50. Preserve
the original bytes and scope. Document any release packaging label separately.

The independently hashed failing fixture is a copy of the same durable
snapshot with its sole `Submissions` row removed and the trace unchanged.
Both cases must pass the bundle integrity preflight. The following are the
checker invocations after isolation, using the packaged names:

```sh
./fault-checker -offline -trace fixtures/F01.jsonl -durable-trace fixtures/F01.json
./fault-checker -offline -trace fixtures/F01.jsonl -durable-trace fixtures/F01-missing-submission.json
```

The first must exit `0` with `valid:true`, one run and zero violations. The
second must exit `1` with `valid:false`, one run and the violations `outbox
event references unknown workflow` and `submission boundary has no durable
submission` with their original IDs. Require a parsed verdict for both cases.
Checksum mismatch, parse/setup failure or network use invalidates replay.
Check paths, member inventory and SHA-256 before execution. On the hosted Linux
runner use a fresh OS network namespace with no external interface or default
route and an empty credential environment. Run the verifier offline without a
database, source checkout, hosted CI artifact or package fetch. Before release
creation exercise the sealed bytes locally under that boundary. After authorized
publication, fetch the same versioned assets from the GitHub release URLs over
HTTPS without credentials or Actions artifact access, then repeat both runs
with networking disabled. Compare the public asset hashes to the prepublication
ones. Record both verdicts, commands, hashes and evidence of isolation.

Ship `SECURITY.md` with supported versions, a private reporting route and
responsible-disclosure instructions. Verify that the repository's private
vulnerability reporting route is enabled and owner-monitored, or record a
verified owner-provided private contact before publication. Ship a parse-checked
`.github/dependabot.yml` with Go modules at `/`, `uv` for `pyproject.toml`
and `uv.lock`, pip coverage for `requirements-worker.txt`, GitHub Actions at
`/`, and Docker base updates for `/deploy/local` and `/deploy/k8s`. Verify
that every registered manifest is visible to the corresponding updater and
check pinned-image files that need explicit lock maintenance. Dependency
update PRs use the usual review/merge checks. Released digest manifests and
checksum assets are immutable. [Dependabot options](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference)

## Publication authorization and permissions

The [workflow draft](release-workflow.draft.yml) stays outside the Actions
workflow directory. Its jobs are disabled and its gate steps deliberately
fail. A later reviewed implementation must install a protected workflow and
owner-controlled environments, validate their existence and protection before
use, and supply the reviewed gate implementation. An input, tag event,
passing job, Claude review or prior release authorization is insufficient.
Each outward-facing operation below requires its own explicit owner
authorization recorded in main-checkout `REVIEW.md` before the operation.

Bind each authorization to operation, exact candidate and workflow SHA,
repository and target ref/package/tag/release, evidence manifest and digests
where applicable, approved runner/time limits, and a one-use run/attempt scope.
The owner and reviewer reconcile the private REVIEW entry with a minimal
protected authorization record and the action's protected environment approval.
CI must verify the record against the active run and target, including the
reviewed evidence and previous gate results. A dispatch input may point to
that record. Gates must evaluate the applicable grant at the time of each
stage, so grants for digests, tag objects or draft IDs can bind to the actual
objects produced during the approved run. Use required owner review, restricted deployment refs, no
self-review and no admin bypass for write jobs. Pre-create and audit the
environments, since an implicitly created environment has no protections.
Keep ordinary candidate verification read-only and avoid persisted checkout
credentials. [GitHub environment protections](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments)

| Owner authorization per operation | Publication scope and permissions |
| --- | --- |
| Enable release workflow or modify its gates, repository policy, package visibility or security report settings | Owner changes reviewed settings or merges the reviewed enablement change. Record activation separately from permission to write release objects. Package visibility changes are separately recorded before use. |
| Dispatch the exact hosted release workflow | Authorize the run identity and $0 budget. Default token is `contents: read`. This authorizes ordinary scrubbed run logs, without any release write. |
| Upload Actions candidate transport | Authorize the named channel and sealed candidate payload. Use GitHub's per-run artifact mechanism with short retention. Exclude credentials and private keys. |
| Publish release evidence to Actions artifacts | Authorize the named evidence and upload scope separately from candidate transport, GHCR and release assets. Keep verification read-only apart from the approved upload. Exclude credentials and private keys. |
| Push images/aliases to GHCR | Dedicated registry environment and `packages: write` with `contents: read`. Allowlist the two package names and sealed digests. A pre-push scan disposition is required because registry publication is itself external. |
| Publish provenance attestations | Dedicated provenance environment, `contents: read`, `id-token: write`, `attestations: write`. GitHub attestation store only. Any optional OCI registry attestation/referrer write needs a separate registry authorization and permission. |
| Create the annotated tag | Separate authorization for the exact unused tag and reviewed SHA. Verify peeled identity, conflicts and the pre-tag evidence first. |
| Push the annotated tag | Separate authorization for the exact tag object and remote ref. Use `contents: write` only in the protected tag job, enabled after both tag approvals. No force, overwrite or implicit tag creation by a release tool. |
| Create a GitHub draft release | Separate authorization with `contents: write` in the draft environment. Bind to the existing verified tag. |
| Upload or replace GitHub release assets | Separate authorization for the draft ID and named SHA-256 asset inventory. Use isolated `contents: write` in the asset environment. Verify the uploaded bytes. A later replacement needs its own authorization. |
| Publish or edit a GitHub release | Separate authorization for the exact draft ID, tag, notes and attachments. Use isolated `contents: write` in the publication environment. This covers final visibility and notes, with claims subject to readback. Any later edit, withdrawal or destructive cleanup needs its own recorded permission. |
| Publish the public J8 result or claim-map update | Separate authorization for the reviewed public write, including a repository push with J8 release claims or evidence. Update the tracked public claim map from reviewed, run-bound release evidence. |

No shared workflow PAT, standing package write grant, global write permissions
or credentials in assets. At each mutation boundary recheck source, tag,
digests, sealed artifacts, approval scope and applicable prior verdicts. A
draft stays a draft when a gate is missing. If an earlier outward step has
already produced immutable objects when a later gate fails, retain their exact
identities and the blocked status in the review record. Stop the remaining
writes pending disposition and any separately authorized correction.

## Budget, verdicts and retained evidence

Budget is **$0**, one candidate sequence on public `ubuntu-24.04`
GitHub-hosted Actions. Use fixture providers and no paid infrastructure. Cap
the sequence at **170 runner-minutes** including three 15-minute quickstart
jobs. Other job caps are admission 10, build and pre-push scans 30, registry
10, provenance 10, digest recheck and HTTP contract 20, pre-release replay 10,
tag 5, draft 5, asset upload 10, publish 5 and public readback/replay 10
minutes. Preserve staging diagnostics, remaining time and teardown evidence.
No automatic retry or replacement. A rerun requires reviewed disposition and
fresh authorization for every repeated outward operation.

- **Pass.** Require all prepublication tier, exact-commit CI, inventory, digest,
  SBOM, scan, provenance, three valid quickstarts, API/seeded-control and
  replay gates before the applicable tag or release mutation. The release is
  accepted only after public readback matches the tag, images and assets and
  passes anonymous-fetch offline replay. Publish the accepted scope after
  review and owner authorization.
- **Fail.** A witnessed contract mismatch, security gate failure, semantic
  negative surviving verification, cutoff miss under valid conditions or
  post-publication mismatch fails the relevant gate. Preserve counts and
  diagnostics. Hold further publication or acceptance claims.
- **Invalid.** Missing approvals, unqualified runner or cold-start conditions,
  bad tool/database inputs, incomplete evidence, intake/setup interruptions
  or unverified cleanup invalidate the affected run or stage. List unstarted
  checks as unperformed. Preserve partial evidence for review and withhold
  the success claim.

Retain the tag and peeled SHA, source/workflow digests, run/attempt IDs, reviewed
tier/claim map, approval references, exact image identities, SBOMs, scans and
dispositions, provenance bundles and control verdicts, cold clocks/profile and
schema observations, OpenAPI/client hashes and replay manifests. Record results
under `experiments/portfolio/release-verification/<release-tag>/` after review.
Mark unpublished stages and missing evidence explicitly. Keep fixtures,
claims and time outside the K ledger. This slice registers the test and its
gates; the hosted results are pending.
