# J8 verifiable release registration

Slice 0 incorporates Claude's Round 687 review and is pending re-review.
The registration base is `bd5664f660a4f23a1b239edc4bd484c6ea1c54d8`. J8 uses
the scoped DUR-054 pull-forward in PLAN §18 and the D037 B4 amendment. D036 preserves the release
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

**Full v1 is the eligible candidate tier.** Main-checkout `PLAN.md:426–434`
enumerates all three tiers. `docs/DECISIONS.md:1645,1673–1681` lifts the old
publication order for J8, and `PLAN.md:3866–3871` retains each dependency for
claims relying on it. Rows T1–T7 cover the tier criteria using completed,
separately reviewed evidence. Gate the tag on Claude's review of the exact
release SHA, its reproduction package and its claims.

| Row | Tier requirement | Reviewed evidence and scope |
| --- | --- | --- |
| T1 | Core Engine MVP deployment, transport and bounded throughput smoke | M5/DUR-025, `REVIEW.md:1012` (Round 23 at `acb28ba`), covers the [real dependency outages](../../m5/outage-recovery.json) and [four-workflow two-owner smoke](../../../internal/engine/m5_smoke_test.go). `PLAN.md:1593–1606` and `REVIEW.md:1844` (DUR-032, Round 49) record the two-scheduler deployed Python/Kafka validation. The separately reviewed [DUR-048 pilot](../../m8/dur042-pilot/) covers a pure deployed path on one host (`REVIEW.md:11981`, Round 75). |
| T2 | Core Engine MVP durable correctness | The [M5 case manifest and checker evidence](../../m5/README.md#L3-L12) cover 48 F01–F11 cases, including retries, checkpoints, fencing, both effect classes and approval boundaries. `REVIEW.md:1880` (Round 50) verified the corrected snapshots at `0002e75`. The [approval and effect integration fixture](../../../internal/incident/production_integration_test.go) is DUR-033A (`REVIEW.md:1718`, Round 45), with a synthetic incident and a sandbox action. |
| T3 | Portfolio MVP deterministic incident, tool and retrieval paths | `PLAN.md:1244–1259` and [M6's fixture and benchmark scope](../../m6/README.md#L3-L26) record the deterministic workflow, frozen gates and separate retrieval benchmark accepted in Round 26 at `1048ad0`. [DUR-055's PostgreSQL FTS/pgvector and official SDK MCP integration](../../m8/dur055-applied-ai-upgrade/README.md#L122-L131) supplies the real retrieval and tool evidence. `REVIEW.md:23411–23433,23470–23471` records DUR-055 acceptance (Round 132 at `f18b210`). Preserve the distinction between M6's SQLite continuity fixture and the PostgreSQL integration. |
| T4 | Portfolio MVP approval walkthrough, F12, adversarial controls and observability | `REVIEW.md:1110` (Round 26) covers [continuity and F12](../../m6/continuity.json), [adversarial/redaction](../../m6/adversarial.json), [citation checks](../../m6/citation-check.json) and the [M6 model-behavior dashboard](../../m6/dashboard.json). Use the deterministic approval walkthrough and the separate DUR-033A integration from T2. |
| T5 | Full v1 frozen engine studies | `PLAN.md:1350–1435` records the separately reviewed M7 [scheduler throughput](../../m7/dur026/results.json), [safeguard cost](../../m7/dur034/results.json), [dispatch decomposition](../../m7/dur035/results.json), [lease recovery](../../m7/dur027/results.json) and [checkpoint tradeoff](../../m7/dur028/results.json). Preserve the unresolved safeguard effect, unresolved dispatch increment and historical topology limits. |
| T6 | Full v1 retrieval and bounded live/adversarial studies | `PLAN.md:1444–1465` and `REVIEW.md:1685` (DUR-029, Round 44 at `5b9d65c`) cover [development-frozen held-out retrieval](../../m7/dur029/retrieval-final.json), [live retrieval with the document-dependent subset](../../m7/dur029/live-evaluation.json) and the [defended-versus-plain ablation](../../m7/dur029/live-adversarial.json). Use the recorded model, workload and sample limits on all three arms. |
| T7 | Full v1 report and interview evidence | `PLAN.md:1525–1557` records the DUR-030 report (`docs/TECHNICAL_REPORT.md:77–324`, Round 46). `PLAN.md:1559–1585` records the DUR-031 Codex walkthroughs under D019 (Round 47). `PLAN.md:1593–1609` records the DUR-032 reproduction review (Rounds 49–51 in `REVIEW.md:1844–1916`). Tracked reader entry points are [DESIGN.md](../../../DESIGN.md), the [public claim map](../recruiter/claim-evidence.md) and the [failure case study](../recruiter/failure-case-study.md). The historical report and walkthroughs supply the tier evidence. |

| Retained prerequisite for dependent claims | Evidence boundary for this release |
| --- | --- |
| Full DUR-045 refresh | `PLAN.md:2059–2067` distinguishes the accepted pre-refresh package from the pending D026 refresh. T7 uses the older DUR-030/031/032 closeouts. The public claim map is a reader index. The tier decision uses the original study artifacts and closeouts in T1–T7. |
| Accepted DUR-050 | `PLAN.md:2351–2359` identifies the deferred AWS campaign. T1 and T5 use local deployment and M7 records. The HTTP admission controls below witness the shipped HTTP branches with disposable fixtures. Their scope is response compatibility. |
| Accepted DUR-052 | `PLAN.md:2625–2635` identifies the deferred operations dashboard and oldest-work alert. T4 uses M6's historical model-behavior dashboard, which C1 links through the historical evidence package. |
| Accepted DUR-053 | `PLAN.md:2677–2685` identifies the Kubernetes campaign and K8 transfer. J8 release tests use Compose. The separate J12 hosted TokenReview result covers J12's identity controls. T1–T7 and the release-note inventory use no K results. |

Before any outward publication stage, bind the tier, claim map, scan and workflow policy,
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

### Planned release-note claims

This is the complete claim inventory. Historical results retain their source
scope. Each release result needs reviewed evidence bound to the exact
candidate; include measured values only after the corresponding gate passes.

| Claim | Planned release-note statement | Required evidence row and scope |
| --- | --- | --- |
| C1 | The release meets the Full v1 criteria and links the historical evidence package. | T1–T7 plus the exact-commit review and DUR-046 receipt. The notes link the studies and their limits without asserting new performance, model or recovery results. |
| C2 | The reviewed source tag identifies the stock Linux/amd64 runtime and worker images by GHCR digest. Digest-bound SBOMs, scans with dispositions and GitHub build provenance are available. | [Release image checks](#release-images-modes-and-migration-inventory), [supply-chain checks](#supply-chain-policy) and public readback. Require tag resolution, matching image and report digests and the substituted-digest rejection. The scan statement is scoped to its frozen database and policy. |
| C3 | The release profile enables J12 tenant auth and namespace authorisation, the J19 expired-attempt policy and J9a repaired relay, with the registered migration inventory. | [Release profile and migration table](#release-images-modes-and-migration-inventory), exact running mode and ledger evidence, J19 Round 639 and the reviewed J12 hosted qualification. Cite J12's hosted run for real Kubernetes TokenReview evidence. |
| C4 | The documented Compose quickstart reaches an independently checked terminal workflow in three cold Linux runs. The notes report pull and start-to-check times and spreads. | [B4 quickstart clocks and cutoff table](#b4-cold-release-quickstart), three reconciled checker, digest and timing records. Label worker identity in every quickstart result **Compose-only TokenReview fixture**. |
| C5 | The OpenAPI document matches the shipped authenticated HTTP contract. The frozen J8 client detected the three registered breaking changes and accepted the additive change. | [HTTP route, response and change tables](#shipped-http-contract-and-seeded-controls). Include namespace and idempotency checks, and HTTP 429 only with its registered admission configuration. Cross-release compatibility remains a J13 claim. |
| C6 | The checksummed public replay asset reproduces the registered historical F01 verdict offline and rejects the failing control. | [Offline replay checks](#offline-replay-and-public-repository-controls) and public readback receipts for both commands in the isolated Linux network namespace. Scope to the shipped bundle and verifier. |
| C7 | The repository supplies private vulnerability-reporting instructions and dependency-update configuration for the registered ecosystems. | [Public repository controls](#offline-replay-and-public-repository-controls) and the reporting-route and manifest coverage checks. |

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
The private worker setup uses a separate TLS Compose-only TokenReview fixture
service, trust root, issuer, signing key, reviewer credential and bound caller
token for a dedicated worker audience. The fixture verifies the signature,
issuer, audience, expiry and expected service account and returns the fixture
pod identity. Mount the worker token through the existing `WORKER_TOKEN_FILE`
seam. Verify both a valid worker credential and a denied tenant token on the
worker listener. Label it **Compose-only TokenReview fixture** in the
quickstart docs and every artifact that references it, including transcripts,
evidence manifests and release notes. J12's separate hosted qualification is
the sole source for real Kubernetes TokenReview evidence.

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
workflow directory, with every job disabled and each step deliberately
failing. Claude reviews its implementation and the protected workflow and
environment setup before activation.

Request **one owner grant** covering exactly the three outward publication
stages in the table below. Claude records that grant in main-checkout
`REVIEW.md` and makes a matching record available to the protected workflow.
Bind it to the repository, exact candidate SHA and workflow SHA, release
tag name, the GHCR destinations and image digests or their derivation rule,
and the release asset inventory. The inventory fixes each asset name and its
expected source in the reviewed workflow. Bind known hashes at grant time.
For outputs such as the same-run provenance bundles, bind the producer and
output in the inventory and verify the resulting SHA-256 in the sealed asset
manifest before publication. When the grant uses a digest derivation rule,
derive the image digests from the sealed Linux/amd64 runtime and worker OCI
manifests built by the reviewed workflow from that candidate and its pinned
inputs. Keep the candidate and artifact identities tied to the same run.

An optional dispatch input identifies the protected record. Immediately before
writing, each outward job loads the same Claude-recorded grant, compares the
running workflow SHA, sealed candidate SHA and tag to the grant, and verifies
the actual object and applicable preceding evidence. A missing record,
mismatch or missing evidence fails the job and stops subsequent publication.
Pre-create and audit the protected environments, restrict deployment refs
and keep credential scopes job-local. Require Claude review for internal
gates without another owner approval prompt. [GitHub environment protections](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments)

| Stage | Handling and permissions |
| --- | --- |
| **GHCR image or attestation push** | **Covered by the one owner grant.** Compare the actual OCI manifest digests, destinations and scan dispositions to the grant and its sealed build record before writing. Apply the same check to subject and destination for any GHCR attestation push. Use the protected GHCR job with `contents: read` and `packages: write`. The registered provenance job uses the GitHub attestation store. |
| **Annotated tag push** | **Covered by the same owner grant.** Create the annotated tag locally under Claude review. Before pushing, inspect the actual object, annotated tag name and target. Compare the name and candidate SHA to the grant, join the exact object SHA to the reviewed local creation receipt and verify the remote ref is unused. Push that object with `contents: write` in the protected tag job. Force, replacement and implicit release-tool tag creation are prohibited. |
| **GitHub release publication with assets** | **Covered by the same owner grant.** Derive the expected draft ID from the reviewed same-run creation receipt for the granted repository and tag. Fetch that exact draft ID from GitHub and verify draft state, tag and peeled commit, reviewed notes, and the attached assets against the grant inventory and sealed checksums. Publish that draft with `contents: write` in the protected publication job. |
| Workflow dispatch, scrubbed logs and per-run Actions artifacts | Internal CI under Claude review. Default token is `contents: read`. Use the per-run artifact mechanism with short retention for sealed candidate transport and scrubbed evidence. Exclude credentials and private keys. |
| GitHub attestation-store provenance | Internal under Claude review. Verify same-run digest binding. Limit `contents: read`, `id-token: write` and `attestations: write` to the provenance job. Keep `push-to-registry: false`. |
| Draft creation and asset staging on an unpublished GitHub release | Internal under Claude review. Use the existing verified tag, seal the returned draft ID with the run and tag, and stage reviewed assets on that unpublished draft. Scope `contents: write` to the separate draft and asset jobs. Verify that the draft is still unpublished and the upload matches the reviewed inventory. |
| Public claim-map update | Ordinary reviewed commit to the tracked claim map using the reviewed, run-bound results for C1–C7. |

Avoid shared workflow PATs, standing write grants, global write permissions
and persisted checkout credentials. If a later gate stops, preserve the
identities of any earlier writes and the blocked status in `REVIEW.md`.
Keep any unpublished draft in draft state. Return publication changes outside
the recorded grant to the owner as one revised release grant.

## Budget, verdicts and retained evidence

Budget is **$0**, one candidate sequence on public `ubuntu-24.04`
GitHub-hosted Actions. Use fixture providers and no paid infrastructure. Cap
the sequence at **170 runner-minutes** including three 15-minute quickstart
jobs. Other job caps are admission 10, build and pre-push scans 30, registry
10, provenance 10, digest recheck and HTTP contract 20, pre-release replay 10,
tag 5, draft 5, asset upload 10, publish 5 and public readback/replay 10
minutes. Preserve staging diagnostics, remaining time and teardown evidence.
No automatic retry or replacement. Claude reviews rerun disposition. A changed
publication scope requires one revised owner grant.

- **Pass.** Require all prepublication tier, exact-commit CI, inventory, digest,
  SBOM, scan, provenance, three valid quickstarts, API/seeded-control and
  replay gates before the applicable tag or release mutation. The release is
  accepted only after public readback matches the tag, images and assets and
  passes anonymous-fetch offline replay. Update the public claim map through
  an ordinary reviewed commit.
- **Fail.** A witnessed contract mismatch, security gate failure, semantic
  negative surviving verification, cutoff miss under valid conditions or
  post-publication mismatch fails the relevant gate. Preserve counts and
  diagnostics. Hold further publication or acceptance claims.
- **Invalid.** A missing or mismatched grant at an outward gate, unqualified
  runner or cold-start conditions,
  bad tool/database inputs, incomplete evidence, intake/setup interruptions
  or unverified cleanup invalidate the affected run or stage. List unstarted
  checks as unperformed. Preserve partial evidence for review and withhold
  the success claim.

Retain the tag and peeled SHA, source/workflow digests, run/attempt IDs, reviewed
tier/claim map, owner grant reference, exact image identities, SBOMs, scans and
dispositions, provenance bundles and control verdicts, cold clocks/profile and
schema observations, OpenAPI/client hashes and replay manifests. Record results
under `experiments/portfolio/release-verification/<release-tag>/` after review.
Mark unpublished stages and missing evidence explicitly. Keep fixtures,
claims and time outside the K ledger. This slice registers the test and its
gates; the hosted results are pending.
