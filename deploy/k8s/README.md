# DUR-053 K3a kind package

Kustomize is the only package renderer. `base/resources.yaml` uses JSON syntax,
a YAML subset, so the independent offline policy tests need no YAML dependency.
Both overlays have identical roles and image digests; kind-baseline changes
only the registered lifecycle flags and the namespace-dependent references.

The source has `UNBOUND` image markers and documentation-only TEST-NET /32
addresses. These deliberately cannot pass the deployment policy. The driver
builds candidate images, obtains registry manifest digests, copies the package
to scratch, binds images and inspected dependency addresses, then renders and
validates it. It never rewrites the checkout. Images are preloaded into kind's
containerd by digest; all application containers use `imagePullPolicy: Never`.

From a clean checkout, the two PR jobs invoke:

```powershell
./scripts/dur053-kubernetes.ps1 -Context kind -Scenario package-check -OutputRoot (Join-Path $env:RUNNER_TEMP "dur053-k3-package")
./scripts/dur053-kubernetes.ps1 -Context kind -Scenario smoke -OutputRoot (Join-Path $env:RUNNER_TEMP "dur053-k3-smoke")
```

The same entry point supports cleanup. Pilot and every measurement scenario
throw `K3b`. The dispatch-only campaign workflow is a fail-closed stub; it
checks the committed ledger and starts no live resources. No SLO is frozen.
Docker, kind and PowerShell execution are CI-only for this task.

The tool lock contains release URLs, checksums, image digests and source schema
provenance. SHA-pinned actions are used only in new jobs; existing jobs and
Compose files are preserved. Hosted Ubuntu's exact image version and observed
CPU/RAM are recorded per job because the hosted VM image itself is not an
immutable workflow input. Calico enforces NetworkPolicy; kind's default CNI is
disabled. KEDA runs in its own namespace and uses a namespace-qualified demand
URL and a fresh per-namespace bearer Secret. No Secret or kubeconfig is exported.

Role pools are checked against [connection-budget.json](connection-budget.json):
public API 12, operator 4, worker-control 12, scheduler/ingestor 18, relay including
LISTEN 8, demand 4, four two-connection Jobs 8, future clients 8 = 74; add eight
administrative/reserved connections, leaving 18 of PostgreSQL's 100 connections.
The observer uses HTTP and runs the load generator and checker sequentially,
within their separately reserved Job budgets. Runtime role overrides are opt-in;
absent overrides retain Compose's eight-connection default.

Migrate and compatibility Jobs run before any public submission Deployment or
client exists. The compatibility Job exports the actual candidate worker registry
in its initContainer and checks it using the actual candidate runtime image and
a two-connection read-only pool. K3a installs fresh namespaces only; in-place
upgrades/rollback and surviving-old-image admission remain later campaign work.

Smoke runs ten short 80/10/10 arrivals, terminal/30-second witness-quiet closeout,
live and offline independent checking, and a matched legacy batch in a fresh
namespace/database after treatment teardown. It records PostgreSQL-outage and
SIGTERM readiness, unchanged outage liveness, network denial and independent
401 auth refusals from real workers, and successful authenticated KEDA polling.
Temporary probe NetworkPolicies are removed. Smoke prevents replica changes while
exporting the no-fault witness; this is not a scaling or fallback measurement.
The host-backed witness PVC is outside the engine database and is exported before
namespace deletion; no replacement/dead-worker witness completeness is claimed.
PDBs apply to evictions, not Deployment rollout or autoscaler scale-down.

Outputs must be fresh and outside the checkout. A PASS requires verified database
and resource cleanup. Failure diagnostics and available snapshot/witness bytes
are retained. Output hashes exclude manifest.json to avoid self-hashing. The
shared kind network is removed when created by this invocation; pre-existing
platform networks are retained. The cumulative ledger refuses the next live job
until preceding job evidence is committed; no failed run is automatically rerun.

Claude's reproducible evidence intake after `gh run download <run-id>` is:

```powershell
python scripts/dur053_record_smoke.py --downloaded <downloaded-artifact-directory>
```

It verifies every artifact hash, refuses duplicate runs/jobs or an existing
destination, copies byte-exact evidence to
`experiments/portfolio/k8s-recovery/k3-smoke-v1/<run-id>/`, and appends the charge
with run/job/head/merge-ref/tree identity to the ledger. Evidence and ledger are
committed together. An interrupted job without a trustworthy artifact is charged
its full registered reservation; missing evidence never restores budget. Before
later runs the driver verifies committed manifest hashes and provenance. A
refusal before resources start receives a conservative minimum one-second charge.
The 72-hour cap includes repeated smoke/failed/invalid runs across PRs.

The schedule is a planning estimate only: 438 episodes, 15 campaign jobs,
224983.5 seconds including one smoke reservation. See protocol-v2 Amendment B;
K3b must recompute reservations from actual pilot b/r/L and refuse oversized
individual runs. All claims remain single-host lifecycle/correctness evidence,
with shared-runner noise, never capacity or HA.
