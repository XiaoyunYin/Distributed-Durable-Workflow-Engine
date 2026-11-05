# K2c checker, witness and chosen-workload prerequisites

Base d243aa849ad23255bd70c5c4750106a3e6dafb20, branch codex/k8s-k2.
PROVISIONAL, uncommitted; Claude commits. K2 remains IN_PROGRESS.

The implementation uses one read-only repeatable-read engine/effect snapshot,
uncapped history and transport exports, snapshot counts/hashes, accepted-key
reconciliation and explicit non-terminal/coverage reporting. A 1001-row fixture
is fully checked; removing the tail or corrupting row 1001 fails. A missing
accepted workflow fails. The checker does not call production transition
validators to decide these properties.

Host-backed fsynced JSONL files are the external invocation ledger. They have
unique session/invocation IDs and workflow/node/iteration/attempt/token/pod/time
identity. The worker's separately collected live inventory attests byte hashes,
counts and write errors. The checker joins starts with claims, successful node
results and distinct committed result-history receipts. Duplicate executions
and winning-results-per-executed-iteration are separate metrics. Missing or
truncated files/exports, missing inventory/claim/receipt, unsuccessful or
unfinished prerequisite work fail. PURE duplicate effects are NOT COVERED.
This host storage is independent of engine state, not independent-host storage
or proof against compromised worker instrumentation. Dead sessions and
unfinished intervals need additional observed-exit evidence in later fault arms;
the K2 no-fault prerequisite refuses to infer completeness from them.

The CPU fixture is optional in both registries and contains SHA-256 work only.
Units (1..1000000, default 10000) bound deterministic work; K3 calibration is
pending. The new generator uses an absolute monotonic schedule, bounded HTTP
concurrency, no response-driven pacing and no catch-up burst. Every arrival has
an immutable key, schedule, send/finish evidence, exact request/response and
expected application result; misses/rejections/uncertainty remain visible.

The separate Compose override leaves default compose.yaml unchanged. The driver
uses isolated projects, databases, broker volumes and host witness directories,
with fixed 1/s arrivals, concurrency 16, seeds 530001..530004, ten 80/10/10 arrivals
at each sleep duration (100/5000/45000 ms), and three CPU arrivals. It waits for
terminal work and 30 s witness quiet, exports/checks live and offline evidence,
and requires its incomplete-witness negative control to fail. It then explicitly
drops its database, removes its project resources, verifies absence and records
PASS/FAIL, including cleanup errors. Failures are retained without automatic
reruns. Source/image/container identities and all submission/evidence bytes are
retained under compose-prerequisites/ by the actual run.

Claude's exact pending command, from the repository root with Docker and
PowerShell 7 available, is:

```powershell
pwsh -File scripts/dur053-kubernetes.ps1 -Context compose -Scenario prerequisites
```

The same command is registered in a separate hosted CI job. No kind scenario
is implemented or run here. R223's chosen-workload Compose proof is PENDING
Claude's execution; the deferred DUR-050 allowlist gap remains OPEN. No measured
Compose, Kubernetes, fault, CPU-calibration, SLO or throughput result is claimed
by this provisional implementation handoff. Local check outcomes and cleanup
are recorded separately in validation-k2c.json and cleanup-k2c.json.
