# Distributed Durable Execution Engine

This repository has completed M0 through M7 implementation and measurement
tasks. M6 was reviewed with
`NO_BLOCKING_FINDINGS` at committed target `1048ad0` (base `db8b462`), and
DUR-019, DUR-020, DUR-021B, and DUR-033 are closed. M5 was reviewed with
`NO_BLOCKING_FINDINGS` at committed target `acb28ba` against round-22 target
`69917db`; R057 remains a nonblocking P3 evidence-labelling limitation.
M7's bounded studies and DUR-033A production-path integration are reviewed and
closed; their evidence is summarized in [the technical report](docs/TECHNICAL_REPORT.md).
M8/DUR-030 is reviewed and closed; DUR-031's interview-evidence pack and three
Codex-performed walkthroughs (delegated by the user) have reviewed engineering
evidence, but DUR-031's personal user exercises remain outstanding (R092).
They do not establish the user's personal fluency; R092 remains an open P3
under DUR-031. DUR-032 is reviewed and closed for reproduction/release
validation, but release/tag creation remains explicitly authorization-gated.
The results remain bounded to their
declared fixtures and host, not to production scale or multi-host durability.

## Three bounded comparative findings

The M7 measurements below predate the deployed scheduler/Kafka-worker wiring
and have not been rerun on it: the numbers describe each study's named harness,
not the current deployment. A deployed rerun would be a separate campaign.

- DUR-026: with a fixed four-process worker pool, two schedulers kept up with
  the tested 2/s offered rate where one scheduler did not on the engine path.
- DUR-035: direct notification was faster than Kafka at the resolved terminal
  stage in the quoted campaign; ready-to-claim direct versus Kafka was
  unresolved and varied across campaigns.
- DUR-028: at one SHA-256 work unit per chunk, every-chunk checkpointing took
  7.5552 times the boundary-only median for the recorded in-process panic
  workload. This is not a general checkpoint policy or crossover estimate.

The M5 48/48 named-fault campaign is a separate bounded validation result, not
a comparative headline. DUR-034 resolved no safeguard-cost effect because its
clean reruns were too variable. The RQ8 adversarial rates retain their counts:
2/20 defended cases versus 6/20 plain cases above each profile's clean-clean
baseline. See the [technical report](docs/TECHNICAL_REPORT.md) and
[final release checklist](docs/RELEASE_CHECKLIST.md) for evidence and limits.
It provides a
reproducible Go/Python development
environment, frozen foundation contracts, deterministic named-boundary fault
fixtures, a deterministic local-first incident workflow, and a real local
PostgreSQL/Kafka topology. PostgreSQL remains
authoritative for workflow state; the transactional outbox, Kafka relay,
inbox/offset consumer, scheduler wake-ups, retry/checkpoint recovery,
approval-gated cooperating effects, and bounded reconciliation paths are
covered by focused integration tests. M4 and M5 are reviewed and closed; M5's
controller-driven fault campaign, outage checks, and telemetry prerequisite
remain development evidence. M6's retrieval, MCP, workflow, source-corpus,
redaction, and continuity artifacts are synthetic local evidence, not live-model
quality or production agent claims. Its portable SQLite adapter exercises the
approval/effect contract seams but is not the production PostgreSQL engine.
This is not a claim of
production authentication, multi-host durability, or final performance.

## Local topology

- PostgreSQL 18.6 with data checksums and durability settings enabled.
- Apache Kafka 4.3.1 in single-node KRaft combined mode.
- Two Go scheduler/API/event-ingestor replicas and two Python Kafka worker
  processes (four consumer/activity slots each).
- OpenTelemetry Collector 0.160.0 and Prometheus 3.13.0 LTS, with both
  runtime replicas verified as Prometheus scrape targets.

All containers share one Docker Desktop Linux VM. This is development evidence,
not a storage-replication or infrastructure-availability demonstration.

## Prerequisites

- Git
- Go with automatic toolchain downloads enabled (`GOTOOLCHAIN=auto`); the module pins Go 1.27.1
- CPython 3.12.7
- uv 0.9.5 or newer
- Docker Desktop 29.7.2 or newer with Docker Compose v5
- PowerShell 7 or Windows PowerShell 5.1

## Quick start

From the repository root:

```powershell
pwsh ./scripts/bootstrap.ps1 -StartServices
pwsh ./scripts/check.ps1
pwsh ./scripts/restart-smoke.ps1
```

If `pwsh` is unavailable, run the same scripts with `powershell.exe`.
`bootstrap.ps1` creates a gitignored `.env`, installs the locked Python
development environment, builds the containers, waits for health, applies the
bootstrap migration, and runs service smoke checks.

Useful endpoints:

- runtime replicas: <http://localhost:8080/healthz> and <http://localhost:8081/healthz>
- worker processes: <http://localhost:8181/healthz> and <http://localhost:8182/healthz>
- Prometheus: <http://localhost:9090>
- PostgreSQL: `localhost:5432`
- Kafka: `localhost:9092`

When `DATABASE_URL` and `KAFKA_BOOTSTRAP_SERVERS` are set, each runtime replica
also starts the bounded transactional-outbox relay. Kafka publication is a
transport action only; workflow state remains in PostgreSQL.

Compose also enables the scheduler repair loop for the `local-runtime`
namespace. Workers consume the task topic, persist inbox disposition through
the control API, then claim and execute allowlisted `pure.echo/v1` and
`pure.add/v1` activities. Unsupported activity versions/effect activities are
paused, not executed. Approval/effect integration retains the separately
reviewed DUR-033A test scope; this wiring adds no external action capability.
Run `scripts/local-demo.ps1` after bootstrap for an API → Kafka → Python →
durable-result demo checked against the independent invariant checker.

Ports can be changed in the local `.env` file.

## Stop and resume

Stop containers while retaining PostgreSQL, Kafka, and Prometheus volumes:

```powershell
docker compose --env-file .env -f deploy/local/compose.yaml down
```

Resume them with:

```powershell
docker compose --env-file .env -f deploy/local/compose.yaml up -d --wait
```

`docker compose ... down --volumes` permanently deletes local dependency data.
Use it only when a clean reset is intended.

## Validation

`scripts/check.ps1` runs Go format/vet/test/build and Python Ruff, mypy, and
pytest checks. `scripts/smoke.ps1` verifies real service health and pinned
PostgreSQL durability settings. `scripts/restart-smoke.ps1` proves PostgreSQL
and Kafka development volumes retain explicit markers across container recreation.
`scripts/ci.ps1` is the shared validation entry point; use `-WithRace` for Go
race checks, `-WithServices` for real dependency checks, and `-WithM5` to run
the bounded two-scheduler smoke plus the isolated F01-F11 campaign. No model
or paid-provider calls are made by these checks.

See [docs/RUNBOOK.md](docs/RUNBOOK.md) for operations and
[docs/BUILD_LOG.md](docs/BUILD_LOG.md) for evidence and known gaps.
