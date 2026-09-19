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
personal walkthroughs are reviewed and closed. DUR-032 remains for final
reproduction/release validation. The results remain bounded to their
declared fixtures and host, not to production scale or multi-host durability.
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
- Two foundation Go runtime processes and two foundation Python worker processes.
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
