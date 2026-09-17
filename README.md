# Distributed Durable Execution Engine

This repository has completed M0 through M2 and has implemented the M3 Kafka
transport/reconciliation slice, pending Claude review. It provides a
reproducible Go/Python development environment, frozen foundation contracts,
deterministic named-boundary fault fixtures, and a real local PostgreSQL/Kafka
topology. PostgreSQL remains authoritative; the transactional outbox, Kafka
relay, inbox/offset consumer, scheduler wake-ups, and bounded reconciliation
paths are covered by focused integration tests. The complete workflow engine,
external-effect ledger, and end-to-end correctness campaign remain future work.

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
race checks and `-WithServices` for real dependency checks. No model or paid
provider calls are made by these checks.

See [docs/RUNBOOK.md](docs/RUNBOOK.md) for operations and
[docs/BUILD_LOG.md](docs/BUILD_LOG.md) for evidence and known gaps.
