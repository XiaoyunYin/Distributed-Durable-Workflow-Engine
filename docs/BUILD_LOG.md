# Build log

## 2026-09-14 - DUR-001 repository and reproducible toolchain

- Base commit: `d722cf7` (planning baseline)
- Target commit: `87e123a`
- Task status: READY_FOR_REVIEW

Built the initial Go/Python repository layout, project-level toolchain pins,
Python lock configuration, version-pinned Docker Compose topology, local secret
generation, migration entry point, health endpoints, validation scripts, and
operations documentation. This foundation exists so later durability work can
be tested against real PostgreSQL and Kafka from the first implementation task.

Selected a single-node local PostgreSQL/Kafka topology to keep development
reproduction bounded. Mocks were rejected because they cannot satisfy DUR-001's
real-dependency health and restart checks. Host-wide Go replacement was avoided;
the module's `toolchain` directive obtains the exact Go version per project.

Validation run on a Windows host with Docker Desktop's Linux engine:

- `scripts/bootstrap.ps1`: PASS. Downloaded project Go 1.27.1, created the
  CPython 3.12.7 `.venv`, and installed the exact `uv.lock` set (including
  Ruff 0.16.7, mypy 2.3.1, and pytest 9.1.1).
- `scripts/check.ps1`: PASS. Go format/vet/test/build, Ruff lint/format,
  strict mypy, and pytest all passed; Go had one package test and pytest had
  one import test.
- `scripts/smoke.ps1`: PASS. PostgreSQL 18.6 and Kafka 4.3.1 responded,
  required topics existed, two runtimes/two workers/collector/Prometheus were
  healthy, both runtimes were scraped, and PostgreSQL reported data checksums,
  `fsync`, `synchronous_commit`, and `full_page_writes` on.
- `scripts/restart-smoke.ps1`: PASS. A PostgreSQL marker row and Kafka marker
  topic survived forced container recreation with named volumes; temporary
  evidence was removed after verification.
- `docker compose ... logs kafka | Select-String /var/lib/kafka/data`: PASS.
  Broker recovery logs identified the named-volume path.

The first service smoke attempt timed out immediately after the initial image
pull despite Docker health having just succeeded; every endpoint returned HTTP
200 on inspection. The script now retries each named URL and reports the URL
and final error. A more important inspection found Kafka's initial logs under
`/tmp/kafka-logs`; setting `KAFKA_LOG_DIRS=/var/lib/kafka/data` corrected
the persistence boundary before the passing container-recreation test.

Remaining gap: the clean-checkout bootstrap will be rerun from the committed
target before review handoff. No durable-execution correctness or performance
result is claimed.

Interview explanation: this task separates repeatable infrastructure evidence
from future engine claims. Pinning versions and testing retained volumes makes
later crash/recovery failures attributable to code and protocol changes instead
of an undocumented local setup.

## 2026-09-16 - M0 foundation completion pass

- Base commit: `198fd4b` (DUR-001 scaffold); target implementation commit: `87e123a`.
- Task status: READY_FOR_REVIEW after the implementation commit.

Completed the remaining M0 foundation work. `docs/CONTRACTS.md` freezes actor
authority, identity separation, transaction boundaries, transition permissions,
the required race traces, and the declared failure-model limits. The stable
workflow partition map is versioned as `sha256-u64-be-v1` with 16 partitions;
Go and Python independently implement and test the same canonical vectors.

Added an event-driven DUR-003 fault fixture. A target reports a named boundary
and waits for an explicit release command; the controller can release or kill
only after that event, writes `fault-trace.v1` JSONL evidence, and records a
bounded timeout when the boundary is not reported. Tests cover release, kill,
timeout, and same-seed trace stability.

Added `scripts/ci.ps1` as the shared validation entry point. Go race checks and
real PostgreSQL/Kafka smoke checks are explicit opt-ins; model and paid-provider
checks are not invoked. This keeps unavailable or intentionally out-of-scope
checks visible instead of treating them as passes.

Validation on 2026-09-16:

- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/bootstrap.ps1 -StartServices`: PASS. Go 1.27.1, locked uv environment, Docker image builds, migration, and service smoke checks completed.
- `scripts/check.ps1` with repository-local `GOCACHE`, `UV_CACHE_DIR`, `TEMP`, `TMP`, and `PYTEST_ADDOPTS`: PASS. Go vet/test/build, Ruff, strict mypy, and 7 pytest tests passed.
- `scripts/restart-smoke.ps1`: PASS. PostgreSQL marker row and Kafka marker topic survived forced container recreation.
- `python -m faults.control --seed 23 --boundary after-effect --action release`: PASS.
- Timeout CLI run with `--skip-boundary --timeout-seconds 0.1`: expected non-zero timeout result and trace recorded.

The first post-change check attempts exposed pre-existing host cache/temporary
directory collisions under the user's profile; those runs were not counted as
project failures. Validation was repeated with task-local cache locations. The
pytest cache warning for the existing `.pytest_cache` ACL remained non-fatal.
No durable-execution correctness or performance result is claimed by M0.

Interview explanation: freeze the protocol before adding concurrency, then
make crash tests wait on named observed boundaries. The controller proves that
the fault was injected at the intended point; the later engine checker can
consume the same versioned trace without guessing from elapsed time.
