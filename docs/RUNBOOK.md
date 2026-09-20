# Local development runbook

## Bootstrap and start

Run `pwsh ./scripts/bootstrap.ps1 -StartServices` from the repository root.
The script creates `.env` only when absent. It never overwrites existing local
configuration.

Inspect state with:

```powershell
docker compose --env-file .env -f deploy/local/compose.yaml ps
docker compose --env-file .env -f deploy/local/compose.yaml logs --tail 100
```

Apply migrations with `pwsh ./scripts/migrate.ps1`. Verify health with
`pwsh ./scripts/smoke.ps1`.

## Stop, resume, and restart

The default Compose runtime enables scheduler scanning only for namespace
`local-runtime`; it does not consume experiment/test namespaces. The worker
registry supports `pure.echo/v1` and `pure.add/v1`, with four Kafka/activity
slots per worker process. Unknown versions or effect activities pause instead
of executing. Control endpoints remain development-only and unauthenticated.

Run `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/local-demo.ps1`
to submit through the API, execute two activities in Python via Kafka, and
verify the durable result and invariant checker. The demo seeds a private
definition and removes its workflow/definition in a finally-style defer,
including on failure. It does not execute paid calls or remediation effects.

For a complete isolated release reproduction, use:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/final-reproduce.ps1 -Commit <reachable-target>
```

This creates a detached worktree and a uniquely named `dur032-check-*` Compose
project with generated credentials, separate ports, and new volumes. It runs
bootstrap, the deployed demo, serial service/race CI, restart smoke, and the
demo again. Only that test project's containers and volumes are removed. The
ignored worktree is retained for audit; `-KeepOnFailure` also retains a failed
test project's containers/volumes for diagnosis. It never tears down the
default development stack. Builds may reuse dependency/image caches; this is
a clean source/service-state test, not a cacheless installation.

`docker compose --env-file .env -f deploy/local/compose.yaml down` removes
containers and the network but retains named volumes. Resume with
`docker compose --env-file .env -f deploy/local/compose.yaml up -d --wait`.

For a dependency-only container recreation and persistence check, run
`pwsh ./scripts/restart-smoke.ps1`.

## Shared validation and fault fixtures

Run `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/ci.ps1`
for the default local checks. Add `-WithRace` for Go race checks and
`-WithServices` when PostgreSQL and Kafka are running. The named-boundary
fixture can be exercised with:

```powershell
uv run python -m faults.control --seed 23 --boundary after-effect --action release
```

Use `--skip-boundary --timeout-seconds 0.1` to verify the explicit timeout
trace. These foundation checks do not call a model or paid provider.

## Reset warning

Adding `--volumes` to `docker compose down` erases PostgreSQL, Kafka, and
Prometheus local data. This cannot be recovered unless it was backed up.

## Troubleshooting

- Port collision: change the matching host port in `.env`; container ports stay fixed.
- Changed database credentials: existing PostgreSQL volumes retain their original credentials. Reset the volume only if data loss is acceptable.
- Failed Kafka initialization: inspect `kafka` and `kafka-init` logs, then rerun `docker compose ... up -d --wait`.
- Toolchain download disabled: set `GOTOOLCHAIN=auto` or install Go 1.27.1 manually.
- Docker Desktop/WSL2 results are development-only; final I/O-sensitive measurements require the Linux host gate in DUR-036.
