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

`docker compose --env-file .env -f deploy/local/compose.yaml down` removes
containers and the network but retains named volumes. Resume with
`docker compose --env-file .env -f deploy/local/compose.yaml up -d --wait`.

For a dependency-only container recreation and persistence check, run
`pwsh ./scripts/restart-smoke.ps1`.

## Reset warning

Adding `--volumes` to `docker compose down` erases PostgreSQL, Kafka, and
Prometheus local data. This cannot be recovered unless it was backed up.

## Troubleshooting

- Port collision: change the matching host port in `.env`; container ports stay fixed.
- Changed database credentials: existing PostgreSQL volumes retain their original credentials. Reset the volume only if data loss is acceptable.
- Failed Kafka initialization: inspect `kafka` and `kafka-init` logs, then rerun `docker compose ... up -d --wait`.
- Toolchain download disabled: set `GOTOOLCHAIN=auto` or install Go 1.27.1 manually.
- Docker Desktop/WSL2 results are development-only; final I/O-sensitive measurements require the Linux host gate in DUR-036.
