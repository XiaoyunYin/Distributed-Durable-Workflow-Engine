# Portfolio recovery evidence

This directory contains supplemental hiring-facing evidence. Existing M7
artifacts remain authoritative for their original task scopes; this directory
does not replace or rewrite them.

## DUR-041a local recovery

Run the scoped wrapper from a dedicated local Compose/database environment:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/portfolio-local-recovery.ps1 -StartServices
```

The wrapper reuses the reviewed DUR-027 process fixtures and writes a new
campaign directory under `experiments/portfolio/local-recovery/`. It promotes
only two local fault families:

- owner reconnects after takeover and attempts stale-epoch operations;
- owner process self-exits at an acknowledged boundary and the peer recovers
  useful affected work.

The wrapper requires confirmed target death, takeover, useful progress, fenced
stale-owner writes, and clean source-harness reconciliation. It reports
median/min/max for takeover and useful-progress delays, with counts and
denominators.

R096 is intentionally deferred under decision D016 and defined in the
[public review status](../../docs/REVIEW_STATUS.md#current-status). The
lock-held takeover case is excluded from the promoted DUR-041a result. The
source harness contains a separate lock-contention probe, but its number is not
treated as proof of a bounded lock wait or scheduler liveness. The result is local Docker
Desktop/WSL2 process evidence, not multi-host or database-host durability.

Generated campaign directories contain `protocol.json`, `pilot.json`,
`dur027-source.json`, and `summary.json`. Review the source artifact and its
independent checker output before quoting any result.

## Recorded run

The committed seed `410041` run is available at
[`local-410041/summary.json`](local-recovery/local-410041/summary.json), with
the full source episodes in
[`local-410041/dur027-source.json`](local-recovery/local-410041/dur027-source.json)
and scope metadata in
[`local-410041/protocol.json`](local-recovery/local-410041/protocol.json).
It reports 60 in-scope episodes, 180 fenced stale-owner writes, and zero false
takeovers. Those numbers are local process evidence only; the lock-held
takeover case remains R096 and is excluded.
