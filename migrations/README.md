# Migrations

Numbered `*.up.sql` files are inspected in lexical order by
`scripts/migrate.ps1`. Each migration must record its numeric filename version
in `engine.schema_migrations`; the script skips versions already recorded and
confirms the ledger entry after applying a new file. Migration `000002` adds
the DUR-005 durable workflow state repository, including revision/history,
lease, attempt, outbox/inbox, approval, effect, and reconciliation-evidence
tables. Migration `000003` adds immutable activity effect-class metadata and
makes evidence follow workflow retention with a cascading foreign key.
Migration `000004` adds the `WORKFLOW_TIMER` purpose used by explicit graph
timer nodes; retry timers continue to use `RETRY_BACKOFF`.
Migration `000005` records explicit graph-timer completion on each node so
timer lifecycle is not inferred from retry counts.
Migration `000006` adds relay publication evidence, durable consumer offsets,
scheduler wake-up claims, reconciliation items, and the retry metadata used by
the M3 transport and recovery paths.
Migration `000007` changes event-inbox retention to cascade with its referenced
outbox event, so workflow cleanup cannot strand transport rows.
Migration `000008` preserves malformed or unknown broker records as durable
offset-addressed poison records so a consumer can acknowledge them without
discarding reconciliation evidence. Migration `000009` links poison records
whose event ID is known to their workflow and partition for reconciliation
scans; records with unknown IDs remain global poison evidence.
