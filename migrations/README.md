# Migrations

Numbered `*.up.sql` files are inspected in lexical order by
`scripts/migrate.ps1`. Each migration must record its numeric filename version
in `engine.schema_migrations`; the script skips versions already recorded and
confirms the ledger entry after applying a new file. Migration `000002` adds
the DUR-005 durable workflow state repository, including revision/history,
lease, attempt, outbox/inbox, approval, effect, and reconciliation-evidence
tables. Migration `000003` adds immutable activity effect-class metadata and
makes evidence follow workflow retention with a cascading foreign key.
