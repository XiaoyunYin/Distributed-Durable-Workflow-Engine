# Migrations

Numbered `*.up.sql` files are inspected in lexical order by
`scripts/migrate.ps1`. Each migration must record its numeric filename version
in `engine.schema_migrations`; the script skips versions already recorded and
confirms the ledger entry after applying a new file. The durable workflow
schema is intentionally deferred to DUR-005.
