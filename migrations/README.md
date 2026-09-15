# Migrations

Numbered `*.up.sql` files are applied in lexical order by `scripts/migrate.ps1`.
Migrations must be safe to run again until a dedicated migration manager is selected.
The durable workflow schema is intentionally deferred to DUR-005.
