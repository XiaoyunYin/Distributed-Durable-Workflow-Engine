# M6 incident-agent evidence

M6 uses a deterministic local fixture generator so correctness checks do not
depend on paid model credentials. `python -m incident_agent fixtures` records
the corpus counts and dashboard label policy. `benchmark` evaluates keyword,
dense, and hybrid retrieval over the same generated chunks. `continuity`
executes the 20 held-out interrupted/uninterrupted pairs and citation checks.
`adversarial` runs the 20 cases x 2 profiles x (`clean-A`, `clean-B`,
`injected`) protocol with fixed redaction and records proposal-change and
canary-leakage results, with approval enforcement reported separately.

The corpus generator creates 60 versioned documents and 300 chunks across five
synthetic families. Ground-truth labels are evaluator-only and never returned
by the MCP surface. The dense arm uses the pinned deterministic
`durable-hash-embed-v1` local embedding fallback; the migration creates the
`source_corpus` schema and PostgreSQL full-text index, while pgvector can be
enabled when the local PostgreSQL image exposes the extension, without
changing the retrieval contract. This deterministic hash adapter is a
reproducibility fixture, not a claim of semantic embedding quality.

`fixtures` also writes the evaluator-only case labels, frozen retrieval
configuration, the MCP schema, and a development-only difficulty audit.
Held-out labels are not returned by MCP methods or used by the workflow.

The repository gate can reproduce all artifacts with
`powershell.exe -ExecutionPolicy Bypass -File scripts/ci.ps1 -WithM6`.

The workflow is SQLite-backed for a portable deterministic demo. Its durable
timeline, approval wait, effect receipt, idempotent resume, citation validator,
and bounded MCP call budget are the same seams a PostgreSQL/engine adapter can
implement. No live model or paid-provider claim is made by these artifacts.
