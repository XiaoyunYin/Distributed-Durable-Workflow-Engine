# DUR-056 solvable-evidence agent study

**Current status:** Round 132 Gate A correction in progress. The previous
freeze package received CHANGES_REQUESTED. Held-out scoring is locked, no
receipt exists, and no held-out case, query, document, or embedding may reach a
provider.

**Current protocol:** [`protocol.md`](protocol.md), registered by D030 as an
amendment to D029. The initial v1 protocol and README are archived in
[`protocol-v1-superseded-round132.md`](protocol-v1-superseded-round132.md) and
[`README-v1-superseded-round132.md`](README-v1-superseded-round132.md). Previous
v1 results, configs, fingerprints, database rows, and ledger entries remain
unchanged as historical baselines.

## V2 fixture validity

V2 uses new seeds: 5603 for 30 development cases and 5604 for 60 held-out
cases. The corpus-level structural oracle scans every chunk in the named split
and derives each truth key only from its log and metric envelopes.

| Split | Cases | Corpus keys | Oracle | Provider calls |
| --- | ---: | ---: | ---: | ---: |
| Development | 30 | 20 | 30/30 | 0 |
| Held out | 60 | 24 | 60/60 | 0 |

Cases that share `(service, active_version, signal)` share one expected
outcome. No-guidance keys have no current directive anywhere in their split
corpus; missing-parameter keys have only incomplete current guidance;
stale-with-winner keys have one complete current directive and superseded
older evidence; unresolved keys have conflicting complete current directives.
Decoys use a different key. The fixture tests include a mutant that inserts a
complete current directive for an insufficient key elsewhere in the corpus;
the corpus oracle then disagrees with the registered labels.

Chunk/query text contains no case IDs, split prefixes, or fixed query padding.
Queries use service, active version, signal, and a symptom phrase from
disjoint development and held-out templates. Negative evidence uses structured
status/version/supersedes/action/parameter facts without category-announcing
sentences. Active versions and answer values are seeded per key; injected
wrong values are seeded per case. Development and held-out answer-value sets
differ.

The final local fixture/oracle check is recorded at
[`fixture-validity-v2-20260928T203436Z.json`](fixture-validity-v2-20260928T203436Z.json).
It used zero model and embedding provider calls. This is a fixture validity
result, not a model result. Earlier v2 validation artifacts remain preserved
as diagnostic history.

## Development-only retrieval and agent work

The corrected retrieval strategy uses sanitized OR-joined PostgreSQL
`to_tsquery('simple', ...)` terms ranked by `ts_rank`, plus pgvector dense
retrieval and hybrid reciprocal rank fusion. The strategy and settings are
selected on development queries only. The dense model remains
`text-embedding-3-small` at 1536 dimensions with HNSW indexing. Agent tuning
continues with `gpt-6-luna` and development cases only.

New v2 development retrieval results, agent results, config fingerprints, and
per-run spend will be linked here after the development pass finishes. No
held-out scoring or tuning is included in this Gate A attempt.

The first v2 retrieval tuning attempt completed on development data, embedded
only the 42 development chunks and 60 development queries, then stopped before
agent tuning because a shared workflow appended the legacy `runbook` suffix to
the second query. Its retrieval output and embedding provenance are preserved
as diagnostics, not as the new Gate A freeze. The paid model and embedding
calls are itemized in the append-only ledger and the aborted-attempt record.

## Comparisons and spend

V2 reports paired, case-level comparisons between each retrieval arm and
no-retrieval, compares each arm against the local oracle ceiling, and reports
each negative category separately. DUR-029's `gpt-4o-mini` 4/20 outcome is
historical only; its fixture and case population differ and are not comparable
to DUR-056.

The existing append-only spend ledger has a **$25.00 USD** hard cap. The v1
history remains intact; v2 development charges will append to it. Gate A
preparation will publish the settled, reserved, and uncertain totals with the
new configs. Live-agent execution and crash-resume on the Go/PostgreSQL engine
remain out of scope.

## Reproduction

Run both local split checks without provider credentials:

```powershell
$env:PYTHONPATH = "python"
uv run python -m incident_agent.dur056 validate-fixtures
```

Only after fixture tests pass, run the development-only tuning command against
the isolated DUR-056 PostgreSQL service. The runner embeds development corpus
and query text only. Gate A stops for Claude's review after the exact commit is
pushed and its CI is green.
