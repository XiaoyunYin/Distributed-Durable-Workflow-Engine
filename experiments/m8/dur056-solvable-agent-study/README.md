# DUR-056 solvable-evidence agent study

**Current status:** Round 132 v2 Gate A package is prepared for full review;
the previous package received CHANGES_REQUESTED. Held-out scoring remains
locked, no receipt exists, and no held-out case, query, document, or embedding
may reach a provider.

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

The Gate A fixture/oracle check is recorded at
[`fixture-validity-v2-20260928T203815Z.json`](fixture-validity-v2-20260928T203815Z.json),
with split reports for [development](oracle-report-development-v2-20260928T203815Z.json)
and [held out](oracle-report-heldout-v2-20260928T203815Z.json). It reports 30/30
development and 60/60 held-out corpus-oracle accuracy, with zero provider
calls. This is a local structural validity result, not a model result. Earlier
v2 validation artifacts remain preserved as diagnostic history.

## Development-only retrieval and agent work

The corrected retrieval strategy uses sanitized OR-joined PostgreSQL
`to_tsquery('simple', ...)` terms ranked by `ts_rank`, plus pgvector dense
retrieval and hybrid reciprocal rank fusion. The strategy and settings are
selected on development queries only. The dense model remains
`text-embedding-3-small` at 1536 dimensions with HNSW indexing. Agent tuning
continues with `gpt-6-luna` and development cases only.

The corrected development run used committed source
`a2390b57cf7baf5e08169e79e8e391b95fc39d6e`. Its [manifest](study-manifest-v2-20260928T203817Z.json)
records PostgreSQL 18.6, pgvector 0.8.6, `text-embedding-3-small` at 1536
dimensions, complete embedding provenance for the 42 development chunks and
60 development queries, and zero held-out provider inputs. It reused v2
development vectors from the preserved [initial embedding manifest](study-manifest-v2-20260928T201912Z.json).

The [development retrieval report](retrieval-development-v2-20260928T203817Z.json)
and [frozen retrieval config](retrieval-frozen-config-v2-20260928T203817Z.json)
record the selected `top_k=3`, keyword threshold `0.0374109`, dense threshold
`0.7943787`, HNSW `ef_search=40`, and RRF `k=60`. The retrieval config
fingerprint is `sha256:10ad8b313376a02513e9ad96bebeb1c6d63cb758a2151074da1947cafdfa2672`.
Development query metrics:

| Retrieval arm | Balanced accuracy | False positives |
| --- | ---: | ---: |
| Keyword | 0.5714 | 15 |
| Dense | 0.5357 | 0 |
| Hybrid | 0.5714 | 15 |

Mean delivered recall across arms was 0.6056.

The [agent development report](agent-development-v2-20260928T204945Z.json)
contains 240 per-run records across two candidates, four arms, and 30
development cases. Candidate `candidate-v2` was selected by the registered
primary-safe rule (76/120 versus candidate-v1's 75/120). The raw report's
original action aggregate also counted matched null proposals; the separate
[recomputed analysis](agent-development-analysis-v2-20260928T211055Z.json)
uses the saved per-run rows, counts actions only on the 21 cases with an
expected action, and reports correct/false abstentions separately. This
correction made no provider calls and did not change the selected candidate.
The [frozen agent config](agent-frozen-config-v2-20260928T204945Z.json)
records model `gpt-6-luna`, prompt profile `evidence-contract-v2`, structured
schema `dur056-structured-v2`, and agent fingerprint
`sha256:c7a35cbffd51dc8786922711f4e78f2cc7f6dfc3aa2b388bf559b406ac8bd9ee`.

Selected candidate development outcomes (30 runs per arm):

| Arm | Safe end to end | Correct action / 21 | Correct / false abstentions | Diagnosis correct | Citation violations | Unsafe negative proposals | Input / output / reasoning tokens | Mean provider / workflow latency | Cost per run |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| No retrieval | 9/30 | 0/21 | 9 / 21 | 12/30 | 0 | 0 | 987.8 / 190.2 / 36.8 | 3160.8 / 3190.3 ms | $0.00019388 |
| Dense | 11/30 | 2/21 | 9 / 19 | 14/30 | 0 | 0 | 1010.0 / 195.1 / 41.9 | 3617.6 / 3662.9 ms | $0.00019855 |
| Keyword | 29/30 | 20/21 | 9 / 1 | 14/30 | 0 | 0 | 1332.1 / 230.7 / 39.9 | 3532.0 / 3568.9 ms | $0.00024854 |
| Hybrid | 27/30 | 20/21 | 7 / 1 | 16/30 | 0 | 2 | 1343.7 / 227.4 / 40.8 | 3140.9 / 3184.9 ms | $0.00024807 |

Both hybrid unsafe proposals were in `insufficient_missing_parameter`; all
other arm/category unsafe counts were zero. Wrong-parameter proposal counts
were zero in every arm.

Across the selected candidate's 120 calls, aggregate usage was 140,208 input,
25,301 output, and 4,781 reasoning tokens; provider latency totaled
403,539.685 ms, workflow latency totaled 408,208.408 ms, and cost was
$0.02667130. The full two-candidate sweep cost $0.04973010 across 240 runs.
All per-run records include tokens, provider and workflow latency, and cost.
The other candidate's results remain in the development report. The [Gate A
bundle](gate-a-freeze-review-v2-20260928T204945Z.json)
is `READY_FOR_FREEZE_REVIEW`, but explicitly records held-out scoring as
unauthorized and held-out provider calls as zero. Its freeze-bundle fingerprint
is `sha256:96ae6bc9064bfdab9e7d2a67fa4a9f0c3b02586bae85ab224136d8e604c39434`.

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

The append-only [spend ledger](spend-ledger.json) has a **$25.00 USD** hard cap.
At this freeze snapshot it records $0.11859702 settled, $0 reserved, and $0
uncertain across preserved prior and v2 work. The v1 history remains intact.
No live-agent execution or crash-resume on the Go/PostgreSQL engine is in
scope.

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
