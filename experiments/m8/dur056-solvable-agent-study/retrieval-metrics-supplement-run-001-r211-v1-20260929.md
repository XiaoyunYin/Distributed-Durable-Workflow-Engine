# DUR-056 retrieval metrics supplement - run-001, R211 v1

**Source run:** Gate B `run-001`, completed 2026-09-29.
**Purpose:** Correct the displayed delivered Recall@3 and MRR denominator to
match the registered protocol.
**Source evidence:** [Write-once held-out units](heldout-units-run-001/),
[scorer report](heldout-report-run-001-finalization-001-20260929T044956Z.json),
and the held-out fixture's relevant chunk IDs. The unit timelines and scorer
report were read only; no scoring or provider calls were repeated.

## Population and calculation

There are 60 held-out incident cases. Fifty-four have one or more relevant
chunk IDs. The six `insufficient_no_guidance` cases have no relevant chunks,
so the protocol excludes them from retrieval ranking metrics. The denominator
is therefore 54 queries for each retrieval arm.

For each case, the `search_runbooks` tool-call event in its primary unit
contains the delivered ranked evidence. Recall@3 is the number of that case's
relevant chunk IDs found in the top three divided by its number of relevant
chunk IDs. Reciprocal rank is `1 / rank` for the first relevant chunk in the
returned ranking, or zero if no relevant chunk is returned. The reported
metrics are arithmetic means over the 54 cases with relevant chunks.

| Retrieval arm | Relevant queries | Mean delivered Recall@3 | Mean reciprocal rank |
| --- | ---: | ---: | ---: |
| Keyword | 54 | 1.000 | 1.000 |
| Dense | 54 | 0.972 | 0.914 |
| Hybrid RRF | 54 | 1.000 | 0.981 |

No-retrieval has no ranked result list, so Recall@3 and MRR are not applicable.
The classifier diagnostics continue to use all 60 cases and are reported
separately in the [Gate B results report](gate-b-results-run-001-20260929.md).

## Preserved scorer values

The machine-readable scorer report is preserved unchanged. Its all-60-row
averages are keyword 0.900/0.900, dense 0.875/0.822, and hybrid 0.900/0.883
(Recall@3/MRR). Those values count each of the six no-guidance cases as zero,
which differs from the protocol's relevant-query denominator. The report and
study README display the corrected 54-query figures above and identify this
original scorer-report calculation explicitly.
