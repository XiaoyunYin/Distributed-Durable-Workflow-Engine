# Distributed Durable Execution Engine — Review and Feedback

## Review contract

Severity:

- P0 — Critical: immediate correctness, security, data-loss, or availability risk that blocks acceptance.
- P1 — High: serious defect or regression with substantial user/system impact; should be fixed before acceptance.
- P2 — Medium: real defect or important test/reliability gap with bounded impact; fix unless explicitly deferred.
- P3 — Low: minor correctness, maintainability, observability, documentation, or test-quality issue that does not block acceptance by itself.

Blocking findings:

- Unresolved P0 and P1 findings always block acceptance.
- An unresolved P2 finding blocks acceptance unless Claude explicitly records it as deferred with a reason and linked follow-up. A deferred P2 remains OPEN; deferral is metadata, not a finding status.
- P3 findings do not block acceptance by themselves.

Finding status lifecycle:

- OPEN — Claude has recorded the finding and it remains unresolved.
- ADDRESSED — Codex has proposed a fix; Claude has not yet verified it.
- DISPUTED — Codex believes the finding is stale or incorrect and has supplied evidence.
- VERIFIED — Claude checked the proposed fix and confirmed the finding is resolved.
- WITHDRAWN — Claude retracts the finding after determining it was incorrect or inapplicable; the withdrawal reason and evidence must be recorded.

The `Status` field directly under each finding is the authoritative current state of that finding. The status inside a `Codex response` records Codex's action for that response, and the status inside a `Claude verification` records Claude's action for that verification step; those subsection fields are an audit trail, not separate competing states. Whenever Codex or Claude records a response or verification, update the finding's top-level `Status` to the resulting current state.

Review verdicts:

- NOT_REVIEWED — No completed Claude review exists for the recorded scope and target.
- CHANGES_REQUESTED — One or more unresolved findings block acceptance of the reviewed scope.
- NO_BLOCKING_FINDINGS — Claude found no unresolved blocking findings for the recorded scope and review basis. Deferred P2 findings, P3 findings, uncertainties, and untested areas may remain and must be stated.

A completed provisional review may use CHANGES_REQUESTED or NO_BLOCKING_FINDINGS, but a provisional review cannot satisfy PLAN.md DONE criteria. Final acceptance requires review of the committed final target.

A handoff with `Task status: READY_FOR_REVIEW` requires `Handoff basis: COMMITTED` and a concrete target commit. A `PROVISIONAL` handoff must remain `IN_PROGRESS`.

## Codex handoff

- Task:
- Task status: <IN_PROGRESS | READY_FOR_REVIEW>
- Handoff basis: <PROVISIONAL | COMMITTED>
- Base commit: <commit | unavailable>
- Target commit: <commit | unavailable>
- Scope and implementation summary:
- Checks run and results:
- Skipped checks and reasons:
- Known limitations:

## Claude review rounds

No review performed yet.

For each round, record:

- Date and round:
- Review basis: <PROVISIONAL | COMMITTED>
- Base and target commits: <commits | unavailable>
- Scope inspected:
- Checks personally run:
- Codex-reported checks considered:
- Deferred P2 findings, if any:
- Remaining P3 findings / uncertainties / untested areas:
- Limitations:
- Verdict: <CHANGES_REQUESTED | NO_BLOCKING_FINDINGS>

## Findings

No findings recorded yet. This does not mean the work has passed review.

Use this structure for each new finding. New findings start OPEN; update the top-level status as the lifecycle advances.

### R001 — Short title

- Severity: <P0 | P1 | P2 | P3>
- Status: OPEN
- Deferred: <no | yes>
- Deferral reason: <required when Deferred is yes>
- Follow-up: <required when Deferred is yes>
- Reviewed commit:
- Location:
- Failure scenario and impact:
- Evidence:
- Suggested correction:
- Suggested validation:

#### Codex response — round <N>

- Change made or reason for disagreement:
- Fix commit:
- Tests and results:
- Status: <ADDRESSED | DISPUTED>

#### Claude verification — round <N>

- Verification commit:
- Evidence and remaining concerns:
- Status: <OPEN | VERIFIED | WITHDRAWN>

For additional review cycles on the same finding, append another `Codex response — round N` and `Claude verification — round N` pair. Never overwrite earlier rounds.
