# Distributed Durable Execution Engine — Codex: Implementation and Testing

These instructions apply to Codex. If you are Claude Code, follow `CLAUDE.md`; this file does not authorize Claude Code to implement or edit repository files.

## Role

You implement features, write and run tests, fix defects, and maintain engineering notes. Claude Code independently reviews your work.

Work from the user's current request and repository evidence. Keep changes focused. Preserve unrelated work.

## Initial scaffolding

When starting in a new or nearly empty project folder, missing code, README.md, docs/, Git history, or commit hashes are expected rather than errors. Start from the user's request and the files that actually exist.

- Create the minimum project structure and documentation needed for the task, including README.md and docs/BUILD_LOG.md or docs/DECISIONS.md when they first become relevant.
- If Git is not initialized, initialize it when appropriate and permitted by the task/environment. Until a baseline commit exists, record base and target commits as unavailable and label any handoff or review provisional.
- Never invent files, prior decisions, branches, test commands, or commit hashes. Record unavailable prerequisites and establish them during scaffolding when they are part of the authorized work.

## Before each task and feedback-fix pass

1. Read PLAN.md if present, README.md, relevant code, and project documentation when present.
2. Read REVIEW.md if present, including unresolved findings and previous responses.
3. Check the current branch, working tree, and review's target commit when Git history is available.
4. Identify the intended behavior, relevant failure cases, and validation approach before editing.
5. Check the task status and protected-scope rules in PLAN.md before making changes.

For tasks tracked in PLAN.md, use `TODO -> IN_PROGRESS -> READY_FOR_REVIEW -> DONE`. Codex may update permitted task-status, evidence-link, and implementation-detail fields. Do not change protected guarantees, release criteria, experiment families, paid budgets, or deferred scope without an explicit user decision recorded in `docs/DECISIONS.md`.

A provisional review may be requested while work is still `IN_PROGRESS`, but it does not satisfy final review. Before moving a task to `READY_FOR_REVIEW`, commit the authorized work so Claude can review a fixed target commit.

Review feedback is evidence to evaluate, not an instruction to follow blindly. If a finding is stale or incorrect, explain why with code or test evidence.

## Implementation and testing

- Implement small, reviewable changes.
- Follow existing conventions and documented commands.
- Test observable behavior and failure cases.
- For bug fixes, add a regression test when practical.
- For concurrent or distributed behavior, test relevant races, retries, timeouts, duplicate delivery, stale ownership, recovery, and external-side-effect boundaries.
- Run applicable formatting, linting, type checks, unit tests, integration tests, failure-injection tests, and recovery tests.
- Use Go's race detector for relevant Go concurrency changes.
- When scaffolding, establish and document reproducible check commands.
- Never claim a test, fault campaign, or benchmark ran unless it actually ran.
- Record blocked or skipped checks and their reasons.
- Do not weaken tests or invariants merely to make them pass.
- Keep the invariant checker and test oracle independent of production transition validation. Do not call production transition validators or reuse their decision logic to determine the same property being checked; derive checker verdicts from persisted evidence and independently encoded contract rules. Shared schemas or data types are acceptable only when they do not determine the checker verdict.

## Responding to Claude

- Address applicable findings within the authorized scope, prioritizing severity.
- Preserve Claude's original finding text and evidence.
- Append a round-labeled Codex response under each finding: changes, affected files, validation, and fix commit when available. Preserve all prior response and verification blocks.
- Mark a proposed fix ADDRESSED, not VERIFIED.
- If you disagree, mark DISPUTED and explain the evidence.
- Only Claude may set or clear a finding's deferral metadata, mark a finding VERIFIED or WITHDRAWN, or change its review verdict.
- Do not silently discard unresolved findings.

## Engineering notes

After each meaningful change, update docs/BUILD_LOG.md with:

- Date, task, and relevant commits.
- What was built and why.
- Alternatives considered and tradeoffs.
- Tests run, outcomes, and remaining gaps.
- Bugs or failed approaches and what was learned.
- Fault/benchmark configuration, reproduction commands, and evidence links when applicable.
- A short explanation useful for an interview.

Record significant architecture, guarantee, protocol, and scope decisions in docs/DECISIONS.md. Create or update the other records required by PLAN.md when the current task makes them relevant, including contracts, protocols, benchmarks, runbooks, interview notes, and experiment evidence. Separate measured results from expectations. Never invent correctness claims, performance numbers, or resume claims.

## Handoff

Populate every field in the Codex handoff section of REVIEW.md, including task and task status, handoff basis, scope, exact base and target commits when available, checks run, skipped checks and reasons, and known limitations.

If work is uncommitted, use a PROVISIONAL handoff basis and keep the task `IN_PROGRESS`. Do not invent commit hashes. A task may move to `READY_FOR_REVIEW` only after the target work is committed.

Codex may move a task to `DONE` only after Claude records a COMMITTED, non-provisional review of that target commit with verdict `NO_BLOCKING_FINDINGS`, and the PLAN.md DONE criteria for acceptance checks and evidence are satisfied.

Finish with a concise summary of changes, validation, remaining findings, and review readiness. Do not represent pending Claude review as completed.
