# Public review status

This is the compact, recruiter-facing status record for findings referenced by
the tracked README, technical report, and portfolio evidence. The detailed
round-by-round process log remains in the local, ignored `REVIEW.md`; this file
keeps public identifiers durable without publishing internal review workflow
notes.

## Current status

| Finding | Status | Public meaning | Evidence boundary |
| --- | --- | --- | --- |
| R057 | VERIFIED | The F01 submission boundary no longer implies durable activity work, and the independent checker rejects activity work at that boundary. | M5 fault archive and round-50 correction target `0002e75`. |
| R083 | VERIFIED | The DUR-027 handoff uses reachable commit `71fd54a`; the orphaned amended commit is historical only. | DUR-027 handoff correction in `0002e75`. |
| R088 | VERIFIED | The approval-target resource guard has focused regression coverage at both validation layers. | DUR-033A correction and focused approval test in `0002e75`. |
| R092 | OPEN | The engineering walkthrough evidence was reproduced by Codex, but personal user attribution remains outstanding. | User must perform or explicitly waive the three exercises; no release claim depends on completion. |
| R096 | OPEN; Deferred: yes | The repair bounds lock acquisition and scheduler iterations with a distinct retryable sentinel while preserving the in-transaction fence. The historical lock-held arm remains outside the `local-410041` campaign scope. | [D017](DECISIONS.md#d017---authorize-and-supersede-the-r096-repair), repair commit `41bb9bc`, and local campaign scope. |

## Scope rule

The local recovery campaign reports only owner-crash and owner-pause evidence:
60 episodes, 180 fenced stale-owner writes, and zero false takeovers. Its
historical campaign scope does not establish bounded scheduler liveness for
R096, multi-host durability, database-host failure recovery, or production
availability. Separately, Claude's round-55 review verified the R096 repair:
the bounded-wait and scheduler-continuation tests preserve the fence while
making a stalled lock acquisition retryable.

This record is not a release authorization and does not move `v0.1.0`. The
historical tag remains at `7e4137d`; later corrections are post-tag work.
