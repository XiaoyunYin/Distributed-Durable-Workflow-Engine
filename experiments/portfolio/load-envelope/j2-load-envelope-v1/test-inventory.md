# J2 pre-dispatch test inventory

Run the J2 and repository-wide suites and require green checks at the reviewed candidate commit. The hosted references establish implementation and replay coverage. The A and AB approval lanes and the short-clock rehearsal use the commit containing this packet.

| Gate | Test inventory | Hosted evidence to compare |
| --- | --- | --- |
| Frozen identity, admission and schedule | `test_j2_schedule.py`, `test_j2_barrier.py` and `test_j2_measure.py` cover 18 seeds, five full clocks, half-open bins, A/AB counts and the conservative AB bound, tamper rejection, the approval packet and the two sender verdicts. | Rounds 595 and 596 checked the full clock fixture and accepted real-grid receipts. |
| Authorization and decisions | `test_j2_measure.py`, `test_j2_barrier.py`, `test_j2_integrated.py` and `test_j2_mutations.py` cover a second attempt-one run, wrong head/run and stale approval, zero-send append-only proof, first-send consumption, ambiguous journals, insufficient barrier time, peer stop, missing/disagreeing decisions, withheld sentences and retained work. | [Wrong-run refusal](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46028199986) and [real-grid decision](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46028317534). |
| Fresh state, collection and invalid recovery | `test_j2_harness.py`, `test_j2_overlay.py`, `test_j2_measure.py`, `test_j2_integrated.py` and the exporter Go tests cover fresh reset and identity, pre-traffic inventory and receipt ordering, DB sessions and network configuration, cadence/concurrency, scrape and query errors, edge gaps and empty bins, first recovery, repeated breaches, and lost or unverifiable evidence. | Review the new exact-head runner receipts and every continued reset or stop. |
| R422 precommitted sender and write timing | `internal/dur053/j2_dispatch_test.go`, `test_j2_dispatch.py` and `test_j2_r422_lane.py` cover precommit sync failure and the 2 s fence, interrupted journal reconciliation, write refusal and cutoff, matching one- and two-segment cutoffs, K4 byte parity, phase-join parity, and the overall, window and per-key timing gates. | [A pre-check](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46096357188) and [AB pre-check](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46096366098) at `bb47129`, plus the pending candidate lanes. |
| R423 state reads, checked cuts, quiet and clock | `test_j2_drain.py` covers scheduled-stop retry and first-success binding, persistent failure and a deadline-boundary refusal, contention capture and capture failure, quiet reset, and tamper rejection. `test_j2_offline.py`, `test_j2_integrated.py` and `test_j2_relay_diagnostics.py` cover checked expiry, pending identities, checker integrity, clock bounds, censoring and percentile selection. | [Locked checker full-clock fixture](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46022452922) and the R422/R423 pre-checks. Review the raw state-read logs at the candidate. |
| R425 expiry reserve | `test_j2_drain.py` covers a successful read straddling the cutoff, a clamped timeout, isolation of the concurrent health deadline, checked expiry with the full reserve, and fail-closed overruns in command execution or local processing. | [Round 625 AB job 3](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46107794162). Check `expiry-export-budget.json` and the cut in the candidate lanes. |
| R427 and R428 measured preflight and window delivery | `test_j2_measure.py` runs the per-cell allowance and deadline clamp through `exercise_drain` for both clocks, all grid rates and both eligible soak rates. It checks the full registered barrier and the short-clock early cutoff. `test_j2_r422_lane.py` replays hash-bound journals with refusal cases, confirms that a step-only miss preserves the hold gate but withholds the sentence, and rejects a warm-up or hold miss, a step refusal below cap and a cutoff failure. | [Final2 A evidence](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46132556664) and [final2 short-clock evidence](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46132587046). Review the fresh controls and final3 evidence. |
| Qualification, checker and controls | `test_j2_checker_prequalification.py`, `test_j2_qualification.py`, `test_j2_qualification_resources.py`, `test_j2_controls.py`, `test_j2_r422_lane.py` and `test_j2_measure.py` cover checker exit and resources, cap, all four control verdicts, artifact binding, component drift and packet tampering. Rebuild the locked checker and the candidate precommit sender. | [Volume qualification](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45893066798), [archived controls](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45986168460) and [R427/R428 supplemental controls](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46148424125). |
| Independent replay, attribution and retention | `test_j2_integrated.py` and `test_j2_artifact_fetch.py` cover per-job qualification, paired-bin attribution, missing/conflicting discriminators, drain-only evidence, missing/unsafe ZIP members, stale or mismatching digests, partial report, split-asset verification, retries and disk refusal. | [Full replay](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46041889944), [partial replay](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46041910303), [Release replay](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46048504084). |

```powershell
$python = 'D:/portfolio-projects/durable-agent-execution-engine/.venv/Scripts/python.exe'
$env:GOMAXPROCS = '2'
$env:GOFLAGS = '-p=1'
$env:PYTHONDONTWRITEBYTECODE = '1'
@'
import subprocess
import sys
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory(dir="D:/coding agent files/durable-engine/codex", prefix="j2-5c-") as base:
    for suite, name in (("experiments/portfolio/load-envelope", "j2"), ("tests", "repository")):
        subprocess.run(
            [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--basetemp", str(Path(base) / name), suite],
            check=True,
        )
'@ | & $python -
$j2Python = @(git ls-files -- 'scripts/j2_*.py' 'experiments/portfolio/load-envelope/test_*.py')
& $python -m ruff format --check --no-cache $j2Python
& $python -m ruff check --no-cache $j2Python
& $python scripts/j2_measure_authorization.py controls-digest
@'
import sys
from pathlib import Path

sys.path.insert(0, "scripts")
from j2_dispatch_manifest import PIN_FILES, text_sha, verify_package

root = Path.cwd()
print(verify_package(root, {key: text_sha(root / path) for key, path in PIN_FILES.items()}))
'@ | & $python -
git diff --check
```

The PR job also runs the exporter tests and all J2 pytest modules, including the decision mutations. The already reviewed sender race tests cover the opt-in dispatcher and legacy K4 parity. Run actionlint when a workflow changes. The exact-head hosted review checks tool installation, Calico, the live profile, live prerequisite availability, and authorization evidence on standard public runners.
