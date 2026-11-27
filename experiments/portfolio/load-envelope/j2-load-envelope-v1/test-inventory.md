# J2 pre-dispatch test inventory

Run the scoped J2 suite and require green checks at the reviewed candidate commit. The table identifies the tests and accepted engineering evidence used to review the registered path.

| Gate | Test inventory | Hosted evidence to compare |
| --- | --- | --- |
| Frozen identity, admission and schedule | `test_j2_schedule.py`, `test_j2_barrier.py` and `test_j2_measure.py` cover 18 seeds, five full clocks, half-open bins, A/AB counts and the conservative AB bound, tamper rejection, the approval packet and the two sender verdicts. | Rounds 595 and 596 checked the full clock fixture and accepted real-grid receipts. |
| Authorization and decisions | `test_j2_measure.py`, `test_j2_barrier.py`, `test_j2_integrated.py` and `test_j2_mutations.py` cover a second attempt-one run, wrong head/run and stale approval, zero-send append-only proof, first-send consumption, ambiguous journals, insufficient barrier time, peer stop, missing/disagreeing decisions, withheld sentences and retained work. | [Wrong-run refusal](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46028199986) and [real-grid decision](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46028317534). |
| Fresh state, collection and invalid recovery | `test_j2_harness.py`, `test_j2_overlay.py`, `test_j2_measure.py`, `test_j2_integrated.py` and the exporter Go tests cover fresh reset and identity, pre-traffic inventory and receipt ordering, DB sessions and network configuration, cadence/concurrency, scrape and query errors, edge gaps and empty bins, first recovery, repeated breaches, and lost or unverifiable evidence. | Review the new exact-head runner receipts and every continued reset or stop. |
| Checked cuts, quiet and clock | `test_j2_offline.py`, `test_j2_drain.py`, `test_j2_integrated.py` and `test_j2_relay_diagnostics.py` cover checked expiry, pending identity reconciliation, checker integrity precedence, 4(a) and 4(c), unevaluable 4(b), invalid lower cells, quiet reset, both clock bounds, unspanned windows, and percentiles, censoring and p99 tie-break. | [Locked checker full-clock fixture](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46022452922). |
| Qualification, checker and controls | `test_j2_checker_prequalification.py`, `test_j2_qualification.py`, `test_j2_qualification_resources.py` and `test_j2_controls.py` cover checker exit/resource classification, cap, completeness and timing, all four control verdicts, and shared-code drift. Rebuild the locked checker and both senders. | [Volume qualification](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45893066798) and [four controls](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45986168460). |
| Independent replay, attribution and retention | `test_j2_integrated.py` and `test_j2_artifact_fetch.py` cover per-job qualification, paired-bin attribution, missing/conflicting discriminators, drain-only evidence, missing/unsafe ZIP members, stale or mismatching digests, partial report, split-asset verification, retries and disk refusal. | [Full replay](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46041889944), [partial replay](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46041910303), [Release replay](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46048504084). |

```powershell
$python = 'D:/portfolio-projects/durable-agent-execution-engine/.venv/Scripts/python.exe'
& $python -m pytest experiments/portfolio/load-envelope
& $python -m ruff format --check scripts/j2_dispatch_manifest.py scripts/j2_measure.py experiments/portfolio/load-envelope/test_j2_measure.py
& $python -m ruff check scripts/j2_dispatch_manifest.py scripts/j2_measure.py experiments/portfolio/load-envelope/test_j2_measure.py
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 -shellcheck= .github/workflows/j2-load-envelope.yml .github/workflows/j2-volume-qualification.yml
go test -p 1 ./experiments/portfolio/load-envelope/exporter ./internal/invariants/... ./internal/dur053/... ./cmd/j2-sender ./cmd/dur053-loadgen
git diff --check
```

The PR job also runs the exporter tests and all J2 pytest modules, including the decision mutations. The exact-head hosted review separately checks tool installation, Calico, the live profile and protected authorization evidence on standard public runners.
