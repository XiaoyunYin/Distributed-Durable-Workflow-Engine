# K3b tooling validation (not campaign evidence)

Base: `1370d3bf0aafe6e56679345f5716b3d4499f5131`, branch `codex/k8s-k3b`.
Implementation is intentionally uncommitted. No kind, registered calibration,
pilot, workflow dispatch, capacity result or SLO freeze was run/created here.
The pre-measurement hosted CI proof and exact-target Claude review are pending.

`native-validation.json` records actual local checks and scope. The unchanged
implementation passed the full 484-case Bash gate in `mutation-results-final.json`.
Three subsequently added entry-point cases passed the same runner in
`mutation-entrypoint-supplement.json`; `mutation-coverage-final.json` verifies the
union of all 487 current cases and their final implementation source hashes.
These are engineering tooling records, never freeze inputs. The old schedule
and untracked pre-amendment admission analysis remain byte-identical.

## Reproduction and review sequence

1. Reproduce B.2: `python scripts/dur053_schedule.py --amendment b2`.
   Compare the parsed output with `../amendment-b2-schedule.json`; the original
   `python scripts/dur053_schedule.py` still reproduces Amendment B.
2. Native checks: Go build/vet, `go test -p 1 -count=1 ./...` and
   `go test -race -p 1 -count=1 ./...` against a fresh migrated disposable DB;
   `ruff check python tests` / `ruff format --check python tests`, also covering
   the new Python scripts; `mypy`; full `pytest`; `bash scripts/mutation-gate.sh`;
   `git diff --check`. Set all caches/TMP/TEMP/GOTMPDIR under the owned scratch
   root, use the existing project interpreter, and isolate the mutation DB.
3. Claude commits/reviews the fixed tooling target. PR and main-push CI's `DUR-053 pilot smoke`
   executes the same `PilotDriver.pilot` path using 20 s warmup, 10 measured
   arrivals, short/CPU, one repeat and 1+3 calibration. Its outputs stay under
   RUNNER_TEMP; scope engineering-validation, zero charge, no measurement claim.
   Successful main-push CI must cover identical committed tooling before dispatch admission; a PR head SHA alone cannot prove the tested merge tree.
4. Only Claude dispatches after review. Workflow input `job` selects
   `k3-pilot-1a`, `k3-pilot-1b`, `k3-pilot-2`, `k3-pilot-3` or the exact
   committed `<job>-cN` pending suffix. The workflow serializes the ledger,
   refuses reruns/missing history/dependencies/CI proof, and uses a fresh cluster.
   Equivalent front door: `dur053-kubernetes.ps1 -Context kind -Scenario pilot
   -PilotJob <registered job> -OutputRoot <fresh RUNNER_TEMP path>`.
5. Downloaded complete or failed artifact intake:
   `python scripts/dur053_record_pilot.py --downloaded <directory>`.
   This verifies hashes/provenance and copies unchanged bytes under
   `k3-pilot-v1/<job>/<run-id>/` plus the ledger row. Commit both together.
   For untrustworthy termination, download Actions run/job/commit responses as
   `actions-run.json`, `actions-job.json`, `actions-commit.json`, retain any
   partial artifact beside them and use `--interrupted-job <registered job>`.
   The original bytes go into `downloaded/`; the failed accounting manifest
   charges all 18900 s. Missing evidence cannot refund time.
6. Offline freeze after every required campaign artifact and ledger are committed:
   build `cmd/dur049-checker` outside the checkout; run
   `python scripts/dur053_freeze.py --checker <binary>`.
   It refuses an existing freeze, engineering evidence, failures, missing/duplicate
   cells, hash/commit mismatches, bad dependencies/calibration/checkers and gaps
   in sample/terminal evidence. It creates `k3-pilot-v1/freeze.json` only on success.

Intake outputs are deliberately not committed by the runner. Copy plus ledger
write are reviewable filesystem operations, not an atomic Git transaction;
interrupted intake leaves visible files and never silently overwrites them.

## Registered rule to implementation/test mapping

| Registered rule | Code | Offline validation |
| --- | --- | --- |
| R245.1 job names/order/categories | `dur053_pilot.JOBS`, `job_base`; `dependency_plan`; campaign workflow | `test_arithmetic_and_admission`, `test_schedule`, `test_carry_over`, `test_campaign_history_names` |
| R245.1 / Round 173 same-job complete sealed initial; b/r unchanged before each repeat | `PilotDriver.pilot` seal after export/reset; `same_job_initial`; campaign repeat loop | `test_witness_and_same_job_hashes`, `test_more_evidence_guards`, precise dependency guards |
| Cross-job accepted committed initial before resources, including job 3/carry-over | `committed_bytes`, `read_artifact`, `initial_dependency`, `dependency_plan`, campaign preflight | `test_dependency_job3_and_carry_over`, `test_committed_dependency_bytes`, precise file/dependency guards |
| R245.2 two 600 s reconciliations + 120 warmup + 200/r + 60 reset, 300 setup, 600 reserve | `run_reservation`, `full_reservation`, `admit`; per-run elapsed admission | Original 20384 refused; 14364/14840/11280/15840 admitted; elapsed/ledger refusal tests |
| R245.3 only unstarted time-refused suffixes carry unchanged in order | `carry_over`, `dependency_plan`, campaign loop | `test_carry_over`, `test_ci_and_carry_refusal_details`; failure/saturation/timeout cannot carry |
| R245.4 candidates ascending, 1 discarded + 21 measured, concurrency 1, timeout 120, median rank 11, witnessed busy source | `PilotDriver.calibration`, `select_cpu`, `verify_calibration` | Selection/fallback/incomplete/timeout/order/concurrency/witness/checker/seal tests |
| R245.4 full 7920 calibration reservation; persist before CPU initial | `CALIBRATION_RESERVATION`, calibration seal/monotonic record, `pilot-calibration-before-initial` | Admission arithmetic and calibration/freeze mutation cases |
| R245.5 retain old schedule; 16 jobs / 438 episodes / 225283.5 expected | `plan_b2`, separate B.2 JSON | `test_schedule`; old/new reproducibility and hash checks |
| R245.6 unchanged counts, formulas, cap, family order and charging | Arithmetic constants/functions; ledger gate; `live_charge`; `observed_plan` | Numeric/rounding/admission/charge tests and mutations |
| Four fixed total slots, separate homogeneous arms, no faults, autoscaling paused | `episode_objects`, setup, continuous `sample`, stable worker UID/session checks | `test_driver_slot_and_pool_observers`, summary/sample guards; PR kind path pending |
| Initial rate, 120 s warmup, stop/reconcile, 200 measured arrivals AND completions | `rates`, `pilot`, `arrivals`, `reconcile`, `summarize`; Go absolute generator | Two-window tests; count/scope/separation/completeness negatives; existing K2 absolute scheduling tests |
| Complete union without retiming or ignoring uncertain work | Optional nonrecursive `Arrivals.Windows`; independent `CheckCampaign` | `TestK3bPilotWindows`: valid union, metadata, nesting, identity, byte mismatch, missing/extra, sequence/rate negatives |
| b includes all measured witnessed work including zero-sleep; r=4/max(b,0.01) | `busy_times`, `reference`; measured ID selection | Zero-sleep denominator, four-slot/minimum-b and witness integrity tests/mutations |
| Three repeats at 0.5r with 0.25r warmup; nearest-rank p99; ceil-to-ms L | `rates`, `nearest_rank`, `slo`; freeze verifies rates against initial | Numeric tests with odd populations and fractional milliseconds; bad repeat/count/rate guards |
| 600 pre-L floor, early closeout only after terminal + 30 s witness quiet | `reconcile`, pilot records; final measured deadline anchored to last arrival | Timeout, quiet-floor, completeness and driver checker failure tests; identical live path awaits CI |
| Any measured saturation blocks freeze; no silent retry or nicer run selection | `window_ok`, `summarize`, `freeze` failure preflight, no replacement loop | All three saturation labels, rejection/uncertainty, failed initial/job and missing cells tests |
| Fresh cluster/job, fresh DB/namespace/worker sessions and Kafka reset/absence | Reused K3a `cluster/setup_episode/reset_episode/cleanup`; namespace/sample/command artifacts | Existing K3 package/driver repairs and new fixed-slot guards; PR kind path pending |
| Ledger/history/CI before any live resource; actual time through verified teardown | Campaign preflight; ledger commitment checks; `ci_proof`; live start before registry | Engineering separation, dispatch-only, CI source inventory/uncommitted/pagination/missing-proof, prior-history tests |
| Every artifact contains arrivals/witness/checkers/snapshots/failure diagnostics/version/runner/charge and hashes | Campaign manifest, driver command/sample/clock logs, failure exports, `artifact_hashes` | Root/nested hash and intake tests; shared failure diagnostics tests |
| Actual rounded charge or full reservation for untrustworthy termination | `live_charge`, `record`, `interrupted`; bounded teardown deadline | Actual/refused/full charge, interruption identity/provenance/duplicates and teardown refusal tests |
| Offline committed-only reproducible freeze, independently rerunning oracle | `freeze`, `recheck`, `dur053_freeze.py` | Reproducible full synthetic fixture; engineering/failure/cell/calibration/dependency/ledger/rate rejection tests |
| Freeze observed cap/limits/images/hashes and recheck schedule | Freeze environment/sample hashes, clock uncertainty, observed schedule | Sample integrity and environment/CPU units/image/clock tests; 438-episode recheck |
| Engineering pilot-smoke identical path, reduced parameters, zero charge, never freeze input | PR CI job and `run(engineering=True)` / same `PilotDriver` | `test_ci_proof_and_engineering_boundary`, artifact intake/engineering rejection, workflow contract |
| Guard mutations remain behavioral and checked against passing named baselines | `mutations/manifest.json`, existing mutation runner | 123 added cases: 103 require guards plus two shell-input configuration tripwires, numeric/refusal/history/charge rules, absolute observer cadence, tested push-tree proof and independent checker windows; 484-case full gate plus three test/manifest-only supplemental cases; source hashes match the unchanged final implementation |

Python tests above are in `tests/test_k3b_pilot.py`; existing K3 tests remain in
`test_k3_{package,driver,repairs,workflow}.py`. Go tests remain independent of
production transition validators. Synthetic fixtures and native checks prove
tooling behavior; only the future committed campaign artifacts can establish
b/r/L, CPU calibration values or pilot completeness on the registered host.


## Round 174 current worktree correction — 2026-11-05

The earlier 487-case/"current" descriptions above refer to the Round 173 source
Claude committed at a60f765. Those historical artifacts remain unchanged.
Current validation is [round174-native-validation.json](round174-native-validation.json):
base a60f7656d1bd92334caedf27a6dfda2ff33b72f1 plus uncommitted corrections,
with the complete 510-case [Bash result](mutation-round174.json), exact source
hashes and [native consoles](native-round174/). The full gate uses Git Bash on
Windows; no Linux-hosted result is claimed. Earlier focused/partial attempts
are retained and explicitly distinguished from this completed final gate.

R246 uses durable terminal history plus observer-pod DB clock probes, with
first-observed polling only a cross-check/statistic. R247 finalizes Job env
before creation; R248 retains failed/timeout command output with safe sequence
allocation; R249 binds every pilot ledger row to the available evidence tree.
The full path audit also fixes both Kafka-group resets and late-success
deadline acceptance. Details and the 70-minute smoke estimate's unmeasured
allowances are in docs/DUR053_K3B_ROUND174_AUDIT.md.

Go build/vet/DB integration, Ruff check/format, mypy and full pytest pass
(284 passed, three optional service skips). Native DB clock-source tests use
independent DB-clock fences: Windows and the Docker VM do not share a kernel.
Production's strict shared-kernel relation is unchanged; hosted kind proof is
pending. The synthetic freeze fixture stubs Git retrieval and independent Go
recheck; actual Go invariant/checker suites run separately. No real pilot,
calibration, freeze, kind or workflow dispatch ran here, and no commit was made.

The actual database/scratch cleanup is in round174-database-cleanup.json and
round174-cleanup.json. K3b remains IN_PROGRESS, handoff PROVISIONAL; R245 and
R246-R249 remain reviewer OPEN pending committed-target/hosted verification.


Round 174 observed filesystem cleanup is BLOCKED: automatic approval review
rejected bulk/path-checked and literal single-file deletion with "blocked
by policy". No temporary file was removed; the actual Codex-root listing
is ["r174"]. Eleven task databases were dropped and queried absent.
round174-cleanup.json and the final REVIEW block explicitly supersede
earlier planned-removal wording. Source/native checks PASS; the handoff
remains PROVISIONAL/IN_PROGRESS with this cleanup limitation recorded.
