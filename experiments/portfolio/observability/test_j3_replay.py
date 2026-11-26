"""Deadline edge, concurrent visibility and history integrity fixtures."""

import copy
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
import j3_replay as j3  # noqa: E402
from j3_diagnostics import enable_exporter_diagnostics  # noqa: E402

T = 1_767_226_800 * j3.SECOND
SETUP, CLOSING = 1_000_000, 3_000_000
HERE = Path(__file__).parent


def fixture_data():
    data = dict(
        namespace="target",
        start_ns=T - 800 * j3.SECOND,
        end_ns=T + 2 * j3.SECOND,
        plan=[],
        workflows=[],
        journal=[],
        history=[],
        first_eval_ns=T,
        last_eval_ns=T,
    )
    start, end = data["start_ns"], data["end_ns"]
    data["clock"] = {
        "setup": {"before_ns": start - SETUP, "db_ns": start, "after_ns": start},
        "closing": {"before_ns": end, "db_ns": end, "after_ns": end + CLOSING},
    }
    for i in range(128):
        key = f"w{i}"
        deadline = {125: T - 120 * j3.SECOND, 126: T - 600 * j3.SECOND, 127: T + j3.SECOND}.get(
            i, T - i * j3.SECOND // 2
        )
        state = {0: "FAILED", **dict.fromkeys(range(2, 18), "WAITING_ACTIVITY")}.get(i, "SUCCEEDED")
        revision = 2 if i == 18 else 1
        ident = {"namespace": "target", "submission_key": key}
        data["plan"].append({**ident, "scheduled_ns": deadline - j3.DEADLINE})
        data["workflows"].append(
            {**ident, "workflow_id": key, "state": state, "revision": revision}
        )
        data["journal"].append(
            {**ident, "workflow_id": key, "outcome": "timeout" if i == 19 else "accepted"}
        )
        if state in j3.TERMINAL:
            width = {-1: 0, 1: -SETUP, 19: SETUP, 20: CLOSING}.get(i, 2 * CLOSING)
            data["history"].append(
                dict(
                    workflow_id=key, revision=revision, new_state=state, created_ns=deadline - width
                )
            )
    return data


def visibility(data, blocked=True):
    counts = {
        window: {"eligible": eligible, "bad": 18 + int(blocked)}
        for window, eligible in (("2m", 125), ("10m", 126), ("all", 127))
    }
    demand_at = "2026-01-01T00:20:00Z"
    sample = dict(
        sample_ns=T,
        u_ns=SETUP,
        windows=counts,
        admissions={w["workflow_id"]: w["submission_key"] for w in data["workflows"]},
        successes={
            h["workflow_id"]: h["revision"]
            for h in data["history"]
            if h["new_state"] == "SUCCEEDED"
            and (not blocked or h["workflow_id"] != "w18")
            and h["created_ns"] <= T
        },
    )
    sample["demand"] = dict(
        namespace="target",
        sampled_at=demand_at,
        active_claims=0,
        claimable_attempts=0,
        oldest_claimable_age_seconds=0,
    )
    data["evaluations"] = [
        dict(
            at_ns=T,
            rule_ok=True,
            valid=True,
            sample_ns=T,
            demand_ns=T,
            scrape_ns=T,
            windows=counts,
            claimable_attempts=0,
            oldest_claimable_age_seconds=0,
            firing=["DeadlineBudgetBurn"] if blocked else [],
        )
    ]
    for key in ("admissions", "successes"):
        sample[key] = {"set": sample[key], "delete": []}
    return sample


def test_both_replays_and_width_boundaries():
    data = fixture_data()
    observed = j3.replay(data, [visibility(data)])
    assert observed["alert_inputs"]["ambiguous_count"] == 1
    assert observed["committed_timeout_keys"] == ["w19"]
    published = observed["published_attainment"]
    assert (published["on_time"], published["eligible"], published["incomplete"]) == (109, 128, 16)
    assert published["width_sensitivity"] == 1
    assert published["raw_durable_latency_ns"]["w20"] == j3.DEADLINE - CLOSING
    assert (
        j3.replay(data, [visibility(data, blocked=False)])["alert_inputs"]["ambiguous_count"] == 0
    )


def test_success_visible_after_statement_timestamp():
    data = fixture_data()
    sample = visibility(data)
    # Snapshot acquisition can follow the statement timestamp.
    sample["successes"]["set"]["w127"] = 1
    assert j3.replay(data, [sample])["alert_inputs"]["ambiguous_count"] == 1


def test_joint_status_and_evaluation_integrity():
    data = fixture_data()
    sample = visibility(data)
    broken = copy.deepcopy(sample)
    broken["windows"]["10m"]["bad"] = 18  # Same ambiguous identity appears in both windows.
    with pytest.raises(ValueError, match="nested windows"):
        j3.replay(data, [broken])
    with pytest.raises(ValueError, match="missing visibility"):
        j3.replay(data, [])
    data["evaluations"][0]["firing"] = []
    with pytest.raises(ValueError, match="actual rule transitions"):
        j3.replay(data, [sample])


def test_nonzero_visibility_band_cannot_cross_threshold():
    data = fixture_data()
    workflow = data["workflows"][17]
    workflow["state"] = "SUCCEEDED"
    created = data["plan"][17]["scheduled_ns"] + j3.DEADLINE - CLOSING
    data["history"].append(
        dict(workflow_id="w17", revision=1, new_state="SUCCEEDED", created_ns=created)
    )
    sample = visibility(data)
    for counts in sample["windows"].values():
        counts["bad"] -= 1
    data["evaluations"][0]["firing"] = []
    rows, _ = j3.reconcile(data)
    band = j3.cohort_band(
        rows, sample, sample["admissions"]["set"], sample["successes"]["set"], SETUP
    )
    assert (band["2m"]["lower"], band["2m"]["upper"]) == (17, 18)
    assert j3.replay(data, [sample])["alert_inputs"]["ambiguous_count"] == 0


@pytest.mark.parametrize("duplicate", [False, True])
def test_bad_final_history_fails(duplicate):
    data = fixture_data()
    row = data["history"][0]
    if duplicate:
        data["history"].append(row)
    else:
        data["history"].remove(row)
    with pytest.raises(ValueError, match="broken terminal history"):
        j3.reconcile(data)


@pytest.mark.parametrize(
    ("defect", "message"),
    [
        (None, None),
        ("catalog", "exporter role diagnostics differ in pg_db_role_setting"),
        ("catalog_jit", "exporter role diagnostics differ in pg_db_role_setting"),
        ("placeholder", "auto_explain is missing or misconfigured in the exporter session"),
        ("duration", "slow statement logging is misconfigured in the exporter session"),
        ("live_jit", "JIT is not disabled in the exporter session"),
    ],
)
def test_exporter_diagnostics_verify_role_defaults_and_loaded_module(tmp_path, defect, message):
    settings = [
        "statement_timeout=2s",
        "default_transaction_read_only=on",
        "jit=off",
        "session_preload_libraries=auto_explain",
        "auto_explain.log_min_duration=500ms",
        "auto_explain.log_analyze=on",
        "auto_explain.log_nested_statements=on",
        "auto_explain.log_timing=off",
        "log_min_duration_statement=500ms",
    ]
    duration = {"vartype": "integer", "unit": "ms", "setting": "500"}
    session = {
        "auto_explain.log_min_duration": duration,
        "log_min_duration_statement": duration,
        "jit": {"vartype": "bool", "unit": None, "setting": "off"},
    }
    if defect == "catalog":
        settings.remove("session_preload_libraries=auto_explain")
    if defect == "catalog_jit":
        settings.remove("jit=off")
    if defect == "placeholder":
        session["auto_explain.log_min_duration"] = {
            "vartype": "string",
            "unit": None,
            "setting": "500",
        }
    if defect == "duration":
        session["log_min_duration_statement"] = {**duration, "setting": "-1"}
    if defect == "live_jit":
        session["jit"]["setting"] = "on"

    calls = []

    def postgres(query, *, role="postgres"):
        calls.append(role)
        if query.startswith("ALTER ROLE"):
            assert role == "postgres"
            assert all(
                line.startswith("ALTER ROLE j3_exporter SET ") for line in query.splitlines()
            )
            return SimpleNamespace(stdout="")
        if role == "postgres":
            assert "pg_catalog.pg_db_role_setting" in query and "s.setdatabase = 0" in query
            return SimpleNamespace(stdout=json.dumps(settings))
        assert "pg_catalog.pg_settings" in query and "session_preload_libraries" not in query
        assert "'jit'" in query
        return SimpleNamespace(stdout=json.dumps(session))

    driver = SimpleNamespace(output=tmp_path, postgres=postgres)
    if message:
        with pytest.raises(ValueError, match=message):
            enable_exporter_diagnostics(driver)
    else:
        enable_exporter_diagnostics(driver)
    assert calls == ["postgres", "postgres", "j3_exporter"]
    evidence = json.loads((tmp_path / "exporter-role-settings.json").read_bytes())
    assert evidence["observed"] == sorted(settings)
    assert evidence["session_settings"] == session


@pytest.mark.skipif(not os.getenv("J3_SQL_CONTAINER"), reason="hosted PostgreSQL fixture required")
def test_postgres_statement_visibility(tmp_path):
    psql = [
        "docker",
        "exec",
        "-i",
        os.environ["J3_SQL_CONTAINER"],
        "stdbuf",
        "-oL",
        "psql",
        "-U",
        "postgres",
        "-XqAt",
        "-v",
        "ON_ERROR_STOP=1",
    ]

    def run(sql):
        return subprocess.run(
            psql, input=sql, text=True, capture_output=True, timeout=30, check=True
        ).stdout.strip()

    root = HERE.parents[2]
    for migration in sorted((root / "migrations").glob("*.up.sql"))[:3]:
        run(migration.read_text())
    monitor = (HERE / "exporter/monitor.sql").read_text()
    # Only the test clock is fixed, so a real statement snapshot can hit exact edges.
    run(
        monitor.replace("statement_timestamp()", "'2026-01-01T00:20:00Z'::timestamptz")
        .replace(":'namespace'", "'target'")
        .replace(":'password'", "'fixture'")
        .replace(":'u_ns'", str(SETUP))
    )
    run(
        "INSERT INTO engine.workflow_definitions (definition_id,version,definition_hash,graph)"
        " VALUES ('fixture',1,'hash','{}');"
    )
    data = fixture_data()
    seeded = []
    for plan, workflow in zip(data["plan"], data["workflows"], strict=True):
        key = plan["submission_key"]
        state = workflow["state"] if key not in {"w18", "w127"} else "WAITING_ACTIVITY"
        seeded.append(
            f"INSERT INTO j2_monitor.j3_plan VALUES ('target','{key}',{plan['scheduled_ns']});"
            "INSERT INTO engine.workflow_executions (workflow_id,namespace,submission_key,"
            "submission_payload_hash,"
            "definition_id,definition_version,partition_id,state,revision) VALUES "
            f"('{key}','target','{key}','hash','fixture',1,0,'{state}',1);"
        )
    run("\n".join(seeded))

    def final(row):
        stamp, fraction = divmod(row["created_ns"], j3.SECOND)
        return (
            "INSERT INTO engine.transition_history (transition_id,workflow_id,revision,"
            "actor_kind,actor_id,new_state,reason,created_at) VALUES "
            f"(gen_random_uuid(),'{row['workflow_id']}',{row['revision']},'fixture','fixture',"
            f"'{row['new_state']}','J3_TEST',to_timestamp({stamp}.{fraction:09d}));"
        )

    closed = next(row for row in data["history"] if row["workflow_id"] == "w18")
    run(
        "\n".join(
            final(row) for row in data["history"] if row is not closed and row["created_ns"] <= T
        )
    )
    with subprocess.Popen(psql, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as writer:
        try:
            assert writer.stdin is not None and writer.stdout is not None
            writer.stdin.write(
                "BEGIN; UPDATE engine.workflow_executions SET state='SUCCEEDED',revision=2 "
                "WHERE workflow_id='w18';\n" + final(closed) + "\n\\echo held\n"
            )
            writer.stdin.flush()
            assert writer.stdout.readline().strip() == "held"
            before = json.loads(run("SELECT j2_monitor.j3_sample();"))
            writer.stdin.write("COMMIT;\n")
            writer.stdin.close()
            assert writer.wait(timeout=10) == 0
        finally:
            if writer.poll() is None:
                writer.kill()
    after = json.loads(run("SELECT j2_monitor.j3_sample();"))
    for blocked, actual in ((True, before), (False, after)):
        expected = visibility(data, blocked)
        assert actual["windows"] == expected["windows"]
        assert actual["admissions"] == expected["admissions"]["set"]
        assert actual["successes"] == expected["successes"]["set"]
        assert actual["sample_ns"] == T and actual["u_ns"] == SETUP
        projected = {**expected, "windows": actual["windows"]}
        assert j3.replay(data, [projected])["alert_inputs"]["ambiguous_count"] == int(blocked)
    stalled = json.loads(
        run(
            "BEGIN; UPDATE engine.workflow_executions SET state='WAITING_ACTIVITY';"
            "SELECT j2_monitor.j3_sample(); ROLLBACK;"
        )
    )
    assert stalled["successes"] == {}
    assert all(c["bad"] == c["eligible"] > 0 for c in stalled["windows"].values())

    run(
        (HERE / "foreign.sql")
        .read_text()
        .replace(":'foreign'", "'foreign'")
        .replace(":'password'", "'fixture'")
    )
    seed_before = int(run("SELECT (extract(epoch FROM clock_timestamp()) * 1e9)::bigint;"))
    foreign_sample = seed_before + 20 * j3.SECOND
    created = (foreign_sample - 40 * j3.SECOND) / j3.SECOND
    run(f"""
        INSERT INTO j2_monitor.j3_plan
        SELECT 'foreign', 'foreign-' || i, {foreign_sample} - (i + 30) * 1000000000::bigint
        FROM generate_series(1, 101) s(i);
        SELECT j2_monitor.j3_seed(submission_key) FROM j2_monitor.j3_plan WHERE namespace='foreign';
        UPDATE engine.workflow_executions SET created_at=to_timestamp({created})
          WHERE namespace='foreign';
        UPDATE engine.activity_attempts SET created_at=to_timestamp({created})
          WHERE workflow_id LIKE 'j3-fixture-foreign-%';
        """)
    sql = (HERE / "foreign-witness.sql").read_text()

    def positive_witness(sample):
        return json.loads(
            run(
                sql.replace(":'foreign'", "'foreign'")
                .replace(":'sample_ns'", str(sample))
                .replace(":'u_ns'", str(SETUP))
            )
        )

    assert positive_witness(seed_before)["claimable_attempts"] == 0
    witness = positive_witness(foreign_sample)
    assert witness["namespace"] == "foreign" and witness["sample_ns"] == foreign_sample
    assert witness["claimable_attempts"] == 101
    assert witness["oldest_claimable_age_ns"] == 40 * j3.SECOND
    assert witness["windows"] == {
        "2m": {"eligible": 92, "bad": 92},
        "10m": {"eligible": 101, "bad": 101},
    }

    settings = "SHOW session_preload_libraries; SHOW log_min_duration_statement; SHOW jit;"
    owner_settings = run(settings)

    def postgres(query, *, role="postgres"):
        return subprocess.run(
            [*psql, "-U", role, "-d", "postgres"],
            input=query,
            text=True,
            capture_output=True,
            timeout=10,
            check=True,
        )

    enable_exporter_diagnostics(SimpleNamespace(output=tmp_path, postgres=postgres))
    assert run(settings) == owner_settings
    assert postgres("SHOW jit;", role="j3_exporter").stdout.strip() == "off"
    assert (
        postgres(
            "SELECT pg_has_role(current_user, 'pg_read_all_settings', 'MEMBER');",
            role="j3_exporter",
        ).stdout.strip()
        == "f"
    )
    postgres("SELECT pg_sleep(0.55) /* j3_role_diagnostic */;", role="j3_exporter")
    expected_logs = [
        "Query Text: SELECT pg_sleep(0.55) /* j3_role_diagnostic */",
        "statement: SELECT pg_sleep(0.55) /* j3_role_diagnostic */",
    ]
    for _ in range(20):
        result = subprocess.run(
            ["docker", "logs", os.environ["J3_SQL_CONTAINER"]],
            text=True,
            capture_output=True,
            timeout=10,
            check=True,
        )
        if all(entry in result.stdout + result.stderr for entry in expected_logs):
            break
        time.sleep(0.1)
    else:
        pytest.fail("role-scoped slow statement or auto_explain evidence missing")
