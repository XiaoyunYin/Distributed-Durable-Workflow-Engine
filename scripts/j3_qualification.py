"""Qualify the J3 harness on a disposable hosted kind cluster."""

import hashlib
import json
import os
import secrets
import subprocess
import time
from contextlib import ExitStack, suppress
from http.client import HTTPException
from math import ceil
from statistics import median

import dur053_k3 as kind
from j2_checker import verify_binary
from j2_offline import utc_ns
from j3_admission_diagnostics import arm_resources
from j3_diagnostics import retain_postgres_log, runner_pressure
from j3_episode import (
    COMMIT_NS,
    RULE_SETTLE_SECONDS,
    CheckerError,
    EpisodeFailure,
    Sampler,
    Sender,
    backlog,
    check_full_volume,
    drain,
    duration_witness,
    install_plan,
    outage_commits,
    plan,
    require_behavior,
    scale,
    scrape_access,
    verify_outage,
    wait_at,
    worker_ledgers,
)
from j3_evidence import (
    foreign_isolation,
    foreign_positive,
    jsonl,
    notifications,
    slot_occupancy,
    target_only_attainment,
)
from j3_overlay import ALERTMANAGER, APP, EXPORTER, FOREIGN, GRAFANA, J3, NS, PROM
from j3_pilot import (
    DB,
    Pilot,
    bootstrap,
    clock,
    collect_replay_inputs,
    dashboard_evidence,
    database_rows,
    endpoint_ready,
    hosted_arguments,
    install_sender,
    json_request,
    locked_images,
    prepare_overlay,
    prom,
    receiver_log,
    request,
    retain_monitor_logs,
)
from j3_prometheus import epoch_ns
from j3_replay import DEADLINE, DOMAIN, SECOND, replay, require
from j3_runner_gate import campaign_id, runner_cpu, verify_admission
from j3_schedule import (
    ARRIVAL_RATE,
    BASE_STOP,
    CALIBRATION_ARMS,
    CONFIRMATION_ARMS,
    FORECAST_CAP,
    QUALIFICATION_CAP,
    derive,
    derive_from_outages,
    measurement_forecast,
    qualification_forecast,
    recovery_age_evidence,
    recovery_evidence,
    restored_rate,
    verify_seal,
)
from j3_slots import OUTAGE_SLOT_BOUNDARIES, SCRAPE_SLOT_BOUNDARIES

ALERT_SETUP_SECONDS = 30


class Qualification(Pilot):
    def __init__(self, output, scratch, checker):
        super().__init__(output, scratch)
        self.job_scratch = scratch
        self.checker = checker.resolve()

    def episode_objects(self, objects):
        super().episode_objects(objects)
        for obj in objects:
            if obj["kind"] == "ScaledObject":
                annotations = obj["metadata"]["annotations"]
                annotations.pop("autoscaling.keda.sh/paused-replicas")
                annotations["autoscaling.keda.sh/paused"] = "true"
        (observer,) = [
            o for o in objects if o["kind"] == "Deployment" and o["metadata"]["name"] == "observer"
        ]
        spec = observer["spec"]["template"]["spec"]
        spec["volumes"].append(
            {
                "name": "j3-witness",
                "persistentVolumeClaim": {"claimName": "witness", "readOnly": True},
            }
        )
        spec["containers"][0]["volumeMounts"].append(
            {
                "name": "j3-witness",
                "mountPath": "/j3-witness",
                "readOnly": True,
            }
        )
        kind.save(self.output / "application-rendered.json", objects)

    def enable_commit_timestamps(self):
        name = kind.CLUSTER + "-postgres"
        self.sql("ALTER SYSTEM SET track_commit_timestamp = on;")
        self.command(["docker", "restart", name], timeout=30)
        self.until(
            lambda: (
                self.command(
                    ["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "durable"],
                    record=False,
                    allowed=True,
                ).returncode
                == 0
            ),
            60,
        )
        require(self.sql("SHOW track_commit_timestamp;") == "on", "commit timestamps are off")

    def fresh_episode(self, addresses, pins, target, binary):
        self.target_namespace = target["namespace"]
        self.scratch = self.job_scratch / self.target_namespace
        self.scratch.mkdir()
        self.monitor_password, self.fixture_password = (secrets.token_hex(32) for _ in range(2))
        self.setup_episode("kind", addresses)
        for namespace in (NS, "j3-unrelated"):
            self.k("create", "namespace", namespace)
        setup = prepare_overlay(self, addresses["192.0.2.1"], pins, target, diagnostics=True)
        observer, sender = install_sender(self, binary)
        self.until(lambda: not self.inspect(APP, "hpa")["items"], 30)
        sessions = self.query(
            "SELECT application_name || '|' || count(*) FROM pg_stat_activity "
            "WHERE usename='j3_exporter' GROUP BY application_name;"
        )
        maximum = int(self.query("SHOW max_connections;"))
        kind.save(
            self.output / "db-sessions.json", {"monitor": sessions, "max_connections": maximum}
        )
        require(sessions == "j3-db-exporter|1" and maximum == 100, "live monitor budget differs")
        return setup, observer, sender

    def reset(self):
        self.k(
            "delete", "namespace", NS, "j3-unrelated", "--wait=true", "--timeout=120s", timeout=130
        )
        self.reset_episode(APP, DB)
        self.sql("DROP ROLE j3_exporter, j3_fixture;")

    def cleanup(self):
        # The shared cleanup probes for the installed kind binary and kubeconfig.
        self.scratch = self.job_scratch
        return super().cleanup()


def foreign_plan(origin):
    rows = plan(FOREIGN, "fixture", origin + 600 * SECOND, 300)
    return [
        {**row, "submission_key": f"foreign-{i}", "scheduled_ns": row["scheduled_ns"] - DEADLINE}
        for i, row in enumerate(rows)
    ]


def seed_foreign(driver, rows, check, end_ns):
    """Use one fixture connection and commit each preinstalled identity independently."""
    path = driver.scratch / "foreign-seed.sql"
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            now = row["scheduled_ns"] + DEADLINE
            sec, ns = divmod(now, SECOND)
            stream.write(f"SELECT pg_sleep_until(to_timestamp({sec}.{ns:09d})) \\g /dev/null\n")
            stream.write(f"SELECT j2_monitor.j3_seed('{row['submission_key']}');\n")
    # Skip the long wait before the first seed using the host episode clock.
    wait_at(rows[0]["scheduled_ns"] + DEADLINE - SECOND, check)
    command = [
        "docker",
        "exec",
        "-i",
        "-e",
        f"PGPASSWORD={driver.fixture_password}",
        kind.CLUSTER + "-postgres",
        "psql",
        "-h",
        "127.0.0.1",
        "-U",
        "j3_fixture",
        "-XqAt",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        DB,
    ]
    with (
        path.open("rb") as sql,
        (driver.output / "foreign-seed.jsonl").open("wb") as output,
        (driver.output / "foreign-seed.stderr").open("wb") as error,
        subprocess.Popen(command, stdin=sql, stdout=output, stderr=error, env=driver.env) as seed,
    ):
        try:
            while seed.poll() is None:
                check()
                require(time.time_ns() < end_ns + 2 * SECOND, "foreign fixture did not finish")
                time.sleep(0.1)
            require(seed.returncode == 0, "foreign fixture writer failed")
        finally:
            if seed.poll() is None:
                seed.terminate()
                driver.query(
                    "SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
                    "WHERE usename='j3_fixture';"
                )
                seed.wait(timeout=5)


def verify_foreign(driver, data, result, fixture, origin, sampler, events):
    rows = json.loads(
        driver.query(f"""
SELECT jsonb_agg(jsonb_build_object(
    'workflow_id', w.workflow_id, 'revision', h.revision, 'key', w.submission_key,
    'commit_seen_ns', {COMMIT_NS % "h"}
)) FROM engine.workflow_executions w JOIN engine.transition_history h
  ON h.workflow_id=w.workflow_id AND h.revision=1
WHERE w.namespace='{FOREIGN}';
""")
    )
    kind.save(driver.output / "foreign-commits.json", rows)
    seeded = list(jsonl(driver.output / "foreign-seed.jsonl"))
    sql = (J3 / "foreign-witness.sql").read_text(encoding="utf-8")
    widths = result["published_attainment"]
    deadlines = {r["submission_key"]: r["scheduled_ns"] + DEADLINE for r in fixture}
    require(
        rows
        and len(rows) == len(seeded) == len(fixture)
        and {r["workflow_id"] for r in rows} == {r["workflow_id"] for r in seeded}
        and {r["key"] for r in rows} == deadlines.keys()
        and all(deadlines[r["key"]] <= r["commit_seen_ns"] < origin + 900 * SECOND for r in rows),
        "missing or late foreign commits",
    )
    quiet = [
        r
        for r in data["evaluations"]
        if r["valid"]
        and not r["firing"]
        and origin + 840 * SECOND <= r["sample_ns"] < origin + 900 * SECOND
    ]
    require(quiet, "no valid quiet evaluation during the foreign control")
    require(
        all(
            r["sql_oldest_claimable_seconds"] <= 28.540
            and r["oldest_claimable_age_seconds"] <= 28.540
            and r.get("ready_slots") == 4
            for r in quiet
        ),
        "target-only SQL age, scoped demand or four scraped slots disagree during foreign load",
    )
    at = quiet[0]["sample_ns"]
    witness = json.loads(
        driver.query(
            sql.replace(":'foreign'", repr(FOREIGN))
            .replace(":'sample_ns'", repr(str(at)))
            .replace(":'u_ns'", repr(str(widths["setup_width_ns"])))
        )
    )
    kind.save(driver.output / "foreign-witness-raw.json", witness)
    data["slot_events"] = events
    slots = sampler.latest["slots"]
    ready_by_uid = sampler.latest["witness"]
    require(
        len(ready_by_uid) == 2 and ready_by_uid.keys() == sampler.inventories.keys(),
        "foreign workers changed or a slot witness is missing",
    )
    isolation = foreign_isolation(data, rows, FOREIGN)
    occupancy = slot_occupancy(data, slots, data["namespace"], exclude_foreign=False)
    target_only_slots = slot_occupancy(data, slots, data["namespace"], exclude_foreign=True)
    target_only_published = target_only_attainment(
        data,
        data["namespace"],
        max(widths["setup_width_ns"], widths["closing_width_ns"]),
        data["end_ns"],
    )
    kind.save(
        driver.output / "foreign-verdict.json",
        {
            "synthetic_foreign_database_state": True,
            "positive": foreign_positive(witness, quiet, rows, FOREIGN),
            "isolation": isolation,
            "slots_with_foreign": occupancy,
            "slots_without_foreign": target_only_slots,
            "published_with_foreign": {k: widths[k] for k in target_only_published},
            "published_without_foreign": target_only_published,
        },
    )
    require(
        occupancy == target_only_slots
        and target_only_published == {k: widths[k] for k in target_only_published},
        "including the foreign fixture changed target occupancy or published attainment",
    )


def verify_scrape(evaluations, transitions, origin, cut, restored):
    invalid = [row for row in evaluations if not row["valid"]]
    require(
        invalid
        and all(cut <= r["at_ns"] <= restored + 10 * SECOND and r["up"] == 0 for r in invalid)
        and all(r["up"] == 1 for r in evaluations if r["valid"]),
        "sample gap outside the scrape block or unrelated telemetry failure",
    )
    require_behavior(
        len(transitions) == 2
        and [r["alert"] for r in transitions] == ["ScrapeAbsent"] * 2
        and [r["firing"] for r in transitions] == [True, False],
        "missing or extra scrape-loss transitions",
    )
    fire, clear = (row["at_ns"] for row in transitions)
    require_behavior(
        cut + 5 * SECOND <= fire <= cut + 20 * SECOND
        and restored <= clear <= restored + 15 * SECOND
        and origin + 604 * SECOND <= fire <= origin + 621 * SECOND
        and origin + 659 * SECOND <= clear <= origin + 676 * SECOND,
        "scrape-loss transition exceeded its registered bound",
    )


def check_slot_witness(sample, initial_workers):
    if sample["phase"] == "healthy":
        require(
            set(sample["witness"]) == initial_workers and len(sample["slots"]) == 4,
            "missing healthy worker slot witness",
        )
    elif sample["phase"] == "restored":
        require(
            len(sample["slots"]) == 4 and not initial_workers.intersection(sample["witness"]),
            "missing restored worker slot witness",
        )


def load_and_observe(driver, arm, origin, stop, observe_end, sender, sampler, fixture):
    initial_workers = set(sampler.latest["witness"])
    down_at, restore_at, ready_by = OUTAGE_SLOT_BOUNDARIES

    def check():
        sampler.check()
        sender.check_running()
        check_slot_witness(sampler.latest, initial_workers)

    fault = {}
    with sender.run():
        driver.until(
            lambda: (
                check() is None
                and driver.python(
                    APP,
                    "observer",
                    "import pathlib; print(pathlib.Path('/evidence/j3-armed').exists())",
                    record=False,
                )
                == "True"
            ),
            max(0, (origin - time.time_ns()) / SECOND),
        )
        if arm == "outage":
            fault["down"] = scale(driver, 0, origin + down_at * SECOND, check)
            wait_at(origin + 660 * SECOND, check)
            require(
                sampler.latest and not sampler.latest["pods"], "worker pods survived past 660 s"
            )
            require(bool(prom("j3_claimable_attempts > 0")), "missing live outage demand")
            wait_at(origin + 899 * SECOND, check)
            fault["backlog"] = backlog(driver)
            fault["restore"] = scale(driver, 2, origin + restore_at * SECOND, check)
            wait_at(origin + ready_by * SECOND, check)
            require(len(sampler.latest["slots"]) == 4, "restored Ready slots missing by 960 s")
        elif arm == "scrape_loss":
            cut_at, reopen_at = SCRAPE_SLOT_BOUNDARIES
            route = tuple(driver.pod(NS, role)["status"]["podIP"] for role in (PROM, EXPORTER))
            fault["cut"] = scrape_access(driver, route, False, origin + cut_at * SECOND, check)
            fault["reopen"] = scrape_access(driver, route, True, origin + reopen_at * SECOND, check)
        elif arm == "foreign":
            seed_foreign(driver, fixture, check, origin + 900 * SECOND)

        wait_at(stop, check)
        quiet = drain(driver, sender, sampler, observe_end, check)
        if arm != "outage" or stop - origin >= BASE_STOP * SECOND:
            wait_at(observe_end, check)
        final = duration_witness(driver)
        require(
            final["revision_hash"] == quiet["revision_hash"]
            and final["unpublished"] == final["active_claims"] == 0
            and sampler.latest["witness"] == quiet["witness"],
            "durable identities changed after quiet",
        )
    return quiet, fault


def check_final_window(evaluations, transitions, origin, bounds):
    for name, key in zip(DOMAIN, ("oldest_clear_by_s", "burn_clear_by_s"), strict=True):
        observed = [r for r in transitions if r["alert"] == name]
        require_behavior(
            len(observed) == 2
            and observed[0]["firing"]
            and not observed[1]["firing"]
            and origin + bounds["restore_s"] * SECOND
            < observed[1]["at_ns"]
            <= origin + bounds[key] * SECOND,
            f"{name} failed its sealed clearance bound",
        )
    window = [
        r
        for r in evaluations
        if origin + bounds["final_healthy_window_s"][0] * SECOND
        <= r["at_ns"]
        < origin + bounds["final_healthy_window_s"][1] * SECOND
    ]
    require_behavior(
        len(window) == 120
        and all(
            r["valid"]
            and not r["firing"]
            and min(r["windows"]["2m"]["eligible"], r["windows"]["10m"]["eligible"]) >= 100
            for r in window
        ),
        "missing fresh sustained recovery",
    )


def settled_alert_state(target):
    # Read the two raw evaluation timestamps behind the live settle margin.
    at_ns = (time.time_ns() - RULE_SETTLE_SECONDS * SECOND) // 1_000_000 * 1_000_000
    at = at_ns / SECOND
    labels = ",".join(f"{key}={json.dumps(value)}" for key, value in target.items())
    rows = prom(f'{{{labels},__name__=~"j3:expected|j3:valid"}}[12s]', at=at)
    samples = {row["metric"]["__name__"]: row["values"][-2:] for row in rows}
    expected = samples.get("j3:expected", [])
    if len(rows) != 2 or len(expected) != 2 or samples.get("j3:valid") != expected:
        return None
    (first, first_value), (last, last_value) = expected
    first_ns, last_ns = epoch_ns(first), epoch_ns(last)
    if (
        first_value != "1"
        or last_value != "1"
        or last_ns - first_ns != 5 * SECOND
        or not 0 <= at_ns - last_ns < 5 * SECOND
    ):
        return None
    up = f'up{{{labels},job="j3"}}'
    if not prom(f"({up} == 1) and (time() - timestamp({up}) <= 5)", at=at):
        return None
    alerts = json_request(19093, "/api/v2/alerts")
    require(isinstance(alerts, list), "invalid Alertmanager alert list")
    if any(
        all(alert["labels"].get(key) == value for key, value in target.items()) for alert in alerts
    ):
        return None
    if prom(f'ALERTS{{{labels},alertstate=~"pending|firing"}}'):
        return None
    return {"query_ns": at_ns, "valid_evaluations_ns": [first_ns, last_ns]}


def wait_for_settled_alerts(driver, target):
    evidence = None

    def ready():
        nonlocal evidence
        try:
            evidence = settled_alert_state(target)
        except (OSError, HTTPException):
            return False
        return evidence is not None

    try:
        with driver.forward(ALERTMANAGER, ["19093:9093"]):
            driver.until(ready, ALERT_SETUP_SECONDS)
    except TimeoutError as error:
        with suppress(Exception):
            receiver_log(driver)
        raise ValueError(
            f"setup alert state did not settle within {ALERT_SETUP_SECONDS} seconds"
        ) from error
    kind.save(driver.output / "alert-arming.json", evidence)


def episode_error_status(error):
    if isinstance(error, CheckerError):
        return error.outcome.upper()
    return "FAIL" if isinstance(error, EpisodeFailure) else "INVALID"


def check_episode(outcome, behavior, delivery):
    first_error = None
    for name, check in (("behavior", behavior), ("delivery", delivery)):
        try:
            check()
        except Exception as error:
            status = episode_error_status(error)
            outcome[name] = {"status": status, "error": str(error)}
            if first_error is None or (
                status == "FAIL" and not isinstance(first_error, EpisodeFailure)
            ):
                first_error = error
        else:
            outcome[name] = {"status": "PASS"}
    if first_error is not None:
        raise first_error


def require_calibration_recovery(recovery):
    require_behavior(
        not recovery["oldest_refires_before_stop"],
        "OldestClaimable re-fired before calibration offers stopped",
    )
    require_behavior(
        recovery["terminal_endpoint"] is not None and recovery["age_endpoint"] is not None,
        "calibration recovery endpoint unobserved",
    )


def run_episode(driver, pins, target, arm, run, setup, observer, binary, duration, bounds, outcome):
    with ExitStack() as stack:
        for role, port in ((PROM, "19090:9090"), (GRAFANA, "13000:3000"), (EXPORTER, "19188:9188")):
            stack.enter_context(driver.forward(role, [port]))
        driver.until(lambda: endpoint_ready(19090, "/-/ready"), 30)
        driver.until(lambda: endpoint_ready(13000, "/api/dashboards/uid/j3-observability"), 45)
        sampler = stack.enter_context(Sampler(driver, pins, arm))
        driver.until(lambda: sampler.latest and len(sampler.latest["slots"]) == 4, 10)
        initial = worker_ledgers(driver, sampler.inventories.copy(), sampler.latest["witness"])
        require(
            len(initial) == 2 and all(r["event"] == "session" for r in initial),
            "episode witness volume contains prior activity",
        )
        stack.enter_context(arm_resources(driver, APP))
        wait_for_settled_alerts(driver, target)
        origin = (time.time_ns() // SECOND + 20) * SECOND
        sampler.origin = origin
        stop = origin + duration * SECOND
        observe_end = stop + 600 * SECOND
        rows = plan(target["namespace"], run, origin, duration)
        fixture = foreign_plan(origin) if arm == "foreign" else []
        install_plan(driver, rows + fixture)
        sender = Sender(driver, observer, binary, rows, run, origin, stop)
        kind.save(
            driver.output / "timing.json",
            {
                "origin_ns": origin,
                "stop_ns": stop,
                "observe_end_ns": observe_end,
                "arrival_count": len(rows),
            },
        )
        end = None
        try:
            quiet, fault = load_and_observe(
                driver, arm, origin, stop, observe_end, sender, sampler, fixture
            )
            end = time.time_ns()
            closing = clock(driver)
            accepted, sender_plan = sender.completed()
            if arm == "foreign":
                dashboard_evidence(driver, target, {r["workflow_id"] for r in accepted})
            evaluations = collect_replay_inputs(driver, target, origin, end)
            if arm != "scrape_loss":
                require(all(r["valid"] for r in evaluations), "unexpected rule input gap")
            durable = database_rows(driver)
            kind.save(driver.output / "durable-rows.json", durable)
            data = {
                **durable,
                "namespace": target["namespace"],
                "start_ns": origin,
                "end_ns": end,
                "clock": {"setup": setup, "closing": closing},
                "plan": rows + fixture,
                "journal": accepted,
                "history": [
                    {**r, "created_ns": utc_ns(r["created_at"])} for r in durable["history"]
                ],
                "first_eval_ns": evaluations[0]["at_ns"],
                "last_eval_ns": evaluations[-1]["at_ns"],
                "evaluations": evaluations,
            }
            kind.save(driver.output / "replay-input.json", data)
            result = replay(data, jsonl(driver.output / "visibility.jsonl"))
            kind.save(driver.output / "replay.json", result)
            attainment = result["published_attainment"]
            require(
                attainment["eligible"] == len(rows) and attainment["incomplete"] == 0,
                "published replay did not reconcile the target population",
            )
            transitions = result["alert_inputs"]["transitions"]

            def delivery():
                receiver = list(jsonl(receiver_log(driver)))
                evidence = notifications(receiver, transitions, target, origin, end)
                kind.save(driver.output / "notification-pairs.json", evidence["matches"])
                kind.save(
                    driver.output / "setup-incidents.json",
                    {"setup_incidents": evidence["setup_incidents"]},
                )

            def behavior():
                if arm == "outage":
                    restore = fault["restore"]
                    age = recovery_age_evidence(evaluations, transitions, restore, stop)
                    first_below = age["first_clear_input"]
                    clears = []
                    for row in transitions:
                        if row["firing"]:
                            continue
                        clear = {
                            **row,
                            "seconds_from_origin": (row["at_ns"] - origin) / SECOND,
                            "seconds_from_restore": (row["at_ns"] - restore) / SECOND,
                        }
                        if row["alert"] == DOMAIN[0]:
                            clear["first_clear_input_ns"] = (
                                first_below["at_ns"] if first_below else None
                            )
                        clears.append(clear)
                    kind.save(
                        driver.output / "actual-clears.json",
                        clears,
                    )
                    evidence = outage_commits(
                        driver, origin, fault["down"], restore, fault["backlog"]
                    )
                    if bounds is None:
                        recovery = recovery_evidence(evidence, evaluations, transitions, stop)
                        kind.save(driver.output / "calibration-recovery.json", recovery)
                        require_calibration_recovery(recovery)
                    witness = verify_outage(
                        evidence,
                        evaluations,
                        list(jsonl(driver.output / "ready-slots.jsonl")),
                        bounds["catchup_before_s"] if bounds else None,
                    )
                    kind.save(driver.output / "outage-witness.json", witness)
                    restored_rate(evidence)
                    if bounds:
                        check_final_window(evaluations, transitions, origin, bounds)
                else:
                    require_behavior(
                        attainment["attainment"] >= 0.99, "healthy attainment below 99%"
                    )
                    if arm == "scrape_loss":
                        verify_scrape(
                            evaluations, transitions, origin, fault["cut"], fault["reopen"]
                        )
                    else:
                        require_behavior(not transitions, "unexpected healthy control alert")
                events = worker_ledgers(
                    driver, sampler.inventories.copy(), sampler.latest["witness"]
                )
                if bounds:
                    check_full_volume(driver, data, sender_plan, quiet, events)
                elif arm == "foreign":
                    verify_foreign(driver, data, result, fixture, origin, sampler, events)

            check_episode(outcome, behavior, delivery)
            return origin, end
        finally:
            if not (driver.output / "durable-rows.json").exists():
                with suppress(Exception):
                    kind.save(driver.output / "durable-rows.json", database_rows(driver))
            if not (driver.output / "notifications.jsonl").exists():
                with suppress(Exception):
                    receiver_log(driver)
            if not (driver.output / "visibility.jsonl").exists():
                with suppress(Exception):
                    (driver.output / "visibility.jsonl").write_bytes(
                        request(19188, "/visibility.jsonl")
                    )
            if not (driver.output / "prometheus-raw.json").exists() and time.time_ns() > origin:
                with suppress(Exception):
                    collect_replay_inputs(driver, target, origin, end or time.time_ns())


def run_qualification(driver, args, addresses, pins, campaign, sealed=None):
    root = args.evidence
    stage = args.stage
    runs = {}
    verdict = {"status": "INVALID", "stage": stage, "campaign": campaign, "episodes": runs}
    started = int(os.environ["J3_JOB_STARTED"])
    first_origin = None

    def save_verdict():
        kind.save(root / "verdict.json", verdict)

    def episode(name, arm, duration, bounds=None, reset=True):
        nonlocal first_origin
        out = root / name
        out.mkdir()
        driver.output = out
        (out / "sender.sha256").write_bytes((root / "sender.sha256").read_bytes())
        target = {
            "campaign": campaign,
            "namespace": f"j3-target-{name}",
        }
        runs[name] = {
            "status": "RUNNING",
            "target": target,
            "behavior": {"status": "UNPERFORMED"},
            "delivery": {"status": "UNPERFORMED"},
        }
        kind.save(
            out / "run.json",
            {
                **json.loads((root / "run.json").read_bytes()),
                "episode": name,
                "target": target,
            },
        )
        save_verdict()
        start = time.time_ns()
        with runner_pressure(out):
            try:
                setup, observer, binary = driver.fresh_episode(addresses, pins, target, args.sender)
                origin, end = run_episode(
                    driver,
                    pins,
                    target,
                    arm,
                    f"j3-{name}",
                    setup,
                    observer,
                    binary,
                    duration,
                    bounds,
                    runs[name],
                )
            except Exception:
                retain_postgres_log(driver, start, required=False)
                retain_monitor_logs(driver, required=False)
                raise
            try:
                retain_postgres_log(driver, start, required=True)
            finally:
                retain_monitor_logs(driver, required=True)
        if reset:
            driver.reset()
        runs[name].update(
            status="PASS",
            setup_seconds=ceil((origin - start) / SECOND),
            arm_seconds=ceil((end - origin) / SECOND),
            export_reset_seconds=ceil((time.time_ns() - end) / SECOND),
        )
        if first_origin is None:
            first_origin = origin
        save_verdict()

    def durations(final_evidence):
        result = {
            "bootstrap": ceil(first_origin / SECOND - started),
            "final_evidence": final_evidence,
        }
        for index, (name, row) in enumerate(runs.items()):
            result[name] = row["arm_seconds"]
            result[f"{name}_closeout"] = row["export_reset_seconds"] + (
                row["setup_seconds"] if index else 0
            )
        return result

    try:
        if stage == "calibration":
            outages = [root / f"outage-{i}" / "outage.json" for i in range(1, 4)]
            for name, (arm, duration, _) in CALIBRATION_ARMS.items():
                if arm == "scrape_loss":
                    rates = [restored_rate(json.loads(path.read_bytes()))[0] for path in outages]
                    if median(rates) <= ARRIVAL_RATE:
                        result = derive(rates, [])
                        kind.save(root / "schedule-proposal.json", result)
                        verdict.update(status="HALT", reason=result["reason"])
                        return verdict
                episode(name, arm, duration)
            t = time.monotonic()
            kind.save(root / "preseal-evidence-sha256.json", kind.artifact_hashes(root))
            measured = durations(ceil(time.monotonic() - t))
            kind.save(root / "forecast-measurements.json", measured)
            result = derive_from_outages(outages, measured)
            proposal = root / "schedule-proposal.json"
            kind.save(proposal, result)
            if result["status"] == "HALT":
                verdict.update(status="HALT", reason=result["reason"])
                return verdict
            schedule = root / "sealed-schedule.json"
            schedule.write_bytes(proposal.read_bytes())
            inputs = [*outages, root / "forecast-measurements.json"]
            inputs.extend(path.with_name("calibration-recovery.json") for path in outages)
            kind.save(
                root / "schedule-seal.json",
                {
                    "sha256": hashlib.sha256(schedule.read_bytes()).hexdigest(),
                    "campaign": campaign,
                    "head": os.environ["GITHUB_SHA"],
                    "inputs": {
                        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in inputs
                    },
                    "sealed_ns": time.time_ns(),
                },
            )
            verdict.update(status="PASS")
        else:
            episode("matched-healthy", "matched_healthy", sealed["matched_healthy"]["stop_s"])
            episode(
                "full-volume", "outage", sealed["primary"]["stop_s"], sealed["primary"], reset=False
            )
            quiet = json.loads((root / "full-volume/identity-drain.json").read_bytes())
            require(quiet["admitted"] == sealed["arrival_volume"], "sealed arrival volume differs")
            forecast = measurement_forecast(
                sealed["delta_s"], sealed["calibration_measured_seconds"], durations(0)
            )
            kind.save(root / "measurement-forecast.json", forecast)
            verdict.update(status="PASS", full_volume="PASS")
            if forecast["forecast_seconds"] > FORECAST_CAP * 60:
                verdict.update(status="HALT", reason="measurement_forecast_over_cap")
    except Exception as error:
        verdict.update(
            status=episode_error_status(error),
            reason=str(error),
            full_volume="UNPERFORMED" if "full-volume" not in runs else "FAILED",
        )
        for row in runs.values():
            if row["status"] == "RUNNING":
                row.update(status=verdict["status"], error=str(error))
        raise
    finally:
        for name in CALIBRATION_ARMS if stage == "calibration" else CONFIRMATION_ARMS:
            runs.setdefault(name, {"status": "UNPERFORMED"})
        save_verdict()
    return verdict


def main():
    args = hosted_arguments(__doc__, with_checker=True)
    campaign = campaign_id()
    sealed = None
    try:
        admission = verify_admission(args.evidence, args.stage)
        if args.stage == "confirmation":
            require(
                args.seal and args.seal_digest, "confirmation requires the calibration artifact"
            )
            sealed, provenance = verify_seal(
                args.seal, args.seal_digest, campaign, os.environ["GITHUB_SHA"]
            )
            kind.save(
                args.evidence / "verified-schedule.json",
                {
                    "artifact_digest": args.seal_digest,
                    "seal": provenance,
                    "schedule": sealed,
                },
            )
        else:
            forecast = qualification_forecast(0)["calibration"]
            require(
                forecast["forecast_seconds"] <= QUALIFICATION_CAP * 60,
                "calibration forecast over cap",
            )
            kind.save(args.evidence / "calibration-forecast.json", forecast)
    except Exception as error:
        kind.save(
            args.evidence / "verdict.json",
            {
                "status": "INVALID",
                "stage": args.stage,
                "campaign": campaign,
                "reason": str(error),
            },
        )
        raise
    kind.CLUSTER, kind.LABEL = "j3-qualification", "j3.qualification=owned"
    driver = Qualification(args.evidence, args.scratch, args.checker)
    # Reserve the last 15 minutes for failure evidence, cleanup and upload.
    driver.live_reservation = FORECAST_CAP * 60 - (time.time() - int(os.environ["J3_JOB_STARTED"]))
    runtime, pins = locked_images(driver)
    kind.save(
        args.evidence / "run.json",
        {
            "scope": "harness qualification",
            "stage": args.stage,
            "campaign": campaign,
            "run": os.environ["GITHUB_RUN_ID"],
            "head": os.environ["GITHUB_SHA"],
            "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
            "stage_attempt": admission["stage_attempt"],
            "pre_arm_gate_rejections": admission["prior_rejections"],
            "in_arm_pressure": "published_covariate_only",
            "runtime_profile": runtime["profile"],
            "checker": verify_binary(driver.checker),
            "images": {**driver.images, **pins},
            "kubernetes_namespace": APP,
            "runner": runner_cpu(),
        },
    )
    verdict = {"status": "INVALID", "reason": "qualification bootstrap did not complete"}
    try:
        addresses = bootstrap(driver, args, pins)
        driver.enable_commit_timestamps()
        verdict = run_qualification(driver, args, addresses, pins, campaign, sealed)
    finally:
        path = args.evidence / "verdict.json"
        if path.exists():
            verdict = json.loads(path.read_bytes())
        try:
            if verdict["status"] not in {"PASS", "HALT"} and driver.cluster_started:
                for namespace in (APP, NS):
                    driver.k(
                        "-n",
                        namespace,
                        "get",
                        "pods,events",
                        "-o",
                        "json",
                        allowed=True,
                        timeout=10,
                    )
        finally:
            try:
                kind.save(args.evidence / "cleanup.json", driver.cleanup())
            except Exception as error:
                kind.save(args.evidence / "cleanup.json", {"status": "FAIL", "error": str(error)})
                verdict.update(status="INVALID", cleanup_error=str(error))
                raise
            finally:
                kind.save(path, verdict)
                kind.save(args.evidence / "sha256.json", kind.artifact_hashes(args.evidence))
    return 0 if verdict["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
