"""Retain PostgreSQL and runner pressure evidence for J3 qualification."""

import json
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread

import dur053_k3 as kind
from j3_replay import SECOND, require

EXPORTER_SETTINGS = {
    "session_preload_libraries": "auto_explain",
    "auto_explain.log_min_duration": "500ms",
    "auto_explain.log_analyze": "on",
    "auto_explain.log_nested_statements": "on",
    "auto_explain.log_timing": "off",
    "log_min_duration_statement": "500ms",
}


def enable_exporter_diagnostics(driver):
    # Capture the query inside j3_sample() and avoid per-node clock reads.
    statement = "".join(
        f"ALTER ROLE j3_exporter SET {name} = '{value}';\n"
        for name, value in EXPORTER_SETTINGS.items()
    )
    driver.postgres(statement)
    expected = sorted(
        ["statement_timeout=2s", "default_transaction_read_only=on", "jit=off"]
        + [f"{name}={value}" for name, value in EXPORTER_SETTINGS.items()]
    )
    catalog = driver.postgres("""
        SELECT pg_catalog.to_json(s.setconfig)
        FROM pg_catalog.pg_db_role_setting AS s
        JOIN pg_catalog.pg_roles AS r ON r.oid = s.setrole
        WHERE s.setdatabase = 0 AND r.rolname = 'j3_exporter';
    """)
    observed = sorted(json.loads(catalog.stdout.strip() or "[]"))
    # The loaded module exposes a typed GUC. A saved custom placeholder does not.
    session = driver.postgres(
        """
        SELECT COALESCE(json_object_agg(name, json_build_object(
            'vartype', vartype, 'unit', unit, 'setting', setting)), '{}'::json)
        FROM pg_catalog.pg_settings
        WHERE name IN ('auto_explain.log_min_duration', 'log_min_duration_statement', 'jit');
        """,
        role="j3_exporter",
    )
    session_settings = json.loads(session.stdout)
    kind.save(
        driver.output / "exporter-role-settings.json",
        {
            "role": "j3_exporter",
            "expected": expected,
            "observed": observed,
            "session_settings": session_settings,
        },
    )
    require(observed == expected, "exporter role diagnostics differ in pg_db_role_setting")
    duration = {"vartype": "integer", "unit": "ms", "setting": "500"}
    require(
        session_settings.get("auto_explain.log_min_duration") == duration,
        "auto_explain is missing or misconfigured in the exporter session",
    )
    require(
        session_settings.get("log_min_duration_statement") == duration,
        "slow statement logging is misconfigured in the exporter session",
    )
    require(
        session_settings.get("jit") == {"vartype": "bool", "unit": None, "setting": "off"},
        "JIT is not disabled in the exporter session",
    )


@contextmanager
def runner_pressure(output, proc=Path("/proc")):
    stop = Event()
    sources = {"loadavg": "loadavg", "cpu": "pressure/cpu", "io": "pressure/io"}
    with (output / "runner-pressure.jsonl").open("w", encoding="utf-8", buffering=1) as stream:

        def sample():
            record = {"at_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}
            for name, path in sources.items():
                try:
                    record[name] = (proc / path).read_text(encoding="ascii").strip()
                except OSError as error:
                    record.setdefault("errors", {})[name] = str(error)
            stream.write(json.dumps(record) + "\n")

        def collect():
            while not stop.wait(1):
                sample()

        sample()
        worker = Thread(target=collect, name="j3-runner-pressure", daemon=True)
        worker.start()
        try:
            yield
        finally:
            stop.set()
            worker.join()


def retain_postgres_log(driver, start_ns, *, required):
    seconds, nanos = divmod(start_ns, SECOND)
    command = [
        "docker",
        "logs",
        "--timestamps",
        "--since",
        f"{seconds}.{nanos:09d}",
        kind.CLUSTER + "-postgres",
    ]
    raw = b""
    status = {"since_ns": start_ns}
    try:
        # Preserve ordering between both Docker streams, including on a failed episode.
        proc = subprocess.run(
            command,
            env=driver.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )
        raw = proc.stdout
        status["exit"] = proc.returncode
    except subprocess.TimeoutExpired as error:
        raw = error.output or b""
        status["error"] = str(error)
    except OSError as error:
        status["error"] = str(error)
    log = raw.decode("utf-8", errors="replace")
    for name in ("password", "demand_token", "monitor_password", "fixture_password"):
        if secret := getattr(driver, name, None):
            log = log.replace(secret, "[REDACTED]")
    (driver.output / "postgres-container.log").write_text(log, encoding="utf-8", newline="\n")
    kind.save(driver.output / "postgres-log.json", status)
    if required:
        require(status.get("exit") == 0, "PostgreSQL container log unavailable")
