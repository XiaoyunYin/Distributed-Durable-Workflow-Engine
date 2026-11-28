"""Exercise Phase B on the existing Calico kind and engine PostgreSQL harness."""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from dur053_k3 import CLUSTER, LOCK, ROOT, Driver
from dur053_k6_b_evidence import Evidence, SmokeFailure
from dur053_k6_identity import (
    APPROVER,
    BINDING,
    ISSUER,
    NO_REVIEW,
    PROBE,
    REVIEWER,
    WORKER,
    deny_egress,
    obj,
    render,
    validate_rendered,
)

ENGINE_ROLE = "durable"
READER_ROLE = "k6_sink_grant_reader"
DECISION_ROLE = "k6_decision_writer"
DATABASE = "dur053_k6_b"
TWO_L = timedelta(milliseconds=104082)
SAFE_CODE = re.compile(r"[A-Z][A-Z_0-9]{2,95}")
LEGACY_ENV_NAMES = {
    "J4_PROVIDER_RESILIENCE",
    "J4_RECORDED_LLM_ACTIVITY",
    "RUNTIME_J9A_STAGE_TIMINGS",
    "RUNTIME_RELAY_FULL_BATCH_LOOP",
    "RUNTIME_TIMEOUT_RETRY_POLICY",
}


def require(condition: bool, code: str) -> None:
    if not condition:
        raise SmokeFailure(code)


def legacy_env(spec: dict) -> dict[str, str]:
    legacy = {}
    for field in ("containers", "initContainers", "ephemeralContainers"):
        for container in spec.get(field, []):
            require(not container.get("envFrom"), "K6_LEGACY_OPAQUE_ENV")
            for entry in container.get("env", []):
                name = entry["name"]
                if name in LEGACY_ENV_NAMES:
                    require(
                        entry.get("value") == "0" and "valueFrom" not in entry,
                        "K6_LEGACY_ENV_VALUE",
                    )
                    legacy[name] = "0"
    return legacy


def private_file(path: Path, data: bytes) -> None:
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
        stream.write(data)


def subject(namespace: str, account: str) -> str:
    return f"system:serviceaccount:{namespace}:{account}"


def sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def json_data(document: object) -> bytes:
    return json.dumps(document).encode()


class Smoke:
    database = DATABASE
    inputs_schema = "dur053.k6-phase-b-inputs.v1"

    def __init__(self, evidence: Evidence, scratch: Path, harness: Driver) -> None:
        self.evidence, self.scratch, self.harness = evidence, scratch, harness
        self.deadline = datetime.now(UTC) + timedelta(minutes=25)
        self.env = dict(harness.env, GOMAXPROCS="2", CGO_ENABLED="0", PYTHONDONTWRITEBYTECODE="1")
        self.service_env = {"PATH": "/usr/bin:/bin"}
        self.ca = self.scratch / "engine.crt"
        self.kind_ca = self.scratch / "kind.crt"
        self.fixture = self.scratch / "fixture"
        self.adapter = self.scratch / "adapter"
        self.approver = self.scratch / "approver"
        self.evidence.secret(harness.password)
        self.evidence.secret(harness.demand_token)
        self.evidence.write(
            "inputs.json",
            {
                "schema": self.inputs_schema,
                "revision": self.command("SOURCE_REVISION", "git", "rev-parse", "HEAD")
                .decode()
                .strip(),
                "tree": self.command("SOURCE_TREE", "git", "rev-parse", "HEAD^{tree}")
                .decode()
                .strip(),
                "head_sha": os.getenv("DUR053_HEAD_SHA"),
                "run_id": os.getenv("GITHUB_RUN_ID"),
                "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
                "job": os.getenv("GITHUB_JOB"),
                "runner_image": os.getenv("ImageVersion"),  # noqa: SIM112
                "tools": LOCK,
                "go": self.command("GO_VERSION", "go", "version").decode().strip(),
                "images": harness.images,
                "issuer": ISSUER,
                "runtime_migration_export_role": ENGINE_ROLE,
                "expired_token_live": "offline only (Kubernetes TokenRequest minimum is 600 s)",
            },
        )

    def command(
        self,
        stage: str,
        *args: str,
        data: bytes | None = None,
        timeout: float = 45,
        isolated: bool = False,
        codes: tuple[int, ...] = (0,),
        kubectl_exec: bool = False,
    ) -> bytes:
        remaining = (self.deadline - datetime.now(UTC)).total_seconds()
        require(remaining > 0, "ENGINEERING_DEADLINE")
        try:
            result = subprocess.run(
                args,
                cwd=ROOT,
                env=self.service_env if isolated else self.env,
                input=data,
                capture_output=True,
                timeout=min(timeout, remaining),
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise SmokeFailure(f"{stage}_TIMEOUT") from None
        except OSError:
            raise SmokeFailure(f"{stage}_UNAVAILABLE") from None
        if result.returncode not in codes:
            # Credential-bearing subprocesses can print arbitrary data.
            # Keep only fixed diagnostics and never their stdout/stderr.
            err = result.stderr.decode(errors="replace").strip()
            if kubectl_exec:
                err = err.removesuffix(f"\ncommand terminated with exit code {result.returncode}")
            detail = (
                err
                if SAFE_CODE.fullmatch(err)
                else next(
                    (
                        label
                        for pattern, label in (
                            ("(Forbidden)", "FORBIDDEN"),
                            ("(NotFound)", "NOT_FOUND"),
                            ("(Invalid)", "INVALID"),
                            ("(AlreadyExists)", "ALREADY_EXISTS"),
                            ("timed out waiting", "WAIT_TIMEOUT"),
                        )
                        if pattern in err
                    ),
                    "",
                )
            )
            self.evidence.check(detail.encode())
            raise SmokeFailure(
                f"{stage}_EXIT_{result.returncode}" + (f"_{detail}" if detail else "")
            )
        return result.stdout

    def k(self, stage: str, *args: str, **kwargs) -> bytes:
        return self.command(stage, "kubectl", "--context", f"kind-{CLUSTER}", *args, **kwargs)

    def create(self, stage: str, manifest: dict) -> None:
        if manifest["kind"] == "Pod":
            legacy_env(manifest["spec"])
        elif manifest["kind"] in {"Deployment", "Job", "StatefulSet", "DaemonSet"}:
            legacy_env(manifest["spec"]["template"]["spec"])
        self.k(stage, "create", "-f", "-", data=json_data(manifest))

    def sql(self, stage: str, query: str) -> str:
        return (
            self.command(
                stage,
                "docker",
                "exec",
                "-i",
                CLUSTER + "-postgres",
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-U",
                ENGINE_ROLE,
                "-d",
                "postgres",
                data=query.encode(),
            )
            .decode()
            .strip()
        )

    def go(self, action: str, **data) -> dict:
        return json.loads(
            self.command(
                f"FIXTURE_{action.upper()}", str(self.fixture), action, data=json_data(data)
            )
        )

    def build(self) -> None:
        for name, executable in (
            ("fixture", self.fixture),
            ("decision", self.adapter),
            ("approver", self.approver),
        ):
            self.command(
                f"GO_BUILD_{name.upper()}",
                "go",
                "build",
                "-p",
                "1",
                "-buildvcs=false",
                "-trimpath",
                "-o",
                str(executable),
                f"./cmd/dur053-k6-{name}",
                timeout=300,
            )

    def bootstrap(self, address: str) -> dict:
        self.address = str(ipaddress.IPv4Address(address))
        discovery = json.loads(
            self.k("OIDC_DISCOVERY", "get", "--raw", "/.well-known/openid-configuration")
        )
        require(discovery.get("issuer") == ISSUER, "KIND_ISSUER_MISMATCH")
        self.api_server = self.k(
            "KIND_API_SERVER",
            "config",
            "view",
            "--minify",
            "-o",
            "jsonpath={.clusters[0].cluster.server}",
        ).decode()
        self.ca_data = self.k(
            "KIND_TRUSTED_CA",
            "config",
            "view",
            "--minify",
            "--raw",
            "-o",
            "jsonpath={.clusters[0].cluster.certificate-authority-data}",
        ).decode()
        private_file(self.kind_ca, base64.b64decode(self.ca_data, validate=True))
        return self.configure_tls()

    def configure_tls(self) -> dict:
        ca_key, key = self.scratch / "ca.key", self.scratch / "server.key"
        cert, csr, extensions = (
            self.scratch / "server.crt",
            self.scratch / "server.csr",
            self.scratch / "server.ext",
        )
        private_file(ca_key, b"")
        private_file(key, b"")
        self.command(
            "CA_GENERATE",
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=DUR-053 K6 engineering CA",
            "-keyout",
            str(ca_key),
            "-out",
            str(self.ca),
        )
        self.evidence.secret(ca_key.read_text())
        self.command(
            "SERVER_KEY_GENERATE",
            "openssl",
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=postgres",
            "-keyout",
            str(key),
            "-out",
            str(csr),
        )
        self.evidence.secret(key.read_text())
        require(
            ca_key.stat().st_mode & 0o777 == key.stat().st_mode & 0o777 == 0o600, "TLS_KEY_MODE"
        )
        extensions.write_text(
            f"subjectAltName=IP:{self.address},DNS:postgres\nextendedKeyUsage=serverAuth\n"
        )
        self.command(
            "SERVER_CERT_SIGN",
            "openssl",
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(self.ca),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            str(extensions),
            "-out",
            str(cert),
        )
        pg = CLUSTER + "-postgres"
        for path in (cert, key):
            self.command("TLS_DOCKER_COPY", "docker", "cp", str(path), f"{pg}:/tmp/k6-{path.name}")
        self.command(
            "POSTGRES_TLS_FILES",
            "docker",
            "exec",
            "-u",
            "0",
            pg,
            "sh",
            "-ec",
            'install -o postgres -g postgres -m 0600 /tmp/k6-server.key "$PGDATA/k6-server.key"; '
            'install -o postgres -g postgres -m 0644 /tmp/k6-server.crt "$PGDATA/k6-server.crt"; '
            "rm /tmp/k6-server.key /tmp/k6-server.crt; "
            "{ printf '%s\\n' 'hostnossl all k6_decision_writer,k6_sink_grant_reader all reject'; "
            'cat "$PGDATA/pg_hba.conf"; } > "$PGDATA/k6-hba"; '
            'cat "$PGDATA/k6-hba" > "$PGDATA/pg_hba.conf"; rm "$PGDATA/k6-hba"',
        )
        self.sql(
            "POSTGRES_TLS_RELOAD",
            "ALTER SYSTEM SET ssl_cert_file = 'k6-server.crt';\n"
            "ALTER SYSTEM SET ssl_key_file = 'k6-server.key';\n"
            "ALTER SYSTEM SET ssl = 'on';\nSELECT pg_catalog.pg_reload_conf();\n",
        )
        for _ in range(20):
            if self.sql("POSTGRES_SSL_SETTING", "SHOW ssl;") == "on":
                break
            time.sleep(0.5)
        else:
            raise SmokeFailure("POSTGRES_TLS_NOT_ENABLED")
        return {
            "engine_host": self.address,
            "server_cert_sha256": hashlib.sha256(cert.read_bytes()).hexdigest(),
        }

    def dsn(self, role: str, password: str, mode: str, ca: str = "") -> str:
        parameters = {"sslmode": mode, "connect_timeout": 2}
        if ca:
            parameters["sslrootcert"] = ca
        return f"postgresql://{role}:{password}@{self.address}:5432/{self.database}?" + urlencode(
            parameters
        )

    def uid(self, namespace: str, kind: str, name: str) -> str:
        uid = self.k(
            "OBJECT_UID", "-n", namespace, "get", kind, name, "-o", "jsonpath={.metadata.uid}"
        ).decode()
        require(bool(re.fullmatch(r"[0-9a-f-]{36}", uid)), "OBJECT_UID_INVALID")
        return uid

    def identities(self, namespace: str) -> tuple[list[dict], dict]:
        objects = render(namespace, self.harness.images["runtime"], self.harness.images["worker"])
        try:
            validate_rendered(objects, namespace)
        except ValueError as error:
            raise SmokeFailure(str(error)) from None
        self.evidence.write(f"{namespace}-objects.json", objects)
        for item in objects:
            if item["kind"] != "Job":
                self.create(f"CREATE_{item['kind'].upper()}", item)
        for account in (APPROVER, REVIEWER, NO_REVIEW, WORKER):
            answer = (
                self.k(
                    "REVIEWER_RBAC",
                    "auth",
                    "can-i",
                    "create",
                    "tokenreviews.authentication.k8s.io",
                    f"--as={subject(namespace, account)}",
                    "--as-group=system:authenticated",
                    "--as-group=system:serviceaccounts",
                    f"--as-group=system:serviceaccounts:{namespace}",
                    codes=(0, 1),
                )
                .decode()
                .strip()
            )
            require(
                answer.partition(" ")[0] == ("yes" if account == REVIEWER else "no"),
                "REVIEWER_RBAC_MISMATCH",
            )
        episode = {
            "Namespace": namespace,
            "ServiceAccountUID": self.uid(namespace, "serviceaccount", APPROVER),
            "SecretName": BINDING,
            "SecretUID": self.uid(namespace, "secret", BINDING),
        }
        return objects, episode

    def prepare_pod(self, namespace: str) -> None:
        self.k(
            "PROBE_READY",
            "-n",
            namespace,
            "wait",
            "pod/" + PROBE,
            "--for=condition=Ready",
            "--timeout=120s",
            timeout=125,
        )
        for path, name in (
            (self.fixture, "fixture"),
            (self.ca, "engine.crt"),
            (self.kind_ca, "kind.crt"),
        ):
            self.k(
                "PROBE_INSTALL",
                "-n",
                namespace,
                "exec",
                "-i",
                PROBE,
                "--",
                "sh",
                "-ec",
                f"umask 077; cat > /tmp/{name}; chmod 700 /tmp/{name}",
                data=path.read_bytes(),
            )

    def migrate(self, namespace: str, migration: dict, stage: str) -> None:
        self.create(stage + "_CREATE", migration)
        self.k(
            stage + "_COMPLETE",
            "-n",
            namespace,
            "wait",
            "job/migrate",
            "--for=condition=Complete",
            "--timeout=180s",
            timeout=185,
        )
        self.k(
            stage + "_DELETE",
            "-n",
            namespace,
            "delete",
            "job/migrate",
            "--wait=true",
            "--timeout=30s",
        )

    def reviewer(self, namespace: str, account: str) -> str:
        value = (
            self.k(
                "REVIEWER_TOKEN_REQUEST",
                "-n",
                namespace,
                "create",
                "token",
                account,
                "--duration=600s",
            )
            .decode()
            .strip()
        )
        self.evidence.secret(value)
        require(bool(value), "REVIEWER_TOKEN_EMPTY")
        return value

    def issue(self, ns: str, account: str, request: dict) -> tuple[str, str]:
        response = json.loads(
            self.k(
                "TOKEN_REQUEST",
                "create",
                "--raw",
                f"/api/v1/namespaces/{ns}/serviceaccounts/{account}/token",
                "-f",
                "-",
                data=json_data(request),
            )
        )
        status = response["status"]
        token = status["token"]
        self.evidence.secret(token)
        require(bool(token), "APPROVER_TOKEN_EMPTY")
        return token, status["expirationTimestamp"]

    @contextmanager
    def adapter_socket(
        self,
        directory: Path,
        socket_dir: Path,
        episode: dict,
        reviewer: str,
        dsn: str,
        *,
        evidence_config: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        config = directory / (socket_dir.name + ".json")
        private_file(
            config,
            json_data(
                {
                    "Episode": episode,
                    "APIServer": self.api_server,
                    "CA": self.ca_data,
                    "Reviewer": reviewer,
                    "DecisionDSN": dsn,
                    **({"Evidence": evidence_config} if evidence_config is not None else {}),
                }
            ),
        )
        process = None
        try:
            process = subprocess.Popen(
                (str(self.adapter), str(config), str(socket_dir)),
                cwd=directory,
                env=self.service_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            for _ in range(100):
                if process.poll() is not None:
                    diagnostic = process.stderr.read().decode(errors="replace").strip()
                    raise SmokeFailure(
                        diagnostic if SAFE_CODE.fullmatch(diagnostic) else "ADAPTER_EXITED"
                    )
                if (socket_dir / "decision.sock").is_socket():
                    break
                time.sleep(0.1)
            else:
                raise SmokeFailure("ADAPTER_START_TIMEOUT")
            yield
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                process.stderr.close()
            config.unlink(missing_ok=True)

    def approve(self, sandbox: Path, socket: str, plan: dict, token: str, **options) -> dict:
        request, credential = sandbox / "request.json", sandbox / "credential"
        try:
            private_file(request, json_data({"Plan": plan, **options}))
            if token:
                private_file(credential, token.encode())
            return json.loads(
                self.command(
                    "APPROVER",
                    "sudo",
                    "-n",
                    "unshare",
                    "--net",
                    "--",
                    "chroot",
                    f"--userspec={os.getuid()}:{os.getgid()}",
                    f"--groups={os.getgid()}",
                    str(sandbox),
                    "/approver",
                    f"/{socket}/decision.sock",
                    "/request.json",
                    "/credential" if token else "-",
                    isolated=True,
                )
            )
        finally:
            request.unlink(missing_ok=True)
            credential.unlink(missing_ok=True)

    def probe(self, ns: str, dsn: str, grant: dict, plan: dict, cutoff: str, *, mode: str) -> str:
        return self.probe_result(ns, dsn, grant, plan, cutoff, mode=mode)["code"]

    def probe_result(
        self, ns: str, dsn: str, grant: dict, plan: dict, cutoff: str, *, mode: str
    ) -> dict:
        result = json.loads(
            self.k(
                "POD_GRANT_PROBE",
                "-n",
                ns,
                "exec",
                "-i",
                PROBE,
                "--",
                "/tmp/fixture",
                "probe",
                data=json_data(
                    {"DSN": dsn, "Grant": grant, "Plan": plan, "Cutoff": cutoff, "Probe": mode}
                ),
            )
        )
        code = result["code"]
        require(bool(re.fullmatch(r"[A-Z0-9_]{3,80}", code)), "POD_PROBE_INVALID_CODE")
        diagnostic = result.get("diagnostic", {})
        require(isinstance(diagnostic, dict), "POD_PROBE_INVALID_DIAGNOSTIC")
        types = diagnostic.get("error_types", [])
        require(
            isinstance(types, list) and all(isinstance(item, str) for item in types),
            "POD_PROBE_INVALID_DIAGNOSTIC",
        )
        clean = {"error_types": types, "timeout": diagnostic.get("timeout", False)}
        require(isinstance(clean["timeout"], bool), "POD_PROBE_INVALID_DIAGNOSTIC")
        for key in ("errno", "op"):
            value = diagnostic.get(key)
            if value is not None:
                require(isinstance(value, str), "POD_PROBE_INVALID_DIAGNOSTIC")
                clean[key] = value
        return {"code": code, "diagnostic": clean}

    def snapshot(self, dsn: str) -> str:
        digest = self.go("snapshot", DSN=dsn)["sha256"]
        require(bool(re.fullmatch(r"[a-f0-9]{64}", digest)), "SNAPSHOT_DIGEST")
        return digest

    def expect_probe(self, expected: str, *args, **kwargs) -> dict:
        actual = self.probe(*args, **kwargs)
        require(actual == expected, f"EXPECTED_{expected}_GOT_{actual}")
        return {"code": actual}

    def expect_decision_denial(self, dsn: str, *args, **kwargs) -> dict:
        before = self.snapshot(dsn)
        result = self.approve(*args, **kwargs)
        after = self.snapshot(dsn)
        require(after == before, "DECISION_DENIAL_CHANGED_ENGINE_STATE")
        require(result == {"status": 403, "code": "K6_AUTH_DENIED"}, "EXPECTED_DECISION_HTTP_403")
        return {**result, "engine_snapshot_sha256": after}

    @staticmethod
    def plan(namespace: str) -> dict:
        return {
            "Namespace": namespace,
            "WorkflowID": namespace + "-workflow",
            "NodeID": "root",
            "Target": "engineering-resource",
            "ResourceRevision": "7",
            "Key": "engineering-effect-key",
            "Arguments": {"nested": {"value": 1}, "array": [False, None, -3]},
        }

    def warmup(
        self, episode: dict, admin_dsn: str, *, sealed_plan: dict[str, Any] | None = None
    ) -> tuple[str, str, int]:
        request = self.go("token-request", Episode=episode)
        clock = self.go("clock", DSN=admin_dsn)
        before, db, after = (
            datetime.fromisoformat(clock[key]) for key in ("before", "db", "after")
        )
        uncertainty = after - before
        require(timedelta(0) < uncertainty <= timedelta(milliseconds=50), "ENGINE_CLOCK_BRACKET")
        start = datetime.now(UTC) + timedelta(seconds=1)
        arrival = start + timedelta(seconds=120 + 180)
        cutoff = (
            db
            + (start - (before + uncertainty / 2))
            + timedelta(seconds=120 + 180 + 600)
            + TWO_L
            + uncertainty
        )
        require(cutoff < self.deadline, "ENGINEERING_DEADLINE_BEFORE_CUTOFF")
        episode.update(
            WarmupStart=start.isoformat(),
            LastPlannedArrival=arrival.isoformat(),
            TwoL=TWO_L // timedelta(microseconds=1) * 1000,
        )
        namespace = episode["Namespace"]
        name = f"{namespace}-manifest.json"
        inputs = {
            "schema": "dur053.k6-phase-b-episode.v1",
            "episode": episode,
            "sealed_fixture_plan": self.plan(namespace) if sealed_plan is None else sealed_plan,
            "measurement_claim": False,
            "charged_seconds": 0,
            "clock_bracket": clock,
            "uncertainty_us": uncertainty // timedelta(microseconds=1),
            "valid_until": cutoff.isoformat(),
            "job_deadline": self.deadline.isoformat(),
            "token_request": request,
            "subject": subject(namespace, APPROVER),
        }
        self.evidence.write(name, inputs)
        time.sleep(max(0, (start - datetime.now(UTC)).total_seconds()))
        require(datetime.now(UTC) < start + timedelta(seconds=2), "MISSED_WARMUP_TOKEN_REQUEST")
        token, expires = self.issue(namespace, APPROVER, request)
        inputs.update(token_expiration=expires, token_sha256=sha(token))
        self.evidence.write(name, inputs)
        self.go("token-window", Episode=episode, Token=token, Expiration=expires)
        return token, cutoff.isoformat(), uncertainty // timedelta(microseconds=1) * 1000

    def delete_binding(self, namespace: str) -> None:
        self.k(
            "REVOKE_TOKEN_BINDING",
            "-n",
            namespace,
            "delete",
            "secret",
            BINDING,
            "--wait=true",
            "--timeout=20s",
        )

    def teardown_identity(self, namespace: str) -> None:
        self.k(
            "REMOVE_REVIEWER_RBAC",
            "delete",
            "clusterrole,clusterrolebinding",
            namespace + "-review",
            "--ignore-not-found",
        )
        self.k(
            "REMOVE_EPISODE_NAMESPACE",
            "delete",
            "namespace",
            namespace,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=75s",
            timeout=85,
        )

    def network_denial(self, namespace: str, grant_args: tuple) -> None:
        policy = deny_egress(namespace)
        self.evidence.write("default-deny.json", policy)
        network_codes = {"TCP_DIAL_TIMEOUT", "TCP_DIAL_REFUSED", "TCP_DIAL_UNREACHABLE"}
        with self.evidence.step("b-default-deny-egress") as row:
            row.update(self.expect_probe("EXACT_ROWS_1", *grant_args, mode="network"))
            self.create("DEFAULT_DENY_CREATE", policy)
            for _ in range(8):
                result = self.probe_result(*grant_args, mode="network")
                actual = result["code"]
                if actual in network_codes:
                    break
                require(actual == "EXACT_ROWS_1", f"EXPECTED_NETWORK_DENIAL_GOT_{actual}")
                time.sleep(1)
            else:
                raise SmokeFailure("CALICO_DEFAULT_DENY_NOT_ENFORCED")
            row.update(code=actual, diagnostic=result["diagnostic"])
        with self.evidence.step("c-connectivity-restored") as row:
            self.k(
                "DEFAULT_DENY_DELETE",
                "-n",
                namespace,
                "delete",
                "networkpolicy",
                policy["metadata"]["name"],
                "--wait=true",
                "--timeout=20s",
            )
            for _ in range(8):
                result = self.probe_result(*grant_args, mode="network")
                actual = result["code"]
                if actual == "EXACT_ROWS_1":
                    break
                require(actual in network_codes, f"EXPECTED_RESTORED_CONNECTIVITY_GOT_{actual}")
                time.sleep(1)
            else:
                raise SmokeFailure("CONNECTIVITY_NOT_RESTORED")
            row.update(code=actual, diagnostic=result["diagnostic"])

    def authentication_denials(self, grant_args: tuple, reader_password: str) -> None:
        namespace, _, grant, plan, cutoff = grant_args
        wrong_password = secrets.token_hex(32)
        self.evidence.secret(wrong_password)
        for name, role, password, mode, ca, expected in (
            (
                "wrong-password",
                READER_ROLE,
                wrong_password,
                "verify-full",
                "/tmp/engine.crt",
                "28P01",
            ),
            ("no-tls", READER_ROLE, reader_password, "disable", "", "28000"),
            (
                "wrong-ca",
                READER_ROLE,
                reader_password,
                "verify-full",
                "/tmp/kind.crt",
                "TLS_VERIFY_FAILED",
            ),
            (
                "wrong-role",
                DECISION_ROLE,
                reader_password,
                "verify-full",
                "/tmp/engine.crt",
                "28P01",
            ),
        ):
            with self.evidence.step("c-" + name) as row:
                dsn = self.dsn(role, password, mode, ca)
                row.update(
                    self.expect_probe(expected, namespace, dsn, grant, plan, cutoff, mode="connect")
                )

    def adapter_denials(
        self,
        directory: Path,
        sandbox: Path,
        episode: dict,
        admin_dsn: str,
        decision_dsn: str,
        plan: dict,
        token: str,
        intent: str,
    ) -> None:
        namespace = episode["Namespace"]
        other_ns = "dur053-k6-foreign"
        other_objects = render(
            other_ns, self.harness.images["runtime"], self.harness.images["worker"]
        )
        for item in other_objects:
            kind = item["kind"]
            if kind in {"Namespace", "Secret"} or (
                kind == "ServiceAccount" and item["metadata"]["name"] == APPROVER
            ):
                self.create("FOREIGN_APPROVER_IDENTITY", item)
        other = {
            "Namespace": other_ns,
            "SecretName": BINDING,
            "SecretUID": self.uid(other_ns, "secret", BINDING),
        }
        for name, identity, account, audience in (
            ("wrong-namespace", other, APPROVER, None),
            ("wrong-audience", episode, APPROVER, "dur053-k6-unrelated"),
            ("worker-token", episode, WORKER, None),
        ):
            with self.evidence.step("d-" + name) as row:
                request = self.go("token-request", Episode=identity)
                if audience is not None:
                    request["spec"]["audiences"] = [audience]
                wrong, _ = self.issue(identity["Namespace"], account, request)
                row.update(
                    self.expect_decision_denial(
                        admin_dsn,
                        sandbox,
                        "live",
                        plan,
                        wrong,
                        IntentID=intent,
                        Choice="APPROVED",
                    )
                )
                row.update(
                    token_sha256=sha(wrong),
                    subject=subject(identity["Namespace"], account),
                    service_account_uid=self.uid(identity["Namespace"], "serviceaccount", account),
                    secret_uid=identity["SecretUID"],
                )
        with self.evidence.step("d-lost-reviewer-access") as row:
            unprivileged = self.reviewer(namespace, NO_REVIEW)
            with self.adapter_socket(
                directory, sandbox / "no-review", episode, unprivileged, decision_dsn
            ):
                row.update(
                    self.expect_decision_denial(
                        admin_dsn,
                        sandbox,
                        "no-review",
                        plan,
                        token,
                        IntentID=intent,
                        Choice="APPROVED",
                    )
                )
            row.update(
                reviewer_subject=subject(namespace, NO_REVIEW),
                reviewer_token_sha256=sha(unprivileged),
            )
        for name, credential, options in (
            ("forged-actor", token, {"Choice": "APPROVED", "Actor": subject(namespace, APPROVER)}),
            ("missing-bearer", "", {"Choice": "APPROVED"}),
            ("decision-change", token, {"Choice": "REJECTED"}),
        ):
            with self.evidence.step("d-" + name) as row:
                row.update(
                    self.expect_decision_denial(
                        admin_dsn,
                        sandbox,
                        "live",
                        plan,
                        credential,
                        IntentID=intent,
                        **options,
                    )
                )
        # Revocation closes this exact credential, so run it after the denials
        # that require an independently valid bearer.
        with self.evidence.step("d-revoked-binding") as row:
            retry = self.approve(sandbox, "live", plan, token, IntentID=intent, Choice="APPROVED")
            require(retry.get("status") == 200, "REVOCATION_CONTROL_NOT_AUTHENTICATED")
            self.delete_binding(namespace)
            # The pinned Kubernetes 1.32 server caches successful token
            # authentication for 10 seconds, including TokenReview requests.
            time.sleep(11)
            row.update(
                self.expect_decision_denial(
                    admin_dsn,
                    sandbox,
                    "live",
                    plan,
                    token,
                    IntentID=intent,
                    Choice="APPROVED",
                )
            )
            row.update(
                token_sha256=sha(token), secret_uid=episode["SecretUID"], cache_wait_seconds=11
            )

    def reader_denials(self, admin_dsn: str, grant_args: tuple) -> None:
        namespace, dsn, grant, plan, cutoff = grant_args
        for mode in ("sql", "table", "list", "set-role", "insert", "update"):
            with self.evidence.step("e-reader-" + mode) as row:
                before = self.snapshot(admin_dsn)
                row.update(
                    self.expect_probe("42501", namespace, dsn, grant, plan, cutoff, mode=mode)
                )
                require(self.snapshot(admin_dsn) == before, "READER_DENIAL_CHANGED_ENGINE_STATE")
                row["engine_snapshot_sha256"] = before
        with self.evidence.step("e-foreign-episode-grant") as row:
            foreign_plan = self.plan("dur053-k6-foreign")
            foreign = self.go("foreign", DSN=admin_dsn, Plan=foreign_plan, Cutoff=cutoff)
            self.evidence.secret(foreign["Token"])
            before = self.snapshot(admin_dsn)
            row.update(
                self.expect_probe(
                    "EXACT_ROWS_0",
                    namespace,
                    dsn,
                    foreign,
                    foreign_plan,
                    cutoff,
                    mode="empty",
                )
            )
            require(self.snapshot(admin_dsn) == before, "READER_DENIAL_CHANGED_ENGINE_STATE")
            row.update(token_sha256=sha(foreign["Token"]), engine_snapshot_sha256=before)

    def episode(self, ns: str, prior: dict | None = None) -> dict:
        phase = "a" if prior is None else "f"
        directory = self.scratch / ns[-2:]
        directory.mkdir(mode=0o700)
        sandbox = directory / "jail"
        sandbox.mkdir(mode=0o700)
        shutil.copy2(self.approver, sandbox / "approver")
        admin_dsn = self.dsn(ENGINE_ROLE, self.harness.password, "disable")
        plan = self.plan(ns)
        provisioned = False
        try:
            with self.evidence.step(f"{phase}-create-identity-and-migrate") as row:
                objects, episode = self.identities(ns)
                migration = next(item for item in objects if item["kind"] == "Job")
                self.create(
                    "CAMPAIGN_DB_SECRET",
                    obj("Secret", "campaign-db", ns, stringData={"url": admin_dsn}, type="Opaque"),
                )
                self.migrate(ns, migration, "PRE_PROVISION_MIGRATION")
                self.prepare_pod(ns)
                review_token = self.reviewer(ns, REVIEWER)
                row.update(
                    subject=subject(ns, APPROVER),
                    service_account_uid=episode["ServiceAccountUID"],
                    secret_uid=episode["SecretUID"],
                    reviewer_subject=subject(ns, REVIEWER),
                    reviewer_uid=self.uid(ns, "serviceaccount", REVIEWER),
                    reviewer_token_sha256=sha(review_token),
                )
            with self.evidence.step(f"{phase}-warmup-token-request") as row:
                token, cutoff, uncertainty = self.warmup(episode, admin_dsn)
                row.update(token_sha256=sha(token), subject=subject(ns, APPROVER))
            with self.evidence.step(f"{phase}-provision-and-connect") as row:
                credentials = self.go(
                    "provision",
                    DSN=admin_dsn,
                    Episode=episode,
                    Cutoff=cutoff,
                    Uncertainty=uncertainty,
                    EngineRoles=[ENGINE_ROLE],
                )
                provisioned = True
                reader_password, decision_password = (
                    credentials["ReaderPassword"],
                    credentials["DecisionPassword"],
                )
                self.evidence.secret(reader_password)
                self.evidence.secret(decision_password)
                row.update(self.go("connect-acl", DSN=admin_dsn))
                self.migrate(ns, migration, "POST_PROVISION_MIGRATION")
                row.update(role=ENGINE_ROLE, migration="COMPLETE")
            decision_dsn = self.dsn(DECISION_ROLE, decision_password, "verify-full", str(self.ca))
            reader_dsn = self.dsn(READER_ROLE, reader_password, "verify-full", "/tmp/engine.crt")
            with self.evidence.step(f"{phase}-lease-owned-intent") as row:
                intent = self.go("intent", DSN=admin_dsn, Plan=plan, Cutoff=cutoff)
                row.update(intent_id=intent["intent_id"], proposal_hash=intent["proposal_hash"])
            with ExitStack() as adapters:
                with self.evidence.step(f"{phase}-adapter-start"):
                    adapters.enter_context(
                        self.adapter_socket(
                            directory, sandbox / "live", episode, review_token, decision_dsn
                        )
                    )
                with self.evidence.step(f"{phase}-pending-lookup") as row:
                    pending = self.approve(sandbox, "live", plan, token)
                    intent_id = intent["intent_id"]
                    require(
                        pending.get("status") == 200
                        and pending["identity"]
                        == {
                            "intent_id": intent_id,
                            "proposal_hash": intent["proposal_hash"],
                            "decision": "PENDING",
                        },
                        "PENDING_IDENTITY_MISMATCH",
                    )
                    row["identity"] = pending["identity"]
                with self.evidence.step(f"{phase}-record-approval") as row:
                    approved = self.approve(
                        sandbox, "live", plan, token, IntentID=intent_id, Choice="APPROVED"
                    )
                    identity = approved.get("identity", {})
                    require(
                        approved.get("status") == 200
                        and set(identity)
                        == {"intent_id", "proposal_hash", "subject", "decision", "decided_at"}
                        and identity["intent_id"] == intent_id
                        and identity["proposal_hash"] == intent["proposal_hash"]
                        and identity["subject"] == subject(ns, APPROVER)
                        and identity["decision"] == "APPROVED"
                        and bool(identity["decided_at"]),
                        "RECORDED_IDENTITY_MISMATCH",
                    )
                    row["identity"] = identity
                with self.evidence.step(f"{phase}-apply-and-read-exact-grant") as row:
                    grant = self.go(
                        "apply",
                        DSN=admin_dsn,
                        Plan=plan,
                        Cutoff=cutoff,
                        IntentID=intent_id,
                        Lease=intent["lease"],
                    )
                    self.evidence.secret(grant["Token"])
                    grant_args = (ns, reader_dsn, grant, plan, cutoff)
                    row.update(self.expect_probe("EXACT_GRANT", *grant_args, mode="read"))
                    row.update(token_sha256=sha(grant["Token"]))
                with self.evidence.step(f"{phase}-checker-export") as row:
                    row.update(self.go("export", DSN=admin_dsn, Plan=plan))
                if prior is None:
                    self.network_denial(ns, grant_args)
                    self.authentication_denials(grant_args, reader_password)
                    self.adapter_denials(
                        directory, sandbox, episode, admin_dsn, decision_dsn, plan, token, intent_id
                    )
                    self.reader_denials(admin_dsn, grant_args)
                else:
                    with self.evidence.step("f-previous-episode-credentials-denied") as row:
                        before = self.snapshot(admin_dsn)
                        row.update(
                            self.expect_probe(
                                "EXACT_ROWS_0",
                                ns,
                                reader_dsn,
                                prior["grant"],
                                prior["plan"],
                                cutoff,
                                mode="empty",
                            )
                        )
                        old_dsn = self.dsn(
                            READER_ROLE, prior["password"], "verify-full", "/tmp/engine.crt"
                        )
                        self.expect_probe("28P01", ns, old_dsn, grant, plan, cutoff, mode="connect")
                        require(
                            self.snapshot(admin_dsn) == before, "CROSS_EPISODE_CHANGED_ENGINE_STATE"
                        )
                        row.update(old_login="28P01", engine_snapshot_sha256=before)
                    with self.evidence.step("f-revoke-second-binding"):
                        self.delete_binding(ns)
            return {"grant": grant, "password": reader_password, "plan": plan}
        finally:
            with self.evidence.step("f-reset-first" if prior is None else "f-reset-second") as row:
                try:
                    if provisioned:
                        row.update(self.go("reset", DSN=admin_dsn, Episode={"Namespace": ns}))
                    else:
                        row["installation_created"] = False
                finally:
                    try:
                        self.teardown_identity(ns)
                    finally:
                        if prior is None:
                            self.teardown_identity("dur053-k6-foreign")


def inherited_failure(output: Path) -> None:
    if not output.exists():
        output.mkdir()
        evidence = Evidence(output)
        evidence.report.update(reason="SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_B")
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "smoke.json"
    prior = None
    if report_path.is_file():
        try:
            prior = json.loads(report_path.read_bytes())
        except (OSError, ValueError):
            prior = None
    evidence = Evidence(output)
    if (
        isinstance(prior, dict)
        and prior.get("schema") == "dur053.k6-phase-b-smoke.v1"
        and isinstance(prior.get("outcomes"), list)
    ):
        evidence.report = prior
    failure = evidence.safe_reason(error)
    evidence.report.update(status="FAIL", reason=failure)
    evidence.report["outcomes"].append(
        {"step": "phase-b-handoff", "status": "FAIL", "reason": failure}
    )
    evidence.checkpoint()


def run(output: Path, harness: Driver | None = None, address: str | None = None) -> int:
    output = output.resolve()
    require(not output.is_relative_to(ROOT), "OUTPUT_INSIDE_REPOSITORY")
    output.mkdir()
    evidence = Evidence(output)
    owns_harness = harness is None
    scratch = None
    try:
        with evidence.step("private-scratch"):
            require(
                bool(os.getenv("RUNNER_TEMP")) and os.name == "posix",
                "HOSTED_LINUX_RUNNER_REQUIRED",
            )
            runner_temp = Path(os.environ["RUNNER_TEMP"]).resolve()
            scratch = Path(tempfile.mkdtemp(prefix="k6b2-", dir=runner_temp))
        if owns_harness:
            with evidence.step("kind-harness"):
                harness_scratch = scratch / "kind"
                harness_scratch.mkdir(mode=0o700)
                logs = scratch / "kind-logs"
                logs.mkdir(mode=0o700)
                harness = Driver(logs, harness_scratch)
                harness.install()
                harness.build()
                for name, addon in LOCK["addons"].items():
                    if addon["installed_in_k3a"]:
                        harness.prepare_addon(name)
                address = harness.cluster()["192.0.2.1"]
        require(harness is not None and address is not None, "KIND_HANDOFF_INCOMPLETE")
        with evidence.step("build-phase-b-binaries"):
            smoke = Smoke(evidence, scratch, harness)
            smoke.build()
        with evidence.step("postgres-tls-and-kind-issuer") as row:
            row.update(smoke.bootstrap(address))
        with evidence.step("engine-database"):
            harness.databases.add(DATABASE)
            smoke.sql("ENGINE_DATABASE_CREATE", f'CREATE DATABASE "{DATABASE}";')
        first = smoke.episode("dur053-k6-b1")
        smoke.episode("dur053-k6-b2", first)
        with evidence.step("engine-database-cleanup"):
            smoke.sql("ENGINE_DATABASE_DROP", f'DROP DATABASE "{DATABASE}" WITH (FORCE);')
            require(
                smoke.sql(
                    "ENGINE_DATABASE_ABSENCE",
                    f"SELECT count(*) FROM pg_database WHERE datname='{DATABASE}';",
                )
                == "0",
                "ENGINE_DATABASE_STILL_PRESENT",
            )
            harness.databases.discard(DATABASE)
    except (Exception, KeyboardInterrupt) as error:
        # A later successful cleanup step must not replace the first failing stage.
        evidence.report.update(
            reason=next(
                (row["reason"] for row in evidence.report["outcomes"] if row["status"] == "FAIL"),
                evidence.safe_reason(error),
            )
        )
    else:
        evidence.report.update(status="PASS", reason="MATRIX_COMPLETE")
    finally:
        failure = evidence.report["reason"]
        if owns_harness and harness is not None:
            try:
                with evidence.step("kind-harness-cleanup"):
                    harness.cleanup()
            except Exception as error:
                evidence.report.update(status="FAIL")
                if failure == "MATRIX_COMPLETE":
                    failure = evidence.safe_reason(error)
        if scratch is not None:
            try:
                shutil.rmtree(scratch)
            except OSError:
                evidence.report.update(status="FAIL")
                if failure == "MATRIX_COMPLETE":
                    failure = "PRIVATE_SCRATCH_CLEANUP_FAILED"
        evidence.report["reason"] = failure
        evidence.checkpoint()
        try:
            evidence.scan()
        except SmokeFailure:
            evidence.report.update(status="FAIL", reason="CREDENTIAL_SCAN_FAILED")
            evidence.report["outcomes"].append(
                {
                    "step": "retained-output-credential-scan",
                    "status": "FAIL",
                    "reason": "CREDENTIAL_SCAN_FAILED",
                }
            )
        else:
            evidence.report["outcomes"].append(
                {
                    "step": "retained-output-credential-scan",
                    "status": "PASS",
                }
            )
        evidence.checkpoint()
    return 0 if evidence.report["status"] == "PASS" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    raise SystemExit(run(parser.parse_args().output_root))
