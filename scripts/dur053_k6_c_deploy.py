"""Private runner setup for the K6 Phase C sink, relay and host receiver."""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import secrets
import socket
import subprocess
import time
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlencode

from dur053_k3 import CLUSTER, LABEL, LOCK, ROOT
from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_b_smoke import ENGINE_ROLE, SAFE_CODE, Smoke, json_data, private_file, require
from dur053_k6_c_render import CLIENT, ENGINE, PORT, RELAY, SINK, budget_for, render
from dur053_k6_identity import obj

NS = "dur053-k6-c"
ENGINE_DB = "dur053_k6_c"
SINK_DB = "dur053_k6_effects"
ENGINE_CONTAINER = CLUSTER + "-postgres"
SINK_CONTAINER = CLUSTER + "-k6-sink-db"
VOLUME = SINK_CONTAINER + "-data"
SINK_ADMIN = "k6_sink_admin"
SINK_ROLE = "k6_sink_service"
EXPORTER = "k6_sink_exporter"
ENGINE_PEER = "spiffe://dur053-k6/worker-control"
RELAY_PEER = "spiffe://dur053-k6/ingress"
SINK_PEER = "spiffe://dur053-k6/sink"
TOKEN_FILE = "/identity/token"


def encoded(data: bytes) -> str:
    return base64.b64encode(data).decode()


class Deployment(Smoke):
    database = ENGINE_DB
    namespace = NS
    sink_container = SINK_CONTAINER
    sink_volume = VOLUME
    live_worker = False
    inputs_schema = "dur053.k6-phase-c-inputs.v1"

    def __init__(self, evidence, scratch, harness):
        super().__init__(evidence, scratch, harness)
        self.sink = scratch / "sink"
        self.relay = scratch / "relay"
        self.probe_bin = scratch / "probe"
        self.witness = scratch / "witness"
        self.journal = scratch / "ingress.jsonl"
        self.witness_config = scratch / "witness.json"
        self.witness_process = None
        self.volume_created = False
        self.sink_created = False
        self.engine_created = False
        self.identity_created = False
        self.provisioned = False
        self.sink_admin_password = self.password()
        self.sink_password = self.password()
        self.export_password = self.password()

    def password(self) -> str:
        password = secrets.token_hex(32)
        self.evidence.secret(password)
        return password

    def build(self) -> None:
        super().build()
        self.build_effect()

    def build_effect(self, tag: str = "") -> None:
        for path, package in (
            (self.sink, "dur053-sink"),
            (self.relay, "dur053-k6-relay"),
            (self.probe_bin, "dur053-k6-effect-probe"),
            (self.witness, "dur053-k6-witness"),
        ):
            self.command(
                "GO_BUILD_EFFECT",
                "go",
                "build",
                "-p",
                "1",
                "-buildvcs=false",
                "-trimpath",
                *(["-tags", tag] if tag else []),
                "-o",
                str(path),
                "./cmd/" + package,
                timeout=300,
            )
        # Check the exact executable subsequently shipped in the image.
        for arg in ("--arm", "--hold", "--fault"):
            self.command("ORDINARY_RELAY_USAGE", str(self.relay), arg, codes=(1,))
        context = self.scratch / "image"
        context.mkdir(mode=0o700)
        for path in (self.sink, self.relay, self.probe_bin):
            (context / path.name).write_bytes(path.read_bytes())
            (context / path.name).chmod(0o755)
        (context / "Dockerfile").write_text(
            "FROM scratch\nCOPY sink relay probe /\nUSER 65532:65532\n", encoding="utf-8"
        )
        image_tag = "localhost:5001/dur053-k6-effect" + ("-campaign" if tag else "") + ":candidate"
        self.command(
            "EFFECT_IMAGE_BUILD",
            "docker",
            "build",
            "--provenance=false",
            "--platform=linux/amd64",
            "-t",
            image_tag,
            str(context),
            timeout=300,
        )
        self.command("EFFECT_IMAGE_PUSH", "docker", "push", image_tag, timeout=120)
        refs = json.loads(
            self.command(
                "EFFECT_IMAGE_DIGEST",
                "docker",
                "inspect",
                "--format",
                "{{json .RepoDigests}}",
                image_tag,
            )
        )
        require(len(refs) == 1, "EFFECT_IMAGE_DIGEST_COUNT")
        self.image = refs[0]
        self.command(
            "EFFECT_IMAGE_LOAD",
            "docker",
            "exec",
            CLUSTER + "-control-plane",
            "ctr",
            "--namespace",
            "k8s.io",
            "images",
            "pull",
            "--hosts-dir",
            "/etc/containerd/certs.d",
            self.image,
            timeout=180,
        )
        self.evidence.write(
            "effect-image.json",
            {
                "image": self.image,
                "relay_build": tag or "ordinary",
                "binaries": {
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in (self.sink, self.relay, self.probe_bin, self.witness)
                },
            },
        )

    def instance_address(self, container: str) -> str:
        return str(
            ipaddress.IPv4Address(
                self.command(
                    "DATABASE_ADDRESS",
                    "docker",
                    "inspect",
                    "--format",
                    '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
                    container,
                )
                .decode()
                .strip()
            )
        )

    def create_databases(self) -> dict:
        self.endpoint_addresses = {
            "engine_db": self.address,
            "api_service": json.loads(
                self.k("API_SERVICE", "get", "svc", "kubernetes", "-o", "json")
            )["spec"]["clusterIP"],
            "api_endpoint": json.loads(
                self.k("API_ENDPOINT", "get", "endpoints", "kubernetes", "-o", "json")
            )["subsets"][0]["addresses"][0]["ip"],
        }
        self.harness.databases.add(self.database)
        self.sql("ENGINE_EPISODE_DB", f'CREATE DATABASE "{self.database}";')
        self.engine_created = True
        self.command(
            "SINK_DISK_CREATE", "docker", "volume", "create", "--label", LABEL, self.sink_volume
        )
        self.volume_created = True
        self.env["POSTGRES_PASSWORD"] = self.sink_admin_password
        self.harness.created.add(self.sink_container)
        try:
            self.command(
                "SINK_POSTGRES_START",
                "docker",
                "run",
                "-d",
                "--name",
                self.sink_container,
                "--label",
                LABEL,
                "--network",
                "kind",
                "--mount",
                f"type=volume,src={self.sink_volume},dst=/var/lib/postgresql",
                "-e",
                "POSTGRES_PASSWORD",
                "-e",
                f"POSTGRES_USER={SINK_ADMIN}",
                "-e",
                f"POSTGRES_DB={SINK_DB}",
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=scram-sha-256",
                LOCK["postgres"],
                "-c",
                "max_connections=30",
                "-c",
                "fsync=on",
                "-c",
                "synchronous_commit=on",
                "-c",
                "full_page_writes=on",
                "-c",
                "log_statement=none",
                "-c",
                "log_parameter_max_length=0",
                "-c",
                "log_parameter_max_length_on_error=0",
            )
        finally:
            self.env.pop("POSTGRES_PASSWORD")
        self.sink_created = True
        for _ in range(45):
            ready = self.command(
                "SINK_POSTGRES_READY",
                "docker",
                "exec",
                self.sink_container,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-U",
                SINK_ADMIN,
                "-d",
                SINK_DB,
                codes=(0, 1, 2),
            )
            if b"accepting connections" in ready:
                break
            time.sleep(1)
        else:
            require(False, "SINK_POSTGRES_NOT_READY")
        self.sink_address = self.instance_address(self.sink_container)
        self.endpoint_addresses["sink_db"] = self.sink_address
        self.endpoint_addresses["witness"] = str(
            ipaddress.IPv4Address(
                self.command(
                    "KIND_BRIDGE_GATEWAY",
                    "docker",
                    "inspect",
                    "--format",
                    '{{(index .NetworkSettings.Networks "kind").Gateway}}',
                    CLUSTER + "-control-plane",
                )
                .decode()
                .strip()
            )
        )
        with socket.socket() as port:
            port.bind((self.endpoint_addresses["witness"], 0))
            self.endpoint_addresses["witness_port"] = str(port.getsockname()[1])
        rows = {}
        for name, container in (("engine", ENGINE_CONTAINER), ("sink", self.sink_container)):
            rows[name] = {
                "container": self.command(
                    "DB_CONTAINER_ID",
                    "docker",
                    "inspect",
                    "--format",
                    "{{.Id}}",
                    container,
                )
                .decode()
                .strip(),
                "instance": self.psql(
                    name, "admin", "SELECT system_identifier FROM pg_control_system();"
                ),
                "disk": self.sink_volume if name == "sink" else None,
            }
        require(
            all(rows["engine"][field] != rows["sink"][field] for field in ("container", "instance"))
            and self.address != self.sink_address,
            "DATABASE_INSTANCES_NOT_SEPARATE",
        )
        return rows

    def psql(self, server: str, role: str, query: str) -> str:
        sink = server == "sink"
        return (
            self.command(
                "PSQL_" + server.upper(),
                "docker",
                "exec",
                "-i",
                "-e",
                "PGAPPNAME=dur053-k6-c-" + role,
                "-e",
                "PGOPTIONS=-c statement_timeout=2500 -c lock_timeout=1000",
                self.sink_container if sink else ENGINE_CONTAINER,
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-U",
                SINK_ADMIN if sink else ENGINE_ROLE,
                "-d",
                SINK_DB if sink else "postgres",
                data=query.encode(),
                timeout=8,
            )
            .decode()
            .strip()
        )

    def migrate_sink(self) -> None:
        for migration in sorted((ROOT / "sink-migrations").glob("*.up.sql")):
            self.psql("sink", "migrate", migration.read_text())
        self.psql(
            "sink",
            "migrate",
            f"""
            REVOKE CONNECT ON DATABASE postgres,template1 FROM PUBLIC;
            SET password_encryption = 'scram-sha-256';
            ALTER ROLE {SINK_ROLE} LOGIN PASSWORD '{self.sink_password}';
            ALTER ROLE {EXPORTER} LOGIN PASSWORD '{self.export_password}';
            GRANT CONNECT ON DATABASE {SINK_DB} TO {SINK_ROLE},{EXPORTER};
            """,
        )
        require(
            self.psql(
                "sink",
                "admin",
                "SELECT count(*) FROM pg_trigger "
                "WHERE tgname='journal_resource_write' AND tgenabled='O';",
            )
            == "1",
            "SINK_MIGRATION_TRIGGER_MISSING",
        )

    def cert(self, name: str, san: str, usages: str) -> dict:
        key, crt, csr, ext = (
            self.scratch / f"{name}.{suffix}" for suffix in ("key", "crt", "csr", "ext")
        )
        private_file(key, b"")
        self.command(
            "EFFECT_KEY_GENERATE",
            "openssl",
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-subj",
            "/CN=" + name,
            "-keyout",
            str(key),
            "-out",
            str(csr),
        )
        self.evidence.secret(key.read_text())
        require(key.stat().st_mode & 0o777 == 0o600, "EFFECT_KEY_MODE")
        ext.write_text(f"subjectAltName={san}\nextendedKeyUsage={usages}\n", encoding="ascii")
        self.command(
            "EFFECT_CERT_SIGN",
            "openssl",
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(self.effect_ca),
            "-CAkey",
            str(self.effect_key),
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            str(ext),
            "-out",
            str(crt),
        )
        return {
            "Certificate": encoded(crt.read_bytes()),
            "Key": encoded(key.read_bytes()),
            "CA": encoded(self.effect_ca.read_bytes()),
        }

    def tls(self) -> dict:
        self.effect_ca, self.effect_key = (
            self.scratch / "effect-ca.crt",
            self.scratch / "effect-ca.key",
        )
        private_file(self.effect_key, b"")
        self.command(
            "EFFECT_CA",
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=DUR-053 K6 C Engineering",
            "-keyout",
            str(self.effect_key),
            "-out",
            str(self.effect_ca),
        )
        self.evidence.secret(self.effect_key.read_text())
        require(self.effect_key.stat().st_mode & 0o777 == 0o600, "EFFECT_CA_MODE")
        self.sink_tls = self.cert(SINK, f"DNS:{SINK},URI:{SINK_PEER}", "serverAuth")
        self.relay_tls = self.cert(RELAY, f"DNS:{RELAY},URI:{RELAY_PEER}", "serverAuth,clientAuth")
        self.engine_tls = self.cert("engine-peer", f"URI:{ENGINE_PEER}", "clientAuth")
        witness_tls = self.cert("host-witness", "DNS:dur053-host-witness", "serverAuth")
        self.cert("sink-postgres", f"IP:{self.sink_address}", "serverAuth")
        for suffix in ("key", "crt"):
            self.command(
                "SINK_PG_TLS_COPY",
                "docker",
                "cp",
                str(self.scratch / ("sink-postgres." + suffix)),
                f"{self.sink_container}:/tmp/sink-postgres.{suffix}",
            )
        self.command(
            "SINK_PG_TLS_FILES",
            "docker",
            "exec",
            "-u",
            "0",
            self.sink_container,
            "sh",
            "-ec",
            'install -o postgres -g postgres -m 0600 /tmp/sink-postgres.key "$PGDATA/server.key"; '
            'install -o postgres -g postgres -m 0644 /tmp/sink-postgres.crt "$PGDATA/server.crt"; '
            "rm /tmp/sink-postgres.key /tmp/sink-postgres.crt; "
            "{ printf '%s\\n' 'hostnossl all all all reject'; "
            'cat "$PGDATA/pg_hba.conf"; } > "$PGDATA/k6-hba"; '
            'cat "$PGDATA/k6-hba" > "$PGDATA/pg_hba.conf"; rm "$PGDATA/k6-hba"',
        )
        self.psql("sink", "admin", "ALTER SYSTEM SET ssl = 'on'; SELECT pg_reload_conf();")
        for _ in range(20):
            if self.psql("sink", "admin", "SHOW ssl;") == "on":
                break
            time.sleep(0.5)
        else:
            require(False, "SINK_PG_TLS_NOT_ENABLED")
        private_file(
            self.witness_config,
            json_data(
                {
                    "Address": self.witness_address,
                    "Journal": str(self.journal),
                    "TLS": witness_tls,
                    "RelayPeer": RELAY_PEER,
                    "SinkPeer": SINK_PEER,
                }
            ),
        )
        return {
            "effect_ca_sha256": hashlib.sha256(self.effect_ca.read_bytes()).hexdigest(),
            "engine_peer": ENGINE_PEER,
            "relay_peer": RELAY_PEER,
            "sink_peer": SINK_PEER,
        }

    @property
    def witness_address(self) -> str:
        return self.endpoint_addresses["witness"] + ":" + self.endpoint_addresses["witness_port"]

    def start_witness(self) -> None:
        self.witness_process = subprocess.Popen(
            (str(self.witness), str(self.witness_config)),
            cwd=self.scratch,
            env=self.service_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        for _ in range(40):
            process = self.witness_process
            if process.poll() is not None:
                code = process.stderr.read(256).decode(errors="replace").strip()
                raise SmokeFailure(code if SAFE_CODE.fullmatch(code) else "WITNESS_PROCESS_EXITED")
            try:
                with socket.create_connection(
                    (
                        self.endpoint_addresses["witness"],
                        int(self.endpoint_addresses["witness_port"]),
                    ),
                    timeout=0.25,
                ):
                    return
            except OSError:
                time.sleep(0.1)
        require(False, "WITNESS_LISTENER_NOT_READY")

    def stop_witness(self) -> None:
        process, self.witness_process = self.witness_process, None
        if process is not None:
            try:
                if process.poll() is None:
                    process.terminate()
                try:
                    exit_code = process.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                    exit_code = -1
                require(exit_code == 0, "WITNESS_SHUTDOWN")
            finally:
                process.stderr.close()

    def sink_dsn(self, role: str, password: str, ca: Path | str) -> str:
        params = urlencode({"sslmode": "verify-full", "sslrootcert": str(ca), "connect_timeout": 2})
        return f"postgresql://{role}:{password}@{self.sink_address}:5432/{SINK_DB}?{params}"

    def export_config(self) -> None:
        self.export_file = self.scratch / "export.json"
        private_file(
            self.export_file,
            json_data({"ExportDSN": self.sink_dsn(EXPORTER, self.export_password, self.effect_ca)}),
        )

    def export_sink(self) -> dict:
        return json.loads(
            self.command("SINK_EXPORT", str(self.sink), "export", str(self.export_file))
        )

    def topology(self, objects: list[dict]) -> list[dict]:
        return objects

    def install_topology(self) -> tuple[dict, dict]:
        objects = render(self.namespace, self.image, self.endpoint_addresses)
        budget = budget_for(objects)
        objects = self.topology(objects)
        self.evidence.write("effect-topology.json", objects)
        self.evidence.write("connection-budget.json", budget)
        identities, episode = self.identities(self.namespace)
        self.create(
            "ENGINE_DB_SECRET",
            obj(
                "Secret",
                "campaign-db",
                self.namespace,
                stringData={"url": self.admin_dsn},
                type="Opaque",
            ),
        )
        for item in objects:
            if item["kind"] == "NetworkPolicy" or item["kind"] == "Service":
                self.create("EFFECT_TOPOLOGY", item)
        self.rendered = objects
        return next(item for item in identities if item["kind"] == "Job"), episode

    @property
    def admin_dsn(self) -> str:
        return (
            self.dsn(ENGINE_ROLE, self.harness.password, "disable")
            + f"&application_name={self.namespace}-fixture"
        )

    def deploy_effect(
        self, reviewer: str, reader_password: str, uncertainty_ns: int, *, signing_seed: str = ""
    ) -> None:
        sink_config = {
            "Address": f":{PORT}",
            "Worker": {
                "Namespace": self.namespace,
                "ServiceAccount": "k6-worker",
                "ServiceAccountUID": self.uid(self.namespace, "serviceaccount", "k6-worker"),
            },
            "APIServer": "https://" + self.endpoint_addresses["api_service"],
            "APICA": self.ca_data,
            "ReviewerToken": reviewer,
            "EnginePeerURI": ENGINE_PEER,
            "IngressPeerURI": RELAY_PEER,
            "ServerCertificate": self.sink_tls["Certificate"],
            "ServerKey": self.sink_tls["Key"],
            "ClientCA": self.sink_tls["CA"],
            "SinkDSN": self.sink_dsn(SINK_ROLE, self.sink_password, "/private/sink-db.crt"),
            "GrantReaderDSN": self.dsn(
                "k6_sink_grant_reader", reader_password, "verify-full", "/private/engine.crt"
            ),
            "ClockUncertaintyMicros": uncertainty_ns // 1000,
        }
        if signing_seed:
            sink_config.update(ReceiptSigningSeed=signing_seed, ObserveTimings=True)
        self.services = {
            name: self.k(
                "EFFECT_SERVICE_IP",
                "-n",
                self.namespace,
                "get",
                "svc",
                name,
                "-o",
                "jsonpath={.spec.clusterIP}",
            ).decode()
            for name in (SINK, RELAY)
        }
        relay_config = {
            "Address": f":{PORT}",
            "SinkAddress": f"{self.services[SINK]}:{PORT}",
            "SinkName": SINK,
            "SinkPeer": SINK_PEER,
            "RelayPeer": RELAY_PEER,
            "WorkerTLS": self.relay_tls,
            "ClientTLS": self.relay_tls,
            "WitnessAddress": self.witness_address,
            "WitnessName": "dur053-host-witness",
        }
        if signing_seed:
            relay_config["ObserveTimings"] = True
        for name, config in ((SINK, sink_config), (RELAY, relay_config)):
            data = {"config.json": encoded(json_data(config))}
            if name == SINK:
                data.update(
                    {
                        "engine.crt": encoded(self.ca.read_bytes()),
                        "sink-db.crt": encoded(self.effect_ca.read_bytes()),
                    }
                )
            self.create(
                "EFFECT_SECRET",
                obj(
                    "Secret",
                    name + "-config",
                    self.namespace,
                    data=data,
                    immutable=True,
                    type="Opaque",
                ),
            )
        for item in self.rendered:
            if item["kind"] in {"Deployment", "Pod"}:
                self.create("EFFECT_WORKLOAD", item)
        for name in (SINK, RELAY):
            self.ready(name)
        if self.live_worker:
            return
        self.k(
            "FIXTURE_CLIENTS_READY",
            "-n",
            self.namespace,
            "wait",
            "--for=condition=Ready",
            f"pod/{CLIENT}",
            f"pod/{ENGINE}",
            "--timeout=90s",
            timeout=95,
        )

    def ready(self, name: str) -> None:
        try:
            self.k(
                "EFFECT_ROLLOUT",
                "-n",
                self.namespace,
                "rollout",
                "status",
                f"deployment/{name}",
                "--timeout=60s",
                timeout=65,
            )
        except Exception:
            diagnostics = {}
            with suppress(Exception):
                pods = json.loads(
                    self.k(
                        "EFFECT_POD_STATUS",
                        "-n",
                        self.namespace,
                        "get",
                        "pod",
                        "-l",
                        f"app={name}",
                        "-o",
                        "json",
                    )
                )
                diagnostics["pods"] = [
                    {
                        key: item["status"].get(key)
                        for key in ("phase", "initContainerStatuses", "containerStatuses")
                    }
                    for item in pods["items"]
                ]
            with suppress(Exception):
                lines = (
                    self.k(
                        "EFFECT_EXIT_CODES",
                        "-n",
                        self.namespace,
                        "logs",
                        "-l",
                        f"app={name}",
                        "--all-containers=true",
                        "--tail=5",
                        timeout=10,
                    )
                    .decode(errors="replace")
                    .splitlines()
                )
                diagnostics["process_codes"] = [line for line in lines if SAFE_CODE.fullmatch(line)]
            self.evidence.write(name + "-start-diagnostics.json", diagnostics)
            raise

    def call(
        self,
        pod: str,
        target: str,
        operation: str,
        request: dict,
        *,
        cert: dict | None = None,
        token: bool = False,
        corrupt_token: bool = False,
        invocation: str = "phase-c-smoke",
        start: str | None = None,
    ) -> dict:
        return self.probe_effect(
            pod,
            "http",
            {
                "Address": f"{self.services[target]}:{PORT}",
                "Name": target,
                "Path": f"/v1/{operation}",
                "Body": encoded(json_data(request)),
                "Invocation": invocation,
                "CA": encoded(self.effect_ca.read_bytes()),
                "TokenFile": TOKEN_FILE if token else "",
                "CorruptToken": corrupt_token,
                **(cert or {}),
                **({"StartAt": start} if start else {}),
            },
        )

    def probe_effect(self, pod: str, operation: str, config: dict) -> dict:
        return json.loads(
            self.k(
                "EFFECT_PROBE",
                "-n",
                self.namespace,
                "exec",
                "-i",
                pod,
                "--",
                "/probe",
                operation,
                data=json_data(config),
                timeout=35,
            )
        )

    def seal(self) -> None:
        pod = self.k(
            "RELAY_SESSION_POD",
            "-n",
            self.namespace,
            "get",
            "pod",
            "-l",
            f"app={RELAY}",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ).decode()
        require(bool(pod), "RELAY_SESSION_POD_MISSING")
        self.k(
            "RELAY_SCALE_DOWN", "-n", self.namespace, "scale", f"deployment/{RELAY}", "--replicas=0"
        )
        self.k(
            "RELAY_DRAIN",
            "-n",
            self.namespace,
            "wait",
            "--for=delete",
            f"pod/{pod}",
            "--timeout=45s",
            timeout=50,
        )

    def teardown(self) -> None:
        failures = []
        operations = [self.stop_witness]
        if self.identity_created:
            operations.append(lambda: self.teardown_identity(self.namespace))
        if self.provisioned:
            operations.append(
                lambda: self.go("reset", DSN=self.admin_dsn, Episode={"Namespace": self.namespace})
            )
        if self.engine_created:

            def drop() -> None:
                self.sql("ENGINE_EPISODE_DROP", f'DROP DATABASE "{self.database}" WITH (FORCE);')
                self.harness.databases.discard(self.database)

            operations.append(drop)
        if self.sink_created:
            operations.append(
                lambda: self.command(
                    "SINK_POSTGRES_REMOVE", "docker", "rm", "-f", self.sink_container
                )
            )
        if self.volume_created:
            operations.append(
                lambda: self.command("SINK_DISK_REMOVE", "docker", "volume", "rm", self.sink_volume)
            )
        for operation in operations:
            try:
                operation()
            except Exception as error:
                failures.append(self.evidence.safe_reason(error))
        if failures:
            raise SmokeFailure(failures[0])
