#!/usr/bin/env python3
"""Named protocol clients against real authenticated, replicated Epoch tablets."""

from __future__ import annotations

import importlib.util
import json
import os
import platform
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any


MODULE_PATH = Path(__file__).with_name("regional-runtime.py")
MODULE_SPEC = importlib.util.spec_from_file_location("regional_runtime", MODULE_PATH)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"could not load {MODULE_PATH}")
regional = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = regional
MODULE_SPEC.loader.exec_module(regional)

RESULT_SCHEMA = "epoch.protocol-regional.evidence/v2"
CLIENTS = {"redis": "8.8.2", "kafka": "4.3.1", "rabbitmq": "5.35.0"}
REDIS_IMAGE = (
    "redis@sha256:2b42a93631132be6df7a31f843b91ea8a907011e955b03395b7edbb13a20a99d"
)
REQUIRED_CHECKS = (
    "released_client_lifecycle",
    "redis_binary_counter_ttl",
    "redis_atomic_set_get",
    "redis_atomic_multi_set",
    "redis_native_collections",
    "kafka_four_codecs_nullable_headers_checkpoint",
    "kafka_native_consumer_groups",
    "kafka_static_identity_rejoin",
    "amqp_confirm_nack_requeue",
    "amqp_topic_ttl_mandatory_return",
    "amqp_headers_and_native_dead_letter",
    "native_rejection_not_acknowledged",
    "disconnected_lease_redelivery",
    "acknowledged_messages_stay_absent",
    "same_volume_reopen",
    "replica_digest_convergence",
)
RESOURCES = (
    (regional.Resource("cache", "sessions"), 1),
    (regional.Resource("stream", "events"), 2),
    (regional.Resource("queue", "jobs"), 1),
    (regional.Resource("queue", "leases"), 1),
    (regional.Resource("queue", "limited"), 1),
    (regional.Resource("queue", "audit"), 1),
    (regional.Resource("queue", "failed-jobs"), 1),
)
RESOURCE_COUNT = len(RESOURCES)
TABLET_COUNT = sum(shards for _, shards in RESOURCES)


def validate_evidence(evidence: dict[str, Any]) -> None:
    if evidence.get("schema") != RESULT_SCHEMA or evidence.get("clients") != CLIENTS:
        raise ValueError("wrong evidence schema or released client versions")
    if evidence.get("checks") != list(REQUIRED_CHECKS):
        raise ValueError("incomplete protocol recovery checks")
    faults = evidence.get("faults", [])
    if [fault.get("kind") for fault in faults] != [
        "gateway_sigkill",
        "leader_sigkill",
        "leader_sigkill",
        "leader_sigkill",
        "all_voter_sigkill_reopen",
    ]:
        raise ValueError("incomplete or reordered fault history")
    leaders = faults[1:4]
    if [fault.get("profile") for fault in leaders] != ["cache", "stream", "queue"]:
        raise ValueError("missing profile leader failure")
    for fault in leaders:
        if any(
            type(fault.get(field)) is not int
            for field in ("old_leader", "new_leader", "old_term", "new_term")
        ) or not (
            fault["old_leader"] in regional.NODES
            and fault["new_leader"] in regional.NODES
            and fault["new_leader"] != fault["old_leader"]
            and 0 < fault["old_term"] < fault["new_term"]
        ):
            raise ValueError("leader failure did not advance leadership and term")


def process_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return value


class ProtocolCampaign:
    def __init__(self) -> None:
        self.cluster = regional.RegionalCluster()
        self.directory = Path(self.cluster.temporary_directory.name)
        self.redis_port, self.kafka_port, self.amqp_port = regional.allocate_ports(3)
        self.gateway = f"{self.cluster.project}-compat"
        self.gateway_created = False
        self.classpath = ""
        self.log = (self.directory / "clients.log").open("w+", encoding="utf-8")

    def command(self, *arguments: str, **kwargs: Any) -> subprocess.CompletedProcess:
        result = subprocess.run(
            arguments,
            cwd=regional.REPO_ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
            **kwargs,
        )
        for output in (result.stdout, result.stderr):
            self.log.write(output.decode("utf-8", errors="replace"))
        self.log.flush()
        result.check_returncode()
        return result

    def prepare_clients(self) -> None:
        version = self.command(
            "docker", "run", "--rm", REDIS_IMAGE, "redis-cli", "--version"
        )
        assert version.stdout.strip() == b"redis-cli 8.8.2", version.stdout
        self.command(
            "./scripts/retry-command.sh",
            "3",
            "2",
            "./sdk/java/mvnw",
            "--file",
            "tests/compatibility/java/pom.xml",
            "--batch-mode",
            "--no-transfer-progress",
            "dependency:build-classpath",
            "-Dmdep.outputFile=target/runtime-classpath.txt",
        )
        dependencies = (
            (
                regional.REPO_ROOT
                / "tests/compatibility/java/target/runtime-classpath.txt"
            )
            .read_text()
            .strip()
        )
        classes = self.directory / "classes"
        classes.mkdir()
        self.command(
            "javac",
            "--release",
            "17",
            "-Xlint:all",
            "-Werror",
            "-cp",
            dependencies,
            "-d",
            str(classes),
            "tests/compatibility/java/ProtocolConformance.java",
            "tests/compatibility/java/RegionalRecoveryConformance.java",
        )
        self.classpath = f"{classes}:{dependencies}"

    def java(self, phase: str) -> None:
        arguments = ["java", "-cp", self.classpath]
        arguments.append(
            "ProtocolConformance"
            if phase == "lifecycle"
            else "RegionalRecoveryConformance"
        )
        arguments.extend(["127.0.0.1", str(self.kafka_port), str(self.amqp_port)])
        if phase != "lifecycle":
            arguments.append(phase)
        self.command(*arguments)
        print(f"Protocol client phase passed: {phase}", flush=True)

    def redis(self, *arguments: str, body: bytes | None = None) -> bytes:
        network = ["--add-host", "host.docker.internal:host-gateway"]
        host = "host.docker.internal"
        if platform.system() == "Linux":
            network, host = ["--network", "host"], "127.0.0.1"
        command = [
            "docker",
            "run",
            "--rm",
            "--interactive",
            *network,
            REDIS_IMAGE,
            "redis-cli",
            "--raw",
            "--no-auth-warning",
            "-h",
            host,
            "-p",
            str(self.redis_port),
            "-a",
            "compat-secret",
        ]
        if body is not None:
            command.append("-x")
        return self.command(*command, *arguments, input=body).stdout

    def start_gateway(self) -> None:
        environment = {
            "EPOCH_COMPAT_ENDPOINTS": ",".join(
                f"http://{self.cluster.service(node)}:7601" for node in regional.NODES
            ),
            "EPOCH_COMPAT_TOKEN": regional.ADMIN_TOKEN,
            "EPOCH_COMPAT_REDIS_PASSWORD": "compat-secret",
            "EPOCH_COMPAT_AMQP_PASSWORD": "compat-secret",
            "EPOCH_COMPAT_KAFKA_ADVERTISED_HOST": "127.0.0.1",
            "EPOCH_COMPAT_KAFKA_ADVERTISED_PORT": str(self.kafka_port),
            "EPOCH_COMPAT_BACKEND_TIMEOUT_MS": "2000",
        }
        arguments = [
            "docker",
            "run",
            "--detach",
            "--name",
            self.gateway,
            "--network",
            f"{self.cluster.project}_default",
        ]
        for external, internal in (
            (self.redis_port, 6379),
            (self.kafka_port, 9092),
            (self.amqp_port, 5672),
        ):
            arguments.extend(["--publish", f"127.0.0.1:{external}:{internal}"])
        for key, value in environment.items():
            arguments.extend(["--env", f"{key}={value}"])
        arguments.append(os.environ.get("EPOCH_COMPAT_IMAGE", "epoch/compat:regional"))
        self.command(*arguments)
        self.gateway_created = True
        self.wait_gateway()

    def wait_gateway(self) -> None:
        def healthy() -> bool:
            for port in (self.redis_port, self.kafka_port, self.amqp_port):
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    pass
            return self.redis("PING") == b"PONG\n"

        regional.wait_until("all real compatibility listeners", healthy)

    def stop_container(self, container: str) -> None:
        self.command("docker", "kill", "--signal", "SIGKILL", container)

        def stopped() -> bool:
            result = self.command(
                "docker", "inspect", "--format", "{{.State.Running}}", container
            )
            if result.stdout.strip() not in (b"true", b"false"):
                raise ValueError("invalid Docker container state")
            return result.stdout.strip() == b"false"

        regional.wait_until(f"container {container} to finish exiting", stopped)

    def replace_gateway(self) -> None:
        self.stop_container(self.gateway)
        self.command("docker", "rm", self.gateway)
        self.gateway_created = False
        self.start_gateway()

    def provision(self) -> None:
        self.cluster.start()
        regional.wait_for_nodes(self.cluster)
        for resource, shards in RESOURCES:
            body = {
                "request_token": f"compat-{resource.kind}-{resource.name}",
                "expected_generation": "0",
                "shard_count": shards,
                "replica_count": 3,
            }
            if resource.kind == "queue":
                # Requeue verification intentionally consumes delivery attempts.
                # Keep the campaign below an explicit native retry ceiling.
                body["configuration"] = {
                    "durability": "quorum_durable",
                    "visibility_timeout_ms": 30_000,
                    "max_messages": 1 if resource.name == "limited" else 100,
                    "retry": {
                        "strategy": "fixed",
                        "initial_delay_ms": 0,
                        "max_delay_ms": 0,
                        "jitter_percent": 0,
                        "max_attempts": 100,
                        "max_age_ms": None,
                    },
                    "dedupe_window_ms": 60_000,
                }
                if resource.name == "jobs":
                    body["configuration"]["advanced"] = {
                        "dead_letter_target": "failed-jobs"
                    }

            def created() -> bool:
                return any(
                    self.cluster.request(
                        node, "PUT", resource.catalog_path, body
                    ).status
                    == 201
                    for node in regional.NODES
                )

            regional.wait_until(f"create {resource.kind}/{resource.name}", created)
        regional.wait_for_catalog(self.cluster, RESOURCE_COUNT, TABLET_COUNT)
        self.routes()

    def routes(self, nodes: tuple[int, ...] = regional.NODES) -> None:
        for resource, shards in RESOURCES:
            for shard in range(shards):
                regional.wait_for_routes(self.cluster, resource, nodes, shard)

    def verify_redis(self) -> None:
        assert self.redis("GET", "durable-binary") == b"persisted\x00\xffvalue\n"
        assert self.redis("GET", "durable-counter") == b"7\n"
        assert self.redis("GET", "atomic-set") == b"second\n"
        assert self.redis("MGET", "durable-one", "durable-two") == b"one\ntwo\n"
        assert self.redis("GET", "durable-three") == b"three\n"
        assert self.redis("GET", "durable-four") == b"four\n"
        ttl = int(self.redis("PTTL", "durable-binary"))
        assert 0 < ttl <= 600_000, ttl
        assert self.redis("HGET", "durable-hash", "stage") == b"beta\n"
        assert self.redis("HGET", "durable-hash", "verified") == b"true\n"
        assert (
            self.redis("LRANGE", "durable-list", "0", "-1") == b"first\nsecond\nthird\n"
        )
        assert self.redis("SCARD", "durable-set") == b"2\n"
        assert self.redis("SISMEMBER", "durable-set", "reader") == b"1\n"
        assert self.redis("ZSCORE", "durable-ranking", "grace") == b"0.5\n"
        structured_ttl = int(self.redis("PTTL", "durable-hash"))
        assert 0 < structured_ttl <= 600_000, structured_ttl

    def verify(self, phase: str = "verify") -> None:
        self.verify_redis()
        self.java(phase)

    def run(self) -> dict[str, Any]:
        self.prepare_clients()
        self.provision()
        self.start_gateway()
        self.java("lifecycle")
        self.java("capacity")
        assert (
            self.redis("SET", "durable-binary", body=b"persisted\x00\xffvalue")
            == b"OK\n"
        )
        assert self.redis("PEXPIRE", "durable-binary", "600000") == b"1\n"
        assert self.redis("INCRBY", "durable-counter", "7") == b"7\n"
        assert self.redis("SET", "atomic-set", "first") == b"OK\n"
        assert self.redis("SET", "atomic-set", "blocked", "NX", "GET") == b"first\n"
        assert self.redis("GET", "atomic-set") == b"first\n"
        assert self.redis("SET", "atomic-set", "second", "GET") == b"first\n"
        assert self.redis("MSET", "durable-one", "one", "durable-two", "two") == b"OK\n"
        assert (
            self.redis("MSETNX", "durable-one", "changed", "durable-three", "blocked")
            == b"0\n"
        )
        assert self.redis("GET", "durable-one") == b"one\n"
        assert self.redis("GET", "durable-three") == b"\n"
        assert (
            self.redis("MSETNX", "durable-three", "three", "durable-four", "four")
            == b"1\n"
        )
        assert self.redis("HSET", "durable-hash", "stage", "beta") == b"1\n"
        assert self.redis("PEXPIRE", "durable-hash", "600000") == b"1\n"
        assert self.redis("HSET", "durable-hash", "verified", "true") == b"1\n"
        assert self.redis("RPUSH", "durable-list", "first", "second", "third") == b"3\n"
        assert self.redis("SADD", "durable-set", "reader", "writer", "reader") == b"2\n"
        assert (
            self.redis("ZADD", "durable-ranking", "1.5", "ada", "0.5", "grace")
            == b"2\n"
        )
        self.java("seed")
        self.verify()
        faults: list[dict[str, Any]] = []
        self.replace_gateway()
        self.verify()
        faults.append({"kind": "gateway_sigkill"})
        for resource, _ in RESOURCES[:3]:
            leader, term = regional.wait_for_routes(self.cluster, resource)
            container = self.cluster.compose(
                "ps", "--all", "--quiet", self.cluster.service(leader)
            ).stdout.strip()
            assert container, "leader container is missing"
            self.stop_container(container)
            survivors = tuple(node for node in regional.NODES if node != leader)
            self.routes(survivors)
            new_leader, new_term = regional.wait_for_routes(
                self.cluster, resource, survivors
            )
            assert new_leader != leader and new_term > term
            self.verify()
            faults.append(
                {
                    "kind": "leader_sigkill",
                    "profile": resource.kind,
                    "old_leader": leader,
                    "new_leader": new_leader,
                    "old_term": term,
                    "new_term": new_term,
                }
            )
            self.cluster.start_node(leader)
            regional.wait_for_nodes(self.cluster)
            self.routes()
        self.java("settle")
        for resource, shards in RESOURCES:
            for shard in range(shards):
                regional.wait_for_profile_convergence(
                    self.cluster, resource, 1, shard=shard
                )
        catalog = regional.wait_for_catalog(self.cluster, RESOURCE_COUNT, TABLET_COUNT)
        regional.wait_for_automatic_checkpoints(self.cluster, TABLET_COUNT + 1, False)
        self.command(
            str(regional.REPO_ROOT / "scripts/crash-restart-compose-services.sh"),
            self.cluster.project,
            str(regional.COMPOSE_FILE),
            *(self.cluster.service(node) for node in regional.NODES),
            env=self.cluster.environment,
        )
        regional.wait_for_nodes(self.cluster)
        self.routes()
        assert (
            regional.wait_for_catalog(self.cluster, RESOURCE_COUNT, TABLET_COUNT)
            == catalog
        )
        self.verify("empty")
        self.replace_gateway()
        self.verify("empty")
        for resource, shards in RESOURCES:
            for shard in range(shards):
                regional.wait_for_profile_convergence(
                    self.cluster, resource, 1, shard=shard
                )
        faults.append({"kind": "all_voter_sigkill_reopen"})
        result = {
            "schema": RESULT_SCHEMA,
            "clients": CLIENTS,
            "faults": faults,
            "checks": list(REQUIRED_CHECKS),
            "catalog_digest": catalog,
            "source_revision": self.command("git", "rev-parse", "HEAD")
            .stdout.decode()
            .strip(),
            "tracked_source_dirty": bool(
                self.command("git", "diff", "--name-only", "HEAD").stdout
            ),
            "images": {},
        }
        for component, image in (
            ("node", os.environ.get("EPOCH_REGIONAL_IMAGE", "epoch/node:regional")),
            ("compat", os.environ.get("EPOCH_COMPAT_IMAGE", "epoch/compat:regional")),
        ):
            result["images"][component] = (
                self.command("docker", "image", "inspect", "--format", "{{.Id}}", image)
                .stdout.decode()
                .strip()
            )
        validate_evidence(result)
        return result

    def close(self) -> None:
        if self.gateway_created:
            subprocess.run(
                ["docker", "rm", "--force", self.gateway],
                check=False,
                capture_output=True,
            )
        self.log.close()
        self.cluster.close()

    def capture(self) -> None:
        self.log.flush()
        destination = Path(
            os.environ.get("EPOCH_COMPAT_ARTIFACT_DIR", str(self.directory))
        )
        destination.mkdir(parents=True, exist_ok=True)
        clients = (self.directory / "clients.log").read_bytes()
        (destination / "clients.log").write_bytes(clients)
        if self.gateway_created:
            logs = subprocess.run(
                ["docker", "logs", self.gateway], check=False, capture_output=True
            )
            (destination / "gateway.log").write_bytes(logs.stdout + logs.stderr)
        self.cluster.artifact_dir = str(destination)
        self.cluster.capture_failure()


def main() -> int:
    campaign = ProtocolCampaign()
    try:
        result = campaign.run()
        destination = os.environ.get("EPOCH_COMPAT_ARTIFACT_DIR")
        if destination:
            Path(destination).mkdir(parents=True, exist_ok=True)
            (Path(destination) / "evidence.json").write_text(
                json.dumps(result, indent=2) + "\n"
            )
        campaign.capture()
        print(json.dumps(result, indent=2), flush=True)
        return 0
    except Exception as error:
        if isinstance(error, subprocess.CalledProcessError):
            print(process_output(error.stdout), file=sys.stderr)
            print(process_output(error.stderr), file=sys.stderr)
        campaign.capture()
        raise
    finally:
        campaign.close()


if __name__ == "__main__":
    raise SystemExit(main())
