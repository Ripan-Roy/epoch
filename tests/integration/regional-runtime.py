#!/usr/bin/env python3
"""Three-container regional catalog and multi-tablet fault campaign."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "deploy/compose/docker-compose.regional.yml"
NODES = (1, 2, 3)
FENCE_HEADERS = {
    "x-epoch-resource-generation": "1",
    "x-epoch-tablet-epoch": "1",
}
TIMEOUT_SECONDS = 30.0
ADMIN_TOKEN = "epoch-dev-admin-v1"
CONTROL_TOKEN = "epoch-dev-control-v1"
AUTH_POLICY_PATH = REPO_ROOT / "spec/auth/bootstrap-policy-v1.example.json"


@dataclass(frozen=True)
class Resource:
    kind: str
    name: str

    @property
    def route_path(self) -> str:
        return (
            "/experimental/v1/regional/resources/"
            f"acme/shop/dev/core/{self.kind}/{self.name}/shards/0"
        )

    @property
    def catalog_path(self) -> str:
        return (
            "/experimental/v1/regional/catalog/resources/"
            f"acme/shop/dev/core/{self.kind}/{self.name}"
        )


RESOURCES = (
    Resource("stream", "orders"),
    Resource("cache", "sessions"),
    Resource("queue", "jobs"),
    Resource("event-bus", "events"),
)
MANAGED_RESOURCE = Resource("stream", "managed-orders")
CAPACITY_REJECTED_RESOURCE = Resource("stream", "too-many-shards")


@dataclass(frozen=True)
class HttpResponse:
    status: int
    document: dict[str, Any]
    headers: dict[str, str]


def allocate_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        return [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


def exact_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def wait_until(description: str, check: Callable[[], Any]) -> Any:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result is not None and result is not False:
                return result
        except (OSError, ValueError, AssertionError) as error:
            last_error = error
        time.sleep(0.1)
    detail = f": {last_error}" if last_error is not None else ""
    raise AssertionError(f"timed out waiting for {description}{detail}")


class RegionalCluster:
    def __init__(self) -> None:
        ports = allocate_ports(8)
        self.http_ports = dict(zip(NODES, ports[:3], strict=True))
        self.peer_ports = dict(zip(NODES, ports[3:6], strict=True))
        self.control_http_port = ports[6]
        self.control_grpc_port = ports[7]
        self.project = os.environ.get(
            "EPOCH_REGIONAL_PROJECT_NAME", f"epoch-regional-smoke-{os.getpid()}"
        )
        self.artifact_dir = os.environ.get("EPOCH_REGIONAL_ARTIFACT_DIR")
        self.temporary_directory = tempfile.TemporaryDirectory(
            prefix="epoch-regional-control-"
        )
        self.control_binary = Path(self.temporary_directory.name) / "epoch-control"
        self.control_state_path = (
            Path(self.temporary_directory.name) / "control-registry.db"
        )
        self.control_log = (
            Path(self.temporary_directory.name) / "epoch-control.log"
        ).open("w+", encoding="utf-8")
        self.control_process: subprocess.Popen[str] | None = None
        self.environment = os.environ.copy()
        for node in NODES:
            self.environment[f"EPOCH_REGIONAL_HTTP_PORT_{node}"] = str(
                self.http_ports[node]
            )
            self.environment[f"EPOCH_REGIONAL_PEER_PORT_{node}"] = str(
                self.peer_ports[node]
            )

    def compose(
        self, *arguments: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                self.project,
                "--file",
                str(COMPOSE_FILE),
                *arguments,
            ],
            cwd=REPO_ROOT,
            env=self.environment,
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    def start(self) -> None:
        arguments = ["up", "--detach"]
        if os.environ.get("EPOCH_REGIONAL_USE_EXISTING_IMAGE") == "1":
            arguments.insert(1, "--no-build")
        else:
            arguments.insert(1, "--build")
        result = self.compose(*arguments)
        if result.stdout:
            print(result.stdout, end="")

    def service(self, node: int) -> str:
        return f"epoch-regional-{node}"

    def stop_node(self, node: int) -> None:
        self.compose("kill", "--signal", "SIGKILL", self.service(node))

    def start_node(self, node: int) -> None:
        self.compose("start", self.service(node))

    def crash_all(self) -> None:
        self.compose("kill", "--signal", "SIGKILL")

    def restart_all(self) -> None:
        self.compose("start")

    def start_control(self) -> None:
        if self.control_process is not None:
            raise AssertionError("epoch-control is already running")
        if not self.control_binary.exists():
            subprocess.run(
                [
                    "go",
                    "build",
                    "-o",
                    str(self.control_binary),
                    "./control/cmd/epoch-control",
                ],
                cwd=REPO_ROOT,
                env=self.environment,
                check=True,
                text=True,
            )
        environment = self.environment.copy()
        environment.update(
            {
                "EPOCH_CONTROL_ADDR": f"127.0.0.1:{self.control_http_port}",
                "EPOCH_CONTROL_GRPC_ADDR": f"127.0.0.1:{self.control_grpc_port}",
                "EPOCH_CONTROL_REGIONAL_ENDPOINTS": ",".join(
                    f"http://127.0.0.1:{self.http_ports[node]}" for node in NODES
                ),
                "EPOCH_CONTROL_ALLOWED_ORIGINS": "https://console.example.test",
                "EPOCH_CONTROL_RECONCILE_INTERVAL": "100ms",
                "EPOCH_CONTROL_STATE_PATH": str(self.control_state_path),
                "EPOCH_AUTH_POLICY_PATH": str(AUTH_POLICY_PATH),
                "EPOCH_CONTROL_REGIONAL_TOKEN": CONTROL_TOKEN,
            }
        )
        self.control_process = subprocess.Popen(
            [str(self.control_binary)],
            cwd=REPO_ROOT,
            env=environment,
            text=True,
            stdout=self.control_log,
            stderr=subprocess.STDOUT,
        )

        def healthy() -> bool:
            if (
                self.control_process is not None
                and self.control_process.poll() is not None
            ):
                raise AssertionError(
                    f"epoch-control exited with {self.control_process.returncode}"
                )
            return self.control_request("GET", "/healthz").status == 200

        wait_until("Go control plane to become healthy", healthy)
        health = self.control_request("GET", "/healthz")
        assert health.document.get("registry") == "bbolt_v1", health
        assert health.document.get("registry_durable") is True, health

    def crash_control(self) -> None:
        if self.control_process is None or self.control_process.poll() is not None:
            raise AssertionError("epoch-control is not running")
        self.control_process.kill()
        self.control_process.wait(timeout=5)
        self.control_process = None

    def stop_control(self) -> None:
        if self.control_process is None:
            return
        if self.control_process.poll() is None:
            self.control_process.terminate()
            try:
                self.control_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.control_process.kill()
                self.control_process.wait(timeout=5)
        self.control_process = None

    def capture_failure(self) -> None:
        self.control_log.flush()
        self.control_log.seek(0)
        control_output = self.control_log.read()
        if not self.artifact_dir:
            result = self.compose("logs", "--no-color", "--tail", "300", check=False)
            print(result.stdout, file=sys.stderr)
            print(control_output, file=sys.stderr)
            return
        destination = Path(self.artifact_dir)
        destination.mkdir(parents=True, exist_ok=True)
        for name, arguments in (
            ("containers.log", ("logs", "--no-color")),
            ("containers.txt", ("ps", "--all")),
        ):
            result = self.compose(*arguments, check=False)
            (destination / name).write_text(result.stdout, encoding="utf-8")
        (destination / "ports.json").write_text(
            json.dumps(
                {
                    "http": self.http_ports,
                    "peer": self.peer_ports,
                    "control_http": self.control_http_port,
                    "control_grpc": self.control_grpc_port,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        (destination / "epoch-control.log").write_text(control_output, encoding="utf-8")

    def close(self) -> None:
        self.stop_control()
        self.compose("down", "--volumes", "--remove-orphans", check=False)
        self.control_log.close()
        self.temporary_directory.cleanup()

    def request(
        self,
        node: int,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        request_headers = dict(headers or {})
        if path.startswith("/experimental/v1/regional/"):
            request_headers.setdefault("authorization", f"Bearer {ADMIN_TOKEN}")
        payload = None
        if body is not None:
            request_headers["content-type"] = "application/json"
            payload = json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.http_ports[node]}{path}",
            data=payload,
            headers=request_headers,
            method=method,
        )
        try:
            response = urllib.request.urlopen(request, timeout=2)
        except urllib.error.HTTPError as error:
            raw = error.read()
            return HttpResponse(
                error.code,
                json.loads(raw) if raw else {},
                dict(error.headers.items()),
            )
        with response:
            raw = response.read()
            return HttpResponse(
                response.status,
                json.loads(raw) if raw else {},
                dict(response.headers.items()),
            )

    def control_request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        request_headers = dict(headers or {})
        if path.startswith("/v1/"):
            request_headers.setdefault("authorization", f"Bearer {ADMIN_TOKEN}")
        payload = None
        if body is not None:
            request_headers["content-type"] = "application/json"
            payload = json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.control_http_port}{path}",
            data=payload,
            headers=request_headers,
            method=method,
        )
        try:
            response = urllib.request.urlopen(request, timeout=2)
        except urllib.error.HTTPError as error:
            raw = error.read()
            return HttpResponse(
                error.code,
                json.loads(raw) if raw else {},
                dict(error.headers.items()),
            )
        with response:
            raw = response.read()
            return HttpResponse(
                response.status,
                json.loads(raw) if raw else {},
                dict(response.headers.items()),
            )


def wait_for_nodes(cluster: RegionalCluster, nodes: tuple[int, ...] = NODES) -> None:
    def healthy() -> bool:
        return all(
            cluster.request(node, "GET", "/healthz").status == 200 for node in nodes
        )

    wait_until(f"nodes {nodes} to become healthy", healthy)


def wait_for_topology(cluster: RegionalCluster, expected_used_groups: int) -> None:
    expected_zones = {
        1: "ap-south-1a",
        2: "ap-south-1b",
        3: "ap-south-1c",
    }

    def observed() -> bool:
        for node in NODES:
            response = cluster.request(
                node, "GET", "/experimental/v1/regional/topology"
            )
            if response.status != 200:
                return False
            capacity = response.document.get("capacity")
            if (
                response.document.get("node_id") != str(node)
                or response.document.get("region") != "ap-south"
                or response.document.get("zone") != expected_zones[node]
                or response.document.get("node_class") != "general-purpose"
                or response.document.get("consensus_voter_node_ids") != ["1", "2", "3"]
                or not isinstance(capacity, dict)
                or capacity.get("max_consensus_groups") != 16
                or capacity.get("used_consensus_groups") != expected_used_groups
                or capacity.get("available_consensus_groups")
                != 16 - expected_used_groups
            ):
                return False
        return True

    wait_until(
        f"regional topology to report {expected_used_groups} used groups",
        observed,
    )


def create_resource(cluster: RegionalCluster, resource: Resource) -> None:
    body = {
        "request_token": f"container-create-{resource.kind}-{resource.name}-v1",
        "expected_generation": "0",
        "shard_count": 1,
        "replica_count": 3,
    }

    def created() -> bool:
        for node in NODES:
            response = cluster.request(node, "PUT", resource.catalog_path, body)
            if response.status == 201:
                return True
        return False

    wait_until(f"catalog creation of {resource.kind}/{resource.name}", created)


def managed_resource_request(
    resource: Resource, shard_count: int = 1
) -> dict[str, Any]:
    return {
        "request_token": f"managed-create-{resource.kind}-{resource.name}-v1",
        "expected_generation": 0,
        "resource": {
            "organization": "acme",
            "project": "shop",
            "environment": "dev",
            "namespace": "core",
            "kind": resource.kind,
            "name": resource.name,
            "spec": {
                "shard_count": shard_count,
                "replica_count": 3,
                "placement": {
                    "allowed_regions": ["ap-south"],
                    "minimum_zones": 3,
                    "required_node_class": "general-purpose",
                },
            },
        },
    }


def create_managed_resource(cluster: RegionalCluster, resource: Resource) -> None:
    response = cluster.control_request(
        "PUT", "/v1/resources", managed_resource_request(resource)
    )
    assert response.status == 201, response


def prove_capacity_rejection(cluster: RegionalCluster) -> None:
    response = cluster.control_request(
        "PUT",
        "/v1/resources",
        managed_resource_request(CAPACITY_REJECTED_RESOURCE, 15),
    )
    assert response.status == 201, response
    canonical_name = "acme/shop/dev/core/stream/" + CAPACITY_REJECTED_RESOURCE.name

    def rejected() -> bool:
        inventory = cluster.control_request("GET", "/v1/regional/resources")
        resources = inventory.document.get("resources")
        if inventory.status != 200 or not isinstance(resources, list):
            return False
        matching = [
            item
            for item in resources
            if isinstance(item, dict) and item.get("canonical_name") == canonical_name
        ]
        return (
            len(matching) == 1
            and matching[0].get("phase") == "failed"
            and matching[0].get("observed_generation") == "0"
            and "consensus_group_capacity" in str(matching[0].get("message", ""))
            and matching[0].get("tablets") == []
        )

    wait_until("capacity admission to reject before catalog apply", rejected)
    for node in NODES:
        catalog = cluster.request(node, "GET", CAPACITY_REJECTED_RESOURCE.catalog_path)
        assert catalog.status == 404, catalog


def replay_managed_resource(cluster: RegionalCluster, resource: Resource) -> None:
    response = cluster.control_request(
        "PUT", "/v1/resources", managed_resource_request(resource)
    )
    assert response.status == 200, response
    assert response.document.get("replayed") is True, response
    stored = response.document.get("resource")
    assert isinstance(stored, dict) and stored.get("generation") == 1, response


def wait_for_managed_placement(
    cluster: RegionalCluster,
    resource: Resource,
    phase: str,
    expected_voters: int,
) -> dict[str, Any]:
    canonical_name = f"acme/shop/dev/core/{resource.kind}/{resource.name}"
    last_inventory: dict[str, Any] | None = None

    def observed() -> dict[str, Any] | None:
        nonlocal last_inventory
        response = cluster.control_request(
            "GET",
            "/v1/regional/resources",
            headers={"origin": "https://console.example.test"},
        )
        last_inventory = {
            "status": response.status,
            "cors_origin": response.headers.get("Access-Control-Allow-Origin"),
            "document": response.document,
        }
        if (
            response.status != 200
            or response.headers.get("Access-Control-Allow-Origin")
            != "https://console.example.test"
        ):
            return None
        resources = response.document.get("resources")
        if not isinstance(resources, list):
            return None
        matched = [
            item
            for item in resources
            if isinstance(item, dict) and item.get("canonical_name") == canonical_name
        ]
        if len(matched) != 1:
            return None
        managed = matched[0]
        if (
            managed.get("phase") != phase
            or managed.get("generation") != "1"
            or managed.get("observed_generation") != "1"
            or managed.get("shard_count") != 1
        ):
            return None
        tablets = managed.get("tablets")
        if not isinstance(tablets, list):
            return None
        if expected_voters == 0:
            return managed if len(tablets) == 0 else None
        if len(tablets) != 1 or not isinstance(tablets[0], dict):
            return None
        tablet = tablets[0]
        for field in (
            "tablet_id",
            "consensus_group_id",
            "tablet_epoch",
            "resource_generation",
        ):
            value = tablet.get(field)
            if not isinstance(value, str) or not value.isdecimal():
                return None
        voters = tablet.get("voter_node_ids")
        if (
            not isinstance(voters, list)
            or len(voters) != expected_voters
            or any(
                not isinstance(voter, str) or not voter.isdecimal() for voter in voters
            )
        ):
            return None
        leader = tablet.get("leader_node_id")
        if not isinstance(leader, str) or leader not in voters:
            return None
        placement = managed.get("placement")
        if not isinstance(placement, dict):
            return None
        nodes = placement.get("nodes")
        if (
            placement.get("minimum_zones") != 3
            or placement.get("achieved_zones") != 3
            or not isinstance(nodes, list)
            or len(nodes) != 3
            or {node.get("zone") for node in nodes if isinstance(node, dict)}
            != {"ap-south-1a", "ap-south-1b", "ap-south-1c"}
        ):
            return None
        return managed

    try:
        return wait_until(
            f"Go BFF to report {phase} placement for {resource.kind}/{resource.name}",
            observed,
        )
    except AssertionError as error:
        inventory = json.dumps(last_inventory, sort_keys=True, separators=(",", ":"))
        raise AssertionError(f"{error}; last_inventory={inventory}") from error


def wait_for_routes(
    cluster: RegionalCluster,
    resource: Resource,
    nodes: tuple[int, ...] = NODES,
) -> tuple[int, int]:
    def converged() -> tuple[int, int] | None:
        routes: list[tuple[int, dict[str, Any]]] = []
        for node in nodes:
            response = cluster.request(node, "GET", resource.route_path)
            if response.status != 200:
                return None
            routes.append((node, response.document))
        leaders = [
            (node, exact_int(route.get("term")))
            for node, route in routes
            if route.get("accepts_writes") is True
        ]
        if len(leaders) != 1 or leaders[0][1] is None:
            return None
        return leaders[0][0], leaders[0][1]

    return wait_until(
        f"{resource.kind}/{resource.name} routes on nodes {nodes}", converged
    )


def profile_write(resource: Resource, term: int, sequence: int) -> tuple[str, dict]:
    expected_term = str(term)
    if resource.kind == "stream":
        return "records", {
            "idempotency_key": f"container-stream-{sequence}",
            "expected_term": expected_term,
            "partition": 0,
            "envelope": {
                "id": f"order-{sequence}",
                "source": "regional-container-test",
                "type": "order.created",
                "time_ms": str(sequence),
                "payload": {"id": sequence},
            },
        }
    if resource.kind == "cache":
        return "mutations", {
            "idempotency_key": f"container-cache-{sequence}",
            "expected_term": expected_term,
            "operation": {
                "kind": "set",
                "key": f"session-{sequence}",
                "value": {"kind": "string", "value": "ready"},
            },
        }
    envelope_type = "job.created" if resource.kind == "queue" else "order.created"
    operation_kind = "enqueue" if resource.kind == "queue" else "publish"
    return "mutations", {
        "idempotency_key": f"container-{resource.kind}-{sequence}",
        "expected_term": expected_term,
        "operation": {
            "kind": operation_kind,
            "envelope": {
                "id": f"{resource.name}-{sequence}",
                "source": "regional-container-test",
                "type": envelope_type,
                "time_ms": str(sequence),
                "payload": {"id": sequence},
            },
        },
    }


def write_profile(
    cluster: RegionalCluster,
    resource: Resource,
    sequence: int,
    nodes: tuple[int, ...] = NODES,
) -> tuple[int, int]:
    leader, term = wait_for_routes(cluster, resource, nodes)
    operation, body = profile_write(resource, term, sequence)
    response = cluster.request(
        leader,
        "POST",
        f"{resource.route_path}/data/{operation}",
        body,
        FENCE_HEADERS,
    )
    assert 200 <= response.status < 300, (
        resource,
        response.status,
        response.document,
    )
    assert response.document.get("state") == "committed", response.document
    return leader, term


def wait_for_profile_apply(
    cluster: RegionalCluster,
    resource: Resource,
    expected: int,
    nodes: tuple[int, ...] = NODES,
) -> str:
    def converged() -> str | None:
        digests: list[str] = []
        for node in nodes:
            response = cluster.request(
                node,
                "GET",
                f"{resource.route_path}/data/status",
                headers=FENCE_HEADERS,
            )
            if response.status != 200:
                return None
            if exact_int(response.document.get("applied_command_count")) != expected:
                return None
            digest = response.document.get("state_digest")
            if not isinstance(digest, str):
                return None
            digests.append(digest)
        return digests[0] if len(set(digests)) == 1 else None

    return wait_until(
        f"{resource.kind}/{resource.name} to apply {expected} commands on {nodes}",
        converged,
    )


def wait_for_catalog(cluster: RegionalCluster, expected: int) -> str:
    def converged() -> str | None:
        digests: list[str] = []
        for node in NODES:
            response = cluster.request(node, "GET", "/experimental/v1/regional/catalog")
            if response.status != 200:
                return None
            if (
                exact_int(response.document.get("resource_count")) != expected
                or exact_int(response.document.get("tablet_count")) != expected
            ):
                return None
            digest = response.document.get("state_digest")
            if not isinstance(digest, str):
                return None
            digests.append(digest)
        return digests[0] if len(set(digests)) == 1 else None

    return wait_until(f"catalog convergence for {expected} resources", converged)


def run_campaign(cluster: RegionalCluster) -> None:
    cluster.start()
    wait_for_nodes(cluster)
    wait_for_topology(cluster, 1)
    cluster.start_control()
    create_managed_resource(cluster, MANAGED_RESOURCE)
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "ready", 3)
    wait_for_topology(cluster, 2)
    prove_capacity_rejection(cluster)
    write_profile(cluster, MANAGED_RESOURCE, 1)
    wait_for_profile_apply(cluster, MANAGED_RESOURCE, 1)
    cluster.crash_control()
    cluster.start_control()
    replay_managed_resource(cluster, MANAGED_RESOURCE)
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "ready", 3)
    wait_for_profile_apply(cluster, MANAGED_RESOURCE, 1)
    for resource in RESOURCES:
        create_resource(cluster, resource)
        write_profile(cluster, resource, 1)
        wait_for_profile_apply(cluster, resource, 1)
    expected_resources = len(RESOURCES) + 1
    initial_catalog_digest = wait_for_catalog(cluster, expected_resources)

    stream = MANAGED_RESOURCE
    old_leader, old_term = wait_for_routes(cluster, stream)
    cluster.stop_node(old_leader)
    survivors = tuple(node for node in NODES if node != old_leader)
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "degraded", 2)
    new_leader, new_term = write_profile(cluster, stream, 2, survivors)
    assert new_leader != old_leader
    assert new_term > old_term
    wait_for_profile_apply(cluster, stream, 2, survivors)

    cluster.start_node(old_leader)
    wait_for_nodes(cluster)
    wait_for_profile_apply(cluster, stream, 2)
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "ready", 3)
    for resource in RESOURCES:
        wait_for_profile_apply(cluster, resource, 1)

    cluster.crash_all()
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "pending", 0)
    cluster.restart_all()
    wait_for_nodes(cluster)
    assert wait_for_catalog(cluster, expected_resources) == initial_catalog_digest
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "ready", 3)
    wait_for_profile_apply(cluster, stream, 2)
    for resource in RESOURCES:
        wait_for_profile_apply(cluster, resource, 1)


def main() -> int:
    cluster = RegionalCluster()
    failed = False
    try:
        run_campaign(cluster)
    except BaseException:
        failed = True
        cluster.capture_failure()
        raise
    finally:
        cluster.close()
    if not failed:
        print(
            "Epoch Go-to-Rust regional catalog/BFF/four-profile/failover/"
            "control-SIGKILL/all-node recovery container campaign passed."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
