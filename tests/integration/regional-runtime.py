#!/usr/bin/env python3
"""Three-container regional catalog and multi-tablet fault campaign."""

from __future__ import annotations

import json
import importlib
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
LOCAL_STALE_FENCE_HEADERS = {
    **FENCE_HEADERS,
    "x-epoch-read-consistency": "local_stale",
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
        return self.route_path_for(0)

    def route_path_for(self, shard: int) -> str:
        return (
            "/experimental/v1/regional/resources/"
            f"acme/shop/dev/core/{self.kind}/{self.name}/shards/{shard}"
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
MANAGED_STREAM_SHARDS = 3
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
        try:
            result = self.compose(*arguments)
        except subprocess.CalledProcessError as error:
            if error.stdout:
                print(error.stdout, end="")
            raise
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
            maintenance = response.document.get("maintenance")
            checkpoints = response.document.get("checkpoints")
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
                or not isinstance(maintenance, dict)
                or maintenance.get("enabled") is not True
                or maintenance.get("interval_ms") != 100
                or exact_int(maintenance.get("passes")) is None
                or exact_int(maintenance.get("errors")) != 0
                or not isinstance(checkpoints, dict)
                or checkpoints.get("enabled") is not True
                or checkpoints.get("interval_ms") != 100
                or checkpoints.get("min_applied_entries") != 1
                or exact_int(checkpoints.get("passes")) is None
                or exact_int(checkpoints.get("errors")) != 0
            ):
                return False
        return True

    wait_until(
        f"regional topology to report {expected_used_groups} used groups",
        observed,
    )


def wait_for_automatic_checkpoints(
    cluster: RegionalCluster,
    expected_groups: int,
    require_local_creation: bool,
) -> None:
    def compacted() -> bool:
        for node in NODES:
            response = cluster.request(
                node, "GET", "/experimental/v1/regional/topology"
            )
            if response.status != 200:
                return False
            checkpoints = response.document.get("checkpoints")
            if not isinstance(checkpoints, dict):
                return False
            groups = checkpoints.get("groups")
            if (
                checkpoints.get("enabled") is not True
                or checkpoints.get("interval_ms") != 100
                or checkpoints.get("min_applied_entries") != 1
                or exact_int(checkpoints.get("errors")) != 0
                or exact_int(checkpoints.get("passes")) is None
                or not isinstance(groups, list)
                or len(groups) != expected_groups
            ):
                return False
            if require_local_creation:
                created = exact_int(checkpoints.get("checkpoints_created"))
                reclaimed = exact_int(checkpoints.get("compacted_log_entries"))
                if created is None or created < expected_groups or reclaimed is None or reclaimed == 0:
                    return False
            for group in groups:
                if not isinstance(group, dict):
                    return False
                applied = exact_int(group.get("applied_index"))
                checkpoint = exact_int(group.get("checkpoint_index"))
                retained = exact_int(group.get("retained_log_first_index"))
                if (
                    applied is None
                    or applied == 0
                    or checkpoint != applied
                    or retained != checkpoint + 1
                ):
                    return False
        return True

    wait_until(
        f"all {expected_groups} local consensus groups to checkpoint and compact",
        compacted,
    )


def current_maintenance_submissions(cluster: RegionalCluster) -> int:
    total = 0
    responding_nodes = 0
    for node in NODES:
        try:
            response = cluster.request(
                node, "GET", "/experimental/v1/regional/topology"
            )
        except OSError:
            continue
        if response.status != 200:
            continue
        maintenance = response.document.get("maintenance")
        assert isinstance(maintenance, dict), response.document
        assert maintenance.get("enabled") is True, maintenance
        assert maintenance.get("interval_ms") == 100, maintenance
        assert exact_int(maintenance.get("errors")) == 0, maintenance
        submissions = exact_int(maintenance.get("proposals_submitted"))
        assert submissions is not None, maintenance
        total += submissions
        responding_nodes += 1
    assert responding_nodes >= 2, responding_nodes
    return total


def wait_for_maintenance_submission(
    cluster: RegionalCluster, previous_submissions: int
) -> int:
    return wait_until(
        "a leader-proposed regional maintenance command",
        lambda: (
            current
            if (current := current_maintenance_submissions(cluster))
            > previous_submissions
            else None
        ),
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
        "PUT",
        "/v1/resources",
        managed_resource_request(resource, MANAGED_STREAM_SHARDS),
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
        "PUT",
        "/v1/resources",
        managed_resource_request(resource, MANAGED_STREAM_SHARDS),
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
    expected_shards: int = MANAGED_STREAM_SHARDS,
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
            or managed.get("shard_count") != expected_shards
        ):
            return None
        tablets = managed.get("tablets")
        if not isinstance(tablets, list):
            return None
        if expected_voters == 0:
            return managed if len(tablets) == 0 else None
        if len(tablets) != expected_shards or any(
            not isinstance(tablet, dict) for tablet in tablets
        ):
            return None
        for shard_index, tablet in enumerate(tablets):
            if tablet.get("shard_index") != shard_index:
                return None
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
                    not isinstance(voter, str) or not voter.isdecimal()
                    for voter in voters
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
    shard: int = 0,
) -> tuple[int, int]:
    def converged() -> tuple[int, int] | None:
        routes: list[tuple[int, dict[str, Any]]] = []
        for node in nodes:
            response = cluster.request(node, "GET", resource.route_path_for(shard))
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
        f"{resource.kind}/{resource.name} shard {shard} routes on nodes {nodes}",
        converged,
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
    shard: int = 0,
) -> str:
    def converged() -> str | None:
        digests: list[str] = []
        for node in nodes:
            response = cluster.request(
                node,
                "GET",
                f"{resource.route_path_for(shard)}/data/status",
                headers=LOCAL_STALE_FENCE_HEADERS,
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
        f"{resource.kind}/{resource.name} shard {shard} to apply {expected} commands on {nodes}",
        converged,
    )


def wait_for_catalog(
    cluster: RegionalCluster, expected_resources: int, expected_tablets: int
) -> str:
    def converged() -> str | None:
        digests: list[str] = []
        for node in NODES:
            response = cluster.request(node, "GET", "/experimental/v1/regional/catalog")
            if response.status != 200:
                return None
            if (
                exact_int(response.document.get("resource_count")) != expected_resources
                or exact_int(response.document.get("tablet_count")) != expected_tablets
            ):
                return None
            digest = response.document.get("state_digest")
            if not isinstance(digest, str):
                return None
            digests.append(digest)
        return digests[0] if len(set(digests)) == 1 else None

    return wait_until(
        f"catalog convergence for {expected_resources} resources and {expected_tablets} tablets",
        converged,
    )


def python_stream_client(cluster: RegionalCluster) -> Any:
    sdk_source = str(REPO_ROOT / "sdk/python/src")
    if sdk_source not in sys.path:
        sys.path.insert(0, sdk_source)
    epoch_sdk = importlib.import_module("epoch_sdk")
    return epoch_sdk.RegionalStreamClient(
        [f"http://127.0.0.1:{cluster.http_ports[node]}" for node in NODES],
        token=ADMIN_TOKEN,
        scope=epoch_sdk.RegionalScope("acme", "shop", "dev", "core"),
        timeout=2.0,
    )


def assert_python_sdk_consumer_session(
    cluster: RegionalCluster,
    resource: Resource,
) -> dict[str, Any]:
    observed = python_stream_client(cluster).consumer_session(resource.name, "billing")
    assert observed.get("shard_index") == 0, observed
    session = observed.get("session")
    assert isinstance(session, dict), observed
    assert session.get("exists") is True, observed
    assert session.get("group") == "billing", observed
    assert session.get("shard_count") == 3, observed
    assert session.get("group_generation") == "3", observed
    members = session.get("members")
    assert isinstance(members, list) and len(members) == 1, observed
    assert members[0].get("member_id") == "python-worker-b", observed
    assert members[0].get("assigned_shards") == [0, 1, 2], observed
    return observed


def assert_python_sdk_stream_batch(
    cluster: RegionalCluster,
    resource: Resource,
) -> None:
    fetched = python_stream_client(cluster).fetch(resource.name, 1, 0, limit=10)
    records = fetched.get("records")
    assert fetched.get("shard_index") == 1, fetched
    assert isinstance(records, list), fetched
    assert [record.get("envelope", {}).get("id") for record in records] == [
        "managed-orders-shard-1",
        "managed-orders-batch-1",
        "managed-orders-batch-2",
    ], fetched
    assert all(record.get("partition") == 1 for record in records), fetched


def assert_python_sdk_fenced_consumption(
    cluster: RegionalCluster,
    resource: Resource,
) -> None:
    client = python_stream_client(cluster)
    for shard in range(MANAGED_STREAM_SHARDS):
        lag = client.lag(resource.name, shard, "billing")
        checkpoint = lag.get("checkpoint")
        assert isinstance(checkpoint, dict), lag
        assert checkpoint.get("member_id") == "python-worker-b", lag
        assert checkpoint.get("group_generation") == "3", lag
        assert checkpoint.get("session_fenced") is True, lag
        fetched = client.fetch_claimed_group(
            resource.name,
            shard,
            "billing",
            "python-worker-b",
            3,
            limit=10,
        )
        records = fetched.get("records")
        assert isinstance(records, list), fetched
        expected = (
            ["managed-orders-batch-1", "managed-orders-batch-2"]
            if shard == 1
            else []
        )
        assert [record.get("envelope", {}).get("id") for record in records] == expected, (
            fetched
        )


def prove_python_sdk_native_stream_after_failover(
    cluster: RegionalCluster,
    resource: Resource,
) -> None:
    sdk_source = str(REPO_ROOT / "sdk/python/src")
    if sdk_source not in sys.path:
        sys.path.insert(0, sdk_source)
    epoch_sdk = importlib.import_module("epoch_sdk")
    client = python_stream_client(cluster)
    event = epoch_sdk.EventEnvelope(
        id="managed-orders-2",
        key="customer-2",
        source="python-regional-sdk",
        event_type="order.created",
        payload={"id": 2},
        time_ms=2,
    )
    committed = client.append_keyed(resource.name, "python-sdk-append-2", event)
    assert committed.get("state") == "committed", committed
    assert committed.get("shard_index") == 0, committed

    replayed = client.append_keyed(resource.name, "python-sdk-append-2", event)
    assert replayed.get("state") == "committed", replayed
    receipt = replayed.get("receipt")
    assert isinstance(receipt, dict) and receipt.get("disposition") == "replayed", (
        replayed
    )
    assert receipt.get("partition") == 0, replayed

    fetched = client.fetch(resource.name, 0, 0, limit=10)
    records = fetched.get("records")
    assert isinstance(records, list) and [
        record.get("envelope", {}).get("id") for record in records
    ] == ["order-1", "managed-orders-2"], fetched

    checkpoint = client.commit_offset(
        resource.name,
        0,
        "billing",
        "python-worker",
        1,
        2,
        idempotency_key="python-sdk-checkpoint-2",
    )
    assert checkpoint.get("state") == "committed", checkpoint
    lag = client.lag(resource.name, 0, "billing")
    observation = lag.get("checkpoint")
    assert isinstance(observation, dict), lag
    assert observation.get("committed_offset") == "2", lag
    assert observation.get("lag") == "0", lag

    retention_submissions = current_maintenance_submissions(cluster)
    configured = client.configure_retention(
        resource.name,
        0,
        "python-sdk-retention-1",
        epoch_sdk.StreamRetentionPolicy(
            max_records_per_partition=1,
            max_bytes_per_partition=1_048_576,
            max_age_ms=2_000,
        ),
    )
    assert configured.get("state") == "committed", configured
    retention_receipt = configured.get("receipt")
    assert isinstance(retention_receipt, dict), configured
    assert retention_receipt.get("base_offset") == "1", configured
    assert retention_receipt.get("retained_records") == 1, configured

    def retention_expired() -> dict[str, Any] | None:
        retention = client.retention(resource.name, 0)
        observation = retention.get("retention")
        if not isinstance(observation, dict):
            return None
        if (
            observation.get("base_offset") != "2"
            or observation.get("end_offset") != "2"
            or observation.get("retained_records") != 0
        ):
            return None
        return retention

    wait_until("automatic Stream age retention", retention_expired)
    wait_for_maintenance_submission(cluster, retention_submissions)

    for shard, key in ((1, "customer-0"), (2, "customer-1")):
        keyed_event = epoch_sdk.EventEnvelope(
            id=f"managed-orders-shard-{shard}",
            key=key,
            source="python-regional-sdk",
            event_type="order.created",
            payload={"shard": shard},
            time_ms=10 + shard,
        )
        assert epoch_sdk.stream_shard_for(key, MANAGED_STREAM_SHARDS) == shard
        keyed = client.append_keyed(
            resource.name, f"python-sdk-keyed-shard-{shard}", keyed_event
        )
        keyed_receipt = keyed.get("receipt")
        assert keyed.get("shard_index") == shard, keyed
        assert isinstance(keyed_receipt, dict), keyed
        assert keyed_receipt.get("partition") == shard, keyed
        fetched = client.fetch(resource.name, shard, 0, limit=10)
        records = fetched.get("records")
        assert fetched.get("shard_index") == shard, fetched
        assert isinstance(records, list) and [
            record.get("envelope", {}).get("id") for record in records
        ] == [keyed_event.id], fetched
        assert all(record.get("partition") == shard for record in records), fetched
        checkpoint = client.commit_offset(
            resource.name,
            shard,
            "billing",
            f"python-worker-{shard}",
            1,
            1,
            idempotency_key=f"python-sdk-checkpoint-shard-{shard}",
        )
        checkpoint_receipt = checkpoint.get("receipt")
        assert isinstance(checkpoint_receipt, dict), checkpoint
        assert checkpoint_receipt.get("partition") == shard, checkpoint
        lag = client.lag(resource.name, shard, "billing")
        observation = lag.get("checkpoint")
        assert lag.get("shard_index") == shard, lag
        assert isinstance(observation, dict), lag
        assert observation.get("partition") == shard, lag
        assert observation.get("lag") == "0", lag

        if shard == 1:
            batch = epoch_sdk.StreamBatchFrame.encode(
                [
                    epoch_sdk.StreamBatchRecord(
                        101,
                        epoch_sdk.EventEnvelope(
                            id="managed-orders-batch-1",
                            key="customer-0",
                            source="python-regional-sdk",
                            event_type="order.created",
                            payload={"batch": 1},
                            time_ms=21,
                        ),
                    ),
                    epoch_sdk.StreamBatchRecord(
                        102,
                        epoch_sdk.EventEnvelope(
                            id="managed-orders-batch-2",
                            key="customer-0",
                            source="python-regional-sdk",
                            event_type="order.created",
                            payload={"batch": 2},
                            time_ms=22,
                        ),
                    ),
                ],
                "gzip",
            )
            committed_batch = client.append_batch(
                resource.name,
                shard,
                "python-sdk-gzip-batch-1",
                batch,
            )
            assert committed_batch.get("state") == "committed", committed_batch
            batch_receipt = committed_batch.get("receipt", {}).get("batch")
            assert isinstance(batch_receipt, dict), committed_batch
            assert batch_receipt.get("compression") == "gzip", committed_batch
            assert batch_receipt.get("record_count") == 2, committed_batch
            correlated = batch_receipt.get("records")
            assert isinstance(correlated, list), committed_batch
            assert [record.get("client_sequence") for record in correlated] == [
                101,
                102,
            ], committed_batch
            assert all(record.get("partition") == shard for record in correlated), (
                committed_batch
            )
            replayed_batch = client.append_batch(
                resource.name,
                shard,
                "python-sdk-gzip-batch-1",
                batch,
            )
            assert replayed_batch.get("receipt", {}).get("disposition") == "replayed", (
                replayed_batch
            )
            assert replayed_batch.get("receipt", {}).get("batch") == batch_receipt, (
                replayed_batch
            )
            assert_python_sdk_stream_batch(cluster, resource)

    joined_a = client.join_consumer_session(
        resource.name,
        "billing",
        "python-worker-a",
        2_000,
        idempotency_key="python-sdk-session-join-a",
    )
    receipt_a = joined_a.get("receipt")
    assert isinstance(receipt_a, dict), joined_a
    assert receipt_a.get("outcome") == "applied", joined_a
    assert receipt_a.get("group_generation") == "1", joined_a
    assert receipt_a.get("assigned_shards") == [0, 1, 2], joined_a

    joined_b = client.join_consumer_session(
        resource.name,
        "billing",
        "python-worker-b",
        300_000,
        idempotency_key="python-sdk-session-join-b",
    )
    receipt_b = joined_b.get("receipt")
    assert isinstance(receipt_b, dict), joined_b
    assert receipt_b.get("outcome") == "applied", joined_b
    assert receipt_b.get("group_generation") == "2", joined_b
    assert receipt_b.get("assigned_shards") == [1], joined_b
    assert [
        member.get("assigned_shards") for member in receipt_b.get("members", [])
    ] == [
        [0, 2],
        [1],
    ], joined_b

    heartbeat = client.heartbeat_consumer_session(
        resource.name,
        "billing",
        "python-worker-b",
        2,
        idempotency_key="python-sdk-session-heartbeat-b",
    )
    heartbeat_receipt = heartbeat.get("receipt")
    assert isinstance(heartbeat_receipt, dict), heartbeat
    assert heartbeat_receipt.get("outcome") == "applied", heartbeat
    assert heartbeat_receipt.get("group_generation") == "2", heartbeat

    session_submissions = current_maintenance_submissions(cluster)
    wait_until(
        "automatic Stream consumer-session expiry",
        lambda: assert_python_sdk_consumer_session(cluster, resource),
    )
    wait_for_maintenance_submission(cluster, session_submissions)

    claimed = client.claim_consumer_session(
        resource.name,
        "billing",
        "python-worker-b",
        3,
        idempotency_key_prefix="python-sdk-session-claim-b-3",
    )
    assert claimed == (0, 1, 2), claimed
    assert_python_sdk_fenced_consumption(cluster, resource)
    try:
        client.fetch_claimed_group(
            resource.name,
            1,
            "billing",
            "python-worker-a",
            2,
            limit=1,
        )
    except epoch_sdk.EpochAPIError as error:
        assert error.status == 409 and error.code == "fenced", error
    else:
        raise AssertionError("stale consumer member unexpectedly fetched claimed records")


def queue_result(document: dict[str, Any], expected_kind: str) -> dict[str, Any]:
    receipt = document.get("receipt")
    assert isinstance(receipt, dict), document
    outcome = receipt.get("outcome")
    assert isinstance(outcome, dict) and outcome.get("status") == "applied", document
    result = outcome.get("result")
    assert isinstance(result, dict) and result.get("kind") == expected_kind, document
    return result


def cache_result(document: dict[str, Any], expected_kind: str) -> dict[str, Any]:
    receipt = document.get("receipt")
    assert isinstance(receipt, dict), document
    outcome = receipt.get("outcome")
    assert isinstance(outcome, dict) and outcome.get("status") == "applied", document
    result = outcome.get("result")
    assert isinstance(result, dict) and result.get("kind") == expected_kind, document
    return result


def bus_result(document: dict[str, Any], expected_kind: str) -> dict[str, Any]:
    receipt = document.get("receipt")
    assert isinstance(receipt, dict), document
    outcome = receipt.get("outcome")
    assert isinstance(outcome, dict) and outcome.get("status") == "applied", document
    result = outcome.get("result")
    assert isinstance(result, dict) and result.get("kind") == expected_kind, document
    return result


def prove_python_sdk_native_bus_after_failover(
    cluster: RegionalCluster,
    resource: Resource,
) -> None:
    sdk_source = str(REPO_ROOT / "sdk/python/src")
    if sdk_source not in sys.path:
        sys.path.insert(0, sdk_source)
    epoch_sdk = importlib.import_module("epoch_sdk")
    client = epoch_sdk.RegionalBusClient(
        [f"http://127.0.0.1:{cluster.http_ports[node]}" for node in NODES],
        token=ADMIN_TOKEN,
        scope=epoch_sdk.RegionalScope("acme", "shop", "dev", "core"),
        timeout=2.0,
    )
    subscription = epoch_sdk.Subscription(
        "orders",
        epoch_sdk.SubscriptionTarget.pull(),
        filter=epoch_sdk.EventFilter(event_type_patterns=["order.*"]),
        delivery_policy=epoch_sdk.DeliveryPolicy(
            timeout_ms=1_000,
            max_in_flight=2,
            retry=epoch_sdk.DeliveryRetryPolicy(
                strategy="fixed",
                initial_delay_ms=0,
                max_delay_ms=0,
                jitter_percent=0,
                max_attempts=2,
            ),
        ),
    )
    bus_result(
        client.upsert_subscription(
            resource.name, 0, "python-bus-upsert-orders", subscription
        ),
        "subscription_upserted",
    )
    event = epoch_sdk.EventEnvelope(
        id="events-2",
        source="python-regional-sdk",
        event_type="order.created",
        payload={"id": 2},
        time_ms=2,
    )
    published = client.publish(resource.name, 0, "python-bus-publish-order-2", event)
    assert bus_result(published, "published").get("delivery_count") == 1, published
    replayed = client.publish(resource.name, 0, "python-bus-publish-order-2", event)
    assert replayed.get("receipt", {}).get("disposition") == "replayed", replayed

    archive = client.replay_archive(
        resource.name,
        0,
        from_ms=0,
        to_ms=(1 << 64) - 1,
        limit=10,
        filter=epoch_sdk.EventFilter(event_type_patterns=["order.*"]),
    )
    records = archive.get("records")
    assert isinstance(records, list) and [
        record.get("envelope", {}).get("id") for record in records
    ] == ["events-1", "events-2"], archive

    acquired = bus_result(
        client.acquire_deliveries(
            resource.name,
            0,
            "python-bus-acquire-order-2",
            subscription="orders",
            dispatcher="python-dispatcher",
            dispatcher_epoch=1,
            max_deliveries=1,
        ),
        "deliveries_acquired",
    )
    deliveries = acquired.get("deliveries")
    assert isinstance(deliveries, list) and len(deliveries) == 1, acquired
    first = deliveries[0]
    delivery_id = first.get("delivery_id")
    assert isinstance(delivery_id, str), first
    delivery_submissions = current_maintenance_submissions(cluster)

    def lease_expired() -> list[dict[str, Any]] | None:
        records = client.query_deliveries(
            resource.name,
            0,
            subscription="orders",
            state="pending",
            limit=10,
        ).get("records")
        if not isinstance(records, list) or len(records) != 1:
            return None
        if records[0].get("delivery_id") != delivery_id:
            return None
        return records

    wait_until("automatic Event Bus delivery-lease expiry", lease_expired)
    wait_for_maintenance_submission(cluster, delivery_submissions)
    reacquired = bus_result(
        client.acquire_deliveries(
            resource.name,
            0,
            "python-bus-reacquire-order-2",
            subscription="orders",
            dispatcher="python-dispatcher",
            dispatcher_epoch=1,
            max_deliveries=1,
        ),
        "deliveries_acquired",
    )["deliveries"][0]
    assert reacquired.get("delivery_id") == delivery_id, reacquired
    assert reacquired.get("attempt") == 2, reacquired
    bus_result(
        client.acknowledge_delivery(
            resource.name,
            0,
            "python-bus-ack-order-2",
            delivery_id,
            "python-dispatcher",
            1,
            reacquired["lease_token"],
        ),
        "delivery_acknowledged",
    )
    deliveries = client.query_deliveries(
        resource.name,
        0,
        subscription="orders",
        state="acknowledged",
        limit=10,
    ).get("records")
    assert isinstance(deliveries, list) and len(deliveries) == 1, deliveries
    assert deliveries[0].get("delivery_id") == delivery_id, deliveries
    assert len(deliveries[0].get("attempts", [])) == 2, deliveries

    bus_result(
        client.remove_subscription(
            resource.name, 0, "python-bus-remove-orders", "orders"
        ),
        "subscription_removed",
    )
    status = client.status(resource.name, 0)
    assert status.get("read_consistency") == "linearizable", status
    assert status.get("subscription_count") == "0", status
    assert status.get("acknowledged_delivery_count") == "1", status
    assert status.get("pending_delivery_count") == "0", status
    assert status.get("in_flight_delivery_count") == "0", status


def prove_python_sdk_native_cache_after_failover(
    cluster: RegionalCluster,
    resource: Resource,
) -> None:
    sdk_source = str(REPO_ROOT / "sdk/python/src")
    if sdk_source not in sys.path:
        sys.path.insert(0, sdk_source)
    epoch_sdk = importlib.import_module("epoch_sdk")
    client = epoch_sdk.RegionalCacheClient(
        [f"http://127.0.0.1:{cluster.http_ports[node]}" for node in NODES],
        token=ADMIN_TOKEN,
        scope=epoch_sdk.RegionalScope("acme", "shop", "dev", "core"),
        timeout=2.0,
    )

    initial = client.observe(resource.name, 0, "session-1")
    assert initial.get("read_consistency") == "linearizable", initial
    item = initial.get("observation", {}).get("item")
    assert isinstance(item, dict), initial
    assert item.get("value") == {"kind": "string", "value": "ready"}, initial
    assert item.get("version") == "1", initial

    set_profile = client.set(
        resource.name,
        0,
        "python-cache-set-profile-2",
        "profile",
        epoch_sdk.RegionalCacheValue.string("v2"),
    )
    cache_result(set_profile, "set")
    replayed = client.set(
        resource.name,
        0,
        "python-cache-set-profile-2",
        "profile",
        epoch_sdk.RegionalCacheValue.string("v2"),
    )
    assert replayed.get("receipt", {}).get("disposition") == "replayed", replayed

    cache_result(
        client.compare_and_set(
            resource.name,
            0,
            "python-cache-cas-session-1",
            "session-1",
            epoch_sdk.RegionalCacheExpectation.version(1),
            epoch_sdk.RegionalCacheValue.blob(b"ready-v2"),
        ),
        "compared_and_set",
    )
    incremented = cache_result(
        client.increment(
            resource.name,
            0,
            "python-cache-increment-visits",
            "visits",
            2,
            expected_version=0,
        ),
        "incremented",
    )
    assert incremented.get("value") == "2", incremented

    committed = cache_result(
        client.transaction(
            resource.name,
            0,
            "python-cache-values-transaction",
            4,
            [
                epoch_sdk.RegionalCacheMutation.set(
                    "typed-string", epoch_sdk.RegionalCacheValue.string("value")
                ),
                epoch_sdk.RegionalCacheMutation.set(
                    "typed-blob", epoch_sdk.RegionalCacheValue.blob(b"\x00\xff")
                ),
                epoch_sdk.RegionalCacheMutation.set(
                    "typed-counter", epoch_sdk.RegionalCacheValue.counter(-7)
                ),
                epoch_sdk.RegionalCacheMutation.set(
                    "typed-hash", epoch_sdk.RegionalCacheValue.hash({"role": "admin"})
                ),
                epoch_sdk.RegionalCacheMutation.set(
                    "typed-list", epoch_sdk.RegionalCacheValue.list(["a", "b"])
                ),
                epoch_sdk.RegionalCacheMutation.set(
                    "typed-set", epoch_sdk.RegionalCacheValue.set(["a", "b"])
                ),
                epoch_sdk.RegionalCacheMutation.set(
                    "typed-ranked",
                    epoch_sdk.RegionalCacheValue.sorted_set({"alice": 1.5}),
                ),
            ],
        ),
        "transaction_committed",
    )
    assert committed.get("revision") == "5", committed
    assert len(committed.get("results", [])) == 7, committed

    acquired = cache_result(
        client.acquire_lock(
            resource.name,
            0,
            "python-cache-acquire-critical",
            "critical",
            "python-worker",
            1,
            60_000,
        ),
        "lock_acquired",
    )
    lease_token = acquired.get("lease_token")
    assert isinstance(lease_token, str) and lease_token, acquired
    guard = epoch_sdk.RegionalCacheLockGuard(
        "critical", "python-worker", 1, lease_token
    )
    guarded_set = cache_result(
        client.set(
            resource.name,
            0,
            "python-cache-guarded-set",
            "protected",
            epoch_sdk.RegionalCacheValue.string("owned"),
            lock_guard=guard,
        ),
        "set",
    )
    protected_version = int(guarded_set["item"]["version"])

    renewed = cache_result(
        client.renew_lock(
            resource.name,
            0,
            "python-cache-renew-critical",
            "critical",
            "python-worker",
            1,
            lease_token,
            60_000,
        ),
        "lock_renewed",
    )
    renewed_token = renewed.get("lease_token")
    assert isinstance(renewed_token, str) and renewed_token != lease_token, renewed
    assert renewed.get("fencing_token") == acquired.get("fencing_token"), renewed
    renewed_guard = epoch_sdk.RegionalCacheLockGuard(
        "critical", "python-worker", 1, renewed_token
    )
    cache_result(
        client.delete(
            resource.name,
            0,
            "python-cache-guarded-delete",
            "protected",
            expected_version=protected_version,
            lock_guard=renewed_guard,
        ),
        "deleted",
    )
    cache_result(
        client.release_lock(
            resource.name,
            0,
            "python-cache-release-critical",
            "critical",
            "python-worker",
            1,
            renewed_token,
        ),
        "lock_released",
    )

    expiry_submissions = current_maintenance_submissions(cluster)
    cache_result(
        client.set(
            resource.name,
            0,
            "python-cache-set-ephemeral",
            "ephemeral",
            epoch_sdk.RegionalCacheValue.string("short"),
            ttl_ms=250,
        ),
        "set",
    )
    final = wait_until(
        "automatic Cache value expiry",
        lambda: (
            observed
            if (observed := client.observe(resource.name, 0, "ephemeral"))
            .get("observation", {})
            .get("item")
            is None
            else None
        ),
    )
    wait_for_maintenance_submission(cluster, expiry_submissions)
    assert final.get("read_consistency") == "linearizable", final
    assert final.get("observation", {}).get("item") is None, final


def prove_python_sdk_native_queue_after_failover(
    cluster: RegionalCluster,
    resource: Resource,
) -> None:
    sdk_source = str(REPO_ROOT / "sdk/python/src")
    if sdk_source not in sys.path:
        sys.path.insert(0, sdk_source)
    epoch_sdk = importlib.import_module("epoch_sdk")
    client = epoch_sdk.RegionalQueueClient(
        [f"http://127.0.0.1:{cluster.http_ports[node]}" for node in NODES],
        token=ADMIN_TOKEN,
        scope=epoch_sdk.RegionalScope("acme", "shop", "dev", "core"),
        timeout=2.0,
    )
    event = epoch_sdk.EventEnvelope(
        id="jobs-2",
        source="python-regional-sdk",
        event_type="job.created",
        payload={"id": 2},
        time_ms=2,
    )
    committed = client.enqueue(resource.name, 0, "python-sdk-enqueue-2", event)
    queue_result(committed, "enqueued")
    replayed = client.enqueue(resource.name, 0, "python-sdk-enqueue-2", event)
    assert replayed.get("receipt", {}).get("disposition") == "replayed", replayed

    acquired = client.acquire(
        resource.name,
        0,
        "python-sdk-acquire-2",
        consumer="python-worker",
        consumer_epoch=1,
        max_messages=2,
        max_in_flight=2,
        visibility_timeout_ms=5_000,
    )
    deliveries = queue_result(acquired, "acquired").get("deliveries")
    assert isinstance(deliveries, list) and len(deliveries) == 2, acquired
    by_message = {
        delivery.get("message_id"): delivery
        for delivery in deliveries
        if isinstance(delivery, dict)
    }
    assert set(by_message) == {"jobs-1", "jobs-2"}, acquired

    renewed = client.extend_lease(
        resource.name,
        0,
        "python-sdk-extend-2",
        "python-worker",
        1,
        by_message["jobs-1"]["lease_token"],
        60_000,
    )
    renewed_token = queue_result(renewed, "lease_extended").get("lease_token")
    assert isinstance(renewed_token, str), renewed
    queue_result(
        client.acknowledge(
            resource.name,
            0,
            "python-sdk-ack-1",
            "python-worker",
            1,
            renewed_token,
        ),
        "acknowledged",
    )
    timer_submissions = current_maintenance_submissions(cluster)
    queue_result(
        client.release(
            resource.name,
            0,
            "python-sdk-release-2",
            "python-worker",
            1,
            by_message["jobs-2"]["lease_token"],
            500,
            "retry once",
        ),
        "released",
    )

    def retry_ready() -> dict[str, Any] | None:
        counts = client.counts(resource.name, 0).get("counts")
        if not isinstance(counts, dict):
            return None
        if counts.get("scheduled") != "0" or counts.get("ready") != "1":
            return None
        return counts

    wait_until("automatic Queue retry scheduling", retry_ready)
    wait_for_maintenance_submission(cluster, timer_submissions)

    reacquired = client.acquire(
        resource.name,
        0,
        "python-sdk-reacquire-2",
        consumer="python-worker",
        consumer_epoch=1,
        max_messages=1,
        max_in_flight=2,
    )
    redelivery = queue_result(reacquired, "acquired")["deliveries"][0]
    rejected = client.reject(
        resource.name,
        0,
        "python-sdk-reject-2",
        "python-worker",
        1,
        redelivery["lease_token"],
        "poison",
    )
    history_id = queue_result(rejected, "dead_lettered").get("dead_letter_history_id")
    assert isinstance(history_id, str) and history_id.isdecimal(), rejected
    dead_letters = client.dead_letters(resource.name, 0, limit=10)
    records = dead_letters.get("records")
    assert isinstance(records, list) and records[-1].get("history_id") == history_id, (
        dead_letters
    )

    queue_result(
        client.redrive(
            resource.name,
            0,
            "python-sdk-redrive-2",
            "jobs-2",
            int(history_id),
        ),
        "redriven",
    )
    final_acquire = client.acquire(
        resource.name,
        0,
        "python-sdk-final-acquire-2",
        consumer="python-worker",
        consumer_epoch=1,
        max_messages=1,
        max_in_flight=2,
    )
    final_delivery = queue_result(final_acquire, "acquired")["deliveries"][0]
    queue_result(
        client.acknowledge(
            resource.name,
            0,
            "python-sdk-final-ack-2",
            "python-worker",
            1,
            final_delivery["lease_token"],
        ),
        "acknowledged",
    )
    counts = client.counts(resource.name, 0).get("counts")
    assert isinstance(counts, dict), counts
    assert counts.get("ready") == "0" and counts.get("in_flight") == "0", counts
    assert counts.get("acknowledged") == "2" and counts.get("dead_lettered") == "0", (
        counts
    )
    flow = client.consumer_flow(resource.name, 0, "python-worker").get("flow")
    assert isinstance(flow, dict) and flow.get("in_flight") == "0", flow
    redrives = client.redrives(resource.name, 0, limit=10).get("records")
    assert isinstance(redrives, list) and len(redrives) == 1, redrives


def run_campaign(cluster: RegionalCluster) -> None:
    cluster.start()
    wait_for_nodes(cluster)
    wait_for_topology(cluster, 1)
    cluster.start_control()
    create_managed_resource(cluster, MANAGED_RESOURCE)
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "ready", 3)
    wait_for_topology(cluster, 1 + MANAGED_STREAM_SHARDS)
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
    expected_tablets = expected_resources + MANAGED_STREAM_SHARDS - 1
    wait_for_topology(cluster, 1 + expected_tablets)
    initial_catalog_digest = wait_for_catalog(
        cluster, expected_resources, expected_tablets
    )
    wait_for_automatic_checkpoints(cluster, 1 + expected_tablets, True)

    stream = MANAGED_RESOURCE
    old_leader, old_term = wait_for_routes(cluster, stream)
    cluster.stop_node(old_leader)
    survivors = tuple(node for node in NODES if node != old_leader)
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "degraded", 2)
    new_leader, new_term = wait_for_routes(cluster, stream, survivors)
    assert new_leader != old_leader
    assert new_term > old_term
    prove_python_sdk_native_stream_after_failover(cluster, stream)
    wait_for_profile_apply(cluster, stream, 11, survivors)
    wait_for_profile_apply(cluster, stream, 5, survivors, shard=1)
    wait_for_profile_apply(cluster, stream, 4, survivors, shard=2)

    cluster.start_node(old_leader)
    wait_for_nodes(cluster)
    wait_for_profile_apply(cluster, stream, 11)
    wait_for_profile_apply(cluster, stream, 5, shard=1)
    wait_for_profile_apply(cluster, stream, 4, shard=2)
    assert_python_sdk_consumer_session(cluster, stream)
    assert_python_sdk_stream_batch(cluster, stream)
    assert_python_sdk_fenced_consumption(cluster, stream)
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "ready", 3)
    for resource in RESOURCES:
        wait_for_profile_apply(cluster, resource, 1)

    queue = next(resource for resource in RESOURCES if resource.kind == "queue")
    queue_old_leader, queue_old_term = wait_for_routes(cluster, queue)
    cluster.stop_node(queue_old_leader)
    queue_survivors = tuple(node for node in NODES if node != queue_old_leader)
    queue_new_leader, queue_new_term = wait_for_routes(cluster, queue, queue_survivors)
    assert queue_new_leader != queue_old_leader
    assert queue_new_term > queue_old_term
    prove_python_sdk_native_queue_after_failover(cluster, queue)
    wait_for_profile_apply(cluster, queue, 12, queue_survivors)
    cluster.start_node(queue_old_leader)
    wait_for_nodes(cluster)
    wait_for_profile_apply(cluster, queue, 12)

    cache = next(resource for resource in RESOURCES if resource.kind == "cache")
    cache_old_leader, cache_old_term = wait_for_routes(cluster, cache)
    cluster.stop_node(cache_old_leader)
    cache_survivors = tuple(node for node in NODES if node != cache_old_leader)
    cache_new_leader, cache_new_term = wait_for_routes(cluster, cache, cache_survivors)
    assert cache_new_leader != cache_old_leader
    assert cache_new_term > cache_old_term
    prove_python_sdk_native_cache_after_failover(cluster, cache)
    wait_for_profile_apply(cluster, cache, 12, cache_survivors)
    cluster.start_node(cache_old_leader)
    wait_for_nodes(cluster)
    wait_for_profile_apply(cluster, cache, 12)

    bus = next(resource for resource in RESOURCES if resource.kind == "event-bus")
    bus_old_leader, bus_old_term = wait_for_routes(cluster, bus)
    cluster.stop_node(bus_old_leader)
    bus_survivors = tuple(node for node in NODES if node != bus_old_leader)
    bus_new_leader, bus_new_term = wait_for_routes(cluster, bus, bus_survivors)
    assert bus_new_leader != bus_old_leader
    assert bus_new_term > bus_old_term
    prove_python_sdk_native_bus_after_failover(cluster, bus)
    wait_for_profile_apply(cluster, bus, 8, bus_survivors)
    cluster.start_node(bus_old_leader)
    wait_for_nodes(cluster)
    wait_for_profile_apply(cluster, bus, 8)

    wait_for_automatic_checkpoints(cluster, 1 + expected_tablets, False)

    cluster.crash_all()
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "pending", 0)
    cluster.restart_all()
    wait_for_nodes(cluster)
    assert (
        wait_for_catalog(cluster, expected_resources, expected_tablets)
        == initial_catalog_digest
    )
    wait_for_managed_placement(cluster, MANAGED_RESOURCE, "ready", 3)
    wait_for_profile_apply(cluster, stream, 11)
    wait_for_profile_apply(cluster, stream, 5, shard=1)
    wait_for_profile_apply(cluster, stream, 4, shard=2)
    assert_python_sdk_consumer_session(cluster, stream)
    assert_python_sdk_stream_batch(cluster, stream)
    assert_python_sdk_fenced_consumption(cluster, stream)
    for resource in RESOURCES:
        expected = (
            12
            if resource == queue
            else 12
            if resource == cache
            else 8
            if resource == bus
            else 1
        )
        wait_for_profile_apply(cluster, resource, expected)
    wait_for_automatic_checkpoints(cluster, 1 + expected_tablets, False)


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
            "Stream-batch-session-Queue-Cache-and-Bus-SDK/control-SIGKILL/"
            "all-node recovery container campaign passed."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
