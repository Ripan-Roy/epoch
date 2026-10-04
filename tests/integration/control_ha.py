#!/usr/bin/env python3
"""Real three/five-controller owner recovery; not full control-HA certification."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any


def load_fixture(name: str, path: Path) -> Any:
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load fixture {path}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


regional = load_fixture(
    "control_ha_regional", Path(__file__).with_name("regional-runtime.py")
)
soak = load_fixture("control_ha_soak", Path(__file__).parents[1] / "soak/epoch_soak.py")
OWNER_SCHEMA = "epoch.control-ha.owner-recovery/v1"
PROFILES = ("cache", "stream", "queue", "event-bus")
OWNER_CHECKS = (
    "concurrent_http_writes_visible_everywhere",
    "exact_retry_and_conflicting_token",
    "canonical_native_materialization",
    "same_label_replacement_has_fresh_owner",
    "actual_owner_sigkill_takeover",
    "paused_owner_takeover_and_four_guard_rejections",
    "catalog_quorum_loss_fails_closed",
    "unknown_quorum_write_resolves_by_original_token",
    "managed_state_and_outcomes_survive_reopen",
    "four_profile_digests_survive_reopen",
)
CONTROL_ROOT = "/experimental/v1/regional/control"


def validate_owner_evidence(evidence: dict[str, Any]) -> None:
    if evidence.get("schema") != OWNER_SCHEMA or evidence.get("status") != "passed":
        raise ValueError("wrong owner-recovery evidence schema or status")
    fleets = evidence.get("fleets")
    if (
        not isinstance(fleets, list)
        or len(fleets) != 2
        or any(not isinstance(fleet, dict) for fleet in fleets)
        or [fleet.get("controller_count") for fleet in fleets] != [3, 5]
    ):
        raise ValueError(
            "owner recovery requires ordered three/five-controller campaigns"
        )
    for fleet in fleets:
        count = fleet["controller_count"]
        if type(count) is not int:
            raise ValueError("controller count must be an integer")
        owners = fleet.get("owners")
        if (
            not isinstance(owners, list)
            or len(owners) != count
            or any(
                not isinstance(owner, str) or not 1 <= len(owner) <= 128
                for owner in owners
            )
            or len(set(owners)) != count
        ):
            raise ValueError("every controller requires a distinct process owner")
        checks = fleet.get("checks")
        if (
            not isinstance(checks, dict)
            or set(checks) != set(OWNER_CHECKS)
            or any(checks[check] is not True for check in OWNER_CHECKS)
        ):
            raise ValueError("missing, malformed, or false owner-recovery invariant")
        before = fleet.get("profile_digests_before_reopen")
        after = fleet.get("profile_digests_after_reopen")
        if (
            not isinstance(before, dict)
            or set(before) != set(PROFILES)
            or after != before
            or any(
                not isinstance(digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                for digest in before.values()
            )
        ):
            raise ValueError("all four exact profile digests must survive reopen")
        transitions = fleet.get("lease_transitions")
        if (
            not isinstance(transitions, list)
            or len(transitions) != 2
            or any(not isinstance(transition, dict) for transition in transitions)
            or [transition.get("fault") for transition in transitions]
            != ["owner_sigkill", "owner_sigstop"]
        ):
            raise ValueError("missing owner fault history")
        for transition in transitions:
            old, new = transition.get("old_owner"), transition.get("new_owner")
            old_fence, new_fence = (
                transition.get("old_fence"),
                transition.get("new_fence"),
            )
            if (
                not isinstance(old, str)
                or not old
                or not isinstance(new, str)
                or not new
                or old == new
                or type(old_fence) is not int
                or type(new_fence) is not int
                or not 0 < old_fence < new_fence
            ):
                raise ValueError(
                    "owner takeover must advance the fence and process identity"
                )


def stop_process(process: subprocess.Popen[Any], *, paused: bool = False) -> None:
    """Only signal an explicitly owned handle; always release SIGSTOP first."""
    if process.poll() is not None:
        return
    if paused:
        process.send_signal(signal.SIGCONT)
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def resource_name(resource: Any) -> dict[str, str]:
    return {
        "organization": "acme",
        "project": "shop",
        "environment": "dev",
        "namespace": "core",
        "kind": resource.kind,
        "name": resource.name,
    }


def resource_path(resource: Any) -> str:
    return f"/v1/resources/acme/shop/dev/core/{control_kind(resource)}/{resource.name}"


def control_kind(resource: Any) -> str:
    # Go's existing management spelling differs from the native Rust route.
    return "event_bus" if resource.kind == "event-bus" else resource.kind


def managed_request(resource: Any) -> dict[str, Any]:
    body = regional.managed_resource_request(resource)
    body["resource"]["kind"] = control_kind(resource)
    return body


class Controller:
    def __init__(self, fleet: OwnerFleet, label: str, suffix: str) -> None:
        self.fleet = fleet
        self.label = label
        self.http_port, self.grpc_port, self.metrics_port = regional.allocate_ports(3)
        self.directory = Path(fleet.cluster.temporary_directory.name) / suffix
        self.directory.mkdir()
        self.log_path = self.directory / "control.log"
        self.log = self.log_path.open("a", encoding="utf-8")
        self.process: subprocess.Popen[Any] | None = None
        self.paused = False
        self.owner_id = ""

    def request(self, method: str, path: str, body: dict | None = None) -> Any:
        headers = {"authorization": f"Bearer {regional.ADMIN_TOKEN}"}
        payload = None
        if body is not None:
            headers["content-type"] = "application/json"
            payload = json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.http_port}{path}",
            data=payload,
            headers=headers,
            method=method,
        )
        try:
            response = urllib.request.urlopen(request, timeout=12)
        except urllib.error.HTTPError as error:
            return regional.HttpResponse(
                error.code, json.loads(error.read()), dict(error.headers)
            )
        with response:
            return regional.HttpResponse(
                response.status, json.loads(response.read()), dict(response.headers)
            )

    def start(self) -> None:
        if self.process is not None:
            raise AssertionError("controller already owns a process")
        environment = self.fleet.cluster.environment.copy()
        environment.update(
            {
                "EPOCH_CONTROL_ADDR": f"127.0.0.1:{self.http_port}",
                "EPOCH_CONTROL_GRPC_ADDR": f"127.0.0.1:{self.grpc_port}",
                "EPOCH_CONTROL_METRICS_ADDR": f"127.0.0.1:{self.metrics_port}",
                "EPOCH_CONTROL_INSTANCE_ID": self.label,
                "EPOCH_CONTROL_REGIONAL_ENDPOINTS": ",".join(
                    f"http://127.0.0.1:{self.fleet.cluster.http_ports[node]}"
                    for node in regional.NODES
                ),
                "EPOCH_CONTROL_ALLOWED_ORIGINS": "https://console.example.test",
                "EPOCH_CONTROL_RECONCILE_INTERVAL": "100ms",
                "EPOCH_CONTROL_LEGACY_STATE_PATH": str(self.directory / "legacy.db"),
                "EPOCH_CONTROL_AUDIT_PATH": str(self.directory / "audit.ndjson"),
                "EPOCH_AUTH_POLICY_PATH": str(regional.AUTH_POLICY_PATH),
                "EPOCH_CONTROL_REGIONAL_TOKEN": regional.CONTROL_TOKEN,
            }
        )
        self.process = subprocess.Popen(
            [str(self.fleet.cluster.control_binary)],
            cwd=regional.REPO_ROOT,
            env=environment,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        regional.wait_until(
            f"controller {self.label} health",
            lambda: self.request("GET", "/healthz").status == 200,
        )
        health = self.request("GET", "/healthz")
        assert (
            health.document.get("registry") == "catalog_consensus_v1"
            and health.document.get("registry_durable") is True
        ), health
        self.log.flush()
        owners = []
        for line in self.log_path.read_text().splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("msg") == "epoch control plane listening":
                owners.append(record.get("control_owner_id"))
        assert owners and isinstance(owners[-1], str), (
            "startup did not report a process owner"
        )
        self.owner_id = owners[-1]

    def resume(self) -> None:
        if self.paused and self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGCONT)
        self.paused = False

    def stop(self) -> None:
        if self.process is not None:
            stop_process(self.process, paused=self.paused)
        self.process = None
        self.paused = False


class OwnerFleet:
    def __init__(self, count: int, artifact_dir: Path) -> None:
        self.cluster = regional.RegionalCluster()
        self.cluster.project = f"epoch-control-ha-{os.getpid()}-{count}"
        self.artifact_dir = artifact_dir
        self.count = count
        for node, port in zip(regional.NODES, regional.allocate_ports(3), strict=True):
            self.cluster.environment[f"EPOCH_REGIONAL_METRICS_PORT_{node}"] = str(port)
        self.controllers = [
            Controller(self, f"ha-control-{count}-{index}", f"control-{index}")
            for index in range(count)
        ]
        self.extras: list[Controller] = []
        self.resources = [
            regional.Resource(profile, f"ha-{profile}") for profile in PROFILES
        ]
        self.http_resources = [
            regional.Resource("cache", f"writer-{index}") for index in range(count)
        ]
        self.result: dict[str, Any] = {
            "controller_count": count,
            "owners": [],
            "checks": {},
            "lease_transitions": [],
            "observations": {},
        }

    def select_control(self) -> Controller:
        controller = next(
            controller
            for controller in self.controllers
            if controller.process is not None
            and controller.process.poll() is None
            and not controller.paused
        )
        self.cluster.control_http_port = controller.http_port
        return controller

    def leader(self) -> int:
        def observed() -> int | None:
            for node in regional.NODES:
                try:
                    response = self.cluster.request(
                        node,
                        "GET",
                        "/experimental/v1/consensus/status",
                        headers={"authorization": f"Bearer {regional.ADMIN_TOKEN}"},
                    )
                except OSError:
                    continue
                if response.status == 200 and response.document.get("role") == "leader":
                    return node
            return None

        return regional.wait_until("Catalog leader", observed)

    def raw(self, method: str, suffix: str, body: dict | None = None) -> Any:
        return self.cluster.request(self.leader(), method, CONTROL_ROOT + suffix, body)

    def lease(self) -> dict[str, Any]:
        response = self.raw("GET", "/lease")
        assert response.status == 200, response
        return response.document

    def active_owner(self, lease: dict[str, Any]) -> Controller:
        return next(
            controller
            for controller in self.controllers
            if controller.owner_id == lease["owner_id"]
            and controller.process is not None
        )

    def takeover(self, old: dict[str, Any], fault: str) -> dict[str, Any]:
        def observed() -> dict[str, Any] | None:
            lease = self.lease()
            if lease["owner_id"] != old["owner_id"] and int(lease["fence"]) > int(
                old["fence"]
            ):
                self.active_owner(lease)
                return lease
            return None

        lease = regional.wait_until(
            f"{fault} fenced takeover", observed, timeout_seconds=35
        )
        self.result["lease_transitions"].append(
            {
                "fault": fault,
                "old_owner": old["owner_id"],
                "new_owner": lease["owner_id"],
                "old_fence": int(old["fence"]),
                "new_fence": int(lease["fence"]),
            }
        )
        return lease

    def stop_nodes(self, nodes: tuple[int, ...]) -> None:
        for node in nodes:
            self.cluster.stop_node(node)
            identifier = self.cluster.compose(
                "ps", "--all", "--quiet", self.cluster.service(node)
            ).stdout.strip()
            assert re.fullmatch(r"[0-9a-f]{64}", identifier), identifier

            def stopped() -> bool:
                result = subprocess.run(
                    ["docker", "inspect", "--format", "{{.State.Running}}", identifier],
                    check=True,
                    text=True,
                    stdout=subprocess.PIPE,
                )
                return result.stdout.strip() == "false"

            regional.wait_until(f"voter {node} stopped", stopped)

    def all_desired(self) -> dict[str, Any]:
        expected = {}
        for resource in self.resources + self.http_resources:
            documents = []
            for controller in self.controllers:
                if controller.process is None:
                    continue
                response = controller.request("GET", resource_path(resource))
                assert response.status == 200, response
                documents.append(
                    {
                        field: response.document[field]
                        for field in ("generation", "spec", "governance")
                    }
                )
            assert documents and all(
                document == documents[0] for document in documents
            ), documents
            expected[resource.name] = documents[0]
        return expected

    def mark(self, check: str) -> None:
        self.result["checks"][check] = True
        print(f"{self.count} controllers: {check}", flush=True)

    def profiles_write(self, sequence: int) -> None:
        for resource in self.resources:
            regional.write_profile(self.cluster, resource, sequence)
            regional.wait_for_profile_apply(self.cluster, resource, sequence)

    def create_and_replay(self) -> None:
        bodies = [
            regional.managed_resource_request(resource)
            for resource in self.http_resources
        ]
        with ThreadPoolExecutor(max_workers=self.count) as executor:
            responses = list(
                executor.map(
                    lambda pair: pair[0].request("PUT", "/v1/resources", pair[1]),
                    zip(self.controllers, bodies, strict=True),
                )
            )
        assert all(response.status == 201 for response in responses), responses
        for resource in self.resources:
            body = managed_request(resource)
            if resource.kind == "cache":
                body["resource"]["spec"]["configuration"] = {
                    "shard_count": 1,
                    "max_entries": 12,
                    "max_memory_bytes": 262144,
                    "max_cold_bytes": 262144,
                    "default_ttl_ms": None,
                    "eviction": "all_keys_lru",
                    "durability": "quorum_durable",
                }
            response = self.select_control().request("PUT", "/v1/resources", body)
            assert response.status == 201, response
        self.all_desired()
        self.mark("concurrent_http_writes_visible_everywhere")
        for body in bodies:
            for controller in self.controllers:
                response = controller.request("PUT", "/v1/resources", body)
                assert (
                    response.status == 200 and response.document.get("replayed") is True
                ), response
            conflicting = json.loads(json.dumps(body))
            conflicting["resource"]["spec"]["shard_count"] = 2
            response = self.select_control().request(
                "PUT", "/v1/resources", conflicting
            )
            assert response.status == 409, response
        self.all_desired()
        self.mark("exact_retry_and_conflicting_token")
        self.select_control()
        for resource in self.resources + self.http_resources:
            regional.wait_for_managed_placement(
                self.cluster,
                regional.Resource(control_kind(resource), resource.name),
                "ready",
                3,
                expected_shards=1,
            )
            native = [
                self.cluster.request(node, "GET", resource.catalog_path).document
                for node in regional.NODES
            ]
            assert all(document == native[0] for document in native), native
            assert len(native[0]["tablets"]) == 1 and native[0]["generation"] == "1", (
                native
            )
        self.mark("canonical_native_materialization")

    def same_label_replacement(self) -> None:
        before = self.lease()
        old = self.active_owner(before)
        replacement = Controller(self, old.label, "replacement")
        self.extras.append(replacement)
        replacement.start()
        assert replacement.owner_id != old.owner_id and replacement.owner_id.startswith(
            old.label + "@"
        ), (old.owner_id, replacement.owner_id)
        after = self.lease()
        assert (
            after["owner_id"] == before["owner_id"]
            and after["fence"] == before["fence"]
        ), (before, after)
        replacement.stop()
        self.result["observations"]["same_label_replacement"] = {
            "label": old.label,
            "original_owner": old.owner_id,
            "replacement_owner": replacement.owner_id,
            "fence": before["fence"],
        }
        self.mark("same_label_replacement_has_fresh_owner")

    def owner_failures(self) -> None:
        old = self.lease()
        owner = self.active_owner(old)
        assert owner.process is not None
        owner.process.kill()
        owner.process.wait(timeout=5)
        owner.process = None
        self.takeover(old, "owner_sigkill")
        self.select_control()
        self.profiles_write(2)
        self.all_desired()
        self.mark("actual_owner_sigkill_takeover")

        old = self.lease()
        owner = self.active_owner(old)
        assert owner.process is not None
        owner.process.send_signal(signal.SIGSTOP)
        owner.paused = True
        try:
            self.takeover(old, "owner_sigstop")
            self.select_control()
            self.profiles_write(3)
            self.reject_old_guard(old)
        finally:
            owner.resume()
        self.all_desired()
        self.mark("paused_owner_takeover_and_four_guard_rejections")

    def reject_old_guard(self, old: dict[str, Any]) -> None:
        resource = self.resources[1]
        suffix = "/resources/acme/shop/dev/core/stream/" + resource.name
        desired = self.raw("GET", suffix).document
        allocations = self.raw("GET", "/allocations").document
        native = self.cluster.request(
            self.leader(), "GET", resource.catalog_path
        ).document
        tablet = native["tablets"][0]
        guard = {
            "owner_id": old["owner_id"],
            "fence": old["fence"],
            "now_ms": str(time.time_ns() // 1000000),
        }
        capacity = [
            {
                "node_id": str(node),
                "max_consensus_groups": 16,
                "used_consensus_groups": len(self.resources + self.http_resources) + 1,
                "catalog_groups": len(self.resources + self.http_resources),
            }
            for node in regional.NODES
        ]
        spec = {
            field: native[field]
            for field in (
                "workload_profile",
                "shard_count",
                "replica_count",
                "configuration",
                "governance",
            )
            if field in native
        }
        placement = {
            "name": resource_name(resource),
            "expected_desired_generation": desired["generation"],
            "expected_catalog_generation": native["generation"],
            "spec": spec,
            "tablet_placements": [
                {"shard_index": 0, "voter_node_ids": list(regional.NODES)}
            ],
        }
        requests = (
            (
                "PUT",
                suffix + "/status",
                {
                    "expected_generation": desired["generation"],
                    "status": {"ha_forbidden": True},
                },
            ),
            ("POST", "/reconcile", {"capacity": capacity, "resources": [placement]}),
            (
                "POST",
                f"/tablets/{tablet['tablet_id']}/membership",
                {
                    "capacity": capacity,
                    "name": resource_name(resource),
                    "expected_desired_generation": desired["generation"],
                    "expected_tablet_epoch": tablet["tablet_epoch"],
                    "expected_resource_generation": native["generation"],
                    "target_voter_node_ids": list(regional.NODES),
                },
            ),
            (
                "DELETE",
                "/materializations/acme/shop/dev/core/stream/" + resource.name,
                {"expected_desired_generation": desired["generation"]},
            ),
        )
        rejected = []
        for index, (method, path, body) in enumerate(requests):
            token = f"ha-stale-{self.count}-{index}"
            response = self.raw(
                method, path, {**body, "request_token": token, "lease": guard}
            )
            assert response.status == 409, response
            operation = self.raw("GET", "/operations/" + token)
            assert (
                operation.status == 200
                and operation.document.get("state") == "failed"
                and operation.document["mutation"].get("code") == "fenced"
            ), operation
            rejected.append(
                {
                    "token": token,
                    "command_kind": operation.document["command_kind"],
                    "code": operation.document["mutation"]["code"],
                }
            )
        after = self.raw("GET", suffix).document
        assert (
            after["generation"] == desired["generation"]
            and after["desired"] == desired["desired"]
            and not after["status"].get("ha_forbidden")
        ), after
        assert self.raw("GET", "/allocations").document == allocations
        self.result["observations"]["stale_guard_rejections"] = rejected

    def quorum_loss(self) -> None:
        self.stop_nodes((1, 2))
        survivors = [
            controller
            for controller in self.controllers
            if controller.process is not None
        ]
        body = regional.managed_resource_request(
            regional.Resource("stream", "unknown-quorum-write")
        )

        def unavailable(controller: Controller) -> int:
            response = controller.request("GET", resource_path(self.resources[0]))
            assert response.status == 503, response
            return response.status

        def uncertain_write(controller: Controller) -> int:
            try:
                response = controller.request("PUT", "/v1/resources", body)
            except OSError:
                return 0
            assert response.status == 503, response
            return response.status

        with ThreadPoolExecutor(max_workers=self.count) as executor:
            reads = list(executor.map(unavailable, survivors))
            writes = list(executor.map(uncertain_write, survivors))
        self.result["observations"]["quorum_loss"] = {
            "read_statuses": reads,
            "write_statuses": writes,
            "original_token": body["request_token"],
        }
        self.mark("catalog_quorum_loss_fails_closed")
        for node in (1, 2):
            self.cluster.start_node(node)
        regional.wait_for_nodes(self.cluster)

        def resolved() -> Any:
            response = self.select_control().request("PUT", "/v1/resources", body)
            return response if response.status in (200, 201) else None

        response = regional.wait_until(
            "unknown write original-token resolution", resolved
        )
        assert response.document["resource"]["generation"] == 1, response
        for controller in survivors:
            replay = controller.request("PUT", "/v1/resources", body)
            assert replay.status == 200 and replay.document.get("replayed") is True, (
                replay
            )
        self.mark("unknown_quorum_write_resolves_by_original_token")

    def reopen(self) -> None:
        desired = self.all_desired()
        before = {
            resource.kind: regional.wait_for_profile_recovery(self.cluster, resource, 3)
            for resource in self.resources
        }
        self.result["profile_digests_before_reopen"] = before
        for controller in self.controllers:
            controller.stop()
        self.stop_nodes(regional.NODES)
        self.cluster.restart_all()
        regional.wait_for_nodes(self.cluster)
        after = {
            resource.kind: regional.wait_for_profile_recovery(self.cluster, resource, 3)
            for resource in self.resources
        }
        self.result["profile_digests_after_reopen"] = after
        assert after == before, (before, after)
        self.mark("four_profile_digests_survive_reopen")
        for controller in self.controllers:
            controller.start()
        assert self.all_desired() == desired
        for resource in self.http_resources:
            body = regional.managed_resource_request(resource)
            for controller in self.controllers:
                response = controller.request("PUT", "/v1/resources", body)
                assert (
                    response.status == 200 and response.document.get("replayed") is True
                ), response
        for rejection in self.result["observations"]["stale_guard_rejections"]:
            response = self.raw("GET", "/operations/" + rejection["token"])
            assert (
                response.status == 200
                and response.document.get("state") == "failed"
                and response.document["mutation"]["code"] == "fenced"
            ), response
        self.mark("managed_state_and_outcomes_survive_reopen")

    def capture(self) -> None:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)

        def redacted(text: str) -> str:
            for token in (regional.ADMIN_TOKEN, regional.CONTROL_TOKEN):
                text = text.replace(token, "[redacted]")
            return text

        for index, controller in enumerate(self.controllers + self.extras):
            controller.log.flush()
            (self.artifact_dir / f"control-{index}.log").write_text(
                redacted(controller.log_path.read_text())
            )
        output = self.cluster.compose(
            "logs", "--no-color", "--tail", "1000", check=False
        )
        (self.artifact_dir / "voters.log").write_text(redacted(output.stdout))
        soak.atomic_write(
            self.artifact_dir / "observations.json", soak.canonical_bytes(self.result)
        )

    def close(self) -> None:
        try:
            for controller in self.controllers + self.extras:
                controller.stop()
            self.capture()
        finally:
            for controller in self.controllers + self.extras:
                controller.resume()
                controller.stop()
                controller.log.close()
            self.cluster.close()

    def run(self) -> dict[str, Any]:
        try:
            self.cluster.start()
            regional.wait_for_nodes(self.cluster)
            subprocess.run(
                [
                    "go",
                    "build",
                    "-o",
                    str(self.cluster.control_binary),
                    "./control/cmd/epoch-control",
                ],
                cwd=regional.REPO_ROOT,
                check=True,
            )
            self.result["observations"]["control_binary_sha256"] = soak.file_receipt(
                self.cluster.control_binary,
                Path(self.cluster.temporary_directory.name),
            )["sha256"]
            for controller in self.controllers:
                controller.start()
            self.result["owners"] = [
                controller.owner_id for controller in self.controllers
            ]
            assert len(set(self.result["owners"])) == self.count
            self.create_and_replay()
            self.profiles_write(1)
            self.same_label_replacement()
            self.owner_failures()
            self.quorum_loss()
            self.reopen()
            return self.result
        finally:
            self.close()


def verify_bundle(path: Path) -> None:
    evidence = soak.load_json(path)
    validate_owner_evidence(evidence)
    if path.read_bytes() != soak.canonical_bytes(evidence):
        raise ValueError("owner evidence must be canonical JSON")
    source = evidence.get("identity", {}).get("source", {})
    if (
        source.get("worktree_clean") is not True
        or re.fullmatch(r"[0-9a-f]{40}", str(source.get("git_revision", ""))) is None
    ):
        raise ValueError("owner evidence requires a frozen clean source revision")
    if evidence.get("identity", {}).get("node_source_matches_image") is not True:
        raise ValueError("node image must match the tested production source tree")
    artifacts = evidence.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("owner evidence requires artifact receipts")
    paths = set()
    for receipt in artifacts:
        if (
            not isinstance(receipt, dict)
            or not isinstance(receipt.get("path"), str)
            or receipt["path"] in paths
        ):
            raise ValueError("invalid or duplicate artifact receipt")
        paths.add(receipt["path"])
        if soak.file_receipt(path.parent / receipt["path"], path.parent) != receipt:
            raise ValueError("owner evidence artifact checksum mismatch")


def run_campaign(output: Path) -> None:
    output = output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("owner evidence destination must be empty")
    source = soak.source_identity()
    if source["worktree_clean"] is not True:
        raise ValueError("freeze the candidate before running owner recovery")
    runtime = soak.runtime_identity(os.environ)
    revision = runtime["image_revision"]
    if (
        not isinstance(revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", revision) is None
        or runtime["image_version"] != source["version"]
    ):
        raise ValueError(
            "node image requires verified source revision and current version labels"
        )
    subprocess.run(
        [
            "git",
            "diff",
            "--quiet",
            revision,
            "HEAD",
            "--",
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            "crates",
            "deploy/docker/Dockerfile.node",
            "spec/proto",
        ],
        cwd=regional.REPO_ROOT,
        check=True,
    )
    identity = {"source": source, "runtime": runtime, "node_source_matches_image": True}
    result: dict[str, Any] = {
        "schema": OWNER_SCHEMA,
        "status": "running",
        "identity": identity,
        "fleets": [],
    }
    output.mkdir(parents=True, exist_ok=True)
    try:
        for count in (3, 5):
            result["fleets"].append(
                OwnerFleet(count, output / f"controllers-{count}").run()
            )
        if soak.source_identity() != source:
            raise ValueError("candidate changed during owner recovery")
        result["status"] = "passed"
        result["artifacts"] = soak.collect_artifacts(output, output)
        validate_owner_evidence(result)
        soak.atomic_write(output / "evidence.json", soak.canonical_bytes(result))
        verify_bundle(output / "evidence.json")
    except BaseException:
        result["status"] = "failed"
        soak.atomic_write(output / "failure.json", soak.canonical_bytes(result))
        raise
    print(f"verified owner-recovery evidence: {output / 'evidence.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    options = parser.parse_args()
    if options.command == "run":
        run_campaign(options.output)
    else:
        verify_bundle(options.manifest)
        print(f"verified owner-recovery evidence: {options.manifest}")


if __name__ == "__main__":
    main()
