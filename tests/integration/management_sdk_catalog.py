#!/usr/bin/env python3
"""Certify public Go/Java/Python management clients against real Catalog faults.

The owned loopback response relay is test instrumentation, never an authority.
Only real controller receipts, stopped Catalog containers, original-token
lookups, and durable application checkpoints can certify the campaign.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import control_ha_full as full
from epoch_sdk._generated.epoch.v1 import common_pb2 as common
from epoch_sdk._generated.epoch.v1 import regional_admin_pb2 as messages
from google.protobuf.message import DecodeError, Message

owner = full.owner
LANGUAGES = ("go", "java", "python")
SCHEMA = "epoch.sdk.management.catalog-certification/v1"
FLEET_SCHEMA = "epoch.sdk.management.catalog-fleet/v1"
PROBE_SCHEMA = "epoch.sdk.management.probe/v1"
PLAN_SCHEMA = "epoch.sdk.management.plan/v1"
# Catalog maps expired retained history (CodeConflict) to gRPC ABORTED.
STALE_WATCH_CODE = 10
PHASES = (
    "prepare",
    "lost-ack",
    "resolved",
    "after-owner",
    "after-quorum",
    "after-reopen",
    "stale",
)


def encoded(message: Message) -> str:
    return base64.b64encode(message.SerializeToString(deterministic=True)).decode(
        "ascii"
    )


def decoded(value: str, message_type: Any) -> Any:
    if not isinstance(value, str) or not value:
        raise ValueError("missing nonempty protobuf witness")
    try:
        data = base64.b64decode(value, validate=True)
        if len(data) > 5 << 20 or base64.b64encode(data).decode("ascii") != value:
            raise ValueError("noncanonical or oversized protobuf witness")
        return message_type.FromString(data)
    except (ValueError, binascii.Error, DecodeError) as error:
        raise ValueError("invalid protobuf witness") from error


def resource_name(count: int, language: str, suffix: str) -> Any:
    if count not in (3, 5) or language not in LANGUAGES:
        raise ValueError("explicit bounded fleet and public SDK language required")
    return common.ResourceName(
        organization="acme",
        project="shop",
        environment="dev",
        namespace="sdk-certification",
        kind=common.RESOURCE_KIND_CACHE,
        name=f"sdk-{count}-{language}-{suffix}",
    )


def resource_spec(language: str) -> Any:
    spec = common.ResourceSpec(
        workload_profile=common.WORKLOAD_PROFILE_CACHE,
        durability=common.DURABILITY_PROFILE_QUORUM_DURABLE,
        replicas=3,
        governance=common.ResourceGovernance(
            owner="team:sdk-certification",
            cost_center="cc-sdk",
            classification=common.DATA_CLASSIFICATION_INTERNAL,
            tags={"sdk": language},
        ),
        placement=common.PlacementPolicy(
            allowed_regions=["ap-south"],
            minimum_zones=3,
            required_node_class="general-purpose",
        ),
    )
    spec.configuration.update({"shard_count": 1})
    return spec


def original_batch(count: int, language: str) -> Any:
    return messages.BatchApplyResourcesRequest(
        request_token=f"sdk-original-batch-{count}-{language}",
        resources=[
            messages.BatchApplyResource(
                name=resource_name(count, language, suffix),
                spec=resource_spec(language),
                expected_generation=0,
            )
            for suffix in ("left", "right")
        ],
    )


def watch_request(*, after: int = 0) -> Any:
    return messages.WatchResourceChangesRequest(
        after_cursor=after,
        batch_size=32,
        organization="acme",
        project="shop",
        environment="dev",
        namespace="sdk-certification",
        kind=common.RESOURCE_KIND_CACHE,
    )


def action(
    method: str, request: Message, *, code: int = 0, unknown: bool = False, **paths: str
) -> dict[str, Any]:
    return {
        "method": method,
        "request_proto": encoded(request),
        "expected_code": code,
        "expected_unknown": unknown,
        **paths,
    }


def plan(
    phase: str, endpoints: list[str], actions: list[dict[str, Any]]
) -> dict[str, Any]:
    for endpoint in endpoints:
        host, port = endpoint.rsplit(":", 1)
        if (
            not ipaddress.ip_address(host).is_loopback
            or not 0 < int(port) <= 65535
            or str(int(port)) != port
        ):
            raise ValueError(
                "campaign must address explicitly owned loopback controllers"
            )
    if not endpoints or len(set(endpoints)) != len(endpoints):
        raise ValueError("distinct owned endpoints required")
    return {
        "schema": PLAN_SCHEMA,
        "phase": phase,
        "endpoints": endpoints,
        "timeout_seconds": 120,
        "actions": actions,
    }


def validate_probe(
    language: str, workload: dict[str, Any], proof: dict[str, Any]
) -> None:
    if (
        language not in LANGUAGES
        or proof.get("schema") != PROBE_SCHEMA
        or proof.get("language") != language
        or proof.get("phase") != workload["phase"]
        or proof.get("passed") is not True
    ):
        raise ValueError("wrong or incomplete public SDK probe receipt")
    observed = proof.get("actions")
    if not isinstance(observed, list) or len(observed) != len(workload["actions"]):
        raise ValueError("SDK did not execute every original planned action")
    for expected, actual in zip(workload["actions"], observed, strict=True):
        if (
            actual.get("method") != expected["method"]
            or actual.get("request_proto") != expected["request_proto"]
            or type(actual.get("grpc_code")) is not int
            or actual["grpc_code"] != expected["expected_code"]
            or actual.get("outcome_may_be_unknown") is not expected["expected_unknown"]
        ):
            raise ValueError("SDK changed original bytes, code, or unknown outcome")
        if (
            type(actual.get("attempts")) is not int
            or type(actual.get("budget")) is not int
            or not 0
            < actual["attempts"]
            <= actual["budget"]
            == len(workload["endpoints"])
        ):
            raise ValueError("SDK exceeded or omitted its actual endpoint budget")
        if actual["grpc_code"] != 0 and actual.get("response_proto"):
            raise ValueError(
                "an uncertain or failed call cannot have an acknowledged receipt"
            )
        if actual["grpc_code"] == 0 and not expected["method"].startswith("Watch"):
            response_type = {
                "ApplyResource": messages.ApplyResourceResponse,
                "BatchApplyResources": messages.BatchApplyResourcesResponse,
                "DeleteResource": messages.DeleteResourceResponse,
                "GetResource": messages.GetResourceResponse,
                "GetOperation": messages.GetOperationResponse,
                "ListResources": messages.ListResourcesResponse,
            }.get(expected["method"])
            if response_type is None:
                raise ValueError("unplanned public management method")
            decoded(actual.get("response_proto"), response_type)
        if expected["method"].startswith("Watch") and actual["grpc_code"] == 0:
            validate_watch(
                decoded(
                    expected["request_proto"], messages.WatchResourceChangesRequest
                ),
                actual,
                reconnect=expected["method"] == "WatchReconnect",
            )


def validate_watch(
    request: Any, proof: dict[str, Any], *, reconnect: bool = False
) -> None:
    pages = proof.get("pages_proto")
    if (
        not isinstance(pages, list)
        or not 1 <= len(pages) <= 512
        or (reconnect and (len(pages) < 2 or proof.get("attempts", 0) <= 1))
    ):
        raise ValueError("missing bounded watch or actual controller reconnect")
    cursor = request.after_cursor
    for raw in pages:
        page = decoded(raw, messages.WatchResourceChangesResponse)
        if (
            not cursor <= page.next_cursor <= page.latest_cursor
            or page.earliest_cursor > cursor + 1
            or page.earliest_cursor > page.latest_cursor + 1
            or len(page.changes) > request.batch_size
        ):
            raise ValueError("watch checkpoint reset, future cursor, or unbounded page")
        previous = cursor
        for change in page.changes:
            if (
                not previous < change.cursor <= page.next_cursor
                or change.generation == 0
                or change.kind not in (1, 2, 3)
            ):
                raise ValueError("watch changes do not match their scanned checkpoint")
            previous = change.cursor
            for field in (
                "organization",
                "project",
                "environment",
                "namespace",
                "kind",
            ):
                expected = getattr(request, field)
                if expected and getattr(change.name, field) != expected:
                    raise ValueError("watch leaked a resource outside original filters")
        cursor = page.next_cursor
    if proof.get("checkpoint") != str(cursor):
        raise ValueError("durable checkpoint differs from the scanned watch cursor")


def operation_witness(raw: str) -> str:
    return encoded(decoded(raw, messages.GetOperationResponse))


def validate_operation(request: Any, operation: Any) -> None:
    if (
        operation.request_token != request.request_token
        or operation.state != messages.OPERATION_STATE_SUCCEEDED
        or operation.proposal_id == 0
        or list(operation.affected_resources)
        != [item.name for item in request.resources]
        or operation.command_kind != "apply_desired"
        or operation.HasField("expected_generation")
        or operation.failure_code
        or operation.failure_message
        or operation.first_change_cursor == 0
        or operation.last_change_cursor
        != operation.first_change_cursor + len(request.resources) - 1
    ):
        raise ValueError(
            "durable operation differs from the original complete atomic batch"
        )


def validate_batch_receipt(request: Any, result: Any) -> None:
    # A reconstructed first receipt may be replay-marked. Operation identity
    # and the exact committed creation page, not flags, prove durable effects.
    if len(result.results) != len(request.resources) or any(
        not item.created
        or not item.changed
        or item.resource.generation != 1
        or item.resource.name != original.name
        or item.resource.spec != original.spec
        for item, original in zip(result.results, request.resources, strict=True)
    ):
        raise ValueError(
            "Catalog did not acknowledge complete original SDK batch creations"
        )


def validate_creation_page(request: Any, operation: Any, page: Any) -> None:
    validate_operation(request, operation)
    if (
        len(page.changes) != len(request.resources)
        or page.next_cursor != operation.last_change_cursor
    ):
        raise ValueError("missing exact atomic SDK batch creation history")
    for index, original in enumerate(request.resources):
        change = page.changes[index]
        if (
            change.cursor != operation.first_change_cursor + index
            or change.kind != messages.RESOURCE_CHANGE_KIND_DESIRED_APPLIED
            or change.name != original.name
            or change.generation != 1
        ):
            raise ValueError(
                "SDK batch history repeated, skipped, or changed an original creation"
            )


def validate_catalog_fault(fault: dict[str, Any], batches: dict[str, Any]) -> None:
    if (
        type(fault.get("old_catalog_leader")) is not int
        or type(fault.get("new_catalog_leader")) is not int
        or fault["old_catalog_leader"] not in owner.regional.NODES
        or fault["new_catalog_leader"] not in owner.regional.NODES
        or fault["old_catalog_leader"] == fault["new_catalog_leader"]
        or re.fullmatch(r"[0-9a-f]{64}", str(fault.get("killed_container", ""))) is None
        or fault.get("pending_callers_at_kill") != 3
        or fault.get("relay_exit_status") != -9
        or fault.get("old_leader_stopped_before_response_drop") is not True
    ):
        raise ValueError(
            "missing real Catalog leader loss with all SDK callers pending"
        )
    held = fault.get("held", {})
    receipts = held.get("receipts")
    if (
        held.get("schema") != "epoch.sdk.management.held-receipts/v1"
        or not isinstance(receipts, list)
        or len(receipts) != 3
    ):
        raise ValueError("missing all three real upstream SDK receipts")
    expected = {batch.request_token: batch for batch in batches.values()}
    seen = set()
    for receipt in receipts:
        token = receipt.get("request_token")
        if token not in expected or token in seen:
            raise ValueError("upstream receipt invented or repeated an original token")
        seen.add(token)
        request = expected[token]
        if receipt.get("request_proto") != encoded(request):
            raise ValueError("relay did not forward exact original SDK command bytes")
        result = decoded(
            receipt.get("response_proto"), messages.BatchApplyResourcesResponse
        )
        validate_batch_receipt(request, result)


def validate_evidence(
    evidence: dict[str, Any], counts: tuple[int, ...] = (3, 5)
) -> None:
    from management_sdk_catalog_evidence import validate_evidence as validate

    validate(evidence, counts)


def verify_bundle(path: Path, counts: tuple[int, ...] = (3, 5)) -> None:
    from management_sdk_catalog_evidence import verify_bundle as verify

    verify(path, counts)


def combine_fleet_bundles(manifests: list[Path], output: Path) -> None:
    from management_sdk_catalog_evidence import combine_fleet_bundles as combine

    combine(manifests, output)


class Probe:
    """One owned CLI process with bounded receipts and deterministic cleanup."""

    def __init__(
        self,
        command: list[str],
        workload: dict[str, Any],
        language: str,
        directory: Path,
        environment: dict[str, str],
    ) -> None:
        self.workload, self.language = workload, language
        self.plan_path = directory / f"sdk-{workload['phase']}-{language}-plan.json"
        self.proof_path = directory / f"sdk-{workload['phase']}-{language}-proof.json"
        self.error_path = directory / f"sdk-{workload['phase']}-{language}.log"
        owner.soak.atomic_write(self.plan_path, owner.soak.canonical_bytes(workload))
        self.output = self.proof_path.open("wb")
        self.errors = self.error_path.open("wb")
        try:
            self.process = subprocess.Popen(
                [*command, "--plan", str(self.plan_path)],
                cwd=owner.regional.REPO_ROOT,
                env=environment,
                stdout=self.output,
                stderr=self.errors,
            )
        except BaseException:
            self.output.close()
            self.errors.close()
            raise

    def finish(self) -> dict[str, Any]:
        try:
            code = self.process.wait(timeout=300)
        except BaseException:
            self.close()
            raise
        self.output.close()
        self.errors.close()
        diagnostics = self.error_path.read_text(errors="replace")
        for secret in (owner.regional.ADMIN_TOKEN, "epoch-dev-reader-v1"):
            diagnostics = diagnostics.replace(secret, "[redacted]")
        owner.soak.atomic_write(self.error_path, diagnostics.encode())
        if code != 0 or self.proof_path.stat().st_size > 8 << 20:
            raise ValueError(
                f"public {self.language} SDK {self.workload['phase']} failed; "
                "see owned redacted diagnostics"
            )
        proof = owner.soak.load_json(self.proof_path)
        validate_probe(self.language, self.workload, proof)
        owner.soak.atomic_write(self.proof_path, owner.soak.canonical_bytes(proof))
        return proof

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        self.output.close()
        self.errors.close()


class SDKFleet(full.FullFleet):
    def __init__(self, count: int, artifact_dir: Path) -> None:
        super().__init__(count, artifact_dir)
        self.pending: list[Probe] = []
        self.commands: dict[str, list[str]] = {}
        self.environment = os.environ.copy()
        self.environment["EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN"] = (
            owner.regional.ADMIN_TOKEN
        )
        self.environment["PYTHONPATH"] = str(
            owner.regional.REPO_ROOT / "sdk/python/src"
        )
        self.proxy: subprocess.Popen[Any] | None = None
        self.original = {
            language: original_batch(count, language) for language in LANGUAGES
        }
        self.operations: dict[str, str] = {}
        self.checkpoints: dict[str, int] = {}
        self.result["sdk"] = {"languages": list(LANGUAGES), "phases": [], "faults": {}}

    def endpoints(self, *, first: Any = None) -> list[str]:
        live = [
            controller
            for controller in self.controllers
            if controller.process is not None
            and controller.process.poll() is None
            and not controller.paused
        ]
        if first is not None:
            live = [
                first,
                *[controller for controller in live if controller is not first],
            ]
        return [f"127.0.0.1:{controller.grpc_port}" for controller in live]

    def runtime(self) -> None:
        temporary = Path(self.cluster.temporary_directory.name)
        for name, package in (
            ("sdk-go", "managementsdkgo"),
            ("sdk-proxy", "managementsdkproxy"),
        ):
            subprocess.run(
                [
                    "go",
                    "build",
                    "-trimpath",
                    "-o",
                    str(temporary / name),
                    f"./tests/integration/{package}",
                ],
                cwd=owner.regional.REPO_ROOT,
                check=True,
            )
        classpath = temporary / "sdk-java-classpath.txt"
        subprocess.run(
            [
                str(owner.regional.REPO_ROOT / "sdk/java/mvnw"),
                "--file",
                str(owner.regional.REPO_ROOT / "sdk/java/pom.xml"),
                "--batch-mode",
                "--no-transfer-progress",
                "-DskipTests",
                "test-compile",
                "dependency:build-classpath",
                f"-Dmdep.outputFile={classpath}",
            ],
            cwd=owner.regional.REPO_ROOT,
            check=True,
        )
        java = shutil.which("java")
        if java is None:
            raise ValueError("Java runtime is required for complete SDK certification")
        java_target = owner.regional.REPO_ROOT / "sdk/java/target"
        if not (
            java_target / "test-classes/io/epoch/sdk/ManagementCatalogProbe.class"
        ).is_file():
            raise ValueError("required Java public probe was not compiled")
        self.commands = {
            "go": [str(temporary / "sdk-go")],
            "java": [
                java,
                "-cp",
                f"{java_target}/classes:{java_target}/test-classes:{classpath.read_text().strip()}",
                "io.epoch.sdk.ManagementCatalogProbe",
            ],
            "python": [
                sys.executable,
                str(
                    owner.regional.REPO_ROOT
                    / "tests/integration/management_sdk_python.py"
                ),
            ],
        }
        binaries = [temporary / "sdk-go", temporary / "sdk-proxy"]
        identity = {
            path.name: owner.soak.file_receipt(path, temporary)["sha256"]
            for path in binaries
        }
        identity["java_classes"] = owner.soak.sha256_bytes(
            owner.soak.canonical_bytes(
                owner.soak.collect_artifacts(java_target / "classes", java_target)
            )
        )
        identity["java_probe"] = owner.soak.file_receipt(
            java_target / "test-classes/io/epoch/sdk/ManagementCatalogProbe.class",
            java_target,
        )["sha256"]
        identity["java_dependencies"] = [
            {
                "file": Path(path).name,
                "sha256": owner.soak.sha256_bytes(Path(path).read_bytes()),
            }
            for path in classpath.read_text().strip().split(os.pathsep)
        ]
        self.result["sdk"]["runtime"] = identity
        owner.soak.atomic_write(
            self.artifact_dir / "sdk-workload.json",
            owner.soak.canonical_bytes(
                {
                    "schema": "epoch.sdk.management.original-batches/v1",
                    "batches": {
                        language: encoded(request)
                        for language, request in self.original.items()
                    },
                }
            ),
        )

    def start_probe(self, language: str, workload: dict[str, Any]) -> Probe:
        probe = Probe(
            self.commands[language],
            workload,
            language,
            self.artifact_dir,
            self.environment,
        )
        self.pending.append(probe)
        return probe

    def execute(
        self,
        phase: str,
        actions: dict[str, list[dict[str, Any]]],
        *,
        endpoints: list[str] | None = None,
    ) -> dict[str, Any]:
        probes = [
            self.start_probe(
                language, plan(phase, endpoints or self.endpoints(), actions[language])
            )
            for language in LANGUAGES
        ]
        with ThreadPoolExecutor(max_workers=3) as workers:
            proofs = dict(
                zip(
                    LANGUAGES,
                    workers.map(lambda probe: probe.finish(), probes),
                    strict=True,
                )
            )
        for probe in probes:
            self.pending.remove(probe)
        self.result["sdk"]["phases"].append(phase)
        print(
            f"{self.count} controllers: all three public SDKs passed {phase}",
            flush=True,
        )
        return proofs

    def checkpoint_path(self, language: str, phase: str) -> Path:
        return self.artifact_dir / f"sdk-{phase}-{language}-checkpoint.json"

    def watch_request(self, *, after: int = 0) -> Any:
        return watch_request(after=after)

    def create_and_replay(self) -> None:
        super().create_and_replay()
        self.runtime()
        actions = {}
        for language in LANGUAGES:
            name, spec = (
                resource_name(self.count, language, "unary"),
                resource_spec(language),
            )
            token = f"sdk-unary-{self.count}-{language}"
            delete = messages.DeleteResourceRequest(
                request_token=token + "-delete", name=name, expected_generation=1
            )
            actions[language] = [
                action(
                    "ApplyResource",
                    messages.ApplyResourceRequest(
                        request_token=token, name=name, spec=spec, expected_generation=0
                    ),
                ),
                action("GetResource", messages.GetResourceRequest(name=name)),
                action(
                    "ListResources",
                    messages.ListResourcesRequest(
                        organization="acme",
                        project="shop",
                        environment="dev",
                        namespace="sdk-certification",
                        owner="team:sdk-certification",
                        tags={"sdk": language},
                        page_size=100,
                    ),
                ),
                action("DeleteResource", delete),
                action("DeleteResource", delete),
                action(
                    "GetOperation",
                    messages.GetOperationRequest(
                        request_token=delete.request_token, affected_resources=[name]
                    ),
                ),
                action("GetResource", messages.GetResourceRequest(name=name), code=5),
                action(
                    "WatchResourceChanges",
                    self.watch_request(),
                    checkpoint_path=str(self.checkpoint_path(language, "prepare")),
                ),
            ]
        prepared = self.execute("prepare", actions)
        self.checkpoints = {
            language: int(proof["actions"][-1]["checkpoint"])
            for language, proof in prepared.items()
        }
        self.lost_ack()
        self.recover("resolved")

    def lost_ack(self) -> None:
        proxy_plan = self.artifact_dir / "sdk-proxy-plan.json"
        ready = self.artifact_dir / "sdk-proxy-ready.json"
        endpoints = self.endpoints()
        owner.soak.atomic_write(
            proxy_plan,
            owner.soak.canonical_bytes(
                {
                    "schema": "epoch.sdk.management.lost-ack-plan/v1",
                    "bindings": [
                        {
                            "request_proto": encoded(self.original[language]),
                            "upstream_authority": endpoints[index % len(endpoints)],
                        }
                        for index, language in enumerate(LANGUAGES)
                    ],
                }
            ),
        )
        proxy_log = (self.artifact_dir / "sdk-proxy.log").open("wb")
        try:
            self.proxy = subprocess.Popen(
                [
                    str(Path(self.cluster.temporary_directory.name) / "sdk-proxy"),
                    "--plan",
                    str(proxy_plan),
                    "--ready",
                    str(ready),
                ],
                cwd=owner.regional.REPO_ROOT,
                env=self.environment,
                stdout=proxy_log,
                stderr=proxy_log,
            )
            owner.regional.wait_until(
                "owned SDK relay ready",
                lambda: ready.is_file() and owner.soak.load_json(ready),
                timeout_seconds=15,
            )
            address = owner.soak.load_json(ready)
            probes = [
                self.start_probe(
                    language,
                    plan(
                        "lost-ack",
                        [address["grpc_authority"]],
                        [
                            action(
                                "BatchApplyResources",
                                self.original[language],
                                code=14,
                                unknown=True,
                            )
                        ],
                    ),
                )
                for language in LANGUAGES
            ]

            def held() -> Any:
                if (
                    self.proxy is None
                    or self.proxy.poll() is not None
                    or any(probe.process.poll() is not None for probe in probes)
                ):
                    raise ValueError(
                        "SDK caller or relay exited before fault injection"
                    )
                with urllib.request.urlopen(
                    address["status_url"], timeout=5
                ) as response:
                    observation = json.loads(response.read((4 << 20) + 1))
                return (
                    observation if len(observation.get("receipts", [])) == 3 else None
                )

            observation = owner.regional.wait_until(
                "all SDK upstream batches acknowledged but callers pending",
                held,
                timeout_seconds=60,
            )
            owner.soak.atomic_write(
                self.artifact_dir / "sdk-held-upstream.json",
                owner.soak.canonical_bytes(observation),
            )
            by_token = {
                receipt["request_token"]: receipt for receipt in observation["receipts"]
            }
            for request in self.original.values():
                receipt = by_token[request.request_token]
                if receipt["request_proto"] != encoded(request):
                    raise ValueError("relay did not forward exact original batch bytes")
                result = decoded(
                    receipt["response_proto"], messages.BatchApplyResourcesResponse
                )
                validate_batch_receipt(request, result)
            old = self.leader()
            container = self.cluster.compose(
                "ps", "--all", "--quiet", self.cluster.service(old)
            ).stdout.strip()
            fault = {
                "old_catalog_leader": old,
                "killed_container": container,
                "pending_callers_at_kill": 3,
                "held": observation,
            }
            self.stop_nodes((old,))
            if any(probe.process.poll() is not None for probe in probes):
                raise ValueError("SDK caller finished before Catalog leader stopped")
            fault["old_leader_stopped_before_response_drop"] = True
            self.proxy.kill()
            fault["relay_exit_status"] = self.proxy.wait(timeout=5)
            self.proxy = None
            with ThreadPoolExecutor(max_workers=3) as workers:
                list(workers.map(lambda probe: probe.finish(), probes))
            for probe in probes:
                self.pending.remove(probe)
            fault["new_catalog_leader"] = self.leader()
            if (
                fault["new_catalog_leader"] == old
                or fault["relay_exit_status"] != -9
                or not container
            ):
                raise ValueError(
                    "required real Catalog and socket-loss faults were not observed"
                )
            self.result["sdk"]["faults"]["catalog_leader"] = fault
            owner.soak.atomic_write(
                self.artifact_dir / "sdk-catalog-leader.json",
                owner.soak.canonical_bytes(fault),
            )
            self.result["sdk"]["phases"].append("lost-ack")
            self.cluster.start_node(old)
            owner.regional.wait_for_nodes(self.cluster)
        finally:
            if self.proxy is not None:
                self.proxy.kill()
                self.proxy.wait(timeout=5)
                self.proxy = None
            proxy_log.close()

    def recover(self, phase: str) -> None:
        actions = {}
        endpoints = self.endpoints()
        for language in LANGUAGES:
            batch = self.original[language]
            lookup = messages.GetOperationRequest(
                request_token=batch.request_token,
                affected_resources=[item.name for item in batch.resources],
            )
            actions[language] = [
                action("GetOperation", lookup),
                action("BatchApplyResources", batch),
                *[
                    action("GetResource", messages.GetResourceRequest(name=item.name))
                    for item in batch.resources
                ],
                action(
                    "WatchResourceChanges",
                    self.watch_request(after=self.checkpoints.get(language, 0)),
                    checkpoint_path=str(self.checkpoint_path(language, phase)),
                ),
            ]
        proofs = self.execute(phase, actions, endpoints=endpoints)
        for language, proof in proofs.items():
            operation = decoded(
                proof["actions"][0]["response_proto"], messages.GetOperationResponse
            )
            validate_operation(self.original[language], operation)
            witness = encoded(operation)
            if language in self.operations and self.operations[language] != witness:
                raise ValueError(
                    "complete original-token operation changed across a Catalog fault"
                )
            self.operations[language] = witness
            self.checkpoints[language] = int(proof["actions"][-1]["checkpoint"])
        if phase == "resolved":
            histories = {}
            for language in LANGUAGES:
                operation = decoded(
                    self.operations[language], messages.GetOperationResponse
                )
                request = self.watch_request(after=operation.first_change_cursor - 1)
                request.batch_size = 2
                histories[language] = [
                    action(
                        "WatchResourceChanges",
                        request,
                        checkpoint_path=str(
                            self.checkpoint_path(language, "resolved-creation")
                        ),
                    )
                ]
            creation_proofs = self.execute("resolved-creation", histories)
            for language, proof in creation_proofs.items():
                validate_creation_page(
                    self.original[language],
                    decoded(self.operations[language], messages.GetOperationResponse),
                    decoded(
                        proof["actions"][0]["pages_proto"][0],
                        messages.WatchResourceChangesResponse,
                    ),
                )
        # A failover list is not proof every endpoint returned the receipt.
        # Run each original token through a single-controller public SDK call.
        for index, endpoint in enumerate(endpoints):
            subphase = f"{phase}-controller-{index}"
            single = {language: [actions[language][0]] for language in LANGUAGES}
            witnesses = self.execute(subphase, single, endpoints=[endpoint])
            for language, proof in witnesses.items():
                if (
                    operation_witness(proof["actions"][0]["response_proto"])
                    != self.operations[language]
                ):
                    raise ValueError(
                        "original durable operation differs at a live controller"
                    )

    def owner_failures(self) -> None:
        lease = self.lease()
        controller = self.active_owner(lease)
        endpoints = self.endpoints(first=controller)
        probes = []
        for language in LANGUAGES:
            paths = {
                "checkpoint_path": str(self.checkpoint_path(language, "owner-watch")),
                "ready_path": str(
                    self.artifact_dir / f"sdk-owner-{language}-ready.json"
                ),
                "release_path": str(
                    self.artifact_dir / f"sdk-owner-{language}-release.json"
                ),
            }
            workload = plan(
                "owner-watch",
                endpoints,
                [
                    action(
                        "WatchReconnect",
                        self.watch_request(after=self.checkpoints[language]),
                        **paths,
                    )
                ],
            )
            probes.append(self.start_probe(language, workload))

        def watching() -> bool:
            if any(probe.process.poll() is not None for probe in probes):
                raise ValueError("public watch closed before actual owner failure")
            return all(
                Path(probe.workload["actions"][0]["ready_path"]).is_file()
                for probe in probes
            )

        owner.regional.wait_until(
            "all durable public watches ready on actual owner",
            watching,
            timeout_seconds=30,
        )
        super().owner_failures()
        transition = next(
            item
            for item in self.result["lease_transitions"]
            if item["fault"] == "owner_sigkill"
        )
        if transition["old_owner"] != lease["owner_id"]:
            raise ValueError("fault did not kill the watched actual controller owner")
        for probe in probes:
            owner.soak.atomic_write(
                Path(probe.workload["actions"][0]["release_path"]), b"{}\n"
            )
        with ThreadPoolExecutor(max_workers=3) as workers:
            proofs = list(workers.map(lambda probe: probe.finish(), probes))
        for language, probe, proof in zip(LANGUAGES, probes, proofs, strict=True):
            self.pending.remove(probe)
            self.checkpoints[language] = int(proof["actions"][0]["checkpoint"])
        fault = {
            **transition,
            "first_endpoint": endpoints[0],
            "pending_watchers_at_kill": 3,
        }
        self.result["sdk"]["faults"]["controller_owner"] = fault
        owner.soak.atomic_write(
            self.artifact_dir / "sdk-controller-owner.json",
            owner.soak.canonical_bytes(fault),
        )
        self.recover("after-owner")

    def quorum_loss(self) -> None:
        super().quorum_loss()
        self.recover("after-quorum")

    def reopen(self) -> None:
        # Base reopen also advances the real history floor. Save application
        # checkpoints before doing so; stale evidence must expire these, not 0.
        saved = dict(self.checkpoints)
        super().reopen()
        self.recover("after-reopen")
        target = max(saved.values())
        print(
            f"{self.count} controllers: expiring durable SDK checkpoints through {target}",
            flush=True,
        )
        resource = self.resources[1]
        suffix = f"/resources/acme/shop/dev/core/stream/{resource.name}/status"
        desired = self.raw("GET", suffix.removesuffix("/status")).document
        with ThreadPoolExecutor(max_workers=4) as workers:
            for sequence in range(8192, 16384, 32):
                response = self.raw(
                    "GET", f"/changes?after={max(self.checkpoints.values())}&limit=1"
                )
                if response.status == 409:
                    response = self.raw("GET", "/changes?limit=1")
                if response.status != 200:
                    raise ValueError("cannot observe the real retained watch floor")
                floor = int(response.document["earliest_cursor"])
                if floor > target + 1:
                    retention = {
                        "earliest_cursor": floor,
                        "latest_cursor": int(response.document["latest_cursor"]),
                        "expired_checkpoints": saved,
                    }
                    break
                list(
                    workers.map(
                        lambda item: self.commit_retention_status(
                            suffix, desired["generation"], item
                        ),
                        range(sequence, sequence + 32),
                    )
                )
                if (sequence - 8192 + 32) % 512 == 0:
                    print(
                        f"{self.count} controllers: advancing SDK watch retention "
                        f"({sequence - 8192 + 32} additional status changes)",
                        flush=True,
                    )
            else:
                raise ValueError(
                    "real retention did not expire all durable SDK checkpoints"
                )
        owner.soak.atomic_write(
            self.artifact_dir / "sdk-retention.json",
            owner.soak.canonical_bytes(retention),
        )
        self.result["sdk"]["retention"] = retention
        self.execute(
            "stale",
            {
                language: [
                    action(
                        "WatchResourceChanges",
                        self.watch_request(after=saved[language]),
                        code=STALE_WATCH_CODE,
                        checkpoint_path=str(self.checkpoint_path(language, "stale")),
                    )
                ]
                for language in LANGUAGES
            },
        )

    def close(self) -> None:
        try:
            for probe in self.pending:
                probe.close()
            if self.proxy is not None and self.proxy.poll() is None:
                self.proxy.kill()
                self.proxy.wait(timeout=5)
        finally:
            super().close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--controllers", type=int, choices=(3, 5))
    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--controllers", type=int, choices=(3, 5))
    combine = commands.add_parser("combine")
    combine.add_argument("--three", type=Path, required=True)
    combine.add_argument("--five", type=Path, required=True)
    combine.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "combine":
        combine_fleet_bundles([args.three, args.five], args.output)
        return
    counts = (args.controllers,) if args.controllers else (3, 5)
    if args.command == "verify":
        verify_bundle(args.manifest, counts)
        return
    owner.run_campaign(
        args.output,
        fleet_type=SDKFleet,
        schema=SCHEMA if counts == (3, 5) else FLEET_SCHEMA,
        controller_counts=counts,
        validator=lambda evidence: validate_evidence(evidence, counts),
        bundle_verifier=lambda manifest: verify_bundle(manifest, counts),
    )


if __name__ == "__main__":
    main()
