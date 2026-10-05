#!/usr/bin/env python3
"""Public Python SDK Catalog-fault probe; explicit owned loopback fixture only."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from google.protobuf.message import DecodeError

from epoch_sdk.management import (
    ManagementClient,
    ManagementConfig,
    ManagementContext,
    ManagementRPCError,
    messages,
)

METHODS = {
    "ApplyResource": (messages.ApplyResourceRequest, "apply_resource"),
    "BatchApplyResources": (
        messages.BatchApplyResourcesRequest,
        "batch_apply_resources",
    ),
    "DeleteResource": (messages.DeleteResourceRequest, "delete_resource"),
    "GetResource": (messages.GetResourceRequest, "get_resource"),
    "GetOperation": (messages.GetOperationRequest, "get_operation"),
    "ListResources": (messages.ListResourcesRequest, "list_resources"),
    "WatchResourceChanges": (
        messages.WatchResourceChangesRequest,
        "watch_resource_changes",
    ),
    "WatchReconnect": (messages.WatchResourceChangesRequest, "watch_resource_changes"),
}


def durable_json(path: Path, document: object) -> None:
    """Atomic file+directory-fsynced fixture progress before public watch ACK."""
    descriptor, temporary = tempfile.mkstemp(prefix=".sdk-checkpoint-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as file:
            file.write(
                json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
            )
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def encoded(message: Any) -> str:
    return base64.b64encode(message.SerializeToString(deterministic=True)).decode(
        "ascii"
    )


def invoke(
    client: ManagementClient,
    item: dict[str, Any],
    context: ManagementContext | None = None,
) -> dict[str, Any]:
    method = item["method"]
    if method not in METHODS:
        raise ValueError("unplanned public management method")
    request_type, member = METHODS[method]
    request = request_type()
    try:
        request.ParseFromString(base64.b64decode(item["request_proto"], validate=True))
    except (ValueError, DecodeError) as error:
        raise ValueError("invalid public SDK request witness") from error
    proof: dict[str, Any] = {
        "method": method,
        "request_proto": item["request_proto"],
        "grpc_code": 0,
        "attempts": 0,
        "budget": 0,
        "outcome_may_be_unknown": False,
    }
    caller = context or ManagementContext(timeout=300)
    try:
        if method.startswith("Watch"):
            watch(client, request, item, proof, caller)
        else:
            result = getattr(client, member)(request, context=caller)
            proof.update(
                response_proto=encoded(result.response),
                attempts=result.info.attempts,
                budget=result.info.budget,
                outcome_may_be_unknown=result.info.outcome_may_be_unknown,
            )
    except ManagementRPCError as error:
        proof.update(
            grpc_code=error.code().value[0],
            attempts=error.info.attempts,
            budget=error.info.budget,
            outcome_may_be_unknown=error.info.outcome_may_be_unknown,
        )
    return proof


def watch(
    client: ManagementClient,
    request: messages.WatchResourceChangesRequest,
    item: dict[str, Any],
    proof: dict[str, Any],
    context: ManagementContext,
) -> None:
    target = 0
    proof["pages_proto"] = []
    with client.watch_resource_changes(request, context=context) as stream:
        for index in range(512):
            page = stream.recv()
            if index == 0:
                target = page.latest_cursor
            checkpoint = str(page.next_cursor)
            durable_json(
                Path(item["checkpoint_path"]),
                {
                    "schema": "epoch.sdk.management.checkpoint/v1",
                    "language": "python",
                    "next_cursor": checkpoint,
                },
            )
            stream.acknowledge(page.next_cursor)
            proof["pages_proto"].append(encoded(page))
            proof.update(
                checkpoint=str(stream.checkpoint),
                attempts=stream.info.attempts,
                budget=stream.info.budget,
            )
            if item["method"] == "WatchReconnect" and index == 0:
                durable_json(Path(item["ready_path"]), proof)
                release = Path(item["release_path"])
                deadline = time.monotonic() + 240
                while not release.exists():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("owned watch release was not observed")
                    time.sleep(0.01)
            if (
                item["method"] == "WatchReconnect"
                and stream.info.attempts > 1
                or item["method"] == "WatchResourceChanges"
                and page.next_cursor >= target
            ):
                return
    raise ValueError("watch exceeded its bounded page budget")


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate SDK plan field")
        result[key] = value
    return result


def load_plan(path: Path) -> dict[str, Any]:
    with path.open("rb") as file:
        data = file.read((8 << 20) + 1)
    if len(data) > 8 << 20:
        raise ValueError("SDK plan exceeds its byte budget")
    plan = json.loads(data.decode("utf-8"), object_pairs_hook=unique_object)
    if (
        not isinstance(plan, dict)
        or set(plan) != {"schema", "phase", "endpoints", "timeout_seconds", "actions"}
        or plan["schema"] != "epoch.sdk.management.plan/v1"
        or not isinstance(plan["phase"], str)
        or not plan["phase"]
        or type(plan["timeout_seconds"]) is not int
        or not 1 <= plan["timeout_seconds"] <= 120
        or not isinstance(plan["actions"], list)
        or not 1 <= len(plan["actions"]) <= 512
    ):
        raise ValueError("invalid bounded public SDK plan")
    for item in plan["actions"]:
        if (
            not isinstance(item, dict)
            or not {"method", "request_proto", "expected_code"} <= set(item)
            or set(item)
            - {
                "method",
                "request_proto",
                "expected_code",
                "expected_unknown",
                "checkpoint_path",
                "ready_path",
                "release_path",
            }
            or item["method"] not in METHODS
            or type(item["expected_code"]) is not int
            or not 0 <= item["expected_code"] <= 16
            or "expected_unknown" in item
            and type(item["expected_unknown"]) is not bool
        ):
            raise ValueError("invalid planned public SDK action")
    return plan


def execute(plan: dict[str, Any], proof: dict[str, Any]) -> None:
    proof["phase"] = plan["phase"]
    with ManagementClient(
        ManagementConfig(
            endpoints=tuple(plan["endpoints"]),
            bearer_token=os.environ["EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN"],
            timeout=plan["timeout_seconds"],
            allow_insecure_loopback=True,
        )
    ) as client:
        context = ManagementContext(timeout=300)
        for item in plan["actions"]:
            result = invoke(client, item, context)
            proof["actions"].append(result)
            if result["grpc_code"] != item["expected_code"] or (
                "expected_unknown" in item
                and result["outcome_may_be_unknown"] != item["expected_unknown"]
            ):
                raise ValueError(
                    "public SDK witness differs from the planned code/unknown outcome"
                )
    proof["passed"] = True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    arguments = parser.parse_args()
    proof: dict[str, Any] = {
        "schema": "epoch.sdk.management.probe/v1",
        "language": "python",
        "phase": "",
        "passed": False,
        "actions": [],
    }
    failed = False
    try:
        execute(load_plan(arguments.plan), proof)
    except Exception as error:
        failed = True
        print(
            f"public Python SDK probe failed ({type(error).__name__})", file=sys.stderr
        )
    print(json.dumps(proof, sort_keys=True, separators=(",", ":")))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
