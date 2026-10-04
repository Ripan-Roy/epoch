#!/usr/bin/env python3
"""Generated gRPC API recovery plus real concurrent owner/voter faults."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

import control_ha as owner

API_SCHEMA = "epoch.control-ha.api-recovery/v1"
GRPC_SCHEMA = "epoch.control-ha.grpc-api/v1"
GRPC_CHECKS = (
    "concurrent_maximum_batch_exact_replay",
    "concurrent_occ_batches_atomic",
    "operation_exact_resource_authorization",
    "delete_recreate_old_tokens_safe",
    "delete_operation_precondition_presence",
    "authorized_watch_disconnect_resume",
    "future_watch_cursor_fails_closed",
    "durable_operations_and_desired_verified",
)
API_CHECKS = (
    *GRPC_CHECKS,
    "stale_watch_cursor_fails_closed",
    "operations_and_desired_survive_owner_and_quorum_loss",
    "operations_and_desired_survive_reopen",
)
API_PHASES = ("prepare", "after-owner", "after-quorum", "after-reopen", "stale")


def validate_api_evidence(evidence: dict[str, Any]) -> None:
    if evidence.get("schema") != API_SCHEMA:
        raise ValueError("wrong generated-client recovery evidence schema")
    owner.validate_owner_evidence({**evidence, "schema": owner.OWNER_SCHEMA})
    for fleet in evidence["fleets"]:
        api = fleet.get("api")
        if not isinstance(api, dict):
            raise ValueError("missing generated-client recovery evidence")
        checks = api.get("checks")
        if (
            not isinstance(checks, dict)
            or set(checks) != set(API_CHECKS)
            or any(checks[check] is not True for check in API_CHECKS)
            or api.get("phases") != list(API_PHASES)
        ):
            raise ValueError("missing, false, or malformed API/fault phase evidence")
        for field, minimum in (("operation_count", 10), ("desired_count", 128)):
            value = api.get(field)
            if type(value) is not int or value < minimum:
                raise ValueError(
                    "generated clients did not verify the required inventory"
                )
        if (
            not isinstance(api.get("helper_sha256"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", api["helper_sha256"]) is None
        ):
            raise ValueError("missing generated-client executable identity")
        retention = api.get("retention")
        if (
            not isinstance(retention, dict)
            or type(retention.get("earliest_cursor")) is not int
            or type(retention.get("latest_cursor")) is not int
            or not 1 < retention["earliest_cursor"] <= retention["latest_cursor"]
        ):
            raise ValueError(
                "stale watch requires an observed advanced retention floor"
            )


def verify_api_bundle(path: Path) -> None:
    owner.verify_bundle(path, validator=validate_api_evidence)
    evidence = owner.soak.load_json(path)
    artifacts = {receipt["path"] for receipt in evidence["artifacts"]}
    for fleet in evidence["fleets"]:
        baseline = None
        for phase in API_PHASES:
            relative = f"controllers-{fleet['controller_count']}/api-{phase}.json"
            if relative not in artifacts:
                raise ValueError("missing checksum-bound generated-client phase")
            proof = owner.soak.load_json(path.parent / relative)
            expected_checks = set(GRPC_CHECKS)
            if phase == "stale":
                expected_checks.add("stale_watch_cursor_fails_closed")
            if (
                proof.get("schema") != GRPC_SCHEMA
                or proof.get("controller_count") != fleet["controller_count"]
                or not isinstance(proof.get("checks"), dict)
                or set(proof["checks"]) != expected_checks
                or any(proof["checks"][check] is not True for check in expected_checks)
                or not isinstance(proof.get("operations"), list)
                or len(proof["operations"]) != fleet["api"]["operation_count"]
                or not isinstance(proof.get("desired_proto"), list)
                or len(proof["desired_proto"]) != fleet["api"]["desired_count"]
            ):
                raise ValueError(
                    "invalid generated-client phase inventory or invariants"
                )
            retained = {
                key: proof.get(key)
                for key in (
                    "operations",
                    "desired_proto",
                    "watch_checkpoint",
                    "watch_matching_cursors",
                )
            }
            if baseline is None:
                baseline = retained
            elif retained != baseline:
                raise ValueError("retained API identities changed across a fault phase")


class APIFleet(owner.OwnerFleet):
    def __init__(self, count: int, artifact_dir: Path) -> None:
        super().__init__(count, artifact_dir)
        self.helper = Path(self.cluster.temporary_directory.name) / "controlgrpc"
        self.proofs: dict[str, dict[str, Any]] = {}
        self.result["api"] = {"checks": {}, "phases": []}

    def generated_clients(self, phase: str) -> dict[str, Any]:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        mode = (
            "prepare"
            if phase == "prepare"
            else "stale"
            if phase == "stale"
            else "verify"
        )
        endpoints = [
            f"127.0.0.1:{controller.grpc_port}"
            for controller in self.controllers
            if controller.process is not None
            and controller.process.poll() is None
            and not controller.paused
        ]
        environment = os.environ.copy()
        environment.update(
            {
                "EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN": owner.regional.ADMIN_TOKEN,
                "EPOCH_CONTROL_HA_GRPC_READER_TOKEN": "epoch-dev-reader-v1",
            }
        )
        output = subprocess.run(
            [
                str(self.helper),
                "--phase",
                mode,
                "--endpoints",
                ",".join(endpoints),
                "--controllers",
                str(self.count),
                "--prefix",
                f"ha-grpc-{self.count}",
                "--state",
                str(self.artifact_dir / "api-prepare.json"),
            ],
            cwd=owner.regional.REPO_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if output.returncode:
            message = output.stderr
            for token in (owner.regional.ADMIN_TOKEN, "epoch-dev-reader-v1"):
                message = message.replace(token, "[redacted]")
            raise AssertionError(f"generated-client phase {phase} failed: {message}")
        proof = json.loads(output.stdout)
        checks = proof.get("checks", {})
        expected = set(GRPC_CHECKS)
        if phase == "stale":
            expected.add("stale_watch_cursor_fails_closed")
        assert set(checks) == expected and all(
            checks[key] is True for key in expected
        ), proof
        self.proofs[phase] = proof
        owner.soak.atomic_write(
            self.artifact_dir / f"api-{phase}.json", owner.soak.canonical_bytes(proof)
        )
        self.result["api"]["checks"].update(checks)
        self.result["api"]["phases"] = [key for key in API_PHASES if key in self.proofs]
        print(f"{self.count} controllers: generated gRPC {phase} passed", flush=True)
        return proof

    def create_and_replay(self) -> None:
        super().create_and_replay()
        subprocess.run(
            ["go", "build", "-o", str(self.helper), "./tests/integration/controlgrpc"],
            cwd=owner.regional.REPO_ROOT,
            check=True,
        )
        self.result["api"]["helper_sha256"] = owner.soak.file_receipt(
            self.helper, Path(self.cluster.temporary_directory.name)
        )["sha256"]
        proof = self.generated_clients("prepare")
        self.result["api"].update(
            operation_count=len(proof["operations"]),
            desired_count=len(proof["desired_proto"]),
        )

    def owner_failures(self) -> None:
        super().owner_failures()
        self.generated_clients("after-owner")

    def quorum_loss(self) -> None:
        super().quorum_loss()
        self.generated_clients("after-quorum")
        self.result["api"]["checks"][
            "operations_and_desired_survive_owner_and_quorum_loss"
        ] = True

    def reopen(self) -> None:
        super().reopen()
        self.generated_clients("after-reopen")
        self.result["api"]["checks"]["operations_and_desired_survive_reopen"] = True
        self.advance_watch_retention()
        self.generated_clients("stale")

    def advance_watch_retention(self) -> None:
        # Recurrent internal status receipts have bounded retention; they can
        # advance the real history without claiming unlimited public tokens.
        resource = self.resources[1]
        suffix = f"/resources/acme/shop/dev/core/stream/{resource.name}/status"
        desired = self.raw("GET", suffix.removesuffix("/status")).document
        cursor = self.proofs["prepare"]["watch_checkpoint"]
        lease: dict[str, Any] = {}
        for sequence in range(8192):
            if sequence % 32 == 0:
                page = self.raw("GET", f"/changes?after={cursor}&limit=1")
                assert page.status == 200, page
                floor = int(page.document["earliest_cursor"])
                cursor = int(page.document["latest_cursor"])
                if floor > 1:
                    self.result["api"]["retention"] = {
                        "earliest_cursor": floor,
                        "latest_cursor": cursor,
                    }
                    return
                lease = self.lease()
                if sequence % 512 == 0:
                    print(
                        f"{self.count} controllers: advancing real watch retention ({sequence} status changes)",
                        flush=True,
                    )
            response = self.raw(
                "PUT",
                suffix,
                {
                    "request_token": f"ha-retention-{self.count}-{sequence}",
                    "expected_generation": desired["generation"],
                    "status": {"ha_retention_sequence": sequence},
                    "lease": {
                        "owner_id": lease["owner_id"],
                        "fence": lease["fence"],
                        "now_ms": str(time.time_ns() // 1000000),
                    },
                },
            )
            assert response.status == 200, response
        raise AssertionError("real history did not advance its retention floor")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--output", required=True, type=Path)
    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", required=True, type=Path)
    options = parser.parse_args()
    if options.command == "run":
        owner.run_campaign(
            options.output,
            fleet_type=APIFleet,
            schema=API_SCHEMA,
            validator=validate_api_evidence,
        )
        verify_api_bundle(options.output / "evidence.json")
    else:
        verify_api_bundle(options.manifest)
        print(f"verified generated-client recovery: {options.manifest}")


if __name__ == "__main__":
    main()
