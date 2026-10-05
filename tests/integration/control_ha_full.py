#!/usr/bin/env python3
"""Complete bounded concurrent control-HA matrix; not production certification."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

import control_ha_api as api
import control_ha_faults as faults

owner = api.owner
FULL_SCHEMA = "epoch.control-ha.full-certification/v1"
FLEET_SCHEMA = "epoch.control-ha.fleet-certification/v1"
LOOKUP_SCHEMA = "epoch.control-ha.operation-bindings/v1"


def validate_lookup_plan(plan: dict[str, Any], bodies: list[dict[str, Any]]) -> None:
    if (
        not isinstance(plan, dict)
        or plan.get("schema") != LOOKUP_SCHEMA
        or not isinstance(bodies, list)
        or not 1 <= len(bodies) <= 8
    ):
        raise ValueError("missing bounded original-token lookup workload")
    expected = []
    tokens = set()
    for body in bodies:
        if not isinstance(body, dict):
            raise ValueError("original leader-loss command must be an object")
        token, resource = body.get("request_token"), body.get("resource")
        if (
            not isinstance(token, str)
            or not 1 <= len(token) <= 128
            or token in tokens
            or not isinstance(resource, dict)
            or resource.get("kind") != "cache"
        ):
            raise ValueError("missing or duplicate exact Cache command identity")
        tokens.add(token)
        name = {}
        for key in ("organization", "project", "environment", "namespace", "name"):
            value = resource.get(key)
            if not isinstance(value, str) or not value:
                raise ValueError("leader-loss resource must be fully qualified")
            name[key] = value
        name["kind"] = "RESOURCE_KIND_CACHE"
        expected.append({"request_token": token, "affected_resources": [name]})
    if plan.get("requests") != expected:
        raise ValueError("lookup plan differs from original tokens or exact scope")


def validate_lookup_proof(proof: dict[str, Any], plan: dict[str, Any]) -> None:
    if proof.get("lookup_bindings") != plan["requests"]:
        raise ValueError("generated proof did not retain the exact lookup plan")
    operations = proof.get("operations")
    if not isinstance(operations, list) or any(
        not isinstance(operation, dict) for operation in operations
    ):
        raise ValueError("generated proof omitted its operation inventory")
    lookups = [
        operation for operation in operations if operation.get("kind") == "lookup"
    ]
    if len(lookups) != len(plan["requests"]):
        raise ValueError("every original token requires one generated lookup witness")
    for lookup in lookups:
        if type(lookup.get("grpc_code")) is not int or lookup["grpc_code"] != 0:
            raise ValueError("original-token lookup did not succeed")
        for field in ("request_proto", "operation_proto"):
            value = lookup.get(field)
            if not isinstance(value, str) or not value:
                raise ValueError(
                    "lookup must retain original request and outcome bytes"
                )
            try:
                if not base64.b64decode(value, validate=True):
                    raise ValueError("empty lookup protobuf")
            except binascii.Error as error:
                raise ValueError("malformed lookup protobuf encoding") from error


def validate_full_evidence(
    evidence: dict[str, Any], *, controller_counts: tuple[int, ...] = (3, 5)
) -> None:
    if evidence.get("schema") != FULL_SCHEMA:
        raise ValueError("wrong complete concurrent-control matrix schema")
    api.validate_api_evidence(
        {**evidence, "schema": api.API_SCHEMA}, controller_counts=controller_counts
    )
    for fleet in evidence["fleets"]:
        leader = fleet.get("catalog_leader_unknown")
        if not isinstance(leader, dict):
            raise ValueError("missing in-flight Catalog-leader unknown-outcome case")
        faults.validate_leader_evidence(leader)
        if (
            leader["controller_count"] != fleet["controller_count"]
            or leader.get("lookups_bound_into_generated_client_proof") is not True
        ):
            raise ValueError(
                "leader case must bind the fleet's exact durable gRPC outcomes"
            )


def verify_full_bundle(
    path: Path,
    *,
    validator: Callable[[dict[str, Any]], None] = validate_full_evidence,
) -> None:
    _verify_full_bundle(path, validator)


def validate_fleet_evidence(evidence: dict[str, Any], count: int) -> None:
    if (
        type(count) is not int
        or count not in (3, 5)
        or evidence.get("schema") != FLEET_SCHEMA
    ):
        raise ValueError("wrong isolated controller-fleet schema or count")
    validate_full_evidence(
        {**evidence, "schema": FULL_SCHEMA}, controller_counts=(count,)
    )


def verify_fleet_bundle(path: Path, count: int) -> None:
    _verify_full_bundle(path, lambda evidence: validate_fleet_evidence(evidence, count))
    evidence = owner.soak.load_json(path)
    runtime = evidence["identity"].get("runtime", {})
    image_id = runtime.get("image_id", "")
    if (
        not isinstance(image_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
    ):
        raise ValueError("fleet proof requires the tested immutable image identity")
    if any(
        not receipt["path"].startswith(f"controllers-{count}/")
        for receipt in evidence["artifacts"]
    ):
        raise ValueError("fleet artifact inventory contains another campaign")


def _verify_full_bundle(
    path: Path, validator: Callable[[dict[str, Any]], None]
) -> None:
    api.verify_api_bundle(path, validator=validator)
    evidence = owner.soak.load_json(path)
    artifacts = {receipt["path"] for receipt in evidence["artifacts"]}
    for fleet in evidence["fleets"]:
        prefix = f"controllers-{fleet['controller_count']}/"
        for name in (
            "leader-unknown.json",
            "leader-requests.json",
            "leader-lookups.json",
        ):
            if prefix + name not in artifacts:
                raise ValueError("missing checksum-bound Catalog-leader workload")
        if (
            owner.soak.load_json(path.parent / (prefix + "leader-unknown.json"))
            != fleet["catalog_leader_unknown"]
        ):
            raise ValueError("leader-loss artifact does not match manifest")
        bodies = owner.soak.load_json_array(
            path.parent / (prefix + "leader-requests.json")
        )
        if not isinstance(bodies, list) or len(bodies) != fleet["controller_count"]:
            raise ValueError("missing exact concurrent request bytes")
        plan = owner.soak.load_json(path.parent / (prefix + "leader-lookups.json"))
        validate_lookup_plan(plan, bodies)
        for phase in api.API_PHASES:
            proof = owner.soak.load_json(path.parent / (prefix + f"api-{phase}.json"))
            validate_lookup_proof(proof, plan)
        by_token = {body["request_token"]: body for body in bodies}
        if len(by_token) != len(bodies):
            raise ValueError("duplicate leader-loss request token")
        for request in fleet["catalog_leader_unknown"]["requests"]:
            body = by_token.get(request["request_token"])
            if (
                body is None
                or "sha256:"
                + hashlib.sha256(
                    json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                != request["command_sha256"]
            ):
                raise ValueError("leader-loss original command checksum differs")


def combine_fleet_bundles(manifests: list[Path], output: Path) -> None:
    """Seal both independently verified fleets without weakening the full gate."""
    if len(manifests) != 2:
        raise ValueError("combining requires ordered three/five-controller proofs")
    candidates = []
    for count, path in zip((3, 5), manifests, strict=True):
        verify_fleet_bundle(path, count)
        candidates.append(owner.soak.load_json(path))
    identity = candidates[0]["identity"]
    if candidates[1]["identity"] != identity:
        raise ValueError("fleet candidate/image identity differs")
    if owner.soak.source_identity() != identity["source"]:
        raise ValueError("fleet source identity differs from the aggregate checkout")
    output = output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("combined evidence destination must be empty")
    if any(
        path.parent.resolve() == output or output in path.resolve().parents
        for path in manifests
    ):
        raise ValueError("aggregate destination must be separate from its inputs")
    output.mkdir(parents=True, exist_ok=True)
    result = {
        "schema": FULL_SCHEMA,
        "status": "passed",
        "identity": identity,
        "fleets": [candidate["fleets"][0] for candidate in candidates],
    }
    try:
        for path, candidate in zip(manifests, candidates, strict=True):
            for receipt in candidate["artifacts"]:
                target = output / receipt["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path.parent / receipt["path"], target)
                if owner.soak.file_receipt(target, output) != receipt:
                    raise ValueError("fleet artifact changed while combining")
        result["artifacts"] = owner.soak.collect_artifacts(output, output)
        owner.soak.atomic_write(
            output / "evidence.json", owner.soak.canonical_bytes(result)
        )
        verify_full_bundle(output / "evidence.json")
    except BaseException:
        result["status"] = "failed"
        owner.soak.atomic_write(
            output / "failure.json", owner.soak.canonical_bytes(result)
        )
        if (output / "evidence.json").exists():
            owner.soak.atomic_write(
                output / "evidence.json", owner.soak.canonical_bytes(result)
            )
        raise
    print(f"verified complete parallel control-HA matrix: {output / 'evidence.json'}")


class FullFleet(api.APIFleet):
    def create_and_replay(self) -> None:
        super().create_and_replay()
        self.catalog_leader_unknown()
        bound = self.generated_clients("bind-operations")
        validate_lookup_proof(
            bound, owner.soak.load_json(self.artifact_dir / "leader-lookups.json")
        )
        self.proofs["prepare"] = bound
        owner.soak.atomic_write(
            self.artifact_dir / "api-prepare.json", owner.soak.canonical_bytes(bound)
        )
        self.result["api"]["operation_count"] = len(bound["operations"])
        leader = self.result["catalog_leader_unknown"]
        leader["lookups_bound_into_generated_client_proof"] = True
        owner.soak.atomic_write(
            self.artifact_dir / "leader-unknown.json",
            owner.soak.canonical_bytes(leader),
        )

    def catalog_leader_unknown(self) -> None:
        bodies = []
        for index, resource in enumerate(self.http_resources):
            body = owner.managed_request(resource)
            body["request_token"] = f"ha-leader-unknown-{self.count}-{index}"
            body["expected_generation"] = 1
            body["resource"]["governance"]["tags"]["ha_fault"] = (
                "catalog_leader_unknown"
            )
            bodies.append(body)
        owner.soak.atomic_write(
            self.artifact_dir / "leader-requests.json",
            owner.soak.canonical_bytes(bodies),
        )
        plan = {
            body["request_token"]: (
                lambda request, controller=controller: controller.request(
                    "PUT", "/v1/resources", request
                )
            )
            for body, controller in zip(bodies, self.controllers, strict=True)
        }
        old = 0
        result: dict[str, Any] = {
            "schema": faults.LEADER_SCHEMA,
            "controller_count": self.count,
            "requests": [],
        }
        # The proxy is outside the product. It cannot fabricate a success,
        # alter proposal bytes, or supply an acknowledgement to a caller.
        with faults.DropCommittedResponses(
            plan, f"Bearer {owner.regional.ADMIN_TOKEN}"
        ) as proxy:
            with ThreadPoolExecutor(max_workers=self.count) as executor:
                calls = [
                    executor.submit(
                        faults.request_unknown,
                        proxy.url,
                        body,
                        f"Bearer {owner.regional.ADMIN_TOKEN}",
                    )
                    for body in bodies
                ]
                try:
                    assert proxy.await_committed(timeout=45), (
                        "upstream control requests did not finish"
                    )
                    assert len(proxy.records) == self.count and all(
                        record.get("upstream_status") in (200, 201)
                        and record.get("generation") == 2
                        for record in proxy.records
                    ), proxy.records
                    assert proxy.pending_callers == self.count and not any(
                        call.done() for call in calls
                    ), "caller requests were not in flight at fault injection"
                    old = self.leader()
                    identifier = self.cluster.compose(
                        "ps", "--all", "--quiet", self.cluster.service(old)
                    ).stdout.strip()
                    result.update(
                        old_catalog_leader=old,
                        killed_container=identifier,
                        pending_callers_at_kill=proxy.pending_callers,
                    )
                    self.stop_nodes((old,))
                    assert not any(call.done() for call in calls), (
                        "caller completed before leader stop"
                    )
                    result["old_leader_stopped_before_response_drop"] = True
                finally:
                    # Always release explicitly owned sockets before pool
                    # shutdown, including an assertion/interrupt path.
                    proxy.drop_responses()
                outcomes = [call.result(timeout=10) for call in calls]
            observed = {record["request_token"]: record for record in proxy.records}
            result["requests"] = [
                {**observed[body["request_token"]], **outcome}
                for body, outcome in zip(bodies, outcomes, strict=True)
            ]
        assert all(
            request["caller_unknown"] is True for request in result["requests"]
        ), result
        result["new_catalog_leader"] = self.leader()
        assert result["new_catalog_leader"] != old, result
        references = []
        for body, request in zip(bodies, result["requests"], strict=True):
            for controller in self.controllers:
                replay = controller.request("PUT", "/v1/resources", body)
                assert (
                    replay.status == 200
                    and replay.document.get("replayed") is True
                    and replay.document["resource"]["generation"] == 2
                ), replay
            request.update(
                replay_status=replay.status,
                replayed=True,
                recovered_generation=replay.document["resource"]["generation"],
            )
            name = {
                key: body["resource"][key]
                for key in (
                    "organization",
                    "project",
                    "environment",
                    "namespace",
                    "name",
                )
            }
            name["kind"] = "RESOURCE_KIND_CACHE"
            references.append(
                {"request_token": body["request_token"], "affected_resources": [name]}
            )
        faults.validate_leader_evidence(result)
        self.result["catalog_leader_unknown"] = result
        lookup_plan = {"schema": LOOKUP_SCHEMA, "requests": references}
        validate_lookup_plan(lookup_plan, bodies)
        owner.soak.atomic_write(
            self.artifact_dir / "leader-lookups.json",
            owner.soak.canonical_bytes(lookup_plan),
        )
        # Restore only the voter killed by this case before the independent
        # active-controller and majority-loss faults start.
        self.cluster.start_node(old)
        owner.regional.wait_for_nodes(self.cluster)
        self.all_desired()
        print(
            f"{self.count} controllers: Catalog leader killed with pending callers; exact unknown tokens resolved",
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--output", type=Path, required=True)
    fleet = commands.add_parser("run-fleet")
    fleet.add_argument("--controllers", type=int, choices=(3, 5), required=True)
    fleet.add_argument("--output", type=Path, required=True)
    verify_fleet = commands.add_parser("verify-fleet")
    verify_fleet.add_argument("--controllers", type=int, choices=(3, 5), required=True)
    verify_fleet.add_argument("--manifest", type=Path, required=True)
    combine = commands.add_parser("combine")
    combine.add_argument("--three", type=Path, required=True)
    combine.add_argument("--five", type=Path, required=True)
    combine.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    options = parser.parse_args()
    if options.command == "run":
        owner.run_campaign(
            options.output,
            fleet_type=FullFleet,
            schema=FULL_SCHEMA,
            validator=validate_full_evidence,
            bundle_verifier=verify_full_bundle,
        )
    elif options.command == "run-fleet":
        owner.run_campaign(
            options.output,
            fleet_type=FullFleet,
            schema=FLEET_SCHEMA,
            validator=lambda evidence: validate_fleet_evidence(
                evidence, options.controllers
            ),
            bundle_verifier=lambda path: verify_fleet_bundle(path, options.controllers),
            controller_counts=(options.controllers,),
        )
    elif options.command == "verify-fleet":
        verify_fleet_bundle(options.manifest, options.controllers)
        print(f"verified isolated control-HA fleet: {options.manifest}")
    elif options.command == "combine":
        combine_fleet_bundles([options.three, options.five], options.output)
    else:
        verify_full_bundle(options.manifest)
        print(f"verified complete bounded control-HA matrix: {options.manifest}")


if __name__ == "__main__":
    main()
