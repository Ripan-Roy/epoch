"""Independent checksum-bound verifier for complete public-SDK Catalog evidence."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import management_sdk_catalog as campaign

owner, full, messages = campaign.owner, campaign.full, campaign.messages


def verify_fleet_bundle(path: Path, count: int) -> None:
    if type(count) is not int or count not in (3, 5):
        raise ValueError("explicit three/five-controller SDK fleet required")
    verify_bundle(path, (count,))
    evidence = owner.soak.load_json(path)
    image_id = evidence.get("identity", {}).get("runtime", {}).get("image_id", "")
    if (
        not isinstance(image_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
    ):
        raise ValueError("SDK fleet requires the tested immutable image identity")
    if any(
        not receipt["path"].startswith(f"controllers-{count}/")
        for receipt in evidence["artifacts"]
    ):
        raise ValueError("SDK fleet inventory contains another campaign")


def _validate_matching_runtimes(candidates: list[dict[str, Any]]) -> None:
    _validate_matching_sdk_runtimes(
        [candidate["fleets"][0] for candidate in candidates]
    )


def _validate_matching_sdk_runtimes(fleets: list[dict[str, Any]]) -> None:
    if any(
        fleet["sdk"]["runtime"] != fleets[0]["sdk"]["runtime"] for fleet in fleets[1:]
    ):
        raise ValueError("fleet public SDK runtime identities differ")


def combine_fleet_bundles(manifests: list[Path], output: Path) -> None:
    """Reuse receipt-safe sealing while requiring SDK and native fault proofs."""
    full.combine_fleet_bundles(
        manifests,
        output,
        schema=campaign.SCHEMA,
        fleet_verifier=verify_fleet_bundle,
        bundle_verifier=verify_bundle,
        candidates_validator=_validate_matching_runtimes,
    )


def validate_evidence(
    evidence: dict[str, Any], counts: tuple[int, ...] = (3, 5)
) -> None:
    if counts not in ((3,), (5,), (3, 5)):
        raise ValueError("explicit bounded public SDK campaign scope required")
    schema = campaign.SCHEMA if counts == (3, 5) else campaign.FLEET_SCHEMA
    if evidence.get("schema") != schema:
        raise ValueError("wrong public SDK Catalog certification scope")
    full.validate_full_evidence(
        {**evidence, "schema": full.FULL_SCHEMA}, controller_counts=counts
    )
    identity = evidence.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("SDK certification requires immutable image identity")
    source, image = identity.get("source"), identity.get("runtime")
    if not isinstance(source, dict) or not isinstance(image, dict):
        raise ValueError("SDK certification requires frozen source/image revisions")
    image_id = image.get("image_id")
    if (
        not isinstance(image_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
    ):
        raise ValueError("SDK certification requires immutable image identity")
    for revision in (source.get("git_revision"), image.get("image_revision")):
        if (
            not isinstance(revision, str)
            or re.fullmatch(r"[0-9a-f]{40}", revision) is None
        ):
            raise ValueError("SDK certification requires frozen source/image revisions")
    for fleet in evidence["fleets"]:
        sdk = fleet.get("sdk")
        if (
            not isinstance(sdk, dict)
            or sdk.get("languages") != list(campaign.LANGUAGES)
            or not isinstance(sdk.get("phases"), list)
            or not isinstance(sdk.get("faults"), dict)
        ):
            raise ValueError(
                "SDK certification requires every language and complete fault phase"
            )
        runtime = sdk.get("runtime", {})
        for field in ("sdk-go", "sdk-proxy", "java_classes", "java_probe"):
            if (
                re.fullmatch(r"sha256:[0-9a-f]{64}", str(runtime.get(field, "")))
                is None
            ):
                raise ValueError(
                    "SDK certification requires immutable runtime identities"
                )
        if (
            not isinstance(runtime.get("java_dependencies"), list)
            or not runtime["java_dependencies"]
            or any(
                re.fullmatch(r"sha256:[0-9a-f]{64}", str(dependency.get("sha256", "")))
                is None
                for dependency in runtime["java_dependencies"]
            )
        ):
            raise ValueError(
                "SDK certification omitted actual Java runtime dependencies"
            )
        expected_phases = []
        count = fleet["controller_count"]
        for phase in campaign.PHASES:
            expected_phases.append(phase)
            if phase == "resolved":
                expected_phases.append("resolved-creation")
            if phase in ("resolved", "after-owner", "after-quorum", "after-reopen"):
                live = count - 1 if phase in ("after-owner", "after-quorum") else count
                expected_phases.extend(
                    f"{phase}-controller-{index}" for index in range(live)
                )
        if sdk["phases"] != expected_phases:
            raise ValueError(
                "SDK certification omitted a live-controller original-token receipt"
            )
    _validate_matching_sdk_runtimes(evidence["fleets"])


class FleetVerifier:
    def __init__(self, root: Path, fleet: dict[str, Any], artifacts: set[str]) -> None:
        self.root, self.fleet, self.artifacts = root, fleet, artifacts
        self.count, self.sdk = fleet["controller_count"], fleet["sdk"]
        self.prefix = f"controllers-{self.count}/"

    def load(self, name: str) -> dict[str, Any]:
        if Path(name).name != name or self.prefix + name not in self.artifacts:
            raise ValueError("missing checksum-bound public SDK campaign artifact")
        return owner.soak.load_json(self.root / self.prefix / name)

    def fault_witnesses(self) -> dict[str, Any]:
        originals = self.load("sdk-workload.json")
        if originals.get("schema") != "epoch.sdk.management.original-batches/v1" or set(
            originals.get("batches", {})
        ) != set(campaign.LANGUAGES):
            raise ValueError("incomplete original SDK batch inventory")
        batches = {
            language: campaign.decoded(
                originals["batches"][language], messages.BatchApplyResourcesRequest
            )
            for language in campaign.LANGUAGES
        }
        if any(
            batch != campaign.original_batch(self.count, language)
            for language, batch in batches.items()
        ):
            raise ValueError(
                "SDK workload changed scope, token, spec, or explicit-zero OCC"
            )
        fault = self.load("sdk-catalog-leader.json")
        if fault != self.sdk["faults"].get("catalog_leader"):
            raise ValueError("SDK Catalog fault artifact differs from its manifest")
        campaign.validate_catalog_fault(fault, batches)
        if self.load("sdk-held-upstream.json") != fault["held"]:
            raise ValueError(
                "original upstream receipts changed across fault injection"
            )
        proxy = self.load("sdk-proxy-plan.json")
        bindings = {
            binding["request_proto"]: binding["upstream_authority"]
            for binding in proxy.get("bindings", [])
        }
        if (
            proxy.get("schema") != "epoch.sdk.management.lost-ack-plan/v1"
            or len(bindings) != 3
            or any(
                bindings.get(receipt["request_proto"]) != receipt["upstream_authority"]
                for receipt in fault["held"]["receipts"]
            )
        ):
            raise ValueError("held receipts differ from original proxy bindings")
        owner_fault = self.load("sdk-controller-owner.json")
        transitions = [
            transition
            for transition in self.fleet["lease_transitions"]
            if transition["fault"] == "owner_sigkill"
        ]
        if (
            owner_fault != self.sdk["faults"].get("controller_owner")
            or len(transitions) != 1
            or any(
                owner_fault.get(field) != value
                for field, value in transitions[0].items()
            )
            or owner_fault.get("pending_watchers_at_kill") != 3
        ):
            raise ValueError(
                "public watches did not witness actual controller-owner SIGKILL"
            )
        retention = self.load("sdk-retention.json")
        if (
            retention != self.sdk.get("retention")
            or set(retention.get("expired_checkpoints", {})) != set(campaign.LANGUAGES)
            or type(retention.get("earliest_cursor")) is not int
            or type(retention.get("latest_cursor")) is not int
            or any(
                type(cursor) is not int
                or not 0
                < cursor + 1
                < retention["earliest_cursor"]
                <= retention["latest_cursor"]
                for cursor in retention["expired_checkpoints"].values()
            )
        ):
            raise ValueError(
                "real retained history did not expire each durable SDK checkpoint"
            )
        return batches

    def recovery(
        self,
        language: str,
        batch: Any,
        workload: dict[str, Any],
        proof: dict[str, Any],
        retained: str | None,
    ) -> str:
        phase = workload["phase"]
        lookup = messages.GetOperationRequest(
            request_token=batch.request_token,
            affected_resources=[item.name for item in batch.resources],
        )
        live = (
            self.count - 1 if phase in ("after-owner", "after-quorum") else self.count
        )
        actions = workload["actions"]
        if (
            len(workload["endpoints"]) != live
            or len(actions) != 5
            or actions[0] != campaign.action("GetOperation", lookup)
            or actions[1] != campaign.action("BatchApplyResources", batch)
        ):
            raise ValueError("SDK recovery omitted original-token resolution/replay")
        operation = campaign.decoded(
            proof["actions"][0].get("response_proto"), messages.GetOperationResponse
        )
        campaign.validate_operation(batch, operation)
        current = campaign.encoded(operation)
        if retained is not None and current != retained:
            raise ValueError(
                "complete original SDK operation changed across fault recovery"
            )
        replay = campaign.decoded(
            proof["actions"][1].get("response_proto"),
            messages.BatchApplyResourcesResponse,
        )
        if not replay.replayed or len(replay.results) != 2:
            raise ValueError("SDK did not replay its already committed original batch")
        for index, original in enumerate(batch.resources):
            if actions[index + 2] != campaign.action(
                "GetResource", messages.GetResourceRequest(name=original.name)
            ):
                raise ValueError("SDK desired-state lookup changed original scope")
            resource = campaign.decoded(
                proof["actions"][index + 2].get("response_proto"),
                messages.GetResourceResponse,
            ).resource
            replayed = replay.results[index].resource
            if any(
                item.name != original.name
                or item.spec != original.spec
                or item.generation != 1
                for item in (resource, replayed)
            ):
                raise ValueError(
                    f"{language} desired state changed across Catalog recovery"
                )
        return current

    def watch(
        self,
        language: str,
        phase: str,
        workload: dict[str, Any],
        proof: dict[str, Any],
        checkpoint: int,
    ) -> int:
        retention = self.sdk["retention"]
        for expected, observed in zip(
            workload["actions"], proof["actions"], strict=True
        ):
            if not expected["method"].startswith("Watch"):
                continue
            after = (
                retention["expired_checkpoints"][language]
                if phase == "stale"
                else checkpoint
            )
            request = campaign.decoded(
                expected["request_proto"], messages.WatchResourceChangesRequest
            )
            if request != campaign.watch_request(after=after):
                raise ValueError(
                    "public watch changed filters or reset application checkpoint"
                )
            if phase == "stale":
                if (
                    expected["expected_code"] != campaign.STALE_WATCH_CODE
                    or observed.get("grpc_code") != campaign.STALE_WATCH_CODE
                    or observed.get("pages_proto")
                    or Path(expected["checkpoint_path"]).name
                    in {Path(artifact).name for artifact in self.artifacts}
                ):
                    raise ValueError(
                        "expired checkpoint was reset, acknowledged, or rewritten"
                    )
                continue
            checkpoint = int(observed["checkpoint"])
            saved = self.load(Path(expected["checkpoint_path"]).name)
            if saved != {
                "schema": "epoch.sdk.management.checkpoint/v1",
                "language": language,
                "next_cursor": str(checkpoint),
            }:
                raise ValueError(
                    "application checkpoint differs from acknowledged scanned page"
                )
            if (
                phase == "after-quorum"
                and checkpoint != retention["expired_checkpoints"][language]
            ):
                raise ValueError(
                    "stale test did not expire a previously durable application checkpoint"
                )
            if phase == "owner-watch":
                ready = self.load(Path(expected["ready_path"]).name)
                if (
                    workload["endpoints"][0]
                    != self.sdk["faults"]["controller_owner"]["first_endpoint"]
                    or ready.get("pages_proto") != observed["pages_proto"][:1]
                    or int(ready.get("checkpoint", "-1")) > checkpoint
                    or ready.get("attempts") != 1
                ):
                    raise ValueError(
                        "watch readiness did not precede actual owner failure"
                    )
                self.load(Path(expected["release_path"]).name)
        return checkpoint

    def language(self, language: str, batch: Any) -> None:
        lookup = messages.GetOperationRequest(
            request_token=batch.request_token,
            affected_resources=[item.name for item in batch.resources],
        )
        retained, checkpoint = None, 0
        phases = [*self.sdk["phases"]]
        phases.insert(phases.index("after-owner"), "owner-watch")
        endpoint_sets = {}
        endpoint_witnesses: dict[str, set[str]] = {}
        for phase in phases:
            workload = self.load(f"sdk-{phase}-{language}-plan.json")
            proof = self.load(f"sdk-{phase}-{language}-proof.json")
            if (
                workload.get("schema") != campaign.PLAN_SCHEMA
                or workload.get("phase") != phase
                or workload
                != campaign.plan(phase, workload["endpoints"], workload["actions"])
            ):
                raise ValueError(
                    "SDK plan differs from bounded owned loopback workload"
                )
            campaign.validate_probe(language, workload, proof)
            if phase == "lost-ack" and (
                workload["actions"]
                != [
                    campaign.action("BatchApplyResources", batch, code=14, unknown=True)
                ]
                or len(workload["endpoints"]) != 1
            ):
                raise ValueError(
                    "lost-ack caller changed original single-attempt batch"
                )
            if phase in ("resolved", "after-owner", "after-quorum", "after-reopen"):
                retained = self.recovery(language, batch, workload, proof, retained)
                endpoint_sets[phase] = set(workload["endpoints"])
                endpoint_witnesses[phase] = set()
            if phase == "resolved-creation":
                operation = campaign.decoded(retained, messages.GetOperationResponse)
                request = campaign.watch_request(
                    after=operation.first_change_cursor - 1
                )
                request.batch_size = 2
                actions = workload["actions"]
                if (
                    len(actions) != 1
                    or actions[0]["method"] != "WatchResourceChanges"
                    or actions[0]["request_proto"] != campaign.encoded(request)
                ):
                    raise ValueError(
                        "SDK initial creation witness changed the exact batch cursor"
                    )
                observed = proof["actions"][0]
                campaign.validate_creation_page(
                    batch,
                    operation,
                    campaign.decoded(
                        observed["pages_proto"][0],
                        messages.WatchResourceChangesResponse,
                    ),
                )
                saved = self.load(Path(actions[0]["checkpoint_path"]).name)
                if saved != {
                    "schema": "epoch.sdk.management.checkpoint/v1",
                    "language": language,
                    "next_cursor": observed["checkpoint"],
                }:
                    raise ValueError(
                        "initial SDK creation history was not durably acknowledged"
                    )
                continue
            if "-controller-" in phase:
                parent = phase.split("-controller-", 1)[0]
                if (
                    workload["actions"] != [campaign.action("GetOperation", lookup)]
                    or len(workload["endpoints"]) != 1
                    or campaign.operation_witness(
                        proof["actions"][0].get("response_proto")
                    )
                    != retained
                ):
                    raise ValueError(
                        "a live controller lost the complete original-token SDK outcome"
                    )
                endpoint = workload["endpoints"][0]
                if endpoint in endpoint_witnesses[parent]:
                    raise ValueError(
                        "SDK evidence repeated one controller instead of testing each"
                    )
                endpoint_witnesses[parent].add(endpoint)
            checkpoint = self.watch(language, phase, workload, proof, checkpoint)
            if phase == "prepare":
                methods = {item["method"] for item in workload["actions"]}
                if (
                    not {
                        "ApplyResource",
                        "GetResource",
                        "ListResources",
                        "DeleteResource",
                        "GetOperation",
                        "WatchResourceChanges",
                    }
                    <= methods
                ):
                    raise ValueError(
                        "SDK preparation omitted the seven-method public API"
                    )
        if endpoint_sets != endpoint_witnesses:
            raise ValueError(
                "original-token public SDK reads did not cover every live controller"
            )


def verify_bundle(path: Path, counts: tuple[int, ...] = (3, 5)) -> None:
    full.verify_full_bundle(
        path, validator=lambda evidence: validate_evidence(evidence, counts)
    )
    evidence = owner.soak.load_json(path)
    artifacts = {receipt["path"] for receipt in evidence["artifacts"]}
    for fleet in evidence["fleets"]:
        verifier = FleetVerifier(path.parent, fleet, artifacts)
        batches = verifier.fault_witnesses()
        for language in campaign.LANGUAGES:
            verifier.language(language, batches[language])
    print(f"verified complete public SDK Catalog fault proof: {path}")
