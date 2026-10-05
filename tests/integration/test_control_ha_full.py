"""The complete bounded concurrent HA matrix cannot omit leader uncertainty."""

from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import control_ha_full
from test_control_ha_api import api_evidence_fixture
from test_control_ha_faults import leader_fixture


def full_fixture() -> dict:
    evidence = api_evidence_fixture()
    evidence["schema"] = control_ha_full.FULL_SCHEMA
    for fleet in evidence["fleets"]:
        fleet["catalog_leader_unknown"] = leader_fixture(fleet["controller_count"])
        fleet["catalog_leader_unknown"]["lookups_bound_into_generated_client_proof"] = (
            True
        )
    return evidence


def full_bundle_fixture(root: Path) -> Path:
    """Write the real artifact shapes; do not mock the bundle readers."""
    soak = control_ha_full.owner.soak
    evidence = full_fixture()
    evidence["identity"] = {
        "source": {"worktree_clean": True, "git_revision": "a" * 40},
        "node_source_matches_image": True,
    }
    for fleet in evidence["fleets"]:
        directory = root / f"controllers-{fleet['controller_count']}"
        directory.mkdir()
        bodies, references, lookups = [], [], []
        for request in fleet["catalog_leader_unknown"]["requests"]:
            resource = {
                "organization": "acme",
                "project": "shop",
                "environment": "dev",
                "namespace": "core",
                "kind": "cache",
                "name": request["request_token"],
            }
            body = {"request_token": request["request_token"], "resource": resource}
            bodies.append(body)
            request["command_sha256"] = (
                "sha256:"
                + hashlib.sha256(soak.canonical_bytes(body).rstrip(b"\n")).hexdigest()
            )
            references.append(
                {
                    "request_token": request["request_token"],
                    "affected_resources": [{**resource, "kind": "RESOURCE_KIND_CACHE"}],
                }
            )
            lookups.append(
                {
                    "kind": "lookup",
                    "grpc_code": 0,
                    "request_proto": "CgFh",
                    "operation_proto": "CgFi",
                }
            )
        plan = {"schema": control_ha_full.LOOKUP_SCHEMA, "requests": references}
        for name, document in (
            ("leader-requests.json", bodies),
            ("leader-lookups.json", plan),
            ("leader-unknown.json", fleet["catalog_leader_unknown"]),
        ):
            soak.atomic_write(directory / name, soak.canonical_bytes(document))
        for phase in control_ha_full.api.API_PHASES:
            checks = {check: True for check in control_ha_full.api.GRPC_CHECKS}
            if phase == "stale":
                checks["stale_watch_cursor_fails_closed"] = True
            proof = {
                "schema": control_ha_full.api.GRPC_SCHEMA,
                "controller_count": fleet["controller_count"],
                "checks": checks,
                "operations": [
                    *lookups,
                    *[
                        {"kind": "batch"}
                        for _ in range(fleet["api"]["operation_count"] - len(lookups))
                    ],
                ],
                "desired_proto": ["CgFh"] * fleet["api"]["desired_count"],
                "lookup_bindings": references,
                "watch_checkpoint": 100,
                "watch_matching_cursors": [1, 2],
                "initial_batch": {
                    "request_proto": "CgFh",
                    "response_proto": ["CgFi"] * fleet["controller_count"],
                    "operation_proto": ["CgFj"] * fleet["controller_count"],
                    "changes_proto": "CgFk",
                },
            }
            soak.atomic_write(
                directory / f"api-{phase}.json", soak.canonical_bytes(proof)
            )
    evidence["artifacts"] = soak.collect_artifacts(root, root)
    manifest = root / "evidence.json"
    soak.atomic_write(manifest, soak.canonical_bytes(evidence))
    return manifest


class FullEvidenceTest(unittest.TestCase):
    def test_v2_bundle_requires_initial_witnesses_even_when_all_receipts_match(
        self,
    ) -> None:
        self._rewrite_initial_witnesses(legacy=False)

    def test_legacy_bundle_remains_readable_with_its_original_contract(self) -> None:
        self._rewrite_initial_witnesses(legacy=True)

    def _rewrite_initial_witnesses(self, *, legacy: bool) -> None:
        soak = control_ha_full.owner.soak
        with tempfile.TemporaryDirectory(prefix="epoch-full-bundle-test-") as folder:
            root = Path(folder)
            manifest = full_bundle_fixture(root)
            for count in (3, 5):
                for phase in control_ha_full.api.API_PHASES:
                    path = root / f"controllers-{count}/api-{phase}.json"
                    proof = soak.load_json(path)
                    proof.pop("initial_batch")
                    if legacy:
                        proof["schema"] = control_ha_full.api.LEGACY_GRPC_SCHEMA
                    soak.atomic_write(path, soak.canonical_bytes(proof))
            evidence = soak.load_json(manifest)
            evidence["artifacts"] = [
                soak.file_receipt(root / receipt["path"], root)
                for receipt in evidence["artifacts"]
            ]
            soak.atomic_write(manifest, soak.canonical_bytes(evidence))
            if legacy:
                control_ha_full.verify_full_bundle(manifest)
            else:
                with self.assertRaises(ValueError):
                    control_ha_full.verify_full_bundle(manifest)

    def test_full_bundle_reads_checksum_bound_original_request_arrays(self) -> None:
        with tempfile.TemporaryDirectory(prefix="epoch-full-bundle-test-") as folder:
            control_ha_full.verify_full_bundle(full_bundle_fixture(Path(folder)))

    def test_full_bundle_rejects_bad_request_arrays_with_valid_receipts(self) -> None:
        soak = control_ha_full.owner.soak
        for invalid in ("object", "empty", "scalar", "duplicate", "scope", "count"):
            with (
                self.subTest(invalid=invalid),
                tempfile.TemporaryDirectory(prefix="epoch-full-bundle-test-") as folder,
            ):
                root = Path(folder)
                manifest = full_bundle_fixture(root)
                path = root / "controllers-3/leader-requests.json"
                bodies = soak.load_json_array(path)
                if invalid == "object":
                    changed = {"requests": bodies}
                elif invalid == "empty":
                    changed = []
                elif invalid == "scalar":
                    changed = [None, *bodies[1:]]
                elif invalid == "duplicate":
                    changed = [bodies[0], bodies[0], bodies[2]]
                elif invalid == "count":
                    changed = bodies[:2]
                else:
                    bodies[0]["resource"]["organization"] = "otherco"
                    changed = bodies
                soak.atomic_write(path, soak.canonical_bytes(changed))
                document = soak.load_json(manifest)
                receipt = soak.file_receipt(path, root)
                document["artifacts"] = [
                    receipt if original["path"] == receipt["path"] else original
                    for original in document["artifacts"]
                ]
                soak.atomic_write(manifest, soak.canonical_bytes(document))
                with self.assertRaises((ValueError, soak.EvidenceError)):
                    control_ha_full.verify_full_bundle(manifest)

    def test_full_bundle_rejects_original_request_artifact_tampering(self) -> None:
        soak = control_ha_full.owner.soak
        with tempfile.TemporaryDirectory(prefix="epoch-full-bundle-test-") as folder:
            root = Path(folder)
            manifest = full_bundle_fixture(root)
            path = root / "controllers-3/leader-requests.json"
            soak.atomic_write(path, path.read_bytes() + b"\n")
            with self.assertRaisesRegex(ValueError, "artifact checksum mismatch"):
                control_ha_full.verify_full_bundle(manifest)

    def test_initial_batch_witnesses_cannot_change_or_disappear_after_recovery(
        self,
    ) -> None:
        soak = control_ha_full.owner.soak
        for field in (
            "request_proto",
            "response_proto",
            "operation_proto",
            "changes_proto",
            None,
        ):
            with (
                self.subTest(field=field),
                tempfile.TemporaryDirectory(prefix="epoch-full-bundle-test-") as folder,
            ):
                root = Path(folder)
                manifest = full_bundle_fixture(root)
                path = root / "controllers-5/api-after-owner.json"
                proof = soak.load_json(path)
                if field is None:
                    proof.pop("initial_batch")
                else:
                    proof["initial_batch"][field] = "altered"
                soak.atomic_write(path, soak.canonical_bytes(proof))
                document = soak.load_json(manifest)
                replacement = soak.file_receipt(path, root)
                document["artifacts"] = [
                    replacement if original["path"] == replacement["path"] else original
                    for original in document["artifacts"]
                ]
                soak.atomic_write(manifest, soak.canonical_bytes(document))
                with self.assertRaises(ValueError):
                    control_ha_full.verify_full_bundle(manifest)

    def test_full_cli_runs_final_verification_inside_campaign(self) -> None:
        with (
            mock.patch("sys.argv", ["control_ha_full.py", "run", "--output", "/proof"]),
            mock.patch.object(control_ha_full.owner, "run_campaign") as campaign,
            mock.patch.object(control_ha_full, "verify_full_bundle") as verifier,
        ):
            control_ha_full.main()
        self.assertIs(verifier, campaign.call_args.kwargs["bundle_verifier"])
        verifier.assert_not_called()

    def test_lookup_plan_binds_original_tokens_and_exact_qualified_names(self) -> None:
        body = {
            "request_token": "original",
            "resource": {
                "organization": "acme",
                "project": "shop",
                "environment": "dev",
                "namespace": "core",
                "kind": "cache",
                "name": "writer-0",
            },
        }
        plan = {
            "schema": "epoch.control-ha.operation-bindings/v1",
            "requests": [
                {
                    "request_token": "original",
                    "affected_resources": [
                        {
                            **{
                                key: value
                                for key, value in body["resource"].items()
                                if key != "kind"
                            },
                            "kind": "RESOURCE_KIND_CACHE",
                        }
                    ],
                }
            ],
        }
        control_ha_full.validate_lookup_plan(plan, [body])
        for mutate in (
            lambda value: value.update(schema="wrong-schema"),
            lambda value: value["requests"][0].update(request_token="wrong"),
            lambda value: value["requests"][0]["affected_resources"][0].update(
                organization="otherco"
            ),
            lambda value: value["requests"].append(value["requests"][0]),
        ):
            broken = copy.deepcopy(plan)
            mutate(broken)
            with self.assertRaises(ValueError):
                control_ha_full.validate_lookup_plan(broken, [body])

        for bodies in (None, [], [None], [body, body], [{"request_token": "original"}]):
            with self.subTest(bodies=bodies), self.assertRaises(ValueError):
                control_ha_full.validate_lookup_plan(plan, bodies)

    def test_generated_proof_must_bind_every_lookup_without_dropped_proto_bytes(
        self,
    ) -> None:
        plan = {"requests": [{"request_token": "original"}]}
        proof = {
            "lookup_bindings": copy.deepcopy(plan["requests"]),
            "operations": [
                {"kind": "batch"},
                {
                    "kind": "lookup",
                    "grpc_code": 0,
                    "request_proto": "CgFh",
                    "operation_proto": "CgFi",
                },
            ],
        }
        control_ha_full.validate_lookup_proof(proof, plan)
        for mutate in (
            lambda value: value.pop("lookup_bindings"),
            lambda value: value["lookup_bindings"][0].update(request_token="wrong"),
            lambda value: value["operations"].pop(),
            lambda value: value["operations"][1].update(request_proto=""),
            lambda value: value["operations"][1].update(operation_proto="not-base64"),
            lambda value: value["operations"][1].update(grpc_code=True),
            lambda value: value["operations"].append(value["operations"][1]),
        ):
            broken = copy.deepcopy(proof)
            mutate(broken)
            with self.assertRaises(ValueError):
                control_ha_full.validate_lookup_proof(broken, plan)

    def test_both_api_and_real_leader_unknown_matrix_are_required(self) -> None:
        control_ha_full.validate_full_evidence(full_fixture())
        evidence = api_evidence_fixture()
        evidence["schema"] = control_ha_full.FULL_SCHEMA
        with self.assertRaises(ValueError):
            control_ha_full.validate_full_evidence(evidence)
        for value in (None, False, 1):
            evidence = full_fixture()
            evidence["fleets"][0]["catalog_leader_unknown"][
                "lookups_bound_into_generated_client_proof"
            ] = value
            with self.assertRaises(ValueError):
                control_ha_full.validate_full_evidence(evidence)

    def test_leader_case_must_match_the_actual_controller_fleet(self) -> None:
        evidence = full_fixture()
        evidence["fleets"][0]["catalog_leader_unknown"] = leader_fixture(5)
        with self.assertRaises(ValueError):
            control_ha_full.validate_full_evidence(evidence)


if __name__ == "__main__":
    unittest.main()
