"""Public SDK Catalog certification must retain real byte-bound fault evidence."""

from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import management_sdk_catalog as catalog
import management_sdk_catalog_evidence as evidence
from epoch_sdk._generated.epoch.v1 import regional_admin_pb2 as messages


def sdk_evidence_fixture(document=None, counts=(3, 5)):
    from test_control_ha_full import full_fixture

    document = copy.deepcopy(document if document is not None else full_fixture())
    document["schema"] = catalog.SCHEMA if counts == (3, 5) else catalog.FLEET_SCHEMA
    document["identity"] = {
        "source": {"worktree_clean": True, "git_revision": "a" * 40},
        "runtime": {
            "image_id": "sha256:" + "b" * 64,
            "image_revision": "c" * 40,
        },
        "node_source_matches_image": True,
    }
    document["fleets"] = [
        fleet for fleet in document["fleets"] if fleet["controller_count"] in counts
    ]
    for fleet in document["fleets"]:
        phases = []
        count = fleet["controller_count"]
        for phase in catalog.PHASES:
            phases.append(phase)
            if phase == "resolved":
                phases.append("resolved-creation")
            if phase in ("resolved", "after-owner", "after-quorum", "after-reopen"):
                live = count - 1 if phase in ("after-owner", "after-quorum") else count
                phases.extend(f"{phase}-controller-{index}" for index in range(live))
        fleet["sdk"] = {
            "languages": list(catalog.LANGUAGES),
            "phases": phases,
            "faults": {},
            "runtime": {
                **{
                    field: "sha256:" + "d" * 64
                    for field in ("sdk-go", "sdk-proxy", "java_classes", "java_probe")
                },
                "java_dependencies": [
                    {"file": "runtime.jar", "sha256": "sha256:" + "e" * 64}
                ],
            },
        }
    return document


class CatalogSDKWitnessTest(unittest.TestCase):
    def test_full_and_isolated_validation_requires_immutable_image_identity(self):
        for counts in ((3,), (5,), (3, 5)):
            document = sdk_evidence_fixture(counts=counts)
            evidence.validate_evidence(document, counts)
            for image_id in (None, True, "mutable-image-tag", "sha256:" + "b" * 63):
                changed = copy.deepcopy(document)
                changed["identity"]["runtime"]["image_id"] = image_id
                with (
                    self.subTest(counts=counts, image_id=image_id),
                    self.assertRaisesRegex(ValueError, "immutable image identity"),
                ):
                    evidence.validate_evidence(changed, counts)

    def test_full_and_isolated_validation_requires_frozen_revision_syntax(self):
        for counts in ((3,), (5,), (3, 5)):
            document = sdk_evidence_fixture(counts=counts)
            for section, field in (
                ("source", "git_revision"),
                ("runtime", "image_revision"),
            ):
                for revision in (None, True, "main", "a" * 39, "A" * 40):
                    changed = copy.deepcopy(document)
                    changed["identity"][section][field] = revision
                    with (
                        self.subTest(counts=counts, field=field, revision=revision),
                        self.assertRaisesRegex(
                            ValueError, "frozen source/image revision"
                        ),
                    ):
                        evidence.validate_evidence(changed, counts)

    def test_full_matrix_validation_rejects_divergent_public_sdk_runtimes(self):
        document = sdk_evidence_fixture()
        evidence.validate_evidence(document)
        runtime = document["fleets"][1]["sdk"]["runtime"]
        for field in runtime:
            changed = copy.deepcopy(document)
            changed_runtime = changed["fleets"][1]["sdk"]["runtime"]
            if field == "java_dependencies":
                changed_runtime[field][0]["sha256"] = "sha256:" + "f" * 64
            else:
                changed_runtime[field] = "sha256:" + "f" * 64
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValueError, "SDK runtime identities differ"),
            ):
                evidence.validate_evidence(changed)

    def test_historical_identity_does_not_require_current_checkout_or_same_revision(
        self,
    ):
        for counts in ((3,), (5,), (3, 5)):
            document = sdk_evidence_fixture(counts=counts)
            with mock.patch.object(
                catalog.owner.soak,
                "source_identity",
                side_effect=AssertionError(
                    "historical verification inspected current checkout"
                ),
            ):
                evidence.validate_evidence(document, counts)

    def test_bundle_verifier_checks_immutable_identity_before_sdk_artifacts(self):
        from test_control_ha_full import full_bundle_fixture

        soak = catalog.owner.soak
        with tempfile.TemporaryDirectory() as temporary:
            manifest = full_bundle_fixture(Path(temporary))
            document = sdk_evidence_fixture(soak.load_json(manifest))
            document["identity"]["runtime"]["image_id"] = "mutable-image-tag"
            soak.atomic_write(manifest, soak.canonical_bytes(document))
            with self.assertRaisesRegex(ValueError, "immutable image identity"):
                evidence.verify_bundle(manifest)

    def test_expired_application_checkpoint_requires_the_catalog_aborted_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            fleet = object.__new__(catalog.SDKFleet)
            fleet.artifact_dir = Path(temporary)
            fleet.count = 3
            fleet.checkpoints = dict(
                zip(catalog.LANGUAGES, (373, 376, 374), strict=True)
            )
            saved = dict(fleet.checkpoints)
            fleet.resources = [None, SimpleNamespace(name="events")]
            fleet.result = {"sdk": {}}
            fleet.raw = mock.Mock(
                side_effect=[
                    SimpleNamespace(document={"generation": 1}),
                    SimpleNamespace(
                        status=200,
                        document={"earliest_cursor": 397, "latest_cursor": 4492},
                    ),
                ]
            )
            fleet.recover, fleet.execute = mock.Mock(), mock.Mock()
            with mock.patch.object(catalog.full.FullFleet, "reopen"):
                fleet.reopen()
            phase, actions = fleet.execute.call_args.args
            self.assertEqual(phase, "stale")
            for language in catalog.LANGUAGES:
                self.assertEqual(actions[language][0]["expected_code"], 10)
                request = catalog.decoded(
                    actions[language][0]["request_proto"],
                    messages.WatchResourceChangesRequest,
                )
                self.assertEqual(request.after_cursor, saved[language])
                self.assertFalse(Path(actions[language][0]["checkpoint_path"]).exists())

    def test_independent_expired_watch_verifier_accepts_only_aborted_without_ack(self):
        language, old_cursor = "go", 373
        request = catalog.watch_request(after=old_cursor)
        item = catalog.action(
            "WatchResourceChanges",
            request,
            code=10,
            checkpoint_path="/owned/stale.json",
        )
        workload = catalog.plan("stale", ["127.0.0.1:9000"], [item])
        proof = {"actions": [{"grpc_code": 10, "pages_proto": []}]}
        verifier = evidence.FleetVerifier(
            Path("/owned"),
            {
                "controller_count": 3,
                "sdk": {"retention": {"expired_checkpoints": {language: old_cursor}}},
            },
            set(),
        )
        self.assertEqual(verifier.watch(language, "stale", workload, proof, 4400), 4400)
        item["expected_code"] = 9
        with self.assertRaises(ValueError):
            verifier.watch(language, "stale", workload, proof, 4400)
        item["expected_code"] = 10
        verifier.artifacts.add("controllers-3/stale.json")
        with self.assertRaises(ValueError):
            verifier.watch(language, "stale", workload, proof, 4400)

    def test_parallel_sdk_combiner_requires_all_independent_verifiers(self):
        inputs = [Path("/owned/three/evidence.json"), Path("/owned/five/evidence.json")]
        with mock.patch.object(catalog.full, "combine_fleet_bundles") as combine:
            evidence.combine_fleet_bundles(inputs, Path("/owned/output"))
        args, options = combine.call_args
        self.assertEqual(args, (inputs, Path("/owned/output")))
        self.assertEqual(options["schema"], catalog.SCHEMA)
        self.assertIs(options["fleet_verifier"], evidence.verify_fleet_bundle)
        self.assertIs(options["bundle_verifier"], evidence.verify_bundle)
        candidates = [
            {"fleets": [{"sdk": {"runtime": {"sdk-go": "sha256:" + "a" * 64}}}]},
            {"fleets": [{"sdk": {"runtime": {"sdk-go": "sha256:" + "a" * 64}}}]},
        ]
        options["candidates_validator"](candidates)
        candidates[1]["fleets"][0]["sdk"]["runtime"]["sdk-go"] = "sha256:" + "b" * 64
        with self.assertRaisesRegex(ValueError, "SDK runtime"):
            options["candidates_validator"](candidates)

    def test_isolated_sdk_verifier_rejects_foreign_inventory_or_missing_image(self):
        # This only isolates the additional shard boundary. Semantic protobuf
        # and fault verification remains mandatory in the separately tested
        # bundle verifier and the live Catalog campaign.
        soak = catalog.owner.soak
        with tempfile.TemporaryDirectory() as temporary:
            manifest = Path(temporary) / "evidence.json"
            document = {
                "identity": {"runtime": {"image_id": "sha256:" + "a" * 64}},
                "artifacts": [{"path": "controllers-3/sdk-workload.json"}],
            }
            soak.atomic_write(manifest, soak.canonical_bytes(document))
            with mock.patch.object(evidence, "verify_bundle") as verify:
                evidence.verify_fleet_bundle(manifest, 3)
                verify.assert_called_once_with(manifest, (3,))
                for field, value in (
                    ("artifacts", [{"path": "controllers-5/sdk-workload.json"}]),
                    ("identity", {"runtime": {"image_id": "mutable-image-tag"}}),
                ):
                    invalid = copy.deepcopy(document)
                    invalid[field] = value
                    soak.atomic_write(manifest, soak.canonical_bytes(invalid))
                    with self.assertRaises(ValueError):
                        evidence.verify_fleet_bundle(manifest, 3)

    def test_combine_cli_dispatches_the_ordered_full_sdk_matrix(self):
        with (
            mock.patch.object(catalog, "combine_fleet_bundles") as combine,
            mock.patch.object(
                catalog.sys,
                "argv",
                [
                    "management_sdk_catalog.py",
                    "combine",
                    "--three",
                    "/owned/3/evidence.json",
                    "--five",
                    "/owned/5/evidence.json",
                    "--output",
                    "/owned/full",
                ],
            ),
        ):
            catalog.main()
        combine.assert_called_once_with(
            [Path("/owned/3/evidence.json"), Path("/owned/5/evidence.json")],
            Path("/owned/full"),
        )

    def test_reconstructed_first_receipt_still_requires_exact_committed_creation_history(
        self,
    ):
        request = catalog.original_batch(3, "go")
        receipt = messages.BatchApplyResourcesResponse(replayed=True)
        for item in request.resources:
            receipt.results.add(
                created=True,
                changed=True,
                replayed=True,
                resource={"name": item.name, "spec": item.spec, "generation": 1},
            )
        catalog.validate_batch_receipt(request, receipt)
        operation = messages.GetOperationResponse(
            request_token=request.request_token,
            proposal_id=41,
            state=messages.OPERATION_STATE_SUCCEEDED,
            affected_resources=[item.name for item in request.resources],
            first_change_cursor=30,
            last_change_cursor=31,
            command_kind="apply_desired",
        )
        page = messages.WatchResourceChangesResponse(
            earliest_cursor=1, latest_cursor=40, next_cursor=31
        )
        for index, item in enumerate(request.resources):
            page.changes.add(
                cursor=30 + index,
                kind=messages.RESOURCE_CHANGE_KIND_DESIRED_APPLIED,
                name=item.name,
                generation=1,
            )
        catalog.validate_creation_page(request, operation, page)
        page.changes[1].generation = 2
        with self.assertRaises(ValueError):
            catalog.validate_creation_page(request, operation, page)
        receipt.results[0].created = False
        with self.assertRaises(ValueError):
            catalog.validate_batch_receipt(request, receipt)

    def test_successful_unary_requires_an_actual_response_witness(self):
        workload = catalog.plan(
            "read",
            ["127.0.0.1:9000"],
            [
                catalog.action(
                    "GetResource",
                    messages.GetResourceRequest(
                        name=catalog.resource_name(3, "go", "left")
                    ),
                )
            ],
        )
        proof = {
            "schema": catalog.PROBE_SCHEMA,
            "language": "go",
            "phase": "read",
            "passed": True,
            "actions": [
                {
                    "method": "GetResource",
                    "request_proto": workload["actions"][0]["request_proto"],
                    "grpc_code": 0,
                    "attempts": 1,
                    "budget": 1,
                    "outcome_may_be_unknown": False,
                }
            ],
        }
        with self.assertRaisesRegex(ValueError, "protobuf witness"):
            catalog.validate_probe("go", workload, proof)

    def test_cli_never_publishes_a_pass_without_independent_sdk_verification(self):
        from unittest.mock import patch

        observed = []
        with (
            patch.object(
                catalog.owner,
                "run_campaign",
                side_effect=lambda *args, **kwargs: observed.append(kwargs),
            ),
            patch.object(
                catalog.sys,
                "argv",
                [
                    "management_sdk_catalog.py",
                    "run",
                    "--output",
                    "/tmp/owned-evidence",
                    "--controllers",
                    "3",
                ],
            ),
        ):
            catalog.main()
        self.assertEqual(observed[0]["schema"], catalog.FLEET_SCHEMA)
        self.assertEqual(observed[0]["controller_counts"], (3,))
        self.assertTrue(callable(observed[0]["bundle_verifier"]))
        with patch.object(catalog, "verify_bundle") as verify:
            observed[0]["bundle_verifier"]("manifest")
            verify.assert_called_once_with("manifest", (3,))

    def test_partial_sdk_campaign_cannot_certify_a_full_matrix(self):
        from test_control_ha_full import full_fixture

        evidence = full_fixture()
        evidence["schema"] = catalog.SCHEMA
        with self.assertRaisesRegex(ValueError, "SDK"):
            catalog.validate_evidence(evidence)

    def test_catalog_fault_requires_pending_callers_and_exact_real_upstream_receipts(
        self,
    ):
        batches = {
            language: catalog.original_batch(3, language)
            for language in catalog.LANGUAGES
        }
        held = []
        for index, request in enumerate(batches.values()):
            response = messages.BatchApplyResourcesResponse()
            for item in request.resources:
                response.results.add(
                    created=True,
                    changed=True,
                    resource={"name": item.name, "spec": item.spec, "generation": 1},
                )
            held.append(
                {
                    "request_token": request.request_token,
                    "request_proto": catalog.encoded(request),
                    "response_proto": catalog.encoded(response),
                    "upstream_authority": f"127.0.0.1:{9000 + index}",
                }
            )
        fault = {
            "old_catalog_leader": 1,
            "new_catalog_leader": 2,
            "killed_container": "a" * 64,
            "pending_callers_at_kill": 3,
            "relay_exit_status": -9,
            "old_leader_stopped_before_response_drop": True,
            "held": {
                "schema": "epoch.sdk.management.held-receipts/v1",
                "receipts": held,
            },
        }
        catalog.validate_catalog_fault(fault, batches)
        for field, value in (
            ("pending_callers_at_kill", 2),
            ("new_catalog_leader", 1),
            ("relay_exit_status", 0),
            ("old_leader_stopped_before_response_drop", False),
            ("killed_container", ""),
        ):
            changed = copy.deepcopy(fault)
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                catalog.validate_catalog_fault(changed, batches)
        changed = copy.deepcopy(fault)
        changed["held"]["receipts"][0]["request_proto"] = "CgF4"
        with self.assertRaises(ValueError):
            catalog.validate_catalog_fault(changed, batches)

    def test_original_batch_has_explicit_zero_occ_and_distinct_stable_tokens(self):
        tokens = set()
        for language in catalog.LANGUAGES:
            request = catalog.original_batch(3, language)
            self.assertNotIn(request.request_token, tokens)
            tokens.add(request.request_token)
            self.assertEqual(len(request.resources), 2)
            for item in request.resources:
                self.assertTrue(item.HasField("expected_generation"))
                self.assertEqual(item.expected_generation, 0)
                self.assertEqual(item.name.namespace, "sdk-certification")
                self.assertEqual(item.spec.governance.tags["sdk"], language)

    def test_unknown_receipt_cannot_change_original_request_or_claim_an_ack(self):
        action = catalog.action(
            "BatchApplyResources",
            catalog.original_batch(3, "go"),
            code=14,
            unknown=True,
        )
        plan = catalog.plan("lost-ack", ["127.0.0.1:9000"], [action])
        proof = {
            "schema": catalog.PROBE_SCHEMA,
            "language": "go",
            "phase": "lost-ack",
            "passed": True,
            "actions": [
                {
                    "method": action["method"],
                    "request_proto": action["request_proto"],
                    "grpc_code": 14,
                    "attempts": 1,
                    "budget": 1,
                    "outcome_may_be_unknown": True,
                }
            ],
        }
        catalog.validate_probe("go", plan, proof)
        for field, value in (
            ("request_proto", "CgF4"),
            ("outcome_may_be_unknown", False),
            ("grpc_code", 0),
            ("response_proto", "CgFh"),
            ("attempts", 0),
        ):
            changed = copy.deepcopy(proof)
            changed["actions"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                catalog.validate_probe("go", plan, changed)

    def test_checkpoint_is_scanned_monotonic_filtered_and_never_reset(self):
        request = messages.WatchResourceChangesRequest(
            after_cursor=10, batch_size=8, namespace="sdk-certification"
        )
        first = messages.WatchResourceChangesResponse(
            earliest_cursor=1, latest_cursor=20, next_cursor=15
        )
        first.changes.add(
            cursor=12,
            kind=1,
            name=catalog.resource_name(3, "go", "first"),
            generation=1,
        )
        second = messages.WatchResourceChangesResponse(
            earliest_cursor=1, latest_cursor=22, next_cursor=22
        )
        witness = {
            "pages_proto": [catalog.encoded(first), catalog.encoded(second)],
            "checkpoint": "22",
            "attempts": 2,
        }
        catalog.validate_watch(request, witness, reconnect=True)
        for mutation in (
            "foreign-filter",
            "cursor-reset",
            "invented-checkpoint",
            "no-reconnect",
        ):
            changed = copy.deepcopy(witness)
            if mutation == "foreign-filter":
                first.changes[0].name.namespace = "another-tenant"
                changed["pages_proto"][0] = catalog.encoded(first)
                first.changes[0].name.namespace = "sdk-certification"
            elif mutation == "cursor-reset":
                second.next_cursor = 9
                changed["pages_proto"][1] = catalog.encoded(second)
                second.next_cursor = 22
            elif mutation == "invented-checkpoint":
                changed["checkpoint"] = "23"
            else:
                changed["attempts"] = 1
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                catalog.validate_watch(request, changed, reconnect=True)

    def test_durable_operation_must_match_original_batch_and_full_receipt(self):
        request = catalog.original_batch(5, "python")
        receipt = messages.GetOperationResponse(
            request_token=request.request_token,
            proposal_id=41,
            state=messages.OPERATION_STATE_SUCCEEDED,
            affected_resources=[item.name for item in request.resources],
            first_change_cursor=30,
            last_change_cursor=31,
            command_kind="apply_desired",
        )
        encoded = catalog.encoded(receipt)
        catalog.validate_operation(request, receipt)
        self.assertEqual(
            catalog.operation_witness(encoded), catalog.operation_witness(encoded)
        )
        for field, value in (
            ("request_token", "new-invented-token"),
            ("state", messages.OPERATION_STATE_PENDING),
            ("proposal_id", 0),
            ("last_change_cursor", 40),
            ("command_kind", "delete_desired"),
        ):
            changed = copy.deepcopy(receipt)
            setattr(changed, field, value)
            with self.subTest(field=field), self.assertRaises(ValueError):
                catalog.validate_operation(request, changed)


if __name__ == "__main__":
    unittest.main()
