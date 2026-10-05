"""Fail-closed contracts for generated-client HA recovery evidence."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import control_ha_api
from test_control_ha import evidence_fixture


def api_evidence_fixture() -> dict:
    evidence = evidence_fixture()
    evidence["schema"] = control_ha_api.API_SCHEMA
    for fleet in evidence["fleets"]:
        fleet["api"] = {
            "checks": {check: True for check in control_ha_api.API_CHECKS},
            "phases": list(control_ha_api.API_PHASES),
            "operation_count": 18 + fleet["controller_count"],
            "desired_count": 135,
            "helper_sha256": "sha256:" + "a" * 64,
            "retention": {"earliest_cursor": 2, "latest_cursor": 4100},
        }
    return evidence


class APIEvidenceContractTest(unittest.TestCase):
    def test_unknown_status_send_retries_exact_bytes_without_new_guard_or_token(
        self,
    ) -> None:
        for failure in (
            TimeoutError("unknown caller outcome"),
            control_ha_api.owner.regional.HttpResponse(
                504, {"code": "catalog_commit_timeout"}, {}
            ),
        ):
            fleet = control_ha_api.APIFleet.__new__(control_ha_api.APIFleet)
            fleet.count = 3
            fleet.lease = mock.Mock(return_value={"owner_id": "owner", "fence": "1"})
            fleet.raw = mock.Mock(
                side_effect=(
                    failure,
                    control_ha_api.owner.regional.HttpResponse(200, {}, {}),
                )
            )
            self.assertEqual(0, fleet.commit_retention_status("/owned/status", "1", 7))
            self.assertEqual(fleet.raw.call_args_list[0], fleet.raw.call_args_list[1])
            fleet.lease.assert_called_once()

    def test_cached_catalog_hint_is_discarded_only_on_typed_leader_loss(self) -> None:
        fleet = control_ha_api.APIFleet.__new__(control_ha_api.APIFleet)
        fleet.leader = mock.Mock(side_effect=(1, 2))
        fleet.cluster = mock.Mock()
        response = control_ha_api.owner.regional.HttpResponse
        fleet.cluster.request.side_effect = (
            response(409, {"code": "not_leader"}, {}),
            response(200, {}, {}),
            response(409, {"code": "catalog_conflict"}, {}),
        )
        self.assertEqual(200, fleet.raw("GET", "/lease").status)
        self.assertEqual(
            409, fleet.raw("PUT", "/resources", {"request_token": "exact"}).status
        )
        self.assertEqual(2, fleet.leader.call_count)
        self.assertEqual(
            [1, 2, 2], [call.args[0] for call in fleet.cluster.request.call_args_list]
        )
        self.assertTrue(
            all(
                call.kwargs["timeout_seconds"] == 12
                for call in fleet.cluster.request.call_args_list
            )
        )

    def test_retention_churn_resolves_fencing_and_uses_fresh_guard_and_token(
        self,
    ) -> None:
        fleet = control_ha_api.APIFleet.__new__(control_ha_api.APIFleet)
        fleet.count = 3
        fleet.lease = mock.Mock(
            side_effect=(
                {"owner_id": "old", "fence": "1"},
                {"owner_id": "new", "fence": "2"},
            )
        )
        response = control_ha_api.owner.regional.HttpResponse
        fleet.raw = mock.Mock(
            side_effect=(
                response(409, {"code": "catalog_conflict"}, {}),
                response(200, {"state": "failed", "mutation": {"code": "fenced"}}, {}),
                response(200, {}, {}),
            )
        )
        self.assertEqual(1, fleet.commit_retention_status("/owned/status", "1", 7))
        first, lookup, second = fleet.raw.call_args_list
        self.assertNotEqual(
            first.args[2]["request_token"], second.args[2]["request_token"]
        )
        self.assertEqual("old", first.args[2]["lease"]["owner_id"])
        self.assertEqual("new", second.args[2]["lease"]["owner_id"])
        self.assertEqual("GET", lookup.args[0])
        self.assertTrue(lookup.args[1].endswith(first.args[2]["request_token"]))

    def test_retention_churn_does_not_retry_other_definitive_rejections(self) -> None:
        fleet = control_ha_api.APIFleet.__new__(control_ha_api.APIFleet)
        fleet.count = 3
        fleet.lease = mock.Mock(return_value={"owner_id": "owner", "fence": "1"})
        response = control_ha_api.owner.regional.HttpResponse
        fleet.raw = mock.Mock(
            side_effect=(
                response(409, {"message": "fenced (untrusted text)"}, {}),
                response(
                    200, {"state": "failed", "mutation": {"code": "capacity"}}, {}
                ),
            )
        )
        with self.assertRaises(AssertionError):
            fleet.commit_retention_status("/owned/status", "1", 7)
        fleet.lease.assert_called_once()

    def test_failed_generated_client_retains_partial_proof_and_redacted_error(
        self,
    ) -> None:
        fleet = control_ha_api.APIFleet.__new__(control_ha_api.APIFleet)
        fleet.count = 3
        fleet.helper = Path("/explicit/owned/controlgrpc")
        controller = mock.Mock()
        controller.process.poll.return_value = None
        controller.paused = False
        controller.grpc_port = 12345
        fleet.controllers = [controller]
        with tempfile.TemporaryDirectory() as temporary:
            fleet.artifact_dir = Path(temporary)
            partial = {
                "schema": control_ha_api.GRPC_SCHEMA,
                "checks": {"completed_case": True},
            }
            with (
                mock.patch.object(
                    control_ha_api.subprocess,
                    "run",
                    return_value=mock.Mock(
                        returncode=1,
                        stderr="failed with epoch-dev-reader-v1",
                        stdout=json.dumps(partial),
                    ),
                ),
                self.assertRaises(AssertionError),
            ):
                fleet.generated_clients("prepare")
            failure = json.loads(
                (fleet.artifact_dir / "api-prepare-failed.json").read_text()
            )
            self.assertEqual(partial, failure["partial_proof"])
            self.assertEqual("failed", failure["status"])
            self.assertNotIn("epoch-dev-reader-v1", failure["error"])

    def test_complete_api_subset_does_not_claim_catalog_leader_certification(
        self,
    ) -> None:
        control_ha_api.validate_api_evidence(api_evidence_fixture())
        self.assertNotEqual(
            "epoch.control-ha.full-certification/v1", control_ha_api.API_SCHEMA
        )

    def test_every_api_check_and_recovery_phase_is_required(self) -> None:
        for check in control_ha_api.API_CHECKS:
            for value in (None, False, 1, "true"):
                evidence = api_evidence_fixture()
                if value is None:
                    del evidence["fleets"][0]["api"]["checks"][check]
                else:
                    evidence["fleets"][0]["api"]["checks"][check] = value
                with (
                    self.subTest(check=check, value=value),
                    self.assertRaises(ValueError),
                ):
                    control_ha_api.validate_api_evidence(evidence)
        for phase in control_ha_api.API_PHASES:
            evidence = api_evidence_fixture()
            evidence["fleets"][1]["api"]["phases"].remove(phase)
            with self.assertRaises(ValueError):
                control_ha_api.validate_api_evidence(evidence)

    def test_stale_cursor_requires_observed_history_retention(self) -> None:
        for value in (0, 1, True, "2"):
            evidence = api_evidence_fixture()
            evidence["fleets"][0]["api"]["retention"]["earliest_cursor"] = value
            with self.assertRaises(ValueError):
                control_ha_api.validate_api_evidence(evidence)

    def test_generated_binary_and_nontrivial_operation_inventory_are_required(
        self,
    ) -> None:
        for field, value in (
            ("helper_sha256", "a" * 64),
            ("helper_sha256", None),
            ("operation_count", 1),
            ("operation_count", True),
            ("desired_count", 127),
            ("desired_count", "128"),
        ):
            evidence = copy.deepcopy(api_evidence_fixture())
            evidence["fleets"][0]["api"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                control_ha_api.validate_api_evidence(evidence)


if __name__ == "__main__":
    unittest.main()
