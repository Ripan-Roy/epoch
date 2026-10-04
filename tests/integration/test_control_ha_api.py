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
