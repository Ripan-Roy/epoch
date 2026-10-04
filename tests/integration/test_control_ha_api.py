"""Fail-closed contracts for generated-client HA recovery evidence."""

from __future__ import annotations

import copy
import unittest

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
