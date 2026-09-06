"""Fail-closed contracts for real-client regional compatibility evidence."""

from __future__ import annotations

import copy
import unittest

import protocol_regional as campaign


class ProtocolRegionalContractTest(unittest.TestCase):
    def evidence(self) -> dict:
        return {
            "schema": campaign.RESULT_SCHEMA,
            "clients": campaign.CLIENTS.copy(),
            "faults": [
                {"kind": "gateway_sigkill"},
                *[
                    {
                        "kind": "leader_sigkill",
                        "profile": profile,
                        "old_leader": 1,
                        "new_leader": 2,
                        "old_term": 3,
                        "new_term": 4,
                    }
                    for profile in ("cache", "stream", "queue")
                ],
                {"kind": "all_voter_sigkill_reopen"},
            ],
            "checks": list(campaign.REQUIRED_CHECKS),
        }

    def test_accepts_complete_fault_and_client_evidence(self) -> None:
        campaign.validate_evidence(self.evidence())

    def test_rejects_missing_fault_or_client_or_check(self) -> None:
        for field in ("faults", "clients", "checks"):
            evidence = self.evidence()
            if isinstance(evidence[field], dict):
                evidence[field].pop("kafka")
            else:
                evidence[field].pop()
            with self.subTest(field=field), self.assertRaises(ValueError):
                campaign.validate_evidence(evidence)

    def test_requires_new_leader_and_strictly_newer_term(self) -> None:
        for field, value in (("new_leader", 1), ("new_term", 3), ("new_term", True)):
            evidence = copy.deepcopy(self.evidence())
            evidence["faults"][1][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                campaign.validate_evidence(evidence)

    def test_rejects_duplicate_profile_instead_of_missing_profile(self) -> None:
        evidence = self.evidence()
        evidence["faults"][2]["profile"] = "cache"
        with self.assertRaises(ValueError):
            campaign.validate_evidence(evidence)


if __name__ == "__main__":
    unittest.main()
