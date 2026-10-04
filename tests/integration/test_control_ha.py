"""Fail-closed contracts for the concurrent controller fault fixture."""

from __future__ import annotations

import copy
import signal
import subprocess
import unittest
from unittest import mock

import control_ha


def evidence_fixture() -> dict:
    fleets = []
    for count in (3, 5):
        fleets.append(
            {
                "controller_count": count,
                "owners": [f"owner-{index}" for index in range(count)],
                "checks": {check: True for check in control_ha.OWNER_CHECKS},
                "profile_digests_before_reopen": {
                    profile: "a" * 64 for profile in control_ha.PROFILES
                },
                "profile_digests_after_reopen": {
                    profile: "a" * 64 for profile in control_ha.PROFILES
                },
                "lease_transitions": [
                    {
                        "fault": fault,
                        "old_owner": "old",
                        "new_owner": "new",
                        "old_fence": 1,
                        "new_fence": 2,
                    }
                    for fault in ("owner_sigkill", "owner_sigstop")
                ],
            }
        )
    return {"schema": control_ha.OWNER_SCHEMA, "status": "passed", "fleets": fleets}


class OwnerEvidenceContractTest(unittest.TestCase):
    def test_complete_owner_matrix_is_accepted_without_claiming_full_certification(
        self,
    ) -> None:
        control_ha.validate_owner_evidence(evidence_fixture())
        self.assertNotEqual(
            "epoch.control-ha.full-certification/v1", control_ha.OWNER_SCHEMA
        )

    def test_missing_false_or_non_boolean_checks_are_rejected(self) -> None:
        for check in control_ha.OWNER_CHECKS:
            for value in (None, False, 1, "true"):
                with self.subTest(check=check, value=value):
                    evidence = evidence_fixture()
                    if value is None:
                        del evidence["fleets"][0]["checks"][check]
                    else:
                        evidence["fleets"][0]["checks"][check] = value
                    with self.assertRaises(ValueError):
                        control_ha.validate_owner_evidence(evidence)

    def test_both_real_controller_counts_and_distinct_owners_are_required(self) -> None:
        for counts in ([3], [3, 3], [True, 5], [5, 3], [3, 5, 5]):
            with self.subTest(counts=counts):
                evidence = evidence_fixture()
                evidence["fleets"] = [
                    {**copy.deepcopy(evidence["fleets"][0]), "controller_count": count}
                    for count in counts
                ]
                with self.assertRaises(ValueError):
                    control_ha.validate_owner_evidence(evidence)
        evidence = evidence_fixture()
        evidence["fleets"][1]["owners"][-1] = evidence["fleets"][1]["owners"][0]
        with self.assertRaises(ValueError):
            control_ha.validate_owner_evidence(evidence)

    def test_reopen_requires_every_profile_and_exact_digest_equality(self) -> None:
        for profile in control_ha.PROFILES:
            evidence = evidence_fixture()
            del evidence["fleets"][0]["profile_digests_after_reopen"][profile]
            with self.assertRaises(ValueError):
                control_ha.validate_owner_evidence(evidence)
            evidence = evidence_fixture()
            evidence["fleets"][0]["profile_digests_after_reopen"][profile] = "b" * 64
            with self.assertRaises(ValueError):
                control_ha.validate_owner_evidence(evidence)

    def test_takeover_requires_a_different_owner_and_strictly_larger_fence(
        self,
    ) -> None:
        for field, value in (
            ("new_owner", "old"),
            ("new_fence", 1),
            ("new_fence", True),
            ("old_fence", 0),
            ("new_owner", None),
        ):
            evidence = evidence_fixture()
            evidence["fleets"][0]["lease_transitions"][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                control_ha.validate_owner_evidence(evidence)

    def test_paused_child_is_resumed_before_shutdown(self) -> None:
        process = mock.Mock()
        process.poll.return_value = None
        control_ha.stop_process(process, paused=True)
        self.assertEqual(
            [
                mock.call.poll(),
                mock.call.send_signal(signal.SIGCONT),
                mock.call.terminate(),
                mock.call.wait(timeout=10),
            ],
            process.mock_calls,
        )

    def test_shutdown_is_bounded_and_does_not_signal_an_exited_child(self) -> None:
        process = mock.Mock()
        process.poll.return_value = 0
        control_ha.stop_process(process, paused=True)
        self.assertEqual([mock.call.poll()], process.mock_calls)
        process = mock.Mock()
        process.poll.return_value = None
        process.wait.side_effect = (
            subprocess.TimeoutExpired("owned-controller", 10),
            0,
        )
        control_ha.stop_process(process)
        process.kill.assert_called_once_with()
        self.assertEqual(mock.call.wait(timeout=5), process.mock_calls[-1])


if __name__ == "__main__":
    unittest.main()
