"""Fail-closed contracts for the concurrent controller fault fixture."""

from __future__ import annotations

import copy
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path
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
    def test_final_bundle_verification_controls_success_and_failure_manifests(
        self,
    ) -> None:
        source = {
            "worktree_clean": True,
            "git_revision": "a" * 40,
            "version": "0.2.0-beta.12",
        }
        runtime = {"image_revision": "b" * 40, "image_version": source["version"]}
        fleets = evidence_fixture()["fleets"]

        def fleet(count: int, directory: Path) -> object:
            directory.mkdir()
            control_ha.soak.atomic_write(
                directory / "workload.json", control_ha.soak.canonical_bytes(count)
            )
            result = next(item for item in fleets if item["controller_count"] == count)
            return mock.Mock(run=mock.Mock(return_value=copy.deepcopy(result)))

        for fails in (False, True):
            with (
                self.subTest(fails=fails),
                tempfile.TemporaryDirectory(
                    prefix="epoch-campaign-verifier-"
                ) as folder,
                mock.patch.object(
                    control_ha.soak, "source_identity", return_value=source
                ),
                mock.patch.object(
                    control_ha.soak, "runtime_identity", return_value=runtime
                ),
                mock.patch.object(control_ha.subprocess, "run"),
                mock.patch("builtins.print") as printed,
            ):
                output = (Path(folder) / "evidence").resolve()
                verifier = mock.Mock(
                    side_effect=ValueError("invalid final artifact") if fails else None
                )
                if fails:
                    with self.assertRaisesRegex(ValueError, "invalid final artifact"):
                        control_ha.run_campaign(
                            output, fleet_type=fleet, bundle_verifier=verifier
                        )
                    printed.assert_not_called()
                    self.assertEqual(
                        "failed",
                        control_ha.soak.load_json(output / "failure.json")["status"],
                    )
                    with self.assertRaises(ValueError):
                        control_ha.verify_bundle(output / "evidence.json")
                else:
                    control_ha.run_campaign(
                        output, fleet_type=fleet, bundle_verifier=verifier
                    )
                    printed.assert_called_once()
                    self.assertFalse((output / "failure.json").exists())
                verifier.assert_called_once_with(output / "evidence.json")
                self.assertEqual(
                    "failed" if fails else "passed",
                    control_ha.soak.load_json(output / "evidence.json")["status"],
                )

    def test_guard_recovery_respects_bounded_internal_receipts_but_keeps_user_delete_outcomes(
        self,
    ) -> None:
        for kind in (
            "update_managed_status",
            "reconcile_managed",
            "plan_managed_membership",
        ):
            self.assertFalse(control_ha.guard_outcome_is_persistent(kind))
        self.assertTrue(control_ha.guard_outcome_is_persistent("delete_managed"))
        with self.assertRaises(ValueError):
            control_ha.guard_outcome_is_persistent("unknown_command")

    def test_reopen_waits_for_strong_managed_reads_after_process_health(self) -> None:
        fleet = control_ha.OwnerFleet.__new__(control_ha.OwnerFleet)
        fleet.cluster = mock.Mock()
        fleet.controllers = []
        fleet.resources = [
            control_ha.regional.Resource(profile, profile)
            for profile in control_ha.PROFILES
        ]
        fleet.http_resources = []
        fleet.result = {"observations": {"stale_guard_rejections": []}}
        fleet.mark = mock.Mock()
        fleet.stop_nodes = mock.Mock()
        expected = {"cache": {"generation": 1}}
        fleet.all_desired = mock.Mock(
            side_effect=(expected, AssertionError("Catalog still electing"), expected)
        )

        def readiness(_description: str, check: object, **_kwargs: object) -> object:
            try:
                return check()  # type: ignore[operator]
            except AssertionError:
                return check()  # type: ignore[operator]

        with (
            mock.patch.object(control_ha.regional, "wait_for_nodes"),
            mock.patch.object(
                control_ha.regional, "wait_for_profile_recovery", return_value="a" * 64
            ),
            mock.patch.object(
                control_ha.regional, "wait_until", side_effect=readiness
            ) as wait,
        ):
            fleet.reopen()
            wait.assert_called_once()
            self.assertEqual(3, fleet.all_desired.call_count)

    def test_owner_fixture_discovers_the_catalog_authority_without_enabling_the_probe(
        self,
    ) -> None:
        fleet = control_ha.OwnerFleet.__new__(control_ha.OwnerFleet)
        fleet.cluster = mock.Mock()

        def request(node: int, method: str, path: str, **_kwargs: object) -> object:
            if path != control_ha.CONTROL_ROOT + "/lease":
                return control_ha.regional.HttpResponse(404, {}, {})
            if node == 2:
                return control_ha.regional.HttpResponse(
                    200, {"owner_id": "owner", "fence": "1"}, {}
                )
            return control_ha.regional.HttpResponse(409, {"code": "not_leader"}, {})

        fleet.cluster.request.side_effect = request
        with mock.patch.object(
            control_ha.regional,
            "wait_until",
            side_effect=lambda _description, check: check(),
        ):
            self.assertEqual(2, fleet.leader())

    def test_event_bus_uses_the_existing_control_kind_without_changing_native_identity(
        self,
    ) -> None:
        resource = control_ha.regional.Resource("event-bus", "events")
        self.assertEqual(
            "/v1/resources/acme/shop/dev/core/event_bus/events",
            control_ha.resource_path(resource),
        )
        self.assertEqual("event-bus", control_ha.resource_name(resource)["kind"])

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
