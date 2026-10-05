"""Deterministic lost-response injection contracts, independent of Docker."""

from __future__ import annotations

import copy
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import control_ha_faults as faults


def leader_fixture(count: int = 3) -> dict:
    return {
        "schema": faults.LEADER_SCHEMA,
        "controller_count": count,
        "old_catalog_leader": 1,
        "new_catalog_leader": 2,
        "killed_container": "a" * 64,
        "pending_callers_at_kill": count,
        "old_leader_stopped_before_response_drop": True,
        "requests": [
            {
                "request_token": f"original-{index}",
                "command_sha256": "sha256:" + "b" * 64,
                "upstream_status": 200,
                "generation": 2,
                "caller_status": None,
                "caller_unknown": True,
                "replay_status": 200,
                "replayed": True,
                "recovered_generation": 2,
            }
            for index in range(count)
        ],
    }


class LostResponseTest(unittest.TestCase):
    def test_committed_responses_remain_pending_until_explicit_fault_release(
        self,
    ) -> None:
        bodies = [{"request_token": f"token-{index}"} for index in range(3)]
        forward = mock.Mock(
            return_value=mock.Mock(status=200, document={"resource": {"generation": 2}})
        )
        plan = {body["request_token"]: forward for body in bodies}
        with faults.DropCommittedResponses(plan, "Bearer test-only") as proxy:
            with ThreadPoolExecutor(max_workers=3) as executor:
                calls = [
                    executor.submit(
                        faults.request_unknown, proxy.url, body, "Bearer test-only"
                    )
                    for body in bodies
                ]
                self.assertTrue(proxy.await_committed(timeout=5))
                self.assertEqual(3, proxy.pending_callers)
                self.assertFalse(any(call.done() for call in calls))
                self.assertEqual(3, forward.call_count)
                self.assertTrue(
                    all(record["upstream_status"] == 200 for record in proxy.records)
                )
                proxy.drop_responses()
                results = [call.result(timeout=5) for call in calls]
                self.assertTrue(
                    all(result["caller_unknown"] is True for result in results)
                )
                self.assertTrue(
                    all(result["caller_status"] is None for result in results)
                )

    def test_definitive_http_rejection_is_not_classified_as_unknown(self) -> None:
        forward = mock.Mock()
        with faults.DropCommittedResponses(
            {"planned": forward}, "Bearer test-only"
        ) as proxy:
            result = faults.request_unknown(
                proxy.url, {"request_token": "planned"}, "Bearer invalid"
            )
            self.assertEqual(401, result["caller_status"])
            self.assertIs(False, result["caller_unknown"])
            forward.assert_not_called()
            self.assertEqual(0, proxy.pending_callers)

    def test_proxy_close_releases_owned_pending_socket_workers(self) -> None:
        forward = mock.Mock(
            return_value=mock.Mock(status=200, document={"resource": {"generation": 2}})
        )
        proxy = faults.DropCommittedResponses({"planned": forward}, "Bearer test-only")
        proxy.start()
        with ThreadPoolExecutor(max_workers=1) as executor:
            call = executor.submit(
                faults.request_unknown,
                proxy.url,
                {"request_token": "planned"},
                "Bearer test-only",
            )
            self.assertTrue(proxy.await_committed(timeout=5))
            proxy.close()
            self.assertIs(True, call.result(timeout=5)["caller_unknown"])
        self.assertFalse(proxy.server_thread.is_alive())
        self.assertTrue(all(not worker.is_alive() for worker in proxy.workers))


class CatalogLeaderEvidenceTest(unittest.TestCase):
    def test_distinct_leader_loss_and_original_token_resolution_are_required(
        self,
    ) -> None:
        for count in (3, 5):
            faults.validate_leader_evidence(leader_fixture(count))
        for field, value in (
            ("old_catalog_leader", 4),
            ("new_catalog_leader", 1),
            ("killed_container", "unknown"),
            ("pending_callers_at_kill", 0),
            ("pending_callers_at_kill", True),
            ("old_leader_stopped_before_response_drop", False),
        ):
            proof = copy.deepcopy(leader_fixture())
            proof[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                faults.validate_leader_evidence(proof)

    def test_false_unknown_or_nonexact_replay_cannot_certify_the_case(self) -> None:
        for field, value in (
            ("upstream_status", 503),
            ("generation", 1),
            ("caller_status", 503),
            ("caller_unknown", 1),
            ("replay_status", 201),
            ("replayed", False),
            ("recovered_generation", 3),
            ("command_sha256", "missing"),
        ):
            proof = copy.deepcopy(leader_fixture())
            proof["requests"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                faults.validate_leader_evidence(proof)


if __name__ == "__main__":
    unittest.main()
