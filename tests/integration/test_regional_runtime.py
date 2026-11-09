"""Contract tests for the regional multi-profile fault campaign."""

from __future__ import annotations

import importlib.util
from concurrent.futures import ThreadPoolExecutor
import json
import sys
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("regional-runtime.py")
MODULE_SPEC = importlib.util.spec_from_file_location("regional_runtime", MODULE_PATH)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"could not load {MODULE_PATH}")
regional_runtime = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = regional_runtime
MODULE_SPEC.loader.exec_module(regional_runtime)


class RegionalRuntimeContractTest(unittest.TestCase):
    def test_real_parallel_allocations_stay_unique_across_calls(self) -> None:
        with ThreadPoolExecutor(max_workers=8) as executor:
            allocated = list(executor.map(regional_runtime.allocate_ports, [3] * 16))
        ports = [port for group in allocated for port in group]
        self.assertEqual(len(ports), 48)
        self.assertEqual(len(set(ports)), 48)
        self.assertTrue(all(0 < port <= 65_535 for port in ports))

    def test_port_space_and_invalid_count_fail_before_opening_sockets(self) -> None:
        with mock.patch.object(regional_runtime.socket, "socket") as create:
            with self.assertRaises(ValueError):
                regional_runtime.allocate_ports(-1)
            with mock.patch.object(
                regional_runtime, "_allocated_ports", set(range(1, 65_536))
            ):
                with self.assertRaisesRegex(RuntimeError, "unique TCP ports"):
                    regional_runtime.allocate_ports(1)
                self.assertEqual(regional_runtime.allocate_ports(0), [])
            create.assert_not_called()

    def test_port_allocations_do_not_reuse_a_closed_prior_fleet_listener(self) -> None:
        listeners = []
        for port in (41001, 41002, 41001, 41003, 41004):
            listener = mock.Mock()
            listener.getsockname.return_value = ("127.0.0.1", port)
            listeners.append(listener)
        with (
            mock.patch.object(regional_runtime, "_allocated_ports", set(), create=True),
            mock.patch.object(regional_runtime.socket, "socket", side_effect=listeners),
        ):
            self.assertEqual(regional_runtime.allocate_ports(2), [41001, 41002])
            self.assertEqual(regional_runtime.allocate_ports(2), [41003, 41004])
        for listener in listeners:
            listener.close.assert_called_once()

    def test_repeated_kernel_port_reuse_fails_with_a_bounded_attempt_count(
        self,
    ) -> None:
        listener = mock.Mock()
        listener.getsockname.return_value = ("127.0.0.1", 41001)
        with (
            mock.patch.object(
                regional_runtime, "_allocated_ports", {41001}, create=True
            ),
            mock.patch.object(
                regional_runtime.socket, "socket", return_value=listener
            ) as create,
        ):
            with self.assertRaisesRegex(RuntimeError, "unique.*port"):
                regional_runtime.allocate_ports(1)
            self.assertLessEqual(create.call_count, 128)
            self.assertEqual(listener.close.call_count, create.call_count)

    def test_port_bind_failure_closes_the_failed_socket_and_does_not_reserve_partial_results(
        self,
    ) -> None:
        first, failed = mock.Mock(), mock.Mock()
        first.getsockname.return_value = ("127.0.0.1", 41001)
        failed.bind.side_effect = OSError("fixture bind failure")
        reserved: set[int] = set()
        with (
            mock.patch.object(
                regional_runtime, "_allocated_ports", reserved, create=True
            ),
            mock.patch.object(
                regional_runtime.socket, "socket", side_effect=[first, failed]
            ),
        ):
            with self.assertRaisesRegex(OSError, "fixture bind failure"):
                regional_runtime.allocate_ports(2)
        first.close.assert_called_once()
        failed.close.assert_called_once()
        self.assertEqual(reserved, set())

    def test_audit_page_verifier_checks_the_canonical_hash_chain(self) -> None:
        fixture = MODULE_PATH.parents[2] / "spec/auth/audit-journal-v1.example.ndjson"
        record = json.loads(fixture.read_text(encoding="utf-8"))
        records, sequence, digest, at_end = regional_runtime.verify_audit_page(
            {
                "records": [record],
                "next_sequence": "1",
                "end_of_journal": True,
            },
            0,
            "0" * 64,
        )
        self.assertEqual([record], records)
        self.assertEqual(1, sequence)
        self.assertEqual(record["record_sha256"], digest)
        self.assertTrue(at_end)

        tampered = json.loads(json.dumps(record))
        tampered["event"]["request_id"] = "tampered-request"
        with self.assertRaises(AssertionError):
            regional_runtime.verify_audit_page(
                {
                    "records": [tampered],
                    "next_sequence": "1",
                    "end_of_journal": True,
                },
                0,
                "0" * 64,
            )

    def test_wait_until_honors_a_scoped_timeout(self) -> None:
        with (
            mock.patch.object(
                regional_runtime.time,
                "monotonic",
                side_effect=(10.0, 10.0, 10.5, 11.0),
            ),
            mock.patch.object(regional_runtime.time, "sleep"),
            self.assertRaisesRegex(AssertionError, "timed out waiting for recovery"),
        ):
            regional_runtime.wait_until("recovery", lambda: None, timeout_seconds=1.0)

    def test_profile_timeout_reports_every_node_observation(self) -> None:
        class Cluster:
            @staticmethod
            def request(
                node: int,
                _method: str,
                _path: str,
                headers: dict[str, str],
            ) -> regional_runtime.HttpResponse:
                del headers
                applied = 7 if node != 2 else 6
                return regional_runtime.HttpResponse(
                    200,
                    {
                        "applied_command_count": applied,
                        "state_digest": f"digest-{applied}",
                    },
                    {},
                )

        def run_once(
            _description: str,
            check: object,
            *,
            timeout_seconds: float,
        ) -> object:
            self.assertEqual(4.0, timeout_seconds)
            return check()  # type: ignore[operator]

        with (
            mock.patch.object(regional_runtime, "wait_until", side_effect=run_once),
            self.assertRaisesRegex(
                AssertionError,
                '"1":.*"applied_command_count":7.*"2":.*"applied_command_count":6',
            ),
        ):
            regional_runtime.wait_for_profile_apply(
                Cluster(),
                regional_runtime.Resource("stream", "orders"),
                7,
                timeout_seconds=4.0,
            )

    def test_recovery_wait_uses_the_longer_bounded_deadline(self) -> None:
        cluster = object()
        resource = regional_runtime.Resource("queue", "jobs")
        with mock.patch.object(
            regional_runtime, "wait_for_profile_apply", return_value="digest"
        ) as wait_for_apply:
            self.assertEqual(
                "digest",
                regional_runtime.wait_for_profile_recovery(
                    cluster, resource, 12, (1, 2, 3), shard=2
                ),
            )

        wait_for_apply.assert_called_once_with(
            cluster,
            resource,
            12,
            (1, 2, 3),
            2,
            timeout_seconds=regional_runtime.RECOVERY_TIMEOUT_SECONDS,
        )


if __name__ == "__main__":
    unittest.main()
