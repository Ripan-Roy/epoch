"""Contract tests for the regional multi-profile fault campaign."""

from __future__ import annotations

import importlib.util
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
