from __future__ import annotations

import math
import unittest
from typing import Any

from epoch_sdk import (
    RegionalCacheClient,
    RegionalCacheExpectation,
    RegionalCacheLockGuard,
    RegionalCacheMutation,
    RegionalCacheValue,
    RegionalScope,
)


class RecordingTransport:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        self.requests.append(
            {"method": method, "path": path, "body": body, "query": query, "headers": headers}
        )
        if method == "GET" and path.endswith("/shards/0"):
            return {
                "resource_generation": "6",
                "tablet_epoch": "4",
                "term": "11",
                "accepts_writes": True,
            }
        return {"state": "committed", "outcome_certainty": "committed"}


class RegionalCacheClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = RecordingTransport()
        self.client = RegionalCacheClient.with_transports(
            [self.transport],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )

    def test_complete_mutation_and_linearizable_read_contract(self) -> None:
        guard = RegionalCacheLockGuard("critical", "worker-a", 7, "lease-7")
        values = [
            RegionalCacheValue.string("alice"),
            RegionalCacheValue.blob(b"\x00\xff"),
            RegionalCacheValue.counter(-2),
            RegionalCacheValue.hash({"role": "admin"}),
            RegionalCacheValue.list(["a", "b"]),
            RegionalCacheValue.set(["a", "b"]),
            RegionalCacheValue.sorted_set({"alice": 1.5}),
        ]

        self.client.set("sessions/eu", 0, "set-1", "profile", values[0], ttl_ms=5_000)
        self.client.delete("sessions/eu", 0, "delete-1", "old", expected_version=4)
        self.client.compare_and_set(
            "sessions/eu",
            0,
            "cas-1",
            "profile",
            RegionalCacheExpectation.version(1),
            values[1],
            lock_guard=guard,
        )
        self.client.increment("sessions/eu", 0, "inc-1", "visits", -3, expected_version=0)
        self.client.get("sessions/eu", 0, "get-1", "profile")
        self.client.atomic_batch(
            "sessions/eu",
            0,
            "batch-1",
            4,
            [
                RegionalCacheMutation.set("hash", values[3]),
                RegionalCacheMutation.set("list", values[4]),
                RegionalCacheMutation.set("set", values[5]),
                RegionalCacheMutation.set("rank", values[6]),
                RegionalCacheMutation.compare_and_set(
                    "new", RegionalCacheExpectation.missing(4), values[2]
                ),
            ],
            lock_guards=[guard],
        )
        self.client.acquire_lock("sessions/eu", 0, "lock-1", "critical", "worker-a", 7, 3_000)
        self.client.renew_lock(
            "sessions/eu", 0, "renew-1", "critical", "worker-a", 7, "lease-7", 4_000
        )
        self.client.release_lock(
            "sessions/eu", 0, "release-1", "critical", "worker-a", 7, "lease-8"
        )
        self.client.maintain("sessions/eu", 0, "maintain-1", max_expirations=100)
        self.client.mutation("sessions/eu", 0, 12)
        self.client.observe("sessions/eu", 0, "profile")
        self.client.status("sessions/eu", 0)

        base = (
            "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/"
            "caches/sessions%2Feu/shards/0"
        )
        operations = self.transport.requests[1::2]
        self.assertEqual(len(operations), 13)
        self.assertTrue(all(request["path"].startswith(base) for request in operations))
        self.assertEqual(operations[4]["body"]["operation"]["kind"], "get")
        tx = operations[5]["body"]["operation"]
        self.assertEqual(tx["expected_revision"], "4")
        self.assertEqual(tx["mutations"][3]["value"]["kind"], "sorted_set")
        self.assertEqual(tx["mutations"][4]["expected"]["shard_revision"], "4")
        self.assertEqual(operations[6]["body"]["operation"]["owner_epoch"], "7")
        self.assertEqual(operations[10]["path"], f"{base}/mutations/12")
        self.assertEqual(operations[11]["query"], {"key": "profile"})
        for request in operations[10:]:
            self.assertEqual(request["headers"]["x-epoch-read-consistency"], "linearizable")

    def test_invalid_values_and_bounds_fail_before_network(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate"):
            RegionalCacheValue.set(["same", "same"])
        with self.assertRaisesRegex(ValueError, "finite"):
            RegionalCacheValue.sorted_set({"bad": math.inf})
        with self.assertRaisesRegex(ValueError, "signed 64-bit"):
            RegionalCacheValue.counter(1 << 63)
        with self.assertRaisesRegex(ValueError, "between 1 and 1000"):
            self.client.maintain("sessions", 0, "maintain", max_expirations=0)
        with self.assertRaisesRegex(ValueError, "between 1 and 128"):
            self.client.transaction("sessions", 0, "tx", 0, [])
        self.assertEqual(self.transport.requests, [])


if __name__ == "__main__":
    unittest.main()
