from __future__ import annotations

import math
import unittest
from typing import Any

from epoch_sdk import (
    RegionalCacheClient,
    RegionalCacheExpectation,
    RegionalCacheLockGuard,
    RegionalCacheMultiplexMutation,
    RegionalCacheMutation,
    RegionalCacheTransform,
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

    def test_advanced_state_backup_query_and_pubsub_routes(self) -> None:
        transform = RegionalCacheTransform("bitmap_set", {"bit": 7, "value": True})

        self.client.transform("sessions", 0, "transform-1", "flags", transform)
        self.client.changes("sessions", 0, 1, limit=100)
        self.client.backup("sessions", 0)
        self.client.restore("sessions", 0, "restore-1", "artifact", 7)
        self.client.query("sessions", 0, "bitmap_get", {"key": "flags", "bit": 7})
        self.client.create_subscription("sessions", 0, channels=["audit"], patterns=["orders.*"])
        self.client.publish("sessions", 0, "audit", {"id": 1})
        self.client.poll_subscription("sessions", 0, "cache-7-1", limit=10)
        self.client.delete_subscription("sessions", 0, "cache-7-1")
        self.client.multiplex(
            "sessions",
            0,
            [
                RegionalCacheMultiplexMutation(
                    "profile",
                    "multiplex-profile",
                    RegionalCacheMutation.set("profile", RegionalCacheValue.string("ready")),
                ),
                RegionalCacheMultiplexMutation(
                    "visits",
                    "multiplex-visits",
                    RegionalCacheMutation.increment("visits", 1),
                ),
            ],
        )

        operations = self.transport.requests[1::2]
        self.assertEqual(len(operations), 10)
        self.assertEqual(
            [request["path"].rsplit("/shards/0", 1)[1] for request in operations],
            [
                "/mutations",
                "/changes",
                "/backup",
                "/mutations",
                "/query",
                "/pubsub/subscriptions",
                "/pubsub/messages",
                "/pubsub/subscriptions/cache-7-1/messages",
                "/pubsub/subscriptions/cache-7-1",
                "/multiplex",
            ],
        )
        self.assertEqual(
            operations[0]["body"]["operation"]["transform"],
            {"kind": "bitmap_set", "bit": 7, "value": True},
        )
        self.assertEqual(operations[4]["method"], "POST")
        self.assertEqual(operations[4]["headers"]["x-epoch-read-consistency"], "linearizable")
        self.assertEqual(operations[8]["method"], "DELETE")
        self.assertEqual(operations[9]["body"]["mutations"][0]["correlation_id"], "profile")

    def test_transaction_transform_and_cold_storage_class(self) -> None:
        self.client.set(
            "sessions",
            0,
            "cold-1",
            "archive",
            RegionalCacheValue.string("value"),
            storage_class="cold",
        )
        self.client.transaction(
            "sessions",
            0,
            "tx-1",
            0,
            [
                RegionalCacheMutation.transform(
                    "flags",
                    RegionalCacheTransform("bitmap_set", {"bit": 1, "value": True}),
                )
            ],
        )

        operations = self.transport.requests[1::2]
        self.assertEqual(operations[0]["body"]["operation"]["storage_class"], "cold")
        mutation = operations[1]["body"]["operation"]["mutations"][0]
        self.assertEqual(mutation["kind"], "transform")
        self.assertEqual(mutation["transform"]["kind"], "bitmap_set")


if __name__ == "__main__":
    unittest.main()
