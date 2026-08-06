from __future__ import annotations

import unittest
from typing import Any

from epoch_sdk import (
    DeliveryPolicy,
    DeliveryRetryPolicy,
    EventEnvelope,
    EventFilter,
    RegionalBusClient,
    RegionalScope,
    Subscription,
    SubscriptionTarget,
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


class RegionalBusClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = RecordingTransport()
        self.client = RegionalBusClient.with_transports(
            [self.transport],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )

    def test_complete_mutation_and_linearizable_read_contract(self) -> None:
        subscription = Subscription(
            "orders",
            SubscriptionTarget.pull(),
            filter=EventFilter(event_type_patterns=["order.*"]),
            delivery_policy=DeliveryPolicy(retry=DeliveryRetryPolicy(strategy="fixed")),
        )
        event = EventEnvelope(
            id="order-2",
            source="python-regional-sdk",
            event_type="order.created",
            payload={"id": 2},
            time_ms=2,
        )
        bus = "events/eu"

        self.client.upsert_subscription(bus, 0, "upsert-1", subscription)
        self.client.publish(bus, 0, "publish-1", event)
        self.client.acquire_deliveries(
            bus,
            0,
            "acquire-1",
            subscription="orders",
            dispatcher="worker-a",
            dispatcher_epoch=7,
            max_deliveries=10,
        )
        self.client.acknowledge_delivery(bus, 0, "ack-1", "delivery-1", "worker-a", 7, "lease-1")
        self.client.fail_delivery(
            bus,
            0,
            "fail-1",
            "delivery-2",
            "worker-a",
            7,
            "lease-2",
            "downstream timeout",
        )
        self.client.maintain_deliveries(bus, 0, "maintain-1", max_deliveries=100)
        self.client.remove_subscription(bus, 0, "remove-1", "orders")
        self.client.mutation(bus, 0, 12)
        self.client.replay_archive(
            bus, 0, from_ms=1, to_ms=10, limit=100, filter=subscription.filter
        )
        self.client.query_deliveries(bus, 0, subscription="orders", state="in_flight", limit=100)
        self.client.status(bus, 0)

        base = (
            "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/"
            "buses/events%2Feu/shards/0"
        )
        operations = self.transport.requests[1::2]
        self.assertEqual(len(operations), 11)
        self.assertTrue(all(request["path"].startswith(base) for request in operations))
        self.assertEqual(operations[0]["body"]["operation"]["kind"], "upsert_subscription")
        self.assertEqual(
            operations[0]["body"]["operation"]["subscription"]["delivery_policy"]["retry"][
                "strategy"
            ],
            "fixed",
        )
        self.assertEqual(operations[7]["path"], f"{base}/mutations/12")
        self.assertEqual(operations[8]["body"]["from_ms"], "1")
        self.assertEqual(operations[9]["body"]["state"], "in_flight")
        for request in operations[7:]:
            self.assertEqual(request["headers"]["x-epoch-read-consistency"], "linearizable")

    def test_invalid_bounds_and_policy_fail_before_network(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 1 and 100"):
            self.client.acquire_deliveries(
                "events",
                0,
                "acquire",
                subscription="orders",
                dispatcher="worker",
                dispatcher_epoch=1,
                max_deliveries=0,
            )
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            self.client.replay_archive("events", 0, from_ms=10, to_ms=1, limit=1)
        with self.assertRaisesRegex(ValueError, "initial delay"):
            DeliveryRetryPolicy(initial_delay_ms=10, max_delay_ms=1)
        self.assertEqual(self.transport.requests, [])


if __name__ == "__main__":
    unittest.main()
