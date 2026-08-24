from __future__ import annotations

import unittest
from typing import Any

from epoch_sdk import (
    DeliveryPolicy,
    DeliveryRateLimit,
    DeliveryRetryPolicy,
    DestinationAuth,
    EventEnvelope,
    EventFilter,
    RegionalBusClient,
    RegionalScope,
    SchemaRegistration,
    SchemaValidationPolicy,
    Subscription,
    SubscriptionTarget,
    TransformLimits,
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
            SubscriptionTarget.signed_webhook("https://example.com/orders", "primary"),
            filter=EventFilter(event_type_patterns=["order.*"]),
            delivery_policy=DeliveryPolicy(
                retry=DeliveryRetryPolicy(strategy="fixed"),
                rate_limit=DeliveryRateLimit(deliveries_per_second=25, burst=50),
                dead_letter_retention_ms=86_400_000,
            ),
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
            wait_ms=5_000,
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
        self.client.reject_delivery(
            bus,
            0,
            "reject-1",
            "delivery-3",
            "worker-a",
            7,
            "lease-3",
            "http status 400",
        )
        self.client.redrive_delivery(bus, 0, "redrive-1", "delivery-3")
        self.client.maintain_deliveries(bus, 0, "maintain-1", max_deliveries=100)
        self.client.maintain_archive(bus, 0, "archive-retention-1", max_events=100)
        self.client.apply_integration(
            bus,
            0,
            "schema-1",
            {"kind": "register_schema", "registration": {"name": "orders"}},
        )
        self.client.remove_subscription(bus, 0, "remove-1", "orders")
        self.client.mutation(bus, 0, 12)
        self.client.replay_archive(
            bus, 0, from_ms=1, to_ms=10, limit=100, filter=subscription.filter
        )
        self.client.query_deliveries(bus, 0, subscription="orders", state="in_flight", limit=100)
        self.client.status(bus, 0)
        self.client.integration_state(bus, 0)

        base = (
            "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/"
            "buses/events%2Feu/shards/0"
        )
        operations = self.transport.requests[1::2]
        self.assertEqual(len(operations), 16)
        self.assertTrue(all(request["path"].startswith(base) for request in operations))
        self.assertEqual(operations[0]["body"]["operation"]["kind"], "upsert_subscription")
        self.assertEqual(
            operations[0]["body"]["operation"]["subscription"]["delivery_policy"]["retry"][
                "strategy"
            ],
            "fixed",
        )
        self.assertEqual(
            operations[0]["body"]["operation"]["subscription"]["target"]["signing_key_id"],
            "primary",
        )
        self.assertEqual(
            operations[0]["body"]["operation"]["subscription"]["delivery_policy"]["rate_limit"],
            {"deliveries_per_second": 25, "burst": 50},
        )
        self.assertEqual(
            operations[0]["body"]["operation"]["subscription"]["delivery_policy"][
                "dead_letter_retention_ms"
            ],
            86_400_000,
        )
        self.assertEqual(operations[2]["body"]["operation"]["wait_ms"], 5_000)
        self.assertEqual(operations[5]["body"]["operation"]["kind"], "reject_delivery")
        self.assertEqual(operations[6]["body"]["operation"]["kind"], "redrive_delivery")
        self.assertEqual(operations[8]["body"]["operation"]["kind"], "maintain_archive")
        self.assertEqual(operations[9]["body"]["operation"]["kind"], "apply_integration")
        self.assertEqual(operations[11]["path"], f"{base}/mutations/12")
        self.assertEqual(operations[12]["body"]["from_ms"], "1")
        self.assertEqual(operations[13]["body"]["state"], "in_flight")
        self.assertEqual(operations[15]["path"], f"{base}/integration/state")
        for request in operations[11:]:
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
            self.client.acquire_deliveries(
                "events",
                0,
                "acquire-wait",
                subscription="orders",
                dispatcher="worker",
                dispatcher_epoch=1,
                max_deliveries=1,
                wait_ms=30_001,
            )
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            self.client.replay_archive("events", 0, from_ms=10, to_ms=1, limit=1)
        with self.assertRaisesRegex(ValueError, "initial delay"):
            DeliveryRetryPolicy(initial_delay_ms=10, max_delay_ms=1)
        with self.assertRaisesRegex(ValueError, "between 1 and 1000000"):
            DeliveryRateLimit(deliveries_per_second=0, burst=1)
        with self.assertRaisesRegex(ValueError, "delivery ID"):
            self.client.redrive_delivery("events", 0, "redrive", "")
        with self.assertRaisesRegex(ValueError, "signing key ID"):
            SubscriptionTarget.signed_webhook("https://example.com/orders", "bad/key")
        with self.assertRaisesRegex(ValueError, "network access"):
            TransformLimits(network_access=True)
        with self.assertRaisesRegex(ValueError, r"HTTP\(S\) token URL"):
            DestinationAuth.oauth2("oauth", "file:///token")
        self.assertEqual(self.transport.requests, [])

    def test_typed_schema_lifecycle_is_validated_and_routed(self) -> None:
        event = EventEnvelope(
            id="order-2",
            source="python-regional-sdk",
            event_type="order.created",
            payload={"id": 2},
            time_ms=2,
        )
        self.client.register_schema(
            "events",
            0,
            "schema-1",
            SchemaRegistration(
                "orders",
                "protobuf",
                'syntax = "proto3"; message Order { string id = 1; }',
                "backward",
                "Order",
            ),
        )
        self.client.upsert_schema_validation_policy(
            "events",
            0,
            "policy-1",
            SchemaValidationPolicy("orders", "order.*", "orders@1", "producer_and_broker"),
        )
        self.client.validate_schema("events", 0, "producer", event)
        self.client.remove_schema_validation_policy("events", 0, "policy-remove-1", "orders")

        operations = self.transport.requests[1::2]
        self.assertEqual(len(operations), 4)
        registration = operations[0]["body"]["operation"]["operation"]["registration"]
        self.assertEqual(registration["format"], "protobuf")
        self.assertEqual(registration["root_message"], "Order")
        self.assertTrue(operations[2]["path"].endswith("/schema/validate"))
        self.assertEqual(operations[2]["body"]["mode"], "producer")
        self.assertEqual(operations[2]["headers"]["x-epoch-read-consistency"], "linearizable")

    def test_invalid_schema_lifecycle_fails_before_network(self) -> None:
        with self.assertRaisesRegex(ValueError, "schema name"):
            SchemaRegistration("bad/name", "json_schema", "{}", "none")
        with self.assertRaisesRegex(ValueError, "schema definition"):
            SchemaRegistration("orders", "json_schema", "", "none")
        with self.assertRaisesRegex(ValueError, "only for Protobuf"):
            SchemaRegistration("orders", "json_schema", "{}", "none", "Order")
        with self.assertRaisesRegex(ValueError, "event type pattern"):
            SchemaValidationPolicy("orders", "", "orders@1", "broker")
        with self.assertRaisesRegex(ValueError, "policy name"):
            self.client.remove_schema_validation_policy("events", 0, "remove", "bad/name")
        self.assertEqual(self.transport.requests, [])


if __name__ == "__main__":
    unittest.main()
