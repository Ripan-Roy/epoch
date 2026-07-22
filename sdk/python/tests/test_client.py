from __future__ import annotations

import unittest
from typing import Any

from epoch_sdk import (
    EpochAPIError,
    EpochClient,
    EventEnvelope,
    EventFilter,
    EventTransform,
    Subscription,
    SubscriptionTarget,
)


class FakeTransport:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, Any, dict[str, Any] | None]] = []
        self.response: Any = {"ok": True}

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        self.requests.append((method, path, body, query))
        return self.response


class EpochClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = FakeTransport()
        self.client = EpochClient(transport=self.transport)

    def test_stream_create_is_truthfully_volatile(self) -> None:
        self.client.create_stream("orders", partitions=4)

        method, path, body, _ = self.transport.requests[-1]
        self.assertEqual((method, path), ("POST", "/v1/streams/orders"))
        self.assertEqual(body["partitions"], 4)
        self.assertEqual(body["durability"], "volatile")

    def test_stream_create_can_request_local_durability(self) -> None:
        self.client.create_stream("audit", durability="local_durable")

        _, _, body, _ = self.transport.requests[-1]
        self.assertEqual(body["durability"], "local_durable")

    def test_queue_create_can_request_local_durability(self) -> None:
        self.client.create_queue("jobs", durability="local_durable")

        method, path, body, _ = self.transport.requests[-1]
        self.assertEqual((method, path), ("POST", "/v1/queues/jobs"))
        self.assertEqual(body["durability"], "local_durable")

    def test_queue_create_defaults_to_volatile(self) -> None:
        self.client.create_queue("jobs")

        _, _, body, _ = self.transport.requests[-1]
        self.assertEqual(body["durability"], "volatile")

    def test_event_envelope_maps_python_name_to_wire_type(self) -> None:
        event = EventEnvelope(
            id="order-1",
            source="checkout",
            event_type="order.created",
            payload={"id": "1"},
            time_ms=1_000,
        )
        self.client.append_stream("orders", event, partition=1)

        _, _, body, _ = self.transport.requests[-1]
        self.assertEqual(body["envelope"]["type"], "order.created")
        self.assertNotIn("event_type", body["envelope"])
        self.assertEqual(body["partition"], 1)

    def test_resource_and_key_segments_are_escaped(self) -> None:
        self.client.cache_set("sessions/eu", "user 42", "active")

        _, path, _, _ = self.transport.requests[-1]
        self.assertEqual(path, "/v1/caches/sessions%2Feu/keys/user%2042")

    def test_invalid_event_is_rejected_before_transport(self) -> None:
        with self.assertRaisesRegex(ValueError, "source"):
            EventEnvelope(source="", event_type="created", payload={})
        self.assertEqual(self.transport.requests, [])

    def test_cache_mutation_operations_map_to_native_routes(self) -> None:
        self.client.cache_set(
            "sessions",
            "user-42",
            "active",
            only_if_absent=True,
        )
        self.client.cache_increment("sessions", "visits", delta=2)
        self.client.cache_delete("sessions", "user-42")

        set_request, increment_request, delete_request = self.transport.requests
        self.assertTrue(set_request[2]["only_if_absent"])
        self.assertEqual(
            increment_request,
            (
                "POST",
                "/v1/caches/sessions/keys/visits/increment",
                {"delta": 2},
                None,
            ),
        )
        self.assertEqual(
            delete_request,
            ("DELETE", "/v1/caches/sessions/keys/user-42", None, None),
        )

    def test_retry_classification_covers_transport_and_server_failures(self) -> None:
        retryable = [
            EpochAPIError(0, "transport_error", "reset"),
            EpochAPIError(500, "internal", "failed"),
            EpochAPIError(400, "unavailable", "transient"),
        ]
        self.assertTrue(all(error.retryable for error in retryable))
        self.assertFalse(EpochAPIError(400, "invalid_argument", "invalid").retryable)

    def test_stream_group_operations_map_to_native_routes(self) -> None:
        self.client.commit_stream_offset("orders", "billing", partition=2, next_offset=7)
        self.client.stream_lag("orders", "billing", partition=2)

        self.assertEqual(
            self.transport.requests,
            [
                (
                    "PUT",
                    "/v1/streams/orders/groups/billing/offsets",
                    {"partition": 2, "next_offset": 7, "reset": False},
                    None,
                ),
                (
                    "GET",
                    "/v1/streams/orders/groups/billing/lag",
                    None,
                    {"partition": 2},
                ),
            ],
        )

    def test_queue_lifecycle_operations_map_to_native_routes(self) -> None:
        self.client.queue_counts("jobs")
        self.client.extend_lease("jobs", "lease-1", extension_ms=5_000)
        self.client.reject("jobs", "lease-2", reason="invalid")
        self.client.redrive("jobs", "message-1")

        self.assertEqual(self.transport.requests[0][1], "/v1/queues/jobs/counts")
        self.assertEqual(
            self.transport.requests[1][2],
            {"action": "extend", "token": "lease-1", "extension_ms": 5_000},
        )
        self.assertEqual(
            self.transport.requests[2][2],
            {"action": "reject", "token": "lease-2", "reason": "invalid"},
        )
        self.assertEqual(
            self.transport.requests[3],
            (
                "POST",
                "/v1/queues/jobs/dead-letters/message-1/redrive",
                None,
                None,
            ),
        )

    def test_bus_subscription_and_replay_use_typed_models(self) -> None:
        subscription = Subscription(
            name="priority-orders",
            target=SubscriptionTarget.queue("jobs"),
            filter=EventFilter(event_type_patterns=["order.*"]),
            transform=EventTransform(add_headers={"routed-by": "epoch"}),
        )

        self.client.upsert_subscription("events", subscription)
        self.client.replay_bus("events", from_ms=100, to_ms=200, event_type="order.*")
        self.client.remove_subscription("events", "priority-orders")

        method, path, body, query = self.transport.requests[0]
        self.assertEqual(
            (method, path, query), ("PUT", "/v1/buses/events/subscriptions/priority-orders", None)
        )
        self.assertEqual(body["target"], {"kind": "queue", "resource": "jobs"})
        self.assertEqual(body["filter"]["event_type_patterns"], ["order.*"])
        self.assertEqual(body["transform"]["add_headers"], {"routed-by": "epoch"})
        self.assertEqual(
            self.transport.requests[1],
            (
                "GET",
                "/v1/buses/events/replay",
                None,
                {"from_ms": 100, "to_ms": 200, "limit": 100, "event_type": "order.*"},
            ),
        )
        self.assertEqual(
            self.transport.requests[2],
            (
                "DELETE",
                "/v1/buses/events/subscriptions/priority-orders",
                None,
                None,
            ),
        )


if __name__ == "__main__":
    unittest.main()
