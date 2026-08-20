from __future__ import annotations

import json
import os
import time
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


def result(document: dict[str, Any]) -> dict[str, Any]:
    operation = document["receipt"]["outcome"]["result"]
    assert isinstance(operation, dict)
    return operation


client = RegionalBusClient(
    os.getenv(
        "EPOCH_REGIONAL_ENDPOINTS",
        "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663",
    ).split(","),
    token=os.getenv("EPOCH_TOKEN", "epoch-dev-admin-v1"),
    scope=RegionalScope("acme", "shop", "dev", "core"),
    timeout=3.0,
)
subscription = Subscription(
    "orders",
    SubscriptionTarget.pull(),
    filter=EventFilter(event_type_patterns=["order.*"]),
    delivery_policy=DeliveryPolicy(retry=DeliveryRetryPolicy(strategy="fixed")),
)
upserted = client.upsert_subscription(
    "events", 0, "docs-python-bus-upsert-v1", subscription
)
queue_subscription = Subscription(
    "queue-jobs",
    SubscriptionTarget.queue("jobs"),
    filter=EventFilter(event_type_patterns=["target.*"]),
    delivery_policy=subscription.delivery_policy,
)
queue_upserted = client.upsert_subscription(
    "events", 0, "docs-python-bus-queue-target-v1", queue_subscription
)
stream_subscription = Subscription(
    "stream-orders",
    SubscriptionTarget.stream("orders"),
    filter=EventFilter(event_type_patterns=["target.*"]),
    delivery_policy=subscription.delivery_policy,
)
stream_upserted = client.upsert_subscription(
    "events", 0, "docs-python-bus-stream-target-v1", stream_subscription
)
event = EventEnvelope(
    id="docs-order-1",
    source="docs-python",
    event_type="order.created",
    payload={"id": 1},
    time_ms=time.time_ns() // 1_000_000,
)
published = client.publish("events", 0, "docs-python-bus-publish-v1", event)
replayed = client.publish("events", 0, "docs-python-bus-publish-v1", event)
acquired = client.acquire_deliveries(
    "events",
    0,
    "docs-python-bus-acquire-v1",
    subscription="orders",
    dispatcher="docs-python",
    dispatcher_epoch=1,
    max_deliveries=1,
)
delivery = result(acquired)["deliveries"][0]
acknowledged = client.acknowledge_delivery(
    "events",
    0,
    "docs-python-bus-ack-v1",
    delivery["delivery_id"],
    "docs-python",
    1,
    delivery["lease_token"],
)
target_published = client.publish(
    "events",
    0,
    "docs-python-bus-target-publish-v1",
    EventEnvelope(
        id="docs-target-1",
        source="docs-python",
        event_type="target.created",
        key="customer-42",
        payload={"id": 2},
    ),
)


def wait_for_target(subscription_name: str, kind: str) -> dict[str, Any]:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        document = client.query_deliveries(
            "events",
            0,
            subscription=subscription_name,
            state="acknowledged",
            limit=100,
        )
        for record in document["records"]:
            if record.get("destination", {}).get("kind") == kind:
                return record
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {kind} target delivery")


queue_delivery = wait_for_target("queue-jobs", "queue")
stream_delivery = wait_for_target("stream-orders", "stream")

print(
    json.dumps(
        {
            "upsert": upserted,
            "queue_target_upsert": queue_upserted,
            "stream_target_upsert": stream_upserted,
            "publish": published,
            "exact_retry": replayed,
            "acknowledge": acknowledged,
            "target_publish": target_published,
            "queue_delivery": queue_delivery,
            "stream_delivery": stream_delivery,
            "archive": client.replay_archive(
                "events",
                0,
                from_ms=0,
                to_ms=(1 << 64) - 1,
                limit=100,
                filter=subscription.filter,
            ),
            "deliveries": client.query_deliveries(
                "events",
                0,
                subscription="orders",
                state="acknowledged",
                limit=100,
            ),
            "status": client.status("events", 0),
        },
        indent=2,
    )
)
