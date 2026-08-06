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

print(
    json.dumps(
        {
            "upsert": upserted,
            "publish": published,
            "exact_retry": replayed,
            "acknowledge": acknowledged,
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
