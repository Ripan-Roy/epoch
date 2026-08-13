from __future__ import annotations

import json
import os

from epoch_sdk import (
    EventEnvelope,
    RegionalScope,
    RegionalStreamClient,
    StreamRetentionPolicy,
    stream_shard_for,
)


endpoints = os.getenv(
    "EPOCH_REGIONAL_ENDPOINTS",
    "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663",
).split(",")
client = RegionalStreamClient(
    endpoints,
    token=os.getenv("EPOCH_TOKEN", "epoch-dev-admin-v1"),
    scope=RegionalScope("acme", "shop", "dev", "core"),
    timeout=3.0,
)
event = EventEnvelope(
    id="docs-python-order-42",
    key="customer-0",
    source="docs-python",
    event_type="order.created",
    payload={"order_id": "python-42"},
    time_ms=42,
)
shard = stream_shard_for(event.key or event.id, 3)

appended = client.append_keyed("orders", "docs-python-keyed-stream-v1", event)
replayed = client.append_keyed("orders", "docs-python-keyed-stream-v1", event)
offset = int(appended["receipt"]["offset"])
fetched = client.fetch("orders", shard, offset, limit=10)
group_records = client.fetch_group("orders", shard, "docs-python", limit=100)
checkpoint = client.commit_offset(
    "orders",
    shard,
    "docs-python",
    "docs-python-worker",
    1,
    offset + 1,
    idempotency_key="docs-python-checkpoint-v1",
)
lag = client.lag("orders", shard, "docs-python")
configured = client.configure_retention(
    "orders",
    shard,
    "docs-python-retention-v1",
    StreamRetentionPolicy(
        max_records_per_partition=10_000,
        max_bytes_per_partition=3 * 1024 * 1024,
        max_age_ms=7 * 24 * 60 * 60 * 1_000,
    ),
)
maintained = client.maintain_retention(
    "orders", shard, "docs-python-retention-sweep-v1"
)
retention = client.retention("orders", shard)

print(
    json.dumps(
        {
            "selected_shard": shard,
            "append": appended,
            "exact_retry": replayed,
            "fetch": fetched,
            "group_fetch": group_records,
            "checkpoint": checkpoint,
            "lag": lag,
            "retention_configure": configured,
            "retention_maintenance": maintained,
            "retention": retention,
        },
        indent=2,
    )
)
