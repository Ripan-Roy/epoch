from __future__ import annotations

import json
import os
from typing import Any

from epoch_sdk import EventEnvelope, RegionalQueueClient, RegionalScope


def result(document: dict[str, Any]) -> dict[str, Any]:
    receipt = document["receipt"]
    assert isinstance(receipt, dict)
    outcome = receipt["outcome"]
    assert isinstance(outcome, dict)
    operation = outcome["result"]
    assert isinstance(operation, dict)
    return operation


client = RegionalQueueClient(
    os.getenv(
        "EPOCH_REGIONAL_ENDPOINTS",
        "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663",
    ).split(","),
    token=os.getenv("EPOCH_TOKEN", "epoch-dev-admin-v1"),
    scope=RegionalScope("acme", "shop", "dev", "core"),
    timeout=3.0,
)
event = EventEnvelope(
    id="docs-python-job-42",
    source="docs-python",
    event_type="job.created",
    payload={"job_id": "python-42"},
    time_ms=42,
)

enqueued = client.enqueue("jobs", 0, "docs-python-enqueue-v1", event)
replayed = client.enqueue("jobs", 0, "docs-python-enqueue-v1", event)
acquired = client.acquire(
    "jobs",
    0,
    "docs-python-acquire-v1",
    consumer="docs-python",
    consumer_epoch=1,
    max_messages=1,
    max_in_flight=1,
    visibility_timeout_ms=5_000,
)
delivery = result(acquired)["deliveries"][0]
extended = client.extend_lease(
    "jobs",
    0,
    "docs-python-extend-v1",
    "docs-python",
    1,
    delivery["lease_token"],
    60_000,
)
released = client.release(
    "jobs",
    0,
    "docs-python-release-v1",
    "docs-python",
    1,
    result(extended)["lease_token"],
    0,
    "demonstrate retry",
)
maintained = client.maintain("jobs", 0, "docs-python-maintain-v1")
reacquired = client.acquire(
    "jobs",
    0,
    "docs-python-reacquire-v1",
    consumer="docs-python",
    consumer_epoch=1,
    max_messages=1,
    max_in_flight=1,
)
redelivery = result(reacquired)["deliveries"][0]
rejected = client.reject(
    "jobs",
    0,
    "docs-python-reject-v1",
    "docs-python",
    1,
    redelivery["lease_token"],
    "poison",
)
history_id = int(result(rejected)["dead_letter_history_id"])
dead_letters = client.dead_letters("jobs", 0, limit=10)
redriven = client.redrive("jobs", 0, "docs-python-redrive-v1", event.id, history_id)
final_acquire = client.acquire(
    "jobs",
    0,
    "docs-python-final-acquire-v1",
    consumer="docs-python",
    consumer_epoch=1,
    max_messages=1,
    max_in_flight=1,
)
final_delivery = result(final_acquire)["deliveries"][0]
acknowledged = client.acknowledge(
    "jobs",
    0,
    "docs-python-ack-v1",
    "docs-python",
    1,
    final_delivery["lease_token"],
)

print(
    json.dumps(
        {
            "enqueue": enqueued,
            "exact_retry": replayed,
            "release": released,
            "maintain": maintained,
            "dead_letters": dead_letters,
            "redrive": redriven,
            "ack": acknowledged,
            "counts": client.counts("jobs", 0),
            "flow": client.consumer_flow("jobs", 0, "docs-python"),
        },
        indent=2,
    )
)
