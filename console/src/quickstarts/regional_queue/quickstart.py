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

session_event = EventEnvelope(
    id="docs-python-session-42",
    source="docs-python",
    event_type="session.job.created",
    payload={"job_id": "python-session-42"},
    time_ms=43,
)
session_enqueue = client.enqueue(
    "jobs",
    0,
    "docs-python-session-enqueue-v1",
    session_event,
    session_id="account-python-7",
    correlation_id="request-python-7",
    reply_to="reply-temporary",
)
correlated = client.correlation("jobs", 0, "request-python-7")
session_acquire = client.acquire(
    "jobs",
    0,
    "docs-python-session-acquire-v1",
    consumer="docs-python-session",
    consumer_epoch=1,
    max_messages=1,
    max_in_flight=1,
    visibility_timeout_ms=5_000,
    session_id="account-python-7",
)
session_renew = client.renew_session_lock(
    "jobs",
    0,
    "docs-python-session-renew-v1",
    "docs-python-session",
    1,
    result(session_acquire)["session_lock_token"],
    30_000,
)
client.acknowledge(
    "jobs",
    0,
    "docs-python-session-ack-v1",
    "docs-python-session",
    1,
    result(session_acquire)["deliveries"][0]["lease_token"],
)
session_release = client.release_session_lock(
    "jobs",
    0,
    "docs-python-session-release-v1",
    "docs-python-session",
    1,
    result(session_renew)["session_lock_token"],
)

deferred_event = EventEnvelope(
    id="docs-python-deferred-42",
    source="docs-python",
    event_type="job.deferred",
    payload={"job_id": "python-deferred-42"},
    time_ms=44,
)
client.enqueue("jobs", 0, "docs-python-deferred-enqueue-v1", deferred_event)
deferred_acquire = client.acquire(
    "jobs",
    0,
    "docs-python-deferred-acquire-v1",
    consumer="docs-python-deferred",
    consumer_epoch=1,
    max_messages=1,
    max_in_flight=1,
)
deferred = client.defer(
    "jobs",
    0,
    "docs-python-defer-v1",
    "docs-python-deferred",
    1,
    result(deferred_acquire)["deliveries"][0]["lease_token"],
    "await dependency",
)
received_deferred = client.receive_deferred(
    "jobs",
    0,
    "docs-python-receive-deferred-v1",
    deferred_event.id,
    "docs-python-deferred",
    1,
    visibility_timeout_ms=5_000,
)
client.acknowledge(
    "jobs",
    0,
    "docs-python-deferred-ack-v1",
    "docs-python-deferred",
    1,
    result(received_deferred)["delivery"]["lease_token"],
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
            "session_enqueue": session_enqueue,
            "correlation": correlated,
            "session_release": session_release,
            "defer": deferred,
            "receive_deferred": received_deferred,
            "advanced": client.advanced_status("jobs", 0),
            "dead_letter_forwards": client.dead_letter_forwards("jobs", 0, limit=10),
        },
        indent=2,
    )
)
