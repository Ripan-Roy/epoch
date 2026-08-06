# Epoch Python SDK

This is the initial typed Python client for Epoch's provisional native HTTP
surface. It covers health and resource discovery plus every Cache, Stream,
Queue, and Event Bus route currently exposed by the standalone node.

```python
from epoch_sdk import EpochClient, EventEnvelope

client = EpochClient("http://127.0.0.1:7601")
client.create_stream("orders", partitions=4, durability="local_durable")
client.create_queue("jobs", durability="local_durable")
receipt = client.append_stream(
    "orders",
    EventEnvelope(source="checkout", event_type="order.created", payload={"id": "1001"}),
)
print(receipt)
```

All profiles accept `volatile`; Stream and Queue creation may explicitly request
`durability="local_durable"` for single-node fsync and restart recovery. Event
Bus filters, transforms, and targets use typed models; the transport is
injectable for fast contract tests. This package is pre-alpha, is not published,
and performs no hidden retries for standalone operations.

`RegionalStreamClient` is the explicit replicated alternative. Configure it
with every Rust node endpoint, a `RegionalScope`, and a bearer token. It
discovers the current leader before every call, copies generation/tablet
fences, preserves caller-owned append/checkpoint idempotency across one bounded
rediscovery, and requests linearizable fetch/lag reads. See the
[complete regional example](../../console/src/quickstarts/regional/quickstart.py)
and [contract guide](../../docs/REGIONAL_STREAM_SDK.md).

`RegionalQueueClient` applies that same discovery, bearer, fence, same-key
rediscovery, and linearizable-read contract to enqueue, credit-aware acquire,
every lease disposition, maintenance, DLQ/redrive history, redrive, counts,
flow, mutation lookup, and status. See the
[complete Queue example](../../console/src/quickstarts/regional_queue/quickstart.py)
and [Queue contract guide](../../docs/REGIONAL_QUEUE_SDK.md).

`RegionalCacheClient` exposes typed values, version/missing CAS, atomic
transactions, fenced lock lifecycle, explicit expiry maintenance, mutation
lookup, linearizable observation, and status through the same shared regional
core. See the
[complete Cache example](../../console/src/quickstarts/regional_cache/quickstart.py)
and [Cache contract guide](../../docs/REGIONAL_CACHE_SDK.md).

Native gRPC streaming, coordinated consumer sessions, generated response
models, and package publication remain future work.
