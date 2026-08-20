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
fences, preserves caller-owned append/batch/checkpoint/session/retention idempotency
across one bounded rediscovery, and requests linearizable
fetch/lag/session/retention reads.
`stream_shard_for` implements the advertised FNV-1a UTF-8 contract, while
`append_keyed` selects that shard from the event key or ID and fails before
writing if the resource generation changes. The client also exposes
single-shard atomic `append_batch`: `StreamBatchFrame.encode` builds canonical
none or gzip frames, while `StreamBatchFrame.from_compressed` wraps
caller-produced standard LZ4, Snappy, or Zstd frames without changing their
bytes. It also exposes
time/size/combined retention configuration and explicit idle maintenance. Its
shard-zero session methods cover join, heartbeat, leave, expiry maintenance,
and deterministic resource-wide assignment observation. See the
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

`RegionalBusClient` exposes subscription delivery policy, publish, delivery
acquire/ack/fail/reject/maintenance, mutation lookup, archive replay, delivery query,
and status through the shared regional core. Exact mutation keys and opaque
lease tokens survive one bounded leader rediscovery.
`SubscriptionTarget.signed_webhook` captures the key ID and
`verify_webhook_signature` authenticates the exact `bytes` body plus canonical
timestamp/delivery/attempt fields before returning the replay identity. See the
[complete Event Bus example](../../console/src/quickstarts/regional_bus/quickstart.py)
and [Event Bus contract guide](../../docs/REGIONAL_EVENT_BUS_SDK.md).

Native gRPC streaming, background/cooperative consumer sessions, atomic
assignment-plus-offset handoff, generated response models, and package
publication remain future work.
