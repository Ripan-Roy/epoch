# Regional Event Bus SDK

**Status:** Repository-local, single-shard regional alpha

Epoch's Go, Java, and Python Event Bus clients call the same replicated tablet hosted by the Rust regional runtime. The clients discover the current leader, send generation and tablet fences, preserve caller-owned mutation identity across bounded rediscovery, and request linearizable archive, delivery, mutation, and status reads.

This is the stable native boundary for the implemented Bus route-plan, archive, and delivery-ledger lifecycle. It is not the standalone `/v1/buses/...` API and does not route data through the Go control plane.

## Resource identity

Every call identifies one exact shard:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{bus}/shards/{shard}
```

`GET` on that root returns the current route. SDKs select the endpoint whose response has `accepts_writes: true`, then send:

- `Authorization: Bearer ...`
- `x-epoch-resource-generation`
- `x-epoch-tablet-epoch`
- `x-epoch-read-consistency: linearizable` for reads

The current implementation accepts shard `0`; keeping the shard in the API prevents a later partitioned Bus from changing resource identity.

## Lifecycle surface

| SDK operation | Tablet operation | Meaning |
|---|---|---|
| `upsert_subscription` | `upsert_subscription` | Create or replace one typed filter/target/transform/delivery policy |
| `remove_subscription` | `remove_subscription` | Remove one exact route-plan entry |
| `publish` | `publish` | Archive and route one strict event envelope |
| `acquire_deliveries` | `acquire_deliveries` | Lease 1–100 delivery intents to one dispatcher epoch |
| `acknowledge_delivery` | `acknowledge_delivery` | Permanently settle one fenced lease |
| `fail_delivery` | `fail_delivery` | Record a reason and transition to retry or dead letter |
| `maintain_deliveries` | `maintain_deliveries` | Explicitly process due retries and expired leases |
| `mutation` | mutation lookup | Resolve one proposal ID |
| `replay_archive` | `archive/replay` | Replay 1–10,000 archived events in an inclusive server-received time range |
| `query_deliveries` | `deliveries/query` | Query 1–10,000 records by subscription/state |
| `status` | status | Observe consensus, route plan, archive, ledger counts, and digest |

The regional materializer enables the existing replicated delivery outbox. It records durable intent only. Epoch does not perform the target's external side effect in this increment.

## Typed subscription policy

A subscription combines:

- event type, source, subject, header, and JSON-equality filters;
- pull, Queue, Stream, webhook, or HTTP target metadata;
- deterministic added headers and payload projections;
- timeout, maximum in-flight delivery count, and bounded retry policy.

Default delivery policy:

```json
{
  "timeout_ms": 30000,
  "max_in_flight": 16,
  "retry": {
    "strategy": "exponential",
    "initial_delay_ms": 1000,
    "max_delay_ms": 60000,
    "jitter_percent": 10,
    "max_attempts": 8,
    "max_age_ms": null
  }
}
```

Retry jitter is deterministic replicated state, not process-local randomness. A dispatcher must use the returned lease token and the same non-zero dispatcher epoch when acknowledging or failing a delivery.

## Go

```go
client, err := epoch.NewRegionalBusClient(
    []string{"http://node-a:7601", "http://node-b:7601", "http://node-c:7601"},
    os.Getenv("EPOCH_TOKEN"),
    epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "prod", Namespace: "core"},
    3*time.Second,
)
policy := epoch.DefaultDeliveryPolicy()
subscription := epoch.Subscription{
    Name: "orders",
    Filter: epoch.EventFilter{EventTypePatterns: []string{"order.*"}},
    Target: epoch.PullTarget(),
    DeliveryPolicy: &policy,
}
_, err = client.UpsertSubscription(ctx, "events", 0, "upsert-orders-v1", subscription)
```

The exact executable source is [quickstart.go](../console/src/quickstarts/regional_bus/quickstart.go).

## Java

```java
RegionalBusClient client = new RegionalBusClient(
    endpoints,
    System.getenv("EPOCH_TOKEN"),
    new RegionalScope("acme", "shop", "prod", "core"),
    Duration.ofSeconds(3));
Subscription subscription = new Subscription("orders", SubscriptionTarget.pull());
client.upsertSubscription("events", 0, "upsert-orders-v1", subscription);
```

Java uses `BigInteger` for the complete unsigned 64-bit range. The exact executable source is [RegionalBusQuickstart.java](../console/src/quickstarts/regional_bus/RegionalBusQuickstart.java).

## Python

```python
client = RegionalBusClient(
    ["http://node-a:7601", "http://node-b:7601", "http://node-c:7601"],
    token=os.environ["EPOCH_TOKEN"],
    scope=RegionalScope("acme", "shop", "prod", "core"),
)
subscription = Subscription(
    "orders",
    SubscriptionTarget.pull(),
    filter=EventFilter(event_type_patterns=["order.*"]),
)
client.upsert_subscription("events", 0, "upsert-orders-v1", subscription)
```

The exact executable source is [quickstart.py](../console/src/quickstarts/regional_bus/quickstart.py).

## Delivery worker sequence

1. Acquire a bounded batch for one subscription and dispatcher epoch.
2. Perform the external operation using the delivery ID as downstream idempotency metadata where supported.
3. Acknowledge with the exact delivery ID, dispatcher identity/epoch, and opaque lease token.
4. On a failed side effect, commit `fail_delivery` with a bounded reason instead.
5. Run explicit maintenance to recover expired leases and apply due retry/dead-letter transitions.

Do not acknowledge before the downstream operation is durably accepted. Epoch proves the delivery ledger transition; it cannot prove an arbitrary external system's business side effect.

## Retry and outcome certainty

Mutation calls require a caller-owned idempotency key. The SDK may rediscover once after retryable transport, leader, fence, route, or read-barrier failures, but it reuses the exact key and body. A committed exact replay returns the original receipt with `disposition: replayed`; a changed body under the same key conflicts.

If a request's outcome is uncertain, resolve its proposal ID before generating a new business identity. Mutation lookup is linearizable at the current leader.

## Read guarantees

Archive replay, delivery query, mutation lookup, and status request a leader ReadIndex barrier. Responses include consistency evidence and browser-safe decimal strings for unsigned 64-bit positions, epochs, terms, times, and indexes. No SDK silently falls back to `local_stale`.

## Current limits and non-claims

- one native Bus shard (`0`) per resource;
- acquire/maintenance batches: 1–100;
- archive/query results: 1–10,000;
- dispatcher identity: bounded, caller-owned, non-session identity;
- no push stream, long poll, automatic lease renewal, or dispatcher coordinator;
- no built-in HTTP/webhook/Queue/Stream executor yet;
- no webhook signing, OAuth/API-key secrets, schema validation, MQTT, or geo routing;
- no exactly-once external-side-effect claim.

The authoritative decision record is [ADR-0020](adr/0020-regional-event-bus-v1-and-sdk-routing.md). The lower-level state machine is documented in [BUS_TABLET.md](BUS_TABLET.md).
