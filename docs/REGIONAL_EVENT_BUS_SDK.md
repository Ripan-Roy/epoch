# Regional Event Bus SDK

**Status:** Repository-local, single-shard regional alpha with Epoch Queue/Stream and signed HTTPS delivery

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
| `fail_delivery` | `fail_delivery` | Record a retryable failure and transition to retry or attempt exhaustion |
| `reject_delivery` | `reject_delivery` | Terminally dead-letter an acquired delivery without another retry |
| `maintain_deliveries` | `maintain_deliveries` | Explicitly process due retries and expired leases |
| `mutation` | mutation lookup | Resolve one proposal ID |
| `replay_archive` | `archive/replay` | Replay 1–10,000 archived events in an inclusive server-received time range |
| `query_deliveries` | `deliveries/query` | Query 1–10,000 records by subscription/state |
| `status` | status | Observe consensus, route plan, archive, ledger counts, and digest |

The regional materializer enables the replicated delivery outbox. Pull,
unsigned webhook, and unsigned HTTP targets record durable intent for an
external dispatcher. The source Bus leader automatically executes **Queue** and
**Stream** targets. A separately configured leader-owned Rust worker executes
**signed** webhook and HTTP targets; an HTTP response remains an external
observation, not a consensus operation.

## Typed subscription policy

A subscription combines:

- event type, source, subject, header, and JSON-equality filters;
- pull, Queue, Stream, webhook, or HTTP target metadata, with an optional
  signing-key ID for HTTP/webhook;
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

## Epoch Queue and Stream delivery

Create the destination Queue or Stream in the same
organization/project/environment/namespace as the Bus, then use the existing
typed constructors:

```go
queue := epoch.Subscription{Name: "queue-jobs", Target: epoch.QueueTarget("jobs")}
stream := epoch.Subscription{Name: "stream-orders", Target: epoch.StreamTarget("orders")}
_, _ = client.UpsertSubscription(ctx, "events", 0, "queue-jobs-v1", queue)
_, _ = client.UpsertSubscription(ctx, "events", 0, "stream-orders-v1", stream)
```

```java
Subscription queue = new Subscription("queue-jobs", SubscriptionTarget.queue("jobs"));
Subscription stream = new Subscription("stream-orders", SubscriptionTarget.stream("orders"));
client.upsertSubscription("events", 0, "queue-jobs-v1", queue);
client.upsertSubscription("events", 0, "stream-orders-v1", stream);
```

```python
queue = Subscription("queue-jobs", SubscriptionTarget.queue("jobs"))
stream = Subscription("stream-orders", SubscriptionTarget.stream("orders"))
client.upsert_subscription("events", 0, "queue-jobs-v1", queue)
client.upsert_subscription("events", 0, "stream-orders-v1", stream)
```

No application dispatcher is required. Every regional node runs the scheduler;
only the current source Bus leader acts. Queue targets bind shard `0`. Stream
targets use the same FNV-1a UTF-8 key router as direct Stream clients, using the
transformed event key and falling back to the event ID. Configure the scan with
`EPOCH_REGIONAL_EPOCH_TARGET_DELIVERY_INTERVAL_MS` (default `100`, accepted
range 1–60,000).

The first source acquisition pins target kind, resource generation, shard,
tablet ID, and tablet epoch. The destination enqueue/append proposal has one
stable idempotency key across Bus attempts. A destination commit therefore
survives an unknown source-settlement outcome without inserting a duplicate in
that target incarnation. The Bus acknowledges only after the target receipt
commits. This ordered pair of commits is not an atomic cross-tablet transaction.

`query_deliveries` returns the read-only `destination` binding on acquired and
settled Queue/Stream records. Applications cannot submit that field through
the acquire API. The executable quickstarts provision both destinations,
publish a keyed event, and wait for both delivery records to become
`acknowledged` with a pinned binding.

## Signed webhook delivery

Enable the worker on every regional node with the same externally distributed
key set:

```shell
EPOCH_REGIONAL_WEBHOOK_SIGNING_KEYS_PATH=/etc/epoch/webhook-keys.json
EPOCH_REGIONAL_WEBHOOK_DELIVERY_INTERVAL_MS=100
```

The key file is strict, bounded, and never replicated:

```json
{
  "format_version": 1,
  "keys": [
    {"id": "primary", "secret": "replace-with-at-least-32-byte-secret"}
  ]
}
```

Use `EPOCH_REGIONAL_WEBHOOK_ALLOW_HTTP_LOOPBACK=true` only for a receiver on
`127.0.0.0/8`, `::1`, or `localhost` during local development. Other HTTP
targets are rejected; production targets require HTTPS and public DNS/IP
answers.

Create a signed target with the matching key ID:

```go
subscription := epoch.Subscription{
    Name: "orders-webhook",
    Filter: epoch.EventFilter{EventTypePatterns: []string{"order.*"}},
    Target: epoch.SignedWebhookTarget("https://receiver.example/orders", "primary"),
}
_, err := client.UpsertSubscription(ctx, "events", 0, "orders-webhook-v1", subscription)
```

```java
Subscription subscription = new Subscription(
    "orders-webhook",
    SubscriptionTarget.signedWebhook("https://receiver.example/orders", "primary"));
client.upsertSubscription("events", 0, "orders-webhook-v1", subscription);
```

```python
subscription = Subscription(
    "orders-webhook",
    SubscriptionTarget.signed_webhook("https://receiver.example/orders", "primary"),
    filter=EventFilter(event_type_patterns=["order.*"]),
)
client.upsert_subscription("events", 0, "orders-webhook-v1", subscription)
```

The request body is the exact JSON payload. Envelope attributes use
CloudEvents 1.0 binary-mode headers. Epoch also sends
`epoch-delivery-id`, `epoch-delivery-attempt`, `epoch-subscription`,
`epoch-signature-key-id`, `epoch-signature-timestamp`, and
`epoch-signature`.

Verify the raw body **before decoding it**. Then transactionally claim the
returned `(delivery ID, attempt)` in the receiver's inbox before applying side
effects:

```go
verified, err := epoch.VerifyWebhookSignature(
    secret, rawBody,
    request.Header.Get("epoch-delivery-id"),
    request.Header.Get("epoch-delivery-attempt"),
    request.Header.Get("epoch-signature-timestamp"),
    request.Header.Get("epoch-signature"),
    time.Now(), 5*time.Minute,
)
```

```java
WebhookSignatures.Verification verified = WebhookSignatures.verify(
    secret, rawBody,
    request.getHeader("epoch-delivery-id"),
    request.getHeader("epoch-delivery-attempt"),
    request.getHeader("epoch-signature-timestamp"),
    request.getHeader("epoch-signature"),
    Instant.now(), Duration.ofMinutes(5));
```

```python
verified = verify_webhook_signature(
    secret,
    raw_body,
    request.headers["epoch-delivery-id"],
    request.headers["epoch-delivery-attempt"],
    request.headers["epoch-signature-timestamp"],
    request.headers["epoch-signature"],
    tolerance_seconds=300,
)
```

The canonical HMAC input is:

```text
v1
<timestamp-seconds>
<delivery-id>
<attempt>
<lowercase-hex-sha256(raw-body)>
```

`2xx` acknowledges; `429`, `5xx`, DNS/connect/timeouts retry under the captured
policy; every other non-2xx response is terminal. Redirects and ambient proxies
are disabled. The complete DNS-plus-request attempt never extends beyond the
replicated lease, and an expired lease emits no request.

## Delivery worker sequence

For pull/unsigned/custom targets:

1. Acquire a bounded batch for one subscription and dispatcher epoch.
2. Perform the external operation using the delivery ID as downstream idempotency metadata where supported.
3. Acknowledge with the exact delivery ID, dispatcher identity/epoch, and opaque lease token.
4. On a failed side effect, commit `fail_delivery` with a bounded reason instead.
5. Let the regional leader recover an expired lease automatically; use explicit maintenance only when an operator needs an immediate bounded sweep.

Do not acknowledge before the downstream operation is durably accepted. Epoch proves the delivery ledger transition; it cannot prove an arbitrary external system's business side effect.

## Retry and outcome certainty

Mutation calls require a caller-owned idempotency key. The SDK may rediscover once after retryable transport, leader, fence, route, or read-barrier failures, but it reuses the exact key and body. A committed exact replay returns the original receipt with `disposition: replayed`; a changed body under the same key conflicts.

If a request's outcome is uncertain, resolve its proposal ID before generating a new business identity. Mutation lookup is linearizable at the current leader.

## Read guarantees

Archive replay, delivery query, mutation lookup, and status request a leader ReadIndex barrier. Responses include consistency evidence and browser-safe decimal strings for unsigned 64-bit positions, epochs, terms, times, and indexes. No SDK silently falls back to `local_stale`.
Queue/Stream delivery records expose their destination generation/tablet fence
with the same browser-safe encoding.

## Current limits and non-claims

- one native Bus shard (`0`) per resource;
- acquire/maintenance batches: 1–100;
- archive/query results: 1–10,000;
- dispatcher identity: bounded, caller-owned, non-session identity;
- no push stream, long poll, automatic lease renewal, or dispatcher coordinator;
- built-in execution covers Epoch Queue/Stream and signed HTTP/webhook targets;
  unsigned HTTP/webhook, long-poll, and managed push executors remain open;
- no OAuth/API-key target auth, key hot reload/secret manager, schema
  validation, MQTT, private egress profile, or geo routing;
- no built-in receiver replay store: the SDK verifier returns the identity the
  receiver must persist;
- no exactly-once external-side-effect claim.

The routing boundary remains [ADR-0020](adr/0020-regional-event-bus-v1-and-sdk-routing.md);
signed delivery is defined by
[ADR-0030](adr/0030-leader-owned-signed-webhook-delivery.md), and Epoch-target
delivery by
[ADR-0031](adr/0031-leader-owned-epoch-target-delivery.md). The lower-level
state machine is documented in [BUS_TABLET.md](BUS_TABLET.md).
