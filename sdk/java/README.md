# Epoch Java SDK

This pre-alpha Java 25 client covers every native HTTP route currently exposed
by the standalone Epoch node. Requests use immutable, typed models; responses
remain Jackson `JsonNode` values until the public wire contract stabilizes.

```java
import io.epoch.sdk.DurabilityProfile;
import io.epoch.sdk.EpochClient;
import io.epoch.sdk.EventEnvelope;
import io.epoch.sdk.QueueConfig;
import io.epoch.sdk.StreamConfig;
import java.util.Map;

EpochClient client = new EpochClient();
client.createStream(
    "orders", new StreamConfig(4, DurabilityProfile.LOCAL_DURABLE, null));
client.createQueue(
    "jobs", new QueueConfig(DurabilityProfile.LOCAL_DURABLE, 30_000, 100_000, 8));
client.appendStream(
    "orders",
    EventEnvelope.builder("checkout", "order.created", Map.of("id", "1001"))
        .build());
```

`LOCAL_DURABLE` currently means fsync and recovery on one node; it does not
provide replication or protection from losing that host and its storage. Queue
messages and transitions use the same boundary; Cache and Event Bus remain
volatile in the runnable slice. Standalone calls perform no hidden retries.

`RegionalStreamClient` is the explicit replicated alternative. It accepts
every Rust node endpoint plus a `RegionalScope` and bearer token, discovers the
current leader before each call, carries generation/tablet fences, reuses the
caller's append/checkpoint idempotency key across one bounded rediscovery, and
requests linearizable fetch/lag reads. See the
[complete regional example](../../console/src/quickstarts/regional/RegionalQuickstart.java)
and [contract guide](../../docs/REGIONAL_STREAM_SDK.md).

`RegionalQueueClient` applies that same discovery, bearer, fence, same-key
rediscovery, and linearizable-read contract to the complete replicated Queue
lifecycle. Java exposes `BigInteger` overloads where Queue fields span the
unsigned 64-bit wire range. See the
[complete Queue example](../../console/src/quickstarts/regional_queue/RegionalQueueQuickstart.java)
and [Queue contract guide](../../docs/REGIONAL_QUEUE_SDK.md).

The SDK does not yet provide native gRPC streaming, coordinated consumer
sessions, generated response types, package publication, or a stable complete
compatibility promise.

Run its complete format, compiler-lint, Checkstyle, unit, transport, and package
gate with the checksum-pinned Maven wrapper:

```shell
./mvnw verify
```
