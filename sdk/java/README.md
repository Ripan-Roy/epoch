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
volatile in the runnable slice. The SDK does not yet provide native gRPC
streaming, automatic retries, or a stable compatibility promise.

Run its complete format, compiler-lint, Checkstyle, unit, transport, and package
gate with the checksum-pinned Maven wrapper:

```shell
./mvnw verify
```
