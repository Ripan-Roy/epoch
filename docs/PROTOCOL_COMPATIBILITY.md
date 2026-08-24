# Redis, Kafka, and RabbitMQ compatibility

Epoch's `epoch-compat` process accepts selected existing wire protocols and
translates them into the authenticated regional Cache, Stream, and Queue APIs.
It is a stateless gateway, not a second storage engine: routing, replication,
durability, expiry, offsets, leases, and recovery remain owned by Epoch.

This document is the public compatibility contract for the beta implementation.
Anything not listed as supported is unsupported, even if a client can encode it.
The component boundary is recorded in
[ADR-0042](adr/0042-bounded-protocol-compatibility-gateways.md).

## Status and version targets

| Ecosystem | Wire target | Client conformance target | Status |
|---|---|---|---|
| Redis | RESP2 and RESP3 negotiation | Redis 8.8.2 `redis-cli` | Partial; string/counter/TTL subset |
| Kafka | Kafka broker protocol | Apache Kafka Java client 4.3.1 | Partial; producer, manual consumer, metadata, offsets |
| RabbitMQ | AMQP 0-9-1 | RabbitMQ Java client 5.34.0 | Partial; direct routing and Queue delivery lifecycle |

The versions above are pinned and executed by CI, not a promise that every
operation in those releases is implemented. The conformance job runs their
supported paths through the real wire listeners, while semantic adapter tests
separately prove authenticated, fenced native HTTP translation. A combined
real regional-cluster certification remains a beta promotion gate.

## Architecture and trust boundary

```text
Redis / Kafka / AMQP client
            |
      bounded wire parser
            |
        epoch-compat
            |
 authenticated, fenced regional HTTP
            |
 Epoch Cache / Stream / Queue tablets
```

Every listener caps connections at 1,024 by default, frames at 8 MiB, message
bodies and cumulative Kafka decompression at 4 MiB, and logical request items
at 1,024. Kafka record counts are checked before decoder allocation, and
Zstandard windows are bounded. The gateway discovers the
current tablet leader and sends the exact resource generation, tablet epoch,
and leader term required by the native API. It does not acknowledge a protocol
write before the corresponding native mutation succeeds.

Protocol credentials authenticate only the client-to-gateway hop. The gateway
uses `EPOCH_COMPAT_TOKEN` for its own scoped Epoch identity. The initial Kafka
listener has no SASL implementation, Redis authentication is optional, and
AMQP uses PLAIN credentials. The backend token and AMQP password are required
at startup and have no embedded defaults. Keep the listeners on a private
network or behind a TLS/authenticating proxy; direct public exposure is
unsupported.

## Run the gateway

Provision a Cache named `sessions`, a Stream named `events`, and a Queue named
`jobs` in `acme/shop/dev/core`, then run:

```bash
cargo run -p epoch-compat -- \
  --endpoints http://127.0.0.1:7601 \
  --token epoch-dev-admin-v1 \
  --redis-cache sessions \
  --redis-password local-redis-password \
  --kafka-advertised-host 127.0.0.1 \
  --amqp-username epoch \
  --amqp-password local-amqp-password
```

The signed non-root image exposes Redis on `6379`, Kafka on `9092`, and AMQP
0-9-1 on `5672`:

```bash
docker run --rm \
  --publish 127.0.0.1:6379:6379 \
  --publish 127.0.0.1:9092:9092 \
  --publish 127.0.0.1:5672:5672 \
  --env EPOCH_COMPAT_ENDPOINTS=http://host.docker.internal:7601 \
  --env EPOCH_COMPAT_TOKEN=epoch-dev-admin-v1 \
  --env EPOCH_COMPAT_REDIS_PASSWORD=local-redis-password \
  --env EPOCH_COMPAT_AMQP_PASSWORD=local-amqp-password \
  ghcr.io/ripan-roy/epoch-compat:<exact-version>
```

Use an exact release tag or digest. The project does not publish a mutable
`latest` tag.

## Redis command matrix

One Redis listener maps to the Cache named by `--redis-cache`. Redis database
selection is limited to database `0`; Epoch resource names replace Redis
database numbers.

| Area | Supported | Boundary |
|---|---|---|
| Connection | `HELLO 2/3`, `AUTH`, `PING`, `ECHO`, `QUIT`, `SELECT 0` | Cluster mode and alternate databases are unsupported |
| Client setup | `CLIENT SETNAME`, `GETNAME`, `ID`, `SETINFO`, `MAINT_NOTIFICATIONS`; bounded `COMMAND` metadata | Tracking and client-side caching are unsupported |
| Strings | `GET`, `SET`, `MGET`, `MSET`, `DEL`, `EXISTS`, `TYPE` | `SET` supports `NX`, `XX`, `GET`, `EX`, and `PX`; multi-key operations are independently committed |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY` | Signed 64-bit integer range |
| Expiry | `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `PERSIST` | Absolute-time options and conditional expiry flags are unsupported |
| Transport | RESP2/RESP3, binary-safe values, pipelining | Keys must be UTF-8; TLS is expected at a private proxy/ingress in this revision |
| Data structures | Not yet exposed | Hash, list, set, sorted-set, bitmap, Pub/Sub, Streams remain native-API-only |
| Atomic programs | Not yet exposed | `MULTI`/`EXEC`, Lua, functions, watches, and modules are unsupported |

Example with redis-py:

```python
import redis

client = redis.Redis(
    host="127.0.0.1",
    port=6379,
    password="local-redis-password",
    protocol=3,
    decode_responses=False,
)

client.set(b"session:42", b"binary\x00value", px=30_000, nx=True)
assert client.get(b"session:42") == b"binary\x00value"
assert client.incrby("requests", 5) == 5
```

## Kafka API matrix

Each Kafka topic name maps to an existing Epoch Stream with the same name. A
Kafka partition maps one-to-one to an Epoch Stream shard. Topics are never
auto-created by the gateway.

| API | Versions | Translation |
|---|---:|---|
| Produce | 3–9 | One partition request becomes one atomic native Stream batch; gzip, Snappy, LZ4, and Zstd are decoded under a cumulative expansion bound |
| Fetch | 4–12 | Stream offsets and records become Kafka v2 record batches |
| ListOffsets | 1–7 | Earliest (`-2`) and latest (`-1`) offsets |
| Metadata | 1–12 | Existing Stream partitions are advertised on one logical broker |
| ApiVersions | 0–4 | Advertises only handlers present in this matrix |
| FindCoordinator | 0–4 | Group coordinator resolves to the compatibility gateway |
| OffsetCommit | 2–9 | Manual-consumer next offsets become durable Epoch checkpoints |
| OffsetFetch | 1–7 | Reads requested durable checkpoints; missing offsets return `-1` |

Current Kafka boundaries:

- manual partition assignment is the supported consumer mode;
- classic and new group membership, rebalancing, and heartbeats are not yet
  advertised;
- idempotent/transactional producers, control batches, admin mutations, SASL,
  ACL APIs, topic creation/deletion, and timestamp offset lookup are unsupported;
- `acks=0` emits no response; other acknowledged writes complete only after the
  native Stream mutation succeeds;
- one Produce partition is submitted as one canonical native batch and becomes
  visible atomically; the translated batch must contain 1–1,000 records, fit
  4 MiB uncompressed, and fit the native 360 KiB compressed proposal boundary;
- record keys, nullable values, timestamps, and headers round-trip through a
  namespaced Epoch envelope; protocol-only broker metadata does not.

Java manual-consumer example:

```java
var consumer = new KafkaConsumer<byte[], byte[]>(Map.of(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092",
    ConsumerConfig.GROUP_ID_CONFIG, "billing",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
));
var partition = new TopicPartition("events", 0);
consumer.assign(List.of(partition));
consumer.seek(partition, 0L);
var records = consumer.poll(Duration.ofSeconds(1));
consumer.commitSync();
```

## RabbitMQ / AMQP 0-9-1 matrix

An AMQP queue name maps to an existing Epoch Queue with the same name.
Server-named queues are unsupported. Declarations verify native resources but
do not create or mutate them.

| Area | Supported | Boundary |
|---|---|---|
| Connection | AMQP 0-9-1 header, PLAIN, `/` vhost, tuning, heartbeats | AMQP 1.0 and TLS termination are not implemented here |
| Channels | Open and close, up to 2,048 per connection | Channel flow and recovery extensions are unsupported |
| Topology | Existing Queue declaration; connection-local direct exchange and binding | Fanout/topic/headers routing, server-named queues, policies, and arguments are unsupported |
| Publish | Default/direct exchange, content body, content type, correlation ID, reply-to | Mandatory returns and immediate publishing are unsupported |
| Reliability | Publisher confirms after native Queue commit | AMQP transactions are unsupported |
| Consume | `basic.consume`, `basic.cancel`, `basic.get`, `basic.qos`, automatic or manual ack | Push consumers poll the native Queue; consumer priority/exclusive arguments are unsupported |
| Settlement | `basic.ack`, `basic.reject`, `basic.nack`; requeue maps to release | Lease renewal is native-API-only; disconnected leases redeliver after visibility expiry |

RabbitMQ Java client example:

```java
var factory = new ConnectionFactory();
factory.setHost("127.0.0.1");
factory.setPort(5672);
factory.setUsername("epoch");
factory.setPassword("local-amqp-password");
try (var connection = factory.newConnection(); var channel = connection.createChannel()) {
  channel.queueDeclare("jobs", true, false, false, Map.of());
  channel.confirmSelect();
  channel.basicPublish("", "jobs", null, "work".getBytes(StandardCharsets.UTF_8));
  channel.waitForConfirmsOrDie(Duration.ofSeconds(5).toMillis());
  channel.basicQos(16);
  channel.basicConsume("jobs", false, (tag, delivery) -> {
    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
  }, tag -> {});
}
```

## Error and retry behavior

- Redis maps missing values to null, conflicts to command-specific null/errors,
  validation failures to `ERR`, and unavailable native routes to `TRYAGAIN`.
- Kafka maps missing resources to `UNKNOWN_TOPIC_OR_PARTITION`, fencing conflicts
  to `NOT_LEADER_OR_FOLLOWER`, oversized messages to `MESSAGE_TOO_LARGE`, and
  backend outages to `BROKER_NOT_AVAILABLE`.
- AMQP malformed, unauthenticated, out-of-order, oversized, and unsupported
  frames fail closed. Client automatic recovery may reconnect, but must not
  infer that an unconfirmed publish committed.

The gateway generates a fresh native idempotency identity per translated
mutation. A connection loss after an uncertain native response is therefore an
unknown outcome unless the client received the protocol acknowledgement. Do
not blindly retry non-idempotent application writes without an application
deduplication key.

## Scan a workload before migration

The same binary includes a bounded, read-only compatibility scanner. Feed it a
newline-delimited feature manifest rather than raw commands or payloads. In
automatic mode each line starts with `redis`, `kafka`, or `amqp`; Kafka lines
include the API version, and Redis lines may list relevant command options.

```text
# compatibility-usage.txt
redis SET NX PX
redis EVAL
kafka Produce 9
kafka JoinGroup 9
amqp basic.publish
amqp tx.commit
```

Generate the versioned JSON report and fail CI if any known-unsupported feature
is present:

```bash
cargo run -p epoch-compat -- scan \
  --format json \
  --fail-on unsupported \
  compatibility-usage.txt > compatibility-report.json
```

Use `--fail-on unknown` for a stricter review gate or `--fail-on partial` when
every boundary must be resolved before cutover. The report schema is
`epoch.compatibility-scan/v1`; every assessment retains its source line. The
scanner is conservative and does not capture live traffic or prove workload
semantics.

## Verification gates

The compatibility crate must pass format, strict Clippy, unit, malformed-frame,
wire-encoding, exact released-client, scanner, and semantic native-adapter
tests. Release promotion additionally requires combined exact-version client
conformance against a real regional Epoch cluster, an inspected non-root OCI
image, SBOMs for amd64 and arm64, and the published matrix matching the APIs
advertised by `ApiVersions` and `COMMAND`.

Performance parity with Redis, Kafka, or RabbitMQ is not claimed by this beta
slice. Comparative throughput and p99 gates in the PRD remain separate work.
