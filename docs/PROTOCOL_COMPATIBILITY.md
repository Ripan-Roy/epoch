# Redis, Kafka, and RabbitMQ compatibility

Epoch's `epoch-compat` process accepts selected existing wire protocols and
translates them into the authenticated regional Cache, Stream, and Queue APIs.
It is a stateless gateway, not a second storage engine: routing, replication,
durability, expiry, offsets, leases, and recovery remain owned by Epoch.

This document is the public compatibility contract for the beta implementation.
Anything not listed as supported is unsupported, even if a client can encode it.
The component boundary is recorded in
[ADR-0042](adr/0042-bounded-protocol-compatibility-gateways.md).
The private-beta additions are fixed by
[ADR-0046](adr/0046-private-beta-protocol-compatibility.md). Transaction,
Pub/Sub, Streams, idempotent-producer, and durable-AMQP decisions are recorded
in [ADR-0049](adr/0049-core-protocol-semantics.md).

## Status and version targets

| Ecosystem | Wire target | Client conformance target | Status |
|---|---|---|---|
| Redis | RESP2 and RESP3 negotiation | Redis 8.8.2 `redis-cli` | Partial; bounded transactions, Pub/Sub, Streams, strings, TTL, and structures |
| Kafka | Kafka broker protocol | Apache Kafka Java client 4.3.1 | Partial; idempotent producer, manual/classic consumer, offsets, and bounded static identities |
| RabbitMQ | AMQP 0-9-1 | RabbitMQ Java client 5.35.0 | Partial; durable topology, four exchange kinds, Queue lifecycle, named DLX, and `x-death` |

The versions above are pinned and executed by CI, not a promise that every
operation in those releases is implemented. In addition to fast wire fixtures
and native adapter contracts, the regional campaign runs these clients through
the production gateway image and authenticated, replicated Cache/Stream/Queue
tablets. It checks gateway replacement, each profile's leader loss, and all
voters reopening their existing volumes after SIGKILL. See
[ADR-0043](adr/0043-lossless-protocol-recovery-contract.md).

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
  --amqp-topology-cache sessions \
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
| Strings | `GET`, `SET`, `MGET`, `MSET`, `MSETNX`, `DEL`, `EXISTS`, `TYPE` | `SET` supports `NX`, `XX`, `GET`, `EX`, and `PX`; `MSET`/`MSETNX` commit all keys atomically; `MGET`, `DEL`, and `EXISTS` are bounded multi-key observations/mutations without a cross-command snapshot |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY` | Signed 64-bit integer range |
| Expiry | `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `PERSIST` | Absolute-time options and conditional expiry flags are unsupported |
| Transport | RESP2/RESP3, binary-safe values, pipelining | Keys must be UTF-8; TLS is expected at a private proxy/ingress in this revision |
| Data structures | Hashes (`HSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`), lists (`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`), sets (`SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`), and sorted sets (`ZADD`, `ZREM`, `ZCARD`, `ZSCORE`, `ZRANGE [WITHSCORES]`) | Structured fields and members must be UTF-8 and each collection is bounded to 1,024 items; Redis bitmap/module commands remain unsupported |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` over the listed string, counter, expiry, and collection commands | At most 128 queued commands and 128 distinct UTF-8 keys; one Cache shard/revision; Pub/Sub and Stream commands cannot be queued |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE` | Node-local, at-most-once, and non-replayable; at most 64 channel/pattern filters per connection; a gateway restart or reconnect loses the subscription |
| Streams | `XADD`, `XLEN`, `XRANGE`, `XREAD`, `XGROUP CREATE/DESTROY/SETID`, `XREADGROUP`, `XACK`, `XPENDING` | Existing Epoch Stream, shard 0, auto-generated IDs only; one-stream new-entry group reads; no trim, delete, claim, or multi-shard Redis semantics |
| Atomic programs | Not exposed | Lua, functions, modules, and script cache operations are unsupported |

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

`SET ... GET` returns the value at the same linearization point as the set,
including when `NX` or `XX` prevents the write. A non-string old value returns
`WRONGTYPE` without modifying it. The regional adapter implements this with a
version/revision-fenced compare-and-set and at most four contention retries;
exhaustion returns a retryable error rather than an unrelated previous value.
Backend read errors are never treated as a missing key.

Structured mutations use the same linearizable native observation boundary. A
missing collection is created with a shard-revision compare-and-set, an existing
collection is replaced under its value-version fence while retaining TTL and
storage class, and removal of the final item deletes the key. Wrong-type and
no-op operations never write. Conflicts are retried at most four times.

`MSET` and `MSETNX` normalize at most 128 distinct UTF-8 keys and submit one
native Cache transaction. `MSETNX` first observes the complete key set and uses
missing-value plus shard-revision fences, returning `0` without mutation when
any key exists. A concurrent revision change retries the whole operation at
most four times. Repeated keys are accepted and the final command value wins.

`MULTI` queues syntactically valid supported commands and `EXEC` evaluates them
in order against one observed Cache image. The resulting mutations commit in
one revision-fenced native transaction, so no intermediate state is visible.
Queue-time errors cause `EXECABORT`; runtime command errors remain in their
result positions while later queued commands still execute. `WATCH` stores
per-key versions, so unrelated Cache mutations do not abort the transaction;
change, deletion, expiry, or recreation of a watched key returns a null `EXEC`.

Redis Pub/Sub deliberately maps to Epoch Cache Pub/Sub: it is transient,
node-affine, at-most-once delivery. Redis Streams maps to a provisioned Epoch
Stream and is the durable/replayable option. Stream fields are binary-safe and
stored in a versioned envelope; consumer-group cursor and pending-entry state is
stored under reserved hashed keys in the configured replicated Cache. Each
group retains at most 4,096 pending entries, reads return at most 1,000 entries,
and blocking waits are capped at 30 seconds. Applications must not mutate
reserved compatibility keys.

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
| JoinGroup | 0–9 | Classic `consumer` membership joins the replicated native Stream session coordinator |
| SyncGroup | 0–5 | Native deterministic shard assignments become Kafka consumer assignments and install generation-fenced claims |
| Heartbeat | 0–4 | Refreshes the replicated session under its current generation |
| LeaveGroup | 0–5 | Removes members through the replicated session coordinator |
| OffsetCommit | 2–9 | Manual next offsets or claimed group-member offsets become durable Epoch checkpoints |
| OffsetFetch | 1–7 | Reads requested durable checkpoints; missing offsets return `-1` |
| InitProducerId | 0–5 | Allocates or rotates a non-transactional producer ID/epoch used by sequence-fenced Produce batches |

Current Kafka boundaries:

- manual assignment and classic `subscribe()` groups are supported; a classic
  member must subscribe to exactly one existing Epoch Stream and offer the
  `range` assignment protocol;
- assignments cover any configured Stream shard count and are sourced from the
  replicated native session coordinator. Sync installs per-shard ownership
  claims, and group commits are fenced by member ID and generation;
- a bounded `group.instance.id` is encoded into the native member identity and
  is checked across join, sync, heartbeat, commit, and leave. The same static
  identity can rejoin without a generation change. Simultaneous duplicate-live
  owner fencing is not yet represented by a separate native owner epoch;
- regex/multi-topic subscriptions, cooperative assignment, and Kafka's newer
  consumer-group protocol are not yet exposed;
- non-transactional idempotent producers are supported. Each partition request
  must use one producer/epoch and contiguous record-batch sequences; an exact
  retry returns the original base offset, while gaps, conflicts, and stale
  epochs return `OUT_OF_ORDER_SEQUENCE_NUMBER`;
- transactional IDs/producers, control batches, admin mutations, SASL, ACL
  APIs, topic creation/deletion, and timestamp offset lookup are unsupported;
- `acks=0` emits no response; other acknowledged writes complete only after the
  native Stream mutation succeeds;
- one Produce partition is submitted as one canonical native batch and becomes
  visible atomically; the translated batch must contain 1–1,000 records, fit
  4 MiB uncompressed, and fit the native 360 KiB compressed proposal boundary;
- record keys, nullable values, producer CreateTime timestamps, and ordered
  duplicate/nullable headers round-trip through a
  namespaced Epoch envelope; protocol-only broker metadata does not.

New native Kafka envelopes use payload `format_version: 2` and an ordered
header-pair array. Legacy unversioned header maps remain readable, but headers
already discarded by an older gateway cannot be recovered. Do not downgrade
the gateway after writing v2 envelopes; a mixed-version gateway rollback window
is not supported. Header counts are capped at 1,024 per record.

Native idempotent multi-record append uses Stream state-command version 8.
Version-7 state commands remain readable, while a batch labeled as version 7 is
rejected. This is an additive durable-format boundary; do not downgrade a voter
after version-8 commands have committed.

Java classic consumer-group example:

```java
var consumer = new KafkaConsumer<byte[], byte[]>(Map.of(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092",
    ConsumerConfig.GROUP_ID_CONFIG, "billing",
    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "billing-worker-a",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
));
consumer.subscribe(List.of("events"));
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
| Topology | Existing Queue declaration; direct, fanout, topic, and headers exchanges; durable exchanges/bindings persisted in the configured Cache; multi-queue bind/unbind; exchange delete | Non-durable topology remains process-local; auto-delete exchanges, server-named queues, and policies are rejected; headers bindings accept only string criteria and `x-match=all|any` |
| Publish | Default/direct/fanout/topic/headers routing, content body, content type, correlation ID, reply-to, UTF-8 string headers, per-message expiration, and mandatory `basic.return` | Non-string headers and immediate publishing are unsupported |
| Reliability | Publisher confirms after native Queue commit | AMQP transactions are unsupported |
| Consume | `basic.consume`, `basic.cancel`, `basic.get`, `basic.qos`, automatic or manual ack | Push consumers poll the native Queue; consumer priority/exclusive arguments are unsupported |
| Settlement | `basic.ack`, `basic.reject`, `basic.nack`; requeue maps to release; default or named DLX declarations may resolve to the provisioned native dead-letter target; bounded `x-death` table arrays | A named DLX route must resolve to exactly that one target Queue; arbitrary DLX fanout and lease renewal over AMQP are unsupported |

Native Queue capacity, visibility, and retry policy remain authoritative.
Durable exchange and binding metadata is stored in the Cache selected by
`--amqp-topology-cache` (or `EPOCH_COMPAT_AMQP_TOPOLOGY_CACHE`); by default it
uses `--redis-cache`. Updates use revision-fenced compare-and-apply and are
bounded to 4,096 exchanges, bindings, and dead-letter declarations per
collection. This Cache must be durable and reachable by every gateway instance.
Non-durable declarations remain local to one gateway process.

Per-message AMQP expiration is translated to native Queue `ttl_ms`; original
exchange, routing key, expiration text, and supported headers survive delivery.
Requeue consumes another delivery attempt; once the configured retry ceiling
is exhausted, native dead-letter handling applies. A Queue declaration may use
`x-dead-letter-exchange=""` and an `x-dead-letter-routing-key` exactly equal to
its provisioned native `advanced.dead_letter_target`, or a named direct/fanout/
topic exchange route that resolves to exactly that target. Reject/nack without
requeue is committed to the native dead-letter history and forwarded through
its durable outbox. Forwarding removes the old expiration, preserves the named
exchange/routing key when configured, and exposes `x-first-death-*`,
`x-last-death-*`, plus a bounded 32-entry `x-death` table-array history with
coalesced queue/reason/exchange counts. A full Queue is not
publisher-confirmed. In this revision a native rejection closes the AMQP
connection; clients must treat unconfirmed publications as unsuccessful or
unknown rather than infer acceptance from TCP delivery.

RabbitMQ Java client example:

```java
var factory = new ConnectionFactory();
factory.setHost("127.0.0.1");
factory.setPort(5672);
factory.setUsername("epoch");
factory.setPassword("local-amqp-password");
try (var connection = factory.newConnection(); var channel = connection.createChannel()) {
  channel.queueDeclare("jobs", true, false, false, Map.of(
      "x-dead-letter-exchange", "",
      "x-dead-letter-routing-key", "failed-jobs"));
  channel.exchangeDeclare("epoch.events", "topic", false, false, Map.of());
  channel.queueBind("jobs", "epoch.events", "orders.*");
  channel.confirmSelect();
  var properties = new AMQP.BasicProperties.Builder().expiration("30000").build();
  channel.basicPublish(
      "epoch.events", "orders.created", true, properties,
      "work".getBytes(StandardCharsets.UTF_8));
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
- Kafka maps missing resources to `UNKNOWN_TOPIC_OR_PARTITION`, ordinary routing
  conflicts to `NOT_LEADER_OR_FOLLOWER`, idempotent sequence/epoch conflicts to
  `OUT_OF_ORDER_SEQUENCE_NUMBER`, oversized messages to `MESSAGE_TOO_LARGE`,
  and backend outages to `BROKER_NOT_AVAILABLE`.
- AMQP malformed, unauthenticated, out-of-order, oversized, and unsupported
  frames fail closed. Client automatic recovery may reconnect, but must not
  infer that an unconfirmed publish committed.

A native HTTP success status can carry a durably committed rejection. The
gateway inspects the receipt and never treats that as an applied Cache or Queue
mutation; unknown or missing receipt outcomes also fail closed.

The gateway generates a fresh native idempotency identity per ordinary
translated mutation. A connection loss after an uncertain native response is
therefore an unknown outcome unless the client received the protocol
acknowledgement. Kafka idempotent Produce is the exception: its producer ID,
epoch, and sequence derive a stable native identity and exact retries resolve to
the original offset. Do not blindly retry other non-idempotent application
writes without an application deduplication key.

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

The repeatable real-cluster command and evidence fields are documented in
[Testing](TESTING.md#5-protocol-compatibility). Its Kafka history checks cover
all four codecs, null keys/values, duplicate/null headers, CreateTime, exact
offsets, a persisted consumer checkpoint, and static identity rejoin; the
exact-client fixture additionally enables the Kafka idempotent producer. Redis
fixture evidence covers atomic transactions, live Pub/Sub, and the documented
Streams group lifecycle. AMQP checks cover four exchange kinds, confirms,
native named-DLX forwarding with `x-death`, capacity rejection, nack/requeue,
disconnected-lease redelivery, and acknowledged messages staying absent after
restart.

The current production-image regional-v2 candidate passes all 21 local checks
with Redis CLI 8.8.2, Kafka Java 4.3.1, and RabbitMQ Java 5.35.0. This includes
gateway replacement, separate Cache/Stream/Queue leader and term changes,
replica convergence, and all-voter SIGKILL/same-volume reopen. Protected CI is
still required for promotion.

Performance parity with Redis, Kafka, or RabbitMQ is not claimed by this beta
slice. Comparative throughput and p99 gates in the PRD remain separate work.
