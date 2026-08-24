# ADR-0042: Bounded protocol-compatibility gateways

- Status: Accepted
- Date: 2026-08-24
- Owners: data plane, protocol compatibility, security

## Context

Epoch needs credible migration paths for applications that already speak Redis,
Kafka, or RabbitMQ protocols. Reimplementing those systems inside the replicated
engines would create a second source of truth, couple storage correctness to
large adversarial parsers, and imply compatibility beyond what Epoch can prove.
Routing protocol traffic directly to tablet-private endpoints would also bypass
the regional authentication, resource-generation, tablet-epoch, leader-term,
and linearizable-read contracts.

Compatibility is narrower than syntax. A successful wire response must preserve
the documented operation's durability and retry meaning even when the original
system and Epoch have different resource or atomicity models. Claims therefore
need exact client versions, executable conformance, and explicit unsupported
boundaries.

## Decision

1. `epoch-compat` is a separate stateless Rust process. Its RESP2/RESP3, Kafka,
   and AMQP 0-9-1 listeners depend only on a narrow asynchronous Cache, Stream,
   and Queue semantic port. Protocol parsing and connection state never enter a
   replicated state machine.
2. The production port uses the authenticated regional HTTP API. Every operation
   discovers its resource shard, supplies resource-generation and tablet-epoch
   fences, supplies the leader term for mutations, and requests linearizable
   reads. The gateway identity is distinct from the client-to-gateway
   credential; backend and AMQP secrets are required inputs and have no embedded
   defaults or unredacted debug representation.
3. Every listener has bounded frames, message bodies, logical item counts,
   connections, channels, prefetch, and server-side polling. Kafka record counts
   are checked before decoder allocation. None, gzip, Kafka Snappy, LZ4, and
   Zstandard record data share a 4 MiB cumulative decompression ceiling;
   Zstandard additionally has a bounded window.
4. One Kafka Produce partition request becomes one canonical gzip Epoch Stream
   batch and one native consensus mutation. The native 1,000-record, 4 MiB
   uncompressed, and 360 KiB compressed limits are enforced before submission.
   This gives whole-batch visibility for that partition. Kafka transactions,
   control batches, and idempotent-producer state remain unsupported rather than
   being weakened silently.
5. One AMQP publish becomes one Queue enqueue and is confirmed only after the
   native mutation succeeds. A delivery settlement uses the exact native lease
   token. Redis multi-key commands are explicitly independent per key in this
   revision; they do not claim Redis transaction atomicity.
6. Unknown native mutation outcomes are not retried automatically with a fresh
   identity. The gateway returns or closes according to the source protocol,
   and the public contract tells clients when application deduplication is
   required.
7. Advertised APIs are an allowlist. Kafka `ApiVersions`, Redis `COMMAND`, and
   the public matrix must agree with implemented dispatch. The initial release
   gate pins Redis CLI 8.8.2, Apache Kafka Java client 4.3.1, and RabbitMQ Java
   client 5.34.0; passing those clients proves only the documented subset.
8. The compatibility scanner consumes a bounded feature manifest and emits the
   versioned `epoch.compatibility-scan/v1` report. It is a conservative planning
   aid, not live traffic capture or semantic certification.

## Consequences

The data plane retains one authority for durability, expiry, offsets, leases,
and recovery. Gateway instances can scale or restart independently, and their
wire parsers can be fuzzed and upgraded without changing persisted formats.
Exact native batch decoding proves that Kafka translation produces the same
canonical payload accepted by the tablet boundary.

The approach deliberately exposes narrower behavior than Redis, Kafka, and
RabbitMQ. It requires a resource to exist in Epoch, maps Kafka partitions to
independent Stream shards, limits Redis to database 0, and supports only direct
AMQP routing. Client-side TLS/SASL, richer data types, Kafka group membership and
transactions, broader AMQP exchanges/1.0, MQTT, differential testing, fuzzing,
combined real-cluster certification, and performance evidence remain separate
promotion gates.

## Rejected alternatives

- Embedding protocol parsers in tablets was rejected because malformed network
  input and session state must not affect deterministic replication.
- Persisting a second Redis/Kafka/RabbitMQ model was rejected because divergent
  state and recovery semantics would make acknowledgements ambiguous.
- Advertising broad protocol versions with runtime `unsupported` responses was
  rejected because discovery must describe only implemented handlers.
- Retrying uncertain mutations with newly generated identities was rejected
  because it can duplicate non-idempotent increments, publishes, and appends.
