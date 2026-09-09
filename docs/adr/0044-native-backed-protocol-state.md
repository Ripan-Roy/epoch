# ADR-0044: Native-backed protocol state

- Status: Accepted
- Date: 2026-09-08
- Owners: data plane, protocol compatibility, verification
- Extends: [ADR-0042](0042-bounded-protocol-compatibility-gateways.md) and
  [ADR-0043](0043-lossless-protocol-recovery-contract.md)
- Extended by: [ADR-0046](0046-private-beta-protocol-compatibility.md)

## Context

The first bounded protocol gateway proved connection, parser, translation, and
recovery contracts for Redis strings, manually assigned Kafka partitions, and
direct AMQP Queue delivery. Useful client migrations also need structured Cache
operations, consumer-group coordination, and broker-style routing. Implementing
those features only in a gateway-local map would make acknowledged state vanish
on restart or let two gateways assign the same durable work.

Not every protocol concept has an equivalent native resource. Cache and Stream
already provide replicated values, versions, expiry, sessions, ownership, and
checkpoints. AMQP exchange declarations and bindings have no native management
resource yet, although routed messages and their delivery lifecycle do map to
Queue. The public boundary must distinguish those cases rather than imply full
Redis, Kafka, or RabbitMQ behavior.

## Decision

1. Encode each supported Redis hash, list, set, or sorted set as one bounded
   structured Cache value. A mutation performs a linearizable observation,
   computes one replacement, and submits one version fence; a missing value uses
   the observed shard revision. A successful replacement preserves native TTL
   and storage class, an empty result deletes the item, contention retries are
   bounded, and a type error or no-op never mutates state.
2. Support only the classic Kafka `consumer` group with one topic per member and
   the `range` assignment strategy. Join, observe, heartbeat, leave, per-shard
   claim, and offset commit use replicated Stream session state. The generation
   fences heartbeat, ownership, and commit. The assignment covers the Stream's
   discovered shard count, so correctness does not depend on a fixed three-node
   or three-partition topology.
3. Keep Kafka gateway connection state disposable. A bounded opaque member token
   identifies the native Stream and member UUID, allowing any gateway instance
   to recover the replicated session. Static membership, multiple topics,
   regular-expression subscriptions, cooperative rebalancing, the newer group
   protocol, transactions, and idempotent producer identities fail explicitly.
4. Share the supported AMQP exchange and binding topology between connections in
   one gateway process. Support built-in or custom non-durable, non-auto-delete,
   argument-free direct, fanout, and topic exchanges, including bind, unbind,
   and delete. This topology is not replicated and does not survive a gateway
   restart; applications must redeclare it after connecting.
5. Route each AMQP publish to every distinct matching Queue and enqueue each copy
   through the native port. A mandatory publish with no route emits
   `basic.return`; a non-mandatory miss is dropped. Publisher confirms describe
   the native enqueue result. Decimal per-message expiration becomes native
   Queue TTL, and the current envelope carries only bounded UTF-8 string headers
   plus original exchange and routing key.
6. Keep protocol discovery, the migration scanner, examples, SDK conformance,
   and the regional fault campaign aligned with this exact subset. Released
   Redis CLI, Kafka Java, and RabbitMQ Java clients must exercise the new paths;
   fixtures alone are insufficient evidence for promotion.

## Consequences

Redis structures and Kafka ownership survive gateway replacement and regional
leader or full-voter recovery because the native profiles remain authoritative.
Stale Kafka members cannot acknowledge work after a rebalance. Redis collection
commands are atomic per key but do not create multi-key transactions.

AMQP delivery data, TTL, leases, acknowledgements, and redelivery are durable,
but exchange and binding declarations are only process-scoped. Running multiple
gateway replicas therefore requires clients to declare equivalent topology on
each target or a future replicated topology resource. Header exchanges, durable
topology, policies, dead-letter configuration, transactions, AMQP 1.0, broader
Redis commands, and broader Kafka group protocols remain explicit future gates.

## Rejected alternatives

- Gateway-local Redis structures or Kafka groups were rejected because restart
  and horizontal scaling would violate acknowledged atomicity and ownership.
- One native Cache item per hash field or collection member was rejected because
  the current profile has no cross-item transaction that could implement one
  Redis command atomically.
- A gateway-computed Kafka assignment without native claims was rejected because
  stale generations could continue consuming or committing the same shard.
- Advertising durable AMQP declarations over process memory was rejected because
  it would turn successful declarations into data-loss promises on restart.
