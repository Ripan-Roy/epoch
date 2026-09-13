# ADR-0049: Core protocol semantics

- Status: Accepted
- Date: 2026-09-13
- Owners: protocol compatibility, data plane, verification
- Extends: [ADR-0046](0046-private-beta-protocol-compatibility.md)

## Context

The first private-beta compatibility slice covered common Redis structures,
Kafka producer/consumer traffic, and the AMQP Queue lifecycle, but left several
migration-critical behaviors explicit and unsupported. Redis applications could
not use transactions, Pub/Sub, or Streams. Kafka clients could not enable the
standard idempotent producer. Durable AMQP exchange/binding declarations were
lost on gateway replacement, and provisioned dead letters lacked named-exchange
routing and RabbitMQ-shaped `x-death` history.

These features cannot be implemented as protocol-only acknowledgements. Their
atomicity, durability, replay, and fencing must map to an existing Epoch engine
with bounded persisted state.

## Decision

1. Implement Redis `MULTI`/`EXEC` as an ordered planner over one Cache snapshot.
   At most 128 supported commands and 128 distinct UTF-8 keys become one
   revision-fenced Cache mutation. Queue-time validation errors cause
   `EXECABORT`; runtime command errors occupy their ordered result position.
   `WATCH` records key versions rather than a shard revision, so unrelated
   writes do not abort the transaction while delete/recreate ABA remains fenced.
2. Map Redis Pub/Sub to native Cache Pub/Sub. It is node-local, node-affine,
   at-most-once, non-replayable, and limited to 64 exact/pattern filters per
   connection. Map Redis Streams to existing native Stream shard 0. Stream
   entries are durable and replayable; versioned binary field envelopes live in
   the Stream while bounded consumer-group cursor and pending state lives in a
   replicated Cache document. Group reads currently support one stream, the
   `>` cursor, 4,096 pending entries, and waits no longer than 30 seconds.
3. Advertise Kafka `InitProducerId` versions 0–5 only for non-transactional
   identities. A Produce partition must contain one producer ID/epoch and a
   contiguous sequence span. Submit all decoded records as one native
   idempotent Stream batch. Exact retry returns the original offset; stale
   epoch, gap, or conflicting replay maps to `OUT_OF_ORDER_SEQUENCE_NUMBER`.
4. Introduce Stream state-command version 8 for atomic idempotent batches.
   Continue decoding version-7 state commands, but reject the new batch action
   when it is labeled version 7. Existing snapshots and histories remain
   readable; rolling downgrade after a v8 commit is unsupported.
5. Persist durable AMQP exchanges, bindings, and Queue dead-letter declarations
   in a canonical, versioned document under a configured replicated Cache.
   Mutations use revision-fenced compare-and-apply with bounded retries and at
   most 4,096 entries in each topology collection. Non-durable declarations
   remain process-local.
6. Accept a named AMQP DLX only when its current route resolves to exactly the
   Queue's provisioned native dead-letter target. The replicated Queue outbox
   remains the sole forwarding authority. Forwarded messages remove expiration,
   carry first/last death metadata, and reconstruct a bounded 32-entry `x-death`
   field-table array with coalesced queue/reason/exchange counts.
7. Reserve the `__epoch:` Cache key prefix from Redis client access. The gateway
   fails direct, watched, and queued access closed so AMQP topology and Redis
   Streams ledgers cannot be overwritten through the same Redis listener.
8. Keep Cache eviction policy explicit per resource. Replicated no-eviction,
   all-key/volatile LRU, LFU, random, and volatile TTL use exact deterministic
   ranks so all voters choose the same victim. Epoch does not infer workloads or
   automatically switch policies, and therefore does not claim adaptive cache
   policy selection.

## Consequences

Applications can use the documented Redis transient and durable messaging
models without confusing their guarantees. Kafka clients may enable
idempotence for retry-safe native batches, while Kafka transactions and control
batches still fail explicitly. Durable AMQP topology survives independent
gateway state when every instance uses the same durable topology Cache; named
DLX routing remains intentionally narrower than arbitrary RabbitMQ exchange
fanout.

Compatibility metadata shares ordinary Cache primitives and consumes its
capacity. Operators should provision a dedicated durable metadata Cache and
scope its native credentials to the gateway. Direct native administrators can
still damage that Cache, so external authorization and backup policy remain
part of the operational boundary.

Redis Lua/functions, Redis Streams trimming/claiming/multi-shard behavior,
Kafka transactions, cooperative/new consumer groups, arbitrary AMQP DLX
fanout, AMQP transactions/1.0, differential fuzzing, and comparative performance
certification remain separate gates.

## Rejected alternatives

- Executing Redis queued commands one by one was rejected because intermediate
  results would become visible and a gateway failure could partially commit.
- Implementing Redis Streams on Pub/Sub was rejected because at-most-once
  transient delivery cannot supply replay, acknowledgements, or pending state.
- Remembering Kafka producer sequences in gateway memory was rejected because
  restart would lose replay and fencing state.
- Re-publishing AMQP dead letters from the gateway was rejected because the
  acknowledge/publish crash boundary can lose or duplicate a message.
- Automatically switching Cache eviction policies was rejected because the
  nondeterministic observation loop would need a separate replicated decision,
  stability controls, operator contract, and performance evidence.
