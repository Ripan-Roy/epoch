# ADR-0043: Lossless protocol records and real regional recovery

- Status: Accepted
- Date: 2026-09-06
- Owners: protocol compatibility, data plane, verification
- Extends: [ADR-0042](0042-bounded-protocol-compatibility-gateways.md)

## Context

Separately passing a wire fixture and a native HTTP mock does not prove that
their composition preserves client semantics. The combined regional campaign
exposed three errors: Kafka Fetch substituted native append time for producer
CreateTime; map-based header representations discarded duplicate keys; and a
successful native HTTP status carrying a committed rejection could become an
AMQP publisher confirmation. A final release audit exposed another composition
error: Redis `SET ... GET` fetched its return value separately, could overwrite
a non-string before returning `WRONGTYPE`, and used the wrong field for a
missing-key revision fence.

Kafka headers are ordered and permit duplicate keys, as specified by
[KIP-82](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers).
The pinned Kafka protocol crate's `Record` represents them with an `IndexMap`,
so changing only Epoch's persisted envelope cannot correct the wire path.

## Decision

1. Retain `kafka-protocol` for requests, responses, batch metadata, and CRC
   validation. Own a narrow bounded magic-2 record-body codec in `epoch-compat`
   for ordered headers and uncompressed Fetch batches. Validate byte lengths,
   exact record boundaries, signed varints, timestamp arithmetic, and at most
   1,024 headers per record before allocating header collections. Keep the
   existing 1,000-record and cumulative 4 MiB Produce limits across codecs.
2. Preserve producer CreateTime in the native event envelope and recover it
   from `envelope.time_ms`. Native `appended_at_ms` remains native append time;
   it must not silently replace the client's timestamp. Reject unimplemented
   idempotent producer identities rather than accepting their writes without
   deduplication semantics.
3. New Kafka payloads use `format_version: 2` and an ordered array of
   `[name, nullable_base64_value]` header pairs. Read legacy unversioned header
   maps without rewriting existing logs. Unknown versions and malformed
   structures fail closed. Previously lost duplicate headers cannot be
   reconstructed. An older gateway cannot read new ordered envelopes safely;
   mixed-version gateway rollback is not a supported upgrade window.
4. Cache and Queue mutation adapters inspect the native receipt, not only the
   HTTP status. Only `receipt.outcome.status = applied` is success. Committed
   rejections map by their typed code to protocol errors; missing/unknown
   outcomes fail closed, and private native error details are not forwarded.
5. Treat Redis condition evaluation, mutation, and the optional previous value
   as one atomic compatibility operation. The native adapter reads a
   linearizable observation, uses its item version or missing-key shard revision
   in a native compare-and-set, and retries contention at most four times. It
   returns `WRONGTYPE` before mutation for a non-string `SET ... GET` source and
   never converts a failed read into absence.
6. Keep fast fixture and adapter tests, and add named real clients through the
   production gateway image against authenticated, replicated regional nodes.
   Require gateway replacement, each profile's leader loss, full-voter
   SIGKILL/same-volume reopen, Kafka metadata/checkpoint preservation, AMQP
   redelivery and durable acknowledgement, Cache binary/counter/TTL survival,
   and a full native Queue never producing a success confirmation.
7. CI reuses its already-built node and gateway images and uploads the client,
   container, and versioned fault-evidence bundle. It does not publish images
   from a PR or substitute fixture results for real regional results.

## Consequences and remaining gates

This closes an integration gap for the advertised subset, not full Redis,
Kafka, or RabbitMQ parity. Native Queue retry limits still govern AMQP requeue;
the campaign sets an explicit retry ceiling because each deliberate delivery
consumes an attempt. Unknown network outcomes are not retried with fresh
mutation identities. Differential broker tests, sustained fuzzing, full client
authentication/TLS, richer protocol features, comparative performance, and
production operating evidence remain separate requirements.

[ADR-0044](0044-native-backed-protocol-state.md) extends this evidence contract
to atomic Redis structures, replicated Kafka group generations and claims, and
AMQP routing, TTL, and mandatory-return behavior.
