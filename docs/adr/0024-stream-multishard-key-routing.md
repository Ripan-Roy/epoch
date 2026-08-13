# ADR-0024: Multi-shard Stream key routing

- Status: Accepted
- Date: 2026-08-13
- Owners: Stream, regional runtime, and SDK maintainers

## Context

Epoch's catalog could allocate several tablets for one resource, but the
regional Stream contract and first-party SDK examples exercised only shard 0.
The Stream tablet's canonical commands also contain physical partition 0. If
that internal number escaped unchanged from every independently replicated
tablet, records from logical shards 1 and above would all claim to belong to
partition 0. That would not satisfy STREAM-001.

Key routing must be identical in Rust, Go, Java, and Python. It must also remain
safe when a resource expands between initial partition discovery and mutation
submission. Silently recomputing the target after a timeout or generation
change could duplicate one semantic append on two shards.

The existing command and snapshot formats are already compatibility-pinned.
Adding a logical shard to the shared tablet scope would change bytes for
Stream, Queue, Cache, and Event Bus aliases and would make old snapshots for
nonzero regional shards ambiguous on decode.

## Decision

### One regional shard is one ordered partition

Every regional Stream shard is an independent logical ordered partition and
maps to one independently replicated tablet/consensus group. The tablet's
canonical state machine remains a single physical partition numbered 0. The
node service binds the materialized catalog shard index as runtime metadata and
externalizes it in regional responses without storing it in canonical tablet
commands or native profile snapshots.

Mutation responses, append and batch receipts, fetched records, group
checkpoint observations/receipts, retention observations, and status therefore
report the outer logical shard. The deterministic tablet continues to validate
physical `partition: 0` internally.

### Versioned cross-language partitioner

Stream route discovery advertises:

```json
{
  "stream_partitioning": {
    "algorithm": "fnv1a64_utf8_mod_n_v1",
    "key_encoding": "utf8",
    "missing_key_fallback": "event_id",
    "shard_count": 3
  }
}
```

`fnv1a64_utf8_mod_n_v1` is unsigned FNV-1a 64 over the exact UTF-8 bytes,
followed by modulo the advertised nonzero shard count. A nonempty event key is
the partition value; a missing or empty key uses the event ID. The identifier
versions the exact offset basis, XOR/multiply order, unsigned 64-bit wrapping,
UTF-8 encoding, and modulo step.

The portable conformance vectors for 16 shards are:

| Value | Shard |
|---|---:|
| `customer-42` | 14 |
| `order-1` | 13 |
| `café` | 9 |
| `東京` | 15 |

Rust exposes `stream_partition_for`, Go exposes `StreamShardFor`, Java exposes
`StreamPartitioner.shardFor`, and Python exposes `stream_shard_for`.

### Keyed append pins resource generation

The three regional SDKs expose `AppendKeyed`, `appendKeyed`, and
`append_keyed`. A keyed append:

1. discovers shard 0 only to read the resource-wide partitioning metadata and
   resource generation;
2. validates the known algorithm, UTF-8 encoding, event-ID fallback, and
   nonzero shard count;
3. computes the target shard;
4. discovers that shard's current leader and fences; and
5. sends the ordinary physical partition-0 append only if the target route has
   the same resource generation as step 1.

A generation mismatch fails before the write. Once an operation is attempted,
ordinary bounded leader rediscovery keeps the same target shard, expected
generation, semantic request, and caller idempotency key. The client never
silently remaps an uncertain mutation.

Explicit per-shard methods remain available for callers that already own
partition assignment. They do not infer or cache a resource-wide partitioner.

### Materialization and recovery

Materialized route metadata carries the resource's shard count, while each
Stream service carries its descriptor's logical shard index. Both are rebuilt
from committed catalog state after process restart. Native Stream checkpoints
remain byte-compatible because logical routing metadata is rebound by the
materializer rather than encoded in the profile image.

## Consequences

- One key maps identically in every first-party language for a fixed resource
  generation and shard count.
- Logical response identities no longer collapse every regional Stream shard
  into partition 0.
- Command v1–v4 and native snapshot bytes remain unchanged.
- Each shard retains independent ordering, leadership, offsets, consumer
  checkpoints, retention policy, and recovery history.
- A resource-generation race is visible and fail-closed instead of becoming an
  implicit remap.

## Rejected alternatives

- Language-native hash functions: their algorithms, seeds, and string
  treatment are not a portable protocol contract.
- Persisting logical shard in `StreamTabletScope`: it changes shared canonical
  bytes and makes old nonzero-shard images decode with the wrong identity.
- Treating one inner Stream as N partitions inside one tablet: it couples
  leadership and recovery and contradicts the catalog's shard-to-tablet model.
- Recomputing after any generation change: it can place one uncertain append
  on both old and new mappings.
- Relying on shard 0 forever: it does not provide partition scale or truthful
  logical record/checkpoint identities.

## Verification

Required evidence includes:

- Rust, Go, Java, and Python tests over the same ASCII and non-ASCII vectors;
- zero-shard rejection and empty-key event-ID fallback;
- three-shard materialization, discovery metadata, logical response identity,
  native snapshot reinstall, and catalog reopen tests;
- SDK tests proving target selection and generation mismatch before write;
- a three-node container campaign routing Python keyed appends to shards 0, 1,
  and 2, then verifying per-shard checkpoint state after leader loss, voter
  return, all-node `SIGKILL`, and same-volume reopen; and
- exact three-language quickstarts compiled by CI and published on Pages.

## Non-claims

This decision does not provide online partition expansion, virtual shards,
split/merge, automatic hot-key mitigation, cross-shard transactions, producer
auto-batching, or a transparent remapping protocol. ADR-0025 subsequently adds
resource-wide session assignment for a fixed shard count, while atomic
assignment-plus-offset handoff and STREAM-011 remapping remain separate.
