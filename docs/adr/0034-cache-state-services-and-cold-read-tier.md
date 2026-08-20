# ADR-0034: Cache state services and cold read tier

**Status:** Accepted

## Context

The first regional Cache slice proved scalar values, optimistic transactions,
expiry, deterministic entry-count eviction, fenced locks, and recovery. The
remaining non-deferred Cache requirements need one coherent persisted model:
byte admission, collection and advanced mutations, a durable change cursor,
exportable point-in-time recovery, a deliberately lossy notification path, and
a different read class for cold values. Implementing any of these as node-local
business state would make voters disagree or would make recovery claims false.

The Cache also needs both atomic batching and partial-success pipelining. Those
are different contracts and must not share a misleading name. Pub/Sub has the
opposite durability requirement from the change stream: it is useful precisely
as a node-local low-overhead notification path, provided loss and affinity are
visible.

## Decision

1. `CacheShard` remains the canonical deterministic state owner. Snapshot v4
   records memory/cold storage class, per-class retained bytes, advanced values,
   the bounded change log, and restoration history. Readers accept v1 through
   v4. Legacy no-tier snapshots and digests remain compatible.
2. Admission enforces `max_entries`, `max_memory_bytes`, and
   `max_cold_bytes`. Each command stages expiry, deterministic eviction, byte
   accounting, and mutation as one transition. The command either fits its
   selected class or rejects without partial state.
3. `quorum_durable` and `replicated_memory` are named regional contracts. The
   current fixed three-voter persisted runtime fulfills `replicated_memory`
   with the stronger quorum-durable implementation and reports both requested
   and achieved durability. It never reports the weaker label as the achieved
   guarantee.
4. Collection operations and bitmap, cardinality, Bloom, Cuckoo, geo, JSON,
   JSON-index, and vector-index operations use a typed `Transform` command.
   Every transform has deterministic bounds and participates in CAS, TTL,
   transactions, snapshots, change records, replay, and the state digest.
   Queries are exact and linearizable in this alpha; vector search is bounded
   exhaustive cosine/text hybrid search rather than an approximate index.
5. The replicated change stream retains the newest 1,024 mutation, expiry,
   eviction, and restore records. A cursor older than the published floor fails
   instead of skipping silently.
6. A canonical checksummed backup is bounded to 320 KiB so its base64 form can
   be restored in one existing consensus proposal. It contains the snapshot and
   retained history window. Restore validates format, digest, configuration,
   byte limits, TTL at restore time, and target revision, then commits one
   atomic replacement with fresh non-ABA versions. This is a resource-local,
   caller-managed artifact—not scheduled backup, encryption, remote retention,
   or disaster-recovery orchestration.
7. Pub/Sub subscriptions, patterns, queues, and sequence numbers are node-local
   memory. Delivery is at-most-once, polling drains messages, overflow drops the
   new message and increments a visible counter, disconnect/node change loses
   state, and callers must retain node affinity. Durable consumers use the
   change stream or Event Bus instead.
8. `AtomicBatch` remains one all-or-nothing consensus transaction.
   `Multiplex` accepts one to 128 independently identified mutations in one HTTP
   request and returns request-ordered correlated outcomes. The server validates
   the whole envelope before the first proposal, but item commits are explicitly
   non-atomic and exact retries retain each item's idempotency identity.
9. A regional voter materializes cold-class values as canonical, fsynced,
   per-key local files after committed apply. Cold observations and queries read
   and integrity-check that file against replicated canonical state. Delete,
   eviction, restore, replay, and checkpoint installation synchronize the
   directory before serving. Status reports the backend and observed local-file
   read microseconds, explicitly not an SLO.

## Consequences

- Every non-deferred Cache capability shares one recovery and idempotency model,
  and Go, Java, and Python expose the same lifecycle.
- Byte caps are deterministic serialized-value admission limits. They are not a
  process RSS limit or a production memory benchmark.
- The cold file is a real fsynced read tier, but canonical state is still kept
  in the replicated tablet image. This alpha does not claim heap offload,
  production flash capacity relief, remote object storage, or a latency SLO.
- Backup/PITR is immediately testable and portable within a matching resource
  configuration, while managed scheduling, encryption, catalogs, retention,
  cross-resource restore, and all-voter disaster recovery remain managed
  operations work.
- Multi-shard Cache routing, automatic client coalescing, Redis/RESP
  compatibility, CRDTs, and package-registry publication remain outside this
  decision.
