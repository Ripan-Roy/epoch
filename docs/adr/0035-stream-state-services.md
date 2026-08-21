# ADR-0035: Replicated Stream state services

- Status: Accepted
- Date: 2026-08-21

## Context

Epoch's ordered Stream tablet already supplied append, retention, batches, and
consumer checkpoints. STREAM-007 through STREAM-015 require additional state
that must agree with the log through retries, leader changes, snapshots, and
full-cluster recovery. Implementing those features in SDKs, background workers,
or an external object store alone would introduce an unreplicated second source
of truth.

## Decision

One bounded `StreamStateServices` value is colocated with each single-partition
Stream tablet and changes only through the existing consensus history. Command
format v7 covers producer sequencing, transactions, compaction, tiering,
manual and scheduled capture, and replication ingress. Tablet snapshot v4
stores a separately versioned canonical state-services snapshot and validates
all references against the ordered log before encoding and after decoding.

Every advanced mutation is applied to cloned log and service state. Both
snapshots and their cross-component invariants must validate before either
clone is published. A capacity or integrity failure therefore cannot leave a
state that commits successfully but fails at the next checkpoint.

Because Raft may commit a command whose state-dependent precondition is no
longer true, ordinary domain failures become typed committed `Rejected`
results. They advance the replicated command history and digest, are exactly
replayable and snapshot-restorable, and publish neither staged clone. Storage
and internal invariant failures remain fatal so corruption is never disguised
as a business rejection.

The contracts are:

- Producer epochs fence old writers; sequences are contiguous and an exact
  retry returns its original positions.
- Transactions are tablet-local, bounded to 128 records, hide pending and
  aborted records under read-committed, and may commit one colocated consumer
  offset atomically.
- Key compaction retains the latest committed value, unkeyed records, and
  unexpired tombstones. Immutable historical objects require compaction first.
- Tier objects record their complete covered offset range, canonical bytes,
  and SHA-256 checksum. Historical fetch verifies and merges them with hot
  records while applying the requested transaction isolation.
- Capture artifacts use canonical JSON Lines or JSON arrays. A replicated
  schedule owns its next offset and deadline; a pending transaction stops the
  boundary, and only the current regional leader proposes due maintenance.
- Replication ingress requires contiguous source offsets, persists the source
  checkpoint and local mapping atomically, returns exact retries, and rejects a
  path containing the local cluster.
- Partition advice is pure and expand-only. Existing catalog expansion keeps
  old tablet identities, adds new shards, and changes the resource generation.
- A superstream is an SDK merge over independently linearizable member reads,
  ordered by append time, member, partition, and offset. It is not a global
  snapshot.
- Push and dedicated modes are bounded HTTP long polls on separate notification
  lanes. They do not imply a bandwidth SLO.

Wire-facing unsigned 64-bit values serialize as decimal strings and accept
canonical numbers or decimal strings on input. State cardinality, request size,
artifact size, interval, history, and snapshot bytes are all bounded.

## Consequences

The complete development feature is deterministic, locally runnable, and
recoverable without a second database. Go, Java, and Python share one route and
semantic contract. Exact replay and snapshot recovery include the advanced
state in the tablet digest and retry registry.

The alpha retains tier and capture object bytes inside replicated state. A
cloud object-store adapter may mirror or offload those immutable bytes later,
but it cannot become the correctness authority. External-store outage and
latency evidence, a deployment-specific cross-region worker, two-region
RPO/RTO drills, dedicated-throughput benchmarks, and atomic cross-shard
transactions remain production-readiness gates rather than hidden claims.
