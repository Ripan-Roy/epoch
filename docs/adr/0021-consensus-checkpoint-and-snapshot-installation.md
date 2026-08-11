# ADR-0021: Consensus checkpoint and snapshot installation v1

- Status: Accepted for the fixed-voter replicated core
- Date: 2026-08-11
- Owners: Rust storage and consensus
- Requirement links: G2, G3, CACHE-008 prerequisite, MGD-006 prerequisite

## Context

Epoch's EPRS journal currently persists every Raft entry from index one and
reconstructs every applied proposal during reopen. A follower that falls behind
the retained leader log can only catch up through entries. The adapter rejects
all Raft snapshots, so it cannot compact its in-memory log, install a durable
checkpoint, or recover a lagging voter from a checkpoint plus a tail.

This blocks bounded replicated-log operation and the storage prerequisite for
profile-native snapshots, restore validation, repair, and backup. It also means
that a future compaction call would silently violate the current restart
validator, which requires `first_index == 1`.

## Decision

Epoch will add a canonical **consensus checkpoint v1** to the fixed-three-voter
adapter. It is an Epoch-owned format carried inside Raft snapshot metadata and
persisted as a new additive EPRS record kind.

### Checkpoint contents

A v1 checkpoint binds:

- group ID and group epoch;
- checkpoint log index and the exact Raft term at that index;
- the fixed voter `ConfState` in the surrounding Raft snapshot metadata;
- the canonical state-history digest;
- the complete, ordered committed-proposal registry needed for exact retry,
  conflict detection, and deterministic profile replay.

Every integer uses fixed-width big-endian encoding. Payload lengths and counts
are explicit. Decoding must consume the complete frame and re-encoding must
produce the identical bytes. The existing state-history digest is recomputed
before a checkpoint is accepted. The snapshot data is bounded so the complete
protobuf peer frame remains below the existing one-MiB transport ceiling.

The complete proposal registry is intentional in v1. It preserves the current
unbounded idempotency promise and lets every existing profile rebuild through
the same deterministic `replay` boundary. Replacing it with a compact
profile-native state image requires a separate retention/idempotency contract.

### Local checkpoint creation

A checkpoint may be created only at the adapter's durable applied index. Epoch
first writes and fsyncs the EPRS checkpoint transition. Only after that barrier
may it replace the in-memory Raft prefix with the snapshot and retain the
strictly contiguous tail after the checkpoint index.

Checkpoint creation fails without mutating memory or advancing the stable
generation when:

- Ready work is still outstanding;
- the index or term cannot be proven from local storage;
- the canonical snapshot exceeds its bound;
- HardState, checkpoint, voter, or digest invariants disagree;
- the stable journal append fails.

Creating the same checkpoint twice is a no-op. Checkpoint creation never emits
a commit receipt and never changes a proposal's retry result.

### Snapshot transport and installation

`MsgSnapshot` is accepted only when its envelope, protobuf message, metadata,
fixed voters, group/epoch, index/term, canonical bytes, and digest agree.
Membership-changing or foreign snapshots remain rejected.

The receiving voter durably appends the EPRS checkpoint transition before it
advances Raft or acknowledges the snapshot. Installation atomically replaces
the prior applied proposal registry and compacted Raft prefix, preserves any
valid later tail supplied by Raft, and publishes a distinct checkpoint-install
event. The node runtime handles that event with `CommittedProposalApplier::replay`
so a typed profile is replaced from the authoritative checkpoint rather than
incrementally double-applied.

After installation, exact retry and conflict behavior, the applied index,
state-history digest, and profile state must match a voter that reached the same
index through ordinary entry replication.

### Reopen and recovery

EPRS replay accepts legacy identity/transition records and the additive
checkpoint record. A checkpoint record resets the logical retained prefix;
later transition records may only append or replace an uncommitted contiguous
tail above the checkpoint index. Reopen materializes a snapshot-aware Raft
store and replays the checkpoint registry plus the committed tail before the
runtime becomes ready.

Corrupt, truncated, non-canonical, regressing, foreign, or internally
inconsistent checkpoint records fail closed. Only the existing incomplete
outer-WAL tail policy may repair bytes.

## Required evidence

The feature is not complete until tests prove:

1. exact codec goldens and malformed/boundary rejection;
2. fsync-before-memory ordering and post-fsync crash reopen;
3. local checkpoint plus committed-tail reopen with unchanged lookup/digest;
4. a lagging follower receives `MsgSnapshot`, replaces its profile through
   replay, catches up the tail, and converges;
5. stale, foreign, wrong-voter, wrong-term, and corrupted snapshots fail closed;
6. checkpointed voters can elect, commit, transfer leadership, and satisfy a
   read barrier after restart;
7. the existing no-snapshot histories remain byte- and behavior-compatible.

## Consequences and non-claims

This decision enables durable consensus checkpoints, logical Raft-prefix
compaction, snapshot-based fixed-voter catch-up, and checkpoint-plus-tail
reopen. It does **not** yet claim:

- profile-native compact state images or bounded idempotency retention;
- physical reclamation of older EPRS outer-WAL records;
- chunked or out-of-band transfer for checkpoints larger than one peer frame;
- dynamic membership, learner promotion, online tablet transfer, or repair;
- standalone Cache snapshots, backup artifacts, PITR, remote tiering, or
  operator-managed restore.

Those remain separate G2/G3/G8 deliverables. Documentation and status surfaces
must call this a consensus checkpoint, not a complete product backup or profile
snapshot.
