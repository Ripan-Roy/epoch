# ADR-0028: Automatic Regional Consensus Checkpoints

**Status:** Accepted

**Date:** 13 August 2026

## Context

Epoch already persists canonical EPSN v2 native-profile checkpoints, compacts
the local Raft prefix, atomically replaces obsolete EPRS generations, catches
up a lagging fixed voter through snapshot installation, and reopens from a
checkpoint plus its retained tail. Until now creation required an explicit
diagnostic request. A long-running regional voter could therefore retain an
unbounded local journal even though the safe reclamation primitive existed.

Checkpoint ownership differs from time-driven profile maintenance. A profile
timer changes replicated business state, so only the current leader may
propose it. A consensus checkpoint changes only one voter's recovery layout;
every voter must be allowed to bound its own journal independently.

## Decision

1. Every regional node evaluates catalog group 1 and every locally
   materialized profile group on a bounded interval. The default is 1,000 ms;
   `EPOCH_REGIONAL_CHECKPOINT_INTERVAL_MS` accepts 1–600,000 ms.
2. A local group is eligible when it is healthy, has no pending Raft `Ready`
   work, has a nonzero applied index, and `applied_index - checkpoint_index` is
   at least `EPOCH_REGIONAL_CHECKPOINT_MIN_APPLIED_ENTRIES`. The default
   threshold is 1,024.
3. Eligibility and checkpoint creation are one consensus-actor command.
   Concurrent or delayed scheduler ticks therefore cannot create duplicate
   journal generations. Pending `Ready` work is a transient skip, not a fatal
   error.
4. Every role owns its local checkpoint. Followers do not wait for leadership;
   this is local recovery state, not a replicated mutation or cluster-wide
   checkpoint barrier.
5. Creation reuses ADR-0021/0022 unchanged: capture the native application at
   the exact applied index, encode and validate EPSN v2, fsync the durable
   checkpoint, atomically compact EPRS, then install the local Raft snapshot.
   Durable/profile failures keep the existing supervised fail-stop behavior.
6. `GET /experimental/v1/regional/topology` reports process-local scheduler
   counters plus the latest observed applied, checkpoint, and retained-first
   indices for every hosted group. All 64-bit group/index fields are decimal
   strings. The endpoint retains `topology.read` authorization.
7. The explicit diagnostic checkpoint endpoint remains available. Automatic
   regional scheduling is an internal voter-recovery mechanism, not a backup,
   PITR, coordinated restore point, or remote artifact.

## Consequences

- Catalog, Stream, Queue, Cache, and Event Bus voters reclaim obsolete EPRS
  history without operator or client calls.
- Each voter can checkpoint at a different applied index. Raft snapshot
  transfer remains the mechanism for a voter that falls behind a peer's
  retained prefix.
- Counters reset on process restart; durable per-group checkpoint indices do
  not. Operators must use the group observations, not cumulative counters, to
  verify recovery boundaries after restart.
- The interval and entry threshold bound command-count growth, not bytes or
  elapsed time. Byte-aware policy, jitter, I/O budgeting, coordinated backup,
  restore campaigns, dynamic membership, and production metrics/alerts remain
  open.

## Rejected alternatives

- Leader-only checkpoints, which leave follower journals unbounded and confuse
  a local recovery operation with replicated business-state ownership.
- A cluster-wide checkpoint barrier, which adds coordination without improving
  the existing Raft snapshot safety contract.
- Checking eligibility outside the actor and then sending an unconditional
  request, which permits scheduler races and duplicate physical work.
- Treating pending Raft `Ready` work as corruption, which can fail-stop a
  healthy actor during ordinary traffic.
