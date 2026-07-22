# ADR-0003: Consensus Adapter and Spike Gate

**Status:** Proposed  
**Date:** 22 July 2026

## Context

Epoch requires strongly consistent metadata, leader fencing, quorum-durable
tablets, membership changes, snapshots, and recovery. Consensus safety is not an
appropriate place for a new product to invent an algorithm. At the same time,
the selected implementation must support many mostly idle groups, a custom
segment store, deterministic fault testing, and predictable active-group
latency.

The PRD leaves the consensus-library choice open.

## Proposed decision

Define an internal `ConsensusAdapter` boundary and provisionally use `raft-rs`
for the Phase 0 spike. The adapter owns Epoch-specific integration while the
library owns the Raft algorithm.

The adapter exposes only the capabilities Epoch needs:

- propose and await a commit position;
- linearizable read barrier;
- leader, term, commit, applied, and replication status;
- membership change and learner promotion;
- leader transfer;
- snapshot creation, installation, and log purge;
- deterministic message/tick injection for simulation;
- persistent and memory-only log-store implementations.

No engine, protocol, or public API may depend on library-specific types. Epoch
implements transport, storage framing, admission, scheduling, metrics, and state
machines around the adapter.

Implementing a custom consensus algorithm is out of scope. Replacing the
provisional library with another vetted Rust library remains possible behind the
adapter if the spike fails.

## Stage 1, local persistence, and probe status

The first workspace slice implements a fixed-three-voter, memory-only adapter
behind Epoch-owned types. It exercises election, majority commit, partition and
catch-up, leader transfer, proposal lookup/deduplication, bounded versioned peer
frames, fail-stop Ready handling, restart-image invariants, and deterministic
fault delivery through `epoch-testkit`. Snapshots and membership entries are
rejected rather than partially supported. See the evidence and non-claims in
[Consensus Feasibility Spike](../CONSENSUS_SPIKE.md).

A follow-on local persistence sub-slice adds `PersistentRaftAdapter` and EPRS
v1 over the checksummed, fsync-backed `FileWal`. Each generation stores explicit
`HardState`, normal-entry, and applied/publishable digest-checkpoint fields rather
than raw library protobuf. Reopen validates immutable identity, reconstructs
logical suffix replacement and applied history, and rejects detected corruption
or invariant regression. The byte and recovery contract is documented in
[EPRS v1 consensus stable journal](../../spec/formats/consensus-stable-store-v1.md).

The evidence now also includes a three-child-process partition and
`SIGKILL`/same-path-reopen smoke, plus an opt-in `epoch-node` probe with a
dedicated actor, bounded ordered HTTP transport, local diagnostic lookup, and a
static three-container topology. The runtime commits only opaque probe bytes;
it does not drive a product state machine. See
[Experimental Consensus Probe](../CONSENSUS_PROBE.md).

This work does not accept the ADR. A runnable node can use the adapter only for
the explicitly experimental opaque probe, and no product API returns a
durable-majority acknowledgement. An exhaustive process-crash and I/O-fault
matrix, snapshots and checkpoint installation, membership and
catalog-authorized epoch transitions, read barriers, authenticated transport,
group density, performance, formal-model, and chaos evidence remain open.

The released `raft-rs` 0.7 dependency graph was rejected because its
`protobuf` 2.28 dependency is affected by `RUSTSEC-2024-0437`. The spike pins
official upstream revision
`ad13f3d90780f53aea2488c6a4b76c0d334bf136` with `prost-codec`, which removes
that dependency and includes the later unstable-entry capacity fix. The
revision is unreleased and still brings the unmaintained `fxhash` dependency
reported by `RUSTSEC-2025-0057`. CI has one explicit temporary exception for
that informational advisory and denies every other Cargo advisory or warning.
The exception, transitive unsafe/C++ inventory, and unreleased revision are
open items under acceptance criterion 8.

## Spike acceptance gate

The proposal becomes Accepted only after a reproducible spike demonstrates:

1. no acknowledged-loss or split-brain history under crash/restart, process
   pause, packet loss, duplication, reordering, partition, and stale-node tests;
2. correct membership change, leader transfer, snapshot install, tail catch-up,
   and partial-write recovery;
3. a segment-backed storage adapter without a second synchronous customer-data
   source of truth;
4. deterministic simulation with history and state digest checking;
5. bounded memory and CPU for at least 10,000 mostly idle groups on a reference
   node, with a documented path to the 100,000-shard deployment target;
6. matched three-node quorum throughput and p99 latency sufficient for the Phase
   0 and Phase 1 benchmark gates;
7. observable commit, apply, replication, snapshot, and election state;
8. a license, dependency, unsafe-code, and security review.

The spike report must record hardware, group count, active fraction, payload,
batching, fsync policy, fault schedule, and complete latency percentiles.

## Consequences

- Product code can proceed against a stable boundary while the highest-risk
  implementation choice is tested.
- The adapter adds a small abstraction cost but prevents consensus internals from
  spreading throughout the engines.
- A failed spike can change library or tablet grouping without rewriting public
  APIs.
- The group-density target may require a shared Multi-Raft scheduler rather than
  one heavyweight async task per group.

## Alternatives to evaluate if the spike fails

- Another production-proven Rust Raft library with equivalent storage and
  simulation hooks.
- A coarser replication group containing carefully isolated same-profile
  tablets, documented in a new ADR.

A custom consensus implementation requires a separate ADR, formal model,
independent expert review, and evidence that vetted libraries cannot satisfy the
requirements.
