# ADR-0005: Injectable Time and Fencing

**Status:** Accepted  
**Date:** 22 July 2026

## Context

Epoch uses time for TTL, delayed delivery, schedules, retries, queue visibility,
consumer sessions, transactions, retention, and deduplication windows. Wall
clocks can move backward or forward. Process-local monotonic clocks do not
survive restart or transfer to another leader. A stale owner that acts after
failover can corrupt order, acknowledgement state, or transaction results.

The system also needs deterministic time in tests and the local emulator.

## Decision

All engines depend on an injectable Epoch clock. Direct engine calls to the
operating system wall clock are prohibited.

The clock exposes three concepts:

- UTC wall time for user-facing timestamps and scheduled instants;
- monotonic elapsed time for process-local waiting and timeout implementation;
- a persisted hybrid logical clock observation for ordered, cross-restart state
  transitions.

State-machine commands that depend on time record the relevant logical time or
deadline. Logical time never moves backward. Wall-clock jumps are clamped or
slewed within a configured uncertainty policy and emit an audit/health event.
During uncertainty, Epoch prefers a conservatively late delivery or expiry over
an early, unsafe transition.

Only the current tablet leader drives durable timer eligibility. Followers
reconstruct timer indexes from committed state.

Every ownership domain uses a monotonic fence:

- tablet leadership uses term and tablet epoch;
- producers use producer ID and producer epoch;
- consumer groups and FIFO sessions use generation/owner epoch;
- queue deliveries use message ID, lease generation, leader epoch, and expiry;
- cache locks use tablet epoch plus acquisition log index for downstream
  fencing, and active-owner epoch, leader term, lease generation, and expiry for
  the current opaque lease token;
- transactions use coordinator and producer epochs.

Every mutating request validates all applicable fences. A token from a previous
leader, owner, lease, or coordinator is rejected even if its wall-clock deadline
appears valid.

On leadership change, the new leader continues from committed logical time. If
clock uncertainty prevents proving that an old lease expired, it delays
redelivery until safe; it never accepts an acknowledgement from the old leader
epoch.

## Consequences

- Timer and lease behavior can be simulated without sleeping.
- Clock anomalies become explicit operational events rather than silent early
  expiry or duplicate acknowledgement.
- Some work can be late during failover or severe clock uncertainty.
- Tokens and persisted transitions carry more epoch metadata.
- Time, lease, and promotion behavior require a formal model and history tests.

## Implementation status

The foundation slice now exposes wall and monotonic milliseconds separately
through the shared Rust `Clock` trait. `SystemClock` derives elapsed time from a
process-local monotonic source, while `ManualClock` can jump wall time without
moving elapsed time backwards and saturates instead of wrapping. A serializable
`HybridTimestamp` and deterministic `HybridLogicalClock` implement local ticks,
remote observation, persisted restart state, and explicit overflow failure.

The fixed-voter consensus adapter now exercises leader transfer and replacement.
Its Queue and Cache tablets capture a server-side candidate time before
proposal, then derive `max(candidate, prior committed effective time)` on every
voter and during EPRS replay. Tests cover wall-clock rollback, descending
assignments in committed order, time-dependent lease deadlines, and identical
live/recovered digests.

The Cache tablet follows the same committed-order normalization for TTL and
lock decisions. Cache lock renewal, release, and guarded mutations
reject tokens whose bound term differs from the committed command term, as well
as rotated opaque tokens, while the downstream fencing token remains the
ordered pair `(tablet_epoch, acquisition_log_index)`. An already-appended
same-term command may commit after a leadership change. New mutations are
admitted only when their expected term matches the actor's current leader term;
this is not a read barrier. A leader change does not let a second owner acquire
before the old exclusive deadline. Deterministic, real-runtime, and container
tests cover node/EPRS failover and recovery for this bounded topology.

Cache owner epochs are retained only while that owner has an active lock. This
bounds owner history; after the last release or expiry, a later acquisition may
use a lower owner epoch but necessarily receives a higher acquisition log-index
fence. Downstream systems order the composite acquisition fence, not owner epoch
alone.

This is partial evidence for the decision, not the completed distributed time
contract. Existing standalone profile commands still record wall-time apply
instants. General clock-anomaly clamping/slewing, audit events, persisted HLC
integration in durable commands, automatic leader-owned timer proposal, and a
complete failover-uncertainty policy remain gates for the replicated runtime.

## Required verification

- backward and forward wall-clock steps;
- process pause and restart;
- leader transfer immediately before and after a deadline;
- duplicated, delayed, and reordered timer work;
- stale Ack, Extend, producer, session, and transaction requests;
- deterministic replay producing the same eligibility and state digest.

## Rejected alternatives

- Using wall-clock comparisons directly inside each engine.
- Trusting a client-supplied delivery or lease epoch.
- Treating lease expiry alone as sufficient fencing.
- Redelivering immediately after failover when old-lease expiry is uncertain.
