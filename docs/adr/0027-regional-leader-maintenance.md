# ADR-0027: Leader-Owned Regional Maintenance

**Status:** Accepted

**Date:** 13 August 2026

## Context

Epoch's replicated profile state machines already model time-driven work as
ordinary commands: Stream age retention and consumer-session expiry, Queue
scheduling/TTL/max-age/dedupe/lease transitions, Cache value and lock expiry,
and Event Bus delivery-lease timeout. Until now the regional runtime required a
client or test to submit those commands. Passive reads deliberately never
mutate state, so an idle resource could retain or expose stale operational
state indefinitely.

A timer running independently on every voter would be unsafe. Local wall-clock
ticks are not replicated, duplicate proposals must not create different
effects, and a follower must never become a second time authority. Maintenance
also has to retain the existing Raft, fencing, checkpoint, replay, and
idempotency guarantees instead of mutating a tablet beside consensus.

## Decision

1. Each profile exposes a pure query for its earliest replicated maintenance
   deadline. The query does not sample time or mutate state.
2. Every regional node scans materialized routes on a bounded interval, 100 ms
   by default and configurable from 1 ms to 60 seconds with
   `EPOCH_REGIONAL_MAINTENANCE_INTERVAL_MS`.
3. A node proposes work only when its local consensus actor currently reports
   leader role and is not fail-stopped. Leadership remains the ownership
   mechanism; the Go control plane and SDKs are not timer authorities.
4. The command's applied time is the exact replicated deadline that became due,
   not the later scheduler tick. This keeps independent voters deterministic
   and prevents scheduler delay from changing retry, expiry, or retention
   results.
5. Proposal identities derive from the operation and due deadline. Bounded
   Queue, Cache, and Event Bus sweeps also include the last applied profile
   index so remaining work at the same deadline can advance in another command.
   Existing committed or pending proposals are observed before submission.
6. Stream retention is evaluated per logical shard. Consumer-session expiry is
   proposed only by logical shard 0. Queue proposes its combined timer sweep;
   Cache reclaims at most 1,000 due values/locks per command; Event Bus examines
   at most 100 expired delivery leases per command.
7. `GET /experimental/v1/regional/topology` reports the configured interval and
   cumulative pass, tablet, leader, due, submitted, pending, and error counters,
   plus the last pass time and optional last error. The endpoint retains its
   existing `topology.read` authorization boundary.
8. Explicit SDK and HTTP maintenance operations remain available for recovery,
   diagnostics, and deterministic tests. Regional correctness no longer
   depends on an application calling them.

## Consequences

- Idle regional resources now advance the implemented time-driven lifecycle
  through the same majority-committed commands as user mutations.
- Leader loss transfers maintenance ownership without a separate lease. Exact
  proposal lookup and deterministic keys make overlapping ticks harmless.
- A delayed tick delays visibility but does not rewrite the replicated due
  time. This is a bounded polling scheduler, not a real-time deadline SLA.
- Scheduler counters are node-local operational observations; product state and
  business receipts remain replicated. Counter reset on process restart is not
  data loss.
- This decision does not add automatic profile-native checkpoint scheduling,
  dynamic placement, cross-region timer ownership, external Event Bus target
  executors, Queue streaming receive, Cache eviction, Stream compaction/tiering,
  or production telemetry export and alerting.

## Rejected alternatives

- Mutate expired state during reads, which would make a read a hidden write and
  bypass majority commit.
- Let every voter propose timer work, which adds avoidable duplicate traffic and
  weakens ownership reasoning during partitions.
- Use the scheduler's current wall time as command time, which makes outcomes
  depend on process delay rather than the first replicated deadline.
- Put timer ownership in the Go control plane, which would make regional data
  correctness depend on hosted management availability.
