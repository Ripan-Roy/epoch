# ADR-0013: Quorum-Confirmed Regional Read Barriers

**Status:** Accepted

**Date:** 29 July 2026

## Context

Epoch's fixed-three-voter tablets already acknowledged writes only after a
majority persisted the log entry and the local typed profile applied it.
Regional reads, however, dispatched directly to a local profile snapshot. A
caller could therefore observe stale state from a follower or an isolated old
leader even when the product semantics required a linearizable leader read.

The regional API needs an explicit consistency boundary without changing the
truthful contract of the direct experimental profile routes or claiming that
the current fixed membership is a production placement system.

## Decision

1. `epoch-consensus` owns a bounded `ReadBarrierRequest` and
   `CompletedReadBarrier`; no `raft-rs` type crosses the adapter boundary.
   Requests carry the exact group, group epoch, expected leader term, and a
   non-zero process-unique request ID.
2. Only the current leader in the expected term may admit a barrier. Epoch uses
   safe Raft `ReadIndex`, not a lease-only read. Because `raft-rs` drops reads
   before a new leader has committed in its own term, Epoch retains such a
   request and submits it after the election entry is durable and applied.
3. A barrier completes only after a majority confirms the read context and the
   local consensus state machine has applied through the returned read index.
   The node actor applies every typed profile commit before publishing the
   corresponding completion, so an HTTP handler cannot observe profile state
   behind its completed barrier.
4. Pending barriers are ephemeral, bounded to 1,024 per group, canceled after
   caller timeout, and discarded on leader or term change. They are not written
   to EPRS and do not alter the replicated command log.
5. Regional semantic reads default to `linearizable`. This includes all typed
   GET operations and the Event Bus `POST .../archive/replay` and
   `POST .../deliveries/query` query operations. They require the data-read
   authorization action, current leader, live majority, and a completed
   barrier.
6. A caller may explicitly request
   `x-epoch-read-consistency: local_stale`. This is the only regional path that
   may serve a stale-capable local observation; Epoch never silently
   downgrades a linearizable request.
7. The bounded wait defaults to two seconds, is constrained to 1–60,000 ms, and is configurable with
   `EPOCH_REGIONAL_READ_BARRIER_TIMEOUT_MS`. Timeout returns retryable HTTP 503
   with `read_barrier_timeout`; a follower or term race returns the existing
   retryable leader-routing conflict.
8. Successful linearizable responses include
   `x-epoch-read-consistency: linearizable`,
   `x-epoch-read-index`, and JSON evidence for barrier term, read index, and
   locally applied index. Direct profile routes and explicit stale reads keep
   `local_profile_applied_stale_capable` and
   `linearizable_read_barrier: false`.

## Consequences

- A successful default regional read is ordered after every write committed
  before the quorum confirmed its read context.
- An isolated leader cannot complete a new default read. The API favors a
  retryable failure over stale success.
- Read barriers add one safe ReadIndex quorum round when a caller does not opt
  into stale local consistency.
- The current implementation serves linearizable reads only from the leader;
  follower forwarding and follower lease reads remain absent.
- Barrier evidence proves ordering inside one fixed-voter tablet. It does not
  prove production placement, authenticated peer identity, dynamic membership,
  cross-tablet transactions, or geo consistency.
- The regional surface remains experimental and is not yet promoted into the
  stable Go, Java, Python, or TypeScript SDK contract.

## Rejected alternatives

- Treat the leader's local applied index as a linearizable-read proof.
- Use lease-based reads without a quorum confirmation.
- Silently serve stale data when the majority or current leader is unavailable.
- Append a replicated no-op command for every read.
- Change direct internal profile routes to imply a guarantee their callers did
  not request through the regional consistency boundary.
