# ADR-0014: Queue Consumer Credit and In-Flight Windows

**Status:** Accepted

**Date:** 30 July 2026

## Context

Epoch's replicated Queue tablet already committed competing-consumer acquire
and settlement transitions, but each acquire was bounded only by its requested
batch size. A fast consumer could repeatedly request batches without declaring
how many unsettled deliveries it could safely hold. QUEUE-011 requires an
explicit credit/prefetch boundary and per-consumer concurrency enforcement.

The first bounded implementation must remain deterministic under replay,
preserve every existing version-one command byte, and avoid claiming the future
native bidirectional streaming protocol before that transport exists.

## Decision

1. The experimental HTTP `acquire` operation retains `max_messages` as the
   credit granted by that request. Supplying `max_in_flight` opts into the
   replicated flow-control contract; omitting it preserves the legacy acquire
   operation and behavior.
2. A flow-controlled transition may deliver at most:

   ```text
   min(requested credit, max_in_flight - current consumer in_flight, ready work)
   ```

   Credit is constrained to 1–100 and `max_in_flight` to 1–10,000.
3. The in-flight count includes every currently applied live lease owned by the
   same consumer identity, including leases created under an older consumer
   epoch. A higher epoch fences old settlement attempts but does not create
   overlapping capacity while an older lease remains live.
4. Maintenance and the concurrency observation execute inside the same cloned,
   actor-serialized state-machine transition. Settling or explicitly expiring a
   lease frees capacity. A read never samples wall time or implicitly expires a
   lease.
5. The acquire receipt returns the requested credit, declared window,
   in-flight count before and after the transition, and remaining capacity.
   `GET .../consumers/{consumer}/flow` exposes the applied consumer epoch and
   current in-flight count. Direct profile reads remain local and stale-capable;
   the regional resource/shard wrapper applies its normal linearizable
   ReadIndex default.
6. `QueueTabletCommand` now supports format versions one and two. Every
   pre-existing operation continues to emit and validate as version one.
   `AcquireWithCredit` alone emits version two. Old canonical payloads,
   proposal IDs, outcome JSON without flow evidence, and digest vectors
   therefore remain unchanged.
7. The Queue state machine derives the count from authoritative applied lease
   state instead of maintaining a second mutable counter index. This favors a
   small, reviewable correctness boundary; an indexed implementation requires
   equivalent recovery and transition proofs before replacement.

## Consequences

- Repeated receives cannot exceed the declared in-flight window for one
  consumer identity.
- Different consumer identities have independent windows and may compete for
  eligible work.
- A saturated receive is still a committed deterministic operation with an
  empty delivery list and exact flow-control evidence.
- The current count is linear in retained Queue messages. Backlog-scale
  indexing and performance evidence remain required before production claims.
- The HTTP request/response slice does not implement a long-lived
  bidirectional stream, connection-scoped credit replenishment, automatic
  prefetch, cross-consumer fairness, or dispatch shaping.
- The route remains experimental and is not yet part of the stable Go, Java,
  Python, or TypeScript SDK contract.

## Rejected alternatives

- Limit only each acquire batch and leave repeated requests unbounded.
- Key the window by consumer epoch, which could let a restarted consumer exceed
  its window while old-epoch leases remain live.
- Expire leases during a read by sampling local wall time.
- Rewrite all operations to command version two and invalidate historical
  canonical vectors.
- Maintain a duplicated mutable counter before the transition corpus proves
  every settlement, expiry, retry, and recovery edge.
