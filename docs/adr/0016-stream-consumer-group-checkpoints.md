# ADR-0016: Replicated Stream Consumer-Group Checkpoints

**Status:** Accepted

**Date:** 5 August 2026

## Context

Epoch's standalone Stream already stored a consumer group's next offset, but
the replicated tablet owned only records. A leader change or tablet rebuild
therefore could not preserve clustered commit/reset state, report authoritative
lag, or fence a former group owner. STREAM-003 requires durable checkpoints,
lag, reset/rewind, and replay before the later coordinated membership protocol
can be credible.

This decision covers the per-partition checkpoint primitive. It establishes
replicated offset and ownership-fence semantics. ADR-0025 subsequently adds a
separate shard-zero join/heartbeat/leave/expiry/assignment coordinator without
changing this v3 checkpoint protocol.

## Decision

1. Existing single append v1 and batch append v2 bytes, proposal IDs, receipts,
   and append digest transitions remain unchanged. `GroupOffset` alone emits
   canonical command format v3. Version and operation kind must agree.
2. A mutation names a bounded consumer group, bounded member ID, nonzero
   caller-supplied group generation, partition `0`, next offset, and either
   `commit` or `reset`. A group's offset always means the next record to fetch.
3. The first accepted generation is exactly `1`. The active member may repeat
   a mutation in its current generation. Another member in that generation is
   fenced, a lower generation is stale, a jump over the next generation is
   rejected, and exactly the next generation may establish a new owner.
4. `commit` is monotonic and cannot move behind the current checkpoint.
   `reset` is the explicit rewind operation. Both must remain within the
   retained range from earliest through end offset. At this decision's
   introduction the tablet had no retention deletion and its earliest offset
   was zero. ADR-0023 later allows that base to advance and makes a checkpoint
   below it explicitly out of range until reset.
5. Static malformed input is rejected before proposal. Ownership, generation,
   range, rewind, and group-capacity races are evaluated in committed order and
   return a typed committed `rejected` receipt. A business rejection changes no
   record, owner, or checkpoint state, but remains in the applied command
   history and state digest so every voter reaches the same result.
6. An accepted transition applies against cloned Stream state before replacing
   the live checkpoint and owner. Exact command replay returns the original
   evidence with `disposition: replayed` and never applies twice.
7. The tablet bounds groups and members to 256 UTF-8 bytes without control
   characters and stores at most 10,000 groups. All 64-bit receipt and
   observation fields are decimal strings in HTTP JSON.
8. The experimental direct routes are:
   - `PUT /experimental/v1/tablets/stream/groups/{group}/offsets`;
   - `GET /experimental/v1/tablets/stream/groups/{group}/lag`; and
   - `GET /experimental/v1/tablets/stream/groups/{group}/records`.
   The generic regional resource/shard router exposes the same operations under
   `data/groups/{group}/...`, with its existing authorization, generation,
   tablet-epoch, leader, and read-barrier rules.
9. The existing Go, Java, and Python SDK offset helpers continue to target the
   standalone API and do not silently replace that stable-local contract.
   [ADR-0017](0017-regional-stream-v1-and-sdk-routing.md) subsequently adds a
   separate regional client for this explicit checkpoint primitive. ADR-0025
   adds regional `ConsumerSession` membership and assignment methods while
   deliberately retaining this per-shard owner generation as a separate fence.

## Consequences

- Records and consumer checkpoints now share one majority-persisted command
  history and deterministic recovery boundary.
- Operators can distinguish a nonexistent group from an existing group at
  offset zero, observe exact owner generation and lag, and replay records from
  the committed next offset.
- A timed-out caller can retry the same idempotency key and semantic input; a
  changed member, generation, offset, or mode conflicts with the original
  command.
- Caller-supplied checkpoint generations are a fencing primitive, not the
  membership service. ADR-0025 can discover dead session members and assign
  logical shards, but it does not atomically install that generation in every
  checkpoint or guarantee cooperative revoke behavior.
- Offset commits are not atomic with record production or transactions in this
  slice. STREAM-008 remains responsible for atomic offsets and read-committed
  transaction semantics.
- STREAM-003 advances from a local prototype to a replicated slice. ADR-0023
  subsequently defines retention interaction: stale checkpoints are preserved,
  flagged out of range, and require explicit reset. ADR-0025 subsequently adds
  coordinated sessions and resource-wide assignment. Atomic checkpoint
  handoff, authorization/audit specificity, generated response contracts,
  scale/fairness evidence, and the production fault matrix remain incomplete.

## Rejected alternatives

- Keep checkpoints only in process-local Stream state, which loses ownership
  and progress at failover.
- Treat every retry or stale-owner race as a transport error, which makes the
  committed outcome ambiguous and can diverge application behavior.
- Allow a new member to reuse the current generation, which cannot fence an
  old owner deterministically.
- Infer a reset whenever a commit moves backward, which hides a destructive
  replay decision inside an ordinary progress update.
- Claim a complete consumer-group protocol from checkpoint storage alone.
