# ADR-0025: Replicated Stream consumer sessions and shard assignment

- Status: Accepted
- Date: 2026-08-13
- Owners: Stream, regional runtime, and SDK maintainers

## Context

Epoch already persists a generation-fenced next offset independently on every
Stream shard, and a multi-shard resource already publishes a stable key-routing
contract. Applications still had to invent group membership, decide which
member owned each shard, detect dead members, and distribute a new generation.
That left the central coordination part of STREAM-003 absent.

Membership must survive leader replacement, fixed-voter catch-up, native
checkpoint installation, and full-cluster reopen. Wall-clock regression after
an election must not resurrect a deadline. Assignment also has to be identical
on every voter and in every SDK without storing a second authority in the Go
control plane or client process.

## Decision

### Shard zero is the resource coordinator

Logical Stream shard 0 owns each resource-wide consumer-session group. A
session command captures the resource's nonzero `shard_count`; a later command
with another count is a committed `shard_count_mismatch` rejection. This makes
an expansion race visible instead of silently assigning an obsolete layout.
Nonzero shards reject session routes.

The Stream tablet adds canonical command format v5 for `join`, `heartbeat`,
`leave`, and `maintain`. Existing v1 append, v2 batch, v3 offset, and v4
retention commands remain unchanged. Native Stream snapshot format v2 adds the
session-group map; a v1 snapshot restores with no sessions and remains
readable.

### Membership and generation

A group stores a bounded, lexically ordered member map, the captured shard
count, a positive membership generation after the first join, and a monotonic
committed time watermark.

- Joining a new member increments the generation exactly once and returns the
  complete membership plus that member's assignment.
- Rejoining an existing member renews its timeout without changing membership
  or generation.
- Heartbeat and leave require the current group generation. Unknown members
  and stale generations are typed committed rejections.
- Leaving or expiring one or more members increments the generation exactly
  once for the command, regardless of how many members disappear.
- An empty group remains observable with its last generation. A later new join
  advances that generation; it does not recreate generation 1.

The limits are 1,024 members per group, 4,096 shards, and a whole-millisecond
session timeout from 1 second through 5 minutes. Existing bounded group and
member identifier rules also apply. A deadline that cannot be represented as
an unsigned 64-bit millisecond value is a typed committed
`deadline_overflow` rejection and cannot create a phantom group.

### Deterministic assignment

Members are sorted by exact identifier bytes. Shard `s` belongs to member
`s mod member_count`. Consequently every shard has exactly one owner while the
group is nonempty, assignments are balanced to within one shard, and all
voters reproduce the same plan without randomness or local state.

This is an eager assignment result. The coordinator does not yet deliver a
cooperative revoke handshake or wait for a former member to acknowledge that
it stopped processing before another member sees the shard.

### Committed logical time and expiry

Every session mutation carries the leader's candidate wall time in the
replicated command. Apply uses `max(previous_watermark, candidate)` so time
cannot move backward after election or replay. A member expires inclusively
when `deadline_ms <= watermark_ms`.

Every command first performs a bounded expiry sweep, and `maintain` exists to
advance time when no member operation is occurring. Expiry is part of the same
replicated transition as the requested operation. Therefore a heartbeat may be
rejected as unknown or stale after its member expired while the command still
commits the expiration and new generation. Clients must inspect the typed
receipt rather than infer that a committed Raft entry means the requested
business action was applied.

Epoch does not run a background session timer in this slice. Operators or
clients must submit maintenance when idle groups need prompt dead-member
detection.

### API and SDK surface

The authenticated regional Stream v1 route exposes these suffixes on shard 0:

```text
POST   /groups/{group}/sessions
GET    /groups/{group}/sessions
PUT    /groups/{group}/sessions/{member}/heartbeat
DELETE /groups/{group}/sessions/{member}
POST   /groups/{group}/sessions/maintenance
```

Mutations carry a caller-owned idempotency key and the discovered leader term.
Heartbeat and leave additionally carry the decimal group generation. The GET
uses the normal leader ReadIndex barrier. Direct experimental tablet routes
expose the same suffixes for runtime verification.

Go, Java, and Python regional clients expose join, heartbeat, leave,
maintenance, and linearizable observation methods. They always route these
operations to shard 0, preserve the semantic request across one bounded
rediscovery, validate static bounds before network I/O, and return the server's
complete receipt or observation.

### Offset checkpoint boundary

The coordinator assigns logical shards but does not atomically propagate its
generation into each independently replicated shard's v3 checkpoint owner.
The existing per-shard checkpoint generation remains a separate fence. An
application must stop work it no longer owns and manage each assigned shard's
checkpoint handoff explicitly. Cross-shard assignment-plus-offset commit and
read-committed transactions remain STREAM-008 work.

## Consequences

- Membership, deadlines, generations, and assignments now survive the same
  majority commit and native checkpoint path as Stream records.
- Leader-clock regression cannot extend an already committed deadline.
- Assignment is portable, deterministic, and balanced without a second
  coordinator service.
- Resource expansion races fail closed through captured shard count.
- Idle expiry requires an explicit committed maintenance operation.
- Eager reassignment can overlap with a slow former owner, so applications
  must obey generation and assignment changes.

## Rejected alternatives

- Client-only coordination: it provides no common durable authority and cannot
  recover deterministically after process loss.
- Go control-plane membership: it places customer data-path correctness in the
  management plane and creates a second replicated-state problem.
- Per-shard coordinators: they can assign the same member set differently and
  cannot produce one resource-wide generation.
- Local timers: firing on one voter would make replay and state digests depend
  on scheduler timing.
- Random or sticky assignment in the first slice: it requires additional
  canonical history and complicates exact recovery evidence.

## Verification

Required evidence includes:

- canonical v5 command and v1-v4 compatibility locks;
- join/rejoin, heartbeat, leave, stale-generation, inclusive-expiry, capacity,
  monotonic-time, exact-retry, deterministic-assignment, and v1/v2 snapshot
  tests;
- a three-voter test that rebalances, replaces leadership, checkpoints, and
  restores identical membership on every voter;
- Go, Java, and Python route/validation contract tests;
- a real three-shard Python campaign after leader loss, followed by old-voter
  catch-up and all-node same-volume reopen; and
- exact three-language examples embedded in the Pages documentation bundle.

## Non-claims

This decision does not provide background scheduling, server-push assignment,
cooperative revoke/acknowledgement, sticky/rack-aware strategies, streaming
fetch, exactly-once processing, atomic multi-shard offsets, transactions,
online shard remapping, dynamic voter membership, package publication, or the
production scale and fault matrix.
