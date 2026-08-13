# ADR-0029: Session-fenced Stream consumption

- Status: Accepted
- Date: 2026-08-13
- Owners: Stream, regional runtime, and SDK maintainers

## Context

ADR-0025 made logical shard 0 the durable resource-wide consumer-session
coordinator. ADR-0016 already stored a durable next offset and owner generation
inside every independently replicated shard. Assignment and checkpoint
ownership still used separate generations, so an application could observe a
new assignment while a slow former member continued fetching or committing on
the old shard fence.

A truthful bounded handoff must preserve the committed next offset, fence stale
members before they fetch or commit, survive leader loss and native checkpoint
restore, and work across independently led shard groups. It must not claim an
atomic cross-shard transaction that the current architecture cannot provide.

## Decision

### Replicated per-shard claim

Stream command format v6 adds the `claim` group-offset mode. A claim carries
the consumer group, member, positive session generation, physical partition
zero, caller idempotency key, and committed leader time. It changes only the
shard's owner fence; it preserves the durable next offset exactly.

An unowned checkpoint accepts generation 1. An existing checkpoint accepts its
exact generation or exactly the next generation. A same-generation claim may
migrate an older unfenced checkpoint owner to the coordinated member; once a
session fence exists, another member at that generation is rejected. Lower,
skipped, or conflicting generations are committed typed rejections. These
bounds prevent an arbitrary claim from jumping a shard permanently ahead of
the coordinator.

Snapshot format v3 records whether an owner is session-fenced. Snapshot v1 and
v2 remain readable when they do not contain this new state. Relabeling a v3
session-fenced owner as an older snapshot is rejected.

### Fenced bounded fetch and commit

The claimed-records read requires the exact member and generation currently
stored on the shard and requires `session_fenced: true`. It begins at the
durable next offset and returns at most 1,000 records. The checkpoint
observation and records are read while holding one tablet read guard. Wrong or
stale ownership returns HTTP `409 fenced` with
`definite_not_committed`; profile unavailability remains a distinct `503`.

After a claim, commit and reset require the exact claimed member and generation.
A higher generation must be installed through a claim first. Consequently a
former member cannot fetch or advance the checkpoint after a replacement claim
commits.

### First-party SDK protocol

Go, Java, and Python expose:

- a low-level per-shard claim;
- a low-level exact-member/generation claimed fetch; and
- a resource-level `ClaimConsumerSession`/
  `claimConsumerSession`/`claim_consumer_session` helper.

The helper performs this bounded protocol:

1. Discover shard 0 and pin the resource generation.
2. Read the session through a leader ReadIndex and require the exact group,
   member, generation, and nonempty sorted assignment.
3. Read every assigned shard's checkpoint through a leader ReadIndex.
4. Plan every missing monotonic generation before making a mutation, rejecting
   a checkpoint ahead of the session, more than 4,096 bridge transitions, or a
   derived idempotency key over 128 UTF-8 bytes.
5. Claim each assigned shard using a deterministic key containing shard and
   generation, retaining the pinned resource generation across discovery.
6. Re-read shard 0 at that resource generation and return the assignment only
   if member, generation, and assigned shards are unchanged.

This sequence is safe to retry with the same prefix. A rebalance or resource
expansion may leave a subset of offset-preserving claims installed; the helper
returns no usable assignment, and the new generation can advance those fences.
The clients distinguish a retryable regional routing fence by its explicit
top-level `retryable: true` envelope. The application-level `409 fenced` from a
stale consumer is definitive for that member/generation and is returned
unchanged rather than consumed by route rediscovery.

### Authorization boundary

The regional API retains namespace-scoped `data.write` authorization for claim
mutations and `data.read` for claimed fetches. The current bootstrap policy does
not bind a bearer principal to a specific consumer member. Therefore direct
low-level claim is an administrative primitive inside the namespace trust
boundary; member-bound identity and per-group ACL/audit specificity remain G5
work. SDK revalidation provides protocol safety, not identity proof.

## Consequences

- A committed claim never changes the next offset and therefore cannot skip a
  record.
- Replacement claims fence stale fetch and commit on that shard.
- Bounded pull credit is explicit in each fetch limit; applications commit the
  returned next offset only after processing.
- Existing checkpoint generations can be bridged without rewriting history.
- A coordinator rebalance is detected after claims, and a resource-generation
  race fails before a target mutation.
- Claim state survives fixed-voter convergence, native checkpoint installation,
  catch-up, and same-volume reopen.

## Rejected alternatives

- Treat session observation alone as a fetch fence: a stale member could race
  the read and checkpoint commit across independently led groups.
- Copy the session generation locally without consensus: recovery and voters
  could disagree about ownership.
- Allow arbitrary generation jumps: one malformed request could make the shard
  unreachable by valid near-term sessions.
- Claim all shards in one command: shards are independent consensus groups and
  no cross-group transaction coordinator exists yet.
- Call the HTTP pull loop a native streaming consumer: it has bounded requests,
  not a persistent server-push or bidirectional transport.

## Verification

Required evidence includes canonical v6 and snapshot-v3 compatibility tests,
deterministic offset preservation/fencing/retry tests, one-read-snapshot HTTP
behavior, authenticated regional routing, real three-voter convergence and
reopen, Go/Java/Python contract suites, executable quickstarts, and the
three-shard Python failover/catch-up/all-node recovery campaign.

## Non-claims

This decision does not provide an atomic multi-shard assignment transaction,
cooperative revoke acknowledgement, member-bound authorization, exactly-once
processing, atomic offset-plus-output commit, read-committed transactions,
server-push assignment, a persistent streaming transport, bandwidth isolation,
fairness/load evidence, or production scale and fault certification.
