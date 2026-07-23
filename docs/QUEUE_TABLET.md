# Replicated Queue Tablet Core

**Status:** Deterministic crate-level state machine; not wired to a node runtime
or public API

`crates/epoch-tablet` now contains the profile application boundary for one
configured, single-partition Work Queue tablet. It proves how an already
committed history can drive Queue delivery, fencing, retries, dead-lettering,
and replay deterministically. It does not yet prove that the node can propose,
replicate, recover, or serve those commands.

## Implemented boundary

```text
caller-supplied committed command
  -> strict canonical QueueTabletCommand v1 decode
  -> scope, order, proposal, and applied-time validation
  -> deterministic Queue transition on a cloned candidate state
  -> recorded applied or rejected business outcome
  -> receipt and complete transition digest
```

- `epoch-queue` owns the Queue domain state and consensus-neutral fenced-lease
  primitives. Its original standalone v1 token and recovery behavior remain
  isolated and unchanged.
- `epoch-tablet` owns committed-command validation, scoped idempotency,
  consumer epochs, authoritative lease-fence derivation, immutable dead-letter
  and redrive history, receipts, and the replicated state digest.
- `epoch-consensus` remains profile-neutral. No Queue applier is attached to its
  actor yet.
- `epoch-node` still serves only the standalone volatile and `local_durable`
  Queue APIs. There is no experimental Queue listener, HTTP route, container
  mode, CLI surface, or SDK surface for this tablet.

The embedded Queue is forced to `volatile` because the containing consensus
history, not a second profile WAL, is intended to own clustered persistence.
At this milestone the crate accepts a `CommittedCommand`; it does not itself
establish that the command was committed or persisted.

## Strict command contract

Every mutation carries format version, tablet ID and epoch, resource name,
idempotency key, leader-assigned `applied_at_ms`, and one typed operation:

| Operation | Deterministic effect |
| --- | --- |
| `Enqueue` | Add or deduplicate one immutable envelope. |
| `Acquire` | Advance a consumer epoch when allowed, select eligible messages, and create fenced leases before returning deliveries. |
| `Acknowledge` | Terminally settle the exact current lease. |
| `ExtendLease` | Replace the current token with one containing a strictly later, bounded deadline. |
| `Release` | Return the message through the configured immediate or delayed retry path. |
| `Nack` | Record a failure reason and apply deterministic retry/backoff or dead-letter policy. |
| `Reject` | Move the leased message directly to dead-letter state. |
| `Redrive` | Reactivate only the message whose current dead-letter history ID exactly matches. |
| `Maintain` | Deterministically promote schedules and process lease, TTL, max-age, retry, and expiry boundaries. |

Version 1 accepts only partition `0`. It rejects unknown fields, unsupported
versions, non-canonical JSON, mismatched scope, and payloads above 512 KiB.
Idempotency keys are limited to 128 bytes; message IDs to 1,024 bytes; consumer
identities to 256 bytes; reasons to 4 KiB; lease tokens to 4 KiB; and acquire
batches to 1–100 messages. Required text cannot be blank or contain control
characters. Visibility timeouts, consumer epochs, lease extensions, and
dead-letter history IDs must be non-zero where present.

A domain-separated SHA-256 prefix maps the full tablet scope and idempotency key
to the current 64-bit proposal ID. The canonical command retains the complete
key, and exact replay validation retains the complete payload digest and commit
metadata, so conflicting reuse fails closed. This experimental identifier is
not a frozen public format.

## Authoritative lease fencing

The tablet derives a lease fence from values it already trusts:

```text
format version + tablet ID + tablet epoch + partition
  + committed leader term + accepted consumer epoch
```

The opaque token also binds the consumer identity, message ID, lease
generation, and deadline. Its checksum detects corruption and non-canonical
encoding; it is not a signature or authentication mechanism.

Consumer epochs are monotonic per consumer identity. An equal epoch may
continue; a higher epoch replaces the current one; a lower epoch is recorded as
fenced. Settlement requires all of the following:

1. the supplied consumer epoch is the tablet's current epoch for that consumer;
2. the token's consumer identity matches the command;
3. the token's complete fence equals the tablet scope, partition, current
   committed term, and consumer epoch; and
4. the complete token is still the Queue's current live lease.

A term change therefore fences settlement with an old-leader token. It does not
prematurely create a second owner: a new-term acquire remains empty before the
old deadline, and maintenance at the exclusive deadline makes the message
eligible for a new fenced lease.

## Monotonic applied time

All time-dependent decisions use `applied_at_ms` captured in the command before
proposal. Replicas never sample their local wall clock during application.
Application accepts equal timestamps and rejects a regression before changing
state. The future leader/runtime integration must choose a value no lower than
the last applied value; that assignment protocol is not implemented yet.

Lease deadlines are exclusive: settlement is valid before the deadline and is
fenced at the deadline. Acquire and renewal clamp the lease to the earliest TTL
or configured max-age terminal boundary. Renewal must produce a strictly later
deadline and rotates the token so its embedded deadline remains truthful.
Backoff jitter is derived deterministically, and `Maintain` is an explicit
committed command rather than background-clock behavior.

## Recorded outcomes and fail-stop errors

The tablet applies each command to a cloned candidate. Successful transitions
swap that candidate into live state. Deterministic business failures—already
exists, not found, invalid argument, conflict, fenced, capacity, or
unavailable—do not swap it. They are nevertheless part of committed history as
a typed `Rejected` outcome and advance applied index, applied time, idempotency
state, and digest. A committed rejection is not evidence that the proposal did
not commit.

Structural divergence remains a `TabletError` and must stop the future
consensus actor: wrong group or epoch, non-canonical or invalid command bytes,
proposal mismatch, commit-order regression, applied-time regression,
conflicting exact-replay metadata, local storage failure, or internal profile
failure. Those errors do not mutate the tablet.

## Exact replay and lease renewal

An exact retry must present the same proposal ID, term, log index, and payload.
It returns the stored receipt with disposition `replayed` and does not mutate
the Queue, histories, counters, applied count, or digest. Any difference is a
conflicting committed command and fails closed.

This makes renewal retry-safe at the tablet boundary. The first committed
`ExtendLease` rotates the token and stores the exact new token and deadline in
its receipt; replay returns that same result even though the old token is no
longer live. A new command attempting to use the superseded token is fenced.

## Dead-letter and redrive history

Each transition into dead-letter state appends a new monotonically identified
history record containing the source proposal ID, committed term and index,
full original envelope, reason, enqueue/dead-letter times, attempt count, and
last error. Redrive removes only the active dead-letter marker. It never deletes
or edits an earlier history record.

Redrive requires both message ID and the exact currently active dead-letter
history ID. A stale ID is a recorded fenced rejection. A successful redrive
appends its own immutable history record with the source proposal, term, index,
time, and referenced dead-letter history. Checked counters fail closed rather
than wrap. These histories are currently memory-resident and are reconstructed
only by replaying the same committed commands; snapshots and bounded retention
do not exist yet.

## Digest and receipt evidence

The initial digest commits to the Queue tablet domain, scope, and normalized
configuration. Every transition then hashes the previous digest, proposal ID,
term, log index, payload digest, the complete Queue recovery checksum, encoded
consumer/DLQ/redrive auxiliary state, applied time, and exact applied or rejected
outcome. Three independent tablets given the same committed history produce
identical receipts, Queue checksums, histories, counts, and state digests.

Receipts serialize all 64-bit identity, term, position, deadline, time, and
history values as decimal strings for browser safety. A receipt currently names
`fixed_voter_majority_persisted` and two durable voter acknowledgements because
that is the bounded evidence expected from the fixed-three-voter adapter before
application. The Queue tablet does not establish those facts itself, is not yet
wired to that adapter, and does not claim the PRD's placement-aware
`quorum_durable` profile.

## Verification

Run the crate-level gates with:

```shell
cargo test --locked -p epoch-queue
cargo test --locked -p epoch-tablet
cargo clippy --locked -p epoch-queue -p epoch-tablet --all-targets --all-features -- -D warnings
```

The deterministic Queue tablet suite covers:

- strict canonical encoding, size bounds, and golden vectors;
- maximum valid token inputs and fail-before-mutation oversized inputs;
- identical enqueue, acquire, reject, redrive, reacquire, and Ack history on
  three independent voters;
- exact renewal replay with the original rotated token;
- stale leader-term and consumer-epoch rejections;
- immutable dead-letter/redrive history and stale-history fencing;
- monotonic applied time and exclusive lease deadlines;
- atomic rejected settlement followed by deterministic redelivery;
- old-lease conservatism and new-term fencing across leader replacement;
- TTL/max-age precedence over scheduled readiness;
- deterministic non-zero retry jitter and progress after a recorded rejection;
- browser-safe nested 64-bit JSON values; and
- complete-outcome digest coverage and a pinned digest vector.

An external integration test also constructs, encodes, proposes, and applies an
enqueue through the crate's public root API while pinning proposal and receipt
JSON vectors and the original Stream public goldens.

## Deliberate limitations

This milestone has no Queue consensus-actor integration, EPRS startup replay,
node listener, peer-runtime selection, HTTP/gRPC endpoint, CLI command, SDK
contract, Compose mode, or end-to-end failover test. The public node remains
standalone and truthfully capped at `local_durable`.

It also has one resource, one tablet, partition `0`, static configuration,
unbounded in-memory idempotency and audit history, no snapshot or compaction,
no catalog-authorized tablet epoch transition, placement, membership change,
consumer-group/session coordinator, read barrier, authenticated peer identity,
token authentication, multi-tenant policy, or exhaustive crash/I/O matrix.
The deterministic three-instance test is application-history evidence, not
proof that three processes durably committed that history.

The standalone Queue WAL and legacy token format remain separate compatibility
paths. Adding this core does not migrate them or raise their guarantee.

See [Architecture](ARCHITECTURE.md), [Semantics](SEMANTICS.md),
[Testing](TESTING.md), [Requirements traceability](REQUIREMENTS_TRACEABILITY.md),
and the [Consensus feasibility spike](CONSENSUS_SPIKE.md) for the surrounding
contract and remaining gates.
