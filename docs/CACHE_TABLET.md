# Experimental Replicated Cache Tablet Core

**Status:** Deterministic profile core implemented and tested; not attached to
the node runtime or a public API

`epoch-cache` and `epoch-tablet` implement the bounded first replicated Cache and
State slice. The slice exists to make shard-local mutation, optimistic
concurrency, TTL, and lock fencing deterministic before those semantics are
connected to the fixed-three-voter runtime. It is not yet a deployable durable
Cache profile and does not change the standalone node's volatile-only Cache
guarantee ceiling.

## Boundary

```text
strict canonical CacheTabletCommand v1
  -> tablet scope, size, and operation validation
  -> committed-order effective time
  -> deterministic CacheShard transaction on staged state
  -> optional advisory lock-guard validation
  -> recorded applied or rejected outcome
  -> exact replay receipt and chained state digest
```

- `epoch-cache::CacheShard` owns deterministic values, a shard-global revision,
  pure observation, bounded transactions, checked counters and TTLs, capacity,
  expiry maintenance, and a canonical recovery checksum.
- `epoch-tablet::CacheTablet` owns committed-command validation, scoped
  idempotency, advisory fenced locks, committed receipts, exact replay, and the
  complete replicated state digest.
- `epoch-consensus` remains profile-neutral. Attaching this tablet to the node,
  recovering it from EPRS, and proving three-process failover are the next
  milestone; none is implied by the core-only tests.
- The existing standalone `Cache` remains a separate volatile compatibility
  path. The new shard is additive and does not silently route volatile writes
  through consensus.

Version 1 supports one shard (`shard = 0`), no eviction, and at most 128
distinct-key mutations in one transaction. Cross-shard transactions, snapshot
restore, change capture, collection-specific mutations, and compatibility
protocols remain outside this slice.

## Command operations

Every command binds its format version, tablet ID and epoch, resource name,
idempotency key, server-assigned candidate time, and one typed operation:

| Operation | Deterministic effect |
| --- | --- |
| `Set` | Replace or create one value, optionally with a TTL and lock guard. |
| `Delete` | Delete one logically live value, optionally with a lock guard. |
| `CompareAndSet` | Require absence at a named shard revision or an exact item version, then write. |
| `Increment` | Checked signed-counter addition; a missing counter is created. |
| `Transaction` | Atomically stage 1–128 distinct-key mutations against one expected shard revision. |
| `AcquireLock` | Acquire an absent or expired advisory lock and return lease and fencing tokens. |
| `RenewLock` | Validate the current owner, leader term, and lease token, extend the deadline, and rotate the lease token. |
| `ReleaseLock` | Validate and remove the exact current lock. |
| `Maintain` | Deterministically reclaim a bounded number of expired values and locks. |

Unknown fields, non-canonical JSON, unsupported versions, invalid scope,
oversized payloads, invalid values, and operations outside the bounded v1
surface fail before application. Public 64-bit JSON outputs use decimal strings
so browsers do not lose precision.

## Revisions, CAS, and transactions

The shard begins at revision `0`. Each successful state-changing value batch
advances the checked `u64` revision exactly once, and every value written by
that batch receives the same revision as its item version. Rejected batches and
no-op deletes do not advance it.

An optimistic transaction supplies the exact shard revision it observed. A
different revision rejects the entire transaction. Each key may appear only
once, which makes capacity accounting and ordered results unambiguous. The
engine validates every version condition, counter addition, deadline,
operation, and final no-eviction capacity limit against cloned state before it
swaps the result into the live shard. Any error leaves values, expiry state,
locks, and revision unchanged.

Item versions are allocated from the shard-global revision rather than from a
per-key counter. They therefore do not reset when a key is deleted or expires.
An expected item version of `0` means the key must be logically absent. The
tablet's explicit `missing` CAS expectation additionally carries the observed
shard revision, preventing an absent-create-delete ABA cycle from being
mistaken for unchanged absence without retaining an unbounded tombstone.

Increment preserves an existing counter's expiry. A missing counter uses the
explicit TTL when supplied or the resource default. Arithmetic, revision, and
deadline overflow are capacity rejections; they never wrap or saturate.

## Expiry and committed time

Observation is pure. An item is visible only while
`observed_at_ms < expires_at_ms`; observing an expired entry does not mutate the
map, revision, recency metadata, or digest. Explicit maintenance reclaims due
values in stable `(deadline, key)` order and is bounded to 1,000 entries.

Replicas never sample local clocks while applying. Each leader embeds a
candidate `applied_at_ms`, and application derives:

```text
effective_applied_at = max(command.applied_at_ms, previous_effective_applied_at)
```

The effective time advances for both applied and rejected committed commands.
This preserves identical TTL and lease decisions through clock rollback,
leader replacement, and replay, including when an earlier higher-time pending
entry commits before a lower-clock leader's entry.

## Advisory lock fencing

A successful acquisition returns two different values:

- an opaque lease token used to prove the exact current owner to Epoch; and
- a fencing token ordered lexicographically as
  `(tablet_epoch, acquisition_index)` for downstream systems.

The opaque token binds the tablet and shard, lock key, owner and owner epoch,
acquisition log index, committed leader term, lease generation, and exclusive
deadline. Its checksum detects corruption and non-canonical encoding; it is not
a signature or authentication mechanism.

The owner epoch is a bounded active-session fence. While an owner has any live
lock, a lower epoch for that owner is fenced across lock keys. Epoch history is
reclaimed after the owner's last lock is released or reaches its exclusive
deadline; a later acquisition may therefore use a lower owner epoch, but it
receives a strictly higher acquisition index. The composite acquisition
fence—not an unbounded owner tombstone—is the durable ordering value for
downstream systems.

Renewal preserves the fencing token but rotates the lease token. Release,
expiry, or renewal makes the old lease token unusable. A later acquisition has
a larger committed log index, or a larger tablet epoch after reincarnation. At
the deadline the lease is expired.

A lease token can authorize only a committed command carrying the same Raft
entry term under which the token was created or renewed. It cannot authorize a
new command admitted in a different term. An already-appended same-term command
may still commit after a leadership change; stronger current-leader fencing
belongs to the pending runtime barrier. The old lock nevertheless remains
reserved until its deadline, so a new leader cannot create a second owner early.
Locks are advisory unless a mutation supplies a guard, in which case guard
validation and the value mutation are one atomic transition.

Callers must propagate the fencing token to the protected downstream resource
and have that resource reject tokens older than the greatest token it has
accepted. A time-valid lease token without downstream fencing is not sufficient
protection against a delayed former owner.

## Outcomes, replay, and digest

Deterministic business failures are committed as typed rejections: already
exists, not found, invalid argument, conflict, fenced, capacity, or unavailable.
A committed rejection still advances the tablet's applied index, effective
time, idempotency history, and transition digest, but does not swap the staged
business state.

An exact retry must present identical proposal ID, leader term, log index, and
payload. It returns the original receipt with `replayed` disposition and does
not mutate revision, locks, values, counters, applied count, or digest. Reusing
the proposal ID with different committed metadata or bytes fails closed.

The initial digest commits to the Cache domain, tablet scope, and normalized
configuration. Every transition chains the previous digest with committed
metadata, the payload digest, effective time, canonical sorted values, canonical
sorted locks, shard revision, and the complete applied or rejected outcome.
Independent tablets replaying one committed history must produce identical
receipts, observations, lock state, recovery checksums, and digests.

## Verification scope

The core gate covers:

- strict command decoding, bounds, scope, proposal identity, and browser-safe
  receipt encoding;
- pure observations and deterministic expiry order;
- non-repeating versions across delete/recreate and expiry/recreate;
- CAS success and mismatch, absent-state ABA fencing, and expected-revision
  conflicts;
- atomic multi-key success plus rollback on version, type, counter, deadline,
  revision, and capacity failures;
- lock contention, renewal token rotation, release/reacquisition, exclusive
  deadlines, active-owner epoch fencing, corrupt tokens, old terms, and guarded
  writes;
- descending candidate times, exact replay, commit ordering, malformed committed
  commands, and independent-replay convergence.

Run it with:

```shell
cargo test --locked -p epoch-cache
cargo test --locked -p epoch-tablet
cargo clippy --locked -p epoch-cache -p epoch-tablet --all-targets --all-features -- -D warnings
```

This evidence moves CACHE-006 from planned to a tested core slice. It does not
complete CACHE-006 or CACHE-007: the node runtime, typed transport, concurrent
history checker, fixed-voter failover/reopen gate, snapshot path, authenticated
transport, production placement, and exhaustive fault matrix remain required.
The in-memory exact-replay map also retains one complete receipt per unique
proposal without a retention window; large overwritten values can therefore
outlive the current Cache entry. Bounded idempotency retention and its advertised
retry window are required before this core can become a long-running service.
