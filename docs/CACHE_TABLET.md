# Experimental Replicated Cache Tablet

**Status:** Deterministic single-shard profile with authenticated regional v1
Go/Java/Python clients and real failover evidence; not a production claim

`epoch-cache`, `epoch-tablet`, and `epoch-node` implement the bounded
non-deferred regional Cache and State contract. A strict internal API proposes canonical
single-shard mutations through the same persistent three-voter actor used by
the Stream and Queue milestones. Every voter rebuilds the Cache from retained
EPRS history before readiness. This remains engineering evidence, not a
placement-aware durable Cache product, and it does not change the standalone
node's volatile-only Cache guarantee ceiling.

## Boundary

```text
strict internal HTTP DTO + idempotency key + expected current term
  -> leader-role/term validation and compatible CacheTabletCommand proposal
  -> fixed-voter majority persistence and consensus commit
  -> tablet scope, size, and operation validation
  -> committed-order effective time
  -> deterministic CacheShard transaction on staged state
  -> optional advisory lock-guard validation
  -> recorded applied or rejected outcome
  -> exact replay receipt and chained state digest
  -> EPRS rebuild before the internal listener becomes ready
```

- `epoch-cache::CacheShard` owns deterministic values, a shard-global revision,
  pure observation, committed access metadata, bounded transactions, checked
  counters and TTLs, byte/entry admission, advanced transforms and indexes,
  change history, backup/PITR, expiry maintenance, and a canonical recovery
  checksum.
- `epoch-tablet::CacheTablet` owns committed-command validation, scoped
  idempotency, advisory fenced locks, committed receipts, exact replay, and the
  complete replicated state digest.
- `epoch-consensus` remains profile-neutral. `CacheTabletService` supplies its
  committed-proposal applier, fail-stops on structural divergence, installs a
  native checkpoint plus retained tail (or replays a legacy history), and never
  applies a missed commit from an HTTP request task.
- The existing standalone `Cache` remains a separate volatile compatibility
  path. The new shard is additive and does not silently route volatile writes
  through consensus.

The current alpha supports one shard (`shard = 0`), named regional durability,
memory and cold byte classes, every advertised eviction policy, one to 128
distinct-key atomic mutations, and one to 128 independently committed
multiplex mutations. Cross-shard transactions, automatic client coalescing,
RESP compatibility, dynamic placement, production SLOs, and CRDTs remain
outside this slice.

## Regional v1 application API

The authenticated versioned shard route is:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}
```

`GET` on the base performs route discovery. `mutations`, `multiplex`,
`mutations/{proposal_id}`, `observations`, `changes`, `backup`, `query`,
`pubsub/...`, and `status` delegate to the same typed tablet. Data requests require the discovered resource-generation and
tablet-epoch fences; mutations also carry the discovered term and a caller-owned
idempotency key. Observation, lookup, and status reads request a leader
ReadIndex. The repository-local Go, Java, and Python `RegionalCacheClient`
surfaces cover every command and every strict value kind; see
[Regional Cache SDK](REGIONAL_CACHE_SDK.md) and
[ADR-0019](adr/0019-regional-cache-v1-and-sdk-routing.md). The managed Cache
configuration is committed in the catalog, immutable for one resource
generation, and used by every voter during tablet materialization.

## Internal runtime API

The Cache profile is opt-in and mutually exclusive with the opaque, Stream, and
Queue modes for one fixed consensus group. Enable it with
`EPOCH_EXPERIMENTAL_CACHE_TABLET_ENABLED=true` and optionally set
`EPOCH_EXPERIMENTAL_CACHE_TABLET_NAME`. Its routes exist only on the separate
internal listener, which defaults to `127.0.0.1:7701`:

| Method and path | Contract |
| --- | --- |
| `GET /experimental/v1/tablets/cache/status` | Local role, term, consensus/profile positions, revision, retained entries, live locks, and digests. |
| `POST /experimental/v1/tablets/cache/mutations` | Submit one strict typed mutation with `idempotency_key` and `expected_term`. |
| `POST /experimental/v1/tablets/cache/multiplex` | Validate and submit one to 128 independently committed mutations with request-ordered correlated outcomes. |
| `GET /experimental/v1/tablets/cache/mutations/{proposal_id}` | Resolve the local observation as unknown, pending, or committed. |
| `GET /experimental/v1/tablets/cache/observations?key=...` | Pure local observation with explicitly stale-capable consistency. |
| `GET /experimental/v1/tablets/cache/changes` | Read the replicated bounded change history from a sequence cursor. |
| `GET /experimental/v1/tablets/cache/backup` | Export a bounded canonical backup and PITR window. |
| `POST /experimental/v1/tablets/cache/query` | Execute one typed bitmap, probabilistic, geo, JSON, or vector read. |
| `/experimental/v1/tablets/cache/pubsub/...` | Create/delete a node-affine subscription, publish, and drain at-most-once messages. |

Mutation success is returned only after the fixed voter majority has persisted
the entry and the local profile actor has applied it. A new receipt returns
`201`; an exact semantic retry returns `200`; an unresolved proposal returns
`202`. Followers return `not_leader`; a stale expected term returns
`stale_term`. Both have unknown global outcome because lookup and submission
cannot prove that another leader did not commit the deterministic proposal ID.

The HTTP boundary accepts signed and unsigned 64-bit values as JSON numbers or
decimal strings and emits them as decimal strings. It rejects unknown fields,
duplicate set members, duplicate map/sorted-set keys, oversized bodies, and
invalid values before proposal. Direct observations are local and stale-capable
and report `linearizable_read_barrier: false`; a direct status lookup is not a
read barrier. Through the regional resource/shard route, reads default to a
safe leader ReadIndex and report exact barrier term/read/applied indexes.
Callers must explicitly request
`x-epoch-read-consistency: local_stale` to retain the direct behavior. The
internal listener is unauthenticated and has no SDK compatibility commitment;
the regional v1 boundary above owns the application-facing alpha contract.

## Command operations

Every command binds its format version, tablet ID and epoch, resource name,
idempotency key, server-assigned candidate time, and one typed operation:

| Operation | Deterministic effect |
| --- | --- |
| `Set` | Replace or create one value, optionally with a TTL and lock guard. |
| `Get` | Return one value through a committed command and record one LRU/LFU access exactly once. |
| `Delete` | Delete one logically live value, optionally with a lock guard. |
| `CompareAndSet` | Require absence at a named shard revision or an exact item version, then write. |
| `Increment` | Checked signed-counter addition; a missing counter is created. |
| `Transaction` | Atomically stage 1–128 ordered distinct-key mutations against one expected shard revision; SDKs also expose it as `AtomicBatch`. |
| `AcquireLock` | Acquire an absent or expired advisory lock and return lease and fencing tokens. |
| `RenewLock` | Validate the current owner, leader term, and lease token, extend the deadline, and rotate the lease token. |
| `ReleaseLock` | Validate and remove the exact current lock. |
| `Maintain` | Deterministically reclaim a bounded number of expired values and locks. |

Unknown fields, non-canonical JSON, unsupported versions, invalid scope,
oversized payloads, invalid values, and operations outside the bounded v1
surface fail before application. HTTP 64-bit JSON outputs use decimal strings
so browsers do not lose precision.

## Revisions, access, CAS, and transactions

The shard begins at revision `0`. Each successful state-changing value batch
advances the checked `u64` revision exactly once, and every value written by
that batch receives the same revision as its item version. Rejected batches and
no-op deletes do not advance it.

`Get` is the version-2 committed-access command. Under LRU/LFU it advances the
shard revision and updates access metadata once; exact idempotent replay does
neither again. Under policies that do not consume access metadata it returns a
committed receipt without changing the engine revision. `Observe` remains a
pure read and never changes victim order.

An optimistic transaction supplies the exact shard revision it observed. A
different revision rejects the entire transaction. Each key may appear only
once, which makes capacity accounting and ordered results unambiguous. The
engine validates every version condition, counter addition, deadline,
operation, and final capacity/eviction result against cloned state before it
swaps the result into the live shard. Any error leaves values, access metadata,
expiry state, locks, and revision unchanged. A successful receipt includes
evicted keys in canonical order.

The policy family is `no_eviction`, all-key LRU/LFU/random, and volatile
LRU/LFU/random/TTL. LRU and LFU consume committed access metadata; TTL selects
the earliest deadline; random uses a domain-separated SHA-256 rank over the
next revision and key. Canonical key order breaks ties. Volatile policies may
choose only expiring keys and reject atomically if the eligible set cannot make
room. Mutation targets are protected from becoming their own victims.

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
may still commit after a leadership change. New HTTP mutations include an
expected term that the actor checks atomically with leader role before proposal;
this fences newly admitted stale-term requests without changing already-appended
Raft history. The old lock nevertheless remains reserved until its deadline, so
a new leader cannot create a second owner early. Locks are advisory unless a
mutation supplies a guard, in which case guard validation and the value mutation
are one atomic transition.

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

At the HTTP boundary, the semantic retry identity is the scoped idempotency key
plus the typed operation. The expected term and server-assigned time are not
client semantics. A request matching a pending or committed command returns its
stored state/receipt; changing any operation input returns
`idempotency_conflict` without rebinding the proposal ID.

The initial digest commits to the Cache domain, tablet scope, and normalized
configuration. Cache shard snapshot v4 persists eviction/access metadata,
storage classes and byte counts, advanced values, changes, and PITR history;
v1 through v3 remain readable. Historical command and golden bytes remain
compatible. Every
transition chains the previous digest with committed
metadata, the payload digest, effective time, canonical sorted values, canonical
sorted locks, shard revision, and the complete applied or rejected outcome.
Independent tablets replaying one committed history must produce identical
receipts, observations, lock state, recovery checksums, and digests.

## Runtime recovery and read scope

On startup the consensus runtime opens and validates EPRS, derives the complete
committed proposal history, and asks `CacheTabletService` to replay it in log
order into a fresh tablet before routing becomes ready. Live commands are
profile-applied synchronously on the consensus actor before commit notification
or successful lookup. A malformed history, commit-order mismatch, or lookup of
a commit without its actor-applied receipt fails the profile closed and drains
the node rather than serving divergent state.

Status samples the profile before the later actor-owned consensus snapshot, so
it rejects an impossible profile index ahead of the reported consensus-applied
index. Cache observations never advance time or reclaim storage. Expired values
are logically absent, while `retained_entry_count` includes their physical
storage until a committed `Maintain` command reclaims them. The regional
wrapper's current leader automatically proposes that command at the first due
value or lock deadline and provides leader ReadIndex ordering; the direct
profile route remains stale-capable and has no scheduler.

## Advanced state and query contract

The `Transform` operation performs collection mutation and the bitmap,
cardinality, Bloom, Cuckoo, geo, JSON, JSON-index, and vector-index mutations
documented in [Regional Cache SDK](REGIONAL_CACHE_SDK.md). It stages type,
version, lock, TTL, entry, byte, and eviction checks before replacing state.
Every transform can be used directly or inside a transaction and has a bounded,
canonical representation.

Advanced reads use a leader ReadIndex and never mutate access metadata. JSON
secondary indexes use exact canonical pointer values. Vector search exhaustively
scores the bounded index with cosine similarity, token overlap, and exact
metadata filters. These are correctness implementations, not ANN, recall, or
latency benchmarks.

## Changes, backup, and restore

The state machine retains the newest 1,024 changes, including explicit
mutations, automatic expiry, eviction, and restore. A read publishes the
retained floor and fails a stale cursor instead of silently skipping history.

Backup exports a canonical checksummed artifact no larger than 320 KiB. The
artifact binds configuration, captured revision/time, state digest, snapshot,
and the retained restoration window. Restore is one committed command: it
validates the artifact and requested revision, reconstructs state at that
revision, excludes values expired at restore time, revalidates capacity, and
assigns fresh versions to prevent ABA. This is not a scheduled/encrypted
managed backup catalog or an all-cluster disaster-recovery claim.

## Pub/Sub and cold values

Pub/Sub is deliberately outside replicated state. Exact-channel and `*`/`?`
pattern subscriptions are node-local, at-most-once, drain on poll, and drop new
messages when a bounded pending queue is full. Subscription creation reports
node affinity, no persistence, and the pending limit. Process loss or rerouting
loses subscriptions and pending messages.

Memory and cold classes have separate deterministic byte caps. After every
committed transition, each regional voter fsyncs canonical cold-value files and
removes stale files before exposing the transition. Cold observations/queries
load and integrity-check the file against replicated canonical state. Recovery
and checkpoint installation rebuild that directory. Status reports observed
local-file read timing and explicitly labels it as not an SLO. Canonical cold
values remain in the tablet image, so this alpha does not claim heap offload or
production flash capacity relief.

## Verification scope

The deterministic and runtime gates cover:

- strict command decoding, bounds, scope, proposal identity, and browser-safe
  receipt encoding;
- pure observations and deterministic expiry order;
- committed access, exact-retry stability, and LRU/LFU ordering;
- deterministic all-key/volatile LRU, LFU, random, and TTL eviction plus
  atomic no-eligible-victim rollback;
- non-repeating versions across delete/recreate and expiry/recreate;
- CAS success and mismatch, absent-state ABA fencing, and expected-revision
  conflicts;
- atomic multi-key/batch success plus rollback on version, type, counter,
  deadline, revision, and capacity failures;
- byte admission and deterministic eviction independently for memory/cold
  classes, compatible snapshot recovery, and fsynced cold-file reads/removal;
- collection and advanced transforms, exact query behavior, bounds, index
  consistency, state-digest convergence, and snapshot replay;
- canonical backup corruption/config/window rejection, PITR rollback,
  non-ABA restoration, and durable change cursors;
- non-atomic correlated multiplex validation/replay and explicit node-local
  at-most-once Pub/Sub matching, draining, and overflow;
- lock contention, renewal token rotation, release/reacquisition, exclusive
  deadlines, active-owner epoch fencing, corrupt tokens, old terms, and guarded
  writes;
- descending candidate times, exact replay, commit ordering, malformed committed
  commands, and independent-replay convergence;
- strict recursive HTTP DTOs, browser-safe signed/unsigned 64-bit boundaries,
  semantic idempotency, body limits, follower and stale-term responses, and
  truthful local-read/status labels;
- real three-runtime majority application, fail-stop behavior, convergence, and
  EPRS reopen; and
- three-container leader replacement, ordinary/advanced/byte/cold operations,
  backup/PITR, changes, Pub/Sub, multiplex, voter catch-up, and all-node
  `SIGKILL` recovery.

Run it with:

```shell
cargo test --locked -p epoch-cache
cargo test --locked -p epoch-tablet
cargo test --locked -p epoch-node --all-targets
make test-cache-tablet
cargo clippy --locked -p epoch-cache -p epoch-tablet -p epoch-node --all-targets --all-features -- -D warnings
```

This closes the alpha implementation contract for CACHE-001 through CACHE-014
with fixed-voter, three-language, recovery, and executable documentation
evidence. `CACHE-015` remains explicitly deferred. The closure is scoped to
the named requirements, not to production readiness: concurrent history and
performance reports, follower-served reads, multi-shard routing, dynamic
placement, production identity/TLS, managed backup scheduling/encryption,
package publication, and the exhaustive fault matrix remain required by their
separate PRD rows.
