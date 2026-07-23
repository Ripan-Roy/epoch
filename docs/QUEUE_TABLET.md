# Experimental Replicated Queue Tablet

**Status:** Working opt-in fixed-three-voter runtime and internal typed HTTP API;
experimental and not production-ready

`crates/epoch-tablet` now contains the profile application boundary for one
configured, single-partition Work Queue tablet. `epoch-node` now attaches that
profile to the persistent consensus actor, proposes typed mutations, rebuilds
the profile from EPRS before readiness, and serves a bounded internal HTTP API.
This is a real replicated milestone, not the final public Queue service.

## Implemented boundary

```text
strict typed HTTP mutation
  -> deterministic scoped proposal ID and leader-assigned time
  -> fixed-three-voter persistent Raft proposal
  -> actor-owned committed proposal application
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
- `epoch-consensus` remains profile-neutral. `epoch-node` supplies a Queue
  `CommittedProposalApplier`; recovery replays the complete applied proposal
  history before the runtime reports ready, and live application stays on the
  consensus actor thread.
- `epoch-node` exposes the typed Queue routes only on the separately configured
  internal consensus listener. The standalone volatile/`local_durable` API,
  CLI, and Go/Java/Python SDKs remain separate compatibility paths.

The embedded Queue is forced to `volatile` because the containing consensus
history, not a second profile WAL, is intended to own clustered persistence.
The tablet crate still accepts only a `CommittedCommand`; the node/consensus
boundary establishes the bounded fixed-voter persistence evidence before it
invokes the tablet.

## Runtime selection and typed routes

Queue mode requires the consensus runtime and is mutually exclusive with Stream
mode for one fixed group:

```shell
export EPOCH_CONSENSUS_PROBE_ENABLED=true
export EPOCH_EXPERIMENTAL_QUEUE_TABLET_ENABLED=true
export EPOCH_EXPERIMENTAL_QUEUE_TABLET_NAME=jobs
```

The existing `EPOCH_CONSENSUS_NODE_ID`, group ID/epoch, peer list, internal
listen address, and stable data path configure the three voters. Opaque
proposal routes are not mounted while a typed profile is selected.

| Method and path | Contract |
| --- | --- |
| `GET /experimental/v1/tablets/queue/status` | Local role/leader/term, consensus and profile positions, applied time/count, Queue counts, and state digest. |
| `POST /experimental/v1/tablets/queue/mutations` | Submit one strict typed operation with `idempotency_key` and `expected_term`. |
| `GET /experimental/v1/tablets/queue/mutations/{proposal_id}` | Resolve the local observation as `unknown`, `pending`, or `committed`. |
| `GET /experimental/v1/tablets/queue/counts` | Read stale-capable local profile counts without advancing time. |
| `GET /experimental/v1/tablets/queue/dead-letters?limit=100` | Read immutable dead-letter history; limit is 1–1,000. |
| `GET /experimental/v1/tablets/queue/redrives?limit=100` | Read immutable redrive history; limit is 1–1,000. |

The mutation body carries no client timestamp:

```json
{
  "idempotency_key": "send-42",
  "expected_term": "7",
  "operation": {
    "kind": "enqueue",
    "partition": 0,
    "envelope": {
      "id": "job-42",
      "source": "example",
      "type": "job.created",
      "time_ms": "1784775000000",
      "payload": {"job": 42}
    }
  }
}
```

`operation.kind` accepts `enqueue`, `acquire`, `acknowledge`, `extend_lease`,
`release`, `nack`, `reject`, `redrive`, and `maintain`, with the fields listed in
the command table below. All 64-bit HTTP inputs accept either an unsigned JSON
number or an exact decimal string. Every 64-bit output is a decimal string.
Unknown top-level, operation, or nested envelope fields are rejected.

The internal listener has no CORS, TLS, authentication, authorization, or SDK
compatibility commitment. It must not be exposed to untrusted networks. The
public standalone listener retains its truthful `local_durable` ceiling.

Writes serialize per node to make time assignment and local request handling
deterministic. The node chooses
`max(local_wall_time_ms, last_applied_time_ms)` before proposal; replicas never
sample clocks while applying. A bounded wait returns `202` with local
`unknown`/`pending` state when commitment is unresolved. Exact semantic retries
return `200` and disposition `replayed`; a new submission returns `201` even
when its committed business outcome is `rejected`. `not_leader`, stale-term,
and idempotency-conflict responses retain globally unknown outcome certainty.

Reads are local, stale-capable, and never propose maintenance. Status samples
the profile before asking the actor for consensus status and fails closed if the
profile could appear ahead of the later consensus snapshot.

## Strict command contract

Every mutation carries format version, tablet ID and epoch, resource name,
idempotency key, leader-assigned `applied_at_ms`, and one typed operation:

| Operation | Operation fields | Deterministic effect |
| --- | --- | --- |
| `Enqueue` | `partition`, `envelope` | Add or deduplicate one immutable envelope. |
| `Acquire` | `partition`, `consumer`, `consumer_epoch`, `max_messages`, optional `visibility_timeout_ms` | Advance a consumer epoch when allowed, select eligible messages, and create fenced leases before returning deliveries. |
| `Acknowledge` | `partition`, `consumer`, `consumer_epoch`, `lease_token` | Terminally settle the exact current lease. |
| `ExtendLease` | settlement fields plus `extension_ms` | Replace the current token with one containing a strictly later, bounded deadline. |
| `Release` | settlement fields, `delay_ms`, optional `reason` | Return the message through the configured immediate or delayed retry path. |
| `Nack` | settlement fields plus `reason` | Record a failure reason and apply deterministic retry/backoff or dead-letter policy. |
| `Reject` | settlement fields plus `reason` | Move the leased message directly to dead-letter state. |
| `Redrive` | `partition`, `message_id`, `dead_letter_history_id` | Reactivate only the message whose current dead-letter history ID exactly matches. |
| `Maintain` | `partition` | Deterministically promote schedules and process lease, TTL, max-age, retry, and expiry boundaries. |

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
state. The runtime chooses a value no lower than the last profile-applied value,
including after wall-clock rollback and EPRS recovery.

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

This makes renewal retry-safe through the HTTP boundary. The first committed
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
`fixed_voter_majority_persisted` and two durable voter acknowledgements. The
node returns that receipt only after the fixed-three-voter adapter has persisted
a majority commit and the local actor has applied it. This is bounded evidence
for a static trusted topology; it does not claim authenticated peers, placement
diversity, or the PRD's production `quorum_durable` profile.

## Verification

Run the crate-level gates with:

```shell
cargo test --locked -p epoch-queue
cargo test --locked -p epoch-tablet
cargo test --locked -p epoch-node
cargo clippy --locked -p epoch-queue -p epoch-tablet -p epoch-node --all-targets --all-features -- -D warnings
make test-queue-tablet
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

The `epoch-node` real-runtime suite exercises strict HTTP extraction, semantic
retry/conflict, server time under wall-clock rollback, committed rejection,
all nine operations, Queue reads, three-voter convergence, and EPRS reopen. The
Docker gate additionally proves scheduled eligibility, follower rejection,
active-leader `SIGKILL`, old-term token fencing, conservative redelivery,
renewal replay, immutable DLQ/redrive reads, all-voter convergence, and exact
state recovery after every container receives `SIGKILL`. CI retains container
logs and EPRS state on failure.

## Deliberate limitations

This milestone still has no public Queue-tablet route, gRPC service, CLI or SDK
surface, streaming credit/prefetch, automatic timer proposal, or production
durability claim. It has one resource, one tablet, partition `0`, static configuration,
unbounded in-memory idempotency and audit history, no snapshot or compaction,
no catalog-authorized tablet epoch transition, placement, membership change,
consumer-group/session coordinator, read barrier, authenticated peer identity,
token authentication, multi-tenant policy, or exhaustive crash/I/O matrix.
The Docker proof covers selected process faults, not every crash point,
filesystem failure, or network partition schedule.

The standalone Queue WAL and legacy token format remain separate compatibility
paths. Adding this core does not migrate them or raise their guarantee.

See [Architecture](ARCHITECTURE.md), [Semantics](SEMANTICS.md),
[Testing](TESTING.md), [Requirements traceability](REQUIREMENTS_TRACEABILITY.md),
and the [Consensus feasibility spike](CONSENSUS_SPIKE.md) for the surrounding
contract and remaining gates.
