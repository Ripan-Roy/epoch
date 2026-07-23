# Epoch API Contracts

**Status:** Target contract; Protobuf definitions not yet frozen  
**Date:** 22 July 2026

This document defines the native data, regional administration, and hosted
management API shape. It is intentionally more concrete than the product
catalog in [PRD.md](PRD.md), while leaving exact field numbers to the versioned
files under `spec/proto`. Observable behavior is defined in
[SEMANTICS.md](SEMANTICS.md), trust boundaries in [SECURITY.md](SECURITY.md), and
component ownership in [ARCHITECTURE.md](ARCHITECTURE.md).

No current HTTP scaffold route or Rust structure is a frozen public contract.

## 1. API surfaces

Epoch has three contract families:

1. **Native data API:** high-throughput gRPC with streaming and batching;
   selected low-rate operations may have HTTP/JSON mappings.
2. **Regional administration API:** Rust-owned gRPC for resources, placement,
   operations, backup, restore, drain, and cluster lifecycle.
3. **Hosted management API:** Go-owned REST/JSON for the console, Terraform, and
   customer automation, plus private gRPC between Go services.

Compatibility gateways translate RESP3, Kafka, AMQP, MQTT, CloudEvents, and
future cloud-compatible HTTP facades into the native typed operations. They are
not alternate internal state owners.

Proposed package layout:

```text
epoch.common.v1
epoch.cache.v1
epoch.stream.v1
epoch.queue.v1
epoch.bus.v1
epoch.schema.v1
epoch.transaction.v1
epoch.admin.v1
epoch.control.v1
```

## 2. Naming and identity

The managed canonical name is:

```text
organizations/{org}/projects/{project}/environments/{environment}/
namespaces/{namespace}/{collection}/{resource}
```

Collections include `caches`, `tables`, `streams`, `queues`, `buses`,
`subscriptions`, `schemas`, `pipes`, `connectors`, and `policies`. Standalone
mode permits a local shorthand, but responses return a canonical name beneath a
synthetic local organization/project/environment.

Names are human-readable and unique within their parent. IDs are immutable,
opaque 128-bit values in a canonical text encoding. SDKs must not infer time,
region, or ordering from an ID even if an implementation uses a time-sortable
encoding.

The following identities are distinct:

- request ID for tracing;
- idempotency/request token for semantic deduplication;
- event or message ID;
- resource generation and tablet/leader epoch;
- producer, consumer-group, session, lease, and transaction epochs;
- logical stream offset or commit position.

## 3. Common messages

### Request context

Every mutation carries or derives:

- `request_id`;
- `idempotency_key` for data mutations or `request_token` for administration;
- client library/version and negotiated capabilities;
- deadline;
- trace context;
- expected resource generation, tablet epoch, producer epoch, session epoch, or
  lease token where the operation requires it.

Authentication identity is derived from the TLS connection and authenticated
metadata. A client cannot assert its own principal, roles, tenant, or internal
forwarding context in a request body.

### Event envelope

The native envelope contains:

- opaque event ID;
- source, type, optional subject, and event time;
- optional partition/ordering key;
- bounded string or byte headers;
- content type and schema reference;
- W3C trace context;
- raw `bytes` payload;
- optional deliver-at, TTL, priority, dedupe, and transaction attributes;
- bounded namespaced protocol extensions as bytes.

The payload is not `google.protobuf.Struct`. JSON is one content type. Gateways
must preserve an unsupported protocol field in a declared extension or report a
lossy translation; they must not silently discard it.

### Write receipt

Every successful mutation returns a common receipt with:

- request/idempotency token and immutable record identity;
- resource ID, generation, and tablet/leader epoch;
- logical position, partition, and offset where applicable;
- configured and achieved durability;
- replica acknowledgement count and commit time;
- `NEW`, `DUPLICATE`, or `REPLAYED` disposition;
- original position for a duplicate;
- route-plan or schema version where applicable;
- current geo checkpoint/lag when requested.

A receipt is the proof of the stated success point. A 2xx/OK response without a
receipt is not a successful native write.

### Resource representation

Administrative resources use:

```text
metadata: id, name, parent, labels, tags, generation, create/update/delete times
spec:     desired typed profile, guarantees, limits, placement, policy references
status:   observed_generation, conditions, achieved placement, endpoints, operation
```

`spec` is declarative. `status` is service-owned and cannot be supplied by a
client. Profile kind and immutable identity cannot change in place.

## 4. Native services

### CacheService

- `Get` and `BatchGet`
- `Mutate` with a typed operation union for Set/Delete/Increment/CAS and data
  structure operations
- `BatchMutate` with declared atomicity and routing key
- `Scan` with opaque, bounded-lifetime continuation token
- `WatchChanges` for resources with change capture enabled

A request for atomic cross-shard mutation is rejected unless it names a
supported transaction. Replica reads require an explicit consistency choice.

### StreamService

- bidirectional `Produce` with per-record results and bounded in-flight credit;
- server-streaming `Fetch` with partition, start position, isolation, byte and
  record limits;
- `ListOffsets` for earliest/latest/time lookup;
- `ConsumerSession` for native group join, heartbeats, assignment, revoke, and
  progress;
- `CommitOffsets`, `FetchOffsets`, and explicit `ResetOffsets`.

One bad record does not make an entire non-atomic Produce batch ambiguous;
results are correlated by client sequence. A transactional batch follows the
transaction result instead.

### QueueService

- bidirectional `Send` with per-message receipts;
- bidirectional `Receive`, where clients grant and replenish delivery credit;
- batch `Settle` with Ack/Nack/Release/Reject and a per-item result;
- batch `ExtendLease`;
- `GetMessage`/`Peek` for authorized diagnostics;
- administration-only preview/redrive methods on the regional API.

Each delivery carries an opaque lease token. Clients return the token unchanged
and must not parse it. Batch settlement is not all-or-nothing unless an explicit
same-tablet transaction is requested.

### BusService

- bidirectional `Publish` with ingress receipts;
- `Pull`/`StreamingPull` for pull subscriptions;
- settlement operations that reuse the Queue lease contract;
- archive search and replay through audited, long-running administration
  operations.

Subscription, filter, transform, target, webhook, and retry configuration are
resources, not ad hoc Publish parameters.

### SchemaService

- `ResolveSchema` and `Validate` on the data path;
- revision, compatibility, ownership, deprecation, and discovery operations on
  the administration path.

A resource chooses producer, broker, both, or disabled validation. A rejection
returns schema revision and bounded violation details without reflecting secret
or excessively large payload content.

### TransactionService

- `InitProducer` returns producer ID and epoch;
- `Begin` returns a bounded transaction ID/deadline;
- `AddParticipants` is internal or capability-gated;
- `Commit`, `Abort`, and `GetTransaction` resolve the durable coordinator
  decision.

Commit timeout is an unknown outcome until `GetTransaction` returns committed
or aborted. Clients cannot assume timeout means abort.

### MutationStatusService

`LookupMutation` accepts resource, operation kind, principal scope, and
idempotency token. It returns:

- `PENDING`;
- `COMMITTED` plus the original receipt;
- `REJECTED` plus the definite error;
- `NOT_COMMITTED`, when the authority can prove no commit and resubmission with
  the same token is safe;
- `EXPIRED_UNRESOLVED`, when evidence aged out before resolution.

## 5. Regional administration

`RegionalAdminService` provides:

- `PlanResourceChange` and `ApplyResource`;
- `Get`, `List`, and `WatchResources`;
- soft `DeleteResource` and separately authorized `PurgeResource`;
- `DrainNode`, `TransferLeader`, `Rebalance`, `Repair`, `Split`, and supported
  merge operations;
- `CreateBackup`, `ValidateBackup`, `Restore`, and point-in-time operations;
- cluster membership, version/capability, policy-bundle, and schema management.

Every mutation has a request token and expected generation. Applying the same
token and semantic spec returns the same operation. Reusing it with a different
spec is a conflict.

Risky work returns an `Operation` with state:

```text
PENDING -> RUNNING -> SUCCEEDED
                  -> FAILED
                  -> CANCELLING -> CANCELLED
```

Cancellation is best effort. An operation reports whether a point of no return
was crossed. Delete is recoverable during its configured window; Purge is a
separate irreversible operation with stronger authorization and preview.

List methods use opaque page tokens bound to query, scope, and a bounded
snapshot. Watch resumes from an opaque resource version and returns an explicit
compaction error when that version is no longer available.

## 6. Hosted management API

The Go API exposes organization/project/environment lifecycle, entitlements,
global desired topology, budgets, billing, fleet operations, and console views.
It stores a desired generation and reconciles through the Rust regional API.

Hosted success means the desired change is durably accepted by the management
system. A resource is ready only when regional `observed_generation` matches and
its conditions satisfy the requested placement and guarantee. APIs and the
console must distinguish `accepted`, `reconciling`, `ready`, `degraded`, and
`failed`.

Go does not expose or synthesize data-path receipts and never reads Epoch data
files.

## 7. Typed errors

Errors use `google.rpc.Status` with stable typed details. Message strings are for
humans and are not a retry contract.

| Detail | Canonical gRPC code | Meaning | Default client action |
|---|---|---|---|
| `NotLeader` | `UNAVAILABLE` | Route/leader epoch is stale | Refresh/redirect and retry same token |
| `Fenced` | `FAILED_PRECONDITION` | A newer owner epoch exists | Rejoin or reacquire; do not replay as old owner |
| `QuorumUnavailable` | `UNAVAILABLE` | Required commit set is unavailable | Back off; retry same token |
| `UnknownCommit` | `ABORTED` | Request may have committed | Lookup, then retry same token only |
| `Throttled` | `RESOURCE_EXHAUSTED` | Named quota/resource is limiting | Honor retry-after and reduce load |
| `SchemaRejected` | `INVALID_ARGUMENT` | Payload violates selected revision/policy | Correct input; do not retry unchanged |
| `Conflict` | `ABORTED` | Generation, CAS value, or token fingerprint differs | Read current state and reconcile |
| `UnsupportedSemantic` | `UNIMPLEMENTED` | Requested guarantee/translation is not supported | Change request; never silently downgrade |
| `PlacementUnsatisfied` | `FAILED_PRECONDITION` | Topology cannot meet resource policy | Change placement/capacity or wait for repair |
| `LeaseLost` | `FAILED_PRECONDITION` | Lease is expired, settled, or fenced | Stop processing/settling that delivery |
| `TransactionAborted` | `ABORTED` | Coordinator durably aborted | Begin a new transaction if safe |
| `OffsetOutOfRange` | `OUT_OF_RANGE` | Requested data is no longer retained or not yet valid | Use returned earliest/latest bounds |
| `CapabilityMismatch` | `FAILED_PRECONDITION` | Client/node versions cannot provide a feature | Negotiate supported capability or upgrade |
| `RecordTooLarge` | `RESOURCE_EXHAUSTED` | Payload exceeds named limit | Reduce or use object-reference pattern |
| `DataCorruption` | `DATA_LOSS` | Verification failed and no safe result exists | Do not auto-retry writes; escalate/repair |

Standard `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, and
`INVALID_ARGUMENT` codes retain their normal meanings and are not automatically
retryable.

Error details include request/resource identity, observed and required epochs,
retry-after, safe endpoint hints, current generation, limiting quota, and a
bounded diagnostic ID as relevant. They never echo credentials or unrestricted
payload content.

For a mutation, a server-generated error also declares outcome certainty when
known: `DEFINITE_NOT_COMMITTED` or `UNKNOWN`. For example, admission throttling
before proposal is definite, while quorum loss after append is unknown and is
paired with `UnknownCommit`. A connection loss that prevents the detail from
arriving is always treated as unknown.

## 8. Retry and cancellation contract

SDKs implement these rules:

1. Reads may retry on `NotLeader` or transient `UNAVAILABLE` within the original
   deadline and consistency request.
2. A mutation is automatically retried only with the identical idempotency token
   and semantic payload.
3. A transport loss after bytes were sent is treated as unknown even if no
   server detail was received.
4. `UnknownCommit` triggers status lookup before a business operation creates a
   new token.
5. `Fenced`, `LeaseLost`, authorization, validation, and data-corruption errors
   are not blind-retryable.
6. `Throttled` honors server retry-after plus jitter and consumes no hidden
   unbounded retry budget.
7. Ack retry uses the same lease token. A consumer never turns `LeaseLost` into
   Ack success.
8. Transaction Commit/Abort timeout is resolved through transaction lookup.

Cancellation stops client interest; it does not roll back a mutation that may
already be committed. The status lookup path remains available for the token.
SDKs expose retry budget, attempt count, final detail, and original receipt.

## 9. Idempotency retention

The service retains token outcome and payload fingerprint for at least the
resource's advertised idempotency window. The response exposes the expiry where
useful. A caller that needs a longer business dedupe window must use a durable
business key or inbox/outbox; it cannot assume Epoch remembers every token
forever.

Tokens are tenant-scoped and are never deduplicated across principals or
namespaces. A protocol gateway maps native producer sequences, message IDs, or
dedupe IDs into this mechanism without broadening their documented scope.

## 10. Webhook delivery contract

Webhook attempts include stable delivery/event IDs, attempt number, target
subscription, timestamp, content type, trace context, idempotency key, and a
versioned signature. Security and URL restrictions are defined in
[SECURITY.md](SECURITY.md).

By default:

- configured 2xx responses acknowledge delivery;
- 429 and 5xx responses are retryable;
- network timeout/reset is unknown to the target and therefore retryable with
  the same delivery ID;
- other 4xx responses are terminal and dead-lettered unless policy explicitly
  classifies them otherwise;
- redirects are not followed by default.

Response bodies are size-bounded and never interpreted as commands unless a
future connector contract explicitly defines that behavior.

## 11. Versioning and compatibility

Within a `v1` Protobuf package, changes are additive. Field numbers and enum
values are never reused. Unknown enum values survive translation or fail with a
capability detail where acting without understanding them is unsafe.

During a rolling upgrade, clients and nodes negotiate capabilities. A feature is
enabled only when every required participant can read and preserve it. Buf
breaking checks, generated-code freshness, golden wire fixtures, and named
client-version suites run in CI.

Compatibility gateways publish four states for each behavior: supported,
partially supported, translated, and unsupported. A request that cannot preserve
the selected durability, ordering, settlement, or transaction behavior fails
explicitly.

## 12. What is implemented now

The current scaffold exposes provisional Rust domain structs, in-memory profile
methods, standalone receipt metadata, JSON/HTTP profile routes, a CLI,
`/healthz` and `/readyz`, and a small local WAL. The two health routes currently
report the same in-process engine state.

On a fresh data directory, the runnable node opens one exclusively owned
segmented WAL at `$EPOCH_DATA_DIR/engine-wal/segment-*.wal`; `engine.wal` is its
activation marker and cross-version lock. The node reports a `local_durable`
guarantee ceiling. Streams and Queues may select `volatile` or `local_durable`.
Durable Stream creation, append, and offset mutations and durable Queue
creation, enqueue, lease, settlement, redrive, and maintenance commands are
fsynced before becoming visible and replayed on restart. Cache and Event Bus
still accept only `volatile`, and every replication or geo mode is rejected.

The v1 frames retain their checksum and global sequence across segment
boundaries. Segment rotation targets `--wal-segment-bytes` /
`EPOCH_WAL_SEGMENT_BYTES` (64 MiB by default), but rotation is not retention or
compaction. A frame is never split, so one frame larger than the target may
occupy an otherwise empty segment. A versioned, checksummed manifest records the
exact committed topology, lengths, sequences, and file checksums. Recovery may
discard only an uncommitted suffix of the active segment. Missing or truncated
committed data, an unexpected or changed segment, metadata mismatch, checksum
failure, or sequence discontinuity fails startup.

A pre-existing valid legacy `$EPOCH_DATA_DIR/engine.wal` remains the active
single-file WAL. The current binary replays and continues appending to it and
does not create `engine-wal/`, preserving safe offline downgrade behavior.
Fresh segmented activation replaces an invalid-to-old-readers staging marker
only after the new layout is durable. Mixed histories without that marker are
rejected; legacy migration is not yet automatic.

When explicitly enabled, a separate internal listener exposes the experimental
fixed-voter consensus probe:

- `POST /internal/v1/consensus/messages` accepts only bounded Epoch peer frames
  with `application/octet-stream`;
- `GET /experimental/v1/consensus/status` reports the local role, leader, term,
  commit/applied indexes, cumulative per-peer queue/delivery/drop evidence, and
  explicit non-production capability fields;
- `POST /experimental/v1/consensus/proposals` proposes opaque diagnostic bytes
  with a caller-supplied proposal ID and expected term; and
- `GET /experimental/v1/consensus/proposals/{proposal_id}` distinguishes a
  local `unknown`, `pending`, or `committed` observation.

These routes have no CORS layer, TLS, authentication, SDK commitment, or
product-profile semantics. They do not change the standalone API's receipt or
durability contract. See [Experimental Consensus Probe](CONSENSUS_PROBE.md).

When `EPOCH_EXPERIMENTAL_STREAM_TABLET_ENABLED=true`, opaque proposal routes are
not mounted on that group. The listener instead exposes:

- `GET /experimental/v1/tablets/stream/status` for local Raft positions and the
  last unique typed mutation index applied to the profile;
- `POST /experimental/v1/tablets/stream/records` for a typed partition-0 append
  with `idempotency_key` and `expected_term`; unknown top-level or nested
  envelope fields are rejected;
- `GET /experimental/v1/tablets/stream/records` for explicitly stale-capable
  local committed reads; and
- `GET /experimental/v1/tablets/stream/mutations/{proposal_id}` for unknown,
  pending, or committed outcome resolution.

JSON syntax, media-type, body-limit, and schema extraction failures use the
same structured `invalid_request` error envelope and are definitely not
committed. Status samples the profile before requesting the actor's consensus
snapshot and rejects an inconsistent result, so `last_profile_mutation_index`
never exceeds `consensus_applied_index` in one document.

The typed receipt separates Raft commit index from Stream offset and reports
`write_evidence: fixed_voter_majority_persisted` with
`durable_voter_acks: 2` only after a fixed three-voter majority is durably
committed and the local tablet has applied the command. This is bounded
trusted-topology evidence, not a claim against spoofed peers and not the PRD's
placement-aware `quorum_durable` profile.
All 64-bit identities, positions, and envelope timestamps are exact decimal
strings in typed JSON. The append endpoint accepts decimal strings for
`expected_term`, `time_ms`, `deliver_at_ms`, and `ttl_ms`. Proposal IDs use the
same representation in the mutation-status URL.
A bounded unresolved wait returns `202`, preserving local `unknown` versus
`pending` state while keeping outcome certainty unknown. Exact retries return
the original offset; changed input under the same key is a conflict, and every
notification/lookup is checked against that semantic input before a receipt is
returned. `not_leader`, `stale_term`, and idempotency-conflict errors have
unknown global outcome certainty. Startup replays the full committed proposal
history before the typed status route becomes ready. A live deterministic apply
failure drains both listeners and exits the process. See
[Experimental Stream Tablet](STREAM_TABLET.md).

A strict Queue tablet command/receipt/state-machine contract now exists in
`crates/epoch-tablet`, including fenced lease settlement and immutable
DLQ/redrive history, but it has no node route or consensus applier yet. It is
therefore not an additional experimental API. See
[Replicated Queue Tablet Core](QUEUE_TABLET.md).

Neither experimental mode is the final tablet service. Snapshots/compaction,
retention deletion, dynamic membership, placement, read barriers, authenticated
transport, public routing, and SDK support remain absent. The standalone engine
journal remains a separate single-node source of truth and is never used by the
experimental replicated tablet.

Initial `epoch.v1` Protobuf source defines common resource/envelope types and a
small `RegionalAdminService`; Buf generation is configured for Go. It is an
early boundary scaffold, not the complete package split or native data API in
this document. No gRPC server is running, and port 7600 is only reserved.

TLS/authentication metadata, typed `google.rpc.Status` details, public native
mutation-status lookup, streaming credit, a Rust regional administration
implementation, long-running operations, metrics on the reserved port, protocol
gateways, full Go/Java/Python generated SDK parity and compatibility negotiation
remain unimplemented. The experimental Stream tablet has only the local
mutation lookup described above; the Queue tablet has no listener. Typed Go,
Java, and Python clients cover the provisional
standalone profile HTTP routes, including explicit local Stream and Queue
durability; they do not cover the experimental tablet listener. All three use
injectable transport boundaries and run against the real standalone node;
the exact quickstarts displayed by the documentation each drive an independent
seed, forced process crash, restart, and recovery proof in CI. Browser calls are
accepted only from the exact HTTP(S) origins configured by
`EPOCH_ALLOWED_ORIGINS`; requests without an `Origin` header remain available
to native clients. The Go control HTTP registry, browser console, current JSON
payload structs, and Rust error enum are provisional scaffolding and may be
migrated before any public compatibility promise.
