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

`ResourceStatus.tablets` reports one descriptor per routable shard:

- tablet ID and consensus-group ID as separate nonzero identities;
- shard index, immutable workload profile, tablet epoch, and resource
  generation;
- desired replica count separately from observed voter node IDs;
- observed leader node ID and tablet lifecycle phase.

An empty voter list or leader ID does not mean the desired placement has been
achieved. Callers must use the phase and conditions, and data requests must
carry the resource generation and tablet epoch returned by routing. The
experimental regional runtime now allocates, commits, materializes, discovers,
and generation/epoch-fences these identities across several fixed-voter groups.
Its `/experimental/v1/regional/*` routes remain an alpha verification surface,
not the versioned application contract. The separate fully qualified regional
Stream v1 route is the first authenticated native adapter over those same
identities. See [ADR-0009](adr/0009-regional-tablet-catalog.md) and
[ADR-0017](adr/0017-regional-stream-v1-and-sdk-routing.md).

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

The current generated `epoch.v1.RegionalAdminService` is a bounded Go-hosted
bridge with `ApplyResource`, `GetResource`, `ListResources`, and
`DeleteResource`. Apply validates a fully qualified data-bearing resource,
profile/kind agreement, nonzero shard count, and the currently fixed replica
count of three. `ResourceSpec.placement` can require allowed regions, a minimum
zone count, and a node class. Before Rust catalog mutation, Go authenticates to
every configured node, verifies an identical fixed-voter inventory, and checks
incremental group capacity. Unsatisfied constraints fail before catalog apply.
It stores desired state, immediately reconciles through the Rust authority, and
returns pending desired state when the region is unavailable. Definitive
conflicts fail; exact apply and delete retries return their original result
without applying the Rust mutation twice. Delete commits the Rust tombstone
before removing Go desired metadata.

This subset has bounded list pages but no watch, opaque continuation, plan,
backup, repair, purge, or long-running operation surface. Its single-owner Go
registry transactionally persists desired resources, observed status,
generation tombstones, and original request-token outcomes before
acknowledgement. Startup rejects corruption, an unknown schema, or concurrent
ownership. This is process-crash durability, not multi-instance linearizability
or a replicated hosted database.

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

The browser-facing alpha inventory is:

```text
GET /v1/regional/resources
```

It returns only fully qualified managed resources. Each row contains canonical
name, kind/profile, desired and observed generation, reconciliation phase and
message, desired shard count, and the achieved tablet placement. Resource
generation, observed generation, tablet/group/epoch/resource-generation IDs,
voter node IDs, and optional leader node ID are JSON decimal strings so a
browser cannot lose 64-bit precision. Desired replicas and observed voters are
separate fields; an authority outage returns `pending` with no current tablet
placement rather than retaining a stale leader claim.

The optional `placement` object contains the requested region/zone/class
constraints, achieved zone count, and policy-protected configured-endpoint topology plus
maximum/used/available consensus-group counts. Node and voter IDs remain
decimal strings. These fields prove the fixed-voter admission decision; they do
not claim rack separation, dynamic membership, or online rebalancing.

The Rust node-local alpha inventory used by Go is:

```text
GET /experimental/v1/regional/topology
```

It requires `topology.read`, reports the fixed peer-derived voter set, and
computes live used groups as catalog group 1 plus materialized tablets. Go
requires a complete consistent response from every configured endpoint before
any mutation. Capacity failures use `consensus_group_capacity` and retain the
limiting node, required groups, and available groups in the internal admission
error; the current public status exposes the stable reason in its message.

The TypeScript console calls this Go endpoint only. Browser CORS is granted to
exact HTTP(S) origins configured by `EPOCH_CONTROL_ALLOWED_ORIGINS`; wildcards,
paths, query strings, opaque origins, and credentials are rejected. Requests
without `Origin` remain available to non-browser clients, but every `/v1`
request still requires bootstrap bearer authentication.

### 6.8 Bootstrap authentication and authorization

The current managed and regional alpha uses the strict policy defined by
`spec/auth/bootstrap-policy-v1.schema.json`. HTTP callers send
`Authorization: Bearer <token>`; gRPC callers send the same value in
`authorization` metadata. A caller may also send one printable
`X-Request-ID`/`x-request-id` of at most 128 bytes. The server generates a safe
identifier otherwise and returns it in the response.

Health endpoints and CORS `OPTIONS` are public. Missing, malformed, repeated, or
invalid credentials fail before a managed or regional handler runs. HTTP
returns `401 {"code":"unauthenticated",...}` and gRPC returns
`UNAUTHENTICATED`. An authenticated principal without the action or tenant
scope receives HTTP `403 {"code":"permission_denied",...}` or gRPC
`PERMISSION_DENIED`.

The implemented action mapping is:

| Boundary | Operation | Required action |
|---|---|---|
| Go HTTP/gRPC | Apply resource | `resource.apply` |
| Go HTTP/gRPC | Delete resource | `resource.delete` |
| Go HTTP/gRPC | Get/List/inventory | `resource.read` |
| Rust regional | Catalog PUT | `catalog.apply` |
| Rust regional | Catalog DELETE | `catalog.delete` |
| Rust regional | Catalog GET | `catalog.read` |
| Rust regional | Shard route GET | `route.read` |
| Rust regional | Typed data GET | `data.read` |
| Rust regional | Event Bus archive replay/delivery query POST | `data.read` |
| Rust regional | Typed data mutation | `data.write` |

Single-resource operations authorize the fully qualified
organization/project/environment/namespace before lookup or mutation.
Collections require the read action and filter each returned record by scope;
an unauthorized tenant record is not disclosed. `epoch-control` uses the
separate workload credential from `EPOCH_CONTROL_REGIONAL_TOKEN` for every Rust
authority request. Authorization decisions never place that credential in
errors or audit fields.

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

Standalone Event Bus creation accepts `durability`, `archive`, and optional
`max_subscriptions`/`max_archive_events`; the shared configuration codec also
defaults `delivery_outbox: false` and `max_outbox_deliveries: 100000`.
Standalone engine creation rejects `delivery_outbox: true` because its public
routes do not mount the replicated lease/settlement protocol. The experimental
Bus tablet forces it on internally. Capacity values must be non-zero and cannot
exceed 100,000 subscriptions or 10,000,000 archived/outbox records. Replay
responses are capped at 10,000 records. Route-plan and publish positions are
checked counters; capacity or counter exhaustion rejects atomically.

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

When `EPOCH_REGIONAL_RUNTIME_ENABLED=true`, the configured consensus identity
and peer listener start the catalog and multi-group runtime instead of a
single-profile probe. `EPOCH_REGIONAL_MAX_GROUPS` bounds the catalog plus data
groups; catalog group 1 is reserved. The provisional HTTP routes are:

```text
GET    /experimental/v1/regional/catalog
GET    /experimental/v1/regional/catalog/resources/{org}/{project}/{environment}/{namespace}/{kind}/{name}
PUT    /experimental/v1/regional/catalog/resources/{org}/{project}/{environment}/{namespace}/{kind}/{name}
DELETE /experimental/v1/regional/catalog/resources/{org}/{project}/{environment}/{namespace}/{kind}/{name}
GET    /experimental/v1/regional/resources/{org}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}
*      /experimental/v1/regional/resources/{org}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}/data/{operation}
```

The versioned regional Stream application route is:

```text
GET /v1/organizations/{org}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}
*   /v1/organizations/{org}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}/{operation}
```

`GET` on the shard base performs discovery. The current Go, Java, and Python
SDK contract maps `records` and
`groups/{group}/{offsets|lag|records}` to the same replicated partition-0
Stream tablet. The stable adapter removes the generic `kind` and `data`
segments but does not introduce another log or state store.

The versioned regional Queue application route follows the same discovery and
fencing boundary:

```text
GET  /v1/organizations/{org}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}
POST /v1/organizations/{org}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/mutations
GET  /v1/organizations/{org}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/{counts|dead-letters|redrives|status}
GET  /v1/organizations/{org}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/mutations/{proposal_id}
GET  /v1/organizations/{org}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/consumers/{consumer}/flow
```

The strict mutation union covers enqueue, credit-aware acquire, acknowledge,
lease extension, release, Nack, Reject, redrive, and maintenance for partition
`0`. Counts, mutation lookup, histories, consumer flow, and status are
linearizable SDK reads. This adapter delegates to the same replicated Queue
tablet and owns no queue state or request translation.

Catalog mutations require a bounded `request_token`, expected generation,
shard count, and the currently fixed replica count of three. The exact token
and semantic request replay their committed result; token rebinding conflicts.
Delete commits a monotonic tombstone, so recreation never reuses prior
tablet/group identities. Discovery returns the local node/role, observed
leader, term, resource generation, tablet/group IDs, and tablet epoch. Every
64-bit JSON value is a decimal string.

Data dispatch is local and never silently forwards. It requires exact
`x-epoch-resource-generation` and `x-epoch-tablet-epoch` headers, validates the
materialized profile, and rejects stale fences before invoking the typed tablet
router. Mutations require the current leader. Reads default to a safe
quorum-confirmed Raft `ReadIndex` on the current leader and complete only after
the local typed profile applies through that index. Event Bus archive replay
and delivery-query POSTs are semantic reads and require `data.read`.

A caller may explicitly send
`x-epoch-read-consistency: local_stale` to bypass the barrier and read the local
profile. Epoch never silently downgrades the default. Successful linearizable
responses return:

```text
x-epoch-read-consistency: linearizable
x-epoch-read-index: <decimal Raft index>
```

The JSON body also reports `read_consistency`, `linearizable_read_barrier`,
`read_barrier_term`, `read_barrier_index`, and
`read_barrier_applied_index`. A bounded wait defaults to 2,000 ms and is set by
`EPOCH_REGIONAL_READ_BARRIER_TIMEOUT_MS` from 1 through 60,000 ms; quorum timeout is retryable HTTP 503
`read_barrier_timeout`. A follower or term race returns the existing retryable
leader-routing conflict. Direct profile routes remain explicitly
`local_profile_applied_stale_capable`.

Regional Stream and Queue SDKs configure one or more Rust endpoints and discover a complete
`accepts_writes: true` route before every operation. They copy the exact
generation/tablet epoch, use the discovered term for mutations, and request
`linearizable` for profile reads. Stream append/checkpoint and every Queue
mutation require a caller-owned idempotency key. Retryable transport, leader, fence,
route, or barrier outcomes permit at most one rediscovery, always with the same
key; definitive validation, authorization, idempotency, or committed business
outcomes return immediately.

See [ADR-0017](adr/0017-regional-stream-v1-and-sdk-routing.md),
[ADR-0018](adr/0018-regional-queue-v1-and-sdk-routing.md),
[Regional Stream SDK](REGIONAL_STREAM_SDK.md), and
[Regional Queue SDK](REGIONAL_QUEUE_SDK.md).

The fence and consistency headers are included in the node's exact-origin CORS
allowlist. Regional HTTP is bootstrap-authenticated and action-authorized, but
still lacks TLS/OIDC/mTLS server and peer identity; it must not be exposed as a
production management or data surface.

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
- `POST /experimental/v1/tablets/stream/records/batches` for one atomic
  partition-0 batch carrying canonical record JSON in a declared `none`, gzip,
  LZ4-frame, Snappy-framed, or Zstd-frame base64 payload;
- `GET /experimental/v1/tablets/stream/records` for explicitly stale-capable
  local committed reads;
- `PUT /experimental/v1/tablets/stream/groups/{group}/offsets` for a
  generation-fenced checkpoint commit or explicit reset;
- `GET /experimental/v1/tablets/stream/groups/{group}/lag` for owner,
  generation, retained range, next offset, end offset, and lag;
- `GET /experimental/v1/tablets/stream/groups/{group}/records` for bounded
  replay beginning at the durable next offset; and
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
A batch request supplies `compression`, `record_count`,
`uncompressed_bytes`, `compressed_bytes`, and `payload_base64`. The expanded
document is a canonical JSON array of strict
`{"client_sequence":u32,"envelope":EventEnvelope}` records with unique
sequences. The hard limits are 1–1,000 records, 360 KiB compressed, and 4 MiB
expanded; Zstd's window is capped at 8 MiB. Canonical base64/JSON, exact
declared metadata, every envelope, and all bounds are validated before
proposal and on voter decode. A successful receipt adds `batch` evidence with
the codec/sizes/count and one decimal offset/disposition per client sequence.
The entire cloned transition becomes visible together. This is the
experimental atomic tablet contract, not yet the stable non-atomic
bidirectional `Produce` API or an SDK promise.
A consumer-group mutation supplies `member_id`, `group_generation`, partition
zero, `next_offset`, and `commit` or `reset`. The first owner uses generation
one. The active member may repeat its generation, exactly the next generation
may replace the owner, and stale, skipped, or same-generation/different-member
requests are typed committed rejections. Commit is monotonic; reset alone may
rewind within the retained range. Group/member identifiers are at most 256
bytes and a tablet retains at most 10,000 groups. Receipt and read positions
are decimal strings. Command v3 applies and recovers this state from the same
EPRS history as v1/v2 records without changing their golden bytes or digest
transitions. The generic regional router maps these operations under
`data/groups/{group}/...`; regional reads use the normal safe ReadIndex default.
This is durable checkpoint/fencing evidence, not coordinated join, heartbeat,
assignment, revoke, rebalance, transactional offsets, or a stable SDK promise.
A bounded unresolved wait returns `202`, preserving local `unknown` versus
`pending` state while keeping outcome certainty unknown. Exact retries return
the original offset; changed input under the same key is a conflict, and every
notification/lookup is checked against that semantic input before a receipt is
returned. `not_leader`, `stale_term`, and idempotency-conflict errors have
unknown global outcome certainty. Startup replays the full committed proposal
history before the typed status route becomes ready. A live deterministic apply
failure drains both listeners and exits the process. See
[Experimental Stream Tablet](STREAM_TABLET.md) and
[ADR-0015](adr/0015-stream-batch-compression.md), plus consumer checkpoints in
[ADR-0016](adr/0016-stream-consumer-group-checkpoints.md).

When `EPOCH_EXPERIMENTAL_QUEUE_TABLET_ENABLED=true`, the same internal listener
instead mounts a Queue profile and does not mount opaque or Stream routes:

- `POST /experimental/v1/tablets/queue/mutations` submits one strict
  `enqueue`, `acquire`, `acknowledge`, `extend_lease`, `release`, `nack`,
  `reject`, `redrive`, or `maintain` operation;
- `GET /experimental/v1/tablets/queue/mutations/{proposal_id}` resolves local
  `unknown`, `pending`, or `committed` state;
- `GET /experimental/v1/tablets/queue/status` reports consensus/profile
  positions, server-applied time, counts, and the complete state digest; and
- `GET /experimental/v1/tablets/queue/counts`, `/dead-letters`, and `/redrives`
  expose explicitly local, stale-capable reads that never advance time; and
- `GET /experimental/v1/tablets/queue/consumers/{consumer}/flow` reports the
  applied consumer epoch and current live-lease count.

Every mutation requires a scoped idempotency key and expected term. The leader
assigns `applied_at_ms = max(wall clock, last profile-applied time)`; clients
cannot supply it. In committed log order, every voter then derives the effective
time as `max(command.applied_at_ms, prior effective time)`. This also covers a
higher-time pending entry retained across failover before a lower-clock leader's
command. HTTP accepts 64-bit inputs as numbers or decimal strings and
serializes all 64-bit output as decimal strings. Exact semantic retries ignore
only expected term and the original server time, return the stored result with
`replayed`, and cannot silently rebind changed input. Committed business
rejections remain committed outcomes. Lease tokens bind tablet/epoch,
partition, committed leader term, consumer epoch/identity, message, generation,
and deadline. An `acquire` that supplies `max_in_flight` treats `max_messages`
as request credit and grants at most the unused per-consumer window. Its receipt
returns before/after/remaining capacity evidence. Legacy acquires remain
canonical command v1; only the additive flow-controlled operation uses v2.
Immutable DLQ/redrive history survives EPRS replay. The regional
`.../data/consumers/{consumer}/flow` wrapper defaults to a leader ReadIndex like
every other regional GET. See
[Experimental Replicated Queue Tablet](QUEUE_TABLET.md) and
[ADR-0014](adr/0014-queue-consumer-credit.md).

The mutually exclusive Cache mode mounts a canonical single-shard Cache tablet
on that same internal listener:

- `POST /experimental/v1/tablets/cache/mutations` submits one strict
  Set/Delete/CAS/Increment/Transaction/lock/Maintain operation with an
  idempotency key and expected current term;
- `GET /experimental/v1/tablets/cache/mutations/{proposal_id}` resolves local
  unknown, pending, or committed state without applying a missed command;
- `GET /experimental/v1/tablets/cache/observations?key=...` returns a pure,
  explicitly local and stale-capable observation; and
- `GET /experimental/v1/tablets/cache/status` reports consensus/profile
  positions, retained entries, active locks, revisions, and recovery/state
  digests.

The API accepts 64-bit counter and metadata inputs as JSON numbers or decimal
strings and serializes every signed or unsigned 64-bit output as a decimal
string. It rejects unknown fields and duplicate collection members/keys before
proposal, records deterministic business rejections as committed outcomes,
replays exact semantic retries, and assigns committed-order effective time on
the server. This direct internal route has no linearizable read barrier, public
gRPC route, or SDK commitment. The regional wrapper described above supplies
the leader barrier. See
[Experimental Replicated Cache Tablet](CACHE_TABLET.md).

The mutually exclusive Event Bus mode mounts a canonical single-partition
ingress/outbox tablet on the internal listener:

- `POST /experimental/v1/tablets/bus/mutations` submits a strict
  `upsert_subscription`, `remove_subscription`, `publish`,
  `acquire_deliveries`, `acknowledge_delivery`, `fail_delivery`, or
  `maintain_deliveries` operation;
- `GET /experimental/v1/tablets/bus/mutations/{proposal_id}` resolves local
  unknown, pending, or committed state;
- `GET /experimental/v1/tablets/bus/status` reports consensus/profile
  positions, route/archive counters, complete recovery/state digests, and
  explicit target-delivery non-claims; and
- `POST /experimental/v1/tablets/bus/archive/replay` performs bounded inclusive
  time-range and optional filtered replay against local applied archive state;
  and
- `POST /experimental/v1/tablets/bus/deliveries/query` returns a bounded,
  explicitly local and stale-capable view of delivery state and attempt
  history.

The same strict idempotency, server-owned non-regressing time, browser-safe
64-bit JSON, majority-before-success, recovery-before-readiness, and fail-stop
rules apply. A publish receipt includes the route-plan version, ingress
position, delivery count, and SHA-256 digest of the transformed ordered
delivery plan. Every match also creates a stable per-subscription record with
captured timeout/max-in-flight/retry policy. Acquires are fenced by leader term
and dispatcher epoch; ack, failure, retry, timeout maintenance, and dead-letter
state are replicated and recovered. Status therefore reports
`target_dispatch: external_executor_not_implemented` and
`durable_target_outbox: true`. There is no built-in target transport, public
route, CLI, or SDK commitment yet, and a dispatcher acknowledgement is not
proof of an arbitrary external business side effect. See
[Experimental Replicated Event Bus Tablet](BUS_TABLET.md).

Neither the earlier single-profile modes nor the regional multi-group mode is
the final tablet service. Snapshots/compaction, retention deletion, dynamic
membership, dynamic constraint-aware placement, follower read routing,
TLS/mTLS transport, and production identity remain absent. The regional Stream
v1 route and Go/Java/Python clients are the first versioned application slice;
stable Cache/Queue/Event-Bus routing, native data gRPC, coordinated streaming,
and generated response types remain absent. The standalone engine journal
remains a separate single-node source of truth and is never used by a
replicated tablet.

Initial `epoch.v1` Protobuf source defines common resource/envelope types and a
small `RegionalAdminService`; Buf generation is configured for Go. It is an
early boundary scaffold, not the complete package split or native data API in
this document. `epoch-control` serves the current four-method RegionalAdmin
subset on gRPC port 8081 and the health/registry/browser BFF on HTTP port 8080.
Rust port 7600 remains reserved for the future native data gRPC service.

TLS/mTLS, OIDC authentication metadata, typed `google.rpc.Status` details,
public native mutation-status lookup, native bidirectional streaming and
connection-scoped credit, a stable Rust gRPC regional administration
implementation, long-running operations, metrics on the reserved port,
protocol gateways, full Go/Java/Python generated SDK parity, and compatibility
negotiation remain unimplemented. The experimental Stream,
Queue, Cache, and Event Bus tablets expose only the mutation/read surfaces
described above. Typed Go, Java, and Python clients cover the provisional
standalone profile HTTP routes, including explicit local Stream and Queue
durability; they do not cover the regional tablet routes. All three use
injectable transport boundaries and run against the real standalone node; the
exact quickstarts displayed by the documentation each drive an independent
seed, forced process crash, restart, and recovery proof in CI.

Node browser calls use exact origins from `EPOCH_ALLOWED_ORIGINS`; Go BFF calls
use `EPOCH_CONTROL_ALLOWED_ORIGINS`. Requests without an `Origin` header remain
available to native clients. The Go control registry uses a versioned,
single-owner bbolt database selected by `EPOCH_CONTROL_STATE_PATH`; its health
response names `bbolt_v1` and reports durable registry state. The console has no
compile-time credential: its operator enters a bootstrap token that is kept
only in browser session storage. The current bootstrap policy, HTTP payloads,
storage schema, and Rust error enum remain provisional scaffolding that may
change before any public compatibility promise.

The Cache tablet rebuilds by replaying the retained EPRS committed history
before readiness. It has no profile snapshot/compaction path, and its
exact-replay receipt map is currently unbounded with no advertised retry window.
