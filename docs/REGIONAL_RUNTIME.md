# Regional Multi-Tablet Runtime

**Status:** Topology-validated fixed-three-voter alpha with regional Stream, Queue, Cache, and Event Bus v1

**Authority:** Rust catalog and tablet consensus groups

**Hosted bridge:** Go desired-state reconciler and browser BFF

This guide describes only the implementation that exists now. The standalone
SDK contract remains separate. Versioned regional Stream, Queue, Cache, and Event Bus v1 clients
now exist for Go, Java, and Python; they do not claim dynamic production
placement.

## Ownership and data flow

```text
TypeScript console
        |
        | session bearer + GET /v1/regional/resources
        v
Go epoch-control (durable desired state + observed status)
        |
        | workload bearer + inventory/admit/apply/observe/delete
        v
Rust catalog group 1 (three EPRS-backed voters)
        |
        | committed catalog snapshot
        v
bounded multi-group supervisor
        |
        +-- Cache tablet group
        +-- Stream tablet group
        +-- Queue tablet group
        `-- Event Bus tablet group

Go / Java / Python application
        |
        | bearer + fully qualified regional Stream/Queue/Cache/Event Bus v1 + route fences
        +----------------------------------------------> Stream tablet leader
        +----------------------------------------------> Queue tablet leader
        +----------------------------------------------> Cache tablet leader
        `----------------------------------------------> Event Bus tablet leader
```

Rust owns catalog correctness, tablet state, consensus, routing, and every data
mutation. Go owns desired metadata and the browser projection. The console
never connects directly to a Rust node.

## Start a local region

The regional Compose model publishes nodes on ports 18661–18663 and gives every
voter an independent named volume. Nodes are labeled `ap-south-1a`,
`ap-south-1b`, and `ap-south-1c` in region `ap-south`, with class
`general-purpose`. It mounts the checked-in development policy read-only at
`/etc/epoch/bootstrap-policy.json`:

```shell
make compose-regional-config
make compose-regional-up
```

Start the Go bridge against all three Rust endpoints:

```shell
EPOCH_CONTROL_REGIONAL_ENDPOINTS=http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663 \
EPOCH_CONTROL_ALLOWED_ORIGINS=http://127.0.0.1:5173 \
EPOCH_CONTROL_STATE_PATH=.epoch/control/registry.db \
EPOCH_AUTH_POLICY_PATH=spec/auth/bootstrap-policy-v1.example.json \
EPOCH_CONTROL_REGIONAL_TOKEN=epoch-dev-control-v1 \
go run ./control/cmd/epoch-control
```

The process owns that database exclusively. `GET /healthz` reports
`"registry":"bbolt_v1"` and `"registry_durable":true`. Corruption, an unknown
schema version, or another process already holding the file makes startup fail
closed.

Start the console in another terminal:

```shell
VITE_EPOCH_API_BASE_URL=http://127.0.0.1:18661 \
VITE_EPOCH_CONTROL_BASE_URL=http://127.0.0.1:8080 \
pnpm --filter @epoch/console dev
```

The first URL is used only for the standalone node overview/forms. Regional
inventory always comes from Go at the second URL. In the regional placement
panel, enter `epoch-dev-admin-v1` as the managed-control credential. The
console retains it only in that tab's `sessionStorage`; the static bundle and
GitHub Pages contain no credential.

These named values are public development fixtures whose SHA-256 fingerprints
appear in the example policy. Replace the policy and credentials for any
non-disposable environment. This baseline still lacks TLS, expiry/revocation,
OIDC, mTLS, and production secret delivery.

## Apply one managed resource

The current HTTP registry is a provisional development entry point to the same
desired registry reconciled by the RegionalAdmin gRPC service:

```shell
curl --fail-with-body \
  -X PUT http://127.0.0.1:8080/v1/resources \
  -H 'authorization: Bearer epoch-dev-admin-v1' \
  -H 'content-type: application/json' \
  --data '{
    "request_token": "docs-create-orders-3-shards-v1",
    "expected_generation": 0,
    "resource": {
      "organization": "acme",
      "project": "shop",
      "environment": "dev",
      "namespace": "core",
      "kind": "stream",
      "name": "orders",
      "spec": {
        "shard_count": 3,
        "replica_count": 3,
        "placement": {
          "allowed_regions": ["ap-south"],
          "minimum_zones": 3,
          "required_node_class": "general-purpose"
        }
      }
    }
  }'
```

Before catalog mutation, Go authenticates to every configured Rust node and
collects `/experimental/v1/regional/topology`. It requires one complete,
consistent sample for each fixed voter, validates the placement policy, and
checks every node has enough live group capacity for newly added shards. It
then applies the desired generation to the Rust catalog leader and samples the
route on every configured node. Poll the browser-safe projection:

```shell
curl --fail-with-body \
  -H 'authorization: Bearer epoch-dev-admin-v1' \
  http://127.0.0.1:8080/v1/regional/resources
```

A ready tablet has three distinct `voter_node_ids` and a `leader_node_id` that
belongs to that observed voter set. Desired replicas never count as observed
voters. Resource generations, observed generations, tablet/group IDs, epochs,
voter IDs, and leader IDs are decimal JSON strings so JavaScript cannot round
them. `placement` reports requested constraints, achieved zones, and per-node
capacity separately from the tablet routes.

Inspect one node directly:

```shell
curl --fail-with-body \
  -H 'authorization: Bearer epoch-dev-admin-v1' \
  http://127.0.0.1:18661/experimental/v1/regional/topology
```

`used_consensus_groups` includes catalog group 1 plus one group for each
materialized tablet. A capacity rejection uses the stable
`consensus_group_capacity` reason and names the limiting node; Rust catalog
`Apply` is not called. This counter does not claim CPU, memory, disk, network,
or workload-specific sizing.

The response also contains `maintenance`. `enabled`, the configured
`interval_ms`, cumulative `passes`, `tablets_examined`, `leader_passes`,
`due_operations`, `proposals_submitted`, `pending_operations`, `errors`, and
the last pass/error make automatic timer ownership inspectable without
exposing it as product state. These counters are node-local and reset on
restart. Configure the scan with `EPOCH_REGIONAL_MAINTENANCE_INTERVAL_MS`; the
default is 100 ms and the accepted range is 1–60,000 ms.

The same response contains `checkpoints`. Each node evaluates catalog and all
local profile groups, regardless of Raft role, because checkpointing changes
only that voter's recovery layout. `interval_ms`, `min_applied_entries`,
cumulative passes/eligible/created/reclaimed/error counters, and `groups` make
the behavior inspectable. Each group reports decimal-string `group_id`,
`group_epoch`, `applied_index`, `checkpoint_index`, and
`retained_log_first_index`. Counters reset on restart; group boundaries come
from durable consensus status. Configure
`EPOCH_REGIONAL_CHECKPOINT_INTERVAL_MS` (default 1,000; 1–600,000) and
`EPOCH_REGIONAL_CHECKPOINT_MIN_APPLIED_ENTRIES` (default 1,024; nonzero).

Topology also contains `epoch_target_delivery`. The worker is always enabled;
only the current source Bus leader selects a Queue/Stream record. Counters
cover passes, examined tablets/leaders/subscriptions, acquired leases, Queue
enqueues, Stream appends, retries, dead letters, errors, and the last pass/error.
They reset on process restart and contain no event payload. Configure
`EPOCH_REGIONAL_EPOCH_TARGET_DELIVERY_INTERVAL_MS` (default 100; 1–60,000).
See [ADR-0031](adr/0031-leader-owned-epoch-target-delivery.md).

When a strict signing-key file is configured, topology also contains
`webhook_delivery`. It reports `enabled`, `interval_ms`, cumulative passes,
examined tablets/leaders/subscriptions, acquired leases, delivered/retried/
dead-lettered outcomes, errors, and the last pass/error. Only the current Bus
tablet leader submits work. Configure
`EPOCH_REGIONAL_WEBHOOK_SIGNING_KEYS_PATH` and optionally
`EPOCH_REGIONAL_WEBHOOK_DELIVERY_INTERVAL_MS` (default 100; 1–60,000).
`EPOCH_REGIONAL_WEBHOOK_ALLOW_HTTP_LOOPBACK=true` is a development-only switch;
normal targets require public HTTPS. See
[ADR-0030](adr/0030-leader-owned-signed-webhook-delivery.md).

## Route and fence a data operation

Discover a shard independently on each node:

```shell
curl --fail-with-body \
  -H 'authorization: Bearer epoch-dev-admin-v1' \
  http://127.0.0.1:18661/experimental/v1/regional/resources/acme/shop/dev/core/stream/orders/shards/0
```

The response identifies the local role, observed leader, current term,
resource generation, tablet ID, consensus-group ID, and tablet epoch. Send a
typed operation only to the node whose response has `accepts_writes: true`.
Every data request must copy the exact fences:

```text
x-epoch-resource-generation: <resource_generation>
x-epoch-tablet-epoch: <tablet_epoch>
```

The suffix beneath `/data/{operation}` is delegated to the materialized
profile-specific tablet router. A stale generation/epoch, wrong profile,
missing resource/shard, unavailable group, or nonleader is rejected before
typed mutation handling. The router does not silently proxy a write.

For a Queue resource, `POST .../data/mutations` may submit an `acquire` with
`max_messages` request credit and `max_in_flight` consumer capacity. The
committed receipt reports exact before/after/remaining capacity.
`GET .../data/consumers/{consumer}/flow` observes the applied consumer epoch
and live-lease count through the read-consistency rules below. This is the
experimental bounded HTTP slice; it is not the future native bidirectional
receive stream.

For a Stream resource, `POST .../data/records/batches` delegates to the
replicated batch handler after the same authorization, generation, epoch, and
leader checks. The request carries one canonical record array as `none`, gzip,
LZ4-frame, Snappy-framed, or Zstd-frame base64 plus exact count and byte
metadata. The handler enforces 1–1,000 records, 360 KiB compressed, 4 MiB
expanded, and an 8 MiB Zstd window before proposal; its receipt maps each unique
client sequence to an exact decimal offset. The whole batch is one atomic
single-shard transition and its regional receipt reports the outer logical
shard. Regional Go, Java, and Python clients expose this exact operation with
built-in canonical none/gzip framing and typed caller frames for every
supported codec. Stable streaming Produce, cross-shard batching, automatic
client batching/negotiation, and partial non-atomic results remain open.

## Use the regional Stream v1 SDK

Applications should use the fully qualified versioned shard route rather than
constructing the generic experimental `kind/data/operation` adapter:

```text
/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/streams/orders/shards/{shard}
```

Go, Java, and Python expose `RegionalScope` and `RegionalStreamClient`. Keyed
append discovers `fnv1a64_utf8_mod_n_v1`, hashes the UTF-8 event key or ID,
selects a logical shard, and requires the target to retain the initially
observed resource generation before sending a write. Every operation then
queries the configured Rust endpoints, selects only `accepts_writes: true`,
copies the resource generation and tablet epoch, and supplies the observed term
for mutations. Append, checkpoint, and session calls require a caller-owned
idempotency key; a bounded route retry reuses it unchanged. Fetch, checkpoint
replay, lag, and session observation explicitly select `linearizable` and never
downgrade to stale reads.

The current SDK methods cover keyed and explicit-shard single-record append,
bounded offset fetch, per-shard checkpoint commit/reset, lag, shard-zero
join/heartbeat/leave/expiry-maintenance and assignment observation, retention,
fetch from the durable checkpoint, per-shard session claim, exact-claim fetch,
and a resource-level bounded claim–revalidate helper. The helper pins resource
generation and returns no assignment after a concurrent rebalance; claims
preserve offsets but are not an atomic cross-shard transaction. The complete
executable Go, Java, and Python examples plus setup
commands are in [Regional Stream SDK](REGIONAL_STREAM_SDK.md) and embedded on
the published documentation page.

## Use the regional Queue v1 SDK

The Queue client uses the fully qualified versioned shard route:

```text
/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/queues/jobs/shards/0
```

Go, Java, and Python expose `RegionalQueueClient` over the same shared discovery,
authorization, fencing, linearizable-read, and one-rediscovery contract as the
Stream client. Mutations require caller-owned idempotency keys and cover enqueue,
acquire, lease extension, acknowledge, release, nack, reject, redrive, and
bounded maintenance. Reads cover mutation receipts, counts, dead-letter and
redrive histories, consumer flow, and status. Acquire additionally carries the
consumer epoch and explicit credit, while settlement operations carry the
opaque lease token returned by acquire.

The exact compiled examples, lifecycle guidance, and failure semantics are in
[Regional Queue SDK](REGIONAL_QUEUE_SDK.md) and embedded on the published
documentation page. This is a single-partition alpha surface; it is not a claim
of streaming receive, automatic session coordination, package publication, or
production placement.

## Use the regional Cache v1 SDK

The Cache client uses the fully qualified versioned shard route:

```text
/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/caches/sessions/shards/0
```

Go, Java, and Python expose `RegionalCacheClient` over the same authenticated
discovery, fencing, linearizable-read, and one-rediscovery core. The typed value
surface covers string, blob, signed counter, hash, list, unique set, and
finite-score sorted set. Mutations cover set, committed access, delete, CAS,
increment, atomic transaction/batch, lock acquire/renew/release, and expiry
maintenance. Managed configuration carries entry capacity, default TTL, and
the deterministic no-eviction/all-key/volatile policy into every voter. The regional
leader schedules due expiry automatically; explicit SDK maintenance remains
available.
Reads cover mutation lookup, key observation, and tablet status.

The exact compiled examples, value/transaction/lock guidance, and retry
semantics are in [Regional Cache SDK](REGIONAL_CACHE_SDK.md) and embedded on the
published docs page. The real campaign kills the Cache leader before running
the Python client, then proves committed LRU access, atomic batching, capacity
eviction, catches up the old voter, and reopens every EPRS volume.

## Use the regional Event Bus v1 SDK

The Event Bus client uses the fully qualified versioned shard route:

```text
/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/buses/events/shards/0
```

Go, Java, and Python expose `RegionalBusClient` over the same authenticated
discovery, fencing, linearizable-read, and one-rediscovery core. Mutations cover
subscription upsert/removal, publish, delivery acquire/ack/fail/reject/maintenance;
reads cover mutation lookup, archive replay, delivery query, and status.
Subscriptions carry bounded timeout, concurrency, attempts, backoff, jitter,
and age policy. Settlement requires the opaque lease token returned by acquire.

Queue and Stream subscription targets are executed automatically. The source
leader commits a lease that pins the exact target generation/shard/tablet/epoch,
forwards a stable-idempotency enqueue or append proposal to that group's known
leader, awaits its committed receipt, and then acknowledges the Bus record.
Queue binds shard `0`; Stream uses the shared FNV-1a key router. This survives
different group leaders and source-settlement uncertainty without duplicating
the target record, but the two commits are not one cross-tablet transaction.

Signed HTTP/webhook targets name an external key ID. When every node has that
key and the worker is enabled, the current leader commits an exact lease,
awaits it, sends a signed CloudEvents 1.0 binary-mode HTTPS request, and commits
the observed result. The exact compiled examples, receiver verification
helpers, retry semantics, and external-side-effect non-claims are in
[Regional Event Bus SDK](REGIONAL_EVENT_BUS_SDK.md) and embedded on the
published docs page. The real process campaign receives a 503 then 204, checks
distinct attempt signatures and converged acknowledgement history, and reopens
every voter from the same storage. That campaign also delivers one event into
Queue and keyed multi-shard Stream targets, proves both Bus records
acknowledged, and reopens every voter without a duplicate destination record.
The broader container campaign also kills
the Event Bus leader before running the Python pull lifecycle. Neither proof
claims exactly-once business effects.

Regional reads are linearizable by default and therefore must target the
current leader. Epoch submits a safe Raft `ReadIndex`, waits for majority
confirmation, applies locally through the returned index, and only then reads
the typed profile:

```shell
curl --fail-with-body \
  -H 'authorization: Bearer epoch-dev-admin-v1' \
  -H 'x-epoch-resource-generation: 1' \
  -H 'x-epoch-tablet-epoch: 1' \
  'http://127.0.0.1:18661/experimental/v1/regional/resources/acme/shop/dev/core/stream/orders/shards/0/data/records?offset=0&limit=100'
```

A successful response reports `read_consistency: "linearizable"`,
`linearizable_read_barrier: true`, and exact decimal barrier term/read/applied
indexes. The same evidence is available in `x-epoch-read-consistency` and
`x-epoch-read-index` response headers. `EPOCH_REGIONAL_READ_BARRIER_TIMEOUT_MS`
sets the 1–60,000 ms wait and defaults to 2,000 ms. A minority cannot satisfy it
and receives retryable `503 read_barrier_timeout`.

Callers that deliberately accept a local stale observation must opt in:

```text
x-epoch-read-consistency: local_stale
```

That response remains `local_profile_applied_stale_capable` with
`linearizable_read_barrier: false`. There is no automatic downgrade. Event Bus
archive replay and delivery-query POSTs are classified as reads and follow the
same consistency and `data.read` authorization contract.

## Failure and recovery behavior

- One voter loss leaves a majority able to elect and commit. Go reports only
  the two routes it can currently validate and marks placement degraded. The
  generation-fenced admission record can still explain the intended zones.
- When every authority endpoint is unavailable, Go retains the last observed
  generation for reconciliation but clears voter/leader placement so the
  browser cannot present stale topology as current.
- Reopening the same Rust volumes replays the catalog first, rematerializes data
  groups, and rebuilds each typed state machine before readiness.
- Exact Go apply/delete retries and exact Rust catalog mutation retries replay
  the original result. Go desired state, observed status, request outcomes, and
  tombstone generations survive a control-process `SIGKILL`. Rebinding a token
  to different input conflicts.
- Delete commits a Rust catalog tombstone before Go removes desired metadata.
  Recreating the name receives new tablet/group identities and a later
  generation.
- Missing/invalid credentials fail with `unauthenticated`; an authenticated
  principal missing the action or tenant scope fails with `permission_denied`.
  Collection reads filter cross-tenant records, and both Go and Rust emit a
  bounded credential-free authorization decision with a request ID.

Run the complete disposable proof:

```shell
make test-regional-runtime
```

It builds the real Go control binary, verifies the three node-local topology
and live capacity responses, creates a three-zone resource through Go, proves
an over-capacity request never reaches the catalog, verifies the
BFF/CORS/placement contract, kills and reopens Go against the same metadata
file, proves exact replay, creates and mutates all four profiles, routes keyed
Python appends/checkpoints across a three-shard Stream, configures retention,
then waits for leader-proposed Stream retention/session expiry, Queue retry
promotion, Cache TTL reclamation, and Event Bus delivery-lease timeout without
calling explicit maintenance. It checks topology submission/error counters,
waits for automatic checkpoint creation and retained-prefix compaction on all
24 voter/group copies, kills and catches up each profile leader, kills all
nodes, reopens the same volumes, verifies durable per-group checkpoint
boundaries, compares per-shard state and digests, and deletes only its scoped
containers/network/volumes.

## Current boundaries

- Placement is exactly three configured voters. Region, zone count, and node
  class are validated, but there is no general voter-selection or rack-aware
  solver.
- Membership changes, online rebalance, repair, split/merge, user-exportable
  backups/PITR are absent. Automatic local native voter checkpoints, physical
  EPRS reclamation, replicated Stream
  time/size/combined logical retention, and leader-owned regional maintenance
  are implemented. Retention still lacks keyed compaction, object-tier
  deletion, and legal-hold governance. Multi-shard Stream routing and a replicated shard-zero
  session coordinator are implemented for a fixed resource generation. Safe
  online expansion/remapping, virtual shards, hot-key mitigation, cooperative
  revoke, and atomic checkpoint handoff are not. Read
  barriers are leader-only and regional-only;
  follower forwarding remains absent.
- Rust regional HTTP and Go management enforce the bootstrap policy, and the
  console supplies a session-only credential. They still have no TLS/OIDC/mTLS,
  token expiry/revocation, rate limiting, replicated policy, or immutable audit
  export. Peer port 7701 and the standalone local API remain unauthenticated.
- Go management metadata is durable for one process and one bbolt file. It is
  not replicated, multi-instance linearizable, backed up automatically, or
  protected by management leader election.
- Go, Java, and Python now share the regional Stream, Queue, Cache, and Event
  Bus v1 route/retry/fence contract, including Stream atomic caller-framed
  batches, retention
  configure/maintain/observe, generation-pinned key routing, and coordinated
  session membership/assignment. They remain repository-local alpha source;
  Event Bus targets and exact-body signature verification are aligned across
  all three languages. Package publication, generated models, transactional assignment/offset
  handoff, safe remapping, and production transport remain open.
- The BFF reports policy-protected configured-endpoint region/zone/class and
  group-capacity evidence. Plain HTTP still lacks Rust server identity.
  Rack separation, dynamic membership, and online rebalancing remain
  explicitly unverified.

Stop the development topology with:

```shell
make compose-regional-down
```

That retains named volumes. Add `--volumes` to the underlying Compose command
only when intentionally discarding all regional test data.
