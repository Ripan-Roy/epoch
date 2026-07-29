# Regional Multi-Tablet Runtime

**Status:** Experimental topology-validated fixed-three-voter alpha

**Authority:** Rust catalog and tablet consensus groups

**Hosted bridge:** Go desired-state reconciler and browser BFF

This guide describes only the implementation that exists now. It does not
upgrade the stable standalone SDK contract or claim dynamic production
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
    "request_token": "docs-create-orders-v1",
    "expected_generation": 0,
    "resource": {
      "organization": "acme",
      "project": "shop",
      "environment": "dev",
      "namespace": "core",
      "kind": "stream",
      "name": "orders",
      "spec": {
        "shard_count": 1,
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
file, proves exact replay, creates and mutates all four profiles, kills a
leader, catches it up, kills all nodes, reopens the same volumes, compares
digests, and deletes only its scoped containers/network/volumes.

## Current boundaries

- Placement is exactly three configured voters. Region, zone count, and node
  class are validated, but there is no general voter-selection or rack-aware
  solver.
- Membership changes, online rebalance, repair, split/merge, snapshots,
  compaction, and retention deletion are absent. Read barriers are leader-only
  and regional-only; follower forwarding and stable public SDK exposure remain
  absent.
- Rust regional HTTP and Go management enforce the bootstrap policy, and the
  console supplies a session-only credential. They still have no TLS/OIDC/mTLS,
  token expiry/revocation, rate limiting, replicated policy, or immutable audit
  export. Peer port 7701 and the standalone local API remain unauthenticated.
- Go management metadata is durable for one process and one bbolt file. It is
  not replicated, multi-instance linearizable, backed up automatically, or
  protected by management leader election.
- There is no public regional SDK contract. Go, Java, and Python SDKs currently
  cover the standalone HTTP profiles documented on the published docs page.
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
