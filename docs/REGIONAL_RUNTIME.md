# Regional Multi-Tablet Runtime

**Status:** Experimental fixed-three-voter alpha  
**Authority:** Rust catalog and tablet consensus groups  
**Hosted bridge:** Go desired-state reconciler and browser BFF

This guide describes only the implementation that exists now. It does not
upgrade the stable standalone SDK contract or claim zone-aware production
placement.

## Ownership and data flow

```text
TypeScript console
        |
        | session bearer + GET /v1/regional/resources
        v
Go epoch-control (durable desired state + observed status)
        |
        | workload bearer + apply/observe/delete against configured Rust nodes
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
voter an independent named volume. It mounts the checked-in development policy
read-only at `/etc/epoch/bootstrap-policy.json`:

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
        "replica_count": 3
      }
    }
  }'
```

The Go reconciler applies the desired generation to the Rust catalog leader and
then samples the route on every configured Rust node. Poll the browser-safe
projection:

```shell
curl --fail-with-body \
  -H 'authorization: Bearer epoch-dev-admin-v1' \
  http://127.0.0.1:8080/v1/regional/resources
```

A ready tablet has three distinct `voter_node_ids` and a `leader_node_id` that
belongs to that observed voter set. Desired replicas never count as observed
voters. Resource generations, observed generations, tablet/group IDs, epochs,
voter IDs, and leader IDs are decimal JSON strings so JavaScript cannot round
them.

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

## Failure and recovery behavior

- One voter loss leaves a majority able to elect and commit. Go reports only
  the two routes it can currently validate and marks placement degraded.
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

It builds the real Go control binary, creates a resource through Go, verifies
the BFF/CORS/placement contract, kills and reopens Go against the same metadata
file, proves exact replay, creates and mutates all four profiles, kills a
leader, catches it up, kills all nodes, reopens the same volumes, compares
digests, and deletes only its scoped containers/network/volumes.

## Current boundaries

- Placement is exactly three configured voters, not a zone/rack-aware solver.
- Membership changes, online rebalance, repair, split/merge, snapshots,
  compaction, retention deletion, and read barriers are absent.
- Rust regional HTTP and Go management enforce the bootstrap policy, and the
  console supplies a session-only credential. They still have no TLS/OIDC/mTLS,
  token expiry/revocation, rate limiting, replicated policy, or immutable audit
  export. Peer port 7701 and the standalone local API remain unauthenticated.
- Go management metadata is durable for one process and one bbolt file. It is
  not replicated, multi-instance linearizable, backed up automatically, or
  protected by management leader election.
- There is no public regional SDK contract. Go, Java, and Python SDKs currently
  cover the standalone HTTP profiles documented on the published docs page.
- The BFF reports node identity only; zone/rack/failure-domain separation is
  explicitly unverified.

Stop the development topology with:

```shell
make compose-regional-down
```

That retains named volumes. Add `--volumes` to the underlying Compose command
only when intentionally discarding all regional test data.
