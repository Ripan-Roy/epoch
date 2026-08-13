# Regional Cache SDK

Epoch's repository-local Go, Java, and Python SDKs expose the complete
implemented single-shard replicated Cache lifecycle through
`RegionalCacheClient`. The client talks directly to Rust voters, discovers the
current leader before every operation, carries generation and tablet-epoch
fences, requests linearizable reads, and preserves caller-owned mutation
identity across one bounded rediscovery.

This contract is separate from the standalone `EpochClient` Cache helpers. The
standalone API can select volatile or local-durable behavior on one host; the
regional client uses the fixed-three-voter replicated Cache tablet.

## Provision a Cache

Start the disposable regional topology and Go management bridge as described in
[Regional Stream SDK](REGIONAL_STREAM_SDK.md), then apply a Cache resource:

```shell
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-sessions-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"cache","name":"sessions",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'
```

The public docs embed these exact executable sources:

- [Go](../console/src/quickstarts/regional_cache/quickstart.go)
- [Java](../console/src/quickstarts/regional_cache/RegionalCacheQuickstart.java)
- [Python](../console/src/quickstarts/regional_cache/quickstart.py)

Each example writes and exactly replays a value, uses versioned CAS, commits a
multi-value transaction, acquires and applies a fenced lock guard, increments a
counter, releases the lock, expires a TTL value explicitly, and reads
linearizable observations and status.

## Client construction

All clients require every voter endpoint, a bearer token, and the complete
namespace scope. Endpoint order controls reachability only. An operation selects
an endpoint only when discovery reports `accepts_writes: true`.

Go:

```go
client, err := epoch.NewRegionalCacheClient(
    []string{"http://127.0.0.1:18661", "http://127.0.0.1:18662", "http://127.0.0.1:18663"},
    token,
    epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
    3*time.Second,
)
```

Java:

```java
var client = new RegionalCacheClient(
    endpoints,
    token,
    new RegionalScope("acme", "shop", "dev", "core"),
    Duration.ofSeconds(3));
```

Python:

```python
client = RegionalCacheClient(
    endpoints,
    token=token,
    scope=RegionalScope("acme", "shop", "dev", "core"),
    timeout=3.0,
)
```

## Value model

Use type-specific constructors rather than passing arbitrary JSON. Every SDK
validates the local representation before discovery.

| Wire kind | Go | Java | Python |
|---|---|---|---|
| String | `NewRegionalCacheString` | `RegionalCacheValue.string` | `RegionalCacheValue.string` |
| Blob | `NewRegionalCacheBlob` | `RegionalCacheValue.blob` | `RegionalCacheValue.blob` |
| Signed counter | `NewRegionalCacheCounter` | `RegionalCacheValue.counter` | `RegionalCacheValue.counter` |
| Hash | `NewRegionalCacheHash` | `RegionalCacheValue.hash` | `RegionalCacheValue.hash` |
| List | `NewRegionalCacheList` | `RegionalCacheValue.list` | `RegionalCacheValue.list` |
| Unique set | `NewRegionalCacheSet` | `RegionalCacheValue.set` | `RegionalCacheValue.set` |
| Finite-score sorted set | `NewRegionalCacheSortedSet` | `RegionalCacheValue.sortedSet` | `RegionalCacheValue.sorted_set` |

Blob values are JSON byte arrays. Counter values and all unsigned 64-bit fields
are canonical decimal strings on the wire. Set constructors reject duplicates;
sorted-set constructors reject NaN and infinity. Java uses `BigInteger` for the
complete unsigned 64-bit range.

## Lifecycle surface

| Behavior | Go | Java | Python |
|---|---|---|---|
| Set | `Set` | `set` | `set` |
| Conditional delete | `Delete` | `delete` | `delete` |
| Version/missing CAS | `CompareAndSet` | `compareAndSet` | `compare_and_set` |
| Signed increment | `Increment` | `increment` | `increment` |
| Atomic transaction | `Transaction` | `transaction` | `transaction` |
| Acquire lock | `AcquireLock` | `acquireLock` | `acquire_lock` |
| Renew lock | `RenewLock` | `renewLock` | `renew_lock` |
| Release lock | `ReleaseLock` | `releaseLock` | `release_lock` |
| Apply expiry | `Maintain` | `maintain` | `maintain` |
| Mutation lookup | `Mutation` | `mutation` | `mutation` |
| Observe key | `Observe` | `observe` | `observe` |
| Tablet status | `Status` | `status` | `status` |

Every mutation requires a nonempty caller-owned idempotency key. Set, delete,
CAS, and increment optionally accept a `RegionalCacheLockGuard`. TTL is nonzero
relative milliseconds. An expected version of `0` means no live item where that
operation supports version matching.

## CAS and transactions

`RegionalCacheVersion(version)` / `RegionalCacheExpectation.version(version)`
checks one non-ABA item version. `RegionalCacheMissing(revision)` /
`RegionalCacheExpectation.missing(revision)` checks that the key is absent and
the shard has not changed since the observation.

A transaction carries the exact observed shard revision and one to 128
distinct-key `RegionalCacheMutation` values. It may contain set, delete, CAS,
and increment. The tablet stages the entire operation: either one new revision
contains every result or the committed receipt reports a business rejection
without partial state. Lock-management and nested transaction operations cannot
be embedded.

## Fenced locks

Acquire requires a lock key, owner, monotonically increasing owner epoch, and
nonzero lease duration. The result includes:

- an opaque `lease_token` for renewal, release, and guarded mutations;
- a `lease_generation` that advances on renewal; and
- a `fencing_token` containing tablet epoch and acquisition index.

Renewal rotates the lease token. Applications must discard the old token.
Downstream protected systems compare the fencing token lexicographically as
`(tablet_epoch, acquisition_index)`; possession of a lease token alone is not a
safe downstream fence.

## TTL and maintenance

Observations are pure reads. A logically expired value is not removed by a
read. In the regional runtime the current Raft leader automatically submits the
same bounded `maintain` command at the earliest value or lock deadline; each
command removes at most 1,000 due entries through consensus. Applications may
still submit explicit maintenance for diagnostics or recovery. The scheduler
uses the exact due time, so expiry ordering remains deterministic and
recoverable across leader loss.

## HTTP contract

The shard base path is:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}
```

`GET` on that path performs discovery. Operations append `mutations`,
`mutations/{proposal_id}`, `observations?key=...`, or `status`.

Every data request carries:

```text
authorization: Bearer <token>
x-epoch-resource-generation: <discovered decimal generation>
x-epoch-tablet-epoch: <discovered decimal tablet epoch>
```

Every SDK read also carries `x-epoch-read-consistency: linearizable`. The server
confirms the read barrier in response headers and JSON. The SDK never silently
falls back to a follower or stale local state.

## Retry and outcome rules

- `not_leader`, `fenced`, `route_not_found`, `route_unavailable`,
  `read_barrier_timeout`, and retryable transport/server errors allow one
  rediscovery cycle.
- A reconstructed mutation retains the exact idempotency key and semantic
  operation. The newly discovered term can change.
- Authentication, scope denial, schema validation, idempotency conflict, and a
  committed business rejection return immediately.
- A timeout can leave the outcome unknown. Retry the same operation with the
  same key or use mutation lookup when the proposal ID is known.

Go returns `*epoch.APIError`, Java returns `EpochApiException`, and Python
returns `EpochAPIError`.

## Verification

```shell
go test ./sdk/go/epoch ./console/src/quickstarts/regional_cache
PYTHONPATH=sdk/python/src python3 -m unittest discover -s sdk/python/tests -v
cd sdk/java && ./mvnw verify
make test-regional-runtime
bash tests/integration/docs-quickstarts.sh
```

The Docker campaign kills the active Cache leader before using the Python SDK,
then covers exact replay, every value kind, CAS, increment, an atomic
transaction, guarded writes, lease renewal, guarded delete, release, explicit
expiry, linearizable reads, voter catch-up, and all-voter reopen. Go and Java
exercise the same request contract in unit tests and compile their exact public
docs programs.

## Current boundary

This alpha is a complete SDK surface over the implemented single-shard tablet,
not a Redis-compatible production service. Background active expiry, LRU/LFU
eviction, multi-shard transactions, snapshots, Pub/Sub, multi-key pipelining,
connection-pool policy, dynamic membership, TLS/OIDC/mTLS, generated response
types, package-registry publication, and the production fault/scale matrix
remain open. See
[ADR-0019](adr/0019-regional-cache-v1-and-sdk-routing.md).
