# Regional Cache SDK

Epoch's repository-local Go, Java, and Python SDKs expose the complete
non-deferred single-shard Cache lifecycle through `RegionalCacheClient`. The
clients discover the current Rust leader, carry resource-generation and tablet
fences, preserve caller-owned mutation identities during rediscovery, and use
linearizable reads by default.

This is separate from the standalone `EpochClient` Cache helpers. The
standalone profile is one-host volatile/local-durable state; the regional
client uses the fixed three-voter tablet.

## Provision the resource

Start the regional topology and Go control bridge, then create a Cache:

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
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"confidential","tags":{"service":"sessions","profile":"cache"}},
      "spec":{"shard_count":1,"replica_count":3,"configuration":{
        "shard_count":1,"max_entries":10000,
        "max_memory_bytes":262144,"max_cold_bytes":262144,
        "default_ttl_ms":null,"eviction":"all_keys_lru",
        "durability":"quorum_durable"
      },"placement":{"allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"}}
    }
  }'
```

The configuration is immutable in this alpha. `max_entries` and the optional
memory/cold byte caps are per shard. Admission counts canonical retained bytes,
not process RSS. Supported eviction policies are `no_eviction`, all-key
LRU/LFU/random, and volatile LRU/LFU/random/TTL. Regional durability accepts
`quorum_durable` or `replicated_memory`; the persisted fixed-voter runtime
reports when the latter is fulfilled by the stronger quorum-durable path.

The public docs embed and compile these exact programs:

- [Go](../console/src/quickstarts/regional_cache/quickstart.go)
- [Java](../console/src/quickstarts/regional_cache/RegionalCacheQuickstart.java)
- [Python](../console/src/quickstarts/regional_cache/quickstart.py)

## Complete cross-language surface

| Capability | Go | Java | Python |
|---|---|---|---|
| Scalar/collection value | `NewRegionalCache*` | `RegionalCacheValue.*` | `RegionalCacheValue.*` |
| Set/get/observe/delete/CAS/increment | `Set` etc. | `set` etc. | `set` etc. |
| Typed atomic transform | `Transform` | `transform` | `transform` |
| Atomic transaction/batch | `Transaction` / `AtomicBatch` | `transaction` / `atomicBatch` | `transaction` / `atomic_batch` |
| Non-atomic correlated pipeline | `Multiplex` | `multiplex` | `multiplex` |
| Fenced lease lock | `AcquireLock` / `RenewLock` / `ReleaseLock` | camel-case equivalents | snake-case equivalents |
| Expiry maintenance | `Maintain` | `maintain` | `maintain` |
| Durable changes | `Changes` | `changes` | `changes` |
| Backup and PITR restore | `Backup` / `Restore` | `backup` / `restore` | `backup` / `restore` |
| Typed advanced query | `Query` | `query` | `query` |
| Lossy Pub/Sub | `CreateSubscription` / `Publish` / `PollSubscription` / `DeleteSubscription` | camel-case equivalents | snake-case equivalents |
| Linearizable status | `Status` | `status` | `status` |

Strict ordinary value constructors cover strings, byte blobs, signed counters,
hashes, lists, unique sets, and finite-score sorted sets. Blobs are JSON byte
arrays and all unsigned 64-bit wire values are decimal strings. Java uses
`BigInteger` for the full range.

## Atomic state operations

`Get` is a consensus mutation because it records access for deterministic
LRU/LFU policy; exact retry does not count twice. `Observe` is a pure
linearizable read and never changes eviction order.

CAS distinguishes a non-ABA item version from a missing-at-shard-revision
expectation. Transactions carry the observed shard revision and one to 128
distinct-key set, delete, CAS, increment, or transform mutations. They stage
the whole result and either commit one revision or reject without partial
state. `AtomicBatch` is an alias for this exact all-or-nothing contract.

`Multiplex` is intentionally different: one HTTP request carries one to 128
unique correlation IDs and unique idempotency keys. Results are returned in
request order, but items commit independently and `atomic` is `false`. Use it
for unrelated calls; use an atomic batch when operations share one invariant.
Long-lived clients reuse their HTTP transport; bound application concurrency.

## Transforms and queries

Construct a transform with `NewRegionalCacheTransform`, Java's
`transform(kind, fields, ...)`, or `RegionalCacheTransform`. Supported kinds:

| Family | Mutation kinds | Query kinds |
|---|---|---|
| Collections | `hash_put`, `hash_remove`, `list_push`, `list_pop`, `set_add`, `set_remove`, `sorted_set_add`, `sorted_set_remove` | observe returned value |
| Bitmap/cardinality | `bitmap_set`, `cardinality_add` | `bitmap_get`, `cardinality_estimate` |
| Probabilistic | `bloom_add`, `cuckoo_add`, `cuckoo_delete` | `bloom_contains`, `cuckoo_contains` |
| Geo | `geo_upsert`, `geo_remove` | `geo_radius` |
| JSON | `json_set`, `json_remove`, `json_index_upsert`, `json_index_remove` | `json_pointer`, `json_search` |
| Vector | `vector_upsert`, `vector_remove` | `vector_search` |

All transforms are atomic, type checked, optionally version/TTL/lock guarded,
and can participate in a transaction. Bounds include a 1,048,576-bit bitmap,
10,000 geo points, 256 KiB JSON documents, 10,000-document/2 MiB JSON indexes
over at most 32 pointers, and 10,000 vector documents with at most 2,048
dimensions. JSON search is exact over canonical pointer values. Vector search
is an exact bounded cosine/text hybrid with metadata filters; it is not an ANN
latency or recall claim.

Python example:

```python
client.transform(
    "sessions", 0, "profile-index-v1", "profiles",
    RegionalCacheTransform("json_index_upsert", {
        "id": "user-1",
        "document": {"value": {"role": "admin", "active": True}},
        "indexed_pointers": ["/role"],
    }),
)
admins = client.query(
    "sessions", 0, "json_search",
    {"key": "profiles", "pointer": "/role", "value": "admin", "limit": 10},
)
```

## Durability, changes, and recovery

`Changes(from_sequence, limit)` reads the replicated 1,024-record mutation,
expiry, eviction, and restore history. The response publishes the next cursor
and retention floor; a stale cursor fails explicitly.

`Backup` returns a canonical base64 artifact, SHA-256 state digest, captured
revision/time, and oldest restorable revision. The decoded artifact is capped
at 320 KiB. `Restore` validates canonical encoding, digest, matching immutable
configuration, capacity, and requested revision before one consensus commit.
It removes values created after that revision, excludes values expired at
restore time, and assigns fresh versions so old CAS tokens cannot revive.

This is caller-managed resource-local backup/PITR. Managed schedules,
encryption/key management, remote catalogs/retention, cross-resource restore,
and full-cluster disaster recovery remain separate managed-service work.

## Cold storage class

`Set` accepts storage class `memory` or `cold`; Python uses
`storage_class="cold"`, Go uses `RegionalCacheWriteOptions.StorageClass`, and
Java uses the extended `set` overload. Each voter fsyncs a canonical per-key
cold file after committed apply. Cold observation/query reads and verifies that
file against replicated state. Status publishes retained/max bytes, backend,
read count, average/max microseconds, and
`observed_local_file_read_micros_not_an_slo`.

The alpha cold path is a real local-file read tier, not heap offload: canonical
values still reside in the replicated tablet image. It does not claim
production flash capacity relief, remote object storage, or a latency SLO.

## Pub/Sub semantics

Cache Pub/Sub is explicitly lossy and node-affine. A subscription contains up
to 64 exact channels or `*`/`?` patterns; polling drains up to 1,000 messages.
Each subscription holds at most 1,024 pending messages. Overflow drops the new
message and reports a counter. Subscriptions are not replicated or persisted,
so disconnect, rerouting, or process loss can lose them and their messages.
Use the durable change stream or Event Bus when replay or acknowledgement is
required.

## HTTP and retry contract

The shard base is:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}
```

Discovery is `GET` on the base. The SDKs route `mutations`, `multiplex`,
`observations`, `changes`, `backup`, `query`, `pubsub/...`, and `status` through
the discovered voter. Data calls carry bearer authorization, resource
generation, and tablet epoch. Mutations also carry the discovered current term;
reads carry `x-epoch-read-consistency: linearizable`.

One rediscovery is allowed for leader/fence/route/read-barrier or retryable
transport failures. Retried mutations preserve the semantic body and
idempotency key. Authentication, authorization, validation, idempotency
conflicts, and committed business rejections return immediately. A timeout can
be an unknown outcome: retry the same operation and identity.

## Verification and boundary

```shell
go test ./sdk/go/epoch/...
PYTHONPATH=sdk/python/src python3 -m unittest discover -s sdk/python/tests -p 'test_*.py'
cd sdk/java && ./mvnw clean test
bash tests/integration/docs-quickstarts.sh
make test-regional-runtime
```

The regional Docker campaign loses the active Cache leader, exercises ordinary
and advanced values, atomic and multiplex calls, byte eviction, cold-file reads,
backup/PITR, changes, lossy Pub/Sub, locks, TTL, catch-up, and all-voter reopen.
This is a fixed-topology single-shard alpha. Multi-shard routing/transactions,
automatic client coalescing, Redis/RESP compatibility, dynamic membership,
production identity/transport, package publication, scale/SLO evidence, and
`CACHE-015` CRDTs remain open. See
[ADR-0034](adr/0034-cache-state-services-and-cold-read-tier.md).
