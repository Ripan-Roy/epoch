# Regional Stream SDK

**Status:** Versioned multi-shard, atomic-batch, and coordinated-session alpha

**Languages:** Go, Java, and Python

The regional Stream client is the application-facing path to a Stream tablet
replicated by the fixed three-voter runtime. It discovers the active Rust
leader, authenticates every request, carries delete/recreate and tablet fences,
and requests quorum-confirmed reads. It does not send application data through
the Go control plane.

See [ADR-0017](adr/0017-regional-stream-v1-and-sdk-routing.md) for the binding
decision, [ADR-0023](adr/0023-stream-retention-policies.md) for retention,
[ADR-0024](adr/0024-stream-multishard-key-routing.md) for keyed routing,
[ADR-0025](adr/0025-stream-consumer-sessions.md) for consumer coordination,
[ADR-0026](adr/0026-regional-stream-batch-sdks.md) for SDK batch framing, and
[Regional runtime](REGIONAL_RUNTIME.md) for provisioning and operations.

## End-to-end flow

```text
application SDK append-by-key
   |
   | 1. authenticated discovery of shard 0 partition metadata
   | 2. FNV-1a UTF-8 key (or event ID) modulo shard count
   v
target logical shard ---- generation must still match step 1
   |
   | 3. current-leader discovery + observed generation/tablet epoch
   |    mutation also carries observed term + caller idempotency key
   v
current Stream tablet leader
   |
   | majority commit, local profile apply, typed receipt
   v
application
```

Discovery occurs before every operation. `AppendKeyed`, `appendKeyed`, and
`append_keyed` first discover the Stream partitioning contract, choose the
logical shard, then pin the initial resource generation while discovering that
shard's leader. A generation mismatch fails before any write; the client never
silently remaps an uncertain mutation across expansion. A leader or fence race
triggers one bounded rediscovery attempt only when the error is retryable. The
mutation's idempotency key never changes.

## Provision the local three-node region

Start the three Rust voters:

```shell
make compose-regional-up
```

Start `epoch-control` in another terminal:

```shell
EPOCH_CONTROL_REGIONAL_ENDPOINTS=http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663 \
EPOCH_CONTROL_STATE_PATH=.epoch/control/registry.db \
EPOCH_AUTH_POLICY_PATH=spec/auth/bootstrap-policy-v1.example.json \
EPOCH_CONTROL_REGIONAL_TOKEN=epoch-dev-control-v1 \
go run ./control/cmd/epoch-control
```

Apply the three-shard `acme/shop/dev/core` Stream named `orders` using the exact request in
[Apply one managed resource](REGIONAL_RUNTIME.md#apply-one-managed-resource).
The development admin token is `epoch-dev-admin-v1`. These checked-in
credentials are public fixtures and must never be reused outside a disposable
local environment.

## Constructors

| Language | Scope | Client |
|---|---|---|
| Go | `epoch.RegionalScope{Organization, Project, Environment, Namespace}` | `epoch.NewRegionalStreamClient(endpoints, token, scope, timeout)` |
| Java | `new RegionalScope(organization, project, environment, namespace)` | `new RegionalStreamClient(endpoints, token, scope, timeout)` |
| Python | `RegionalScope(organization, project, environment, namespace)` | `RegionalStreamClient(endpoints, token=..., scope=..., timeout=...)` |

Pass every known node endpoint. For the local Compose region those are
`http://127.0.0.1:18661`, `:18662`, and `:18663`. Custom transports are
available for tests in all three SDKs.

## Operations

| Semantics | Go | Java | Python |
|---|---|---|---|
| Published shard selection | `StreamShardFor` | `StreamPartitioner.shardFor` | `stream_shard_for` |
| Keyed append | `AppendKeyed` | `appendKeyed` | `append_keyed` |
| Single append | `Append` | `append` | `append` |
| Atomic framed batch | `AppendBatch` | `appendBatch` | `append_batch` |
| Encode none/gzip batch | `EncodeStreamBatch` | `StreamBatchFrame.encode` | `StreamBatchFrame.encode` |
| Wrap any supported frame | `NewStreamBatchFrame` | `StreamBatchFrame.compressed` | `StreamBatchFrame.from_compressed` |
| Offset fetch | `Fetch` | `fetch` | `fetch` |
| Commit or reset checkpoint | `CommitOffset` | `commitOffset` | `commit_offset` |
| Observe checkpoint and lag | `Lag` | `lag` | `lag` |
| Fetch from checkpoint | `FetchGroup` | `fetchGroup` | `fetch_group` |
| Join/renew consumer session | `JoinConsumerSession` | `joinConsumerSession` | `join_consumer_session` |
| Heartbeat session member | `HeartbeatConsumerSession` | `heartbeatConsumerSession` | `heartbeat_consumer_session` |
| Leave consumer session | `LeaveConsumerSession` | `leaveConsumerSession` | `leave_consumer_session` |
| Commit expiry maintenance | `MaintainConsumerSession` | `maintainConsumerSession` | `maintain_consumer_session` |
| Observe membership/assignment | `ConsumerSession` | `consumerSession` | `consumer_session` |
| Configure retention | `ConfigureRetention` | `configureRetention` | `configure_retention` |
| Commit idle maintenance | `MaintainRetention` | `maintainRetention` | `maintain_retention` |
| Observe retention | `Retention` | `retention` | `retention` |

Keyed append reads `EventEnvelope.key`; an empty or absent key falls back to
`EventEnvelope.id`. The algorithm identifier is
`fnv1a64_utf8_mod_n_v1`: unsigned FNV-1a 64 over the exact UTF-8 bytes, modulo
the advertised nonzero shard count. These contract vectors are pinned in Rust,
Go, Java, and Python:

| Value | Shards | Result |
|---|---:|---:|
| `customer-42` | 16 | 14 |
| `order-1` | 16 | 13 |
| `café` | 16 | 9 |
| `東京` | 16 | 15 |

## Atomic framed batches

One batch targets one explicit logical shard and becomes visible as one atomic
tablet transition. Every record has a unique unsigned 32-bit
`client_sequence`; the response correlates that sequence with an exact decimal
offset and the outer logical partition. Batches contain 1–1,000 records, at
most 4 MiB of canonical expanded JSON, and at most 360 KiB of compressed frame
bytes.

Go `EncodeStreamBatch`, Java `StreamBatchFrame.encode`, and Python
`StreamBatchFrame.encode` build compact Rust/Serde-compatible JSON and provide
dependency-free `none` and RFC 1952 gzip framing. `NewStreamBatchFrame`,
`StreamBatchFrame.compressed`, and `StreamBatchFrame.from_compressed` wrap exact
caller-produced standard LZ4, Snappy, Zstandard, gzip, or uncompressed bytes.
The Rust server still decompresses and validates the full canonical document,
declared sizes/count, unique sequences, envelope validity, output ceiling, and
Zstandard window before proposing anything.

Retry an unknown outcome using the same `StreamBatchFrame` and idempotency key.
Do not recompress equivalent records under that key: exact frame bytes and
metadata are semantic identity. The SDK never splits a batch across keys or
shards and does not provide automatic batching, compression negotiation,
connection credit, or non-atomic partial success.

Minimal Python gzip batch:

```python
frame = StreamBatchFrame.encode(
    [
        StreamBatchRecord(101, first_event),
        StreamBatchRecord(102, second_event),
    ],
    "gzip",
)
receipt = client.append_batch(
    "orders", shard, "orders-gzip-batch-1", frame,
)
```

Append and checkpoint operations require an explicit idempotency key. A
checkpoint also requires the caller's nonzero member generation. `reset` is the
only operation allowed to rewind. The first accepted generation is 1; another
member must take exactly the next generation.

Fetch limits are 1–1,000 records. Offsets mean the next record to fetch and are
serialized as decimal strings by the server. Go uses `uint64`, Python uses
arbitrary-precision `int`, and Java provides `BigInteger` overloads so the
complete unsigned 64-bit range remains representable.

`StreamRetentionPolicy` accepts optional per-partition record, canonical-byte,
and age limits. Go uses zero to omit a bound; Java uses `null`; Python uses
`None`. Configured values must be within 100,000 records, 3 MiB, and ten years.
Configuration and maintenance require idempotency keys. Retention observation
is linearizable and reports the active policy, watermark, base/end offsets,
retained record count, and retained canonical bytes.

## Coordinated consumer sessions

Consumer sessions are resource-wide and always use logical shard 0 as the
coordinator. A successful join returns the current `group_generation`, complete
lexically ordered membership, and the joining member's `assigned_shards`.
Shard `s` belongs to member `s % member_count`, so every live shard has exactly
one deterministic owner and assignment sizes differ by at most one.

Session timeouts are whole milliseconds from 1,000 through 300,000. Heartbeat
and leave require the current positive generation; an old generation is a
committed `stale_generation` business rejection. Deadlines expire inclusively.
Session commands sweep expired members before applying their requested action,
while `MaintainConsumerSession`, `maintainConsumerSession`, and
`maintain_consumer_session` provide an explicit sweep for idle groups. There is
no background timer in this alpha.

Membership generation and the pre-existing per-shard checkpoint-owner
generation are separate fences. Assignment tells an application which logical
shards it should process; the application must still perform an explicit
checkpoint handoff on each shard and stop processing revoked assignments. This
release does not claim atomic assignment-plus-offset commit, cooperative revoke
acknowledgement, or exactly-once processing.

Minimal Python lifecycle:

```python
joined = client.join_consumer_session(
    "orders", "billing", "worker-a", 30_000,
    idempotency_key="billing-join-worker-a",
)
generation = int(joined["receipt"]["group_generation"])
assigned = joined["receipt"]["assigned_shards"]
heartbeat = client.heartbeat_consumer_session(
    "orders", "billing", "worker-a", generation,
    idempotency_key="billing-heartbeat-worker-a-1",
)
observed = client.consumer_session("orders", "billing")
left = client.leave_consumer_session(
    "orders", "billing", "worker-a", generation,
    idempotency_key="billing-leave-worker-a",
)
```

## Executable examples

The complete examples select a shard from `customer-0`, perform a keyed append,
repeat the exact append, submit a two-record gzip batch, fetch by offset, fetch from a group checkpoint, commit
that checkpoint, observe lag, join/heartbeat/observe/leave a coordinated
consumer session, configure a combined retention policy, commit idle
maintenance, and inspect retention:

- [Go regional quickstart](../console/src/quickstarts/regional/quickstart.go)
- [Java regional quickstart](../console/src/quickstarts/regional/RegionalQuickstart.java)
- [Python regional quickstart](../console/src/quickstarts/regional/quickstart.py)

The same files are embedded verbatim in the published documentation page.

Run Go:

```shell
go run ./console/src/quickstarts/regional/quickstart.go
```

Run Python:

```shell
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python
python console/src/quickstarts/regional/quickstart.py
```

Run Java:

```shell
cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional/RegionalQuickstart.java \
  -d target/regional-docs-classes
java -cp "target/regional-docs-classes:$EPOCH_JAVA_CP" RegionalQuickstart
```

Override `EPOCH_REGIONAL_ENDPOINTS` with a comma-separated endpoint list and
`EPOCH_TOKEN` with the scoped bearer credential.

## HTTP contract

The base route is:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}
```

`GET` on that path performs discovery. Data operations append:

```text
/records
/records/batches
/groups/{group}/offsets
/groups/{group}/lag
/groups/{group}/records
/groups/{group}/sessions
/groups/{group}/sessions/{member}/heartbeat
/groups/{group}/sessions/{member}
/groups/{group}/sessions/maintenance
/retention
/retention/maintenance
```

Stream discovery includes:

```json
{
  "stream_partitioning": {
    "algorithm": "fnv1a64_utf8_mod_n_v1",
    "key_encoding": "utf8",
    "missing_key_fallback": "event_id",
    "shard_count": 3
  }
}
```

The outer `{shard}` is the logical Stream partition. Each independently
replicated tablet still accepts canonical physical `partition: 0` commands;
the regional response layer externalizes the logical shard in mutation
responses, receipts, batch record receipts, fetched records, checkpoints,
retention observations, and status. This preserves existing command and
snapshot bytes while preventing callers from seeing every shard as partition
zero.

Session suffixes are valid only on shard 0. Join is `POST .../sessions`,
heartbeat is `PUT .../sessions/{member}/heartbeat`, leave is
`DELETE .../sessions/{member}`, maintenance is
`POST .../sessions/maintenance`, and observation is `GET .../sessions`.
Mutations return browser-safe decimal generation, deadline, and watermark
values. Observation is linearizable.

Every data request carries:

```text
authorization: Bearer <token>
x-epoch-resource-generation: <discovered decimal generation>
x-epoch-tablet-epoch: <discovered decimal tablet epoch>
```

Reads additionally carry
`x-epoch-read-consistency: linearizable`. Successful reads expose the exact
barrier evidence in both response headers and JSON. There is no SDK option that
silently downgrades these calls to a stale follower.

## Error and retry rules

- Authentication, scope denial, validation, idempotency conflict, and committed
  business rejection are definitive and are not rewritten as availability
  failures.
- `not_leader`, `fenced`, `route_not_found`, `route_unavailable`,
  `read_barrier_timeout`, and retryable transport/server failures allow one
  rediscovery cycle.
- A timeout can leave a mutation outcome unknown. Retry the same semantic
  request with the same idempotency key.
- Keyed append treats a resource-generation change between partition discovery
  and target discovery as definitive for that attempt and sends no mutation.
  Rediscover intentionally after resolving the resource change.
- After two unsuccessful discovery/operation cycles the SDK returns its typed
  unavailable/API error with the final cause. It does not loop indefinitely.

Go uses `*epoch.APIError`, Java uses `EpochApiException`, and Python uses
`EpochAPIError`.

## Verification

Focused SDK and route gates:

```shell
go test ./sdk/go/...
PYTHONPATH=sdk/python/src python3 -m unittest discover -s sdk/python/tests -p 'test_*.py'
cd sdk/java && ./mvnw verify
cargo test -p epoch-node regional_router::tests
```

The real recovery gate builds the node image, creates three independently
replicated Stream shards, kills the active leader, routes Python keyed appends
to shards 0, 1, and 2, verifies logical receipt/record/checkpoint identities,
runs a correlated gzip SDK batch plus two-member join/rebalance/heartbeat/inclusive expiry and retention
configure/maintenance/observation, restarts and catches up the old voter, kills
every voter, reopens the same volumes, and verifies the session plus per-shard
state converged:

```shell
make test-regional-runtime
```

## Current boundaries

This versioned alpha covers several independently replicated Stream shards,
stable key routing for the current resource generation, direct per-shard
operations, replicated deterministic join/heartbeat/leave/dead-member expiry
and resource-wide assignment, caller-supplied checkpoint generations, and
replicated time/size/combined retention, and bounded caller-framed atomic
batches in every first-party SDK. Automatic split/merge/remapping,
virtual shards, background session or retention scheduling, cooperative revoke
handshake, sticky/rack-aware assignment, server-push consumption, atomic
assignment-plus-offset handoff, keyed compaction, legal hold, cross-shard
produce-and-offset transactions, streaming Produce, automatic batching or
compression selection, generated
response models, package-registry publication, TLS/OIDC/mTLS, dynamic voter
membership, and the production fault/scale matrix remain open.
