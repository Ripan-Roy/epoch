# Regional Stream SDK

**Status:** Versioned multi-shard, transactional state-services alpha

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
[ADR-0026](adr/0026-regional-stream-batch-sdks.md) for SDK batch framing,
[ADR-0035](adr/0035-stream-state-services.md) for producer sequencing,
transactions, compaction, tiering, capture, replication, and superstreams, and
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
| Fetch with transaction isolation | `FetchWithIsolation` | `fetchWithIsolation` | `fetch(..., isolation=...)` |
| Push/dedicated long poll | `ConsumeLongPoll` | `consumeLongPoll` | `consume_long_poll` |
| Idempotent producer append | `AppendIdempotent` | `appendIdempotent` | `append_idempotent` |
| Begin transaction | `BeginTransaction` | `beginTransaction` | `begin_transaction` |
| Transactional append | `AppendTransaction` | `appendTransaction` | `append_transaction` |
| Commit transaction + offset | `CommitTransaction` | `commitTransaction` | `commit_transaction` |
| Abort transaction | `AbortTransaction` | `abortTransaction` | `abort_transaction` |
| Observe transaction | `Transaction` | `transaction` | `transaction` |
| Key compaction | `Compact` | `compact` | `compact` |
| Tier a hot prefix | `TierPrefix` | `tierPrefix` | `tier_prefix` |
| List tier manifests | `TierObjects` | `tierObjects` | `tier_objects` |
| Manual open-format capture | `Capture` | `capture` | `capture` |
| Configure automatic capture | `ConfigureCaptureSchedule` | `configureCaptureSchedule` | `configure_capture_schedule` |
| Observe capture schedule | `CaptureSchedule` | `captureSchedule` | `capture_schedule` |
| Read capture artifact | `CaptureArtifact` | `captureArtifact` | `capture_artifact` |
| Replicate source batch | `Replicate` | `replicate` | `replicate` |
| Partition recommendation | `PartitionAdvice` | `partitionAdvice` | `partition_advice` |
| Logical superstream merge | `FetchSuperstream` | `fetchSuperstream` | `fetch_superstream` |
| Commit or reset checkpoint | `CommitOffset` | `commitOffset` | `commit_offset` |
| Observe checkpoint and lag | `Lag` | `lag` | `lag` |
| Fetch from checkpoint | `FetchGroup` | `fetchGroup` | `fetch_group` |
| Claim one shard fence | `ClaimGroup` | `claimGroup` | `claim_group` |
| Fetch behind exact claim | `FetchClaimedGroup` | `fetchClaimedGroup` | `fetch_claimed_group` |
| Claim and revalidate assignment | `ClaimConsumerSession` | `claimConsumerSession` | `claim_consumer_session` |
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

## Producer, transaction, and isolation contract

An idempotent producer is identified by a nonempty producer ID and a nonzero
epoch. Sequences start at zero and must be contiguous. Retrying the exact
epoch/sequence/payload returns the original positions; changing the payload at
that sequence conflicts, and advancing the epoch fences the old producer. The
bounded replay history retains 256 sequences per producer.

A transaction is bound to one producer epoch and one physical Stream shard. It
may contain up to 128 records. Pending records appear only under
`read_uncommitted`; commit makes the whole transaction visible and can advance
one consumer checkpoint in the same replicated state transition. Abort keeps
the records hidden from `read_committed`. This is a tablet-local transaction,
not an atomic cross-shard or external-sink transaction.

Push and dedicated consumption use bounded HTTP long polling (1–30,000 ms).
Dedicated mode requires a consumer ID and runs on its own notification lane;
push mode forbids one. This is useful isolation in the alpha runtime, but it is
not a published dedicated-throughput SLO or a persistent bidirectional stream.

## Compaction, tiering, and capture

`Compact` keeps the latest committed record for each nonempty key, retains
unkeyed records, removes aborted records, and expires JSON `null` tombstones at
the configured inclusive deadline. Compact before tiering: tier objects are
immutable and a shard with a historical object rejects later compaction.

`TierPrefix` moves at most 1,024 non-pending hot records into a canonical JSON
object with an SHA-256 checksum and exact covered offset range. Read-committed
and read-uncommitted fetches transparently merge verified historical objects
with the hot log. The current alpha keeps object bytes in the replicated tablet
snapshot so the complete feature works locally; an external cloud-object-store
adapter, outage drill, and tier-fetch SLO remain production gates.

Manual capture writes a bounded range as canonical JSON Lines or a JSON array.
Automatic capture schedules use a 1-second to 31-day interval, a replicated
next-offset checkpoint, and a stable leader-owned maintenance proposal. A
pending transaction stops the capture boundary so it cannot be skipped. The
leader catches up missed deadlines without clock drift, and every artifact,
checksum, schedule, and checkpoint survives failover and full restart. At most
32 schedules and 32 retained artifacts exist per tablet; the oldest artifact
for the same automatic schedule is replaced when that bounded history fills.

Python example:

```python
producer = client.append_idempotent(
    "orders", shard, "producer-0", "checkout", 1, 0, event,
)
client.begin_transaction("orders", shard, "tx-open", "tx-1", "checkout", 1)
client.append_transaction(
    "orders", shard, "tx-write", "tx-1", "checkout", 1, 1, [result_event],
)
client.commit_transaction(
    "orders", shard, "tx-commit", "tx-1",
    offset_commit=StreamOffsetCommit("billing", shard, next_offset),
)
visible = client.fetch("orders", shard, 0, isolation="read_committed")
schedule = client.configure_capture_schedule(
    "orders", shard, "capture-config", "analytics", 60_000,
    format="json_lines",
)
```

The same lifecycle in Go:

```go
producer, err := client.AppendIdempotent(
    ctx, "orders", shard, "producer-0", "checkout", 1, 0, event,
)
_, err = client.BeginTransaction(ctx, "orders", shard, "tx-open", "tx-1", "checkout", 1)
_, err = client.AppendTransaction(
    ctx, "orders", shard, "tx-write", "tx-1", "checkout", 1, 1,
    []epoch.EventEnvelope{resultEvent},
)
committed, err := client.CommitTransaction(
    ctx, "orders", shard, "tx-commit", "tx-1",
    &epoch.StreamOffsetCommit{Group: "billing", Partition: shard, NextOffset: nextOffset},
)
visible, err := client.FetchWithIsolation(
    ctx, "orders", shard, 0, 100, epoch.StreamReadCommitted,
)
schedule, err := client.ConfigureCaptureSchedule(
    ctx, "orders", shard, "capture-config", "analytics", time.Minute,
    epoch.StreamCaptureJSONLines,
)
```

And Java:

```java
JsonNode producer = client.appendIdempotent(
    "orders", shard, "producer-0", "checkout", BigInteger.ONE,
    BigInteger.ZERO, event);
client.beginTransaction(
    "orders", shard, "tx-open", "tx-1", "checkout", BigInteger.ONE);
client.appendTransaction(
    "orders", shard, "tx-write", "tx-1", "checkout", BigInteger.ONE,
    BigInteger.ONE, List.of(resultEvent));
JsonNode committed = client.commitTransaction(
    "orders", shard, "tx-commit", "tx-1",
    new StreamOffsetCommit("billing", shard, nextOffset));
JsonNode visible = client.fetchWithIsolation(
    "orders", shard, BigInteger.ZERO, 100, StreamReadIsolation.READ_COMMITTED);
JsonNode schedule = client.configureCaptureSchedule(
    "orders", shard, "capture-config", "analytics", Duration.ofMinutes(1),
    StreamCaptureFormat.JSON_LINES);
```

## Replication, expansion, and superstreams

`Replicate` accepts one contiguous source-cluster/source-stream/source-shard
batch. The replicated checkpoint maps every source offset to one local offset,
an exact retry returns the same mapping, a gap conflicts, and a traversed path
containing the local cluster rejects a loop. This release provides the complete
ingress/checkpoint primitive; deployment-specific inter-region workers and a
two-region RPO/RTO drill remain operational work.

`PartitionAdvice` calculates an expand-only shard target from observed record
and byte density. The catalog already rejects decreases, preserves existing
tablet identities and ordered histories, allocates only new shard tablets, and
increments the resource generation. Generation-pinned keyed appends fail
closed across that expansion boundary.

A superstream is a client-side logical merge of 1–128 named Stream shards. The
SDK independently performs a linearizable fetch for every member, decorates
each record with its member name, and sorts by
`appended_at_ms/member/partition/offset`. The response declares
`snapshot_scope=independently_linearizable_members`; it is deterministic but
not an atomic cross-shard snapshot.

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
`maintain_consumer_session` provide an explicit sweep for diagnostics. The
current regional leader also proposes the exact due maintenance command
automatically.

Membership and per-shard checkpoint ownership remain separate replicated
states, but the SDKs provide an offset-preserving claim–revalidate handoff. The
resource helper pins resource generation, verifies the exact member,
generation, and sorted assignment on shard 0, reads every assigned checkpoint,
plans no more than 4,096 monotonic transitions, claims every shard with
deterministic idempotency keys, then re-reads shard 0. It returns no assignment
if a rebalance occurred. Partial claims are safe because they never move the
next offset; a newer session can advance the fence.

After claiming, use exact-member/generation claimed fetch. Each request is a
linearizable bounded pull of 1–1,000 records from the durable next offset.
Process records, then commit with the same member and generation. A stale member
receives `409 fenced` for fetch and a committed typed rejection for mutation.
Namespace authorization currently permits a writer to call the low-level
claim; member-bound identity remains future G5 work. This release does not
claim atomic assignment-plus-offset commit, cooperative revoke acknowledgement,
exactly-once processing, or a persistent streaming transport.

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
claimed = client.claim_consumer_session(
    "orders", "billing", "worker-a", generation,
    idempotency_key_prefix="billing-claim-worker-a-v1",
)
records = client.fetch_claimed_group(
    "orders", claimed[0], "billing", "worker-a", generation, limit=100,
)
left = client.leave_consumer_session(
    "orders", "billing", "worker-a", generation,
    idempotency_key="billing-leave-worker-a",
)
```

## Executable examples

The complete examples select a shard from `customer-0`, perform a keyed append,
repeat the exact append, submit a two-record gzip batch, fetch by offset, fetch from a group checkpoint, commit
that checkpoint, observe lag, join/heartbeat/observe, claim the assignment,
perform an exact-member/generation bounded fetch, and leave a coordinated
consumer session, then configure a combined retention policy, commit idle
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
- `not_leader`, `route_not_found`, `route_unavailable`,
  `read_barrier_timeout`, retryable transport/server failures, and a routing
  `fenced` envelope with top-level `retryable: true` allow one rediscovery
  cycle. An application `409 fenced` such as a stale consumer member/generation
  is returned unchanged and never rewritten as `unavailable`.
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

The real recovery gate builds the node image, creates independently replicated
Stream resources, kills active leaders, routes Python keyed appends across three
shards, and verifies logical receipt/record/checkpoint identities. It also runs
gzip batch, join/rebalance/heartbeat/expiry, retention, producer fencing,
committed rejection replay, transaction isolation and atomic offset commit,
compaction, tiering, manual/automatic capture, replication ingress, partition
advice, long poll, and superstream reads. It catches up stopped voters, kills
every voter, reopens the same volumes, and re-verifies all advanced state:

```shell
make test-regional-runtime
```

## Current boundaries

This versioned alpha covers independently replicated Stream shards, stable
generation-pinned key routing and expand-only allocation, direct per-shard
operations, consumer sessions and claims, time/size retention, caller-framed
atomic batches, producer fencing, same-tablet transactions, compaction,
transparent embedded tier history, capture scheduling, replication ingress,
bounded long polling, and client-side logical superstreams in every first-party
SDK. Automatic split/merge/remapping, virtual shards, cooperative revoke,
sticky/rack-aware assignment, persistent bidirectional streaming, legal hold,
cross-shard transactions, automatic producer batching/codec selection,
generated response models, package-registry publication, TLS/OIDC/mTLS,
dynamic voter membership, external object-store and cross-region workers, and
the production fault/scale/SLO matrix remain open.
