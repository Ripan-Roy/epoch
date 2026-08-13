# Experimental Stream Tablet

The first profile-integrated consensus slice is a single configured,
single-partition Stream tablet backed by one fixed three-voter Raft group. It
exists to prove the command, commit, application, idempotency, failover, and
recovery boundary before Epoch exposes clustered durability through the public
API or SDKs.

The regional runtime now materializes several of these tablets for one Stream:
one logical shard/partition and one independent consensus group per tablet.
Canonical tablet commands still name physical partition 0. The node binds the
catalog shard as runtime metadata and externalizes that logical partition in
regional receipts, records, checkpoints, retention observations, and status.
This preserves historical command bytes and legacy snapshot decoding; see
[ADR-0024](adr/0024-stream-multishard-key-routing.md).

## What is implemented

```text
typed append, compressed batch, checkpoint, retention, or session request
  -> canonical versioned tablet command
  -> persistent Raft proposal
  -> durable fixed-voter-majority commit
  -> deterministic StreamTablet.apply
  -> typed receipt
```

- `epoch-tablet` owns the strict command codec and deterministic Stream state.
- `epoch-consensus` remains profile-agnostic and owns Raft/EPRS persistence.
- `epoch-node` applies committed commands on the consensus actor before making
  a successful typed result observable. A missing actor-applied receipt fails
  closed; an HTTP task never applies profile state on the actor's behalf.
- Startup replays every committed proposal into a fresh tablet before the
  experimental status endpoint becomes available.
- The consensus log is the only clustered source of truth. Commands are never
  duplicated into the standalone `engine.wal`.
- A stable idempotency key maps to a scoped proposal ID. Pending and committed
  retries inspect the original canonical bytes, so a server timestamp is not
  resampled and a changed semantic payload is rejected. Every commit
  notification is revalidated against the waiting request, so an overwritten
  old-leader proposal cannot satisfy a waiter for different input.

The historical single append remains canonical command v1 byte-for-byte. A
batch alone emits v2, a consumer-group offset mutation alone emits v3, and a
retention configuration or maintenance mutation alone emits v4. Consumer
session join, heartbeat, leave, and maintenance alone emit v5. All
accept only partition `0`, reject unknown fields and
version/kind mismatches, are limited to the consensus proposal ceiling, and
must match canonical JSON exactly. A batch contains 1–1,000 unique client
sequences in canonical record JSON, transported as standard base64 with
`none`, gzip, LZ4 frame, Snappy framed, or Zstd frame compression. Its frame is
limited to 360 KiB and decompressed output to 4 MiB; Zstd's window is capped at
8 MiB. Exact counts and sizes are checked before proposal and again on every
voter.

Command v3 replicates a group's next offset and caller-supplied ownership
generation in the same history as records. The first owner uses generation 1;
the same member may continue in that generation, exactly the next generation
may establish a new owner, and old, skipped, or same-generation/different-member
requests are committed as typed fenced rejections. `commit` moves only forward;
`reset` is the explicit retained-range rewind. A group/member is limited to 256
bytes and one tablet retains at most 10,000 groups. These are checkpoint and
fencing semantics, not automatic membership or rebalancing.

Command v5 is coordinated on logical shard 0 and captures the resource shard
count. It replicates bounded members, 1–300 second deadlines, a monotonic time
watermark, and one membership generation. New join, leave, or one-or-more
inclusive deadline expirations advance the generation once. Rejoin and valid
heartbeat renew a deadline without changing it. Lexically sorted members own
shard `s` by `s mod member_count`, producing a deterministic resource-wide
assignment. Heartbeat and leave are generation-fenced typed outcomes; explicit
maintenance advances idle expiry. Native snapshot v2 persists the session map
and accepts legacy v1 snapshots as an empty map. See
[ADR-0025](adr/0025-stream-consumer-sessions.md).

Command v4 replaces or maintains an independent per-partition time, canonical
persisted-byte, and record-count policy. Age expiry is inclusive, combined
policies remove the oldest record whenever any configured bound requires it,
and offsets never change identity. Every append enforces the active policy;
idle streams advance age deletion through an explicit committed maintenance
call. The retained time watermark cannot regress across leader changes.
Configuration is bounded to 100,000 records, 3 MiB, and ten years per
partition. A record larger than the byte bound fails before mutation.

Retention preserves a group checkpoint below the new base and exposes
`checkpoint_out_of_range: true`. Group replay then fails at the retained-range
boundary until the caller explicitly resets to a valid offset. Profile
deduplication expires with its record, while the independently bounded
consensus retry suffix can still resolve an exact old proposal.

The proposal ID is currently a scope-separated 64-bit prefix of SHA-256; the
complete key remains in the command, so a collision fails as a conflict instead
of returning another operation's result. This is an experimental boundary, not
the final identifier format. JSON responses encode all 64-bit identity,
position, and envelope-time metadata as exact decimal strings so browser
clients do not lose precision. Append endpoints accept `expected_term` and
single-event `time_ms`, `deliver_at_ms`, and `ttl_ms` as either unsigned JSON
numbers or decimal strings; browser callers should use strings.

## Run the disposable proof

```shell
make test-stream-tablet
```

The gate builds one node image and starts three containers with independent
EPRS volumes. It verifies:

1. a follower returns typed `not_leader`;
2. typed and internal routes stay off the public listener, opaque proposal
   routes stay off the typed group, and public health remains `local_durable`;
3. an isolated leader never returns committed success;
4. the leader returns success only after majority persistence and local profile
   application;
5. all five batch modes round-trip through a real three-runtime cluster with
   per-client-sequence offsets, exact retry, changed-input conflict, and EPRS
   reopen;
6. the container client independently builds an RFC 1952 gzip frame in Python,
   commits it after leader replacement, and recovers both records after an
   all-voter `SIGKILL`;
7. commit, exact retry, wrong-owner rejection, generation handoff, explicit
   reset, stale-owner rejection, lag, and replay converge on three real voters
   and rebuild from EPRS;
8. malformed base64/frames, false metadata, duplicate sequences,
   non-canonical JSON, oversized expansion, and an excessive Zstd window fail
   before profile mutation;
9. the receipt reports `fixed_voter_majority_persisted`, two durable voter
   acknowledgements, Raft commit position, and Stream offset without claiming
   zone-aware quorum durability;
10. an exact retry returns the original offset while changed input conflicts;
11. a replacement leader safely rebinds an overwritten minority-only proposal,
   while the original semantic input conflicts instead of receiving its result;
12. the restarted voter catches up once; and
13. after all three containers receive `SIGKILL`, their existing EPRS volumes
   rebuild the exact pre-crash record document, checkpoint/lag/replay view, and
   state digest before readiness, and an exact retry still resolves to the
   original offset.

The script allocates loopback ports dynamically, uses a unique Compose project,
and removes only its own containers, network, and volumes. CI uploads its logs,
port map, and EPRS state if the proof fails.

## Manual topology

```shell
EPOCH_EXPERIMENTAL_STREAM_TABLET_ENABLED=true \
EPOCH_EXPERIMENTAL_STREAM_TABLET_NAME=orders \
docker compose -f deploy/compose/docker-compose.consensus-probe.yml up --build --detach
```

The default peer/experimental listeners are `127.0.0.1:17701` through
`127.0.0.1:17703`. Query each tablet status document and use the node reporting
`"role":"leader"` and its current `term`:

```shell
curl --fail --silent --show-error \
  http://127.0.0.1:17701/experimental/v1/tablets/stream/status
```

Append on the actual leader port:

```shell
curl --fail --silent --show-error \
  --header 'content-type: application/json' \
  --data '{
    "idempotency_key":"order-request-1",
    "expected_term":"1",
    "partition":0,
    "envelope":{
      "id":"order-1",
      "source":"checkout",
      "type":"order.created",
      "time_ms":"1",
      "payload":{"order_id":"1"}
    }
  }' \
  http://127.0.0.1:17701/experimental/v1/tablets/stream/records
```

Replace the example term and port with the observed leader values. A successful
response is `201 Created`; an exact completed retry is `200 OK`; a request that
is still unresolved at the bounded server wait is `202 Accepted`, preserving
whether the local state is `unknown` or `pending`; either has unknown outcome
certainty. `not_leader`, `stale_term`, and semantic-conflict responses are also
globally unknown because another voter may hold newer evidence. Resolve the
proposal ID decimal string through:

```text
GET /experimental/v1/tablets/stream/mutations/{proposal_id}
```

### Submit a compressed batch end to end

The batch endpoint accepts the frame produced by another runtime; it does not
require Rust's encoder. This Python example creates the exact canonical record
array, compresses it with the standard-library gzip implementation, and sends
the declared bytes to the current leader:

```shell
export EPOCH_STREAM_PORT=17701
export EPOCH_STREAM_TERM=1

python3 - <<'PYTHON' > /tmp/epoch-stream-batch.json
import base64
import gzip
import json
import os

records = []
for sequence in (10, 11):
    records.append({
        "client_sequence": sequence,
        "envelope": {
            "id": f"order-{sequence}",
            "source": "python-example",
            "type": "order.created",
            "time_ms": 1000,
            "headers": {},
            "content_type": "application/json",
            "payload": {"order_id": sequence},
            "priority": 0,
            "extensions": {},
        },
    })

plain = json.dumps(records, separators=(",", ":")).encode()
compressed = gzip.compress(plain, mtime=0)
print(json.dumps({
    "idempotency_key": "order-batch-1",
    "expected_term": os.environ["EPOCH_STREAM_TERM"],
    "partition": 0,
    "compression": "gzip",
    "record_count": len(records),
    "uncompressed_bytes": len(plain),
    "compressed_bytes": len(compressed),
    "payload_base64": base64.b64encode(compressed).decode(),
}, separators=(",", ":")))
PYTHON

curl --fail --silent --show-error \
  --header 'content-type: application/json' \
  --data-binary @/tmp/epoch-stream-batch.json \
  "http://127.0.0.1:${EPOCH_STREAM_PORT}/experimental/v1/tablets/stream/records/batches"
```

The receipt's top-level `offset` is the first result for compatibility. Its
`batch.records` array carries the unique `client_sequence`, partition, exact
decimal offset, and disposition for each input. Re-run the same bytes and key
to resolve an unknown outcome; changing the frame or metadata under that key is
an idempotency conflict. The regional path is the same operation at
`.../shards/{shard}/data/records/batches` with the normal authorization,
generation, tablet-epoch, and leader fences. Its response replaces the inner
physical partition 0 with the outer logical shard, including every correlated
batch record receipt.

### Commit, reset, and replay a consumer-group checkpoint

Use the observed leader port and term. An offset is the next record the group
will fetch:

```shell
curl --fail --silent --show-error \
  --request PUT \
  --header 'content-type: application/json' \
  --data '{
    "idempotency_key":"billing-checkpoint-1",
    "expected_term":"1",
    "member_id":"worker-a",
    "group_generation":"1",
    "partition":0,
    "next_offset":"2",
    "mode":"commit"
  }' \
  http://127.0.0.1:17701/experimental/v1/tablets/stream/groups/billing/offsets
```

`outcome: applied` means the checkpoint and owner were changed by the committed
command. `outcome: rejected` plus `owner_mismatch`, `stale_generation`,
`generation_gap`, `commit_rewind`, `offset_before_retained`,
`offset_beyond_end`, or `group_capacity_reached` is also a definite committed
business outcome. Reusing the exact idempotency key returns the original
receipt as `replayed`; changing any client-controlled field conflicts.

Only `reset` may rewind. A new owner must advance the generation by exactly one:

```shell
curl --fail --silent --show-error \
  --request PUT \
  --header 'content-type: application/json' \
  --data '{
    "idempotency_key":"billing-reset-1",
    "expected_term":"1",
    "member_id":"worker-b",
    "group_generation":"2",
    "partition":0,
    "next_offset":"0",
    "mode":"reset"
  }' \
  http://127.0.0.1:17701/experimental/v1/tablets/stream/groups/billing/offsets
```

Observe lag and fetch replay records beginning at the durable next offset:

```text
GET /experimental/v1/tablets/stream/groups/billing/lag?partition=0
GET /experimental/v1/tablets/stream/groups/billing/records?partition=0&limit=100
```

The generic regional equivalents are
`.../shards/{shard}/data/groups/billing/{lag|records}` and
`.../shards/{shard}/data/groups/billing/offsets`. The application-facing v1
equivalents remove the generic `data` segment beneath the fully qualified
`/v1/organizations/.../namespaces/.../streams/.../shards/{shard}` base.
Regional reads retain the normal safe ReadIndex default; direct reads remain
local and stale-capable. The repository's standalone offset helpers keep their
local contract. The separate Go, Java, and Python `RegionalStreamClient`
explicitly opts into replicated member/generation fencing and linearizable
reads.

### Coordinate a multi-shard consumer session

Session operations are accepted only by the service bound to logical shard 0:

```text
POST   /experimental/v1/tablets/stream/groups/billing/sessions
GET    /experimental/v1/tablets/stream/groups/billing/sessions
PUT    /experimental/v1/tablets/stream/groups/billing/sessions/worker-a/heartbeat
DELETE /experimental/v1/tablets/stream/groups/billing/sessions/worker-a
POST   /experimental/v1/tablets/stream/groups/billing/sessions/maintenance
```

Join supplies `idempotency_key`, `expected_term`, `member_id`, and decimal
`session_timeout_ms`. Heartbeat and leave supply the idempotency key, term, and
decimal `group_generation`; the member comes from the encoded path. Maintenance
supplies only the mutation identity and term. Receipts include the operation,
captured shard count, current generation and watermark, complete member plan,
the caller's assignment, expired members, and typed applied/rejected outcome.

The authenticated regional v1 route exposes the same suffixes beneath
`.../streams/{name}/shards/0`; its observation uses a leader ReadIndex barrier.
Go, Java, and Python clients always select shard 0 automatically. Assignment
does not automatically transfer or atomically commit each independent shard's
v3 offset checkpoint.

### Configure and maintain retention

Configure any combination of time, canonical bytes, and record count on the
current leader:

```shell
curl --fail-with-body --request PUT \
  --header 'content-type: application/json' \
  --data '{
    "idempotency_key":"orders-retention-v1",
    "expected_term":"1",
    "max_records_per_partition":10000,
    "max_bytes_per_partition":"3145728",
    "max_age_ms":"604800000"
  }' \
  http://127.0.0.1:17701/experimental/v1/tablets/stream/retention
```

An append or configuration evaluates the policy immediately. Commit
maintenance to advance age expiry while the Stream is idle, then inspect the
effective policy, watermark, base/end offsets, retained records, and canonical
bytes:

```text
POST /experimental/v1/tablets/stream/retention/maintenance
GET  /experimental/v1/tablets/stream/retention
```

The maintenance body contains `idempotency_key` and `expected_term`. The
authenticated regional v1 route exposes the same `retention` and
`retention/maintenance` suffixes; the regional GET uses the normal linearizable
ReadIndex contract. See [ADR-0023](adr/0023-stream-retention-policies.md) for
the byte definition, inclusive age boundary, recovery rules, and non-claims.

Leader-local committed reads use:

```text
GET /experimental/v1/tablets/stream/records?offset=0&limit=100
```

Direct profile reads are explicitly
`local_profile_applied_stale_capable`. The regional
`.../shards/{shard}/data/records` boundary now defaults to a safe leader
ReadIndex barrier and reports exact term/read/applied evidence; an explicit
`x-epoch-read-consistency: local_stale` request preserves this direct local
contract. Status exposes `last_profile_mutation_index`, the Raft index
of the latest unique typed command reflected in the Stream. It is not a Raft
applied watermark: election no-ops can make `consensus_applied_index` advance
without changing it.

## Deliberate non-claims

This mode is on the dedicated experimental listener. It has no CORS, TLS,
authentication, authorization, SDK commitment, public compatibility promise,
or multi-tenant isolation. Its fixed-voter evidence assumes the three configured
peer endpoints are isolated and trusted; an unauthenticated peer can spoof a
voter, so this is not durable-majority proof under a hostile network. Do not
expose it to an untrusted network.

It also has static Raft membership, one consensus group per logical partition,
no automatic checkpoint schedule on the direct profile route (the regional
wrapper now schedules local voter checkpoints),
user-exportable backup/PITR, follower
read routing, catalog-authorized epoch
transition, placement, authenticated peer identity, bounded idempotency
retention, replica-progress/ISR contract, or exhaustive crash/I/O matrix.
Consumer sessions provide replicated join, heartbeat, leave,
dead-member expiry, automatic membership generations, and deterministic
resource-wide assignment. The regional runtime schedules expiry through the
shard-zero leader; the direct route remains explicit. They do not add server-push
assignment, cooperative revoke acknowledgement, sticky/rack-aware strategies,
streaming fetch, atomic checkpoint handoff, transactional offset commit, or
exactly-once processing.
Retention does not add keyed compaction, tombstones, legal hold, object-tier
deletion, namespace policy guardrails, or a resource-wide policy coordinator.
The regional Stream v1 SDK exposes both the per-shard checkpoint primitive and
the shard-zero session coordinator, but their generations remain separate
fences rather than one atomic cross-shard protocol.
The batch route is whole-command atomic and client-framed; it does not provide
the future bidirectional Produce stream, connection credit, automatic producer
batching, codec negotiation, non-atomic per-record rejection, compression
dictionary management, a stable SDK, or matched throughput/latency evidence.
The three local voters are not placement or zone evidence, so the typed receipt
uses `write_evidence: fixed_voter_majority_persisted` and
`durable_voter_acks: 2`; it deliberately does not report the PRD's
zone-aware `quorum_durable` profile. A deterministic profile-application error
fails the actor, drains both HTTP listeners, and exits the process.

The public port `7601` remains the standalone API and still rejects
`quorum_durable`. Its health response remains capped at `local_durable`. This
experimental milestone therefore advances the replicated core without turning
partial evidence into a production claim.

See [Architecture](ARCHITECTURE.md), [Semantics](SEMANTICS.md),
[API contracts](API_CONTRACTS.md), and the
[Consensus feasibility spike](CONSENSUS_SPIKE.md) for the surrounding contract.
Checkpoint details are recorded in
[ADR-0016](adr/0016-stream-consumer-group-checkpoints.md); versioned route and
SDK behavior are recorded in
[ADR-0017](adr/0017-regional-stream-v1-and-sdk-routing.md), and session
coordination is recorded in
[ADR-0025](adr/0025-stream-consumer-sessions.md).
