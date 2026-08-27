# Testing Strategy

Epoch's guarantees are product behavior, so tests must prove semantics under
failure, not merely exercise successful API calls. This document defines the
test layers and the evidence required as the implementation grows.

Test construction and definition-of-done rules are mandatory and documented in
[ENGINEERING_STANDARDS.md](ENGINEERING_STANDARDS.md). Bug fixes begin with a
reproducing failing test; new behavior follows red → green → refactor.

## Local gate

Run before sending a change for review:

```shell
make check
```

It performs generated-code freshness, formatting, static analysis, Rust
documentation checks, unit tests for every language area currently present,
and the Rust and npm dependency advisory gates. The extended deterministic
local gate is:

```shell
make ci
```

`make ci` additionally validates pinned toolchains, builds all components, and
validates the Compose model. Long-running compatibility, fuzz, simulation,
chaos, soak, and performance suites remain separate so the fast gate stays
useful.

### Dependency advisory gates

Both pull-request CI and `make audit` install or require `cargo-audit` 0.22.2
and run:

```shell
cargo audit --deny warnings --ignore RUSTSEC-2025-0057
```

All reported Rust advisories and audit warnings fail the gate except
`RUSTSEC-2025-0057`. That single temporary exception is the unmaintained
`fxhash` dependency inherited through `raft`; it is not a vulnerability waiver
or an acceptance of the adapter. ADR-0003 remains Proposed until the dependency
decision and replacement path are reviewed. No additional advisory may be
ignored without a documented review and a bounded removal condition.

The console CI job and `make audit` also run `pnpm audit` against the frozen
workspace lockfile. Every npm advisory fails the gate; patched transitive
versions are pinned through root workspace overrides when an owning direct
dependency has not yet refreshed its own lock resolution. No npm advisory is
silenced or accepted without a documented, bounded exception.

The Rust CI job also tests and builds the complete workspace with
`--all-features`, and builds workspace documentation with
`RUSTDOCFLAGS="-D warnings"`. It installs `protoc` 35.1 through the repository's
Linux installer before any Rust compilation and verifies the exact
`libprotoc 35.1` output.

## Test layers

### 1. Unit and property tests

Unit tests live beside the code they cover. They must be deterministic and must
not require Docker, external networks, wall-clock sleeps, or fixed host ports.

Use property tests for:

- persisted frame encode/decode and checksum behavior;
- log append, truncation, recovery, and compaction;
- TTL, delay, lease, retry, and scheduling state transitions;
- queue ready/in-flight/ack/dead-letter indexes;
- stream offset and transaction visibility;
- cache eviction and shard-local atomic operations;
- routing filters and protocol-envelope translation.

Persisted formats require golden vectors that include the format version,
endianness, malformed lengths, checksum failures, unknown fields, and prior
supported versions.

### 2. Deterministic simulation

The simulator owns virtual network, disk, process lifecycle, monotonic time, and
wall-clock observations. Every run accepts and prints a seed. A failing seed is
an artifact and a permanent regression test until the underlying state space is
covered another way.

Simulation injects:

- reordered, duplicated, delayed, and dropped messages;
- one-way and complete network partitions;
- process crash at every persistence boundary;
- partial writes, fsync failure, disk-full, and checksum corruption;
- leader transfer, stale epochs, replica repair, and membership changes;
- monotonic advancement and wall-clock jumps;
- concurrent retry, lease expiry, acknowledgement, and cancellation.

Do not use `sleep` to make a distributed assertion pass. Advance the injected
clock or wait on an observable state transition with a bounded deadline.

The current `epoch-testkit` foundation provides a stable SplitMix64-seeded
scheduler, independent virtual wall/monotonic time, occurrence-indexed crash,
I/O, partial-write, drop, delay, and duplicate actions, directed and
bidirectional peer partitions, and a strictly decoded EPTR v1 trace with a
stable history digest. Golden tests pin the random sequence, generic trace
framing, and one complete seeded transport history. A failing run must retain
its seed and fault plan alongside the trace: EPTR does not yet encode an
executable replay bundle. This is the simulation kernel only; persistent
consensus storage, process lifecycle, and profile state machines must still
publish invariant histories before the simulator or emulator requirement is
complete.

The Stage 1 `epoch-consensus` harness now adopts the peer-transport portion of
that kernel. A fixed seed and canonical trace cover three fixed voters under
directed partitions, delayed/reordered delivery, duplicate delivery, election,
majority-only commit, old-leader replacement, catch-up, and leader transfer.
Full applied histories and SHA-256 state digests are compared; proposal tests
cover restart reconstruction, overwrite, exact duplicate application,
conflicting payload reuse, and corrupt restart images. The bounded peer-frame
suite covers destination, membership, canonical encoding, corruption, local
message classes, canonical EPSN snapshots, foreign group/epoch/voter/term
fences, and maximum size.

The distributed fault histories still use `MemStorage` and graceful in-process
restart images. A separate EPRS v1 suite exercises the local fsync-backed
stable journal's exact identity bytes, create/reopen, immutable identity,
writer exclusion, `HardState`/entry replay, uncommitted-suffix replacement,
partial-tail repair, corruption rejection, and safety regressions. This is not
yet an exhaustive injected-I/O or real-process-crash matrix. Persistent adapter
tests additionally reopen a three-voter committed history, preserve an isolated
pending proposal, verify stable-barrier message ordering, recover after an
injected post-append error, and publish a commit-ahead-of-checkpoint receipt
exactly once. The checkpoint corpus adds pinned canonical EPSN v1/v2 digests,
malformed/oversize rejection, fsync-before-memory failure recovery,
checkpoint-plus-tail reopen, bounded exact-retry preservation and truthful
expiry, physical WAL replacement/reopen histories, lagging-follower
installation, and post-restart election/commit/read-barrier behavior.

The explicit `test-consensus-process` gate extends that evidence across real
process boundaries. A parent harness starts three child test executables, each
with its own EPRS journal and loopback control socket, and routes their bounded
peer frames through the deterministic `PeerTransport`. It isolates a leader,
proves that a minority proposal remains pending without a receipt, heals the
partition, compares the committed receipt, payload, and state digest at all
three voters, sends `SIGKILL` to one child and then all children, and reopens the
same journal paths without republishing duplicate receipts. Failure retains the
seed, EPTR trace, child logs, and data directories; CI uploads those artifacts.
This is process-crash evidence for the isolated adapter, not node-to-node HTTP
transport, product profile replication, or a complete crash-point matrix.
The complementary `three_probe_runtimes_elect_and_commit_over_real_http` test
starts three persistent probe runtimes with ephemeral loopback listeners,
elects through the runtime probe HTTP transport, commits opaque bytes, creates
a checkpoint through the experimental route, and verifies the reported
compacted range. `lagging_profile_voter_replays_a_checkpoint_before_applying_its_tail`
proves typed Catalog snapshot installation and tail convergence after a
listener outage. Profile-specific core, tablet, and node tests cover canonical
capture/install for Catalog, Stream, Queue, Cache, and Event Bus; every real
three-voter profile restart now forces a native checkpoint before shutdown and
proves automatic restoration before readiness. These cover runtime transport
but not
separate process loss; the two test layers intentionally prove different
boundaries. `sustained_minority_outage_drops_only_that_peers_frames_and_majority_commits`
uses a one-frame outbound queue, keeps the lower-ID destination unavailable
until drops and exhausted retries are observed, and proves eight subsequent
proposals still commit on the healthy majority.

The `test-consensus-probe` container gate now adds the process boundary: it
stops one follower, checkpoints the live leader through HTTP, verifies the
reported compacted range, commits a tail on the majority, restarts the follower,
requires the follower's installed checkpoint index and tail lookup to converge,
then performs the existing active-leader replacement and final all-voter
comparison.

The typed Stream-tablet layer adds bounded durable fixed-voter-majority and
deterministic profile-application evidence for one fixed group and partition.
Its batch corpus pins legacy v1 plus additive v2 golden commands, all required
codec round trips, strict base64/JSON/count/size/sequence validation, bounded
decompression, an 8 MiB Zstd window, atomic cloned apply, correlated offsets,
exact retry, and unchanged v1 digests. The real-runtime test commits and reopens
every codec through three EPRS-backed voters. Its internal voter-recovery image
is now covered by the shared native-checkpoint suite; it does not add a
user-exportable Stream backup, membership-transition, authoritative
epoch-fencing, public product
acknowledgement, stable streaming Produce, or complete G3 evidence. Those
scenarios remain required before G3 or the emulator is complete; see
[Consensus Feasibility Spike](CONSENSUS_SPIKE.md).

The Stream consumer-group corpus adds canonical command v3, next-offset
commit/reset, exact replay, generation gaps, wrong/stale owner fencing, rewind
and end-offset rejection, lag/replay reads, state/digest reconstruction, and
unchanged v1/v2 compatibility. The real-runtime test applies accepted and
rejected outcomes across three voters and reopens every EPRS history. This v3
corpus proves replicated checkpoints independently of the later v5
join/heartbeat/assignment corpus; neither is a complete production coordinator
fault matrix.

The Stream-retention corpus adds canonical command v4 while pinning v1/v2/v3
bytes and operation kinds. Core tests cover the inclusive time boundary,
canonical retained bytes, oldest-first combined policies, oversized-record
rollback, dedupe reclamation, monotonic time, stale checkpoint reporting, and
snapshot validation. Tablet tests pin exact v4 bytes and deterministic digests;
the node test commits through three real EPRS-backed voters, advances explicit
maintenance, compares every retained boundary, checkpoints, reopens all voter
paths, and verifies the same policy/base/end/watermark before readiness. SDK
contract suites cover strict configure/maintain/observe requests in Go, Java,
and Python. The regional container campaign runs the Python path and verifies
the retained range through leader loss and same-volume recovery. Leader-owned
automatic retention is covered separately; keyed compaction is now part of the
v7 state-services corpus below. Production scale/fault evidence remains open.

The multi-shard Stream corpus pins `fnv1a64_utf8_mod_n_v1` with the same ASCII
and non-ASCII vectors in Rust, Go, Java, and Python, including empty-key
event-ID fallback and zero-shard rejection. Materializer/router tests create
three tablets, advertise the complete resource shard count, externalize logical
partition identities, and reopen expanded catalog state without changing
canonical Stream scope or snapshot bytes. SDK tests prove target-shard
selection and a generation-change failure before any write. The regional
container campaign creates a three-shard Stream, routes real Python keyed
appends to shards 0, 1, and 2, checks logical receipts/records/checkpoints, then
proves per-shard convergence after leader loss, voter return, all-node
`SIGKILL`, and same-volume reopen. Catalog/materializer tests now cover safe
expand-only allocation and reopen. Split/merge, hot-key, and cross-shard
transaction campaigns remain open.

The consumer-session corpus pins canonical command v5 while retaining v1–v4
goldens and validates join/rejoin, fenced heartbeat/leave, lexical round-robin
assignment, bounds, monotonic time, inclusive expiry, multi-member single-bump
rebalance, checked deadline overflow without phantom state, exact retry, legacy
snapshot v1 restore, and snapshot v2 recovery.

The regional Stream batch SDK corpus builds identical canonical record bytes in
Go, Java, and Python, including Unicode and sorted nested maps; rejects empty,
duplicate-sequence, unsupported-codec, inconsistent-size, and oversized input
before network I/O; and preserves exact frame plus idempotency key across one
leader rediscovery. Exact published sources compile in all three languages.
The Python regional campaign sends a two-record gzip batch after active Stream
leader loss, checks logical-partition correlated receipts and exact replay,
then fetches the same ordered offsets after old-voter catch-up and all-node
same-volume reopen. Rust remains the authoritative decoder for every frame.
A real three-voter node test compares all member plans, installs a native
checkpoint, and reopens every voter. Go, Java, and Python contract tests prove
shard-zero routing, encoded identifiers, decimal generations, whole-millisecond
timeout bounds, and linearizable observation. The regional container campaign
creates two members after Stream leader loss, heartbeats one, expires the other,
checks the generation-3 all-shard assignment, catches up the old voter, then
reopens every node and observes the same session. The regional campaign now
waits for the shard-zero leader to propose expiry instead of calling
maintenance. This does not prove cooperative revoke, atomic per-shard offset handoff,
cooperative revoke, scale fairness, or a production fault matrix. Same-tablet
transactions and bounded long polling are covered by the v7 corpus below.

The Stream state-services corpus exercises command v7 and snapshot v4 across
the core, tablet, HTTP adapter, three real EPRS-backed voters, and Go/Java/Python
clients. It covers producer gaps/conflicts/fencing/exact retry; transaction
visibility, abort, atomic offset commit, and pending-capture barriers; sparse
key compaction and tombstone expiry; immutable range/checksum/corruption/tier
reads; manual and scheduled JSON Lines/Array capture; source checkpoint mapping
and loop rejection; partition advice; push/dedicated long-poll wakeup; and
deterministic non-atomic superstream merge. Snapshot encoding validates the
advanced state against the ordered log before install and after decode. The
real-voter campaign checkpoints and reopens the complete history. External
object-store outage/latency, two-region RPO/RTO, and dedicated-bandwidth
benchmarks remain separate production campaigns.

The Docker regional campaign additionally drives the Python SDK through a
dedicated advanced Stream after process-level leader loss. It commits and
replays a typed state rejection, completes and aborts transactions, validates
both isolation levels, captures automatically through leader maintenance,
compacts and tiers history, applies and retries replication ingress, performs
long-poll and superstream reads, catches up a stopped voter, then SIGKILLs and
reopens all three voters before repeating the linearizable observations.

Regional adapter tests cover the fully qualified Stream v1 route and its strict
authorization actions/scope. A handler-level regression sends a group lag read
through both router layers: it first reproduced a real HTTP 500 caused by
cached outer path parameters contaminating the inner `Path<String>` extractor,
then proves the adapter clears that routing metadata while preserving the
intentional read-barrier extension.

The Queue-tablet layers check strict canonical commands, three-instance
convergence, leader/consumer/token fencing, exact renewed-token replay,
monotonic committed-order time including descending leader assignments,
exclusive deadlines, rejection rollback, non-zero jitter, TTL/max-age
precedence, credit/window saturation, consumer isolation, settlement
replenishment, immutable DLQ/redrive provenance, browser-safe receipts, and
pinned proposal/command/digest vectors. Advanced suites add exact count/byte
overflow, dedupe-before-eviction, durable idle expiry, session FIFO/renewal and
fencing, priority aging, rate/burst/concurrency/circuit transitions, defer and
correlation snapshot recovery, and durable outbox binding/completion. A real
three-runtime test drives every Queue operation plus flow-controlled receive
and consumer-flow observation through typed HTTP and EPRS reopen. The container
gate adds bounded credit evidence, scheduled eligibility, follower rejection,
leader `SIGKILL`, old-term-token fencing, session/deferred/correlation behavior,
Queue-to-Queue DLQ forwarding, redelivery, DLQ/redrive reads, convergence, and
all-node `SIGKILL` replay. This is bounded fixed-voter evidence, not a native
streaming, production fairness/load, complete crash/I/O-fault, or production
placement matrix. See [Experimental Replicated Queue Tablet](QUEUE_TABLET.md).

The Cache suite checks pure observations, checked shard revisions and
non-repeating item versions, distinct-key transaction bounds, absent-state ABA
protection, atomic rollback on version/type/counter/deadline/revision/capacity
failure, deterministic expiry, committed-access exact replay, deterministic
all-key/volatile LRU, LFU, random, and TTL eviction, snapshot compatibility,
memory/cold byte admission, collection transforms, bitmap/cardinality,
Bloom/Cuckoo filters, geo, JSON pointer/secondary-index, exact vector hybrid
search, durable change retention, canonical backup/PITR/corruption rejection,
non-ABA restore, advisory lock contention,
token rotation, active-owner epoch and cross-entry-term fencing, bounded
owner-history reclamation, guarded writes, descending candidate-time clamping,
exact replay, and independent-tablet digest convergence. Node-service tests add
strict recursive HTTP decoding, decimal 64-bit boundaries, fail-stop recovery,
real three-runtime majority application, and EPRS reopen. The container gate
adds follower/stale-term rejection, exact retry/conflict, CAS/atomic-batch
rollback, independently correlated multiplex replay, committed LRU access and
entry/byte eviction, fsynced cold write/read/removal, advanced typed queries,
backup/PITR, change cursors, at-most-once Pub/Sub, TTL maintenance, lock fencing
across leader replacement, catch-up,
convergence, and all-node `SIGKILL` replay. A concurrent linearizability history,
read barrier, I/O-fault matrix, and production placement proof remain required;
see [Experimental Replicated Cache Tablet](CACHE_TABLET.md).

The Event Bus suite checks a route truth table across event type, source,
subject, headers, JSON equality, Unicode wildcard matching, deterministic
lexical fan-out and transformation. Boundary tests cover strict configuration,
filter, JSON path, resource, and HTTP target validation; subscription/archive
capacity; replay bounds; and atomic route-plan/publish counter exhaustion.
Tablet tests add canonical command decoding, scoped proposal identity, exact
replay, conflicting and out-of-order commit rejection, recordable capacity
failure, atomic lexical outbox creation, term/dispatcher fencing, independent
target failure, retry eligibility, attempt exhaustion, dead-letter state,
committed rate/burst, redrive/terminal retention, archive retention, long-poll
wakeup/timeout, bounded lease-expiry maintenance, complete business-state digests, and
three-independent-tablet convergence. Node tests add strict recursive DTOs,
browser-safe attempt history, semantic retry/conflict, fail-stop behavior,
committed acquire/ack, bounded delivery queries, three real HTTP runtimes, and
EPRS reopen. Signed-delivery tests add deterministic oldest-candidate and exact
acquire ordering, v1/v2 compatibility, terminal rejection, RFC 4231 and one
cross-language HMAC vector, CloudEvents binary headers, exact raw body,
strict/redacted key files, invalid-header rejection, response classification,
special-address and mixed-DNS denial, DNS pinning, redirect/proxy suppression,
and a real loopback receiver. The real-process gate returns 503 then 204,
observes attempts 1/2 with stable identity and distinct signatures, converges
the failed/Ack history on all voters, and verifies it after all voters reopen.
Epoch-target tests add v3 command/snapshot compatibility, immutable destination
binding, public-acquire isolation, shared keyed Stream routing, stable
source/destination-scoped target identity, and internal proposal forwarding.
The same real-process gate writes one event to Queue and a keyed multi-shard
Stream through independently led groups, acknowledges both source records,
and verifies one destination record after every voter reopens.
Integration-platform tests cover schema formats/revisions/compatibility and
payload policies; bounded transform/lookup enrichment; connector partial
outcomes, crash-idempotent checkpoint replay and secret versions; MQTT
session/expiry/retained/QoS/shared routing; catalog search, endpoint failover,
and function revisions/status. Snapshot tests recompute a digest over malformed
integration state and still require semantic restore rejection; capacity tests
prove atomic rejection at 2 MiB and tablet mutations must remain snapshotable.
Managed-target tests cover binary/structured CloudEvents, API-key/bearer/OAuth
secrets, OAuth caching/response bounds, safe DNS-pinned loopback, allowlists,
stable idempotency, endpoint failure observation, connector checkpoint-before-
source-settlement, and status counters. Seven focused worker tests include a
real OAuth token and target receiver. The three-process campaign delivers a
structured managed event, converges Ack, reopens every voter, and proves full
state convergence.
Source-connector tests cover direction/status eligibility, exact cursor
progression, duplicate/oversized/malformed batch rejection, stable record
proposal identities, real loopback headers/authentication, error routing,
connector fairness, topology counters, and a real three-process checkpoint and
all-voter reopen history. Deterministic adapter suites cover immutable-object
ordering/overwrite detection and cursors, PostgreSQL transaction/LSN assembly,
MySQL transaction/binlog positions, Kafka partition offsets/error routing,
credential and transport policy, crash-before-upstream-ack reconciliation, and
session cleanup. The pinned connector-conformance Compose stack then runs
MinIO/S3, PostgreSQL 17 logical replication, MySQL 8 row binlogs, and Kafka 4
against those adapters.
The container gate adds follower rejection,
majority-before-success, acquire/ack replication, leader loss, catch-up,
archive/outbox/digest agreement, all-node `SIGKILL`, and same-volume recovery.
Schema tests compile Avro, JSON Schema, and self-contained Protobuf definitions,
reject malformed inputs, derive adjacent-revision compatibility, select an
explicit Protobuf root message, separate producer advice from broker
enforcement, mask payload values, and recover the same registry and policies
through a real three-voter route. External JSON Schema references and Protobuf
imports are intentionally rejected. An MQTT wire/protocol corpus, official
CloudEvents conformance, live Azure/GCS cloud campaigns, unsigned legacy target
execution, sustained adapter load/soak, and a broader crash-at-every-network-
boundary matrix remain open; see [Experimental Replicated Event Bus Tablet](BUS_TABLET.md).

The `epoch-catalog` unit suite begins at the regional multi-tablet boundary. It
proves collision-free identity across resources and shards, stable existing
routes during expansion, monotonic generation fencing, delete/recreate without
tablet ID reuse, exact idempotency replay and token-rebinding rejection,
profile immutability, resource-kind/profile compatibility, strict name and
capacity bounds, canonical versioned command decoding, and identical snapshots
after command replay.

Node integration tests extend that state machine through dedicated catalog
consensus, shared peer-frame group/epoch demultiplexing, bounded multi-group
supervision, catalog-driven typed tablet materialization, and resource/shard
routing. `regional_process` additionally launches signed-webhook, Epoch-target,
and managed-target workers on every real process, retries through real receivers,
forwards Queue and Stream writes to their independently elected leaders, and
proves converged acknowledgement plus duplicate-free destination state after
all three storage directories reopen. The suites reject unknown group/epoch,
stale generation, stale tablet epoch,
nonleader writes, wrong profile dispatch, missing routes, and inconsistent
materialization. `regional_process` starts three actual `epoch-node` binaries,
creates all four profiles in distinct groups, commits through each leader,
kills and replaces a data leader, catches it up, kills all three processes, and
reopens the same directories before comparing catalog and profile digests.
It also performs a default regional leader read, requires safe ReadIndex
headers/JSON evidence, and uses explicit `local_stale` only for all-follower
convergence polling. Consensus tests separately prove no barrier completion on
an isolated leader, real-HTTP timeout without a majority, follower/stale-term
rejection, and cancellation. This proves the current fixed-voter topology; it
does not prove dynamic membership, follower routing, exhaustive
linearizability, or exhaustive crash/I/O faults.

### 3. Integration tests

Integration tests start real Epoch processes with isolated temporary data
directories and allocated ports. They cover:

- standalone startup, health, drain, shutdown, and restart;
- a three-node cluster, election, replication, and quorum loss;
- committed write recovery from snapshot plus log tail;
- queue lease, redelivery, retry, schedule, dead letter, and redrive;
- stream append, fetch, offset commit, rewind, consumer sessions, and retention;
- Stream session-fenced per-shard claim, exact-member/generation bounded fetch,
  stale-member rejection, offset preservation, coordinator revalidation, and
  claim recovery across leader loss, voter catch-up, and all-node reopen;
- cross-language calls over generated Protobuf contracts;
- OpenTelemetry/metrics and immutable audit event emission.

Tests must always capture node configuration, logs, seed, process exit status,
and relevant data manifests on failure. Payloads and credentials are redacted.

The current fast integration smoke starts real Rust and Go processes, exercises
all four standalone profile APIs through the Python SDK, validates the Go and Java SDKs
against the same node, restarts the Rust process, proves local-durable Stream
and Queue state survived, and proves volatile resources did not. The node HTTP
suite forces a small segment threshold, verifies physical rotation, restarts,
and checks Stream and Queue state across multiple files. Storage tests verify
one global sequence across segment boundaries and exclusive writer ownership.
Fresh directories activate through a staging and then active marker at
`engine.wal`; direct tests cover rejection by the older single-file reader and
recovery from a torn staging marker. Relative-root and nested-directory tests
cover durable parent selection and component-wise creation; the node does not
pre-create the data root outside the storage boundary. A separate downgrade
test verifies that an existing valid `engine.wal` remains the only journal,
receives new appends through the legacy writer, and does not create
`engine-wal/`.

Run the persistent consensus process smoke directly with:

```shell
make test-consensus-process
make test-consensus-probe
make test-stream-tablet
make test-queue-tablet
make test-cache-tablet
make test-bus-tablet
make test-regional-runtime
```

`test-consensus-process` is ignored by Cargo's default suite so it cannot run
accidentally as a unit test, while the Make target, extended local integration
gate, and GitHub Actions Rust job select it explicitly.

`test-consensus-probe` builds a single node image, starts three containers with
independent EPRS volumes and dynamically allocated loopback ports, verifies the
truthful experimental status contract, commits an opaque proposal, stops the
leader, observes a higher-term election and majority commit, restarts the old
leader, and waits for identical local lookup at all voters. The script uses a
unique Compose project and deletes only its ephemeral containers, network, and
volumes. On CI failure it retains container logs, state, and port assignments as
an uploaded artifact.

`test-stream-tablet` selects the mutually exclusive typed mode on the same
three-voter runtime. It verifies a follower error, success only after majority
commit and local application, `fixed_voter_majority_persisted` with two durable
voters, ordered Stream offsets, exact retry, and changed-input conflict
behavior. It isolates an old leader and proves no committed response, then
commits different input under the same deterministic ID on a higher-term
majority and proves the original input conflicts rather than receiving that
receipt. After failover it submits a Python-standard-library gzip frame through
the v2 batch route, verifies exact per-sequence offsets, retry and conflict, and
checks the advertised codec/limit status. It verifies old-voter catch-up, sends
`SIGKILL` to all three
containers. The same campaign commits a consumer checkpoint, retries it,
commits wrong-owner and stale-generation rejections, advances ownership,
explicitly resets, and compares lag plus checkpoint-based replay at every
voter. It then reopens the same EPRS volumes, compares every record,
checkpoint/lag/replay observation, and profile digest with the pre-crash state
across every voter, and proves a retry still resolves to the original offset.
On failure, CI retains the scoped logs, port map, and state volumes as an
artifact. The process still starts its empty
standalone engine for the separate public API,
but typed commands are never appended to that engine journal. Unit/runtime
tests additionally prove strict command/request decoding, every none/gzip/LZ4/
Snappy/Zstd path, decompression-bomb rejection, atomic batch visibility,
browser-safe 64-bit identity/position/time encoding, generation fencing,
commit/reset range rules, and committed business rejection,
actor-only application, and process supervision after an injected live profile
apply failure.

`test-cache-tablet` selects the third typed mode. It exercises strict decimal
inputs, follower and stale-term admission, exact retry and rebinding conflict,
CAS including missing-state ABA protection, atomic transaction rollback,
checked increment, TTL plus committed maintenance, and advisory lock token
rotation/fencing. It kills the leader, proves the replacement term rejects the
old lock token as a committed outcome, catches the old voter up, compares every
profile digest/observation, then kills and reopens all voters from their EPRS
volumes. Reads remain explicitly local and stale-capable throughout.

`test-bus-tablet` selects the fourth typed mode. It rejects a follower write,
commits a subscription and publish only through the leader majority, checks
exact retry and changed-input conflict, compares route/archive counters and
state digests on all voters, performs browser-safe filtered archive replay,
kills the leader, commits through its replacement, catches the old voter up,
then kills and reopens all voters from their EPRS volumes. Status must continue
to report a durable target outbox and that external target dispatch is not implemented.

`test-regional-runtime` starts three regional containers with independent EPRS
volumes and dynamic loopback ports, then builds and launches the real
`epoch-control` process against those nodes. One resource is accepted through
authenticated Go desired state and reconciled into the Rust catalog with a
separate control-workload credential; the browser BFF must return exact-origin
CORS, decimal-string 64-bit IDs, one leader, and three actually observed
voters. Before mutation, the campaign verifies three distinct configured
zones, identical peer-derived voter sets, and live group counts from the
authorization-protected Rust topology endpoints. It also requests more shards than the
limiting node can host, waits for the stable capacity reason, and proves the
catalog resource was never created. Every direct regional
catalog/route/data/topology request also authenticates.
The managed resource includes canonical governance. The campaign queries it by
owner, cost center, classification, and two exact tags, checks post-filter cost
attribution, compares the Go BFF value with every Rust catalog voter, and repeats
those checks after Go `SIGKILL` and all-node same-volume reopen.
The process campaign performs a linearizable-by-default Stream read on the
current leader and verifies the quorum barrier term, read index, applied index,
and response headers. Polls that intentionally compare every follower opt into
`local_stale`; no test depends on an implicit consistency downgrade.
It then sends `SIGKILL` to the Go process, reopens
the same bbolt metadata file, proves the original apply token replays without a
second Rust mutation, and waits for placement to reconcile ready again. The
campaign also creates Cache, Stream, Queue, and Event Bus resources directly
through the Rust catalog and commits one typed operation per profile. It kills
the managed Stream leader, waits for the Go BFF to report degraded two-voter
placement, commits through the replacement, restarts and catches up the old
voter. After leader loss, the real Python `RegionalStreamClient` uses all three
endpoints to append, repeat the exact idempotency key, perform a linearizable
fetch, commit a generation-fenced checkpoint, and verify lag. The campaign then
kills the active Queue leader and runs the real Python `RegionalQueueClient`
through enqueue/exact replay, credit acquire, lease extension, acknowledge,
release, automatic scheduled-retry promotion, reacquire, terminal reject,
immutable dead-letter observation, redrive, final settlement, and linearizable
counts/flow/redrive reads. It waits for 12
Queue commands to converge on the survivors and catches up the old voter. It
then kills the configured advanced Queue leader and exercises dedupe, ordinary
session exclusion, exclusive lock acquire/renew/release, commit-order session
delivery, correlation, defer/exact receive, terminal rejection, and durable
forwarding into a separate `failed-jobs` Queue. Both source/outbox and target
state must converge, catch up, and reopen from the same voter volumes.
It repeats the leader-loss proof with the real Python `RegionalCacheClient` across
strict/advanced values, CAS, committed LRU access, atomic batch and independent
multiplex, deterministic entry/byte capacity eviction, a real cold-file read,
backup/PITR, durable changes, explicitly lossy Pub/Sub, fenced locks, automatic
expiry, typed query, pure observation, and requested/achieved durability status.
Finally, the real Python `RegionalBusClient`
proves exact publish, archive replay, acquire/automatic-timeout/reacquire/ack,
acknowledged-delivery query, subscription removal, and linearizable status
after Event Bus leader loss. Before faulting the cluster, the campaign waits
for catalog plus all eight profile groups to checkpoint and physically compact
on every voter: 27 local voter/group copies. Each old voter catches up before
the campaign verifies durable applied/checkpoint/retained-first boundaries,
kills every node, verifies Go clears stale placement while authority is
unavailable, and reopens the same volumes before comparing those boundaries,
catalog/profile digests, and the increased applied-command index. CI captures control logs,
container logs, port assignments, and scoped state evidence on failure.

`tests/integration/docs-quickstarts.sh` separately executes the exact Go, Java,
and Python source imported into the documentation page. Each language gets a
fresh data directory, creates a local-durable Stream and Queue, acknowledges
only `job-1001`, kills the node with `SIGKILL`, restarts from the same bytes,
and proves that the Stream record, acknowledgement count, and only `job-1002`
survived. The GitHub Pages deploy job depends on this lifecycle test as well as
the documentation-only frontend build. The same script compiles the exact
displayed regional Stream, Queue, Cache, and Event Bus Go and Java programs and Python bytecode
against the current repository-local SDKs; the real regional Python executions remain in
`test-regional-runtime`. Pull-request runs execute both gates but cannot upload
or deploy Pages; publication is restricted to `main`.

Manifest recovery tests distinguish a safe uncommitted suffix from committed
data damage. Only bytes beyond the manifest's committed length in the active
segment are discarded. Missing or truncated committed segments, bytes appended
to a sealed segment, a content-checksum mismatch, a sequence/topology mismatch,
an untracked segment, missing identity or manifest metadata, and a foreign WAL
identity fail closed. Pending-rotation recovery can create a missing expected
target or adopt it only when empty; the direct unit test covers the missing-file
branch.

Current direct unit evidence includes:

- `segmented_wal_rotates_and_recovers_global_sequences` and
  `segmented_wal_rejects_a_second_writer`;
- `segmented_wal_cannot_rotate_past_a_poisoned_active_segment`, which forces an
  append and rollback failure and proves no later rotation or manifest change
  can bypass the terminal fault;
- `segmented_wal_repairs_only_the_active_tail`,
  `segmented_wal_discards_bytes_not_committed_by_the_manifest`, and
  `segmented_wal_rejects_a_partial_sealed_segment`;
- `segmented_wal_rejects_a_missing_committed_final_segment`,
  `segmented_wal_rejects_all_committed_segments_missing`,
  `segmented_wal_rejects_a_truncated_committed_active_segment`,
  `segmented_wal_rejects_a_missing_manifest_after_activation`, and
  `segmented_wal_rejects_a_missing_identity`;
- `segmented_wal_rejects_sequence_gaps_between_files`,
  `segmented_wal_rejects_a_foreign_manifest_identity`,
  `segmented_wal_rejects_valid_frames_that_do_not_match_the_manifest`, and
  `segmented_wal_rejects_checksum_corruption`;
- `segmented_wal_completes_a_manifested_pending_rotation`; and
- `standalone_wal_activation_blocks_single_file_writers`,
  `standalone_wal_resumes_a_torn_staging_marker`,
  `standalone_wal_rejects_a_missing_activated_segment_directory`,
  `standalone_wal_keeps_existing_legacy_history_downgrade_safe`, and
  `standalone_wal_rejects_ambiguous_legacy_and_segmented_histories`.

The Queue lifecycle test restarts across enqueue, acquire, extend, Ack, Reject,
redrive, and scheduled eligibility. Injected journal failures prove proposed
Queue enqueues and settlements never leak into live memory. Container CI mounts
the data directory into a named volume, asserts that a small configured rotation
threshold creates multiple files under `engine-wal/`, and repeats Stream and
Queue recovery after replacing the running container.

These standalone tests are segmented-journal evidence only. Snapshot restore,
compaction, retention deletion, and production placement-aware replica recovery
and quorum acknowledgement remain future gates; the bounded typed Stream/Queue/Cache/Bus
fixed-voter evidence is described separately above.

### 4. History and consistency checking

Concurrent test clients record invocation, response, timeout, resource epoch,
commit position, and observed value. Offline checkers validate:

- linearizability for supported cache/state operations;
- no successful quorum acknowledgement before durable majority commit;
- no queue deletion before committed acknowledgement state;
- no silently skipped committed eligible record on at-least-once paths;
- fencing of stale leaders, producers, consumers, and session owners;
- read-committed consumers never exposing aborted transaction records.

An unknown outcome after a client timeout is not treated as failure or success
without resolving its idempotency key or commit receipt.

### 5. Protocol compatibility

Compatibility claims name exact client and protocol versions. Differential
suites compare supported behavior with pinned reference containers for Redis,
Kafka, and RabbitMQ. Tests cover the public support matrix, error mapping,
metadata round trips, retries, backpressure, malformed frames, and lossy
translation disclosures.

Unsupported behavior must fail explicitly. A test that happens to pass outside
the published subset does not expand the compatibility promise.

### 6. Fuzzing and concurrency exploration

Fuzz all externally controlled parsers and stateful boundaries:

- RESP, Kafka, AMQP, MQTT, HTTP, and native frames;
- Protobuf/JSON envelopes, schemas, compression, and transformations;
- log frames, manifests, snapshots, and restore input;
- filter expressions, connector configuration, and webhook headers.

Run Loom-style concurrency tests for small synchronization components. Run Miri
for any crate granted an unsafe-code exception. Corpus and crash output are
local artifacts; minimized non-sensitive regressions belong in test fixtures.

### 7. Chaos, recovery, and soak

Chaos tests run against production-shaped multi-node deployments. Required
scenarios include node and zone loss during peak load, snapshot, compaction,
rebalance, repair, backup, restore, and mixed-version upgrade. Recovery tests
must prove data and index digests, not only service availability.

Soak duration grows with maturity: 30 days before private-alpha exit, then 60
and 90 day campaigns before stronger release claims. Every campaign records
configuration, build identity, workload, saturation level, injected faults,
and SLO distribution.

The alpha-exit branch implements the first resumable evidence runner in
`tests/soak/epoch_soak.py`. Its accelerated CI profile wraps the existing real
regional campaign once; its `thirty-day` profile accumulates only successful
active round time across exact-identity resumes. Atomic state, per-attempt
SHA-256 receipts, a canonical event log, explicit invariant receipts, and an
Ed25519-signed manifest make interruption and tampering visible. Campaign
runtime percentiles are not presented as request-latency SLOs. See
[Resumable load, fault, and soak evidence](SOAK_TESTING.md).

The first local accelerated campaign passed one exact-image round in 48,434 ms
with all seven named process faults and six required invariant receipts. Its
manifest explicitly marks itself accelerated-only and denies throughput,
latency, managed-service SLO, and production-certification claims. Protected CI
and the elapsed 30-day campaign remain required.

The separate live Kubernetes acceptance runner in
`tests/integration/kubernetes_alpha_exit.py` creates a pinned one-control-plane,
four-worker Kind cluster. One clean local run now passes mTLS install, all four
profiles, encrypted backup, compacted-log learner catch-up and voter
replacement, the post-request backup upgrade gate, serialized four-node
rollout, fresh-cluster restore, exact Catalog/profile digest comparison, and
post-restore writes. `tests/integration/test_kubernetes_alpha_exit.py` protects
the evidence schema, N-node placement planner, integer normalization, identity,
and fail-closed command contracts without requiring Docker. See
[Live Kubernetes alpha-exit campaign](KUBERNETES_ALPHA_EXIT.md).

### 8. Benchmarks

Performance results state hardware, payload sizes, concurrency, dataset size,
durability, replicas, acknowledgement mode, batching, compression, and failure
conditions. Report p50, p95, p99, p99.9, maximum, and a saturation curve.
Averages alone are not accepted.

Reference comparisons with Redis, Kafka, or RabbitMQ use matched semantics.
Volatile writes are not compared with quorum writes, and hot-tier results are
not blended with object-tier fetches. Benchmarks should remain non-blocking in
ordinary pull requests and enforce reviewed regression budgets on stable,
dedicated runners.

### 9. Security and tenancy

Security suites cover authorization across every protocol, tenant isolation,
policy-cache expiry, key rotation, audit integrity, payload redaction, webhook
SSRF, connector egress allowlists, decompression/schema bombs, credential
replay, resource exhaustion, and object-tier tampering.

The implemented security slices currently contribute:

- one Go/Rust decision corpus proving identical action/scope results;
- strict policy parsing, token-fingerprint uniqueness, bounded input, and
  credential-free debug/error tests in `control/internal/auth` and
  `epoch-auth`;
- Go HTTP and gRPC tests for missing/invalid authentication, action denial,
  mutation-before-authorization prevention, tenant-filtered collections,
  request IDs, exact-origin `Authorization` preflight, and bounded audit
  events;
- Rust regional route tests for catalog/route/data action separation, exact and
  cross-tenant scope, unauthenticated/forbidden responses, and a resource name
  that cannot confuse route versus data authorization;
- real Rust multiprocess and Go-to-Rust container recovery campaigns in which
  every regional request is authenticated; and
- signed-webhook tests for canonical HMAC and constant-time receiver helpers,
  key-file bounds/redaction, HTTPS-only operation, explicit loopback
  development, IPv4/IPv6 special-purpose and embedded-address rejection,
  mixed-answer DNS failure, per-attempt address pinning, disabled redirects and
  ambient proxies, invalid header values, lease-bounded timeout, and real
  retry/recovery; and
- managed-target tests for strict/redacted API-key/bearer/OAuth files, token
  response/expiry bounds, function/connector allowlists, shared SSRF controls,
  stable idempotency, endpoint health mutation, and checkpoint ordering; and
- Rust/Go transport tests for mandatory TLS inputs, trusted and untrusted
  client chains, hostname verification, secure listener startup, peer recovery,
  and the no-proxy/no-redirect client boundary; Go, Java, Python, and CLI tests
  for custom trust roots and optional client identities; and
- regional backup tests for canonical/bounded manifests, quorum barriers,
  Catalog-derived inventory, a seven-node layout with disjoint tablet leaders,
  exact three/five-voter membership, tamper/wrong-key rejection, no-overwrite
  publication, retention, non-empty-destination rejection, and encrypted
  all-profile restore/digest equivalence after a full reopen; and
- fake-Kubernetes operator tests for required TLS/backup Secret and RWX PVC
  validation, secure N-node rendering, idempotent backup CronJobs, strict
  termination receipts, failure-to-success status recovery, restore init
  containers, immutable restore references, post-request backup gating,
  all-ordinal partition freeze, preflight/drain/update/postflight ordering,
  exact-image pod readiness, and guarded rollback entry; and
- Rust maintenance tests for canonical all-node inventory, cluster-wide group
  coverage, stable three/five-voter membership, complete replication/apply
  progress, lag/joint-change rejection, deterministic transfer choice, HTTPS
  authority validation, and group-epoch/term fencing; and
- Catalog/consensus/regional membership tests for canonical v5 plan/finalize
  bytes, exact replay, stale/conflicting/multi-voter rejection, learner
  catch-up gating, joint-consensus persistence, current/target materialization,
  Go pending/ready status, and a four-node Stream replacement that preserves
  data, stops the removed host, and reopens on the new voter set; and
- console tests proving the managed credential stays session-scoped and that
  empty, whitespace-bearing, or oversized values are rejected; and
- OCI candidate checks that build node/control/operator/CLI/compatibility from pinned bases,
  reject root users, wrong entrypoints, source/version/revision/license label
  drift, and credential-shaped defaults; a scratch-fixture regression proves
  root identity and revision drift fail closed; and local evidence generates a
  nonempty SPDX JSON inventory for every built image.

This is not yet the cross-protocol production security suite: OIDC, expiry and
revocation, certificate issuance/automated rotation, certificate-role policy,
policy replication/cache expiry, immutable audit export,
network-enforced/private egress, secret-manager rotation, quotas, WAL/data
encryption and external KMS, protected live-Kubernetes evidence, mixed-version
upgrade/rollback, and abuse/load campaigns remain open.

Current dependency gates include Cargo advisory/license policy, Go and Python
vulnerability scanning, JavaScript dependency scanning, and secret scanning.
The release workflow additionally pins every action by full commit, builds and
inspects five images without publishing on pull requests, generates five SPDX
documents, and defines exact-main/tag-only
multi-architecture publication with native per-platform runners, digest-only
handoff, manifest provenance, per-platform SBOM attestations, and keyless
signing. An executable contract rejects QEMU, mutable tags, missing runner
bindings, or lost evidence gates. A disposable local registry proves that two
BuildKit provenance/SBOM-bearing platform outputs assemble into exactly one
amd64 and one arm64 runtime manifest. Protected pull-request/tag execution,
exact-main rerun, and clean published-digest pulls remain alpha-exit release
gates.

## Test organization

```text
crates/*/src/                    Rust unit tests close to implementation
crates/*/tests/                  Crate-level black-box tests
tests/integration/               Real-process and cross-language tests
tests/simulation/                Deterministic simulator scenarios and seeds
tests/compatibility/             Protocol/client conformance matrices
tests/chaos/                     Cluster fault campaigns
tests/benchmarks/                Reproducible workload drivers and baselines
tests/fixtures/                  Non-sensitive golden vectors and corpora
spec/models/                     Formal models and checked invariants
```

Large generated data, runtime volumes, payload captures, and benchmark results
are not committed. Small minimized fixtures that protect a correctness rule are
committed and documented.

## CI topology

| Pipeline | Trigger | Required work |
| --- | --- | --- |
| Fast | Every pull request | Format, lint, generation freshness, unit/property tests |
| Integration | Every pull request | Standalone, three-node process, connector, and live four-node Kubernetes tests |
| Simulation | Pull request seed sample; larger nightly matrix | Deterministic failure exploration |
| Compatibility | Nightly and release | Pinned Redis/Kafka/RabbitMQ client matrix |
| Security | Pull request and scheduled | Rust advisory gate; secrets, fuzz smoke, authorization as implemented |
| Performance | Nightly or scheduled dedicated runner | Baselines, saturation, regression analysis |
| Chaos/soak | Scheduled environments | Fault, upgrade, repair, restore, and long-duration evidence |
| Platform | Weekly and release | Linux amd64/arm64 and macOS arm64 smoke/release matrix |

Linux is the primary test platform. macOS validates developer workflows and
portable standalone behavior; release claims require the supported deployment
matrix, not only the author's workstation.

## Flake and timeout policy

- A failed correctness test blocks the change until explained.
- Retrying is diagnostic, not a way to turn red into green.
- A flaky test receives an owner and issue immediately; quarantine is bounded
  and visible, and it cannot hide a release invariant.
- Deadlines derive from explicit test configuration and emit state on expiry.
- Random tests always print their seed and use a stable seed in CI artifacts.
- Wall-clock-sensitive assertions use tolerances only where the public contract
  itself has a tolerance, such as scheduled-delivery eligibility.

## Pull-request evidence

A behavior change should identify:

1. the PRD requirement and architecture/semantics section it implements;
2. the test layer that proves the normal path;
3. the injected failure or boundary case that could falsify the guarantee;
4. compatibility, storage-format, rollout, and rollback impact;
5. benchmark evidence when the data path or resource accounting changes.

Passing tests are necessary but do not create a product claim by themselves.
Update the requirements traceability and compatibility matrices only when the
corresponding acceptance evidence is reproducible.

## Related documents

- [Development](DEVELOPMENT.md)
- [Architecture](ARCHITECTURE.md)
- [Requirements traceability](REQUIREMENTS_TRACEABILITY.md)
- [Product requirements](PRD.md)
