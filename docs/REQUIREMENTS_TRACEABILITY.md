# Epoch Requirements Traceability

This register turns the prioritized catalog in [PRD.md](./PRD.md) into a delivery and verification index. It is intentionally terse: the PRD remains the source of semantic detail, while this document owns milestone, dependency, status, and evidence tracking.

Last synchronized with PRD version 0.3 on 13 August 2026.

## How to use this register

Status values are:

- **Slice** — part of the foundational vertical slice. This means it is in the first implementation scope, not that its acceptance evidence already exists.
- **Planned** — assigned to a later milestone; implementation has not started.
- **Deferred** — intentionally outside the committed delivery milestones.

Evidence remains partial unless a row explicitly names a completed artifact.
Replace each remaining placeholder with a durable link to a test run,
model-check report, benchmark, drill record, conformance report, security
review, or release artifact. A feature is not complete merely because code
exists.

Milestones are:

| Code | Milestone | Window |
|---|---|---|
| M0 | Architecture and semantic freeze | Before implementation |
| M1 | Foundational vertical slice | Months 0–3 |
| M2 | Private alpha core | Months 4–8 |
| M3 | Private beta compatibility | Months 9–14 |
| M4 | Public beta managed service | Months 15–20 |
| M5 | Initial GA | Months 21–26 |
| M6 | North-star expansion | Months 27–36+ |
| D | Explicitly deferred | Dependency/customer threshold required |

Dependency gates are:

| Gate | Exit condition |
|---|---|
| G0 Semantics | Commit, ordering, lease, retry, expiry, transaction, compatibility, and error contracts are versioned. |
| G1 Foundation | Rust/Go workspaces, Protobuf boundary, common envelope, native API, deterministic test harness, and unsafe-code policy exist. |
| G2 Storage | Checksummed segmented log, recovery, snapshots, monotonic timers, and durable index rebuild pass fault tests. |
| G3 Consensus | Metadata consensus, quorum commit, epochs, fencing, placement, leader transfer, and repair pass model and chaos tests. |
| G4 Profile core | Cache, stream, queue, and routing state machines satisfy their native correctness suites. |
| G5 Trust | Identity, authorization, encryption, audit, quotas, redaction, and profile golden signals are enforced end to end. |
| G6 Compatibility | Named protocol/client versions pass conformance, differential, fuzz, and mixed-version tests. |
| G7 Data services | Transactions, schema registry, object tier, transforms, connectors, and checkpoint contracts are proven. |
| G8 Managed | Fleet reconciliation, safe upgrades, capacity reserve, autoscaling, metering, and support operations meet SLOs. |
| G9 Geo | Replication checkpoints, promotion, failback, loop prevention, residency, RPO, and RTO are proven in drills. |
| G10 Release | Signed artifacts, SBOM, packaging, migration, documentation, and support matrices are release-ready. |

The source-preview release path now checks one repository version against Rust,
Go, Java, Python, and TypeScript metadata, accepts release tags only at the
current `main` commit, and keeps curated notes in the repository. This is
partial G10 evidence only: licensing, signed binaries, SBOM/provenance,
registry packages, installation matrices, migration support, and production
support limits remain open.

The bootstrap trust slice supplies partial G5 evidence. Go and Rust parse the
same bounded fingerprint-only policy and decision corpus; managed HTTP/gRPC and
regional catalog/route/data boundaries fail closed, authorize explicit
action/scope, filter cross-tenant collections, and emit credential-free
structured decisions. This is not G5 completion: OIDC, credential
expiry/revocation, TLS/mTLS and peer identity, replicated policy, encryption,
immutable audit export, telemetry, quotas, and production security operations
remain open.

The segmented standalone WAL supplies partial G2 evidence: configured physical
rotation, checksummed v1 frames, single-writer ownership, global sequence
validation, manifest-bounded active-suffix repair, restart replay, durable
identity/topology checks, and crash-safe fresh-layout activation. Existing valid
single-file journals remain on the legacy writer and are not migrated. The
replicated core separately supplies a bounded canonical consensus checkpoint,
logical Raft-prefix compaction, checkpoint-plus-tail reopen, fixed-voter
snapshot catch-up, native state images for all five profiles, and atomic
physical EPRS reclamation. G2 remains open because durable derived-index
rebuild, retention, tiering, backup/PITR, scheduled restore campaigns, and
general production replica recovery are not implemented. Stream logical
time/size/combined retention now advances through a separate replicated v4
state transition; automatic maintenance scheduling and product-wide retention
governance remain open. Regional Streams now materialize several independent
ordered shard tablets, publish one versioned cross-language UTF-8 key
partitioner, and bind logical shard identity outside compatibility-pinned
tablet/snapshot bytes. Safe online expansion/remapping remains open. The
bounded EPRS and typed-profile recovery evidence is tracked separately below.

The shared clock now distinguishes wall and process-local monotonic time, and
hybrid-logical-clock tests cover backward wall jumps, remote observations,
persisted continuation, and overflow. General durable timer indexes,
uncertainty handling, automatic leader ownership, and cross-profile
restart/failover integration remain open G2/G3 work; the Queue-specific bounded
evidence appears below.

The Stage 2 consensus slice supplies partial adapter and local stable-store
evidence: a fixed three-voter `raft-rs` group uses Epoch-owned types, bounded
versioned peer frames, restart-reconstructed proposal lookup, exact-duplicate
suppression, conflicting-payload fail-stop, SHA-256 applied-history digests,
and seeded `epoch-testkit` partition/delay/duplicate histories. EPRS v1 adds a
checksummed, fsync-backed `FileWal` journal for immutable identity, complete
`HardState`, normal entries, and applied/publishable checkpoints; unit tests
cover local reopen, suffix replacement, partial-tail repair, corruption, and
writer exclusion. Adapter tests reopen three voters with identical committed
history/digests, verify stable-barrier ordering, recover a post-append unknown
outcome, and publish commit-ahead-of-checkpoint recovery once. A separate
three-process smoke proves minority non-commit, partition healing, identical
receipts/digests, and one-voter plus all-voter `SIGKILL`/same-path reopen without
duplicate receipt publication. An opt-in node runtime adds bounded real HTTP
transport and opaque diagnostic status/propose/lookup endpoints. A mutually
exclusive experimental profile mode now applies either one typed,
single-partition Stream, Queue, or Event Bus ingress/outbox tablet or one
single-shard Cache tablet after commit. Startup installs a canonical native
image when present, then applies only the retained committed tail before
readiness; legacy histories still replay.
All four return bounded two-durable-voter evidence. The Event Bus retains
independent delivery intent and attempt state but explicitly excludes a built-in
target executor or external-delivery claim. The executable gates
cover minority non-commit, leader rebinding, typed profile semantics, catch-up,
exact retry, convergence, and all-container `SIGKILL`/reopen. Public product
profiles remain standalone. The Stream mode additionally carries canonical
atomic batches through none/gzip/LZ4/Snappy/Zstd command frames with hard
decompression limits, correlated offsets, every-codec real-runtime reopen, and
an independent Python-gzip container proof while preserving command v1. Its
additive command v3 also replicates consumer-group next offsets, explicit
reset, caller-generation ownership fencing, committed business rejections,
lag/replay reads, three-runtime convergence, and container `SIGKILL` recovery
without changing v1/v2 evidence. The
regional boundary now adds safe leader
ReadIndex requests that require majority confirmation and local typed-profile
application; default regional reads expose exact barrier evidence, explicit
`local_stale` reads remain available, and a minority times out without
downgrade. Compatible EPSN v1/v2 checkpoints now fsync before local or received
installation. V2 binds canonical native state, rolling EPDG state, and a bounded
retry suffix; EPRS kind 4 atomically reclaims obsolete generations. The runtime
reopens with a committed tail and installs a lagging voter's typed state before
tail application. G3 remains open for the exhaustive crash matrix,
membership and authoritative epoch transitions, follower read routing,
authenticated transport, dynamic placement/repair, model and chaos reports,
density, and performance. See [Consensus Feasibility Spike](CONSENSUS_SPIKE.md),
[ADR-0013](adr/0013-quorum-read-barriers.md),
[ADR-0015](adr/0015-stream-batch-compression.md),
[ADR-0016](adr/0016-stream-consumer-group-checkpoints.md),
[ADR-0023](adr/0023-stream-retention-policies.md),
[ADR-0024](adr/0024-stream-multishard-key-routing.md),
[ADR-0021](adr/0021-consensus-checkpoint-and-snapshot-installation.md),
[ADR-0022](adr/0022-profile-native-checkpoints-and-physical-reclamation.md),
[Consensus Checkpoints](CONSENSUS_CHECKPOINTS.md),
[Experimental Stream Tablet](STREAM_TABLET.md), and
[Experimental Replicated Queue Tablet](QUEUE_TABLET.md), and
[Experimental Replicated Cache Tablet](CACHE_TABLET.md), and
[Experimental Replicated Event Bus Tablet](BUS_TABLET.md). The Cache and Bus proofs are
still a fixed-topology internal milestone, not placement-aware public quorum
durability.

## Cache and State

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| CACHE-001 | P0 | Core scalar and collection types | M1 prototype → M2 | Slice | G0, G1, G4 | Strict string/blob/counter/hash/list/set/sorted-set values participate in canonical commands, digests, EPRS replay, and browser-safe observations; typed Go/Java/Python constructors reject duplicate sets/non-finite scores and the post-failover Python transaction executes every kind. Pending: type-specific collection-mutation/property matrix |
| CACHE-002 | P0 | Key/default TTL and expiry events | M1 → M2 | Slice | G0, G1, G2, G4 | Deterministic expiry, pure passive reads, committed maintenance, time rollback, failover, replay, three-language TTL/maintain methods, and live post-leader-loss expiry are tested; pending: background active expiry and expiry events |
| CACHE-003 | P0 | Eviction policy family | M1 prototype → M2 | Slice | G0, G4, G5 | Deterministic no-eviction capacity and rollback are tested; pending: LRU/LFU/TTL/random volatile/all-key policies and memory-pressure benchmarks |
| CACHE-004 | P0 | Shard-local atomic operations | M1 prototype → M2 | Slice | G0, G3, G4 | Bounded distinct-key atomic success/rollback is covered through engine, tablet, runtime, typed three-language transaction builders, and a real post-failover multi-value Python transaction; pending: concurrent linearizability report |
| CACHE-005 | P0 | Pipeline, multiplex, batch, pool guidance | M1 → M2 | Slice | G1, G4 | Pending: ordering and throughput suite |
| CACHE-006 | P0 | CAS, optimistic transaction, increment, fenced lock | M2 | Slice | G0, G3, G4 | Deterministic and real-runtime tests plus the authenticated Cache v1 route and complete Go/Java/Python clients cover non-ABA CAS, atomic transaction/rollback, checked increment/TTL, guarded locks, opaque-token renewal, current-term admission, leader loss, EPRS replay, convergence, and leader ReadIndex observations. Pending: concurrent history checker, follower routing, and production fault matrix |
| CACHE-007 | P0 | Volatile, replicated-memory, quorum modes | M1 prototype → M2 | Slice | G0, G2, G3, G4 | Standalone volatile Cache remains separate; the regional Cache v1 path exposes the fixed-three-voter majority-persisted tablet through authenticated routing and proves post-leader-loss/all-voter replay without claiming a generally selectable public quorum profile. Pending: replicated-memory mode, named durability selection, and placement-aware durability fault matrix |
| CACHE-008 | P1 | Snapshot, WAL restore, backup, PITR | M3 | Planned | G2, G5, G7 | The internal compact Cache voter image and automatic fixed-voter restore are storage prerequisites, not an exportable backup/PITR product; pending: artifact/catalog, encryption, retention, semantic PITR, and scheduled restore drill |
| CACHE-009 | P1 | Explicitly lossy Pub/Sub and patterns | M3 | Planned | G0, G4, G6 | Pending: route and disconnect semantics suite |
| CACHE-010 | P1 | Durable mutation change stream | M3 | Planned | G2, G4, G7 | Pending: mutation-to-offset reconciliation |
| CACHE-011 | P2 | Bitmap, cardinality, probabilistic, geo types | M6 | Planned | G2, G4 | Pending: accuracy and persistence corpus |
| CACHE-012 | P2 | JSON operations and secondary indexes | M6 | Planned | G2, G4, G7 | Pending: index consistency/rebuild suite |
| CACHE-013 | P2 | Vector and hybrid search | M6 | Planned | G4, G7 | Pending: recall, latency, and rebuild benchmark |
| CACHE-014 | P2 | Flash/cold value tier | M6 | Planned | G2, G7, G8 | Pending: hot/cold integrity and SLO report |
| CACHE-015 | Deferred | Selected active-active CRDTs | D | Deferred | G0, G3, G9; named demand | Pending: CRDT convergence model and ADR |

## Stream Log

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| STREAM-001 | P0 | Partitioned append log and key routing | M1 prototype → M2 | Slice | G0, G1, G2, G4 | Regional resources materialize one independently replicated ordered tablet per shard. Discovery publishes versioned unsigned FNV-1a 64 over UTF-8 with event-ID fallback and shard count; Rust/Go/Java/Python vectors agree. SDK keyed append pins resource generation, and logical shard identity is externalized without changing canonical partition-0 command/snapshot bytes. PR #62, exact-main CI, main-only Pages, and the real three-shard Python failover/all-node-recovery campaign are green. Pending: safe online expansion/remapping, virtual shards, hot-key mitigation, and production scale/fault tests |
| STREAM-002 | P0 | Time/size/combined retention | M1 basic → M2 | Slice | G0, G2, G4 | Canonical command v4 replicates complete record-count, compact-JSON-byte, and inclusive-age policies plus explicit idle maintenance without changing v1/v2/v3 bytes. Deterministic core/tablet tests cover exact boundaries, combined oldest-first deletion, oversized-record rollback, dedupe reclamation, monotonic time, digest convergence, and native snapshot restore. Direct and authenticated regional routes plus Go/Java/Python clients expose per-shard configure/maintain/observe; real three-runtime checkpoint/reopen and the regional Python SDK container campaign preserve the retained base and policy through recovery. PR #60, exact-main CI, Pages deployment, and live documentation are green. Pending: automatic maintenance scheduling, keyed compaction/tombstones, object-tier retention, namespace/legal-hold governance, and scale tests |
| STREAM-003 | P0 | Consumer groups, offsets, lag, reset/replay | M2 | Slice | G0, G2, G3, G4, G5 | Canonical v3 commands replicate each shard's next offset, explicit reset, caller-generation owner fencing, typed rejection, exact retry, lag and replay; v4 retention preserves and flags out-of-range checkpoints. Additive v5 commands on shard 0 replicate bounded join/rejoin, generation-fenced heartbeat/leave, monotonic committed time, inclusive dead-member expiry, and lexical round-robin assignment over the captured resource shard count. Snapshot v2 plus legacy-v1 restore, deterministic and real three-voter checkpoint/reopen tests, authenticated routes, matching Go/Java/Python clients, and the Python post-leader-loss recovery campaign pass through PR #63 and exact-main CI 31699302841; Pages 31699302769 publishes the exact examples. Assignment generation remains separate from each shard's v3 checkpoint owner. Pending: background maintenance, cooperative revoke acknowledgement, atomic assignment-plus-offset handoff/transactions, streaming consumption, authorization/audit specificity, generated response types, scale/fairness, and production fault matrix |
| STREAM-004 | P0 | Partition order and acknowledgement policy | M1 prototype → M2 | Slice | G0, G2, G3, G4 | Local fsync-before-apply plus experimental fixed-voter majority-before-local-profile-apply receipts (`durable_voter_acks=2`), minority non-commit, ordered offsets, semantic retry/rebinding, and conflict tests; pending: placement-aware public/multi-policy durable ack matrix |
| STREAM-005 | P0 | Zone replication, election, ISR visibility | M1 prototype → M2 | Slice | G2, G3, G5 | Fixed-voter deterministic histories plus typed real-runtime/container leader replacement, old-voter catch-up, and all-voter `SIGKILL` replay; pending: placement domains and authenticated replica/ISR visibility |
| STREAM-006 | P0 | Batching and required compression paths | M2 | Slice | G2, G4, G6 | Canonical command v2 carries bounded atomic batches through `none`, gzip, LZ4 frame, Snappy framed, and Zstd frame paths; strict unit/golden/malformed/bomb tests and real three-runtime commit/retry/rebuild preserve v1 bytes/digests. Go/Java/Python regional clients expose one explicit-shard atomic operation with canonical none/gzip encoders, exact caller frames for all five codecs, and exact-frame/idempotency retry. Cross-language unit and published-source tests plus the updated Python post-leader-loss batch, catch-up, and all-node recovery campaign pass locally; protected evidence is pending. Regional responses externalize the target logical shard. Pending: stable bidirectional Produce, producer auto-batching and negotiation, cross-shard planning, non-atomic partial results, fuzz corpus, and matched compression benchmarks |
| STREAM-007 | P1 | Idempotent producer sequencing | M5 | Planned | G2, G3, G7 | Pending: duplicate/recovery history |
| STREAM-008 | P1 | Transactions, atomic offsets, read-committed | M5 | Planned | G0, G2, G3, G7 | Pending: transaction model/history report |
| STREAM-009 | P1 | Key compaction and tombstones | M3 | Planned | G2, G4, G7 | Pending: compaction/recovery corpus |
| STREAM-010 | P1 | Object-tier historical fetch | M3 | Planned | G2, G5, G7 | Pending: tier integrity/outage/SLO report |
| STREAM-011 | P1 | Partition advice and online expansion | M3 | Planned | G3, G5, G8 | Pending: expansion availability/order report |
| STREAM-012 | P2 | Push, pull, isolated-bandwidth consumers | M6 | Planned | G4, G8 | Pending: bandwidth isolation benchmark |
| STREAM-013 | P1 | Open-format capture/export | M3 | Planned | G2, G7 | Pending: manifest/checkpoint reconciliation |
| STREAM-014 | P1 | Cross-cluster/region replication | M4 → M5 | Planned | G2, G3, G9 | Pending: loop and checkpoint-mapping drill |
| STREAM-015 | P1 | Logical superstream | M3 | Planned | G3, G4, G6 | Pending: aggregate discovery/routing suite |

## Work Queue

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| QUEUE-001 | P0 | Competing consumers and delivery transitions | M1 prototype → M2 | Slice | G0, G2, G4 | Typed real-runtime enqueue/acquire/Ack and committed rejection plus complete regional Go/Java/Python clients; the Python SDK settles competing deliveries after active-leader loss and all-voter reopen; pending: concurrent saturation/fairness corpus and native streaming receive |
| QUEUE-002 | P0 | Renewable visibility/acquisition lease | M1 prototype → M2 | Slice | G0, G2, G3, G4 | Exact renewed-token HTTP replay, consumer epoch, deadline, active-leader SIGKILL, old-term-token rejection, new-term redelivery, all-voter recovery, and regional SDK renewal/opaque-token APIs in three languages; pending: exhaustive partition/crash-point matrix |
| QUEUE-003 | P0 | Durability-aware publisher confirmation | M1 prototype → M2 | Slice | G0, G2, G3 | Standalone fsync evidence plus fixed-three-voter typed receipts only after majority persistence/local actor apply, minority semantics inherited from consensus gate; pending: authenticated placement-aware quorum matrix |
| QUEUE-004 | P0 | Delayed and scheduled messages | M1 prototype → M2 | Slice | G0, G2, G4 | Deterministic committed-order time normalization (including descending failover assignments), TTL/max-age tests, real container scheduled ineligibility/promotion, explicit committed maintenance, and three-language regional maintenance/release APIs; pending: automatic timer proposal and precision load report |
| QUEUE-005 | P0 | Retry/backoff/jitter/attempt-age policy | M1 prototype → M2 | Slice | G0, G2, G4 | Deterministic non-zero jitter/terminal corpus plus typed release/Nack/maintain real-runtime flow, container visibility-timeout retry after failover, and full regional SDK dispositions; pending: broader policy and fault corpus |
| QUEUE-006 | P0 | Provenance-rich DLQ and redrive | M1 prototype → M2 | Slice | G2, G4, G5 | Typed Reject/redrive and immutable history APIs converge and survive all-node SIGKILL; regional v1 adds scoped linearizable history and exact-history redrive in Go/Java/Python, with Python executing it after Queue leader loss; pending: external immutable audit export and admin-only policy refinement |
| QUEUE-007 | P1 | TTL, queue expiry, capacity and overflow | M3 | Planned | G0, G2, G4 | Pending: lifecycle/capacity boundary suite |
| QUEUE-008 | P1 | FIFO sessions and renewable lock | M5 | Planned | G0, G2, G3, G4 | Pending: per-session order/fencing history |
| QUEUE-009 | P1 | Dedupe identifier and window | M5 | Planned | G0, G2, G7 | Pending: restart/window suppression suite |
| QUEUE-010 | P1 | Fair priority bands | M5 | Planned | G0, G2, G4 | Pending: eligibility/starvation benchmark |
| QUEUE-011 | P0 | Credit/prefetch and consumer concurrency | M1 native → M2 | Slice | G0, G4, G6 | Deterministic and real-three-runtime suites prove bounded request credit, per-consumer saturation across epochs, settlement replenishment, exact capacity evidence, and pure consumer-flow observations; regional Go/Java/Python clients expose bounded credit/window input and linearizable flow reads, and Python executes both after leader loss; pending: native bidirectional connection credit, automatic prefetch, fairness/backpressure load report, and indexed backlog performance |
| QUEUE-012 | P1 | Dispatch shaping and circuit breaker | M5 | Planned | G4, G5, G8 | Pending: downstream protection load report |
| QUEUE-013 | P2 | Deferred retrieval by identifier | M6 | Planned | G2, G4, G5 | Pending: deferred lifecycle/access suite |
| QUEUE-014 | P2 | Request/reply and temporary destinations | M6 | Planned | G0, G4, G6 | Pending: correlation/cleanup failure suite |
| QUEUE-015 | P1 | At-least-once quorum DL forwarding | M5 | Planned | G2, G3, G7 | Pending: crash-boundary forwarding history |

## Event Bus and Pub/Sub

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| BUS-001 | P0 | Topics, subscriptions, route/fan-out/wildcards | M1 basic → M2 | Slice | G0, G1, G4 | Deterministic lexical fan-out, Unicode wildcard truth table, bounded canonical route updates, atomic per-subscription outbox creation, majority-before-success, failover/catch-up, EPRS/all-node recovery, complete digest convergence, and an authenticated regional shard-0 route with three SDKs; pending: multi-topic/multi-shard routing and executor-level backpressure proof |
| BUS-002 | P0 | Attribute and JSON-content filters | M4 | Slice | G0, G4 | Conjunctive event/source/subject/header/JSON truth table plus strict bounded path/map validation; pending: compiled/interpreted differential suite |
| BUS-003 | P0 | Pull, push, webhook, queue, stream targets | M4 | Slice | G0, G4, G5, G6 | Target-shaped records and a fenced acquire/ack/fail pull-worker protocol are replicated, authenticated, exposed by regional Go/Java/Python clients, queried, recovered, and target-isolated; pending: built-in long-poll/push/Queue/Stream/webhook/HTTP executors and their public contracts |
| BUS-004 | P0 | Per-target retry, timeout, rate, DLQ | M4 | Slice | G0, G2, G4, G5 | Captured per-subscription timeout/max-in-flight/retry policy, deterministic backoff/jitter, bounded lease-expiry maintenance, attempt exhaustion, dead-letter state, fencing, authenticated regional SDK validation, real-runtime/container convergence, and EPRS recovery; pending: rate limiting, redrive/retention, executor crash matrix, and external audit |
| BUS-005 | P0 | CloudEvents 1.0 over HTTP | M1 envelope → M4 | Slice | G0, G1, G6 | Pending: CloudEvents conformance/round-trip |
| BUS-006 | P1 | Archive and filtered replay | M4 | Slice | G2, G5, G7 | Inclusive time/filter replay, bounded browser-safe response, checked archive capacity, atomic rejection, replicated recovery-state digest, authenticated linearizable regional SDK reads, real-runtime convergence, and EPRS/container recovery; pending: replay-attempt reconciliation and retention |
| BUS-007 | P1 | Declarative input transformation | M4 | Slice | G0, G4, G7 | Deterministic header addition and payload projection included in the delivery-plan digest/convergence suite; pending: broader golden corpus and runtime target evidence |
| BUS-008 | P2 | Bounded synchronous enrichment | M6 | Planned | G5, G7, G8 | Pending: timeout/size/isolation security suite |
| BUS-009 | P1 | Schema validation integration | M4 | Planned | G5, G7 | Pending: schema rejection/reference trace |
| BUS-010 | P1 | MQTT 5 state and QoS mapping | M4 | Planned | G0, G4, G6 | Pending: named MQTT conformance matrix |
| BUS-011 | P0 | Signed webhooks and replay defense | M4 | Planned | G5, G6 | Pending: crypto/rotation/replay/SSRF report |
| BUS-012 | P1 | Authenticated API destinations | M4 | Planned | G5, G7 | Pending: secret rotation and auth refresh suite |
| BUS-013 | P2 | Global endpoint health/failover | M6 | Planned | G8, G9 | Pending: regional routing/failover drill |
| BUS-014 | P2 | Owner/schema/lineage event catalog | M6 | Planned | G5, G7, G8 | Pending: catalog authorization/lineage suite |
| BUS-015 | P1 | Function and managed-connector targets | M4 | Planned | G5, G7, G8 | Pending: target lifecycle/checkpoint suite |

## Managed Platform

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| MGD-001 | P1 | Serverless and dedicated choices | M4 | Planned | G4, G5, G8, G10 | Pending: topology/semantic/isolation matrix |
| MGD-002 | P0 | Automatic placement and online rebalance | M1 prototype → M2 | Slice | G2, G3, G5 | The consensus-backed Rust catalog allocates stable tablet/group identities, the bounded supervisor materializes fixed three-voter profile groups, and Go validates policy-protected configured-endpoint region/zone/class plus incremental group capacity before catalog mutation while separating desired replicas from observed voters; pending: mTLS server identity, general voter selection, transactional reservation, dynamic membership, online transfer/rebalance, and production chaos report |
| MGD-003 | P1 | Policy-bound multidimensional autoscaling | M4 | Planned | G5, G8 | Pending: hysteresis/headroom load report |
| MGD-004 | P0 | Multi-zone replicas and failover | M1 prototype → M2 | Slice | G2, G3, G5 | Bounded fixed-voter evidence includes three policy-protected configured zones, catalog plus simultaneous Cache/Stream/Queue/Bus groups, EPRS persistence, real-process/container leader replacement, truthful two-voter degradation in the Go BFF, entry and consensus-snapshot catch-up, minority non-commit, and all-voter `SIGKILL` replay; pending: mTLS server identity, rack constraints, dynamic membership, public durability policy, broader fault matrix, and failover SLO report |
| MGD-005 | P1 | Geo DR, switch, promotion, failback | M4 → M5 | Planned | G3, G8, G9 | Pending: RPO/RTO and split-brain drill |
| MGD-006 | P1 | Backup, validation, semantic PITR | M3 | Planned | G2, G5, G7, G8 | Native checkpoints now prove bounded internal fixed-voter catch-up/restart and physical journal reclamation only; pending: backup artifact/catalog, encryption, semantic PITR, retention, scheduled restore, and validation evidence |
| MGD-007 | P1 | Guarded rolling upgrades | M5 | Planned | G3, G5, G6, G8, G10 | Pending: mixed-version stop/rollback drill |
| MGD-008 | P0 | Unified workload identity and authorization | M2 baseline → M4 | Slice | G0, G5, G6 | A shared strict fingerprint-only bootstrap policy authenticates Go HTTP/gRPC, Go-to-Rust workload calls, and Rust regional HTTP; explicit actions plus organization/project/environment/namespace scopes fail closed and one Go/Rust corpus prevents evaluator drift. Pending: OIDC, short-lived/revocable credentials, mTLS/peer identity, replicated policy/ACLs, compatibility-protocol mapping, and the full authorization differential matrix |
| MGD-009 | P1 | Private ingress and controlled egress | M4 | Planned | G5, G8 | Pending: cloud connectivity/isolation report |
| MGD-010 | P0 | Transit/at-rest encryption and managed keys | M4 | Planned | G2, G5, G8 | Pending: TLS/storage/rotation report |
| MGD-011 | P0 | Immutable audit and history export | M1 basics → M4 | Slice | G2, G5, G8 | Go and Rust emit bounded structured authentication/authorization decisions with request/principal/policy/action/decision/reason/scope fields and no credential/payload field; pending: durable append-only storage, required-event completeness, tenant access history, integrity/retention, export, and delivery-failure reconciliation |
| MGD-012 | P0 | Telemetry, dashboards, alerts, OTel | M1 basics → M2 | Slice | G1, G5 | Pending: golden-signal and alert fault suite |
| MGD-013 | P1 | Metering, budget, quotas, anomaly alerts | M4 → M5 | Planned | G5, G8 | Pending: raw-usage/billing reconciliation |
| MGD-014 | P0 | CLI, core SDKs, emulator, operator | M1 core → M2 | Slice | G1, G5, G10 | Pending: artifact/lifecycle/e2e matrix |
| MGD-015 | P2 | Connector marketplace and lifecycle | M6 | Planned | G5, G7, G8, G10 | Pending: install/upgrade/provenance suite |
| MGD-016 | P1 | Customer-managed key rotation | M4 | Planned | G5, G8 | Pending: revoke/rotate/recover drill |
| MGD-017 | P1 | Terraform provider | M3 | Planned | G1, G5, G10 | Pending: plan/apply/import/drift suite |

## Control Plane

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| CTRL-001 | P0 | Idempotent declarative resource API | M1 → M2 | Slice | G0, G1, G3 | Generated RegionalAdmin gRPC Apply/Get/List/Delete, scoped bootstrap authorization, transactionally persisted apply/delete outcomes, exact replay across Go `SIGKILL`, token-rebinding rejection, Rust catalog replay, disconnect/reconnect, conflict mapping, and authenticated real Go-to-Rust apply pass; pending: long-running operation lookup, multi-instance ownership, and an advertised unknown-outcome/token-retention report |
| CTRL-002 | P0 | Strong versioned metadata and OCC | M1 prototype → M2 | Slice | G0, G2, G3 | Monotonic Rust and Go generations, expected-generation conflicts, durable Go/Rust tombstones, generation-fenced status, browser-safe desired/observed generations, version/corruption fail-closed tests, and single-owner metadata recovery pass; pending: multi-instance linearizability, management leader election, watch/resume, migration/backup policy, and broader metadata recovery report |
| CTRL-003 | P0 | Placement/residency/tenancy constraints | M2 | Slice | G0, G3, G5 | `ResourceSpec` supports allowed regions, minimum zones, and required node class; Go validates every authenticated fixed voter before Rust catalog mutation and publishes achieved topology. Pending: dedicated tenancy, rack constraints, residency export enforcement, general solver, and policy inheritance |
| CTRL-004 | P0 | Safe topology and repair operations | M1 prototype → M2 | Slice | G2, G3, G5 | In-memory caught-up leader-transfer history; pending: membership, split/merge, repair/rebalance, persistent transition, and chaos evidence |
| CTRL-005 | P0 | Safe admission and limiting-resource reason | M2 | Slice | G3, G5, G8 | Rust reports maximum/used/available group slots; Go charges only additional shards, returns the stable limiting-node capacity reason, and unit/container tests prove rejection occurs before catalog apply. Pending: transactional reservations, multidimensional CPU/memory/disk/network sizing, concurrent-controller safety, and saturation report |
| CTRL-006 | P1 | Change plan, approval, rollback | M4 | Planned | G3, G5, G8 | Pending: preview/apply/rollback audit suite |
| CTRL-007 | P1 | Versioned common resource templates | M3 | Planned | G0, G1, G5 | Pending: template golden manifests |
| CTRL-008 | P1 | Organization policy guardrails | M4 | Planned | G3, G5 | Pending: inherited-policy allow/deny matrix |

## Schemas, Transformations, and Connectors

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| INT-001 | P1 | Three schema formats and compatibility | M3 | Planned | G0, G7 | Pending: compatibility corpus |
| INT-002 | P1 | Producer/broker validation modes | M3 | Planned | G5, G6, G7 | Pending: validation rejection matrix |
| INT-003 | P1 | Declarative field transforms | M4 | Planned | G0, G7 | Pending: transform golden/property suite |
| INT-004 | P2 | Resource-bounded transform sandbox | M6 | Planned | G5, G7 | Pending: escape/exhaustion security report |
| INT-005 | P1 | Source/target-aware connector checkpoints | M4 | Planned | G2, G7 | Pending: crash-boundary gap/duplicate history |
| INT-006 | P1 | Record errors, partial batch, replay/backfill | M4 | Planned | G4, G7 | Pending: mixed-result recovery suite |
| INT-007 | P1 | Rotatable references and connector egress policy | M4 | Planned | G5, G7 | Pending: secret/egress abuse report |
| INT-008 | P1 | Initial storage, CDC, Kafka, HTTP connectors | M4 | Planned | G5, G6, G7, G8 | Pending: per-connector certification pack |
| INT-009 | P2 | Warehouse/search/analytics/bus connectors | M6 | Planned | G5, G7, G8 | Pending: marketplace conformance pack |

## Developer Experience

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| DX-001 | P0 | Official Go, Java, and Python SDKs | M1 one SDK → M2 | Slice | G0, G1, G4, G10 | Go/Java/Python standalone tests and exact-source restart quickstarts remain green. All three share regional Stream, complete Queue, complete Cache, and complete implemented Event Bus discovery/auth/fence/idempotent-retry/read-barrier contracts with unit tests and Pages examples. Stream clients additionally expose identical versioned UTF-8 shard selection, generation-pinned keyed append, atomic caller-framed batches, and shard-zero consumer-session join/heartbeat/leave/maintenance/observation; Python executes three Stream shards and all four profiles after real leader loss. Pending: generated regional response types, native streaming/cooperative consumption, package publication, and the complete native contract/version matrix |
| DX-002 | P0 | Generated guarantee-aware API docs | M1 → M2 | Slice | G0, G1, G10 | Hand-authored guarantee/error guidance, exact executable standalone quickstarts, and exact compilable regional Stream/Queue/Cache/Event Bus Go/Java/Python sources are built into the docs-only Pages artifact; ADR-0017–0020 plus ADR-0023–0026 and the regional SDK guides own route/retry/non-claim semantics. The Stream examples include keyed routing, a two-record gzip atomic batch, checkpoints, sessions, and retention. Pending: generated API reference and full doc lint |
| DX-003 | P0 | Deterministic single-binary emulator | M1 → M2 | Slice | G1, G2, G4, G10 | Seeded scheduler, virtual clocks/fault plan/transport, golden EPTR history, fixed-voter consensus, real-process EPRS/SIGKILL, and typed Stream/Queue/Cache/Bus runtimes; pending: executable replay bundle and runnable emulator controls |
| DX-004 | P0 | Test containers and ephemeral namespaces | M1 → M2 | Slice | G1, G5, G10 | A unique three-node Compose project uses independent volumes, dynamic loopback ports, and mounted policy; it proves Go recovery plus simultaneous four-profile failover/catch-up/all-node recovery. Post-failover Python runs Stream append/checkpoint, Queue credit/lease/DLQ/redrive, Cache values/CAS/transaction/fenced-lock/expiry, and Event Bus ingress/archive/retry/settlement/query lifecycles with linearizable reads, catch-up, and reopen. Pending: parallel campaigns, Go/Java live regional execution, broader isolation, and injected disk/network matrices |
| DX-005 | P1 | Audited/redacted console message browser | M3 → M4 | Planned | G5, G7, G8 | Pending: access/redaction/action audit matrix |
| DX-006 | P0 | Explain live guarantees and cost drivers | M1 basic → M2 | Slice | G0, G3, G5 | The TypeScript console consumes only the Go BFF with an interactively entered session-only bearer, displays desired versus observed generation, per-shard voters/leader, verified region/zone/class evidence, and per-node group capacity while preserving rack/dynamic-membership non-claims; pending: OIDC/session exchange, achieved durability and broader cost inputs, browser accessibility evidence, and historical topology context |
| DX-007 | P1 | Compatibility usage scanner | M3 | Planned | G0, G6 | Pending: unsupported-feature fixture corpus |
| DX-008 | P1 | End-to-end event trace | M4 | Planned | G1, G4, G5, G7 | Pending: trace/history reconciliation |
| DX-009 | P1 | TypeScript, Rust, .NET SDKs | M3 | Planned | G0, G1, G6, G10 | Pending: multi-language client matrix |

## Lifecycle and Governance

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| GOV-001 | P1 | Recoverable delete and explicit purge | M4 | Planned | G2, G5, G7, G8 | Pending: recovery/purge completeness drill |
| GOV-002 | P2 | Legal hold and retention lock | M6 | Planned | G2, G5, G7 | Pending: non-bypass and audit review |
| GOV-003 | P1 | Payload/field redaction hooks | M4 | Planned | G5, G8 | Pending: restricted-data leakage corpus |
| GOV-004 | P1 | Residency policy and region allowlist | M4 | Planned | G3, G5, G9 | Pending: placement/export enforcement suite |
| GOV-005 | P0 | Ownership, cost, classification, tags | M2 | Planned | G1, G3, G5 | Pending: tag policy/query/cost suite |
| GOV-006 | P0 | Exportable sensitive-action audit trail | M1 basics → M4 | Slice | G2, G5, G8 | Pending: event-matrix/export reconciliation |

## Packaging and Runtime

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| PKG-001 | P0 | Selective four-profile Rust node | M1 scaffold → M4 complete | Slice | G1, G4, G10 | One binary retains all four standalone profiles, supports the earlier mutually exclusive single-group modes, and now runs catalog plus simultaneous Cache, Stream, Queue, and Event Bus groups through one bounded regional supervisor; pending: production role/profile selection, dynamic membership, resource budgets, and complete feature/config startup matrix |
| PKG-002 | P0 | Shared engine/format standalone and cluster | M1 → M2 | Slice | G1, G2, G3, G10 | Checksummed segmented standalone format plus canonical typed Stream, Queue, Cache, and Event Bus commands applied from EPRS without a second clustered WAL; pending: supported standalone-to-cluster format/migration equivalence |
| PKG-003 | P0 | Standalone without hosted Go services | M1 | Slice | G1, G2, G10 | Rust node restart/recovery test; pending: extended disconnected lifecycle suite |
| PKG-004 | P0 | Three-node quorum/failover/placement | M1 prototype → M2 | Slice | G2, G3, G10 | Deterministic, real-process, and three-container evidence covers a dedicated catalog group, three authenticated zones, live group capacity, pre-catalog rejection, three independently replicated shards of one Stream alongside all profiles, generation/epoch fencing, majority commit, leader loss, Go-observed degradation, entry and consensus-snapshot catch-up, and per-shard same-volume all-node `SIGKILL` recovery; pending: dynamic membership/voter selection, rack placement, stable public APIs, exhaustive faults, and published SLO report |
| PKG-005 | P0 | OCI, Kubernetes dev, signed binaries | M1 dev → M2 | Slice | G1, G5, G10 | Pending: clean-install/signature/SBOM CI |
| PKG-006 | P1 | Rust embedded engine with guarantee ceiling | M2 experimental → M3 | Planned | G0, G1, G2, G10 | Pending: lifecycle/persistence contract suite |
| PKG-007 | P1 | Supervised sidecar/child for other languages | M2 → M3 | Planned | G1, G5, G10 | Pending: crash/isolation/upgrade matrix |
| PKG-008 | P1 | Deterministic parent lifecycle controls | M2 → M3 | Planned | G1, G2, G10 | Pending: process state-machine suite |
| PKG-009 | P0 | Truthful deployment mode in health/config | M1 | Slice | G0, G1, G3 | Pending: health/guarantee conformance suite |
| PKG-010 | P1 | No-reencoding standalone-to-cluster migration | M3 | Planned | G2, G3, G10 | Pending: golden dataset migration/rollback |
| PKG-011 | P1 | Signed Debian/RPM packages | M3 | Planned | G5, G10 | Pending: OS install/upgrade/service matrix |

## Coverage check

| Priority | Count |
|---|---:|
| P0 | 51 |
| P1 | 54 |
| P2 | 14 |
| Explicitly deferred catalog item | 1 |
| **Total** | **120** |

The catalog count excludes the three transaction classes separately deferred in PRD §8.5: arbitrary global transactions, transactions against unknown external APIs, and unbounded cross-profile transactions. Those are tracked as delivery constraints in [DELIVERY_PLAN.md](./DELIVERY_PLAN.md).
