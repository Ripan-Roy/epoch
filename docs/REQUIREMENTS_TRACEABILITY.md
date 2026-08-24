# Epoch Requirements Traceability

This register turns the prioritized catalog in [PRD.md](./PRD.md) into a delivery and verification index. It is intentionally terse: the PRD remains the source of semantic detail, while this document owns milestone, dependency, status, and evidence tracking.

Last synchronized with PRD version 0.3 on 21 August 2026.

## How to use this register

Status values are:

- **Complete** — the full requirement named by the row has implementation,
  tests, documentation, and feature-release evidence; broader related
  capabilities remain separate requirements.
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

The release path checks one repository version against Rust, Go, Java, Python,
and TypeScript metadata, accepts release tags only at the current `main` commit,
and keeps curated notes in the repository. The current beta candidate
additionally builds five exact-tag, non-root multi-architecture OCI images on
native amd64 and arm64 runners, inspects them in pull requests, assembles exact
manifests from immutable platform digests, generates per-platform SPDX SBOMs,
attaches provenance, and keylessly signs immutable manifests. This is partial
G10 evidence until the protected/tag jobs publish and independently verify it;
raw signed binaries, package-manager distributions, installation matrices,
migration support, and production support limits remain open.

The protocol-compatibility candidate supplies partial G6 evidence. A separate
bounded Rust process translates a named RESP2/RESP3 string/counter/TTL subset,
Kafka producer/manual-consumer metadata and checkpoint APIs, and AMQP 0-9-1
direct Queue publish/confirm/consume/settlement into the fenced native regional
ports. Redis CLI 8.8.2, Kafka Java 4.3.1, and RabbitMQ Java 5.34.0 execute the
real wire listeners; native adapter contracts separately prove bearer identity,
generation/tablet/term fences, routes, payloads, and responses. Parser/semantic
tests, all four Kafka compression paths, the DX-007 versioned migration scanner,
the public matrix, Pages UI, and a fifth signed non-root OCI component are local.
Combined real-regional conformance, differential and fuzz evidence, richer
Redis types, Kafka membership/transactions, broader AMQP routing/1.0, MQTT, and
performance parity remain open; no G6 completion is claimed.

The bootstrap trust slice supplies partial G5 evidence. Go and Rust parse the
same bounded fingerprint-only policy and decision corpus; managed HTTP/gRPC and
regional catalog/route/data boundaries fail closed, authorize explicit
action/scope, filter cross-tenant collections, and emit credential-free
structured decisions. This is not G5 completion: OIDC, credential
expiry/revocation, certificate issuance/revocation and role policy, replicated
policy, WAL/data-volume encryption, external KMS, immutable audit export,
telemetry, quotas, live-Kubernetes security evidence, and production security
operations remain open. The alpha-exit branch adds required public TLS and
peer/control mTLS for the operator deployment, custom-CA/client identities in
Go/Java/Python/CLI, `backup.create`, and AES-256-GCM semantic backup artifacts;
protected evidence is pending.

The segmented standalone WAL supplies partial G2 evidence: configured physical
rotation, checksummed v1 frames, single-writer ownership, global sequence
validation, manifest-bounded active-suffix repair, restart replay, durable
identity/topology checks, and crash-safe fresh-layout activation. Existing valid
single-file journals remain on the legacy writer and are not migrated. The
replicated core separately supplies a bounded canonical consensus checkpoint,
logical Raft-prefix compaction, checkpoint-plus-tail reopen, fixed-voter
snapshot catch-up, native state images for all five profiles, and atomic
physical EPRS reclamation. The regional runtime now schedules those local
checkpoints independently on every healthy catalog/profile voter after
configurable applied-index growth and publishes durable per-group boundaries.
The alpha-exit branch now composes Catalog plus all declared tablet checkpoints
through distributed quorum barriers, validates a bounded canonical artifact,
restores all five profiles into fresh journals, and schedules encrypted
operator backups with retention and immutable creation-time restore. One clean
live Kubernetes backup/restore digest proof passes locally. G2 remains open
because log-based semantic PITR, product-wide derived-index rebuild, external
tier providers, protected cloud-storage/RPO evidence, and general production
replica recovery are not implemented. Stream logical time/size/combined retention now
advances through a separate replicated v4 state transition; command v7 adds
key compaction and checksum-verified embedded historical objects, while
product-wide retention governance remains open. Regional Streams materialize several independent
ordered shard tablets, publish one versioned cross-language UTF-8 key
partitioner, and bind logical shard identity outside compatibility-pinned
tablet/snapshot bytes. Expand-only allocation preserves existing tablets and
bumps the generation; split/merge and key remapping remain open. The
bounded EPRS and typed-profile recovery evidence is tracked separately below.

The shared clock now distinguishes wall and process-local monotonic time, and
hybrid-logical-clock tests cover backward wall jumps, remote observations,
persisted continuation, and overflow. The regional fixed-voter slice now adds
pure per-profile deadline indexes, automatic current-leader ownership,
deterministic consensus proposal identities, topology counters, and
cross-profile leader-loss/restart integration. General uncertainty handling,
timer scale evidence, dynamic/cross-region ownership, and deadline SLOs remain
open G2/G3 work.

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
All four return bounded two-durable-voter evidence. The direct Event Bus profile
retains independent delivery intent and attempt state without running network
I/O; the regional runtime optionally layers the leader-owned signed webhook
worker described by ADR-0030 and always layers the source-leader Epoch
Queue/Stream worker described by ADR-0031. Neither path claims an atomic
cross-tablet transaction or arbitrary external business side effect. The executable gates
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
tail application. Automatic actor-serialized checkpointing now runs on every
healthy regional voter and is exercised across all 27 voter/group copies,
leader loss, catch-up, and all-node restart. G3 remains open for the exhaustive crash matrix,
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
[ADR-0028](adr/0028-automatic-regional-consensus-checkpoints.md),
[ADR-0029](adr/0029-stream-session-fenced-consumption.md),
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
| CACHE-001 | P0 | Core scalar and collection types | M1 prototype → M2 | Complete | G0, G1, G4 | Canonical string/blob/counter/hash/list/set/sorted-set values and typed hash/list/set/sorted-set transforms pass bounds, type errors, atomic transactions, snapshots, digest convergence, EPRS replay, three-language SDK tests, and regional failover/reopen. |
| CACHE-002 | P0 | Key/default TTL and expiry events | M1 → M2 | Complete | G0, G1, G2, G4 | Key/default TTL, pure reads, active leader maintenance, explicit bounded maintenance, rollback clocks, volatile eligibility, expiry change records, leader loss, replay, and three-language APIs pass deterministic and regional recovery tests. Deadline and scale SLOs remain separate performance requirements. |
| CACHE-003 | P0 | Eviction policy family | M1 prototype → M2 | Complete | G0, G4, G5 | No-eviction and deterministic all-key/volatile LRU, LFU, random, and TTL policies enforce entry plus memory/cold byte caps. Tests cover committed access, canonical ties, atomic rollback, sorted victims, class isolation, snapshot/replay equivalence, materialized config, and real failover eviction. |
| CACHE-004 | P0 | Shard-local atomic operations | M1 prototype → M2 | Complete | G0, G3, G4 | One-to-128 distinct-key atomic transactions cover ordinary and advanced transforms, guarded mutations, success, type/version/revision/capacity rollback, exact replay, three-language builders, and regional failover/reopen. Product-wide concurrent-history reporting remains G3 evidence. |
| CACHE-005 | P0 | Pipeline, multiplex, batch, pool guidance | M1 → M2 | Complete | G1, G4 | Three SDKs expose all-or-nothing `AtomicBatch` plus a native one-request, one-to-128-item `Multiplex` with unique identities, request-ordered correlations, independent outcomes, envelope prevalidation, and exact replay. Docs require long-lived transport reuse and bounded concurrency; automatic coalescing/throughput SLOs remain performance work. |
| CACHE-006 | P0 | CAS, optimistic transaction, increment, fenced lock | M2 | Complete | G0, G3, G4 | Non-ABA version/missing CAS, transactions, checked increment/TTL, opaque rotating lease tokens, downstream fencing, guarded writes, stale-term admission, automatic expiry, leader loss, convergence, EPRS replay, and linearizable reads pass deterministic, three-language, and regional recovery suites. |
| CACHE-007 | P0 | Volatile, replicated-memory, quorum modes | M1 prototype → M2 | Complete | G0, G2, G3, G4 | Standalone volatile state remains separate. Regional creation accepts named `replicated_memory` or `quorum_durable`; the fixed-three-voter persisted path truthfully reports requested and achieved durability and explicit stronger fulfillment, with leader-loss/all-voter reopen evidence. Dynamic placement fault matrices remain separate G3/G8 work. |
| CACHE-008 | P1 | Snapshot, WAL restore, backup, PITR | M3 | Complete | G2, G5, G7 | Compatible Cache snapshots/EPRS restore plus a canonical checksummed 320 KiB artifact, published restorable window, digest/config/capacity validation, revision reconstruction, expired-value filtering, atomic restore, and fresh non-ABA versions pass corruption/PITR and regional failover tests. Managed schedules, encryption, catalogs, retention, and disaster-recovery orchestration remain MGD requirements. |
| CACHE-009 | P1 | Explicitly lossy Pub/Sub and patterns | M3 | Complete | G0, G4, G6 | Node-local exact-channel and `*`/`?` pattern subscriptions expose at-most-once/no-persistence/node-affinity semantics, bounded filters/payload/pending queues, drain-on-poll, overflow drops/counters, deletion, three SDKs, and regional execution. Durable use cases are directed to changes/Event Bus. |
| CACHE-010 | P1 | Durable mutation change stream | M3 | Complete | G2, G4, G7 | Replicated 1,024-record history covers mutations, expiry, eviction, and restore with monotonic sequence, explicit retention floor/stale-cursor rejection, snapshot/digest recovery, three SDKs, and regional failover/reopen reconciliation. |
| CACHE-011 | P2 | Bitmap, cardinality, probabilistic, geo types | M6 | Complete | G2, G4 | Bounded bitmap, HLL-like cardinality, Bloom/Cuckoo, and microdegree geo transforms/queries pass deterministic behavior, accuracy/membership, invalid-layout/capacity, snapshot, digest, transaction, SDK, and regional recovery tests. |
| CACHE-012 | P2 | JSON operations and secondary indexes | M6 | Complete | G2, G4, G7 | Bounded JSON pointer set/remove and exact canonical secondary indexes pass depth/size/pointer/document limits, upsert/remove consistency, snapshot rebuild validation, digest recovery, typed query, SDK, and regional failover tests. |
| CACHE-013 | P2 | Vector and hybrid search | M6 | Complete | G4, G7 | Bounded exact vector/text hybrid search validates finite dimensions, dimension consistency, metadata filters, deterministic ranking, upsert/remove, snapshot/digest recovery, SDKs, and regional failover. ANN recall/latency benchmarks are explicitly not claimed. |
| CACHE-014 | P2 | Flash/cold value tier | M6 | Complete | G2, G7, G8 | Per-resource cold byte caps and storage class are replicated; every voter fsyncs canonical per-key files after apply, removes stale files on delete/eviction/restore, rebuilds on recovery, integrity-checks cold reads, and publishes observed local-file timing as not an SLO. Canonical state remains in memory, so heap offload and production flash capacity relief are not claimed. |
| CACHE-015 | Deferred | Selected active-active CRDTs | D | Deferred | G0, G3, G9; named demand | Pending: CRDT convergence model and ADR |

## Stream Log

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| STREAM-001 | P0 | Partitioned append log and key routing | M1 prototype → M2 | Slice | G0, G1, G2, G4 | Regional resources materialize one independently replicated ordered tablet per shard. Discovery publishes versioned unsigned FNV-1a 64 over UTF-8 with event-ID fallback and shard count; Rust/Go/Java/Python vectors agree. SDK keyed append pins resource generation, logical shard identity is externalized without changing canonical partition-0 command/snapshot bytes, and expand-only catalog changes preserve existing tablets while allocating new shards. Pending: virtual shards, hot-key mitigation, and production scale/fault tests. |
| STREAM-002 | P0 | Time/size/combined retention | M1 basic → M2 | Slice | G0, G2, G4 | Canonical command v4 replicates complete record-count, compact-JSON-byte, and inclusive-age policies without changing v1/v2/v3 bytes. Deterministic core/tablet tests cover exact boundaries, combined oldest-first deletion, oversized-record rollback, dedupe reclamation, monotonic time, digest convergence, native snapshot restore, and earliest-deadline selection. Direct/regional routes and Go/Java/Python clients retain explicit configure/maintain/observe. The regional current leader proposes due idle maintenance automatically; keyed compaction/tombstones and transparent immutable historical fetch are now covered by STREAM-009/010. Pending: namespace/legal-hold governance, timer scale/SLO evidence, and broader fault matrix. |
| STREAM-003 | P0 | Consumer groups, offsets, lag, reset/replay | M2 | Slice | G0, G2, G3, G4, G5 | Canonical v3 commands replicate each shard's next offset, explicit reset, lag/replay, and owner fencing. V5 shard-zero sessions add durable membership, expiry, monotonic generation, and deterministic assignment. V6 claims preserve each assigned shard's offset while installing a bounded monotonic session fence; exact-member/generation fetch and commit reject stale owners. Go/Java/Python pin resource generation, pre-plan at most 4,096 bridges, claim every shard with deterministic keys, and revalidate the coordinator. STREAM-008 adds atomic same-tablet output-plus-offset commit and STREAM-012 adds bounded push/dedicated long poll. Pending: cooperative revoke acknowledgement, member-bound authorization/audit, generated response types, scale/fairness, and production fault matrix. |
| STREAM-004 | P0 | Partition order and acknowledgement policy | M1 prototype → M2 | Slice | G0, G2, G3, G4 | Local fsync-before-apply plus experimental fixed-voter majority-before-local-profile-apply receipts (`durable_voter_acks=2`), minority non-commit, ordered offsets, semantic retry/rebinding, and conflict tests; pending: placement-aware public/multi-policy durable ack matrix |
| STREAM-005 | P0 | Zone replication, election, ISR visibility | M1 prototype → M2 | Slice | G2, G3, G5 | Fixed-voter deterministic histories plus typed real-runtime/container leader replacement, old-voter catch-up, and all-voter `SIGKILL` replay; pending: placement domains and authenticated replica/ISR visibility |
| STREAM-006 | P0 | Batching and required compression paths | M2 | Slice | G2, G4, G6 | Canonical command v2 carries bounded atomic batches through `none`, gzip, LZ4 frame, Snappy framed, and Zstd frame paths; strict unit/golden/malformed/bomb tests and real three-runtime commit/retry/rebuild preserve v1 bytes/digests. Go/Java/Python regional clients expose one explicit-shard atomic operation with canonical none/gzip encoders, exact caller frames for all five codecs, and exact-frame/idempotency retry. Cross-language unit and published-source tests plus the Python post-leader-loss batch, catch-up, and all-node recovery campaign pass protected alpha.7 CI. Regional responses externalize the target logical shard. Pending: stable bidirectional Produce, producer auto-batching and negotiation, cross-shard planning, non-atomic partial results, fuzz corpus, and matched compression benchmarks |
| STREAM-007 | P1 | Idempotent producer sequencing | M5 | Complete | G2, G3, G7 | Replicated producer epochs, contiguous sequences, payload conflict, 256-entry exact retry history, fencing, canonical snapshots, real three-voter commit, and full-cluster reopen pass. Go/Java/Python preserve the complete unsigned-64-bit epoch and sequence contract. |
| STREAM-008 | P1 | Transactions, atomic offsets, read-committed | M5 | Complete | G0, G2, G3, G7 | Bounded tablet-local transactions atomically expose up to 128 records and one consumer offset. Pending/aborted visibility, read committed/uncommitted, exact retry, push wakeup after commit, snapshot reinstall, and real quorum recovery pass. Cross-shard/external-sink atomicity is not claimed. |
| STREAM-009 | P1 | Key compaction and tombstones | M3 | Complete | G2, G4, G7 | Deterministic compaction keeps the latest committed keyed value and unkeyed records, removes aborted/superseded values, expires null tombstones inclusively, preserves offset holes in v2 snapshots, and requires compaction before immutable tiering. |
| STREAM-010 | P1 | Object-tier historical fetch | M3 | Complete | G2, G5, G7 | Bounded immutable objects retain exact covered ranges, canonical record bytes, and SHA-256 checksums. Isolation-aware reads verify and merge historical plus hot records; corruption, overlap, compaction order, aborted history, snapshot, quorum, and restart tests pass. Alpha bytes remain embedded in replicated state; external-provider outage/SLO evidence is a production gate. |
| STREAM-011 | P1 | Partition advice and online expansion | M3 | Complete | G3, G5, G8 | Pure record/byte density advice returns expand-only targets. Catalog tests reject decreases, preserve existing tablet/group identities, allocate only new shards, bump generation, materialize/reopen expanded shards, and generation-pin SDK keyed writes across races. Scale/availability SLO reporting remains operational evidence. |
| STREAM-012 | P2 | Push, pull, isolated-bandwidth consumers | M6 | Complete | G4, G8 | Pull, shared push long poll, and consumer-identified dedicated long poll are strict bounded APIs. Separate notification lanes, transaction-visibility wakeup, timeout behavior, validation, quorum recovery, and three SDKs pass. “Dedicated” is scheduling isolation in this alpha; no throughput reservation or bandwidth SLO is claimed. |
| STREAM-013 | P1 | Open-format capture/export | M3 | Complete | G2, G7 | Manual and leader-driven automatic JSON Lines/JSON Array capture use SHA-256 artifacts, replicated schedule deadlines and next-offset checkpoints, stable maintenance identities, missed-deadline catch-up, bounded artifact rotation, pending-transaction barriers, snapshot/quorum recovery, and three SDKs. External cloud-object mirroring remains an integration gate. |
| STREAM-014 | P1 | Cross-cluster/region replication | M4 → M5 | Complete | G2, G3, G9 | A bounded contiguous ingress batch atomically persists source checkpoint, source-to-local mappings, loop path, and records. Exact retry, gap, local-loop, canonical snapshot, tablet command, HTTP, three SDK, and recovery tests pass. A deployment-specific transport worker and two-region RPO/RTO drill remain operational gates. |
| STREAM-015 | P1 | Logical superstream | M3 | Complete | G3, G4, G6 | Rust pins deterministic merge ordering; Go/Java/Python validate 1–128 unique named members, perform independently linearizable member reads, retain member identity, globally limit results, and sort by append time/member/partition/offset. The response explicitly denies an atomic cross-shard snapshot. |

## Work Queue

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| QUEUE-001 | P0 | Competing consumers and delivery transitions | M1 prototype → M2 | Slice | G0, G2, G4 | Typed real-runtime enqueue/acquire/Ack and committed rejection plus complete regional Go/Java/Python clients; the Python SDK settles competing deliveries after active-leader loss and all-voter reopen; pending: concurrent saturation/fairness corpus and native streaming receive |
| QUEUE-002 | P0 | Renewable visibility/acquisition lease | M1 prototype → M2 | Slice | G0, G2, G3, G4 | Exact renewed-token HTTP replay, consumer epoch, deadline, active-leader SIGKILL, old-term-token rejection, new-term redelivery, all-voter recovery, and regional SDK renewal/opaque-token APIs in three languages; pending: exhaustive partition/crash-point matrix |
| QUEUE-003 | P0 | Durability-aware publisher confirmation | M1 prototype → M2 | Slice | G0, G2, G3 | Standalone fsync evidence plus fixed-three-voter typed receipts only after majority persistence/local actor apply, minority semantics inherited from consensus gate; pending: authenticated placement-aware quorum matrix |
| QUEUE-004 | P0 | Delayed and scheduled messages | M1 prototype → M2 | Slice | G0, G2, G4 | Deterministic committed-order time normalization (including descending failover assignments), pure earliest-deadline selection across scheduled/TTL/max-age/dedupe/lease state, committed maintenance, and three-language maintenance/release APIs pass. The current regional leader now proposes due timers automatically; Python proves a delayed retry becomes ready without maintenance after Queue leader loss, converges, and reopens. Pending: precision/load/SLO report, indexed timer scale, and broader crash matrix |
| QUEUE-005 | P0 | Retry/backoff/jitter/attempt-age policy | M1 prototype → M2 | Slice | G0, G2, G4 | Deterministic non-zero jitter/terminal corpus plus typed release/Nack/maintain real-runtime flow, container visibility-timeout retry after failover, and full regional SDK dispositions; pending: broader policy and fault corpus |
| QUEUE-006 | P0 | Provenance-rich DLQ and redrive | M1 prototype → M2 | Slice | G2, G4, G5 | Typed Reject/redrive and immutable history APIs converge and survive all-node SIGKILL; regional v1 adds scoped linearizable history and exact-history redrive in Go/Java/Python, with Python executing it after Queue leader loss; pending: external immutable audit export and admin-only policy refinement |
| QUEUE-007 | P1 | TTL, queue expiry, capacity and overflow | M3 | Slice | G0, G2, G4 | Canonical metadata byte charging, exact count/3-MiB bounds, reject/drop/dead-letter overflow, non-lease eviction, TTL, durable idle expiry, snapshot validation, and dedupe-before-eviction boundaries pass deterministic tests. The rebuilt regional recovery/reopen campaign passes locally; protected release evidence is pending. |
| QUEUE-008 | P1 | FIFO sessions and renewable lock | M5 | Slice | G0, G2, G3, G4 | V3 session commands bind one exclusive lock to Queue incarnation, term, consumer epoch, generation, and deadline; FIFO selection, ordinary-consumer exclusion, renewal token rotation, stale fencing, snapshot recovery, three SDKs, and the rebuilt real failover/reopen scenario pass locally; protected evidence is pending. |
| QUEUE-009 | P1 | Dedupe identifier and window | M5 | Slice | G0, G2, G7 | The existing replicated `dedupe_id` window is preserved across compatible snapshots and now resolves before byte/count overflow without metadata replacement; deterministic, SDK, post-leader-loss, and full-reopen replay tests pass locally; protected release evidence is pending. |
| QUEUE-010 | P1 | Fair priority bands | M5 | Slice | G0, G2, G4 | Replicated effective priority ages by committed wait time, caps at 255, and ties by commit position/message ID; starvation and recovery determinism tests pass. Production fairness/load benchmarks remain an operational gate. |
| QUEUE-011 | P0 | Credit/prefetch and consumer concurrency | M1 native → M2 | Slice | G0, G4, G6 | Deterministic and real-three-runtime suites prove bounded request credit, per-consumer saturation across epochs, settlement replenishment, Queue-wide advanced concurrency, exact capacity evidence, pure flow observations, and Go/Java/Python APIs after leader loss. Batch acquisition is the current explicit prefetch mechanism; native streaming/automatic prefetch and backlog performance remain transport/scale gates. |
| QUEUE-012 | P1 | Dispatch shaping and circuit breaker | M5 | Slice | G4, G5, G8 | A replicated integer token bucket enforces rate/burst, Queue-wide live-delivery concurrency, consecutive-failure opening, committed-time cooldown, one half-open probe, Ack reset, and Nack/Reject failure recording. Deterministic tests pass; production downstream load/SLO evidence remains open. |
| QUEUE-013 | P2 | Deferred retrieval by identifier | M6 | Slice | G2, G4, G5 | V3 fenced defer and exact-ID receive exclude hidden work from ordinary acquisition, preserve reason/state through snapshots, reject session bypass, and are exposed/tested in Go/Java/Python. Exact retrieval after leader loss and all-voter reopen passes locally; protected recovery evidence is pending. |
| QUEUE-014 | P2 | Request/reply and temporary destinations | M6 | Slice | G0, G4, G6 | Bounded correlation/reply metadata is persisted and digested; linearizable commit-ordered correlation reads and all SDKs pass locally. A temporary destination is a managed Queue with durable idle expiry; no exactly-once RPC or cross-Queue transaction is claimed. |
| QUEUE-015 | P1 | At-least-once quorum DL forwarding | M5 | Slice | G2, G3, G7 | `quorum_durable` admission, durable source outbox, exact target-incarnation binding, stable source-history target identity, target-before-source-complete ordering, snapshot fencing, and the leader worker pass deterministic tests. The rebuilt Compose campaign observes one forwarded target record across source-leader loss, voter catch-up, and all-voter reopen; protected release evidence is pending. |

## Event Bus and Pub/Sub

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| BUS-001 | P0 | Topics, subscriptions, route/fan-out/wildcards | M1 basic → M2 | Slice | G0, G1, G4 | Deterministic lexical fan-out, Unicode wildcard truth table, bounded canonical route updates, atomic per-subscription outbox creation, majority-before-success, failover/catch-up, EPRS/all-node recovery, digest convergence, and authenticated regional routing in three SDKs pass locally. One Bus remains one logical shard; multi-shard routing and production backpressure evidence remain open. |
| BUS-002 | P0 | Attribute and JSON-content filters | M4 | Slice | G0, G4 | Conjunctive event/source/subject/header/JSON filtering and strict bounded path/map validation pass deterministic routing/recovery tests. A compiled/interpreted differential and fuzz corpus remains a compatibility gate. |
| BUS-003 | P0 | Pull, push, webhook, queue, stream targets | M4 | Slice | G0, G4, G5, G6 | Replicated pull acquisition supports bounded long poll; leader-owned executors cover signed webhook, API destination, endpoint pool, function, managed connector, and pinned Epoch Queue/Stream targets. Exact lease-before-I/O, stable idempotency, committed settlement, failover, and all-voter reopen pass locally and all three SDKs expose the lifecycle. Native bidirectional streaming transport and production backpressure/SLO evidence remain open. |
| BUS-004 | P0 | Per-target retry, timeout, rate, DLQ | M4 | Slice | G0, G2, G4, G5 | Per-subscription timeout/max-in-flight/retry, deterministic backoff/jitter, replicated integer rate/burst admission, automatic lease maintenance, terminal DLQ, bounded DLQ retention, exact redrive, and archive maintenance pass deterministic/tablet/SDK tests. Managed HTTP outcomes classify 2xx, 429/5xx, network, auth, and terminal failures. Broader crash/load/fairness and external audit evidence remain production gates. |
| BUS-005 | P0 | CloudEvents 1.0 over HTTP | M1 envelope → M4 | Slice | G0, G1, G6 | Signed and managed HTTP delivery emit CloudEvents 1.0 binary or structured JSON with stable ID/source/type/subject, content type, trace context, and side-effect idempotency. Real loopback OAuth/target and signed receiver tests pass. Official CloudEvents conformance, extension-registry, and non-JSON data matrices remain open. |
| BUS-006 | P1 | Archive and filtered replay | M4 | Slice | G2, G5, G7 | Inclusive time/filter replay, bounded browser-safe responses, count/age retention with leader-driven maintenance, checked capacity, atomic rejection, SDK reads, digest convergence, and EPRS/all-voter recovery pass locally. Production archive scale/object-tier and replay-campaign evidence remain open. |
| BUS-007 | P1 | Declarative input transformation | M4 | Slice | G0, G4, G7 | Bounded projection, rename, constants, templates, header addition, lookup enrichment, exact output/operation ceilings, and delivery-plan digest convergence pass deterministic and target-runtime tests. A broader differential/fuzz corpus remains open. |
| BUS-008 | P2 | Bounded synchronous enrichment | M6 | Slice | G5, G7, G8 | Replicated lookup definitions enforce deterministic no-network execution, required/missing behavior, and strict timeout/input/output/record bounds before routing. External enrichment calls are intentionally not implemented; scale/isolation evidence remains open. |
| BUS-009 | P1 | Schema validation integration | M4 | Complete | G5, G7 | Compiler-backed Avro, JSON Schema, and self-contained Protobuf definitions, monotonic immutable revisions, adjacent compatibility, exact references, producer/broker policies, bounded masked payload rejection, snapshot semantic revalidation, and regional recovery pass locally. External JSON references, Protobuf imports, and per-schema administration beyond namespace authorization are explicit non-claims. |
| BUS-010 | P1 | MQTT 5 state and QoS mapping | M4 | Slice | G0, G4, G6 | Persistent session/expiry, retained-message, wildcard, QoS-minimum, deterministic shared-subscription cursor, snapshot validation, and recovery semantics pass locally. An MQTT 5 wire gateway and named protocol conformance matrix remain open and are not claimed by this alpha. |
| BUS-011 | P0 | Signed webhooks and replay defense | M4 | Slice | G5, G6 | ADR-0030 defines leader ownership, exact lease-before-I/O ordering, HMAC-SHA-256 canonical input, external multi-key rotation, and at-least-once receiver duties. Rust and Go/Java/Python pin one shared vector; SDKs reject stale/noncanonical inputs and return `(delivery ID, attempt)`. PR #74, exact-main CI `32365193683`, and Pages `32365193694` cover public-only HTTPS, explicit loopback development, mixed/special-address rejection, DNS pinning, redirect/proxy suppression, invalid headers, bounded/redacted keys, 503 retry, 204 Ack, convergence, reopen, and published receiver guidance. Pending: receiver-owned durable replay-store reference, key hot reload/secret manager, network egress proxy, formal security review/fuzzing, and production penetration report |
| BUS-012 | P1 | Authenticated API destinations | M4 | Slice | G5, G7 | API destinations support API-key and OAuth2 client-credentials references through a strict bounded external secret store; function and connector resources may additionally use bearer references. OAuth retrieval/caching and target delivery share safe DNS-pinned, no-redirect/no-proxy egress and stable idempotency. External secret-manager integration, hot reload, private egress, and broader OAuth flows remain open. |
| BUS-013 | P2 | Global endpoint health/failover | M6 | Slice | G8, G9 | Replicated endpoint observations select deterministic healthy priority/region routes; actual egress failure commits unhealthy state before a later failover attempt. Local real-target tests pass. Active probing/automatic restoration and multi-region drills remain open. |
| BUS-014 | P2 | Owner/schema/lineage event catalog | M6 | Slice | G5, G7, G8 | Bounded replicated owner/schema/source/consumer/classification/sample metadata, revisions, deterministic search, snapshot validation, and integration-state reads pass locally. Dedicated catalog query UI/API authorization and lineage governance remain open. |
| BUS-015 | P1 | Function and managed-connector targets | M4 | Slice | G5, G7, G8 | Replicated function/connector revision and status, identities, allowlists, secret refs, partial record outcomes, replay intent, secret versions, crash-idempotent batch checkpoints, leader-owned target execution, and settlement ordering pass locally. Automatic external source polling, marketplace lifecycle, and production connector certification remain open. |

## Managed Platform

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| MGD-001 | P1 | Serverless and dedicated choices | M4 | Planned | G4, G5, G8, G10 | Pending: topology/semantic/isolation matrix |
| MGD-002 | P0 | Automatic placement and online rebalance | M1 prototype → M2 | Slice | G2, G3, G5 | The consensus-backed Rust Catalog allocates stable tablet/group identities and explicit three/five-voter placements across 3–1,024 physical nodes; each bounded supervisor materializes only assigned or transition-target groups. A committed plan replaces one voter through learner catch-up, joint consensus, Catalog finalization, removed-node shutdown, and durable reopen. Go validates mTLS-protected region/zone/class/capacity and reports current/target placement without consuming customer generations. One clean local four-node Kubernetes replacement passes after backup compaction. Pending: transactional multi-request reservation, rack-aware solving, automatic multi-tablet transfer/rebalance, protected Kubernetes evidence, and production chaos evidence. |
| MGD-003 | P1 | Policy-bound multidimensional autoscaling | M4 | Planned | G5, G8 | Pending: hysteresis/headroom load report |
| MGD-004 | P0 | Multi-zone replicas and failover | M1 prototype → M2 | Slice | G2, G3, G5 | Bounded evidence includes N-node physical capacity, independent three/five-voter profile placements, policy and mTLS-protected topology, simultaneous Cache/Stream/Queue/Bus groups, EPRS persistence, leader replacement, truthful degradation, entry/snapshot catch-up, minority non-commit, all-voter `SIGKILL` replay, and a real four-node joint learner replacement with data continuity and reopen. Pending: rack constraints, public durability policy, broader fault matrix, protected/live-cluster evidence, and failover SLO report. |
| MGD-005 | P1 | Geo DR, switch, promotion, failback | M4 → M5 | Planned | G3, G8, G9 | Pending: RPO/RTO and split-brain drill |
| MGD-006 | P1 | Backup, validation, semantic PITR | M3 | Slice | G2, G5, G7, G8 | The alpha-exit branch captures one versioned/bounded/checksummed Catalog-plus-tablet artifact from distributed leaders, restores all five profiles into fresh journals, wraps managed objects in AES-256-GCM, publishes atomically, authenticates retention candidates, schedules a non-overlapping operator CronJob, records exact status, and renders an immutable creation-time restore. Unit, seven-node, all-profile restore/reopen, tamper, fake-Kubernetes, and one clean live-Kubernetes backup/fresh-restore/digest/post-write campaign pass locally. Pending: log-based semantic PITR, external destinations/KMS, measured RPO/RTO, protected CI, and release publication. |
| MGD-007 | P1 | Guarded rolling upgrades | M5 | Slice | G3, G5, G6, G8, G10 | A data-node image change is now a restart-safe CR-status plan gated by a post-request encrypted backup, all-node mTLS inventory, stable three/five-voter membership, all-voter apply/replication evidence, term-fenced leader drain, explicit StatefulSet partition, exact-image readiness, and postflight verification before each lower ordinal. Failure stops and enters one-node-at-a-time stable-image rollback; strict receipts, stale epoch/term, lagging learner, transition ordering, and partition freeze pass locally. Pending: adjacent-version capability negotiation, real mixed-version Kubernetes upgrade/rollback under traffic, load/SLO stops, protected CI, and release evidence. |
| MGD-008 | P0 | Unified workload identity and authorization | M2 baseline → M4 | Slice | G0, G5, G6 | A shared strict fingerprint-only bootstrap policy authenticates Go HTTP/gRPC, Go-to-Rust workload calls, and Rust regional HTTP; explicit actions including cluster-scoped `backup.create` plus tenant scopes fail closed and one Go/Rust corpus prevents evaluator drift. The supported operator path adds mandatory role-scoped TLS Secrets and peer/control mTLS. Pending: OIDC, short-lived/revocable credentials, certificate issuance/revocation and subject-role policy, replicated policy/ACLs, compatibility-protocol mapping, and the full authorization differential matrix. |
| MGD-009 | P1 | Private ingress and controlled egress | M4 | Planned | G5, G8 | Pending: cloud connectivity/isolation report |
| MGD-010 | P0 | Transit/at-rest encryption and managed keys | M4 | Slice | G2, G5, G8 | Mandatory public TLS and peer/control mTLS are rendered for the operator deployment; Go/Java/Python/CLI support strict custom trust roots and client identities. Managed semantic backups use authenticated AES-256-GCM with mounted 32-byte keys and public key IDs. Pending: certificate issuance/rotation evidence, WAL/data-volume encryption, external KMS/customer-managed rotation drill, live handshake campaign, and security review. |
| MGD-011 | P0 | Immutable audit and history export | M1 basics → M4 | Slice | G2, G5, G8 | Go and Rust emit bounded structured authentication/authorization decisions with request/principal/policy/action/decision/reason/scope fields and no credential/payload field; pending: durable append-only storage, required-event completeness, tenant access history, integrity/retention, export, and delivery-failure reconciliation |
| MGD-012 | P0 | Telemetry, dashboards, alerts, OTel | M1 basics → M2 | Slice | G1, G5 | Authenticated topology now exposes node-local regional maintenance interval/pass/tablet/leader/due/submitted/pending/error/last-pass observations, and the three-node campaign asserts submissions advance with zero errors. Pending: exported metrics/OTel, profile golden signals, dashboards, alerts, retention, cardinality controls, and fault suite |
| MGD-013 | P1 | Metering, budget, quotas, anomaly alerts | M4 → M5 | Planned | G5, G8 | Pending: raw-usage/billing reconciliation |
| MGD-014 | P0 | CLI, core SDKs, emulator, operator | M1 core → M2 | Slice | G1, G5, G10 | Go, Java, and Python regional SDKs cover all four native profiles and strict TLS configuration; the generated-contract Go CLI provides management lifecycle, diagnostics, and custom-CA/client identity; and the leader-elected Go operator reconciles N physical nodes, three/five-voter Catalog placement, durable control, Services/PVCs, mandatory trust material, scheduled encrypted backup, fresh-cluster restore, guarded partitioned data-node upgrades, hardened Jobs/pods, and observed current/target membership status. Rust automatically executes a committed single-voter plan through learner promotion/removal. Fake-Kubernetes, manifest, Rust replacement/reopen, restore, focused upgrade-ordering, and one clean live-cluster replace/upgrade/restore campaign pass locally. Pending: published artifacts, protected live evidence, policy-driven automatic rebalance, and a complete emulator command surface. |
| MGD-015 | P2 | Connector marketplace and lifecycle | M6 | Planned | G5, G7, G8, G10 | Pending: install/upgrade/provenance suite |
| MGD-016 | P1 | Customer-managed key rotation | M4 | Planned | G5, G8 | Pending: revoke/rotate/recover drill |
| MGD-017 | P1 | Terraform provider | M3 | Planned | G1, G5, G10 | Pending: plan/apply/import/drift suite |

## Control Plane

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| CTRL-001 | P0 | Idempotent declarative resource API | M1 → M2 | Slice | G0, G1, G3 | Generated RegionalAdmin gRPC Apply/Get/List/Delete, scoped bootstrap authorization, transactionally persisted apply/delete outcomes, exact replay across Go `SIGKILL`, token-rebinding rejection, Rust catalog replay, disconnect/reconnect, conflict mapping, and authenticated real Go-to-Rust apply pass; pending: long-running operation lookup, multi-instance ownership, and an advertised unknown-outcome/token-retention report |
| CTRL-002 | P0 | Strong versioned metadata and OCC | M1 prototype → M2 | Slice | G0, G2, G3 | Monotonic Rust and Go generations, expected-generation conflicts, durable Go/Rust tombstones, generation-fenced status, browser-safe desired/observed generations, version/corruption fail-closed tests, and single-owner metadata recovery pass; pending: multi-instance linearizability, management leader election, watch/resume, migration/backup policy, and broader metadata recovery report |
| CTRL-003 | P0 | Placement/residency/tenancy constraints | M2 | Slice | G0, G3, G5 | `ResourceSpec` supports allowed regions, minimum zones, and required node class; Go validates every authenticated fixed voter before Rust catalog mutation and publishes achieved topology. Pending: dedicated tenancy, rack constraints, residency export enforcement, general solver, and policy inheritance |
| CTRL-004 | P0 | Safe topology and repair operations | M1 prototype → M2 | Slice | G2, G3, G5 | Term-fenced leader transfer plus a v5 Catalog-planned, durable, idempotent single-voter transition enforce add-learner, leader-observed catch-up, joint consensus, finalize, stop, and reopen ordering. Go status distinguishes current/bootstrap/target/committed/reachable voters and preserves customer generations. A local live-Kubernetes replacement and guarded rollout pass. Pending: split/merge, automatic repair/rebalance planning, multi-plan reservation, protected evidence, and broader chaos. |
| CTRL-005 | P0 | Safe admission and limiting-resource reason | M2 | Slice | G3, G5, G8 | Rust reports maximum/used/available group slots; Go charges only additional shards, returns the stable limiting-node capacity reason, and unit/container tests prove rejection occurs before catalog apply. Pending: transactional reservations, multidimensional CPU/memory/disk/network sizing, concurrent-controller safety, and saturation report |
| CTRL-006 | P1 | Change plan, approval, rollback | M4 | Planned | G3, G5, G8 | Pending: preview/apply/rollback audit suite |
| CTRL-007 | P1 | Versioned common resource templates | M3 | Planned | G0, G1, G5 | Pending: template golden manifests |
| CTRL-008 | P1 | Organization policy guardrails | M4 | Planned | G3, G5 | Pending: inherited-policy allow/deny matrix |

## Schemas, Transformations, and Connectors

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| INT-001 | P1 | Three schema formats and compatibility | M3 | Complete | G0, G7 | Official Avro parsing/reader-writer compatibility, JSON Schema meta-validation/payload compilation with conservative fail-closed compatibility, and self-contained proto2/proto3 descriptor compilation/canonical JSON validation pass malformed-definition, payload, adjacent-revision, root-message, snapshot, and recovery tests. External JSON references and Protobuf imports are deliberately unsupported. |
| INT-002 | P1 | Producer/broker validation modes | M3 | Complete | G5, G6, G7 | Disabled, producer, broker, and combined policies pass a mode-separation matrix. Go, Java, and Python expose typed registration/policy/removal plus linearizable explicit validation; publish commits deterministic broker rejection, and bounded errors do not reflect payload values. |
| INT-003 | P1 | Declarative field transforms | M4 | Planned | G0, G7 | Pending: transform golden/property suite |
| INT-004 | P2 | Resource-bounded transform sandbox | M6 | Planned | G5, G7 | Pending: escape/exhaustion security report |
| INT-005 | P1 | Source/target-aware connector checkpoints | M4 | Slice | G2, G7 | Managed targets commit checkpoint-before-source-settlement. HTTP opaque cursors, immutable object identity, PostgreSQL commit LSNs, MySQL binlog file/positions, and Kafka next offsets all pass through one record-before-checkpoint pipeline. Stable source/batch/index identities make crash-before-checkpoint replay duplicate-safe; PostgreSQL/Kafka acknowledge upstream only afterward. Real-process HTTP recovery and local pinned protocol conformance pass. Pending: broader crash-at-every-boundary and sustained failover histories. |
| INT-006 | P1 | Record errors, partial batch, replay/backfill | M4 | Slice | G4, G7 | Replicated connector state records per-record applied/error-routed outcomes, partial-result metadata, explicit replay intent, and bounded checkpoint history. HTTP rejects malformed/duplicate/oversized batches; immutable objects, raw CDC transactions, and Kafka records convert malformed/oversized input into stable error records without advancing past an incomplete database transaction. Pending: managed backfill orchestration, exhaustive mixed-result failover, and load certification. |
| INT-007 | P1 | Rotatable references and connector egress policy | M4 | Slice | G5, G7 | Source and target workers resolve bounded typed node-local secret references without replicating values. HTTP enforces allowlists, public-address DNS pinning, and no redirects/proxies; database/broker adapters default to verified TLS, restrict plaintext to explicit loopback development, require configured hosts in the allowlist, and close stateful sessions on lost eligibility. Pending: external secret-manager hot rotation, cloud workload identity, private managed egress identities, revocation drills, and an independent abuse report. |
| INT-008 | P1 | Initial storage, CDC, Kafka, HTTP connectors | M4 | Slice | G5, G6, G7, G8 | Leader-owned HTTP/CloudEvents, S3-compatible/Azure/GCS immutable-object, PostgreSQL logical-replication, MySQL row-binlog, and Kafka source adapters plus managed HTTP targets are implemented behind bounded source-reader contracts. Deterministic tests and a pinned MinIO/PostgreSQL/MySQL/Kafka Compose matrix exercise ordering, cursors, credentials, real reads, post-checkpoint acknowledgement, and session release locally. Pending: live Azure/GCS cloud IAM, production load/soak/security certification, broad network crash injection, and protected release evidence. |
| INT-009 | P2 | Warehouse/search/analytics/bus connectors | M6 | Planned | G5, G7, G8 | Pending: marketplace conformance pack |

## Developer Experience

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| DX-001 | P0 | Official Go, Java, and Python SDKs | M1 one SDK → M2 | Slice | G0, G1, G4, G10 | Go/Java/Python standalone tests and exact-source restart quickstarts remain green. All three share regional Stream, Queue, Event Bus, and the complete non-deferred Cache lifecycle: ordinary/advanced state, atomic batch, independent multiplex, locks/TTL, changes, backup/PITR, typed query, lossy Pub/Sub, cold status, discovery/auth/fence/retry/read-barrier contracts. Stream additionally exposes keyed routing, batches, sessions, and claim/fenced fetch. The real Python campaign executes all four profiles after leader loss. Pending: generated response types, persistent native streaming/cooperative revoke, package publication, and the complete native contract/version matrix |
| DX-002 | P0 | Generated guarantee-aware API docs | M1 → M2 | Slice | G0, G1, G10 | Hand-authored guarantee/error guidance, exact executable standalone quickstarts, and exact compilable regional Stream/Queue/Cache/Event Bus Go/Java/Python sources are built into the docs-only Pages artifact. The compatibility page adds the bounded Redis/Kafka/AMQP architecture, security boundary, exact version targets, supported and unsupported surfaces, and end-to-end standard-client examples. ADR-0034 plus the Cache guide/examples cover the complete non-deferred state/recovery/query/SDK contract and non-claims; main-only compatibility publication is pending. Pending: generated API reference and full doc lint |
| DX-003 | P0 | Deterministic single-binary emulator | M1 → M2 | Slice | G1, G2, G4, G10 | Seeded scheduler, virtual clocks/fault plan/transport, golden EPTR history, fixed-voter consensus, real-process EPRS/SIGKILL, and typed Stream/Queue/Cache/Bus runtimes; pending: executable replay bundle and runnable emulator controls |
| DX-004 | P0 | Test containers and ephemeral namespaces | M1 → M2 | Slice | G1, G5, G10 | A unique three-node Compose project uses independent volumes, dynamic loopback ports, mounted policy, Go recovery, simultaneous four-profile failover/catch-up/all-node recovery, and post-failover Python lifecycles. The alpha-exit runner makes that real campaign resumable across exact-identity rounds, requires typed profile/fault/invariant receipts, hashes every artifact, duration-gates completion, signs canonical evidence with Ed25519, and passes one 48,434-ms accelerated round locally. A separate digest-pinned Kind campaign passes four-node mTLS install, all-profile traffic, backup, compacted-log voter replacement, guarded rollout, fresh restore, exact digests, and post-restore writes locally. Pending: protected evidence, elapsed 30-day operation, parallel campaigns, Go/Java live regional execution, and injected disk/network matrices. |
| DX-005 | P1 | Audited/redacted console message browser | M3 → M4 | Planned | G5, G7, G8 | Pending: access/redaction/action audit matrix |
| DX-006 | P0 | Explain live guarantees and cost drivers | M1 basic → M2 | Slice | G0, G3, G5 | The TypeScript console consumes only the Go BFF with an interactively entered session-only bearer, displays desired versus observed generation, per-shard voters/leader, verified region/zone/class evidence, per-node group capacity, exact governance filters, and authorized resource/shard attribution by cost center and classification. Pending: OIDC/session exchange, achieved durability, usage/rate/currency inputs, browser accessibility evidence, and historical topology context |
| DX-007 | P1 | Compatibility usage scanner | M3 | Slice | G0, G6 | `epoch-compat scan` accepts bounded auto/fixed-protocol newline manifests, preserves source lines, evaluates Kafka API versions and Redis option/database boundaries, emits the versioned `epoch.compatibility-scan/v1` text/JSON report, and fails CI at partial/unknown/unsupported thresholds. The unsupported/unknown fixture corpus passes locally; live traffic capture and protected evidence remain open. |
| DX-008 | P1 | End-to-end event trace | M4 | Planned | G1, G4, G5, G7 | Pending: trace/history reconciliation |
| DX-009 | P1 | TypeScript, Rust, .NET SDKs | M3 | Planned | G0, G1, G6, G10 | Pending: multi-language client matrix |

## Lifecycle and Governance

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| GOV-001 | P1 | Recoverable delete and explicit purge | M4 | Planned | G2, G5, G7, G8 | Pending: recovery/purge completeness drill |
| GOV-002 | P2 | Legal hold and retention lock | M6 | Planned | G2, G5, G7 | Pending: non-bypass and audit review |
| GOV-003 | P1 | Payload/field redaction hooks | M4 | Planned | G5, G8 | Pending: restricted-data leakage corpus |
| GOV-004 | P1 | Residency policy and region allowlist | M4 | Planned | G3, G5, G9 | Pending: placement/export enforcement suite |
| GOV-005 | P0 | Ownership, cost, classification, tags | M2 | Complete | G1, G3, G5 | New managed resources require canonical owner/cost center/classification/tags; Go durable state, Protobuf/HTTP, Rust catalog command/snapshot v3, exact AND filters, authorized deterministic cost-driver aggregation, console controls, legacy recovery, and the real control/data-plane restart campaign are covered by ADR-0033 and the resource-governance guide. ABAC, metering, rates, and billing are separate requirements |
| GOV-006 | P0 | Exportable sensitive-action audit trail | M1 basics → M4 | Slice | G2, G5, G8 | Pending: event-matrix/export reconciliation |

## Packaging and Runtime

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| PKG-001 | P0 | Selective four-profile Rust node | M1 scaffold → M4 complete | Slice | G1, G4, G10 | One binary retains all four standalone profiles, supports the earlier mutually exclusive single-group modes, and now runs Catalog plus simultaneous Cache, Stream, Queue, and Event Bus groups through one bounded regional supervisor. Transition-target nodes materialize as learners and removed nodes stop after Catalog finalization. Pending: production role/profile selection, resource budgets, complete feature/config startup matrix, and protected/live replacement evidence. |
| PKG-002 | P0 | Shared engine/format standalone and cluster | M1 → M2 | Slice | G1, G2, G3, G10 | Checksummed segmented standalone format plus canonical typed Stream, Queue, Cache, and Event Bus commands applied from EPRS without a second clustered WAL; pending: supported standalone-to-cluster format/migration equivalence |
| PKG-003 | P0 | Standalone without hosted Go services | M1 | Slice | G1, G2, G10 | Rust node restart/recovery test; pending: extended disconnected lifecycle suite |
| PKG-004 | P0 | Three-node quorum/failover/placement | M1 prototype → M2 | Slice | G2, G3, G10 | Deterministic, real-process, and three-container evidence covers a dedicated Catalog, authenticated zones, live group capacity, independently replicated shards and profiles, generation/epoch fencing, majority commit, leader loss, Go-observed degradation, entry/snapshot catch-up, and same-volume all-node recovery. The alpha-exit four-node campaign adds explicit one-voter selection, learner catch-up including refreshed snapshots after compaction, joint consensus, data continuity, removed-host shutdown, reopen on the new voter set, and one clean live-Kubernetes lifecycle locally. Pending: rack placement, stable public repair APIs, exhaustive faults, protected Kubernetes evidence, and a published SLO report. |
| PKG-005 | P0 | OCI, Kubernetes dev, signed binaries | M1 dev → M2 | Slice | G1, G5, G10 | Five digest-base-pinned, exact-version, non-root node/control/operator/CLI/compatibility Dockerfiles build and pass strict local inspection. PR CI builds without publishing and generates structurally validated SPDX JSON evidence. The exact-main tag workflow builds Linux amd64/arm64 concurrently on matching native runners, transfers only immutable digests, assembles exact tags in a bounded finalize stage, attaches manifest provenance plus per-platform SBOM attestations, keylessly signs/verifies each manifest, and retains ten release SBOMs. ADR-0041 and the release-artifact guide freeze consumer verification. Pending: protected/tag execution, clean digest-pull Kubernetes evidence, raw signed binary archives, and wider install/vulnerability/reproducibility matrices. |
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
