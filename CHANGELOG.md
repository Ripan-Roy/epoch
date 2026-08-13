# Changelog

Epoch uses prerelease versions while its APIs, storage formats, and operational
contracts remain provisional. GitHub releases are source previews unless their
notes explicitly list additional verified artifacts.

## Unreleased

### Added

- Regional Go, Java, and Python Stream clients now expose a single-shard atomic
  batch operation over the existing authenticated and fenced v1 route. Exact
  frame bytes and caller idempotency survive one bounded leader rediscovery.
- Dependency-free canonical none/gzip batch encoders plus typed exact-frame
  constructors for LZ4, Snappy, and Zstd. Cross-language tests cover canonical
  field order, recursively sorted object maps, Unicode, sequence uniqueness,
  metadata consistency, and size bounds before network I/O.
- ADR-0026, end-to-end SDK/API/runtime/testing guidance, exact published
  Go/Java/Python gzip examples, and a real Python post-leader-loss batch
  campaign that proves correlated replay through voter catch-up and all-node
  same-volume reopen.
- Additive Stream command v5 and native snapshot v2 for a shard-zero consumer
  coordinator. Bounded join/rejoin, generation-fenced heartbeat/leave,
  monotonic committed time, inclusive dead-member expiry, and lexical
  round-robin assignment over the captured resource shard count survive
  fixed-voter leader loss, native checkpoint installation, and reopen; legacy
  snapshot v1 remains readable with no session state.
- Strict direct and authenticated regional session join, observe, heartbeat,
  leave, and maintenance routes plus matching Go, Java, and Python
  `RegionalStreamClient` methods. Contract tests, exact Pages examples, a
  three-voter convergence/checkpoint test, and a two-member Python failover and
  all-node-recovery campaign cover the implemented lifecycle. Background
  expiry, cooperative revoke, and atomic assignment-plus-offset handoff remain
  explicit non-claims.
- ADR-0025 and end-to-end PRD, API, architecture, semantics, runtime, testing,
  traceability, delivery-checklist, SDK, and published-doc updates for
  coordinated consumer sessions.
- Regional multi-shard Stream materialization with one independently replicated
  ordered tablet per logical partition, resource-wide shard-count discovery,
  truthful logical shard identities in mutation, record, checkpoint, retention,
  batch, and status responses while retaining canonical partition-0 command
  identity.
- A versioned `fnv1a64_utf8_mod_n_v1` cross-language partition contract over
  exact UTF-8 bytes with event-ID fallback. Go `StreamShardFor`/`AppendKeyed`,
  Java `StreamPartitioner`/`appendKeyed`, and Python
  `stream_shard_for`/`append_keyed` pin the initial resource generation and
  fail before write if expansion races target discovery.
- ADR-0024, multi-shard API/architecture/semantics/SDK documentation, exact
  three-language keyed Pages quickstarts, shared ASCII/non-ASCII vectors, and a
  real three-node Python campaign that routes shards 0/1/2 through leader loss,
  voter catch-up, and all-node same-volume recovery.
- Additive Stream command v4 for deterministic record-count, compact canonical
  JSON byte, inclusive-age, and combined retention. Configure, append, and
  explicit idle maintenance advance one replicated monotonic watermark while
  preserving v1/v2/v3 command bytes and never renumbering retained offsets.
- Retention-aware consumer observations preserve stale checkpoints, report
  `checkpoint_out_of_range`, clamp lag to readable retained records, and require
  an explicit generation-fenced reset before replay. Native checkpoints restore
  the exact policy, retained base, dedupe state, and time watermark.
- Strict direct and authenticated regional retention configure, maintain, and
  linearizable observe routes, with matching Go, Java, and Python clients,
  executable Pages quickstarts, real three-voter checkpoint/reopen tests, and a
  Python-driven regional container campaign.
- Additive EPSN v2 native-profile checkpoints for Catalog, Stream, Queue,
  Cache, and Event Bus, with canonical scope/configuration validation, rolling
  consensus digests, a bounded 1,024-record/1 MiB exact-retry suffix, and a
  SHA-256 digest over the complete at-most-6-MiB frame.
- Crash-safe physical EPRS reclamation through an additive compacted-baseline
  record and locked sibling-WAL replacement, plus automatic profile restore
  before retained-tail application in every real three-voter profile restart.
  This is internal voter recovery, not a downloadable backup, PITR, scheduled
  restore, dynamic membership, or production repair workflow.
- Canonical bounded EPSN v1 consensus checkpoints embedded in additive EPRS
  records, with fsync-before-install ordering, logical Raft-prefix compaction,
  checkpoint-plus-tail reopen, exact proposal retry preservation, and
  fail-closed metadata/digest validation.
- Snapshot-based catch-up for a lagging fixed voter, including typed profile
  replay before committed-tail application, real three-runtime HTTP evidence,
  local checkpoint status, and an experimental checkpoint trigger. EPSN v2
  extends this foundation without turning it into a backup/PITR product.

- A strict version-one bootstrap identity policy shared by Go and Rust, with
  SHA-256 token fingerprints, explicit actions, hierarchical tenant scopes, a
  JSON Schema, and a cross-language decision corpus.
- Deny-by-default bearer authentication and scoped authorization for the Go
  managed HTTP/gRPC APIs, the Go-to-Rust authority path, and Rust regional
  catalog/route/data endpoints. Collection reads filter unauthorized tenant
  records.
- Bounded structured authorization decisions with request, principal, policy,
  action, decision, reason, and scope fields but no credential or payload
  field.
- A session-only managed-control credential flow in the console, authenticated
  real-process/container recovery tests, ADR-0011, and end-to-end operator
  documentation.
- A policy-protected Rust topology endpoint that reports configured
  region/zone/class, the peer-derived fixed voter set, and live
  consensus-group capacity.
- Topology-aware Go admission for allowed regions, minimum zones, required node
  class, and incremental shard capacity, with stable limiting-node rejection
  before Rust catalog mutation.
- Generated Protobuf and browser-safe status for requested versus achieved
  placement, plus console evidence for verified configured zones and per-node
  group capacity.
- Safe Raft `ReadIndex` barriers with leader/term fencing, bounded pending
  requests, cancellation, real-majority tests, and local profile-apply proof.
- Linearizable-by-default regional reads for all typed GETs and Event Bus query
  POSTs, with explicit `local_stale` opt-in, exact barrier evidence, bounded
  timeout configuration, and no silent consistency downgrade.
- Replicated Queue consumer flow control with bounded request credit,
  per-consumer in-flight windows across consumer epochs, exact saturation and
  remaining-capacity evidence, and an applied-state consumer-flow read.
- Additive Queue command format v2 for flow-controlled acquire while every
  legacy operation remains canonical v1, plus deterministic, real-three-node,
  and container coverage for saturation and settlement replenishment.
- Additive Stream command format v2 for atomic 1–1,000-record batches through
  none, gzip, LZ4-frame, Snappy-framed, and Zstd-frame payloads, with canonical
  input, bounded decompression, per-client-sequence offsets, exact retry, and
  unchanged v1 command/digest evidence.
- Strict direct/regional Stream batch routes, advertised codec/size limits,
  all-codec real-three-runtime recovery tests, and a container failover/restart
  campaign using an independently generated Python gzip frame.
- Additive Stream command format v3 for replicated partition-0 consumer-group
  next offsets, monotonic commit, explicit reset, caller-generation ownership
  fencing, and typed committed business rejections without changing v1/v2
  bytes or digest evidence.
- Strict direct/regional consumer-group mutation, lag, and checkpoint-replay
  routes with browser-safe observations, real-three-runtime convergence/EPRS
  rebuild, and container failover plus all-voter `SIGKILL` recovery evidence.
- A fully qualified regional Stream v1 route with strict tenant-scoped
  authentication, route/data authorization, generation/tablet fencing,
  leader-only mutation admission, and linearizable fetch/lag reads over the
  existing replicated tablet.
- Separate Go, Java, and Python `RegionalStreamClient` implementations for
  leader discovery, single-record append, bounded fetch, checkpoint
  commit/reset, lag, and checkpoint replay. All preserve caller idempotency
  across bounded rediscovery and have exact compilable Pages examples.
- ADR-0017 and an end-to-end regional SDK guide embedded in the public docs;
  the container campaign runs the Python SDK after leader loss and through
  subsequent all-voter recovery.
- A fully qualified regional Queue v1 route over the existing replicated tablet,
  with strict scoped route/data authorization, generation/tablet fencing,
  leader-only mutation admission, and linearizable counts, mutation, history,
  flow, and status reads.
- Complete repository-local Go, Java, and Python `RegionalQueueClient`
  lifecycle surfaces for enqueue, credit acquire, acknowledgement, renewal,
  release, Nack, Reject, maintenance, dead-letter/redrive history, redrive,
  mutation lookup, counts, consumer flow, and status. Shared private regional
  cores keep Stream, Queue, Cache, and Event Bus discovery/retry rules identical.
- ADR-0018, an end-to-end regional Queue SDK guide, exact three-language Pages
  examples, and a real post-leader-loss Python Queue lifecycle followed by
  survivor convergence, voter catch-up, and all-voter recovery.
- A fully qualified regional Cache v1 route over the existing replicated
  tablet, with strict scoped route/data authorization, generation/tablet
  fencing, leader-only mutation admission, and linearizable observation,
  mutation-lookup, and status reads.
- Complete repository-local Go, Java, and Python `RegionalCacheClient`
  surfaces with strict constructors for all seven value kinds, version and
  missing-at-revision CAS, set/delete/increment, bounded atomic transactions,
  fenced lock acquire/renew/release, explicit expiry maintenance, and exact
  same-key rediscovery.
- ADR-0019, an end-to-end regional Cache SDK guide, exact three-language Pages
  examples, and a real post-leader-loss Python Cache lifecycle covering all
  value kinds, CAS, transaction, fenced locks, expiry, survivor convergence,
  voter catch-up, and all-voter recovery.
- A fully qualified regional Event Bus v1 route over the existing replicated
  tablet, with strict scoped route/data authorization, generation/tablet
  fencing, leader-only mutation admission, linearizable archive, delivery,
  mutation, and status reads, and delivery outbox materialization enabled.
- Complete repository-local Go, Java, and Python `RegionalBusClient` lifecycle
  surfaces for subscription policy and removal, publish, delivery
  acquire/ack/fail/maintenance, mutation lookup, archive replay, delivery query,
  and status. Bounded rediscovery preserves exact caller-owned identities and
  opaque lease tokens.
- ADR-0020, an end-to-end regional Event Bus SDK guide, exact three-language
  Pages examples, and a real post-leader-loss Python lifecycle covering exact
  publish, archive, retry, settlement, query, survivor convergence, voter
  catch-up, and all-voter recovery.

### Fixed

- Regional dispatch now clears cached outer-router path parameters before
  invoking the profile router. Without that request boundary, parameterized
  Stream group routes could fail immediately with HTTP 500 even though
  authentication, leadership, and replication were healthy.

### Limitations

- The bootstrap policy uses public long-lived development fixtures in the
  checked-in examples. OIDC, expiry/revocation, TLS/mTLS and peer identity,
  replicated/hot-reloaded policy, encryption, immutable audit export, quotas,
  and production secret delivery remain unimplemented.
- The standalone local-emulator API and consensus peer listener remain
  unauthenticated.
- Placement remains the immutable three-voter set. There is no rack-aware
  general solver, dynamic membership, transactional multi-controller
  reservation, multidimensional capacity model, online transfer, repair, or
  rebalance. Plain HTTP also does not authenticate the Rust server to Go.
- Linearizable reads are leader-only and limited to the experimental regional
  surface. Direct profile routes remain stale-capable; follower forwarding,
  dynamic membership, and cross-tablet read transactions remain unimplemented.
- Queue receive is currently an alpha HTTP request/response slice, even though
  the full implemented tablet lifecycle now has a versioned regional route and
  repository-local SDK coverage. Native
  bidirectional receive, connection-scoped credit, automatic prefetch,
  cross-consumer fairness, indexed backlog-scale counting, generated response
  models, and package publication remain unimplemented.
- Stream batching/compression is currently an experimental single-partition,
  whole-command-atomic HTTP/tablet and repository-local regional SDK slice.
  Stable bidirectional Produce,
  automatic producer batching and codec negotiation, non-atomic partial
  results, compression dictionaries, and fuzz/load benchmarks
  remain unimplemented.
- Replicated Stream consumer groups provide independent per-shard checkpoint
  storage plus a shard-zero session coordinator for join, heartbeat, leave,
  explicit dead-member expiry, automatic membership generations, and
  deterministic resource-wide assignment. Background expiry, cooperative
  revoke acknowledgement, sticky/rack-aware strategies, native streaming
  consumption, atomic assignment-plus-offset handoff, transactional offsets,
  scale/fairness evidence, generated response types, and production fault
  coverage remain unimplemented. Regional retention policy is independent per
  logical shard; automatic maintenance scheduling, keyed
  compaction/tombstones, object-tier retention, and legal-hold governance
  remain open. Standalone offset helpers keep their local contract.
- The regional SDKs remain repository-local alpha source. Package publication,
  generated response models, Event Bus
  external webhook/HTTP/push execution and signing, Cache background
  expiry/eviction/multi-shard routing, Stream online expansion/remapping and
  virtual shards, TLS/OIDC/mTLS, dynamic membership, and live-cluster execution
  for every language remain open.

## [0.1.0-alpha.3] - 2026-07-29

This source prerelease adds the regional multi-tablet runtime, durable
single-owner Go control metadata, truthful browser placement, and complete
Go-to-Rust/Rust-process recovery evidence.

See the complete [v0.1.0-alpha.3 release notes](docs/releases/v0.1.0-alpha.3.md).

### Added

- Deterministic Rust regional catalog state machine for fully qualified
  resources, monotonic generations, canonical lifecycle commands, and stable
  shard-to-tablet routing identity.
- Versioned tablet descriptors in the regional Protobuf contract, including
  separate desired replicas and observed placement fields.
- A dedicated three-voter catalog consensus group, shared group/epoch peer
  transport, bounded multi-group supervisor, and catalog-driven simultaneous
  Cache, Stream, Queue, and Event Bus materialization in one Rust node process.
- Experimental resource/shard discovery and typed data dispatch with exact
  resource-generation and tablet-epoch fencing, nonleader rejection, decimal
  64-bit JSON, and deterministic route identity.
- A real Go `RegionalAdminService`, generation-fenced desired-state reconciler,
  multi-endpoint Rust authority adapter, idempotent apply/delete lifecycle, and
  exact-origin browser inventory BFF that reports only observed voters/leaders.
- A versioned, single-owner bbolt control registry that transactionally persists
  desired resources, observed status, request outcomes, and generation
  tombstones before acknowledgement; startup recovers exact replay state and
  fails closed for corruption, unknown schemas, or concurrent ownership.
- A regional console view for desired/observed generations, pending, ready,
  degraded, and failed state, per-shard voters/leaders, and explicit
  failure-domain non-claims. The console obtains regional state only through
  the Go BFF.
- Real-process and three-container campaigns covering several consensus groups,
  all four profiles, Go-to-Rust apply, Go control `SIGKILL`/same-file replay,
  leader `SIGKILL`, truthful degradation, catch-up, all-node `SIGKILL`,
  same-volume reopen, and digest convergence.
- A table-based delivery checklist covering program gates, milestone readiness,
  the active multi-tablet slice, pull-request quality gates, and releases.

### Fixed

- Go regional reconciliation now retries a catalog mutation against the other
  configured nodes when a follower returns `409 not_leader`, while preserving
  definitive generation conflicts. Regional container failures also retain the
  last browser inventory response for actionable CI evidence, and the
  real-process campaign re-discovers leadership after explicit transient
  `not_leader` or `stale_term` responses.

### Limitations

- Regional placement is fixed to three configured voters; zone/rack constraints,
  dynamic membership, online rebalance, repair, snapshots, compaction, and read
  barriers are not implemented.
- Regional Rust HTTP routes and the console/control surfaces are experimental
  and unauthenticated. Go metadata durability is single-process and
  single-owner; it is not replicated, multi-instance linearizable, or backed up
  automatically.

## [0.1.0-alpha.2] - 2026-07-27

This release adds deterministic replicated tablet slices for Stream, Queue,
Cache, and Event Bus workloads, durable fixed-voter recovery evidence, and the
redesigned executable SDK documentation experience.

See the complete [v0.1.0-alpha.2 release notes](docs/releases/v0.1.0-alpha.2.md).

## [0.1.0-alpha.1] - 2026-07-23

The first source preview established the Rust/Go/TypeScript workspace,
standalone profiles, local durable Stream and Queue storage, repository-local
Go/Java/Python SDKs, deterministic testkit, fixed-voter consensus probe, CI,
development containers, and main-only documentation deployment.

[0.1.0-alpha.3]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.2...v0.1.0-alpha.3
[0.1.0-alpha.2]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.1...v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/Ripan-Roy/epoch/releases/tag/v0.1.0-alpha.1
