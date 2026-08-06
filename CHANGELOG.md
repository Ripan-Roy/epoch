# Changelog

Epoch uses prerelease versions while its APIs, storage formats, and operational
contracts remain provisional. GitHub releases are source previews unless their
notes explicitly list additional verified artifacts.

## Unreleased

### Added

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
  whole-command-atomic HTTP/tablet slice. Stable bidirectional Produce,
  automatic producer batching and codec negotiation, non-atomic partial
  results, compression dictionaries, fuzz/load benchmarks, and SDK exposure
  remain unimplemented.
- Replicated Stream consumer groups currently provide checkpoint storage and a
  caller-supplied generation fence only. Join, heartbeat, assignment, revoke,
  dead-member detection, automatic generation allocation, rebalance,
  multi-partition ownership, transactional offset commits, retention
  interaction, scale/fairness evidence, generated coordinated-session types,
  and production fault coverage remain unimplemented. The regional clients
  expose only the explicit partition-0 checkpoint primitive; standalone offset
  helpers keep their local contract.
- The regional SDKs remain repository-local alpha source. Package publication,
  generated response models, Stream batch/compression helpers, Event Bus
  external webhook/HTTP/push execution and signing, Cache background
  expiry/eviction/multi-shard routing, TLS/OIDC/mTLS, dynamic membership, and
  live-cluster execution for every language remain open.

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
