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
  dynamic membership, stable SDK exposure, and cross-tablet read transactions
  remain unimplemented.

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
