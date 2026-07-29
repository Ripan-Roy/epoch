# Changelog

Epoch uses prerelease versions while its APIs, storage formats, and operational
contracts remain provisional. GitHub releases are source previews unless their
notes explicitly list additional verified artifacts.

## Unreleased

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
- A regional console view for desired/observed generations, pending, ready,
  degraded, and failed state, per-shard voters/leaders, and explicit
  failure-domain non-claims. The console obtains regional state only through
  the Go BFF.
- Real-process and three-container campaigns covering several consensus groups,
  all four profiles, Go-to-Rust apply, leader `SIGKILL`, truthful degradation,
  catch-up, all-node `SIGKILL`, same-volume reopen, and digest convergence.
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
  and unauthenticated. Go desired state and replay metadata are in memory.

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

[0.1.0-alpha.2]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.1...v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/Ripan-Roy/epoch/releases/tag/v0.1.0-alpha.1
