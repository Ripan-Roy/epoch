# Changelog

Epoch uses prerelease versions while its APIs, storage formats, and operational
contracts remain provisional. GitHub releases are source previews unless their
notes explicitly list additional verified artifacts.

## Unreleased

## [0.2.0-beta.5] - 2026-08-24

### Added

- Added the separate `epoch-compat` Rust gateway with bounded RESP2/RESP3,
  Kafka broker-protocol, and AMQP 0-9-1 listeners over one authenticated,
  generation/tablet/term-fenced native semantic adapter.
- Added a documented Redis strings/counters/TTL subset, Kafka producer/manual
  consumer/metadata/checkpoint subset with gzip, Snappy, LZ4, and Zstandard,
  and RabbitMQ direct publish/confirm/push-or-pull consume/settlement subset.
- Added exact wire conformance for Redis CLI 8.8.2, Apache Kafka Java client
  4.3.1, and RabbitMQ Java client 5.34.0, plus parser/semantic/native-adapter
  tests and a dedicated protected CI job.
- Added `epoch-compat scan` with the versioned
  `epoch.compatibility-scan/v1` report, conservative support levels, source-line
  retention, and CI failure thresholds.
- Added the fifth exact-tag, non-root amd64/arm64 OCI component with PR SBOM
  inspection and tag-only provenance, per-platform SBOM, and signing coverage.
- Added ADR-0042, the exact public support matrix, runnable client examples,
  migration/security/retry guidance, and a visible documentation-site page.

### Security and correctness

- Kafka record counts are checked before allocation and all supported
  compression codecs share a cumulative 4 MiB expansion ceiling. Each Produce
  partition is translated into one canonical atomic native Stream batch.
- Runtime backend and AMQP secrets have no embedded defaults and are redacted
  from configuration debug output. Backend URLs reject credentials and path
  ambiguity, native HTTP bodies stream under a hard limit, and unsupported AMQP
  exchange semantics fail closed.

### Limitations

- Redis structures, transactions/scripts, Pub/Sub, Streams, and clustering;
  Kafka group membership, idempotence/transactions/admin/SASL; and broader AMQP
  routing, transactions, AMQP 1.0, and TLS termination remain unsupported.
- Combined exact-client conformance against a faulted real regional cluster,
  differential/fuzz suites, MQTT, comparative performance, and production SLO
  evidence remain beta promotion gates.

## [0.2.0-beta.4] - 2026-08-24

### Added

- Replaced Event Bus field-overlay-only schema checks with compiler-backed Avro,
  JSON Schema, and self-contained Protobuf definition validation and payload
  validation.
- Added format-derived adjacent-revision compatibility, explicit Protobuf root
  message selection, deterministic snapshot semantic revalidation, and bounded
  payload-safe rejection details.
- Added producer-advisory and broker-enforced validation modes through a
  linearizable regional route, deterministic committed publish rejection, and
  typed Go, Java, and Python schema lifecycle APIs.
- Added end-to-end SDK quickstarts, visible documentation-site guides, a real
  three-voter route/recovery test, and synchronized PRD, API, testing,
  traceability, and delivery evidence for `BUS-009`, `INT-001`, and `INT-002`.

### Limitations

- JSON Schema external references and Protobuf imports are deliberately
  unsupported. Advanced JSON Schema compatibility forms fail closed when Epoch
  cannot conservatively prove compatibility.
- This release does not add RESP3, Kafka broker, AMQP, or MQTT wire
  compatibility; those remain the next private-beta feature boundary.

## [0.2.0-beta.3] - 2026-08-24

### Fixed

- Replaced the tag workflow's emulated two-platform image builds with eight
  concurrent native builds: amd64 uses an x86_64 runner and arm64 uses
  GitHub's `ubuntu-24.04-arm` runner.
- Added digest-only platform handoff and a fail-closed manifest assembly stage,
  retaining BuildKit provenance, exact platform resolution, per-platform SPDX
  attestations, keyless manifest signing, and eight release SBOM assets.
- Added architecture-scoped GitHub Actions caches and an executable release
  workflow contract that rejects QEMU, mutable tags, missing native runners,
  or weakened release evidence.

### Release notes

- `v0.2.0-beta.2` passed protected source, exact-main, Pages, Kubernetes,
  connector, soak, and native-arm64 checks, but the tag workflow still compiled
  the arm64 Rust node through QEMU and reached its 90-minute timeout. The
  immutable tag was not moved and no GitHub prerelease was created. Beta.3
  supersedes that incomplete publication attempt without changing product
  scope.

## [0.2.0-beta.2] - 2026-08-24

### Fixed

- Made the node image use an explicit target-native Protobuf compiler, avoiding
  `protobuf-build`'s vendored compiler fallback under arm64 emulation.
- Added a native `linux/arm64` node-image build and OCI inspection to protected
  CI so both release architectures are proven before another tag is created.
- Made the accelerated regional campaign close each restarted voter's
  per-group recovery window before injecting the next leader fault, and report
  every node's last applied count and digest on a bounded recovery timeout.

### Release notes

- `v0.2.0-beta.1` passed source, exact-main, Pages, Kubernetes, connector, and
  amd64 image checks, but its tag workflow aborted while building the arm64 node
  image. Beta.2 repaired that compiler selection and passed a protected native
  arm64 image gate. Its tag publication nevertheless timed out because the
  separate release workflow still used QEMU for arm64 Rust compilation. The
  immutable beta.2 tag was not moved and no GitHub prerelease was created.

## [0.2.0-beta.1] - 2026-08-24

### Added

- Added a shared leader-owned source-reader architecture for bounded
  HTTP/CloudEvents, immutable S3-compatible/Azure/GCS objects, PostgreSQL
  logical replication, MySQL row binlogs, and Kafka consumer groups. Every
  adapter commits applied/error-routed records before its exact source cursor;
  PostgreSQL feedback and Kafka group offsets follow the Epoch checkpoint.
- Added typed, redacted node-local connector credentials, verified TLS/mTLS and
  allowlist policy, stateful session fencing, immutable-object overwrite
  detection, transaction-complete CDC positions, and Epoch-authoritative Kafka
  assignment seeks.
- Added deterministic adapter suites, a pinned live MinIO/PostgreSQL/MySQL/Kafka
  Compose conformance matrix and CI job, ADR-0040, a complete source operations
  guide, three-language SDK lifecycle calls, and visible documentation-site
  coverage.
- Added alpha-exit secure transport, workload-identity manifests, versioned
  regional backup/fresh-cluster restore, encrypted scheduled backups, guarded
  data-node upgrades, and learner-first joint-consensus voter replacement.
- Added digest-base-pinned, non-root node/control/operator/CLI images; strict PR
  inspection and SPDX evidence; exact-main, tag-only amd64/arm64 GHCR
  publication; per-platform SBOM and manifest provenance attestations; keyless
  Sigstore signing/verification; ADR-0041; and an end-user artifact verification
  guide. No mutable `latest` tag is published.
- Added a resumable four-profile load/fault/soak runner with atomic attempt
  checkpoints, exact source/image identity, typed fault/invariant receipts,
  SHA-256 artifact inventory, duration gating, accelerated CI coverage, and an
  Ed25519-signed canonical evidence manifest with independent tamper
  verification.
- Added a clean-build, digest-pinned four-node Kubernetes acceptance campaign
  covering mTLS install, all-profile traffic, encrypted backup, compacted-log
  voter replacement, the post-request backup upgrade gate, serialized rollout,
  fresh-cluster restore, exact source/restored digests, continued writes,
  SHA-256 evidence, cleanup, CI artifacts, and visible Pages documentation.

### Fixed

- Pinned Epoch's Rustls listener and client-certificate verifier to an explicit
  crypto provider so object/database connector dependencies cannot make mTLS
  startup ambiguous.
- Refreshed membership-bearing native snapshots after learner admission and
  reported older in-flight snapshots stale, allowing a newly assigned voter to
  catch up safely when a pre-replacement backup already compacted the log.

### Limitations

- Live Azure/GCS cloud IAM, private connector networking, secret-manager hot
  rotation, schema-aware decoded CDC records, exhaustive network crash
  injection, and sustained connector load/soak certification remain beta
  operating evidence. OCI/SBOM/provenance passes local image inspection but
  still requires protected/tag publication evidence. One exact-source local
  Kubernetes lifecycle passes, but protected/exact-main evidence, a genuinely
  mixed-version rollout, the actual 30-day soak, and the verified prerelease
  tag remain open.

## [0.1.0-alpha.10] - 2026-08-23

### Added

- Added active-leader HTTP/CloudEvents source connector polling with bounded
  strict batches, safe shared egress, stable per-record proposal identities,
  applied/error-routed outcomes, checkpoint-after-record ordering, observable
  counters, and a real three-process failover/reopen proof.
- Added a controller-runtime `EpochCluster` operator and Kustomize install for
  the fixed three-voter regional runtime: stable peer identity, PVCs, Services,
  required anti-affinity, explicit placement, hardened pods, referenced policy
  and credentials, leader election, drift repair, and observed status.
- Added a generated-contract Go `epoch` management CLI with strict JSON/YAML
  apply, fully qualified get/list/delete, automatic retry tokens, optimistic
  concurrency, stable protobuf JSON output, and HTTP plus authenticated-gRPC
  diagnostics.
- Added node/control/operator source-built OCI definitions, CI manifest and
  image gates, ADR-0038, Kubernetes/CLI/source runbooks, deployment-focused
  Pages documentation, updated traceability, and release notes.

### Limitations

- This remains a fixed-voter source alpha. TLS/mTLS/OIDC and workload identity,
  automated backup/restore and guarded rolling upgrades, dynamic membership,
  published OCI/package artifacts, CDC/Kafka/object-storage connectors, and
  production load/soak/fault certification remain beta gates.

## [0.1.0-alpha.9] - 2026-08-23

### Added

- Completed the native Event Bus development surface with committed rate/burst,
  bounded long poll, dead-letter retention/redrive, archive count/age
  retention, broader declarative transforms, and deterministic no-network
  enrichment.
- Added bounded replicated schema revisions/validation policies, MQTT session/
  retained/QoS/shared-route state, connector lifecycle/checkpoints/replay/
  partial errors, event catalog, endpoint health, and managed function state.
- Added leader-owned API destination, endpoint-pool, function, and target/
  bidirectional connector delivery with binary/structured CloudEvents,
  API-key/bearer/OAuth external secrets, safe DNS-pinned/allowlisted egress,
  stable idempotency, endpoint failover, and checkpoint-before-source-
  settlement ordering.
- Added atomic 2-MiB integration-state admission, snapshotability checks on
  successful tablet mutations, semantic restore validation, real OAuth/target
  and three-voter reopen tests, complete Go/Java/Python lifecycle coverage,
  ADR-0037, end-to-end Pages documentation, traceability, and release notes.

### Limitations

- Event Bus remains one logical shard. MQTT wire compatibility, official
  schema/MQTT/CloudEvents conformance, streaming push, automatic source-
  connector polling, active health restoration, private egress, secret-manager
  hot reload, connector OS isolation/certification, production scale/security
  evidence, and exactly-once arbitrary external side effects remain open.

## [0.1.0-alpha.8] - 2026-08-21

### Added

- Completed the remaining Work Queue development surface with count/byte
  admission, reject/drop/dead-letter overflow, durable idle expiry,
  dedupe-before-eviction, deterministic priority aging, and replicated
  rate/burst/concurrency plus circuit-breaker state.
- Added FIFO session/message groups with renewable exclusive locks, deferred
  exact-ID retrieval, request/reply correlation and reply metadata, command v3,
  Queue snapshot v2 compatibility, complete digest coverage, and linearizable
  regional observations.
- Added a quorum-only crash-safe Queue-to-Queue dead-letter outbox. The source
  leader durably pins one target incarnation, uses a stable history-derived
  target mutation, and records completion only after the target enqueue commits.
- Extended Go, Java, and Python SDKs, exact executable quickstarts, public Pages
  documentation, catalog materialization, and the regional leader-loss/reopen
  campaign across the complete Queue state-services lifecycle.

### Limitations

- Queue remains one physical partition in this alpha. Native streaming receive,
  automatic prefetch, cross-Queue transactions, external DLQ adapters,
  production load/fairness SLOs, and an exhaustive crash/I/O matrix remain
  release gates.

## [0.1.0-alpha.7] - 2026-08-21

### Added

- Completed the Stream state-services development boundary with replicated
  producer epochs/sequences, same-tablet transactions and atomic offset commit,
  read-committed/uncommitted fetch, keyed compaction/tombstones, and immutable
  SHA-256 historical tier objects.
- Added pull, push long poll, consumer-identified dedicated long poll,
  expand-only partition advice, automatic JSON Lines/Array capture with durable
  schedule checkpoints, and contiguous loop-fenced cross-cluster replication
  ingress.
- Extended Go, Java, and Python with the complete advanced Stream API and a
  deterministic logical superstream merge over independently linearizable
  named members. Exact unsigned 64-bit fields remain browser-safe decimal
  strings.
- Added command v7, Stream snapshot v2/tablet snapshot v4 compatibility,
  staged snapshotability and cross-component recovery validation, ADR-0035,
  end-to-end SDK documentation, console coverage, and real three-voter
  checkpoint/reopen evidence.
- Added committed typed state rejections that preserve tablet availability,
  leave staged log/service state unchanged, and survive exact retry, snapshot,
  quorum replication, and all-node recovery.
- Expanded the Docker regional campaign with a dedicated advanced Stream driven
  through the Python SDK across process leader loss, voter catch-up, all-node
  SIGKILL, and same-volume reopen.

### Limitations

- Stream transactions remain one-tablet operations. Alpha tier/capture bytes
  remain embedded in replicated state, dedicated delivery is scheduling rather
  than a throughput SLO, and deployment-specific external object-store and
  inter-region workers remain production gates.

## [0.1.0-alpha.6] - 2026-08-21

### Added

- Completed the alpha implementation contract for `CACHE-001` through
  `CACHE-014`: typed collection and advanced state transforms, exact
  bitmap/probabilistic/geo/JSON/vector queries, entry plus memory/cold byte
  admission, and named requested/achieved regional durability.
- Added replicated bounded Cache changes, canonical checksummed backup and
  point-in-time restore with fresh non-ABA versions, request-correlated
  non-atomic multiplexing, and explicitly node-local at-most-once Pub/Sub.
- Added a per-voter canonical fsynced cold-file read path with recovery
  synchronization, integrity checking, removal on committed state changes, and
  observed read timing disclosed as not an SLO or heap-offload claim.
- Extended Go, Java, and Python SDKs, the Go control plane, console, public
  executable documentation, focused/runtime/recovery suites, traceability, and
  the regional leader-loss/all-voter-reopen campaign across the complete Cache
  lifecycle.
- Added the MIT License and contributor guide. External contributions remain
  small and issue-scoped; maintainer feature releases retain cross-stack
  coordination.

## [0.1.0-alpha.5] - 2026-08-20

### Added

- Required canonical governance for newly managed regional resources: owner,
  cost center, public/internal/confidential/restricted classification, and up to
  32 bounded custom tags. Governance is generation-fenced in durable Go desired
  state and replicated through Rust catalog command/snapshot v3 while valid
  legacy records remain readable.
- Exact AND inventory filtering over owner, cost center, classification, and
  repeated tags, plus deterministic post-authorization resource/shard
  attribution by cost center and classification. The console exposes the same
  filters, governance inventory, and cost-driver summary without claiming usage
  metering or billing.
- ADR-0033, a dedicated end-to-end governance guide and Pages route, strict
  gRPC/HTTP/console contract tests, and a real container proof across Go
  `SIGKILL`, Rust catalog replication, profile failover, and all-node reopen.

- Deterministic regional Cache eviction for no-eviction, all-key LRU/LFU/random,
  and volatile LRU/LFU/random/TTL policies. The Go management plane now forwards
  strict Cache configuration into the replicated Rust catalog and every voter
  materializes the same immutable capacity/default-TTL/policy contract.
- Versioned committed Cache `Get` records LRU/LFU access exactly once across
  idempotent replay while pure observation stays side-effect free. Staged
  admission reports canonical evicted keys, and Cache snapshots retain backward
  compatibility while persisting policy access metadata.
- Go, Java, and Python add one-request atomic-batch helpers over the existing
  ordered transaction command. Executable quickstarts, SDK guidance, ADR-0032,
  and the real failover/reopen campaign cover access, batching, and eviction.

- Leader-owned Event Bus delivery into regional Epoch Queue and Stream targets.
  The source lease pins resource generation, logical shard, tablet ID, and
  tablet epoch before the destination write; Queue uses shard `0` and Stream
  uses the shared FNV-1a key router.
- Stable source-and-destination-scoped target proposal identities, internal
  follower-to-known-leader forwarding, destination receipt resolution, and
  source acknowledgement only after Queue enqueue or Stream append commits.
  This provides duplicate-free insertion within the pinned target incarnation
  across Bus retries without claiming an atomic cross-tablet transaction.
- Additive Event Bus command/snapshot v3 for destination bindings, browser-safe
  read-only destination evidence, always-enabled topology counters, ADR-0031,
  three-language end-to-end examples, and a real three-process convergence and
  all-voter-reopen campaign.

## [0.1.0-alpha.4] - 2026-08-20

### Added

- Opt-in leader-owned signed HTTP/webhook delivery for regional Event Bus
  tablets. The current leader commits and awaits an exact delivery lease before
  I/O, sends the supported envelope in CloudEvents 1.0 binary mode, then commits
  2xx acknowledgement, retryable 429/5xx/network failure, or terminal rejection.
- Versioned HMAC-SHA-256 signatures over timestamp, delivery ID, attempt, and
  exact-body digest; strict bounded external multi-key files support overlap
  rotation without replicating secret bytes. Go, Java, and Python add signed
  target constructors and constant-time verification helpers with one shared
  replay vector.
- HTTPS/public-address-only egress validation with explicit loopback development,
  per-attempt all-answer DNS validation and pinning, IPv4/IPv6 special-purpose
  denial, disabled redirects and ambient proxies, strict derived headers,
  discarded response bodies, and lease-capped request time.
- ADR-0030, end-to-end receiver/operator/SDK documentation, and a real
  three-process 503/204 retry campaign that verifies two distinct signatures,
  converged immutable attempt history, and all-voter same-storage reopen.

- Session-fenced Stream consumption through canonical command v6 and native
  snapshot v3. Per-shard claims preserve the durable next offset, accept only
  bounded monotonic generations, and require exact member/generation ownership
  for claimed fetch plus later commit/reset.
- Go, Java, and Python expose low-level claim/fetch operations and a
  resource-level claim–revalidate helper that pins resource generation, plans
  every bounded bridge before mutation, uses deterministic per-generation
  idempotency keys, and detects concurrent rebalances. Executable Pages
  quickstarts and the real three-shard Python recovery campaign exercise the
  complete path.
- Regional SDK rediscovery now distinguishes an explicitly retryable routing
  fence from a definitive Stream consumer-session fence, preserving stale
  member/generation failures as typed `409 fenced` responses in all three
  languages.
- ADR-0029 and aligned PRD/API/architecture/semantics/runtime/testing/SDK/
  traceability/checklist documentation describe at-least-once behavior,
  namespace-level authorization, and the atomic-handoff/streaming/transaction
  non-claims.

- Automatic node-local consensus checkpoints for regional catalog, Stream,
  Queue, Cache, and Event Bus groups. Every healthy voter uses an
  actor-serialized applied-growth threshold, skips transient pending Raft work,
  reuses canonical EPSN v2 plus physical EPRS reclamation, and exposes exact
  per-group applied/checkpoint/retained boundaries and process-local counters
  through authorized topology. The Docker campaign proves creation on all 24
  voter/group copies, leader-failure catch-up, and all-node same-volume reopen.
- ADR-0028, updated checkpoint/runtime/API/architecture/semantics/traceability
  documentation, and public docs coverage for the automatic recovery policy
  and its backup/PITR non-claims.

- Leader-owned automatic maintenance in the regional runtime. Pure deadline
  queries drive consensus proposals for Stream age retention and shard-zero
  consumer-session expiry, Queue timers and leases, Cache values and locks, and
  Event Bus delivery leases without turning reads into mutations.
- Deterministic due-time proposal identities, pending/committed suppression,
  bounded repeat sweeps, a configurable 1–60,000 ms interval (100 ms default),
  and authorized topology counters for passes, leaders, due/submitted/pending
  work, and errors.
- ADR-0027 plus a real three-node Python/Compose campaign that removes manual
  maintenance calls, proves every profile's idle transition after leader loss,
  catches up the old voter, and reopens all nodes from the same volumes.
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

- Await webhook acquire and settlement proposals through their exact committed
  receipt. Previously an initially pending acquire could commit after the worker
  returned, leaving an in-flight record for lease-timeout maintenance without
  ever attempting HTTP.
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
  dead-member expiry, automatic membership generations, and deterministic
  resource-wide assignment. Regional leader-owned expiry is now implemented;
  cooperative
  revoke acknowledgement, sticky/rack-aware strategies, native streaming
  consumption, atomic assignment-plus-offset handoff, transactional offsets,
  scale/fairness evidence, generated response types, and production fault
  coverage remain unimplemented. Regional retention policy is independent per
  logical shard; leader-owned maintenance is implemented, while keyed
  compaction/tombstones, object-tier retention, and legal-hold governance
  remain open. Standalone offset helpers keep their local contract.
- The regional SDKs remain repository-local alpha source. Package publication,
  generated response models, Event Bus
  external webhook/HTTP/push execution and signing, Cache
  eviction/multi-shard routing, Stream online expansion/remapping and
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

[0.1.0-alpha.5]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.4...v0.1.0-alpha.5
[0.1.0-alpha.6]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.5...v0.1.0-alpha.6
[0.1.0-alpha.4]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.3...v0.1.0-alpha.4
[0.1.0-alpha.3]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.2...v0.1.0-alpha.3
[0.1.0-alpha.2]: https://github.com/Ripan-Roy/epoch/compare/v0.1.0-alpha.1...v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/Ripan-Roy/epoch/releases/tag/v0.1.0-alpha.1
