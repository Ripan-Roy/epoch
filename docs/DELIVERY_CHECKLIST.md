# Epoch Delivery Checklist

**Last reviewed:** 24 August 2026
**Current release target:** `v0.2.0-beta.4`
**Current core target:** Alpha exit: secure transport, recovery, membership, connectors, supply chain, and operating evidence

This is the operational checklist for turning PRD scope into verified,
releasable increments. [PRD.md](PRD.md) owns product scope,
[DELIVERY_PLAN.md](DELIVERY_PLAN.md) owns sequence, and
[REQUIREMENTS_TRACEABILITY.md](REQUIREMENTS_TRACEABILITY.md) owns the
requirement-by-requirement ledger. This document owns delivery gates.

## Status legend

| Mark | Meaning | Delivery rule |
|---|---|---|
| ✅ | Verified | Implemented, evidence linked, merged to `main`, and its required CI is green |
| 🟡 | Slice | A bounded subset is verified; the row lists the remaining non-claims |
| ⬜ | Open | Required work or evidence has not passed |
| ⛔ | Blocked | A named dependency or external decision prevents progress |
| ➖ | Not applicable | The gate does not apply to this particular change |

“Code complete” is not a delivery state. A capability becomes verified only
when its contract, implementation, tests, documentation, traceability, and
protected-branch evidence agree.

## Program gates

| Gate | Deliverable | Current state | Required exit evidence | Evidence / next action |
|---|---|---:|---|---|
| G0 | Semantic contracts | 🟡 | Versioned envelope, errors, durability, ordering, delivery, time, fencing, and transaction limits | [Semantics](SEMANTICS.md), [API contracts](API_CONTRACTS.md); same-tablet Stream transaction limits are frozen, while cross-profile/cross-shard contracts remain open |
| G1 | Repository and deterministic foundation | 🟡 | Reproducible toolchains, generated contracts, deterministic clock/fault harness, cross-language build | [Development](DEVELOPMENT.md), [Testing](TESTING.md), CI; broader fuzz/formal harness remains open |
| G2 | Storage and recovery | 🟡 | Checksummed formats, crash recovery, corruption policy, snapshots, compaction, retention, tiering | Segmented WAL, compatible EPSN v1/v2 checkpoints, native images for all five profiles, checkpoint-plus-tail reopen, fixed-voter catch-up, physical EPRS reclamation, Stream retention, automatic voter checkpoints, Cache backup/PITR and cold reads pass protected `main`. The Stream candidate adds sparse compaction snapshots and checksum-verified embedded historical objects; protected evidence, product-wide external tiering, managed restore campaigns, and production repair remain open |
| G3 | Consensus, catalog, and placement | 🟡 | Quorum safety, persistent catalog, multi-group supervision, membership, placement, repair, read barriers | Dedicated catalog consensus, shared multi-group supervision, deterministic multi-shard materialization, authenticated region/zone/class validation, limiting group-capacity admission, fenced routing, safe leader ReadIndex barriers, and per-shard same-volume recovery pass protected `main`. The alpha-exit branch adds durable three/five-voter membership, learner catch-up from a refreshed post-compaction snapshot, joint single-voter replacement, control-plane transition status, four-node reopen, and one clean live-Kubernetes lifecycle locally; protected evidence, rack placement, general automatic rebalance/repair, follower reads, and broader model evidence remain open. |
| G4 | Native profile cores | 🟡 | Cache, Stream, Queue, and Bus P0 semantics with truthful public routing and fault evidence | All four typed tablet cores run simultaneously behind resource/shard routing and authenticated three-language clients. Cache, Stream, Queue, and the native Event Bus development surface shipped through alpha.9; production protocol/scale evidence remains open. |
| G5 | Trust and observability | 🟡 | Identity, authorization, TLS/mTLS, encryption, audit, telemetry, quotas, explain | Shared Go/Rust authorization and audit pass protected `main`. The beta branch adds fail-closed TLS/mTLS workload identity across public, peer, control, CLI, and SDK paths plus required AES-256-GCM backup encryption; protected evidence, OIDC, expiry/revocation, rotation automation, replicated policy, immutable audit export, exported telemetry/alerts, and quotas remain open. |
| G6 | Compatibility gateways | 🟡 | Named protocol/client matrix, differential tests, fuzzing, malformed frames, migration evidence | The compatibility feature adds a separate bounded Rust gateway for RESP2/RESP3 strings/counters/TTL, Kafka produce/manual consume/metadata/offsets with four compression codecs, and AMQP 0-9-1 direct publish/confirm/push-or-pull consume/prefetch/settlement. Redis CLI 8.8.2, Kafka Java 4.3.1, and RabbitMQ Java 5.34.0 pass the real wire listeners; authenticated/fenced native adapter contracts, a conservative migration scanner, the public matrix, docs UI, and a signed non-root OCI component are local. Combined real-regional conformance, differential/fuzz evidence, protected evidence, richer Redis types, Kafka membership/transactions, broader AMQP routing/1.0, and MQTT remain open. |
| G7 | Data services and integrations | 🟡 | Schemas, pipes, connectors, target execution, checkpoints, transaction boundaries | Alpha.9 shipped replicated schemas/validation, transforms/enrichment, MQTT state, catalogs/endpoints, connector checkpoints/replay, and leader-owned signed/Epoch/API/function/managed-target execution. Alpha.10 added HTTP/CloudEvents source polling; the alpha-exit branch adds bounded immutable-object, PostgreSQL, MySQL, and Kafka readers behind the same durable checkpoint contract plus real MinIO/database/broker conformance locally. Private egress, official protocol conformance, live Azure/GCS identity, load/soak, broader crash certification, and protected evidence remain open. |
| G8 | Managed operations | 🟡 | Durable Go reconciliation, operator, autoscaling, backup, metering, billing, private networking | The leader-elected operator now reconciles 3–1,024 physical nodes, independent three/five-voter placements, a durable control owner, stable storage/network identity, encrypted backup/restore, guarded upgrade, and learner-first replacement. One exact-source four-node lifecycle passes locally; protected/cloud CSI evidence, autoscaling, metering/billing, and private networking remain open. |
| G9 | Geo | ⬜ | Replication, RPO/RTO, promotion, failback, residency, split-brain drills | Not implemented |
| G10 | Release readiness | 🟡 | Synchronized versions, CI, Pages, notes, verified tag provenance, artifacts, security and compatibility statements | `v0.2.0-beta.2` passed protected source, exact-main, Pages, Kubernetes, connector, soak, and native-arm64 evidence, but its QEMU-based tag job timed out before publishing the node image or GitHub prerelease. The local beta.3 candidate replaces release emulation with concurrent native platform builds, immutable digest handoff, scoped caches, and fail-closed manifest assembly while retaining provenance, per-platform SBOM attestations, and keyless signing. Protected beta.3 execution, published-digest proof, raw signed binaries, package-manager artifacts, and GA operating evidence remain open. |

## Milestone readiness

| Milestone | Outcome | Entry gate | Exit checklist | State |
|---|---|---|---|---:|
| M0 | Architecture and semantic freeze | PRD approved | ADRs, API/format contracts, model plan, threat model, benchmark method, compatibility policy | 🟡 |
| M1 | Foundational vertical slice | G0–G1 bounded | Standalone plus fixed-voter profiles, deterministic faults, SDK quickstarts, CI, source prerelease | 🟡 |
| M2 | Private alpha core | M1 evidence | Public multi-tablet native core, catalog/placement, auth baseline, fault matrix, soak, profile benchmarks | ⬜ |
| M3 | Private beta compatibility | M2 core | Snapshots/restore, named gateway subsets, schemas, migrations, Terraform, compatibility matrix | ⬜ |
| M4 | Public beta managed service | M3 migration evidence | Managed fleet, complete Bus beta, connectors, private networking, metering, beta SLO | ⬜ |
| M5 | GA core | M4 operating evidence | Bounded transactions, guarded upgrades, geo failover/failback, signed artifacts, support readiness | ⬜ |
| M6 | Post-GA breadth | GA reliability | P2 data types, search, advanced connectors, marketplace, selected deferred capabilities | ⬜ |

Milestones remain open until every exit item has linked evidence. A source
prerelease may mark a bounded slice without marking its parent milestone
complete.

## Current core delivery: regional multi-tablet runtime

| ID | Checklist item | Owner boundary | Dependency | State | Evidence / acceptance |
|---|---|---|---|---:|---|
| MT-01 | Fully qualified resource identity and immutable profile | Rust catalog | G0 | ✅ | `epoch-catalog` lifecycle and validation tests |
| MT-02 | Monotonic resource generations and delete/recreate tombstones | Rust catalog | MT-01 | ✅ | Stale generation, recreation, and non-reuse tests |
| MT-03 | Stable shard-to-tablet/group allocation and expansion | Rust catalog | MT-01 | ✅ | Multi-resource collision and expansion tests |
| MT-04 | Canonical, versioned, idempotent catalog commands | Rust catalog | MT-01 | ✅ | Codec, replay, unknown-version, and token-rebinding tests |
| MT-05 | Tablet descriptor in the versioned Rust/Go contract | Protobuf | MT-01 | ✅ | Buf lint and generated-binding freshness |
| MT-06 | Persist catalog commands through a dedicated consensus group | Rust regional control | G2, G3, MT-04 | 🟡 | Dedicated group 1 commits canonical catalog commands through three EPRS voters; minority, replay, restart, catch-up, corruption, and protected `main` CI gates pass. Dynamic membership remains open |
| MT-07 | Demultiplex peer frames by group and epoch | Rust node transport | MT-06 | 🟡 | Shared listener rejects unknown group/epoch, keeps groups isolated, and uses bounded ordered peer queues; broader load/backpressure benchmarks remain open |
| MT-08 | Supervise several consensus groups in one node process | Rust node runtime | MT-07 | 🟡 | One supervisor reserves catalog group 1, enforces a group cap, runs catalog plus several profile groups, and reopens them after `SIGKILL`; dynamic membership and production resource accounting remain open |
| MT-09 | Materialize typed profile tablets from committed catalog state | Rust regional control | MT-06, MT-08 | 🟡 | Catalog apply/delete/reconcile tests and real process/container runs create Cache, multi-shard Stream, Queue, and Bus tablets together and recover them from committed state; online shard transfer remains open |
| MT-10 | Route public requests by resource, shard, leader, generation, and epoch | Rust gateway | MT-09 | 🟡 | Generic resource/shard discovery and data dispatch reject stale generation, stale tablet epoch, nonleaders, missing routes, profile mismatches, missing credentials, action denial, and cross-tenant scope; regional reads default to safe leader ReadIndex with explicit stale opt-in. Fully qualified Stream, Queue, Cache, and Event Bus v1 adapters plus three SDKs implement native leader/fence routing; Stream additionally publishes generation-safe keyed routing and a shard-zero session coordinator. Production identity/TLS, follower routing, and safe online remapping remain open |
| MT-11 | Reconcile hosted desired state through the Rust authority | Go control plane | MT-06, MT-10 | 🟡 | Real authenticated gRPC lifecycle, transactional bbolt desired/status/token/tombstone state, exact replay and generation continuity across restart, corruption/version/exclusive-owner rejection, complete topology inventory, incremental capacity admission, generation-fenced status, and Go-to-Rust container reconciliation pass locally; multi-instance consistency, transactional reservations, OIDC/mTLS, and replicated policy remain open |
| MT-12 | Show achieved placement and risk without overclaiming | TypeScript console | MT-10, MT-11 | 🟡 | The console reads only the Go BFF with a session-only interactive credential, preserves decimal 64-bit IDs, distinguishes pending/ready/degraded/failed, and lists observed voters/leaders plus consistent configured zone/class/group-capacity evidence and remaining server-identity/rack/dynamic-membership non-claims; OIDC exchange and browser visual/accessibility automation remain open |
| MT-13 | Real-process and container fault campaign | Test infrastructure | MT-06–MT-12 | 🟡 | Three policy-protected regional containers cover simultaneous four-profile tablets, authenticated control recovery, leader losses, catch-up, all-node `SIGKILL`, and same-volume reopen. The beta branch additionally passes one signed accelerated fault round and one clean local four-node Kubernetes backup/replacement/upgrade/restore lifecycle; broader crash/I/O/auth abuse, protected Kubernetes, and the elapsed 30-day gate remain open. |
| MT-14 | Documentation, traceability, changelog, and release notes | Cross-cutting | MT-13 | 🟡 | ADRs, executable SDK guides, release notes, and main-only Pages are published through `v0.1.0-alpha.9`. ADR-0038 plus source, CLI, and operator runbooks are the alpha.10 candidate; protected publication remains open. |

## Current compatibility delivery: Redis, Kafka, and RabbitMQ

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| PC-01 | Keep protocol I/O outside replicated engines | Rust gateway | 🟡 | `epoch-compat` depends on one narrow async semantic port and translates into authenticated, generation/tablet/term-fenced regional Cache, Stream, and Queue calls; gateway restart/failover evidence remains open. |
| PC-02 | Bound adversarial protocol input and concurrency | Rust gateway | 🟡 | 8 MiB frames, 4 MiB bodies/cumulative Kafka expansion, 1,024 logical items, preflighted Kafka record counts, bounded Zstandard windows, bounded per-listener connection semaphores, strict trailing-byte rejection, AMQP state ordering, and RESP partial-frame handling pass local tests; fuzz/minimized malformed corpus remains open. |
| PC-03 | Support high-value Redis client traffic | RESP2/RESP3 | 🟡 | HELLO/AUTH/client setup, binary-safe values, NX/XX/GET plus EX/PX, MGET/MSET, delete/existence/type, signed counters, TTL/expiry/persist, RESP3, and pipelining pass semantic/parser tests and Redis CLI 8.8.2. Rich structures, transactions/scripts, Pub/Sub/Streams, binary keys, cluster/TLS, and combined regional conformance remain open. |
| PC-04 | Support Kafka producer and manual consumer traffic | Kafka wire | 🟡 | ApiVersions advertises only dispatched Produce 3–9, Fetch 4–12, ListOffsets 1–7, Metadata 1–12, FindCoordinator 0–4, OffsetCommit 2–9, and OffsetFetch 1–7. gzip/Snappy/LZ4/Zstd batches and Kafka Java 4.3.1 producer/manual-consumer/commit flows pass; each Produce partition is exact-codec-decoded and submitted as one canonical atomic native Stream batch. Group membership, idempotence/transactions/admin/SASL and combined regional conformance remain open. |
| PC-05 | Support RabbitMQ work delivery lifecycle | AMQP 0-9-1 | 🟡 | PLAIN/open/tune/heartbeat, channels, existing Queue declaration, connection-local direct bindings, binary publish, confirms, push consume/basic.get, QoS, ack/reject/nack/requeue, cancel, and selected properties pass tests plus RabbitMQ Java 5.34.0. Broader routing, arguments/policies, mandatory returns, transactions, AMQP 1.0/TLS and combined regional conformance remain open. |
| PC-06 | Publish an exact compatibility matrix and migration-safe non-claims | Docs | 🟡 | [Protocol compatibility](PROTOCOL_COMPATIBILITY.md) names version targets, APIs/commands, lossy translations, security, retry behavior, and unsupported features; the GitHub Pages UI exposes the matrix plus Redis Python, Kafka Java, and RabbitMQ Java examples. Main-only Pages evidence remains open. |
| PC-07 | Ship the gateway as a supply-chain component | OCI/release | 🟡 | A numeric non-root, pinned-base `epoch-compat` OCI image joins PR inspection, SPDX generation, native amd64/arm64 tag builds, immutable manifest assembly, provenance, signing, and release assets. Local image inspection and protected tag publication remain open. |
| PC-08 | Prove real-client compatibility | CI | 🟡 | A repeatable CI job pins Redis CLI 8.8.2, Apache Kafka Java 4.3.1, and RabbitMQ Java 5.34.0 and proves supported wire lifecycles; separate adapter contracts prove authenticated/fenced native requests and response translation. Pending: run both layers together against a faulted regional cluster and publish protected evidence. |
| PC-09 | Scan migrations before cutover | CLI | 🟡 | `epoch-compat scan` emits bounded `epoch.compatibility-scan/v1` text/JSON assessments with source lines, CI failure thresholds, supported/partial/unknown/unsupported outcomes, Kafka version ranges, Redis option/database boundaries, and an unsupported feature corpus. Dynamic traffic capture and protected evidence remain open. |
| PC-10 | Meet comparative performance gates | Benchmark | ⬜ | Run matched-semantics Redis/Kafka/RabbitMQ baselines and publish throughput/p50/p99 saturation evidence; no performance parity is claimed yet. |

## Current placement delivery: topology-aware fixed-voter admission

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| PL-01 | Report immutable node topology from validated runtime config | Rust node | ✅ | Bounded region/zone/class plus peer-derived voter IDs reject invalid or ambiguous startup identity |
| PL-02 | Report live consensus-group capacity | Rust node | ✅ | Authenticated topology route counts catalog plus materialized tablets and returns maximum/used/available groups |
| PL-03 | Collect a complete consistent inventory | Go authority | ✅ | Every configured endpoint must respond; duplicate IDs, malformed labels, mismatched voters, and inconsistent capacity fail closed |
| PL-04 | Validate requested placement before catalog mutation | Go reconciler | ✅ | Allowed regions, minimum zones, and node class are checked against the exact immutable voter set |
| PL-05 | Reject the limiting resource without over-reserving retries | Go reconciler | ✅ | New resources charge all shards; expansion charges only added observed shards; observation retries charge zero; capacity failure names the limiting node |
| PL-06 | Publish requested versus achieved topology | Protobuf + Go BFF | ✅ | Generated contract and browser-safe projection include policy, achieved zones, nodes, fixed voters, and capacity with decimal 64-bit IDs |
| PL-07 | Explain evidence and non-claims | TypeScript console | ✅ | Console verifies zone evidence and per-node capacity while naming rack placement, membership changes, and rebalance as absent |
| PL-08 | Prove the vertical path in containers | Integration | ✅ | Campaign checks three topology endpoints, accepts a three-zone resource, rejects 15 shards before catalog apply, then repeats failover/recovery |
| PL-09 | Pass protected pull-request evidence | GitHub | ✅ | PR #41 was squash-merged as `038b8c09`; exact-main CI `30477768038` and Pages `30477767094` passed |

## Current read delivery: quorum-confirmed regional reads

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| RB-01 | Keep Raft implementation types behind Epoch-owned contracts | Rust consensus | ✅ | Typed request/completion IDs carry group, epoch, and expected term without exporting `raft-rs` types |
| RB-02 | Complete only after majority confirmation and local apply | Rust consensus | ✅ | Safe ReadIndex completion requires quorum `ReadState` plus `applied_index >= read_index`; new-leader requests wait for a current-term commit |
| RB-03 | Bound, cancel, and fence pending reads | Rust consensus + actor | ✅ | 1,024-request cap, unique IDs, caller timeout cancellation, and role/term invalidation are locally tested |
| RB-04 | Apply typed profile state before releasing read proof | Rust node actor | ✅ | Commit application precedes barrier notification; a real three-runtime test proves success with a majority and timeout without one |
| RB-05 | Make regional reads linearizable by default without silent downgrade | Rust gateway | ✅ | Typed GETs and Event Bus query POSTs use leader barriers; explicit `local_stale` alone bypasses them |
| RB-06 | Return machine-readable consistency evidence and errors | HTTP contract | ✅ | Response headers/JSON carry achieved consistency and exact term/read/applied indexes; timeout is retryable 503 |
| RB-07 | Preserve authorization semantics | Rust auth | ✅ | Event Bus query POSTs require `data.read`; mutation POSTs still require `data.write` |
| RB-08 | Prove the full process path | Integration | ✅ | Regional process test performs a linearizable leader read, validates evidence, and retains explicit stale convergence reads |
| RB-09 | Document design and non-claims | Documentation | ✅ | ADR-0013 and API/runtime/consensus/profile/testing/traceability docs distinguish regional barriers from direct stale routes |
| RB-10 | Pass protected pull-request evidence | GitHub | ✅ | PR #42 was squash-merged as `77debaf2`; exact-main CI `30482274859` and Pages `30482275423` passed |

## Current Queue delivery: consumer credit and concurrency

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| QF-01 | Freeze bounded credit/window semantics and non-claims | Semantics + ADR | ✅ | ADR-0014 defines the atomic grant formula, cross-epoch identity scope, explicit maintenance, and native-streaming/fairness non-claims |
| QF-02 | Preserve historical command compatibility | Rust tablet contract | ✅ | Existing operations remain canonical v1; only `AcquireWithCredit` emits v2; public golden tests pin both encodings |
| QF-03 | Enforce the consumer window atomically | Rust Queue/tablet | ✅ | Actor-serialized cloned transition counts authoritative live leases and fails bounds before live mutation |
| QF-04 | Return exact flow evidence and a pure observation | Rust HTTP | ✅ | Acquire receipts expose requested/window/before/after/remaining values; consumer-flow GET exposes applied epoch/count without sampling time |
| QF-05 | Prove saturation and replenishment through real consensus | Rust tests | ✅ | Deterministic tests plus a real three-runtime HTTP cluster prove independent windows, cross-epoch accounting, saturation, and Ack replenishment |
| QF-06 | Exercise the container boundary | Compose integration | ✅ | Queue campaign uses v2 credit, checks exact evidence and consumer-flow read, then retains failover and all-node recovery coverage |
| QF-07 | Keep product claims and traceability aligned | Documentation + console | ✅ | Queue/API/semantics/architecture/testing/plan/traceability/user docs name the bounded slice and remaining work |
| QF-08 | Pass protected pull-request evidence | GitHub | ✅ | PR #43 was squash-merged as `8de7234a`; exact-main CI `30485896425` and Pages `30485900141` passed |

## Current Stream delivery: batch append and bounded compression

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| SF-01 | Freeze framing, atomicity, limits, and non-claims | Semantics + ADR | ✅ | ADR-0015 specifies canonical records, codec frames, exact idempotent input, whole-batch visibility, ceilings, and stable-native non-claims |
| SF-02 | Preserve historical command and digest compatibility | Rust tablet contract | ✅ | Single `Append` remains canonical v1; only `AppendBatch` emits v2; public golden tests pin both command forms and the original digest |
| SF-03 | Bound and validate every decompression path | Rust codec boundary | ✅ | `none`, gzip, LZ4 frame, Snappy framed, and Zstd frame validate canonical base64/JSON, exact sizes/count, unique sequences, 4 MiB output, and an 8 MiB Zstd window before mutation |
| SF-04 | Apply a correlated batch atomically | Rust Stream/tablet | ✅ | A cloned state transition exposes no prefix; receipts return codec/size/count evidence and one exact decimal offset/disposition per client sequence |
| SF-05 | Expose strict direct and regional routes | Rust HTTP | ✅ | `records/batches` validates client-supplied frames, advertises limits/codecs in status, inherits regional fencing/auth/leader checks, and preserves exact retry/conflict semantics |
| SF-06 | Prove all codecs through real consensus and recovery | Rust tests | ✅ | Unit corpus plus a real three-runtime HTTP cluster commit/retry every codec, compare correlated offsets, and rebuild all batches from EPRS |
| SF-07 | Exercise an independent client frame in containers | Compose integration | ✅ | The Stream campaign generates gzip with Python, commits after leader replacement, retries/conflicts, then proves all-voter convergence and `SIGKILL` replay |
| SF-08 | Keep product claims and documentation aligned | Documentation + console | ✅ | Stream/API/semantics/architecture/testing/plan/traceability/user docs and Pages content name the bounded slice and remaining work |
| SF-09 | Pass protected pull-request evidence | GitHub | ✅ | PR #47 merged as `cdbee3a7`; exact-main CI `31031439800` and Pages `31031439717` passed |

## Current Stream delivery: consumer-group checkpoints and fencing

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| CG-01 | Freeze next-offset, ownership, reset, and non-claim semantics | Semantics + ADR | ✅ | ADR-0016 defines first/next generation, owner fencing, monotonic commit, explicit retained-range reset, committed rejection, and coordinator/SDK non-claims |
| CG-02 | Preserve append and batch compatibility | Rust tablet contract | ✅ | `GroupOffset` alone emits canonical v3; public tests retain exact v1 command/digest and v2 batch goldens |
| CG-03 | Apply checkpoints and fences deterministically | Rust Stream/tablet | ✅ | Cloned transitions replicate owner plus next offset; wrong/stale/skipped generations, rewind/range, and capacity races return typed committed rejection without business-state mutation |
| CG-04 | Expose strict mutation, lag, and replay routes | Rust HTTP + regional router | ✅ | Direct `groups/{group}/{offsets|lag|records}` routes use browser-safe positions; regional mapping inherits authorization, generation/tablet fences, leader admission, and safe read barriers |
| CG-05 | Prove exact retry, convergence, and rebuild | Rust tests | ✅ | Canonical/state tests plus a real three-runtime HTTP cluster prove commit, replay, handoff/reset, owner fencing, lag/replay convergence, and EPRS reopen |
| CG-06 | Exercise process failover and crash recovery | Compose integration | ✅ | Stream container campaign commits applied and rejected group outcomes after leader replacement, compares all voters, then verifies checkpoint/lag/replay plus digest after all-node `SIGKILL` |
| CG-07 | Keep SDK and product claims honest | Documentation + console | ✅ | Stream/API/architecture/semantics/testing/plan/traceability/changelog/Pages content distinguish standalone helpers from the explicit regional Go/Java/Python checkpoint primitive and list every remaining coordinator/session behavior |
| CG-08 | Pass protected pull-request evidence | GitHub | ✅ | PR #49 merged as `14719d32`; exact-main CI `31035898600` and Pages `31035898592` passed |

## Current native delivery: regional Stream v1 and SDK routing

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| RS-01 | Freeze the fully qualified route and retry contract | API + ADR | ✅ | ADR-0017 defines one organization/project/environment/namespace/Stream/shard identity, current-leader discovery, generation/tablet fences, linearizable reads, and same-key bounded rediscovery |
| RS-02 | Authenticate and authorize the exact tenant scope | Rust ingress | ✅ | Strict route parsing maps discovery to `route.read`, GET data to `data.read`, and mutations to `data.write`; middleware tests cover missing credentials, denied writes, and cross-tenant denial |
| RS-03 | Adapt v1 to the existing replicated tablet | Rust gateway | ✅ | The versioned route delegates to the same materialized Stream router and state machine; record and parameterized group routes retain fences/read barriers with no Go proxy or second store |
| RS-04 | Prevent outer/inner path-parameter contamination | Rust regression | ✅ | A red handler test reproduced the real post-failover group-route 500; dispatch now clears cached outer parameters at the adapter boundary while preserving intentional read metadata |
| RS-05 | Implement one route contract in three SDKs | Go + Java + Python | ✅ | `RegionalScope` and `RegionalStreamClient` cover append, bounded fetch, checkpoint commit/reset, lag, and checkpoint replay with custom-transport seams and header-aware HTTP transports |
| RS-06 | Prove routing, fences, term, reads, and validation | SDK tests | ✅ | All three unit suites assert leader selection, bearer/fence/term propagation, encoded segments, linearizable reads, group paths, and fail-fast input validation |
| RS-07 | Prove a real SDK after leader loss and full restart | Compose integration | ✅ | The three-node campaign kills the old leader, runs Python append/exact-retry/fetch/checkpoint/lag against all endpoints, catches up the old voter, then reopens all EPRS volumes and verifies applied convergence |
| RS-08 | Publish executable end-to-end docs | Docs + Pages | ✅ | A dedicated SDK guide, ADR, API/runtime/architecture/traceability updates, and exact compilable Go/Java/Python examples are embedded in the docs-only bundle with content assertions |
| RS-09 | Pass protected pull-request evidence | GitHub | ✅ | PR #50 merged as `34e86ddb`; exact-main CI `31041617501` and Pages `31041621626` passed and the live Pages URL contained the regional SDK content |

## Current native delivery: regional Queue v1 and SDK routing

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| RQ-01 | Freeze the fully qualified Queue route and lifecycle contract | API + ADR | ✅ | ADR-0018 defines discovery, fences, strict mutation union, linearizable reads, same-key bounded rediscovery, and streaming non-claims |
| RQ-02 | Authenticate and authorize the exact tenant scope | Rust ingress | ✅ | Strict native parsing and middleware tests cover Queue discovery/read/write, missing credential, denied write, and cross-tenant denial |
| RQ-03 | Adapt v1 to the existing replicated tablet | Rust gateway | ✅ | Versioned Queue routes delegate to the same materialized Work Queue router with generation/tablet fences, leader admission, and ReadIndex barriers; no Go proxy or second store exists |
| RQ-04 | Implement the complete lifecycle in three SDKs | Go + Java + Python | ✅ | Shared private regional cores plus `RegionalQueueClient` expose all nine mutations and mutation/count/history/flow/status reads with fail-fast bounds and opaque tokens |
| RQ-05 | Prove exact contracts and same-key rediscovery | SDK tests | ✅ | Go, Java, and Python tests cover encoded resource/consumer paths, bearer/fences/term, every lifecycle route, browser-safe integers, linearizable reads, validation, and unchanged mutation identity |
| RQ-06 | Prove the SDK after Queue leader loss and full restart | Compose integration | ✅ | Real Python runs enqueue replay, credit acquire, renewal, settlement/retry, DLQ/redrive, reads, 11-command survivor convergence, old-voter catch-up, and all-voter EPRS reopen |
| RQ-07 | Publish executable end-to-end docs | Docs + Pages | ✅ | Regional Queue guide, ADR, cross-cutting docs, exact Go/Java/Python programs, docs compile gate, navigation, and Pages content assertions are published |
| RQ-08 | Pass protected pull-request evidence | GitHub | ✅ | PR #51 merged as `f3652fba`; exact-main CI `31073989851` and Pages `31073989858` passed and the live Pages URL contained the Queue SDK content |

## Current native delivery: regional Cache v1 and SDK routing

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| RC-01 | Freeze the fully qualified Cache route and lifecycle contract | API + ADR | ✅ | ADR-0019 defines discovery, fences, strict values/mutations, CAS/transaction/expiry/lock semantics, linearizable reads, and same-key bounded rediscovery |
| RC-02 | Authenticate and authorize the exact tenant scope | Rust ingress | ✅ | Strict parsing accepts `caches`; unit and middleware tests cover discovery/read/write actions, missing credential, denied write, and cross-tenant denial |
| RC-03 | Adapt v1 to the existing replicated Cache tablet | Rust gateway | ✅ | Versioned Cache routes delegate to the same materialized Cache router with generation/tablet fences, leader admission, and ReadIndex barriers; no Go proxy or second store exists |
| RC-04 | Implement the complete current lifecycle in three SDKs | Go + Java + Python | ✅ | `RegionalCacheClient` plus typed values, CAS expectations, transaction mutations, and lock guards expose all original nine mutations and lookup/observation/status reads with fail-fast bounds; CE-05 tracks committed `Get` and the batch alias |
| RC-05 | Prove exact contracts and validation | SDK tests | ✅ | Go, Java, and Python suites cover encoded paths, auth/fences/term, all value kinds, every lifecycle route, decimal 64-bit encoding, linearizable reads, transaction bounds, and invalid values before network |
| RC-06 | Prove the SDK after Cache leader loss and full restart | Compose integration | ✅ | Real Python runs exact replay, all value kinds, CAS, increment, transaction, lock renewal/guarded delete, expiry, 12-command convergence, old-voter catch-up, and all-voter EPRS reopen |
| RC-07 | Publish executable end-to-end docs | Docs + Pages | ✅ | Regional Cache guide, ADR, cross-cutting docs, exact Go/Java/Python programs, docs compile gate, navigation, and Pages content assertions are published on the live docs site |
| RC-08 | Pass protected pull-request evidence | GitHub | ✅ | PR #52 merged as `4afea088`; exact-main CI `31076973784` and Pages `31076973783` passed and the live Pages bundle contained the Cache SDK guide and maintenance example |

## Current Cache delivery: deterministic eviction and atomic access batches

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| CE-01 | Freeze committed access, victim selection, batch, and compatibility semantics | ADR + contracts | ✅ | ADR-0032 defines pure observation versus committed `Get`, all eight policies, canonical/random ranking, staged rollback, atomic-batch aliasing, and v1 compatibility |
| CE-02 | Persist and expose managed Cache configuration end to end | Go control + Rust catalog + console | ✅ | Focused Go/Rust/TypeScript tests prove strict forwarding, catalog persistence/snapshot restore, immutability, normalization, voter materialization, safe BFF projection, and visible policy/capacity intent |
| CE-03 | Implement deterministic policy eviction | Rust Cache engine | ✅ | Engine tests cover LRU, LFU, volatile eligibility/TTL rejection, deterministic random replay, canonical victims, and atomic capacity rollback |
| CE-04 | Make access metadata a committed replicated transition | Rust tablet + gateway | ✅ | Version-2 `Get` validation, one-time idempotent access, receipts, status policy, digest, snapshot/reopen, and pure `Observe` behavior pass focused tablet/node tests |
| CE-05 | Expose bounded ordered batches in every SDK | Go + Java + Python | ✅ | `AtomicBatch`/`atomicBatch`/`atomic_batch` reuse the one-to-128 transaction wire command; each SDK contract suite covers committed `Get` and the alias |
| CE-06 | Publish runnable three-language guidance | Docs + Pages | ✅ | Exact quickstarts and Cache SDK/API/semantics/architecture/runtime/testing docs include configured LRU, committed access, atomic batching, pooling guidance, and non-claims; main-only Pages run `32398199351` published the bundle |
| CE-07 | Prove failover, catch-up, and same-volume reopen | Compose integration | ✅ | The real campaign creates Cache configuration through Go, then proves committed LRU access, atomic batch, sorted eviction receipt, voter convergence, catch-up, and post-restart state |
| CE-08 | Pass all local and protected gates | CI + GitHub | ✅ | [PR #78](https://github.com/Ripan-Roy/epoch/pull/78) merged as `c50db1c`; exact-main [CI run 32398199013](https://github.com/Ripan-Roy/epoch/actions/runs/32398199013), main-only [Pages run 32398199351](https://github.com/Ripan-Roy/epoch/actions/runs/32398199351), and the live Cache bundle marker are green |

## Current governance delivery: ownership, classification, and cost drivers

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| RG-01 | Freeze canonical metadata, compatibility, and non-claims | Contract + ADR | ✅ | ADR-0033 defines required managed fields, authoritative environment identity, bounds, exact matching, legacy readability, authorization ordering, and billing non-claims |
| RG-02 | Persist governance in management desired state | Go registry | ✅ | Canonical values participate in apply equality, tokens, generation fencing, defensive copies, bbolt validation, recovery, and mandatory managed creation |
| RG-03 | Replicate governance into the data-plane catalog | Go authority + Rust catalog | ✅ | Protobuf/HTTP forwarding and catalog command/snapshot v3 preserve governance; valid v1/v2 resources remain readable |
| RG-04 | Filter inventory exactly and safely | Go gRPC + HTTP | ✅ | Owner, cost center, classification, and repeated tags use normalized exact AND matching with strict malformed/duplicate/reserved/bounded rejection |
| RG-05 | Attribute visible allocation drivers | Go BFF | ✅ | Deterministic post-authorization aggregation reports resource and desired-shard counts by cost center and classification without claiming metering or billing |
| RG-06 | Expose governance and attribution in the console | TypeScript console | ✅ | Session-authenticated inventory adds explicit filters, governance columns, and attribution cards with safe response validation |
| RG-07 | Prove control/data-plane recovery | Compose integration | ✅ | The real three-container campaign compares Go and Rust governance before/after Go `SIGKILL`, profile failover, and all-node same-volume reopen |
| RG-08 | Publish end-to-end documentation | Docs + Pages | ✅ | Resource-governance guide, ADR-0033, API/security/architecture/semantics/testing docs, and the dedicated Pages route are published from `main` through PR #79 |
| RG-09 | Pass full feature and release gates | Quality + GitHub | ✅ | PR #79 merged after complete local and protected checks; exact-main CI/Pages passed and `v0.1.0-alpha.5` was published with curated release notes |

## Current Cache completion delivery

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| CC-01 | Freeze guarantees, limits, compatibility, and non-claims | ADR + semantics | ✅ | ADR-0034 defines snapshot v4 compatibility, byte classes, named durability, transforms/query, change retention, backup/PITR, multiplex, lossy Pub/Sub, cold-file reads, and production non-claims; protected review shipped in alpha.6 |
| CC-02 | Complete deterministic state behavior | Rust Cache engine | ✅ | All 44 Cache tests cover collection transforms, bitmap/cardinality/Bloom/Cuckoo/geo/JSON/vector behavior, exact indexes/search, byte admission/eviction, changes, backup corruption/window/PITR, non-ABA restore, snapshot recovery, and digest convergence in protected CI |
| CC-03 | Preserve one replicated recovery model | Rust tablet + node | ✅ | Compatible commands/snapshots, transactions with transforms, restore proposal bounds, public cold-Set normalization, leader ReadIndex queries, native multiplex exact replay, requested/achieved durability, and fsynced cold synchronization pass tablet plus all 23 focused node tests |
| CC-04 | Expose the complete lifecycle in every SDK | Go + Java + Python | ✅ | Go, 42 Java tests, and 40 Python tests cover cold writes, transform, multiplex, changes, backup/restore, typed query, Pub/Sub, and status with pre-network validation and exact route/auth/fence behavior |
| CC-05 | Materialize and operate the resource end to end | Go control + console | ✅ | Memory/cold caps and named durability flow through desired state, catalog, every voter, BFF, inventory, and form; cold latency disclosure is visible and Go/console tests, typecheck, lint, and production build pass |
| CC-06 | Prove real leader loss and all-voter reopen | Compose integration | ✅ | The rebuilt regional campaign passes ordinary/advanced state, atomic/multiplex writes, byte eviction, cold disk read, backup/PITR, changes, lossy Pub/Sub, convergence, catch-up, checkpoints, and same-volume reopen |
| CC-07 | Publish executable end-to-end docs | Docs + Pages | ✅ | All three displayed quickstarts compile; the Cache guide/tablet/ADR, PRD/traceability/checklist, SDK matrix, production docs build, and Pages bundle shipped from `main` |
| CC-08 | Release `v0.1.0-alpha.6` | Quality + GitHub | ✅ | Synchronized source-only prerelease notes, protected CI, main-only Pages, tag provenance, and GitHub release are published; package-manager artifacts remain intentionally deferred |

## Current native delivery: regional Event Bus v1 and SDK routing

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| RBU-01 | Freeze the fully qualified Event Bus route and lifecycle contract | API + ADR | ✅ | ADR-0020 defines discovery, fences, subscription delivery policy, ingress, archive, delivery leases and settlement, linearizable reads, same-key bounded rediscovery, and target-executor non-claims |
| RBU-02 | Authenticate and authorize the exact tenant scope | Rust ingress | ✅ | Strict parsing accepts `buses`; unit and middleware tests cover discovery/read/write actions, query-shaped POST reads, missing credential, denied write, and cross-tenant denial |
| RBU-03 | Adapt v1 to the existing replicated Event Bus tablet | Rust gateway | ✅ | Versioned Event Bus routes delegate to the same materialized Bus router with generation/tablet fences, leader admission, ReadIndex barriers, and durable delivery outbox enabled; no Go proxy or second store exists |
| RBU-04 | Implement the complete current lifecycle in three SDKs | Go + Java + Python | ✅ | `RegionalBusClient` exposes subscription upsert/removal, publish, delivery acquire/ack/fail/maintenance, mutation lookup, archive replay, delivery query, and status with bounded delivery-policy models |
| RBU-05 | Prove exact contracts and validation | SDK tests | ✅ | Go, Java, and Python suites cover encoded paths, auth/fences/term, all lifecycle routes, exact caller keys, opaque lease tokens, linearizable GET and POST reads, delivery-policy serialization, and pre-network validation |
| RBU-06 | Prove the SDK after Event Bus leader loss and full restart | Compose integration | ✅ | Real Python exact publish, archive, acquire/fail/maintenance/reacquire/ack, acknowledged-query, subscription removal, nine-command survivor convergence, old-voter catch-up, and all-voter EPRS reopen passed in exact-main CI run `31085110527` |
| RBU-07 | Publish executable end-to-end docs | Docs + Pages | ✅ | Event Bus guide, ADR, cross-cutting docs, exact Go/Java/Python programs, compile gate, navigation, and content assertions passed main-only Pages run `31085110436`; the live bundle exposes the guide and all three SDK examples |
| RBU-08 | Pass protected pull-request evidence | GitHub | ✅ | PR #53 was squash-merged as `b6fa972e`; exact-main CI `31085110527` and Pages `31085110436` passed and the live Pages bundle contained the Event Bus SDK guide and lifecycle example |

## Current native delivery: signed HTTP/webhook execution

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| WH-01 | Freeze leader ownership, delivery order, crypto, replay, and non-claims | ADR + semantics | ✅ | ADR-0030 defines exact lease-before-I/O, at-least-once recovery, HMAC-SHA-256 input, `(delivery ID, attempt)` replay identity, key overlap rotation, response classes, and external-side-effect limits; accepted through [PR #74](https://github.com/Ripan-Roy/epoch/pull/74) |
| WH-02 | Preserve existing command and snapshot bytes | Rust core | ✅ | Unsigned histories remain v1; signed targets, exact internal acquire, and terminal reject use v2. Golden, wrong-version, legacy snapshot, deterministic candidate, and full workspace tests pass protected `main` |
| WH-03 | Dispatch only from the current replicated owner | Rust regional runtime | ✅ | The current non-fail-stopped Event Bus leader commits and awaits an exact candidate lease, performs I/O outside the state machine, then commits Ack/retry/reject with deterministic proposal identity; protected Rust and container gates pass |
| WH-04 | Enforce HTTPS, SSRF, DNS, redirect, proxy, timeout, and metadata boundaries | Rust security | ✅ | HTTPS/public-only validation, explicit loopback development, per-attempt all-address DNS validation/pinning, IANA special-address corpus including IPv4-in-IPv6, no redirects/proxies, strict headers, discarded bodies, a complete DNS-plus-request lease cap with no expired-lease request, and bounded redacted key files pass protected CI; network egress proxy and penetration review remain open |
| WH-05 | Align target creation and verification in all first-party SDKs | Go + Java + Python | ✅ | Signed target constructors serialize the key ID; exact-body verifiers share one vector, constant-time comparison, canonical decimal/lowercase-hex parsing, timestamp tolerance, and replay identity. Full Go/Java/Python suites pass exact-main [CI `32365193683`](https://github.com/Ripan-Roy/epoch/actions/runs/32365193683) |
| WH-06 | Prove real retry, convergence, and recovery | Real-process integration | ✅ | A real loopback receiver returns 503 then 204; attempts 1/2 have the same delivery ID and distinct signatures. Every voter converges the failed/Ack history and returns it after all three processes reopen existing storage in protected CI |
| WH-07 | Publish end-to-end receiver and operator documentation | Docs + Pages | ✅ | Regional Bus guide, API/security/semantics/runtime docs, SDK READMEs, ADR, traceability, and the rendered docs page cover key setup, signed target creation, raw-body verification, replay storage, outcomes, and non-claims; main-only [Pages `32365193694`](https://github.com/Ripan-Roy/epoch/actions/runs/32365193694) published the verified live bundle |
| WH-08 | Pass full local and protected evidence | Quality + GitHub | ✅ | Complete `make check`, all-language build, real-process suite, Docker integration campaign, and displayed SDK quickstarts passed locally and on protected `main`; PR #74 squash-merged as `6acf316`, exact-main CI `32365193683`, Pages `32365193694`, and live-bundle verification are green |

## Current native delivery: Event Bus to Epoch Queue and Stream

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| ET-01 | Freeze ownership, binding, shard routing, commit order, and non-claims | ADR + semantics | ✅ | ADR-0031 defines source-leader ownership, exact generation/shard/tablet/epoch binding, Queue shard `0`, shared FNV-1a Stream routing, target-before-source settlement, and the non-atomic cross-tablet boundary; accepted through PR #77. |
| ET-02 | Preserve existing command and snapshot histories | Rust core | ✅ | Legacy v1 and signed v2 bytes remain stable; destination-bound exact acquire and snapshots require v3. Golden, restore, mislabeled-version, mismatch, and protected regression tests pass. |
| ET-03 | Prevent public lease bypass or destination rebinding | Rust API | ✅ | Legacy batch acquire stops at Queue/Stream work, exact public acquire without a binding conflicts, public JSON rejects `destination`, and queries expose the binding read-only; protected CI `32377688082` passes. |
| ET-04 | Execute from the source owner across independently led groups | Rust regional runtime | ✅ | The current source Bus leader resolves and pins the target, internally forwards to the destination group's known leader, awaits its committed receipt, and settles the source with deterministic proposal identities; the real regional campaign passes on protected `main`. |
| ET-05 | Avoid duplicate destination insertion after uncertain settlement | Rust profiles + consensus | ✅ | The target key excludes Bus attempt and includes source plus destination incarnations; a real three-voter lost-result replay proves exact committed-payload lookup without a duplicate, and all-voter reopen preserves one Queue message and one Stream record. |
| ET-06 | Publish truthful operator and SDK behavior | API + Go/Java/Python | ✅ | Topology exposes payload-free process-local counters; delivery queries expose browser-safe pinned coordinates; typed target constructors and executable provisioning/publish/query examples pass the three SDK gates and main-only Pages `32377688018`. |
| ET-07 | Prove real convergence and recovery | Real-process integration | ✅ | Three actual nodes deliver to Queue and keyed multi-shard Stream targets through differing leaders, converge source and destination state, reopen all voter storage, and retain exactly one destination record in protected CI. |
| ET-08 | Pass full local and protected evidence | Quality + GitHub | ✅ | [PR #77](https://github.com/Ripan-Roy/epoch/pull/77) squash-merged as `94c63a3`; exact-main CI `32377688082`, main-only Pages `32377688018`, full local gates, Docker integration, and displayed SDK quickstarts pass. |

## Current storage delivery: consensus checkpoints and snapshot catch-up

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| CP-01 | Freeze checkpoint bytes, ordering, and non-claims | Format + ADR | ✅ | EPSN v1 and ADR-0021 define canonical bytes, the 768 KiB ceiling, complete retry registry, fsync-before-install order, fixed voters, and backup/PITR/physical-reclamation non-claims; merged through [PR #57](https://github.com/Ripan-Roy/epoch/pull/57) |
| CP-02 | Preserve legacy EPRS histories | Rust storage | ✅ | EPRS kinds 1/2 remain readable and byte-compatible; additive kind 3 embeds EPSN plus a contiguous tail; the full regression passed exact-main [CI run 31490167750](https://github.com/Ripan-Roy/epoch/actions/runs/31490167750) |
| CP-03 | Create and reopen a durable compacted checkpoint | Rust consensus | ✅ | Tests prove idempotent creation, no generation advance on exact retry, checkpoint-plus-committed-tail reopen, lookup/digest preservation, and retained-index status on merge `febf5eb7` |
| CP-04 | Enforce the stable barrier and corruption policy | Rust storage | ✅ | Injected post-fsync failure fail-stops memory and reopens the durable result; malformed, foreign, wrong-voter/term, digest-corrupt, oversize, and noncontiguous images fail closed in exact-main CI |
| CP-05 | Catch up a lagging fixed voter through Raft snapshot | Rust consensus | ✅ | Persistent and three-container harnesses isolate a voter before the checkpointed proposal, compact the leader, commit a tail, heal, observe installation, and compare the complete committed result |
| CP-06 | Replace typed profile state before tail apply | Rust node + container | ✅ | Three real HTTP runtimes prove Catalog replay-before-tail convergence; the exact-main Container/Compose job independently forces a missing checkpoint prefix, installs it after restart, and converges both checkpointed and tail proposals |
| CP-07 | Expose truthful operator evidence and documentation | API + Docs + Pages | ✅ | Experimental POST trigger, local checkpoint/retained-first status, format/operations docs, traceability, changelog, and docs-site section passed main-only [Pages run 31490167676](https://github.com/Ripan-Roy/epoch/actions/runs/31490167676); the [live docs](https://ripan-roy.github.io/epoch/) serve the checkpoint and SDK content |
| CP-08 | Pass full local and protected pull-request evidence | Quality + GitHub | ✅ | `make check`, `make build`, multiprocess `SIGKILL`, deterministic three-container catch-up/failover, all PR checks, exact-main [CI](https://github.com/Ripan-Roy/epoch/actions/runs/31490167750), and live Pages passed; PR #57 was squash-merged as `febf5eb7` |

## Current storage delivery: profile-native checkpoints and physical reclamation

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| NC-01 | Freeze compatible bytes, bounds, ordering, and non-claims | Format + ADR | ✅ | ADR-0022, EPSN v2, and additive EPRS kind 4 specify a 4 MiB profile limit, 6 MiB image limit, 1,024-record/1 MiB retry suffix, final frame digest, durable order, and backup/PITR non-claims; merged through [PR #58](https://github.com/Ripan-Roy/epoch/pull/58) |
| NC-02 | Preserve EPSN v1 and legacy EPRS histories | Rust consensus | ✅ | V1 fixtures remain unchanged; v1 reopen, explicit v1-to-v2 conversion, and the complete all-features workspace regression passed locally and on protected `main` |
| NC-03 | Capture and install every native profile canonically | Rust profiles + tablets | ✅ | Catalog, Stream, Queue, Cache, and Event Bus validate complete scope/configuration, canonical bytes, state/payload digests, corruption, unknown version, and domain invariants in the protected all-features suite |
| NC-04 | Bound retry metadata without weakening live state | Rust consensus + profiles | ✅ | The newest exact-retry suffix survives checkpoint/reopen, aged-out IDs become `unknown`, and each profile retains matching typed receipts independently of complete live business state; consensus/profile regressions pass |
| NC-05 | Reclaim obsolete physical EPRS generations safely | Rust storage + consensus | ✅ | Locked sibling replacement, fsync/rename/parent-fsync order, orphan handling, corruption rejection, file-size reduction, and continued logical generation advancement pass the full Rust and multiprocess gates |
| NC-06 | Restore profiles before applying retained tails | Rust node + containers | ✅ | The profile-neutral lagging-voter test installs a Catalog image before its tail; every profile's real three-voter restart forces a checkpoint and restores before readiness; node plus regional/profile container campaigns pass |
| NC-07 | Publish architecture, operations, format, traceability, and docs-page content | Docs + Pages | ✅ | Repository docs and the rendered docs page describe v2 behavior/non-claims; main-only [Pages run 31503915546](https://github.com/Ripan-Roy/epoch/actions/runs/31503915546) published the [live docs](https://ripan-roy.github.io/epoch/) |
| NC-08 | Pass full local and protected pull-request evidence | Quality + GitHub | ✅ | Rust, Go, Python, Java, generated-contract, shell/workflow, SDK smoke, multiprocess crash, and all container gates passed; squash merge `3ef7554` and exact-main [CI run 31503915491](https://github.com/Ripan-Roy/epoch/actions/runs/31503915491) are green |

## Current Stream delivery: replicated retention policies

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| RT-01 | Freeze deterministic policy, byte, time, and compatibility semantics | Stream + ADR | ✅ | ADR-0023 fixes complete policy replacement, compact canonical JSON byte accounting, inclusive expiry, monotonic committed time, hard bounds, and unchanged v1/v2/v3 command bytes; merged in PR #60 |
| RT-02 | Enforce time, size, count, and combined deletion atomically | Rust Stream core | ✅ | Exact age, oldest-first combined enforcement, oversized-record rollback, dedupe removal, disabled bounds, snapshot validation, and full workspace regressions pass protected `main` |
| RT-03 | Replicate retention through a versioned command | Rust tablet | ✅ | Canonical v4 configure/maintain mutations return typed receipts, replay exactly, converge digests, reject kind/version drift, and preserve golden v1/v2/v3 behavior |
| RT-04 | Make consumer data loss explicit | Stream groups | ✅ | A checkpoint below the retained base is preserved and reported as `checkpoint_out_of_range`; lag counts readable retained records and fetch requires an explicit generation-fenced reset |
| RT-05 | Expose direct and regional operations in every first-party SDK | HTTP + Go/Java/Python | ✅ | Strict configure, maintain, and linearizable observe routes plus all three repository-local clients and executable quickstarts pass protected CI |
| RT-06 | Recover the exact retained boundary | Rust node + checkpoints | ✅ | Real three-voter checkpoint/reopen and regional Python SDK campaigns restore base/end/policy/watermark after leader and all-node loss |
| RT-07 | Publish truthful product and operator documentation | Docs + Pages | ✅ | Main-only [Pages run 31517466409](https://github.com/Ripan-Roy/epoch/actions/runs/31517466409) published retention docs to the [live site](https://ripan-roy.github.io/epoch/) |
| RT-08 | Pass full local and protected evidence | Quality + GitHub | ✅ | [PR #60](https://github.com/Ripan-Roy/epoch/pull/60) squash-merged as `674848b`; exact-main [CI run 31517466426](https://github.com/Ripan-Roy/epoch/actions/runs/31517466426) is green |

## Current Stream delivery: multi-shard key routing

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| MS-01 | Freeze logical/physical partition identity and remap safety | Semantics + ADR | ✅ | ADR-0024 defines one logical shard per tablet, compatible inner partition 0, FNV-1a UTF-8 routing, event-ID fallback, generation pinning, and online-expansion non-claims; merged through [PR #62](https://github.com/Ripan-Roy/epoch/pull/62) |
| MS-02 | Publish one cross-language partition contract | Rust + route discovery | ✅ | Discovery advertises algorithm/encoding/fallback/shard count; Rust/Go/Java/Python vectors cover ASCII, non-ASCII, empty-key fallback, and zero-shard rejection on protected `main` |
| MS-03 | Materialize and recover every Stream shard independently | Rust regional runtime | ✅ | Three-shard route/materializer tests validate shard count and logical identity, then shut down and reopen expanded catalog state without changing canonical scope/snapshot bytes |
| MS-04 | Externalize truthful logical partition identities | Rust Stream adapter | ✅ | Mutation states/receipts, batch records, fetch records, group checkpoints, retention observations, and status receive the outer shard; native snapshot reinstall rebinds runtime shard metadata |
| MS-05 | Implement keyed append in every first-party SDK | Go + Java + Python | ✅ | All three publish the same vectors, select shard 14 for `customer-42/16`, fall back to event ID, and fail before write on target-generation mismatch |
| MS-06 | Prove failover and all-node recovery across three shards | Compose integration | ✅ | Exact-main [CI run 31694162556](https://github.com/Ripan-Roy/epoch/actions/runs/31694162556) routes Python keys to shards 0/1/2, validates receipts/records/checkpoints, catches up the killed voter, then reopens every EPRS volume with per-shard counts intact |
| MS-07 | Publish executable end-to-end documentation | Docs + Pages | ✅ | ADR/API/architecture/semantics/testing/plan/traceability/SDK/changelog and exact Go/Java/Python keyed examples passed [Pages run 31694162565](https://github.com/Ripan-Roy/epoch/actions/runs/31694162565) and are visible on the [live site](https://ripan-roy.github.io/epoch/) |
| MS-08 | Pass full local and protected evidence | Quality + GitHub | ✅ | PR #62 squash-merged as `4d10f705`; exact-main CI and main-only Pages passed and the live bundle contains the partition contract and all three keyed examples |

## Current Stream delivery: coordinated consumer sessions

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| CS-01 | Freeze coordinator, generation, time, assignment, and non-claims | Semantics + ADR | ✅ | ADR-0025 defines shard-zero authority, canonical v5 commands, monotonic committed time, inclusive deadlines, lexical round-robin assignment, separate checkpoint fences, and explicit non-claims; accepted through PR #63 |
| CS-02 | Preserve historical Stream compatibility | Rust tablet contract | ✅ | v1–v4 commands remain pinned; snapshot v2 stores sessions while legacy snapshot v1 restores with an empty session map on protected `main` |
| CS-03 | Apply bounded membership and expiry deterministically | Rust Stream/tablet | ✅ | Tests cover join/rejoin, heartbeat, leave, stale/unknown fences, shard/member/timeout bounds, checked deadline overflow without phantom state, single-bump rebalance, monotonic time, inclusive expiry, exact retry, and canonical assignment |
| CS-04 | Expose strict direct and authenticated regional routes | Rust node + gateway | ✅ | Join/observe/heartbeat/leave/maintenance exist only on logical shard 0, inherit leader/term/generation/tablet/auth fences, and use a linearizable observation barrier |
| CS-05 | Implement one coordinator contract in three SDKs | Go + Java + Python | ✅ | All clients select shard 0 and expose join, heartbeat, leave, maintenance, and observation with pre-network timeout/generation/identifier validation and same-key bounded rediscovery |
| CS-06 | Prove convergence, checkpoint, and restore | Rust real-voter tests | ✅ | A three-voter test joins two members, fences heartbeat, expires/rebalances, compares every voter, installs a native checkpoint, and reopens the same state |
| CS-07 | Prove real SDK failover and all-node recovery | Compose integration | ✅ | Exact-main [CI run 31699302841](https://github.com/Ripan-Roy/epoch/actions/runs/31699302841) coordinates two Python members after leader replacement and preserves generation-3 assignment through voter catch-up and all-node reopen |
| CS-08 | Publish executable end-to-end documentation | Docs + Pages | ✅ | Main-only [Pages run 31699302769](https://github.com/Ripan-Roy/epoch/actions/runs/31699302769) published ADR/API/runtime/SDK guidance and exact Go/Java/Python lifecycle examples to the [live site](https://ripan-roy.github.io/epoch/) |
| CS-09 | Pass full local and protected evidence | Quality + GitHub | ✅ | [PR #63](https://github.com/Ripan-Roy/epoch/pull/63) squash-merged as `ec163008`; exact-main CI and Pages are green |

## Current Stream delivery: regional atomic batch SDKs

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| AB-01 | Freeze the first-party frame and retry contract | Semantics + ADR | ✅ | ADR-0026 defines canonical input, exact-frame identity, single-shard atomicity, built-in/caller-frame codec ownership, limits, and explicit native-streaming non-claims; accepted through PR #64 |
| AB-02 | Encode canonical none and gzip frames in every SDK | Go + Java + Python | ✅ | Cross-language tests pin Rust/Serde field order, compact UTF-8 JSON, recursively sorted object maps, Unicode, unique sequences, counts, and size ceilings on protected `main` |
| AB-03 | Accept caller-produced frames for every required codec | Go + Java + Python | ✅ | Typed frame constructors preserve exact none/gzip/LZ4/Snappy/Zstd bytes and reject unsupported codecs or inconsistent metadata before network I/O |
| AB-04 | Route and retry one atomic regional operation | Go + Java + Python | ✅ | `AppendBatch`/`appendBatch`/`append_batch` use the existing authenticated leader discovery and fences, preserve exact frame plus idempotency key across one bounded rediscovery, and target one explicit logical shard |
| AB-05 | Prove real failover and recovery | Compose integration | ✅ | Exact-main CI submits and exactly replays a two-record gzip SDK batch after leader loss, then verifies both correlated offsets after voter catch-up and all-node same-volume reopen |
| AB-06 | Publish executable end-to-end documentation | Docs + Pages | ✅ | SDK READMEs, PRD/API/semantics/runtime/testing/plan/traceability, ADR-0026, and exact Go/Java/Python gzip examples passed Pages `31706698723` and live-bundle verification |
| AB-07 | Pass complete local quality gates | Quality | ✅ | `make check`, `make build`, focused SDK tests, exact displayed-source compilation/restart, docs-only Pages assertions, and the complete regional Docker campaign passed locally and in exact-main CI `31706698718` |
| AB-08 | Pass protected pull-request and exact-main evidence | GitHub | ✅ | [PR #64](https://github.com/Ripan-Roy/epoch/pull/64) squash-merged as `ecffbbf`; exact-main CI `31706698718`, main-only Pages `31706698723`, and the live bundle with all three batch examples passed |

## Current regional delivery: leader-owned automatic maintenance

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| AM-01 | Freeze ownership, time, retry, and non-claim semantics | Semantics + ADR | ✅ | ADR-0027 fixes current-leader-only ownership, exact due-time commands, deterministic proposal identity, pure reads, bounded repeat sweeps, explicit-operation compatibility, and timer-SLO/dynamic-ownership non-claims |
| AM-02 | Publish pure next-deadline queries for every profile | Rust profile cores | ✅ | Focused tests cover oldest Stream record, first Stream session, Queue schedule/TTL/max-age/dedupe/lease, Cache value/lock, and Event Bus in-flight lease deadlines without state mutation |
| AM-03 | Build deterministic maintenance commands | Rust tablet adapters | ✅ | Stream retention/session, Queue, Cache, and Bus services emit exact due-time payloads and stable proposal IDs; bounded sweep identities include applied profile index where residual work can remain |
| AM-04 | Enforce leader-only proposal ownership | Rust consensus runtime | ✅ | Each scan reads actor role/term/fail-stop state, skips followers, observes committed/pending proposal state, proposes through the existing consensus actor, and treats leadership races as benign; broader partition/model evidence remains open |
| AM-05 | Bound and configure the scheduler | Rust node/runtime | ✅ | `EPOCH_REGIONAL_MAINTENANCE_INTERVAL_MS` and CLI accept 1–60,000 ms with 100 ms default; missed ticks are skipped and existing runtime failure handling remains intact |
| AM-06 | Expose authorized operational evidence | Rust topology | ✅ | `topology.read` response reports enabled/interval plus cumulative pass/tablet/leader/due/submitted/pending/error/last-pass/error observations; counters are explicitly process-local |
| AM-07 | Prove every profile after leader loss and recovery | Compose integration | ✅ | The three-node campaign removes client maintenance for every implemented timer, asserts submissions with zero errors, catches up each killed leader, and reopens all volumes |
| AM-08 | Align contracts, user docs, traceability, and Pages | Docs + console | ✅ | PRD, ADR-0027, API/runtime/architecture/semantics/plan/traceability/checklist/changelog and public docs content describe the merged implementation and boundaries |
| AM-09 | Pass complete local quality gates | Quality | ✅ | `make check`, `make build`, focused scheduler/topology tests, docs-only production bundle assertions, and the complete regional Docker campaign passed locally and in exact-main CI `31713406045` |
| AM-10 | Pass protected pull-request and exact-main evidence | GitHub | ✅ | [PR #65](https://github.com/Ripan-Roy/epoch/pull/65) squash-merged as `d888e92`; exact-main CI `31713406045`, main-only Pages `31713406105`, and live bundle verification passed |

## Current recovery delivery: automatic regional consensus checkpoints

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| AC-01 | Freeze local ownership and non-claims | Semantics + ADR | ✅ | ADR-0028 distinguishes every-voter local recovery from leader-owned replicated timers and excludes backup/PITR, cluster-wide barriers, byte/time policy, and production I/O budgeting; accepted through PR #67 |
| AC-02 | Make eligibility and creation actor-atomic | Rust consensus actor | ✅ | `checkpoint_if_applied_growth` validates a nonzero threshold, skips pending `Ready` work, and conditionally creates one native checkpoint as one actor command |
| AC-03 | Schedule catalog and every profile group | Rust regional runtime | ✅ | Configurable 1–600,000 ms interval and nonzero applied-entry threshold scan catalog plus all materialized routes on every voter; defaults are 1,000 ms and 1,024 entries |
| AC-04 | Preserve durable ordering and failure semantics | Rust consensus + storage | ✅ | The scheduler reuses EPSN v2 capture, fsync-before-install, atomic kind-4 EPRS replacement, native restore, and supervised fatal storage/profile failures without changing canonical formats |
| AC-05 | Expose authorized operational evidence | Rust topology | ✅ | `checkpoints` reports configuration, cumulative local counters, last error/pass, and decimal-string applied/checkpoint/retained-first boundaries for each hosted group |
| AC-06 | Prove all-voter compaction, failover, and reopen | Compose integration | ✅ | Exact-main CI observes creation and prefix compaction on all 24 voter/group copies, then proves sequential leader catch-up and durable boundaries after all-node `SIGKILL`/same-volume reopen |
| AC-07 | Align contracts, traceability, changelog, and Pages | Docs + console | ✅ | PRD, ADR-0028, API/checkpoint/runtime/architecture/semantics/plan/traceability/checklist/changelog and public docs content are published |
| AC-08 | Pass complete local quality gates | Quality | ✅ | `make check`, `make build`, focused node/consensus/topology tests, all-target Clippy, docs-only bundle assertions, Compose validation, and the rebuilt-image regional Docker campaign passed locally and in exact-main CI `31717722384` |
| AC-09 | Pass protected pull-request and exact-main evidence | GitHub | ✅ | [PR #67](https://github.com/Ripan-Roy/epoch/pull/67) squash-merged as `3f41804`; exact-main CI `31717722384`, main-only Pages `31717722351`, and live-bundle verification passed |

## Current Stream delivery: session-fenced consumption

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| FC-01 | Freeze claim, fetch, retry, and non-claim semantics | Semantics + ADR | ✅ | ADR-0029 defines offset preservation, monotonic claim bounds, exact-member/generation fetch, SDK revalidation, namespace auth, at-least-once behavior, and atomic/streaming non-claims; accepted through PR #68 |
| FC-02 | Preserve historical command and snapshot compatibility | Rust tablet | ✅ | Public goldens pin v6 claim; v1–v5 commands remain unchanged; snapshot v3 records the fence, accepts legitimate v1/v2 images, and rejects a v3 fence mislabeled as legacy; exact-main CI `31726157672` is green |
| FC-03 | Replicate an offset-preserving owner fence | Rust tablet | ✅ | Deterministic tests cover unowned/current/next generations, stale/conflicting/gap rejection, exact retry, unchanged next offset, replacement fencing, and state digest convergence; exact-main CI `31726157672` is green |
| FC-04 | Expose strict claim and claimed-fetch routes | Rust node + regional router | ✅ | Direct and authenticated regional v1 routes separate `data.write` claim from `data.read` linearizable fetch; wrong owner/generation returns typed 409 and profile failure remains 503; accepted through PR #68 |
| FC-05 | Implement one bounded protocol in three SDKs | Go + Java + Python | ✅ | All SDKs expose low-level claim/fetch and a resource helper that pins generation, validates sorted assignment, pre-plans at most 4,096 transitions, derives bounded keys, and revalidates shard 0; cross-language checks pass in exact-main CI `31726157672` |
| FC-06 | Prove real voter recovery and stale fencing | Rust + Compose | ✅ | Real three-voter tests converge and reopen the fence; the rebuilt Python/Compose campaign claims all three shards after leader loss, preserves typed stale-fetch fencing, catches up the old voter, and repeats after all-node reopen in exact-main CI `31726157672` |
| FC-07 | Publish executable end-to-end documentation | Docs + Pages | ✅ | Go/Java/Python quickstarts join, claim, fenced-fetch, and leave; PRD/API/runtime/SDK/semantics/testing/traceability/changelog/ADR/console are aligned, main-only Pages run `31726157684` deployed, and the live bundle exposes the claim guide and ADR-0029 |
| FC-08 | Pass local and protected quality evidence | Quality + GitHub | ✅ | `make check`, `make build`, all-target Clippy/Rustdoc, exact displayed quickstarts, docs-only Pages assertions, and the rebuilt regional Docker campaign pass locally and in protected CI; [PR #68](https://github.com/Ripan-Roy/epoch/pull/68) squash-merged as `c30c511`, exact-main CI `31726157672`, main-only Pages `31726157684`, and live-bundle verification passed |

## Current Stream delivery: complete state services

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| SS-01 | Freeze the advanced state model and non-claims | ADR + semantics | ✅ | ADR-0035 colocates producer, transaction, tier, capture, and replication truth with one tablet and explicitly excludes cross-shard atomicity, external-store authority, global superstream snapshots, and bandwidth SLO claims; accepted through PR #83. |
| SS-02 | Fence producers and preserve exact retry | Rust Stream/tablet | ✅ | Epoch/sequence validation, payload conflict, bounded contiguous history, v7 commands, browser-safe decimal positions, snapshot recovery, and real-voter exact retry pass locally and in protected CI. |
| SS-03 | Commit transactions and offsets atomically | Rust Stream/tablet | ✅ | Pending/committed/aborted visibility, 128-record bounds, one colocated offset commit, long-poll wakeup, snapshot reinstall, digest convergence, and full-cluster reopen pass protected `main`. |
| SS-04 | Compact keys and preserve tombstone semantics | Rust Stream | ✅ | Latest committed keyed values, unkeyed retention, aborted removal, inclusive tombstone expiry, sparse-offset v2 snapshots, and compact-before-tier rejection pass protected `main`. |
| SS-05 | Tier immutable history transparently | Rust Stream/tablet | ✅ | Exact covered ranges, canonical bytes, SHA-256, overlap/corruption rejection, read isolation, aborted-history reads, hot merge, and snapshot cross-validation pass protected `main`. |
| SS-06 | Recommend and safely expand partitions | Rust catalog/runtime + SDK | ✅ | Pure density advice, expand-only catalog fencing, stable existing tablet identities, new-shard allocation, materializer reopen, and generation-pinned keyed writes pass protected regression and regional recovery tests. |
| SS-07 | Serve pull, push, and dedicated consumers | Rust HTTP + SDK | ✅ | Strict bounded pull and long-poll queries, separate push/dedicated notification lanes, consumer ID rules, timeout, transaction visibility wakeup, and three SDK contracts pass protected CI. |
| SS-08 | Capture open formats automatically | Rust maintenance + SDK | ✅ | JSON Lines/Array artifacts, checksums, replicated schedules/checkpoints, stable due proposals, catch-up without drift, pending-transaction barriers, bounded rotation, reads, and recovery pass protected `main`. |
| SS-09 | Replicate cross-cluster batches safely | Rust Stream/tablet + SDK | ✅ | Contiguous source checkpoints, exact retry, source-to-local mappings, loop rejection, local cluster identity, snapshots, strict HTTP, and three SDKs pass protected CI and full-cluster recovery. |
| SS-10 | Merge logical superstreams consistently | Go + Java + Python | ✅ | All SDKs validate named members, independently fetch with explicit isolation, decorate identity, sort deterministically, apply one global limit, and declare the non-atomic snapshot scope; all three protected SDK suites pass. |
| SS-11 | Publish end-to-end user documentation | Docs + console + Pages | ✅ | Regional SDK guide, ADR, API/architecture/semantics/traceability, console feature surface, executable source snippets, and release notes are live through main-only Pages run `32485430871`. |
| SS-12 | Pass local, protected, and release evidence | Quality + GitHub | ✅ | [PR #83](https://github.com/Ripan-Roy/epoch/pull/83) squash-merged as `aa5d319`; exact-main CI `32485430646`, main-only Pages `32485430871`, displayed quickstarts, tag provenance, and the published `v0.1.0-alpha.7` prerelease are green. |

## Current Queue delivery: complete state services

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| QS-01 | Freeze capacity, ordering, dispatch, expiry, and forwarding semantics | ADR + contracts | ✅ | ADR-0036 defines exact admission, dedupe ordering, FIFO lock fences, priority aging, token bucket/breaker, temporary destinations, durable target binding, crash ordering, compatibility, and non-claims; protected review passed in PR #85. |
| QS-02 | Preserve historical Queue contracts | Rust Queue/tablet | ✅ | Existing command v1/v2, snapshot v1 reads, legacy recovery checksum golden, and result encodings pass protected CI; advanced operations use v3 and snapshot v2. |
| QS-03 | Implement bounded capacity, expiry, dedupe, and fairness | Rust Queue | ✅ | Count/byte admission, three overflow policies, non-lease victim choice, durable idle expiry, dedupe-before-overflow, and committed-time priority aging pass protected deterministic tests. |
| QS-04 | Implement sessions, dispatch protection, defer, and correlation | Rust Queue/tablet | ✅ | Exclusive renewable session locks, FIFO selection, rate/burst/concurrency, circuit transitions, exact defer/receive, correlation metadata, full digest, and snapshot recovery pass protected tests. |
| QS-05 | Deliver dead letters crash-safely | Rust node | ✅ | Quorum-only catalog admission, pending/bound/completed outbox, source-leader ownership, exact incarnation binding, stable target key, target-before-complete ordering, and recovery/key tests pass protected CI. |
| QS-06 | Persist configuration and expose strict regional APIs | Rust catalog/node | ✅ | Catalog normalization and materialization retain advanced configuration; strict v3 request conversion and linearizable advanced/correlation/outbox reads pass protected node tests. |
| QS-07 | Expose the complete lifecycle in every SDK | Go + Java + Python | ✅ | All three protected SDK suites pass session metadata/acquire/renew/release, defer/exact receive, advanced state, correlation, and outbox route/auth/fence contracts. |
| QS-08 | Prove failover, forwarding, convergence, and reopen | Compose | ✅ | The protected campaign kills the advanced Queue leader, runs the Python session/dedupe/correlation/defer lifecycle, observes the forwarded target DLQ record, catches up the stopped voter, kills every node, reopens the same volumes, and proves converged state. |
| QS-09 | Publish end-to-end documentation | Docs + Pages | ✅ | Queue guide/tablet/ADR, exact three-language quickstarts, PRD/traceability/checklist, API/semantics/testing docs, and public Pages content shipped in alpha.8. |
| QS-10 | Pass feature PR, exact-main gates, and release alpha.8 | Quality + GitHub | ✅ | PR #85 merged as `b7fa0d5`; exact-main CI `32514143406`, main-only Pages `32514143435`, tag provenance, and the published `v0.1.0-alpha.8` prerelease are green. |

## Current Event Bus delivery: integration platform

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| EI-01 | Freeze state, delivery ordering, security, compatibility, and non-claims | ADR + contracts | ✅ | ADR-0037 is reviewed and published with the replicated ownership, lease/checkpoint ordering, recovery, egress, idempotency, compatibility, and alpha non-claims. |
| EI-02 | Complete routing, filters, transforms, rate limits, retention, and redrive | Rust Bus/tablet | ✅ | Focused engine/tablet tests and the protected alpha.9 matrix cover filters, transformation/enrichment, integer rate/burst, DLQ retention/redrive, archive retention, and bounded long poll. |
| EI-03 | Replicate schemas, MQTT state, catalog, endpoints, functions, and connectors | Rust Bus/tablet | ✅ | Strict registries, compiler-backed schema revisions and policies, MQTT state, endpoint/catalog/function state, connector outcomes/checkpoints/replay, canonical snapshots, and semantic restore validation pass. |
| EI-04 | Execute managed HTTP and connector targets safely | Rust node | ✅ | Protected tests cover lease-before-I/O, CloudEvents modes, API key/bearer/OAuth, bounded secret files, allowlists, public-only DNS pinning, no redirects/proxies, failover, and checkpoint ordering. |
| EI-05 | Persist Bus configuration and expose strict regional APIs | Rust catalog/node | ✅ | Catalog, materialization, authenticated/fenced integration operations, recovery, and executor topology evidence shipped together. |
| EI-06 | Expose the complete regional lifecycle in every SDK | Go + Java + Python | ✅ | Go, Java, and Python expose the complete bounded native integration lifecycle, including typed schema registration/policy/removal and linearizable explicit validation, with route/fence/idempotency/pre-network validation tests and published examples. |
| EI-07 | Prove real failover, side effects, convergence, and reopen | Real three-process runtime | ✅ | The process and Compose campaigns prove structured delivery identity, Ack convergence, leader replacement, voter catch-up, all-node reopen, and cross-language quickstarts. |
| EI-08 | Bound every live and recovery state path | Rust Bus/tablet | ✅ | Clone-staged capacity admission and semantic restore validation cover identities, revisions, references, receipts, checkpoints, URLs, MQTT state, canonical bytes, and digests. |
| EI-09 | Publish end-to-end documentation and honest feature coverage | Docs + console + Pages | ✅ | PRD, ADR-0037, traceability, SDK/API/runtime/security/testing guides, executable examples, release notes, and main-only Pages are published. |
| EI-10 | Pass one large feature PR and release alpha.9 | Quality + GitHub | ✅ | PR #86 merged with protected CI/Pages green; annotated tag `v0.1.0-alpha.9` points at exact `main` and the curated GitHub prerelease is published. |

## Current schema compiler and validation delivery

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| SC-01 | Compile the three PRD schema formats | Rust Bus | ✅ | Apache Avro, JSON Schema meta-validation, and proto2/proto3 descriptor compilation reject malformed definitions before registry mutation. |
| SC-02 | Derive payload validation from definitions | Rust Bus | ✅ | Official-format payload tests pass without the legacy structural-field overlay; JSON errors are masked and every error is bounded. |
| SC-03 | Enforce adjacent-revision compatibility | Rust Bus | ✅ | Avro reader/writer, conservative JSON object-contract, and Protobuf number/type/cardinality/oneof/required rules pass compatible and incompatible revision tests. |
| SC-04 | Separate producer advice and broker enforcement | Rust Bus + node | ✅ | A four-mode matrix plus a real three-voter HTTP route proves read-only producer validation and committed broker rejection. |
| SC-05 | Expose typed lifecycle APIs in every P0 SDK | Go + Java + Python | ✅ | Registration, policy upsert/removal, and explicit validation have typed models, fail-fast validation, exact route/header tests, and compiling end-to-end quickstarts. |
| SC-06 | Preserve deterministic recovery and security bounds | Rust Bus | ✅ | Legacy snapshots default the additive root-message field; restore recompiles definitions, external references/imports are prohibited, and payload values are not reflected. |
| SC-07 | Publish end-to-end schema documentation | Docs + Pages | ✅ | The regional Bus guide and visible docs page cover all three languages, formats, compatibility, validation modes, limits, and non-claims. |

## Current product/runtime closure delivery

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| PRC-01 | Automatically ingest bounded HTTP/CloudEvents source batches | Rust node | ✅ | Protected alpha.10 tests cover active-leader polling, safe egress, strict batch/cursor validation, error routing, fair iteration, and topology counters. |
| PRC-02 | Preserve source exactly-replayable crash ordering | Rust Bus/node | ✅ | Protected alpha.10 evidence covers stable proposal identities, record-before-checkpoint ordering, and real three-process crash/restart convergence. |
| PRC-03 | Reconcile a runnable fixed-voter Kubernetes topology | Go operator | ✅ | The alpha.10 CRD/controller, data PVCs, durable control owner, Services, anti-affinity, security, placement identity, mounts, and status passed protected race tests. |
| PRC-04 | Fail closed and reconcile without write churn | Go operator | ✅ | Alpha.10 rejects missing policy/credentials before workload creation, treats API defaults as no-op, and repairs owned drift under protected tests. |
| PRC-05 | Ship Kubernetes install and image definitions | Deployment | ✅ | The protected alpha.10 Kustomize and source-image definitions validate; the beta train extends their artifact contract separately below. |
| PRC-06 | Ship generated-contract management CLI | Go control | ✅ | Alpha.10 shipped strict manifests, bounded input, qualified CRUD, retry identities, OCC, stable output, and HTTP/gRPC diagnostics. |
| PRC-07 | Publish executable operations documentation | Docs + console | ✅ | Alpha.10 published the source connector, CLI, Kubernetes lifecycle, Pages deployment guide, and ADR-0038. |
| PRC-08 | Synchronize and verify `v0.1.0-alpha.10` | Release | ✅ | Rust, Go, Java, Python, TypeScript, user-agent, and lock metadata were synchronized in the published tag. |
| PRC-09 | Pass the complete local release matrix | Quality | ✅ | Protected CI, build, integration, Kubernetes configuration, source images, and docs-only assertions passed for alpha.10. |
| PRC-10 | Merge and publish the product/runtime closure | GitHub | ✅ | PR #89 merged; exact-main CI/Pages passed and the annotated `v0.1.0-alpha.10` prerelease was published. |

## Current alpha-exit delivery: learner-first voter replacement

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| MR-01 | Persist current, bootstrap, and target membership canonically | Rust Catalog | 🟡 | Command/snapshot v5, exact replay, stale generation/epoch, conflicting plan, direct placement, multi-voter rejection, and snapshot reopen pass locally. |
| MR-02 | Preserve committed membership across journal reopen | Rust consensus | 🟡 | Durable three/five-voter `ConfState`, learner and joint entries, catch-up promotion guard, stable-store reopen, and focused consensus histories pass locally. |
| MR-03 | Materialize the transition target safely | Rust node | 🟡 | Current-or-target hosting starts the incoming runtime from immutable bootstrap identity without restarting retained voters; finalization stops only the removed host. |
| MR-04 | Reconcile one action from committed state | Rust node | 🟡 | The leader-only worker reconstructs add-learner, catch-up, reconfigure, and Catalog-finalize actions; restart cannot skip a gate. |
| MR-05 | Keep customer generations and management state coherent | Go + Rust | 🟡 | Membership maintenance preserves the customer resource generation; Go adopts policy-compliant finalized placement and reports active transitions as pending. |
| MR-06 | Expose authorized transition evidence | API + Protobuf + BFF | 🟡 | Cluster-scoped `catalog.apply`, decimal-safe plan IDs, current/bootstrap/target/committed/reachable voter fields, and generated Go contracts pass local auth and race tests. |
| MR-07 | Prove data continuity and full reopen | Real four-node runtime | 🟡 | A Stream moves from voters 1/2/3 to 1/2/4, retains committed data on node 4, stops node 3, shuts down every runtime, and reopens the same new voter set locally. |
| MR-08 | Publish complete operating documentation | Docs + Pages | 🟡 | Architecture, API, runtime, operator, traceability, delivery, and the dedicated replacement runbook are synchronized locally; docs-only bundle and live Pages remain. |
| MR-09 | Pass protected replacement evidence | GitHub + Kubernetes | 🟡 | The exact-source local Kubernetes campaign now passes backup-before-replacement, refreshed snapshot catch-up, joint replacement, guarded rollout, fresh restore, digest equality, and post-restore writes. Full frozen-tree matrix, protected PR CI/Pages, exact-main rerun, and release evidence remain required. |

## Current alpha-exit delivery: initial source adapters

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| SA-01 | Keep protocol I/O outside replicated state transitions | Rust node | 🟡 | HTTP, object, PostgreSQL, MySQL, and Kafka readers return one bounded normalized batch; the shared worker alone proposes Bus records and checkpoints. Full node tests and Clippy pass locally. |
| SA-02 | Preserve record-before-checkpoint crash ordering | Rust Bus/node | 🟡 | Stable connector/batch/index identities, committed applied/error outcomes, exact source fencing, and post-checkpoint acknowledgement reuse the existing replicated connector contract. HTTP real-process failover/reopen and deterministic adapter recovery tests pass locally. |
| SA-03 | Read immutable object stores safely | Rust object adapter | 🟡 | S3-compatible, Azure Blob/Data Lake, and GCS builders enforce bounded lexical scans, conditioned reads, immutable version/ETag/size cursors, overwrite detection, record/byte ceilings, formats, allowlists, and typed credentials. MinIO conformance passes locally; live Azure/GCS remains. |
| SA-04 | Read committed PostgreSQL transactions | Rust PostgreSQL adapter | 🟡 | Bounded `pgoutput` assembly emits only at Commit LSN, source-fences cursors, defaults to verified TLS/mTLS, and sends applied-LSN feedback only after the Epoch checkpoint. PostgreSQL 17 conformance passes locally. |
| SA-05 | Read complete MySQL row-binlog transactions | Rust MySQL adapter | 🟡 | Exact file/position, GTID/BEGIN/XID/COMMIT, rotation, partial-transaction replay, bounded buffers, allowlists, and verified TLS/mTLS pass deterministic tests and MySQL 8 conformance locally. |
| SA-06 | Fence Kafka groups with Epoch-authoritative offsets | Rust Kafka adapter | 🟡 | Assignment generation seeks from replicated per-partition cursors, `read_committed` plus disabled auto-store/commit prevent gaps, and synchronous group commit follows the Epoch checkpoint. Kafka 4 conformance passes locally. |
| SA-07 | Close stale source sessions | Rust runtime | 🟡 | Leadership loss, route loss, connector pause/deletion, and source-identity change drop PostgreSQL/Kafka sessions; live tests assert release locally. |
| SA-08 | Keep credentials and transport fail closed | Rust security | 🟡 | Strict bounded `connector_credentials` values are redacted, database/broker hosts require allowlists, verified TLS is default, and plaintext is loopback-development only. The full node suite exposed and now covers explicit Rustls provider selection with mixed connector dependencies. |
| SA-09 | Publish executable contracts and SDK lifecycle | Docs + console | 🟡 | ADR-0040, PRD, API, architecture, runtime, semantics, security, testing, traceability, Event Bus SDK, source guide, README, checklist, changelog, and visible Pages content are synchronized locally; docs bundle verification remains. |
| SA-10 | Pass real protocols in local and protected CI | CI + GitHub | 🟡 | Pinned MinIO/PostgreSQL/MySQL/Kafka Compose tests pass 4/4 locally and CI owns a separate conformance job with failure logs and volume cleanup. Workflow validation and protected evidence remain. |

## Current alpha-exit delivery: OCI supply chain

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| OCI-01 | Build the complete runtime artifact set | Docker | 🟡 | Node, control, operator, CLI, and compatibility gateway images build from synchronized source with digest-pinned bases, explicit non-root users, exact entrypoints, and bounded component contents. All five clean local builds pass. |
| OCI-02 | Bind artifacts to exact source metadata | Docker + release | 🟡 | OCI title, description, source, documentation, MIT license, vendor, synchronized version, exact Git revision, exact entrypoint, non-root user, and credential-free defaults are required by one fail-closed inspection script. All five local images pass, while scratch-fixture root and revision drift are rejected. |
| OCI-03 | Keep pull requests non-publishing | GitHub Actions | 🟡 | PR CI builds and inspects all five images without registry login or push, generates five SPDX JSON documents, validates their version/package structure, and retains short-lived evidence. Protected execution remains. |
| OCI-04 | Publish only an exact current-main tag | GitHub Actions + GHCR | 🟡 | The only publication trigger is pushed `v*`; synchronized version, checked-in notes, exact current `main`, and successful CI/Pages push runs at that SHA are required. Matching native runners push untagged platform results, and a bounded finalize stage creates the only public exact tag after receiving exactly one amd64 and one arm64 digest. There is no `latest` or manual publication path. Protected beta.3 tag execution remains. |
| OCI-05 | Bind provenance and SBOMs to immutable digests | GitHub Actions | 🟡 | BuildKit provenance travels with each native platform result and GitHub provenance targets the assembled manifest; separate SPDX SBOMs and GitHub attestations target every amd64/arm64 runtime digest, yielding ten retained release assets. A local registry assembly proof and fail-closed helper tests pass; protected registry verification remains. |
| OCI-06 | Sign and verify every manifest | Sigstore + GitHub | 🟡 | Pinned Cosign keylessly signs each manifest and verifies the exact repository workflow/tag identity and GitHub OIDC issuer; `gh attestation verify` gates release publication. Protected tag execution remains. |
| OCI-07 | Publish consumer verification and non-claims | Docs + Pages | 🟡 | ADR-0041, release-artifact guide, security, operator, releasing, PRD, traceability, README, changelog, and public deployment page define digest-first verification, architecture/SBOM scope, no-`latest` policy, and deferred packages/raw binaries. Docs build/Pages remain. |
| OCI-08 | Prove clean pull and protected release | GitHub + Kubernetes | ⬜ | Beta.2 passed protected PR/main CI, Pages, and live Kubernetes, but its emulated arm64 node release build timed out before a prerelease existed. Merge beta.3, pass its exact-main CI/Pages and native tag workflow, then pull exact published digests into a clean environment and verify signatures, attestations, SBOM assets, and the curated prerelease. |

## Current alpha-exit delivery: resumable load, fault, and soak evidence

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| LF-01 | Drive the real mixed-profile recovery workload | Python integration | 🟡 | One round wraps the real regional Go/Rust/Python campaign and requires Cache, Stream, Queue, Event Bus, control loss, five profile-leader losses, all-voter loss/reopen, and six typed invariants. The local accelerated round passed in 48,434 ms. |
| LF-02 | Resume without hiding incomplete work | Python runner | 🟡 | Atomic state marks running/passed/failed/interrupted attempts, retries only the unfinished round with a new attempt number, and never counts failed or offline time. Unit recovery and one real round pass locally. |
| LF-03 | Prevent evidence from mixing builds | Python runner | 🟡 | Resume binds the canonical plan, Git revision/version, full tracked/untracked source hash, clean/dirty state, image ID/version/revision, platform, and Python version. Identity drift rejection passes locally. |
| LF-04 | Bind every result and log | Python runner | 🟡 | Safe relative regular-file receipts include byte count and SHA-256; symlinks, traversal, duplicate keys, noncanonical JSON, false/missing invariants, and artifact tampering fail closed. Focused tests pass locally. |
| LF-05 | Sign only complete evidence | OpenSSL + Python | 🟡 | The runner rejects unsafe/private-key-in-evidence paths and modes, signs canonical `epoch.soak.evidence/v1` with Ed25519, records the DER public-key fingerprint, publishes the manifest last, and immediately re-verifies signature, plan, duration, event log, attempts, and artifacts. Local key-boundary/signature/tamper tests and independent real-bundle verification pass. |
| LF-06 | Keep accelerated and operating claims separate | Test contract + docs | 🟡 | The accelerated profile requires one complete round and marks itself harness-only. The 30-day profile requires 2,592,000,000 successful active milliseconds, cannot be shortened by round budget, and claims no throughput, latency, managed-service SLO, or production certification. |
| LF-07 | Run the protected accelerated profile | GitHub Actions | 🟡 | Container CI now signs and verifies the same four-profile campaign it already ran, uploads public evidence for 30 days, and keeps its ephemeral private key outside the upload. Protected execution remains. |
| LF-08 | Accumulate long-duration operating evidence | Dedicated environment | ⬜ | Run the exact-build `thirty-day` profile to completion with a separately trusted public key, then review failures, saturation/load shape, platform scope, and SLO non-claims. The elapsed 30-day gate cannot be completed by accelerated CI. |

## Current alpha-exit delivery: live Kubernetes lifecycle

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| KAE-01 | Create a pinned disposable N-node environment | Python + Kind | 🟡 | One control-plane and four worker nodes run Kubernetes `v1.34.0` from a digest-pinned Kind image. The campaign validates exact image IDs, mTLS inputs, storage topology, node identities, and cleanup on success, failure, or interrupt. |
| KAE-02 | Prove every managed profile through the real operator | Kubernetes + Go + Rust | 🟡 | A four-physical-node cluster with three Catalog voters reaches ready, then Stream, Cache, Queue, and Event Bus each commit and converge real traffic through authenticated control and data paths. |
| KAE-03 | Prove encrypted backup and compacted-log voter replacement | Rust + operator | 🟡 | An AES-256-GCM semantic backup completes before one observed three-of-four Stream placement replaces a voter. The incoming learner receives a refreshed native snapshot after compaction, catches up, enters the target voter set, and preserves the Stream digest. |
| KAE-04 | Prove the guarded rollout state machine | Go operator + maintenance CLI | 🟡 | An image request remains frozen until a post-request backup succeeds; all four ordinals then produce preflight, leadership-drain, exact-image readiness, and postflight receipts one at a time. Both tags intentionally resolve to one image ID, so mixed-version compatibility is not claimed. |
| KAE-05 | Prove fresh-cluster restore and continued writes | Kubernetes + Rust | 🟡 | A separate `EpochCluster` restores the final encrypted object, matches exact Catalog and all-profile state digests, and commits another traffic sequence after restore. No RPO/RTO or production SLO is inferred. |
| KAE-06 | Retain fail-closed machine-verifiable evidence | Python + CI | 🟡 | Atomic `evidence.json`, SHA-256 manifest, scoped source/deployment identity, image IDs, Kubernetes version, step receipts, result digests, and failure diagnostics are locally validated. CI uploads the same bundle for 30 days. |
| KAE-07 | Close the protected beta gate | GitHub + release | ⬜ | The frozen candidate must pass the new protected `Live Kubernetes alpha-exit lifecycle` job, exact-main CI/Pages, and the published-digest rerun before beta release publication. |

## Current security delivery: bootstrap trust baseline

| ID | Checklist item | Boundary | State | Evidence / acceptance |
|---|---|---|---:|---|
| AUTH-01 | Version and bound a shared fingerprint-only policy | Go + Rust contract | ✅ | JSON Schema, example, strict unknown-field/duplicate/size/action/scope rejection, and one cross-language decision corpus pass protected `main` CI |
| AUTH-02 | Authenticate managed HTTP and gRPC before handlers | Go ingress | ✅ | Missing/malformed/invalid credentials return 401/Unauthenticated; health and CORS preflight alone remain public |
| AUTH-03 | Authorize parsed actions and scopes before state access | Go service | ✅ | Apply/Get/Delete deny before registry/reconciler access; List/inventory filter every unauthorized tenant record |
| AUTH-04 | Authenticate the Go-to-Rust service path | Go egress | ✅ | Every catalog/route request sends the distinct bounded credential from `EPOCH_CONTROL_REGIONAL_TOKEN`; redirect rejection remains enabled |
| AUTH-05 | Reauthenticate catalog, route, and data actions | Rust ingress | ✅ | Regional middleware separates catalog apply/delete/read, route read, and data read/write at the decoded tenant scope |
| AUTH-06 | Emit bounded credential-free decisions | Go + Rust audit | ✅ | Structured request/principal/policy/action/decision/reason/scope events validate bounds and expose no token/payload field |
| AUTH-07 | Keep browser credentials out of static artifacts | TypeScript console | ✅ | Interactive password field stores only in `sessionStorage`; token validation, clear, test, typecheck, lint, and build pass protected `main` CI |
| AUTH-08 | Exercise authenticated recovery topologies | Process + Compose | ✅ | Rust real-process recovery and Go-to-Rust container campaign use mounted policy plus separate admin/control credentials |
| AUTH-09 | Record limits without overstating G5 | Documentation | ✅ | ADR-0011 and security/API/runtime/development/testing/traceability docs keep OIDC, mTLS/TLS, encryption, replicated policy, immutable export, and quotas explicit |
| AUTH-10 | Pass protected pull-request evidence | GitHub | ✅ | PR #40 was squash-merged and exact main SHA `3898e90` passed required CI and Pages deployment |

## Pull request checklist

| Check | Required action | Evidence |
|---|---|---|
| Scope | Name the PRD/traceability IDs and explicit non-claims | PR description and updated traceability row |
| Design | Record a new boundary or irreversible choice in an ADR | ADR link, or “not applicable” with reason |
| TDD | Capture the failing behavior before implementation | Focused regression/acceptance test |
| Clean design | Keep profile, storage, transport, control, and UI ownership separate | Dependency review and narrow public interfaces |
| Correctness | Test success, rejection, idempotency, fencing, overflow, and restart paths | Unit/property/history tests |
| Compatibility | Regenerate versioned contracts and reject stale bindings | `make generate-check`, Buf lint/breaking policy |
| Quality | Format, lint, type-check, test, document, and audit every changed language | `make check` |
| Build | Build every deliverable affected by the change | `make build` |
| Integration | Run real-process/container tests proportional to the changed guarantee | Named integration command and CI link |
| Documentation | Update API, architecture, testing, development, and user docs where affected | Linked files and Pages preview |
| Release staging | Add user-visible changes and limitations under `Unreleased` | [Changelog](../CHANGELOG.md) |
| Merge | Use protected PR checks; never bypass a red required check | Green PR checks and squash merge |

## Release checklist

| Order | Release action | Required evidence | State for next release |
|---:|---|---|---:|
| 1 | Select a completed, merged milestone boundary | Compiler-backed schema registry, regional validation, three SDKs, docs, and traceability pass locally; protected merge remains | 🟡 |
| 2 | Choose the next semantic prerelease version | `v0.2.0-beta.4` is the next feature release and does not already exist | ✅ |
| 3 | Synchronize Rust, Go, Java, Python, TypeScript, SDK user agents, and lockfiles | `./scripts/check-release-version.sh` passes locally at `0.2.0-beta.4`; protected verification remains | 🟡 |
| 4 | Write curated, version-controlled release notes | `docs/releases/v0.2.0-beta.4.md` names behavior, artifacts, migration, verification, compatibility, and beta limitations; merge remains | 🟡 |
| 5 | Pass protected `main` CI and main-only Pages | Both workflow runs green at the same commit | ⬜ |
| 6 | Verify the live docs show the release and governance/SDK content | Public Pages bundle assertions | ⬜ |
| 7 | Create an annotated tag at the exact current `main` commit | Local and remote commit IDs match | ⬜ |
| 8 | Pass tag/version/main provenance workflow | Release-tag workflow green | ⬜ |
| 9 | Publish the GitHub release from the checked-in notes | GitHub prerelease targets `v0.2.0-beta.4` | ⬜ |
| 10 | Verify downloads and package claims | Four OCI manifests and eight platform SBOM assets match the notes; package-manager publication remains deferred | ⬜ |
| 11 | Start the next `Unreleased` section | Changelog prepared for continued delivery | ✅ |

After `v0.2.0-beta.4` completes this sequence, the table above intentionally
resets for the next release.

## Feature delivery template

Copy this table into an issue or design note for each bounded feature.

| Phase | Question | Done when | State |
|---|---|---|---:|
| Contract | What exact behavior and failure semantics are promised? | Versioned contract and non-claims are reviewed | ⬜ |
| Tests first | Which test proves the behavior is currently absent? | Focused test fails for the intended reason | ⬜ |
| Implementation | What is the smallest vertical path that satisfies it? | Code is clean, bounded, and independently reviewable | ⬜ |
| Recovery | What survives timeout, crash, restart, and stale ownership? | Deterministic and real-process histories pass | ⬜ |
| Security | Who may perform it, and what is audited or redacted? | Authorization and audit tests pass | ⬜ |
| Observability | How does an operator distinguish healthy, pending, degraded, and failed? | Metrics/status/conditions are asserted | ⬜ |
| Integration | Which real clients and deployment modes exercise it? | Named end-to-end matrix passes | ⬜ |
| Documentation | Can a user execute and understand it without hidden steps? | Exact published examples and limitations pass | ⬜ |
| Traceability | Which PRD IDs moved, and what remains? | Evidence register is precise and current | ⬜ |
| Release | Is it merged, versioned, and represented in release notes? | Protected `main`, changelog, notes, and artifact claims agree | ⬜ |

## Maintenance rule

Update this checklist in the same pull request that changes a delivery state.
Never mark a row ✅ from local evidence alone, and never replace an explicit
open item with an unqualified “done.”
