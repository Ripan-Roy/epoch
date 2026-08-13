# Epoch Delivery Checklist

**Last reviewed:** 13 August 2026
**Current release:** `v0.1.0-alpha.3`
**Current core target:** Regional atomic Stream batch SDKs and recovery

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
| G0 | Semantic contracts | 🟡 | Versioned envelope, errors, durability, ordering, delivery, time, fencing, and transaction limits | [Semantics](SEMANTICS.md), [API contracts](API_CONTRACTS.md); transaction limits remain open |
| G1 | Repository and deterministic foundation | 🟡 | Reproducible toolchains, generated contracts, deterministic clock/fault harness, cross-language build | [Development](DEVELOPMENT.md), [Testing](TESTING.md), CI; broader fuzz/formal harness remains open |
| G2 | Storage and recovery | 🟡 | Checksummed formats, crash recovery, corruption policy, snapshots, compaction, retention, tiering | Segmented WAL, compatible EPSN v1/v2 checkpoints, native images for all five profiles, checkpoint-plus-tail reopen, fixed-voter catch-up, physical EPRS reclamation, and replicated Stream logical time/size/combined retention pass protected `main`. Three-shard Stream recovery passes locally; tiering, backup/PITR, scheduled restore campaigns, and production repair remain open |
| G3 | Consensus, catalog, and placement | 🟡 | Quorum safety, persistent catalog, multi-group supervision, membership, placement, repair, read barriers | Dedicated catalog consensus, shared multi-group supervision, deterministic multi-shard materialization, authenticated fixed-voter region/zone/class validation, limiting group-capacity admission, fenced routing, safe leader ReadIndex barriers, leader replacement, and per-shard same-volume recovery pass locally; dynamic membership/voter selection, rack placement, transfer/repair, follower reads, and broader model evidence remain open |
| G4 | Native profile cores | 🟡 | Cache, Stream, Queue, and Bus P0 semantics with truthful public routing and fault evidence | All four typed tablet cores run simultaneously behind resource/shard routing and pass process/container recovery; all four have authenticated versioned three-language clients. External Event Bus target execution and remaining P0 breadth are open |
| G5 | Trust and observability | 🟡 | Identity, authorization, TLS/mTLS, encryption, audit, telemetry, quotas, explain | Shared Go/Rust bootstrap authentication, scoped action authorization, collection isolation, session-only console credential, and credential-free decision logs pass protected `main` CI; OIDC, expiry/revocation, TLS/mTLS/peer identity, encryption, replicated policy, immutable audit export, telemetry, and quotas remain open |
| G6 | Compatibility gateways | ⬜ | Named protocol/client matrix, differential tests, fuzzing, malformed frames, migration evidence | Native APIs only; RESP3, Kafka, AMQP, MQTT compatibility is not claimed |
| G7 | Data services and integrations | 🟡 | Schemas, pipes, connectors, target execution, checkpoints, transaction boundaries | Bus intent/outbox passes; external target executors, schemas, and connectors remain open |
| G8 | Managed operations | 🟡 | Durable Go reconciliation, operator, autoscaling, backup, metering, billing, private networking | Single-owner bbolt desired/status/token/tombstone transactions, Go `SIGKILL` recovery, RegionalAdmin reconciliation, complete topology inventory, pre-catalog capacity rejection, truthful placement status, exact-origin browser BFF, and real Go-to-Rust recovery pass locally; replicated multi-instance metadata and operator/autoscaling/backup/metering/billing/private networking remain open |
| G9 | Geo | ⬜ | Replication, RPO/RTO, promotion, failback, residency, split-brain drills | Not implemented |
| G10 | Release readiness | 🟡 | Synchronized versions, CI, Pages, notes, verified tag provenance, artifacts, security and compatibility statements | `v0.1.0-alpha.3` passes the source-prerelease workflow; packaged artifacts and GA evidence remain open |

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
| MT-13 | Real-process and container fault campaign | Test infrastructure | MT-06–MT-12 | 🟡 | Three policy-protected regional containers cover topology/capacity, catalog plus seven tablets/four profiles including a three-shard Stream, authenticated Go recovery, and separate active-leader losses followed by real Python Stream sessions plus Stream, Queue, Cache, and Event Bus lifecycles, voter catch-up, all-node `SIGKILL`, same-volume reopen, and digest convergence locally; this increment's protected-branch CI and broader crash/I/O/auth-abuse faults remain |
| MT-14 | Documentation, traceability, changelog, and release notes | Cross-cutting | MT-13 | 🟡 | ADR-0010–0025, architecture, security, APIs, testing, development, SDK/runtime guides, traceability, checklist, published docs content, and `Unreleased` notes describe the implemented boundaries and non-claims; final version notes and release follow merge and green `main` |

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
| RC-04 | Implement the complete current lifecycle in three SDKs | Go + Java + Python | ✅ | `RegionalCacheClient` plus typed values, CAS expectations, transaction mutations, and lock guards expose all nine mutations and lookup/observation/status reads with fail-fast bounds |
| RC-05 | Prove exact contracts and validation | SDK tests | ✅ | Go, Java, and Python suites cover encoded paths, auth/fences/term, all value kinds, every lifecycle route, decimal 64-bit encoding, linearizable reads, transaction bounds, and invalid values before network |
| RC-06 | Prove the SDK after Cache leader loss and full restart | Compose integration | ✅ | Real Python runs exact replay, all value kinds, CAS, increment, transaction, lock renewal/guarded delete, expiry, 12-command convergence, old-voter catch-up, and all-voter EPRS reopen |
| RC-07 | Publish executable end-to-end docs | Docs + Pages | ✅ | Regional Cache guide, ADR, cross-cutting docs, exact Go/Java/Python programs, docs compile gate, navigation, and Pages content assertions are published on the live docs site |
| RC-08 | Pass protected pull-request evidence | GitHub | ✅ | PR #52 merged as `4afea088`; exact-main CI `31076973784` and Pages `31076973783` passed and the live Pages bundle contained the Cache SDK guide and maintenance example |

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
| AB-01 | Freeze the first-party frame and retry contract | Semantics + ADR | 🟡 | ADR-0026 defines canonical input, exact-frame identity, single-shard atomicity, built-in/caller-frame codec ownership, limits, and explicit native-streaming non-claims; protected review is pending |
| AB-02 | Encode canonical none and gzip frames in every SDK | Go + Java + Python | 🟡 | Cross-language tests pin Rust/Serde field order, compact UTF-8 JSON, recursively sorted object maps, Unicode, unique sequences, counts, and size ceilings locally |
| AB-03 | Accept caller-produced frames for every required codec | Go + Java + Python | 🟡 | Typed frame constructors preserve exact none/gzip/LZ4/Snappy/Zstd bytes and reject unsupported codecs or inconsistent metadata before network I/O |
| AB-04 | Route and retry one atomic regional operation | Go + Java + Python | 🟡 | `AppendBatch`/`appendBatch`/`append_batch` use the existing authenticated leader discovery and fences, preserve exact frame plus idempotency key across one bounded rediscovery, and target one explicit logical shard |
| AB-05 | Prove real failover and recovery | Compose integration | 🟡 | The complete local Docker campaign submits and exactly replays a two-record gzip SDK batch after leader loss, then verifies both correlated offsets after voter catch-up and all-node same-volume reopen; protected execution is pending |
| AB-06 | Publish executable end-to-end documentation | Docs + Pages | 🟡 | SDK READMEs, PRD/API/semantics/runtime/testing/plan/traceability, ADR-0026, and exact Go/Java/Python gzip examples are updated locally; Pages publication is pending |
| AB-07 | Pass complete local quality gates | Quality | 🟡 | `make check`, `make build`, focused SDK tests, exact displayed-source compilation/restart, docs-only Pages assertions, and the complete regional Docker campaign pass on the final local diff; protected execution is pending |
| AB-08 | Pass protected pull-request and exact-main evidence | GitHub | ⬜ | Feature PR, merge SHA, exact-main CI, main-only Pages run, and live-bundle evidence are pending |

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
| 1 | Select a completed, merged milestone boundary | Traceability and changelog agree with `main` | ⬜ |
| 2 | Choose the next semantic prerelease version | Version has not been tagged or released | ⬜ |
| 3 | Synchronize Rust, Go, Java, Python, TypeScript, SDK user agents, and lockfiles | `./scripts/check-release-version.sh` | ⬜ |
| 4 | Write curated, version-controlled release notes | `docs/releases/vX.Y.Z.md` with highlights, verification, compatibility, and limitations | ⬜ |
| 5 | Pass protected `main` CI and main-only Pages | Both workflow runs green at the same commit | ⬜ |
| 6 | Verify the live docs show the release and SDK content | Public Pages bundle assertions | ⬜ |
| 7 | Create an annotated tag at the exact current `main` commit | Local and remote commit IDs match | ⬜ |
| 8 | Pass tag/version/main provenance workflow | Release-tag workflow green | ⬜ |
| 9 | Publish the GitHub release from the checked-in notes | Non-draft release URL and correct prerelease flag | ⬜ |
| 10 | Verify downloads and package claims | Only artifacts named in the notes are present | ⬜ |
| 11 | Start the next `Unreleased` section | Changelog prepared for continued delivery | ✅ |

`v0.1.0-alpha.3` completed this sequence as a source-only prerelease. The table
above intentionally resets for the next release.

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
