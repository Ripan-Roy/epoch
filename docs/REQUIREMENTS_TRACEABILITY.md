# Epoch Requirements Traceability

This register turns the prioritized catalog in [PRD.md](./PRD.md) into a delivery and verification index. It is intentionally terse: the PRD remains the source of semantic detail, while this document owns milestone, dependency, status, and evidence tracking.

Last synchronized with PRD version 0.3 on 22 July 2026.

## How to use this register

Status values are:

- **Slice** — part of the foundational vertical slice. This means it is in the first implementation scope, not that its acceptance evidence already exists.
- **Planned** — assigned to a later milestone; implementation has not started.
- **Deferred** — intentionally outside the committed delivery milestones.

All evidence is currently pending. Replace each evidence placeholder with a durable link to a test run, model-check report, benchmark, drill record, conformance report, security review, or release artifact. A feature is not complete merely because code exists.

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

## Cache and State

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| CACHE-001 | P0 | Core scalar and collection types | M1 prototype → M2 | Slice | G0, G1, G4 | Pending: type/property and persistence matrix |
| CACHE-002 | P0 | Key/default TTL and expiry events | M1 → M2 | Slice | G0, G1, G2, G4 | Pending: deterministic-clock expiry history |
| CACHE-003 | P0 | Eviction policy family | M1 prototype → M2 | Slice | G0, G4, G5 | Pending: memory-pressure policy benchmark |
| CACHE-004 | P0 | Shard-local atomic operations | M1 prototype → M2 | Slice | G0, G3, G4 | Pending: linearizability report |
| CACHE-005 | P0 | Pipeline, multiplex, batch, pool guidance | M1 → M2 | Slice | G1, G4 | Pending: ordering and throughput suite |
| CACHE-006 | P0 | CAS, optimistic transaction, increment, fenced lock | M2 | Planned | G0, G3, G4 | Pending: concurrency history and stale-token test |
| CACHE-007 | P0 | Volatile, replicated-memory, quorum modes | M1 prototype → M2 | Slice | G0, G2, G3, G4 | Pending: durability fault matrix |
| CACHE-008 | P1 | Snapshot, WAL restore, backup, PITR | M1 snapshot prototype → M3 | Slice | G2, G5, G7 | Pending: checksummed restore/PITR drill |
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
| STREAM-001 | P0 | Partitioned append log and key routing | M1 prototype → M2 | Slice | G0, G1, G2, G4 | Local WAL restart + partial-tail HTTP suite; pending: segmented/replicated recovery |
| STREAM-002 | P0 | Time/size/combined retention | M1 basic → M2 | Slice | G0, G2, G4 | Pending: segment-boundary retention suite |
| STREAM-003 | P0 | Consumer groups, offsets, lag, reset/replay | M2 | Slice | G0, G2, G3, G4, G5 | Local offset restart/lag suite; pending: coordinated group history |
| STREAM-004 | P0 | Partition order and acknowledgement policy | M1 prototype → M2 | Slice | G0, G2, G3, G4 | fsync-before-apply failure test + local restart; pending: replicated ack matrix |
| STREAM-005 | P0 | Zone replication, election, ISR visibility | M1 prototype → M2 | Slice | G2, G3, G5 | Pending: node/zone chaos report |
| STREAM-006 | P0 | Batching and required compression paths | M2 | Planned | G2, G4, G6 | Pending: round-trip corpus and compression benchmark |
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
| QUEUE-001 | P0 | Competing consumers and delivery transitions | M1 prototype → M2 | Slice | G0, G2, G4 | Pending: ack/redelivery history check |
| QUEUE-002 | P0 | Renewable visibility/acquisition lease | M1 prototype → M2 | Slice | G0, G2, G3, G4 | Pending: monotonic timer and stale-owner suite |
| QUEUE-003 | P0 | Durability-aware publisher confirmation | M1 prototype → M2 | Slice | G0, G2, G3 | Pending: acknowledgement-point fault matrix |
| QUEUE-004 | P0 | Delayed and scheduled messages | M1 prototype → M2 | Slice | G0, G2, G4 | Pending: schedule precision load report |
| QUEUE-005 | P0 | Retry/backoff/jitter/attempt-age policy | M1 prototype → M2 | Slice | G0, G2, G4 | Pending: deterministic policy corpus |
| QUEUE-006 | P0 | Provenance-rich DLQ and redrive | M1 prototype → M2 | Slice | G2, G4, G5 | Pending: poison/redrive audit history |
| QUEUE-007 | P1 | TTL, queue expiry, capacity and overflow | M3 | Planned | G0, G2, G4 | Pending: lifecycle/capacity boundary suite |
| QUEUE-008 | P1 | FIFO sessions and renewable lock | M5 | Planned | G0, G2, G3, G4 | Pending: per-session order/fencing history |
| QUEUE-009 | P1 | Dedupe identifier and window | M5 | Planned | G0, G2, G7 | Pending: restart/window suppression suite |
| QUEUE-010 | P1 | Fair priority bands | M5 | Planned | G0, G2, G4 | Pending: eligibility/starvation benchmark |
| QUEUE-011 | P0 | Credit/prefetch and consumer concurrency | M1 native → M2 | Slice | G0, G4, G6 | Pending: flow-control saturation suite |
| QUEUE-012 | P1 | Dispatch shaping and circuit breaker | M5 | Planned | G4, G5, G8 | Pending: downstream protection load report |
| QUEUE-013 | P2 | Deferred retrieval by identifier | M6 | Planned | G2, G4, G5 | Pending: deferred lifecycle/access suite |
| QUEUE-014 | P2 | Request/reply and temporary destinations | M6 | Planned | G0, G4, G6 | Pending: correlation/cleanup failure suite |
| QUEUE-015 | P1 | At-least-once quorum DL forwarding | M5 | Planned | G2, G3, G7 | Pending: crash-boundary forwarding history |

## Event Bus and Pub/Sub

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| BUS-001 | P0 | Topics, subscriptions, route/fan-out/wildcards | M1 basic → M2 | Slice | G0, G1, G4 | Pending: route truth-table/property suite |
| BUS-002 | P0 | Attribute and JSON-content filters | M4 | Planned | G0, G4 | Pending: compiled/interpreted differential suite |
| BUS-003 | P0 | Pull, push, webhook, queue, stream targets | M4 | Planned | G0, G4, G5, G6 | Pending: per-target failure/backpressure suite |
| BUS-004 | P0 | Per-target retry, timeout, rate, DLQ | M4 | Planned | G0, G2, G4, G5 | Pending: target-isolation history |
| BUS-005 | P0 | CloudEvents 1.0 over HTTP | M1 envelope → M4 | Slice | G0, G1, G6 | Pending: CloudEvents conformance/round-trip |
| BUS-006 | P1 | Archive and filtered replay | M4 | Planned | G2, G5, G7 | Pending: archive/replay reconciliation |
| BUS-007 | P1 | Declarative input transformation | M4 | Planned | G0, G4, G7 | Pending: deterministic transform golden suite |
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
| MGD-002 | P0 | Automatic placement and online rebalance | M1 prototype → M2 | Slice | G2, G3, G5 | Pending: constraint/rebalance chaos report |
| MGD-003 | P1 | Policy-bound multidimensional autoscaling | M4 | Planned | G5, G8 | Pending: hysteresis/headroom load report |
| MGD-004 | P0 | Multi-zone replicas and failover | M1 prototype → M2 | Slice | G2, G3, G5 | Pending: node/zone loss SLO report |
| MGD-005 | P1 | Geo DR, switch, promotion, failback | M4 → M5 | Planned | G3, G8, G9 | Pending: RPO/RTO and split-brain drill |
| MGD-006 | P1 | Backup, validation, semantic PITR | M3 | Planned | G2, G5, G7, G8 | Pending: scheduled restore evidence |
| MGD-007 | P1 | Guarded rolling upgrades | M5 | Planned | G3, G5, G6, G8, G10 | Pending: mixed-version stop/rollback drill |
| MGD-008 | P0 | Unified workload identity and authorization | M4 | Planned | G0, G5, G6 | Pending: authorization differential matrix |
| MGD-009 | P1 | Private ingress and controlled egress | M4 | Planned | G5, G8 | Pending: cloud connectivity/isolation report |
| MGD-010 | P0 | Transit/at-rest encryption and managed keys | M4 | Planned | G2, G5, G8 | Pending: TLS/storage/rotation report |
| MGD-011 | P0 | Immutable audit and history export | M1 basics → M4 | Slice | G2, G5, G8 | Pending: required-event/export reconciliation |
| MGD-012 | P0 | Telemetry, dashboards, alerts, OTel | M1 basics → M2 | Slice | G1, G5 | Pending: golden-signal and alert fault suite |
| MGD-013 | P1 | Metering, budget, quotas, anomaly alerts | M4 → M5 | Planned | G5, G8 | Pending: raw-usage/billing reconciliation |
| MGD-014 | P0 | CLI, core SDKs, emulator, operator | M1 core → M2 | Slice | G1, G5, G10 | Pending: artifact/lifecycle/e2e matrix |
| MGD-015 | P2 | Connector marketplace and lifecycle | M6 | Planned | G5, G7, G8, G10 | Pending: install/upgrade/provenance suite |
| MGD-016 | P1 | Customer-managed key rotation | M4 | Planned | G5, G8 | Pending: revoke/rotate/recover drill |
| MGD-017 | P1 | Terraform provider | M3 | Planned | G1, G5, G10 | Pending: plan/apply/import/drift suite |

## Control Plane

| ID | Pri | Capability shorthand | Milestone | Status | Dependency gates | Verification evidence placeholder |
|---|---:|---|---|---|---|---|
| CTRL-001 | P0 | Idempotent declarative resource API | M1 → M2 | Slice | G0, G1, G3 | Pending: token replay/unknown-outcome suite |
| CTRL-002 | P0 | Strong versioned metadata and OCC | M1 prototype → M2 | Slice | G0, G2, G3 | Pending: metadata linearizability report |
| CTRL-003 | P0 | Placement/residency/tenancy constraints | M2 | Planned | G0, G3, G5 | Pending: solver/admission corpus |
| CTRL-004 | P0 | Safe topology and repair operations | M1 prototype → M2 | Slice | G2, G3, G5 | Pending: transition chaos/history report |
| CTRL-005 | P0 | Safe admission and limiting-resource reason | M2 | Planned | G3, G5, G8 | Pending: reserve/saturation rejection report |
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
| DX-001 | P0 | Official Go, Java, and Python SDKs | M1 one SDK → M2 | Slice | G0, G1, G4, G10 | Java/Python HTTP unit + real-node smoke slices and Go generated bindings; pending: native contract/version matrix for all three |
| DX-002 | P0 | Generated guarantee-aware API docs | M1 → M2 | Slice | G0, G1, G10 | Pending: doc lint and executable examples |
| DX-003 | P0 | Deterministic single-binary emulator | M1 → M2 | Slice | G1, G2, G4, G10 | Pending: seeded replay/fault suite |
| DX-004 | P0 | Test containers and ephemeral namespaces | M1 → M2 | Slice | G1, G5, G10 | Pending: parallel lifecycle/isolation CI |
| DX-005 | P1 | Audited/redacted console message browser | M3 → M4 | Planned | G5, G7, G8 | Pending: access/redaction/action audit matrix |
| DX-006 | P0 | Explain live guarantees and cost drivers | M1 basic → M2 | Slice | G0, G3, G5 | Pending: live-state reconciliation suite |
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
| PKG-001 | P0 | Selective four-profile Rust node | M1 scaffold → M4 complete | Slice | G1, G4, G10 | Pending: feature/config startup matrix |
| PKG-002 | P0 | Shared engine/format standalone and cluster | M1 → M2 | Slice | G1, G2, G3, G10 | Pending: format and semantic equivalence suite |
| PKG-003 | P0 | Standalone without hosted Go services | M1 | Slice | G1, G2, G10 | Rust node restart/recovery test; pending: extended disconnected lifecycle suite |
| PKG-004 | P0 | Three-node quorum/failover/placement | M1 prototype → M2 | Slice | G2, G3, G10 | Pending: three-node fault report |
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
