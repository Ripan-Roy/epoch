# Epoch Delivery Checklist

**Last reviewed:** 29 July 2026
**Current release:** `v0.1.0-alpha.3`
**Current core target:** select the next dependency-critical M2 feature

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
| G2 | Storage and recovery | 🟡 | Checksummed formats, crash recovery, corruption policy, snapshots, compaction, retention, tiering | Segmented WAL and EPRS pass; snapshots, compaction, tiering, and restore campaigns remain open |
| G3 | Consensus, catalog, and placement | 🟡 | Quorum safety, persistent catalog, multi-group supervision, membership, placement, repair, read barriers | Dedicated catalog consensus, shared multi-group supervision, deterministic materialization, fixed placement, fenced routing, leader replacement, and same-volume recovery pass locally; dynamic membership, constraint-aware placement, transfer/repair, and read barriers remain open |
| G4 | Native profile cores | 🟡 | Cache, Stream, Queue, and Bus P0 semantics with truthful public routing and fault evidence | All four typed tablet cores run simultaneously behind resource/shard routing and pass process/container recovery; stable authenticated native APIs and remaining P0 breadth are open |
| G5 | Trust and observability | ⬜ | Identity, authorization, TLS/mTLS, encryption, audit, telemetry, quotas, explain | Security boundary is documented; production controls and fault evidence remain open |
| G6 | Compatibility gateways | ⬜ | Named protocol/client matrix, differential tests, fuzzing, malformed frames, migration evidence | Native APIs only; RESP3, Kafka, AMQP, MQTT compatibility is not claimed |
| G7 | Data services and integrations | 🟡 | Schemas, pipes, connectors, target execution, checkpoints, transaction boundaries | Bus intent/outbox passes; external target executors, schemas, and connectors remain open |
| G8 | Managed operations | 🟡 | Durable Go reconciliation, operator, autoscaling, backup, metering, billing, private networking | Single-owner bbolt desired/status/token/tombstone transactions, Go `SIGKILL` recovery, RegionalAdmin reconciliation, Rust HTTP authority adapter, truthful placement status, exact-origin browser BFF, and real Go-to-Rust recovery pass locally; replicated multi-instance metadata and operator/autoscaling/backup/metering/billing/private networking remain open |
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
| MT-09 | Materialize typed profile tablets from committed catalog state | Rust regional control | MT-06, MT-08 | 🟡 | Catalog apply/delete/reconcile tests and real process/container runs create Cache, Stream, Queue, and Bus tablets together and recover them from committed state; online shard transfer remains open |
| MT-10 | Route public requests by resource, shard, leader, generation, and epoch | Rust gateway | MT-09 | 🟡 | Experimental resource/shard discovery and data dispatch reject stale generation, stale tablet epoch, nonleaders, missing routes, and profile mismatches; stable authenticated native routing and read barriers remain open |
| MT-11 | Reconcile hosted desired state through the Rust authority | Go control plane | MT-06, MT-10 | 🟡 | Real gRPC lifecycle, transactional bbolt desired/status/token/tombstone state, exact replay and generation continuity across restart, corruption/version/exclusive-owner rejection, conflict/disconnect/reconnect, generation-fenced status, multi-endpoint authority sampling, and Go-to-Rust container reconciliation pass locally; multi-instance consistency and authorization remain open |
| MT-12 | Show achieved placement and risk without overclaiming | TypeScript console | MT-10, MT-11 | 🟡 | The console reads only the Go BFF, preserves decimal 64-bit IDs, distinguishes pending/ready/degraded/failed, and lists observed voters/leaders plus topology non-claims; browser visual/accessibility automation remains open |
| MT-13 | Real-process and container fault campaign | Test infrastructure | MT-06–MT-12 | 🟡 | Three real processes and three containers cover catalog plus five resources/four profiles, Go-to-Rust apply/BFF, Go control `SIGKILL`/same-file reopen/exact replay, leader `SIGKILL`, two-voter degradation, catch-up, all-node `SIGKILL`, same-volume reopen, and digest convergence locally; this increment's protected-branch CI and broader crash/I/O faults remain |
| MT-14 | Documentation, traceability, changelog, and release notes | Cross-cutting | MT-13 | 🟡 | ADR-0010, architecture, APIs, testing, development, traceability, checklist, published docs content, and `Unreleased` notes describe durable single-owner control metadata and its non-claims; final version notes and release follow merge and green `main` |

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
