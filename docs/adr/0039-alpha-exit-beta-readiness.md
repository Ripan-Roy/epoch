# ADR-0039: Alpha exit and beta-readiness boundary

- Status: Accepted
- Date: 2026-08-23
- Owners: data plane, control plane, operator, SDK, release engineering
- Supersedes: the operational non-claims in ADR-0038 when every gate below has
  protected evidence

## Context

`v0.1.0-alpha.10` closes Epoch's native product/runtime development train, but
it deliberately keeps fixed voters, plaintext transport, source-only images,
internal-only checkpoints, and manual upgrades. Calling that surface beta would
make the word "beta" meaningless. The next feature must therefore close the
operational boundary as one vertical release rather than spread mutually
dependent safety work across small pull requests.

This ADR defines the one alpha-exit feature. It does not weaken the later
compatibility, managed-service, or GA gates in the PRD.

## Decision

The next feature PR is accepted only when all of the following slices are
implemented, tested locally, documented, and represented by one synchronized
beta prerelease candidate.

### 1. Secure transport and workload identity

- Public HTTP and gRPC listeners use TLS in the supported deployment.
- Data-plane peer traffic and control-to-data traffic use mutually authenticated
  TLS with a configured trust bundle. A missing certificate, key, or trust
  bundle fails startup before a socket serves application traffic.
- Server certificates are verified against the requested DNS name. Client
  certificates establish workload identity at the transport boundary; bearer
  policy remains an independent authorization layer.
- The operator mounts referenced TLS material, emits only `https` peer and
  control endpoints, and reports missing or malformed references as a
  fail-closed condition.
- Go, Java, and Python clients plus the management CLI support a custom CA and
  optional client certificate without disabling hostname verification.

### 2. Portable backup and restore

- A regional backup is a bounded, checksummed, versioned manifest containing a
  catalog checkpoint plus every materialized tablet checkpoint at one declared
  high-water mark.
- Backup creation first obtains quorum read barriers, then captures canonical
  native snapshots. It never labels a partial node-directory copy as a backup.
- Restore validates format, checksums, group identity, resource identity, and
  size limits before mutating a fresh destination. Existing non-empty regional
  state is rejected.
- The operator schedules backups, records the last successful object and
  failure condition, and supports an explicit restore reference during cluster
  creation. Encryption is required at the configured object/PVC boundary.

### 3. Guarded upgrade and voter replacement

- An image change becomes an explicit upgrade plan, not an immediate
  StatefulSet rollout. The controller checks backup freshness and cluster
  health, transfers leadership away from the target voter, updates at most one
  voter, waits for catch-up, and stops on degradation.
- Membership changes use joint-consensus configuration entries. Odd voter sets
  of three or five are supported; a replacement is add learner, catch up,
  promote jointly, then remove the old voter. Epoch/group fencing continues to
  reject stale traffic.
- Stable state, snapshots, transport admission, and status expose the committed
  membership. A process restart cannot revert to the bootstrap voter list.

### 4. Initial production source adapters

- S3-compatible/object storage, PostgreSQL CDC, MySQL CDC, and Kafka sources
  implement the same leader-owned record-before-checkpoint contract as HTTP.
- Each adapter has a bounded batch and response size, explicit source position,
  stable record identity, secret reference, allowlist/private-egress policy,
  retry classification, lag/status, and deterministic replay test.
- Adapter protocol libraries remain behind a small source-reader interface so
  connector I/O cannot mutate replicated state directly.

### 5. Releasable artifacts and operating evidence

- GitHub Actions builds node, control, operator, and CLI images from the exact
  release tag, publishes them to GHCR, generates SPDX SBOMs, and attaches
  provenance/signatures. Pull requests build and inspect the same images but do
  not publish.
- A local reproducible campaign covers TLS abuse, backup/restore equivalence,
  one-at-a-time upgrade, voter replacement, connector replay, process loss,
  and all-node reopen.
- A live-Kubernetes smoke creates a cluster, writes through every native
  profile, backs it up, replaces a voter, upgrades it, restores into a fresh
  cluster, and compares canonical state.
- The soak runner is resumable and emits a signed evidence manifest. Accelerated
  tests may validate the harness, but they do not replace the PRD's elapsed
  30-day operating evidence; beta source distribution may begin while the
  managed-service SLO claim remains explicitly withheld.

## Architecture boundaries

- Rust remains authoritative for membership, snapshots, restore validation,
  connector positions, and profile state.
- Go owns desired upgrade/backup schedules and Kubernetes orchestration. It may
  call versioned Rust APIs but never reads or rewrites Raft journals.
- The operator is a state machine whose transitions are persisted in CR status;
  reconciliation must be idempotent after controller restarts.
- SDK transport configuration is immutable per client and keeps secrets out of
  debug/string representations.
- Every network and file input is bounded and strictly decoded. Unknown fields,
  duplicate identities, checksum mismatches, path traversal, and ambiguous
  credentials fail closed.

## Test-first delivery rule

Each slice starts with focused failing tests for its security and recovery
contract. Implementation follows only after the intended failure is observed.
The complete local matrix must pass with race detection, Clippy warnings denied,
strict format/lint/type checks, container recovery, and the live-cluster smoke
before the feature branch is pushed for protected review.

On 24 August 2026, the exact-source local live-cluster campaign passed the full
same-binary lifecycle described above across four physical nodes. It also
exposed and drove a test-first fix for learner admission after backup compaction:
the leader refreshes the membership-bearing native snapshot and reports the
older in-flight snapshot stale before retry. Protected pull-request and
exact-main evidence are still required by this decision.

## Consequences

This is intentionally a large feature PR because the safety properties depend
on each other: an upgrade without backup, a backup without restore, or mTLS
without client support is not an alpha exit. Review remains tractable through
separate modules, narrow interfaces, test evidence, and the delivery checklist.

The feature does not claim full Redis/Kafka/AMQP protocol compatibility,
cross-region failover, billing, a hosted 99.95% SLO, package-manager SDK
publication, arbitrary voter counts, or GA production readiness.
