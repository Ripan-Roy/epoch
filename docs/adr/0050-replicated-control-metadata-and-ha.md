# ADR-0050: Replicated regional control metadata and active-owner fencing

- Status: Accepted
- Date: 2026-09-16
- Owners: control plane, regional data plane, operator
- Supersedes: [ADR-0010](0010-durable-managed-metadata.md) for managed regional deployments
- Extends: [ADR-0002](0002-rust-go-boundary.md), [ADR-0009](0009-regional-tablet-catalog.md), [ADR-0047](0047-automatic-topology-repair-and-rebalance.md)

## Context

The original managed-control slice acknowledged desired resources, status,
generation tombstones, and request-token results to one Go-owned bbolt file.
That made process restart deterministic, but it also made one control pod the
metadata authority. Running more replicas would create split ownership, stale
status publication, duplicate placement decisions, and a storage migration
problem. Kubernetes restart ordering could improve availability but could not
provide multi-instance linearizability.

The Rust regional Catalog already owns the consensus boundary for native
resource generations, tablets, membership, and capacity. Managed metadata must
share that boundary without moving customer data or storage-engine ownership
into Go.

## Decision

1. Replicate managed desired documents, observed status, generation high-water
   marks, tombstones, request-token outcomes, and a resumable change log in the
   Rust Catalog consensus group. Go control replicas are replaceable clients;
   their local lease cache and resource count are never acknowledgement
   authority.
2. Serve every managed read after a Catalog `ReadIndex` barrier. Apply one or
   1–128 strictly sorted desired resources as one command, bind the request
   token to the exact canonical command, and commit deterministic semantic
   rejection outcomes so a stale or invalid committed proposal cannot poison
   later application.
3. Permit only one current Go reconciler owner to mutate materialized state.
   A replicated lease contains owner ID, monotonically increasing fence, and
   logical expiry. Status, materialization, membership planning, and managed
   deletion carry that lease guard. A standby observes a live foreign lease and
   does not challenge it before expiry. It may submit a user-requested delete
   with the observed active guard so public traffic needs no sticky routing;
   periodic reconciliation remains owner-only.
4. Make admission and materialization one transaction. A managed reconcile
   contains the desired and native Catalog generation fences, all resource
   placements, and a complete node-capacity observation. Rust rejects partial,
   stale, or overcommitted observations before changing any resource. A
   learner-first membership plan reserves the current/target union by the same
   rule.
5. Delete managed desired state and its native Catalog materialization in one
   lease-fenced command. Public HTTP and gRPC deletion must delegate through
   the reconciler; they may not call the desired-state store directly.
6. Expose durable operation lookup and bounded change pages. Operation records
   retain canonical affected resource identities even for rejected commands so
   the Go API can authorize the exact tenant set. The gRPC watch emits both the
   Catalog high-water mark and an explicit scanned `next_cursor`; filtered
   changes still advance the checkpoint. Stale and future cursors fail
   explicitly. Cursor zero is valid only while the retention floor is one; it
   becomes stale after prefix compaction and therefore cannot skip history.
7. Run three `epoch-control` StatefulSet replicas with stable pod-derived owner
   IDs, required host anti-affinity, ordered startup, and a two-instance
   disruption budget. The replicas share no writable control database.
8. Migrate the previous bbolt registry exactly once. Ordered pod zero reads a
   consistent live-resource and generation/tombstone image, imports it into an
   empty Catalog atomically, then later replicas start. The old database remains
   mounted as rollback evidence; its historical token ledger is not promoted
   because the old records do not contain the new canonical command shape.
   The atomic import accepts at most 4,096 generation records within the
   existing 512 KiB command and 4 MiB native-checkpoint envelope. An existing one-replica
   StatefulSet updates and verifies ordinal zero before scaling to three. A
   valid former local identity with no organization/project/environment maps to
   `epoch-legacy/local/default` in both live and tombstone records. Mapping is
   deterministic and a collision with an already-qualified identity fails
   before the import request.
9. Page internal resource inventory with a canonical key cursor, at most 128
   resources and 768 KiB per page, and one stable high-water cursor across the
   scan. Bound recurring lease, status, reconciliation, and membership request
   outcomes to the newest internal suffix while keeping public operation
   outcomes durable. Bind reconciliation and membership attempt tokens to the
   complete volatile lease/capacity evidence. Native application checkpoint v2
   stores raw Catalog bytes under the 4 MiB consensus envelope, reserves 64
   bytes for its binary header, and does not duplicate the consensus retry
   suffix; retained public token outcomes reconstruct replayed mutations while
   recurring internal outcomes follow their bounded policy. Continue reading
   v1 JSON/base64 images during upgrade. A subsequent checkpoint accepts an
   omitted internal receipt only when the installed image already covers its
   commit index. Bound watch history by both 4,096 entries and the remaining
   complete encoded snapshot byte budget. If no history suffix can make a new
   command fit, persist a token-bound non-mutating capacity rejection rather
   than failing the post-consensus applier. If unique rejected commands consume
   the reserved rejection space, persist snapshot format v8 and seal Catalog
   growth. The format version is the seal marker, so the transition adds no
   snapshot payload field. A sealed Catalog rejects ordinary commands and
   no-op deletes without state growth, but applies a delete when it removes live
   state and strictly shrinks the complete encoded snapshot. Incremental cleanup
   retains the seal until the ordinary rejection reserve is restored, then
   resumes admission under recovered snapshot format v9. Bound every Go
   authority response to 5 MiB: the 4 MiB Catalog checkpoint ceiling plus a
   1 MiB JSON/envelope allowance. This preserves synchronous resolution for a
   valid large desired-state batch while retaining a finite peer-read bound.
   Native checkpoint restore reconstructs exact consensus-retained retries in
   both sealed and recovered states.
   Token-only operation lookup for commands first received during sealing is
   not retained; migrate or shard a Catalog that cannot reclaim enough space.
10. Expose each durable operation's command kind and the original desired-delete
    generation precondition. Replay compares that retained command value,
    including the distinction between no precondition and generation zero,
    rather than inferring it from a result generation that may be a retained
    tombstone high-water mark.
11. Derive the current native generation inside a lease-fenced managed-delete
    transition instead of accepting the controller's asynchronously published
    status value. This closes the crash window between committed
    materialization and status publication. New atomic managed-delete commands
    and snapshots use format v7; legacy v6 commands with an explicit native
    generation fence remain canonical and readable.

## Consequences

A control process or Kubernetes node may fail without losing acknowledged
managed metadata. A stale controller cannot publish status, reserve capacity,
plan membership, or delete after ownership changes. Desired batches and managed
deletion no longer expose partially committed Go/Rust state. Existing regional
data paths remain independent of Go availability.

The Catalog consensus group now carries management metadata and therefore has
an explicit scaling boundary. Horizontal metadata sharding/capacity, a public
request-token retention window, long-duration lease-clock fault evidence, and
protected multi-control chaos remain open.
Capacity sealing is a safety boundary rather than a scaling mechanism: it
preserves availability for reads, checkpointing, deterministic refusals, and
strictly space-reclaiming deletes. Ordinary writes resume only after cleanup
restores the complete reserve. A Catalog without reclaimable live state still
requires operator-led migration to a Catalog with capacity.
Legacy token outcomes are not reconstructible during the one-time bbolt import;
operators must retain the old file for rollback and treat an ambiguous
pre-migration request as an operation requiring manual generation inspection.

## Rejected alternatives

- Sharing the bbolt file between pods was rejected because the filesystem lock
  is not a distributed-consensus or fencing protocol.
- Relying only on Kubernetes Lease leader election was rejected because a stale
  process can outlive API connectivity and still reach Rust.
- Keeping desired metadata in Go while fencing only native Catalog writes was
  rejected because acknowledged desired state and operation replay would still
  disappear with the single owner.
- Letting each public endpoint compose desired and native deletes was rejected
  because a crash between the two mutations leaves an orphan on one side.
- Returning operation status by token alone was rejected because tokens are not
  authorization capabilities and could disclose cross-tenant activity.
