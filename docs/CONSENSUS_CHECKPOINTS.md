# Consensus Checkpoints and Snapshot Catch-up

Epoch's fixed-three-voter replicated core can persist a canonical native-profile
checkpoint, compact the local Raft prefix, physically reclaim obsolete EPRS
generations, send that checkpoint to a lagging voter, and reopen from the
checkpoint plus a committed log tail. This is an internal alpha voter-recovery
surface. It is not a downloadable backup, point-in-time restore artifact, or
production repair workflow.

The design is frozen in
[ADR-0021](adr/0021-consensus-checkpoint-and-snapshot-installation.md) and its
native-profile extension in
[ADR-0022](adr/0022-profile-native-checkpoints-and-physical-reclamation.md).
Canonical bytes are specified by the compatible
[EPSN v1](../spec/formats/consensus-checkpoint-v1.md) and
[EPSN v2](../spec/formats/consensus-checkpoint-v2.md) formats; their containing
disk records are specified in
[EPRS v1](../spec/formats/consensus-stable-store-v1.md).

## Inspect local checkpoint state

The diagnostic listener exposes local-only checkpoint evidence in the existing
consensus status document:

```shell
curl --fail --silent --show-error \
  http://127.0.0.1:17701/experimental/v1/consensus/status
```

Relevant fields are:

```json
{
  "commit_index": 12,
  "applied_index": 12,
  "checkpoint_index": 9,
  "retained_log_first_index": 10,
  "observation_scope": "local",
  "stability": "experimental"
}
```

`checkpoint_index: 0` means this voter has not created or installed a
checkpoint. `retained_log_first_index` is the first Raft entry still available
locally; after a checkpoint at index 9 it is 10. These are voter-local facts,
not cluster-wide health or backup-completeness claims.

## Create a local checkpoint

Send an empty POST to one voter's diagnostic listener when its actor has
finished current Ready work:

```shell
curl --fail-with-body --request POST \
  http://127.0.0.1:17701/experimental/v1/consensus/checkpoints
```

A successful response is `201 Created`:

```json
{
  "index": 9,
  "term": 3,
  "proposal_count": 6,
  "encoded_bytes": 1240,
  "durability": "fsync_before_install",
  "compaction": "logical_raft_prefix_and_physical_eprs"
}
```

Creation is local. The regional runtime also schedules it automatically for
catalog and every materialized profile group after 1,024 new applied entries by
default; every healthy voter owns its local recovery layout independently. The
explicit diagnostic trigger remains available. The operation rejects an empty
history, pending Ready work, inconsistent term or
digest state, a profile payload above 4 MiB, or a complete v2 image above 6
MiB. V2 retains at most 1,024 retry records and 1 MiB of encoded retry data.
Retrying the exact checkpoint is idempotent and does not append another EPRS
generation.

Configure the regional scheduler with
`EPOCH_REGIONAL_CHECKPOINT_INTERVAL_MS` (default 1,000; range 1–600,000) and
`EPOCH_REGIONAL_CHECKPOINT_MIN_APPLIED_ENTRIES` (default 1,024; nonzero).
Eligibility and creation run atomically on the consensus actor. A group with
pending Raft `Ready` work is skipped until another tick. See
[ADR-0028](adr/0028-automatic-regional-consensus-checkpoints.md).

## Persistence and catch-up order

The safety order is fixed:

1. Capture and canonically validate the typed profile at the local applied
   index, without mutating live state.
2. Encode and validate one canonical EPSN v2 image with its rolling EPDG state,
   bounded retry suffix, payload digest, and complete-image digest.
3. Append the additive EPRS checkpoint record and complete its fsync barrier.
4. Atomically replace the physical journal with identity plus one kind-4
   compacted baseline, then fsync the parent directory.
5. Install the snapshot in the local Raft store and discard the prefix while
   retaining a contiguous later tail.
6. When a voter is behind that prefix, Raft sends the canonical snapshot. The
   receiver validates and durably records it before replacing typed profile
   state and applying the committed tail exactly once.

An error after the durable append fail-stops the live adapter; reopening the
same journal recovers the durable checkpoint. Corrupt complete records,
foreign metadata, voter changes, digest disagreement, and noncontiguous tails
fail closed. Only an incomplete final outer-WAL frame uses EPRS's existing
crash-tail repair rule.

## Reproduce the evidence

```shell
cargo test --locked -p epoch-consensus
cargo test --locked -p epoch-node \
  three_probe_runtimes_elect_and_commit_over_real_http -- --nocapture
cargo test --locked -p epoch-node --lib
```

The tests pin both EPSN codecs, reject malformed/foreign metadata and size
boundaries, prove durable ordering, checkpoint-plus-tail reopen, bounded retry
retention/expiry, physical file reduction, lagging-voter installation, and
automatic native-profile restoration for Catalog, Stream, Queue, Cache, and
Event Bus across real three-voter reopen.

## Explicit non-claims

- EPSN v1 remains readable and retains its complete committed proposal registry.
  V2 bounds consensus exact-retry metadata; aged-out IDs truthfully become
  unknown and product APIs must define their own idempotency horizon.
- Kind-4 replacement reclaims obsolete EPRS generations but is not retention,
  secure erasure, backup lifecycle management, or standalone-WAL compaction.
- The at-most-6-MiB v2 image is transported in one bounded peer frame. Chunked or
  out-of-band snapshot transfer does not exist.
- There is no dynamic membership, learner promotion, online tablet movement,
  automated repair, restore orchestration, backup catalog, PITR, remote tier,
  encryption, or authenticated peer transport.
- A Stream consumer-group offset is an application checkpoint. It is unrelated
  to this consensus checkpoint and cannot restore a voter.
