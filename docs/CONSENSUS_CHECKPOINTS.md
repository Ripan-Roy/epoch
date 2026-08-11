# Consensus Checkpoints and Snapshot Catch-up

Epoch's fixed-three-voter replicated core can persist a canonical consensus
checkpoint, logically compact the local Raft prefix, send that checkpoint to a
lagging voter, and reopen from the checkpoint plus a committed log tail. This
is an internal alpha operations surface. It is not a Cache backup, profile
snapshot, point-in-time restore artifact, or production repair workflow.

The design is frozen in
[ADR-0021](adr/0021-consensus-checkpoint-and-snapshot-installation.md). The
canonical bytes are specified in
[EPSN v1](../spec/formats/consensus-checkpoint-v1.md), and the containing disk
record is specified in
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
  "compaction": "logical_raft_prefix"
}
```

Creation is local and explicit; Epoch does not yet schedule checkpoints. The
operation rejects an empty history, pending Ready work, inconsistent term or
digest state, and returns `413 checkpoint_too_large` for a canonical image
larger than 768 KiB. Retrying the exact
checkpoint is idempotent and does not append another EPRS generation.

## Persistence and catch-up order

The safety order is fixed:

1. Encode and validate one canonical `EPSN` image at the local applied index.
2. Append the additive EPRS checkpoint record and complete its fsync barrier.
3. Install the snapshot in the local Raft store and logically discard the
   prefix while retaining a contiguous later tail.
4. When a voter is behind that prefix, Raft sends the canonical snapshot.
5. The receiver verifies envelope, group, epoch, voter set, index, term,
   canonical bytes, size, proposal history, and digest.
6. The receiver fsyncs its EPRS checkpoint record before advancing Raft.
7. A typed profile is rebuilt through `CommittedProposalApplier::replay`, then
   any committed tail is applied exactly once.

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
cargo test --locked -p epoch-node \
  lagging_profile_voter_replays_a_checkpoint_before_applying_its_tail -- --nocapture
```

The tests pin the EPSN codec digest, reject malformed/foreign metadata and the
size boundary, prove fsync-before-memory ordering, checkpoint-plus-tail reopen,
exact retry state, post-restart election and quorum read barriers, real-HTTP
checkpoint creation, lagging-voter snapshot installation, typed Catalog replay,
tail application, and final profile convergence.

## Explicit non-claims

- EPSN v1 retains the complete committed proposal registry. It does not bound
  idempotency history or provide a compact profile-native state image.
- Logical Raft prefix compaction does not reclaim older records from the
  physical single-file EPRS outer WAL.
- The 768 KiB image is transported in one bounded peer frame. Chunked or
  out-of-band snapshot transfer does not exist.
- There is no dynamic membership, learner promotion, online tablet movement,
  automated repair, restore orchestration, backup catalog, PITR, remote tier,
  encryption, or authenticated peer transport.
- A Stream consumer-group offset is an application checkpoint. It is unrelated
  to this consensus checkpoint and cannot restore a voter.
