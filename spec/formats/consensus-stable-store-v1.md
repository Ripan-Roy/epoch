# EPRS v1 consensus stable journal

**Status:** internal pre-alpha feasibility format

**Format implementation:** `crates/epoch-consensus/src/stable.rs`

**Adapter integration:** `crates/epoch-consensus/src/lib.rs`

EPRS v1 is the local stable-state journal for one fixed-three-voter consensus
group. It records enough state to reconstruct the retained Raft log, complete
`HardState`, publishable application checkpoint, and applied proposal history.
It is not the production tablet segment or snapshot format.

## Physical container

EPRS records are payloads inside one `epoch_storage::FileWal`. The outer WAL
uses its `EPCH` v1 frame, monotonically increasing sequence, 16 MiB payload
limit, and CRC32 checksum. EPRS always writes an outer timestamp of zero and
requests a durable append. Consequently, one EPRS transition is one
checksummed `FileWal` append and sync boundary.

Outer sequence zero is the identity record. In a legacy journal, every later
outer sequence is a transition whose sequence equals the EPRS generation;
generations start at one and are contiguous. A compacted journal instead puts
kind 4 at physical sequence one with the latest logical generation, after
which physical sequence and logical generation advance independently and
contiguously.

`FileWal` may truncate only an incomplete final outer frame during open. A
complete frame with an invalid checksum is corruption and fails open. The file
is exclusively locked while the store is open, so a second writer is rejected.

## Common EPRS frame

All integers in the EPRS frame are unsigned and big-endian; the surrounding
`EPCH` frame retains its own little-endian encoding. Offsets below are relative
to the beginning of the EPRS payload stored in the outer WAL record.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `EPRS` |
| 4 | 2 | Format version, exactly `1` |
| 6 | 2 | Record kind: `1` identity, `2` transition, `3` checkpoint transition, `4` compacted checkpoint baseline |
| 8 | 4 | Kind-payload length |
| 12 | variable | Kind payload |

The declared payload length must equal the remaining bytes. Unknown versions
or kinds, truncation, length overflow, and trailing bytes are rejected. A
decoded record must re-encode to the exact input bytes.

## Kind 1: immutable identity

The identity payload is exactly 48 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Local node ID |
| 8 | 8 | Group ID |
| 16 | 8 | Group epoch |
| 24 | 8 | Voter 0 node ID |
| 32 | 8 | Voter 1 node ID |
| 40 | 8 | Voter 2 node ID |

All identifiers are nonzero. Voters are distinct and include the local node.
The complete identity, including voter order, must exactly match the identity
supplied when reopening the store. An identity record anywhere except outer
sequence zero, or a transition at sequence zero, is invalid.

## Kind 2: stable transition

The transition begins with this fixed 84-byte prefix:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Stable generation |
| 8 | 8 | HardState term |
| 16 | 8 | HardState vote, or zero |
| 24 | 8 | HardState commit index |
| 32 | 8 | Checkpoint applied index |
| 40 | 8 | Checkpoint publishable index |
| 48 | 32 | EPDG v1/v2 state digest determined by the active checkpoint |
| 80 | 4 | Entry count |

Each entry then has this layout:

| Size | Field |
|---:|---|
| 8 | Log index |
| 8 | Log term |
| 4 | Data length |
| variable | Data bytes |

The entry type is implicitly normal. EPRS v1 cannot encode a configuration
change. Raft entry context is required to be empty and the deprecated
`sync_log` value is required to be false. Data is either empty for a normal
Raft no-op or an EPCM v1 Epoch command. Raw `raft-rs` protobuf bytes are never
stored in EPRS.

Every transition carries the complete `HardState` and checkpoint, even when it
primarily changes entries. A transition with no entry, `HardState`, or checkpoint
change is rejected by the writer.

## Kind 3: checkpoint transition

Kind 3 is additive to the original EPRS v1 record set. Its fixed prefix is 88
bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Stable generation |
| 8 | 8 | HardState term |
| 16 | 8 | HardState vote, or zero |
| 24 | 8 | HardState commit index |
| 32 | 8 | Checkpoint applied index |
| 40 | 8 | Checkpoint publishable index |
| 48 | 32 | EPDG state digest matching the embedded EPSN version |
| 80 | 4 | Embedded EPSN image length |
| 84 | 4 | Retained tail entry count |

One complete canonical [EPSN v1 image](consensus-checkpoint-v1.md) or additive
[EPSN v2 profile checkpoint](consensus-checkpoint-v2.md) follows the prefix,
then exactly `tail entry count` normal entries using the kind-2 entry layout.
The first tail entry, when present, is checkpoint index plus one and the
remainder is contiguous. The EPSN index equals both application checkpoint
indexes, and its digest equals the EPDG field. For v2, replay validates later
committed entries by advancing its rolling EPDG chain.

## Kind 4: compacted checkpoint baseline

Kind 4 has the same payload layout and validation rules as kind 3. It differs
only in placement and generation semantics:

- it is valid only at outer WAL sequence one, immediately after identity;
- it is the only transition in that replacement file when created;
- its logical stable generation may be greater than outer sequence one; and
- it establishes that logical generation for subsequent contiguous kind-2 or
  kind-3 records.

The active file is replaced only after the same checkpoint has already been
durably appended to the old file. A kind-4 record elsewhere, or a first legacy
kind-2/3 record whose generation is not one, is corruption.

## Replay invariants

Transitions are applied in generation order to an initially empty retained log.
Replay enforces all of the following:

- legacy generation equals outer WAL sequence; after kind 4, logical
  generation and physical sequence advance independently but each remains
  contiguous;
- `HardState` term and commit index never decrease;
- a nonzero vote names a fixed voter, and an existing nonzero vote cannot
  change within the same term;
- entries have nonzero, contiguous indexes and nonzero, nondecreasing terms;
- an entry batch may replace an uncommitted suffix, but its first index must be
  greater than the previously committed index;
- before kind 3, the retained log is complete from index one; after kind 3 it
  begins immediately after the latest checkpoint and its final term does not
  exceed the `HardState` term;
- checkpoint index, term, group, epoch, voter metadata, proposal history, and
  digest agree, and later kind-2 transitions can modify only the contiguous
  uncommitted tail above the checkpoint;
- commit does not exceed the final retained index;
- applied and publishable indexes are equal in v1, never decrease, and do not
  exceed commit;
- every nonempty entry is a valid, in-scope EPCM command; conflicting reuse of
  a proposal ID fails closed; and
- replay derives the unique retained proposals through the checkpoint index
  and validates EPDG v1 from complete history or advances EPDG v2 from the
  compact profile checkpoint.

After successful replay, the implementation materializes a snapshot-aware Raft
storage view with the immutable voter configuration, latest canonical EPSN
image, recovered `HardState`, and retained tail. The journal remains the stable
source; this memory view is reconstructed state.

## Recovery guarantee

Within this feasibility slice, a successfully returned transition has been
written through the local `FileWal` durable append path before it becomes the
store's current generation. Reopen reconstructs only complete, canonical,
checksummed generations; a partial outer tail is discarded, while detected
complete-frame corruption fails closed. The latest validated checkpoint yields
the recovered applied proposal history and state digest.

`PersistentRaftAdapter::open` materializes that state and returns a
`PersistentOpenResult`. Its output must be consumed: it can contain receipts or
peer messages that became publishable while recovery advanced a checkpoint
that lagged the durable commit index. Recovery persists the advanced checkpoint
before returning those receipts.

This is local journal and reopen evidence. It does not establish a public
quorum acknowledgement or the complete system fault model.

## Limitations and non-claims

- The container remains one active `FileWal`. Kind 4 plus atomic sibling-WAL
  replacement reclaims generations older than the latest checkpoint; this is
  not general segmentation, retention, or backup lifecycle management.
- Without a committed-length manifest, an incomplete final outer frame is
  treated as a crash tail. The store cannot prove whether arbitrary later
  truncation damaged a frame that had previously synced; such post-ack media or
  filesystem loss is outside this slice's demonstrated fault model.
- Kind 4 reclamation is internal voter maintenance, not a user-visible purge,
  downloadable snapshot, backup catalog, or product restore format.
- Membership changes, learners, joint consensus, and voter-set migration are
  unsupported; identity is fixed for the file's lifetime.
- A complete valid prefix or whole-file rollback cannot be detected locally.
  There is no authenticated monotonic witness, anti-rollback counter, or backup
  generation proof.
- CRC32 detects accidental corruption; it is not authentication, encryption,
  or protection against a malicious writer.
- There is no exhaustive injected-I/O or real-process crash-boundary matrix yet.
- There is no replica repair, placement, authoritative catalog fencing,
  follower read routing, or production peer transport. The experimental
  leader-only ReadIndex barrier is separate from the stable format.
- The runnable Epoch node and public APIs do not yet expose this as a
  quorum-durable mode. Local EPRS persistence alone is not proof that a voter
  majority durably stored an acknowledged command.
- The format is internal and pre-alpha. Compatibility with a future production
  tablet format is not promised without a new reviewed version or migration.

The implementation's unit suite pins the exact v1 identity bytes and covers
create/reopen, immutable-identity mismatch, writer exclusion, `HardState` plus
entry replay, uncommitted-suffix replacement, incomplete-tail repair, checksum
corruption, and key safety regressions.

The adapter integration suite also reopens a committed three-voter history,
preserves an uncommitted isolated-leader proposal, verifies that persisted
messages follow a durable stable-store barrier, recovers a fully appended
proposal after an injected error prevents publication, and emits a committed
entry ahead of the stored checkpoint exactly once while reopening. Checkpoint
tests add canonical v1/v2 codec and corruption coverage, fsync-before-memory
failure reopen, checkpoint-plus-tail reopen, bounded exact retry
preservation/expiry, native-profile capture and install for all five profiles,
physical replacement histories, lagging-voter snapshot installation, and
post-restart election/read-barrier evidence.
