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

Outer sequence zero is the identity record. Every later outer sequence is a
transition, and its sequence must equal the EPRS generation. Generations start
at one and are contiguous.

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
| 6 | 2 | Record kind: `1` identity, `2` transition |
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
| 48 | 32 | EPDG v1 SHA-256 state digest |
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

## Replay invariants

Transitions are applied in generation order to an initially empty retained log.
Replay enforces all of the following:

- generation equals the outer WAL sequence and advances by exactly one;
- `HardState` term and commit index never decrease;
- a nonzero vote names a fixed voter, and an existing nonzero vote cannot
  change within the same term;
- entries have nonzero, contiguous indexes and nonzero, nondecreasing terms;
- an entry batch may replace an uncommitted suffix, but its first index must be
  greater than the previously committed index;
- the resulting retained log is complete from index one, without snapshots or
  compaction, and its final term does not exceed the `HardState` term;
- commit does not exceed the final retained index;
- applied and publishable indexes are equal in v1, never decrease, and do not
  exceed commit;
- every nonempty entry is a valid, in-scope EPCM command; conflicting reuse of
  a proposal ID fails closed; and
- replay derives the unique applied proposals through the checkpoint index and
  recomputes the EPDG digest, which must exactly match the checkpoint.

After successful replay, the implementation materializes a `MemStorage` view
with the immutable voter configuration, recovered `HardState`, and retained log.
The journal remains the stable source; this memory view is reconstructed state.

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

- The container is the current single-file `FileWal`; EPRS has no segment,
  manifest, rotation, retention, or migration contract.
- Without a committed-length manifest, an incomplete final outer frame is
  treated as a crash tail. The store cannot prove whether arbitrary later
  truncation damaged a frame that had previously synced; such post-ack media or
  filesystem loss is outside this slice's demonstrated fault model.
- There are no snapshots, log compaction, purge, or checkpoint installation.
- Membership changes, learners, joint consensus, and voter-set migration are
  unsupported; identity is fixed for the file's lifetime.
- A complete valid prefix or whole-file rollback cannot be detected locally.
  There is no authenticated monotonic witness, anti-rollback counter, or backup
  generation proof.
- CRC32 detects accidental corruption; it is not authentication, encryption,
  or protection against a malicious writer.
- There is no exhaustive injected-I/O or real-process crash-boundary matrix yet.
- There is no replica repair, placement, authoritative catalog fencing,
  linearizable read barrier, or production peer transport.
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
entry ahead of the stored checkpoint exactly once while reopening.
