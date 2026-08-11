# EPSN v2 profile checkpoint

**Status:** internal pre-alpha fixed-voter format

**Implementation:** `crates/epoch-consensus/src/lib.rs`

EPSN v2 is an additive native-profile checkpoint carried in a Raft snapshot
and embedded in EPRS checkpoint records. EPSN v1 remains readable and retains
its original bytes and semantics.

All integers are unsigned and big-endian. The full image is at most 6 MiB, the
profile payload is at most 4 MiB, and encoded retry records total at most 1
MiB. Decoding consumes the entire frame and canonical re-encoding must produce
identical bytes.

## Fixed 172-byte header

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `EPSN` |
| 4 | 2 | Format version, exactly `2` |
| 6 | 2 | Flags, exactly `1` (`application_snapshot_present`) |
| 8 | 8 | Group ID |
| 16 | 8 | Group epoch |
| 24 | 8 | Checkpoint Raft index |
| 32 | 8 | Checkpoint Raft term |
| 40 | 32 | EPDG v2 rolling consensus digest |
| 72 | 8 | Total unique committed-command count |
| 80 | 4 | Retained exact-retry record count |
| 84 | 16 | Application format identifier |
| 100 | 2 | Application schema version |
| 102 | 2 | Reserved, exactly zero |
| 104 | 4 | Application payload length |
| 108 | 32 | Application state digest |
| 140 | 32 | SHA-256 application payload digest |

Group, epoch, index, term, total command count, format identifier, and schema
version are nonzero. Total command count is at least the retry count. The outer
Raft snapshot repeats the exact index and term and contains exactly the fixed
three voters with no learner or joint configuration.

The application payload follows the header. Its format-specific decoder must
validate scope, immutable configuration, state digest, domain invariants, and
canonical re-encoding before installation.

## Exact-retry records

Exactly `retry_count` records follow the application payload:

| Size | Field |
|---:|---|
| 8 | Proposal ID |
| 8 | Commit term |
| 8 | Commit log index |
| 4 | Payload length |
| variable | Original canonical proposal payload |

The records use the EPSN v1 proposal encoding. They are strictly ordered by
increasing log index, contain unique nonzero proposal IDs, and do not exceed
the checkpoint index. The count is at most 1,024 and their complete encoded
size is at most 1 MiB. The suffix must be maximal under those two limits: the
writer considers commands newest-first and retains every consecutive command
that fits, then emits the selected suffix in forward order.

## Image digest trailer

A 32-byte SHA-256 digest follows the final retry record. It is the digest of
every preceding byte in the EPSN v2 frame, from the `EPSN` magic through the
final retry payload. The decoder verifies this trailer before accepting the
image. Consequently the smallest structurally valid frame is 204 bytes: the
172-byte header plus the 32-byte trailer.

## EPDG v2

The initial digest is:

```text
SHA-256("EPDG" || u16(2) || group_id || group_epoch)
```

For each unique committed proposal in log order:

```text
SHA-256(
  previous_digest ||
  log_index || term || proposal_id || u64(payload_length) || payload
)
```

The EPDG field is the rolling state digest after `total_command_count`
transitions. A checkpoint created from v1 recomputes this chain over the
complete v1 applied history. Once a v2 checkpoint is installed, later entries
advance it from the stored state digest without needing the discarded prefix.
The independent image-digest trailer protects the complete serialized frame.

## Validation and compatibility

EPSN v2 additionally requires:

- application payload SHA-256 and profile state digest agreement;
- application capture index equal to the EPSN index;
- retry records whose commands are canonical and match group and epoch;
- tail entries contiguous after the checkpoint;
- total command count and EPDG v2 advance consistently for later entries; and
- canonical application bytes for the exact declared format and version.

Unknown formats are retained by consensus only long enough to reject runtime
installation; a voter must not report ready without a matching profile
installer. EPSN v1 is never silently upgraded during read. The next explicit
checkpoint with a native profile capture performs the v1-to-v2 transition.

This format is an internal voter-recovery artifact, not a backup/PITR or
portable user snapshot.
