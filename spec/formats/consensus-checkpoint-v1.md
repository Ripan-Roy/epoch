# EPSN v1 consensus checkpoint

**Status:** internal pre-alpha fixed-voter format

**Implementation:** `crates/epoch-consensus/src/lib.rs`

EPSN v1 is the canonical Epoch state-history image carried in a Raft snapshot
and embedded in an EPRS kind-3 checkpoint record. It binds one group and epoch
to one applied Raft index/term, one state digest, and the complete ordered
committed-proposal registry. It is not a public backup or profile-native
snapshot format.

All integers are unsigned and big-endian. The full image is at most 768 KiB.
Decoding must consume the entire frame and canonical re-encoding must produce
the identical bytes.

## Fixed 76-byte header

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `EPSN` |
| 4 | 2 | Format version, exactly `1` |
| 6 | 2 | Flags, exactly `0` |
| 8 | 8 | Group ID |
| 16 | 8 | Group epoch |
| 24 | 8 | Checkpoint Raft index |
| 32 | 8 | Checkpoint Raft term |
| 40 | 32 | EPDG v1 SHA-256 applied-history digest |
| 72 | 4 | Committed proposal count |

Group, epoch, index, and term are nonzero. The outer Raft snapshot metadata
must repeat the exact index and term and contain exactly the fixed three voters,
with no learners or joint configuration.

## Proposal records

Exactly `proposal_count` records follow in applied order:

| Size | Field |
|---:|---|
| 8 | Proposal ID |
| 8 | Commit term |
| 8 | Commit log index |
| 4 | Payload length |
| variable | Original canonical proposal payload |

Proposal IDs, terms, and log indexes are nonzero. Log indexes increase strictly
and do not exceed the checkpoint index. Each payload is at most 512 KiB and
must decode as the in-scope canonical Epoch command when the image is installed
for an adapter.

The decoder reconstructs each receipt with the header group and epoch,
recomputes EPDG over the complete ordered history, compares it with the stored
digest, and rejects trailing bytes or a noncanonical representation.

## Pinned compatibility vector

The unit suite pins SHA-256
`5c00f66c57529944fe1002067bafcd7526b658f20dc4ea60b959c84135d14eb1`
for the canonical fixture containing group 7, epoch 1, checkpoint index 9/term
3, and one proposal (`id=11`, commit index 7/term 2, payload `abc`). Any byte
change requires an explicit new format version and compatibility decision.

## Validation boundary

Snapshot acceptance additionally requires the peer envelope source and
destination to be fixed voters; envelope group/epoch and message term to agree;
snapshot metadata to match EPSN index/term; and the local configured group,
epoch, and voter order to match exactly. Oversize, foreign, corrupt, stale, or
membership-changing input is rejected or safely ignored by Raft before state
installation.
