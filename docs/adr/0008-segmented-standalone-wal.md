# ADR-0008: Segmented Standalone WAL

**Status:** Accepted for the Phase 0 standalone slice
**Date:** 22 July 2026

## Context

The first local-durable Stream and Queue slice used one exclusively locked
`$EPOCH_DATA_DIR/engine.wal`. Its checksummed v1 frames, fsync-before-apply rule,
and restart replay prove useful semantics, but an indefinitely growing file does
not provide the segment boundaries needed for later retention, compaction,
snapshots, transfer, or object-tier work.

Changing the layout must not let an older binary append an independent history,
silently accept a missing final segment, or infer committed state from whichever
files happen to remain after a crash. Existing single-file data must remain safe
to run and downgrade until an explicit migration exists.

## Decision

A fresh or empty standalone data directory is activated atomically into this
layout:

```text
$EPOCH_DATA_DIR/
├── engine.wal                       # staging/active layout marker and shared lock
└── engine-wal/
    ├── .writer.lock
    ├── identity.v1                  # version, WAL UUID, activation state
    ├── manifest.v1                  # ordered committed topology and checksums
    ├── segment-00000000000000000000.wal
    └── segment-<first-sequence>.wal
```

The following rules apply:

1. Epoch locks `engine.wal` before selecting a layout and retains that lock for
   the process lifetime. A segmented writer also locks `.writer.lock` and each
   open segment. A second old or new process cannot become a concurrent writer.
   The raw segmented constructor is private so callers cannot bypass the outer
   activation lock.
2. Fresh directories first receive a staging marker that is invalid as a v1 WAL
   frame. Epoch creates and syncs the segmented identity, manifest, and initial
   segment, then atomically replaces it with the active marker. An older
   single-file binary therefore fails to parse the directory after activation,
   even while no new Epoch process is running. A torn staging marker can resume
   only when no conflicting segmented history exists. Missing data-directory
   components are created one at a time and each parent directory, including
   `.` for the default relative path, is synced before acknowledgement.
3. `identity.v1` and `manifest.v1` are independently versioned and checksummed.
   Their WAL UUIDs must match. The manifest is the commit authority: it records
   every segment's first and last sequence, exact committed byte length, and
   whole-file CRC32. A missing identity, manifest, or committed segment; a
   foreign identity; an unexpected segment; or a metadata checksum failure
   fails startup.
4. Segment frames retain the existing v1 binary layout and per-frame checksum.
   Record sequence is global and contiguous across files. Startup rejects gaps,
   duplicates, reordered files, unsupported flags or versions, and checksum
   mismatches.
5. An append syncs the active segment before atomically committing its new
   length and checksum in the manifest. Recovery may discard bytes only beyond
   the active segment's manifest-committed length. A truncated committed range
   or any extra bytes in a sealed segment fail startup. A failed metadata update
   is reconciled against the exact old or new manifest; an ambiguous result
   poisons the writer instead of guessing. An append whose rollback cannot be
   completed poisons the active segment and cannot be bypassed by rotation.
6. Rotation uses a manifest `pending_segment` transition. Recovery may finish a
   pending rotation only at the expected next sequence and only when its new
   segment is absent or empty. A non-empty uncommitted segment or any unrelated
   file fails closed.
7. The configured target defaults to 64 MiB and is set with
   `--wal-segment-bytes` or `EPOCH_WAL_SEGMENT_BYTES`. It is physical rotation,
   not retention, a quota, or deletion. Frames are never split, so one valid
   frame larger than the target may occupy an otherwise empty segment.
8. A pre-existing valid legacy `engine.wal` stays on the single-file layout.
   This binary locks, repairs only its incomplete crash tail, replays, and
   continues appending to that same file; it does not create `engine-wal/`.
   This preserves safe rollback to the earlier binary. Automatic legacy-to-
   segmented migration is deliberately deferred. Legacy and segmented histories
   without a valid activation marker are rejected as ambiguous.
9. Stream and Queue replay consumes the resulting global record sequence.
   Volatile mutations still bypass the WAL.

## Evidence required for this milestone

- rotation under a deliberately small configured threshold;
- append and restart replay across multiple segments with a contiguous sequence;
- rejection of old/new and new/new concurrent writers;
- active uncommitted-suffix repair without loss of committed bytes;
- rejection of missing/truncated/reordered/foreign/checksum-corrupt history;
- deterministic completion of an empty pending rotation;
- crash-safe activation that permanently blocks old single-file readers;
- safe single-file fallback and downgrade for valid legacy data;
- process/container restart preserving local-durable Stream and Queue state.

These tests are evidence for the standalone journal only.

## Consequences

- The manifest makes missing-tail and topology corruption detectable rather than
  reconstructing truth from directory contents.
- A fresh installation receives segment boundaries without changing v1 frame
  payloads or application replay order.
- Existing single-file installations remain usable but do not rotate until an
  explicit, separately tested migration is implemented.
- Segment files accumulate until a retention/compaction policy is implemented.
  Rotation alone does not reclaim space.
- The global in-memory record index and single host/writer remain scalability
  and durability limits of this slice.

## Explicitly not delivered

- legacy-to-segmented migration;
- snapshots or point-in-time restore;
- log compaction, retention deletion, or segment garbage collection;
- sparse indexes, object tier, backup, or restore validation;
- cryptographic manifest authentication;
- detection of a self-consistent whole-volume rollback or replacement of the
  activation marker and complete inner WAL as one unit; operators must restore
  the data directory atomically and validate it as a single backup object;
- consensus terms/indexes, replication, quorum acknowledgement, leader
  fencing, replica repair, or multi-zone survival;
- a frozen public tablet storage format.

Those capabilities remain governed by ADR-0003, ADR-0004, and the later
delivery gates in `REQUIREMENTS_TRACEABILITY.md`.

## Rejected alternatives

- Continuing one unbounded `engine.wal` for fresh installations indefinitely.
- Reconstructing committed topology from segment filenames alone.
- Treating any partial or missing segment as disposable.
- Automatically converting a legacy WAL without a downgrade protocol.
- Letting old binaries parse the activation marker as an empty valid WAL.
- Claiming segment rotation as retention, compaction, snapshot, or replication.
