# ADR-0004: Storage Format and Versioning

**Status:** Accepted  
**Date:** 22 July 2026

## Context

Embedded, standalone, clustered, and managed Epoch deployments must use the same
engine and data-format contracts. Rolling upgrades require mixed-version
operation, and retained Stream data may outlive many software releases. Opaque
serialization of Rust structs would couple stored data to compiler and library
implementation details.

Writing a consensus WAL and then synchronously copying every record into a
second application log would also add latency, write amplification, and a second
recovery truth.

## Decision

The tablet consensus log is its ordered application commit log. The consensus
storage adapter writes versioned frames into immutable segment files. Profile
state machines build derived indexes and versioned snapshots from committed
entries.

The stable representation consists of:

- an explicit fixed or self-describing binary frame header;
- versioned Protobuf metadata;
- raw payload bytes;
- per-frame length and checksum;
- segment headers, footers, sparse indexes, and a cryptographic manifest digest.

Frames identify their format/features, tablet/group, consensus term/index,
profile logical position, type, flags, time observation, encryption key, and
compression where applicable. Consensus index and user-visible logical offset
are separate fields.

The following are prohibited as durable contracts:

- `bincode` or another encoding of native Rust object layouts;
- pointer-width, host-endian, compiler, or platform-dependent structures;
- unversioned JSON or Protobuf blobs without a containing format version;
- in-place destructive migration of the only valid copy.

### Evolution rules

1. Readers support every format advertised by the release compatibility window.
2. During a mixed-version rollout, writers use the newest format supported by
   every eligible replica.
3. A regional catalog feature gate activates a new write format only after all
   required readers and rollback nodes advertise support.
4. A format migration writes and verifies a new segment or snapshot before an
   old copy is retired.
5. Unknown required features fail explicitly; optional fields can be skipped
   according to their declared compatibility rule.
6. Golden files, corrupt/truncated corpora, upgrade/rollback fixtures, and format
   readers are retained in the repository.

Exact byte layouts, compatibility windows, and feature-bit allocation live in
`spec/formats` and require their own review before data is persisted.

ADR-0008 records the narrower Phase 0 standalone step. A fresh or empty data
directory first receives a staging marker at `$EPOCH_DATA_DIR/engine.wal`; once
the segmented journal is durable, that marker is atomically replaced by an
active marker. Both marker values are deliberately invalid to the old
single-file WAL reader, preventing it from treating a segmented history as an
empty legacy journal.

The segmented journal preserves the checksummed v1 frame format and one global
sequence under `$EPOCH_DATA_DIR/engine-wal/`. Its versioned, checksummed
`identity.v1` and `manifest.v1` metadata bind a WAL UUID to the ordered segment
topology, committed byte lengths, last sequences, whole-committed-file CRC32
values, and any pending rotation. Recovery may discard only a suffix beyond the
committed length of the active segment. Missing or truncated committed files,
foreign identity, untracked files, and sealed-segment or committed-content
changes fail closed; the explicit pending state permits recovery of only the
expected empty rotation target.

A pre-existing valid `engine.wal` is not incorporated as a prefix and is not
migrated automatically. The new node continues reading and appending that file
with the legacy single-file writer and does not create a segmented history, so
an offline downgrade remains possible. The standalone manifest is transitional
recovery metadata, not the final tablet manifest: this journal still has no
snapshot, compaction, retention, consensus, or replication semantics and does
not supersede this ADR's target format.

### Object storage

Only sealed, committed segments are uploaded. A local segment is removed only
after remote checksum verification and a durable manifest update. The primary
remote object is the open Epoch segment format. Parquet, JSON, and other
analytics formats are separate capture/export products.

### Encryption

Segments and snapshots record an envelope-encryption key reference. Rotation
writes new objects with the current key; background re-encryption is a resumable,
audited operation. Plaintext keys and payloads are never included in manifests,
logs, or metrics.

## Consequences

- The target format is designed to move data from standalone to cluster without
  application-level re-encoding; the Phase 0 legacy-to-segmented migration is
  not implemented.
- Rolling upgrade and rollback require feature negotiation and format fixtures.
- Derived-index corruption can be repaired from a verified snapshot and log.
- The custom storage adapter is on the correctness and performance critical
  path and needs property, fuzz, crash, and partial-write testing.
- Capturing analytics data does not constrain the broker's replication layout.

## Rejected alternatives

- A second synchronous application-log write after consensus persistence.
- Treating a general-purpose embedded database format as Epoch's public storage
  contract.
- Making Parquet the active replication log.
- Automatically rewriting the only valid stored copy in place.
