# Versioned formats and fixtures

This directory contains reviewed specifications and golden vectors for
provisional Epoch persisted formats. A fixture protects byte-level
compatibility within its format version; it does not make the pre-alpha format
a permanent public contract.

## Engine journal version 1

The standalone WAL outer frame is defined by `epoch-storage`: `EPCH` magic,
outer format version, sequence, timestamp, payload length, and CRC32 checksum.
Frame payloads are capped at 16 MiB so a corrupt length cannot force an
unbounded recovery allocation. The payload is compact UTF-8 JSON with this
envelope:

```json
{"format_version":1,"mutation":{"kind":"..."}}
```

Version 1 mutation kinds are:

- `create_stream`: resource name plus complete `StreamConfig`;
- `append_stream`: resource name, envelope, selected partition, and apply time;
- `set_stream_offset`: resource/group identity, partition, next offset, and reset flag;
- `create_queue`: resource name plus complete `QueueConfig`;
- `enqueue_queue`: resource name, envelope, and apply time;
- `acquire_queue`: resource/consumer identity, batch/visibility options, and apply time;
- `settle_queue`: Ack, Release, Reject, or Extend command plus apply time;
- `redrive_queue`: resource/message identity plus apply time;
- `maintain_queue`: deterministic schedule, TTL, lease-expiry, retry, and dedupe maintenance time.

Those snake-case strings are the persisted mutation discriminants. Queue
settlement discriminants are `ack`, `release`, `reject`, and `extend`. The
compact writer emits `format_version` before `mutation`; a mutation emits
`kind` first and then its fields in the Rust declaration order. Queue variants
emit `expected_state_checksum` last when present. JSON readers do not rely on
object-member order, but the reviewed fixtures pin writer output because the
outer WAL frame checksum covers these exact payload bytes.

Every Queue mutation written by current binaries also carries an optional
`expected_state_checksum`: the CRC32 of the complete Queue state immediately
after that command was applied. The canonical checksum input starts with the
`epoch.queue.recovery-state` domain and encoding version, uses fixed
little-endian integers plus length-prefixed UTF-8 and collections, sorts
message and dedupe maps by their UTF-8 keys, recursively sorts JSON object
keys, and preserves Queue and dead-letter order. It covers the full config,
messages, queue order, dead-letter contents, dedupe receipts/expiry,
commit position, and lease generation. Recovery recomputes the value after
each command and fails closed on a mismatch, detecting replay-algorithm drift
such as different selected messages or lease tokens.

The field is omitted when absent, so earlier v1 entries and the reviewed
`create_queue` fixture remain byte-compatible and continue to replay without a
comparison. Once a checksummed entry appears, it checkpoints all state produced
by earlier entries too. CRC32 is used only as a deterministic replay-drift
guard; it is not cryptographic authentication or protection from a malicious
writer.

Recovery rejects an unknown engine format, unsupported outer flags, malformed
JSON, mutation ordering that cannot be applied, and checksum corruption. The
writer fsyncs each local-durable mutation before applying it to live Stream or
Queue memory, so a persistence failure cannot produce a successful or visible
mutation. Queue replay re-executes commands at their recorded apply times and
verifies each available post-state checksum, preserving lease generations and
tokens or failing startup when behavior diverges.

### Standalone physical layouts

A fresh or empty data directory uses the segmented layout. Startup writes a
staging value to `engine.wal`, initializes `engine-wal/`, and atomically replaces
the staging value with an active marker. Both values are intentionally invalid
v1 frames, so an older single-file reader fails instead of creating a second
history. `engine-wal/identity.v1` and `engine-wal/manifest.v1` are themselves
versioned and checksummed. They share a WAL UUID; the manifest records the
ordered segment topology and, for each segment, its committed byte length, last
sequence, and CRC32 over all committed bytes. It also records an optional
pending segment during rotation.

On segmented recovery, only bytes after the manifest's committed length in the
active segment may be discarded. Recovery fails closed for a missing or
truncated committed segment; missing identity or manifest; a foreign identity;
an extra/untracked segment; sequence or topology drift; a committed-content
checksum mismatch; or bytes beyond the committed length of a sealed segment. A
pending rotation is completed only when its expected target is absent or empty;
nonempty or unrelated untracked files are rejected.

A pre-existing valid `engine.wal` selects the legacy layout instead. The node
continues appending v1 frames to that same file and does not create
`engine-wal/`; there is no automatic migration or logical legacy prefix. This
preserves compatibility with an offline downgrade. The legacy path retains its
original single-file incomplete-tail recovery behavior.

The `create_stream`, checksum-less `create_queue`, and checksummed
`create_queue` vectors are compiled into the engine test suite. Any intentional
change requires a new version or an explicit compatible-fixture review. This
node-level journal is replaced by the tablet log/snapshot format in the
replicated architecture; it has no snapshot or compaction contract.

## Deterministic test trace version 1

`epoch-testkit` serializes reproducible simulation evidence as EPTR v1. This is
a test-artifact format, not customer storage or a cryptographic audit log. Its
canonical little-endian layout is:

```text
"EPTR" | u16 version=1 | u16 flags=0 | u64 event_count
repeat event_count times:
  u64 sequence | u64 monotonic_ms | u32 kind_len | kind UTF-8
  u64 payload_len | payload bytes
```

Sequences start at zero and are contiguous. Decoding rejects unknown versions
or flags, invalid UTF-8, truncation, noncanonical sequences, and trailing bytes.
The history digest is the fixed 64-bit FNV-1a checksum of the complete canonical
encoding; it detects drift between runs but is not collision-resistant or
authenticated. Compatibility tests pin both full golden bytes and digest
`bd94a233541b2179`. Any incompatible encoding change requires EPTR version 2;
changing only the implementation while retaining v1 must preserve these bytes.
EPTR stores observations only; a seed and fault plan must be captured separately
until a versioned executable scenario bundle is defined.

## Consensus feasibility formats version 1

The consensus spike uses big-endian Epoch frames. They are internal feasibility
formats, not a production transport or tablet-log compatibility promise.

An Epoch command stored inside a normal Raft entry is EPCM v1:

```text
"EPCM" | u16 version=1 | u64 group_id | u64 group_epoch
       | u64 proposal_id | u32 payload_len | payload bytes
```

Zero identifiers, unsupported versions, truncated or trailing bytes, and
group/epoch mismatches are rejected. The same proposal ID and payload is
applied once. The public proposal path rejects conflicting ID reuse before
Raft; if conflicting bytes instead reach committed-entry processing, the
adapter fail-stops.

An in-process peer transport frame is EPPM v1:

```text
"EPPM" | u16 version=1 | u64 group_id | u64 group_epoch
       | u64 from | u64 to | u64 raft_term | u32 opaque_len
       | canonical opaque raft-rs protobuf bytes
```

The complete frame is capped at 1 MiB before allocation. Decoding checks the
expected destination, rejects a self route, requires the envelope to match the
opaque message, and re-encodes the opaque payload to enforce canonical bytes.
The adapter additionally requires both endpoints to be in its fixed voter set
and rejects local-only, snapshot, and membership-changing message classes.
Production peer transport still requires authenticated identity, encryption,
flow control, batching, version negotiation, and compatibility fixtures.

The applied-history digest input starts with `EPDG`, big-endian version 1,
group ID, group epoch, and applied command count. Every unique applied command
then contributes log index, Raft term, proposal ID, payload length, and payload.
SHA-256 covers the complete canonical sequence. The digest compares deterministic
state histories; it does not replace full histories or authenticate an
adversarial writer.

The disk stable-store sub-slice stores EPRS v1 records inside the checksummed
`FileWal` frame. Sequence zero fixes the node, group, epoch, and three voters;
each later generation atomically records explicit `HardState` fields, a
publishable EPDG checkpoint, and normal-entry index/term/data fields. It does
not persist raw `raft-rs` protobuf. The exact byte layout, replay invariants,
recovery boundary, and limitations are specified in
[EPRS v1 consensus stable journal](consensus-stable-store-v1.md).
