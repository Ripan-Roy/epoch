# ADR-0015: Replicated Stream Batch Compression

**Status:** Accepted

**Date:** 5 August 2026

## Context

The replicated Stream tablet accepted one event per consensus proposal. That
proved ordering, majority persistence, retry, failover, and recovery, but it
left the P0 batching and compression requirement in STREAM-006 entirely open.
A useful first implementation must carry actual compressed frames through the
replicated command boundary, preserve historical version-one bytes, correlate
every record with the producer's sequence, and reject decompression bombs before
any profile state changes.

This decision covers the experimental single-partition tablet. It does not
pretend that the future bidirectional native `Produce` service, automatic
producer batching, or Kafka protocol negotiation already exists.

## Decision

1. `StreamTabletCommand` continues to emit the exact canonical version-one
   document for a single `Append`. Only `AppendBatch` emits command format
   version two. Version and operation kind must agree; old golden bytes,
   proposal IDs, response JSON without batch evidence, and state digests remain
   unchanged.
2. A batch is a canonical JSON array of strict records. Each record contains a
   unique unsigned 32-bit `client_sequence` and one `EventEnvelope`. The array
   is encoded as standard padded base64 after applying one declared compression
   mode.
3. The supported modes are `none`, RFC 1952 gzip, LZ4 frame, Snappy framed, and
   Zstandard frame. The compression mode, record count, compressed byte count,
   uncompressed byte count, and base64 payload are part of the idempotent
   semantic input. Reusing a key with different frame bytes or metadata is a
   conflict, even if decompression might produce equivalent events.
4. One batch contains 1–1,000 records, at most 360 KiB of compressed frame
   bytes, and at most 4 MiB after decompression. Standard-base64 canonicality,
   both declared sizes, record count, unique sequences, strict event validity,
   and canonical decompressed JSON are checked before proposal and again on
   every voter during command decoding. Zstandard decoding additionally caps
   the frame window at 8 MiB. All decoders stop after one byte beyond the 4 MiB
   ceiling so expansion cannot allocate an unbounded output.
5. A committed batch applies to a cloned Stream state machine. Every record
   must append successfully before the clone replaces live state, so a future
   profile error cannot expose a prefix. The command is one atomic tablet
   transition; it is not the non-atomic partial-success mode described for the
   eventual native `Produce` stream.
6. The receipt retains the historical top-level first offset and adds optional
   batch evidence: codec, exact sizes/count, and one result per
   `client_sequence` with a browser-safe decimal offset and disposition. Exact
   command replay returns the original correlated result without appending
   again.
7. The experimental HTTP endpoint is
   `POST /experimental/v1/tablets/stream/records/batches`. The regional
   resource/shard router exposes the same handler under `data/records/batches`
   with its existing generation, tablet-epoch, leader, and authorization
   checks. Status advertises the codecs and hard limits. These routes remain on
   the experimental boundary and are not added to the standalone SDK contract.

## Consequences

- A producer can amortize one consensus proposal across a bounded ordered batch
  and choose each compression format required by STREAM-006.
- Malformed base64, corrupt frames, unknown JSON fields, duplicate client
  sequences, false size/count declarations, and oversized expansion fail
  definitively before consensus mutation. A structurally invalid committed
  command still fail-stops the profile actor, preserving divergence safety.
- Compression applies to the replicated command and recovery history; records
  are decompressed into ordinary Stream state and fetch responses.
- The compressed frame is retained in EPRS as command input, so startup pays
  bounded decompression cost while rebuilding unsnapshotted history.
- Pure-Rust gzip, LZ4, and Snappy implementations are used. Zstandard uses the
  pinned `zstd` crate and its native library build. The Docker release build and
  dependency audit therefore cover the new transitive surface.
- STREAM-006 advances from Planned to Slice. Stable gRPC streaming Produce,
  connection credit, producer-side automatic batching, compression
  negotiation, multi-partition routing, non-atomic partial results, corpus
  fuzzing, and matched compression throughput/latency reports remain required
  before the P0 requirement is complete.

## Rejected alternatives

- Compress only the HTTP request and replicate uncompressed events, which would
  not exercise compressed consensus or recovery paths.
- Trust a declared uncompressed length or a codec's frame header without an
  independent hard output bound.
- Apply records directly to live state and rely on current append operations
  never failing midway.
- Rewrite single-record commands to version two and invalidate existing
  canonical/digest evidence.
- Expose an SDK helper before its routing, framing, retry, and dependency
  boundary is reviewed. ADR-0026 now records that additive regional SDK
  decision without redefining this atomic command as native streaming Produce.
