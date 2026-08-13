# ADR-0026: Regional Stream Atomic Batch SDKs

**Status:** Accepted

**Date:** 13 August 2026

## Context

ADR-0015 established a canonical, bounded, single-shard Stream batch command
and direct/regional HTTP route. The Rust runtime already accepts `none`, gzip,
LZ4 frame, Snappy framed, and Zstandard frame bytes, replicates the exact frame
through one consensus proposal, applies every record atomically, and returns a
result for each caller sequence. First-party Go, Java, and Python regional
clients could not invoke that route, so applications had to reproduce routing,
fencing, retry, canonical JSON, and size metadata themselves.

The SDK boundary must expose the implemented protocol without claiming the
later bidirectional, credit-aware, automatically batched, non-atomic native
`Produce` service. It must also avoid forcing a different native compression
dependency into each SDK merely to support applications that already own
standard compressor libraries.

## Decision

1. Every first-party regional Stream client exposes a single-shard atomic
   `AppendBatch`/`appendBatch`/`append_batch` operation. It uses the ordinary
   authenticated leader discovery, resource-generation/tablet-epoch fencing,
   observed term, and one bounded same-key rediscovery contract.
2. A typed batch frame contains the compression identifier, record count,
   expanded byte count, exact compressed bytes, and derived compressed byte
   count/base64. Constructors reject unsupported codecs, zero or oversized
   counts/sizes, and inconsistent uncompressed-frame metadata before network
   I/O.
3. Every SDK provides dependency-free canonical encoders for `none` and RFC
   1952 gzip. They validate 1–1,000 records, unique unsigned 32-bit client
   sequences, and event envelopes; emit Rust/Serde-compatible field order,
   UTF-8, compact JSON, and sorted object-map keys; and enforce the 4 MiB
   expanded and 360 KiB frame ceilings.
4. LZ4, Snappy, and Zstandard remain fully supported through typed
   caller-supplied standard frames. Epoch does not select an optional native or
   third-party compressor dependency on the application's behalf. The Rust
   authority decompresses and validates canonical JSON, exact sizes/counts,
   unique sequences, envelopes, and the Zstandard window cap before proposal.
5. An idempotency key identifies the entire frame and metadata. A retry keeps
   the exact frame bytes and key, even after leader rediscovery. Equivalent
   records recompressed into different bytes are a different semantic request
   and conflict under the same key.
6. This API remains single-shard and whole-batch atomic. It does not choose a
   shard from multiple record keys, split a batch, report independent partial
   success, negotiate compression, manage connection credit, or batch records
   automatically.

## Consequences

- Go, Java, and Python applications can use the already-proven replicated batch
  protocol without hand-writing regional routing or canonical JSON.
- The built-in gzip path is portable and sufficient for exact executable docs
  and recovery campaigns; applications retain freedom to use their existing
  LZ4/Snappy/Zstd libraries without SDK dependency expansion.
- The authoritative Rust decoder remains the final validation boundary, so an
  opaque caller frame cannot bypass decompression-bomb or canonicality checks.
- STREAM-006 advances through a versioned first-party SDK surface, but stable
  native streaming Produce, automatic batching/codec selection, non-atomic
  per-record errors, cross-shard planning, fuzz/load evidence, and matched
  compression benchmarks remain open.

## Rejected alternatives

- Add LZ4, Snappy, and Zstandard libraries to every SDK, increasing package and
  native build surface even when an application already has a codec stack.
- Accept lists of events and silently choose shards, which could split one
  caller identity across resource-generation changes and overstate atomicity.
- Re-encode caller-supplied frames during retry, which would change the
  idempotent semantic request after an unknown outcome.
- Name this operation `Produce`, which would conflate a bounded atomic HTTP
  mutation with the future bidirectional native protocol.
