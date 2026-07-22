# Persisted format fixtures

This directory contains reviewed golden vectors for provisional Epoch persisted
formats. A fixture protects byte-level compatibility within its format version;
it does not make the pre-alpha format a permanent public contract.

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

Recovery rejects an unknown engine format, malformed JSON, mutation ordering
that cannot be applied, and checksum corruption. It truncates only an incomplete
outer tail. The writer fsyncs each local-durable mutation before applying it to
live Stream or Queue memory, so a persistence failure cannot produce a
successful or visible mutation. Queue replay re-executes deterministic commands
at their recorded apply times, preserving lease generations and tokens.

The `create_stream` and `create_queue` vectors are compiled into the engine test
suite. Any intentional change requires a new version or an explicit
compatible-fixture review. This node-level journal is replaced by the tablet
log/snapshot format in the replicated architecture; it has no snapshot or
compaction contract.
