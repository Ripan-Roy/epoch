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
- `set_stream_offset`: resource/group identity, partition, next offset, and reset flag.

Recovery rejects an unknown engine format, malformed JSON, mutation ordering
that cannot be applied, and checksum corruption. It truncates only an incomplete
outer tail. The writer fsyncs each local-durable mutation before applying it to
live Stream memory, so a persistence failure cannot produce a successful or
visible mutation.

`engine-journal-v1-create-stream.json` is compiled into the engine test suite.
Any intentional change requires a new version or an explicit compatible-fixture
review. This node-level journal is replaced by the tablet log/snapshot format in
the replicated architecture; it has no snapshot or compaction contract.
