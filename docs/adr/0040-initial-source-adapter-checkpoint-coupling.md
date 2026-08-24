# ADR-0040: Initial source adapters and checkpoint coupling

- Status: Accepted
- Date: 2026-08-24
- Owners: data plane, integrations, security, release engineering
- Extends: ADR-0037 and ADR-0039

## Context

Epoch already owned a replicated Event Bus connector checkpoint and a leader-
owned HTTP source worker. Object stores, database replication streams, and
Kafka expose different positions and acknowledgement mechanisms, but allowing
each protocol implementation to mutate Bus state or advance its cursor directly
would create different loss windows for every adapter.

The alpha-exit boundary requires S3-compatible/object storage, PostgreSQL,
MySQL, and Kafka sources without weakening the existing record-before-
checkpoint invariant.

## Decision

Protocol libraries remain behind adapter-specific readers in the Rust regional
runtime. A reader may perform source I/O and return a bounded `SourceBatch`; it
cannot apply a Bus command. The shared delivery worker alone proposes records,
records applied/error-routed outcomes, commits the batch receipt and exact
source cursor, and performs any post-checkpoint upstream acknowledgement.

The normalized batch contains:

- one stable batch identity;
- the exact source position from which the read began;
- one advancing source position for the complete batch; and
- 1–1,000 ordered event or stable error records.

Every record identity is stable for the same source position. The shared worker
derives its proposal identity from connector, batch, and record index. It
resolves an unknown result through replicated mutation lookup rather than
issuing a different proposal.

### Ownership and lifecycle

Only the current non-fail-stopped source Bus leader may read. Each pass builds
the set of currently eligible stateful connector identities. Leadership loss,
route loss, connector pause, deletion, or source-identity change closes
PostgreSQL replication and Kafka consumer sessions. MySQL opens a bounded
non-blocking binlog stream from the Epoch cursor for each pass; object and HTTP
reads are stateless.

### Source positions

| Adapter | Epoch-authoritative position | Complete batch boundary |
|---|---|---|
| HTTP/CloudEvents | Exact opaque source cursor | Strict response batch |
| Immutable object | Lexical key plus version/ETag/size evidence | Complete conditioned object read |
| PostgreSQL | Commit LSN bound to source configuration | `Commit` for one buffered transaction |
| MySQL | Binlog filename plus next position bound to source configuration | `XID` or explicit transaction `COMMIT` |
| Kafka | Next offset per assigned topic/partition bound to source configuration | Bounded poll result |

An object previously checkpointed under different version, ETag, or size is an
overwrite error. PostgreSQL and MySQL never checkpoint a partial transaction.
Kafka seeks each assignment from the replicated Epoch cursor; automatic offset
storage and commit are disabled.

### Durable ordering

For a non-empty batch the worker:

1. verifies current leadership and connector identity;
2. commits or resolves every event proposal;
3. commits every applied or error-routed record result;
4. commits the batch outcome and exact `source_to` checkpoint; and
5. then sends PostgreSQL applied-LSN feedback or synchronously commits Kafka
   group offsets.

A crash before step 4 replays stable Epoch proposal identities. A crash between
steps 4 and 5 reconciles the pending protocol session against the replicated
Epoch cursor and retries only the upstream acknowledgement. MySQL reconnects
from the Epoch checkpoint and object/HTTP sources require no upstream commit.

### Security and bounds

- Replicated connector configuration contains references, never secret values.
- A strict node-local `connector_credentials` entry provides 1–64 named values;
  debug and status representations redact them.
- Database and broker hosts must match the connector allowlist. Verified TLS is
  the default; plaintext is accepted only for explicitly enabled loopback
  development. Optional CA and client certificates preserve hostname
  verification.
- Object endpoints reuse safe-target validation and exact allowlists. Provider
  credentials are explicit; anonymous access cannot be combined with a secret.
- Object scans, transaction buffers, Kafka polls, payloads, topics, brokers,
  and timeouts have hard limits before replicated mutation.

Malformed or oversized complete records become stable connector error records
where a source can advance safely. Incomplete database transactions do not
advance. Configuration, credential, transport, cursor-gap, and source-identity
errors fail the connector pass without changing its checkpoint; another
connector may still run.

## Verification

- Deterministic adapter tests cover strict configuration, cursors, transaction
  assembly, ordering, overwrite/gap detection, stable errors, secret redaction,
  transport policy, pending-ack reconciliation, and session cleanup.
- A pinned Compose campaign exercises MinIO/S3, PostgreSQL logical replication,
  MySQL row binlogs, and Kafka consumer groups through real protocol servers.
- The existing real three-process HTTP campaign proves shared Bus checkpoint
  convergence, leader failover, stable replay, and all-voter reopen.
- CI runs deterministic tests on every change and the real connector campaign
  as a separate conformance job.

## Consequences

Adding another source requires implementing its bounded reader, canonical
cursor, stable identities, security policy, and conformance fixture; it does
not receive a separate Bus mutation path. Epoch provides at-least-once source
acquisition with duplicate-safe Bus admission for stable source identities. It
does not claim exactly-once external downstream effects.

Live Azure/GCS cloud IAM, private networking, secret-manager hot rotation,
schema-aware decoded CDC records, managed slot/WAL retention, production load
and soak, crash injection at every network boundary, and official Kafka client
compatibility remain separate gates.
