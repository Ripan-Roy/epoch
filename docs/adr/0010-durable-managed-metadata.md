# ADR-0010: Durable Single-Owner Go Control Metadata

**Status:** Accepted
**Date:** 29 July 2026

## Context

The first managed-control slice kept desired resources, observed reconciliation
status, generation tombstones, and request-token outcomes in Go process memory.
Restarting `epoch-control` therefore forgot accepted intent and made an exact
client retry indistinguishable from a new request. That behavior could exercise
the Go-to-Rust ownership boundary, but it could not satisfy the PRD requirement
that an acknowledged hosted mutation survive a process crash.

This state is management metadata only. Rust remains authoritative for the
regional catalog, placement, tablet state, and every customer-data path.

## Decision

Epoch stores the bounded Go control registry in an embedded bbolt database with
an explicit version-one schema.

1. The database contains separate metadata, live-resource, last-generation,
   and request-token buckets. The schema version is checked before any state is
   served.
2. Desired resources, observed status, delete/recreate generation tombstones,
   request fingerprints, and original apply/delete outcomes are recovered on
   startup.
3. A mutation is committed in one bbolt write transaction before the registry
   publishes it in memory or acknowledges it to the caller. A failed commit
   leaves the prior in-memory state visible.
4. bbolt's synchronous transaction behavior remains enabled. The database file
   is restricted to mode `0600`; newly required parent directories use `0700`.
5. One process owns the database through bbolt's exclusive file lock. A second
   owner, corrupt file, missing bucket, malformed record, or unknown schema
   version fails startup instead of silently creating empty state.
6. `EPOCH_CONTROL_STATE_PATH` selects the file and defaults to
   `data/control/registry.db`. Health reports `registry: bbolt_v1` and
   `registry_durable: true`.
7. Unit tests cover restart recovery, exact replay, generation continuity,
   corruption, schema rejection, exclusive ownership, and commit-before-
   visibility. The regional container campaign kills and reopens the real Go
   process against the same file before continuing reconciliation.

## Consequences

- A single `epoch-control` process can recover acknowledged desired intent and
  safely resolve exact retries after `SIGKILL`.
- The persisted format now requires explicit schema migration before an
  incompatible change.
- Observed placement can be briefly stale after restart, but the reconciler
  refreshes it through the Rust authority and generation fencing prevents an
  older observation from marking newer intent ready.
- This slice does not provide multi-instance linearizability, leader election,
  replicated management storage, backup automation, authorization, encryption
  with customer-managed keys, or an advertised token-retention window. Those
  remain managed-service work.

## Rejected alternatives

- Continue using process memory and weaken hosted durability language.
- Persist resources but discard token outcomes or generation tombstones.
- Write JSON files with rename-based coordination and a custom transaction log.
- Allow two Go processes to open the same database without a replicated
  ownership protocol.
- Store Go management metadata inside Rust customer-data logs or segment files.
