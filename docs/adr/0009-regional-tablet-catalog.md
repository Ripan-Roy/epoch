# ADR-0009: Deterministic Regional Tablet Catalog

**Status:** Accepted  
**Date:** 27 July 2026

## Context

Epoch's typed Stream, Queue, Cache, and Event Bus tablets already prove
fixed-voter commit and recovery independently. The experimental node still
selects exactly one profile for one consensus group at process startup, while
the Go control service keeps a separate in-memory desired-resource map. That
shape cannot support multiple resources, multiple shards, safe routing, or a
regional authority.

A Multi-Raft runtime needs one deterministic source for:

- fully qualified resource identity and monotonic resource generation;
- shard-to-tablet and tablet-to-consensus-group identity;
- immutable workload profile and fencing epoch;
- desired replica count and later placement observations;
- idempotent apply/delete commands and recovery.

## Decision

Epoch introduces `epoch-catalog`, an I/O-free Rust state machine that is the
authoritative domain boundary for regional resources and tablets.

1. Resource keys include organization, project, environment, namespace, kind,
   and name. Catalog keys do not assume a single hosted parent or one catalog
   group.
2. Each data-bearing resource declares an immutable workload profile, a bounded
   shard count, and a bounded desired replica count.
3. Every shard receives a never-reused tablet ID and a separate never-reused
   consensus-group ID. The first allocator advances both monotonically but APIs
   never equate them.
4. Resource generations increase on material updates, deletion, and
   recreation. Existing tablets receive the new resource generation so data
   requests can be fenced against stale configuration.
5. Online shard expansion preserves existing tablet identities and allocates
   only the suffix. Shrink, merge, and profile conversion are rejected until
   their explicit migration protocols exist.
6. Deletion removes live routes but retains the generation tombstone and never
   returns tablet or group identities to the allocator.
7. A request token is durably bound to one exact command. Replaying that command
   returns its original result; rebinding the token fails.
8. Catalog commands have strict canonical bytes and deterministic replay. The
   state machine does not perform I/O, placement, consensus, or profile
   mutations.
9. The versioned regional API reports tablet descriptors separately from
   desired resource specs. Desired replica count is not achieved placement.

## Runtime sequence

The implementation proceeds in bounded steps:

1. deterministic catalog commands, routing, lifecycle, and replay;
2. attach the catalog to its own EPRS-backed consensus group;
3. add a node-level group supervisor and peer-message demultiplexer;
4. materialize/dematerialize typed tablet services from committed catalog
   records;
5. route public data requests by resource generation, shard, tablet, leader,
   and epoch;
6. add placement reconciliation and observed voter/leader status;
7. shard the catalog behind a root namespace map when density evidence requires
   it.

Until steps 2–5 pass real-process and container fault tests, the catalog is a
state-machine foundation and the public data APIs retain their standalone
guarantee ceiling.

## Consequences

- Rust remains authoritative for regional data-path metadata; Go submits
  desired state through the versioned administration contract.
- Tablet identity, group identity, resource generation, and tablet epoch are
  distinct fences and can evolve independently.
- Deterministic allocation is easy to replay and test but requires a later
  explicit import/restore policy for identity ranges.
- Shrink and profile conversion remain visible migrations instead of hidden
  in-place changes.
- Placement and health cannot be inferred from desired replica count.

## Rejected alternatives

- Treating one Go process's in-memory registry as regional truth.
- Using resource names directly as consensus-group identities.
- Reusing tablet IDs after deletion.
- Remapping every shard when a resource expands.
- Starting one operating-system process per resource or tablet.
- Allowing profile conversion or shard shrink without a migration protocol.
