# ADR-0001: Workload Profiles and Tablets

**Status:** Accepted  
**Date:** 22 July 2026

## Context

Epoch must provide cache, stream, queue, and event-bus behavior without erasing
the semantics or performance properties that make those workload families
useful. A single execution path would put durable-log latency in the volatile
cache path, confuse queue acknowledgement with stream offsets, and couple
unrelated retention and recovery rules.

Epoch also needs one operational unit for placement, replication, failover,
snapshot, repair, and resource accounting.

## Decision

Epoch exposes four immutable resource profiles:

- Cache and State;
- Stream Log;
- Work Queue;
- Event Bus.

A resource has one or more logical **shards**. A shard maps to one physical
**tablet**. A tablet is a leader-fenced, replicated state machine and the unit of
placement, failover, transfer, snapshot, restore, split, repair, and admission.

The initial implementation follows these rules:

1. One tablet hosts one profile-specific state machine.
2. Different tablets and profiles may be co-hosted in one process, but their
   logs, retention, compaction, and failure state remain independent.
3. A durable tablet has one consensus group. The node runtime multiplexes many
   groups over shared transports, schedulers, I/O batching, and telemetry.
4. Volatile Cache mutations bypass the durable commit log.
5. Stream records, queue transitions, durable Cache mutations, and durable Bus
   ingress are typed log entries applied by their profile state machine.
6. Consumer groups, transaction coordinators, schemas, and subscription ledgers
   use separately sharded system tablets rather than the regional catalog.
7. Physical payload sharing is allowed only within one tablet and transaction
   domain in v1. Cross-tablet pipes commit a new target record and preserve
   origin identity and position.
8. Changing a resource from one profile to another is a migration or pipe, not
   an in-place semantic conversion.

## Consequences

- The fast Cache path stays short, while protected resources retain a precise
  commit point.
- Queue delivery state and Stream consumer state can evolve independently.
- Cross-profile features reuse storage and operations without pretending that
  their user-visible semantics are identical.
- A Multi-Raft runtime and low per-tablet overhead are required to meet the
  resource-count target.
- Cross-tablet pipes initially pay an extra write and may copy the payload. This
  is accepted to avoid distributed reference counting and hidden retention
  coupling.
- Profile conversions are explicit and therefore observable, testable, and
  reversible.

## Rejected alternatives

- One universal log path for every Cache operation.
- One process or replica set per logical resource.
- A global payload heap with cross-resource reference counting in v1.
- Storing queue acknowledgements or consumer offsets in the catalog consensus
  group.

