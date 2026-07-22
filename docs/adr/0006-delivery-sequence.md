# ADR-0006: Delivery Sequence and Initial Wedge

**Status:** Accepted  
**Date:** 22 July 2026

## Context

The complete PRD covers four workload profiles, six compatibility surfaces,
multiple deployment models, a managed service, cross-region recovery, schemas,
connectors, and advanced P2 data structures. Attempting all of them in parallel
would delay proof of the hardest shared properties: durable commit, fencing,
recovery, and queue delivery state.

The recommended first customers already operate multiple infrastructure
products. Stream plus Queue provides a useful commercial wedge and exercises the
replicated log, offsets, leases, scheduling, acknowledgement, and recovery that
the other profiles need.

## Decision

Build evidence-producing vertical slices in this order:

1. **Foundation:** contracts, invariants, deterministic testkit, format fixtures,
   and matched Redis/Kafka/RabbitMQ benchmark environments.
2. **Standalone Stream:** crash-safe segment append/fetch, recovery, native API,
   CLI, metrics, and initial Go, Java, and Python SDK slices.
3. **Replicated foundation:** three-node catalog and tablet consensus, quorum
   acknowledgement, leader fencing, membership, snapshot, catch-up, placement,
   and injected failures.
4. **Queue core:** send/acquire/settle, fenced leases, retry/backoff, schedule,
   dead-letter, credits, publisher receipts, and queue observability.
5. **Cache core:** an independent volatile path with P0 data types, TTL,
   eviction, atomic same-shard mutations, and CAS; then replicated-memory and
   durable changelog/snapshot modes.
6. **Regional productization:** declarative administration, quotas, auth, audit,
   backup/restore, repair/rebalance, Kubernetes operator, production SDK parity,
   and local integration packages.
7. **Compatibility and P1 storage:** named Kafka, RESP3, and AMQP subsets;
   producer idempotence, consumer groups, durable Cache restore, compaction,
   schemas, tiering, capture, and migration tooling.
8. **Event Bus and integration:** routing, filters, subscription ledgers,
   webhooks, archive/replay, transformations, MQTT, and initial connectors.
9. **Managed service:** Go fleet plane, dedicated then serverless pools, console,
   private networking, metering/billing, customer-managed keys, and geo-async
   DR.
10. **GA and north-star expansion:** bounded transactions, FIFO sessions,
    dedupe/rate controls, certified compatibility, DR/upgrade hardening, then P2
    search/vector/probabilistic/flash/global/connector breadth.

Stream plus Queue is the initial commercial wedge. All four profile types remain
part of the architecture and feature traceability; sequencing is not a removal
of later requirements.

## Exit policy

Each slice must include:

- documented semantics and typed API/error behavior;
- format and upgrade fixtures where it persists data;
- deterministic correctness and fault histories;
- security and tenant-boundary review proportional to the slice;
- profile metrics, traces, and audit behavior;
- matched latency, throughput, saturation, and recovery benchmarks;
- a restore or rollback path.

The next breadth feature does not bypass a failed quorum, fencing, recovery, or
acknowledgement invariant.

## Consequences

- The first useful product is narrower than the full catalog.
- Protocol compatibility begins only after native semantics are measurable.
- Event Bus reuses proven Stream ingress and Queue-like subscription delivery.
- Cache work can proceed in parallel after the tablet/runtime boundary is stable,
  but its volatile path remains physically independent.
- Dedicated and self-hosted deployments can ship before serverless, reducing the
  first operational surface.
- Feature status must be maintained against every PRD requirement so postponed
  work is visible.

## Rejected alternatives

- Implementing all protocol gateways before a native replicated vertical slice.
- Launching serverless before dedicated/self-hosted failure behavior is proven.
- Cutting quorum correctness, fencing, restore, or observability to preserve
  feature breadth.
- Positioning an alpha as full Redis, Kafka, RabbitMQ, or cloud-service parity.
