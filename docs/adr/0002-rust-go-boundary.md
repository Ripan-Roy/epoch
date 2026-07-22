# ADR-0002: Rust and Go Boundary

**Status:** Accepted  
**Date:** 22 July 2026

## Context

Epoch needs predictable data-path latency and a single correctness boundary, but
also needs productive cloud APIs, fleet reconciliation, metering, billing, and
Kubernetes integration. The managed service must not become a runtime
dependency for a self-hosted cluster or an already-running region.

## Decision

Rust owns every component that reads, stores, replicates, routes, transforms, or
delivers customer data. This includes:

- storage, snapshots, indexes, compaction, tiering, and recovery;
- consensus, regional metadata, placement, fencing, leases, and transactions;
- Cache, Stream, Queue, Bus, schemas, routes, webhooks, and connector execution;
- native and compatibility protocol gateways;
- local administration, health, backup, restore, and migration data paths.

Go owns hosted management concerns:

- organization, project, environment, entitlement, and commercial APIs;
- global desired state and multi-region orchestration;
- fleet capacity, autoscaling policy, cloud infrastructure, and upgrades;
- metering, budgets, billing, and console backend services;
- the Kubernetes operator.

TypeScript and React own the browser console. It calls a Go management API/BFF,
which uses audited Rust APIs for any permitted data access.

Rust and Go communicate through versioned Protobuf/gRPC contracts. Go submits a
declarative desired spec with an idempotency token and expected generation. The
Rust regional authority validates and commits it, then reports
`observed_generation`, conditions, and operation status.

Go must never directly read or mutate:

- Epoch segment, snapshot, manifest, or object-tier files;
- replication or consensus logs;
- queue acknowledgement, lease, retry, or dedupe indexes;
- producer, transaction, or consumer-group state;
- Cache memory or durable Cache state.

Regional Rust services cache signed identity keys, policies, quotas, and the
partition map needed to continue serving. Loss of the hosted Go plane may block
new management operations, but it must not stop existing regional data paths,
leader election, repair, or delivery.

## Consequences

- Data correctness has one language/runtime boundary.
- Standalone and self-hosted deployments reuse the managed data-plane code.
- The Go plane can evolve quickly without becoming a write-path dependency.
- Regional APIs and policy-cache expiry/revocation behavior must be designed
  explicitly.
- Some functionality that has convenient Go libraries must still execute in a
  Rust worker or behind a strictly non-data-path management integration.
- Cross-language contract generation and compatibility tests are required from
  the first build.

## Rejected alternatives

- Go services directly accessing Rust storage or queue state.
- A hosted control-plane round trip on every data request.
- A second Node.js server acting as the product control plane.
- A C ABI that exposes internal engine structures to other languages.

