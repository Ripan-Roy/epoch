# ADR-0017: Regional Stream v1 and SDK Routing

**Status:** Accepted

**Date:** 6 August 2026

## Context

The regional runtime already materialized replicated Stream tablets and exposed
them through a generic experimental resource router. That route proved
consensus, fencing, and recovery, but it was unsuitable as an application
contract:

- callers had to construct an internal profile operation suffix;
- the path did not distinguish a stable Stream surface from the generic tablet
  adapter;
- every caller had to independently discover leadership and copy fences; and
- the Go, Java, and Python SDKs could only use the standalone process API.

The hosted Go control plane must not become a data-path proxy or correctness
dependency. An already materialized regional Stream must keep serving through
Rust when Go is unavailable.

## Decision

1. The versioned Stream shard base path is:

   ```text
   /v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}
   ```

   It is fully qualified so authorization and delete/recreate fencing never
   depend on an ambient tenant or a name-only cache.
2. `GET` on the base path is route discovery. It returns the local role,
   current term, leader identity, resource generation, tablet identity and
   epoch, and `accepts_writes`. All 64-bit values remain decimal strings.
3. Operations beneath the base path are an adapter over the same materialized
   Stream tablet. The adapter does not create a second state machine, log,
   checkpoint store, or write path.
4. Discovery requires `route.read`. Data GETs require `data.read`; data
   mutations require `data.write`. Authentication and authorization run on the
   fully parsed organization/project/environment/namespace scope.
5. Every data operation carries the exact
   `x-epoch-resource-generation` and `x-epoch-tablet-epoch` observed during
   discovery. A stale value, missing route, nonleader, or unavailable group is
   a retryable routing outcome.
6. Go, Java, and Python expose an explicit `RegionalStreamClient` and
   `RegionalScope`. This client is separate from the standalone client because
   the durability, identity, routing, and retry contracts differ.
7. Before every operation, the client queries configured Rust endpoints and
   selects one whose complete route response reports `accepts_writes: true`.
   Reads deliberately use the leader too.
8. Append and group checkpoint mutations require a caller-owned idempotency
   key. The client supplies the discovered term, never invents a replacement
   key, and performs at most two discovery/operation cycles for retryable
   transport, routing, fencing, or read-barrier outcomes. Definitive semantic
   errors return immediately.
9. Fetch, group fetch, and lag explicitly request `linearizable` consistency.
   The client never silently downgrades to `local_stale`.
10. The first SDK surface covers partition-0 single-record append, bounded
    offset fetch, generation-fenced checkpoint commit/reset, lag, and fetch
    from the durable checkpoint. Responses preserve the server JSON document
    until generated response types are introduced.
11. The existing `/experimental/v1/regional/resources/.../data/...` adapter and
    direct tablet routes remain available for internal verification. This
    decision does not rename them or grant them a public compatibility
    commitment.

## Consequences

- Applications can survive a leader replacement without embedding Epoch's
  routing and fence protocol in business code.
- The same idempotency key resolves an ambiguous mutation outcome after
  rediscovery; the SDK cannot promise success when every endpoint is
  unavailable.
- Go control-plane loss does not interrupt an already materialized Stream data
  path because SDK discovery targets Rust nodes directly.
- Clearing outer-router path parameters at the adapter boundary is required
  before dispatching to handlers with their own `Path` extractors. A regression
  test covers group routes; without this boundary Axum rejects the incompatible
  cached parameter shape before the tablet handler runs.
- This is a versioned alpha application surface, not a completed consumer
  coordinator. Join, heartbeat, assignment, revoke, dead-member detection,
  automatic generation allocation, rebalance, multi-partition ownership,
  transactional offsets, producer auto-batching, compression negotiation,
  generated response models, and package-registry publication remain open.
  ADR-0023 subsequently adds retention configure, maintain, and observe methods
  to this route and all three clients without changing its discovery/retry
  contract. ADR-0024 subsequently extends the resource to several independent
  shard tablets, publishes versioned key-partition metadata, and adds
  generation-pinned keyed append while preserving explicit-shard methods and
  the original inner partition-0 tablet contract.

## Rejected alternatives

- Proxy application data through Go, which adds latency and makes hosted
  control availability a regional data-path dependency.
- Accept a name-only route, which is ambiguous across tenants and unsafe across
  delete/recreate generations.
- Cache a leader indefinitely, which makes stale terms and tablet epochs normal
  application failures instead of explicit rediscovery.
- Generate a new idempotency key during retry, which can duplicate a mutation
  whose first response was lost.
- Default reads to a follower or silently downgrade after a barrier timeout,
  which would weaken the consistency selected by the application.
