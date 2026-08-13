# ADR-0018: Regional Queue v1 and SDK Routing

**Status:** Accepted

**Date:** 6 August 2026

## Context

The replicated Queue tablet already owned enqueue, credit-aware acquire,
lease renewal and settlement, timeout maintenance, dead-letter history, and
redrive. Applications could reach it only through the generic experimental
regional adapter or the direct tablet listener. Those routes proved the state
machine but forced each caller to reproduce Epoch's leader discovery, fencing,
authorization, browser-safe integer, and ambiguous-outcome rules.

The regional Stream v1 work established a direct-to-Rust application contract
that keeps Go out of the data path. Queue needs the same routing primitive
without creating a second queue state machine or pretending the alpha HTTP
request/response acquire is the future bidirectional receive protocol.

## Decision

1. The versioned Queue shard base path is:

   ```text
   /v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}
   ```

   `GET` on this path performs route discovery.
2. Data routes beneath that base are exact adapters over the existing
   materialized Work Queue tablet:

   ```text
   POST mutations
   GET  mutations/{proposal_id}
   GET  counts
   GET  dead-letters?limit={1..1000}
   GET  redrives?limit={1..1000}
   GET  consumers/{consumer}/flow
   GET  status
   ```
3. Mutations use the tablet's strict tagged operation union: `enqueue`,
   `acquire`, `acknowledge`, `extend_lease`, `release`, `nack`, `reject`,
   `redrive`, and `maintain`. The regional adapter performs no body
   translation and owns no state.
4. Every data call carries the discovered resource generation and tablet
   epoch. Every mutation also carries the discovered term and a caller-owned
   idempotency key. The client never substitutes a key during retry.
5. Discovery requires `route.read`; Queue GETs require `data.read`; Queue
   mutations require `data.write`. Scope comes from the fully parsed tenant
   path before route dispatch.
6. Counts, history, flow, mutation lookup, and status reads explicitly request
   `linearizable`. There is no SDK fallback to `local_stale`.
7. Go, Java, and Python expose a separate `RegionalQueueClient`. Shared private
   regional cores own endpoint validation, bearer handling, route parsing,
   fencing, retry classification, and bounded rediscovery for both Stream and
   Queue clients.
8. One operation gets at most two discovery/operation cycles. Retryable
   transport, leader, route, fence, or read-barrier errors rediscover; definitive
   authentication, authorization, validation, idempotency conflict, and
   committed business rejection return immediately.
9. The first contract is one shard mapped to partition `0`. Acquire accepts
   one to 100 messages, optional one-to-10,000 `max_in_flight`, and an optional
   nonzero visibility timeout. SDKs preserve 64-bit values as decimal strings
   on JSON fields that must be browser safe; Java also exposes `BigInteger`
   overloads.
10. Responses remain server JSON documents until generated response models are
    introduced. Opaque lease tokens are returned unchanged and must not be
    parsed by applications.

## Consequences

- An application can run the complete Queue lifecycle through a leader
  replacement without depending on the Go management process.
- Exact retry can resolve a lost response, but a caller that changes either the
  idempotency key or semantic operation has created a different request or an
  idempotency conflict.
- The real three-node campaign now kills the Queue leader, runs the Python SDK
  through exact enqueue replay, credit acquire, renewal, release, reject,
  immutable dead-letter observation, redrive, acknowledgement, linearizable
  counts/flow/history, voter catch-up, and all-voter reopen.
- This route does not claim native bidirectional receive, connection-scoped
  replenishment, automatic prefetch, cross-consumer fairness, multi-partition
  routing, timer precision/load evidence, indexed backlog-scale counts, dynamic
  membership, TLS/OIDC/mTLS, generated response types, or package publication.

## Rejected alternatives

- Proxy Queue data through Go, making management availability a data-path
  dependency.
- Add convenience endpoints that translate into a second request identity,
  increasing the risk that SDK and tablet retry semantics diverge.
- Let the SDK choose idempotency keys, which prevents applications from safely
  resolving ambiguous outcomes across process restarts.
- Read from a follower after a barrier failure, which silently weakens the
  requested consistency.
- Present request/response acquire as the final streaming receive design.
