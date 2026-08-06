# ADR-0019: Regional Cache v1 and SDK Routing

**Status:** Accepted

**Date:** 6 August 2026

## Context

Epoch's replicated Cache tablet already owns strict scalar and collection
values, compare-and-set, conditional delete, atomic shard-local transactions,
signed increments, explicit expiry maintenance, and lease-based locks with
fencing tokens. Applications could reach that implementation only through the
generic experimental regional adapter or the direct tablet listener. Those
interfaces prove the state machine, but leave every caller to reproduce leader
discovery, routing fences, authorization, browser-safe integer encoding,
idempotent retry, and ambiguous-outcome handling.

The regional Stream and Queue v1 clients established a direct-to-Rust data path
with a shared private discovery core. Cache needs the same boundary without
creating a second cache state machine or weakening linearizable reads after a
leader change.

## Decision

1. The versioned Cache shard base path is:

   ```text
   /v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}
   ```

   `GET` on this path performs route discovery.
2. Data routes beneath that base are exact adapters over the existing
   materialized Cache and State tablet:

   ```text
   POST mutations
   GET  mutations/{proposal_id}
   GET  observations?key={key}
   GET  status
   ```
3. Mutations use the tablet's strict tagged operation union: `set`, `delete`,
   `compare_and_set`, `increment`, `transaction`, `acquire_lock`, `renew_lock`,
   `release_lock`, and `maintain`. The regional adapter performs no body
   translation and owns no state.
4. SDK value constructors cover `string`, `blob`, `counter`, `hash`, `list`,
   `set`, and `sorted_set`. They reject invalid local representations before a
   network call. Signed and unsigned 64-bit JSON fields are emitted as canonical
   decimal strings; Java exposes `BigInteger` where an unsigned range is needed.
5. Every data call carries the discovered resource generation and tablet epoch.
   Every mutation also carries the discovered term and a caller-owned
   idempotency key. Rediscovery never substitutes a key or semantic operation.
6. Discovery requires `route.read`; Cache observation, mutation lookup, and
   status require `data.read`; Cache mutations require `data.write`. Scope is
   parsed from the fully qualified path before dispatch.
7. Every SDK read explicitly requests `linearizable`. There is no automatic
   fallback to `local_stale`.
8. Go, Java, and Python expose a separate `RegionalCacheClient`. Their shared
   private regional cores continue to own endpoint validation, bearer handling,
   route parsing, fences, retry classification, and at most one rediscovery.
9. The v1 tablet has one shard. Transactions require the observed shard
   revision, contain one to 128 distinct-key mutations, and either commit as one
   revision or reject without partial state. SDK mutation helpers cannot embed
   transaction or lock-management operations inside a transaction.
10. TTL is relative at proposal time. Expired entries remain observable until a
    replicated `maintain` command removes them; v1 performs no background expiry
    or implicit mutation during reads. `maintain` accepts one to 1,000
    expirations per command.
11. Lock owner epochs are caller-monotonic unsigned integers. Lease tokens are
    opaque and rotated on renewal. Downstream systems compare the returned
    `(tablet_epoch, acquisition_index)` fencing token, not the lease token.
12. Responses remain server JSON documents until generated response models are
    introduced. A committed business rejection is a successful mutation receipt
    and is not retried by the SDK.

## Consequences

- Applications can exercise the full implemented Cache lifecycle through a
  leader replacement without depending on the Go management process.
- Exact retry can resolve a lost response, while reusing a key with different
  semantics produces a deterministic idempotency conflict.
- Cache observations after successful mutation are linearizable, but expiry is
  deliberately command-driven and therefore testable without hidden timers.
- The real three-voter campaign can kill the Cache leader before driving typed
  Python SDK values, CAS, transaction, increment, fenced lock, expiry
  maintenance, voter catch-up, and all-voter reopen.
- This route does not claim active background expiry, LRU/LFU eviction,
  multi-shard transactions, snapshots, Pub/Sub, multi-key command pipelining,
  connection pooling policy, dynamic membership, TLS/OIDC/mTLS, generated
  response types, package publication, or the production fault/scale matrix.

## Rejected alternatives

- Proxy Cache data through Go, making management availability a data-path
  dependency.
- Add convenience server endpoints that synthesize mutations and therefore
  create a second request-identity contract.
- Generate mutation idempotency keys inside an SDK, preventing exact recovery
  after an application restart or timeout.
- Remove expired values as a side effect of reads, making observation mutate
  replicated state without a consensus command.
- Treat lease tokens as fencing tokens; a delayed holder could then write after
  leadership or ownership changed.
- Read from a follower when a quorum barrier fails, silently weakening the
  requested consistency.
