# ADR-0032: Regional Cache eviction and committed access batches

**Status:** Accepted

## Context

The standalone Cache already implements the P0 eviction-policy names, while
the replicated Cache deliberately rejects every policy except `no_eviction`.
The managed resource API also retains a Cache configuration in Go desired
state but sends only shard and replica counts to the Rust catalog. As a result,
an operator can select an eviction policy that no regional tablet ever uses.

Replicated eviction has an additional correctness constraint. Ordinary Cache
observations are pure quorum-confirmed reads. Updating LRU or LFU metadata from
those node-local observations would make voters diverge. Access metadata must
therefore be part of the committed state-machine history.

CACHE-005 also requires a bounded batch surface. Epoch already has a
distinct-key, shard-local atomic transaction, but the SDKs expose it only as an
optimistic transaction. The same wire operation can provide one-request
ordered batching without creating a second, weaker state transition.

## Decision

1. The Go reconciler forwards a bounded canonical profile configuration to the
   Rust catalog. The catalog persists it with the resource and the regional
   materializer derives the Cache tablet configuration from that committed
   value. A Cache configuration is immutable after creation in this slice;
   changing capacity, default TTL, or eviction policy requires a deliberate
   delete/recreate and therefore a new resource generation and tablet identity.
2. Replicated Cache supports `no_eviction`, `all_keys_lru`, `all_keys_lfu`,
   `all_keys_random`, `volatile_lru`, `volatile_lfu`, `volatile_random`, and
   `volatile_ttl`. Expired entries are logically absent before capacity is
   evaluated. Volatile policies reject atomically when no expiring victim is
   eligible.
3. LRU ranks by the last committed access revision. LFU ranks by committed
   access count and then access revision. TTL ranks by the earliest expiry.
   Ties are resolved by canonical UTF-8 key order. Random policies use a
   domain-separated SHA-256 rank over the next shard revision and candidate
   key. The result varies across admissions while remaining identical on every
   voter and after replay.
4. A versioned committed `Get` operation returns the value and advances access
   metadata exactly once. Replaying its idempotency key returns the prior
   receipt without another access. `Observe` remains a pure linearizable or
   explicitly stale diagnostic read and never affects eviction order.
5. Capacity admission is staged. A rejected command changes no value, access
   metadata, revision, receipt registry, or digest. Successful commands report
   their evicted keys in canonical order. New values in the command are not
   selected as their own admission victims; if the existing eligible set
   cannot make room, the complete command is rejected.
6. The existing transaction command remains the one-to-128 operation atomic
   batch. Go, Java, and Python add an `AtomicBatch` convenience API over the
   exact transaction wire contract. It preserves caller order, returns one
   correlated result per operation, and consumes one HTTP request and one
   consensus proposal. It is not a Redis-style partial-success pipeline.
7. Cache shard snapshots add a backward-readable v2 form containing eviction
   and access metadata. V1 no-eviction images and command bytes remain readable
   and byte-stable. No-eviction state digests remain stable; policy-specific
   state uses a new domain-separated digest extension.

## Consequences

- Eviction choices made in the console or SDK resource specification become
  real regional behavior rather than decorative metadata.
- LRU/LFU reads use a consensus write and have write availability and latency.
  Callers that only need observation can retain the pure read route.
- The policy is deterministic under leader replacement, checkpoint restore,
  and full-cluster replay.
- Atomic batching amortizes request and proposal overhead, but it does not yet
  provide partial results, cross-shard batching, automatic client coalescing,
  or a native streaming transport.
- Memory-pressure byte accounting and performance/SLO evidence remain separate
  work; this slice enforces the existing entry-count capacity contract.
