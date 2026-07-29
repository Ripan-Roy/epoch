# ADR-0012: Topology-Aware Fixed-Voter Admission

**Status:** Accepted

**Date:** 29 July 2026

## Context

The regional alpha ran real three-voter catalog and data groups, but the
control plane knew only which node IDs answered a route request. Desired
replicas could not express a region, zone count, or node class, and the Go
reconciler could ask Rust to materialize more groups than a node's configured
supervisor limit. The console therefore had to call every failure domain
unverified.

The current consensus adapter has immutable three-node membership. This
increment must improve admission and evidence without pretending that a
general placement solver, membership protocol, or online rebalance already
exists.

## Decision

1. Every regional Rust process receives bounded `region`, `zone`, and
   `node_class` identifiers at startup. Its fixed consensus voter IDs come from
   the validated peer configuration rather than a second operator-supplied
   list.
2. Rust exposes the authorization-protected
   `GET /experimental/v1/regional/topology`. The response contains the local
   node ID, immutable topology, exact fixed voter set, configured maximum
   consensus groups, live used groups, and available groups. The catalog
   counts as one used group; each materialized tablet contributes one.
3. `topology.read` is a separate bootstrap-policy action. Go uses its existing
   workload credential to sample every configured endpoint. A partial,
   malformed, duplicate, or voter-inconsistent inventory cannot authorize a
   catalog mutation. This authenticates Go to Rust; plain HTTP does not
   authenticate the Rust server to Go, so mTLS remains required for production.
4. `ResourceSpec.placement` carries an optional allowed-region set, minimum
   zone count, and required node class. An omitted policy defaults only to one
   zone for backward compatibility.
5. Go admits only the exact current three-voter set. Every voter must satisfy
   the region and class filters, the set must span the requested zone count,
   and every node must have capacity for each additional desired shard.
   Updates charge `desired_shards - observed_shards`; exact retries and
   observation-only reconciliation charge zero.
6. Admission failure occurs before Rust catalog `Apply`. It has a stable code
   and, for capacity, the limiting node, required groups, and available groups.
   Desired Go metadata remains visible as failed so an operator can inspect the
   reason.
7. Resource status and the browser projection separate requested placement,
   policy-protected configured-endpoint evidence, achieved zones, and observed
   serving voters.
   A one-node outage may reuse the generation-fenced admitted topology while
   fresh route sampling truthfully reports two serving voters. A total outage
   still clears current route and topology status.

## Consequences

- The fixed local Compose region proves three configured zones and rejects an
  over-capacity resource without mutating the catalog.
- The console can mark zone separation verified and explain per-node group
  capacity. It continues to name rack separation, membership changes, and
  dynamic rebalancing as non-claims.
- A fresh complete inventory is mandatory for every catalog mutation. This
  favors safety over accepting desired changes during a topology outage.
- Capacity is group-count admission, not CPU, memory, disk, network, or
  per-profile sizing. Reservations are not yet transactional across multiple
  concurrent reconcilers; the current Go metadata store has one owner.
- Residency export enforcement, dedicated tenancy, rack constraints, dynamic
  membership, online transfer/repair/rebalance, and production peer identity
  remain future slices.

## Rejected alternatives

- Treat three distinct node IDs as proof of three zones.
- Put operator-entered topology labels only in Go without Rust node-local
  evidence from the configured endpoints.
- Allow Go to select a voter subset that the immutable Rust membership cannot
  enact.
- Admit against total configured capacity instead of live used capacity.
- Reserve total desired shards again on every retry.
- Claim the bounded fixed-voter validator is the final placement solver.
