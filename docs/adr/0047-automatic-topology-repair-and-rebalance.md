# ADR-0047: Automatic topology repair and serialized rebalance

- Status: Accepted
- Date: 2026-09-11
- Owners: regional control, data plane, managed API, verification
- Extends: [ADR-0012](0012-topology-aware-admission.md),
  [ADR-0039](0039-alpha-exit-beta-readiness.md)

## Context

Epoch already records an explicit three- or five-voter placement for every
tablet and can execute one committed voter replacement through learner catch-up,
joint consensus, Catalog finalization, and durable reopen. The remaining manual
gap is choosing that replacement when a node becomes ineligible, a placement
loses its required failure-domain spread, or the regional group load is
unbalanced.

Selecting a target only in Rust would duplicate the Go control plane's policy
and capacity view. Mutating Raft membership directly from Go would create a
second consensus authority. Automatic repair therefore needs a narrow boundary:
Go may plan one policy-compliant target from a fresh inventory, while Rust alone
commits and executes the fenced membership transition.

## Decision

1. Every Rust regional node reports a bounded, validated `region`, `zone`,
   `rack`, node class, Catalog voter set, and live consensus-group capacity.
   `EPOCH_REGIONAL_RACK` configures the rack label. Nodes without an explicit
   value report the single compatibility domain `unassigned`; Epoch never
   infers independent racks from zones.
2. The placement policy supports allowed regions, minimum zones, minimum racks,
   required node class, and an explicit sorted set of excluded physical node
   IDs. New three- and five-voter placements must satisfy every constraint before
   Catalog mutation.
3. Automatic planning requires a fresh, complete, internally consistent sample
   from every configured physical node. A stale generation-fenced sample may
   explain a serving resource during a transient outage, but it never authorizes
   a membership mutation.
4. Go evaluates every legal one-voter replacement deterministically. It first
   reduces policy-ineligible or missing voters, then reduces zone/rack deficits,
   and only then considers a load-balancing move. Ties use placement quality,
   normalized maximum utilization, utilization sum of squares, shard index, and
   the sorted target voter set.
5. A policy or topology repair may begin only when the current assigned and
   committed voters agree, the leader is reachable, and a majority of current
   voters is reachable. A pure load rebalance additionally requires every
   current voter to be reachable. The incoming node must advertise free group
   capacity.
6. The Go reconciler serializes its mutation path and commits at most one active
   membership target per resource. It observes Catalog finalization before
   planning another move for that resource. The plan token deterministically
   binds resource identity, customer generation, tablet, tablet epoch, and
   target voters.
7. Applying a new customer desired generation and planning an operational move
   occur on separate reconciliation passes. The desired spec is committed
   unchanged first. Later repair/rebalance plans do not increment the customer
   generation. Go's desired/observed generation is distinct from the Rust
   `catalog_generation`: Go-owned policy can advance while a no-op Rust apply
   retains its cursor. Every tablet `resource_generation`, membership fence,
   later Catalog apply, and delete uses the explicit Rust cursor. Legacy status
   without the additive field falls back to the formerly coupled generation.
   An absent-resource create expects zero; a recreate adopts the positive
   successor returned from Rust's retained Catalog tombstone.
8. Go submits the chosen target through the existing authorized Catalog
   membership endpoint. Rust revalidates the generation, tablet epoch, current
   placement, exact single-voter delta, and request-token binding before
   committing the plan. The existing Rust worker remains responsible for
   learner materialization, catch-up, joint consensus, finalization, removed
   runtime shutdown, and recovery after restart.
9. Browser-safe status reports requested and achieved zones/racks, excluded
   nodes, current/target/committed/reachable voters, and transition state. A
   resource cannot report `ready` if an assigned voter is missing, excluded, or
   outside the requested region/class policy, or if the achieved domain counts
   do not match the voter evidence. It exposes desired, observed, and Catalog
   generations as decimal strings and requires each tablet generation to equal
   the Catalog cursor.

## Consequences

An operator can evacuate a node by adding it to `excluded_node_ids`; the Go
control plane automatically commits the first safe learner-first repair and
continues one transition at a time until the resource satisfies policy. The
same planner repairs missing voters and failure-domain deficits and can improve
regional group distribution without changing application configuration.

The planner works across 3–1,024 physical nodes and explicit three- or
five-voter tablets. Its output is deterministic for one inventory snapshot.
Automatic motion stops when topology is stale, the current transition is still
active, the safety preconditions fail, no compliant target has capacity, or no
move improves placement quality/load.

This revision does not provide a transactional reservation shared by several
resources or Go controller instances. A just-committed learner may therefore
not be reflected in a different resource's immediately following inventory
sample. Multi-plan reservation, multi-instance management ownership, explicit
rebalance hysteresis, whole-node fleet evacuation progress, split/merge,
Kubernetes node-label attestation, broader chaos, and production placement SLOs
remain later gates.

## Rejected alternatives

- Direct Go mutation of Raft configuration was rejected because Catalog and the
  Rust regional controller are the durable membership authority.
- Repair from cached topology was rejected because stale capacity or failure
  domains can select an unsafe incoming voter.
- Replacing several voters in one plan was rejected because it bypasses the
  existing overlapping-majority learner-first proof.
- Treating a missing rack as its zone was rejected because that would claim
  physical separation that the node did not attest.
- Starting pure rebalance with only quorum reachability was rejected because
  optional load movement must not reduce an already degraded placement.
