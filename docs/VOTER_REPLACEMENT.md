# Learner-first voter replacement

Epoch can replace one voter in an existing three- or five-voter tablet without
changing the tablet, consensus-group, tablet-epoch, or customer resource
generation. The physical regional inventory may contain 3–1,024 nodes; only
the explicit voter set for one tablet changes.

Rust owns the Catalog plan, Raft transition, durable membership, and tablet
materialization. Go may either expose the manual administrative primitive or
automatically select its next exact target from fresh topology. The automatic
planner enforces region, zone, rack, class, exclusion, reachability, and
capacity policy without rewriting Raft storage.

## Safety contract

One replacement follows this committed sequence:

1. Commit a Catalog plan containing the unchanged current voters and one exact
   target set.
2. Materialize the tablet runtime on the incoming physical node using the
   immutable bootstrap voter identity.
3. Add only the incoming node as a non-voting learner.
4. Wait until the leader reports the learner matched and committed through the
   current commit index, with no pending snapshot and recent activity.
5. Commit the target through Raft joint consensus. A three-voter group remains
   available through the overlapping majorities; a five-voter group uses the
   same rule.
6. After the stable committed membership equals the target, commit Catalog
   finalization and stop the removed node's local runtime.

Every worker pass reconstructs its next action from committed Catalog and Raft
state. Process restart therefore cannot skip learner catch-up or revert to the
bootstrap voter list. An in-memory pending map suppresses duplicate in-flight
requests but is not correctness state.

## Automatic planning

On a reconciliation pass after the current customer generation is already
observed, Go may commit one target automatically. It requires fresh complete
topology from every configured physical node and no active transition for the
resource. Candidate priority is:

1. replace an excluded, missing, wrong-region, or wrong-class voter;
2. improve a minimum-zone or minimum-rack deficit; then
3. improve normalized regional group utilization without weakening policy.

Policy/topology repairs require an observed leader and a reachable current
quorum. Optional load rebalance requires every current voter to be reachable.
The chosen incoming node must advertise capacity. Go waits for Catalog
finalization before it plans the resource's next move, so a multi-shard
resource converges through a series of independently durable single-voter
transitions.

The management and data-plane clocks are explicit. `generation` and
`observed_generation` identify the accepted and reconciled Go desired state;
`catalog_generation` identifies the Rust Catalog resource version and equals
the tablet `resource_generation`. Adding an exclusion may advance only the Go
clock because placement policy is Go-owned. The subsequent membership plan is
fenced by the unchanged Catalog/tablet generation and does not advance either
clock.

## Plan API

The same provisional regional administration route remains available to the Go
reconciler and authorized operators:

```text
POST /experimental/v1/regional/catalog/tablets/{tablet_id}/membership
```

It requires supported TLS, bearer authentication, and the cluster-scoped
`catalog.apply` action. IDs may be decimal JSON strings, which avoids loss in
JavaScript clients.

```json
{
  "request_token": "replace-orders-3-with-4-v1",
  "expected_tablet_epoch": "1",
  "expected_resource_generation": "7",
  "target_voter_node_ids": ["1", "2", "4"]
}
```

The target must be strictly sorted, contain exactly three or five nodes as
declared by the tablet, remain inside the physical-node directory, and differ
from the current set by exactly one removal and one addition. The
request token is replay-safe and cannot be rebound. Stale generation/epoch,
conflicting active plans, multi-voter replacement, zero/duplicate IDs, and
direct placement mutation are rejected before Catalog state changes.

The accepted response is `202 Accepted`. Its resource contains:

- `voter_node_ids`: the currently assigned Catalog voters;
- `bootstrap_voter_node_ids`: immutable startup identity for journal reopen;
- `target_voter_node_ids`: the active transition target, or an empty array
  after finalization.

Planning and finalization do not increment either generation clock. Go desired
generation fences management updates. Rust Catalog/tablet generation fences
SDK routing and the operational membership request together with tablet epoch,
the committed plan, and durable Raft configuration.

## Observe the transition

The Go browser-safe inventory is:

```shell
curl --fail-with-body \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  https://epoch-control.example/v1/regional/resources
```

An active replacement reports resource phase `pending` and separates:

- `generation` / `observed_generation`: accepted and reconciled Go intent;
- `catalog_generation`: Rust Catalog cursor shared by the tablets;

- `assigned_node_ids`: current Catalog placement;
- `bootstrap_voter_node_ids`: immutable initial voters;
- `target_voter_node_ids`: planned placement;
- `voter_node_ids`: currently committed Raft voters;
- `reachable_voter_node_ids`: voters observed through matching routes.

Committed voters may equal the current or target set while joint consensus is
being completed. They may never be an arbitrary mixture. After Catalog
finalization, `assigned_node_ids` becomes the target, the target field clears,
and a fully reachable leader-bearing tablet returns to `ready`.

## Recovery evidence

The focused four-node runtime campaign creates a three-voter Stream on nodes
1/2/3, commits data, plans node 3 → node 4, waits for automatic learner
catch-up and finalization, proves the record on node 4, proves node 3 stops
hosting the tablet, shuts down every process, and reopens the same journals as
voters 1/2/4. Catalog command/snapshot v5 tests separately prove canonical
bytes, exact replay, stale/conflicting rejection, and snapshot recovery.

Run the focused evidence with:

```shell
cargo test -p epoch-catalog --test catalog_membership
cargo test -p epoch-node --lib \
  catalog_planned_voter_replacement_catches_up_finalizes_and_reopens
go test -race ./control/internal/regional ./control/internal/resources
```

The regional Compose campaign additionally proves explicit three-rack
admission and browser-safe achieved-rack evidence. The live Kubernetes runner
updates a managed Stream with one current voter in `excluded_node_ids`, waits
for the Go reconciler to commit the automatic target, then proves the same
learner-first data-continuity/reopen path without directly calling this plan
API.

## Current limits

- A plan replaces exactly one voter; it never changes three voters directly.
- Only explicit three- and five-voter groups are supported.
- Rack-aware selection and one-resource-at-a-time automatic policy/topology
  repair and load balancing are implemented. Transactional reservation across
  several resources or controller instances, whole-fleet evacuation progress,
  explicit hysteresis, split/merge repair, and multi-failure repair remain
  open.
- One exact-source local Kubernetes campaign passes backup compaction, refreshed
  learner snapshot catch-up, automatic policy repair, rollout, restore, and
  digest equality. Kubernetes does not yet attest rack labels from cloud/node
  topology. Protected CI and broader container/network/disk fault injection
  remain required before the beta checklist row is complete.
