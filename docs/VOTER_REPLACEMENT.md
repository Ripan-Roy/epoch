# Learner-first voter replacement

Epoch can replace one voter in an existing three- or five-voter tablet without
changing the tablet, consensus-group, tablet-epoch, or customer resource
generation. The physical regional inventory may contain 3–1,024 nodes; only
the explicit voter set for one tablet changes.

This is an operational repair primitive, not an automatic balancing solver.
Rust owns the Catalog plan, Raft transition, durable membership, and tablet
materialization. Go validates and reports the resulting policy-compliant
placement without rewriting Raft storage.

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

## Plan API

The provisional regional administration route is:

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
declared by the tablet, remain inside the immutable physical-node directory,
and differ from the current set by exactly one removal and one addition. The
request token is replay-safe and cannot be rebound. Stale generation/epoch,
conflicting active plans, multi-voter replacement, zero/duplicate IDs, and
direct placement mutation are rejected before Catalog state changes.

The accepted response is `202 Accepted`. Its resource contains:

- `voter_node_ids`: the currently assigned Catalog voters;
- `bootstrap_voter_node_ids`: immutable startup identity for journal reopen;
- `target_voter_node_ids`: the active transition target, or an empty array
  after finalization.

Planning and finalization do not increment the customer resource generation.
That generation fences application-spec changes and SDK routing; operational
membership is separately fenced by the exact generation, tablet epoch,
committed plan, and durable Raft configuration.

## Observe the transition

The Go browser-safe inventory is:

```shell
curl --fail-with-body \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  https://epoch-control.example/v1/regional/resources
```

An active replacement reports resource phase `pending` and separates:

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

## Current limits

- A plan replaces exactly one voter; it never changes three voters directly.
- Only explicit three- and five-voter groups are supported.
- Rack-aware selection, automatic multi-tablet balancing, concurrent
  reservation across several plans, split/merge repair, and evacuation of an
  entire physical node remain open.
- One exact-source local Kubernetes campaign passes backup compaction, refreshed
  learner snapshot catch-up, replacement, rollout, restore, and digest equality.
  Protected CI and broader container/network/disk fault injection remain
  required before the alpha-exit checklist row is complete.
