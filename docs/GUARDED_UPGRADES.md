# Guarded data-plane upgrades

Changing `spec.nodeImage` creates a durable upgrade plan. It does not authorize
Kubernetes to roll every pod. The operator owns an explicit StatefulSet
`RollingUpdate.partition`, persists every transition in
`.status.upgrade`, and releases one ordinal only after Rust supplies the
corresponding consensus evidence.

The same-binary retagged rollout passes in one local live Kubernetes lifecycle,
including its post-request backup gate and all four ordinals. A genuinely
mixed-version campaign and protected CI evidence are still required before the
broader compatibility gate is complete.

## Configure the guardrails

```yaml
spec:
  upgrade:
    backupMaxAgeSeconds: 3600
    stepDeadlineSeconds: 900
    rollbackOnFailure: true
    # Change only to explicitly retry the same failed target image.
    retryToken: attempt-2
```

Omitted values default to a one-hour maximum backup age, a 15-minute deadline
per active step, and automatic rollback. A retry token is part of the upgrade
request identity; changing it starts a new plan without weakening image or
generation fencing.

## Forward state machine

| Phase | Required evidence | StatefulSet permission |
|---|---|---|
| `WaitingForBackup` | Every physical node is ready and an encrypted backup with the active key completed after this request started | Partition equals replica count; target image is not rendered |
| `Preflight` | Cluster-wide, mTLS-authenticated inventory has one leader per group, stable three/five-voter membership, caught-up voters, no pending snapshots, and no fail-stopped group | Current target ordinal remains blocked |
| `Draining` | Every group led by the target transfers to a caught-up, recently active voter using group-epoch and term fencing | Current target ordinal remains blocked |
| `Updating` | Drain receipt succeeded | Partition releases exactly the target ordinal |
| `Verifying` | Updated pod runs the exact target image and is Ready; a fresh cluster-wide invariant receipt succeeds | No lower ordinal is released |
| `Stable` | Every ordinal passed the same sequence | Partition returns to zero and the target becomes the recorded current image |

Ordinals progress from highest to lowest. Previously verified target pods form
one suffix, which is exactly the boundary a StatefulSet partition can express.
The controller stores the current image, target image, request ID, phase,
ordinal, timestamps, mutation marker, count of verified nodes, and bounded
failure message in custom-resource status, so an operator restart resumes the
same transition.

## Rust maintenance boundary

The node image includes `epoch-maintenance`. Operator Jobs call these commands
through every ordinal's internal peer listener:

```text
GET  /internal/v1/maintenance/groups
POST /internal/v1/maintenance/groups/{group_id}/leadership
```

The listener requires mutual TLS and is not mounted on the public router. The
inventory is strictly decoded and bounded. Leadership requests include
`group_epoch`, `expected_term`, and `target_node_id`; stale epochs or terms fail
with conflict and cannot transfer a newer leader accidentally.

`epoch-maintenance verify` checks every observed group, including groups not
hosted by the node being upgraded. It requires:

- a canonical unique inventory from every physical node;
- stable, identical three- or five-voter membership without a joint change or
  learner in progress;
- exactly one leader and one term/leader view across its voters;
- every voter applied through the leader commit, with a valid checkpoint
  boundary and no fail-stop;
- complete leader replication progress, no pending snapshot, and recent
  activity for every non-leader voter.

`epoch-maintenance drain` repeats the same checks, selects the most caught-up
non-target voter (lowest node ID breaks a tie), submits term-fenced transfers,
and succeeds only after the target leads no group. Both commands return a
strict termination receipt. The operator rejects a successful Job whose
receipt does not name the exact operation, target node, and complete canonical
physical-node inventory.

## Failure and rollback

A failed Job, invalid receipt, invariant breach, unexpected pod image, or step
deadline stops forward progress. With automatic rollback enabled, the operator
switches the template back to the recorded stable image and processes changed
ordinals one at a time. A ready target is verified and drained before rollback;
an unready target is allowed to restart directly because it cannot serve as a
healthy leader. Every restored pod must become Ready and pass post-rollback
cluster verification before the next ordinal is released.

If rollback itself cannot prove safety, phase becomes `Failed` and the
partition stays at the narrowest known boundary. The operator never reports a
failed target as the current image. Correct the cause, then change
`spec.upgrade.retryToken` to retry the same target. If automatic rollback was
disabled, reverting `spec.nodeImage` to the recorded current image still starts
the guarded rollback state machine.

## Observe and diagnose

```bash
kubectl -n epoch-system get epochcluster epoch \
  -o jsonpath='{.status.upgrade}'

kubectl -n epoch-system get jobs \
  -l platform.epoch.dev/upgrade-owner=epoch

kubectl -n epoch-system get statefulset epoch-node \
  -o jsonpath='{.spec.updateStrategy.rollingUpdate.partition}'
```

The `UpgradeReady` condition is true only in `Stable`. `Progressing` remains
true during an upgrade even if all pods are temporarily Ready. Maintenance
Jobs are immutable, request- and stage-labeled, run as non-root with a
read-only filesystem, mount only the control-plane TLS identity, use no proxy
or redirects, and expire after completion.

## Current release boundary

The local suite proves the fresh-backup gate, partition freeze, preflight,
drain, one-ordinal release, exact-image readiness, postflight gate, failure
stop, guarded rollback entry, strict receipts, learner catch-up rejection, and
epoch/term fencing. The following remain beta acceptance evidence rather than
implemented claims:

- a real mixed-version Kubernetes upgrade and rollback;
- version/capability negotiation across adjacent released binaries;
- load/SLO stop signals beyond the consensus invariants;
- policy-driven automatic multi-tablet rebalance (explicit learner-first
  single-voter replacement is implemented separately); and
- exact-main CI, published OCI provenance, and release notes.
