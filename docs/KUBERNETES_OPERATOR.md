# Kubernetes operator

Epoch ships a real controller-runtime operator for the
`platform.epoch.dev/v1alpha1` `EpochCluster` custom resource. One reconciliation
creates a fixed three-voter Rust data plane, a durable single-owner Go control
plane, peer/public/control Services, PVCs, probes, owner references, and
observed status conditions.

## Current topology contract

- Exactly three data replicas. Other replica counts fail admission and
  reconciliation because the current consensus membership is fixed.
- Required pod anti-affinity places each voter on a distinct Kubernetes node.
- Stable StatefulSet ordinals become consensus node IDs 1, 2, and 3.
- `spec.region` and `spec.nodeClass` become the voter placement identity;
  omitted values default to `local` and `general-purpose`. The voter zone is
  the scheduled Kubernetes node name.
- Peer DNS names and the complete voter list are deterministic.
- Every voter owns a `ReadWriteOnce` PVC; the Go control owner has a separate
  PVC for bbolt metadata.
- The bootstrap policy is mounted from a named ConfigMap. The regional bearer
  is read from a named Secret and is never copied into the custom resource or
  status.
- Containers run non-root, drop every Linux capability, deny privilege
  escalation, and use read-only root filesystems.
- Two operator replicas use Kubernetes Lease leader election so one controller
  mutates desired state at a time.
- Reconciliation treats API-server defaulted fields as a no-op, avoiding
  periodic update churn, while repairing drift in operator-owned fields.

The voter `zone` observation uses the distinct Kubernetes node name
as its scheduling failure-domain identity. Cloud zone-label discovery, rack
constraints, dynamic membership, automatic repair/rebalance, and online shard
transfer are not claimed.

## Build the images

The alpha.10 release is source-first. Build and push images to a registry your
cluster can pull:

```bash
docker build -f deploy/docker/Dockerfile.node \
  -t registry.example/epoch-node:alpha.10 .
docker build -f deploy/docker/Dockerfile.control \
  -t registry.example/epoch-control:alpha.10 .
docker build -f deploy/docker/Dockerfile.operator \
  -t registry.example/epoch-operator:alpha.10 .
docker push registry.example/epoch-node:alpha.10
docker push registry.example/epoch-control:alpha.10
docker push registry.example/epoch-operator:alpha.10
```

Change the operator image in
`deploy/kubernetes/operator/deployment.yaml` and the node/control images in
`sample-cluster.yaml`, or use a Kustomize image override.

## Install

```bash
kubectl apply -k deploy/kubernetes/operator

kubectl -n epoch-system create configmap epoch-auth-policy \
  --from-file=bootstrap-policy.json=spec/auth/bootstrap-policy-v1.example.json

kubectl -n epoch-system create secret generic epoch-control-credentials \
  --from-literal=regional-token="$EPOCH_CONTROL_REGIONAL_TOKEN"

kubectl apply -f deploy/kubernetes/operator/sample-cluster.yaml
```

The checked-in bootstrap policy and token names are public development
fixtures. For a real evaluation, generate a separate token, store only its
fingerprint in a private policy, and deliver the token through your secret
manager.

Observe reconciliation:

```bash
kubectl -n epoch-system get epochclusters.platform.epoch.dev -w
kubectl -n epoch-system describe epochcluster epoch
kubectl -n epoch-system get statefulsets,pods,pvc,services
```

The controller publishes `Available` and `Progressing`, observed generation,
ready voter count, control readiness, and the internal control endpoint. It
does not infer data-plane health from desired replica counts.

## Validate before installation

```bash
make kubernetes-config
go test -race ./operator/...
```

The first command renders the Kustomize tree and asks `kubectl` to perform a
client-side dry run. Controller tests use a fake Kubernetes API to prove
fail-closed configuration references, idempotent reconciliation, exact peer
wiring, PVC requests, anti-affinity, security contexts, Services, and status.

## Lifecycle limits

Changing images updates StatefulSet pod templates, but this alpha does not yet
claim a guarded one-voter-at-a-time upgrade, mixed-version compatibility, or
automatic rollback. Do not perform an unattended production upgrade.

Deleting an `EpochCluster` deletes controller-owned StatefulSets and Services.
StatefulSet PVCs are deliberately not garbage-collected automatically; inspect
and delete them explicitly only after a verified backup. Scheduled backup,
semantic PITR, automated restore validation, certificate issuance/rotation,
and dynamic membership remain beta-hardening work.
