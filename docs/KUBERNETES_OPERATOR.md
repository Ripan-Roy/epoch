# Kubernetes operator

Epoch ships a controller-runtime operator for the
`platform.epoch.dev/v1alpha1` `EpochCluster` custom resource. One resource
reconciles a regional Rust data plane, a durable single-owner Go control plane,
peer/public/control Services, per-node PVCs, required workload identities, and
scheduled application-layer encrypted semantic backups.

## Topology contract

- `spec.replicas` is physical data-node capacity and accepts 3–1,024 nodes.
- `spec.catalogReplicas` is three or five and cannot exceed physical capacity.
  Each profile tablet independently records an explicit three- or five-voter
  placement; a physical node hosts only the groups assigned to it.
- Required pod anti-affinity places physical nodes on distinct Kubernetes
  nodes by default. Stable StatefulSet ordinals become consensus node IDs
  starting at one.
- `spec.region` and `spec.nodeClass` define placement identity; the scheduled
  Kubernetes node name is the observed zone in this alpha-exit deployment.
- Every data node owns a `ReadWriteOnce` PVC. The Go control owner has a
  separate durable bbolt PVC. Semantic backups use a pre-provisioned
  `ReadWriteMany` PVC so any scheduled Job and every restore init container can
  access the same encrypted objects.
- Two operator replicas use Kubernetes Lease leader election. Reconciliation
  is idempotent, treats API-server defaults as no-ops, and repairs drift in
  operator-owned objects.

Rust now executes an explicitly planned single-voter replacement through
learner catch-up and joint consensus while the operator keeps every physical
ordinal provisioned. Rack-aware placement, automatic multi-tablet rebalance,
and general repair remain beta gates. One local live Kubernetes campaign passes
backup-before-replacement, refreshed snapshot catch-up after compaction, joint
consensus, guarded rollout, fresh restore, exact digests, and continued writes.
Protected evidence and a genuinely mixed-version campaign remain beta gates.

## Required trust material

The supported Kubernetes deployment has no plaintext fallback. The operator
will not create a workload until all referenced objects are valid:

| Reference | Required data | Purpose |
|---|---|---|
| `authPolicyConfigMap` | `bootstrap-policy.json` | Strict bearer authorization policy |
| `credentialSecret` | `regional-token` | Go control and backup authorization |
| `transportSecurity.dataPlaneSecret` | `ca.crt`, `tls.crt`, `tls.key` | Rust HTTPS server and peer mTLS identity |
| `transportSecurity.controlPlaneSecret` | `ca.crt`, `tls.crt`, `tls.key` | Go HTTPS/gRPC server and Rust client identity |
| `backup.encryptionSecret` | 32 raw bytes at `encryption.key`; optional 32-byte `previous.<key-id>` entries | AES-256-GCM active/retention keyring |

Issue both workload certificates from the same regional trust domain with
`serverAuth` and `clientAuth` extended key usages. The data-plane certificate
must cover the public Service, peer Service, and every ordinal peer DNS name.
For the checked-in sample that includes at least:

```text
epoch.epoch-system.svc
epoch-peer.epoch-system.svc
epoch-node-0.epoch-peer.epoch-system.svc
epoch-node-1.epoch-peer.epoch-system.svc
epoch-node-2.epoch-peer.epoch-system.svc
```

The control certificate must cover `epoch-control.epoch-system.svc`. Set
`transportSecurity.regionalServerName` to a DNS SAN present in the data-plane
certificate. Epoch verifies the requested server name and trust chain; do not
disable hostname verification. Certificate issuance and rotation belong to
cert-manager, a private CA, or the deployment's secret manager—private keys are
never checked into this repository.

Create the references after issuance:

```bash
kubectl create namespace epoch-system

kubectl -n epoch-system create configmap epoch-auth-policy \
  --from-file=bootstrap-policy.json=spec/auth/bootstrap-policy-v1.example.json

kubectl -n epoch-system create secret generic epoch-control-credentials \
  --from-literal=regional-token=epoch-dev-control-v1

kubectl -n epoch-system create secret generic epoch-data-plane-tls \
  --from-file=ca.crt=/secure/epoch-ca.crt \
  --from-file=tls.crt=/secure/epoch-data-plane.crt \
  --from-file=tls.key=/secure/epoch-data-plane.key

kubectl -n epoch-system create secret generic epoch-control-plane-tls \
  --from-file=ca.crt=/secure/epoch-ca.crt \
  --from-file=tls.crt=/secure/epoch-control-plane.crt \
  --from-file=tls.key=/secure/epoch-control-plane.key

umask 077
head -c 32 /dev/urandom > epoch-backup.key
kubectl -n epoch-system create secret generic epoch-backup-key \
  --from-file=encryption.key=epoch-backup.key
rm -f epoch-backup.key

kubectl apply -f deploy/kubernetes/operator/sample-backup-pvc.yaml
```

The checked-in policy and bearer value are public development fixtures. Use a
new token and store only its SHA-256 fingerprint in a private policy outside a
disposable evaluation environment.

## Build and install

Until the tag-only GHCR publication gate is complete, build and push the three
cluster images to a registry your cluster can pull. Build the fourth CLI image
when administrators will run it from a container:

```bash
docker build -f deploy/docker/Dockerfile.node \
  -t registry.example/epoch-node:alpha-exit .
docker build -f deploy/docker/Dockerfile.control \
  -t registry.example/epoch-control:alpha-exit .
docker build -f deploy/docker/Dockerfile.operator \
  -t registry.example/epoch-operator:alpha-exit .
docker build -f deploy/docker/Dockerfile.cli \
  -t registry.example/epoch-cli:alpha-exit .
docker push registry.example/epoch-node:alpha-exit
docker push registry.example/epoch-control:alpha-exit
docker push registry.example/epoch-operator:alpha-exit
docker push registry.example/epoch-cli:alpha-exit
```

After a verified release, resolve and verify each official exact tag, then pin
the resulting immutable digest in the deployment. Epoch does not publish
`latest`. [Release artifacts](RELEASE_ARTIFACTS.md) contains the exact GHCR
names, Cosign identity, GitHub attestation command, SBOM scope, and clean-pull
procedure.

Update the image references in `deployment.yaml` and `sample-cluster.yaml`, or
use Kustomize image overrides. Install the controller and create the cluster:

```bash
kubectl apply -k deploy/kubernetes/operator
kubectl apply -f deploy/kubernetes/operator/sample-cluster.yaml
```

Containers run as non-root, drop all Linux capabilities, deny privilege
escalation, and use read-only root filesystems. Policy, credentials, TLS, and
backup keys are mounted read-only and are not copied into the custom resource
or status.

## Observe the cluster, backups, and upgrades

```bash
kubectl -n epoch-system get epochclusters.platform.epoch.dev -w
kubectl -n epoch-system describe epochcluster epoch
kubectl -n epoch-system get statefulsets,pods,pvc,services
kubectl -n epoch-system get cronjob epoch-backup
kubectl -n epoch-system get jobs \
  -l platform.epoch.dev/backup-owner=epoch
kubectl -n epoch-system get epochcluster epoch \
  -o jsonpath='{.status.backup}'
kubectl -n epoch-system get epochcluster epoch \
  -o jsonpath='{.status.upgrade}'
kubectl -n epoch-system get jobs \
  -l platform.epoch.dev/upgrade-owner=epoch
```

Status distinguishes desired reconciliation from evidence:

- `Available` reports configuration or workload readiness;
- `Progressing` reports ready data nodes and the control owner;
- `BackupReady` reports whether the newest observed scheduled outcome is a
  success or failure; and
- `UpgradeReady` is true only while the recorded data-node image is stable;
- `.status.upgrade` is the durable image/phase/ordinal/deadline/rollback plan;
  and
- `.status.backup` records the schedule, exact encrypted object, manifest
  digest, key ID, retained count, completion time, and bounded latest failure.

The operator does not infer Raft health merely from desired pod counts. Live
tablet membership and leaders remain authoritative in Rust topology.

## Backup and restore

The operator creates one `ForbidConcurrent` CronJob. Its `epoch-backup` process
tries every physical node until the Catalog leader coordinates a distributed
quorum-barrier snapshot, encrypts it with AES-256-GCM, atomically publishes it
to the RWX PVC, applies retention, and returns a bounded termination receipt.
See [Regional backup and restore](REGIONAL_BACKUP_RESTORE.md) for the artifact
contract and offline inspection commands.

Restore is creation-time only. Create fresh data PVCs and add the immutable
artifact plus key reference before the first reconciliation:

```yaml
spec:
  restore:
    objectName: 1787520000000-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.epoch-backup.enc
    encryptionSecret: epoch-backup-key-2026-08
```

An init container authenticates and decrypts the object into an `EmptyDir`.
Each node restores only its assigned consensus groups, then durably writes a
completion marker after the regional runtime starts. The operator rejects
adding, removing, or changing the restore object or key Secret after cluster
initialization.

## Guarded rolling upgrade

Changing `spec.nodeImage` first freezes the StatefulSet at a partition equal
to `spec.replicas`. The operator requires a successful encrypted backup
captured after the request began, then runs cluster-wide Rust preflight,
term-fenced leader drain, exactly one ordinal update, exact-image readiness,
and postflight verification. Only then does it release the next lower ordinal.

```yaml
spec:
  nodeImage: registry.example/epoch-node:v0.2.0-beta.2
  upgrade:
    backupMaxAgeSeconds: 3600
    stepDeadlineSeconds: 900
    rollbackOnFailure: true
```

Failures stop forward progress and, by default, roll changed ordinals back one
at a time to `.status.upgrade.currentNodeImage`. The plan survives operator
restart in custom-resource status. See
[Guarded data-plane upgrades](GUARDED_UPGRADES.md) for every phase, receipt,
failure condition, retry rule, and current evidence boundary.

## Validate before installation

```bash
make kubernetes-config
go test -race ./operator/...
go vet ./operator/...
go run ./operator/cmd/epoch-manifest-check \
  < deploy/kubernetes/operator/sample-cluster.yaml
```

Controller tests cover fail-closed references, TLS mounts and secure endpoints,
N-node rendering, three/five-member placement inputs, idempotent reconciliation,
PVC/access-mode requirements, scheduled backup status recovery, restore
initialization, immutable restore references, the fresh-backup gate, explicit
partition freeze, preflight/drain/update/postflight ordering, rollback entry,
strict maintenance receipts, security contexts, Services, and status.

The clean live acceptance campaign is documented in
[Live Kubernetes alpha-exit campaign](KUBERNETES_ALPHA_EXIT.md). It exercises
the real controller and workloads across four physical nodes rather than fake
clients, and retains machine-verifiable lifecycle evidence in CI.

## Current limits

- The backup backend is an application-encrypted RWX PVC. Cloud-object
  destination adapters and KMS integration are not yet claimed.
- The guarded upgrade state machine is locally tested, but a real
  mixed-version live-Kubernetes upgrade/rollback and capability-compatibility
  window remain beta gates.
- Learner-first single-voter replacement, joint consensus, old-voter removal,
  and compacted-log snapshot catch-up pass locally. General automatic rebalance,
  rack-aware repair policy, and multi-failure repair remain open.
- Certificate issuance/renewal is external. Epoch fails closed on missing or
  malformed files, but the live certificate-rotation campaign remains open.
- StatefulSet PVCs are deliberately retained after cluster deletion. Delete
  them only after verifying a recoverable semantic backup.
- The live-Kubernetes write/backup/replace/upgrade/restore/digest campaign
  passes locally. The exact protected pull-request commit and exact-main rerun
  are still required before the beta release gate can close.
