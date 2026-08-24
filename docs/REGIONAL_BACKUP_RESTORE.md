# Regional backup and restore

**Status:** alpha-exit implementation and local live-Kubernetes restore pass;
protected exact-commit evidence pending

Epoch regional backups are semantic, portable checkpoints. They are not copies
of a node directory. One artifact carries the captured Catalog image and one
native checkpoint for every tablet named by that exact Catalog image.

## Guarantees

- `POST /v1/admin/backups` requires mTLS at the supported deployment boundary,
  bearer authentication, and the cluster-scoped `backup.create` action.
- Only the Catalog leader coordinates a capture. A request to another node
  returns `409 backup_coordinator_not_leader` without claiming success.
- The coordinator obtains a quorum read barrier for the Catalog, derives the
  exact tablet inventory from that checkpoint, and collects each tablet image
  from its current leader over the existing peer mTLS client. Tablet leaders do
  not need to be co-located with the Catalog leader.
- Every group image contains its group/epoch fence, applied and read indexes,
  committed membership, application format/version/digest, canonical
  consensus checkpoint, and SHA-256 checksum. The manifest is strictly sorted,
  canonically encoded, globally checksummed, and bounded to 128 MiB.
- Restore verifies the complete manifest and Catalog/tablet inventory before
  creating fresh journals. A non-empty regional consensus destination, an
  unknown field, mismatched identity, unfinished joint configuration, checksum
  failure, oversized input, or partial output fails closed.
- Go, Java, and Python profile bytes are not re-encoded during restore. Catalog,
  Stream, Cache, Queue, and Event Bus reopen through their native application
  checkpoint implementations and retain the exact state digest.

`captured_at_ms` is the start of one coordinated capture. Each group also
declares its own quorum-confirmed `read_index` and `applied_index`. Epoch does
not claim a cross-tablet transaction or one global Raft index.

## Managed encrypted format

The `epoch-backup` utility validates the regional manifest before publication
and wraps it in the binary `EPBKAE01` format:

- AES-256-GCM with a fresh 96-bit system-random nonce;
- exactly 32 raw key bytes from a mounted Secret;
- canonical authenticated metadata containing the key ID, creation time,
  plaintext length and SHA-256, and regional manifest SHA-256;
- atomic, no-overwrite publication in the destination directory; and
- authenticated validation of every retained object before an older object is
  deleted.

For key rotation, the active Secret may also contain prior 32-byte keys named
`previous.<key-id>`. Retention reads each artifact's authenticated key ID and
requires the matching active or previous key before it deletes anything. An
unknown key ID, malformed prior key, duplicate material, or tampered object
fails the Job without partially applying retention. Remove a previous key only
after no retained object references it.

The key ID is public metadata. The encryption key never enters the custom
resource, artifact header, process arguments, status, or logs.

## Kubernetes prerequisites

The initial operator backend is a pre-provisioned `ReadWriteMany` PVC. The
application-layer encryption requirement applies even when the storage class
also encrypts volumes.

```bash
kubectl apply -f deploy/kubernetes/operator/sample-backup-pvc.yaml

umask 077
head -c 32 /dev/urandom > epoch-backup.key
kubectl -n epoch-system create secret generic epoch-backup-key \
  --from-file=encryption.key=epoch-backup.key
rm -f epoch-backup.key
```

Configure the immutable destination policy and schedule:

```yaml
spec:
  backup:
    schedule: "*/15 * * * *"
    destinationPVC: epoch-backups
    encryptionSecret: epoch-backup-key
    keyID: backup-key-2026-08
    retentionCount: 7
```

The operator refuses to create workloads until the PVC is `Bound`, advertises
`ReadWriteMany`, and the Secret contains exactly 32 bytes at
`encryption.key`. Optional `previous.<key-id>` fields must also contain exactly
32 unique bytes and cannot reuse the active key ID. It reconciles one
`Forbid`-concurrency CronJob idempotently.
The Job uses the node image's `epoch-backup` binary, the control-plane client
identity, the regional bearer file, a read-only root filesystem, and the
mounted encrypted destination.

## Observe and verify backups

```bash
kubectl -n epoch-system get cronjob epoch-backup
kubectl -n epoch-system get jobs -l platform.epoch.dev/backup-owner=epoch
kubectl -n epoch-system get epochcluster epoch \
  -o jsonpath='{.status.backup}'
```

The successful backup process writes a bounded JSON receipt to the Kubernetes
termination message. The operator copies only validated fields into status:
the exact object name, manifest digest, successful time, key ID-derived
encryption evidence, schedule, and retained count. Failed Jobs publish the
latest bounded failure and set `BackupReady=False`; a later successful Job
returns it to `BackupReady=True` without erasing the earlier Kubernetes Job
evidence.

For an offline integrity check or controlled decryption:

```bash
epoch-backup inspect --input /backups/OBJECT.epoch-backup.enc
epoch-backup decrypt \
  --input /backups/OBJECT.epoch-backup.enc \
  --encryption-key /secure/epoch-backup.key \
  --output /tmp/epoch-regional-backup.json
```

`inspect` validates the bounded envelope structure but cannot authenticate it
without a key. `decrypt` authenticates the entire envelope, validates the
plaintext digest and semantic regional manifest, and refuses to overwrite an
existing output.

## Restore a fresh cluster

Keep an old key while any retained object still uses it. During active rotation,
copy the old material to `previous.<old-key-id>` in the new active Secret. A
restore reference independently names both the object and its encryption
Secret, so a fresh cluster may mount a dedicated old-key Secret.

```yaml
spec:
  backup:
    schedule: "*/15 * * * *"
    destinationPVC: epoch-backups
    encryptionSecret: epoch-backup-key-current
    keyID: backup-key-2026-09
    retentionCount: 7
  restore:
    objectName: 1787520000000-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.epoch-backup.enc
    encryptionSecret: epoch-backup-key-2026-08
```

Use this only when creating a fresh `EpochCluster` with fresh data PVCs. An init
container authenticates and decrypts the artifact into an `EmptyDir`; the node
passes that path to `EPOCH_REGIONAL_RESTORE_PATH`. Epoch restores only the
locally assigned tablet groups, finishes initial Catalog reconciliation, then
fsyncs `.epoch-regional-restore-complete` on that node's data PVC. Restarts skip
the restore input after this marker exists. A failed first attempt removes only
the explicitly scoped partial regional consensus directory before retrying the
same immutable artifact.

The operator records the accepted object and restore-key Secret in status and
rejects adding, removing, or changing either reference after initialization.

## Current boundary

- The managed backend in this slice is an encrypted RWX PVC. S3-compatible,
  Azure Blob, and GCS destination adapters belong to the source/storage adapter
  and release-hardening work that follows.
- This is a complete semantic snapshot, not point-in-time log replay.
- Backup success does not by itself prove a disaster-recovery RPO/RTO. One local
  live Kubernetes replace/upgrade/fresh-restore/digest campaign passes; protected
  exact-commit evidence and measured RPO/RTO remain separate gates.
- Key generation, escrow, rotation approval, and external KMS integration
  remain deployment responsibilities; Epoch never invents a default key.

See [ADR-0039](adr/0039-alpha-exit-beta-readiness.md),
[security](SECURITY.md), [Kubernetes operator](KUBERNETES_OPERATOR.md),
[live Kubernetes campaign](KUBERNETES_ALPHA_EXIT.md), and the
[alpha-exit checklist](ALPHA_EXIT_CHECKLIST.md).
