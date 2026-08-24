# Live Kubernetes alpha-exit campaign

Epoch's alpha-exit Kubernetes campaign is a disposable, fail-closed acceptance
test for the complete managed regional lifecycle. It is release evidence, not a
production benchmark or an availability claim.

## Accepted lifecycle

One run creates a pinned Kind Kubernetes `v1.34.0` cluster with one control-plane
node and four workers, builds the exact local Epoch node, control-plane, and
operator images, and then proves all of the following without mocked Kubernetes
clients:

1. install the CRD, least-privilege RBAC, and two leader-elected operator replicas;
2. create a four-physical-node `EpochCluster` with three Catalog voters and mTLS
   on both data/control and peer transports;
3. provision Stream, Cache, Queue, and Event Bus resources and commit real
   profile traffic;
4. capture an AES-256-GCM encrypted semantic backup and verify the operator's
   durable backup receipt;
5. replace one of three tablet voters with the spare physical node through
   learner admission, refreshed snapshot catch-up after log compaction, joint
   consensus, Catalog finalization, and removed-host shutdown;
6. reject an image rollout until a post-request backup exists, then perform one
   preflight/drain/restart/postflight ordinal at a time;
7. capture the final source state, restore it into a separate fresh
   `EpochCluster`, compare exact Catalog and per-profile state digests, and commit
   new traffic after restore.

The scheduler may choose any canonical three-of-four placement. The campaign
derives the incoming and outgoing voter from observed state; it does not assume
that the first three physical nodes always host a tablet. The same planner is
unit-tested with eight physical nodes and both three- and five-voter groups.

## Run it locally

Prerequisites are Docker, Git, OpenSSL, Kind `v0.32.0`, and kubectl compatible
with Kubernetes `v1.34.0`. Docker must have enough space for three application
images plus the five Kind node containers.

```bash
make test-kubernetes-runner
make test-kubernetes-live
```

For an explicit evidence destination:

```bash
tests/integration/kubernetes_alpha_exit.py \
  --cluster-name epoch-alpha-exit-local \
  --evidence-dir /secure/evidence/epoch-kubernetes-alpha-exit
```

The cluster is deleted on success, assertion failure, command failure, and
interrupt. `--keep-cluster` is only for bounded local diagnosis. `--skip-build`
may accelerate a diagnostic rerun, but final release evidence must build from
the exact candidate tree.

## Evidence contract

The runner writes `evidence.json` last through an atomic rename and creates a
`manifest.sha256` covering every evidence file. The bundle includes:

- Git revision, scoped source-input hash, dirty/drift state, immutable local
  image IDs, Kind node digest, and observed Kubernetes version;
- every accepted lifecycle step and its relevant API/Kubernetes receipt;
- pre/post replacement voters and the resulting profile state digest;
- upgrade Jobs, ordinals, images, and operator status;
- source/restored Catalog and profile digests plus post-restore traffic state;
- diagnostic resources and pod logs when the campaign fails.

Application executable identity is fixed by immutable image IDs. The separate
source hash covers deployment, policy, and campaign inputs so evidence creation
does not depend on unrelated workspace files.

CI runs the same clean-build command in the `Live Kubernetes alpha-exit
lifecycle` job, installs checksum-pinned Kind and kubectl binaries, and retains
the complete bundle as a 30-day workflow artifact. A beta tag must not be cut
until this protected job passes for the exact commit merged to `main`.

## Claim boundary

The upgrade stage intentionally retags the exact same node image ID. It proves
backup gating, orchestration, fencing, persistence, and post-rollout readiness;
it does not claim mixed-version wire or storage compatibility. The campaign also
makes no throughput, latency, production SLO, RPO, RTO, cloud-IAM, multi-region,
or long-duration claim. Those require separate evidence described in
[Testing](TESTING.md), [Soak testing](SOAK_TESTING.md), and the
[delivery checklist](DELIVERY_CHECKLIST.md).
