# ADR-0038: Product runtime closure

- Status: Accepted for `v0.1.0-alpha.10`
- Date: 2026-08-23

## Context

The four profile cores and Event integration platform can run behind the
regional API, but a user still needs a repeatable existing-infrastructure
installation, a typed management command, and automatic source ingestion. A
manifest renderer alone would not satisfy the Kubernetes operator boundary,
and source polling without consensus-bound identities could skip or duplicate
events across leader loss.

## Decision

Epoch adds three product/runtime boundaries in one feature release.

1. A Go controller-runtime operator owns a namespaced `EpochCluster` CRD. It
   validates the exact fixed-voter topology, fails closed until referenced
   policy/credential objects exist, reconciles StatefulSets and Services with
   owner references, requires voter anti-affinity, persists data/control state,
   hardens pod security, carries explicit region/node-class placement identity,
   elects one controller, avoids writes for API-default-only differences,
   repairs owned drift, and publishes observed status.
2. A Go `epoch` CLI consumes the generated RegionalAdmin API. It accepts strict
   protobuf JSON/YAML, preserves fully qualified identities and 64-bit
   generations, generates retry tokens, supports OCC delete, and checks HTTP
   health plus authenticated gRPC in `doctor`.
3. A Rust source worker supports HTTP and CloudEvents source connectors. Only
   the current Bus leader polls through the shared safe HTTP/secret boundary.
   Each record receives a deterministic proposal identity. The batch checkpoint
   commits only after every record has a committed applied or error-routed
   outcome.

Kubernetes manifests and separate node, control, and operator Dockerfiles form
the source-built installation package. The CI gate renders/dry-runs the
Kustomize tree and builds every image.

## Consequences

- A source crash between record commit and checkpoint is replayable without a
  duplicate Bus append for the same connector/batch/index identity.
- Cursor gaps fail closed; one broken connector does not prevent later
  connectors on the same Bus tablet from being examined.
- Kubernetes requires three schedulable nodes for this topology. The operator
  refuses five replicas rather than implying unsupported dynamic membership.
- Secrets stay outside CRs and pod specs, but the alpha transport remains
  plaintext; TLS/mTLS/OIDC is a beta gate.
- Image changes are declarative but not yet a guarded upgrade workflow.
- Built-in generic HTTP/CloudEvents source ingestion is complete. CDC, Kafka,
  object-store adapters, private egress, and connector certification remain
  separate breadth/hardening work.

## Verification

- Rust focused parser/network/status tests plus the real three-process
  failover, convergence, stop/restart, and reopen campaign.
- Go race tests for CLI and fake-Kubernetes reconciliation.
- `make kubernetes-config`, all three Docker builds, full `make check`,
  `make build`, and `make test-integration`.
