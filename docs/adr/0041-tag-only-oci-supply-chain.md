# ADR-0041: Tag-only OCI supply chain

- Status: Accepted
- Date: 2026-08-24
- Owners: release engineering, data plane, control plane, operator

## Context

Epoch's source prereleases did not publish an installable, authenticated runtime
artifact. The alpha-exit boundary requires node, control, operator, and CLI
images without allowing a pull-request workflow, stale tag, mutable tag, or
untrusted registry digest to become an accidental release.

A multi-architecture image also has two distinct integrity levels: the top-level
manifest selected by an operator and each platform image that contains the
actual packages. One generic SBOM that is not bound to a platform digest is
insufficient evidence.

## Decision

1. Only a pushed `v*` tag can enter the publication workflow. Its synchronized
   version and checked-in notes must pass, its commit must equal the current
   remote `main`, and the exact commit's main CI and Pages workflows must both
   succeed before publication proceeds.
2. Epoch publishes exactly four GHCR repositories: `epoch-node`,
   `epoch-control`, `epoch-operator`, and `epoch-cli`. Each release publishes
   one exact version tag containing Linux amd64 and arm64. No `latest` tag is
   created.
3. Every Dockerfile pins builder and runtime bases by manifest digest, produces
   an explicit non-root runtime, and records exact version/revision/source/
   license/documentation OCI metadata. Secrets never enter image arguments,
   labels, or default environment variables.
4. Pull requests perform the same four builds and strict runtime inspection but
   never log in or push. They generate short-lived SPDX JSON evidence for every
   candidate image.
5. The tag workflow distributes every component/architecture pair to a matching
   native runner, publishes untagged immutable platform results, and requires
   exactly one amd64 and one arm64 digest before assembling the public tag. It
   attaches BuildKit provenance and a GitHub build-provenance attestation to
   the manifest, generates and attests a separate SPDX JSON SBOM for each
   platform digest, and retains all eight SBOMs with the GitHub release.
6. Each manifest digest is signed keylessly with Sigstore. Verification pins
   the exact repository workflow identity at the version tag and the GitHub
   Actions OIDC issuer. GitHub registry attestations are also verified before
   the release job can run.
7. Deployment documentation resolves and verifies immutable digests. Exact tags
   are discovery handles, not a substitute for digest pinning.

## Consequences

Release publication is intentionally unavailable from manual dispatch and pull
requests. A tag behind or ahead of `main` fails, so release preparation must be
merged before tagging. Each component and platform has independently reviewable
package evidence.

Native amd64 and arm64 builds run concurrently and use component/architecture
scoped GitHub Actions caches written only by the verified tag path. Immutable
digest artifacts are the sole cross-job handoff, and manifest assembly fails
unless both target architectures are present exactly once. Base-image digest
pinning improves input stability but does not claim bit-for-bit
reproducibility: language registries and compiler output still require future
hermetic-build evidence.

This decision does not publish package-manager SDKs, OS packages, Helm charts,
or standalone binary archives. It also does not claim that an authenticated
artifact has passed load, live-Kubernetes, security-review, or production-SLO
gates; those remain independent beta evidence.
