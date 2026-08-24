# Release artifacts and verification

Epoch's alpha-exit release pipeline publishes a bounded OCI artifact set from
an exact version tag after that tag is proved to equal the current `main`
commit. Pull requests build and inspect the same images but cannot publish.

This document describes the implemented release contract. No artifact is
claimed as published until its tag workflow and independent verification are
green.

## Official image set

| Component | Image | Entrypoint | Runtime identity |
|---|---|---|---|
| Rust data node and maintenance tools | `ghcr.io/ripan-roy/epoch-node:<tag>` | `/usr/local/bin/epoch-node` | non-root `epoch` user |
| Go regional control plane | `ghcr.io/ripan-roy/epoch-control:<tag>` | `/usr/local/bin/epoch-control` | distroless non-root |
| Go Kubernetes operator | `ghcr.io/ripan-roy/epoch-operator:<tag>` | `/usr/local/bin/epoch-operator` | distroless non-root |
| Go management CLI | `ghcr.io/ripan-roy/epoch-cli:<tag>` | `/usr/local/bin/epoch` | distroless non-root |
| Rust Redis/Kafka/AMQP gateway | `ghcr.io/ripan-roy/epoch-compat:<tag>` | `/usr/local/bin/epoch-compat` | non-root `epoch` user |

Each tag is one OCI manifest containing only `linux/amd64` and `linux/arm64`.
Epoch deliberately does not publish a mutable `latest` tag. The OCI labels bind
the image to the synchronized Epoch version, exact Git revision, MIT license,
source repository, documentation, vendor, and component description.

Builder and runtime base images are pinned by multi-architecture manifest
digest. Go binaries are built with `CGO_ENABLED=0`, `-trimpath`, and stripped
linker flags. The Rust runtime contains only its required CA certificates,
health-check client, init process, non-root account, and the node, storage,
backup, and maintenance binaries.

## Publication boundary

`.github/workflows/release-tag.yml` is the only publication path:

1. `v<version>` must match every synchronized package and user-agent version.
2. The tagged commit must equal the current remote `main` commit and the
   version-controlled release-notes file must already exist.
3. The exact commit's `main` CI and Pages push workflows must both complete
   successfully; publication waits for them for a bounded interval and fails
   closed on timeout or a non-success conclusion.
4. BuildKit builds each component separately on matching native amd64 and arm64
   runners. Architecture-scoped caches are written only by this verified tag
   path; QEMU is not in the publication path.
5. Each native job pushes an untagged immutable platform result. A bounded
   finalize job requires exactly two digest artifacts and creates the only
   public exact-version tag; `latest` is never created.
6. The workflow resolves and records the immutable assembled manifest and both
   platform digests, then inspects the published amd64 runtime.
7. Build provenance is attached to the manifest digest. SPDX JSON SBOMs are
   generated and attested separately for the amd64 and arm64 digests.
8. Sigstore keyless signing binds the manifest digest to the exact tag workflow
   identity and GitHub Actions OIDC issuer.
9. GitHub verifies the registry attestations, and the release retains the ten
   platform-specific SBOM files as downloadable assets.

The workflow is tag-only, has no `workflow_dispatch` publication entry point,
and uses narrowly scoped job permissions. The Pages workflow still deploys
only from `main`; pull requests may build its artifact but cannot deploy it.

## Verify before use

Choose a concrete tag and resolve the manifest digest. Do not copy a digest
from an untrusted issue, log, or chat message.

```bash
export EPOCH_RELEASE_TAG=v0.2.0-beta.4
export EPOCH_COMPONENT=node
export EPOCH_IMAGE="ghcr.io/ripan-roy/epoch-${EPOCH_COMPONENT}"

docker buildx imagetools inspect "${EPOCH_IMAGE}:${EPOCH_RELEASE_TAG}"
export EPOCH_IMAGE_DIGEST="sha256:replace-with-the-inspected-manifest-digest"
```

Verify the keyless signature against both the repository workflow identity and
GitHub Actions issuer:

```bash
cosign verify \
  --certificate-identity \
  "https://github.com/Ripan-Roy/epoch/.github/workflows/release-tag.yml@refs/tags/${EPOCH_RELEASE_TAG}" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "${EPOCH_IMAGE}@${EPOCH_IMAGE_DIGEST}"
```

Verify GitHub's provenance/SBOM attestation envelope and the owning repository:

```bash
gh attestation verify \
  "oci://${EPOCH_IMAGE}@${EPOCH_IMAGE_DIGEST}" \
  --repo Ripan-Roy/epoch
```

Inspect and run by immutable digest after verification:

```bash
docker pull "${EPOCH_IMAGE}@${EPOCH_IMAGE_DIGEST}"
docker image inspect "${EPOCH_IMAGE}@${EPOCH_IMAGE_DIGEST}" \
  --format '{{.Config.User}} {{index .Config.Labels "org.opencontainers.image.version"}} {{index .Config.Labels "org.opencontainers.image.revision"}}'
```

The CLI image can run without a writable home directory. Supply credentials and
CA/client-certificate files at runtime; never bake them into a derivative image
or environment layer. Resolve and verify the CLI manifest independently, then
set its digest:

```bash
export EPOCH_CLI_DIGEST="sha256:replace-with-the-verified-cli-manifest-digest"
docker run --rm \
  --mount type=bind,src=/secure/epoch-client,dst=/tls,readonly \
  "ghcr.io/ripan-roy/epoch-cli@${EPOCH_CLI_DIGEST}" \
  --help
```

Repeat digest and attestation verification for every component deployed. A
signature on the node image does not authenticate the control, operator, or CLI
image, and a manifest signature does not replace platform-specific SBOM review.

## Pull-request and local evidence

The container CI job builds the five images without `push`, validates Linux
architecture, explicit non-root identity, entrypoint, credential-free runtime
environment, and required OCI metadata, then generates and structurally checks
an SPDX JSON SBOM for each image. It uploads those SBOMs as short-lived CI
evidence.

The separate `Live Kubernetes alpha-exit lifecycle` job builds the exact local
node, control, and operator candidates, runs the complete four-node managed
lifecycle, and uploads its SHA-256-bound bundle for 30 days. See
[Live Kubernetes alpha-exit campaign](KUBERNETES_ALPHA_EXIT.md). This is
same-binary rollout evidence; it does not replace post-publication digest,
signature, attestation, SBOM, or mixed-version verification.

To reproduce the metadata inspection locally:

```bash
version="$(cat VERSION)"
revision="$(git rev-parse HEAD)"

for component in node control operator cli compat; do
  docker build \
    --file "deploy/docker/Dockerfile.${component}" \
    --build-arg "EPOCH_VERSION=${version}" \
    --build-arg "VCS_REF=${revision}" \
    --tag "epoch/${component}:verify" .
done

./scripts/inspect-oci-image.sh epoch/node:verify "$version" "$revision" /usr/local/bin/epoch-node "Epoch node"
./scripts/inspect-oci-image.sh epoch/control:verify "$version" "$revision" /usr/local/bin/epoch-control "Epoch control plane"
./scripts/inspect-oci-image.sh epoch/operator:verify "$version" "$revision" /usr/local/bin/epoch-operator "Epoch operator"
./scripts/inspect-oci-image.sh epoch/cli:verify "$version" "$revision" /usr/local/bin/epoch "Epoch CLI"
./scripts/inspect-oci-image.sh epoch/compat:verify "$version" "$revision" /usr/local/bin/epoch-compat "Epoch compatibility gateway"
```

Local inspection proves the candidate Dockerfiles and metadata. Only the
protected pull-request job proves the clean GitHub runner path, and only the
exact-main tag job proves GHCR publication, signatures, and registry
attestations.

## Explicit non-claims

- No `latest`, rolling-channel, or floating major/minor tags are published.
- No Windows image, macOS image, or non-Linux architecture is published.
- Debian/RPM, standalone binary archives, package-manager SDKs, Helm charts,
  vulnerability-free claims, and bit-for-bit reproducibility are not included
  in this gate.
- Signing authenticates source workflow identity and digest; it does not by
  itself prove operational suitability, compatibility, or a production SLO.
