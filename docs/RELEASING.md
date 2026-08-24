# Releasing Epoch

Epoch publishes a GitHub prerelease after a meaningful milestone has merged and
the protected `main` checks and documentation deployment are green. A release
never broadens the guarantees proved by its tests.

## Current release class

Published alpha tags through `v0.1.0-alpha.10` are source previews. The
`v0.2.0-beta.3` workflow candidate adds four official OCI images: node, control
plane, operator, and management CLI. A release is not
described as an OCI release until the exact tag workflow has published and
verified those immutable manifests.

Package-manager distributions, operating-system installers, Cargo crates, Go
modules, Maven artifacts, and Python/npm packages remain deferred. Repository
source and released images are MIT licensed; a registry artifact is supported
only when it is named in that tag's version-controlled release notes.

See [Release artifacts](RELEASE_ARTIFACTS.md) for image names, architecture and
tag policy, SBOM/signature scope, verification commands, and clean pull/run
checks.

## Protected release procedure

1. Create a release branch from the current `main`.
2. Update `VERSION`, every language/package version, user-agent versions, the
   changelog, and `docs/releases/<tag>.md`.
3. Run `make release-check`, `make check`, `make build`, and the integration
   gates appropriate to the milestone. Alpha-exit/beta additionally requires
   `make test-kubernetes-live` from the frozen candidate tree.
4. Merge the version bump through a protected pull request.
5. Verify the resulting `main` CI run—including the uploaded live Kubernetes
   lifecycle evidence—and the main-only Pages deployment.
6. Create and push an annotated `v<version>` tag at the exact current `main`
   commit.
7. Wait for **Release tag verification** to prove the tag/version match and
   main provenance.
8. Let the tag workflow publish all four exact-version OCI manifests, attach
   provenance and per-platform SBOM attestations, keylessly sign each manifest,
   and create the GitHub prerelease from the version-controlled notes.
9. Independently verify each immutable digest, signature identity,
   attestations, downloadable SBOM, non-root runtime, target commit, notes,
   source archives, and public documentation link.

`scripts/check-release-version.sh` is the executable source of truth for the
cross-language metadata invariant. Passing an expected tag also verifies that
the tag exactly matches `VERSION`.

Release notes must name:

- implemented behavior and its verification evidence;
- user-visible changes;
- compatibility or migration impact;
- unsupported behavior and guarantee ceilings;
- exactly which artifacts were published;
- links to CI, documentation, traceability, and the compared source range.

Only `main` may be tagged. A feature branch, unverified commit, or tag with
stale package metadata is not releasable.

The workflow never publishes `latest`. Operators promote an exact tag or,
preferably, the verified manifest digest. Pull requests build and inspect the
same four Dockerfiles and retain SPDX evidence without authenticating to or
writing to a registry.
