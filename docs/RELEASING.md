# Releasing Epoch

Epoch publishes a GitHub prerelease after a meaningful milestone has merged and
the protected `main` checks and documentation deployment are green. A release
never broadens the guarantees proved by its tests.

## Current release class

Alpha releases are source previews. GitHub supplies source archives for the
tagged tree, but Epoch does not yet publish binaries, container images,
installers, Cargo crates, Go modules, Maven artifacts, Python distributions, or
npm packages. No license has been selected, so a source preview must not be
described as an open-source or package-registry release.

Signed artifacts, SBOM and provenance, clean-install matrices, migration and
rollback support, vulnerability attestations, and production support limits are
required before the release class can be expanded.

## Protected release procedure

1. Create a release branch from the current `main`.
2. Update `VERSION`, every language/package version, user-agent versions, the
   changelog, and `docs/releases/<tag>.md`.
3. Run `make release-check`, `make check`, `make build`, and the integration
   gates appropriate to the milestone.
4. Merge the version bump through a protected pull request.
5. Verify the resulting `main` CI run and main-only Pages deployment.
6. Create and push an annotated `v<version>` tag at the exact current `main`
   commit.
7. Wait for **Release tag verification** to prove the tag/version match and
   main provenance.
8. Create a GitHub prerelease using the version-controlled notes file.
9. Verify the published tag, target commit, notes, source archives, and public
   documentation link.

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
