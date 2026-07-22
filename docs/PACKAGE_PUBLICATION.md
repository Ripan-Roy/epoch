# Package publication

Epoch separates **package shape** from **publication approval**. A wheel, JAR,
crate archive, module, or npm tarball can be structurally valid while its name,
license, ownership, signing identity, or supported API is still undecided. CI
must never turn the first fact into the second claim.

The machine-readable source of truth is
[`release/package-readiness.json`](../release/package-readiness.json). It maps
the shared development release `0.1.0-alpha.2` to Python `0.1.0a2` and Go tag
`v0.1.0-alpha.2`, declares every package surface, and names every unresolved
blocker. Unknown packages and unknown blocker identifiers fail the policy gate.

## Current status

| Ecosystem | Current coordinate | State | Publication boundary |
|---|---|---|---|
| Rust | none | Blocked | Every workspace crate is explicitly `publish = false`; public crate/API names are undecided. |
| Go | `epoch.local/epoch` | Blocked | The module path is deliberately non-public and the root-versus-SDK-module layout is undecided. |
| Java | provisional `io.epoch:epoch-sdk` | Blocked | Central namespace, license, developer identity, signatures, and account ownership are unresolved. |
| Python | provisional `epoch-sdk` | Blocked | Package shape is tested, but license and PyPI project ownership are unresolved. |
| TypeScript | none | Planned | No SDK package exists; the console and workspace remain private packages. |

The published [`v0.1.0-alpha.1`](https://github.com/Ripan-Roy/epoch/releases/tag/v0.1.0-alpha.1)
GitHub prerelease is a source preview only. It must never be reused for registry
packages produced from another commit. The first possible registry version is
therefore `0.1.0-alpha.2` (`0.1.0a2` on PyPI), after every applicable blocker
is resolved on the exact tag commit.

## Local package-shape gate

Run:

```shell
make package-shape
```

The gate is intentionally nonpublishing. It performs policy tests, checks
explicit Rust privacy, verifies the provisional Go boundary, builds and
install-tests Java and Python package shapes, and rejects an accidentally
public TypeScript package. It uses temporary directories and never invokes a
registry upload endpoint.

GitHub Actions runs the same checks without `id-token: write`, registry
credentials, publish commands, or distributable release uploads. The manual
release-policy workflow remains red while blockers exist. A red release-policy
result is a safety decision, not a flaky build.

## Ecosystem acceptance rules

### Rust

- Every workspace crate must explicitly choose `publish = false` or an approved
  registry allowlist; Cargo's implicit publishable default is forbidden.
- A public crate needs a chosen license, repository/readme metadata, an owned
  name, and exact version fallbacks for internal path dependencies.
- Public crates cannot depend on private workspace crates or git-only normal
  dependencies.
- Runtime internals do not become a supported public SDK merely because
  `cargo package` can produce an archive.

### Go

- Shape mode requires the provisional `epoch.local/epoch` path and forbids a
  tag/proxy claim.
- Registry mode must atomically replace that path in the module, generated
  code, quickstarts, and documentation with a permanent owned path.
- A root module uses `v0.1.0-alpha.2`; a nested `sdk/go` module would instead
  use `sdk/go/v0.1.0-alpha.2`. This choice is immutable once fetched publicly.

### Java

- The structural gate verifies the main JAR, sources, Javadocs, POM, and a clean
  consumer compile/run test. A real Central candidate must additionally provide
  its required checksums and signatures.
- The Central plugin is configured with publishing hard-disabled in shape mode.
- Structural output is not Central-ready until the POM has the chosen license,
  developers and SCM, the namespace is verified, and every required file is
  signed by the real release identity.

### Python

- CI builds both sdist and wheel with pinned build tools, runs strict Twine
  validation, inspects archive metadata/content, and installs each artifact in
  a clean environment before importing the SDK and checking its version.
- The missing distribution license is a hard release blocker even when these
  mechanical checks pass.
- A future PyPI workflow must use a protected GitHub environment and Trusted
  Publishing; no long-lived token belongs in this repository.

### TypeScript

- The absent SDK is recorded as planned. Any new `sdk/typescript/package.json`
  must begin `private: true` until name and scope ownership are approved.
- Publication later requires an explicit file/exports/types allowlist plus a
  packed-tarball consumer test. The console is an application, not the SDK.

## Promotion sequence

1. Record the distribution license and commercial/open-source boundary.
2. Clear the permanent brand, repository, domain, registry coordinates, and Go
   module layout.
3. Decide the supported public Rust and TypeScript API surfaces.
4. Configure at least two registry owners, 2FA/recovery, protected GitHub
   environments, and provider-specific Trusted Publishing where supported.
5. Configure the real Maven namespace and signing-key custody; do not generate
   a throwaway key merely to make CI green.
6. Change only the approved manifest entries from blocked/private to candidate,
   run package shape and strict release policy on the exact commit, and review
   the produced SBOM/provenance/signature evidence.
7. Create one immutable tag and publish every artifact from that tag's bytes.
   Never move a tag or rebuild the same version from a different commit.

See [Release readiness](RELEASE_READINESS.md) for the live decision checklist.
