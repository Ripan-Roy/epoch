# Release readiness

This checklist distinguishes the existing GitHub source preview from a package
registry release. It is intentionally strict: source visibility, a green build,
and installable archives do not grant a license or prove registry ownership.

## Released source preview

- [x] `v0.1.0-alpha.1` points to verified merge commit `664f4a27028500600a9e826e4b8d2c282e159cd3`.
- [x] Main CI and the main-only Pages deployment passed for that commit.
- [x] Release notes state that no binaries, images, installers, SDK packages, or registry artifacts were published.
- [x] The release states that no open-source or commercial license has been selected.

## Alpha.2 package shape

- [x] One machine-readable policy maps SemVer, Python, and Go tag versions.
- [x] Existing Rust crates are explicitly private instead of relying on Cargo's publishable default.
- [x] Java and Python shapes are built and install-tested without uploads.
- [x] Go's provisional module boundary and the absent/private TypeScript surface are enforced.
- [x] CI jobs have read-only repository permissions and no registry identity token or secret.
- [ ] A strict release-policy run is green.

The final item must remain unchecked while any blocker below exists.

## Decisions and external ownership still required

- [ ] Distribution license and open-source/commercial boundary.
- [ ] Formal Epoch naming, trademark, domain, repository, and registry clearance.
- [ ] Permanent Go module path and root-versus-nested SDK module layout.
- [ ] Supported public Rust crate/API surface and owned crate names.
- [ ] npm scope and a real TypeScript SDK surface.
- [ ] PyPI project ownership and protected Trusted Publisher environment.
- [ ] Maven Central namespace, Publisher terms, developer identity, and real PGP key custody.
- [ ] crates.io owners and first-publish bootstrap for any approved crate.
- [ ] Supported consumer runtime floors for Rust, Go, Java, Python, and Node.
- [ ] SBOM, signing, provenance, and immutable-tag protection for distributable artifacts.

Until these decisions are recorded, `0.1.0-alpha.2` is a development version,
not a registry release. See [Package publication](PACKAGE_PUBLICATION.md) for
the exact ecosystem gates and no-tag-reuse rule.
