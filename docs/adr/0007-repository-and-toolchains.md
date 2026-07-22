# ADR-0007: Provisional Repository and Toolchains

**Status:** Proposed  
**Date:** 22 July 2026

## Context

Epoch needs a reproducible polyglot workspace that preserves the Rust data
plane, Go hosted-control, and TypeScript console boundary. The current scaffold
has verified concrete tool versions, but package boundaries will evolve as
vertical slices provide evidence for them.

## Proposed decision

Use this top-level ownership layout:

```text
crates/      Rust engines, storage, protocols, node, CLI, and testkit
control/     Go hosted APIs, fleet, reconciliation, and metering
operator/    Go Kubernetes operator
console/     TypeScript/React console
sdk/         Native SDKs and generated bindings
spec/        Protobuf, formats, models, and compatibility contracts
tests/       Integration, conformance, simulation, chaos, and benchmarks
deploy/      Containers and deployment packages
docs/        Architecture, development, security, and operations
tools/       Code generation and benchmark helpers
```

### Rust

The initial Cargo workspace contains or reserves:

```text
epoch-core       I/O-free domain types, envelope, guarantees, errors, clock traits
epoch-storage    versioned frames, segments, snapshots, and recovery
epoch-cache      Cache profile state machine
epoch-stream     Stream profile state machine
epoch-queue      Queue profile state machine
epoch-bus        Event Bus profile state machine
epoch-engine     routing and composition of profile interfaces
epoch-node       executable and role lifecycle
epoch-cli        user-facing CLI
```

Add narrower crates for consensus, catalog/tablets, protocols, schemas,
transactions, connectors, auth, observability, testkit, and embedding only when
they provide a real dependency, security, build, or testing boundary. Avoid a
crate per helper.

Dependencies flow in one direction:

```text
core -> storage/consensus -> tablet/profile engines -> protocols/admin/node
```

Engines do not depend on protocol gateways, Go packages, the console, or binary
crates. Unsafe Rust is forbidden by default; an exception needs its own ADR,
documented invariants, narrow scope, fuzz/property tests, and security review.

### Go

Use one provisional root module, `epoch.local/epoch`, for `control`, `operator`,
and `sdk/go`. Replace the path once, in a reviewed change, after the public
organization and package namespace are cleared. Split the operator into a
separate module only when dependency isolation or release cadence justifies it.

Go packages share generated Protobuf contracts. They do not share Rust storage
layouts or unversioned internal structs.

### Console, SDKs, and contracts

Use a Vite React console backed by the Go BFF; do not add a second Node control
plane. Go and Java are the first native SDKs, followed by Python, TypeScript,
.NET, and Rust. Generated transport bindings are wrapped by hand-written,
guarantee-aware clients and are never edited manually.

Versioned contracts live below `spec/proto/epoch/<surface>/v1`. Buf owns lint,
generation, and breaking checks. Durable golden vectors live under
`spec/formats`; formal models live under `spec/models`. CI rejects stale
generated output once contracts exist.

### Pinned toolchains

| Tool | Repository policy |
|---|---|
| Rust | `1.97.1` in `rust-toolchain.toml`, with rustfmt and Clippy |
| Rust installer | Homebrew is supported on the current workstation; rustup is optional at the same pin |
| Go | `1.26.5` through the root module `go`/`toolchain` declarations |
| Node.js | `24` LTS through `.node-version` |
| pnpm | `10.28.0` through the root `packageManager` field |
| protoc | `35.1`, validated by `make bootstrap-check` |
| Buf | `1.72.0`, validated by `make bootstrap-check` |
| Java | `25` LTS when Java SDK work begins; not required by the server scaffold |

Developer-specific absolute paths do not enter build files. Bootstrap and
verification commands live in `docs/DEVELOPMENT.md`. Nightly Rust is isolated
to a task that requires it, such as Miri, fuzzing, or a sanitizer.

## Acceptance gate

Accept this ADR after a clean machine and CI can reproducibly:

1. select every pinned toolchain;
2. build, format, lint, and test the Rust workspace and root Go module;
3. generate the first Protobuf contract without a diff;
4. pass a cross-language gRPC health call;
5. build the console and one generated API client;
6. run without developer-specific paths or mutable global generated state.

## Consequences

- Repository layout makes language and correctness ownership visible.
- Version changes are deliberate compatibility changes.
- One Go module keeps the scaffold simple but can be split later.
- Package boundaries remain evidence-driven rather than creating dozens of empty
  crates now.

## Rejected alternatives

- One unstructured source tree for all languages.
- Unpinned global toolchains or unreproducible code generation.
- A separate Node.js control backend duplicating Go.
- A general cross-language C ABI exposing Rust engine internals in v1.
