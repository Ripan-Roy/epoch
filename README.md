# Epoch

> One runtime. Every real-time workload.

Epoch is a cloud-neutral real-time data platform with four explicit workload
profiles: Cache and State, Stream Log, Work Queue, and Event Bus. The profiles
share identity, policy, observability, storage primitives, and operational
tooling while retaining the distinct semantics that make each workload useful.

Epoch is currently an early engineering scaffold. Interfaces, storage formats,
and compatibility claims are not stable, and no production guarantee is
implied yet. The source of truth for product scope is [the PRD](docs/PRD.md).
The runnable node currently accepts only volatile profile resources; its local
WAL proof is not yet wired into profile recovery, so stronger durability is
rejected rather than implied.

## Design boundaries

| Area | Implementation | Boundary |
| --- | --- | --- |
| Data plane | Rust | Stores, replicates, routes, transforms, and delivers customer data |
| Managed control plane | Go | Fleet reconciliation, placement, autoscaling, hosted APIs, and metering |
| Web console | TypeScript and React | Browser-based management experience |
| Contracts | Protobuf and gRPC | Versioned public and internal service boundaries |

The Rust node must keep serving an already configured regional data path when
the hosted Go management plane is unavailable. Go services must never read or
mutate Epoch storage files directly.

## Workload profiles

- **Cache and State:** memory-first values, TTL, eviction, atomic operations,
  and explicit volatile or durable modes.
- **Stream Log:** partitioned ordering, retention, replay, compaction, consumer
  offsets, replication, and transactions.
- **Work Queue:** acknowledgements, renewable leases, retries, scheduling,
  priorities, competing consumers, and dead-lettering.
- **Event Bus:** filtering, fan-out, push and pull delivery, webhooks, schemas,
  transformations, connectors, and archive/replay.

Semantics are explicit. Epoch does not claim global ordering or arbitrary
external exactly-once effects, and it never silently upgrades or downgrades a
durability profile.

## Repository layout

```text
crates/          Rust engines, node, CLI, protocols, and shared libraries
control/         Go hosted control-plane services
operator/        Go Kubernetes operator
console/         TypeScript/React console
sdk/             Native SDKs and generated bindings
spec/            Protobuf contracts, schemas, compatibility data, formal models
tests/           Integration, simulation, compatibility, chaos, and benchmarks
deploy/          Container and local orchestration assets
docs/            Product, architecture, semantics, testing, and operations docs
```

Not every target directory exists yet. New components should be introduced only
with a defined responsibility, dependency boundary, and acceptance test.

Go, Java, and Python are the P0 SDK ecosystems. The first Python HTTP client is
under `sdk/python`; generated/native streaming parity remains tracked by DX-001.

## Quick start

The supported local baseline is macOS on Apple Silicon with:

- Go 1.26.5
- Rust 1.97.1, including `rustfmt` and Clippy
- Protobuf compiler 35.1
- Buf 1.72.0
- Python 3.11 or newer, Ruff 0.15.19, actionlint 1.7.12, and ShellCheck 0.11.0
- Node.js 24 LTS and pnpm 10.28.0
- Docker Desktop with Compose v2 for container tests

Check the environment:

```shell
make bootstrap-check
```

Install JavaScript workspace dependencies when a frontend package is present:

```shell
pnpm install
```

Run the local verification suite:

```shell
make check
```

Build and inspect the development container configuration:

```shell
make compose-config
make compose-up
```

See [Development](docs/DEVELOPMENT.md) for toolchain setup and
[Testing](docs/TESTING.md) for the test layers and required gates.
All changes follow the repository's [engineering standards](docs/ENGINEERING_STANDARDS.md),
including test-driven development, SOLID dependency boundaries, and clean-code
definition-of-done checks.

## Planning and traceability

- [Product requirements](docs/PRD.md)
- [Requirements traceability](docs/REQUIREMENTS_TRACEABILITY.md)
- [Delivery plan](docs/DELIVERY_PLAN.md)

The traceability matrix is the contract for “all features”: every PRD
requirement must have an owner, phase, design reference, implementation status,
and acceptance evidence. Breadth does not replace correctness.

## Current development ports

| Port | Purpose |
| --- | --- |
| `7600` | Native gRPC API (reserved while the first scaffold uses HTTP) |
| `7601` | Native/admin HTTP API and health endpoints |
| `9464` | Prometheus/OpenTelemetry metrics |

These ports are development defaults, not a public compatibility promise.

## License and name

The open-source/commercial boundary and license have not been selected. Epoch is
also a working name pending formal trademark, domain, repository, and package
registry clearance. Do not publish packages or artifacts before those decisions
are recorded.
