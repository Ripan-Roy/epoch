# Epoch

> One runtime. Every real-time workload.

Epoch is a cloud-neutral real-time data platform with four explicit workload
profiles: Cache and State, Stream Log, Work Queue, and Event Bus. The profiles
share identity, policy, observability, storage primitives, and operational
tooling while retaining the distinct semantics that make each workload useful.

Epoch is currently an early engineering scaffold. Interfaces, storage formats,
and compatibility claims are not stable, and no production guarantee is
implied yet. The source of truth for product scope is [the PRD](docs/PRD.md).
Delivery status and release gates are tracked in the
[delivery checklist](docs/DELIVERY_CHECKLIST.md).
The runnable node supports volatile resources for all four profiles and an
explicit `local_durable` mode for Streams and Work Queues. Durable Stream
records/offsets and Queue messages/leases/settlements are fsynced into a
checksummed, rotating, single-node journal and replayed on restart. This is not
replication, a snapshot/compaction system, or protection from total machine
loss.

An opt-in regional alpha also runs a three-voter catalog plus several
profile-specific consensus groups in each Rust node. It is fixed-topology
engineering evidence, not a production or multi-zone guarantee.

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

Go, Java, and Python are the P0 SDK ecosystems. Typed HTTP clients are under
`sdk/go`, `sdk/java`, and `sdk/python`; generated native streaming parity across
all three remains tracked by DX-001.

`crates/epoch-testkit` is the no-sleep correctness harness for the replicated
foundation: seeded scheduling, independent wall/monotonic time, scripted fault
occurrences, partitionable peer transport, and versioned trace serialization
and comparison. It is test infrastructure and does not raise the standalone
node's guarantee ceiling.

`crates/epoch-consensus` contains fixed-three-voter memory and EPRS-backed
`raft-rs` feasibility adapters. `crates/epoch-tablet` supplies separate typed
profile state machines. Its opt-in, single-partition Stream tablet commits
through the real three-node runtime and rebuilds from the consensus log. Unit,
real-runtime, and three-container gates prove typed leader errors, minority
non-commit, majority-before-success, bounded two-durable-voter evidence,
semantic retry/rebinding, ordered offsets, leader replacement, catch-up, and
all-node `SIGKILL` replay.

`crates/epoch-catalog` supplies deterministic multi-resource generations and
shard/tablet/group routing identity. The regional runtime commits it through
dedicated catalog group 1, demultiplexes several groups on one peer listener,
materializes Cache, Stream, Queue, and Event Bus tablets together, and routes
by resource/shard with exact generation and tablet-epoch fences. A real Go
RegionalAdmin reconciler applies desired state through Rust and exposes a
browser-safe placement BFF; the TypeScript console never contacts storage nodes.
Regional reads now default to a safe leader ReadIndex barrier, complete only
after majority confirmation and local typed-profile apply, and expose exact
barrier evidence. Callers must explicitly select `local_stale` to bypass it.

The strict single-partition Queue tablet now runs through that same persistent
three-node actor boundary. Its internal typed API covers enqueue, acquire,
renewal, settlement, retry/scheduling, maintenance, DLQ, redrive, status, and
mutation lookup with deterministic time and leader/consumer-fenced leases.
Real-runtime and container gates prove failover, exact renewal replay,
convergence, and all-node `SIGKILL` recovery. It remains experimental, has no
public SDK surface, and does not raise the standalone `local_durable` ceiling.

The deterministic single-shard Cache tablet now runs through the same opt-in
fixed-voter actor and rebuilds from EPRS before serving. Its internal typed API
covers pure local observations, checked revisions, CAS, atomic transactions,
increment, explicit expiry maintenance, advisory fenced locks, status, and
mutation lookup. It remains experimental, stale-read capable, single-shard,
unauthenticated, and outside the public SDK contract.

The Event Bus ingress/outbox tablet is the fourth opt-in typed profile. It
replicates subscription changes and publish ingress, atomically creates one
durable delivery record per matching subscription, and rebuilds route, archive,
lease, retry, acknowledgement, dead-letter, and attempt-history state from EPRS
before serving. Strict internal mutations cover fenced acquire/ack/fail and
bounded timeout maintenance; a bounded local query exposes the ledger.
Real-runtime and container gates prove retry/conflict behavior, target
isolation, leader failover, convergence, and all-node recovery. No built-in
Queue, Stream, webhook, HTTP, or network pull executor exists yet, so outbox
state is not a target-delivery claim. See the
[Cache tablet guide](docs/CACHE_TABLET.md),
[Queue tablet guide](docs/QUEUE_TABLET.md),
[Stream tablet guide](docs/STREAM_TABLET.md),
[Event Bus tablet guide](docs/BUS_TABLET.md),
[regional runtime guide](docs/REGIONAL_RUNTIME.md),
[probe guide](docs/CONSENSUS_PROBE.md), [spike report](docs/CONSENSUS_SPIKE.md),
and proposed [ADR-0003](docs/adr/0003-consensus-adapter.md). An exhaustive crash
matrix, snapshots, membership/epoch transitions, follower read routing,
authenticated transport, dynamic/zone-aware placement, and stable public
multi-tablet routing remain open.

## Quick start

The supported local baseline is macOS on Apple Silicon with:

- Go 1.26.5
- Rust 1.97.1, including `rustfmt` and Clippy
- Protobuf compiler 35.1
- Buf 1.72.0
- Python 3.11 or newer, Ruff 0.15.19, actionlint 1.7.12, and ShellCheck 0.11.0
- Java 25 LTS or newer; the checked-in wrapper pins Maven 3.9.16
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

Start a standalone node and create restart-safe local Stream and Queue resources:

```shell
cargo run -p epoch-node -- --data-dir .epoch
cargo run -p epoch-cli -- stream create audit --durability local-durable
cargo run -p epoch-cli -- queue create jobs --durability local-durable
```

Use a separate terminal for the CLI commands. Omitting `--durability` creates a
volatile Stream or Queue; Cache and Event Bus are currently volatile-only.

Fresh installations store the standalone journal under
`$EPOCH_DATA_DIR/engine-wal/` as `segment-*.wal`; `engine.wal` becomes a
crash-safe activation marker and cross-version lock. New segments rotate at a
configured byte threshold: 64 MiB by default, set with `--wal-segment-bytes` or
`EPOCH_WAL_SEGMENT_BYTES`. Rotation does not delete, compact, or snapshot older
segments. Frames are never split, so one frame larger than the target may
occupy an otherwise empty segment.

A versioned identity and checksummed manifest bind the ordered segment set,
committed lengths, sequence range, and file checksums. Recovery may discard only
an uncommitted suffix beyond the active segment's manifested length; missing,
truncated, unexpected, reordered, or corrupted committed history fails startup.
A pre-existing valid legacy `engine.wal` remains the active single-file WAL and
the current binary continues appending to it, preserving safe offline downgrade.
Automatic legacy migration is deliberately deferred, and ambiguous mixed
histories fail closed.

The Vite application includes both the live node console and a public SDK
quickstart. Run it locally, then use the top navigation or hash routes:

```shell
pnpm --filter @epoch/console dev
# http://127.0.0.1:5173/#/console
# http://127.0.0.1:5173/#/docs
```

The node allows the local Vite development and preview origins by default.
Set the comma-delimited `EPOCH_ALLOWED_ORIGINS` only when the live console is
served from another trusted HTTP(S) origin. CORS is not authentication; keep
the unauthenticated alpha node on a trusted network.

Regional console inventory additionally requires `epoch-control` on port 8080.
Its browser allowlist is `EPOCH_CONTROL_ALLOWED_ORIGINS`, and the console points
to it with `VITE_EPOCH_CONTROL_BASE_URL`. Managed `/v1` and RegionalAdmin gRPC
calls require a bearer principal from `EPOCH_AUTH_POLICY_PATH`; the console
accepts this credential interactively and stores it only for the browser
session. `epoch-control` authenticates to regional Rust nodes with
`EPOCH_CONTROL_REGIONAL_TOKEN`. The checked-in example values are public
development fixtures, not production secrets. Management metadata is
transactionally stored at `data/control/registry.db` by default; set
`EPOCH_CONTROL_STATE_PATH` to choose another single-owner file. See the
[regional runtime guide](docs/REGIONAL_RUNTIME.md) for the exact three-node
startup and Go-to-Rust verification path.

Static builds are base-path aware for GitHub Pages and other subdirectory
hosts. For this repository's Pages path, build with:

```shell
VITE_BASE_PATH=/epoch/ VITE_DEFAULT_PAGE=docs VITE_DOCS_ONLY=true \
  pnpm --filter @epoch/console build
```

The Pages artifact contains documentation only—no localhost console client.
Its configured deployment target is
[`https://ripan-roy.github.io/epoch/`](https://ripan-roy.github.io/epoch/).
The workflow executes every displayed Go, Java, and Python seed → forced crash
→ restart → verification example before deployment. Pull requests build and
verify the same artifact but never publish it; deployment is permitted only
from `main` (including a manual dispatch that targets `main`). The public site
is live with enforced HTTPS. The SDKs remain repository-local pre-alpha packages
and are not presented as registry releases.

Verified milestones are published as
[GitHub prereleases](https://github.com/Ripan-Roy/epoch/releases) from tagged
`main` commits with version-controlled release notes. The current alpha
releases remain source-only: they do not publish registry packages, container
images, installers, or signed binaries. See
[Releasing](docs/RELEASING.md) for the protected release procedure and
[Changelog](CHANGELOG.md) for the release history.

Run the local verification suite:

```shell
make check
```

Run the disposable Go-to-Rust multi-tablet failure/recovery proof with:

```shell
make test-regional-runtime
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
