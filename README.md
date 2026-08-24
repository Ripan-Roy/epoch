# Epoch

[![CI](https://github.com/Ripan-Roy/epoch/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Ripan-Roy/epoch/actions/workflows/ci.yml)
[![Docs](https://github.com/Ripan-Roy/epoch/actions/workflows/pages.yml/badge.svg?branch=main)](https://github.com/Ripan-Roy/epoch/actions/workflows/pages.yml)
[![Release](https://img.shields.io/github/v/release/Ripan-Roy/epoch?include_prereleases&sort=semver)](https://github.com/Ripan-Roy/epoch/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> One runtime. Every real-time workload.

Epoch is an open-source, cloud-neutral data platform for real-time applications.
It gives Cache, Stream, Queue, and Event Bus workloads one operational foundation
without erasing the semantics that make each model useful.

[Documentation](https://ripan-roy.github.io/epoch/) ·
[Quickstart](https://ripan-roy.github.io/epoch/#/docs/quickstart) ·
[Architecture](docs/ARCHITECTURE.md) ·
[Roadmap and scope](docs/PRD.md) ·
[Delivery status](docs/DELIVERY_CHECKLIST.md)

## Why Epoch?

- **Four native workload profiles.** Use TTL and atomic state, ordered replayable
  logs, leased work delivery, or filtered event fan-out through purpose-built APIs.
- **One platform boundary.** Identity, policy, observability, storage primitives,
  deployment tooling, and failure handling are shared.
- **Replicated regional runtime.** Independent profile tablets use fixed-voter
  consensus, linearizable reads, snapshots, encrypted backup, and fresh-cluster
  restore.
- **Data-plane independence.** The Rust data plane keeps serving configured paths
  when the Go management plane is unavailable.
- **Typed SDKs.** Go, Java, and Python clients provide leader discovery, fencing,
  idempotent retries, and profile-specific operations.
- **Bounded migration gateways.** Documented Redis, Kafka, and RabbitMQ client
  subsets translate into the same fenced native Cache, Stream, and Queue paths.

| Profile | Built for |
| --- | --- |
| Cache and State | Low-latency values, TTL, CAS, transactions, and fenced locks |
| Stream Log | Ordered append, replay, retention, offsets, and transactions |
| Work Queue | Leases, retries, scheduling, FIFO sessions, and dead lettering |
| Event Bus | Filtering, fan-out, webhooks, schemas, connectors, and archive replay |

## Quick start

Prerequisites and pinned tool versions are checked automatically:

```shell
make bootstrap-check
make check
make build
```

Run a standalone node:

```shell
cargo run -p epoch-node -- --data-dir .epoch
```

Then create restart-safe resources from another terminal:

```shell
cargo run -p epoch-cli -- stream create audit --durability local-durable
cargo run -p epoch-cli -- queue create jobs --durability local-durable
```

Or start the containerized development runtime:

```shell
docker compose -f deploy/compose/docker-compose.yml up --build
```

See the [quickstart](https://ripan-roy.github.io/epoch/#/docs/quickstart) for
standalone and regional examples in Go, Java, and Python.

## Architecture

| Layer | Technology | Responsibility |
| --- | --- | --- |
| Data plane | Rust | Store, replicate, route, transform, and deliver data |
| Control plane | Go | Reconcile fleets, placement, hosted APIs, and metering |
| Console and docs | TypeScript + React | Browser management and documentation |
| Contracts | Protobuf + HTTP/gRPC | Versioned public and internal boundaries |

The regional runtime supports bounded three- and five-voter groups across a
physical-node inventory. Go services do not read or mutate Epoch storage files.

## Documentation

- [Concepts and architecture](https://ripan-roy.github.io/epoch/#/docs/concepts)
- [Regional runtime](docs/REGIONAL_RUNTIME.md)
- [Stream SDK](docs/REGIONAL_STREAM_SDK.md)
- [Queue SDK](docs/REGIONAL_QUEUE_SDK.md)
- [Cache SDK](docs/REGIONAL_CACHE_SDK.md)
- [Event Bus SDK](docs/REGIONAL_EVENT_BUS_SDK.md)
- [Redis, Kafka, and RabbitMQ compatibility](docs/PROTOCOL_COMPATIBILITY.md)
- [Kubernetes operator](docs/KUBERNETES_OPERATOR.md)
- [Testing and release evidence](docs/TESTING.md)
- [Security policy](docs/SECURITY.md)

## Project status

Epoch is in private beta. Interfaces and storage formats remain provisional,
and the project does not yet claim production SLOs, arbitrary external
exactly-once effects, or a production multi-zone guarantee. Package-manager
publication is intentionally deferred; use source builds and release OCI images
while the beta contract is completed.

See the [requirements traceability matrix](docs/REQUIREMENTS_TRACEABILITY.md)
for implemented and open PRD requirements.

## Contributing

Epoch uses TDD, clean-code boundaries, deterministic tests, and feature-sized
pull requests. Read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a
change. By participating, you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

Licensed under the [MIT License](LICENSE).
