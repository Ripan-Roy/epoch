# Testing Strategy

Epoch's guarantees are product behavior, so tests must prove semantics under
failure, not merely exercise successful API calls. This document defines the
test layers and the evidence required as the implementation grows.

## Local gate

Run before sending a change for review:

```shell
make check
```

It performs generated-code freshness, formatting, static analysis, and unit
tests for every language area currently present. The extended deterministic
local gate is:

```shell
make ci
```

`make ci` additionally validates pinned toolchains, builds all components, and
validates the Compose model. Long-running compatibility, fuzz, simulation,
chaos, soak, and performance suites remain separate so the fast gate stays
useful.

## Test layers

### 1. Unit and property tests

Unit tests live beside the code they cover. They must be deterministic and must
not require Docker, external networks, wall-clock sleeps, or fixed host ports.

Use property tests for:

- persisted frame encode/decode and checksum behavior;
- log append, truncation, recovery, and compaction;
- TTL, delay, lease, retry, and scheduling state transitions;
- queue ready/in-flight/ack/dead-letter indexes;
- stream offset and transaction visibility;
- cache eviction and shard-local atomic operations;
- routing filters and protocol-envelope translation.

Persisted formats require golden vectors that include the format version,
endianness, malformed lengths, checksum failures, unknown fields, and prior
supported versions.

### 2. Deterministic simulation

The simulator owns virtual network, disk, process lifecycle, monotonic time, and
wall-clock observations. Every run accepts and prints a seed. A failing seed is
an artifact and a permanent regression test until the underlying state space is
covered another way.

Simulation injects:

- reordered, duplicated, delayed, and dropped messages;
- one-way and complete network partitions;
- process crash at every persistence boundary;
- partial writes, fsync failure, disk-full, and checksum corruption;
- leader transfer, stale epochs, replica repair, and membership changes;
- monotonic advancement and wall-clock jumps;
- concurrent retry, lease expiry, acknowledgement, and cancellation.

Do not use `sleep` to make a distributed assertion pass. Advance the injected
clock or wait on an observable state transition with a bounded deadline.

### 3. Integration tests

Integration tests start real Epoch processes with isolated temporary data
directories and allocated ports. They cover:

- standalone startup, health, drain, shutdown, and restart;
- a three-node cluster, election, replication, and quorum loss;
- committed write recovery from snapshot plus log tail;
- queue lease, redelivery, retry, schedule, dead letter, and redrive;
- stream append, fetch, offset commit, rewind, and retention;
- cross-language calls over generated Protobuf contracts;
- OpenTelemetry/metrics and immutable audit event emission.

Tests must always capture node configuration, logs, seed, process exit status,
and relevant data manifests on failure. Payloads and credentials are redacted.

### 4. History and consistency checking

Concurrent test clients record invocation, response, timeout, resource epoch,
commit position, and observed value. Offline checkers validate:

- linearizability for supported cache/state operations;
- no successful quorum acknowledgement before durable majority commit;
- no queue deletion before committed acknowledgement state;
- no silently skipped committed eligible record on at-least-once paths;
- fencing of stale leaders, producers, consumers, and session owners;
- read-committed consumers never exposing aborted transaction records.

An unknown outcome after a client timeout is not treated as failure or success
without resolving its idempotency key or commit receipt.

### 5. Protocol compatibility

Compatibility claims name exact client and protocol versions. Differential
suites compare supported behavior with pinned reference containers for Redis,
Kafka, and RabbitMQ. Tests cover the public support matrix, error mapping,
metadata round trips, retries, backpressure, malformed frames, and lossy
translation disclosures.

Unsupported behavior must fail explicitly. A test that happens to pass outside
the published subset does not expand the compatibility promise.

### 6. Fuzzing and concurrency exploration

Fuzz all externally controlled parsers and stateful boundaries:

- RESP, Kafka, AMQP, MQTT, HTTP, and native frames;
- Protobuf/JSON envelopes, schemas, compression, and transformations;
- log frames, manifests, snapshots, and restore input;
- filter expressions, connector configuration, and webhook headers.

Run Loom-style concurrency tests for small synchronization components. Run Miri
for any crate granted an unsafe-code exception. Corpus and crash output are
local artifacts; minimized non-sensitive regressions belong in test fixtures.

### 7. Chaos, recovery, and soak

Chaos tests run against production-shaped multi-node deployments. Required
scenarios include node and zone loss during peak load, snapshot, compaction,
rebalance, repair, backup, restore, and mixed-version upgrade. Recovery tests
must prove data and index digests, not only service availability.

Soak duration grows with maturity: 30 days before private-alpha exit, then 60
and 90 day campaigns before stronger release claims. Every campaign records
configuration, build identity, workload, saturation level, injected faults,
and SLO distribution.

### 8. Benchmarks

Performance results state hardware, payload sizes, concurrency, dataset size,
durability, replicas, acknowledgement mode, batching, compression, and failure
conditions. Report p50, p95, p99, p99.9, maximum, and a saturation curve.
Averages alone are not accepted.

Reference comparisons with Redis, Kafka, or RabbitMQ use matched semantics.
Volatile writes are not compared with quorum writes, and hot-tier results are
not blended with object-tier fetches. Benchmarks should remain non-blocking in
ordinary pull requests and enforce reviewed regression budgets on stable,
dedicated runners.

### 9. Security and tenancy

Security suites cover authorization across every protocol, tenant isolation,
policy-cache expiry, key rotation, audit integrity, payload redaction, webhook
SSRF, connector egress allowlists, decompression/schema bombs, credential
replay, resource exhaustion, and object-tier tampering.

Dependency and release gates include Cargo advisory/license policy, Go
vulnerability scanning, JavaScript dependency scanning, secret scanning, SBOM
generation, artifact signing, and provenance verification.

## Test organization

```text
crates/*/src/                    Rust unit tests close to implementation
crates/*/tests/                  Crate-level black-box tests
tests/integration/               Real-process and cross-language tests
tests/simulation/                Deterministic simulator scenarios and seeds
tests/compatibility/             Protocol/client conformance matrices
tests/chaos/                     Cluster fault campaigns
tests/benchmarks/                Reproducible workload drivers and baselines
tests/fixtures/                  Non-sensitive golden vectors and corpora
spec/models/                     Formal models and checked invariants
```

Large generated data, runtime volumes, payload captures, and benchmark results
are not committed. Small minimized fixtures that protect a correctness rule are
committed and documented.

## CI topology

| Pipeline | Trigger | Required work |
| --- | --- | --- |
| Fast | Every pull request | Format, lint, generation freshness, unit/property tests |
| Integration | Every pull request once available | Standalone and three-node process tests |
| Simulation | Pull request seed sample; larger nightly matrix | Deterministic failure exploration |
| Compatibility | Nightly and release | Pinned Redis/Kafka/RabbitMQ client matrix |
| Security | Pull request and scheduled | Dependencies, secrets, fuzz smoke, authorization |
| Performance | Nightly or scheduled dedicated runner | Baselines, saturation, regression analysis |
| Chaos/soak | Scheduled environments | Fault, upgrade, repair, restore, and long-duration evidence |
| Platform | Weekly and release | Linux amd64/arm64 and macOS arm64 smoke/release matrix |

Linux is the primary test platform. macOS validates developer workflows and
portable standalone behavior; release claims require the supported deployment
matrix, not only the author's workstation.

## Flake and timeout policy

- A failed correctness test blocks the change until explained.
- Retrying is diagnostic, not a way to turn red into green.
- A flaky test receives an owner and issue immediately; quarantine is bounded
  and visible, and it cannot hide a release invariant.
- Deadlines derive from explicit test configuration and emit state on expiry.
- Random tests always print their seed and use a stable seed in CI artifacts.
- Wall-clock-sensitive assertions use tolerances only where the public contract
  itself has a tolerance, such as scheduled-delivery eligibility.

## Pull-request evidence

A behavior change should identify:

1. the PRD requirement and architecture/semantics section it implements;
2. the test layer that proves the normal path;
3. the injected failure or boundary case that could falsify the guarantee;
4. compatibility, storage-format, rollout, and rollback impact;
5. benchmark evidence when the data path or resource accounting changes.

Passing tests are necessary but do not create a product claim by themselves.
Update the requirements traceability and compatibility matrices only when the
corresponding acceptance evidence is reproducible.

## Related documents

- [Development](DEVELOPMENT.md)
- [Architecture](ARCHITECTURE.md)
- [Requirements traceability](REQUIREMENTS_TRACEABILITY.md)
- [Product requirements](PRD.md)
