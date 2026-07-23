# Testing Strategy

Epoch's guarantees are product behavior, so tests must prove semantics under
failure, not merely exercise successful API calls. This document defines the
test layers and the evidence required as the implementation grows.

Test construction and definition-of-done rules are mandatory and documented in
[ENGINEERING_STANDARDS.md](ENGINEERING_STANDARDS.md). Bug fixes begin with a
reproducing failing test; new behavior follows red → green → refactor.

## Local gate

Run before sending a change for review:

```shell
make check
```

It performs generated-code freshness, formatting, static analysis, Rust
documentation checks, unit tests for every language area currently present,
and the Rust dependency advisory gate. The extended deterministic local gate
is:

```shell
make ci
```

`make ci` additionally validates pinned toolchains, builds all components, and
validates the Compose model. Long-running compatibility, fuzz, simulation,
chaos, soak, and performance suites remain separate so the fast gate stays
useful.

### Rust dependency gate

Both pull-request CI and `make audit` install or require `cargo-audit` 0.22.2
and run:

```shell
cargo audit --deny warnings --ignore RUSTSEC-2025-0057
```

All reported Rust advisories and audit warnings fail the gate except
`RUSTSEC-2025-0057`. That single temporary exception is the unmaintained
`fxhash` dependency inherited through `raft`; it is not a vulnerability waiver
or an acceptance of the adapter. ADR-0003 remains Proposed until the dependency
decision and replacement path are reviewed. No additional advisory may be
ignored without a documented review and a bounded removal condition.

The Rust CI job also tests and builds the complete workspace with
`--all-features`, and builds workspace documentation with
`RUSTDOCFLAGS="-D warnings"`. It installs `protoc` 35.1 through the repository's
Linux installer before any Rust compilation and verifies the exact
`libprotoc 35.1` output.

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

The current `epoch-testkit` foundation provides a stable SplitMix64-seeded
scheduler, independent virtual wall/monotonic time, occurrence-indexed crash,
I/O, partial-write, drop, delay, and duplicate actions, directed and
bidirectional peer partitions, and a strictly decoded EPTR v1 trace with a
stable history digest. Golden tests pin the random sequence, generic trace
framing, and one complete seeded transport history. A failing run must retain
its seed and fault plan alongside the trace: EPTR does not yet encode an
executable replay bundle. This is the simulation kernel only; persistent
consensus storage, process lifecycle, and profile state machines must still
publish invariant histories before the simulator or emulator requirement is
complete.

The Stage 1 `epoch-consensus` harness now adopts the peer-transport portion of
that kernel. A fixed seed and canonical trace cover three fixed voters under
directed partitions, delayed/reordered delivery, duplicate delivery, election,
majority-only commit, old-leader replacement, catch-up, and leader transfer.
Full applied histories and SHA-256 state digests are compared; proposal tests
cover restart reconstruction, overwrite, exact duplicate application,
conflicting payload reuse, and corrupt restart images. The bounded peer-frame
suite covers destination, membership, canonical encoding, corruption, local
message classes, snapshots, and maximum size.

The distributed fault histories still use `MemStorage` and graceful in-process
restart images. A separate EPRS v1 suite exercises the local fsync-backed
stable journal's exact identity bytes, create/reopen, immutable identity,
writer exclusion, `HardState`/entry replay, uncommitted-suffix replacement,
partial-tail repair, corruption rejection, and safety regressions. This is not
yet an exhaustive injected-I/O or real-process-crash matrix. Persistent adapter
tests additionally reopen a three-voter committed history, preserve an isolated
pending proposal, verify stable-barrier message ordering, recover after an
injected post-append error, and publish a commit-ahead-of-checkpoint receipt
exactly once.

The explicit `test-consensus-process` gate extends that evidence across real
process boundaries. A parent harness starts three child test executables, each
with its own EPRS journal and loopback control socket, and routes their bounded
peer frames through the deterministic `PeerTransport`. It isolates a leader,
proves that a minority proposal remains pending without a receipt, heals the
partition, compares the committed receipt, payload, and state digest at all
three voters, sends `SIGKILL` to one child and then all children, and reopens the
same journal paths without republishing duplicate receipts. Failure retains the
seed, EPTR trace, child logs, and data directories; CI uploads those artifacts.
This is process-crash evidence for the isolated adapter, not node-to-node HTTP
transport, product profile replication, or a complete crash-point matrix.
The complementary `three_probe_runtimes_elect_and_commit_over_real_http` test
starts three persistent probe runtimes with ephemeral loopback listeners,
elects through the runtime probe HTTP transport, commits opaque bytes, and
compares proposal lookup at every voter. It covers runtime transport but not
separate process loss; the two test layers intentionally prove different
boundaries. `sustained_minority_outage_drops_only_that_peers_frames_and_majority_commits`
uses a one-frame outbound queue, keeps the lower-ID destination unavailable
until drops and exhausted retries are observed, and proves eight subsequent
proposals still commit on the healthy majority.

The typed Stream-tablet layer adds bounded durable fixed-voter-majority and
deterministic profile-application evidence for one fixed group and partition. It
does not add snapshot, membership-transition, authoritative epoch-fencing,
public product acknowledgement, or complete G3 evidence. Those scenarios remain
required before G3 or the emulator is complete; see
[Consensus Feasibility Spike](CONSENSUS_SPIKE.md).

The Queue-tablet layers check strict canonical commands, three-instance
convergence, leader/consumer/token fencing, exact renewed-token replay,
monotonic committed-order time including descending leader assignments,
exclusive deadlines, rejection rollback, non-zero jitter, TTL/max-age
precedence, immutable DLQ/redrive provenance, browser-safe receipts, and pinned
proposal/command/digest vectors. A real three-runtime test drives every Queue
operation through typed HTTP and EPRS reopen. The container
gate adds scheduled eligibility, follower rejection, leader `SIGKILL`,
old-term-token fencing, redelivery, DLQ/redrive reads, convergence, and all-node
`SIGKILL` replay. This is bounded fixed-voter evidence, not a complete crash,
I/O-fault, or production placement matrix. See
[Experimental Replicated Queue Tablet](QUEUE_TABLET.md).

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

The current fast integration smoke starts real Rust and Go processes, exercises
all four profile APIs through the Python SDK, validates the Go and Java SDKs
against the same node, restarts the Rust process, proves local-durable Stream
and Queue state survived, and proves volatile resources did not. The node HTTP
suite forces a small segment threshold, verifies physical rotation, restarts,
and checks Stream and Queue state across multiple files. Storage tests verify
one global sequence across segment boundaries and exclusive writer ownership.
Fresh directories activate through a staging and then active marker at
`engine.wal`; direct tests cover rejection by the older single-file reader and
recovery from a torn staging marker. Relative-root and nested-directory tests
cover durable parent selection and component-wise creation; the node does not
pre-create the data root outside the storage boundary. A separate downgrade
test verifies that an existing valid `engine.wal` remains the only journal,
receives new appends through the legacy writer, and does not create
`engine-wal/`.

Run the persistent consensus process smoke directly with:

```shell
make test-consensus-process
make test-consensus-probe
make test-stream-tablet
```

`test-consensus-process` is ignored by Cargo's default suite so it cannot run
accidentally as a unit test, while the Make target, extended local integration
gate, and GitHub Actions Rust job select it explicitly.

`test-consensus-probe` builds a single node image, starts three containers with
independent EPRS volumes and dynamically allocated loopback ports, verifies the
truthful experimental status contract, commits an opaque proposal, stops the
leader, observes a higher-term election and majority commit, restarts the old
leader, and waits for identical local lookup at all voters. The script uses a
unique Compose project and deletes only its ephemeral containers, network, and
volumes. On CI failure it retains container logs, state, and port assignments as
an uploaded artifact.

`test-stream-tablet` selects the mutually exclusive typed mode on the same
three-voter runtime. It verifies a follower error, success only after majority
commit and local application, `fixed_voter_majority_persisted` with two durable
voters, ordered Stream offsets, exact retry, and changed-input conflict
behavior. It isolates an old leader and proves no committed response, then
commits different input under the same deterministic ID on a higher-term
majority and proves the original input conflicts rather than receiving that
receipt. It verifies old-voter catch-up, sends `SIGKILL` to all three
containers, reopens the same EPRS volumes, compares every record and profile
digest with the pre-crash state across every voter, and proves a retry still
resolves to the original offset. On failure, CI retains the scoped logs, port
map, and state volumes as an artifact. The process still starts its empty
standalone engine for the separate public API,
but typed commands are never appended to that engine journal. Unit/runtime
tests additionally prove strict command/request decoding, browser-safe 64-bit
identity/position/time encoding,
actor-only application, and process supervision after an injected live profile
apply failure.

`tests/integration/docs-quickstarts.sh` separately executes the exact Go, Java,
and Python source imported into the documentation page. Each language gets a
fresh data directory, creates a local-durable Stream and Queue, acknowledges
only `job-1001`, kills the node with `SIGKILL`, restarts from the same bytes,
and proves that the Stream record, acknowledgement count, and only `job-1002`
survived. The GitHub Pages deploy job depends on this lifecycle test as well as
the documentation-only frontend build. Pull-request runs execute both gates but
cannot upload or deploy Pages; publication is restricted to `main`.

Manifest recovery tests distinguish a safe uncommitted suffix from committed
data damage. Only bytes beyond the manifest's committed length in the active
segment are discarded. Missing or truncated committed segments, bytes appended
to a sealed segment, a content-checksum mismatch, a sequence/topology mismatch,
an untracked segment, missing identity or manifest metadata, and a foreign WAL
identity fail closed. Pending-rotation recovery can create a missing expected
target or adopt it only when empty; the direct unit test covers the missing-file
branch.

Current direct unit evidence includes:

- `segmented_wal_rotates_and_recovers_global_sequences` and
  `segmented_wal_rejects_a_second_writer`;
- `segmented_wal_cannot_rotate_past_a_poisoned_active_segment`, which forces an
  append and rollback failure and proves no later rotation or manifest change
  can bypass the terminal fault;
- `segmented_wal_repairs_only_the_active_tail`,
  `segmented_wal_discards_bytes_not_committed_by_the_manifest`, and
  `segmented_wal_rejects_a_partial_sealed_segment`;
- `segmented_wal_rejects_a_missing_committed_final_segment`,
  `segmented_wal_rejects_all_committed_segments_missing`,
  `segmented_wal_rejects_a_truncated_committed_active_segment`,
  `segmented_wal_rejects_a_missing_manifest_after_activation`, and
  `segmented_wal_rejects_a_missing_identity`;
- `segmented_wal_rejects_sequence_gaps_between_files`,
  `segmented_wal_rejects_a_foreign_manifest_identity`,
  `segmented_wal_rejects_valid_frames_that_do_not_match_the_manifest`, and
  `segmented_wal_rejects_checksum_corruption`;
- `segmented_wal_completes_a_manifested_pending_rotation`; and
- `standalone_wal_activation_blocks_single_file_writers`,
  `standalone_wal_resumes_a_torn_staging_marker`,
  `standalone_wal_rejects_a_missing_activated_segment_directory`,
  `standalone_wal_keeps_existing_legacy_history_downgrade_safe`, and
  `standalone_wal_rejects_ambiguous_legacy_and_segmented_histories`.

The Queue lifecycle test restarts across enqueue, acquire, extend, Ack, Reject,
redrive, and scheduled eligibility. Injected journal failures prove proposed
Queue enqueues and settlements never leak into live memory. Container CI mounts
the data directory into a named volume, asserts that a small configured rotation
threshold creates multiple files under `engine-wal/`, and repeats Stream and
Queue recovery after replacing the running container.

These tests are segmented-journal evidence only. Snapshot restore, compaction,
retention deletion, replica recovery, and quorum acknowledgement remain future
test gates.

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

Dependency and release gates include Cargo advisory/license policy, Go and
Python vulnerability scanning, JavaScript dependency scanning, secret scanning,
SBOM generation, artifact signing, and provenance verification.

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
| Security | Pull request and scheduled | Rust advisory gate; secrets, fuzz smoke, authorization as implemented |
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
