# Development

This guide describes the reproducible local environment and the commands used
to build Epoch. The product is in its initial scaffold, so some commands skip a
language area until that area contains source files.

Behavioral work follows the mandatory TDD, SOLID, and clean-code policy in
[ENGINEERING_STANDARDS.md](ENGINEERING_STANDARDS.md).

## Supported local baseline

The first development environment is macOS 26 on Apple Silicon. Linux is the
primary CI and container target. The current repository pins or validates:

| Tool | Version | Notes |
| --- | --- | --- |
| Go | 1.26.5 | Installed through Homebrew; also pinned by `go.mod` |
| Rust | 1.97.1 | Homebrew toolchain currently installed |
| Cargo | 1.97.1 | Installed with Rust |
| rustfmt | 1.9.0 | Required by `make format-check` |
| Clippy | 0.1.97 | Required by `make lint` |
| cargo-audit | 0.22.2 | Required by `make audit` and `make check` |
| protoc | 35.1 | Required for contract tooling and language generators |
| Buf | 1.72.0 | Lints and generates Protobuf contracts |
| Python | 3.11 or newer | Runs the official Python SDK and integration client |
| Java | 25 LTS or newer | Compiles Java 25 bytecode for the official Java SDK |
| Maven | 3.9.16 | Downloaded and checksum-pinned by `sdk/java/mvnw` |
| Ruff | 0.15.19 | Formats and lints `sdk/python` |
| actionlint | 1.7.12 | Validates GitHub Actions workflows and embedded shell |
| ShellCheck | 0.11.0 | Lints repository integration scripts |
| Node.js | 24 LTS | `.node-version` pins 24.18.0 |
| pnpm | 10.28.0 | Pinned by the root `package.json` |
| Docker | 29 or newer | Docker Desktop and Compose v2 |
| GNU Make | 3.81 or newer | Root task interface; `just` is not required |

The project intentionally uses Node 24 LTS. The current machine also has Node
26 globally linked; do not unlink it if other projects need it. Put the
Homebrew Node 24 installation first only in the shell used for Epoch:

```shell
export PATH="/opt/homebrew/opt/node@24/bin:$PATH"
node --version
```

If `node@24` is missing or stale:

```shell
brew install node@24
brew upgrade node@24
```

## Install the native toolchain on macOS

Homebrew is the lowest-risk path for the current workstation:

```shell
brew install go rust protobuf buf pkgconf openjdk@25 node@24 pnpm actionlint shellcheck
python3 -m pip install ruff==0.15.19
```

Xcode and its command-line tools supply Clang and the linker:

```shell
xcode-select -p
clang --version
```

The checked-in `rust-toolchain.toml` records Rust 1.97.1 and the required
components. A direct Homebrew `cargo` does not interpret that file, so
`make bootstrap-check` also validates the active compiler explicitly.

`rustup` is optional. It is useful for contributors who need automatic
toolchain selection, Miri, nightly-only fuzzing, or cross-compilation targets.
Avoid unintentionally mixing Homebrew Rust and rustup proxies: if rustup is
installed, ensure the intended toolchain appears first on `PATH`, confirm it
with `command -v rustc`, and keep the version at 1.97.1 for normal builds.

Install the dependency auditor at the repository-pinned version:

```shell
cargo install cargo-audit --version 0.22.2 --locked
cargo audit --version
```

Make never installs this tool implicitly. `make audit` and `make check` fail
with an installation hint when the executable is missing or has another
version.

Linux CI obtains `protoc` from the upstream v35.1 release with a pinned SHA-256
for each supported runner architecture. The installer requires a new, absolute
destination and supports Linux x86_64 and aarch64 only:

```shell
protoc_destination="${TMPDIR:-/tmp}/epoch-protoc-35.1"
test ! -e "$protoc_destination"
./scripts/install-protoc.sh "$protoc_destination"
export PATH="$protoc_destination/bin:$PATH"
test "$(protoc --version)" = "libprotoc 35.1"
```

It rejects an existing destination, unsupported platform, missing extraction
tools, checksum mismatch, unexpected archive layout, or unexpected compiler
version. It never falls back to an ambient `protoc`. macOS developers continue
to use the Homebrew package validated by `make bootstrap-check`.

## Verify the environment

From the repository root:

```shell
make bootstrap-check
```

The check is deliberately strict for Go, Rust, Maven, Ruff, protoc, and Buf. It
also requires Java 25 and Python 3.11 or newer and rejects a non-LTS Node major.
Docker Desktop must be running for Compose and integration tests, although
compilation and unit tests do not require the daemon.

## Repository-level package managers

Epoch deliberately keeps the native build systems:

- Cargo owns the Rust workspace and committed `Cargo.lock`.
- One Go module initially owns `control`, `operator`, generated bindings, and
  the typed native HTTP client under `sdk/go`.
- The checksum-pinned Maven wrapper owns the Java 25 client under `sdk/java`.
- Python packaging metadata owns the typed client under `sdk/python`.
- pnpm owns `console`, `sdk/typescript`, and browser tooling.
- Buf owns Protobuf linting, breaking-change policy, and generation.
- Make provides memorable root commands without hiding the native commands.

The Go module path `epoch.local/epoch` is explicitly provisional. It prevents
accidental publication under an uncleared organization name. Replace it in one
reviewed migration after the company, repository, domain, and package namespace
are selected; do not publish it as a stable import path.

## Dependency update policy

Dependabot staggers the six package ecosystems across the week and opens at
most one grouped routine version-update pull request per ecosystem after a
seven-day cooldown. Routine Go, Java, Python, npm, and GitHub Actions updates
are limited to SemVer minor and patch releases. Routine Cargo updates are
patch-only because a `0.x` minor change can be API-breaking and the consensus
dependency graph is deliberately pinned. Security updates use a separate group
and are never filtered or delayed by the routine-version policy.

Major migrations, Cargo `0.x` minor migrations, runtime-floor changes, and
coupled toolchain upgrades are maintained on explicit branches. They require a
compatibility note and the same language, integration, container, and
documentation gates as product code. Dependabot pull requests are not
auto-merged; a green check on one dependency does not prove that a collection
of independently generated updates is compatible.

## Common commands

```shell
make help             # list commands
make format           # update source formatting
make format-check     # verify formatting without writes
make generate         # regenerate Go Protobuf bindings
make lint             # Rust, Go, Java, Python, TypeScript, and Protobuf checks
make audit            # pinned Rust dependency advisory gate
make test             # local unit tests
make test-integration # real processes plus exact published SDK quickstarts
make build            # compile all current components
make package-shape    # nonpublishing package and clean-consumer checks
make check            # normal pre-commit gate
make ci               # local deterministic CI gate
```

`make check` includes `make audit`; it therefore requires the pinned
`cargo-audit` installation above. The Make targets remain thin wrappers. When
debugging a failure, rerun the native command printed by Make rather than adding
behavior that exists only in the wrapper.

`make package-shape` proves only that the declared private/package boundaries
and local artifacts are mechanically sound. It is neither a registry dry-run
nor publication approval: it has no publishing identity and deliberately fails
to make a release claim while licensing, permanent coordinates, signing, and
registry ownership remain unresolved. See
[Package publication](PACKAGE_PUBLICATION.md) for the promotion contract.

## Protobuf contracts

Place versioned contracts under:

```text
spec/proto/epoch/<surface>/v1/*.proto
```

Public data APIs, Rust regional administration APIs, and internal Go fleet APIs
must use separate packages even when they share common messages. Wire packages
are versioned from their first commit. Never expose Rust or Go implementation
types as a persisted or network contract.

The initial `buf.gen.yaml` generates Go message and gRPC bindings into
`sdk/go/gen` with pinned remote plugins. Generation therefore requires network
access to the Buf Schema Registry. Rust bindings remain owned by the Rust
workspace build/`xtask` path so tonic/prost versions stay aligned with Cargo.

Run:

```shell
make generate
buf lint
```

Generated output is reviewed and committed. Do not edit it manually. CI runs
generation and rejects a diff. Before a public API is released, CI must also run
Buf's breaking check against the default branch or a published registry label.

## Runtime configuration and local ports

The initial standalone development contract is:

| Setting | Default |
| --- | --- |
| Deployment mode | `standalone` |
| Native gRPC address | `0.0.0.0:7600` |
| Native/admin HTTP address | `0.0.0.0:7601` |
| Metrics address | `0.0.0.0:9464` |
| Data directory in the image | `/var/lib/epoch` |
| Browser origins | local Vite dev/preview on `127.0.0.1` and `localhost` |

The first node exposes its implemented native and administrative HTTP routes on
7601. Port 7600 is reserved for the native gRPC service as contracts land.
Health endpoints are `/healthz` and `/readyz`; metrics are reserved on 9464 and
will use `/metrics` when the exporter lands.

Browser access is restricted to a comma-delimited exact-origin allowlist. Set
`--allowed-origins` or `EPOCH_ALLOWED_ORIGINS` when serving the local console
from another HTTP(S) origin; paths, query strings, credentials, opaque origins,
and wildcards are invalid. Origins are canonicalized before matching. This
setting does not authenticate native clients and must not be used as a network
security boundary.

For a fresh data directory, the standalone node stores its active engine
journal in `$EPOCH_DATA_DIR/engine-wal/`. Files are named `segment-*.wal`; the
default development paths therefore begin at `.epoch/engine-wal/` locally and
`/var/lib/epoch/engine-wal/` in the development image. `engine.wal` is a
versioned activation marker and the cross-version writer lock. The segmented
directory also contains a writer lock, a durable WAL identity, and the manifest
that defines committed segment topology.

The active segment rotates before ordinary growth crosses the configured
threshold. The default is 64 MiB and can be changed with
`--wal-segment-bytes` or `EPOCH_WAL_SEGMENT_BYTES`; this is a rotation setting,
not retention, compaction, or a disk quota. Frames are never split, so one frame
larger than the threshold may occupy an otherwise empty segment. All segments
use checksummed v1 WAL frames and one global, contiguous sequence. The
checksummed `manifest.v1` records each segment's exact committed length, ending
sequence, and whole-file checksum. Startup rejects missing or unexpected files,
gaps, reordered segments, truncated committed data, invalid frames, foreign
metadata, and checksum corruption. It may discard only an uncommitted suffix
beyond the active segment's manifested length; extra bytes in a sealed segment
fail startup.

Streams and Queues configured as `local_durable` use this journal; volatile
resources are intentionally absent after restart. Queue commands include
enqueue, acquire, settlement, redrive, and time-driven maintenance so recovered
lease tokens and retry state remain deterministic.

`$EPOCH_DATA_DIR/engine.wal` was also the earlier single-file WAL location. A
pre-existing valid legacy WAL stays in that layout: the current node locks,
replays, repairs only an incomplete crash tail, and continues appending to the
same file. It does not create segmented history, so an offline downgrade remains
safe. Automatic conversion is intentionally deferred until Epoch has an
explicit migration and rollback protocol.

On a fresh installation, Epoch first writes a staging marker that an older
single-file-only binary cannot parse, creates and syncs the segmented journal,
then atomically installs the active marker. Do not modify either marker or mix a
legacy file with `engine-wal/`; ambiguous histories fail startup instead of
guessing. A second old or new process is rejected by the shared lock.

This milestone has no snapshots, compaction, retention deletion, object tier,
replication, or replica repair. Losing the host and its storage can still lose
all locally durable data.

## Containers

Validate Compose without starting a daemon workload:

```shell
make compose-config
```

Start the standalone node:

```shell
make compose-up
docker compose -f deploy/compose/docker-compose.yml logs --follow epoch-node
```

Stop it while retaining the named data volume:

```shell
make compose-down
```

Validate or run the opt-in fixed-three-voter consensus probe:

```shell
make compose-probe-config
make compose-probe-up
make compose-probe-down
```

Its three public profile endpoints are still standalone. The separate local
diagnostic listeners replicate opaque probe bytes only; see
[Experimental Consensus Probe](CONSENSUS_PROBE.md) before using them.

To discard local data, explicitly add `--volumes` to the Compose down command.
That is destructive and is intentionally not part of the Make target.

## Code boundaries

- `epoch-core` remains I/O-free and must not acquire Tokio, protocol, storage,
  or cloud dependencies.
- Gateways authenticate, authorize, validate, normalize, and route. They own no
  durable state.
- Profile engines use narrow storage and consensus interfaces and do not call
  one another. Pipes are explicit cross-profile resources.
- Volatile cache operations do not append to the durable log unless the resource
  selects durability or change capture.
- Go reconciles desired state through versioned Rust administration APIs. It
  never opens a node data file.
- Time-dependent logic receives an injected monotonic/deterministic clock.
- Runtime configuration selects enabled roles and profiles. Official semantics
  must not depend on undocumented Cargo feature combinations.

The workspace forbids unsafe Rust. A future low-level exception requires an ADR
with stated invariants, a narrowly scoped crate, fuzz/property coverage, Miri
where applicable, and security review.

## Local data and secrets

Runtime state belongs under `.epoch`, `data`, or a configured external path;
these root directories are ignored by Git. Do not put data under a source crate.
Never commit `.env`, private keys, certificates, webhook secrets, connector
credentials, payload dumps, or production traces. Examples use `.env.example`
with non-secret values.

## Troubleshooting

### The wrong Rust compiler is active

```shell
command -v rustc
rustc --version
command -v cargo
cargo --version
```

Both commands must resolve to the same Homebrew or rustup toolchain and report
1.97.1.

### `make bootstrap-check` reports Node 26

Prepend Homebrew Node 24 for the current shell:

```shell
export PATH="/opt/homebrew/opt/node@24/bin:$PATH"
hash -r
make bootstrap-check
```

If the keg-only Node 24 binary fails to load a Homebrew library (for example,
an older `libsimdjson` soname), refresh that keg before changing `PATH`:

```shell
brew upgrade node@24
```

If Homebrew reports it current but the dynamic-link error remains, use
`brew reinstall node@24`. This repairs the keg against current Homebrew
dependencies; do not work around it with a hand-made library symlink.

### Docker cannot access its socket

Start Docker Desktop and verify the selected context:

```shell
docker context show
docker info
```

Do not change socket permissions broadly. Fix the Desktop/context state.

### Buf cannot generate

Confirm `buf --version`, network access to `buf.build`, and that at least one
contract exists under `spec/proto`. `make generate` safely skips an empty
contract tree during the initial scaffold.

## Related documents

- [Architecture](ARCHITECTURE.md)
- [Testing](TESTING.md)
- [Delivery plan](DELIVERY_PLAN.md)
- [Requirements traceability](REQUIREMENTS_TRACEABILITY.md)
- [Product requirements](PRD.md)
