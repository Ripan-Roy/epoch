# Dependency security and temporary source patches

Security fixes are checked against the current advisory databases, not inferred
from an older green run. `make audit` retains the sole documented Raft/fxhash
exception (`RUSTSEC-2025-0057`); this change adds no exceptions and disables no
compression codec or MySQL binlog feature.

## Zstandard migration

The October 26 audit found four vulnerabilities and ten denied unsoundness
warnings in the old Zstandard graph. The complete resolved graph now uses
`zstd 0.14.3`, `zstd-safe 8.2.0`, and `zstd-sys 2.1.2+zstd.1.5.7`.
Updating the workspace requirement alone is insufficient: Kafka record batches
and MySQL compressed binlog transactions also depend on Zstandard.

| Consumer | Temporary migration | Removal condition |
| --- | --- | --- |
| Native Stream and Kafka gateway | Workspace minimum `zstd 0.14.3` | Keep the security floor when updating the codec. |
| `kafka-protocol 0.18.0` | Pin upstream commit `7faf833aeb152d3b92f9bd50074a764c674f17b5`, which accepts Zstandard 0.14. | A reviewed published release accepts the patched graph and passes the same contracts. |
| `mysql_async 0.37.1` binlog decoder | Patch `mysql_common 0.37.3` from a verified local source copy; change only the normalized manifest's Zstandard requirement from `0.13` to `0.14.3`. | A reviewed published MySQL dependency resolves only the patched graph and passes source conformance. |

The pinned [Kafka upstream change](https://github.com/kafka-protocol-rs/kafka-protocol-rs/commit/7faf833aeb152d3b92f9bd50074a764c674f17b5)
also includes the preceding record-count reservation guard and CRC implementation
migration since the published crate. The existing Kafka suite and a fixed
pre-upgrade record-batch vector protect those wire boundaries. The dependency
is pinned by full immutable revision, not a moving branch.

## MySQL source provenance

The [official crate archive](https://static.crates.io/crates/mysql_common/mysql_common-0.37.3.crate)
has SHA-256
`0f27695f286b461da077b8c2f72f47feaa04ce3c3f9c0976257410e90e21208a`.
It records upstream source revision `fc5ce466e0d471e8974a00f93654cf9aac4891dc`.
`third_party/mysql_common-0.37.3` retains all 83 files except the unused upstream
`Cargo.lock` and `.gitignore`. Both licenses and `Cargo.toml.orig` are unchanged.
No MySQL Rust source is modified.

`EPOCH_SOURCE.json` records every original file hash. The offline verifier pins
the archive identity, complete inventory digest, and approved patched-manifest
digest independently of that JSON. Editing a source file and its recorded hash
together therefore fails. Missing/extra files, symlinks, changed licenses,
manifest rollback, and any other manifest edit also fail.

```shell
make test-dependency-security
make audit
```

`--record-upstream ARCHIVE` regenerates provenance only after checking the exact
official archive checksum; it never extracts files or downloads data. Normal
verification is offline and non-mutating. The Rust CI job verifies source before
compilation; the contract job and local unit/audit gates run all security
contracts. Both Rust Docker builders copy and verify the same source before
compilation. The patch is excluded from workspace membership, not from
dependency compilation or source-integrity checking.

## Verification and remaining gates

Fixed vectors captured before the migration exercise historical Stream v2
command decoding, application, snapshot recovery and exact replay; Kafka
Zstandard records including key/value/header/timestamp/offset preservation; and
the transitive MySQL transaction-payload decoder including truncated input.
All advertised-codec tests remain enabled. These tests do not reproduce every
upstream advisory or replace real-server conformance.

The disposable MySQL fixture enables transaction compression. Its ignored
source-adapter test requires a real `TRANSACTION_PAYLOAD_EVENT`, decoded row
markers and terminal XID, the physical end checkpoint, a fresh-adapter EOF,
and a later compressed transaction with deterministic replay. Existing source
conformance CI runs this test; a compiled or ignored test alone is not live
evidence.

RabbitMQ Java conformance is upgraded to `5.37.0` with synchronized assertions,
banners, evidence metadata and current documentation. Previous-version evidence
is rejected. Fresh fixture, real-regional recovery, source-connector and
three/five-controller CI evidence remain required for the combined candidate.
Do not close superseded Dependabot PRs before the equivalent update lands on
`main`, or publish a tag using an earlier commit's passing checks.
