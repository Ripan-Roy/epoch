# Alpha-exit delivery checklist

This table is the acceptance contract for the single alpha-exit feature PR.
Rows become complete only with local and protected evidence; implementation
alone is not enough.

`🟡` means the candidate has local implementation/evidence but still needs the
exact protected pull-request or tag run; `⬜` means the gate has not started.

Last reviewed: 5 October 2026. The bounded alpha-exit baseline is delivered in
the published [beta.11 release](https://github.com/Ripan-Roy/epoch/releases/tag/v0.2.0-beta.11)
at `f2c5381a6aa39b1b6cfc661fd2bd7038a9d6a32b`.
[Exact-main CI](https://github.com/Ripan-Roy/epoch/actions/runs/34781777502)
passed all eleven jobs, including live Kubernetes lifecycle and all-profile
recovery. [Pages](https://github.com/Ripan-Roy/epoch/actions/runs/34781777485)
and [tag verification](https://github.com/Ripan-Roy/epoch/actions/runs/34788472020)
also passed. Five signed multi-platform OCI manifests and ten platform SBOM
release assets were published. The earlier beta.2 emulated-arm64 timeout is
historical; native amd64/arm64 builds resolved that publication dependency.

This closes only this bounded alpha-exit acceptance contract. It does not close
the full PRD's private/public-beta milestones, long-duration operating evidence,
independent clean-cluster digest-pull drill, or production readiness. Beta.12
is a separate, untagged control-HA release candidate: its release-commit
[CI](https://github.com/Ripan-Roy/epoch/actions/runs/37189330373) failed Stream
follower recovery, and the hardening branch must pass the complete concurrent
controller matrix plus protected exact-head/main checks before publication.
See [delivery gates](DELIVERY_CHECKLIST.md) and
[control-HA certification](CONTROL_HA_CERTIFICATION.md).

| ID | Deliverable | Required evidence | State |
|---|---|---|---:|
| AE-01 | Public TLS and peer/control mTLS | Startup-failure, hostname, untrusted-client, rotation, and three-process recovery tests | ✅ |
| AE-02 | Workload identity in Kubernetes | Secret validation, secure endpoint rendering, least-privilege mounts, and live handshake | ✅ |
| AE-03 | SDK and CLI secure transport | Go, Java, Python, and CLI custom-CA/client-certificate tests and executable docs | ✅ |
| AE-04 | Versioned regional backup | Quorum barrier, bounded manifest, canonical checksums, atomic publication, and tamper tests | ✅ |
| AE-05 | Fresh-cluster restore | Reject non-empty state; restore all profiles; compare canonical digests after restart | ✅ |
| AE-06 | Scheduled operator backup | Idempotent schedule, encrypted destination policy, status/retention, and failure recovery | ✅ |
| AE-07 | Guarded rolling upgrade | Fresh-backup gate, leader drain, one voter at a time, catch-up gate, stop/rollback conditions | ✅ |
| AE-08 | Joint-consensus membership | Durable three/five-voter configuration, learner promotion, replacement, fencing, and reopen | ✅ |
| AE-09 | Object-storage source | Bounded poll/list/read, stable positions, duplicate-safe failover, lag/status, and replay | ✅ |
| AE-10 | PostgreSQL/MySQL CDC sources | Transaction/LSN or binlog positions, schema/error routing, failover, and replay | ✅ |
| AE-11 | Kafka source | Partition/offset positions, group fencing, record-before-offset checkpoint, failover, and replay | ✅ |
| AE-12 | OCI/SBOM/provenance release | PR image inspection plus tag-only GHCR publication and verifiable attestations; published beta.11 tag workflow `34788472020` | ✅ |
| AE-13 | Load/fault/soak harness | Resumable workload, invariant checks, evidence manifest, accelerated CI profile | ✅ |
| AE-14 | Live Kubernetes campaign | Install, all-profile traffic, backup, replace, upgrade, restore, digest comparison | ✅ |
| AE-15 | Documentation and traceability | Architecture, security, operations, SDK, PRD, ADR, checklist, and public Pages agree | ✅ |
| AE-16 | Beta release gate | Full local matrix, protected integration, exact-main CI `34781777502` / Pages `34781777485`, verified tag `34788472020`, and published beta.11 notes | ✅ |

## Pull-request rule

Do not open the alpha-exit PR while any AE-01–AE-15 row lacks local evidence.
Do not merge it while protected CI or Pages is incomplete. AE-16 closes only
after the exact-main tag and release are verified. A later release candidate
must repeat its own gates; it cannot inherit green checks from beta.11.
