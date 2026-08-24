# Alpha-exit delivery checklist

This table is the acceptance contract for the single alpha-exit feature PR.
Rows become complete only with local and protected evidence; implementation
alone is not enough.

`🟡` means the candidate has local implementation/evidence but still needs the
exact protected pull-request or tag run; `⬜` means the gate has not started.

The beta.2 source candidate passed exact-main
[CI](https://github.com/Ripan-Roy/epoch/actions/runs/32729208065) and
[Pages](https://github.com/Ripan-Roy/epoch/actions/runs/32729207964) at commit
`69b21ca932f3b5d8b93d90289739d302d6e0be92`. Its tag workflow timed out only in
the emulated arm64 node-image publication step, so artifact publication and the
final beta release gate remain open for beta.3.

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
| AE-12 | OCI/SBOM/provenance release | PR image inspection plus tag-only GHCR publication and verifiable attestations | 🟡 |
| AE-13 | Load/fault/soak harness | Resumable workload, invariant checks, evidence manifest, accelerated CI profile | ✅ |
| AE-14 | Live Kubernetes campaign | Install, all-profile traffic, backup, replace, upgrade, restore, digest comparison | ✅ |
| AE-15 | Documentation and traceability | Architecture, security, operations, SDK, PRD, ADR, checklist, and public Pages agree | ✅ |
| AE-16 | Beta release gate | Full local matrix, one protected PR, exact-main CI/Pages, verified tag, curated beta notes | ⬜ |

## Pull-request rule

Do not open the alpha-exit PR while any AE-01–AE-15 row lacks local evidence.
Do not merge it while protected CI or Pages is incomplete. AE-16 closes only
after the exact-main tag and release are verified.
