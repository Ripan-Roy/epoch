# Concurrent control-plane failure certification

Status: implementation in progress on the control-HA hardening branch.
The existing regional and Kubernetes campaigns do not close this gate: their
control restart evidence is not a concurrent-controller chaos campaign.

## Ownership boundary

Rust Catalog consensus remains the authority for managed metadata, leases,
fences, native resource placement, and exact operation outcomes. Each Go
control registry is a replaceable client. Production registry construction
now derives a distinct owner from the stable instance label and a fresh random
128-bit nonce; deterministic unit fixtures may still inject an exact owner.

The stable label may be reused on a hostname, pod restart, or replacement.
The derived owner must not be reused. A replacement waits out an unexpired
foreign lease before acquiring a new fence. This uses existing command and
snapshot formats; it neither rewrites durable tokens nor creates a new
metadata authority.

## Current verification

The new regression first reproduced identical owners for two production
registries with the same instance label. It now passes with tests for maximum
label length, canonical bounded ownership, exact nonce bytes, insufficient
entropy, invalid labels, and refusal to adopt or challenge the predecessor's
live lease. The complete Go race suite, vet, and build pass locally.

These unit results do not prove live multi-controller failover. The acceptance
matrix below is still required before the feature PR is ready.

### Owner-recovery fixture

`tests/integration/control_ha.py` adds a dedicated first part of that matrix:
three and five concurrent Go processes, authenticated HTTP writes/replays,
canonical materialization, an overlapping same-label replacement, actual lease
owner `SIGKILL`, owner `SIGSTOP`/takeover/resume, four stale-guard commands,
Catalog majority loss, original-token resolution, and same-volume reopen with
all four profile digests. Its eleven fail-closed contract tests pass
locally and are required in CI. The clean `671fde4` candidate passed all ten
owner-recovery checks with both three and five Go controllers. Independent
verification checked all fourteen artifact receipts and exact before/after
digests for Cache, Stream, Queue, and Event Bus. The node image came from
`fed6b59`; the driver verified identical Rust production source and separately
recorded the current Go binary digest. Protected exact-head/main execution is
still required.

The first live run also exposed a management HTTP gap: qualified create existed
but item GET/DELETE only parsed the legacy three-segment key. The candidate
adds six-segment item parsing, requires all tenant segments, preserves exact
scope authorization/deletion coordination, and retains the legacy route.
Regression tests reproduced the missing GET and tenant-authorization path
before the fix; the owning package race suite passes. The fixture preserves
Go's `event_bus` and native Rust's `event-bus` spellings at their boundaries.

Receipt retention is not a fencing guarantee. Catalog intentionally retains
only its newest eight recurring lease/status/materialization/membership
receipts. The fixture verifies persistent user apply and managed-delete
outcomes across reopen, then submits fresh attempts with the old guard to
prove all four fences survived. It does not advertise indefinite internal
operation lookup or expand the public request-token retention promise.

The fixture deliberately uses `epoch.control-ha.owner-recovery/v1`, not a full
certification schema. It does not close the gRPC batch/OCC, operation-lookup
authorization/precondition, watch/resume/stale-cursor, Catalog-leader unknown
outcome, or delete/recreate rows. These must be added and verified before the
dedicated CTRL-001/CTRL-002 chaos gate can close.

Run from a clean candidate tree with a revision/version-labelled node image:

```sh
EPOCH_REGIONAL_IMAGE=epoch/node:ha-candidate \
EPOCH_REGIONAL_USE_EXISTING_IMAGE=1 \
EPOCH_CONTROL_HA_ARTIFACT_DIR=/absolute/empty/evidence-directory \
make test-control-ha-owner
python3 tests/integration/control_ha.py verify \
  --manifest /absolute/empty/evidence-directory/evidence.json
```

The driver checks that the image revision has identical Rust production source,
then records the separate current source revision and built Go binary digest.
It rejects dirty/drifting source, captures credential-redacted logs, verifies
artifact receipts, and always resumes a paused owned child before shutdown.

### Recovery regression evidence

The clean `fed6b59` candidate's rebuilt arm64 node image passed the existing
accelerated regional campaign in 59,354 ms. Its independently verified signed
bundle covered all eight regional invariants, all four profiles, control loss,
five profile-leader losses, and all-voter same-volume reopen. This local result
addresses the release-commit Stream catch-up failure; it is not protected-main
proof and is not a concurrent-controller certification result. Beta.12 remains
untagged until the fixed exact-main CI, Pages, and release gates pass.

## Required live matrix

| Case | Fault or workload | Required invariant |
|---|---|---|
| Concurrent APIs | Three and five Go controllers; desired writes and reads through every endpoint | Every acknowledged desired generation is durably visible through every replica; native materialization has one canonical generation |
| Exact retries | Concurrent identical tokens, then conflicting bytes | One retained outcome, exact replay, explicit conflict, no duplicate mutation |
| Atomic batches | Simultaneous gRPC batches with conflicting OCC | No partial desired state, exact affected-resource authorization, durable success or rejection |
| Operation lookup | Resolve successful and rejected tokens through all controllers | Stable command kind, affected identities, caller precondition presence, and outcome across failover |
| Watches | Authorized gRPC streams disconnect during mutations and reconnect from scanned cursor | No skipped matching changes, no cross-tenant disclosure, explicit stale-cursor failure |
| Active owner loss | SIGKILL the real lease owner while standby APIs remain live | New owner/fence after expiry, continued data paths, no acknowledged metadata loss |
| Paused old owner | SIGSTOP owner, allow takeover, resume stale owner | Old guard is fenced and cannot publish status, reserve capacity, plan membership, or delete |
| Reused instance label | Start a replacement while its predecessor is alive | Distinct process owners; replacement cannot adopt the live lease |
| Catalog leader loss | Kill a Catalog voter leader during concurrent control requests | Unknown outcomes resolve by original token; no success without quorum |
| Quorum loss | Stop a Catalog majority | Strong operations fail explicitly; restoration preserves acknowledged outcomes |
| Durable reopen | Kill and reopen all voters on the same volumes, then restart controllers | Managed generations, tombstones, operation bindings, and all four profile digests survive |
| Delete/recreate | Race exact delete retries with a new generation | An old missing or completed delete cannot remove the new incarnation |

Use a dedicated temporary Compose project and explicit child-process handles.
Never stop unrelated containers. Every failure must resume a paused child
before bounded shutdown, retain credential-redacted logs and evidence, and
clean up only the campaign's own workloads.

## Evidence and promotion

The campaign must bind its result to a frozen source revision and relevant
source hashes, record each injected fault and observed lease/fence transition,
retain per-invariant pass/fail results, and verify its artifact checksum
manifest. Unit tests must reject missing, false, or malformed evidence fields.

Only passing local and protected exact-head/main campaigns may update the
CTRL-001/CTRL-002 dedicated-chaos gate. This matrix makes no claim about a
30-day operating soak, horizontal Catalog sharding, terminal-seal migration,
clock-skew tolerance, mixed-version rollback, security penetration testing,
cloud CSI, or production SLOs.
