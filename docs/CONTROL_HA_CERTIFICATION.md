# Concurrent control-plane failure certification

Status: bounded runtime matrix locally verified; protected exact-head/main
execution and beta.12 publication remain open.
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

Unit results alone do not prove live multi-controller failover. The captured
live matrix and its independently verified artifacts are described below;
protected exact-head/main evidence is still required before promotion.

### Owner-recovery fixture

`tests/integration/control_ha.py` adds a dedicated first part of that matrix:
three and five concurrent Go processes, authenticated HTTP writes/replays,
canonical materialization, an overlapping same-label replacement, actual lease
owner `SIGKILL`, owner `SIGSTOP`/takeover/resume, four stale-guard commands,
Catalog majority loss, original-token resolution, and same-volume reopen with
all four profile digests. Its twelve fail-closed contract tests pass
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

### Generated-client recovery fixture (candidate)

`tests/integration/control_ha_api.py` extends the owner fixture through the
generated Go gRPC bindings. It issues identical 128-resource batches through
every controller concurrently, races distinct two-resource batches on one OCC
generation, and checks every resulting resource for partial mutation. It keeps
the exact original request and operation protobufs, including rejected batches,
command kinds, affected identities, and optional delete precondition presence.
The same records are checked through surviving controllers after owner loss,
quorum recovery, and all-voter/controller reopen.

The candidate also exercises scoped-reader operation lookup, partial and mixed
affected-resource sets, missing credentials, completed/missing delete retries
against recreated resources, and filtered two-item watches that disconnect and
resume through a different controller's scanned cursor. Recurrent fenced
status commands advance the actual retained history without claiming unlimited
public-token retention; gRPC must explicitly reject cursor zero after the
observed floor advances. This uses `epoch.control-ha.api-recovery/v1`; it still
does not certify in-flight Catalog-leader unknown outcomes or production limits.
Local Go race and fixture contract suites pass. These generated-client cases
are now locally verified within the complete matrix below; the standalone
API-only runner and its schema still do not certify Catalog-leader unknown
outcomes or production limits.

The clean `0f585ed` API run passed all eight generated-client checks with
three controllers at preparation, after owner failure, after quorum recovery,
and after all-voter/controller reopen. Seventeen exact operation protobufs and
135 desired resources were retained across those phases. The campaign then
failed on its two-second fixture HTTP deadline near the 4,096-entry history
boundary; it did not reach the stale-cursor assertion or the five-controller
case. These partial results are not a passing API campaign.

```sh
EPOCH_REGIONAL_IMAGE=epoch/node:ha-candidate \
EPOCH_REGIONAL_USE_EXISTING_IMAGE=1 \
EPOCH_CONTROL_HA_ARTIFACT_DIR=/absolute/empty/api-evidence-directory \
make test-control-ha-api
python3 tests/integration/control_ha_api.py verify \
  --manifest /absolute/empty/api-evidence-directory/evidence.json
```

### Complete bounded matrix fixture (candidate)

`tests/integration/control_ha_full.py` combines the owner and generated-client
cases with real Catalog-leader loss while three or five caller requests remain
pending. A private, loopback-only test proxy forwards each original command to
its assigned real Go controller, observes successful upstream commits, and
holds every caller response without sending headers. The driver identifies and
stops the real Catalog leader, verifies its container is stopped while all
callers are still pending, and closes only those owned response sockets.
Callers observe an unknown network outcome, never a fabricated HTTP status.
Every exact original token is then replayed through every controller and bound
to its fully qualified generated-client `GetOperation` request and durable
outcome. Those exact protobuf witnesses survive the subsequent owner, quorum,
and reopen faults.

This fault is the committed/lost-ack branch of an unknown outcome. It does not
claim to interrupt every Raft proposal position; majority-loss refusal remains
a separate case. An unchanged token is never rebound to a changed command.
The independent verifier checks the original command hashes, exact scope and
token lookup plan, all generated lookup witnesses, and every recovery phase.
Its schema is `epoch.control-ha.full-certification/v1`, which means only the
bounded matrix below—not production certification or PRD completion.

The retention fixture now uses a twelve-second native HTTP deadline and a
cached leader hint; every response still passes through the authority's strong
read or commit path. Typed `not_leader` responses invalidate that hint.
Ambiguous sends retry the identical command, lease, clock value, and token at
most four times. Only a resolved, typed fencing rejection permits a separate
attempt with a newly observed guard and token. This does not assert a two-second
service SLO or widen the public operation-retention contract.

All 35 HA fixture contract tests, the complete Go race suite, vet, and build
pass locally. CI has a separately bounded 120-minute
native-arm64 HA job that consumes the already inspected exact-source node image
from the arm64 build job, verifies its archive checksum and OCI revision, and
retains passing or failed evidence for 30 days. It neither rebuilds that Rust
image nor publishes it to a registry.

### Locally verified bounded matrix — 5 October 2026

The clean `19e02175bd5c4970236a84fedd7eeb818197b6b6` capture completed every
runtime scenario for both controller counts, including actual history pruning
and generated gRPC stale-cursor rejection. The original CLI then exited with
an evidence-reader error: the request artifact is an array, but the reader
required an object. It must not be represented as a successful original run.

The regression was reproduced before correction in `e2e46e6`. That correction
adds a distinct array reader without weakening object-only manifest readers,
retains nested duplicate-key rejection, and moves complete verification inside
the campaign's failure handling. Future verification failures rewrite both
the manifest and failure record as failed instead of leaving a passed manifest.
It does not change the captured Go/Rust production code or generated client.

Independent verification with the corrected reader now passes against the
unchanged captured manifest and all 32 checksum-bound artifacts. The original
source identity and bytes are preserved; this is a reverified capture, not a
claim that the runtime was rerun at the verifier-fix revision.

| Observation | Three controllers | Five controllers |
|---|---:|---:|
| Owner/recovery checks | 10/10 | 10/10 |
| API/recovery checks | 11/11 | 11/11 |
| Pending callers when the real Catalog leader was stopped | 3 | 5 |
| Exact retained operation witnesses | 20 | 24 |
| Desired resources checked across recovery phases | 135 | 135 |
| Observed history floor / latest cursor | 29 / 4,124 | 29 / 4,124 |
| Exact profile digests before/after all-voter reopen | 4/4 | 4/4 |

The canonical manifest SHA-256 is
`0e4d3ffcfebd283002fbabe691430fc3b93187c80a2f47390a7ed8aa4e2d8e0d`.
The node image revision is `fed6b59`, with image ID
`sha256:1ee8ef6f46297f0071174e5661cc78ab49b8a7cf6798018d7cdc17dc74c94a64`;
the campaign checked that its Rust production source matched the capture.
Separate receipts identify the controller and generated-client binaries.
The fresh protected CI campaign must still run and independently verify the
current exact source. No public retention, clock-skew, mixed-version, scale,
long-soak, or production-SLO gate is inherited from this bounded local proof.

```sh
EPOCH_REGIONAL_IMAGE=epoch/node:ha-candidate \
EPOCH_REGIONAL_USE_EXISTING_IMAGE=1 \
EPOCH_CONTROL_HA_ARTIFACT_DIR=/absolute/empty/full-evidence-directory \
make test-control-ha-full
python3 tests/integration/control_ha_full.py verify \
  --manifest /absolute/empty/full-evidence-directory/evidence.json
```

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
