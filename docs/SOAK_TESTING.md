# Resumable load, fault, and soak evidence

Epoch's soak runner repeatedly executes the real three-node regional campaign,
not a synthetic sleep loop. Every successful round writes through Cache,
Stream, Queue, and Event Bus, exercises the Go control plane and Python SDK,
injects process failures, and proves state convergence after recovery.

The runner is an evidence mechanism, not permission to claim an SLO. The
accelerated profile proves the harness in CI. Only the `thirty-day` profile can
satisfy the PRD's elapsed private-alpha operating gate, and even that evidence
does not establish a managed-service availability or latency SLO.

## What one round proves

The regional driver emits `epoch.regional-runtime.evidence/v1` only after all of
these fault boundaries and invariants pass:

| Boundary | Required observation |
|---|---|
| Control-plane `SIGKILL` | Durable desired state and exact retry identity replay |
| Stream leader `SIGKILL` | Higher-term leader, SDK state/session/batch continuity, follower catch-up |
| Queue and advanced Queue leader `SIGKILL` | Lease/settlement, session/DLQ state, and retry continuity |
| Cache leader `SIGKILL` | Typed state, fencing, backup/PITR/query, and digest continuity |
| Event Bus leader `SIGKILL` | Publish/delivery/integration state and idempotent retry continuity |
| All voter `SIGKILL` and reopen | Exact Catalog digest, all profile state, and automatic checkpoints recover |

The driver must explicitly report all four profiles, every named fault, and
these invariant booleans: Catalog digest preservation, profile convergence,
managed-intent replay, higher-term fencing, idempotent retry preservation, and
automatic-checkpoint reopen. Missing, false, duplicate, or unknown-shape
evidence fails the round.

Faults are sequential, not overlapping. Docker health proves only that a
process can serve HTTP; before the next leader is killed, the driver separately
requires the restarted voter to match the exact applied-command count and state
digest for the affected Raft groups. Ordinary observations retain a 30-second
deadline. Restart catch-up has a separate bounded 90-second deadline, and a
failure records every node's final HTTP status, applied count, and digest.

## Evidence model

`tests/soak/epoch_soak.py` checkpoints `state.json` atomically after a round
starts and after it passes or fails. A restart converts an unfinished `running`
attempt to `interrupted`, hashes any partial artifacts, and retries the same
round with a new attempt number. Completed rounds are never rerun.

Resume is rejected if any of these change:

- campaign plan and its canonical SHA-256 digest;
- Git revision, synchronized Epoch version, clean/dirty state, or complete
  tracked/untracked source-tree hash;
- selected regional image identity, OCI version, or OCI revision;
- runtime operating system, machine architecture, or Python version.

For each attempt, the runner retains the regional JSON receipt, combined driver
log, and failure artifacts. Every regular file has a relative path, byte count,
and SHA-256 receipt. Symlinks, absolute paths, traversal, duplicate JSON keys,
NaN values, and files outside the evidence directory fail closed.

On completion, `evidence.json` is canonical JSON and contains the plan,
identity, attempts, artifact receipts, event-log receipt, total active campaign
time, failure count, and campaign-runtime distribution. It explicitly records
that no managed-service SLO or production certification is claimed. The runner
signs the exact manifest bytes with Ed25519 and immediately verifies the
signature and every artifact before returning success.

The output is published atomically in this order: public key, verified signature,
then manifest. The manifest is the final completion marker, so a power loss
during signing remains resumable rather than looking complete.

## Accelerated local or CI campaign

Keep the private key outside the evidence directory. CI creates an ephemeral
key on its temporary runner and uploads only the public key, signature,
manifest, state, logs, and results. The runner rejects symlink/non-file keys,
group/world-readable key modes, and any key located under the evidence tree.

```bash
export EPOCH_SOAK_DIR="$(mktemp -d)"
export EPOCH_SOAK_KEY="$(mktemp)"
rm -f -- "$EPOCH_SOAK_KEY"

python3 tests/soak/epoch_soak.py keygen --output "$EPOCH_SOAK_KEY"

EPOCH_REGIONAL_IMAGE=epoch/node:alpha-exit \
EPOCH_REGIONAL_USE_EXISTING_IMAGE=1 \
python3 tests/soak/epoch_soak.py run \
  --profile accelerated \
  --state-dir "$EPOCH_SOAK_DIR" \
  --signing-key "$EPOCH_SOAK_KEY"

python3 tests/soak/epoch_soak.py verify \
  --manifest "$EPOCH_SOAK_DIR/evidence.json" \
  --public-key "$EPOCH_SOAK_DIR/evidence-public.pem"
```

The accelerated profile requires exactly one complete real fault round. Its
manifest sets `accelerated_harness_only: true` and cannot be presented as the
30-day gate.

## Thirty-day campaign and resumption

Use a controlled Ed25519 key whose public-key fingerprint is recorded before
the campaign. Store the key in a secret manager or protected runner mount; do
not put it in the repository, evidence directory, container image, shell
history, or uploaded artifact.

```bash
python3 tests/soak/epoch_soak.py run \
  --profile thirty-day \
  --state-dir /evidence/epoch-thirty-day \
  --signing-key /run/secrets/epoch-soak-ed25519.pem
```

The target is 2,592,000,000 milliseconds of accumulated successful round
runtime. Offline time, failed attempts, and interrupted attempts do not count.
Stop the process normally for maintenance and run the same command to resume.
The source, image, plan, and platform identity must remain exact. A deliberate
upgrade starts a new campaign; evidence from different builds is not combined.

For scheduled windows, `--round-budget N` checkpoints after at most `N` new
rounds and exits with code 75 while incomplete. This option cannot reduce the
profile's required duration or produce a signed completion manifest early.

## Independent verification

Distribute the trusted public-key fingerprint separately from the evidence
bundle. Anyone holding an untrusted bundle can replace both its manifest and
its embedded public key, so verifying only the embedded key is not an identity
claim.

```bash
openssl pkey \
  -pubin \
  -in /trusted/epoch-soak-public.pem \
  -outform DER \
  | shasum -a 256

python3 tests/soak/epoch_soak.py verify \
  --manifest /evidence/epoch-thirty-day/evidence.json \
  --public-key /trusted/epoch-soak-public.pem
```

Verification checks the Ed25519 signature, canonical JSON, public-key
fingerprint, plan digest, target rounds/duration, passed invariant set, event
log, every attempt artifact, and summary counts. A modified log, result, public
key, signature, path, duration, or invariant invalidates the bundle.

## Operational non-claims

- Campaign runtime is reported separately from request latency; it is not an
  API latency percentile.
- One host and Docker failure domains are not a multi-zone availability test.
- The harness does not inject disk corruption, packet reordering, clock skew,
  cloud-object outages, or certificate-authority failure in this first profile.
- The 30-day profile is required operating evidence but does not replace live
  Kubernetes, security review, capacity/saturation curves, or design-partner
  traffic.
