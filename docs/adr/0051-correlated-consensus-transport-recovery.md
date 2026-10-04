# ADR-0051: Correlated consensus transport recovery

- Status: Accepted for the recovery-hardening implementation; protected and
  live campaign evidence is still required for release.
- Date: 2026-10-04
- Owners: consensus adapter, node actor, and regional membership reconciler

## Context

The beta.12 exact-main regional fault campaign left a restarted Stream voter
at one applied command while its healthy peers reached eleven. A real-HTTP
regression reproduced an idle follower that never recovered after the latest
compacted snapshot exhausted its transport retries. No further write or
checkpoint was available to unstick Raft's pending-snapshot progress.

The node's bounded per-peer workers counted drops and exhausted retries but
never reported those delivery facts to the consensus adapter. Refreshing only
an older pending snapshot during a later checkpoint did not cover the lost
latest image. Snapshot feedback also exposed a learner catch-up dependency:
an older baseline can omit a newly committed learner, which Raft correctly
refuses to restore. Refresh must not depend on observing an in-flight flag.

## Decision

1. The adapter prepares an unchanged canonical EPPM frame plus an opaque,
   process-local `PeerDelivery` token. Tokens bind adapter incarnation, group,
   epoch, source, destination, term, attempt sequence, and optional snapshot
   index. They are not serialized or persisted.
2. Retain at most one current snapshot token per provisioned peer. Same-index,
   same-term attempts have distinct sequences. A previous adapter incarnation
   cannot complete a replacement's attempt.
3. Only the actor mutates Raft progress. Apply the following outcome mapping:

   | Transport fact                                                               | Actor action                                       |
   | ---------------------------------------------------------------------------- | -------------------------------------------------- |
   | Snapshot queue full or worker closed                                         | Synchronously fail the exact pending attempt       |
   | Snapshot HTTP retries exhausted                                              | Enqueue a correlated snapshot-failure result       |
   | Snapshot HTTP accepted                                                       | Enqueue a correlated snapshot-finish result        |
   | Ordinary frame retries exhausted or locally dropped                          | Report unreachable to current-term leader progress |
   | Ordinary frame HTTP accepted                                                 | No log acknowledgement or commit action            |
   | Foreign, old-term, old-incarnation, superseded, or duplicate snapshot result | No progress mutation                               |

4. Forward valid outcomes through the pinned Raft adapter's `report_snapshot`
   and `report_unreachable` interfaces. Normal heartbeat/response processing
   drives retry; a new application mutation is not required. Transport success
   never advances a follower's matched index or acknowledges quorum durability.
5. Queue drops are processed directly on the actor; the actor never blocks by
   sending to its own bounded command queue. Workers use weak command senders,
   upgrading only while reporting a result, so transport feedback cannot form
   an actor/worker/channel ownership cycle.
6. If a learner is behind a compacted baseline and that baseline is older than
   the applied state, refresh it even when no snapshot is currently pending.
   Preserve the existing pending-snapshot refresh. Do not rewrite an already
   current checkpoint or weaken learner catch-up/promotion gates.

## Verification

- Observed the no-new-write/no-new-checkpoint HTTP regression fail before the
  fix, then pass with correlated feedback.
- Exercise three and five voter scopes, same-index retry supersession, foreign
  identities, old terms/incarnations, duplicate results, and unchanged wire
  bytes, matched indexes, commit indexes, and applied indexes.
- Exercise full and closed outbound queues, ordinary per-peer ordering,
  redirect rejection, exact snapshot results, and weak-channel ownership.
- Reproduce the learner-replacement regression introduced by prompt transport
  completion, then prove refreshed-baseline catch-up, finalization, retained
  Stream data, and same-volume reopen.
- Require the complete Rust test/lint gate, existing Go race gate, and rebuilt
  regional fault campaign before a feature PR. Exact-main protected CI and
  release artifacts remain separate gates.

## Consequences and non-claims

There is no EPPM, EPSN, EPRS, proposal, receipt, or public API format change.
Correlation state is bounded and ephemeral. Peer redirects, authentication,
canonical validation, persistence-before-publication, and fail-stop behavior
are unchanged. This does not add chunked snapshots, cross-version repair,
snapshot transfer SLOs, unbounded retries, Catalog sharding, or production
certification. The concurrent-controller matrix remains tracked separately in
[CONTROL_HA_CERTIFICATION.md](../CONTROL_HA_CERTIFICATION.md).
