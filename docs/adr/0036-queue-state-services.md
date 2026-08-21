# ADR-0036: Replicated Queue state services

- Status: Accepted for `v0.1.0-alpha.8`
- Date: 2026-08-21
- Requirements: `QUEUE-007` through `QUEUE-015`

## Decision

Epoch completes the remaining Work Queue development requirements with one
deterministic state-services layer owned by each Queue tablet. The layer is
snapshotted and digested with the existing Queue engine and is mutated only by
committed Queue tablet commands. The state machine performs no network or
object-store I/O.

The existing Queue command v1/v2 and tablet snapshot v1 contracts remain
readable. Advanced operations use command v3. Tablet snapshot v2 adds the
state-services image and accepts v1 by constructing empty/default advanced
state from the persisted Queue configuration.

## Capacity, expiry, and deduplication

Queue admission applies the configured message-count ceiling and an optional
1-byte through 3-MiB active-message byte ceiling. Canonical envelope plus
Queue-metadata bytes are charged. The overflow policy is explicit:

- `reject_new` commits a typed capacity rejection without mutation;
- `drop_oldest` expires the oldest non-leased active message; and
- `dead_letter_oldest` moves that message into the durable dead-letter history.

Admission never steals an active lease. If no eligible victim can satisfy the
limit, the new message is rejected atomically. Queue idle expiry starts after
the first committed use, runs only while the queue has no active messages,
session lease, or pending dead-letter forward, and produces a durable expired
state. It does not silently delete the catalog resource. Reads expose the
expired state and data operations reject until a new resource incarnation is
created.

Deduplication uses the existing message `dedupe_id` and configured replicated
window. A duplicate returns the original message/commit receipt and performs
no overflow eviction or metadata replacement. Expiry and restart boundaries
are inclusive and deterministic.

## FIFO sessions and priority fairness

Messages may carry a bounded session/message-group ID. Grouped messages are
ineligible for ordinary competing-consumer acquisition. A session acquire
creates one renewable opaque lock bound to tablet ID/epoch, leader term,
consumer ID/epoch, session ID, lock generation, and deadline. Only that owner
can acquire the session's messages until release or expiry. Delivery selection
within a session is commit-order FIFO; independent sessions may progress in
parallel.

Ordinary messages use an effective priority of
`min(255, base_priority + waited_ms / aging_interval_ms)` when aging is
configured. Ties use original commit position and message ID. Thus a lower
band eventually reaches the highest band while replay and every voter make the
same choice. This is deterministic starvation protection, not a latency SLO.

## Dispatch shaping and circuit breaking

An optional queue-wide dispatch policy contains an integer messages-per-second
rate, burst, maximum active deliveries, consecutive-failure threshold, and
open interval. The replicated token bucket refills with checked integer
arithmetic from committed time. Acquisition consumes tokens only for leases it
actually creates. Acknowledgement closes/resets the breaker; nack/reject
records a downstream failure and opens the breaker at the configured
threshold. One deterministic half-open probe is permitted after the open
deadline. This protects dispatch admission but does not rate-limit publishers.

## Deferred messages and request/reply

A consumer can defer its live fenced delivery with a reason. The message stays
durable but is excluded from ordinary/session acquisition until an exact
message-ID receive leases it. Session ownership is still enforced. Deferral,
exact retrieval, expiry, and retry are part of the same replicated history.

Advanced ingress may attach bounded `correlation_id` and `reply_to` metadata.
Linearizable lookup returns matching messages in commit order. A normal Queue
configured with idle expiry is the temporary reply destination; Epoch does not
invent an untracked process-local queue. Correlation is routing metadata, not
an exactly-once RPC claim.

## Quorum dead-letter forwarding

An optional, distinct `quorum_durable` target Queue creates a durable outbox
entry for each new local dead-letter history record. Catalog admission and
runtime resolution reject a Queue that targets itself, and runtime resolution
rejects a non-quorum target. Only the current non-fail-stopped source leader may
operate the outbox. It first commits an immutable target binding (resource
generation, shard, tablet, and tablet epoch), then enqueues the original
envelope with a stable source-history-derived target idempotency key, then
commits source completion. A crash between target commit and source completion
repeats the same target identity. The result is at-least-once forwarding with
exact insertion into an Epoch target incarnation; no distributed transaction
or exactly-once external side effect is claimed.

## Atomicity and rejection model

Every command stages cloned Queue and state-services values. Validation,
snapshot encoding, and cross-component invariants complete before publication.
State-dependent domain errors commit as typed `rejected` receipts and leave
both staged components unchanged. Storage/internal failures remain fatal.
Exact retries return the original receipt, including rejections.

## Bounds and non-claims

- identifiers are 1-256 non-control UTF-8 bytes;
- one Queue tablet remains one physical partition;
- active state, session locks, deferred records, correlations, and outbox
  entries are bounded by explicit constants and the 4-MiB snapshot ceiling;
- rate, fairness, and expiry are based only on committed monotonic time;
- queue expiry is a durable data-plane state, not automatic catalog deletion;
- request/reply does not provide cross-queue transactions;
- DLQ forwarding is limited to a distinct, `quorum_durable`, materialized Queue
  in the same regional namespace in this release; and
- production throughput/fairness SLOs, dynamic placement, and a broad
  partition/network/disk fault report remain release gates rather than alpha
  guarantees.
