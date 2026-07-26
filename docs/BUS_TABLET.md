# Deterministic Event Bus Tablet Core

**Status:** Working bounded replicated state-machine core; not mounted by
`epoch-node` and not a public durability or delivery claim

`epoch-bus` and `epoch-tablet` implement the first canonical Event Bus route-plan
boundary. The standalone engine owns validated subscriptions, deterministic
filtering and transformation, checked publish positions, and bounded archive
replay. `BusTablet` applies strict commands only after a caller supplies
consensus commit metadata, records exact retry receipts, and chains every
committed outcome into a deterministic state digest.

## Boundary

```text
canonical BusTabletCommand v1
  -> tablet scope, size, idempotency, and operation validation
  -> fixed-voter committed metadata supplied by the consensus boundary
  -> committed-order effective time
  -> operation on a cloned EventBus candidate
  -> applied result or deterministic rejected outcome
  -> complete recoverable Bus-state digest
  -> exact receipt replay and chained tablet transition digest
```

The candidate is installed only after the operation and digest complete.
Subscription capacity, archive capacity, malformed input, and checked-counter
exhaustion therefore leave the prior route/archive state intact. A recordable
business rejection still consumes its committed log index and is included in
the tablet digest.

This crate boundary does not itself prove that a majority persisted a command.
Its receipt names `fixed_voter_majority_persisted` because its caller contract
is `CommittedCommand`, the same post-commit boundary used by the mounted typed
profiles. `epoch-node` does not yet construct a Bus tablet or expose its
commands. That integration requires retained-history rebuild and real
fixed-voter tests before the evidence is externally reachable.

## Configuration and hard limits

`BusConfig` remains backward-compatible with SDK requests containing only
`durability` and `archive`. The server supplies these defaults:

| Field | Default | Hard maximum | Meaning |
| --- | ---: | ---: | --- |
| `max_subscriptions` | 1,024 | 100,000 | Named routes retained by one Bus |
| `max_archive_events` | 100,000 | 10,000,000 | Archived ingress records retained in this in-memory slice |

Both limits must be non-zero. The archive limit is validated even when archive
capture is disabled so enabling it never reveals an invalid latent
configuration. Replay returns at most 10,000 records per call.

One subscription permits at most 64 patterns in each pattern collection, 64
header predicates, 64 JSON-equality predicates, 64 headers added by a
transform, and 64 projected payload fields. Pattern, header, path, field, JSON
value, and target URL byte lengths have explicit bounds in `epoch-bus`.
Tablet commands are capped at 512 KiB, matching the consensus proposal ceiling.

## Commands

Every command binds format version `1`, tablet ID and epoch, resource name,
idempotency key, candidate application time, and one operation:

| Operation | Deterministic result |
| --- | --- |
| `upsert_subscription` | Validate and insert or replace one named route, then advance the checked route-plan version. |
| `remove_subscription` | Remove a present route and advance the version, or return a stable `removed: false` result without changing it. |
| `publish` | Validate the envelope, capture the current route-plan version, evaluate routes in lexical subscription-name order, optionally archive, and advance the checked publish position. |

Unknown fields, unsupported versions, non-canonical JSON, oversized payloads,
wrong tablet scope, invalid proposal identity, and malformed operations fail
structurally. They are not converted into business receipts because accepting
them on one voter could hide state-machine divergence.

## Route evaluation

Event-type, source, and subject patterns support literal text, `*`, and `?`.
Matching is Unicode-scalar aware. An empty pattern collection accepts that
dimension; patterns within one collection are alternatives. The pattern
dimensions, exact header predicates, and JSON-equality predicates are then
conjunctive.

JSON paths support `$`, `$.a.b`, and `a.b`. Empty segments and control
characters are rejected. This slice intentionally does not claim general
JSONPath arrays, predicates, escaping, or compiled bytecode.

Queue and Stream targets require valid Epoch resource names. HTTP and webhook
targets require a bounded absolute `http` or `https` URL with a host and no
embedded credentials or fragment. This is syntax validation, not the final
webhook security boundary: private-address egress policy, DNS rebinding
defense, signing, secret rotation, replay defense, timeout, and rate control
remain future runtime work.

Transforms add bounded headers and optionally project named top-level output
fields from deterministic JSON paths. Each publish receipt retains the route
plan version, position, delivery count, and lowercase SHA-256 digest of the
ordered fully transformed delivery list. It does not retain an envelope copy
per target.

## Ordering, replay, and digests

The route-plan version begins at `1`; publish positions begin at `1`. Both use
checked `u64` addition and reject before mutation on exhaustion. A publish uses
exactly one captured route-plan version. Later subscription changes cannot
alter its stored receipt or archive record.

Tablet proposal IDs are SHA-256-derived from a domain separator, tablet ID,
tablet epoch, resource, and bounded idempotency key. An exact reapplication
must match proposal ID, term, log index, and payload digest. It returns the
stored result with disposition `replayed` and changes no counters, archive
records, receipt count, or digest. Reusing the proposal ID with different
committed metadata fails closed.

The business-state digest covers normalized configuration, sorted
subscriptions, route-plan version, publish position, and archive. The tablet
transition digest additionally covers the prior tablet digest, proposal ID,
term, log index, payload digest, effective application time, business-state
digest, and canonical outcome. Deterministic rejection changes the tablet
history digest but not the business-state digest.

Archive replay selects records by inclusive receive-time range, applies the
same validated filter evaluator, preserves publish order, and enforces the
response limit. It currently returns archived records only; it does not enqueue
new durable subscription attempts or assign replay lineage.

## Evidence and non-claims

The tests cover:

- the route truth table and Unicode wildcard behavior;
- lexical fan-out and deterministic transformation;
- legacy configuration defaults and insertion-independent recovery digests;
- invalid filter, path, resource, HTTP target, and capacity boundaries;
- atomic route-plan, archive, and publish-position exhaustion;
- strict canonical command size/version/scope validation;
- scoped proposal identity, exact retry, conflict, and commit ordering;
- recordable capacity rejection without partial business mutation; and
- identical results, archive state, positions, and digests across three
  independent tablets replaying one committed history.

Still required before mounting the profile are an actor-owned apply service,
strict internal HTTP DTOs, EPRS rebuild-before-readiness, real three-runtime and
all-node `SIGKILL` tests, durable per-subscription delivery/outbox state,
backpressure isolation, retry/DLQ ledgers, replay attempt lineage, snapshots,
compaction, authentication, target egress security, and public SDK contracts.
