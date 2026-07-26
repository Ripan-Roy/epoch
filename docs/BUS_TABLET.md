# Experimental Replicated Event Bus Tablet

**Status:** Working bounded fixed-three-voter route-plan/ingress profile,
mounted only on the experimental internal listener; not a public durability or
target-delivery claim

`epoch-bus` and `epoch-tablet` implement the first canonical Event Bus route-plan
boundary. The standalone engine owns validated subscriptions, deterministic
filtering and transformation, checked publish positions, and bounded archive
replay. `BusTablet` applies strict commands only after consensus commit, records
exact retry receipts, and chains every committed outcome into a deterministic
state digest. `epoch-node` can mount that state machine as the one typed profile
for a fixed consensus group, rebuild it from EPRS before exposing the internal
API, and fail-stop the process if committed application diverges.

## Boundary

```text
canonical BusTabletCommand v1
  -> strict internal HTTP DTO and semantic idempotency validation
  -> current leader/term admission and fixed-voter majority persistence
  -> actor-owned committed metadata supplied to the tablet
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

The tablet crate alone does not prove that a majority persisted a command. The
mounted service supplies that proof through the same actor-owned post-commit
boundary as the other typed profiles. It reports
`fixed_voter_majority_persisted` with two durable voter acknowledgements only
after the local tablet applies the committed command. This is evidence for the
fixed trusted three-node topology, not the PRD's placement-aware public quorum
durability.

## Runtime API

Set `EPOCH_CONSENSUS_PROBE_ENABLED=true` and
`EPOCH_EXPERIMENTAL_BUS_TABLET_ENABLED=true`; optionally set
`EPOCH_EXPERIMENTAL_BUS_TABLET_NAME`. Bus, Cache, Queue, Stream, and opaque
proposal modes are mutually exclusive for one group. The typed routes exist
only on the separate internal consensus listener:

| Route | Contract |
| --- | --- |
| `GET /experimental/v1/tablets/bus/status` | Local consensus/profile positions, route/archive counters, complete digests, and explicit delivery non-claims. |
| `POST /experimental/v1/tablets/bus/mutations` | Strict `upsert_subscription`, `remove_subscription`, or `publish` command with an idempotency key and expected term. |
| `GET /experimental/v1/tablets/bus/mutations/{proposal_id}` | Local `unknown`, `pending`, or `committed` outcome resolution. |
| `POST /experimental/v1/tablets/bus/archive/replay` | Inclusive time-range and optional filtered replay from the local applied profile. |

All 64-bit JSON values are emitted as exact decimal strings. Mutation input
accepts a JSON number or decimal string for `expected_term` and envelope time
fields. Unknown fields are rejected at the request, operation, subscription,
filter, target, transform, and envelope boundaries. The leader owns
`applied_at_ms`; retries compare only semantic input, so changing an expected
term does not conflict while changing the operation does.

Status reports `target_dispatch: not_implemented` and
`durable_target_outbox: false`. A publish receipt proves replicated ingress,
the captured deterministic route plan, and archive state. It does not claim
that a pull, Queue, Stream, webhook, or HTTP target received anything.

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
- recordable capacity rejection without partial business mutation;
- identical results, archive state, positions, and digests across three
  independent tablets replaying one committed history;
- strict DTOs, browser-safe metadata, semantic retry/conflict, actor-missed
  commit fail-stop, and recovery ordering;
- three real HTTP consensus runtimes committing, converging, shutting down, and
  reopening from EPRS; and
- a three-container gate with follower rejection, leader loss, catch-up, archive
  agreement, all-node `SIGKILL`, and same-volume recovery.

Reproduce the deployment proof with:

```shell
make test-bus-tablet
```

Still required are durable per-subscription delivery/outbox state, backpressure
isolation, retry/DLQ ledgers, replay attempt lineage, snapshots, compaction,
authentication, target egress security, and public API/SDK contracts.
