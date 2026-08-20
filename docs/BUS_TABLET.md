# Experimental Replicated Event Bus Tablet

**Status:** Working bounded fixed-three-voter ingress and per-subscription
delivery-ledger profile with an authenticated regional v1 adapter and optional
leader-owned signed HTTP/webhook plus automatic Epoch Queue/Stream execution

`epoch-bus` and `epoch-tablet` implement a canonical Event Bus ingress and
delivery-ledger boundary. The standalone engine owns validated subscriptions,
deterministic filtering and transformation, checked publish positions, and
bounded archive replay. The replicated tablet additionally enables a bounded
outbox: each matched subscription receives a stable delivery ID, captured
policy, independent lease/attempt history, retry eligibility, acknowledgement,
and terminal dead-letter state. `BusTablet` applies strict commands only after
consensus commit, records exact retry receipts, and chains every committed
outcome into a deterministic state digest. `epoch-node` mounts that state
machine as the one typed profile for a fixed consensus group and as one
catalog-materialized regional group. Startup installs a canonical native Bus
checkpoint plus retained EPRS tail before exposing its APIs, while legacy
histories still replay; committed application divergence fail-stops the group.

## Boundary

```text
canonical BusTabletCommand v1/v2/v3
  -> strict internal HTTP DTO and semantic idempotency validation
  -> current leader/term admission and fixed-voter majority persistence
  -> actor-owned committed metadata supplied to the tablet
  -> committed-order effective time
  -> operation on a cloned EventBus candidate
  -> applied result or deterministic rejected outcome
  -> complete recoverable route, archive, outbox, dispatcher-epoch, and attempt digest
  -> exact receipt replay and chained tablet transition digest
```

The candidate is installed only after the operation and digest complete.
Subscription, archive, and outbox capacity, malformed input, lease/retry
deadline overflow, and checked-counter exhaustion therefore leave the prior
business state intact. A recordable business rejection still consumes its
committed log index and is included in the tablet digest.

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
proposal modes are mutually exclusive for one group. The direct typed routes
exist on the separate internal consensus listener:

| Route | Contract |
| --- | --- |
| `GET /experimental/v1/tablets/bus/status` | Local consensus/profile positions, route/archive/outbox counters, complete digests, and explicit executor non-claims. |
| `POST /experimental/v1/tablets/bus/mutations` | Strict route/publish plus `acquire_deliveries`, `acknowledge_delivery`, `fail_delivery`, and `maintain_deliveries` commands with an idempotency key and expected term. |
| `GET /experimental/v1/tablets/bus/mutations/{proposal_id}` | Local `unknown`, `pending`, or `committed` outcome resolution. |
| `POST /experimental/v1/tablets/bus/archive/replay` | Inclusive time-range and optional filtered replay from the local applied profile. |
| `POST /experimental/v1/tablets/bus/deliveries/query` | Bounded local, stale-capable delivery-ledger and immutable attempt-history observation. |

All 64-bit JSON values are emitted as exact decimal strings. Mutation input
accepts a JSON number or decimal string for `expected_term` and envelope time
fields. Unknown fields are rejected at the request, operation, subscription,
filter, target, transform, and envelope boundaries. The leader owns
`applied_at_ms`; retries compare only semantic input, so changing an expected
term does not conflict while changing the operation does.

The profile-local status reports `target_dispatch:
regional_epoch_targets_and_configured_signed_webhooks`; the tablet itself still
performs no I/O. Regional topology separately reports signed-webhook and
always-enabled Epoch-target pass/lease/outcome/error counters. Status also
reports `durable_target_outbox: true`. A publish receipt proves replicated ingress,
the captured deterministic route plan, archive state, and durable delivery
intent. An acknowledgement proves only that an internal dispatcher committed
the target result it observed. The regional runtime executes signed
HTTP/webhook and Epoch Queue/Stream targets; unsigned HTTP/webhook and network
pull remain external.

The regional runtime additionally maps the fully qualified authenticated v1
route below to that same tablet without a second store or Go data proxy:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}
```

Go, Java, and Python `RegionalBusClient` implementations cover every command
and read above with resource-generation, tablet-epoch, and leader-term fences.
Archive replay, delivery query, mutation lookup, and status require a
linearizable ReadIndex barrier; archive/query retain POST bodies but authorize
as `data.read`. See [Regional Event Bus SDK](REGIONAL_EVENT_BUS_SDK.md) and
[ADR-0020](adr/0020-regional-event-bus-v1-and-sdk-routing.md).

## Configuration and hard limits

`BusConfig` remains backward-compatible with SDK requests containing only
`durability` and `archive`. The server supplies these defaults:

| Field | Default | Hard maximum | Meaning |
| --- | ---: | ---: | --- |
| `max_subscriptions` | 1,024 | 100,000 | Named routes retained by one Bus |
| `max_archive_events` | 100,000 | 10,000,000 | Archived ingress records retained in this in-memory slice |
| `delivery_outbox` | `false` standalone; forced `true` by `BusTablet` | n/a | Retain independent delivery state for every matching subscription |
| `max_outbox_deliveries` | 100,000 | 10,000,000 | All pending, in-flight, acknowledged, and dead-lettered records retained by the bounded ledger |

All three capacity limits must be non-zero. Archive and outbox limits are
validated even when their feature is disabled so enabling it never reveals an
invalid latent configuration. Replay and delivery queries return at most
10,000 records per call; acquisition and maintenance process at most 100.

Each subscription has a backward-compatible `delivery_policy`. Its defaults
are a 30-second attempt timeout, 16 in-flight records, exponential retry from
1 to 60 seconds with 10% deterministic jitter, eight attempts, and no max age.
The hard bounds are 1,000 in-flight records, 100 attempts, and seven days for
timeout or maximum retry delay. The policy is captured into each delivery so a
later route edit cannot retroactively change existing work.

One subscription permits at most 64 patterns in each pattern collection, 64
header predicates, 64 JSON-equality predicates, 64 headers added by a
transform, and 64 projected payload fields. Pattern, header, path, field, JSON
value, and target URL byte lengths have explicit bounds in `epoch-bus`.
Tablet commands are capped at 512 KiB, matching the consensus proposal ceiling.

## Commands

Every command binds a format version, tablet ID and epoch, resource name,
idempotency key, candidate application time, and one operation. Existing
unsigned operations retain their exact v1 bytes. A signed subscription,
internal exact-delivery acquisition, or terminal rejection uses v2. An exact
Queue/Stream acquisition containing a pinned destination uses additive v3:

| Operation | Deterministic result |
| --- | --- |
| `upsert_subscription` | Validate and insert or replace one named route, then advance the checked route-plan version. |
| `remove_subscription` | Remove a present route and advance the version, or return a stable `removed: false` result without changing it. |
| `publish` | Validate the envelope, capture the route-plan version, evaluate routes in lexical name order, atomically append one outbox record per match, optionally archive, and advance the checked publish position. |
| `acquire_deliveries` | Lease an ordered bounded batch for one subscription under the current leader term and dispatcher epoch. |
| `acknowledge_delivery` | Fence by exact active lease and commit terminal acknowledgement. |
| `fail_delivery` | Fence by exact active lease and commit deterministic retry eligibility or terminal dead-letter state. |
| `reject_delivery` | Fence by exact active lease and immediately commit a bounded terminal dead-letter reason. |
| `maintain_deliveries` | Settle a bounded batch of expired leases as timeout failures. |

The public mutation DTO never accepts a destination binding. The regional
source-leader worker alone submits an exact v3 acquire that binds target kind,
resource generation, shard, tablet ID, and tablet epoch. Queue uses shard `0`;
Stream uses the shared FNV-1a key router. Legacy batch acquisition does not
lease Queue/Stream records, so an external pull dispatcher cannot bypass that
binding step.

An internal dispatcher first commits an acquisition:

```json
{
  "idempotency_key": "acquire-orders-42",
  "expected_term": "7",
  "operation": {
    "kind": "acquire_deliveries",
    "subscription": "orders",
    "dispatcher": "webhook-sender",
    "dispatcher_epoch": "3",
    "max_deliveries": 10
  }
}
```

The `deliveries_acquired` receipt contains the transformed envelope, stable
`delivery_id`, attempt number, opaque lease token, and exact decimal-string
deadline. After the target-specific worker finishes, it commits either
`acknowledge_delivery` with the same dispatcher identity, epoch, ID, and token,
or `fail_delivery` with those fields plus a bounded reason. A failure receipt
reports `pending` with `next_eligible_at_ms` or `dead_lettered`. An exact retry
of any mutation returns its original result and never runs the transition
twice.

The current ledger can be inspected without mutation:

```json
{
  "subscription": "orders",
  "state": "dead_lettered",
  "limit": 100
}
```

Post that body to `/experimental/v1/tablets/bus/deliveries/query`. The result
contains captured target/policy, current state, and immutable attempt outcomes.
It is a local stale-capable observation when called on the direct internal
profile route. Regional archive replay and delivery-query POSTs are classified
as `data.read`, default to a safe leader ReadIndex barrier, and report exact
term/read/applied evidence. `x-epoch-read-consistency: local_stale` is the
explicit opt-in to this direct behavior.

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
embedded credentials or fragment. An optional `signing_key_id` is a bounded
resource name captured into every matching outbox record. Only a signed
HTTP/webhook target is eligible for the network executor; Queue/Stream records
are owned by the separate native-target executor.

On each attempt the regional leader revalidates the URL, resolves every domain,
rejects any non-public or mixed DNS answer, and pins the validated addresses.
HTTPS is mandatory except for an explicitly enabled loopback development
target. Redirects and ambient proxies are disabled, response bodies are
discarded, and the request timeout cannot exceed the replicated lease. The
worker emits CloudEvents 1.0 binary-mode headers and an exact-body
HMAC-SHA-256 signature. See
[ADR-0030](adr/0030-leader-owned-signed-webhook-delivery.md) for the canonical
input, key rotation, replay identity, and non-claims.

Transforms add bounded headers and optionally project named top-level output
fields from deterministic JSON paths. Each publish receipt retains the route
plan version, position, delivery count, and versioned lowercase SHA-256 digest
of the ordered fully transformed delivery list. The outbox separately retains
the transformed envelope and captured target/policy required for later
dispatch; receipts remain bounded to count and digest.

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

The latest business-state digest covers normalized configuration, sorted
subscriptions, route-plan version, publish position, archive, every outbox
record and attempt, and dispatcher epoch high-water marks. The v2 tablet
transition digest additionally covers the prior tablet digest, proposal ID,
term, log index, payload digest, effective application time, business-state
digest, and canonical outcome. Deterministic rejection changes the tablet
history digest but not the business-state digest.

Delivery IDs are `epoch.bus.delivery.v1.<publish-position>.<subscription>`.
Attempts are ordered by publish position and subscription, observe an inclusive
retry-eligibility boundary, and use an exclusive lease deadline. A newer
dispatcher epoch fences older settlement tokens; a leader-term change also
fences the old lease. Failure of one record cannot mutate another target.
Terminal records and immutable attempts remain in the bounded ledger; pruning,
retention, and redrive are intentionally not implemented yet.

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
- atomic lexical outbox creation and capacity rejection;
- dispatcher/leader fencing, bounded in-flight isolation, exact lease
  acknowledgement, retry boundaries, timeout maintenance, attempt exhaustion,
  and dead-letter state;
- identical results, archive state, positions, and digests across three
  independent tablets replaying one committed history;
- strict DTOs, browser-safe metadata, semantic retry/conflict, actor-missed
  commit fail-stop, and recovery ordering;
- v1 byte preservation plus v2 signed-target/exact-acquire/rejection commands
  and snapshots, plus v3 destination-bound commands/snapshots and mislabeled
  version rejection;
- signed binary CloudEvents, exact HMAC vectors, strict/redacted rotating key
  files, special-address and mixed-DNS rejection, redirect/proxy suppression,
  invalid-header rejection, and a real loopback receiver;
- three real HTTP consensus runtimes committing, converging, shutting down, and
  reopening from EPRS, including one 503/204 signed retry whose two-attempt
  acknowledged history survives full voter restart, plus Queue enqueue and
  keyed multi-shard Stream append through independently led groups without a
  duplicate after full-voter reopen; and
- a three-container gate with follower rejection, committed acquire/ack,
  leader loss, catch-up, archive/outbox agreement, all-node `SIGKILL`, and
  same-volume recovery.

Reproduce the deployment proof with:

```shell
make test-bus-tablet
```

Still required are rate limiting, redrive and terminal-record retention, replay
attempt lineage, unsigned HTTP writes, long-poll and
push transports, OAuth/API-key destinations, private managed egress, secret
manager/hot reload, broader CloudEvents conformance, user-exportable
backups/PITR, production identity/TLS, generated response models, package
publication, and multi-shard routing.
