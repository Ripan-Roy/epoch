# Experimental Stream Tablet

The first profile-integrated consensus slice is a single configured,
single-partition Stream tablet backed by one fixed three-voter Raft group. It
exists to prove the command, commit, application, idempotency, failover, and
recovery boundary before Epoch exposes clustered durability through the public
API or SDKs.

## What is implemented

```text
typed append request
  -> canonical versioned tablet command
  -> persistent Raft proposal
  -> durable fixed-voter-majority commit
  -> deterministic StreamTablet.apply
  -> typed receipt
```

- `epoch-tablet` owns the strict command codec and deterministic Stream state.
- `epoch-consensus` remains profile-agnostic and owns Raft/EPRS persistence.
- `epoch-node` applies committed commands on the consensus actor before making
  a successful typed result observable. A missing actor-applied receipt fails
  closed; an HTTP task never applies profile state on the actor's behalf.
- Startup replays every committed proposal into a fresh tablet before the
  experimental status endpoint becomes available.
- The consensus log is the only clustered source of truth. Commands are never
  duplicated into the standalone `engine.wal`.
- A stable idempotency key maps to a scoped proposal ID. Pending and committed
  retries inspect the original canonical bytes, so a server timestamp is not
  resampled and a changed semantic payload is rejected. Every commit
  notification is revalidated against the waiting request, so an overwritten
  old-leader proposal cannot satisfy a waiter for different input.

The v1 command accepts only partition `0`, rejects unknown fields and versions,
is limited to the consensus proposal ceiling, and must match its canonical JSON
encoding exactly. The proposal ID is currently a scope-separated 64-bit prefix
of SHA-256; the complete key remains in the command, so a collision fails as a
conflict instead of returning another operation's result. This is an
experimental boundary, not the final identifier format. JSON responses encode
all 64-bit identity, position, and envelope-time metadata as exact decimal
strings so browser clients do not lose precision. The append endpoint accepts
`expected_term`, `time_ms`, `deliver_at_ms`, and `ttl_ms` as either unsigned JSON
numbers or decimal strings; browser callers should use the string form shown
below.

## Run the disposable proof

```shell
make test-stream-tablet
```

The gate builds one node image and starts three containers with independent
EPRS volumes. It verifies:

1. a follower returns typed `not_leader`;
2. typed and internal routes stay off the public listener, opaque proposal
   routes stay off the typed group, and public health remains `local_durable`;
3. an isolated leader never returns committed success;
4. the leader returns success only after majority persistence and local profile
   application;
5. the receipt reports `fixed_voter_majority_persisted`, two durable voter
   acknowledgements, Raft commit position, and Stream offset without claiming
   zone-aware quorum durability;
6. an exact retry returns the original offset while changed input conflicts;
7. a replacement leader safely rebinds an overwritten minority-only proposal,
   while the original semantic input conflicts instead of receiving its result;
8. the restarted voter catches up once; and
9. after all three containers receive `SIGKILL`, their existing EPRS volumes
   rebuild the exact pre-crash record document and state digest before
   readiness, and an exact retry still resolves to the original offset.

The script allocates loopback ports dynamically, uses a unique Compose project,
and removes only its own containers, network, and volumes. CI uploads its logs,
port map, and EPRS state if the proof fails.

## Manual topology

```shell
EPOCH_EXPERIMENTAL_STREAM_TABLET_ENABLED=true \
EPOCH_EXPERIMENTAL_STREAM_TABLET_NAME=orders \
docker compose -f deploy/compose/docker-compose.consensus-probe.yml up --build --detach
```

The default peer/experimental listeners are `127.0.0.1:17701` through
`127.0.0.1:17703`. Query each tablet status document and use the node reporting
`"role":"leader"` and its current `term`:

```shell
curl --fail --silent --show-error \
  http://127.0.0.1:17701/experimental/v1/tablets/stream/status
```

Append on the actual leader port:

```shell
curl --fail --silent --show-error \
  --header 'content-type: application/json' \
  --data '{
    "idempotency_key":"order-request-1",
    "expected_term":"1",
    "partition":0,
    "envelope":{
      "id":"order-1",
      "source":"checkout",
      "type":"order.created",
      "time_ms":"1",
      "payload":{"order_id":"1"}
    }
  }' \
  http://127.0.0.1:17701/experimental/v1/tablets/stream/records
```

Replace the example term and port with the observed leader values. A successful
response is `201 Created`; an exact completed retry is `200 OK`; a request that
is still unresolved at the bounded server wait is `202 Accepted`, preserving
whether the local state is `unknown` or `pending`; either has unknown outcome
certainty. `not_leader`, `stale_term`, and semantic-conflict responses are also
globally unknown because another voter may hold newer evidence. Resolve the
proposal ID decimal string through:

```text
GET /experimental/v1/tablets/stream/mutations/{proposal_id}
```

Leader-local committed reads use:

```text
GET /experimental/v1/tablets/stream/records?offset=0&limit=100
```

They are explicitly `local_profile_applied_stale_capable`; no linearizable read
barrier exists yet. Status exposes `last_profile_mutation_index`, the Raft index
of the latest unique typed command reflected in the Stream. It is not a Raft
applied watermark: election no-ops can make `consensus_applied_index` advance
without changing it.

## Deliberate non-claims

This mode is on the dedicated experimental listener. It has no CORS, TLS,
authentication, authorization, SDK commitment, public compatibility promise,
or multi-tenant isolation. Its fixed-voter evidence assumes the three configured
peer endpoints are isolated and trusted; an unauthenticated peer can spoof a
voter, so this is not durable-majority proof under a hostile network. Do not
expose it to an untrusted network.

It also has static membership, one group/resource, one partition, no snapshots,
log compaction, retention deletion, read barrier, catalog-authorized epoch
transition, placement, authenticated peer identity, bounded idempotency
retention, replica-progress/ISR contract, or exhaustive crash/I/O matrix.
The three local voters are not placement or zone evidence, so the typed receipt
uses `write_evidence: fixed_voter_majority_persisted` and
`durable_voter_acks: 2`; it deliberately does not report the PRD's
zone-aware `quorum_durable` profile. A deterministic profile-application error
fails the actor, drains both HTTP listeners, and exits the process.

The public port `7601` remains the standalone API and still rejects
`quorum_durable`. Its health response remains capped at `local_durable`. This
experimental milestone therefore advances the replicated core without turning
partial evidence into a production claim.

See [Architecture](ARCHITECTURE.md), [Semantics](SEMANTICS.md),
[API contracts](API_CONTRACTS.md), and the
[Consensus feasibility spike](CONSENSUS_SPIKE.md) for the surrounding contract.
