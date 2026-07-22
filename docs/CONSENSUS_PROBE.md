# Experimental Consensus Probe

The consensus probe is an opt-in process/runtime integration for Epoch's
fixed-three-voter persistent Raft adapter. It replicates opaque diagnostic
payloads only. Cache, Stream, Queue, and Event Bus APIs remain independent
standalone engines with a `local_durable` guarantee ceiling.

This surface exists to exercise process lifecycle, real HTTP peer delivery,
EPRS recovery, election, and proposal lookup without claiming that product
profile replication is complete.

## Start three local containers

```shell
make compose-probe-up
```

The topology exposes standalone profile APIs on `127.0.0.1:17661` through
`17663`, and the unauthenticated experimental/internal listeners on
`127.0.0.1:17701` through `17703`. Peer listeners bind only to loopback on the
host; do not expose them to an untrusted network.

Inspect each local Raft view:

```shell
curl --fail --silent --show-error \
  http://127.0.0.1:17701/experimental/v1/consensus/status
curl --fail --silent --show-error \
  http://127.0.0.1:17702/experimental/v1/consensus/status
curl --fail --silent --show-error \
  http://127.0.0.1:17703/experimental/v1/consensus/status
```

One response eventually reports `"role":"leader"`. Its `term` must be sent
as `expected_term` when proposing an opaque payload:

```shell
curl --fail --silent --show-error \
  --header 'content-type: application/json' \
  --data '{"proposal_id":101,"expected_term":1,"payload":[101,112,111,99,104]}' \
  http://127.0.0.1:17701/experimental/v1/consensus/proposals
```

Use the actual leader port and current term from status. A follower returns a
structured `not_leader` conflict. Proposal acceptance is asynchronous, so
resolve the outcome on every voter:

```shell
curl --fail --silent --show-error \
  http://127.0.0.1:17701/experimental/v1/consensus/proposals/101
```

`unknown`, `pending`, and `committed` are deliberately distinct. A committed
response includes its Raft term, log index, and original bytes. Every lookup is
a local observation; this endpoint is not a client acknowledgement contract.

Restarting a container reopens the same node-specific EPRS journal:

```shell
docker compose -f deploy/compose/docker-compose.consensus-probe.yml \
  restart epoch-probe-1
```

Stop the topology without deleting its three volumes:

```shell
make compose-probe-down
```

Run the disposable end-to-end election/failover/catch-up proof with:

```shell
make test-consensus-probe
```

That smoke uses dynamically allocated loopback ports and a unique Compose
project, then removes only the containers, network, and volumes it created.

## Native process configuration

Set `EPOCH_CONSENSUS_PROBE_ENABLED=true` and provide the following values to
`epoch-node`:

| Variable | Meaning |
|---|---|
| `EPOCH_CONSENSUS_NODE_ID` | Non-zero local voter ID; required when enabled |
| `EPOCH_CONSENSUS_GROUP_ID` | Fixed group ID; defaults to `1` |
| `EPOCH_CONSENSUS_GROUP_EPOCH` | Fixed group epoch; defaults to `1` |
| `EPOCH_CONSENSUS_LISTEN` | Dedicated internal HTTP listener; defaults to `127.0.0.1:7701` |
| `EPOCH_CONSENSUS_PEERS` | Exactly three `node_id=http://authority` entries |
| `EPOCH_CONSENSUS_TICK_MS` | Tick interval from 10 ms through 60 seconds; defaults to 100 ms |

Each node stores its stable journal at
`$EPOCH_DATA_DIR/consensus/group-{group_id}/node-{node_id}.wal`. Configuration
identity is immutable for those bytes, and another writer cannot open the same
journal concurrently.

The dedicated listener has no browser CORS layer. Peer frames require
`application/octet-stream`, have a strict body limit, and flow through bounded
per-peer ordered queues. Enqueue is nonblocking and isolated by destination: a
full or closed minority queue drops only that peer's retryable Raft frame, so it
cannot stall actor ticks or delivery to a healthy majority. Raft is responsible
for retransmission. The internal HTTP client ignores ambient proxy variables
and never follows redirects, preserving the configured peer authority boundary.
The diagnostic JSON surface is on the same listener. The public standalone API
retains its explicit origin allowlist.

## Explicit non-claims

The probe has static membership, plaintext unauthenticated transport, one
fixed group, no snapshot installation, no compaction, no read barrier, no
catalog-authorized epoch transition, and no profile state-machine integration.
Status therefore always reports:

```json
{
  "stability": "experimental",
  "production_readiness": "not_production_ready",
  "observation_scope": "local",
  "profile_replication": false,
  "profile_guarantee_ceiling": "local_durable",
  "peer_authentication": "none",
  "outbound_transport": [
    {
      "peer_id": 2,
      "observed_condition": "healthy",
      "queue_capacity": 128,
      "queued_frames": 0,
      "enqueued_frames": 12,
      "delivered_frames": 12,
      "dropped_queue_full_frames": 0,
      "dropped_worker_closed_frames": 0,
      "exhausted_retry_frames": 0
    }
  ]
}
```

The transport array is cumulative, local diagnostic evidence. `degraded` means
this process has observed a full queue or exhausted retry; `unavailable` means
its worker channel closed. Neither value is an authenticated reachability
oracle, and counters reset on process restart.

See [Consensus Feasibility Spike](CONSENSUS_SPIKE.md) for the proven evidence
and remaining G3 acceptance work.
