# Regional Queue SDK

Epoch's repository-local Go, Java, and Python SDKs expose the complete
single-shard replicated Queue lifecycle through `RegionalQueueClient`. The
client talks directly to Rust voters, discovers the current leader before each
operation, carries route fences, and preserves caller-owned mutation identity
across one bounded rediscovery.

This contract is separate from the standalone `EpochClient` Queue helpers. The
standalone API can select `local_durable` on one host; the regional client uses
the fixed-three-voter replicated Queue tablet.

## Provision a Queue

Start the disposable regional topology and Go management bridge as described in
[Regional Stream SDK](REGIONAL_STREAM_SDK.md), then apply a Queue resource:

```shell
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-jobs-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"queue","name":"jobs",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'
```

The public docs embed the exact executable sources:

- [Go](../console/src/quickstarts/regional_queue/quickstart.go)
- [Java](../console/src/quickstarts/regional_queue/RegionalQueueQuickstart.java)
- [Python](../console/src/quickstarts/regional_queue/quickstart.py)

Each example enqueues and exactly replays a message, acquires with explicit
credit, renews and releases the lease, reacquires and rejects it, observes the
immutable dead-letter record, redrives, reacquires and acknowledges, then reads
linearizable counts and consumer flow.

## Client construction

All clients require every voter endpoint, a bearer token, and the complete
namespace scope. Configure endpoint order for reachability, not correctness;
the client selects only a complete discovery response with
`accepts_writes: true`.

Go:

```go
client, err := epoch.NewRegionalQueueClient(
    []string{"http://127.0.0.1:18661", "http://127.0.0.1:18662", "http://127.0.0.1:18663"},
    token,
    epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
    3*time.Second,
)
```

Java:

```java
var client = new RegionalQueueClient(
    endpoints,
    token,
    new RegionalScope("acme", "shop", "dev", "core"),
    Duration.ofSeconds(3));
```

Python:

```python
client = RegionalQueueClient(
    endpoints,
    token=token,
    scope=RegionalScope("acme", "shop", "dev", "core"),
    timeout=3.0,
)
```

## Lifecycle surface

| Behavior | Go | Java | Python |
|---|---|---|---|
| Enqueue | `Enqueue` | `enqueue` | `enqueue` |
| Credit acquire | `Acquire` | `acquire` | `acquire` |
| Acknowledge | `Acknowledge` | `acknowledge` | `acknowledge` |
| Renew | `ExtendLease` | `extendLease` | `extend_lease` |
| Release | `Release` | `release` | `release` |
| Retryable failure | `Nack` | `nack` | `nack` |
| Terminal failure | `Reject` | `reject` | `reject` |
| Redrive | `Redrive` | `redrive` | `redrive` |
| Apply timers | `Maintain` | `maintain` | `maintain` |
| Mutation lookup | `Mutation` | `mutation` | `mutation` |
| Counts | `Counts` | `counts` | `counts` |
| DLQ history | `DeadLetters` | `deadLetters` | `dead_letters` |
| Redrive history | `Redrives` | `redrives` | `redrives` |
| Consumer flow | `ConsumerFlow` | `consumerFlow` | `consumer_flow` |
| Tablet status | `Status` | `status` | `status` |

Every mutation requires a nonempty idempotency key. Acquire also requires a
consumer, nonzero consumer epoch, and one to 100 requested messages. The
optional in-flight window is one to 10,000. Lease tokens are opaque; pass the
exact latest token returned by acquire or renewal.

## HTTP contract

The shard base path is:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}
```

`GET` on that path performs discovery. Operations append `mutations`,
`mutations/{proposal_id}`, `counts`, `dead-letters`, `redrives`,
`consumers/{consumer}/flow`, or `status`.

Every data request carries:

```text
authorization: Bearer <token>
x-epoch-resource-generation: <discovered decimal generation>
x-epoch-tablet-epoch: <discovered decimal tablet epoch>
```

Every SDK read also carries:

```text
x-epoch-read-consistency: linearizable
```

The server confirms the read barrier in response headers and JSON. The SDK
does not silently fall back to a follower or stale local profile.

## Retry and outcome rules

- `not_leader`, `fenced`, `route_not_found`, `route_unavailable`,
  `read_barrier_timeout`, and retryable transport/server errors allow one
  rediscovery cycle.
- The reconstructed request retains the exact idempotency key and semantic
  operation. Server-owned time and the newly discovered term may change.
- Authentication, scope denial, validation, idempotency conflict, and committed
  business rejection return immediately.
- A timeout can leave a mutation outcome unknown. Retry the same operation with
  the same key or use mutation lookup when the proposal ID is known.

Go returns `*epoch.APIError`, Java returns `EpochApiException`, and Python
returns `EpochAPIError`.

## Verification

```shell
go test ./sdk/go/epoch ./console/src/quickstarts/regional_queue
PYTHONPATH=sdk/python/src python3 -m unittest discover -s sdk/python/tests -v
cd sdk/java && ./mvnw verify
make test-regional-runtime
bash tests/integration/docs-quickstarts.sh
```

The Docker campaign kills the active Queue leader before using the Python SDK,
waits for all 12 Queue commands—including automatic delayed-retry promotion—to
converge on the two survivors, catches up the
old voter, kills every voter, reopens the same EPRS volumes, and verifies the
same applied state. Go and Java exercise the identical request contract in unit
tests and compile their exact public-docs programs.

## Current boundary

This alpha is a complete SDK surface over the implemented single-partition
tablet, not the final Queue protocol. Native bidirectional receive,
connection-scoped credit replenishment, automatic prefetch and fairness,
multi-partition routing, timer precision/load SLO evidence, generated response types,
backlog-scale indexed counting, TLS/OIDC/mTLS, package-registry publication,
dynamic membership, and the production fault/scale matrix remain open. See
[ADR-0018](adr/0018-regional-queue-v1-and-sdk-routing.md).
