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
[Regional Stream SDK](REGIONAL_STREAM_SDK.md), create the dead-letter target,
then apply the configured source Queue:

```shell
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-failed-jobs-v1","expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"queue","name":"failed-jobs",
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"internal","tags":{"service":"jobs","profile":"queue"}},
      "spec":{"shard_count":1,"replica_count":3,"placement":{"allowed_regions":["ap-south"],"minimum_zones":3,"required_node_class":"general-purpose"}}
    }
  }'

curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-jobs-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"queue","name":"jobs",
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"internal","tags":{"service":"jobs","profile":"queue"}},
      "spec":{"shard_count":1,"replica_count":3,"configuration":{
        "durability":"quorum_durable","visibility_timeout_ms":30000,
        "max_messages":100000,
        "retry":{"strategy":"exponential","initial_delay_ms":1000,"max_delay_ms":60000,"jitter_percent":10,"max_attempts":8,"max_age_ms":null},
        "dedupe_window_ms":60000,
        "advanced":{"max_active_bytes":3145728,"overflow":"dead_letter_oldest",
          "idle_expiry_ms":600000,"priority_aging_interval_ms":1000,
          "dispatch":{"messages_per_second":1000,"burst":100,"max_in_flight":100,"failure_threshold":5,"open_interval_ms":30000},
          "dead_letter_target":"failed-jobs"}
      },"placement":{
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

Each example runs the ordinary retry/DLQ/redrive lifecycle, then runs a FIFO
session with lock renewal, request/reply correlation, exact deferred retrieval,
and advanced/outbox observations. The source Queue's leader forwards each new
dead-letter record to `failed-jobs` with a stable target mutation identity.

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
| Enqueue session/correlation/reply metadata | `EnqueueAdvanced` | `enqueueAdvanced` | `enqueue` keyword arguments |
| Credit acquire | `Acquire` | `acquire` | `acquire` |
| FIFO session acquire/continue | `Acquire` with session options | `acquireSession` | `acquire` with session options |
| Renew session lock | `RenewSessionLock` | `renewSessionLock` | `renew_session_lock` |
| Release session lock | `ReleaseSessionLock` | `releaseSessionLock` | `release_session_lock` |
| Acknowledge | `Acknowledge` | `acknowledge` | `acknowledge` |
| Renew | `ExtendLease` | `extendLease` | `extend_lease` |
| Release | `Release` | `release` | `release` |
| Retryable failure | `Nack` | `nack` | `nack` |
| Terminal failure | `Reject` | `reject` | `reject` |
| Redrive | `Redrive` | `redrive` | `redrive` |
| Apply timers | `Maintain` | `maintain` | `maintain` |
| Defer delivery | `Defer` | `defer` | `defer` |
| Receive exact deferred message | `ReceiveDeferred` | `receiveDeferred` | `receive_deferred` |
| Mutation lookup | `Mutation` | `mutation` | `mutation` |
| Counts | `Counts` | `counts` | `counts` |
| DLQ history | `DeadLetters` | `deadLetters` | `dead_letters` |
| Redrive history | `Redrives` | `redrives` | `redrives` |
| Consumer flow | `ConsumerFlow` | `consumerFlow` | `consumer_flow` |
| Advanced status | `AdvancedStatus` | `advancedStatus` | `advanced_status` |
| Correlation lookup | `Correlation` | `correlation` | `correlation` |
| DLQ forwarding outbox | `DeadLetterForwards` | `deadLetterForwards` | `dead_letter_forwards` |
| Tablet status | `Status` | `status` | `status` |

Every mutation requires a nonempty idempotency key. Acquire also requires a
consumer, nonzero consumer epoch, and one to 100 requested messages. The
optional in-flight window is one to 10,000. Session, correlation, reply, and
message identifiers are bounded to 256 bytes at the advanced contract. Lease
and session-lock tokens are opaque; pass the exact latest token returned by
acquire or renewal.

The configured Queue admits at most `max_messages` and, when present,
`advanced.max_active_bytes`. Overflow is one of `reject_new`, `drop_oldest`, or
`dead_letter_oldest`. Priority aging, dispatch rate/burst/concurrency, the
circuit breaker, idle expiry, and DLQ forwarding use only committed Queue time
and state. `dead_letter_target` is accepted only on a `quorum_durable` regional
Queue and must name a distinct `quorum_durable` Queue in the same namespace.

## HTTP contract

The shard base path is:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}
```

`GET` on that path performs discovery. Operations append `mutations`,
`mutations/{proposal_id}`, `counts`, `dead-letters`, `redrives`,
`consumers/{consumer}/flow`, `advanced`, `correlations/{correlation_id}`,
`dead-letter-forwards`, or `status`.

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

The Docker campaign kills the active advanced-Queue leader before using the
Python SDK. It proves dedupe, session exclusion/FIFO/renewal/fencing,
correlation, defer/exact receive, DLQ outbox delivery into `failed-jobs`, voter
convergence, old-voter catch-up, and all-voter EPRS reopen. The original Queue
campaign still covers automatic delayed retry, credit, old-term lease fencing,
redrive, and exact recovery. Go and Java exercise the identical request
contract in unit tests and compile their exact public-docs programs.

## Current boundary

This alpha is a complete SDK surface over the implemented single-partition
tablet, not the final Queue protocol. Native bidirectional receive,
connection-scoped credit replenishment, automatic prefetch, multi-partition
routing, timer/throughput/fairness SLO evidence, generated response types,
backlog-scale indexed counting, TLS/OIDC/mTLS, package-registry publication,
dynamic membership, and the production fault/scale matrix remain open. See
[ADR-0018](adr/0018-regional-queue-v1-and-sdk-routing.md) and
[ADR-0036](adr/0036-queue-state-services.md).
