import goSource from "../quickstarts/quickstart.go?raw";
import javaSource from "../quickstarts/Quickstart.java?raw";
import pythonSource from "../quickstarts/quickstart.py?raw";
import regionalGoSource from "../quickstarts/regional/quickstart.go?raw";
import regionalJavaSource from "../quickstarts/regional/RegionalQuickstart.java?raw";
import regionalPythonSource from "../quickstarts/regional/quickstart.py?raw";
import regionalQueueGoSource from "../quickstarts/regional_queue/quickstart.go?raw";
import regionalQueueJavaSource from "../quickstarts/regional_queue/RegionalQueueQuickstart.java?raw";
import regionalQueuePythonSource from "../quickstarts/regional_queue/quickstart.py?raw";
import regionalCacheGoSource from "../quickstarts/regional_cache/quickstart.go?raw";
import regionalCacheJavaSource from "../quickstarts/regional_cache/RegionalCacheQuickstart.java?raw";
import regionalCachePythonSource from "../quickstarts/regional_cache/quickstart.py?raw";
import regionalBusGoSource from "../quickstarts/regional_bus/quickstart.go?raw";
import regionalBusJavaSource from "../quickstarts/regional_bus/RegionalBusQuickstart.java?raw";
import regionalBusPythonSource from "../quickstarts/regional_bus/quickstart.py?raw";

export const repositoryUrl = "https://github.com/Ripan-Roy/epoch";
export const repositoryDocsUrl = `${repositoryUrl}/blob/main/docs`;
export const releaseVersion = "0.1.0-alpha.4";

export type LanguageId = "go" | "java" | "python";

export interface LanguageMeta {
  id: LanguageId;
  label: string;
  version: string;
}

export const languages: ReadonlyArray<LanguageMeta> = [
  { id: "go", label: "Go", version: "Go 1.26" },
  { id: "java", label: "Java", version: "Java 25" },
  { id: "python", label: "Python", version: "Python 3.11+" },
];

export interface LanguageGuide {
  id: LanguageId;
  label: string;
  version: string;
  setupTitle: string;
  setup: string;
  filename: string;
  source: string;
  run: string;
  errorType: string;
  errorDetail: string;
}

export const nodeStart = `git clone https://github.com/Ripan-Roy/epoch.git
cd epoch
cargo run -p epoch-node -- --data-dir .epoch`;

export const nodeRestart = `# In the node terminal, press Ctrl-C, then restart with the same data directory:
cargo run -p epoch-node -- --data-dir .epoch`;

export const regionalNodes = `# Terminal A · build and start three fixed voters
make compose-regional-up`;

export const consensusCheckpoint = `# Start the disposable fixed-voter probe
make compose-probe-up

# Inspect voter-local checkpoint and retained-log positions
curl --fail --silent --show-error \
  http://127.0.0.1:17701/experimental/v1/consensus/status

# Fsync a native-profile checkpoint and atomically reclaim old EPRS generations
curl --fail-with-body --request POST \
  http://127.0.0.1:17701/experimental/v1/consensus/checkpoints`;

export const regionalControl = `# Terminal B · keep the managed bridge running
EPOCH_CONTROL_REGIONAL_ENDPOINTS=http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663 \
EPOCH_CONTROL_STATE_PATH=.epoch/control/registry.db \
EPOCH_AUTH_POLICY_PATH=spec/auth/bootstrap-policy-v1.example.json \
EPOCH_CONTROL_REGIONAL_TOKEN=epoch-dev-control-v1 \
go run ./control/cmd/epoch-control`;

export const regionalResource = `# Terminal C · create one three-shard replicated Stream
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-orders-3-shards-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"stream","name":"orders",
      "spec":{"shard_count":3,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

export const regionalQueueResource = `# Terminal C · create one replicated Queue
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
  }'`;

export const regionalCacheResource = `# Terminal C · create one replicated Cache
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-sessions-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"cache","name":"sessions",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

export const regionalBusResource = `# Terminal C · create one replicated Event Bus
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-events-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"event-bus","name":"events",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

export const regionalWebhookConfiguration = `# On every regional node
EPOCH_REGIONAL_WEBHOOK_SIGNING_KEYS_PATH=/etc/epoch/webhook-keys.json
EPOCH_REGIONAL_WEBHOOK_DELIVERY_INTERVAL_MS=100

# /etc/epoch/webhook-keys.json (development example only)
{"format_version":1,"keys":[{"id":"primary","secret":"replace-with-at-least-32-byte-secret"}]}`;

export const languageGuides: ReadonlyArray<LanguageGuide> = [
  {
    id: "go",
    label: "Go",
    version: "Go 1.26",
    setupTitle: "Use the repository-local module",
    setup: `# From the repository root
go version
# Save the example below as quickstart.go`,
    filename: "quickstart.go",
    source: goSource,
    run: `go run ./quickstart.go seed
# Restart epoch-node in the other terminal, then:
go run ./quickstart.go verify`,
    errorType: "*epoch.APIError",
    errorDetail: "Inspect StatusCode, Code, Detail, and Retryable().",
  },
  {
    id: "java",
    label: "Java",
    version: "Java 25",
    setupTitle: "Build the local Maven artifact and classpath",
    setup: `# From the repository root
cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \\
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
# Save the example below as Quickstart.java
javac -cp "$EPOCH_JAVA_CP" Quickstart.java`,
    filename: "Quickstart.java",
    source: javaSource,
    run: `java -cp ".:$EPOCH_JAVA_CP" Quickstart seed
# Restart epoch-node in the other terminal, then:
java -cp ".:$EPOCH_JAVA_CP" Quickstart verify`,
    errorType: "EpochApiException",
    errorDetail: "Inspect status(), code(), detail(), and retryable().",
  },
  {
    id: "python",
    label: "Python",
    version: "Python 3.11+",
    setupTitle: "Install the typed SDK from this checkout",
    setup: `# From the repository root
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python
# Save the example below as quickstart.py`,
    filename: "quickstart.py",
    source: pythonSource,
    run: `python quickstart.py seed
# Restart epoch-node in the other terminal, then:
python quickstart.py verify`,
    errorType: "EpochAPIError",
    errorDetail: "Inspect status, code, detail, and retryable.",
  },
];

export interface RegionalGuide {
  filename: string;
  source: string;
  setup: string;
  run: string;
}

export const regionalLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "quickstart.go",
    source: regionalGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional/quickstart.go",
  },
  java: {
    filename: "RegionalQuickstart.java",
    source: regionalJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional/RegionalQuickstart.java \
  -d target/regional-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-docs-classes:$EPOCH_JAVA_CP" RegionalQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalPythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional/quickstart.py",
  },
};

export const regionalQueueLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "quickstart.go",
    source: regionalQueueGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional_queue/quickstart.go",
  },
  java: {
    filename: "RegionalQueueQuickstart.java",
    source: regionalQueueJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional_queue/RegionalQueueQuickstart.java \
  -d target/regional-queue-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-queue-docs-classes:$EPOCH_JAVA_CP" RegionalQueueQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalQueuePythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional_queue/quickstart.py",
  },
};

export const regionalCacheLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "quickstart.go",
    source: regionalCacheGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional_cache/quickstart.go",
  },
  java: {
    filename: "RegionalCacheQuickstart.java",
    source: regionalCacheJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional_cache/RegionalCacheQuickstart.java \
  -d target/regional-cache-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-cache-docs-classes:$EPOCH_JAVA_CP" RegionalCacheQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalCachePythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional_cache/quickstart.py",
  },
};

export const regionalBusLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "quickstart.go",
    source: regionalBusGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional_bus/quickstart.go",
  },
  java: {
    filename: "RegionalBusQuickstart.java",
    source: regionalBusJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional_bus/RegionalBusQuickstart.java \
  -d target/regional-bus-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-bus-docs-classes:$EPOCH_JAVA_CP" RegionalBusQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalBusPythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional_bus/quickstart.py",
  },
};

export const signedWebhookLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "receiver.go",
    setup: `subscription := epoch.Subscription{
    Name: "orders-webhook",
    Filter: epoch.EventFilter{EventTypePatterns: []string{"order.*"}},
    Target: epoch.SignedWebhookTarget("https://receiver.example/orders", "primary"),
}
_, err := client.UpsertSubscription(ctx, "events", 0, "orders-webhook-v1", subscription)`,
    source: `// Read the raw body before JSON decoding.
verified, err := epoch.VerifyWebhookSignature(
    secret, rawBody,
    request.Header.Get("epoch-delivery-id"),
    request.Header.Get("epoch-delivery-attempt"),
    request.Header.Get("epoch-signature-timestamp"),
    request.Header.Get("epoch-signature"),
    time.Now(), 5*time.Minute,
)
if err != nil { /* return 401 without side effects */ }
// Atomically insert (verified.DeliveryID, verified.Attempt) into your inbox,
// apply the business effect only on first insert, then return 204.`,
    run: "go test ./sdk/go/epoch -run TestVerifyWebhook -count=1",
  },
  java: {
    filename: "Receiver.java",
    setup: `Subscription subscription = new Subscription(
    "orders-webhook",
    SubscriptionTarget.signedWebhook("https://receiver.example/orders", "primary"));
client.upsertSubscription("events", 0, "orders-webhook-v1", subscription);`,
    source: `// Read the exact byte[] body before JSON decoding.
WebhookSignatures.Verification verified = WebhookSignatures.verify(
    secret, rawBody,
    request.getHeader("epoch-delivery-id"),
    request.getHeader("epoch-delivery-attempt"),
    request.getHeader("epoch-signature-timestamp"),
    request.getHeader("epoch-signature"),
    Instant.now(), Duration.ofMinutes(5));
// Atomically claim (verified.deliveryId(), verified.attempt()) in your inbox,
// apply the business effect only on first insert, then return 204.`,
    run: "cd sdk/java && ./mvnw -q -Dtest=WebhookSignaturesTest test",
  },
  python: {
    filename: "receiver.py",
    setup: `subscription = Subscription(
    "orders-webhook",
    SubscriptionTarget.signed_webhook("https://receiver.example/orders", "primary"),
    filter=EventFilter(event_type_patterns=["order.*"]),
)
client.upsert_subscription("events", 0, "orders-webhook-v1", subscription)`,
    source: `# Read the exact bytes body before JSON decoding.
verified = verify_webhook_signature(
    secret,
    raw_body,
    request.headers["epoch-delivery-id"],
    request.headers["epoch-delivery-attempt"],
    request.headers["epoch-signature-timestamp"],
    request.headers["epoch-signature"],
    tolerance_seconds=300,
)
# Atomically claim (verified.delivery_id, verified.attempt) in your inbox,
# apply the business effect only on first insert, then return 204.`,
    run: "cd sdk/python && PYTHONPATH=src python3 -m unittest tests.test_webhook -v",
  },
};

export const sdkSurface = [
  {
    area: "Connection",
    go: "NewClient · NewClientWithTransport",
    java: "new EpochClient(…)",
    python: "EpochClient(…)",
  },
  {
    area: "Node",
    go: "Health · Resources",
    java: "health · resources",
    python: "health · resources",
  },
  {
    area: "Cache",
    go: "CreateCache · CacheSet · CacheGet · CacheDelete · CacheIncrement",
    java: "createCache · cacheSet · cacheGet · cacheDelete · cacheIncrement",
    python: "create_cache · cache_set · cache_get · cache_delete · cache_increment",
  },
  {
    area: "Stream",
    go: "CreateStream · AppendStream · FetchStream · CommitStreamOffset · StreamLag",
    java: "createStream · appendStream · fetchStream · commitStreamOffset · streamLag",
    python: "create_stream · append_stream · fetch_stream · commit_stream_offset · stream_lag",
  },
  {
    area: "Regional Stream",
    go: "RegionalStreamClient · StreamShardFor · AppendKeyed · Append · EncodeStreamBatch · NewStreamBatchFrame · AppendBatch · Fetch · CommitOffset · Lag · FetchGroup · ClaimGroup · FetchClaimedGroup · ClaimConsumerSession · JoinConsumerSession · HeartbeatConsumerSession · LeaveConsumerSession · MaintainConsumerSession · ConsumerSession · ConfigureRetention · MaintainRetention · Retention",
    java: "RegionalStreamClient · StreamPartitioner.shardFor · appendKeyed · append · StreamBatchFrame.encode · StreamBatchFrame.compressed · appendBatch · fetch · commitOffset · lag · fetchGroup · claimGroup · fetchClaimedGroup · claimConsumerSession · joinConsumerSession · heartbeatConsumerSession · leaveConsumerSession · maintainConsumerSession · consumerSession · configureRetention · maintainRetention · retention",
    python:
      "RegionalStreamClient · stream_shard_for · append_keyed · append · StreamBatchFrame.encode · StreamBatchFrame.from_compressed · append_batch · fetch · commit_offset · lag · fetch_group · claim_group · fetch_claimed_group · claim_consumer_session · join_consumer_session · heartbeat_consumer_session · leave_consumer_session · maintain_consumer_session · consumer_session · configure_retention · maintain_retention · retention",
  },
  {
    area: "Regional Queue",
    go: "RegionalQueueClient · Enqueue · Acquire · ExtendLease · Acknowledge · Release · Nack · Reject · Redrive · Maintain · Counts · ConsumerFlow",
    java: "RegionalQueueClient · enqueue · acquire · extendLease · acknowledge · release · nack · reject · redrive · maintain · counts · consumerFlow",
    python:
      "RegionalQueueClient · enqueue · acquire · extend_lease · acknowledge · release · nack · reject · redrive · maintain · counts · consumer_flow",
  },
  {
    area: "Regional Cache",
    go: "RegionalCacheClient · Set · Delete · CompareAndSet · Increment · Transaction · AcquireLock · RenewLock · ReleaseLock · Maintain · Observe",
    java: "RegionalCacheClient · set · delete · compareAndSet · increment · transaction · acquireLock · renewLock · releaseLock · maintain · observe",
    python:
      "RegionalCacheClient · set · delete · compare_and_set · increment · transaction · acquire_lock · renew_lock · release_lock · maintain · observe",
  },
  {
    area: "Regional Event Bus",
    go: "RegionalBusClient · SignedWebhookTarget · VerifyWebhookSignature · UpsertSubscription · RemoveSubscription · Publish · AcquireDeliveries · AcknowledgeDelivery · FailDelivery · RejectDelivery · MaintainDeliveries · Mutation · ReplayArchive · QueryDeliveries · Status",
    java: "RegionalBusClient · SubscriptionTarget.signedWebhook · WebhookSignatures.verify · upsertSubscription · removeSubscription · publish · acquireDeliveries · acknowledgeDelivery · failDelivery · rejectDelivery · maintainDeliveries · mutation · replayArchive · queryDeliveries · status",
    python:
      "RegionalBusClient · SubscriptionTarget.signed_webhook · verify_webhook_signature · upsert_subscription · remove_subscription · publish · acquire_deliveries · acknowledge_delivery · fail_delivery · reject_delivery · maintain_deliveries · mutation · replay_archive · query_deliveries · status",
  },
  {
    area: "Queue",
    go: "CreateQueue · Send · Receive · Acknowledge · Release · Reject · ExtendLease · QueueCounts · Redrive",
    java: "createQueue · send · receive · acknowledge · release · reject · extendLease · queueCounts · redrive",
    python:
      "create_queue · send · receive · acknowledge · release · reject · extend_lease · queue_counts · redrive",
  },
  {
    area: "Event Bus",
    go: "CreateBus · Publish · UpsertSubscription · RemoveSubscription · ReplayBus",
    java: "createBus · publish · upsertSubscription · removeSubscription · replayBus",
    python: "create_bus · publish · upsert_subscription · remove_subscription · replay_bus",
  },
] as const;
