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
export const releaseVersion = "0.2.0-beta.6";

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

export const kubernetesInstall = `# Build the cluster images and optional containerized CLI
docker build -f deploy/docker/Dockerfile.node -t registry.example/epoch-node:beta.1 .
docker build -f deploy/docker/Dockerfile.control -t registry.example/epoch-control:beta.1 .
docker build -f deploy/docker/Dockerfile.operator -t registry.example/epoch-operator:beta.1 .
docker build -f deploy/docker/Dockerfile.cli -t registry.example/epoch-cli:beta.1 .

# Install the CRD, least-privilege RBAC, and two-replica leader-elected operator
kubectl create namespace epoch-system
kubectl apply -k deploy/kubernetes/operator

# Supply policy, credential, and CA-issued workload identity references
kubectl -n epoch-system create configmap epoch-auth-policy \
  --from-file=bootstrap-policy.json=spec/auth/bootstrap-policy-v1.example.json
kubectl -n epoch-system create secret generic epoch-control-credentials \
  --from-literal=regional-token="$EPOCH_CONTROL_REGIONAL_TOKEN"
kubectl -n epoch-system create secret generic epoch-data-plane-tls \
  --from-file=ca.crt=/secure/epoch-ca.crt \
  --from-file=tls.crt=/secure/epoch-data-plane.crt \
  --from-file=tls.key=/secure/epoch-data-plane.key
kubectl -n epoch-system create secret generic epoch-control-plane-tls \
  --from-file=ca.crt=/secure/epoch-ca.crt \
  --from-file=tls.crt=/secure/epoch-control-plane.crt \
  --from-file=tls.key=/secure/epoch-control-plane.key

# Supply the encrypted semantic-backup destination and 32-byte key
umask 077
head -c 32 /dev/urandom > epoch-backup.key
kubectl -n epoch-system create secret generic epoch-backup-key \
  --from-file=encryption.key=epoch-backup.key
rm -f epoch-backup.key
kubectl apply -f deploy/kubernetes/operator/sample-backup-pvc.yaml

# Edit image names in the sample, then create the regional cluster
kubectl apply -f deploy/kubernetes/operator/sample-cluster.yaml
kubectl -n epoch-system get epochclusters.platform.epoch.dev -w`;

export const kubernetesAlphaExitCampaign = `# Contract tests do not require a cluster.
make test-kubernetes-runner

# Build exact local images, create a disposable one-control/four-worker Kind
# cluster, run the complete managed lifecycle, write evidence, and clean up.
tests/integration/kubernetes_alpha_exit.py \\
  --cluster-name epoch-alpha-exit-local \\
  --evidence-dir /secure/evidence/epoch-kubernetes-alpha-exit

# Verify every retained evidence file against the bundle manifest.
cd /secure/evidence/epoch-kubernetes-alpha-exit
sha256sum --check manifest.sha256`;

export const releaseArtifactVerification = `# Exact tags are discovery handles; deploy the verified digest.
export EPOCH_RELEASE_TAG=v0.2.0-beta.6
export EPOCH_IMAGE=ghcr.io/ripan-roy/epoch-node

docker buildx imagetools inspect "$EPOCH_IMAGE:$EPOCH_RELEASE_TAG"
export EPOCH_IMAGE_DIGEST=sha256:replace-with-the-inspected-manifest-digest

cosign verify \
  --certificate-identity \
  "https://github.com/Ripan-Roy/epoch/.github/workflows/release-tag.yml@refs/tags/$EPOCH_RELEASE_TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$EPOCH_IMAGE@$EPOCH_IMAGE_DIGEST"

gh attestation verify \
  "oci://$EPOCH_IMAGE@$EPOCH_IMAGE_DIGEST" \
  --repo Ripan-Roy/epoch

docker pull "$EPOCH_IMAGE@$EPOCH_IMAGE_DIGEST"`;

export const soakCampaign = `# The fast profile proves the resumable harness, not a 30-day SLO.
export EPOCH_SOAK_DIR=/secure/evidence/epoch-accelerated
export EPOCH_SOAK_KEY=/run/secrets/epoch-soak-ed25519.pem

python3 tests/soak/epoch_soak.py run \
  --profile accelerated \
  --state-dir "$EPOCH_SOAK_DIR" \
  --signing-key "$EPOCH_SOAK_KEY"

python3 tests/soak/epoch_soak.py verify \
  --manifest "$EPOCH_SOAK_DIR/evidence.json" \
  --public-key "$EPOCH_SOAK_DIR/evidence-public.pem"

# The long profile resumes only with the exact same source, image, and plan.
python3 tests/soak/epoch_soak.py run \
  --profile thirty-day \
  --state-dir /secure/evidence/epoch-thirty-day \
  --signing-key "$EPOCH_SOAK_KEY"`;

export const backupRestoreSpec = `spec:
  backup:
    schedule: "*/15 * * * *"
    destinationPVC: epoch-backups
    encryptionSecret: epoch-backup-key-current
    keyID: backup-key-2026-09
    retentionCount: 7
  restore:
    objectName: 1787520000000-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.epoch-backup.enc
    encryptionSecret: epoch-backup-key-2026-08`;

export const backupStatus = `# Observe schedule, Jobs, and the exact encrypted object receipt
kubectl -n epoch-system get cronjob epoch-backup
kubectl -n epoch-system get jobs \
  -l platform.epoch.dev/backup-owner=epoch
kubectl -n epoch-system get epochcluster epoch \
  -o jsonpath='{.status.backup}'

# Authenticate and decode one artifact in a controlled admin environment
epoch-backup decrypt \
  --input /backups/OBJECT.epoch-backup.enc \
  --encryption-key /secure/epoch-backup.key \
  --output /tmp/epoch-regional-backup.json`;

export const guardedUpgradeSpec = `spec:
  nodeImage: ghcr.io/ripan-roy/epoch-node:v0.2.0-beta.6
  upgrade:
    backupMaxAgeSeconds: 3600
    stepDeadlineSeconds: 900
    rollbackOnFailure: true
    # Change only to retry the same failed target image.
    retryToken: attempt-2`;

export const guardedUpgradeStatus = `# Durable phase, image, ordinal, deadline, and rollback state
kubectl -n epoch-system get epochcluster epoch \\
  -o jsonpath='{.status.upgrade}'

# Immutable mTLS verification/drain Jobs for this plan
kubectl -n epoch-system get jobs \\
  -l platform.epoch.dev/upgrade-owner=epoch

# The operator lowers this by exactly one only after drain succeeds
kubectl -n epoch-system get statefulset epoch-node \\
  -o jsonpath='{.spec.updateStrategy.rollingUpdate.partition}'`;

export const voterReplacementPlan = `# Discover the tablet ID, epoch, generation, and current voters first.
# The supported deployment uses TLS/mTLS and a principal with catalog.apply.
curl --fail-with-body --request POST \
  https://epoch.example/experimental/v1/regional/catalog/tablets/41/membership \
  --cacert /secure/epoch-ca.crt \
  --cert /secure/epoch-admin.crt \
  --key /secure/epoch-admin.key \
  --header "authorization: Bearer $EPOCH_TOKEN" \
  --header 'content-type: application/json' \
  --data '{
    "request_token": "replace-orders-3-with-4-v1",
    "expected_tablet_epoch": "1",
    "expected_resource_generation": "7",
    "target_voter_node_ids": ["1", "2", "4"]
  }'`;

export const voterReplacementStatus = `# During catch-up this reports pending with current and target voters.
curl --fail-with-body \
  https://epoch-control.example/v1/regional/resources \
  --cacert /secure/epoch-ca.crt \
  --header "authorization: Bearer $EPOCH_TOKEN"

# Focused four-node plan → catch-up → finalize → reopen proof
cargo test -p epoch-node --lib \
  catalog_planned_voter_replacement_catches_up_finalizes_and_reopens`;

export const managementCli = `# Build and verify both management boundaries
go build -o ./bin/epoch ./control/cmd/epoch
EPOCH_TOKEN=epoch-dev-admin-v1 ./bin/epoch doctor

# Apply strict protobuf JSON or YAML with an automatically generated retry token
EPOCH_TOKEN=epoch-dev-admin-v1 ./bin/epoch apply --file resource.yaml

# Read the fully qualified resource and preserve 64-bit generations
EPOCH_TOKEN=epoch-dev-admin-v1 ./bin/epoch get \
  acme/shop/dev/core/stream/orders`;

export const sourceConnectorContract = `# The active Bus leader sends these headers
GET /events HTTP/1.1
Accept: application/json
Epoch-Connector-Identity: orders-source-reader
Epoch-Connector-Position: cursor-10

# Return one bounded strict batch, or HTTP 204 when no work is ready
{
  "batch_id": "orders-11",
  "source_from": "cursor-10",
  "source_to": "cursor-11",
  "events": [{
    "id": "order-11",
    "source": "urn:orders",
    "type": "order.created",
    "time_ms": 11,
    "payload": {"order_id": 11}
  }]
}`;

export const governanceInventory = `# Filter with exact AND semantics
curl --fail --get http://127.0.0.1:8080/v1/regional/resources \\
  --header 'authorization: Bearer epoch-dev-admin-v1' \\
  --data-urlencode 'owner=team:platform' \\
  --data-urlencode 'cost_center=cc-1042' \\
  --data-urlencode 'classification=confidential' \\
  --data-urlencode 'tag=service=orders' \\
  --data-urlencode 'tag=profile=stream'`;

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
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"confidential","tags":{"service":"orders","profile":"stream"}},
      "spec":{"shard_count":3,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

export const regionalQueueResource = `# Terminal C · create the dead-letter target first
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-failed-jobs-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"queue","name":"failed-jobs",
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"internal","tags":{"service":"jobs","profile":"queue"}},
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'

# Create one bounded replicated Queue with all state services enabled
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
        "retry":{"strategy":"exponential","initial_delay_ms":1000,
          "max_delay_ms":60000,"jitter_percent":10,"max_attempts":8,"max_age_ms":null},
        "dedupe_window_ms":60000,
        "advanced":{"max_active_bytes":3145728,"overflow":"dead_letter_oldest",
          "idle_expiry_ms":600000,"priority_aging_interval_ms":1000,
          "dispatch":{"messages_per_second":1000,"burst":100,
            "max_in_flight":100,"failure_threshold":5,"open_interval_ms":30000},
          "dead_letter_target":"failed-jobs"}
      },"placement":{
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
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"confidential","tags":{"service":"sessions","profile":"cache"}},
      "spec":{"shard_count":1,"replica_count":3,"configuration":{
        "shard_count":1,"max_entries":10000,
        "max_memory_bytes":262144,"max_cold_bytes":262144,
        "default_ttl_ms":null,"eviction":"all_keys_lru",
        "durability":"quorum_durable"
      },"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

export const regionalBusResource = `# Terminal C · create the Queue and Stream targets
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
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'

curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-orders-target-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"stream","name":"orders",
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"confidential","tags":{"service":"orders","profile":"stream"}},
      "spec":{"shard_count":3,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'

# Create the replicated Event Bus
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-events-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"event_bus","name":"events",
      "governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"internal","tags":{"service":"events","profile":"event_bus"}},
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

export const epochTargetLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "targets.go",
    setup: "// QueueTarget and StreamTarget use the existing regional Bus client.",
    source: `queue := epoch.Subscription{
    Name: "queue-jobs",
    Filter: epoch.EventFilter{EventTypePatterns: []string{"target.*"}},
    Target: epoch.QueueTarget("jobs"),
}
stream := epoch.Subscription{
    Name: "stream-orders",
    Filter: epoch.EventFilter{EventTypePatterns: []string{"target.*"}},
    Target: epoch.StreamTarget("orders"),
}
_, _ = client.UpsertSubscription(ctx, "events", 0, "queue-jobs-v1", queue)
_, _ = client.UpsertSubscription(ctx, "events", 0, "stream-orders-v1", stream)`,
    run: "go run ./console/src/quickstarts/regional_bus/quickstart.go",
  },
  java: {
    filename: "Targets.java",
    setup: "// Queue and Stream targets need no application delivery worker.",
    source: `Subscription queue = new Subscription(
    "queue-jobs", SubscriptionTarget.queue("jobs"));
Subscription stream = new Subscription(
    "stream-orders", SubscriptionTarget.stream("orders"));
client.upsertSubscription("events", 0, "queue-jobs-v1", queue);
client.upsertSubscription("events", 0, "stream-orders-v1", stream);`,
    run: "java RegionalBusQuickstart",
  },
  python: {
    filename: "targets.py",
    setup: "# The regional source leader owns target execution.",
    source: `queue = Subscription("queue-jobs", SubscriptionTarget.queue("jobs"))
stream = Subscription("stream-orders", SubscriptionTarget.stream("orders"))
client.upsert_subscription("events", 0, "queue-jobs-v1", queue)
client.upsert_subscription("events", 0, "stream-orders-v1", stream)`,
    run: "python console/src/quickstarts/regional_bus/quickstart.py",
  },
};

export const regionalWebhookConfiguration = `# On every regional node
EPOCH_REGIONAL_WEBHOOK_SIGNING_KEYS_PATH=/etc/epoch/webhook-keys.json
EPOCH_REGIONAL_WEBHOOK_DELIVERY_INTERVAL_MS=100

# /etc/epoch/webhook-keys.json (development example only)
{"format_version":1,"keys":[{"id":"primary","secret":"replace-with-at-least-32-byte-secret"}]}`;

export const regionalManagedTargetConfiguration = `# On every regional node
EPOCH_REGIONAL_MANAGED_TARGET_SECRETS_PATH=/etc/epoch/managed-target-secrets.json
EPOCH_REGIONAL_MANAGED_TARGET_DELIVERY_INTERVAL_MS=100

# /etc/epoch/managed-target-secrets.json (development example only)
{"format_version":1,"secrets":[
  {"kind":"api_key","reference":"billing-key","value":"replace-me","header":"x-api-key"},
  {"kind":"oauth2_client","reference":"orders-oauth","client_id":"epoch","client_secret":"replace-me","token_url":"https://identity.example/token","scopes":["orders.write"]}
]}`;

export const managedTargetLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "managed_target.go",
    setup: "// Secret values remain in the node-local file; the subscription carries only references.",
    source: `auth := epoch.DestinationAuth{Kind: "oauth2", SecretRef: "orders-oauth",
    TokenURL: "https://identity.example/token", Scopes: []string{"orders.write"}}
subscription := epoch.Subscription{
    Name: "orders-api",
    Target: epoch.APIDestinationTarget("https://api.example/events", auth, "structured"),
}
_, err := client.UpsertSubscription(ctx, "events", 0, "orders-api-v1", subscription)`,
    run: "go test ./sdk/go/epoch -run RegionalBus -count=1",
  },
  java: {
    filename: "ManagedTarget.java",
    setup: "// DestinationAuth serializes a credential reference, never the secret value.",
    source: `DestinationAuth auth = DestinationAuth.oauth2(
    "orders-oauth", "https://identity.example/token", List.of("orders.write"));
Subscription subscription = new Subscription(
    "orders-api",
    SubscriptionTarget.apiDestination("https://api.example/events", auth, "structured"));
client.upsertSubscription("events", 0, "orders-api-v1", subscription);`,
    run: "cd sdk/java && ./mvnw -q -Dtest=RegionalBusClientTest test",
  },
  python: {
    filename: "managed_target.py",
    setup: "# The SDK validates the reference, URL, and CloudEvents mode before discovery.",
    source: `auth = DestinationAuth.oauth2(
    "orders-oauth", "https://identity.example/token", ("orders.write",)
)
subscription = Subscription(
    "orders-api",
    SubscriptionTarget.api_destination(
        "https://api.example/events", auth, "structured"
    ),
)
client.upsert_subscription("events", 0, "orders-api-v1", subscription)`,
    run: "cd sdk/python && PYTHONPATH=src python3 -m unittest tests.test_regional_bus -v",
  },
};

export const schemaLifecycleLanguageGuides: Record<LanguageId, RegionalGuide> = {
  go: {
    filename: "schema.go",
    setup: "// Typed registration and policy validation fail before discovery when malformed.",
    source: `_, err := client.RegisterSchema(ctx, "events", 0, "schema-order-v1", epoch.SchemaRegistration{
    Name: "order", Format: epoch.JSONSchema,
    Definition: \`{"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"type":"integer"}}}\`,
    Compatibility: epoch.BackwardSchemaCompatibility,
})
if err != nil { return err }
_, err = client.UpsertSchemaValidationPolicy(ctx, "events", 0, "schema-policy-orders-v1", epoch.SchemaValidationPolicy{
    Name: "orders", EventTypePattern: "order.*", SchemaRef: "order@1",
    Mode: epoch.ProducerAndBrokerSchemaValidation,
})
if err != nil { return err }
event.SchemaRef = "order@1"
_, err = client.ValidateSchema(ctx, "events", 0, epoch.ProducerValidationStage, event)
if err != nil { return err }
_, err = client.Publish(ctx, "events", 0, "publish-order-1", event)`,
    run: "go test ./sdk/go/epoch -run 'TestRegionalBusClient.*Schema' -count=1",
  },
  java: {
    filename: "SchemaLifecycle.java",
    setup: "// The same exact revision is used for advice and atomic broker enforcement.",
    source: `String definition = """
    {"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"type":"integer"}}}
    """;
client.registerSchema("events", 0, "schema-order-v1",
    new SchemaRegistration("order", SchemaFormat.JSON_SCHEMA, definition,
        SchemaCompatibility.BACKWARD));
client.upsertSchemaValidationPolicy("events", 0, "schema-policy-orders-v1",
    new SchemaValidationPolicy("orders", "order.*", "order@1",
        SchemaValidationMode.PRODUCER_AND_BROKER));
EventEnvelope event = EventEnvelope.builder("checkout", "order.created", Map.of("id", 42))
    .id("order-1").timeMs(1).schemaRef("order@1").build();
client.validateSchema("events", 0, SchemaValidationStage.PRODUCER, event);
client.publish("events", 0, "publish-order-1", event);`,
    run: "cd sdk/java && ./mvnw -q -Dtest=RegionalBusClientTest test",
  },
  python: {
    filename: "schema_lifecycle.py",
    setup: "# Server errors are bounded and do not reflect payload values.",
    source: `client.register_schema(
    "events", 0, "schema-order-v1",
    SchemaRegistration(
        "order", "json_schema",
        '{"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"type":"integer"}}}',
        "backward",
    ),
)
client.upsert_schema_validation_policy(
    "events", 0, "schema-policy-orders-v1",
    SchemaValidationPolicy("orders", "order.*", "order@1", "producer_and_broker"),
)
event = EventEnvelope(
    id="order-1", source="checkout", event_type="order.created", time_ms=1,
    schema_ref="order@1", payload={"id": 42},
)
client.validate_schema("events", 0, "producer", event)
client.publish("events", 0, "publish-order-1", event)`,
    run: "cd sdk/python && PYTHONPATH=src python3 -m unittest tests.test_regional_bus -v",
  },
};

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
    go: "RegionalStreamClient · StreamShardFor · AppendKeyed · Append · AppendBatch · Fetch · FetchWithIsolation · ConsumeLongPoll · AppendIdempotent · BeginTransaction · AppendTransaction · CommitTransaction · AbortTransaction · Transaction · Compact · TierPrefix · TierObjects · Capture · CaptureArtifact · ConfigureCaptureSchedule · CaptureSchedule · Replicate · PartitionAdvice · FetchSuperstream · CommitOffset · Lag · ClaimConsumerSession · ConfigureRetention",
    java: "RegionalStreamClient · StreamPartitioner.shardFor · appendKeyed · append · appendBatch · fetch · fetchWithIsolation · consumeLongPoll · appendIdempotent · beginTransaction · appendTransaction · commitTransaction · abortTransaction · transaction · compact · tierPrefix · tierObjects · capture · captureArtifact · configureCaptureSchedule · captureSchedule · replicate · partitionAdvice · fetchSuperstream · commitOffset · lag · claimConsumerSession · configureRetention",
    python:
      "RegionalStreamClient · stream_shard_for · append_keyed · append · append_batch · fetch · consume_long_poll · append_idempotent · begin_transaction · append_transaction · commit_transaction · abort_transaction · transaction · compact · tier_prefix · tier_objects · capture · capture_artifact · configure_capture_schedule · capture_schedule · replicate · partition_advice · fetch_superstream · commit_offset · lag · claim_consumer_session · configure_retention",
  },
  {
    area: "Regional Queue",
    go: "RegionalQueueClient · Enqueue · EnqueueAdvanced · Acquire · RenewSessionLock · ReleaseSessionLock · Defer · ReceiveDeferred · ExtendLease · Acknowledge · Release · Nack · Reject · Redrive · Maintain · Counts · ConsumerFlow · AdvancedStatus · Correlation · DeadLetterForwards",
    java: "RegionalQueueClient · enqueue · enqueueAdvanced · acquire · acquireSession · renewSessionLock · releaseSessionLock · defer · receiveDeferred · extendLease · acknowledge · release · nack · reject · redrive · maintain · counts · consumerFlow · advancedStatus · correlation · deadLetterForwards",
    python:
      "RegionalQueueClient · enqueue · acquire · renew_session_lock · release_session_lock · defer · receive_deferred · extend_lease · acknowledge · release · nack · reject · redrive · maintain · counts · consumer_flow · advanced_status · correlation · dead_letter_forwards",
  },
  {
    area: "Regional Cache",
    go: "RegionalCacheClient · Set · Get · Delete · CompareAndSet · Increment · Transform · Transaction · AtomicBatch · Multiplex · AcquireLock · RenewLock · ReleaseLock · Maintain · Observe · Changes · Backup · Restore · Query · CreateSubscription · Publish · PollSubscription · DeleteSubscription · Status",
    java: "RegionalCacheClient · set · get · delete · compareAndSet · increment · transform · transaction · atomicBatch · multiplex · acquireLock · renewLock · releaseLock · maintain · observe · changes · backup · restore · query · createSubscription · publish · pollSubscription · deleteSubscription · status",
    python:
      "RegionalCacheClient · set · get · delete · compare_and_set · increment · transform · transaction · atomic_batch · multiplex · acquire_lock · renew_lock · release_lock · maintain · observe · changes · backup · restore · query · create_subscription · publish · poll_subscription · delete_subscription · status",
  },
  {
    area: "Regional Event Bus",
    go: "RegionalBusClient · APIDestinationTarget · EndpointPoolTarget · FunctionTarget · ConnectorTarget · SignedWebhookTarget · VerifyWebhookSignature · UpsertSubscription · RemoveSubscription · Publish · AcquireDeliveries · AcknowledgeDelivery · FailDelivery · RejectDelivery · RedriveDelivery · MaintainDeliveries · MaintainArchive · RegisterSchema · UpsertSchemaValidationPolicy · RemoveSchemaValidationPolicy · ValidateSchema · ApplyIntegration · IntegrationState · Mutation · ReplayArchive · QueryDeliveries · Status",
    java: "RegionalBusClient · SubscriptionTarget.apiDestination/endpointPool/function/connector/signedWebhook · WebhookSignatures.verify · upsertSubscription · removeSubscription · publish · acquireDeliveries · acknowledgeDelivery · failDelivery · rejectDelivery · redriveDelivery · maintainDeliveries · maintainArchive · registerSchema · upsertSchemaValidationPolicy · removeSchemaValidationPolicy · validateSchema · applyIntegration · integrationState · mutation · replayArchive · queryDeliveries · status",
    python:
      "RegionalBusClient · SubscriptionTarget.api_destination/endpoint_pool/function/connector/signed_webhook · verify_webhook_signature · upsert_subscription · remove_subscription · publish · acquire_deliveries · acknowledge_delivery · fail_delivery · reject_delivery · redrive_delivery · maintain_deliveries · maintain_archive · register_schema · upsert_schema_validation_policy · remove_schema_validation_policy · validate_schema · apply_integration · integration_state · mutation · replay_archive · query_deliveries · status",
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
