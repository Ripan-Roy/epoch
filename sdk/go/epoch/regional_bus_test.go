package epoch

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestRegionalBusClientRoutesCompleteMutationAndReadContracts(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalBusClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := DefaultDeliveryPolicy()
	policy.Retry.Strategy = FixedDeliveryBackoff
	retentionMS := uint64(86_400_000)
	policy.RateLimit = &DeliveryRateLimit{DeliveriesPerSecond: 25, Burst: 50}
	policy.DeadLetterRetentionMS = &retentionMS
	subscription := Subscription{
		Name:           "orders",
		Filter:         EventFilter{EventTypePatterns: []string{"order.*"}},
		Target:         SignedWebhookTarget("https://example.com/orders", "primary"),
		DeliveryPolicy: &policy,
	}
	event := EventEnvelope{ID: "order-2", Source: "go-regional-sdk", Type: "order.created", TimeMS: 2, Payload: map[string]any{"id": 2}}
	ctx := context.Background()
	bus := "events/eu"

	if _, err = client.UpsertSubscription(ctx, bus, 0, "upsert-1", subscription); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Publish(ctx, bus, 0, "publish-1", event); err != nil {
		t.Fatal(err)
	}
	if _, err = client.AcquireDeliveries(ctx, bus, 0, "acquire-1", RegionalBusAcquireOptions{Subscription: "orders", Dispatcher: "worker-a", DispatcherEpoch: 7, MaxDeliveries: 10, Wait: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.AcknowledgeDelivery(ctx, bus, 0, "ack-1", "delivery-1", "worker-a", 7, "lease-1"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.FailDelivery(ctx, bus, 0, "fail-1", "delivery-2", "worker-a", 7, "lease-2", "downstream timeout"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RejectDelivery(ctx, bus, 0, "reject-1", "delivery-3", "worker-a", 7, "lease-3", "http status 400"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RedriveDelivery(ctx, bus, 0, "redrive-1", "delivery-3"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.MaintainDeliveries(ctx, bus, 0, "maintain-1", 100); err != nil {
		t.Fatal(err)
	}
	if _, err = client.MaintainArchive(ctx, bus, 0, "archive-retention-1", 100); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ApplyIntegration(ctx, bus, 0, "schema-1", Document{"kind": "register_schema", "registration": Document{"name": "orders"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RemoveSubscription(ctx, bus, 0, "remove-1", "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Mutation(ctx, bus, 0, 12); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ReplayArchive(ctx, bus, 0, RegionalBusReplayOptions{FromMS: 1, ToMS: 10, Limit: 100, Filter: &subscription.Filter}); err != nil {
		t.Fatal(err)
	}
	state := InFlightDelivery
	if _, err = client.QueryDeliveries(ctx, bus, 0, RegionalBusDeliveryQuery{Subscription: "orders", State: &state, Limit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Status(ctx, bus, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = client.IntegrationState(ctx, bus, 0); err != nil {
		t.Fatal(err)
	}

	base := "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/buses/events%2Feu/shards/0"
	var operations []Request
	for index := 1; index < len(leader.requests); index += 2 {
		operations = append(operations, leader.requests[index])
	}
	if len(operations) != 16 {
		t.Fatalf("expected 16 operations, got %d", len(operations))
	}
	if operations[0].Path != base+"/mutations" || operations[11].Path != base+"/mutations/12" {
		t.Fatalf("unexpected Bus paths: %#v", operations)
	}
	var upsert map[string]any
	payload, err := json.Marshal(operations[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &upsert); err != nil {
		t.Fatal(err)
	}
	operation := upsert["operation"].(map[string]any)
	if operation["kind"] != "upsert_subscription" || operation["subscription"].(map[string]any)["delivery_policy"] == nil {
		t.Fatalf("unexpected subscription operation: %#v", operation)
	}
	target := operation["subscription"].(map[string]any)["target"].(map[string]any)
	if target["signing_key_id"] != "primary" {
		t.Fatalf("signed target was not serialized: %#v", target)
	}
	deliveryPolicy := operation["subscription"].(map[string]any)["delivery_policy"].(map[string]any)
	if deliveryPolicy["dead_letter_retention_ms"] != float64(retentionMS) || deliveryPolicy["rate_limit"].(map[string]any)["burst"] != float64(50) {
		t.Fatalf("delivery controls were not serialized: %#v", deliveryPolicy)
	}
	var acquire map[string]any
	payload, err = json.Marshal(operations[2].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &acquire); err != nil {
		t.Fatal(err)
	}
	if acquire["operation"].(map[string]any)["wait_ms"] != float64(5_000) {
		t.Fatalf("long-poll wait was not serialized: %#v", acquire)
	}
	var rejection map[string]any
	payload, err = json.Marshal(operations[5].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &rejection); err != nil {
		t.Fatal(err)
	}
	if rejection["operation"].(map[string]any)["kind"] != "reject_delivery" {
		t.Fatalf("unexpected rejection operation: %#v", rejection)
	}
	var redrive map[string]any
	payload, err = json.Marshal(operations[6].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &redrive); err != nil {
		t.Fatal(err)
	}
	if redrive["operation"].(map[string]any)["kind"] != "redrive_delivery" {
		t.Fatalf("unexpected redrive operation: %#v", redrive)
	}
	var integration map[string]any
	payload, err = json.Marshal(operations[9].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &integration); err != nil {
		t.Fatal(err)
	}
	if integration["operation"].(map[string]any)["kind"] != "apply_integration" {
		t.Fatalf("unexpected integration operation: %#v", integration)
	}
	var archiveMaintenance map[string]any
	payload, err = json.Marshal(operations[8].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &archiveMaintenance); err != nil {
		t.Fatal(err)
	}
	if archiveMaintenance["operation"].(map[string]any)["kind"] != "maintain_archive" {
		t.Fatalf("unexpected archive maintenance operation: %#v", archiveMaintenance)
	}
	if operations[12].Path != base+"/archive/replay" || operations[13].Path != base+"/deliveries/query" || operations[15].Path != base+"/integration/state" {
		t.Fatalf("unexpected Bus read paths: %#v", operations[12:])
	}
	for _, request := range operations[11:] {
		if request.Headers[regionalReadHeader] != "linearizable" {
			t.Fatalf("read %q was not linearizable", request.Path)
		}
	}
}

func TestRegionalBusClientRejectsInvalidBoundsBeforeNetwork(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalBusClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = client.AcquireDeliveries(ctx, "events", 0, "acquire", RegionalBusAcquireOptions{Subscription: "orders", Dispatcher: "worker", DispatcherEpoch: 1, MaxDeliveries: 0}); err == nil {
		t.Fatal("zero acquire bound should fail")
	}
	if _, err = client.AcquireDeliveries(ctx, "events", 0, "acquire-wait", RegionalBusAcquireOptions{Subscription: "orders", Dispatcher: "worker", DispatcherEpoch: 1, MaxDeliveries: 1, Wait: 30*time.Second + time.Millisecond}); err == nil {
		t.Fatal("oversized long poll should fail")
	}
	if _, err = client.ReplayArchive(ctx, "events", 0, RegionalBusReplayOptions{FromMS: 10, ToMS: 1, Limit: 1}); err == nil {
		t.Fatal("reversed replay range should fail")
	}
	badPolicy := DefaultDeliveryPolicy()
	badPolicy.Retry.InitialDelayMS = badPolicy.Retry.MaxDelayMS + 1
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert", Subscription{Name: "orders", Target: PullTarget(), DeliveryPolicy: &badPolicy}); err == nil {
		t.Fatal("invalid delivery retry range should fail")
	}
	badRatePolicy := DefaultDeliveryPolicy()
	badRatePolicy.RateLimit = &DeliveryRateLimit{}
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert-rate", Subscription{Name: "orders", Target: PullTarget(), DeliveryPolicy: &badRatePolicy}); err == nil {
		t.Fatal("invalid delivery rate should fail")
	}
	if _, err = client.RedriveDelivery(ctx, "events", 0, "redrive", ""); err == nil {
		t.Fatal("empty delivery ID should fail")
	}
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert-signed", Subscription{Name: "orders", Target: SignedWebhookTarget("https://example.com/orders", "bad/key")}); err == nil {
		t.Fatal("invalid signing key ID should fail")
	}
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert-empty-key", Subscription{Name: "orders", Target: SignedWebhookTarget("https://example.com/orders", "")}); err == nil {
		t.Fatal("empty signing key ID should fail")
	}
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert-transform", Subscription{
		Name: "orders", Target: PullTarget(),
		Transform: EventTransform{Limits: &TransformLimits{MaxOperations: 64, MaxOutputBytes: 256 * 1024, MaxValueBytes: 64 * 1024, TimeoutMS: 100, NetworkAccess: true}},
	}); err == nil {
		t.Fatal("network-enabled deterministic transform should fail")
	}
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert-auth", Subscription{
		Name: "orders", Target: APIDestinationTarget("https://example.com/orders", DestinationAuth{Kind: "oauth2", SecretRef: "oauth", TokenURL: "file:///token"}, "binary"),
	}); err == nil {
		t.Fatal("non-HTTP OAuth token URL should fail")
	}
	if !reflect.DeepEqual(leader.requests, []Request(nil)) {
		t.Fatalf("invalid calls reached network: %#v", leader.requests)
	}
}

func TestRegionalBusClientRoutesTypedSchemaLifecycle(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalBusClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	event := EventEnvelope{ID: "order-2", Source: "go-regional-sdk", Type: "order.created", TimeMS: 2, Payload: map[string]any{"id": 2}}

	if _, err = client.RegisterSchema(ctx, "events", 0, "schema-1", SchemaRegistration{
		Name:          "orders",
		Format:        ProtobufSchema,
		Definition:    "syntax = \"proto3\"; message Order { string id = 1; }",
		Compatibility: BackwardSchemaCompatibility,
		RootMessage:   "Order",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.UpsertSchemaValidationPolicy(ctx, "events", 0, "policy-1", SchemaValidationPolicy{
		Name:             "orders",
		EventTypePattern: "order.*",
		SchemaRef:        "orders@1",
		Mode:             ProducerAndBrokerSchemaValidation,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ValidateSchema(ctx, "events", 0, ProducerValidationStage, event); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RemoveSchemaValidationPolicy(ctx, "events", 0, "policy-remove-1", "orders"); err != nil {
		t.Fatal(err)
	}

	var operations []Request
	for index := 1; index < len(leader.requests); index += 2 {
		operations = append(operations, leader.requests[index])
	}
	if len(operations) != 4 {
		t.Fatalf("expected four schema lifecycle operations, got %d", len(operations))
	}
	base := "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/buses/events/shards/0"
	if operations[0].Path != base+"/mutations" || operations[1].Path != base+"/mutations" || operations[2].Path != base+"/schema/validate" || operations[3].Path != base+"/mutations" {
		t.Fatalf("unexpected schema lifecycle paths: %#v", operations)
	}
	if operations[2].Headers[regionalReadHeader] != "linearizable" {
		t.Fatal("explicit schema validation was not linearizable")
	}

	var registration map[string]any
	payload, err := json.Marshal(operations[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &registration); err != nil {
		t.Fatal(err)
	}
	integration := registration["operation"].(map[string]any)["operation"].(map[string]any)
	registered := integration["registration"].(map[string]any)
	if integration["kind"] != "register_schema" || registered["format"] != "protobuf" || registered["root_message"] != "Order" {
		t.Fatalf("unexpected typed schema registration: %#v", integration)
	}

	var validation map[string]any
	payload, err = json.Marshal(operations[2].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &validation); err != nil {
		t.Fatal(err)
	}
	if validation["mode"] != "producer" || validation["envelope"].(map[string]any)["type"] != "order.created" {
		t.Fatalf("unexpected typed schema validation: %#v", validation)
	}
}

func TestRegionalBusClientRejectsInvalidSchemaLifecycleBeforeNetwork(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalBusClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	event := EventEnvelope{ID: "order-2", Source: "go-regional-sdk", Type: "order.created", TimeMS: 2, Payload: map[string]any{"id": 2}}

	registrations := []SchemaRegistration{
		{Name: "bad/name", Format: JSONSchema, Definition: "{}", Compatibility: NoSchemaCompatibility},
		{Name: "orders", Format: "xml", Definition: "<schema />", Compatibility: NoSchemaCompatibility},
		{Name: "orders", Format: JSONSchema, Definition: "", Compatibility: NoSchemaCompatibility},
		{Name: "orders", Format: JSONSchema, Definition: "{}", Compatibility: NoSchemaCompatibility, RootMessage: "Order"},
	}
	for _, registration := range registrations {
		if _, err = client.RegisterSchema(ctx, "events", 0, "schema", registration); err == nil {
			t.Fatalf("invalid schema registration reached the network: %#v", registration)
		}
	}
	policies := []SchemaValidationPolicy{
		{Name: "bad/name", EventTypePattern: "order.*", SchemaRef: "orders@1", Mode: BrokerSchemaValidation},
		{Name: "orders", EventTypePattern: "", SchemaRef: "orders@1", Mode: BrokerSchemaValidation},
		{Name: "orders", EventTypePattern: "order.*", SchemaRef: "", Mode: BrokerSchemaValidation},
		{Name: "orders", EventTypePattern: "order.*", SchemaRef: "orders@1", Mode: "unknown"},
	}
	for _, policy := range policies {
		if _, err = client.UpsertSchemaValidationPolicy(ctx, "events", 0, "policy", policy); err == nil {
			t.Fatalf("invalid schema policy reached the network: %#v", policy)
		}
	}
	if _, err = client.RemoveSchemaValidationPolicy(ctx, "events", 0, "remove", "bad/name"); err == nil {
		t.Fatal("invalid schema policy name should fail")
	}
	if _, err = client.ValidateSchema(ctx, "events", 0, "unknown", event); err == nil {
		t.Fatal("invalid schema validation stage should fail")
	}
	if !reflect.DeepEqual(leader.requests, []Request(nil)) {
		t.Fatalf("invalid schema lifecycle calls reached network: %#v", leader.requests)
	}
}
