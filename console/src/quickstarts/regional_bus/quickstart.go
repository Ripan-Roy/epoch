package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
)

func main() {
	client, err := epoch.NewRegionalBusClient(
		strings.Split(environment("EPOCH_REGIONAL_ENDPOINTS", "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663"), ","),
		environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
		epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
		3*time.Second,
	)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	schema, err := client.RegisterSchema(ctx, "events", 0, "docs-go-bus-schema-v1", epoch.SchemaRegistration{
		Name: "order", Format: epoch.JSONSchema,
		Definition:    `{"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"type":"integer"}}}`,
		Compatibility: epoch.BackwardSchemaCompatibility,
	})
	must(err)
	schemaPolicy, err := client.UpsertSchemaValidationPolicy(ctx, "events", 0, "docs-go-bus-schema-policy-v1", epoch.SchemaValidationPolicy{
		Name: "orders", EventTypePattern: "order.*", SchemaRef: "order@1",
		Mode: epoch.ProducerAndBrokerSchemaValidation,
	})
	must(err)
	policy := epoch.DefaultDeliveryPolicy()
	policy.Retry.Strategy = epoch.FixedDeliveryBackoff
	subscription := epoch.Subscription{
		Name: "orders", Filter: epoch.EventFilter{EventTypePatterns: []string{"order.*"}},
		Target: epoch.PullTarget(), DeliveryPolicy: &policy,
	}
	upserted, err := client.UpsertSubscription(ctx, "events", 0, "docs-go-bus-upsert-v1", subscription)
	must(err)
	queueSubscription := epoch.Subscription{
		Name: "queue-jobs", Filter: epoch.EventFilter{EventTypePatterns: []string{"target.*"}},
		Target: epoch.QueueTarget("jobs"), DeliveryPolicy: &policy,
	}
	queueUpserted, err := client.UpsertSubscription(ctx, "events", 0, "docs-go-bus-queue-target-v1", queueSubscription)
	must(err)
	streamSubscription := epoch.Subscription{
		Name: "stream-orders", Filter: epoch.EventFilter{EventTypePatterns: []string{"target.*"}},
		Target: epoch.StreamTarget("orders"), DeliveryPolicy: &policy,
	}
	streamUpserted, err := client.UpsertSubscription(ctx, "events", 0, "docs-go-bus-stream-target-v1", streamSubscription)
	must(err)
	event := epoch.EventEnvelope{ID: "docs-order-1", Source: "docs-go", Type: "order.created", TimeMS: uint64(time.Now().UnixMilli()), SchemaRef: "order@1", Payload: map[string]any{"id": 1}}
	validated, err := client.ValidateSchema(ctx, "events", 0, epoch.ProducerValidationStage, event)
	must(err)
	published, err := client.Publish(ctx, "events", 0, "docs-go-bus-publish-v1", event)
	must(err)
	replayed, err := client.Publish(ctx, "events", 0, "docs-go-bus-publish-v1", event)
	must(err)
	acquired, err := client.AcquireDeliveries(ctx, "events", 0, "docs-go-bus-acquire-v1", epoch.RegionalBusAcquireOptions{Subscription: "orders", Dispatcher: "docs-go", DispatcherEpoch: 1, MaxDeliveries: 1})
	must(err)
	delivery := result(acquired)["deliveries"].([]any)[0].(map[string]any)
	acknowledged, err := client.AcknowledgeDelivery(ctx, "events", 0, "docs-go-bus-ack-v1", delivery["delivery_id"].(string), "docs-go", 1, delivery["lease_token"].(string))
	must(err)
	targetEvent := epoch.EventEnvelope{ID: "docs-target-1", Source: "docs-go", Type: "target.created", TimeMS: uint64(time.Now().UnixMilli()), Key: "customer-42", Payload: map[string]any{"id": 2}}
	targetPublished, err := client.Publish(ctx, "events", 0, "docs-go-bus-target-publish-v1", targetEvent)
	must(err)
	queueDelivery := waitForTarget(ctx, client, "queue-jobs", "queue")
	streamDelivery := waitForTarget(ctx, client, "stream-orders", "stream")
	archive, err := client.ReplayArchive(ctx, "events", 0, epoch.RegionalBusReplayOptions{FromMS: 0, ToMS: ^uint64(0), Limit: 100, Filter: &subscription.Filter})
	must(err)
	state := epoch.AcknowledgedDelivery
	deliveries, err := client.QueryDeliveries(ctx, "events", 0, epoch.RegionalBusDeliveryQuery{Subscription: "orders", State: &state, Limit: 100})
	must(err)
	status, err := client.Status(ctx, "events", 0)
	must(err)

	output, err := json.MarshalIndent(map[string]any{
		"schema": schema, "schema_policy": schemaPolicy, "schema_validation": validated,
		"upsert": upserted, "queue_target_upsert": queueUpserted, "stream_target_upsert": streamUpserted,
		"publish": published, "exact_retry": replayed, "target_publish": targetPublished,
		"queue_delivery": queueDelivery, "stream_delivery": streamDelivery,
		"acknowledge": acknowledged, "archive": archive, "deliveries": deliveries, "status": status,
	}, "", "  ")
	must(err)
	fmt.Println(string(output))
}

func waitForTarget(ctx context.Context, client *epoch.RegionalBusClient, subscription, kind string) map[string]any {
	state := epoch.AcknowledgedDelivery
	for {
		document, err := client.QueryDeliveries(ctx, "events", 0, epoch.RegionalBusDeliveryQuery{Subscription: subscription, State: &state, Limit: 100})
		must(err)
		for _, value := range document["records"].([]any) {
			record := value.(map[string]any)
			destination, ok := record["destination"].(map[string]any)
			if ok && destination["kind"] == kind {
				return record
			}
		}
		select {
		case <-ctx.Done():
			panic(fmt.Errorf("waiting for %s target delivery: %w", kind, ctx.Err()))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func result(document epoch.Document) map[string]any {
	return document["receipt"].(map[string]any)["outcome"].(map[string]any)["result"].(map[string]any)
}

func environment(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
