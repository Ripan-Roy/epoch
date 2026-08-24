package epoch

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	maxRegionalBusDeliveryBatch = 100
	maxRegionalBusReadResults   = 10_000
	maxRegionalBusLongPoll      = 30 * time.Second
)

// SchemaFormat selects one compiler-backed schema language.
type SchemaFormat string

const (
	AvroSchema     SchemaFormat = "avro"
	JSONSchema     SchemaFormat = "json_schema"
	ProtobufSchema SchemaFormat = "protobuf"
)

// SchemaCompatibility controls adjacent-revision admission.
type SchemaCompatibility string

const (
	NoSchemaCompatibility       SchemaCompatibility = "none"
	BackwardSchemaCompatibility SchemaCompatibility = "backward"
	ForwardSchemaCompatibility  SchemaCompatibility = "forward"
	FullSchemaCompatibility     SchemaCompatibility = "full"
)

// SchemaRegistration is one compiler-validated schema revision request.
type SchemaRegistration struct {
	Name          string              `json:"name"`
	Format        SchemaFormat        `json:"format"`
	Definition    string              `json:"definition"`
	Compatibility SchemaCompatibility `json:"compatibility"`
	RootMessage   string              `json:"root_message,omitempty"`
}

// SchemaValidationMode selects producer advice, broker enforcement, both, or neither.
type SchemaValidationMode string

const (
	DisabledSchemaValidation          SchemaValidationMode = "disabled"
	ProducerSchemaValidation          SchemaValidationMode = "producer"
	BrokerSchemaValidation            SchemaValidationMode = "broker"
	ProducerAndBrokerSchemaValidation SchemaValidationMode = "producer_and_broker"
)

// SchemaValidationPolicy binds an event-type pattern to one immutable revision.
type SchemaValidationPolicy struct {
	Name             string               `json:"name"`
	EventTypePattern string               `json:"event_type_pattern"`
	SchemaRef        string               `json:"schema_ref"`
	Mode             SchemaValidationMode `json:"mode"`
}

// SchemaValidationStage selects an explicit read-only validation path.
type SchemaValidationStage string

const (
	ProducerValidationStage SchemaValidationStage = "producer"
	BrokerValidationStage   SchemaValidationStage = "broker"
)

// RegionalBusAcquireOptions identifies one bounded dispatcher lease request.
type RegionalBusAcquireOptions struct {
	Subscription    string
	Dispatcher      string
	DispatcherEpoch uint64
	MaxDeliveries   uint16
	Wait            time.Duration
}

// RegionalBusReplayOptions bounds one linearizable archive replay.
type RegionalBusReplayOptions struct {
	FromMS uint64
	ToMS   uint64
	Limit  uint16
	Filter *EventFilter
}

// RegionalBusDeliveryState filters the replicated delivery ledger.
type RegionalBusDeliveryState string

const (
	PendingDelivery      RegionalBusDeliveryState = "pending"
	InFlightDelivery     RegionalBusDeliveryState = "in_flight"
	AcknowledgedDelivery RegionalBusDeliveryState = "acknowledged"
	DeadLetteredDelivery RegionalBusDeliveryState = "dead_lettered"
)

// RegionalBusDeliveryQuery bounds one linearizable delivery-ledger query.
type RegionalBusDeliveryQuery struct {
	Subscription string
	State        *RegionalBusDeliveryState
	Limit        uint16
}

// RegionalBusClient routes authenticated Event Bus calls across regional nodes.
// Mutation retries preserve the caller's exact idempotency key.
type RegionalBusClient struct {
	regional *regionalClient
}

// NewRegionalBusClient builds a regional Event Bus client over HTTP endpoints.
func NewRegionalBusClient(endpoints []string, token string, scope RegionalScope, timeout time.Duration) (*RegionalBusClient, error) {
	regional, err := newRegionalClient(endpoints, token, scope, timeout)
	if err != nil {
		return nil, err
	}
	return &RegionalBusClient{regional: regional}, nil
}

// NewRegionalBusClientWithTransports injects transports for tests or custom networking.
func NewRegionalBusClientWithTransports(transports []Transport, token string, scope RegionalScope) (*RegionalBusClient, error) {
	regional, err := newRegionalClientWithTransports(transports, token, scope)
	if err != nil {
		return nil, err
	}
	return &RegionalBusClient{regional: regional}, nil
}

// UpsertSubscription atomically creates or replaces a typed route plan entry.
func (client *RegionalBusClient) UpsertSubscription(ctx context.Context, bus string, shard uint32, idempotencyKey string, subscription Subscription) (Document, error) {
	subscription, err := subscription.normalized()
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind         string       `json:"kind"`
		Subscription Subscription `json:"subscription"`
	}{"upsert_subscription", subscription})
}

// RemoveSubscription deletes one exact route plan entry.
func (client *RegionalBusClient) RemoveSubscription(ctx context.Context, bus string, shard uint32, idempotencyKey, name string) (Document, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("epoch: subscription name is required")
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}{"remove_subscription", name})
}

// Publish routes and archives one strict event envelope.
func (client *RegionalBusClient) Publish(ctx context.Context, bus string, shard uint32, idempotencyKey string, event EventEnvelope) (Document, error) {
	event, err := event.normalized()
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind     string        `json:"kind"`
		Envelope EventEnvelope `json:"envelope"`
	}{"publish", event})
}

// AcquireDeliveries leases a bounded batch for one dispatcher epoch.
func (client *RegionalBusClient) AcquireDeliveries(ctx context.Context, bus string, shard uint32, idempotencyKey string, options RegionalBusAcquireOptions) (Document, error) {
	if strings.TrimSpace(options.Subscription) == "" {
		return nil, fmt.Errorf("epoch: subscription name is required")
	}
	if strings.TrimSpace(options.Dispatcher) == "" {
		return nil, fmt.Errorf("epoch: dispatcher is required")
	}
	if options.DispatcherEpoch == 0 {
		return nil, fmt.Errorf("epoch: dispatcher epoch must be non-zero")
	}
	if err := validateBusDeliveryBatch(options.MaxDeliveries); err != nil {
		return nil, err
	}
	if options.Wait < 0 || options.Wait > maxRegionalBusLongPoll || options.Wait%time.Millisecond != 0 {
		return nil, fmt.Errorf("epoch: delivery wait must be a whole number of milliseconds between 0 and %d", maxRegionalBusLongPoll.Milliseconds())
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind            string `json:"kind"`
		Subscription    string `json:"subscription"`
		Dispatcher      string `json:"dispatcher"`
		DispatcherEpoch string `json:"dispatcher_epoch"`
		MaxDeliveries   uint16 `json:"max_deliveries"`
		WaitMS          int64  `json:"wait_ms"`
	}{"acquire_deliveries", options.Subscription, options.Dispatcher, fmt.Sprintf("%d", options.DispatcherEpoch), options.MaxDeliveries, options.Wait.Milliseconds()})
}

// AcknowledgeDelivery permanently settles one fenced delivery lease.
func (client *RegionalBusClient) AcknowledgeDelivery(ctx context.Context, bus string, shard uint32, idempotencyKey, deliveryID, dispatcher string, dispatcherEpoch uint64, leaseToken string) (Document, error) {
	operation, err := busSettlement("acknowledge_delivery", deliveryID, dispatcher, dispatcherEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, operation)
}

// FailDelivery records a bounded failure and deterministic retry/dead-letter transition.
func (client *RegionalBusClient) FailDelivery(ctx context.Context, bus string, shard uint32, idempotencyKey, deliveryID, dispatcher string, dispatcherEpoch uint64, leaseToken, reason string) (Document, error) {
	operation, err := busSettlement("fail_delivery", deliveryID, dispatcher, dispatcherEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("epoch: delivery failure reason is required")
	}
	operation["reason"] = reason
	return client.mutate(ctx, bus, shard, idempotencyKey, operation)
}

// RejectDelivery records a terminal failure and dead-letters the fenced attempt.
func (client *RegionalBusClient) RejectDelivery(ctx context.Context, bus string, shard uint32, idempotencyKey, deliveryID, dispatcher string, dispatcherEpoch uint64, leaseToken, reason string) (Document, error) {
	operation, err := busSettlement("reject_delivery", deliveryID, dispatcher, dispatcherEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("epoch: delivery rejection reason is required")
	}
	operation["reason"] = reason
	return client.mutate(ctx, bus, shard, idempotencyKey, operation)
}

// RedriveDelivery returns one dead-lettered record to pending delivery with preserved history.
func (client *RegionalBusClient) RedriveDelivery(ctx context.Context, bus string, shard uint32, idempotencyKey, deliveryID string) (Document, error) {
	if strings.TrimSpace(deliveryID) == "" {
		return nil, fmt.Errorf("epoch: delivery ID is required")
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind       string `json:"kind"`
		DeliveryID string `json:"delivery_id"`
	}{"redrive_delivery", deliveryID})
}

// MaintainDeliveries applies due retry and expired-lease transitions explicitly.
func (client *RegionalBusClient) MaintainDeliveries(ctx context.Context, bus string, shard uint32, idempotencyKey string, maxDeliveries uint16) (Document, error) {
	if err := validateBusDeliveryBatch(maxDeliveries); err != nil {
		return nil, err
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind          string `json:"kind"`
		MaxDeliveries uint16 `json:"max_deliveries"`
	}{"maintain_deliveries", maxDeliveries})
}

// MaintainArchive applies bounded replicated age/count retention immediately.
func (client *RegionalBusClient) MaintainArchive(ctx context.Context, bus string, shard uint32, idempotencyKey string, maxEvents uint16) (Document, error) {
	if err := validateBusReadLimit(maxEvents); err != nil {
		return nil, err
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind      string `json:"kind"`
		MaxEvents uint16 `json:"max_events"`
	}{"maintain_archive", maxEvents})
}

// ApplyIntegration commits one typed schema, connector, MQTT, catalog, enrichment, or endpoint operation.
func (client *RegionalBusClient) ApplyIntegration(ctx context.Context, bus string, shard uint32, idempotencyKey string, operation Document) (Document, error) {
	if operation == nil {
		return nil, fmt.Errorf("epoch: integration operation is required")
	}
	kind, ok := operation["kind"].(string)
	if !ok || strings.TrimSpace(kind) == "" {
		return nil, fmt.Errorf("epoch: integration operation kind is required")
	}
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind      string   `json:"kind"`
		Operation Document `json:"operation"`
	}{"apply_integration", operation})
}

// RegisterSchema compiles and commits one immutable schema revision.
func (client *RegionalBusClient) RegisterSchema(ctx context.Context, bus string, shard uint32, idempotencyKey string, registration SchemaRegistration) (Document, error) {
	if err := registration.validate(); err != nil {
		return nil, err
	}
	return client.ApplyIntegration(ctx, bus, shard, idempotencyKey, Document{
		"kind":         "register_schema",
		"registration": registration,
	})
}

// UpsertSchemaValidationPolicy binds one event-type pattern to an immutable schema revision.
func (client *RegionalBusClient) UpsertSchemaValidationPolicy(ctx context.Context, bus string, shard uint32, idempotencyKey string, policy SchemaValidationPolicy) (Document, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return client.ApplyIntegration(ctx, bus, shard, idempotencyKey, Document{
		"kind":   "upsert_validation_policy",
		"policy": policy,
	})
}

// RemoveSchemaValidationPolicy removes one exact validation binding.
func (client *RegionalBusClient) RemoveSchemaValidationPolicy(ctx context.Context, bus string, shard uint32, idempotencyKey, name string) (Document, error) {
	if !validResourceName(name) {
		return nil, fmt.Errorf("epoch: schema validation policy name is invalid")
	}
	return client.ApplyIntegration(ctx, bus, shard, idempotencyKey, Document{
		"kind": "remove_validation_policy",
		"name": name,
	})
}

// ValidateSchema performs a linearizable, read-only producer or broker validation.
// Broker-mode validation is also enforced atomically by Publish when configured.
func (client *RegionalBusClient) ValidateSchema(ctx context.Context, bus string, shard uint32, stage SchemaValidationStage, event EventEnvelope) (Document, error) {
	if stage != ProducerValidationStage && stage != BrokerValidationStage {
		return nil, fmt.Errorf("epoch: schema validation stage must be producer or broker")
	}
	event, err := event.normalized()
	if err != nil {
		return nil, err
	}
	return client.read(ctx, bus, shard, "POST", "/schema/validate", struct {
		Mode     SchemaValidationStage `json:"mode"`
		Envelope EventEnvelope         `json:"envelope"`
	}{stage, event})
}

// Mutation resolves one proposal from the current leader.
func (client *RegionalBusClient) Mutation(ctx context.Context, bus string, shard uint32, proposalID uint64) (Document, error) {
	if proposalID == 0 {
		return nil, fmt.Errorf("epoch: Event Bus proposal ID must be non-zero")
	}
	return client.read(ctx, bus, shard, "GET", fmt.Sprintf("/mutations/%d", proposalID), nil)
}

// ReplayArchive performs a bounded linearizable archive query.
func (client *RegionalBusClient) ReplayArchive(ctx context.Context, bus string, shard uint32, options RegionalBusReplayOptions) (Document, error) {
	if options.FromMS > options.ToMS {
		return nil, fmt.Errorf("epoch: Event Bus replay from time must not exceed to time")
	}
	if err := validateBusReadLimit(options.Limit); err != nil {
		return nil, err
	}
	body := map[string]any{"from_ms": fmt.Sprintf("%d", options.FromMS), "to_ms": fmt.Sprintf("%d", options.ToMS), "limit": options.Limit}
	if options.Filter != nil {
		body["filter"] = normalizedEventFilter(*options.Filter)
	}
	return client.read(ctx, bus, shard, "POST", "/archive/replay", body)
}

// QueryDeliveries returns bounded replicated delivery records.
func (client *RegionalBusClient) QueryDeliveries(ctx context.Context, bus string, shard uint32, query RegionalBusDeliveryQuery) (Document, error) {
	if err := validateBusReadLimit(query.Limit); err != nil {
		return nil, err
	}
	body := map[string]any{"limit": query.Limit}
	if strings.TrimSpace(query.Subscription) != "" {
		body["subscription"] = query.Subscription
	}
	if query.State != nil {
		if !query.State.valid() {
			return nil, fmt.Errorf("epoch: unsupported Event Bus delivery state %q", *query.State)
		}
		body["state"] = *query.State
	}
	return client.read(ctx, bus, shard, "POST", "/deliveries/query", body)
}

// Status returns the linearizable Event Bus tablet status and digest.
func (client *RegionalBusClient) Status(ctx context.Context, bus string, shard uint32) (Document, error) {
	return client.read(ctx, bus, shard, "GET", "/status", nil)
}

// IntegrationState returns the complete linearizable schema, connector, MQTT, catalog, and endpoint state.
func (client *RegionalBusClient) IntegrationState(ctx context.Context, bus string, shard uint32) (Document, error) {
	return client.read(ctx, bus, shard, "GET", "/integration/state", nil)
}

func (client *RegionalBusClient) mutate(ctx context.Context, bus string, shard uint32, idempotencyKey string, operation any) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "buses", "Event Bus", bus, shard, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/mutations", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			Operation      any    `json:"operation"`
		}{idempotencyKey, route.Term, operation}}
	})
}

func (client *RegionalBusClient) read(ctx context.Context, bus string, shard uint32, method, path string, body any) (Document, error) {
	return regionalCall[Document](ctx, client.regionalClient(), "buses", "Event Bus", bus, shard, func(_ regionalRoute) Request {
		return Request{Method: method, Path: path, Body: body, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

func (client *RegionalBusClient) regionalClient() *regionalClient {
	if client == nil {
		return nil
	}
	return client.regional
}

func busSettlement(kind, deliveryID, dispatcher string, dispatcherEpoch uint64, leaseToken string) (map[string]any, error) {
	for label, value := range map[string]string{"delivery ID": deliveryID, "dispatcher": dispatcher, "delivery lease token": leaseToken} {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("epoch: %s is required", label)
		}
	}
	if dispatcherEpoch == 0 {
		return nil, fmt.Errorf("epoch: dispatcher epoch must be non-zero")
	}
	return map[string]any{"kind": kind, "delivery_id": deliveryID, "dispatcher": dispatcher, "dispatcher_epoch": fmt.Sprintf("%d", dispatcherEpoch), "lease_token": leaseToken}, nil
}

func validateBusDeliveryBatch(value uint16) error {
	if value == 0 || value > maxRegionalBusDeliveryBatch {
		return fmt.Errorf("epoch: Event Bus max deliveries must be between 1 and %d", maxRegionalBusDeliveryBatch)
	}
	return nil
}

func validateBusReadLimit(value uint16) error {
	if value == 0 || value > maxRegionalBusReadResults {
		return fmt.Errorf("epoch: Event Bus read limit must be between 1 and %d", maxRegionalBusReadResults)
	}
	return nil
}

func (state RegionalBusDeliveryState) valid() bool {
	return state == PendingDelivery || state == InFlightDelivery || state == AcknowledgedDelivery || state == DeadLetteredDelivery
}

func (registration SchemaRegistration) validate() error {
	if !validResourceName(registration.Name) {
		return fmt.Errorf("epoch: schema name is invalid")
	}
	if strings.TrimSpace(registration.Definition) == "" {
		return fmt.Errorf("epoch: schema definition is required")
	}
	if registration.Format != AvroSchema && registration.Format != JSONSchema && registration.Format != ProtobufSchema {
		return fmt.Errorf("epoch: unsupported schema format %q", registration.Format)
	}
	if registration.Compatibility != NoSchemaCompatibility && registration.Compatibility != BackwardSchemaCompatibility && registration.Compatibility != ForwardSchemaCompatibility && registration.Compatibility != FullSchemaCompatibility {
		return fmt.Errorf("epoch: unsupported schema compatibility %q", registration.Compatibility)
	}
	if registration.Format != ProtobufSchema && strings.TrimSpace(registration.RootMessage) != "" {
		return fmt.Errorf("epoch: root message is valid only for Protobuf schemas")
	}
	return nil
}

func (policy SchemaValidationPolicy) validate() error {
	if !validResourceName(policy.Name) {
		return fmt.Errorf("epoch: schema validation policy name is invalid")
	}
	if strings.TrimSpace(policy.EventTypePattern) == "" {
		return fmt.Errorf("epoch: schema validation event type pattern is required")
	}
	if strings.TrimSpace(policy.SchemaRef) == "" {
		return fmt.Errorf("epoch: schema reference is required")
	}
	if policy.Mode != DisabledSchemaValidation && policy.Mode != ProducerSchemaValidation && policy.Mode != BrokerSchemaValidation && policy.Mode != ProducerAndBrokerSchemaValidation {
		return fmt.Errorf("epoch: unsupported schema validation mode %q", policy.Mode)
	}
	return nil
}

func normalizedEventFilter(filter EventFilter) EventFilter {
	if filter.EventTypePatterns == nil {
		filter.EventTypePatterns = []string{}
	}
	if filter.SourcePatterns == nil {
		filter.SourcePatterns = []string{}
	}
	if filter.SubjectPatterns == nil {
		filter.SubjectPatterns = []string{}
	}
	if filter.Headers == nil {
		filter.Headers = map[string]string{}
	}
	if filter.JSONEquals == nil {
		filter.JSONEquals = map[string]any{}
	}
	return filter
}
