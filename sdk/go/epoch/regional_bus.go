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
)

// RegionalBusAcquireOptions identifies one bounded dispatcher lease request.
type RegionalBusAcquireOptions struct {
	Subscription    string
	Dispatcher      string
	DispatcherEpoch uint64
	MaxDeliveries   uint16
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
	return client.mutate(ctx, bus, shard, idempotencyKey, struct {
		Kind            string `json:"kind"`
		Subscription    string `json:"subscription"`
		Dispatcher      string `json:"dispatcher"`
		DispatcherEpoch string `json:"dispatcher_epoch"`
		MaxDeliveries   uint16 `json:"max_deliveries"`
	}{"acquire_deliveries", options.Subscription, options.Dispatcher, fmt.Sprintf("%d", options.DispatcherEpoch), options.MaxDeliveries})
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
