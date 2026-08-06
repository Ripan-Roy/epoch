package epoch

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maxRegionalQueueAcquireBatch = 100
	maxRegionalQueueInFlight     = 10_000
	maxRegionalQueueHistory      = 1_000
)

// RegionalQueueAcquireOptions controls a credit-aware acquire operation.
type RegionalQueueAcquireOptions struct {
	Consumer            string
	ConsumerEpoch       uint64
	MaxMessages         uint16
	MaxInFlight         *uint16
	VisibilityTimeoutMS *uint64
}

// RegionalQueueClient routes authenticated Queue calls across regional nodes.
// Mutation retries preserve the caller's idempotency key across rediscovery.
type RegionalQueueClient struct {
	regional *regionalClient
}

// NewRegionalQueueClient builds a regional Queue client over one or more HTTP endpoints.
func NewRegionalQueueClient(endpoints []string, token string, scope RegionalScope, timeout time.Duration) (*RegionalQueueClient, error) {
	regional, err := newRegionalClient(endpoints, token, scope, timeout)
	if err != nil {
		return nil, err
	}
	return &RegionalQueueClient{regional: regional}, nil
}

// NewRegionalQueueClientWithTransports injects endpoint transports for tests or custom networking.
func NewRegionalQueueClientWithTransports(transports []Transport, token string, scope RegionalScope) (*RegionalQueueClient, error) {
	regional, err := newRegionalClientWithTransports(transports, token, scope)
	if err != nil {
		return nil, err
	}
	return &RegionalQueueClient{regional: regional}, nil
}

// Enqueue appends one message using a caller-owned mutation identity.
func (client *RegionalQueueClient) Enqueue(ctx context.Context, queue string, shard uint32, idempotencyKey string, event EventEnvelope) (Document, error) {
	event, err := event.normalized()
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, queue, shard, idempotencyKey, struct {
		Kind      string        `json:"kind"`
		Partition uint32        `json:"partition"`
		Envelope  EventEnvelope `json:"envelope"`
	}{"enqueue", 0, event})
}

// Acquire leases up to MaxMessages messages and optionally applies a per-consumer in-flight ceiling.
func (client *RegionalQueueClient) Acquire(ctx context.Context, queue string, shard uint32, idempotencyKey string, options RegionalQueueAcquireOptions) (Document, error) {
	if err := validateQueueConsumer(options.Consumer, options.ConsumerEpoch); err != nil {
		return nil, err
	}
	if options.MaxMessages == 0 || options.MaxMessages > maxRegionalQueueAcquireBatch {
		return nil, fmt.Errorf("epoch: Queue max messages must be between 1 and %d", maxRegionalQueueAcquireBatch)
	}
	if options.MaxInFlight != nil && (*options.MaxInFlight == 0 || *options.MaxInFlight > maxRegionalQueueInFlight) {
		return nil, fmt.Errorf("epoch: Queue max in flight must be between 1 and %d", maxRegionalQueueInFlight)
	}
	if options.VisibilityTimeoutMS != nil && *options.VisibilityTimeoutMS == 0 {
		return nil, fmt.Errorf("epoch: Queue visibility timeout must be non-zero when provided")
	}
	return client.mutate(ctx, queue, shard, idempotencyKey, struct {
		Kind                string  `json:"kind"`
		Partition           uint32  `json:"partition"`
		Consumer            string  `json:"consumer"`
		ConsumerEpoch       string  `json:"consumer_epoch"`
		MaxMessages         uint16  `json:"max_messages"`
		MaxInFlight         *uint16 `json:"max_in_flight,omitempty"`
		VisibilityTimeoutMS *string `json:"visibility_timeout_ms,omitempty"`
	}{"acquire", 0, options.Consumer, strconv.FormatUint(options.ConsumerEpoch, 10), options.MaxMessages, options.MaxInFlight, decimalPointer(options.VisibilityTimeoutMS)})
}

// Acknowledge permanently settles a fenced lease.
func (client *RegionalQueueClient) Acknowledge(ctx context.Context, queue string, shard uint32, idempotencyKey, consumer string, consumerEpoch uint64, leaseToken string) (Document, error) {
	operation, err := settlementOperation("acknowledge", consumer, consumerEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, queue, shard, idempotencyKey, operation)
}

// ExtendLease lengthens a fenced lease by extensionMS.
func (client *RegionalQueueClient) ExtendLease(ctx context.Context, queue string, shard uint32, idempotencyKey, consumer string, consumerEpoch uint64, leaseToken string, extensionMS uint64) (Document, error) {
	if extensionMS == 0 {
		return nil, fmt.Errorf("epoch: Queue lease extension must be non-zero")
	}
	operation, err := settlementOperation("extend_lease", consumer, consumerEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	operation["extension_ms"] = strconv.FormatUint(extensionMS, 10)
	return client.mutate(ctx, queue, shard, idempotencyKey, operation)
}

// Release returns a fenced lease to the ready or delayed set.
func (client *RegionalQueueClient) Release(ctx context.Context, queue string, shard uint32, idempotencyKey, consumer string, consumerEpoch uint64, leaseToken string, delayMS uint64, reason string) (Document, error) {
	operation, err := settlementOperation("release", consumer, consumerEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	operation["delay_ms"] = strconv.FormatUint(delayMS, 10)
	if strings.TrimSpace(reason) != "" {
		operation["reason"] = reason
	}
	return client.mutate(ctx, queue, shard, idempotencyKey, operation)
}

// Nack records a retryable processing failure for a fenced lease.
func (client *RegionalQueueClient) Nack(ctx context.Context, queue string, shard uint32, idempotencyKey, consumer string, consumerEpoch uint64, leaseToken, reason string) (Document, error) {
	return client.disposition(ctx, queue, shard, idempotencyKey, "nack", consumer, consumerEpoch, leaseToken, reason)
}

// Reject dead-letters a fenced lease with a terminal reason.
func (client *RegionalQueueClient) Reject(ctx context.Context, queue string, shard uint32, idempotencyKey, consumer string, consumerEpoch uint64, leaseToken, reason string) (Document, error) {
	return client.disposition(ctx, queue, shard, idempotencyKey, "reject", consumer, consumerEpoch, leaseToken, reason)
}

func (client *RegionalQueueClient) disposition(ctx context.Context, queue string, shard uint32, idempotencyKey, kind, consumer string, consumerEpoch uint64, leaseToken, reason string) (Document, error) {
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("epoch: Queue disposition reason is required")
	}
	operation, err := settlementOperation(kind, consumer, consumerEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	operation["reason"] = reason
	return client.mutate(ctx, queue, shard, idempotencyKey, operation)
}

// Redrive moves one exact dead-letter history entry back to the ready set.
func (client *RegionalQueueClient) Redrive(ctx context.Context, queue string, shard uint32, idempotencyKey, messageID string, deadLetterHistoryID uint64) (Document, error) {
	if strings.TrimSpace(messageID) == "" {
		return nil, fmt.Errorf("epoch: Queue message ID is required")
	}
	if deadLetterHistoryID == 0 {
		return nil, fmt.Errorf("epoch: Queue dead-letter history ID must be non-zero")
	}
	return client.mutate(ctx, queue, shard, idempotencyKey, struct {
		Kind                string `json:"kind"`
		Partition           uint32 `json:"partition"`
		MessageID           string `json:"message_id"`
		DeadLetterHistoryID string `json:"dead_letter_history_id"`
	}{"redrive", 0, messageID, strconv.FormatUint(deadLetterHistoryID, 10)})
}

// Maintain applies due-delay and visibility-timeout transitions deterministically.
func (client *RegionalQueueClient) Maintain(ctx context.Context, queue string, shard uint32, idempotencyKey string) (Document, error) {
	return client.mutate(ctx, queue, shard, idempotencyKey, struct {
		Kind      string `json:"kind"`
		Partition uint32 `json:"partition"`
	}{"maintain", 0})
}

// Mutation returns one mutation outcome by proposal ID.
func (client *RegionalQueueClient) Mutation(ctx context.Context, queue string, shard uint32, proposalID uint64) (Document, error) {
	if proposalID == 0 {
		return nil, fmt.Errorf("epoch: Queue proposal ID must be non-zero")
	}
	return client.read(ctx, queue, shard, "/mutations/"+strconv.FormatUint(proposalID, 10), nil)
}

// Counts returns a linearizable Queue state summary.
func (client *RegionalQueueClient) Counts(ctx context.Context, queue string, shard uint32) (Document, error) {
	return client.read(ctx, queue, shard, "/counts", nil)
}

// DeadLetters returns a bounded linearizable dead-letter history.
func (client *RegionalQueueClient) DeadLetters(ctx context.Context, queue string, shard uint32, limit uint16) (Document, error) {
	return client.history(ctx, queue, shard, "/dead-letters", limit)
}

// Redrives returns a bounded linearizable redrive history.
func (client *RegionalQueueClient) Redrives(ctx context.Context, queue string, shard uint32, limit uint16) (Document, error) {
	return client.history(ctx, queue, shard, "/redrives", limit)
}

// ConsumerFlow returns linearizable credit and in-flight state for one consumer.
func (client *RegionalQueueClient) ConsumerFlow(ctx context.Context, queue string, shard uint32, consumer string) (Document, error) {
	consumerSegment, err := segment(consumer, "Queue consumer")
	if err != nil {
		return nil, err
	}
	return client.read(ctx, queue, shard, "/consumers/"+consumerSegment+"/flow", nil)
}

// Status returns the linearizable Queue tablet status and digest.
func (client *RegionalQueueClient) Status(ctx context.Context, queue string, shard uint32) (Document, error) {
	return client.read(ctx, queue, shard, "/status", nil)
}

func (client *RegionalQueueClient) history(ctx context.Context, queue string, shard uint32, path string, limit uint16) (Document, error) {
	if limit == 0 || limit > maxRegionalQueueHistory {
		return nil, fmt.Errorf("epoch: Queue history limit must be between 1 and %d", maxRegionalQueueHistory)
	}
	return client.read(ctx, queue, shard, path, url.Values{"limit": {strconv.FormatUint(uint64(limit), 10)}})
}

func (client *RegionalQueueClient) read(ctx context.Context, queue string, shard uint32, path string, query url.Values) (Document, error) {
	return regionalCall[Document](ctx, client.regionalClient(), "queues", "Queue", queue, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: path, Query: query, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

func (client *RegionalQueueClient) mutate(ctx context.Context, queue string, shard uint32, idempotencyKey string, operation any) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "queues", "Queue", queue, shard, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/mutations", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			Operation      any    `json:"operation"`
		}{idempotencyKey, route.Term, operation}}
	})
}

func (client *RegionalQueueClient) regionalClient() *regionalClient {
	if client == nil {
		return nil
	}
	return client.regional
}

func settlementOperation(kind, consumer string, consumerEpoch uint64, leaseToken string) (map[string]any, error) {
	if err := validateQueueConsumer(consumer, consumerEpoch); err != nil {
		return nil, err
	}
	if strings.TrimSpace(leaseToken) == "" {
		return nil, fmt.Errorf("epoch: Queue lease token is required")
	}
	return map[string]any{
		"kind":           kind,
		"partition":      uint32(0),
		"consumer":       consumer,
		"consumer_epoch": strconv.FormatUint(consumerEpoch, 10),
		"lease_token":    leaseToken,
	}, nil
}

func validateQueueConsumer(consumer string, consumerEpoch uint64) error {
	if strings.TrimSpace(consumer) == "" {
		return fmt.Errorf("epoch: Queue consumer is required")
	}
	if consumerEpoch == 0 {
		return fmt.Errorf("epoch: Queue consumer epoch must be non-zero")
	}
	return nil
}

func decimalPointer(value *uint64) *string {
	if value == nil {
		return nil
	}
	decimal := strconv.FormatUint(*value, 10)
	return &decimal
}
