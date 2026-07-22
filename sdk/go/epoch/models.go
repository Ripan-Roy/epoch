package epoch

import (
	"crypto/rand"
	"fmt"
	"math"
	"strings"
	"time"
)

// Document represents a decoded JSON object returned by the native API.
type Document map[string]any

// DurabilityProfile names an explicit Epoch acknowledgement contract.
type DurabilityProfile string

const (
	Volatile         DurabilityProfile = "volatile"
	ReplicatedMemory DurabilityProfile = "replicated_memory"
	LocalDurable     DurabilityProfile = "local_durable"
	QuorumDurable    DurabilityProfile = "quorum_durable"
	GeoAsync         DurabilityProfile = "geo_async"
	GeoSync          DurabilityProfile = "geo_sync"
)

// CacheConfig configures a standalone Cache profile.
type CacheConfig struct {
	MaxEntries   uint64
	DefaultTTLMS *uint64
	Eviction     string
}

// DefaultCacheConfig returns the standalone Cache defaults.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{MaxEntries: 10_000, Eviction: "no_eviction"}
}

// StreamConfig configures a standalone Stream profile.
type StreamConfig struct {
	Partitions             uint32
	Durability             DurabilityProfile
	MaxRecordsPerPartition *uint64
}

// DefaultStreamConfig returns the standalone Stream defaults.
func DefaultStreamConfig() StreamConfig {
	return StreamConfig{Partitions: 1, Durability: Volatile}
}

// QueueConfig configures a standalone Work Queue profile.
type QueueConfig struct {
	VisibilityTimeoutMS uint64
	MaxMessages         uint64
	MaxAttempts         uint32
}

// DefaultQueueConfig returns the standalone Work Queue defaults.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		VisibilityTimeoutMS: 30_000,
		MaxMessages:         100_000,
		MaxAttempts:         8,
	}
}

// CacheWriteOptions controls conditional Cache writes.
type CacheWriteOptions struct {
	TTLMS           *uint64
	ExpectedVersion *uint64
	OnlyIfAbsent    bool
	OnlyIfPresent   bool
}

// QueueReceiveOptions controls one Work Queue lease acquisition.
type QueueReceiveOptions struct {
	Consumer            string
	MaxMessages         uint32
	VisibilityTimeoutMS *uint64
}

// BusReplayOptions bounds an Event Bus archive replay.
type BusReplayOptions struct {
	FromMS    uint64
	ToMS      uint64
	Limit     uint32
	EventType string
}

// DefaultBusReplayOptions returns the full-time-range replay defaults.
func DefaultBusReplayOptions() BusReplayOptions {
	return BusReplayOptions{FromMS: 0, ToMS: math.MaxUint64, Limit: 100}
}

// EventEnvelope is the common record envelope accepted by every profile.
type EventEnvelope struct {
	ID            string            `json:"id"`
	Source        string            `json:"source"`
	Type          string            `json:"type"`
	TimeMS        uint64            `json:"time_ms"`
	Subject       string            `json:"subject,omitempty"`
	Key           string            `json:"key,omitempty"`
	Headers       map[string]string `json:"headers"`
	ContentType   string            `json:"content_type"`
	SchemaRef     string            `json:"schema_ref,omitempty"`
	Traceparent   string            `json:"traceparent,omitempty"`
	Payload       any               `json:"payload"`
	DeliverAtMS   *uint64           `json:"deliver_at_ms,omitempty"`
	TTLMS         *uint64           `json:"ttl_ms,omitempty"`
	Priority      uint8             `json:"priority"`
	DedupeID      string            `json:"dedupe_id,omitempty"`
	TransactionID string            `json:"transaction_id,omitempty"`
	Extensions    map[string]any    `json:"extensions"`
}

// NewEventEnvelope constructs an event with an opaque ID and current time.
func NewEventEnvelope(source, eventType string, payload any) EventEnvelope {
	return EventEnvelope{
		ID:          rand.Text(),
		Source:      source,
		Type:        eventType,
		TimeMS:      uint64(time.Now().UnixMilli()),
		Headers:     map[string]string{},
		ContentType: "application/json",
		Payload:     payload,
		Extensions:  map[string]any{},
	}
}

// EventFilter uses the native Event Bus matching vocabulary.
type EventFilter struct {
	EventTypePatterns []string          `json:"event_type_patterns"`
	SourcePatterns    []string          `json:"source_patterns"`
	SubjectPatterns   []string          `json:"subject_patterns"`
	Headers           map[string]string `json:"headers"`
	JSONEquals        map[string]any    `json:"json_equals"`
}

// EventTransform defines deterministic header and payload projections.
type EventTransform struct {
	AddHeaders        map[string]string `json:"add_headers"`
	PayloadProjection map[string]string `json:"payload_projection"`
}

// TargetKind identifies an Event Bus delivery target.
type TargetKind string

const (
	PullTargetKind    TargetKind = "pull"
	QueueTargetKind   TargetKind = "queue"
	StreamTargetKind  TargetKind = "stream"
	WebhookTargetKind TargetKind = "webhook"
	HTTPTargetKind    TargetKind = "http"
)

// SubscriptionTarget is a typed Event Bus delivery destination.
type SubscriptionTarget struct {
	Kind     TargetKind `json:"kind"`
	Resource string     `json:"resource,omitempty"`
	URL      string     `json:"url,omitempty"`
}

// PullTarget creates a subscription consumed through pull delivery.
func PullTarget() SubscriptionTarget {
	return SubscriptionTarget{Kind: PullTargetKind}
}

// QueueTarget routes matching events into a Work Queue.
func QueueTarget(resource string) SubscriptionTarget {
	return SubscriptionTarget{Kind: QueueTargetKind, Resource: resource}
}

// StreamTarget routes matching events into a Stream.
func StreamTarget(resource string) SubscriptionTarget {
	return SubscriptionTarget{Kind: StreamTargetKind, Resource: resource}
}

// WebhookTarget routes matching events to a webhook URL.
func WebhookTarget(targetURL string) SubscriptionTarget {
	return SubscriptionTarget{Kind: WebhookTargetKind, URL: targetURL}
}

// HTTPTarget routes matching events to a generic HTTP endpoint.
func HTTPTarget(targetURL string) SubscriptionTarget {
	return SubscriptionTarget{Kind: HTTPTargetKind, URL: targetURL}
}

// Subscription is a typed Event Bus routing resource.
type Subscription struct {
	Name      string             `json:"name"`
	Filter    EventFilter        `json:"filter"`
	Target    SubscriptionTarget `json:"target"`
	Transform EventTransform     `json:"transform"`
}

// Uint32 returns a pointer suitable for an optional uint32 field.
func Uint32(value uint32) *uint32 {
	return &value
}

// Uint64 returns a pointer suitable for an optional uint64 field.
func Uint64(value uint64) *uint64 {
	return &value
}

func (profile DurabilityProfile) validate() error {
	switch profile {
	case Volatile, ReplicatedMemory, LocalDurable, QuorumDurable, GeoAsync, GeoSync:
		return nil
	default:
		return fmt.Errorf("epoch: unsupported durability profile %q", profile)
	}
}

func (event EventEnvelope) normalized() (EventEnvelope, error) {
	if strings.TrimSpace(event.ID) == "" {
		return EventEnvelope{}, fmt.Errorf("epoch: event ID is required")
	}
	if strings.TrimSpace(event.Source) == "" {
		return EventEnvelope{}, fmt.Errorf("epoch: event source is required")
	}
	if strings.TrimSpace(event.Type) == "" {
		return EventEnvelope{}, fmt.Errorf("epoch: event type is required")
	}
	if event.Priority > 9 {
		return EventEnvelope{}, fmt.Errorf("epoch: event priority must be between 0 and 9")
	}
	if event.Headers == nil {
		event.Headers = map[string]string{}
	}
	if event.Extensions == nil {
		event.Extensions = map[string]any{}
	}
	if event.ContentType == "" {
		event.ContentType = "application/json"
	}
	return event, nil
}

func (target SubscriptionTarget) validate() error {
	switch target.Kind {
	case PullTargetKind:
		if target.Resource != "" || target.URL != "" {
			return fmt.Errorf("epoch: pull targets do not accept a resource or URL")
		}
	case QueueTargetKind, StreamTargetKind:
		if strings.TrimSpace(target.Resource) == "" || target.URL != "" {
			return fmt.Errorf("epoch: %s targets require only a resource", target.Kind)
		}
	case WebhookTargetKind, HTTPTargetKind:
		if strings.TrimSpace(target.URL) == "" || target.Resource != "" {
			return fmt.Errorf("epoch: %s targets require only a URL", target.Kind)
		}
	default:
		return fmt.Errorf("epoch: unsupported subscription target %q", target.Kind)
	}
	return nil
}

func (subscription Subscription) normalized() (Subscription, error) {
	if strings.TrimSpace(subscription.Name) == "" {
		return Subscription{}, fmt.Errorf("epoch: subscription name is required")
	}
	if err := subscription.Target.validate(); err != nil {
		return Subscription{}, err
	}
	if subscription.Filter.EventTypePatterns == nil {
		subscription.Filter.EventTypePatterns = []string{}
	}
	if subscription.Filter.SourcePatterns == nil {
		subscription.Filter.SourcePatterns = []string{}
	}
	if subscription.Filter.SubjectPatterns == nil {
		subscription.Filter.SubjectPatterns = []string{}
	}
	if subscription.Filter.Headers == nil {
		subscription.Filter.Headers = map[string]string{}
	}
	if subscription.Filter.JSONEquals == nil {
		subscription.Filter.JSONEquals = map[string]any{}
	}
	if subscription.Transform.AddHeaders == nil {
		subscription.Transform.AddHeaders = map[string]string{}
	}
	if subscription.Transform.PayloadProjection == nil {
		subscription.Transform.PayloadProjection = map[string]string{}
	}
	return subscription, nil
}
