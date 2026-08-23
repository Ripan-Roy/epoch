package epoch

import (
	"crypto/rand"
	"fmt"
	"math"
	"net/url"
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
	MaxEntries     uint64
	MaxMemoryBytes *uint64
	MaxColdBytes   *uint64
	DefaultTTLMS   *uint64
	Eviction       string
	Durability     DurabilityProfile
}

// DefaultCacheConfig returns the standalone Cache defaults.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{MaxEntries: 10_000, Eviction: "no_eviction", Durability: Volatile}
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
	Durability          DurabilityProfile
	VisibilityTimeoutMS uint64
	MaxMessages         uint64
	MaxAttempts         uint32
}

// DefaultQueueConfig returns the standalone Work Queue defaults.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		Durability:          Volatile,
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
	TopicPatterns     []string          `json:"topic_patterns"`
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
	RenameFields      map[string]string `json:"rename_fields"`
	Constants         map[string]any    `json:"constants"`
	Templates         map[string]string `json:"templates"`
	Limits            *TransformLimits  `json:"limits,omitempty"`
	EnrichmentRef     string            `json:"enrichment_ref,omitempty"`
}

// TransformLimits bound deterministic transform CPU, memory, time, and network access.
type TransformLimits struct {
	MaxOperations  uint16 `json:"max_operations"`
	MaxOutputBytes uint64 `json:"max_output_bytes"`
	MaxValueBytes  uint64 `json:"max_value_bytes"`
	TimeoutMS      uint64 `json:"timeout_ms"`
	NetworkAccess  bool   `json:"network_access"`
}

// DeliveryBackoffStrategy controls deterministic Event Bus retry scheduling.
type DeliveryBackoffStrategy string

const (
	ExponentialDeliveryBackoff DeliveryBackoffStrategy = "exponential"
	FixedDeliveryBackoff       DeliveryBackoffStrategy = "fixed"
)

// DeliveryRetryPolicy bounds Event Bus delivery attempts.
type DeliveryRetryPolicy struct {
	Strategy       DeliveryBackoffStrategy `json:"strategy"`
	InitialDelayMS uint64                  `json:"initial_delay_ms"`
	MaxDelayMS     uint64                  `json:"max_delay_ms"`
	JitterPercent  uint8                   `json:"jitter_percent"`
	MaxAttempts    uint32                  `json:"max_attempts"`
	MaxAgeMS       *uint64                 `json:"max_age_ms"`
}

// DeliveryPolicy controls one subscription's replicated delivery ledger.
type DeliveryPolicy struct {
	TimeoutMS             uint64              `json:"timeout_ms"`
	MaxInFlight           uint16              `json:"max_in_flight"`
	Retry                 DeliveryRetryPolicy `json:"retry"`
	RateLimit             *DeliveryRateLimit  `json:"rate_limit,omitempty"`
	DeadLetterRetentionMS *uint64             `json:"dead_letter_retention_ms,omitempty"`
}

// DeliveryRateLimit bounds one subscription's committed delivery starts.
type DeliveryRateLimit struct {
	DeliveriesPerSecond uint32 `json:"deliveries_per_second"`
	Burst               uint32 `json:"burst"`
}

// DefaultDeliveryPolicy returns the replicated Event Bus delivery defaults.
func DefaultDeliveryPolicy() DeliveryPolicy {
	return DeliveryPolicy{
		TimeoutMS:   30_000,
		MaxInFlight: 16,
		Retry: DeliveryRetryPolicy{
			Strategy:       ExponentialDeliveryBackoff,
			InitialDelayMS: 1_000,
			MaxDelayMS:     60_000,
			JitterPercent:  10,
			MaxAttempts:    8,
		},
	}
}

// TargetKind identifies an Event Bus delivery target.
type TargetKind string

const (
	PullTargetKind           TargetKind = "pull"
	QueueTargetKind          TargetKind = "queue"
	StreamTargetKind         TargetKind = "stream"
	WebhookTargetKind        TargetKind = "webhook"
	HTTPTargetKind           TargetKind = "http"
	APIDestinationTargetKind TargetKind = "api_destination"
	EndpointPoolTargetKind   TargetKind = "endpoint_pool"
	FunctionTargetKind       TargetKind = "function"
	ConnectorTargetKind      TargetKind = "connector"
)

// DestinationAuth references rotatable credentials without carrying secret values.
type DestinationAuth struct {
	Kind      string   `json:"kind"`
	SecretRef string   `json:"secret_ref,omitempty"`
	Header    string   `json:"header,omitempty"`
	TokenURL  string   `json:"token_url,omitempty"`
	Scopes    []string `json:"scopes,omitempty"`
}

// SubscriptionTarget is a typed Event Bus delivery destination.
type SubscriptionTarget struct {
	Kind            TargetKind       `json:"kind"`
	Resource        string           `json:"resource,omitempty"`
	URL             string           `json:"url,omitempty"`
	SigningKeyID    string           `json:"signing_key_id,omitempty"`
	Pool            string           `json:"pool,omitempty"`
	Auth            *DestinationAuth `json:"auth,omitempty"`
	CloudEventsMode string           `json:"cloud_events_mode,omitempty"`
	requireSigning  bool
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

// SignedWebhookTarget declares the external key ID captured by the outbox.
func SignedWebhookTarget(targetURL, signingKeyID string) SubscriptionTarget {
	return SubscriptionTarget{
		Kind: WebhookTargetKind, URL: targetURL, SigningKeyID: signingKeyID, requireSigning: true,
	}
}

// HTTPTarget routes matching events to a generic HTTP endpoint.
func HTTPTarget(targetURL string) SubscriptionTarget {
	return SubscriptionTarget{Kind: HTTPTargetKind, URL: targetURL}
}

// SignedHTTPTarget declares the external key ID captured by the outbox.
func SignedHTTPTarget(targetURL, signingKeyID string) SubscriptionTarget {
	return SubscriptionTarget{
		Kind: HTTPTargetKind, URL: targetURL, SigningKeyID: signingKeyID, requireSigning: true,
	}
}

// APIDestinationTarget routes through a rotatable API-key or OAuth credential reference.
func APIDestinationTarget(targetURL string, auth DestinationAuth, mode string) SubscriptionTarget {
	return SubscriptionTarget{Kind: APIDestinationTargetKind, URL: targetURL, Auth: &auth, CloudEventsMode: mode}
}

// EndpointPoolTarget selects the healthiest endpoint from one replicated pool.
func EndpointPoolTarget(pool string, auth DestinationAuth, mode string) SubscriptionTarget {
	return SubscriptionTarget{Kind: EndpointPoolTargetKind, Pool: pool, Auth: &auth, CloudEventsMode: mode}
}

// FunctionTarget routes matching events to a named function executor.
func FunctionTarget(resource string) SubscriptionTarget {
	return SubscriptionTarget{Kind: FunctionTargetKind, Resource: resource}
}

// ConnectorTarget routes matching events to a managed connector worker.
func ConnectorTarget(resource string) SubscriptionTarget {
	return SubscriptionTarget{Kind: ConnectorTargetKind, Resource: resource}
}

// Subscription is a typed Event Bus routing resource.
type Subscription struct {
	Name           string             `json:"name"`
	Filter         EventFilter        `json:"filter"`
	Target         SubscriptionTarget `json:"target"`
	Transform      EventTransform     `json:"transform"`
	DeliveryPolicy *DeliveryPolicy    `json:"delivery_policy,omitempty"`
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
		if target.Resource != "" || target.URL != "" || target.SigningKeyID != "" || target.Pool != "" || target.Auth != nil {
			return fmt.Errorf("epoch: pull targets do not accept a resource or URL")
		}
	case QueueTargetKind, StreamTargetKind, FunctionTargetKind, ConnectorTargetKind:
		if strings.TrimSpace(target.Resource) == "" || target.URL != "" || target.SigningKeyID != "" || target.Pool != "" || target.Auth != nil {
			return fmt.Errorf("epoch: %s targets require only a resource", target.Kind)
		}
	case WebhookTargetKind, HTTPTargetKind:
		if strings.TrimSpace(target.URL) == "" || target.Resource != "" || target.Pool != "" || target.Auth != nil {
			return fmt.Errorf("epoch: %s targets require only a URL", target.Kind)
		}
		if target.requireSigning && target.SigningKeyID == "" {
			return fmt.Errorf("epoch: signed %s targets require a signing key ID", target.Kind)
		}
		if target.SigningKeyID != "" && !validResourceName(target.SigningKeyID) {
			return fmt.Errorf("epoch: signing key ID must be a 1-128 byte resource name")
		}
	case APIDestinationTargetKind:
		if strings.TrimSpace(target.URL) == "" || target.Resource != "" || target.Pool != "" || target.SigningKeyID != "" || target.Auth == nil {
			return fmt.Errorf("epoch: API destination targets require only a URL and auth reference")
		}
		if err := target.Auth.validate(); err != nil {
			return err
		}
		if !validHTTPURL(target.URL) {
			return fmt.Errorf("epoch: API destination requires an absolute HTTP(S) URL without credentials or fragments")
		}
		if target.CloudEventsMode != "" && target.CloudEventsMode != "binary" && target.CloudEventsMode != "structured" {
			return fmt.Errorf("epoch: CloudEvents mode must be binary or structured")
		}
	case EndpointPoolTargetKind:
		if strings.TrimSpace(target.Pool) == "" || target.Resource != "" || target.URL != "" || target.SigningKeyID != "" || target.Auth == nil {
			return fmt.Errorf("epoch: endpoint pool targets require only a pool and auth reference")
		}
		if err := target.Auth.validate(); err != nil {
			return err
		}
		if target.CloudEventsMode != "" && target.CloudEventsMode != "binary" && target.CloudEventsMode != "structured" {
			return fmt.Errorf("epoch: CloudEvents mode must be binary or structured")
		}
	default:
		return fmt.Errorf("epoch: unsupported subscription target %q", target.Kind)
	}
	return nil
}

func (auth DestinationAuth) validate() error {
	switch auth.Kind {
	case "none":
		if auth.SecretRef != "" || auth.Header != "" || auth.TokenURL != "" || len(auth.Scopes) != 0 {
			return fmt.Errorf("epoch: none destination auth cannot carry credential fields")
		}
	case "api_key":
		if !validResourceName(auth.SecretRef) || strings.TrimSpace(auth.Header) == "" || auth.TokenURL != "" || len(auth.Scopes) != 0 {
			return fmt.Errorf("epoch: API-key auth requires a secret reference and header")
		}
	case "oauth2":
		if !validResourceName(auth.SecretRef) || !validHTTPURL(auth.TokenURL) || auth.Header != "" || len(auth.Scopes) > 64 {
			return fmt.Errorf("epoch: OAuth2 auth requires a secret reference and token URL")
		}
		for _, scope := range auth.Scopes {
			if strings.TrimSpace(scope) == "" || len(scope) > 4*1024 {
				return fmt.Errorf("epoch: OAuth2 scopes must be non-empty and at most 4096 bytes")
			}
		}
	default:
		return fmt.Errorf("epoch: unsupported destination auth %q", auth.Kind)
	}
	return nil
}

func validHTTPURL(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" && parsed.User == nil && parsed.Fragment == ""
}

func validResourceName(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, character := range []byte(value) {
		if !(character >= 'a' && character <= 'z') &&
			!(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') &&
			character != '-' && character != '_' && character != '.' {
			return false
		}
	}
	return true
}

func (subscription Subscription) normalized() (Subscription, error) {
	if strings.TrimSpace(subscription.Name) == "" {
		return Subscription{}, fmt.Errorf("epoch: subscription name is required")
	}
	if err := subscription.Target.validate(); err != nil {
		return Subscription{}, err
	}
	if subscription.DeliveryPolicy != nil {
		policy := *subscription.DeliveryPolicy
		if err := policy.validate(); err != nil {
			return Subscription{}, err
		}
		subscription.DeliveryPolicy = &policy
	}
	if subscription.Transform.Limits != nil {
		limits := subscription.Transform.Limits
		operations := len(subscription.Transform.AddHeaders) + len(subscription.Transform.PayloadProjection) + len(subscription.Transform.RenameFields) + len(subscription.Transform.Constants) + len(subscription.Transform.Templates)
		if limits.MaxOperations == 0 || limits.MaxOperations > 256 || operations > int(limits.MaxOperations) {
			return Subscription{}, fmt.Errorf("epoch: transform operations exceed the configured or platform limit")
		}
		if limits.MaxOutputBytes == 0 || limits.MaxOutputBytes > 1024*1024 || limits.MaxValueBytes == 0 || limits.MaxValueBytes > 256*1024 || limits.MaxValueBytes > limits.MaxOutputBytes {
			return Subscription{}, fmt.Errorf("epoch: transform byte limits are invalid")
		}
		if limits.TimeoutMS == 0 || limits.TimeoutMS > 1_000 {
			return Subscription{}, fmt.Errorf("epoch: transform timeout must be between 1 and 1000 milliseconds")
		}
		if limits.NetworkAccess {
			return Subscription{}, fmt.Errorf("epoch: deterministic transforms cannot enable network access")
		}
	}
	if subscription.Transform.EnrichmentRef != "" && !validResourceName(subscription.Transform.EnrichmentRef) {
		return Subscription{}, fmt.Errorf("epoch: enrichment reference must be a resource name")
	}
	if subscription.Filter.EventTypePatterns == nil {
		subscription.Filter.EventTypePatterns = []string{}
	}
	if subscription.Filter.TopicPatterns == nil {
		subscription.Filter.TopicPatterns = []string{}
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
	if subscription.Transform.RenameFields == nil {
		subscription.Transform.RenameFields = map[string]string{}
	}
	if subscription.Transform.Constants == nil {
		subscription.Transform.Constants = map[string]any{}
	}
	if subscription.Transform.Templates == nil {
		subscription.Transform.Templates = map[string]string{}
	}
	return subscription, nil
}

func (policy DeliveryPolicy) validate() error {
	const maxTimeoutMS = uint64(7 * 24 * 60 * 60 * 1_000)
	if policy.TimeoutMS == 0 || policy.TimeoutMS > maxTimeoutMS {
		return fmt.Errorf("epoch: delivery timeout must be between 1 and %d milliseconds", maxTimeoutMS)
	}
	if policy.MaxInFlight == 0 || policy.MaxInFlight > 1_000 {
		return fmt.Errorf("epoch: delivery max in flight must be between 1 and 1000")
	}
	if policy.Retry.Strategy != ExponentialDeliveryBackoff && policy.Retry.Strategy != FixedDeliveryBackoff {
		return fmt.Errorf("epoch: unsupported delivery backoff strategy %q", policy.Retry.Strategy)
	}
	if policy.Retry.MaxAttempts == 0 || policy.Retry.MaxAttempts > 100 {
		return fmt.Errorf("epoch: delivery retry attempts must be between 1 and 100")
	}
	if policy.Retry.InitialDelayMS > policy.Retry.MaxDelayMS {
		return fmt.Errorf("epoch: delivery retry initial delay must not exceed max delay")
	}
	if policy.Retry.MaxDelayMS > maxTimeoutMS {
		return fmt.Errorf("epoch: delivery retry max delay must not exceed %d milliseconds", maxTimeoutMS)
	}
	if policy.Retry.JitterPercent > 100 {
		return fmt.Errorf("epoch: delivery retry jitter percent cannot exceed 100")
	}
	if policy.Retry.MaxAgeMS != nil && *policy.Retry.MaxAgeMS == 0 {
		return fmt.Errorf("epoch: delivery retry max age must be non-zero when provided")
	}
	if policy.RateLimit != nil && (policy.RateLimit.DeliveriesPerSecond == 0 || policy.RateLimit.DeliveriesPerSecond > 1_000_000 || policy.RateLimit.Burst == 0 || policy.RateLimit.Burst > 1_000_000) {
		return fmt.Errorf("epoch: delivery rate and burst must be between 1 and 1000000")
	}
	if policy.DeadLetterRetentionMS != nil && (*policy.DeadLetterRetentionMS == 0 || *policy.DeadLetterRetentionMS > 31_536_000_000) {
		return fmt.Errorf("epoch: dead-letter retention must be between 1 and 31536000000 milliseconds")
	}
	return nil
}
