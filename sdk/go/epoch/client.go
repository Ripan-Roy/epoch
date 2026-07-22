package epoch

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultEndpoint = "http://127.0.0.1:7601"

// Client is a synchronous, context-aware client for one Epoch node.
type Client struct {
	transport Transport
}

// NewClient constructs an HTTP-backed client. An empty endpoint uses the local default.
func NewClient(endpoint string, timeout time.Duration) (*Client, error) {
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	transport, err := NewHTTPTransport(endpoint, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{transport: transport}, nil
}

// NewClientWithTransport constructs a client around an injected transport.
func NewClientWithTransport(transport Transport) (*Client, error) {
	if transport == nil {
		return nil, fmt.Errorf("epoch: transport is required")
	}
	return &Client{transport: transport}, nil
}

// Health returns the node's current health and guarantee ceiling.
func (client *Client) Health(ctx context.Context) (Document, error) {
	return execute[Document](ctx, client, Request{Method: "GET", Path: "/healthz"})
}

// Resources lists the resources currently hosted by the standalone node.
func (client *Client) Resources(ctx context.Context) ([]Document, error) {
	return execute[[]Document](ctx, client, Request{Method: "GET", Path: "/v1/resources"})
}

// CreateCache creates a volatile Cache with typed configuration.
func (client *Client) CreateCache(ctx context.Context, name string, config CacheConfig) (Document, error) {
	path, err := resourcePath("caches", name)
	if err != nil {
		return nil, err
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = DefaultCacheConfig().MaxEntries
	}
	if config.Eviction == "" {
		config.Eviction = DefaultCacheConfig().Eviction
	}
	if config.DefaultTTLMS != nil && *config.DefaultTTLMS == 0 {
		return nil, fmt.Errorf("epoch: default TTL must be greater than zero")
	}
	body := struct {
		MaxEntries   uint64            `json:"max_entries"`
		DefaultTTLMS *uint64           `json:"default_ttl_ms"`
		Eviction     string            `json:"eviction"`
		Durability   DurabilityProfile `json:"durability"`
	}{config.MaxEntries, config.DefaultTTLMS, config.Eviction, Volatile}
	return execute[Document](ctx, client, Request{Method: "POST", Path: path, Body: body})
}

// CreateStream creates a Stream with an explicit durability profile.
func (client *Client) CreateStream(ctx context.Context, name string, config StreamConfig) (Document, error) {
	path, err := resourcePath("streams", name)
	if err != nil {
		return nil, err
	}
	if config.Partitions == 0 {
		config.Partitions = DefaultStreamConfig().Partitions
	}
	if config.Durability == "" {
		config.Durability = Volatile
	}
	if err := config.Durability.validate(); err != nil {
		return nil, err
	}
	if config.MaxRecordsPerPartition != nil && *config.MaxRecordsPerPartition == 0 {
		return nil, fmt.Errorf("epoch: max records per partition must be greater than zero")
	}
	body := struct {
		Partitions             uint32            `json:"partitions"`
		Durability             DurabilityProfile `json:"durability"`
		MaxRecordsPerPartition *uint64           `json:"max_records_per_partition"`
	}{config.Partitions, config.Durability, config.MaxRecordsPerPartition}
	return execute[Document](ctx, client, Request{Method: "POST", Path: path, Body: body})
}

// CreateQueue creates a Work Queue with an explicit durability profile.
func (client *Client) CreateQueue(ctx context.Context, name string, config QueueConfig) (Document, error) {
	path, err := resourcePath("queues", name)
	if err != nil {
		return nil, err
	}
	defaults := DefaultQueueConfig()
	if config.Durability == "" {
		config.Durability = defaults.Durability
	}
	if err := config.Durability.validate(); err != nil {
		return nil, err
	}
	if config.VisibilityTimeoutMS == 0 {
		config.VisibilityTimeoutMS = defaults.VisibilityTimeoutMS
	}
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = defaults.MaxAttempts
	}
	body := struct {
		Durability          DurabilityProfile `json:"durability"`
		VisibilityTimeoutMS uint64            `json:"visibility_timeout_ms"`
		MaxMessages         uint64            `json:"max_messages"`
		Retry               queueRetryConfig  `json:"retry"`
		DedupeWindowMS      *uint64           `json:"dedupe_window_ms"`
	}{
		Durability:          config.Durability,
		VisibilityTimeoutMS: config.VisibilityTimeoutMS,
		MaxMessages:         config.MaxMessages,
		Retry: queueRetryConfig{
			Strategy:       "exponential",
			InitialDelayMS: 1_000,
			MaxDelayMS:     60_000,
			JitterPercent:  10,
			MaxAttempts:    config.MaxAttempts,
		},
	}
	return execute[Document](ctx, client, Request{Method: "POST", Path: path, Body: body})
}

// CreateBus creates a volatile Event Bus and optionally enables its archive.
func (client *Client) CreateBus(ctx context.Context, name string, archive bool) (Document, error) {
	path, err := resourcePath("buses", name)
	if err != nil {
		return nil, err
	}
	body := struct {
		Durability DurabilityProfile `json:"durability"`
		Archive    bool              `json:"archive"`
	}{Volatile, archive}
	return execute[Document](ctx, client, Request{Method: "POST", Path: path, Body: body})
}

// CacheSet writes one string value, optionally with conditional semantics.
func (client *Client) CacheSet(ctx context.Context, cache, key, value string, options CacheWriteOptions) (Document, error) {
	path, err := cacheKeyPath(cache, key)
	if err != nil {
		return nil, err
	}
	if options.TTLMS != nil && *options.TTLMS == 0 {
		return nil, fmt.Errorf("epoch: TTL must be greater than zero")
	}
	if options.ExpectedVersion != nil && *options.ExpectedVersion == 0 {
		return nil, fmt.Errorf("epoch: expected version must be greater than zero")
	}
	if options.OnlyIfAbsent && options.OnlyIfPresent {
		return nil, fmt.Errorf("epoch: only-if-absent and only-if-present are mutually exclusive")
	}
	body := struct {
		Value struct {
			Kind  string `json:"kind"`
			Value string `json:"value"`
		} `json:"value"`
		TTLMS           *uint64 `json:"ttl_ms"`
		ExpectedVersion *uint64 `json:"expected_version"`
		OnlyIfAbsent    bool    `json:"only_if_absent"`
		OnlyIfPresent   bool    `json:"only_if_present"`
	}{
		TTLMS:           options.TTLMS,
		ExpectedVersion: options.ExpectedVersion,
		OnlyIfAbsent:    options.OnlyIfAbsent,
		OnlyIfPresent:   options.OnlyIfPresent,
	}
	body.Value.Kind = "string"
	body.Value.Value = value
	return execute[Document](ctx, client, Request{Method: "PUT", Path: path, Body: body})
}

// CacheGet reads one Cache key.
func (client *Client) CacheGet(ctx context.Context, cache, key string) (Document, error) {
	path, err := cacheKeyPath(cache, key)
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "GET", Path: path})
}

// CacheDelete deletes one Cache key.
func (client *Client) CacheDelete(ctx context.Context, cache, key string) (Document, error) {
	path, err := cacheKeyPath(cache, key)
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "DELETE", Path: path})
}

// CacheIncrement atomically adds delta to one numeric Cache key.
func (client *Client) CacheIncrement(ctx context.Context, cache, key string, delta int64) (Document, error) {
	path, err := cacheKeyPath(cache, key)
	if err != nil {
		return nil, err
	}
	body := struct {
		Delta int64 `json:"delta"`
	}{delta}
	return execute[Document](ctx, client, Request{Method: "POST", Path: path + "/increment", Body: body})
}

// AppendStream appends one event to an optional Stream partition.
func (client *Client) AppendStream(ctx context.Context, stream string, event EventEnvelope, partition *uint32) (Document, error) {
	name, err := segment(stream, "stream")
	if err != nil {
		return nil, err
	}
	event, err = event.normalized()
	if err != nil {
		return nil, err
	}
	body := struct {
		Envelope  EventEnvelope `json:"envelope"`
		Partition *uint32       `json:"partition"`
	}{event, partition}
	return execute[Document](ctx, client, Request{Method: "POST", Path: "/v1/streams/" + name + "/records", Body: body})
}

// FetchStream fetches records beginning at one partition offset.
func (client *Client) FetchStream(ctx context.Context, stream string, partition uint32, offset uint64, limit uint32) ([]Document, error) {
	name, err := segment(stream, "stream")
	if err != nil {
		return nil, err
	}
	if limit == 0 || limit > 10_000 {
		return nil, fmt.Errorf("epoch: limit must be between 1 and 10000")
	}
	query := url.Values{}
	query.Set("partition", strconv.FormatUint(uint64(partition), 10))
	query.Set("offset", strconv.FormatUint(offset, 10))
	query.Set("limit", strconv.FormatUint(uint64(limit), 10))
	return execute[[]Document](ctx, client, Request{Method: "GET", Path: "/v1/streams/" + name + "/records", Query: query})
}

// CommitStreamOffset records or explicitly resets a consumer group's next offset.
func (client *Client) CommitStreamOffset(ctx context.Context, stream, group string, partition uint32, nextOffset uint64, reset bool) (Document, error) {
	path, err := streamGroupPath(stream, group)
	if err != nil {
		return nil, err
	}
	body := struct {
		Partition  uint32 `json:"partition"`
		NextOffset uint64 `json:"next_offset"`
		Reset      bool   `json:"reset"`
	}{partition, nextOffset, reset}
	return execute[Document](ctx, client, Request{Method: "PUT", Path: path + "/offsets", Body: body})
}

// StreamLag returns one consumer group's lag for a partition.
func (client *Client) StreamLag(ctx context.Context, stream, group string, partition uint32) (Document, error) {
	path, err := streamGroupPath(stream, group)
	if err != nil {
		return nil, err
	}
	query := url.Values{"partition": {strconv.FormatUint(uint64(partition), 10)}}
	return execute[Document](ctx, client, Request{Method: "GET", Path: path + "/lag", Query: query})
}

// Send enqueues one event in a Work Queue.
func (client *Client) Send(ctx context.Context, queue string, event EventEnvelope) (Document, error) {
	name, err := segment(queue, "queue")
	if err != nil {
		return nil, err
	}
	event, err = event.normalized()
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "POST", Path: "/v1/queues/" + name + "/messages", Body: event})
}

// Receive acquires a bounded batch of renewable Work Queue leases.
func (client *Client) Receive(ctx context.Context, queue string, options QueueReceiveOptions) ([]Document, error) {
	name, err := segment(queue, "queue")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(options.Consumer) == "" {
		return nil, fmt.Errorf("epoch: consumer is required")
	}
	if options.MaxMessages == 0 || options.MaxMessages > 1_000 {
		return nil, fmt.Errorf("epoch: max messages must be between 1 and 1000")
	}
	if options.VisibilityTimeoutMS != nil && *options.VisibilityTimeoutMS == 0 {
		return nil, fmt.Errorf("epoch: visibility timeout must be greater than zero")
	}
	body := struct {
		Consumer            string  `json:"consumer"`
		MaxMessages         uint32  `json:"max_messages"`
		VisibilityTimeoutMS *uint64 `json:"visibility_timeout_ms"`
	}{options.Consumer, options.MaxMessages, options.VisibilityTimeoutMS}
	return execute[[]Document](ctx, client, Request{Method: "POST", Path: "/v1/queues/" + name + "/acquire", Body: body})
}

// Acknowledge settles one leased message successfully.
func (client *Client) Acknowledge(ctx context.Context, queue, leaseToken string) (Document, error) {
	return client.settle(ctx, queue, settlement{Action: "ack", Token: leaseToken})
}

// Release returns one leased message to the Queue after an optional delay.
func (client *Client) Release(ctx context.Context, queue, leaseToken string, delayMS uint64, reason string) (Document, error) {
	return client.settle(ctx, queue, settlement{Action: "release", Token: leaseToken, DelayMS: &delayMS, Reason: reason})
}

// Reject terminally rejects one leased message with a reason.
func (client *Client) Reject(ctx context.Context, queue, leaseToken, reason string) (Document, error) {
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("epoch: rejection reason is required")
	}
	return client.settle(ctx, queue, settlement{Action: "reject", Token: leaseToken, Reason: reason})
}

// ExtendLease lengthens one active Work Queue lease.
func (client *Client) ExtendLease(ctx context.Context, queue, leaseToken string, extensionMS uint64) (Document, error) {
	if extensionMS == 0 {
		return nil, fmt.Errorf("epoch: extension must be greater than zero")
	}
	return client.settle(ctx, queue, settlement{Action: "extend", Token: leaseToken, ExtensionMS: &extensionMS})
}

// QueueCounts returns the Queue's current state counters.
func (client *Client) QueueCounts(ctx context.Context, queue string) (Document, error) {
	name, err := segment(queue, "queue")
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "GET", Path: "/v1/queues/" + name + "/counts"})
}

// Redrive moves one dead-lettered message back into delivery.
func (client *Client) Redrive(ctx context.Context, queue, messageID string) (Document, error) {
	name, err := segment(queue, "queue")
	if err != nil {
		return nil, err
	}
	message, err := segment(messageID, "message ID")
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "POST", Path: "/v1/queues/" + name + "/dead-letters/" + message + "/redrive"})
}

// Publish publishes one event to an Event Bus.
func (client *Client) Publish(ctx context.Context, bus string, event EventEnvelope) (Document, error) {
	name, err := segment(bus, "bus")
	if err != nil {
		return nil, err
	}
	event, err = event.normalized()
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "POST", Path: "/v1/buses/" + name + "/events", Body: event})
}

// UpsertSubscription creates or replaces one typed Event Bus subscription.
func (client *Client) UpsertSubscription(ctx context.Context, bus string, subscription Subscription) (Document, error) {
	name, err := segment(bus, "bus")
	if err != nil {
		return nil, err
	}
	subscription, err = subscription.normalized()
	if err != nil {
		return nil, err
	}
	subscriptionName, err := segment(subscription.Name, "subscription")
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "PUT", Path: "/v1/buses/" + name + "/subscriptions/" + subscriptionName, Body: subscription})
}

// RemoveSubscription deletes one Event Bus subscription.
func (client *Client) RemoveSubscription(ctx context.Context, bus, subscription string) (Document, error) {
	name, err := segment(bus, "bus")
	if err != nil {
		return nil, err
	}
	subscriptionName, err := segment(subscription, "subscription")
	if err != nil {
		return nil, err
	}
	return execute[Document](ctx, client, Request{Method: "DELETE", Path: "/v1/buses/" + name + "/subscriptions/" + subscriptionName})
}

// ReplayBus reads archived events within explicit time and type bounds.
func (client *Client) ReplayBus(ctx context.Context, bus string, options BusReplayOptions) ([]Document, error) {
	name, err := segment(bus, "bus")
	if err != nil {
		return nil, err
	}
	if options.FromMS > options.ToMS {
		return nil, fmt.Errorf("epoch: replay start cannot exceed end")
	}
	if options.Limit == 0 || options.Limit > 10_000 {
		return nil, fmt.Errorf("epoch: limit must be between 1 and 10000")
	}
	query := url.Values{}
	query.Set("from_ms", strconv.FormatUint(options.FromMS, 10))
	query.Set("to_ms", strconv.FormatUint(options.ToMS, 10))
	query.Set("limit", strconv.FormatUint(uint64(options.Limit), 10))
	if options.EventType != "" {
		query.Set("event_type", options.EventType)
	}
	return execute[[]Document](ctx, client, Request{Method: "GET", Path: "/v1/buses/" + name + "/replay", Query: query})
}

type queueRetryConfig struct {
	Strategy       string  `json:"strategy"`
	InitialDelayMS uint64  `json:"initial_delay_ms"`
	MaxDelayMS     uint64  `json:"max_delay_ms"`
	JitterPercent  uint8   `json:"jitter_percent"`
	MaxAttempts    uint32  `json:"max_attempts"`
	MaxAgeMS       *uint64 `json:"max_age_ms"`
}

type settlement struct {
	Action      string  `json:"action"`
	Token       string  `json:"token"`
	DelayMS     *uint64 `json:"delay_ms,omitempty"`
	Reason      string  `json:"reason,omitempty"`
	ExtensionMS *uint64 `json:"extension_ms,omitempty"`
}

func (client *Client) settle(ctx context.Context, queue string, body settlement) (Document, error) {
	name, err := segment(queue, "queue")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(body.Token) == "" {
		return nil, fmt.Errorf("epoch: lease token is required")
	}
	return execute[Document](ctx, client, Request{Method: "POST", Path: "/v1/queues/" + name + "/settle", Body: body})
}

func execute[T any](ctx context.Context, client *Client, request Request) (T, error) {
	var result T
	if client == nil || client.transport == nil {
		return result, fmt.Errorf("epoch: client transport is not configured")
	}
	if err := client.transport.Do(ctx, request, &result); err != nil {
		return result, err
	}
	return result, nil
}

func resourcePath(collection, name string) (string, error) {
	encoded, err := segment(name, "resource name")
	if err != nil {
		return "", err
	}
	return "/v1/" + collection + "/" + encoded, nil
}

func cacheKeyPath(cache, key string) (string, error) {
	cacheName, err := segment(cache, "cache")
	if err != nil {
		return "", err
	}
	cacheKey, err := segment(key, "cache key")
	if err != nil {
		return "", err
	}
	return "/v1/caches/" + cacheName + "/keys/" + cacheKey, nil
}

func streamGroupPath(stream, group string) (string, error) {
	streamName, err := segment(stream, "stream")
	if err != nil {
		return "", err
	}
	groupName, err := segment(group, "consumer group")
	if err != nil {
		return "", err
	}
	return "/v1/streams/" + streamName + "/groups/" + groupName, nil
}

func segment(value, label string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("epoch: %s is required", label)
	}
	return url.PathEscape(value), nil
}
