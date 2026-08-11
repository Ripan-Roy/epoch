package epoch

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	regionalAuthorizationHeader = "authorization"
	regionalGenerationHeader    = "x-epoch-resource-generation"
	regionalTabletEpochHeader   = "x-epoch-tablet-epoch"
	regionalReadHeader          = "x-epoch-read-consistency"
	maxRegionalFetchRecords     = 1_000
	maxStreamRetentionRecords   = 100_000
	maxStreamRetentionBytes     = 3 * 1024 * 1024
	maxStreamRetentionAgeMS     = 10 * 365 * 24 * 60 * 60 * 1_000
)

// RegionalScope identifies one fully-qualified Epoch namespace.
type RegionalScope struct {
	Organization string
	Project      string
	Environment  string
	Namespace    string
}

// RegionalStreamClient routes authenticated Stream calls across regional nodes.
// It discovers the current leader before each operation and retries only with
// the caller's unchanged idempotency key.
type RegionalStreamClient struct {
	regional *regionalClient
}

type regionalClient struct {
	transports []Transport
	token      string
	scopePath  string
}

type regionalRoute struct {
	ResourceGeneration string `json:"resource_generation"`
	TabletEpoch        string `json:"tablet_epoch"`
	Term               string `json:"term"`
	AcceptsWrites      bool   `json:"accepts_writes"`
}

// StreamRetentionPolicy bounds retained records independently by count, canonical bytes,
// and age. A zero field disables that bound.
type StreamRetentionPolicy struct {
	MaxRecordsPerPartition uint64
	MaxBytesPerPartition   uint64
	MaxAgeMS               uint64
}

func (policy StreamRetentionPolicy) validate() error {
	if policy.MaxRecordsPerPartition > maxStreamRetentionRecords {
		return fmt.Errorf("epoch: Stream retention max records must be between 1 and %d when set", maxStreamRetentionRecords)
	}
	if policy.MaxBytesPerPartition > maxStreamRetentionBytes {
		return fmt.Errorf("epoch: Stream retention max bytes must be between 1 and %d when set", maxStreamRetentionBytes)
	}
	if policy.MaxAgeMS > maxStreamRetentionAgeMS {
		return fmt.Errorf("epoch: Stream retention max age must be between 1 and %d milliseconds when set", maxStreamRetentionAgeMS)
	}
	return nil
}

// NewRegionalStreamClient builds a regional client over one or more HTTP endpoints.
func NewRegionalStreamClient(endpoints []string, token string, scope RegionalScope, timeout time.Duration) (*RegionalStreamClient, error) {
	regional, err := newRegionalClient(endpoints, token, scope, timeout)
	if err != nil {
		return nil, err
	}
	return &RegionalStreamClient{regional: regional}, nil
}

// NewRegionalStreamClientWithTransports injects endpoint transports for tests or custom networking.
func NewRegionalStreamClientWithTransports(transports []Transport, token string, scope RegionalScope) (*RegionalStreamClient, error) {
	regional, err := newRegionalClientWithTransports(transports, token, scope)
	if err != nil {
		return nil, err
	}
	return &RegionalStreamClient{regional: regional}, nil
}

func newRegionalClient(endpoints []string, token string, scope RegionalScope, timeout time.Duration) (*regionalClient, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("epoch: at least one regional endpoint is required")
	}
	transports := make([]Transport, 0, len(endpoints))
	for _, endpoint := range endpoints {
		transport, err := NewHTTPTransport(endpoint, timeout)
		if err != nil {
			return nil, err
		}
		transports = append(transports, transport)
	}
	return newRegionalClientWithTransports(transports, token, scope)
}

func newRegionalClientWithTransports(transports []Transport, token string, scope RegionalScope) (*regionalClient, error) {
	if len(transports) == 0 {
		return nil, fmt.Errorf("epoch: at least one regional transport is required")
	}
	for _, transport := range transports {
		if transport == nil {
			return nil, fmt.Errorf("epoch: regional transports cannot contain nil")
		}
	}
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, fmt.Errorf("epoch: bearer token is required and must fit one HTTP header")
	}
	scopePath, err := regionalScopePath(scope)
	if err != nil {
		return nil, err
	}
	return &regionalClient{transports: append([]Transport(nil), transports...), token: token, scopePath: scopePath}, nil
}

// Append appends one record after discovering the current leader and route fences.
func (client *RegionalStreamClient) Append(ctx context.Context, stream string, shard uint32, idempotencyKey string, event EventEnvelope) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	event, err := event.normalized()
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{
			Method: "POST",
			Path:   "/records",
			Body: struct {
				IdempotencyKey string        `json:"idempotency_key"`
				ExpectedTerm   string        `json:"expected_term"`
				Partition      uint32        `json:"partition"`
				Envelope       EventEnvelope `json:"envelope"`
			}{idempotencyKey, route.Term, 0, event},
		}
	})
}

// Fetch performs a linearizable bounded read from one Stream shard.
func (client *RegionalStreamClient) Fetch(ctx context.Context, stream string, shard uint32, offset uint64, limit uint32) (Document, error) {
	if limit == 0 || limit > maxRegionalFetchRecords {
		return nil, fmt.Errorf("epoch: fetch limit must be between 1 and %d", maxRegionalFetchRecords)
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/records", Query: url.Values{
			"offset": {strconv.FormatUint(offset, 10)},
			"limit":  {strconv.FormatUint(uint64(limit), 10)},
		}, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// CommitOffset commits or explicitly resets a generation-fenced next offset.
func (client *RegionalStreamClient) CommitOffset(ctx context.Context, stream string, shard uint32, group, member string, generation, nextOffset uint64, reset bool, idempotencyKey string) (Document, error) {
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(member) == "" {
		return nil, fmt.Errorf("epoch: consumer member is required")
	}
	if generation == 0 {
		return nil, fmt.Errorf("epoch: consumer group generation must be non-zero")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	mode := "commit"
	if reset {
		mode = "reset"
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{Method: "PUT", Path: "/groups/" + groupSegment + "/offsets", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			MemberID       string `json:"member_id"`
			Generation     string `json:"group_generation"`
			Partition      uint32 `json:"partition"`
			NextOffset     string `json:"next_offset"`
			Mode           string `json:"mode"`
		}{idempotencyKey, route.Term, member, strconv.FormatUint(generation, 10), 0, strconv.FormatUint(nextOffset, 10), mode}}
	})
}

// Lag returns the linearizable checkpoint and lag observation for a group.
func (client *RegionalStreamClient) Lag(ctx context.Context, stream string, shard uint32, group string) (Document, error) {
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/groups/" + groupSegment + "/lag", Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// FetchGroup performs a linearizable fetch beginning at the durable group checkpoint.
func (client *RegionalStreamClient) FetchGroup(ctx context.Context, stream string, shard uint32, group string, limit uint32) (Document, error) {
	if limit == 0 || limit > maxRegionalFetchRecords {
		return nil, fmt.Errorf("epoch: fetch limit must be between 1 and %d", maxRegionalFetchRecords)
	}
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/groups/" + groupSegment + "/records", Query: url.Values{"limit": {strconv.FormatUint(uint64(limit), 10)}}, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// ConfigureRetention commits a replacement time/size/count policy and immediately applies it.
func (client *RegionalStreamClient) ConfigureRetention(ctx context.Context, stream string, shard uint32, idempotencyKey string, policy StreamRetentionPolicy) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		body := struct {
			IdempotencyKey         string `json:"idempotency_key"`
			ExpectedTerm           string `json:"expected_term"`
			MaxRecordsPerPartition uint64 `json:"max_records_per_partition,omitempty"`
			MaxBytesPerPartition   string `json:"max_bytes_per_partition,omitempty"`
			MaxAgeMS               string `json:"max_age_ms,omitempty"`
		}{
			IdempotencyKey:         idempotencyKey,
			ExpectedTerm:           route.Term,
			MaxRecordsPerPartition: policy.MaxRecordsPerPartition,
		}
		if policy.MaxBytesPerPartition != 0 {
			body.MaxBytesPerPartition = strconv.FormatUint(policy.MaxBytesPerPartition, 10)
		}
		if policy.MaxAgeMS != 0 {
			body.MaxAgeMS = strconv.FormatUint(policy.MaxAgeMS, 10)
		}
		return Request{Method: "PUT", Path: "/retention", Body: body}
	})
}

// MaintainRetention commits an idle-stream age sweep using the current leader time.
func (client *RegionalStreamClient) MaintainRetention(ctx context.Context, stream string, shard uint32, idempotencyKey string) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/retention/maintenance", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
		}{idempotencyKey, route.Term}}
	})
}

// Retention returns the linearizable policy, watermark, retained boundary, and byte count.
func (client *RegionalStreamClient) Retention(ctx context.Context, stream string, shard uint32) (Document, error) {
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/retention", Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

func (client *RegionalStreamClient) regionalClient() *regionalClient {
	if client == nil {
		return nil
	}
	return client.regional
}

func regionalCall[T any](ctx context.Context, client *regionalClient, collection, resourceLabel, resource string, shard uint32, requestFor func(regionalRoute) Request) (T, error) {
	var zero T
	if client == nil {
		return zero, fmt.Errorf("epoch: regional %s client is not configured", resourceLabel)
	}
	basePath, err := client.resourceShardPath(collection, resourceLabel, resource, shard)
	if err != nil {
		return zero, err
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		transport, route, discoverErr := client.discoverLeader(ctx, basePath)
		if discoverErr != nil {
			lastErr = discoverErr
			if !regionalRediscoveryError(discoverErr) {
				return zero, discoverErr
			}
			continue
		}
		request := requestFor(route)
		request.Path = basePath + request.Path
		request.Headers = mergedRegionalHeaders(request.Headers, client.token, route)
		var result T
		if callErr := transport.Do(ctx, request, &result); callErr == nil {
			return result, nil
		} else {
			lastErr = callErr
			if !regionalRediscoveryError(callErr) {
				return zero, callErr
			}
		}
	}
	return zero, fmt.Errorf("epoch: regional %s operation could not reach a current leader: %w", resourceLabel, lastErr)
}

func (client *regionalClient) discoverLeader(ctx context.Context, path string) (Transport, regionalRoute, error) {
	var lastErr error
	for _, transport := range client.transports {
		var route regionalRoute
		err := transport.Do(ctx, Request{Method: "GET", Path: path, Headers: map[string]string{regionalAuthorizationHeader: "Bearer " + client.token}}, &route)
		if err != nil {
			if !regionalRediscoveryError(err) {
				return nil, regionalRoute{}, err
			}
			lastErr = err
			continue
		}
		if !validRegionalRoute(route) {
			lastErr = fmt.Errorf("epoch: regional route response is incomplete")
			continue
		}
		if route.AcceptsWrites {
			return transport, route, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("epoch: no configured endpoint reported the current leader")
	}
	return nil, regionalRoute{}, lastErr
}

func validRegionalRoute(route regionalRoute) bool {
	for _, value := range []string{route.ResourceGeneration, route.TabletEpoch, route.Term} {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || parsed == 0 || value != strconv.FormatUint(parsed, 10) {
			return false
		}
	}
	return true
}

func mergedRegionalHeaders(headers map[string]string, token string, route regionalRoute) map[string]string {
	merged := make(map[string]string, len(headers)+3)
	for name, value := range headers {
		merged[name] = value
	}
	merged[regionalAuthorizationHeader] = "Bearer " + token
	merged[regionalGenerationHeader] = route.ResourceGeneration
	merged[regionalTabletEpochHeader] = route.TabletEpoch
	return merged
}

func regionalRediscoveryError(err error) bool {
	var failure *APIError
	if !errors.As(err, &failure) {
		return false
	}
	if failure.Retryable() {
		return true
	}
	switch failure.Code {
	case "not_leader", "fenced", "route_not_found", "route_unavailable", "read_barrier_timeout":
		return true
	default:
		return false
	}
}

func regionalScopePath(scope RegionalScope) (string, error) {
	organization, err := segment(scope.Organization, "organization")
	if err != nil {
		return "", err
	}
	project, err := segment(scope.Project, "project")
	if err != nil {
		return "", err
	}
	environment, err := segment(scope.Environment, "environment")
	if err != nil {
		return "", err
	}
	namespace, err := segment(scope.Namespace, "namespace")
	if err != nil {
		return "", err
	}
	return "/v1/organizations/" + organization + "/projects/" + project + "/environments/" + environment + "/namespaces/" + namespace, nil
}

func (client *regionalClient) resourceShardPath(collection, resourceLabel, resource string, shard uint32) (string, error) {
	resourceName, err := segment(resource, resourceLabel)
	if err != nil {
		return "", err
	}
	return client.scopePath + "/" + collection + "/" + resourceName + "/shards/" + strconv.FormatUint(uint64(shard), 10), nil
}
