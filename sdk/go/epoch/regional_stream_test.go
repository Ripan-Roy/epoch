package epoch

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type regionalFakeTransport struct {
	route           Document
	routes          map[string]Document
	requests        []Request
	operationErrors []error
}

func (transport *regionalFakeTransport) Do(_ context.Context, request Request, result any) error {
	transport.requests = append(transport.requests, request)
	response := transport.route
	shardMarker := strings.LastIndex(request.Path, "/shards/")
	isDiscovery := request.Method == "GET" && shardMarker >= 0 &&
		!strings.Contains(request.Path[shardMarker+len("/shards/"):], "/")
	if isDiscovery && transport.routes != nil {
		response = transport.routes[request.Path[shardMarker+len("/shards/"):]]
	}
	if !isDiscovery && len(transport.operationErrors) > 0 {
		err := transport.operationErrors[0]
		transport.operationErrors = transport.operationErrors[1:]
		return err
	}
	if !isDiscovery {
		response = Document{"state": "committed", "outcome_certainty": "committed"}
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, result)
}

func TestRegionalStreamKeyedAppendUsesPublishedUTF8Partitioning(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "5",
		"tablet_epoch":        "3",
		"term":                "8",
		"accepts_writes":      true,
		"stream_partitioning": Document{
			"algorithm":            "fnv1a64_utf8_mod_n_v1",
			"key_encoding":         "utf8",
			"missing_key_fallback": "event_id",
			"shard_count":          float64(16),
		},
	}}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader},
		"secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	event := NewEventEnvelope("checkout", "order.created", map[string]any{"id": "42"})
	event.ID = "order-1"
	event.Key = "customer-42"
	event.TimeMS = 42

	if shard, err := StreamShardFor("customer-42", 16); err != nil || shard != 14 {
		t.Fatalf("unexpected published partition: shard=%d err=%v", shard, err)
	}
	for value, expected := range map[string]uint32{
		"order-1": 13,
		"café":    9,
		"東京":      15,
	} {
		if shard, vectorErr := StreamShardFor(value, 16); vectorErr != nil || shard != expected {
			t.Fatalf("unexpected vector for %q: shard=%d err=%v", value, shard, vectorErr)
		}
	}
	if _, err := client.AppendKeyed(context.Background(), "orders", "append-42", event); err != nil {
		t.Fatal(err)
	}

	if len(leader.requests) != 3 {
		t.Fatalf("expected routing discovery, shard discovery, and write; got %d requests", len(leader.requests))
	}
	if !strings.HasSuffix(leader.requests[0].Path, "/shards/0") ||
		!strings.HasSuffix(leader.requests[1].Path, "/shards/14") ||
		!strings.HasSuffix(leader.requests[2].Path, "/shards/14/records") {
		t.Fatalf("keyed append did not route through shard 14: %#v", leader.requests)
	}
}

func TestRegionalStreamKeyedAppendFailsClosedWhenRoutingGenerationChanges(t *testing.T) {
	bootstrap := Document{
		"resource_generation": "5", "tablet_epoch": "3", "term": "8", "accepts_writes": true,
		"stream_partitioning": Document{
			"algorithm": "fnv1a64_utf8_mod_n_v1", "key_encoding": "utf8",
			"missing_key_fallback": "event_id", "shard_count": float64(16),
		},
	}
	target := Document{
		"resource_generation": "6", "tablet_epoch": "3", "term": "9", "accepts_writes": true,
		"stream_partitioning": bootstrap["stream_partitioning"],
	}
	transport := &regionalFakeTransport{routes: map[string]Document{"0": bootstrap, "14": target}}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{transport}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	event := NewEventEnvelope("checkout", "order.created", nil)
	event.ID = "order-1"
	event.Key = "customer-42"

	_, err = client.AppendKeyed(context.Background(), "orders", "append-42", event)
	if err == nil || !strings.Contains(err.Error(), "generation changed") {
		t.Fatalf("expected a fail-closed routing generation error, got %v", err)
	}
	if len(transport.requests) != 2 {
		t.Fatalf("generation mismatch must fail before a write, got %#v", transport.requests)
	}
}

func TestRegionalStreamClientRediscoversAndKeepsMutationIdentity(t *testing.T) {
	leader := &regionalFakeTransport{
		route: Document{
			"resource_generation": "5",
			"tablet_epoch":        "3",
			"term":                "8",
			"accepts_writes":      true,
		},
		operationErrors: []error{&APIError{
			StatusCode: 409,
			Code:       "not_leader",
			Detail:     "leadership changed",
		}},
	}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader},
		"secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	event := NewEventEnvelope("checkout", "order.created", map[string]any{"id": "42"})
	event.ID = "order-42"
	event.TimeMS = 42

	if _, err := client.Append(context.Background(), "orders", 0, "append-42", event); err != nil {
		t.Fatal(err)
	}

	if len(leader.requests) != 4 {
		t.Fatalf("expected discover/write/discover/write, got %d requests", len(leader.requests))
	}
	for _, index := range []int{1, 3} {
		body, marshalErr := json.Marshal(leader.requests[index].Body)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		var document map[string]any
		if unmarshalErr := json.Unmarshal(body, &document); unmarshalErr != nil {
			t.Fatal(unmarshalErr)
		}
		if document["idempotency_key"] != "append-42" {
			t.Fatalf("request %d changed mutation identity: %#v", index, document)
		}
	}
}

func TestRegionalStreamClientPreservesDefinitiveDiscoveryFailure(t *testing.T) {
	denied := &regionalErrorTransport{err: &APIError{
		StatusCode: 403,
		Code:       "forbidden",
		Detail:     "scope denied",
	}}
	unused := &regionalFakeTransport{route: Document{
		"resource_generation": "5",
		"tablet_epoch":        "3",
		"term":                "8",
		"accepts_writes":      true,
	}}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{denied, unused},
		"secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Fetch(context.Background(), "orders", 0, 0, 1)
	var failure *APIError
	if !errors.As(err, &failure) || failure.StatusCode != 403 || failure.Code != "forbidden" {
		t.Fatalf("expected the definitive discovery failure, got %v", err)
	}
	if denied.requests != 1 || len(unused.requests) != 0 {
		t.Fatalf("definitive denial must stop discovery, got %d denied and %d unused requests", denied.requests, len(unused.requests))
	}
}

func TestRegionalRouteRequiresCanonicalUnsignedIntegers(t *testing.T) {
	for _, route := range []regionalRoute{
		{ResourceGeneration: "01", TabletEpoch: "3", Term: "8"},
		{ResourceGeneration: "5", TabletEpoch: "+3", Term: "8"},
		{ResourceGeneration: "5", TabletEpoch: "3", Term: "18446744073709551616"},
	} {
		if validRegionalRoute(route) {
			t.Fatalf("accepted noncanonical or out-of-range route: %#v", route)
		}
	}
}

func TestRegionalStreamClientDiscoversLeaderAndCarriesAuthFencesAndTerm(t *testing.T) {
	follower := &regionalFakeTransport{route: Document{
		"resource_generation": "5",
		"tablet_epoch":        "3",
		"term":                "8",
		"accepts_writes":      false,
	}}
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "5",
		"tablet_epoch":        "3",
		"term":                "8",
		"accepts_writes":      true,
	}}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{follower, leader},
		"secret-token",
		RegionalScope{
			Organization: "acme",
			Project:      "shop",
			Environment:  "dev",
			Namespace:    "core",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	event := NewEventEnvelope("checkout", "order.created", map[string]any{"id": "42"})
	event.ID = "order-42"
	event.TimeMS = 42

	response, err := client.Append(context.Background(), "orders/eu", 0, "append-42", event)
	if err != nil {
		t.Fatal(err)
	}
	if response["state"] != "committed" {
		t.Fatalf("unexpected response: %#v", response)
	}
	if len(follower.requests) != 1 || len(leader.requests) != 2 {
		t.Fatalf("expected one follower discovery and leader discovery/write, got %d and %d", len(follower.requests), len(leader.requests))
	}
	path := "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/streams/orders%2Feu/shards/0"
	if follower.requests[0].Path != path || leader.requests[0].Path != path {
		t.Fatalf("unexpected discovery paths: %q %q", follower.requests[0].Path, leader.requests[0].Path)
	}
	write := leader.requests[1]
	if write.Path != path+"/records" || write.Method != "POST" {
		t.Fatalf("unexpected write request: %#v", write)
	}
	if write.Headers["authorization"] != "Bearer secret-token" ||
		write.Headers["x-epoch-resource-generation"] != "5" ||
		write.Headers["x-epoch-tablet-epoch"] != "3" {
		t.Fatalf("write did not carry authentication and fences: %#v", write.Headers)
	}
	body, err := json.Marshal(write.Body)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(body, &document); err != nil {
		t.Fatal(err)
	}
	if document["idempotency_key"] != "append-42" || document["expected_term"] != "8" {
		t.Fatalf("write did not carry stable mutation identity and term: %#v", document)
	}
}

func TestRegionalStreamClientBuildsGroupAndLinearizableReadContracts(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "2",
		"tablet_epoch":        "4",
		"term":                "9",
		"accepts_writes":      true,
	}}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader},
		"secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.CommitOffset(context.Background(), "orders", 0, "billing/eu", "member-a", 3, 11, false, "commit-11"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Fetch(context.Background(), "orders", 0, 11, 25); err != nil {
		t.Fatal(err)
	}

	commit := leader.requests[1]
	if commit.Method != "PUT" || commit.Path[len(commit.Path)-len("/groups/billing%2Feu/offsets"):] != "/groups/billing%2Feu/offsets" {
		t.Fatalf("unexpected group commit path: %#v", commit)
	}
	read := leader.requests[3]
	if read.Method != "GET" || read.Headers["x-epoch-read-consistency"] != "linearizable" {
		t.Fatalf("unexpected read contract: %#v", read)
	}
	if read.Query.Get("offset") != "11" || read.Query.Get("limit") != "25" {
		t.Fatalf("unexpected fetch query: %v", read.Query)
	}
}

func TestRegionalStreamClientBuildsRetentionMutationAndReadContracts(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "2", "tablet_epoch": "4", "term": "9", "accepts_writes": true,
	}}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := StreamRetentionPolicy{
		MaxRecordsPerPartition: 100,
		MaxBytesPerPartition:   1_048_576,
		MaxAgeMS:               86_400_000,
	}

	if _, err := client.ConfigureRetention(context.Background(), "orders", 0, "retention-1", policy); err != nil {
		t.Fatal(err)
	}
	if _, err := client.MaintainRetention(context.Background(), "orders", 0, "retention-sweep-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Retention(context.Background(), "orders", 0); err != nil {
		t.Fatal(err)
	}

	configure := leader.requests[1]
	if configure.Method != "PUT" || !strings.HasSuffix(configure.Path, "/retention") {
		t.Fatalf("unexpected retention configuration: %#v", configure)
	}
	body, marshalErr := json.Marshal(configure.Body)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var document map[string]any
	if unmarshalErr := json.Unmarshal(body, &document); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if document["idempotency_key"] != "retention-1" || document["expected_term"] != "9" ||
		document["max_bytes_per_partition"] != "1048576" || document["max_age_ms"] != "86400000" {
		t.Fatalf("unexpected retention body: %#v", document)
	}
	maintenance := leader.requests[3]
	if maintenance.Method != "POST" || !strings.HasSuffix(maintenance.Path, "/retention/maintenance") {
		t.Fatalf("unexpected retention maintenance: %#v", maintenance)
	}
	read := leader.requests[5]
	if read.Method != "GET" || !strings.HasSuffix(read.Path, "/retention") ||
		read.Headers[regionalReadHeader] != "linearizable" {
		t.Fatalf("unexpected retention read: %#v", read)
	}
}

func TestRegionalStreamRetentionRejectsInvalidPolicyBeforeNetwork(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "2", "tablet_epoch": "4", "term": "9", "accepts_writes": true,
	}}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ConfigureRetention(context.Background(), "orders", 0, "retention-invalid", StreamRetentionPolicy{
		MaxBytesPerPartition: 3*1024*1024 + 1,
	})
	if err == nil || len(leader.requests) != 0 {
		t.Fatalf("invalid retention must fail locally, got %v and %d requests", err, len(leader.requests))
	}
}

type regionalErrorTransport struct {
	err      error
	requests int
}

func (transport *regionalErrorTransport) Do(_ context.Context, _ Request, _ any) error {
	transport.requests++
	return transport.err
}
