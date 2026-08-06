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
	requests        []Request
	operationErrors []error
}

func (transport *regionalFakeTransport) Do(_ context.Context, request Request, result any) error {
	transport.requests = append(transport.requests, request)
	response := transport.route
	isDiscovery := request.Method == "GET" && strings.HasSuffix(request.Path, "/shards/0")
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

type regionalErrorTransport struct {
	err      error
	requests int
}

func (transport *regionalErrorTransport) Do(_ context.Context, _ Request, _ any) error {
	transport.requests++
	return transport.err
}
