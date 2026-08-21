package epoch

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type regionalFakeTransport struct {
	route              Document
	routes             map[string]Document
	requests           []Request
	operationErrors    []error
	operationResponses []Document
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
		if len(transport.operationResponses) > 0 {
			response = transport.operationResponses[0]
			transport.operationResponses = transport.operationResponses[1:]
		} else {
			response = Document{"state": "committed", "outcome_certainty": "committed"}
		}
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

func TestRegionalStreamClientBuildsCoordinatedConsumerSessionContracts(t *testing.T) {
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
	ctx := context.Background()
	if _, err := client.JoinConsumerSession(ctx, "orders", "billing/eu", "member-a", 30*time.Second, "join-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.HeartbeatConsumerSession(ctx, "orders", "billing/eu", "member-a", 3, "heartbeat-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ConsumerSession(ctx, "orders", "billing/eu"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.MaintainConsumerSession(ctx, "orders", "billing/eu", "maintain-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.LeaveConsumerSession(ctx, "orders", "billing/eu", "member-a", 3, "leave-a"); err != nil {
		t.Fatal(err)
	}

	expected := []struct {
		index  int
		method string
		suffix string
	}{
		{1, "POST", "/groups/billing%2Feu/sessions"},
		{3, "PUT", "/groups/billing%2Feu/sessions/member-a/heartbeat"},
		{5, "GET", "/groups/billing%2Feu/sessions"},
		{7, "POST", "/groups/billing%2Feu/sessions/maintenance"},
		{9, "DELETE", "/groups/billing%2Feu/sessions/member-a"},
	}
	for _, want := range expected {
		request := leader.requests[want.index]
		if request.Method != want.method || !strings.HasSuffix(request.Path, want.suffix) {
			t.Fatalf("unexpected session request %d: %#v", want.index, request)
		}
		if want.method == "GET" && request.Headers[regionalReadHeader] != "linearizable" {
			t.Fatalf("session observation must be linearizable: %#v", request)
		}
	}
}

func TestRegionalStreamClientClaimsAndRevalidatesAssignedShardsBeforeFencedFetch(t *testing.T) {
	session := Document{"session": Document{
		"exists": true, "group": "billing/eu", "shard_count": float64(3),
		"group_generation": "1", "members": []any{
			map[string]any{"member_id": "member-a", "assigned_shards": []any{float64(0), float64(2)}},
		},
	}}
	claim := Document{"receipt": Document{"outcome": "applied", "session_fenced": true}}
	unclaimed := Document{"checkpoint": Document{"exists": false}}
	leader := &regionalFakeTransport{
		route:              Document{"resource_generation": "2", "tablet_epoch": "4", "term": "9", "accepts_writes": true},
		operationResponses: []Document{session, unclaimed, unclaimed, claim, claim, session, Document{"records": []any{}}},
	}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}

	shards, err := client.ClaimConsumerSession(
		context.Background(), "orders", "billing/eu", "member-a", 1, "claim-cycle-a",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 2 || shards[0] != 0 || shards[1] != 2 {
		t.Fatalf("unexpected claimed assignment: %v", shards)
	}
	if _, err := client.FetchClaimedGroup(
		context.Background(), "orders", 2, "billing/eu", "member-a", 1, 25,
	); err != nil {
		t.Fatal(err)
	}

	wants := []struct {
		index  int
		method string
		suffix string
	}{
		{2, "GET", "/groups/billing%2Feu/sessions"},
		{4, "GET", "/groups/billing%2Feu/lag"},
		{6, "GET", "/groups/billing%2Feu/lag"},
		{8, "PUT", "/groups/billing%2Feu/claim"},
		{10, "PUT", "/groups/billing%2Feu/claim"},
		{12, "GET", "/groups/billing%2Feu/sessions"},
		{14, "GET", "/groups/billing%2Feu/claimed-records"},
	}
	for _, want := range wants {
		request := leader.requests[want.index]
		if request.Method != want.method || !strings.HasSuffix(request.Path, want.suffix) {
			t.Fatalf("unexpected fenced-consumption request %d: %#v", want.index, request)
		}
	}
	if leader.requests[8].Body.(struct {
		IdempotencyKey string `json:"idempotency_key"`
		ExpectedTerm   string `json:"expected_term"`
		MemberID       string `json:"member_id"`
		Generation     string `json:"group_generation"`
		Partition      uint32 `json:"partition"`
	}).IdempotencyKey != "claim-cycle-a-shard-0-generation-1" {
		t.Fatal("claim must derive a stable per-shard idempotency key")
	}
	fetch := leader.requests[14]
	if fetch.Query.Get("member_id") != "member-a" || fetch.Query.Get("group_generation") != "1" || fetch.Query.Get("limit") != "25" {
		t.Fatalf("unexpected claimed fetch query: %v", fetch.Query)
	}
}

func TestConsumerClaimPlannerBridgesOnlyBoundedMonotonicGenerations(t *testing.T) {
	bridged, err := claimGenerations(Document{"checkpoint": Document{
		"exists": true, "group_generation": "1",
	}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(bridged) != 2 || bridged[0] != 2 || bridged[1] != 3 {
		t.Fatalf("unexpected generation bridge: %v", bridged)
	}
	retry, err := claimGenerations(Document{"checkpoint": Document{
		"exists": true, "group_generation": "3",
	}}, 3)
	if err != nil || len(retry) != 1 || retry[0] != 3 {
		t.Fatalf("exact generation must be reclaimed idempotently: %v %v", retry, err)
	}
	if _, err := claimGenerations(Document{"checkpoint": Document{
		"exists": true, "group_generation": "4",
	}}, 3); err == nil {
		t.Fatal("checkpoint ahead of the coordinator must fail closed")
	}
	if _, err := claimGenerations(Document{"checkpoint": Document{"exists": false}}, 4_097); err == nil {
		t.Fatal("unbounded generation bridge must fail closed")
	}
}

func TestRegionalStreamClientPreservesConsumerFenceWithoutRoutingRediscovery(t *testing.T) {
	fenced := &APIError{
		StatusCode: 409,
		Code:       "fenced",
		Detail:     "consumer member or session generation is fenced",
		Body:       json.RawMessage(`{"error":{"code":"fenced","outcome_certainty":"definite_not_committed"}}`),
	}
	leader := &regionalFakeTransport{
		route: Document{
			"resource_generation": "2", "tablet_epoch": "4", "term": "9", "accepts_writes": true,
		},
		operationErrors: []error{fenced},
	}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.FetchClaimedGroup(
		context.Background(), "orders", 0, "billing", "member-old", 2, 1,
	)
	if !errors.Is(err, fenced) {
		t.Fatalf("consumer fence must be preserved, got %v", err)
	}
	if len(leader.requests) != 2 {
		t.Fatalf("consumer fence must not trigger route rediscovery: %#v", leader.requests)
	}
}

func TestRegionalStreamClientBuildsCanonicalGzipBatchAndKeepsItAcrossRetry(t *testing.T) {
	leader := &regionalFakeTransport{
		route: Document{
			"resource_generation": "2", "tablet_epoch": "4", "term": "9", "accepts_writes": true,
		},
		operationErrors: []error{&APIError{StatusCode: 409, Code: "not_leader", Detail: "leadership changed"}},
	}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	records := []StreamBatchRecord{
		{ClientSequence: 7, Envelope: batchEvent("order-7", "customer-7")},
		{ClientSequence: 8, Envelope: batchEvent("order-8", "customer-8")},
	}
	frame, err := EncodeStreamBatch(records, StreamCompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AppendBatch(context.Background(), "orders", 2, "batch-7", frame); err != nil {
		t.Fatal(err)
	}

	if len(leader.requests) != 4 {
		t.Fatalf("expected discover/write/discover/write, got %d requests", len(leader.requests))
	}
	first := batchRequestDocument(t, leader.requests[1])
	second := batchRequestDocument(t, leader.requests[3])
	if !bytes.Equal(first, second) {
		t.Fatalf("retry changed the exact batch body:\n%s\n%s", first, second)
	}
	var body map[string]any
	if err := json.Unmarshal(first, &body); err != nil {
		t.Fatal(err)
	}
	if body["idempotency_key"] != "batch-7" || body["compression"] != "gzip" ||
		body["record_count"] != float64(2) || body["partition"] != float64(0) {
		t.Fatalf("unexpected batch body: %#v", body)
	}
	payload, err := base64.StdEncoding.DecodeString(body["payload_base64"].(string))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"client_sequence":7,"envelope":{"id":"order-7","source":"checkout","type":"order.created","time_ms":42,"key":"customer-7","headers":{"a":"first","z":"last"},"content_type":"application/json","payload":{"a":7,"z":[{"a":1,"y":2}]},"priority":0,"extensions":{"a":true,"z":{"a":1,"b":2}}}},{"client_sequence":8,"envelope":{"id":"order-8","source":"checkout","type":"order.created","time_ms":42,"key":"customer-8","headers":{"a":"first","z":"last"},"content_type":"application/json","payload":{"a":7,"z":[{"a":1,"y":2}]},"priority":0,"extensions":{"a":true,"z":{"a":1,"b":2}}}}]`
	if string(plain) != want {
		t.Fatalf("batch JSON is not the canonical cross-language contract:\n%s", plain)
	}
}

func TestStreamBatchRejectsDuplicateSequencesAndBadFramesBeforeNetwork(t *testing.T) {
	event := batchEvent("order-7", "customer-7")
	if _, err := EncodeStreamBatch([]StreamBatchRecord{
		{ClientSequence: 7, Envelope: event},
		{ClientSequence: 7, Envelope: event},
	}, StreamCompressionNone); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate sequence rejection, got %v", err)
	}
	for _, compression := range []StreamCompression{
		StreamCompressionNone,
		StreamCompressionGzip,
		StreamCompressionLZ4,
		StreamCompressionSnappy,
		StreamCompressionZstd,
	} {
		if _, err := NewStreamBatchFrame(compression, 1, 1, []byte("x")); err != nil {
			t.Fatalf("valid %s frame metadata was rejected: %v", compression, err)
		}
	}
	if _, err := NewStreamBatchFrame(StreamCompression("brotli"), 1, 2, []byte("x")); err == nil {
		t.Fatal("unsupported compression must be rejected")
	}
}

func TestStreamBatchCanonicalJSONKeepsSerdeCompatibleUnicode(t *testing.T) {
	event := batchEvent("订单\u2028七", "東京")
	event.Payload = map[string]any{"message": "<paid>&\u2029"}
	frame, err := EncodeStreamBatch(
		[]StreamBatchRecord{{ClientSequence: 1, Envelope: event}},
		StreamCompressionNone,
	)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := base64.StdEncoding.DecodeString(frame.PayloadBase64)
	if err != nil {
		t.Fatal(err)
	}
	for _, escaped := range [][]byte{[]byte(`\u2028`), []byte(`\u2029`), []byte(`\u003c`), []byte(`\u003e`), []byte(`\u0026`)} {
		if bytes.Contains(plain, escaped) {
			t.Fatalf("canonical batch JSON contains Go-only escape %q: %s", escaped, plain)
		}
	}
	if !bytes.Contains(plain, []byte("订单\u2028七")) || !bytes.Contains(plain, []byte("<paid>&\u2029")) {
		t.Fatalf("canonical batch JSON lost Unicode: %s", plain)
	}
}

func batchEvent(id, key string) EventEnvelope {
	event := NewEventEnvelope("checkout", "order.created", map[string]any{
		"z": []any{map[string]any{"y": 2, "a": 1}}, "a": 7,
	})
	event.ID = id
	event.Key = key
	event.TimeMS = 42
	event.Headers = map[string]string{"z": "last", "a": "first"}
	event.Extensions = map[string]any{"z": map[string]any{"b": 2, "a": 1}, "a": true}
	return event
}

func batchRequestDocument(t *testing.T, request Request) []byte {
	t.Helper()
	if request.Method != "POST" || !strings.HasSuffix(request.Path, "/records/batches") {
		t.Fatalf("unexpected batch request: %#v", request)
	}
	document, err := json.Marshal(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	return document
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

func TestRegionalStreamAdvancedStateContractsAreFencedAndBrowserSafe(t *testing.T) {
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
	event := NewEventEnvelope("checkout", "order.created", map[string]any{"id": "42"})
	event.ID = "order-42"
	event.TimeMS = 42
	ctx := context.Background()

	if _, err = client.AppendIdempotent(ctx, "orders", 2, "producer-1", "checkout", ^uint64(0), ^uint64(0), event); err != nil {
		t.Fatal(err)
	}
	if _, err = client.BeginTransaction(ctx, "orders", 2, "tx-begin", "tx-1", "checkout", 7); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CommitTransaction(ctx, "orders", 2, "tx-commit", "tx-1", &StreamOffsetCommit{Group: "workers", Partition: 2, NextOffset: ^uint64(0)}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.FetchWithIsolation(ctx, "orders", 2, ^uint64(0), 10, StreamReadUncommitted); err != nil {
		t.Fatal(err)
	}
	if _, err = client.PartitionAdvice(ctx, "orders", 1000, 1_048_576); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ConsumeLongPoll(ctx, "orders", 2, 0, 10, StreamReadCommitted, StreamConsumerDedicated, "analytics-a", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ConfigureCaptureSchedule(ctx, "orders", 2, "capture-schedule", "analytics", time.Minute, StreamCaptureJSONLines); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CaptureSchedule(ctx, "orders", 2, "analytics"); err != nil {
		t.Fatal(err)
	}

	producer := leader.requests[1]
	if producer.Method != "POST" || !strings.HasSuffix(producer.Path, "/state") {
		t.Fatalf("unexpected producer request: %#v", producer)
	}
	encoded, marshalErr := json.Marshal(producer.Body)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var body map[string]any
	if err = json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	operation := body["operation"].(map[string]any)
	if body["expected_term"] != "9" || operation["producer_epoch"] != "18446744073709551615" || operation["sequence"] != "18446744073709551615" {
		t.Fatalf("advanced producer metadata was not exact: %#v", body)
	}

	commit := leader.requests[5]
	encoded, marshalErr = json.Marshal(commit.Body)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if err = json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	operation = body["operation"].(map[string]any)
	offset := operation["offset_commit"].(map[string]any)
	if offset["partition"] != float64(0) || offset["next_offset"] != "18446744073709551615" {
		t.Fatalf("transaction offset was not local and exact: %#v", operation)
	}

	read := leader.requests[7]
	if read.Query.Get("isolation") != "read_uncommitted" || read.Query.Get("offset") != "18446744073709551615" {
		t.Fatalf("unexpected isolated fetch: %#v", read)
	}
	advice := leader.requests[9]
	if !strings.HasSuffix(advice.Path, "/partitions/advice") || advice.Headers[regionalReadHeader] != "linearizable" {
		t.Fatalf("unexpected partition advice request: %#v", advice)
	}
	consume := leader.requests[11]
	if !strings.HasSuffix(consume.Path, "/records/consume") || consume.Query.Get("mode") != "dedicated" || consume.Query.Get("consumer_id") != "analytics-a" {
		t.Fatalf("unexpected dedicated consume request: %#v", consume)
	}
	schedule := leader.requests[13]
	encoded, marshalErr = json.Marshal(schedule.Body)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if err = json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	operation = body["operation"].(map[string]any)
	if operation["action"] != "configure_capture_schedule" || operation["interval_ms"] != "60000" {
		t.Fatalf("unexpected automatic capture schedule: %#v", operation)
	}
	if !strings.HasSuffix(leader.requests[15].Path, "/capture-schedules/analytics") {
		t.Fatalf("unexpected capture schedule read: %#v", leader.requests[15])
	}
}

func TestRegionalStreamSuperstreamMergesIndependentShardsDeterministically(t *testing.T) {
	leader := &regionalFakeTransport{
		route: Document{
			"resource_generation": "2", "tablet_epoch": "4", "term": "9", "accepts_writes": true,
		},
		operationResponses: []Document{
			{"records": []any{
				Document{"appended_at_ms": "20", "partition": 0, "offset": "4", "value": "a"},
				Document{"appended_at_ms": "30", "partition": 0, "offset": "5", "value": "c"},
			}},
			{"records": []any{
				Document{"appended_at_ms": "10", "partition": 1, "offset": "9", "value": "b"},
			}},
		},
	}
	client, err := NewRegionalStreamClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := client.FetchSuperstream(context.Background(), []StreamSuperstreamMember{
		{Name: "orders", Stream: "orders", Shard: 0, Offset: 4},
		{Name: "audit", Stream: "audit", Shard: 1, Offset: 9},
	}, 2, StreamReadCommitted)
	if err != nil {
		t.Fatal(err)
	}
	records := merged["records"].([]any)
	first := records[0].(Document)
	second := records[1].(Document)
	if first["value"] != "b" || first["member"] != "audit" || second["value"] != "a" {
		t.Fatalf("unexpected superstream order: %#v", records)
	}
	if merged["member_count"] != 2 || merged["snapshot_scope"] != "independently_linearizable_members" {
		t.Fatalf("unexpected superstream metadata: %#v", merged)
	}
	if leader.requests[1].Query.Get("isolation") != "read_committed" {
		t.Fatalf("superstream member fetch was not isolated: %#v", leader.requests[1])
	}
	requestCount := len(leader.requests)
	_, err = client.FetchSuperstream(context.Background(), []StreamSuperstreamMember{
		{Name: "same", Stream: "orders", Shard: 0},
		{Name: "same", Stream: "audit", Shard: 0},
	}, 10, StreamReadCommitted)
	if err == nil || !strings.Contains(err.Error(), "duplicate superstream member") || len(leader.requests) != requestCount {
		t.Fatalf("duplicate member must fail before network: err=%v requests=%d", err, len(leader.requests))
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
