package epoch

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

type recordingTransport struct {
	requests []Request
	response any
}

func (transport *recordingTransport) Do(_ context.Context, request Request, result any) error {
	transport.requests = append(transport.requests, request)
	if result == nil || transport.response == nil {
		return nil
	}
	payload, err := json.Marshal(transport.response)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, result)
}

func TestCreateStreamUsesTypedDurability(t *testing.T) {
	transport := &recordingTransport{response: Document{"generation": float64(1)}}
	client := testClient(t, transport)

	_, err := client.CreateStream(context.Background(), "audit", StreamConfig{
		Partitions: 4,
		Durability: LocalDurable,
	})
	if err != nil {
		t.Fatalf("CreateStream returned an error: %v", err)
	}

	request := lastRequest(t, transport)
	if request.Method != "POST" || request.Path != "/v1/streams/audit" {
		t.Fatalf("unexpected route: %s %s", request.Method, request.Path)
	}
	body := requestBody(t, request)
	if body["partitions"] != float64(4) || body["durability"] != "local_durable" {
		t.Fatalf("unexpected stream config: %#v", body)
	}
}

func TestEventEnvelopeAndSegmentsUseWireContract(t *testing.T) {
	transport := &recordingTransport{response: Document{"offset": float64(0)}}
	client := testClient(t, transport)
	event := NewEventEnvelope("checkout", "order.created", Document{"id": "1"})
	event.ID = "order-1"
	event.TimeMS = 1_000

	_, err := client.AppendStream(context.Background(), "orders/eu", event, Uint32(1))
	if err != nil {
		t.Fatalf("AppendStream returned an error: %v", err)
	}

	request := lastRequest(t, transport)
	if request.Path != "/v1/streams/orders%2Feu/records" {
		t.Fatalf("unexpected escaped path: %s", request.Path)
	}
	body := requestBody(t, request)
	envelope := body["envelope"].(map[string]any)
	if envelope["type"] != "order.created" {
		t.Fatalf("wire type is missing: %#v", envelope)
	}
	if _, exists := envelope["event_type"]; exists {
		t.Fatalf("event_type leaked onto the wire: %#v", envelope)
	}
	if body["partition"] != float64(1) {
		t.Fatalf("partition was not encoded: %#v", body)
	}
}

func TestCacheRoutesAndConditionalWriteOptions(t *testing.T) {
	transport := &recordingTransport{}
	client := testClient(t, transport)
	ctx := context.Background()

	_, err := client.CacheSet(ctx, "sessions/eu", "user 42", "active", CacheWriteOptions{
		OnlyIfAbsent: true,
	})
	if err != nil {
		t.Fatalf("CacheSet returned an error: %v", err)
	}
	_, err = client.CacheIncrement(ctx, "sessions", "visits", 2)
	if err != nil {
		t.Fatalf("CacheIncrement returned an error: %v", err)
	}
	_, err = client.CacheDelete(ctx, "sessions", "user-42")
	if err != nil {
		t.Fatalf("CacheDelete returned an error: %v", err)
	}

	if transport.requests[0].Path != "/v1/caches/sessions%2Feu/keys/user%2042" {
		t.Fatalf("unexpected cache path: %s", transport.requests[0].Path)
	}
	if requestBody(t, transport.requests[0])["only_if_absent"] != true {
		t.Fatal("only_if_absent was not encoded")
	}
	if !reflect.DeepEqual(requestBody(t, transport.requests[1]), map[string]any{"delta": float64(2)}) {
		t.Fatalf("unexpected increment body: %#v", requestBody(t, transport.requests[1]))
	}
	if transport.requests[2].Method != "DELETE" {
		t.Fatalf("unexpected delete method: %s", transport.requests[2].Method)
	}
}

func TestStreamGroupRoutes(t *testing.T) {
	transport := &recordingTransport{}
	client := testClient(t, transport)
	ctx := context.Background()

	_, err := client.CommitStreamOffset(ctx, "orders", "billing", 2, 7, false)
	if err != nil {
		t.Fatalf("CommitStreamOffset returned an error: %v", err)
	}
	_, err = client.StreamLag(ctx, "orders", "billing", 2)
	if err != nil {
		t.Fatalf("StreamLag returned an error: %v", err)
	}

	if transport.requests[0].Path != "/v1/streams/orders/groups/billing/offsets" {
		t.Fatalf("unexpected commit route: %s", transport.requests[0].Path)
	}
	if transport.requests[1].Query.Get("partition") != "2" {
		t.Fatalf("partition query is missing: %v", transport.requests[1].Query)
	}
}

func TestQueueLifecycleRoutes(t *testing.T) {
	transport := &recordingTransport{}
	client := testClient(t, transport)
	ctx := context.Background()

	_, _ = client.QueueCounts(ctx, "jobs")
	_, _ = client.ExtendLease(ctx, "jobs", "lease-1", 5_000)
	_, _ = client.Reject(ctx, "jobs", "lease-2", "invalid")
	_, _ = client.Redrive(ctx, "jobs", "message-1")

	if transport.requests[0].Path != "/v1/queues/jobs/counts" {
		t.Fatalf("unexpected counts route: %s", transport.requests[0].Path)
	}
	if requestBody(t, transport.requests[1])["action"] != "extend" {
		t.Fatal("extend action was not encoded")
	}
	if requestBody(t, transport.requests[2])["reason"] != "invalid" {
		t.Fatal("reject reason was not encoded")
	}
	if transport.requests[3].Path != "/v1/queues/jobs/dead-letters/message-1/redrive" {
		t.Fatalf("unexpected redrive route: %s", transport.requests[3].Path)
	}
}

func TestBusSubscriptionAndReplayUseTypedModels(t *testing.T) {
	transport := &recordingTransport{}
	client := testClient(t, transport)
	ctx := context.Background()
	subscription := Subscription{
		Name:      "priority-orders",
		Target:    QueueTarget("jobs"),
		Filter:    EventFilter{EventTypePatterns: []string{"order.*"}},
		Transform: EventTransform{AddHeaders: map[string]string{"routed-by": "epoch"}},
	}

	_, err := client.UpsertSubscription(ctx, "events", subscription)
	if err != nil {
		t.Fatalf("UpsertSubscription returned an error: %v", err)
	}
	_, err = client.ReplayBus(ctx, "events", BusReplayOptions{
		FromMS:    100,
		ToMS:      200,
		Limit:     100,
		EventType: "order.*",
	})
	if err != nil {
		t.Fatalf("ReplayBus returned an error: %v", err)
	}
	_, err = client.RemoveSubscription(ctx, "events", "priority-orders")
	if err != nil {
		t.Fatalf("RemoveSubscription returned an error: %v", err)
	}

	body := requestBody(t, transport.requests[0])
	target := body["target"].(map[string]any)
	if target["kind"] != "queue" || target["resource"] != "jobs" {
		t.Fatalf("unexpected target: %#v", target)
	}
	if transport.requests[1].Query.Get("event_type") != "order.*" {
		t.Fatalf("unexpected replay query: %v", transport.requests[1].Query)
	}
	if transport.requests[2].Method != "DELETE" {
		t.Fatalf("unexpected subscription removal: %#v", transport.requests[2])
	}
}

func TestInvalidEventFailsBeforeTransport(t *testing.T) {
	transport := &recordingTransport{}
	client := testClient(t, transport)
	event := NewEventEnvelope("", "order.created", Document{})

	_, err := client.Publish(context.Background(), "events", event)
	if err == nil {
		t.Fatal("Publish accepted an invalid event")
	}
	if len(transport.requests) != 0 {
		t.Fatalf("invalid event reached transport: %#v", transport.requests)
	}
}

func TestRemainingNativeRoutes(t *testing.T) {
	transport := &recordingTransport{}
	client := testClient(t, transport)
	ctx := context.Background()
	event := NewEventEnvelope("test", "test.created", Document{"ok": true})

	_, _ = client.Health(ctx)
	_, _ = client.Resources(ctx)
	_, _ = client.CreateCache(ctx, "sessions", DefaultCacheConfig())
	_, _ = client.CreateQueue(ctx, "jobs", DefaultQueueConfig())
	_, _ = client.CreateBus(ctx, "events", true)
	_, _ = client.CacheGet(ctx, "sessions", "user-42")
	_, _ = client.FetchStream(ctx, "orders", 0, 0, 100)
	_, _ = client.Send(ctx, "jobs", event)
	_, _ = client.Receive(ctx, "jobs", QueueReceiveOptions{Consumer: "worker-1", MaxMessages: 1})
	_, _ = client.Acknowledge(ctx, "jobs", "lease-1")
	_, _ = client.Release(ctx, "jobs", "lease-2", 50, "retry")
	_, _ = client.Publish(ctx, "events", event)

	want := []struct {
		method string
		path   string
	}{
		{"GET", "/healthz"},
		{"GET", "/v1/resources"},
		{"POST", "/v1/caches/sessions"},
		{"POST", "/v1/queues/jobs"},
		{"POST", "/v1/buses/events"},
		{"GET", "/v1/caches/sessions/keys/user-42"},
		{"GET", "/v1/streams/orders/records"},
		{"POST", "/v1/queues/jobs/messages"},
		{"POST", "/v1/queues/jobs/acquire"},
		{"POST", "/v1/queues/jobs/settle"},
		{"POST", "/v1/queues/jobs/settle"},
		{"POST", "/v1/buses/events/events"},
	}
	if len(transport.requests) != len(want) {
		t.Fatalf("got %d requests, want %d", len(transport.requests), len(want))
	}
	for index, expected := range want {
		request := transport.requests[index]
		if request.Method != expected.method || request.Path != expected.path {
			t.Errorf("request %d = %s %s, want %s %s", index, request.Method, request.Path, expected.method, expected.path)
		}
	}
	if requestBody(t, transport.requests[2])["durability"] != "volatile" {
		t.Fatal("cache did not declare volatile durability")
	}
	if requestBody(t, transport.requests[3])["max_messages"] != float64(100_000) {
		t.Fatal("queue defaults were not encoded")
	}
	if transport.requests[6].Query.Get("limit") != "100" {
		t.Fatalf("fetch options were not encoded: %v", transport.requests[6].Query)
	}
	if requestBody(t, transport.requests[9])["action"] != "ack" {
		t.Fatal("ack settlement was not encoded")
	}
	if requestBody(t, transport.requests[10])["delay_ms"] != float64(50) {
		t.Fatal("release delay was not encoded")
	}
}

func TestClientRejectsInvalidOptionsBeforeTransport(t *testing.T) {
	transport := &recordingTransport{}
	client := testClient(t, transport)
	ctx := context.Background()

	checks := []func() error{
		func() error {
			_, err := client.CreateStream(ctx, "orders", StreamConfig{Durability: "invented"})
			return err
		},
		func() error {
			_, err := client.FetchStream(ctx, "orders", 0, 0, 0)
			return err
		},
		func() error {
			_, err := client.Receive(ctx, "jobs", QueueReceiveOptions{Consumer: "", MaxMessages: 1})
			return err
		},
		func() error {
			_, err := client.ReplayBus(ctx, "events", BusReplayOptions{FromMS: 2, ToMS: 1, Limit: 1})
			return err
		},
		func() error {
			_, err := client.CreateCache(ctx, "sessions", CacheConfig{DefaultTTLMS: Uint64(0)})
			return err
		},
		func() error {
			_, err := client.CreateStream(ctx, "orders", StreamConfig{MaxRecordsPerPartition: Uint64(0)})
			return err
		},
		func() error {
			_, err := client.CacheSet(ctx, "sessions", "key", "value", CacheWriteOptions{TTLMS: Uint64(0)})
			return err
		},
		func() error {
			_, err := client.CacheSet(ctx, "sessions", "key", "value", CacheWriteOptions{OnlyIfAbsent: true, OnlyIfPresent: true})
			return err
		},
	}
	for index, check := range checks {
		if err := check(); err == nil {
			t.Errorf("invalid option check %d returned no error", index)
		}
	}
	if len(transport.requests) != 0 {
		t.Fatalf("invalid options reached transport: %#v", transport.requests)
	}
}

func lastRequest(t *testing.T, transport *recordingTransport) Request {
	t.Helper()
	if len(transport.requests) == 0 {
		t.Fatal("transport received no request")
	}
	return transport.requests[len(transport.requests)-1]
}

func testClient(t *testing.T, transport Transport) *Client {
	t.Helper()
	client, err := NewClientWithTransport(transport)
	if err != nil {
		t.Fatalf("NewClientWithTransport returned an error: %v", err)
	}
	return client
}

func requestBody(t *testing.T, request Request) map[string]any {
	t.Helper()
	payload, err := json.Marshal(request.Body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	return body
}
