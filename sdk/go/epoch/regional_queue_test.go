package epoch

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestRegionalQueueClientRoutesCompleteMutationAndReadContracts(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6",
		"tablet_epoch":        "4",
		"term":                "11",
		"accepts_writes":      true,
	}}
	client, err := NewRegionalQueueClientWithTransports(
		[]Transport{leader},
		"secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	event := NewEventEnvelope("checkout", "job.created", map[string]any{"id": "42"})
	event.ID = "job-42"
	event.TimeMS = 42

	if _, err := client.Enqueue(context.Background(), "jobs/eu", 0, "enqueue-42", event); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Acquire(context.Background(), "jobs/eu", 0, "acquire-42", RegionalQueueAcquireOptions{
		Consumer: "worker-a", ConsumerEpoch: 7, MaxMessages: 4, MaxInFlight: pointer(uint16(2)), VisibilityTimeoutMS: pointer(uint64(5_000)),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Counts(context.Background(), "jobs/eu", 0); err != nil {
		t.Fatal(err)
	}

	base := "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/queues/jobs%2Feu/shards/0"
	if len(leader.requests) != 6 {
		t.Fatalf("expected discovery/operation for three calls, got %d", len(leader.requests))
	}
	for _, index := range []int{0, 2, 4} {
		if leader.requests[index].Path != base {
			t.Fatalf("unexpected discovery path %q", leader.requests[index].Path)
		}
	}
	for _, index := range []int{1, 3} {
		request := leader.requests[index]
		if request.Path != base+"/mutations" || request.Method != "POST" {
			t.Fatalf("unexpected mutation request: %#v", request)
		}
		if request.Headers["authorization"] != "Bearer secret-token" || request.Headers["x-epoch-resource-generation"] != "6" || request.Headers["x-epoch-tablet-epoch"] != "4" {
			t.Fatalf("mutation did not carry auth and route fences: %#v", request.Headers)
		}
	}
	var acquire map[string]any
	payload, marshalErr := json.Marshal(leader.requests[3].Body)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if unmarshalErr := json.Unmarshal(payload, &acquire); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	operation := acquire["operation"].(map[string]any)
	if acquire["idempotency_key"] != "acquire-42" || acquire["expected_term"] != "11" || operation["kind"] != "acquire" || operation["consumer_epoch"] != "7" {
		t.Fatalf("unexpected acquire contract: %#v", acquire)
	}
	read := leader.requests[5]
	if read.Path != base+"/counts" || read.Headers["x-epoch-read-consistency"] != "linearizable" {
		t.Fatalf("unexpected counts contract: %#v", read)
	}
}

func TestRegionalQueueClientCoversEveryLifecycleOperation(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalQueueClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	queue := "jobs"
	if _, err = client.RenewSessionLock(ctx, queue, 0, "renew-session", "worker", 7, "session", 1_000); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ReleaseSessionLock(ctx, queue, 0, "release-session", "worker", 7, "session"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Defer(ctx, queue, 0, "defer", "worker", 7, "lease", "dependency"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ReceiveDeferred(ctx, queue, 0, "receive-deferred", "message", "worker", 7, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Acknowledge(ctx, queue, 0, "ack", "worker", 7, "lease"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ExtendLease(ctx, queue, 0, "extend", "worker", 7, "lease", 1_000); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Release(ctx, queue, 0, "release", "worker", 7, "lease", 50, "retry"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Nack(ctx, queue, 0, "nack", "worker", 7, "lease", "retry"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Reject(ctx, queue, 0, "reject", "worker", 7, "lease", "invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Redrive(ctx, queue, 0, "redrive", "message", 9); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Maintain(ctx, queue, 0, "maintain"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Mutation(ctx, queue, 0, 12); err != nil {
		t.Fatal(err)
	}
	if _, err = client.DeadLetters(ctx, queue, 0, 25); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Redrives(ctx, queue, 0, 25); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ConsumerFlow(ctx, queue, 0, "worker/a"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.AdvancedStatus(ctx, queue, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Correlation(ctx, queue, 0, "correlation/a"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.DeadLetterForwards(ctx, queue, 0, 25); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Status(ctx, queue, 0); err != nil {
		t.Fatal(err)
	}

	var paths []string
	for index := 1; index < len(leader.requests); index += 2 {
		paths = append(paths, leader.requests[index].Path)
	}
	base := "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/queues/jobs/shards/0"
	want := []string{
		base + "/mutations", base + "/mutations", base + "/mutations", base + "/mutations",
		base + "/mutations", base + "/mutations", base + "/mutations", base + "/mutations",
		base + "/mutations", base + "/mutations", base + "/mutations", base + "/mutations/12",
		base + "/dead-letters", base + "/redrives", base + "/consumers/worker%2Fa/flow",
		base + "/advanced", base + "/correlations/correlation%2Fa", base + "/dead-letter-forwards", base + "/status",
	}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("unexpected lifecycle paths:\n got %#v\nwant %#v", paths, want)
	}
	for index := 23; index < len(leader.requests); index += 2 {
		if leader.requests[index].Headers[regionalReadHeader] != "linearizable" {
			t.Fatalf("read %q was not linearizable", leader.requests[index].Path)
		}
	}
}

func TestRegionalQueueClientRediscoversWithoutChangingMutationIdentity(t *testing.T) {
	leader := &regionalFakeTransport{
		route: Document{
			"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
		},
		operationErrors: []error{&APIError{StatusCode: 409, Code: "not_leader", Detail: "changed"}},
	}
	client, err := NewRegionalQueueClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Maintain(context.Background(), "jobs", 0, "maintain-stable"); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{1, 3} {
		payload, err := json.Marshal(leader.requests[index].Body)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err := json.Unmarshal(payload, &body); err != nil {
			t.Fatal(err)
		}
		if body["idempotency_key"] != "maintain-stable" {
			t.Fatalf("retry changed mutation identity: %#v", body)
		}
	}
}

func pointer[T any](value T) *T {
	return &value
}
