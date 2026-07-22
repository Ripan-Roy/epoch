package epoch

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestStandaloneSmoke(t *testing.T) {
	endpoint := os.Getenv("EPOCH_GO_INTEGRATION_URL")
	if endpoint == "" {
		t.Skip("EPOCH_GO_INTEGRATION_URL is not set")
	}
	client, err := NewClient(endpoint, 10*time.Second)
	if err != nil {
		t.Fatalf("NewClient returned an error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	health, err := client.Health(ctx)
	if err != nil {
		t.Fatalf("Health returned an error: %v", err)
	}
	if health["status"] != "ok" {
		t.Fatalf("node is not healthy: %#v", health)
	}
	_, err = client.CreateStream(ctx, "go-sdk-smoke", DefaultStreamConfig())
	if err != nil {
		t.Fatalf("CreateStream returned an error: %v", err)
	}
	event := NewEventEnvelope("go-sdk", "smoke.created", Document{"language": "go"})
	event.ID = "go-sdk-1"
	_, err = client.AppendStream(ctx, "go-sdk-smoke", event, nil)
	if err != nil {
		t.Fatalf("AppendStream returned an error: %v", err)
	}
	records, err := client.FetchStream(ctx, "go-sdk-smoke", 0, 0, 10)
	if err != nil {
		t.Fatalf("FetchStream returned an error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	envelope, ok := records[0]["envelope"].(map[string]any)
	if !ok || envelope["id"] != "go-sdk-1" {
		t.Fatalf("unexpected record: %#v", records[0])
	}
}
