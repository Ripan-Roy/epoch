package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
)

const (
	streamName = "orders"
	queueName  = "jobs"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: go run quickstart.go [seed|verify]")
	}

	endpoint := os.Getenv("EPOCH_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:7601"
	}
	client, err := epoch.NewClient(endpoint, 10*time.Second)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	switch os.Args[1] {
	case "seed":
		seed(ctx, client)
	case "verify":
		verify(ctx, client)
	default:
		log.Fatal("mode must be seed or verify")
	}
}

func seed(ctx context.Context, client *epoch.Client) {
	streamConfig := epoch.DefaultStreamConfig()
	streamConfig.Durability = epoch.LocalDurable
	_, err := client.CreateStream(ctx, streamName, streamConfig)
	must(err)

	queueConfig := epoch.DefaultQueueConfig()
	queueConfig.Durability = epoch.LocalDurable
	_, err = client.CreateQueue(ctx, queueName, queueConfig)
	must(err)

	order := epoch.NewEventEnvelope(
		"quickstart", "order.created", epoch.Document{"order_id": "o-1001"},
	)
	order.ID = "order-o-1001"
	receipt, err := client.AppendStream(ctx, streamName, order, nil)
	must(err)
	acknowledgement, ok := receipt["acknowledgement"].(map[string]any)
	if !ok || acknowledgement["durability"] != string(epoch.LocalDurable) {
		log.Fatalf("unexpected durability receipt: %#v", receipt)
	}

	records, err := client.FetchStream(ctx, streamName, 0, 0, 10)
	must(err)
	fmt.Printf("stream records before restart: %d\n", len(records))

	for _, id := range []string{"job-1001", "job-1002"} {
		job := epoch.NewEventEnvelope(
			"quickstart", "job.requested", epoch.Document{"job_id": id},
		)
		job.ID = id
		_, err = client.Send(ctx, queueName, job)
		must(err)
	}

	deliveries, err := client.Receive(ctx, queueName, epoch.QueueReceiveOptions{
		Consumer: "worker-a", MaxMessages: 1,
	})
	must(err)
	if len(deliveries) != 1 {
		log.Fatalf("expected one delivery, got %d", len(deliveries))
	}
	message, ok := deliveries[0]["message"].(map[string]any)
	if !ok || message["id"] != "job-1001" {
		log.Fatalf("expected job-1001 first, got %#v", deliveries[0])
	}
	token, ok := deliveries[0]["lease_token"].(string)
	if !ok {
		log.Fatal("delivery did not include a lease_token")
	}
	_, err = client.Acknowledge(ctx, queueName, token)
	must(err)
	fmt.Println("acked one job; restart the node now")
}

func verify(ctx context.Context, client *epoch.Client) {
	records, err := client.FetchStream(ctx, streamName, 0, 0, 10)
	must(err)
	if len(records) != 1 {
		log.Fatalf("expected one recovered stream record, got %d", len(records))
	}
	envelope, ok := records[0]["envelope"].(map[string]any)
	if !ok || envelope["id"] != "order-o-1001" {
		log.Fatalf("unexpected recovered stream record: %#v", records[0])
	}

	counts, err := client.QueueCounts(ctx, queueName)
	must(err)
	if counts["acknowledged"] != float64(1) {
		log.Fatalf("expected one recovered acknowledgement, got %#v", counts)
	}

	deliveries, err := client.Receive(ctx, queueName, epoch.QueueReceiveOptions{
		Consumer: "worker-b", MaxMessages: 10,
	})
	must(err)
	if len(deliveries) != 1 {
		log.Fatalf("expected only the unacked job after restart, got %d", len(deliveries))
	}
	message, ok := deliveries[0]["message"].(map[string]any)
	if !ok || message["id"] != "job-1002" {
		log.Fatalf("expected job-1002 after restart, got %#v", deliveries[0])
	}
	token, ok := deliveries[0]["lease_token"].(string)
	if !ok {
		log.Fatal("delivery did not include a lease_token")
	}
	_, err = client.Acknowledge(ctx, queueName, token)
	must(err)
	fmt.Println("restart verified: stream record and queue settlement survived")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
