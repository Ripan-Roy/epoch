package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
)

func main() {
	client, err := epoch.NewRegionalQueueClient(
		strings.Split(environment("EPOCH_REGIONAL_ENDPOINTS", "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663"), ","),
		environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
		epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
		3*time.Second,
	)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	event := epoch.NewEventEnvelope("docs-go", "job.created", map[string]any{"job_id": "go-42"})
	event.ID, event.TimeMS = "docs-go-job-42", 42

	enqueued, err := client.Enqueue(ctx, "jobs", 0, "docs-go-enqueue-v1", event)
	must(err)
	replayed, err := client.Enqueue(ctx, "jobs", 0, "docs-go-enqueue-v1", event)
	must(err)
	window, timeout := uint16(1), uint64(5_000)
	acquired, err := client.Acquire(ctx, "jobs", 0, "docs-go-acquire-v1", epoch.RegionalQueueAcquireOptions{
		Consumer: "docs-go", ConsumerEpoch: 1, MaxMessages: 1,
		MaxInFlight: &window, VisibilityTimeoutMS: &timeout,
	})
	must(err)
	delivery := result(acquired)["deliveries"].([]any)[0].(map[string]any)
	extended, err := client.ExtendLease(ctx, "jobs", 0, "docs-go-extend-v1", "docs-go", 1, delivery["lease_token"].(string), 60_000)
	must(err)
	released, err := client.Release(ctx, "jobs", 0, "docs-go-release-v1", "docs-go", 1, result(extended)["lease_token"].(string), 0, "demonstrate retry")
	must(err)
	maintained, err := client.Maintain(ctx, "jobs", 0, "docs-go-maintain-v1")
	must(err)
	reacquired, err := client.Acquire(ctx, "jobs", 0, "docs-go-reacquire-v1", epoch.RegionalQueueAcquireOptions{Consumer: "docs-go", ConsumerEpoch: 1, MaxMessages: 1, MaxInFlight: &window})
	must(err)
	redelivery := result(reacquired)["deliveries"].([]any)[0].(map[string]any)
	rejected, err := client.Reject(ctx, "jobs", 0, "docs-go-reject-v1", "docs-go", 1, redelivery["lease_token"].(string), "poison")
	must(err)
	historyID, err := strconv.ParseUint(fmt.Sprint(result(rejected)["dead_letter_history_id"]), 10, 64)
	must(err)
	deadLetters, err := client.DeadLetters(ctx, "jobs", 0, 10)
	must(err)
	redriven, err := client.Redrive(ctx, "jobs", 0, "docs-go-redrive-v1", event.ID, historyID)
	must(err)
	finalAcquire, err := client.Acquire(ctx, "jobs", 0, "docs-go-final-acquire-v1", epoch.RegionalQueueAcquireOptions{Consumer: "docs-go", ConsumerEpoch: 1, MaxMessages: 1, MaxInFlight: &window})
	must(err)
	finalDelivery := result(finalAcquire)["deliveries"].([]any)[0].(map[string]any)
	acknowledged, err := client.Acknowledge(ctx, "jobs", 0, "docs-go-ack-v1", "docs-go", 1, finalDelivery["lease_token"].(string))
	must(err)

	sessionID, correlationID, replyTo := "account-go-7", "request-go-7", "reply-temporary"
	sessionEvent := epoch.NewEventEnvelope("docs-go", "session.job.created", map[string]any{"job_id": "go-session-42"})
	sessionEvent.ID, sessionEvent.TimeMS = "docs-go-session-42", 43
	sessionEnqueue, err := client.EnqueueAdvanced(ctx, "jobs", 0, "docs-go-session-enqueue-v1", sessionEvent, epoch.RegionalQueueEnqueueOptions{
		SessionID: sessionID, CorrelationID: correlationID, ReplyTo: replyTo,
	})
	must(err)
	correlated, err := client.Correlation(ctx, "jobs", 0, correlationID)
	must(err)
	sessionAcquire, err := client.Acquire(ctx, "jobs", 0, "docs-go-session-acquire-v1", epoch.RegionalQueueAcquireOptions{
		Consumer: "docs-go-session", ConsumerEpoch: 1, MaxMessages: 1,
		MaxInFlight: &window, VisibilityTimeoutMS: &timeout, SessionID: sessionID,
	})
	must(err)
	sessionResult := result(sessionAcquire)
	sessionRenew, err := client.RenewSessionLock(ctx, "jobs", 0, "docs-go-session-renew-v1", "docs-go-session", 1, sessionResult["session_lock_token"].(string), 30_000)
	must(err)
	sessionDelivery := sessionResult["deliveries"].([]any)[0].(map[string]any)
	_, err = client.Acknowledge(ctx, "jobs", 0, "docs-go-session-ack-v1", "docs-go-session", 1, sessionDelivery["lease_token"].(string))
	must(err)
	sessionRelease, err := client.ReleaseSessionLock(ctx, "jobs", 0, "docs-go-session-release-v1", "docs-go-session", 1, result(sessionRenew)["session_lock_token"].(string))
	must(err)

	deferredEvent := epoch.NewEventEnvelope("docs-go", "job.deferred", map[string]any{"job_id": "go-deferred-42"})
	deferredEvent.ID, deferredEvent.TimeMS = "docs-go-deferred-42", 44
	_, err = client.Enqueue(ctx, "jobs", 0, "docs-go-deferred-enqueue-v1", deferredEvent)
	must(err)
	deferredAcquire, err := client.Acquire(ctx, "jobs", 0, "docs-go-deferred-acquire-v1", epoch.RegionalQueueAcquireOptions{Consumer: "docs-go-deferred", ConsumerEpoch: 1, MaxMessages: 1, MaxInFlight: &window})
	must(err)
	deferredDelivery := result(deferredAcquire)["deliveries"].([]any)[0].(map[string]any)
	deferred, err := client.Defer(ctx, "jobs", 0, "docs-go-defer-v1", "docs-go-deferred", 1, deferredDelivery["lease_token"].(string), "await dependency")
	must(err)
	receivedDeferred, err := client.ReceiveDeferred(ctx, "jobs", 0, "docs-go-receive-deferred-v1", deferredEvent.ID, "docs-go-deferred", 1, &timeout)
	must(err)
	_, err = client.Acknowledge(ctx, "jobs", 0, "docs-go-deferred-ack-v1", "docs-go-deferred", 1, result(receivedDeferred)["delivery"].(map[string]any)["lease_token"].(string))
	must(err)
	advanced, err := client.AdvancedStatus(ctx, "jobs", 0)
	must(err)
	forwards, err := client.DeadLetterForwards(ctx, "jobs", 0, 10)
	must(err)
	counts, err := client.Counts(ctx, "jobs", 0)
	must(err)
	flow, err := client.ConsumerFlow(ctx, "jobs", 0, "docs-go")
	must(err)

	output, err := json.MarshalIndent(map[string]any{
		"enqueue": enqueued, "exact_retry": replayed, "release": released, "maintain": maintained,
		"dead_letters": deadLetters, "redrive": redriven, "ack": acknowledged, "counts": counts, "flow": flow,
		"session_enqueue": sessionEnqueue, "correlation": correlated, "session_release": sessionRelease,
		"defer": deferred, "receive_deferred": receivedDeferred, "advanced": advanced, "dead_letter_forwards": forwards,
	}, "", "  ")
	must(err)
	fmt.Println(string(output))
}

func result(document epoch.Document) map[string]any {
	receipt := document["receipt"].(map[string]any)
	outcome := receipt["outcome"].(map[string]any)
	return outcome["result"].(map[string]any)
}

func environment(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
