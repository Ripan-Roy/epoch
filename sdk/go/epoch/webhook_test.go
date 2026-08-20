package epoch

import (
	"testing"
	"time"
)

func TestVerifyWebhookSignaturePinsCrossLanguageVectorAndReplayIdentity(t *testing.T) {
	t.Parallel()
	const (
		deliveryID = "epoch.bus.delivery.v1.1.orders"
		timestamp  = "1700000000"
		signature  = "v1=866b035f5c00f59cc64a7caea8a4d16be04dd41966774cdfc336e7cf341d18d9"
	)
	verified, err := VerifyWebhookSignature(
		[]byte("0123456789abcdef0123456789abcdef"),
		[]byte(`{"order_id":"one"}`),
		deliveryID,
		"2",
		timestamp,
		signature,
		time.Unix(1_700_000_010, 0),
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("verify webhook: %v", err)
	}
	if verified.DeliveryID != deliveryID || verified.Attempt != 2 {
		t.Fatalf("unexpected replay identity: %#v", verified)
	}

	if _, err = VerifyWebhookSignature(
		[]byte("0123456789abcdef0123456789abcdef"),
		[]byte(`{"order_id":"changed"}`),
		deliveryID,
		"2",
		timestamp,
		signature,
		time.Unix(1_700_000_010, 0),
		30*time.Second,
	); err == nil {
		t.Fatal("changed body unexpectedly verified")
	}
	if _, err = VerifyWebhookSignature(
		[]byte("0123456789abcdef0123456789abcdef"),
		[]byte(`{"order_id":"one"}`),
		deliveryID,
		"2",
		timestamp,
		signature,
		time.Unix(1_700_000_031, 0),
		30*time.Second,
	); err == nil {
		t.Fatal("stale signature unexpectedly verified")
	}
}

func TestVerifyWebhookSignatureRejectsNonCanonicalReplayHeaders(t *testing.T) {
	t.Parallel()
	arguments := func(attempt, timestamp string) error {
		_, err := VerifyWebhookSignature(
			[]byte("0123456789abcdef0123456789abcdef"),
			[]byte(`{"order_id":"one"}`),
			"epoch.bus.delivery.v1.1.orders",
			attempt,
			timestamp,
			"v1=866b035f5c00f59cc64a7caea8a4d16be04dd41966774cdfc336e7cf341d18d9",
			time.Unix(1_700_000_010, 0),
			30*time.Second,
		)
		return err
	}
	if err := arguments("02", "1700000000"); err == nil {
		t.Fatal("zero-padded attempt unexpectedly verified")
	}
	if err := arguments("2", "01700000000"); err == nil {
		t.Fatal("zero-padded timestamp unexpectedly verified")
	}
}
