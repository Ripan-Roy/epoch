package epoch

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// WebhookVerification is the authenticated replay identity of one Epoch
// webhook request. Persist DeliveryID and Attempt before applying side effects.
type WebhookVerification struct {
	DeliveryID string
	Attempt    uint32
	SignedAt   time.Time
}

// VerifyWebhookSignature verifies the exact request body, delivery identity,
// attempt, and timestamp produced by Epoch's signed webhook executor.
func VerifyWebhookSignature(
	secret, body []byte,
	deliveryID, attemptHeader, timestampHeader, signatureHeader string,
	now time.Time,
	tolerance time.Duration,
) (WebhookVerification, error) {
	if len(secret) == 0 {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook secret is required")
	}
	if strings.TrimSpace(deliveryID) == "" {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook delivery ID is required")
	}
	attemptValue, err := strconv.ParseUint(attemptHeader, 10, 32)
	if err != nil || attemptValue == 0 {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook attempt must be a positive uint32")
	}
	if strconv.FormatUint(attemptValue, 10) != attemptHeader {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook attempt must use canonical decimal encoding")
	}
	timestamp, err := strconv.ParseInt(timestampHeader, 10, 64)
	if err != nil || timestamp < 0 {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook timestamp must be a non-negative integer")
	}
	if strconv.FormatInt(timestamp, 10) != timestampHeader {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook timestamp must use canonical decimal encoding")
	}
	if tolerance <= 0 {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook timestamp tolerance must be positive")
	}
	signedAt := time.Unix(timestamp, 0).UTC()
	if delta := now.UTC().Sub(signedAt); delta < -tolerance || delta > tolerance {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook timestamp is outside the allowed tolerance")
	}
	if len(signatureHeader) != len("v1=")+sha256.Size*2 || !strings.HasPrefix(signatureHeader, "v1=") {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook signature must use v1 lowercase hexadecimal")
	}
	encodedSignature := strings.TrimPrefix(signatureHeader, "v1=")
	if encodedSignature != strings.ToLower(encodedSignature) {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook signature must use v1 lowercase hexadecimal")
	}
	provided, err := hex.DecodeString(encodedSignature)
	if err != nil {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook signature must use v1 lowercase hexadecimal")
	}
	attempt := uint32(attemptValue)
	expected := webhookMAC(secret, timestamp, deliveryID, attempt, body)
	if subtle.ConstantTimeCompare(provided, expected) != 1 {
		return WebhookVerification{}, fmt.Errorf("epoch: webhook signature is invalid")
	}
	return WebhookVerification{DeliveryID: deliveryID, Attempt: attempt, SignedAt: signedAt}, nil
}

func webhookMAC(secret []byte, timestamp int64, deliveryID string, attempt uint32, body []byte) []byte {
	bodyDigest := sha256.Sum256(body)
	canonical := fmt.Sprintf(
		"v1\n%d\n%s\n%d\n%x",
		timestamp,
		deliveryID,
		attempt,
		bodyDigest,
	)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return mac.Sum(nil)
}
