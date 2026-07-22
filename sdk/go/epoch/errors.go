package epoch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// APIError is returned for transport failures and non-success HTTP responses.
type APIError struct {
	StatusCode int
	Code       string
	Detail     string
	Body       json.RawMessage
	cause      error
}

func (failure *APIError) Error() string {
	if failure.StatusCode == 0 {
		return fmt.Sprintf("epoch: %s: %s", failure.Code, failure.Detail)
	}
	return fmt.Sprintf("epoch: HTTP %d %s: %s", failure.StatusCode, failure.Code, failure.Detail)
}

// Unwrap exposes the underlying networking error, when one exists.
func (failure *APIError) Unwrap() error {
	return failure.cause
}

// Retryable reports whether retrying may be safe at the transport level.
// Callers must still honor the operation's idempotency contract.
func (failure *APIError) Retryable() bool {
	if errors.Is(failure.cause, context.Canceled) {
		return false
	}
	if failure.StatusCode == 0 || failure.StatusCode == 408 || failure.StatusCode == 425 || failure.StatusCode == 429 || failure.StatusCode >= 500 {
		return true
	}
	switch failure.Code {
	case "deadline_exceeded", "overloaded", "rate_limited", "transport_error", "unavailable":
		return true
	default:
		return false
	}
}
