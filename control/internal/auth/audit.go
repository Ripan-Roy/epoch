package auth

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const maxAuditFieldBytes = 256

// Decision is the stable authorization outcome.
type Decision string

const (
	DecisionAllow Decision = "allow"
	DecisionDeny  Decision = "deny"
)

// DecisionReason explains an authorization outcome without revealing policy
// internals or credential material.
type DecisionReason string

const (
	ReasonPolicyGrant         DecisionReason = "policy_grant"
	ReasonActionNotGranted    DecisionReason = "action_not_granted"
	ReasonScopeMismatch       DecisionReason = "scope_mismatch"
	ReasonMissingCredential   DecisionReason = "missing_credential"
	ReasonMalformedCredential DecisionReason = "malformed_credential"
	ReasonInvalidCredential   DecisionReason = "invalid_credential"
)

var validReasons = map[DecisionReason]struct{}{
	ReasonPolicyGrant:         {},
	ReasonActionNotGranted:    {},
	ReasonScopeMismatch:       {},
	ReasonMissingCredential:   {},
	ReasonMalformedCredential: {},
	ReasonInvalidCredential:   {},
}

// DecisionEvent is a bounded credential-free audit record.
type DecisionEvent struct {
	Timestamp   time.Time
	RequestID   string
	PrincipalID string
	PolicyID    string
	Action      Action
	Decision    Decision
	Reason      DecisionReason
	Scope       Scope
}

// Validate rejects incomplete or unbounded audit records before emission.
func (event DecisionEvent) Validate() error {
	if event.Timestamp.IsZero() {
		return fmt.Errorf("audit timestamp is required")
	}
	for name, value := range map[string]string{
		"request_id":   event.RequestID,
		"principal_id": event.PrincipalID,
		"policy_id":    event.PolicyID,
	} {
		if value == "" || len(value) > maxAuditFieldBytes {
			return fmt.Errorf("audit %s must contain between 1 and %d bytes", name, maxAuditFieldBytes)
		}
	}
	if _, valid := validActions[event.Action]; !valid {
		return fmt.Errorf("audit action is invalid")
	}
	if event.Decision != DecisionAllow && event.Decision != DecisionDeny {
		return fmt.Errorf("audit decision is invalid")
	}
	if _, valid := validReasons[event.Reason]; !valid {
		return fmt.Errorf("audit reason is invalid")
	}
	for name, value := range map[string]string{
		"organization": event.Scope.Organization,
		"project":      event.Scope.Project,
		"environment":  event.Scope.Environment,
		"namespace":    event.Scope.Namespace,
	} {
		if len(value) > 128 {
			return fmt.Errorf("audit %s scope exceeds 128 bytes", name)
		}
	}
	return nil
}

// AuditSink consumes authorization decisions. Implementations must not block
// request correctness on export availability.
type AuditSink interface {
	Record(context.Context, DecisionEvent)
}

// SlogAuditSink emits structured decision records through the process logger.
type SlogAuditSink struct {
	logger *slog.Logger
}

// NewSlogAuditSink constructs a structured audit sink.
func NewSlogAuditSink(logger *slog.Logger) *SlogAuditSink {
	if logger == nil {
		panic("auth: nil audit logger")
	}
	return &SlogAuditSink{logger: logger}
}

// Record emits a valid event and drops invalid events with a separate safe
// diagnostic rather than serializing attacker-controlled unbounded fields.
func (sink *SlogAuditSink) Record(ctx context.Context, event DecisionEvent) {
	if err := event.Validate(); err != nil {
		sink.logger.ErrorContext(ctx, "authorization audit event rejected", "error", err)
		return
	}
	sink.logger.InfoContext(
		ctx,
		"authorization decision",
		"event_type", "authorization.decision",
		"event_time", event.Timestamp.Format(time.RFC3339Nano),
		"request_id", event.RequestID,
		"principal_id", event.PrincipalID,
		"policy_id", event.PolicyID,
		"action", event.Action,
		"decision", event.Decision,
		"reason", event.Reason,
		"organization", event.Scope.Organization,
		"project", event.Scope.Project,
		"environment", event.Scope.Environment,
		"namespace", event.Scope.Namespace,
	)
}

// MemoryAuditSink captures events for deterministic tests.
type MemoryAuditSink struct {
	mutex  sync.Mutex
	events []DecisionEvent
}

// NewMemoryAuditSink constructs an empty in-memory sink.
func NewMemoryAuditSink() *MemoryAuditSink {
	return &MemoryAuditSink{}
}

// Record appends one defensive event copy.
func (sink *MemoryAuditSink) Record(_ context.Context, event DecisionEvent) {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	sink.events = append(sink.events, event)
}

// Events returns a defensive snapshot in emission order.
func (sink *MemoryAuditSink) Events() []DecisionEvent {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	return append([]DecisionEvent(nil), sink.events...)
}
