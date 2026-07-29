package auth

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestMemoryAuditSinkReturnsDefensiveCopiesInOrder(t *testing.T) {
	sink := NewMemoryAuditSink()
	first := DecisionEvent{
		Timestamp:   time.Unix(10, 0).UTC(),
		RequestID:   "request-1",
		PrincipalID: "reader",
		PolicyID:    "policy-1",
		Action:      ActionResourceRead,
		Decision:    DecisionAllow,
		Reason:      ReasonPolicyGrant,
		Scope:       Scope{Organization: "acme", Namespace: "orders"},
	}
	second := first
	second.RequestID = "request-2"
	second.Decision = DecisionDeny
	second.Reason = ReasonScopeMismatch
	sink.Record(context.Background(), first)
	sink.Record(context.Background(), second)

	events := sink.Events()
	if len(events) != 2 || events[0].RequestID != "request-1" || events[1].RequestID != "request-2" {
		t.Fatalf("events = %+v", events)
	}
	events[0].PrincipalID = "corrupted"
	if sink.Events()[0].PrincipalID != "reader" {
		t.Fatal("mutating Events() result changed the sink")
	}
}

func TestSlogAuditSinkEmitsBoundedDecisionWithoutCredentials(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	sink := NewSlogAuditSink(logger)
	event := DecisionEvent{
		Timestamp:   time.Unix(10, 0).UTC(),
		RequestID:   "request-1",
		PrincipalID: "development-reader",
		PolicyID:    "epoch-development-v1",
		Action:      ActionResourceRead,
		Decision:    DecisionAllow,
		Reason:      ReasonPolicyGrant,
		Scope: Scope{
			Organization: "acme",
			Project:      "payments",
			Environment:  "production",
			Namespace:    "orders",
		},
	}
	sink.Record(context.Background(), event)
	encoded := output.String()
	for expected := range map[string]struct{}{
		`"event_type":"authorization.decision"`: {},
		`"request_id":"request-1"`:              {},
		`"principal_id":"development-reader"`:   {},
		`"action":"resource.read"`:              {},
		`"decision":"allow"`:                    {},
		`"namespace":"orders"`:                  {},
	} {
		if !strings.Contains(encoded, expected) {
			t.Fatalf("audit output missing %s: %s", expected, encoded)
		}
	}
	for forbidden := range map[string]struct{}{
		readerToken:        {},
		`"authorization":`: {},
		`"token":`:         {},
	} {
		if strings.Contains(strings.ToLower(encoded), strings.ToLower(forbidden)) {
			t.Fatalf("audit output contains forbidden credential field %q: %s", forbidden, encoded)
		}
	}
}

func TestDecisionEventValidationRejectsUnboundedOrIncompleteFields(t *testing.T) {
	valid := DecisionEvent{
		Timestamp:   time.Unix(10, 0).UTC(),
		RequestID:   "request-1",
		PrincipalID: "reader",
		PolicyID:    "policy-1",
		Action:      ActionResourceRead,
		Decision:    DecisionAllow,
		Reason:      ReasonPolicyGrant,
		Scope:       Scope{Organization: "acme", Project: "*", Environment: "*", Namespace: "orders"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*DecisionEvent)
	}{
		{name: "missing request", mutate: func(event *DecisionEvent) { event.RequestID = "" }},
		{name: "unknown action", mutate: func(event *DecisionEvent) { event.Action = "root" }},
		{name: "unknown decision", mutate: func(event *DecisionEvent) { event.Decision = "maybe" }},
		{name: "unknown reason", mutate: func(event *DecisionEvent) { event.Reason = "because" }},
		{
			name: "oversized principal",
			mutate: func(event *DecisionEvent) {
				event.PrincipalID = strings.Repeat("a", maxAuditFieldBytes+1)
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := valid
			testCase.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("Validate() succeeded")
			}
		})
	}
}
