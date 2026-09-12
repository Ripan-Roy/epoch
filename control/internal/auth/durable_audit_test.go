package auth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDurableAuditJournalVerifiesCrossLanguageGoldenRecord(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "..", "spec", "auth", "audit-journal-v1.example.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "audit.ndjson")
	if err := os.WriteFile(path, fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err := OpenDurableAuditJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	page, err := journal.ReadAuditPage(context.Background(), 0, 10, auditTestPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].Sequence != "1" ||
		page.Records[0].RecordSHA256 != "c10b0019b9aa4f883aed2c004c00a525fe2bd0191c9364b4a4695ab99132459e" ||
		page.Records[0].Event.AuthenticationMethod != AuthenticationOIDCEdDSA {
		t.Fatalf("golden audit page = %#v", page)
	}
}

func TestDurableAuditJournalAppendsReopensPagesAndVerifiesChain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "audit.ndjson")
	journal, err := OpenDurableAuditJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		event := testDecisionEvent(index + 1)
		if err := journal.Record(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	page, err := journal.ReadAuditPage(context.Background(), 0, 2, auditTestPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || page.NextSequence != "2" || page.EndOfJournal {
		t.Fatalf("first page = %#v", page)
	}
	if page.Records[0].Sequence != "1" ||
		page.Records[0].PreviousSHA256 != strings.Repeat("0", 64) ||
		page.Records[0].RecordSHA256 == strings.Repeat("0", 64) {
		t.Fatalf("first record = %#v", page.Records[0])
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDurableAuditJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	page, err = reopened.ReadAuditPage(context.Background(), 2, 2, auditTestPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.NextSequence != "3" || !page.EndOfJournal ||
		page.Records[0].Event.RequestID != "request-3" {
		t.Fatalf("second page = %#v", page)
	}
}

func TestDurableAuditJournalRejectsTamperingPartialTailAndWeakPermissions(t *testing.T) {
	original := filepath.Join(t.TempDir(), "audit.ndjson")
	journal, err := OpenDurableAuditJournal(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Record(context.Background(), testDecisionEvent(1)); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}

	tampered := filepath.Join(t.TempDir(), "tampered.ndjson")
	if err := os.WriteFile(tampered, []byte(strings.Replace(string(encoded), "request-1", "request-9", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableAuditJournal(tampered); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("tampered journal error = %v", err)
	}

	partial := filepath.Join(t.TempDir(), "partial.ndjson")
	if err := os.WriteFile(partial, encoded[:len(encoded)-1], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableAuditJournal(partial); err == nil || !strings.Contains(err.Error(), "partial") {
		t.Fatalf("partial journal error = %v", err)
	}

	weak := filepath.Join(t.TempDir(), "weak.ndjson")
	if err := os.WriteFile(weak, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(weak, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableAuditJournal(weak); err == nil || !strings.Contains(err.Error(), "permissions") {
		t.Fatalf("weak permission error = %v", err)
	}
}

func TestDurableAuditJournalRejectsInvalidEventsAndPageBounds(t *testing.T) {
	journal, err := OpenDurableAuditJournal(filepath.Join(t.TempDir(), "audit.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	invalid := testDecisionEvent(1)
	invalid.RequestID = ""
	if err := journal.Record(context.Background(), invalid); err == nil {
		t.Fatal("invalid event was accepted")
	}
	for _, limit := range []int{0, maxAuditPageSize + 1} {
		if _, err := journal.ReadAuditPage(context.Background(), 0, limit, auditTestPrincipal()); err == nil {
			t.Fatalf("page limit %d was accepted", limit)
		}
	}
}

func TestDurableAuditJournalReturnsErrorsAfterClose(t *testing.T) {
	journal, err := OpenDurableAuditJournal(filepath.Join(t.TempDir(), "audit.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Record(context.Background(), testDecisionEvent(1)); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("record after close error = %v", err)
	}
	if _, err := journal.ReadAuditPage(context.Background(), 0, 10, auditTestPrincipal()); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("read after close error = %v", err)
	}
}

func TestDurableAuditJournalBecomesStickyFailedAfterLiveTampering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.ndjson")
	journal, err := OpenDurableAuditJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	if err := journal.Record(context.Background(), testDecisionEvent(1)); err != nil {
		t.Fatal(err)
	}
	tampered, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tampered = []byte(strings.Replace(string(tampered), "request-1", "request-9", 1))
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.ReadAuditPage(context.Background(), 0, 10, auditTestPrincipal()); err == nil {
		t.Fatal("live tampering was not detected")
	}
	if err := journal.Record(context.Background(), testDecisionEvent(2)); err == nil {
		t.Fatal("sticky-failed journal accepted another event")
	}
}

func auditTestPrincipal() Principal {
	return Principal{
		id: "auditor", policyID: "identity-v2",
		actions:              []Action{ActionAuditRead},
		actionSet:            map[Action]struct{}{ActionAuditRead: {}},
		scope:                Scope{Organization: "*", Project: "*", Environment: "*", Namespace: "*"},
		authenticationMethod: AuthenticationOIDCEdDSA,
	}
}

func testDecisionEvent(sequence int) DecisionEvent {
	return DecisionEvent{
		Timestamp:            time.Date(2026, 9, 12, 1, 2, sequence, 123, time.UTC),
		RequestID:            "request-" + string(rune('0'+sequence)),
		PrincipalID:          "oidc:principal",
		PolicyID:             "identity-v2",
		AuthenticationMethod: AuthenticationOIDCEdDSA,
		Action:               ActionResourceRead,
		Decision:             DecisionAllow,
		Reason:               ReasonPolicyGrant,
		Scope: Scope{
			Organization: "acme",
			Project:      "payments",
			Environment:  "production",
			Namespace:    "orders",
		},
	}
}
