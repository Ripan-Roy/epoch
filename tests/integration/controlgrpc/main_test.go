package main

import (
	"testing"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/protobuf/proto"
)

func TestOperationComparisonPreservesDeletePreconditionPresence(t *testing.T) {
	zero := uint64(0)
	want := &epochv1.GetOperationResponse{
		RequestToken: "delete-token", ProposalId: 7,
		State:       epochv1.OperationState_OPERATION_STATE_SUCCEEDED,
		CommandKind: "delete_managed", AffectedResources: []*epochv1.ResourceName{testName()},
	}
	if err := compareOperation(want, proto.Clone(want).(*epochv1.GetOperationResponse)); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*epochv1.GetOperationResponse){
		func(value *epochv1.GetOperationResponse) { value.ExpectedGeneration = &zero },
		func(value *epochv1.GetOperationResponse) { value.CommandKind = "delete_desired" },
		func(value *epochv1.GetOperationResponse) { value.AffectedResources[0].Organization = "otherco" },
		func(value *epochv1.GetOperationResponse) { value.ProposalId++ },
		func(value *epochv1.GetOperationResponse) { value.LastChangeCursor++ },
		func(value *epochv1.GetOperationResponse) {
			value.State = epochv1.OperationState_OPERATION_STATE_PENDING
		},
	} {
		got := proto.Clone(want).(*epochv1.GetOperationResponse)
		mutate(got)
		if err := compareOperation(want, got); err == nil {
			t.Fatalf("accepted altered durable operation: %v", got)
		}
	}
	if err := compareOperation(nil, nil); err == nil {
		t.Fatal("accepted missing operation")
	}
}

func TestWatchCheckpointUsesScannedCursorAndRejectsTenantDisclosure(t *testing.T) {
	page := &epochv1.WatchResourceChangesResponse{EarliestCursor: 1, LatestCursor: 8, NextCursor: 4}
	next, err := observeWatch(2, page, testName())
	if err != nil || next != 4 {
		t.Fatalf("empty filtered page = %d, %v", next, err)
	}
	page.Changes = []*epochv1.ResourceChange{{Cursor: 3, Name: testName(), Generation: 1}}
	if _, err := observeWatch(2, page, testName()); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*epochv1.WatchResourceChangesResponse){
		func(value *epochv1.WatchResourceChangesResponse) { value.NextCursor = 1 },
		func(value *epochv1.WatchResourceChangesResponse) { value.NextCursor = 9 },
		func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0].Cursor = 2 },
		func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0].Cursor = 5 },
		func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0].Name.Organization = "otherco" },
		func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0].Name.Project = "other-project" },
		func(value *epochv1.WatchResourceChangesResponse) {
			value.Changes = append(value.Changes, value.Changes[0])
		},
	} {
		got := proto.Clone(page).(*epochv1.WatchResourceChangesResponse)
		mutate(got)
		if _, err := observeWatch(2, got, testName()); err == nil {
			t.Fatalf("accepted malformed or unauthorized page: %v", got)
		}
	}
}

func TestDesiredWitnessIgnoresOnlyObservedStatus(t *testing.T) {
	want := &epochv1.Resource{Name: testName(), Generation: 3, Spec: cacheSpec("witness")}
	want.Status = &epochv1.ResourceStatus{ObservedGeneration: 3}
	got, err := desiredWitness(want)
	if err != nil || got.Status != nil || got.Generation != 3 || !proto.Equal(got.Spec, want.Spec) {
		t.Fatalf("desired witness = %v, %v", got, err)
	}
	if want.Status == nil {
		t.Fatal("witness modified the response")
	}
	for _, broken := range []*epochv1.Resource{nil, {}, {Name: testName(), Generation: 1}, {Spec: cacheSpec("missing-name"), Generation: 1}} {
		if _, err := desiredWitness(broken); err == nil {
			t.Fatalf("accepted incomplete desired state: %v", broken)
		}
	}
}

func testName() *epochv1.ResourceName {
	return &epochv1.ResourceName{Organization: "acme", Project: "payments", Environment: "production", Namespace: "orders", Kind: epochv1.ResourceKind_RESOURCE_KIND_CACHE, Name: "fixture"}
}
