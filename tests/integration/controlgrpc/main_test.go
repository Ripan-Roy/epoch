package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/protobuf/proto"
)

func TestBoundedReadsVisitEveryResourceAtEveryController(t *testing.T) {
	resources := make([]*epochv1.Resource, 135)
	for index := range resources {
		resources[index] = &epochv1.Resource{Name: scopedName(fmt.Sprintf("resource-%d", index)), Generation: 1, Spec: cacheSpec("expected")}
	}
	var active, maximum atomic.Int32
	var lock sync.Mutex
	visits := make(map[string]int)
	err := verifyDesiredAtEveryController(5, resources, func(controller int, name *epochv1.ResourceName) (*epochv1.Resource, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		time.Sleep(time.Millisecond)
		lock.Lock()
		visits[fmt.Sprintf("%d/%s", controller, name.Name)]++
		lock.Unlock()
		for _, resource := range resources {
			if proto.Equal(resource.Name, name) {
				return proto.Clone(resource).(*epochv1.Resource), nil
			}
		}
		return nil, errors.New("unknown resource")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(visits) != 5*135 || maximum.Load() < 2 || maximum.Load() > 16 {
		t.Fatalf("visits=%d maximum=%d", len(visits), maximum.Load())
	}
	for key, count := range visits {
		if count != 1 {
			t.Fatalf("%s visited %d times", key, count)
		}
	}
}

func TestBoundedReadsRejectAnyLostGenerationSpecOrRPCFailure(t *testing.T) {
	expected := &epochv1.Resource{Name: scopedName("required"), Generation: 2, Spec: cacheSpec("exact")}
	for _, change := range []func(*epochv1.Resource) error{
		func(resource *epochv1.Resource) error { resource.Generation--; return nil },
		func(resource *epochv1.Resource) error { resource.Spec.Labels["ha_revision"] = "changed"; return nil },
		func(*epochv1.Resource) error { return errors.New("RPC failed") },
	} {
		err := verifyDesiredAtEveryController(5, []*epochv1.Resource{expected}, func(controller int, _ *epochv1.ResourceName) (*epochv1.Resource, error) {
			resource := proto.Clone(expected).(*epochv1.Resource)
			if controller == 4 {
				return resource, change(resource)
			}
			return resource, nil
		})
		if err == nil {
			t.Fatal("last controller's changed state or failure was hidden")
		}
	}
}

func maximumBatchFixture() (*epochv1.BatchApplyResourcesRequest, []*epochv1.BatchApplyResourcesResponse, []*epochv1.GetOperationResponse, *epochv1.WatchResourceChangesResponse) {
	request := &epochv1.BatchApplyResourcesRequest{RequestToken: "maximum-token"}
	response := &epochv1.BatchApplyResourcesResponse{Replayed: true}
	operation := &epochv1.GetOperationResponse{RequestToken: request.RequestToken, ProposalId: 42,
		State: epochv1.OperationState_OPERATION_STATE_SUCCEEDED, CommandKind: "apply_desired",
		FirstChangeCursor: 11, LastChangeCursor: 138,
	}
	page := &epochv1.WatchResourceChangesResponse{EarliestCursor: 1, LatestCursor: 140, NextCursor: 138}
	for index := range 128 {
		name := testName()
		name.Name = fmt.Sprintf("maximum-%03d", index)
		request.Resources = append(request.Resources, batchItem(name, "initial", 0))
		response.Results = append(response.Results, &epochv1.ApplyResourceResponse{
			Resource: &epochv1.Resource{Name: name, Generation: 1, Spec: cacheSpec("initial")},
			Created:  true, Changed: true, Replayed: true,
		})
		operation.AffectedResources = append(operation.AffectedResources, name)
		page.Changes = append(page.Changes, &epochv1.ResourceChange{
			Cursor: uint64(11 + index), Name: name, Generation: 1,
			Kind: epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_APPLIED,
		})
	}
	responses := make([]*epochv1.BatchApplyResourcesResponse, 5)
	operations := make([]*epochv1.GetOperationResponse, 5)
	for index := range responses {
		responses[index] = proto.Clone(response).(*epochv1.BatchApplyResourcesResponse)
		operations[index] = proto.Clone(operation).(*epochv1.GetOperationResponse)
	}
	return request, responses, operations, page
}

func TestMaximumBatchProvesOneDurableOutcomeRatherThanCountingReplayFlags(t *testing.T) {
	request, responses, operations, page := maximumBatchFixture()
	// A receipt reconstructed between the application-receipt read and the
	// consensus lookup can mark even the first caller as replayed. Five flags
	// are not five outcomes: exact operation identity and history are decisive.
	if err := validateMaximumBatch(request, responses, operations, page); err != nil {
		t.Fatal(err)
	}
	responses[2].Replayed = false
	for _, result := range responses[2].Results {
		result.Replayed = false
	}
	if err := validateMaximumBatch(request, responses, operations, page); err != nil {
		t.Fatal(err)
	}
}

func TestMaximumBatchRejectsIncompleteOrAlteredResponseEvidence(t *testing.T) {
	for label, mutate := range map[string]func(*epochv1.BatchApplyResourcesResponse){
		"missing result":     func(value *epochv1.BatchApplyResourcesResponse) { value.Results = value.Results[:127] },
		"duplicate identity": func(value *epochv1.BatchApplyResourcesResponse) { value.Results[1] = value.Results[0] },
		"wrong generation":   func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0].Resource.Generation++ },
		"wrong spec": func(value *epochv1.BatchApplyResourcesResponse) {
			value.Results[0].Resource.Spec = cacheSpec("different")
		},
		"lost creation":       func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0].Created = false },
		"lost change":         func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0].Changed = false },
		"inconsistent replay": func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0].Replayed = false },
		"nil result":          func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0] = nil },
	} {
		t.Run(label, func(t *testing.T) {
			request, responses, operations, page := maximumBatchFixture()
			mutate(responses[4])
			if err := validateMaximumBatch(request, responses, operations, page); err == nil {
				t.Fatal("accepted incomplete or altered response")
			}
		})
	}
}

func TestMaximumBatchRejectsDifferentOrMalformedDurableOutcomes(t *testing.T) {
	for label, mutate := range map[string]func(*epochv1.GetOperationResponse){
		"zero proposal":       func(value *epochv1.GetOperationResponse) { value.ProposalId = 0 },
		"wrong token":         func(value *epochv1.GetOperationResponse) { value.RequestToken = "another" },
		"missing scope":       func(value *epochv1.GetOperationResponse) { value.AffectedResources = value.AffectedResources[:127] },
		"duplicate scope":     func(value *epochv1.GetOperationResponse) { value.AffectedResources[1] = value.AffectedResources[0] },
		"failed outcome":      func(value *epochv1.GetOperationResponse) { value.State = epochv1.OperationState_OPERATION_STATE_FAILED },
		"wrong kind":          func(value *epochv1.GetOperationResponse) { value.CommandKind = "delete_managed" },
		"delete precondition": func(value *epochv1.GetOperationResponse) { value.ExpectedGeneration = proto.Uint64(0) },
		"zero cursor":         func(value *epochv1.GetOperationResponse) { value.FirstChangeCursor = 0 },
		"short history":       func(value *epochv1.GetOperationResponse) { value.LastChangeCursor-- },
		"failure details":     func(value *epochv1.GetOperationResponse) { value.FailureCode = "conflict" },
	} {
		t.Run(label, func(t *testing.T) {
			request, responses, operations, page := maximumBatchFixture()
			// Alter all observers as well: agreement alone cannot validate a
			// wrong token, scope, command, or partial history.
			for _, operation := range operations {
				mutate(operation)
			}
			if err := validateMaximumBatch(request, responses, operations, page); err == nil {
				t.Fatal("accepted malformed durable outcome")
			}
		})
	}
	request, responses, operations, page := maximumBatchFixture()
	operations[4].ProposalId++
	if err := validateMaximumBatch(request, responses, operations, page); err == nil {
		t.Fatal("accepted two different durable proposal identities")
	}
}

func TestMaximumBatchRejectsDuplicateMissingOrForeignChangeHistory(t *testing.T) {
	for label, mutate := range map[string]func(*epochv1.WatchResourceChangesResponse){
		"missing change":   func(value *epochv1.WatchResourceChangesResponse) { value.Changes = value.Changes[:127] },
		"duplicate change": func(value *epochv1.WatchResourceChangesResponse) { value.Changes[1] = value.Changes[0] },
		"wrong cursor":     func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0].Cursor-- },
		"wrong identity":   func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0].Name.Organization = "otherco" },
		"wrong generation": func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0].Generation++ },
		"status instead of desired": func(value *epochv1.WatchResourceChangesResponse) {
			value.Changes[0].Kind = epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_STATUS_UPDATED
		},
		"unscanned change":  func(value *epochv1.WatchResourceChangesResponse) { value.NextCursor-- },
		"invalid watermark": func(value *epochv1.WatchResourceChangesResponse) { value.LatestCursor = value.NextCursor - 1 },
		"nil change":        func(value *epochv1.WatchResourceChangesResponse) { value.Changes[0] = nil },
	} {
		t.Run(label, func(t *testing.T) {
			request, responses, operations, page := maximumBatchFixture()
			mutate(page)
			if err := validateMaximumBatch(request, responses, operations, page); err == nil {
				t.Fatal("accepted duplicate, missing, or foreign change history")
			}
		})
	}
	request, responses, operations, page := maximumBatchFixture()
	if err := validateMaximumBatch(request, responses[:4], operations, page); err == nil {
		t.Fatal("accepted missing controller response")
	}
	if err := validateMaximumBatch(request, responses, operations[:4], page); err == nil {
		t.Fatal("accepted missing controller operation")
	}
}

func TestMaximumBatchSerializedWitnessesRemainFailClosed(t *testing.T) {
	request, responses, operations, page := maximumBatchFixture()
	proof := &maximumBatchProof{Request: marshal(request), Changes: marshal(page)}
	for index := range responses {
		proof.Responses = append(proof.Responses, marshal(responses[index]))
		proof.Operations = append(proof.Operations, marshal(operations[index]))
	}
	if err := validateMaximumBatchProof(proof, 5); err != nil {
		t.Fatal(err)
	}
	for label, mutate := range map[string]func(*maximumBatchProof){
		"missing request":     func(value *maximumBatchProof) { value.Request = nil },
		"missing responses":   func(value *maximumBatchProof) { value.Responses = value.Responses[:4] },
		"missing operations":  func(value *maximumBatchProof) { value.Operations = value.Operations[:4] },
		"missing changes":     func(value *maximumBatchProof) { value.Changes = nil },
		"malformed request":   func(value *maximumBatchProof) { value.Request = []byte{0xff} },
		"malformed response":  func(value *maximumBatchProof) { value.Responses[0] = []byte{0xff} },
		"malformed operation": func(value *maximumBatchProof) { value.Operations[0] = []byte{0xff} },
		"malformed changes":   func(value *maximumBatchProof) { value.Changes = []byte{0xff} },
	} {
		t.Run(label, func(t *testing.T) {
			broken := *proof
			broken.Responses = append([][]byte(nil), proof.Responses...)
			broken.Operations = append([][]byte(nil), proof.Operations...)
			mutate(&broken)
			if err := validateMaximumBatchProof(&broken, 5); err == nil {
				t.Fatal("accepted incomplete or malformed serialized witnesses")
			}
		})
	}
	if err := validateMaximumBatchProof(nil, 5); err == nil {
		t.Fatal("accepted missing witnesses")
	}
}

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

func TestWatchDetectsFilteredChangesInsideRatherThanOnlyAfterVisiblePage(t *testing.T) {
	page := &epochv1.WatchResourceChangesResponse{LatestCursor: 20, NextCursor: 13,
		Changes: []*epochv1.ResourceChange{{Cursor: 11}, {Cursor: 13}},
	}
	if !watchPageFiltered(10, page) {
		t.Fatal("hidden cursor 12 inside page was not recorded as scanned")
	}
	page.Changes = []*epochv1.ResourceChange{{Cursor: 11}, {Cursor: 12}, {Cursor: 13}}
	if watchPageFiltered(10, page) {
		t.Fatal("unfiltered page was marked filtered")
	}
	page.Changes = nil
	if !watchPageFiltered(10, page) {
		t.Fatal("empty authorized page lost its scanned checkpoint")
	}
}

func testName() *epochv1.ResourceName {
	return &epochv1.ResourceName{Organization: "acme", Project: "payments", Environment: "production", Namespace: "orders", Kind: epochv1.ResourceKind_RESOURCE_KIND_CACHE, Name: "fixture"}
}

func TestLookupBindingRetainsOnlyExactCommittedApplyOutcomes(t *testing.T) {
	request := &epochv1.GetOperationRequest{RequestToken: "unknown-caller", AffectedResources: []*epochv1.ResourceName{testName()}}
	response := &epochv1.GetOperationResponse{RequestToken: request.RequestToken, ProposalId: 7,
		State: epochv1.OperationState_OPERATION_STATE_SUCCEEDED, CommandKind: "apply_desired",
		AffectedResources: request.AffectedResources, FirstChangeCursor: 10, LastChangeCursor: 10,
	}
	lookup := func(*epochv1.GetOperationRequest) (*epochv1.GetOperationResponse, error) { return response, nil }
	bound, err := bindLookups([]*epochv1.GetOperationRequest{request}, nil, lookup)
	if err != nil || len(bound) != 1 || bound[0].Kind != "lookup" {
		t.Fatalf("bound lookup = %v, %v", bound, err)
	}
	original := proto.Clone(response).(*epochv1.GetOperationResponse)
	for _, mutate := range []func(*epochv1.GetOperationResponse){
		func(value *epochv1.GetOperationResponse) { value.RequestToken = "wrong-token" },
		func(value *epochv1.GetOperationResponse) {
			value.State = epochv1.OperationState_OPERATION_STATE_PENDING
		},
		func(value *epochv1.GetOperationResponse) { value.CommandKind = "delete_managed" },
		func(value *epochv1.GetOperationResponse) { value.AffectedResources[0].Organization = "wrong-tenant" },
	} {
		response = proto.Clone(original).(*epochv1.GetOperationResponse)
		mutate(response)
		if _, err := bindLookups([]*epochv1.GetOperationRequest{request}, nil, lookup); err == nil {
			t.Fatal("accepted non-exact committed lookup")
		}
	}
	response = proto.Clone(original).(*epochv1.GetOperationResponse)
	if _, err := bindLookups([]*epochv1.GetOperationRequest{request, request}, nil, lookup); err == nil {
		t.Fatal("accepted duplicate bindings")
	}
	if _, err := bindLookups([]*epochv1.GetOperationRequest{request}, nil, func(*epochv1.GetOperationRequest) (*epochv1.GetOperationResponse, error) {
		return nil, errors.New("no durable outcome")
	}); err == nil {
		t.Fatal("accepted missing outcome")
	}
}
