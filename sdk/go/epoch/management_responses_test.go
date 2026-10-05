package epoch

import (
	"testing"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func managementTestResource() *epochv1.Resource {
	return &epochv1.Resource{Name: managementTestName(), Spec: &epochv1.ResourceSpec{}, Generation: 1}
}

func TestManagementReadAndApplyResponsesRejectMissingOrForeignIdentity(t *testing.T) {
	for _, resource := range []*epochv1.Resource{nil, {}, {Name: managementTestName(), Generation: 1}, {Name: managementTestName(), Spec: &epochv1.ResourceSpec{}}} {
		if status.Code(validateManagementGetResponse(&epochv1.GetResourceRequest{Name: managementTestName()}, &epochv1.GetResourceResponse{Resource: resource})) != codes.DataLoss {
			t.Fatal("accepted incomplete get result")
		}
		if status.Code(validateManagementApplyResponse(&epochv1.ApplyResourceRequest{Name: managementTestName()}, &epochv1.ApplyResourceResponse{Resource: resource})) != codes.DataLoss {
			t.Fatal("accepted incomplete apply result")
		}
	}
	resource := managementTestResource()
	if err := validateManagementGetResponse(&epochv1.GetResourceRequest{Name: resource.Name}, &epochv1.GetResourceResponse{Resource: resource}); err != nil {
		t.Fatal(err)
	}
	foreign := proto.Clone(resource).(*epochv1.Resource)
	foreign.Name.Organization = "otherco"
	if err := validateManagementGetResponse(&epochv1.GetResourceRequest{Name: resource.Name}, &epochv1.GetResourceResponse{Resource: foreign}); err == nil {
		t.Fatal("accepted foreign get identity")
	}
	list := &epochv1.ListResourcesResponse{Resources: []*epochv1.Resource{resource}}
	if err := validateManagementListResponse(&epochv1.ListResourcesRequest{Organization: "acme"}, list); err != nil {
		t.Fatal(err)
	}
	if err := validateManagementListResponse(&epochv1.ListResourcesRequest{Organization: "otherco"}, list); err == nil {
		t.Fatal("accepted a list outside the requested scope")
	}
	list.Resources = append(list.Resources, resource)
	if err := validateManagementListResponse(&epochv1.ListResourcesRequest{}, list); err == nil {
		t.Fatal("accepted duplicate list resources")
	}
	if err := validateManagementListResponse(&epochv1.ListResourcesRequest{PageSize: 1}, list); err == nil {
		t.Fatal("accepted an oversized list page")
	}
}

func TestManagementBatchResponsesMustContainEveryExactResourceOnce(t *testing.T) {
	first, second := managementTestResource(), managementTestResource()
	second.Name.Name = "second"
	request := &epochv1.BatchApplyResourcesRequest{Resources: []*epochv1.BatchApplyResource{{Name: first.Name, Spec: first.Spec}, {Name: second.Name, Spec: second.Spec}}}
	response := &epochv1.BatchApplyResourcesResponse{Replayed: true, Results: []*epochv1.ApplyResourceResponse{{Resource: second, Replayed: true}, {Resource: first, Replayed: true}}}
	if err := validateManagementBatchResponse(request, response); err != nil {
		t.Fatal("canonical response order cannot depend on caller order", err)
	}
	for label, mutate := range map[string]func(*epochv1.BatchApplyResourcesResponse){
		"partial":             func(value *epochv1.BatchApplyResourcesResponse) { value.Results = value.Results[:1] },
		"duplicate":           func(value *epochv1.BatchApplyResourcesResponse) { value.Results[1] = value.Results[0] },
		"inconsistent replay": func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0].Replayed = false },
		"missing resource":    func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0].Resource = nil },
		"foreign resource":    func(value *epochv1.BatchApplyResourcesResponse) { value.Results[0].Resource.Name.Environment = "prod" },
	} {
		t.Run(label, func(t *testing.T) {
			broken := proto.Clone(response).(*epochv1.BatchApplyResourcesResponse)
			mutate(broken)
			if status.Code(validateManagementBatchResponse(request, broken)) != codes.DataLoss {
				t.Fatal("accepted incomplete or altered batch receipt")
			}
		})
	}
}

func TestManagementListResponsesMustMatchEveryGovernanceFilter(t *testing.T) {
	request := &epochv1.ListResourcesRequest{Owner: "team:checkout", CostCenter: "cc-42", Classification: epochv1.DataClassification_DATA_CLASSIFICATION_INTERNAL, Tags: map[string]string{"cost": "team"}}
	resource := managementTestResource()
	resource.Spec.Governance = &epochv1.ResourceGovernance{Owner: request.Owner, CostCenter: request.CostCenter, Classification: request.Classification, Tags: map[string]string{"cost": "team", "extra": "allowed"}}
	response := &epochv1.ListResourcesResponse{Resources: []*epochv1.Resource{resource}}
	if err := validateManagementListResponse(request, response); err != nil {
		t.Fatal("valid governance filter receipt was rejected", err)
	}
	for label, mutate := range map[string]func(*epochv1.Resource){
		"absent governance": func(value *epochv1.Resource) { value.Spec.Governance = nil },
		"wrong owner":       func(value *epochv1.Resource) { value.Spec.Governance.Owner = "other-team" },
		"wrong cost center": func(value *epochv1.Resource) { value.Spec.Governance.CostCenter = "cc-other" },
		"wrong classification": func(value *epochv1.Resource) {
			value.Spec.Governance.Classification = epochv1.DataClassification_DATA_CLASSIFICATION_PUBLIC
		},
		"missing tag": func(value *epochv1.Resource) { delete(value.Spec.Governance.Tags, "cost") },
		"wrong tag":   func(value *epochv1.Resource) { value.Spec.Governance.Tags["cost"] = "other" },
	} {
		t.Run(label, func(t *testing.T) {
			broken := proto.Clone(response).(*epochv1.ListResourcesResponse)
			mutate(broken.Resources[0])
			if status.Code(validateManagementListResponse(request, broken)) != codes.DataLoss {
				t.Fatal("accepted a list outside its requested governance filter")
			}
		})
	}
}

func TestManagementOperationResponsesPreserveScopeStateAndCursorBounds(t *testing.T) {
	request := &epochv1.GetOperationRequest{RequestToken: "original", AffectedResources: []*epochv1.ResourceName{managementTestName()}}
	response := &epochv1.GetOperationResponse{RequestToken: request.RequestToken, ProposalId: 42, State: epochv1.OperationState_OPERATION_STATE_SUCCEEDED, CommandKind: "delete_managed", AffectedResources: request.AffectedResources, ExpectedGeneration: proto.Uint64(0)}
	if err := validateManagementOperationResponse(request, response); err != nil {
		t.Fatal(err)
	}
	for label, mutate := range map[string]func(*epochv1.GetOperationResponse){
		"wrong token":          func(value *epochv1.GetOperationResponse) { value.RequestToken = "another" },
		"zero proposal":        func(value *epochv1.GetOperationResponse) { value.ProposalId = 0 },
		"missing kind":         func(value *epochv1.GetOperationResponse) { value.CommandKind = "" },
		"unknown state":        func(value *epochv1.GetOperationResponse) { value.State = 99 },
		"wrong scope":          func(value *epochv1.GetOperationResponse) { value.AffectedResources[0].Project = "other" },
		"missing scope":        func(value *epochv1.GetOperationResponse) { value.AffectedResources = nil },
		"cursor inversion":     func(value *epochv1.GetOperationResponse) { value.FirstChangeCursor = 2; value.LastChangeCursor = 1 },
		"orphan end cursor":    func(value *epochv1.GetOperationResponse) { value.LastChangeCursor = 1 },
		"missing failure code": func(value *epochv1.GetOperationResponse) { value.State = epochv1.OperationState_OPERATION_STATE_FAILED },
		"failure on success":   func(value *epochv1.GetOperationResponse) { value.FailureMessage = "not successful" },
	} {
		t.Run(label, func(t *testing.T) {
			broken := proto.Clone(response).(*epochv1.GetOperationResponse)
			mutate(broken)
			if status.Code(validateManagementOperationResponse(request, broken)) != codes.DataLoss {
				t.Fatal("accepted wrong durable operation identity or outcome")
			}
		})
	}
}
