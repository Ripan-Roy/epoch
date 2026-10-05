package epoch

import (
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func managementProtocolError() error {
	return status.Error(codes.DataLoss, "management response violates its identity or outcome contract")
}

func validateManagementResource(resource *epochv1.Resource) error {
	if resource == nil || validateManagementName(resource.Name) != nil || resource.Generation == 0 || resource.Spec == nil {
		return managementProtocolError()
	}
	return nil
}

func validateManagementApplyResponse(request *epochv1.ApplyResourceRequest, response *epochv1.ApplyResourceResponse) error {
	if validateManagementResource(response.Resource) != nil || !proto.Equal(request.Name, response.GetResource().GetName()) {
		return managementProtocolError()
	}
	return nil
}

func validateManagementGetResponse(request *epochv1.GetResourceRequest, response *epochv1.GetResourceResponse) error {
	if validateManagementResource(response.Resource) != nil || !proto.Equal(request.Name, response.GetResource().GetName()) {
		return managementProtocolError()
	}
	return nil
}

func validateManagementListResponse(request *epochv1.ListResourcesRequest, response *epochv1.ListResourcesResponse) error {
	limit := request.PageSize
	if limit == 0 {
		limit = 50
	}
	if len(response.Resources) > int(limit) {
		return managementProtocolError()
	}
	names := make([]*epochv1.ResourceName, 0, len(response.Resources))
	for _, resource := range response.Resources {
		if validateManagementResource(resource) != nil {
			return managementProtocolError()
		}
		name := resource.Name
		if (request.Organization != "" && request.Organization != name.Organization) ||
			(request.Project != "" && request.Project != name.Project) ||
			(request.Environment != "" && request.Environment != name.Environment) ||
			(request.Namespace != "" && request.Namespace != name.Namespace) ||
			(request.Kind != 0 && request.Kind != name.Kind) {
			return managementProtocolError()
		}
		governance := resource.Spec.GetGovernance()
		if (request.Owner != "" && request.Owner != governance.GetOwner()) ||
			(request.CostCenter != "" && request.CostCenter != governance.GetCostCenter()) ||
			(request.Classification != 0 && request.Classification != governance.GetClassification()) {
			return managementProtocolError()
		}
		for key, expected := range request.Tags {
			if observed, exists := governance.GetTags()[key]; !exists || observed != expected {
				return managementProtocolError()
			}
		}
		names = append(names, name)
	}
	if len(names) != 0 && validateManagementNames(names) != nil {
		return managementProtocolError()
	}
	return nil
}

func validateManagementDeleteResponse(request *epochv1.DeleteResourceRequest, response *epochv1.DeleteResourceResponse) error {
	if !proto.Equal(request.Name, response.Name) || (response.Deleted && response.Generation == 0) {
		return managementProtocolError()
	}
	return nil
}

func validateManagementBatchResponse(request *epochv1.BatchApplyResourcesRequest, response *epochv1.BatchApplyResourcesResponse) error {
	if len(response.Results) != len(request.Resources) {
		return managementProtocolError()
	}
	remaining := make([]*epochv1.ResourceName, len(request.Resources))
	for index, item := range request.Resources {
		remaining[index] = item.Name
	}
	for _, result := range response.Results {
		if result == nil || validateManagementResource(result.Resource) != nil || result.Replayed != response.Replayed {
			return managementProtocolError()
		}
		found := false
		for index, name := range remaining {
			if name != nil && proto.Equal(name, result.Resource.Name) {
				remaining[index] = nil
				found = true
				break
			}
		}
		if !found {
			return managementProtocolError()
		}
	}
	return nil
}

func validateManagementOperationResponse(request *epochv1.GetOperationRequest, response *epochv1.GetOperationResponse) error {
	if response.RequestToken != request.RequestToken || response.ProposalId == 0 || response.CommandKind == "" || response.State < epochv1.OperationState_OPERATION_STATE_PENDING || response.State > epochv1.OperationState_OPERATION_STATE_FAILED || len(response.AffectedResources) != len(request.AffectedResources) || validateManagementNames(response.AffectedResources) != nil || response.LastChangeCursor < response.FirstChangeCursor || (response.FirstChangeCursor == 0 && response.LastChangeCursor != 0) {
		return managementProtocolError()
	}
	for _, name := range request.AffectedResources {
		found := false
		for _, observed := range response.AffectedResources {
			if proto.Equal(name, observed) {
				found = true
				break
			}
		}
		if !found {
			return managementProtocolError()
		}
	}
	if response.State == epochv1.OperationState_OPERATION_STATE_FAILED {
		if response.FailureCode == "" {
			return managementProtocolError()
		}
	} else if response.FailureCode != "" || response.FailureMessage != "" {
		return managementProtocolError()
	}
	return nil
}
