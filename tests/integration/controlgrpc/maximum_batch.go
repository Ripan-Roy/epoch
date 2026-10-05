package main

import (
	"errors"
	"fmt"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/protobuf/proto"
)

// Retain the actual concurrent replies, all controller lookups, and the
// committed change page even when a later invariant fails. Replay flags do
// not identify independent durable effects: a first caller can receive a
// reconstructed receipt after its original application-receipt read races
// consensus commit (catalog_api::commit_command_with_mode).
type maximumBatchProof struct {
	Request    []byte   `json:"request_proto"`
	Responses  [][]byte `json:"response_proto"`
	Operations [][]byte `json:"operation_proto"`
	Changes    []byte   `json:"changes_proto"`
}

func validateMaximumBatchProof(proof *maximumBatchProof, controllers int) error {
	if proof == nil || len(proof.Responses) != controllers || len(proof.Operations) != controllers {
		return errors.New("missing complete initial concurrent batch witnesses")
	}
	request := &epochv1.BatchApplyResourcesRequest{}
	if err := proto.Unmarshal(proof.Request, request); err != nil {
		return err
	}
	responses := make([]*epochv1.BatchApplyResourcesResponse, controllers)
	operations := make([]*epochv1.GetOperationResponse, controllers)
	for index := range controllers {
		responses[index] = &epochv1.BatchApplyResourcesResponse{}
		if err := proto.Unmarshal(proof.Responses[index], responses[index]); err != nil {
			return err
		}
		operations[index] = &epochv1.GetOperationResponse{}
		if err := proto.Unmarshal(proof.Operations[index], operations[index]); err != nil {
			return err
		}
	}
	page := &epochv1.WatchResourceChangesResponse{}
	if err := proto.Unmarshal(proof.Changes, page); err != nil {
		return err
	}
	return validateMaximumBatch(request, responses, operations, page)
}

func (fleet *clients) proveMaximumBatch(request *epochv1.BatchApplyResourcesRequest, responses []*epochv1.BatchApplyResourcesResponse) error {
	names := make([]*epochv1.ResourceName, 0, len(request.Resources))
	for _, item := range request.Resources {
		names = append(names, item.Name)
	}
	operations := make([]*epochv1.GetOperationResponse, 0, len(fleet.connections))
	for index := range fleet.connections {
		operation, err := fleet.operation(index, fleet.admin, request.RequestToken, names)
		if err != nil {
			return fmt.Errorf("maximum batch operation at controller %d: %w", index, err)
		}
		operations = append(operations, operation)
		fleet.proof.InitialBatch.Operations = append(fleet.proof.InitialBatch.Operations, marshal(operation))
	}
	first := operations[0].GetFirstChangeCursor()
	if first == 0 {
		return errors.New("maximum batch has no committed change cursor")
	}
	ctx, cancel := callContext(fleet.admin)
	defer cancel()
	stream, err := fleet.client(0).WatchResourceChanges(ctx, &epochv1.WatchResourceChangesRequest{AfterCursor: first - 1, BatchSize: 128})
	if err != nil {
		return err
	}
	page, err := stream.Recv()
	if err != nil {
		return err
	}
	fleet.proof.InitialBatch.Changes = marshal(page)
	return validateMaximumBatch(request, responses, operations, page)
}

func validateMaximumBatch(request *epochv1.BatchApplyResourcesRequest, responses []*epochv1.BatchApplyResourcesResponse, operations []*epochv1.GetOperationResponse, page *epochv1.WatchResourceChangesResponse) error {
	if request == nil || request.GetRequestToken() == "" || len(request.GetResources()) != 128 || (len(responses) != 3 && len(responses) != 5) || len(operations) != len(responses) {
		return errors.New("maximum batch lacks its exact request or complete controller inventory")
	}
	for index, item := range request.Resources {
		if item == nil || item.Name == nil || item.Spec == nil || item.ExpectedGeneration == nil || *item.ExpectedGeneration != 0 {
			return errors.New("maximum batch lacks a generation-zero creation request")
		}
		if index > 0 && item.Name.GetName() <= request.Resources[index-1].Name.GetName() {
			return errors.New("maximum batch request identities are not distinct and ordered")
		}
	}
	first := operations[0]
	if first == nil || first.GetProposalId() == 0 || first.GetRequestToken() != request.GetRequestToken() || first.GetState() != epochv1.OperationState_OPERATION_STATE_SUCCEEDED || first.GetCommandKind() != "apply_desired" || first.ExpectedGeneration != nil || first.GetFailureCode() != "" || first.GetFailureMessage() != "" || len(first.GetAffectedResources()) != len(request.Resources) || first.GetFirstChangeCursor() == 0 || first.GetLastChangeCursor() < first.GetFirstChangeCursor() || first.GetLastChangeCursor()-first.GetFirstChangeCursor() != 127 {
		return errors.New("maximum batch lacks one exact successful durable creation outcome")
	}
	for index, item := range request.Resources {
		if !proto.Equal(first.AffectedResources[index], item.Name) {
			return errors.New("maximum batch operation changed its affected identities")
		}
	}
	for controller, response := range responses {
		if err := compareOperation(first, operations[controller]); err != nil {
			return err
		}
		if response == nil || len(response.GetResults()) != len(request.Resources) {
			return errors.New("maximum batch returned a partial creation result")
		}
		for index, result := range response.Results {
			wanted := &epochv1.Resource{Name: request.Resources[index].Name, Spec: request.Resources[index].Spec, Generation: 1}
			observed, err := desiredWitness(result.GetResource())
			if err != nil || !proto.Equal(wanted, observed) || !result.GetCreated() || !result.GetChanged() || result.GetReplayed() != response.GetReplayed() {
				return errors.New("maximum batch altered its retained creation result")
			}
		}
	}
	if page == nil || page.GetEarliestCursor() == 0 || page.GetEarliestCursor() > first.GetFirstChangeCursor() || page.GetNextCursor() != first.GetLastChangeCursor() || page.GetLatestCursor() < page.GetNextCursor() || len(page.GetChanges()) != len(request.Resources) {
		return errors.New("maximum batch lacks its complete retained change page")
	}
	for index, change := range page.Changes {
		if change == nil || change.GetCursor() != first.GetFirstChangeCursor()+uint64(index) || change.GetGeneration() != 1 || change.GetKind() != epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_APPLIED || !proto.Equal(change.GetName(), request.Resources[index].Name) {
			return errors.New("maximum batch changed, duplicated, or omitted a committed creation event")
		}
	}
	return nil
}
