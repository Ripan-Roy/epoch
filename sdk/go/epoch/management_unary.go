package epoch

import (
	"context"
	"strconv"
	"strings"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func managementInvalid(message string) error { return status.Error(codes.InvalidArgument, message) }

func validateManagementToken(token string) error {
	if token == "" || strings.TrimSpace(token) != token || len(token) > 256 {
		return managementInvalid("management request token must be canonical and between 1 and 256 bytes")
	}
	return nil
}

func validateManagementName(name *epochv1.ResourceName) error {
	if name == nil || name.Kind == epochv1.ResourceKind_RESOURCE_KIND_UNSPECIFIED || epochv1.ResourceKind_name[int32(name.Kind)] == "" {
		return managementInvalid("a known fully qualified management resource name is required")
	}
	for _, value := range []string{name.Organization, name.Project, name.Environment, name.Namespace, name.Name} {
		if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "/\x00\r\n") {
			return managementInvalid("management resource identity segments must be nonempty and canonical")
		}
	}
	return nil
}

func validateManagementNames(names []*epochv1.ResourceName) error {
	if len(names) == 0 || len(names) > 128 {
		return managementInvalid("management scope must contain 1 to 128 distinct resource names")
	}
	seen := map[string]bool{}
	for _, name := range names {
		if err := validateManagementName(name); err != nil {
			return err
		}
		key := strings.Join([]string{name.Organization, name.Project, name.Environment, name.Namespace, strconv.Itoa(int(name.Kind)), name.Name}, "\x00")
		if seen[key] {
			return managementInvalid("management resource names must be distinct")
		}
		seen[key] = true
	}
	return nil
}

// ApplyResource accepts desired state; it does not equate acceptance with readiness.
func (client *ManagementClient) ApplyResource(ctx context.Context, request *epochv1.ApplyResourceRequest) (*epochv1.ApplyResourceResponse, ManagementCallInfo, error) {
	if request == nil || request.Spec == nil {
		return nil, ManagementCallInfo{}, managementInvalid("management apply request and spec are required")
	}
	if err := validateManagementToken(request.RequestToken); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	if err := validateManagementName(request.Name); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	return managementCall(ctx, client, request, true, func(ctx context.Context, endpoint epochv1.RegionalAdminServiceClient, request *epochv1.ApplyResourceRequest) (*epochv1.ApplyResourceResponse, error) {
		return endpoint.ApplyResource(ctx, request)
	}, validateManagementApplyResponse)
}

// GetResource returns one validated, fully qualified resource identity.
func (client *ManagementClient) GetResource(ctx context.Context, request *epochv1.GetResourceRequest) (*epochv1.GetResourceResponse, ManagementCallInfo, error) {
	if request == nil {
		return nil, ManagementCallInfo{}, managementInvalid("management get request is required")
	}
	if err := validateManagementName(request.Name); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	return managementCall(ctx, client, request, false, func(ctx context.Context, endpoint epochv1.RegionalAdminServiceClient, request *epochv1.GetResourceRequest) (*epochv1.GetResourceResponse, error) {
		return endpoint.GetResource(ctx, request)
	}, validateManagementGetResponse)
}

// ListResources returns a bounded inventory with exact scope/governance filters.
// The current service rejects nonempty page tokens; pagination remains future work.
func (client *ManagementClient) ListResources(ctx context.Context, request *epochv1.ListResourcesRequest) (*epochv1.ListResourcesResponse, ManagementCallInfo, error) {
	if request == nil || request.PageSize < 0 || request.PageSize > 100 {
		return nil, ManagementCallInfo{}, managementInvalid("management list request must have page_size between 0 and 100")
	}
	if epochv1.ResourceKind_name[int32(request.Kind)] == "" || epochv1.DataClassification_name[int32(request.Classification)] == "" {
		return nil, ManagementCallInfo{}, managementInvalid("management list filter enums must be known")
	}
	return managementCall(ctx, client, request, false, func(ctx context.Context, endpoint epochv1.RegionalAdminServiceClient, request *epochv1.ListResourcesRequest) (*epochv1.ListResourcesResponse, error) {
		return endpoint.ListResources(ctx, request)
	}, validateManagementListResponse)
}

// DeleteResource retains the caller's token and omitted/zero/nonzero OCC presence.
func (client *ManagementClient) DeleteResource(ctx context.Context, request *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, ManagementCallInfo, error) {
	if request == nil {
		return nil, ManagementCallInfo{}, managementInvalid("management delete request is required")
	}
	if err := validateManagementToken(request.RequestToken); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	if err := validateManagementName(request.Name); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	return managementCall(ctx, client, request, true, func(ctx context.Context, endpoint epochv1.RegionalAdminServiceClient, request *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
		return endpoint.DeleteResource(ctx, request)
	}, validateManagementDeleteResponse)
}

// BatchApplyResources atomically accepts 1–128 distinct desired resources, not
// physical materialization. A receipt must contain every exact identity once.
func (client *ManagementClient) BatchApplyResources(ctx context.Context, request *epochv1.BatchApplyResourcesRequest) (*epochv1.BatchApplyResourcesResponse, ManagementCallInfo, error) {
	if request == nil {
		return nil, ManagementCallInfo{}, managementInvalid("management batch request is required")
	}
	if err := validateManagementToken(request.RequestToken); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	names := make([]*epochv1.ResourceName, 0, len(request.Resources))
	for _, item := range request.Resources {
		if item == nil || item.Spec == nil {
			return nil, ManagementCallInfo{}, managementInvalid("management batch resources and specs cannot be nil")
		}
		names = append(names, item.Name)
	}
	if err := validateManagementNames(names); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	return managementCall(ctx, client, request, true, func(ctx context.Context, endpoint epochv1.RegionalAdminServiceClient, request *epochv1.BatchApplyResourcesRequest) (*epochv1.BatchApplyResourcesResponse, error) {
		return endpoint.BatchApplyResources(ctx, request)
	}, validateManagementBatchResponse)
}

// GetOperation resolves the original token only for its exact affected-resource
// set. NotFound is not evidence that an unknown write never committed.
func (client *ManagementClient) GetOperation(ctx context.Context, request *epochv1.GetOperationRequest) (*epochv1.GetOperationResponse, ManagementCallInfo, error) {
	if request == nil {
		return nil, ManagementCallInfo{}, managementInvalid("management operation request is required")
	}
	if err := validateManagementToken(request.RequestToken); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	if err := validateManagementNames(request.AffectedResources); err != nil {
		return nil, ManagementCallInfo{}, err
	}
	return managementCall(ctx, client, request, false, func(ctx context.Context, endpoint epochv1.RegionalAdminServiceClient, request *epochv1.GetOperationRequest) (*epochv1.GetOperationResponse, error) {
		return endpoint.GetOperation(ctx, request)
	}, validateManagementOperationResponse)
}
