package regional

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

const (
	maxAuthorityResponseBytes      = 1 << 20
	maxAuthorityBearerBytes        = 4 << 10
	regionalTopologyPath           = "/experimental/v1/regional/topology"
	regionalControlReconcilePath   = "/experimental/v1/regional/control/reconcile"
	regionalControlAllocationsPath = "/experimental/v1/regional/control/allocations"
	regionalControlMembershipPath  = "/experimental/v1/regional/control/tablets/"
)

// HTTPAuthority adapts the current Rust regional catalog and discovery routes
// to the Go reconciliation boundary.
type HTTPAuthority struct {
	endpoints   []*url.URL
	client      *http.Client
	bearerToken string
}

// NewHTTPAuthority validates an explicit regional-node allowlist. Redirects
// are never followed because a catalog node cannot delegate authority to an
// unconfigured host.
func NewHTTPAuthority(endpoints []string, client *http.Client) (*HTTPAuthority, error) {
	return newHTTPAuthority(endpoints, client, "")
}

// NewAuthenticatedHTTPAuthority constructs the managed-control adapter with a
// bootstrap workload credential. The raw credential is held only in memory
// and is never included in errors or response diagnostics.
func NewAuthenticatedHTTPAuthority(
	endpoints []string,
	client *http.Client,
	bearerToken string,
) (*HTTPAuthority, error) {
	if !validAuthorityBearer(bearerToken) {
		return nil, fmt.Errorf("regional authority bearer token is invalid")
	}
	return newHTTPAuthority(endpoints, client, bearerToken)
}

func newHTTPAuthority(
	endpoints []string,
	client *http.Client,
	bearerToken string,
) (*HTTPAuthority, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("regional authority requires at least one endpoint")
	}
	parsed := make([]*url.URL, 0, len(endpoints))
	seen := make(map[string]struct{}, len(endpoints))
	for _, raw := range endpoints {
		endpoint, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("invalid regional authority endpoint: %w", err)
		}
		if !validAuthorityEndpoint(endpoint) {
			return nil, fmt.Errorf(
				"regional authority endpoint must contain only an http(s) scheme and authority",
			)
		}
		endpoint.Path = ""
		canonical := endpoint.String()
		if _, exists := seen[canonical]; exists {
			return nil, fmt.Errorf("regional authority endpoints must be unique")
		}
		seen[canonical] = struct{}{}
		parsed = append(parsed, endpoint)
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	safeClient := *client
	safeClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPAuthority{
		endpoints:   parsed,
		client:      &safeClient,
		bearerToken: bearerToken,
	}, nil
}

func validAuthorityBearer(token string) bool {
	if token == "" || len(token) > maxAuthorityBearerBytes {
		return false
	}
	for _, character := range token {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func validAuthorityEndpoint(endpoint *url.URL) bool {
	return (endpoint.Scheme == "http" || endpoint.Scheme == "https") &&
		endpoint.Host != "" &&
		endpoint.User == nil &&
		(endpoint.Path == "" || endpoint.Path == "/") &&
		endpoint.RawPath == "" &&
		endpoint.RawQuery == "" &&
		endpoint.Fragment == ""
}

type applyAuthorityBody struct {
	RequestToken       string                        `json:"request_token"`
	ExpectedGeneration string                        `json:"expected_generation"`
	ShardCount         uint32                        `json:"shard_count"`
	ReplicaCount       uint16                        `json:"replica_count"`
	TabletPlacements   []TabletPlacement             `json:"tablet_placements"`
	Configuration      map[string]any                `json:"configuration,omitempty"`
	Governance         *resources.ResourceGovernance `json:"governance,omitempty"`
}

type deleteAuthorityBody struct {
	RequestToken       string `json:"request_token"`
	ExpectedGeneration string `json:"expected_generation"`
}

type planMembershipAuthorityBody struct {
	RequestToken               string   `json:"request_token"`
	ExpectedTabletEpoch        string   `json:"expected_tablet_epoch"`
	ExpectedResourceGeneration string   `json:"expected_resource_generation"`
	TargetVoterNodeIDs         []uint64 `json:"target_voter_node_ids"`
}

type managedReconcileAuthorityBody struct {
	RequestToken string                          `json:"request_token"`
	Lease        managedLeaseAuthorityBody       `json:"lease"`
	Capacity     []managedCapacityAuthorityBody  `json:"capacity"`
	Resources    []managedPlacementAuthorityBody `json:"resources"`
}

type managedMembershipAuthorityBody struct {
	RequestToken               string                         `json:"request_token"`
	Lease                      managedLeaseAuthorityBody      `json:"lease"`
	Capacity                   []managedCapacityAuthorityBody `json:"capacity"`
	Name                       controlResourceNameDocument    `json:"name"`
	ExpectedDesiredGeneration  string                         `json:"expected_desired_generation"`
	ExpectedTabletEpoch        string                         `json:"expected_tablet_epoch"`
	ExpectedResourceGeneration string                         `json:"expected_resource_generation"`
	TargetVoterNodeIDs         []uint64                       `json:"target_voter_node_ids"`
}

type managedLeaseAuthorityBody struct {
	OwnerID string `json:"owner_id"`
	Fence   string `json:"fence"`
	NowMS   string `json:"now_ms"`
}

type managedCapacityAuthorityBody struct {
	NodeID              string `json:"node_id"`
	MaxConsensusGroups  uint32 `json:"max_consensus_groups"`
	UsedConsensusGroups uint32 `json:"used_consensus_groups"`
	CatalogGroups       uint32 `json:"catalog_groups"`
}

type managedOperationMutationDocument struct {
	Kind    string `json:"kind"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type managedOperationDocument struct {
	RequestToken  string                            `json:"request_token"`
	State         ControlOperationState             `json:"state"`
	ResourceNames []controlResourceNameDocument     `json:"resource_names"`
	Mutation      *managedOperationMutationDocument `json:"mutation"`
}

type managedPlacementAuthorityBody struct {
	Name                      controlResourceNameDocument `json:"name"`
	ExpectedDesiredGeneration string                      `json:"expected_desired_generation"`
	ExpectedCatalogGeneration string                      `json:"expected_catalog_generation"`
	Spec                      managedNativeSpecDocument   `json:"spec"`
	TabletPlacements          []TabletPlacement           `json:"tablet_placements"`
}

type managedNativeSpecDocument struct {
	WorkloadProfile string                        `json:"workload_profile"`
	ShardCount      uint32                        `json:"shard_count"`
	ReplicaCount    uint16                        `json:"replica_count"`
	Configuration   map[string]any                `json:"configuration,omitempty"`
	Governance      *resources.ResourceGovernance `json:"governance,omitempty"`
}

type managedAllocationsDocument struct {
	Allocations []managedAllocationDocument `json:"allocations"`
}

type managedAllocationDocument struct {
	NodeID        decimalUint64 `json:"node_id"`
	CatalogGroups uint32        `json:"catalog_groups"`
}

type managedReconcileDocument struct {
	Mutation struct {
		Kind      string                    `json:"kind"`
		Resources []catalogResourceDocument `json:"resources"`
	} `json:"mutation"`
}

type catalogApplyDocument struct {
	Mutation struct {
		Kind     string                  `json:"kind"`
		Resource catalogResourceDocument `json:"resource"`
	} `json:"mutation"`
}

type catalogDeleteDocument struct {
	Mutation struct {
		Kind       string        `json:"kind"`
		Generation decimalUint64 `json:"generation"`
		Deleted    bool          `json:"deleted"`
	} `json:"mutation"`
}

type catalogResourceDocument struct {
	Generation   decimalUint64           `json:"generation"`
	ReplicaCount uint16                  `json:"replica_count"`
	Tablets      []catalogTabletDocument `json:"tablets"`
}

type catalogTabletDocument struct {
	TabletID           decimalUint64   `json:"tablet_id"`
	ConsensusGroupID   decimalUint64   `json:"consensus_group_id"`
	ShardIndex         uint32          `json:"shard_index"`
	TabletEpoch        decimalUint64   `json:"tablet_epoch"`
	ResourceGeneration decimalUint64   `json:"resource_generation"`
	ReplicaCount       uint16          `json:"replica_count"`
	VoterNodeIDs       []decimalUint64 `json:"voter_node_ids"`
	BootstrapVoterIDs  []decimalUint64 `json:"bootstrap_voter_node_ids"`
	TargetVoterNodeIDs []decimalUint64 `json:"target_voter_node_ids"`
}

type routeDocument struct {
	ResourceGeneration decimalUint64   `json:"resource_generation"`
	TabletID           decimalUint64   `json:"tablet_id"`
	ConsensusGroupID   decimalUint64   `json:"consensus_group_id"`
	TabletEpoch        decimalUint64   `json:"tablet_epoch"`
	LocalNodeID        decimalUint64   `json:"local_node_id"`
	LeaderNodeID       *decimalUint64  `json:"leader_node_id"`
	VoterNodeIDs       []decimalUint64 `json:"voter_node_ids"`
	AcceptsWrites      bool            `json:"accepts_writes"`
}

type topologyDocument struct {
	NodeID                decimalUint64    `json:"node_id"`
	Region                string           `json:"region"`
	Zone                  string           `json:"zone"`
	Rack                  string           `json:"rack"`
	NodeClass             string           `json:"node_class"`
	ConsensusVoterNodeIDs []decimalUint64  `json:"consensus_voter_node_ids"`
	Capacity              capacityDocument `json:"capacity"`
}

type capacityDocument struct {
	MaxConsensusGroups       uint32 `json:"max_consensus_groups"`
	UsedConsensusGroups      uint32 `json:"used_consensus_groups"`
	AvailableConsensusGroups uint32 `json:"available_consensus_groups"`
}

type decimalUint64 uint64

func (value *decimalUint64) UnmarshalJSON(encoded []byte) error {
	raw := strings.TrimSpace(string(encoded))
	if strings.HasPrefix(raw, `"`) {
		var text string
		if err := json.Unmarshal(encoded, &text); err != nil {
			return err
		}
		raw = text
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("expected a decimal u64: %w", err)
	}
	*value = decimalUint64(parsed)
	return nil
}

// Inventory samples every configured regional endpoint. A missing or malformed
// node fails the whole operation so admission never reasons from a partial
// failure-domain or capacity view.
func (authority *HTTPAuthority) Inventory(ctx context.Context) (NodeInventory, error) {
	nodes := make([]RegionalNode, 0, len(authority.endpoints))
	for _, endpoint := range authority.endpoints {
		response, status, err := authority.requestEndpoint(
			ctx,
			endpoint,
			http.MethodGet,
			regionalTopologyPath,
			nil,
		)
		if err != nil {
			return NodeInventory{}, availabilityError(
				"regional topology inventory is incomplete: " + err.Error(),
			)
		}
		if status < 200 || status >= 300 {
			return NodeInventory{}, availabilityError(
				"regional topology inventory is incomplete: " +
					authorityErrorMessage(response, status),
			)
		}
		var topology topologyDocument
		if err := decodeAuthorityJSON(response, &topology); err != nil {
			return NodeInventory{}, err
		}
		voters := make([]uint64, 0, len(topology.ConsensusVoterNodeIDs))
		for _, voter := range topology.ConsensusVoterNodeIDs {
			voters = append(voters, uint64(voter))
		}
		nodes = append(nodes, RegionalNode{
			NodeID:                   uint64(topology.NodeID),
			Region:                   topology.Region,
			Zone:                     topology.Zone,
			Rack:                     topologyRack(topology.Rack),
			NodeClass:                topology.NodeClass,
			ConsensusVoterNodeIDs:    voters,
			MaxConsensusGroups:       topology.Capacity.MaxConsensusGroups,
			UsedConsensusGroups:      topology.Capacity.UsedConsensusGroups,
			AvailableConsensusGroups: topology.Capacity.AvailableConsensusGroups,
		})
	}
	sort.Slice(nodes, func(left, right int) bool {
		return nodes[left].NodeID < nodes[right].NodeID
	})
	return NodeInventory{Nodes: nodes}, nil
}

func topologyRack(rack string) string {
	if rack != "" {
		return rack
	}
	// Nodes from the pre-rack rollout remain schedulable for the one-rack
	// default, but never masquerade as distinct rack failure domains.
	return "unassigned"
}

// Apply idempotently sends one desired generation to the first available
// catalog leader, then samples placement from every configured node.
func (authority *HTTPAuthority) Apply(
	ctx context.Context,
	request AuthorityApplyRequest,
) (AuthorityObservation, error) {
	body := applyAuthorityBody{
		RequestToken:       request.RequestToken,
		ExpectedGeneration: strconv.FormatUint(request.ExpectedGeneration, 10),
		ShardCount:         request.ShardCount,
		ReplicaCount:       request.ReplicaCount,
		TabletPlacements:   cloneTabletPlacements(request.TabletPlacements),
		Configuration:      request.Configuration,
		Governance:         request.Governance,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return AuthorityObservation{}, invalidAuthorityError(err.Error())
	}
	response, err := authority.requestAny(
		ctx,
		http.MethodPut,
		catalogResourcePath(request.Key),
		encoded,
	)
	if err != nil {
		return AuthorityObservation{}, err
	}
	var applied catalogApplyDocument
	if err := decodeAuthorityJSON(response, &applied); err != nil {
		return AuthorityObservation{}, err
	}
	if applied.Mutation.Kind != "applied" {
		return AuthorityObservation{}, invalidAuthorityError(
			"regional catalog apply response did not contain an applied resource",
		)
	}
	return authority.observePlacement(ctx, request.Key, applied.Mutation.Resource)
}

// ApplyManaged commits capacity reservation and native Catalog materialization
// in one lease-fenced command. A concurrent controller or stale inventory is
// rejected by the replicated state machine without partial tablet allocation.
func (authority *HTTPAuthority) ApplyManaged(
	ctx context.Context,
	request AuthorityManagedApplyRequest,
) (AuthorityObservation, error) {
	requestToken := managedApplyToken(request.RequestToken, request.Lease.Fence)
	if replayed, replayErr := authority.replayManagedOperation(
		ctx,
		requestToken,
		request.Key,
		"managed_reconciled",
	); replayErr != nil {
		return AuthorityObservation{}, replayErr
	} else if replayed {
		return authority.Observe(ctx, request.Key)
	}
	capacity, err := authority.managedCapacityObservation(ctx)
	if err != nil {
		return AuthorityObservation{}, err
	}
	body := managedReconcileAuthorityBody{
		RequestToken: requestToken,
		Lease: managedLeaseAuthorityBody{
			OwnerID: request.Lease.OwnerID,
			Fence:   strconv.FormatUint(request.Lease.Fence, 10),
			NowMS:   strconv.FormatUint(request.Lease.NowMS, 10),
		},
		Capacity: capacity,
		Resources: []managedPlacementAuthorityBody{{
			Name:                      controlName(request.Key),
			ExpectedDesiredGeneration: strconv.FormatUint(request.DesiredGeneration, 10),
			ExpectedCatalogGeneration: strconv.FormatUint(request.ExpectedGeneration, 10),
			Spec: managedNativeSpecDocument{
				WorkloadProfile: profileName(request.Key.Kind),
				ShardCount:      request.ShardCount,
				ReplicaCount:    request.ReplicaCount,
				Configuration:   request.Configuration,
				Governance:      request.Governance,
			},
			TabletPlacements: cloneTabletPlacements(request.TabletPlacements),
		}},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return AuthorityObservation{}, invalidAuthorityError(err.Error())
	}
	response, err := authority.requestAny(
		ctx,
		http.MethodPost,
		regionalControlReconcilePath,
		encoded,
	)
	if err != nil {
		if replayed, replayErr := authority.replayManagedOperation(
			ctx,
			requestToken,
			request.Key,
			"managed_reconciled",
		); replayErr != nil {
			return AuthorityObservation{}, replayErr
		} else if replayed {
			return authority.Observe(ctx, request.Key)
		}
		return AuthorityObservation{}, err
	}
	var reconciled managedReconcileDocument
	if err := decodeAuthorityJSON(response, &reconciled); err != nil {
		return AuthorityObservation{}, err
	}
	if reconciled.Mutation.Kind != "managed_reconciled" ||
		len(reconciled.Mutation.Resources) != 1 {
		return AuthorityObservation{}, invalidAuthorityError(
			"regional managed reconcile response did not contain exactly one resource",
		)
	}
	return authority.observePlacement(ctx, request.Key, reconciled.Mutation.Resources[0])
}

// PlanManagedMembership reserves the transition's learner capacity and
// commits its target under the same controller fence as managed apply.
func (authority *HTTPAuthority) PlanManagedMembership(
	ctx context.Context,
	request AuthorityManagedMembershipPlanRequest,
) (AuthorityObservation, error) {
	requestToken := managedMembershipToken(request.RequestToken, request.Lease.Fence)
	if replayed, replayErr := authority.replayManagedOperation(
		ctx,
		requestToken,
		request.Key,
		"applied",
	); replayErr != nil {
		return AuthorityObservation{}, replayErr
	} else if replayed {
		return authority.Observe(ctx, request.Key)
	}
	capacity, err := authority.managedCapacityObservation(ctx)
	if err != nil {
		return AuthorityObservation{}, err
	}
	body := managedMembershipAuthorityBody{
		RequestToken: requestToken,
		Lease: managedLeaseAuthorityBody{
			OwnerID: request.Lease.OwnerID,
			Fence:   strconv.FormatUint(request.Lease.Fence, 10),
			NowMS:   strconv.FormatUint(request.Lease.NowMS, 10),
		},
		Capacity:                   capacity,
		Name:                       controlName(request.Key),
		ExpectedDesiredGeneration:  strconv.FormatUint(request.DesiredGeneration, 10),
		ExpectedTabletEpoch:        strconv.FormatUint(request.ExpectedTabletEpoch, 10),
		ExpectedResourceGeneration: strconv.FormatUint(request.ExpectedResourceGeneration, 10),
		TargetVoterNodeIDs:         append([]uint64(nil), request.TargetVoterNodeIDs...),
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return AuthorityObservation{}, invalidAuthorityError(err.Error())
	}
	path := regionalControlMembershipPath + strconv.FormatUint(request.TabletID, 10) + "/membership"
	response, err := authority.requestAny(ctx, http.MethodPost, path, encoded)
	if err != nil {
		if replayed, replayErr := authority.replayManagedOperation(
			ctx,
			requestToken,
			request.Key,
			"applied",
		); replayErr != nil {
			return AuthorityObservation{}, replayErr
		} else if replayed {
			return authority.Observe(ctx, request.Key)
		}
		return AuthorityObservation{}, err
	}
	var planned catalogApplyDocument
	if err := decodeAuthorityJSON(response, &planned); err != nil {
		return AuthorityObservation{}, err
	}
	if planned.Mutation.Kind != "applied" {
		return AuthorityObservation{}, invalidAuthorityError(
			"regional managed membership response did not contain an applied resource",
		)
	}
	return authority.observePlacement(ctx, request.Key, planned.Mutation.Resource)
}

func (authority *HTTPAuthority) managedCapacityObservation(
	ctx context.Context,
) ([]managedCapacityAuthorityBody, error) {
	inventory, err := authority.Inventory(ctx)
	if err != nil {
		return nil, err
	}
	allocationResponse, err := authority.readControlAny(ctx, regionalControlAllocationsPath)
	if err != nil {
		return nil, err
	}
	var allocations managedAllocationsDocument
	if err := decodeAuthorityJSON(allocationResponse, &allocations); err != nil {
		return nil, err
	}
	byNode := make(map[uint64]uint32, len(allocations.Allocations))
	for _, allocation := range allocations.Allocations {
		nodeID := uint64(allocation.NodeID)
		if nodeID == 0 {
			return nil, invalidAuthorityError(
				"regional Catalog returned a zero capacity-allocation node",
			)
		}
		if _, duplicate := byNode[nodeID]; duplicate {
			return nil, invalidAuthorityError(
				"regional Catalog returned duplicate capacity allocations",
			)
		}
		byNode[nodeID] = allocation.CatalogGroups
	}
	capacity := make([]managedCapacityAuthorityBody, 0, len(inventory.Nodes))
	for _, node := range inventory.Nodes {
		catalogGroups := byNode[node.NodeID]
		if catalogGroups > node.UsedConsensusGroups {
			return nil, invalidAuthorityError(
				"regional topology used capacity trails Catalog allocations",
			)
		}
		capacity = append(capacity, managedCapacityAuthorityBody{
			NodeID:              strconv.FormatUint(node.NodeID, 10),
			MaxConsensusGroups:  node.MaxConsensusGroups,
			UsedConsensusGroups: node.UsedConsensusGroups,
			CatalogGroups:       catalogGroups,
		})
		delete(byNode, node.NodeID)
	}
	if len(byNode) != 0 {
		return nil, invalidAuthorityError(
			"regional topology omits a node with Catalog allocations",
		)
	}
	return capacity, nil
}

func profileName(kind resources.Kind) string {
	switch kind {
	case resources.KindCache, resources.KindTable:
		return "cache_and_state"
	case resources.KindStream:
		return "stream_log"
	case resources.KindQueue:
		return "work_queue"
	case resources.KindEventBus:
		return "event_bus"
	default:
		return ""
	}
}

func managedApplyToken(base string, fence uint64) string {
	digest := sha256.Sum256([]byte(base + "\x00" + strconv.FormatUint(fence, 10)))
	return "epoch-control.reconcile.v1." + hex.EncodeToString(digest[:])
}

func managedMembershipToken(base string, fence uint64) string {
	digest := sha256.Sum256([]byte(base + "\x00" + strconv.FormatUint(fence, 10)))
	return "epoch-control.membership.v1." + hex.EncodeToString(digest[:])
}

// replayManagedOperation resolves a previously submitted semantic mutation
// before a controller recomputes volatile lease-time or capacity evidence.
// Catalog proposal identity is token-derived, so a pending or completed token
// must never be submitted again with newly sampled command bytes.
func (authority *HTTPAuthority) replayManagedOperation(
	ctx context.Context,
	requestToken string,
	key resources.ResourceKey,
	expectedMutationKind string,
) (bool, error) {
	response, err := authority.readControlAny(
		ctx,
		controlOperationsPath+"/"+url.PathEscape(requestToken),
	)
	if errors.Is(err, errManagedResourceNotFound) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	var operation managedOperationDocument
	if err := decodeAuthorityJSON(response, &operation); err != nil {
		return true, err
	}
	if operation.RequestToken != requestToken || !validControlOperationState(operation.State) {
		return true, invalidAuthorityError(
			"regional Catalog returned an inconsistent managed operation",
		)
	}
	if operation.State == ControlOperationPending {
		return true, availabilityError("regional Catalog operation is still pending")
	}
	if len(operation.ResourceNames) != 1 {
		return true, invalidAuthorityError(
			"regional Catalog operation omitted its exact resource identity",
		)
	}
	operationKey, keyErr := keyFromControlName(operation.ResourceNames[0])
	if keyErr != nil || operationKey != key {
		return true, conflictError(
			"regional Catalog request token is bound to a different resource",
		)
	}
	if operation.Mutation == nil {
		return true, invalidAuthorityError(
			"regional Catalog operation omitted its mutation outcome",
		)
	}
	if operation.State == ControlOperationFailed {
		if operation.Mutation.Kind != "rejected" || operation.Mutation.Message == "" {
			return true, invalidAuthorityError(
				"regional Catalog returned an inconsistent managed rejection",
			)
		}
		if operation.Mutation.Code == "invalid_argument" {
			return true, invalidAuthorityError(operation.Mutation.Message)
		}
		return true, conflictError(operation.Mutation.Message)
	}
	if operation.Mutation.Kind != expectedMutationKind {
		return true, invalidAuthorityError(
			"regional Catalog returned an unexpected managed mutation outcome",
		)
	}
	return true, nil
}

// Observe reads current catalog identity and samples placement without
// resubmitting an already-observed desired generation.
func (authority *HTTPAuthority) Observe(
	ctx context.Context,
	key resources.ResourceKey,
) (AuthorityObservation, error) {
	response, err := authority.requestAny(
		ctx,
		http.MethodGet,
		catalogResourcePath(key),
		nil,
	)
	if err != nil {
		return AuthorityObservation{}, err
	}
	var resource catalogResourceDocument
	if err := decodeAuthorityJSON(response, &resource); err != nil {
		return AuthorityObservation{}, err
	}
	return authority.observePlacement(ctx, key, resource)
}

// PlanMembership commits one fenced learner-first target through the catalog
// leader and returns the same truthful all-endpoint placement observation used
// by ordinary reconciliation.
func (authority *HTTPAuthority) PlanMembership(
	ctx context.Context,
	request AuthorityMembershipPlanRequest,
) (AuthorityObservation, error) {
	encoded, err := json.Marshal(planMembershipAuthorityBody{
		RequestToken:               request.RequestToken,
		ExpectedTabletEpoch:        strconv.FormatUint(request.ExpectedTabletEpoch, 10),
		ExpectedResourceGeneration: strconv.FormatUint(request.ExpectedResourceGeneration, 10),
		TargetVoterNodeIDs:         append([]uint64(nil), request.TargetVoterNodeIDs...),
	})
	if err != nil {
		return AuthorityObservation{}, invalidAuthorityError(err.Error())
	}
	response, err := authority.requestAny(
		ctx,
		http.MethodPost,
		catalogTabletMembershipPath(request.TabletID),
		encoded,
	)
	if err != nil {
		return AuthorityObservation{}, err
	}
	var planned catalogApplyDocument
	if err := decodeAuthorityJSON(response, &planned); err != nil {
		return AuthorityObservation{}, err
	}
	if planned.Mutation.Kind != "applied" {
		return AuthorityObservation{}, invalidAuthorityError(
			"regional catalog membership response did not contain an applied resource",
		)
	}
	return authority.observePlacement(ctx, request.Key, planned.Mutation.Resource)
}

// Delete persists a catalog tombstone through the first available leader.
func (authority *HTTPAuthority) Delete(
	ctx context.Context,
	request AuthorityDeleteRequest,
) (AuthorityDeleteObservation, error) {
	encoded, err := json.Marshal(deleteAuthorityBody{
		RequestToken:       request.RequestToken,
		ExpectedGeneration: strconv.FormatUint(request.ExpectedGeneration, 10),
	})
	if err != nil {
		return AuthorityDeleteObservation{}, invalidAuthorityError(err.Error())
	}
	response, err := authority.requestAny(
		ctx,
		http.MethodDelete,
		catalogResourcePath(request.Key),
		encoded,
	)
	if err != nil {
		return AuthorityDeleteObservation{}, err
	}
	var deleted catalogDeleteDocument
	if err := decodeAuthorityJSON(response, &deleted); err != nil {
		return AuthorityDeleteObservation{}, err
	}
	if deleted.Mutation.Kind != "deleted" {
		return AuthorityDeleteObservation{}, invalidAuthorityError(
			"regional catalog delete response did not contain a tombstone",
		)
	}
	return AuthorityDeleteObservation{
		Generation: uint64(deleted.Mutation.Generation),
		Deleted:    deleted.Mutation.Deleted,
	}, nil
}

func (authority *HTTPAuthority) observePlacement(
	ctx context.Context,
	key resources.ResourceKey,
	resource catalogResourceDocument,
) (AuthorityObservation, error) {
	tablets := make([]resources.TabletStatus, len(resource.Tablets))
	for index, catalogTablet := range resource.Tablets {
		tablet := resources.TabletStatus{
			TabletID:           uint64(catalogTablet.TabletID),
			ConsensusGroupID:   uint64(catalogTablet.ConsensusGroupID),
			ShardIndex:         catalogTablet.ShardIndex,
			TabletEpoch:        uint64(catalogTablet.TabletEpoch),
			ResourceGeneration: uint64(catalogTablet.ResourceGeneration),
			DesiredReplicas:    uint32(catalogTablet.ReplicaCount),
		}
		for _, nodeID := range catalogTablet.VoterNodeIDs {
			tablet.AssignedNodeIDs = append(tablet.AssignedNodeIDs, uint64(nodeID))
		}
		for _, nodeID := range catalogTablet.BootstrapVoterIDs {
			tablet.BootstrapVoterNodeIDs = append(
				tablet.BootstrapVoterNodeIDs,
				uint64(nodeID),
			)
		}
		for _, nodeID := range catalogTablet.TargetVoterNodeIDs {
			tablet.TargetVoterNodeIDs = append(tablet.TargetVoterNodeIDs, uint64(nodeID))
		}
		leaders := make(map[uint64]struct{})
		reachable := make(map[uint64]struct{})
		var committed []uint64
		membershipConsistent := true
		for _, endpoint := range authority.endpoints {
			route, ok := authority.observeRoute(ctx, endpoint, key, tablet)
			if !ok {
				continue
			}
			routeVoters := make([]uint64, 0, len(route.VoterNodeIDs))
			for _, nodeID := range route.VoterNodeIDs {
				routeVoters = append(routeVoters, uint64(nodeID))
			}
			if !matchesRequestedVoterSet(routeVoters, tablet.DesiredReplicas) ||
				!slices.Contains(routeVoters, uint64(route.LocalNodeID)) {
				continue
			}
			if committed == nil {
				committed = routeVoters
			} else if !slices.Equal(committed, routeVoters) {
				membershipConsistent = false
			}
			reachable[uint64(route.LocalNodeID)] = struct{}{}
			if route.LeaderNodeID != nil {
				leaders[uint64(*route.LeaderNodeID)] = struct{}{}
			}
		}
		if membershipConsistent {
			tablet.VoterNodeIDs = append(tablet.VoterNodeIDs, committed...)
		}
		if len(tablet.AssignedNodeIDs) == 0 {
			tablet.AssignedNodeIDs = append(tablet.AssignedNodeIDs, tablet.VoterNodeIDs...)
		}
		for voter := range reachable {
			tablet.ReachableVoterNodeIDs = append(tablet.ReachableVoterNodeIDs, voter)
		}
		sort.Slice(tablet.ReachableVoterNodeIDs, func(left, right int) bool {
			return tablet.ReachableVoterNodeIDs[left] < tablet.ReachableVoterNodeIDs[right]
		})
		if len(leaders) == 1 {
			for leader := range leaders {
				if _, observed := reachable[leader]; observed && slices.Contains(tablet.VoterNodeIDs, leader) {
					tablet.LeaderNodeID = leader
				}
			}
		}
		tablets[index] = tablet
	}
	sort.Slice(tablets, func(left, right int) bool {
		return tablets[left].ShardIndex < tablets[right].ShardIndex
	})
	return AuthorityObservation{
		Generation: uint64(resource.Generation),
		Tablets:    tablets,
	}, nil
}

func matchesRequestedVoterSet(voters []uint64, replicas uint32) bool {
	return len(voters) == int(replicas) &&
		len(voters) > 0 &&
		voters[0] != 0 &&
		slices.IsSorted(voters) &&
		!hasAdjacentDuplicate(voters)
}

func (authority *HTTPAuthority) observeRoute(
	ctx context.Context,
	endpoint *url.URL,
	key resources.ResourceKey,
	expected resources.TabletStatus,
) (routeDocument, bool) {
	response, status, err := authority.requestEndpoint(
		ctx,
		endpoint,
		http.MethodGet,
		resourceRoutePath(key, expected.ShardIndex),
		nil,
	)
	if err != nil || status != http.StatusOK {
		return routeDocument{}, false
	}
	var route routeDocument
	if decodeAuthorityJSON(response, &route) != nil {
		return routeDocument{}, false
	}
	if uint64(route.ResourceGeneration) != expected.ResourceGeneration ||
		uint64(route.TabletID) != expected.TabletID ||
		uint64(route.ConsensusGroupID) != expected.ConsensusGroupID ||
		uint64(route.TabletEpoch) != expected.TabletEpoch ||
		uint64(route.LocalNodeID) == 0 {
		return routeDocument{}, false
	}
	return route, true
}

func (authority *HTTPAuthority) requestAny(
	ctx context.Context,
	method string,
	path string,
	body []byte,
) ([]byte, error) {
	var failures []string
	for _, endpoint := range authority.endpoints {
		response, status, err := authority.requestEndpoint(ctx, endpoint, method, path, body)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		switch {
		case status >= 200 && status < 300:
			return response, nil
		case status == http.StatusConflict && authorityErrorCode(response) == "not_leader":
			failures = append(failures, authorityErrorMessage(response, status))
		case status == http.StatusConflict:
			return nil, conflictError(authorityErrorMessage(response, status))
		case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
			return nil, invalidAuthorityError(authorityErrorMessage(response, status))
		case status == http.StatusNotFound:
			return nil, invalidAuthorityError(authorityErrorMessage(response, status))
		case status >= 500 || status == http.StatusTooManyRequests:
			failures = append(failures, authorityErrorMessage(response, status))
		default:
			return nil, invalidAuthorityError(authorityErrorMessage(response, status))
		}
	}
	return nil, availabilityError(
		"no regional authority endpoint completed the request: " + strings.Join(failures, "; "),
	)
}

func authorityErrorCode(encoded []byte) string {
	var body struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(encoded, &body) != nil {
		return ""
	}
	return body.Code
}

func (authority *HTTPAuthority) requestEndpoint(
	ctx context.Context,
	endpoint *url.URL,
	method string,
	path string,
	body []byte,
) ([]byte, int, error) {
	target := *endpoint
	target.Path, target.RawQuery, _ = strings.Cut(path, "?")
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), reader)
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("accept", "application/json")
	if authority.bearerToken != "" {
		request.Header.Set("authorization", "Bearer "+authority.bearerToken)
	}
	if body != nil {
		request.Header.Set("content-type", "application/json")
	}
	response, err := authority.client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maxAuthorityResponseBytes+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return nil, 0, err
	}
	if len(encoded) > maxAuthorityResponseBytes {
		return nil, 0, fmt.Errorf("regional authority response exceeded %d bytes", maxAuthorityResponseBytes)
	}
	return encoded, response.StatusCode, nil
}

func decodeAuthorityJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := decoder.Decode(target); err != nil {
		return invalidAuthorityError("regional authority returned invalid JSON: " + err.Error())
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return invalidAuthorityError("regional authority returned trailing JSON")
	}
	return nil
}

func authorityErrorMessage(encoded []byte, status int) string {
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal(encoded, &body) == nil && body.Message != "" {
		return fmt.Sprintf("regional authority HTTP %d %s: %s", status, body.Code, body.Message)
	}
	return fmt.Sprintf("regional authority returned HTTP %d", status)
}

func catalogResourcePath(key resources.ResourceKey) string {
	return "/experimental/v1/regional/catalog/resources/" + resourceSegments(key)
}

func catalogTabletMembershipPath(tabletID uint64) string {
	return "/experimental/v1/regional/catalog/tablets/" +
		strconv.FormatUint(tabletID, 10) +
		"/membership"
}

func resourceRoutePath(key resources.ResourceKey, shard uint32) string {
	return "/experimental/v1/regional/resources/" +
		resourceSegments(key) +
		"/shards/" +
		strconv.FormatUint(uint64(shard), 10)
}

func resourceSegments(key resources.ResourceKey) string {
	return strings.Join([]string{
		url.PathEscape(key.Organization),
		url.PathEscape(key.Project),
		url.PathEscape(key.Environment),
		url.PathEscape(key.Namespace),
		url.PathEscape(authorityKind(key.Kind)),
		url.PathEscape(key.Name),
	}, "/")
}

func authorityKind(kind resources.Kind) string {
	if kind == resources.KindEventBus {
		return "event-bus"
	}
	return string(kind)
}
