package regional

import (
	"bytes"
	"context"
	"encoding/json"
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
	maxAuthorityResponseBytes = 1 << 20
	maxAuthorityBearerBytes   = 4 << 10
	regionalTopologyPath      = "/experimental/v1/regional/topology"
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
	target.Path = path
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
