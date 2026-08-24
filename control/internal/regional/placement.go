package regional

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

const (
	maxTopologyLabelBytes = 63
	maxAllowedRegions     = 16
	maxRegionalNodes      = 1024
)

// PlacementPolicy is the bounded failure-domain request carried by desired
// resource state.
type PlacementPolicy struct {
	AllowedRegions    []string `json:"allowed_regions,omitempty"`
	MinimumZones      uint32   `json:"minimum_zones,omitempty"`
	RequiredNodeClass string   `json:"required_node_class,omitempty"`
}

// RegionalNode is one policy-protected physical-node topology and capacity
// sample. ConsensusVoterNodeIDs identifies the bounded catalog group; data
// tablet voter assignments are selected independently from the full inventory.
type RegionalNode struct {
	NodeID                   uint64   `json:"node_id"`
	Region                   string   `json:"region"`
	Zone                     string   `json:"zone"`
	NodeClass                string   `json:"node_class"`
	ConsensusVoterNodeIDs    []uint64 `json:"consensus_voter_node_ids"`
	MaxConsensusGroups       uint32   `json:"max_consensus_groups"`
	UsedConsensusGroups      uint32   `json:"used_consensus_groups"`
	AvailableConsensusGroups uint32   `json:"available_consensus_groups"`
}

// NodeInventory contains one current sample for every configured regional
// endpoint. Partial inventories never pass admission.
type NodeInventory struct {
	Nodes []RegionalNode `json:"nodes"`
}

// TabletPlacement is one complete, deterministic shard voter assignment.
type TabletPlacement struct {
	ShardIndex   uint32   `json:"shard_index"`
	VoterNodeIDs []uint64 `json:"voter_node_ids"`
}

// PlacementDecision records all concrete shard voters and per-node capacity
// reservations made by one successful admission.
type PlacementDecision struct {
	TabletPlacements       []TabletPlacement
	EligibleNodeIDs        []uint64
	AchievedZones          uint32
	AdditionalGroupsByNode map[uint64]uint32
	Nodes                  []RegionalNode
	Policy                 PlacementPolicy
}

// AdmissionCode is a stable, machine-readable precondition failure.
type AdmissionCode string

const (
	AdmissionInvalidRequest         AdmissionCode = "invalid_placement_request"
	AdmissionInconsistentInventory  AdmissionCode = "inconsistent_regional_inventory"
	AdmissionFixedVotersIneligible  AdmissionCode = "eligible_nodes_insufficient"
	AdmissionInsufficientZones      AdmissionCode = "insufficient_zones"
	AdmissionConsensusGroupCapacity AdmissionCode = "consensus_group_capacity"
)

// AdmissionError names the limiting node and capacity where applicable.
type AdmissionError struct {
	Code           AdmissionCode
	Message        string
	LimitingNodeID uint64
	Required       uint32
	Available      uint32
}

func (err *AdmissionError) Error() string {
	return fmt.Sprintf("placement admission %s: %s", err.Code, err.Message)
}

// AdmitPlacement validates requested constraints against an N-node physical
// inventory and assigns exactly three or five voters to every desired shard.
// Existing assignments are preserved and charged by the live inventory; only
// newly allocated groups consume additional advertised capacity.
func AdmitPlacement(
	policy PlacementPolicy,
	replicas uint32,
	desiredShards uint32,
	existing []TabletPlacement,
	inventory NodeInventory,
) (PlacementDecision, error) {
	nodes, err := validateInventory(inventory, replicas)
	if err != nil {
		return PlacementDecision{}, err
	}
	if desiredShards == 0 || uint32(len(existing)) > desiredShards {
		return PlacementDecision{}, admissionError(
			AdmissionInvalidRequest,
			"desired shard count must be non-zero and cannot shrink the observed shard set",
		)
	}
	policy, err = normalizePlacementPolicy(policy, replicas)
	if err != nil {
		return PlacementDecision{}, err
	}

	allowedRegions := make(map[string]struct{}, len(policy.AllowedRegions))
	for _, region := range policy.AllowedRegions {
		allowedRegions[region] = struct{}{}
	}
	eligible := make([]RegionalNode, 0, len(nodes))
	for _, node := range nodes {
		if len(allowedRegions) > 0 {
			if _, allowed := allowedRegions[node.Region]; !allowed {
				continue
			}
		}
		if policy.RequiredNodeClass != "" && node.NodeClass != policy.RequiredNodeClass {
			continue
		}
		eligible = append(eligible, node)
	}
	if len(eligible) < int(replicas) {
		return PlacementDecision{}, admissionError(
			AdmissionFixedVotersIneligible,
			fmt.Sprintf("%d eligible nodes cannot fill a %d-voter tablet group", len(eligible), replicas),
		)
	}
	zones := make(map[string]struct{}, len(eligible))
	for _, node := range eligible {
		zones[node.Zone] = struct{}{}
	}
	if uint32(len(zones)) < policy.MinimumZones {
		return PlacementDecision{}, admissionError(
			AdmissionInsufficientZones,
			fmt.Sprintf(
				"eligible nodes span %d zones; %d are required",
				len(zones),
				policy.MinimumZones,
			),
		)
	}

	eligibleByID := make(map[uint64]RegionalNode, len(eligible))
	eligibleIDs := make([]uint64, 0, len(eligible))
	for _, node := range eligible {
		eligibleByID[node.NodeID] = node
		eligibleIDs = append(eligibleIDs, node.NodeID)
	}
	placements, err := validateExistingPlacements(
		existing,
		desiredShards,
		replicas,
		policy.MinimumZones,
		eligibleByID,
	)
	if err != nil {
		return PlacementDecision{}, err
	}
	additionalByNode := make(map[uint64]uint32, len(eligible))
	for shard := uint32(len(existing)); shard < desiredShards; shard++ {
		placement, placementErr := selectTabletVoters(
			shard,
			replicas,
			policy.MinimumZones,
			eligible,
			additionalByNode,
		)
		if placementErr != nil {
			return PlacementDecision{}, placementErr
		}
		placements = append(placements, placement)
		for _, nodeID := range placement.VoterNodeIDs {
			additionalByNode[nodeID]++
		}
	}
	return PlacementDecision{
		TabletPlacements:       cloneTabletPlacements(placements),
		EligibleNodeIDs:        eligibleIDs,
		AchievedZones:          minimumPlacementZones(placements, eligibleByID),
		AdditionalGroupsByNode: additionalByNode,
		Nodes:                  cloneRegionalNodes(nodes),
		Policy:                 policy,
	}, nil
}

func validateInventory(inventory NodeInventory, replicas uint32) ([]RegionalNode, error) {
	if replicas != 3 && replicas != 5 {
		return nil, admissionError(
			AdmissionInvalidRequest,
			"tablet replica count must be exactly 3 or 5",
		)
	}
	if len(inventory.Nodes) < int(replicas) || len(inventory.Nodes) > maxRegionalNodes {
		return nil, admissionError(
			AdmissionInconsistentInventory,
			fmt.Sprintf(
				"regional inventory requires %d..=%d complete node samples; observed %d",
				replicas,
				maxRegionalNodes,
				len(inventory.Nodes),
			),
		)
	}
	nodes := cloneRegionalNodes(inventory.Nodes)
	sort.Slice(nodes, func(left, right int) bool {
		return nodes[left].NodeID < nodes[right].NodeID
	})
	nodeIDs := make([]uint64, 0, len(nodes))
	for index, node := range nodes {
		if node.NodeID == 0 || (index > 0 && nodes[index-1].NodeID == node.NodeID) {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				"regional node IDs must be distinct and non-zero",
			)
		}
		if !validTopologyLabel(node.Region) ||
			!validTopologyLabel(node.Zone) ||
			!validTopologyLabel(node.NodeClass) {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				fmt.Sprintf("node %d returned an invalid topology label", node.NodeID),
			)
		}
		if node.MaxConsensusGroups == 0 ||
			node.UsedConsensusGroups > node.MaxConsensusGroups ||
			node.AvailableConsensusGroups != node.MaxConsensusGroups-node.UsedConsensusGroups {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				fmt.Sprintf("node %d returned inconsistent group capacity", node.NodeID),
			)
		}
		nodeIDs = append(nodeIDs, node.NodeID)
	}
	var catalogVoters []uint64
	for _, node := range nodes {
		voters := append([]uint64(nil), node.ConsensusVoterNodeIDs...)
		if !matchesBoundedVoterSet(voters) {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				fmt.Sprintf("node %d returned an invalid catalog voter set", node.NodeID),
			)
		}
		if catalogVoters == nil {
			catalogVoters = voters
		}
		if !slices.Equal(voters, catalogVoters) {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				fmt.Sprintf("node %d returned a different catalog voter set", node.NodeID),
			)
		}
	}
	for _, voter := range catalogVoters {
		if !slices.Contains(nodeIDs, voter) {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				fmt.Sprintf("catalog voter %d is absent from regional inventory", voter),
			)
		}
	}
	return nodes, nil
}

func matchesBoundedVoterSet(voters []uint64) bool {
	return (len(voters) == 3 || len(voters) == 5) &&
		voters[0] != 0 &&
		slices.IsSorted(voters) &&
		!hasAdjacentDuplicate(voters)
}

func hasAdjacentDuplicate(values []uint64) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}

func validateExistingPlacements(
	existing []TabletPlacement,
	desiredShards uint32,
	replicas uint32,
	minimumZones uint32,
	eligible map[uint64]RegionalNode,
) ([]TabletPlacement, error) {
	placements := cloneTabletPlacements(existing)
	for index, placement := range placements {
		if placement.ShardIndex != uint32(index) || placement.ShardIndex >= desiredShards {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				"existing tablet placements must be contiguous and ordered by shard index",
			)
		}
		if len(placement.VoterNodeIDs) != int(replicas) ||
			!slices.IsSorted(placement.VoterNodeIDs) ||
			hasAdjacentDuplicate(placement.VoterNodeIDs) {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				fmt.Sprintf("existing shard %d has an invalid voter assignment", placement.ShardIndex),
			)
		}
		zones := make(map[string]struct{}, len(placement.VoterNodeIDs))
		for _, nodeID := range placement.VoterNodeIDs {
			node, ok := eligible[nodeID]
			if !ok {
				return nil, admissionError(
					AdmissionFixedVotersIneligible,
					fmt.Sprintf("existing shard %d voter %d is no longer eligible", placement.ShardIndex, nodeID),
				)
			}
			zones[node.Zone] = struct{}{}
		}
		if uint32(len(zones)) < minimumZones {
			return nil, admissionError(
				AdmissionInsufficientZones,
				fmt.Sprintf("existing shard %d spans %d zones; %d are required", placement.ShardIndex, len(zones), minimumZones),
			)
		}
	}
	return placements, nil
}

func selectTabletVoters(
	shard uint32,
	replicas uint32,
	minimumZones uint32,
	nodes []RegionalNode,
	additional map[uint64]uint32,
) (TabletPlacement, error) {
	selected := make(map[uint64]struct{}, replicas)
	selectedZones := make(map[string]struct{}, replicas)
	voters := make([]uint64, 0, replicas)
	for uint32(len(voters)) < replicas {
		requireNewZone := uint32(len(selectedZones)) < minimumZones
		candidates := make([]RegionalNode, 0, len(nodes))
		for _, node := range nodes {
			if _, alreadySelected := selected[node.NodeID]; alreadySelected {
				continue
			}
			if additional[node.NodeID] >= node.AvailableConsensusGroups {
				continue
			}
			if requireNewZone {
				if _, zoneUsed := selectedZones[node.Zone]; zoneUsed {
					continue
				}
			}
			candidates = append(candidates, node)
		}
		if len(candidates) == 0 {
			limiting := limitingCapacityNode(nodes, selected, selectedZones, requireNewZone, additional)
			return TabletPlacement{}, &AdmissionError{
				Code:           AdmissionConsensusGroupCapacity,
				Message:        fmt.Sprintf("no eligible node has capacity for shard %d voter %d", shard, len(voters)+1),
				LimitingNodeID: limiting.NodeID,
				Required:       additional[limiting.NodeID] + 1,
				Available:      limiting.AvailableConsensusGroups,
			}
		}
		sort.Slice(candidates, func(left, right int) bool {
			return placementNodeLess(candidates[left], candidates[right], additional)
		})
		chosen := candidates[0]
		selected[chosen.NodeID] = struct{}{}
		selectedZones[chosen.Zone] = struct{}{}
		voters = append(voters, chosen.NodeID)
	}
	slices.Sort(voters)
	return TabletPlacement{ShardIndex: shard, VoterNodeIDs: voters}, nil
}

func placementNodeLess(left, right RegionalNode, additional map[uint64]uint32) bool {
	leftAdded := additional[left.NodeID]
	rightAdded := additional[right.NodeID]
	if leftAdded != rightAdded {
		return leftAdded < rightAdded
	}
	leftProjected := uint64(left.UsedConsensusGroups + leftAdded)
	rightProjected := uint64(right.UsedConsensusGroups + rightAdded)
	leftScaled := leftProjected * uint64(right.MaxConsensusGroups)
	rightScaled := rightProjected * uint64(left.MaxConsensusGroups)
	if leftScaled != rightScaled {
		return leftScaled < rightScaled
	}
	return left.NodeID < right.NodeID
}

func limitingCapacityNode(
	nodes []RegionalNode,
	selected map[uint64]struct{},
	selectedZones map[string]struct{},
	requireNewZone bool,
	additional map[uint64]uint32,
) RegionalNode {
	var limiting RegionalNode
	for _, node := range nodes {
		if _, alreadySelected := selected[node.NodeID]; alreadySelected {
			continue
		}
		if requireNewZone {
			if _, zoneUsed := selectedZones[node.Zone]; zoneUsed {
				continue
			}
		}
		if limiting.NodeID == 0 || placementNodeLess(node, limiting, additional) {
			limiting = node
		}
	}
	return limiting
}

func minimumPlacementZones(placements []TabletPlacement, nodes map[uint64]RegionalNode) uint32 {
	minimum := uint32(0)
	for _, placement := range placements {
		zones := make(map[string]struct{}, len(placement.VoterNodeIDs))
		for _, nodeID := range placement.VoterNodeIDs {
			zones[nodes[nodeID].Zone] = struct{}{}
		}
		count := uint32(len(zones))
		if minimum == 0 || count < minimum {
			minimum = count
		}
	}
	return minimum
}

func cloneTabletPlacements(placements []TabletPlacement) []TabletPlacement {
	cloned := append([]TabletPlacement(nil), placements...)
	for index := range cloned {
		cloned[index].VoterNodeIDs = append([]uint64(nil), cloned[index].VoterNodeIDs...)
	}
	return cloned
}

func normalizePlacementPolicy(
	policy PlacementPolicy,
	replicas uint32,
) (PlacementPolicy, error) {
	if policy.MinimumZones == 0 {
		policy.MinimumZones = 1
	}
	if policy.MinimumZones > replicas {
		return PlacementPolicy{}, admissionError(
			AdmissionInvalidRequest,
			"minimum zones cannot exceed the requested replica count",
		)
	}
	if policy.RequiredNodeClass != "" && !validTopologyLabel(policy.RequiredNodeClass) {
		return PlacementPolicy{}, admissionError(
			AdmissionInvalidRequest,
			"required node class is invalid",
		)
	}
	seen := make(map[string]struct{}, len(policy.AllowedRegions))
	if len(policy.AllowedRegions) > maxAllowedRegions {
		return PlacementPolicy{}, admissionError(
			AdmissionInvalidRequest,
			fmt.Sprintf("allowed regions cannot contain more than %d entries", maxAllowedRegions),
		)
	}
	normalizedRegions := make([]string, 0, len(policy.AllowedRegions))
	for _, region := range policy.AllowedRegions {
		trimmed := strings.TrimSpace(region)
		if trimmed != region || !validTopologyLabel(region) {
			return PlacementPolicy{}, admissionError(
				AdmissionInvalidRequest,
				"allowed region is invalid",
			)
		}
		if _, duplicate := seen[region]; duplicate {
			return PlacementPolicy{}, admissionError(
				AdmissionInvalidRequest,
				"allowed regions must be unique",
			)
		}
		seen[region] = struct{}{}
		normalizedRegions = append(normalizedRegions, region)
	}
	slices.Sort(normalizedRegions)
	policy.AllowedRegions = normalizedRegions
	return policy, nil
}

func validTopologyLabel(value string) bool {
	if value == "" || len(value) > maxTopologyLabelBytes {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			(index > 0 && (character == '.' || character == '_' || character == '-')) {
			continue
		}
		return false
	}
	return true
}

func admissionError(code AdmissionCode, message string) *AdmissionError {
	return &AdmissionError{Code: code, Message: message}
}

func cloneRegionalNodes(nodes []RegionalNode) []RegionalNode {
	cloned := append([]RegionalNode(nil), nodes...)
	for index := range cloned {
		cloned[index].ConsensusVoterNodeIDs = append(
			[]uint64(nil),
			cloned[index].ConsensusVoterNodeIDs...,
		)
	}
	return cloned
}
