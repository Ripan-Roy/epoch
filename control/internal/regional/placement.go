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
)

// PlacementPolicy is the bounded failure-domain request carried by desired
// resource state. The fixed-voter alpha runtime can validate these constraints
// but does not yet perform membership changes or rebalancing.
type PlacementPolicy struct {
	AllowedRegions    []string `json:"allowed_regions,omitempty"`
	MinimumZones      uint32   `json:"minimum_zones,omitempty"`
	RequiredNodeClass string   `json:"required_node_class,omitempty"`
}

// RegionalNode is one policy-protected configured-endpoint topology and
// capacity sample. Transport-level server identity remains a separate concern.
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

// PlacementDecision records the concrete fixed voters and capacity charged by
// one successful admission.
type PlacementDecision struct {
	VoterNodeIDs            []uint64
	AchievedZones           uint32
	AdditionalGroupsPerNode uint32
	Nodes                   []RegionalNode
	Policy                  PlacementPolicy
}

// AdmissionCode is a stable, machine-readable precondition failure.
type AdmissionCode string

const (
	AdmissionInvalidRequest         AdmissionCode = "invalid_placement_request"
	AdmissionInconsistentInventory  AdmissionCode = "inconsistent_regional_inventory"
	AdmissionFixedVotersIneligible  AdmissionCode = "fixed_voters_ineligible"
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

// AdmitPlacement validates requested constraints against the actual fixed
// voter inventory and checks incremental consensus-group capacity.
func AdmitPlacement(
	policy PlacementPolicy,
	replicas uint32,
	desiredShards uint32,
	existingShards uint32,
	inventory NodeInventory,
) (PlacementDecision, error) {
	nodes, err := validateInventory(inventory, replicas)
	if err != nil {
		return PlacementDecision{}, err
	}
	if desiredShards == 0 || desiredShards < existingShards {
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
	zones := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if len(allowedRegions) > 0 {
			if _, allowed := allowedRegions[node.Region]; !allowed {
				return PlacementDecision{}, admissionError(
					AdmissionFixedVotersIneligible,
					fmt.Sprintf("fixed voter node %d is outside allowed regions", node.NodeID),
				)
			}
		}
		if policy.RequiredNodeClass != "" && node.NodeClass != policy.RequiredNodeClass {
			return PlacementDecision{}, admissionError(
				AdmissionFixedVotersIneligible,
				fmt.Sprintf(
					"fixed voter node %d has class %q, not %q",
					node.NodeID,
					node.NodeClass,
					policy.RequiredNodeClass,
				),
			)
		}
		zones[node.Zone] = struct{}{}
	}
	if uint32(len(zones)) < policy.MinimumZones {
		return PlacementDecision{}, admissionError(
			AdmissionInsufficientZones,
			fmt.Sprintf(
				"fixed voters span %d zones; %d are required",
				len(zones),
				policy.MinimumZones,
			),
		)
	}

	additional := desiredShards - existingShards
	if additional > 0 {
		for _, node := range nodes {
			if node.AvailableConsensusGroups < additional {
				return PlacementDecision{}, &AdmissionError{
					Code:           AdmissionConsensusGroupCapacity,
					Message:        fmt.Sprintf("node %d limits consensus-group capacity", node.NodeID),
					LimitingNodeID: node.NodeID,
					Required:       additional,
					Available:      node.AvailableConsensusGroups,
				}
			}
		}
	}
	voters := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		voters = append(voters, node.NodeID)
	}
	return PlacementDecision{
		VoterNodeIDs:            voters,
		AchievedZones:           uint32(len(zones)),
		AdditionalGroupsPerNode: additional,
		Nodes:                   cloneRegionalNodes(nodes),
		Policy:                  policy,
	}, nil
}

func validateInventory(inventory NodeInventory, replicas uint32) ([]RegionalNode, error) {
	if replicas != 3 || len(inventory.Nodes) != int(replicas) {
		return nil, admissionError(
			AdmissionInconsistentInventory,
			fmt.Sprintf(
				"fixed-voter runtime requires exactly 3 complete node samples; observed %d",
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
	for _, node := range nodes {
		voters := append([]uint64(nil), node.ConsensusVoterNodeIDs...)
		slices.Sort(voters)
		if !slices.Equal(voters, nodeIDs) {
			return nil, admissionError(
				AdmissionInconsistentInventory,
				fmt.Sprintf("node %d returned a different fixed voter set", node.NodeID),
			)
		}
	}
	return nodes, nil
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
