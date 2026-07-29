package regional

import (
	"slices"
	"testing"
)

func TestAdmitPlacementSelectsActualFixedVotersAcrossZones(t *testing.T) {
	inventory := threeZoneInventory(8)
	decision, err := AdmitPlacement(
		PlacementPolicy{
			AllowedRegions:    []string{"ap-south"},
			MinimumZones:      3,
			RequiredNodeClass: "general-purpose",
		},
		3,
		4,
		0,
		inventory,
	)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	if !slices.Equal(decision.VoterNodeIDs, []uint64{1, 2, 3}) {
		t.Fatalf("voters = %v", decision.VoterNodeIDs)
	}
	if decision.AchievedZones != 3 || decision.AdditionalGroupsPerNode != 4 {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestAdmitPlacementRejectsUnsatisfiedFailureDomains(t *testing.T) {
	inventory := threeZoneInventory(8)
	inventory.Nodes[2].Zone = "ap-south-1b"
	_, err := AdmitPlacement(
		PlacementPolicy{AllowedRegions: []string{"ap-south"}, MinimumZones: 3},
		3,
		1,
		0,
		inventory,
	)
	assertAdmissionCode(t, err, AdmissionInsufficientZones)
}

func TestAdmitPlacementRejectsClassThatCannotFillTheFixedVoterSet(t *testing.T) {
	inventory := threeZoneInventory(8)
	inventory.Nodes[2].NodeClass = "memory-optimized"
	_, err := AdmitPlacement(
		PlacementPolicy{RequiredNodeClass: "general-purpose"},
		3,
		1,
		0,
		inventory,
	)
	assertAdmissionCode(t, err, AdmissionFixedVotersIneligible)
}

func TestAdmitPlacementReportsTheLimitingCapacityBeforeCatalogApply(t *testing.T) {
	inventory := threeZoneInventory(8)
	inventory.Nodes[1].AvailableConsensusGroups = 1
	inventory.Nodes[1].UsedConsensusGroups = 15
	_, err := AdmitPlacement(PlacementPolicy{}, 3, 2, 0, inventory)
	assertAdmissionCode(t, err, AdmissionConsensusGroupCapacity)
	admission := err.(*AdmissionError)
	if admission.LimitingNodeID != 2 || admission.Required != 2 || admission.Available != 1 {
		t.Fatalf("admission error = %+v", admission)
	}
}

func TestAdmitPlacementChargesOnlyAddedShardsOnAnUpdate(t *testing.T) {
	inventory := threeZoneInventory(2)
	decision, err := AdmitPlacement(PlacementPolicy{}, 3, 5, 3, inventory)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	if decision.AdditionalGroupsPerNode != 2 {
		t.Fatalf("additional groups = %d", decision.AdditionalGroupsPerNode)
	}
}

func TestAdmitPlacementRejectsInconsistentFixedVoterEvidence(t *testing.T) {
	inventory := threeZoneInventory(8)
	inventory.Nodes[2].ConsensusVoterNodeIDs = []uint64{1, 2, 4}
	_, err := AdmitPlacement(PlacementPolicy{}, 3, 1, 0, inventory)
	assertAdmissionCode(t, err, AdmissionInconsistentInventory)
}

func threeZoneInventory(available uint32) NodeInventory {
	nodes := make([]RegionalNode, 0, 3)
	for index, zone := range []string{"ap-south-1a", "ap-south-1b", "ap-south-1c"} {
		nodes = append(nodes, RegionalNode{
			NodeID:                   uint64(index + 1),
			Region:                   "ap-south",
			Zone:                     zone,
			NodeClass:                "general-purpose",
			ConsensusVoterNodeIDs:    []uint64{1, 2, 3},
			MaxConsensusGroups:       16,
			UsedConsensusGroups:      16 - available,
			AvailableConsensusGroups: available,
		})
	}
	return NodeInventory{Nodes: nodes}
}

func assertAdmissionCode(t *testing.T, err error, code AdmissionCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("AdmitPlacement() succeeded, want %s", code)
	}
	admission, ok := err.(*AdmissionError)
	if !ok || admission.Code != code {
		t.Fatalf("AdmitPlacement() error = %#v, want %s", err, code)
	}
}
