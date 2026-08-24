package regional

import (
	"slices"
	"testing"
)

func TestAdmitPlacementBalancesThreeVoterTabletsAcrossSevenPhysicalNodes(t *testing.T) {
	inventory := regionalInventory(7, 8)
	decision, err := AdmitPlacement(
		PlacementPolicy{
			AllowedRegions:    []string{"ap-south"},
			MinimumZones:      3,
			RequiredNodeClass: "general-purpose",
		},
		3,
		4,
		nil,
		inventory,
	)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	want := [][]uint64{{1, 2, 3}, {4, 5, 6}, {1, 2, 7}, {3, 4, 5}}
	if len(decision.TabletPlacements) != len(want) {
		t.Fatalf("placements = %+v", decision.TabletPlacements)
	}
	for index, placement := range decision.TabletPlacements {
		if placement.ShardIndex != uint32(index) || !slices.Equal(placement.VoterNodeIDs, want[index]) {
			t.Fatalf("placement %d = %+v, want %v", index, placement, want[index])
		}
	}
	if decision.AchievedZones != 3 || decision.AdditionalGroupsByNode[1] != 2 ||
		decision.AdditionalGroupsByNode[6] != 1 || len(decision.EligibleNodeIDs) != 7 {
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
		nil,
		inventory,
	)
	assertAdmissionCode(t, err, AdmissionInsufficientZones)
}

func TestAdmitPlacementRejectsClassThatCannotFillTheFixedVoterSet(t *testing.T) {
	inventory := regionalInventory(7, 8)
	for index := 2; index < len(inventory.Nodes); index++ {
		inventory.Nodes[index].NodeClass = "memory-optimized"
	}
	_, err := AdmitPlacement(
		PlacementPolicy{RequiredNodeClass: "general-purpose"},
		3,
		1,
		nil,
		inventory,
	)
	assertAdmissionCode(t, err, AdmissionFixedVotersIneligible)
}

func TestAdmitPlacementReportsTheLimitingCapacityBeforeCatalogApply(t *testing.T) {
	inventory := threeZoneInventory(8)
	inventory.Nodes[1].AvailableConsensusGroups = 1
	inventory.Nodes[1].UsedConsensusGroups = 15
	_, err := AdmitPlacement(PlacementPolicy{}, 3, 2, nil, inventory)
	assertAdmissionCode(t, err, AdmissionConsensusGroupCapacity)
	admission := err.(*AdmissionError)
	if admission.LimitingNodeID != 2 || admission.Required != 2 || admission.Available != 1 {
		t.Fatalf("admission error = %+v", admission)
	}
}

func TestAdmitPlacementChargesOnlyAddedShardsOnAnUpdate(t *testing.T) {
	inventory := threeZoneInventory(2)
	existing := []TabletPlacement{
		{ShardIndex: 0, VoterNodeIDs: []uint64{1, 2, 3}},
		{ShardIndex: 1, VoterNodeIDs: []uint64{1, 2, 3}},
		{ShardIndex: 2, VoterNodeIDs: []uint64{1, 2, 3}},
	}
	decision, err := AdmitPlacement(PlacementPolicy{}, 3, 5, existing, inventory)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	if decision.AdditionalGroupsByNode[1] != 2 || decision.AdditionalGroupsByNode[2] != 2 ||
		decision.AdditionalGroupsByNode[3] != 2 {
		t.Fatalf("additional groups = %+v", decision.AdditionalGroupsByNode)
	}
}

func TestAdmitPlacementRejectsInconsistentFixedVoterEvidence(t *testing.T) {
	inventory := threeZoneInventory(8)
	inventory.Nodes[2].ConsensusVoterNodeIDs = []uint64{1, 2, 4}
	_, err := AdmitPlacement(PlacementPolicy{}, 3, 1, nil, inventory)
	assertAdmissionCode(t, err, AdmissionInconsistentInventory)
}

func TestAdmitPlacementSupportsFiveVotersWithinANineNodeCluster(t *testing.T) {
	decision, err := AdmitPlacement(
		PlacementPolicy{MinimumZones: 5},
		5,
		2,
		nil,
		regionalInventory(9, 4),
	)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	if !slices.Equal(decision.TabletPlacements[0].VoterNodeIDs, []uint64{1, 2, 3, 4, 5}) ||
		!slices.Equal(decision.TabletPlacements[1].VoterNodeIDs, []uint64{1, 6, 7, 8, 9}) {
		t.Fatalf("five-voter placements = %+v", decision.TabletPlacements)
	}
}

func threeZoneInventory(available uint32) NodeInventory {
	return regionalInventory(3, available)
}

func regionalInventory(count int, available uint32) NodeInventory {
	nodes := make([]RegionalNode, 0, count)
	catalogVoters := []uint64{1, 2, 3}
	for index := range count {
		nodes = append(nodes, RegionalNode{
			NodeID:                   uint64(index + 1),
			Region:                   "ap-south",
			Zone:                     "ap-south-1" + string(rune('a'+index)),
			NodeClass:                "general-purpose",
			ConsensusVoterNodeIDs:    append([]uint64(nil), catalogVoters...),
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
