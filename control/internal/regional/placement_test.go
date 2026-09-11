package regional

import (
	"encoding/json"
	"fmt"
	"math"
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

func TestAdmitPlacementSupportsMaximumPhysicalNodeInventory(t *testing.T) {
	nodes := make([]RegionalNode, 0, maxRegionalNodes)
	for index := range maxRegionalNodes {
		nodes = append(nodes, RegionalNode{
			NodeID:                   uint64(index + 1),
			Region:                   "ap-south",
			Zone:                     fmt.Sprintf("zone-%04d", index),
			Rack:                     fmt.Sprintf("rack-%04d", index),
			NodeClass:                "general-purpose",
			ConsensusVoterNodeIDs:    []uint64{1, 2, 3},
			MaxConsensusGroups:       16,
			AvailableConsensusGroups: 16,
		})
	}
	decision, err := AdmitPlacement(
		PlacementPolicy{MinimumZones: 5, MinimumRacks: 5},
		5,
		1,
		nil,
		NodeInventory{Nodes: nodes},
	)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	if len(decision.EligibleNodeIDs) != maxRegionalNodes ||
		!slices.Equal(decision.TabletPlacements[0].VoterNodeIDs, []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestAdmitPlacementSpreadsVotersAcrossRequiredRacks(t *testing.T) {
	inventory := regionalInventory(6, 8)
	inventory.Nodes[0].Zone, inventory.Nodes[0].Rack = "ap-south-1a", "rack-a"
	inventory.Nodes[1].Zone, inventory.Nodes[1].Rack = "ap-south-1a", "rack-b"
	inventory.Nodes[2].Zone, inventory.Nodes[2].Rack = "ap-south-1b", "rack-c"
	inventory.Nodes[3].Zone, inventory.Nodes[3].Rack = "ap-south-1b", "rack-d"
	inventory.Nodes[4].Zone, inventory.Nodes[4].Rack = "ap-south-1c", "rack-e"
	inventory.Nodes[5].Zone, inventory.Nodes[5].Rack = "ap-south-1c", "rack-f"

	decision, err := AdmitPlacement(
		PlacementPolicy{MinimumZones: 2, MinimumRacks: 3},
		3,
		2,
		nil,
		inventory,
	)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	if decision.AchievedZones < 2 || decision.AchievedRacks != 3 {
		t.Fatalf("decision = %+v", decision)
	}
	for _, placement := range decision.TabletPlacements {
		racks := make(map[string]struct{})
		for _, nodeID := range placement.VoterNodeIDs {
			racks[inventory.Nodes[nodeID-1].Rack] = struct{}{}
		}
		if len(racks) != 3 {
			t.Fatalf("placement = %+v, racks = %v", placement, racks)
		}
	}
}

func TestAdmitPlacementBacktracksAcrossCorrelatedZoneAndRackDomains(t *testing.T) {
	inventory := regionalInventory(4, 8)
	inventory.Nodes[0].Zone, inventory.Nodes[0].Rack = "zone-a", "rack-x"
	inventory.Nodes[1].Zone, inventory.Nodes[1].Rack = "zone-a", "rack-y"
	inventory.Nodes[2].Zone, inventory.Nodes[2].Rack = "zone-b", "rack-x"
	inventory.Nodes[3].Zone, inventory.Nodes[3].Rack = "zone-c", "rack-z"

	decision, err := AdmitPlacement(
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3},
		3,
		1,
		nil,
		inventory,
	)
	if err != nil {
		t.Fatalf("AdmitPlacement() error = %v", err)
	}
	if !slices.Equal(decision.TabletPlacements[0].VoterNodeIDs, []uint64{2, 3, 4}) {
		t.Fatalf("placement = %+v", decision.TabletPlacements[0])
	}
}

func TestAdmitPlacementRejectsIncompatibleZoneAndRackDomains(t *testing.T) {
	inventory := regionalInventory(5, 8)
	inventory.Nodes[0].Zone, inventory.Nodes[0].Rack = "zone-a", "rack-x"
	inventory.Nodes[1].Zone, inventory.Nodes[1].Rack = "zone-a", "rack-y"
	inventory.Nodes[2].Zone, inventory.Nodes[2].Rack = "zone-a", "rack-z"
	inventory.Nodes[3].Zone, inventory.Nodes[3].Rack = "zone-b", "rack-x"
	inventory.Nodes[4].Zone, inventory.Nodes[4].Rack = "zone-c", "rack-x"

	_, err := AdmitPlacement(
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3},
		3,
		1,
		nil,
		inventory,
	)
	assertAdmissionCode(t, err, AdmissionIncompatibleDomains)
}

func TestPlacementPolicyDecodesNumericAndProtobufJSONNodeIDs(t *testing.T) {
	var policy PlacementPolicy
	if err := json.Unmarshal(
		[]byte(`{"minimum_zones":2,"minimum_racks":2,"excluded_node_ids":[3,"18446744073709551615"]}`),
		&policy,
	); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !slices.Equal(policy.ExcludedNodeIDs, []uint64{3, math.MaxUint64}) {
		t.Fatalf("excluded node IDs = %v", policy.ExcludedNodeIDs)
	}
	for _, invalid := range []string{
		`{"excluded_node_ids":["03"]}`,
		`{"excluded_node_ids":[3.0]}`,
		`{"excluded_node_ids":[-1]}`,
		`{"excluded_node_ids":[null]}`,
		`{"unknown":true}`,
	} {
		if err := json.Unmarshal([]byte(invalid), &policy); err == nil {
			t.Fatalf("Unmarshal(%s) succeeded", invalid)
		}
	}
}

func TestPlanNextPlacementTransitionRepairsAnExcludedVoter(t *testing.T) {
	inventory := regionalInventory(5, 8)
	current := []TabletPlacement{{ShardIndex: 0, VoterNodeIDs: []uint64{1, 2, 3}}}

	transition, err := PlanNextPlacementTransition(
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3, ExcludedNodeIDs: []uint64{1}},
		3,
		current,
		inventory,
	)
	if err != nil {
		t.Fatalf("PlanNextPlacementTransition() error = %v", err)
	}
	if transition == nil || transition.Reason != TransitionPolicyRepair ||
		transition.ShardIndex != 0 ||
		!slices.Equal(transition.CurrentVoterNodeIDs, []uint64{1, 2, 3}) ||
		!slices.Equal(transition.TargetVoterNodeIDs, []uint64{2, 3, 4}) {
		t.Fatalf("transition = %+v", transition)
	}
}

func TestPlanNextPlacementTransitionRepairsFailureDomainsOneVoterAtATime(t *testing.T) {
	inventory := regionalInventory(5, 8)
	for index := range 3 {
		inventory.Nodes[index].Zone = "ap-south-1a"
		inventory.Nodes[index].Rack = "rack-a"
	}
	inventory.Nodes[3].Zone, inventory.Nodes[3].Rack = "ap-south-1b", "rack-b"
	inventory.Nodes[4].Zone, inventory.Nodes[4].Rack = "ap-south-1c", "rack-c"
	current := []TabletPlacement{{ShardIndex: 0, VoterNodeIDs: []uint64{1, 2, 3}}}

	first, err := PlanNextPlacementTransition(
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3},
		3,
		current,
		inventory,
	)
	if err != nil {
		t.Fatalf("first PlanNextPlacementTransition() error = %v", err)
	}
	if first == nil || first.Reason != TransitionTopologyRepair ||
		!singleVoterReplacement(first.CurrentVoterNodeIDs, first.TargetVoterNodeIDs) ||
		!slices.Contains(first.TargetVoterNodeIDs, uint64(4)) {
		t.Fatalf("first transition = %+v", first)
	}

	secondCurrent := []TabletPlacement{{ShardIndex: 0, VoterNodeIDs: first.TargetVoterNodeIDs}}
	second, err := PlanNextPlacementTransition(
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3},
		3,
		secondCurrent,
		inventory,
	)
	if err != nil {
		t.Fatalf("second PlanNextPlacementTransition() error = %v", err)
	}
	if second == nil || second.Reason != TransitionTopologyRepair ||
		!singleVoterReplacement(second.CurrentVoterNodeIDs, second.TargetVoterNodeIDs) ||
		!slices.Contains(second.TargetVoterNodeIDs, uint64(5)) {
		t.Fatalf("second transition = %+v", second)
	}
}

func TestPlanNextPlacementTransitionRebalancesAcrossNNodes(t *testing.T) {
	inventory := regionalInventory(9, 8)
	for index := range inventory.Nodes {
		inventory.Nodes[index].MaxConsensusGroups = 10
		inventory.Nodes[index].UsedConsensusGroups = 2
		inventory.Nodes[index].AvailableConsensusGroups = 8
	}
	for index := range 5 {
		inventory.Nodes[index].UsedConsensusGroups = 9
		inventory.Nodes[index].AvailableConsensusGroups = 1
	}
	current := []TabletPlacement{{ShardIndex: 0, VoterNodeIDs: []uint64{1, 2, 3, 4, 5}}}

	transition, err := PlanNextPlacementTransition(PlacementPolicy{MinimumZones: 5}, 5, current, inventory)
	if err != nil {
		t.Fatalf("PlanNextPlacementTransition() error = %v", err)
	}
	if transition == nil || transition.Reason != TransitionRebalance ||
		!singleVoterReplacement(transition.CurrentVoterNodeIDs, transition.TargetVoterNodeIDs) ||
		!slices.Equal(transition.TargetVoterNodeIDs, []uint64{1, 2, 3, 4, 6}) {
		t.Fatalf("transition = %+v", transition)
	}
}

func TestPlanNextPlacementTransitionStopsAtABalancedFixedPoint(t *testing.T) {
	inventory := regionalInventory(6, 8)
	current := []TabletPlacement{
		{ShardIndex: 0, VoterNodeIDs: []uint64{1, 2, 3}},
		{ShardIndex: 1, VoterNodeIDs: []uint64{4, 5, 6}},
	}

	transition, err := PlanNextPlacementTransition(PlacementPolicy{MinimumZones: 3}, 3, current, inventory)
	if err != nil {
		t.Fatalf("PlanNextPlacementTransition() error = %v", err)
	}
	if transition != nil {
		t.Fatalf("transition = %+v, want balanced fixed point", transition)
	}
}

func TestPlanNextPlacementTransitionFailsBeforeUnsafeRepairWithoutCapacity(t *testing.T) {
	inventory := regionalInventory(4, 8)
	inventory.Nodes[3].UsedConsensusGroups = inventory.Nodes[3].MaxConsensusGroups
	inventory.Nodes[3].AvailableConsensusGroups = 0
	current := []TabletPlacement{{ShardIndex: 0, VoterNodeIDs: []uint64{1, 2, 3}}}

	_, err := PlanNextPlacementTransition(
		PlacementPolicy{ExcludedNodeIDs: []uint64{1}},
		3,
		current,
		inventory,
	)
	assertAdmissionCode(t, err, AdmissionConsensusGroupCapacity)
}

func TestPlanNextPlacementTransitionRepairsAVoterMissingFromInventory(t *testing.T) {
	inventory := regionalInventory(4, 8)
	current := []TabletPlacement{{ShardIndex: 0, VoterNodeIDs: []uint64{1, 2, 99}}}

	transition, err := PlanNextPlacementTransition(
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3},
		3,
		current,
		inventory,
	)
	if err != nil {
		t.Fatalf("PlanNextPlacementTransition() error = %v", err)
	}
	if transition == nil || transition.Reason != TransitionPolicyRepair ||
		!slices.Equal(transition.TargetVoterNodeIDs, []uint64{1, 2, 3}) {
		t.Fatalf("transition = %+v", transition)
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
			Rack:                     "rack-" + string(rune('a'+index)),
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
