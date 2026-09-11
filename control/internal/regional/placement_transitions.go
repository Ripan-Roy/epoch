package regional

import "slices"

// PlanNextPlacementTransition finds the next deterministic learner-first move
// for an existing resource. It repairs policy exclusions and failure-domain
// deficits before considering a load-balancing move. A caller must not invoke
// it while the resource already has an active membership transition.
func PlanNextPlacementTransition(
	policy PlacementPolicy,
	replicas uint32,
	existing []TabletPlacement,
	inventory NodeInventory,
) (*PlacementTransition, error) {
	decision, err := AdmitPlacement(
		policy,
		replicas,
		uint32(len(existing)),
		existing,
		inventory,
	)
	if err != nil {
		return nil, err
	}
	nodesByID := make(map[uint64]RegionalNode, len(decision.Nodes))
	for _, node := range decision.Nodes {
		nodesByID[node.NodeID] = node
	}
	eligible := eligibleNodes(decision.Policy, decision.Nodes)
	eligibleByID := make(map[uint64]struct{}, len(eligible))
	for _, node := range eligible {
		eligibleByID[node.NodeID] = struct{}{}
	}
	baseline := placementLoadScore(decision.Nodes, 0, 0)
	var best *transitionCandidate
	repairRequired := false
	for _, placement := range decision.TabletPlacements {
		currentQuality := placementQualityFor(
			placement.VoterNodeIDs,
			nodesByID,
			eligibleByID,
			decision.Policy,
		)
		if !currentQuality.satisfied() {
			repairRequired = true
		}
		for outgoingIndex := len(placement.VoterNodeIDs) - 1; outgoingIndex >= 0; outgoingIndex-- {
			outgoing := placement.VoterNodeIDs[outgoingIndex]
			for _, incomingNode := range eligible {
				incoming := incomingNode.NodeID
				if slices.Contains(placement.VoterNodeIDs, incoming) ||
					incomingNode.AvailableConsensusGroups == 0 {
					continue
				}
				target := replaceVoter(placement.VoterNodeIDs, outgoing, incoming)
				targetQuality := placementQualityFor(
					target,
					nodesByID,
					eligibleByID,
					decision.Policy,
				)
				reason, acceptable := transitionReason(currentQuality, targetQuality)
				if !acceptable {
					continue
				}
				load := placementLoadScore(decision.Nodes, outgoing, incoming)
				if reason == TransitionRebalance && !load.less(baseline) {
					continue
				}
				candidate := transitionCandidate{
					transition: PlacementTransition{
						ShardIndex:          placement.ShardIndex,
						CurrentVoterNodeIDs: append([]uint64(nil), placement.VoterNodeIDs...),
						TargetVoterNodeIDs:  target,
						Reason:              reason,
					},
					quality: targetQuality,
					load:    load,
				}
				if best == nil || candidate.less(*best) {
					best = &candidate
				}
			}
		}
	}
	if best != nil {
		transition := best.transition
		return &transition, nil
	}
	if repairRequired {
		limiting := firstUnselectedEligibleNode(decision.TabletPlacements, eligible)
		return nil, &AdmissionError{
			Code:           AdmissionConsensusGroupCapacity,
			Message:        "no policy-compliant learner target has available consensus-group capacity",
			LimitingNodeID: limiting.NodeID,
			Required:       1,
			Available:      limiting.AvailableConsensusGroups,
		}
	}
	return nil, nil
}

type placementQuality struct {
	ineligible  uint32
	zoneDeficit uint32
	rackDeficit uint32
}

func (quality placementQuality) satisfied() bool {
	return quality.ineligible == 0 && quality.zoneDeficit == 0 && quality.rackDeficit == 0
}

func (quality placementQuality) less(other placementQuality) bool {
	if quality.ineligible != other.ineligible {
		return quality.ineligible < other.ineligible
	}
	qualityDomains := quality.zoneDeficit + quality.rackDeficit
	otherDomains := other.zoneDeficit + other.rackDeficit
	if qualityDomains != otherDomains {
		return qualityDomains < otherDomains
	}
	if quality.zoneDeficit != other.zoneDeficit {
		return quality.zoneDeficit < other.zoneDeficit
	}
	return quality.rackDeficit < other.rackDeficit
}

func placementQualityFor(
	voters []uint64,
	nodes map[uint64]RegionalNode,
	eligible map[uint64]struct{},
	policy PlacementPolicy,
) placementQuality {
	zones := make(map[string]struct{}, len(voters))
	racks := make(map[string]struct{}, len(voters))
	quality := placementQuality{}
	for _, nodeID := range voters {
		if _, ok := eligible[nodeID]; !ok {
			quality.ineligible++
		}
		if node, exists := nodes[nodeID]; exists {
			zones[node.Zone] = struct{}{}
			racks[node.Rack] = struct{}{}
		}
	}
	if uint32(len(zones)) < policy.MinimumZones {
		quality.zoneDeficit = policy.MinimumZones - uint32(len(zones))
	}
	if uint32(len(racks)) < policy.MinimumRacks {
		quality.rackDeficit = policy.MinimumRacks - uint32(len(racks))
	}
	return quality
}

func transitionReason(
	current placementQuality,
	target placementQuality,
) (PlacementTransitionReason, bool) {
	if current.ineligible > 0 && target.ineligible < current.ineligible {
		return TransitionPolicyRepair, true
	}
	if current.ineligible == 0 &&
		(current.zoneDeficit > 0 || current.rackDeficit > 0) &&
		target.ineligible == 0 && target.less(current) {
		return TransitionTopologyRepair, true
	}
	if current.satisfied() && target.satisfied() {
		return TransitionRebalance, true
	}
	return "", false
}

type loadScore struct {
	maximum    uint64
	sumSquares uint64
}

func (score loadScore) less(other loadScore) bool {
	return score.maximum < other.maximum ||
		(score.maximum == other.maximum && score.sumSquares < other.sumSquares)
}

func placementLoadScore(nodes []RegionalNode, outgoing, incoming uint64) loadScore {
	const utilizationScale = uint64(1_000_000)
	var score loadScore
	for _, node := range nodes {
		used := uint64(node.UsedConsensusGroups)
		if node.NodeID == outgoing && used > 0 {
			used--
		}
		if node.NodeID == incoming {
			used++
		}
		scaled := used * utilizationScale / uint64(node.MaxConsensusGroups)
		score.maximum = max(score.maximum, scaled)
		score.sumSquares += scaled * scaled
	}
	return score
}

type transitionCandidate struct {
	transition PlacementTransition
	quality    placementQuality
	load       loadScore
}

func (candidate transitionCandidate) less(other transitionCandidate) bool {
	candidatePriority := transitionPriority(candidate.transition.Reason)
	otherPriority := transitionPriority(other.transition.Reason)
	if candidatePriority != otherPriority {
		return candidatePriority < otherPriority
	}
	if candidate.quality != other.quality {
		return candidate.quality.less(other.quality)
	}
	if candidate.load != other.load {
		return candidate.load.less(other.load)
	}
	if candidate.transition.ShardIndex != other.transition.ShardIndex {
		return candidate.transition.ShardIndex < other.transition.ShardIndex
	}
	return slices.Compare(
		candidate.transition.TargetVoterNodeIDs,
		other.transition.TargetVoterNodeIDs,
	) < 0
}

func transitionPriority(reason PlacementTransitionReason) uint8 {
	switch reason {
	case TransitionPolicyRepair:
		return 0
	case TransitionTopologyRepair:
		return 1
	case TransitionRebalance:
		return 2
	default:
		return 3
	}
}

func replaceVoter(current []uint64, outgoing, incoming uint64) []uint64 {
	target := append([]uint64(nil), current...)
	for index, nodeID := range target {
		if nodeID == outgoing {
			target[index] = incoming
			break
		}
	}
	slices.Sort(target)
	return target
}

func firstUnselectedEligibleNode(
	placements []TabletPlacement,
	eligible []RegionalNode,
) RegionalNode {
	for _, node := range eligible {
		selectedEverywhere := true
		for _, placement := range placements {
			if !slices.Contains(placement.VoterNodeIDs, node.NodeID) {
				selectedEverywhere = false
				break
			}
		}
		if !selectedEverywhere {
			return node
		}
	}
	return RegionalNode{}
}
