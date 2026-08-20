import type {
  ManagedRegionalResource,
  RegionalResource,
  RegionalPlacementEvidence,
  RegionalResourcePhase,
  RegionalTabletPlacement,
} from "./api/types";

export interface RegionalPlacementAssessment {
  phase: RegionalResourcePhase;
  summary: string;
  risks: string[];
}

export function assessRegionalPlacement(
  tablets: RegionalTabletPlacement[],
  expectedShardCount: number,
  topology: RegionalPlacementEvidence | null = null,
): RegionalPlacementAssessment {
  if (tablets.length === 0) {
    return {
      phase: "pending",
      summary: "No serving placement observed",
      risks: ["The managed control plane has not observed a serving route for this resource."],
    };
  }

  const risks: string[] = [];
  if (tablets.length !== expectedShardCount) {
    risks.push(
      `Observed ${tablets.length} of ${expectedShardCount} catalog shards; routing evidence is incomplete.`,
    );
  }

  let minimumVoters = Number.POSITIVE_INFINITY;
  let desiredReplicas = 0;
  for (const tablet of tablets) {
    const voterCount = new Set(tablet.voterNodeIds).size;
    minimumVoters = Math.min(minimumVoters, voterCount);
    desiredReplicas = Math.max(desiredReplicas, tablet.desiredReplicas);
    if (voterCount < tablet.desiredReplicas) {
      const missing = tablet.desiredReplicas - voterCount;
      risks.push(
        `Shard ${tablet.shardIndex} is missing ${missing} voter${missing === 1 ? "" : "s"} (${voterCount}/${tablet.desiredReplicas} observed).`,
      );
    }
    if (tablet.leaderNodeId === null) {
      risks.push(`Shard ${tablet.shardIndex} has no leader in the observed placement.`);
    }
  }

  const shardLabel = `${tablets.length} shard${tablets.length === 1 ? "" : "s"}`;
  const voterSummary = `${minimumVoters}/${desiredReplicas} voters`;
  if (risks.length > 0) {
    return {
      phase: "degraded",
      summary: `${shardLabel} observed · ${voterSummary}`,
      risks,
    };
  }
  const topologyRisk = assessTopologyEvidence(tablets, topology);
  if (topologyRisk !== null) {
    return {
      phase: topologyRisk.phase,
      summary: `${shardLabel} serving · ${voterSummary}${topologyRisk.summarySuffix}`,
      risks: topologyRisk.risks,
    };
  }
  return {
    phase: "ready",
    summary: `${shardLabel} serving · ${voterSummary} observed`,
    risks: ["Node count is observed; zone, rack, and failure-domain separation are not yet verified."],
  };
}

function assessTopologyEvidence(
  tablets: RegionalTabletPlacement[],
  topology: RegionalPlacementEvidence | null,
): { phase: RegionalResourcePhase; summarySuffix: string; risks: string[] } | null {
  if (topology === null) {
    return null;
  }
  const nodeIDs = new Set(topology.nodes.map((node) => node.nodeId));
  const zones = new Set(topology.nodes.map((node) => node.zone));
  const unknownVoter = tablets.some((tablet) => tablet.voterNodeIds.some((voter) => !nodeIDs.has(voter)));
  if (
    topology.minimumZones < 1 ||
    topology.achievedZones < topology.minimumZones ||
    zones.size < topology.minimumZones ||
    unknownVoter
  ) {
    return {
      phase: "degraded",
      summarySuffix: " · topology evidence inconsistent",
      risks: ["Reported topology does not prove the admitted placement constraints."],
    };
  }
  return {
    phase: "ready",
    summarySuffix: ` · ${topology.achievedZones} configured zones observed`,
    risks: [
      "Configured zone labels agree across allowlisted responses; physical rack separation, Rust server identity, membership changes, and dynamic rebalancing remain outside this alpha.",
    ],
  };
}

export function mapRegionalInventory(resource: ManagedRegionalResource): RegionalResource {
  const tablets = resource.tablets.map((tablet): RegionalTabletPlacement => ({
    tabletId: tablet.tablet_id,
    consensusGroupId: tablet.consensus_group_id,
    shardIndex: tablet.shard_index,
    tabletEpoch: tablet.tablet_epoch,
    resourceGeneration: tablet.resource_generation,
    desiredReplicas: tablet.desired_replicas,
    voterNodeIds: [...tablet.voter_node_ids],
    leaderNodeId: tablet.leader_node_id,
  }));
  const topology: RegionalPlacementEvidence | null = resource.placement
    ? {
        allowedRegions: [...resource.placement.allowed_regions],
        minimumZones: resource.placement.minimum_zones,
        requiredNodeClass: resource.placement.required_node_class ?? null,
        achievedZones: resource.placement.achieved_zones,
        nodes: resource.placement.nodes.map((node) => ({
          nodeId: node.node_id,
          region: node.region,
          zone: node.zone,
          nodeClass: node.node_class,
          consensusVoterNodeIds: [...node.consensus_voter_node_ids],
          maxConsensusGroups: node.max_consensus_groups,
          usedConsensusGroups: node.used_consensus_groups,
          availableConsensusGroups: node.available_consensus_groups,
        })),
      }
    : null;
  const placement = assessRegionalPlacement(tablets, resource.shard_count, topology);
  const risks = [...placement.risks];
  let phase = resource.phase;
  let summary = placement.summary;

  if (resource.phase === "ready" && placement.phase !== "ready") {
    phase = "degraded";
    risks.unshift(placement.summary);
  } else if (resource.phase === "pending") {
    summary = "Regional reconciliation is pending";
  } else if (resource.phase === "failed") {
    summary = "Regional reconciliation failed";
  }
  if (resource.message && resource.phase !== "ready" && !risks.includes(resource.message)) {
    risks.unshift(resource.message);
  }

  return {
    canonicalName: resource.canonical_name,
    kind: resource.kind,
    name: resource.name,
    generation: resource.generation,
    observedGeneration: resource.observed_generation,
    workloadProfile: resource.workload_profile,
    tablets,
    phase,
    summary,
    risks,
    placement: topology,
    cacheConfiguration: resource.cache_configuration
      ? {
          maxEntriesPerShard: resource.cache_configuration.max_entries_per_shard,
          maxMemoryBytesPerShard: resource.cache_configuration.max_memory_bytes_per_shard,
          maxColdBytesPerShard: resource.cache_configuration.max_cold_bytes_per_shard,
          defaultTTLMS: resource.cache_configuration.default_ttl_ms,
          eviction: resource.cache_configuration.eviction,
          durability: resource.cache_configuration.durability,
          coldLatencyDisclosure: resource.cache_configuration.cold_latency_disclosure,
        }
      : null,
    governance: resource.governance
      ? {
          owner: resource.governance.owner,
          costCenter: resource.governance.cost_center,
          classification: resource.governance.classification,
          tags: { ...resource.governance.tags },
        }
      : null,
  };
}
