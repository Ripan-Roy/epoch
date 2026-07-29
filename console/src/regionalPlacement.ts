import type {
  ManagedRegionalResource,
  RegionalResource,
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
  return {
    phase: "ready",
    summary: `${shardLabel} serving · ${voterSummary} observed`,
    risks: ["Node count is observed; zone, rack, and failure-domain separation are not yet verified."],
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
  const placement = assessRegionalPlacement(tablets, resource.shard_count);
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
  };
}
