import assert from "node:assert/strict";
import test from "node:test";

import { assessRegionalPlacement, mapRegionalInventory } from "../src/regionalPlacement.ts";
import type { RegionalTabletPlacement } from "../src/api/types.ts";

const completeTablet: RegionalTabletPlacement = {
  tabletId: "10",
  consensusGroupId: "20",
  shardIndex: 0,
  tabletEpoch: "1",
  resourceGeneration: "4",
  desiredReplicas: 3,
  voterNodeIds: ["1", "2", "3"],
  leaderNodeId: "2",
};

test("empty placement remains pending without a replica claim", () => {
  assert.deepEqual(assessRegionalPlacement([], 1), {
    phase: "pending",
    summary: "No serving placement observed",
    risks: ["The managed control plane has not observed a serving route for this resource."],
  });
});

test("missing voters and leaders are degraded", () => {
  const placement = {
    ...completeTablet,
    voterNodeIds: ["1", "2"],
    leaderNodeId: null,
  };
  const assessment = assessRegionalPlacement([placement], 1);
  assert.equal(assessment.phase, "degraded");
  assert.match(assessment.summary, /2\/3 voters/);
  assert.ok(assessment.risks.some((risk) => risk.includes("no leader")));
  assert.ok(assessment.risks.some((risk) => risk.includes("missing 1 voter")));
});

test("complete observed voters are ready but topology remains an explicit risk", () => {
  const assessment = assessRegionalPlacement([completeTablet], 1);
  assert.equal(assessment.phase, "ready");
  assert.equal(assessment.summary, "1 shard serving · 3/3 voters observed");
  assert.deepEqual(assessment.risks, [
    "Node count is observed; zone, rack, and failure-domain separation are not yet verified.",
  ]);
});

test("missing catalog shards fail closed as degraded", () => {
  const assessment = assessRegionalPlacement([completeTablet], 2);
  assert.equal(assessment.phase, "degraded");
  assert.ok(assessment.risks.some((risk) => risk.includes("1 of 2 catalog shards")));
});

test("managed inventory maps browser-safe identifiers without contacting data nodes", () => {
  const mapped = mapRegionalInventory({
    canonical_name: "acme/payments/production/orders/stream/events",
    organization: "acme",
    project: "payments",
    environment: "production",
    namespace: "orders",
    kind: "stream",
    name: "events",
    generation: "9007199254740993",
    observed_generation: "9007199254740993",
    workload_profile: "stream_log",
    shard_count: 1,
    phase: "ready",
    message: "regional placement converged",
    tablets: [
      {
        tablet_id: "9007199254740994",
        consensus_group_id: "9007199254740995",
        shard_index: 0,
        tablet_epoch: "9007199254740996",
        resource_generation: "9007199254740993",
        desired_replicas: 3,
        voter_node_ids: ["9007199254740997", "9007199254740998", "9007199254740999"],
        leader_node_id: "9007199254740998",
      },
    ],
  });

  assert.equal(mapped.generation, "9007199254740993");
  assert.equal(mapped.phase, "ready");
  assert.equal(mapped.tablets[0]?.tabletId, "9007199254740994");
  assert.equal(mapped.tablets[0]?.leaderNodeId, "9007199254740998");
});

test("managed ready state fails closed when placement evidence is incomplete", () => {
  const mapped = mapRegionalInventory({
    canonical_name: "acme/payments/staging/orders/queue/jobs",
    organization: "acme",
    project: "payments",
    environment: "staging",
    namespace: "orders",
    kind: "queue",
    name: "jobs",
    generation: "1",
    observed_generation: "1",
    workload_profile: "work_queue",
    shard_count: 1,
    phase: "ready",
    message: "regional placement converged",
    tablets: [],
  });

  assert.equal(mapped.phase, "degraded");
  assert.ok(mapped.risks.some((risk) => risk.includes("No serving placement")));
});
