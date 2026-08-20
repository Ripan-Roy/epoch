export type DeploymentMode = "embedded" | "standalone" | "cluster" | "managed";

export type DurabilityProfile =
  "volatile" | "replicated_memory" | "local_durable" | "quorum_durable" | "geo_async" | "geo_sync";

export type ResourceKind = "cache" | "stream" | "queue" | "event_bus" | "subscription" | "schema" | "pipe";

export type CreateProfile = "cache" | "stream" | "queue" | "event_bus";

export interface EngineHealth {
  status: string;
  deployment_mode: DeploymentMode;
  profiles: ResourceKind[];
  resource_count: number;
  guarantee_ceiling: DurabilityProfile;
  hosted_control_plane_required: boolean;
}

export interface ResourceSummary {
  name: string;
  kind: ResourceKind;
  durability: DurabilityProfile;
  epoch: number;
}

export interface ResourceCreated {
  name: string;
  resource_epoch: number;
}

export type RegionalResourcePhase = "pending" | "ready" | "degraded" | "failed";
export type RegionalDataKind = "cache" | "table" | "stream" | "queue" | "event_bus";
export type DataClassification = "public" | "internal" | "confidential" | "restricted";
export type CacheEvictionPolicy =
  | "no_eviction"
  | "all_keys_lru"
  | "all_keys_lfu"
  | "all_keys_random"
  | "volatile_lru"
  | "volatile_lfu"
  | "volatile_random"
  | "volatile_ttl";

export interface RegionalCacheConfiguration {
  maxEntriesPerShard: number;
  defaultTTLMS: number | null;
  eviction: CacheEvictionPolicy;
}

export interface RegionalGovernance {
  owner: string;
  costCenter: string;
  classification: DataClassification;
  tags: Record<string, string>;
}

export interface RegionalCostAttribution {
  costCenter: string;
  classification: DataClassification | "unspecified";
  resourceCount: number;
  shardCount: number;
}

export interface RegionalInventory {
  resources: RegionalResource[];
  costAttribution: RegionalCostAttribution[];
}

export interface RegionalGovernanceFilter {
  owner?: string;
  costCenter?: string;
  classification?: DataClassification;
  tags?: Record<string, string>;
}

export interface RegionalTabletPlacement {
  tabletId: string;
  consensusGroupId: string;
  shardIndex: number;
  tabletEpoch: string;
  resourceGeneration: string;
  desiredReplicas: number;
  voterNodeIds: string[];
  leaderNodeId: string | null;
}

export interface RegionalResource {
  canonicalName: string;
  kind: RegionalDataKind;
  name: string;
  generation: string;
  observedGeneration: string;
  workloadProfile: string;
  tablets: RegionalTabletPlacement[];
  phase: RegionalResourcePhase;
  summary: string;
  risks: string[];
  placement: RegionalPlacementEvidence | null;
  cacheConfiguration: RegionalCacheConfiguration | null;
  governance: RegionalGovernance | null;
}

export interface ManagedRegionalTablet {
  tablet_id: string;
  consensus_group_id: string;
  shard_index: number;
  tablet_epoch: string;
  resource_generation: string;
  desired_replicas: number;
  voter_node_ids: string[];
  leader_node_id: string | null;
}

export interface ManagedRegionalNode {
  node_id: string;
  region: string;
  zone: string;
  node_class: string;
  consensus_voter_node_ids: string[];
  max_consensus_groups: number;
  used_consensus_groups: number;
  available_consensus_groups: number;
}

export interface ManagedRegionalPlacement {
  allowed_regions: string[];
  minimum_zones: number;
  required_node_class?: string;
  achieved_zones: number;
  nodes: ManagedRegionalNode[];
}

export interface RegionalPlacementNode {
  nodeId: string;
  region: string;
  zone: string;
  nodeClass: string;
  consensusVoterNodeIds: string[];
  maxConsensusGroups: number;
  usedConsensusGroups: number;
  availableConsensusGroups: number;
}

export interface RegionalPlacementEvidence {
  allowedRegions: string[];
  minimumZones: number;
  requiredNodeClass: string | null;
  achievedZones: number;
  nodes: RegionalPlacementNode[];
}

export interface ManagedRegionalResource {
  canonical_name: string;
  organization: string;
  project: string;
  environment: string;
  namespace: string;
  kind: RegionalDataKind;
  name: string;
  generation: string;
  observed_generation: string;
  workload_profile: string;
  shard_count: number;
  phase: RegionalResourcePhase;
  message?: string;
  tablets: ManagedRegionalTablet[];
  placement?: ManagedRegionalPlacement;
  cache_configuration?: {
    max_entries_per_shard: number;
    default_ttl_ms: number | null;
    eviction: CacheEvictionPolicy;
  };
  governance?: {
    owner: string;
    cost_center: string;
    classification: DataClassification;
    tags: Record<string, string>;
  };
}

export interface ManagedRegionalCostAttribution {
  cost_center: string;
  classification: DataClassification | "unspecified";
  resource_count: number;
  shard_count: number;
}

export interface ManagedRegionalInventory {
  resources: ManagedRegionalResource[];
  count: number;
  cost_attribution: ManagedRegionalCostAttribution[];
}

export interface CacheConfig {
  max_entries: number;
  default_ttl_ms: number | null;
  durability: "volatile";
  eviction: CacheEvictionPolicy;
}

export interface StreamConfig {
  partitions: number;
  durability: "volatile" | "local_durable";
  max_records_per_partition: number | null;
}

export interface QueueConfig {
  durability: "volatile" | "local_durable";
  visibility_timeout_ms: number;
  max_messages: number;
  retry: {
    strategy: "exponential" | "fixed";
    initial_delay_ms: number;
    max_delay_ms: number;
    jitter_percent: number;
    max_attempts: number;
    max_age_ms: number | null;
  };
  dedupe_window_ms: number | null;
}

export interface BusConfig {
  durability: "volatile";
  archive: boolean;
  max_subscriptions?: number;
  max_archive_events?: number;
}

export type ResourceConfig = CacheConfig | StreamConfig | QueueConfig | BusConfig;

export interface CreateResourceInput {
  profile: CreateProfile;
  name: string;
  config: ResourceConfig;
}
