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

export interface CacheConfig {
  max_entries: number;
  default_ttl_ms: number | null;
  durability: "volatile";
  eviction:
    | "no_eviction"
    | "all_keys_lru"
    | "all_keys_lfu"
    | "all_keys_random"
    | "volatile_lru"
    | "volatile_lfu"
    | "volatile_random"
    | "volatile_ttl";
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
}

export type ResourceConfig = CacheConfig | StreamConfig | QueueConfig | BusConfig;

export interface CreateResourceInput {
  profile: CreateProfile;
  name: string;
  config: ResourceConfig;
}
