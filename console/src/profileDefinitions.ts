import type { CreateProfile } from "./api/types";

export interface ProfileDefinition {
  profile: CreateProfile;
  eyebrow: string;
  title: string;
  description: string;
  guarantee: string;
  caveat: string;
}

export const profileDefinitions: ProfileDefinition[] = [
  {
    profile: "cache",
    eyebrow: "CACHE + STATE",
    title: "Fast state",
    description: "Memory-first values, expiry, eviction, counters, and shard-local atomic operations.",
    guarantee: "Configured: volatile",
    caveat: "Acknowledged cache writes can be lost when this process exits.",
  },
  {
    profile: "stream",
    eyebrow: "STREAM LOG",
    title: "Replayable events",
    description: "Partitioned, ordered records with offsets, retention, and consumer progress.",
    guarantee: "Selectable: local durable or volatile",
    caveat: "Local durability fsyncs one node; it does not provide replication or machine-loss protection.",
  },
  {
    profile: "queue",
    eyebrow: "WORK QUEUE",
    title: "Reliable work",
    description: "Lease-based delivery with retry, scheduling, acknowledgements, and dead letters.",
    guarantee: "Selectable: local durable or volatile",
    caveat: "Local durability survives process restart on one machine; duplicate delivery remains possible.",
  },
  {
    profile: "event_bus",
    eyebrow: "EVENT BUS",
    title: "Routed events",
    description: "Filtered fan-out into local queues and streams with an optional replay archive.",
    guarantee: "Configured: volatile",
    caveat: "The archive is process-local; recovery and external target delivery are not claimed.",
  },
];
