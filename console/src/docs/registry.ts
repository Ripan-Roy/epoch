import type { ReactNode } from "react";

import { repositoryUrl } from "./content";
import {
  BackupRestoreBody,
  ClusterMilestoneBody,
  ConsensusRecoveryBody,
  DeploymentBody,
  GuardedUpgradeBody,
  GuaranteesBody,
  OverviewBody,
  ProtocolCompatibilityBody,
  QuickstartBody,
  ReferenceBody,
  ResourceGovernanceBody,
  RegionalBusBody,
  RegionalCacheBody,
  RegionalQueueBody,
  RegionalStreamBody,
  RestartBody,
  SdkReferenceBody,
  VoterReplacementBody,
} from "./pages";

export type DocsPageId =
  | "overview"
  | "quickstart"
  | "restart"
  | "guarantees"
  | "cluster-milestone"
  | "deployment"
  | "guarded-upgrades"
  | "voter-replacement"
  | "backup-restore"
  | "consensus-recovery"
  | "resource-governance"
  | "regional-stream"
  | "regional-queue"
  | "regional-cache"
  | "regional-bus"
  | "protocol-compatibility"
  | "sdk-reference"
  | "reference";

export interface DocsHeading {
  id: string;
  label: string;
}

export interface DocsPageMeta {
  id: DocsPageId;
  group: string;
  label: string;
  title: string;
  summary: string;
  headings: ReadonlyArray<DocsHeading>;
  Body: () => ReactNode;
}

const regionalHeadings: ReadonlyArray<DocsHeading> = [
  { id: "provision", label: "1. Provision" },
  { id: "install", label: "2. Prepare the SDK" },
  { id: "example", label: "3. Run the example" },
  { id: "guarantees", label: "What the client guarantees" },
  { id: "boundary", label: "Current boundary" },
];

const regionalBusHeadings: ReadonlyArray<DocsHeading> = [
  { id: "provision", label: "1. Provision" },
  { id: "install", label: "2. Prepare the SDK" },
  { id: "example", label: "3. Run the example" },
  { id: "guarantees", label: "What the client guarantees" },
  { id: "epoch-targets", label: "Queue & Stream targets" },
  { id: "managed-targets", label: "Managed targets" },
  { id: "integrations", label: "Integration state" },
  { id: "signed-webhooks", label: "Signed webhooks" },
  { id: "boundary", label: "Current boundary" },
];

export const docsPages: ReadonlyArray<DocsPageMeta> = [
  {
    id: "overview",
    group: "Get started",
    label: "Overview",
    title: "Epoch documentation",
    summary:
      "Create a durable Stream and Work Queue, move real events through both, restart the process, and verify exactly what survived — using the SDK you ship.",
    headings: [
      { id: "start-here", label: "Start here" },
      { id: "regional-guides", label: "Regional SDK guides" },
    ],
    Body: OverviewBody,
  },
  {
    id: "quickstart",
    group: "Get started",
    label: "Quickstart",
    title: "Quickstart",
    summary:
      "Run one local node, create a Stream and a Work Queue with explicit local durability, and move real records through both.",
    headings: [
      { id: "start-node", label: "1. Start a node" },
      { id: "install-sdk", label: "2. Install the SDK" },
      { id: "write-example", label: "3. Write the example" },
      { id: "run-seed", label: "4. Run the first half" },
    ],
    Body: QuickstartBody,
  },
  {
    id: "restart",
    group: "Get started",
    label: "Restart verification",
    title: "Restart verification",
    summary: "Use the same bytes, not a fresh node. Then read what actually came back.",
    headings: [
      { id: "restart-node", label: "1. Restart the node" },
      { id: "what-survives", label: "What the verify run proves" },
    ],
    Body: RestartBody,
  },
  {
    id: "guarantees",
    group: "Core concepts",
    label: "Guarantees & errors",
    title: "Guarantees and errors",
    summary:
      "Local durable is deliberately narrow. This page states the edge of the claim in both directions.",
    headings: [
      { id: "scope", label: "What local durable covers" },
      { id: "errors", label: "Error contract" },
    ],
    Body: GuaranteesBody,
  },
  {
    id: "cluster-milestone",
    group: "Core concepts",
    label: "Regional runtime",
    title: "Regional runtime milestone",
    summary:
      "One Catalog assigns independent three- or five-voter profile groups across N physical nodes. This page records what that does and does not mean today.",
    headings: [
      { id: "scope", label: "What it does today" },
      { id: "evidence", label: "Observed evidence" },
      { id: "proofs", label: "Run the proofs" },
    ],
    Body: ClusterMilestoneBody,
  },
  {
    id: "deployment",
    group: "Core concepts",
    label: "Deploy & operate",
    title: "Deploy and operate Epoch",
    summary:
      "Run the secured N-node regional system, manage it with the CLI, and ingest crash-safe HTTP, object, CDC, and Kafka sources.",
    headings: [
      { id: "kubernetes", label: "Install on Kubernetes" },
      { id: "live-campaign", label: "Live lifecycle proof" },
      { id: "release-artifacts", label: "Release artifacts" },
      { id: "cli", label: "Management CLI" },
      { id: "source-connectors", label: "Source connectors" },
      { id: "soak", label: "Fault & soak evidence" },
      { id: "operations", label: "Evidence and limits" },
    ],
    Body: DeploymentBody,
  },
  {
    id: "backup-restore",
    group: "Core concepts",
    label: "Backup & restore",
    title: "Regional backup and restore",
    summary:
      "Capture every distributed tablet at a quorum-confirmed boundary, encrypt the semantic artifact, and restore a fresh regional cluster.",
    headings: [
      { id: "capture", label: "Distributed capture" },
      { id: "encryption", label: "Encryption & retention" },
      { id: "restore", label: "Fresh-cluster restore" },
      { id: "limits", label: "Current limits" },
    ],
    Body: BackupRestoreBody,
  },
  {
    id: "guarded-upgrades",
    group: "Core concepts",
    label: "Guarded upgrades",
    title: "Guarded data-plane upgrades",
    summary:
      "Gate every image transition on a fresh backup, cluster-wide Raft evidence, leader drain, one ordinal, readiness, verification, and guarded rollback.",
    headings: [
      { id: "configure", label: "Configure guardrails" },
      { id: "forward", label: "One-ordinal path" },
      { id: "rollback", label: "Stop & rollback" },
      { id: "observe", label: "Observe the plan" },
    ],
    Body: GuardedUpgradeBody,
  },
  {
    id: "voter-replacement",
    group: "Core concepts",
    label: "Voter replacement",
    title: "Learner-first voter replacement",
    summary:
      "Commit one exact target, catch up the incoming learner, enter joint consensus, finalize Catalog, stop the removed host, and reopen the new voter set.",
    headings: [
      { id: "contract", label: "Safety contract" },
      { id: "plan", label: "Commit a plan" },
      { id: "observe", label: "Observe transition" },
      { id: "recovery", label: "Recovery & limits" },
    ],
    Body: VoterReplacementBody,
  },
  {
    id: "consensus-recovery",
    group: "Core concepts",
    label: "Consensus recovery",
    title: "Consensus recovery",
    summary: "Checkpoint, compact, catch up, and reopen from durable three- or five-voter groups.",
    headings: [
      { id: "how-it-works", label: "How a checkpoint is taken" },
      { id: "inspect", label: "Inspect a local checkpoint" },
    ],
    Body: ConsensusRecoveryBody,
  },
  {
    id: "resource-governance",
    group: "Core concepts",
    label: "Resource governance",
    title: "Resource governance",
    summary:
      "Require owner, cost center, classification, and tags; filter authorized inventory and explain allocation drivers.",
    headings: [
      { id: "contract", label: "Governance contract" },
      { id: "filter", label: "Inventory filters" },
      { id: "cost", label: "Cost attribution" },
      { id: "recovery", label: "Recovery evidence" },
    ],
    Body: ResourceGovernanceBody,
  },
  {
    id: "regional-stream",
    group: "SDK guides",
    label: "Regional Stream",
    title: "Regional Stream SDK",
    summary:
      "Build fenced producers, transactions, compaction, tiering, automatic capture, replication, and logical superstreams over replicated partitions.",
    headings: regionalHeadings,
    Body: RegionalStreamBody,
  },
  {
    id: "regional-queue",
    group: "SDK guides",
    label: "Regional Queue",
    title: "Regional Queue SDK",
    summary:
      "Run sessions, deferred retrieval, correlation, fair dispatch, overflow, and crash-safe DLQ forwarding through one fenced API.",
    headings: regionalHeadings,
    Body: RegionalQueueBody,
  },
  {
    id: "regional-cache",
    group: "SDK guides",
    label: "Regional Cache",
    title: "Regional Cache SDK",
    summary: "Compare-and-set, transactions, fenced locks, deterministic expiry, and recovery.",
    headings: regionalHeadings,
    Body: RegionalCacheBody,
  },
  {
    id: "regional-bus",
    group: "SDK guides",
    label: "Regional Event Bus",
    title: "Regional Event Bus SDK",
    summary:
      "Route, long-poll, redrive, manage schemas and connectors, and deliver to native or authenticated managed targets through one replicated Bus.",
    headings: regionalBusHeadings,
    Body: RegionalBusBody,
  },
  {
    id: "sdk-reference",
    group: "Reference",
    label: "SDK reference",
    title: "SDK reference",
    summary:
      "The same operation, native to each ecosystem. Every implemented standalone operation has a Go, Java, and Python entry point.",
    headings: [
      { id: "surface", label: "Implemented operations" },
      { id: "conventions", label: "Conventions" },
    ],
    Body: SdkReferenceBody,
  },
  {
    id: "protocol-compatibility",
    group: "Reference",
    label: "Redis, Kafka & AMQP",
    title: "Protocol compatibility",
    summary:
      "Connect selected Redis, Apache Kafka, and RabbitMQ clients to Epoch through one bounded, stateless gateway — with an exact support matrix.",
    headings: [
      { id: "run", label: "Run the gateway" },
      { id: "redis", label: "Redis / RESP" },
      { id: "kafka", label: "Apache Kafka" },
      { id: "amqp", label: "RabbitMQ / AMQP" },
      { id: "migration-scan", label: "Migration scanner" },
      { id: "security", label: "Security & boundaries" },
    ],
    Body: ProtocolCompatibilityBody,
  },
  {
    id: "reference",
    group: "Reference",
    label: "Design reference",
    title: "Design reference",
    summary: "These repository documents own the API, semantic, and evidence contracts.",
    headings: [
      { id: "contracts", label: "Core contracts" },
      { id: "sdk-contracts", label: "SDK contracts" },
      { id: "runtime", label: "Runtime and recovery" },
      { id: "stream-design", label: "Stream design decisions" },
      { id: "workload-design", label: "Queue, Cache, Event Bus" },
    ],
    Body: ReferenceBody,
  },
];

export const docsGroups: ReadonlyArray<{ label: string; pages: ReadonlyArray<DocsPageMeta> }> = [
  "Get started",
  "Core concepts",
  "SDK guides",
  "Reference",
].map((label) => ({
  label,
  pages: docsPages.filter((page) => page.group === label),
}));

export function findDocsPage(id: string | null | undefined): DocsPageMeta {
  return docsPages.find((page) => page.id === id) ?? docsPages[0]!;
}

export function isDocsPageId(candidate: string | null | undefined): candidate is DocsPageId {
  return docsPages.some((page) => page.id === candidate);
}

export const editPageUrl = `${repositoryUrl}/edit/main/console/src/docs/pages.tsx`;
