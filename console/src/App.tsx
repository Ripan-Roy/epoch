import { useCallback, useEffect, useState } from "react";

import { apiBaseUrl, createResource, getHealth, listResources } from "./api/client";
import type {
  CreateResourceInput,
  DurabilityProfile,
  EngineHealth,
  ResourceKind,
  ResourceSummary,
} from "./api/types";
import { ProfileCreateCard } from "./components/ProfileCreateCard";
import { profileDefinitions } from "./profileDefinitions";

const refreshIntervalMs = 15_000;

const durabilityRank: Record<DurabilityProfile, number> = {
  volatile: 0,
  replicated_memory: 1,
  local_durable: 2,
  quorum_durable: 3,
  geo_async: 4,
  geo_sync: 5,
};

function App() {
  const [health, setHealth] = useState<EngineHealth | null>(null);
  const [resources, setResources] = useState<ResourceSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);

  const loadOverview = useCallback(async (quiet = false) => {
    if (!quiet) {
      setLoading(true);
    }
    try {
      const [nextHealth, nextResources] = await Promise.all([getHealth(), listResources()]);
      setHealth(nextHealth);
      setResources(nextResources);
      setLoadError(null);
      setLastChecked(new Date());
    } catch (caught) {
      setHealth(null);
      setResources([]);
      setLoadError(caught instanceof Error ? caught.message : "The Epoch node could not be reached.");
      setLastChecked(new Date());
    } finally {
      if (!quiet) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void loadOverview();
    const interval = window.setInterval(() => void loadOverview(true), refreshIntervalMs);
    return () => window.clearInterval(interval);
  }, [loadOverview]);

  const connected = health?.status === "ok";

  async function handleCreate(input: CreateResourceInput) {
    if (!connected) {
      throw new Error("The node must report healthy before the console can create a resource.");
    }
    const created = await createResource(input);
    setNotice(
      `${profileLabel(input.profile)} “${created.name}” created at resource epoch ${created.resource_epoch}.`,
    );
    await loadOverview(true);
  }

  const connectionLabel = health ? formatEnum(health.status) : loading ? "Checking" : "Unavailable";
  const connectionTone = health?.status === "ok" ? "good" : loading ? "neutral" : "bad";

  return (
    <>
      <a className="skip-link" href="#main-content">
        Skip to main content
      </a>

      <header className="topbar">
        <div className="shell topbar__inner">
          <a className="brand" href="#top" aria-label="Epoch console home">
            <span className="brand__mark" aria-hidden="true">
              E
            </span>
            <span>
              <strong>Epoch</strong>
              <small>runtime console</small>
            </span>
          </a>
          <span className="alpha-pill">FOUNDATION ALPHA</span>
        </div>
      </header>

      <main id="main-content">
        <div className="shell" id="top">
          <aside className="alpha-banner" aria-label="Alpha limitations">
            <strong>Evidence before promises.</strong>
            <span>
              This console reflects one local Rust node. Streams and Queues can opt into WAL-backed local
              durability; Cache, Event Bus, and volatile resources remain process-local. Replication and
              quorum are not wired yet.
            </span>
          </aside>

          <section className="hero" aria-labelledby="overview-title">
            <div>
              <p className="eyebrow">NODE OVERVIEW</p>
              <h1 id="overview-title">One runtime, four explicit behaviors.</h1>
              <p className="hero__lede">
                Inspect what this node can actually guarantee, then create the workload profile whose
                semantics fit the job.
              </p>
            </div>
            <div className="hero__actions">
              <code>{apiBaseUrl}</code>
              <button
                className="button button--secondary"
                type="button"
                onClick={() => void loadOverview()}
                disabled={loading}
              >
                {loading ? "Checking…" : "Refresh node"}
              </button>
            </div>
          </section>

          {loadError ? (
            <div className="callout callout--error" role="alert">
              <strong>Node unavailable</strong>
              <span>{loadError}</span>
              <span>
                Start <code>epoch-node</code> on port 7601, then refresh.
              </span>
            </div>
          ) : null}

          {notice ? (
            <div className="callout callout--success" role="status" aria-live="polite">
              <strong>Resource accepted</strong>
              <span>{notice}</span>
              <button type="button" className="text-button" onClick={() => setNotice(null)}>
                Dismiss
              </button>
            </div>
          ) : null}

          <section className="status-grid" aria-label="Node status" aria-busy={loading}>
            <StatusCard label="Connection" value={connectionLabel} tone={connectionTone}>
              {lastChecked
                ? `Checked ${formatCheckTime(lastChecked)}`
                : "Waiting for the first health response"}
            </StatusCard>
            <StatusCard label="Deployment" value={health ? formatEnum(health.deployment_mode) : "Unknown"}>
              {health ? deploymentDescription(health) : "No deployment mode has been observed"}
            </StatusCard>
            <StatusCard
              label="Reported ceiling"
              value={health ? formatEnum(health.guarantee_ceiling) : "Unknown"}
            >
              {health
                ? guaranteeDescription(health.guarantee_ceiling)
                : "The console will not infer a guarantee"}
            </StatusCard>
            <StatusCard label="Live resources" value={health ? String(health.resource_count) : "—"}>
              {health && health.profiles.length > 0
                ? `Active: ${health.profiles.map(profileLabel).join(", ")}`
                : "No active resource profiles reported"}
            </StatusCard>
          </section>

          <section className="section" aria-labelledby="create-title">
            <div className="section-heading">
              <div>
                <p className="eyebrow">CREATE</p>
                <h2 id="create-title">Choose behavior, not a vendor analogy.</h2>
              </div>
              <p>Alpha forms intentionally expose only the local guarantees implemented by this node.</p>
            </div>
            <div className="profile-grid">
              {profileDefinitions.map((definition) => (
                <ProfileCreateCard
                  key={definition.profile}
                  definition={definition}
                  connected={connected}
                  onCreate={handleCreate}
                />
              ))}
            </div>
          </section>

          <section className="section resources-section" aria-labelledby="resources-title">
            <div className="section-heading">
              <div>
                <p className="eyebrow">INVENTORY</p>
                <h2 id="resources-title">Resources reported by this process</h2>
              </div>
              <p>
                Configured durability and the reported node ceiling are separate; neither is independent
                evidence.
              </p>
            </div>

            {connected && resources.length === 0 ? (
              <div className="empty-state">
                <strong>No resources yet.</strong>
                <span>Create one above; the inventory refreshes after the node accepts it.</span>
              </div>
            ) : null}

            {resources.length > 0 ? (
              <div className="table-wrap">
                <table>
                  <caption className="sr-only">Epoch resources and configured guarantees</caption>
                  <thead>
                    <tr>
                      <th scope="col">Resource</th>
                      <th scope="col">Profile</th>
                      <th scope="col">Configured durability</th>
                      <th scope="col">Epoch</th>
                      <th scope="col">Console check</th>
                    </tr>
                  </thead>
                  <tbody>
                    {resources.map((resource) => {
                      const assessment = assessDurability(resource, health);
                      return (
                        <tr key={`${resource.kind}:${resource.name}`}>
                          <th scope="row">{resource.name}</th>
                          <td>
                            <span className="profile-token" data-kind={resource.kind}>
                              {profileLabel(resource.kind)}
                            </span>
                          </td>
                          <td>{formatEnum(resource.durability)}</td>
                          <td>{resource.epoch}</td>
                          <td>
                            <span className="assessment" data-tone={assessment.tone}>
                              {assessment.label}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : null}
          </section>
        </div>
      </main>

      <footer>
        <div className="shell footer__inner">
          <span>Epoch Console · 0.1 alpha</span>
          <span>Reported state only. No silent guarantee upgrades.</span>
        </div>
      </footer>
    </>
  );
}

function StatusCard({
  label,
  value,
  tone = "neutral",
  children,
}: {
  label: string;
  value: string;
  tone?: "good" | "bad" | "neutral";
  children: string;
}) {
  return (
    <article className="status-card">
      <div className="status-card__label">
        <span className="status-dot" data-tone={tone} aria-hidden="true" />
        {label}
      </div>
      <strong>{value}</strong>
      <p>{children}</p>
    </article>
  );
}

function assessDurability(
  resource: ResourceSummary,
  health: EngineHealth | null,
): { label: string; tone: "good" | "warn" | "bad" } {
  if (!health) {
    return { label: "Not verified", tone: "warn" };
  }
  if (durabilityRank[resource.durability] > durabilityRank[health.guarantee_ceiling]) {
    return { label: "Exceeds node ceiling", tone: "bad" };
  }
  if (resource.durability === "volatile") {
    return { label: "Configured volatile", tone: "warn" };
  }
  if (health.deployment_mode === "standalone" || health.deployment_mode === "embedded") {
    return { label: "Within ceiling · unverified", tone: "warn" };
  }
  return { label: "Within reported ceiling", tone: "good" };
}

function deploymentDescription(health: EngineHealth): string {
  switch (health.deployment_mode) {
    case "embedded":
      return "Runs inside one application process";
    case "standalone":
      return "One process and one machine failure domain";
    case "cluster":
      return "Cluster mode reported; inspect placement before trusting quorum";
    case "managed":
      return health.hosted_control_plane_required
        ? "Managed mode; hosted control plane required"
        : "Managed topology reported by the node";
  }
}

function guaranteeDescription(profile: DurabilityProfile): string {
  switch (profile) {
    case "volatile":
      return "Acknowledged state may be lost on process failure";
    case "replicated_memory":
      return "Memory replicas only; simultaneous power loss remains exposed";
    case "local_durable":
      return "Node-reported class; verify commit and recovery evidence";
    case "quorum_durable":
      return "Maximum reported class; achieved placement still matters";
    case "geo_async":
      return "Regional disaster recovery has a non-zero replication RPO";
    case "geo_sync":
      return "Cross-region commit latency and availability trade-offs apply";
  }
}

function profileLabel(profile: ResourceKind | CreateResourceInput["profile"]): string {
  switch (profile) {
    case "cache":
      return "Cache";
    case "stream":
      return "Stream";
    case "queue":
      return "Queue";
    case "event_bus":
      return "Event Bus";
    case "subscription":
      return "Subscription";
    case "schema":
      return "Schema";
    case "pipe":
      return "Pipe";
  }
}

function formatEnum(value: string): string {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatCheckTime(date: Date): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

export default App;
