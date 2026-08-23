import { useCallback, useEffect, useState, type FormEvent } from "react";

import {
  apiBaseUrl,
  controlBaseUrl,
  createResource,
  getHealth,
  listRegionalInventory,
  listResources,
} from "./api/client";
import {
  clearBrowserManagedToken,
  loadBrowserManagedToken,
  saveBrowserManagedToken,
} from "./api/managedAuth";
import type {
  CreateResourceInput,
  DurabilityProfile,
  EngineHealth,
  RegionalCostAttribution,
  RegionalGovernanceFilter,
  RegionalResource,
  ResourceKind,
  ResourceSummary,
} from "./api/types";
import { ProfileCreateCard } from "./components/ProfileCreateCard";
import { ThemeToggle } from "./components/ThemeToggle";
import { DocsPage } from "./DocsPage";
import { parseGovernanceTagFilter } from "./governance";
import { profileDefinitions } from "./profileDefinitions";

const refreshIntervalMs = 15_000;
const docsOnly = import.meta.env.VITE_DOCS_ONLY === "true";
const releaseVersion = "0.1.0-alpha.9";

const durabilityRank: Record<DurabilityProfile, number> = {
  volatile: 0,
  replicated_memory: 1,
  local_durable: 2,
  quorum_durable: 3,
  geo_async: 4,
  geo_sync: 5,
};

interface AppRoute {
  page: "console" | "docs";
  section: string | null;
  heading: string | null;
}

interface GovernanceFilterDraft {
  owner: string;
  costCenter: string;
  classification: "" | "public" | "internal" | "confidential" | "restricted";
  tags: string;
}

const emptyGovernanceFilterDraft: GovernanceFilterDraft = {
  owner: "",
  costCenter: "",
  classification: "",
  tags: "",
};

function App() {
  return docsOnly ? <DocumentationApp /> : <EpochApp />;
}

function DocumentationApp() {
  const [route, setRoute] = useState(readDocsRoute);

  useEffect(() => {
    const updateRoute = () => setRoute(readDocsRoute());
    window.addEventListener("hashchange", updateRoute);
    return () => window.removeEventListener("hashchange", updateRoute);
  }, []);

  useEffect(() => {
    document.title = "Epoch Docs · Alpha";
  }, []);

  return (
    <>
      <a className="skip-link" href="#main-content" onClick={focusMainContent}>
        Skip to main content
      </a>

      <DocsHeader />

      <DocsPage section={route.section} heading={route.heading} />

      <footer>
        <div className="docs-header-shell footer__inner">
          <span>Epoch Docs · {releaseVersion}</span>
          <span>Reported state only. No silent guarantee upgrades.</span>
        </div>
      </footer>
    </>
  );
}

function EpochApp() {
  const [route, setRoute] = useState<AppRoute>(readRoute);
  const [health, setHealth] = useState<EngineHealth | null>(null);
  const [resources, setResources] = useState<ResourceSummary[]>([]);
  const [regionalResources, setRegionalResources] = useState<RegionalResource[]>([]);
  const [regionalCostAttribution, setRegionalCostAttribution] = useState<RegionalCostAttribution[]>([]);
  const [governanceFilter, setGovernanceFilter] = useState<RegionalGovernanceFilter>({});
  const [governanceFilterDraft, setGovernanceFilterDraft] =
    useState<GovernanceFilterDraft>(emptyGovernanceFilterDraft);
  const [governanceFilterError, setGovernanceFilterError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [regionalError, setRegionalError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);
  const [managedTokenDraft, setManagedTokenDraft] = useState("");
  const [managedCredentialConfigured, setManagedCredentialConfigured] = useState(
    () => loadBrowserManagedToken() !== null,
  );
  const [managedCredentialError, setManagedCredentialError] = useState<string | null>(null);

  const loadOverview = useCallback(
    async (quiet = false) => {
      if (!quiet) {
        setLoading(true);
      }
      const [nodeResult, regionalResult] = await Promise.allSettled([
        Promise.all([getHealth(), listResources()]),
        listRegionalInventory(governanceFilter),
      ]);
      if (nodeResult.status === "fulfilled") {
        const [nextHealth, nextResources] = nodeResult.value;
        setHealth(nextHealth);
        setResources(nextResources);
        setLoadError(null);
      } else {
        setHealth(null);
        setResources([]);
        setLoadError(
          nodeResult.reason instanceof Error
            ? nodeResult.reason.message
            : "The Epoch node could not be reached.",
        );
      }
      if (regionalResult.status === "fulfilled") {
        setRegionalResources(regionalResult.value.resources);
        setRegionalCostAttribution(regionalResult.value.costAttribution);
        setRegionalError(null);
      } else {
        setRegionalResources([]);
        setRegionalCostAttribution([]);
        setRegionalError(
          regionalResult.reason instanceof Error
            ? regionalResult.reason.message
            : "Regional placement could not be observed.",
        );
      }
      setLastChecked(new Date());
      if (!quiet) {
        setLoading(false);
      }
    },
    [governanceFilter],
  );

  useEffect(() => {
    const updateRoute = () => setRoute(readRoute());
    window.addEventListener("hashchange", updateRoute);
    return () => window.removeEventListener("hashchange", updateRoute);
  }, []);

  useEffect(() => {
    document.title = route.page === "docs" ? "Epoch Docs · Alpha" : "Epoch Console · Alpha";
  }, [route.page]);

  useEffect(() => {
    if (route.page !== "console") {
      return;
    }
    window.requestAnimationFrame(() => {
      window.scrollTo({ top: 0 });
      document.getElementById("main-content")?.focus({ preventScroll: true });
    });
  }, [route.page]);

  useEffect(() => {
    if (route.page !== "console") {
      return;
    }
    void loadOverview();
    const interval = window.setInterval(() => void loadOverview(true), refreshIntervalMs);
    return () => window.clearInterval(interval);
  }, [loadOverview, route.page]);

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

  async function handleManagedCredential(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    try {
      saveBrowserManagedToken(managedTokenDraft);
      setManagedTokenDraft("");
      setManagedCredentialConfigured(true);
      setManagedCredentialError(null);
      await loadOverview();
    } catch (error) {
      setManagedCredentialError(
        error instanceof Error ? error.message : "The managed credential could not be stored.",
      );
    }
  }

  function handleClearManagedCredential() {
    clearBrowserManagedToken();
    setManagedCredentialConfigured(false);
    setManagedCredentialError(null);
    setRegionalResources([]);
    setRegionalCostAttribution([]);
    setRegionalError("A managed-control bearer token is required.");
  }

  function handleGovernanceFilter(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    try {
      const tags = parseGovernanceTagFilter(governanceFilterDraft.tags);
      setGovernanceFilter({
        ...(governanceFilterDraft.owner.trim() ? { owner: governanceFilterDraft.owner.trim() } : {}),
        ...(governanceFilterDraft.costCenter.trim()
          ? { costCenter: governanceFilterDraft.costCenter.trim() }
          : {}),
        ...(governanceFilterDraft.classification
          ? { classification: governanceFilterDraft.classification }
          : {}),
        ...(Object.keys(tags).length > 0 ? { tags } : {}),
      });
      setGovernanceFilterError(null);
    } catch (error) {
      setGovernanceFilterError(error instanceof Error ? error.message : "The governance filter is invalid.");
    }
  }

  function handleClearGovernanceFilter() {
    setGovernanceFilterDraft(emptyGovernanceFilterDraft);
    setGovernanceFilter({});
    setGovernanceFilterError(null);
  }

  const connectionLabel = health ? formatEnum(health.status) : loading ? "Checking" : "Unavailable";
  const connectionTone = health?.status === "ok" ? "good" : loading ? "neutral" : "bad";

  return (
    <>
      <a className="skip-link" href="#main-content" onClick={focusMainContent}>
        Skip to main content
      </a>

      {route.page === "docs" ? <DocsHeader showConsoleLink /> : <ConsoleHeader />}

      {route.page === "docs" ? (
        <DocsPage section={route.section} heading={route.heading} />
      ) : (
        <main id="main-content" tabIndex={-1}>
          <div className="shell" id="top">
            <aside className="alpha-banner" aria-label="Alpha limitations">
              <strong>Evidence before promises.</strong>
              <span>
                Local resources and managed regional placement are reported separately. The Go control plane
                reports only Rust routes and configured topology it actually observed; physical failure-domain
                identity and dynamic membership remain explicit non-claims.
              </span>
            </aside>

            <section className="page-header" aria-labelledby="overview-title">
              <div>
                <h1 id="overview-title">One runtime, four explicit behaviors</h1>
                <p className="hero__lede">
                  Inspect what this node can actually guarantee, then create the workload profile whose
                  semantics fit the job.
                </p>
              </div>
              <div className="hero__actions">
                <span className="endpoint-chip">{apiBaseUrl}</span>
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

            <section className="section resources-section" aria-labelledby="regional-title">
              <div className="section-heading">
                <div>
                  <p className="eyebrow">Regional placement</p>
                  <h2 id="regional-title">Observed catalog and serving risk</h2>
                </div>
                <p>
                  Read through the managed control BFF at <code>{controlBaseUrl}</code>. Desired replicas
                  never count as observed voters.
                </p>
              </div>

              <form className="credential-panel" onSubmit={(event) => void handleManagedCredential(event)}>
                <div>
                  <strong>Managed-control credential</strong>
                  <span>
                    Stored only in this browser tab session; it is never compiled into the console bundle.
                  </span>
                </div>
                <label className="credential-field">
                  <span className="sr-only">Managed-control bearer token</span>
                  <input
                    type="password"
                    autoComplete="off"
                    value={managedTokenDraft}
                    placeholder={managedCredentialConfigured ? "Replace session token" : "Bearer token"}
                    onChange={(event) => {
                      setManagedTokenDraft(event.target.value);
                      setManagedCredentialError(null);
                    }}
                  />
                </label>
                <button className="button button--secondary" type="submit" disabled={!managedTokenDraft}>
                  {managedCredentialConfigured ? "Replace" : "Connect"}
                </button>
                {managedCredentialConfigured ? (
                  <button className="text-button" type="button" onClick={handleClearManagedCredential}>
                    Clear
                  </button>
                ) : null}
                {managedCredentialError ? (
                  <span className="form-error credential-panel__error" role="alert">
                    {managedCredentialError}
                  </span>
                ) : null}
              </form>

              <form className="governance-filter" onSubmit={handleGovernanceFilter}>
                <div>
                  <strong>Governance inventory</strong>
                  <span>Exact-match owner, cost, classification, and tag filters are combined with AND.</span>
                </div>
                <label>
                  <span>Owner</span>
                  <input
                    value={governanceFilterDraft.owner}
                    placeholder="team:payments"
                    onChange={(event) =>
                      setGovernanceFilterDraft((current) => ({ ...current, owner: event.target.value }))
                    }
                  />
                </label>
                <label>
                  <span>Cost center</span>
                  <input
                    value={governanceFilterDraft.costCenter}
                    placeholder="cc-1042"
                    onChange={(event) =>
                      setGovernanceFilterDraft((current) => ({ ...current, costCenter: event.target.value }))
                    }
                  />
                </label>
                <label>
                  <span>Classification</span>
                  <select
                    value={governanceFilterDraft.classification}
                    onChange={(event) =>
                      setGovernanceFilterDraft((current) => ({
                        ...current,
                        classification: event.target.value as GovernanceFilterDraft["classification"],
                      }))
                    }
                  >
                    <option value="">Any</option>
                    <option value="public">Public</option>
                    <option value="internal">Internal</option>
                    <option value="confidential">Confidential</option>
                    <option value="restricted">Restricted</option>
                  </select>
                </label>
                <label>
                  <span>Tags</span>
                  <input
                    value={governanceFilterDraft.tags}
                    placeholder="service=checkout,tier=critical"
                    onChange={(event) =>
                      setGovernanceFilterDraft((current) => ({ ...current, tags: event.target.value }))
                    }
                  />
                </label>
                <button className="button button--secondary" type="submit">
                  Apply filters
                </button>
                <button className="text-button" type="button" onClick={handleClearGovernanceFilter}>
                  Clear filters
                </button>
                {governanceFilterError ? (
                  <span className="form-error" role="alert">
                    {governanceFilterError}
                  </span>
                ) : null}
              </form>

              {regionalError ? (
                <div className="callout callout--warning" role="status">
                  <strong>Regional state unavailable</strong>
                  <span>{regionalError}</span>
                  <span>Local node controls remain available independently.</span>
                </div>
              ) : null}

              {!regionalError && regionalResources.length === 0 ? (
                <div className="empty-state">
                  <strong>No regional resources in the committed catalog.</strong>
                  <span>Create desired state through RegionalAdmin, then refresh placement.</span>
                </div>
              ) : null}

              {regionalCostAttribution.length > 0 ? (
                <div className="governance-summary" aria-label="Filtered cost attribution">
                  {regionalCostAttribution.map((entry) => (
                    <article key={`${entry.costCenter}:${entry.classification}`}>
                      <span>{entry.costCenter}</span>
                      <strong>{entry.resourceCount} resources</strong>
                      <small>
                        {entry.shardCount} shards · {formatEnum(entry.classification)}
                      </small>
                    </article>
                  ))}
                </div>
              ) : null}

              {regionalResources.length > 0 ? (
                <div className="table-wrap">
                  <table>
                    <caption className="sr-only">
                      Regional resources, observed generations, placements, and risks
                    </caption>
                    <thead>
                      <tr>
                        <th scope="col">Resource</th>
                        <th scope="col">Generation</th>
                        <th scope="col">Governance</th>
                        <th scope="col">State</th>
                        <th scope="col">Observed placement</th>
                        <th scope="col">Remaining risk</th>
                      </tr>
                    </thead>
                    <tbody>
                      {regionalResources.map((resource) => (
                        <tr key={resource.canonicalName}>
                          <th scope="row">
                            <span className="resource-name">{resource.name}</span>
                            <code className="resource-path">{resource.canonicalName}</code>
                            {resource.cacheConfiguration ? (
                              <span className="resource-generation-detail">
                                {formatEnum(resource.cacheConfiguration.eviction)} ·{" "}
                                {resource.cacheConfiguration.maxEntriesPerShard} entries/shard ·{" "}
                                {resource.cacheConfiguration.maxMemoryBytesPerShard === null
                                  ? "memory bytes unbounded"
                                  : `${resource.cacheConfiguration.maxMemoryBytesPerShard} memory bytes/shard`}
                                {" · "}
                                {resource.cacheConfiguration.maxColdBytesPerShard === null
                                  ? "cold class disabled"
                                  : `${resource.cacheConfiguration.maxColdBytesPerShard} cold-class bytes/shard`}
                                {" · "}
                                {formatEnum(resource.cacheConfiguration.durability)} requested ·{" "}
                                {resource.cacheConfiguration.coldLatencyDisclosure === "disabled"
                                  ? "cold latency unavailable"
                                  : "cold reads report observed local-file microseconds; no SLO"}
                                {" · "}
                                {resource.cacheConfiguration.defaultTTLMS === null
                                  ? "no default TTL"
                                  : `${resource.cacheConfiguration.defaultTTLMS} ms default TTL`}
                              </span>
                            ) : null}
                          </th>
                          <td>
                            {resource.generation}
                            <span className="resource-generation-detail">
                              observed {resource.observedGeneration}
                            </span>
                          </td>
                          <td>
                            {resource.governance ? (
                              <div className="governance-cell">
                                <strong>{resource.governance.owner}</strong>
                                <span>
                                  {resource.governance.costCenter} ·{" "}
                                  {formatEnum(resource.governance.classification)}
                                </span>
                                <span>
                                  {Object.entries(resource.governance.tags)
                                    .map(([key, value]) => `${key}=${value}`)
                                    .join(" · ") || "No custom tags"}
                                </span>
                              </div>
                            ) : (
                              <span className="risk-copy">Legacy resource: governance unassigned</span>
                            )}
                          </td>
                          <td>
                            <span className="phase-token" data-phase={resource.phase}>
                              {formatEnum(resource.phase)}
                            </span>
                          </td>
                          <td>
                            <strong className="placement-summary">{resource.summary}</strong>
                            <ul className="tablet-list">
                              {resource.tablets.map((tablet) => (
                                <li key={tablet.tabletId}>
                                  Shard {tablet.shardIndex} · voters{" "}
                                  {tablet.voterNodeIds.length > 0 ? tablet.voterNodeIds.join(", ") : "none"} ·
                                  leader {tablet.leaderNodeId ?? "none"}
                                </li>
                              ))}
                              {resource.placement?.nodes.map((node) => (
                                <li key={`node-${node.nodeId}`}>
                                  Node {node.nodeId} · {node.region}/{node.zone} · {node.nodeClass} · capacity{" "}
                                  {node.availableConsensusGroups}/{node.maxConsensusGroups} groups available
                                </li>
                              ))}
                            </ul>
                          </td>
                          <td>
                            <ul className="risk-list">
                              {resource.risks.map((risk) => (
                                <li key={risk}>{risk}</li>
                              ))}
                            </ul>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : null}
            </section>

            <section className="section" aria-labelledby="create-title">
              <div className="section-heading">
                <div>
                  <p className="eyebrow">Create</p>
                  <h2 id="create-title">Choose behavior, not a vendor analogy</h2>
                </div>
                <p>
                  These alpha forms target the standalone node API; regional desired state uses RegionalAdmin.
                </p>
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
                  <p className="eyebrow">Inventory</p>
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
      )}

      <footer>
        <div className={route.page === "docs" ? "docs-header-shell footer__inner" : "shell footer__inner"}>
          <span>
            Epoch {route.page === "docs" ? "Docs" : "Console"} · {releaseVersion}
          </span>
          <span>Reported state only. No silent guarantee upgrades.</span>
        </div>
      </footer>
    </>
  );
}

function DocsHeader({ showConsoleLink = false }: { showConsoleLink?: boolean }) {
  return (
    <header className="topbar">
      <div className="docs-header-shell topbar__inner">
        <div className="topbar__left">
          <a className="brand" href="#/docs" aria-label="Epoch documentation home">
            <span className="brand__mark" aria-hidden="true">
              E
            </span>
            <strong>Epoch</strong>
          </a>
          <span className="brand__context">Docs</span>
        </div>
        <div className="topbar__right">
          <nav className="topnav topnav--docs" aria-label="Primary navigation">
            <a href="#/docs/quickstart">Quickstart</a>
            <a href="#/docs/sdk-reference">SDKs</a>
            {showConsoleLink ? <a href="#/console">Console</a> : null}
            <a href="https://github.com/Ripan-Roy/epoch" target="_blank" rel="noreferrer">
              GitHub <span aria-hidden="true">↗</span>
            </a>
          </nav>
          <span className="version-tag">v{releaseVersion}</span>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}

function ConsoleHeader() {
  return (
    <header className="topbar">
      <div className="shell topbar__inner">
        <div className="topbar__left">
          <a className="brand" href="#/console" aria-label="Epoch runtime console home">
            <span className="brand__mark" aria-hidden="true">
              E
            </span>
            <strong>Epoch</strong>
          </a>
          <span className="brand__context">Console</span>
        </div>
        <div className="topbar__right">
          <nav className="topnav" aria-label="Primary navigation">
            <a href="#/console" aria-current="page">
              Console
            </a>
            <a href="#/docs">Docs</a>
            <a href="https://github.com/Ripan-Roy/epoch" target="_blank" rel="noreferrer">
              GitHub <span aria-hidden="true">↗</span>
            </a>
          </nav>
          <span className="version-tag">v{releaseVersion}</span>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}

function readRoute(): AppRoute {
  if (!window.location.hash || window.location.hash === "#") {
    return import.meta.env.VITE_DEFAULT_PAGE === "docs"
      ? { page: "docs", section: null, heading: null }
      : { page: "console", section: null, heading: null };
  }
  const [page, section, heading] = window.location.hash.replace(/^#\/?/, "").split("/");
  if (page === "docs") {
    return { page: "docs", section: section || null, heading: heading || null };
  }
  return { page: "console", section: null, heading: null };
}

function readDocsRoute(): { section: string | null; heading: string | null } {
  const [page, section, heading] = window.location.hash.replace(/^#\/?/, "").split("/");
  if (page !== "docs") {
    return { section: null, heading: null };
  }
  return { section: section || null, heading: heading || null };
}

function focusMainContent() {
  window.requestAnimationFrame(() => {
    const main = document.getElementById("main-content");
    main?.scrollIntoView();
    main?.focus({ preventScroll: true });
  });
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
