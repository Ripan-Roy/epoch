import { useEffect, useRef, useState, type KeyboardEvent } from "react";

import goSource from "./quickstarts/quickstart.go?raw";
import javaSource from "./quickstarts/Quickstart.java?raw";
import pythonSource from "./quickstarts/quickstart.py?raw";

const repositoryUrl = "https://github.com/Ripan-Roy/epoch";
const repositoryDocsUrl = `${repositoryUrl}/blob/main/docs`;

type LanguageId = "go" | "java" | "python";

interface LanguageGuide {
  id: LanguageId;
  label: string;
  version: string;
  setupTitle: string;
  setup: string;
  filename: string;
  source: string;
  run: string;
  errorType: string;
  errorDetail: string;
}

const nodeStart = `git clone https://github.com/Ripan-Roy/epoch.git
cd epoch
cargo run -p epoch-node -- --data-dir .epoch`;

const nodeRestart = `# In the node terminal, press Ctrl-C, then restart with the same data directory:
cargo run -p epoch-node -- --data-dir .epoch`;

const languageGuides: LanguageGuide[] = [
  {
    id: "go",
    label: "Go",
    version: "Go 1.26",
    setupTitle: "Use the repository-local module",
    setup: `# From the repository root
go version
# Save the example below as quickstart.go`,
    filename: "quickstart.go",
    source: goSource,
    run: `go run ./quickstart.go seed
# Restart epoch-node in the other terminal, then:
go run ./quickstart.go verify`,
    errorType: "*epoch.APIError",
    errorDetail: "Inspect StatusCode, Code, Detail, and Retryable().",
  },
  {
    id: "java",
    label: "Java",
    version: "Java 25",
    setupTitle: "Build the local Maven artifact and classpath",
    setup: `# From the repository root
cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \\
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
# Save the example below as Quickstart.java
javac -cp "$EPOCH_JAVA_CP" Quickstart.java`,
    filename: "Quickstart.java",
    source: javaSource,
    run: `java -cp ".:$EPOCH_JAVA_CP" Quickstart seed
# Restart epoch-node in the other terminal, then:
java -cp ".:$EPOCH_JAVA_CP" Quickstart verify`,
    errorType: "EpochApiException",
    errorDetail: "Inspect status(), code(), detail(), and retryable().",
  },
  {
    id: "python",
    label: "Python",
    version: "Python 3.11+",
    setupTitle: "Install the typed SDK from this checkout",
    setup: `# From the repository root
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python
# Save the example below as quickstart.py`,
    filename: "quickstart.py",
    source: pythonSource,
    run: `python quickstart.py seed
# Restart epoch-node in the other terminal, then:
python quickstart.py verify`,
    errorType: "EpochAPIError",
    errorDetail: "Inspect status, code, detail, and retryable.",
  },
];

const sdkSurface = [
  {
    area: "Connection",
    go: "NewClient · NewClientWithTransport",
    java: "new EpochClient(…)",
    python: "EpochClient(…)",
  },
  {
    area: "Node",
    go: "Health · Resources",
    java: "health · resources",
    python: "health · resources",
  },
  {
    area: "Cache",
    go: "CreateCache · CacheSet · CacheGet · CacheDelete · CacheIncrement",
    java: "createCache · cacheSet · cacheGet · cacheDelete · cacheIncrement",
    python: "create_cache · cache_set · cache_get · cache_delete · cache_increment",
  },
  {
    area: "Stream",
    go: "CreateStream · AppendStream · FetchStream · CommitStreamOffset · StreamLag",
    java: "createStream · appendStream · fetchStream · commitStreamOffset · streamLag",
    python: "create_stream · append_stream · fetch_stream · commit_stream_offset · stream_lag",
  },
  {
    area: "Queue",
    go: "CreateQueue · Send · Receive · Acknowledge · Release · Reject · ExtendLease · QueueCounts · Redrive",
    java: "createQueue · send · receive · acknowledge · release · reject · extendLease · queueCounts · redrive",
    python:
      "create_queue · send · receive · acknowledge · release · reject · extend_lease · queue_counts · redrive",
  },
  {
    area: "Event Bus",
    go: "CreateBus · Publish · UpsertSubscription · RemoveSubscription · ReplayBus",
    java: "createBus · publish · upsertSubscription · removeSubscription · replayBus",
    python: "create_bus · publish · upsert_subscription · remove_subscription · replay_bus",
  },
] as const;

interface DocsPageProps {
  section: string | null;
}

type DocsSectionId =
  "quickstart" | "restart" | "guarantees" | "cluster-milestone" | "sdk-reference" | "reference";

interface DocsNavigationGroup {
  label: string;
  items: ReadonlyArray<{
    id: DocsSectionId;
    label: string;
  }>;
}

const docsNavigation: ReadonlyArray<DocsNavigationGroup> = [
  {
    label: "Get started",
    items: [
      { id: "quickstart", label: "Quickstart" },
      { id: "restart", label: "Restart verification" },
    ],
  },
  {
    label: "Core concepts",
    items: [
      { id: "guarantees", label: "Guarantees & errors" },
      { id: "cluster-milestone", label: "Cluster milestone" },
    ],
  },
  {
    label: "SDKs & reference",
    items: [
      { id: "sdk-reference", label: "SDK reference" },
      { id: "reference", label: "Design reference" },
    ],
  },
];

const docsSections = docsNavigation.flatMap((group) => group.items);

export function DocsPage({ section }: DocsPageProps) {
  const [language, setLanguage] = useState<LanguageId>("go");
  const [activeSection, setActiveSection] = useState<DocsSectionId>(
    isDocsSection(section) ? section : "quickstart",
  );
  const [mobileNavigationOpen, setMobileNavigationOpen] = useState(false);
  const mobileNavigationButton = useRef<HTMLButtonElement>(null);
  const guide = languageGuides.find((candidate) => candidate.id === language) ?? languageGuides[0];

  useEffect(() => {
    navigateToSection(section);
    if (isDocsSection(section)) {
      setActiveSection(section);
    }
  }, [section]);

  useEffect(() => {
    const observedSections = docsSections
      .map(({ id }) => document.getElementById(id))
      .filter((candidate): candidate is HTMLElement => candidate !== null);
    if (!("IntersectionObserver" in window) || observedSections.length === 0) {
      return;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((entry) => entry.isIntersecting)
          .sort(
            (left, right) => Math.abs(left.boundingClientRect.top) - Math.abs(right.boundingClientRect.top),
          );
        const current = visible[0]?.target.id;
        if (isDocsSection(current)) {
          setActiveSection(current);
        }
      },
      {
        rootMargin: "-18% 0px -68% 0px",
        threshold: 0,
      },
    );

    observedSections.forEach((candidate) => observer.observe(candidate));
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!mobileNavigationOpen) {
      return;
    }

    const closeOnEscape = (event: globalThis.KeyboardEvent) => {
      if (event.key !== "Escape") {
        return;
      }
      setMobileNavigationOpen(false);
      mobileNavigationButton.current?.focus();
    };
    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [mobileNavigationOpen]);

  function handleNavigation(id: DocsSectionId) {
    setActiveSection(id);
    setMobileNavigationOpen(false);
    if (window.location.hash === `#/docs/${id}`) {
      navigateToSection(id);
    }
  }

  function handleLanguageKey(event: KeyboardEvent<HTMLButtonElement>, current: LanguageId) {
    const currentIndex = languageGuides.findIndex((candidate) => candidate.id === current);
    let nextIndex: number | null = null;
    if (event.key === "ArrowRight" || event.key === "ArrowDown") {
      nextIndex = (currentIndex + 1) % languageGuides.length;
    } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
      nextIndex = (currentIndex - 1 + languageGuides.length) % languageGuides.length;
    } else if (event.key === "Home") {
      nextIndex = 0;
    } else if (event.key === "End") {
      nextIndex = languageGuides.length - 1;
    }
    if (nextIndex === null) {
      return;
    }

    event.preventDefault();
    const nextLanguage = languageGuides[nextIndex];
    if (!nextLanguage) {
      return;
    }
    setLanguage(nextLanguage.id);
    window.requestAnimationFrame(() => document.getElementById(`language-tab-${nextLanguage.id}`)?.focus());
  }

  if (!guide) {
    return null;
  }

  return (
    <main id="main-content" className="docs-main" tabIndex={-1}>
      <div className="docs-shell">
        <section className="docs-hero" aria-labelledby="docs-title">
          <div>
            <p className="eyebrow">Epoch documentation</p>
            <h1 id="docs-title">Prove the guarantee. Then build on it.</h1>
            <p className="docs-hero__lede">
              Create a durable Stream and Work Queue, move real events through both, restart the process, and
              verify exactly what survived—using the SDK you ship.
            </p>
            <div className="docs-hero__actions">
              <a
                className="button button--primary button--link"
                href="#/docs/quickstart"
                onClick={() => navigateToSection("quickstart")}
              >
                Start the walkthrough
              </a>
              <a
                className="button button--secondary button--link"
                href={`${repositoryUrl}#readme`}
                target="_blank"
                rel="noreferrer"
              >
                View repository
              </a>
            </div>
          </div>
          <dl className="docs-proof-card" aria-label="Quickstart scope">
            <div>
              <dt>Time</dt>
              <dd>≈ 10 minutes</dd>
            </div>
            <div>
              <dt>Topology</dt>
              <dd>One local node</dd>
            </div>
            <div>
              <dt>Guarantee</dt>
              <dd>Local durable</dd>
            </div>
            <div>
              <dt>Outcome</dt>
              <dd>Restart evidence</dd>
            </div>
          </dl>
        </section>

        <aside className="docs-access-note" aria-label="Private alpha package access">
          <strong>Private alpha access</strong>
          <span>
            Running these examples requires access to the repository checkout. The SDK packages are not
            published to public registries yet; the exact reviewed source remains embedded below.
          </span>
        </aside>

        <div className="docs-mobile-navigation">
          <button
            ref={mobileNavigationButton}
            type="button"
            aria-expanded={mobileNavigationOpen}
            aria-controls="mobile-docs-navigation"
            onClick={() => setMobileNavigationOpen((open) => !open)}
          >
            <span className="docs-mobile-navigation__icon" aria-hidden="true">
              <i />
              <i />
              <i />
            </span>
            <span>
              <small>Documentation</small>
              {docsSections.find(({ id }) => id === activeSection)?.label ?? "Browse"}
            </span>
            <span aria-hidden="true">{mobileNavigationOpen ? "×" : "⌄"}</span>
          </button>
          {mobileNavigationOpen ? (
            <DocsNavigation
              id="mobile-docs-navigation"
              activeSection={activeSection}
              onNavigate={handleNavigation}
            />
          ) : null}
        </div>

        <div className="docs-layout">
          <aside className="docs-sidebar" aria-label="Documentation navigation">
            <DocsNavigation activeSection={activeSection} onNavigate={handleNavigation} />
            <div className="docs-sidebar__status">
              <span className="status-dot" data-tone="good" aria-hidden="true" />
              <span>
                <strong>Foundation alpha</strong>
                APIs are provisional
              </span>
            </div>
          </aside>

          <article className="docs-article">
            <section
              id="quickstart"
              className="docs-section"
              aria-labelledby="quickstart-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>01</span>
                <div>
                  <p className="eyebrow">RUN A NODE</p>
                  <h2 id="quickstart-title">Start from a clean, named data directory.</h2>
                  <p>
                    Keep this terminal running. Every SDK below talks to the native HTTP endpoint at
                    <code>127.0.0.1:7601</code>. Set <code>EPOCH_URL</code> to use another node address.
                  </p>
                </div>
              </div>
              <CodeBlock label="Terminal A · repository root" value={nodeStart} />

              <div className="language-picker">
                <div>
                  <p className="eyebrow">CHOOSE YOUR SDK</p>
                  <h3>One lifecycle, three real clients.</h3>
                </div>
                <div className="language-tabs" role="tablist" aria-label="Quickstart language">
                  {languageGuides.map((candidate) => (
                    <button
                      key={candidate.id}
                      id={`language-tab-${candidate.id}`}
                      type="button"
                      role="tab"
                      aria-selected={language === candidate.id}
                      aria-controls={`language-panel-${candidate.id}`}
                      tabIndex={language === candidate.id ? 0 : -1}
                      onClick={() => setLanguage(candidate.id)}
                      onKeyDown={(event) => handleLanguageKey(event, candidate.id)}
                    >
                      <span>{candidate.label}</span>
                      <small>{candidate.version}</small>
                    </button>
                  ))}
                </div>
              </div>

              <div
                id={`language-panel-${guide.id}`}
                className="language-panel"
                role="tabpanel"
                aria-labelledby={`language-tab-${guide.id}`}
              >
                <div className="guide-intro">
                  <span className="step-badge">A</span>
                  <div>
                    <h3>{guide.setupTitle}</h3>
                    <p>
                      The SDKs are pre-alpha and repository-local. These setup commands use the checked-in
                      package rather than implying a public registry release.
                    </p>
                  </div>
                </div>
                <CodeBlock label={`${guide.label} · setup`} value={guide.setup} />

                <div className="guide-intro">
                  <span className="step-badge">B</span>
                  <div>
                    <h3>Create, publish, consume, and acknowledge</h3>
                    <p>
                      Seed mode creates both resources with explicit local durability, appends a Stream event,
                      enqueues two jobs, acquires one lease, and acknowledges it.
                    </p>
                  </div>
                </div>
                <CodeBlock label={guide.filename} value={guide.source} tall />

                <div className="guide-intro">
                  <span className="step-badge">C</span>
                  <div>
                    <h3>Run the first half</h3>
                    <p>When seed mode asks for a restart, leave this terminal open.</p>
                  </div>
                </div>
                <CodeBlock label={`Terminal B · ${guide.label}`} value={guide.run} />
              </div>
            </section>

            <section id="restart" className="docs-section" aria-labelledby="restart-title" tabIndex={-1}>
              <div className="docs-section__heading">
                <span>02</span>
                <div>
                  <p className="eyebrow">RESTART VERIFICATION</p>
                  <h2 id="restart-title">Use the same bytes, not a fresh node.</h2>
                  <p>
                    Stop only the process. Keep <code>.epoch</code>, restart the node, then run the selected
                    SDK in <code>verify</code> mode.
                  </p>
                </div>
              </div>
              <CodeBlock label="Terminal A · restart" value={nodeRestart} />
              <div className="verification-grid">
                <article>
                  <span>STREAM</span>
                  <strong>One record returns at offset 0.</strong>
                  <p>The append was fsynced and replayed from the standalone journal.</p>
                </article>
                <article>
                  <span>QUEUE</span>
                  <strong>Only the unacknowledged job returns.</strong>
                  <p>The message and the earlier lease settlement both survived restart.</p>
                </article>
                <article>
                  <span>DISK</span>
                  <strong>Segmented WAL remains local.</strong>
                  <p>
                    Evidence lives under <code>.epoch/engine-wal/</code>; it is not a replica or backup.
                  </p>
                </article>
              </div>
            </section>

            <section
              id="guarantees"
              className="docs-section"
              aria-labelledby="guarantees-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>03</span>
                <div>
                  <p className="eyebrow">READ THE RECEIPT</p>
                  <h2 id="guarantees-title">Local durable is deliberately narrow.</h2>
                </div>
              </div>
              <div className="guarantee-grid">
                <div className="guarantee-grid__yes">
                  <p className="eyebrow">WHAT IT DOES</p>
                  <ul>
                    <li>Fsyncs accepted Stream and Queue mutations before applying them.</li>
                    <li>Replays checksum-valid records after a process restart.</li>
                    <li>Persists queue leases, settlements, retries, and redrives.</li>
                  </ul>
                </div>
                <div className="guarantee-grid__no">
                  <p className="eyebrow">WHAT IT DOES NOT DO</p>
                  <ul>
                    <li>Replicate to another process, host, zone, or region.</li>
                    <li>Survive loss of the machine and its storage.</li>
                    <li>Provide snapshots, compaction, PITR, or quorum acknowledgement.</li>
                  </ul>
                </div>
              </div>

              <div className="error-contract">
                <div>
                  <p className="eyebrow">ERROR CONTRACT</p>
                  <h3>Transport-retryable does not mean mutation-safe.</h3>
                  <p>
                    The SDKs perform no hidden retries. A timeout can leave a write outcome unknown, so
                    inspect the typed error and the operation’s idempotency contract before trying again.
                  </p>
                </div>
                <dl>
                  {languageGuides.map((candidate) => (
                    <div key={candidate.id}>
                      <dt>{candidate.label}</dt>
                      <dd>
                        <code>{candidate.errorType}</code>
                        <span>{candidate.errorDetail}</span>
                      </dd>
                    </div>
                  ))}
                </dl>
              </div>
            </section>

            <section
              id="cluster-milestone"
              className="docs-section"
              aria-labelledby="cluster-milestone-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>04</span>
                <div>
                  <p className="eyebrow">REGIONAL MULTI-TABLET ALPHA</p>
                  <h2 id="cluster-milestone-title">
                    One catalog materializes four profile-specific groups in every node.
                  </h2>
                  <p>
                    Three Rust nodes now run a dedicated catalog group plus simultaneous Cache, Stream, Queue,
                    and Event Bus tablets behind resource/shard routing. The Go control plane reconciles
                    desired state through Rust, transactionally persists management metadata, and exposes
                    observed placement to the browser; the console never contacts a storage node. The public
                    SDK quickstart above remains standalone and <code>local_durable</code>. Fixed three-voter
                    evidence is not zone-aware placement, the Go metadata database has one process owner, and
                    these regional routes remain experimental and unauthenticated.
                  </p>
                </div>
              </div>
              <div className="verification-grid">
                <article>
                  <span>MAJORITY</span>
                  <strong>Catalog and data groups commit through durable voter majorities.</strong>
                  <p>Generation and tablet-epoch fences reject stale routes before typed dispatch.</p>
                </article>
                <article>
                  <span>OBSERVATION</span>
                  <strong>The Go BFF reports achieved voters and leaders, not desired replicas.</strong>
                  <p>
                    A leader loss becomes degraded two-voter placement; a total outage clears stale topology.
                  </p>
                </article>
                <article>
                  <span>RECOVERY</span>
                  <strong>Go intent and Rust groups recover from their owned durable state.</strong>
                  <p>
                    Control restart preserves exact retries; a full data-plane <code>SIGKILL</code> cycle
                    reopens the same EPRS volumes and profile digests.
                  </p>
                </article>
              </div>
              <CodeBlock
                label="Disposable fixed-group and regional proofs"
                value={
                  "make test-stream-tablet\nmake test-queue-tablet\nmake test-cache-tablet\nmake test-bus-tablet\nmake test-regional-runtime"
                }
              />
            </section>

            <section
              id="sdk-reference"
              className="docs-section"
              aria-labelledby="sdk-reference-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>05</span>
                <div>
                  <p className="eyebrow">STANDALONE ALPHA SURFACE</p>
                  <h2 id="sdk-reference-title">The same operation, native to each ecosystem.</h2>
                  <p>
                    All implemented standalone profile operations have Go, Java, and Python entry points. The
                    experimental tablet routes above intentionally have no SDK contract. Responses are still
                    dynamic documents in this alpha; mutation calls never perform hidden retries.
                  </p>
                </div>
              </div>

              <div className="table-wrap sdk-surface-table">
                <table>
                  <caption className="sr-only">Implemented SDK methods by language and workload</caption>
                  <thead>
                    <tr>
                      <th scope="col">Area</th>
                      <th scope="col">Go</th>
                      <th scope="col">Java</th>
                      <th scope="col">Python</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sdkSurface.map((row) => (
                      <tr key={row.area}>
                        <th scope="row">{row.area}</th>
                        <td>
                          <code>{row.go}</code>
                        </td>
                        <td>
                          <code>{row.java}</code>
                        </td>
                        <td>
                          <code>{row.python}</code>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="sdk-notes" aria-label="SDK conventions">
                <article>
                  <span>CONFIGURATION</span>
                  <strong>Defaults stay explicit.</strong>
                  <p>
                    Go exposes <code>Default*Config</code>, Java exposes <code>*.defaults()</code>, and Python
                    uses typed keyword defaults. Set <code>EPOCH_URL</code> in the walkthrough to select a
                    node.
                  </p>
                </article>
                <article>
                  <span>FAILURES</span>
                  <strong>Inspect the typed API error.</strong>
                  <p>
                    Read status, code, detail, body, and retry classification. A transport-retryable error can
                    still leave a mutation outcome unknown.
                  </p>
                </article>
                <article>
                  <span>CONTROL</span>
                  <strong>The server owns semantic validation.</strong>
                  <p>
                    Client-side checks improve feedback but do not replace server validation. Go also accepts
                    a context for per-call cancellation and deadlines.
                  </p>
                </article>
              </div>
            </section>

            <section id="reference" className="docs-section" aria-labelledby="reference-title" tabIndex={-1}>
              <div className="docs-section__heading">
                <span>06</span>
                <div>
                  <p className="eyebrow">SOURCE OF TRUTH</p>
                  <h2 id="reference-title">Go deeper without losing the boundary.</h2>
                  <p>These repository documents own the API, semantic, and evidence contracts.</p>
                </div>
              </div>
              <div className="reference-grid">
                <ReferenceCard
                  eyebrow="SURFACE"
                  title="API contracts"
                  description="Routes, envelopes, errors, pagination, health, and the implemented alpha slice."
                  href={`${repositoryDocsUrl}/API_CONTRACTS.md`}
                />
                <ReferenceCard
                  eyebrow="BEHAVIOR"
                  title="Semantics"
                  description="Ordering, durability, acknowledgement, time, replay, and failure meaning."
                  href={`${repositoryDocsUrl}/SEMANTICS.md`}
                />
                <ReferenceCard
                  eyebrow="EVIDENCE"
                  title="Testing strategy"
                  description="Restart, corruption, history, integration, and release evidence expectations."
                  href={`${repositoryDocsUrl}/TESTING.md`}
                />
                <ReferenceCard
                  eyebrow="DELIVERY"
                  title="Delivery checklist"
                  description="Table-based program gates, current core work, pull-request requirements, and release readiness."
                  href={`${repositoryDocsUrl}/DELIVERY_CHECKLIST.md`}
                />
                <ReferenceCard
                  eyebrow="REGIONAL RUNTIME"
                  title="Multi-tablet operations"
                  description="Catalog authority, Go reconciliation, browser BFF, fenced routes, local startup, recovery campaign, and explicit non-claims."
                  href={`${repositoryDocsUrl}/REGIONAL_RUNTIME.md`}
                />
                <ReferenceCard
                  eyebrow="CLUSTER CORE"
                  title="Experimental Stream tablet"
                  description="Typed command, fixed-voter majority, failover, idempotency, and all-voter recovery boundary."
                  href={`${repositoryDocsUrl}/STREAM_TABLET.md`}
                />
                <ReferenceCard
                  eyebrow="QUEUE TABLET"
                  title="Experimental replicated Queue"
                  description="Typed mutations, fenced leases, failover/redelivery, immutable DLQ/redrive history, and all-voter recovery."
                  href={`${repositoryDocsUrl}/QUEUE_TABLET.md`}
                />
                <ReferenceCard
                  eyebrow="CACHE TABLET"
                  title="Experimental replicated Cache"
                  description="CAS, atomic transactions, checked expiry, fenced locks, failover, exact EPRS replay, and stale-capable local observations."
                  href={`${repositoryDocsUrl}/CACHE_TABLET.md`}
                />
                <ReferenceCard
                  eyebrow="BUS TABLET"
                  title="Experimental replicated Event Bus"
                  description="Replicated ingress, per-subscription outbox leases, retry/DLQ history, archive replay, failover, EPRS recovery, and explicit executor non-claims."
                  href={`${repositoryDocsUrl}/BUS_TABLET.md`}
                />
                <ReferenceCard
                  eyebrow="RELEASE"
                  title="v0.1.0-alpha.2 release notes"
                  description="Verified milestone highlights, source-only artifacts, compatibility guidance, and explicit alpha limitations."
                  href={`${repositoryDocsUrl}/releases/v0.1.0-alpha.2.md`}
                />
              </div>
            </section>
          </article>

          <aside className="docs-toc">
            <nav aria-label="On this page">
              <p>On this page</p>
              {docsSections.map(({ id, label }) => (
                <a
                  key={id}
                  href={`#/docs/${id}`}
                  aria-current={activeSection === id ? "location" : undefined}
                  onClick={() => handleNavigation(id)}
                >
                  {label}
                </a>
              ))}
            </nav>
            <a
              className="docs-toc__edit"
              href={`${repositoryUrl}/edit/main/console/src/DocsPage.tsx`}
              target="_blank"
              rel="noreferrer"
            >
              Edit this page <span aria-hidden="true">↗</span>
            </a>
          </aside>
        </div>
      </div>
    </main>
  );
}

function DocsNavigation({
  id,
  activeSection,
  onNavigate,
}: {
  id?: string;
  activeSection: DocsSectionId;
  onNavigate: (id: DocsSectionId) => void;
}) {
  return (
    <nav id={id} className="docs-navigation" aria-label="Documentation sections">
      {docsNavigation.map((group) => (
        <div key={group.label} className="docs-navigation__group">
          <p>{group.label}</p>
          {group.items.map((item) => (
            <a
              key={item.id}
              href={`#/docs/${item.id}`}
              aria-current={activeSection === item.id ? "location" : undefined}
              onClick={() => onNavigate(item.id)}
            >
              <span aria-hidden="true" />
              {item.label}
            </a>
          ))}
        </div>
      ))}
    </nav>
  );
}

function isDocsSection(candidate: string | null | undefined): candidate is DocsSectionId {
  return docsSections.some(({ id }) => id === candidate);
}

function navigateToSection(section: string | null) {
  window.requestAnimationFrame(() => {
    if (!section) {
      window.scrollTo({ top: 0 });
      document.getElementById("main-content")?.focus({ preventScroll: true });
      return;
    }
    const target = document.getElementById(section);
    target?.scrollIntoView();
    target?.focus({ preventScroll: true });
  });
}

function CodeBlock({ label, value, tall = false }: { label: string; value: string; tall?: boolean }) {
  const [copyStatus, setCopyStatus] = useState<"idle" | "copied" | "failed">("idle");

  async function copy() {
    try {
      await navigator.clipboard.writeText(value);
      setCopyStatus("copied");
    } catch {
      setCopyStatus("failed");
    }
  }

  const copyLabel = copyStatus === "copied" ? "Copied" : copyStatus === "failed" ? "Copy failed" : "Copy";

  return (
    <div className="code-block" data-tall={tall || undefined}>
      <div className="code-block__toolbar">
        <span>{label}</span>
        <button type="button" onClick={() => void copy()} aria-live="polite">
          {copyLabel}
        </button>
      </div>
      <pre tabIndex={0}>
        <code>{value}</code>
      </pre>
    </div>
  );
}

function ReferenceCard({
  eyebrow,
  title,
  description,
  href,
}: {
  eyebrow: string;
  title: string;
  description: string;
  href: string;
}) {
  return (
    <a className="reference-card" href={href} target="_blank" rel="noreferrer">
      <span>{eyebrow}</span>
      <strong>{title}</strong>
      <p>{description}</p>
      <em aria-hidden="true">Read on GitHub ↗</em>
    </a>
  );
}
