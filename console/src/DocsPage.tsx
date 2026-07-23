import { useEffect, useState, type KeyboardEvent } from "react";

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

export function DocsPage({ section }: DocsPageProps) {
  const [language, setLanguage] = useState<LanguageId>("go");
  const guide = languageGuides.find((candidate) => candidate.id === language) ?? languageGuides[0];

  useEffect(() => {
    navigateToSection(section);
  }, [section]);

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
      <div className="shell">
        <section className="docs-hero" aria-labelledby="docs-title">
          <div>
            <p className="eyebrow">STANDALONE QUICKSTART</p>
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

        <div className="docs-layout">
          <aside className="docs-sidebar">
            <nav aria-label="Documentation sections">
              <p>On this page</p>
              <a href="#/docs/quickstart" onClick={() => navigateToSection("quickstart")}>
                Quickstart
              </a>
              <a href="#/docs/restart" onClick={() => navigateToSection("restart")}>
                Restart verification
              </a>
              <a href="#/docs/guarantees" onClick={() => navigateToSection("guarantees")}>
                Guarantees &amp; errors
              </a>
              <a href="#/docs/cluster-milestone" onClick={() => navigateToSection("cluster-milestone")}>
                Cluster milestone
              </a>
              <a href="#/docs/sdk-reference" onClick={() => navigateToSection("sdk-reference")}>
                SDK reference
              </a>
              <a href="#/docs/reference" onClick={() => navigateToSection("reference")}>
                Design reference
              </a>
            </nav>
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
                  <p className="eyebrow">EXPERIMENTAL CLUSTER CORE</p>
                  <h2 id="cluster-milestone-title">
                    Stream, Queue, and Cache cross the same persistent consensus boundary.
                  </h2>
                  <p>
                    These mutually exclusive, opt-in engineering profiles run on a separate, unauthenticated
                    listener. The public SDK quickstart above remains standalone and{" "}
                    <code>local_durable</code>; no SDK or public quorum contract is implied. Two durable
                    voters are fixed-topology evidence, not multi-zone placement proof. Cache observations
                    are explicitly local and stale-capable; there is no linearizable read barrier yet.
                  </p>
                </div>
              </div>
              <div className="verification-grid">
                <article>
                  <span>MAJORITY</span>
                  <strong>Two of three voters persist before typed success.</strong>
                  <p>Each typed profile applies the exact committed outcome before returning success.</p>
                </article>
                <article>
                  <span>FAILOVER</span>
                  <strong>A replacement leader preserves ordering and fences stale lease tokens.</strong>
                  <p>The stopped voter rejoins, catches up, and each profile applies every command once.</p>
                </article>
                <article>
                  <span>RECOVERY</span>
                  <strong>All three voters rebuild the same digest.</strong>
                  <p>
                    A full <code>SIGKILL</code> cycle replays the consensus history without a second WAL.
                  </p>
                </article>
              </div>
              <CodeBlock
                label="Disposable three-container proofs"
                value={"make test-stream-tablet\nmake test-queue-tablet\nmake test-cache-tablet"}
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
              </div>
            </section>
          </article>
        </div>
      </div>
    </main>
  );
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
