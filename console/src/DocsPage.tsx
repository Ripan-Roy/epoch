import { useEffect, useRef, useState, type KeyboardEvent } from "react";

import goSource from "./quickstarts/quickstart.go?raw";
import javaSource from "./quickstarts/Quickstart.java?raw";
import pythonSource from "./quickstarts/quickstart.py?raw";
import regionalGoSource from "./quickstarts/regional/quickstart.go?raw";
import regionalJavaSource from "./quickstarts/regional/RegionalQuickstart.java?raw";
import regionalPythonSource from "./quickstarts/regional/quickstart.py?raw";
import regionalQueueGoSource from "./quickstarts/regional_queue/quickstart.go?raw";
import regionalQueueJavaSource from "./quickstarts/regional_queue/RegionalQueueQuickstart.java?raw";
import regionalQueuePythonSource from "./quickstarts/regional_queue/quickstart.py?raw";
import regionalCacheGoSource from "./quickstarts/regional_cache/quickstart.go?raw";
import regionalCacheJavaSource from "./quickstarts/regional_cache/RegionalCacheQuickstart.java?raw";
import regionalCachePythonSource from "./quickstarts/regional_cache/quickstart.py?raw";
import regionalBusGoSource from "./quickstarts/regional_bus/quickstart.go?raw";
import regionalBusJavaSource from "./quickstarts/regional_bus/RegionalBusQuickstart.java?raw";
import regionalBusPythonSource from "./quickstarts/regional_bus/quickstart.py?raw";

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

const regionalNodes = `# Terminal A · build and start three fixed voters
make compose-regional-up`;

const consensusCheckpoint = `# Start the disposable fixed-voter probe
make compose-probe-up

# Inspect voter-local checkpoint and retained-log positions
curl --fail --silent --show-error \
  http://127.0.0.1:17701/experimental/v1/consensus/status

# Fsync a native-profile checkpoint and atomically reclaim old EPRS generations
curl --fail-with-body --request POST \
  http://127.0.0.1:17701/experimental/v1/consensus/checkpoints`;

const regionalControl = `# Terminal B · keep the managed bridge running
EPOCH_CONTROL_REGIONAL_ENDPOINTS=http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663 \
EPOCH_CONTROL_STATE_PATH=.epoch/control/registry.db \
EPOCH_AUTH_POLICY_PATH=spec/auth/bootstrap-policy-v1.example.json \
EPOCH_CONTROL_REGIONAL_TOKEN=epoch-dev-control-v1 \
go run ./control/cmd/epoch-control`;

const regionalResource = `# Terminal C · create one replicated Stream
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-orders-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"stream","name":"orders",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

const regionalQueueResource = `# Terminal C · create one replicated Queue
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-jobs-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"queue","name":"jobs",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

const regionalCacheResource = `# Terminal C · create one replicated Cache
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-sessions-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"cache","name":"sessions",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

const regionalBusResource = `# Terminal C · create one replicated Event Bus
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-events-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev","namespace":"core",
      "kind":"event-bus","name":"events",
      "spec":{"shard_count":1,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'`;

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

const regionalLanguageGuides: Record<
  LanguageId,
  { filename: string; source: string; setup: string; run: string }
> = {
  go: {
    filename: "quickstart.go",
    source: regionalGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional/quickstart.go",
  },
  java: {
    filename: "RegionalQuickstart.java",
    source: regionalJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional/RegionalQuickstart.java \
  -d target/regional-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-docs-classes:$EPOCH_JAVA_CP" RegionalQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalPythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional/quickstart.py",
  },
};

const regionalQueueLanguageGuides: typeof regionalLanguageGuides = {
  go: {
    filename: "quickstart.go",
    source: regionalQueueGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional_queue/quickstart.go",
  },
  java: {
    filename: "RegionalQueueQuickstart.java",
    source: regionalQueueJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional_queue/RegionalQueueQuickstart.java \
  -d target/regional-queue-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-queue-docs-classes:$EPOCH_JAVA_CP" RegionalQueueQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalQueuePythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional_queue/quickstart.py",
  },
};

const regionalCacheLanguageGuides: typeof regionalLanguageGuides = {
  go: {
    filename: "quickstart.go",
    source: regionalCacheGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional_cache/quickstart.go",
  },
  java: {
    filename: "RegionalCacheQuickstart.java",
    source: regionalCacheJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional_cache/RegionalCacheQuickstart.java \
  -d target/regional-cache-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-cache-docs-classes:$EPOCH_JAVA_CP" RegionalCacheQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalCachePythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional_cache/quickstart.py",
  },
};

const regionalBusLanguageGuides: typeof regionalLanguageGuides = {
  go: {
    filename: "quickstart.go",
    source: regionalBusGoSource,
    setup: "# Uses the repository-local Go module; no separate install is required.",
    run: "go run ./console/src/quickstarts/regional_bus/quickstart.go",
  },
  java: {
    filename: "RegionalBusQuickstart.java",
    source: regionalBusJavaSource,
    setup: `cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional_bus/RegionalBusQuickstart.java \
  -d target/regional-bus-docs-classes`,
    run: `cd sdk/java
java -cp "target/regional-bus-docs-classes:$EPOCH_JAVA_CP" RegionalBusQuickstart`,
  },
  python: {
    filename: "quickstart.py",
    source: regionalBusPythonSource,
    setup: `python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python`,
    run: "python console/src/quickstarts/regional_bus/quickstart.py",
  },
};

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
    area: "Regional Stream",
    go: "RegionalStreamClient · Append · Fetch · CommitOffset · Lag · FetchGroup",
    java: "RegionalStreamClient · append · fetch · commitOffset · lag · fetchGroup",
    python: "RegionalStreamClient · append · fetch · commit_offset · lag · fetch_group",
  },
  {
    area: "Regional Queue",
    go: "RegionalQueueClient · Enqueue · Acquire · ExtendLease · Acknowledge · Release · Nack · Reject · Redrive · Maintain · Counts · ConsumerFlow",
    java: "RegionalQueueClient · enqueue · acquire · extendLease · acknowledge · release · nack · reject · redrive · maintain · counts · consumerFlow",
    python:
      "RegionalQueueClient · enqueue · acquire · extend_lease · acknowledge · release · nack · reject · redrive · maintain · counts · consumer_flow",
  },
  {
    area: "Regional Cache",
    go: "RegionalCacheClient · Set · Delete · CompareAndSet · Increment · Transaction · AcquireLock · RenewLock · ReleaseLock · Maintain · Observe",
    java: "RegionalCacheClient · set · delete · compareAndSet · increment · transaction · acquireLock · renewLock · releaseLock · maintain · observe",
    python:
      "RegionalCacheClient · set · delete · compare_and_set · increment · transaction · acquire_lock · renew_lock · release_lock · maintain · observe",
  },
  {
    area: "Regional Event Bus",
    go: "RegionalBusClient · UpsertSubscription · RemoveSubscription · Publish · AcquireDeliveries · AcknowledgeDelivery · FailDelivery · MaintainDeliveries · Mutation · ReplayArchive · QueryDeliveries · Status",
    java: "RegionalBusClient · upsertSubscription · removeSubscription · publish · acquireDeliveries · acknowledgeDelivery · failDelivery · maintainDeliveries · mutation · replayArchive · queryDeliveries · status",
    python:
      "RegionalBusClient · upsert_subscription · remove_subscription · publish · acquire_deliveries · acknowledge_delivery · fail_delivery · maintain_deliveries · mutation · replay_archive · query_deliveries · status",
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
  | "quickstart"
  | "restart"
  | "guarantees"
  | "cluster-milestone"
  | "consensus-recovery"
  | "regional-stream"
  | "regional-queue"
  | "regional-cache"
  | "regional-bus"
  | "sdk-reference"
  | "reference";

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
      { id: "consensus-recovery", label: "Consensus recovery" },
    ],
  },
  {
    label: "SDKs & reference",
    items: [
      { id: "regional-stream", label: "Regional Stream SDK" },
      { id: "regional-queue", label: "Regional Queue SDK" },
      { id: "regional-cache", label: "Regional Cache SDK" },
      { id: "regional-bus", label: "Regional Event Bus SDK" },
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
  const regionalGuide = regionalLanguageGuides[language];
  const regionalQueueGuide = regionalQueueLanguageGuides[language];
  const regionalCacheGuide = regionalCacheLanguageGuides[language];
  const regionalBusGuide = regionalBusLanguageGuides[language];

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

  function handleLanguageKey(
    event: KeyboardEvent<HTMLButtonElement>,
    current: LanguageId,
    tabPrefix = "language-tab",
  ) {
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
    window.requestAnimationFrame(() => document.getElementById(`${tabPrefix}-${nextLanguage.id}`)?.focus());
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
                    SDK quickstart above remains standalone and <code>local_durable</code>. The fixed voters
                    now report configured region, zone, class, and live group capacity; Go validates requested
                    regions, minimum zones, class, and incremental capacity before touching the catalog. That
                    is not dynamic membership, rack placement, or online rebalance. The Go metadata database
                    still has one process owner and these regional routes remain experimental. Regional reads
                    default to a safe leader <code>ReadIndex</code>, wait for majority confirmation and local
                    profile apply, and expose exact barrier evidence; callers must explicitly request{" "}
                    <code>local_stale</code> to bypass that barrier. Queue acquire can now declare request
                    credit and a per-consumer <code>max_in_flight</code> window; the replicated transition
                    returns exact saturation and remaining-capacity evidence, and settlement replenishes the
                    window. Stream append can now carry 1–1,000 correlated records through none, gzip, LZ4,
                    Snappy, or Zstd frames with hard compressed and expanded limits; the complete batch
                    becomes visible atomically and exact retries retain every sequence-to-offset result. A
                    Stream consumer group can now replicate its next offset, commit forward, reset explicitly,
                    observe lag, and replay from that checkpoint; caller-supplied generations fence an old or
                    conflicting member across failover and EPRS rebuild. These are experimental HTTP/tablet
                    slices, not yet coordinated join, heartbeat, assignment, rebalance, native bidirectional
                    streaming, automatic client batching, or compression negotiation. The separate regional
                    Stream, Queue, Cache, and Event Bus v1 clients below expose the implemented
                    partition-0/shard-0 operations with leader/fence-aware Go, Java, and Python routing.
                    Stream remains uncoordinated; Queue and Event Bus delivery are request/response rather
                    than managed streaming sessions; Cache expiry is explicit and single-shard. Managed
                    HTTP/gRPC and regional HTTP require a shared deny-by-default bootstrap bearer policy, but
                    that is not OIDC, TLS/mTLS, credential expiry/revocation, or immutable audit export.
                  </p>
                </div>
              </div>
              <div className="verification-grid">
                <article>
                  <span>MAJORITY</span>
                  <strong>Catalog and data groups commit through durable voter majorities.</strong>
                  <p>
                    Generation/tablet fences reject stale routes; default reads require quorum-confirmed
                    ReadIndex evidence with no silent downgrade.
                  </p>
                </article>
                <article>
                  <span>QUEUE FLOW</span>
                  <strong>Consumer credit cannot exceed its declared live-lease window.</strong>
                  <p>
                    Repeated receive saturates at zero; Ack, Nack, Release, Reject, or expiry processing
                    replenishes capacity from replicated state.
                  </p>
                </article>
                <article>
                  <span>QUEUE SDK RECOVERY</span>
                  <strong>Lease, retry, dead-letter, redrive, and settle survive leader loss.</strong>
                  <p>
                    The real Python client preserves <code>docs-python-redrive-v1</code>, route fences, and
                    opaque lease tokens while the Docker campaign kills the Queue leader and later reopens
                    every voter from the same volumes.
                  </p>
                </article>
                <article>
                  <span>CACHE SDK RECOVERY</span>
                  <strong>Typed values, CAS, transaction, fencing, and expiry survive leader loss.</strong>
                  <p>
                    The Python client executes the complete shard-0 lifecycle after Cache leadership changes,
                    then the old voter catches up and all three reopen the same committed state.
                  </p>
                </article>
                <article>
                  <span>EVENT BUS SDK RECOVERY</span>
                  <strong>
                    Ingress, delivery leases, retry, archive, and settlement survive leader loss.
                  </strong>
                  <p>
                    The Python client preserves exact publish and settlement identities while the same Docker
                    campaign replaces the Event Bus leader, catches up the old voter, and reopens every
                    volume.
                  </p>
                </article>
                <article>
                  <span>STREAM BATCH</span>
                  <strong>Every compressed frame is bounded before one atomic replicated apply.</strong>
                  <p>
                    Five codec modes return exact per-sequence offsets; real-runtime and Python-gzip container
                    tests prove retry, failover, and EPRS recovery without changing v1 append bytes.
                  </p>
                </article>
                <article>
                  <span>STREAM GROUP</span>
                  <strong>Checkpoint progress and ownership fences survive leader and process loss.</strong>
                  <p>
                    Commit, reset, lag, and replay converge on every voter; stale or wrong generations are
                    durable rejected outcomes without moving the next offset.
                  </p>
                </article>
                <article>
                  <span>ADMISSION + OBSERVATION</span>
                  <strong>
                    Go checks fixed-voter zones and capacity, then reports actual serving routes.
                  </strong>
                  <p>
                    A limiting node rejects before catalog apply; leader loss becomes degraded two-voter
                    placement.
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
                <article>
                  <span>TRUST BASELINE</span>
                  <strong>Go and Rust authorize the action at the parsed tenant scope.</strong>
                  <p>
                    Cross-tenant lists are filtered, Go uses a distinct Rust workload credential, and bounded
                    decision logs contain no bearer value or payload.
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
              id="consensus-recovery"
              className="docs-section"
              aria-labelledby="consensus-recovery-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>05</span>
                <div>
                  <p className="eyebrow">FIXED-VOTER RECOVERY CORE</p>
                  <h2 id="consensus-recovery-title">Checkpoint, compact, catch up, and reopen.</h2>
                  <p>
                    The replicated core can encode a bounded canonical <code>EPSN v2</code> image for Catalog,
                    Stream, Queue, Cache, or Event Bus at one voter&apos;s applied Raft index. It fsyncs the
                    checkpoint, atomically replaces the EPRS journal with one compacted baseline, then
                    installs the logical snapshot. A lagging fixed voter validates and persists that native
                    state before applying its retained committed tail. EPSN v1 remains readable.
                  </p>
                </div>
              </div>

              <div className="verification-grid">
                <article>
                  <span>DURABLE ORDER</span>
                  <strong>Disk becomes authoritative before memory changes.</strong>
                  <p>
                    A post-fsync failure stops the live adapter; reopening the same journal recovers the
                    checkpoint, proposal lookup, and digest without inventing another receipt.
                  </p>
                </article>
                <article>
                  <span>FOLLOWER CATCH-UP</span>
                  <strong>Native profile restore completes before the retained tail applies.</strong>
                  <p>
                    Catalog proves lagging-voter snapshot-plus-tail catch-up, while all five profiles force a
                    checkpoint and restore automatically in their real three-voter restart tests.
                  </p>
                </article>
                <article>
                  <span>EXACT BOUNDARY</span>
                  <strong>This is a consensus checkpoint, not a backup.</strong>
                  <p>
                    V2 bounds exact-retry metadata to 1,024 records and 1 MiB, caps profile bytes at 4 MiB and
                    the complete image at 6 MiB, and reclaims older journal generations. It is not a
                    downloadable backup, PITR, dynamic membership, or production repair workflow.
                  </p>
                </article>
              </div>

              <CodeBlock label="Local checkpoint evidence" value={consensusCheckpoint} />

              <aside className="docs-access-note">
                <strong>Local experimental surface</strong>
                <span>
                  <code>checkpoint_index</code> and <code>retained_log_first_index</code> are voter-local
                  facts. The trigger is explicit and unauthenticated on the diagnostic listener; do not expose
                  it to an untrusted network.
                </span>
              </aside>
            </section>

            <section
              id="regional-stream"
              className="docs-section"
              aria-labelledby="regional-stream-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>06</span>
                <div>
                  <p className="eyebrow">VERSIONED REGIONAL STREAM V1</p>
                  <h2 id="regional-stream-title">Run one authenticated client across three voters.</h2>
                  <p>
                    This path is separate from the standalone quickstart. It uses a fully qualified tenant
                    scope, discovers the current leader before every operation, carries the observed resource
                    generation and tablet epoch, and keeps the caller&apos;s idempotency key unchanged across
                    a bounded rediscovery retry.
                  </p>
                </div>
              </div>

              <div className="guide-intro">
                <span className="step-badge">A</span>
                <div>
                  <h3>Start the voters and provision the Stream</h3>
                  <p>
                    The credentials below are public development fixtures. Use them only with this disposable
                    local topology. The Go bridge is needed to create the resource; once materialized, the SDK
                    data path remains available if that bridge stops.
                  </p>
                </div>
              </div>
              <CodeBlock label="Terminal A · regional nodes" value={regionalNodes} />
              <CodeBlock label="Terminal B · managed bridge" value={regionalControl} />
              <CodeBlock label="Terminal C · provision" value={regionalResource} />

              <div className="language-picker">
                <div>
                  <p className="eyebrow">CHOOSE YOUR REGIONAL SDK</p>
                  <h3>One route and recovery contract in three ecosystems.</h3>
                </div>
                <div className="language-tabs" role="tablist" aria-label="Regional Stream language">
                  {languageGuides.map((candidate) => (
                    <button
                      key={candidate.id}
                      id={`regional-language-tab-${candidate.id}`}
                      type="button"
                      role="tab"
                      aria-selected={language === candidate.id}
                      aria-controls={`regional-language-panel-${candidate.id}`}
                      tabIndex={language === candidate.id ? 0 : -1}
                      onClick={() => setLanguage(candidate.id)}
                      onKeyDown={(event) => handleLanguageKey(event, candidate.id, "regional-language-tab")}
                    >
                      <span>{candidate.label}</span>
                      <small>{candidate.version}</small>
                    </button>
                  ))}
                </div>
              </div>

              <div
                id={`regional-language-panel-${language}`}
                className="language-panel"
                role="tabpanel"
                aria-labelledby={`regional-language-tab-${language}`}
              >
                <CodeBlock label={`${guide.label} · setup`} value={regionalGuide.setup} />
                <CodeBlock label={regionalGuide.filename} value={regionalGuide.source} tall />
                <CodeBlock label="Terminal C · run" value={regionalGuide.run} />
              </div>

              <div className="sdk-notes" aria-label="Regional SDK guarantees">
                <article>
                  <span>ROUTING</span>
                  <strong>Discovery is part of every call.</strong>
                  <p>
                    Configure every node endpoint. The client selects only a response with{" "}
                    <code>accepts_writes: true</code> and sends the observed term and fences to that node.
                  </p>
                </article>
                <article>
                  <span>UNKNOWN OUTCOME</span>
                  <strong>Mutation identity stays with the caller.</strong>
                  <p>
                    Append and checkpoint calls require an idempotency key. A routing retry reuses that exact
                    key; changing the key or request creates a different mutation or a conflict.
                  </p>
                </article>
                <article>
                  <span>CONSISTENCY</span>
                  <strong>Reads require a leader barrier.</strong>
                  <p>
                    Fetch, group fetch, and lag explicitly request <code>linearizable</code>. They never fall
                    back to a local stale read. A minority returns a retryable unavailability error.
                  </p>
                </article>
              </div>

              <aside className="docs-access-note">
                <strong>Current boundary</strong>
                <span>
                  Regional v1 covers single-record append, bounded fetch, checkpoint commit/reset, lag, and
                  checkpoint replay for partition 0. The caller still supplies member generations. Join,
                  heartbeat, assignment, rebalance, multi-partition ownership, transactional offsets,
                  automatic batching/compression, and public package-registry releases remain open.
                </span>
              </aside>
            </section>

            <section
              id="regional-queue"
              className="docs-section"
              aria-labelledby="regional-queue-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>07</span>
                <div>
                  <p className="eyebrow">VERSIONED REGIONAL QUEUE V1</p>
                  <h2 id="regional-queue-title">Run the complete Queue lifecycle through one fenced API.</h2>
                  <p>
                    Regional Queue SDK calls target the existing replicated Queue tablet. Every operation is
                    tenant-qualified and authenticated; discovery selects a writable leader, then the request
                    carries the observed resource generation, tablet epoch, and—on mutations—leader term. One
                    routing retry preserves the caller&apos;s exact mutation identity.
                  </p>
                </div>
              </div>

              <div className="guide-intro">
                <span className="step-badge">A</span>
                <div>
                  <h3>Start the voters and provision the Queue</h3>
                  <p>
                    Reuse the same disposable regional topology and development-only credentials. The Go
                    bridge provisions <code>jobs</code>; the native data path then goes directly to the
                    discovered Rust Queue leader.
                  </p>
                </div>
              </div>
              <CodeBlock label="Terminal A · regional nodes" value={regionalNodes} />
              <CodeBlock label="Terminal B · managed bridge" value={regionalControl} />
              <CodeBlock label="Terminal C · provision" value={regionalQueueResource} />

              <div className="language-picker">
                <div>
                  <p className="eyebrow">CHOOSE YOUR REGIONAL QUEUE SDK</p>
                  <h3>One lease and redrive contract in three ecosystems.</h3>
                </div>
                <div className="language-tabs" role="tablist" aria-label="Regional Queue language">
                  {languageGuides.map((candidate) => (
                    <button
                      key={candidate.id}
                      id={`regional-queue-language-tab-${candidate.id}`}
                      type="button"
                      role="tab"
                      aria-selected={language === candidate.id}
                      aria-controls={`regional-queue-language-panel-${candidate.id}`}
                      tabIndex={language === candidate.id ? 0 : -1}
                      onClick={() => setLanguage(candidate.id)}
                      onKeyDown={(event) =>
                        handleLanguageKey(event, candidate.id, "regional-queue-language-tab")
                      }
                    >
                      <span>{candidate.label}</span>
                      <small>{candidate.version}</small>
                    </button>
                  ))}
                </div>
              </div>

              <div
                id={`regional-queue-language-panel-${language}`}
                className="language-panel"
                role="tabpanel"
                aria-labelledby={`regional-queue-language-tab-${language}`}
              >
                <CodeBlock label={`${guide.label} · setup`} value={regionalQueueGuide.setup} />
                <CodeBlock label={regionalQueueGuide.filename} value={regionalQueueGuide.source} tall />
                <CodeBlock label="Terminal C · run" value={regionalQueueGuide.run} />
              </div>

              <div className="sdk-notes" aria-label="Regional Queue SDK guarantees">
                <article>
                  <span>LEASE SAFETY</span>
                  <strong>Settlement is fenced twice.</strong>
                  <p>
                    Acquire declares a consumer epoch and credit window. Extend, acknowledge, release, nack,
                    and reject require the opaque lease token returned by the replicated transition.
                  </p>
                </article>
                <article>
                  <span>UNKNOWN OUTCOME</span>
                  <strong>Every mutation starts with caller-owned identity.</strong>
                  <p>
                    Enqueue through maintenance require an idempotency key. Rediscovery reuses the same key
                    and payload, so an exact replay returns the committed receipt instead of duplicating work.
                  </p>
                </article>
                <article>
                  <span>OBSERVATION</span>
                  <strong>Operational reads require a leader barrier.</strong>
                  <p>
                    Counts, flow, mutation receipts, status, dead letters, and redrive history explicitly
                    request <code>linearizable</code> and never silently downgrade to stale state.
                  </p>
                </article>
              </div>

              <aside className="docs-access-note">
                <strong>Current boundary</strong>
                <span>
                  Regional Queue v1 is a repository-local, single-partition alpha. Native bidirectional
                  receive, automatic session management, fairness/load evidence, dynamic placement, generated
                  response models, and public package-registry releases remain open.
                </span>
              </aside>
            </section>

            <section
              id="regional-cache"
              className="docs-section"
              aria-labelledby="regional-cache-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>08</span>
                <div>
                  <p className="eyebrow">VERSIONED REGIONAL CACHE V1</p>
                  <h2 id="regional-cache-title">CAS, transaction, fencing, expiry, and recovery.</h2>
                  <p>
                    Regional Cache SDK calls target the existing replicated Cache tablet. Discovery chooses
                    the writable Rust leader; every data request carries generation and tablet-epoch fences,
                    every mutation preserves caller-owned identity, and observations wait for a quorum-backed
                    leader barrier.
                  </p>
                </div>
              </div>

              <div className="guide-intro">
                <span className="step-badge">A</span>
                <div>
                  <h3>Start the voters and provision the Cache</h3>
                  <p>
                    Reuse the disposable three-zone topology and development credentials. The Go bridge
                    provisions <code>sessions</code>; application data then travels directly to the discovered
                    Rust Cache leader.
                  </p>
                </div>
              </div>
              <CodeBlock label="Terminal A · regional nodes" value={regionalNodes} />
              <CodeBlock label="Terminal B · managed bridge" value={regionalControl} />
              <CodeBlock label="Terminal C · provision" value={regionalCacheResource} />

              <div className="language-picker">
                <div>
                  <p className="eyebrow">CHOOSE YOUR REGIONAL CACHE SDK</p>
                  <h3>One strict value and fencing contract in three ecosystems.</h3>
                </div>
                <div className="language-tabs" role="tablist" aria-label="Regional Cache language">
                  {languageGuides.map((candidate) => (
                    <button
                      key={candidate.id}
                      id={`regional-cache-language-tab-${candidate.id}`}
                      type="button"
                      role="tab"
                      aria-selected={language === candidate.id}
                      aria-controls={`regional-cache-language-panel-${candidate.id}`}
                      tabIndex={language === candidate.id ? 0 : -1}
                      onClick={() => setLanguage(candidate.id)}
                      onKeyDown={(event) =>
                        handleLanguageKey(event, candidate.id, "regional-cache-language-tab")
                      }
                    >
                      <span>{candidate.label}</span>
                      <small>{candidate.version}</small>
                    </button>
                  ))}
                </div>
              </div>

              <div
                id={`regional-cache-language-panel-${language}`}
                className="language-panel"
                role="tabpanel"
                aria-labelledby={`regional-cache-language-tab-${language}`}
              >
                <CodeBlock label={`${guide.label} · setup`} value={regionalCacheGuide.setup} />
                <CodeBlock label={regionalCacheGuide.filename} value={regionalCacheGuide.source} tall />
                <CodeBlock label="Terminal C · run" value={regionalCacheGuide.run} />
              </div>

              <div className="sdk-notes" aria-label="Regional Cache SDK guarantees">
                <article>
                  <span>STRICT VALUES</span>
                  <strong>Seven kinds, one canonical wire contract.</strong>
                  <p>
                    Typed constructors cover string, blob, signed counter, hash, list, unique set, and finite
                    sorted set. Invalid members, scores, integers, and transaction bounds fail before
                    discovery.
                  </p>
                </article>
                <article>
                  <span>ATOMICITY &amp; FENCING</span>
                  <strong>Revision checks and lock guards survive leader loss.</strong>
                  <p>
                    CAS distinguishes exact version from missing-at-revision. Transactions commit one
                    revision, while guarded writes require the newest opaque lease token and expose a
                    downstream fence.
                  </p>
                </article>
                <article>
                  <span>DETERMINISTIC EXPIRY</span>
                  <strong>Reads stay pure; maintenance is a replicated command.</strong>
                  <p>
                    TTL never causes a hidden read mutation. Submit bounded maintenance to reclaim due keys
                    and locks; observation, lookup, and status explicitly request <code>linearizable</code>.
                  </p>
                </article>
              </div>

              <aside className="docs-access-note">
                <strong>Current boundary</strong>
                <span>
                  Regional Cache v1 is a repository-local, single-shard alpha. Background active expiry,
                  eviction families, multi-shard transactions, snapshots, Pub/Sub, generated response models,
                  and public package-registry releases remain open.
                </span>
              </aside>
            </section>

            <section
              id="regional-bus"
              className="docs-section"
              aria-labelledby="regional-bus-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>09</span>
                <div>
                  <p className="eyebrow">VERSIONED REGIONAL EVENT BUS V1</p>
                  <h2 id="regional-bus-title">Route, archive, lease, settle, and recover.</h2>
                  <p>
                    Regional Event Bus SDK calls target the replicated Event Bus tablet directly. Discovery
                    selects the writable Rust leader; every request carries generation and tablet-epoch
                    fences, every mutation preserves caller-owned identity, and archive, delivery, and status
                    reads wait for a quorum-backed leader barrier.
                  </p>
                </div>
              </div>

              <div className="guide-intro">
                <span className="step-badge">A</span>
                <div>
                  <h3>Start the voters and provision the Event Bus</h3>
                  <p>
                    Reuse the disposable three-zone topology and development credentials. The Go bridge
                    provisions <code>events</code>; application traffic then goes directly to the discovered
                    Rust Event Bus leader.
                  </p>
                </div>
              </div>
              <CodeBlock label="Terminal A · regional nodes" value={regionalNodes} />
              <CodeBlock label="Terminal B · managed bridge" value={regionalControl} />
              <CodeBlock label="Terminal C · provision" value={regionalBusResource} />

              <div className="language-picker">
                <div>
                  <p className="eyebrow">CHOOSE YOUR REGIONAL EVENT BUS SDK</p>
                  <h3>One delivery and recovery contract in three ecosystems.</h3>
                </div>
                <div className="language-tabs" role="tablist" aria-label="Regional Event Bus language">
                  {languageGuides.map((candidate) => (
                    <button
                      key={candidate.id}
                      id={`regional-bus-language-tab-${candidate.id}`}
                      type="button"
                      role="tab"
                      aria-selected={language === candidate.id}
                      aria-controls={`regional-bus-language-panel-${candidate.id}`}
                      tabIndex={language === candidate.id ? 0 : -1}
                      onClick={() => setLanguage(candidate.id)}
                      onKeyDown={(event) =>
                        handleLanguageKey(event, candidate.id, "regional-bus-language-tab")
                      }
                    >
                      <span>{candidate.label}</span>
                      <small>{candidate.version}</small>
                    </button>
                  ))}
                </div>
              </div>

              <div
                id={`regional-bus-language-panel-${language}`}
                className="language-panel"
                role="tabpanel"
                aria-labelledby={`regional-bus-language-tab-${language}`}
              >
                <CodeBlock label={`${guide.label} · setup`} value={regionalBusGuide.setup} />
                <CodeBlock label={regionalBusGuide.filename} value={regionalBusGuide.source} tall />
                <CodeBlock label="Terminal C · run" value={regionalBusGuide.run} />
              </div>

              <div className="sdk-notes" aria-label="Regional Event Bus SDK guarantees">
                <article>
                  <span>ROUTING &amp; RETRY</span>
                  <strong>Discovery preserves the exact caller-owned mutation.</strong>
                  <p>
                    Publish, subscription, delivery, maintenance, and settlement calls retain the same
                    idempotency key and body across one bounded leader rediscovery. A changed body is a
                    conflict, not a second event.
                  </p>
                </article>
                <article>
                  <span>DELIVERY FENCING</span>
                  <strong>Policy is replicated; settlement requires the opaque lease token.</strong>
                  <p>
                    Pull subscriptions bound timeout, concurrency, attempts, backoff, jitter, and age. Acquire
                    returns a fenced delivery intent; acknowledge and fail cannot settle a stale lease.
                  </p>
                </article>
                <article>
                  <span>LINEARIZABLE OBSERVATION</span>
                  <strong>Query-shaped POST reads still require a leader barrier.</strong>
                  <p>
                    Archive replay, delivery query, mutation lookup, and status explicitly request
                    <code>linearizable</code>. Maintenance advances retry or dead-letter state through a
                    replicated command.
                  </p>
                </article>
              </div>

              <aside className="docs-access-note">
                <strong>Current boundary</strong>
                <span>
                  Regional Event Bus v1 is a repository-local, single-shard pull-delivery alpha. External
                  webhook execution, request signing, managed push workers, automatic polling, cross-shard
                  ordering, generated response models, and public package-registry releases remain open.
                </span>
              </aside>
            </section>

            <section
              id="sdk-reference"
              className="docs-section"
              aria-labelledby="sdk-reference-title"
              tabIndex={-1}
            >
              <div className="docs-section__heading">
                <span>10</span>
                <div>
                  <p className="eyebrow">STANDALONE ALPHA SURFACE</p>
                  <h2 id="sdk-reference-title">The same operation, native to each ecosystem.</h2>
                  <p>
                    All implemented standalone profile operations have Go, Java, and Python entry points. The
                    versioned regional Stream, Queue, Cache, and Event Bus clients above are intentionally
                    separate because they add authentication, route discovery, fencing, and bounded idempotent
                    rediscovery. Responses remain dynamic documents in this alpha.
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
                    node. Regional clients instead take all configured voter endpoints plus a bearer token.
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
                    a context for per-call cancellation and deadlines. Standalone helpers keep their local
                    contract; <code>RegionalStreamClient</code>, <code>RegionalQueueClient</code>, and
                    <code>RegionalCacheClient</code> are the explicit replicated alternatives.
                  </p>
                </article>
              </div>
            </section>

            <section id="reference" className="docs-section" aria-labelledby="reference-title" tabIndex={-1}>
              <div className="docs-section__heading">
                <span>11</span>
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
                  eyebrow="SDK CONTRACT"
                  title="Regional Stream SDK"
                  description="Fully qualified v1 routes, leader discovery, generation/tablet fencing, idempotent retry, linearizable reads, and three-language examples."
                  href={`${repositoryDocsUrl}/REGIONAL_STREAM_SDK.md`}
                />
                <ReferenceCard
                  eyebrow="SDK CONTRACT"
                  title="Regional Queue SDK"
                  description="Complete Queue lifecycle, leader discovery, generation/tablet fencing, exact mutation replay, linearizable reads, and three-language examples."
                  href={`${repositoryDocsUrl}/REGIONAL_QUEUE_SDK.md`}
                />
                <ReferenceCard
                  eyebrow="SDK CONTRACT"
                  title="Regional Cache SDK"
                  description="Strict values, CAS, atomic transaction, fenced locks, explicit expiry, leader discovery, exact retry, and three-language examples."
                  href={`${repositoryDocsUrl}/REGIONAL_CACHE_SDK.md`}
                />
                <ReferenceCard
                  eyebrow="SDK CONTRACT"
                  title="Regional Event Bus SDK"
                  description="Subscription policy, replicated ingress, delivery leases, retry and dead-letter transitions, archive replay, exact retry, and three-language examples."
                  href={`${repositoryDocsUrl}/REGIONAL_EVENT_BUS_SDK.md`}
                />
                <ReferenceCard
                  eyebrow="REGIONAL RUNTIME"
                  title="Multi-tablet operations"
                  description="Catalog authority, topology/capacity admission, fenced routes, quorum-confirmed reads, recovery campaign, and explicit non-claims."
                  href={`${repositoryDocsUrl}/REGIONAL_RUNTIME.md`}
                />
                <ReferenceCard
                  eyebrow="CONSISTENCY"
                  title="Quorum read barriers"
                  description="Safe ReadIndex admission, majority and local-apply completion, explicit stale opt-in, timeout behavior, and non-claims."
                  href={`${repositoryDocsUrl}/adr/0013-quorum-read-barriers.md`}
                />
                <ReferenceCard
                  eyebrow="RECOVERY CORE"
                  title="Consensus checkpoints"
                  description="EPSN v1/v2 bytes, native profile restore, bounded retry history, physical reclamation, lagging-voter catch-up, checkpoint-plus-tail reopen, and exact non-claims."
                  href={`${repositoryDocsUrl}/CONSENSUS_CHECKPOINTS.md`}
                />
                <ReferenceCard
                  eyebrow="RECOVERY DESIGN"
                  title="Native checkpoints and reclamation"
                  description="Profile ownership, rolling digest and retry bounds, durable ordering, atomic EPRS replacement, required evidence, and backup/PITR non-claims."
                  href={`${repositoryDocsUrl}/adr/0022-profile-native-checkpoints-and-physical-reclamation.md`}
                />
                <ReferenceCard
                  eyebrow="CLUSTER CORE"
                  title="Experimental Stream tablet"
                  description="Typed single and bounded compressed-batch commands, fixed-voter majority, correlated offsets, failover, idempotency, and all-voter recovery."
                  href={`${repositoryDocsUrl}/STREAM_TABLET.md`}
                />
                <ReferenceCard
                  eyebrow="STREAM DESIGN"
                  title="Batch compression decision"
                  description="Canonical framing, all required codecs, atomicity, decompression limits, compatibility rules, and explicit native/SDK non-claims."
                  href={`${repositoryDocsUrl}/adr/0015-stream-batch-compression.md`}
                />
                <ReferenceCard
                  eyebrow="CONSUMER GROUPS"
                  title="Replicated checkpoint decision"
                  description="Next-offset commit/reset, caller-generation owner fencing, committed rejection, lag/replay routes, recovery evidence, and coordinator/SDK non-claims."
                  href={`${repositoryDocsUrl}/adr/0016-stream-consumer-group-checkpoints.md`}
                />
                <ReferenceCard
                  eyebrow="QUEUE TABLET"
                  title="Experimental replicated Queue"
                  description="Typed mutations, fenced leases, bounded consumer credit, failover/redelivery, immutable DLQ/redrive history, and all-voter recovery."
                  href={`${repositoryDocsUrl}/QUEUE_TABLET.md`}
                />
                <ReferenceCard
                  eyebrow="FLOW CONTROL"
                  title="Queue credit and in-flight windows"
                  description="Atomic grant semantics, cross-epoch consumer accounting, command compatibility, flow evidence, and streaming non-claims."
                  href={`${repositoryDocsUrl}/adr/0014-queue-consumer-credit.md`}
                />
                <ReferenceCard
                  eyebrow="QUEUE DESIGN"
                  title="Regional Queue routing decision"
                  description="Native v1 route shape, shared discovery and retry contract, lease-token handling, authorization, recovery evidence, and explicit alpha boundaries."
                  href={`${repositoryDocsUrl}/adr/0018-regional-queue-v1-and-sdk-routing.md`}
                />
                <ReferenceCard
                  eyebrow="CACHE DESIGN"
                  title="Regional Cache routing decision"
                  description="Native v1 route shape, strict values and mutations, CAS/transaction/expiry/lock semantics, shared retry contract, and alpha boundaries."
                  href={`${repositoryDocsUrl}/adr/0019-regional-cache-v1-and-sdk-routing.md`}
                />
                <ReferenceCard
                  eyebrow="EVENT BUS DESIGN"
                  title="Regional Event Bus routing decision"
                  description="Native v1 route shape, complete pull-delivery lifecycle, subscription policy, shared retry contract, recovery evidence, and executor non-claims."
                  href={`${repositoryDocsUrl}/adr/0020-regional-event-bus-v1-and-sdk-routing.md`}
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
                  title="v0.1.0-alpha.3 release notes"
                  description="Verified milestone highlights, source-only artifacts, compatibility guidance, and explicit alpha limitations."
                  href={`${repositoryDocsUrl}/releases/v0.1.0-alpha.3.md`}
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
