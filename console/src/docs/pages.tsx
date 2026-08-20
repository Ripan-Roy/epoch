import type { ReactNode } from "react";

import { CodeBlock, CodeTabs, type CodeSample } from "./CodeBlock";
import {
  consensusCheckpoint,
  epochTargetLanguageGuides,
  governanceInventory,
  languageGuides,
  nodeRestart,
  nodeStart,
  regionalBusLanguageGuides,
  regionalBusResource,
  regionalCacheLanguageGuides,
  regionalCacheResource,
  regionalControl,
  regionalLanguageGuides,
  regionalNodes,
  regionalQueueLanguageGuides,
  regionalQueueResource,
  regionalResource,
  regionalWebhookConfiguration,
  repositoryDocsUrl,
  sdkSurface,
  signedWebhookLanguageGuides,
  type LanguageId,
  type RegionalGuide,
} from "./content";

/* --------------------------------------------------------------------------
   Shared building blocks
   -------------------------------------------------------------------------- */

function Step({
  index,
  id,
  title,
  children,
}: {
  index: number;
  id: string;
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="docs-step" id={id}>
      <h2>
        <span className="step-badge" aria-hidden="true">
          {index}
        </span>
        {title}
      </h2>
      {children}
    </section>
  );
}

function Topic({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  return (
    <section className="docs-topic" id={id}>
      <h2>{title}</h2>
      {children}
    </section>
  );
}

function Note({ title, children }: { title: string; children: ReactNode }) {
  return (
    <aside className="docs-access-note">
      <strong>{title}</strong>
      <span>{children}</span>
    </aside>
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

function EvidenceCard({ label, claim, children }: { label: string; claim: string; children: ReactNode }) {
  return (
    <article>
      <span>{label}</span>
      <strong>{claim}</strong>
      <p>{children}</p>
    </article>
  );
}

function samplesFrom(
  pick: (guide: (typeof languageGuides)[number]) => string,
  filename: (guide: (typeof languageGuides)[number]) => string,
): CodeSample[] {
  return languageGuides.map((guide) => ({
    language: guide.id,
    filename: filename(guide),
    code: pick(guide),
  }));
}

function regionalSamples(
  guides: Record<LanguageId, RegionalGuide>,
  pick: (guide: RegionalGuide) => string,
  filename: (guide: RegionalGuide) => string,
): CodeSample[] {
  return (Object.keys(guides) as LanguageId[]).map((language) => ({
    language,
    filename: filename(guides[language]),
    code: pick(guides[language]),
  }));
}

/* --------------------------------------------------------------------------
   Overview
   -------------------------------------------------------------------------- */

export function OverviewBody() {
  return (
    <>
      <dl className="docs-specline" aria-label="Quickstart scope">
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

      <Note title="Private alpha access">
        Running these examples requires access to the repository checkout. The SDK packages are not published
        to public registries yet; the exact reviewed source remains embedded in each guide.
      </Note>

      <Topic id="start-here" title="Start here">
        <div className="reference-grid">
          <a className="reference-card" href="#/docs/quickstart">
            <span>Guide</span>
            <strong>Quickstart</strong>
            <p>
              Create a durable Stream and Work Queue, move real events through both, then restart the process.
            </p>
            <em aria-hidden="true">10 minutes →</em>
          </a>
          <a className="reference-card" href="#/docs/restart">
            <span>Guide</span>
            <strong>Restart verification</strong>
            <p>Prove which records survived the restart, and which were never claimed to.</p>
            <em aria-hidden="true">5 minutes →</em>
          </a>
          <a className="reference-card" href="#/docs/guarantees">
            <span>Concept</span>
            <strong>Guarantees &amp; errors</strong>
            <p>What local durable covers, what it does not, and how to read a typed failure.</p>
            <em aria-hidden="true">Read →</em>
          </a>
          <a className="reference-card" href="#/docs/resource-governance">
            <span>Concept</span>
            <strong>Resource governance</strong>
            <p>Require ownership and classification, filter inventory, and explain cost drivers.</p>
            <em aria-hidden="true">Read →</em>
          </a>
          <a className="reference-card" href="#/docs/sdk-reference">
            <span>Reference</span>
            <strong>SDK reference</strong>
            <p>Every implemented operation with its Go, Java, and Python entry point.</p>
            <em aria-hidden="true">Read →</em>
          </a>
        </div>
      </Topic>

      <Topic id="regional-guides" title="Regional SDK guides">
        <p>
          The regional clients are separate from the standalone quickstart: they add authentication, leader
          discovery, generation and tablet fencing, and bounded idempotent retry.
        </p>
        <div className="reference-grid">
          <a className="reference-card" href="#/docs/regional-stream">
            <span>SDK</span>
            <strong>Regional Stream</strong>
            <p>Keyed routing across replicated shards, atomic batches, and coordinated consumers.</p>
            <em aria-hidden="true">Read →</em>
          </a>
          <a className="reference-card" href="#/docs/regional-queue">
            <span>SDK</span>
            <strong>Regional Queue</strong>
            <p>The complete lease, retry, dead-letter, and redrive lifecycle through one fenced API.</p>
            <em aria-hidden="true">Read →</em>
          </a>
          <a className="reference-card" href="#/docs/regional-cache">
            <span>SDK</span>
            <strong>Regional Cache</strong>
            <p>Strict typed values, compare-and-set, transactions, fenced locks, and explicit expiry.</p>
            <em aria-hidden="true">Read →</em>
          </a>
          <a className="reference-card" href="#/docs/regional-bus">
            <span>SDK</span>
            <strong>Regional Event Bus</strong>
            <p>Replicated ingress, delivery leases, retry and dead-letter transitions, and archive replay.</p>
            <em aria-hidden="true">Read →</em>
          </a>
        </div>
      </Topic>
    </>
  );
}

/* --------------------------------------------------------------------------
   Quickstart
   -------------------------------------------------------------------------- */

export function QuickstartBody() {
  return (
    <>
      <Step index={1} id="start-node" title="Start a node">
        <p>
          Keep this terminal running. Every SDK below talks to the native HTTP endpoint at{" "}
          <code>127.0.0.1:7601</code>. Set <code>EPOCH_URL</code> to use another node address.
        </p>
        <CodeBlock label="Terminal A · repository root" value={nodeStart} />
      </Step>

      <Step index={2} id="install-sdk" title="Install the SDK">
        <p>
          The SDKs are pre-alpha and repository-local. These setup commands use the checked-in package rather
          than implying a public registry release.
        </p>
        <CodeTabs
          label="SDK setup"
          samples={samplesFrom(
            (guide) => guide.setup,
            () => "Terminal · setup",
          )}
          collapsible={false}
        />
      </Step>

      <Step index={3} id="write-example" title="Create, publish, consume, and acknowledge">
        <p>
          Seed mode creates both resources with explicit local durability, appends a Stream event, enqueues
          two jobs, acquires one lease, and acknowledges it.
        </p>
        <CodeTabs
          label="Walkthrough source"
          samples={samplesFrom(
            (guide) => guide.source,
            (guide) => guide.filename,
          )}
        />
      </Step>

      <Step index={4} id="run-seed" title="Run the first half">
        <p>When seed mode asks for a restart, leave this terminal open.</p>
        <CodeTabs
          label="Run"
          samples={samplesFrom(
            (guide) => guide.run,
            () => "Terminal · run",
          )}
          collapsible={false}
        />
      </Step>

      <p className="docs-nextstep">
        Next: <a href="#/docs/restart">restart the node and verify what survived →</a>
      </p>
    </>
  );
}

/* --------------------------------------------------------------------------
   Restart verification
   -------------------------------------------------------------------------- */

export function RestartBody() {
  return (
    <>
      <Step index={1} id="restart-node" title="Restart the same data directory">
        <p>
          Stop only the process. Keep <code>.epoch</code>, restart the node, then run the selected SDK in{" "}
          <code>verify</code> mode.
        </p>
        <CodeBlock label="Terminal A · restart" value={nodeRestart} />
      </Step>

      <Topic id="what-survives" title="What the verify run proves">
        <div className="verification-grid">
          <EvidenceCard label="Stream" claim="One record returns at offset 0.">
            The append was fsynced and replayed from the standalone journal.
          </EvidenceCard>
          <EvidenceCard label="Queue" claim="Only the unacknowledged job returns.">
            The message and the earlier lease settlement both survived restart.
          </EvidenceCard>
          <EvidenceCard label="Disk" claim="Segmented WAL remains local.">
            Evidence lives under <code>.epoch/engine-wal/</code>; it is not a replica or backup.
          </EvidenceCard>
        </div>
      </Topic>

      <p className="docs-nextstep">
        Next: <a href="#/docs/guarantees">read exactly what local durable does and does not claim →</a>
      </p>
    </>
  );
}

/* --------------------------------------------------------------------------
   Guarantees & errors
   -------------------------------------------------------------------------- */

export function GuaranteesBody() {
  return (
    <>
      <Topic id="scope" title="What local durable covers">
        <div className="guarantee-grid">
          <div className="guarantee-grid__yes">
            <p className="eyebrow">What it does</p>
            <ul>
              <li>Fsyncs accepted Stream and Queue mutations before applying them.</li>
              <li>Replays checksum-valid records after a process restart.</li>
              <li>Persists queue leases, settlements, retries, and redrives.</li>
            </ul>
          </div>
          <div className="guarantee-grid__no">
            <p className="eyebrow">What it does not do</p>
            <ul>
              <li>Replicate to another process, host, zone, or region.</li>
              <li>Survive loss of the machine and its storage.</li>
              <li>Provide snapshots, compaction, PITR, or quorum acknowledgement.</li>
            </ul>
          </div>
        </div>
      </Topic>

      <Topic id="errors" title="Error contract">
        <div className="error-contract">
          <div>
            <h3>Transport-retryable does not mean mutation-safe.</h3>
            <p>
              The SDKs perform no hidden retries. A timeout can leave a write outcome unknown, so inspect the
              typed error and the operation’s idempotency contract before trying again.
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
      </Topic>
    </>
  );
}

/* --------------------------------------------------------------------------
   Regional runtime milestone
   -------------------------------------------------------------------------- */

export function ClusterMilestoneBody() {
  return (
    <>
      <Topic id="scope" title="What the regional runtime does today">
        <p>
          Three Rust nodes now run a dedicated catalog group plus simultaneous Cache, Stream, Queue, and Event
          Bus tablets behind resource/shard routing. The Go control plane reconciles desired state through
          Rust, transactionally persists management metadata, and exposes observed placement to the browser;
          the console never contacts a storage node. The public SDK quickstart remains standalone and{" "}
          <code>local_durable</code>.
        </p>
        <p>
          The fixed voters now report configured region, zone, class, and live group capacity; Go validates
          requested regions, minimum zones, class, and incremental capacity before touching the catalog. That
          is not dynamic membership, rack placement, or online rebalance. The Go metadata database still has
          one process owner and these regional routes remain experimental.
        </p>
        <p>
          Regional reads default to a safe leader <code>ReadIndex</code>, wait for majority confirmation and
          local profile apply, and expose exact barrier evidence; callers must explicitly request{" "}
          <code>local_stale</code> to bypass that barrier. Queue acquire can now declare request credit and a
          per-consumer <code>max_in_flight</code> window; the replicated transition returns exact saturation
          and remaining-capacity evidence, and settlement replenishes the window.
        </p>
        <p>
          Stream append can now carry 1–1,000 correlated records through none, gzip, LZ4, Snappy, or Zstd
          frames with hard compressed and expanded limits; the complete batch becomes visible atomically and
          exact retries retain every sequence-to-offset result. A Stream consumer group can now replicate its
          next offset, commit forward, reset explicitly, observe lag, and replay from that checkpoint;
          caller-supplied generations fence an old or conflicting member across failover and EPRS rebuild.
        </p>
        <p>
          Canonical command v4 now commits per-partition time, persisted-byte, and record bounds; the regional
          leader proposes idle-stream maintenance at the first replicated deadline, and every voter preserves
          the same base offset, retention watermark, and stale-checkpoint signal through checkpoint restore.
          Logical shard 0 now also replicates bounded join, heartbeat, leave, leader-owned dead-member expiry,
          one membership generation, and deterministic assignment of every Stream shard.
        </p>
        <p>
          These are experimental HTTP/tablet slices, not yet cooperative revoke, atomic checkpoint handoff,
          native bidirectional streaming, automatic client batching, or compression negotiation. The separate
          regional Stream, Queue, Cache, and Event Bus v1 clients expose the implemented operations with
          leader/fence-aware Go, Java, and Python routing. Stream resources can now route one versioned UTF-8
          key across several independently replicated shards while failing closed on a concurrent generation
          change.
        </p>
        <p>
          Stream session assignment is coordinated but remains separate from each shard&apos;s
          checkpoint-owner fence; Queue delivery and Event Bus pull remain request/response rather than
          managed streaming sessions. An opt-in Event Bus worker now executes signed HTTP/webhook targets from
          the current leader after a replicated lease. The current regional leaders also propose Stream
          retention/session, Queue timer, Cache value/lock, and Event Bus lease-timeout commands at exact
          replicated deadlines; topology reports node-local scheduler and webhook counters. Managed HTTP/gRPC
          and regional HTTP require a shared deny-by-default bootstrap bearer policy, but that is not OIDC,
          TLS/mTLS, credential expiry/revocation, or immutable audit export.
        </p>
      </Topic>

      <Topic id="evidence" title="Observed evidence">
        <div className="verification-grid">
          <EvidenceCard
            label="Majority"
            claim="Catalog and data groups commit through durable voter majorities."
          >
            Generation/tablet fences reject stale routes; default reads require quorum-confirmed ReadIndex
            evidence with no silent downgrade.
          </EvidenceCard>
          <EvidenceCard
            label="Queue flow"
            claim="Consumer credit cannot exceed its declared live-lease window."
          >
            Repeated receive saturates at zero; Ack, Nack, Release, Reject, or expiry processing replenishes
            capacity from replicated state.
          </EvidenceCard>
          <EvidenceCard
            label="Queue SDK recovery"
            claim="Lease, retry, dead-letter, redrive, and settle survive leader loss."
          >
            The real Python client preserves <code>docs-python-redrive-v1</code>, route fences, and opaque
            lease tokens while the Docker campaign kills the Queue leader and later reopens every voter from
            the same volumes.
          </EvidenceCard>
          <EvidenceCard
            label="Cache SDK recovery"
            claim="Typed values, CAS, transaction, fencing, and expiry survive leader loss."
          >
            The Python client executes the complete shard-0 lifecycle after Cache leadership changes, then the
            old voter catches up and all three reopen the same committed state.
          </EvidenceCard>
          <EvidenceCard
            label="Event Bus SDK recovery"
            claim="Ingress, delivery leases, retry, archive, and settlement survive leader loss."
          >
            The Python client preserves exact publish and settlement identities while the same Docker campaign
            replaces the Event Bus leader, catches up the old voter, and reopens every volume.
          </EvidenceCard>
          <EvidenceCard
            label="Signed webhook recovery"
            claim="A 503 retry becomes one durable two-attempt acknowledgement."
          >
            A real receiver observes attempts 1 and 2 with one delivery ID and distinct signatures. All three
            voters converge the failed/Ack history and return it after every process reopens existing storage.
          </EvidenceCard>
          <EvidenceCard
            label="Stream batch"
            claim="Every compressed frame is bounded before one atomic replicated apply."
          >
            Five codec modes return exact per-sequence offsets; real-runtime and Python-gzip container tests
            prove retry, failover, and EPRS recovery without changing v1 append bytes.
          </EvidenceCard>
          <EvidenceCard
            label="Stream group"
            claim="Checkpoint progress and ownership fences survive leader and process loss."
          >
            Commit, reset, lag, and replay converge on every voter; stale or wrong generations are durable
            rejected outcomes without moving the next offset.
          </EvidenceCard>
          <EvidenceCard
            label="Stream retention"
            claim="Time, byte, and combined deletion advances through one committed boundary."
          >
            Configure, leader-owned idle maintenance, append-triggered expiry, out-of-range checkpoint
            signaling, exact retry, three-voter convergence, and native snapshot restore are covered without
            renumbering offsets.
          </EvidenceCard>
          <EvidenceCard
            label="Admission + observation"
            claim="Go checks fixed-voter zones and capacity, then reports actual serving routes."
          >
            A limiting node rejects before catalog apply; leader loss becomes degraded two-voter placement.
          </EvidenceCard>
          <EvidenceCard
            label="Recovery"
            claim="Go intent and Rust groups recover from their owned durable state."
          >
            Control restart preserves exact retries; a full data-plane <code>SIGKILL</code> cycle reopens the
            same EPRS volumes and profile digests.
          </EvidenceCard>
          <EvidenceCard
            label="Trust baseline"
            claim="Go and Rust authorize the action at the parsed tenant scope."
          >
            Cross-tenant lists are filtered, Go uses a distinct Rust workload credential, and bounded decision
            logs contain no bearer value or payload.
          </EvidenceCard>
        </div>
      </Topic>

      <Topic id="proofs" title="Run the proofs">
        <CodeBlock
          label="Disposable fixed-group and regional proofs"
          value={
            "make test-stream-tablet\nmake test-queue-tablet\nmake test-cache-tablet\nmake test-bus-tablet\nmake test-regional-runtime"
          }
        />
      </Topic>
    </>
  );
}

/* --------------------------------------------------------------------------
   Consensus recovery
   -------------------------------------------------------------------------- */

export function ConsensusRecoveryBody() {
  return (
    <>
      <Topic id="how-it-works" title="How a checkpoint is taken">
        <p>
          The replicated core can encode a bounded canonical <code>EPSN v2</code> image for Catalog, Stream,
          Queue, Cache, or Event Bus at one voter&apos;s applied Raft index. It fsyncs the checkpoint,
          atomically replaces the EPRS journal with one compacted baseline, then installs the logical
          snapshot. A lagging fixed voter validates and persists that native state before applying its
          retained committed tail. EPSN v1 remains readable.
        </p>
        <div className="verification-grid">
          <EvidenceCard label="Durable order" claim="Disk becomes authoritative before memory changes.">
            A post-fsync failure stops the live adapter; reopening the same journal recovers the checkpoint,
            proposal lookup, and digest without inventing another receipt.
          </EvidenceCard>
          <EvidenceCard
            label="Follower catch-up"
            claim="Native profile restore completes before the retained tail applies."
          >
            Catalog proves lagging-voter snapshot-plus-tail catch-up, while all five profiles force a
            checkpoint and restore automatically in their real three-voter restart tests.
          </EvidenceCard>
          <EvidenceCard label="Exact boundary" claim="This is a consensus checkpoint, not a backup.">
            V2 bounds exact-retry metadata to 1,024 records and 1 MiB, caps profile bytes at 4 MiB and the
            complete image at 6 MiB, and reclaims older journal generations. It is not a downloadable backup,
            PITR, dynamic membership, or production repair workflow.
          </EvidenceCard>
        </div>
      </Topic>

      <Topic id="inspect" title="Inspect a local checkpoint">
        <CodeBlock label="Local checkpoint evidence" value={consensusCheckpoint} />
        <Note title="Local experimental surface">
          <code>checkpoint_index</code> and <code>retained_log_first_index</code> are voter-local facts. The
          trigger is explicit and unauthenticated on the diagnostic listener; do not expose it to an untrusted
          network.
        </Note>
      </Topic>
    </>
  );
}

export function ResourceGovernanceBody() {
  return (
    <>
      <Topic id="contract" title="Governance is managed desired state">
        <p>
          Every newly managed regional resource declares an owner, cost center, classification, and optional
          bounded custom tags. Environment remains authoritative in the fully qualified resource name, so it
          cannot drift from authorization or placement scope.
        </p>
        <div className="sdk-notes">
          <EvidenceCard label="Required" claim="New managed resources fail closed without governance.">
            Owner and cost center are canonical lower-case identifiers. Classification is exactly public,
            internal, confidential, or restricted. The <code>epoch.io/</code> tag prefix is reserved.
          </EvidenceCard>
          <EvidenceCard label="Generation fenced" claim="Metadata changes are real desired-state changes.">
            Ownership transfer, reclassification, cost-center changes, and tag changes require the current
            expected generation and participate in idempotency fingerprints.
          </EvidenceCard>
          <EvidenceCard label="Compatible" claim="Valid legacy state remains readable.">
            Existing Go registry and Rust catalog records without governance reopen unchanged. The stricter
            requirement applies when a new managed regional resource is accepted.
          </EvidenceCard>
        </div>
      </Topic>

      <Topic id="filter" title="Filter the authorized inventory">
        <p>
          All supplied filters use AND semantics. Repeat <code>tag=key=value</code> for exact tag matches.
          Canonical duplicate keys, invalid classifications, reserved prefixes, and oversized values are
          rejected instead of being ignored.
        </p>
        <CodeBlock label="Managed inventory query" value={governanceInventory} />
      </Topic>

      <Topic id="cost" title="Explain allocation drivers">
        <p>
          The Go browser BFF aggregates only resources that passed tenant authorization and the requested
          filters. Deterministically ordered rows report resource and desired-shard counts by cost center and
          classification. This is explainability metadata, not currency, usage metering, invoicing, or a
          billing ledger.
        </p>
      </Topic>

      <Topic id="recovery" title="Survive both control and data-plane recovery">
        <p>
          Go durably stores the canonical value and forwards it to the Rust regional catalog. Catalog command
          and snapshot version 3 preserve it through quorum replication. The container campaign compares the
          Go and Rust views before and after control-process <code>SIGKILL</code>, leader loss, and all-node
          same-volume reopen.
        </p>
        <div className="reference-grid">
          <ReferenceCard
            eyebrow="Guide"
            title="Complete governance contract"
            description="Validation, query, compatibility, recovery, and non-claims."
            href={`${repositoryDocsUrl}/RESOURCE_GOVERNANCE.md`}
          />
          <ReferenceCard
            eyebrow="Decision"
            title="ADR-0033"
            description="Why environment is not duplicated and aggregation follows authorization."
            href={`${repositoryDocsUrl}/adr/0033-resource-governance-and-cost-attribution.md`}
          />
        </div>
      </Topic>
    </>
  );
}

/* --------------------------------------------------------------------------
   Regional SDK guides — one shared shape, four workloads
   -------------------------------------------------------------------------- */

interface RegionalGuidePageProps {
  provisionLabel: string;
  provision: string;
  provisionTitle: string;
  provisionBody: ReactNode;
  guides: Record<LanguageId, RegionalGuide>;
  guarantees: ReactNode;
  extra?: ReactNode;
  boundary: ReactNode;
}

function RegionalGuideBody({
  provisionLabel,
  provision,
  provisionTitle,
  provisionBody,
  guides,
  guarantees,
  extra,
  boundary,
}: RegionalGuidePageProps) {
  return (
    <>
      <Step index={1} id="provision" title={provisionTitle}>
        {provisionBody}
        <CodeBlock label="Terminal A · regional nodes" value={regionalNodes} />
        <CodeBlock label="Terminal B · managed bridge" value={regionalControl} />
        <CodeBlock label={provisionLabel} value={provision} />
      </Step>

      <Step index={2} id="install" title="Prepare the SDK">
        <CodeTabs
          label="SDK setup"
          samples={regionalSamples(
            guides,
            (guide) => guide.setup,
            () => "Terminal · setup",
          )}
          collapsible={false}
        />
      </Step>

      <Step index={3} id="example" title="Run the example">
        <CodeTabs
          label="Regional example"
          samples={regionalSamples(
            guides,
            (guide) => guide.source,
            (guide) => guide.filename,
          )}
        />
        <CodeTabs
          label="Run"
          samples={regionalSamples(
            guides,
            (guide) => guide.run,
            () => "Terminal · run",
          )}
          collapsible={false}
        />
      </Step>

      <Topic id="guarantees" title="What the client guarantees">
        <div className="sdk-notes">{guarantees}</div>
      </Topic>

      {extra}

      <Topic id="boundary" title="Current boundary">
        {boundary}
      </Topic>
    </>
  );
}

export function RegionalStreamBody() {
  return (
    <RegionalGuideBody
      provisionTitle="Start the voters and provision three Stream shards"
      provisionBody={
        <p>
          The credentials below are public development fixtures. Use them only with this disposable local
          topology. The Go bridge is needed to create the resource; once materialized, the SDK data path
          remains available if that bridge stops.
        </p>
      }
      provisionLabel="Terminal C · provision"
      provision={regionalResource}
      guides={regionalLanguageGuides}
      guarantees={
        <>
          <EvidenceCard label="Key routing" claim="The server publishes one cross-language partitioner.">
            <code>fnv1a64_utf8_mod_n_v1</code> hashes the event key, or its ID when the key is empty. Go,
            Java, and Python calculate the same shard and report that logical partition in every receipt,
            record, checkpoint, retention observation, and status response.
          </EvidenceCard>
          <EvidenceCard label="Expansion race" claim="A keyed append pins the discovered generation.">
            If the target shard reports a different resource generation, the client fails before sending the
            record. It never silently remaps an uncertain mutation after the shard count changes. Ordinary
            bounded leader rediscovery still reuses the exact idempotency key.
          </EvidenceCard>
          <EvidenceCard label="Consistency" claim="Reads require a leader barrier.">
            Fetch, group fetch, lag, session, and retention observation explicitly request{" "}
            <code>linearizable</code>. They never fall back to a local stale read. A minority returns a
            retryable unavailability error.
          </EvidenceCard>
          <EvidenceCard label="Atomic batches" claim="Client-framed atomic batches preserve exact bytes.">
            Every SDK builds canonical <code>none</code> or gzip frames and accepts exact caller-produced LZ4,
            Snappy, or Zstd frames. One explicit shard receives all records as a single transition; retry
            reuses the same frame and idempotency key.
          </EvidenceCard>
          <EvidenceCard label="Consumer sessions" claim="Shard zero owns one durable membership generation.">
            Join, heartbeat, and leave replicate with the Stream. The shard-zero leader proposes expiry at the
            first replicated member deadline; explicit maintenance remains available. Lexically ordered
            members receive deterministic round-robin shard assignments that survive leader loss and full-node
            reopen. The examples above exercise this lifecycle.
          </EvidenceCard>
          <EvidenceCard
            label="Automatic maintenance"
            claim="Only the current Raft leader owns each due timer."
          >
            Stream, Queue, Cache, and Event Bus publish pure earliest deadlines. A deterministic consensus
            proposal carries the exact due time, while authorized topology counters expose passes, due work,
            submissions, pending proposals, and errors.
          </EvidenceCard>
        </>
      }
      boundary={
        <p>
          Regional v1 covers keyed append, direct per-shard operations, explicit-shard atomic batches, and
          shard-zero coordinated membership/assignment. Each shard&apos;s record command remains physical
          partition 0 for compatibility. Cooperative revoke, atomic assignment-plus-offset handoff, online
          expansion/remapping, automatic producer batching, cross-shard transactions, and package-registry
          releases remain open.
        </p>
      }
    />
  );
}

export function RegionalQueueBody() {
  return (
    <RegionalGuideBody
      provisionTitle="Start the voters and provision the Queue"
      provisionBody={
        <p>
          Reuse the same disposable regional topology and development-only credentials. The Go bridge
          provisions <code>jobs</code>; the native data path then goes directly to the discovered Rust Queue
          leader.
        </p>
      }
      provisionLabel="Terminal C · provision"
      provision={regionalQueueResource}
      guides={regionalQueueLanguageGuides}
      guarantees={
        <>
          <EvidenceCard label="Lease safety" claim="Settlement is fenced twice.">
            Acquire declares a consumer epoch and credit window. Extend, acknowledge, release, nack, and
            reject require the opaque lease token returned by the replicated transition.
          </EvidenceCard>
          <EvidenceCard label="Unknown outcome" claim="Every mutation starts with caller-owned identity.">
            Enqueue through maintenance require an idempotency key. Rediscovery reuses the same key and
            payload, so an exact replay returns the committed receipt instead of duplicating work.
          </EvidenceCard>
          <EvidenceCard label="Observation" claim="Operational reads require a leader barrier.">
            Counts, flow, mutation receipts, status, dead letters, and redrive history explicitly request{" "}
            <code>linearizable</code> and never silently downgrade to stale state.
          </EvidenceCard>
        </>
      }
      boundary={
        <p>
          Regional Queue v1 is a repository-local, single-partition alpha. Native bidirectional receive,
          automatic session management, fairness/load evidence, dynamic placement, generated response models,
          and public package-registry releases remain open.
        </p>
      }
    />
  );
}

export function RegionalCacheBody() {
  return (
    <RegionalGuideBody
      provisionTitle="Start the voters and provision the Cache"
      provisionBody={
        <p>
          Reuse the disposable three-zone topology and development credentials. The Go bridge provisions{" "}
          <code>sessions</code> with LRU, memory/cold byte caps, and named quorum durability; application data
          then travels directly to the discovered Rust Cache leader.
        </p>
      }
      provisionLabel="Terminal C · provision"
      provision={regionalCacheResource}
      guides={regionalCacheLanguageGuides}
      guarantees={
        <>
          <EvidenceCard label="Strict values" claim="Seven kinds, one canonical wire contract.">
            Typed constructors cover string, blob, signed counter, hash, list, unique set, and finite sorted
            set. Invalid members, scores, integers, and transaction bounds fail before discovery.
          </EvidenceCard>
          <EvidenceCard
            label="Atomicity &amp; fencing"
            claim="Revision checks and lock guards survive leader loss."
          >
            CAS distinguishes exact version from missing-at-revision. Atomic batches preserve caller order and
            commit through one request and proposal, while guarded writes require the newest opaque lease
            token and expose a downstream fence.
          </EvidenceCard>
          <EvidenceCard
            label="Deterministic eviction"
            claim="Entry and byte admission produce the same victim everywhere."
          >
            Managed configuration reaches every voter. <code>Get</code> records LRU/LFU access exactly once;
            pure <code>Observe</code> never changes order. All-key and volatile LRU, LFU, random, and TTL
            admissions stage memory/cold byte accounting, eviction, and the write atomically.
          </EvidenceCard>
          <EvidenceCard
            label="Deterministic expiry"
            claim="Reads stay pure; maintenance is a replicated command."
          >
            TTL never causes a hidden read mutation. The regional leader submits bounded maintenance at the
            earliest value or lock deadline; explicit maintenance remains available, and observation, lookup,
            and status explicitly request <code>linearizable</code>.
          </EvidenceCard>
          <EvidenceCard label="Recovery" claim="Backup, PITR, and changes share the replicated history.">
            A canonical bounded backup publishes its digest and restorable revision window. Restore is one
            checked consensus transition with fresh non-ABA versions, while the durable change cursor records
            mutations, expiry, eviction, and restore.
          </EvidenceCard>
          <EvidenceCard label="Advanced state" claim="Transforms and exact queries recover with the tablet.">
            Collection, bitmap, cardinality, Bloom, Cuckoo, geo, JSON-index, and vector-index operations are
            bounded state-machine transforms. Exact typed queries use the same leader barrier and canonical
            state.
          </EvidenceCard>
          <EvidenceCard
            label="Pipeline &amp; signals"
            claim="Atomic batch and multiplex are different contracts."
          >
            Atomic batch is all-or-nothing. Multiplex returns request-ordered outcomes for independently
            committed identities. Node-affine Pub/Sub is explicitly at-most-once; durable consumers use the
            change stream.
          </EvidenceCard>
          <EvidenceCard label="Cold class" claim="Cold reads use an fsynced local-file path.">
            Every voter synchronizes cold files after committed apply and integrity-checks them on read.
            Status exposes retained bytes and observed local-file microseconds as a disclosure—not an SLO or
            heap offload claim.
          </EvidenceCard>
        </>
      }
      boundary={
        <p>
          Regional Cache v1 is a repository-local, fixed-topology single-shard alpha. Multi-shard routing and
          transactions, automatic client coalescing, RESP compatibility, generated response models, dynamic
          placement, production scale/SLO evidence, CRDTs, managed backup scheduling, and package-registry
          releases remain open.
        </p>
      }
    />
  );
}

export function RegionalBusBody() {
  return (
    <RegionalGuideBody
      provisionTitle="Start the voters and provision the Event Bus"
      provisionBody={
        <p>
          Reuse the disposable three-zone topology and development credentials. The Go bridge provisions{" "}
          <code>events</code>; application traffic then goes directly to the discovered Rust Event Bus leader.
        </p>
      }
      provisionLabel="Terminal C · provision"
      provision={regionalBusResource}
      guides={regionalBusLanguageGuides}
      guarantees={
        <>
          <EvidenceCard
            label="Routing &amp; retry"
            claim="Discovery preserves the exact caller-owned mutation."
          >
            Publish, subscription, delivery, maintenance, and settlement calls retain the same idempotency key
            and body across one bounded leader rediscovery. A changed body is a conflict, not a second event.
          </EvidenceCard>
          <EvidenceCard
            label="Delivery fencing"
            claim="Policy is replicated; settlement requires the opaque lease token."
          >
            Pull subscriptions bound timeout, concurrency, attempts, backoff, jitter, and age. Acquire returns
            a fenced delivery intent; acknowledge and fail cannot settle a stale lease.
          </EvidenceCard>
          <EvidenceCard
            label="Linearizable observation"
            claim="Query-shaped POST reads still require a leader barrier."
          >
            Archive replay, delivery query, mutation lookup, and status explicitly request{" "}
            <code>linearizable</code>. Maintenance advances retry or dead-letter state through a replicated
            command.
          </EvidenceCard>
          <EvidenceCard label="Signed HTTPS" claim="The leader leases before external I/O.">
            Signed HTTP/webhook targets carry only a replicated key ID. The current Bus leader awaits the
            exact acquire receipt, sends one CloudEvents binary-mode request through public-address-only
            egress, then commits Ack, retry, or terminal rejection.
          </EvidenceCard>
          <EvidenceCard label="Epoch targets" claim="The target commit precedes the Bus acknowledgement.">
            Queue and keyed multi-shard Stream writes are automatic. The source lease pins the exact target
            generation, shard, tablet, and epoch; a stable destination proposal prevents a second target
            record when source settlement is uncertain.
          </EvidenceCard>
        </>
      }
      extra={
        <>
          <Topic id="epoch-targets" title="Deliver directly to Epoch Queue and Stream">
            <p>
              Provision <code>jobs</code> and <code>orders</code> in the same namespace as the Bus, then
              create typed Queue and Stream subscriptions. Every regional node runs the scheduler, but only
              the current source Bus leader acts. No application dispatcher is required. Tune the scan with{" "}
              <code>EPOCH_REGIONAL_EPOCH_TARGET_DELIVERY_INTERVAL_MS</code> when the 100 ms default is not
              appropriate.
            </p>
            <CodeTabs
              label="Create native targets"
              samples={regionalSamples(
                epochTargetLanguageGuides,
                (guide) => guide.source,
                (guide) => guide.filename,
              )}
              collapsible={false}
            />
            <p>
              Queue binds shard <code>0</code>. Stream uses the same FNV-1a UTF-8 key router as direct SDK
              appends, with event ID fallback. Delivery query exposes the pinned destination coordinates as
              browser-safe strings after acquisition; clients cannot submit or replace that binding.
            </p>
            <Note title="Commit boundary">
              Epoch commits the destination enqueue or append before acknowledging the Bus record. The target
              proposal is stable across Bus attempts, but the two Raft-group commits are not one atomic
              cross-tablet transaction.
            </Note>
          </Topic>
          <Topic id="signed-webhooks" title="Receive and verify a signed webhook">
            <p>
              Give every voter the same external key set. Secret bytes never enter the subscription or Raft
              state; the target captures only <code>primary</code>. Normal delivery requires public HTTPS.
              Plain HTTP is restricted to an explicit loopback-only development switch.
            </p>
            <CodeBlock label="Regional node configuration" value={regionalWebhookConfiguration} />
            <CodeTabs
              label="Create the signed target"
              samples={regionalSamples(
                signedWebhookLanguageGuides,
                (guide) => guide.setup,
                (guide) => guide.filename,
              )}
              collapsible={false}
            />
            <p>
              Verify the exact raw body before decoding JSON. Enforce the timestamp window, then atomically
              claim <code>(delivery ID, attempt)</code> in a durable inbox before applying side effects. A
              valid signature authenticates the request; it does not make the receiver exactly once.
            </p>
            <CodeTabs
              label="Receiver verification"
              samples={regionalSamples(
                signedWebhookLanguageGuides,
                (guide) => guide.source,
                (guide) => guide.filename,
              )}
            />
            <CodeTabs
              label="Run the shared verifier tests"
              samples={regionalSamples(
                signedWebhookLanguageGuides,
                (guide) => guide.run,
                () => "Terminal · verify",
              )}
              collapsible={false}
            />
            <Note title="Outcome contract">
              Epoch acknowledges 2xx, retries 429/5xx and network failures, and terminally dead-letters other
              non-2xx responses. Redirects and ambient proxies are disabled; each request is capped by its
              replicated lease.
            </Note>
          </Topic>
        </>
      }
      boundary={
        <p>
          Regional Event Bus v1 is a repository-local, single-shard alpha. Built-in execution covers Epoch
          Queue/Stream and signed HTTP/webhook targets. Unsigned target workers, long-poll/push, target rate
          limiting, private managed egress, hot key reload, cross-shard ordering, generated response models,
          and public package-registry releases remain open.
        </p>
      }
    />
  );
}

/* --------------------------------------------------------------------------
   SDK reference
   -------------------------------------------------------------------------- */

export function SdkReferenceBody() {
  return (
    <>
      <Topic id="surface" title="Implemented operations">
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
      </Topic>

      <Topic id="conventions" title="Conventions">
        <div className="sdk-notes">
          <EvidenceCard label="Configuration" claim="Defaults stay explicit.">
            Go exposes <code>Default*Config</code>, Java exposes <code>*.defaults()</code>, and Python uses
            typed keyword defaults. Set <code>EPOCH_URL</code> in the walkthrough to select a node. Regional
            clients instead take all configured voter endpoints plus a bearer token.
          </EvidenceCard>
          <EvidenceCard label="Failures" claim="Inspect the typed API error.">
            Read status, code, detail, body, and retry classification. A transport-retryable error can still
            leave a mutation outcome unknown.
          </EvidenceCard>
          <EvidenceCard label="Control" claim="The server owns semantic validation.">
            Client-side checks improve feedback but do not replace server validation. Go also accepts a
            context for per-call cancellation and deadlines. Standalone helpers keep their local contract;{" "}
            <code>RegionalStreamClient</code>, <code>RegionalQueueClient</code>, and{" "}
            <code>RegionalCacheClient</code> are the explicit replicated alternatives.
          </EvidenceCard>
        </div>
      </Topic>
    </>
  );
}

/* --------------------------------------------------------------------------
   Design reference
   -------------------------------------------------------------------------- */

export function ReferenceBody() {
  return (
    <>
      <Topic id="contracts" title="Core contracts">
        <div className="reference-grid">
          <ReferenceCard
            eyebrow="Surface"
            title="API contracts"
            description="Routes, envelopes, errors, pagination, health, and the implemented alpha slice."
            href={`${repositoryDocsUrl}/API_CONTRACTS.md`}
          />
          <ReferenceCard
            eyebrow="Behavior"
            title="Semantics"
            description="Ordering, durability, acknowledgement, time, replay, and failure meaning."
            href={`${repositoryDocsUrl}/SEMANTICS.md`}
          />
          <ReferenceCard
            eyebrow="Evidence"
            title="Testing strategy"
            description="Restart, corruption, history, integration, and release evidence expectations."
            href={`${repositoryDocsUrl}/TESTING.md`}
          />
          <ReferenceCard
            eyebrow="Delivery"
            title="Delivery checklist"
            description="Table-based program gates, current core work, pull-request requirements, and release readiness."
            href={`${repositoryDocsUrl}/DELIVERY_CHECKLIST.md`}
          />
        </div>
      </Topic>

      <Topic id="sdk-contracts" title="SDK contracts">
        <div className="reference-grid">
          <ReferenceCard
            eyebrow="SDK contract"
            title="Regional Stream SDK"
            description="Fully qualified v1 routes, leader discovery, generation/tablet fencing, idempotent retry, linearizable reads, and three-language examples."
            href={`${repositoryDocsUrl}/REGIONAL_STREAM_SDK.md`}
          />
          <ReferenceCard
            eyebrow="SDK contract"
            title="Regional Queue SDK"
            description="Complete Queue lifecycle, leader discovery, generation/tablet fencing, exact mutation replay, linearizable reads, and three-language examples."
            href={`${repositoryDocsUrl}/REGIONAL_QUEUE_SDK.md`}
          />
          <ReferenceCard
            eyebrow="SDK contract"
            title="Regional Cache SDK"
            description="Strict and advanced state, byte/cold admission, atomic batch and multiplex, locks, changes, backup/PITR, lossy Pub/Sub, exact retry, and three-language examples."
            href={`${repositoryDocsUrl}/REGIONAL_CACHE_SDK.md`}
          />
          <ReferenceCard
            eyebrow="SDK contract"
            title="Regional Event Bus SDK"
            description="Subscription policy, replicated ingress, delivery leases, retry and dead-letter transitions, archive replay, exact retry, and three-language examples."
            href={`${repositoryDocsUrl}/REGIONAL_EVENT_BUS_SDK.md`}
          />
        </div>
      </Topic>

      <Topic id="runtime" title="Runtime and recovery">
        <div className="reference-grid">
          <ReferenceCard
            eyebrow="Regional runtime"
            title="Multi-tablet operations"
            description="Catalog authority, topology/capacity admission, fenced routes, quorum-confirmed reads, recovery campaign, and explicit non-claims."
            href={`${repositoryDocsUrl}/REGIONAL_RUNTIME.md`}
          />
          <ReferenceCard
            eyebrow="Consistency"
            title="Quorum read barriers"
            description="Safe ReadIndex admission, majority and local-apply completion, explicit stale opt-in, timeout behavior, and non-claims."
            href={`${repositoryDocsUrl}/adr/0013-quorum-read-barriers.md`}
          />
          <ReferenceCard
            eyebrow="Recovery core"
            title="Consensus checkpoints"
            description="EPSN v1/v2 bytes, native profile restore, bounded retry history, physical reclamation, lagging-voter catch-up, checkpoint-plus-tail reopen, and exact non-claims."
            href={`${repositoryDocsUrl}/CONSENSUS_CHECKPOINTS.md`}
          />
          <ReferenceCard
            eyebrow="Recovery design"
            title="Native checkpoints and reclamation"
            description="Profile ownership, rolling digest and retry bounds, durable ordering, atomic EPRS replacement, required evidence, and backup/PITR non-claims."
            href={`${repositoryDocsUrl}/adr/0022-profile-native-checkpoints-and-physical-reclamation.md`}
          />
          <ReferenceCard
            eyebrow="Automatic recovery"
            title="Every voter bounds its local journal"
            description="Actor-atomic applied-growth policy, catalog and profile scheduling, per-group topology evidence, all-voter compaction, and backup/PITR non-claims."
            href={`${repositoryDocsUrl}/adr/0028-automatic-regional-consensus-checkpoints.md`}
          />
          <ReferenceCard
            eyebrow="Regional timers"
            title="Leader-owned automatic maintenance"
            description="Pure profile deadlines, current-leader proposal ownership, exact due-time commands, deterministic retry identity, topology counters, recovery proof, and operational non-claims."
            href={`${repositoryDocsUrl}/adr/0027-regional-leader-maintenance.md`}
          />
        </div>
      </Topic>

      <Topic id="stream-design" title="Stream design decisions">
        <div className="reference-grid">
          <ReferenceCard
            eyebrow="Cluster core"
            title="Experimental Stream tablet"
            description="Typed single and bounded compressed-batch commands, fixed-voter majority, correlated offsets, failover, idempotency, and all-voter recovery."
            href={`${repositoryDocsUrl}/STREAM_TABLET.md`}
          />
          <ReferenceCard
            eyebrow="Stream design"
            title="Batch compression decision"
            description="Canonical framing, all required codecs, atomicity, decompression limits, compatibility rules, and stable-native non-claims."
            href={`${repositoryDocsUrl}/adr/0015-stream-batch-compression.md`}
          />
          <ReferenceCard
            eyebrow="SDK batch design"
            title="Regional atomic batch clients"
            description="Canonical none/gzip encoders, exact caller frames for every required codec, fenced routing, exact retry identity, and explicit streaming non-claims."
            href={`${repositoryDocsUrl}/adr/0026-regional-stream-batch-sdks.md`}
          />
          <ReferenceCard
            eyebrow="Consumer groups"
            title="Replicated checkpoint decision"
            description="Next-offset commit/reset, caller-generation owner fencing, committed rejection, lag/replay routes, recovery evidence, and coordinator/SDK non-claims."
            href={`${repositoryDocsUrl}/adr/0016-stream-consumer-group-checkpoints.md`}
          />
          <ReferenceCard
            eyebrow="Stream retention"
            title="Replicated time and size retention"
            description="Canonical byte accounting, inclusive age boundaries, combined policies, committed maintenance, checkpoint interaction, SDK routes, recovery evidence, and explicit non-claims."
            href={`${repositoryDocsUrl}/adr/0023-stream-retention-policies.md`}
          />
          <ReferenceCard
            eyebrow="Stream routing"
            title="Multi-shard key routing"
            description="Logical-to-physical partition identity, versioned FNV-1a UTF-8 vectors, generation-pinned keyed append, compatibility, recovery evidence, and online-expansion non-claims."
            href={`${repositoryDocsUrl}/adr/0024-stream-multishard-key-routing.md`}
          />
          <ReferenceCard
            eyebrow="Consumer sessions"
            title="Replicated membership and assignment"
            description="Shard-zero authority, bounded join/heartbeat/leave, monotonic generations and deadlines, deterministic assignment, checkpoint recovery, SDK routes, and atomic-handoff non-claims."
            href={`${repositoryDocsUrl}/adr/0025-stream-consumer-sessions.md`}
          />
          <ReferenceCard
            eyebrow="Fenced consumption"
            title="Claim, revalidate, then consume"
            description="Monotonic per-shard session claims, offset-preserving owner fences, bounded pull credit, SDK assignment revalidation, recovery evidence, and cross-shard atomicity non-claims."
            href={`${repositoryDocsUrl}/adr/0029-stream-session-fenced-consumption.md`}
          />
        </div>
      </Topic>

      <Topic id="workload-design" title="Queue, Cache, and Event Bus design decisions">
        <div className="reference-grid">
          <ReferenceCard
            eyebrow="Queue tablet"
            title="Experimental replicated Queue"
            description="Typed mutations, fenced leases, bounded consumer credit, failover/redelivery, immutable DLQ/redrive history, and all-voter recovery."
            href={`${repositoryDocsUrl}/QUEUE_TABLET.md`}
          />
          <ReferenceCard
            eyebrow="Flow control"
            title="Queue credit and in-flight windows"
            description="Atomic grant semantics, cross-epoch consumer accounting, command compatibility, flow evidence, and streaming non-claims."
            href={`${repositoryDocsUrl}/adr/0014-queue-consumer-credit.md`}
          />
          <ReferenceCard
            eyebrow="Queue design"
            title="Regional Queue routing decision"
            description="Native v1 route shape, shared discovery and retry contract, lease-token handling, authorization, recovery evidence, and explicit alpha boundaries."
            href={`${repositoryDocsUrl}/adr/0018-regional-queue-v1-and-sdk-routing.md`}
          />
          <ReferenceCard
            eyebrow="Cache design"
            title="Regional Cache routing decision"
            description="Native v1 route shape, strict values and mutations, CAS/transaction/expiry/lock semantics, shared retry contract, and alpha boundaries."
            href={`${repositoryDocsUrl}/adr/0019-regional-cache-v1-and-sdk-routing.md`}
          />
          <ReferenceCard
            eyebrow="Cache eviction"
            title="Committed access and atomic batches"
            description="Managed policy materialization, deterministic all-key/volatile victim selection, pure observation, one-request atomic batching, snapshot compatibility, and throughput non-claims."
            href={`${repositoryDocsUrl}/adr/0032-regional-cache-eviction-and-access-batches.md`}
          />
          <ReferenceCard
            eyebrow="Cache completion"
            title="State services and cold read tier"
            description="Typed transforms and queries, byte admission, named durability, change history, backup/PITR, non-atomic multiplex, lossy Pub/Sub, fsynced cold reads, and exact non-claims."
            href={`${repositoryDocsUrl}/adr/0034-cache-state-services-and-cold-read-tier.md`}
          />
          <ReferenceCard
            eyebrow="Event Bus design"
            title="Regional Event Bus routing decision"
            description="Native v1 route shape, complete pull-delivery lifecycle, subscription policy, shared retry contract, recovery evidence, and target boundaries."
            href={`${repositoryDocsUrl}/adr/0020-regional-event-bus-v1-and-sdk-routing.md`}
          />
          <ReferenceCard
            eyebrow="Webhook security"
            title="Leader-owned signed webhook delivery"
            description="Lease-before-I/O ordering, CloudEvents binary mode, exact-body HMAC, key rotation, receiver replay identity, public-only egress, and recovery evidence."
            href={`${repositoryDocsUrl}/adr/0030-leader-owned-signed-webhook-delivery.md`}
          />
          <ReferenceCard
            eyebrow="Cross-profile delivery"
            title="Leader-owned Epoch Queue and Stream targets"
            description="Pinned destination generations, shared Stream key routing, stable target idempotency, cross-group forwarding, recovery, and non-atomic transaction boundaries."
            href={`${repositoryDocsUrl}/adr/0031-leader-owned-epoch-target-delivery.md`}
          />
          <ReferenceCard
            eyebrow="Cache tablet"
            title="Experimental replicated Cache"
            description="Typed state, deterministic byte admission, atomic and multiplex mutations, changes, backup/PITR, Pub/Sub, cold reads, failover, and exact EPRS replay."
            href={`${repositoryDocsUrl}/CACHE_TABLET.md`}
          />
          <ReferenceCard
            eyebrow="Bus tablet"
            title="Experimental replicated Event Bus"
            description="Replicated ingress, per-subscription outbox leases, retry/DLQ history, signed webhook execution, archive replay, failover, and EPRS recovery."
            href={`${repositoryDocsUrl}/BUS_TABLET.md`}
          />
          <ReferenceCard
            eyebrow="Release"
            title="v0.1.0-alpha.6 release notes"
            description="Verified milestone highlights, source-only artifacts, compatibility guidance, and explicit alpha limitations."
            href={`${repositoryDocsUrl}/releases/v0.1.0-alpha.6.md`}
          />
        </div>
      </Topic>
    </>
  );
}
