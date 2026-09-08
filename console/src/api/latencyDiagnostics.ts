import type { LatencyCause, LatencyDiagnosis, RegionalDataKind, RegionalResource } from "./types";

const causes = new Set<LatencyCause>([
  "quota",
  "hot_partition",
  "replication",
  "storage",
  "routing",
  "target",
  "client",
]);

export function latencyDiagnosticPath(resource: RegionalResource): string | null {
  const profile = diagnosticProfile(resource.kind);
  if (profile === null) {
    return null;
  }
  const query = new URLSearchParams({
    organization: resource.organization,
    project: resource.project,
    environment: resource.environment,
    namespace: resource.namespace,
    profile,
  });
  return `/v1/observability/latency?${query.toString()}`;
}

export function parseLatencyDiagnosis(value: unknown): LatencyDiagnosis {
  if (typeof value !== "object" || value === null) {
    throw new Error("Managed latency diagnosis is not an object");
  }
  const raw = value as Record<string, unknown>;
  if (
    typeof raw.cause !== "string" ||
    !causes.has(raw.cause as LatencyCause) ||
    raw.stage !== raw.cause ||
    !Number.isSafeInteger(raw.observed_p99_ms) ||
    (raw.observed_p99_ms as number) < 0 ||
    !Number.isSafeInteger(raw.samples) ||
    (raw.samples as number) < 1 ||
    typeof raw.recommendation !== "string" ||
    raw.recommendation.length < 1 ||
    raw.recommendation.length > 1_024 ||
    typeof raw.regional_endpoint !== "string" ||
    !/^regional-[1-9][0-9]*$/.test(raw.regional_endpoint)
  ) {
    throw new Error("Managed latency diagnosis violates the bounded contract");
  }
  return {
    cause: raw.cause as LatencyCause,
    stage: raw.stage as LatencyCause,
    observedP99MS: raw.observed_p99_ms as number,
    samples: raw.samples as number,
    recommendation: raw.recommendation,
    regionalEndpoint: raw.regional_endpoint,
  };
}

function diagnosticProfile(kind: RegionalDataKind): string | null {
  switch (kind) {
    case "cache":
    case "stream":
    case "queue":
      return kind;
    case "event_bus":
      return "bus";
    case "table":
      return null;
  }
}
