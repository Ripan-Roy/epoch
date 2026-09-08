import assert from "node:assert/strict";
import test from "node:test";

import { latencyDiagnosticPath, parseLatencyDiagnosis } from "../src/api/latencyDiagnostics.ts";
import type { RegionalResource } from "../src/api/types.ts";

const resource: RegionalResource = {
  canonicalName: "acme/payments/prod/core/stream/orders",
  organization: "acme",
  project: "payments",
  environment: "prod",
  namespace: "core",
  kind: "stream",
  name: "orders",
  generation: "2",
  observedGeneration: "2",
  workloadProfile: "stream_log",
  tablets: [],
  phase: "ready",
  summary: "ready",
  risks: [],
  placement: null,
  cacheConfiguration: null,
  governance: null,
};

test("latency diagnostic path uses exact encoded tenant identity and bounded profile", () => {
  assert.equal(
    latencyDiagnosticPath(resource),
    "/v1/observability/latency?organization=acme&project=payments&environment=prod&namespace=core&profile=stream",
  );
  assert.match(latencyDiagnosticPath({ ...resource, kind: "event_bus" }) ?? "", /profile=bus$/);
  assert.equal(latencyDiagnosticPath({ ...resource, kind: "table" }), null);
});

test("latency diagnosis decoder rejects invented or unsafe values", () => {
  assert.deepEqual(
    parseLatencyDiagnosis({
      cause: "replication",
      stage: "replication",
      observed_p99_ms: 640,
      samples: 12,
      recommendation: "Inspect replica lag.",
      regional_endpoint: "regional-2",
    }),
    {
      cause: "replication",
      stage: "replication",
      observedP99MS: 640,
      samples: 12,
      recommendation: "Inspect replica lag.",
      regionalEndpoint: "regional-2",
    },
  );
  for (const invalid of [
    {
      cause: "magic",
      stage: "magic",
      observed_p99_ms: 1,
      samples: 1,
      recommendation: "Guess.",
      regional_endpoint: "regional-1",
    },
    {
      cause: "storage",
      stage: "storage",
      observed_p99_ms: -1,
      samples: 1,
      recommendation: "Inspect I/O.",
      regional_endpoint: "regional-1",
    },
    {
      cause: "storage",
      stage: "storage",
      observed_p99_ms: 1,
      samples: 0,
      recommendation: "Inspect I/O.",
      regional_endpoint: "regional-1",
    },
  ]) {
    assert.throws(() => parseLatencyDiagnosis(invalid));
  }
});
