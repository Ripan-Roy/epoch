import assert from "node:assert/strict";
import test from "node:test";

import { governanceFilterSearchParams, parseGovernanceTagFilter } from "../src/governance.ts";

test("governance tag drafts canonicalize keys and reject ambiguous input", () => {
  assert.deepEqual(parseGovernanceTagFilter(" Service = checkout, tier= critical "), {
    service: "checkout",
    tier: "critical",
  });
  for (const invalid of ["missing-separator", "=value", "key=", "Service=one,service=two"]) {
    assert.throws(() => parseGovernanceTagFilter(invalid));
  }
});

test("governance inventory filters use stable repeated exact tag parameters", () => {
  const query = governanceFilterSearchParams({
    owner: "TEAM:PLATFORM",
    costCenter: "CC-1042",
    classification: "confidential",
    tags: { tier: "critical", service: "checkout" },
  });
  assert.equal(
    query.toString(),
    "owner=TEAM%3APLATFORM&cost_center=CC-1042&classification=confidential&tag=service%3Dcheckout&tag=tier%3Dcritical",
  );
});
