import type { RegionalGovernanceFilter } from "./api/types";

export function parseGovernanceTagFilter(raw: string): Record<string, string> {
  const tags: Record<string, string> = {};
  if (!raw.trim()) {
    return tags;
  }
  for (const pair of raw.split(",")) {
    const separator = pair.indexOf("=");
    if (separator <= 0 || separator === pair.length - 1) {
      throw new Error("Tags must use comma-separated key=value pairs.");
    }
    const key = pair.slice(0, separator).trim();
    const value = pair.slice(separator + 1).trim();
    if (!key || !value) {
      throw new Error("Tag keys and values cannot be empty.");
    }
    const canonicalKey = key.toLowerCase();
    if (Object.hasOwn(tags, canonicalKey)) {
      throw new Error(`Tag ${canonicalKey} is repeated.`);
    }
    tags[canonicalKey] = value;
  }
  return tags;
}

export function governanceFilterSearchParams(filter: RegionalGovernanceFilter): URLSearchParams {
  const query = new URLSearchParams();
  if (filter.owner) query.set("owner", filter.owner);
  if (filter.costCenter) query.set("cost_center", filter.costCenter);
  if (filter.classification) query.set("classification", filter.classification);
  for (const [key, value] of Object.entries(filter.tags ?? {}).sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    query.append("tag", `${key}=${value}`);
  }
  return query;
}
