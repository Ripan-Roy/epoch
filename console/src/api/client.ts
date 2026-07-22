import type {
  CreateResourceInput,
  EngineHealth,
  ResourceCreated,
  ResourceSummary,
} from "./types";

const configuredBaseUrl = import.meta.env.VITE_EPOCH_API_BASE_URL?.trim();

export const apiBaseUrl = (configuredBaseUrl || "http://127.0.0.1:7601").replace(/\/$/, "");

const profilePaths = {
  cache: "caches",
  stream: "streams",
  queue: "queues",
  event_bus: "buses",
} as const;

interface NodeErrorPayload {
  error?:
    | string
    | {
        code?: string;
        detail?: string;
      };
}

export class NodeApiError extends Error {
  readonly status: number;
  readonly code?: string;

  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = "NodeApiError";
    this.status = status;
    this.code = code;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${apiBaseUrl}${path}`, {
      ...init,
      headers: {
        Accept: "application/json",
        ...init?.headers,
      },
    });
  } catch (error) {
    const detail = error instanceof Error ? error.message : "connection failed";
    throw new NodeApiError(`Could not reach the Epoch node at ${apiBaseUrl}: ${detail}`, 0);
  }

  if (!response.ok) {
    let payload: NodeErrorPayload | undefined;
    try {
      payload = (await response.json()) as NodeErrorPayload;
    } catch {
      // A proxy or transport may return a non-JSON error; the HTTP status is
      // still more truthful than inventing an Epoch error code.
    }
    const nodeError = payload?.error;
    const code = typeof nodeError === "object" ? nodeError.code : undefined;
    const detail =
      typeof nodeError === "string"
        ? nodeError
        : typeof nodeError === "object"
          ? nodeError.detail
          : undefined;
    throw new NodeApiError(detail || `Epoch node returned HTTP ${response.status}`, response.status, code);
  }

  return (await response.json()) as T;
}

export function getHealth(): Promise<EngineHealth> {
  return request<EngineHealth>("/healthz");
}

export function listResources(): Promise<ResourceSummary[]> {
  return request<ResourceSummary[]>("/v1/resources");
}

export function createResource(input: CreateResourceInput): Promise<ResourceCreated> {
  const collection = profilePaths[input.profile];
  return request<ResourceCreated>(`/v1/${collection}/${encodeURIComponent(input.name)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input.config),
  });
}
