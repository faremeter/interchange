import { type } from "arktype";

export const CreateAgent = type({
  name: "string",
  "description?": "string",
  "systemPrompt?": "string",
  "skills?": "Record<string, unknown>",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  "capabilities?": "Record<string, unknown>",
  "initialGrants?": type({
    resource: "string",
    action: "string",
    effect: "'allow' | 'deny' | 'ask'",
    "conditions?": "Record<string, unknown> | null",
  }).array(),
});

export const UpdateAgent = type({
  "name?": "string",
  "description?": "string",
  "systemPrompt?": "string",
  "skills?": "Record<string, unknown>",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  "capabilities?": "Record<string, unknown>",
});

export const AgentResponse = type({
  id: "string",
  tenantId: "string",
  principalId: "string",
  name: "string",
  "description?": "string | null",
  "systemPrompt?": "string | null",
  "skills?": "Record<string, unknown>",
  "contextConfig?": "Record<string, unknown>",
  "initialState?": "Record<string, unknown>",
  "modelConfig?": "Record<string, unknown>",
  currentVersion: "string",
  status: "'deployed' | 'stopped' | 'updating' | 'error'",
  "kernelId?": "string | null",
  "capabilities?": "Record<string, unknown>",
  createdAt: "string",
  updatedAt: "string",
});

export const AgentVersion = type({
  version: "string",
  status: "'active' | 'inactive' | 'failed'",
  createdAt: "string",
});

export const AgentHealth = type({
  liveness: "'ok' | 'unhealthy'",
  readiness: "'ok' | 'not_ready' | 'unhealthy'",
  "lastCheckedAt?": "string | null",
});

export const RollbackRequest = type({
  version: "string",
});

export const Capability = type({
  id: "string",
  agentId: "string",
  name: "string",
  "description?": "string | null",
  "pricing?": {
    "base?": {
      amount: "string",
      currency: "string",
    },
    "methods?": "string[]",
    "negotiable?": "boolean",
    "bounds?": {
      "min?": "string",
      "max?": "string",
    },
  },
});
