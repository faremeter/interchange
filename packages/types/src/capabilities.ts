import { type } from "arktype";

export const CreateCapability = type({
  agentId: "string",
  name: "string",
  "description?": "string",
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
  "schema?": "Record<string, unknown>",
});

export const UpdateCapability = type({
  "name?": "string",
  "description?": "string",
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
  "schema?": "Record<string, unknown>",
});

export const CapabilitySearch = type({
  "name?": "string",
  "minPrice?": "string",
  "maxPrice?": "string",
  "paymentMethod?": "string",
});

export const CapabilityDetail = type({
  id: "string",
  agentId: "string",
  agentName: "string",
  tenantId: "string",
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
  "schema?": "Record<string, unknown> | null",
});

export const ModelInfo = type({
  id: "string",
  providerId: "string",
  name: "string",
  "description?": "string | null",
  "capabilities?": "string[]",
  "pricing?": {
    "input?": "string",
    "output?": "string",
    "cacheRead?": "string",
    "cacheWrite?": "string",
  },
  "limits?": {
    "context?": "number",
    "output?": "number",
  },
});
