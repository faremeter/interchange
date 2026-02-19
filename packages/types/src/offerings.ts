import { type } from "arktype";

export const CreateOffering = type({
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

export const UpdateOffering = type({
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

export const OfferingSearch = type({
  "name?": "string",
  "minPrice?": "string",
  "maxPrice?": "string",
  "paymentMethod?": "string",
});

export const OfferingDetail = type({
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
