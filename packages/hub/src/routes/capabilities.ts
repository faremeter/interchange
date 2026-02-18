import { Hono } from "hono";
import { describeRoute, resolver } from "hono-openapi";
import { CapabilityDetail, ModelInfo, ErrorResponse } from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Discovery"],
    summary: "Search capabilities",
    description:
      "Searches capabilities across discoverable agents in the tenant and federated tenants. Filterable by capability name, pricing range, and payment method.",
    parameters: [
      { name: "name", in: "query", schema: { type: "string" } },
      { name: "minPrice", in: "query", schema: { type: "string" } },
      { name: "maxPrice", in: "query", schema: { type: "string" } },
      { name: "paymentMethod", in: "query", schema: { type: "string" } },
    ],
    responses: {
      200: {
        description: "List of capabilities",
        content: {
          "application/json": {
            schema: resolver(CapabilityDetail.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:capabilityId",
  describeRoute({
    tags: ["Discovery"],
    summary: "Get capability details",
    description:
      "Returns pricing, agent info, and request/response type information.",
    responses: {
      200: {
        description: "Capability details",
        content: {
          "application/json": { schema: resolver(CapabilityDetail) },
        },
      },
      404: {
        description: "Capability not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { app as capabilityRoutes };

// Models endpoint is global (not tenant-scoped)
const modelsApp = new Hono<AppEnv>();

modelsApp.get(
  "/",
  describeRoute({
    tags: ["Discovery"],
    summary: "List available models",
    description:
      "Lists available models across configured providers with capabilities, pricing, and limits.",
    responses: {
      200: {
        description: "List of models",
        content: {
          "application/json": {
            schema: resolver(ModelInfo.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { modelsApp as modelRoutes };
