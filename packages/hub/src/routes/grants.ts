import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  CreateGrant,
  UpdateGrant,
  GrantResponse,
  EvaluateRequest,
  EvaluateResult,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Grants"],
    summary: "List capability grants in the tenant",
    description:
      "Lists all capability grants. Filterable by principalId, roleId, resource pattern, and effect.",
    parameters: [
      { name: "principalId", in: "query", schema: { type: "string" } },
      { name: "roleId", in: "query", schema: { type: "string" } },
      { name: "resource", in: "query", schema: { type: "string" } },
      {
        name: "effect",
        in: "query",
        schema: { type: "string", enum: ["allow", "deny", "ask"] },
      },
    ],
    responses: {
      200: {
        description: "List of grants",
        content: {
          "application/json": {
            schema: resolver(GrantResponse.array()),
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

app.post(
  "/",
  describeRoute({
    tags: ["Grants"],
    summary: "Create a capability grant",
    description:
      "Creates a grant targeting either a role or a principal directly. Exactly one of roleId or principalId must be provided.",
    responses: {
      201: {
        description: "Grant created",
        content: {
          "application/json": { schema: resolver(GrantResponse) },
        },
      },
      400: {
        description: "Validation error",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", CreateGrant),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:grantId",
  describeRoute({
    tags: ["Grants"],
    summary: "Get grant details",
    responses: {
      200: {
        description: "Grant details",
        content: {
          "application/json": { schema: resolver(GrantResponse) },
        },
      },
      404: {
        description: "Grant not found",
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

app.patch(
  "/:grantId",
  describeRoute({
    tags: ["Grants"],
    summary: "Update a grant",
    description: "Update effect, conditions, or expiry on an existing grant.",
    responses: {
      200: {
        description: "Grant updated",
        content: {
          "application/json": { schema: resolver(GrantResponse) },
        },
      },
      404: {
        description: "Grant not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateGrant),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:grantId",
  describeRoute({
    tags: ["Grants"],
    summary: "Revoke a grant",
    responses: {
      204: {
        description: "Grant revoked",
      },
      404: {
        description: "Grant not found",
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

export { app as grantRoutes };

// Evaluate endpoint is mounted under principals
const evaluateApp = new Hono<AppEnv>();

evaluateApp.post(
  "/",
  describeRoute({
    tags: ["Grants"],
    summary: "Evaluate grants for a principal",
    description:
      "Evaluates what would happen if a principal attempted an operation. Returns the resolved effect and all matching grants. Useful for debugging authorization.",
    responses: {
      200: {
        description: "Evaluation result",
        content: {
          "application/json": { schema: resolver(EvaluateResult) },
        },
      },
      404: {
        description: "Principal not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", EvaluateRequest),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { evaluateApp as evaluateRoutes };
