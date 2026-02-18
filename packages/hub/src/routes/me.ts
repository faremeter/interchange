import { Hono } from "hono";
import { describeRoute, resolver } from "hono-openapi";
import {
  UserProfile,
  PrincipalSummary,
  AgentSummary,
  SessionSummary,
  ApprovalSummary,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["User"],
    summary: "Get current user profile",
    responses: {
      200: {
        description: "User profile",
        content: {
          "application/json": { schema: resolver(UserProfile) },
        },
      },
      401: {
        description: "Not authenticated",
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

app.get(
  "/principals",
  describeRoute({
    tags: ["User"],
    summary: "List principals across all tenants",
    description:
      "Returns all of the authenticated user's principals across tenants, with tenant name, roles, and status in each.",
    responses: {
      200: {
        description: "List of principals across tenants",
        content: {
          "application/json": {
            schema: resolver(PrincipalSummary.array()),
          },
        },
      },
      401: {
        description: "Not authenticated",
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

app.get(
  "/agents",
  describeRoute({
    tags: ["User"],
    summary: "List agents across all tenants",
    description:
      "Aggregates agents from all tenants the user belongs to. Each result is tagged with tenantId.",
    responses: {
      200: {
        description: "Agents across tenants",
        content: {
          "application/json": {
            schema: resolver(AgentSummary.array()),
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
  "/sessions",
  describeRoute({
    tags: ["User"],
    summary: "List sessions across all tenants",
    description:
      "Aggregates active sessions from all tenants the user belongs to. Each result is tagged with tenantId.",
    responses: {
      200: {
        description: "Sessions across tenants",
        content: {
          "application/json": {
            schema: resolver(SessionSummary.array()),
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
  "/approvals",
  describeRoute({
    tags: ["User"],
    summary: "List pending approvals across all tenants",
    description:
      "Aggregates pending approval requests from all tenants the user belongs to. Each result is tagged with tenantId.",
    responses: {
      200: {
        description: "Approvals across tenants",
        content: {
          "application/json": {
            schema: resolver(ApprovalSummary.array()),
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

export { app as meRoutes };
