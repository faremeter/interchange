import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  CreateAgent,
  UpdateAgent,
  AgentResponse,
  AgentVersion,
  AgentHealth,
  RollbackRequest,
  Capability,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Agents"],
    summary: "List agents in the tenant",
    description: "Filterable by capability and status.",
    parameters: [
      { name: "capability", in: "query", schema: { type: "string" } },
      {
        name: "status",
        in: "query",
        schema: {
          type: "string",
          enum: ["deployed", "stopped", "updating", "error"],
        },
      },
    ],
    responses: {
      200: {
        description: "List of agents",
        content: {
          "application/json": {
            schema: resolver(AgentResponse.array()),
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
    tags: ["Agents"],
    summary: "Create an agent",
    description:
      "Creates an agent and its corresponding principal. Accepts the agent definition and optional initial capability grants for the agent's principal.",
    responses: {
      201: {
        description: "Agent created",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
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
  validator("json", CreateAgent),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:agentId",
  describeRoute({
    tags: ["Agents"],
    summary: "Get agent details",
    description:
      "Returns the agent definition, status, health, capabilities, and principal ID.",
    responses: {
      200: {
        description: "Agent details",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
        },
      },
      404: {
        description: "Agent not found",
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
  "/:agentId",
  describeRoute({
    tags: ["Agents"],
    summary: "Update agent definition",
    description:
      "Updates the agent definition and creates a new version. The new version is deployed alongside the current version until health checks pass.",
    responses: {
      200: {
        description: "Agent updated",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
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
  validator("json", UpdateAgent),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:agentId",
  describeRoute({
    tags: ["Agents"],
    summary: "Retire an agent",
    description:
      "Deactivates the agent's principal and begins graceful shutdown. In-flight work is drained before the agent stops.",
    responses: {
      204: {
        description: "Agent retirement initiated",
      },
      404: {
        description: "Agent not found",
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
  "/:agentId/versions",
  describeRoute({
    tags: ["Agents"],
    summary: "List agent versions",
    description: "Lists all versions with their deployment status.",
    responses: {
      200: {
        description: "List of versions",
        content: {
          "application/json": {
            schema: resolver(AgentVersion.array()),
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
  "/:agentId/rollback",
  describeRoute({
    tags: ["Agents"],
    summary: "Rollback to a previous version",
    description:
      "Shifts traffic back to the specified version. The current version is stopped.",
    responses: {
      200: {
        description: "Rollback initiated",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
        },
      },
      400: {
        description: "Invalid version",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", RollbackRequest),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:agentId/health",
  describeRoute({
    tags: ["Agents"],
    summary: "Get agent health status",
    description: "Returns liveness and readiness status.",
    responses: {
      200: {
        description: "Health status",
        content: {
          "application/json": { schema: resolver(AgentHealth) },
        },
      },
      404: {
        description: "Agent not found",
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
  "/:agentId/capabilities",
  describeRoute({
    tags: ["Agents"],
    summary: "List agent capabilities",
    description:
      "Returns the agent's exposed capabilities with pricing metadata.",
    responses: {
      200: {
        description: "List of capabilities",
        content: {
          "application/json": {
            schema: resolver(Capability.array()),
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

export { app as agentRoutes };
