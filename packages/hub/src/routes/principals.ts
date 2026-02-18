import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  PrincipalResponse,
  UpdatePrincipal,
  InviteMember,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Principals"],
    summary: "List principals in the tenant",
    description:
      "Lists all principals (users and agents) in the tenant. Filterable by kind and status.",
    parameters: [
      {
        name: "kind",
        in: "query",
        schema: { type: "string", enum: ["user", "agent"] },
      },
      {
        name: "status",
        in: "query",
        schema: {
          type: "string",
          enum: ["active", "suspended", "invited", "deactivated"],
        },
      },
    ],
    responses: {
      200: {
        description: "List of principals",
        content: {
          "application/json": {
            schema: resolver(PrincipalResponse.array()),
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
  "/:principalId",
  describeRoute({
    tags: ["Principals"],
    summary: "Get principal details",
    description:
      "Returns principal details including kind, status, assigned roles, and effective grants.",
    responses: {
      200: {
        description: "Principal details",
        content: {
          "application/json": { schema: resolver(PrincipalResponse) },
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
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.patch(
  "/:principalId",
  describeRoute({
    tags: ["Principals"],
    summary: "Update principal status",
    description: "Activate, suspend, or deactivate a principal.",
    responses: {
      200: {
        description: "Principal updated",
        content: {
          "application/json": { schema: resolver(PrincipalResponse) },
        },
      },
      403: {
        description: "Insufficient grants",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdatePrincipal),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:principalId",
  describeRoute({
    tags: ["Principals"],
    summary: "Remove principal from tenant",
    description:
      "Removes a user or agent principal from the tenant. For agents, use agent deletion instead.",
    responses: {
      204: {
        description: "Principal removed",
      },
      403: {
        description: "Insufficient grants",
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

export { app as principalRoutes };

// Invite is mounted separately at ../members/invite in app.ts
const inviteApp = new Hono<AppEnv>();

inviteApp.post(
  "/",
  describeRoute({
    tags: ["Principals"],
    summary: "Invite a user to the tenant",
    description:
      "Invites a user by email. Creates a principal with invited status and optionally assigns a role.",
    responses: {
      201: {
        description: "Invitation sent",
        content: {
          "application/json": { schema: resolver(PrincipalResponse) },
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
  validator("json", InviteMember),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { inviteApp as inviteRoutes };
