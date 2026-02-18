import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  CreateRole,
  UpdateRole,
  RoleResponse,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.get(
  "/",
  describeRoute({
    tags: ["Roles"],
    summary: "List roles in the tenant",
    description:
      "Lists both system roles (owner, admin, member) and custom roles.",
    responses: {
      200: {
        description: "List of roles",
        content: {
          "application/json": {
            schema: resolver(RoleResponse.array()),
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
    tags: ["Roles"],
    summary: "Create a custom role",
    responses: {
      201: {
        description: "Role created",
        content: {
          "application/json": { schema: resolver(RoleResponse) },
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
  validator("json", CreateRole),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:roleId",
  describeRoute({
    tags: ["Roles"],
    summary: "Get role details",
    description: "Returns role details including attached capability grants.",
    responses: {
      200: {
        description: "Role details",
        content: {
          "application/json": { schema: resolver(RoleResponse) },
        },
      },
      404: {
        description: "Role not found",
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
  "/:roleId",
  describeRoute({
    tags: ["Roles"],
    summary: "Update a role",
    description: "Update name or description. System roles cannot be modified.",
    responses: {
      200: {
        description: "Role updated",
        content: {
          "application/json": { schema: resolver(RoleResponse) },
        },
      },
      403: {
        description: "Cannot modify system role",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", UpdateRole),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:roleId",
  describeRoute({
    tags: ["Roles"],
    summary: "Delete a custom role",
    description:
      "Deletes a custom role. Fails if principals are currently assigned to it. System roles cannot be deleted.",
    responses: {
      204: {
        description: "Role deleted",
      },
      400: {
        description: "Role still assigned to principals",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      403: {
        description: "Cannot delete system role",
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

export { app as roleRoutes };

// Role assignment routes are mounted under principals
const assignApp = new Hono<AppEnv>();

assignApp.post(
  "/:roleId",
  describeRoute({
    tags: ["Roles"],
    summary: "Assign a role to a principal",
    description:
      "Assigns a role to a user or agent principal within the tenant.",
    responses: {
      204: {
        description: "Role assigned",
      },
      404: {
        description: "Principal or role not found",
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

assignApp.delete(
  "/:roleId",
  describeRoute({
    tags: ["Roles"],
    summary: "Remove a role from a principal",
    responses: {
      204: {
        description: "Role removed",
      },
      404: {
        description: "Assignment not found",
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

export { assignApp as roleAssignRoutes };
