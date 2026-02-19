import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { role, principalRole, principal } from "@interchange/db/schema";
import {
  CreateRole,
  UpdateRole,
  RoleResponse,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";

import type { TenantEnv } from "../context";
import { first, ts } from "../format";
import { generateId } from "../ids";
import { requireGrant, idResource } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

function formatRole(row: typeof role.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    name: row.name,
    description: row.description ?? null,
    isSystem: row.isSystem,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

const app = new Hono<TenantEnv>();

app.get(
  "/",
  requireGrant("role:*", "read"),
  describeRoute({
    tags: ["Roles"],
    summary: "List roles in the tenant",
    description:
      "Lists both system roles (owner, admin, member) and custom roles.",
    parameters: [...pageParameters],
    responses: {
      200: {
        description: "List of roles",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(RoleResponse)),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(role.tenantId, tenantCtx.id)];
    if (cursor) {
      conditions.push(cursorCondition(role.createdAt, role.id, cursor));
    }

    const rows = await db.query.role.findMany({
      where: and(...conditions),
      orderBy: pageOrder(role.createdAt, role.id),
      limit,
    });

    return c.json(paginatedResponse(rows.map(formatRole), rows, limit));
  },
);

app.post(
  "/",
  requireGrant("role:*", "create"),
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
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json" as never) as typeof CreateRole.infer;
    const db = c.get("db");

    const now = new Date();
    const row = first(
      await db
        .insert(role)
        .values({
          id: generateId("role"),
          tenantId: tenantCtx.id,
          name: body.name,
          description: body.description ?? null,
          isSystem: false,
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    return c.json(formatRole(row), 201);
  },
);

app.get(
  "/:roleId",
  requireGrant(idResource("role", "roleId"), "read"),
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
  async (c) => {
    const tenantCtx = c.get("tenant");
    const roleId = c.req.param("roleId");
    const db = c.get("db");

    const row = await db.query.role.findFirst({
      where: and(eq(role.id, roleId), eq(role.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Role not found" } },
        404,
      );
    }

    return c.json(formatRole(row));
  },
);

app.patch(
  "/:roleId",
  requireGrant(idResource("role", "roleId"), "manage"),
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
  async (c) => {
    const tenantCtx = c.get("tenant");
    const roleId = c.req.param("roleId");
    const body = c.req.valid("json" as never) as typeof UpdateRole.infer;
    const db = c.get("db");

    const existing = await db.query.role.findFirst({
      where: and(eq(role.id, roleId), eq(role.tenantId, tenantCtx.id)),
    });

    if (!existing) {
      return c.json(
        { error: { code: "not_found", message: "Role not found" } },
        404,
      );
    }

    if (existing.isSystem) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Cannot modify system roles",
          },
        },
        403,
      );
    }

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.description !== undefined)
      updates["description"] = body.description;

    const updated = first(
      await db.update(role).set(updates).where(eq(role.id, roleId)).returning(),
    );

    return c.json(formatRole(updated));
  },
);

app.delete(
  "/:roleId",
  requireGrant(idResource("role", "roleId"), "manage"),
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
  async (c) => {
    const tenantCtx = c.get("tenant");
    const roleId = c.req.param("roleId");
    const db = c.get("db");

    const existing = await db.query.role.findFirst({
      where: and(eq(role.id, roleId), eq(role.tenantId, tenantCtx.id)),
    });

    if (!existing) {
      return c.json(
        { error: { code: "not_found", message: "Role not found" } },
        404,
      );
    }

    if (existing.isSystem) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Cannot delete system roles",
          },
        },
        403,
      );
    }

    const assignments = await db.query.principalRole.findMany({
      where: eq(principalRole.roleId, roleId),
    });

    if (assignments.length > 0) {
      return c.json(
        {
          error: {
            code: "bad_request",
            message: `Role is still assigned to ${assignments.length} principal(s)`,
          },
        },
        400,
      );
    }

    await db.delete(role).where(eq(role.id, roleId));

    return c.body(null, 204);
  },
);

export { app as roleRoutes };

// Role assignment routes are mounted under principals
const assignApp = new Hono<TenantEnv>();

assignApp.post(
  "/:roleId",
  requireGrant("role:*", "manage"),
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
  async (c) => {
    const tenantCtx = c.get("tenant");
    const principalId = c.req.param("principalId") ?? "";
    const roleId = c.req.param("roleId") ?? "";
    const db = c.get("db");

    const principalRow = await db.query.principal.findFirst({
      where: and(
        eq(principal.id, principalId),
        eq(principal.tenantId, tenantCtx.id),
      ),
    });
    if (!principalRow) {
      return c.json(
        { error: { code: "not_found", message: "Principal not found" } },
        404,
      );
    }

    const roleRow = await db.query.role.findFirst({
      where: and(eq(role.id, roleId), eq(role.tenantId, tenantCtx.id)),
    });
    if (!roleRow) {
      return c.json(
        { error: { code: "not_found", message: "Role not found" } },
        404,
      );
    }

    const existing = await db.query.principalRole.findFirst({
      where: and(
        eq(principalRole.principalId, principalId),
        eq(principalRole.roleId, roleId),
      ),
    });

    if (!existing) {
      await db.insert(principalRole).values({
        principalId,
        roleId,
        createdAt: new Date(),
      });
    }

    return c.body(null, 204);
  },
);

assignApp.delete(
  "/:roleId",
  requireGrant("role:*", "manage"),
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
  async (c) => {
    const principalId = c.req.param("principalId") ?? "";
    const roleId = c.req.param("roleId") ?? "";
    const db = c.get("db");

    const deleted = await db
      .delete(principalRole)
      .where(
        and(
          eq(principalRole.principalId, principalId),
          eq(principalRole.roleId, roleId),
        ),
      )
      .returning();

    if (deleted.length === 0) {
      return c.json(
        {
          error: { code: "not_found", message: "Assignment not found" },
        },
        404,
      );
    }

    return c.body(null, 204);
  },
);

export { assignApp as roleAssignRoutes };
