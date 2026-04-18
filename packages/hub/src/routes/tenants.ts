import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import {
  tenant,
  principal,
  role,
  principalRole,
  grant,
} from "@interchange/db/schema";
import {
  CreateTenant,
  UpdateTenant,
  TenantResponse,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";
import { first, ts } from "../format";
import { generateId } from "../ids";

const SYSTEM_ROLES = ["owner", "admin", "member"] as const;

function formatTenant(row: typeof tenant.$inferSelect) {
  return {
    id: row.id,
    name: row.name,
    slug: row.slug,
    domain: row.domain,
    parentId: row.parentId ?? null,
    config: (row.config as Record<string, unknown>) ?? undefined,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

const app = new Hono<AppEnv>();

app.post(
  "/",
  describeRoute({
    tags: ["Tenants"],
    summary: "Create a tenant",
    description:
      "Creates a new tenant. The authenticated user becomes the owner with a principal and default owner role.",
    responses: {
      201: {
        description: "Tenant created",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
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
  validator("json", CreateTenant),
  async (c) => {
    const user = c.get("user");
    if (!user) {
      return c.json(
        {
          error: { code: "unauthorized", message: "Authentication required" },
        },
        401,
      );
    }

    const body = c.req.valid("json");
    const db = c.get("db");

    const tenantId = generateId("tenant");
    const domain = `${body.slug}.localhost`;

    const existing = await db.query.tenant.findFirst({
      where: eq(tenant.slug, body.slug),
    });
    if (existing) {
      return c.json(
        {
          error: { code: "conflict", message: "Slug already taken" },
        },
        409,
      );
    }

    const now = new Date();

    const tenantRow = first(
      await db
        .insert(tenant)
        .values({
          id: tenantId,
          name: body.name,
          slug: body.slug,
          domain,
          parentId: body.parentId ?? null,
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    const roleIds: Record<string, string> = {};
    for (const roleName of SYSTEM_ROLES) {
      const roleId = generateId("role");
      roleIds[roleName] = roleId;
      await db.insert(role).values({
        id: roleId,
        tenantId,
        name: roleName,
        description: `System ${roleName} role`,
        isSystem: true,
        createdAt: now,
        updatedAt: now,
      });
    }

    const ownerRoleId = roleIds["owner"];
    if (!ownerRoleId) throw new Error("Owner role was not created");

    const principalId = generateId("principal");
    await db.insert(principal).values({
      id: principalId,
      tenantId,
      kind: "user",
      refId: user.id,
      status: "active",
      createdAt: now,
      updatedAt: now,
    });

    await db.insert(principalRole).values({
      principalId,
      roleId: ownerRoleId,
      createdAt: now,
    });

    // Grant owner role full access
    await db.insert(grant).values({
      id: generateId("grant"),
      tenantId,
      roleId: ownerRoleId,
      resource: "*",
      action: "*",
      effect: "allow",
      source: "system",
      createdAt: now,
      updatedAt: now,
    });

    // Grant admin role broad management access
    const adminRoleId = roleIds["admin"];
    if (adminRoleId) {
      await db.insert(grant).values([
        {
          id: generateId("grant"),
          tenantId,
          roleId: adminRoleId,
          resource: "*",
          action: "read",
          effect: "allow",
          source: "system",
          createdAt: now,
          updatedAt: now,
        },
        {
          id: generateId("grant"),
          tenantId,
          roleId: adminRoleId,
          resource: "*",
          action: "create",
          effect: "allow",
          source: "system",
          createdAt: now,
          updatedAt: now,
        },
        {
          id: generateId("grant"),
          tenantId,
          roleId: adminRoleId,
          resource: "*",
          action: "manage",
          effect: "allow",
          source: "system",
          createdAt: now,
          updatedAt: now,
        },
      ]);
    }

    // Grant member role read-only access
    const memberRoleId = roleIds["member"];
    if (memberRoleId) {
      await db.insert(grant).values({
        id: generateId("grant"),
        tenantId,
        roleId: memberRoleId,
        resource: "*",
        action: "read",
        effect: "allow",
        source: "system",
        createdAt: now,
        updatedAt: now,
      });
    }

    return c.json(formatTenant(tenantRow), 201);
  },
);

app.get(
  "/:tenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Get tenant details",
    responses: {
      200: {
        description: "Tenant details",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
        },
      },
      403: {
        description: "Not a member of this tenant",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      404: {
        description: "Tenant not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const user = c.get("user");
    if (!user) {
      return c.json(
        {
          error: { code: "unauthorized", message: "Authentication required" },
        },
        401,
      );
    }

    const tenantId = c.req.param("tenantId");
    const db = c.get("db");

    const tenantRow = await db.query.tenant.findFirst({
      where: eq(tenant.id, tenantId),
    });
    if (!tenantRow) {
      return c.json(
        { error: { code: "not_found", message: "Tenant not found" } },
        404,
      );
    }

    const membership = await db.query.principal.findFirst({
      where: and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "user"),
        eq(principal.refId, user.id),
      ),
    });
    if (!membership) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Not a member of this tenant",
          },
        },
        403,
      );
    }

    return c.json(formatTenant(tenantRow));
  },
);

app.patch(
  "/:tenantId",
  describeRoute({
    tags: ["Tenants"],
    summary: "Update tenant config",
    description: "Requires admin or higher grant within the tenant.",
    responses: {
      200: {
        description: "Tenant updated",
        content: {
          "application/json": { schema: resolver(TenantResponse) },
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
  validator("json", UpdateTenant),
  async (c) => {
    const user = c.get("user");
    if (!user) {
      return c.json(
        {
          error: { code: "unauthorized", message: "Authentication required" },
        },
        401,
      );
    }

    const tenantId = c.req.param("tenantId");
    const body = c.req.valid("json");
    const db = c.get("db");

    const membership = await db.query.principal.findFirst({
      where: and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "user"),
        eq(principal.refId, user.id),
      ),
    });
    if (!membership) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Not a member of this tenant",
          },
        },
        403,
      );
    }

    const updates: Record<string, unknown> = { updatedAt: new Date() };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.config !== undefined) updates["config"] = body.config;

    const [updated] = await db
      .update(tenant)
      .set(updates)
      .where(eq(tenant.id, tenantId))
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "not_found", message: "Tenant not found" } },
        404,
      );
    }

    return c.json(formatTenant(updated));
  },
);

export { app as tenantRoutes };
